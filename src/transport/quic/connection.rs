//! Connection management for QUIC transport.
//!
//! This module provides connection pooling and lifecycle management:
//! - Connection caching with LRU eviction
//! - Automatic reconnection on failure
//! - Connection health monitoring
//! - Thundering herd prevention via pending connection tracking
//! - Strict connection limits via semaphore

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_channel::{bounded, Receiver, Sender};
use async_lock::{RwLock, Semaphore, SemaphoreGuardArc};
use futures::FutureExt;
use parking_lot::Mutex as SyncMutex;
use quinn::{Connection, Endpoint};

/// Type alias for semaphore permit with Arc ownership.
type SemaphorePermitArc = SemaphoreGuardArc;

use super::config::QuicConfig;
use super::error::QuicError;

/// Statistics for the connection manager.
///
/// # Note on Accuracy
///
/// `active_connections` is a **point-in-time snapshot** taken when `stats()` is called.
/// It may be stale by the time you read it due to concurrent connection changes.
/// The atomic counters (`connections_established`, `messages_sent`, etc.) are eventually
/// consistent but may show temporary inconsistencies under high concurrency.
///
/// For monitoring purposes, these values are suitable for dashboards and alerts.
/// For precise connection limiting, the internal semaphore provides strict enforcement.
#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    /// Total connections established (monotonically increasing).
    pub connections_established: u64,
    /// Total connections closed (monotonically increasing).
    pub connections_closed: u64,
    /// Current active connections (approximate snapshot, may be stale).
    pub active_connections: usize,
    /// Connection errors encountered (monotonically increasing).
    pub connection_errors: u64,
    /// Messages sent (monotonically increasing).
    pub messages_sent: u64,
    /// Bytes sent (monotonically increasing).
    pub bytes_sent: u64,
}

/// Internal statistics tracking.
#[derive(Debug, Default)]
struct StatsInner {
    connections_established: AtomicU64,
    connections_closed: AtomicU64,
    connection_errors: AtomicU64,
    messages_sent: AtomicU64,
    bytes_sent: AtomicU64,
}

/// A pooled connection with metadata.
///
/// This struct owns the semaphore permit, ensuring it's automatically released
/// when the connection is dropped (RAII pattern). This prevents permit leaks
/// in all scenarios: explicit removal, eviction, overwrites, and panics.
struct PooledConnection {
    /// The QUIC connection.
    connection: Connection,
    /// Semaphore permit - released automatically on drop.
    /// This ensures we never leak permits regardless of how the connection is removed.
    _permit: SemaphorePermitArc,
    /// When the connection was established.
    #[allow(dead_code)]
    established_at: Instant,
    /// Last time the connection was used.
    last_used: Instant,
    /// Number of messages sent on this connection.
    messages_sent: u64,
}

impl PooledConnection {
    fn new(connection: Connection, permit: SemaphorePermitArc) -> Self {
        let now = Instant::now();
        Self {
            connection,
            _permit: permit,
            established_at: now,
            last_used: now,
            messages_sent: 0,
        }
    }

    fn touch(&mut self) {
        self.last_used = Instant::now();
        self.messages_sent += 1;
    }

    fn is_usable(&self) -> bool {
        // Check if connection is still open
        self.connection.close_reason().is_none()
    }

    #[allow(dead_code)]
    fn age(&self) -> Duration {
        self.established_at.elapsed()
    }

    fn idle_time(&self) -> Duration {
        self.last_used.elapsed()
    }
}

impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("connection", &"...")
            .field("established_at", &self.established_at)
            .field("last_used", &self.last_used)
            .field("messages_sent", &self.messages_sent)
            .finish()
    }
}

/// Result type for pending connection broadcasts.
type PendingResult = Result<Connection, Arc<QuicError>>;

/// Entry for tracking a pending connection attempt.
struct PendingConnection {
    /// Receiver to await the result of the connection attempt.
    receiver: Receiver<PendingResult>,
}

/// LRU index for efficient O(log N) eviction.
///
/// Maps last_used timestamps to peer IDs. When we need to evict the oldest
/// connection, we can find it in O(log N) by looking at the first entry.
struct LruIndex<I> {
    /// Maps timestamp -> set of peer IDs with that timestamp.
    /// Using a set because multiple connections might have the same timestamp.
    by_time: BTreeMap<Instant, HashSet<I>>,
    /// Maps peer ID -> timestamp for O(1) lookup when updating.
    by_peer: HashMap<I, Instant>,
}

impl<I: Clone + Eq + Hash> LruIndex<I> {
    fn new() -> Self {
        Self {
            by_time: BTreeMap::new(),
            by_peer: HashMap::new(),
        }
    }

    /// Insert or update a peer's timestamp.
    fn touch(&mut self, peer: &I, new_time: Instant) {
        // Remove old timestamp if exists
        if let Some(old_time) = self.by_peer.get(peer) {
            if let Some(peers) = self.by_time.get_mut(old_time) {
                peers.remove(peer);
                if peers.is_empty() {
                    self.by_time.remove(old_time);
                }
            }
        }

        // Insert new timestamp
        self.by_time
            .entry(new_time)
            .or_default()
            .insert(peer.clone());
        self.by_peer.insert(peer.clone(), new_time);
    }

    /// Remove a peer from the index.
    fn remove(&mut self, peer: &I) {
        if let Some(time) = self.by_peer.remove(peer) {
            if let Some(peers) = self.by_time.get_mut(&time) {
                peers.remove(peer);
                if peers.is_empty() {
                    self.by_time.remove(&time);
                }
            }
        }
    }

    /// Get the oldest (least recently used) peer ID.
    /// Returns None if the index is empty.
    fn oldest(&self) -> Option<I> {
        self.by_time
            .first_key_value()
            .and_then(|(_, peers)| peers.iter().next().cloned())
    }

    /// Check if index is empty.
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.by_peer.is_empty()
    }
}

/// Manages QUIC connections with pooling and LRU eviction.
pub struct ConnectionManager<I> {
    /// The QUIC endpoint for creating connections.
    endpoint: Endpoint,
    /// Client configuration for outbound connections.
    client_config: quinn::ClientConfig,
    /// Pooled connections indexed by peer ID.
    connections: RwLock<HashMap<I, PooledConnection>>,
    /// LRU index for O(log N) eviction of oldest connections.
    lru: RwLock<LruIndex<I>>,
    /// Pending connections being established (prevents thundering herd).
    /// Uses synchronous mutex to enable panic-safe cleanup via Drop guards.
    pending: SyncMutex<HashMap<I, PendingConnection>>,
    /// Semaphore to enforce strict connection limit.
    connection_semaphore: Arc<Semaphore>,
    /// Configuration.
    config: QuicConfig,
    /// Statistics.
    stats: StatsInner,
}

impl<I> ConnectionManager<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    /// Create a new connection manager.
    pub fn new(endpoint: Endpoint, client_config: quinn::ClientConfig, config: QuicConfig) -> Self {
        let max_connections = config.connection.max_connections;
        Self {
            endpoint,
            client_config,
            connections: RwLock::new(HashMap::new()),
            lru: RwLock::new(LruIndex::new()),
            pending: SyncMutex::new(HashMap::new()),
            connection_semaphore: Arc::new(Semaphore::new(max_connections)),
            config,
            stats: StatsInner::default(),
        }
    }

    /// Get or create a connection to a peer.
    ///
    /// This method prevents the "thundering herd" problem by tracking pending
    /// connection attempts. If multiple tasks try to connect to the same peer
    /// simultaneously, only one will initiate the actual connection while
    /// others wait for the result.
    ///
    /// # Panic Safety
    ///
    /// The pending map cleanup uses a RAII guard (`PendingGuard`) that ensures
    /// the entry is removed even if `do_connect` panics. This prevents peers
    /// from getting stuck in the pending state.
    ///
    /// # Race Condition (Best-Effort Check)
    ///
    /// There is a potential race between the fast path (checking existing connection
    /// with a read lock) and the slow path (acquiring the pending lock). A connection
    /// could close or be removed between these two checks. This is acceptable because:
    ///
    /// - Reconnection is idempotent (creating a new connection is harmless)
    /// - The pending map prevents redundant concurrent connects to the same peer
    /// - Holding both async RwLock and sync Mutex simultaneously risks deadlock
    ///
    /// The worst case is a single unnecessary reconnection, which is preferable to
    /// the complexity and deadlock risk of stricter locking.
    pub async fn get_or_connect(
        &self,
        peer: &I,
        addr: SocketAddr,
    ) -> Result<Connection, QuicError> {
        // Fast path: check for existing usable connection (read lock)
        {
            let connections = self.connections.read().await;
            if let Some(pooled) = connections.get(peer) {
                if pooled.is_usable() {
                    return Ok(pooled.connection.clone());
                }
            }
        }

        // Slow path: need to connect or wait for pending connection
        // Determine if we should initiate connection or wait for pending one
        enum Action {
            /// We are the initiator - perform the connection
            Initiate(Sender<PendingResult>),
            /// Another task is connecting - wait for result
            Wait(Receiver<PendingResult>),
        }

        let action = {
            let mut pending = self.pending.lock();

            // Double-check for existing connection after acquiring lock
            // Note: We can't hold the async lock here while holding the sync lock,
            // but this is a best-effort check. The connection manager handles races.

            // Check if there's already a pending connection for this peer
            if let Some(pending_conn) = pending.get(peer) {
                // Another task is already connecting, wait for its result
                Action::Wait(pending_conn.receiver.clone())
            } else {
                // We're the first to connect - create a broadcast channel
                let (sender, receiver) = bounded::<PendingResult>(1);

                // Store the pending connection
                pending.insert(
                    peer.clone(),
                    PendingConnection {
                        receiver: receiver.clone(),
                    },
                );

                Action::Initiate(sender)
            }
            // pending lock is dropped here before we do any async work
        };

        match action {
            Action::Initiate(sender) => {
                // RAII guard for panic-safe cleanup
                // This ensures the pending entry is removed even if do_connect panics
                struct PendingGuard<'a, I: Clone + Eq + Hash> {
                    pending: &'a SyncMutex<HashMap<I, PendingConnection>>,
                    peer: I,
                }

                impl<I: Clone + Eq + Hash> Drop for PendingGuard<'_, I> {
                    fn drop(&mut self) {
                        // Synchronous cleanup - safe to call from Drop
                        let mut pending = self.pending.lock();
                        pending.remove(&self.peer);
                    }
                }

                // Create guard - will cleanup on drop (normal or panic)
                let _guard = PendingGuard {
                    pending: &self.pending,
                    peer: peer.clone(),
                };

                // We are responsible for establishing the connection
                let result = self.do_connect(peer.clone(), addr, &sender).await;

                // Guard is dropped here, removing from pending map
                // (happens whether we return Ok, Err, or panic)

                result.map_err(|e| match Arc::try_unwrap(e) {
                    Ok(err) => err,
                    Err(arc_err) => QuicError::Internal(arc_err.to_string()),
                })
            }
            Action::Wait(receiver) => {
                // Wait for the pending connection result
                match receiver.recv().await {
                    Ok(Ok(conn)) => Ok(conn),
                    Ok(Err(arc_err)) => Err(match Arc::try_unwrap(arc_err) {
                        Ok(err) => err,
                        Err(arc_err) => QuicError::Internal(arc_err.to_string()),
                    }),
                    Err(_) => Err(QuicError::Internal(
                        "pending connection channel closed".to_string(),
                    )),
                }
            }
        }
    }

    /// Internal method to perform the actual connection with semaphore-based limiting.
    ///
    /// # Semaphore Behavior
    ///
    /// Uses `async_lock::Semaphore::acquire_arc()` which returns `Future<Output = SemaphoreGuardArc>`
    /// (not a Result). The future completes when a permit is available - it never errors.
    /// The select! with timeout ensures we don't wait forever if the pool is exhausted.
    ///
    /// The semaphore is owned by ConnectionManager, so it cannot be dropped while
    /// do_connect is running, making the acquire safe.
    async fn do_connect(
        &self,
        peer: I,
        addr: SocketAddr,
        result_sender: &Sender<PendingResult>,
    ) -> Result<Connection, Arc<QuicError>> {
        // Acquire semaphore permit before connecting (enforces strict limit)
        // Try to acquire without blocking first
        let permit = match self.connection_semaphore.try_acquire_arc() {
            Some(permit) => permit,
            None => {
                // At connection limit - try to evict an idle connection
                self.evict_idle().await;

                // Try again with a timeout
                let acquire_result = futures::select! {
                    permit = self.connection_semaphore.acquire_arc().fuse() => Some(permit),
                    _ = futures_timer::Delay::new(Duration::from_secs(5)).fuse() => None,
                };

                match acquire_result {
                    Some(permit) => permit,
                    None => {
                        // Get current count from connections map
                        let current = self.connections.read().await.len();
                        let err = Arc::new(QuicError::MaxConnectionsReached {
                            current,
                            max: self.config.connection.max_connections,
                        });
                        let _ = result_sender.send(Err(err.clone())).await;
                        return Err(err);
                    }
                }
            }
        };

        // Perform the connection with retry logic
        let mut last_error = None;
        for attempt in 0..=self.config.connection.retries {
            if attempt > 0 {
                futures_timer::Delay::new(self.config.connection.retry_delay).await;
            }

            match self.try_connect(addr).await {
                Ok(connection) => {
                    // Store the connection with its permit (RAII)
                    // The permit is moved into PooledConnection and will be automatically
                    // released when the connection is dropped (removed, evicted, or on panic)
                    let now = Instant::now();
                    {
                        let mut connections = self.connections.write().await;
                        let pooled = PooledConnection::new(connection.clone(), permit);
                        connections.insert(peer.clone(), pooled);
                    }
                    // Update LRU index
                    {
                        let mut lru = self.lru.write().await;
                        lru.touch(&peer, now);
                    }
                    self.stats
                        .connections_established
                        .fetch_add(1, Ordering::Relaxed);

                    // Broadcast success to any waiting tasks
                    let _ = result_sender.send(Ok(connection.clone())).await;

                    return Ok(connection);
                }
                Err(e) => {
                    last_error = Some(e);
                    self.stats.connection_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let err = Arc::new(last_error.unwrap_or(QuicError::Internal(
            "connection failed with no error".to_string(),
        )));

        // Broadcast failure
        let _ = result_sender.send(Err(err.clone())).await;

        // Permit is dropped here automatically, releasing the slot
        Err(err)
    }

    /// Try to establish a connection once.
    async fn try_connect(&self, addr: SocketAddr) -> Result<Connection, QuicError> {
        // Use configured server name for TLS SNI, defaulting to "localhost" for self-signed certs.
        // For production with real certificates, configure `tls.server_name` to match the cert's CN/SAN.
        let server_name = self.config.tls.server_name_or_default();

        let connecting = self
            .endpoint
            .connect_with(self.client_config.clone(), addr, server_name)
            .map_err(|_e| QuicError::Connection(quinn::ConnectionError::LocallyClosed))?;

        // Wait for connection with timeout using futures::select
        let timeout_duration = self.config.connection.handshake_timeout;
        let timer = futures_timer::Delay::new(timeout_duration).fuse();
        let connect_fut = connecting.fuse();

        futures::pin_mut!(timer, connect_fut);

        futures::select! {
            result = connect_fut => {
                result.map_err(QuicError::Connection)
            }
            _ = timer => {
                Err(QuicError::HandshakeTimeout {
                    addr,
                    timeout_ms: timeout_duration.as_millis() as u64,
                })
            }
        }
    }

    /// Send data on a connection to a peer.
    pub async fn send(
        &self,
        peer: &I,
        addr: SocketAddr,
        data: bytes::Bytes,
    ) -> Result<(), QuicError> {
        let connection = self.get_or_connect(peer, addr).await?;

        // Open a unidirectional stream and send
        let mut stream = connection
            .open_uni()
            .await
            .map_err(|e| QuicError::Stream(e.to_string()))?;

        stream.write_all(&data).await.map_err(QuicError::Write)?;
        stream
            .finish()
            .map_err(|_| QuicError::Write(quinn::WriteError::ClosedStream))?;

        // Update stats and touch connection
        self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.stats
            .bytes_sent
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        let now = Instant::now();
        {
            let mut connections = self.connections.write().await;
            if let Some(pooled) = connections.get_mut(peer) {
                pooled.touch();
            }
        }
        // Update LRU index
        {
            let mut lru = self.lru.write().await;
            lru.touch(peer, now);
        }

        Ok(())
    }

    /// Remove a connection to a peer.
    ///
    /// The semaphore permit is automatically released when the `PooledConnection`
    /// is dropped (RAII pattern).
    pub async fn remove(&self, peer: &I) -> bool {
        let removed = {
            let mut connections = self.connections.write().await;
            if let Some(pooled) = connections.remove(peer) {
                pooled.connection.close(0u32.into(), b"removed");
                self.stats
                    .connections_closed
                    .fetch_add(1, Ordering::Relaxed);
                // Permit is released automatically when pooled is dropped
                true
            } else {
                false
            }
        };
        if removed {
            let mut lru = self.lru.write().await;
            lru.remove(peer);
        }
        removed
    }

    /// Evict the most idle (least recently used) connection.
    ///
    /// Uses the LRU index for O(log N) lookup instead of scanning all connections.
    /// The semaphore permit is automatically released when the `PooledConnection`
    /// is dropped (RAII pattern).
    async fn evict_idle(&self) {
        // Find the oldest connection using LRU index (O(log N))
        let oldest_peer = {
            let lru = self.lru.read().await;
            lru.oldest()
        };

        if let Some(peer_id) = oldest_peer {
            // Remove from connections
            let removed = {
                let mut connections = self.connections.write().await;
                if let Some(pooled) = connections.remove(&peer_id) {
                    pooled.connection.close(0u32.into(), b"evicted");
                    self.stats
                        .connections_closed
                        .fetch_add(1, Ordering::Relaxed);
                    // Permit is released automatically when pooled is dropped
                    true
                } else {
                    false
                }
            };
            // Remove from LRU index
            if removed {
                let mut lru = self.lru.write().await;
                lru.remove(&peer_id);
            }
        }
    }

    /// Close all connections.
    ///
    /// All semaphore permits are automatically released when the `PooledConnection`s
    /// are dropped (RAII pattern).
    pub async fn close_all(&self) {
        {
            let mut connections = self.connections.write().await;
            for (_, pooled) in connections.drain() {
                pooled.connection.close(0u32.into(), b"shutdown");
                self.stats
                    .connections_closed
                    .fetch_add(1, Ordering::Relaxed);
                // Permit is released automatically when pooled is dropped
            }
        }
        // Clear LRU index
        {
            let mut lru = self.lru.write().await;
            *lru = LruIndex::new();
        }
    }

    /// Get the number of active connections.
    #[allow(dead_code)]
    pub async fn connection_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Get connection statistics.
    pub async fn stats(&self) -> ConnectionStats {
        ConnectionStats {
            connections_established: self.stats.connections_established.load(Ordering::Relaxed),
            connections_closed: self.stats.connections_closed.load(Ordering::Relaxed),
            active_connections: self.connections.read().await.len(),
            connection_errors: self.stats.connection_errors.load(Ordering::Relaxed),
            messages_sent: self.stats.messages_sent.load(Ordering::Relaxed),
            bytes_sent: self.stats.bytes_sent.load(Ordering::Relaxed),
        }
    }

    /// Get the RTT for a connection to a peer.
    pub async fn get_rtt(&self, peer: &I) -> Option<Duration> {
        let connections = self.connections.read().await;
        connections
            .get(peer)
            .filter(|p| p.is_usable())
            .map(|p| p.connection.rtt())
    }

    /// Clean up stale connections.
    ///
    /// Removes connections that have been idle longer than the idle timeout.
    /// Semaphore permits are automatically released when connections are dropped (RAII).
    pub async fn cleanup_stale(&self) -> usize {
        let idle_timeout = self.config.connection.idle_timeout;

        let stale: Vec<I> = {
            let connections = self.connections.read().await;
            connections
                .iter()
                .filter(|(_, pooled)| !pooled.is_usable() || pooled.idle_time() > idle_timeout)
                .map(|(id, _)| id.clone())
                .collect()
        };

        let count = stale.len();
        if count > 0 {
            let mut connections = self.connections.write().await;
            let mut lru = self.lru.write().await;

            for peer_id in &stale {
                if let Some(pooled) = connections.remove(peer_id) {
                    if pooled.is_usable() {
                        pooled.connection.close(0u32.into(), b"idle");
                    }
                    self.stats
                        .connections_closed
                        .fetch_add(1, Ordering::Relaxed);
                    lru.remove(peer_id);
                    // Permit is released automatically when pooled is dropped
                }
            }
        }
        count
    }

    /// Check if a peer has an active connection.
    pub async fn has_connection(&self, peer: &I) -> bool {
        let connections = self.connections.read().await;
        connections
            .get(peer)
            .map(|p| p.is_usable())
            .unwrap_or(false)
    }

    /// Get the endpoint.
    #[allow(dead_code)]
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

impl<I> Debug for ConnectionManager<I>
where
    I: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionManager")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full connection tests require tokio runtime and actual QUIC endpoints.
    // These tests verify the non-async parts of the module.

    #[test]
    fn test_pooled_connection_age() {
        // This test would require a mock Connection, which is complex.
        // For now, we verify the stats structure.
        let stats = ConnectionStats::default();
        assert_eq!(stats.connections_established, 0);
        assert_eq!(stats.active_connections, 0);
    }

    #[test]
    fn test_connection_stats_default() {
        let stats = ConnectionStats::default();
        assert_eq!(stats.connections_established, 0);
        assert_eq!(stats.connections_closed, 0);
        assert_eq!(stats.active_connections, 0);
        assert_eq!(stats.connection_errors, 0);
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.bytes_sent, 0);
    }
}
