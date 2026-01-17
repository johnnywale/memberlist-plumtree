//! Connection management for QUIC transport.
//!
//! This module provides connection pooling and lifecycle management:
//! - Connection caching with LRU eviction
//! - Automatic reconnection on failure
//! - Connection health monitoring

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_lock::RwLock;
use futures::FutureExt;
use quinn::{Connection, Endpoint};

use super::config::QuicConfig;
use super::error::QuicError;

/// Statistics for the connection manager.
#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    /// Total connections established.
    pub connections_established: u64,
    /// Total connections closed.
    pub connections_closed: u64,
    /// Current active connections.
    pub active_connections: usize,
    /// Connection errors encountered.
    pub connection_errors: u64,
    /// Messages sent.
    pub messages_sent: u64,
    /// Bytes sent.
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
#[derive(Debug)]
struct PooledConnection {
    /// The QUIC connection.
    connection: Connection,
    /// When the connection was established.
    #[allow(dead_code)]
    established_at: Instant,
    /// Last time the connection was used.
    last_used: Instant,
    /// Number of messages sent on this connection.
    messages_sent: u64,
}

impl PooledConnection {
    fn new(connection: Connection) -> Self {
        let now = Instant::now();
        Self {
            connection,
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

/// Manages QUIC connections with pooling and LRU eviction.
pub struct ConnectionManager<I> {
    /// The QUIC endpoint for creating connections.
    endpoint: Endpoint,
    /// Client configuration for outbound connections.
    client_config: quinn::ClientConfig,
    /// Pooled connections indexed by peer ID.
    connections: RwLock<HashMap<I, PooledConnection>>,
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
    pub fn new(
        endpoint: Endpoint,
        client_config: quinn::ClientConfig,
        config: QuicConfig,
    ) -> Self {
        Self {
            endpoint,
            client_config,
            connections: RwLock::new(HashMap::new()),
            config,
            stats: StatsInner::default(),
        }
    }

    /// Get or create a connection to a peer.
    pub async fn get_or_connect(
        &self,
        peer: &I,
        addr: SocketAddr,
    ) -> Result<Connection, QuicError> {
        // Try to get existing connection
        {
            let connections = self.connections.read().await;
            if let Some(pooled) = connections.get(peer) {
                if pooled.is_usable() {
                    return Ok(pooled.connection.clone());
                }
            }
        }

        // Need to create a new connection
        self.connect(peer.clone(), addr).await
    }

    /// Create a new connection to a peer.
    async fn connect(&self, peer: I, addr: SocketAddr) -> Result<Connection, QuicError> {
        // Check connection limit
        {
            let connections = self.connections.read().await;
            if connections.len() >= self.config.connection.max_connections {
                // Try to evict an idle connection
                drop(connections);
                self.evict_idle().await;
            }
        }

        // Recheck limit after potential eviction
        {
            let connections = self.connections.read().await;
            if connections.len() >= self.config.connection.max_connections {
                return Err(QuicError::MaxConnectionsReached {
                    current: connections.len(),
                    max: self.config.connection.max_connections,
                });
            }
        }

        // Connect with retry logic
        let mut last_error = None;
        for attempt in 0..=self.config.connection.retries {
            if attempt > 0 {
                futures_timer::Delay::new(self.config.connection.retry_delay).await;
            }

            match self.try_connect(addr).await {
                Ok(connection) => {
                    let mut connections = self.connections.write().await;
                    let pooled = PooledConnection::new(connection.clone());
                    connections.insert(peer, pooled);
                    self.stats.connections_established.fetch_add(1, Ordering::Relaxed);
                    return Ok(connection);
                }
                Err(e) => {
                    last_error = Some(e);
                    self.stats.connection_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        Err(last_error.unwrap_or(QuicError::Internal(
            "connection failed with no error".to_string(),
        )))
    }

    /// Try to establish a connection once.
    async fn try_connect(&self, addr: SocketAddr) -> Result<Connection, QuicError> {
        // Use "localhost" as server name for self-signed certs
        let server_name = "localhost";

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
        stream.finish().map_err(|_| QuicError::Write(quinn::WriteError::ClosedStream))?;

        // Update stats and touch connection
        self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.stats.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);

        {
            let mut connections = self.connections.write().await;
            if let Some(pooled) = connections.get_mut(peer) {
                pooled.touch();
            }
        }

        Ok(())
    }

    /// Remove a connection to a peer.
    pub async fn remove(&self, peer: &I) -> bool {
        let mut connections = self.connections.write().await;
        if let Some(pooled) = connections.remove(peer) {
            pooled.connection.close(0u32.into(), b"removed");
            self.stats.connections_closed.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Evict the most idle connection.
    async fn evict_idle(&self) {
        let mut connections = self.connections.write().await;

        // Find the most idle connection
        let most_idle = connections
            .iter()
            .max_by_key(|(_, pooled)| pooled.idle_time())
            .map(|(id, _)| id.clone());

        if let Some(peer_id) = most_idle {
            if let Some(pooled) = connections.remove(&peer_id) {
                pooled.connection.close(0u32.into(), b"evicted");
                self.stats.connections_closed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Close all connections.
    pub async fn close_all(&self) {
        let mut connections = self.connections.write().await;
        for (_, pooled) in connections.drain() {
            pooled.connection.close(0u32.into(), b"shutdown");
            self.stats.connections_closed.fetch_add(1, Ordering::Relaxed);
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
    pub async fn cleanup_stale(&self) {
        let idle_timeout = self.config.connection.idle_timeout;
        let mut connections = self.connections.write().await;

        let stale: Vec<I> = connections
            .iter()
            .filter(|(_, pooled)| !pooled.is_usable() || pooled.idle_time() > idle_timeout)
            .map(|(id, _)| id.clone())
            .collect();

        for peer_id in stale {
            if let Some(pooled) = connections.remove(&peer_id) {
                if pooled.is_usable() {
                    pooled.connection.close(0u32.into(), b"idle");
                }
                self.stats.connections_closed.fetch_add(1, Ordering::Relaxed);
            }
        }
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
