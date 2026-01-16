//! Pooled transport with connection management and concurrency control.
//!
//! This module provides a transport wrapper that adds:
//!
//! - **Concurrency limiting**: Prevent overwhelming individual peers
//! - **Request queuing**: Buffer messages when at capacity
//! - **Fair scheduling**: Ensure all peers get fair access
//! - **Statistics**: Track pool usage and performance
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{PooledTransport, PoolConfig, Transport};
//!
//! let inner = MyTransport::new();
//! let pooled = PooledTransport::new(inner, PoolConfig::default());
//!
//! // Use like any other transport
//! pooled.send_to(&peer_id, data).await?;
//!
//! // Check pool statistics
//! let stats = pooled.stats();
//! println!("Active: {}, Queued: {}", stats.active_sends, stats.queued_sends);
//! ```

use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use async_lock::Semaphore;
use bytes::Bytes;
use parking_lot::Mutex;

use crate::transport::Transport;

/// Configuration for the pooled transport.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum concurrent sends per peer.
    pub max_concurrent_per_peer: usize,

    /// Global maximum concurrent sends across all peers.
    pub max_concurrent_global: usize,

    /// Maximum pending sends in queue per peer before dropping.
    pub max_queue_per_peer: usize,

    /// Whether to enable fair scheduling across peers.
    pub fair_scheduling: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_concurrent_per_peer: 8,
            max_concurrent_global: 256,
            max_queue_per_peer: 64,
            fair_scheduling: true,
        }
    }
}

impl PoolConfig {
    /// Create a new pool configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configuration for high-throughput scenarios.
    pub fn high_throughput() -> Self {
        Self {
            max_concurrent_per_peer: 16,
            max_concurrent_global: 512,
            max_queue_per_peer: 128,
            fair_scheduling: true,
        }
    }

    /// Configuration for low-latency scenarios.
    pub fn low_latency() -> Self {
        Self {
            max_concurrent_per_peer: 4,
            max_concurrent_global: 128,
            max_queue_per_peer: 32,
            fair_scheduling: false,
        }
    }

    /// Configuration for large clusters.
    pub fn large_cluster() -> Self {
        Self {
            max_concurrent_per_peer: 4,
            max_concurrent_global: 1024,
            max_queue_per_peer: 32,
            fair_scheduling: true,
        }
    }

    /// Set max concurrent per peer (builder pattern).
    pub const fn with_max_concurrent_per_peer(mut self, max: usize) -> Self {
        self.max_concurrent_per_peer = max;
        self
    }

    /// Set global max concurrent (builder pattern).
    pub const fn with_max_concurrent_global(mut self, max: usize) -> Self {
        self.max_concurrent_global = max;
        self
    }

    /// Set max queue per peer (builder pattern).
    pub const fn with_max_queue_per_peer(mut self, max: usize) -> Self {
        self.max_queue_per_peer = max;
        self
    }
}

/// Statistics for the pooled transport.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total messages sent successfully.
    pub messages_sent: u64,

    /// Total messages dropped due to queue overflow.
    pub messages_dropped: u64,

    /// Current number of active sends.
    pub active_sends: u64,

    /// Current number of queued sends (waiting).
    pub queued_sends: u64,

    /// Total send errors encountered.
    pub send_errors: u64,

    /// Number of peers with active connections.
    pub active_peers: usize,

    /// Peak concurrent sends observed.
    pub peak_concurrent: u64,
}

/// Per-peer state in the pool.
#[derive(Debug)]
struct PeerState {
    /// Semaphore for limiting concurrent sends to this peer.
    semaphore: Arc<Semaphore>,
    /// Current queue depth for this peer.
    queue_depth: AtomicU64,
    /// Messages sent to this peer.
    messages_sent: AtomicU64,
    /// Messages dropped for this peer.
    messages_dropped: AtomicU64,
}

impl PeerState {
    fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            queue_depth: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
        }
    }
}

/// Error type for pooled transport.
#[derive(Debug)]
pub enum PooledTransportError<E> {
    /// Queue is full, message was dropped.
    QueueFull,
    /// Underlying transport error.
    Transport(E),
}

impl<E: std::fmt::Display> std::fmt::Display for PooledTransportError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PooledTransportError::QueueFull => write!(f, "pool queue full, message dropped"),
            PooledTransportError::Transport(e) => write!(f, "transport error: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PooledTransportError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PooledTransportError::QueueFull => None,
            PooledTransportError::Transport(e) => Some(e),
        }
    }
}

/// Pooled transport wrapper that provides connection management.
#[derive(Debug)]
pub struct PooledTransport<T, I> {
    /// Inner transport.
    inner: T,
    /// Configuration.
    config: PoolConfig,
    /// Per-peer state.
    peers: Mutex<HashMap<I, Arc<PeerState>>>,
    /// Global concurrency semaphore.
    global_semaphore: Arc<Semaphore>,
    /// Statistics.
    stats: PoolStatsInner,
}

/// Internal statistics tracking.
#[derive(Debug, Default)]
struct PoolStatsInner {
    messages_sent: AtomicU64,
    messages_dropped: AtomicU64,
    active_sends: AtomicU64,
    send_errors: AtomicU64,
    peak_concurrent: AtomicU64,
}

impl<T, I> PooledTransport<T, I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    T: Transport<I>,
{
    /// Create a new pooled transport wrapping the given inner transport.
    pub fn new(inner: T, config: PoolConfig) -> Self {
        let global_semaphore = Arc::new(Semaphore::new(config.max_concurrent_global));
        Self {
            inner,
            config,
            peers: Mutex::new(HashMap::new()),
            global_semaphore,
            stats: PoolStatsInner::default(),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(inner: T) -> Self {
        Self::new(inner, PoolConfig::default())
    }

    /// Get or create peer state.
    fn get_peer_state(&self, peer: &I) -> Arc<PeerState> {
        let mut peers = self.peers.lock();
        peers
            .entry(peer.clone())
            .or_insert_with(|| Arc::new(PeerState::new(self.config.max_concurrent_per_peer)))
            .clone()
    }

    /// Get current statistics.
    pub fn stats(&self) -> PoolStats {
        let peers = self.peers.lock();
        let queued: u64 = peers
            .values()
            .map(|p| p.queue_depth.load(Ordering::Relaxed))
            .sum();

        PoolStats {
            messages_sent: self.stats.messages_sent.load(Ordering::Relaxed),
            messages_dropped: self.stats.messages_dropped.load(Ordering::Relaxed),
            active_sends: self.stats.active_sends.load(Ordering::Relaxed),
            queued_sends: queued,
            send_errors: self.stats.send_errors.load(Ordering::Relaxed),
            active_peers: peers.len(),
            peak_concurrent: self.stats.peak_concurrent.load(Ordering::Relaxed),
        }
    }

    /// Reset statistics.
    pub fn reset_stats(&self) {
        self.stats.messages_sent.store(0, Ordering::Relaxed);
        self.stats.messages_dropped.store(0, Ordering::Relaxed);
        self.stats.send_errors.store(0, Ordering::Relaxed);
        self.stats.peak_concurrent.store(0, Ordering::Relaxed);
    }

    /// Remove a peer from the pool.
    ///
    /// Call this when a peer disconnects to clean up resources.
    pub fn remove_peer(&self, peer: &I) {
        let mut peers = self.peers.lock();
        peers.remove(peer);
    }

    /// Clear all peer state.
    pub fn clear(&self) {
        let mut peers = self.peers.lock();
        peers.clear();
    }

    /// Get the configuration.
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Get a reference to the inner transport.
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Send a message to a peer with pooling and concurrency control.
    pub async fn send_to(
        &self,
        target: &I,
        data: Bytes,
    ) -> Result<(), PooledTransportError<T::Error>> {
        let peer_state = self.get_peer_state(target);

        // Check queue depth
        let current_queue = peer_state.queue_depth.fetch_add(1, Ordering::Relaxed);
        if current_queue >= self.config.max_queue_per_peer as u64 {
            peer_state.queue_depth.fetch_sub(1, Ordering::Relaxed);
            peer_state.messages_dropped.fetch_add(1, Ordering::Relaxed);
            self.stats.messages_dropped.fetch_add(1, Ordering::Relaxed);
            return Err(PooledTransportError::QueueFull);
        }

        // Acquire permits (peer + global)
        // In fair scheduling mode, we acquire global first to prevent per-peer starvation
        let (global_permit, peer_permit) = if self.config.fair_scheduling {
            let global = self.global_semaphore.acquire_arc().await;
            let peer = peer_state.semaphore.acquire_arc().await;
            (global, peer)
        } else {
            let peer = peer_state.semaphore.acquire_arc().await;
            let global = self.global_semaphore.acquire_arc().await;
            (global, peer)
        };

        // Update active count and track peak
        let active = self.stats.active_sends.fetch_add(1, Ordering::Relaxed) + 1;
        self.stats
            .peak_concurrent
            .fetch_max(active, Ordering::Relaxed);

        // Decrement queue depth now that we're active
        peer_state.queue_depth.fetch_sub(1, Ordering::Relaxed);

        // Send the message
        let result = self.inner.send_to(target, data).await;

        // Release permits and update stats
        drop(peer_permit);
        drop(global_permit);
        self.stats.active_sends.fetch_sub(1, Ordering::Relaxed);

        match result {
            Ok(()) => {
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                peer_state.messages_sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.send_errors.fetch_add(1, Ordering::Relaxed);
                Err(PooledTransportError::Transport(e))
            }
        }
    }
}

impl<T, I> Clone for PooledTransport<T, I>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            config: self.config.clone(),
            peers: Mutex::new(HashMap::new()),
            global_semaphore: Arc::new(Semaphore::new(self.config.max_concurrent_global)),
            stats: PoolStatsInner::default(),
        }
    }
}

// Implement Transport for PooledTransport so it can be used as a drop-in replacement
impl<T, I> Transport<I> for PooledTransport<T, I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    T: Transport<I>,
{
    type Error = PooledTransportError<T::Error>;

    async fn send_to(&self, target: &I, data: Bytes) -> Result<(), Self::Error> {
        PooledTransport::send_to(self, target, data).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{ChannelTransport, NoopTransport};

    #[test]
    fn test_pool_config_defaults() {
        let config = PoolConfig::default();
        assert_eq!(config.max_concurrent_per_peer, 8);
        assert_eq!(config.max_concurrent_global, 256);
    }

    #[test]
    fn test_pool_config_presets() {
        let high = PoolConfig::high_throughput();
        assert!(high.max_concurrent_per_peer > PoolConfig::default().max_concurrent_per_peer);

        let low = PoolConfig::low_latency();
        assert!(low.max_concurrent_per_peer < PoolConfig::default().max_concurrent_per_peer);
    }

    #[tokio::test]
    async fn test_pooled_transport_basic() {
        let (inner, rx) = ChannelTransport::<u64>::bounded(16);
        let pooled = PooledTransport::with_defaults(inner);

        pooled.send_to(&42u64, Bytes::from("hello")).await.unwrap();

        let (target, data) = rx.recv().await.unwrap();
        assert_eq!(target, 42);
        assert_eq!(data, Bytes::from("hello"));

        let stats = pooled.stats();
        assert_eq!(stats.messages_sent, 1);
        assert_eq!(stats.active_peers, 1);
    }

    #[tokio::test]
    async fn test_pooled_transport_stats() {
        let inner = NoopTransport;
        let pooled = PooledTransport::with_defaults(inner);

        for i in 0..10u64 {
            pooled.send_to(&(i % 3), Bytes::from("test")).await.unwrap();
        }

        let stats = pooled.stats();
        assert_eq!(stats.messages_sent, 10);
        assert_eq!(stats.active_peers, 3); // 0, 1, 2
        assert_eq!(stats.send_errors, 0);
    }

    #[tokio::test]
    async fn test_pooled_transport_queue_full() {
        let inner = NoopTransport;
        let config = PoolConfig::default().with_max_queue_per_peer(0);
        let pooled = PooledTransport::new(inner, config);

        // First send should work (no queue, direct send)
        // But with max_queue_per_peer=0, even the first will fail
        let result = pooled.send_to(&1u64, Bytes::from("test")).await;

        // With 0 queue capacity, should get QueueFull
        assert!(matches!(result, Err(PooledTransportError::QueueFull)));

        let stats = pooled.stats();
        assert_eq!(stats.messages_dropped, 1);
    }

    #[tokio::test]
    async fn test_pooled_transport_remove_peer() {
        let inner = NoopTransport;
        let pooled = PooledTransport::with_defaults(inner);

        pooled.send_to(&1u64, Bytes::from("test")).await.unwrap();
        pooled.send_to(&2u64, Bytes::from("test")).await.unwrap();

        let stats = pooled.stats();
        assert_eq!(stats.active_peers, 2);

        pooled.remove_peer(&1u64);

        let stats = pooled.stats();
        assert_eq!(stats.active_peers, 1);
    }

    #[tokio::test]
    async fn test_pooled_transport_clear() {
        let inner = NoopTransport;
        let pooled = PooledTransport::with_defaults(inner);

        pooled.send_to(&1u64, Bytes::from("test")).await.unwrap();
        pooled.send_to(&2u64, Bytes::from("test")).await.unwrap();
        pooled.send_to(&3u64, Bytes::from("test")).await.unwrap();

        pooled.clear();

        let stats = pooled.stats();
        assert_eq!(stats.active_peers, 0);
    }

    #[tokio::test]
    async fn test_pooled_transport_concurrent() {
        use std::sync::Arc;

        let inner = NoopTransport;
        let config = PoolConfig::default()
            .with_max_concurrent_per_peer(2)
            .with_max_concurrent_global(4);
        let pooled = Arc::new(PooledTransport::new(inner, config));

        // Spawn multiple concurrent sends
        let mut handles = vec![];
        for i in 0..10u64 {
            let p = pooled.clone();
            handles.push(tokio::spawn(async move {
                p.send_to(&(i % 2), Bytes::from("test")).await.unwrap();
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let stats = pooled.stats();
        assert_eq!(stats.messages_sent, 10);
        // Peak should be limited by global max
        assert!(stats.peak_concurrent <= 4);
    }

    #[test]
    fn test_pool_stats_default() {
        let stats = PoolStats::default();
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.messages_dropped, 0);
        assert_eq!(stats.active_sends, 0);
    }

    #[tokio::test]
    async fn test_pooled_transport_reset_stats() {
        let inner = NoopTransport;
        let pooled = PooledTransport::with_defaults(inner);

        pooled.send_to(&1u64, Bytes::from("test")).await.unwrap();

        let stats = pooled.stats();
        assert_eq!(stats.messages_sent, 1);

        pooled.reset_stats();

        let stats = pooled.stats();
        assert_eq!(stats.messages_sent, 0);
    }
}
