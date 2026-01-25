//! Core Plumtree protocol implementation.
//!
//! This module provides the main `Plumtree` struct that combines
//! eager/lazy push mechanisms for efficient O(n) broadcast.

use async_channel::{Receiver, Sender};
use async_lock::RwLock;
use bytes::Bytes;
use futures_timer::Delay;
use smallvec::SmallVec;
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
};
use tracing::{debug, debug_span, info, instrument, trace, warn, Instrument};

use crate::{
    adaptive_batcher::{AdaptiveBatcher, BatcherConfig},
    cleanup_tuner::{CleanupConfig, CleanupTuner},
    config::PlumtreeConfig,
    error::{Error, Result},
    health::{HealthReport, HealthReportBuilder},
    message::{MessageCache, MessageId, PlumtreeMessage},
    peer_scoring::{PeerScoring, ScoringConfig},
    peer_state::SharedPeerState,
    rate_limiter::RateLimiter,
    scheduler::{GraftTimer, IHaveScheduler, PendingIHave},
    PeerStateBuilder,
};

/// Number of shards for the seen map.
/// 16 shards provides good concurrency while keeping memory overhead low.
const SEEN_MAP_SHARDS: usize = 16;

/// Default max capacity per shard for the seen map.
/// With 16 shards, this gives a total capacity of ~160,000 entries.
const DEFAULT_MAX_CAPACITY_PER_SHARD: usize = 10_000;

/// Sharded map for tracking seen messages.
///
/// Uses multiple shards to reduce lock contention:
/// - Each operation only locks one shard (based on message ID hash)
/// - Cleanup iterates through shards one at a time, yielding between shards
/// - Emergency eviction occurs when a shard exceeds its capacity
///
/// This prevents the cleanup task from blocking all message processing
/// and ensures bounded memory usage even under high message velocity.
struct ShardedSeenMap<I> {
    shards: Vec<RwLock<HashMap<MessageId, SeenEntry<I>>>>,
    /// Maximum entries per shard before emergency eviction.
    max_capacity_per_shard: usize,
}

impl<I> ShardedSeenMap<I> {
    /// Create a new sharded seen map with the default number of shards and capacity.
    fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_CAPACITY_PER_SHARD)
    }

    /// Create a new sharded seen map with custom max capacity per shard.
    fn with_capacity(max_capacity_per_shard: usize) -> Self {
        let shards = (0..SEEN_MAP_SHARDS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self {
            shards,
            max_capacity_per_shard,
        }
    }

    /// Get the shard index for a message ID.
    fn shard_index(&self, id: &MessageId) -> usize {
        // Use timestamp and random bytes for distribution
        let hash = id.timestamp() as usize ^ (id.random() as usize);
        hash % self.shards.len()
    }

    /// Get read access to the shard containing the given message ID.
    async fn read_shard(
        &self,
        id: &MessageId,
    ) -> async_lock::RwLockReadGuard<'_, HashMap<MessageId, SeenEntry<I>>> {
        let idx = self.shard_index(id);
        self.shards[idx].read().await
    }

    /// Get write access to the shard containing the given message ID.
    async fn write_shard(
        &self,
        id: &MessageId,
    ) -> async_lock::RwLockWriteGuard<'_, HashMap<MessageId, SeenEntry<I>>> {
        let idx = self.shard_index(id);
        self.shards[idx].write().await
    }
}

impl<I: Clone> ShardedSeenMap<I> {
    /// Check if a message has been seen and get its entry.
    #[allow(dead_code)]
    async fn get(&self, id: &MessageId) -> Option<SeenEntry<I>> {
        let shard = self.read_shard(id).await;
        shard.get(id).cloned()
    }

    /// Check if a message has been seen.
    async fn contains(&self, id: &MessageId) -> bool {
        let shard = self.read_shard(id).await;
        shard.contains_key(id)
    }

    /// Insert a seen entry for a message.
    /// Returns true if this is a new entry, false if it already existed.
    async fn insert(&self, id: MessageId, entry: SeenEntry<I>) -> bool {
        let mut shard = self.write_shard(&id).await;
        if shard.contains_key(&id) {
            false
        } else {
            shard.insert(id, entry);
            true
        }
    }

    /// Update an existing entry or insert a new one.
    #[allow(dead_code)]
    async fn get_or_insert_with<F>(&self, id: MessageId, f: F) -> (SeenEntry<I>, bool)
    where
        F: FnOnce() -> SeenEntry<I>,
    {
        let mut shard = self.write_shard(&id).await;
        if let Some(entry) = shard.get(&id) {
            (entry.clone(), false)
        } else {
            let entry = f();
            shard.insert(id, entry.clone());
            (entry, true)
        }
    }

    /// Increment receive count for an existing entry.
    /// Returns the new count, or None if the entry doesn't exist.
    #[allow(dead_code)]
    async fn increment_receive_count(&self, id: &MessageId) -> Option<u32> {
        let mut shard = self.write_shard(id).await;
        if let Some(entry) = shard.get_mut(id) {
            entry.receive_count += 1;
            Some(entry.receive_count)
        } else {
            None
        }
    }

    /// Check if message is seen; if so, increment count. Otherwise insert new entry.
    /// Returns (is_duplicate, receive_count, evicted_count).
    ///
    /// If the shard exceeds capacity, emergency eviction removes the oldest entries.
    async fn check_and_mark_seen(
        &self,
        id: MessageId,
        entry_fn: impl FnOnce() -> SeenEntry<I>,
    ) -> (bool, u32, usize) {
        let idx = self.shard_index(&id);
        let mut shard = self.shards[idx].write().await;

        if let Some(entry) = shard.get_mut(&id) {
            entry.receive_count += 1;
            (true, entry.receive_count, 0)
        } else {
            // Check if we need emergency eviction before inserting
            let evicted = if shard.len() >= self.max_capacity_per_shard {
                Self::emergency_evict(&mut shard, self.max_capacity_per_shard / 10)
            } else {
                0
            };

            let entry = entry_fn();
            let count = entry.receive_count;
            shard.insert(id, entry);
            (false, count, evicted)
        }
    }

    /// Perform emergency eviction of oldest entries from a shard.
    /// Removes at least `target_evict` entries (more if needed to make room).
    /// Returns the number of entries evicted.
    fn emergency_evict(shard: &mut HashMap<MessageId, SeenEntry<I>>, target_evict: usize) -> usize {
        if shard.is_empty() || target_evict == 0 {
            return 0;
        }

        // Find the oldest entries by seen_at timestamp
        let mut entries: Vec<(MessageId, std::time::Instant)> = shard
            .iter()
            .map(|(id, entry)| (*id, entry.seen_at))
            .collect();

        // Sort by seen_at (oldest first)
        entries.sort_by_key(|(_, seen_at)| *seen_at);

        // Remove the oldest entries
        let to_remove = target_evict.min(entries.len());
        for (id, _) in entries.into_iter().take(to_remove) {
            shard.remove(&id);
        }

        to_remove
    }

    /// Clean up expired entries from one shard.
    /// Returns the number of entries removed.
    async fn cleanup_shard(
        &self,
        shard_idx: usize,
        now: std::time::Instant,
        ttl: std::time::Duration,
    ) -> usize {
        let mut shard = self.shards[shard_idx].write().await;
        let before = shard.len();
        shard.retain(|_, entry| now.duration_since(entry.seen_at) < ttl);
        before.saturating_sub(shard.len())
    }

    /// Get the total number of entries across all shards.
    #[allow(dead_code)]
    async fn len(&self) -> usize {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.read().await.len();
        }
        total
    }

    /// Try to get the approximate total number of entries (non-blocking).
    /// Returns None if any shard lock cannot be acquired immediately.
    fn try_len(&self) -> Option<usize> {
        let mut total = 0;
        for shard in &self.shards {
            total += shard.try_read()?.len();
        }
        Some(total)
    }

    /// Get the number of shards.
    fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Get the maximum capacity per shard.
    fn max_capacity_per_shard(&self) -> usize {
        self.max_capacity_per_shard
    }

    /// Get the total maximum capacity across all shards.
    fn total_capacity(&self) -> usize {
        self.shards.len() * self.max_capacity_per_shard
    }
}

/// Delegate trait for receiving Plumtree events.
///
/// Implement this trait to handle delivered messages and other events.
///
/// # Type Parameters
///
/// - `I`: The peer identifier type (same as used with `Plumtree<I, D>`)
///
/// # Important Notes
///
/// - **Non-blocking**: All methods should return quickly. If your implementation
///   needs to perform heavy computation or blocking I/O (e.g., writing to a database),
///   spawn the work in a separate task to avoid blocking the Plumtree message loop.
/// - **Thread-safe**: Methods may be called concurrently from multiple threads.
///
/// # Example (Non-blocking delivery)
///
/// ```ignore
/// impl PlumtreeDelegate<NodeId> for MyApp {
///     fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
///         let tx = self.delivery_tx.clone();
///         tokio::spawn(async move {
///             // Heavy processing in background
///             tx.send((message_id, payload)).await.ok();
///         });
///     }
///
///     fn on_graft_failed(&self, message_id: &MessageId, peer: &NodeId) {
///         tracing::warn!("Graft failed for {:?} from peer {:?}", message_id, peer);
///     }
/// }
/// ```
pub trait PlumtreeDelegate<I = ()>: Send + Sync + 'static {
    /// Called when a message is delivered (first time received).
    ///
    /// **Warning**: This is called synchronously in the message processing path.
    /// Long-running operations here will block message forwarding and tree repair.
    /// For heavy processing, spawn a background task instead.
    fn on_deliver(&self, message_id: MessageId, payload: Bytes);

    /// Called when a peer is promoted to eager.
    fn on_eager_promotion(&self, _peer: &I) {}

    /// Called when a peer is demoted to lazy.
    fn on_lazy_demotion(&self, _peer: &I) {}

    /// Called when a Graft is sent (tree repair).
    fn on_graft_sent(&self, _peer: &I, _message_id: &MessageId) {}

    /// Called when a Prune is sent (tree optimization).
    fn on_prune_sent(&self, _peer: &I) {}

    /// Called when Graft retries for a message are exhausted (zombie peer detection).
    ///
    /// This indicates that the message could not be retrieved from any peer after
    /// maximum retry attempts. The peer that originally sent the IHave may be dead
    /// or unreachable.
    ///
    /// Implementations may want to:
    /// - Log a warning about the potentially dead peer
    /// - Track failed peers for health monitoring
    /// - Trigger peer removal if failures persist
    fn on_graft_failed(&self, _message_id: &MessageId, _peer: &I) {}
}

// Manual implementations for Arc<T> and Box<T> since auto_impl doesn't handle generic traits well
impl<I, T: PlumtreeDelegate<I> + ?Sized> PlumtreeDelegate<I> for Arc<T> {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        (**self).on_deliver(message_id, payload)
    }
    fn on_eager_promotion(&self, peer: &I) {
        (**self).on_eager_promotion(peer)
    }
    fn on_lazy_demotion(&self, peer: &I) {
        (**self).on_lazy_demotion(peer)
    }
    fn on_graft_sent(&self, peer: &I, message_id: &MessageId) {
        (**self).on_graft_sent(peer, message_id)
    }
    fn on_prune_sent(&self, peer: &I) {
        (**self).on_prune_sent(peer)
    }
    fn on_graft_failed(&self, message_id: &MessageId, peer: &I) {
        (**self).on_graft_failed(message_id, peer)
    }
}

impl<I, T: PlumtreeDelegate<I> + ?Sized> PlumtreeDelegate<I> for Box<T> {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        (**self).on_deliver(message_id, payload)
    }
    fn on_eager_promotion(&self, peer: &I) {
        (**self).on_eager_promotion(peer)
    }
    fn on_lazy_demotion(&self, peer: &I) {
        (**self).on_lazy_demotion(peer)
    }
    fn on_graft_sent(&self, peer: &I, message_id: &MessageId) {
        (**self).on_graft_sent(peer, message_id)
    }
    fn on_prune_sent(&self, peer: &I) {
        (**self).on_prune_sent(peer)
    }
    fn on_graft_failed(&self, message_id: &MessageId, peer: &I) {
        (**self).on_graft_failed(message_id, peer)
    }
}

/// No-op delegate for when no handler is needed.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopDelegate;

impl<I> PlumtreeDelegate<I> for NoopDelegate {
    fn on_deliver(&self, _message_id: MessageId, _payload: Bytes) {}
}

/// Core Plumtree broadcast implementation.
///
/// Combines SWIM-based membership (via memberlist) with Plumtree's
/// epidemic broadcast trees for efficient O(n) message dissemination.
///
/// # Type Parameters
///
/// - `I`: Node identifier type (must be clonable, hashable, and serializable)
/// - `D`: Delegate type for receiving events
pub struct Plumtree<I, D = NoopDelegate> {
    /// Inner state.
    inner: Arc<PlumtreeInner<I, D>>,
}

struct PlumtreeInner<I, D> {
    /// Peer state (eager and lazy sets).
    peers: SharedPeerState<I>,

    /// Message cache for Graft requests.
    cache: Arc<MessageCache>,

    /// IHave scheduler.
    scheduler: Arc<IHaveScheduler>,

    /// Graft timer for tracking pending messages.
    graft_timer: Arc<GraftTimer<I>>,

    /// Rate limiter for Graft requests (per-peer).
    graft_rate_limiter: RateLimiter<I>,

    /// Peer scoring for RTT-based optimization and zombie peer detection.
    peer_scoring: Arc<PeerScoring<I>>,

    /// Dynamic cleanup tuning based on system load.
    cleanup_tuner: Arc<CleanupTuner>,

    /// Adaptive IHave batching based on network conditions.
    adaptive_batcher: Arc<AdaptiveBatcher>,

    /// Event delegate.
    delegate: D,

    /// Configuration.
    config: PlumtreeConfig,

    /// Local node ID.
    local_id: I,

    /// Current broadcast round.
    round: AtomicU32,

    /// Shutdown flag.
    shutdown: AtomicBool,

    /// Channel for outgoing messages.
    outgoing_tx: Sender<OutgoingMessage<I>>,

    /// Channel for incoming messages.
    incoming_tx: Sender<IncomingMessage<I>>,

    /// Track messages seen for deduplication and parent tracking.
    ///
    /// Uses sharded locking to reduce contention:
    /// - Each message operation only locks one shard (based on message ID hash)
    /// - Cleanup iterates through shards one at a time, yielding between shards
    /// - This prevents the cleanup task from blocking all message processing
    seen: ShardedSeenMap<I>,
}

/// Entry for tracking seen messages and their delivery parent.
#[derive(Debug, Clone)]
struct SeenEntry<I> {
    /// Round when first seen.
    /// Reserved for future use in protocol diagnostics.
    #[allow(dead_code)]
    round: u32,
    /// Number of times received.
    receive_count: u32,
    /// When this entry was first seen (for TTL-based cleanup).
    seen_at: std::time::Instant,
    /// Parent peer who delivered this message first.
    /// Reserved for future use in tree repair when a better path is found.
    #[allow(dead_code)]
    parent: Option<I>,
}

/// Statistics about the seen map (deduplication map).
#[derive(Debug, Clone, Copy)]
pub struct SeenMapStats {
    /// Current number of entries in the map.
    pub size: usize,
    /// Total maximum capacity across all shards.
    pub capacity: usize,
    /// Current utilization (size / capacity).
    pub utilization: f64,
    /// Number of shards.
    pub shard_count: usize,
    /// Maximum entries per shard.
    pub max_per_shard: usize,
}

/// Outgoing message to be sent.
#[derive(Debug)]
pub struct OutgoingMessage<I> {
    /// Target peer for unicast messages, None for broadcast.
    pub target: Option<I>,
    /// Message to send.
    pub message: PlumtreeMessage,
}

impl<I> OutgoingMessage<I> {
    /// Create a unicast message to a specific peer.
    pub fn unicast(target: I, message: PlumtreeMessage) -> Self {
        Self {
            target: Some(target),
            message,
        }
    }

    /// Create a broadcast message (for initial broadcast only).
    pub fn broadcast(message: PlumtreeMessage) -> Self {
        Self {
            target: None,
            message,
        }
    }

    /// Check if this is a unicast message.
    pub fn is_unicast(&self) -> bool {
        self.target.is_some()
    }

    /// Check if this is a broadcast message.
    pub fn is_broadcast(&self) -> bool {
        self.target.is_none()
    }
}

/// Incoming message received from a peer.
#[derive(Debug)]
pub struct IncomingMessage<I> {
    /// Source peer.
    pub from: I,
    /// Received message.
    pub message: PlumtreeMessage,
}

impl<I, D> Plumtree<I, D>
where
    I: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    /// Create a new Plumtree instance.
    ///
    /// # Arguments
    ///
    /// - `local_id`: This node's identifier
    /// - `config`: Plumtree configuration
    /// - `delegate`: Event handler
    pub fn new(local_id: I, config: PlumtreeConfig, delegate: D) -> (Self, PlumtreeHandle<I>) {
        // Initialize metrics descriptions (safe to call multiple times)
        #[cfg(feature = "metrics")]
        crate::metrics::init_metrics();

        let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);
        let (incoming_tx, incoming_rx) = async_channel::bounded(1024);

        // Rate limiter: allow 10 Graft requests per peer per second, burst of 20
        let graft_rate_limiter = RateLimiter::new(
            config.graft_rate_limit_burst,
            config.graft_rate_limit_per_second,
        );

        // Create peer state with or without hash ring topology
        // Always set local_id for diverse peer selection even without hash ring
        let mut peer_builder = PeerStateBuilder::new()
            .with_local_id(local_id.clone())
            .use_hash_ring(config.use_hash_ring)
            .with_protect_ring_neighbors(config.protect_ring_neighbors)
            .with_max_protected_neighbors(config.max_protected_neighbors);

        // Apply hard caps if configured
        if let Some(max) = config.max_eager_peers {
            peer_builder = peer_builder.with_max_eager_peers(max);
        }
        if let Some(max) = config.max_lazy_peers {
            peer_builder = peer_builder.with_max_lazy_peers(max);
        }

        let peers = Arc::new(peer_builder.build());

        let inner = Arc::new(PlumtreeInner {
            peers,
            cache: Arc::new(MessageCache::new(
                config.message_cache_ttl,
                config.message_cache_max_size,
            )),
            scheduler: Arc::new(IHaveScheduler::new(
                config.ihave_interval,
                config.ihave_batch_size,
                10000,
            )),
            graft_timer: Arc::new(GraftTimer::new(config.graft_timeout)),
            graft_rate_limiter,
            peer_scoring: Arc::new(PeerScoring::new(ScoringConfig::default())),
            cleanup_tuner: Arc::new(CleanupTuner::new(CleanupConfig::default())),
            adaptive_batcher: Arc::new(AdaptiveBatcher::new(BatcherConfig::default())),
            delegate,
            config,
            local_id,
            round: AtomicU32::new(0),
            shutdown: AtomicBool::new(false),
            outgoing_tx,
            incoming_tx,
            seen: ShardedSeenMap::new(),
        });

        let plumtree = Self {
            inner: inner.clone(),
        };

        let handle = PlumtreeHandle {
            outgoing_rx,
            incoming_rx,
            incoming_tx: plumtree.inner.incoming_tx.clone(),
        };

        (plumtree, handle)
    }

    /// Get the local node ID.
    pub fn local_id(&self) -> &I {
        &self.inner.local_id
    }

    /// Get the configuration.
    pub fn config(&self) -> &PlumtreeConfig {
        &self.inner.config
    }

    /// Get peer statistics.
    pub fn peer_stats(&self) -> crate::peer_state::PeerStats {
        self.inner.peers.stats()
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> crate::message::CacheStats {
        self.inner.cache.stats()
    }

    /// Get seen map statistics (deduplication map).
    ///
    /// Returns None if the statistics cannot be computed without blocking.
    pub fn seen_map_stats(&self) -> Option<SeenMapStats> {
        let size = self.inner.seen.try_len()?;
        let capacity = self.inner.seen.total_capacity();
        let utilization = if capacity > 0 {
            size as f64 / capacity as f64
        } else {
            0.0
        };
        Some(SeenMapStats {
            size,
            capacity,
            utilization,
            shard_count: self.inner.seen.shard_count(),
            max_per_shard: self.inner.seen.max_capacity_per_shard(),
        })
    }

    /// Get peer scoring statistics.
    pub fn scoring_stats(&self) -> crate::peer_scoring::ScoringStats {
        self.inner.peer_scoring.stats()
    }

    /// Get cleanup tuner statistics.
    pub fn cleanup_stats(&self) -> crate::cleanup_tuner::CleanupStats {
        self.inner.cleanup_tuner.stats()
    }

    /// Get adaptive batcher statistics.
    pub fn batcher_stats(&self) -> crate::adaptive_batcher::BatcherStats {
        self.inner.adaptive_batcher.stats()
    }

    /// Record an RTT sample for a peer.
    ///
    /// Call this when receiving a response from a peer to update their score.
    /// Lower RTT results in higher peer scores.
    pub fn record_peer_rtt(&self, peer: &I, rtt: std::time::Duration) {
        self.inner.peer_scoring.record_rtt(peer, rtt);
    }

    /// Record a failure for a peer (e.g., timeout, connection error).
    ///
    /// This penalizes the peer's score, making them less likely to be
    /// selected for eager promotion.
    pub fn record_peer_failure(&self, peer: &I) {
        self.inner.peer_scoring.record_failure(peer);
    }

    /// Get the score for a specific peer.
    pub fn get_peer_score(&self, peer: &I) -> Option<crate::peer_scoring::PeerScore> {
        self.inner.peer_scoring.get_score(peer)
    }

    /// Get the best peers by score for potential eager promotion.
    pub fn best_peers_for_eager(&self, count: usize) -> Vec<I> {
        self.inner.peer_scoring.best_peers(count)
    }

    /// Get a health report for the protocol.
    ///
    /// The health report provides information about peer connectivity,
    /// message delivery status, and cache utilization.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let health = plumtree.health();
    /// if health.status.is_unhealthy() {
    ///     eprintln!("Warning: {}", health.message);
    /// }
    /// ```
    pub fn health(&self) -> HealthReport {
        let peer_stats = self.inner.peers.stats();
        let cache_stats = self.inner.cache.stats();
        let pending_grafts = self.inner.graft_timer.pending_count();

        // For now, we don't track successful/failed grafts in the struct
        // This would need additional atomic counters to track properly
        // For the initial implementation, we'll use 0/0 which results in 0% failure rate
        let successful_grafts = 0_u64;
        let failed_grafts = 0_u64;

        let total_peers = peer_stats.eager_count + peer_stats.lazy_count;

        HealthReportBuilder::new()
            .peers(
                total_peers,
                peer_stats.eager_count,
                peer_stats.lazy_count,
                self.inner.config.eager_fanout,
            )
            .grafts(pending_grafts, successful_grafts, failed_grafts)
            .cache(
                cache_stats.entries,
                self.inner.config.message_cache_max_size,
                self.inner.config.message_cache_ttl,
            )
            .shutdown(self.inner.shutdown.load(Ordering::Acquire))
            .build()
    }

    /// Add a peer with automatic classification.
    ///
    /// The peer is automatically classified as eager or lazy based on
    /// the current state and configuration:
    /// - If `max_peers` is set and the limit is reached, the peer is rejected
    /// - If the eager set is below `eager_fanout`, the peer joins as eager
    /// - Otherwise, the peer joins as lazy
    ///
    /// Returns the result of the add operation.
    pub fn add_peer(&self, peer: I) -> crate::peer_state::AddPeerResult {
        use crate::peer_state::AddPeerResult;

        if peer == self.inner.local_id {
            return AddPeerResult::AlreadyExists;
        }

        let result = self.inner.peers.add_peer_auto(
            peer,
            self.inner.config.max_peers,
            self.inner.config.eager_fanout,
        );

        // Record metric for successful peer additions
        #[cfg(feature = "metrics")]
        match &result {
            AddPeerResult::AddedEager | AddPeerResult::AddedLazy => {
                crate::metrics::record_peer_added();
            }
            _ => {}
        }

        result
    }

    /// Add a peer to the lazy set only (traditional behavior).
    ///
    /// New peers always start as lazy. They are promoted to eager
    /// via the Graft mechanism if needed.
    ///
    /// This bypasses the `max_peers` limit check and auto-classification.
    pub fn add_peer_lazy(&self, peer: I) {
        if peer != self.inner.local_id {
            self.inner.peers.add_peer(peer.clone());

            #[cfg(feature = "metrics")]
            crate::metrics::record_peer_added();
        }
    }

    /// Remove a peer.
    ///
    /// Removes the peer from both the topology state (eager/lazy sets) and
    /// the scoring state (RTT, failure counts) to prevent memory leaks.
    pub fn remove_peer(&self, peer: &I) {
        self.inner
            .peers
            .remove_peer_auto(peer, self.inner.config.eager_fanout);
        // Clean up scoring data to free memory for departed peers
        self.inner.peer_scoring.remove_peer(peer);

        #[cfg(feature = "metrics")]
        crate::metrics::record_peer_removed();
    }

    /// Broadcast a message to all nodes.
    ///
    /// The message is sent immediately to eager peers and queued
    /// as IHave announcements for lazy peers.
    ///
    /// Returns the unique message ID assigned to this broadcast.
    #[instrument(skip(self, payload), fields(payload_size))]
    pub async fn broadcast(&self, payload: impl Into<Bytes>) -> Result<MessageId> {
        let payload = payload.into();
        let payload_size = payload.len();
        tracing::Span::current().record("payload_size", payload_size);

        // Check size limit
        if payload_size > self.inner.config.max_message_size {
            warn!(
                payload_size,
                max_size = self.inner.config.max_message_size,
                "message too large"
            );
            return Err(Error::MessageTooLarge {
                size: payload_size,
                max_size: self.inner.config.max_message_size,
            });
        }

        let msg_id = MessageId::new();
        let round = self.inner.round.fetch_add(1, Ordering::Relaxed);

        debug!(
            message_id = %msg_id,
            round,
            payload_size,
            "broadcasting new message"
        );

        // Cache the message
        self.inner.cache.insert(msg_id, payload.clone());

        // Mark as seen (no parent since we originated this message)
        self.inner
            .seen
            .insert(
                msg_id,
                SeenEntry {
                    round,
                    receive_count: 1,
                    seen_at: std::time::Instant::now(),
                    parent: None, // We originated this message
                },
            )
            .await;

        // Send to eager peers with backpressure handling
        let eager_peers = self.inner.peers.eager_peers();
        let eager_count = eager_peers.len();
        let mut dropped_count = 0;

        for peer in eager_peers {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round,
                payload: payload.clone(),
            };
            // Use try_send to avoid blocking under backpressure
            if let Err(_e) = self
                .inner
                .outgoing_tx
                .try_send(OutgoingMessage::unicast(peer, msg))
            {
                dropped_count += 1;
                trace!(
                    message_id = %msg_id,
                    "outgoing queue full, message to eager peer dropped"
                );
            }
        }

        // Queue IHave for lazy peers
        self.inner.scheduler.queue().push(msg_id, round);

        // Check for backpressure condition
        if dropped_count > 0 {
            warn!(
                message_id = %msg_id,
                dropped = dropped_count,
                total_eager = eager_count,
                "backpressure: some eager push messages were dropped"
            );
            // Still return the message ID since it was cached and IHave was queued.
            // The message can still reach peers via lazy push (IHave/Graft).
            // But warn the caller about backpressure so they can throttle.
            if dropped_count == eager_count {
                // All messages dropped - this is severe backpressure
                return Err(Error::QueueFull {
                    dropped: dropped_count,
                    capacity: self.inner.outgoing_tx.capacity().unwrap_or(1024),
                });
            }
        }

        trace!(
            message_id = %msg_id,
            eager_peers = eager_count,
            sent = eager_count - dropped_count,
            "broadcast complete, IHave queued for lazy peers"
        );

        // Record metrics
        #[cfg(feature = "metrics")]
        {
            crate::metrics::record_broadcast();
            crate::metrics::record_message_size(payload_size);
            // Record gossip sent for each eager peer we sent to
            for _ in 0..(eager_count - dropped_count) {
                crate::metrics::record_gossip_sent();
            }
        }

        Ok(msg_id)
    }

    /// Handle an incoming Plumtree message.
    ///
    /// This should be called when a message is received from the network.
    pub async fn handle_message(&self, from: I, message: PlumtreeMessage) -> Result<()> {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(Error::Shutdown);
        }

        let msg_type = message.type_name();
        let span = debug_span!("handle_message", msg_type, ?from);

        async {
            match message {
                PlumtreeMessage::Gossip { id, round, payload } => {
                    self.handle_gossip(from, id, round, payload).await
                }
                PlumtreeMessage::IHave { message_ids, round } => {
                    self.handle_ihave(from, message_ids, round).await
                }
                PlumtreeMessage::Graft { message_id, round } => {
                    self.handle_graft(from, message_id, round).await
                }
                PlumtreeMessage::Prune => self.handle_prune(from).await,
                PlumtreeMessage::Sync(sync_msg) => {
                    // Sync message handling is not implemented in this module.
                    // Use the sync module for anti-entropy synchronization.
                    trace!("received sync message: {:?}", sync_msg.type_name());
                    Ok(())
                }
            }
        }
        .instrument(span)
        .await
    }

    /// Handle a Gossip message (eager push).
    async fn handle_gossip(
        &self,
        from: I,
        msg_id: MessageId,
        round: u32,
        payload: Bytes,
    ) -> Result<()> {
        let payload_size = payload.len();
        trace!(
            message_id = %msg_id,
            round,
            payload_size,
            "received gossip"
        );

        // Record message for cleanup tuner's message rate tracking
        self.inner.cleanup_tuner.record_message();
        // Record message for adaptive batcher's throughput tracking
        self.inner.adaptive_batcher.record_message();

        // Cancel any pending Graft timer for this message
        // Returns true if a graft was actually sent and satisfied
        if self.inner.graft_timer.message_received(&msg_id) {
            // Record graft success for adaptive batcher to adjust batch sizes
            self.inner.adaptive_batcher.record_graft_received();

            #[cfg(feature = "metrics")]
            crate::metrics::record_graft_success();
        }

        // Check if already seen and track parent atomically (single shard lock)
        let (is_duplicate, receive_count, evicted) = self
            .inner
            .seen
            .check_and_mark_seen(msg_id, || SeenEntry {
                round,
                receive_count: 1,
                seen_at: std::time::Instant::now(),
                parent: Some(from.clone()), // Track who delivered this message
            })
            .await;

        if evicted > 0 {
            debug!(
                message_id = %msg_id,
                evicted,
                "emergency eviction in seen map due to capacity"
            );
            #[cfg(feature = "metrics")]
            crate::metrics::record_seen_map_evictions(evicted);
        }

        if is_duplicate {
            trace!(
                message_id = %msg_id,
                receive_count,
                "duplicate gossip received"
            );

            #[cfg(feature = "metrics")]
            crate::metrics::record_duplicate();

            // Optimization: prune redundant path after threshold
            if receive_count > self.inner.config.optimization_threshold {
                // Only send Prune if the peer is in the eager set
                // Pruning a peer that is already lazy is redundant traffic
                if self.inner.peers.is_eager(&from) {
                    debug!(
                        message_id = %msg_id,
                        receive_count,
                        threshold = self.inner.config.optimization_threshold,
                        "pruning redundant path"
                    );
                    // Send Prune unicast to the duplicate sender
                    let _ = self
                        .inner
                        .outgoing_tx
                        .send(OutgoingMessage::unicast(
                            from.clone(),
                            PlumtreeMessage::Prune,
                        ))
                        .await;
                    self.inner.delegate.on_prune_sent(&from);

                    #[cfg(feature = "metrics")]
                    crate::metrics::record_prune_sent();

                    // Demote to lazy
                    if self.inner.peers.demote_to_lazy(&from) {
                        self.inner.delegate.on_lazy_demotion(&from);

                        #[cfg(feature = "metrics")]
                        crate::metrics::record_peer_demotion();
                    }
                }
            }
            return Ok(());
        }

        // First time seeing this message (parent already tracked above)
        debug!(
            message_id = %msg_id,
            round,
            payload_size,
            "delivering new message"
        );

        // Cache for potential Graft requests
        self.inner.cache.insert(msg_id, payload.clone());

        // Deliver to application
        self.inner.delegate.on_deliver(msg_id, payload.clone());

        // Record delivery metrics
        #[cfg(feature = "metrics")]
        {
            crate::metrics::record_delivery();
            crate::metrics::record_message_size(payload_size);
            crate::metrics::record_message_hops(round);
        }

        // Forward to eager peers (except sender) via unicast
        let eager_peers = self.inner.peers.random_eager_except(&from, usize::MAX);
        let forward_count = eager_peers.len();
        let mut dropped = 0;

        for peer in eager_peers {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round: round + 1,
                payload: payload.clone(),
            };
            // Use try_send to avoid blocking under backpressure
            if self
                .inner
                .outgoing_tx
                .try_send(OutgoingMessage::unicast(peer, msg))
                .is_err()
            {
                dropped += 1;
            }
        }

        // Queue IHave for lazy peers (except sender)
        self.inner.scheduler.queue().push(msg_id, round + 1);

        if dropped > 0 {
            debug!(
                message_id = %msg_id,
                dropped,
                total = forward_count,
                "backpressure: some gossip forwards dropped"
            );
        }

        trace!(
            message_id = %msg_id,
            forward_count,
            sent = forward_count - dropped,
            "forwarded gossip to eager peers"
        );

        // Record gossip forwarding metrics
        #[cfg(feature = "metrics")]
        {
            let sent_count = forward_count - dropped;
            for _ in 0..sent_count {
                crate::metrics::record_gossip_sent();
            }
        }

        Ok(())
    }

    /// Handle an IHave message (lazy push announcement).
    async fn handle_ihave(
        &self,
        from: I,
        message_ids: SmallVec<[MessageId; 8]>,
        round: u32,
    ) -> Result<()> {
        let count = message_ids.len();
        trace!(count, round, "received IHave batch");

        let mut missing_count = 0;
        for msg_id in message_ids {
            // Check if we already have this message (only locks one shard)
            let have_message = self.inner.seen.contains(&msg_id).await;

            if !have_message {
                missing_count += 1;
                trace!(message_id = %msg_id, "missing message, sending Graft");

                // We don't have this message - start Graft timer
                // Get alternative peers to try if the primary fails
                let alternatives: Vec<I> = self.inner.peers.random_eager_except(&from, 2);

                self.inner.graft_timer.expect_message_with_alternatives(
                    msg_id,
                    from.clone(),
                    alternatives,
                    round,
                );

                // Note: We do NOT demote any existing parent here. This avoids a race condition
                // where we might demote a peer that is about to (or just did) deliver the message.
                //
                // Example race without this fix:
                // 1. IHave(msg) arrives from C, we check seen -> not seen
                // 2. Meanwhile Gossip(msg) from B arrives and sets message_parents[msg] = B
                // 3. We demote B even though B just delivered successfully!
                //
                // Instead, tree optimization happens naturally via handle_gossip:
                // - If we receive duplicate Gossip, we send Prune to the redundant sender
                // - This is the correct place to demote, as it's based on actual delivery

                // Promote sender to eager to get this and future messages
                if self.inner.peers.promote_to_eager(&from) {
                    debug!("promoted peer to eager after IHave");
                    self.inner.delegate.on_eager_promotion(&from);

                    #[cfg(feature = "metrics")]
                    crate::metrics::record_peer_promotion();
                }

                // Send Graft to get the missing message
                let _ = self
                    .inner
                    .outgoing_tx
                    .send(OutgoingMessage::unicast(
                        from.clone(),
                        PlumtreeMessage::Graft {
                            message_id: msg_id,
                            round,
                        },
                    ))
                    .await;

                self.inner.delegate.on_graft_sent(&from, &msg_id);

                #[cfg(feature = "metrics")]
                crate::metrics::record_graft_sent();
            }
        }

        if missing_count > 0 {
            debug!(
                total = count,
                missing = missing_count,
                "IHave processing complete"
            );
        }

        Ok(())
    }

    /// Handle a Graft message (request to establish eager link).
    async fn handle_graft(&self, from: I, msg_id: MessageId, round: u32) -> Result<()> {
        trace!(message_id = %msg_id, round, "received Graft request");

        // Rate limit Graft requests per peer
        if !self.inner.graft_rate_limiter.check(&from) {
            warn!(message_id = %msg_id, "rate limiting Graft request");
            return Ok(());
        }

        // Promote requester to eager (may be rejected if at max_eager_peers limit)
        if self.inner.peers.promote_to_eager(&from) {
            debug!("promoted peer to eager after Graft request");
            self.inner.delegate.on_eager_promotion(&from);
        } else if !self.inner.peers.is_eager(&from) {
            // Promotion was rejected (likely due to max_eager_peers limit)
            debug!("could not promote peer to eager (at capacity or unknown peer)");
        }

        // Send the requested message unicast to the requester
        if let Some(payload) = self.inner.cache.get(&msg_id) {
            debug!(
                message_id = %msg_id,
                payload_size = payload.len(),
                "responding to Graft with cached message"
            );
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round,
                payload: (*payload).clone(),
            };
            let _ = self
                .inner
                .outgoing_tx
                .send(OutgoingMessage::unicast(from, msg))
                .await;
        } else {
            debug!(message_id = %msg_id, "Graft request for unknown message");
        }

        Ok(())
    }

    /// Handle a Prune message (demote to lazy).
    async fn handle_prune(&self, from: I) -> Result<()> {
        trace!("received Prune request");
        if self.inner.peers.demote_to_lazy(&from) {
            debug!("demoted peer to lazy after Prune");
            self.inner.delegate.on_lazy_demotion(&from);
        }
        Ok(())
    }

    /// Run the IHave scheduler background task.
    ///
    /// This should be spawned as a background task.
    /// Uses a "linger" strategy: flushes immediately if batch is full,
    /// otherwise waits for the configured interval.
    ///
    /// Uses the AdaptiveBatcher for dynamic batch size adjustment based on:
    /// - Network latency (smaller batches for low-latency)
    /// - Graft success rate (reduce batches if many Grafts fail)
    /// - Message throughput (larger batches under high load)
    #[instrument(skip(self), name = "ihave_scheduler")]
    pub async fn run_ihave_scheduler(&self) {
        info!("IHave scheduler started with adaptive batching");
        let ihave_interval = self.inner.config.ihave_interval;
        // Check for early flush more frequently (every 10ms)
        let check_interval = std::time::Duration::from_millis(10);
        let mut last_flush = std::time::Instant::now();
        let mut last_batch_size_update = std::time::Instant::now();
        let batch_size_update_interval = std::time::Duration::from_secs(5);

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                info!("IHave scheduler shutting down");
                break;
            }

            // Periodically update batch size from adaptive batcher
            if last_batch_size_update.elapsed() >= batch_size_update_interval {
                let recommended_size = self.inner.adaptive_batcher.recommended_batch_size();
                self.inner
                    .scheduler
                    .queue()
                    .set_flush_threshold(recommended_size);
                trace!(
                    batch_size = recommended_size,
                    "updated IHave batch size from adaptive batcher"
                );
                last_batch_size_update = std::time::Instant::now();
            }

            // Check if we should flush early (queue reached batch size)
            let should_flush = self.inner.scheduler.queue().should_flush();
            let interval_elapsed = last_flush.elapsed() >= ihave_interval;

            if should_flush || interval_elapsed {
                // Flush: either queue is full or interval elapsed
                self.flush_ihave_batch().await;
                last_flush = std::time::Instant::now();
            } else {
                // Wait for a short check period before checking again
                Delay::new(check_interval).await;
            }
        }
    }

    /// Flush pending IHave messages to lazy peers.
    async fn flush_ihave_batch(&self) {
        // Get batch of IHaves to send
        let batch: SmallVec<[PendingIHave; 16]> = self.inner.scheduler.pop_batch();

        if batch.is_empty() {
            return;
        }

        // Collect message IDs
        let message_ids: SmallVec<[MessageId; 8]> = batch.iter().map(|p| p.message_id).collect();
        let round = batch.iter().map(|p| p.round).max().unwrap_or(0);

        // Get lazy peers to send to
        let lazy_peers = self
            .inner
            .peers
            .random_lazy_except(&self.inner.local_id, self.inner.config.lazy_fanout);

        let peer_count = lazy_peers.len();
        let batch_size = message_ids.len();

        trace!(
            batch_size,
            peer_count,
            round,
            "flushing IHave batch to lazy peers"
        );

        // Send IHave to each lazy peer with backpressure handling
        let mut ihave_dropped = 0;
        for peer in lazy_peers {
            let msg = PlumtreeMessage::IHave {
                message_ids: message_ids.clone(),
                round,
            };
            // Use try_send to avoid blocking under backpressure
            if self
                .inner
                .outgoing_tx
                .try_send(OutgoingMessage::unicast(peer, msg))
                .is_err()
            {
                ihave_dropped += 1;
            }
        }

        if ihave_dropped > 0 {
            debug!(
                batch_size,
                dropped = ihave_dropped,
                total = peer_count,
                "backpressure: some IHave messages dropped"
            );
        }

        // Record IHaves sent for adaptive batcher feedback
        let ihaves_sent = (peer_count - ihave_dropped) * batch_size;
        self.inner.adaptive_batcher.record_ihave_sent(ihaves_sent);

        // Record metrics
        #[cfg(feature = "metrics")]
        {
            crate::metrics::record_ihave_batch_size(batch_size);
            let sent_count = peer_count - ihave_dropped;
            for _ in 0..sent_count {
                crate::metrics::record_ihave_sent();
            }
        }
    }

    /// Run the Graft timer checker background task.
    ///
    /// This should be spawned as a background task.
    #[instrument(skip(self), name = "graft_timer")]
    pub async fn run_graft_timer(&self) {
        info!("Graft timer started");
        let check_interval = self.inner.config.graft_timeout / 2;
        let mut interval = Delay::new(check_interval);

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                info!("Graft timer shutting down");
                break;
            }

            // Wait for interval
            (&mut interval).await;
            interval.reset(check_interval);

            // Check for expired Graft timers and failures
            let (expired, failed) = self.inner.graft_timer.get_expired_with_failures();

            // Handle failed grafts (zombie peer detection)
            for failed_graft in &failed {
                warn!(
                    message_id = %failed_graft.message_id,
                    retries = failed_graft.total_retries,
                    peer = ?failed_graft.original_peer,
                    "Graft failed after max retries - penalizing peer"
                );
                // Record failure in peer scoring to penalize unresponsive peers
                self.inner
                    .peer_scoring
                    .record_failure(&failed_graft.original_peer);
                // Record failure for adaptive batcher to adjust batch sizes
                self.inner.adaptive_batcher.record_graft_timeout();
                self.inner
                    .delegate
                    .on_graft_failed(&failed_graft.message_id, &failed_graft.original_peer);

                #[cfg(feature = "metrics")]
                crate::metrics::record_graft_failed();
            }

            for expired_graft in expired {
                // Send Graft to the peer determined by the timer (primary or alternative)
                debug!(
                    message_id = %expired_graft.message_id,
                    attempt = expired_graft.retry_count,
                    is_retry = expired_graft.retry_count > 0,
                    "sending Graft request"
                );

                // Record retry metric if this is a retry attempt
                #[cfg(feature = "metrics")]
                if expired_graft.retry_count > 0 {
                    crate::metrics::record_graft_retry();
                }

                let _ = self
                    .inner
                    .outgoing_tx
                    .send(OutgoingMessage::unicast(
                        expired_graft.peer.clone(),
                        PlumtreeMessage::Graft {
                            message_id: expired_graft.message_id,
                            round: expired_graft.round,
                        },
                    ))
                    .await;

                self.inner
                    .delegate
                    .on_graft_sent(&expired_graft.peer, &expired_graft.message_id);
            }
        }
    }

    /// Run the seen map cleanup background task.
    ///
    /// This removes old entries from the deduplication map to prevent
    /// unbounded memory growth. Entries older than message_cache_ttl are removed.
    ///
    /// Uses the CleanupTuner for dynamic cleanup intervals based on:
    /// - Cache utilization (more aggressive when near capacity)
    /// - Message rate (more conservative under high load)
    /// - Cleanup duration (adjusts batch size for responsiveness)
    ///
    /// This should be spawned as a background task.
    #[instrument(skip(self), name = "seen_cleanup")]
    pub async fn run_seen_cleanup(&self) {
        info!("seen map cleanup started with dynamic tuning");
        let ttl = self.inner.config.message_cache_ttl;
        let tuner = &self.inner.cleanup_tuner;

        // Start with initial interval from tuner
        let initial_params = tuner.get_parameters(0.0, ttl);
        let mut interval = Delay::new(initial_params.interval);
        let mut rate_window_reset = std::time::Instant::now();

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                info!("seen map cleanup shutting down");
                break;
            }

            // Wait for dynamically-tuned interval
            (&mut interval).await;

            // Get current utilization for tuning
            let utilization = self.seen_map_stats().map(|s| s.utilization).unwrap_or(0.0);

            // Get tuned parameters based on current state
            let params = tuner.get_parameters(utilization, ttl);

            trace!(
                interval_ms = params.interval.as_millis(),
                batch_size = params.batch_size,
                aggressive = params.aggressive,
                utilization = format!("{:.2}", params.utilization),
                message_rate = format!("{:.1}", params.message_rate),
                "cleanup tuner parameters"
            );

            // Reset for next interval using tuned duration
            interval.reset(params.interval);

            // Clean up expired entries from seen map, one shard at a time
            let cleanup_start = std::time::Instant::now();
            let now = cleanup_start;
            let mut total_removed = 0;
            let shard_count = self.inner.seen.shard_count();

            for shard_idx in 0..shard_count {
                // Check for shutdown between shards
                if self.inner.shutdown.load(Ordering::Acquire) {
                    break;
                }

                // Clean up one shard (only locks that shard)
                let removed = self.inner.seen.cleanup_shard(shard_idx, now, ttl).await;
                total_removed += removed;

                // Brief yield to allow other tasks to make progress
                Delay::new(std::time::Duration::from_micros(1)).await;
            }

            // Record cleanup metrics for tuner feedback
            let cleanup_duration = cleanup_start.elapsed();
            tuner.record_cleanup(cleanup_duration, total_removed, &params);

            if total_removed > 0 {
                debug!(
                    removed = total_removed,
                    shards = shard_count,
                    duration_ms = cleanup_duration.as_millis(),
                    aggressive = params.aggressive,
                    "cleaned up expired entries from seen map"
                );
            }

            // Periodically reset the rate window for responsive tuning
            if rate_window_reset.elapsed() >= tuner.config().rate_window {
                tuner.reset_rate_window();
                rate_window_reset = std::time::Instant::now();
            }

            // Update seen map size metric after cleanup
            #[cfg(feature = "metrics")]
            if let Some(stats) = self.seen_map_stats() {
                crate::metrics::set_seen_map_size(stats.size);
            }
        }
    }

    /// Run the periodic topology maintenance loop.
    ///
    /// This background task periodically checks if the eager peer count has
    /// dropped below `eager_fanout` and promotes lazy peers to restore the
    /// spanning tree. This is critical for automatic recovery from node failures.
    ///
    /// The loop includes random jitter to prevent "thundering herd" effects
    /// when multiple nodes detect topology degradation simultaneously.
    ///
    /// This should be spawned as a background task.
    #[instrument(skip(self), name = "maintenance_loop")]
    pub async fn run_maintenance_loop(&self)
    where
        I: Clone + Eq + std::hash::Hash + std::fmt::Debug,
    {
        use rand::Rng;

        let interval = self.inner.config.maintenance_interval;
        let jitter = self.inner.config.maintenance_jitter;

        // Skip if maintenance is disabled
        if interval.is_zero() {
            info!("maintenance loop disabled (interval=0)");
            return;
        }

        info!(
            interval_ms = interval.as_millis(),
            jitter_ms = jitter.as_millis(),
            "topology maintenance loop started"
        );

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                info!("maintenance loop shutting down");
                break;
            }

            // Add random jitter to prevent thundering herd
            let jitter_duration = if !jitter.is_zero() {
                let jitter_ms = rand::rng().random_range(0..jitter.as_millis() as u64);
                std::time::Duration::from_millis(jitter_ms)
            } else {
                std::time::Duration::ZERO
            };

            // Wait for interval + jitter
            Delay::new(interval + jitter_duration).await;

            // Check for shutdown after waking
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Check if repair is needed
            let target_eager = self.inner.config.eager_fanout;
            if self.inner.peers.needs_repair(target_eager) {
                let stats_before = self.inner.peers.stats();

                // Use network-aware rebalancing with PeerScoring
                // This prefers peers with lower RTT and higher reliability for eager set
                let peer_scoring = &self.inner.peer_scoring;
                let scorer = |peer: &I| peer_scoring.normalized_score(peer, 0.5);

                // Try non-blocking rebalance first to avoid contention
                if self
                    .inner
                    .peers
                    .try_rebalance_with_scorer(target_eager, scorer)
                {
                    let stats_after = self.inner.peers.stats();
                    let promoted = stats_after
                        .eager_count
                        .saturating_sub(stats_before.eager_count);

                    if promoted > 0 {
                        info!(
                            promoted = promoted,
                            eager_before = stats_before.eager_count,
                            eager_after = stats_after.eager_count,
                            lazy_count = stats_after.lazy_count,
                            target = target_eager,
                            "topology repair: promoted lazy peers to eager (network-aware)"
                        );

                        #[cfg(feature = "metrics")]
                        for _ in 0..promoted {
                            crate::metrics::record_peer_promotion();
                        }
                    }
                } else {
                    // Lock was contended, will retry next cycle
                    trace!("maintenance: lock contended, will retry");
                }
            }

            // Update metrics
            #[cfg(feature = "metrics")]
            {
                let stats = self.inner.peers.stats();
                crate::metrics::set_eager_peers(stats.eager_count);
                crate::metrics::set_lazy_peers(stats.lazy_count);
                crate::metrics::set_total_peers(stats.eager_count + stats.lazy_count);

                // Update cache, queue, and pending grafts gauges
                let cache_stats = self.inner.cache.stats();
                crate::metrics::set_cache_size(cache_stats.entries);
                crate::metrics::set_ihave_queue_size(self.inner.scheduler.queue().len());
                crate::metrics::set_pending_grafts(self.inner.graft_timer.pending_count());
            }
        }
    }

    /// Run anti-entropy sync background task.
    ///
    /// This task periodically compares message state with random peers
    /// and requests any missing messages. This enables recovery from:
    /// - Network partitions
    /// - Node crashes
    /// - Long disconnects (> cache TTL)
    ///
    /// This should be spawned as a background task when sync is enabled.
    ///
    /// # Note
    ///
    /// This is a no-op if sync is not configured. Check `config.sync` before
    /// spawning this task to avoid unnecessary work.
    #[instrument(skip(self), name = "anti_entropy_sync")]
    pub async fn run_anti_entropy_sync(&self) {
        let Some(ref sync_config) = self.inner.config.sync else {
            info!("Anti-entropy sync not configured, skipping");
            return;
        };

        if !sync_config.enabled {
            info!("Anti-entropy sync disabled");
            return;
        }

        info!(
            interval_s = sync_config.sync_interval.as_secs(),
            window_s = sync_config.sync_window.as_secs(),
            "Anti-entropy sync started"
        );

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                info!("Anti-entropy sync shutting down");
                break;
            }

            // Wait for sync interval
            Delay::new(sync_config.sync_interval).await;

            // Check for shutdown after waking
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Pick a random peer
            if let Some(peer) = self.inner.peers.random_peer() {
                debug!(?peer, "Initiating sync with peer");

                // For now, we just log that sync would happen
                // Full implementation requires storage integration
                // which is done in the PlumtreeDiscovery layer
                trace!("Sync protocol not fully integrated at Plumtree layer");
            } else {
                trace!("No peers available for sync");
            }
        }
    }

    /// Shutdown the Plumtree instance.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.scheduler.shutdown();
        self.inner.outgoing_tx.close();
        self.inner.incoming_tx.close();
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }

    /// Rebalance peers to match target fanout using network-aware scoring.
    ///
    /// Uses hybrid scoring that combines:
    /// - Topology proximity (hash ring distance)
    /// - Network performance (RTT and reliability from PeerScoring)
    ///
    /// Ring neighbors are protected and will not be demoted.
    pub fn rebalance_peers(&self) {
        let peer_scoring = &self.inner.peer_scoring;
        self.inner
            .peers
            .rebalance_with_scorer(self.inner.config.eager_fanout, |peer| {
                peer_scoring.normalized_score(peer, 0.5)
            });
    }

    /// Get access to the peer state for testing/debugging.
    pub fn peers(&self) -> &SharedPeerState<I> {
        &self.inner.peers
    }
}

impl<I, D> Clone for Plumtree<I, D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Handle for interacting with Plumtree from the network layer.
///
/// Provides channels for sending and receiving messages.
pub struct PlumtreeHandle<I> {
    /// Channel for receiving outgoing messages to send.
    outgoing_rx: Receiver<OutgoingMessage<I>>,
    /// Channel for receiving incoming messages (for internal processing).
    incoming_rx: Receiver<IncomingMessage<I>>,
    /// Channel for submitting incoming messages.
    incoming_tx: Sender<IncomingMessage<I>>,
}

impl<I> PlumtreeHandle<I> {
    /// Get the next outgoing message to send.
    pub async fn next_outgoing(&self) -> Option<OutgoingMessage<I>> {
        self.outgoing_rx.recv().await.ok()
    }

    /// Try to get the next outgoing message without blocking.
    ///
    /// Returns `Some(message)` if a message is available, `None` otherwise.
    /// This is useful for non-blocking polling in tests and simulations.
    pub fn try_next_outgoing(&self) -> Option<OutgoingMessage<I>> {
        self.outgoing_rx.try_recv().ok()
    }

    /// Submit an incoming message for processing.
    pub async fn submit_incoming(&self, from: I, message: PlumtreeMessage) -> Result<()> {
        self.incoming_tx
            .send(IncomingMessage { from, message })
            .await
            .map_err(|e| Error::Channel(e.to_string()))
    }

    /// Get a stream of outgoing messages.
    pub fn outgoing_stream(&self) -> impl futures::Stream<Item = OutgoingMessage<I>> + '_ {
        self.outgoing_rx.clone()
    }

    /// Get a stream of incoming messages for processing.
    ///
    /// Use this to receive messages that were submitted via `submit_incoming`.
    /// This is useful for custom message processing pipelines.
    pub fn incoming_stream(&self) -> impl futures::Stream<Item = IncomingMessage<I>> + '_ {
        self.incoming_rx.clone()
    }

    /// Get the next incoming message for processing.
    ///
    /// Use this to receive messages that were submitted via `submit_incoming`.
    pub async fn next_incoming(&self) -> Option<IncomingMessage<I>> {
        self.incoming_rx.recv().await.ok()
    }

    /// Check if the handle is closed.
    pub fn is_closed(&self) -> bool {
        self.outgoing_rx.is_closed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct TestNodeId(u64);

    struct TestDelegate {
        delivered: parking_lot::Mutex<Vec<(MessageId, Bytes)>>,
    }

    impl TestDelegate {
        fn new() -> Self {
            Self {
                delivered: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn delivered_count(&self) -> usize {
            self.delivered.lock().len()
        }
    }

    impl PlumtreeDelegate<TestNodeId> for TestDelegate {
        fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
            self.delivered.lock().push((message_id, payload));
        }
    }

    #[tokio::test]
    async fn test_broadcast() {
        let delegate = Arc::new(TestDelegate::new());
        let (plumtree, _handle) =
            Plumtree::new(TestNodeId(1), PlumtreeConfig::default(), delegate.clone());

        // Add some peers
        plumtree.add_peer(TestNodeId(2));
        plumtree.add_peer(TestNodeId(3));
        plumtree.add_peer(TestNodeId(4));

        // Broadcast a message
        let msg_id = plumtree
            .broadcast(Bytes::from_static(b"hello"))
            .await
            .unwrap();

        // Message should be in cache
        assert!(plumtree.inner.cache.contains(&msg_id));
    }

    #[tokio::test]
    async fn test_handle_gossip() {
        let delegate = Arc::new(TestDelegate::new());
        let (plumtree, _handle) =
            Plumtree::new(TestNodeId(1), PlumtreeConfig::default(), delegate.clone());

        plumtree.add_peer(TestNodeId(2));

        let msg_id = MessageId::new();
        let payload = Bytes::from_static(b"test message");

        // Handle incoming gossip
        plumtree
            .handle_message(
                TestNodeId(2),
                PlumtreeMessage::Gossip {
                    id: msg_id,
                    round: 0,
                    payload: payload.clone(),
                },
            )
            .await
            .unwrap();

        // Message should be delivered
        assert_eq!(delegate.delivered_count(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_detection() {
        let delegate = Arc::new(TestDelegate::new());
        let (plumtree, _handle) =
            Plumtree::new(TestNodeId(1), PlumtreeConfig::default(), delegate.clone());

        plumtree.add_peer(TestNodeId(2));

        let msg_id = MessageId::new();
        let payload = Bytes::from_static(b"test message");

        // Handle same message twice
        for _ in 0..2 {
            plumtree
                .handle_message(
                    TestNodeId(2),
                    PlumtreeMessage::Gossip {
                        id: msg_id,
                        round: 0,
                        payload: payload.clone(),
                    },
                )
                .await
                .unwrap();
        }

        // Should only be delivered once
        assert_eq!(delegate.delivered_count(), 1);
    }

    #[tokio::test]
    async fn test_peer_promotion() {
        let delegate = Arc::new(TestDelegate::new());
        let (plumtree, _handle) =
            Plumtree::new(TestNodeId(1), PlumtreeConfig::default(), delegate.clone());

        // Use add_peer_lazy to test promotion from lazy to eager
        plumtree.add_peer_lazy(TestNodeId(2));

        // Peer starts as lazy
        assert!(plumtree.inner.peers.is_lazy(&TestNodeId(2)));

        // Send IHave for unknown message - should trigger promotion
        let msg_id = MessageId::new();
        plumtree
            .handle_message(
                TestNodeId(2),
                PlumtreeMessage::IHave {
                    message_ids: smallvec::smallvec![msg_id],
                    round: 0,
                },
            )
            .await
            .unwrap();

        // Peer should now be eager
        assert!(plumtree.inner.peers.is_eager(&TestNodeId(2)));
    }

    #[tokio::test]
    async fn test_message_too_large() {
        let delegate = Arc::new(TestDelegate::new());
        let config = PlumtreeConfig::default().with_max_message_size(10);
        let (plumtree, _handle) = Plumtree::new(TestNodeId(1), config, delegate);

        let result = plumtree
            .broadcast(Bytes::from_static(b"this is too large"))
            .await;

        assert!(matches!(result, Err(Error::MessageTooLarge { .. })));
    }
}
