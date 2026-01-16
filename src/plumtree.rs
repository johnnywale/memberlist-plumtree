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

use crate::{
    config::PlumtreeConfig,
    error::{Error, Result},
    message::{MessageCache, MessageId, PlumtreeMessage},
    peer_state::{PeerState, SharedPeerState},
    rate_limiter::RateLimiter,
    scheduler::{GraftTimer, IHaveScheduler, PendingIHave},
};

/// Number of shards for the seen map.
/// 16 shards provides good concurrency while keeping memory overhead low.
const SEEN_MAP_SHARDS: usize = 16;

/// Sharded map for tracking seen messages.
///
/// Uses multiple shards to reduce lock contention:
/// - Each operation only locks one shard (based on message ID hash)
/// - Cleanup iterates through shards one at a time, yielding between shards
///
/// This prevents the cleanup task from blocking all message processing.
struct ShardedSeenMap<I> {
    shards: Vec<RwLock<HashMap<MessageId, SeenEntry<I>>>>,
}

impl<I> ShardedSeenMap<I> {
    /// Create a new sharded seen map with the default number of shards.
    fn new() -> Self {
        let shards = (0..SEEN_MAP_SHARDS)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();
        Self { shards }
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
    /// Returns (is_duplicate, receive_count).
    async fn check_and_mark_seen(&self, id: MessageId, entry_fn: impl FnOnce() -> SeenEntry<I>) -> (bool, u32) {
        let mut shard = self.write_shard(&id).await;
        if let Some(entry) = shard.get_mut(&id) {
            entry.receive_count += 1;
            (true, entry.receive_count)
        } else {
            let entry = entry_fn();
            let count = entry.receive_count;
            shard.insert(id, entry);
            (false, count)
        }
    }

    /// Clean up expired entries from one shard.
    /// Returns the number of entries removed.
    async fn cleanup_shard(&self, shard_idx: usize, now: std::time::Instant, ttl: std::time::Duration) -> usize {
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

    /// Get the number of shards.
    fn shard_count(&self) -> usize {
        self.shards.len()
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
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
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
        let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);
        let (incoming_tx, incoming_rx) = async_channel::bounded(1024);

        // Rate limiter: allow 10 Graft requests per peer per second, burst of 20
        let graft_rate_limiter = RateLimiter::new(
            config.graft_rate_limit_burst,
            config.graft_rate_limit_per_second,
        );

        let inner = Arc::new(PlumtreeInner {
            peers: Arc::new(PeerState::new()),
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

    /// Add a peer (joins as lazy initially).
    pub fn add_peer(&self, peer: I) {
        if peer != self.inner.local_id {
            self.inner.peers.add_peer(peer);
        }
    }

    /// Remove a peer.
    pub fn remove_peer(&self, peer: &I) {
        self.inner.peers.remove_peer(peer);
    }

    /// Broadcast a message to all nodes.
    ///
    /// The message is sent immediately to eager peers and queued
    /// as IHave announcements for lazy peers.
    ///
    /// Returns the unique message ID assigned to this broadcast.
    pub async fn broadcast(&self, payload: impl Into<Bytes>) -> Result<MessageId> {
        let payload = payload.into();

        // Check size limit
        if payload.len() > self.inner.config.max_message_size {
            return Err(Error::MessageTooLarge {
                size: payload.len(),
                max_size: self.inner.config.max_message_size,
            });
        }

        let msg_id = MessageId::new();
        let round = self.inner.round.fetch_add(1, Ordering::Relaxed);

        // Cache the message
        self.inner.cache.insert(msg_id, payload.clone());

        // Mark as seen (no parent since we originated this message)
        self.inner.seen.insert(
            msg_id,
            SeenEntry {
                round,
                receive_count: 1,
                seen_at: std::time::Instant::now(),
                parent: None, // We originated this message
            },
        ).await;

        // Send to eager peers
        let eager_peers = self.inner.peers.eager_peers();
        for peer in eager_peers {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round,
                payload: payload.clone(),
            };
            // Send unicast to specific eager peer
            let _ = self
                .inner
                .outgoing_tx
                .send(OutgoingMessage::unicast(peer, msg))
                .await;
        }

        // Queue IHave for lazy peers
        self.inner.scheduler.queue().push(msg_id, round);

        Ok(msg_id)
    }

    /// Handle an incoming Plumtree message.
    ///
    /// This should be called when a message is received from the network.
    pub async fn handle_message(&self, from: I, message: PlumtreeMessage) -> Result<()> {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(Error::Shutdown);
        }

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
        }
    }

    /// Handle a Gossip message (eager push).
    async fn handle_gossip(
        &self,
        from: I,
        msg_id: MessageId,
        round: u32,
        payload: Bytes,
    ) -> Result<()> {
        // Cancel any pending Graft timer for this message
        self.inner.graft_timer.message_received(&msg_id);

        // Check if already seen and track parent atomically (single shard lock)
        let (is_duplicate, receive_count) = self.inner.seen.check_and_mark_seen(
            msg_id,
            || SeenEntry {
                round,
                receive_count: 1,
                seen_at: std::time::Instant::now(),
                parent: Some(from.clone()), // Track who delivered this message
            }
        ).await;

        if is_duplicate {
            // Optimization: prune redundant path after threshold
            if receive_count > self.inner.config.optimization_threshold {
                // Only send Prune if the peer is in the eager set
                // Pruning a peer that is already lazy is redundant traffic
                if self.inner.peers.is_eager(&from) {
                    // Send Prune unicast to the duplicate sender
                    let _ = self
                        .inner
                        .outgoing_tx
                        .send(OutgoingMessage::unicast(from.clone(), PlumtreeMessage::Prune))
                        .await;
                    self.inner.delegate.on_prune_sent(&from);

                    // Demote to lazy
                    if self.inner.peers.demote_to_lazy(&from) {
                        self.inner.delegate.on_lazy_demotion(&from);
                    }
                }
            }
            return Ok(());
        }

        // First time seeing this message (parent already tracked above)

        // Cache for potential Graft requests
        self.inner.cache.insert(msg_id, payload.clone());

        // Deliver to application
        self.inner.delegate.on_deliver(msg_id, payload.clone());

        // Forward to eager peers (except sender) via unicast
        let eager_peers = self.inner.peers.random_eager_except(&from, usize::MAX);
        for peer in eager_peers {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round: round + 1,
                payload: payload.clone(),
            };
            // Send unicast to specific eager peer
            let _ = self
                .inner
                .outgoing_tx
                .send(OutgoingMessage::unicast(peer, msg))
                .await;
        }

        // Queue IHave for lazy peers (except sender)
        self.inner.scheduler.queue().push(msg_id, round + 1);

        Ok(())
    }

    /// Handle an IHave message (lazy push announcement).
    async fn handle_ihave(
        &self,
        from: I,
        message_ids: SmallVec<[MessageId; 8]>,
        round: u32,
    ) -> Result<()> {
        for msg_id in message_ids {
            // Check if we already have this message (only locks one shard)
            let have_message = self.inner.seen.contains(&msg_id).await;

            if !have_message {
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
                    self.inner.delegate.on_eager_promotion(&from);
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
            }
        }

        Ok(())
    }

    /// Handle a Graft message (request to establish eager link).
    async fn handle_graft(&self, from: I, msg_id: MessageId, round: u32) -> Result<()> {
        // Rate limit Graft requests per peer
        if !self.inner.graft_rate_limiter.check(&from) {
            tracing::warn!("rate limiting Graft request from {:?}", from);
            return Ok(());
        }

        // Promote requester to eager
        if self.inner.peers.promote_to_eager(&from) {
            self.inner.delegate.on_eager_promotion(&from);
        }

        // Send the requested message unicast to the requester
        if let Some(payload) = self.inner.cache.get(&msg_id) {
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
        }

        Ok(())
    }

    /// Handle a Prune message (demote to lazy).
    async fn handle_prune(&self, from: I) -> Result<()> {
        if self.inner.peers.demote_to_lazy(&from) {
            self.inner.delegate.on_lazy_demotion(&from);
        }
        Ok(())
    }

    /// Run the IHave scheduler background task.
    ///
    /// This should be spawned as a background task.
    /// Uses a "linger" strategy: flushes immediately if batch is full,
    /// otherwise waits for the configured interval.
    pub async fn run_ihave_scheduler(&self) {
        let ihave_interval = self.inner.config.ihave_interval;
        // Check for early flush more frequently (every 10ms)
        let check_interval = std::time::Duration::from_millis(10);
        let mut last_flush = std::time::Instant::now();

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
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

        // Send IHave to each lazy peer
        for peer in lazy_peers {
            let msg = PlumtreeMessage::IHave {
                message_ids: message_ids.clone(),
                round,
            };
            // Send IHave unicast to specific lazy peer
            let _ = self
                .inner
                .outgoing_tx
                .send(OutgoingMessage::unicast(peer, msg))
                .await;
        }
    }

    /// Run the Graft timer checker background task.
    ///
    /// This should be spawned as a background task.
    pub async fn run_graft_timer(&self) {
        let check_interval = self.inner.config.graft_timeout / 2;
        let mut interval = Delay::new(check_interval);

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Wait for interval
            (&mut interval).await;
            interval.reset(check_interval);

            // Check for expired Graft timers and failures
            let (expired, failed) = self.inner.graft_timer.get_expired_with_failures();

            // Handle failed grafts (zombie peer detection)
            for failed_graft in failed {
                tracing::warn!(
                    "Graft failed for message {:?} after {} retries from peer {:?}",
                    failed_graft.message_id,
                    failed_graft.total_retries,
                    failed_graft.original_peer
                );
                self.inner
                    .delegate
                    .on_graft_failed(&failed_graft.message_id, &failed_graft.original_peer);
            }

            for expired_graft in expired {
                // Send Graft to the peer determined by the timer (primary or alternative)
                tracing::debug!(
                    "Graft {} for message {:?} to peer {:?} (attempt {})",
                    if expired_graft.retry_count == 0 {
                        "initial"
                    } else {
                        "retry"
                    },
                    expired_graft.message_id,
                    expired_graft.peer,
                    expired_graft.retry_count
                );

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
    /// This should be spawned as a background task.
    pub async fn run_seen_cleanup(&self) {
        // Run cleanup every half the TTL period
        let cleanup_interval = self.inner.config.message_cache_ttl / 2;
        let ttl = self.inner.config.message_cache_ttl;
        let mut interval = Delay::new(cleanup_interval);

        loop {
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Wait for interval
            (&mut interval).await;
            interval.reset(cleanup_interval);

            // Clean up expired entries from seen map, one shard at a time
            // This prevents cleanup from blocking all message processing
            let now = std::time::Instant::now();
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
                // This prevents cleanup from monopolizing the executor
                Delay::new(std::time::Duration::from_micros(1)).await;
            }

            if total_removed > 0 {
                tracing::debug!("cleaned up {} expired entries from seen map ({} shards)", total_removed, shard_count);
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

    /// Rebalance peers to match target fanout.
    pub fn rebalance_peers(&self) {
        self.inner.peers.rebalance(self.inner.config.eager_fanout);
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

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

        plumtree.add_peer(TestNodeId(2));

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
