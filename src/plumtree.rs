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
    time::Instant,
};

use crate::{
    config::PlumtreeConfig,
    error::{Error, Result},
    message::{MessageCache, MessageId, PlumtreeMessage},
    peer_state::{PeerState, SharedPeerState},
    rate_limiter::RateLimiter,
    scheduler::{ExpiredGraft, GraftTimer, IHaveScheduler, PendingIHave},
};

#[cfg(feature = "metrics")]
use crate::metrics;

/// Delegate trait for receiving Plumtree events.
///
/// Implement this trait to handle delivered messages and other events.
///
/// # Type Parameters
///
/// - `I`: The node identifier type used in your cluster
///
/// # Important
///
/// The `on_deliver` callback is invoked synchronously within the message
/// processing loop. **It must return quickly** to avoid blocking the protocol.
/// For time-consuming operations (database writes, complex processing), queue
/// the work for async processing elsewhere.
#[auto_impl::auto_impl(Box, Arc)]
pub trait PlumtreeDelegate<I = ()>: Send + Sync + 'static {
    /// Called when a message is delivered (first time received).
    ///
    /// **Important**: This is called synchronously. Do not perform blocking
    /// operations here. Queue work for async processing if needed.
    fn on_deliver(&self, message_id: MessageId, payload: Bytes);

    /// Called when a peer is promoted to eager (will receive full messages).
    ///
    /// # Arguments
    /// - `peer`: The peer that was promoted
    fn on_eager_promotion(&self, _peer: &I) {}

    /// Called when a peer is demoted to lazy (will only receive IHave).
    ///
    /// # Arguments
    /// - `peer`: The peer that was demoted
    fn on_lazy_demotion(&self, _peer: &I) {}

    /// Called when a Graft message is sent (tree repair).
    ///
    /// # Arguments
    /// - `peer`: The peer we sent the Graft to
    /// - `message_id`: The message we're requesting
    fn on_graft_sent(&self, _peer: &I, _message_id: &MessageId) {}

    /// Called when a Prune message is sent (tree optimization).
    ///
    /// # Arguments
    /// - `peer`: The peer we sent the Prune to
    fn on_prune_sent(&self, _peer: &I) {}
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

    /// Shutdown notification channel - receivers can wait on this for shutdown signal.
    shutdown_rx: async_channel::Receiver<()>,

    /// Shutdown sender - closing this notifies all receivers.
    shutdown_tx: async_channel::Sender<()>,

    /// Channel for outgoing messages.
    outgoing_tx: Sender<OutgoingMessage<I>>,

    /// Channel for incoming messages.
    incoming_tx: Sender<IncomingMessage<I>>,

    /// Track messages seen for deduplication.
    seen: RwLock<HashMap<MessageId, SeenEntry>>,

    /// Track parent peer for each message (who we received it from first).
    /// Used for tree repair when a better path is found.
    message_parents: RwLock<HashMap<MessageId, ParentEntry<I>>>,
}

/// Entry tracking the parent peer for a message.
#[derive(Debug, Clone)]
struct ParentEntry<I> {
    /// The peer who sent us this message first.
    /// Reserved for future use in tree repair diagnostics.
    #[allow(dead_code)]
    peer: I,
    /// When this parent was recorded.
    recorded_at: Instant,
}

/// Entry for tracking seen messages.
#[derive(Debug, Clone)]
struct SeenEntry {
    /// When this message was first seen.
    first_seen: Instant,
    /// Round when first seen.
    /// Reserved for future use in protocol diagnostics.
    #[allow(dead_code)]
    round: u32,
    /// Number of times received.
    receive_count: u32,
}

/// Outgoing message to be sent.
#[derive(Debug)]
pub struct OutgoingMessage<I> {
    /// Target peer.
    pub target: I,
    /// Message to send.
    pub message: PlumtreeMessage,
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
        // Shutdown channel - closing the sender notifies all receivers
        let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

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
            shutdown_rx,
            shutdown_tx,
            outgoing_tx,
            incoming_tx,
            seen: RwLock::new(HashMap::new()),
            message_parents: RwLock::new(HashMap::new()),
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
            #[cfg(feature = "metrics")]
            {
                let stats = self.inner.peers.stats();
                metrics::set_eager_peers(stats.eager_count);
                metrics::set_lazy_peers(stats.lazy_count);
            }
        }
    }

    /// Remove a peer.
    pub fn remove_peer(&self, peer: &I) {
        self.inner.peers.remove_peer(peer);
        #[cfg(feature = "metrics")]
        {
            let stats = self.inner.peers.stats();
            metrics::set_eager_peers(stats.eager_count);
            metrics::set_lazy_peers(stats.lazy_count);
        }
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

        // Mark as seen
        {
            let mut seen = self.inner.seen.write().await;
            seen.insert(
                msg_id,
                SeenEntry {
                    first_seen: Instant::now(),
                    round,
                    receive_count: 1,
                },
            );
        }

        // Send to eager peers
        let eager_peers = self.inner.peers.eager_peers();
        #[cfg(feature = "metrics")]
        let mut gossip_count = 0usize;
        for peer in eager_peers {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round,
                payload: payload.clone(),
            };
            if self
                .inner
                .outgoing_tx
                .send(OutgoingMessage {
                    target: peer.clone(),
                    message: msg,
                })
                .await
                .is_err()
            {
                tracing::warn!("outgoing channel closed while broadcasting to {:?}", peer);
                break;
            }
            #[cfg(feature = "metrics")]
            {
                gossip_count += 1;
            }
        }

        // Queue IHave for lazy peers
        self.inner.scheduler.queue().push(msg_id, round);

        // Record metrics
        #[cfg(feature = "metrics")]
        {
            metrics::record_broadcast();
            for _ in 0..gossip_count {
                metrics::record_gossip_sent();
            }
            metrics::set_cache_size(self.inner.cache.len());
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

        // Check if already seen
        let is_duplicate = {
            let mut seen = self.inner.seen.write().await;
            if let Some(entry) = seen.get_mut(&msg_id) {
                entry.receive_count += 1;
                true
            } else {
                seen.insert(
                    msg_id,
                    SeenEntry {
                        first_seen: Instant::now(),
                        round,
                        receive_count: 1,
                    },
                );
                false
            }
        };

        if is_duplicate {
            #[cfg(feature = "metrics")]
            metrics::record_duplicate();

            // Optimization: prune redundant path after threshold
            let receive_count = {
                let seen = self.inner.seen.read().await;
                seen.get(&msg_id).map(|e| e.receive_count).unwrap_or(0)
            };

            if receive_count > self.inner.config.optimization_threshold {
                // Deterministic arbitration to prevent race conditions:
                // Only the node with the "smaller" ID (by hash) should issue Prune.
                // This prevents both nodes from pruning each other when they
                // simultaneously send duplicates, which would disconnect the tree.
                let should_prune = {
                    use std::hash::Hasher;
                    let mut local_hasher = std::collections::hash_map::DefaultHasher::new();
                    self.inner.local_id.hash(&mut local_hasher);
                    let local_hash = local_hasher.finish();

                    let mut from_hasher = std::collections::hash_map::DefaultHasher::new();
                    from.hash(&mut from_hasher);
                    let from_hash = from_hasher.finish();

                    local_hash < from_hash
                };

                if should_prune {
                    // Send Prune to the duplicate sender.
                    // IMPORTANT: We use blocking send for Prune because it's a control message.
                    // If Prune fails to reach the remote, they continue as Eager causing
                    // redundant traffic. Using blocking send ensures the message is queued.
                    // We only demote locally AFTER successful queue, maintaining consistency.
                    if self
                        .inner
                        .outgoing_tx
                        .send(OutgoingMessage {
                            target: from.clone(),
                            message: PlumtreeMessage::Prune,
                        })
                        .await
                        .is_ok()
                    {
                        #[cfg(feature = "metrics")]
                        metrics::record_prune_sent();

                        // Demote to lazy only if Prune was successfully queued
                        if self.inner.peers.demote_to_lazy(&from) {
                            #[cfg(feature = "metrics")]
                            {
                                metrics::record_peer_demotion();
                                let stats = self.inner.peers.stats();
                                metrics::set_eager_peers(stats.eager_count);
                                metrics::set_lazy_peers(stats.lazy_count);
                            }
                            self.inner.delegate.on_lazy_demotion(&from);
                            self.inner.delegate.on_prune_sent(&from);
                        }
                    } else {
                        tracing::warn!(
                            "failed to send Prune to {:?} - channel closed, keeping eager",
                            from
                        );
                    }
                }
            }
            return Ok(());
        }

        // First time seeing this message

        // Track the parent (sender) for this message
        {
            let mut parents = self.inner.message_parents.write().await;
            parents.insert(
                msg_id,
                ParentEntry {
                    peer: from.clone(),
                    recorded_at: Instant::now(),
                },
            );
        }

        // Cache for potential Graft requests
        self.inner.cache.insert(msg_id, payload.clone());

        // Deliver to application
        self.inner.delegate.on_deliver(msg_id, payload.clone());

        #[cfg(feature = "metrics")]
        {
            metrics::record_delivery();
            metrics::set_cache_size(self.inner.cache.len());
        }

        // Forward to eager peers (except sender)
        let eager_peers = self.inner.peers.random_eager_except(&from, usize::MAX);
        for peer in eager_peers {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round: round + 1,
                payload: payload.clone(),
            };
            if self
                .inner
                .outgoing_tx
                .send(OutgoingMessage {
                    target: peer.clone(),
                    message: msg,
                })
                .await
                .is_err()
            {
                tracing::warn!("outgoing channel closed while forwarding to {:?}", peer);
                break;
            }
            #[cfg(feature = "metrics")]
            metrics::record_gossip_sent();
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
            // Check if we already have this message
            let have_message = {
                let seen = self.inner.seen.read().await;
                seen.contains_key(&msg_id)
            };

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


                // Promote sender to eager to get this and future messages
                // Use try_promote_to_eager to enforce fanout limit and prevent
                // unbounded growth which would degrade to flooding
                if self
                    .inner
                    .peers
                    .try_promote_to_eager(&from, self.inner.config.eager_fanout)
                {
                    #[cfg(feature = "metrics")]
                    {
                        metrics::record_peer_promotion();
                        let stats = self.inner.peers.stats();
                        metrics::set_eager_peers(stats.eager_count);
                        metrics::set_lazy_peers(stats.lazy_count);
                    }
                    self.inner.delegate.on_eager_promotion(&from);
                }

                // Send Graft to get the missing message (control message - log failures)
                if self
                    .inner
                    .outgoing_tx
                    .send(OutgoingMessage {
                        target: from.clone(),
                        message: PlumtreeMessage::Graft {
                            message_id: msg_id,
                            round,
                        },
                    })
                    .await
                    .is_err()
                {
                    tracing::error!(
                        "failed to send Graft for {:?} to {:?} - outgoing channel closed",
                        msg_id,
                        from
                    );
                } else {
                    #[cfg(feature = "metrics")]
                    metrics::record_graft_sent();
                    self.inner.delegate.on_graft_sent(&from, &msg_id);
                }
            }
        }

        Ok(())
    }

    /// Handle a Graft message (request to establish eager link).
    async fn handle_graft(&self, from: I, msg_id: MessageId, round: u32) -> Result<()> {
        // Rate limit Graft requests per peer
        if !self.inner.graft_rate_limiter.check(&from) {
            tracing::warn!("rate limiting Graft request from {:?}", from);
            #[cfg(feature = "metrics")]
            metrics::record_rate_limited();
            return Ok(());
        }

        // Promote requester to eager (use higher limit than normal to honor requests,
        // but still prevent unbounded growth from excessive Graft messages)
        let max_eager_on_graft = self.inner.config.eager_fanout * 2;
        if self
            .inner
            .peers
            .try_promote_to_eager(&from, max_eager_on_graft)
        {
            #[cfg(feature = "metrics")]
            {
                metrics::record_peer_promotion();
                let stats = self.inner.peers.stats();
                metrics::set_eager_peers(stats.eager_count);
                metrics::set_lazy_peers(stats.lazy_count);
            }
            self.inner.delegate.on_eager_promotion(&from);
        }

        // Send the requested message if we have it
        if let Some(payload) = self.inner.cache.get(&msg_id) {
            let msg = PlumtreeMessage::Gossip {
                id: msg_id,
                round,
                payload: (*payload).clone(),
            };
            if self
                .inner
                .outgoing_tx
                .send(OutgoingMessage {
                    target: from.clone(),
                    message: msg,
                })
                .await
                .is_ok()
            {
                #[cfg(feature = "metrics")]
                metrics::record_gossip_sent();
            } else {
                tracing::warn!(
                    "failed to send Graft response for {:?} to {:?} - channel closed",
                    msg_id,
                    from
                );
            }
        } else {
            tracing::debug!("Graft request for unknown message {:?} from {:?}", msg_id, from);
        }

        Ok(())
    }

    /// Handle a Prune message (demote to lazy).
    async fn handle_prune(&self, from: I) -> Result<()> {
        if self.inner.peers.demote_to_lazy(&from) {
            #[cfg(feature = "metrics")]
            {
                metrics::record_peer_demotion();
                let stats = self.inner.peers.stats();
                metrics::set_eager_peers(stats.eager_count);
                metrics::set_lazy_peers(stats.lazy_count);
            }
            self.inner.delegate.on_lazy_demotion(&from);
        }
        Ok(())
    }

    /// Run the IHave scheduler background task.
    ///
    /// This should be spawned as a background task.
    pub async fn run_ihave_scheduler(&self) {
        use futures::future::FutureExt;
        use std::time::Duration;

        let mut interval = Delay::new(self.inner.config.ihave_interval);
        let cleanup_interval = Duration::from_secs(10);
        let mut last_cleanup = Instant::now();

        loop {
            // Wait for either interval or shutdown signal
            let shutdown_recv = self.inner.shutdown_rx.recv().fuse();
            futures::pin_mut!(shutdown_recv);

            futures::select! {
                _ = (&mut interval).fuse() => {
                    interval.reset(self.inner.config.ihave_interval);
                }
                _ = shutdown_recv => {
                    // Shutdown signal received
                    break;
                }
            }

            // Also check atomic flag for sync shutdown
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Periodic cleanup of stale metadata (every 10 seconds)
            if last_cleanup.elapsed() >= cleanup_interval {
                self.cleanup_stale_metadata().await;
                last_cleanup = Instant::now();
            }

            // Get batch of IHaves to send
            let batch: SmallVec<[PendingIHave; 16]> = self.inner.scheduler.pop_batch();

            if batch.is_empty() {
                continue;
            }

            // Collect message IDs
            let message_ids: SmallVec<[MessageId; 8]> =
                batch.iter().map(|p| p.message_id).collect();
            let round = batch.iter().map(|p| p.round).max().unwrap_or(0);

            // Get lazy peers to send to
            let lazy_peers = self
                .inner
                .peers
                .random_lazy_except(&self.inner.local_id, self.inner.config.lazy_fanout);

            // Send IHave to each lazy peer (non-blocking - IHave is non-critical)
            for peer in lazy_peers {
                let msg = PlumtreeMessage::IHave {
                    message_ids: message_ids.clone(),
                    round,
                };
                // Use try_send for IHave - these are non-critical and can be dropped
                // if the channel is full, avoiding blocking the protocol loop
                match self.inner.outgoing_tx.try_send(OutgoingMessage {
                    target: peer.clone(),
                    message: msg,
                }) {
                    Ok(()) => {
                        #[cfg(feature = "metrics")]
                        metrics::record_ihave_sent();
                    }
                    Err(async_channel::TrySendError::Full(_)) => {
                        tracing::debug!("IHave dropped for {:?}: channel full", peer);
                    }
                    Err(async_channel::TrySendError::Closed(_)) => {
                        tracing::debug!("IHave scheduler: outgoing channel closed");
                        return;
                    }
                }
            }

            #[cfg(feature = "metrics")]
            metrics::set_ihave_queue_size(self.inner.scheduler.queue().len());
        }
    }

    /// Run the Graft timer checker background task.
    ///
    /// This should be spawned as a background task.
    pub async fn run_graft_timer(&self) {
        use futures::future::FutureExt;

        let check_interval = self.inner.config.graft_timeout / 2;
        let mut interval = Delay::new(check_interval);

        loop {
            // Wait for either interval or shutdown signal
            let shutdown_recv = self.inner.shutdown_rx.recv().fuse();
            futures::pin_mut!(shutdown_recv);

            futures::select! {
                _ = (&mut interval).fuse() => {
                    interval.reset(check_interval);
                }
                _ = shutdown_recv => {
                    // Shutdown signal received
                    break;
                }
            }

            // Also check atomic flag for sync shutdown
            if self.inner.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Check for expired Graft timers
            let expired: Vec<ExpiredGraft<I>> = self.inner.graft_timer.get_expired();

            for expired_graft in expired {
                // The GraftTimer already handles round-robin through alternatives
                tracing::debug!(
                    "Graft retry {} for message {:?} to {:?} (attempt {})",
                    if expired_graft.retry_count == 0 {
                        "initial"
                    } else {
                        "backoff"
                    },
                    expired_graft.message_id,
                    expired_graft.peer,
                    expired_graft.retry_count
                );

                #[cfg(feature = "metrics")]
                {
                    if expired_graft.retry_count > 0 {
                        metrics::record_graft_retry();
                    }
                    // Check if this is the last retry attempt (entry will be removed after this)
                    if expired_graft.retry_count + 1 >= self.inner.config.graft_max_retries {
                        metrics::record_graft_timeout();
                    }
                }

                if self
                    .inner
                    .outgoing_tx
                    .send(OutgoingMessage {
                        target: expired_graft.peer.clone(),
                        message: PlumtreeMessage::Graft {
                            message_id: expired_graft.message_id,
                            round: expired_graft.round,
                        },
                    })
                    .await
                    .is_err()
                {
                    tracing::error!(
                        "failed to send Graft retry for {:?} - channel closed",
                        expired_graft.message_id
                    );
                    return;
                }

                #[cfg(feature = "metrics")]
                metrics::record_graft_sent();

                self.inner
                    .delegate
                    .on_graft_sent(&expired_graft.peer, &expired_graft.message_id);
            }
        }
    }

    /// Shutdown the Plumtree instance.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
        // Close the shutdown channel to wake up all background tasks
        self.inner.shutdown_tx.close();
        self.inner.scheduler.shutdown();
        self.inner.outgoing_tx.close();
        self.inner.incoming_tx.close();
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.inner.shutdown.load(Ordering::Acquire)
    }

    /// Clean up stale entries from internal metadata maps.
    ///
    /// This removes entries from `seen` and `message_parents` that are older
    /// than the configured `message_cache_ttl`. Should be called periodically
    /// to prevent memory growth.
    ///
    /// Uses batch cleanup to minimize lock contention: first identifies expired
    /// keys with a read lock, then removes them in batches with short write locks.
    pub async fn cleanup_stale_metadata(&self) {
        const BATCH_SIZE: usize = 1000;
        let ttl = self.inner.config.message_cache_ttl;
        let now = Instant::now();

        // Clean up seen entries in batches to reduce lock contention
        loop {
            // Phase 1: Identify expired keys (read lock)
            let expired_keys: Vec<MessageId> = {
                let seen = self.inner.seen.read().await;
                seen.iter()
                    .filter(|(_, entry)| now.duration_since(entry.first_seen) >= ttl)
                    .take(BATCH_SIZE)
                    .map(|(k, _)| *k)
                    .collect()
            };

            if expired_keys.is_empty() {
                break;
            }

            // Phase 2: Remove expired keys (write lock, short duration)
            {
                let mut seen = self.inner.seen.write().await;
                for key in &expired_keys {
                    seen.remove(key);
                }
            }

            // If we got a full batch, there might be more - continue
            if expired_keys.len() < BATCH_SIZE {
                break;
            }
        }

        // Clean up message_parents entries in batches
        loop {
            let expired_keys: Vec<MessageId> = {
                let parents = self.inner.message_parents.read().await;
                parents
                    .iter()
                    .filter(|(_, entry)| now.duration_since(entry.recorded_at) >= ttl)
                    .take(BATCH_SIZE)
                    .map(|(k, _)| *k)
                    .collect()
            };

            if expired_keys.is_empty() {
                break;
            }

            {
                let mut parents = self.inner.message_parents.write().await;
                for key in &expired_keys {
                    parents.remove(key);
                }
            }

            if expired_keys.len() < BATCH_SIZE {
                break;
            }
        }
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
