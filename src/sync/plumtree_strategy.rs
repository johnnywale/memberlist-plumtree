//! Plumtree-native sync strategy using 4-message protocol.
//!
//! This strategy runs its own background task for periodic anti-entropy sync.
//! Use this when not using memberlist as the discovery provider (e.g., static
//! discovery, custom discovery).
//!
//! # Protocol
//!
//! ```text
//! Initiator                          Responder
//!     │                                   │
//!     │──── SyncRequest(root_hash) ──────>│
//!     │                                   │
//!     │<─── SyncResponse(match/ids) ──────│
//!     │                                   │
//!     │──── SyncPull(missing_ids) ───────>│  (if mismatch)
//!     │                                   │
//!     │<─── SyncPush(messages) ───────────│
//!     │                                   │
//! ```

use bytes::Bytes;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::memberlist_strategy::handle_sync_message_common;
use super::strategy::{SyncResult, SyncStrategy};
use super::SyncHandler;
use crate::config::SyncConfig;
use crate::message::{PlumtreeMessage, SyncMessage};
use crate::peer_state::PeerState;
use crate::storage::{current_time_ms, MessageStore};
use crate::{encode_plumtree_envelope, IdCodec, MessageId, Transport};

/// Sync strategy using Plumtree's custom 4-message protocol.
///
/// This strategy runs its own background task for periodic anti-entropy sync.
/// Use this when not using memberlist as the discovery provider.
///
/// # Type Parameters
///
/// * `I` - Node identifier type (must implement `IdCodec` for envelope encoding)
/// * `S` - Storage backend type
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::sync::PlumtreeSyncStrategy;
///
/// let strategy = PlumtreeSyncStrategy::new(
///     local_id,
///     sync_handler,
///     peers,
///     SyncConfig::enabled(),
///     shutdown,
/// );
///
/// // Run with transport
/// strategy.run_background_sync(transport).await;
/// ```
pub struct PlumtreeSyncStrategy<I, S: MessageStore> {
    local_id: I,
    sync_handler: Arc<SyncHandler<S>>,
    peers: Arc<PeerState<I>>,
    config: SyncConfig,
    shutdown: Arc<AtomicBool>,
}

impl<I, S> PlumtreeSyncStrategy<I, S>
where
    I: Clone + Send + Sync + 'static,
    S: MessageStore,
{
    /// Create a new Plumtree sync strategy.
    ///
    /// # Arguments
    ///
    /// * `local_id` - This node's identifier
    /// * `sync_handler` - The sync handler for state management
    /// * `peers` - Peer state for random peer selection
    /// * `config` - Sync configuration
    /// * `shutdown` - Shutdown signal flag
    pub fn new(
        local_id: I,
        sync_handler: Arc<SyncHandler<S>>,
        peers: Arc<PeerState<I>>,
        config: SyncConfig,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            local_id,
            sync_handler,
            peers,
            config,
            shutdown,
        }
    }

    /// Create with default config (enabled).
    pub fn with_defaults(
        local_id: I,
        sync_handler: Arc<SyncHandler<S>>,
        peers: Arc<PeerState<I>>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self::new(local_id, sync_handler, peers, SyncConfig::enabled(), shutdown)
    }

    /// Get the sync handler reference.
    pub fn sync_handler(&self) -> &Arc<SyncHandler<S>> {
        &self.sync_handler
    }

    /// Get the sync configuration.
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }
}

impl<I, S> SyncStrategy<I, S> for PlumtreeSyncStrategy<I, S>
where
    I: Clone + Eq + Hash + Ord + std::fmt::Debug + IdCodec + Send + Sync + 'static,
    S: MessageStore + 'static,
{
    fn needs_background_task(&self) -> bool {
        true // Runs own periodic sync
    }

    async fn local_state(&self) -> Bytes {
        // Not used by this strategy - return empty
        Bytes::new()
    }

    async fn merge_remote_state<F>(&self, _buf: &[u8], _peer_resolver: F)
    where
        F: FnOnce() -> Option<I> + Send,
    {
        // Not used by this strategy
    }

    async fn run_background_sync<T>(&self, transport: T)
    where
        T: Transport<I>,
    {
        if !self.config.enabled {
            tracing::info!("Plumtree sync disabled");
            return;
        }

        tracing::info!(
            interval_s = self.config.sync_interval.as_secs(),
            window_s = self.config.sync_window.as_secs(),
            "Plumtree sync strategy started"
        );

        loop {
            if self.shutdown.load(Ordering::Acquire) {
                tracing::info!("Plumtree sync shutting down");
                break;
            }

            // Wait for sync interval
            futures_timer::Delay::new(self.config.sync_interval).await;

            // Check for shutdown after waking
            if self.shutdown.load(Ordering::Acquire) {
                break;
            }

            // Pick a random peer
            if let Some(peer) = self.peers.random_peer() {
                self.initiate_sync(&transport, &peer).await;
            } else {
                tracing::trace!("no peers available for sync");
            }
        }
    }

    async fn handle_sync_message(
        &self,
        from: I,
        message: SyncMessage,
    ) -> SyncResult<Option<SyncMessage>> {
        handle_sync_message_common(&self.sync_handler, from, message).await
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    fn root_hash(&self) -> [u8; 32] {
        self.sync_handler.root_hash()
    }

    fn record_message(&self, id: MessageId, payload: &[u8]) {
        self.sync_handler.record_message(id, payload);
    }

    fn remove_message(&self, id: &MessageId) {
        self.sync_handler.remove_message(id);
    }
}

impl<I, S> PlumtreeSyncStrategy<I, S>
where
    I: Clone + Eq + Hash + Ord + std::fmt::Debug + IdCodec + Send + Sync + 'static,
    S: MessageStore,
{
    /// Initiate a sync with a specific peer.
    async fn initiate_sync<T>(&self, transport: &T, peer: &I)
    where
        T: Transport<I>,
    {
        tracing::debug!(?peer, "initiating sync");

        // Calculate time range
        let now = current_time_ms();
        let start = now.saturating_sub(self.config.sync_window.as_millis() as u64);

        // Get root hash
        let root_hash = self.sync_handler.root_hash();

        // Build and send SyncRequest
        let request = PlumtreeMessage::Sync(SyncMessage::Request {
            root_hash,
            time_start: start,
            time_end: now,
        });

        let encoded = encode_plumtree_envelope(&self.local_id, &request);
        if let Err(e) = transport.send_to(peer, encoded).await {
            tracing::warn!(?peer, "failed to send sync request: {}", e);
        }
    }

    /// Manually trigger a sync with a specific peer.
    ///
    /// Useful for testing or when you want to sync immediately after
    /// detecting a potential issue.
    pub async fn sync_with_peer<T>(&self, transport: &T, peer: &I)
    where
        T: Transport<I>,
    {
        self.initiate_sync(transport, peer).await;
    }
}

/// Builder for PlumtreeSyncStrategy with fluent configuration.
pub struct PlumtreeSyncStrategyBuilder<I, S: MessageStore> {
    local_id: I,
    sync_handler: Arc<SyncHandler<S>>,
    peers: Arc<PeerState<I>>,
    shutdown: Arc<AtomicBool>,
    config: SyncConfig,
}

impl<I, S> PlumtreeSyncStrategyBuilder<I, S>
where
    I: Clone + Send + Sync + 'static,
    S: MessageStore,
{
    /// Create a new builder.
    pub fn new(
        local_id: I,
        sync_handler: Arc<SyncHandler<S>>,
        peers: Arc<PeerState<I>>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            local_id,
            sync_handler,
            peers,
            shutdown,
            config: SyncConfig::enabled(),
        }
    }

    /// Set the sync interval.
    pub fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.config.sync_interval = interval;
        self
    }

    /// Set the sync window.
    pub fn with_sync_window(mut self, window: Duration) -> Self {
        self.config.sync_window = window;
        self
    }

    /// Set the max batch size.
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.config.max_batch_size = size;
        self
    }

    /// Enable or disable sync.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Build the strategy.
    pub fn build(self) -> PlumtreeSyncStrategy<I, S> {
        PlumtreeSyncStrategy::new(
            self.local_id,
            self.sync_handler,
            self.peers,
            self.config,
            self.shutdown,
        )
    }
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;

    fn make_strategy() -> PlumtreeSyncStrategy<u64, MemoryStore> {
        let store = Arc::new(MemoryStore::new(1000));
        let sync_handler = Arc::new(SyncHandler::new(store));
        let peers = Arc::new(PeerState::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        PlumtreeSyncStrategy::new(
            0u64,
            sync_handler,
            peers,
            SyncConfig::enabled(),
            shutdown,
        )
    }

    #[test]
    fn test_plumtree_strategy_enabled() {
        let strategy = make_strategy();

        assert!(strategy.is_enabled());
        assert!(strategy.needs_background_task());
    }

    #[test]
    fn test_plumtree_strategy_record_message() {
        let strategy = make_strategy();

        // Initially empty
        assert_eq!(strategy.root_hash(), [0u8; 32]);

        // Record a message
        let id = MessageId::new();
        strategy.record_message(id, b"test");

        // Hash should change
        assert_ne!(strategy.root_hash(), [0u8; 32]);

        // Remove message
        strategy.remove_message(&id);

        // Hash should return to zero
        assert_eq!(strategy.root_hash(), [0u8; 32]);
    }

    #[tokio::test]
    async fn test_plumtree_strategy_handle_sync_request() {
        let strategy = make_strategy();

        // Record a message
        let id = MessageId::new();
        strategy.record_message(id, b"test");

        // Handle sync request with different hash
        let request = SyncMessage::Request {
            root_hash: [0u8; 32], // Different from our hash
            time_start: 0,
            time_end: u64::MAX,
        };

        let result = strategy.handle_sync_message(1u64, request).await;
        assert!(result.is_ok());

        if let Some(SyncMessage::Response { matches, .. }) = result.unwrap() {
            assert!(!matches); // Should not match
        } else {
            panic!("expected SyncResponse");
        }
    }

    #[tokio::test]
    async fn test_plumtree_strategy_handle_sync_response_match() {
        let strategy = make_strategy();

        // Handle sync response that matches
        let response = SyncMessage::Response {
            matches: true,
            message_ids: smallvec::smallvec![],
            has_more: false,
        };

        let result = strategy.handle_sync_message(1u64, response).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // No follow-up needed
    }

    #[test]
    fn test_builder() {
        let store = Arc::new(MemoryStore::new(1000));
        let sync_handler = Arc::new(SyncHandler::new(store));
        let peers = Arc::new(PeerState::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        let strategy = PlumtreeSyncStrategyBuilder::new(0u64, sync_handler, peers, shutdown)
            .with_sync_interval(Duration::from_secs(60))
            .with_sync_window(Duration::from_secs(180))
            .with_max_batch_size(200)
            .build();

        assert_eq!(strategy.config().sync_interval, Duration::from_secs(60));
        assert_eq!(strategy.config().sync_window, Duration::from_secs(180));
        assert_eq!(strategy.config().max_batch_size, 200);
    }
}
