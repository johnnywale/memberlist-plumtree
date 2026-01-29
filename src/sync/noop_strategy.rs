//! No-op sync strategy (disabled sync).
//!
//! Use this when anti-entropy sync is not needed, such as:
//! - Testing environments
//! - Small clusters with reliable networks
//! - Applications that handle their own recovery

use bytes::Bytes;
use std::marker::PhantomData;

use super::strategy::{SyncError, SyncResult, SyncStrategy};
use super::SyncState;
use crate::message::SyncMessage;
use crate::storage::MessageStore;
use crate::MessageId;
use crate::Transport;
use parking_lot::RwLock;

/// No-op sync strategy that disables anti-entropy sync.
///
/// All sync operations are no-ops or return errors indicating sync is disabled.
/// The sync state is still maintained for potential future use.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::sync::NoOpSyncStrategy;
///
/// let strategy = NoOpSyncStrategy::<NodeId, MemoryStore>::new();
/// // Sync is disabled, but API is compatible
/// ```
pub struct NoOpSyncStrategy<I, S> {
    sync_state: RwLock<SyncState>,
    _marker: PhantomData<(I, S)>,
}

impl<I, S> NoOpSyncStrategy<I, S> {
    /// Create a new no-op sync strategy.
    pub fn new() -> Self {
        Self {
            sync_state: RwLock::new(SyncState::new()),
            _marker: PhantomData,
        }
    }
}

impl<I, S> Default for NoOpSyncStrategy<I, S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, S> SyncStrategy<I, S> for NoOpSyncStrategy<I, S>
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    S: MessageStore + 'static,
{
    fn needs_background_task(&self) -> bool {
        false
    }

    async fn local_state(&self) -> Bytes {
        Bytes::new()
    }

    async fn merge_remote_state<F>(&self, _buf: &[u8], _peer_resolver: F)
    where
        F: FnOnce() -> Option<I> + Send,
    {
        // No-op
    }

    async fn run_background_sync<T>(&self, _transport: T)
    where
        T: Transport<I>,
    {
        // Never runs - just pending forever if called
        std::future::pending::<()>().await
    }

    async fn handle_sync_message(
        &self,
        _from: I,
        _message: SyncMessage,
    ) -> SyncResult<Option<SyncMessage>> {
        Err(SyncError::Disabled)
    }

    fn is_enabled(&self) -> bool {
        false
    }

    fn root_hash(&self) -> [u8; 32] {
        self.sync_state.read().root_hash()
    }

    fn record_message(&self, id: MessageId, payload: &[u8]) {
        self.sync_state.write().insert(id, payload);
    }

    fn remove_message(&self, id: &MessageId) {
        self.sync_state.write().remove(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;

    #[tokio::test]
    async fn test_noop_strategy_disabled() {
        let strategy = NoOpSyncStrategy::<u64, MemoryStore>::new();

        assert!(!strategy.is_enabled());
        assert!(!strategy.needs_background_task());
    }

    #[tokio::test]
    async fn test_noop_strategy_local_state_empty() {
        let strategy = NoOpSyncStrategy::<u64, MemoryStore>::new();

        let state = strategy.local_state().await;
        assert!(state.is_empty());
    }

    #[tokio::test]
    async fn test_noop_strategy_handle_message_returns_error() {
        let strategy = NoOpSyncStrategy::<u64, MemoryStore>::new();

        let msg = SyncMessage::Request {
            root_hash: [0u8; 32],
            time_start: 0,
            time_end: 100,
        };

        let result = strategy.handle_sync_message(1u64, msg).await;
        assert!(matches!(result, Err(SyncError::Disabled)));
    }

    #[test]
    fn test_noop_strategy_still_tracks_state() {
        let strategy = NoOpSyncStrategy::<u64, MemoryStore>::new();

        // Should still track messages for potential future use
        let id = MessageId::new();
        strategy.record_message(id, b"test");

        assert_ne!(strategy.root_hash(), [0u8; 32]);

        strategy.remove_message(&id);
        assert_eq!(strategy.root_hash(), [0u8; 32]);
    }
}
