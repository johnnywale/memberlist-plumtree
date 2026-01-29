//! Memberlist-based sync strategy using push-pull hooks.
//!
//! This strategy piggybacks on memberlist's existing TCP connections
//! for anti-entropy sync, avoiding the need for a separate background task.
//!
//! # How it works
//!
//! 1. During memberlist push-pull, `local_state()` returns XOR root hash
//! 2. Peer's `merge_remote_state()` compares hashes
//! 3. If mismatch, queues a sync request via the provided channel
//! 4. Sync messages (Pull/Push) flow via Plumtree's unicast channel
//!
//! # Wire Format
//!
//! ```text
//! local_state format (48 bytes):
//! ┌────────────────┬──────────────┬──────────────┐
//! │  root_hash     │  timestamp   │  window_ms   │
//! │  (32 bytes)    │  (8 bytes)   │  (8 bytes)   │
//! └────────────────┴──────────────┴──────────────┘
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;

use super::strategy::{SyncResult, SyncStrategy};
use super::SyncHandler;
use crate::message::SyncMessage;
use crate::storage::{current_time_ms, MessageStore};
use crate::MessageId;
use crate::Transport;

/// Sync strategy that uses memberlist's push-pull mechanism.
///
/// This strategy piggybacks on memberlist's existing TCP connections
/// for anti-entropy sync, avoiding the need for a separate background task.
///
/// # Type Parameters
///
/// * `I` - Node identifier type
/// * `S` - Storage backend type
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::sync::MemberlistSyncStrategy;
///
/// let (sync_tx, sync_rx) = async_channel::bounded(64);
///
/// let strategy = MemberlistSyncStrategy::new(
///     local_id,
///     sync_handler,
///     Duration::from_secs(90),  // sync_window
///     sync_tx,
/// );
///
/// // In PlumtreeNodeDelegate, use:
/// // - strategy.local_state() in local_state()
/// // - strategy.merge_remote_state() in merge_remote_state()
/// ```
pub struct MemberlistSyncStrategy<I, S: MessageStore> {
    sync_handler: Arc<SyncHandler<S>>,
    sync_window: Duration,
    /// Channel to send sync requests when hash mismatch detected.
    sync_request_tx: async_channel::Sender<I>,
}

impl<I, S> MemberlistSyncStrategy<I, S>
where
    I: Clone + Send + Sync + 'static,
    S: MessageStore,
{
    /// Create a new memberlist sync strategy.
    ///
    /// # Arguments
    ///
    /// * `sync_handler` - The sync handler for state management
    /// * `sync_window` - Time window for sync (how far back to check)
    /// * `sync_request_tx` - Channel to send sync requests when mismatch detected
    pub fn new(
        sync_handler: Arc<SyncHandler<S>>,
        sync_window: Duration,
        sync_request_tx: async_channel::Sender<I>,
    ) -> Self {
        Self {
            sync_handler,
            sync_window,
            sync_request_tx,
        }
    }

    /// Get the sync handler reference.
    pub fn sync_handler(&self) -> &Arc<SyncHandler<S>> {
        &self.sync_handler
    }
}

impl<I, S> SyncStrategy<I, S> for MemberlistSyncStrategy<I, S>
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    S: MessageStore + 'static,
{
    fn needs_background_task(&self) -> bool {
        false // Uses memberlist's push-pull
    }

    async fn local_state(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(48);

        // Root hash (32 bytes)
        let root_hash = self.sync_handler.root_hash();
        buf.put_slice(&root_hash);

        // Current timestamp (8 bytes)
        let now = current_time_ms();
        buf.put_u64(now);

        // Sync window in ms (8 bytes)
        buf.put_u64(self.sync_window.as_millis() as u64);

        buf.freeze()
    }

    async fn merge_remote_state<F>(&self, buf: &[u8], peer_resolver: F)
    where
        F: FnOnce() -> Option<I> + Send,
    {
        if buf.len() < 48 {
            tracing::trace!(len = buf.len(), "sync state too short, ignoring");
            return;
        }

        // Parse remote state
        let remote_hash: [u8; 32] = buf[0..32].try_into().unwrap();
        let _remote_time = (&buf[32..40]).get_u64();
        let _window_ms = (&buf[40..48]).get_u64();

        // Compare with local hash
        let local_hash = self.sync_handler.root_hash();

        if remote_hash != local_hash {
            tracing::debug!(
                local = ?&local_hash[..8],
                remote = ?&remote_hash[..8],
                "sync hash mismatch, queuing sync request"
            );

            // Get peer ID and queue sync request
            if let Some(peer) = peer_resolver() {
                if let Err(e) = self.sync_request_tx.try_send(peer) {
                    tracing::warn!("failed to queue sync request: {}", e);
                }
            } else {
                tracing::trace!("no peer available for sync request");
            }
        } else {
            tracing::trace!("sync hashes match, no action needed");
        }
    }

    async fn run_background_sync<T>(&self, _transport: T)
    where
        T: Transport<I>,
    {
        // No background task needed - sync happens via push-pull hooks
        // This will never be called since needs_background_task() returns false
        std::future::pending::<()>().await
    }

    async fn handle_sync_message(
        &self,
        from: I,
        message: SyncMessage,
    ) -> SyncResult<Option<SyncMessage>> {
        handle_sync_message_common(&self.sync_handler, from, message).await
    }

    fn is_enabled(&self) -> bool {
        true
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

/// Common sync message handling logic shared by all strategies.
pub(crate) async fn handle_sync_message_common<I, S>(
    sync_handler: &SyncHandler<S>,
    from: I,
    message: SyncMessage,
) -> SyncResult<Option<SyncMessage>>
where
    I: Clone + std::fmt::Debug + Send + Sync,
    S: MessageStore,
{
    match message {
        SyncMessage::Request {
            root_hash,
            time_start,
            time_end,
        } => {
            tracing::debug!(?from, "handling sync request");

            let response = sync_handler
                .handle_sync_request(root_hash, (time_start, time_end))
                .await;

            Ok(Some(SyncMessage::Response {
                matches: response.matches,
                message_ids: SmallVec::from_vec(response.message_ids),
                has_more: response.has_more,
            }))
        }

        SyncMessage::Response {
            matches,
            message_ids,
            has_more: _,
        } => {
            if matches {
                tracing::debug!(?from, "sync complete - hashes match");
                return Ok(None);
            }

            tracing::debug!(?from, ids = message_ids.len(), "sync response - checking for missing");

            if let Some(pull) = sync_handler
                .handle_sync_response(message_ids.to_vec())
                .await
            {
                Ok(Some(SyncMessage::Pull {
                    message_ids: SmallVec::from_vec(pull.message_ids),
                }))
            } else {
                Ok(None)
            }
        }

        SyncMessage::Pull { message_ids } => {
            tracing::debug!(?from, ids = message_ids.len(), "handling sync pull");

            let push = sync_handler.handle_sync_pull(message_ids.to_vec()).await;

            let messages: Vec<_> = push
                .messages
                .into_iter()
                .map(|m| (m.id, m.round, m.payload))
                .collect();

            Ok(Some(SyncMessage::Push { messages }))
        }

        SyncMessage::Push { .. } => {
            // Push messages are handled at the Plumtree level
            // (delivered via handle_message as Gossip)
            Ok(None)
        }
    }
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;

    async fn make_strategy() -> (
        MemberlistSyncStrategy<u64, MemoryStore>,
        async_channel::Receiver<u64>,
    ) {
        let store = Arc::new(MemoryStore::new(1000));
        let sync_handler = Arc::new(SyncHandler::new(store));
        let (tx, rx) = async_channel::bounded(64);

        let strategy =
            MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

        (strategy, rx)
    }

    #[tokio::test]
    async fn test_memberlist_strategy_enabled() {
        let (strategy, _rx) = make_strategy().await;

        assert!(strategy.is_enabled());
        assert!(!strategy.needs_background_task());
    }

    #[tokio::test]
    async fn test_memberlist_strategy_local_state_format() {
        let (strategy, _rx) = make_strategy().await;

        let state = strategy.local_state().await;
        assert_eq!(state.len(), 48);

        // First 32 bytes should be root hash (all zeros for empty state)
        assert_eq!(&state[0..32], &[0u8; 32]);
    }

    #[tokio::test]
    async fn test_memberlist_strategy_merge_triggers_sync() {
        let (strategy, rx) = make_strategy().await;

        // Record a message so our hash is non-zero
        let id = MessageId::new();
        strategy.record_message(id, b"test");

        // Create remote state with different hash (zeros)
        let mut remote_state = BytesMut::with_capacity(48);
        remote_state.put_slice(&[0u8; 32]); // Different hash
        remote_state.put_u64(current_time_ms());
        remote_state.put_u64(90_000); // 90s window

        // Merge should trigger sync request
        strategy
            .merge_remote_state(&remote_state, || Some(42u64))
            .await;

        // Should have received sync request for peer 42
        let peer = rx.try_recv().unwrap();
        assert_eq!(peer, 42);
    }

    #[tokio::test]
    async fn test_memberlist_strategy_merge_no_sync_when_match() {
        let (strategy, rx) = make_strategy().await;

        // Get our current hash
        let local_hash = strategy.root_hash();

        // Create remote state with same hash
        let mut remote_state = BytesMut::with_capacity(48);
        remote_state.put_slice(&local_hash);
        remote_state.put_u64(current_time_ms());
        remote_state.put_u64(90_000);

        // Merge should NOT trigger sync request
        strategy
            .merge_remote_state(&remote_state, || Some(42u64))
            .await;

        // Should NOT have received any sync request
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_memberlist_strategy_handle_sync_request() {
        let (strategy, _rx) = make_strategy().await;

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
}
