//! Sync protocol handler.
//!
//! Handles sync request/response messages for anti-entropy synchronization.

use crate::storage::{MessageStore, StoredMessage};
use crate::MessageId;
use parking_lot::RwLock;
use std::sync::Arc;

use super::SyncState;

/// Maximum message IDs per sync response (prevent MTU overflow).
pub const MAX_SYNC_IDS_PER_RESPONSE: usize = 100;

/// Handles sync protocol messages.
///
/// The `SyncHandler` coordinates anti-entropy synchronization by:
/// 1. Maintaining an XOR-based sync state for fast comparison
/// 2. Handling sync requests from peers
/// 3. Processing sync responses to identify missing messages
/// 4. Fulfilling pull requests for missing messages
///
/// # Type Parameters
///
/// * `S` - Storage backend implementing [`MessageStore`]
pub struct SyncHandler<S: MessageStore> {
    store: Arc<S>,
    sync_state: RwLock<SyncState>,
}

impl<S: MessageStore> SyncHandler<S> {
    /// Create a new sync handler with the given storage backend.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            sync_state: RwLock::new(SyncState::new()),
        }
    }

    /// Handle incoming SyncRequest (peer wants to compare state).
    ///
    /// Compares the peer's root hash with local state. If they match,
    /// returns a success response. If they differ, returns local message
    /// IDs for the given time range.
    ///
    /// Note: This method releases the read lock before performing I/O
    /// to avoid blocking other operations.
    pub async fn handle_sync_request(
        &self,
        remote_root_hash: [u8; 32],
        time_range: (u64, u64),
    ) -> SyncResponse {
        self.handle_sync_request_paged(remote_root_hash, time_range, 0)
            .await
    }

    /// Handle incoming SyncRequest starting at a given pagination offset.
    ///
    /// Like [`SyncHandler::handle_sync_request`], but resumes the ID listing at
    /// `offset` within the time window. A peer that receives a response with
    /// `has_more == true` continues by re-requesting at
    /// `offset + message_ids.len()`; without this the same first page is
    /// compared forever and messages past it never get repaired.
    ///
    /// # Root hash scope
    ///
    /// The comparison hash is scoped to `time_range`, matching the set of IDs
    /// the response can actually carry. Comparing an all-history hash against
    /// window-scoped responses cannot converge: history older than the window
    /// mismatches on every round with no exchange able to repair it.
    pub async fn handle_sync_request_paged(
        &self,
        remote_root_hash: [u8; 32],
        time_range: (u64, u64),
        offset: usize,
    ) -> SyncResponse {
        // Compare only the window the response can speak about.
        let (local_hash, functional) = {
            let state = self.sync_state.read();
            (
                state.root_hash_in_window(time_range.0, time_range.1),
                state.is_functional(),
            )
        };

        // Without the `sync` cargo feature every root hash is permanently zero,
        // so a hash comparison would report `matches: true` against any peer
        // and sync would report success while recovering nothing. Fall through
        // to listing IDs instead, so the exchange still transfers data, and say
        // so loudly since this is a build misconfiguration.
        if !functional {
            static WARNED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                tracing::warn!(
                    "sync protocol is in use but the crate was built without the `sync` \
                     feature: root hashes are not computed, so hash comparison is skipped. \
                     Enable the `sync` feature."
                );
            }
        } else if local_hash == remote_root_hash {
            // We are in sync for this window.
            return SyncResponse {
                matches: true,
                message_ids: vec![],
                has_more: false,
            };
        }

        // Mismatch found - fetch this page of IDs from store (lock released)
        let (local_ids, has_more) = self
            .store
            .get_range(
                time_range.0,
                time_range.1,
                MAX_SYNC_IDS_PER_RESPONSE,
                offset,
            )
            .await
            .unwrap_or((vec![], false));

        SyncResponse {
            matches: false,
            message_ids: local_ids,
            has_more,
        }
    }

    /// Handle SyncResponse (peer told us what they have).
    ///
    /// Compares the peer's message IDs with local state to identify
    /// messages we're missing.
    pub async fn handle_sync_response(&self, remote_ids: Vec<MessageId>) -> Option<SyncPull> {
        let mut missing = Vec::new();

        for id in remote_ids {
            // Check if we have this message
            if !self.store.contains(&id).await.unwrap_or(true) {
                missing.push(id);
            }
        }

        if missing.is_empty() {
            return None;
        }

        Some(SyncPull {
            message_ids: missing,
        })
    }

    /// Handle SyncPull (peer is asking for specific messages).
    ///
    /// Retrieves the requested messages from storage and returns them
    /// for delivery to the peer.
    ///
    /// The result may hold more messages than fit in a single `Push` on the
    /// wire; use [`SyncHandler::handle_sync_pull_chunked`] to get them split to
    /// the wire limit.
    pub async fn handle_sync_pull(&self, ids: Vec<MessageId>) -> SyncPush {
        let mut messages = Vec::new();

        for id in ids {
            if let Ok(Some(msg)) = self.store.get(&id).await {
                messages.push(msg);
            }
        }

        SyncPush { messages }
    }

    /// Handle SyncPull, splitting the reply into wire-sized chunks.
    ///
    /// A `Pull` may ask for up to `MAX_SYNC_IDS_PER_RESPONSE` IDs, but a
    /// receiver rejects any `Push` carrying more than
    /// [`MAX_SYNC_PUSH_MESSAGES`] messages as malformed. Answering a large pull
    /// with one oversized `Push` therefore transfers nothing and the peers
    /// repeat the identical failed exchange forever. This returns as many
    /// `SyncPush` batches as needed, each within the limit.
    ///
    /// [`MAX_SYNC_PUSH_MESSAGES`]: crate::message::MAX_SYNC_PUSH_MESSAGES
    pub async fn handle_sync_pull_chunked(&self, ids: Vec<MessageId>) -> Vec<SyncPush> {
        let push = self.handle_sync_pull(ids).await;

        if push.messages.is_empty() {
            return Vec::new();
        }

        push.messages
            .chunks(crate::message::MAX_SYNC_PUSH_MESSAGES)
            .map(|chunk| SyncPush {
                messages: chunk.to_vec(),
            })
            .collect()
    }

    /// Record a new message in sync state - O(1).
    ///
    /// Should be called when a message is delivered to update the sync state.
    pub fn record_message(&self, id: MessageId, payload: &[u8]) {
        self.sync_state.write().insert(id, payload);
    }

    /// Remove a message from sync state - O(1).
    ///
    /// Should be called when a message is pruned from storage.
    pub fn remove_message(&self, id: &MessageId) {
        self.sync_state.write().remove(id);
    }

    /// Get current root hash over all tracked messages.
    ///
    /// This hash can be compared with a peer's root hash to quickly
    /// determine if sync is needed.
    ///
    /// Prefer [`SyncHandler::root_hash_in_window`] when comparing against a
    /// peer whose sync responses only cover a bounded window.
    pub fn root_hash(&self) -> [u8; 32] {
        self.sync_state.read().root_hash()
    }

    /// Get the root hash over only the messages in `[start, end]`.
    ///
    /// Scoping the hash to the sync window keeps mismatches actionable: the
    /// comparison then covers exactly the messages the exchange is able to
    /// transfer, so divergence in older history cannot wedge the peers into
    /// mismatching forever with no repair available.
    pub fn root_hash_in_window(&self, start: u64, end: u64) -> [u8; 32] {
        self.sync_state.read().root_hash_in_window(start, end)
    }

    /// Get the number of messages tracked in sync state.
    pub fn len(&self) -> usize {
        self.sync_state.read().len()
    }

    /// Check if sync state is empty.
    pub fn is_empty(&self) -> bool {
        self.sync_state.read().is_empty()
    }

    /// Check if a message is in the sync state.
    pub fn contains(&self, id: &MessageId) -> bool {
        self.sync_state.read().contains(id)
    }

    /// Clear the sync state.
    ///
    /// Use with caution - this will require a full rebuild of the sync state.
    pub fn clear(&self) {
        self.sync_state.write().clear();
    }

    /// Rebuild sync state from a set of messages.
    ///
    /// This is O(n) and should be used sparingly (e.g., after pruning).
    pub fn rebuild_from_messages<'a>(&self, messages: impl Iterator<Item = (MessageId, &'a [u8])>) {
        self.sync_state.write().rebuild(messages);
    }

    /// Get a reference to the underlying store.
    pub fn store(&self) -> &Arc<S> {
        &self.store
    }
}

/// Response to a sync request.
#[derive(Debug, Clone)]
pub struct SyncResponse {
    /// Whether the root hashes matched (sync is complete).
    pub matches: bool,
    /// Message IDs in the time range (empty if matches=true).
    pub message_ids: Vec<MessageId>,
    /// Whether there are more message IDs beyond this response (pagination).
    ///
    /// When true, the requester continues by issuing another `SyncRequest`
    /// whose `time_start` begins just after the newest ID in this response.
    pub has_more: bool,
}

/// Pull request for missing messages.
#[derive(Debug, Clone)]
pub struct SyncPull {
    /// Message IDs to request.
    pub message_ids: Vec<MessageId>,
}

/// Push response with requested messages.
#[derive(Debug, Clone)]
pub struct SyncPush {
    /// Requested messages.
    pub messages: Vec<StoredMessage>,
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    use super::*;
    use crate::storage::MemoryStore;
    use bytes::Bytes;

    async fn make_handler() -> SyncHandler<MemoryStore> {
        let store = Arc::new(MemoryStore::new(1000));
        SyncHandler::new(store)
    }

    #[tokio::test]
    async fn test_record_and_root_hash() {
        let handler = make_handler().await;

        assert!(handler.is_empty());
        assert_eq!(handler.root_hash(), [0u8; 32]);

        let id = MessageId::new();
        handler.record_message(id, b"hello");

        assert_eq!(handler.len(), 1);
        assert!(handler.contains(&id));
        assert_ne!(handler.root_hash(), [0u8; 32]);
    }

    #[tokio::test]
    async fn test_remove_message() {
        let handler = make_handler().await;
        let id = MessageId::new();

        handler.record_message(id, b"hello");
        assert_eq!(handler.len(), 1);

        handler.remove_message(&id);
        assert_eq!(handler.len(), 0);
        assert_eq!(handler.root_hash(), [0u8; 32]);
    }

    #[tokio::test]
    async fn test_sync_request_match() {
        let handler = make_handler().await;
        let id = MessageId::new();
        handler.record_message(id, b"hello");

        let local_hash = handler.root_hash();

        // Request with matching hash
        let response = handler.handle_sync_request(local_hash, (0, u64::MAX)).await;

        assert!(response.matches);
        assert!(response.message_ids.is_empty());
        assert!(!response.has_more);
    }

    #[tokio::test]
    async fn test_sync_request_mismatch() {
        let handler = make_handler().await;

        // Insert message into store
        let id = MessageId::new();
        let msg = StoredMessage::with_timestamp(id, 0, Bytes::from_static(b"hello"), 1000);
        handler.store.insert(&msg).await.unwrap();
        handler.record_message(id, b"hello");

        // Request with different hash
        let wrong_hash = [1u8; 32];
        let response = handler.handle_sync_request(wrong_hash, (0, u64::MAX)).await;

        assert!(!response.matches);
        assert_eq!(response.message_ids.len(), 1);
        assert_eq!(response.message_ids[0], id);
    }

    #[tokio::test]
    async fn test_sync_response_identifies_missing() {
        let handler = make_handler().await;

        let id1 = MessageId::new();
        let id2 = MessageId::new();
        let id3 = MessageId::new();

        // We have id1 and id2
        handler
            .store
            .insert(&StoredMessage::with_timestamp(
                id1,
                0,
                Bytes::from_static(b"msg1"),
                1000,
            ))
            .await
            .unwrap();
        handler
            .store
            .insert(&StoredMessage::with_timestamp(
                id2,
                0,
                Bytes::from_static(b"msg2"),
                2000,
            ))
            .await
            .unwrap();

        // Peer says they have id2 and id3
        let pull = handler.handle_sync_response(vec![id2, id3]).await.unwrap();

        // We should request id3 (we don't have it)
        assert_eq!(pull.message_ids.len(), 1);
        assert!(pull.message_ids.contains(&id3));
    }

    #[tokio::test]
    async fn test_sync_response_no_missing() {
        let handler = make_handler().await;

        let id = MessageId::new();
        handler
            .store
            .insert(&StoredMessage::with_timestamp(
                id,
                0,
                Bytes::from_static(b"msg"),
                1000,
            ))
            .await
            .unwrap();

        // Peer says they have same message
        let pull = handler.handle_sync_response(vec![id]).await;

        // Nothing missing
        assert!(pull.is_none());
    }

    #[tokio::test]
    async fn test_sync_pull() {
        let handler = make_handler().await;

        let id1 = MessageId::new();
        let id2 = MessageId::new();
        let id3 = MessageId::new();

        // Store some messages
        handler
            .store
            .insert(&StoredMessage::with_timestamp(
                id1,
                0,
                Bytes::from_static(b"msg1"),
                1000,
            ))
            .await
            .unwrap();
        handler
            .store
            .insert(&StoredMessage::with_timestamp(
                id2,
                0,
                Bytes::from_static(b"msg2"),
                2000,
            ))
            .await
            .unwrap();

        // Request id1, id2, id3 (id3 doesn't exist)
        let push = handler.handle_sync_pull(vec![id1, id2, id3]).await;

        // Should return id1 and id2
        assert_eq!(push.messages.len(), 2);
        let ids: Vec<_> = push.messages.iter().map(|m| m.id).collect();
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[tokio::test]
    async fn test_clear() {
        let handler = make_handler().await;

        handler.record_message(MessageId::new(), b"msg1");
        handler.record_message(MessageId::new(), b"msg2");

        assert_eq!(handler.len(), 2);

        handler.clear();

        assert!(handler.is_empty());
        assert_eq!(handler.root_hash(), [0u8; 32]);
    }
}
