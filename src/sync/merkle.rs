//! XOR-based sync state for fast set comparison.
//!
//! Uses XOR of SHA-256 hashes for O(1) insert/remove operations.
//!
//! # XOR Hash Properties
//!
//! - **Commutative**: Order doesn't matter
//! - **Self-inverse**: `a ^ a = 0`, enabling O(1) removal
//! - **Collision probability**: 1/2^256 (negligible)
//!
//! # Security Note
//!
//! This approach is suitable for trusted P2P mesh networks. For networks
//! with untrusted nodes, consider using a proper Merkle Tree that can
//! detect malicious hash manipulation.

#[cfg(feature = "sync")]
use sha2::{Digest, Sha256};

use crate::MessageId;
use std::collections::HashMap;

/// Sync state using XOR-based hash for fast set comparison.
///
/// XOR is commutative and its own inverse, allowing O(1) updates:
/// - Insert: `new_root = old_root ^ new_hash`
/// - Remove: `new_root = old_root ^ deleted_hash`
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::sync::SyncState;
///
/// let mut state = SyncState::new();
///
/// // O(1) insert
/// state.insert(msg_id, payload);
///
/// // O(1) remove
/// state.remove(&msg_id);
///
/// // Compare root hashes for set equality
/// if state.root_hash() == other_state.root_hash() {
///     println!("Sets are identical!");
/// }
/// ```
#[derive(Debug, Clone)]
pub struct SyncState {
    /// Message ID -> leaf hash (for O(1) removal)
    leaves: HashMap<MessageId, [u8; 32]>,
    /// XOR of all leaf hashes - O(1) update
    root_hash: [u8; 32],
}

impl SyncState {
    /// Create a new empty sync state.
    pub fn new() -> Self {
        Self {
            leaves: HashMap::new(),
            root_hash: [0u8; 32],
        }
    }

    /// Create a new sync state with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            leaves: HashMap::with_capacity(capacity),
            root_hash: [0u8; 32],
        }
    }

    /// O(1) insert - XOR new hash into root.
    ///
    /// Computes SHA-256(message_id || payload) and XORs it into the root hash.
    /// If the message ID already exists, this is a no-op.
    #[cfg(feature = "sync")]
    pub fn insert(&mut self, id: MessageId, payload: &[u8]) {
        // Don't insert duplicates
        if self.leaves.contains_key(&id) {
            return;
        }

        // Compute leaf hash: SHA-256(id || payload)
        let mut hasher = Sha256::new();
        hasher.update(id.encode_to_bytes());
        hasher.update(payload);
        let hash: [u8; 32] = hasher.finalize().into();

        // XOR into root (O(1))
        for (dest, src) in self.root_hash.iter_mut().zip(hash.iter()) {
            *dest ^= src;
        }

        self.leaves.insert(id, hash);
    }

    /// O(1) insert - Stub when sync feature is not enabled.
    #[cfg(not(feature = "sync"))]
    pub fn insert(&mut self, _id: MessageId, _payload: &[u8]) {
        // No-op without sync feature
    }

    /// O(1) remove - XOR out the hash (XOR is its own inverse).
    ///
    /// If the message ID doesn't exist, this is a no-op.
    pub fn remove(&mut self, id: &MessageId) {
        if let Some(hash) = self.leaves.remove(id) {
            // XOR out of root (O(1))
            for (dest, src) in self.root_hash.iter_mut().zip(hash.iter()) {
                *dest ^= src;
            }
        }
    }

    /// Get the current root hash.
    ///
    /// This hash represents the XOR of all leaf hashes. Two sync states
    /// with the same set of messages will have the same root hash.
    pub fn root_hash(&self) -> [u8; 32] {
        self.root_hash
    }

    /// Get the number of messages in the sync state.
    pub fn len(&self) -> usize {
        self.leaves.len()
    }

    /// Check if the sync state is empty.
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Get all message IDs in this sync state.
    pub fn message_ids(&self) -> Vec<MessageId> {
        self.leaves.keys().copied().collect()
    }

    /// Check if a message is in the sync state.
    pub fn contains(&self, id: &MessageId) -> bool {
        self.leaves.contains_key(id)
    }

    /// Rebuild from scratch (use sparingly - O(n)).
    ///
    /// Clears the current state and recomputes from the provided messages.
    #[cfg(feature = "sync")]
    pub fn rebuild<'a>(&mut self, messages: impl Iterator<Item = (MessageId, &'a [u8])>) {
        self.leaves.clear();
        self.root_hash = [0u8; 32];

        for (id, payload) in messages {
            self.insert(id, payload);
        }
    }

    /// Rebuild stub when sync feature is not enabled.
    #[cfg(not(feature = "sync"))]
    pub fn rebuild<'a>(&mut self, _messages: impl Iterator<Item = (MessageId, &'a [u8])>) {
        // No-op without sync feature
    }

    /// Clear all messages from the sync state.
    pub fn clear(&mut self) {
        self.leaves.clear();
        self.root_hash = [0u8; 32];
    }

    /// Compute the difference between this state and another.
    ///
    /// Returns message IDs that are in this state but not in the other.
    /// This is a O(n) operation that requires comparing all IDs.
    pub fn diff(&self, other: &SyncState) -> Vec<MessageId> {
        self.leaves
            .keys()
            .filter(|id| !other.contains(id))
            .copied()
            .collect()
    }
}

impl Default for SyncState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_empty_state() {
        let state = SyncState::new();
        assert!(state.is_empty());
        assert_eq!(state.len(), 0);
        assert_eq!(state.root_hash(), [0u8; 32]);
    }

    #[test]
    fn test_insert_single() {
        let mut state = SyncState::new();
        let id = MessageId::new();
        let payload = b"hello world";

        state.insert(id, payload);

        assert_eq!(state.len(), 1);
        assert!(state.contains(&id));
        // Root hash should not be all zeros after insert
        assert_ne!(state.root_hash(), [0u8; 32]);
    }

    #[test]
    fn test_insert_duplicate_is_noop() {
        let mut state = SyncState::new();
        let id = MessageId::new();
        let payload = b"hello";

        state.insert(id, payload);
        let hash_after_first = state.root_hash();

        // Insert same ID again - should be no-op
        state.insert(id, payload);

        assert_eq!(state.len(), 1);
        assert_eq!(state.root_hash(), hash_after_first);
    }

    #[test]
    fn test_remove() {
        let mut state = SyncState::new();
        let id = MessageId::new();
        let payload = b"hello";

        state.insert(id, payload);
        assert_eq!(state.len(), 1);

        state.remove(&id);
        assert_eq!(state.len(), 0);
        assert!(!state.contains(&id));
        // Root hash should return to zero after removing only element
        assert_eq!(state.root_hash(), [0u8; 32]);
    }

    #[test]
    fn test_remove_nonexistent_is_noop() {
        let mut state = SyncState::new();
        let id = MessageId::new();

        // Remove from empty state - should not panic
        state.remove(&id);
        assert_eq!(state.len(), 0);
    }

    #[test]
    fn test_order_independence() {
        let id1 = MessageId::new();
        let id2 = MessageId::new();
        let payload1 = b"msg1";
        let payload2 = b"msg2";

        // Insert in order 1, 2
        let mut state_a = SyncState::new();
        state_a.insert(id1, payload1);
        state_a.insert(id2, payload2);

        // Insert in order 2, 1
        let mut state_b = SyncState::new();
        state_b.insert(id2, payload2);
        state_b.insert(id1, payload1);

        // Root hashes should be equal (XOR is commutative)
        assert_eq!(state_a.root_hash(), state_b.root_hash());
    }

    #[test]
    fn test_same_content_different_ids() {
        let id1 = MessageId::new();
        let id2 = MessageId::new();
        let payload = b"same payload";

        let mut state_a = SyncState::new();
        state_a.insert(id1, payload);

        let mut state_b = SyncState::new();
        state_b.insert(id2, payload);

        // Different IDs should produce different hashes
        assert_ne!(state_a.root_hash(), state_b.root_hash());
    }

    #[test]
    fn test_message_ids() {
        let mut state = SyncState::new();
        let id1 = MessageId::new();
        let id2 = MessageId::new();

        state.insert(id1, b"msg1");
        state.insert(id2, b"msg2");

        let ids = state.message_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn test_clear() {
        let mut state = SyncState::new();
        state.insert(MessageId::new(), b"msg1");
        state.insert(MessageId::new(), b"msg2");

        assert_eq!(state.len(), 2);

        state.clear();

        assert!(state.is_empty());
        assert_eq!(state.root_hash(), [0u8; 32]);
    }

    #[test]
    fn test_rebuild() {
        let mut state = SyncState::new();
        let id1 = MessageId::new();
        let id2 = MessageId::new();
        let payload1 = Bytes::from_static(b"msg1");
        let payload2 = Bytes::from_static(b"msg2");

        // Initial state
        state.insert(id1, payload1.as_ref());
        let hash_with_one = state.root_hash();

        // Rebuild with both messages
        state.rebuild([(id1, payload1.as_ref()), (id2, payload2.as_ref())].into_iter());

        assert_eq!(state.len(), 2);
        assert!(state.contains(&id1));
        assert!(state.contains(&id2));
        // Hash should be different with two messages
        assert_ne!(state.root_hash(), hash_with_one);
    }

    #[test]
    fn test_diff() {
        let mut state_a = SyncState::new();
        let mut state_b = SyncState::new();

        let id1 = MessageId::new();
        let id2 = MessageId::new();
        let id3 = MessageId::new();

        // State A has id1, id2
        state_a.insert(id1, b"msg1");
        state_a.insert(id2, b"msg2");

        // State B has id2, id3
        state_b.insert(id2, b"msg2");
        state_b.insert(id3, b"msg3");

        // A - B should be {id1}
        let diff_a = state_a.diff(&state_b);
        assert_eq!(diff_a.len(), 1);
        assert!(diff_a.contains(&id1));

        // B - A should be {id3}
        let diff_b = state_b.diff(&state_a);
        assert_eq!(diff_b.len(), 1);
        assert!(diff_b.contains(&id3));
    }
}
