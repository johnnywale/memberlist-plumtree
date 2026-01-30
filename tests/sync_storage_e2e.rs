//! End-to-End (E2E) Tests for Anti-Entropy Sync and Storage Features.
//!
//! These tests verify the storage backends and sync state management work correctly.
//!
//! # Test Scenarios
//!
//! 1. **MemoryStore Operations**: Insert, get, get_range, prune, pagination
//! 2. **SyncState XOR Hash**: O(1) insert/remove with hash consistency
//! 3. **SyncHandler Protocol Flow**: Request/Response/Pull/Push cycle
//! 4. **Anti-Entropy Sync Integration**: Full sync between nodes
//! 5. **SledStore Persistence**: (with storage-sled feature)

#![cfg(feature = "sync")]

use bytes::Bytes;
use memberlist_plumtree::storage::{MemoryStore, MessageStore, StoredMessage};
use memberlist_plumtree::sync::{SyncHandler, SyncState};
use memberlist_plumtree::MessageId;
use std::sync::Arc;

// ============================================================================
// Helper Functions
// ============================================================================

/// Create a test message with a specific timestamp.
fn make_message(ts: u64, payload: &[u8]) -> StoredMessage {
    StoredMessage::with_timestamp(MessageId::new(), 0, Bytes::copy_from_slice(payload), ts)
}

/// Create a test message with a specific ID and timestamp.
fn make_message_with_id(id: MessageId, ts: u64, payload: &[u8]) -> StoredMessage {
    StoredMessage::with_timestamp(id, 0, Bytes::copy_from_slice(payload), ts)
}

// ============================================================================
// Test Scenario 1: MemoryStore Basic Operations
// ============================================================================

/// Verify that MemoryStore correctly handles insert, get, and contains operations.
#[tokio::test]
async fn test_memory_store_basic_operations() {
    let store = MemoryStore::new(1000);

    // Create test messages
    let msg1 = make_message(1000, b"hello");
    let msg2 = make_message(2000, b"world");
    let id1 = msg1.id;
    let id2 = msg2.id;

    // Test initial state
    assert!(!store.contains(&id1).await.unwrap());
    assert!(!store.contains(&id2).await.unwrap());
    assert_eq!(store.count().await.unwrap(), 0);

    // Insert first message
    let was_new = store.insert(&msg1).await.unwrap();
    assert!(was_new, "First insert should return true");
    assert!(store.contains(&id1).await.unwrap());
    assert_eq!(store.count().await.unwrap(), 1);

    // Insert second message
    let was_new = store.insert(&msg2).await.unwrap();
    assert!(was_new, "Second insert should return true");
    assert!(store.contains(&id2).await.unwrap());
    assert_eq!(store.count().await.unwrap(), 2);

    // Duplicate insert should return false
    let was_new = store.insert(&msg1).await.unwrap();
    assert!(!was_new, "Duplicate insert should return false");
    assert_eq!(store.count().await.unwrap(), 2);

    // Get message
    let retrieved = store.get(&id1).await.unwrap().unwrap();
    assert_eq!(retrieved.id, id1);
    assert_eq!(retrieved.timestamp, 1000);
    assert_eq!(retrieved.payload.as_ref(), b"hello");

    // Get non-existent message
    let fake_id = MessageId::new();
    assert!(store.get(&fake_id).await.unwrap().is_none());

    println!("=== Test Passed: MemoryStore basic operations work correctly ===");
}

// ============================================================================
// Test Scenario 2: MemoryStore Range Queries
// ============================================================================

/// Verify that MemoryStore correctly handles get_range with pagination.
#[tokio::test]
async fn test_memory_store_range_queries() {
    let store = MemoryStore::new(1000);

    // Insert messages at different timestamps
    for ts in [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000] {
        let msg = make_message(ts, format!("msg-{}", ts).as_bytes());
        store.insert(&msg).await.unwrap();
    }

    assert_eq!(store.count().await.unwrap(), 10);

    // Get full range
    let (ids, has_more) = store.get_range(100, 1000, 100, 0).await.unwrap();
    assert_eq!(ids.len(), 10);
    assert!(!has_more);

    // Get partial range [300, 700]
    let (ids, has_more) = store.get_range(300, 700, 100, 0).await.unwrap();
    assert_eq!(ids.len(), 5, "Should have 5 messages in [300, 700]");
    assert!(!has_more);

    // Test pagination with limit
    let (ids, has_more) = store.get_range(100, 1000, 3, 0).await.unwrap();
    assert_eq!(ids.len(), 3);
    assert!(has_more, "Should have more results");

    // Test pagination with offset
    let (ids, has_more) = store.get_range(100, 1000, 3, 3).await.unwrap();
    assert_eq!(ids.len(), 3);
    assert!(has_more);

    // Test pagination at end
    let (ids, has_more) = store.get_range(100, 1000, 3, 9).await.unwrap();
    assert_eq!(ids.len(), 1);
    assert!(!has_more);

    // Empty range
    let (ids, has_more) = store.get_range(2000, 3000, 100, 0).await.unwrap();
    assert_eq!(ids.len(), 0);
    assert!(!has_more);

    println!("=== Test Passed: MemoryStore range queries work correctly ===");
}

// ============================================================================
// Test Scenario 3: MemoryStore Pruning
// ============================================================================

/// Verify that MemoryStore correctly prunes old messages.
#[tokio::test]
async fn test_memory_store_pruning() {
    let store = MemoryStore::new(1000);

    // Insert messages at different timestamps
    for ts in [100, 200, 300, 400, 500] {
        let msg = make_message(ts, format!("msg-{}", ts).as_bytes());
        store.insert(&msg).await.unwrap();
    }

    assert_eq!(store.count().await.unwrap(), 5);

    // Prune messages older than 300 (removes 100, 200)
    let removed = store.prune(300).await.unwrap();
    assert_eq!(removed, 2, "Should remove 2 messages");
    assert_eq!(store.count().await.unwrap(), 3);

    // Verify remaining messages
    let (ids, _) = store.get_range(100, 500, 100, 0).await.unwrap();
    assert_eq!(ids.len(), 3, "Should have 3 messages remaining");

    // Prune more
    let removed = store.prune(450).await.unwrap();
    assert_eq!(removed, 2, "Should remove 2 more messages (300, 400)");
    assert_eq!(store.count().await.unwrap(), 1);

    // Prune all
    let removed = store.prune(1000).await.unwrap();
    assert_eq!(removed, 1, "Should remove last message");
    assert_eq!(store.count().await.unwrap(), 0);

    println!("=== Test Passed: MemoryStore pruning works correctly ===");
}

// ============================================================================
// Test Scenario 4: MemoryStore Eviction
// ============================================================================

/// Verify that MemoryStore evicts oldest messages when at capacity.
#[tokio::test]
async fn test_memory_store_eviction() {
    let store = MemoryStore::new(5); // Small capacity for testing

    // Insert messages up to capacity
    let mut ids = Vec::new();
    for ts in [100, 200, 300, 400, 500] {
        let msg = make_message(ts, format!("msg-{}", ts).as_bytes());
        ids.push(msg.id);
        store.insert(&msg).await.unwrap();
    }

    assert_eq!(store.count().await.unwrap(), 5);
    assert!(
        store.contains(&ids[0]).await.unwrap(),
        "Oldest should exist"
    );

    // Insert one more - should evict oldest
    let new_msg = make_message(600, b"new");
    store.insert(&new_msg).await.unwrap();

    // Count should stay at capacity
    assert_eq!(store.count().await.unwrap(), 5);

    // Oldest should be evicted
    assert!(
        !store.contains(&ids[0]).await.unwrap(),
        "Oldest should be evicted"
    );

    // Newest should exist
    assert!(store.contains(&new_msg.id).await.unwrap());

    println!("=== Test Passed: MemoryStore eviction works correctly ===");
}

// ============================================================================
// Test Scenario 5: SyncState XOR Hash Operations
// ============================================================================

/// Verify that SyncState correctly computes XOR-based hashes.
#[test]
fn test_sync_state_xor_hash() {
    let mut state = SyncState::new();

    // Initial state should have zero hash
    assert!(state.is_empty());
    assert_eq!(state.len(), 0);
    assert_eq!(state.root_hash(), [0u8; 32]);

    // Insert first message
    let id1 = MessageId::new();
    state.insert(id1, b"hello");

    assert!(!state.is_empty());
    assert_eq!(state.len(), 1);
    let hash1 = state.root_hash();
    assert_ne!(hash1, [0u8; 32], "Hash should change after insert");

    // Insert second message
    let id2 = MessageId::new();
    state.insert(id2, b"world");

    assert_eq!(state.len(), 2);
    let hash2 = state.root_hash();
    assert_ne!(hash2, hash1, "Hash should change after second insert");

    // XOR property: inserting then removing should return to previous hash
    state.remove(&id2);
    assert_eq!(state.len(), 1);
    assert_eq!(
        state.root_hash(),
        hash1,
        "Hash should return to previous after remove"
    );

    // Remove all
    state.remove(&id1);
    assert!(state.is_empty());
    assert_eq!(
        state.root_hash(),
        [0u8; 32],
        "Hash should be zero when empty"
    );

    println!("=== Test Passed: SyncState XOR hash operations work correctly ===");
}

// ============================================================================
// Test Scenario 6: SyncState Commutativity
// ============================================================================

/// Verify that SyncState hash is commutative (order-independent).
#[test]
fn test_sync_state_commutativity() {
    let id1 = MessageId::new();
    let id2 = MessageId::new();
    let id3 = MessageId::new();

    // Insert in order 1, 2, 3
    let mut state_a = SyncState::new();
    state_a.insert(id1, b"msg1");
    state_a.insert(id2, b"msg2");
    state_a.insert(id3, b"msg3");

    // Insert in order 3, 1, 2
    let mut state_b = SyncState::new();
    state_b.insert(id3, b"msg3");
    state_b.insert(id1, b"msg1");
    state_b.insert(id2, b"msg2");

    // Insert in order 2, 3, 1
    let mut state_c = SyncState::new();
    state_c.insert(id2, b"msg2");
    state_c.insert(id3, b"msg3");
    state_c.insert(id1, b"msg1");

    // All should have the same hash
    assert_eq!(
        state_a.root_hash(),
        state_b.root_hash(),
        "Hash should be commutative"
    );
    assert_eq!(
        state_b.root_hash(),
        state_c.root_hash(),
        "Hash should be commutative"
    );

    println!("=== Test Passed: SyncState hash is commutative ===");
}

// ============================================================================
// Test Scenario 7: SyncState Duplicate Handling
// ============================================================================

/// Verify that SyncState correctly handles duplicate inserts.
#[test]
fn test_sync_state_duplicates() {
    let mut state = SyncState::new();

    let id1 = MessageId::new();
    state.insert(id1, b"hello");

    let hash_after_first = state.root_hash();
    assert_eq!(state.len(), 1);

    // Insert same ID again - should be a no-op
    state.insert(id1, b"hello");
    assert_eq!(state.len(), 1, "Duplicate insert should not increase count");
    assert_eq!(
        state.root_hash(),
        hash_after_first,
        "Hash should not change on duplicate"
    );

    // Insert same ID with different payload - should still be a no-op
    state.insert(id1, b"different payload");
    assert_eq!(state.len(), 1);
    assert_eq!(state.root_hash(), hash_after_first);

    println!("=== Test Passed: SyncState duplicate handling works correctly ===");
}

// ============================================================================
// Test Scenario 8: SyncState Contains and Message IDs
// ============================================================================

/// Verify that SyncState correctly tracks contained message IDs.
#[test]
fn test_sync_state_contains_and_ids() {
    let mut state = SyncState::new();

    let id1 = MessageId::new();
    let id2 = MessageId::new();
    let id3 = MessageId::new();

    // Initially empty
    assert!(!state.contains(&id1));
    assert!(!state.contains(&id2));

    // Insert messages
    state.insert(id1, b"msg1");
    state.insert(id2, b"msg2");

    // Check contains
    assert!(state.contains(&id1));
    assert!(state.contains(&id2));
    assert!(!state.contains(&id3));

    // Check message_ids
    let ids = state.message_ids();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&id1));
    assert!(ids.contains(&id2));

    // Remove and check
    state.remove(&id1);
    assert!(!state.contains(&id1));
    assert!(state.contains(&id2));

    let ids = state.message_ids();
    assert_eq!(ids.len(), 1);
    assert!(!ids.contains(&id1));
    assert!(ids.contains(&id2));

    println!("=== Test Passed: SyncState contains and message_ids work correctly ===");
}

// ============================================================================
// Test Scenario 9: SyncHandler Basic Flow
// ============================================================================

/// Verify that SyncHandler correctly handles sync request/response flow.
#[tokio::test]
async fn test_sync_handler_basic_flow() {
    // Create two stores with different messages
    let store_a = Arc::new(MemoryStore::new(1000));
    let store_b = Arc::new(MemoryStore::new(1000));

    // Create handlers
    let handler_a = SyncHandler::new(store_a.clone());
    let handler_b = SyncHandler::new(store_b.clone());

    // Add common message to both
    let common_id = MessageId::new();
    let common_msg = make_message_with_id(common_id, 100, b"common");
    store_a.insert(&common_msg).await.unwrap();
    store_b.insert(&common_msg).await.unwrap();
    handler_a.record_message(common_id, b"common");
    handler_b.record_message(common_id, b"common");

    // Add unique message to A
    let unique_a_id = MessageId::new();
    let unique_a_msg = make_message_with_id(unique_a_id, 200, b"unique-a");
    store_a.insert(&unique_a_msg).await.unwrap();
    handler_a.record_message(unique_a_id, b"unique-a");

    // Add unique message to B
    let unique_b_id = MessageId::new();
    let unique_b_msg = make_message_with_id(unique_b_id, 300, b"unique-b");
    store_b.insert(&unique_b_msg).await.unwrap();
    handler_b.record_message(unique_b_id, b"unique-b");

    // Verify root hashes are different
    let hash_a = handler_a.root_hash();
    let hash_b = handler_b.root_hash();
    assert_ne!(hash_a, hash_b, "Hashes should differ when content differs");

    // A sends sync request to B
    let response = handler_b.handle_sync_request(hash_a, (0, 1000)).await;

    assert!(!response.matches, "Response should indicate mismatch");
    assert!(
        !response.message_ids.is_empty(),
        "Response should contain message IDs"
    );

    // A determines what it's missing
    let pull = handler_a.handle_sync_response(response.message_ids).await;
    assert!(pull.is_some(), "Should have messages to pull");

    let pull = pull.unwrap();
    assert!(
        pull.message_ids.contains(&unique_b_id),
        "Should need unique-b"
    );

    // B sends the requested messages
    let push = handler_b.handle_sync_pull(pull.message_ids).await;
    assert_eq!(push.messages.len(), 1);
    assert_eq!(push.messages[0].id, unique_b_id);

    println!("=== Test Passed: SyncHandler basic flow works correctly ===");
}

// ============================================================================
// Test Scenario 10: SyncHandler Matching State
// ============================================================================

/// Verify that SyncHandler correctly identifies matching states.
#[tokio::test]
async fn test_sync_handler_matching_state() {
    let store_a = Arc::new(MemoryStore::new(1000));
    let store_b = Arc::new(MemoryStore::new(1000));

    let handler_a = SyncHandler::new(store_a.clone());
    let handler_b = SyncHandler::new(store_b.clone());

    // Add same messages to both
    for i in 0..5 {
        let id = MessageId::new();
        let payload = format!("msg-{}", i);
        let msg = make_message_with_id(id, i as u64 * 100, payload.as_bytes());

        store_a.insert(&msg).await.unwrap();
        store_b.insert(&msg).await.unwrap();
        handler_a.record_message(id, payload.as_bytes());
        handler_b.record_message(id, payload.as_bytes());
    }

    // Verify hashes match
    let hash_a = handler_a.root_hash();
    let hash_b = handler_b.root_hash();
    assert_eq!(hash_a, hash_b, "Hashes should match when content is same");

    // Sync request should show match
    let response = handler_b.handle_sync_request(hash_a, (0, 1000)).await;
    assert!(response.matches, "Response should indicate match");
    assert!(
        response.message_ids.is_empty(),
        "No IDs needed when matched"
    );

    println!("=== Test Passed: SyncHandler correctly identifies matching states ===");
}

// ============================================================================
// Test Scenario 11: SyncHandler Remove Message
// ============================================================================

/// Verify that SyncHandler correctly handles message removal.
#[tokio::test]
async fn test_sync_handler_remove_message() {
    let store = Arc::new(MemoryStore::new(1000));
    let handler = SyncHandler::new(store.clone());

    // Add messages
    let id1 = MessageId::new();
    let id2 = MessageId::new();

    let msg1 = make_message_with_id(id1, 100, b"msg1");
    let msg2 = make_message_with_id(id2, 200, b"msg2");

    store.insert(&msg1).await.unwrap();
    store.insert(&msg2).await.unwrap();
    handler.record_message(id1, b"msg1");
    handler.record_message(id2, b"msg2");

    let hash_with_both = handler.root_hash();

    // Remove one message
    handler.remove_message(&id2);

    let hash_with_one = handler.root_hash();
    assert_ne!(
        hash_with_both, hash_with_one,
        "Hash should change after removal"
    );

    // Remove the other
    handler.remove_message(&id1);

    assert_eq!(
        handler.root_hash(),
        [0u8; 32],
        "Hash should be zero when empty"
    );

    println!("=== Test Passed: SyncHandler remove message works correctly ===");
}

// ============================================================================
// Test Scenario 12: SyncState Rebuild
// ============================================================================

/// Verify that SyncState can be rebuilt from scratch.
#[test]
fn test_sync_state_rebuild() {
    let mut state = SyncState::new();

    // Add some messages
    let id1 = MessageId::new();
    let id2 = MessageId::new();
    let id3 = MessageId::new();

    state.insert(id1, b"msg1");
    state.insert(id2, b"msg2");
    state.insert(id3, b"msg3");

    let original_hash = state.root_hash();
    assert_eq!(state.len(), 3);

    // Rebuild with same messages
    let messages: Vec<(MessageId, &[u8])> = vec![
        (id1, b"msg1" as &[u8]),
        (id2, b"msg2" as &[u8]),
        (id3, b"msg3" as &[u8]),
    ];
    state.rebuild(messages.into_iter());

    assert_eq!(state.len(), 3);
    assert_eq!(
        state.root_hash(),
        original_hash,
        "Rebuilt hash should match"
    );

    // Rebuild with fewer messages
    let messages: Vec<(MessageId, &[u8])> = vec![(id1, b"msg1" as &[u8])];
    state.rebuild(messages.into_iter());

    assert_eq!(state.len(), 1);
    assert_ne!(state.root_hash(), original_hash);

    println!("=== Test Passed: SyncState rebuild works correctly ===");
}

// ============================================================================
// Test Scenario 13: Large Scale Storage
// ============================================================================

/// Verify that MemoryStore handles large numbers of messages efficiently.
#[tokio::test]
async fn test_memory_store_large_scale() {
    let store = MemoryStore::new(10000);
    let count = 5000;

    // Insert many messages
    let mut ids = Vec::new();
    for i in 0..count {
        let msg = make_message(i as u64, format!("msg-{}", i).as_bytes());
        ids.push(msg.id);
        store.insert(&msg).await.unwrap();
    }

    assert_eq!(store.count().await.unwrap(), count);

    // Range query should work
    // Note: timestamps are 0-4999, so [1000, 1999] gives 1000 messages
    let (range_ids, _) = store.get_range(1000, 1999, 1000, 0).await.unwrap();
    assert_eq!(
        range_ids.len(),
        1000,
        "Should have 1000 messages in [1000, 1999]"
    );

    // Prune half
    let removed = store.prune(2500).await.unwrap();
    assert_eq!(removed, 2500);
    assert_eq!(store.count().await.unwrap(), 2500);

    println!("=== Test Passed: MemoryStore handles large scale correctly ===");
}

// ============================================================================
// Test Scenario 14: Concurrent Store Access
// ============================================================================

/// Verify that MemoryStore is thread-safe under concurrent access.
#[tokio::test]
async fn test_memory_store_concurrent_access() {
    let store = Arc::new(MemoryStore::new(10000));
    let mut handles = Vec::new();

    // Spawn multiple tasks that insert messages
    for task_id in 0..10 {
        let store = store.clone();
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let ts = (task_id * 1000 + i) as u64;
                let msg = make_message(ts, format!("task-{}-msg-{}", task_id, i).as_bytes());
                store.insert(&msg).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Should have all messages
    assert_eq!(store.count().await.unwrap(), 1000);

    println!("=== Test Passed: MemoryStore is thread-safe ===");
}

// ============================================================================
// Test Scenario 15: SyncHandler Pagination
// ============================================================================

/// Verify that SyncHandler respects pagination limits.
#[tokio::test]
async fn test_sync_handler_pagination() {
    let store = Arc::new(MemoryStore::new(1000));
    let handler = SyncHandler::new(store.clone());

    // Add many messages
    for i in 0..200 {
        let id = MessageId::new();
        let payload = format!("msg-{}", i);
        let msg = make_message_with_id(id, i as u64, payload.as_bytes());
        store.insert(&msg).await.unwrap();
        handler.record_message(id, payload.as_bytes());
    }

    // Sync request with different hash should return paginated results
    let fake_hash = [1u8; 32]; // Different from actual hash
    let response = handler.handle_sync_request(fake_hash, (0, 199)).await;

    assert!(!response.matches);
    // MAX_SYNC_IDS_PER_RESPONSE is 100
    assert!(
        response.message_ids.len() <= 100,
        "Should respect pagination limit"
    );
    assert!(response.has_more, "Should indicate more results available");

    println!("=== Test Passed: SyncHandler respects pagination limits ===");
}

// ============================================================================
// Sled Backend Tests (feature-gated)
// ============================================================================

#[cfg(feature = "storage-sled")]
mod sled_tests {
    use super::*;
    use memberlist_plumtree::storage::SledStore;
    use tempfile::tempdir;

    /// Verify that SledStore correctly handles basic operations.
    #[tokio::test]
    async fn test_sled_store_basic_operations() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

        let msg = make_message(1000, b"hello");
        let id = msg.id;

        // Insert
        let was_new = store.insert(&msg).await.unwrap();
        assert!(was_new);

        // Contains
        assert!(store.contains(&id).await.unwrap());

        // Get
        let retrieved = store.get(&id).await.unwrap().unwrap();
        assert_eq!(retrieved.id, id);
        assert_eq!(retrieved.timestamp, 1000);

        // Count
        assert_eq!(store.count().await.unwrap(), 1);

        println!("=== Test Passed: SledStore basic operations work correctly ===");
    }

    /// Verify that SledStore persists data across restarts.
    #[tokio::test]
    async fn test_sled_store_persistence() {
        let dir = tempdir().unwrap();
        let msg = make_message(1000, b"persistent");
        let id = msg.id;

        // Insert and close
        {
            let store = SledStore::open(dir.path()).unwrap();
            store.insert(&msg).await.unwrap();
            store.flush().await.unwrap();
        }

        // Reopen and verify
        {
            let store = SledStore::open(dir.path()).unwrap();
            assert!(store.contains(&id).await.unwrap());
            let retrieved = store.get(&id).await.unwrap().unwrap();
            assert_eq!(retrieved.payload.as_ref(), b"persistent");
        }

        println!("=== Test Passed: SledStore persistence works correctly ===");
    }

    /// Verify that SledStore correctly handles range queries.
    #[tokio::test]
    async fn test_sled_store_range_queries() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

        // Insert messages at different timestamps
        for ts in [100, 200, 300, 400, 500] {
            let msg = make_message(ts, format!("msg-{}", ts).as_bytes());
            store.insert(&msg).await.unwrap();
        }

        // Get range [200, 400]
        let (ids, has_more) = store.get_range(200, 400, 10, 0).await.unwrap();
        assert_eq!(ids.len(), 3);
        assert!(!has_more);

        // Test pagination
        let (ids, has_more) = store.get_range(100, 500, 2, 0).await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(has_more);

        println!("=== Test Passed: SledStore range queries work correctly ===");
    }

    /// Verify that SledStore correctly handles pruning.
    #[tokio::test]
    async fn test_sled_store_pruning() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

        // Insert messages
        for ts in [100, 200, 300, 400, 500] {
            let msg = make_message(ts, format!("msg-{}", ts).as_bytes());
            store.insert(&msg).await.unwrap();
        }

        assert_eq!(store.count().await.unwrap(), 5);

        // Prune old messages
        let removed = store.prune(300).await.unwrap();
        assert_eq!(removed, 2);
        assert_eq!(store.count().await.unwrap(), 3);

        println!("=== Test Passed: SledStore pruning works correctly ===");
    }
}
