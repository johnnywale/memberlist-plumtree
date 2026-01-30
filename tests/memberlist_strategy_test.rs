//! End-to-End Tests for MemberlistSyncStrategy.
//!
//! These tests verify that the `MemberlistSyncStrategy` correctly:
//! 1. Integrates with `PlumtreeDiscovery` via `with_sync_strategy()`
//! 2. Returns proper local_state format (48 bytes: hash + timestamp + window)
//! 3. Triggers sync requests on hash mismatch in merge_remote_state()
//! 4. Does NOT run a background task (uses memberlist push-pull instead)
//! 5. Handles sync messages correctly (Request/Response/Pull/Push)

#![cfg(all(feature = "tokio", feature = "sync"))]

use bytes::{Buf, BufMut, Bytes, BytesMut};
use memberlist_plumtree::storage::{current_time_ms, MemoryStore, MessageStore};
use memberlist_plumtree::sync::{MemberlistSyncStrategy, SyncHandler, SyncStrategy};
use memberlist_plumtree::{
    IdCodec, MessageId, PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery, StorageConfig,
    SyncConfig, SyncMessage,
};
use nodecraft::CheapClone;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

// ============================================================================
// Test Node ID
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TestId(u64);

impl CheapClone for TestId {}

impl fmt::Display for TestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestId({})", self.0)
    }
}

impl IdCodec for TestId {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 8 {
            Some(TestId(buf.get_u64()))
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        8
    }
}

// ============================================================================
// Tracking Delegate
// ============================================================================

#[derive(Debug, Default, Clone)]
struct TrackingDelegate {
    delivered: Arc<Mutex<Vec<(MessageId, Bytes)>>>,
    delivered_ids: Arc<Mutex<HashSet<MessageId>>>,
}

#[allow(dead_code)]
impl TrackingDelegate {
    fn new() -> Self {
        Self::default()
    }

    fn delivered_count(&self) -> usize {
        self.delivered.lock().len()
    }

    fn has_message(&self, id: &MessageId) -> bool {
        self.delivered_ids.lock().contains(id)
    }

    fn get_payloads(&self) -> Vec<Bytes> {
        self.delivered
            .lock()
            .iter()
            .map(|(_, p)| p.clone())
            .collect()
    }
}

impl PlumtreeDelegate<TestId> for TrackingDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.delivered.lock().push((message_id, payload.clone()));
        self.delivered_ids.lock().insert(message_id);
    }
}

// ============================================================================
// Unit Tests for MemberlistSyncStrategy
// ============================================================================

#[tokio::test]
async fn test_memberlist_strategy_does_not_need_background_task() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, _rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // MemberlistSyncStrategy should NOT need a background task
    assert!(!strategy.needs_background_task());
    assert!(strategy.is_enabled());
}

#[tokio::test]
async fn test_memberlist_strategy_local_state_is_48_bytes() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, _rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    let state = strategy.local_state().await;

    // Should be exactly 48 bytes: 32 (hash) + 8 (timestamp) + 8 (window)
    assert_eq!(state.len(), 48);
}

#[tokio::test]
async fn test_memberlist_strategy_local_state_contains_root_hash() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, _rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Record a message to change the hash
    let id = MessageId::new();
    strategy.record_message(id, b"test payload");

    let state = strategy.local_state().await;
    let hash_from_state: [u8; 32] = state[0..32].try_into().unwrap();

    // The hash in local_state should match root_hash()
    assert_eq!(hash_from_state, strategy.root_hash());

    // And it should NOT be all zeros (since we recorded a message)
    assert_ne!(hash_from_state, [0u8; 32]);
}

#[tokio::test]
async fn test_memberlist_strategy_merge_triggers_sync_on_mismatch() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Record a message so our hash is non-zero
    let id = MessageId::new();
    strategy.record_message(id, b"test");

    // Create remote state with DIFFERENT hash (zeros)
    let mut remote_state = BytesMut::with_capacity(48);
    remote_state.put_slice(&[0u8; 32]); // Different hash
    remote_state.put_u64(current_time_ms());
    remote_state.put_u64(90_000); // 90s window

    // Merge should trigger sync request
    strategy
        .merge_remote_state(&remote_state, || Some(TestId(42)))
        .await;

    // Should have received sync request for peer 42
    let peer = rx.try_recv().expect("should have received sync request");
    assert_eq!(peer, TestId(42));
}

#[tokio::test]
async fn test_memberlist_strategy_merge_no_sync_on_match() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Get our current hash
    let local_hash = strategy.root_hash();

    // Create remote state with SAME hash
    let mut remote_state = BytesMut::with_capacity(48);
    remote_state.put_slice(&local_hash);
    remote_state.put_u64(current_time_ms());
    remote_state.put_u64(90_000);

    // Merge should NOT trigger sync request
    strategy
        .merge_remote_state(&remote_state, || Some(TestId(42)))
        .await;

    // Should NOT have received any sync request
    assert!(
        rx.try_recv().is_err(),
        "should NOT have sync request when hashes match"
    );
}

#[tokio::test]
async fn test_memberlist_strategy_merge_ignores_short_buffer() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Create invalid remote state (too short)
    let short_state = vec![0u8; 32]; // Only 32 bytes, needs 48

    // Merge should ignore invalid state
    strategy
        .merge_remote_state(&short_state, || Some(TestId(42)))
        .await;

    // Should NOT have triggered sync
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn test_memberlist_strategy_merge_no_peer_resolver() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Record a message so hashes differ
    strategy.record_message(MessageId::new(), b"test");

    // Create remote state with different hash
    let mut remote_state = BytesMut::with_capacity(48);
    remote_state.put_slice(&[0u8; 32]);
    remote_state.put_u64(current_time_ms());
    remote_state.put_u64(90_000);

    // Merge with peer_resolver returning None
    strategy.merge_remote_state(&remote_state, || None).await;

    // Should NOT have queued anything (no peer to sync with)
    assert!(rx.try_recv().is_err());
}

// ============================================================================
// Sync Message Handling Tests
// ============================================================================

#[tokio::test]
async fn test_memberlist_strategy_handles_sync_request() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store.clone()));
    let (tx, _rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Record some messages (both in sync state AND store)
    let id1 = MessageId::new();
    let id2 = MessageId::new();
    let payload1 = Bytes::from_static(b"message 1");
    let payload2 = Bytes::from_static(b"message 2");

    // Store messages in storage (required for handle_sync_request to return IDs)
    store
        .insert(&memberlist_plumtree::storage::StoredMessage::new(
            id1,
            0,
            payload1.clone(),
        ))
        .await
        .unwrap();
    store
        .insert(&memberlist_plumtree::storage::StoredMessage::new(
            id2,
            0,
            payload2.clone(),
        ))
        .await
        .unwrap();

    // Record in sync state (for hash tracking)
    strategy.record_message(id1, &payload1);
    strategy.record_message(id2, &payload2);

    // Handle sync request with different hash
    let request = SyncMessage::Request {
        root_hash: [0u8; 32], // Different from our hash
        time_start: 0,
        time_end: u64::MAX,
    };

    let result = strategy.handle_sync_message(TestId(1), request).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_some());

    if let Some(SyncMessage::Response {
        matches,
        message_ids,
        ..
    }) = response
    {
        assert!(!matches, "should not match since hashes differ");
        assert!(!message_ids.is_empty(), "should include message IDs");
    } else {
        panic!("expected SyncResponse");
    }
}

#[tokio::test]
async fn test_memberlist_strategy_handles_sync_request_with_matching_hash() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, _rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Get our hash (empty state = all zeros)
    let our_hash = strategy.root_hash();

    // Handle sync request with SAME hash
    let request = SyncMessage::Request {
        root_hash: our_hash,
        time_start: 0,
        time_end: u64::MAX,
    };

    let result = strategy.handle_sync_message(TestId(1), request).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_some());

    if let Some(SyncMessage::Response { matches, .. }) = response {
        assert!(matches, "should match since hashes are the same");
    } else {
        panic!("expected SyncResponse");
    }
}

#[tokio::test]
async fn test_memberlist_strategy_handles_sync_pull() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store.clone()));
    let (tx, _rx) = async_channel::bounded::<TestId>(64);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Store a message in the store
    let id = MessageId::new();
    let payload = Bytes::from_static(b"test payload");
    let msg = memberlist_plumtree::storage::StoredMessage::new(id, 0, payload.clone());
    store.insert(&msg).await.unwrap();
    strategy.record_message(id, &payload);

    // Handle sync pull requesting our message
    let pull = SyncMessage::Pull {
        message_ids: smallvec::smallvec![id],
    };

    let result = strategy.handle_sync_message(TestId(1), pull).await;

    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.is_some());

    if let Some(SyncMessage::Push { messages }) = response {
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].0, id);
        assert_eq!(messages[0].2, payload);
    } else {
        panic!("expected SyncPush");
    }
}

// ============================================================================
// Integration with PlumtreeDiscovery
// ============================================================================

#[tokio::test]
async fn test_memberlist_strategy_with_plumtree_discovery() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store.clone()));
    let (sync_tx, _sync_rx) = async_channel::bounded::<TestId>(64);

    let strategy =
        MemberlistSyncStrategy::new(sync_handler.clone(), Duration::from_secs(90), sync_tx);

    let config = PlumtreeConfig::default()
        .with_sync(SyncConfig::enabled())
        .with_storage(StorageConfig::enabled().with_max_messages(1000));

    let delegate = TrackingDelegate::new();

    // Create PlumtreeDiscovery with MemberlistSyncStrategy
    let pm = PlumtreeDiscovery::with_sync_strategy(
        TestId(1),
        config,
        delegate.clone(),
        store,
        sync_handler,
        strategy,
    );

    // Verify the strategy methods are accessible via PlumtreeDiscovery
    assert!(!pm.sync_needs_background_task());
    assert!(pm.sync_is_enabled());

    // Get local state through PlumtreeDiscovery
    let state = pm.sync_local_state().await;
    assert_eq!(state.len(), 48);
}

#[tokio::test]
async fn test_memberlist_strategy_integration_merge_triggers_sync() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store.clone()));
    let (sync_tx, sync_rx) = async_channel::bounded::<TestId>(64); // sync_rx is used below

    let strategy =
        MemberlistSyncStrategy::new(sync_handler.clone(), Duration::from_secs(90), sync_tx);

    let config = PlumtreeConfig::default()
        .with_sync(SyncConfig::enabled())
        .with_storage(StorageConfig::enabled().with_max_messages(1000));

    let delegate = TrackingDelegate::new();

    let pm = PlumtreeDiscovery::with_sync_strategy(
        TestId(1),
        config,
        delegate,
        store,
        sync_handler.clone(),
        strategy,
    );

    // Record a message to change hash
    sync_handler.record_message(MessageId::new(), b"test");

    // Create remote state with different hash
    let mut remote_state = BytesMut::with_capacity(48);
    remote_state.put_slice(&[0u8; 32]);
    remote_state.put_u64(current_time_ms());
    remote_state.put_u64(90_000);

    // Merge via PlumtreeDiscovery API
    pm.sync_merge_remote_state(&remote_state, || Some(TestId(42)))
        .await;

    // Should have triggered sync request
    let peer = sync_rx.try_recv().expect("should have sync request");
    assert_eq!(peer, TestId(42));
}

// ============================================================================
// Two Node Sync Simulation
// ============================================================================

#[tokio::test]
async fn test_memberlist_strategy_two_node_sync_simulation() {
    // Simulate two nodes with MemberlistSyncStrategy doing push-pull sync

    // Node 1 setup
    let store1 = Arc::new(MemoryStore::new(1000));
    let sync_handler1 = Arc::new(SyncHandler::new(store1.clone()));
    let (sync_tx1, _sync_rx1) = async_channel::bounded::<TestId>(64);
    let strategy1 =
        MemberlistSyncStrategy::new(sync_handler1.clone(), Duration::from_secs(90), sync_tx1);

    // Node 2 setup
    let store2 = Arc::new(MemoryStore::new(1000));
    let sync_handler2 = Arc::new(SyncHandler::new(store2.clone()));
    let (sync_tx2, sync_rx2) = async_channel::bounded::<TestId>(64);
    let strategy2 =
        MemberlistSyncStrategy::new(sync_handler2.clone(), Duration::from_secs(90), sync_tx2);

    // Node 1 has a message that Node 2 doesn't have
    let msg_id = MessageId::new();
    let payload = Bytes::from_static(b"message from node 1");
    sync_handler1.record_message(msg_id, &payload);
    let stored_msg = memberlist_plumtree::storage::StoredMessage::new(msg_id, 0, payload.clone());
    store1.insert(&stored_msg).await.unwrap();

    // === Push-Pull Phase ===
    // Node 1 sends local_state to Node 2
    let state1 = strategy1.local_state().await;

    // Node 2 merges Node 1's state (simulating memberlist push-pull)
    strategy2
        .merge_remote_state(&state1, || Some(TestId(1)))
        .await;

    // Node 2 should have queued a sync request for Node 1
    let peer = sync_rx2
        .try_recv()
        .expect("node 2 should request sync from node 1");
    assert_eq!(peer, TestId(1));

    // === Sync Protocol Phase ===
    // Node 2 sends SyncRequest to Node 1
    let request = SyncMessage::Request {
        root_hash: sync_handler2.root_hash(),
        time_start: 0,
        time_end: u64::MAX,
    };

    // Node 1 handles request, returns response with its message IDs
    let response = strategy1
        .handle_sync_message(TestId(2), request)
        .await
        .unwrap()
        .unwrap();

    // Verify response indicates mismatch with message IDs
    if let SyncMessage::Response {
        matches,
        message_ids,
        ..
    } = &response
    {
        assert!(!matches);
        assert!(message_ids.contains(&msg_id));
    } else {
        panic!("expected SyncResponse");
    }

    // Node 2 handles response, identifies missing message, sends Pull
    if let SyncMessage::Response { message_ids, .. } = response {
        // Node 2 checks which messages it's missing
        let missing: Vec<_> = message_ids
            .iter()
            .filter(|id| !sync_handler2.contains(id))
            .cloned()
            .collect();

        assert_eq!(missing.len(), 1);
        assert!(missing.contains(&msg_id));

        // Node 2 sends Pull for missing message
        let pull = SyncMessage::Pull {
            message_ids: smallvec::smallvec![msg_id],
        };

        // Node 1 handles Pull, returns Push with message data
        let push = strategy1
            .handle_sync_message(TestId(2), pull)
            .await
            .unwrap()
            .unwrap();

        // Verify Push contains the message
        if let SyncMessage::Push { messages } = push {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].0, msg_id);
            assert_eq!(messages[0].2, payload);

            // Node 2 would deliver this message and update its sync state
            sync_handler2.record_message(msg_id, &payload);

            // Now hashes should match
            assert_eq!(sync_handler1.root_hash(), sync_handler2.root_hash());
        } else {
            panic!("expected SyncPush");
        }
    }
}

#[tokio::test]
async fn test_memberlist_strategy_bidirectional_sync() {
    // Both nodes have messages the other doesn't have

    // Node 1 setup
    let store1 = Arc::new(MemoryStore::new(1000));
    let sync_handler1 = Arc::new(SyncHandler::new(store1.clone()));
    let (sync_tx1, _sync_rx1) = async_channel::bounded::<TestId>(64);
    let strategy1 =
        MemberlistSyncStrategy::new(sync_handler1.clone(), Duration::from_secs(90), sync_tx1);

    // Node 2 setup
    let store2 = Arc::new(MemoryStore::new(1000));
    let sync_handler2 = Arc::new(SyncHandler::new(store2.clone()));
    let (sync_tx2, _sync_rx2) = async_channel::bounded::<TestId>(64);
    let strategy2 =
        MemberlistSyncStrategy::new(sync_handler2.clone(), Duration::from_secs(90), sync_tx2);

    // Node 1 has message A
    let msg_a = MessageId::new();
    let payload_a = Bytes::from_static(b"message A from node 1");
    sync_handler1.record_message(msg_a, &payload_a);
    store1
        .insert(&memberlist_plumtree::storage::StoredMessage::new(
            msg_a,
            0,
            payload_a.clone(),
        ))
        .await
        .unwrap();

    // Node 2 has message B
    let msg_b = MessageId::new();
    let payload_b = Bytes::from_static(b"message B from node 2");
    sync_handler2.record_message(msg_b, &payload_b);
    store2
        .insert(&memberlist_plumtree::storage::StoredMessage::new(
            msg_b,
            0,
            payload_b.clone(),
        ))
        .await
        .unwrap();

    // Hashes should be different
    assert_ne!(sync_handler1.root_hash(), sync_handler2.root_hash());

    // === Node 1 initiates sync with Node 2 ===
    let request1 = SyncMessage::Request {
        root_hash: sync_handler1.root_hash(),
        time_start: 0,
        time_end: u64::MAX,
    };

    let response1 = strategy2
        .handle_sync_message(TestId(1), request1)
        .await
        .unwrap()
        .unwrap();

    if let SyncMessage::Response {
        matches,
        message_ids,
        ..
    } = response1
    {
        assert!(!matches);
        // Node 2's response contains msg_b (which Node 1 is missing)
        assert!(message_ids.contains(&msg_b));

        // Node 1 pulls msg_b
        let pull = SyncMessage::Pull {
            message_ids: smallvec::smallvec![msg_b],
        };
        let push = strategy2
            .handle_sync_message(TestId(1), pull)
            .await
            .unwrap()
            .unwrap();

        if let SyncMessage::Push { messages } = push {
            // Node 1 receives and records msg_b
            for (id, _round, payload) in messages.iter() {
                sync_handler1.record_message(*id, payload);
            }
        }
    }

    // === Node 2 initiates sync with Node 1 ===
    let request2 = SyncMessage::Request {
        root_hash: sync_handler2.root_hash(),
        time_start: 0,
        time_end: u64::MAX,
    };

    let response2 = strategy1
        .handle_sync_message(TestId(2), request2)
        .await
        .unwrap()
        .unwrap();

    if let SyncMessage::Response {
        matches,
        message_ids,
        ..
    } = response2
    {
        assert!(!matches);
        // Node 1's response contains msg_a (which Node 2 is missing)
        assert!(message_ids.contains(&msg_a));

        // Node 2 pulls msg_a
        let pull = SyncMessage::Pull {
            message_ids: smallvec::smallvec![msg_a],
        };
        let push = strategy1
            .handle_sync_message(TestId(2), pull)
            .await
            .unwrap()
            .unwrap();

        if let SyncMessage::Push { messages } = push {
            // Node 2 receives and records msg_a
            for (id, _round, payload) in messages.iter() {
                sync_handler2.record_message(*id, payload);
            }
        }
    }

    // Now both nodes should have the same hash (both have msg_a and msg_b)
    assert_eq!(
        sync_handler1.root_hash(),
        sync_handler2.root_hash(),
        "after bidirectional sync, hashes should match"
    );
}

// ============================================================================
// Edge Cases
// ============================================================================

#[tokio::test]
async fn test_memberlist_strategy_empty_sync() {
    // Both nodes have no messages
    let store1 = Arc::new(MemoryStore::new(1000));
    let sync_handler1 = Arc::new(SyncHandler::new(store1));
    let (tx1, _) = async_channel::bounded::<TestId>(64);
    let strategy1 =
        MemberlistSyncStrategy::new(sync_handler1.clone(), Duration::from_secs(90), tx1);

    let store2 = Arc::new(MemoryStore::new(1000));
    let sync_handler2 = Arc::new(SyncHandler::new(store2));
    let (tx2, rx2) = async_channel::bounded::<TestId>(64);
    let strategy2 =
        MemberlistSyncStrategy::new(sync_handler2.clone(), Duration::from_secs(90), tx2);

    // Both have empty state (zero hash)
    let state1 = strategy1.local_state().await;
    strategy2
        .merge_remote_state(&state1, || Some(TestId(1)))
        .await;

    // Should NOT trigger sync (both have same empty state)
    assert!(
        rx2.try_recv().is_err(),
        "should not sync when both are empty"
    );
}

#[tokio::test]
async fn test_memberlist_strategy_record_and_remove() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    let (tx, _) = async_channel::bounded::<TestId>(64);
    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Record a message
    let id = MessageId::new();
    strategy.record_message(id, b"test");
    let hash_with_message = strategy.root_hash();

    // Remove the message
    strategy.remove_message(&id);
    let hash_without_message = strategy.root_hash();

    // Hash should return to zero (XOR is self-inverse)
    assert_eq!(hash_without_message, [0u8; 32]);
    assert_ne!(hash_with_message, hash_without_message);
}

#[tokio::test]
async fn test_memberlist_strategy_channel_full() {
    let store = Arc::new(MemoryStore::new(1000));
    let sync_handler = Arc::new(SyncHandler::new(store));
    // Very small channel
    let (tx, _rx) = async_channel::bounded::<TestId>(1);

    let strategy = MemberlistSyncStrategy::new(sync_handler, Duration::from_secs(90), tx);

    // Record a message so hash differs
    strategy.record_message(MessageId::new(), b"test");

    // Create remote state with different hash
    let mut remote_state = BytesMut::with_capacity(48);
    remote_state.put_slice(&[0u8; 32]);
    remote_state.put_u64(current_time_ms());
    remote_state.put_u64(90_000);

    // First merge should succeed
    strategy
        .merge_remote_state(&remote_state, || Some(TestId(1)))
        .await;

    // Second merge - channel is full, should not panic (graceful handling)
    strategy
        .merge_remote_state(&remote_state, || Some(TestId(2)))
        .await;

    // Third merge - still should not panic
    strategy
        .merge_remote_state(&remote_state, || Some(TestId(3)))
        .await;
}
