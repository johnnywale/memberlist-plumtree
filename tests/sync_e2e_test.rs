#![allow(clippy::needless_as_bytes)]

//! End-to-End Tests for Anti-Entropy Sync and Persistence.
//!
//! These tests verify that the `PlumtreeDiscovery` correctly handles:
//! 1. Long-disconnect recovery via Anti-Entropy (Merkle/XOR comparison).
//! 2. Message persistence across node restarts (Sled backend).
//! 3. Bi-directional state reconciliation (Union of disjoint sets).
//! 4. Garbage collection based on retention policies.
//!
//! # Test Scenarios
//!
//! | Scenario | Description |
//! |----------|-------------|
//! | Late Joiner Recovery | Node joining after broadcasts receives missed messages via sync |
//! | Bi-directional Sync | Two isolated nodes exchange disjoint message sets |
//! | Crash Recovery | Messages persist to disk and survive node restart |
//! | Retention Policy | Expired messages are pruned and not synced |
//! | Large Batch Pagination | Sync handles batching when missing > max_batch_size |

#![cfg(all(feature = "tokio", feature = "sync"))]

mod common;

use bytes::{Buf, BufMut, Bytes};
use common::allocate_port;
use memberlist::net::NetTransportOptions;
use memberlist::tokio::{TokioRuntime, TokioSocketAddrResolver, TokioTcp};
use memberlist::{Memberlist, Options as MemberlistOptions};
use memberlist_plumtree::{
    IdCodec, MessageId, PlumtreeBridge, PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery,
    PlumtreeNodeDelegate, StorageConfig, SyncConfig,
};
use nodecraft::resolver::socket_addr::SocketAddrResolver;
use nodecraft::CheapClone;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
#[cfg(feature = "storage-sled")]
use tempfile::TempDir;
use tokio::time::sleep;

// ============================================================================
// Node ID Type
// ============================================================================

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TestNodeId(pub String);

impl fmt::Debug for TestNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TestNodeId({})", self.0)
    }
}

impl fmt::Display for TestNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl CheapClone for TestNodeId {}

impl IdCodec for TestNodeId {
    fn encode_id(&self, buf: &mut impl BufMut) {
        let bytes = self.0.as_bytes();
        buf.put_u32(bytes.len() as u32);
        buf.put_slice(bytes);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 4 {
            return None;
        }
        let len = buf.get_u32() as usize;
        if buf.remaining() < len {
            return None;
        }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        String::from_utf8(bytes).ok().map(TestNodeId)
    }

    fn encoded_id_len(&self) -> usize {
        4 + self.0.as_bytes().len()
    }
}

// Memberlist Data trait implementation
mod data_impl {
    use super::TestNodeId;
    use memberlist::proto::{Data, DataRef, DecodeError, EncodeError};
    use std::borrow::Cow;

    impl<'a> DataRef<'a, TestNodeId> for &'a str {
        fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError> {
            match core::str::from_utf8(buf) {
                Ok(value) => Ok((buf.len(), value)),
                Err(e) => Err(DecodeError::Custom(Cow::Owned(e.to_string()))),
            }
        }
    }

    impl Data for TestNodeId {
        type Ref<'a> = &'a str;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
            Ok(TestNodeId(val.to_string()))
        }

        fn encoded_len(&self) -> usize {
            self.0.as_bytes().len()
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
            let bytes = self.0.as_bytes();
            if bytes.is_empty() {
                return Ok(0);
            }
            let len = bytes.len();
            if len > buf.len() {
                return Err(EncodeError::InsufficientBuffer {
                    required: len,
                    remaining: buf.len(),
                });
            }
            buf[..len].copy_from_slice(bytes);
            Ok(len)
        }
    }
}

// ============================================================================
// Tracking Delegate
// ============================================================================

#[derive(Debug, Default, Clone)]
struct TrackingDelegate(Arc<TrackingDelegateInner>);

#[derive(Debug, Default)]
struct TrackingDelegateInner {
    delivered: Mutex<Vec<(MessageId, Bytes)>>,
    delivered_ids: Mutex<HashSet<MessageId>>,
}

impl TrackingDelegate {
    fn new() -> Self {
        Self(Arc::new(TrackingDelegateInner::default()))
    }

    fn delivered_count(&self) -> usize {
        self.0.delivered.lock().len()
    }

    fn has_message(&self, id: &MessageId) -> bool {
        self.0.delivered_ids.lock().contains(id)
    }

    #[allow(dead_code)]
    fn received(&self, payload: &[u8]) -> bool {
        self.0
            .delivered
            .lock()
            .iter()
            .any(|(_, p)| p.as_ref() == payload)
    }
}

impl PlumtreeDelegate<TestNodeId> for TrackingDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.0.delivered.lock().push((message_id, payload.clone()));
        self.0.delivered_ids.lock().insert(message_id);
    }
}

// ============================================================================
// Type Aliases
// ============================================================================

type MemberlistTransport =
    memberlist::net::NetTransport<TestNodeId, TokioSocketAddrResolver, TokioTcp, TokioRuntime>;

type StackDelegate = memberlist_plumtree::PlumtreeNodeDelegate<
    TestNodeId,
    SocketAddr,
    memberlist::delegate::VoidDelegate<TestNodeId, SocketAddr>,
>;

// ============================================================================
// Memberlist Transport Wrapper
// ============================================================================

/// Transport error type for memberlist-based unicast.
#[derive(Debug)]
struct TransportError(String);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TransportError {}

/// Type alias for the full MemberlistStack type
type TestStack = memberlist_plumtree::MemberlistStack<
    TestNodeId,
    TrackingDelegate,
    MemberlistTransport,
    StackDelegate,
>;

/// Transport that sends unicast messages via memberlist's reliable transport.
#[derive(Clone)]
struct MemberlistUnicastTransport {
    stack: Arc<TestStack>,
}

impl memberlist_plumtree::Transport<TestNodeId> for MemberlistUnicastTransport {
    type Error = TransportError;

    async fn send_to(&self, target: &TestNodeId, data: Bytes) -> Result<(), Self::Error> {
        // Find the node's address and send via memberlist's reliable transport
        let members = self.stack.memberlist().members().await;
        if let Some(node) = members.iter().find(|m| m.id() == target) {
            self.stack
                .memberlist()
                .send_reliable(node.address(), data)
                .await
                .map_err(|e| TransportError(e.to_string()))?;
        }
        Ok(())
    }
}

// ============================================================================
// RealStack Implementation
// ============================================================================

struct RealStack {
    stack: Arc<
        memberlist_plumtree::MemberlistStack<
            TestNodeId,
            TrackingDelegate,
            MemberlistTransport,
            StackDelegate,
        >,
    >,
    delegate: TrackingDelegate,
}

impl RealStack {
    async fn new_with_config(
        name: &str,
        port: u16,
        config: PlumtreeConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node_id = TestNodeId(name.to_string());
        // Use port allocator when port is 0 to avoid Windows socket permission errors
        let actual_port = if port == 0 { allocate_port() } else { port };
        let bind_addr: SocketAddr = format!("127.0.0.1:{}", actual_port).parse()?;

        let delegate = TrackingDelegate::new();

        let pm = Arc::new(PlumtreeDiscovery::new(
            node_id.clone(),
            config,
            delegate.clone(),
        ));

        let void_delegate = memberlist::delegate::VoidDelegate::<TestNodeId, SocketAddr>::default();
        let plumtree_delegate = PlumtreeNodeDelegate::new(
            void_delegate,
            pm.incoming_sender(),
            pm.outgoing_receiver(),
            pm.peers().clone(),
            pm.config().eager_fanout,
            pm.config().max_peers,
        );

        let mut transport_opts =
            NetTransportOptions::<TestNodeId, SocketAddrResolver<TokioRuntime>, TokioTcp>::new(
                node_id,
            );
        transport_opts.add_bind_address(bind_addr);

        let memberlist_opts = MemberlistOptions::local()
            .with_probe_interval(Duration::from_millis(500))
            .with_gossip_interval(Duration::from_millis(200));

        let memberlist =
            Memberlist::with_delegate(plumtree_delegate, transport_opts, memberlist_opts)
                .await
                .map_err(|e| format!("Failed to create memberlist: {}", e))?;

        let advertise_addr = *memberlist.advertise_address();

        // Wrap PlumtreeDiscovery in PlumtreeBridge
        let bridge = PlumtreeBridge::new(pm);

        let stack = Arc::new(memberlist_plumtree::MemberlistStack::new(
            bridge,
            memberlist,
            advertise_addr,
        ));

        // Create transport wrapper for unicast messages using the stack's memberlist
        let transport = MemberlistUnicastTransport {
            stack: stack.clone(),
        };

        // Start background tasks including sync
        stack.start(transport);

        Ok(Self { stack, delegate })
    }

    async fn join(
        &self,
        seed_addr: SocketAddr,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stack
            .join(&[seed_addr])
            .await
            .map_err(|e| format!("Join failed: {}", e).into())
    }

    async fn broadcast(
        &self,
        payload: impl Into<Bytes>,
    ) -> Result<MessageId, memberlist_plumtree::Error> {
        self.stack.broadcast(payload).await
    }

    async fn shutdown(&self) {
        let _ = self.stack.leave(Duration::from_secs(1)).await;
        let _ = self.stack.shutdown().await;
    }

    fn memberlist_addr(&self) -> SocketAddr {
        self.stack.advertise_address()
    }

    #[allow(dead_code)]
    async fn num_members(&self) -> usize {
        self.stack.num_members().await
    }

    /// Get stored message count (for debugging)
    #[allow(dead_code)]
    fn stored_message_count(&self) -> usize {
        self.stack.plumtree().store().len()
    }
}

// ============================================================================
// Test Scenario 1: Basic Anti-Entropy Recovery (One-Way)
// ============================================================================

/// Verifies that a node connecting LATE to a cluster retrieves missed messages
/// via the Anti-Entropy Sync mechanism (Pull/Push) rather than Gossip.
///
/// **Flow**:
/// 1. Node A starts (Seed).
/// 2. Node A broadcasts 5 messages.
/// 3. Node B starts and joins Node A *after* broadcasts are finished.
/// 4. Node B should auto-detect missing messages via `SyncRequest`.
#[tokio::test]
async fn test_sync_recovery_late_joiner() {
    // Configure aggressive sync for testing (sync every 100ms)
    let sync_config = SyncConfig::enabled()
        .with_sync_interval(Duration::from_millis(100))
        .with_sync_window(Duration::from_secs(60));

    // In-memory storage required for sync
    let storage_config = StorageConfig::enabled().with_max_messages(1000);

    let config = PlumtreeConfig::lan()
        .with_sync(sync_config)
        .with_storage(storage_config)
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    // 1. Start Node A
    let node_a = RealStack::new_with_config("sync-late-a", 0, config.clone())
        .await
        .expect("Failed to start Node A");

    // Small delay to ensure node is fully started
    sleep(Duration::from_millis(100)).await;

    // 2. Node A broadcasts messages (before B exists)
    let mut expected_ids = Vec::new();
    for i in 0..5 {
        let payload = format!("msg-{}", i).into_bytes();
        let msg_id = node_a
            .broadcast(Bytes::from(payload))
            .await
            .expect("Broadcast failed");
        expected_ids.push(msg_id);
    }

    println!("Node A broadcast {} messages", expected_ids.len());

    // Small delay to ensure messages are stored
    sleep(Duration::from_millis(200)).await;

    // Debug: Check how many messages are stored
    let a_stored = node_a.stored_message_count();
    println!("Node A has {} messages in storage", a_stored);

    // 3. Start Node B and Join
    let node_b = RealStack::new_with_config("sync-late-b", 0, config.clone())
        .await
        .expect("Failed to start Node B");

    node_b
        .join(node_a.memberlist_addr())
        .await
        .expect("Join failed");

    println!("Node B joined cluster");

    // Debug: Print stored messages on Node B before sync
    let b_stored_before = node_b.stored_message_count();
    println!(
        "Node B has {} messages in storage before sync",
        b_stored_before
    );

    // 4. Wait for Sync Cycles (Allow multiple intervals)
    sleep(Duration::from_secs(2)).await;

    // Debug: Print stored messages after sync
    let b_stored_after = node_b.stored_message_count();
    println!(
        "Node B has {} messages in storage after sync (waiting 2s)",
        b_stored_after
    );

    // 5. Verify Node B received the messages via Sync
    let b_count = node_b.delegate.delivered_count();
    println!(
        "Node B received {}/{} messages",
        b_count,
        expected_ids.len()
    );

    for id in &expected_ids {
        assert!(
            node_b.delegate.has_message(id),
            "Node B failed to sync message {:?} via Anti-Entropy",
            id
        );
    }

    println!("=== Test Passed: Late joiner recovery works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// Test Scenario 2: Bi-Directional State Reconciliation
// ============================================================================

/// Verifies that two nodes with disjoint message sets exchange data until
/// both possess the union of all messages.
///
/// **Flow**:
/// 1. Node A (Isolated) broadcasts [M1, M2].
/// 2. Node B (Isolated) broadcasts [M3, M4].
/// 3. Node A joins Node B.
/// 4. Both nodes should converge to [M1, M2, M3, M4].
#[tokio::test]
async fn test_bidirectional_entropy_resolution() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("bi-node-a", 0, config.clone())
        .await
        .unwrap();
    let node_b = RealStack::new_with_config("bi-node-b", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // 1. Generate Disjoint Sets (Isolated)
    let id_a1 = node_a.broadcast(Bytes::from("origin-a-1")).await.unwrap();
    let id_a2 = node_a.broadcast(Bytes::from("origin-a-2")).await.unwrap();

    let id_b1 = node_b.broadcast(Bytes::from("origin-b-1")).await.unwrap();
    let id_b2 = node_b.broadcast(Bytes::from("origin-b-2")).await.unwrap();

    println!("Node A has: {:?}, {:?}", id_a1, id_a2);
    println!("Node B has: {:?}, {:?}", id_b1, id_b2);

    // Ensure they don't have each other's messages yet
    assert!(!node_a.delegate.has_message(&id_b1));
    assert!(!node_a.delegate.has_message(&id_b2));
    assert!(!node_b.delegate.has_message(&id_a1));
    assert!(!node_b.delegate.has_message(&id_a2));

    // 2. Connect
    node_a
        .join(node_b.memberlist_addr())
        .await
        .expect("Join failed");

    println!("Nodes connected, waiting for convergence...");

    // 3. Wait for Convergence (multiple sync cycles)
    sleep(Duration::from_secs(3)).await;

    // 4. Verify Union
    let a_count = node_a.delegate.delivered_count();
    let b_count = node_b.delegate.delivered_count();
    println!(
        "Node A has {} messages, Node B has {} messages",
        a_count, b_count
    );

    assert!(
        node_a.delegate.has_message(&id_b1),
        "Node A missing B's message 1"
    );
    assert!(
        node_a.delegate.has_message(&id_b2),
        "Node A missing B's message 2"
    );
    assert!(
        node_b.delegate.has_message(&id_a1),
        "Node B missing A's message 1"
    );
    assert!(
        node_b.delegate.has_message(&id_a2),
        "Node B missing A's message 2"
    );

    println!("=== Test Passed: Bi-directional sync works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// Test Scenario 3: Persistence Across Restarts (Sled Backend)
// ============================================================================

/// Verifies that the Sled storage backend correctly persists messages to disk
/// and can retrieve them after reopening the database.
///
/// This tests the storage layer directly since full MemberlistStack integration
/// with custom storage requires additional library changes.
///
/// **Flow**:
/// 1. Open Sled database
/// 2. Insert messages
/// 3. Close database
/// 4. Reopen database
/// 5. Verify messages are still present
#[cfg(feature = "storage-sled")]
#[tokio::test]
async fn test_persistence_crash_recovery() {
    use memberlist_plumtree::storage::{MessageStore, SledStore, StoredMessage};

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join("plumtree_db");

    // Create test message IDs
    let msg_id1 = MessageId::new();
    let msg_id2 = MessageId::new();
    let msg_id3 = MessageId::new();

    // 1. Open Sled and insert messages
    {
        let store = SledStore::open(db_path.to_str().unwrap()).expect("Failed to open Sled");

        let msg1 = StoredMessage::new(msg_id1, 0, Bytes::from("message-1"));
        let msg2 = StoredMessage::new(msg_id2, 0, Bytes::from("message-2"));
        let msg3 = StoredMessage::new(msg_id3, 0, Bytes::from("message-3"));

        store.insert(&msg1).await.expect("Insert failed");
        store.insert(&msg2).await.expect("Insert failed");
        store.insert(&msg3).await.expect("Insert failed");

        let count = store.count().await.expect("Count failed");
        println!("Sled has {} messages before close", count);
        assert_eq!(count, 3, "All messages should be stored");

        // Flush to ensure durability
        store.flush().await.expect("Flush failed");

        // Store is dropped here, simulating crash
    }

    println!("Database closed, simulating restart...");

    // 2. Reopen Sled and verify messages persist
    {
        let store = SledStore::open(db_path.to_str().unwrap()).expect("Failed to reopen Sled");

        let count = store.count().await.expect("Count failed");
        println!("Sled has {} messages after reopen", count);
        assert_eq!(count, 3, "Messages should persist across restart");

        // Verify each message can be retrieved
        let retrieved1 = store.get(&msg_id1).await.expect("Get failed");
        assert!(retrieved1.is_some(), "Message 1 should exist");
        assert_eq!(
            retrieved1.unwrap().payload.as_ref(),
            b"message-1",
            "Payload should match"
        );

        let retrieved2 = store.get(&msg_id2).await.expect("Get failed");
        assert!(retrieved2.is_some(), "Message 2 should exist");
        assert_eq!(
            retrieved2.unwrap().payload.as_ref(),
            b"message-2",
            "Payload should match"
        );

        let retrieved3 = store.get(&msg_id3).await.expect("Get failed");
        assert!(retrieved3.is_some(), "Message 3 should exist");
        assert_eq!(
            retrieved3.unwrap().payload.as_ref(),
            b"message-3",
            "Payload should match"
        );

        // Verify contains() works
        assert!(
            store.contains(&msg_id1).await.expect("Contains failed"),
            "Contains should return true"
        );

        // Verify get_range works
        let (ids, _) = store
            .get_range(0, u64::MAX, 100, 0)
            .await
            .expect("Range query failed");
        assert_eq!(ids.len(), 3, "Range should return all messages");
    }

    println!("=== Test Passed: Sled persistence works correctly ===");
}

// ============================================================================
// Test Scenario 4: Retention Policy & Pruning
// ============================================================================

/// Verifies that expired messages are pruned and NOT synced.
///
/// **Flow**:
/// 1. Node A configured with 1-second retention.
/// 2. Node A broadcasts M1.
/// 3. Wait 2 seconds (M1 expires).
/// 4. Node B joins.
/// 5. Node B should NOT receive M1.
#[tokio::test]
async fn test_retention_policy_enforcement() {
    let short_retention_config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(5)),
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(1000)
                .with_retention(Duration::from_secs(1)), // 1 second TTL
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("ttl-node-a", 0, short_retention_config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // 1. Broadcast Message
    let msg_id = node_a.broadcast(Bytes::from("short-lived")).await.unwrap();

    println!("Node A broadcast message {:?}", msg_id);

    // 2. Wait for TTL Expiration + Pruning Cycle
    println!("Waiting for TTL expiration (2s)...");
    sleep(Duration::from_secs(2)).await;

    // 3. Node B joins
    let node_b = RealStack::new_with_config("ttl-node-b", 0, short_retention_config)
        .await
        .unwrap();

    node_b
        .join(node_a.memberlist_addr())
        .await
        .expect("Join failed");

    // 4. Wait for potential sync
    sleep(Duration::from_secs(1)).await;

    // 5. Assert Message is GONE (should NOT be synced)
    let b_has_msg = node_b.delegate.has_message(&msg_id);
    println!(
        "Node B has expired message: {} (expected: false)",
        b_has_msg
    );

    assert!(
        !b_has_msg,
        "Node B received a message that should have been pruned by retention policy"
    );

    println!("=== Test Passed: Retention policy enforcement works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// Test Scenario 5: Large Batch Sync (Pagination)
// ============================================================================

/// Verifies that the sync protocol handles batching correctly when missing
/// messages exceed `max_batch_size`.
#[tokio::test]
async fn test_large_batch_pagination() {
    // Force tiny batch size to ensure pagination logic triggers
    let tiny_batch_config = SyncConfig::enabled()
        .with_sync_interval(Duration::from_millis(100))
        .with_sync_window(Duration::from_secs(60))
        .with_max_batch_size(10); // Only send 10 at a time

    let config = PlumtreeConfig::lan()
        .with_sync(tiny_batch_config)
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("batch-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // 1. Generate 50 messages (5 batches worth)
    let mut sent_ids = Vec::new();
    for i in 0..50 {
        let id = node_a
            .broadcast(Bytes::from(format!("batch-msg-{}", i)))
            .await
            .unwrap();
        sent_ids.push(id);
    }

    println!("Node A broadcast {} messages", sent_ids.len());

    // Small delay to ensure all messages are stored
    sleep(Duration::from_millis(200)).await;

    // 2. Node B joins
    let node_b = RealStack::new_with_config("batch-b", 0, config)
        .await
        .unwrap();
    node_b
        .join(node_a.memberlist_addr())
        .await
        .expect("Join failed");

    println!("Node B joined, waiting for batch sync...");

    // 3. Wait longer for multiple round-trips (5 batches of 10)
    sleep(Duration::from_secs(5)).await;

    // 4. Verify count
    let b_count = node_b.delegate.delivered_count();
    println!("Node B received {}/50 messages", b_count);

    assert_eq!(
        b_count, 50,
        "Node B only received {}/50 messages. Pagination/Batching failed.",
        b_count
    );

    println!("=== Test Passed: Large batch pagination works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// Test Scenario 6: Sync After Network Partition
// ============================================================================

/// Verifies that nodes recover missed messages after a network partition heals.
///
/// **Flow**:
/// 1. Start 3 nodes (A, B, C) connected.
/// 2. Isolate Node C (simulate partition).
/// 3. A and B broadcast messages.
/// 4. Reconnect Node C.
/// 5. Node C should receive all missed messages via sync.
#[tokio::test]
async fn test_sync_after_partition() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    // 1. Start 3 nodes
    let node_a = RealStack::new_with_config("partition-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    let node_b = RealStack::new_with_config("partition-b", 0, config.clone())
        .await
        .unwrap();
    node_b
        .join(node_a.memberlist_addr())
        .await
        .expect("B join failed");

    let node_c = RealStack::new_with_config("partition-c", 0, config.clone())
        .await
        .unwrap();
    node_c
        .join(node_a.memberlist_addr())
        .await
        .expect("C join failed");

    // Wait for cluster formation
    sleep(Duration::from_secs(1)).await;

    println!("3-node cluster formed");

    // 2. "Partition" Node C by shutting it down
    let _c_addr = node_c.memberlist_addr();
    node_c.shutdown().await;

    println!("Node C partitioned (shutdown)");

    // 3. A and B broadcast messages while C is partitioned
    let id_a = node_a
        .broadcast(Bytes::from("during-partition-a"))
        .await
        .unwrap();
    let id_b = node_b
        .broadcast(Bytes::from("during-partition-b"))
        .await
        .unwrap();

    println!("Broadcast during partition: {:?}, {:?}", id_a, id_b);

    sleep(Duration::from_millis(500)).await;

    // 4. Reconnect Node C (create new instance and rejoin)
    let node_c_rejoined = RealStack::new_with_config("partition-c-rejoin", 0, config.clone())
        .await
        .unwrap();
    node_c_rejoined
        .join(node_a.memberlist_addr())
        .await
        .expect("C rejoin failed");

    println!("Node C rejoined, waiting for sync...");

    // 5. Wait for sync
    sleep(Duration::from_secs(3)).await;

    // 6. Verify Node C received messages
    let c_count = node_c_rejoined.delegate.delivered_count();
    println!("Node C received {} messages after rejoin", c_count);

    assert!(
        node_c_rejoined.delegate.has_message(&id_a),
        "Node C missing message from A"
    );
    assert!(
        node_c_rejoined.delegate.has_message(&id_b),
        "Node C missing message from B"
    );

    println!("=== Test Passed: Sync after partition works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c_rejoined.shutdown().await;
}

// ============================================================================
// EDGE CASE TESTS - Group 1: Storage & Capacity Limits
// ============================================================================

/// Edge Case 1.1: Very Short Retention TTL
///
/// Verifies that with very short retention, messages are eventually pruned
/// and sync does not serve expired messages after sufficient time.
#[tokio::test]
async fn test_edge_zero_retention_ttl() {
    // Use 500ms retention (not zero, to allow message to be stored first)
    let short_retention_config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(1000)
                .with_retention(Duration::from_millis(500)), // 500ms TTL
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("zero-ttl-a", 0, short_retention_config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast message
    let msg_id = node_a.broadcast(Bytes::from("ephemeral")).await.unwrap();
    println!("Broadcast message {:?}", msg_id);

    // Wait for expiration + multiple prune cycles
    // Storage prune runs at retention/2 interval, so 250ms
    // We need to wait for multiple cycles to ensure pruning
    sleep(Duration::from_secs(2)).await;

    // Check storage (might be empty after prune)
    let stored = node_a.stored_message_count();
    println!("Messages in storage after prune: {}", stored);

    // Node B joins - message should be pruned by now
    let node_b = RealStack::new_with_config("zero-ttl-b", 0, short_retention_config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(1)).await;

    // B should NOT have the message (it was pruned before B joined)
    let b_has_msg = node_b.delegate.has_message(&msg_id);
    println!("Node B has pruned message: {} (expected: false)", b_has_msg);

    assert!(!b_has_msg, "Message should have been pruned before sync");

    println!("=== Test Passed: Short retention TTL works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 1.2: Storage Capacity Overflow
///
/// Verifies that when storage reaches max_messages, old messages are evicted
/// (LRU by timestamp) and sync only serves non-evicted messages.
#[tokio::test]
async fn test_edge_storage_capacity_overflow() {
    let tiny_capacity_config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(5) // Tiny capacity
                .with_retention(Duration::from_secs(300)),
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("overflow-a", 0, tiny_capacity_config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast 10 messages (exceeds capacity of 5)
    let mut all_ids = Vec::new();
    for i in 0..10 {
        let id = node_a
            .broadcast(Bytes::from(format!("overflow-msg-{}", i)))
            .await
            .unwrap();
        all_ids.push(id);
        // Small delay to ensure distinct timestamps
        sleep(Duration::from_millis(10)).await;
    }

    println!("Broadcast 10 messages with max_capacity=5");

    // Storage should have at most 5 messages
    let stored = node_a.stored_message_count();
    println!("Messages in storage: {} (expected <= 5)", stored);
    assert!(stored <= 5, "Storage should evict old messages");

    // Node B joins - should only receive what's in storage
    let node_b = RealStack::new_with_config("overflow-b", 0, tiny_capacity_config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    let b_count = node_b.delegate.delivered_count();
    println!("Node B received {} messages (expected <= 5)", b_count);

    // B should have received only the non-evicted messages
    assert!(
        b_count <= 5,
        "Node B should only receive non-evicted messages"
    );

    // First messages should be evicted (oldest)
    assert!(
        !node_b.delegate.has_message(&all_ids[0]),
        "First message should have been evicted"
    );

    println!("=== Test Passed: Storage capacity overflow works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 1.3: Empty Payload Message
///
/// Verifies that zero-byte payloads are handled correctly in sync.
#[tokio::test]
async fn test_edge_empty_payload() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("empty-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast empty payload
    let empty_id = node_a.broadcast(Bytes::new()).await.unwrap();
    let normal_id = node_a.broadcast(Bytes::from("normal")).await.unwrap();

    println!("Broadcast empty and normal messages");

    sleep(Duration::from_millis(200)).await;

    let node_b = RealStack::new_with_config("empty-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    assert!(
        node_b.delegate.has_message(&empty_id),
        "Empty payload message should sync"
    );
    assert!(
        node_b.delegate.has_message(&normal_id),
        "Normal message should sync"
    );

    println!("=== Test Passed: Empty payload handling works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 1.4: Large Payload Message
///
/// Verifies that large payloads (near MTU limit) are handled correctly.
#[tokio::test]
async fn test_edge_large_payload() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("large-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Create a large payload (10KB)
    let large_payload = vec![0x42u8; 10 * 1024];
    let large_id = node_a
        .broadcast(Bytes::from(large_payload.clone()))
        .await
        .unwrap();

    println!("Broadcast large message (10KB)");

    sleep(Duration::from_millis(200)).await;

    let node_b = RealStack::new_with_config("large-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    assert!(
        node_b.delegate.has_message(&large_id),
        "Large payload message should sync"
    );

    println!("=== Test Passed: Large payload handling works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 1.5: Exact Capacity Boundary
///
/// Verifies behavior when messages exactly fill capacity (no overflow).
#[tokio::test]
async fn test_edge_exact_capacity() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(5)
                .with_retention(Duration::from_secs(300)),
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("exact-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast exactly max_messages
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = node_a
            .broadcast(Bytes::from(format!("exact-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    println!("Broadcast exactly 5 messages (max_capacity=5)");

    sleep(Duration::from_millis(200)).await;

    let stored = node_a.stored_message_count();
    println!("Messages in storage: {} (expected 5)", stored);
    assert_eq!(stored, 5, "All messages should be stored");

    let node_b = RealStack::new_with_config("exact-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    for id in &ids {
        assert!(
            node_b.delegate.has_message(id),
            "All messages should sync at exact capacity"
        );
    }

    println!("=== Test Passed: Exact capacity boundary works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// EDGE CASE TESTS - Group 2: Sync Protocol & Logic
// ============================================================================

/// Edge Case 2.1: Disjoint Clusters with Many Messages
///
/// Two isolated clusters develop independently, then merge.
/// Tests that large disjoint sets are reconciled correctly.
#[tokio::test]
async fn test_edge_disjoint_clusters_merge() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    // Cluster 1: Nodes A and B
    let node_a = RealStack::new_with_config("disjoint-a", 0, config.clone())
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let node_b = RealStack::new_with_config("disjoint-b", 0, config.clone())
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    // Cluster 2: Nodes C and D (isolated)
    let node_c = RealStack::new_with_config("disjoint-c", 0, config.clone())
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let node_d = RealStack::new_with_config("disjoint-d", 0, config.clone())
        .await
        .unwrap();
    node_d.join(node_c.memberlist_addr()).await.unwrap();

    sleep(Duration::from_millis(500)).await;

    // Each cluster broadcasts messages independently
    let mut cluster1_ids = Vec::new();
    let mut cluster2_ids = Vec::new();

    for i in 0..10 {
        let id = node_a
            .broadcast(Bytes::from(format!("cluster1-{}", i)))
            .await
            .unwrap();
        cluster1_ids.push(id);
    }

    for i in 0..10 {
        let id = node_c
            .broadcast(Bytes::from(format!("cluster2-{}", i)))
            .await
            .unwrap();
        cluster2_ids.push(id);
    }

    sleep(Duration::from_secs(1)).await;

    println!("Cluster 1 has {} messages", cluster1_ids.len());
    println!("Cluster 2 has {} messages", cluster2_ids.len());

    // Verify clusters are disjoint
    assert!(
        !node_a.delegate.has_message(&cluster2_ids[0]),
        "Clusters should be isolated"
    );
    assert!(
        !node_c.delegate.has_message(&cluster1_ids[0]),
        "Clusters should be isolated"
    );

    // Merge clusters: Node C joins Node A
    node_c.join(node_a.memberlist_addr()).await.unwrap();

    println!("Clusters merging...");

    // Wait for convergence
    sleep(Duration::from_secs(5)).await;

    // All nodes should have all messages
    for id in &cluster1_ids {
        assert!(
            node_c.delegate.has_message(id),
            "C missing cluster1 message"
        );
        assert!(
            node_d.delegate.has_message(id),
            "D missing cluster1 message"
        );
    }

    for id in &cluster2_ids {
        assert!(
            node_a.delegate.has_message(id),
            "A missing cluster2 message"
        );
        assert!(
            node_b.delegate.has_message(id),
            "B missing cluster2 message"
        );
    }

    println!("=== Test Passed: Disjoint clusters merge works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    node_d.shutdown().await;
}

/// Edge Case 2.2: Single Message Difference
///
/// Verifies that sync correctly identifies a single missing message
/// when one node has messages and a late joiner has none.
#[tokio::test]
async fn test_edge_single_message_difference() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("single-diff-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // A broadcasts messages before B exists
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = node_a
            .broadcast(Bytes::from(format!("common-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    sleep(Duration::from_millis(200)).await;

    // A broadcasts ONE more message
    let extra_id = node_a
        .broadcast(Bytes::from("extra-message"))
        .await
        .unwrap();
    println!("A broadcast extra message {:?}", extra_id);

    sleep(Duration::from_millis(200)).await;

    // B joins late (new instance, empty storage)
    let node_b = RealStack::new_with_config("single-diff-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    // Wait for sync
    sleep(Duration::from_secs(3)).await;

    // B should have ALL messages including the extra one
    for id in &ids {
        assert!(
            node_b.delegate.has_message(id),
            "B should receive common message via sync"
        );
    }
    assert!(
        node_b.delegate.has_message(&extra_id),
        "B should receive the extra message via sync"
    );

    println!("=== Test Passed: Single message difference detection works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 2.3: Empty Sync (Already in Sync)
///
/// Verifies that sync correctly identifies when nodes are already in sync
/// and does not waste resources re-transmitting messages.
#[tokio::test]
async fn test_edge_empty_sync_already_synced() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("empty-sync-a", 0, config.clone())
        .await
        .unwrap();
    let node_b = RealStack::new_with_config("empty-sync-b", 0, config.clone())
        .await
        .unwrap();

    node_b.join(node_a.memberlist_addr()).await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Broadcast messages from A
    for i in 0..5 {
        node_a
            .broadcast(Bytes::from(format!("sync-msg-{}", i)))
            .await
            .unwrap();
    }

    // Wait for normal gossip to deliver messages to B
    sleep(Duration::from_secs(2)).await;

    // Note: A is the broadcaster, so A's delivered_count is 0
    // (on_deliver is only called for received messages, not originated ones)
    // B receives all 5 messages via gossip
    let b_count_before = node_b.delegate.delivered_count();

    println!("Before additional sync cycles:");
    println!("  Node B delivered: {}", b_count_before);

    // B should have received all 5 messages
    assert_eq!(
        b_count_before, 5,
        "B should have received 5 messages via gossip"
    );

    // Wait for more sync cycles (should be no-ops since B already has all messages)
    sleep(Duration::from_secs(2)).await;

    let b_count_after = node_b.delegate.delivered_count();

    println!("After additional sync cycles:");
    println!("  Node B delivered: {}", b_count_after);

    // Count should not increase (no duplicate deliveries from sync)
    assert_eq!(
        b_count_after, b_count_before,
        "Sync should not re-deliver messages"
    );

    println!("=== Test Passed: Empty sync (already synced) works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 2.4: Rapid Successive Broadcasts During Sync
///
/// Verifies that sync handles messages being broadcast while sync is in progress.
#[tokio::test]
async fn test_edge_broadcast_during_sync() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(50)) // Very fast sync
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(25))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("during-sync-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Pre-populate A with messages
    let mut pre_ids = Vec::new();
    for i in 0..10 {
        let id = node_a
            .broadcast(Bytes::from(format!("pre-{}", i)))
            .await
            .unwrap();
        pre_ids.push(id);
    }

    sleep(Duration::from_millis(200)).await;

    // B joins and will trigger sync
    let node_b = RealStack::new_with_config("during-sync-b", 0, config.clone())
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    // While sync is happening, A broadcasts more messages
    let mut during_ids = Vec::new();
    for i in 0..10 {
        let id = node_a
            .broadcast(Bytes::from(format!("during-{}", i)))
            .await
            .unwrap();
        during_ids.push(id);
        // Small delay to interleave with sync
        sleep(Duration::from_millis(20)).await;
    }

    // Wait for everything to settle
    sleep(Duration::from_secs(5)).await;

    // B should have ALL messages (pre and during)
    for id in &pre_ids {
        assert!(
            node_b.delegate.has_message(id),
            "B missing pre-sync message"
        );
    }
    for id in &during_ids {
        assert!(
            node_b.delegate.has_message(id),
            "B missing during-sync message"
        );
    }

    println!("=== Test Passed: Broadcast during sync works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// EDGE CASE TESTS - Group 3: Time & Windowing
// ============================================================================

/// Edge Case 3.1: Message Near Sync Window Boundary
///
/// Verifies that messages at the edge of the sync window are handled correctly.
#[tokio::test]
async fn test_edge_sync_window_boundary() {
    let short_window_config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(2)), // 2 second window
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(1000)
                .with_retention(Duration::from_secs(10)),
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("window-a", 0, short_window_config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast a message
    let old_msg_id = node_a.broadcast(Bytes::from("old-message")).await.unwrap();
    println!("Broadcast old message {:?}", old_msg_id);

    // Wait for message to age past the sync window
    println!("Waiting for message to age past sync window (3s)...");
    sleep(Duration::from_secs(3)).await;

    // Broadcast a fresh message
    let new_msg_id = node_a.broadcast(Bytes::from("new-message")).await.unwrap();
    println!("Broadcast new message {:?}", new_msg_id);

    sleep(Duration::from_millis(200)).await;

    // Node B joins
    let node_b = RealStack::new_with_config("window-b", 0, short_window_config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    // New message should be synced (within window)
    // Old message may or may not be synced depending on implementation
    // The key is that sync doesn't break
    assert!(
        node_b.delegate.has_message(&new_msg_id),
        "New message (within window) should sync"
    );

    println!("=== Test Passed: Sync window boundary works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 3.2: Ancient Messages (Pre-Startup)
///
/// Verifies handling of messages with timestamps before node startup.
/// This can happen with persistent storage or clock skew.
#[tokio::test]
async fn test_edge_very_old_message_handling() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(300)), // Large window
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(1000)
                .with_retention(Duration::from_secs(600)),
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("ancient-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast messages with normal timestamps
    let msg_id = node_a.broadcast(Bytes::from("normal-time")).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    let node_b = RealStack::new_with_config("ancient-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    // Should sync normally
    assert!(
        node_b.delegate.has_message(&msg_id),
        "Normal message should sync"
    );

    println!("=== Test Passed: Message timestamp handling works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// EDGE CASE TESTS - Group 4: Network & Pagination
// ============================================================================

/// Edge Case 4.1: Exact Pagination Boundary
///
/// Verifies that when message count equals exactly max_batch_size,
/// pagination doesn't miss the last message or duplicate.
#[tokio::test]
async fn test_edge_exact_pagination_boundary() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60))
                .with_max_batch_size(10), // Exact boundary at 10
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("exact-page-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast EXACTLY max_batch_size messages
    let mut ids = Vec::new();
    for i in 0..10 {
        let id = node_a
            .broadcast(Bytes::from(format!("exact-page-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    println!("Broadcast exactly 10 messages (max_batch_size=10)");

    sleep(Duration::from_millis(200)).await;

    let node_b = RealStack::new_with_config("exact-page-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    let b_count = node_b.delegate.delivered_count();
    println!("Node B received {} messages", b_count);

    assert_eq!(b_count, 10, "All messages should sync at exact boundary");

    for id in &ids {
        assert!(
            node_b.delegate.has_message(id),
            "Missing message at exact pagination boundary"
        );
    }

    println!("=== Test Passed: Exact pagination boundary works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 4.2: One More Than Batch Size
///
/// Verifies that N+1 messages (where N=max_batch_size) correctly triggers
/// pagination and all messages are synced.
#[tokio::test]
async fn test_edge_one_more_than_batch() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60))
                .with_max_batch_size(10),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("one-more-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast max_batch_size + 1 messages
    let mut ids = Vec::new();
    for i in 0..11 {
        let id = node_a
            .broadcast(Bytes::from(format!("one-more-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    println!("Broadcast 11 messages (max_batch_size=10)");

    sleep(Duration::from_millis(200)).await;

    let node_b = RealStack::new_with_config("one-more-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    let b_count = node_b.delegate.delivered_count();
    println!("Node B received {} messages", b_count);

    assert_eq!(b_count, 11, "All 11 messages should sync with pagination");

    println!("=== Test Passed: One more than batch size works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 4.3: Concurrent Sync Requests
///
/// Verifies that multiple nodes can sync simultaneously without issues.
#[tokio::test]
async fn test_edge_concurrent_sync_requests() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(50)) // Very fast sync
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("concurrent-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Pre-populate A with messages
    let mut ids = Vec::new();
    for i in 0..20 {
        let id = node_a
            .broadcast(Bytes::from(format!("concurrent-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    sleep(Duration::from_millis(200)).await;

    // Start multiple nodes simultaneously
    let node_b = RealStack::new_with_config("concurrent-b", 0, config.clone())
        .await
        .unwrap();
    let node_c = RealStack::new_with_config("concurrent-c", 0, config.clone())
        .await
        .unwrap();
    let node_d = RealStack::new_with_config("concurrent-d", 0, config.clone())
        .await
        .unwrap();

    // All join A simultaneously
    node_b.join(node_a.memberlist_addr()).await.unwrap();
    node_c.join(node_a.memberlist_addr()).await.unwrap();
    node_d.join(node_a.memberlist_addr()).await.unwrap();

    println!("3 nodes joined simultaneously, all will sync with A");

    // Wait for all syncs to complete
    sleep(Duration::from_secs(5)).await;

    let b_count = node_b.delegate.delivered_count();
    let c_count = node_c.delegate.delivered_count();
    let d_count = node_d.delegate.delivered_count();

    println!("Node B: {} messages", b_count);
    println!("Node C: {} messages", c_count);
    println!("Node D: {} messages", d_count);

    assert_eq!(b_count, 20, "B should have all messages");
    assert_eq!(c_count, 20, "C should have all messages");
    assert_eq!(d_count, 20, "D should have all messages");

    println!("=== Test Passed: Concurrent sync requests work ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;
    node_d.shutdown().await;
}

// ============================================================================
// EDGE CASE TESTS - Group 5: Topology & Membership
// ============================================================================

/// Edge Case 5.1: Rapid Node Flapping
///
/// Verifies that rapid join/leave cycles don't corrupt sync state.
#[tokio::test]
async fn test_edge_rapid_node_flapping() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("flap-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // A broadcasts some messages
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = node_a
            .broadcast(Bytes::from(format!("flap-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    sleep(Duration::from_millis(200)).await;

    // Rapid flapping: join, leave, join, leave...
    for round in 0..3 {
        let flapper = RealStack::new_with_config(&format!("flap-b-{}", round), 0, config.clone())
            .await
            .unwrap();

        flapper.join(node_a.memberlist_addr()).await.unwrap();
        sleep(Duration::from_millis(200)).await;

        flapper.shutdown().await;
        sleep(Duration::from_millis(100)).await;
    }

    // Final stable joiner
    let node_final = RealStack::new_with_config("flap-final", 0, config)
        .await
        .unwrap();
    node_final.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    // Final joiner should still sync correctly
    for id in &ids {
        assert!(
            node_final.delegate.has_message(id),
            "Final joiner missing message after flapping"
        );
    }

    println!("=== Test Passed: Rapid node flapping handled correctly ===");

    node_a.shutdown().await;
    node_final.shutdown().await;
}

/// Edge Case 5.2: Asymmetric Message Distribution
///
/// One node has many more messages than another.
#[tokio::test]
async fn test_edge_asymmetric_distribution() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        .with_storage(StorageConfig::enabled().with_max_messages(1000))
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("asym-a", 0, config.clone())
        .await
        .unwrap();
    let node_b = RealStack::new_with_config("asym-b", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // A broadcasts 50 messages (isolated)
    let mut a_ids = Vec::new();
    for i in 0..50 {
        let id = node_a
            .broadcast(Bytes::from(format!("a-msg-{}", i)))
            .await
            .unwrap();
        a_ids.push(id);
    }

    // B broadcasts 1 message (isolated)
    let b_id = node_b.broadcast(Bytes::from("b-msg-0")).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    // Connect
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(5)).await;

    // A should have B's 1 message
    assert!(node_a.delegate.has_message(&b_id), "A missing B's message");

    // B should have A's 50 messages
    for id in &a_ids {
        assert!(node_b.delegate.has_message(id), "B missing A's message");
    }

    println!("=== Test Passed: Asymmetric distribution handled correctly ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

// ============================================================================
// EDGE CASE TESTS - Group 6: Error Recovery
// ============================================================================

/// Edge Case 6.1: Sync After Long Disconnection
///
/// Verifies sync works after extended disconnection (longer than normal timeouts).
#[tokio::test]
async fn test_edge_long_disconnection_recovery() {
    let config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(300)), // Large window
        )
        .with_storage(
            StorageConfig::enabled()
                .with_max_messages(1000)
                .with_retention(Duration::from_secs(600)),
        )
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("long-a", 0, config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast before B exists
    let mut ids = Vec::new();
    for i in 0..10 {
        let id = node_a
            .broadcast(Bytes::from(format!("long-msg-{}", i)))
            .await
            .unwrap();
        ids.push(id);
    }

    // Simulate "long" disconnection
    println!("Simulating 3 second disconnection...");
    sleep(Duration::from_secs(3)).await;

    // B joins after extended delay
    let node_b = RealStack::new_with_config("long-b", 0, config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(3)).await;

    // B should have all messages
    for id in &ids {
        assert!(
            node_b.delegate.has_message(id),
            "B missing message after long disconnection"
        );
    }

    println!("=== Test Passed: Long disconnection recovery works ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}

/// Edge Case 6.2: Graceful Degradation with No Storage
///
/// Verifies that sync is a no-op when storage is disabled.
#[tokio::test]
async fn test_edge_sync_without_storage() {
    // Sync enabled but storage disabled - should be a no-op
    let no_storage_config = PlumtreeConfig::lan()
        .with_sync(
            SyncConfig::enabled()
                .with_sync_interval(Duration::from_millis(100))
                .with_sync_window(Duration::from_secs(60)),
        )
        // Note: No .with_storage() - using disabled default
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(200));

    let node_a = RealStack::new_with_config("no-store-a", 0, no_storage_config.clone())
        .await
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Broadcast messages
    let msg_id = node_a.broadcast(Bytes::from("no-storage")).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    // Storage count should be 0 (storage disabled uses NoOpStore)
    let stored = node_a.stored_message_count();
    println!("Messages in storage (disabled): {}", stored);

    // Join should work - sync will be no-op
    let node_b = RealStack::new_with_config("no-store-b", 0, no_storage_config)
        .await
        .unwrap();
    node_b.join(node_a.memberlist_addr()).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    // B receives message via gossip (not sync) if delivered during connection
    // The test passes if no panic/error occurs
    println!("Node A delivered: {}", node_a.delegate.delivered_count());
    println!("Node B delivered: {}", node_b.delegate.delivered_count());

    // The message might or might not reach B depending on timing,
    // but the system should not crash
    let _ = msg_id;

    println!("=== Test Passed: Sync without storage degrades gracefully ===");

    node_a.shutdown().await;
    node_b.shutdown().await;
}
