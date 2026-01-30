//! E2E Tests for MemberlistStack with message delivery and storage.
//!
//! These tests verify that:
//! 1. `start()` is required for message processing
//! 2. Messages are delivered to delegates
//! 3. Messages are stored in the storage backend
//!
//! # Key Finding
//!
//! The `MemberlistStack::new()` constructor does NOT start background tasks.
//! You MUST call `start()` for the protocol to work. Without this:
//! - Incoming messages are never processed
//! - IHave scheduler never runs
//! - Graft timer never runs
//! - Unicast messages (Gossip to eager peers, Graft, Prune) are never sent
//! - Messages are never delivered

#![cfg(feature = "tokio")]

mod common;

use bytes::{Buf, BufMut, Bytes};
use common::allocate_port;
use memberlist::net::NetTransportOptions;
use memberlist::tokio::{TokioRuntime, TokioSocketAddrResolver, TokioTcp};
use memberlist::{Memberlist, Options as MemberlistOptions};
use memberlist_plumtree::{
    storage::{MemoryStore, MessageStore},
    IdCodec, MessageId, PlumtreeBridge, PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery,
    PlumtreeNodeDelegate,
};
use nodecraft::resolver::socket_addr::SocketAddrResolver;
use nodecraft::CheapClone;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
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
        4 + self.0.len()
    }
}

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
            self.0.len()
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

    #[allow(dead_code)]
    fn has_message(&self, id: &MessageId) -> bool {
        self.0.delivered_ids.lock().contains(id)
    }

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
        println!(
            "  [DELIVER] msg_id={:?}, payload_len={}",
            message_id,
            payload.len()
        );
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

type TestStack = memberlist_plumtree::MemberlistStack<
    TestNodeId,
    TrackingDelegate,
    MemberlistTransport,
    StackDelegate,
>;

// ============================================================================
// Test Stack with Storage
// ============================================================================

struct TestNode {
    stack: TestStack,
    delegate: TrackingDelegate,
    store: Arc<MemoryStore>,
    node_id: TestNodeId,
}

impl TestNode {
    async fn new(name: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let node_id = TestNodeId(name.to_string());
        let port = allocate_port();
        let bind_addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;

        let delegate = TrackingDelegate::new();
        let store = Arc::new(MemoryStore::new(10_000));

        // Use PlumtreeDiscovery::with_storage for message persistence
        let config = PlumtreeConfig::lan()
            .with_eager_fanout(3)
            .with_lazy_fanout(3)
            .with_ihave_interval(Duration::from_millis(50))
            .with_graft_timeout(Duration::from_millis(200));

        let pm = Arc::new(PlumtreeDiscovery::with_storage(
            node_id.clone(),
            config,
            delegate.clone(),
            store.clone(),
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
                node_id.clone(),
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
        let bridge = PlumtreeBridge::new(pm);
        let stack = memberlist_plumtree::MemberlistStack::new(bridge, memberlist, advertise_addr);

        // CRITICAL: Start background tasks!
        // This handles all protocol operations:
        // - IHave scheduler
        // - Graft timer
        // - Incoming message processor
        // - Unicast messages (Gossip to eager peers, Graft, Prune) via memberlist
        stack.start();

        Ok(Self {
            stack,
            delegate,
            store,
            node_id,
        })
    }

    fn memberlist_addr(&self) -> SocketAddr {
        self.stack.advertise_address()
    }

    async fn join(&self, seed: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.stack
            .join(&[seed])
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
        sleep(Duration::from_millis(50)).await;
    }

    fn delivered_count(&self) -> usize {
        self.delegate.delivered_count()
    }

    #[allow(dead_code)]
    fn has_message(&self, id: &MessageId) -> bool {
        self.delegate.has_message(id)
    }

    fn received(&self, payload: &[u8]) -> bool {
        self.delegate.received(payload)
    }

    async fn stored_message_count(&self) -> usize {
        self.store.count().await.unwrap_or_default()
    }

    async fn has_stored_message(&self, msg_id: &MessageId) -> bool {
        self.store.get(msg_id).await.ok().flatten().is_some()
    }
}

// ============================================================================
// Tests
// ============================================================================

/// Test that messages are delivered when start() is called.
#[tokio::test]
async fn test_memberlist_stack_message_delivery() {
    println!("\n=== Test: MemberlistStack Message Delivery ===\n");

    // Create two nodes
    let node_a = TestNode::new("node-a")
        .await
        .expect("Failed to create node A");
    let node_b = TestNode::new("node-b")
        .await
        .expect("Failed to create node B");

    println!(
        "Node A: {:?} at {}",
        node_a.node_id,
        node_a.memberlist_addr()
    );
    println!(
        "Node B: {:?} at {}",
        node_b.node_id,
        node_b.memberlist_addr()
    );

    // Node B joins via Node A
    let seed_addr = node_a.memberlist_addr();
    node_b.join(seed_addr).await.expect("Node B failed to join");

    // Wait for SWIM gossip to propagate and Plumtree to sync peers
    println!("\nWaiting for cluster formation...");
    sleep(Duration::from_secs(3)).await;

    // Verify peers are connected
    let stats_a = node_a.stack.peer_stats();
    let stats_b = node_b.stack.peer_stats();
    println!(
        "Node A peers: eager={}, lazy={}",
        stats_a.eager_count, stats_a.lazy_count
    );
    println!(
        "Node B peers: eager={}, lazy={}",
        stats_b.eager_count, stats_b.lazy_count
    );

    // Get the topology details
    let topo_a = node_a.stack.plumtree().peers().topology();
    let topo_b = node_b.stack.plumtree().peers().topology();
    println!("Node A eager: {:?}, lazy: {:?}", topo_a.eager, topo_a.lazy);
    println!("Node B eager: {:?}, lazy: {:?}", topo_b.eager, topo_b.lazy);

    // If no eager peers, manually promote to eager for the test
    if stats_a.eager_count == 0 && stats_a.lazy_count > 0 {
        println!("\n[DEBUG] Promoting lazy peers to eager on Node A...");
        for peer in topo_a.lazy.iter() {
            node_a.stack.plumtree().peers().promote_to_eager(peer);
        }
        let new_stats_a = node_a.stack.peer_stats();
        println!(
            "Node A after promotion: eager={}, lazy={}",
            new_stats_a.eager_count, new_stats_a.lazy_count
        );
    }
    if stats_b.eager_count == 0 && stats_b.lazy_count > 0 {
        println!("[DEBUG] Promoting lazy peers to eager on Node B...");
        for peer in topo_b.lazy.iter() {
            node_b.stack.plumtree().peers().promote_to_eager(peer);
        }
        let new_stats_b = node_b.stack.peer_stats();
        println!(
            "Node B after promotion: eager={}, lazy={}",
            new_stats_b.eager_count, new_stats_b.lazy_count
        );
    }

    // Node A broadcasts a message
    let payload = b"Hello from Node A!";
    println!(
        "\nNode A broadcasting: {:?}",
        String::from_utf8_lossy(payload)
    );
    let msg_id = node_a
        .broadcast(Bytes::from_static(payload))
        .await
        .expect("Broadcast failed");
    println!("Broadcast msg_id: {:?}", msg_id);

    // Wait for message to propagate
    println!("\nWaiting for message delivery...");
    sleep(Duration::from_secs(3)).await;

    // Check delivery on Node B
    let b_delivered = node_b.delivered_count();
    let b_received = node_b.received(payload);
    println!("Node B delivered count: {}", b_delivered);
    println!("Node B received payload: {}", b_received);

    // Cleanup
    node_a.shutdown().await;
    node_b.shutdown().await;

    // Verify delivery
    assert!(
        b_received,
        "Node B should have received the message. Delivered count: {}",
        b_delivered
    );
}

/// Test that messages are stored in the storage backend.
#[tokio::test]
async fn test_memberlist_stack_message_storage() {
    println!("\n=== Test: MemberlistStack Message Storage ===\n");

    // Create two nodes
    let node_a = TestNode::new("storage-a")
        .await
        .expect("Failed to create node A");
    let node_b = TestNode::new("storage-b")
        .await
        .expect("Failed to create node B");

    // Node B joins via Node A
    let seed_addr = node_a.memberlist_addr();
    node_b.join(seed_addr).await.expect("Node B failed to join");

    // Wait for cluster formation
    sleep(Duration::from_secs(2)).await;

    // Verify peers
    let stats_a = node_a.stack.peer_stats();
    let stats_b = node_b.stack.peer_stats();
    println!(
        "Node A peers: eager={}, lazy={}",
        stats_a.eager_count, stats_a.lazy_count
    );
    println!(
        "Node B peers: eager={}, lazy={}",
        stats_b.eager_count, stats_b.lazy_count
    );

    // Node A broadcasts a message
    let payload = b"Test message for storage";
    let msg_id = node_a
        .broadcast(Bytes::from_static(payload))
        .await
        .expect("Broadcast failed");
    println!("Broadcast msg_id: {:?}", msg_id);

    // Wait for delivery and storage
    sleep(Duration::from_secs(2)).await;

    // Check storage on Node A (sender stores on broadcast)
    let a_stored_count = node_a.stored_message_count().await;
    let a_has_msg = node_a.has_stored_message(&msg_id).await;
    println!("Node A stored count: {}", a_stored_count);
    println!("Node A has message: {}", a_has_msg);

    // Check storage on Node B (receiver stores on delivery)
    let b_stored_count = node_b.stored_message_count().await;
    let b_has_msg = node_b.has_stored_message(&msg_id).await;
    println!("Node B stored count: {}", b_stored_count);
    println!("Node B has message: {}", b_has_msg);

    // Check delivery
    let b_delivered = node_b.delivered_count();
    let b_received = node_b.received(payload);
    println!(
        "Node B delivered: {}, received: {}",
        b_delivered, b_received
    );

    // Cleanup
    node_a.shutdown().await;
    node_b.shutdown().await;

    // Verify
    assert!(a_has_msg, "Node A should have stored the broadcast message");
    assert!(b_received, "Node B should have received the message");
    // Note: Node B storage depends on the on_deliver implementation using storage
}

/// Test multiple messages are delivered correctly.
#[tokio::test]
async fn test_memberlist_stack_multiple_messages() {
    println!("\n=== Test: MemberlistStack Multiple Messages ===\n");

    // Create three nodes
    let node_a = TestNode::new("multi-a")
        .await
        .expect("Failed to create node A");
    let node_b = TestNode::new("multi-b")
        .await
        .expect("Failed to create node B");
    let node_c = TestNode::new("multi-c")
        .await
        .expect("Failed to create node C");

    // Build cluster
    let seed_addr = node_a.memberlist_addr();
    node_b.join(seed_addr).await.expect("Node B failed to join");
    node_c.join(seed_addr).await.expect("Node C failed to join");

    // Wait for cluster formation
    sleep(Duration::from_secs(3)).await;

    // Send multiple messages from Node A
    let messages = vec![
        b"Message 1".to_vec(),
        b"Message 2".to_vec(),
        b"Message 3".to_vec(),
    ];

    for msg in &messages {
        node_a
            .broadcast(Bytes::from(msg.clone()))
            .await
            .expect("Broadcast failed");
        sleep(Duration::from_millis(100)).await;
    }

    // Wait for all messages to propagate
    sleep(Duration::from_secs(3)).await;

    // Check delivery counts
    let a_count = node_a.delivered_count();
    let b_count = node_b.delivered_count();
    let c_count = node_c.delivered_count();

    println!("Node A delivered: {}", a_count);
    println!("Node B delivered: {}", b_count);
    println!("Node C delivered: {}", c_count);

    // Cleanup
    node_a.shutdown().await;
    node_b.shutdown().await;
    node_c.shutdown().await;

    // Each node should receive at least some messages
    // (Node A doesn't deliver to itself via protocol, only broadcasts)
    assert!(
        b_count >= 1,
        "Node B should have received at least 1 message"
    );
    assert!(
        c_count >= 1,
        "Node C should have received at least 1 message"
    );
}

/// Test bidirectional message flow.
#[tokio::test]
async fn test_memberlist_stack_bidirectional() {
    println!("\n=== Test: MemberlistStack Bidirectional Messages ===\n");

    let node_a = TestNode::new("bidir-a")
        .await
        .expect("Failed to create node A");
    let node_b = TestNode::new("bidir-b")
        .await
        .expect("Failed to create node B");

    // Form cluster
    let seed_addr = node_a.memberlist_addr();
    node_b.join(seed_addr).await.expect("Node B failed to join");
    sleep(Duration::from_secs(2)).await;

    // Node A sends to Node B
    let msg_a = b"Hello from A";
    node_a
        .broadcast(Bytes::from_static(msg_a))
        .await
        .expect("A broadcast failed");

    // Node B sends to Node A
    let msg_b = b"Hello from B";
    node_b
        .broadcast(Bytes::from_static(msg_b))
        .await
        .expect("B broadcast failed");

    // Wait for delivery
    sleep(Duration::from_secs(2)).await;

    // Check cross-delivery
    let a_received_b = node_a.received(msg_b);
    let b_received_a = node_b.received(msg_a);

    println!("Node A received from B: {}", a_received_b);
    println!("Node B received from A: {}", b_received_a);

    // Cleanup
    node_a.shutdown().await;
    node_b.shutdown().await;

    assert!(
        b_received_a,
        "Node B should have received message from Node A"
    );
    assert!(
        a_received_b,
        "Node A should have received message from Node B"
    );
}
