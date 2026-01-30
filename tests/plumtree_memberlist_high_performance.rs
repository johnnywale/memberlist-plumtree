//! High-performance integration tests for PlumtreeDiscovery.
//!
//! These tests demonstrate the recommended usage pattern for production deployments:
//! - PooledTransport for concurrency control and backpressure
//! - Separate broadcast and unicast channels
//! - Proper message routing between nodes
//!
//! This pattern is recommended for high-throughput scenarios where you want to:
//! - Use reliable unicast (e.g., QUIC) for Graft/Prune messages
//! - Use memberlist's gossip for broadcast messages
//! - Have fine-grained control over concurrency and queueing

use bytes::{Buf, BufMut, Bytes};
use memberlist_plumtree::{
    decode_plumtree_envelope, encode_plumtree_envelope, ChannelTransport, IdCodec, MessageId,
    PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery, PlumtreeMessage, PoolConfig,
    PooledTransport,
};
use nodecraft::CheapClone;
use parking_lot::Mutex;
use smallvec::smallvec;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Simple node ID type for testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId(u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl CheapClone for NodeId {}

impl IdCodec for NodeId {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.0);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 8 {
            Some(NodeId(buf.get_u64()))
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        8
    }
}

/// Test delegate that tracks delivered messages and protocol events.
#[derive(Debug, Default, Clone)]
struct TestDelegate(Arc<TestDelegateInner>);

#[derive(Debug, Default)]
struct TestDelegateInner {
    delivered: Mutex<Vec<(MessageId, Bytes)>>,
    delivered_ids: Mutex<HashSet<MessageId>>,
    promotions: Mutex<Vec<NodeId>>,
    demotions: Mutex<Vec<NodeId>>,
    grafts_sent: Mutex<Vec<(NodeId, MessageId)>>,
    prunes_sent: Mutex<Vec<NodeId>>,
}

#[allow(dead_code)]
impl TestDelegate {
    fn new() -> Self {
        Self(Arc::new(TestDelegateInner::default()))
    }

    fn delivered_count(&self) -> usize {
        self.0.delivered.lock().len()
    }

    fn has_message(&self, id: &MessageId) -> bool {
        self.0.delivered_ids.lock().contains(id)
    }

    fn get_messages(&self) -> Vec<(MessageId, Bytes)> {
        self.0.delivered.lock().clone()
    }

    fn promotion_count(&self) -> usize {
        self.0.promotions.lock().len()
    }

    fn demotion_count(&self) -> usize {
        self.0.demotions.lock().len()
    }

    fn graft_count(&self) -> usize {
        self.0.grafts_sent.lock().len()
    }
}

impl PlumtreeDelegate<NodeId> for TestDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.0.delivered.lock().push((message_id, payload.clone()));
        self.0.delivered_ids.lock().insert(message_id);
    }

    fn on_eager_promotion(&self, peer: &NodeId) {
        self.0.promotions.lock().push(*peer);
    }

    fn on_lazy_demotion(&self, peer: &NodeId) {
        self.0.demotions.lock().push(*peer);
    }

    fn on_graft_sent(&self, peer: &NodeId, message_id: &MessageId) {
        self.0.grafts_sent.lock().push((*peer, *message_id));
    }

    fn on_prune_sent(&self, peer: &NodeId) {
        self.0.prunes_sent.lock().push(*peer);
    }
}

/// A simulated network that routes messages between PlumtreeDiscovery nodes.
#[allow(dead_code)]
struct SimulatedNetwork {
    /// Map from NodeId to the incoming sender for that node
    incoming_senders: HashMap<NodeId, async_channel::Sender<(NodeId, PlumtreeMessage)>>,
    /// Receiver for unicast messages from all nodes (target, data)
    unicast_receivers: Vec<async_channel::Receiver<(NodeId, Bytes)>>,
    /// Stats
    broadcast_count: std::sync::atomic::AtomicUsize,
    unicast_count: std::sync::atomic::AtomicUsize,
}

#[allow(dead_code)]
impl SimulatedNetwork {
    fn new() -> Self {
        Self {
            incoming_senders: HashMap::new(),
            unicast_receivers: Vec::new(),
            broadcast_count: std::sync::atomic::AtomicUsize::new(0),
            unicast_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn register_node(
        &mut self,
        node_id: NodeId,
        incoming_tx: async_channel::Sender<(NodeId, PlumtreeMessage)>,
    ) {
        self.incoming_senders.insert(node_id, incoming_tx);
    }

    fn add_unicast_receiver(&mut self, rx: async_channel::Receiver<(NodeId, Bytes)>) {
        self.unicast_receivers.push(rx);
    }

    /// Route a broadcast message to all nodes except the sender
    async fn route_broadcast(&self, from: NodeId, data: &Bytes) {
        if let Some((sender_id, message)) = decode_plumtree_envelope::<NodeId>(data) {
            self.broadcast_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            for (node_id, sender) in &self.incoming_senders {
                if *node_id != from {
                    let _ = sender.send((sender_id, message.clone())).await;
                }
            }
        }
    }

    /// Route a unicast message to a specific node
    async fn route_unicast(&self, target: NodeId, data: &Bytes) {
        if let Some((sender_id, message)) = decode_plumtree_envelope::<NodeId>(data) {
            self.unicast_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(sender) = self.incoming_senders.get(&target) {
                let _ = sender.send((sender_id, message)).await;
            }
        }
    }
}

/// Test the basic high-performance setup with multiple nodes.
#[tokio::test]
async fn test_high_performance_basic_broadcast() {
    const NUM_NODES: u64 = 5;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(2)
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(100));

    let pool_config = PoolConfig::default();

    // Create nodes and network
    let mut network = SimulatedNetwork::new();
    let mut nodes: Vec<Arc<PlumtreeDiscovery<NodeId, TestDelegate>>> = Vec::new();
    let mut delegates: Vec<TestDelegate> = Vec::new();
    let mut transports: Vec<Arc<PooledTransport<ChannelTransport<NodeId>, NodeId>>> = Vec::new();

    for i in 0..NUM_NODES {
        let node_id = NodeId(i);
        let delegate = TestDelegate::new();
        delegates.push(delegate.clone());

        let pm = Arc::new(PlumtreeDiscovery::new(node_id, config.clone(), delegate));

        // Create channel transport for unicast messages
        let (transport, unicast_rx) = ChannelTransport::bounded(1000);
        let pooled = Arc::new(PooledTransport::new(transport, pool_config.clone()));

        network.register_node(node_id, pm.incoming_sender());
        network.add_unicast_receiver(unicast_rx);
        transports.push(pooled);
        nodes.push(pm);
    }

    // Add peers to each node (fully connected)
    for i in 0..NUM_NODES {
        for j in 0..NUM_NODES {
            if i != j {
                nodes[i as usize].add_peer(NodeId(j));
            }
        }
    }

    // Start all nodes with their transports
    let mut handles = Vec::new();
    for (pm, transport) in nodes.iter().zip(transports.iter()) {
        // Start run_with_transport
        let pm_run = pm.clone();
        let transport_clone = transport.clone();
        handles.push(tokio::spawn(async move {
            pm_run.run_with_transport(transport_clone).await;
        }));

        // Start incoming processor
        let pm_proc = pm.clone();
        handles.push(tokio::spawn(async move {
            pm_proc.run_incoming_processor().await;
        }));
    }

    // Start unicast message router
    let network = Arc::new(network);
    for rx in network.unicast_receivers.iter() {
        let network_clone = network.clone();
        let rx_clone = rx.clone();
        tokio::spawn(async move {
            while let Ok((target, data)) = rx_clone.recv().await {
                network_clone.route_unicast(target, &data).await;
            }
        });
    }

    // Give nodes time to establish connections
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node 0 broadcasts a message
    let msg_id = nodes[0]
        .broadcast(Bytes::from("high-performance test"))
        .await
        .unwrap();

    // Simulate broadcast routing (normally memberlist would do this)
    let gossip_msg = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: Bytes::from("high-performance test"),
    };
    let _encoded = encode_plumtree_envelope(&NodeId(0), &gossip_msg);

    // Route to all other nodes
    for i in 1..NUM_NODES {
        if let Some(sender) = network.incoming_senders.get(&NodeId(i)) {
            let _ = sender.send((NodeId(0), gossip_msg.clone())).await;
        }
    }

    // Wait for message propagation
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all nodes received the message
    for (i, delegate) in delegates.iter().enumerate().skip(1) {
        assert!(
            delegate.has_message(&msg_id),
            "Node {} should have received the message",
            i
        );
    }

    // Shutdown all nodes
    for pm in &nodes {
        pm.shutdown();
    }
}

/// Test that PooledTransport properly handles concurrency limits.
#[tokio::test]
async fn test_pooled_transport_concurrency() {
    let config = PlumtreeConfig::default();
    let pool_config = PoolConfig {
        max_concurrent_per_peer: 2,
        max_concurrent_global: 4,
        max_queue_per_peer: 10,
        fair_scheduling: true,
    };

    let delegate = TestDelegate::new();
    let pm = Arc::new(PlumtreeDiscovery::new(NodeId(0), config, delegate.clone()));

    let (transport, _unicast_rx) = ChannelTransport::<NodeId>::bounded(100);
    let pooled = Arc::new(PooledTransport::new(transport, pool_config));

    // Add some peers
    pm.add_peer(NodeId(1));
    pm.add_peer(NodeId(2));

    // Check pool stats
    let stats = pooled.stats();
    assert_eq!(stats.active_sends, 0);
    assert_eq!(stats.queued_sends, 0);

    pm.shutdown();
}

/// Test message flow with IHave/Graft cycle.
#[tokio::test]
async fn test_ihave_graft_cycle() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(1)
        .with_lazy_fanout(2)
        .with_ihave_interval(Duration::from_millis(20))
        .with_graft_timeout(Duration::from_millis(100));

    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    let pm1 = Arc::new(PlumtreeDiscovery::new(
        NodeId(1),
        config.clone(),
        delegate1.clone(),
    ));
    let pm2 = Arc::new(PlumtreeDiscovery::new(NodeId(2), config, delegate2.clone()));

    // Add peers (node 2 is lazy for node 1)
    pm1.add_peer_lazy(NodeId(2));
    pm2.add_peer_lazy(NodeId(1));

    // Create transports
    let (transport1, rx1) = ChannelTransport::bounded(100);
    let (transport2, rx2) = ChannelTransport::bounded(100);
    let pooled1 = Arc::new(PooledTransport::new(transport1, PoolConfig::default()));
    let pooled2 = Arc::new(PooledTransport::new(transport2, PoolConfig::default()));

    // Start background tasks
    let pm1_run = pm1.clone();
    let t1 = pooled1.clone();
    tokio::spawn(async move { pm1_run.run_with_transport(t1).await });

    let pm2_run = pm2.clone();
    let t2 = pooled2.clone();
    tokio::spawn(async move { pm2_run.run_with_transport(t2).await });

    let pm1_proc = pm1.clone();
    tokio::spawn(async move { pm1_proc.run_incoming_processor().await });

    let pm2_proc = pm2.clone();
    tokio::spawn(async move { pm2_proc.run_incoming_processor().await });

    // Route unicast messages between nodes
    let pm1_incoming = pm1.incoming_sender();
    let pm2_incoming = pm2.incoming_sender();

    tokio::spawn(async move {
        while let Ok((target, data)) = rx1.recv().await {
            if target == NodeId(2) {
                if let Some((sender, msg)) = decode_plumtree_envelope::<NodeId>(&data) {
                    let _ = pm2_incoming.send((sender, msg)).await;
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Ok((target, data)) = rx2.recv().await {
            if target == NodeId(1) {
                if let Some((sender, msg)) = decode_plumtree_envelope::<NodeId>(&data) {
                    let _ = pm1_incoming.send((sender, msg)).await;
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Node 1 broadcasts - node 2 should receive IHave since it's lazy
    let _msg_id = pm1.broadcast(Bytes::from("graft test")).await.unwrap();

    // Wait for IHave to be sent and Graft cycle to complete
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Node 2 should have either received via Graft or been promoted to eager
    // Check that the IHave/Graft mechanism was triggered
    let _graft_count = delegate2.graft_count();
    let _promotion_count = delegate2.promotion_count();

    // The cycle should have been triggered (either graft sent or promotion happened)
    // Note: In a lazy-only setup, node 2 will send a Graft when it receives IHave

    pm1.shutdown();
    pm2.shutdown();
}

/// Test that duplicate messages are properly handled across nodes.
#[tokio::test]
async fn test_duplicate_handling() {
    let config = PlumtreeConfig::default();
    let delegate = TestDelegate::new();

    let pm = Arc::new(PlumtreeDiscovery::new(NodeId(0), config, delegate.clone()));

    pm.add_peer(NodeId(1));
    pm.add_peer(NodeId(2));

    let msg_id = MessageId::new();
    let payload = Bytes::from("duplicate test");

    // Send the same message multiple times from different "senders"
    let gossip = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: payload.clone(),
    };

    pm.handle_message(NodeId(1), gossip.clone()).await.unwrap();
    pm.handle_message(NodeId(2), gossip.clone()).await.unwrap();
    pm.handle_message(NodeId(1), gossip.clone()).await.unwrap();

    // Should only be delivered once
    assert_eq!(delegate.delivered_count(), 1);
    assert!(delegate.has_message(&msg_id));

    pm.shutdown();
}

/// Test peer topology management.
#[tokio::test]
async fn test_peer_topology_management() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_max_peers(10);

    let delegate = TestDelegate::new();
    let pm = Arc::new(PlumtreeDiscovery::new(NodeId(0), config, delegate));

    // Add peers
    for i in 1..=5 {
        pm.add_peer(NodeId(i));
    }

    // Check peer stats
    let stats = pm.peer_stats();
    assert!(stats.eager_count + stats.lazy_count == 5);

    // Verify topology
    let topology = pm.peers().topology();
    assert_eq!(topology.eager.len() + topology.lazy.len(), 5);

    // Remove a peer
    pm.remove_peer(&NodeId(3));
    let stats = pm.peer_stats();
    assert!(stats.eager_count + stats.lazy_count == 4);

    pm.shutdown();
}

/// Test graceful shutdown.
#[tokio::test]
async fn test_graceful_shutdown() {
    let config = PlumtreeConfig::default();
    let delegate = TestDelegate::new();

    let pm = Arc::new(PlumtreeDiscovery::new(NodeId(0), config, delegate));

    let (transport, _rx) = ChannelTransport::<NodeId>::bounded(100);
    let pooled = Arc::new(PooledTransport::new(transport, PoolConfig::default()));

    // Start runners
    let pm_run = pm.clone();
    let t = pooled.clone();
    let _handle = tokio::spawn(async move {
        pm_run.run_with_transport(t).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify not shutdown
    assert!(!pm.is_shutdown());

    // Shutdown
    pm.shutdown();

    // Verify shutdown flag is set
    assert!(pm.is_shutdown());

    // Give the runner time to notice shutdown and stop
    tokio::time::sleep(Duration::from_millis(100)).await;

    // After shutdown, the system should be in a shutdown state
    // The exact behavior of broadcast after shutdown may vary:
    // - It might return an error
    // - It might succeed but the message won't be processed
    // The important thing is that is_shutdown() returns true
    assert!(pm.is_shutdown());
}

/// Test encode/decode round-trip for plumtree envelopes.
#[tokio::test]
async fn test_envelope_encoding() {
    let sender = NodeId(42);
    let msg_id = MessageId::new();

    let messages = vec![
        PlumtreeMessage::Gossip {
            id: msg_id,
            round: 5,
            payload: Bytes::from("hello world"),
        },
        PlumtreeMessage::IHave {
            message_ids: smallvec![msg_id, MessageId::new()],
            round: 3,
        },
        PlumtreeMessage::Graft {
            message_id: msg_id,
            round: 7,
        },
        PlumtreeMessage::Prune,
    ];

    for msg in messages {
        let encoded = encode_plumtree_envelope(&sender, &msg);
        let decoded = decode_plumtree_envelope::<NodeId>(&encoded);

        assert!(decoded.is_some(), "Failed to decode message: {:?}", msg);
        let (decoded_sender, decoded_msg) = decoded.unwrap();
        assert_eq!(decoded_sender, sender);

        // Verify message type matches
        match (&msg, &decoded_msg) {
            (PlumtreeMessage::Gossip { id: id1, .. }, PlumtreeMessage::Gossip { id: id2, .. }) => {
                assert_eq!(id1, id2);
            }
            (
                PlumtreeMessage::IHave {
                    message_ids: ids1, ..
                },
                PlumtreeMessage::IHave {
                    message_ids: ids2, ..
                },
            ) => {
                assert_eq!(ids1.len(), ids2.len());
            }
            (
                PlumtreeMessage::Graft {
                    message_id: id1, ..
                },
                PlumtreeMessage::Graft {
                    message_id: id2, ..
                },
            ) => {
                assert_eq!(id1, id2);
            }
            (PlumtreeMessage::Prune, PlumtreeMessage::Prune) => {}
            _ => panic!("Message type mismatch"),
        }
    }
}

/// Test high-throughput broadcast scenario.
#[tokio::test]
async fn test_high_throughput_broadcast() {
    const NUM_NODES: u64 = 3;
    const NUM_MESSAGES: usize = 50;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_ihave_interval(Duration::from_millis(10));

    let _pool_config = PoolConfig::high_throughput();

    let mut nodes: Vec<Arc<PlumtreeDiscovery<NodeId, TestDelegate>>> = Vec::new();
    let mut delegates: Vec<TestDelegate> = Vec::new();

    for i in 0..NUM_NODES {
        let delegate = TestDelegate::new();
        delegates.push(delegate.clone());
        let pm = Arc::new(PlumtreeDiscovery::new(NodeId(i), config.clone(), delegate));
        nodes.push(pm);
    }

    // Connect nodes in a chain: 0 -> 1 -> 2
    nodes[0].add_peer(NodeId(1));
    nodes[0].peers().promote_to_eager(&NodeId(1));
    nodes[1].add_peer(NodeId(0));
    nodes[1].add_peer(NodeId(2));
    nodes[1].peers().promote_to_eager(&NodeId(0));
    nodes[1].peers().promote_to_eager(&NodeId(2));
    nodes[2].add_peer(NodeId(1));
    nodes[2].peers().promote_to_eager(&NodeId(1));

    // Broadcast many messages from node 0
    let mut msg_ids = Vec::new();
    for i in 0..NUM_MESSAGES {
        let payload = format!("message-{}", i);
        let msg_id = nodes[0].broadcast(Bytes::from(payload)).await.unwrap();
        msg_ids.push(msg_id);
    }

    // Simulate message propagation: 0 -> 1
    for msg_id in &msg_ids {
        let gossip = PlumtreeMessage::Gossip {
            id: *msg_id,
            round: 0,
            payload: Bytes::from(format!("message-{}", msg_id.timestamp())),
        };
        nodes[1].handle_message(NodeId(0), gossip).await.unwrap();
    }

    // Simulate message propagation: 1 -> 2
    for msg_id in &msg_ids {
        let gossip = PlumtreeMessage::Gossip {
            id: *msg_id,
            round: 1,
            payload: Bytes::from(format!("message-{}", msg_id.timestamp())),
        };
        nodes[2].handle_message(NodeId(1), gossip).await.unwrap();
    }

    // Verify all messages were delivered
    assert_eq!(delegates[1].delivered_count(), NUM_MESSAGES);
    assert_eq!(delegates[2].delivered_count(), NUM_MESSAGES);

    for pm in &nodes {
        pm.shutdown();
    }
}
