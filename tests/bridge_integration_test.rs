//! Integration tests for the Plumtree-Memberlist bridge.
//!
//! These tests verify that the bridge correctly synchronizes membership events
//! from Memberlist to Plumtree's peer topology.

use memberlist_plumtree::{
    // Bridge types
    BridgeConfig,
    BridgeEventDelegate,
    // Transport for testing
    ChannelTransport,
    // Core protocol
    NoopDelegate,
    PlumtreeBridge,
    PlumtreeConfig,
    PlumtreeDiscovery,
    PlumtreeStackBuilder,
    PoolConfig,
    PooledTransport,
};
use std::sync::Arc;

type NodeId = u64;

/// Test that bridge correctly adds peers when notify_join is called.
#[tokio::test]
async fn test_bridge_notify_join_adds_peer() {
    // Create PlumtreeDiscovery for node 1
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    // Create bridge
    let bridge = PlumtreeBridge::new(pm.clone());

    // Verify initial state - no peers
    let stats = bridge.peer_stats();
    assert_eq!(stats.eager_count + stats.lazy_count, 0);

    // Manually add a peer through the bridge (simulating what notify_join does)
    bridge.add_peer(2u64);

    // Verify peer was added
    let stats = bridge.peer_stats();
    assert!(stats.eager_count + stats.lazy_count > 0);

    // Check topology
    let topology = bridge.topology();
    assert!(
        topology.eager.contains(&2u64) || topology.lazy.contains(&2u64),
        "Peer 2 should be in either eager or lazy set"
    );
}

/// Test that bridge correctly removes peers when notify_leave is called.
#[tokio::test]
async fn test_bridge_notify_leave_removes_peer() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::new(pm.clone());

    // Add peers
    bridge.add_peer(2u64);
    bridge.add_peer(3u64);

    // Verify peers exist
    let stats = bridge.peer_stats();
    assert_eq!(stats.eager_count + stats.lazy_count, 2);

    // Remove one peer
    bridge.remove_peer(&2u64);

    // Verify peer was removed
    let stats = bridge.peer_stats();
    assert_eq!(stats.eager_count + stats.lazy_count, 1);

    let topology = bridge.topology();
    assert!(
        !topology.eager.contains(&2u64) && !topology.lazy.contains(&2u64),
        "Peer 2 should be removed"
    );
    assert!(
        topology.eager.contains(&3u64) || topology.lazy.contains(&3u64),
        "Peer 3 should still exist"
    );
}

/// Test bridge configuration options.
#[tokio::test]
async fn test_bridge_config_options() {
    let config = BridgeConfig::new()
        .with_log_changes(false)
        .with_auto_promote(false);

    assert!(!config.log_changes);
    assert!(!config.auto_promote);

    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::default(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::with_config(pm.clone(), config);

    // Bridge should work with custom config
    bridge.add_peer(2u64);
    assert!(bridge.peer_stats().eager_count + bridge.peer_stats().lazy_count > 0);
}

/// Test PlumtreeStackBuilder API.
#[tokio::test]
async fn test_stack_builder_api() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeStackBuilder::new(pm.clone())
        .with_config(BridgeConfig::default())
        .build();

    assert!(!bridge.is_shutdown());

    // Test plumtree accessor
    let plumtree_ref = bridge.plumtree();
    assert!(Arc::ptr_eq(plumtree_ref, &pm));
}

/// Test bridge broadcast functionality.
#[tokio::test]
async fn test_bridge_broadcast() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::new(pm.clone());

    // Add a peer so broadcast has somewhere to go
    bridge.add_peer(2u64);

    // Broadcast through bridge
    let result = bridge.broadcast("test message").await;
    assert!(result.is_ok());
}

/// Test bridge shutdown.
#[tokio::test]
async fn test_bridge_shutdown() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::new(pm.clone());

    assert!(!bridge.is_shutdown());

    bridge.shutdown();

    assert!(bridge.is_shutdown());
}

/// Test multi-node topology simulation.
#[tokio::test]
async fn test_multi_node_topology_simulation() {
    // Simulate a 5-node cluster
    let nodes: Vec<Arc<PlumtreeDiscovery<NodeId, NoopDelegate>>> = (1..=5)
        .map(|id| {
            Arc::new(PlumtreeDiscovery::new(
                id as u64,
                PlumtreeConfig::lan(),
                NoopDelegate,
            ))
        })
        .collect();

    let bridges: Vec<PlumtreeBridge<NodeId, NoopDelegate>> = nodes
        .iter()
        .map(|pm| PlumtreeBridge::new(pm.clone()))
        .collect();

    // Simulate full mesh discovery - each node discovers all others
    for (i, bridge) in bridges.iter().enumerate() {
        for j in 0..5 {
            if i != j {
                let peer_id = (j + 1) as u64;
                bridge.add_peer(peer_id);
            }
        }
    }

    // Verify each node has 4 peers
    for (i, bridge) in bridges.iter().enumerate() {
        let stats = bridge.peer_stats();
        let total_peers = stats.eager_count + stats.lazy_count;
        assert_eq!(
            total_peers,
            4,
            "Node {} should have 4 peers, but has {}",
            i + 1,
            total_peers
        );
    }

    // Verify topology distribution
    // With eager_fanout=3, each node should have at most 3 eager peers
    for (i, bridge) in bridges.iter().enumerate() {
        let topology = bridge.topology();
        assert!(
            topology.eager.len() <= 3,
            "Node {} has {} eager peers, expected at most 3",
            i + 1,
            topology.eager.len()
        );
    }
}

/// Test bridge with pooled transport setup (no actual network).
#[tokio::test]
async fn test_bridge_with_pooled_transport() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    // Create channel transport for testing (returns tuple of transport and receiver)
    let (transport, _receiver) = ChannelTransport::<NodeId>::bounded(100);
    let pooled = PooledTransport::new(transport, PoolConfig::default());

    let bridge = PlumtreeBridge::new(pm.clone());

    // Add peers
    bridge.add_peer(2u64);
    bridge.add_peer(3u64);

    // Get incoming sender for message injection
    let incoming_sender = bridge.incoming_sender();
    assert!(!incoming_sender.is_closed());

    // Pooled transport should be usable
    assert_eq!(pooled.stats().messages_sent, 0);
}

/// Test BridgeEventDelegate can be cloned.
#[tokio::test]
async fn test_bridge_event_delegate_clone() {
    use memberlist_plumtree::memberlist::delegate::VoidDelegate;

    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::new(pm.clone());
    let delegate = BridgeEventDelegate::<
        NodeId,
        std::net::SocketAddr,
        NoopDelegate,
        VoidDelegate<NodeId, std::net::SocketAddr>,
    >::new(bridge);

    // Clone the delegate
    let delegate_clone = delegate.clone();

    // Both should reference the same underlying PlumtreeDiscovery
    assert!(Arc::ptr_eq(
        delegate.bridge().plumtree(),
        delegate_clone.bridge().plumtree()
    ));
}

/// Test that bridge handles rapid add/remove cycles.
#[tokio::test]
async fn test_rapid_topology_changes() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::new(pm.clone());

    // Rapid add/remove cycle
    for cycle in 0..10 {
        // Add 5 peers
        for i in 2..=6 {
            bridge.add_peer(i + cycle * 10);
        }

        // Remove 3 peers
        for i in 2..=4 {
            bridge.remove_peer(&(i + cycle * 10));
        }
    }

    // Should have some peers remaining
    let stats = bridge.peer_stats();
    assert!(stats.eager_count + stats.lazy_count > 0);
}

/// Test topology snapshot consistency.
#[tokio::test]
async fn test_topology_snapshot_consistency() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    let bridge = PlumtreeBridge::new(pm.clone());

    // Add peers
    for i in 2..=10 {
        bridge.add_peer(i);
    }

    // Get topology and stats
    let topology = bridge.topology();
    let stats = bridge.peer_stats();

    // Verify consistency
    assert_eq!(
        topology.eager.len(),
        stats.eager_count,
        "Eager count mismatch"
    );
    assert_eq!(topology.lazy.len(), stats.lazy_count, "Lazy count mismatch");

    // No peer should be in both sets
    for peer in &topology.eager {
        assert!(
            !topology.lazy.contains(peer),
            "Peer {} is in both eager and lazy sets",
            peer
        );
    }
}

/// Test that bridge correctly delegates to inner delegate (via stack builder).
#[tokio::test]
async fn test_stack_builder_delegate_creation() {
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        NoopDelegate,
    ));

    // Build delegate with void inner - specify address type via turbofish
    let delegate = PlumtreeStackBuilder::new(pm.clone())
        .with_config(BridgeConfig::default())
        .build_delegate::<std::net::SocketAddr>();

    // Should be usable
    delegate.bridge().add_peer(2u64);
    assert!(
        delegate.bridge().peer_stats().eager_count + delegate.bridge().peer_stats().lazy_count > 0
    );
}

// ============================================================================
// Full Receive-and-Process Closed Loop Tests
// ============================================================================

use bytes::Bytes;
use memberlist_plumtree::{MessageId, PlumtreeDelegate, PlumtreeMessage};
use parking_lot::Mutex;
use std::collections::HashSet;
use tokio::time::{sleep, Duration};

/// A tracking delegate that records delivered messages for verification.
#[derive(Clone)]
struct TrackingDelegate {
    inner: Arc<TrackingDelegateInner>,
}

#[derive(Default)]
struct TrackingDelegateInner {
    delivered_messages: Mutex<Vec<(MessageId, Bytes)>>,
    delivered_ids: Mutex<HashSet<MessageId>>,
}

impl TrackingDelegate {
    fn new() -> Self {
        Self {
            inner: Arc::new(TrackingDelegateInner::default()),
        }
    }

    fn delivered_count(&self) -> usize {
        self.inner.delivered_messages.lock().len()
    }

    fn has_message(&self, msg_id: &MessageId) -> bool {
        self.inner.delivered_ids.lock().contains(msg_id)
    }

    fn get_messages(&self) -> Vec<(MessageId, Bytes)> {
        self.inner.delivered_messages.lock().clone()
    }
}

impl PlumtreeDelegate<NodeId> for TrackingDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.inner
            .delivered_messages
            .lock()
            .push((message_id, payload));
        self.inner.delivered_ids.lock().insert(message_id);
    }
}

/// Test full receive integration: inject a message and verify it's processed.
/// This tests the complete closed loop from raw message injection to delegate callback.
#[tokio::test]
async fn test_full_receive_integration() {
    // 1. Set up Node A (receiver) with tracking delegate
    let node_a_id = 1u64;
    let delegate = TrackingDelegate::new();

    let pm_a = Arc::new(PlumtreeDiscovery::new(
        node_a_id,
        PlumtreeConfig::lan(),
        delegate.clone(),
    ));

    // 2. Add sender as a peer (required for message acceptance)
    let sender_id = 99u64;
    pm_a.add_peer(sender_id);

    // 3. Start the background incoming processor
    let pm_proc = pm_a.clone();
    tokio::spawn(async move {
        pm_proc.run_incoming_processor().await;
    });

    // Give processor time to start
    sleep(Duration::from_millis(10)).await;

    // 4. Create and inject a Gossip message
    let payload = Bytes::from("Hello, this is a real message!");
    let msg_id = MessageId::new();

    let gossip = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: payload.clone(),
    };

    // 5. Inject the message via incoming_sender (simulating network receipt)
    pm_a.incoming_sender()
        .send((sender_id, gossip))
        .await
        .unwrap();

    // 6. Verify: use eventual consistency pattern to wait for processing
    let mut success = false;
    for _ in 0..20 {
        if delegate.has_message(&msg_id) {
            success = true;
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    assert!(
        success,
        "Plumtree should automatically process and deliver the injected message"
    );

    // Verify the delivered message content
    assert_eq!(delegate.delivered_count(), 1);
    let messages = delegate.get_messages();
    assert_eq!(messages[0].0, msg_id);
    assert_eq!(messages[0].1, payload);

    pm_a.shutdown();
}

/// Test full integration with multiple messages.
#[tokio::test]
async fn test_full_receive_multiple_messages() {
    let delegate = TrackingDelegate::new();

    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        delegate.clone(),
    ));

    // Add sender as peer
    pm.add_peer(99u64);

    // Start processor
    let pm_proc = pm.clone();
    tokio::spawn(async move {
        pm_proc.run_incoming_processor().await;
    });

    sleep(Duration::from_millis(10)).await;

    // Send multiple messages
    let messages: Vec<_> = (0..5)
        .map(|i| {
            let msg_id = MessageId::new();
            let payload = Bytes::from(format!("Message {}", i));
            (msg_id, payload)
        })
        .collect();

    for (msg_id, payload) in &messages {
        let gossip = PlumtreeMessage::Gossip {
            id: *msg_id,
            round: 0,
            payload: payload.clone(),
        };
        pm.incoming_sender().send((99u64, gossip)).await.unwrap();
    }

    // Wait for all messages to be processed
    let mut received_count = 0;
    for _ in 0..40 {
        received_count = delegate.delivered_count();
        if received_count >= 5 {
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(
        received_count, 5,
        "All 5 messages should be delivered, got {}",
        received_count
    );

    // Verify all message IDs were received
    for (msg_id, _) in &messages {
        assert!(
            delegate.has_message(msg_id),
            "Message {:?} should have been delivered",
            msg_id
        );
    }

    pm.shutdown();
}

/// Test that duplicate messages are only delivered once.
#[tokio::test]
async fn test_full_receive_duplicate_rejection() {
    let delegate = TrackingDelegate::new();

    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        delegate.clone(),
    ));

    pm.add_peer(99u64);
    pm.add_peer(100u64);

    // Start processor
    let pm_proc = pm.clone();
    tokio::spawn(async move {
        pm_proc.run_incoming_processor().await;
    });

    sleep(Duration::from_millis(10)).await;

    // Create a single message
    let msg_id = MessageId::new();
    let payload = Bytes::from("Duplicate test message");

    let gossip = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: payload.clone(),
    };

    // Send the same message from multiple senders (simulating duplicate receipt)
    pm.incoming_sender()
        .send((99u64, gossip.clone()))
        .await
        .unwrap();
    pm.incoming_sender()
        .send((100u64, gossip.clone()))
        .await
        .unwrap();
    pm.incoming_sender()
        .send((99u64, gossip.clone()))
        .await
        .unwrap();

    // Wait for processing
    sleep(Duration::from_millis(100)).await;

    // Should only be delivered once despite multiple sends
    assert_eq!(
        delegate.delivered_count(),
        1,
        "Duplicate messages should be rejected, but delivered {} times",
        delegate.delivered_count()
    );

    pm.shutdown();
}

/// Test IHave -> Graft cycle through the closed loop.
#[tokio::test]
async fn test_full_receive_ihave_graft_cycle() {
    let delegate = TrackingDelegate::new();

    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan()
            .with_graft_timeout(Duration::from_millis(100))
            .with_ihave_interval(Duration::from_millis(50)),
        delegate.clone(),
    ));

    // Add sender as lazy peer (will receive IHave, not direct Gossip)
    pm.add_peer_lazy(99u64);

    // Start processor
    let pm_proc = pm.clone();
    tokio::spawn(async move {
        pm_proc.run_incoming_processor().await;
    });

    sleep(Duration::from_millis(10)).await;

    // Send IHave message (announcing a message we don't have)
    let msg_id = MessageId::new();
    let ihave = PlumtreeMessage::IHave {
        message_ids: vec![msg_id].into(),
        round: 0,
    };

    pm.incoming_sender().send((99u64, ihave)).await.unwrap();

    // Wait for Graft timer to fire and request the message
    sleep(Duration::from_millis(200)).await;

    // The message won't be delivered because we only sent IHave (no actual Gossip with payload)
    // But we can verify the Graft mechanism was triggered by checking that
    // the peer was promoted to eager (since it had a message we wanted)
    let topology = pm.peers().topology();

    // Peer 99 should have been promoted to eager after we sent Graft
    assert!(
        topology.eager.contains(&99u64),
        "Peer should be promoted to eager after IHave triggers Graft"
    );

    pm.shutdown();
}

/// Test full two-node communication with simulated network routing.
#[tokio::test]
async fn test_full_two_node_communication() {
    use memberlist_plumtree::{
        decode_plumtree_envelope, ChannelTransport, PoolConfig, PooledTransport,
    };

    let delegate1 = TrackingDelegate::new();
    let delegate2 = TrackingDelegate::new();

    let pm1 = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        delegate1.clone(),
    ));
    let pm2 = Arc::new(PlumtreeDiscovery::new(
        2u64,
        PlumtreeConfig::lan(),
        delegate2.clone(),
    ));

    // Add each other as eager peers
    pm1.add_peer(2u64);
    pm2.add_peer(1u64);

    // Create transports
    let (transport1, rx1) = ChannelTransport::bounded(100);
    let (transport2, rx2) = ChannelTransport::bounded(100);
    let pooled1 = Arc::new(PooledTransport::new(transport1, PoolConfig::default()));
    let pooled2 = Arc::new(PooledTransport::new(transport2, PoolConfig::default()));

    // Start background tasks
    let pm1_t = pm1.clone();
    let t1 = pooled1.clone();
    tokio::spawn(async move { pm1_t.run_with_transport(t1).await });

    let pm2_t = pm2.clone();
    let t2 = pooled2.clone();
    tokio::spawn(async move { pm2_t.run_with_transport(t2).await });

    let pm1_proc = pm1.clone();
    tokio::spawn(async move { pm1_proc.run_incoming_processor().await });

    let pm2_proc = pm2.clone();
    tokio::spawn(async move { pm2_proc.run_incoming_processor().await });

    // Route messages between nodes (simulating network layer)
    let pm1_incoming = pm1.incoming_sender();
    let pm2_incoming = pm2.incoming_sender();

    tokio::spawn(async move {
        while let Ok((target, data)) = rx1.recv().await {
            if target == 2u64 {
                if let Some((sender, msg)) = decode_plumtree_envelope::<NodeId>(&data) {
                    let _ = pm2_incoming.send((sender, msg)).await;
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Ok((target, data)) = rx2.recv().await {
            if target == 1u64 {
                if let Some((sender, msg)) = decode_plumtree_envelope::<NodeId>(&data) {
                    let _ = pm1_incoming.send((sender, msg)).await;
                }
            }
        }
    });

    // Wait for setup
    sleep(Duration::from_millis(50)).await;

    // Node 1 broadcasts a message
    let broadcast_payload = Bytes::from("Hello from node 1!");
    let msg_id = pm1.broadcast(broadcast_payload.clone()).await.unwrap();

    // Wait for message to propagate
    let mut received = false;
    for _ in 0..40 {
        if delegate2.has_message(&msg_id) {
            received = true;
            break;
        }
        sleep(Duration::from_millis(25)).await;
    }

    assert!(received, "Node 2 should receive the broadcast from Node 1");

    // Verify content
    let messages = delegate2.get_messages();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].1, broadcast_payload);

    pm1.shutdown();
    pm2.shutdown();
}
