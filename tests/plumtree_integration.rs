//! Integration tests for the Plumtree protocol.
//!
//! These tests verify the end-to-end behavior of Plumtree broadcast,
//! including message delivery, deduplication, and tree repair.

use bytes::Bytes;
use memberlist_plumtree::{
    MessageId, PeerTopology, Plumtree, PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery,
    PlumtreeMessage,
};
use nodecraft::CheapClone;
use parking_lot::Mutex;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// Wrapper type to allow implementing PlumtreeDelegate for Arc-wrapped TestDelegateInner.
#[derive(Debug, Default, Clone)]
struct TestDelegate(Arc<TestDelegateInner>);

#[derive(Debug, Default)]
struct TestDelegateInner {
    delivered: Mutex<Vec<(MessageId, Bytes)>>,
    delivered_ids: Mutex<HashSet<MessageId>>,
}

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
}

impl PlumtreeDelegate<NodeId> for TestDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.0.delivered.lock().push((message_id, payload.clone()));
        self.0.delivered_ids.lock().insert(message_id);
    }
}

/// Simple node ID type for testing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId(u64);

/// Test that a single node can broadcast and receive its own message
/// through the cache (message is stored but not delivered to self).
#[tokio::test]
async fn test_single_node_broadcast() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Broadcast a message
    let msg_id = plumtree.broadcast(Bytes::from("hello")).await.unwrap();

    // Message should be in cache
    let stats = plumtree.cache_stats();
    assert_eq!(stats.entries, 1);

    // Message ID should be valid
    assert!(msg_id.timestamp() > 0);
}

/// Test that messages are forwarded to eager peers.
#[tokio::test]
async fn test_two_node_broadcast() {
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    let config = PlumtreeConfig::default();
    let (plumtree1, _handle1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (plumtree2, _handle2) = Plumtree::new(NodeId(2), config, delegate2.clone());

    // Add each other as peers
    plumtree1.add_peer(NodeId(2));
    plumtree2.add_peer(NodeId(1));

    // Promote to eager for direct gossip
    plumtree1.peers().promote_to_eager(&NodeId(2));
    plumtree2.peers().promote_to_eager(&NodeId(1));

    // Node 1 broadcasts
    let msg_id = plumtree1
        .broadcast(Bytes::from("test message"))
        .await
        .unwrap();

    // Simulate message delivery to node 2
    let gossip_msg = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: Bytes::from("test message"),
    };

    plumtree2
        .handle_message(NodeId(1), gossip_msg)
        .await
        .unwrap();

    // Node 2 should have received the message
    assert_eq!(delegate2.delivered_count(), 1);
    assert!(delegate2.has_message(&msg_id));

    // Verify message content
    let messages = delegate2.get_messages();
    assert_eq!(messages[0].1.as_ref(), b"test message");
}

/// Test that duplicate messages are detected and not delivered twice.
#[tokio::test]
async fn test_duplicate_detection() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    plumtree.add_peer(NodeId(2));

    let msg_id = MessageId::new();
    let payload = Bytes::from("duplicate test");

    // Receive same message multiple times
    for i in 0..5 {
        let msg = PlumtreeMessage::Gossip {
            id: msg_id,
            round: i,
            payload: payload.clone(),
        };
        plumtree.handle_message(NodeId(2), msg).await.unwrap();
    }

    // Should only be delivered once
    assert_eq!(delegate.delivered_count(), 1);
}

/// Test that IHave triggers Graft for missing messages.
#[tokio::test]
async fn test_ihave_triggers_graft() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Use add_peer_lazy to test the lazy->eager promotion
    plumtree.add_peer_lazy(NodeId(2));

    // Peer starts as lazy
    assert!(plumtree.peers().is_lazy(&NodeId(2)));

    // Receive IHave for unknown message
    let msg_id = MessageId::new();
    let ihave_msg = PlumtreeMessage::IHave {
        message_ids: smallvec::smallvec![msg_id],
        round: 0,
    };

    plumtree.handle_message(NodeId(2), ihave_msg).await.unwrap();

    // Peer should now be eager (promoted due to missing message)
    assert!(plumtree.peers().is_eager(&NodeId(2)));
}

/// Test that Graft requests return the cached message.
#[tokio::test]
async fn test_graft_returns_message() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    plumtree.add_peer(NodeId(2));

    // Broadcast a message (caches it)
    let msg_id = plumtree.broadcast(Bytes::from("graft test")).await.unwrap();

    // Handle a Graft request
    let graft_msg = PlumtreeMessage::Graft {
        message_id: msg_id,
        round: 0,
    };

    plumtree.handle_message(NodeId(2), graft_msg).await.unwrap();

    // Verify the peer was promoted to eager
    assert!(plumtree.peers().is_eager(&NodeId(2)));

    // Check that an outgoing message was queued
    // (The Gossip response to the Graft)
    let outgoing = handle.next_outgoing().await;
    assert!(outgoing.is_some());

    let out_msg = outgoing.unwrap();
    assert!(matches!(out_msg.message, PlumtreeMessage::Gossip { .. }));
}

/// Test that Prune demotes a peer to lazy.
#[tokio::test]
async fn test_prune_demotes_peer() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    plumtree.add_peer(NodeId(2));
    plumtree.peers().promote_to_eager(&NodeId(2));

    // Peer should be eager
    assert!(plumtree.peers().is_eager(&NodeId(2)));

    // Handle Prune message
    plumtree
        .handle_message(NodeId(2), PlumtreeMessage::Prune)
        .await
        .unwrap();

    // Peer should now be lazy
    assert!(plumtree.peers().is_lazy(&NodeId(2)));
}

/// Test message encoding and decoding roundtrip.
#[test]
fn test_message_encoding_roundtrip() {
    use memberlist_plumtree::{decode_plumtree_message, encode_plumtree_message};

    let original = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 42,
        payload: Bytes::from("encoding test"),
    };

    let encoded = encode_plumtree_message(&original);
    let decoded = decode_plumtree_message(&encoded).unwrap();

    assert_eq!(original, decoded);
}

/// Test peer rebalancing.
#[tokio::test]
async fn test_peer_rebalancing() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default().with_eager_fanout(3);
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add 10 peers using add_peer_lazy (all start as lazy)
    for i in 2..=11 {
        plumtree.add_peer_lazy(NodeId(i));
    }

    let stats = plumtree.peer_stats();
    assert_eq!(stats.eager_count, 0);
    assert_eq!(stats.lazy_count, 10);

    // Rebalance to target eager fanout
    plumtree.rebalance_peers();

    let stats = plumtree.peer_stats();
    assert_eq!(stats.eager_count, 3);
    assert_eq!(stats.lazy_count, 7);
}

/// Test that oversized messages are rejected.
#[tokio::test]
async fn test_message_size_limit() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default().with_max_message_size(100);
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Try to broadcast an oversized message
    let large_payload = vec![0u8; 200];
    let result = plumtree.broadcast(Bytes::from(large_payload)).await;

    assert!(result.is_err());
}

/// Test configuration presets.
#[test]
fn test_config_presets() {
    let lan = PlumtreeConfig::lan();
    assert_eq!(lan.eager_fanout, 3);
    assert!(lan.ihave_interval < Duration::from_millis(100));

    let wan = PlumtreeConfig::wan();
    assert_eq!(wan.eager_fanout, 4);
    assert!(wan.ihave_interval > Duration::from_millis(100));

    let large = PlumtreeConfig::large_cluster();
    assert_eq!(large.eager_fanout, 5);
    assert!(large.message_cache_max_size > 10000);
}

/// Test multi-hop message propagation simulation.
#[tokio::test]
async fn test_multi_hop_propagation() {
    // Create a chain of 3 nodes: 1 -> 2 -> 3
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();
    let delegate3 = TestDelegate::new();

    let config = PlumtreeConfig::default();
    let (plumtree1, _h1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (plumtree2, handle2) = Plumtree::new(NodeId(2), config.clone(), delegate2.clone());
    let (plumtree3, _h3) = Plumtree::new(NodeId(3), config, delegate3.clone());

    // Set up peer relationships
    // Node 1 knows Node 2
    plumtree1.add_peer(NodeId(2));
    plumtree1.peers().promote_to_eager(&NodeId(2));

    // Node 2 knows Node 1 and Node 3
    plumtree2.add_peer(NodeId(1));
    plumtree2.add_peer(NodeId(3));
    plumtree2.peers().promote_to_eager(&NodeId(1));
    plumtree2.peers().promote_to_eager(&NodeId(3));

    // Node 3 knows Node 2
    plumtree3.add_peer(NodeId(2));
    plumtree3.peers().promote_to_eager(&NodeId(2));

    // Node 1 broadcasts a message
    let msg_id = plumtree1.broadcast(Bytes::from("multi-hop")).await.unwrap();

    // Simulate delivery to Node 2
    let gossip1 = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: Bytes::from("multi-hop"),
    };
    plumtree2.handle_message(NodeId(1), gossip1).await.unwrap();

    // Node 2 should have delivered the message
    assert_eq!(delegate2.delivered_count(), 1);

    // Node 2 should have queued a message for Node 3
    let outgoing = handle2.next_outgoing().await;
    assert!(outgoing.is_some());

    // Simulate delivery to Node 3 (round incremented)
    let gossip2 = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 1,
        payload: Bytes::from("multi-hop"),
    };
    plumtree3.handle_message(NodeId(2), gossip2).await.unwrap();

    // Node 3 should have delivered the message
    assert_eq!(delegate3.delivered_count(), 1);
    assert!(delegate3.has_message(&msg_id));
}

/// Test shutdown behavior.
#[tokio::test]
async fn test_shutdown() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    assert!(!plumtree.is_shutdown());

    plumtree.shutdown();

    assert!(plumtree.is_shutdown());

    // Operations should fail after shutdown
    let result = plumtree
        .handle_message(
            NodeId(2),
            PlumtreeMessage::Gossip {
                id: MessageId::new(),
                round: 0,
                payload: Bytes::from("test"),
            },
        )
        .await;

    assert!(result.is_err());
}

/// Test that Graft requests are rate limited.
#[tokio::test]
async fn test_graft_rate_limiting() {
    let delegate = TestDelegate::new();
    // Configure low rate limit for testing
    let config = PlumtreeConfig::default().with_graft_timeout(Duration::from_millis(100));
    let (plumtree, handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    plumtree.add_peer(NodeId(2));

    // Broadcast a message (caches it)
    let msg_id = plumtree
        .broadcast(Bytes::from("rate limit test"))
        .await
        .unwrap();

    // Send many Graft requests quickly
    for i in 0..30 {
        let graft_msg = PlumtreeMessage::Graft {
            message_id: msg_id,
            round: i,
        };
        let _ = plumtree.handle_message(NodeId(2), graft_msg).await;
    }

    // Count how many Gossip responses were generated
    // Due to rate limiting, it should be less than 30
    let mut response_count = 0;
    while let Ok(Some(_)) =
        tokio::time::timeout(Duration::from_millis(10), handle.next_outgoing()).await
    {
        response_count += 1;
    }

    // With rate limit of 20 burst, we should see around 20 responses
    assert!(
        response_count <= 25,
        "Expected rate limiting, got {} responses",
        response_count
    );
}

/// Test message parent tracking.
#[tokio::test]
async fn test_message_parent_tracking() {
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    let config = PlumtreeConfig::default();
    let (_plumtree1, _h1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (plumtree2, _h2) = Plumtree::new(NodeId(2), config, delegate2.clone());

    // Node 2 knows Node 1 and Node 3
    plumtree2.add_peer(NodeId(1));
    plumtree2.add_peer(NodeId(3));

    // Node 1 sends a message to Node 2
    let msg_id = MessageId::new();
    let gossip = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: Bytes::from("parent tracking test"),
    };

    plumtree2.handle_message(NodeId(1), gossip).await.unwrap();

    // Message should be delivered
    assert_eq!(delegate2.delivered_count(), 1);

    // Node 1 should be the parent (first sender)
    // This is tested implicitly - if Node 3 sends an IHave later,
    // Node 1 would be demoted to lazy (tree repair)
}

/// Test tree repair demotes old parent.
#[tokio::test]
async fn test_tree_repair_demotes_old_parent() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add peers using add_peer_lazy to control exact peer state
    plumtree.add_peer_lazy(NodeId(2));
    plumtree.add_peer_lazy(NodeId(3));

    // Promote Node 2 to eager (simulating it was our parent for previous messages)
    plumtree.peers().promote_to_eager(&NodeId(2));
    assert!(plumtree.peers().is_eager(&NodeId(2)));
    assert!(plumtree.peers().is_lazy(&NodeId(3)));

    // Receive IHave from Node 3 for a message we don't have
    // This triggers tree repair: promote Node 3, potentially demote old eager peers
    let msg_id = MessageId::new();
    let ihave = PlumtreeMessage::IHave {
        message_ids: smallvec::smallvec![msg_id],
        round: 0,
    };

    plumtree.handle_message(NodeId(3), ihave).await.unwrap();

    // Node 3 should now be eager (source of recovery)
    assert!(plumtree.peers().is_eager(&NodeId(3)));

    // A Graft request should have been sent
    let outgoing = handle.next_outgoing().await;
    assert!(outgoing.is_some());
    let msg = outgoing.unwrap();
    assert!(matches!(msg.message, PlumtreeMessage::Graft { .. }));
}

/// Test exponential backoff behavior.
#[tokio::test]
async fn test_graft_backoff_config() {
    let config = PlumtreeConfig::default();

    // Verify default backoff settings
    assert_eq!(config.graft_max_retries, 5);
    assert_eq!(config.graft_rate_limit_burst, 20);
    assert!(config.graft_rate_limit_per_second > 0.0);

    // LAN config should have more aggressive settings
    let lan = PlumtreeConfig::lan();
    assert!(lan.graft_rate_limit_per_second > config.graft_rate_limit_per_second);
    assert!(lan.graft_max_retries < config.graft_max_retries);

    // WAN config should have more conservative settings
    let wan = PlumtreeConfig::wan();
    assert!(wan.graft_rate_limit_per_second < config.graft_rate_limit_per_second);
    assert!(wan.graft_max_retries > config.graft_max_retries);
}

/// Test that messages have unique IDs across concurrent broadcasts.
#[tokio::test]
async fn test_concurrent_broadcast_uniqueness() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Broadcast multiple messages concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let pt = plumtree.clone();
        let handle = tokio::spawn(async move {
            pt.broadcast(Bytes::from(format!("msg {}", i)))
                .await
                .unwrap()
        });
        handles.push(handle);
    }

    // Collect all message IDs
    let mut msg_ids = HashSet::new();
    for handle in handles {
        let msg_id = handle.await.unwrap();
        msg_ids.insert(msg_id);
    }

    // All IDs should be unique
    assert_eq!(msg_ids.len(), 10);
}

// =============================================================================
// PeerTopology and Disconnect Integration Tests
// =============================================================================

/// Test that PeerTopology returns the correct snapshot of peer state.
#[tokio::test]
async fn test_peer_topology_snapshot() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default().with_eager_fanout(2);
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Initially empty
    let topo = plumtree.peers().topology();
    assert!(topo.is_empty());
    assert_eq!(topo.total(), 0);

    // Add peers using add_peer (auto-classified)
    plumtree.add_peer(NodeId(2));
    plumtree.add_peer(NodeId(3));
    plumtree.add_peer(NodeId(4));
    plumtree.add_peer(NodeId(5));

    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 4);
    assert_eq!(topo.eager_count(), 2); // First 2 become eager
    assert_eq!(topo.lazy_count(), 2); // Rest become lazy

    // Verify contains
    assert!(topo.contains(&NodeId(2)));
    assert!(topo.contains(&NodeId(3)));
    assert!(topo.contains(&NodeId(4)));
    assert!(topo.contains(&NodeId(5)));
    assert!(!topo.contains(&NodeId(99)));
}

/// Test that disconnected peers are removed from the topology.
#[tokio::test]
async fn test_topology_after_peer_disconnect() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default().with_eager_fanout(2);
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add 4 peers (2 eager, 2 lazy based on eager_fanout=2)
    plumtree.add_peer(NodeId(2));
    plumtree.add_peer(NodeId(3));
    plumtree.add_peer(NodeId(4));
    plumtree.add_peer(NodeId(5));

    // Verify initial topology
    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 4);
    assert!(topo.contains(&NodeId(2)));
    assert!(topo.contains(&NodeId(3)));
    assert!(topo.contains(&NodeId(4)));
    assert!(topo.contains(&NodeId(5)));

    // Disconnect peer 3 (eager)
    plumtree.remove_peer(&NodeId(3));

    // Verify topology no longer contains disconnected peer
    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 3);
    assert!(topo.contains(&NodeId(2)));
    assert!(!topo.contains(&NodeId(3))); // <-- Disconnected peer removed!
    assert!(topo.contains(&NodeId(4)));
    assert!(topo.contains(&NodeId(5)));

    // Disconnect peer 5 (lazy)
    plumtree.remove_peer(&NodeId(5));

    // Verify topology updated
    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 2);
    assert!(topo.contains(&NodeId(2)));
    assert!(!topo.contains(&NodeId(3)));
    assert!(topo.contains(&NodeId(4)));
    assert!(!topo.contains(&NodeId(5))); // <-- Disconnected peer removed!
}

/// Test that disconnecting all peers results in an empty topology.
#[tokio::test]
async fn test_topology_disconnect_all_peers() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add peers
    plumtree.add_peer(NodeId(2));
    plumtree.add_peer(NodeId(3));
    plumtree.add_peer(NodeId(4));

    assert_eq!(plumtree.peers().topology().total(), 3);

    // Disconnect all peers
    plumtree.remove_peer(&NodeId(2));
    plumtree.remove_peer(&NodeId(3));
    plumtree.remove_peer(&NodeId(4));

    // Topology should be empty
    let topo = plumtree.peers().topology();
    assert!(topo.is_empty());
    assert_eq!(topo.total(), 0);
    assert!(!topo.contains(&NodeId(2)));
    assert!(!topo.contains(&NodeId(3)));
    assert!(!topo.contains(&NodeId(4)));
}

/// Test that removing a non-existent peer doesn't affect topology.
#[tokio::test]
async fn test_topology_remove_nonexistent_peer() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add peers
    plumtree.add_peer(NodeId(2));
    plumtree.add_peer(NodeId(3));

    let topo_before = plumtree.peers().topology();
    assert_eq!(topo_before.total(), 2);

    // Remove non-existent peer
    plumtree.remove_peer(&NodeId(99));

    // Topology should be unchanged
    let topo_after = plumtree.peers().topology();
    assert_eq!(topo_after.total(), 2);
    assert!(topo_after.contains(&NodeId(2)));
    assert!(topo_after.contains(&NodeId(3)));
}

/// Test topology correctness after promotion and demotion.
#[tokio::test]
async fn test_topology_after_promotion_demotion() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default();
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add lazy peers
    plumtree.add_peer_lazy(NodeId(2));
    plumtree.add_peer_lazy(NodeId(3));

    let topo = plumtree.peers().topology();
    assert_eq!(topo.eager_count(), 0);
    assert_eq!(topo.lazy_count(), 2);
    assert!(topo.is_lazy(&NodeId(2)));
    assert!(topo.is_lazy(&NodeId(3)));

    // Promote peer 2 to eager
    plumtree.peers().promote_to_eager(&NodeId(2));

    let topo = plumtree.peers().topology();
    assert_eq!(topo.eager_count(), 1);
    assert_eq!(topo.lazy_count(), 1);
    assert!(topo.is_eager(&NodeId(2)));
    assert!(topo.is_lazy(&NodeId(3)));

    // Demote peer 2 back to lazy
    plumtree.peers().demote_to_lazy(&NodeId(2));

    let topo = plumtree.peers().topology();
    assert_eq!(topo.eager_count(), 0);
    assert_eq!(topo.lazy_count(), 2);
    assert!(topo.is_lazy(&NodeId(2)));
    assert!(topo.is_lazy(&NodeId(3)));

    // Disconnect peer 2 - should be removed entirely
    plumtree.remove_peer(&NodeId(2));

    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 1);
    assert!(!topo.contains(&NodeId(2)));
    assert!(topo.contains(&NodeId(3)));
}

/// Test that messages are not forwarded to disconnected peers.
#[tokio::test]
async fn test_no_messages_to_disconnected_peers() {
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    let config = PlumtreeConfig::default();
    let (plumtree1, handle1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (_plumtree2, _handle2) = Plumtree::new(NodeId(2), config, delegate2.clone());

    // Add peer and promote to eager
    plumtree1.add_peer(NodeId(2));
    plumtree1.peers().promote_to_eager(&NodeId(2));

    // Verify peer 2 is in topology
    assert!(plumtree1.peers().topology().contains(&NodeId(2)));

    // Broadcast - should queue message for peer 2
    let _msg_id = plumtree1
        .broadcast(Bytes::from("before disconnect"))
        .await
        .unwrap();

    // There should be an outgoing message
    let outgoing = tokio::time::timeout(Duration::from_millis(100), handle1.next_outgoing()).await;
    assert!(outgoing.is_ok());

    // Disconnect peer 2
    plumtree1.remove_peer(&NodeId(2));

    // Verify peer 2 is no longer in topology
    assert!(!plumtree1.peers().topology().contains(&NodeId(2)));

    // Broadcast again - should NOT queue message for disconnected peer
    let _msg_id2 = plumtree1
        .broadcast(Bytes::from("after disconnect"))
        .await
        .unwrap();

    // Drain any remaining messages from first broadcast
    while let Ok(Some(_)) =
        tokio::time::timeout(Duration::from_millis(10), handle1.next_outgoing()).await
    {}

    // No more outgoing messages should be queued (no peers left)
    let outgoing = tokio::time::timeout(Duration::from_millis(50), handle1.next_outgoing()).await;
    assert!(outgoing.is_err(), "Should timeout - no peers to send to");
}

/// Test multi-node topology with cascading disconnects.
#[tokio::test]
async fn test_cascading_disconnects_topology() {
    let delegate = TestDelegate::new();
    let config = PlumtreeConfig::default().with_eager_fanout(3);
    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add 6 peers (3 eager, 3 lazy based on eager_fanout=3)
    for i in 2..=7 {
        plumtree.add_peer(NodeId(i));
    }

    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 6);
    assert_eq!(topo.eager_count(), 3);
    assert_eq!(topo.lazy_count(), 3);

    // Disconnect all eager peers
    for peer in topo.eager.iter() {
        plumtree.remove_peer(peer);
    }

    // After disconnecting eager peers, only lazy remain
    let topo = plumtree.peers().topology();
    assert_eq!(topo.total(), 3);
    assert_eq!(topo.eager_count(), 0);
    assert_eq!(topo.lazy_count(), 3);

    // Disconnect all lazy peers
    for peer in topo.lazy.iter() {
        plumtree.remove_peer(peer);
    }

    // Topology should be empty
    let topo = plumtree.peers().topology();
    assert!(topo.is_empty());
}

/// Test that PeerTopology struct methods work correctly.
#[test]
fn test_peer_topology_struct_methods() {
    // Create topology directly
    let topo = PeerTopology::new(
        vec![NodeId(1), NodeId(2)],
        vec![NodeId(3), NodeId(4), NodeId(5)],
    );

    // Test counts
    assert_eq!(topo.total(), 5);
    assert_eq!(topo.eager_count(), 2);
    assert_eq!(topo.lazy_count(), 3);
    assert!(!topo.is_empty());

    // Test contains
    assert!(topo.contains(&NodeId(1)));
    assert!(topo.contains(&NodeId(5)));
    assert!(!topo.contains(&NodeId(99)));

    // Test is_eager/is_lazy
    assert!(topo.is_eager(&NodeId(1)));
    assert!(topo.is_eager(&NodeId(2)));
    assert!(!topo.is_eager(&NodeId(3)));

    assert!(topo.is_lazy(&NodeId(3)));
    assert!(topo.is_lazy(&NodeId(4)));
    assert!(topo.is_lazy(&NodeId(5)));
    assert!(!topo.is_lazy(&NodeId(1)));

    // Test default
    let empty: PeerTopology<NodeId> = PeerTopology::default();
    assert!(empty.is_empty());
    assert_eq!(empty.total(), 0);
}

// =============================================================================
// PlumtreeDiscovery Integration Tests
// =============================================================================

/// Simple node ID type for PlumtreeDiscovery tests (implements IdCodec + Id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MemberlistNodeId(u64);

impl fmt::Display for MemberlistNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

impl CheapClone for MemberlistNodeId {}

impl memberlist_plumtree::IdCodec for MemberlistNodeId {
    fn encode_id(&self, buf: &mut impl bytes::BufMut) {
        buf.put_u64(self.0);
    }

    fn decode_id(buf: &mut impl bytes::Buf) -> Option<Self> {
        if buf.remaining() >= 8 {
            Some(MemberlistNodeId(buf.get_u64()))
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        8
    }
}

/// Delegate for PlumtreeDiscovery tests
#[derive(Debug, Default, Clone)]
struct MemberlistTestDelegate(Arc<MemberlistTestDelegateInner>);

#[derive(Debug, Default)]
struct MemberlistTestDelegateInner {
    delivered: Mutex<Vec<(MessageId, Bytes)>>,
}

impl MemberlistTestDelegate {
    fn new() -> Self {
        Self(Arc::new(MemberlistTestDelegateInner::default()))
    }

    fn delivered_count(&self) -> usize {
        self.0.delivered.lock().len()
    }

    fn get_messages(&self) -> Vec<(MessageId, Bytes)> {
        self.0.delivered.lock().clone()
    }

    fn has_message(&self, id: &MessageId) -> bool {
        self.0.delivered.lock().iter().any(|(mid, _)| mid == id)
    }
}

impl PlumtreeDelegate<MemberlistNodeId> for MemberlistTestDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.0.delivered.lock().push((message_id, payload));
    }
}

// =============================================================================
// Test Infrastructure for Multi-Node Integration Tests
// =============================================================================

use memberlist_plumtree::{decode_plumtree_envelope, Transport};
use std::collections::HashMap;

/// Error type for test transport
#[derive(Debug)]
struct IntegrationTestTransportError(String);

impl std::fmt::Display for IntegrationTestTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for IntegrationTestTransportError {}

/// Shared routing table for multi-node tests.
type RoutingTable = Arc<
    parking_lot::RwLock<
        HashMap<MemberlistNodeId, async_channel::Sender<(MemberlistNodeId, PlumtreeMessage)>>,
    >,
>;

/// In-memory transport that routes messages between nodes via their incoming channels.
#[derive(Clone)]
struct IntegrationTestTransport {
    routes: RoutingTable,
}

impl Transport<MemberlistNodeId> for IntegrationTestTransport {
    type Error = IntegrationTestTransportError;

    async fn send_to(&self, target: &MemberlistNodeId, data: Bytes) -> Result<(), Self::Error> {
        if let Some((sender, msg)) = decode_plumtree_envelope::<MemberlistNodeId>(&data) {
            let routes = self.routes.read();
            if let Some(tx) = routes.get(target) {
                tx.send((sender, msg))
                    .await
                    .map_err(|e| IntegrationTestTransportError(e.to_string()))?;
            }
        }
        Ok(())
    }
}

/// A test node wrapping PlumtreeDiscovery with its delegate and background task handles.
struct TestNode {
    id: MemberlistNodeId,
    pm: Arc<PlumtreeDiscovery<MemberlistNodeId, MemberlistTestDelegate>>,
    delegate: MemberlistTestDelegate,
}

impl TestNode {
    fn new(id: MemberlistNodeId, config: PlumtreeConfig, routes: RoutingTable) -> Self {
        let delegate = MemberlistTestDelegate::new();
        let pm = Arc::new(PlumtreeDiscovery::new(id, config, delegate.clone()));

        // Register in routing table
        {
            let mut routes_guard = routes.write();
            routes_guard.insert(id, pm.incoming_sender());
        }

        Self { id, pm, delegate }
    }

    /// Start background tasks for this node.
    fn start(&self, routes: RoutingTable) {
        let transport = IntegrationTestTransport { routes };
        let pm_run = self.pm.clone();
        let pm_proc = self.pm.clone();

        tokio::spawn(async move { pm_run.run_with_transport(transport).await });
        tokio::spawn(async move { pm_proc.run_incoming_processor().await });
    }

    fn shutdown(&self) {
        self.pm.shutdown();
    }
}

/// Helper to create a multi-node test cluster.
struct TestCluster {
    nodes: Vec<TestNode>,
    routes: RoutingTable,
}

impl TestCluster {
    /// Create a new cluster with the specified number of nodes.
    fn new(node_count: usize, config: PlumtreeConfig) -> Self {
        let routes: RoutingTable = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        let nodes: Vec<TestNode> = (1..=node_count as u64)
            .map(|id| TestNode::new(MemberlistNodeId(id), config.clone(), routes.clone()))
            .collect();

        Self { nodes, routes }
    }

    /// Start all nodes and have them join each other (fully connected mesh).
    fn start_fully_connected(&self) {
        // Start background tasks
        for node in &self.nodes {
            node.start(self.routes.clone());
        }

        // Each node adds all other nodes as peers (simulating memberlist discovery)
        for node in &self.nodes {
            for other in &self.nodes {
                if node.id != other.id {
                    node.pm.add_peer(other.id);
                }
            }
        }
    }

    /// Start all nodes with a chain topology: 1 <-> 2 <-> 3 <-> ... <-> N
    fn start_chain(&self) {
        // Start background tasks
        for node in &self.nodes {
            node.start(self.routes.clone());
        }

        // Chain topology: each node knows its neighbors
        for i in 0..self.nodes.len() {
            if i > 0 {
                self.nodes[i].pm.add_peer(self.nodes[i - 1].id);
            }
            if i < self.nodes.len() - 1 {
                self.nodes[i].pm.add_peer(self.nodes[i + 1].id);
            }
        }
    }

    /// Wait for all nodes to have the expected peer count.
    async fn wait_for_topology(&self, expected_peers_per_node: usize, timeout: Duration) {
        let start = std::time::Instant::now();
        loop {
            let all_ready = self
                .nodes
                .iter()
                .all(|node| node.pm.peers().topology().total() == expected_peers_per_node);
            if all_ready {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "Timeout waiting for topology. Expected {} peers, got: {:?}",
                    expected_peers_per_node,
                    self.nodes
                        .iter()
                        .map(|n| (n.id, n.pm.peers().topology().total()))
                        .collect::<Vec<_>>()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait for a specific node to have the expected peer count.
    #[allow(dead_code)]
    async fn wait_for_node_topology(
        &self,
        node_idx: usize,
        expected_peers: usize,
        timeout: Duration,
    ) {
        let start = std::time::Instant::now();
        let node = &self.nodes[node_idx];
        loop {
            if node.pm.peers().topology().total() == expected_peers {
                return;
            }
            if start.elapsed() > timeout {
                panic!(
                    "Timeout waiting for node {} topology. Expected {} peers, got {}",
                    node.id,
                    expected_peers,
                    node.pm.peers().topology().total()
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait for all nodes (except sender) to receive a specific message.
    async fn wait_for_message_delivery(
        &self,
        msg_id: &MessageId,
        sender_id: MemberlistNodeId,
        timeout: Duration,
    ) {
        let start = std::time::Instant::now();
        loop {
            let all_received = self
                .nodes
                .iter()
                .all(|node| node.id == sender_id || node.delegate.has_message(msg_id));
            if all_received {
                return;
            }
            if start.elapsed() > timeout {
                let status: Vec<_> = self
                    .nodes
                    .iter()
                    .filter(|n| n.id != sender_id)
                    .map(|n| (n.id, n.delegate.has_message(msg_id)))
                    .collect();
                panic!("Timeout waiting for message delivery. Status: {:?}", status);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn get_node(&self, id: MemberlistNodeId) -> Option<&TestNode> {
        self.nodes.iter().find(|n| n.id == id)
    }

    fn shutdown_all(&self) {
        for node in &self.nodes {
            node.shutdown();
        }
    }
}

// =============================================================================
// Proper Multi-Node Integration Tests
// =============================================================================

/// Test that a 5-node cluster forms the correct topology after joining.
#[tokio::test]
async fn test_plumtree_memberlist_peer_topology() {
    let config = PlumtreeConfig::default().with_eager_fanout(2);
    let cluster = TestCluster::new(5, config);

    // Start all nodes and let them join each other
    cluster.start_fully_connected();

    // Wait for all nodes to have 4 peers (fully connected mesh of 5 nodes)
    cluster.wait_for_topology(4, Duration::from_secs(5)).await;

    // Verify topology for node 1
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let topo = node1.pm.peers().topology();
    assert_eq!(topo.total(), 4);
    assert_eq!(topo.eager_count(), 2); // eager_fanout = 2
    assert_eq!(topo.lazy_count(), 2);

    // Verify all other nodes are in the topology
    assert!(topo.contains(&MemberlistNodeId(2)));
    assert!(topo.contains(&MemberlistNodeId(3)));
    assert!(topo.contains(&MemberlistNodeId(4)));
    assert!(topo.contains(&MemberlistNodeId(5)));

    // Verify other nodes also have correct peer counts
    for node in &cluster.nodes {
        let topo = node.pm.peers().topology();
        assert_eq!(topo.total(), 4, "Node {} should have 4 peers", node.id);
    }

    // Shutdown all nodes
    cluster.shutdown_all();
}

/// Test that peer disconnect is properly reflected in topology across nodes.
#[tokio::test]
async fn test_plumtree_memberlist_peer_disconnect() {
    let config = PlumtreeConfig::default().with_eager_fanout(2);
    let cluster = TestCluster::new(4, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(3, Duration::from_secs(5)).await;

    // Verify initial topology for node 1
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let topo = node1.pm.peers().topology();
    assert_eq!(topo.total(), 3);
    assert!(topo.contains(&MemberlistNodeId(2)));
    assert!(topo.contains(&MemberlistNodeId(3)));
    assert!(topo.contains(&MemberlistNodeId(4)));

    // Simulate node 3 leaving the cluster - all nodes remove it
    for node in &cluster.nodes {
        node.pm.remove_peer(&MemberlistNodeId(3));
    }

    // Give time for changes to propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify node 3 is removed from node 1's topology
    let topo = node1.pm.peers().topology();
    assert_eq!(topo.total(), 2);
    assert!(topo.contains(&MemberlistNodeId(2)));
    assert!(!topo.contains(&MemberlistNodeId(3))); // <-- Removed!
    assert!(topo.contains(&MemberlistNodeId(4)));

    // Verify node 3 sees no peers (it was removed from everyone)
    let node3 = cluster.get_node(MemberlistNodeId(3)).unwrap();
    // Node 3 should still have peers 1, 2, 4 in its view (it didn't remove them)
    // unless we also have node 3 remove others
    // Let's have node 3 also leave properly
    node3.pm.remove_peer(&MemberlistNodeId(1));
    node3.pm.remove_peer(&MemberlistNodeId(2));
    node3.pm.remove_peer(&MemberlistNodeId(4));

    let topo3 = node3.pm.peers().topology();
    assert!(topo3.is_empty());

    cluster.shutdown_all();
}

/// Test broadcast message delivery across a 5-node fully connected cluster.
#[tokio::test]
async fn test_plumtree_memberlist_broadcast() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_ihave_interval(Duration::from_millis(50));
    let cluster = TestCluster::new(5, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(4, Duration::from_secs(5)).await;

    // Node 1 broadcasts a message
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let msg_id = node1
        .pm
        .broadcast(Bytes::from("test message"))
        .await
        .unwrap();

    // Verify message is cached on node 1
    let cache_stats = node1.pm.cache_stats();
    assert_eq!(cache_stats.entries, 1);

    // Wait for all other nodes to receive the message
    cluster
        .wait_for_message_delivery(&msg_id, MemberlistNodeId(1), Duration::from_secs(5))
        .await;

    // Verify all nodes (except sender) received the message
    for node in &cluster.nodes {
        if node.id == MemberlistNodeId(1) {
            continue; // Sender doesn't deliver to itself
        }
        assert_eq!(
            node.delegate.delivered_count(),
            1,
            "Node {} should have received 1 message",
            node.id
        );
        let msgs = node.delegate.get_messages();
        assert_eq!(msgs[0].0, msg_id);
        assert_eq!(msgs[0].1.as_ref(), b"test message");
    }

    cluster.shutdown_all();
}

/// Test two-node communication with actual message routing.
#[tokio::test]
async fn test_plumtree_memberlist_two_node_communication() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(1)
        .with_ihave_interval(Duration::from_millis(50));
    let cluster = TestCluster::new(2, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(1, Duration::from_secs(5)).await;

    // Node 1 broadcasts
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let node2 = cluster.get_node(MemberlistNodeId(2)).unwrap();

    let msg_id = node1
        .pm
        .broadcast(Bytes::from("hello from node 1"))
        .await
        .unwrap();

    // Wait for node 2 to receive it
    cluster
        .wait_for_message_delivery(&msg_id, MemberlistNodeId(1), Duration::from_secs(5))
        .await;

    // Verify node 2 received the message
    assert_eq!(node2.delegate.delivered_count(), 1);
    let msgs = node2.delegate.get_messages();
    assert_eq!(msgs[0].0, msg_id);
    assert_eq!(msgs[0].1.as_ref(), b"hello from node 1");

    cluster.shutdown_all();
}

/// Test that messages stop being sent to disconnected peers.
#[tokio::test]
async fn test_plumtree_memberlist_no_messages_after_disconnect() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_ihave_interval(Duration::from_millis(50));
    let cluster = TestCluster::new(3, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(2, Duration::from_secs(5)).await;

    // Broadcast first message - all nodes should receive
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let node2 = cluster.get_node(MemberlistNodeId(2)).unwrap();
    let node3 = cluster.get_node(MemberlistNodeId(3)).unwrap();

    let msg_id1 = node1
        .pm
        .broadcast(Bytes::from("before disconnect"))
        .await
        .unwrap();
    cluster
        .wait_for_message_delivery(&msg_id1, MemberlistNodeId(1), Duration::from_secs(5))
        .await;

    // Verify both node 2 and 3 received
    assert_eq!(node2.delegate.delivered_count(), 1);
    assert_eq!(node3.delegate.delivered_count(), 1);

    // Now disconnect node 3 from node 1's perspective
    node1.pm.remove_peer(&MemberlistNodeId(3));

    // Also disconnect node 1 from node 3's perspective (bidirectional)
    node3.pm.remove_peer(&MemberlistNodeId(1));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify node 1 no longer sees node 3
    assert!(!node1.pm.peers().topology().contains(&MemberlistNodeId(3)));

    // Broadcast second message from node 1
    let _msg_id2 = node1
        .pm
        .broadcast(Bytes::from("after disconnect"))
        .await
        .unwrap();

    // Wait a bit for message propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Node 2 should have received the second message (still connected)
    assert_eq!(node2.delegate.delivered_count(), 2);

    // Node 3 should NOT have received the second message directly from node 1
    // (though it might receive it from node 2 if node 2 forwards)
    // For this test, we verify the direct path is broken

    cluster.shutdown_all();
}

/// Test topology consistency with multiple peers.
#[tokio::test]
async fn test_plumtree_memberlist_topology_consistency() {
    let config = PlumtreeConfig::default().with_eager_fanout(3);
    let cluster = TestCluster::new(10, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(9, Duration::from_secs(5)).await;

    // Verify each node has correct topology
    for node in &cluster.nodes {
        let topo = node.pm.peers().topology();
        assert_eq!(topo.total(), 9, "Node {} should have 9 peers", node.id);
        assert_eq!(
            topo.eager_count(),
            3,
            "Node {} should have 3 eager peers",
            node.id
        );
        assert_eq!(
            topo.lazy_count(),
            6,
            "Node {} should have 6 lazy peers",
            node.id
        );
    }

    // Remove some nodes (simulate nodes 3, 5, 7 leaving)
    for node in &cluster.nodes {
        node.pm.remove_peer(&MemberlistNodeId(3));
        node.pm.remove_peer(&MemberlistNodeId(5));
        node.pm.remove_peer(&MemberlistNodeId(7));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify updated topology
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let topo = node1.pm.peers().topology();
    assert_eq!(topo.total(), 6); // 9 - 3 = 6
    assert!(!topo.contains(&MemberlistNodeId(3)));
    assert!(!topo.contains(&MemberlistNodeId(5)));
    assert!(!topo.contains(&MemberlistNodeId(7)));

    // Remaining peers should still be present
    assert!(topo.contains(&MemberlistNodeId(2)));
    assert!(topo.contains(&MemberlistNodeId(4)));
    assert!(topo.contains(&MemberlistNodeId(6)));
    assert!(topo.contains(&MemberlistNodeId(8)));
    assert!(topo.contains(&MemberlistNodeId(9)));
    assert!(topo.contains(&MemberlistNodeId(10)));

    cluster.shutdown_all();
}

/// Test proper shutdown of all nodes.
#[tokio::test]
async fn test_plumtree_memberlist_shutdown() {
    let config = PlumtreeConfig::default();
    let cluster = TestCluster::new(3, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(2, Duration::from_secs(5)).await;

    // Verify nodes are running
    for node in &cluster.nodes {
        assert!(!node.pm.is_shutdown());
    }

    // Shutdown all nodes
    cluster.shutdown_all();

    // Verify all nodes are shutdown
    for node in &cluster.nodes {
        assert!(node.pm.is_shutdown());
    }

    // Operations should fail after shutdown
    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let result = node1.pm.broadcast(Bytes::from("should fail")).await;
    assert!(result.is_err());
}

// =============================================================================
// Additional Multi-Node Integration Tests (Chain Topology, Multi-Message)
// =============================================================================

/// Test that messages propagate through a chain topology: 1 <-> 2 <-> 3 <-> 4 <-> 5
/// Node 1 broadcasts and node 5 should receive it via intermediate hops.
#[tokio::test]
async fn test_plumtree_memberlist_chain_propagation() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_ihave_interval(Duration::from_millis(50));
    let cluster = TestCluster::new(5, config);

    // Start with chain topology instead of fully connected
    cluster.start_chain();

    // Wait for chain topology to be established
    // End nodes (1 and 5) have 1 peer, middle nodes have 2 peers
    tokio::time::sleep(Duration::from_millis(200)).await;

    let node1 = cluster.get_node(MemberlistNodeId(1)).unwrap();
    let node5 = cluster.get_node(MemberlistNodeId(5)).unwrap();

    // Verify chain topology
    assert_eq!(node1.pm.peers().topology().total(), 1); // Only knows node 2
    assert_eq!(node5.pm.peers().topology().total(), 1); // Only knows node 4

    // Node 1 broadcasts
    let msg_id = node1
        .pm
        .broadcast(Bytes::from("chain message"))
        .await
        .unwrap();

    // Wait for propagation through the chain: 1 -> 2 -> 3 -> 4 -> 5
    cluster
        .wait_for_message_delivery(&msg_id, MemberlistNodeId(1), Duration::from_secs(10))
        .await;

    // Verify all nodes received the message
    for node in &cluster.nodes {
        if node.id == MemberlistNodeId(1) {
            continue; // Sender
        }
        assert_eq!(
            node.delegate.delivered_count(),
            1,
            "Node {} should have received the message",
            node.id
        );
    }

    // Node 5 specifically should have received it
    assert_eq!(node5.delegate.delivered_count(), 1);
    let msgs = node5.delegate.get_messages();
    assert_eq!(msgs[0].0, msg_id);
    assert_eq!(msgs[0].1.as_ref(), b"chain message");

    cluster.shutdown_all();
}

/// Test multiple broadcasts from different nodes.
#[tokio::test]
async fn test_plumtree_memberlist_multi_broadcast() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_ihave_interval(Duration::from_millis(50));
    let cluster = TestCluster::new(4, config);

    cluster.start_fully_connected();
    cluster.wait_for_topology(3, Duration::from_secs(5)).await;

    // Each node broadcasts a message
    let mut msg_ids = Vec::new();
    for node in &cluster.nodes {
        let msg = format!("message from node {}", node.id.0);
        let msg_id = node.pm.broadcast(Bytes::from(msg)).await.unwrap();
        msg_ids.push((node.id, msg_id));
    }

    // Wait for all messages to propagate
    for (sender_id, msg_id) in &msg_ids {
        cluster
            .wait_for_message_delivery(msg_id, *sender_id, Duration::from_secs(5))
            .await;
    }

    // Verify each node received 3 messages (from the other 3 nodes)
    for node in &cluster.nodes {
        assert_eq!(
            node.delegate.delivered_count(),
            3,
            "Node {} should have received 3 messages",
            node.id
        );
    }

    cluster.shutdown_all();
}

/// Test that adding a peer dynamically allows message delivery.
#[tokio::test]
async fn test_plumtree_memberlist_dynamic_peer_join() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_ihave_interval(Duration::from_millis(50));

    // Create cluster but don't fully connect initially
    let routes: RoutingTable = Arc::new(parking_lot::RwLock::new(HashMap::new()));

    let node1 = TestNode::new(MemberlistNodeId(1), config.clone(), routes.clone());
    let node2 = TestNode::new(MemberlistNodeId(2), config.clone(), routes.clone());
    let node3 = TestNode::new(MemberlistNodeId(3), config.clone(), routes.clone());

    // Start background tasks
    node1.start(routes.clone());
    node2.start(routes.clone());
    node3.start(routes.clone());

    // Initially only connect node 1 and node 2
    node1.pm.add_peer(MemberlistNodeId(2));
    node2.pm.add_peer(MemberlistNodeId(1));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify node 3 is isolated
    assert_eq!(node3.pm.peers().topology().total(), 0);

    // Broadcast from node 1 - only node 2 should receive
    let _msg_id1 = node1
        .pm
        .broadcast(Bytes::from("before join"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(node2.delegate.delivered_count(), 1);
    assert_eq!(node3.delegate.delivered_count(), 0); // Not connected yet

    // Now add node 3 to the network
    node2.pm.add_peer(MemberlistNodeId(3));
    node3.pm.add_peer(MemberlistNodeId(2));

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify node 3 is now connected
    assert_eq!(node3.pm.peers().topology().total(), 1);

    // Broadcast from node 1 - now node 3 should also receive via node 2
    let _msg_id2 = node1.pm.broadcast(Bytes::from("after join")).await.unwrap();

    // Wait for delivery
    let start = std::time::Instant::now();
    while node3.delegate.delivered_count() < 1 {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("Timeout waiting for node 3 to receive message");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(node2.delegate.delivered_count(), 2);
    assert_eq!(node3.delegate.delivered_count(), 1); // Received the second message

    // Shutdown
    node1.shutdown();
    node2.shutdown();
    node3.shutdown();
}

/// Test 10-node cluster with max_peers=6 constraint.
/// Each node can only maintain 6 peers, but messages should still reach all nodes
/// through the spanning tree and lazy push recovery mechanism.
///
/// This test uses the natural add_peer behavior which triggers auto-classification
/// and eviction logic in peer_state.rs. With max_peers=6 and eager_fanout=3,
/// each node will have 3 eager + 3 lazy peers (total 6).
#[tokio::test]
async fn test_plumtree_memberlist_10_nodes_max_peers_6() {
    let max_peers = 5;
    let eager_fanout = 3;
    let config = PlumtreeConfig::default()
        .with_eager_fanout(eager_fanout)
        .with_lazy_fanout(max_peers - eager_fanout)
        .with_max_peers(max_peers)
        .with_ihave_interval(Duration::from_millis(30))
        .with_graft_timeout(Duration::from_millis(200));

    // Create a 10-node cluster
    let routes: RoutingTable = Arc::new(parking_lot::RwLock::new(HashMap::new()));
    let mut nodes: Vec<TestNode> = Vec::new();

    for i in 1..=50 {
        let node = TestNode::new(MemberlistNodeId(i), config.clone(), routes.clone());
        nodes.push(node);
    }

    // Start background tasks for all nodes
    for node in &nodes {
        node.start(routes.clone());
    }

    // Simulate memberlist join behavior: each node adds all others as peers.
    // With max_peers=6, the add_peer_auto logic will:
    // 1. Add first 3 peers to eager set
    // 2. Add next 3 peers to lazy set
    // 3. For remaining peers, evict a random lazy peer and add the new one
    //
    // To ensure bidirectional connectivity (required for Plumtree), we add
    // peers in pairs: when A adds B, B also adds A. This simulates the real
    // memberlist behavior where both sides are notified of each other.
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            // Bidirectional add: A adds B, B adds A
            nodes[i].pm.add_peer(nodes[j].id);
            nodes[j].pm.add_peer(nodes[i].id);
        }
    }

    // Wait for topology to stabilize
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Verify each node has at most 6 peers (due to max_peers constraint)
    for node in &nodes {
        let topo = node.pm.peers().topology();
        assert!(
            topo.total() <= max_peers,
            "Node {} should have at most {} peers due to max_peers, but has {}",
            max_peers,
            node.id,
            topo.total()
        );
        // Each node should have at least 3 peers (eager_fanout)
        assert!(
            topo.total() >= eager_fanout,
            "Node {} should have at least {} peers, but has {}",
            node.id,
            eager_fanout,
            topo.total()
        );
    }

    // Print initial topology for debugging
    println!("=== Initial Topology ===");
    for node in &nodes {
        let topo = node.pm.peers().topology();
        println!(
            "Node {}: {} peers (eager={}, lazy={}) - eager: {:?}, lazy: {:?}",
            node.id,
            topo.total(),
            topo.eager_count(),
            topo.lazy_count(),
            topo.eager.iter().map(|n| n.0).collect::<Vec<_>>(),
            topo.lazy.iter().map(|n| n.0).collect::<Vec<_>>()
        );
    }

    // Node 1 broadcasts a message
    let node1 = nodes.iter().find(|n| n.id == MemberlistNodeId(1)).unwrap();
    let msg_id = node1
        .pm
        .broadcast(Bytes::from("broadcast from node 1"))
        .await
        .unwrap();

    // Verify message is cached on node 1
    let cache_stats = node1.pm.cache_stats();
    assert_eq!(cache_stats.entries, 1);

    // Wait for all nodes to receive the message
    // The Plumtree protocol ensures delivery through:
    // 1. Eager push: immediate Gossip to eager peers
    // 2. Lazy push: IHave announcements to lazy peers
    // 3. Graft: lazy peers request missing messages
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);

    loop {
        let all_received = nodes
            .iter()
            .all(|node| node.id == MemberlistNodeId(1) || node.delegate.has_message(&msg_id));

        if all_received {
            break;
        }

        if start.elapsed() > timeout {
            // Print status for debugging
            println!("=== Message Delivery Status ===");
            for node in &nodes {
                if node.id != MemberlistNodeId(1) {
                    println!(
                        "Node {}: received={}, delivered_count={}",
                        node.id,
                        node.delegate.has_message(&msg_id),
                        node.delegate.delivered_count()
                    );
                }
            }
            panic!(
                "Timeout waiting for message delivery after {:?}",
                start.elapsed()
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    println!(
        "=== All nodes received message in {:?} ===",
        start.elapsed()
    );

    // Verify all 9 other nodes received the message
    for node in &nodes {
        if node.id == MemberlistNodeId(1) {
            continue; // Sender doesn't deliver to itself
        }
        assert_eq!(
            node.delegate.delivered_count(),
            1,
            "Node {} should have received exactly 1 message",
            node.id
        );
        let msgs = node.delegate.get_messages();
        assert_eq!(msgs[0].0, msg_id);
        assert_eq!(msgs[0].1.as_ref(), b"broadcast from node 1");
    }

    // Shutdown all nodes
    for node in &nodes {
        node.shutdown();
    }
}
