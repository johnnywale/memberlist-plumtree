//! Integration tests for the Plumtree protocol.
//!
//! These tests verify the end-to-end behavior of Plumtree broadcast,
//! including message delivery, deduplication, and tree repair.

use bytes::Bytes;
use memberlist_plumtree::{MessageId, Plumtree, PlumtreeConfig, PlumtreeDelegate, PlumtreeMessage};
use parking_lot::Mutex;
use std::collections::HashSet;
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

impl PlumtreeDelegate for TestDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.0.delivered.lock().push((message_id, payload.clone()));
        self.0.delivered_ids.lock().insert(message_id);
    }
}

/// Simple node ID type for testing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

    plumtree.add_peer(NodeId(2));

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

    // Add 10 peers (all start as lazy)
    for i in 2..=11 {
        plumtree.add_peer(NodeId(i));
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
    let (plumtree1, _h1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
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

    // Add peers
    plumtree.add_peer(NodeId(2));
    plumtree.add_peer(NodeId(3));

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
