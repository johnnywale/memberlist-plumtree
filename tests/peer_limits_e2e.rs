//! End-to-end tests for peer limit configuration.
//!
//! These tests verify that:
//! 1. max_eager_peers and max_lazy_peers limits are respected
//! 2. GRAFT mechanism still works correctly with limits
//! 3. Message propagation works end-to-end with the new settings

use bytes::Bytes;
use memberlist_plumtree::{MessageId, Plumtree, PlumtreeConfig, PlumtreeDelegate, PlumtreeMessage};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Simple node ID type for testing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId(u64);

/// Test delegate that tracks delivered messages.
#[allow(dead_code)] // Test utility - methods reserved for future assertions
#[derive(Debug, Default, Clone)]
struct TestDelegate {
    inner: Arc<TestDelegateInner>,
}

#[derive(Debug, Default)]
struct TestDelegateInner {
    delivered: Mutex<Vec<(MessageId, Bytes)>>,
    delivered_ids: Mutex<HashSet<MessageId>>,
    graft_count: AtomicUsize,
    promotion_count: AtomicUsize,
}

impl TestDelegate {
    fn new() -> Self {
        Self {
            inner: Arc::new(TestDelegateInner::default()),
        }
    }

    fn delivered_count(&self) -> usize {
        self.inner.delivered.lock().len()
    }

    fn has_message(&self, id: &MessageId) -> bool {
        self.inner.delivered_ids.lock().contains(id)
    }

    #[allow(dead_code)] // Reserved for future graft tracking assertions
    fn graft_count(&self) -> usize {
        self.inner.graft_count.load(Ordering::Relaxed)
    }

    fn promotion_count(&self) -> usize {
        self.inner.promotion_count.load(Ordering::Relaxed)
    }
}

impl PlumtreeDelegate<NodeId> for TestDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.inner.delivered.lock().push((message_id, payload));
        self.inner.delivered_ids.lock().insert(message_id);
    }

    fn on_graft_sent(&self, _peer: &NodeId, _message_id: &MessageId) {
        self.inner.graft_count.fetch_add(1, Ordering::Relaxed);
    }

    fn on_eager_promotion(&self, _peer: &NodeId) {
        self.inner.promotion_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Test that max_eager_peers limit is respected during GRAFT.
#[tokio::test]
async fn test_max_eager_peers_limit_on_graft() {
    let delegate = TestDelegate::new();

    // Configure with max_eager_peers = 2
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_max_eager_peers(2);

    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add 5 peers as lazy
    for i in 2..=6 {
        plumtree.add_peer_lazy(NodeId(i));
    }

    // Verify all start as lazy
    assert_eq!(plumtree.peer_stats().lazy_count, 5);
    assert_eq!(plumtree.peer_stats().eager_count, 0);

    // Broadcast a message (caches it)
    let msg_id = plumtree.broadcast(Bytes::from("test")).await.unwrap();

    // Simulate GRAFT requests from all peers
    for i in 2..=6 {
        let graft_msg = PlumtreeMessage::Graft {
            message_id: msg_id,
            round: 0,
        };
        plumtree.handle_message(NodeId(i), graft_msg).await.unwrap();
    }

    // Should only have 2 eager peers (the limit)
    let stats = plumtree.peer_stats();
    assert_eq!(
        stats.eager_count, 2,
        "Expected 2 eager peers (max_eager_peers limit), got {}",
        stats.eager_count
    );
    assert_eq!(
        stats.lazy_count, 3,
        "Expected 3 lazy peers, got {}",
        stats.lazy_count
    );

    // Only 2 promotions should have occurred
    assert_eq!(
        delegate.promotion_count(),
        2,
        "Expected 2 promotions, got {}",
        delegate.promotion_count()
    );
}

/// Test that GRAFT works correctly when under the limit.
#[tokio::test]
async fn test_graft_works_under_limit() {
    let delegate = TestDelegate::new();

    // Configure with generous limits
    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_max_eager_peers(10); // High limit

    let (plumtree, handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add 3 peers as lazy
    for i in 2..=4 {
        plumtree.add_peer_lazy(NodeId(i));
    }

    // Broadcast a message
    let msg_id = plumtree
        .broadcast(Bytes::from("test message"))
        .await
        .unwrap();

    // GRAFT from peer 2
    let graft_msg = PlumtreeMessage::Graft {
        message_id: msg_id,
        round: 0,
    };
    plumtree.handle_message(NodeId(2), graft_msg).await.unwrap();

    // Peer 2 should be eager now
    assert!(
        plumtree.peers().is_eager(&NodeId(2)),
        "Peer 2 should be promoted to eager after GRAFT"
    );

    // Should have received a Gossip response
    let outgoing = handle.next_outgoing().await;
    assert!(outgoing.is_some(), "Should have outgoing Gossip message");

    let out_msg = outgoing.unwrap();
    assert!(
        matches!(out_msg.message, PlumtreeMessage::Gossip { .. }),
        "Response should be Gossip"
    );
}

/// Test full message propagation with new config settings.
#[tokio::test]
async fn test_message_propagation_e2e() {
    // Create 5 nodes with the new config
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_hash_ring(true)
        .with_protect_ring_neighbors(true)
        .with_max_protected_neighbors(2)
        .with_max_eager_peers(3)
        .with_max_lazy_peers(10);

    let mut nodes: HashMap<u64, (Plumtree<NodeId, TestDelegate>, _, TestDelegate)> = HashMap::new();

    // Create nodes
    for i in 1..=5 {
        let delegate = TestDelegate::new();
        let (plumtree, handle) = Plumtree::new(NodeId(i), config.clone(), delegate.clone());
        nodes.insert(i, (plumtree, handle, delegate));
    }

    // Connect nodes in a mesh (everyone knows everyone)
    for i in 1..=5 {
        for j in 1..=5 {
            if i != j {
                nodes.get(&i).unwrap().0.add_peer(NodeId(j));
            }
        }
    }

    // Promote some to eager for initial tree
    nodes
        .get(&1)
        .unwrap()
        .0
        .peers()
        .promote_to_eager(&NodeId(2));
    nodes
        .get(&2)
        .unwrap()
        .0
        .peers()
        .promote_to_eager(&NodeId(1));
    nodes
        .get(&2)
        .unwrap()
        .0
        .peers()
        .promote_to_eager(&NodeId(3));
    nodes
        .get(&3)
        .unwrap()
        .0
        .peers()
        .promote_to_eager(&NodeId(2));

    // Node 1 broadcasts
    let msg_id = nodes
        .get(&1)
        .unwrap()
        .0
        .broadcast(Bytes::from("hello cluster"))
        .await
        .unwrap();

    // Simulate message propagation: Node 1 -> Node 2
    let gossip = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: Bytes::from("hello cluster"),
    };

    nodes
        .get(&2)
        .unwrap()
        .0
        .handle_message(NodeId(1), gossip.clone())
        .await
        .unwrap();

    // Node 2 should have delivered the message
    assert!(
        nodes.get(&2).unwrap().2.has_message(&msg_id),
        "Node 2 should have received the message"
    );

    // Simulate: Node 2 -> Node 3
    let gossip_r1 = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 1,
        payload: Bytes::from("hello cluster"),
    };

    nodes
        .get(&3)
        .unwrap()
        .0
        .handle_message(NodeId(2), gossip_r1)
        .await
        .unwrap();

    // Node 3 should have delivered the message
    assert!(
        nodes.get(&3).unwrap().2.has_message(&msg_id),
        "Node 3 should have received the message"
    );
}

/// Test IHave -> Graft flow with max_eager_peers limit.
#[tokio::test]
async fn test_ihave_graft_flow_with_limits() {
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_max_eager_peers(3);

    let (node1, handle1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (node2, _handle2) = Plumtree::new(NodeId(2), config, delegate2.clone());

    // Node 1 has peer 2 as lazy
    node1.add_peer_lazy(NodeId(2));
    // Node 2 has peer 1 as lazy
    node2.add_peer_lazy(NodeId(1));

    // Node 1 broadcasts
    let msg_id = node1.broadcast(Bytes::from("test payload")).await.unwrap();

    // Node 1 sends IHave to lazy peer (node 2)
    // Simulate node 2 receiving IHave
    let ihave = PlumtreeMessage::IHave {
        message_ids: smallvec::smallvec![msg_id],
        round: 0,
    };

    node2.handle_message(NodeId(1), ihave).await.unwrap();

    // Node 2 should have promoted node 1 to eager (missing message triggers GRAFT)
    assert!(
        node2.peers().is_eager(&NodeId(1)),
        "Node 1 should be promoted to eager on node 2 after IHave for missing message"
    );

    // Check that node 2 queued a GRAFT request
    // (In real scenario, this would be sent back to node 1)

    // Now simulate node 1 receiving the GRAFT from node 2
    let graft = PlumtreeMessage::Graft {
        message_id: msg_id,
        round: 0,
    };

    node1.handle_message(NodeId(2), graft).await.unwrap();

    // Node 1 should have promoted node 2 to eager
    assert!(
        node1.peers().is_eager(&NodeId(2)),
        "Node 2 should be promoted to eager on node 1 after GRAFT"
    );

    // Node 1 should have responded with Gossip
    let outgoing = handle1.next_outgoing().await;
    assert!(outgoing.is_some(), "Should have Gossip response to GRAFT");

    let out = outgoing.unwrap();
    if let PlumtreeMessage::Gossip { id, payload, .. } = out.message {
        assert_eq!(id, msg_id);
        assert_eq!(payload.as_ref(), b"test payload");
    } else {
        panic!("Expected Gossip message");
    }

    // Simulate node 2 receiving the Gossip
    let gossip = PlumtreeMessage::Gossip {
        id: msg_id,
        round: 0,
        payload: Bytes::from("test payload"),
    };

    node2.handle_message(NodeId(1), gossip).await.unwrap();

    // Node 2 should now have the message
    assert!(
        delegate2.has_message(&msg_id),
        "Node 2 should have received the message via GRAFT"
    );
    assert_eq!(delegate2.delivered_count(), 1);
}

/// Test that max_lazy_peers limit is respected.
#[tokio::test]
async fn test_max_lazy_peers_limit() {
    let delegate = TestDelegate::new();

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_max_lazy_peers(3); // Only allow 3 lazy peers

    let (plumtree, _handle) = Plumtree::new(NodeId(1), config, delegate);

    // Try to add 10 peers
    let mut added = 0;
    for i in 2..=11 {
        if plumtree.peers().add_peer(NodeId(i)) {
            added += 1;
        }
    }

    // Should only have added 3 (the limit)
    assert_eq!(added, 3, "Should only add 3 peers (max_lazy_peers limit)");
    assert_eq!(plumtree.peer_stats().lazy_count, 3);
}

/// Test that protocol works without limits (default config).
#[tokio::test]
async fn test_protocol_works_without_limits() {
    let delegate = TestDelegate::new();

    // Default config has no limits
    let config = PlumtreeConfig::default();
    assert!(config.max_eager_peers.is_none());
    assert!(config.max_lazy_peers.is_none());

    let (plumtree, handle) = Plumtree::new(NodeId(1), config, delegate.clone());

    // Add 10 peers
    for i in 2..=11 {
        plumtree.add_peer_lazy(NodeId(i));
    }

    // All should be added
    assert_eq!(plumtree.peer_stats().lazy_count, 10);

    // Broadcast
    let msg_id = plumtree.broadcast(Bytes::from("no limits")).await.unwrap();

    // All can GRAFT and become eager
    for i in 2..=11 {
        let graft = PlumtreeMessage::Graft {
            message_id: msg_id,
            round: 0,
        };
        plumtree.handle_message(NodeId(i), graft).await.unwrap();
    }

    // All should be eager now (no limit)
    assert_eq!(plumtree.peer_stats().eager_count, 10);
    assert_eq!(plumtree.peer_stats().lazy_count, 0);

    // Each GRAFT should have generated a Gossip response
    let mut gossip_count = 0;
    while let Ok(Some(_)) =
        tokio::time::timeout(Duration::from_millis(10), handle.next_outgoing()).await
    {
        gossip_count += 1;
    }
    assert_eq!(gossip_count, 10, "Should have 10 Gossip responses");
}

/// Test ring neighbor protection with new settings.
#[tokio::test]
async fn test_ring_neighbor_protection_e2e() {
    let delegate = TestDelegate::new();

    let config = PlumtreeConfig::default()
        .with_hash_ring(true)
        .with_protect_ring_neighbors(true)
        .with_max_protected_neighbors(2) // Only i±1
        .with_eager_fanout(3);

    let (plumtree, _handle) = Plumtree::new(NodeId(50), config, delegate);

    // Add peers - they'll be sorted by hash
    for i in 1..=100 {
        if i != 50 {
            plumtree.add_peer(NodeId(i));
        }
    }

    // Rebalance to get eager peers
    plumtree.rebalance_peers();

    // Get ring neighbors
    let ring_neighbors = plumtree.peers().ring_neighbors();

    // With max_protected_neighbors=2, we should have at most 2 protected
    assert!(
        ring_neighbors.len() <= 2,
        "Should have at most 2 ring neighbors with max_protected_neighbors=2"
    );

    // Try to demote a ring neighbor - should fail
    for neighbor in &ring_neighbors {
        let could_demote = plumtree.peers().demote_to_lazy(neighbor);
        assert!(
            !could_demote,
            "Should not be able to demote protected ring neighbor"
        );
    }
}

/// Test that disabling protection allows demotion.
#[tokio::test]
async fn test_disabled_ring_protection() {
    let delegate = TestDelegate::new();

    let config = PlumtreeConfig::default()
        .with_hash_ring(true)
        .with_protect_ring_neighbors(false) // Disabled!
        .with_eager_fanout(3);

    let (plumtree, _handle) = Plumtree::new(NodeId(50), config, delegate);

    // Add peers
    for i in 1..=20 {
        if i != 50 {
            plumtree.add_peer(NodeId(i));
        }
    }

    // Rebalance
    plumtree.rebalance_peers();

    // Ring neighbors should be empty (protection disabled)
    let ring_neighbors = plumtree.peers().ring_neighbors();
    assert!(
        ring_neighbors.is_empty(),
        "Ring neighbors should be empty when protection is disabled"
    );

    // All eager peers should be demotable
    let eager_peers: Vec<_> = plumtree.peers().eager_peers();
    for peer in eager_peers {
        let could_demote = plumtree.peers().demote_to_lazy(&peer);
        assert!(could_demote, "Should be able to demote any peer");
    }
}

/// Test the full IHave -> GRAFT flow with the scheduler running.
///
/// This test creates a controlled topology where:
/// - Node 1 has Node 2 as LAZY (no eager link)
/// - Node 1 broadcasts a message
/// - The IHave scheduler sends IHave to Node 2
/// - Node 2 receives IHave, sends GRAFT back
/// - Node 1 responds with Gossip
/// - Node 2 delivers the message
#[tokio::test]
async fn test_ihave_graft_full_flow_with_scheduler() {
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    // Use very short intervals for testing
    let config = PlumtreeConfig::default()
        .with_eager_fanout(1)
        .with_lazy_fanout(4)
        .with_ihave_interval(Duration::from_millis(10)) // Short interval
        .with_graft_timeout(Duration::from_millis(50));

    let (node1, handle1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (node2, handle2) = Plumtree::new(NodeId(2), config, delegate2.clone());

    // Set up topology: Node 1 has Node 2 as LAZY only
    // This ensures no GOSSIP path exists, forcing IHave -> GRAFT
    node1.add_peer_lazy(NodeId(2));
    node2.add_peer_lazy(NodeId(1));

    // Verify topology
    assert_eq!(node1.peer_stats().lazy_count, 1);
    assert_eq!(node1.peer_stats().eager_count, 0);
    assert_eq!(node2.peer_stats().lazy_count, 1);
    assert_eq!(node2.peer_stats().eager_count, 0);

    // Start the IHave schedulers in background tasks
    let node1_scheduler = node1.clone();
    let scheduler1_handle = tokio::spawn(async move {
        node1_scheduler.run_ihave_scheduler().await;
    });

    let node2_scheduler = node2.clone();
    let scheduler2_handle = tokio::spawn(async move {
        node2_scheduler.run_ihave_scheduler().await;
    });

    // Node 1 broadcasts a message
    let msg_id = node1.broadcast(Bytes::from("test payload")).await.unwrap();

    // Note: broadcast() does NOT auto-deliver to self
    // The message is sent to eager peers (none in this case) and IHave queued for lazy peers

    // Wait for IHave to be scheduled and sent
    // Use longer timeout for CI environments (especially macOS) which may be slower
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Process outgoing messages from Node 1 (should include IHave to Node 2)
    // Retry loop with longer individual timeouts for CI stability
    let mut ihave_sent = false;
    for _ in 0..10 {
        while let Ok(Some(out)) =
            tokio::time::timeout(Duration::from_millis(50), handle1.next_outgoing()).await
        {
            if let PlumtreeMessage::IHave { message_ids, .. } = &out.message {
                if message_ids.contains(&msg_id) {
                    ihave_sent = true;
                    // Route this IHave to Node 2
                    node2
                        .handle_message(NodeId(1), out.message.clone())
                        .await
                        .unwrap();
                }
            }
        }
        if ihave_sent {
            break;
        }
        // Give the scheduler more time to fire
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(ihave_sent, "Node 1 should have sent IHave to lazy peer");

    // Node 2 should have promoted Node 1 to eager and sent GRAFT
    // The promotion happens in handle_ihave when message is missing
    assert!(
        node2.peers().is_eager(&NodeId(1)),
        "Node 2 should have promoted Node 1 to eager after receiving IHave for missing message"
    );

    // Process outgoing messages from Node 2 (should include GRAFT)
    let mut graft_sent = false;
    while let Ok(Some(out)) =
        tokio::time::timeout(Duration::from_millis(50), handle2.next_outgoing()).await
    {
        if let PlumtreeMessage::Graft { message_id, .. } = &out.message {
            if *message_id == msg_id {
                graft_sent = true;
                // Route this GRAFT to Node 1
                node1
                    .handle_message(NodeId(2), out.message.clone())
                    .await
                    .unwrap();
            }
        }
    }

    assert!(
        graft_sent,
        "Node 2 should have sent GRAFT for missing message"
    );

    // Node 1 should have promoted Node 2 to eager and sent Gossip response
    assert!(
        node1.peers().is_eager(&NodeId(2)),
        "Node 1 should have promoted Node 2 to eager after receiving GRAFT"
    );

    // Process outgoing Gossip from Node 1
    let mut gossip_sent = false;
    while let Ok(Some(out)) =
        tokio::time::timeout(Duration::from_millis(50), handle1.next_outgoing()).await
    {
        if let PlumtreeMessage::Gossip { id, payload, .. } = &out.message {
            if *id == msg_id {
                gossip_sent = true;
                // Route this Gossip to Node 2
                node2
                    .handle_message(NodeId(1), out.message.clone())
                    .await
                    .unwrap();
                assert_eq!(payload.as_ref(), b"test payload");
            }
        }
    }

    assert!(
        gossip_sent,
        "Node 1 should have sent Gossip in response to GRAFT"
    );

    // Node 2 should now have the message
    assert!(
        delegate2.has_message(&msg_id),
        "Node 2 should have received the message via GRAFT flow"
    );
    assert_eq!(delegate2.delivered_count(), 1);

    // Verify promotion callbacks were triggered
    assert_eq!(
        delegate1.promotion_count(),
        1,
        "Node 1 should have recorded 1 promotion (Node 2)"
    );
    assert_eq!(
        delegate2.promotion_count(),
        1,
        "Node 2 should have recorded 1 promotion (Node 1)"
    );

    // Clean up
    node1.shutdown();
    node2.shutdown();
    let _ = scheduler1_handle.await;
    let _ = scheduler2_handle.await;
}

/// Test that GRAFT is not sent when message is already known.
#[tokio::test]
async fn test_no_graft_when_message_already_known() {
    let delegate1 = TestDelegate::new();
    let delegate2 = TestDelegate::new();

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(4);

    let (node1, handle1) = Plumtree::new(NodeId(1), config.clone(), delegate1.clone());
    let (node2, handle2) = Plumtree::new(NodeId(2), config, delegate2.clone());

    // Set up topology: Node 1 has Node 2 as EAGER
    node1.add_peer(NodeId(2));
    node2.add_peer(NodeId(1));

    // Verify Node 2 is eager
    assert!(node1.peers().is_eager(&NodeId(2)));

    // Node 1 broadcasts
    let msg_id = node1.broadcast(Bytes::from("test payload")).await.unwrap();

    // Get the Gossip message from Node 1
    let gossip = loop {
        if let Ok(Some(out)) =
            tokio::time::timeout(Duration::from_millis(10), handle1.next_outgoing()).await
        {
            if let PlumtreeMessage::Gossip { id, .. } = &out.message {
                if *id == msg_id {
                    break out.message;
                }
            }
        } else {
            panic!("Expected Gossip message");
        }
    };

    // Node 2 receives Gossip first
    node2.handle_message(NodeId(1), gossip).await.unwrap();

    // Verify Node 2 delivered the message
    assert!(
        delegate2.has_message(&msg_id),
        "Node 2 should have the message via Gossip"
    );

    // Now send IHave for the same message - should NOT trigger GRAFT
    let ihave = PlumtreeMessage::IHave {
        message_ids: smallvec::smallvec![msg_id],
        round: 0,
    };

    node2.handle_message(NodeId(1), ihave).await.unwrap();

    // Check for GRAFT - there should be none
    let graft_found = tokio::time::timeout(Duration::from_millis(50), async {
        while let Some(out) = handle2.next_outgoing().await {
            if matches!(out.message, PlumtreeMessage::Graft { .. }) {
                return true;
            }
        }
        false
    })
    .await;

    assert!(
        graft_found.is_err() || !graft_found.unwrap(),
        "No GRAFT should be sent when message is already known"
    );

    // Delivery count should still be 1 (no duplicate delivery)
    assert_eq!(delegate2.delivered_count(), 1);
}
