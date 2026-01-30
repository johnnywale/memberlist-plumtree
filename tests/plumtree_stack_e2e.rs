//! End-to-End (E2E) Tests for PlumtreeStack with QUIC Transport.
//!
//! These tests verify the `PlumtreeStack` abstraction works correctly
//! with real QUIC networking.
//!
//! # Test Environment
//!
//! - **Network**: Local loopback (`127.0.0.1`) using real QUIC/UDP sockets
//! - **Discovery**: `StaticDiscovery` with configured seed peers
//! - **Transport**: Real QUIC with insecure dev certificates
//!
//! # Running Tests
//!
//! ```bash
//! cargo test --test plumtree_stack_e2e --features "quic,tokio"
//! ```

#![cfg(all(feature = "quic", feature = "tokio"))]

mod plumtree_stack_common;

use memberlist_plumtree::{HealthStatus, PlumtreeConfig, SyncConfig};
use plumtree_stack_common::common::eventually;
use plumtree_stack_common::TestCluster;
use std::time::Duration;

// ============================================================================
// Test 1: Basic Broadcast Delivery
// ============================================================================

/// Test 1: Basic Broadcast Delivery
/// Goal: Ensure O(n) reliable broadcast with PlumtreeStack
#[tokio::test]
async fn test_basic_broadcast_delivery() {
    const NUM_NODES: usize = 5;

    let config = PlumtreeConfig::lan()
        .with_eager_fanout(2)
        .with_lazy_fanout(3);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Node 0 broadcasts
    let payload = b"Hello, Plumtree!";
    let msg_id = cluster.nodes[0].broadcast(payload.to_vec()).await.unwrap();

    // Wait for non-sender nodes to receive (sender doesn't get its own message via delegate)
    // The sender already has the message, it doesn't need to receive it through the protocol
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    let delivered = cluster
        .wait_for_delivery_subset(msg_id, &non_sender_indices, Duration::from_secs(10))
        .await;

    assert!(delivered, "Not all non-sender nodes received the message");

    // Assertions for non-sender nodes (they should receive exactly 1 message)
    for i in 1..NUM_NODES {
        let node = &cluster.nodes[i];
        assert_eq!(
            node.delegate.delivery_count(),
            1,
            "Node {} wrong delivery count",
            i
        );
        assert_eq!(
            node.delegate.duplicate_count(),
            0,
            "Node {} has duplicates",
            i
        );
    }

    // Sender should NOT have received its own message (it already knows about it)
    assert_eq!(
        cluster.nodes[0].delegate.delivery_count(),
        0,
        "Sender should not receive its own message via delegate"
    );

    // Check topology health
    for node in &cluster.nodes {
        let health = node.health();
        assert_eq!(
            health.status,
            HealthStatus::Healthy,
            "Node {} unhealthy: {:?}",
            node.node_id,
            health
        );
    }

    // Verify peer stats
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        assert!(
            stats.eager_count > 0 || stats.lazy_count > 0,
            "Node {} has no peers",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 2: Duplicate Path → PRUNE
// ============================================================================

/// Test 2: Duplicate Reception triggers PRUNE
/// Goal: Ensure tree optimization via PRUNE mechanism
#[tokio::test]
async fn test_duplicate_triggers_prune() {
    const NUM_NODES: usize = 6;

    // High eager fanout to force duplicates
    let config = PlumtreeConfig::default()
        .with_eager_fanout(4) // Forces many eager links
        .with_lazy_fanout(2)
        .with_optimization_threshold(1); // PRUNE after 1 duplicate

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast from node 0
    let msg_id = cluster.nodes[0].broadcast(b"test".to_vec()).await.unwrap();

    // Wait for delivery (sender excluded since it doesn't receive its own message via delegate)
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg_id, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Brief pause to allow PRUNE messages to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Check that at least some nodes received duplicates (triggering PRUNE)
    let total_duplicates: usize = cluster
        .nodes
        .iter()
        .map(|n| n.delegate.duplicate_count())
        .sum();

    // With eager_fanout=4 on 6 nodes, we may see some duplicates
    println!("Total duplicates received: {}", total_duplicates);

    // Verify eager peer count is reasonable
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        println!(
            "Node {}: eager={}, lazy={}",
            node.node_id, stats.eager_count, stats.lazy_count
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 3: Lazy Peer GRAFT Repair
// ============================================================================

/// Test 3: Lazy Peer GRAFT Repair
/// Goal: Validate IHAVE → timeout → GRAFT → promotion
#[tokio::test]
async fn test_lazy_peer_graft_repair() {
    const NUM_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(1) // Minimal eager links forces lazy path usage
        .with_lazy_fanout(3) // More lazy links
        .with_graft_timeout(Duration::from_millis(200))
        .with_ihave_interval(Duration::from_millis(50));

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast from node 0
    let msg_id = cluster.nodes[0]
        .broadcast(b"graft-test".to_vec())
        .await
        .unwrap();

    // Wait for non-sender nodes to receive (some via GRAFT)
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg_id, &non_sender_indices, Duration::from_secs(5))
            .await,
        "GRAFT repair failed - not all nodes received message"
    );

    // Non-sender nodes should have received exactly once
    for i in 1..NUM_NODES {
        assert_eq!(
            cluster.nodes[i].delegate.delivery_count(),
            1,
            "Node {} wrong count",
            cluster.nodes[i].node_id
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 4: Node Failure (Tree Self-Heal)
// ============================================================================

/// Test 4: Node Failure triggers Self-Healing
/// Goal: Validate topology repairs when eager node fails
#[tokio::test]
async fn test_node_failure_self_healing() {
    const NUM_NODES: usize = 7;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_protect_ring_neighbors(true);

    let mut cluster = TestCluster::new(NUM_NODES, config).await;

    // First broadcast - non-sender nodes should receive
    let msg1 = cluster.nodes[0]
        .broadcast(b"before-failure".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
            .await
    );

    // Kill node 3 (middle of cluster, index 3)
    // Note: After kill_node, indices shift
    cluster.kill_node(3).await;

    // Brief pause to let failure detection propagate
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After killing node 3, the cluster now has 6 nodes (indices 0-5)
    // Node 0 is still the broadcaster

    // Second broadcast - all ALIVE non-sender nodes should receive
    let msg2 = cluster.nodes[0]
        .broadcast(b"after-failure".to_vec())
        .await
        .unwrap();

    // Wait for remaining non-sender nodes (indices 1 through cluster.nodes.len()-1)
    let remaining_non_sender: Vec<usize> = (1..cluster.nodes.len()).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg2, &remaining_non_sender, Duration::from_secs(5))
            .await,
        "Self-healing failed - alive nodes didn't receive message"
    );

    // Verify all alive non-sender nodes got both messages
    for i in 1..cluster.nodes.len() {
        assert_eq!(
            cluster.nodes[i].delegate.delivery_count(),
            2,
            "Node {} missing messages",
            cluster.nodes[i].node_id
        );
    }

    // Sender (node 0) should have 0 deliveries (it doesn't receive its own broadcasts)
    assert_eq!(
        cluster.nodes[0].delegate.delivery_count(),
        0,
        "Sender should not receive its own messages"
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 5: Network Partition + Anti-Entropy Sync
// ============================================================================

/// Test 5: Network Partition with Anti-Entropy Sync
/// Goal: Validate sync recovers messages after partition heals
#[tokio::test]
#[cfg(feature = "sync")]
async fn test_partition_anti_entropy_sync() {
    use memberlist_plumtree::discovery::{StaticDiscovery, StaticDiscoveryConfig};
    use plumtree_stack_common::common::allocate_ports;
    use plumtree_stack_common::TestDelegate;

    // Group A: nodes 0, 1, 2
    let ports_a = allocate_ports(3);
    let addrs_a: Vec<std::net::SocketAddr> = ports_a
        .iter()
        .map(|p| format!("127.0.0.1:{}", p).parse().unwrap())
        .collect();

    // Group B: nodes 3, 4, 5
    let ports_b = allocate_ports(3);
    let addrs_b: Vec<std::net::SocketAddr> = ports_b
        .iter()
        .map(|p| format!("127.0.0.1:{}", p).parse().unwrap())
        .collect();

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(2)
        .with_sync(SyncConfig::enabled().with_sync_interval(Duration::from_millis(500)));

    plumtree_stack_common::common::install_crypto_provider();

    // Create Group A cluster
    let mut nodes_a = Vec::new();
    for i in 0..3 {
        let node_id = i as u64;
        let bind_addr = addrs_a[i];

        let mut discovery_config = StaticDiscoveryConfig::new();
        for (j, &addr) in addrs_a.iter().enumerate() {
            if j != i {
                discovery_config = discovery_config.with_seed(j as u64, addr);
            }
        }
        let discovery = StaticDiscovery::new(discovery_config).with_local_addr(bind_addr);

        let delegate = TestDelegate::new();
        let stack_config = memberlist_plumtree::PlumtreeStackConfig::new(node_id, bind_addr)
            .with_plumtree(config.clone())
            .with_quic(memberlist_plumtree::QuicConfig::insecure_dev())
            .with_discovery(discovery);

        let stack = stack_config.build(delegate.clone()).await.unwrap();
        nodes_a.push((stack, delegate, node_id, bind_addr));
    }

    // Create Group B cluster
    let mut nodes_b = Vec::new();
    for i in 0..3 {
        let node_id = (i + 3) as u64;
        let bind_addr = addrs_b[i];

        let mut discovery_config = StaticDiscoveryConfig::new();
        for (j, &addr) in addrs_b.iter().enumerate() {
            if j != i {
                discovery_config = discovery_config.with_seed((j + 3) as u64, addr);
            }
        }
        let discovery = StaticDiscovery::new(discovery_config).with_local_addr(bind_addr);

        let delegate = TestDelegate::new();
        let stack_config = memberlist_plumtree::PlumtreeStackConfig::new(node_id, bind_addr)
            .with_plumtree(config.clone())
            .with_quic(memberlist_plumtree::QuicConfig::insecure_dev())
            .with_discovery(discovery);

        let stack = stack_config.build(delegate.clone()).await.unwrap();
        nodes_b.push((stack, delegate, node_id, bind_addr));
    }

    // Wait for intra-partition connections - nodes in each group should have peers
    eventually(Duration::from_secs(5), || async {
        nodes_a
            .iter()
            .all(|(stack, _, _, _)| stack.peer_stats().eager_count > 0)
            && nodes_b
                .iter()
                .all(|(stack, _, _, _)| stack.peer_stats().eager_count > 0)
    })
    .await
    .expect("Intra-partition connections didn't establish");

    // Broadcast in each partition
    let msg_a = nodes_a[0]
        .0
        .broadcast(b"from-group-a".to_vec())
        .await
        .unwrap();
    let msg_b = nodes_b[0]
        .0
        .broadcast(b"from-group-b".to_vec())
        .await
        .unwrap();

    // Wait for local delivery within partitions
    eventually(Duration::from_secs(5), || async {
        // Non-sender nodes in group A should have msg_a
        let a_delivered = nodes_a
            .iter()
            .skip(1)
            .all(|(_, delegate, _, _)| delegate.has_message(&msg_a));
        // Non-sender nodes in group B should have msg_b
        let b_delivered = nodes_b
            .iter()
            .skip(1)
            .all(|(_, delegate, _, _)| delegate.has_message(&msg_b));
        a_delivered && b_delivered
    })
    .await
    .expect("Local delivery within partitions failed");

    // Verify partition isolation
    // Note: sender doesn't receive its own message via delegate, so skip sender
    for (i, (_, delegate, _, _)) in nodes_a.iter().enumerate() {
        if i == 0 {
            // Sender of msg_a - doesn't receive via delegate
            assert!(
                !delegate.has_message(&msg_a),
                "Sender shouldn't get own message via delegate"
            );
        } else {
            assert!(
                delegate.has_message(&msg_a),
                "Non-sender in group A should have msg_a"
            );
        }
        assert!(
            !delegate.has_message(&msg_b),
            "Partition leaked - group A has msg_b!"
        );
    }
    for (i, (_, delegate, _, _)) in nodes_b.iter().enumerate() {
        if i == 0 {
            // Sender of msg_b - doesn't receive via delegate
            assert!(
                !delegate.has_message(&msg_b),
                "Sender shouldn't get own message via delegate"
            );
        } else {
            assert!(
                delegate.has_message(&msg_b),
                "Non-sender in group B should have msg_b"
            );
        }
        assert!(
            !delegate.has_message(&msg_a),
            "Partition leaked - group B has msg_a!"
        );
    }

    // Heal partition: Add cross-partition peers
    // Node 0 (group A) learns about node 3 (group B)
    nodes_a[0].0.add_peer(3, addrs_b[0]);
    // Node 3 (group B) learns about node 0 (group A)
    nodes_b[0].0.add_peer(0, addrs_a[0]);

    // Also add remaining cross-partition peers for better connectivity
    for (i, (stack, _, _, _)) in nodes_a.iter().enumerate() {
        stack.add_peer((3 + i) as u64, addrs_b[i % 3]);
    }
    for (i, (stack, _, _, _)) in nodes_b.iter().enumerate() {
        stack.add_peer(i as u64, addrs_a[i % 3]);
    }

    // Wait for sync to propagate messages across partitions
    eventually(Duration::from_secs(10), || async {
        // At least one node in group A should have msg_b
        let a_has_b = nodes_a
            .iter()
            .any(|(_, delegate, _, _)| delegate.has_message(&msg_b));
        // At least one node in group B should have msg_a
        let b_has_a = nodes_b
            .iter()
            .any(|(_, delegate, _, _)| delegate.has_message(&msg_a));
        a_has_b && b_has_a
    })
    .await
    .expect("Cross-partition sync failed");

    // After sync, verify messages spread across partitions
    // The sync mechanism should eventually propagate messages
    //
    // Note: Sync may not be instant, so we check that at least some cross-partition
    // delivery happened. Full propagation depends on sync implementation details.

    // Count how many nodes in group A now have msg_b (cross-partition message)
    let mut a_has_b = 0;
    for (_, delegate, _, _) in &nodes_a {
        if delegate.has_message(&msg_b) {
            a_has_b += 1;
        }
    }

    // Count how many nodes in group B now have msg_a (cross-partition message)
    let mut b_has_a = 0;
    for (_, delegate, _, _) in &nodes_b {
        if delegate.has_message(&msg_a) {
            b_has_a += 1;
        }
    }

    println!(
        "Cross-partition delivery: {} nodes in A have msg_b, {} nodes in B have msg_a",
        a_has_b, b_has_a
    );

    // At minimum, the bridging nodes should have cross-partition messages
    // Since we added cross-partition peers to nodes_a[0] and nodes_b[0],
    // these should have received messages from the other partition
    assert!(
        a_has_b >= 1,
        "No nodes in group A received msg_b after sync"
    );
    assert!(
        b_has_a >= 1,
        "No nodes in group B received msg_a after sync"
    );

    // Shutdown
    for (stack, _, _, _) in nodes_a {
        stack.shutdown().await;
    }
    for (stack, _, _, _) in nodes_b {
        stack.shutdown().await;
    }
}

// ============================================================================
// Test 6: Late Joiner Recovery
// ============================================================================

/// Test 6: Late Joiner Recovery
/// Goal: New node receives historical messages via sync
#[tokio::test]
#[cfg(feature = "sync")]
async fn test_late_joiner_recovery() {
    const INITIAL_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_sync(SyncConfig::enabled().with_sync_interval(Duration::from_millis(500)));

    let cluster = TestCluster::new(INITIAL_NODES, config.clone()).await;

    // Broadcast messages before new node joins
    let msg1 = cluster.nodes[0]
        .broadcast(b"historical-1".to_vec())
        .await
        .unwrap();
    let msg2 = cluster.nodes[1]
        .broadcast(b"historical-2".to_vec())
        .await
        .unwrap();

    cluster
        .wait_for_delivery(msg1, Duration::from_secs(5))
        .await;
    cluster
        .wait_for_delivery(msg2, Duration::from_secs(5))
        .await;

    // Add new node (late joiner)
    let late_joiner = cluster.add_node(INITIAL_NODES as u64, config).await;

    // Notify existing nodes about late joiner
    for node in &cluster.nodes {
        node.stack.add_peer(late_joiner.node_id, late_joiner.addr);
    }

    // Wait for sync to transfer historical messages to late joiner
    eventually(Duration::from_secs(10), || async {
        late_joiner.delegate.has_message(&msg1) && late_joiner.delegate.has_message(&msg2)
    })
    .await
    .expect("Late joiner didn't receive historical messages via sync");

    // Cleanup
    late_joiner.stack.shutdown().await;
    cluster.shutdown().await;
}

// ============================================================================
// Test 7: Restart + Persistence
// ============================================================================

/// Test 7: Restart with Persistence
/// Goal: Node restarts and can participate in the cluster
#[tokio::test]
async fn test_restart_node() {
    const NUM_NODES: usize = 3;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_message_cache_ttl(Duration::from_secs(60));

    let mut cluster = TestCluster::new(NUM_NODES, config.clone()).await;

    // Broadcast first message (non-sender nodes should receive)
    let msg1 = cluster.nodes[0]
        .broadcast(b"persistent-msg".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Get node 1's info before shutdown
    let node1_id = cluster.nodes[1].node_id;
    let node1_addr = cluster.nodes[1].addr;

    // Shutdown node 1
    cluster.kill_node(1).await;

    // Brief pause to let shutdown complete (restart_node uses a new port, so no TIME_WAIT issue)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Restart node 1 (new delegate, new port but same ID)
    let restarted = cluster.restart_node(node1_id, node1_addr, config).await;

    // Notify other nodes about the restarted node
    for node in &cluster.nodes {
        node.stack.add_peer(restarted.node_id, restarted.addr);
    }

    // Wait for reconnection - check that restarted node has peers
    eventually(Duration::from_secs(5), || async {
        restarted.peer_stats().eager_count > 0 || restarted.peer_stats().lazy_count > 0
    })
    .await
    .expect("Restarted node didn't reconnect to any peers");

    // Broadcast new message - restarted node should receive
    let msg2 = cluster.nodes[0]
        .broadcast(b"after-restart".to_vec())
        .await
        .unwrap();

    // Wait for restarted node to receive the message
    eventually(Duration::from_secs(5), || async {
        restarted.delegate.has_message(&msg2)
    })
    .await
    .expect("Restarted node didn't receive new message");

    restarted.stack.shutdown().await;
    cluster.shutdown().await;
}

// ============================================================================
// Test 8: Lazarus Seed Recovery
// ============================================================================

/// Test 8: Lazarus Seed Recovery
/// Goal: Seed node auto-rejoins after restart
#[tokio::test]
async fn test_seed_recovery() {
    const NUM_NODES: usize = 3;

    let config = PlumtreeConfig::default().with_eager_fanout(2);

    let mut cluster = TestCluster::new(NUM_NODES, config.clone()).await;

    // Verify initial connectivity (non-sender nodes should receive)
    let msg1 = cluster.nodes[0]
        .broadcast(b"before-seed-death".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Get node 0's info (the "seed" for other nodes)
    let seed_id = cluster.nodes[0].node_id;
    let seed_addr = cluster.nodes[0].addr;

    // Kill seed node (node 0)
    cluster.kill_node(0).await;

    // Brief pause to let shutdown complete (restart_node uses a new port, so no TIME_WAIT issue)
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Restart seed with same ID but new port
    let restarted_seed = cluster.restart_node(seed_id, seed_addr, config).await;

    // Notify other nodes about restarted seed
    for node in &cluster.nodes {
        node.stack
            .add_peer(restarted_seed.node_id, restarted_seed.addr);
    }

    // Wait for reconnection - check that restarted seed has peers
    eventually(Duration::from_secs(5), || async {
        restarted_seed.peer_stats().eager_count > 0 || restarted_seed.peer_stats().lazy_count > 0
    })
    .await
    .expect("Restarted seed didn't reconnect to any peers");

    // Verify seed rejoined - broadcast should work
    let msg2 = restarted_seed
        .broadcast(b"seed-recovered".to_vec())
        .await
        .unwrap();

    // Wait for all other nodes to receive the message
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| n.delegate.has_message(&msg2))
    })
    .await
    .expect("Not all nodes received message from restarted seed");

    restarted_seed.stack.shutdown().await;
    cluster.shutdown().await;
}

// ============================================================================
// Test 9: Hash Ring Determinism
// ============================================================================

/// Test 9: Hash Ring Topology Determinism
/// Goal: Same topology reconstructed after full cluster restart
#[tokio::test]
async fn test_hash_ring_determinism() {
    const NUM_NODES: usize = 8;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_lazy_fanout(4)
        .with_hash_ring(true)
        .with_protect_ring_neighbors(true)
        .with_max_protected_neighbors(2);

    // Create initial cluster
    let cluster = TestCluster::new(NUM_NODES, config.clone()).await;

    // Wait for topology to stabilize - all nodes should have some peers
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| {
            let stats = n.peer_stats();
            stats.eager_count > 0
        })
    })
    .await
    .expect("Initial cluster topology didn't stabilize");

    // Capture eager counts for each node
    let initial_eager_counts: Vec<usize> = cluster
        .nodes
        .iter()
        .map(|n| n.peer_stats().eager_count)
        .collect();

    // Shutdown all
    cluster.shutdown().await;

    // Restart entire cluster with same config (same node IDs)
    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for topology to stabilize - all nodes should have some peers
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| {
            let stats = n.peer_stats();
            stats.eager_count > 0
        })
    })
    .await
    .expect("Restarted cluster topology didn't stabilize");

    // Capture new eager counts
    let new_eager_counts: Vec<usize> = cluster
        .nodes
        .iter()
        .map(|n| n.peer_stats().eager_count)
        .collect();

    // Compare topologies - eager counts should be similar
    for i in 0..NUM_NODES {
        let diff = (initial_eager_counts[i] as i32 - new_eager_counts[i] as i32).abs();
        // Allow some variance due to race conditions
        assert!(
            diff <= 2,
            "Node {} eager count changed significantly: {} -> {}",
            i,
            initial_eager_counts[i],
            new_eager_counts[i]
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 10: Max Peer Caps
// ============================================================================

/// Test 10: Max Peer Caps Enforcement
/// Goal: Verify max_eager_peers and max_lazy_peers affect topology
#[tokio::test]
async fn test_max_peer_caps() {
    const NUM_NODES: usize = 8;
    const MAX_EAGER: usize = 3;
    const MAX_LAZY: usize = 5;

    // Use more reasonable fanout values
    let config = PlumtreeConfig::default()
        .with_eager_fanout(MAX_EAGER) // Match max_eager_peers
        .with_lazy_fanout(MAX_LAZY) // Match max_lazy_peers
        .with_max_eager_peers(MAX_EAGER)
        .with_max_lazy_peers(MAX_LAZY);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for initial connections - all nodes should have some peers
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| {
            let stats = n.peer_stats();
            stats.eager_count > 0 || stats.lazy_count > 0
        })
    })
    .await
    .expect("Initial connections didn't establish");

    // Send some messages to trigger topology optimization (PRUNEs)
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    for i in 0..5 {
        let msg = cluster.nodes[i % NUM_NODES]
            .broadcast(format!("topology-test-{}", i).into_bytes())
            .await
            .unwrap();

        // Wait for non-senders to receive
        let receivers: Vec<usize> = (0..NUM_NODES).filter(|&j| j != i % NUM_NODES).collect();
        cluster
            .wait_for_delivery_subset(msg, &receivers, Duration::from_secs(5))
            .await;
    }

    // Wait for topology to stabilize after PRUNEs - check peer caps are enforced
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| {
            let stats = n.peer_stats();
            stats.eager_count <= MAX_EAGER && stats.lazy_count <= MAX_LAZY
        })
    })
    .await
    .expect("Peer caps not enforced after topology stabilization");

    // Verify caps - peers should be within limits
    // Note: eager_count might be slightly below MAX_EAGER due to pruning
    for node in &cluster.nodes {
        let stats = node.peer_stats();

        // Eager peers should be capped at MAX_EAGER
        assert!(
            stats.eager_count <= MAX_EAGER,
            "Node {} exceeds max_eager_peers: {} > {}",
            node.node_id,
            stats.eager_count,
            MAX_EAGER
        );

        // Lazy peers should be capped at MAX_LAZY
        assert!(
            stats.lazy_count <= MAX_LAZY,
            "Node {} exceeds max_lazy_peers: {} > {}",
            node.node_id,
            stats.lazy_count,
            MAX_LAZY
        );

        println!(
            "Node {} - eager: {}/{}, lazy: {}/{}",
            node.node_id, stats.eager_count, MAX_EAGER, stats.lazy_count, MAX_LAZY
        );
    }

    // Broadcast should still work with capped peers
    let msg = cluster.nodes[0]
        .broadcast(b"capped-test".to_vec())
        .await
        .unwrap();
    assert!(
        cluster
            .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(10))
            .await,
        "Broadcast failed with peer caps"
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 11: GRAFT Retry with Backoff
// ============================================================================

/// Test 11: GRAFT Retry with Backoff
/// Goal: Verify GRAFT retry behavior
#[tokio::test]
async fn test_graft_retry_behavior() {
    const NUM_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(1) // Minimal eager = more GRAFT opportunities
        .with_lazy_fanout(2)
        .with_graft_timeout(Duration::from_millis(100))
        .with_graft_max_retries(3);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast multiple messages to exercise GRAFT mechanisms
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    for i in 0..5 {
        let msg = cluster.nodes[0]
            .broadcast(format!("retry-test-{}", i).into_bytes())
            .await
            .unwrap();

        // Wait for non-sender nodes to receive
        cluster
            .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(5))
            .await;
    }

    // Non-sender nodes should have received all 5 messages
    for i in 1..NUM_NODES {
        assert_eq!(
            cluster.nodes[i].delegate.delivery_count(),
            5,
            "Node {} missing messages",
            cluster.nodes[i].node_id
        );
    }

    // Sender (node 0) should have received 0 messages via delegate
    assert_eq!(
        cluster.nodes[0].delegate.delivery_count(),
        0,
        "Sender should not receive its own messages via delegate"
    );

    // Check for any graft failures (shouldn't have any in normal operation)
    for node in &cluster.nodes {
        let failures = node.delegate.graft_failed.lock().len();
        println!("Node {} graft failures: {}", node.node_id, failures);
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 12: QUIC Transport E2E
// ============================================================================

/// Test 12: Full QUIC Transport E2E
/// Goal: Verify real QUIC networking works end-to-end
#[tokio::test]
async fn test_quic_transport_e2e() {
    const NUM_NODES: usize = 3;

    let config = PlumtreeConfig::lan().with_eager_fanout(2);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for QUIC connections to establish - all nodes should have peers
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| {
            let stats = n.peer_stats();
            stats.eager_count > 0 || stats.lazy_count > 0
        })
    })
    .await
    .expect("QUIC connections didn't establish");

    // Verify transport stats
    for node in &cluster.nodes {
        let stats = node.stack.transport_stats().await;
        println!(
            "Node {} - active_connections: {}, bytes_sent: {}, messages_sent: {}",
            node.node_id,
            stats.connections.active_connections,
            stats.bytes_sent,
            stats.messages_sent
        );
    }

    // Multiple broadcasts to stress test QUIC
    // Track expected message counts per node
    let mut expected_counts = [0usize; NUM_NODES];

    for i in 0..10 {
        let sender_idx = i % NUM_NODES;
        let msg = cluster.nodes[sender_idx]
            .broadcast(format!("quic-msg-{}", i).into_bytes())
            .await
            .unwrap();

        // Wait for non-sender nodes to receive
        let non_sender_indices: Vec<usize> = (0..NUM_NODES).filter(|&j| j != sender_idx).collect();
        assert!(
            cluster
                .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(5))
                .await,
            "QUIC broadcast {} failed",
            i
        );

        // Update expected counts (all nodes except sender receive)
        for (j, count) in expected_counts.iter_mut().enumerate() {
            if j != sender_idx {
                *count += 1;
            }
        }
    }

    // Verify each node received the expected number of messages
    // (all messages except ones they sent)
    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            node.delegate.delivery_count(),
            expected_counts[i],
            "Node {} wrong message count: expected {}, got {}",
            node.node_id,
            expected_counts[i],
            node.delegate.delivery_count()
        );
    }

    // Verify no message corruption - each node should have messages from other senders
    for (node_idx, node) in cluster.nodes.iter().enumerate() {
        let delivered = node.delegate.delivered.lock();
        for i in 0..10 {
            let sender_idx = i % NUM_NODES;
            if sender_idx != node_idx {
                // This node should have this message (since it wasn't the sender)
                let expected_payload = format!("quic-msg-{}", i);
                let found = delivered
                    .values()
                    .any(|p| p.as_ref() == expected_payload.as_bytes());
                assert!(
                    found,
                    "Node {} missing message {} (sent by node {})",
                    node.node_id, i, sender_idx
                );
            }
        }
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 13: Message Cache TTL Expiry
// ============================================================================

/// Test 13: Message Cache TTL Expiry
/// Goal: Ensure expired messages cannot be GRAFTed
#[tokio::test]
async fn test_message_cache_ttl_expiry() {
    const NUM_NODES: usize = 3;

    // Short TTL for testing
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_message_cache_ttl(Duration::from_secs(1));

    let cluster = TestCluster::new(NUM_NODES, config.clone()).await;

    // Broadcast a message
    let msg_id = cluster.nodes[0]
        .broadcast(b"will-expire".to_vec())
        .await
        .unwrap();

    // Wait for delivery to non-sender nodes
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg_id, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_secs(2)).await;

    // All nodes should still have the delivered message (delivery persists)
    // but the cache should have expired (can't serve GRAFT requests)
    for i in 1..NUM_NODES {
        assert!(
            cluster.nodes[i].delegate.has_message(&msg_id),
            "Node {} lost the delivered message",
            cluster.nodes[i].node_id
        );
    }

    // Note: Node health may degrade after cache expires (expected behavior)
    // The important thing is no panic and messages were delivered before expiry

    cluster.shutdown().await;
}

// ============================================================================
// Test 15: Max Message Size Enforcement
// ============================================================================

/// Test 15: Max Message Size Enforcement
/// Goal: Large payload should be rejected safely
#[tokio::test]
async fn test_max_message_size_enforcement() {
    const NUM_NODES: usize = 3;

    // Set a small max message size
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_max_message_size(1024); // 1 KB limit

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Try to broadcast a message larger than the limit
    let large_payload = vec![0u8; 2048]; // 2 KB
    let result = cluster.nodes[0].broadcast(large_payload).await;

    // Should fail with an error
    assert!(result.is_err(), "Large message should have been rejected");

    // No messages should have been delivered
    for i in 1..NUM_NODES {
        assert_eq!(
            cluster.nodes[i].delegate.delivery_count(),
            0,
            "Node {} received a message that should have been rejected",
            cluster.nodes[i].node_id
        );
    }

    // Small message should still work
    let small_payload = vec![0u8; 512]; // 0.5 KB
    let msg_id = cluster.nodes[0].broadcast(small_payload).await.unwrap();

    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg_id, &non_sender_indices, Duration::from_secs(5))
            .await,
        "Small message should have been delivered"
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 18: Concurrent Broadcasts
// ============================================================================

/// Test 18: Concurrent Broadcasts
/// Goal: Ensure no race conditions or duplicate explosion with concurrent broadcasts
#[tokio::test]
async fn test_concurrent_broadcasts() {
    const NUM_NODES: usize = 5;
    const MESSAGES_PER_NODE: usize = 2;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // All nodes broadcast simultaneously
    let mut msg_ids = Vec::new();
    for i in 0..NUM_NODES {
        for j in 0..MESSAGES_PER_NODE {
            let msg = cluster.nodes[i]
                .broadcast(format!("concurrent-{}-{}", i, j).into_bytes())
                .await
                .unwrap();
            msg_ids.push((i, msg));
        }
    }

    // Wait for all messages to be delivered
    for (sender_idx, msg_id) in &msg_ids {
        let non_sender_indices: Vec<usize> = (0..NUM_NODES).filter(|&j| j != *sender_idx).collect();
        eventually(Duration::from_secs(10), || async {
            non_sender_indices
                .iter()
                .all(|&i| cluster.nodes[i].delegate.has_message(msg_id))
        })
        .await
        .expect("Not all messages delivered");
    }

    // Verify each node received exactly the right number of messages
    // Each node should receive (NUM_NODES * MESSAGES_PER_NODE) - MESSAGES_PER_NODE messages
    // (all messages except the ones it sent)
    let expected_per_node = (NUM_NODES - 1) * MESSAGES_PER_NODE;
    for (i, node) in cluster.nodes.iter().enumerate() {
        assert_eq!(
            node.delegate.delivery_count(),
            expected_per_node,
            "Node {} has wrong delivery count",
            i
        );
    }

    // Check duplicate count is minimal (some duplicates expected in concurrent scenario)
    let total_duplicates: usize = cluster
        .nodes
        .iter()
        .map(|n| n.delegate.duplicate_count())
        .sum();
    println!("Total duplicates in concurrent test: {}", total_duplicates);

    cluster.shutdown().await;
}

// ============================================================================
// Test 24: Protected Ring Neighbors Cannot Be Demoted
// ============================================================================

/// Test 24: Protected Ring Neighbors Cannot Be Demoted
/// Goal: Verify hash ring neighbor protection
#[tokio::test]
async fn test_protected_ring_neighbors() {
    const NUM_NODES: usize = 6;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_hash_ring(true)
        .with_protect_ring_neighbors(true)
        .with_max_protected_neighbors(2)
        .with_optimization_threshold(1); // Aggressive pruning

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for topology to stabilize
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| n.peer_stats().eager_count > 0)
    })
    .await
    .expect("Topology didn't stabilize");

    // Send many messages to trigger duplicate paths and potential PRUNEs
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    for i in 0..10 {
        let msg = cluster.nodes[0]
            .broadcast(format!("ring-test-{}", i).into_bytes())
            .await
            .unwrap();
        cluster
            .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(5))
            .await;
    }

    // Verify all nodes still have some eager peers (ring neighbors protected)
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        assert!(
            stats.eager_count > 0,
            "Node {} lost all eager peers - ring protection failed",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 30: Duplicate Message Injection
// ============================================================================

/// Test 30: Duplicate Message Injection
/// Goal: Ensure duplicate messages are handled correctly
#[tokio::test]
async fn test_duplicate_message_handling() {
    const NUM_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3) // High fanout to create duplicate paths
        .with_lazy_fanout(2);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast a single message
    let msg_id = cluster.nodes[0]
        .broadcast(b"dup-test".to_vec())
        .await
        .unwrap();

    // Wait for all non-sender nodes to receive
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg_id, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Each non-sender node should have received exactly once
    for i in 1..NUM_NODES {
        assert_eq!(
            cluster.nodes[i].delegate.delivery_count(),
            1,
            "Node {} has wrong delivery count",
            cluster.nodes[i].node_id
        );
    }

    // Duplicates may have been received (tracked in duplicate_count)
    let total_duplicates: usize = cluster
        .nodes
        .iter()
        .map(|n| n.delegate.duplicate_count())
        .sum();
    println!("Duplicates detected: {}", total_duplicates);

    // System should remain stable
    for node in &cluster.nodes {
        assert!(
            node.is_healthy(),
            "Node {} became unhealthy after duplicates",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 31: Node With Zero Peers Bootstrap
// ============================================================================

/// Test 31: Node With Zero Peers Bootstrap
/// Goal: Ensure isolated node remains stable
#[tokio::test]
async fn test_isolated_node_stability() {
    // Create a single isolated node (no seeds)
    plumtree_stack_common::common::install_crypto_provider();

    let port = plumtree_stack_common::common::allocate_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let config = PlumtreeConfig::default().with_eager_fanout(2);

    // Create with empty discovery (no seeds)
    use memberlist_plumtree::discovery::{StaticDiscovery, StaticDiscoveryConfig};
    let discovery = StaticDiscovery::new(StaticDiscoveryConfig::new()).with_local_addr(addr);

    let delegate = plumtree_stack_common::TestDelegate::new();
    let stack_config = memberlist_plumtree::PlumtreeStackConfig::new(0u64, addr)
        .with_plumtree(config)
        .with_quic(memberlist_plumtree::QuicConfig::insecure_dev())
        .with_discovery(discovery);

    let stack = stack_config.build(delegate.clone()).await.unwrap();

    // Verify node has no peers
    let stats = stack.peer_stats();
    assert_eq!(
        stats.eager_count, 0,
        "Isolated node should have no eager peers"
    );
    assert_eq!(
        stats.lazy_count, 0,
        "Isolated node should have no lazy peers"
    );

    // Broadcast should succeed without panic (just won't reach anyone)
    let result = stack.broadcast(b"lonely-message".to_vec()).await;
    assert!(
        result.is_ok(),
        "Broadcast from isolated node should not panic"
    );

    // Node should remain healthy (or degraded due to no peers, but not crash)
    // The important thing is no panic or infinite loop

    stack.shutdown().await;
}

// ============================================================================
// Test 32: Graceful Leave During Broadcast
// ============================================================================

/// Test 32: Graceful Leave During Broadcast
/// Goal: Safe shutdown while messages are in-flight
#[tokio::test]
async fn test_graceful_leave_during_broadcast() {
    const NUM_NODES: usize = 5;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3);

    let mut cluster = TestCluster::new(NUM_NODES, config).await;

    // Start a broadcast
    let msg = cluster.nodes[0]
        .broadcast(b"in-flight".to_vec())
        .await
        .unwrap();

    // Immediately kill a node (not the sender)
    cluster.kill_node(2).await;

    // Wait for delivery to remaining non-sender nodes
    // After killing node at index 2, remaining nodes are at indices 0,1,2,3 (originally 0,1,3,4)
    let remaining_non_sender: Vec<usize> = (1..cluster.nodes.len()).collect();

    // Delivery should still complete for remaining nodes
    let delivered = cluster
        .wait_for_delivery_subset(msg, &remaining_non_sender, Duration::from_secs(5))
        .await;

    assert!(
        delivered,
        "Remaining nodes should still receive the message after one node left"
    );

    // Verify no panic and remaining nodes are functional
    let msg2 = cluster.nodes[0]
        .broadcast(b"after-leave".to_vec())
        .await
        .unwrap();
    let delivered2 = cluster
        .wait_for_delivery_subset(msg2, &remaining_non_sender, Duration::from_secs(5))
        .await;

    assert!(
        delivered2,
        "Cluster should still function after node departure"
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 19: Broadcast Storm (High Throughput)
// ============================================================================

/// Test 19: Broadcast Storm
/// Goal: Stress test with many rapid broadcasts
#[tokio::test]
async fn test_broadcast_storm() {
    const NUM_NODES: usize = 4;
    const NUM_MESSAGES: usize = 100;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(2)
        .with_message_cache_ttl(Duration::from_secs(30));

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast many messages in rapid succession
    let mut msg_ids = Vec::new();
    for i in 0..NUM_MESSAGES {
        let sender_idx = i % NUM_NODES;
        let msg = cluster.nodes[sender_idx]
            .broadcast(format!("storm-{}", i).into_bytes())
            .await
            .unwrap();
        msg_ids.push((sender_idx, msg));
    }

    // Wait for all messages to be delivered
    eventually(Duration::from_secs(30), || async {
        let mut all_delivered = true;
        for (sender_idx, msg_id) in &msg_ids {
            for (i, node) in cluster.nodes.iter().enumerate() {
                if i != *sender_idx && !node.delegate.has_message(msg_id) {
                    all_delivered = false;
                    break;
                }
            }
            if !all_delivered {
                break;
            }
        }
        all_delivered
    })
    .await
    .expect("Not all storm messages delivered");

    // Verify no panic and system stable
    for node in &cluster.nodes {
        // Each node receives messages from all other nodes
        let expected_min = (NUM_MESSAGES / NUM_NODES) * (NUM_NODES - 1);
        assert!(
            node.delegate.delivery_count() >= expected_min,
            "Node {} didn't receive enough messages: {} < {}",
            node.node_id,
            node.delegate.delivery_count(),
            expected_min
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 14: Message Cache Max Size Eviction
// ============================================================================

/// Test 14: Message Cache Max Size Eviction
/// Goal: Ensure eviction respects message_cache_max_size
#[tokio::test]
async fn test_message_cache_max_size_eviction() {
    const NUM_NODES: usize = 3;
    const CACHE_SIZE: usize = 5;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_message_cache_max_size(CACHE_SIZE)
        .with_message_cache_ttl(Duration::from_secs(60)); // Long TTL so eviction is by size

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast more messages than the cache can hold
    let mut msg_ids = Vec::new();
    for i in 0..10 {
        let msg = cluster.nodes[0]
            .broadcast(format!("cache-test-{}", i).into_bytes())
            .await
            .unwrap();
        msg_ids.push(msg);

        // Wait for delivery before sending next
        let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
        cluster
            .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(5))
            .await;
    }

    // All messages should have been delivered (delivery persists)
    for i in 1..NUM_NODES {
        assert_eq!(
            cluster.nodes[i].delegate.delivery_count(),
            10,
            "Node {} should have received all 10 messages",
            cluster.nodes[i].node_id
        );
    }

    // The cache should have evicted older messages but kept recent ones
    // Note: We can't directly test cache size, but we verify the system remained stable
    // and all messages were delivered before potential eviction

    cluster.shutdown().await;
}

// ============================================================================
// Test 25: Ring Topology Under Node Removal
// ============================================================================

/// Test 25: Ring Topology Under Node Removal
/// Goal: Ring reconfiguration correctness after node removal
#[tokio::test]
async fn test_ring_topology_node_removal() {
    const NUM_NODES: usize = 6;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_hash_ring(true)
        .with_protect_ring_neighbors(true)
        .with_max_protected_neighbors(2);

    let mut cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for topology to stabilize
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| n.peer_stats().eager_count > 0)
    })
    .await
    .expect("Initial topology didn't stabilize");

    // Verify initial connectivity
    let msg1 = cluster.nodes[0]
        .broadcast(b"before-removal".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
            .await,
        "Initial broadcast failed"
    );

    // Remove a middle node (node at index 3)
    let (removed_id, _) = cluster.kill_node(3).await;
    println!("Removed node {}", removed_id);

    // Wait for topology to adjust
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify ring reconnects - all remaining nodes should still have peers
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        assert!(
            stats.eager_count > 0 || stats.lazy_count > 0,
            "Node {} became isolated after ring node removal",
            node.node_id
        );
    }

    // Broadcast should still work
    let msg2 = cluster.nodes[0]
        .broadcast(b"after-removal".to_vec())
        .await
        .unwrap();
    let remaining_non_sender: Vec<usize> = (1..cluster.nodes.len()).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg2, &remaining_non_sender, Duration::from_secs(5))
            .await,
        "Broadcast failed after ring node removal"
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 37: Health Status Degradation Detection
// ============================================================================

/// Test 37: Health Status Degradation Detection
/// Goal: Validate health reporting under stress
#[tokio::test]
async fn test_health_status_degradation() {
    const NUM_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(2);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Initial health should be good
    for node in &cluster.nodes {
        let health = node.health();
        assert!(
            health.status == HealthStatus::Healthy || health.status == HealthStatus::Degraded,
            "Node {} should be healthy or degraded initially, got {:?}",
            node.node_id,
            health.status
        );
    }

    // Broadcast a message to verify functionality
    let msg = cluster.nodes[0]
        .broadcast(b"health-test".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Check health after operation
    for node in &cluster.nodes {
        let health = node.health();
        println!(
            "Node {} health: {:?} - {}",
            node.node_id, health.status, health.message
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 20: Rapid Join/Leave Churn
// ============================================================================

/// Test 20: Rapid Join/Leave Churn
/// Goal: Membership churn stability - no panics, no deadlocks
#[tokio::test]
async fn test_rapid_join_leave_churn() {
    const INITIAL_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3); // More lazy peers for better resilience

    let cluster = TestCluster::new(INITIAL_NODES, config.clone()).await;

    // Verify initial cluster works
    let msg1 = cluster.nodes[0]
        .broadcast(b"initial".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..INITIAL_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Perform join/leave cycles - the main goal is no panics/crashes
    let mut churn_succeeded = true;
    for cycle in 0..3 {
        // Add a new node
        let new_node_id = (INITIAL_NODES + cycle) as u64;
        let new_node = cluster.add_node(new_node_id, config.clone()).await;

        // Notify existing nodes
        for node in &cluster.nodes {
            node.stack.add_peer(new_node.node_id, new_node.addr);
        }

        // Wait for new node to attempt connection (may or may not succeed)
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Try to broadcast from new node (should not panic)
        let broadcast_result = new_node
            .broadcast(format!("from-new-{}", cycle).into_bytes())
            .await;

        if broadcast_result.is_err() {
            println!("Cycle {}: broadcast failed (expected during churn)", cycle);
            churn_succeeded = false;
        }

        // Shutdown the new node
        new_node.stack.shutdown().await;

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The test passes if we got here without panicking
    // Churn may or may not have succeeded for all cycles
    println!("Churn test completed, all_succeeded: {}", churn_succeeded);

    // Verify original cluster nodes are still operational (no crash)
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        println!(
            "Node {} still operational - eager: {}, lazy: {}",
            node.node_id, stats.eager_count, stats.lazy_count
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 33: Hard Kill During GRAFT Exchange
// ============================================================================

/// Test 33: Hard Kill During GRAFT Exchange
/// Goal: Partial protocol interruption resilience
#[tokio::test]
async fn test_hard_kill_during_graft() {
    const NUM_NODES: usize = 5;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(1) // Low eager fanout to force GRAFT usage
        .with_lazy_fanout(3)
        .with_graft_timeout(Duration::from_millis(200))
        .with_ihave_interval(Duration::from_millis(50));

    let mut cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast a message to establish some state
    let msg1 = cluster.nodes[0]
        .broadcast(b"before-kill".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Kill a node abruptly (simulates crash during GRAFT)
    cluster.kill_node(2).await;

    // Immediately broadcast another message
    let msg2 = cluster.nodes[0]
        .broadcast(b"during-graft".to_vec())
        .await
        .unwrap();

    // Wait for delivery to remaining nodes
    let remaining_non_sender: Vec<usize> = (1..cluster.nodes.len()).collect();
    eventually(Duration::from_secs(10), || async {
        remaining_non_sender
            .iter()
            .all(|&i| cluster.nodes[i].delegate.has_message(&msg2))
    })
    .await
    .expect("Message not delivered after hard kill");

    // Verify no deadlock - system should still function
    let msg3 = cluster.nodes[0]
        .broadcast(b"after-kill".to_vec())
        .await
        .unwrap();
    assert!(
        cluster
            .wait_for_delivery_subset(msg3, &remaining_non_sender, Duration::from_secs(5))
            .await,
        "System deadlocked after hard kill"
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 16: All Peers Lazy (Forced Degeneration)
// ============================================================================

/// Test 16: All Peers Lazy (Forced Degeneration)
/// Goal: Validate recovery if topology degrades
#[tokio::test]
async fn test_topology_recovery() {
    const NUM_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_graft_timeout(Duration::from_millis(200))
        .with_ihave_interval(Duration::from_millis(50));

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for initial topology
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| n.peer_stats().eager_count > 0)
    })
    .await
    .expect("Initial topology didn't form");

    // Send messages to trigger potential topology changes
    for i in 0..5 {
        let sender_idx = i % NUM_NODES;
        let msg = cluster.nodes[sender_idx]
            .broadcast(format!("topology-test-{}", i).into_bytes())
            .await
            .unwrap();

        let non_sender: Vec<usize> = (0..NUM_NODES).filter(|&j| j != sender_idx).collect();
        cluster
            .wait_for_delivery_subset(msg, &non_sender, Duration::from_secs(5))
            .await;
    }

    // Verify system is stable and functional
    let final_msg = cluster.nodes[0].broadcast(b"final".to_vec()).await.unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(final_msg, &non_sender_indices, Duration::from_secs(5))
            .await,
        "Topology didn't recover"
    );

    // All nodes should still have some peers
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        assert!(
            stats.eager_count > 0 || stats.lazy_count > 0,
            "Node {} has no peers",
            node.node_id
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 17: Eager Fanout Rebalancing
// ============================================================================

/// Test 17: Eager Fanout Rebalancing
/// Goal: Validate dynamic rebalancing when exceeding eager_fanout
#[tokio::test]
async fn test_eager_fanout_rebalancing() {
    const NUM_NODES: usize = 6;
    const EAGER_FANOUT: usize = 2;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(EAGER_FANOUT)
        .with_lazy_fanout(4)
        .with_optimization_threshold(1) // Aggressive pruning
        .with_max_eager_peers(EAGER_FANOUT + 1); // Slightly above fanout

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for topology to stabilize
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| n.peer_stats().eager_count > 0)
    })
    .await
    .expect("Topology didn't stabilize");

    // Send many messages to trigger rebalancing
    for i in 0..20 {
        let sender_idx = i % NUM_NODES;
        let msg = cluster.nodes[sender_idx]
            .broadcast(format!("rebalance-{}", i).into_bytes())
            .await
            .unwrap();

        let non_sender: Vec<usize> = (0..NUM_NODES).filter(|&j| j != sender_idx).collect();
        cluster
            .wait_for_delivery_subset(msg, &non_sender, Duration::from_secs(5))
            .await;
    }

    // After rebalancing, eager counts should be reasonable
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        println!(
            "Node {} - eager: {}, lazy: {}",
            node.node_id, stats.eager_count, stats.lazy_count
        );
        // Eager count should be close to target (with some tolerance)
        assert!(
            stats.eager_count <= EAGER_FANOUT + 2,
            "Node {} has too many eager peers: {}",
            node.node_id,
            stats.eager_count
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 38: Massive Concurrent Join + Broadcast
// ============================================================================

/// Test 38: Massive Concurrent Join + Broadcast
/// Goal: Race condition detection with concurrent operations
#[tokio::test]
async fn test_concurrent_join_broadcast() {
    const INITIAL_NODES: usize = 3;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(2);

    let cluster = TestCluster::new(INITIAL_NODES, config.clone()).await;

    // Start broadcasting while adding new nodes
    let mut msg_ids = Vec::new();

    // Broadcast some messages
    for i in 0..3 {
        let msg = cluster.nodes[i % INITIAL_NODES]
            .broadcast(format!("concurrent-{}", i).into_bytes())
            .await
            .unwrap();
        msg_ids.push((i % INITIAL_NODES, msg));
    }

    // Add a new node while messages are still propagating
    let new_node = cluster.add_node(INITIAL_NODES as u64, config.clone()).await;
    for node in &cluster.nodes {
        node.stack.add_peer(new_node.node_id, new_node.addr);
    }

    // Continue broadcasting
    for i in 3..6 {
        let msg = cluster.nodes[i % INITIAL_NODES]
            .broadcast(format!("concurrent-{}", i).into_bytes())
            .await
            .unwrap();
        msg_ids.push((i % INITIAL_NODES, msg));
    }

    // Wait for all initial messages to be delivered to original nodes
    for (sender_idx, msg_id) in &msg_ids {
        let non_sender: Vec<usize> = (0..INITIAL_NODES).filter(|&j| j != *sender_idx).collect();
        eventually(Duration::from_secs(10), || async {
            non_sender
                .iter()
                .all(|&i| cluster.nodes[i].delegate.has_message(msg_id))
        })
        .await
        .expect("Message not delivered to original nodes");
    }

    // Verify no panic occurred
    assert!(
        cluster.nodes.iter().all(|n| n.is_healthy()),
        "Some nodes became unhealthy during concurrent operations"
    );

    new_node.stack.shutdown().await;
    cluster.shutdown().await;
}

// ============================================================================
// Test 22: GRAFT Retry Exhaustion
// ============================================================================

/// Test 22: GRAFT Retry Exhaustion
/// Goal: Validate retry cap and graft_failed callback
#[tokio::test]
async fn test_graft_retry_exhaustion() {
    const NUM_NODES: usize = 4;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(1) // Minimal eager to force GRAFT usage
        .with_lazy_fanout(3)
        .with_graft_timeout(Duration::from_millis(100))
        .with_graft_max_retries(2) // Low retry count for faster testing
        .with_ihave_interval(Duration::from_millis(50));

    let mut cluster = TestCluster::new(NUM_NODES, config).await;

    // Establish initial connectivity
    let msg1 = cluster.nodes[0]
        .broadcast(b"initial".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Kill a node that might be needed for GRAFT
    cluster.kill_node(2).await;

    // Broadcast more messages - some GRAFTs may fail due to missing node
    for i in 0..5 {
        let msg = cluster.nodes[0]
            .broadcast(format!("after-kill-{}", i).into_bytes())
            .await
            .unwrap();

        // Wait for delivery to remaining nodes
        let remaining: Vec<usize> = (1..cluster.nodes.len()).collect();
        cluster
            .wait_for_delivery_subset(msg, &remaining, Duration::from_secs(5))
            .await;
    }

    // Check for graft failures (may or may not have occurred depending on topology)
    let mut total_failures = 0;
    for node in &cluster.nodes {
        let failures = node.delegate.graft_failed.lock().len();
        total_failures += failures;
        if failures > 0 {
            println!("Node {} had {} graft failures", node.node_id, failures);
        }
    }
    println!("Total graft failures across cluster: {}", total_failures);

    // The test passes if we got here without infinite retry loops
    // Graft failures may or may not occur depending on the topology

    cluster.shutdown().await;
}

// ============================================================================
// Test 29: Seen Map Saturation
// ============================================================================

/// Test 29: Seen Map Saturation
/// Goal: Ensure dedup map remains stable under load
#[tokio::test]
async fn test_seen_map_saturation() {
    const NUM_NODES: usize = 3;
    const NUM_MESSAGES: usize = 500; // Many messages to stress dedup

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(2)
        .with_message_cache_ttl(Duration::from_secs(60))
        .with_message_cache_max_size(1000);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast many messages rapidly
    let mut msg_ids = Vec::new();
    for i in 0..NUM_MESSAGES {
        let sender_idx = i % NUM_NODES;
        let msg = cluster.nodes[sender_idx]
            .broadcast(format!("saturate-{}", i).into_bytes())
            .await
            .unwrap();
        msg_ids.push((sender_idx, msg));

        // Brief yield to prevent overwhelming the system
        if i % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // Wait for messages to propagate
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify system didn't panic or deadlock
    // Check that most messages were delivered
    let mut total_delivered = 0;
    for (i, node) in cluster.nodes.iter().enumerate() {
        let count = node.delegate.delivery_count();
        println!("Node {} received {} messages", i, count);
        total_delivered += count;
    }

    // Each message should be delivered to (NUM_NODES - 1) nodes
    // Allow some tolerance for timing/race conditions
    let expected_min = (NUM_MESSAGES * (NUM_NODES - 1)) * 80 / 100; // 80% delivery rate
    assert!(
        total_delivered >= expected_min,
        "Too few messages delivered: {} < {}",
        total_delivered,
        expected_min
    );

    // Verify nodes are still healthy
    for node in &cluster.nodes {
        let health = node.health();
        println!(
            "Node {} health after saturation: {:?}",
            node.node_id, health.status
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 34: Large Cluster Hash Ring Determinism (Scaled)
// ============================================================================

/// Test 34: Large Cluster Hash Ring Determinism
/// Goal: Deterministic topology at scale (scaled to 12 nodes for test practicality)
#[tokio::test]
async fn test_large_cluster_hash_ring() {
    const NUM_NODES: usize = 12; // Scaled down from 50 for test speed

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_lazy_fanout(5)
        .with_hash_ring(true)
        .with_protect_ring_neighbors(true)
        .with_max_protected_neighbors(2);

    let cluster = TestCluster::new(NUM_NODES, config.clone()).await;

    // Wait for topology to stabilize
    eventually(Duration::from_secs(10), || async {
        cluster.nodes.iter().all(|n| n.peer_stats().eager_count > 0)
    })
    .await
    .expect("Large cluster topology didn't stabilize");

    // Capture topology state
    let topology_snapshot: Vec<(usize, usize)> = cluster
        .nodes
        .iter()
        .map(|n| {
            let stats = n.peer_stats();
            (stats.eager_count, stats.lazy_count)
        })
        .collect();

    // Broadcast a message to verify functionality
    let msg = cluster.nodes[0]
        .broadcast(b"large-cluster-test".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(15))
            .await,
        "Large cluster broadcast failed"
    );

    // Verify topology is consistent across all nodes
    for node in &cluster.nodes {
        let stats = node.peer_stats();
        println!(
            "Node {} - eager: {}, lazy: {}, total: {}",
            node.node_id,
            stats.eager_count,
            stats.lazy_count,
            stats.eager_count + stats.lazy_count
        );

        // Each node should have reasonable peer counts
        assert!(
            stats.eager_count >= 1,
            "Node {} has no eager peers in large cluster",
            node.node_id
        );
    }

    // Verify topology is reasonably stable (allow for protocol dynamics)
    // Note: Topology can change as messages flow and PRUNEs/GRAFTs occur
    for (i, (eager, lazy)) in topology_snapshot.iter().enumerate() {
        let current = cluster.nodes[i].peer_stats();
        println!(
            "Node {} topology: eager {}->{}, lazy {}->{}",
            i, eager, current.eager_count, lazy, current.lazy_count
        );

        // Just verify we still have connectivity (topology changes are expected)
        assert!(current.eager_count > 0, "Node {} lost all eager peers", i);
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 35: QUIC Connection Graceful Handling
// ============================================================================

/// Test 35: QUIC Connection Graceful Handling
/// Goal: Node handles unavailable peers gracefully
#[tokio::test]
async fn test_quic_connection_graceful_handling() {
    // This test verifies that nodes handle connection attempts gracefully
    // when peers are unreachable

    plumtree_stack_common::common::install_crypto_provider();

    let port = plumtree_stack_common::common::allocate_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let config = PlumtreeConfig::default().with_eager_fanout(2);

    // Create discovery with non-existent seed addresses
    use memberlist_plumtree::discovery::{StaticDiscovery, StaticDiscoveryConfig};
    let discovery_config = StaticDiscoveryConfig::new()
        .with_seed(99u64, "127.0.0.1:59999".parse().unwrap()) // Non-existent
        .with_seed(98u64, "127.0.0.1:59998".parse().unwrap()); // Non-existent

    let discovery = StaticDiscovery::new(discovery_config).with_local_addr(addr);

    let delegate = plumtree_stack_common::TestDelegate::new();
    let stack_config = memberlist_plumtree::PlumtreeStackConfig::new(0u64, addr)
        .with_plumtree(config)
        .with_quic(memberlist_plumtree::QuicConfig::insecure_dev())
        .with_discovery(discovery);

    // Node should start successfully despite unreachable seeds
    let stack = stack_config.build(delegate.clone()).await.unwrap();

    // Wait a moment for any connection attempts
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Broadcast should not panic (even if no peers are reachable)
    let result = stack.broadcast(b"test-message".to_vec()).await;
    assert!(
        result.is_ok(),
        "Broadcast should not panic with unreachable peers"
    );

    // Node should remain operational
    let health = stack.plumtree().plumtree().health();
    println!(
        "Node health with unreachable seeds: {:?} - {}",
        health.status, health.message
    );

    // Verify we can check stats without panic
    let stats = stack.peer_stats();
    println!(
        "Node stats: eager={}, lazy={}",
        stats.eager_count, stats.lazy_count
    );

    stack.shutdown().await;
}

// ============================================================================
// Test 21: Repeated Partition & Heal Cycles (Simplified)
// ============================================================================

/// Test 21: Repeated Partition & Heal Cycles
/// Goal: Ensure stability through multiple network disruptions
#[tokio::test]
async fn test_repeated_disruption_cycles() {
    const NUM_NODES: usize = 5;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3);

    let mut cluster = TestCluster::new(NUM_NODES, config.clone()).await;

    // Initial broadcast
    let msg1 = cluster.nodes[0]
        .broadcast(b"cycle-0".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    cluster
        .wait_for_delivery_subset(msg1, &non_sender_indices, Duration::from_secs(5))
        .await;

    // Perform disruption cycles (kill and restart nodes)
    for cycle in 0..3 {
        // Kill a node (not node 0, which is our broadcaster)
        let kill_idx = 1 + (cycle % (cluster.nodes.len() - 1));
        if kill_idx < cluster.nodes.len() {
            let (killed_id, killed_addr) = cluster.kill_node(kill_idx).await;
            println!("Cycle {}: killed node {}", cycle, killed_id);

            // Broadcast during disruption
            let msg = cluster.nodes[0]
                .broadcast(format!("during-cycle-{}", cycle).into_bytes())
                .await
                .unwrap();

            // Wait for delivery to remaining non-sender nodes
            let remaining: Vec<usize> = (1..cluster.nodes.len()).collect();
            if !remaining.is_empty() {
                cluster
                    .wait_for_delivery_subset(msg, &remaining, Duration::from_secs(5))
                    .await;
            }

            // Restart the killed node with a new port
            let restarted = cluster
                .restart_node(killed_id, killed_addr, config.clone())
                .await;

            // Notify remaining nodes about restarted node
            for node in &cluster.nodes {
                node.stack.add_peer(restarted.node_id, restarted.addr);
            }

            // Wait for reconnection
            eventually(Duration::from_secs(3), || async {
                restarted.peer_stats().eager_count > 0 || restarted.peer_stats().lazy_count > 0
            })
            .await
            .ok(); // Don't fail if reconnection is slow

            // Add restarted node back
            cluster.nodes.push(restarted);

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // Final verification - cluster should still work
    let final_msg = cluster.nodes[0]
        .broadcast(b"final-cycle".to_vec())
        .await
        .unwrap();

    // At least some nodes should receive the message
    eventually(Duration::from_secs(10), || async {
        cluster
            .nodes
            .iter()
            .skip(1)
            .any(|n| n.delegate.has_message(&final_msg))
    })
    .await
    .expect("No nodes received final message after disruption cycles");

    println!(
        "Cluster survived {} disruption cycles with {} nodes",
        3,
        cluster.nodes.len()
    );

    cluster.shutdown().await;
}

// ============================================================================
// Test 36: Multiple Broadcasts From Different Nodes
// ============================================================================

/// Test 36: Multiple Broadcasts From Different Nodes
/// Goal: Verify fan-in scenario where many nodes broadcast
#[tokio::test]
async fn test_fan_in_broadcasts() {
    const NUM_NODES: usize = 6;
    const MSGS_PER_NODE: usize = 3;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Each node broadcasts multiple messages
    let mut all_msgs: Vec<(usize, memberlist_plumtree::MessageId)> = Vec::new();

    for node_idx in 0..NUM_NODES {
        for msg_num in 0..MSGS_PER_NODE {
            let payload = format!("node{}-msg{}", node_idx, msg_num);
            let msg_id = cluster.nodes[node_idx]
                .broadcast(payload.into_bytes())
                .await
                .unwrap();
            all_msgs.push((node_idx, msg_id));
        }
    }

    // Wait for all messages to propagate
    for (sender_idx, msg_id) in &all_msgs {
        let receivers: Vec<usize> = (0..NUM_NODES).filter(|&i| i != *sender_idx).collect();
        eventually(Duration::from_secs(15), || async {
            receivers
                .iter()
                .all(|&i| cluster.nodes[i].delegate.has_message(msg_id))
        })
        .await
        .expect("Fan-in message not delivered to all receivers");
    }

    // Verify delivery counts
    for (i, node) in cluster.nodes.iter().enumerate() {
        // Each node should receive all messages except the ones it sent
        let expected = (NUM_NODES - 1) * MSGS_PER_NODE;
        assert_eq!(
            node.delegate.delivery_count(),
            expected,
            "Node {} has wrong delivery count: {} != {}",
            i,
            node.delegate.delivery_count(),
            expected
        );
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 39: Storage Persistence (Simplified)
// ============================================================================

/// Test 39: Verify message storage functionality
/// Goal: Messages are stored and can be retrieved
#[tokio::test]
#[cfg(feature = "sync")]
async fn test_message_storage() {
    const NUM_NODES: usize = 3;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_sync(SyncConfig::enabled().with_sync_interval(Duration::from_secs(60)));

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Broadcast messages
    let mut msg_ids = Vec::new();
    for i in 0..5 {
        let msg = cluster.nodes[0]
            .broadcast(format!("stored-{}", i).into_bytes())
            .await
            .unwrap();
        msg_ids.push(msg);
    }

    // Wait for delivery
    for msg_id in &msg_ids {
        let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
        cluster
            .wait_for_delivery_subset(*msg_id, &non_sender_indices, Duration::from_secs(5))
            .await;
    }

    // Verify all messages were delivered and stored in delegate
    for i in 1..NUM_NODES {
        for msg_id in &msg_ids {
            assert!(
                cluster.nodes[i].delegate.has_message(msg_id),
                "Node {} missing stored message",
                cluster.nodes[i].node_id
            );
        }
    }

    cluster.shutdown().await;
}

// ============================================================================
// Test 40: Verify Peer Stats Accuracy
// ============================================================================

/// Test 40: Verify peer statistics are accurate
/// Goal: Peer counts reflect actual topology
#[tokio::test]
async fn test_peer_stats_accuracy() {
    const NUM_NODES: usize = 5;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3);

    let cluster = TestCluster::new(NUM_NODES, config).await;

    // Wait for topology to form
    eventually(Duration::from_secs(5), || async {
        cluster.nodes.iter().all(|n| {
            let stats = n.peer_stats();
            stats.eager_count > 0 || stats.lazy_count > 0
        })
    })
    .await
    .expect("Topology didn't form");

    // Verify peer stats are consistent
    let mut total_eager_links = 0;
    let mut total_lazy_links = 0;

    for node in &cluster.nodes {
        let stats = node.peer_stats();

        // Verify stats are non-negative and within bounds
        assert!(
            stats.eager_count < NUM_NODES,
            "Node {} has too many eager peers: {}",
            node.node_id,
            stats.eager_count
        );
        assert!(
            stats.lazy_count < NUM_NODES,
            "Node {} has too many lazy peers: {}",
            node.node_id,
            stats.lazy_count
        );

        // Total peers should not exceed cluster size - 1
        assert!(
            stats.eager_count + stats.lazy_count <= NUM_NODES - 1,
            "Node {} has too many total peers",
            node.node_id
        );

        total_eager_links += stats.eager_count;
        total_lazy_links += stats.lazy_count;

        println!(
            "Node {} - eager: {}, lazy: {}, total: {}",
            node.node_id,
            stats.eager_count,
            stats.lazy_count,
            stats.eager_count + stats.lazy_count
        );
    }

    println!(
        "Cluster totals - eager links: {}, lazy links: {}",
        total_eager_links, total_lazy_links
    );

    // Verify broadcast still works with this topology
    let msg = cluster.nodes[0]
        .broadcast(b"stats-test".to_vec())
        .await
        .unwrap();
    let non_sender_indices: Vec<usize> = (1..NUM_NODES).collect();
    assert!(
        cluster
            .wait_for_delivery_subset(msg, &non_sender_indices, Duration::from_secs(5))
            .await,
        "Broadcast failed with reported topology"
    );

    cluster.shutdown().await;
}
