//! Cascading failure tests.
//!
//! These tests verify protocol behavior when multiple nodes fail
//! in rapid succession, simulating real-world failure scenarios.

use bytes::Bytes;
use std::time::Duration;

use super::harness::ChaosTestCluster;

/// Test single node failure.
#[test]
fn test_single_node_failure() {
    let cluster = ChaosTestCluster::new(5);

    // Fail node 2
    cluster.node(2).unwrap().go_offline();

    // Broadcast from node 0
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // All online nodes should receive the message
    assert!(cluster.node(0).unwrap().has_message(&id));
    assert!(cluster.node(1).unwrap().has_message(&id));
    assert!(!cluster.node(2).unwrap().has_message(&id)); // Offline
    assert!(cluster.node(3).unwrap().has_message(&id));
    assert!(cluster.node(4).unwrap().has_message(&id));
}

/// Test multiple simultaneous failures.
#[test]
fn test_simultaneous_failures() {
    let cluster = ChaosTestCluster::new(7);

    // Fail multiple nodes at once
    cluster.node(1).unwrap().go_offline();
    cluster.node(3).unwrap().go_offline();
    cluster.node(5).unwrap().go_offline();

    // Broadcast from node 0
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(2));

    // Only online nodes should receive the message
    assert_eq!(cluster.delivery_count(&id), 4); // Nodes 0, 2, 4, 6
}

/// Test cascading failures (nodes fail one after another).
#[test]
fn test_cascading_failures() {
    let cluster = ChaosTestCluster::new(5);

    // First broadcast - all nodes online
    let id1 = cluster.broadcast(0, Bytes::from("msg1")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id1), 5);

    // First failure
    cluster.node(4).unwrap().go_offline();
    let id2 = cluster.broadcast(0, Bytes::from("msg2")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id2), 4);

    // Second failure
    cluster.node(3).unwrap().go_offline();
    let id3 = cluster.broadcast(0, Bytes::from("msg3")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id3), 3);

    // Third failure
    cluster.node(2).unwrap().go_offline();
    let id4 = cluster.broadcast(0, Bytes::from("msg4")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id4), 2); // Only nodes 0 and 1
}

/// Test node recovery after failure.
#[test]
fn test_node_recovery() {
    let cluster = ChaosTestCluster::new(5);

    // Fail node 2
    cluster.node(2).unwrap().go_offline();

    // Broadcast during failure
    let id1 = cluster.broadcast(0, Bytes::from("during failure")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert!(!cluster.node(2).unwrap().has_message(&id1));

    // Recover node 2
    cluster.node(2).unwrap().go_online();

    // Broadcast after recovery
    let id2 = cluster.broadcast(0, Bytes::from("after recovery")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert!(cluster.node(2).unwrap().has_message(&id2));
}

/// Test leader failure (source node fails).
#[test]
fn test_leader_failure() {
    let cluster = ChaosTestCluster::new(5);

    // Node 0 broadcasts
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();

    // Node 0 fails immediately after broadcast
    cluster.node(0).unwrap().go_offline();

    // Let messages propagate
    cluster.run_until_quiet(Duration::from_secs(1));

    // Other nodes should still receive the message
    assert!(cluster.node(1).unwrap().has_message(&id));
    assert!(cluster.node(2).unwrap().has_message(&id));
    assert!(cluster.node(3).unwrap().has_message(&id));
    assert!(cluster.node(4).unwrap().has_message(&id));
}

/// Test majority failure.
#[test]
fn test_majority_failure() {
    let cluster = ChaosTestCluster::new(5);

    // Fail majority (3 out of 5)
    cluster.node(2).unwrap().go_offline();
    cluster.node(3).unwrap().go_offline();
    cluster.node(4).unwrap().go_offline();

    // Broadcast from surviving node
    let id = cluster
        .broadcast(0, Bytes::from("minority message"))
        .unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only surviving nodes should have the message
    assert_eq!(cluster.delivery_count(&id), 2); // Nodes 0 and 1
}

/// Test all but one failure.
#[test]
fn test_single_survivor() {
    let cluster = ChaosTestCluster::new(5);

    // Fail all but node 0
    cluster.node(1).unwrap().go_offline();
    cluster.node(2).unwrap().go_offline();
    cluster.node(3).unwrap().go_offline();
    cluster.node(4).unwrap().go_offline();

    // Node 0 can still broadcast (to itself)
    let id = cluster.broadcast(0, Bytes::from("alone")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    assert_eq!(cluster.delivery_count(&id), 1); // Only node 0
    assert!(cluster.node(0).unwrap().has_message(&id));
}

/// Test failure and partition combined.
#[test]
fn test_failure_with_partition() {
    let cluster = ChaosTestCluster::new(6);

    // Create partition: {0,1,2} | {3,4,5}
    cluster.chaos.split_brain(vec![0, 1, 2], vec![3, 4, 5]);

    // Also fail one node in each partition
    cluster.node(1).unwrap().go_offline();
    cluster.node(4).unwrap().go_offline();

    // Broadcast in partition A
    let id_a = cluster.broadcast(0, Bytes::from("partition A")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only nodes 0, 2 should have the message (not 1-offline, not 3,4,5-partitioned)
    assert!(cluster.node(0).unwrap().has_message(&id_a));
    assert!(!cluster.node(1).unwrap().has_message(&id_a)); // Offline
    assert!(cluster.node(2).unwrap().has_message(&id_a));
    assert!(!cluster.node(3).unwrap().has_message(&id_a)); // Partitioned
}

/// Test rapid failure/recovery cycles.
#[test]
fn test_flapping_nodes() {
    let cluster = ChaosTestCluster::new(4);

    for _ in 0..5 {
        // Node 2 goes offline
        cluster.node(2).unwrap().go_offline();

        let id1 = cluster.broadcast(0, Bytes::from("offline")).unwrap();
        cluster.run_until_quiet(Duration::from_millis(100));
        assert!(!cluster.node(2).unwrap().has_message(&id1));

        // Node 2 comes back
        cluster.node(2).unwrap().go_online();

        let id2 = cluster.broadcast(0, Bytes::from("online")).unwrap();
        cluster.run_until_quiet(Duration::from_millis(100));
        assert!(cluster.node(2).unwrap().has_message(&id2));
    }
}

/// Test gradual cluster expansion and contraction.
#[test]
fn test_cluster_resize() {
    let cluster = ChaosTestCluster::new(5);

    // Start with all nodes online
    let id1 = cluster.broadcast(0, Bytes::from("5 nodes")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id1), 5);

    // Gradually shrink
    cluster.node(4).unwrap().go_offline();
    let id2 = cluster.broadcast(0, Bytes::from("4 nodes")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id2), 4);

    cluster.node(3).unwrap().go_offline();
    let id3 = cluster.broadcast(0, Bytes::from("3 nodes")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id3), 3);

    // Gradually expand
    cluster.node(3).unwrap().go_online();
    let id4 = cluster.broadcast(0, Bytes::from("4 nodes again")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id4), 4);

    cluster.node(4).unwrap().go_online();
    let id5 = cluster.broadcast(0, Bytes::from("5 nodes again")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id5), 5);
}
