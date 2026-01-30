//! Split-brain (network partition) tests.
//!
//! These tests verify that the protocol handles network partitions correctly
//! and can recover when partitions heal.

use bytes::Bytes;
use std::time::Duration;

use super::harness::ChaosTestCluster;

/// Test that messages only propagate within partition groups.
#[test]
fn test_split_brain_isolation() {
    let cluster = ChaosTestCluster::new(6);

    // Create split-brain: nodes 0,1,2 in group A, nodes 3,4,5 in group B
    cluster.chaos.split_brain(vec![0, 1, 2], vec![3, 4, 5]);

    // Broadcast from node 0 (group A)
    let id_a = cluster.broadcast(0, Bytes::from("from group A")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only group A nodes should receive it
    assert!(cluster.node(0).unwrap().has_message(&id_a));
    assert!(cluster.node(1).unwrap().has_message(&id_a));
    assert!(cluster.node(2).unwrap().has_message(&id_a));
    assert!(!cluster.node(3).unwrap().has_message(&id_a));
    assert!(!cluster.node(4).unwrap().has_message(&id_a));
    assert!(!cluster.node(5).unwrap().has_message(&id_a));

    // Broadcast from node 4 (group B)
    let id_b = cluster.broadcast(4, Bytes::from("from group B")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only group B nodes should receive it
    assert!(!cluster.node(0).unwrap().has_message(&id_b));
    assert!(!cluster.node(1).unwrap().has_message(&id_b));
    assert!(!cluster.node(2).unwrap().has_message(&id_b));
    assert!(cluster.node(3).unwrap().has_message(&id_b));
    assert!(cluster.node(4).unwrap().has_message(&id_b));
    assert!(cluster.node(5).unwrap().has_message(&id_b));
}

/// Test that messages propagate after partition heals.
#[test]
fn test_partition_heal() {
    let cluster = ChaosTestCluster::new(4);

    // Create partition
    cluster.chaos.split_brain(vec![0, 1], vec![2, 3]);

    // Broadcast during partition
    let id = cluster
        .broadcast(0, Bytes::from("during partition"))
        .unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only nodes 0,1 have the message
    assert_eq!(cluster.delivery_count(&id), 2);

    // Heal partition
    cluster.chaos.heal_split_brain();

    // Broadcast new message
    let id2 = cluster.broadcast(0, Bytes::from("after heal")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // All nodes should receive the new message
    assert!(cluster.all_online_received(&id2));
}

/// Test asymmetric partition (one-way communication failure).
#[test]
fn test_asymmetric_partition() {
    let cluster = ChaosTestCluster::new(3);

    // Node 1 can send to node 2, but node 2 cannot send to node 1
    // (This simulates a one-way network failure)
    cluster.chaos.partition.partition(2, 1);

    // Broadcast from node 0
    let _id = cluster.broadcast(0, Bytes::from("test")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Message propagation depends on topology
    // In a ring 0-1-2-0, message from 0 goes to 1 and 2
    // Node 2 can forward to 0 but not to 1 (partition)
    // The exact delivery depends on the eager/lazy topology
}

/// Test minority partition (small group isolated).
#[test]
fn test_minority_partition() {
    let cluster = ChaosTestCluster::new(5);

    // Isolate node 0 from everyone
    cluster.chaos.partition.isolate(0, vec![1, 2, 3, 4]);

    // Broadcast from node 1 (majority)
    let id = cluster.broadcast(1, Bytes::from("from majority")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // All nodes except 0 should receive it
    assert!(!cluster.node(0).unwrap().has_message(&id));
    assert!(cluster.node(1).unwrap().has_message(&id));
    assert!(cluster.node(2).unwrap().has_message(&id));
    assert!(cluster.node(3).unwrap().has_message(&id));
    assert!(cluster.node(4).unwrap().has_message(&id));

    // Broadcast from isolated node 0
    let id2 = cluster.broadcast(0, Bytes::from("from isolated")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only node 0 should have it
    assert_eq!(cluster.delivery_count(&id2), 1);
}

/// Test partition during message propagation.
#[test]
fn test_partition_during_propagation() {
    let cluster = ChaosTestCluster::new(5);

    // Start broadcast
    let _id = cluster.broadcast(0, Bytes::from("test")).unwrap();

    // Immediately create partition
    cluster.chaos.split_brain(vec![0, 1], vec![2, 3, 4]);

    // Let messages propagate
    cluster.run_until_quiet(Duration::from_secs(1));

    // Result depends on timing - some messages may have crossed before partition
    // This test mainly checks for panics/deadlocks during partition creation
}

/// Test multiple sequential partitions.
#[test]
fn test_sequential_partitions() {
    let cluster = ChaosTestCluster::new(4);

    // First partition
    cluster.chaos.split_brain(vec![0, 1], vec![2, 3]);
    let id1 = cluster.broadcast(0, Bytes::from("p1")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));
    assert_eq!(cluster.delivery_count(&id1), 2);

    // Heal and create different partition
    cluster.chaos.heal_split_brain();
    cluster.chaos.split_brain(vec![0, 2], vec![1, 3]);
    let id2 = cluster.broadcast(0, Bytes::from("p2")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Node 0 and 2 should have message
    assert!(cluster.node(0).unwrap().has_message(&id2));
    assert!(cluster.node(2).unwrap().has_message(&id2));
}

/// Test three-way partition.
#[test]
fn test_three_way_partition() {
    let cluster = ChaosTestCluster::new(6);

    // Create three isolated groups: {0,1}, {2,3}, {4,5}
    // Group 1 isolated from groups 2 and 3
    for a in [0, 1] {
        for b in [2, 3, 4, 5] {
            cluster.chaos.partition.partition(a, b);
        }
    }
    // Group 2 isolated from group 3
    for a in [2, 3] {
        for b in [4, 5] {
            cluster.chaos.partition.partition(a, b);
        }
    }

    // Broadcast from each group
    let id0 = cluster.broadcast(0, Bytes::from("g0")).unwrap();
    let id2 = cluster.broadcast(2, Bytes::from("g1")).unwrap();
    let id4 = cluster.broadcast(4, Bytes::from("g2")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Each message should only reach its group
    assert_eq!(cluster.delivery_count(&id0), 2);
    assert_eq!(cluster.delivery_count(&id2), 2);
    assert_eq!(cluster.delivery_count(&id4), 2);
}
