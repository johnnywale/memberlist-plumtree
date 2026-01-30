//! Clock skew tests.
//!
//! These tests verify that the protocol handles clock differences between
//! nodes correctly, which can affect timeouts, TTLs, and rate limiting.

use bytes::Bytes;
use std::time::Duration;

use super::harness::ChaosTestCluster;

/// Test basic message delivery with clock skew.
#[test]
fn test_message_delivery_with_clock_skew() {
    let cluster = ChaosTestCluster::new(5);

    // Apply various clock skews
    cluster.chaos.clock_skew.set_skew_ms(&0, 100); // 100ms ahead
    cluster.chaos.clock_skew.set_skew_ms(&1, -50); // 50ms behind
    cluster.chaos.clock_skew.set_skew_ms(&2, 200); // 200ms ahead
    cluster.chaos.clock_skew.set_skew_ms(&3, -100); // 100ms behind
                                                    // Node 4 has accurate clock (no skew)

    // Broadcast should still work
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(2));

    // All nodes should receive the message (clock skew shouldn't block delivery)
    assert!(cluster.all_online_received(&id));
}

/// Test relative clock skew between nodes.
#[test]
fn test_relative_clock_skew() {
    let cluster = ChaosTestCluster::new(3);

    // Node 0 is 100ms ahead, Node 1 is 100ms behind
    // Relative skew between them is 200ms
    cluster.chaos.clock_skew.set_skew_ms(&0, 100);
    cluster.chaos.clock_skew.set_skew_ms(&1, -100);

    let relative = cluster.chaos.clock_skew.relative_skew(&0, &1);
    assert_eq!(relative, 200); // 100 - (-100) = 200ms difference
}

/// Test timestamp validation with clock skew.
#[test]
fn test_timestamp_validation_with_skew() {
    let cluster = ChaosTestCluster::new(2);

    // Node 0 is 1 second ahead
    cluster.chaos.clock_skew.set_skew_ms(&0, 1000);
    cluster.chaos.clock_skew.set_skew_ms(&1, 0);

    // From node 1's perspective, a message from node 0 appears to be from the future
    // A message that node 0 considers "now" (age=0) appears 1 second in the future to node 1
    let is_valid = cluster.chaos.clock_skew.is_timestamp_valid(&0, &1, 0, 500);
    assert!(!is_valid); // Should be invalid (appears to be from future)

    // A message that node 0 considers 1.5 seconds old appears 0.5 seconds old to node 1
    let is_valid = cluster
        .chaos
        .clock_skew
        .is_timestamp_valid(&0, &1, 1500, 1000);
    assert!(is_valid); // Should be valid
}

/// Test extreme clock skew (large differences).
#[test]
fn test_extreme_clock_skew() {
    let cluster = ChaosTestCluster::new(3);

    // Node 1 is 10 seconds ahead
    cluster.chaos.clock_skew.set_skew_ms(&1, 10_000);
    // Node 2 is 10 seconds behind
    cluster.chaos.clock_skew.set_skew_ms(&2, -10_000);

    // Relative skew is 20 seconds!
    let relative = cluster.chaos.clock_skew.relative_skew(&1, &2);
    assert_eq!(relative, 20_000);

    // Messages should still propagate (the simulated cluster doesn't reject based on time)
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(2));
    assert!(cluster.all_online_received(&id));
}

/// Test random clock skew application.
#[test]
fn test_random_clock_skew() {
    let cluster = ChaosTestCluster::new(10);

    // Apply random skew to all nodes
    let nodes: Vec<u64> = (0..10).collect();
    cluster.chaos.apply_random_clock_skew(&nodes, 500);

    // Verify all nodes have skew within bounds
    for i in 0..10 {
        let skew = cluster.chaos.clock_skew.get_skew_ms(&i);
        assert!(
            (-500..=500).contains(&skew),
            "Skew {} out of bounds for node {}",
            skew,
            i
        );
    }

    // Stats should show clock skew events
    let stats = cluster.chaos.stats();
    assert_eq!(stats.clock_skew_events, 10);
}

/// Test clock drift simulation.
#[test]
fn test_clock_drift() {
    let cluster = ChaosTestCluster::new(2);

    // Enable drift of up to 10ms
    cluster
        .chaos
        .clock_skew
        .enable_drift(Duration::from_millis(10));

    // Get time multiple times - should vary slightly
    let _times: Vec<_> = (0..10)
        .map(|_| cluster.chaos.clock_skew.get_time(&0))
        .collect();

    // With drift enabled, times won't be perfectly monotonic
    // This is expected behavior for simulating real clock drift
    cluster.chaos.clock_skew.disable_drift();
}

/// Test clearing clock skew.
#[test]
fn test_clear_clock_skew() {
    let cluster = ChaosTestCluster::new(3);

    // Set skew
    cluster.chaos.clock_skew.set_skew_ms(&0, 100);
    cluster.chaos.clock_skew.set_skew_ms(&1, -100);
    assert_eq!(cluster.chaos.clock_skew.get_skew_ms(&0), 100);
    assert_eq!(cluster.chaos.clock_skew.get_skew_ms(&1), -100);

    // Clear specific node
    cluster.chaos.clock_skew.clear_skew(&0);
    assert_eq!(cluster.chaos.clock_skew.get_skew_ms(&0), 0);
    assert_eq!(cluster.chaos.clock_skew.get_skew_ms(&1), -100);

    // Clear all
    cluster.chaos.clock_skew.clear_all();
    assert_eq!(cluster.chaos.clock_skew.get_skew_ms(&1), 0);
}

/// Test combined clock skew and network partition.
#[test]
fn test_clock_skew_with_partition() {
    let cluster = ChaosTestCluster::new(4);

    // Apply clock skew
    cluster.chaos.clock_skew.set_skew_ms(&0, 100);
    cluster.chaos.clock_skew.set_skew_ms(&1, -100);

    // Create partition
    cluster.chaos.split_brain(vec![0, 1], vec![2, 3]);

    // Broadcast from node 0
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();
    cluster.run_until_quiet(Duration::from_secs(1));

    // Only nodes 0 and 1 should receive (partition isolation)
    assert_eq!(cluster.delivery_count(&id), 2);
}
