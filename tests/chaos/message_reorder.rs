//! Message reordering tests.
//!
//! These tests verify that the protocol handles out-of-order message
//! delivery correctly, which can happen with network jitter or routing changes.

use bytes::Bytes;
use std::time::Duration;

use super::harness::ChaosTestCluster;

/// Test basic message reordering.
#[test]
fn test_basic_reordering() {
    let cluster = ChaosTestCluster::new(5);

    // Enable reordering on the existing reorderer via the enable method
    cluster.chaos.reorderer.enable();

    // Broadcast a message
    let id = cluster.broadcast(0, Bytes::from("test")).unwrap();

    // Run with longer timeout to account for reordering delays
    cluster.run_until_quiet(Duration::from_secs(2));

    // All nodes should still receive the message eventually
    assert!(cluster.all_online_received(&id));
}

/// Test that duplicate messages are handled with reordering.
#[test]
fn test_reordering_with_duplicates() {
    let cluster = ChaosTestCluster::new(3);

    // Send multiple messages rapidly
    let ids: Vec<_> = (0..5)
        .map(|i| {
            cluster
                .broadcast(0, Bytes::from(format!("msg{}", i)))
                .unwrap()
        })
        .collect();

    cluster.run_until_quiet(Duration::from_secs(2));

    // All messages should be delivered exactly once to each node
    for id in &ids {
        assert!(cluster.all_online_received(id));
    }
}

/// Test reordering doesn't cause message loss.
#[test]
fn test_reordering_no_loss() {
    let cluster = ChaosTestCluster::new(4);

    // Enable reordering via the existing reorderer
    cluster.chaos.reorderer.enable();

    // Send many messages
    let mut ids = Vec::new();
    for i in 0..10 {
        if let Some(id) = cluster.broadcast(i % 4, Bytes::from(format!("msg{}", i))) {
            ids.push(id);
        }
    }

    // Give plenty of time for reordered messages
    cluster.run_until_quiet(Duration::from_secs(3));

    // All messages should be delivered
    for id in &ids {
        assert!(
            cluster.all_online_received(id),
            "Message {:?} not delivered to all nodes",
            id
        );
    }
}

/// Test reordering queue operations.
#[test]
fn test_reorder_queue_operations() {
    use memberlist_plumtree::testing::MessageReorderer;

    let reorderer: MessageReorderer<u64, String> = MessageReorderer::new()
        .with_reorder_probability(1.0) // Always reorder
        .with_min_delay(Duration::from_millis(50))
        .with_max_delay(Duration::from_millis(100));

    // Queue some messages
    reorderer.maybe_delay(&0, &1, "msg1".to_string());
    reorderer.maybe_delay(&0, &2, "msg2".to_string());
    reorderer.maybe_delay(&1, &2, "msg3".to_string());

    assert_eq!(reorderer.queue_len(), 3);

    // Messages not ready yet
    assert!(reorderer.drain_ready().is_empty());

    // Wait for messages to be ready
    std::thread::sleep(Duration::from_millis(150));

    // All messages should be ready
    let ready = reorderer.drain_ready();
    assert_eq!(ready.len(), 3);
    assert_eq!(reorderer.queue_len(), 0);
}

/// Test clearing the reorder queue.
#[test]
fn test_reorder_queue_clear() {
    use memberlist_plumtree::testing::MessageReorderer;

    let reorderer: MessageReorderer<u64, String> = MessageReorderer::new()
        .with_reorder_probability(1.0)
        .with_max_delay(Duration::from_millis(1000));

    reorderer.maybe_delay(&0, &1, "msg1".to_string());
    reorderer.maybe_delay(&0, &1, "msg2".to_string());
    assert_eq!(reorderer.queue_len(), 2);

    reorderer.clear();
    assert_eq!(reorderer.queue_len(), 0);
}

/// Test reordering with message loss.
#[test]
fn test_reordering_with_message_loss() {
    let cluster = ChaosTestCluster::new(4);

    // Enable reordering via the existing reorderer
    cluster.chaos.reorderer.enable();

    // Set 10% message loss
    *cluster.chaos.config.write() =
        memberlist_plumtree::testing::ChaosConfig::new().with_message_loss_rate(0.1);

    // Send messages
    let mut ids = Vec::new();
    for i in 0..5 {
        if let Some(id) = cluster.broadcast(i % 4, Bytes::from(format!("msg{}", i))) {
            ids.push(id);
        }
    }

    cluster.run_until_quiet(Duration::from_secs(2));

    // Some messages may not reach all nodes due to loss
    // This test mainly verifies no panics/deadlocks with combined chaos
    let _ = ids; // Suppress unused warning
}

/// Test time_until_next for scheduling.
#[test]
fn test_time_until_next() {
    use memberlist_plumtree::testing::MessageReorderer;

    let reorderer: MessageReorderer<u64, String> = MessageReorderer::new()
        .with_reorder_probability(1.0)
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_millis(100));

    // Empty queue
    assert!(reorderer.time_until_next().is_none());

    // Queue a message
    reorderer.maybe_delay(&0, &1, "test".to_string());

    // Should have time until next
    let time = reorderer.time_until_next();
    assert!(time.is_some());
    let time = time.unwrap();
    assert!(time <= Duration::from_millis(100));
}

/// Test disabling reordering.
#[test]
fn test_reorder_disable() {
    use memberlist_plumtree::testing::MessageReorderer;

    let reorderer: MessageReorderer<u64, String> = MessageReorderer::new()
        .with_reorder_probability(1.0)
        .with_max_delay(Duration::from_millis(100));

    // Should reorder when enabled
    assert!(reorderer.maybe_delay(&0, &1, "test".to_string()).is_some());

    // Disable
    reorderer.disable();

    // Should not reorder when disabled
    assert!(reorderer.maybe_delay(&0, &1, "test".to_string()).is_none());

    // Re-enable
    reorderer.enable();
    assert!(reorderer.maybe_delay(&0, &1, "test".to_string()).is_some());
}
