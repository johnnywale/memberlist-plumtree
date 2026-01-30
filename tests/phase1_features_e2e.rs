//! End-to-end tests for Phase 1 features.
//!
//! These tests verify the new Phase 1 features work correctly in realistic scenarios:
//! - Message Compression
//! - Message Priority Queues
//! - Enhanced Peer Health Monitoring

mod common;

use std::sync::Arc;
use std::time::Duration;

#[allow(unused_imports)]
use common::allocate_ports;

#[cfg(feature = "compression")]
use memberlist_plumtree::CompressionStats;
use memberlist_plumtree::{
    CompressionAlgorithm, CompressionConfig, MessagePriority, PeerHealthConfig, PeerHealthTracker,
    PeerStatus, PriorityConfig, PriorityQueue, ZombieAction,
};

// =============================================================================
// COMPRESSION TESTS
// =============================================================================

#[test]
fn test_compression_config_presets() {
    // Test that all compression presets work correctly
    let zstd = CompressionConfig::zstd(3);
    assert!(zstd.enabled);
    assert!(matches!(
        zstd.algorithm,
        CompressionAlgorithm::Zstd { level: 3 }
    ));

    let gzip = CompressionConfig::gzip(6);
    assert!(gzip.enabled);
    assert!(matches!(
        gzip.algorithm,
        CompressionAlgorithm::Gzip { level: 6 }
    ));

    let default = CompressionConfig::default();
    assert!(!default.enabled);
}

#[test]
fn test_compression_threshold_filtering() {
    let config = CompressionConfig::zstd(3).with_min_size(100);

    // Small payloads should not be compressed
    assert!(!config.should_compress(50));
    assert!(!config.should_compress(99));

    // Payloads at or above threshold should be compressed
    assert!(config.should_compress(100));
    assert!(config.should_compress(1000));
}

#[cfg(feature = "compression")]
#[test]
fn test_compression_real_payloads() {
    use memberlist_plumtree::{compress, decompress};

    // Test with JSON-like payload (common in real applications)
    // Using decompress to verify roundtrip
    let json_payload: Vec<u8> = (0..100)
        .map(|i| format!(r#"{{"id":{},"name":"user{}","active":true}}"#, i, i))
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes();

    let compressed = compress(&json_payload, CompressionAlgorithm::Zstd { level: 3 }).unwrap();

    // JSON compresses well due to repetitive structure
    let ratio = compressed.len() as f64 / json_payload.len() as f64;
    assert!(
        ratio < 0.5,
        "JSON should compress to <50% of original size, got {:.1}%",
        ratio * 100.0
    );

    // Verify roundtrip
    let decompressed = decompress(&compressed, CompressionAlgorithm::Zstd { level: 3 }).unwrap();
    assert_eq!(json_payload, decompressed.as_ref());
}

#[cfg(feature = "compression")]
#[test]
fn test_compression_stats_tracking() {
    use memberlist_plumtree::compress;

    let mut stats = CompressionStats::default();

    // Compress some payloads
    let payload1: Vec<u8> = vec![0u8; 1000]; // Highly compressible
    let compressed1 = compress(&payload1, CompressionAlgorithm::Zstd { level: 3 }).unwrap();
    stats.record(payload1.len(), compressed1.len());

    let payload2: Vec<u8> = (0..1000).map(|i| i as u8).collect(); // Moderately compressible
    let compressed2 = compress(&payload2, CompressionAlgorithm::Zstd { level: 3 }).unwrap();
    stats.record(payload2.len(), compressed2.len());

    // Verify stats
    assert_eq!(stats.bytes_in, 2000);
    assert!(stats.bytes_out < stats.bytes_in);
    assert!(stats.bytes_saved() > 0);
    assert!(stats.ratio() < 1.0);
}

// =============================================================================
// PRIORITY QUEUE TESTS
// =============================================================================

#[test]
fn test_priority_queue_ordering() {
    let config = PriorityConfig::default();
    let mut queue: PriorityQueue<String> = PriorityQueue::new(config);

    // Add messages with different priorities (in reverse order)
    queue.push("low".to_string(), MessagePriority::Low).unwrap();
    queue
        .push("normal".to_string(), MessagePriority::Normal)
        .unwrap();
    queue
        .push("high".to_string(), MessagePriority::High)
        .unwrap();
    queue
        .push("critical".to_string(), MessagePriority::Critical)
        .unwrap();

    // Should dequeue in priority order (highest first)
    assert_eq!(queue.pop().unwrap().0, "critical");
    assert_eq!(queue.pop().unwrap().0, "high");
    assert_eq!(queue.pop().unwrap().0, "normal");
    assert_eq!(queue.pop().unwrap().0, "low");
}

#[test]
fn test_priority_queue_weighted_batch() {
    let config = PriorityConfig {
        enabled: true,
        queue_depths: [100, 100, 100, 100],
        weights: [4, 2, 1, 1], // Critical: 50%, High: 25%, Normal: 12.5%, Low: 12.5%
    };
    let mut queue: PriorityQueue<usize> = PriorityQueue::new(config);

    // Add 20 messages to each priority level
    for i in 0..20 {
        queue.push(i, MessagePriority::Critical).unwrap();
        queue.push(i + 100, MessagePriority::High).unwrap();
        queue.push(i + 200, MessagePriority::Normal).unwrap();
        queue.push(i + 300, MessagePriority::Low).unwrap();
    }

    // Dequeue a batch of 16
    let batch = queue.pop_batch(16);
    assert_eq!(batch.len(), 16);

    // Count by priority
    let critical_count = batch
        .iter()
        .filter(|(_, p)| *p == MessagePriority::Critical)
        .count();
    let high_count = batch
        .iter()
        .filter(|(_, p)| *p == MessagePriority::High)
        .count();
    let normal_count = batch
        .iter()
        .filter(|(_, p)| *p == MessagePriority::Normal)
        .count();
    let low_count = batch
        .iter()
        .filter(|(_, p)| *p == MessagePriority::Low)
        .count();

    // Critical should have the most (roughly 50%)
    assert!(
        critical_count >= high_count,
        "Critical ({}) should >= High ({})",
        critical_count,
        high_count
    );
    assert!(
        high_count >= normal_count,
        "High ({}) should >= Normal ({})",
        high_count,
        normal_count
    );

    println!(
        "Batch distribution: Critical={}, High={}, Normal={}, Low={}",
        critical_count, high_count, normal_count, low_count
    );
}

#[test]
fn test_priority_queue_backpressure() {
    let config = PriorityConfig {
        enabled: true,
        queue_depths: [2, 2, 2, 2], // Very small queues
        weights: [1, 1, 1, 1],
    };
    let mut queue: PriorityQueue<usize> = PriorityQueue::new(config);

    // Fill the high priority queue
    assert!(queue.push(1, MessagePriority::High).is_ok());
    assert!(queue.push(2, MessagePriority::High).is_ok());

    // Third message should be rejected (queue full)
    assert!(queue.push(3, MessagePriority::High).is_err());

    // But other priorities still have room
    assert!(queue.push(4, MessagePriority::Low).is_ok());

    // Check dropped count
    assert_eq!(queue.stats().dropped(MessagePriority::High), 1);
    assert_eq!(queue.stats().dropped(MessagePriority::Low), 0);
}

#[test]
fn test_priority_queue_starvation_prevention() {
    // Even with aggressive weighting, lower priorities should eventually get served
    let config = PriorityConfig {
        enabled: true,
        queue_depths: [100, 100, 100, 100],
        weights: [100, 1, 1, 1], // Heavily favor critical
    };
    let mut queue: PriorityQueue<usize> = PriorityQueue::new(config);

    // Add messages to all priorities
    for i in 0..10 {
        queue.push(i, MessagePriority::Critical).unwrap();
        queue.push(i, MessagePriority::High).unwrap();
        queue.push(i, MessagePriority::Normal).unwrap();
        queue.push(i, MessagePriority::Low).unwrap();
    }

    // Drain the queue
    let mut counts = [0usize; 4];
    while let Some((_, priority)) = queue.pop() {
        counts[priority.index()] += 1;
    }

    // All messages should eventually be delivered
    assert_eq!(counts.iter().sum::<usize>(), 40);
    for (i, &count) in counts.iter().enumerate() {
        assert_eq!(
            count,
            10,
            "Priority {:?} should have 10 messages, got {}",
            MessagePriority::from_index(i),
            count
        );
    }
}

// =============================================================================
// PEER HEALTH MONITORING TESTS
// =============================================================================

#[test]
fn test_peer_health_rtt_tracking() {
    let config = PeerHealthConfig {
        adaptive_timeout: true,
        rtt_multiplier: 3.0,
        ema_alpha: 0.5,
        ..Default::default()
    };
    let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

    // Record RTT samples
    tracker.record_success(&1, Duration::from_millis(100));
    tracker.record_success(&1, Duration::from_millis(120));
    tracker.record_success(&1, Duration::from_millis(80));

    // Timeout should be roughly 3x the EMA of RTT
    let timeout = tracker.timeout(&1);
    assert!(
        timeout.as_millis() >= 200 && timeout.as_millis() <= 400,
        "Timeout should be ~300ms (3x ~100ms RTT), got {:?}",
        timeout
    );
}

#[test]
fn test_peer_health_zombie_detection() {
    let config = PeerHealthConfig {
        zombie_threshold: 3,
        zombie_action: ZombieAction::Demote,
        ..Default::default()
    };
    let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

    // Record failures
    assert_eq!(tracker.status(&1), PeerStatus::Healthy); // No data = healthy

    tracker.record_failure(&1);
    assert_eq!(tracker.status(&1), PeerStatus::Degraded);

    tracker.record_failure(&1);
    assert_eq!(tracker.status(&1), PeerStatus::Degraded);

    // Third failure crosses threshold
    let action = tracker.record_failure(&1);
    assert_eq!(action, Some(ZombieAction::Demote));
    assert_eq!(tracker.status(&1), PeerStatus::Zombie);
    assert!(tracker.is_zombie(&1));

    // Zombies list should contain this peer
    let zombies = tracker.zombies();
    assert!(zombies.contains(&1));
}

#[test]
fn test_peer_health_recovery() {
    let config = PeerHealthConfig {
        zombie_threshold: 3,
        ..Default::default()
    };
    let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

    // Make peer a zombie
    for _ in 0..3 {
        tracker.record_failure(&1);
    }
    assert!(tracker.is_zombie(&1));

    // Single success should recover the peer
    tracker.record_success(&1, Duration::from_millis(50));
    assert_eq!(tracker.status(&1), PeerStatus::Healthy);
    assert!(!tracker.is_zombie(&1));
}

#[test]
fn test_peer_health_summary() {
    let config = PeerHealthConfig {
        zombie_threshold: 3,
        ..Default::default()
    };
    let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

    // Create peers in different states
    tracker.record_success(&1, Duration::from_millis(10)); // Healthy
    tracker.record_success(&2, Duration::from_millis(20)); // Healthy
    tracker.record_failure(&3); // Degraded
    for _ in 0..3 {
        tracker.record_failure(&4); // Zombie
    }

    let summary = tracker.summary();
    assert_eq!(summary.total_peers, 4);
    assert_eq!(summary.healthy, 2);
    assert_eq!(summary.degraded, 1);
    assert_eq!(summary.zombie, 1);
    assert_eq!(summary.total_successes, 2);
    assert_eq!(summary.total_failures, 4);
}

#[test]
fn test_peer_health_adaptive_timeout_bounds() {
    let config = PeerHealthConfig {
        adaptive_timeout: true,
        rtt_multiplier: 3.0,
        min_timeout: Duration::from_millis(100),
        max_timeout: Duration::from_secs(5),
        ema_alpha: 1.0, // Use exact values
        ..Default::default()
    };
    let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

    // Very fast peer - should clamp to min
    tracker.record_success(&1, Duration::from_micros(100));
    let timeout1 = tracker.timeout(&1);
    assert!(
        timeout1 >= Duration::from_millis(100),
        "Should be at least min_timeout"
    );

    // Very slow peer - should clamp to max
    tracker.record_success(&2, Duration::from_secs(10));
    let timeout2 = tracker.timeout(&2);
    assert!(
        timeout2 <= Duration::from_secs(5),
        "Should be at most max_timeout"
    );
}

#[test]
fn test_peer_health_concurrent_tracking() {
    use std::thread;

    let config = PeerHealthConfig::default();
    let tracker = Arc::new(PeerHealthTracker::<u64>::new(config));

    // Spawn multiple threads recording health data
    let mut handles = vec![];

    for peer_id in 0..10 {
        let tracker = Arc::clone(&tracker);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                tracker.record_success(&peer_id, Duration::from_millis(10));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All peers should be tracked
    let summary = tracker.summary();
    assert_eq!(summary.total_peers, 10);
    assert_eq!(summary.total_successes, 1000); // 10 peers * 100 successes
    assert_eq!(summary.healthy, 10);
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

#[test]
fn test_priority_with_message_types() {
    // Simulate how Plumtree message types would be prioritized
    #[derive(Debug, Clone)]
    enum PlumtreeMessageType {
        Gossip,
        IHave,
        Graft,
        Prune,
    }

    fn get_priority(msg_type: &PlumtreeMessageType) -> MessagePriority {
        match msg_type {
            PlumtreeMessageType::Graft => MessagePriority::Critical, // Tree repair
            PlumtreeMessageType::Gossip => MessagePriority::High,    // Payload delivery
            PlumtreeMessageType::IHave => MessagePriority::Normal,   // Announcements
            PlumtreeMessageType::Prune => MessagePriority::Low,      // Optimization
        }
    }

    let config = PriorityConfig::default();
    let mut queue: PriorityQueue<PlumtreeMessageType> = PriorityQueue::new(config);

    // Add messages in random order
    queue
        .push(
            PlumtreeMessageType::Prune,
            get_priority(&PlumtreeMessageType::Prune),
        )
        .unwrap();
    queue
        .push(
            PlumtreeMessageType::IHave,
            get_priority(&PlumtreeMessageType::IHave),
        )
        .unwrap();
    queue
        .push(
            PlumtreeMessageType::Gossip,
            get_priority(&PlumtreeMessageType::Gossip),
        )
        .unwrap();
    queue
        .push(
            PlumtreeMessageType::Graft,
            get_priority(&PlumtreeMessageType::Graft),
        )
        .unwrap();

    // Graft (tree repair) should be processed first
    let (msg, _) = queue.pop().unwrap();
    assert!(matches!(msg, PlumtreeMessageType::Graft));

    // Then Gossip (payload delivery)
    let (msg, _) = queue.pop().unwrap();
    assert!(matches!(msg, PlumtreeMessageType::Gossip));

    // Then IHave (announcements)
    let (msg, _) = queue.pop().unwrap();
    assert!(matches!(msg, PlumtreeMessageType::IHave));

    // Finally Prune (optimization)
    let (msg, _) = queue.pop().unwrap();
    assert!(matches!(msg, PlumtreeMessageType::Prune));
}

#[test]
fn test_health_based_timeout_selection() {
    // Simulate selecting timeout based on peer health for Graft retries
    let config = PeerHealthConfig {
        adaptive_timeout: true,
        rtt_multiplier: 2.0,
        min_timeout: Duration::from_millis(50),
        max_timeout: Duration::from_secs(10),
        ..Default::default()
    };
    let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

    // Fast peer (good network)
    tracker.record_success(&1, Duration::from_millis(10));
    tracker.record_success(&1, Duration::from_millis(12));
    tracker.record_success(&1, Duration::from_millis(8));

    // Slow peer (bad network or far away)
    tracker.record_success(&2, Duration::from_millis(200));
    tracker.record_success(&2, Duration::from_millis(250));
    tracker.record_success(&2, Duration::from_millis(180));

    let timeout1 = tracker.timeout(&1);
    let timeout2 = tracker.timeout(&2);

    // Slow peer should have longer timeout
    assert!(
        timeout2 > timeout1,
        "Slow peer timeout {:?} should be > fast peer timeout {:?}",
        timeout2,
        timeout1
    );

    // Both should be within bounds
    assert!(timeout1 >= Duration::from_millis(50));
    assert!(timeout2 <= Duration::from_secs(10));
}
