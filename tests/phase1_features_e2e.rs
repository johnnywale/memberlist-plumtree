//! End-to-end tests for Phase 1 features.
//!
//! These tests verify the new Phase 1 features work correctly in realistic scenarios:
//! - Message Compression
//! - Message Priority Queues

mod common;

#[allow(unused_imports)]
use common::allocate_ports;

#[cfg(feature = "compression")]
use memberlist_plumtree::CompressionStats;
use memberlist_plumtree::{
    CompressionAlgorithm, CompressionConfig, MessagePriority, PriorityConfig, PriorityQueue,
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
