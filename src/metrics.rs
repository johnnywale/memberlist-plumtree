//! Metrics for Plumtree protocol.
//!
//! Provides counters, gauges, and histograms for monitoring protocol performance.
//!
//! ## Available Metrics
//!
//! ### Counters
//! - `plumtree_messages_broadcast_total` - Total broadcasts initiated
//! - `plumtree_messages_delivered_total` - Total messages delivered to application
//! - `plumtree_messages_duplicate_total` - Total duplicate messages received
//! - `plumtree_gossip_sent_total` - Total Gossip messages sent
//! - `plumtree_ihave_sent_total` - Total IHave messages sent
//! - `plumtree_graft_sent_total` - Total Graft messages sent
//! - `plumtree_prune_sent_total` - Total Prune messages sent
//! - `plumtree_peer_promotions_total` - Total peers promoted to eager
//! - `plumtree_peer_demotions_total` - Total peers demoted to lazy
//! - `plumtree_peer_added_total` - Total peers added to the cluster
//! - `plumtree_peer_removed_total` - Total peers removed from the cluster
//! - `plumtree_graft_success_total` - Successful Graft requests
//! - `plumtree_graft_failed_total` - Failed Graft requests (max retries exceeded)
//! - `plumtree_graft_retries_total` - Total Graft retry attempts
//!
//! ### Histograms
//! - `plumtree_graft_latency_seconds` - Latency from Graft request to message delivery
//! - `plumtree_message_size_bytes` - Message payload size distribution
//! - `plumtree_message_hops` - Number of hops (rounds) messages travel
//! - `plumtree_ihave_batch_size` - Size of IHave batches sent
//!
//! ### Gauges
//! - `plumtree_eager_peers` - Current number of eager peers
//! - `plumtree_lazy_peers` - Current number of lazy peers
//! - `plumtree_total_peers` - Current total number of peers
//! - `plumtree_cache_size` - Current number of messages in cache
//! - `plumtree_seen_map_size` - Current number of entries in the deduplication map
//! - `plumtree_ihave_queue_size` - Current number of pending IHave announcements
//! - `plumtree_pending_grafts` - Current number of pending Graft requests
//! - `plumtree_seen_map_evictions_total` - Total emergency evictions from seen map
//! - `plumtree_compression_bytes_in_total` - Total bytes before compression
//! - `plumtree_compression_bytes_saved_total` - Total bytes saved by compression
//! - `plumtree_compression_messages_total` - Total messages that were compressed
//! - `plumtree_compression_skipped_total` - Total messages skipped (too small or no benefit)
//! - `plumtree_priority_critical` - Messages with critical priority
//! - `plumtree_priority_high` - Messages with high priority
//! - `plumtree_priority_normal` - Messages with normal priority
//! - `plumtree_priority_low` - Messages with low priority
//! - `plumtree_peers_healthy` - Number of healthy peers
//! - `plumtree_peers_degraded` - Number of degraded peers
//! - `plumtree_peers_zombie` - Number of zombie peers

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

/// Initialize metric descriptions.
///
/// Call this once at application startup to register all metric descriptions.
/// This makes metrics more discoverable in monitoring systems.
pub fn init_metrics() {
    // Counters
    describe_counter!(
        "plumtree_messages_broadcast_total",
        "Total number of messages broadcast"
    );
    describe_counter!(
        "plumtree_messages_delivered_total",
        "Total number of messages delivered to application"
    );
    describe_counter!(
        "plumtree_messages_duplicate_total",
        "Total number of duplicate messages received"
    );
    describe_counter!(
        "plumtree_gossip_sent_total",
        "Total number of Gossip messages sent"
    );
    describe_counter!(
        "plumtree_ihave_sent_total",
        "Total number of IHave messages sent"
    );
    describe_counter!(
        "plumtree_graft_sent_total",
        "Total number of Graft messages sent"
    );
    describe_counter!(
        "plumtree_prune_sent_total",
        "Total number of Prune messages sent"
    );
    describe_counter!(
        "plumtree_peer_promotions_total",
        "Total number of peers promoted to eager"
    );
    describe_counter!(
        "plumtree_peer_demotions_total",
        "Total number of peers demoted to lazy"
    );
    describe_counter!(
        "plumtree_peer_added_total",
        "Total number of peers added to the cluster"
    );
    describe_counter!(
        "plumtree_peer_removed_total",
        "Total number of peers removed from the cluster"
    );
    describe_counter!(
        "plumtree_graft_success_total",
        "Total number of successful Graft requests (message received after Graft)"
    );
    describe_counter!(
        "plumtree_graft_failed_total",
        "Total number of failed Graft requests (timeout before message received)"
    );
    describe_counter!(
        "plumtree_graft_retries_total",
        "Total number of Graft retry attempts"
    );

    // Histograms
    describe_histogram!(
        "plumtree_graft_latency_seconds",
        "Latency from Graft request to message delivery in seconds"
    );
    describe_histogram!(
        "plumtree_message_size_bytes",
        "Message payload size in bytes"
    );
    describe_histogram!(
        "plumtree_message_hops",
        "Number of hops (rounds) a message has traveled"
    );
    describe_histogram!(
        "plumtree_ihave_batch_size",
        "Number of message IDs in IHave batches"
    );

    // Gauges
    describe_gauge!("plumtree_eager_peers", "Current number of eager peers");
    describe_gauge!("plumtree_lazy_peers", "Current number of lazy peers");
    describe_gauge!("plumtree_total_peers", "Current total number of peers");
    describe_gauge!("plumtree_cache_size", "Current number of messages in cache");
    describe_gauge!(
        "plumtree_seen_map_size",
        "Current number of entries in the deduplication (seen) map"
    );
    describe_gauge!(
        "plumtree_ihave_queue_size",
        "Current number of pending IHave announcements"
    );
    describe_gauge!(
        "plumtree_pending_grafts",
        "Current number of pending Graft requests"
    );
    describe_counter!(
        "plumtree_seen_map_evictions_total",
        "Total number of entries evicted from seen map due to capacity"
    );

    // Compression metrics
    describe_counter!(
        "plumtree_compression_bytes_in_total",
        "Total bytes before compression"
    );
    describe_counter!(
        "plumtree_compression_bytes_saved_total",
        "Total bytes saved by compression"
    );
    describe_counter!(
        "plumtree_compression_messages_total",
        "Total messages that were successfully compressed"
    );
    describe_counter!(
        "plumtree_compression_skipped_total",
        "Total messages skipped (too small or compression not beneficial)"
    );

    // Priority queue metrics
    describe_gauge!(
        "plumtree_priority_critical",
        "Number of messages with critical priority"
    );
    describe_gauge!(
        "plumtree_priority_high",
        "Number of messages with high priority"
    );
    describe_gauge!(
        "plumtree_priority_normal",
        "Number of messages with normal priority"
    );
    describe_gauge!(
        "plumtree_priority_low",
        "Number of messages with low priority"
    );

    // Peer health metrics
    describe_gauge!("plumtree_peers_healthy", "Number of healthy peers");
    describe_gauge!("plumtree_peers_degraded", "Number of degraded peers");
    describe_gauge!("plumtree_peers_zombie", "Number of zombie peers");
}

/// Record a message broadcast.
pub fn record_broadcast() {
    counter!("plumtree_messages_broadcast_total").increment(1);
}

/// Record a message delivery.
pub fn record_delivery() {
    counter!("plumtree_messages_delivered_total").increment(1);
}

/// Record a duplicate message.
pub fn record_duplicate() {
    counter!("plumtree_messages_duplicate_total").increment(1);
}

/// Record a Gossip message sent.
pub fn record_gossip_sent() {
    counter!("plumtree_gossip_sent_total").increment(1);
}

/// Record an IHave message sent.
pub fn record_ihave_sent() {
    counter!("plumtree_ihave_sent_total").increment(1);
}

/// Record a Graft message sent.
pub fn record_graft_sent() {
    counter!("plumtree_graft_sent_total").increment(1);
}

/// Record a Prune message sent.
pub fn record_prune_sent() {
    counter!("plumtree_prune_sent_total").increment(1);
}

/// Record a peer promotion.
pub fn record_peer_promotion() {
    counter!("plumtree_peer_promotions_total").increment(1);
}

/// Record a peer demotion.
pub fn record_peer_demotion() {
    counter!("plumtree_peer_demotions_total").increment(1);
}

/// Update eager peers gauge.
pub fn set_eager_peers(count: usize) {
    gauge!("plumtree_eager_peers").set(count as f64);
}

/// Update lazy peers gauge.
pub fn set_lazy_peers(count: usize) {
    gauge!("plumtree_lazy_peers").set(count as f64);
}

/// Update cache size gauge.
pub fn set_cache_size(count: usize) {
    gauge!("plumtree_cache_size").set(count as f64);
}

/// Update IHave queue size gauge.
pub fn set_ihave_queue_size(count: usize) {
    gauge!("plumtree_ihave_queue_size").set(count as f64);
}

/// Record a successful Graft (message received after Graft request).
pub fn record_graft_success() {
    counter!("plumtree_graft_success_total").increment(1);
}

/// Record a failed Graft (timeout before message received).
pub fn record_graft_failed() {
    counter!("plumtree_graft_failed_total").increment(1);
}

/// Record Graft latency in seconds.
pub fn record_graft_latency(latency_secs: f64) {
    histogram!("plumtree_graft_latency_seconds").record(latency_secs);
}

/// Record a Graft retry attempt.
pub fn record_graft_retry() {
    counter!("plumtree_graft_retries_total").increment(1);
}

/// Record a peer being added.
pub fn record_peer_added() {
    counter!("plumtree_peer_added_total").increment(1);
}

/// Record a peer being removed.
pub fn record_peer_removed() {
    counter!("plumtree_peer_removed_total").increment(1);
}

/// Record message size in bytes.
pub fn record_message_size(size: usize) {
    histogram!("plumtree_message_size_bytes").record(size as f64);
}

/// Record the number of hops a message has traveled.
pub fn record_message_hops(hops: u32) {
    histogram!("plumtree_message_hops").record(hops as f64);
}

/// Record the size of an IHave batch.
pub fn record_ihave_batch_size(size: usize) {
    histogram!("plumtree_ihave_batch_size").record(size as f64);
}

/// Update total peers gauge.
pub fn set_total_peers(count: usize) {
    gauge!("plumtree_total_peers").set(count as f64);
}

/// Update pending grafts gauge.
pub fn set_pending_grafts(count: usize) {
    gauge!("plumtree_pending_grafts").set(count as f64);
}

/// Update seen map size gauge.
pub fn set_seen_map_size(count: usize) {
    gauge!("plumtree_seen_map_size").set(count as f64);
}

/// Record emergency evictions from the seen map.
pub fn record_seen_map_evictions(count: usize) {
    counter!("plumtree_seen_map_evictions_total").increment(count as u64);
}

/// Record a compression operation.
///
/// # Arguments
///
/// * `original_size` - Size of the original data in bytes
/// * `compressed_size` - Size of the compressed data in bytes
///
/// If `compressed_size < original_size`, records as successful compression.
/// Otherwise, records as skipped.
pub fn record_compression(original_size: usize, compressed_size: usize) {
    counter!("plumtree_compression_bytes_in_total").increment(original_size as u64);

    if compressed_size < original_size {
        let saved = original_size - compressed_size;
        counter!("plumtree_compression_bytes_saved_total").increment(saved as u64);
        counter!("plumtree_compression_messages_total").increment(1);
    } else {
        counter!("plumtree_compression_skipped_total").increment(1);
    }
}

/// Record a compression skip (payload below threshold).
pub fn record_compression_skipped() {
    counter!("plumtree_compression_skipped_total").increment(1);
}

/// Set priority queue metrics.
///
/// # Arguments
///
/// * `critical` - Number of messages with critical priority
/// * `high` - Number of messages with high priority
/// * `normal` - Number of messages with normal priority
/// * `low` - Number of messages with low priority
pub fn set_priority_queue(critical: usize, high: usize, normal: usize, low: usize) {
    gauge!("plumtree_priority_critical").set(critical as f64);
    gauge!("plumtree_priority_high").set(high as f64);
    gauge!("plumtree_priority_normal").set(normal as f64);
    gauge!("plumtree_priority_low").set(low as f64);
}

/// Set peer health metrics.
///
/// # Arguments
///
/// * `healthy` - Number of healthy peers
/// * `degraded` - Number of degraded peers
/// * `zombie` - Number of zombie peers
pub fn set_peer_health(healthy: usize, degraded: usize, zombie: usize) {
    gauge!("plumtree_peers_healthy").set(healthy as f64);
    gauge!("plumtree_peers_degraded").set(degraded as f64);
    gauge!("plumtree_peers_zombie").set(zombie as f64);
}

/// Increment the healthy peers gauge by 1.
pub fn inc_peers_healthy() {
    gauge!("plumtree_peers_healthy").increment(1.0);
}

/// Decrement the healthy peers gauge by 1.
pub fn dec_peers_healthy() {
    gauge!("plumtree_peers_healthy").decrement(1.0);
}

/// Increment the degraded peers gauge by 1.
pub fn inc_peers_degraded() {
    gauge!("plumtree_peers_degraded").increment(1.0);
}

/// Decrement the degraded peers gauge by 1.
pub fn dec_peers_degraded() {
    gauge!("plumtree_peers_degraded").decrement(1.0);
}

/// Increment the zombie peers gauge by 1.
pub fn inc_peers_zombie() {
    gauge!("plumtree_peers_zombie").increment(1.0);
}

/// Decrement the zombie peers gauge by 1.
pub fn dec_peers_zombie() {
    gauge!("plumtree_peers_zombie").decrement(1.0);
}

// ============================================================================
// Node-scoped metrics (for multi-node deployments)
// ============================================================================

/// Node-scoped metrics recorder.
///
/// Use this for multi-node deployments (e.g., simulations, tests) where each node
/// should have its own metrics distinguished by a `node` label.
///
/// # Example
///
/// ```rust,ignore
/// use memberlist_plumtree::metrics::NodeMetrics;
///
/// let metrics = NodeMetrics::new("node_1");
/// metrics.record_broadcast();
/// metrics.set_eager_peers(3);
///
/// // Prometheus output:
/// // plumtree_messages_broadcast_total{node="node_1"} 1
/// // plumtree_eager_peers{node="node_1"} 3
/// ```
#[derive(Clone, Debug)]
pub struct NodeMetrics {
    node_id: String,
}

impl NodeMetrics {
    /// Create a new node-scoped metrics recorder.
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    // --- Counters ---

    /// Record a message broadcast.
    pub fn record_broadcast(&self) {
        counter!("plumtree_messages_broadcast_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a message delivery.
    pub fn record_delivery(&self) {
        counter!("plumtree_messages_delivered_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a duplicate message.
    pub fn record_duplicate(&self) {
        counter!("plumtree_messages_duplicate_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a Gossip message sent.
    pub fn record_gossip_sent(&self) {
        counter!("plumtree_gossip_sent_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record an IHave message sent.
    pub fn record_ihave_sent(&self) {
        counter!("plumtree_ihave_sent_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a Graft message sent.
    pub fn record_graft_sent(&self) {
        counter!("plumtree_graft_sent_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a Prune message sent.
    pub fn record_prune_sent(&self) {
        counter!("plumtree_prune_sent_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a peer promotion.
    pub fn record_peer_promotion(&self) {
        counter!("plumtree_peer_promotions_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a peer demotion.
    pub fn record_peer_demotion(&self) {
        counter!("plumtree_peer_demotions_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a successful Graft.
    pub fn record_graft_success(&self) {
        counter!("plumtree_graft_success_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a failed Graft.
    pub fn record_graft_failed(&self) {
        counter!("plumtree_graft_failed_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a Graft retry attempt.
    pub fn record_graft_retry(&self) {
        counter!("plumtree_graft_retries_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a peer being added.
    pub fn record_peer_added(&self) {
        counter!("plumtree_peer_added_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record a peer being removed.
    pub fn record_peer_removed(&self) {
        counter!("plumtree_peer_removed_total", "node" => self.node_id.clone()).increment(1);
    }

    /// Record emergency evictions from the seen map.
    pub fn record_seen_map_evictions(&self, count: usize) {
        counter!("plumtree_seen_map_evictions_total", "node" => self.node_id.clone())
            .increment(count as u64);
    }

    // --- Histograms ---

    /// Record Graft latency in seconds.
    pub fn record_graft_latency(&self, latency_secs: f64) {
        histogram!("plumtree_graft_latency_seconds", "node" => self.node_id.clone())
            .record(latency_secs);
    }

    /// Record message size in bytes.
    pub fn record_message_size(&self, size: usize) {
        histogram!("plumtree_message_size_bytes", "node" => self.node_id.clone())
            .record(size as f64);
    }

    /// Record the number of hops a message has traveled.
    pub fn record_message_hops(&self, hops: u32) {
        histogram!("plumtree_message_hops", "node" => self.node_id.clone()).record(hops as f64);
    }

    /// Record the size of an IHave batch.
    pub fn record_ihave_batch_size(&self, size: usize) {
        histogram!("plumtree_ihave_batch_size", "node" => self.node_id.clone()).record(size as f64);
    }

    // --- Gauges ---

    /// Update eager peers gauge.
    pub fn set_eager_peers(&self, count: usize) {
        gauge!("plumtree_eager_peers", "node" => self.node_id.clone()).set(count as f64);
    }

    /// Update lazy peers gauge.
    pub fn set_lazy_peers(&self, count: usize) {
        gauge!("plumtree_lazy_peers", "node" => self.node_id.clone()).set(count as f64);
    }

    /// Update total peers gauge.
    pub fn set_total_peers(&self, count: usize) {
        gauge!("plumtree_total_peers", "node" => self.node_id.clone()).set(count as f64);
    }

    /// Update cache size gauge.
    pub fn set_cache_size(&self, count: usize) {
        gauge!("plumtree_cache_size", "node" => self.node_id.clone()).set(count as f64);
    }

    /// Update seen map size gauge.
    pub fn set_seen_map_size(&self, count: usize) {
        gauge!("plumtree_seen_map_size", "node" => self.node_id.clone()).set(count as f64);
    }

    /// Update IHave queue size gauge.
    pub fn set_ihave_queue_size(&self, count: usize) {
        gauge!("plumtree_ihave_queue_size", "node" => self.node_id.clone()).set(count as f64);
    }

    /// Update pending grafts gauge.
    pub fn set_pending_grafts(&self, count: usize) {
        gauge!("plumtree_pending_grafts", "node" => self.node_id.clone()).set(count as f64);
    }

    // --- Compression metrics ---

    /// Record a compression operation.
    ///
    /// If `compressed_size < original_size`, records as successful compression.
    /// Otherwise, records as skipped.
    pub fn record_compression(&self, original_size: usize, compressed_size: usize) {
        counter!("plumtree_compression_bytes_in_total", "node" => self.node_id.clone())
            .increment(original_size as u64);

        if compressed_size < original_size {
            let saved = original_size - compressed_size;
            counter!("plumtree_compression_bytes_saved_total", "node" => self.node_id.clone())
                .increment(saved as u64);
            counter!("plumtree_compression_messages_total", "node" => self.node_id.clone())
                .increment(1);
        } else {
            counter!("plumtree_compression_skipped_total", "node" => self.node_id.clone())
                .increment(1);
        }
    }

    /// Record a compression skip (payload below threshold).
    pub fn record_compression_skipped(&self) {
        counter!("plumtree_compression_skipped_total", "node" => self.node_id.clone()).increment(1);
    }

    // --- Priority queue metrics ---

    /// Set priority queue metrics.
    pub fn set_priority_queue(&self, critical: usize, high: usize, normal: usize, low: usize) {
        gauge!("plumtree_priority_critical", "node" => self.node_id.clone()).set(critical as f64);
        gauge!("plumtree_priority_high", "node" => self.node_id.clone()).set(high as f64);
        gauge!("plumtree_priority_normal", "node" => self.node_id.clone()).set(normal as f64);
        gauge!("plumtree_priority_low", "node" => self.node_id.clone()).set(low as f64);
    }

    // --- Peer health metrics ---

    /// Set peer health metrics.
    pub fn set_peer_health(&self, healthy: usize, degraded: usize, zombie: usize) {
        gauge!("plumtree_peers_healthy", "node" => self.node_id.clone()).set(healthy as f64);
        gauge!("plumtree_peers_degraded", "node" => self.node_id.clone()).set(degraded as f64);
        gauge!("plumtree_peers_zombie", "node" => self.node_id.clone()).set(zombie as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_metrics_creation() {
        let metrics = NodeMetrics::new("test_node_1");
        assert_eq!(metrics.node_id(), "test_node_1");

        let metrics2 = NodeMetrics::new(String::from("test_node_2"));
        assert_eq!(metrics2.node_id(), "test_node_2");
    }

    #[test]
    fn test_node_metrics_clone() {
        let metrics = NodeMetrics::new("cloneable");
        let cloned = metrics.clone();
        assert_eq!(metrics.node_id(), cloned.node_id());
    }

    #[test]
    fn test_init_metrics_idempotent() {
        // Should not panic when called multiple times
        init_metrics();
        init_metrics();
        init_metrics();
    }

    #[test]
    fn test_global_metrics_do_not_panic() {
        // These should not panic even without a recorder installed
        record_broadcast();
        record_delivery();
        record_duplicate();
        record_gossip_sent();
        record_ihave_sent();
        record_graft_sent();
        record_prune_sent();
        record_peer_promotion();
        record_peer_demotion();
        record_graft_success();
        record_graft_failed();
        record_graft_retry();
        record_peer_added();
        record_peer_removed();
        record_seen_map_evictions(5);
        record_graft_latency(0.5);
        record_message_size(1024);
        record_message_hops(3);
        record_ihave_batch_size(16);
        set_eager_peers(3);
        set_lazy_peers(6);
        set_total_peers(9);
        set_cache_size(100);
        set_seen_map_size(50);
        set_ihave_queue_size(10);
        set_pending_grafts(2);
    }

    #[test]
    fn test_node_metrics_do_not_panic() {
        let metrics = NodeMetrics::new("test");

        // All methods should work without a recorder
        metrics.record_broadcast();
        metrics.record_delivery();
        metrics.record_duplicate();
        metrics.record_gossip_sent();
        metrics.record_ihave_sent();
        metrics.record_graft_sent();
        metrics.record_prune_sent();
        metrics.record_peer_promotion();
        metrics.record_peer_demotion();
        metrics.record_graft_success();
        metrics.record_graft_failed();
        metrics.record_graft_retry();
        metrics.record_peer_added();
        metrics.record_peer_removed();
        metrics.record_seen_map_evictions(5);
        metrics.record_graft_latency(0.5);
        metrics.record_message_size(1024);
        metrics.record_message_hops(3);
        metrics.record_ihave_batch_size(16);
        metrics.set_eager_peers(3);
        metrics.set_lazy_peers(6);
        metrics.set_total_peers(9);
        metrics.set_cache_size(100);
        metrics.set_seen_map_size(50);
        metrics.set_ihave_queue_size(10);
        metrics.set_pending_grafts(2);
    }

    #[test]
    fn test_global_compression_metrics_do_not_panic() {
        // Compression metrics should not panic without a recorder
        record_compression(1000, 500); // Successful compression
        record_compression(100, 150); // No benefit (skipped)
        record_compression_skipped(); // Below threshold
    }

    #[test]
    fn test_node_compression_metrics_do_not_panic() {
        let metrics = NodeMetrics::new("test");

        // Compression metrics should not panic
        metrics.record_compression(1000, 500); // Successful compression
        metrics.record_compression(100, 150); // No benefit (skipped)
        metrics.record_compression_skipped(); // Below threshold
    }
}
