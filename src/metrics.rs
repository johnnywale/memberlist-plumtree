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
