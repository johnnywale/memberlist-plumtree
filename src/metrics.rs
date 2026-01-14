//! Metrics for Plumtree protocol.
//!
//! Provides counters and gauges for monitoring protocol performance.

use metrics::{counter, describe_counter, describe_gauge, gauge};

/// Initialize metric descriptions.
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
        "plumtree_graft_retries_total",
        "Total number of Graft retry attempts"
    );
    describe_counter!(
        "plumtree_graft_timeouts_total",
        "Total number of Graft requests that timed out after all retries"
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
        "plumtree_rate_limited_total",
        "Total number of rate-limited Graft requests"
    );

    // Gauges
    describe_gauge!("plumtree_eager_peers", "Current number of eager peers");
    describe_gauge!("plumtree_lazy_peers", "Current number of lazy peers");
    describe_gauge!("plumtree_cache_size", "Current number of messages in cache");
    describe_gauge!(
        "plumtree_ihave_queue_size",
        "Current number of pending IHave announcements"
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

/// Record a Graft retry attempt.
pub fn record_graft_retry() {
    counter!("plumtree_graft_retries_total").increment(1);
}

/// Record a Graft timeout (all retries exhausted).
pub fn record_graft_timeout() {
    counter!("plumtree_graft_timeouts_total").increment(1);
}

/// Record a rate-limited Graft request.
pub fn record_rate_limited() {
    counter!("plumtree_rate_limited_total").increment(1);
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
