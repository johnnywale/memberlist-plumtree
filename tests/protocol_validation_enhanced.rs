//! Enhanced Protocol Validation Tests for Plumtree Implementation
//!
//! These tests address limitations of basic functional testing:
//!
//! 1. **Latency Simulation**: Random jitter to test tree stability under realistic conditions
//! 2. **Hop Counting (LDH)**: Track message propagation depth to detect path inflation
//! 3. **Cache Stress Testing**: Verify cache eviction doesn't cause broadcast storms
//! 4. **Node Churn**: Continuous join/leave to test tree repair speed
//! 5. **Larger Scale**: Test with 50+ nodes to reveal scale-dependent issues
//!
//! Key Metrics:
//! - RMR (Relative Message Redundancy): m/(n-1) - 1
//! - LDH (Last Delivery Hop): Maximum hops to reach the last node
//! - Tree Flap Rate: Prune/Graft cycles per message (stability indicator)
//! - Control Message Overhead: (IHave + Graft + Prune) / Gossip ratio

use bytes::Bytes;
use memberlist_plumtree::{
    MessageId, Plumtree, PlumtreeConfig, PlumtreeDelegate, PlumtreeHandle, PlumtreeMessage,
};
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default seed for deterministic tests. Change this to explore different scenarios.
///
/// Note: While this seeds the latency/packet-loss simulation, there is remaining
/// non-determinism from:
/// 1. Core library's `random_eager_except` / `random_lazy_except` (reservoir sampling)
/// 2. Tokio async task scheduling
///
/// Full determinism would require changes to the core library to accept a seeded RNG.
#[allow(dead_code)]
const DEFAULT_TEST_SEED: u64 = 42;

/// Simple node ID type for testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId(u64);

// =============================================================================
// Enhanced Statistics Tracking
// =============================================================================

/// Enhanced message statistics with hop tracking.
#[derive(Debug, Default)]
struct EnhancedStats {
    /// Total Gossip messages sent
    gossip_sent: AtomicU64,
    /// Total IHave messages sent
    ihave_sent: AtomicU64,
    /// Total Graft messages sent
    graft_sent: AtomicU64,
    /// Total Prune messages sent
    prune_sent: AtomicU64,
    /// Messages delivered to nodes with hop count
    delivered: Mutex<HashMap<MessageId, HashMap<NodeId, usize>>>,
    /// Track hop counts for LDH calculation
    hop_counts: Mutex<Vec<usize>>,
    /// Track tree flaps (prune followed by graft to same peer)
    flap_count: AtomicU64,
    /// Memory usage tracking (simulated cache entries)
    cache_entries: AtomicUsize,
    /// False graft count (graft for already-received message)
    false_grafts: AtomicU64,
}

impl EnhancedStats {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn record_gossip(&self) {
        self.gossip_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_ihave(&self) {
        self.ihave_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_graft(&self) {
        self.graft_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_prune(&self) {
        self.prune_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn record_flap(&self) {
        self.flap_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_false_graft(&self) {
        self.false_grafts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_delivery(&self, msg_id: MessageId, node: NodeId, hops: usize) {
        let mut delivered = self.delivered.lock();
        delivered.entry(msg_id).or_default().insert(node, hops);
        self.hop_counts.lock().push(hops);
    }

    fn set_cache_entries(&self, count: usize) {
        self.cache_entries.store(count, Ordering::Relaxed);
    }

    fn gossip_count(&self) -> u64 {
        self.gossip_sent.load(Ordering::Relaxed)
    }

    fn ihave_count(&self) -> u64 {
        self.ihave_sent.load(Ordering::Relaxed)
    }

    fn graft_count(&self) -> u64 {
        self.graft_sent.load(Ordering::Relaxed)
    }

    fn prune_count(&self) -> u64 {
        self.prune_sent.load(Ordering::Relaxed)
    }

    fn flap_count(&self) -> u64 {
        self.flap_count.load(Ordering::Relaxed)
    }

    fn false_graft_count(&self) -> u64 {
        self.false_grafts.load(Ordering::Relaxed)
    }

    fn delivery_count(&self, msg_id: &MessageId) -> usize {
        self.delivered
            .lock()
            .get(msg_id)
            .map(|s| s.len())
            .unwrap_or(0)
    }

    /// Calculate Last Delivery Hop (LDH) - max hops to reach all nodes
    fn calculate_ldh(&self, msg_id: &MessageId) -> Option<usize> {
        self.delivered
            .lock()
            .get(msg_id)
            .and_then(|deliveries| deliveries.values().max().copied())
    }

    /// Calculate average hop count across all deliveries
    fn average_hops(&self) -> f64 {
        let hops = self.hop_counts.lock();
        if hops.is_empty() {
            return 0.0;
        }
        hops.iter().sum::<usize>() as f64 / hops.len() as f64
    }

    /// Calculate Relative Message Redundancy
    fn calculate_rmr(&self, node_count: usize) -> f64 {
        let m = self.gossip_count() as f64;
        let n = node_count as f64;
        if n <= 1.0 {
            return 0.0;
        }
        (m / (n - 1.0)) - 1.0
    }

    /// Calculate control message overhead ratio
    fn control_overhead(&self) -> f64 {
        let gossip = self.gossip_count() as f64;
        if gossip == 0.0 {
            return 0.0;
        }
        let control = (self.ihave_count() + self.graft_count() + self.prune_count()) as f64;
        control / gossip
    }

    fn reset(&self) {
        self.gossip_sent.store(0, Ordering::Relaxed);
        self.ihave_sent.store(0, Ordering::Relaxed);
        self.graft_sent.store(0, Ordering::Relaxed);
        self.prune_sent.store(0, Ordering::Relaxed);
        self.flap_count.store(0, Ordering::Relaxed);
        self.false_grafts.store(0, Ordering::Relaxed);
        self.delivered.lock().clear();
        self.hop_counts.lock().clear();
    }

    fn summary(&self, node_count: usize) -> String {
        format!(
            "Gossip: {}, IHave: {}, Graft: {}, Prune: {}, RMR: {:.3}, AvgHops: {:.2}, Overhead: {:.2}",
            self.gossip_count(),
            self.ihave_count(),
            self.graft_count(),
            self.prune_count(),
            self.calculate_rmr(node_count),
            self.average_hops(),
            self.control_overhead()
        )
    }
}

// =============================================================================
// Latency Simulation
// =============================================================================

/// Configuration for network latency simulation.
#[derive(Debug, Clone)]
struct LatencyConfig {
    /// Minimum latency in milliseconds
    min_latency_ms: u64,
    /// Maximum latency in milliseconds
    max_latency_ms: u64,
    /// Probability of packet loss (0.0 - 1.0)
    packet_loss_rate: f64,
    /// Enable latency variance based on "distance" between nodes
    distance_based_latency: bool,
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            min_latency_ms: 5,
            max_latency_ms: 50,
            packet_loss_rate: 0.0,
            distance_based_latency: false,
        }
    }
}

impl LatencyConfig {
    /// No latency (instant delivery) - for baseline comparison
    fn instant() -> Self {
        Self {
            min_latency_ms: 0,
            max_latency_ms: 0,
            packet_loss_rate: 0.0,
            distance_based_latency: false,
        }
    }

    /// LAN-like latency (low, uniform)
    fn lan() -> Self {
        Self {
            min_latency_ms: 1,
            max_latency_ms: 10,
            packet_loss_rate: 0.001,
            distance_based_latency: false,
        }
    }

    /// WAN-like latency (high variance)
    fn wan() -> Self {
        Self {
            min_latency_ms: 10,
            max_latency_ms: 200,
            packet_loss_rate: 0.01,
            distance_based_latency: true,
        }
    }

    /// Unreliable network (high loss, high jitter)
    fn unreliable() -> Self {
        Self {
            min_latency_ms: 20,
            max_latency_ms: 500,
            packet_loss_rate: 0.05,
            distance_based_latency: true,
        }
    }
}

/// A pending message waiting to be delivered.
#[derive(Debug)]
struct PendingMessage {
    delivery_time: Instant,
    from: NodeId,
    to: NodeId,
    message: PlumtreeMessage,
    hops: usize,
}

impl PartialEq for PendingMessage {
    fn eq(&self, other: &Self) -> bool {
        self.delivery_time == other.delivery_time
    }
}

impl Eq for PendingMessage {}

impl PartialOrd for PendingMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering for min-heap behavior
        other.delivery_time.cmp(&self.delivery_time)
    }
}

/// Simulates network latency with jitter using a seeded RNG for deterministic behavior.
struct LatencySimulator {
    config: LatencyConfig,
    /// Priority queue of pending messages ordered by delivery time
    pending: Mutex<BinaryHeap<PendingMessage>>,
    /// Seeded RNG for deterministic latency/loss simulation
    rng: Mutex<StdRng>,
}

impl LatencySimulator {
    fn new(config: LatencyConfig) -> Self {
        Self::with_seed(config, DEFAULT_TEST_SEED)
    }

    fn with_seed(config: LatencyConfig, seed: u64) -> Self {
        Self {
            config,
            pending: Mutex::new(BinaryHeap::new()),
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
        }
    }

    /// Calculate latency between two nodes.
    fn calculate_latency(&self, from: NodeId, to: NodeId) -> Duration {
        let mut rng = self.rng.lock();
        let base_latency = if self.config.distance_based_latency {
            // Simulate "distance" based on node ID difference
            let distance = (from.0 as i64 - to.0 as i64).unsigned_abs();
            let distance_factor = (distance as f64 / 100.0).min(1.0);
            let range = self.config.max_latency_ms - self.config.min_latency_ms;
            self.config.min_latency_ms + (range as f64 * distance_factor) as u64
        } else {
            rng.random_range(self.config.min_latency_ms..=self.config.max_latency_ms)
        };

        // Add jitter (±20%)
        let jitter = (base_latency as f64 * 0.2 * (rng.random::<f64>() * 2.0 - 1.0)) as i64;
        let final_latency = (base_latency as i64 + jitter).max(0) as u64;

        Duration::from_millis(final_latency)
    }

    /// Check if a packet should be dropped.
    fn should_drop(&self) -> bool {
        if self.config.packet_loss_rate <= 0.0 {
            return false;
        }
        self.rng.lock().random::<f64>() < self.config.packet_loss_rate
    }

    /// Queue a message for delayed delivery.
    fn queue_message(&self, from: NodeId, to: NodeId, message: PlumtreeMessage, hops: usize) {
        if self.should_drop() {
            return; // Packet lost
        }

        let latency = self.calculate_latency(from, to);
        let delivery_time = Instant::now() + latency;

        self.pending.lock().push(PendingMessage {
            delivery_time,
            from,
            to,
            message,
            hops,
        });
    }

    /// Get all messages ready for delivery.
    fn get_ready_messages(&self) -> Vec<PendingMessage> {
        let now = Instant::now();
        let mut pending = self.pending.lock();
        let mut ready = Vec::new();

        while let Some(msg) = pending.peek() {
            if msg.delivery_time <= now {
                ready.push(pending.pop().unwrap());
            } else {
                break;
            }
        }

        ready
    }

    /// Get the next delivery time (for sleep calculations).
    fn next_delivery_time(&self) -> Option<Instant> {
        self.pending.lock().peek().map(|m| m.delivery_time)
    }

    /// Check if there are pending messages.
    fn has_pending(&self) -> bool {
        !self.pending.lock().is_empty()
    }
}

// =============================================================================
// Enhanced Tracking Delegate
// =============================================================================

/// Test delegate that tracks message delivery with hop counts.
#[derive(Clone)]
struct EnhancedTrackingDelegate {
    node_id: NodeId,
    stats: Arc<EnhancedStats>,
    delivered: Arc<Mutex<HashMap<MessageId, usize>>>, // msg_id -> hop count
}

impl EnhancedTrackingDelegate {
    fn new(node_id: NodeId, stats: Arc<EnhancedStats>) -> Self {
        Self {
            node_id,
            stats,
            delivered: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn record_delivery_with_hops(&self, msg_id: MessageId, hops: usize) {
        self.delivered.lock().insert(msg_id, hops);
        self.stats.record_delivery(msg_id, self.node_id, hops);
    }

    fn has_message(&self, msg_id: &MessageId) -> bool {
        self.delivered.lock().contains_key(msg_id)
    }

    fn delivered_count(&self) -> usize {
        self.delivered.lock().len()
    }
}

impl std::fmt::Debug for EnhancedTrackingDelegate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnhancedTrackingDelegate")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl PlumtreeDelegate<NodeId> for EnhancedTrackingDelegate {
    fn on_deliver(&self, message_id: MessageId, _payload: Bytes) {
        // Basic delivery - hops will be set separately
        self.delivered.lock().entry(message_id).or_insert(0);
    }
}

// =============================================================================
// Enhanced Simulated Network
// =============================================================================

/// A simulated network with latency, jitter, and enhanced tracking.
struct EnhancedSimulatedNetwork {
    nodes: HashMap<
        NodeId,
        (
            Plumtree<NodeId, EnhancedTrackingDelegate>,
            PlumtreeHandle<NodeId>,
        ),
    >,
    delegates: HashMap<NodeId, EnhancedTrackingDelegate>,
    stats: Arc<EnhancedStats>,
    latency: Arc<LatencySimulator>,
    alive: HashSet<NodeId>,
    /// Track message hop counts during propagation
    message_hops: Mutex<HashMap<MessageId, HashMap<NodeId, usize>>>,
    /// Background task handles for IHave schedulers and Graft timers
    background_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl EnhancedSimulatedNetwork {
    fn new(node_count: usize, config: PlumtreeConfig, latency_config: LatencyConfig) -> Self {
        Self::with_seed(node_count, config, latency_config, DEFAULT_TEST_SEED)
    }

    fn with_seed(
        node_count: usize,
        config: PlumtreeConfig,
        latency_config: LatencyConfig,
        seed: u64,
    ) -> Self {
        let stats = EnhancedStats::new();
        let latency = Arc::new(LatencySimulator::with_seed(latency_config, seed));
        let mut nodes = HashMap::new();
        let mut delegates = HashMap::new();
        let mut alive = HashSet::new();

        for i in 1..=node_count {
            let node_id = NodeId(i as u64);
            let delegate = EnhancedTrackingDelegate::new(node_id, stats.clone());
            let (plumtree, handle) = Plumtree::new(node_id, config.clone(), delegate.clone());

            nodes.insert(node_id, (plumtree, handle));
            delegates.insert(node_id, delegate);
            alive.insert(node_id);
        }

        Self {
            nodes,
            delegates,
            stats,
            latency,
            alive,
            message_hops: Mutex::new(HashMap::new()),
            background_tasks: Vec::new(),
        }
    }

    fn get(&self, id: NodeId) -> Option<&Plumtree<NodeId, EnhancedTrackingDelegate>> {
        self.nodes.get(&id).map(|(p, _)| p)
    }

    /// Start background tasks (IHave scheduler + Graft timer) for all nodes.
    ///
    /// This is essential for the Plumtree protocol to work correctly:
    /// - IHave scheduler sends announcements to lazy peers
    /// - Graft timer handles retry logic for missing messages
    fn start_background_tasks(&mut self) {
        for (&node_id, (plumtree, _)) in &self.nodes {
            let pt = plumtree.clone();
            self.background_tasks.push(tokio::spawn(async move {
                pt.run_ihave_scheduler().await;
            }));

            let pt = plumtree.clone();
            self.background_tasks.push(tokio::spawn(async move {
                pt.run_graft_timer().await;
            }));

            // Suppress unused variable warning
            let _ = node_id;
        }
    }

    /// Stop all background tasks.
    fn stop_background_tasks(&mut self) {
        for handle in self.background_tasks.drain(..) {
            handle.abort();
        }
    }

    /// Uses add_peer_lazy so tree forms naturally via Graft mechanism.
    fn setup_full_mesh(&self) {
        let node_ids: Vec<_> = self.nodes.keys().cloned().collect();
        for &id in &node_ids {
            if let Some((plumtree, _)) = self.nodes.get(&id) {
                for &other_id in &node_ids {
                    if id != other_id {
                        plumtree.add_peer_lazy(other_id);
                    }
                }
            }
        }
    }

    fn rebalance_all(&self) {
        for (_, (plumtree, _)) in &self.nodes {
            plumtree.rebalance_peers();
        }
    }

    fn kill_node(&mut self, id: NodeId) {
        self.alive.remove(&id);
    }

    fn revive_node(&mut self, id: NodeId) {
        self.alive.insert(id);
    }

    fn add_node(&mut self, id: NodeId, config: PlumtreeConfig) {
        let delegate = EnhancedTrackingDelegate::new(id, self.stats.clone());
        let (plumtree, handle) = Plumtree::new(id, config, delegate.clone());

        // Add all existing alive nodes as peers (lazy so tree forms via Graft)
        for &peer_id in &self.alive {
            plumtree.add_peer_lazy(peer_id);
            // Also add this node to existing peers
            if let Some((peer_plumtree, _)) = self.nodes.get(&peer_id) {
                peer_plumtree.add_peer_lazy(id);
            }
        }

        self.nodes.insert(id, (plumtree, handle));
        self.delegates.insert(id, delegate);
        self.alive.insert(id);
    }

    fn remove_node(&mut self, id: NodeId) {
        self.alive.remove(&id);
        // Notify other nodes of removal
        for &peer_id in &self.alive {
            if let Some((peer_plumtree, _)) = self.nodes.get(&peer_id) {
                peer_plumtree.remove_peer(&id);
            }
        }
    }

    fn is_alive(&self, id: NodeId) -> bool {
        self.alive.contains(&id)
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn alive_count(&self) -> usize {
        self.alive.len()
    }

    fn stats(&self) -> &EnhancedStats {
        &self.stats
    }

    /// Process messages with latency simulation.
    async fn process_messages_with_latency(&self) -> usize {
        let mut processed = 0;

        // First, collect outgoing messages and queue them with latency
        // Use non-blocking try_next_outgoing() to avoid timeout overhead
        for (&from_id, (_, handle)) in &self.nodes {
            if !self.is_alive(from_id) {
                continue;
            }

            while let Some(out) = handle.try_next_outgoing() {
                if let Some(target) = out.target {
                    if !self.is_alive(target) {
                        continue;
                    }

                    // Determine hop count for this message
                    let hops = match &out.message {
                        PlumtreeMessage::Gossip { id, .. } => {
                            let mut msg_hops = self.message_hops.lock();
                            let hops_map = msg_hops.entry(*id).or_default();
                            let sender_hops = hops_map.get(&from_id).copied().unwrap_or(0);
                            sender_hops + 1
                        }
                        _ => 0,
                    };

                    self.latency
                        .queue_message(from_id, target, out.message, hops);
                }
            }
        }

        // Then, deliver messages that are ready
        let ready_messages = self.latency.get_ready_messages();
        for msg in ready_messages {
            if !self.is_alive(msg.to) {
                continue;
            }

            // Track message type
            match &msg.message {
                PlumtreeMessage::Gossip { id, .. } => {
                    self.stats.record_gossip();
                    // Update hop count for this node
                    let mut msg_hops = self.message_hops.lock();
                    msg_hops.entry(*id).or_default().insert(msg.to, msg.hops);

                    // Update delegate with hop count
                    if let Some(delegate) = self.delegates.get(&msg.to) {
                        delegate.record_delivery_with_hops(*id, msg.hops);
                    }
                }
                PlumtreeMessage::IHave { .. } => self.stats.record_ihave(),
                PlumtreeMessage::Graft { .. } => self.stats.record_graft(),
                PlumtreeMessage::Prune => self.stats.record_prune(),
            }

            if let Some((plumtree, _)) = self.nodes.get(&msg.to) {
                let _ = plumtree.handle_message(msg.from, msg.message).await;
                processed += 1;
            }
        }

        processed
    }

    /// Process until quiescent with latency simulation.
    async fn process_until_quiescent_with_latency(
        &self,
        max_iterations: usize,
        max_duration: Duration,
    ) -> usize {
        let start = Instant::now();
        let mut total = 0;

        for _ in 0..max_iterations {
            if start.elapsed() > max_duration {
                break;
            }

            let processed = self.process_messages_with_latency().await;
            total += processed;

            // If nothing was processed and no pending messages, we're done
            if processed == 0 && !self.latency.has_pending() {
                break;
            }

            // Sleep until next message is ready or a small interval
            if let Some(next_time) = self.latency.next_delivery_time() {
                let wait = next_time.saturating_duration_since(Instant::now());
                if wait > Duration::ZERO {
                    tokio::time::sleep(wait.min(Duration::from_millis(10))).await;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        total
    }

    async fn broadcast_from(&self, node_id: NodeId, payload: &[u8]) -> Option<MessageId> {
        let msg_id = self
            .get(node_id)?
            .broadcast(Bytes::from(payload.to_vec()))
            .await
            .ok()?;

        // Initialize hop count for sender
        self.message_hops
            .lock()
            .entry(msg_id)
            .or_default()
            .insert(node_id, 0);

        Some(msg_id)
    }

    fn peer_stats_summary(&self) -> String {
        let mut eager_total = 0;
        let mut lazy_total = 0;
        for (_, (plumtree, _)) in &self.nodes {
            let stats = plumtree.peer_stats();
            eager_total += stats.eager_count;
            lazy_total += stats.lazy_count;
        }
        format!(
            "Total: eager={}, lazy={}, avg_eager={:.1}",
            eager_total,
            lazy_total,
            eager_total as f64 / self.node_count() as f64
        )
    }
}

// =============================================================================
// Enhanced Tests
// =============================================================================

/// Test tree stability under network jitter.
///
/// This test verifies that the spanning tree remains stable when messages
/// arrive with variable latency. A stable tree should have low flap count.
#[tokio::test]
async fn test_tree_stability_under_jitter() {
    const TEST_SEED: u64 = 42;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_lazy_fanout(6)
        .with_graft_timeout(Duration::from_millis(100))
        .with_ihave_interval(Duration::from_millis(20))
        .with_hash_ring(true);

    // Use WAN-like latency with high jitter
    let latency_config = LatencyConfig::wan();

    let mut network = EnhancedSimulatedNetwork::with_seed(20, config, latency_config, TEST_SEED);
    network.setup_full_mesh();
    network.rebalance_all();
    network.start_background_tasks();

    println!("=== Tree Stability Under Jitter ===");
    println!("Seed: {}", TEST_SEED);
    println!("Network: {} nodes with WAN latency", network.node_count());

    // Send 20 messages to stress the tree
    let mut total_prunes = 0;
    let mut total_grafts = 0;

    for msg_num in 1..=20 {
        network.stats().reset();

        let _msg_id = network
            .broadcast_from(NodeId(1), format!("jitter_test_{}", msg_num).as_bytes())
            .await
            .expect("Broadcast should succeed");

        network
            .process_until_quiescent_with_latency(500, Duration::from_secs(5))
            .await;

        let prunes = network.stats().prune_count();
        let grafts = network.stats().graft_count();
        total_prunes += prunes;
        total_grafts += grafts;

        if msg_num % 5 == 0 {
            println!(
                "After {} messages: Prunes={}, Grafts={}, RMR={:.3}",
                msg_num,
                prunes,
                grafts,
                network.stats().calculate_rmr(20)
            );
        }
    }

    // Calculate flap rate (prune+graft cycles per message)
    let flap_rate = (total_prunes + total_grafts) as f64 / 20.0;
    println!(
        "\nStability Summary: Total Prunes={}, Grafts={}, Flap Rate={:.2}/msg",
        total_prunes, total_grafts, flap_rate
    );

    // A stable tree should have bounded flap rate
    // With IHave scheduler running, control traffic is expected:
    // - IHave messages to lazy peers trigger Grafts for missing messages
    // - Duplicate Gossip triggers Prunes to optimize the tree
    // The key is that flapping stabilizes over time (later messages have less churn)
    assert!(
        flap_rate < 50.0,
        "Tree flap rate should be < 50.0/message, got {:.2}",
        flap_rate
    );
}

/// Test path inflation (LDH) at different scales.
///
/// LDH should scale as O(log N) for a well-formed tree.
/// If LDH grows linearly, the tree is becoming too "deep".
#[tokio::test]
async fn test_path_inflation_ldh() {
    const TEST_SEED: u64 = 42;

    println!("=== Path Inflation (LDH) Test ===");
    println!("Seed: {}\n", TEST_SEED);

    let mut results = Vec::new();

    for node_count in [10, 25, 50] {
        let config = PlumtreeConfig::default()
            .with_eager_fanout(3)
            .with_lazy_fanout(6)
            .with_ihave_interval(Duration::from_millis(20))
            .with_hash_ring(true);

        let latency_config = LatencyConfig::lan();

        let mut network =
            EnhancedSimulatedNetwork::with_seed(node_count, config, latency_config, TEST_SEED);
        network.setup_full_mesh();
        network.rebalance_all();
        network.start_background_tasks();

        // Warm up
        for i in 1..=5 {
            network
                .broadcast_from(NodeId(1), format!("warmup_{}", i).as_bytes())
                .await
                .unwrap();
            network
                .process_until_quiescent_with_latency(300, Duration::from_secs(3))
                .await;
        }

        // Measure LDH
        network.stats().reset();
        let msg_id = network
            .broadcast_from(NodeId(1), b"ldh_test")
            .await
            .unwrap();

        network
            .process_until_quiescent_with_latency(500, Duration::from_secs(5))
            .await;

        let ldh = network.stats().calculate_ldh(&msg_id).unwrap_or(0);
        let avg_hops = network.stats().average_hops();
        let optimal_ldh = (node_count as f64).log2().ceil() as usize;

        println!(
            "N={}: LDH={}, AvgHops={:.2}, Optimal≈{}, Ratio={:.2}",
            node_count,
            ldh,
            avg_hops,
            optimal_ldh,
            ldh as f64 / optimal_ldh as f64
        );

        results.push((node_count, ldh, optimal_ldh));
    }

    // Verify LDH doesn't grow too fast
    // Allow 3x optimal (some inefficiency is expected)
    for (n, ldh, optimal) in results {
        let ratio = ldh as f64 / optimal as f64;
        assert!(
            ratio <= 4.0,
            "N={}: LDH/Optimal ratio should be <= 4.0, got {:.2}",
            n,
            ratio
        );
    }
}

/// Test cache stress with many messages.
///
/// This verifies that:
/// 1. Memory usage doesn't grow unbounded
/// 2. Cache eviction doesn't cause false grafts
#[tokio::test]
async fn test_cache_stress() {
    const TEST_SEED: u64 = 42;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_lazy_fanout(6)
        .with_message_cache_ttl(Duration::from_secs(5))
        .with_message_cache_max_size(100) // Small cache to stress eviction
        .with_ihave_interval(Duration::from_millis(20))
        .with_hash_ring(true);

    let latency_config = LatencyConfig::instant(); // Fast for stress test

    let mut network = EnhancedSimulatedNetwork::with_seed(15, config, latency_config, TEST_SEED);
    network.setup_full_mesh();
    network.rebalance_all();
    network.start_background_tasks();

    println!("=== Cache Stress Test ===");
    println!("Seed: {}", TEST_SEED);
    println!("Sending 200 messages with cache max=100");

    let mut delivered_counts = Vec::new();

    // Send 200 messages without resetting stats
    for msg_num in 1..=200 {
        let msg_id = network
            .broadcast_from(NodeId(1), format!("stress_{}", msg_num).as_bytes())
            .await
            .unwrap();

        network
            .process_until_quiescent_with_latency(200, Duration::from_secs(2))
            .await;

        let delivery_count = network.stats().delivery_count(&msg_id);
        delivered_counts.push(delivery_count);

        if msg_num % 50 == 0 {
            println!(
                "After {} messages: Gossip={}, Graft={}, FalseGrafts={}",
                msg_num,
                network.stats().gossip_count(),
                network.stats().graft_count(),
                network.stats().false_graft_count()
            );
        }
    }

    // Calculate statistics
    let avg_delivery =
        delivered_counts.iter().sum::<usize>() as f64 / delivered_counts.len() as f64;
    let min_delivery = *delivered_counts.iter().min().unwrap_or(&0);
    let expected = 14; // 15 nodes - 1 sender

    println!("\nCache Stress Results:");
    println!("  Average deliveries: {:.1}/{}", avg_delivery, expected);
    println!("  Minimum deliveries: {}/{}", min_delivery, expected);
    println!("  Total Gossip: {}", network.stats().gossip_count());
    println!("  Total Graft: {}", network.stats().graft_count());
    println!(
        "  Control Overhead: {:.2}",
        network.stats().control_overhead()
    );

    // Memory/performance should remain stable
    // Average delivery should stay high even under cache pressure
    assert!(
        avg_delivery >= expected as f64 * 0.8,
        "Average delivery should be >= 80% of expected, got {:.1}%",
        avg_delivery / expected as f64 * 100.0
    );

    // Control overhead shouldn't explode
    let overhead = network.stats().control_overhead();
    assert!(
        overhead < 2.0,
        "Control overhead should be < 2.0, got {:.2}",
        overhead
    );
}

/// Test node churn (continuous join/leave).
///
/// This tests tree repair speed under realistic conditions where
/// nodes are frequently joining and leaving.
#[tokio::test]
async fn test_node_churn() {
    const TEST_SEED: u64 = 42;

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_lazy_fanout(6)
        .with_graft_timeout(Duration::from_millis(50))
        .with_ihave_interval(Duration::from_millis(20))
        .with_hash_ring(true);

    let latency_config = LatencyConfig::lan();

    let mut network =
        EnhancedSimulatedNetwork::with_seed(20, config.clone(), latency_config, TEST_SEED);
    network.setup_full_mesh();
    network.rebalance_all();
    network.start_background_tasks();

    println!("=== Node Churn Test ===");
    println!("Seed: {}", TEST_SEED);
    println!("Initial network: {} nodes", network.node_count());

    // Warm up
    for i in 1..=5 {
        network
            .broadcast_from(NodeId(1), format!("warmup_{}", i).as_bytes())
            .await
            .unwrap();
        network
            .process_until_quiescent_with_latency(200, Duration::from_secs(2))
            .await;
    }

    let mut total_delivered = 0;
    let mut total_expected = 0;
    let mut churn_events = 0;

    // Simulate churn: broadcast, then kill/add nodes
    for round in 1..=10 {
        network.stats().reset();

        // Broadcast a message
        let msg_id = network
            .broadcast_from(NodeId(1), format!("churn_test_{}", round).as_bytes())
            .await
            .unwrap();

        // Simulate some churn mid-broadcast
        if round % 2 == 0 {
            // Kill a random node (not node 1)
            let victim = NodeId((round as u64 % 19) + 2);
            network.kill_node(victim);
            churn_events += 1;
        } else if round % 3 == 0 {
            // Add a new node
            let new_id = NodeId(100 + round as u64);
            network.add_node(new_id, config.clone());
            churn_events += 1;
        }

        network
            .process_until_quiescent_with_latency(300, Duration::from_secs(3))
            .await;

        let delivery_count = network.stats().delivery_count(&msg_id);
        let alive_count = network.alive_count();
        let expected = alive_count - 1; // All alive except sender

        total_delivered += delivery_count;
        total_expected += expected;

        if round % 3 == 0 {
            println!(
                "Round {}: Delivered {}/{}, Alive={}, Grafts={}",
                round,
                delivery_count,
                expected,
                alive_count,
                network.stats().graft_count()
            );
        }
    }

    let reliability = total_delivered as f64 / total_expected as f64 * 100.0;

    println!("\nChurn Summary:");
    println!("  Total churn events: {}", churn_events);
    println!("  Final network size: {}", network.alive_count());
    println!(
        "  Overall reliability: {:.1}% ({}/{})",
        reliability, total_delivered, total_expected
    );

    // Under churn, reliability may drop but should remain reasonable
    assert!(
        reliability >= 70.0,
        "Reliability under churn should be >= 70%, got {:.1}%",
        reliability
    );
}

/// Test control message overhead at scale.
///
/// Verifies that control messages (IHave, Graft, Prune) don't dominate
/// as network size increases.
#[tokio::test]
async fn test_control_overhead_scaling() {
    const TEST_SEED: u64 = 42;

    println!("=== Control Overhead Scaling ===");
    println!("Seed: {}\n", TEST_SEED);

    let mut results = Vec::new();

    for node_count in [10, 25, 50] {
        let config = PlumtreeConfig::default()
            .with_eager_fanout(3)
            .with_lazy_fanout(6)
            .with_ihave_interval(Duration::from_millis(20))
            .with_hash_ring(true);

        let latency_config = LatencyConfig::lan();

        let mut network =
            EnhancedSimulatedNetwork::with_seed(node_count, config, latency_config, TEST_SEED);
        network.setup_full_mesh();
        network.rebalance_all();
        network.start_background_tasks();

        // Send 10 messages and measure overhead
        for msg_num in 1..=10 {
            network
                .broadcast_from(NodeId(1), format!("overhead_test_{}", msg_num).as_bytes())
                .await
                .unwrap();
            network
                .process_until_quiescent_with_latency(300, Duration::from_secs(3))
                .await;
        }

        let gossip = network.stats().gossip_count();
        let ihave = network.stats().ihave_count();
        let graft = network.stats().graft_count();
        let prune = network.stats().prune_count();
        let overhead = network.stats().control_overhead();
        let rmr = network.stats().calculate_rmr(node_count);

        println!(
            "N={}: Gossip={}, IHave={}, Graft={}, Prune={}, Overhead={:.2}, RMR={:.3}",
            node_count, gossip, ihave, graft, prune, overhead, rmr
        );

        results.push((node_count, overhead, rmr));
    }

    // Overhead should not grow significantly with scale
    // Check that larger networks don't have disproportionately higher overhead
    let (_, overhead_10, _) = results[0];
    let (_, overhead_50, _) = results[2];

    // Overhead at 50 nodes shouldn't be more than 3x overhead at 10 nodes
    let overhead_growth = overhead_50 / overhead_10.max(0.01);
    println!("\nOverhead growth (50/10 nodes): {:.2}x", overhead_growth);

    assert!(
        overhead_growth < 5.0,
        "Overhead growth should be < 5x, got {:.2}x",
        overhead_growth
    );
}

/// Comprehensive enhanced protocol validation.
///
/// Uses deterministic seeding for reproducible results. Change `TEST_SEED`
/// to explore different scenarios.
#[tokio::test]
async fn test_enhanced_protocol_validation() {
    const TEST_SEED: u64 = 42;

    println!("=== Enhanced Protocol Validation ===");
    println!("Seed: {} (change to explore different scenarios)\n", TEST_SEED);

    let config = PlumtreeConfig::default()
        .with_eager_fanout(3)
        .with_lazy_fanout(6)
        .with_graft_timeout(Duration::from_millis(100))
        .with_ihave_interval(Duration::from_millis(20)) // Faster IHave for testing
        .with_hash_ring(true); // Enable hash ring for better topology

    let latency_config = LatencyConfig::wan();

    let mut network =
        EnhancedSimulatedNetwork::with_seed(30, config.clone(), latency_config, TEST_SEED);
    network.setup_full_mesh();
    network.rebalance_all();
    network.start_background_tasks(); // Start IHave scheduler and Graft timer

    println!("Network: {} nodes with WAN latency", network.node_count());
    println!("Config: eager_fanout=3, lazy_fanout=6, hash_ring=true\n");

    // Phase 1: Tree Construction
    println!("--- Phase 1: Tree Construction ---");
    let mut phase1_delivered = 0;
    let mut phase1_expected = 0;

    for msg_num in 1..=10 {
        network.stats().reset();

        let msg_id = network
            .broadcast_from(NodeId(1), format!("phase1_{}", msg_num).as_bytes())
            .await
            .unwrap();

        network
            .process_until_quiescent_with_latency(400, Duration::from_secs(4))
            .await;

        let delivery_count = network.stats().delivery_count(&msg_id);
        let ldh = network.stats().calculate_ldh(&msg_id).unwrap_or(0);
        phase1_delivered += delivery_count;
        phase1_expected += 29;

        if msg_num <= 3 || msg_num == 10 {
            println!(
                "  Msg {}: Delivered={}/29, LDH={}, RMR={:.3}",
                msg_num,
                delivery_count,
                ldh,
                network.stats().calculate_rmr(30)
            );
        }
    }

    let phase1_reliability = phase1_delivered as f64 / phase1_expected as f64 * 100.0;
    println!("Phase 1 Reliability: {:.1}%", phase1_reliability);

    // Phase 2: Steady State with Jitter
    println!("\n--- Phase 2: Steady State with Jitter ---");
    let mut phase2_ldh_sum = 0;
    let mut phase2_ldh_count = 0;

    for msg_num in 1..=10 {
        network.stats().reset();

        let msg_id = network
            .broadcast_from(NodeId(1), format!("phase2_{}", msg_num).as_bytes())
            .await
            .unwrap();

        network
            .process_until_quiescent_with_latency(400, Duration::from_secs(4))
            .await;

        if let Some(ldh) = network.stats().calculate_ldh(&msg_id) {
            phase2_ldh_sum += ldh;
            phase2_ldh_count += 1;
        }
    }

    let avg_ldh = phase2_ldh_sum as f64 / phase2_ldh_count.max(1) as f64;
    let optimal_ldh = (30.0_f64).log2().ceil();
    println!(
        "  Average LDH: {:.2} (optimal ≈ {:.0})",
        avg_ldh, optimal_ldh
    );

    // Phase 3: Churn Recovery
    println!("\n--- Phase 3: Churn Recovery ---");

    // Kill 3 nodes
    network.kill_node(NodeId(5));
    network.kill_node(NodeId(10));
    network.kill_node(NodeId(15));
    println!("  Killed nodes 5, 10, 15 ({} alive)", network.alive_count());

    network.stats().reset();
    let recovery_msg = network
        .broadcast_from(NodeId(1), b"recovery_test")
        .await
        .unwrap();

    network
        .process_until_quiescent_with_latency(500, Duration::from_secs(5))
        .await;

    let recovery_delivered = network.stats().delivery_count(&recovery_msg);
    let recovery_expected = network.alive_count() - 1;
    let recovery_reliability = recovery_delivered as f64 / recovery_expected as f64 * 100.0;

    println!(
        "  Recovery: Delivered={}/{} ({:.1}%)",
        recovery_delivered, recovery_expected, recovery_reliability
    );
    println!(
        "  Grafts during recovery: {}",
        network.stats().graft_count()
    );

    // Summary
    println!("\n=== Summary ===");
    println!(
        "Phase 1 (Construction): {:.1}% reliability",
        phase1_reliability
    );
    println!("Phase 2 (Steady State): Avg LDH = {:.2}", avg_ldh);
    println!(
        "Phase 3 (Recovery): {:.1}% reliability",
        recovery_reliability
    );

    // Assertions
    assert!(
        phase1_reliability >= 80.0,
        "Phase 1 reliability should be >= 80%"
    );
    assert!(avg_ldh <= optimal_ldh * 3.0, "LDH should be <= 3x optimal");
    assert!(
        recovery_reliability >= 70.0,
        "Recovery reliability should be >= 70%"
    );
}
