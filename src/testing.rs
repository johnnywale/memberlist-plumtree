//! Chaos testing utilities for Plumtree protocol.
//!
//! This module provides tools for injecting failures and testing protocol
//! resilience under adverse conditions.
//!
//! ## Features
//!
//! - **Message Loss**: Randomly drop messages with configurable probability
//! - **Network Partitions**: Simulate network splits between node groups
//! - **Latency Injection**: Add artificial delays to message delivery
//! - **Node Churn**: Simulate nodes joining and leaving the cluster
//!
//! ## Example
//!
//! ```ignore
//! use memberlist_plumtree::testing::{ChaosTransport, ChaosConfig};
//!
//! let chaos = ChaosConfig::new()
//!     .with_message_loss_rate(0.1)  // 10% message loss
//!     .with_latency(Duration::from_millis(50));
//!
//! let transport = ChaosTransport::new(base_transport, chaos);
//! ```

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use rand::Rng;

/// Configuration for chaos testing.
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of dropping a message (0.0 to 1.0).
    pub message_loss_rate: f64,

    /// Additional latency to add to all messages.
    pub base_latency: Duration,

    /// Random jitter added to latency (0 to this value).
    pub latency_jitter: Duration,

    /// Whether to enable chaos testing.
    pub enabled: bool,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            message_loss_rate: 0.0,
            base_latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            enabled: false,
        }
    }
}

impl ChaosConfig {
    /// Create a new chaos configuration with defaults (no chaos).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a chaos configuration for moderate failure testing.
    ///
    /// - 5% message loss
    /// - 10ms base latency with 20ms jitter
    pub fn moderate() -> Self {
        Self {
            message_loss_rate: 0.05,
            base_latency: Duration::from_millis(10),
            latency_jitter: Duration::from_millis(20),
            enabled: true,
        }
    }

    /// Create a chaos configuration for aggressive failure testing.
    ///
    /// - 20% message loss
    /// - 50ms base latency with 100ms jitter
    pub fn aggressive() -> Self {
        Self {
            message_loss_rate: 0.20,
            base_latency: Duration::from_millis(50),
            latency_jitter: Duration::from_millis(100),
            enabled: true,
        }
    }

    /// Set the message loss rate (0.0 to 1.0).
    pub fn with_message_loss_rate(mut self, rate: f64) -> Self {
        self.message_loss_rate = rate.clamp(0.0, 1.0);
        self.enabled = true;
        self
    }

    /// Set the base latency.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.base_latency = latency;
        self.enabled = true;
        self
    }

    /// Set the latency jitter (random additional delay).
    pub fn with_jitter(mut self, jitter: Duration) -> Self {
        self.latency_jitter = jitter;
        self.enabled = true;
        self
    }

    /// Enable or disable chaos testing.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Check if a message should be dropped based on loss rate.
    pub fn should_drop(&self) -> bool {
        if !self.enabled || self.message_loss_rate == 0.0 {
            return false;
        }
        rand::rng().random::<f64>() < self.message_loss_rate
    }

    /// Get the latency to apply to a message (base + random jitter).
    pub fn get_latency(&self) -> Duration {
        if !self.enabled {
            return Duration::ZERO;
        }
        let jitter = if self.latency_jitter > Duration::ZERO {
            let jitter_ms = rand::rng().random_range(0..=self.latency_jitter.as_millis() as u64);
            Duration::from_millis(jitter_ms)
        } else {
            Duration::ZERO
        };
        self.base_latency + jitter
    }
}

/// Network partition simulator.
///
/// Allows creating artificial network partitions between groups of nodes.
#[derive(Debug)]
pub struct NetworkPartition<I> {
    /// Pairs of nodes that cannot communicate.
    partitioned: RwLock<HashSet<(I, I)>>,
    /// Whether any partition is active.
    active: AtomicBool,
}

impl<I: Clone + Eq + Hash> NetworkPartition<I> {
    /// Create a new network partition controller.
    pub fn new() -> Self {
        Self {
            partitioned: RwLock::new(HashSet::new()),
            active: AtomicBool::new(false),
        }
    }

    /// Create a partition between two nodes (bidirectional).
    pub fn partition(&self, node_a: I, node_b: I) {
        let mut partitioned = self.partitioned.write();
        partitioned.insert((node_a.clone(), node_b.clone()));
        partitioned.insert((node_b, node_a));
        self.active.store(true, Ordering::Release);
    }

    /// Heal a partition between two nodes.
    pub fn heal(&self, node_a: &I, node_b: &I) {
        let mut partitioned = self.partitioned.write();
        partitioned.remove(&(node_a.clone(), node_b.clone()));
        partitioned.remove(&(node_b.clone(), node_a.clone()));
        if partitioned.is_empty() {
            self.active.store(false, Ordering::Release);
        }
    }

    /// Heal all partitions.
    pub fn heal_all(&self) {
        let mut partitioned = self.partitioned.write();
        partitioned.clear();
        self.active.store(false, Ordering::Release);
    }

    /// Check if two nodes are partitioned.
    pub fn is_partitioned(&self, from: &I, to: &I) -> bool {
        if !self.active.load(Ordering::Acquire) {
            return false;
        }
        let partitioned = self.partitioned.read();
        partitioned.contains(&(from.clone(), to.clone()))
    }

    /// Create a partition isolating one node from all others.
    pub fn isolate(&self, node: I, others: impl IntoIterator<Item = I>) {
        for other in others {
            self.partition(node.clone(), other);
        }
    }
}

impl<I: Clone + Eq + Hash> Default for NetworkPartition<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics collected during chaos testing.
#[derive(Debug, Default)]
pub struct ChaosStats {
    /// Total messages processed.
    pub messages_total: AtomicU64,
    /// Messages dropped due to configured loss rate.
    pub messages_dropped: AtomicU64,
    /// Messages blocked due to partition.
    pub messages_partitioned: AtomicU64,
    /// Messages delayed.
    pub messages_delayed: AtomicU64,
}

impl ChaosStats {
    /// Create new stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a message being processed.
    pub fn record_message(&self) {
        self.messages_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a message being dropped.
    pub fn record_drop(&self) {
        self.messages_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a message blocked by partition.
    pub fn record_partition_block(&self) {
        self.messages_partitioned.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a message being delayed.
    pub fn record_delay(&self) {
        self.messages_delayed.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the drop rate (dropped / total).
    pub fn drop_rate(&self) -> f64 {
        let total = self.messages_total.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.messages_dropped.load(Ordering::Relaxed) as f64 / total as f64
    }

    /// Get a snapshot of stats.
    pub fn snapshot(&self) -> ChaosStatsSnapshot {
        ChaosStatsSnapshot {
            messages_total: self.messages_total.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            messages_partitioned: self.messages_partitioned.load(Ordering::Relaxed),
            messages_delayed: self.messages_delayed.load(Ordering::Relaxed),
        }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        self.messages_total.store(0, Ordering::Relaxed);
        self.messages_dropped.store(0, Ordering::Relaxed);
        self.messages_partitioned.store(0, Ordering::Relaxed);
        self.messages_delayed.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of chaos statistics at a point in time.
#[derive(Debug, Clone)]
pub struct ChaosStatsSnapshot {
    /// Total messages processed.
    pub messages_total: u64,
    /// Messages dropped.
    pub messages_dropped: u64,
    /// Messages blocked by partition.
    pub messages_partitioned: u64,
    /// Messages delayed.
    pub messages_delayed: u64,
}

impl ChaosStatsSnapshot {
    /// Get the effective delivery rate (1.0 - drop rate - partition rate).
    pub fn delivery_rate(&self) -> f64 {
        if self.messages_total == 0 {
            return 1.0;
        }
        let failed = self.messages_dropped + self.messages_partitioned;
        1.0 - (failed as f64 / self.messages_total as f64)
    }
}

/// Controller for chaos testing a Plumtree cluster.
#[derive(Debug)]
pub struct ChaosController<I> {
    /// Chaos configuration.
    pub config: Arc<RwLock<ChaosConfig>>,
    /// Network partition controller.
    pub partition: Arc<NetworkPartition<I>>,
    /// Statistics.
    pub stats: Arc<ChaosStats>,
}

impl<I: Clone + Eq + Hash> ChaosController<I> {
    /// Create a new chaos controller with default configuration.
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(ChaosConfig::default())),
            partition: Arc::new(NetworkPartition::new()),
            stats: Arc::new(ChaosStats::new()),
        }
    }

    /// Create a chaos controller with specific configuration.
    pub fn with_config(config: ChaosConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            partition: Arc::new(NetworkPartition::new()),
            stats: Arc::new(ChaosStats::new()),
        }
    }

    /// Update the chaos configuration.
    pub fn set_config(&self, config: ChaosConfig) {
        *self.config.write() = config;
    }

    /// Enable chaos testing with moderate settings.
    pub fn enable_moderate(&self) {
        self.set_config(ChaosConfig::moderate());
    }

    /// Enable chaos testing with aggressive settings.
    pub fn enable_aggressive(&self) {
        self.set_config(ChaosConfig::aggressive());
    }

    /// Disable all chaos testing.
    pub fn disable(&self) {
        self.set_config(ChaosConfig::default());
        self.partition.heal_all();
    }

    /// Check if a message from `from` to `to` should be delivered.
    ///
    /// Returns `Some(latency)` if the message should be delivered (possibly delayed),
    /// or `None` if it should be dropped.
    pub fn should_deliver(&self, from: &I, to: &I) -> Option<Duration> {
        self.stats.record_message();

        // Check partition first
        if self.partition.is_partitioned(from, to) {
            self.stats.record_partition_block();
            return None;
        }

        let config = self.config.read();

        // Check random drop
        if config.should_drop() {
            self.stats.record_drop();
            return None;
        }

        // Get latency
        let latency = config.get_latency();
        if latency > Duration::ZERO {
            self.stats.record_delay();
        }

        Some(latency)
    }

    /// Get current statistics.
    pub fn stats(&self) -> ChaosStatsSnapshot {
        self.stats.snapshot()
    }

    /// Reset statistics.
    pub fn reset_stats(&self) {
        self.stats.reset();
    }
}

impl<I: Clone + Eq + Hash> Default for ChaosController<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Clone + Eq + Hash> Clone for ChaosController<I> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            partition: self.partition.clone(),
            stats: self.stats.clone(),
        }
    }
}

// =============================================================================
// Clock Skew Simulation
// =============================================================================

/// Simulates clock skew between nodes for testing time-dependent behavior.
///
/// This allows testing scenarios where different nodes have different notions
/// of the current time, which can affect:
/// - Message TTL expiration
/// - Graft timeout calculations
/// - Anti-entropy sync windows
/// - Rate limiting
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::testing::ClockSkewSimulator;
/// use std::time::Duration;
///
/// let simulator: ClockSkewSimulator<u64> = ClockSkewSimulator::new();
///
/// // Node 1 is 100ms ahead
/// simulator.set_skew(&1, Duration::from_millis(100));
///
/// // Node 2 is 50ms behind
/// simulator.set_skew(&2, Duration::from_millis(50).neg());
///
/// // Get "current time" from each node's perspective
/// let node1_time = simulator.get_time(&1);
/// let node2_time = simulator.get_time(&2);
/// ```
#[derive(Debug)]
pub struct ClockSkewSimulator<I> {
    /// Skew offset per node (positive = ahead, negative = behind).
    skew_per_node: RwLock<HashMap<I, i64>>,
    /// Maximum random drift to add on each time query.
    max_drift_ms: AtomicU64,
    /// Whether drift is enabled.
    drift_enabled: AtomicBool,
}

impl<I: Clone + Eq + Hash> ClockSkewSimulator<I> {
    /// Create a new clock skew simulator with no skew.
    pub fn new() -> Self {
        Self {
            skew_per_node: RwLock::new(HashMap::new()),
            max_drift_ms: AtomicU64::new(0),
            drift_enabled: AtomicBool::new(false),
        }
    }

    /// Set the clock skew for a specific node.
    ///
    /// Positive values mean the node's clock is ahead of real time,
    /// negative values mean the node's clock is behind.
    pub fn set_skew(&self, node: &I, skew: Duration) {
        let mut skews = self.skew_per_node.write();
        skews.insert(node.clone(), skew.as_millis() as i64);
    }

    /// Set the clock skew for a node with a signed millisecond value.
    ///
    /// Positive = ahead, negative = behind.
    pub fn set_skew_ms(&self, node: &I, skew_ms: i64) {
        let mut skews = self.skew_per_node.write();
        skews.insert(node.clone(), skew_ms);
    }

    /// Get the current skew for a node (in milliseconds).
    pub fn get_skew_ms(&self, node: &I) -> i64 {
        self.skew_per_node.read().get(node).copied().unwrap_or(0)
    }

    /// Clear the skew for a specific node.
    pub fn clear_skew(&self, node: &I) {
        let mut skews = self.skew_per_node.write();
        skews.remove(node);
    }

    /// Clear all clock skews.
    pub fn clear_all(&self) {
        let mut skews = self.skew_per_node.write();
        skews.clear();
    }

    /// Enable random clock drift with the given maximum value.
    ///
    /// Each call to `get_time()` will add a random value between
    /// -max_drift and +max_drift to simulate clock jitter.
    pub fn enable_drift(&self, max_drift: Duration) {
        self.max_drift_ms
            .store(max_drift.as_millis() as u64, Ordering::Release);
        self.drift_enabled.store(true, Ordering::Release);
    }

    /// Disable random clock drift.
    pub fn disable_drift(&self) {
        self.drift_enabled.store(false, Ordering::Release);
    }

    /// Get the "current time" as perceived by a specific node.
    ///
    /// This returns the actual instant adjusted by the node's skew
    /// and optional random drift.
    pub fn get_time(&self, node: &I) -> Instant {
        let base = Instant::now();
        let skew_ms = self.get_skew_ms(node);

        // Add random drift if enabled
        let drift_ms = if self.drift_enabled.load(Ordering::Acquire) {
            let max = self.max_drift_ms.load(Ordering::Acquire) as i64;
            if max > 0 {
                rand::rng().random_range(-max..=max)
            } else {
                0
            }
        } else {
            0
        };

        let total_offset_ms = skew_ms + drift_ms;

        // Apply the offset (handle both positive and negative)
        if total_offset_ms >= 0 {
            base + Duration::from_millis(total_offset_ms as u64)
        } else {
            // For negative offsets, saturate at the epoch
            base.checked_sub(Duration::from_millis((-total_offset_ms) as u64))
                .unwrap_or(base)
        }
    }

    /// Get the skewed duration between two nodes.
    ///
    /// Returns how much time difference node_a perceives relative to node_b.
    pub fn relative_skew(&self, node_a: &I, node_b: &I) -> i64 {
        self.get_skew_ms(node_a) - self.get_skew_ms(node_b)
    }

    /// Check if a message timestamp from `sender` would be considered valid by `receiver`.
    ///
    /// This simulates timestamp validation with clock skew.
    pub fn is_timestamp_valid(
        &self,
        sender: &I,
        receiver: &I,
        timestamp_age_ms: i64,
        max_age_ms: i64,
    ) -> bool {
        let relative = self.relative_skew(sender, receiver);
        // From receiver's perspective, the message age is adjusted by the relative skew
        let perceived_age = timestamp_age_ms - relative;
        perceived_age >= 0 && perceived_age <= max_age_ms
    }
}

impl<I: Clone + Eq + Hash> Default for ClockSkewSimulator<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Clone + Eq + Hash> Clone for ClockSkewSimulator<I> {
    fn clone(&self) -> Self {
        Self {
            skew_per_node: RwLock::new(self.skew_per_node.read().clone()),
            max_drift_ms: AtomicU64::new(self.max_drift_ms.load(Ordering::Relaxed)),
            drift_enabled: AtomicBool::new(self.drift_enabled.load(Ordering::Relaxed)),
        }
    }
}

// =============================================================================
// Message Reordering
// =============================================================================

/// A delayed message waiting to be delivered.
#[derive(Debug, Clone)]
pub struct DelayedMessage<I, M> {
    /// Source node.
    pub from: I,
    /// Destination node.
    pub to: I,
    /// The message payload.
    pub message: M,
    /// When the message was queued.
    pub queued_at: Instant,
    /// When the message should be delivered.
    pub deliver_at: Instant,
    /// Sequence number for ordering.
    pub sequence: u64,
}

/// Simulates message reordering for testing protocol robustness.
///
/// This can delay messages randomly, causing them to arrive out of order.
/// Useful for testing that the protocol handles out-of-order delivery correctly.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::testing::MessageReorderer;
/// use std::time::Duration;
///
/// let reorderer: MessageReorderer<u64, String> = MessageReorderer::new()
///     .with_reorder_probability(0.3)  // 30% of messages get delayed
///     .with_max_delay(Duration::from_millis(100));
///
/// // Queue a message
/// if let Some(delay) = reorderer.maybe_delay(&1, &2, "hello".to_string()) {
///     // Message was delayed, will need to be delivered later
/// }
///
/// // Get messages ready for delivery
/// for msg in reorderer.drain_ready() {
///     // Deliver msg.message to msg.to
/// }
/// ```
#[derive(Debug)]
pub struct MessageReorderer<I, M> {
    /// Queue of delayed messages.
    delay_queue: RwLock<VecDeque<DelayedMessage<I, M>>>,
    /// Probability of reordering a message (0.0 to 1.0).
    reorder_probability: f64,
    /// Maximum delay to apply to reordered messages.
    max_delay: Duration,
    /// Minimum delay to apply to reordered messages.
    min_delay: Duration,
    /// Sequence counter for message ordering.
    sequence: AtomicU64,
    /// Whether reordering is enabled.
    enabled: AtomicBool,
}

impl<I: Clone, M: Clone> MessageReorderer<I, M> {
    /// Create a new message reorderer with default settings (disabled).
    pub fn new() -> Self {
        Self {
            delay_queue: RwLock::new(VecDeque::new()),
            reorder_probability: 0.0,
            max_delay: Duration::ZERO,
            min_delay: Duration::ZERO,
            sequence: AtomicU64::new(0),
            enabled: AtomicBool::new(false),
        }
    }

    /// Set the probability of reordering a message.
    pub fn with_reorder_probability(mut self, probability: f64) -> Self {
        self.reorder_probability = probability.clamp(0.0, 1.0);
        if probability > 0.0 {
            self.enabled.store(true, Ordering::Release);
        }
        self
    }

    /// Set the maximum delay for reordered messages.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Set the minimum delay for reordered messages.
    pub fn with_min_delay(mut self, min_delay: Duration) -> Self {
        self.min_delay = min_delay;
        self
    }

    /// Enable message reordering.
    pub fn enable(&self) {
        self.enabled.store(true, Ordering::Release);
    }

    /// Disable message reordering.
    pub fn disable(&self) {
        self.enabled.store(false, Ordering::Release);
    }

    /// Check if a message should be delayed and queue it if so.
    ///
    /// Returns `Some(delay)` if the message was queued for later delivery,
    /// or `None` if it should be delivered immediately.
    pub fn maybe_delay(&self, from: &I, to: &I, message: M) -> Option<Duration> {
        if !self.enabled.load(Ordering::Acquire) {
            return None;
        }

        if self.reorder_probability == 0.0 {
            return None;
        }

        if rand::rng().random::<f64>() >= self.reorder_probability {
            return None;
        }

        // Calculate random delay
        let delay = if self.max_delay > self.min_delay {
            let range_ms = (self.max_delay - self.min_delay).as_millis() as u64;
            let random_ms = rand::rng().random_range(0..=range_ms);
            self.min_delay + Duration::from_millis(random_ms)
        } else {
            self.max_delay
        };

        let now = Instant::now();
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);

        let delayed = DelayedMessage {
            from: from.clone(),
            to: to.clone(),
            message,
            queued_at: now,
            deliver_at: now + delay,
            sequence,
        };

        let mut queue = self.delay_queue.write();
        queue.push_back(delayed);

        Some(delay)
    }

    /// Get all messages that are ready for delivery.
    pub fn drain_ready(&self) -> Vec<DelayedMessage<I, M>> {
        let now = Instant::now();
        let mut queue = self.delay_queue.write();
        let mut ready = Vec::new();

        // Drain all messages whose deliver_at has passed
        while let Some(msg) = queue.front() {
            if msg.deliver_at <= now {
                ready.push(queue.pop_front().unwrap());
            } else {
                break;
            }
        }

        ready
    }

    /// Get the next message ready for delivery, if any.
    pub fn pop_ready(&self) -> Option<DelayedMessage<I, M>> {
        let now = Instant::now();
        let mut queue = self.delay_queue.write();

        if let Some(msg) = queue.front() {
            if msg.deliver_at <= now {
                return queue.pop_front();
            }
        }

        None
    }

    /// Get the number of messages currently in the delay queue.
    pub fn queue_len(&self) -> usize {
        self.delay_queue.read().len()
    }

    /// Clear all pending delayed messages.
    pub fn clear(&self) {
        self.delay_queue.write().clear();
    }

    /// Get the time until the next message is ready.
    pub fn time_until_next(&self) -> Option<Duration> {
        let now = Instant::now();
        self.delay_queue
            .read()
            .front()
            .map(|msg| msg.deliver_at.saturating_duration_since(now))
    }
}

impl<I: Clone, M: Clone> Default for MessageReorderer<I, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Clone, M: Clone> Clone for MessageReorderer<I, M> {
    fn clone(&self) -> Self {
        Self {
            delay_queue: RwLock::new(self.delay_queue.read().clone()),
            reorder_probability: self.reorder_probability,
            max_delay: self.max_delay,
            min_delay: self.min_delay,
            sequence: AtomicU64::new(self.sequence.load(Ordering::Relaxed)),
            enabled: AtomicBool::new(self.enabled.load(Ordering::Relaxed)),
        }
    }
}

// =============================================================================
// Enhanced Chaos Controller
// =============================================================================

/// Enhanced chaos controller with clock skew, message reordering, and split-brain simulation.
///
/// This provides Jepsen-style testing capabilities for the Plumtree protocol:
/// - Network partitions (split-brain)
/// - Message loss
/// - Message reordering
/// - Clock skew between nodes
/// - Cascading failures
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::testing::EnhancedChaosController;
/// use std::time::Duration;
///
/// let chaos: EnhancedChaosController<u64, Vec<u8>> = EnhancedChaosController::new();
///
/// // Enable clock skew
/// chaos.clock_skew.set_skew_ms(&1, 100);  // Node 1 is 100ms ahead
/// chaos.clock_skew.set_skew_ms(&2, -50);  // Node 2 is 50ms behind
///
/// // Enable message reordering
/// chaos.reorderer.enable();
///
/// // Create network partition (split-brain)
/// chaos.split_brain(vec![1, 2], vec![3, 4, 5]);
/// ```
#[derive(Debug)]
pub struct EnhancedChaosController<I, M = Vec<u8>> {
    /// Base chaos configuration.
    pub config: Arc<RwLock<ChaosConfig>>,
    /// Network partition controller.
    pub partition: Arc<NetworkPartition<I>>,
    /// Clock skew simulator.
    pub clock_skew: Arc<ClockSkewSimulator<I>>,
    /// Message reorderer.
    pub reorderer: Arc<MessageReorderer<I, M>>,
    /// Statistics.
    pub stats: Arc<EnhancedChaosStats>,
}

/// Extended statistics for enhanced chaos testing.
#[derive(Debug, Default)]
pub struct EnhancedChaosStats {
    /// Base chaos stats.
    pub base: ChaosStats,
    /// Messages reordered.
    pub messages_reordered: AtomicU64,
    /// Messages affected by clock skew.
    pub clock_skew_events: AtomicU64,
    /// Split-brain events created.
    pub split_brain_events: AtomicU64,
}

impl EnhancedChaosStats {
    /// Create new enhanced stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a message being reordered.
    pub fn record_reorder(&self) {
        self.messages_reordered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a clock skew event.
    pub fn record_clock_skew_event(&self) {
        self.clock_skew_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a split-brain event.
    pub fn record_split_brain(&self) {
        self.split_brain_events.fetch_add(1, Ordering::Relaxed);
    }

    /// Get enhanced stats snapshot.
    pub fn snapshot(&self) -> EnhancedChaosStatsSnapshot {
        EnhancedChaosStatsSnapshot {
            base: self.base.snapshot(),
            messages_reordered: self.messages_reordered.load(Ordering::Relaxed),
            clock_skew_events: self.clock_skew_events.load(Ordering::Relaxed),
            split_brain_events: self.split_brain_events.load(Ordering::Relaxed),
        }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        self.base.reset();
        self.messages_reordered.store(0, Ordering::Relaxed);
        self.clock_skew_events.store(0, Ordering::Relaxed);
        self.split_brain_events.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of enhanced chaos statistics.
#[derive(Debug, Clone)]
pub struct EnhancedChaosStatsSnapshot {
    /// Base stats.
    pub base: ChaosStatsSnapshot,
    /// Messages reordered.
    pub messages_reordered: u64,
    /// Clock skew events.
    pub clock_skew_events: u64,
    /// Split-brain events.
    pub split_brain_events: u64,
}

impl<I: Clone + Eq + Hash, M: Clone> EnhancedChaosController<I, M> {
    /// Create a new enhanced chaos controller.
    pub fn new() -> Self {
        Self {
            config: Arc::new(RwLock::new(ChaosConfig::default())),
            partition: Arc::new(NetworkPartition::new()),
            clock_skew: Arc::new(ClockSkewSimulator::new()),
            reorderer: Arc::new(MessageReorderer::new()),
            stats: Arc::new(EnhancedChaosStats::new()),
        }
    }

    /// Create a split-brain partition between two groups.
    ///
    /// Nodes in group_a can communicate with each other,
    /// nodes in group_b can communicate with each other,
    /// but no communication is possible between the groups.
    pub fn split_brain(&self, group_a: Vec<I>, group_b: Vec<I>) {
        for a in &group_a {
            for b in &group_b {
                self.partition.partition(a.clone(), b.clone());
            }
        }
        self.stats.record_split_brain();
    }

    /// Heal the split-brain partition.
    pub fn heal_split_brain(&self) {
        self.partition.heal_all();
    }

    /// Simulate cascading failures where nodes fail one after another.
    ///
    /// Returns the list of nodes that were isolated.
    pub fn cascading_failure(&self, nodes: Vec<I>, delay_between: Duration) -> Vec<I> {
        let mut failed: Vec<I> = Vec::new();
        for node in nodes {
            // Isolate this node from all previously healthy nodes
            for other in &failed {
                self.partition.partition(node.clone(), other.clone());
            }
            failed.push(node);
            std::thread::sleep(delay_between);
        }
        failed
    }

    /// Apply random clock skew to a set of nodes.
    ///
    /// Each node gets a random skew between -max_skew_ms and +max_skew_ms.
    pub fn apply_random_clock_skew(&self, nodes: &[I], max_skew_ms: i64) {
        for node in nodes {
            let skew = rand::rng().random_range(-max_skew_ms..=max_skew_ms);
            self.clock_skew.set_skew_ms(node, skew);
            self.stats.record_clock_skew_event();
        }
    }

    /// Check if a message should be delivered, handling partitions and loss.
    ///
    /// Returns `Some(latency)` if the message should be delivered,
    /// or `None` if it should be dropped.
    pub fn should_deliver(&self, from: &I, to: &I) -> Option<Duration> {
        self.stats.base.record_message();

        // Check partition
        if self.partition.is_partitioned(from, to) {
            self.stats.base.record_partition_block();
            return None;
        }

        let config = self.config.read();

        // Check random drop
        if config.should_drop() {
            self.stats.base.record_drop();
            return None;
        }

        let latency = config.get_latency();
        if latency > Duration::ZERO {
            self.stats.base.record_delay();
        }

        Some(latency)
    }

    /// Maybe delay a message for reordering.
    ///
    /// Returns `Some(delay)` if the message was queued,
    /// or `None` if it should be delivered immediately.
    pub fn maybe_reorder(&self, from: &I, to: &I, message: M) -> Option<Duration> {
        if let Some(delay) = self.reorderer.maybe_delay(from, to, message) {
            self.stats.record_reorder();
            Some(delay)
        } else {
            None
        }
    }

    /// Get messages ready for delivery from the reorder queue.
    pub fn drain_reordered(&self) -> Vec<DelayedMessage<I, M>> {
        self.reorderer.drain_ready()
    }

    /// Get current statistics.
    pub fn stats(&self) -> EnhancedChaosStatsSnapshot {
        self.stats.snapshot()
    }

    /// Reset all statistics.
    pub fn reset_stats(&self) {
        self.stats.reset();
    }

    /// Disable all chaos effects.
    pub fn disable_all(&self) {
        *self.config.write() = ChaosConfig::default();
        self.partition.heal_all();
        self.clock_skew.clear_all();
        self.reorderer.disable();
        self.reorderer.clear();
    }
}

impl<I: Clone + Eq + Hash, M: Clone> Default for EnhancedChaosController<I, M> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Clone + Eq + Hash, M: Clone> Clone for EnhancedChaosController<I, M> {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            partition: self.partition.clone(),
            clock_skew: self.clock_skew.clone(),
            reorderer: self.reorderer.clone(),
            stats: self.stats.clone(),
        }
    }
}

// =============================================================================
// Test Utilities (for internal and external test use)
// =============================================================================

/// Wait for a condition to become true, with timeout.
///
/// This is useful for tests that need to wait for asynchronous conditions
/// without using fixed sleeps which can be flaky in CI environments.
///
/// # Arguments
///
/// * `condition` - A closure that returns `true` when the condition is met
/// * `timeout` - Maximum time to wait for the condition
/// * `poll_interval` - How often to check the condition
///
/// # Returns
///
/// `Ok(())` if the condition was met, `Err(message)` if timeout occurred.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::testing::wait_for;
/// use std::time::Duration;
///
/// let counter = std::sync::atomic::AtomicUsize::new(0);
///
/// // In another thread: counter.fetch_add(1, Ordering::SeqCst);
///
/// wait_for(
///     || counter.load(Ordering::SeqCst) >= 5,
///     Duration::from_secs(1),
///     Duration::from_millis(10),
/// ).expect("counter should reach 5");
/// ```
pub fn wait_for<F>(
    mut condition: F,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), String>
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while !condition() {
        if start.elapsed() > timeout {
            return Err(format!("Timeout after {:?} waiting for condition", timeout));
        }
        std::thread::sleep(poll_interval);
    }
    Ok(())
}

/// Wait for a condition with default poll interval (10ms).
///
/// Convenience wrapper around [`wait_for`] with a sensible default poll interval.
pub fn wait_for_condition<F>(condition: F, timeout: Duration) -> Result<(), String>
where
    F: FnMut() -> bool,
{
    wait_for(condition, timeout, Duration::from_millis(10))
}

/// Assert that a condition becomes true within a timeout.
///
/// This macro provides a convenient way to assert conditions that may take
/// time to become true, without using flaky fixed sleeps.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::assert_eventually;
///
/// let value = std::sync::atomic::AtomicBool::new(false);
///
/// // In another thread: value.store(true, Ordering::SeqCst);
///
/// assert_eventually!(
///     value.load(Ordering::SeqCst),
///     timeout = Duration::from_secs(1),
///     "value should become true"
/// );
/// ```
#[macro_export]
macro_rules! assert_eventually {
    ($condition:expr, timeout = $timeout:expr) => {
        $crate::testing::wait_for_condition(|| $condition, $timeout)
            .expect(concat!("Condition not met: ", stringify!($condition)));
    };
    ($condition:expr, timeout = $timeout:expr, $msg:expr) => {
        $crate::testing::wait_for_condition(|| $condition, $timeout).expect($msg);
    };
    ($condition:expr, timeout = $timeout:expr, poll = $poll:expr) => {
        $crate::testing::wait_for(|| $condition, $timeout, $poll)
            .expect(concat!("Condition not met: ", stringify!($condition)));
    };
    ($condition:expr, timeout = $timeout:expr, poll = $poll:expr, $msg:expr) => {
        $crate::testing::wait_for(|| $condition, $timeout, $poll).expect($msg);
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_config_defaults() {
        let config = ChaosConfig::new();
        assert!(!config.enabled);
        assert_eq!(config.message_loss_rate, 0.0);
        assert!(!config.should_drop());
    }

    #[test]
    fn test_chaos_config_moderate() {
        let config = ChaosConfig::moderate();
        assert!(config.enabled);
        assert_eq!(config.message_loss_rate, 0.05);
    }

    #[test]
    fn test_chaos_config_aggressive() {
        let config = ChaosConfig::aggressive();
        assert!(config.enabled);
        assert_eq!(config.message_loss_rate, 0.20);
    }

    #[test]
    fn test_network_partition() {
        let partition: NetworkPartition<u64> = NetworkPartition::new();

        // Initially no partition
        assert!(!partition.is_partitioned(&1, &2));

        // Create partition
        partition.partition(1, 2);
        assert!(partition.is_partitioned(&1, &2));
        assert!(partition.is_partitioned(&2, &1)); // Bidirectional

        // Heal partition
        partition.heal(&1, &2);
        assert!(!partition.is_partitioned(&1, &2));
    }

    #[test]
    fn test_network_partition_isolate() {
        let partition: NetworkPartition<u64> = NetworkPartition::new();

        // Isolate node 1 from nodes 2, 3, 4
        partition.isolate(1, vec![2, 3, 4]);

        assert!(partition.is_partitioned(&1, &2));
        assert!(partition.is_partitioned(&1, &3));
        assert!(partition.is_partitioned(&1, &4));
        assert!(!partition.is_partitioned(&2, &3)); // Other nodes can still communicate
    }

    #[test]
    fn test_chaos_stats() {
        let stats = ChaosStats::new();

        stats.record_message();
        stats.record_message();
        stats.record_drop();

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.messages_total, 2);
        assert_eq!(snapshot.messages_dropped, 1);
        assert_eq!(stats.drop_rate(), 0.5);
    }

    #[test]
    fn test_chaos_controller() {
        let controller: ChaosController<u64> = ChaosController::new();

        // Without chaos, all messages should be delivered immediately
        let result = controller.should_deliver(&1, &2);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Duration::ZERO);

        // Create a partition
        controller.partition.partition(1, 2);
        let result = controller.should_deliver(&1, &2);
        assert!(result.is_none()); // Blocked by partition

        // Check stats
        let stats = controller.stats();
        assert_eq!(stats.messages_total, 2);
        assert_eq!(stats.messages_partitioned, 1);
    }

    #[test]
    fn test_chaos_latency() {
        let config = ChaosConfig::new()
            .with_latency(Duration::from_millis(100))
            .with_jitter(Duration::from_millis(50));

        let latency = config.get_latency();
        assert!(latency >= Duration::from_millis(100));
        assert!(latency <= Duration::from_millis(150));
    }

    #[test]
    fn test_delivery_rate() {
        let snapshot = ChaosStatsSnapshot {
            messages_total: 100,
            messages_dropped: 10,
            messages_partitioned: 5,
            messages_delayed: 20,
        };

        assert_eq!(snapshot.delivery_rate(), 0.85); // 85% delivered
    }

    #[test]
    fn test_clock_skew_simulator() {
        let simulator: ClockSkewSimulator<u64> = ClockSkewSimulator::new();

        // No skew by default
        assert_eq!(simulator.get_skew_ms(&1), 0);

        // Set skew
        simulator.set_skew_ms(&1, 100);
        assert_eq!(simulator.get_skew_ms(&1), 100);

        simulator.set_skew_ms(&2, -50);
        assert_eq!(simulator.get_skew_ms(&2), -50);

        // Test relative skew
        let relative = simulator.relative_skew(&1, &2);
        assert_eq!(relative, 150); // 100 - (-50) = 150

        // Clear specific
        simulator.clear_skew(&1);
        assert_eq!(simulator.get_skew_ms(&1), 0);

        // Clear all
        simulator.clear_all();
        assert_eq!(simulator.get_skew_ms(&2), 0);
    }

    #[test]
    fn test_clock_skew_timestamp_validation() {
        let simulator: ClockSkewSimulator<u64> = ClockSkewSimulator::new();

        // Node 1 is 100ms ahead, node 2 is accurate
        simulator.set_skew_ms(&1, 100);
        simulator.set_skew_ms(&2, 0);

        // A message sent by node 1 at its "now" appears 100ms in the future to node 2
        // So if node 1 sends a message with age 0, node 2 sees it as -100ms old
        assert!(!simulator.is_timestamp_valid(&1, &2, 0, 1000));

        // But if node 1 sends a message that is 200ms old, node 2 sees it as 100ms old
        assert!(simulator.is_timestamp_valid(&1, &2, 200, 1000));
    }

    #[test]
    fn test_message_reorderer() {
        let reorderer: MessageReorderer<u64, String> = MessageReorderer::new()
            .with_reorder_probability(1.0) // Always reorder for testing
            .with_min_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(50));

        // Queue a message
        let delay = reorderer.maybe_delay(&1, &2, "hello".to_string());
        assert!(delay.is_some());
        assert_eq!(reorderer.queue_len(), 1);

        // Message not ready immediately
        assert!(reorderer.pop_ready().is_none());

        // Wait and check again
        std::thread::sleep(Duration::from_millis(60));
        let msg = reorderer.pop_ready();
        assert!(msg.is_some());
        let msg = msg.unwrap();
        assert_eq!(msg.from, 1);
        assert_eq!(msg.to, 2);
        assert_eq!(msg.message, "hello");
    }

    #[test]
    fn test_message_reorderer_disabled() {
        let reorderer: MessageReorderer<u64, String> =
            MessageReorderer::new().with_reorder_probability(0.0); // Never reorder

        let delay = reorderer.maybe_delay(&1, &2, "hello".to_string());
        assert!(delay.is_none());
        assert_eq!(reorderer.queue_len(), 0);
    }

    #[test]
    fn test_enhanced_chaos_controller() {
        let chaos: EnhancedChaosController<u64, Vec<u8>> = EnhancedChaosController::new();

        // Test split-brain
        chaos.split_brain(vec![1, 2], vec![3, 4]);

        // Nodes in same group can communicate
        assert!(chaos.should_deliver(&1, &2).is_some());
        assert!(chaos.should_deliver(&3, &4).is_some());

        // Nodes in different groups cannot
        assert!(chaos.should_deliver(&1, &3).is_none());
        assert!(chaos.should_deliver(&2, &4).is_none());

        // Heal and verify
        chaos.heal_split_brain();
        assert!(chaos.should_deliver(&1, &3).is_some());

        // Check stats
        let stats = chaos.stats();
        assert_eq!(stats.split_brain_events, 1);
    }

    #[test]
    fn test_enhanced_chaos_random_clock_skew() {
        let chaos: EnhancedChaosController<u64, Vec<u8>> = EnhancedChaosController::new();

        // Apply random skew to nodes
        chaos.apply_random_clock_skew(&[1, 2, 3], 100);

        // Verify skews were applied
        let skew1 = chaos.clock_skew.get_skew_ms(&1);
        let skew2 = chaos.clock_skew.get_skew_ms(&2);
        let skew3 = chaos.clock_skew.get_skew_ms(&3);

        assert!((-100..=100).contains(&skew1));
        assert!((-100..=100).contains(&skew2));
        assert!((-100..=100).contains(&skew3));

        // Check stats
        let stats = chaos.stats();
        assert_eq!(stats.clock_skew_events, 3);
    }

    #[test]
    fn test_enhanced_chaos_disable_all() {
        let chaos: EnhancedChaosController<u64, Vec<u8>> = EnhancedChaosController::new();

        // Enable various chaos
        chaos.split_brain(vec![1], vec![2]);
        chaos.clock_skew.set_skew_ms(&1, 100);
        *chaos.config.write() = ChaosConfig::moderate();

        // Disable all
        chaos.disable_all();

        // Verify everything is disabled
        assert!(chaos.should_deliver(&1, &2).is_some());
        assert_eq!(chaos.clock_skew.get_skew_ms(&1), 0);
        assert!(!chaos.config.read().enabled);
    }
}
