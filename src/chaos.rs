//! Chaos engineering support for Plumtree protocol.
//!
//! This module provides a trait-based chaos injection system:
//! - **Production**: Use [`NoopChaosPolicy`] - all operations are inlined no-ops
//! - **Dev/Test**: Use [`ConfigurableChaosPolicy`] - full chaos with runtime config
//!
//! # Design
//!
//! The chaos system is purely trait-based. No feature flags needed.
//! The compiler inlines `NoopChaosPolicy` methods to nothing, so there's
//! zero runtime overhead when using it in production.
//!
//! # Key Types
//!
//! - [`ChaosDecision`] - Semantic enum for chaos decisions (Deliver, Delay, Drop, Partition)
//! - [`ChaosPolicy`] - Trait for chaos injection policies
//! - [`NoopChaosPolicy`] - Zero-overhead no-op for production
//! - [`ConfigurableChaosPolicy`] - Full chaos support with runtime config
//! - [`crate::chaos::MessageType`] - Protocol message types for targeted chaos
//!
//! # Usage
//!
//! ```ignore
//! use memberlist_plumtree::chaos::{ChaosPolicy, NoopChaosPolicy, ConfigurableChaosPolicy, ChaosDecision};
//!
//! // Production - zero overhead
//! let policy = NoopChaosPolicy;
//!
//! // Dev/Test - full chaos support
//! let policy = ConfigurableChaosPolicy::<NodeId>::new();
//! policy.set_loss_rate(0.1);  // 10% packet loss
//! policy.partition(node_a, node_b);  // Network partition
//!
//! // Check decision
//! match policy.decide(&from, &to) {
//!     ChaosDecision::Deliver => send_immediately(),
//!     ChaosDecision::Delay(d) => schedule_send(d),
//!     ChaosDecision::Drop => { /* discard */ }
//!     ChaosDecision::Partition => { /* blocked by partition */ }
//! }
//! ```
//!
//! # Deterministic Chaos
//!
//! For reproducible tests, use a seeded configuration:
//!
//! ```ignore
//! let config = ChaosConfig::deterministic(42)  // Seed = 42
//!     .with_loss_rate(0.1);
//!
//! // Same seed + same sequence of calls = same decisions
//! ```

use std::{
    collections::HashSet,
    fmt::Debug,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

// =============================================================================
// ChaosDecision - Semantic decision enum
// =============================================================================

/// Decision made by chaos policy for a message.
///
/// This enum provides explicit semantics for chaos decisions, making code
/// clearer than using `Option<Duration>`.
///
/// # Example
///
/// ```ignore
/// match chaos.decide(&from, &to) {
///     ChaosDecision::Deliver => send_immediately(),
///     ChaosDecision::Delay(d) => schedule_send(d),
///     ChaosDecision::Drop => { /* discard */ }
///     ChaosDecision::Partition => { /* blocked */ }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ChaosDecision {
    /// Deliver the message immediately.
    #[default]
    Deliver,
    /// Delay the message by the specified duration.
    Delay(Duration),
    /// Drop the message (simulates random packet loss).
    Drop,
    /// Block the message due to network partition.
    Partition,
}

impl ChaosDecision {
    /// Check if the message should be delivered (eventually).
    ///
    /// Returns `true` for `Deliver` and `Delay`, `false` for `Drop` and `Partition`.
    #[inline]
    pub fn is_delivered(&self) -> bool {
        matches!(self, ChaosDecision::Deliver | ChaosDecision::Delay(_))
    }

    /// Check if the message is dropped (not delivered).
    ///
    /// Returns `true` for `Drop` and `Partition`, `false` for `Deliver` and `Delay`.
    #[inline]
    pub fn is_dropped(&self) -> bool {
        matches!(self, ChaosDecision::Drop | ChaosDecision::Partition)
    }

    /// Get the delay duration, if any.
    ///
    /// Returns `Some(duration)` for `Delay`, `None` otherwise.
    #[inline]
    pub fn delay(&self) -> Option<Duration> {
        match self {
            ChaosDecision::Delay(d) => Some(*d),
            _ => None,
        }
    }

    /// Convert to `Option<Duration>` for backward compatibility.
    ///
    /// - `Deliver` → `Some(Duration::ZERO)`
    /// - `Delay(d)` → `Some(d)`
    /// - `Drop` → `None`
    /// - `Partition` → `None`
    #[inline]
    pub fn to_option(&self) -> Option<Duration> {
        match self {
            ChaosDecision::Deliver => Some(Duration::ZERO),
            ChaosDecision::Delay(d) => Some(*d),
            ChaosDecision::Drop | ChaosDecision::Partition => None,
        }
    }

    /// Get the kind of decision as a static string (for metrics).
    #[inline]
    pub fn kind(&self) -> &'static str {
        match self {
            ChaosDecision::Deliver => "deliver",
            ChaosDecision::Delay(_) => "delay",
            ChaosDecision::Drop => "drop",
            ChaosDecision::Partition => "partition",
        }
    }
}

// =============================================================================
// MessageType - Protocol message types for targeted chaos
// =============================================================================

/// Message types for protocol-aware chaos injection.
///
/// Use with [`ChaosPolicy::decide_typed`] to apply different chaos policies
/// to different message types.
///
/// # Example
///
/// ```ignore
/// // Drop 50% of Gossip messages but deliver all Graft messages
/// let decision = chaos.decide_typed(&from, &to, MessageType::Gossip);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MessageType {
    /// Full message payload (Gossip/eager push).
    Gossip,
    /// Message ID announcement (IHave/lazy push).
    IHave,
    /// Request to establish eager link and get missing message.
    Graft,
    /// Request to demote sender from eager to lazy.
    Prune,
    /// Anti-entropy synchronization message.
    Sync,
    /// Unknown or unclassified message type.
    #[default]
    Unknown,
}

impl MessageType {
    /// Get the message type as a static string (for metrics).
    #[inline]
    pub fn name(&self) -> &'static str {
        match self {
            MessageType::Gossip => "gossip",
            MessageType::IHave => "ihave",
            MessageType::Graft => "graft",
            MessageType::Prune => "prune",
            MessageType::Sync => "sync",
            MessageType::Unknown => "unknown",
        }
    }
}

// =============================================================================
// ChaosPolicy Trait
// =============================================================================

/// Trait for chaos injection policies.
///
/// Implementations control how messages are affected during transit:
/// - Drop messages (simulate packet loss)
/// - Add latency (simulate network delays)
/// - Partition nodes (simulate network splits)
///
/// # Production Use
///
/// Use [`NoopChaosPolicy`] which compiles to no-ops with zero overhead.
///
/// # Testing Use
///
/// Use [`ConfigurableChaosPolicy`] which supports runtime configuration.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::chaos::{ChaosPolicy, ConfigurableChaosPolicy, ChaosDecision};
///
/// fn send_message<I, C: ChaosPolicy<I>>(chaos: &C, from: &I, to: &I, data: &[u8]) {
///     match chaos.decide(from, to) {
///         ChaosDecision::Deliver => {
///             // Send immediately
///         }
///         ChaosDecision::Delay(d) => {
///             // Schedule send after delay
///         }
///         ChaosDecision::Drop | ChaosDecision::Partition => {
///             // Message dropped
///         }
///     }
/// }
/// ```
pub trait ChaosPolicy<I>: Send + Sync + Debug {
    /// Decide what to do with a message from `from` to `to`.
    ///
    /// Returns a [`ChaosDecision`] indicating how the message should be handled.
    fn decide(&self, from: &I, to: &I) -> ChaosDecision;

    /// Decide with message type awareness (advanced).
    ///
    /// Allows applying different chaos policies to different message types.
    /// For example, you might want to drop Gossip messages but always deliver Graft.
    ///
    /// Default implementation ignores message type and calls [`decide`](Self::decide).
    #[inline]
    fn decide_typed(&self, from: &I, to: &I, _msg_type: MessageType) -> ChaosDecision {
        self.decide(from, to)
    }

    /// Check if a message from `from` to `to` should be delivered.
    ///
    /// Returns `Some(latency)` if the message should be delivered (with optional delay),
    /// or `None` if the message should be dropped.
    ///
    /// **Deprecated**: Use [`decide`](Self::decide) instead for clearer semantics.
    #[inline]
    fn should_deliver(&self, from: &I, to: &I) -> Option<Duration> {
        self.decide(from, to).to_option()
    }

    /// Check if chaos is enabled.
    fn is_enabled(&self) -> bool;

    /// Get a snapshot of chaos statistics.
    fn stats(&self) -> ChaosStatsSnapshot;

    /// Reset statistics.
    fn reset_stats(&self);
}

// =============================================================================
// NoopChaosPolicy - Zero overhead for production
// =============================================================================

/// No-op chaos policy for production use.
///
/// All operations are `#[inline(always)]` no-ops. The compiler optimizes
/// these away completely, resulting in zero runtime overhead.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::chaos::{ChaosPolicy, NoopChaosPolicy};
///
/// let policy = NoopChaosPolicy;
///
/// // This compiles to nothing - zero overhead
/// if let Some(_) = policy.should_deliver(&1u64, &2u64) {
///     // Always executes
/// }
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopChaosPolicy;

impl NoopChaosPolicy {
    /// Create a new no-op chaos policy.
    #[inline(always)]
    pub const fn new() -> Self {
        Self
    }
}

impl<I> ChaosPolicy<I> for NoopChaosPolicy {
    #[inline(always)]
    fn decide(&self, _from: &I, _to: &I) -> ChaosDecision {
        ChaosDecision::Deliver
    }

    #[inline(always)]
    fn decide_typed(&self, _from: &I, _to: &I, _msg_type: MessageType) -> ChaosDecision {
        ChaosDecision::Deliver
    }

    #[inline(always)]
    fn is_enabled(&self) -> bool {
        false
    }

    #[inline(always)]
    fn stats(&self) -> ChaosStatsSnapshot {
        ChaosStatsSnapshot::ZERO
    }

    #[inline(always)]
    fn reset_stats(&self) {
        // No-op
    }
}

// =============================================================================
// ChaosStatsSnapshot
// =============================================================================

/// Snapshot of chaos statistics.
#[derive(Debug, Clone, Default)]
pub struct ChaosStatsSnapshot {
    /// Total messages processed.
    pub messages_total: u64,
    /// Messages dropped due to configured loss rate.
    pub messages_dropped: u64,
    /// Messages blocked due to partition.
    pub messages_partitioned: u64,
    /// Messages delayed.
    pub messages_delayed: u64,
}

impl ChaosStatsSnapshot {
    /// Zero stats constant for no-op policy.
    pub const ZERO: Self = Self {
        messages_total: 0,
        messages_dropped: 0,
        messages_partitioned: 0,
        messages_delayed: 0,
    };

    /// Get the effective delivery rate (1.0 - drop rate - partition rate).
    pub fn delivery_rate(&self) -> f64 {
        if self.messages_total == 0 {
            return 1.0;
        }
        let failed = self.messages_dropped + self.messages_partitioned;
        1.0 - (failed as f64 / self.messages_total as f64)
    }

    /// Get the drop rate.
    pub fn drop_rate(&self) -> f64 {
        if self.messages_total == 0 {
            return 0.0;
        }
        self.messages_dropped as f64 / self.messages_total as f64
    }

    /// Get the partition rate.
    pub fn partition_rate(&self) -> f64 {
        if self.messages_total == 0 {
            return 0.0;
        }
        self.messages_partitioned as f64 / self.messages_total as f64
    }

    /// Get the delay rate.
    pub fn delay_rate(&self) -> f64 {
        if self.messages_total == 0 {
            return 0.0;
        }
        self.messages_delayed as f64 / self.messages_total as f64
    }
}

// =============================================================================
// ChaosConfig - Configuration for chaos injection
// =============================================================================

/// Configuration for chaos injection.
///
/// # Presets
///
/// - [`ChaosConfig::default`] - Disabled (no chaos)
/// - [`ChaosConfig::moderate`] - 5% loss, 10ms latency, 20ms jitter
/// - [`ChaosConfig::aggressive`] - 20% loss, 50ms latency, 100ms jitter
/// - [`ChaosConfig::deterministic`] - Reproducible chaos with seeded RNG
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::chaos::ChaosConfig;
///
/// let config = ChaosConfig::new()
///     .with_loss_rate(0.1)
///     .with_latency(Duration::from_millis(50))
///     .with_jitter(Duration::from_millis(25));
///
/// // For reproducible tests
/// let deterministic = ChaosConfig::deterministic(42)
///     .with_loss_rate(0.2);
/// ```
#[derive(Debug, Clone)]
pub struct ChaosConfig {
    /// Probability of dropping a message (0.0 to 1.0).
    pub loss_rate: f64,
    /// Base latency to add to all messages.
    pub base_latency: Duration,
    /// Random jitter added to latency (0 to this value).
    pub latency_jitter: Duration,
    /// Whether chaos is enabled.
    pub enabled: bool,
    /// Optional seed for deterministic behavior.
    ///
    /// When set, chaos decisions are reproducible.
    /// Same seed = same sequence of drops/delays.
    pub seed: Option<u64>,
}

impl Default for ChaosConfig {
    fn default() -> Self {
        Self {
            loss_rate: 0.0,
            base_latency: Duration::ZERO,
            latency_jitter: Duration::ZERO,
            enabled: false,
            seed: None,
        }
    }
}

impl ChaosConfig {
    /// Create a new chaos configuration (disabled by default).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a deterministic chaos configuration with a seed.
    ///
    /// Same seed = same sequence of drops/delays for reproducible tests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Two tests with same seed will have identical chaos behavior
    /// let config1 = ChaosConfig::deterministic(42).with_loss_rate(0.1);
    /// let config2 = ChaosConfig::deterministic(42).with_loss_rate(0.1);
    /// // Both will drop the same messages in the same order
    /// ```
    pub fn deterministic(seed: u64) -> Self {
        Self {
            seed: Some(seed),
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a moderate chaos configuration.
    ///
    /// - 5% message loss
    /// - 10ms base latency
    /// - 20ms jitter
    pub fn moderate() -> Self {
        Self {
            loss_rate: 0.05,
            base_latency: Duration::from_millis(10),
            latency_jitter: Duration::from_millis(20),
            enabled: true,
            seed: None,
        }
    }

    /// Create an aggressive chaos configuration.
    ///
    /// - 20% message loss
    /// - 50ms base latency
    /// - 100ms jitter
    pub fn aggressive() -> Self {
        Self {
            loss_rate: 0.20,
            base_latency: Duration::from_millis(50),
            latency_jitter: Duration::from_millis(100),
            enabled: true,
            seed: None,
        }
    }

    /// Set the message loss rate (0.0 to 1.0).
    pub fn with_loss_rate(mut self, rate: f64) -> Self {
        self.loss_rate = rate.clamp(0.0, 1.0);
        if rate > 0.0 {
            self.enabled = true;
        }
        self
    }

    /// Set the base latency.
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.base_latency = latency;
        if latency > Duration::ZERO {
            self.enabled = true;
        }
        self
    }

    /// Set the latency jitter.
    pub fn with_jitter(mut self, jitter: Duration) -> Self {
        self.latency_jitter = jitter;
        self
    }

    /// Enable chaos.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set seed for reproducible tests.
    ///
    /// When set, chaos decisions become deterministic: same seed = same outcomes.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }
}

// =============================================================================
// ConfigurableChaosPolicy - Full chaos support for dev/test
// =============================================================================

/// Configurable chaos policy for dev/test environments.
///
/// Supports:
/// - Random message loss
/// - Latency injection with jitter
/// - Network partitions between nodes
/// - Runtime configuration changes
/// - Statistics tracking
/// - Deterministic mode with seeded RNG
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::chaos::{ChaosPolicy, ConfigurableChaosPolicy, ChaosConfig, ChaosDecision};
/// use std::time::Duration;
///
/// // Create with config
/// let chaos = ConfigurableChaosPolicy::<u64>::with_config(ChaosConfig::moderate());
///
/// // Or create and configure at runtime
/// let chaos = ConfigurableChaosPolicy::<u64>::new();
/// chaos.set_loss_rate(0.1);  // 10% loss
/// chaos.set_latency(Duration::from_millis(50));
///
/// // Create network partition
/// chaos.partition(1, 2);  // Node 1 and 2 cannot communicate
///
/// // Check decision (preferred)
/// match chaos.decide(&1, &3) {
///     ChaosDecision::Deliver => println!("Send immediately"),
///     ChaosDecision::Delay(d) => println!("Delay by {:?}", d),
///     ChaosDecision::Drop => println!("Random drop"),
///     ChaosDecision::Partition => println!("Blocked by partition"),
/// }
///
/// // Check stats
/// let stats = chaos.stats();
/// println!("Delivery rate: {:.1}%", stats.delivery_rate() * 100.0);
/// ```
///
/// # Deterministic Mode
///
/// ```ignore
/// // For reproducible tests, use a seeded config
/// let chaos = ConfigurableChaosPolicy::<u64>::with_config(
///     ChaosConfig::deterministic(42).with_loss_rate(0.1)
/// );
/// // Same seed = same sequence of decisions
/// ```
#[derive(Debug)]
pub struct ConfigurableChaosPolicy<I> {
    config: RwLock<ChaosConfig>,
    partitions: RwLock<HashSet<(I, I)>>,
    partition_active: AtomicBool,
    stats: ChaosStats,
    /// Seeded RNG for deterministic mode (None = non-deterministic).
    rng: RwLock<Option<StdRng>>,
}

#[derive(Debug, Default)]
struct ChaosStats {
    messages_total: AtomicU64,
    messages_dropped: AtomicU64,
    messages_partitioned: AtomicU64,
    messages_delayed: AtomicU64,
}

impl<I: Clone + Eq + Hash + Send + Sync + Debug> ConfigurableChaosPolicy<I> {
    /// Create a new configurable chaos policy (disabled by default).
    pub fn new() -> Self {
        Self {
            config: RwLock::new(ChaosConfig::default()),
            partitions: RwLock::new(HashSet::new()),
            partition_active: AtomicBool::new(false),
            stats: ChaosStats::default(),
            rng: RwLock::new(None),
        }
    }

    /// Create with initial configuration.
    pub fn with_config(config: ChaosConfig) -> Self {
        let rng = config.seed.map(StdRng::seed_from_u64);
        Self {
            config: RwLock::new(config),
            partitions: RwLock::new(HashSet::new()),
            partition_active: AtomicBool::new(false),
            stats: ChaosStats::default(),
            rng: RwLock::new(rng),
        }
    }

    /// Update the chaos configuration at runtime.
    pub fn set_config(&self, config: ChaosConfig) {
        // Update RNG if seed changed
        if config.seed.is_some() {
            *self.rng.write() = config.seed.map(StdRng::seed_from_u64);
        }
        *self.config.write() = config;
    }

    /// Get the current configuration.
    pub fn config(&self) -> ChaosConfig {
        self.config.read().clone()
    }

    /// Set message loss rate (0.0 to 1.0).
    pub fn set_loss_rate(&self, rate: f64) {
        let mut config = self.config.write();
        config.loss_rate = rate.clamp(0.0, 1.0);
        if rate > 0.0 {
            config.enabled = true;
        }
    }

    /// Set base latency.
    pub fn set_latency(&self, latency: Duration) {
        let mut config = self.config.write();
        config.base_latency = latency;
        if latency > Duration::ZERO {
            config.enabled = true;
        }
    }

    /// Set latency jitter.
    pub fn set_jitter(&self, jitter: Duration) {
        self.config.write().latency_jitter = jitter;
    }

    /// Enable chaos.
    pub fn enable(&self) {
        self.config.write().enabled = true;
    }

    /// Disable chaos (messages still counted in stats).
    pub fn disable(&self) {
        self.config.write().enabled = false;
    }

    /// Create a bidirectional partition between two nodes.
    ///
    /// After this call, messages between `node_a` and `node_b` will be dropped.
    pub fn partition(&self, node_a: I, node_b: I) {
        let mut partitions = self.partitions.write();
        partitions.insert((node_a.clone(), node_b.clone()));
        partitions.insert((node_b, node_a));
        self.partition_active.store(true, Ordering::Release);
    }

    /// Heal a partition between two nodes.
    pub fn heal(&self, node_a: &I, node_b: &I) {
        let mut partitions = self.partitions.write();
        partitions.remove(&(node_a.clone(), node_b.clone()));
        partitions.remove(&(node_b.clone(), node_a.clone()));
        if partitions.is_empty() {
            self.partition_active.store(false, Ordering::Release);
        }
    }

    /// Heal all partitions.
    pub fn heal_all(&self) {
        self.partitions.write().clear();
        self.partition_active.store(false, Ordering::Release);
    }

    /// Check if two nodes are partitioned.
    pub fn is_partitioned(&self, from: &I, to: &I) -> bool {
        if !self.partition_active.load(Ordering::Acquire) {
            return false;
        }
        self.partitions.read().contains(&(from.clone(), to.clone()))
    }

    /// Isolate a node from all others.
    ///
    /// Creates partitions between `node` and each node in `others`.
    pub fn isolate(&self, node: I, others: impl IntoIterator<Item = I>) {
        for other in others {
            self.partition(node.clone(), other);
        }
    }

    /// Create a split-brain partition between two groups.
    ///
    /// Nodes within each group can communicate, but no communication
    /// is possible between the groups.
    pub fn split_brain(
        &self,
        group_a: impl IntoIterator<Item = I>,
        group_b: impl IntoIterator<Item = I>,
    ) {
        let group_a: Vec<_> = group_a.into_iter().collect();
        let group_b: Vec<_> = group_b.into_iter().collect();
        for a in &group_a {
            for b in &group_b {
                self.partition(a.clone(), b.clone());
            }
        }
    }

    /// Generate a random f64 in [0.0, 1.0).
    ///
    /// Uses seeded RNG if configured, otherwise uses thread-local RNG.
    fn random_f64(&self) -> f64 {
        let mut rng_guard = self.rng.write();
        if let Some(ref mut rng) = *rng_guard {
            // Deterministic mode
            rng.random()
        } else {
            // Non-deterministic mode
            rand::rng().random()
        }
    }

    /// Generate a random u64 in the given range.
    fn random_range(&self, range: std::ops::RangeInclusive<u64>) -> u64 {
        let mut rng_guard = self.rng.write();
        if let Some(ref mut rng) = *rng_guard {
            rng.random_range(range)
        } else {
            rand::rng().random_range(range)
        }
    }

    fn should_drop(&self, config: &ChaosConfig) -> bool {
        if config.loss_rate == 0.0 {
            return false;
        }
        self.random_f64() < config.loss_rate
    }

    fn get_latency(&self, config: &ChaosConfig) -> Duration {
        let jitter = if config.latency_jitter > Duration::ZERO {
            let jitter_ms = self.random_range(0..=config.latency_jitter.as_millis() as u64);
            Duration::from_millis(jitter_ms)
        } else {
            Duration::ZERO
        };
        config.base_latency + jitter
    }
}

impl<I: Clone + Eq + Hash + Send + Sync + Debug> Default for ConfigurableChaosPolicy<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: Clone + Eq + Hash + Send + Sync + Debug> ChaosPolicy<I> for ConfigurableChaosPolicy<I> {
    fn decide(&self, from: &I, to: &I) -> ChaosDecision {
        // Always count messages
        self.stats.messages_total.fetch_add(1, Ordering::Relaxed);

        // Record chaos event for metrics
        #[cfg(feature = "metrics")]
        let decision_for_metrics;

        // Check partition first - partitions ALWAYS block, regardless of enabled flag
        if self.is_partitioned(from, to) {
            self.stats
                .messages_partitioned
                .fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            {
                decision_for_metrics = ChaosDecision::Partition;
                crate::metrics::record_chaos_event(decision_for_metrics.kind());
            }
            return ChaosDecision::Partition;
        }

        let config = self.config.read();

        // If not enabled, deliver immediately (but partitions above still block)
        if !config.enabled {
            #[cfg(feature = "metrics")]
            crate::metrics::record_chaos_event("deliver");
            return ChaosDecision::Deliver;
        }

        // Check random drop
        if self.should_drop(&config) {
            self.stats.messages_dropped.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            crate::metrics::record_chaos_event("drop");
            return ChaosDecision::Drop;
        }

        // Get latency
        let latency = self.get_latency(&config);
        if latency > Duration::ZERO {
            self.stats.messages_delayed.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            crate::metrics::record_chaos_event("delay");
            return ChaosDecision::Delay(latency);
        }

        #[cfg(feature = "metrics")]
        crate::metrics::record_chaos_event("deliver");
        ChaosDecision::Deliver
    }

    fn is_enabled(&self) -> bool {
        self.config.read().enabled || self.partition_active.load(Ordering::Acquire)
    }

    fn stats(&self) -> ChaosStatsSnapshot {
        ChaosStatsSnapshot {
            messages_total: self.stats.messages_total.load(Ordering::Relaxed),
            messages_dropped: self.stats.messages_dropped.load(Ordering::Relaxed),
            messages_partitioned: self.stats.messages_partitioned.load(Ordering::Relaxed),
            messages_delayed: self.stats.messages_delayed.load(Ordering::Relaxed),
        }
    }

    fn reset_stats(&self) {
        self.stats.messages_total.store(0, Ordering::Relaxed);
        self.stats.messages_dropped.store(0, Ordering::Relaxed);
        self.stats.messages_partitioned.store(0, Ordering::Relaxed);
        self.stats.messages_delayed.store(0, Ordering::Relaxed);
    }
}

// =============================================================================
// Type aliases and factory functions
// =============================================================================

/// Type alias for a shared chaos policy.
pub type SharedChaosPolicy<I> = Arc<dyn ChaosPolicy<I>>;

/// Create a no-op chaos policy (for production).
#[inline]
pub fn noop_policy<I: 'static>() -> Arc<NoopChaosPolicy> {
    Arc::new(NoopChaosPolicy)
}

/// Create a configurable chaos policy (for dev/test).
pub fn configurable_policy<I: Clone + Eq + Hash + Send + Sync + Debug + 'static>(
) -> Arc<ConfigurableChaosPolicy<I>> {
    Arc::new(ConfigurableChaosPolicy::new())
}

/// Create a configurable chaos policy with initial config.
pub fn configurable_policy_with_config<I: Clone + Eq + Hash + Send + Sync + Debug + 'static>(
    config: ChaosConfig,
) -> Arc<ConfigurableChaosPolicy<I>> {
    Arc::new(ConfigurableChaosPolicy::with_config(config))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // ChaosDecision Tests
    // =========================================================================

    #[test]
    fn test_chaos_decision_deliver() {
        let decision = ChaosDecision::Deliver;
        assert!(decision.is_delivered());
        assert!(!decision.is_dropped());
        assert_eq!(decision.delay(), None);
        assert_eq!(decision.to_option(), Some(Duration::ZERO));
        assert_eq!(decision.kind(), "deliver");
    }

    #[test]
    fn test_chaos_decision_delay() {
        let delay = Duration::from_millis(100);
        let decision = ChaosDecision::Delay(delay);
        assert!(decision.is_delivered());
        assert!(!decision.is_dropped());
        assert_eq!(decision.delay(), Some(delay));
        assert_eq!(decision.to_option(), Some(delay));
        assert_eq!(decision.kind(), "delay");
    }

    #[test]
    fn test_chaos_decision_drop() {
        let decision = ChaosDecision::Drop;
        assert!(!decision.is_delivered());
        assert!(decision.is_dropped());
        assert_eq!(decision.delay(), None);
        assert_eq!(decision.to_option(), None);
        assert_eq!(decision.kind(), "drop");
    }

    #[test]
    fn test_chaos_decision_partition() {
        let decision = ChaosDecision::Partition;
        assert!(!decision.is_delivered());
        assert!(decision.is_dropped());
        assert_eq!(decision.delay(), None);
        assert_eq!(decision.to_option(), None);
        assert_eq!(decision.kind(), "partition");
    }

    #[test]
    fn test_chaos_decision_default() {
        let decision = ChaosDecision::default();
        assert_eq!(decision, ChaosDecision::Deliver);
    }

    // =========================================================================
    // MessageType Tests
    // =========================================================================

    #[test]
    fn test_message_type_names() {
        assert_eq!(MessageType::Gossip.name(), "gossip");
        assert_eq!(MessageType::IHave.name(), "ihave");
        assert_eq!(MessageType::Graft.name(), "graft");
        assert_eq!(MessageType::Prune.name(), "prune");
        assert_eq!(MessageType::Sync.name(), "sync");
        assert_eq!(MessageType::Unknown.name(), "unknown");
    }

    #[test]
    fn test_message_type_default() {
        let msg_type = MessageType::default();
        assert_eq!(msg_type, MessageType::Unknown);
    }

    // =========================================================================
    // NoopChaosPolicy Tests
    // =========================================================================

    #[test]
    fn test_noop_policy_decide() {
        let policy = NoopChaosPolicy;

        // Should always deliver immediately
        let decision = ChaosPolicy::<u64>::decide(&policy, &1, &2);
        assert_eq!(decision, ChaosDecision::Deliver);
        assert!(!ChaosPolicy::<u64>::is_enabled(&policy));

        let stats = ChaosPolicy::<u64>::stats(&policy);
        assert_eq!(stats.messages_total, 0);
    }

    #[test]
    fn test_noop_policy_decide_typed() {
        let policy = NoopChaosPolicy;

        // All message types should deliver
        for msg_type in [
            MessageType::Gossip,
            MessageType::IHave,
            MessageType::Graft,
            MessageType::Prune,
            MessageType::Sync,
        ] {
            let decision = ChaosPolicy::<u64>::decide_typed(&policy, &1, &2, msg_type);
            assert_eq!(decision, ChaosDecision::Deliver);
        }
    }

    #[test]
    fn test_noop_policy_backward_compat() {
        let policy = NoopChaosPolicy;

        // should_deliver still works
        assert_eq!(
            ChaosPolicy::<u64>::should_deliver(&policy, &1, &2),
            Some(Duration::ZERO)
        );
    }

    // =========================================================================
    // ChaosStatsSnapshot Tests
    // =========================================================================

    #[test]
    fn test_chaos_stats_snapshot() {
        let snapshot = ChaosStatsSnapshot {
            messages_total: 100,
            messages_dropped: 10,
            messages_partitioned: 5,
            messages_delayed: 20,
        };

        assert_eq!(snapshot.delivery_rate(), 0.85);
        assert_eq!(snapshot.drop_rate(), 0.10);
        assert_eq!(snapshot.partition_rate(), 0.05);
        assert_eq!(snapshot.delay_rate(), 0.20);
    }

    // =========================================================================
    // ChaosConfig Tests
    // =========================================================================

    #[test]
    fn test_chaos_config_presets() {
        let default = ChaosConfig::default();
        assert!(!default.enabled);
        assert_eq!(default.loss_rate, 0.0);
        assert!(default.seed.is_none());

        let moderate = ChaosConfig::moderate();
        assert!(moderate.enabled);
        assert_eq!(moderate.loss_rate, 0.05);

        let aggressive = ChaosConfig::aggressive();
        assert!(aggressive.enabled);
        assert_eq!(aggressive.loss_rate, 0.20);
    }

    #[test]
    fn test_chaos_config_builder() {
        let config = ChaosConfig::new()
            .with_loss_rate(0.15)
            .with_latency(Duration::from_millis(100))
            .with_jitter(Duration::from_millis(50));

        assert!(config.enabled);
        assert_eq!(config.loss_rate, 0.15);
        assert_eq!(config.base_latency, Duration::from_millis(100));
        assert_eq!(config.latency_jitter, Duration::from_millis(50));
    }

    #[test]
    fn test_chaos_config_deterministic() {
        let config = ChaosConfig::deterministic(42);
        assert!(config.enabled);
        assert_eq!(config.seed, Some(42));
    }

    #[test]
    fn test_chaos_config_with_seed() {
        let config = ChaosConfig::new().with_loss_rate(0.1).with_seed(12345);

        assert_eq!(config.seed, Some(12345));
        assert!(config.enabled);
    }

    // =========================================================================
    // ConfigurableChaosPolicy Tests
    // =========================================================================

    #[test]
    fn test_configurable_policy_disabled() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();

        // Should deliver when disabled
        let decision = policy.decide(&1, &2);
        assert_eq!(decision, ChaosDecision::Deliver);
        assert!(!policy.is_enabled());

        // Stats should still track
        assert_eq!(policy.stats().messages_total, 1);
    }

    #[test]
    fn test_configurable_policy_partition() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();

        // Create partition (enables chaos automatically via partition_active)
        policy.partition(1, 2);

        // Should return Partition decision across partition
        assert_eq!(policy.decide(&1, &2), ChaosDecision::Partition);
        assert_eq!(policy.decide(&2, &1), ChaosDecision::Partition);

        // Should deliver to non-partitioned nodes
        assert_eq!(policy.decide(&1, &3), ChaosDecision::Deliver);

        // Check stats
        let stats = policy.stats();
        assert_eq!(stats.messages_partitioned, 2);
        assert_eq!(stats.messages_total, 3);

        // Heal partition
        policy.heal(&1, &2);
        assert_eq!(policy.decide(&1, &2), ChaosDecision::Deliver);
    }

    #[test]
    fn test_configurable_policy_loss_rate() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();
        policy.set_loss_rate(1.0); // 100% loss

        // Should drop all messages
        assert_eq!(policy.decide(&1, &2), ChaosDecision::Drop);

        let stats = policy.stats();
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn test_configurable_policy_latency() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();
        policy.set_latency(Duration::from_millis(100));

        // Should deliver with latency
        let decision = policy.decide(&1, &2);
        match decision {
            ChaosDecision::Delay(d) => assert!(d >= Duration::from_millis(100)),
            _ => panic!("Expected Delay decision, got {:?}", decision),
        }

        let stats = policy.stats();
        assert_eq!(stats.messages_delayed, 1);
    }

    #[test]
    fn test_configurable_policy_latency_with_jitter() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();
        policy.set_latency(Duration::from_millis(100));
        policy.set_jitter(Duration::from_millis(50));

        // Should deliver with latency between 100-150ms
        let decision = policy.decide(&1, &2);
        match decision {
            ChaosDecision::Delay(latency) => {
                assert!(latency >= Duration::from_millis(100));
                assert!(latency <= Duration::from_millis(150));
            }
            _ => panic!("Expected Delay decision, got {:?}", decision),
        }
    }

    #[test]
    fn test_configurable_policy_isolate() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();

        // Isolate node 1 from nodes 2, 3, 4
        policy.isolate(1, vec![2, 3, 4]);

        assert!(policy.is_partitioned(&1, &2));
        assert!(policy.is_partitioned(&1, &3));
        assert!(policy.is_partitioned(&1, &4));

        // Other nodes can still communicate
        assert!(!policy.is_partitioned(&2, &3));
    }

    #[test]
    fn test_configurable_policy_split_brain() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();

        // Split into two groups
        policy.split_brain(vec![1, 2], vec![3, 4]);

        // Within group communication works
        assert!(!policy.is_partitioned(&1, &2));
        assert!(!policy.is_partitioned(&3, &4));

        // Cross-group communication blocked
        assert!(policy.is_partitioned(&1, &3));
        assert!(policy.is_partitioned(&1, &4));
        assert!(policy.is_partitioned(&2, &3));
        assert!(policy.is_partitioned(&2, &4));

        // Heal and verify
        policy.heal_all();
        assert!(!policy.is_partitioned(&1, &3));
    }

    #[test]
    fn test_reset_stats() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();
        policy.partition(1, 2);

        policy.decide(&1, &2);
        assert!(policy.stats().messages_partitioned > 0);

        policy.reset_stats();
        assert_eq!(policy.stats().messages_partitioned, 0);
        assert_eq!(policy.stats().messages_total, 0);
    }

    // =========================================================================
    // Deterministic Chaos Tests
    // =========================================================================

    #[test]
    fn test_deterministic_chaos_reproducible() {
        // Create two policies with same seed
        let config = ChaosConfig::deterministic(42).with_loss_rate(0.5);

        let policy1: ConfigurableChaosPolicy<u64> =
            ConfigurableChaosPolicy::with_config(config.clone());
        let policy2: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::with_config(config);

        // Run same sequence of decisions
        let mut decisions1 = Vec::new();
        let mut decisions2 = Vec::new();

        for i in 0..100u64 {
            decisions1.push(policy1.decide(&i, &(i + 1)).is_delivered());
            decisions2.push(policy2.decide(&i, &(i + 1)).is_delivered());
        }

        // Should produce identical sequences
        assert_eq!(decisions1, decisions2);
    }

    #[test]
    fn test_deterministic_chaos_different_seeds() {
        // Create two policies with different seeds
        let config1 = ChaosConfig::deterministic(42).with_loss_rate(0.5);
        let config2 = ChaosConfig::deterministic(123).with_loss_rate(0.5);

        let policy1: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::with_config(config1);
        let policy2: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::with_config(config2);

        // Run same sequence of decisions
        let mut decisions1 = Vec::new();
        let mut decisions2 = Vec::new();

        for i in 0..100u64 {
            decisions1.push(policy1.decide(&i, &(i + 1)).is_delivered());
            decisions2.push(policy2.decide(&i, &(i + 1)).is_delivered());
        }

        // Should produce different sequences (very unlikely to be identical)
        assert_ne!(decisions1, decisions2);
    }

    #[test]
    fn test_deterministic_jitter() {
        // Create two policies with same seed
        let config = ChaosConfig::deterministic(42)
            .with_latency(Duration::from_millis(100))
            .with_jitter(Duration::from_millis(50));

        let policy1: ConfigurableChaosPolicy<u64> =
            ConfigurableChaosPolicy::with_config(config.clone());
        let policy2: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::with_config(config);

        // Run same sequence of decisions
        let mut latencies1 = Vec::new();
        let mut latencies2 = Vec::new();

        for i in 0..10u64 {
            if let ChaosDecision::Delay(d) = policy1.decide(&i, &(i + 1)) {
                latencies1.push(d);
            }
            if let ChaosDecision::Delay(d) = policy2.decide(&i, &(i + 1)) {
                latencies2.push(d);
            }
        }

        // Should produce identical latency sequences
        assert_eq!(latencies1, latencies2);
    }

    // =========================================================================
    // Backward Compatibility Tests
    // =========================================================================

    #[test]
    fn test_backward_compat_should_deliver() {
        let policy: ConfigurableChaosPolicy<u64> = ConfigurableChaosPolicy::new();

        // should_deliver still works
        let result = policy.should_deliver(&1, &2);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Duration::ZERO);

        // With latency
        policy.set_latency(Duration::from_millis(50));
        let result = policy.should_deliver(&1, &2);
        assert!(result.is_some());
        assert!(result.unwrap() >= Duration::from_millis(50));

        // With partition
        policy.partition(1, 2);
        assert!(policy.should_deliver(&1, &2).is_none());
    }

    // =========================================================================
    // Factory Functions Tests
    // =========================================================================

    #[test]
    fn test_factory_functions() {
        let noop: Arc<NoopChaosPolicy> = noop_policy::<u64>();
        assert!(!ChaosPolicy::<u64>::is_enabled(noop.as_ref()));

        let configurable: Arc<ConfigurableChaosPolicy<u64>> = configurable_policy();
        assert!(!configurable.is_enabled());

        let with_config: Arc<ConfigurableChaosPolicy<u64>> =
            configurable_policy_with_config(ChaosConfig::moderate());
        assert!(with_config.is_enabled());
    }

    #[test]
    fn test_shared_chaos_policy_trait_object() {
        // Test that we can use trait objects
        let policies: Vec<SharedChaosPolicy<u64>> = vec![
            Arc::new(NoopChaosPolicy),
            Arc::new(ConfigurableChaosPolicy::new()),
        ];

        for policy in policies {
            let _ = policy.decide(&1, &2);
        }
    }
}
