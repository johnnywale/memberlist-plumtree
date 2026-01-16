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
    collections::HashSet,
    hash::Hash,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
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
}
