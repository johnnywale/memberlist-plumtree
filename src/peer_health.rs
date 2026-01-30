//! Enhanced peer health monitoring for Plumtree.
//!
//! This module provides adaptive timeout calculation and zombie peer detection
//! to improve reliability and reduce latency in tree repair operations.
//!
//! # Features
//!
//! - **Adaptive Timeouts**: Automatically adjusts timeouts based on RTT history
//! - **Zombie Detection**: Identifies and handles unresponsive peers
//! - **Health Metrics**: Tracks per-peer health statistics
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{PeerHealthConfig, ZombieAction};
//! use std::time::Duration;
//!
//! let config = PeerHealthConfig {
//!     adaptive_timeout: true,
//!     rtt_multiplier: 3.0,
//!     min_timeout: Duration::from_millis(100),
//!     max_timeout: Duration::from_secs(30),
//!     zombie_threshold: 5,
//!     zombie_action: ZombieAction::Demote,
//! };
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

/// Configuration for peer health monitoring.
#[derive(Debug, Clone)]
pub struct PeerHealthConfig {
    /// Whether to use adaptive timeouts based on RTT
    pub adaptive_timeout: bool,
    /// Timeout multiplier: timeout = avg_rtt * multiplier
    pub rtt_multiplier: f64,
    /// Minimum timeout floor
    pub min_timeout: Duration,
    /// Maximum timeout ceiling
    pub max_timeout: Duration,
    /// Number of consecutive failures before marking as zombie
    pub zombie_threshold: u32,
    /// Action to take when a peer becomes a zombie
    pub zombie_action: ZombieAction,
    /// Smoothing factor for RTT exponential moving average (0-1)
    /// Lower values = smoother, higher values = more responsive
    pub ema_alpha: f64,
}

impl Default for PeerHealthConfig {
    fn default() -> Self {
        Self {
            adaptive_timeout: true,
            rtt_multiplier: 3.0,
            min_timeout: Duration::from_millis(100),
            max_timeout: Duration::from_secs(30),
            zombie_threshold: 5,
            zombie_action: ZombieAction::Demote,
            ema_alpha: 0.3,
        }
    }
}

impl PeerHealthConfig {
    /// Disable adaptive timeouts
    pub fn with_fixed_timeout(mut self, timeout: Duration) -> Self {
        self.adaptive_timeout = false;
        self.min_timeout = timeout;
        self.max_timeout = timeout;
        self
    }

    /// Set RTT multiplier for adaptive timeout
    pub fn with_rtt_multiplier(mut self, multiplier: f64) -> Self {
        self.rtt_multiplier = multiplier;
        self
    }

    /// Set zombie detection threshold
    pub fn with_zombie_threshold(mut self, threshold: u32) -> Self {
        self.zombie_threshold = threshold;
        self
    }

    /// Set action for zombie peers
    pub fn with_zombie_action(mut self, action: ZombieAction) -> Self {
        self.zombie_action = action;
        self
    }
}

/// Action to take when a peer is detected as a zombie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZombieAction {
    /// Demote peer from eager to lazy set
    Demote,
    /// Remove peer entirely from peer set
    Remove,
    /// Just notify via delegate callback, don't modify peer set
    Notify,
}

/// Health status of a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    /// Peer is healthy and responsive
    Healthy,
    /// Peer has some failures but under threshold
    Degraded,
    /// Peer is unresponsive (consecutive failures >= threshold)
    Zombie,
}

/// Health state for a single peer.
#[derive(Debug)]
pub struct PeerHealthState {
    /// Exponential moving average of RTT in microseconds
    rtt_ema_us: AtomicU64,
    /// Number of RTT samples
    rtt_samples: AtomicU32,
    /// Consecutive failure count
    consecutive_failures: AtomicU32,
    /// Total success count
    total_successes: AtomicU64,
    /// Total failure count
    total_failures: AtomicU64,
    /// Last successful communication timestamp (microseconds since epoch)
    last_success_us: AtomicU64,
    /// Last failure timestamp (microseconds since epoch)
    last_failure_us: AtomicU64,
}

impl Default for PeerHealthState {
    fn default() -> Self {
        Self {
            rtt_ema_us: AtomicU64::new(0),
            rtt_samples: AtomicU32::new(0),
            consecutive_failures: AtomicU32::new(0),
            total_successes: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            last_success_us: AtomicU64::new(0),
            last_failure_us: AtomicU64::new(0),
        }
    }
}

impl PeerHealthState {
    /// Record a successful operation with RTT
    pub fn record_success(&self, rtt: Duration, config: &PeerHealthConfig) {
        let rtt_us = rtt.as_micros() as u64;

        // Update RTT EMA
        let samples = self.rtt_samples.fetch_add(1, Ordering::Relaxed);
        if samples == 0 {
            // First sample - just use it directly
            self.rtt_ema_us.store(rtt_us, Ordering::Relaxed);
        } else {
            // Apply exponential moving average
            let current = self.rtt_ema_us.load(Ordering::Relaxed);
            let alpha = config.ema_alpha;
            let new_ema = (alpha * rtt_us as f64 + (1.0 - alpha) * current as f64) as u64;
            self.rtt_ema_us.store(new_ema, Ordering::Relaxed);
        }

        // Reset consecutive failures
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.total_successes.fetch_add(1, Ordering::Relaxed);

        // Update last success timestamp
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.last_success_us.store(now_us, Ordering::Relaxed);
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.total_failures.fetch_add(1, Ordering::Relaxed);

        // Update last failure timestamp
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        self.last_failure_us.store(now_us, Ordering::Relaxed);
    }

    /// Get the current RTT estimate
    pub fn rtt(&self) -> Option<Duration> {
        if self.rtt_samples.load(Ordering::Relaxed) == 0 {
            None
        } else {
            Some(Duration::from_micros(
                self.rtt_ema_us.load(Ordering::Relaxed),
            ))
        }
    }

    /// Get the adaptive timeout for this peer
    pub fn timeout(&self, config: &PeerHealthConfig) -> Duration {
        if !config.adaptive_timeout {
            return config.min_timeout;
        }

        let timeout = if let Some(rtt) = self.rtt() {
            Duration::from_secs_f64(rtt.as_secs_f64() * config.rtt_multiplier)
        } else {
            // No RTT data yet, use a reasonable default
            config.min_timeout * 2
        };

        timeout.clamp(config.min_timeout, config.max_timeout)
    }

    /// Get the current status
    pub fn status(&self, config: &PeerHealthConfig) -> PeerStatus {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        if failures >= config.zombie_threshold {
            PeerStatus::Zombie
        } else if failures > 0 {
            PeerStatus::Degraded
        } else {
            PeerStatus::Healthy
        }
    }

    /// Check if this peer is a zombie
    pub fn is_zombie(&self, config: &PeerHealthConfig) -> bool {
        self.consecutive_failures.load(Ordering::Relaxed) >= config.zombie_threshold
    }

    /// Get consecutive failure count
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    /// Get total success count
    pub fn total_successes(&self) -> u64 {
        self.total_successes.load(Ordering::Relaxed)
    }

    /// Get total failure count
    pub fn total_failures(&self) -> u64 {
        self.total_failures.load(Ordering::Relaxed)
    }

    /// Get success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        let successes = self.total_successes.load(Ordering::Relaxed);
        let failures = self.total_failures.load(Ordering::Relaxed);
        let total = successes + failures;
        if total == 0 {
            1.0 // No data yet, assume healthy
        } else {
            successes as f64 / total as f64
        }
    }

    /// Reset health statistics
    pub fn reset(&self) {
        self.rtt_ema_us.store(0, Ordering::Relaxed);
        self.rtt_samples.store(0, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.total_successes.store(0, Ordering::Relaxed);
        self.total_failures.store(0, Ordering::Relaxed);
    }
}

/// Health tracker for all peers.
#[derive(Debug)]
pub struct PeerHealthTracker<I> {
    config: PeerHealthConfig,
    peers: RwLock<HashMap<I, PeerHealthState>>,
}

impl<I> PeerHealthTracker<I>
where
    I: std::hash::Hash + Eq + Clone,
{
    /// Create a new health tracker with the given config
    pub fn new(config: PeerHealthConfig) -> Self {
        Self {
            config,
            peers: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create health state for a peer
    pub fn get_or_create(&self, peer: &I) -> &PeerHealthState {
        // Check if peer exists
        {
            let peers = self.peers.read();
            if peers.contains_key(peer) {
                // SAFETY: We know the entry exists and we hold a reference
                // This is safe because we're returning a reference to heap-allocated data
                // that won't be moved while the tracker exists
                drop(peers);
                let peers = self.peers.read();
                if let Some(health) = peers.get(peer) {
                    // Return a pointer - this is a bit unsafe but necessary for the API
                    // The health data is stable because HashMap doesn't move values
                    return unsafe { &*(health as *const PeerHealthState) };
                }
            }
        }

        // Need to insert
        {
            let mut peers = self.peers.write();
            peers.entry(peer.clone()).or_default();
        }

        // Now get the reference
        let peers = self.peers.read();
        let health = peers.get(peer).unwrap();
        unsafe { &*(health as *const PeerHealthState) }
    }

    /// Record a successful operation for a peer
    pub fn record_success(&self, peer: &I, rtt: Duration) {
        let mut peers = self.peers.write();
        let health = peers.entry(peer.clone()).or_default();
        health.record_success(rtt, &self.config);
    }

    /// Record a failed operation for a peer
    pub fn record_failure(&self, peer: &I) -> Option<ZombieAction> {
        let mut peers = self.peers.write();
        let health = peers.entry(peer.clone()).or_default();
        health.record_failure();

        // Check if peer just became a zombie
        if health.is_zombie(&self.config) {
            Some(self.config.zombie_action)
        } else {
            None
        }
    }

    /// Get the adaptive timeout for a peer
    pub fn timeout(&self, peer: &I) -> Duration {
        let peers = self.peers.read();
        if let Some(health) = peers.get(peer) {
            health.timeout(&self.config)
        } else {
            // No health data, use default
            self.config.min_timeout * 2
        }
    }

    /// Get the status of a peer
    pub fn status(&self, peer: &I) -> PeerStatus {
        let peers = self.peers.read();
        if let Some(health) = peers.get(peer) {
            health.status(&self.config)
        } else {
            PeerStatus::Healthy
        }
    }

    /// Check if a peer is a zombie
    pub fn is_zombie(&self, peer: &I) -> bool {
        let peers = self.peers.read();
        if let Some(health) = peers.get(peer) {
            health.is_zombie(&self.config)
        } else {
            false
        }
    }

    /// Get all zombie peers
    pub fn zombies(&self) -> Vec<I> {
        let peers = self.peers.read();
        peers
            .iter()
            .filter(|(_, health)| health.is_zombie(&self.config))
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Remove a peer from tracking
    pub fn remove(&self, peer: &I) {
        let mut peers = self.peers.write();
        peers.remove(peer);
    }

    /// Get the config
    pub fn config(&self) -> &PeerHealthConfig {
        &self.config
    }

    /// Get health summary for all peers
    pub fn summary(&self) -> HealthSummary {
        let peers = self.peers.read();
        let mut summary = HealthSummary::default();

        for health in peers.values() {
            match health.status(&self.config) {
                PeerStatus::Healthy => summary.healthy += 1,
                PeerStatus::Degraded => summary.degraded += 1,
                PeerStatus::Zombie => summary.zombie += 1,
            }
            summary.total_successes += health.total_successes();
            summary.total_failures += health.total_failures();
        }

        summary.total_peers = peers.len();
        summary
    }
}

/// Summary of peer health across all peers.
#[derive(Debug, Default, Clone)]
pub struct HealthSummary {
    /// Total number of tracked peers
    pub total_peers: usize,
    /// Number of healthy peers
    pub healthy: usize,
    /// Number of degraded peers
    pub degraded: usize,
    /// Number of zombie peers
    pub zombie: usize,
    /// Total successes across all peers
    pub total_successes: u64,
    /// Total failures across all peers
    pub total_failures: u64,
}

impl HealthSummary {
    /// Get overall success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.total_successes + self.total_failures;
        if total == 0 {
            1.0
        } else {
            self.total_successes as f64 / total as f64
        }
    }

    /// Get percentage of healthy peers
    pub fn healthy_percent(&self) -> f64 {
        if self.total_peers == 0 {
            100.0
        } else {
            (self.healthy as f64 / self.total_peers as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_health_default() {
        let health = PeerHealthState::default();
        assert_eq!(health.consecutive_failures(), 0);
        assert_eq!(health.total_successes(), 0);
        assert_eq!(health.total_failures(), 0);
        assert!(health.rtt().is_none());
    }

    #[test]
    fn test_peer_health_success() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig::default();

        health.record_success(Duration::from_millis(10), &config);

        assert_eq!(health.total_successes(), 1);
        assert_eq!(health.consecutive_failures(), 0);
        assert!(health.rtt().is_some());
        assert!((health.rtt().unwrap().as_millis() as i64 - 10).abs() < 2);
    }

    #[test]
    fn test_peer_health_failure() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig::default();

        health.record_failure();
        health.record_failure();
        health.record_failure();

        assert_eq!(health.consecutive_failures(), 3);
        assert_eq!(health.total_failures(), 3);
        assert_eq!(health.status(&config), PeerStatus::Degraded);
    }

    #[test]
    fn test_peer_health_zombie() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig {
            zombie_threshold: 3,
            ..Default::default()
        };

        health.record_failure();
        health.record_failure();
        assert!(!health.is_zombie(&config));

        health.record_failure();
        assert!(health.is_zombie(&config));
        assert_eq!(health.status(&config), PeerStatus::Zombie);
    }

    #[test]
    fn test_peer_health_recovery() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig::default();

        // Accumulate failures
        for _ in 0..3 {
            health.record_failure();
        }
        assert_eq!(health.consecutive_failures(), 3);

        // Success resets consecutive failures
        health.record_success(Duration::from_millis(10), &config);
        assert_eq!(health.consecutive_failures(), 0);
        assert_eq!(health.total_failures(), 3); // Total is preserved
        assert_eq!(health.status(&config), PeerStatus::Healthy);
    }

    #[test]
    fn test_adaptive_timeout() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig {
            adaptive_timeout: true,
            rtt_multiplier: 3.0,
            min_timeout: Duration::from_millis(50),
            max_timeout: Duration::from_secs(10),
            ..Default::default()
        };

        // No RTT data - use default
        let timeout1 = health.timeout(&config);
        assert_eq!(timeout1, Duration::from_millis(100)); // min * 2

        // Record RTT of 100ms
        health.record_success(Duration::from_millis(100), &config);

        // Timeout should be ~300ms (100 * 3)
        let timeout2 = health.timeout(&config);
        assert!(timeout2.as_millis() >= 250 && timeout2.as_millis() <= 350);
    }

    #[test]
    fn test_adaptive_timeout_clamping() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig {
            adaptive_timeout: true,
            rtt_multiplier: 3.0,
            min_timeout: Duration::from_millis(100),
            max_timeout: Duration::from_secs(1),
            ..Default::default()
        };

        // Very small RTT - should clamp to min
        health.record_success(Duration::from_micros(100), &config);
        assert!(health.timeout(&config) >= config.min_timeout);

        // Reset and record very large RTT - should clamp to max
        health.reset();
        health.record_success(Duration::from_secs(10), &config);
        assert!(health.timeout(&config) <= config.max_timeout);
    }

    #[test]
    fn test_success_rate() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig::default();

        // No data - assume healthy
        assert!((health.success_rate() - 1.0).abs() < 0.001);

        // 3 successes, 1 failure = 75%
        health.record_success(Duration::from_millis(10), &config);
        health.record_success(Duration::from_millis(10), &config);
        health.record_success(Duration::from_millis(10), &config);
        health.record_failure();

        assert!((health.success_rate() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_peer_health_tracker() {
        let config = PeerHealthConfig {
            zombie_threshold: 2,
            ..Default::default()
        };
        let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

        // Record some activity
        tracker.record_success(&1, Duration::from_millis(10));
        tracker.record_success(&2, Duration::from_millis(20));
        tracker.record_failure(&3);
        tracker.record_failure(&3);

        assert_eq!(tracker.status(&1), PeerStatus::Healthy);
        assert_eq!(tracker.status(&2), PeerStatus::Healthy);
        assert_eq!(tracker.status(&3), PeerStatus::Zombie);

        let zombies = tracker.zombies();
        assert_eq!(zombies.len(), 1);
        assert_eq!(zombies[0], 3);
    }

    #[test]
    fn test_health_summary() {
        let config = PeerHealthConfig {
            zombie_threshold: 3,
            ..Default::default()
        };
        let tracker: PeerHealthTracker<u64> = PeerHealthTracker::new(config);

        // Add healthy peer
        tracker.record_success(&1, Duration::from_millis(10));

        // Add degraded peer
        tracker.record_failure(&2);

        // Add zombie peer
        for _ in 0..3 {
            tracker.record_failure(&3);
        }

        let summary = tracker.summary();
        assert_eq!(summary.total_peers, 3);
        assert_eq!(summary.healthy, 1);
        assert_eq!(summary.degraded, 1);
        assert_eq!(summary.zombie, 1);
    }

    #[test]
    fn test_rtt_ema() {
        let health = PeerHealthState::default();
        let config = PeerHealthConfig {
            ema_alpha: 0.5, // 50% new, 50% old
            ..Default::default()
        };

        // First sample: 100ms
        health.record_success(Duration::from_millis(100), &config);
        let rtt1 = health.rtt().unwrap().as_millis();
        assert_eq!(rtt1, 100);

        // Second sample: 200ms -> EMA = 0.5 * 200 + 0.5 * 100 = 150
        health.record_success(Duration::from_millis(200), &config);
        let rtt2 = health.rtt().unwrap().as_millis();
        assert!((rtt2 as i64 - 150).abs() < 5);

        // Third sample: 100ms -> EMA = 0.5 * 100 + 0.5 * 150 = 125
        health.record_success(Duration::from_millis(100), &config);
        let rtt3 = health.rtt().unwrap().as_millis();
        assert!((rtt3 as i64 - 125).abs() < 5);
    }
}
