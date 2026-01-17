//! Adaptive IHave batching based on network conditions.
//!
//! This module provides intelligent batch size adjustment based on:
//!
//! - **Network latency**: Smaller batches for low-latency, larger for high-latency
//! - **Graft success rate**: Reduce batches if many Grafts fail (indicates over-eager batching)
//! - **Message throughput**: Larger batches under high load to reduce packet overhead
//! - **Cluster size**: Scale batch size with cluster size
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::AdaptiveBatcher;
//!
//! let batcher = AdaptiveBatcher::new(BatcherConfig::default());
//!
//! // Record network observations
//! batcher.record_ihave_sent(16); // Sent a batch of 16
//! batcher.record_graft_received();  // Peer requested message
//! batcher.record_latency(Duration::from_millis(5));
//!
//! // Get recommended batch size
//! let batch_size = batcher.recommended_batch_size();
//! ```

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::RwLock;

/// Configuration for adaptive batching.
#[derive(Debug, Clone)]
pub struct BatcherConfig {
    /// Minimum batch size (floor).
    pub min_batch_size: usize,

    /// Maximum batch size (ceiling).
    pub max_batch_size: usize,

    /// Target batch size for moderate conditions.
    pub target_batch_size: usize,

    /// Latency threshold for low-latency optimization (ms).
    pub low_latency_threshold: Duration,

    /// Latency threshold for high-latency optimization (ms).
    pub high_latency_threshold: Duration,

    /// Graft success rate threshold for batch size reduction.
    pub min_graft_success_rate: f64,

    /// Message rate threshold for high-throughput mode (per second).
    pub high_throughput_threshold: f64,

    /// How often to recalculate the optimal batch size.
    pub recalc_interval: Duration,

    /// Smoothing factor for exponential moving average (0.0-1.0).
    /// Lower values = smoother, slower to respond.
    /// Higher values = more responsive, potentially jittery.
    pub ema_alpha: f64,

    /// Observation window for rate calculations.
    pub observation_window: Duration,

    /// Momentum factor for damping batch size changes (0.0-1.0).
    /// Higher values = more damping, smoother adjustments.
    pub momentum: f64,

    /// Hysteresis threshold to prevent oscillation (0.0-1.0).
    /// Batch size must change by at least this fraction to be applied.
    pub hysteresis: f64,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 4,
            max_batch_size: 64,
            target_batch_size: 16,
            low_latency_threshold: Duration::from_millis(10),
            high_latency_threshold: Duration::from_millis(100),
            min_graft_success_rate: 0.7,
            high_throughput_threshold: 100.0,
            recalc_interval: Duration::from_secs(5),
            ema_alpha: 0.2,
            observation_window: Duration::from_secs(30),
            momentum: 0.3,
            hysteresis: 0.1,
        }
    }
}

impl BatcherConfig {
    /// Create a new batcher configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configuration for LAN environments.
    ///
    /// Smaller batches for faster tree repair.
    pub fn lan() -> Self {
        Self {
            min_batch_size: 4,
            max_batch_size: 32,
            target_batch_size: 8,
            low_latency_threshold: Duration::from_millis(2),
            high_latency_threshold: Duration::from_millis(20),
            min_graft_success_rate: 0.8,
            high_throughput_threshold: 500.0,
            recalc_interval: Duration::from_secs(3),
            ema_alpha: 0.3,
            observation_window: Duration::from_secs(15),
            momentum: 0.4,
            hysteresis: 0.15,
        }
    }

    /// Configuration for WAN environments.
    ///
    /// Larger batches to amortize network overhead.
    pub fn wan() -> Self {
        Self {
            min_batch_size: 8,
            max_batch_size: 128,
            target_batch_size: 32,
            low_latency_threshold: Duration::from_millis(50),
            high_latency_threshold: Duration::from_millis(200),
            min_graft_success_rate: 0.6,
            high_throughput_threshold: 50.0,
            recalc_interval: Duration::from_secs(10),
            ema_alpha: 0.15,
            observation_window: Duration::from_secs(60),
            momentum: 0.25,
            hysteresis: 0.08,
        }
    }

    /// Configuration for large clusters (1000+ nodes).
    pub fn large_cluster() -> Self {
        Self {
            min_batch_size: 16,
            max_batch_size: 128,
            target_batch_size: 48,
            low_latency_threshold: Duration::from_millis(20),
            high_latency_threshold: Duration::from_millis(150),
            min_graft_success_rate: 0.65,
            high_throughput_threshold: 200.0,
            recalc_interval: Duration::from_secs(5),
            ema_alpha: 0.2,
            observation_window: Duration::from_secs(30),
            momentum: 0.3,
            hysteresis: 0.1,
        }
    }

    /// Set minimum batch size (builder pattern).
    pub const fn with_min_batch_size(mut self, size: usize) -> Self {
        self.min_batch_size = size;
        self
    }

    /// Set maximum batch size (builder pattern).
    pub const fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Set target batch size (builder pattern).
    pub const fn with_target_batch_size(mut self, size: usize) -> Self {
        self.target_batch_size = size;
        self
    }

    /// Set momentum factor (builder pattern).
    pub const fn with_momentum(mut self, momentum: f64) -> Self {
        self.momentum = momentum;
        self
    }

    /// Set hysteresis threshold (builder pattern).
    pub const fn with_hysteresis(mut self, hysteresis: f64) -> Self {
        self.hysteresis = hysteresis;
        self
    }

    /// Set observation window duration (builder pattern).
    pub const fn with_observation_window(mut self, window: Duration) -> Self {
        self.observation_window = window;
        self
    }

    /// Set recalculation interval (builder pattern).
    pub const fn with_recalc_interval(mut self, interval: Duration) -> Self {
        self.recalc_interval = interval;
        self
    }

    /// Set EMA alpha smoothing factor (builder pattern).
    pub const fn with_ema_alpha(mut self, alpha: f64) -> Self {
        self.ema_alpha = alpha;
        self
    }
}

/// Statistics about batching behavior.
#[derive(Debug, Clone)]
pub struct BatcherStats {
    /// Total IHaves sent.
    pub ihaves_sent: u64,

    /// Total Grafts observed (both success and failure).
    pub grafts_total: u64,

    /// Successful Grafts.
    pub grafts_success: u64,

    /// Failed Grafts.
    pub grafts_failed: u64,

    /// Current Graft success rate.
    pub graft_success_rate: f64,

    /// Current average latency (EMA).
    pub avg_latency: Duration,

    /// Current recommended batch size.
    pub current_batch_size: usize,

    /// Previous batch size (for tracking changes).
    pub previous_batch_size: usize,

    /// Message rate (per second).
    pub message_rate: f64,

    /// Number of batch size adjustments made.
    pub adjustments: u64,

    /// Batch size trend.
    pub trend: BatchSizeTrend,
}

/// Trend direction for batch size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchSizeTrend {
    /// Batch size is increasing.
    Increasing,
    /// Batch size is decreasing.
    Decreasing,
    /// Batch size is stable.
    Stable,
}

impl fmt::Display for BatcherStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BatcherStats {{ batch_size: {} ({}), graft_success: {:.1}% ({}/{}), \
             latency: {:.1}ms, msg_rate: {:.1}/s, adjustments: {} }}",
            self.current_batch_size,
            match self.trend {
                BatchSizeTrend::Increasing => "↑",
                BatchSizeTrend::Decreasing => "↓",
                BatchSizeTrend::Stable => "→",
            },
            self.graft_success_rate * 100.0,
            self.grafts_success,
            self.grafts_total,
            self.avg_latency.as_secs_f64() * 1000.0,
            self.message_rate,
            self.adjustments,
        )
    }
}

/// Internal state for adaptive batching.
#[derive(Debug)]
struct BatcherState {
    /// Observation start time.
    window_start: Instant,

    /// IHaves sent in current window.
    ihaves_sent: u64,

    /// Grafts triggered in current window.
    grafts_success: u64,
    grafts_failed: u64,

    /// Latency samples (for EMA calculation).
    latency_ema_ms: f64,
    latency_samples: u64,
    /// Last latency reset time (to prevent EMA from becoming too stiff).
    latency_reset_time: Instant,

    /// Messages processed in current window.
    messages_processed: u64,

    /// Current computed batch size.
    current_batch_size: usize,

    /// Previous batch size (for momentum calculation).
    previous_batch_size: usize,

    /// Last recalculation time.
    last_recalc: Instant,

    /// Total adjustments made.
    adjustments: u64,

    /// Historical totals (across windows).
    total_ihaves: u64,
    total_grafts_success: u64,
    total_grafts_failed: u64,
}

impl BatcherState {
    fn new(initial_batch_size: usize) -> Self {
        let now = Instant::now();
        Self {
            window_start: now,
            ihaves_sent: 0,
            grafts_success: 0,
            grafts_failed: 0,
            latency_ema_ms: 50.0, // Start with reasonable default
            latency_samples: 0,
            latency_reset_time: now,
            messages_processed: 0,
            current_batch_size: initial_batch_size,
            previous_batch_size: initial_batch_size,
            last_recalc: now,
            adjustments: 0,
            total_ihaves: 0,
            total_grafts_success: 0,
            total_grafts_failed: 0,
        }
    }

    fn reset_window(&mut self) {
        // Preserve totals
        self.total_ihaves = self.total_ihaves.saturating_add(self.ihaves_sent);
        self.total_grafts_success = self
            .total_grafts_success
            .saturating_add(self.grafts_success);
        self.total_grafts_failed = self.total_grafts_failed.saturating_add(self.grafts_failed);

        // Reset window counters
        self.window_start = Instant::now();
        self.ihaves_sent = 0;
        self.grafts_success = 0;
        self.grafts_failed = 0;
        self.messages_processed = 0;
    }

    fn should_reset_latency_ema(&self) -> bool {
        // Reset EMA every hour to prevent it from becoming too stiff
        self.latency_reset_time.elapsed() > Duration::from_secs(3600)
    }

    fn reset_latency_ema(&mut self) {
        self.latency_samples = 0;
        self.latency_reset_time = Instant::now();
    }
}

/// Adaptive batcher that adjusts IHave batch sizes based on network conditions.
#[derive(Debug)]
pub struct AdaptiveBatcher {
    config: BatcherConfig,
    state: RwLock<BatcherState>,
    /// Atomic counter for fast message recording without lock.
    message_counter: AtomicU64,
}

impl AdaptiveBatcher {
    /// Create a new adaptive batcher with the given configuration.
    pub fn new(config: BatcherConfig) -> Self {
        let initial_batch_size = config.target_batch_size;
        Self {
            config,
            state: RwLock::new(BatcherState::new(initial_batch_size)),
            message_counter: AtomicU64::new(0),
        }
    }

    /// Create an adaptive batcher with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(BatcherConfig::default())
    }

    /// Record that IHaves were sent.
    pub fn record_ihave_sent(&self, count: usize) {
        let mut state = self.state.write();
        state.ihaves_sent = state.ihaves_sent.saturating_add(count as u64);
    }

    /// Record a successful Graft (message was requested and received).
    pub fn record_graft_received(&self) {
        let mut state = self.state.write();
        state.grafts_success += 1;
    }

    /// Record a failed Graft (message request timed out or not found).
    pub fn record_graft_timeout(&self) {
        let mut state = self.state.write();
        state.grafts_failed += 1;
    }

    /// Alias for backward compatibility.
    #[deprecated(since = "0.2.0", note = "Use record_graft_received instead")]
    pub fn record_graft_success(&self) {
        self.record_graft_received();
    }

    /// Alias for backward compatibility.
    #[deprecated(since = "0.2.0", note = "Use record_graft_timeout instead")]
    pub fn record_graft_failed(&self) {
        self.record_graft_timeout();
    }

    /// Record observed network latency.
    pub fn record_latency(&self, latency: Duration) {
        let mut state = self.state.write();

        // Reset EMA if it's been too long (prevents stiffness)
        if state.should_reset_latency_ema() {
            state.reset_latency_ema();
        }

        let latency_ms = latency.as_secs_f64() * 1000.0;

        // Update EMA
        if state.latency_samples == 0 {
            state.latency_ema_ms = latency_ms;
        } else {
            state.latency_ema_ms = self.config.ema_alpha * latency_ms
                + (1.0 - self.config.ema_alpha) * state.latency_ema_ms;
        }
        state.latency_samples += 1;
    }

    /// Record a message being processed (fast path, uses atomic).
    pub fn record_message(&self) {
        self.message_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Record multiple messages being processed.
    pub fn record_messages(&self, count: u64) {
        self.message_counter.fetch_add(count, Ordering::Relaxed);
    }

    /// Record an IHave batch with its Graft results.
    ///
    /// This is a convenience method that combines `record_ihave_sent` and
    /// multiple graft result recordings.
    pub fn record_ihave_batch(&self, batch_size: usize, graft_results: &[bool]) {
        let mut state = self.state.write();

        state.ihaves_sent = state.ihaves_sent.saturating_add(batch_size as u64);

        for &success in graft_results {
            if success {
                state.grafts_success = state.grafts_success.saturating_add(1);
            } else {
                state.grafts_failed = state.grafts_failed.saturating_add(1);
            }
        }
    }

    /// Get the recommended batch size based on current conditions.
    ///
    /// This may trigger a recalculation if enough time has passed.
    /// Thread-safe: properly synchronizes atomic counter with state.
    pub fn recommended_batch_size(&self) -> usize {
        // Fast path: check if recalc is needed without full sync
        {
            let state = self.state.read();
            if state.last_recalc.elapsed() < self.config.recalc_interval {
                return state.current_batch_size;
            }
        }

        // Slow path: sync messages and recalculate
        // Acquire write lock once to sync atomic counter
        {
            let mut state = self.state.write();
            let messages = self.message_counter.swap(0, Ordering::Relaxed);
            if messages > 0 {
                state.messages_processed = state.messages_processed.saturating_add(messages);
            }
        }

        // Release lock before heavy computation
        self.recalculate_batch_size();

        self.state.read().current_batch_size
    }

    /// Force recalculation of the optimal batch size.
    fn recalculate_batch_size(&self) {
        let mut state = self.state.write();

        // Check and reset window first (at the beginning of recalc)
        if state.window_start.elapsed() >= self.config.observation_window {
            state.reset_window();
        }

        // Cache window elapsed calculation
        let window_elapsed = state.window_start.elapsed();
        let window_elapsed_secs = window_elapsed.as_secs_f64();

        // Calculate Graft success rate (require minimum sample size)
        let total_grafts = state.grafts_success + state.grafts_failed;
        let graft_success_rate = if total_grafts > 0 {
            state.grafts_success as f64 / total_grafts as f64
        } else {
            1.0 // Assume good if no data
        };

        // Calculate message rate
        let message_rate = if window_elapsed_secs > 0.0 {
            state.messages_processed as f64 / window_elapsed_secs
        } else {
            0.0
        };

        // Get current latency
        let latency_ms = state.latency_ema_ms;

        // Start with target batch size
        let mut batch_size = self.config.target_batch_size as f64;

        // Adjust based on latency
        let low_lat_ms = self.config.low_latency_threshold.as_secs_f64() * 1000.0;
        let high_lat_ms = self.config.high_latency_threshold.as_secs_f64() * 1000.0;

        if latency_ms < low_lat_ms {
            // Low latency: smaller batches for faster tree repair
            batch_size *= 0.75;
        } else if latency_ms > high_lat_ms {
            // High latency: larger batches to amortize overhead
            batch_size *= 1.5;
        }

        // Adjust based on Graft success rate (softer adjustment with deadband)
        if total_grafts > 20 && graft_success_rate < self.config.min_graft_success_rate {
            // Use softer scaling: 0.6 + 0.4 * (rate / min_rate)
            // This prevents too aggressive reductions
            let rate_ratio = graft_success_rate / self.config.min_graft_success_rate;
            let scale_factor = 0.6 + 0.4 * rate_ratio;
            batch_size *= scale_factor;
        }

        // Adjust based on message throughput (logarithmic scaling)
        if message_rate > self.config.high_throughput_threshold {
            // Use logarithmic scaling to prevent jumpy behavior
            let throughput_ratio = message_rate / self.config.high_throughput_threshold;
            let throughput_factor = 1.0 + throughput_ratio.ln().max(0.0) * 0.3;
            batch_size *= throughput_factor.min(1.8); // Cap at 1.8x
        }

        // Clamp to configured bounds before applying momentum
        let target_batch_size = (batch_size.round() as usize)
            .clamp(self.config.min_batch_size, self.config.max_batch_size);

        // Apply momentum (damping) to smooth transitions
        let momentum_batch_size = self.config.momentum * state.previous_batch_size as f64
            + (1.0 - self.config.momentum) * target_batch_size as f64;

        let new_batch_size = (momentum_batch_size.round() as usize)
            .clamp(self.config.min_batch_size, self.config.max_batch_size);

        // Apply hysteresis to prevent oscillation
        let change_ratio = if state.current_batch_size > 0 {
            ((new_batch_size as f64 - state.current_batch_size as f64).abs()
                / state.current_batch_size as f64)
                .max(0.0)
        } else {
            1.0 // Always apply if current is zero
        };

        if change_ratio >= self.config.hysteresis {
            // Record if adjustment was made
            if new_batch_size != state.current_batch_size {
                state.adjustments = state.adjustments.saturating_add(1);
            }

            state.previous_batch_size = state.current_batch_size;
            state.current_batch_size = new_batch_size;
        }

        state.last_recalc = Instant::now();
    }

    /// Get current statistics.
    pub fn stats(&self) -> BatcherStats {
        let state = self.state.read();

        let total_grafts = state.grafts_success + state.grafts_failed;
        let graft_success_rate = if total_grafts > 0 {
            state.grafts_success as f64 / total_grafts as f64
        } else {
            1.0
        };

        let window_elapsed = state.window_start.elapsed();
        let message_rate = if window_elapsed.as_secs_f64() > 0.0 {
            state.messages_processed as f64 / window_elapsed.as_secs_f64()
        } else {
            0.0
        };

        let trend = if state.current_batch_size > state.previous_batch_size {
            BatchSizeTrend::Increasing
        } else if state.current_batch_size < state.previous_batch_size {
            BatchSizeTrend::Decreasing
        } else {
            BatchSizeTrend::Stable
        };

        BatcherStats {
            ihaves_sent: state.total_ihaves.saturating_add(state.ihaves_sent),
            grafts_total: state
                .total_grafts_success
                .saturating_add(state.total_grafts_failed)
                .saturating_add(state.grafts_success)
                .saturating_add(state.grafts_failed),
            grafts_success: state
                .total_grafts_success
                .saturating_add(state.grafts_success),
            grafts_failed: state
                .total_grafts_failed
                .saturating_add(state.grafts_failed),
            graft_success_rate,
            avg_latency: Duration::from_secs_f64(state.latency_ema_ms / 1000.0),
            current_batch_size: state.current_batch_size,
            previous_batch_size: state.previous_batch_size,
            message_rate,
            adjustments: state.adjustments,
            trend,
        }
    }

    /// Reset all statistics.
    pub fn reset_stats(&self) {
        let mut state = self.state.write();
        *state = BatcherState::new(self.config.target_batch_size);
    }

    /// Get the configuration.
    pub fn config(&self) -> &BatcherConfig {
        &self.config
    }

    /// Get debug information about the batcher's current state.
    pub fn debug_info(&self) -> String {
        let state = self.state.read();
        let stats = self.stats();

        format!(
            "AdaptiveBatcher Debug Info:
  Current Batch Size: {} (prev: {}, trend: {:?})
  Latency EMA: {:.2}ms (samples: {})
  Graft Success Rate: {:.1}% ({}/{} total)
  Message Rate: {:.1}/sec
  Window Age: {:.1}s / {:.1}s
  Adjustments: {}
  Config: min={}, max={}, target={}",
            state.current_batch_size,
            state.previous_batch_size,
            stats.trend,
            state.latency_ema_ms,
            state.latency_samples,
            stats.graft_success_rate * 100.0,
            stats.grafts_success,
            stats.grafts_total,
            stats.message_rate,
            state.window_start.elapsed().as_secs_f64(),
            self.config.observation_window.as_secs_f64(),
            state.adjustments,
            self.config.min_batch_size,
            self.config.max_batch_size,
            self.config.target_batch_size,
        )
    }

    /// Update the cluster size hint for batch size scaling.
    ///
    /// Larger clusters may benefit from larger batches.
    pub fn set_cluster_size_hint(&self, node_count: usize) {
        // Scale target based on cluster size (log scale)
        // More nodes = more IHave traffic = benefit from larger batches
        let scale_factor = if node_count > 100 {
            1.0 + (node_count as f64 / 100.0).ln() * 0.2
        } else {
            1.0
        };

        let mut state = self.state.write();
        let scaled_target = (self.config.target_batch_size as f64 * scale_factor) as usize;
        let new_size = scaled_target.clamp(self.config.min_batch_size, self.config.max_batch_size);

        state.previous_batch_size = state.current_batch_size;
        state.current_batch_size = new_size;
    }
}

impl Default for AdaptiveBatcher {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl Clone for AdaptiveBatcher {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            state: RwLock::new(BatcherState::new(self.config.target_batch_size)),
            message_counter: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batcher_config_defaults() {
        let config = BatcherConfig::default();
        assert_eq!(config.min_batch_size, 4);
        assert_eq!(config.max_batch_size, 64);
        assert_eq!(config.target_batch_size, 16);
    }

    #[test]
    fn test_batcher_config_presets() {
        let lan = BatcherConfig::lan();
        assert!(lan.target_batch_size < BatcherConfig::default().target_batch_size);

        let wan = BatcherConfig::wan();
        assert!(wan.target_batch_size > BatcherConfig::default().target_batch_size);

        let large = BatcherConfig::large_cluster();
        assert!(large.target_batch_size > BatcherConfig::default().target_batch_size);
    }

    #[test]
    fn test_initial_batch_size() {
        let batcher = AdaptiveBatcher::with_defaults();
        // Initially should be target batch size
        assert_eq!(batcher.recommended_batch_size(), 16);
    }

    #[test]
    fn test_low_latency_reduces_batch() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Record very low latency
        for _ in 0..20 {
            batcher.record_latency(Duration::from_millis(2));
        }

        // Force recalculation
        batcher.recalculate_batch_size();

        let batch_size = batcher.recommended_batch_size();
        assert!(batch_size < 16, "Low latency should reduce batch size");
    }

    #[test]
    fn test_high_latency_increases_batch() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Record high latency
        for _ in 0..20 {
            batcher.record_latency(Duration::from_millis(200));
        }

        batcher.recalculate_batch_size();

        let batch_size = batcher.recommended_batch_size();
        assert!(batch_size > 16, "High latency should increase batch size");
    }

    #[test]
    fn test_low_graft_success_reduces_batch() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Record many failed Grafts
        for _ in 0..80 {
            batcher.record_graft_timeout();
        }
        for _ in 0..20 {
            batcher.record_graft_received();
        }

        batcher.recalculate_batch_size();

        let batch_size = batcher.recommended_batch_size();
        assert!(
            batch_size < 16,
            "Low Graft success should reduce batch size"
        );
    }

    #[test]
    fn test_high_throughput_increases_batch() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Record high message throughput - sync to state first
        {
            let mut state = batcher.state.write();
            state.messages_processed = 5000;
        }

        batcher.recalculate_batch_size();

        let batch_size = batcher.recommended_batch_size();
        assert!(
            batch_size > 16,
            "High throughput should increase batch size, got {}",
            batch_size
        );
    }

    #[test]
    fn test_batch_size_clamped() {
        let config = BatcherConfig::default()
            .with_min_batch_size(8)
            .with_max_batch_size(32);

        let batcher = AdaptiveBatcher::new(config);

        // Try to push it very low with low latency
        for _ in 0..100 {
            batcher.record_latency(Duration::from_micros(100));
        }
        batcher.recalculate_batch_size();

        let batch_size = batcher.recommended_batch_size();
        assert!(batch_size >= 8, "Batch size should not go below minimum");

        // Try to push it very high
        for _ in 0..100 {
            batcher.record_latency(Duration::from_secs(1));
        }
        batcher.record_messages(100000);
        batcher.recalculate_batch_size();

        let batch_size = batcher.recommended_batch_size();
        assert!(batch_size <= 32, "Batch size should not exceed maximum");
    }

    #[test]
    fn test_stats() {
        let batcher = AdaptiveBatcher::with_defaults();

        batcher.record_ihave_sent(10);
        batcher.record_graft_received();
        batcher.record_graft_received();
        batcher.record_graft_timeout();
        batcher.record_latency(Duration::from_millis(25));

        let stats = batcher.stats();
        assert_eq!(stats.ihaves_sent, 10);
        assert_eq!(stats.grafts_success, 2);
        assert_eq!(stats.grafts_failed, 1);
        assert_eq!(stats.grafts_total, 3);
    }

    #[test]
    fn test_cluster_size_scaling() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Small cluster - no scaling
        batcher.set_cluster_size_hint(50);
        let small_batch = batcher.recommended_batch_size();

        // Large cluster - should scale up
        batcher.set_cluster_size_hint(1000);
        let large_batch = batcher.recommended_batch_size();

        assert!(
            large_batch >= small_batch,
            "Large cluster should have >= batch size"
        );
    }

    #[test]
    fn test_reset_stats() {
        let batcher = AdaptiveBatcher::with_defaults();

        batcher.record_ihave_sent(100);
        batcher.record_graft_received();

        batcher.reset_stats();

        let stats = batcher.stats();
        assert_eq!(stats.ihaves_sent, 0);
        assert_eq!(stats.grafts_success, 0);
    }

    #[test]
    fn test_adjustment_counting() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Force an adjustment
        for _ in 0..20 {
            batcher.record_latency(Duration::from_millis(200));
        }
        batcher.recalculate_batch_size();

        let stats = batcher.stats();
        assert!(
            stats.adjustments >= 1,
            "Should have made at least one adjustment"
        );
    }

    #[test]
    fn test_ihave_batch_recording() {
        let batcher = AdaptiveBatcher::with_defaults();

        let graft_results = vec![true, true, false, true];
        batcher.record_ihave_batch(10, &graft_results);

        let stats = batcher.stats();
        assert_eq!(stats.ihaves_sent, 10);
        assert_eq!(stats.grafts_success, 3);
        assert_eq!(stats.grafts_failed, 1);
    }

    #[test]
    fn test_hysteresis_prevents_oscillation() {
        let config = BatcherConfig::default().with_hysteresis(0.2);
        let batcher = AdaptiveBatcher::new(config);

        let initial = batcher.recommended_batch_size();

        // Make a small change that shouldn't trigger adjustment
        batcher.record_latency(Duration::from_millis(12));
        batcher.recalculate_batch_size();

        let after = batcher.recommended_batch_size();
        assert_eq!(initial, after, "Hysteresis should prevent small changes");
    }

    #[test]
    fn test_momentum_smooths_transitions() {
        let config = BatcherConfig::default()
            .with_momentum(0.5)
            .with_hysteresis(0.0); // Disable hysteresis for this test
        let batcher = AdaptiveBatcher::new(config);

        let initial = batcher.recommended_batch_size();

        // Make a change that should be dampened by momentum
        for _ in 0..50 {
            batcher.record_latency(Duration::from_millis(300));
        }
        batcher.recalculate_batch_size();

        let after_first = batcher.recommended_batch_size();

        // Should have changed but not by full amount due to momentum
        assert!(after_first > initial, "Should increase with high latency");

        // Second recalc should move further
        batcher.recalculate_batch_size();
        let after_second = batcher.recommended_batch_size();

        assert!(
            after_second >= after_first,
            "Momentum should cause gradual adjustment"
        );
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let batcher = Arc::new(AdaptiveBatcher::with_defaults());
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent operations
        for i in 0..4 {
            let batcher_clone = Arc::clone(&batcher);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    batcher_clone.record_message();
                    batcher_clone.record_latency(Duration::from_millis(10 + (i * j) % 50));

                    if j % 10 == 0 {
                        batcher_clone.record_ihave_sent(8);
                    }

                    if j % 3 == 0 {
                        batcher_clone.record_graft_received();
                    } else if j % 7 == 0 {
                        batcher_clone.record_graft_timeout();
                    }

                    // Periodically get batch size (triggers recalc)
                    if j % 20 == 0 {
                        let _ = batcher_clone.recommended_batch_size();
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Should complete without panicking and have reasonable stats
        let stats = batcher.stats();
        assert!(stats.ihaves_sent > 0);
        assert!(stats.grafts_total > 0);
        assert!(stats.current_batch_size >= batcher.config.min_batch_size);
        assert!(stats.current_batch_size <= batcher.config.max_batch_size);
    }

    #[test]
    fn test_latency_ema_reset() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Record initial latency
        for _ in 0..100 {
            batcher.record_latency(Duration::from_millis(50));
        }

        let stats_before = batcher.stats();
        let latency_before = stats_before.avg_latency;

        // Simulate time passing (manipulate internal state for test)
        {
            let mut state = batcher.state.write();
            state.latency_reset_time = Instant::now() - Duration::from_secs(3700);
        }

        // Record new very different latency
        batcher.record_latency(Duration::from_millis(5));

        let stats_after = batcher.stats();
        let latency_after = stats_after.avg_latency;

        // After reset, new latency should have more impact
        assert!(
            (latency_after.as_millis() as i64 - 5).abs()
                < (latency_before.as_millis() as i64 - 5).abs(),
            "EMA reset should make new samples more impactful"
        );
    }

    #[test]
    fn test_window_reset() {
        let config = BatcherConfig::default().with_observation_window(Duration::from_millis(100));
        let batcher = AdaptiveBatcher::new(config);

        // Record some data
        batcher.record_ihave_sent(50);
        batcher.record_graft_received();
        batcher.record_messages(100);

        // Wait for window to expire
        std::thread::sleep(Duration::from_millis(150));

        // Trigger recalc which should reset window
        let _ = batcher.recommended_batch_size();

        // Check that window counters are in totals
        let stats = batcher.stats();
        assert!(
            stats.ihaves_sent >= 50,
            "Window data should be preserved in totals"
        );
    }

    #[test]
    fn test_softer_graft_adjustment() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Simulate 50% graft success (below threshold of 70%)
        for _ in 0..50 {
            batcher.record_graft_received();
        }
        for _ in 0..50 {
            batcher.record_graft_timeout();
        }

        {
            let mut state = batcher.state.write();
            state.messages_processed = 100;
        }

        batcher.recalculate_batch_size();
        let batch_size = batcher.recommended_batch_size();

        // With softer adjustment (0.5 + 0.5 * rate_ratio), minimum multiplier is 0.5
        // So batch size shouldn't drop below 8 (16 * 0.5 with some other factors)
        assert!(
            batch_size >= 6,
            "Softer adjustment should prevent extreme reductions, got {}",
            batch_size
        );
    }

    #[test]
    fn test_logarithmic_throughput_scaling() {
        let config = BatcherConfig::default();
        let batcher = AdaptiveBatcher::new(config);

        // Very high throughput
        {
            let mut state = batcher.state.write();
            state.messages_processed = 100000;
            state.window_start = Instant::now() - Duration::from_secs(1);
        }

        batcher.recalculate_batch_size();
        let batch_size = batcher.recommended_batch_size();

        // Should scale up but be capped reasonably (logarithmic + 1.8x cap)
        assert!(batch_size > 16, "High throughput should increase batch");
        assert!(
            batch_size <= 64,
            "Logarithmic scaling should prevent excessive growth"
        );
    }

    #[test]
    fn test_builder_pattern() {
        let config = BatcherConfig::default()
            .with_min_batch_size(10)
            .with_max_batch_size(50)
            .with_target_batch_size(25)
            .with_momentum(0.4)
            .with_hysteresis(0.15)
            .with_observation_window(Duration::from_secs(60))
            .with_recalc_interval(Duration::from_secs(10))
            .with_ema_alpha(0.25);

        assert_eq!(config.min_batch_size, 10);
        assert_eq!(config.max_batch_size, 50);
        assert_eq!(config.target_batch_size, 25);
        assert_eq!(config.momentum, 0.4);
        assert_eq!(config.hysteresis, 0.15);
        assert_eq!(config.observation_window, Duration::from_secs(60));
        assert_eq!(config.recalc_interval, Duration::from_secs(10));
        assert_eq!(config.ema_alpha, 0.25);
    }

    #[test]
    fn test_fast_path_avoids_recalc() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Get initial batch size (triggers recalc)
        let _ = batcher.recommended_batch_size();

        // Immediately get again - should use fast path
        let start = Instant::now();
        let _ = batcher.recommended_batch_size();
        let elapsed = start.elapsed();

        // Fast path should be very quick (< 1ms)
        assert!(
            elapsed < Duration::from_millis(1),
            "Fast path should be very quick"
        );
    }

    #[test]
    fn test_deprecated_methods_still_work() {
        let batcher = AdaptiveBatcher::with_defaults();

        #[allow(deprecated)]
        {
            batcher.record_graft_success();
            batcher.record_graft_failed();
        }

        let stats = batcher.stats();
        assert_eq!(stats.grafts_success, 1);
        assert_eq!(stats.grafts_failed, 1);
    }

    #[test]
    fn test_zero_messages_no_panic() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Should not panic with no data
        let _ = batcher.recommended_batch_size();
        let stats = batcher.stats();

        assert_eq!(stats.message_rate, 0.0);
        assert_eq!(stats.graft_success_rate, 1.0); // Assumes good when no data
    }

    #[test]
    fn test_extreme_latency_values() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Very low latency
        batcher.record_latency(Duration::from_micros(1));

        // Very high latency
        batcher.record_latency(Duration::from_secs(10));

        // Should handle without panicking
        batcher.recalculate_batch_size();
        let batch_size = batcher.recommended_batch_size();

        assert!(batch_size >= batcher.config.min_batch_size);
        assert!(batch_size <= batcher.config.max_batch_size);
    }

    #[test]
    fn test_batch_size_trend() {
        let batcher = AdaptiveBatcher::with_defaults();

        let initial_stats = batcher.stats();
        assert_eq!(initial_stats.trend, BatchSizeTrend::Stable);

        // Force increase
        for _ in 0..50 {
            batcher.record_latency(Duration::from_millis(300));
        }
        batcher.recalculate_batch_size();

        let stats = batcher.stats();
        if stats.current_batch_size > stats.previous_batch_size {
            assert_eq!(stats.trend, BatchSizeTrend::Increasing);
        }
    }

    #[test]
    fn test_debug_info() {
        let batcher = AdaptiveBatcher::with_defaults();

        batcher.record_latency(Duration::from_millis(25));
        batcher.record_ihave_sent(10);
        batcher.record_graft_received();

        let debug = batcher.debug_info();

        assert!(debug.contains("Current Batch Size"));
        assert!(debug.contains("Latency EMA"));
        assert!(debug.contains("Graft Success Rate"));
    }

    #[test]
    fn test_saturating_arithmetic() {
        let batcher = AdaptiveBatcher::with_defaults();

        // This should not panic even with very large numbers
        for _ in 0..1000 {
            batcher.record_ihave_sent(usize::MAX / 2000);
            batcher.record_messages(u64::MAX / 2000);
        }

        let stats = batcher.stats();
        // Should have saturated rather than overflowed
        assert!(stats.ihaves_sent > 0);
    }

    #[test]
    fn test_graft_deadband() {
        let batcher = AdaptiveBatcher::with_defaults();

        // Record only a few grafts (below deadband of 20)
        for _ in 0..5 {
            batcher.record_graft_timeout();
        }
        for _ in 0..5 {
            batcher.record_graft_received();
        }

        let initial = batcher.recommended_batch_size();
        batcher.recalculate_batch_size();
        let after = batcher.recommended_batch_size();

        // Should not adjust based on so few samples
        assert_eq!(
            initial, after,
            "Should not adjust with only {} grafts (deadband is 20)",
            10
        );
    }

    #[test]
    fn test_softer_graft_penalty() {
        let batcher = AdaptiveBatcher::with_defaults();

        // 50% success rate (well below 70% threshold)
        for _ in 0..50 {
            batcher.record_graft_received();
        }
        for _ in 0..50 {
            batcher.record_graft_timeout();
        }

        {
            let mut state = batcher.state.write();
            state.messages_processed = 100;
        }

        batcher.recalculate_batch_size();
        let batch_size = batcher.recommended_batch_size();

        // With 0.6 + 0.4 * rate_ratio formula:
        // rate_ratio = 0.5/0.7 ≈ 0.714
        // scale = 0.6 + 0.4 * 0.714 ≈ 0.886
        // So batch shouldn't drop below ~14 (16 * 0.886)
        assert!(
            batch_size >= 10,
            "Softer penalty (0.6+0.4*rate) should prevent extreme drops, got {}",
            batch_size
        );
    }

    #[test]
    fn test_stats_display() {
        let batcher = AdaptiveBatcher::with_defaults();

        batcher.record_ihave_sent(20);
        batcher.record_graft_received();
        batcher.record_graft_received();
        batcher.record_graft_timeout();

        let stats = batcher.stats();
        let display = format!("{}", stats);

        assert!(display.contains("batch_size"));
        assert!(display.contains("graft_success"));
        assert!(display.contains("%"));
    }
}
