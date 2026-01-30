//! Dynamic cleanup tuning based on system load.
//!
//! This module provides adaptive cleanup behavior that adjusts based on:
//!
//! - **Cache utilization**: Clean more aggressively when cache is near capacity
//! - **Message rate**: Be more conservative when under high load
//! - **Cleanup duration**: Adjust batch sizes to keep cleanup fast
//! - **Removal efficiency**: Scan more when hit rate is low, less when high
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::CleanupTuner;
//!
//! let tuner = CleanupTuner::new(CleanupConfig::default());
//!
//! // Batch recording for efficiency (recommended for throughput > 10k msg/s)
//! tuner.record_messages(50);
//!
//! // Get tuned parameters before cleanup
//! let params = tuner.get_parameters(cache_utilization, Duration::from_secs(300));
//!
//! // After cleanup completes
//! tuner.record_cleanup(duration, entries_removed, &params);
//! ```
//!
//! # Performance Notes
//!
//! - `record_message()` and `record_messages()` are fully lock-free (atomic only)
//! - The mutex is only acquired when calculating parameters or reading stats
//! - For >50k msg/s, batch every 100-200 messages with `record_messages()`

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;

/// Configuration for dynamic cleanup tuning.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Base cleanup interval (when utilization is moderate).
    pub base_interval: Duration,

    /// Minimum cleanup interval (under high utilization).
    pub min_interval: Duration,

    /// Maximum cleanup interval (under low utilization).
    pub max_interval: Duration,

    /// Cache utilization threshold for aggressive cleanup (0.0-1.0).
    pub high_utilization_threshold: f64,

    /// Cache utilization threshold for relaxed cleanup (0.0-1.0).
    pub low_utilization_threshold: f64,

    /// Hysteresis for utilization thresholds (prevents flapping).
    /// Applied as ± adjustment to thresholds when already in a state.
    pub utilization_hysteresis: f64,

    /// Utilization threshold for DropSome backpressure (0.0-1.0).
    /// Default: 0.93
    pub drop_some_threshold: f64,

    /// Utilization threshold for DropMost backpressure (0.0-1.0).
    /// Default: 0.96
    pub drop_most_threshold: f64,

    /// Utilization threshold for BlockNew backpressure (0.0-1.0).
    /// Default: 0.98
    pub block_new_threshold: f64,

    /// Target cleanup duration (try to keep cleanup under this).
    pub target_cleanup_duration: Duration,

    /// Message rate threshold for high load (messages per second).
    pub high_load_message_rate: f64,

    /// Window size for calculating message rate.
    pub rate_window: Duration,

    /// Batch size smoothing factor (0.0-1.0). Higher = more responsive to changes.
    /// Recommended: 0.1-0.2 for production stability.
    pub batch_size_smoothing: f64,

    /// Cleanup interval should be at least TTL divided by this value.
    /// Default is 4, meaning cleanup runs at least 4 times per TTL period.
    /// Valid range: 2.0-20.0 (enforced at construction).
    pub ttl_divisor: f64,

    /// Weight for removal efficiency in batch size calculation (0.0-1.0).
    /// Higher values make batch size more responsive to how many entries
    /// are actually being removed vs just scanned.
    pub efficiency_weight: f64,

    /// Smoothing factor for efficiency EMA (0.0-1.0).
    /// Lower = smoother/slower to respond, higher = more responsive.
    pub efficiency_smoothing: f64,

    /// Minimum cleanup cycles before efficiency data is trusted.
    /// Prevents wild adjustments during startup.
    pub min_cycles_for_efficiency: u64,

    /// Maximum TTL-based interval multiplier to prevent huge intervals.
    /// If TTL is very large, interval is capped at max_interval * this factor.
    pub max_ttl_interval_factor: f64,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            base_interval: Duration::from_secs(30),
            min_interval: Duration::from_secs(5),
            max_interval: Duration::from_secs(120),
            high_utilization_threshold: 0.8,
            low_utilization_threshold: 0.3,
            utilization_hysteresis: 0.03,
            drop_some_threshold: 0.93,
            drop_most_threshold: 0.96,
            block_new_threshold: 0.98,
            target_cleanup_duration: Duration::from_millis(50),
            high_load_message_rate: 1000.0,
            rate_window: Duration::from_secs(10),
            batch_size_smoothing: 0.15,
            ttl_divisor: 4.0,
            efficiency_weight: 0.3,
            efficiency_smoothing: 0.2,
            min_cycles_for_efficiency: 3,
            max_ttl_interval_factor: 2.0,
        }
    }
}

impl CleanupConfig {
    /// Create a new cleanup config with default values.
    pub fn new() -> Self {
        let mut config = Self::default();
        config.validate();
        config
    }

    /// Configuration for high-throughput scenarios.
    ///
    /// More aggressive cleanup to prevent memory pressure.
    pub fn high_throughput() -> Self {
        let mut config = Self {
            base_interval: Duration::from_secs(15),
            min_interval: Duration::from_secs(2),
            max_interval: Duration::from_secs(60),
            high_utilization_threshold: 0.7,
            low_utilization_threshold: 0.2,
            utilization_hysteresis: 0.04,
            drop_some_threshold: 0.90,
            drop_most_threshold: 0.94,
            block_new_threshold: 0.97,
            target_cleanup_duration: Duration::from_millis(25),
            high_load_message_rate: 5000.0,
            rate_window: Duration::from_secs(5),
            batch_size_smoothing: 0.2,
            ttl_divisor: 4.0,
            efficiency_weight: 0.4,
            efficiency_smoothing: 0.25,
            min_cycles_for_efficiency: 2,
            max_ttl_interval_factor: 1.5,
        };
        config.validate();
        config
    }

    /// Configuration for low-latency scenarios.
    ///
    /// Smaller, more frequent cleanups to minimize impact.
    pub fn low_latency() -> Self {
        let mut config = Self {
            base_interval: Duration::from_secs(20),
            min_interval: Duration::from_secs(5),
            max_interval: Duration::from_secs(60),
            high_utilization_threshold: 0.75,
            low_utilization_threshold: 0.25,
            utilization_hysteresis: 0.03,
            drop_some_threshold: 0.93,
            drop_most_threshold: 0.96,
            block_new_threshold: 0.98,
            target_cleanup_duration: Duration::from_millis(10),
            high_load_message_rate: 500.0,
            rate_window: Duration::from_secs(5),
            batch_size_smoothing: 0.1,
            ttl_divisor: 4.0,
            efficiency_weight: 0.2,
            efficiency_smoothing: 0.15,
            min_cycles_for_efficiency: 4,
            max_ttl_interval_factor: 2.0,
        };
        config.validate();
        config
    }

    /// Validate and clamp configuration values to safe ranges.
    fn validate(&mut self) {
        self.high_utilization_threshold = self.high_utilization_threshold.clamp(0.5, 0.99);
        self.low_utilization_threshold = self.low_utilization_threshold.clamp(0.01, 0.5);
        self.utilization_hysteresis = self.utilization_hysteresis.clamp(0.01, 0.1);
        self.batch_size_smoothing = self.batch_size_smoothing.clamp(0.05, 0.5);
        self.ttl_divisor = self.ttl_divisor.clamp(2.0, 20.0);
        self.efficiency_weight = self.efficiency_weight.clamp(0.0, 1.0);
        self.efficiency_smoothing = self.efficiency_smoothing.clamp(0.05, 0.5);
        self.min_cycles_for_efficiency = self.min_cycles_for_efficiency.clamp(1, 10);
        self.max_ttl_interval_factor = self.max_ttl_interval_factor.clamp(1.0, 5.0);

        // Ensure thresholds are properly ordered
        if self.high_utilization_threshold <= self.low_utilization_threshold {
            self.high_utilization_threshold = self.low_utilization_threshold + 0.2;
        }

        // Ensure backpressure thresholds are ordered
        self.drop_some_threshold = self.drop_some_threshold.clamp(0.85, 0.99);
        self.drop_most_threshold = self
            .drop_most_threshold
            .clamp(self.drop_some_threshold + 0.01, 0.99);
        self.block_new_threshold = self
            .block_new_threshold
            .clamp(self.drop_most_threshold + 0.01, 0.995);
    }

    /// Set the base cleanup interval (builder pattern).
    pub const fn with_base_interval(mut self, interval: Duration) -> Self {
        self.base_interval = interval;
        self
    }

    /// Set the minimum cleanup interval (builder pattern).
    pub const fn with_min_interval(mut self, interval: Duration) -> Self {
        self.min_interval = interval;
        self
    }

    /// Set the maximum cleanup interval (builder pattern).
    pub const fn with_max_interval(mut self, interval: Duration) -> Self {
        self.max_interval = interval;
        self
    }

    /// Set the high utilization threshold (builder pattern).
    pub fn with_high_utilization_threshold(mut self, threshold: f64) -> Self {
        self.high_utilization_threshold = threshold;
        self.validate();
        self
    }

    /// Set the low utilization threshold (builder pattern).
    pub fn with_low_utilization_threshold(mut self, threshold: f64) -> Self {
        self.low_utilization_threshold = threshold;
        self.validate();
        self
    }

    /// Set the TTL divisor (builder pattern).
    pub fn with_ttl_divisor(mut self, divisor: f64) -> Self {
        self.ttl_divisor = divisor;
        self.validate();
        self
    }

    /// Set backpressure thresholds (builder pattern).
    pub fn with_backpressure_thresholds(
        mut self,
        drop_some: f64,
        drop_most: f64,
        block_new: f64,
    ) -> Self {
        self.drop_some_threshold = drop_some;
        self.drop_most_threshold = drop_most;
        self.block_new_threshold = block_new;
        self.validate();
        self
    }

    /// Set efficiency smoothing factor (builder pattern).
    pub fn with_efficiency_smoothing(mut self, smoothing: f64) -> Self {
        self.efficiency_smoothing = smoothing;
        self.validate();
        self
    }
}

/// Hint about whether to apply backpressure or drop messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureHint {
    /// No backpressure needed.
    None,
    /// Consider dropping some lower-priority messages.
    DropSome,
    /// Drop most non-critical messages.
    DropMost,
    /// Block new messages temporarily.
    BlockNew,
}

/// Reason for cleanup parameter decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupReason {
    /// High cache utilization detected.
    HighUtilization,
    /// Low cache utilization, relaxed cleanup.
    LowUtilization,
    /// Moderate utilization, normal cleanup.
    ModerateUtilization,
    /// Critical utilization, immediate cleanup needed.
    CriticalUtilization {
        /// The level of backpressure recommended.
        backpressure: BackpressureHint,
    },
}

/// Efficiency trend direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EfficiencyTrend {
    /// Efficiency is improving (removing more per scan).
    Improving,
    /// Efficiency is degrading (removing less per scan).
    Degrading,
    /// Efficiency is stable.
    Stable,
    /// Not enough data to determine trend.
    Unknown,
}

/// Pressure trend direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureTrend {
    /// Pressure is increasing (utilization rising).
    Increasing,
    /// Pressure is decreasing (utilization falling).
    Decreasing,
    /// Pressure is stable.
    Stable,
    /// Not enough data to determine trend.
    Unknown,
}

/// Tuned cleanup parameters.
#[derive(Debug, Clone, Copy)]
pub struct CleanupParameters {
    /// Recommended interval until next cleanup.
    pub interval: Duration,

    /// Recommended batch size per shard.
    pub batch_size: usize,

    /// Whether aggressive cleanup is recommended.
    pub aggressive: bool,

    /// The utilization level that triggered these parameters.
    pub utilization: f64,

    /// Current message rate (messages per second).
    pub message_rate: f64,

    /// Reason for these parameters.
    pub reason: CleanupReason,
}

impl CleanupParameters {
    /// Check if immediate cleanup is recommended.
    pub fn should_cleanup_now(&self) -> bool {
        matches!(self.reason, CleanupReason::CriticalUtilization { .. })
    }

    /// Get backpressure hint if applicable.
    pub fn backpressure_hint(&self) -> BackpressureHint {
        match self.reason {
            CleanupReason::CriticalUtilization { backpressure } => backpressure,
            _ => BackpressureHint::None,
        }
    }
}

/// Statistics about cleanup performance.
#[derive(Debug, Clone)]
pub struct CleanupStats {
    /// Total cleanup cycles completed.
    pub cleanup_cycles: u64,

    /// Total entries removed.
    pub entries_removed: u64,

    /// Total entries scanned.
    pub entries_scanned: u64,

    /// Average cleanup duration.
    pub avg_cleanup_duration: Duration,

    /// Maximum cleanup duration observed.
    pub max_cleanup_duration: Duration,

    /// Current message rate (messages per second).
    pub current_message_rate: f64,

    /// Number of times aggressive cleanup was triggered.
    pub aggressive_cleanups: u64,

    /// Average removal efficiency (entries removed / entries scanned).
    pub avg_removal_efficiency: f64,

    /// Recent removal efficiency (EMA-smoothed).
    pub recent_efficiency: f64,

    /// Current efficiency trend.
    pub efficiency_trend: EfficiencyTrend,

    /// Current pressure trend (based on recent utilization).
    pub pressure_trend: PressureTrend,

    /// Recent average utilization (EMA-smoothed).
    pub recent_utilization: f64,
}

/// Lock-free rate counter for high-throughput message recording.
///
/// This counter is completely lock-free for recording operations.
/// The mutex is only needed when calculating rate (which syncs window state).
#[derive(Debug)]
struct LockFreeRateCounter {
    /// Current window message count (fully lock-free).
    current_count: AtomicU64,
}

impl LockFreeRateCounter {
    fn new() -> Self {
        Self {
            current_count: AtomicU64::new(0),
        }
    }

    /// Record a single message (fully lock-free).
    #[inline]
    fn record(&self) {
        self.current_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record multiple messages (fully lock-free).
    #[inline]
    fn record_batch(&self, count: u64) {
        self.current_count.fetch_add(count, Ordering::Relaxed);
    }

    /// Drain the counter and return the count (used during window sync).
    fn drain(&self) -> u64 {
        self.current_count.swap(0, Ordering::AcqRel)
    }

    /// Read current count without draining.
    fn load(&self) -> u64 {
        self.current_count.load(Ordering::Acquire)
    }
}

/// Dual-window rate tracker state (requires mutex for window management).
#[derive(Debug)]
struct RateWindowState {
    /// Current window start time.
    current_start: Instant,
    /// Previous window message count.
    previous_count: u64,
    /// Previous window duration.
    previous_duration: Duration,
    /// Window size.
    window_size: Duration,
    /// Accumulated count from lock-free counter since last sync.
    accumulated_current: u64,
}

impl RateWindowState {
    fn new(window_size: Duration) -> Self {
        Self {
            current_start: Instant::now(),
            previous_count: 0,
            previous_duration: Duration::ZERO,
            window_size,
            accumulated_current: 0,
        }
    }

    /// Sync with lock-free counter and calculate rate.
    ///
    /// Hardened against:
    /// - Clock jumps/backwards movement
    /// - Very long pauses between calls
    /// - Multiple missed window boundaries
    fn sync_and_calculate_rate(&mut self, counter: &LockFreeRateCounter) -> f64 {
        let now = Instant::now();

        // Drain atomic counter into accumulated
        let drained = counter.drain();
        self.accumulated_current = self.accumulated_current.saturating_add(drained);

        // Protect against clock issues
        let current_elapsed = now
            .checked_duration_since(self.current_start)
            .unwrap_or(Duration::ZERO);

        // Handle window rollover (cap iterations to prevent infinite loops)
        let mut rollovers = 0;
        let mut elapsed = current_elapsed;

        while elapsed >= self.window_size && rollovers < 10 {
            // Store previous window data
            self.previous_count = self.accumulated_current;
            self.previous_duration = self.window_size;
            self.accumulated_current = 0;

            // Advance window start
            self.current_start += self.window_size;

            // Recalculate elapsed
            elapsed = now
                .checked_duration_since(self.current_start)
                .unwrap_or(Duration::ZERO);
            rollovers += 1;
        }

        // If we hit rollover limit, force reset to current time
        if rollovers >= 10 {
            self.previous_count = self.accumulated_current;
            self.previous_duration = self.window_size;
            self.current_start = now;
            self.accumulated_current = 0;
            elapsed = Duration::ZERO;
        }

        // Add any remaining count from counter (for accurate current window)
        let current = self.accumulated_current + counter.load();

        // Blend current and previous windows
        let total_count = current + self.previous_count;
        let total_duration = elapsed + self.previous_duration;

        if total_duration.is_zero() {
            0.0
        } else {
            total_count as f64 / total_duration.as_secs_f64()
        }
    }

    /// Force reset the window.
    fn reset(&mut self, counter: &LockFreeRateCounter) {
        let drained = counter.drain();
        let elapsed = Instant::now()
            .checked_duration_since(self.current_start)
            .unwrap_or(self.window_size);

        self.previous_count = self.accumulated_current.saturating_add(drained);
        self.previous_duration = self.window_size.min(elapsed);
        self.current_start = Instant::now();
        self.accumulated_current = 0;
    }
}

/// Internal state for tracking cleanup metrics.
#[derive(Debug)]
struct CleanupState {
    /// Rate window state (requires mutex).
    rate_window: RateWindowState,

    /// Smoothed batch size target.
    target_batch_size: f64,

    /// Last cleanup duration.
    last_cleanup_duration: Duration,

    /// Total cleanup cycles.
    cleanup_cycles: u64,

    /// Total entries removed.
    entries_removed: u64,

    /// Total entries scanned.
    entries_scanned: u64,

    /// Max cleanup duration.
    max_cleanup_duration: Duration,

    /// Sum of cleanup durations (for average).
    total_cleanup_duration: Duration,

    /// Aggressive cleanup count.
    aggressive_cleanups: u64,

    /// Was the last cleanup aggressive? (for hysteresis).
    was_aggressive: bool,

    /// Raw last efficiency (for trend calculation).
    last_raw_efficiency: f64,

    /// EMA-smoothed efficiency.
    efficiency_ema: f64,

    /// Previous EMA efficiency (for trend).
    prev_efficiency_ema: f64,

    /// EMA-smoothed utilization (for pressure trend).
    utilization_ema: f64,

    /// Previous EMA utilization (for trend).
    prev_utilization_ema: f64,
}

impl CleanupState {
    fn new(window_size: Duration) -> Self {
        Self {
            rate_window: RateWindowState::new(window_size),
            target_batch_size: 100.0,
            last_cleanup_duration: Duration::ZERO,
            cleanup_cycles: 0,
            entries_removed: 0,
            entries_scanned: 0,
            max_cleanup_duration: Duration::ZERO,
            total_cleanup_duration: Duration::ZERO,
            aggressive_cleanups: 0,
            was_aggressive: false,
            last_raw_efficiency: 0.5,
            efficiency_ema: 0.5,
            prev_efficiency_ema: 0.5,
            utilization_ema: 0.5,
            prev_utilization_ema: 0.5,
        }
    }

    /// Calculate efficiency trend based on EMA changes.
    fn efficiency_trend(&self, min_cycles: u64) -> EfficiencyTrend {
        if self.cleanup_cycles < min_cycles {
            return EfficiencyTrend::Unknown;
        }

        let diff = self.efficiency_ema - self.prev_efficiency_ema;
        const THRESHOLD: f64 = 0.02; // 2% change threshold

        if diff > THRESHOLD {
            EfficiencyTrend::Improving
        } else if diff < -THRESHOLD {
            EfficiencyTrend::Degrading
        } else {
            EfficiencyTrend::Stable
        }
    }

    /// Calculate pressure trend based on utilization EMA changes.
    fn pressure_trend(&self, min_cycles: u64) -> PressureTrend {
        if self.cleanup_cycles < min_cycles {
            return PressureTrend::Unknown;
        }

        let diff = self.utilization_ema - self.prev_utilization_ema;
        const THRESHOLD: f64 = 0.02; // 2% change threshold

        if diff > THRESHOLD {
            PressureTrend::Increasing
        } else if diff < -THRESHOLD {
            PressureTrend::Decreasing
        } else {
            PressureTrend::Stable
        }
    }
}

/// Dynamic cleanup tuner that adjusts parameters based on load.
#[derive(Debug)]
pub struct CleanupTuner {
    config: CleanupConfig,
    /// Lock-free counter for message recording (no mutex needed).
    rate_counter: LockFreeRateCounter,
    /// State requiring synchronization.
    state: Mutex<CleanupState>,
}

impl CleanupTuner {
    /// Create a new cleanup tuner with the given configuration.
    pub fn new(mut config: CleanupConfig) -> Self {
        config.validate();
        let window_size = config.rate_window;
        Self {
            config,
            rate_counter: LockFreeRateCounter::new(),
            state: Mutex::new(CleanupState::new(window_size)),
        }
    }

    /// Create a tuner with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(CleanupConfig::default())
    }

    /// Record a message being processed (fully lock-free, very fast).
    ///
    /// This method uses atomic operations only - no mutex is acquired.
    ///
    /// **For high-throughput scenarios (>10k msg/s)**: Use `record_messages()`
    /// to batch updates every 50-200 messages. This improves cache locality
    /// while maintaining the same lock-free performance.
    #[inline]
    pub fn record_message(&self) {
        self.rate_counter.record();
    }

    /// Record multiple messages being processed (fully lock-free).
    ///
    /// This method uses atomic operations only - no mutex is acquired.
    ///
    /// **Recommended batching**:
    /// - < 10k msg/s: batch every 10-50 messages
    /// - 10k-50k msg/s: batch every 50-100 messages
    /// - > 50k msg/s: batch every 100-200 messages
    #[inline]
    pub fn record_messages(&self, count: u64) {
        self.rate_counter.record_batch(count);
    }

    /// Record a cleanup cycle completion.
    ///
    /// Call this after each cleanup cycle with the duration and entries removed.
    ///
    /// # Arguments
    ///
    /// * `duration` - How long the cleanup took
    /// * `entries_removed` - Number of entries actually removed/cleaned up
    /// * `params` - The parameters that were used for this cleanup (for tracking)
    pub fn record_cleanup(
        &self,
        duration: Duration,
        entries_removed: usize,
        params: &CleanupParameters,
    ) {
        let mut state = self.state.lock();

        // Calculate removal efficiency
        let entries_scanned = params.batch_size;
        let raw_efficiency = if entries_scanned > 0 {
            (entries_removed as f64 / entries_scanned as f64).clamp(0.0, 1.0)
        } else {
            state.last_raw_efficiency // Use previous if no data
        };

        // Update efficiency EMA
        state.prev_efficiency_ema = state.efficiency_ema;
        state.efficiency_ema = self.config.efficiency_smoothing * raw_efficiency
            + (1.0 - self.config.efficiency_smoothing) * state.efficiency_ema;
        state.last_raw_efficiency = raw_efficiency;

        // Update utilization EMA
        state.prev_utilization_ema = state.utilization_ema;
        state.utilization_ema = self.config.efficiency_smoothing * params.utilization
            + (1.0 - self.config.efficiency_smoothing) * state.utilization_ema;

        state.cleanup_cycles += 1;
        state.entries_removed += entries_removed as u64;
        state.entries_scanned += entries_scanned as u64;
        state.last_cleanup_duration = duration;
        state.total_cleanup_duration += duration;
        state.was_aggressive = params.aggressive;

        if duration > state.max_cleanup_duration {
            state.max_cleanup_duration = duration;
        }
        if params.aggressive {
            state.aggressive_cleanups += 1;
        }
    }

    /// Get the current message rate (messages per second).
    ///
    /// This synchronizes the lock-free counter with the rate window state.
    pub fn message_rate(&self) -> f64 {
        let mut state = self.state.lock();
        state
            .rate_window
            .sync_and_calculate_rate(&self.rate_counter)
    }

    /// Force reset the message rate window.
    ///
    /// **NOTE**: This is usually not needed as the window resets automatically.
    /// Only call this if you need to explicitly reset statistics.
    pub fn reset_rate_window(&self) {
        let mut state = self.state.lock();
        state.rate_window.reset(&self.rate_counter);
    }

    /// Get tuned cleanup parameters based on current state.
    ///
    /// # Arguments
    ///
    /// * `cache_utilization` - Current cache utilization (0.0-1.0)
    /// * `current_ttl` - The TTL duration used for cache entries. Cleanup interval
    ///   will be at least `current_ttl / ttl_divisor` to ensure entries are cleaned
    ///   multiple times per TTL period. Default divisor is 4.
    pub fn get_parameters(
        &self,
        cache_utilization: f64,
        current_ttl: Duration,
    ) -> CleanupParameters {
        let mut state = self.state.lock();
        let message_rate = state
            .rate_window
            .sync_and_calculate_rate(&self.rate_counter);

        // Apply hysteresis to prevent threshold flapping
        let high_threshold = if state.was_aggressive {
            self.config.high_utilization_threshold - self.config.utilization_hysteresis
        } else {
            self.config.high_utilization_threshold + self.config.utilization_hysteresis
        };

        let low_threshold = if state.was_aggressive {
            self.config.low_utilization_threshold + self.config.utilization_hysteresis
        } else {
            self.config.low_utilization_threshold - self.config.utilization_hysteresis
        };

        // Determine if we're under high load
        let high_load = message_rate > self.config.high_load_message_rate;

        // Determine backpressure level based on configurable thresholds
        let backpressure = if cache_utilization >= self.config.block_new_threshold {
            if high_load {
                BackpressureHint::BlockNew
            } else {
                BackpressureHint::DropMost
            }
        } else if cache_utilization >= self.config.drop_most_threshold {
            BackpressureHint::DropMost
        } else if cache_utilization >= self.config.drop_some_threshold {
            BackpressureHint::DropSome
        } else {
            BackpressureHint::None
        };

        // Determine cleanup reason and aggressiveness
        let (reason, aggressive) = if backpressure != BackpressureHint::None {
            (CleanupReason::CriticalUtilization { backpressure }, true)
        } else if cache_utilization > high_threshold {
            (CleanupReason::HighUtilization, true)
        } else if cache_utilization < low_threshold {
            (CleanupReason::LowUtilization, false)
        } else {
            (CleanupReason::ModerateUtilization, false)
        };

        // Calculate interval based on utilization and backpressure
        let base_interval = match reason {
            CleanupReason::CriticalUtilization { backpressure } => {
                match backpressure {
                    BackpressureHint::BlockNew | BackpressureHint::DropMost => {
                        // Extreme pressure: very aggressive cleanup even under high load
                        self.config.min_interval / 2
                    }
                    BackpressureHint::DropSome => self.config.min_interval,
                    BackpressureHint::None => unreachable!(),
                }
            }
            CleanupReason::HighUtilization => {
                if high_load {
                    // High load + high utilization: balance cleanup vs message processing
                    self.config.min_interval * 2
                } else {
                    self.config.min_interval
                }
            }
            CleanupReason::LowUtilization => {
                // Low utilization: relaxed cleanup
                self.config.max_interval.min(self.config.base_interval * 2)
            }
            CleanupReason::ModerateUtilization => {
                // Moderate utilization: interpolate based on position in range
                let range = high_threshold - low_threshold;
                let position = ((cache_utilization - low_threshold) / range).clamp(0.0, 1.0);
                let max_ms = self.config.max_interval.as_millis() as f64;
                let base_ms = self.config.base_interval.as_millis() as f64;
                let interval_ms = max_ms - (position * (max_ms - base_ms));
                Duration::from_millis(interval_ms as u64)
            }
        };

        // Calculate batch size adjustment
        let batch_size = self.calculate_batch_size(&mut state);

        // Apply TTL constraint with safety cap
        let ttl_secs = current_ttl.as_secs_f64();
        let min_ttl_interval = if ttl_secs > 0.0 && ttl_secs.is_finite() {
            let raw_interval = ttl_secs / self.config.ttl_divisor;
            // Cap TTL-based interval to prevent huge values
            let max_ttl_interval =
                self.config.max_interval.as_secs_f64() * self.config.max_ttl_interval_factor;
            Duration::from_secs_f64(raw_interval.min(max_ttl_interval))
        } else {
            Duration::ZERO
        };

        let final_interval = base_interval
            .max(min_ttl_interval)
            .max(self.config.min_interval);

        CleanupParameters {
            interval: final_interval,
            batch_size,
            aggressive,
            utilization: cache_utilization,
            message_rate,
            reason,
        }
    }

    /// Calculate batch size with smoothing and trend-aware efficiency adjustment.
    fn calculate_batch_size(&self, state: &mut CleanupState) -> usize {
        let target_ms = self.config.target_cleanup_duration.as_millis() as f64;
        let last_ms = state.last_cleanup_duration.as_millis().max(1) as f64;

        // Calculate desired batch size based on duration
        let duration_ratio = (target_ms / last_ms).clamp(0.5, 2.0);

        // Use EMA efficiency for smoother adjustment (only if we have enough data)
        let efficiency_factor = if state.cleanup_cycles >= self.config.min_cycles_for_efficiency {
            self.calculate_efficiency_factor(state)
        } else {
            1.0 // Neutral until we have enough data
        };

        // Apply trend adjustment - if efficiency is improving, be less aggressive
        // in increasing batch size; if degrading, be more willing to adjust
        let trend_factor = match state.efficiency_trend(self.config.min_cycles_for_efficiency) {
            EfficiencyTrend::Improving => 0.95, // Slightly dampen increases
            EfficiencyTrend::Degrading => 1.05, // Slightly boost adjustments
            EfficiencyTrend::Stable | EfficiencyTrend::Unknown => 1.0,
        };

        let desired_batch =
            (state.target_batch_size * duration_ratio * efficiency_factor * trend_factor)
                .clamp(10.0, 500.0);

        // Apply smoothing to avoid jumpy behavior
        let alpha = self.config.batch_size_smoothing;
        state.target_batch_size = alpha * desired_batch + (1.0 - alpha) * state.target_batch_size;

        // Safety: ensure batch_size doesn't get stuck at extreme values
        state.target_batch_size = state.target_batch_size.clamp(10.0, 500.0);

        state.target_batch_size.round() as usize
    }

    /// Calculate efficiency factor with smooth transitions (no sharp thresholds).
    fn calculate_efficiency_factor(&self, state: &CleanupState) -> f64 {
        let eff = state.efficiency_ema;

        // Target efficiency range: 0.25 - 0.6
        // Below 0.25: we're scanning too much for what we're removing → scan more
        // Above 0.6: we're very efficient → can scan less
        // Within range: linear interpolation

        const LOW_EFF: f64 = 0.15;
        const TARGET_LOW: f64 = 0.25;
        const TARGET_HIGH: f64 = 0.6;
        const HIGH_EFF: f64 = 0.85;

        if eff < LOW_EFF {
            // Very low efficiency - increase scanning significantly but smoothly
            // Linear ramp from 1.3 at 0 to 1.15 at LOW_EFF
            1.3 - (eff / LOW_EFF) * 0.15
        } else if eff < TARGET_LOW {
            // Below target - increase scanning moderately
            // Linear ramp from 1.15 at LOW_EFF to 1.0 at TARGET_LOW
            let t = (eff - LOW_EFF) / (TARGET_LOW - LOW_EFF);
            1.15 - t * 0.15
        } else if eff <= TARGET_HIGH {
            // Within target range - neutral
            1.0
        } else if eff < HIGH_EFF {
            // Above target - decrease scanning moderately
            // Linear ramp from 1.0 at TARGET_HIGH to 0.85 at HIGH_EFF
            let t = (eff - TARGET_HIGH) / (HIGH_EFF - TARGET_HIGH);
            1.0 - t * 0.15
        } else {
            // Very high efficiency - decrease scanning more
            // But cap at 0.8 to avoid too aggressive reduction
            0.85 - (eff - HIGH_EFF).min(0.15) * 0.33
        }
        .clamp(0.8, 1.3)
    }

    /// Get cleanup statistics.
    pub fn stats(&self) -> CleanupStats {
        let mut state = self.state.lock();
        let avg_duration = if state.cleanup_cycles > 0 {
            state.total_cleanup_duration / state.cleanup_cycles as u32
        } else {
            Duration::ZERO
        };

        let avg_efficiency = if state.entries_scanned > 0 {
            state.entries_removed as f64 / state.entries_scanned as f64
        } else {
            0.0
        };

        let efficiency_trend = state.efficiency_trend(self.config.min_cycles_for_efficiency);
        let pressure_trend = state.pressure_trend(self.config.min_cycles_for_efficiency);

        CleanupStats {
            cleanup_cycles: state.cleanup_cycles,
            entries_removed: state.entries_removed,
            entries_scanned: state.entries_scanned,
            avg_cleanup_duration: avg_duration,
            max_cleanup_duration: state.max_cleanup_duration,
            current_message_rate: state
                .rate_window
                .sync_and_calculate_rate(&self.rate_counter),
            aggressive_cleanups: state.aggressive_cleanups,
            avg_removal_efficiency: avg_efficiency,
            recent_efficiency: state.efficiency_ema,
            efficiency_trend,
            pressure_trend,
            recent_utilization: state.utilization_ema,
        }
    }

    /// Reset all statistics.
    pub fn reset_stats(&self) {
        let mut state = self.state.lock();
        state.cleanup_cycles = 0;
        state.entries_removed = 0;
        state.entries_scanned = 0;
        state.max_cleanup_duration = Duration::ZERO;
        state.total_cleanup_duration = Duration::ZERO;
        state.aggressive_cleanups = 0;
        state.last_raw_efficiency = 0.5;
        state.efficiency_ema = 0.5;
        state.prev_efficiency_ema = 0.5;
        state.utilization_ema = 0.5;
        state.prev_utilization_ema = 0.5;
    }

    /// Get the configuration.
    pub fn config(&self) -> &CleanupConfig {
        &self.config
    }
}

impl Default for CleanupTuner {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl Clone for CleanupTuner {
    fn clone(&self) -> Self {
        // Create new tuner with same config but fresh state
        Self::new(self.config.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_cleanup_config_defaults() {
        let config = CleanupConfig::default();
        assert_eq!(config.base_interval, Duration::from_secs(30));
        assert_eq!(config.min_interval, Duration::from_secs(5));
        assert_eq!(config.max_interval, Duration::from_secs(120));
        assert_eq!(config.ttl_divisor, 4.0);
    }

    #[test]
    fn test_cleanup_config_presets() {
        let high = CleanupConfig::high_throughput();
        assert!(high.min_interval < CleanupConfig::default().min_interval);

        let low = CleanupConfig::low_latency();
        assert!(low.target_cleanup_duration < CleanupConfig::default().target_cleanup_duration);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // Intentionally testing validation of invalid values
    fn test_cleanup_config_validation() {
        let mut config = CleanupConfig::default();
        config.ttl_divisor = 0.5; // Too low
        config.validate();
        assert!(config.ttl_divisor >= 2.0);

        config.ttl_divisor = 100.0; // Too high
        config.validate();
        assert!(config.ttl_divisor <= 20.0);
    }

    #[test]
    fn test_cleanup_parameters_low_utilization() {
        let tuner = CleanupTuner::with_defaults();
        let params = tuner.get_parameters(0.1, Duration::from_secs(300));

        assert!(!params.aggressive);
        assert!(params.interval >= tuner.config().base_interval);
        assert_eq!(params.reason, CleanupReason::LowUtilization);
    }

    #[test]
    fn test_cleanup_parameters_high_utilization() {
        let tuner = CleanupTuner::with_defaults();
        // Use a shorter TTL so TTL constraint doesn't dominate
        let params = tuner.get_parameters(0.9, Duration::from_secs(60));

        assert!(params.aggressive);
        // With TTL=60s and divisor=4, min_ttl_interval=15s, which is >= min_interval
        // so we check it's reasonable (less than base or at TTL constraint)
        assert!(params.interval <= tuner.config().base_interval.max(Duration::from_secs(15)));
        assert_eq!(params.reason, CleanupReason::HighUtilization);
    }

    #[test]
    fn test_cleanup_parameters_moderate_utilization() {
        let tuner = CleanupTuner::with_defaults();
        let params = tuner.get_parameters(0.5, Duration::from_secs(300));

        assert!(!params.aggressive);
        assert_eq!(params.reason, CleanupReason::ModerateUtilization);
    }

    #[test]
    fn test_cleanup_parameters_critical_with_backpressure() {
        let tuner = CleanupTuner::with_defaults();

        // Critical utilization at DropSome level
        let params = tuner.get_parameters(0.94, Duration::from_secs(300));
        assert!(params.should_cleanup_now());
        assert_eq!(params.backpressure_hint(), BackpressureHint::DropSome);

        // Critical utilization at DropMost level
        let params = tuner.get_parameters(0.97, Duration::from_secs(300));
        assert_eq!(params.backpressure_hint(), BackpressureHint::DropMost);
    }

    #[test]
    fn test_hysteresis_prevents_flapping() {
        let tuner = CleanupTuner::with_defaults();

        // Cross threshold from below
        let params1 = tuner.get_parameters(0.79, Duration::from_secs(300));
        assert!(!params1.aggressive);

        // Record cleanup to set state
        tuner.record_cleanup(Duration::from_millis(10), 50, &params1);

        // Slightly above threshold
        let params2 = tuner.get_parameters(0.81, Duration::from_secs(300));
        // Should still not be aggressive due to hysteresis
        assert!(!params2.aggressive);

        // Well above threshold
        let params3 = tuner.get_parameters(0.85, Duration::from_secs(300));
        assert!(params3.aggressive);
    }

    #[test]
    fn test_message_rate_tracking() {
        let tuner = CleanupTuner::with_defaults();

        // Record messages
        tuner.record_messages(100);
        thread::sleep(Duration::from_millis(10));

        let rate = tuner.message_rate();
        assert!(rate > 0.0);
    }

    #[test]
    fn test_cleanup_stats() {
        let tuner = CleanupTuner::with_defaults();
        let params = tuner.get_parameters(0.5, Duration::from_secs(300));

        tuner.record_cleanup(Duration::from_millis(20), 50, &params);
        tuner.record_cleanup(Duration::from_millis(30), 75, &params);

        let stats = tuner.stats();
        assert_eq!(stats.cleanup_cycles, 2);
        assert_eq!(stats.entries_removed, 125);
    }

    #[test]
    fn test_reset_stats() {
        let tuner = CleanupTuner::with_defaults();
        let params = tuner.get_parameters(0.5, Duration::from_secs(300));

        tuner.record_cleanup(Duration::from_millis(20), 50, &params);
        tuner.reset_stats();

        let stats = tuner.stats();
        assert_eq!(stats.cleanup_cycles, 0);
        assert_eq!(stats.entries_removed, 0);
    }

    #[test]
    fn test_batch_size_adjustment() {
        let tuner = CleanupTuner::with_defaults();
        let params_initial = tuner.get_parameters(0.5, Duration::from_secs(300));

        // Record several slow cleanups
        for _ in 0..5 {
            let params = tuner.get_parameters(0.5, Duration::from_secs(300));
            tuner.record_cleanup(Duration::from_millis(200), 100, &params);
        }

        let params_final = tuner.get_parameters(0.5, Duration::from_secs(300));

        // Should decrease batch size due to slow cleanups
        assert!(params_final.batch_size < params_initial.batch_size);
        assert!(params_final.batch_size >= 10); // Respects minimum
    }

    #[test]
    fn test_should_cleanup_now() {
        let tuner = CleanupTuner::with_defaults();

        // Not critical
        let params = tuner.get_parameters(0.5, Duration::from_secs(300));
        assert!(!params.should_cleanup_now());

        // Critical
        let params = tuner.get_parameters(0.95, Duration::from_secs(300));
        assert!(params.should_cleanup_now());
    }

    #[test]
    fn test_lock_free_recording() {
        let tuner = CleanupTuner::with_defaults();

        // These should be fully lock-free
        for _ in 0..1000 {
            tuner.record_message();
        }

        tuner.record_messages(500);

        thread::sleep(Duration::from_millis(10));
        let rate = tuner.message_rate();
        assert!(rate > 0.0);
    }

    #[test]
    fn test_efficiency_trend_tracking() {
        // Use a config with higher smoothing for more responsive trends
        let config = CleanupConfig::default().with_efficiency_smoothing(0.4); // More responsive
        let tuner = CleanupTuner::new(config);

        // Record enough cleanups to have valid trend data
        for _ in 0..5 {
            let params = tuner.get_parameters(0.5, Duration::from_secs(60));
            // Use batch_size from params for accurate efficiency calculation
            let batch = params.batch_size;
            // 50% efficiency (half of batch removed)
            tuner.record_cleanup(Duration::from_millis(20), batch / 2, &params);
        }

        let stats = tuner.stats();
        // After enough cycles, trend should be determined (not Unknown)
        assert_ne!(
            stats.efficiency_trend,
            EfficiencyTrend::Unknown,
            "Trend should be known after {} cycles (min_cycles={})",
            stats.cleanup_cycles,
            tuner.config().min_cycles_for_efficiency
        );
        // Recent efficiency should be reasonable (around 0.5)
        assert!(
            stats.recent_efficiency > 0.3 && stats.recent_efficiency < 0.7,
            "Recent efficiency {} should be around 0.5",
            stats.recent_efficiency
        );
    }

    #[test]
    fn test_pressure_trend_tracking() {
        let tuner = CleanupTuner::with_defaults();

        // Record cleanups with increasing utilization
        for i in 0..5 {
            let util = 0.4 + (i as f64 * 0.1);
            let params = tuner.get_parameters(util, Duration::from_secs(300));
            tuner.record_cleanup(Duration::from_millis(20), 50, &params);
        }

        let stats = tuner.stats();
        assert_eq!(stats.pressure_trend, PressureTrend::Increasing);
    }

    #[test]
    fn test_huge_ttl_safety() {
        let tuner = CleanupTuner::with_defaults();

        // Very large TTL should not cause huge intervals
        let params = tuner.get_parameters(0.5, Duration::from_secs(86400 * 30)); // 30 days

        // Should be capped by max_interval * max_ttl_interval_factor
        assert!(
            params.interval
                <= tuner
                    .config()
                    .max_interval
                    .mul_f64(tuner.config().max_ttl_interval_factor + 0.1)
        );
    }

    #[test]
    fn test_zero_efficiency_startup() {
        let tuner = CleanupTuner::with_defaults();

        // First few cleanups with 0 efficiency should not cause wild adjustments
        for _ in 0..3 {
            let params = tuner.get_parameters(0.5, Duration::from_secs(300));
            tuner.record_cleanup(Duration::from_millis(20), 0, &params);
        }

        let params = tuner.get_parameters(0.5, Duration::from_secs(300));

        // Batch size should still be reasonable
        assert!(params.batch_size >= 10);
        assert!(params.batch_size <= 500);
    }

    #[test]
    fn test_efficiency_factor_smooth_transitions() {
        let tuner = CleanupTuner::with_defaults();
        let mut state = CleanupState::new(Duration::from_secs(10));

        // Test various efficiency levels
        let test_cases = [
            (0.05, 1.25..1.35), // Very low efficiency → increase scan
            (0.15, 1.10..1.20), // Low efficiency
            (0.25, 0.95..1.05), // At target low
            (0.40, 0.95..1.05), // Mid target range
            (0.60, 0.95..1.05), // At target high
            (0.75, 0.85..0.95), // Above target
            (0.90, 0.78..0.88), // Very high efficiency → decrease scan
        ];

        for (eff, expected_range) in test_cases {
            state.efficiency_ema = eff;
            state.cleanup_cycles = 10; // Ensure we have enough data
            let factor = tuner.calculate_efficiency_factor(&state);
            assert!(
                factor >= expected_range.start && factor <= expected_range.end,
                "Efficiency {}: factor {} not in range {:?}",
                eff,
                factor,
                expected_range
            );
        }
    }

    #[test]
    fn test_stats_includes_derived_metrics() {
        let tuner = CleanupTuner::with_defaults();

        for i in 0..5 {
            let params = tuner.get_parameters(0.5 + (i as f64 * 0.05), Duration::from_secs(300));
            tuner.record_cleanup(Duration::from_millis(20), 50, &params);
        }

        let stats = tuner.stats();

        // Check all derived metrics are present
        assert!(stats.recent_efficiency > 0.0);
        assert!(stats.recent_utilization > 0.0);
        assert!(stats.entries_scanned > 0);
        // Trend should be known after enough cycles
        assert_ne!(stats.efficiency_trend, EfficiencyTrend::Unknown);
    }

    #[test]
    fn test_concurrent_recording() {
        use std::sync::Arc;

        let tuner = Arc::new(CleanupTuner::with_defaults());
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent recording
        for _ in 0..4 {
            let tuner_clone = Arc::clone(&tuner);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    tuner_clone.record_message();
                }
                tuner_clone.record_messages(500);
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        thread::sleep(Duration::from_millis(10));
        let rate = tuner.message_rate();
        assert!(rate > 0.0);

        // Total should be ~5500 per thread * 4 threads = 22000
        // Rate will vary based on timing
    }

    #[test]
    fn test_rate_tracker_window_rollover() {
        use crate::testing::wait_for_condition;

        let counter = LockFreeRateCounter::new();
        let state = std::cell::RefCell::new(RateWindowState::new(Duration::from_millis(100)));

        // Add some messages
        counter.record_batch(50);

        // Wait for window rollover using condition-based waiting
        wait_for_condition(
            || {
                let rate = state.borrow_mut().sync_and_calculate_rate(&counter);
                rate > 0.0
            },
            Duration::from_secs(2),
        )
        .expect("Rate should become positive after window rollover");

        // Previous window should be preserved
        counter.record_batch(50);
        let rate2 = state.borrow_mut().sync_and_calculate_rate(&counter);
        assert!(rate2 > 0.0);
    }

    #[test]
    fn test_extreme_pressure_backpressure() {
        let config = CleanupConfig::default();
        let tuner = CleanupTuner::new(config);

        // Simulate high load
        tuner.record_messages(10000);
        thread::sleep(Duration::from_millis(10));

        // Very high utilization under high load - use short TTL to avoid TTL constraint
        let params = tuner.get_parameters(0.98, Duration::from_secs(10));

        // Should recommend blocking new messages
        assert_eq!(params.backpressure_hint(), BackpressureHint::BlockNew);

        // Should still do aggressive cleanup despite high load
        // With TTL=10s and divisor=4, min_ttl_interval=2.5s which is less than min_interval=5s
        // So the critical interval (min_interval/2 = 2.5s) should be used
        assert!(params.interval <= tuner.config().min_interval);
    }

    #[test]
    fn test_builder_pattern() {
        let config = CleanupConfig::default()
            .with_base_interval(Duration::from_secs(45))
            .with_min_interval(Duration::from_secs(10))
            .with_max_interval(Duration::from_secs(180))
            .with_ttl_divisor(6.0)
            .with_efficiency_smoothing(0.25);

        assert_eq!(config.base_interval, Duration::from_secs(45));
        assert_eq!(config.min_interval, Duration::from_secs(10));
        assert_eq!(config.max_interval, Duration::from_secs(180));
        assert_eq!(config.ttl_divisor, 6.0);
        assert_eq!(config.efficiency_smoothing, 0.25);
    }
}
