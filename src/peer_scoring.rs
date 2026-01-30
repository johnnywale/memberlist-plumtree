//! Peer scoring for RTT-based topology optimization.
//!
//! This module provides RTT (Round-Trip Time) tracking and scoring for peers,
//! enabling the protocol to prefer lower-latency peers for the eager set.
//!
//! ## How It Works
//!
//! 1. RTT samples are collected when messages are exchanged (Graft → response)
//! 2. Samples are aggregated using exponential moving average (EMA)
//! 3. Peers with lower RTT get higher scores
//! 4. Failures are tracked with exponential decay over time
//! 5. During rebalancing, high-scoring peers are preferred for eager promotion
//!
//! ## Scoring Algorithm
//!
//! The score is calculated as:
//! ```text
//! base_score = SCORE_SCALE / (rtt_ema + floor)^exponent
//! final_score = base_score / (1 + loss_rate * loss_penalty)
//! ```
//!
//! This approach:
//! - Reduces extreme score differences at low RTTs (via floor)
//! - Penalizes high latency progressively (via exponent)
//! - Applies loss penalty based on decayed failure rate
//!
//! ## Example
//!
//! ```ignore
//! use memberlist_plumtree::peer_scoring::{PeerScoring, ScoringConfig};
//!
//! let scoring = PeerScoring::new(ScoringConfig::default());
//!
//! // Record RTT sample
//! scoring.record_rtt(&peer_id, Duration::from_millis(15));
//!
//! // Get best peers for eager set
//! let best = scoring.best_peers(3);
//! ```

use parking_lot::RwLock;
use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

/// Scale factor for score calculations.
const SCORE_SCALE: f64 = 1000.0;

/// Minimum RTT in milliseconds to prevent division by zero and extreme scores.
const MIN_RTT_MS: f64 = 0.1;

/// Configuration for peer scoring.
#[derive(Debug, Clone)]
pub struct ScoringConfig {
    /// Weight for new RTT samples in EMA calculation (0.0 to 1.0).
    /// Higher values make the score more responsive to recent measurements.
    /// Default: 0.3
    pub ema_alpha: f64,

    /// Maximum age of RTT data before it's considered stale.
    /// Stale entries are excluded from scoring decisions.
    /// Default: 5 minutes
    pub max_sample_age: Duration,

    /// Initial RTT estimate for peers without measurements.
    /// Default: 100ms
    pub initial_rtt_estimate: Duration,

    /// Minimum number of samples before score is considered reliable.
    /// Default: 3
    pub min_samples: u32,

    /// Penalty multiplier for peers with high packet loss.
    /// Score is divided by (1 + loss_rate * penalty).
    /// Default: 2.0
    pub loss_penalty: f64,

    /// Half-life for failure decay in seconds.
    /// Failures older than this are exponentially reduced.
    /// Default: 1800 (30 minutes)
    pub failure_decay_halflife: f64,

    /// Exponent for RTT scoring curve.
    /// Higher values penalize high latency more.
    /// Default: 1.15
    pub rtt_score_exponent: f64,

    /// Soft floor added to RTT before scoring to reduce low-RTT sensitivity.
    /// Default: 10.0 ms
    pub rtt_score_floor: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            ema_alpha: 0.3,
            max_sample_age: Duration::from_secs(300), // 5 minutes
            initial_rtt_estimate: Duration::from_millis(100),
            min_samples: 3,
            loss_penalty: 2.0,
            failure_decay_halflife: 1800.0, // 30 minutes
            rtt_score_exponent: 1.15,
            rtt_score_floor: 10.0,
        }
    }
}

impl ScoringConfig {
    /// Create a new scoring configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configuration for LAN environments (faster convergence).
    pub fn lan() -> Self {
        Self {
            ema_alpha: 0.5,                           // More responsive
            max_sample_age: Duration::from_secs(180), // 3 minutes
            initial_rtt_estimate: Duration::from_millis(10),
            min_samples: 2,
            loss_penalty: 3.0, // Higher penalty in LAN (loss is unusual)
            failure_decay_halflife: 900.0, // 15 minutes (faster recovery)
            rtt_score_exponent: 1.15,
            rtt_score_floor: 5.0, // Lower floor for LAN
        }
    }

    /// Configuration for WAN environments (more stable).
    pub fn wan() -> Self {
        Self {
            ema_alpha: 0.2,                           // More stable
            max_sample_age: Duration::from_secs(600), // 10 minutes
            initial_rtt_estimate: Duration::from_millis(200),
            min_samples: 5,
            loss_penalty: 1.5,              // Lower penalty (some loss is normal)
            failure_decay_halflife: 3600.0, // 1 hour (slower recovery)
            rtt_score_exponent: 1.15,
            rtt_score_floor: 20.0, // Higher floor for WAN
        }
    }

    /// Set the EMA alpha value.
    pub fn with_ema_alpha(mut self, alpha: f64) -> Self {
        self.ema_alpha = alpha.clamp(0.01, 1.0);
        self
    }

    /// Set the maximum sample age.
    pub fn with_max_sample_age(mut self, age: Duration) -> Self {
        self.max_sample_age = age;
        self
    }

    /// Set the initial RTT estimate.
    pub fn with_initial_rtt_estimate(mut self, rtt: Duration) -> Self {
        self.initial_rtt_estimate = rtt;
        self
    }

    /// Set the failure decay half-life.
    pub fn with_failure_decay_halflife(mut self, halflife: Duration) -> Self {
        self.failure_decay_halflife = halflife.as_secs_f64();
        self
    }

    /// Get initial RTT in milliseconds.
    #[inline]
    fn initial_rtt_ms(&self) -> f64 {
        self.initial_rtt_estimate.as_secs_f64() * 1000.0
    }

    /// Compute base score for a given RTT.
    #[inline]
    fn compute_base_score(&self, rtt_ms: f64) -> f64 {
        let rtt_adjusted =
            (rtt_ms.max(MIN_RTT_MS) + self.rtt_score_floor).powf(self.rtt_score_exponent);
        SCORE_SCALE / rtt_adjusted
    }
}

/// RTT and scoring data for a single peer.
#[derive(Debug, Clone)]
pub struct PeerScore {
    /// Exponential moving average of RTT in milliseconds.
    pub rtt_ema_ms: f64,

    /// Minimum observed RTT.
    pub rtt_min_ms: f64,

    /// Maximum observed RTT.
    pub rtt_max_ms: f64,

    /// Number of RTT samples collected.
    pub sample_count: u32,

    /// Number of observed failures (timeouts, errors).
    /// This is the raw count and doesn't decay.
    pub failure_count: u32,

    /// Effective failure weight after exponential decay.
    /// Used for scoring to implement time-based forgiveness.
    effective_failure: f64,

    /// Last time an RTT sample was recorded.
    pub last_sample: Instant,

    /// Last time the effective failure weight was updated.
    last_failure_update: Instant,

    /// Computed score (higher is better).
    /// This is computed on-demand with current decay applied.
    score: f64,
}

impl PeerScore {
    /// Create a new peer score with initial estimate.
    fn new(initial_rtt_ms: f64, config: &ScoringConfig) -> Self {
        let now = Instant::now();
        let base_score = config.compute_base_score(initial_rtt_ms);

        Self {
            rtt_ema_ms: initial_rtt_ms,
            rtt_min_ms: initial_rtt_ms,
            rtt_max_ms: initial_rtt_ms,
            sample_count: 0,
            failure_count: 0,
            effective_failure: 0.0,
            last_sample: now,
            last_failure_update: now,
            score: base_score,
        }
    }

    /// Update with a new RTT sample.
    fn update(&mut self, rtt: Duration, config: &ScoringConfig) {
        // Clamp RTT to minimum value to prevent extreme scores
        let rtt_ms = (rtt.as_secs_f64() * 1000.0).max(MIN_RTT_MS);

        if self.sample_count == 0 {
            // First sample
            self.rtt_ema_ms = rtt_ms;
            self.rtt_min_ms = rtt_ms;
            self.rtt_max_ms = rtt_ms;
        } else {
            // Exponential moving average
            self.rtt_ema_ms =
                config.ema_alpha * rtt_ms + (1.0 - config.ema_alpha) * self.rtt_ema_ms;
            self.rtt_min_ms = self.rtt_min_ms.min(rtt_ms);
            self.rtt_max_ms = self.rtt_max_ms.max(rtt_ms);
        }

        self.sample_count += 1;
        self.last_sample = Instant::now();
        self.apply_decay_and_recompute(config);
    }

    /// Record a failure (timeout or error).
    fn record_failure(&mut self, config: &ScoringConfig) {
        self.apply_decay_and_recompute(config);
        self.failure_count += 1;
        self.effective_failure += 1.0;
        self.last_failure_update = Instant::now();
        self.recompute_score(config);
    }

    /// Apply exponential decay to effective failure weight based on time elapsed.
    fn apply_decay(&mut self, now: Instant, config: &ScoringConfig) {
        if self.effective_failure > 0.0 {
            let elapsed = now.duration_since(self.last_failure_update).as_secs_f64();

            if elapsed > 0.0 {
                // Exponential decay: N(t) = N(0) * e^(-λt)
                // where λ = ln(2) / half_life
                let lambda = 2_f64.ln() / config.failure_decay_halflife;
                let decay_factor = (-lambda * elapsed).exp();
                self.effective_failure *= decay_factor;

                // Clean up very small values
                if self.effective_failure < 0.01 {
                    self.effective_failure = 0.0;
                }

                self.last_failure_update = now;
            }
        }
    }

    /// Apply decay and recompute score in one operation.
    #[inline]
    fn apply_decay_and_recompute(&mut self, config: &ScoringConfig) {
        self.apply_decay(Instant::now(), config);
        self.recompute_score(config);
    }

    /// Recompute the score based on current data.
    /// This uses the effective_failure (after decay) for loss rate calculation.
    fn recompute_score(&mut self, config: &ScoringConfig) {
        // Base score: inverse with soft floor and exponent
        let base_score = config.compute_base_score(self.rtt_ema_ms);

        // Apply loss penalty using effective (decayed) failures
        let total_attempts = self.sample_count as f64 + self.effective_failure;
        let loss_rate = if total_attempts > 0.0 {
            self.effective_failure / total_attempts
        } else {
            0.0
        };
        let penalty = 1.0 + loss_rate * config.loss_penalty;

        self.score = base_score / penalty;
    }

    /// Get the current score with up-to-date decay applied.
    /// This is the public API for reading scores - it applies decay before returning.
    pub fn current_score(&self, config: &ScoringConfig) -> f64 {
        let mut copy = self.clone();
        copy.apply_decay_and_recompute(config);
        copy.score
    }

    /// Check if this score is reliable (has enough samples).
    pub fn is_reliable(&self, config: &ScoringConfig) -> bool {
        self.sample_count >= config.min_samples
    }

    /// Check if the data is stale.
    pub fn is_stale(&self, config: &ScoringConfig) -> bool {
        self.last_sample.elapsed() > config.max_sample_age
    }

    /// Get the current loss rate (0.0 to 1.0) with decay applied.
    pub fn loss_rate(&self, config: &ScoringConfig) -> f64 {
        let mut copy = self.clone();
        copy.apply_decay(Instant::now(), config);

        let total = self.sample_count as f64 + copy.effective_failure;
        if total == 0.0 {
            0.0
        } else {
            copy.effective_failure / total
        }
    }

    /// Get the RTT variance (max - min).
    pub fn rtt_variance_ms(&self) -> f64 {
        self.rtt_max_ms - self.rtt_min_ms
    }

    /// Get the raw (non-decayed) failure count.
    pub fn raw_failure_count(&self) -> u32 {
        self.failure_count
    }

    /// Get the effective (decayed) failure weight.
    pub fn effective_failure_weight(&self, config: &ScoringConfig) -> f64 {
        let mut copy = self.clone();
        copy.apply_decay(Instant::now(), config);
        copy.effective_failure
    }
}

/// Peer scoring system for RTT-based topology optimization.
#[derive(Debug)]
pub struct PeerScoring<I> {
    /// Scoring configuration.
    config: ScoringConfig,
    /// Score data per peer.
    scores: RwLock<HashMap<I, PeerScore>>,
}

impl<I: Clone + Eq + Hash + Ord> PeerScoring<I> {
    /// Create a new peer scoring system.
    pub fn new(config: ScoringConfig) -> Self {
        Self {
            config,
            scores: RwLock::new(HashMap::new()),
        }
    }

    /// Record an RTT sample for a peer.
    pub fn record_rtt(&self, peer: &I, rtt: Duration) {
        let mut scores = self.scores.write();
        let initial_rtt_ms = self.config.initial_rtt_ms();

        // Avoid cloning when entry exists
        if let Some(entry) = scores.get_mut(peer) {
            entry.update(rtt, &self.config);
        } else {
            let mut entry = PeerScore::new(initial_rtt_ms, &self.config);
            entry.update(rtt, &self.config);
            scores.insert(peer.clone(), entry);
        }
    }

    /// Record a failure for a peer (timeout, error, etc.).
    pub fn record_failure(&self, peer: &I) {
        let mut scores = self.scores.write();
        let initial_rtt_ms = self.config.initial_rtt_ms();

        // Avoid cloning when entry exists
        if let Some(entry) = scores.get_mut(peer) {
            entry.record_failure(&self.config);
        } else {
            let mut entry = PeerScore::new(initial_rtt_ms, &self.config);
            entry.record_failure(&self.config);
            scores.insert(peer.clone(), entry);
        }
    }

    /// Get the score data for a peer.
    /// The returned score includes up-to-date decay applied via `current_score()`.
    pub fn get_score(&self, peer: &I) -> Option<PeerScore> {
        self.scores.read().get(peer).cloned()
    }

    /// Get a normalized score for a peer (0.0 to 1.0).
    ///
    /// This method returns a score suitable for hybrid scoring with topology:
    /// - 1.0 = excellent peer (low RTT, no failures)
    /// - 0.5 = average/unknown peer
    /// - 0.0 = poor peer (high RTT, many failures)
    ///
    /// Uses sigmoid normalization based on the raw score to produce smooth
    /// values that work well when combined with topological scores.
    ///
    /// For unknown peers, returns `default_score` (typically 0.5).
    pub fn normalized_score(&self, peer: &I, default_score: f64) -> f64 {
        let scores = self.scores.read();

        if let Some(score) = scores.get(peer) {
            if score.is_stale(&self.config) {
                return default_score;
            }

            let raw_score = score.current_score(&self.config);

            // Normalize using sigmoid-like transformation
            // Raw scores typically range from ~1 (500ms RTT) to ~70 (1ms RTT)
            // We want to map this to 0.0-1.0 with:
            // - score ~50+ → close to 1.0 (excellent)
            // - score ~10 → around 0.5 (average)
            // - score ~1 → close to 0.0 (poor)
            //
            // Using: normalized = raw_score / (raw_score + k)
            // where k controls the midpoint (score at which normalized = 0.5)
            const MIDPOINT_K: f64 = 10.0;
            let normalized = raw_score / (raw_score + MIDPOINT_K);

            normalized.clamp(0.0, 1.0)
        } else {
            default_score
        }
    }

    /// Get normalized scores for multiple peers at once (more efficient than individual calls).
    ///
    /// Returns a closure that can be passed to `PeerState::rebalance()`.
    /// This method acquires the lock once and returns scores for all requested peers.
    pub fn scorer(&self) -> impl Fn(&I) -> f64 + '_ {
        move |peer: &I| self.normalized_score(peer, 0.5)
    }

    /// Get the best N peers by score (highest first).
    /// Applies decay optimistically at read time for accurate ranking.
    pub fn best_peers(&self, count: usize) -> Vec<I> {
        let scores = self.scores.read();
        let mut peers: Vec<_> = scores
            .iter()
            .filter(|(_, score)| !score.is_stale(&self.config))
            .map(|(id, score)| (id.clone(), score.current_score(&self.config)))
            .collect();

        // Stable sort by score descending, then by peer ID for consistency
        peers.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        peers.into_iter().take(count).map(|(id, _)| id).collect()
    }

    /// Get the best N peers from a given set.
    /// Applies decay optimistically at read time for accurate ranking.
    pub fn best_peers_from(&self, candidates: &[I], count: usize) -> Vec<I> {
        let scores = self.scores.read();
        let initial_score = self.config.compute_base_score(self.config.initial_rtt_ms());

        let mut scored: Vec<_> = candidates
            .iter()
            .map(|peer| {
                let score = scores
                    .get(peer)
                    .filter(|s| !s.is_stale(&self.config))
                    .map(|s| s.current_score(&self.config))
                    .unwrap_or(initial_score);
                (peer.clone(), score)
            })
            .collect();

        // Stable sort by score descending, then by peer ID
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        scored.into_iter().take(count).map(|(id, _)| id).collect()
    }

    /// Get the worst N peers by score (lowest first).
    /// Applies decay optimistically at read time for accurate ranking.
    pub fn worst_peers(&self, count: usize) -> Vec<I> {
        let scores = self.scores.read();
        let mut peers: Vec<_> = scores
            .iter()
            .filter(|(_, score)| !score.is_stale(&self.config))
            .map(|(id, score)| (id.clone(), score.current_score(&self.config)))
            .collect();

        // Stable sort by score ascending, then by peer ID
        peers.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        peers.into_iter().take(count).map(|(id, _)| id).collect()
    }

    /// Remove a peer from scoring.
    pub fn remove_peer(&self, peer: &I) {
        self.scores.write().remove(peer);
    }

    /// Clean up stale entries.
    pub fn cleanup_stale(&self) -> usize {
        let mut scores = self.scores.write();
        let before = scores.len();
        scores.retain(|_, score| !score.is_stale(&self.config));
        before - scores.len()
    }

    /// Get statistics about the scoring system.
    /// Applies decay optimistically for accurate statistics.
    pub fn stats(&self) -> ScoringStats {
        let scores = self.scores.read();
        let total = scores.len();
        let reliable = scores
            .values()
            .filter(|s| s.is_reliable(&self.config))
            .count();
        let stale = scores.values().filter(|s| s.is_stale(&self.config)).count();

        let (avg_rtt, avg_score) = if total > 0 {
            let sum_rtt: f64 = scores.values().map(|s| s.rtt_ema_ms).sum();
            let sum_score: f64 = scores.values().map(|s| s.current_score(&self.config)).sum();
            (sum_rtt / total as f64, sum_score / total as f64)
        } else {
            (0.0, 0.0)
        };

        let reliable_pct = if total > 0 {
            (reliable as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        ScoringStats {
            total_peers: total,
            reliable_peers: reliable,
            reliable_percentage: reliable_pct,
            stale_peers: stale,
            avg_rtt_ms: avg_rtt,
            avg_score,
        }
    }

    /// Get the configuration.
    pub fn config(&self) -> &ScoringConfig {
        &self.config
    }
}

impl<I: Clone + Eq + Hash + Ord> Default for PeerScoring<I> {
    fn default() -> Self {
        Self::new(ScoringConfig::default())
    }
}

/// Statistics about the peer scoring system.
#[derive(Debug, Clone)]
pub struct ScoringStats {
    /// Total number of tracked peers.
    pub total_peers: usize,
    /// Number of peers with reliable scores.
    pub reliable_peers: usize,
    /// Percentage of reliable peers (0.0 to 100.0).
    pub reliable_percentage: f64,
    /// Number of peers with stale data.
    pub stale_peers: usize,
    /// Average RTT in milliseconds.
    pub avg_rtt_ms: f64,
    /// Average score.
    pub avg_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_score_update() {
        let config = ScoringConfig::default();
        let mut score = PeerScore::new(100.0, &config);

        // First sample
        score.update(Duration::from_millis(50), &config);
        assert_eq!(score.sample_count, 1);
        assert_eq!(score.rtt_ema_ms, 50.0);

        // Second sample (EMA)
        score.update(Duration::from_millis(100), &config);
        assert_eq!(score.sample_count, 2);
        // EMA: 0.3 * 100 + 0.7 * 50 = 30 + 35 = 65
        assert!((score.rtt_ema_ms - 65.0).abs() < 0.001);
    }

    #[test]
    fn test_rtt_zero_protection() {
        let config = ScoringConfig::default();
        let mut score = PeerScore::new(100.0, &config);

        // Test with zero duration
        score.update(Duration::from_nanos(0), &config);
        assert!(score.rtt_ema_ms >= MIN_RTT_MS);
        assert!(score.score.is_finite());
    }

    #[test]
    fn test_peer_scoring_best_peers() {
        let scoring: PeerScoring<u64> = PeerScoring::new(ScoringConfig::default());

        // Add some peers with different RTTs
        scoring.record_rtt(&1, Duration::from_millis(10)); // Best
        scoring.record_rtt(&2, Duration::from_millis(50));
        scoring.record_rtt(&3, Duration::from_millis(100)); // Worst
        scoring.record_rtt(&4, Duration::from_millis(30));

        let best = scoring.best_peers(2);
        assert_eq!(best.len(), 2);
        assert_eq!(best[0], 1); // Lowest RTT = highest score
        assert_eq!(best[1], 4);

        let worst = scoring.worst_peers(1);
        assert_eq!(worst[0], 3); // Highest RTT = lowest score
    }

    #[test]
    fn test_stable_sorting() {
        let scoring: PeerScoring<u64> = PeerScoring::new(ScoringConfig::default());

        // Create peers with identical RTTs
        for i in 0..10 {
            scoring.record_rtt(&i, Duration::from_millis(50));
        }

        // Multiple calls should return the same order
        let result1 = scoring.best_peers(5);
        let result2 = scoring.best_peers(5);
        let result3 = scoring.best_peers(5);

        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }

    #[test]
    fn test_loss_penalty() {
        let config = ScoringConfig::default();
        let scoring: PeerScoring<u64> = PeerScoring::new(config);

        // Peer 1: good (no failures)
        scoring.record_rtt(&1, Duration::from_millis(50));
        scoring.record_rtt(&1, Duration::from_millis(50));

        // Peer 2: same RTT but with failures
        scoring.record_rtt(&2, Duration::from_millis(50));
        scoring.record_failure(&2);
        scoring.record_failure(&2);

        let score1 = scoring.get_score(&1).unwrap();
        let score2 = scoring.get_score(&2).unwrap();

        // Peer 1 should have higher score due to no failures
        assert!(score1.current_score(&scoring.config) > score2.current_score(&scoring.config));
        assert!(score2.loss_rate(&scoring.config) > 0.0);

        // Check raw vs effective failures
        assert_eq!(score2.raw_failure_count(), 2);
        assert!(score2.effective_failure_weight(&scoring.config) > 0.0);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // Intentionally testing with modified config
    fn test_failure_decay_over_time() {
        use crate::testing::wait_for_condition;

        let mut config = ScoringConfig::default();
        config.failure_decay_halflife = 0.1; // 0.1 second for fast testing

        let mut score = PeerScore::new(100.0, &config);
        score.record_failure(&config);

        assert_eq!(score.failure_count, 1);
        let initial_effective = score.effective_failure;
        assert_eq!(initial_effective, 1.0);

        // Wait until decay has occurred (condition-based, not fixed sleep)
        // We use a RefCell to allow mutation inside the closure
        let score_cell = std::cell::RefCell::new(score);
        wait_for_condition(
            || {
                score_cell.borrow_mut().apply_decay(Instant::now(), &config);
                score_cell.borrow().effective_failure < 0.8
            },
            Duration::from_secs(2),
        )
        .expect("Decay should occur within timeout");

        let score = score_cell.into_inner();

        // Should have decayed significantly
        assert!(
            score.effective_failure < initial_effective,
            "effective_failure {} should be less than initial {}",
            score.effective_failure,
            initial_effective
        );

        // But raw count should stay the same
        assert_eq!(score.failure_count, 1);
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // Intentionally testing with modified config
    fn test_optimistic_read_decay() {
        let mut config = ScoringConfig::default();
        config.failure_decay_halflife = 0.1;

        let scoring: PeerScoring<u64> = PeerScoring::new(config);

        // Record RTT first so we have samples, then record failures
        // (loss_rate = failures / (samples + failures), so we need samples for decay to affect rate)
        scoring.record_rtt(&1, Duration::from_millis(50));
        scoring.record_failure(&1);
        scoring.record_failure(&1);

        let score_before = scoring.get_score(&1).unwrap();
        let loss_before = score_before.loss_rate(&scoring.config);

        // Wait for decay
        std::thread::sleep(Duration::from_millis(150));

        // Get score again - should have lower loss rate due to read-time decay
        let score_after = scoring.get_score(&1).unwrap();
        let loss_after = score_after.loss_rate(&scoring.config);

        assert!(
            loss_after < loss_before,
            "Loss rate should decrease over time: before={}, after={}",
            loss_before,
            loss_after
        );
    }

    #[test]
    fn test_best_peers_from() {
        let scoring: PeerScoring<u64> = PeerScoring::new(ScoringConfig::default());

        scoring.record_rtt(&1, Duration::from_millis(10));
        scoring.record_rtt(&2, Duration::from_millis(100));
        scoring.record_rtt(&3, Duration::from_millis(50));

        // Get best 2 from subset [2, 3]
        let best = scoring.best_peers_from(&[2, 3], 2);
        assert_eq!(best.len(), 2);
        assert_eq!(best[0], 3); // 50ms < 100ms
        assert_eq!(best[1], 2);
    }

    #[test]
    fn test_scoring_config_presets() {
        let lan = ScoringConfig::lan();
        let wan = ScoringConfig::wan();

        // LAN should be more responsive (higher alpha)
        assert!(lan.ema_alpha > wan.ema_alpha);

        // WAN should have higher initial estimate
        assert!(wan.initial_rtt_estimate > lan.initial_rtt_estimate);

        // LAN should have shorter max sample age (but not too short)
        assert!(lan.max_sample_age < wan.max_sample_age);
        assert!(lan.max_sample_age >= Duration::from_secs(180)); // At least 3 minutes
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // Intentionally testing with modified config
    fn test_cleanup_stale() {
        let mut config = ScoringConfig::default();
        config.max_sample_age = Duration::from_millis(1);

        let scoring: PeerScoring<u64> = PeerScoring::new(config);
        scoring.record_rtt(&1, Duration::from_millis(50));

        // Wait for staleness
        std::thread::sleep(Duration::from_millis(10));

        let removed = scoring.cleanup_stale();
        assert_eq!(removed, 1);
    }

    #[test]
    fn test_stats() {
        let scoring: PeerScoring<u64> = PeerScoring::new(ScoringConfig::default());

        scoring.record_rtt(&1, Duration::from_millis(50));
        scoring.record_rtt(&1, Duration::from_millis(50));
        scoring.record_rtt(&1, Duration::from_millis(50)); // Now reliable

        scoring.record_rtt(&2, Duration::from_millis(100)); // Not reliable yet

        let stats = scoring.stats();
        assert_eq!(stats.total_peers, 2);
        assert_eq!(stats.reliable_peers, 1);
        assert_eq!(stats.reliable_percentage, 50.0);
    }

    #[test]
    fn test_improved_scoring_curve() {
        let config = ScoringConfig::default();
        let mut score_low = PeerScore::new(100.0, &config);
        let mut score_high = PeerScore::new(100.0, &config);

        score_low.update(Duration::from_millis(1), &config);
        score_high.update(Duration::from_millis(5), &config);

        // Score ratio should be less extreme than 5x (old: 5ms→200, 1ms→1000 = 5x)
        let ratio = score_low.score / score_high.score;
        assert!(ratio < 3.0, "Score ratio {} is too extreme", ratio);
    }

    #[test]
    fn test_integer_failure_count() {
        let scoring: PeerScoring<u64> = PeerScoring::new(ScoringConfig::default());

        scoring.record_failure(&1);
        scoring.record_failure(&1);
        scoring.record_failure(&1);

        let score = scoring.get_score(&1).unwrap();

        // Raw count should be integer
        assert_eq!(score.raw_failure_count(), 3);

        // Effective weight should be close to 3.0 (may be slightly less due to minimal decay)
        // With default half-life of 1800s, decay over a few ms is negligible but nonzero
        let effective = score.effective_failure_weight(&scoring.config);
        assert!(
            (2.99..=3.01).contains(&effective),
            "Effective failure weight {} should be approximately 3.0",
            effective
        );
    }
}
