//! Rate limiting for Plumtree protocol operations.
//!
//! Provides token bucket rate limiting to prevent abuse and resource exhaustion.

use parking_lot::Mutex;
use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

/// Token bucket rate limiter for per-peer rate limiting.
///
/// Uses a token bucket algorithm where tokens are replenished over time
/// and consumed when operations are performed.
#[derive(Debug)]
pub struct RateLimiter<K> {
    /// Per-key token buckets.
    buckets: Mutex<HashMap<K, TokenBucket>>,
    /// Maximum tokens per bucket.
    max_tokens: u32,
    /// Token refill rate (tokens per second).
    refill_rate: f64,
    /// Cleanup interval for stale buckets.
    cleanup_interval: Duration,
    /// Last cleanup time.
    last_cleanup: Mutex<Instant>,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    /// Current token count.
    tokens: f64,
    /// Last time tokens were updated.
    last_update: Instant,
}

impl<K: Clone + Eq + Hash> RateLimiter<K> {
    /// Create a new rate limiter.
    ///
    /// # Arguments
    ///
    /// * `max_tokens` - Maximum tokens per bucket (burst capacity)
    /// * `refill_rate` - Tokens added per second
    pub fn new(max_tokens: u32, refill_rate: f64) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            max_tokens,
            refill_rate,
            cleanup_interval: Duration::from_secs(60),
            last_cleanup: Mutex::new(Instant::now()),
        }
    }

    /// Create a rate limiter with custom cleanup interval.
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Check if an operation is allowed and consume a token if so.
    ///
    /// Returns `true` if the operation is allowed, `false` if rate limited.
    pub fn check(&self, key: &K) -> bool {
        self.check_n(key, 1)
    }

    /// Check if N operations are allowed and consume tokens if so.
    ///
    /// Returns `true` if all operations are allowed, `false` if rate limited.
    pub fn check_n(&self, key: &K, n: u32) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock();

        // Maybe cleanup stale buckets
        self.maybe_cleanup(&mut buckets, now);

        let bucket = buckets.entry(key.clone()).or_insert_with(|| TokenBucket {
            tokens: self.max_tokens as f64,
            last_update: now,
        });

        // Refill tokens based on elapsed time
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.max_tokens as f64);
        bucket.last_update = now;

        // Try to consume tokens
        let required = n as f64;
        if bucket.tokens >= required {
            bucket.tokens -= required;
            true
        } else {
            false
        }
    }

    /// Get the current token count for a key.
    pub fn tokens(&self, key: &K) -> f64 {
        let now = Instant::now();
        let buckets = self.buckets.lock();

        if let Some(bucket) = buckets.get(key) {
            let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
            (bucket.tokens + elapsed * self.refill_rate).min(self.max_tokens as f64)
        } else {
            self.max_tokens as f64
        }
    }

    /// Reset the rate limiter for a specific key.
    pub fn reset(&self, key: &K) {
        let mut buckets = self.buckets.lock();
        buckets.remove(key);
    }

    /// Clear all rate limiting state.
    pub fn clear(&self) {
        let mut buckets = self.buckets.lock();
        buckets.clear();
    }

    /// Remove stale buckets that haven't been used recently.
    fn maybe_cleanup(&self, buckets: &mut HashMap<K, TokenBucket>, now: Instant) {
        let mut last_cleanup = self.last_cleanup.lock();
        if now.duration_since(*last_cleanup) < self.cleanup_interval {
            return;
        }
        *last_cleanup = now;
        drop(last_cleanup);

        // Remove buckets that are at max capacity (haven't been used recently)
        let stale_threshold = now - self.cleanup_interval;
        buckets.retain(|_, bucket| {
            bucket.last_update > stale_threshold || bucket.tokens < self.max_tokens as f64 - 0.1
        });
    }
}

impl<K: Clone + Eq + Hash> Default for RateLimiter<K> {
    fn default() -> Self {
        // Default: 10 requests per second, burst of 20
        Self::new(20, 10.0)
    }
}

/// Global rate limiter for all peers combined.
#[derive(Debug)]
pub struct GlobalRateLimiter {
    /// Token bucket for global rate.
    bucket: Mutex<TokenBucket>,
    /// Maximum tokens.
    max_tokens: u32,
    /// Token refill rate (tokens per second).
    refill_rate: f64,
}

impl GlobalRateLimiter {
    /// Create a new global rate limiter.
    pub fn new(max_tokens: u32, refill_rate: f64) -> Self {
        Self {
            bucket: Mutex::new(TokenBucket {
                tokens: max_tokens as f64,
                last_update: Instant::now(),
            }),
            max_tokens,
            refill_rate,
        }
    }

    /// Check if an operation is allowed.
    pub fn check(&self) -> bool {
        self.check_n(1)
    }

    /// Check if N operations are allowed.
    pub fn check_n(&self, n: u32) -> bool {
        let now = Instant::now();
        let mut bucket = self.bucket.lock();

        // Refill tokens
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_rate).min(self.max_tokens as f64);
        bucket.last_update = now;

        // Try to consume
        let required = n as f64;
        if bucket.tokens >= required {
            bucket.tokens -= required;
            true
        } else {
            false
        }
    }

    /// Get current token count.
    pub fn tokens(&self) -> f64 {
        let now = Instant::now();
        let bucket = self.bucket.lock();
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        (bucket.tokens + elapsed * self.refill_rate).min(self.max_tokens as f64)
    }

    /// Reset the rate limiter.
    pub fn reset(&self) {
        let mut bucket = self.bucket.lock();
        bucket.tokens = self.max_tokens as f64;
        bucket.last_update = Instant::now();
    }
}

impl Default for GlobalRateLimiter {
    fn default() -> Self {
        // Default: 100 requests per second, burst of 200
        Self::new(200, 100.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_burst() {
        let limiter: RateLimiter<u64> = RateLimiter::new(5, 1.0);

        // Should allow burst up to max_tokens
        for _ in 0..5 {
            assert!(limiter.check(&1));
        }

        // Should be rate limited
        assert!(!limiter.check(&1));
    }

    #[test]
    fn test_rate_limiter_refills() {
        let limiter: RateLimiter<u64> = RateLimiter::new(5, 100.0); // 100 tokens/sec

        // Exhaust tokens
        for _ in 0..5 {
            assert!(limiter.check(&1));
        }
        assert!(!limiter.check(&1));

        // Wait for refill
        std::thread::sleep(Duration::from_millis(50)); // Should get ~5 tokens

        // Should be allowed again
        assert!(limiter.check(&1));
    }

    #[test]
    fn test_rate_limiter_per_key() {
        let limiter: RateLimiter<u64> = RateLimiter::new(2, 1.0);

        // Exhaust key 1
        assert!(limiter.check(&1));
        assert!(limiter.check(&1));
        assert!(!limiter.check(&1));

        // Key 2 should still have tokens
        assert!(limiter.check(&2));
        assert!(limiter.check(&2));
        assert!(!limiter.check(&2));
    }

    #[test]
    fn test_rate_limiter_check_n() {
        let limiter: RateLimiter<u64> = RateLimiter::new(10, 1.0);

        // Request 5 tokens
        assert!(limiter.check_n(&1, 5));

        // Request 4 more
        assert!(limiter.check_n(&1, 4));

        // Request 2 more - should fail (only 1 left)
        assert!(!limiter.check_n(&1, 2));

        // Request 1 - should succeed
        assert!(limiter.check(&1));
    }

    #[test]
    fn test_global_rate_limiter() {
        let limiter = GlobalRateLimiter::new(3, 1.0);

        assert!(limiter.check());
        assert!(limiter.check());
        assert!(limiter.check());
        assert!(!limiter.check());
    }
}
