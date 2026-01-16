//! Configuration for Plumtree protocol.

use std::time::Duration;

/// Configuration options for the Plumtree protocol.
///
/// These parameters control the balance between broadcast efficiency,
/// reliability, and resource usage.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PlumtreeConfig {
    /// Number of eager peers (spanning tree fanout).
    ///
    /// Eager peers receive full messages immediately. Higher values
    /// increase reliability but also increase message overhead.
    ///
    /// Recommended: 3-4 for most deployments.
    ///
    /// Default: 3
    pub eager_fanout: usize,

    /// Number of lazy peers to send IHave announcements to.
    ///
    /// Lazy peers receive only message IDs and can request the full
    /// message if they haven't received it via eager push.
    ///
    /// Recommended: 6-8 for most deployments.
    ///
    /// Default: 6
    pub lazy_fanout: usize,

    /// Interval for sending batched IHave announcements.
    ///
    /// Shorter intervals reduce latency for tree repair but increase
    /// network overhead.
    ///
    /// Default: 100ms
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub ihave_interval: Duration,

    /// How long to cache messages for potential Graft requests.
    ///
    /// Messages are kept in memory for this duration to serve Graft
    /// requests from nodes that missed the initial broadcast.
    ///
    /// Default: 60s
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub message_cache_ttl: Duration,

    /// Maximum number of messages to cache.
    ///
    /// When the cache exceeds this size, oldest messages are evicted
    /// regardless of TTL.
    ///
    /// Default: 10000
    pub message_cache_max_size: usize,

    /// Number of gossip rounds before considering tree optimization.
    ///
    /// When a message is received from multiple paths, the redundant
    /// paths are pruned after this many rounds to optimize the tree.
    ///
    /// Higher values increase reliability but reduce efficiency.
    ///
    /// Default: 3
    pub optimization_threshold: u32,

    /// Maximum number of IHave messages to batch together.
    ///
    /// Batching IHaves reduces packet overhead but increases latency.
    ///
    /// **Large Cluster Recommendation**: In clusters with 1000+ nodes,
    /// set this to 32-64 to reduce IHave storm overhead. The total IHave
    /// traffic scales with `nodes × lazy_fanout × messages`, so larger
    /// batches significantly reduce packet count.
    ///
    /// Default: 16
    pub ihave_batch_size: usize,

    /// Timeout for expecting a message after receiving an IHave.
    ///
    /// If a message isn't received within this timeout after an IHave,
    /// the node will Graft to get the message.
    ///
    /// Default: 500ms
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub graft_timeout: Duration,

    /// Maximum message payload size.
    ///
    /// Messages larger than this will be rejected.
    ///
    /// Default: 64KB
    pub max_message_size: usize,

    /// Rate limit for Graft requests per peer (requests per second).
    ///
    /// Prevents abuse from peers sending excessive Graft requests.
    ///
    /// Default: 10.0
    pub graft_rate_limit_per_second: f64,

    /// Burst capacity for Graft rate limiting.
    ///
    /// Maximum number of Graft requests allowed in a burst.
    ///
    /// Default: 20
    pub graft_rate_limit_burst: u32,

    /// Maximum number of Graft retry attempts.
    ///
    /// After this many retries with exponential backoff, give up.
    ///
    /// Default: 5
    pub graft_max_retries: u32,

    /// Enable metrics collection.
    ///
    /// Default: true (if metrics feature is enabled)
    #[cfg(feature = "metrics")]
    pub enable_metrics: bool,
}

impl Default for PlumtreeConfig {
    fn default() -> Self {
        Self {
            eager_fanout: 3,
            lazy_fanout: 6,
            ihave_interval: Duration::from_millis(100),
            message_cache_ttl: Duration::from_secs(60),
            message_cache_max_size: 10000,
            optimization_threshold: 3,
            ihave_batch_size: 16,
            graft_timeout: Duration::from_millis(500),
            max_message_size: 64 * 1024, // 64KB
            graft_rate_limit_per_second: 10.0,
            graft_rate_limit_burst: 20,
            graft_max_retries: 5,
            #[cfg(feature = "metrics")]
            enable_metrics: true,
        }
    }
}

impl PlumtreeConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Configuration optimized for LAN environments.
    ///
    /// - Lower latency settings
    /// - Smaller cache TTL
    /// - More aggressive optimization
    pub fn lan() -> Self {
        Self {
            eager_fanout: 3,
            lazy_fanout: 6,
            ihave_interval: Duration::from_millis(50),
            message_cache_ttl: Duration::from_secs(30),
            message_cache_max_size: 10000,
            optimization_threshold: 2,
            ihave_batch_size: 8,
            graft_timeout: Duration::from_millis(200),
            max_message_size: 64 * 1024,
            graft_rate_limit_per_second: 20.0,
            graft_rate_limit_burst: 40,
            graft_max_retries: 3,
            #[cfg(feature = "metrics")]
            enable_metrics: true,
        }
    }

    /// Configuration optimized for WAN environments.
    ///
    /// - Higher latency tolerance
    /// - Longer cache TTL
    /// - More conservative optimization
    pub fn wan() -> Self {
        Self {
            eager_fanout: 4,
            lazy_fanout: 8,
            ihave_interval: Duration::from_millis(200),
            message_cache_ttl: Duration::from_secs(120),
            message_cache_max_size: 20000,
            optimization_threshold: 4,
            ihave_batch_size: 16,
            graft_timeout: Duration::from_secs(1),
            max_message_size: 64 * 1024,
            graft_rate_limit_per_second: 5.0,
            graft_rate_limit_burst: 15,
            graft_max_retries: 7,
            #[cfg(feature = "metrics")]
            enable_metrics: true,
        }
    }

    /// Configuration for large clusters (1000+ nodes).
    ///
    /// - Higher fanout for better coverage
    /// - Larger cache for more redundancy
    /// - Conservative optimization to maintain reliability
    pub fn large_cluster() -> Self {
        Self {
            eager_fanout: 5,
            lazy_fanout: 10,
            ihave_interval: Duration::from_millis(150),
            message_cache_ttl: Duration::from_secs(90),
            message_cache_max_size: 50000,
            optimization_threshold: 5,
            ihave_batch_size: 32,
            graft_timeout: Duration::from_millis(750),
            max_message_size: 64 * 1024,
            graft_rate_limit_per_second: 10.0,
            graft_rate_limit_burst: 30,
            graft_max_retries: 5,
            #[cfg(feature = "metrics")]
            enable_metrics: true,
        }
    }

    /// Set the eager fanout (builder pattern).
    pub const fn with_eager_fanout(mut self, fanout: usize) -> Self {
        self.eager_fanout = fanout;
        self
    }

    /// Set the lazy fanout (builder pattern).
    pub const fn with_lazy_fanout(mut self, fanout: usize) -> Self {
        self.lazy_fanout = fanout;
        self
    }

    /// Set the IHave interval (builder pattern).
    pub const fn with_ihave_interval(mut self, interval: Duration) -> Self {
        self.ihave_interval = interval;
        self
    }

    /// Set the message cache TTL (builder pattern).
    pub const fn with_message_cache_ttl(mut self, ttl: Duration) -> Self {
        self.message_cache_ttl = ttl;
        self
    }

    /// Set the message cache max size (builder pattern).
    pub const fn with_message_cache_max_size(mut self, size: usize) -> Self {
        self.message_cache_max_size = size;
        self
    }

    /// Set the optimization threshold (builder pattern).
    pub const fn with_optimization_threshold(mut self, threshold: u32) -> Self {
        self.optimization_threshold = threshold;
        self
    }

    /// Set the IHave batch size (builder pattern).
    ///
    /// For large clusters (1000+ nodes), use 32-64 to reduce IHave storm overhead.
    pub const fn with_ihave_batch_size(mut self, size: usize) -> Self {
        self.ihave_batch_size = size;
        self
    }

    /// Set the graft timeout (builder pattern).
    pub const fn with_graft_timeout(mut self, timeout: Duration) -> Self {
        self.graft_timeout = timeout;
        self
    }

    /// Set the maximum message size (builder pattern).
    pub const fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Set the Graft rate limit per second (builder pattern).
    pub fn with_graft_rate_limit_per_second(mut self, rate: f64) -> Self {
        self.graft_rate_limit_per_second = rate;
        self
    }

    /// Set the Graft rate limit burst (builder pattern).
    pub const fn with_graft_rate_limit_burst(mut self, burst: u32) -> Self {
        self.graft_rate_limit_burst = burst;
        self
    }

    /// Set the maximum Graft retries (builder pattern).
    pub const fn with_graft_max_retries(mut self, retries: u32) -> Self {
        self.graft_max_retries = retries;
        self
    }
}

#[cfg(feature = "serde")]
mod humantime_serde_impl {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&format!("{}ms", duration.as_millis()))
        } else {
            serializer.serialize_u64(duration.as_millis() as u64)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            // Simple parsing: expect "Nms" format
            let ms: u64 = s
                .trim_end_matches("ms")
                .parse()
                .map_err(serde::de::Error::custom)?;
            Ok(Duration::from_millis(ms))
        } else {
            let ms = u64::deserialize(deserializer)?;
            Ok(Duration::from_millis(ms))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PlumtreeConfig::default();
        assert_eq!(config.eager_fanout, 3);
        assert_eq!(config.lazy_fanout, 6);
    }

    #[test]
    fn test_builder_pattern() {
        let config = PlumtreeConfig::new()
            .with_eager_fanout(5)
            .with_lazy_fanout(10)
            .with_ihave_interval(Duration::from_millis(200));

        assert_eq!(config.eager_fanout, 5);
        assert_eq!(config.lazy_fanout, 10);
        assert_eq!(config.ihave_interval, Duration::from_millis(200));
    }
}
