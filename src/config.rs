//! Configuration for Plumtree protocol.

use std::time::Duration;

/// Configuration options for the Plumtree protocol.
///
/// These parameters control the balance between broadcast efficiency,
/// reliability, and resource usage.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PlumtreeConfig {
    /// Maximum number of peers to maintain.
    ///
    /// When set, the node will only maintain connections to this many peers.
    /// Excess peers will be rejected or evicted (oldest lazy peers first).
    /// This enables partial mesh topology instead of full mesh.
    ///
    /// The limit is enforced as: `eager_fanout + lazy_fanout <= max_peers`.
    /// If not set (None), no limit is enforced and all peers are accepted.
    ///
    /// Default: None (unlimited)
    pub max_peers: Option<usize>,

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

    /// Interval for periodic topology maintenance.
    ///
    /// The maintenance loop checks if the eager peer count has dropped below
    /// `eager_fanout` and promotes lazy peers to restore the spanning tree.
    ///
    /// Set to `Duration::ZERO` to disable periodic maintenance (not recommended).
    ///
    /// Default: 2s
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub maintenance_interval: Duration,

    /// Maximum random jitter added to maintenance interval.
    ///
    /// Jitter prevents "thundering herd" effects when multiple nodes detect
    /// topology degradation simultaneously. Each maintenance cycle will have
    /// a random delay between 0 and this value added.
    ///
    /// Default: 500ms
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub maintenance_jitter: Duration,

    /// Enable metrics collection.
    ///
    /// Default: true (if metrics feature is enabled)
    #[cfg(feature = "metrics")]
    pub enable_metrics: bool,

    /// Enable hash ring topology for peer selection.
    ///
    /// When enabled, peers are organized in a deterministic hash ring:
    /// - Adjacent peers (i±1, i±2) form the base connectivity layer (Z≥2 redundancy)
    /// - Long-range jump links (i±N/4) optimize message propagation
    /// - Ring neighbors are protected from demotion
    /// - Eviction uses fingerprint-based selection to preserve topological diversity
    ///
    /// This is automatically enabled when using `PlumtreeMemberlist`.
    ///
    /// Default: false
    pub use_hash_ring: bool,
}

impl Default for PlumtreeConfig {
    fn default() -> Self {
        Self {
            max_peers: None,
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
            maintenance_interval: Duration::from_secs(2),
            maintenance_jitter: Duration::from_millis(500),
            #[cfg(feature = "metrics")]
            enable_metrics: true,
            use_hash_ring: false,
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
            max_peers: None,
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
            maintenance_interval: Duration::from_secs(1),
            maintenance_jitter: Duration::from_millis(200),
            #[cfg(feature = "metrics")]
            enable_metrics: true,
            use_hash_ring: false,
        }
    }

    /// Configuration optimized for WAN environments.
    ///
    /// - Higher latency tolerance
    /// - Longer cache TTL
    /// - More conservative optimization
    pub fn wan() -> Self {
        Self {
            max_peers: None,
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
            maintenance_interval: Duration::from_secs(5),
            maintenance_jitter: Duration::from_secs(1),
            #[cfg(feature = "metrics")]
            enable_metrics: true,
            use_hash_ring: false,
        }
    }

    /// Configuration for large clusters (1000+ nodes).
    ///
    /// - Higher fanout for better coverage
    /// - Larger cache for more redundancy
    /// - Conservative optimization to maintain reliability
    pub fn large_cluster() -> Self {
        Self {
            max_peers: None,
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
            maintenance_interval: Duration::from_secs(3),
            maintenance_jitter: Duration::from_millis(750),
            #[cfg(feature = "metrics")]
            enable_metrics: true,
            use_hash_ring: true,
        }
    }

    /// Set the maximum number of peers (builder pattern).
    ///
    /// When set, enables partial mesh topology where the node only
    /// maintains connections to this many peers. Peers are auto-classified
    /// as eager or lazy based on `eager_fanout` and `lazy_fanout`.
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::PlumtreeConfig;
    ///
    /// let config = PlumtreeConfig::default()
    ///     .with_max_peers(8)      // Limit to 8 peers total
    ///     .with_eager_fanout(2)   // 2 eager peers
    ///     .with_lazy_fanout(6);   // 6 lazy peers
    /// ```
    pub const fn with_max_peers(mut self, max_peers: usize) -> Self {
        self.max_peers = Some(max_peers);
        self
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

    /// Set the maintenance interval (builder pattern).
    ///
    /// The maintenance loop runs periodically to check and repair the
    /// topology by promoting lazy peers when eager count drops.
    pub const fn with_maintenance_interval(mut self, interval: Duration) -> Self {
        self.maintenance_interval = interval;
        self
    }

    /// Set the maintenance jitter (builder pattern).
    ///
    /// Random jitter prevents thundering herd effects when multiple nodes
    /// detect topology degradation simultaneously.
    pub const fn with_maintenance_jitter(mut self, jitter: Duration) -> Self {
        self.maintenance_jitter = jitter;
        self
    }

    /// Enable or disable hash ring topology (builder pattern).
    ///
    /// When enabled, peers are organized in a deterministic hash ring:
    /// - Adjacent peers (i±1, i±2) form the base connectivity layer
    /// - Long-range jump links (i±N/4) optimize message propagation
    /// - Ring neighbors are protected from demotion
    ///
    /// This is automatically enabled when using `PlumtreeMemberlist`.
    pub const fn with_hash_ring(mut self, enable: bool) -> Self {
        self.use_hash_ring = enable;
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
