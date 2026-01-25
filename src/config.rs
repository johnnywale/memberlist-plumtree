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
    /// This is automatically enabled when using `PlumtreeDiscovery`.
    ///
    /// Default: false
    pub use_hash_ring: bool,

    /// Enable protection of ring neighbors from demotion.
    ///
    /// When enabled, adjacent ring neighbors (determined by `max_protected_neighbors`)
    /// cannot be demoted from eager to lazy. They can only be removed when
    /// the peer leaves the cluster entirely.
    ///
    /// This ensures Z≥2 redundancy for message delivery even under churn.
    ///
    /// Only effective when `use_hash_ring` is true.
    ///
    /// Default: true
    pub protect_ring_neighbors: bool,

    /// Maximum number of ring neighbors to protect from demotion.
    ///
    /// Controls how many adjacent peers on the hash ring are protected:
    /// - 2: Only immediate neighbors (i±1)
    /// - 4: Immediate + second-nearest (i±1, i±2) - provides Z≥2 redundancy
    /// - 0: No protection (all peers can be demoted)
    ///
    /// Only effective when `use_hash_ring` and `protect_ring_neighbors` are true.
    ///
    /// Default: 4
    pub max_protected_neighbors: usize,

    /// Hard cap on the number of eager peers.
    ///
    /// When set, the node will reject GRAFT requests that would push the
    /// eager peer count above this limit. The peer remains lazy instead.
    ///
    /// This is useful for bandwidth-constrained environments where you want
    /// to strictly limit the number of full message streams.
    ///
    /// Note: This may cause some peers to rely more heavily on IHAVE/GRAFT
    /// for message recovery if they cannot join the eager set.
    ///
    /// Default: None (limited only by `eager_fanout` during rebalancing)
    pub max_eager_peers: Option<usize>,

    /// Hard cap on the number of lazy peers.
    ///
    /// When set, the node will reject new peers that would push the
    /// lazy peer count above this limit (after eager slots are full).
    ///
    /// This is useful for memory-constrained environments where you want
    /// to limit the IHAVE tracking overhead.
    ///
    /// Default: None (unlimited lazy peers)
    pub max_lazy_peers: Option<usize>,

    /// Anti-entropy sync configuration.
    ///
    /// Enables recovery of missed messages after network partitions
    /// or node restarts.
    ///
    /// Default: None (disabled)
    #[cfg_attr(feature = "serde", serde(default))]
    pub sync: Option<SyncConfig>,

    /// Storage configuration for message persistence.
    ///
    /// Enables message persistence for crash recovery and anti-entropy sync.
    ///
    /// Default: None (disabled)
    #[cfg_attr(feature = "serde", serde(default))]
    pub storage: Option<StorageConfig>,
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
            protect_ring_neighbors: true,
            max_protected_neighbors: 4,
            max_eager_peers: None,
            max_lazy_peers: None,
            sync: None,
            storage: None,
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
    /// - Minimal ring neighbor protection (small clusters don't need it)
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
            protect_ring_neighbors: false, // Small LAN clusters don't need protection
            max_protected_neighbors: 2,
            max_eager_peers: None,
            max_lazy_peers: None,
            sync: None,
            storage: None,
        }
    }

    /// Configuration optimized for WAN environments.
    ///
    /// - Higher latency tolerance
    /// - Longer cache TTL
    /// - More conservative optimization
    /// - Ring neighbor protection enabled for reliability
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
            use_hash_ring: true,
            protect_ring_neighbors: true,
            max_protected_neighbors: 4,
            max_eager_peers: None,
            max_lazy_peers: None,
            sync: None,
            storage: None,
        }
    }

    /// Configuration for large clusters (1000+ nodes).
    ///
    /// - Higher fanout for better coverage
    /// - Larger cache for more redundancy
    /// - Conservative optimization to maintain reliability
    /// - Full ring neighbor protection for stability
    /// - Hard cap on eager peers to prevent runaway growth
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
            protect_ring_neighbors: true,
            max_protected_neighbors: 4,
            max_eager_peers: Some(8),  // Hard cap to prevent unbounded growth
            max_lazy_peers: Some(50),  // Limit IHAVE tracking overhead
            sync: None,
            storage: None,
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
    /// This is automatically enabled when using `PlumtreeDiscovery`.
    pub const fn with_hash_ring(mut self, enable: bool) -> Self {
        self.use_hash_ring = enable;
        self
    }

    /// Enable or disable ring neighbor protection (builder pattern).
    ///
    /// When enabled, adjacent ring neighbors cannot be demoted from eager to lazy.
    /// This ensures Z≥2 redundancy for message delivery.
    ///
    /// Only effective when `use_hash_ring` is true.
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::PlumtreeConfig;
    ///
    /// // Disable ring neighbor protection for small clusters
    /// let config = PlumtreeConfig::default()
    ///     .with_hash_ring(true)
    ///     .with_protect_ring_neighbors(false);
    /// ```
    pub const fn with_protect_ring_neighbors(mut self, enable: bool) -> Self {
        self.protect_ring_neighbors = enable;
        self
    }

    /// Set the maximum number of protected ring neighbors (builder pattern).
    ///
    /// Controls how many adjacent hash ring peers are protected from demotion:
    /// - 2: Only immediate neighbors (i±1)
    /// - 4: Immediate + second-nearest (i±1, i±2) - provides Z≥2 redundancy
    /// - 0: No protection (same as `with_protect_ring_neighbors(false)`)
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::PlumtreeConfig;
    ///
    /// // Only protect immediate neighbors, not second-nearest
    /// let config = PlumtreeConfig::default()
    ///     .with_hash_ring(true)
    ///     .with_max_protected_neighbors(2);
    /// ```
    pub const fn with_max_protected_neighbors(mut self, count: usize) -> Self {
        self.max_protected_neighbors = count;
        self
    }

    /// Set a hard cap on eager peers (builder pattern).
    ///
    /// When set, GRAFT requests that would exceed this limit are rejected.
    /// The peer remains lazy instead of being promoted to eager.
    ///
    /// This is useful for bandwidth-constrained environments.
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::PlumtreeConfig;
    ///
    /// // Hard limit of 5 eager peers, regardless of GRAFT requests
    /// let config = PlumtreeConfig::default()
    ///     .with_eager_fanout(3)      // Target 3 eager peers
    ///     .with_max_eager_peers(5);  // But allow up to 5
    /// ```
    pub const fn with_max_eager_peers(mut self, max: usize) -> Self {
        self.max_eager_peers = Some(max);
        self
    }

    /// Set a hard cap on lazy peers (builder pattern).
    ///
    /// When set, new peers that would exceed this limit are rejected
    /// if there are no eager slots available.
    ///
    /// This is useful for memory-constrained environments.
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::PlumtreeConfig;
    ///
    /// // Limit lazy peers to reduce IHAVE tracking overhead
    /// let config = PlumtreeConfig::default()
    ///     .with_max_lazy_peers(20);
    /// ```
    pub const fn with_max_lazy_peers(mut self, max: usize) -> Self {
        self.max_lazy_peers = Some(max);
        self
    }

    /// Enable anti-entropy sync with the given configuration (builder pattern).
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::{PlumtreeConfig, SyncConfig};
    ///
    /// let config = PlumtreeConfig::default()
    ///     .with_sync(SyncConfig::enabled());
    /// ```
    pub fn with_sync(mut self, sync: SyncConfig) -> Self {
        self.sync = Some(sync);
        self
    }

    /// Enable storage with the given configuration (builder pattern).
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::{PlumtreeConfig, StorageConfig};
    ///
    /// let config = PlumtreeConfig::default()
    ///     .with_storage(StorageConfig::enabled());
    /// ```
    pub fn with_storage(mut self, storage: StorageConfig) -> Self {
        self.storage = Some(storage);
        self
    }
}

/// Anti-entropy synchronization configuration.
///
/// Anti-entropy sync enables recovery of missed messages after network partitions
/// or node restarts by periodically comparing message state with peers.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SyncConfig {
    /// Enable anti-entropy sync.
    ///
    /// Default: false
    pub enabled: bool,

    /// Interval between sync rounds.
    ///
    /// Lower values increase sync frequency but also network overhead.
    ///
    /// Default: 30s
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub sync_interval: Duration,

    /// Time window for sync (how far back to check).
    ///
    /// Messages older than this window are not included in sync comparisons.
    /// Should be >= message_cache_ttl for effective recovery.
    ///
    /// Default: 90s
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub sync_window: Duration,

    /// Maximum message IDs per sync response.
    ///
    /// Limits response size to prevent MTU overflow.
    ///
    /// Default: 100
    pub max_batch_size: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sync_interval: Duration::from_secs(30),
            sync_window: Duration::from_secs(90),
            max_batch_size: 100,
        }
    }
}

impl SyncConfig {
    /// Create a new sync configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable sync with default settings.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Set enabled state (builder pattern).
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set sync interval (builder pattern).
    pub const fn with_sync_interval(mut self, interval: Duration) -> Self {
        self.sync_interval = interval;
        self
    }

    /// Set sync window (builder pattern).
    pub const fn with_sync_window(mut self, window: Duration) -> Self {
        self.sync_window = window;
        self
    }

    /// Set max batch size (builder pattern).
    pub const fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }
}

/// Storage configuration for message persistence.
///
/// Enables message persistence for crash recovery and anti-entropy sync.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct StorageConfig {
    /// Enable message persistence.
    ///
    /// Default: false
    pub enabled: bool,

    /// Maximum messages to store.
    ///
    /// When exceeded, oldest messages are evicted.
    ///
    /// Default: 100,000
    pub max_messages: usize,

    /// Message retention duration.
    ///
    /// Messages older than this are garbage collected.
    ///
    /// Default: 300s (5 minutes)
    #[cfg_attr(feature = "serde", serde(with = "humantime_serde_impl"))]
    pub retention: Duration,

    /// Storage path for disk backends (e.g., Sled).
    ///
    /// Only used by persistent storage backends.
    ///
    /// Default: None
    #[cfg_attr(feature = "serde", serde(default))]
    pub path: Option<std::path::PathBuf>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_messages: 100_000,
            retention: Duration::from_secs(300),
            path: None,
        }
    }
}

impl StorageConfig {
    /// Create a new storage configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable storage with default settings.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Self::default()
        }
    }

    /// Set enabled state (builder pattern).
    pub const fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set max messages (builder pattern).
    pub const fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = max;
        self
    }

    /// Set retention duration (builder pattern).
    pub const fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self
    }

    /// Set storage path (builder pattern).
    pub fn with_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.path = Some(path.into());
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

    #[test]
    fn test_ring_neighbor_protection_config() {
        // Default config has protection enabled
        let config = PlumtreeConfig::default();
        assert!(config.protect_ring_neighbors);
        assert_eq!(config.max_protected_neighbors, 4);

        // Can disable protection
        let config = PlumtreeConfig::default().with_protect_ring_neighbors(false);
        assert!(!config.protect_ring_neighbors);

        // Can reduce protected neighbors
        let config = PlumtreeConfig::default().with_max_protected_neighbors(2);
        assert_eq!(config.max_protected_neighbors, 2);
    }

    #[test]
    fn test_peer_limits_config() {
        // Default config has no limits
        let config = PlumtreeConfig::default();
        assert!(config.max_eager_peers.is_none());
        assert!(config.max_lazy_peers.is_none());

        // Can set eager limit
        let config = PlumtreeConfig::default().with_max_eager_peers(5);
        assert_eq!(config.max_eager_peers, Some(5));

        // Can set lazy limit
        let config = PlumtreeConfig::default().with_max_lazy_peers(20);
        assert_eq!(config.max_lazy_peers, Some(20));

        // Can set both
        let config = PlumtreeConfig::default()
            .with_max_eager_peers(5)
            .with_max_lazy_peers(20);
        assert_eq!(config.max_eager_peers, Some(5));
        assert_eq!(config.max_lazy_peers, Some(20));
    }

    #[test]
    fn test_lan_preset_no_ring_protection() {
        let config = PlumtreeConfig::lan();
        assert!(!config.protect_ring_neighbors);
        assert!(!config.use_hash_ring);
    }

    #[test]
    fn test_wan_preset_with_ring_protection() {
        let config = PlumtreeConfig::wan();
        assert!(config.protect_ring_neighbors);
        assert!(config.use_hash_ring);
    }

    #[test]
    fn test_large_cluster_preset_with_limits() {
        let config = PlumtreeConfig::large_cluster();
        assert!(config.protect_ring_neighbors);
        assert!(config.use_hash_ring);
        assert_eq!(config.max_eager_peers, Some(8));
        assert_eq!(config.max_lazy_peers, Some(50));
    }

    #[test]
    fn test_sync_config_default() {
        let config = SyncConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.sync_interval, Duration::from_secs(30));
        assert_eq!(config.sync_window, Duration::from_secs(90));
        assert_eq!(config.max_batch_size, 100);
    }

    #[test]
    fn test_sync_config_builder() {
        let config = SyncConfig::new()
            .with_enabled(true)
            .with_sync_interval(Duration::from_secs(60))
            .with_sync_window(Duration::from_secs(180))
            .with_max_batch_size(200);

        assert!(config.enabled);
        assert_eq!(config.sync_interval, Duration::from_secs(60));
        assert_eq!(config.sync_window, Duration::from_secs(180));
        assert_eq!(config.max_batch_size, 200);
    }

    #[test]
    fn test_sync_config_enabled_preset() {
        let config = SyncConfig::enabled();
        assert!(config.enabled);
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.max_messages, 100_000);
        assert_eq!(config.retention, Duration::from_secs(300));
        assert!(config.path.is_none());
    }

    #[test]
    fn test_storage_config_builder() {
        let config = StorageConfig::new()
            .with_enabled(true)
            .with_max_messages(50_000)
            .with_retention(Duration::from_secs(600))
            .with_path("/tmp/plumtree");

        assert!(config.enabled);
        assert_eq!(config.max_messages, 50_000);
        assert_eq!(config.retention, Duration::from_secs(600));
        assert_eq!(
            config.path,
            Some(std::path::PathBuf::from("/tmp/plumtree"))
        );
    }
}
