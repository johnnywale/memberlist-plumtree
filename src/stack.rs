//! Standalone Plumtree stack configuration and types.
//!
//! This module provides the high-level configuration for running Plumtree
//! standalone without memberlist, using QUIC transport and pluggable discovery.
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{
//!     PlumtreeStackConfig, PlumtreeConfig, QuicConfig,
//!     discovery::{StaticDiscovery, StaticDiscoveryConfig},
//! };
//!
//! // Using static discovery with known peer addresses
//! let discovery = StaticDiscovery::from_seeds(vec![
//!     (1u64, "192.168.1.10:9000".parse().unwrap()),
//!     (2u64, "192.168.1.11:9000".parse().unwrap()),
//! ]);
//!
//! let config = PlumtreeStackConfig::new(0u64, "0.0.0.0:9000".parse().unwrap())
//!     .with_discovery(discovery)
//!     .with_plumtree(PlumtreeConfig::default())
//!     .with_quic(QuicConfig::insecure_dev());
//! ```

use crate::discovery::{ClusterDiscovery, NoOpDiscovery};
use crate::PlumtreeConfig;
#[cfg(feature = "quic")]
use crate::QuicConfig;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;

/// Configuration for a standalone Plumtree stack.
///
/// This combines all the configuration needed to run Plumtree without memberlist:
/// - Local node identity
/// - Network bind address
/// - Peer discovery mechanism
/// - Plumtree protocol settings
/// - QUIC transport settings
///
/// # Type Parameters
///
/// * `I` - The node ID type (e.g., `u64`, `String`)
/// * `D` - The discovery mechanism type
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::{
///     PlumtreeStackConfig, PlumtreeConfig,
///     discovery::NoOpDiscovery,
/// };
///
/// // Single-node deployment with no discovery
/// let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap());
///
/// // With custom settings
/// let config = PlumtreeStackConfig::new(1u64, "0.0.0.0:9000".parse().unwrap())
///     .with_plumtree(PlumtreeConfig::lan());
/// ```
#[derive(Debug, Clone)]
pub struct PlumtreeStackConfig<I, D = NoOpDiscovery<I>>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    D: ClusterDiscovery<I>,
{
    /// Local node ID.
    pub local_id: I,

    /// Address to bind for QUIC transport.
    pub bind_addr: SocketAddr,

    /// Peer discovery mechanism.
    pub discovery: D,

    /// Plumtree protocol configuration.
    pub plumtree: PlumtreeConfig,

    /// QUIC transport configuration.
    #[cfg(feature = "quic")]
    pub quic: QuicConfig,
}

impl<I> PlumtreeStackConfig<I, NoOpDiscovery<I>>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    /// Create a new stack configuration with defaults.
    ///
    /// Uses no-op discovery (manual peer management) by default.
    ///
    /// # Arguments
    ///
    /// * `local_id` - The local node's ID
    /// * `bind_addr` - Address to bind for QUIC transport
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::PlumtreeStackConfig;
    ///
    /// let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap());
    /// ```
    pub fn new(local_id: I, bind_addr: SocketAddr) -> Self {
        Self {
            local_id,
            bind_addr,
            discovery: NoOpDiscovery::new(bind_addr),
            plumtree: PlumtreeConfig::default(),
            #[cfg(feature = "quic")]
            quic: QuicConfig::default(),
        }
    }
}

impl<I, D> PlumtreeStackConfig<I, D>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    D: ClusterDiscovery<I>,
{
    /// Set a custom discovery mechanism (builder pattern).
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::{
    ///     PlumtreeStackConfig,
    ///     discovery::{StaticDiscovery, StaticDiscoveryConfig},
    /// };
    ///
    /// let discovery = StaticDiscovery::from_seeds(vec![
    ///     (1u64, "192.168.1.10:9000".parse().unwrap()),
    /// ]);
    ///
    /// let config = PlumtreeStackConfig::new(0u64, "0.0.0.0:9000".parse().unwrap())
    ///     .with_discovery(discovery);
    /// ```
    pub fn with_discovery<D2>(self, discovery: D2) -> PlumtreeStackConfig<I, D2>
    where
        D2: ClusterDiscovery<I>,
    {
        PlumtreeStackConfig {
            local_id: self.local_id,
            bind_addr: self.bind_addr,
            discovery,
            plumtree: self.plumtree,
            #[cfg(feature = "quic")]
            quic: self.quic,
        }
    }

    /// Set Plumtree protocol configuration (builder pattern).
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::{PlumtreeStackConfig, PlumtreeConfig};
    ///
    /// let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap())
    ///     .with_plumtree(PlumtreeConfig::lan());
    /// ```
    pub fn with_plumtree(mut self, plumtree: PlumtreeConfig) -> Self {
        self.plumtree = plumtree;
        self
    }

    /// Set QUIC transport configuration (builder pattern).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeStackConfig, QuicConfig};
    ///
    /// let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap())
    ///     .with_quic(QuicConfig::insecure_dev());
    /// ```
    #[cfg(feature = "quic")]
    pub fn with_quic(mut self, quic: QuicConfig) -> Self {
        self.quic = quic;
        self
    }

    /// Set the bind address (builder pattern).
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    /// Set the local node ID (builder pattern).
    pub fn with_local_id(mut self, id: I) -> Self {
        self.local_id = id;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::{StaticDiscovery, StaticDiscoveryConfig};

    #[test]
    fn test_default_config() {
        let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap());

        assert_eq!(config.local_id, 1);
        assert_eq!(config.bind_addr, "127.0.0.1:9000".parse().unwrap());
    }

    #[test]
    fn test_with_plumtree_config() {
        let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap())
            .with_plumtree(PlumtreeConfig::lan());

        assert_eq!(config.plumtree.eager_fanout, 3);
    }

    #[test]
    fn test_with_static_discovery() {
        let discovery = StaticDiscovery::from_seeds(vec![
            (2u64, "192.168.1.10:9000".parse().unwrap()),
            (3u64, "192.168.1.11:9000".parse().unwrap()),
        ]);

        let config = PlumtreeStackConfig::new(1u64, "0.0.0.0:9000".parse().unwrap())
            .with_discovery(discovery);

        // Verify the config has discovery (can't directly inspect StaticDiscovery internals)
        assert_eq!(config.local_id, 1);
    }

    #[test]
    fn test_builder_chain() {
        let discovery_config = StaticDiscoveryConfig::new()
            .with_seed(2u64, "192.168.1.10:9000".parse().unwrap());

        let _config = PlumtreeStackConfig::new(1u64, "0.0.0.0:9000".parse().unwrap())
            .with_plumtree(PlumtreeConfig::wan())
            .with_discovery(StaticDiscovery::new(discovery_config))
            .with_bind_addr("0.0.0.0:9001".parse().unwrap());
    }

    #[cfg(feature = "quic")]
    #[test]
    fn test_with_quic_config() {
        let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap())
            .with_quic(QuicConfig::insecure_dev());

        // Just verify it compiles and works
        assert_eq!(config.local_id, 1);
    }
}
