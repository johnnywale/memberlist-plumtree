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

#[cfg(feature = "quic")]
use bytes::Bytes;
#[cfg(feature = "quic")]
use nodecraft::Id;
#[cfg(feature = "quic")]
use std::sync::Arc;
#[cfg(feature = "quic")]
use tokio::task::JoinHandle;

#[cfg(feature = "quic")]
use crate::{
    transport::quic::{
        CleanupTaskHandle, ConnectionEvent, IncomingConfig, IncomingHandle, MapPeerResolver,
        QuicError, QuicStats, QuicTransport,
    },
    IdCodec, MessageId, PlumtreeDelegate, PlumtreeDiscovery, Result,
};

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

// ============================================================================
// PlumtreeStack - High-level abstraction for running Plumtree with QUIC
// ============================================================================

/// A running Plumtree stack with QUIC transport.
///
/// This is the high-level entry point for running Plumtree standalone
/// (without Memberlist) using QUIC transport. It manages:
/// - QUIC transport for peer-to-peer communication
/// - PlumtreeDiscovery for protocol state
/// - All background tasks (IHave scheduler, Graft timer, etc.)
/// - Incoming message handling
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::{
///     PlumtreeStackConfig, PlumtreeConfig, QuicConfig,
///     discovery::StaticDiscovery,
/// };
///
/// // Build the stack
/// let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse()?)
///     .with_plumtree(PlumtreeConfig::default())
///     .with_quic(QuicConfig::insecure_dev())
///     .with_discovery(StaticDiscovery::from_seeds(vec![
///         (2u64, "192.168.1.10:9000".parse()?),
///     ]));
///
/// let stack = config.build(MyDelegate).await?;
///
/// // Broadcast messages
/// let msg_id = stack.broadcast(b"Hello cluster!").await?;
///
/// // Shutdown
/// stack.shutdown().await;
/// ```
#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub struct PlumtreeStack<I, PD>
where
    I: Id + IdCodec + Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// The underlying PlumtreeDiscovery instance.
    pm: Arc<PlumtreeDiscovery<I, PD>>,

    /// QUIC transport for peer communication.
    transport: QuicTransport<I, MapPeerResolver<I>>,

    /// Shared peer resolver (for address updates).
    resolver: Arc<MapPeerResolver<I>>,

    /// Handle for incoming message acceptor.
    incoming_handle: Option<IncomingHandle>,

    /// Handle for cleanup task.
    cleanup_handle: Option<CleanupTaskHandle>,

    /// Background task handles.
    runner_handle: Option<JoinHandle<()>>,
    incoming_processor_handle: Option<JoinHandle<()>>,
    quic_forwarder_handle: Option<JoinHandle<()>>,

    /// Connection event receiver (optional).
    event_rx: Option<async_channel::Receiver<ConnectionEvent<I>>>,

    /// Local node ID.
    local_id: I,

    /// Bind address.
    bind_addr: SocketAddr,
}

#[cfg(feature = "quic")]
impl<I, D> PlumtreeStackConfig<I, D>
where
    I: Id + IdCodec + Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: ClusterDiscovery<I>,
{
    /// Build and start the Plumtree stack with the given delegate.
    ///
    /// This creates:
    /// - QUIC transport bound to `bind_addr`
    /// - PlumtreeDiscovery with the configured settings
    /// - Background tasks for message processing
    /// - Incoming connection acceptor
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - QUIC transport fails to bind
    /// - TLS configuration fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeStackConfig, PlumtreeConfig, QuicConfig, NoopDelegate};
    ///
    /// let config = PlumtreeStackConfig::new(1u64, "127.0.0.1:9000".parse().unwrap())
    ///     .with_quic(QuicConfig::insecure_dev());
    ///
    /// let stack = config.build(NoopDelegate).await?;
    /// stack.broadcast(b"Hello").await?;
    /// ```
    pub async fn build<PD>(
        self,
        delegate: PD,
    ) -> std::result::Result<PlumtreeStack<I, PD>, QuicError>
    where
        PD: PlumtreeDelegate<I>,
    {
        // 1. Create shared resolver with discovery seeds
        let resolver = Arc::new(MapPeerResolver::new(self.bind_addr));
        for (peer_id, addr) in self.discovery.initial_peers() {
            if peer_id != self.local_id {
                resolver.add_peer(peer_id, addr);
            }
        }

        // 2. Create QUIC transport
        let transport = QuicTransport::with_shared_resolver(
            self.bind_addr,
            self.quic.clone(),
            resolver.clone(),
        )
        .await?;

        // 3. Subscribe to connection events
        let (event_rx, transport) = transport.subscribe_events(256);

        // 4. Start incoming acceptor
        let (incoming_rx, incoming_handle) =
            transport.start_incoming::<I>(IncomingConfig::default());

        // 5. Start cleanup task
        let cleanup_handle = transport.spawn_cleanup_task_default();

        // 6. Create PlumtreeDiscovery
        let pm = Arc::new(PlumtreeDiscovery::new(
            self.local_id.clone(),
            self.plumtree.clone(),
            delegate,
        ));

        // 7. Add initial peers from discovery
        for (peer_id, _) in self.discovery.initial_peers() {
            if peer_id != self.local_id {
                pm.add_peer(peer_id);
            }
        }

        // 8. Forward QUIC incoming -> PlumtreeDiscovery
        let pm_sender = pm.incoming_sender();
        let quic_forwarder_handle = tokio::spawn(async move {
            while let Ok((sender, msg)) = incoming_rx.recv().await {
                if pm_sender.send((sender, msg)).await.is_err() {
                    break;
                }
            }
        });

        // 9. Start Plumtree runner with transport
        let pm_clone = pm.clone();
        let transport_clone = transport.clone();
        let runner_handle = tokio::spawn(async move {
            pm_clone.run_with_transport(transport_clone).await;
        });

        // 10. Start incoming processor
        let pm_incoming = pm.clone();
        let incoming_processor_handle = tokio::spawn(async move {
            pm_incoming.run_incoming_processor().await;
        });

        Ok(PlumtreeStack {
            pm,
            transport,
            resolver,
            incoming_handle: Some(incoming_handle),
            cleanup_handle: Some(cleanup_handle),
            runner_handle: Some(runner_handle),
            incoming_processor_handle: Some(incoming_processor_handle),
            quic_forwarder_handle: Some(quic_forwarder_handle),
            event_rx: Some(event_rx),
            local_id: self.local_id,
            bind_addr: self.bind_addr,
        })
    }
}

#[cfg(feature = "quic")]
impl<I, PD> PlumtreeStack<I, PD>
where
    I: Id + IdCodec + Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// Broadcast a message to all peers.
    ///
    /// Uses Plumtree's efficient O(n) spanning tree broadcast.
    pub async fn broadcast(&self, payload: impl Into<Bytes>) -> Result<MessageId> {
        self.pm.broadcast(payload).await
    }

    /// Add a peer to the topology.
    ///
    /// The peer is added to both the resolver (for address mapping) and
    /// the Plumtree peer state (for message routing).
    pub fn add_peer(&self, peer: I, addr: SocketAddr) {
        self.resolver.add_peer(peer.clone(), addr);
        self.pm.add_peer(peer);
    }

    /// Remove a peer from the topology.
    pub fn remove_peer(&self, peer: &I) {
        self.resolver.remove_peer(peer);
        self.pm.remove_peer(peer);
    }

    /// Get the peer resolver for address management.
    pub fn resolver(&self) -> &Arc<MapPeerResolver<I>> {
        &self.resolver
    }

    /// Get the underlying PlumtreeDiscovery.
    pub fn plumtree(&self) -> &Arc<PlumtreeDiscovery<I, PD>> {
        &self.pm
    }

    /// Get the QUIC transport.
    pub fn transport(&self) -> &QuicTransport<I, MapPeerResolver<I>> {
        &self.transport
    }

    /// Get transport statistics.
    pub async fn transport_stats(&self) -> QuicStats {
        self.transport.stats().await
    }

    /// Take the connection event receiver (can only be called once).
    ///
    /// Use this to monitor connection lifecycle events:
    /// - New connections established
    /// - Connections closed or lost
    /// - Connection migrations (address changes)
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(events) = stack.take_event_receiver() {
    ///     tokio::spawn(async move {
    ///         while let Ok(event) = events.recv().await {
    ///             match event {
    ///                 ConnectionEvent::Connected { peer, rtt, .. } => {
    ///                     println!("Connected to {:?} (RTT: {:?})", peer, rtt);
    ///                 }
    ///                 ConnectionEvent::Disconnected { peer, reason } => {
    ///                     println!("Disconnected from {:?}: {:?}", peer, reason);
    ///                 }
    ///                 _ => {}
    ///             }
    ///         }
    ///     });
    /// }
    /// ```
    pub fn take_event_receiver(&mut self) -> Option<async_channel::Receiver<ConnectionEvent<I>>> {
        self.event_rx.take()
    }

    /// Get the local node ID.
    pub fn local_id(&self) -> &I {
        &self.local_id
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Get peer statistics.
    pub fn peer_stats(&self) -> crate::peer_state::PeerStats {
        self.pm.peer_stats()
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> crate::message::CacheStats {
        self.pm.cache_stats()
    }

    /// Check if the stack has been shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.pm.is_shutdown()
    }

    /// Shutdown the stack.
    ///
    /// This stops all background tasks and closes the QUIC transport.
    pub async fn shutdown(mut self) {
        // Shutdown Plumtree first to stop generating new messages
        self.pm.shutdown();

        // Stop background tasks
        if let Some(h) = self.runner_handle.take() {
            h.abort();
        }
        if let Some(h) = self.incoming_processor_handle.take() {
            h.abort();
        }
        if let Some(h) = self.quic_forwarder_handle.take() {
            h.abort();
        }

        // Stop incoming acceptor
        if let Some(h) = self.incoming_handle.take() {
            h.stop();
        }

        // Stop cleanup
        if let Some(h) = self.cleanup_handle.take() {
            h.stop();
        }

        // Shutdown transport
        self.transport.shutdown().await;
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
        let discovery_config =
            StaticDiscoveryConfig::new().with_seed(2u64, "192.168.1.10:9000".parse().unwrap());

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
