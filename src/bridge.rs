//! Fully automated Plumtree-Memberlist integration bridge.
//!
//! This module provides automatic synchronization between Memberlist's membership
//! events and Plumtree's peer topology. When a node joins or leaves the cluster
//! via Memberlist, the bridge automatically updates Plumtree's neighbor table
//! and (optionally) the QUIC address resolver.
//!
//! # Architecture
//!
//! The bridge acts as an intermediary that:
//! 1. Implements Memberlist's `EventDelegate` to receive membership events
//! 2. Automatically adds/removes peers from Plumtree's topology
//! 3. Updates the address resolver for QUIC transport (when enabled)
//! 4. Provides a clean API for starting the full integration stack
//!
//! # Seed Recovery ("Lazarus" Feature)
//!
//! The bridge supports automatic recovery of seed nodes that have failed and restarted.
//! When enabled, a background task periodically probes configured static seeds and
//! attempts to rejoin them if they're not in the cluster's alive set.
//!
//! This solves the "Ghost Seed" problem where a restarted seed node remains isolated
//! because the cluster stopped pinging it after marking it dead.
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{
//!     PlumtreeBridge, BridgeConfig, PlumtreeConfig,
//!     PlumtreeDiscovery, NoopDelegate,
//! };
//! use std::sync::Arc;
//! use std::net::SocketAddr;
//!
//! // Create the Plumtree instance
//! let local_id: u64 = 1;
//! let pm = Arc::new(PlumtreeDiscovery::new(
//!     local_id,
//!     PlumtreeConfig::lan(),
//!     NoopDelegate,
//! ));
//!
//! // Create the bridge with static seeds for automatic recovery
//! let config = BridgeConfig::new()
//!     .with_static_seeds(vec![
//!         "192.168.1.100:7946".parse().unwrap(),
//!         "192.168.1.101:7946".parse().unwrap(),
//!     ])
//!     .with_lazarus_enabled(true);
//!
//! let bridge = PlumtreeBridge::with_config(pm.clone(), config);
//! ```
//!
//! # With QUIC Transport
//!
//! ```ignore
//! use memberlist_plumtree::{
//!     PlumtreeBridge, PlumtreeConfig, PlumtreeDiscovery,
//!     QuicTransport, QuicConfig, PooledTransport, PoolConfig,
//!     MapPeerResolver, NoopDelegate,
//! };
//! use std::sync::Arc;
//!
//! let local_addr: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
//! let resolver = Arc::new(MapPeerResolver::new(local_addr));
//!
//! let pm = Arc::new(PlumtreeDiscovery::new(1u64, PlumtreeConfig::lan(), NoopDelegate));
//!
//! // Create bridge with resolver for automatic address updates
//! let bridge = PlumtreeBridge::with_resolver(pm.clone(), resolver.clone());
//!
//! // Start the full stack
//! // bridge.start_stack(transport, memberlist).await;
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_lock::RwLock;
use bytes::Bytes;

// Import types from discovery module
use crate::discovery::Id;
use crate::{
    decode_plumtree_envelope, IdCodec, PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery,
    PlumtreeMessage,
};

#[cfg(not(feature = "quic"))]
use std::marker::PhantomData;

#[cfg(feature = "quic")]
use crate::MapPeerResolver;

/// Configuration for the Plumtree bridge.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Whether to log topology changes.
    pub log_changes: bool,
    /// Whether to automatically promote new peers to eager based on fanout settings.
    pub auto_promote: bool,
    /// Static seed addresses for the cluster.
    ///
    /// These seeds will be periodically probed by the Lazarus task to ensure
    /// that restarted seeds are automatically rediscovered.
    pub static_seeds: Vec<SocketAddr>,
    /// Enable the Lazarus background task for seed recovery.
    ///
    /// When enabled, the bridge will periodically check if static seeds are
    /// alive and attempt to rejoin them if they're missing from the cluster.
    pub lazarus_enabled: bool,
    /// Interval between Lazarus probes for dead seeds.
    ///
    /// Default: 30 seconds
    pub lazarus_interval: Duration,
    /// Path to persist known peers for recovery after restart.
    ///
    /// If set, the bridge will save known peer addresses to this file
    /// on shutdown and load them on startup.
    pub persistence_path: Option<PathBuf>,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            log_changes: true,
            auto_promote: true,
            static_seeds: Vec::new(),
            lazarus_enabled: false,
            lazarus_interval: Duration::from_secs(30),
            persistence_path: None,
        }
    }
}

impl BridgeConfig {
    /// Create a new bridge configuration with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable logging of topology changes.
    pub fn with_log_changes(mut self, log: bool) -> Self {
        self.log_changes = log;
        self
    }

    /// Enable or disable automatic promotion of new peers.
    pub fn with_auto_promote(mut self, auto: bool) -> Self {
        self.auto_promote = auto;
        self
    }

    /// Set static seed addresses for the cluster.
    ///
    /// These seeds will be periodically probed by the Lazarus task (if enabled)
    /// to ensure restarted seeds are automatically rediscovered.
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::BridgeConfig;
    ///
    /// let config = BridgeConfig::new()
    ///     .with_static_seeds(vec![
    ///         "192.168.1.100:7946".parse().unwrap(),
    ///         "192.168.1.101:7946".parse().unwrap(),
    ///     ]);
    /// ```
    pub fn with_static_seeds(mut self, seeds: Vec<SocketAddr>) -> Self {
        self.static_seeds = seeds;
        self
    }

    /// Enable or disable the Lazarus background task for seed recovery.
    ///
    /// When enabled, the bridge will periodically probe static seeds and
    /// attempt to rejoin any that are not in the cluster's alive set.
    pub fn with_lazarus_enabled(mut self, enabled: bool) -> Self {
        self.lazarus_enabled = enabled;
        self
    }

    /// Set the interval between Lazarus probes for dead seeds.
    ///
    /// Default: 30 seconds. Lower values provide faster recovery but
    /// increase network overhead.
    pub fn with_lazarus_interval(mut self, interval: Duration) -> Self {
        self.lazarus_interval = interval;
        self
    }

    /// Set the path for persisting known peers.
    ///
    /// When set, the bridge will save known peer addresses to this file
    /// on shutdown and load them on startup, providing additional bootstrap
    /// options beyond static seeds.
    pub fn with_persistence_path(mut self, path: PathBuf) -> Self {
        self.persistence_path = Some(path);
        self
    }

    /// Check if Lazarus probing is enabled and has seeds configured.
    pub fn should_run_lazarus(&self) -> bool {
        self.lazarus_enabled && !self.static_seeds.is_empty()
    }
}

/// Plumtree-Memberlist integration bridge.
///
/// This struct holds the Plumtree instance and provides automatic synchronization
/// with Memberlist's membership events. When used as an `EventDelegate`, it
/// automatically updates Plumtree's topology when nodes join or leave.
///
/// # Type Parameters
///
/// - `I`: The node identifier type (must implement `Id`, `IdCodec`, etc.)
/// - `PD`: The Plumtree delegate type for message delivery
pub struct PlumtreeBridge<I, PD>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// The Plumtree instance.
    pub pm: Arc<PlumtreeDiscovery<I, PD>>,
    /// Configuration for the bridge.
    pub config: BridgeConfig,
    /// Address resolver for QUIC transport (optional).
    #[cfg(feature = "quic")]
    pub resolver: Option<Arc<MapPeerResolver<I>>>,
    #[cfg(not(feature = "quic"))]
    _marker: PhantomData<I>,
}

impl<I, PD> PlumtreeBridge<I, PD>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// Create a new bridge without an address resolver.
    ///
    /// Use this when not using QUIC transport or when handling address
    /// resolution separately.
    pub fn new(pm: Arc<PlumtreeDiscovery<I, PD>>) -> Self {
        Self {
            pm,
            config: BridgeConfig::default(),
            #[cfg(feature = "quic")]
            resolver: None,
            #[cfg(not(feature = "quic"))]
            _marker: PhantomData,
        }
    }

    /// Create a new bridge with custom configuration.
    pub fn with_config(pm: Arc<PlumtreeDiscovery<I, PD>>, config: BridgeConfig) -> Self {
        Self {
            pm,
            config,
            #[cfg(feature = "quic")]
            resolver: None,
            #[cfg(not(feature = "quic"))]
            _marker: PhantomData,
        }
    }

    /// Create a new bridge with an address resolver for QUIC transport.
    ///
    /// When nodes join or leave, the resolver is automatically updated
    /// with their addresses.
    #[cfg(feature = "quic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
    pub fn with_resolver(
        pm: Arc<PlumtreeDiscovery<I, PD>>,
        resolver: Arc<MapPeerResolver<I>>,
    ) -> Self {
        Self {
            pm,
            config: BridgeConfig::default(),
            resolver: Some(resolver),
        }
    }

    /// Create a new bridge with both custom configuration and address resolver.
    #[cfg(feature = "quic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
    pub fn with_config_and_resolver(
        pm: Arc<PlumtreeDiscovery<I, PD>>,
        config: BridgeConfig,
        resolver: Arc<MapPeerResolver<I>>,
    ) -> Self {
        Self {
            pm,
            config,
            resolver: Some(resolver),
        }
    }

    /// Get a reference to the underlying PlumtreeDiscovery.
    pub fn plumtree(&self) -> &Arc<PlumtreeDiscovery<I, PD>> {
        &self.pm
    }

    /// Get the address resolver (if configured).
    #[cfg(feature = "quic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
    pub fn resolver(&self) -> Option<&Arc<MapPeerResolver<I>>> {
        self.resolver.as_ref()
    }

    /// Get the current peer statistics from Plumtree.
    pub fn peer_stats(&self) -> crate::PeerStats {
        self.pm.peer_stats()
    }

    /// Get the current peer topology from Plumtree.
    pub fn topology(&self) -> crate::PeerTopology<I> {
        self.pm.peers().topology()
    }

    /// Manually add a peer to the topology.
    ///
    /// This is normally not needed when using the bridge as an EventDelegate,
    /// as peers are added automatically on join events.
    pub fn add_peer(&self, peer: I) {
        self.pm.add_peer(peer);
    }

    /// Manually remove a peer from the topology.
    ///
    /// This is normally not needed when using the bridge as an EventDelegate,
    /// as peers are removed automatically on leave events.
    pub fn remove_peer(&self, peer: &I) {
        self.pm.remove_peer(peer);
    }

    /// Broadcast a message to all nodes in the cluster.
    pub async fn broadcast(&self, payload: impl Into<Bytes>) -> crate::Result<crate::MessageId> {
        self.pm.broadcast(payload).await
    }

    /// Get the sender for incoming messages.
    ///
    /// Use this to inject messages received from Memberlist into Plumtree.
    pub fn incoming_sender(&self) -> async_channel::Sender<(I, crate::PlumtreeMessage)> {
        self.pm.incoming_sender()
    }

    /// Shutdown the bridge and underlying Plumtree instance.
    pub fn shutdown(&self) {
        self.pm.shutdown();
    }

    /// Check if the bridge is shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.pm.is_shutdown()
    }

    /// Get the local node ID.
    pub fn local_id(&self) -> &I {
        self.pm.plumtree().local_id()
    }

    /// Create a new bridge with the given delegate.
    ///
    /// This creates the underlying `PlumtreeDiscovery` internally.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeBridge, PlumtreeConfig, NoopDelegate};
    ///
    /// let bridge = PlumtreeBridge::create(node_id, PlumtreeConfig::default(), NoopDelegate);
    /// ```
    pub fn create(local_id: I, config: PlumtreeConfig, delegate: PD) -> Self {
        let pm = Arc::new(PlumtreeDiscovery::new(local_id, config, delegate));
        Self::new(pm)
    }

    /// Start the bridge with a shared router for local message routing.
    ///
    /// This method:
    /// 1. Registers with the router for message dispatch
    /// 2. Creates a `ChannelTransport` for message sending
    /// 3. Starts the routing task to forward messages to other nodes
    /// 4. Starts the Plumtree runner (processes outgoing messages)
    /// 5. Starts the incoming processor (handles received messages)
    ///
    /// # Arguments
    ///
    /// * `router` - Shared router for dispatching messages between local nodes
    ///
    /// # Returns
    ///
    /// A [`BridgeTaskHandle`] that can be used to stop the background tasks.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeBridge, BridgeRouter, PlumtreeConfig, NoopDelegate};
    /// use std::sync::Arc;
    ///
    /// let router = Arc::new(BridgeRouter::new());
    /// let bridge = PlumtreeBridge::create(node_id, PlumtreeConfig::default(), NoopDelegate);
    /// let handle = bridge.start(&router).await;
    ///
    /// // Send messages
    /// bridge.broadcast(b"Hello").await?;
    ///
    /// // Shutdown
    /// handle.stop();
    /// bridge.shutdown();
    /// ```
    #[cfg(feature = "tokio")]
    pub async fn start(&self, router: &BridgeRouter<I>) -> BridgeTaskHandle {
        let local_id = self.pm.plumtree().local_id().clone();

        // 1. Register with router
        router
            .register(local_id.clone(), self.pm.incoming_sender())
            .await;

        // 2. Create ChannelTransport
        let (transport, transport_rx) = crate::ChannelTransport::<I>::bounded(1000);

        // 3. Start routing task
        let routing_handle = router.start_routing_task(local_id, transport_rx);

        // 4. Start Plumtree runner with transport
        let pm_clone = self.pm.clone();
        let runner_handle = tokio::spawn(async move {
            pm_clone.run_with_transport(transport).await;
        });

        // 5. Start incoming processor
        let pm_incoming = self.pm.clone();
        let incoming_handle = tokio::spawn(async move {
            pm_incoming.run_incoming_processor().await;
        });

        BridgeTaskHandle {
            routing_handle,
            runner_handle,
            incoming_handle,
        }
    }
}

impl<I, PD> Clone for PlumtreeBridge<I, PD>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    fn clone(&self) -> Self {
        Self {
            pm: self.pm.clone(),
            config: self.config.clone(),
            #[cfg(feature = "quic")]
            resolver: self.resolver.clone(),
            #[cfg(not(feature = "quic"))]
            _marker: PhantomData,
        }
    }
}

// ============================================================================
// Bridge Router (for multiple local nodes)
// ============================================================================

/// Shared router for multiple local Plumtree nodes.
///
/// This is useful for testing and demos where multiple nodes run in the same process.
/// Each node registers its `incoming_sender()` with the router, and the router dispatches
/// messages to the correct node.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::{BridgeRouter, PlumtreeBridge, PlumtreeDiscovery, PlumtreeConfig, NoopDelegate};
/// use std::sync::Arc;
///
/// // Create shared router
/// let router = Arc::new(BridgeRouter::<u64>::new());
///
/// // Create nodes and register with router
/// for i in 0..10 {
///     let pm = Arc::new(PlumtreeDiscovery::new(i, PlumtreeConfig::default(), NoopDelegate));
///     let bridge = PlumtreeBridge::new(pm.clone());
///
///     // Register this node's incoming sender with the router
///     router.register(i, pm.incoming_sender()).await;
///
///     // Create transport and start routing task
///     let (transport, transport_rx) = ChannelTransport::bounded(1000);
///     router.start_routing_task(i, transport_rx);
///
///     // Start the Plumtree runner
///     tokio::spawn(async move { pm.run_with_transport(transport).await });
/// }
/// ```
pub struct BridgeRouter<I>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
{
    /// Map of node ID to their incoming message sender
    incoming_senders: Arc<RwLock<HashMap<I, async_channel::Sender<(I, PlumtreeMessage)>>>>,
}

impl<I> BridgeRouter<I>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
{
    /// Create a new bridge router.
    pub fn new() -> Self {
        Self {
            incoming_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a node's incoming message sender with the router.
    ///
    /// Messages destined for this node will be dispatched to the provided sender.
    pub async fn register(&self, node_id: I, sender: async_channel::Sender<(I, PlumtreeMessage)>) {
        self.incoming_senders.write().await.insert(node_id, sender);
    }

    /// Unregister a node from the router.
    ///
    /// Call this when a node goes offline to stop routing messages to it.
    pub async fn unregister(&self, node_id: &I) {
        self.incoming_senders.write().await.remove(node_id);
    }

    /// Start a routing task that reads from a transport receiver and routes messages.
    ///
    /// This task reads outgoing messages from the given transport receiver, decodes
    /// the target node from the envelope, and dispatches the message to that node's
    /// `incoming_sender()`.
    ///
    /// # Arguments
    ///
    /// * `sender_id` - The ID of the node sending messages
    /// * `transport_rx` - Receiver for outgoing messages from ChannelTransport
    ///
    /// # Returns
    ///
    /// A task handle that can be used to abort the routing task.
    #[cfg(feature = "tokio")]
    pub fn start_routing_task(
        &self,
        sender_id: I,
        transport_rx: async_channel::Receiver<(I, Bytes)>,
    ) -> tokio::task::JoinHandle<()> {
        let router = self.incoming_senders.clone();

        tokio::spawn(async move {
            while let Ok((target, data)) = transport_rx.recv().await {
                // Decode the envelope to get sender and message
                if let Some((_envelope_sender, message)) = decode_plumtree_envelope::<I>(&data) {
                    let senders = router.read().await;
                    if let Some(tx) = senders.get(&target) {
                        let _ = tx.send((sender_id.clone(), message)).await;
                    }
                }
            }
        })
    }

    /// Get the number of registered nodes.
    pub async fn node_count(&self) -> usize {
        self.incoming_senders.read().await.len()
    }

    /// Check if a node is registered.
    pub async fn is_registered(&self, node_id: &I) -> bool {
        self.incoming_senders.read().await.contains_key(node_id)
    }
}

impl<I> Default for BridgeRouter<I>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<I> Clone for BridgeRouter<I>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            incoming_senders: self.incoming_senders.clone(),
        }
    }
}

// ============================================================================
// Bridge Task Handle
// ============================================================================

/// Handle for background tasks started by [`PlumtreeBridge::start()`].
///
/// This handle manages three background tasks:
/// 1. **Routing task**: Dispatches outgoing messages to target nodes via the router
/// 2. **Runner task**: Processes outgoing messages from Plumtree (IHave batching, Graft timer, etc.)
/// 3. **Incoming task**: Processes incoming messages from other nodes
///
/// Call [`stop()`](Self::stop) to abort all tasks when shutting down.
///
/// # Example
///
/// ```ignore
/// let handle = bridge.start(&router).await;
///
/// // ... use the bridge ...
///
/// // Shutdown
/// handle.stop();
/// bridge.shutdown();
/// ```
#[cfg(feature = "tokio")]
pub struct BridgeTaskHandle {
    routing_handle: tokio::task::JoinHandle<()>,
    runner_handle: tokio::task::JoinHandle<()>,
    incoming_handle: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "tokio")]
impl BridgeTaskHandle {
    /// Stop all background tasks.
    ///
    /// This aborts the routing, runner, and incoming processor tasks.
    /// After calling this, you should also call `bridge.shutdown()` to
    /// clean up the underlying Plumtree instance.
    pub fn stop(&self) {
        self.routing_handle.abort();
        self.runner_handle.abort();
        self.incoming_handle.abort();
    }

    /// Check if all tasks are finished.
    ///
    /// Returns `true` if all background tasks have completed (either normally or via abort).
    pub fn is_finished(&self) -> bool {
        self.routing_handle.is_finished()
            && self.runner_handle.is_finished()
            && self.incoming_handle.is_finished()
    }

    /// Wait for all tasks to complete.
    ///
    /// This is useful after calling `stop()` to ensure all tasks have been aborted.
    pub async fn join(self) {
        let _ = self.routing_handle.await;
        let _ = self.runner_handle.await;
        let _ = self.incoming_handle.await;
    }
}

// Extract socket address from NodeState's address.
// This is a helper trait to handle different address types.
/// Trait for extracting socket address from memberlist node addresses.
///
/// Requires the `memberlist` feature.
#[cfg(feature = "memberlist")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist")))]
pub trait AddressExtractor<A> {
    /// Extract the socket address from the address type.
    fn extract_addr(addr: &A) -> Option<SocketAddr>;
}

// Default implementation for types that can be converted to SocketAddr
#[cfg(feature = "memberlist")]
impl<A> AddressExtractor<A> for ()
where
    A: AsRef<std::net::SocketAddr>,
{
    fn extract_addr(addr: &A) -> Option<SocketAddr> {
        Some(*addr.as_ref())
    }
}

// ============================================================================
// Peer Persistence
// ============================================================================

/// Persistence module for saving and loading known peer addresses.
///
/// This provides crash recovery by allowing nodes to remember peers they've
/// seen, reducing dependency on static seeds after initial bootstrap.
pub mod persistence {
    use std::fs::{self, File};
    use std::io::{BufRead, BufReader, Write};
    use std::net::SocketAddr;
    use std::path::Path;

    /// Error type for persistence operations.
    #[derive(Debug)]
    pub enum PersistenceError {
        /// I/O error during file operations.
        Io(std::io::Error),
        /// Invalid address format in persisted data.
        InvalidAddress(String),
    }

    impl std::fmt::Display for PersistenceError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Io(e) => write!(f, "persistence I/O error: {}", e),
                Self::InvalidAddress(s) => write!(f, "invalid address in persistence: {}", s),
            }
        }
    }

    impl std::error::Error for PersistenceError {}

    impl From<std::io::Error> for PersistenceError {
        fn from(e: std::io::Error) -> Self {
            Self::Io(e)
        }
    }

    /// Save peer addresses to a file.
    ///
    /// Addresses are stored one per line in `ip:port` format.
    pub fn save_peers(path: &Path, peers: &[SocketAddr]) -> Result<(), PersistenceError> {
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut file = File::create(path)?;
        for peer in peers {
            writeln!(file, "{}", peer)?;
        }
        file.sync_all()?;
        Ok(())
    }

    /// Load peer addresses from a file.
    ///
    /// Returns an empty vector if the file doesn't exist.
    /// Invalid lines are logged and skipped.
    pub fn load_peers(path: &Path) -> Result<Vec<SocketAddr>, PersistenceError> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut peers = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }

            match trimmed.parse::<SocketAddr>() {
                Ok(addr) => peers.push(addr),
                Err(_) => {
                    tracing::warn!(
                        line = trimmed,
                        "skipping invalid address in peer persistence file"
                    );
                }
            }
        }

        Ok(peers)
    }

    /// Atomically update the peer persistence file.
    ///
    /// Writes to a temp file first, then renames to avoid corruption.
    pub fn save_peers_atomic(path: &Path, peers: &[SocketAddr]) -> Result<(), PersistenceError> {
        let temp_path = path.with_extension("tmp");
        save_peers(&temp_path, peers)?;
        fs::rename(&temp_path, path)?;
        Ok(())
    }
}

// ============================================================================
// Lazarus Task (Seed Recovery)
// ============================================================================

/// Statistics for the Lazarus seed recovery task.
#[derive(Debug, Clone, Default)]
pub struct LazarusStats {
    /// Number of probes sent to dead seeds.
    pub probes_sent: u64,
    /// Number of successful reconnections.
    pub reconnections: u64,
    /// Number of failed reconnection attempts.
    pub failures: u64,
    /// Number of seeds currently missing from the cluster.
    pub missing_seeds: usize,
}

/// Handle for controlling the Lazarus background task.
#[derive(Clone)]
pub struct LazarusHandle {
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    stats: Arc<parking_lot::RwLock<LazarusStats>>,
}

impl LazarusHandle {
    /// Create a new Lazarus handle.
    fn new() -> Self {
        Self {
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stats: Arc::new(parking_lot::RwLock::new(LazarusStats::default())),
        }
    }

    /// Signal the Lazarus task to shutdown.
    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get current Lazarus statistics.
    pub fn stats(&self) -> LazarusStats {
        self.stats.read().clone()
    }

    /// Update missing seeds count.
    #[cfg(feature = "memberlist")]
    pub(crate) fn set_missing_seeds(&self, count: usize) {
        self.stats.write().missing_seeds = count;
    }

    /// Record a probe sent.
    #[cfg(feature = "memberlist")]
    pub(crate) fn record_probe(&self) {
        self.stats.write().probes_sent += 1;
    }

    /// Record a successful reconnection.
    #[cfg(feature = "memberlist")]
    pub(crate) fn record_reconnection(&self) {
        self.stats.write().reconnections += 1;
    }

    /// Record a failed reconnection attempt.
    #[cfg(feature = "memberlist")]
    pub(crate) fn record_failure(&self) {
        self.stats.write().failures += 1;
    }
}

impl Default for LazarusHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoopDelegate, PlumtreeConfig};

    #[test]
    fn test_bridge_creation() {
        let pm = Arc::new(PlumtreeDiscovery::new(
            1u64,
            PlumtreeConfig::default(),
            NoopDelegate,
        ));
        let bridge = PlumtreeBridge::new(pm);
        assert!(!bridge.is_shutdown());
    }

    #[test]
    fn test_bridge_config() {
        let config = BridgeConfig::new()
            .with_log_changes(false)
            .with_auto_promote(false);

        assert!(!config.log_changes);
        assert!(!config.auto_promote);
    }

    #[cfg(feature = "quic")]
    #[test]
    fn test_bridge_with_resolver() {
        use std::net::SocketAddr;

        let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let resolver = Arc::new(MapPeerResolver::new(local_addr));

        let pm = Arc::new(PlumtreeDiscovery::new(
            1u64,
            PlumtreeConfig::default(),
            NoopDelegate,
        ));

        let bridge = PlumtreeBridge::with_resolver(pm, resolver.clone());
        assert!(bridge.resolver().is_some());
    }

    // ========================================================================
    // BridgeConfig Tests
    // ========================================================================

    #[test]
    fn test_bridge_config_static_seeds() {
        let seeds: Vec<SocketAddr> = vec![
            "192.168.1.100:7946".parse().unwrap(),
            "192.168.1.101:7946".parse().unwrap(),
        ];

        let config = BridgeConfig::new()
            .with_static_seeds(seeds.clone())
            .with_lazarus_enabled(true)
            .with_lazarus_interval(Duration::from_secs(60));

        assert_eq!(config.static_seeds.len(), 2);
        assert!(config.lazarus_enabled);
        assert_eq!(config.lazarus_interval, Duration::from_secs(60));
        assert!(config.should_run_lazarus());
    }

    #[test]
    fn test_bridge_config_should_run_lazarus() {
        // Disabled, no seeds
        let config = BridgeConfig::new();
        assert!(!config.should_run_lazarus());

        // Enabled, no seeds
        let config = BridgeConfig::new().with_lazarus_enabled(true);
        assert!(!config.should_run_lazarus());

        // Disabled, has seeds
        let seeds: Vec<SocketAddr> = vec!["192.168.1.100:7946".parse().unwrap()];
        let config = BridgeConfig::new().with_static_seeds(seeds.clone());
        assert!(!config.should_run_lazarus());

        // Enabled, has seeds
        let config = BridgeConfig::new()
            .with_static_seeds(seeds)
            .with_lazarus_enabled(true);
        assert!(config.should_run_lazarus());
    }

    #[test]
    fn test_bridge_config_persistence_path() {
        let config = BridgeConfig::new().with_persistence_path(PathBuf::from("/tmp/peers.txt"));

        assert_eq!(
            config.persistence_path,
            Some(PathBuf::from("/tmp/peers.txt"))
        );
    }

    // ========================================================================
    // Persistence Tests
    // ========================================================================

    #[test]
    fn test_persistence_save_and_load() {
        use std::fs;
        use tempfile::tempdir;

        // Create a temp directory for the test
        let dir = tempdir().unwrap();
        let path = dir.path().join("peers.txt");

        let peers: Vec<SocketAddr> = vec![
            "192.168.1.100:7946".parse().unwrap(),
            "10.0.0.1:7946".parse().unwrap(),
            "172.16.0.1:7946".parse().unwrap(),
        ];

        // Save peers
        persistence::save_peers(&path, &peers).unwrap();

        // Load peers
        let loaded = persistence::load_peers(&path).unwrap();

        assert_eq!(loaded.len(), 3);
        assert!(loaded.contains(&"192.168.1.100:7946".parse().unwrap()));
        assert!(loaded.contains(&"10.0.0.1:7946".parse().unwrap()));
        assert!(loaded.contains(&"172.16.0.1:7946".parse().unwrap()));

        // Cleanup
        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_persistence_load_nonexistent() {
        let path = PathBuf::from("/nonexistent/path/peers.txt");
        let loaded = persistence::load_peers(&path).unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_persistence_save_atomic() {
        use std::fs;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("peers.txt");

        let peers: Vec<SocketAddr> = vec![
            "192.168.1.100:7946".parse().unwrap(),
            "192.168.1.101:7946".parse().unwrap(),
        ];

        // Save atomically
        persistence::save_peers_atomic(&path, &peers).unwrap();

        // Verify file exists and can be loaded
        let loaded = persistence::load_peers(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        // Temp file should not exist
        let temp_path = path.with_extension("tmp");
        assert!(!temp_path.exists());

        // Cleanup
        fs::remove_file(&path).ok();
    }

    #[test]
    fn test_persistence_handles_comments_and_empty_lines() {
        use std::fs::{self, File};
        use std::io::Write;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let path = dir.path().join("peers.txt");

        // Write a file with comments and empty lines
        let mut file = File::create(&path).unwrap();
        writeln!(file, "# This is a comment").unwrap();
        writeln!(file, "192.168.1.100:7946").unwrap();
        writeln!(file).unwrap(); // Empty line
        writeln!(file, "   ").unwrap(); // Whitespace only
        writeln!(file, "# Another comment").unwrap();
        writeln!(file, "192.168.1.101:7946").unwrap();

        let loaded = persistence::load_peers(&path).unwrap();
        assert_eq!(loaded.len(), 2);

        // Cleanup
        fs::remove_file(&path).ok();
    }

    // ========================================================================
    // LazarusHandle Tests
    // ========================================================================

    #[test]
    fn test_lazarus_handle_creation() {
        let handle = LazarusHandle::new();
        assert!(!handle.is_shutdown());

        let stats = handle.stats();
        assert_eq!(stats.probes_sent, 0);
        assert_eq!(stats.reconnections, 0);
        assert_eq!(stats.failures, 0);
        assert_eq!(stats.missing_seeds, 0);
    }

    #[test]
    fn test_lazarus_handle_shutdown() {
        let handle = LazarusHandle::new();
        assert!(!handle.is_shutdown());

        handle.shutdown();
        assert!(handle.is_shutdown());
    }

    #[test]
    fn test_lazarus_handle_stats_recording() {
        let handle = LazarusHandle::new();

        handle.record_probe();
        handle.record_probe();
        handle.record_reconnection();
        handle.record_failure();
        handle.set_missing_seeds(3);

        let stats = handle.stats();
        assert_eq!(stats.probes_sent, 2);
        assert_eq!(stats.reconnections, 1);
        assert_eq!(stats.failures, 1);
        assert_eq!(stats.missing_seeds, 3);
    }

    #[test]
    fn test_lazarus_handle_clone() {
        let handle = LazarusHandle::new();
        let handle2 = handle.clone();

        // Both handles share the same state
        handle.record_probe();
        assert_eq!(handle2.stats().probes_sent, 1);

        handle2.shutdown();
        assert!(handle.is_shutdown());
    }

    // ========================================================================
    // BridgeTaskHandle Tests
    // ========================================================================

    #[test]
    fn test_bridge_create() {
        let bridge = PlumtreeBridge::create(1u64, PlumtreeConfig::default(), NoopDelegate);
        assert!(!bridge.is_shutdown());
        assert_eq!(*bridge.local_id(), 1u64);
    }

    #[tokio::test]
    async fn test_bridge_start_and_stop() {
        let router = BridgeRouter::<u64>::new();
        let bridge = PlumtreeBridge::create(1u64, PlumtreeConfig::default(), NoopDelegate);

        // Start the bridge
        let handle = bridge.start(&router).await;

        // Verify router registered the node
        assert!(router.is_registered(&1u64).await);
        assert_eq!(router.node_count().await, 1);

        // Tasks should not be finished yet
        assert!(!handle.is_finished());

        // Stop the tasks
        handle.stop();

        // Shutdown the bridge
        bridge.shutdown();
        assert!(bridge.is_shutdown());

        // Unregister from router
        router.unregister(&1u64).await;
        assert!(!router.is_registered(&1u64).await);
    }

    #[tokio::test]
    async fn test_bridge_start_multiple_nodes() {
        let router = BridgeRouter::<u64>::new();
        let bridge1 = PlumtreeBridge::create(1u64, PlumtreeConfig::default(), NoopDelegate);
        let bridge2 = PlumtreeBridge::create(2u64, PlumtreeConfig::default(), NoopDelegate);
        let bridge3 = PlumtreeBridge::create(3u64, PlumtreeConfig::default(), NoopDelegate);

        // Start all bridges
        let handle1 = bridge1.start(&router).await;
        let handle2 = bridge2.start(&router).await;
        let handle3 = bridge3.start(&router).await;

        // Verify all nodes registered
        assert_eq!(router.node_count().await, 3);
        assert!(router.is_registered(&1u64).await);
        assert!(router.is_registered(&2u64).await);
        assert!(router.is_registered(&3u64).await);

        // Stop all tasks
        handle1.stop();
        handle2.stop();
        handle3.stop();

        // Shutdown all bridges
        bridge1.shutdown();
        bridge2.shutdown();
        bridge3.shutdown();
    }
}
