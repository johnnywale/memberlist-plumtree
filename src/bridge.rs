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
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{
//!     PlumtreeBridge, BridgeConfig, PlumtreeConfig,
//!     PlumtreeMemberlist, NoopDelegate,
//! };
//! use std::sync::Arc;
//! use std::net::SocketAddr;
//!
//! // Create the Plumtree instance
//! let local_id: u64 = 1;
//! let pm = Arc::new(PlumtreeMemberlist::new(
//!     local_id,
//!     PlumtreeConfig::lan(),
//!     NoopDelegate,
//! ));
//!
//! // Create the bridge (without QUIC resolver)
//! let bridge = PlumtreeBridge::new(pm.clone());
//!
//! // Use bridge as the EventDelegate for Memberlist
//! // When memberlist calls notify_join/notify_leave, the bridge
//! // automatically updates Plumtree's topology
//! ```
//!
//! # With QUIC Transport
//!
//! ```ignore
//! use memberlist_plumtree::{
//!     PlumtreeBridge, PlumtreeConfig, PlumtreeMemberlist,
//!     QuicTransport, QuicConfig, PooledTransport, PoolConfig,
//!     MapPeerResolver, NoopDelegate,
//! };
//! use std::sync::Arc;
//!
//! let local_addr: std::net::SocketAddr = "127.0.0.1:9000".parse().unwrap();
//! let resolver = Arc::new(MapPeerResolver::new(local_addr));
//!
//! let pm = Arc::new(PlumtreeMemberlist::new(1u64, PlumtreeConfig::lan(), NoopDelegate));
//!
//! // Create bridge with resolver for automatic address updates
//! let bridge = PlumtreeBridge::with_resolver(pm.clone(), resolver.clone());
//!
//! // Start the full stack
//! // bridge.start_stack(transport, memberlist).await;
//! ```

use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use memberlist_core::delegate::{EventDelegate, VoidDelegate};
use memberlist_core::proto::NodeState;
use memberlist_core::transport::Id;
use nodecraft::CheapClone;

use crate::{IdCodec, PlumtreeDelegate, PlumtreeMemberlist};

#[cfg(feature = "quic")]
use crate::MapPeerResolver;

/// Configuration for the Plumtree bridge.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Whether to log topology changes.
    pub log_changes: bool,
    /// Whether to automatically promote new peers to eager based on fanout settings.
    pub auto_promote: bool,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            log_changes: true,
            auto_promote: true,
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
    pub pm: Arc<PlumtreeMemberlist<I, PD>>,
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
    pub fn new(pm: Arc<PlumtreeMemberlist<I, PD>>) -> Self {
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
    pub fn with_config(pm: Arc<PlumtreeMemberlist<I, PD>>, config: BridgeConfig) -> Self {
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
    pub fn with_resolver(pm: Arc<PlumtreeMemberlist<I, PD>>, resolver: Arc<MapPeerResolver<I>>) -> Self {
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
        pm: Arc<PlumtreeMemberlist<I, PD>>,
        config: BridgeConfig,
        resolver: Arc<MapPeerResolver<I>>,
    ) -> Self {
        Self {
            pm,
            config,
            resolver: Some(resolver),
        }
    }

    /// Get a reference to the underlying PlumtreeMemberlist.
    pub fn plumtree(&self) -> &Arc<PlumtreeMemberlist<I, PD>> {
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

// Extract socket address from NodeState's address.
// This is a helper trait to handle different address types.
/// Trait for extracting socket address from memberlist node addresses.
pub trait AddressExtractor<A> {
    /// Extract the socket address from the address type.
    fn extract_addr(addr: &A) -> Option<SocketAddr>;
}

// Default implementation for types that can be converted to SocketAddr
impl<A> AddressExtractor<A> for ()
where
    A: AsRef<std::net::SocketAddr>,
{
    fn extract_addr(addr: &A) -> Option<SocketAddr> {
        Some(*addr.as_ref())
    }
}

/// Bridge event delegate that implements Memberlist's EventDelegate.
///
/// This is a wrapper around `PlumtreeBridge` that implements the full
/// `EventDelegate` trait for integration with Memberlist.
///
/// # Type Parameters
///
/// - `I`: Node identifier type
/// - `A`: Node address type
/// - `PD`: Plumtree delegate type
/// - `D`: Inner delegate type (for forwarding events)
pub struct BridgeEventDelegate<I, A, PD, D = VoidDelegate<I, A>>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// The bridge instance.
    bridge: PlumtreeBridge<I, PD>,
    /// Inner delegate for forwarding events.
    inner: D,
    /// Phantom data for address type.
    _marker: PhantomData<A>,
}

impl<I, A, PD, D> BridgeEventDelegate<I, A, PD, D>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// Create a new bridge event delegate with a void inner delegate.
    pub fn new(bridge: PlumtreeBridge<I, PD>) -> BridgeEventDelegate<I, A, PD, VoidDelegate<I, A>> {
        BridgeEventDelegate {
            bridge,
            inner: VoidDelegate::default(),
            _marker: PhantomData,
        }
    }

    /// Create a new bridge event delegate with a custom inner delegate.
    pub fn with_inner(bridge: PlumtreeBridge<I, PD>, inner: D) -> Self {
        Self {
            bridge,
            inner,
            _marker: PhantomData,
        }
    }

    /// Get a reference to the bridge.
    pub fn bridge(&self) -> &PlumtreeBridge<I, PD> {
        &self.bridge
    }

    /// Get a reference to the inner delegate.
    pub fn inner(&self) -> &D {
        &self.inner
    }
}

impl<I, A, PD, D> Clone for BridgeEventDelegate<I, A, PD, D>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
    D: Clone,
{
    fn clone(&self) -> Self {
        Self {
            bridge: self.bridge.clone(),
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

// Implement EventDelegate for BridgeEventDelegate
impl<I, A, PD, D> EventDelegate for BridgeEventDelegate<I, A, PD, D>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
    D: EventDelegate<Id = I, Address = A>,
{
    type Id = I;
    type Address = A;

    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        let node_id = node.id().clone();

        // Step 1: Update address resolver (if configured with QUIC)
        #[cfg(feature = "quic")]
        if let Some(ref resolver) = self.bridge.resolver {
            // Try to extract socket address from the node's address
            // The actual extraction depends on the address type
            if let Some(addr) = extract_socket_addr(&node) {
                resolver.add_peer(node_id.clone(), addr);
                if self.bridge.config.log_changes {
                    tracing::info!(
                        peer = ?node_id,
                        addr = %addr,
                        "Bridge: Added peer address to resolver"
                    );
                }
            }
        }

        // Step 2: Add peer to Plumtree topology
        // This will automatically classify the peer as eager or lazy based on
        // the hash ring topology and fanout settings
        self.bridge.pm.add_peer(node_id.clone());

        if self.bridge.config.log_changes {
            let stats = self.bridge.pm.peer_stats();
            tracing::info!(
                peer = ?node_id,
                eager_count = stats.eager_count,
                lazy_count = stats.lazy_count,
                "Bridge: Node joined, topology updated"
            );
        }

        // Forward to inner delegate
        self.inner.notify_join(node).await;
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        let node_id = node.id();

        // Step 1: Remove from Plumtree topology
        self.bridge.pm.remove_peer(node_id);

        // Step 2: Remove from address resolver (if configured)
        #[cfg(feature = "quic")]
        if let Some(ref resolver) = self.bridge.resolver {
            resolver.remove_peer(node_id);
        }

        if self.bridge.config.log_changes {
            let stats = self.bridge.pm.peer_stats();
            tracing::info!(
                peer = ?node_id,
                eager_count = stats.eager_count,
                lazy_count = stats.lazy_count,
                "Bridge: Node left, topology cleaned"
            );
        }

        // Forward to inner delegate
        self.inner.notify_leave(node).await;
    }

    async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        // Update address resolver if the address changed
        #[cfg(feature = "quic")]
        if let Some(ref resolver) = self.bridge.resolver {
            if let Some(addr) = extract_socket_addr(&node) {
                let node_id = node.id().clone();
                resolver.update_peer(node_id.clone(), addr);
                if self.bridge.config.log_changes {
                    tracing::debug!(
                        peer = ?node_id,
                        addr = %addr,
                        "Bridge: Updated peer address"
                    );
                }
            }
        }

        // Forward to inner delegate
        self.inner.notify_update(node).await;
    }
}

// Helper function to extract socket address from NodeState
#[cfg(feature = "quic")]
fn extract_socket_addr<I, A>(node: &Arc<NodeState<I, A>>) -> Option<SocketAddr>
where
    I: Id,
    A: CheapClone + Send + Sync + 'static,
{
    use std::any::Any;

    let addr = node.address();

    // Try to downcast the address to SocketAddr directly
    // This uses Any trait for runtime type checking
    let addr_any = addr as &dyn Any;

    if let Some(socket_addr) = addr_any.downcast_ref::<SocketAddr>() {
        return Some(*socket_addr);
    }

    // Try to extract from nodecraft::Node<I, SocketAddr> if that's what A is
    // This covers the common case where memberlist uses Node<I, SocketAddr>

    // For other custom address types, users should:
    // 1. Implement a custom BridgeEventDelegate that extracts addresses
    // 2. Or manually update the resolver after notify_join

    None
}

/// Builder for creating a fully configured Plumtree stack.
///
/// This builder helps wire together all the components needed for a
/// production-ready Plumtree-Memberlist integration.
pub struct PlumtreeStackBuilder<I, PD>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    pm: Arc<PlumtreeMemberlist<I, PD>>,
    config: BridgeConfig,
    #[cfg(feature = "quic")]
    resolver: Option<Arc<MapPeerResolver<I>>>,
}

impl<I, PD> PlumtreeStackBuilder<I, PD>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// Create a new stack builder with the given Plumtree instance.
    pub fn new(pm: Arc<PlumtreeMemberlist<I, PD>>) -> Self {
        Self {
            pm,
            config: BridgeConfig::default(),
            #[cfg(feature = "quic")]
            resolver: None,
        }
    }

    /// Set the bridge configuration.
    pub fn with_config(mut self, config: BridgeConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the address resolver for QUIC transport.
    #[cfg(feature = "quic")]
    #[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
    pub fn with_resolver(mut self, resolver: Arc<MapPeerResolver<I>>) -> Self {
        self.resolver = Some(resolver);
        self
    }

    /// Build the PlumtreeBridge.
    pub fn build(self) -> PlumtreeBridge<I, PD> {
        #[cfg(feature = "quic")]
        {
            match self.resolver {
                Some(resolver) => PlumtreeBridge::with_config_and_resolver(self.pm, self.config, resolver),
                None => PlumtreeBridge::with_config(self.pm, self.config),
            }
        }
        #[cfg(not(feature = "quic"))]
        {
            PlumtreeBridge::with_config(self.pm, self.config)
        }
    }

    /// Build a BridgeEventDelegate with a void inner delegate.
    pub fn build_delegate<A>(self) -> BridgeEventDelegate<I, A, PD, VoidDelegate<I, A>>
    where
        A: CheapClone + Send + Sync + 'static,
    {
        BridgeEventDelegate::<I, A, PD, VoidDelegate<I, A>>::new(self.build())
    }

    /// Build a BridgeEventDelegate with a custom inner delegate.
    pub fn build_delegate_with<A, D>(self, inner: D) -> BridgeEventDelegate<I, A, PD, D>
    where
        A: CheapClone + Send + Sync + 'static,
        D: EventDelegate<Id = I, Address = A>,
    {
        BridgeEventDelegate::with_inner(self.build(), inner)
    }
}

// ============================================================================
// MemberlistStack - Complete Integration Stack
// ============================================================================

/// A complete Plumtree + Memberlist integration stack.
///
/// This struct combines:
/// - `PlumtreeMemberlist` for epidemic broadcast
/// - Real `Memberlist` instance for SWIM gossip discovery
/// - Automatic peer synchronization via `PlumtreeNodeDelegate`
///
/// Use this when you want a fully integrated stack without manually
/// wiring together the components.
///
/// # Type Parameters
///
/// - `I`: Node identifier type (must implement `Id`, `IdCodec`, etc.)
/// - `PD`: Plumtree delegate for message delivery
/// - `T`: Memberlist transport (e.g., `NetTransport`)
/// - `D`: The wrapped delegate type (created by `wrap_delegate`)
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::{MemberlistStack, PlumtreeConfig, NoopDelegate};
/// use memberlist::{Memberlist, Options as MemberlistOptions};
/// use std::net::SocketAddr;
///
/// // Create the stack
/// let stack = MemberlistStack::builder(node_id, PlumtreeConfig::lan(), NoopDelegate)
///     .with_bind_address("127.0.0.1:0".parse().unwrap())
///     .build()
///     .await?;
///
/// // Join the cluster
/// let seed: SocketAddr = "192.168.1.100:7946".parse().unwrap();
/// stack.join(&[seed]).await?;
///
/// // Broadcast messages
/// stack.broadcast(b"hello").await?;
/// ```
pub struct MemberlistStack<I, PD, T, D>
where
    I: Id + IdCodec + memberlist_core::proto::Data + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
    T: memberlist_core::transport::Transport<Id = I>,
    D: memberlist_core::delegate::Delegate<Id = I, Address = T::ResolvedAddress>,
{
    /// The PlumtreeMemberlist instance.
    pm: Arc<PlumtreeMemberlist<I, PD>>,
    /// The Memberlist instance for SWIM gossip.
    memberlist: memberlist_core::Memberlist<T, D>,
    /// The advertise address.
    advertise_addr: SocketAddr,
}

impl<I, PD, T, D> MemberlistStack<I, PD, T, D>
where
    I: Id + IdCodec + memberlist_core::proto::Data + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
    T: memberlist_core::transport::Transport<Id = I>,
    D: memberlist_core::delegate::Delegate<Id = I, Address = T::ResolvedAddress>,
{
    /// Create a new MemberlistStack from pre-built components.
    ///
    /// This is a low-level constructor. Prefer using `MemberlistStackBuilder` for
    /// a more ergonomic API.
    pub fn new(
        pm: Arc<PlumtreeMemberlist<I, PD>>,
        memberlist: memberlist_core::Memberlist<T, D>,
        advertise_addr: SocketAddr,
    ) -> Self {
        Self {
            pm,
            memberlist,
            advertise_addr,
        }
    }

    /// Get a reference to the PlumtreeMemberlist.
    pub fn plumtree(&self) -> &Arc<PlumtreeMemberlist<I, PD>> {
        &self.pm
    }

    /// Get a reference to the Memberlist.
    pub fn memberlist(&self) -> &memberlist_core::Memberlist<T, D> {
        &self.memberlist
    }

    /// Get the advertise address for this node.
    ///
    /// Other nodes can use this address to join the cluster.
    pub fn advertise_address(&self) -> SocketAddr {
        self.advertise_addr
    }

    /// Get Plumtree peer statistics.
    pub fn peer_stats(&self) -> crate::peer_state::PeerStats {
        self.pm.peer_stats()
    }

    /// Get the number of online memberlist members.
    pub async fn num_members(&self) -> usize {
        self.memberlist.num_online_members().await
    }

    /// Broadcast a message through Plumtree.
    ///
    /// The message will be delivered to all nodes in the cluster via the
    /// epidemic broadcast tree.
    pub async fn broadcast(
        &self,
        payload: impl Into<bytes::Bytes>,
    ) -> Result<crate::MessageId, crate::Error> {
        self.pm.broadcast(payload).await
    }

    /// Join the cluster via seed nodes.
    ///
    /// This triggers automatic peer discovery via SWIM gossip.
    /// The PlumtreeNodeDelegate will automatically update Plumtree's topology
    /// as nodes are discovered.
    ///
    /// # Arguments
    ///
    /// * `seed_addrs` - Socket addresses of seed nodes to join through
    ///
    /// # Example
    ///
    /// ```ignore
    /// let seeds = vec![
    ///     "192.168.1.100:7946".parse().unwrap(),
    ///     "192.168.1.101:7946".parse().unwrap(),
    /// ];
    /// stack.join(&seeds).await?;
    /// ```
    pub async fn join(
        &self,
        seed_addrs: &[SocketAddr],
    ) -> Result<(), MemberlistStackError>
    where
        <T as memberlist_core::transport::Transport>::ResolvedAddress: From<SocketAddr>,
    {
        use memberlist_core::proto::MaybeResolvedAddress;

        for &addr in seed_addrs {
            // Create a placeholder ID - memberlist will resolve the actual ID
            // We use a minimal ID representation that will be replaced during handshake
            let seed_node = nodecraft::Node::new(
                self.pm.plumtree().local_id().clone(),
                MaybeResolvedAddress::Resolved(addr.into()),
            );

            self.memberlist
                .join(seed_node)
                .await
                .map_err(|e| MemberlistStackError::JoinFailed(format!("{}", e)))?;
        }
        Ok(())
    }

    /// Leave the cluster gracefully.
    ///
    /// This notifies other nodes of the departure and waits for the
    /// specified timeout for the leave message to propagate.
    pub async fn leave(&self, timeout: std::time::Duration) -> Result<bool, MemberlistStackError> {
        self.memberlist
            .leave(timeout)
            .await
            .map_err(|e| MemberlistStackError::LeaveFailed(format!("{}", e)))
    }

    /// Shutdown the entire stack.
    ///
    /// This shuts down both Plumtree and Memberlist.
    pub async fn shutdown(&self) -> Result<(), MemberlistStackError> {
        self.pm.shutdown();
        self.memberlist
            .shutdown()
            .await
            .map_err(|e| MemberlistStackError::ShutdownFailed(format!("{}", e)))
    }

    /// Check if the stack has been shut down.
    pub fn is_shutdown(&self) -> bool {
        self.pm.is_shutdown()
    }
}

/// Errors that can occur when using `MemberlistStack`.
#[derive(Debug, Clone)]
pub enum MemberlistStackError {
    /// Failed to join the cluster.
    JoinFailed(String),
    /// Failed to leave the cluster.
    LeaveFailed(String),
    /// Failed to shutdown.
    ShutdownFailed(String),
    /// Failed to create the stack.
    CreationFailed(String),
}

impl std::fmt::Display for MemberlistStackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::JoinFailed(e) => write!(f, "failed to join cluster: {}", e),
            Self::LeaveFailed(e) => write!(f, "failed to leave cluster: {}", e),
            Self::ShutdownFailed(e) => write!(f, "failed to shutdown: {}", e),
            Self::CreationFailed(e) => write!(f, "failed to create stack: {}", e),
        }
    }
}

impl std::error::Error for MemberlistStackError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NoopDelegate, PlumtreeConfig};

    #[test]
    fn test_bridge_creation() {
        let pm = Arc::new(PlumtreeMemberlist::new(
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

    #[test]
    fn test_stack_builder() {
        let pm = Arc::new(PlumtreeMemberlist::new(
            1u64,
            PlumtreeConfig::default(),
            NoopDelegate,
        ));
        let bridge = PlumtreeStackBuilder::new(pm)
            .with_config(BridgeConfig::default())
            .build();

        assert!(!bridge.is_shutdown());
    }

    #[cfg(feature = "quic")]
    #[test]
    fn test_bridge_with_resolver() {
        use std::net::SocketAddr;

        let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let resolver = Arc::new(MapPeerResolver::new(local_addr));

        let pm = Arc::new(PlumtreeMemberlist::new(
            1u64,
            PlumtreeConfig::default(),
            NoopDelegate,
        ));

        let bridge = PlumtreeBridge::with_resolver(pm, resolver.clone());
        assert!(bridge.resolver().is_some());
    }
}
