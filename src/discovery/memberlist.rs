//! Memberlist-based peer discovery using SWIM protocol.
//!
//! This discovery mechanism wraps memberlist to provide automatic peer
//! discovery using the SWIM (Scalable Weakly-consistent Infection-style
//! Process Group Membership) protocol.
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::discovery::{MemberlistDiscovery, MemberlistDiscoveryConfig};
//!
//! // Create memberlist discovery
//! let config = MemberlistDiscoveryConfig::new()
//!     .with_bind_addr("0.0.0.0:7946".parse().unwrap())
//!     .with_seeds(vec!["192.168.1.10:7946".parse().unwrap()]);
//!
//! let discovery = MemberlistDiscovery::new(config);
//! ```

use super::traits::{ClusterDiscovery, DiscoveryEvent, DiscoveryHandle};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

// Re-export memberlist-core types for convenience
pub use memberlist_core::{
    delegate::{
        AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, NodeDelegate,
        PingDelegate, VoidDelegate,
    },
    proto::{Meta, NodeState},
    Memberlist,
};
pub use nodecraft::{CheapClone, Id};

use bytes::Bytes;

use crate::bridge::{BridgeConfig, PlumtreeBridge};
use crate::integration::{decode_plumtree_envelope, BroadcastEnvelope, IdCodec};
use crate::message::PlumtreeMessage;
use crate::peer_state::PeerState;
use crate::{PlumtreeDelegate, PlumtreeDiscovery};

#[cfg(feature = "quic")]
use crate::MapPeerResolver;

/// Magic byte prefix for Plumtree messages.
const PLUMTREE_MAGIC: u8 = 0x50;

/// Configuration for memberlist-based discovery.
#[derive(Debug, Clone)]
pub struct MemberlistDiscoveryConfig {
    /// Address to bind for memberlist (SWIM protocol).
    ///
    /// Default: 0.0.0.0:7946
    pub bind_addr: SocketAddr,

    /// Seed addresses for cluster bootstrap.
    pub seeds: Vec<SocketAddr>,

    /// Advertise address (external address for other nodes to connect).
    ///
    /// Default: Same as bind_addr
    pub advertise_addr: Option<SocketAddr>,
}

impl Default for MemberlistDiscoveryConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:7946".parse().unwrap(),
            seeds: Vec::new(),
            advertise_addr: None,
        }
    }
}

impl MemberlistDiscoveryConfig {
    /// Create a new memberlist discovery configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the bind address (builder pattern).
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    /// Set seed addresses for cluster bootstrap (builder pattern).
    pub fn with_seeds(mut self, seeds: Vec<SocketAddr>) -> Self {
        self.seeds = seeds;
        self
    }

    /// Add a seed address (builder pattern).
    pub fn with_seed(mut self, addr: SocketAddr) -> Self {
        self.seeds.push(addr);
        self
    }

    /// Set the advertise address (builder pattern).
    pub fn with_advertise_addr(mut self, addr: SocketAddr) -> Self {
        self.advertise_addr = Some(addr);
        self
    }
}

/// Handle for memberlist discovery.
#[derive(Debug)]
pub struct MemberlistDiscoveryHandle {
    running: Arc<AtomicBool>,
}

impl MemberlistDiscoveryHandle {
    fn new(running: Arc<AtomicBool>) -> Self {
        Self { running }
    }
}

impl DiscoveryHandle for MemberlistDiscoveryHandle {
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }
}

/// Memberlist-based peer discovery.
///
/// Uses memberlist's SWIM protocol for automatic peer discovery and
/// failure detection. This discovery mechanism provides automatic cluster
/// membership through the SWIM gossip protocol.
///
/// # Usage with PlumtreeStackConfig
///
/// ```ignore
/// use memberlist_plumtree::{
///     PlumtreeStackConfig, PlumtreeConfig, MemberlistStack,
///     discovery::{MemberlistDiscovery, MemberlistDiscoveryConfig},
/// };
///
/// // Create stack config with memberlist discovery
/// let discovery = MemberlistDiscovery::new(
///     MemberlistDiscoveryConfig::new()
///         .with_bind_addr("0.0.0.0:7946".parse().unwrap())
///         .with_seed("192.168.1.10:7946".parse().unwrap())
/// );
///
/// let stack_config = PlumtreeStackConfig::new(1u64, "0.0.0.0:9000".parse().unwrap())
///     .with_discovery(discovery)
///     .with_plumtree(PlumtreeConfig::default());
///
/// // Build the memberlist stack from config
/// let stack = MemberlistStack::from_config(stack_config, transport, delegate).await?;
/// ```
///
/// # Example
///
/// ```
/// use memberlist_plumtree::discovery::{MemberlistDiscovery, MemberlistDiscoveryConfig};
///
/// let config = MemberlistDiscoveryConfig::new()
///     .with_bind_addr("0.0.0.0:7946".parse().unwrap())
///     .with_seed("192.168.1.10:7946".parse().unwrap());
///
/// let discovery = MemberlistDiscovery::<u64>::new(config);
///
/// // Access configuration
/// assert_eq!(discovery.bind_addr(), "0.0.0.0:7946".parse().unwrap());
/// assert_eq!(discovery.seeds().len(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct MemberlistDiscovery<I> {
    config: MemberlistDiscoveryConfig,
    _marker: std::marker::PhantomData<I>,
}

impl<I> MemberlistDiscovery<I> {
    /// Create a new memberlist discovery.
    pub fn new(config: MemberlistDiscoveryConfig) -> Self {
        Self {
            config,
            _marker: std::marker::PhantomData,
        }
    }

    /// Create memberlist discovery from seed addresses.
    ///
    /// This is a convenience constructor for simple setups.
    ///
    /// # Example
    ///
    /// ```
    /// use memberlist_plumtree::discovery::MemberlistDiscovery;
    ///
    /// let discovery = MemberlistDiscovery::<u64>::from_seeds(
    ///     "0.0.0.0:7946".parse().unwrap(),
    ///     vec!["192.168.1.10:7946".parse().unwrap()],
    /// );
    /// ```
    pub fn from_seeds(bind_addr: SocketAddr, seeds: Vec<SocketAddr>) -> Self {
        Self::new(
            MemberlistDiscoveryConfig::new()
                .with_bind_addr(bind_addr)
                .with_seeds(seeds),
        )
    }

    /// Get the underlying configuration.
    pub fn config(&self) -> &MemberlistDiscoveryConfig {
        &self.config
    }

    /// Get the bind address.
    pub fn bind_addr(&self) -> SocketAddr {
        self.config.bind_addr
    }

    /// Get the seed addresses.
    pub fn seeds(&self) -> &[SocketAddr] {
        &self.config.seeds
    }

    /// Get the advertise address.
    pub fn advertise_addr(&self) -> SocketAddr {
        self.config.advertise_addr.unwrap_or(self.config.bind_addr)
    }

    /// Convert to owned configuration.
    pub fn into_config(self) -> MemberlistDiscoveryConfig {
        self.config
    }
}

impl<I> ClusterDiscovery<I> for MemberlistDiscovery<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Handle = MemberlistDiscoveryHandle;

    async fn start(&self) -> (async_channel::Receiver<DiscoveryEvent<I>>, Self::Handle) {
        // This is a "shell" discovery that provides the config.
        // The actual peer discovery happens through PlumtreeNodeDelegate
        // when integrated with memberlist via MemberlistStack.
        //
        // The channel is created but events come from PlumtreeNodeDelegate
        // callbacks (on_join, on_leave) which are wired up separately.
        let (tx, rx) = async_channel::bounded(64);
        let running = Arc::new(AtomicBool::new(true));

        // No background task needed - events come from memberlist callbacks
        drop(tx);

        (rx, MemberlistDiscoveryHandle::new(running))
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        Some(self.advertise_addr())
    }
}

/// Callback for handling peer promotion events during rebalancing.
pub type PromotionCallback<I> = Arc<dyn Fn(&I) + Send + Sync>;

/// Callback for handling peer demotion events during rebalancing.
pub type DemotionCallback<I> = Arc<dyn Fn(&I) + Send + Sync>;

/// Delegate that intercepts messages for Plumtree protocol.
///
/// Wraps a user delegate and handles Plumtree message routing.
/// Use this when creating a memberlist to integrate Plumtree.
pub struct PlumtreeNodeDelegate<I, A, D> {
    /// Inner user delegate.
    inner: D,
    /// Channel to send received Plumtree messages to PlumtreeDiscovery.
    plumtree_tx: async_channel::Sender<(I, PlumtreeMessage)>,
    /// Outgoing Plumtree messages to broadcast via memberlist (unencooded).
    /// Serialization is deferred until messages are sent over the network.
    outgoing_rx: async_channel::Receiver<BroadcastEnvelope<I>>,
    /// Shared peer state for synchronizing membership changes.
    /// When memberlist detects a node join/leave, this is updated to keep
    /// Plumtree's peer state in sync.
    peers: Arc<PeerState<I>>,
    /// Target number of eager peers for auto-classification.
    eager_fanout: usize,
    /// Maximum total peers allowed (None = unlimited).
    max_peers: Option<usize>,
    /// Optional callback for peer promotion events during rebalancing.
    on_promotion: Option<PromotionCallback<I>>,
    /// Optional callback for peer demotion events during rebalancing.
    on_demotion: Option<DemotionCallback<I>>,
    /// Marker.
    _marker: PhantomData<A>,
}

impl<I, A, D> PlumtreeNodeDelegate<I, A, D> {
    /// Create a new Plumtree node delegate.
    ///
    /// This wraps a user's memberlist delegate to automatically synchronize
    /// peer state when nodes join or leave the cluster.
    ///
    /// # Arguments
    ///
    /// * `inner` - The user's delegate implementation
    /// * `plumtree_tx` - Sender for forwarding received Plumtree messages
    /// * `outgoing_rx` - Receiver for outgoing Plumtree messages to broadcast
    /// * `peers` - Shared peer state for synchronizing membership changes
    /// * `eager_fanout` - Target number of eager peers
    /// * `max_peers` - Maximum total peers allowed (None = unlimited)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Create PlumtreeDiscovery first
    /// let pm = Arc::new(PlumtreeDiscovery::new(node_id, config, delegate));
    ///
    /// // Create PlumtreeNodeDelegate directly
    /// let wrapped_delegate = PlumtreeNodeDelegate::new(
    ///     your_memberlist_delegate,
    ///     pm.incoming_sender(),
    ///     pm.outgoing_receiver(),
    ///     pm.peers().clone(),
    ///     pm.config().eager_fanout,
    ///     pm.config().max_peers,
    /// );
    ///
    /// // Use wrapped_delegate when creating memberlist
    /// let memberlist = Memberlist::new(wrapped_delegate, memberlist_config).await?;
    /// ```
    pub fn new(
        inner: D,
        plumtree_tx: async_channel::Sender<(I, PlumtreeMessage)>,
        outgoing_rx: async_channel::Receiver<BroadcastEnvelope<I>>,
        peers: Arc<PeerState<I>>,
        eager_fanout: usize,
        max_peers: Option<usize>,
    ) -> Self {
        Self {
            inner,
            plumtree_tx,
            outgoing_rx,
            peers,
            eager_fanout,
            max_peers,
            on_promotion: None,
            on_demotion: None,
            _marker: PhantomData,
        }
    }

    /// Set the callback for peer promotion events during rebalancing.
    ///
    /// This callback is invoked when a lazy peer is promoted to eager
    /// during rebalancing (e.g., when an eager peer leaves the cluster).
    pub fn with_promotion_callback(mut self, callback: PromotionCallback<I>) -> Self {
        self.on_promotion = Some(callback);
        self
    }

    /// Set the callback for peer demotion events during rebalancing.
    ///
    /// This callback is invoked when an eager peer is demoted to lazy
    /// during rebalancing (e.g., when too many eager peers exist).
    pub fn with_demotion_callback(mut self, callback: DemotionCallback<I>) -> Self {
        self.on_demotion = Some(callback);
        self
    }

    /// Get a reference to the inner delegate.
    pub fn inner(&self) -> &D {
        &self.inner
    }

    /// Get a clone of the plumtree message sender.
    ///
    /// This can be used to forward Plumtree messages from external sources.
    pub fn plumtree_sender(&self) -> async_channel::Sender<(I, PlumtreeMessage)> {
        self.plumtree_tx.clone()
    }

    /// Get a clone of the outgoing envelope receiver.
    ///
    /// This can be used to receive outgoing Plumtree messages from external sources.
    /// Messages are unencooded - call `.encode()` on the envelope to get bytes.
    #[allow(dead_code)]
    pub(crate) fn outgoing_receiver(&self) -> async_channel::Receiver<BroadcastEnvelope<I>> {
        self.outgoing_rx.clone()
    }
}

impl<I, A, D> NodeDelegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: NodeDelegate,
{
    async fn node_meta(&self, limit: usize) -> Meta {
        self.inner.node_meta(limit).await
    }

    async fn notify_message(&self, msg: std::borrow::Cow<'_, [u8]>) {
        // Check if this is a Plumtree message
        if msg.len() > 1 && msg[0] == PLUMTREE_MAGIC {
            // Decode Plumtree envelope (sender_id + message)
            if let Some((sender, plumtree_msg)) = decode_plumtree_envelope::<I>(&msg) {
                tracing::trace!(
                    "received plumtree {:?} from {:?}",
                    plumtree_msg.tag(),
                    sender
                );
                // Forward to Plumtree for proper handling with sender identity
                if self.plumtree_tx.send((sender, plumtree_msg)).await.is_err() {
                    tracing::warn!("plumtree channel closed, cannot forward message");
                }
            } else {
                tracing::warn!("failed to decode plumtree envelope");
            }
        } else {
            // Forward non-Plumtree messages to inner delegate
            self.inner.notify_message(msg).await;
        }
    }

    async fn broadcast_messages<F>(
        &self,
        limit: usize,
        encoded_len: F,
    ) -> impl Iterator<Item = Bytes> + Send
    where
        F: Fn(Bytes) -> (usize, Bytes) + Send + Sync + 'static,
    {
        // Use proportional limiting to prevent Plumtree from starving user messages.
        // Reserve at least 30% of bandwidth for user messages to ensure they always
        // have a chance to be sent, even under heavy Plumtree control traffic.
        const PLUMTREE_MAX_RATIO: usize = 70;
        let plumtree_limit = (limit * PLUMTREE_MAX_RATIO) / 100;
        let user_reserved = limit - plumtree_limit;

        // Collect Plumtree outgoing messages up to proportional limit
        // Serialization is deferred to here - encode just before network transmission
        let mut plumtree_msgs = Vec::new();
        let mut plumtree_used = 0;

        while let Ok(envelope) = self.outgoing_rx.try_recv() {
            // Encode at the last moment before network transmission
            let encoded = envelope.encode();
            let (len, msg) = encoded_len(encoded);
            if plumtree_used + len <= plumtree_limit {
                plumtree_used += len;
                plumtree_msgs.push(msg);
            } else {
                // Plumtree hit its proportional limit, stop collecting
                break;
            }
        }

        // User gets reserved space plus any unused Plumtree quota
        let user_limit = user_reserved + (plumtree_limit - plumtree_used);
        let user_msgs = self.inner.broadcast_messages(user_limit, encoded_len).await;

        // Combine both iterators
        plumtree_msgs.into_iter().chain(user_msgs)
    }

    async fn local_state(&self, join: bool) -> Bytes {
        self.inner.local_state(join).await
    }

    async fn merge_remote_state(&self, buf: &[u8], join: bool) {
        self.inner.merge_remote_state(buf, join).await
    }
}

impl<I, A, D> PingDelegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: PingDelegate<Id = I, Address = A>,
{
    type Id = I;
    type Address = A;

    async fn ack_payload(&self) -> Bytes {
        self.inner.ack_payload().await
    }

    async fn notify_ping_complete(
        &self,
        node: Arc<NodeState<Self::Id, Self::Address>>,
        rtt: std::time::Duration,
        payload: Bytes,
    ) {
        self.inner.notify_ping_complete(node, rtt, payload).await
    }

    fn disable_reliable_pings(&self, target: &Self::Id) -> bool {
        self.inner.disable_reliable_pings(target)
    }
}

impl<I, A, D> EventDelegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: EventDelegate<Id = I, Address = A>,
{
    type Id = I;
    type Address = A;

    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        // Sync: Add to Plumtree peers with hash ring-based auto-classification
        // This ensures new peers are placed correctly in the topology
        let result = self
            .peers
            .add_peer_auto(node.id().clone(), self.max_peers, self.eager_fanout);

        // Record metrics for successful peer additions
        #[cfg(feature = "metrics")]
        match result {
            crate::peer_state::AddPeerResult::AddedEager
            | crate::peer_state::AddPeerResult::AddedLazy
            | crate::peer_state::AddPeerResult::AddedAfterEviction => {
                crate::metrics::record_peer_added();
            }
            _ => {}
        }

        self.inner.notify_join(node).await;
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        // Sync: Remove from Plumtree peers immediately to stop sending to dead node
        let result = self.peers.remove_peer(node.id());

        // Record metrics for peer removal
        #[cfg(feature = "metrics")]
        if result.was_removed() {
            crate::metrics::record_peer_removed();
        }

        // If an eager peer was removed, rebalance to promote a lazy peer
        // This maintains tree connectivity when eager peers leave
        if result.was_eager() {
            let rebalance_result = self.peers.rebalance(self.eager_fanout);

            // Trigger callbacks for any peers that were promoted or demoted
            if let Some(ref on_promotion) = self.on_promotion {
                for peer in &rebalance_result.promoted {
                    on_promotion(peer);
                }
            }
            if let Some(ref on_demotion) = self.on_demotion {
                for peer in &rebalance_result.demoted {
                    on_demotion(peer);
                }
            }

            // Record metrics for promotions/demotions from rebalance
            #[cfg(feature = "metrics")]
            {
                for _ in &rebalance_result.promoted {
                    crate::metrics::record_peer_promotion();
                }
                for _ in &rebalance_result.demoted {
                    crate::metrics::record_peer_demotion();
                }
            }
        }

        self.inner.notify_leave(node).await;
    }

    async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        self.inner.notify_update(node).await;
    }
}

impl<I, A, D> ConflictDelegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: ConflictDelegate<Id = I, Address = A>,
{
    type Id = I;
    type Address = A;

    async fn notify_conflict(
        &self,
        existing: Arc<NodeState<Self::Id, Self::Address>>,
        other: Arc<NodeState<Self::Id, Self::Address>>,
    ) {
        self.inner.notify_conflict(existing, other).await;
    }
}

impl<I, A, D> AliveDelegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: AliveDelegate<Id = I, Address = A>,
{
    type Error = D::Error;
    type Id = I;
    type Address = A;

    async fn notify_alive(
        &self,
        peer: Arc<NodeState<Self::Id, Self::Address>>,
    ) -> std::result::Result<(), Self::Error> {
        self.inner.notify_alive(peer).await
    }
}

impl<I, A, D> MergeDelegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: MergeDelegate<Id = I, Address = A>,
{
    type Error = D::Error;
    type Id = I;
    type Address = A;

    async fn notify_merge(
        &self,
        peers: Arc<[NodeState<Self::Id, Self::Address>]>,
    ) -> std::result::Result<(), Self::Error> {
        self.inner.notify_merge(peers).await
    }
}

impl<I, A, D> Delegate for PlumtreeNodeDelegate<I, A, D>
where
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: Delegate<Id = I, Address = A>,
{
    type Id = I;
    type Address = A;
}

// ============================================================================
// BridgeEventDelegate - Memberlist EventDelegate integration
// ============================================================================

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

// ============================================================================
// PlumtreeStackBuilder - Builder for the integration stack
// ============================================================================

/// Builder for creating a fully configured Plumtree stack.
///
/// This builder helps wire together all the components needed for a
/// production-ready Plumtree-Memberlist integration.
pub struct PlumtreeStackBuilder<I, PD>
where
    I: Id + IdCodec + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    pm: Arc<PlumtreeDiscovery<I, PD>>,
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
    pub fn new(pm: Arc<PlumtreeDiscovery<I, PD>>) -> Self {
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
                Some(resolver) => {
                    PlumtreeBridge::with_config_and_resolver(self.pm, self.config, resolver)
                }
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
/// - `PlumtreeDiscovery` for epidemic broadcast
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
/// - `D`: The wrapped delegate type (created via `PlumtreeNodeDelegate::new`)
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
    /// The PlumtreeBridge instance (wraps PlumtreeDiscovery).
    bridge: PlumtreeBridge<I, PD>,
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
    /// This is a low-level constructor that requires manually creating and wiring
    /// all components. For a higher-level API with automatic configuration,
    /// see the `PlumtreeStackConfig` builder pattern.
    ///
    /// # Arguments
    ///
    /// * `bridge` - The PlumtreeBridge instance (wraps PlumtreeDiscovery)
    /// * `memberlist` - The Memberlist instance for SWIM gossip
    /// * `advertise_addr` - The address this node advertises to the cluster
    pub fn new(
        bridge: PlumtreeBridge<I, PD>,
        memberlist: memberlist_core::Memberlist<T, D>,
        advertise_addr: SocketAddr,
    ) -> Self {
        Self {
            bridge,
            memberlist,
            advertise_addr,
        }
    }

    /// Create a MemberlistStack from a PlumtreeStackConfig with MemberlistDiscovery.
    ///
    /// This is the recommended way to create a MemberlistStack when using the
    /// `with_discovery` pattern with `MemberlistDiscovery`.
    ///
    /// # Type Parameters
    ///
    /// - `InnerD`: The inner delegate type that will be wrapped by `PlumtreeNodeDelegate`
    ///
    /// # Arguments
    ///
    /// * `config` - Stack configuration with `MemberlistDiscovery`
    /// * `bridge` - PlumtreeBridge instance (wraps PlumtreeDiscovery)
    /// * `memberlist` - Pre-built Memberlist instance (with wrapped delegate)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{
    ///     PlumtreeStackConfig, PlumtreeConfig, MemberlistStack,
    ///     PlumtreeBridge, PlumtreeDiscovery, NoopDelegate,
    ///     discovery::{MemberlistDiscovery, MemberlistDiscoveryConfig},
    /// };
    ///
    /// // 1. Create stack config with memberlist discovery
    /// let discovery = MemberlistDiscovery::from_seeds(
    ///     "0.0.0.0:7946".parse().unwrap(),
    ///     vec!["192.168.1.10:7946".parse().unwrap()],
    /// );
    /// let stack_config = PlumtreeStackConfig::new(node_id, "0.0.0.0:9000".parse().unwrap())
    ///     .with_discovery(discovery)
    ///     .with_plumtree(PlumtreeConfig::default());
    ///
    /// // 2. Create PlumtreeDiscovery and wrap in PlumtreeBridge
    /// let pm = Arc::new(PlumtreeDiscovery::from_stack_config(&stack_config, NoopDelegate));
    /// let bridge = PlumtreeBridge::new(pm.clone());
    ///
    /// // 3. Create wrapped delegate and memberlist
    /// let wrapped_delegate = PlumtreeNodeDelegate::new(
    ///     your_inner_delegate,
    ///     pm.incoming_sender(),
    ///     pm.outgoing_receiver(),
    ///     pm.peers().clone(),
    ///     pm.config().eager_fanout,
    ///     pm.config().max_peers,
    /// );
    /// let memberlist = Memberlist::new(transport, wrapped_delegate, options).await?;
    ///
    /// // 4. Create the stack
    /// let stack = MemberlistStack::from_stack_config(stack_config, bridge, memberlist);
    /// ```
    pub fn from_stack_config(
        config: crate::PlumtreeStackConfig<I, MemberlistDiscovery<I>>,
        bridge: PlumtreeBridge<I, PD>,
        memberlist: memberlist_core::Memberlist<T, D>,
    ) -> Self
    where
        I: std::hash::Hash,
    {
        let advertise_addr = config.discovery.advertise_addr();
        Self {
            bridge,
            memberlist,
            advertise_addr,
        }
    }

    /// Get a reference to the PlumtreeBridge.
    pub fn bridge(&self) -> &PlumtreeBridge<I, PD> {
        &self.bridge
    }

    /// Get a reference to the underlying PlumtreeDiscovery.
    pub fn plumtree(&self) -> &Arc<PlumtreeDiscovery<I, PD>> {
        self.bridge.plumtree()
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

    /// Start the Plumtree background tasks.
    ///
    /// **IMPORTANT**: This method MUST be called after creating the stack for the
    /// Plumtree protocol to work correctly. Without it:
    /// - IHave messages won't be sent to lazy peers
    /// - Graft retry logic won't work
    /// - Tree self-healing will be broken
    ///
    /// This spawns the following background tasks:
    /// - IHave scheduler (sends announcements to lazy peers)
    /// - Graft timer (handles retry logic for missing messages)
    /// - Seen map cleanup (memory management)
    /// - Outgoing message processor (routes messages)
    /// - Incoming message processor (handles received messages)
    ///
    /// # Arguments
    ///
    /// * `transport` - Transport for unicast message delivery (Graft/Prune messages)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let stack = MemberlistStack::new(pm, memberlist, advertise_addr);
    ///
    /// // REQUIRED: Start background tasks before using the stack
    /// stack.start(transport);
    ///
    /// // Now you can join and broadcast
    /// stack.join(&seeds).await?;
    /// stack.broadcast(b"Hello!").await?;
    /// ```
    #[cfg(feature = "tokio")]
    pub fn start<PT>(&self, transport: PT)
    where
        PT: crate::Transport<I> + Clone + 'static,
        PD: 'static,
    {
        // Spawn the main Plumtree runner (IHave scheduler, Graft timer, etc.)
        let pm_runner = self.bridge.pm.clone();
        tokio::spawn(async move {
            pm_runner.run_with_transport(transport).await;
        });

        // Spawn the incoming message processor
        let pm_incoming = self.bridge.pm.clone();
        tokio::spawn(async move {
            pm_incoming.run_incoming_processor().await;
        });

        tracing::info!("MemberlistStack background tasks started");
    }

    /// Start the Plumtree background tasks without a transport.
    ///
    /// Use this when you handle unicast message delivery separately (e.g., via
    /// memberlist's reliable send). For most use cases, prefer [`start()`](Self::start)
    /// which handles unicast automatically.
    ///
    /// # Warning
    ///
    /// If you use this method, you MUST handle unicast messages manually by:
    /// 1. Consuming messages from `plumtree().unicast_receiver()`
    /// 2. Sending them to the target peer via your transport
    ///
    /// Failure to do so will break Graft/Prune messages and prevent tree self-healing.
    #[cfg(feature = "tokio")]
    pub fn start_without_transport(&self)
    where
        PD: 'static,
    {
        // Spawn the main Plumtree runner without unicast handling
        let pm_runner = self.bridge.pm.clone();
        tokio::spawn(async move {
            pm_runner.run().await;
        });

        // Spawn the incoming message processor
        let pm_incoming = self.bridge.pm.clone();
        tokio::spawn(async move {
            pm_incoming.run_incoming_processor().await;
        });

        tracing::info!("MemberlistStack background tasks started (without unicast transport)");
    }

    /// Get Plumtree peer statistics.
    pub fn peer_stats(&self) -> crate::peer_state::PeerStats {
        self.bridge.peer_stats()
    }

    /// Get the current cache statistics from Plumtree.
    pub fn cache_stats(&self) -> crate::message::CacheStats {
        self.bridge.pm.cache_stats()
    }

    /// Get the current seen map statistics from Plumtree (deduplication map).
    pub fn seen_map_stats(&self) -> Option<crate::plumtree::SeenMapStats> {
        self.bridge.pm.seen_map_stats()
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
        self.bridge.broadcast(payload).await
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
    pub async fn join(&self, seed_addrs: &[SocketAddr]) -> Result<(), MemberlistStackError>
    where
        <T as memberlist_core::transport::Transport>::ResolvedAddress: From<SocketAddr>,
    {
        use memberlist_core::proto::MaybeResolvedAddress;

        for &addr in seed_addrs {
            // Create a placeholder ID - memberlist will resolve the actual ID
            // We use a minimal ID representation that will be replaced during handshake
            let seed_node = nodecraft::Node::new(
                self.bridge.local_id().clone(),
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
        self.bridge.shutdown();
        self.memberlist
            .shutdown()
            .await
            .map_err(|e| MemberlistStackError::ShutdownFailed(format!("{}", e)))
    }

    /// Check if the stack has been shut down.
    pub fn is_shutdown(&self) -> bool {
        self.bridge.is_shutdown()
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

// ============================================================================
// MemberlistStack Lazarus Integration
// ============================================================================

impl<I, PD, T, D> MemberlistStack<I, PD, T, D>
where
    I: Id + IdCodec + memberlist_core::proto::Data + Clone + Ord + Send + Sync + 'static,
    PD: PlumtreeDelegate<I> + 'static,
    T: memberlist_core::transport::Transport<Id = I> + 'static,
    D: memberlist_core::delegate::Delegate<Id = I, Address = T::ResolvedAddress> + 'static,
    T::ResolvedAddress: From<SocketAddr>,
{
    /// Spawn the Lazarus background task for automatic seed recovery.
    ///
    /// This task periodically checks if any configured static seeds are missing
    /// from the cluster and attempts to rejoin them. This handles the "Ghost Seed"
    /// problem where a restarted seed node remains isolated because other nodes
    /// stopped probing it after marking it dead.
    ///
    /// # Arguments
    ///
    /// * `config` - The bridge configuration containing static seeds and interval
    ///
    /// # Returns
    ///
    /// A `LazarusHandle` that can be used to check stats and shutdown the task.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = BridgeConfig::new()
    ///     .with_static_seeds(vec![
    ///         "192.168.1.100:7946".parse().unwrap(),
    ///         "192.168.1.101:7946".parse().unwrap(),
    ///     ])
    ///     .with_lazarus_enabled(true)
    ///     .with_lazarus_interval(Duration::from_secs(30));
    ///
    /// let handle = stack.spawn_lazarus_task(config);
    ///
    /// // Later, check stats
    /// let stats = handle.stats();
    /// println!("Reconnections: {}", stats.reconnections);
    ///
    /// // Shutdown when done
    /// handle.shutdown();
    /// ```
    pub fn spawn_lazarus_task(&self, config: BridgeConfig) -> crate::bridge::LazarusHandle
    where
        I: std::fmt::Debug,
    {
        let handle = crate::bridge::LazarusHandle::default();

        if !config.should_run_lazarus() {
            tracing::debug!("Lazarus task not started: disabled or no seeds configured");
            return handle;
        }

        let handle_clone = handle.clone();
        let memberlist = self.memberlist.clone();
        let local_id = self.bridge.local_id().clone();
        let seeds = config.static_seeds.clone();
        let interval = config.lazarus_interval;

        // Spawn the background task using the runtime
        // Note: This requires a tokio runtime to be available
        #[cfg(feature = "tokio")]
        tokio::spawn(async move {
            tracing::info!(
                seeds = ?seeds,
                interval = ?interval,
                "Lazarus task started: monitoring {} static seeds",
                seeds.len()
            );

            loop {
                // Check for shutdown
                if handle_clone.is_shutdown() {
                    tracing::info!("Lazarus task shutting down");
                    break;
                }

                // Sleep for the configured interval
                futures_timer::Delay::new(interval).await;

                // Check for shutdown again after sleep
                if handle_clone.is_shutdown() {
                    tracing::info!("Lazarus task shutting down");
                    break;
                }

                // Get alive member addresses
                let alive_addrs = Self::get_alive_addresses(&memberlist).await;

                // Find missing seeds
                let missing_seeds: Vec<_> = seeds
                    .iter()
                    .filter(|seed| !alive_addrs.contains(seed))
                    .cloned()
                    .collect();

                handle_clone.set_missing_seeds(missing_seeds.len());

                if missing_seeds.is_empty() {
                    tracing::trace!("Lazarus: all static seeds are alive");
                    continue;
                }

                tracing::info!(
                    missing = ?missing_seeds,
                    "Lazarus: {} static seed(s) not in alive set, attempting rejoin",
                    missing_seeds.len()
                );

                // Attempt to rejoin each missing seed
                for seed_addr in missing_seeds {
                    handle_clone.record_probe();

                    // Create a node for the join attempt
                    // We use the local_id as a placeholder - memberlist will resolve the actual ID
                    use memberlist_core::proto::MaybeResolvedAddress;
                    let seed_node = nodecraft::Node::new(
                        local_id.clone(),
                        MaybeResolvedAddress::Resolved(seed_addr.into()),
                    );

                    match memberlist.join(seed_node).await {
                        Ok(_) => {
                            handle_clone.record_reconnection();
                            tracing::info!(
                                seed = %seed_addr,
                                "Lazarus: successfully reconnected to seed"
                            );
                        }
                        Err(e) => {
                            handle_clone.record_failure();
                            tracing::debug!(
                                seed = %seed_addr,
                                error = %e,
                                "Lazarus: failed to reconnect to seed"
                            );
                        }
                    }
                }
            }
        });

        // When tokio is not available, log a warning
        #[cfg(not(feature = "tokio"))]
        {
            tracing::warn!("Lazarus task requires the 'tokio' feature. Seed recovery is disabled.");
        }

        handle
    }

    /// Get socket addresses of all alive members.
    ///
    /// This is used by the Lazarus task to check which seeds are currently alive.
    async fn get_alive_addresses(
        memberlist: &Memberlist<T, D>,
    ) -> std::collections::HashSet<SocketAddr> {
        let mut addrs = std::collections::HashSet::new();

        // Get online members and extract their addresses
        let members = memberlist.online_members().await;
        for member in members.iter() {
            // Try to extract SocketAddr from the node's address
            // This handles the common case where the address is a SocketAddr
            if let Some(addr) = Self::extract_member_addr(member) {
                addrs.insert(addr);
            }
        }

        addrs
    }

    /// Extract socket address from a member node.
    fn extract_member_addr(
        node: &std::sync::Arc<memberlist_core::proto::NodeState<I, T::ResolvedAddress>>,
    ) -> Option<SocketAddr> {
        use std::any::Any;

        let addr = node.address();
        let addr_any = addr as &dyn Any;

        // Try direct SocketAddr
        if let Some(socket_addr) = addr_any.downcast_ref::<SocketAddr>() {
            return Some(*socket_addr);
        }

        None
    }

    /// Save current cluster members to persistence file.
    ///
    /// This should be called periodically or on graceful shutdown to save
    /// known peers for recovery after restart.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to save the peer list
    pub async fn save_peers_to_file(
        &self,
        path: &std::path::Path,
    ) -> Result<(), crate::bridge::persistence::PersistenceError> {
        let addrs: Vec<SocketAddr> = Self::get_alive_addresses(&self.memberlist)
            .await
            .into_iter()
            .collect();

        crate::bridge::persistence::save_peers_atomic(path, &addrs)?;

        tracing::debug!(
            path = ?path,
            count = addrs.len(),
            "Saved {} peer addresses to persistence file",
            addrs.len()
        );

        Ok(())
    }

    /// Load persisted peers and combine with static seeds.
    ///
    /// This provides a comprehensive list of potential bootstrap addresses.
    ///
    /// # Arguments
    ///
    /// * `config` - Bridge configuration with persistence path and static seeds
    ///
    /// # Returns
    ///
    /// Combined list of unique addresses from both persistence and static seeds.
    pub fn load_bootstrap_addresses(config: &BridgeConfig) -> Vec<SocketAddr> {
        let mut addrs = std::collections::HashSet::new();

        // Add static seeds
        for seed in &config.static_seeds {
            addrs.insert(*seed);
        }

        // Load persisted peers if path is configured
        if let Some(ref path) = config.persistence_path {
            match crate::bridge::persistence::load_peers(path) {
                Ok(persisted) => {
                    tracing::info!(
                        path = ?path,
                        count = persisted.len(),
                        "Loaded {} peers from persistence file",
                        persisted.len()
                    );
                    for addr in persisted {
                        addrs.insert(addr);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        path = ?path,
                        error = %e,
                        "Failed to load persisted peers"
                    );
                }
            }
        }

        addrs.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = MemberlistDiscoveryConfig::new()
            .with_bind_addr("0.0.0.0:7946".parse().unwrap())
            .with_seed("192.168.1.10:7946".parse().unwrap())
            .with_seed("192.168.1.11:7946".parse().unwrap())
            .with_advertise_addr("10.0.0.1:7946".parse().unwrap());

        assert_eq!(config.bind_addr, "0.0.0.0:7946".parse().unwrap());
        assert_eq!(config.seeds.len(), 2);
        assert_eq!(
            config.advertise_addr,
            Some("10.0.0.1:7946".parse().unwrap())
        );
    }

    #[test]
    fn test_config_with_seeds() {
        let config = MemberlistDiscoveryConfig::new().with_seeds(vec![
            "192.168.1.10:7946".parse().unwrap(),
            "192.168.1.11:7946".parse().unwrap(),
        ]);

        assert_eq!(config.seeds.len(), 2);
    }

    #[test]
    fn test_memberlist_discovery() {
        let config = MemberlistDiscoveryConfig::new()
            .with_bind_addr("0.0.0.0:7946".parse().unwrap())
            .with_advertise_addr("10.0.0.1:7946".parse().unwrap());

        let discovery = MemberlistDiscovery::<u64>::new(config);

        assert_eq!(discovery.bind_addr(), "0.0.0.0:7946".parse().unwrap());
        assert_eq!(discovery.advertise_addr(), "10.0.0.1:7946".parse().unwrap());
        assert_eq!(
            discovery.local_addr(),
            Some("10.0.0.1:7946".parse().unwrap())
        );
    }

    #[test]
    fn test_advertise_defaults_to_bind() {
        let config =
            MemberlistDiscoveryConfig::new().with_bind_addr("127.0.0.1:7946".parse().unwrap());

        let discovery = MemberlistDiscovery::<u64>::new(config);

        assert_eq!(
            discovery.advertise_addr(),
            "127.0.0.1:7946".parse().unwrap()
        );
    }

    #[tokio::test]
    async fn test_start_returns_handle() {
        let discovery = MemberlistDiscovery::<u64>::new(MemberlistDiscoveryConfig::new());

        let (rx, handle) = discovery.start().await;

        assert!(handle.is_running());
        handle.stop();
        assert!(!handle.is_running());

        // Channel should be closed (no events from this shell implementation)
        assert!(rx.recv().await.is_err());
    }

    #[test]
    fn test_from_seeds() {
        let discovery = MemberlistDiscovery::<u64>::from_seeds(
            "0.0.0.0:7946".parse().unwrap(),
            vec![
                "192.168.1.10:7946".parse().unwrap(),
                "192.168.1.11:7946".parse().unwrap(),
            ],
        );

        assert_eq!(discovery.bind_addr(), "0.0.0.0:7946".parse().unwrap());
        assert_eq!(discovery.seeds().len(), 2);
    }

    #[test]
    fn test_config_accessor() {
        let discovery = MemberlistDiscovery::<u64>::from_seeds(
            "0.0.0.0:7946".parse().unwrap(),
            vec!["192.168.1.10:7946".parse().unwrap()],
        );

        let config = discovery.config();
        assert_eq!(config.bind_addr, "0.0.0.0:7946".parse().unwrap());
        assert_eq!(config.seeds.len(), 1);
    }

    #[test]
    fn test_into_config() {
        let discovery = MemberlistDiscovery::<u64>::from_seeds(
            "0.0.0.0:7946".parse().unwrap(),
            vec!["192.168.1.10:7946".parse().unwrap()],
        );

        let config = discovery.into_config();
        assert_eq!(config.bind_addr, "0.0.0.0:7946".parse().unwrap());
        assert_eq!(config.seeds.len(), 1);
    }

    #[test]
    fn test_with_discovery_pattern() {
        use crate::PlumtreeStackConfig;

        // Test that MemberlistDiscovery works with with_discovery
        let discovery = MemberlistDiscovery::<u64>::from_seeds(
            "0.0.0.0:7946".parse().unwrap(),
            vec!["192.168.1.10:7946".parse().unwrap()],
        );

        let stack_config = PlumtreeStackConfig::new(1u64, "0.0.0.0:9000".parse().unwrap())
            .with_discovery(discovery);

        assert_eq!(stack_config.local_id, 1);
        assert_eq!(
            stack_config.discovery.bind_addr(),
            "0.0.0.0:7946".parse().unwrap()
        );
        assert_eq!(stack_config.discovery.seeds().len(), 1);
    }
}
