//! Integration with memberlist delegate system.
//!
//! This module provides the glue between Plumtree and memberlist,
//! handling message routing and membership synchronization.
//!
//! # Sender Identity
//!
//! Since memberlist's broadcast mechanism doesn't provide sender information,
//! Plumtree wraps messages in a `NetworkEnvelope` that includes the sender's
//! node ID. This is critical for proper protocol operation because:
//!
//! - **IHave**: Sender must be known to promote to Eager and send Graft
//! - **Graft**: Requester must be known to send data directly to them
//! - **Prune**: Sender must be known to demote to Lazy
//!
//! The envelope is transparent to users - encoding/decoding happens automatically.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use memberlist_core::{
    delegate::{
        AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, NodeDelegate,
        PingDelegate,
    },
    proto::{Meta, NodeState},
};
use nodecraft::{CheapClone, Id};
use std::{borrow::Cow, fmt::Debug, hash::Hash, marker::PhantomData, sync::Arc};

use crate::{
    config::PlumtreeConfig,
    error::Result,
    message::{MessageId, PlumtreeMessage},
    peer_state::PeerState,
    plumtree::{Plumtree, PlumtreeDelegate, PlumtreeHandle},
};

/// Trait for encoding and decoding node IDs for network transmission.
///
/// This trait must be implemented for any node ID type used with
/// `PlumtreeMemberlist` to enable sender identity tracking.
///
/// # Example
///
/// ```rust
/// use memberlist_plumtree::IdCodec;
/// use bytes::{Buf, BufMut};
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct MyNodeId(u64);
///
/// impl IdCodec for MyNodeId {
///     fn encode_id(&self, buf: &mut impl BufMut) {
///         buf.put_u64(self.0);
///     }
///
///     fn decode_id(buf: &mut impl Buf) -> Option<Self> {
///         if buf.remaining() >= 8 {
///             Some(MyNodeId(buf.get_u64()))
///         } else {
///             None
///         }
///     }
///
///     fn encoded_id_len(&self) -> usize {
///         8
///     }
/// }
/// ```
pub trait IdCodec: Sized {
    /// Encode this ID into the buffer.
    fn encode_id(&self, buf: &mut impl BufMut);

    /// Decode an ID from the buffer.
    fn decode_id(buf: &mut impl Buf) -> Option<Self>;

    /// Get the encoded length of this ID in bytes.
    fn encoded_id_len(&self) -> usize;
}

// Implement IdCodec for common integer types
impl IdCodec for u8 {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u8(*self);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 1 {
            Some(buf.get_u8())
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        1
    }
}

impl IdCodec for u16 {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u16(*self);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 2 {
            Some(buf.get_u16())
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        2
    }
}

impl IdCodec for u32 {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u32(*self);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 4 {
            Some(buf.get_u32())
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        4
    }
}

impl IdCodec for u64 {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u64(*self);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 8 {
            Some(buf.get_u64())
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        8
    }
}

impl IdCodec for u128 {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u128(*self);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 16 {
            Some(buf.get_u128())
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        16
    }
}

// Implement IdCodec for String (variable-length)
impl IdCodec for String {
    fn encode_id(&self, buf: &mut impl BufMut) {
        let bytes = self.as_bytes();
        buf.put_u16(bytes.len() as u16);
        buf.put_slice(bytes);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 2 {
            return None;
        }
        let len = buf.get_u16() as usize;
        if buf.remaining() < len {
            return None;
        }
        let bytes = buf.copy_to_bytes(len);
        String::from_utf8(bytes.to_vec()).ok()
    }

    fn encoded_id_len(&self) -> usize {
        2 + self.len()
    }
}

/// Magic byte prefix for Plumtree messages.
///
/// Used to distinguish Plumtree protocol messages from user messages.
const PLUMTREE_MAGIC: u8 = 0x50;

/// Outgoing broadcast envelope (unencooded).
///
/// This represents a Plumtree message that should be broadcast to all cluster
/// members via memberlist's gossip/broadcast mechanism.
///
/// Use [`encode()`](Self::encode) to serialize for network transmission.
#[derive(Debug, Clone)]
pub struct BroadcastEnvelope<I> {
    /// Sender's node ID (included in envelope for protocol operation).
    pub sender: I,
    /// The Plumtree protocol message.
    pub message: PlumtreeMessage,
}

impl<I: IdCodec> BroadcastEnvelope<I> {
    /// Encode this envelope for network transmission.
    ///
    /// Returns bytes that can be passed to memberlist's broadcast API.
    pub fn encode(&self) -> Bytes {
        encode_plumtree_envelope(&self.sender, &self.message)
    }
}

/// Outgoing unicast envelope (unencooded).
///
/// Used internally to defer serialization until the message is actually
/// sent over the network, reducing unnecessary allocations.
#[derive(Debug, Clone)]
pub(crate) struct UnicastEnvelope<I> {
    /// Sender's node ID (included in envelope for protocol operation).
    sender: I,
    /// Target peer to send to.
    target: I,
    /// The Plumtree protocol message.
    message: PlumtreeMessage,
}

impl<I: IdCodec> UnicastEnvelope<I> {
    /// Encode this envelope for network transmission.
    fn encode(&self) -> Bytes {
        encode_plumtree_envelope(&self.sender, &self.message)
    }
}

/// Combined Plumtree + Memberlist system.
///
/// Wraps a memberlist instance and provides efficient O(n) broadcast
/// via the Plumtree protocol while using memberlist for membership.
///
/// # Recommended Usage (High-Performance Integration)
///
/// For production, it is recommended to combine Plumtree with a reliable unicast
/// transport (like QUIC) and a pooling layer for backpressure control.
///
/// ```ignore
/// use memberlist_plumtree::{
///     PlumtreeMemberlist, PlumtreeConfig, NoopDelegate,
///     QuicTransport, QuicConfig, PooledTransport, PoolConfig,
///     MapPeerResolver, decode_plumtree_envelope
/// };
/// use std::sync::Arc;
///
/// // 1. Setup QUIC Transport with Connection Pooling
/// // MapPeerResolver maps Node IDs to actual Socket Addresses
/// let resolver = Arc::new(MapPeerResolver::new(local_addr));
/// let quic_transport = QuicTransport::new(QuicConfig::default(), resolver, quinn_endpoint);
///
/// // 2. Wrap with PooledTransport for Concurrency & Queueing control
/// let pool_config = PoolConfig::default()
///     .with_max_concurrent_global(100)
///     .with_max_queue_per_peer(512);
/// let transport = PooledTransport::new(quic_transport, pool_config);
///
/// // 3. Initialize PlumtreeMemberlist
/// let pm = Arc::new(PlumtreeMemberlist::new(local_id, PlumtreeConfig::lan(), NoopDelegate));
///
/// // 4. Start the runners
/// // run_with_transport automatically drives the internal PlumtreeRunner (runner.rs)
/// // and pumps Unicast messages (Graft/Prune) through your PooledTransport.
/// let pm_run = pm.clone();
/// let transport_clone = transport.clone();
/// tokio::spawn(async move {
///     pm_run.run_with_transport(transport_clone).await
/// });
///
/// // Handle incoming message processor
/// let pm_proc = pm.clone();
/// tokio::spawn(async move { pm_proc.run_incoming_processor().await });
///
/// // 5. Bridge Broadcasts to Memberlist Gossip
/// // While Unicast uses QUIC, common Gossip/IHave still use memberlist's UDP channel.
/// let pm_out = pm.clone();
/// let ml = memberlist_instance.clone();
/// tokio::spawn(async move {
///     let mut broadcast_rx = pm_out.outgoing_receiver_raw();
///     while let Ok(envelope) = broadcast_rx.recv().await {
///         ml.broadcast(envelope.encode()).await;
///     }
/// });
///
/// // 6. Inbound Injection (Inside your NodeDelegate or Transport Acceptor)
/// // if let Some((sender_id, message)) = decode_plumtree_envelope::<u64>(&raw_data) {
/// //     pm.incoming_sender().send((sender_id, message)).await.ok();
/// // }
/// ```
pub struct PlumtreeMemberlist<I, PD>
where
    I: Id,
{
    /// Plumtree broadcast layer.
    plumtree: Plumtree<I, PlumtreeEventHandler<I, PD>>,
    /// Handle for message I/O.
    handle: PlumtreeHandle<I>,
    /// Channel for sending incoming Plumtree messages to be processed.
    incoming_tx: async_channel::Sender<(I, PlumtreeMessage)>,
    /// Channel for receiving incoming Plumtree messages from memberlist.
    incoming_rx: async_channel::Receiver<(I, PlumtreeMessage)>,
    /// Channel for outgoing broadcast messages (unencooded, serialization deferred).
    outgoing_rx: async_channel::Receiver<BroadcastEnvelope<I>>,
    /// Sender for outgoing broadcast messages.
    outgoing_tx: async_channel::Sender<BroadcastEnvelope<I>>,
    /// Channel for outgoing unicast messages (unencooded, serialization deferred).
    /// These MUST be sent directly to the target peer, NOT broadcast!
    unicast_rx: async_channel::Receiver<UnicastEnvelope<I>>,
    /// Sender for outgoing unicast messages.
    unicast_tx: async_channel::Sender<UnicastEnvelope<I>>,
}

impl<I, PD> PlumtreeMemberlist<I, PD>
where
    I: Id + IdCodec + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// Create a new PlumtreeMemberlist instance.
    ///
    /// # Arguments
    ///
    /// * `local_id` - The local node's identifier
    /// * `config` - Plumtree configuration (hash ring is automatically enabled)
    /// * `delegate` - Application delegate for message delivery
    ///
    /// # Note
    ///
    /// Hash ring topology is automatically enabled for `PlumtreeMemberlist` to ensure
    /// deterministic peer selection and Z≥2 redundancy guarantees.
    pub fn new(local_id: I, config: PlumtreeConfig, delegate: PD) -> Self {
        // Create channels for communication
        let (incoming_tx, incoming_rx) = async_channel::bounded(1024);
        let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);
        let (unicast_tx, unicast_rx) = async_channel::bounded(1024);

        // Create the event handler that wraps the user delegate
        let event_handler = PlumtreeEventHandler::new(delegate);

        // Force hash ring topology for PlumtreeMemberlist
        let config = config.with_hash_ring(true);

        // Create the Plumtree instance
        let (plumtree, handle) = Plumtree::new(local_id, config, event_handler);

        Self {
            plumtree,
            handle,
            incoming_tx,
            incoming_rx,
            outgoing_rx,
            outgoing_tx,
            unicast_rx,
            unicast_tx,
        }
    }

    /// Get the sender for incoming Plumtree messages.
    ///
    /// Pass this to `PlumtreeNodeDelegate` to forward received messages.
    pub fn incoming_sender(&self) -> async_channel::Sender<(I, PlumtreeMessage)> {
        self.incoming_tx.clone()
    }

    /// Get the receiver for outgoing broadcast envelopes.
    ///
    /// These messages have no specific target and should be disseminated
    /// via memberlist's gossip/broadcast mechanism.
    ///
    /// **Note**: Messages are unencooded. Call `.encode()` on the envelope
    /// to get bytes for transmission.
    ///
    /// Pass this to `PlumtreeNodeDelegate` for memberlist broadcast.
    #[allow(dead_code)]
    pub(crate) fn outgoing_receiver_raw(&self) -> async_channel::Receiver<BroadcastEnvelope<I>> {
        self.outgoing_rx.clone()
    }

    /// Get the receiver for outgoing unicast envelopes.
    ///
    /// **CRITICAL**: These messages MUST be sent directly to the specified
    /// target peer using memberlist's direct send API (e.g., `send_reliable`,
    /// `send_to`, or similar). DO NOT broadcast these messages!
    ///
    /// **Note**: Messages are unencooded. Call `.encode()` on the envelope
    /// to get bytes, and use `.target` to get the destination peer.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let unicast_rx = pm.unicast_receiver_raw();
    /// while let Ok(envelope) = unicast_rx.recv().await {
    ///     let target = envelope.target.clone();
    ///     let encoded = envelope.encode();
    ///     // Use memberlist's direct send, NOT broadcast!
    ///     memberlist.send_reliable(&target, encoded).await;
    /// }
    /// ```
    #[allow(dead_code)]
    pub(crate) fn unicast_receiver_raw(&self) -> async_channel::Receiver<UnicastEnvelope<I>> {
        self.unicast_rx.clone()
    }

    /// Get the outgoing broadcast envelope sender.
    #[allow(dead_code)]
    pub(crate) fn outgoing_sender_raw(&self) -> async_channel::Sender<BroadcastEnvelope<I>> {
        self.outgoing_tx.clone()
    }

    /// Get the outgoing unicast envelope sender.
    #[allow(dead_code)]
    pub(crate) fn unicast_sender_raw(&self) -> async_channel::Sender<UnicastEnvelope<I>> {
        self.unicast_tx.clone()
    }

    /// Broadcast a message to all nodes in the cluster.
    ///
    /// Uses Plumtree's efficient O(n) spanning tree broadcast.
    pub async fn broadcast(&self, payload: impl Into<Bytes>) -> Result<MessageId> {
        self.plumtree.broadcast(payload).await
    }

    /// Handle an incoming Plumtree message from the network.
    ///
    /// This should be called when a Plumtree message is received via memberlist.
    pub async fn handle_message(&self, from: I, message: PlumtreeMessage) -> Result<()> {
        self.plumtree.handle_message(from, message).await
    }

    /// Get a reference to the underlying Plumtree instance.
    pub fn plumtree(&self) -> &Plumtree<I, PlumtreeEventHandler<I, PD>> {
        &self.plumtree
    }

    /// Get a reference to the Plumtree handle.
    pub fn handle(&self) -> &PlumtreeHandle<I> {
        &self.handle
    }

    /// Get the Plumtree configuration.
    pub fn config(&self) -> &PlumtreeConfig {
        self.plumtree.config()
    }

    /// Get peer statistics.
    pub fn peer_stats(&self) -> crate::peer_state::PeerStats {
        self.plumtree.peer_stats()
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> crate::message::CacheStats {
        self.plumtree.cache_stats()
    }

    /// Add a peer to the Plumtree overlay with automatic classification.
    ///
    /// Called when a node joins the memberlist cluster.
    /// The peer is automatically classified as eager or lazy based on
    /// the current state and configuration.
    ///
    /// Returns the result of the add operation.
    pub fn add_peer(&self, peer: I) -> crate::peer_state::AddPeerResult {
        self.plumtree.add_peer(peer)
    }

    /// Add a peer to the lazy set only (traditional behavior).
    ///
    /// This bypasses the `max_peers` limit check and auto-classification.
    pub fn add_peer_lazy(&self, peer: I) {
        self.plumtree.add_peer_lazy(peer);
    }

    /// Remove a peer from the Plumtree overlay.
    ///
    /// Called when a node leaves the memberlist cluster.
    pub fn remove_peer(&self, peer: &I) {
        self.plumtree.remove_peer(peer);
    }

    /// Get a reference to the shared peer state.
    ///
    /// This returns the Plumtree's internal peer state, which tracks
    /// eager and lazy peers for message routing.
    pub fn peers(&self) -> &Arc<PeerState<I>> {
        self.plumtree.peers()
    }

    /// Wrap a memberlist delegate with Plumtree integration.
    ///
    /// This creates a `PlumtreeNodeDelegate` that wraps your delegate and
    /// automatically synchronizes peer state when nodes join or leave the cluster.
    ///
    /// The wrapped delegate should be passed to memberlist when creating it.
    ///
    /// # Arguments
    ///
    /// * `delegate` - Your memberlist delegate implementation
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeMemberlist, PlumtreeConfig};
    ///
    /// // Create PlumtreeMemberlist
    /// let pm = PlumtreeMemberlist::new(node_id, PlumtreeConfig::default(), my_plumtree_delegate);
    ///
    /// // Wrap your memberlist delegate
    /// let wrapped = pm.wrap_delegate(my_memberlist_delegate);
    ///
    /// // Create memberlist with the wrapped delegate
    /// let memberlist = Memberlist::new(wrapped, memberlist_config).await?;
    ///
    /// // Run PlumtreeMemberlist with transport
    /// pm.run_with_transport(transport).await;
    /// ```
    pub fn wrap_delegate<A, D>(&self, delegate: D) -> PlumtreeNodeDelegate<I, A, D>
    where
        A: Send + Sync + 'static,
    {
        let config = self.plumtree.config();
        PlumtreeNodeDelegate::new(
            delegate,
            self.incoming_tx.clone(),
            self.outgoing_rx.clone(),
            self.plumtree.peers().clone(),
            config.eager_fanout,
            config.max_peers,
        )
    }

    /// Run the Plumtree background tasks (manual unicast handling).
    ///
    /// This runs the IHave scheduler, Graft timer, and outgoing processor.
    /// Unicast messages are sent to `unicast_receiver()` channel.
    ///
    /// **WARNING**: You MUST process messages from `unicast_receiver()` and send them
    /// directly to the target peer. Failing to do so will break the protocol.
    ///
    /// For safer usage, prefer [`run_with_transport`](Self::run_with_transport) which
    /// handles unicast delivery automatically.
    pub async fn run(&self) {
        futures::future::join4(
            self.plumtree.run_ihave_scheduler(),
            self.plumtree.run_graft_timer(),
            self.plumtree.run_seen_cleanup(),
            self.run_outgoing_processor(),
        )
        .await;
    }

    /// Run the Plumtree background tasks with automatic unicast handling.
    ///
    /// This is the **recommended** way to run PlumtreeMemberlist. It handles
    /// unicast message delivery automatically using the provided transport,
    /// preventing protocol failures from forgotten unicast handling.
    ///
    /// # Arguments
    ///
    /// * `transport` - Implementation of [`Transport`](crate::Transport) for unicast delivery
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeMemberlist, Transport};
    ///
    /// struct MyTransport { memberlist: Memberlist }
    ///
    /// impl Transport<NodeId> for MyTransport {
    ///     type Error = MyError;
    ///     async fn send_to(&self, target: &NodeId, data: Bytes) -> Result<(), Self::Error> {
    ///         self.memberlist.send_reliable(target, data).await
    ///     }
    /// }
    ///
    /// // No need to handle unicast_receiver() - it's done automatically!
    /// let transport = MyTransport { memberlist };
    /// pm.run_with_transport(transport).await;
    /// ```
    pub async fn run_with_transport<T>(&self, transport: T)
    where
        T: crate::Transport<I>,
    {
        futures::future::join5(
            self.plumtree.run_ihave_scheduler(),
            self.plumtree.run_graft_timer(),
            self.plumtree.run_seen_cleanup(),
            self.run_outgoing_processor(),
            self.run_unicast_sender(transport),
        )
        .await;
    }

    /// Internal task that sends unicast messages via the provided transport.
    ///
    /// Serialization is deferred to this point to minimize allocations.
    async fn run_unicast_sender<T>(&self, transport: T)
    where
        T: crate::Transport<I>,
    {
        while let Ok(envelope) = self.unicast_rx.recv().await {
            // Encode at the last moment before network transmission
            let encoded = envelope.encode();
            if let Err(e) = transport.send_to(&envelope.target, encoded).await {
                tracing::warn!(
                    "failed to send unicast message to {:?}: {}",
                    envelope.target,
                    e
                );
            }
        }
    }

    /// Run the outgoing message processor.
    ///
    /// Routes outgoing messages to the appropriate channel:
    /// - Unicast messages (with target) -> unicast_tx
    /// - Broadcast messages (no target) -> outgoing_tx
    ///
    /// Messages are passed as unencooded envelopes. Serialization is deferred
    /// until the message is actually sent over the network to minimize
    /// unnecessary allocations.
    async fn run_outgoing_processor(&self) {
        let local_id = self.plumtree.local_id().clone();

        while let Some(outgoing) = self.handle.next_outgoing().await {
            if let Some(target) = outgoing.target {
                // Unicast: create envelope with target (encoding deferred)
                let envelope = UnicastEnvelope {
                    sender: local_id.clone(),
                    target,
                    message: outgoing.message,
                };
                if self.unicast_tx.send(envelope).await.is_err() {
                    // Channel closed
                    break;
                }
            } else {
                // Broadcast: create envelope (encoding deferred)
                let envelope = BroadcastEnvelope {
                    sender: local_id.clone(),
                    message: outgoing.message,
                };
                if self.outgoing_tx.send(envelope).await.is_err() {
                    // Channel closed
                    break;
                }
            }
        }
    }

    /// Process incoming messages from the incoming channel.
    ///
    /// Should be spawned as a background task if using channels.
    pub async fn run_incoming_processor(&self) {
        while let Ok((from, message)) = self.incoming_rx.recv().await {
            if let Err(e) = self.plumtree.handle_message(from, message).await {
                tracing::warn!("failed to handle plumtree message: {}", e);
            }
        }
    }

    /// Shutdown the Plumtree layer.
    pub fn shutdown(&self) {
        self.plumtree.shutdown();
        self.outgoing_tx.close();
        self.unicast_tx.close();
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.plumtree.is_shutdown()
    }
}

/// Delegate that intercepts messages for Plumtree protocol.
///
/// Wraps a user delegate and handles Plumtree message routing.
/// Use this when creating a memberlist to integrate Plumtree.
pub struct PlumtreeNodeDelegate<I, A, D> {
    /// Inner user delegate.
    inner: D,
    /// Channel to send received Plumtree messages to PlumtreeMemberlist.
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
    /// // Create PlumtreeMemberlist first
    /// let pm = PlumtreeMemberlist::new(node_id, config, delegate);
    ///
    /// // Wrap your memberlist delegate with PlumtreeNodeDelegate
    /// let wrapped_delegate = pm.wrap_delegate(your_memberlist_delegate);
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
            _marker: PhantomData,
        }
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

    async fn notify_message(&self, msg: Cow<'_, [u8]>) {
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
        self.peers
            .add_peer_auto(node.id().clone(), self.max_peers, self.eager_fanout);

        self.inner.notify_join(node).await;
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        // Sync: Remove from Plumtree peers immediately to stop sending to dead node
        self.peers.remove_peer(node.id());

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

/// Event handler that synchronizes memberlist events to Plumtree.
///
/// Wraps a user's PlumtreeDelegate to forward events.
pub struct PlumtreeEventHandler<I, PD> {
    /// Inner Plumtree delegate for application events.
    inner: PD,
    /// Marker for I type parameter.
    _marker: std::marker::PhantomData<I>,
}

impl<I, PD> PlumtreeEventHandler<I, PD> {
    /// Create a new event handler.
    pub fn new(inner: PD) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, PD> PlumtreeDelegate<I> for PlumtreeEventHandler<I, PD>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.inner.on_deliver(message_id, payload);
    }

    fn on_eager_promotion(&self, peer: &I) {
        self.inner.on_eager_promotion(peer);
    }

    fn on_lazy_demotion(&self, peer: &I) {
        self.inner.on_lazy_demotion(peer);
    }

    fn on_graft_sent(&self, peer: &I, message_id: &MessageId) {
        self.inner.on_graft_sent(peer, message_id);
    }

    fn on_prune_sent(&self, peer: &I) {
        self.inner.on_prune_sent(peer);
    }

    fn on_graft_failed(&self, message_id: &MessageId, peer: &I) {
        self.inner.on_graft_failed(message_id, peer);
    }
}

/// Encode a Plumtree message for transmission via memberlist.
///
/// Format: `[MAGIC][message]`
///
/// **Note**: This function does NOT include sender identity. For proper protocol
/// operation, use [`encode_plumtree_envelope`] which includes the sender ID.
pub fn encode_plumtree_message(msg: &PlumtreeMessage) -> Bytes {
    let encoded = msg.encode_to_bytes();
    let mut buf = BytesMut::with_capacity(1 + encoded.len());
    buf.put_u8(PLUMTREE_MAGIC);
    buf.extend_from_slice(&encoded);
    buf.freeze()
}

/// Encode a Plumtree message with sender identity for transmission.
///
/// Format: `[MAGIC][sender_id][message]`
///
/// This is the **recommended** encoding function as it includes the sender's
/// node ID, which is critical for proper Plumtree protocol operation.
///
/// # Zero-Copy Optimization
///
/// This function encodes directly into a single pre-sized buffer, avoiding
/// intermediate allocations. The buffer size is calculated upfront using
/// [`envelope_encoded_len`] to ensure a single allocation.
pub fn encode_plumtree_envelope<I: IdCodec>(sender: &I, msg: &PlumtreeMessage) -> Bytes {
    let id_len = sender.encoded_id_len();
    let msg_len = msg.encoded_len();
    let mut buf = BytesMut::with_capacity(1 + id_len + msg_len);
    buf.put_u8(PLUMTREE_MAGIC);
    sender.encode_id(&mut buf);
    // Encode message directly into buffer (zero-copy, no intermediate allocation)
    msg.encode(&mut buf);
    buf.freeze()
}

/// Encode a Plumtree envelope directly into an existing buffer.
///
/// This is useful for buffer pooling scenarios where you want to reuse
/// buffers to avoid allocation overhead.
///
/// Format: `[MAGIC][sender_id][message]`
///
/// # Arguments
///
/// * `sender` - The sender's node ID
/// * `msg` - The Plumtree message to encode
/// * `buf` - The buffer to encode into (must have sufficient capacity)
///
/// # Returns
///
/// The number of bytes written to the buffer.
pub fn encode_plumtree_envelope_into<I: IdCodec>(
    sender: &I,
    msg: &PlumtreeMessage,
    buf: &mut impl BufMut,
) -> usize {
    let start_len = 1 + sender.encoded_id_len() + msg.encoded_len();
    buf.put_u8(PLUMTREE_MAGIC);
    sender.encode_id(buf);
    msg.encode(buf);
    start_len
}

/// Calculate the encoded length of a Plumtree envelope.
///
/// Use this to pre-allocate buffers of the correct size.
///
/// # Arguments
///
/// * `sender` - The sender's node ID
/// * `msg` - The Plumtree message
///
/// # Returns
///
/// The total number of bytes the encoded envelope will occupy.
pub fn envelope_encoded_len<I: IdCodec>(sender: &I, msg: &PlumtreeMessage) -> usize {
    1 + sender.encoded_id_len() + msg.encoded_len()
}

/// Decode a Plumtree envelope extracting sender ID and message.
///
/// Format: `[MAGIC][sender_id][message]`
///
/// Returns `Some((sender, message))` on success, `None` on decode failure.
pub fn decode_plumtree_envelope<I: IdCodec>(data: &[u8]) -> Option<(I, PlumtreeMessage)> {
    if data.is_empty() || data[0] != PLUMTREE_MAGIC {
        return None;
    }

    let mut cursor = std::io::Cursor::new(&data[1..]);

    // Decode sender ID
    let sender = I::decode_id(&mut cursor)?;

    // Decode message from remaining bytes
    let msg = PlumtreeMessage::decode(&mut cursor)?;

    Some((sender, msg))
}

/// Try to decode a Plumtree message from received bytes (without sender).
///
/// Use this for simple testing or when sender is tracked separately.
/// For production use, prefer [`decode_plumtree_envelope`] which extracts
/// the sender identity needed for proper protocol operation.
pub fn decode_plumtree_message(data: &[u8]) -> Option<PlumtreeMessage> {
    if data.len() > 1 && data[0] == PLUMTREE_MAGIC {
        PlumtreeMessage::decode_from_slice(&data[1..])
    } else {
        None
    }
}

/// Check if data is a Plumtree message.
pub fn is_plumtree_message(data: &[u8]) -> bool {
    !data.is_empty() && data[0] == PLUMTREE_MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopDelegate;

    #[test]
    fn test_encode_decode() {
        let msg = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 5,
            payload: Bytes::from_static(b"hello"),
        };

        let encoded = encode_plumtree_message(&msg);
        assert!(is_plumtree_message(&encoded));

        let decoded = decode_plumtree_message(&encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_non_plumtree_message() {
        let data = b"regular user message";
        assert!(!is_plumtree_message(data));
        assert!(decode_plumtree_message(data).is_none());
    }

    #[test]
    fn test_envelope_encode_decode() {
        let sender: u64 = 42;
        let msg = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 5,
            payload: Bytes::from_static(b"hello"),
        };

        // Encode with sender ID
        let encoded = encode_plumtree_envelope(&sender, &msg);
        assert!(is_plumtree_message(&encoded));

        // Decode and verify sender + message
        let (decoded_sender, decoded_msg): (u64, PlumtreeMessage) =
            decode_plumtree_envelope(&encoded).unwrap();
        assert_eq!(decoded_sender, sender);
        assert_eq!(decoded_msg, msg);
    }

    #[test]
    fn test_envelope_with_string_id() {
        let sender = "node-1".to_string();
        let msg = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new()],
            round: 3,
        };

        let encoded = encode_plumtree_envelope(&sender, &msg);
        let (decoded_sender, decoded_msg): (String, PlumtreeMessage) =
            decode_plumtree_envelope(&encoded).unwrap();

        assert_eq!(decoded_sender, sender);
        assert_eq!(decoded_msg, msg);
    }

    #[test]
    fn test_envelope_graft_message() {
        let sender: u64 = 123;
        let msg = PlumtreeMessage::Graft {
            message_id: MessageId::new(),
            round: 7,
        };

        let encoded = encode_plumtree_envelope(&sender, &msg);
        let (decoded_sender, decoded_msg): (u64, PlumtreeMessage) =
            decode_plumtree_envelope(&encoded).unwrap();

        assert_eq!(decoded_sender, sender);
        assert_eq!(decoded_msg, msg);
    }

    #[test]
    fn test_envelope_prune_message() {
        let sender: u64 = 999;
        let msg = PlumtreeMessage::Prune;

        let encoded = encode_plumtree_envelope(&sender, &msg);
        let (decoded_sender, decoded_msg): (u64, PlumtreeMessage) =
            decode_plumtree_envelope(&encoded).unwrap();

        assert_eq!(decoded_sender, sender);
        assert_eq!(decoded_msg, msg);
    }

    #[test]
    fn test_plumtree_memberlist_creation() {
        let pm: PlumtreeMemberlist<u64, NoopDelegate> =
            PlumtreeMemberlist::new(1u64, PlumtreeConfig::default(), NoopDelegate);

        assert_eq!(*pm.plumtree().local_id(), 1u64);
        assert!(!pm.is_shutdown());
    }

    #[tokio::test]
    async fn test_plumtree_memberlist_broadcast() {
        let pm: PlumtreeMemberlist<u64, NoopDelegate> =
            PlumtreeMemberlist::new(1u64, PlumtreeConfig::default(), NoopDelegate);

        // Add a peer
        pm.add_peer(2u64);

        // Broadcast should succeed
        let msg_id = pm.broadcast(Bytes::from("test")).await.unwrap();
        assert!(msg_id.timestamp() > 0);

        // Check stats
        let cache_stats = pm.cache_stats();
        assert_eq!(cache_stats.entries, 1);
    }

    #[test]
    fn test_plumtree_memberlist_peer_management() {
        let pm: PlumtreeMemberlist<u64, NoopDelegate> =
            PlumtreeMemberlist::new(1u64, PlumtreeConfig::default(), NoopDelegate);

        assert_eq!(pm.peer_stats().total(), 0);

        pm.add_peer(2u64);
        pm.add_peer(3u64);
        assert_eq!(pm.peer_stats().total(), 2);

        pm.remove_peer(&2u64);
        assert_eq!(pm.peer_stats().total(), 1);
    }

    #[test]
    fn test_envelope_encoded_len() {
        let sender: u64 = 42;
        let msg = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 5,
            payload: Bytes::from_static(b"hello world"),
        };

        // Verify encoded_len matches actual encoded length
        let calculated_len = envelope_encoded_len(&sender, &msg);
        let encoded = encode_plumtree_envelope(&sender, &msg);
        assert_eq!(calculated_len, encoded.len());
    }

    #[test]
    fn test_encode_envelope_into_buffer() {
        let sender: u64 = 123;
        let msg = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new(), MessageId::new()],
            round: 10,
        };

        // Encode into a pre-allocated buffer
        let expected_len = envelope_encoded_len(&sender, &msg);
        let mut buf = BytesMut::with_capacity(expected_len);
        let written = encode_plumtree_envelope_into(&sender, &msg, &mut buf);

        assert_eq!(written, expected_len);
        assert_eq!(buf.len(), expected_len);

        // Verify it decodes correctly
        let (decoded_sender, decoded_msg): (u64, PlumtreeMessage) =
            decode_plumtree_envelope(&buf.freeze()).unwrap();
        assert_eq!(decoded_sender, sender);
        assert_eq!(decoded_msg, msg);
    }

    #[test]
    fn test_zero_copy_encoding_produces_same_result() {
        // Verify the optimized encoding produces identical results
        let sender: u64 = 999;
        let msg = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 42,
            payload: Bytes::from_static(b"test payload"),
        };

        // Both methods should produce identical output
        let encoded1 = encode_plumtree_envelope(&sender, &msg);

        let mut buf = BytesMut::with_capacity(envelope_encoded_len(&sender, &msg));
        encode_plumtree_envelope_into(&sender, &msg, &mut buf);
        let encoded2 = buf.freeze();

        assert_eq!(encoded1, encoded2);
    }

    #[test]
    fn test_envelope_encoded_len_all_message_types() {
        let sender: u64 = 1;

        // Gossip
        let gossip = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 0,
            payload: Bytes::from_static(b"test"),
        };
        assert_eq!(
            envelope_encoded_len(&sender, &gossip),
            encode_plumtree_envelope(&sender, &gossip).len()
        );

        // IHave
        let ihave = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new()],
            round: 0,
        };
        assert_eq!(
            envelope_encoded_len(&sender, &ihave),
            encode_plumtree_envelope(&sender, &ihave).len()
        );

        // Graft
        let graft = PlumtreeMessage::Graft {
            message_id: MessageId::new(),
            round: 0,
        };
        assert_eq!(
            envelope_encoded_len(&sender, &graft),
            encode_plumtree_envelope(&sender, &graft).len()
        );

        // Prune
        let prune = PlumtreeMessage::Prune;
        assert_eq!(
            envelope_encoded_len(&sender, &prune),
            encode_plumtree_envelope(&sender, &prune).len()
        );
    }
}
