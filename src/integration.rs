//! Integration with memberlist delegate system.
//!
//! This module provides the glue between Plumtree and memberlist,
//! handling message routing and membership synchronization.

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

/// Magic header for Plumtree messages: "PLT" (3 bytes).
///
/// Used to distinguish Plumtree protocol messages from user messages.
/// A longer header reduces collision probability with user data.
const PLUMTREE_MAGIC: &[u8] = b"PLT";

/// Current protocol version.
const PLUMTREE_VERSION: u8 = 0x01;

/// Minimum envelope size: magic (3) + version (1) + sender_len (2) + tag (1) = 7 bytes.
const MIN_ENVELOPE_SIZE: usize = 7;

/// Maximum sender ID length (256 bytes should be plenty for any node ID).
const MAX_SENDER_ID_LEN: usize = 256;

/// Trait for encoding and decoding node IDs to/from bytes.
///
/// This trait must be implemented for your node ID type to use the
/// Plumtree-memberlist integration. It enables sender identification
/// in broadcast messages, which is critical for Plumtree's tree
/// optimization (Prune/Graft).
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::IdCodec;
///
/// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// struct MyNodeId(u64);
///
/// impl IdCodec for MyNodeId {
///     fn encode_id(&self) -> Bytes {
///         Bytes::copy_from_slice(&self.0.to_be_bytes())
///     }
///
///     fn decode_id(bytes: &[u8]) -> Option<Self> {
///         if bytes.len() == 8 {
///             let arr: [u8; 8] = bytes.try_into().ok()?;
///             Some(MyNodeId(u64::from_be_bytes(arr)))
///         } else {
///             None
///         }
///     }
/// }
/// ```
pub trait IdCodec: Sized {
    /// Encode the ID to bytes for wire transmission.
    fn encode_id(&self) -> Bytes;

    /// Decode an ID from bytes. Returns `None` if the bytes are invalid.
    fn decode_id(bytes: &[u8]) -> Option<Self>;
}

/// Result of decoding a Plumtree envelope.
#[derive(Debug)]
pub enum DecodeResult<I> {
    /// Successfully decoded a Plumtree message with sender.
    Ok {
        /// The sender's node ID.
        sender: I,
        /// The decoded message.
        message: PlumtreeMessage,
    },
    /// Data is not a Plumtree message (no magic header match).
    NotPlumtree,
    /// Protocol version is incompatible.
    IncompatibleVersion(u8),
    /// Message is malformed (bad length, invalid sender, etc.).
    Malformed,
}

/// Combined Plumtree + Memberlist system.
///
/// Wraps a memberlist instance and provides efficient O(n) broadcast
/// via the Plumtree protocol while using memberlist for membership.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::{PlumtreeMemberlist, PlumtreeConfig, NoopDelegate};
///
/// // Create with existing memberlist
/// let plumtree_memberlist = PlumtreeMemberlistBuilder::new()
///     .with_config(PlumtreeConfig::lan())
///     .with_delegate(NoopDelegate)
///     .build(memberlist);
///
/// // Broadcast a message
/// plumtree_memberlist.broadcast(b"hello cluster").await?;
/// ```
pub struct PlumtreeMemberlist<I, PD>
where
    I: Id,
{
    /// Local node ID (used for encoding outgoing messages).
    local_id: I,
    /// Plumtree broadcast layer.
    plumtree: Plumtree<I, PlumtreeEventHandler<I, PD>>,
    /// Handle for message I/O.
    handle: PlumtreeHandle<I>,
    /// Channel for sending incoming Plumtree messages to be processed.
    incoming_tx: async_channel::Sender<(I, PlumtreeMessage)>,
    /// Channel for receiving incoming Plumtree messages from memberlist.
    incoming_rx: async_channel::Receiver<(I, PlumtreeMessage)>,
    /// Channel for outgoing encoded messages to memberlist broadcast.
    outgoing_rx: async_channel::Receiver<Bytes>,
    /// Sender for outgoing messages (clone for runner).
    outgoing_tx: async_channel::Sender<Bytes>,
    /// Peer state reference for external sync.
    peers: Arc<PeerState<I>>,
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
    /// * `config` - Plumtree configuration
    /// * `delegate` - Application delegate for message delivery
    pub fn new(local_id: I, config: PlumtreeConfig, delegate: PD) -> Self {
        // Create channels for communication
        let (incoming_tx, incoming_rx) = async_channel::bounded(1024);
        let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);

        // Create shared peer state
        let peers = Arc::new(PeerState::new());

        // Create the event handler that wraps the user delegate
        let event_handler = PlumtreeEventHandler::new(delegate, peers.clone());

        // Create the Plumtree instance
        let (plumtree, handle) = Plumtree::new(local_id.clone(), config, event_handler);

        Self {
            local_id,
            plumtree,
            handle,
            incoming_tx,
            incoming_rx,
            outgoing_rx,
            outgoing_tx,
            peers,
        }
    }

    /// Get the sender for incoming Plumtree messages.
    ///
    /// Pass this to `PlumtreeNodeDelegate` to forward received messages.
    pub fn incoming_sender(&self) -> async_channel::Sender<(I, PlumtreeMessage)> {
        self.incoming_tx.clone()
    }

    /// Get the receiver for outgoing encoded messages.
    ///
    /// Pass this to `PlumtreeNodeDelegate` for memberlist broadcast.
    pub fn outgoing_receiver(&self) -> async_channel::Receiver<Bytes> {
        self.outgoing_rx.clone()
    }

    /// Get the outgoing message sender for the runner.
    pub fn outgoing_sender(&self) -> async_channel::Sender<Bytes> {
        self.outgoing_tx.clone()
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

    /// Add a peer to the Plumtree overlay.
    ///
    /// Called when a node joins the memberlist cluster.
    pub fn add_peer(&self, peer: I) {
        self.plumtree.add_peer(peer);
    }

    /// Remove a peer from the Plumtree overlay.
    ///
    /// Called when a node leaves the memberlist cluster.
    pub fn remove_peer(&self, peer: &I) {
        self.plumtree.remove_peer(peer);
    }

    /// Get a reference to the shared peer state.
    pub fn peers(&self) -> &Arc<PeerState<I>> {
        &self.peers
    }

    /// Run the Plumtree background tasks.
    ///
    /// This runs the IHave scheduler and Graft timer.
    /// Should be spawned as a background task.
    pub async fn run(&self) {
        futures::future::join3(
            self.plumtree.run_ihave_scheduler(),
            self.plumtree.run_graft_timer(),
            self.run_outgoing_processor(),
        )
        .await;
    }

    /// Run the outgoing message processor.
    ///
    /// Encodes outgoing Plumtree messages and sends them to the outgoing channel.
    async fn run_outgoing_processor(&self) {
        while let Some(outgoing) = self.handle.next_outgoing().await {
            // Encode the message with our local ID as sender
            let encoded = encode_plumtree_envelope(&self.local_id, &outgoing.message);

            if self.outgoing_tx.send(encoded).await.is_err() {
                // Channel closed
                break;
            }
        }
    }

    /// Get the local node ID.
    pub fn local_id(&self) -> &I {
        &self.local_id
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
    /// Outgoing Plumtree messages to broadcast via memberlist.
    outgoing_rx: async_channel::Receiver<Bytes>,
    /// Marker.
    _marker: PhantomData<A>,
}

impl<I, A, D> PlumtreeNodeDelegate<I, A, D> {
    /// Create a new Plumtree node delegate.
    ///
    /// # Arguments
    ///
    /// * `inner` - The user's delegate implementation
    /// * `plumtree_tx` - Sender for forwarding received Plumtree messages
    /// * `outgoing_rx` - Receiver for outgoing Plumtree messages to broadcast
    pub fn new(
        inner: D,
        plumtree_tx: async_channel::Sender<(I, PlumtreeMessage)>,
        outgoing_rx: async_channel::Receiver<Bytes>,
    ) -> Self {
        Self {
            inner,
            plumtree_tx,
            outgoing_rx,
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

    /// Get a clone of the outgoing message receiver.
    ///
    /// This can be used to receive outgoing Plumtree messages from external sources.
    pub fn outgoing_receiver(&self) -> async_channel::Receiver<Bytes> {
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
        // Try to decode as a Plumtree envelope
        match decode_plumtree_envelope::<I>(&msg) {
            DecodeResult::Ok { sender, message } => {
                tracing::trace!(
                    "received plumtree message from {:?}: {:?}",
                    sender,
                    message.tag()
                );

                // Forward to the Plumtree processor with sender info
                if let Err(e) = self.plumtree_tx.try_send((sender, message)) {
                    // Log based on message type - control messages are more critical
                    match e {
                        async_channel::TrySendError::Full((_, ref msg)) => {
                            if msg.is_prune() || msg.is_graft() {
                                tracing::error!(
                                    "plumtree channel full, dropped control message: {:?}",
                                    msg.tag()
                                );
                            } else {
                                tracing::warn!(
                                    "plumtree channel full, dropped message: {:?}",
                                    msg.tag()
                                );
                            }
                        }
                        async_channel::TrySendError::Closed(_) => {
                            tracing::debug!("plumtree channel closed");
                        }
                    }
                }
            }
            DecodeResult::NotPlumtree => {
                // Not a Plumtree message - forward to inner delegate
                self.inner.notify_message(msg).await;
            }
            DecodeResult::IncompatibleVersion(version) => {
                tracing::warn!(
                    "received plumtree message with incompatible version: {}, expected: {}",
                    version,
                    PLUMTREE_VERSION
                );
                // Don't forward to inner delegate - this was clearly meant to be
                // a Plumtree message but from an incompatible version
            }
            DecodeResult::Malformed => {
                // Message started with magic but is malformed.
                // This could be a corrupted Plumtree message or a user message
                // that happens to start with "PLT". Forward to inner delegate
                // to be safe.
                tracing::debug!(
                    "malformed plumtree envelope, forwarding to inner delegate"
                );
                self.inner.notify_message(msg).await;
            }
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
        // Collect Plumtree outgoing messages
        let mut plumtree_msgs = Vec::new();
        let mut used = 0;

        while let Ok(msg) = self.outgoing_rx.try_recv() {
            let (len, msg) = encoded_len(msg);
            if used + len <= limit {
                used += len;
                plumtree_msgs.push(msg);
            } else {
                break;
            }
        }

        // Get user messages with remaining space
        let remaining = limit.saturating_sub(used);
        let user_msgs = self.inner.broadcast_messages(remaining, encoded_len).await;

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
    I: Id + IdCodec + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: EventDelegate<Id = I, Address = A>,
{
    type Id = I;
    type Address = A;

    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
        self.inner.notify_join(node).await;
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
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
/// Wraps a user's PlumtreeDelegate and provides peer state management.
pub struct PlumtreeEventHandler<I, PD> {
    /// Inner Plumtree delegate for application events.
    inner: PD,
    /// Reference to peer state for management.
    peers: Arc<PeerState<I>>,
}

impl<I, PD> PlumtreeEventHandler<I, PD> {
    /// Create a new event handler.
    pub fn new(inner: PD, peers: Arc<PeerState<I>>) -> Self {
        Self { inner, peers }
    }

    /// Get a reference to the peer state.
    pub fn peers(&self) -> &Arc<PeerState<I>> {
        &self.peers
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
}

/// Encode a Plumtree envelope for transmission via memberlist.
///
/// Format: [MAGIC (3)][VERSION (1)][SENDER_LEN (2)][SENDER][MESSAGE]
///
/// This is the primary encoding function that includes sender identity,
/// enabling proper Plumtree protocol operation (Prune/Graft).
pub fn encode_plumtree_envelope<I: IdCodec>(sender: &I, msg: &PlumtreeMessage) -> Bytes {
    let sender_bytes = sender.encode_id();
    let message_bytes = msg.encode_to_bytes();

    let total_len = PLUMTREE_MAGIC.len() + 1 + 2 + sender_bytes.len() + message_bytes.len();
    let mut buf = BytesMut::with_capacity(total_len);

    // Magic header
    buf.put_slice(PLUMTREE_MAGIC);
    // Version
    buf.put_u8(PLUMTREE_VERSION);
    // Sender length and data
    buf.put_u16(sender_bytes.len() as u16);
    buf.put_slice(&sender_bytes);
    // Message
    buf.put_slice(&message_bytes);

    buf.freeze()
}

/// Decode a Plumtree envelope from received bytes.
///
/// Returns a `DecodeResult` indicating success, not-plumtree, version mismatch, or malformed.
pub fn decode_plumtree_envelope<I: IdCodec>(data: &[u8]) -> DecodeResult<I> {
    // Check if we have enough bytes for magic header
    if data.len() < PLUMTREE_MAGIC.len() {
        return DecodeResult::NotPlumtree;
    }

    // Check magic header first - if not present, it's not a Plumtree message
    if &data[0..3] != PLUMTREE_MAGIC {
        return DecodeResult::NotPlumtree;
    }

    // At this point, magic is present - so any further issues are Malformed
    // Check minimum size for header
    if data.len() < MIN_ENVELOPE_SIZE {
        return DecodeResult::Malformed;
    }

    // Check version
    let version = data[3];
    if version != PLUMTREE_VERSION {
        return DecodeResult::IncompatibleVersion(version);
    }

    // Parse sender length
    let mut cursor = &data[4..];
    if cursor.remaining() < 2 {
        return DecodeResult::Malformed;
    }
    let sender_len = cursor.get_u16() as usize;

    // Validate sender length (DoS protection)
    if sender_len > MAX_SENDER_ID_LEN {
        return DecodeResult::Malformed;
    }

    // Check remaining data has sender + at least 1 byte for message
    if cursor.remaining() < sender_len + 1 {
        return DecodeResult::Malformed;
    }

    // Decode sender
    let sender_bytes = &cursor[..sender_len];
    let sender = match I::decode_id(sender_bytes) {
        Some(s) => s,
        None => return DecodeResult::Malformed,
    };
    cursor.advance(sender_len);

    // Decode message
    let message = match PlumtreeMessage::decode(&mut cursor) {
        Some(m) => m,
        None => return DecodeResult::Malformed,
    };

    DecodeResult::Ok { sender, message }
}

/// Encode a Plumtree message for transmission (legacy format without sender).
///
/// **Deprecated**: Use `encode_plumtree_envelope` instead for full protocol support.
/// This function is kept for backward compatibility and testing.
///
/// Format: [MAGIC (3)][VERSION (1)][message]
#[deprecated(
    since = "0.2.0",
    note = "Use encode_plumtree_envelope for full protocol support with sender identity"
)]
pub fn encode_plumtree_message(msg: &PlumtreeMessage) -> Bytes {
    let encoded = msg.encode_to_bytes();
    let mut buf = BytesMut::with_capacity(PLUMTREE_MAGIC.len() + 1 + encoded.len());
    buf.put_slice(PLUMTREE_MAGIC);
    buf.put_u8(PLUMTREE_VERSION);
    buf.extend_from_slice(&encoded);
    buf.freeze()
}

/// Try to decode a Plumtree message from received bytes (legacy format without sender).
///
/// **Deprecated**: Use `decode_plumtree_envelope` instead.
/// This function is kept for backward compatibility and testing.
#[deprecated(
    since = "0.2.0",
    note = "Use decode_plumtree_envelope for full protocol support with sender identity"
)]
pub fn decode_plumtree_message(data: &[u8]) -> Option<PlumtreeMessage> {
    if data.len() > 4 && &data[0..3] == PLUMTREE_MAGIC && data[3] == PLUMTREE_VERSION {
        PlumtreeMessage::decode_from_slice(&data[4..])
    } else {
        None
    }
}

/// Check if data is a Plumtree message (checks magic header).
pub fn is_plumtree_message(data: &[u8]) -> bool {
    data.len() >= PLUMTREE_MAGIC.len() && &data[0..3] == PLUMTREE_MAGIC
}

// IdCodec implementations for common types

impl IdCodec for u64 {
    fn encode_id(&self) -> Bytes {
        Bytes::copy_from_slice(&self.to_be_bytes())
    }

    fn decode_id(bytes: &[u8]) -> Option<Self> {
        if bytes.len() == 8 {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(u64::from_be_bytes(arr))
        } else {
            None
        }
    }
}

impl IdCodec for u32 {
    fn encode_id(&self) -> Bytes {
        Bytes::copy_from_slice(&self.to_be_bytes())
    }

    fn decode_id(bytes: &[u8]) -> Option<Self> {
        if bytes.len() == 4 {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(u32::from_be_bytes(arr))
        } else {
            None
        }
    }
}

impl IdCodec for String {
    fn encode_id(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }

    fn decode_id(bytes: &[u8]) -> Option<Self> {
        std::str::from_utf8(bytes).ok().map(|s| s.to_string())
    }
}

impl IdCodec for Bytes {
    fn encode_id(&self) -> Bytes {
        self.clone()
    }

    fn decode_id(bytes: &[u8]) -> Option<Self> {
        Some(Bytes::copy_from_slice(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoopDelegate;

    #[test]
    fn test_encode_decode_envelope() {
        let sender = 42u64;
        let msg = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 5,
            payload: Bytes::from_static(b"hello"),
        };

        let encoded = encode_plumtree_envelope(&sender, &msg);
        assert!(is_plumtree_message(&encoded));

        match decode_plumtree_envelope::<u64>(&encoded) {
            DecodeResult::Ok {
                sender: decoded_sender,
                message: decoded_msg,
            } => {
                assert_eq!(decoded_sender, sender);
                assert_eq!(decoded_msg, msg);
            }
            other => panic!("expected Ok, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_decode_all_message_types() {
        let sender = 123u64;

        // Test Gossip
        let gossip = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 1,
            payload: Bytes::from_static(b"test"),
        };
        let encoded = encode_plumtree_envelope(&sender, &gossip);
        if let DecodeResult::Ok { message, .. } = decode_plumtree_envelope::<u64>(&encoded) {
            assert_eq!(message, gossip);
        } else {
            panic!("failed to decode Gossip");
        }

        // Test IHave
        let ihave = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new(), MessageId::new()],
            round: 2,
        };
        let encoded = encode_plumtree_envelope(&sender, &ihave);
        if let DecodeResult::Ok { message, .. } = decode_plumtree_envelope::<u64>(&encoded) {
            assert_eq!(message, ihave);
        } else {
            panic!("failed to decode IHave");
        }

        // Test Graft
        let graft = PlumtreeMessage::Graft {
            message_id: MessageId::new(),
            round: 3,
        };
        let encoded = encode_plumtree_envelope(&sender, &graft);
        if let DecodeResult::Ok { message, .. } = decode_plumtree_envelope::<u64>(&encoded) {
            assert_eq!(message, graft);
        } else {
            panic!("failed to decode Graft");
        }

        // Test Prune
        let prune = PlumtreeMessage::Prune;
        let encoded = encode_plumtree_envelope(&sender, &prune);
        if let DecodeResult::Ok { message, .. } = decode_plumtree_envelope::<u64>(&encoded) {
            assert_eq!(message, prune);
        } else {
            panic!("failed to decode Prune");
        }
    }

    #[test]
    fn test_non_plumtree_message() {
        let data = b"regular user message";
        assert!(!is_plumtree_message(data));
        assert!(matches!(
            decode_plumtree_envelope::<u64>(data),
            DecodeResult::NotPlumtree
        ));
    }

    #[test]
    fn test_incompatible_version() {
        // Create a message with wrong version
        let mut data = BytesMut::new();
        data.put_slice(PLUMTREE_MAGIC);
        data.put_u8(0xFF); // Wrong version
        data.put_u16(8); // sender len
        data.put_u64(42); // sender
        data.put_u8(1); // message tag

        assert!(matches!(
            decode_plumtree_envelope::<u64>(&data),
            DecodeResult::IncompatibleVersion(0xFF)
        ));
    }

    #[test]
    fn test_malformed_sender_too_long() {
        // Create a message with sender_len > MAX_SENDER_ID_LEN
        let mut data = BytesMut::new();
        data.put_slice(PLUMTREE_MAGIC);
        data.put_u8(PLUMTREE_VERSION);
        data.put_u16(1000); // Too long

        assert!(matches!(
            decode_plumtree_envelope::<u64>(&data),
            DecodeResult::Malformed
        ));
    }

    #[test]
    fn test_malformed_truncated() {
        // Create a truncated message
        let mut data = BytesMut::new();
        data.put_slice(PLUMTREE_MAGIC);
        data.put_u8(PLUMTREE_VERSION);
        data.put_u16(8); // sender len = 8
        data.put_slice(&[1, 2, 3]); // Only 3 bytes, not 8

        assert!(matches!(
            decode_plumtree_envelope::<u64>(&data),
            DecodeResult::Malformed
        ));
    }

    #[test]
    fn test_plumtree_memberlist_creation() {
        let pm: PlumtreeMemberlist<u64, NoopDelegate> =
            PlumtreeMemberlist::new(1u64, PlumtreeConfig::default(), NoopDelegate);

        assert_eq!(*pm.plumtree().local_id(), 1u64);
        assert_eq!(*pm.local_id(), 1u64);
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
    fn test_id_codec_u64() {
        let id = 0x123456789ABCDEFu64;
        let encoded = id.encode_id();
        let decoded = u64::decode_id(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_id_codec_string() {
        let id = "node-1".to_string();
        let encoded = id.encode_id();
        let decoded = String::decode_id(&encoded).unwrap();
        assert_eq!(id, decoded);
    }
}
