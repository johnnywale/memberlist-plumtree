//! Integration with memberlist delegate system.
//!
//! This module provides the glue between Plumtree and memberlist,
//! handling message routing and membership synchronization.

use bytes::{BufMut, Bytes, BytesMut};
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

/// Magic byte prefix for Plumtree messages.
///
/// Used to distinguish Plumtree protocol messages from user messages.
const PLUMTREE_MAGIC: u8 = 0x50;

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
    I: Id + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    PD: PlumtreeDelegate,
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
        let (plumtree, handle) = Plumtree::new(local_id, config, event_handler);

        Self {
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
            // Encode the message for broadcast
            let encoded = encode_plumtree_message(&outgoing.message);

            if self.outgoing_tx.send(encoded).await.is_err() {
                // Channel closed
                break;
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
    I: Id + Clone + Send + Sync + 'static,
    A: CheapClone + Send + Sync + 'static,
    D: NodeDelegate,
{
    async fn node_meta(&self, limit: usize) -> Meta {
        self.inner.node_meta(limit).await
    }

    async fn notify_message(&self, msg: Cow<'_, [u8]>) {
        // Check if this is a Plumtree message
        if msg.len() > 1 && msg[0] == PLUMTREE_MAGIC {
            // Decode Plumtree message
            if let Some(plumtree_msg) = PlumtreeMessage::decode_from_slice(&msg[1..]) {
                tracing::trace!("received plumtree message: {:?}", plumtree_msg.tag());
                // Note: memberlist broadcast doesn't provide sender info
                // For full Plumtree functionality, use point-to-point messages
                // or include sender in the message payload
                tracing::debug!("plumtree message received via broadcast (sender unknown)");
            } else {
                tracing::warn!("failed to decode plumtree message");
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
    I: Id + Clone + Send + Sync + 'static,
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
    I: Id + Clone + Send + Sync + 'static,
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
    I: Id + Clone + Send + Sync + 'static,
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
    I: Id + Clone + Send + Sync + 'static,
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
    I: Id + Clone + Send + Sync + 'static,
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
    I: Id + Clone + Send + Sync + 'static,
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

impl<I, PD> PlumtreeDelegate for PlumtreeEventHandler<I, PD>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    PD: PlumtreeDelegate,
{
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.inner.on_deliver(message_id, payload);
    }

    fn on_eager_promotion(&self, peer: &[u8]) {
        self.inner.on_eager_promotion(peer);
    }

    fn on_lazy_demotion(&self, peer: &[u8]) {
        self.inner.on_lazy_demotion(peer);
    }

    fn on_graft_sent(&self, peer: &[u8], message_id: &MessageId) {
        self.inner.on_graft_sent(peer, message_id);
    }

    fn on_prune_sent(&self, peer: &[u8]) {
        self.inner.on_prune_sent(peer);
    }
}

/// Encode a Plumtree message for transmission via memberlist.
///
/// Format: [MAGIC][message]
pub fn encode_plumtree_message(msg: &PlumtreeMessage) -> Bytes {
    let encoded = msg.encode_to_bytes();
    let mut buf = BytesMut::with_capacity(1 + encoded.len());
    buf.put_u8(PLUMTREE_MAGIC);
    buf.extend_from_slice(&encoded);
    buf.freeze()
}

/// Try to decode a Plumtree message from received bytes (without sender).
///
/// Use this for simple testing or when sender is tracked separately.
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
}
