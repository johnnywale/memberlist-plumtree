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
use nodecraft::Id;
use smallvec::SmallVec;
use std::{fmt::Debug, hash::Hash, sync::Arc};


use crate::{
    config::PlumtreeConfig,
    error::Result,
    message::{MessageId, PlumtreeMessage, SyncMessage},
    peer_state::PeerState,
    plumtree::{Plumtree, PlumtreeDelegate, PlumtreeHandle},
    storage::{current_time_ms, MemoryStore, MessageStore, StoredMessage},
    sync::SyncHandler,
};

/// Storage type alias for convenience.
/// Users can provide their own storage backend by implementing MessageStore.
pub type DefaultStore = MemoryStore;

/// Trait for encoding and decoding node IDs for network transmission.
///
/// This trait must be implemented for any node ID type used with
/// `PlumtreeDiscovery` to enable sender identity tracking.
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
///     PlumtreeDiscovery, PlumtreeConfig, NoopDelegate,
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
/// // 3. Initialize PlumtreeDiscovery
/// let pm = Arc::new(PlumtreeDiscovery::new(local_id, PlumtreeConfig::lan(), NoopDelegate));
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
/// Combined Plumtree + Memberlist system with configurable storage.
///
/// # Type Parameters
///
/// - `I`: Node identifier type
/// - `PD`: Plumtree delegate for message delivery callbacks
/// - `S`: Storage backend implementing [`MessageStore`]
///
/// # Default Storage
///
/// Use [`PlumtreeDiscovery::new`] for in-memory storage (default).
/// Use [`PlumtreeDiscovery::with_storage`] to provide a custom storage backend.
pub struct PlumtreeDiscovery<I, PD, S = DefaultStore>
where
    I: Id,
    S: MessageStore,
{
    /// Plumtree broadcast layer.
    plumtree: Plumtree<I, PlumtreeEventHandler<I, PD, S>>,
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
    /// Message storage for sync/persistence.
    store: Arc<S>,
    /// Sync handler for anti-entropy.
    sync_handler: Arc<SyncHandler<S>>,
}

impl<I, PD> PlumtreeDiscovery<I, PD, DefaultStore>
where
    I: Id + IdCodec + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
{
    /// Create a new PlumtreeDiscovery instance with default in-memory storage.
    ///
    /// # Arguments
    ///
    /// * `local_id` - The local node's identifier
    /// * `config` - Plumtree configuration (hash ring is automatically enabled)
    /// * `delegate` - Application delegate for message delivery
    ///
    /// # Note
    ///
    /// Hash ring topology is automatically enabled for `PlumtreeDiscovery` to ensure
    /// deterministic peer selection and Z≥2 redundancy guarantees.
    ///
    /// Use [`with_storage`](Self::with_storage) to provide a custom storage backend
    /// (e.g., Sled for persistence).
    pub fn new(local_id: I, config: PlumtreeConfig, delegate: PD) -> Self {
        // Create storage based on config
        let max_messages = config
            .storage
            .as_ref()
            .map(|s| s.max_messages)
            .unwrap_or(100_000);
        let store = Arc::new(MemoryStore::new(max_messages));

        Self::with_storage(local_id, config, delegate, store)
    }

    /// Create a PlumtreeDiscovery from a PlumtreeStackConfig with MemberlistDiscovery.
    ///
    /// This is a convenience method for creating a PlumtreeDiscovery when using
    /// the `with_discovery` pattern with `MemberlistDiscovery`.
    ///
    /// # Arguments
    ///
    /// * `config` - Stack configuration with `MemberlistDiscovery`
    /// * `delegate` - Application delegate for message delivery
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{
    ///     PlumtreeStackConfig, PlumtreeConfig, PlumtreeDiscovery, NoopDelegate,
    ///     discovery::{MemberlistDiscovery, MemberlistDiscoveryConfig},
    /// };
    ///
    /// // Create stack config with memberlist discovery
    /// let discovery = MemberlistDiscovery::from_seeds(
    ///     "0.0.0.0:7946".parse().unwrap(),
    ///     vec!["192.168.1.10:7946".parse().unwrap()],
    /// );
    /// let stack_config = PlumtreeStackConfig::new(node_id, "0.0.0.0:9000".parse().unwrap())
    ///     .with_discovery(discovery)
    ///     .with_plumtree(PlumtreeConfig::default());
    ///
    /// // Create PlumtreeDiscovery from config
    /// let pm = PlumtreeDiscovery::from_stack_config(&stack_config, NoopDelegate);
    /// ```
    pub fn from_stack_config<D>(
        config: &crate::PlumtreeStackConfig<I, D>,
        delegate: PD,
    ) -> Self
    where
        D: crate::discovery::ClusterDiscovery<I>,
    {
        Self::new(config.local_id.clone(), config.plumtree.clone(), delegate)
    }
}

impl<I, PD, S> PlumtreeDiscovery<I, PD, S>
where
    I: Id + IdCodec + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
    S: MessageStore + 'static,
{
    /// Create a new PlumtreeDiscovery instance with a custom storage backend.
    ///
    /// # Arguments
    ///
    /// * `local_id` - The local node's identifier
    /// * `config` - Plumtree configuration (hash ring is automatically enabled)
    /// * `delegate` - Application delegate for message delivery
    /// * `store` - Storage backend for message persistence
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::{PlumtreeDiscovery, SledStore};
    ///
    /// let store = Arc::new(SledStore::open("/tmp/plumtree")?);
    /// let pm = PlumtreeDiscovery::with_storage(node_id, config, delegate, store);
    /// ```
    pub fn with_storage(local_id: I, config: PlumtreeConfig, delegate: PD, store: Arc<S>) -> Self {
        // Create channels for communication
        let (incoming_tx, incoming_rx) = async_channel::bounded(1024);
        let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);
        let (unicast_tx, unicast_rx) = async_channel::bounded(1024);

        // Create sync handler
        let sync_handler = Arc::new(SyncHandler::new(store.clone()));

        // Create the event handler that wraps the user delegate and storage
        let event_handler = PlumtreeEventHandler::new(delegate, store.clone(), sync_handler.clone());

        // Force hash ring topology for PlumtreeDiscovery
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
            store,
            sync_handler,
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
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::PlumtreeNodeDelegate;
    ///
    /// let plumtree_delegate = PlumtreeNodeDelegate::new(
    ///     void_delegate,
    ///     pm.incoming_sender(),
    ///     pm.outgoing_receiver(),
    ///     pm.peers().clone(),
    ///     pm.config().eager_fanout,
    ///     pm.config().max_peers,
    /// );
    /// ```
    pub fn outgoing_receiver(&self) -> async_channel::Receiver<BroadcastEnvelope<I>> {
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
    /// The message is also stored for sync/persistence.
    pub async fn broadcast(&self, payload: impl Into<Bytes>) -> Result<MessageId> {
        let payload = payload.into();

        // Broadcast via Plumtree
        let msg_id = self.plumtree.broadcast(payload.clone()).await?;

        // Store for sync/persistence (same as on_deliver)
        self.sync_handler.record_message(msg_id, &payload);

        let msg = StoredMessage::new(msg_id, 0, payload);
        if let Err(e) = self.store.insert(&msg).await {
            tracing::warn!("failed to store broadcast message: {}", e);
        }

        Ok(msg_id)
    }

    /// Handle an incoming Plumtree message from the network.
    ///
    /// This should be called when a Plumtree message is received via memberlist.
    pub async fn handle_message(&self, from: I, message: PlumtreeMessage) -> Result<()> {
        self.plumtree.handle_message(from, message).await
    }

    /// Get a reference to the underlying Plumtree instance.
    pub fn plumtree(&self) -> &Plumtree<I, PlumtreeEventHandler<I, PD, S>> {
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
    /// This is the **recommended** way to run PlumtreeDiscovery. It handles
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
    /// use memberlist_plumtree::{PlumtreeDiscovery, Transport};
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
        T: crate::Transport<I> + Clone,
    {
        // Clone transport for sync task
        let sync_transport = transport.clone();

        futures::future::join(
            futures::future::join(
                futures::future::join5(
                    self.plumtree.run_ihave_scheduler(),
                    self.plumtree.run_graft_timer(),
                    self.plumtree.run_seen_cleanup(),
                    self.run_outgoing_processor(),
                    self.run_unicast_sender(transport),
                ),
                self.run_storage_prune(),
            ),
            self.run_anti_entropy_sync(sync_transport),
        )
        .await;
    }

    /// Run storage pruning background task.
    ///
    /// Periodically removes expired messages based on retention policy.
    async fn run_storage_prune(&self) {
        let Some(ref storage_config) = self.plumtree.config().storage else {
            tracing::trace!("Storage not configured, skipping prune task");
            return;
        };

        if !storage_config.enabled {
            return;
        }

        // Default prune interval is 1/10 of retention, minimum 1 second
        let prune_interval = std::cmp::max(
            storage_config.retention / 10,
            std::time::Duration::from_secs(1),
        );

        tracing::info!(
            retention_s = storage_config.retention.as_secs(),
            interval_s = prune_interval.as_secs(),
            "Storage prune task started"
        );

        loop {
            if self.plumtree.is_shutdown() {
                tracing::info!("Storage prune task shutting down");
                break;
            }

            futures_timer::Delay::new(prune_interval).await;

            if self.plumtree.is_shutdown() {
                break;
            }

            // Calculate cutoff timestamp
            let cutoff = current_time_ms().saturating_sub(storage_config.retention.as_millis() as u64);

            // Prune expired messages from storage
            match self.store.prune(cutoff).await {
                Ok(removed) if removed > 0 => {
                    tracing::debug!(removed, "pruned expired messages from storage");

                    // Also update sync state - rebuild from storage
                    // This is O(n) but happens infrequently (every prune interval)
                    if let Ok((ids, _)) = self.store.get_range(0, u64::MAX, usize::MAX, 0).await {
                        let mut messages_to_rebuild = Vec::new();
                        for id in ids {
                            if let Ok(Some(msg)) = self.store.get(&id).await {
                                messages_to_rebuild.push((id, msg.payload));
                            }
                        }
                        self.sync_handler.rebuild_from_messages(
                            messages_to_rebuild.iter().map(|(id, p)| (*id, p.as_ref())),
                        );
                    }
                }
                Ok(_) => {} // No messages pruned
                Err(e) => {
                    tracing::warn!("failed to prune storage: {}", e);
                }
            }
        }
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
    /// Handles both regular Plumtree messages and sync messages.
    pub async fn run_incoming_processor(&self) {
        while let Ok((from, message)) = self.incoming_rx.recv().await {
            // Handle sync messages specially
            if let PlumtreeMessage::Sync(sync_msg) = &message {
                if let Err(e) = self.handle_sync_message(from.clone(), sync_msg.clone()).await {
                    tracing::warn!("failed to handle sync message: {}", e);
                }
            } else {
                // Regular Plumtree messages
                if let Err(e) = self.plumtree.handle_message(from, message).await {
                    tracing::warn!("failed to handle plumtree message: {}", e);
                }
            }
        }
    }

    /// Handle a sync protocol message.
    async fn handle_sync_message(&self, from: I, sync_msg: SyncMessage) -> Result<()> {
        let local_id = self.plumtree.local_id().clone();

        match sync_msg {
            SyncMessage::Request {
                root_hash,
                time_start,
                time_end,
            } => {
                tracing::debug!(?from, "handling sync request");
                let response = self
                    .sync_handler
                    .handle_sync_request(root_hash, (time_start, time_end))
                    .await;

                // Send response back to requester
                let response_msg = PlumtreeMessage::Sync(SyncMessage::Response {
                    matches: response.matches,
                    message_ids: SmallVec::from_vec(response.message_ids),
                    has_more: response.has_more,
                });
                let envelope = UnicastEnvelope {
                    sender: local_id,
                    target: from,
                    message: response_msg,
                };
                let _ = self.unicast_tx.send(envelope).await;
            }

            SyncMessage::Response {
                matches,
                message_ids,
                has_more: _,
            } => {
                if matches {
                    tracing::debug!(?from, "sync complete - hashes match");
                    return Ok(());
                }

                tracing::debug!(?from, ids = message_ids.len(), "sync response - checking for missing");

                // Check which messages we're missing
                if let Some(pull) = self
                    .sync_handler
                    .handle_sync_response(message_ids.to_vec())
                    .await
                {
                    // Request missing messages
                    let pull_msg = PlumtreeMessage::Sync(SyncMessage::Pull {
                        message_ids: SmallVec::from_vec(pull.message_ids),
                    });
                    let envelope = UnicastEnvelope {
                        sender: local_id,
                        target: from,
                        message: pull_msg,
                    };
                    let _ = self.unicast_tx.send(envelope).await;
                }
            }

            SyncMessage::Pull { message_ids } => {
                tracing::debug!(?from, ids = message_ids.len(), "handling sync pull");

                // Get requested messages from storage
                let push = self
                    .sync_handler
                    .handle_sync_pull(message_ids.to_vec())
                    .await;

                // Send messages back
                let messages: Vec<(MessageId, u32, Bytes)> = push
                    .messages
                    .into_iter()
                    .map(|m| (m.id, m.round, m.payload))
                    .collect();

                let push_msg = PlumtreeMessage::Sync(SyncMessage::Push { messages });
                let envelope = UnicastEnvelope {
                    sender: local_id,
                    target: from,
                    message: push_msg,
                };
                let _ = self.unicast_tx.send(envelope).await;
            }

            SyncMessage::Push { messages } => {
                tracing::debug!(?from, count = messages.len(), "received sync push");

                // Deliver each message through the normal Plumtree path
                for (id, round, payload) in messages {
                    // Create a Gossip message and process it
                    let gossip = PlumtreeMessage::Gossip { id, round, payload };
                    if let Err(e) = self.plumtree.handle_message(from.clone(), gossip).await {
                        tracing::warn!("failed to deliver synced message {:?}: {}", id, e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Run anti-entropy sync background task.
    ///
    /// Periodically initiates sync with random peers to recover missed messages.
    async fn run_anti_entropy_sync<T>(&self, transport: T)
    where
        T: crate::Transport<I>,
    {
        let Some(ref sync_config) = self.plumtree.config().sync else {
            tracing::info!("Anti-entropy sync not configured");
            return;
        };

        if !sync_config.enabled {
            tracing::info!("Anti-entropy sync disabled");
            return;
        }

        tracing::info!(
            interval_s = sync_config.sync_interval.as_secs(),
            window_s = sync_config.sync_window.as_secs(),
            "Anti-entropy sync started"
        );

        let local_id = self.plumtree.local_id().clone();

        loop {
            if self.plumtree.is_shutdown() {
                tracing::info!("Anti-entropy sync shutting down");
                break;
            }

            // Wait for sync interval
            futures_timer::Delay::new(sync_config.sync_interval).await;

            // Check for shutdown after waking
            if self.plumtree.is_shutdown() {
                break;
            }

            // Pick a random peer
            if let Some(peer) = self.plumtree.peers().random_peer() {
                tracing::debug!(?peer, "initiating sync with peer");

                // Calculate time range for sync
                let now = current_time_ms();
                let start = now.saturating_sub(sync_config.sync_window.as_millis() as u64);

                // Get our root hash
                let root_hash = self.sync_handler.root_hash();

                // Send sync request
                let request = PlumtreeMessage::Sync(SyncMessage::Request {
                    root_hash,
                    time_start: start,
                    time_end: now,
                });

                let encoded = encode_plumtree_envelope(&local_id, &request);
                if let Err(e) = transport.send_to(&peer, encoded).await {
                    tracing::warn!(?peer, "failed to send sync request: {}", e);
                }
            } else {
                tracing::trace!("no peers available for sync");
            }
        }
    }

    /// Get a reference to the sync handler.
    pub fn sync_handler(&self) -> &Arc<SyncHandler<S>> {
        &self.sync_handler
    }

    /// Get a reference to the message store.
    pub fn store(&self) -> &Arc<S> {
        &self.store
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

/// Event handler that synchronizes memberlist events to Plumtree.
///
/// Wraps a user's PlumtreeDelegate to forward events and handles
/// message storage for sync/persistence.
///
/// # Type Parameters
///
/// - `I`: Node identifier type
/// - `PD`: Plumtree delegate for message delivery callbacks
/// - `S`: Storage backend implementing [`MessageStore`]
pub struct PlumtreeEventHandler<I, PD, S: MessageStore = DefaultStore> {
    /// Inner Plumtree delegate for application events.
    inner: PD,
    /// Message storage for sync/persistence.
    store: Arc<S>,
    /// Sync handler for anti-entropy.
    sync_handler: Arc<SyncHandler<S>>,
    /// Marker for I type parameter.
    _marker: std::marker::PhantomData<I>,
}

impl<I, PD, S: MessageStore> PlumtreeEventHandler<I, PD, S> {
    /// Create a new event handler with storage integration.
    pub fn new(inner: PD, store: Arc<S>, sync_handler: Arc<SyncHandler<S>>) -> Self {
        Self {
            inner,
            store,
            sync_handler,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<I, PD, S> PlumtreeDelegate<I> for PlumtreeEventHandler<I, PD, S>
where
    I: Clone + Eq + Hash + Send + Sync + 'static,
    PD: PlumtreeDelegate<I>,
    S: MessageStore + 'static,
{
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        // Record in sync state for hash comparison (O(1), synchronous)
        self.sync_handler.record_message(message_id, &payload);

        // Store message for sync/persistence
        // Since on_deliver is synchronous but storage may be async, we spawn a task
        let msg = StoredMessage::new(message_id, 0, payload.clone());
        let store = self.store.clone();

        // Fire-and-forget storage write - spawn a background task
        // This is safe because the sync state is already updated (for hash comparison)
        // and the message is already in the Plumtree cache (for Graft requests)
        std::thread::spawn(move || {
            futures::executor::block_on(async {
                if let Err(e) = store.insert(&msg).await {
                    tracing::warn!("failed to store message: {}", e);
                }
            });
        });

        // Forward to user delegate
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
        let pm: PlumtreeDiscovery<u64, NoopDelegate> =
            PlumtreeDiscovery::new(1u64, PlumtreeConfig::default(), NoopDelegate);

        assert_eq!(*pm.plumtree().local_id(), 1u64);
        assert!(!pm.is_shutdown());
    }

    #[tokio::test]
    async fn test_plumtree_memberlist_broadcast() {
        let pm: PlumtreeDiscovery<u64, NoopDelegate> =
            PlumtreeDiscovery::new(1u64, PlumtreeConfig::default(), NoopDelegate);

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
        let pm: PlumtreeDiscovery<u64, NoopDelegate> =
            PlumtreeDiscovery::new(1u64, PlumtreeConfig::default(), NoopDelegate);

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
