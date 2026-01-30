//! Web-based Chat example for Plumtree protocol testing with real networking
//!
//! ## Architecture: MemberlistStack with Real SWIM Discovery
//!
//! This example uses `MemberlistStack` for **real networking** with 50 nodes.
//! Each node runs on a unique port and uses SWIM gossip for automatic peer discovery.
//!
//! Key components:
//! - `MemberlistStack` - combines Plumtree + Memberlist with auto peer sync
//! - `NetTransport` - real UDP/TCP networking on localhost
//! - `PlumtreeNodeDelegate` - automatically syncs Plumtree topology on SWIM events
//! - `ChannelTransport` - routes Plumtree messages between local nodes
//!
//! ## Features
//! - Real-time WebSocket updates
//! - Static HTML UI with peer tree visualization
//! - User switching via dropdown (NUM_USERS users)
//! - Protocol metrics display
//! - Fault injection (node failure)
//! - Real SWIM-based peer discovery (no manual add_peer)
//! - Proper shutdown/restart on online/offline toggle
//!
//! Run with: cargo run --example web-chat --features tokio
//! Then open: http://localhost:3000

#![allow(clippy::type_complexity)]
#![allow(clippy::needless_range_loop)]

mod web_chat_common;

use bytes::{Buf, BufMut, Bytes};
use memberlist::net::NetTransportOptions;
use memberlist::tokio::{TokioRuntime, TokioSocketAddrResolver, TokioTcp};
use memberlist::{Memberlist, Options as MemberlistOptions};
use memberlist_plumtree::{
    storage::MemoryStore, CompressionConfig, Error, IdCodec, MessageId, PeerHealthConfig,
    PeerHealthTracker, PeerStats, PeerTopology, PlumtreeBridge, PlumtreeConfig, PlumtreeDelegate,
    PlumtreeDiscovery, PlumtreeNodeDelegate, SyncConfig,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use nodecraft::resolver::socket_addr::SocketAddrResolver;
use nodecraft::CheapClone;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, RwLock};

// Import shared utilities and types
use web_chat_common::{
    AppState, ChatDelegate, ChatMessage, ChatNode as ChatNodeTrait, DeliveryMethod, Metrics,
    NodeIdFormatter, ProtocolEvent, ReceivedMessage, TimestampedEvent, WebChatConfig,
};

// ============================================================================
// Node ID Type - implements all required traits for Memberlist + Plumtree
// ============================================================================

/// Simple node identifier that implements all required traits.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId(u32);

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

impl CheapClone for NodeId {}

impl IdCodec for NodeId {
    fn encode_id(&self, buf: &mut impl BufMut) {
        buf.put_u32(self.0);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() >= 4 {
            Some(NodeId(buf.get_u32()))
        } else {
            None
        }
    }

    fn encoded_id_len(&self) -> usize {
        4
    }
}

impl NodeIdFormatter for NodeId {
    fn to_user_name(&self) -> String {
        format!("U{}", self.0 + 1)
    }

    fn from_index(idx: usize) -> Self {
        NodeId(idx as u32)
    }
}

// Required for memberlist wire protocol - implement memberlist_proto::Data
mod data_impl {
    use super::NodeId;
    use memberlist::proto::{Data, DataRef, DecodeError, EncodeError};
    use std::borrow::Cow;

    // DataRef implementation for decoding
    impl<'a> DataRef<'a, NodeId> for u32 {
        fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError> {
            if buf.len() < 4 {
                return Err(DecodeError::Custom(Cow::Borrowed(
                    "insufficient data for u32",
                )));
            }
            let value = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            Ok((4, value))
        }
    }

    impl Data for NodeId {
        type Ref<'a> = u32;

        fn from_ref(val: Self::Ref<'_>) -> Result<Self, DecodeError> {
            Ok(NodeId(val))
        }

        fn encoded_len(&self) -> usize {
            4
        }

        fn encode(&self, buf: &mut [u8]) -> Result<usize, EncodeError> {
            if buf.len() < 4 {
                return Err(EncodeError::InsufficientBuffer {
                    required: 4,
                    remaining: buf.len(),
                });
            }
            buf[..4].copy_from_slice(&self.0.to_be_bytes());
            Ok(4)
        }
    }
}

// ============================================================================
// Type Aliases
// ============================================================================

/// The memberlist transport type.
type MemberlistTransport =
    memberlist::net::NetTransport<NodeId, TokioSocketAddrResolver, TokioTcp, TokioRuntime>;

/// The delegate type used by MemberlistStack.
type StackDelegate = memberlist_plumtree::PlumtreeNodeDelegate<
    NodeId,
    SocketAddr,
    memberlist::delegate::VoidDelegate<NodeId, SocketAddr>,
>;

/// The full MemberlistStack type.
type ChatStack = memberlist_plumtree::MemberlistStack<
    NodeId,
    Arc<MemberlistChatDelegate>,
    MemberlistTransport,
    StackDelegate,
>;

// ============================================================================
// Plumtree Delegate (uses shared types from web_chat_common)
// ============================================================================

/// Plumtree delegate that handles message delivery.
struct MemberlistChatDelegate {
    node_name: String,
    messages: Arc<RwLock<Vec<ReceivedMessage>>>,
    events: Arc<RwLock<Vec<TimestampedEvent>>>,
    metrics: Arc<Metrics>,
    update_tx: broadcast::Sender<Value>,
    pending_context: Arc<RwLock<HashMap<MessageId, (String, u32)>>>,
    start_time: Instant,
    node_idx: usize,
    /// Reference to the PlumtreeDiscovery (set after construction)
    pm: Arc<RwLock<Option<Arc<PlumtreeDiscovery<NodeId, Arc<MemberlistChatDelegate>>>>>>,
    /// Peer health tracker for Phase 1 features
    #[allow(dead_code)]
    peer_health: Arc<PeerHealthTracker<NodeId>>,
}

impl MemberlistChatDelegate {
    fn new(node_idx: usize, node_name: String, update_tx: broadcast::Sender<Value>) -> Self {
        Self {
            node_name,
            messages: Arc::new(RwLock::new(Vec::new())),
            events: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(Metrics::new()),
            update_tx,
            pending_context: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            node_idx,
            pm: Arc::new(RwLock::new(None)),
            peer_health: Arc::new(PeerHealthTracker::new(PeerHealthConfig::default())),
        }
    }

    /// Set the PlumtreeDiscovery reference (called after construction)
    async fn set_pm(&self, pm: Arc<PlumtreeDiscovery<NodeId, Arc<MemberlistChatDelegate>>>) {
        *self.pm.write().await = Some(pm);
    }

    /// Clear the PlumtreeDiscovery reference (called on shutdown)
    async fn clear_pm(&self) {
        *self.pm.write().await = None;
    }

    fn format_elapsed(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs();
        format!("{:02}:{:02}", elapsed / 60, elapsed % 60)
    }

    #[allow(dead_code)]
    async fn get_topology(&self) -> PeerTopology<NodeId> {
        let pm_guard = self.pm.read().await;
        if let Some(ref pm) = *pm_guard {
            pm.peers().topology()
        } else {
            PeerTopology::default()
        }
    }
}

// Implement ChatDelegate trait for shared handlers
#[allow(clippy::manual_async_fn)]
impl ChatDelegate for MemberlistChatDelegate {
    fn messages(&self) -> &Arc<RwLock<Vec<ReceivedMessage>>> {
        &self.messages
    }

    fn events(&self) -> &Arc<RwLock<Vec<TimestampedEvent>>> {
        &self.events
    }

    fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    fn get_peers_json(&self) -> impl std::future::Future<Output = Value> + Send {
        let pm_ref = self.pm.clone();
        async move {
            let pm_guard = pm_ref.read().await;
            if let Some(ref pm) = *pm_guard {
                let topo = pm.peers().topology();
                let eager: Vec<String> =
                    topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect();
                let lazy: Vec<String> = topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect();
                json!({ "eager": eager, "lazy": lazy })
            } else {
                json!({ "eager": [], "lazy": [] })
            }
        }
    }

    fn add_event(&self, event: ProtocolEvent) -> impl std::future::Future<Output = ()> + Send {
        let events = self.events.clone();
        let time = self.format_elapsed();
        async move {
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped);
            if evts.len() > 100 {
                evts.remove(0);
            }
        }
    }
}

impl PlumtreeDelegate<NodeId> for MemberlistChatDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        if let Some(chat_msg) = ChatMessage::decode(&payload) {
            let is_self = chat_msg.from == self.node_name;
            let pending = self.pending_context.clone();
            let messages = self.messages.clone();
            let update_tx = self.update_tx.clone();
            let node_idx = self.node_idx;
            let metrics = self.metrics.clone();
            let pm_ref = self.pm.clone();

            tokio::spawn(async move {
                let delivery = if is_self {
                    DeliveryMethod::SelfBroadcast
                } else {
                    let ctx = pending.read().await;
                    if let Some((forwarder, round)) = ctx.get(&message_id) {
                        DeliveryMethod::Gossip {
                            round: *round,
                            forwarder: forwarder.clone(),
                        }
                    } else {
                        DeliveryMethod::Gossip {
                            round: 0,
                            forwarder: chat_msg.from.clone(),
                        }
                    }
                };

                let timestamp = chat_msg.format_time();
                let received = ReceivedMessage {
                    from: chat_msg.from,
                    text: chat_msg.text,
                    timestamp,
                    delivery: delivery.to_string(),
                    message_id,
                };

                messages.write().await.push(received.clone());

                let peers_json = {
                    let pm_guard = pm_ref.read().await;
                    if let Some(ref pm) = *pm_guard {
                        let topo = pm.peers().topology();
                        json!({
                            "eager": topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>(),
                            "lazy": topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>()
                        })
                    } else {
                        json!({ "eager": [], "lazy": [] })
                    }
                };

                let _ = update_tx.send(json!({
                    "type": "message",
                    "node": node_idx,
                    "message": received.to_json(),
                    "metrics": metrics.to_json(),
                    "peers": peers_json
                }));
            });
        }
    }

    fn on_eager_promotion(&self, peer: &NodeId) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", peer.0 + 1);
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let pm_ref = self.pm.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PeerPromoted { peer: peer_name };
            let timestamped = TimestampedEvent {
                time,
                event: event.clone(),
            };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let peers_json = {
                let pm_guard = pm_ref.read().await;
                if let Some(ref pm) = *pm_guard {
                    let topo = pm.peers().topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>()
                    })
                } else {
                    json!({ "eager": [], "lazy": [] })
                }
            };

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.to_json(),
                "peers": peers_json
            }));
        });
    }

    fn on_lazy_demotion(&self, peer: &NodeId) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", peer.0 + 1);
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let pm_ref = self.pm.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PeerDemoted { peer: peer_name };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let peers_json = {
                let pm_guard = pm_ref.read().await;
                if let Some(ref pm) = *pm_guard {
                    let topo = pm.peers().topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>()
                    })
                } else {
                    json!({ "eager": [], "lazy": [] })
                }
            };

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.to_json(),
                "peers": peers_json
            }));
        });
    }

    fn on_graft_sent(&self, peer: &NodeId, message_id: &MessageId) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", peer.0 + 1);
        let msg_id = message_id.to_string();
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let pm_ref = self.pm.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::GraftSent {
                to: peer_name,
                msg_id,
            };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let peers_json = {
                let pm_guard = pm_ref.read().await;
                if let Some(ref pm) = *pm_guard {
                    let topo = pm.peers().topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>()
                    })
                } else {
                    json!({ "eager": [], "lazy": [] })
                }
            };

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.to_json(),
                "peers": peers_json
            }));
        });
    }

    fn on_prune_sent(&self, peer: &NodeId) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", peer.0 + 1);
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let pm_ref = self.pm.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PruneSent { to: peer_name };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let peers_json = {
                let pm_guard = pm_ref.read().await;
                if let Some(ref pm) = *pm_guard {
                    let topo = pm.peers().topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect::<Vec<_>>()
                    })
                } else {
                    json!({ "eager": [], "lazy": [] })
                }
            };

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.to_json(),
                "peers": peers_json
            }));
        });
    }
}

// ============================================================================
// Chat Node using MemberlistStack
// ============================================================================

/// Chat node using MemberlistStack for real networking.
struct ChatNode {
    node_idx: usize,
    node_id: NodeId,
    delegate: Arc<MemberlistChatDelegate>,
    /// The stack is created on start() and dropped on stop() - wrapped in Arc for cloning into async
    stack: Option<Arc<ChatStack>>,
    is_online: bool,
    port: u16,
    seed_addr: Option<SocketAddr>,
    /// Persistent message store (survives restarts for sync recovery)
    message_store: Arc<MemoryStore>,
}

impl ChatNode {
    fn new(
        node_idx: usize,
        update_tx: broadcast::Sender<Value>,
        seed_addr: Option<SocketAddr>,
        base_port: u16,
    ) -> Self {
        let node_id = NodeId(node_idx as u32);
        let node_name = format!("U{}", node_idx + 1);
        let delegate = Arc::new(MemberlistChatDelegate::new(node_idx, node_name, update_tx));
        let port = base_port + node_idx as u16;

        Self {
            node_idx,
            node_id,
            delegate,
            stack: None,
            is_online: false,
            port,
            seed_addr,
            message_store: Arc::new(MemoryStore::new(10_000)),
        }
    }
}

// Implement ChatNode trait for shared handlers
// We use `impl Future + Send` instead of `async fn` because the trait requires
// explicit Send bounds for compatibility with axum's WebSocket handlers.
#[allow(clippy::manual_async_fn)]
impl ChatNodeTrait for ChatNode {
    fn start(&mut self) -> impl std::future::Future<Output = Result<(), String>> + Send {
        async move {
            if self.is_online {
                return Ok(());
            }

            let node_id = self.node_id.clone();
            let bind_addr: SocketAddr = format!("127.0.0.1:{}", self.port)
                .parse()
                .map_err(|e| format!("Invalid address: {}", e))?;

            // Configure compression
            #[cfg(feature = "compression")]
            let compression_config = CompressionConfig::zstd(3).with_min_size(64);
            #[cfg(not(feature = "compression"))]
            let compression_config = CompressionConfig::default();

            let config = PlumtreeConfig::default()
                .with_eager_fanout(2)
                .with_max_peers(6)
                .with_lazy_fanout(3)
                .with_ihave_interval(Duration::from_millis(10))
                .with_graft_timeout(Duration::from_millis(100))
                .with_message_cache_ttl(Duration::from_secs(60))
                .with_hash_ring(true)
                .with_protect_ring_neighbors(true)
                .with_max_eager_peers(3)
                .with_max_lazy_peers(8)
                .with_sync(
                    SyncConfig::enabled()
                        .with_sync_interval(Duration::from_secs(5))
                        .with_sync_window(Duration::from_secs(120)),
                )
                .with_compression(compression_config);

            let pm = Arc::new(PlumtreeDiscovery::with_storage(
                node_id.clone(),
                config,
                self.delegate.clone(),
                self.message_store.clone(),
            ));

            self.delegate.set_pm(pm.clone()).await;

            let void_delegate = memberlist::delegate::VoidDelegate::<NodeId, SocketAddr>::default();

            let delegate_for_promotion = self.delegate.clone();
            let delegate_for_demotion = self.delegate.clone();

            let plumtree_delegate = PlumtreeNodeDelegate::new(
                void_delegate,
                pm.incoming_sender(),
                pm.outgoing_receiver(),
                pm.peers().clone(),
                pm.config().eager_fanout,
                pm.config().max_peers,
            )
            .with_promotion_callback(std::sync::Arc::new(move |peer: &NodeId| {
                let delegate = delegate_for_promotion.clone();
                let peer = peer.clone();
                tokio::spawn(async move {
                    delegate.on_eager_promotion(&peer);
                });
            }))
            .with_demotion_callback(std::sync::Arc::new(move |peer: &NodeId| {
                let delegate = delegate_for_demotion.clone();
                let peer = peer.clone();
                tokio::spawn(async move {
                    delegate.on_lazy_demotion(&peer);
                });
            }));

            let mut transport_opts =
                NetTransportOptions::<NodeId, SocketAddrResolver<TokioRuntime>, TokioTcp>::new(
                    node_id.clone(),
                );
            transport_opts.add_bind_address(bind_addr);

            let memberlist_opts = MemberlistOptions::local()
                .with_probe_interval(Duration::from_millis(500))
                .with_gossip_interval(Duration::from_millis(200));

            let memberlist =
                Memberlist::with_delegate(plumtree_delegate, transport_opts, memberlist_opts)
                    .await
                    .map_err(|e| format!("Failed to create memberlist: {}", e))?;

            let advertise_addr = *memberlist.advertise_address();
            let bridge = PlumtreeBridge::new(pm);
            let stack =
                memberlist_plumtree::MemberlistStack::new(bridge, memberlist, advertise_addr);

            stack.start();

            if let Some(seed) = self.seed_addr {
                stack
                    .join(&[seed])
                    .await
                    .map_err(|e| format!("Failed to join cluster: {}", e))?;
            }

            self.stack = Some(Arc::new(stack));
            self.is_online = true;
            ChatDelegate::add_event(&*self.delegate, ProtocolEvent::NodeOnline).await;

            Ok(())
        }
    }

    fn stop(&mut self) -> impl std::future::Future<Output = ()> + Send {
        async move {
            if !self.is_online {
                return;
            }

            if let Some(stack) = self.stack.take() {
                let _ = stack.leave(Duration::from_secs(1)).await;
                let _ = stack.shutdown().await;
            }

            self.delegate.clear_pm().await;
            self.is_online = false;
            ChatDelegate::add_event(&*self.delegate, ProtocolEvent::NodeOffline).await;
        }
    }

    fn is_online(&self) -> bool {
        self.is_online
    }

    fn broadcast(
        &self,
        text: &str,
    ) -> impl std::future::Future<Output = Result<MessageId, Error>> + Send {
        // Capture all needed data synchronously
        let text = text.to_string();
        let node_idx = self.node_idx;
        let is_online = self.is_online;
        let delegate = self.delegate.clone();
        let stack = self.stack.clone(); // Arc clone is cheap

        async move {
            if !is_online {
                return Err(Error::Shutdown);
            }

            let stack = stack.ok_or(Error::Shutdown)?;

            let msg = ChatMessage::new(format!("U{}", node_idx + 1), &text);
            let payload = msg.encode();

            let msg_id = stack.broadcast(payload.clone()).await?;
            delegate.on_deliver(msg_id, payload);
            Ok(msg_id)
        }
    }

    fn get_peers_json(&self) -> impl std::future::Future<Output = Value> + Send {
        // Collect topology data synchronously
        let peers_json = if let Some(ref stack) = self.stack {
            let topo = stack.plumtree().peers().topology();
            let eager: Vec<String> = topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect();
            let lazy: Vec<String> = topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect();
            json!({ "eager": eager, "lazy": lazy })
        } else {
            json!({ "eager": [], "lazy": [] })
        };

        async move { peers_json }
    }

    fn peer_stats(&self) -> PeerStats {
        if let Some(ref stack) = self.stack {
            stack.plumtree().peer_stats()
        } else {
            PeerStats {
                eager_count: 0,
                lazy_count: 0,
            }
        }
    }

    fn promote_all_to_eager(&self) -> impl std::future::Future<Output = ()> + Send {
        // Do the promotion synchronously
        if let Some(ref stack) = self.stack {
            let topology = stack.plumtree().peers().topology();
            for peer in topology.eager.iter().chain(topology.lazy.iter()) {
                stack.plumtree().peers().promote_to_eager(peer);
            }
        }

        async {}
    }

    fn demote_all_to_lazy(&self) -> impl std::future::Future<Output = ()> + Send {
        // Do the demotion synchronously
        if let Some(ref stack) = self.stack {
            let topology = stack.plumtree().peers().topology();
            for peer in topology.eager.clone() {
                stack.plumtree().peers().demote_to_lazy(&peer);
            }
        }

        async {}
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
    let config = WebChatConfig::memberlist();

    web_chat_common::print_startup_banner(&config);

    // Initialize Prometheus metrics recorder
    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    println!("Prometheus metrics recorder installed");

    // Create broadcast channel for WebSocket updates
    let (update_tx, _) = broadcast::channel::<Value>(1000);

    // Seed address is node 0
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", config.base_port).parse().unwrap();

    // Create nodes
    let mut nodes: Vec<Arc<RwLock<ChatNode>>> = Vec::new();
    let mut delegates = Vec::new();

    for i in 0..config.num_users {
        let node_seed = if i == 0 { None } else { Some(seed_addr) };
        let node = ChatNode::new(i, update_tx.clone(), node_seed, config.base_port);
        delegates.push(node.delegate.clone());
        nodes.push(Arc::new(RwLock::new(node)));
    }

    // Start seed node first
    println!("Starting seed node U1 on port {}...", config.base_port);
    if let Err(e) = nodes[0].write().await.start().await {
        eprintln!("Failed to start seed node: {}", e);
        return;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start remaining nodes
    for i in 1..config.num_users {
        print!("Starting node U{}...", i + 1);
        match nodes[i].write().await.start().await {
            Ok(_) => println!(" OK"),
            Err(e) => println!(" FAILED: {}", e),
        }
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    println!("\nAll {} nodes started!", config.num_users);
    println!("Waiting for SWIM gossip to propagate...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Print cluster status
    let seed_members = nodes[0]
        .read()
        .await
        .stack
        .as_ref()
        .map(|s| futures::executor::block_on(s.num_members()))
        .unwrap_or(0);
    println!(
        "Cluster formed with {} members discovered by seed",
        seed_members
    );

    // Print Plumtree peer topology for first few nodes
    println!("\nPlumtree peer topology (first 5 nodes):");
    for i in 0..5.min(config.num_users) {
        let node_guard = nodes[i].read().await;
        if let Some(ref stack) = node_guard.stack {
            let stats = stack.plumtree().peer_stats();
            println!(
                "  U{}: {} eager, {} lazy",
                i + 1,
                stats.eager_count,
                stats.lazy_count
            );
        }
    }

    // Create broadcast channels for sending messages
    let mut broadcast_txs = Vec::new();

    for i in 0..config.num_users {
        let node = nodes[i].clone();
        let (broadcast_tx, mut broadcast_rx) = mpsc::channel::<String>(100);
        broadcast_txs.push(broadcast_tx);

        tokio::spawn(async move {
            while let Some(text) = broadcast_rx.recv().await {
                let node_guard = node.read().await;
                if node_guard.is_online() {
                    if let Err(e) = node_guard.broadcast(&text).await {
                        eprintln!("Broadcast error: {}", e);
                    }
                }
            }
        });
    }

    // Create AppState using shared type
    let app_state = AppState::new(
        broadcast_txs,
        update_tx,
        delegates,
        nodes,
        prometheus_handle,
        config,
    );

    // Run shared server
    web_chat_common::run_server(app_state).await;
}
