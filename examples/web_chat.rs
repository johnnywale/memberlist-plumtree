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

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use bytes::{Buf, BufMut, Bytes};
use futures::{SinkExt, StreamExt};
use memberlist::net::NetTransportOptions;
use memberlist::tokio::{TokioRuntime, TokioSocketAddrResolver, TokioTcp};
use memberlist::{Memberlist, Options as MemberlistOptions};
use memberlist_plumtree::{
    decode_plumtree_envelope, ChannelTransport, IdCodec, MessageId, PeerStats, PeerTopology,
    PlumtreeConfig, PlumtreeDelegate, PlumtreeMemberlist, PlumtreeMessage,
};
use nodecraft::resolver::socket_addr::SocketAddrResolver;
use nodecraft::CheapClone;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, RwLock};
use tower_http::services::ServeDir;

/// Number of users in the chat
const NUM_USERS: usize = 50;

/// Base port for memberlist (each node uses BASE_PORT + node_idx)
const BASE_PORT: u16 = 17000;

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

// Required for memberlist wire protocol - implement memberlist_proto::Data
mod data_impl {
    use super::NodeId;
    use memberlist::proto::{Data, DataRef, DecodeError, EncodeError};
    use std::borrow::Cow;

    // DataRef implementation for decoding
    impl<'a> DataRef<'a, NodeId> for u32 {
        fn decode(buf: &'a [u8]) -> Result<(usize, Self), DecodeError> {
            if buf.len() < 4 {
                return Err(DecodeError::Custom(Cow::Borrowed("insufficient data for u32")));
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
type MemberlistTransport = memberlist::net::NetTransport<
    NodeId,
    TokioSocketAddrResolver,
    TokioTcp,
    TokioRuntime,
>;

/// The delegate type used by MemberlistStack.
type StackDelegate = memberlist_plumtree::PlumtreeNodeDelegate<
    NodeId,
    SocketAddr,
    memberlist::delegate::VoidDelegate<NodeId, SocketAddr>,
>;

/// The full MemberlistStack type.
type ChatStack = memberlist_plumtree::MemberlistStack<
    NodeId,
    Arc<ChatPlumtreeDelegate>,
    MemberlistTransport,
    StackDelegate,
>;

// ============================================================================
// Message Router for local nodes
// ============================================================================

/// Routes Plumtree messages between local nodes.
///
/// This is needed because `run_with_transport()` sends messages to `ChannelTransport`,
/// and we need to dispatch them to the target node's `incoming_sender()`.
struct MessageRouter {
    /// Map of node ID to their incoming message sender
    incoming_senders: Arc<RwLock<HashMap<NodeId, async_channel::Sender<(NodeId, PlumtreeMessage)>>>>,
}

impl MessageRouter {
    fn new() -> Self {
        Self {
            incoming_senders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn register(&self, node_id: NodeId, sender: async_channel::Sender<(NodeId, PlumtreeMessage)>) {
        self.incoming_senders.write().await.insert(node_id, sender);
    }

    async fn unregister(&self, node_id: &NodeId) {
        self.incoming_senders.write().await.remove(node_id);
    }

    /// Start a routing task that reads from a transport receiver and routes messages
    fn start_routing_task(
        &self,
        sender_id: NodeId,
        transport_rx: async_channel::Receiver<(NodeId, Bytes)>,
    ) -> tokio::task::JoinHandle<()> {
        let router = self.incoming_senders.clone();

        tokio::spawn(async move {
            while let Ok((target, data)) = transport_rx.recv().await {
                // Decode the envelope to get sender and message
                if let Some((_envelope_sender, message)) = decode_plumtree_envelope::<NodeId>(&data) {
                    let senders = router.read().await;
                    if let Some(tx) = senders.get(&target) {
                        let _ = tx.send((sender_id.clone(), message)).await;
                    }
                }
            }
        })
    }
}

// ============================================================================
// Protocol Types
// ============================================================================

/// Delivery method for a message
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum DeliveryMethod {
    Gossip { round: u32, forwarder: String },
    Graft { forwarder: String },
    SelfBroadcast,
}

impl DeliveryMethod {
    fn to_string(&self) -> String {
        match self {
            DeliveryMethod::Gossip { round, forwarder } => {
                format!("GOSSIP from {} (round {})", forwarder, round)
            }
            DeliveryMethod::Graft { forwarder } => {
                format!("GRAFT from {} (tree repair)", forwarder)
            }
            DeliveryMethod::SelfBroadcast => "LOCAL (self-broadcast)".to_string(),
        }
    }
}

/// Protocol event for the event log.
#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ProtocolEvent {
    MessageSent { to: Vec<String> },
    MessageReceived { from: String, method: String },
    GraftSent { to: String, msg_id: String },
    PruneSent { to: String },
    PeerPromoted { peer: String },
    PeerDemoted { peer: String },
    NodeOffline,
    NodeOnline,
    NodeJoined { peer: String },
    NodeLeft { peer: String },
}

impl ProtocolEvent {
    fn to_json(&self) -> Value {
        match self {
            ProtocolEvent::MessageSent { to } => {
                json!({ "type": "sent", "peers": to })
            }
            ProtocolEvent::MessageReceived { from, method } => {
                json!({ "type": "received", "from": from, "method": method })
            }
            ProtocolEvent::GraftSent { to, msg_id } => {
                json!({ "type": "graft", "to": to, "msg_id": &msg_id[..8.min(msg_id.len())] })
            }
            ProtocolEvent::PruneSent { to } => {
                json!({ "type": "prune", "to": to })
            }
            ProtocolEvent::PeerPromoted { peer } => {
                json!({ "type": "promote", "peer": peer })
            }
            ProtocolEvent::PeerDemoted { peer } => {
                json!({ "type": "demote", "peer": peer })
            }
            ProtocolEvent::NodeOffline => json!({ "type": "offline" }),
            ProtocolEvent::NodeOnline => json!({ "type": "online" }),
            ProtocolEvent::NodeJoined { peer } => {
                json!({ "type": "join", "peer": peer })
            }
            ProtocolEvent::NodeLeft { peer } => {
                json!({ "type": "leave", "peer": peer })
            }
        }
    }
}

/// Metrics counters for protocol events.
#[derive(Debug, Default)]
struct Metrics {
    messages_sent: AtomicUsize,
    messages_received: AtomicUsize,
    grafts_sent: AtomicUsize,
    prunes_sent: AtomicUsize,
    promotions: AtomicUsize,
    demotions: AtomicUsize,
    nodes_joined: AtomicUsize,
    nodes_left: AtomicUsize,
    #[allow(dead_code)]
    packets_dropped: AtomicU64,
}

impl Metrics {
    fn new() -> Self {
        Self::default()
    }

    fn reset(&self) {
        self.messages_sent.store(0, Ordering::SeqCst);
        self.messages_received.store(0, Ordering::SeqCst);
        self.grafts_sent.store(0, Ordering::SeqCst);
        self.prunes_sent.store(0, Ordering::SeqCst);
        self.promotions.store(0, Ordering::SeqCst);
        self.demotions.store(0, Ordering::SeqCst);
        self.nodes_joined.store(0, Ordering::SeqCst);
        self.nodes_left.store(0, Ordering::SeqCst);
        self.packets_dropped.store(0, Ordering::SeqCst);
    }

    fn to_json(&self) -> Value {
        json!({
            "sent": self.messages_sent.load(Ordering::SeqCst),
            "received": self.messages_received.load(Ordering::SeqCst),
            "grafts": self.grafts_sent.load(Ordering::SeqCst),
            "prunes": self.prunes_sent.load(Ordering::SeqCst),
            "promotions": self.promotions.load(Ordering::SeqCst),
            "demotions": self.demotions.load(Ordering::SeqCst),
            "joined": self.nodes_joined.load(Ordering::SeqCst),
            "left": self.nodes_left.load(Ordering::SeqCst)
        })
    }
}

/// Chat message received
#[derive(Debug, Clone)]
struct ReceivedMessage {
    from: String,
    text: String,
    timestamp: String,
    delivery: String,
    #[allow(dead_code)]
    message_id: MessageId,
}

impl ReceivedMessage {
    fn to_json(&self) -> Value {
        json!({
            "from": self.from,
            "text": self.text,
            "timestamp": self.timestamp,
            "delivery": self.delivery
        })
    }
}

/// Timestamped event
#[derive(Debug, Clone)]
struct TimestampedEvent {
    time: String,
    event: ProtocolEvent,
}

impl TimestampedEvent {
    fn to_json(&self) -> Value {
        let mut evt = self.event.to_json();
        if let Value::Object(ref mut map) = evt {
            map.insert("time".to_string(), json!(self.time));
        }
        evt
    }
}

/// Chat message payload.
#[derive(Debug, Clone)]
struct ChatMessage {
    from: String,
    text: String,
    timestamp: u64,
}

impl ChatMessage {
    fn new(from: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            text: text.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    fn encode(&self) -> Bytes {
        Bytes::from(format!("{}|{}|{}", self.from, self.timestamp, self.text))
    }

    fn decode(data: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(data).ok()?;
        let mut parts = s.splitn(3, '|');
        Some(Self {
            from: parts.next()?.to_string(),
            timestamp: parts.next()?.parse().ok()?,
            text: parts.next()?.to_string(),
        })
    }

    fn format_time(&self) -> String {
        let secs = (self.timestamp / 1000) % 86400;
        format!(
            "{:02}:{:02}:{:02}",
            (secs / 3600) % 24,
            (secs % 3600) / 60,
            secs % 60
        )
    }
}

// ============================================================================
// Plumtree Delegate
// ============================================================================

/// Plumtree delegate that handles message delivery.
struct ChatPlumtreeDelegate {
    node_name: String,
    messages: Arc<RwLock<Vec<ReceivedMessage>>>,
    events: Arc<RwLock<Vec<TimestampedEvent>>>,
    metrics: Arc<Metrics>,
    update_tx: broadcast::Sender<Value>,
    pending_context: Arc<RwLock<HashMap<MessageId, (String, u32)>>>,
    start_time: Instant,
    node_idx: usize,
    /// Reference to the PlumtreeMemberlist (set after construction)
    pm: Arc<RwLock<Option<Arc<PlumtreeMemberlist<NodeId, Arc<ChatPlumtreeDelegate>>>>>>,
}

impl ChatPlumtreeDelegate {
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
        }
    }

    /// Set the PlumtreeMemberlist reference (called after construction)
    async fn set_pm(&self, pm: Arc<PlumtreeMemberlist<NodeId, Arc<ChatPlumtreeDelegate>>>) {
        *self.pm.write().await = Some(pm);
    }

    /// Clear the PlumtreeMemberlist reference (called on shutdown)
    async fn clear_pm(&self) {
        *self.pm.write().await = None;
    }

    fn format_elapsed(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs();
        format!("{:02}:{:02}", elapsed / 60, elapsed % 60)
    }

    async fn get_peers_json(&self) -> Value {
        let topo = self.get_topology().await;
        let eager: Vec<String> = topo.eager.iter().map(|p| format!("U{}", p.0 + 1)).collect();
        let lazy: Vec<String> = topo.lazy.iter().map(|p| format!("U{}", p.0 + 1)).collect();
        json!({ "eager": eager, "lazy": lazy })
    }

    async fn get_topology(&self) -> PeerTopology<NodeId> {
        let pm_guard = self.pm.read().await;
        if let Some(ref pm) = *pm_guard {
            pm.peers().topology()
        } else {
            PeerTopology::default()
        }
    }

    #[allow(dead_code)]
    async fn get_peer_stats(&self) -> PeerStats {
        let pm_guard = self.pm.read().await;
        if let Some(ref pm) = *pm_guard {
            pm.peer_stats()
        } else {
            PeerStats {
                eager_count: 0,
                lazy_count: 0,
            }
        }
    }

    async fn add_event(&self, event: ProtocolEvent) {
        let timestamped = TimestampedEvent {
            time: self.format_elapsed(),
            event,
        };
        let mut evts = self.events.write().await;
        evts.push(timestamped);
        if evts.len() > 100 {
            evts.remove(0);
        }
    }
}

impl PlumtreeDelegate<NodeId> for ChatPlumtreeDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.metrics
            .messages_received
            .fetch_add(1, Ordering::SeqCst);

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

                // Get peer topology from PlumtreeMemberlist
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
        self.metrics.promotions.fetch_add(1, Ordering::SeqCst);
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
        self.metrics.demotions.fetch_add(1, Ordering::SeqCst);
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
        self.metrics.grafts_sent.fetch_add(1, Ordering::SeqCst);
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
        self.metrics.prunes_sent.fetch_add(1, Ordering::SeqCst);
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
    delegate: Arc<ChatPlumtreeDelegate>,
    /// The stack is created on start() and dropped on stop()
    stack: Option<ChatStack>,
    /// Handle for the Plumtree runner task
    runner_handle: Option<tokio::task::JoinHandle<()>>,
    /// Handle for the routing task
    routing_handle: Option<tokio::task::JoinHandle<()>>,
    /// Handle for the incoming processor task
    incoming_handle: Option<tokio::task::JoinHandle<()>>,
    is_online: bool,
    port: u16,
    seed_addr: Option<SocketAddr>,
    /// Shared message router
    router: Arc<MessageRouter>,
}

impl ChatNode {
    fn new(
        node_idx: usize,
        update_tx: broadcast::Sender<Value>,
        seed_addr: Option<SocketAddr>,
        router: Arc<MessageRouter>,
    ) -> Self {
        let node_id = NodeId(node_idx as u32);
        let node_name = format!("U{}", node_idx + 1);
        let delegate = Arc::new(ChatPlumtreeDelegate::new(node_idx, node_name, update_tx));
        let port = BASE_PORT + node_idx as u16;

        Self {
            node_idx,
            node_id,
            delegate,
            stack: None,
            runner_handle: None,
            routing_handle: None,
            incoming_handle: None,
            is_online: false,
            port,
            seed_addr,
            router,
        }
    }

    fn is_online(&self) -> bool {
        self.is_online
    }

    /// Start the node and join the cluster
    async fn start(&mut self) -> Result<(), String> {
        if self.is_online {
            return Ok(());
        }

        let node_id = self.node_id.clone();
        let bind_addr: SocketAddr = format!("127.0.0.1:{}", self.port)
            .parse()
            .map_err(|e| format!("Invalid address: {}", e))?;

        // Create PlumtreeConfig
        let config = PlumtreeConfig::default()
            .with_eager_fanout(2)
            .with_max_peers(6)
            .with_lazy_fanout(4)
            .with_ihave_interval(Duration::from_millis(100))
            .with_graft_timeout(Duration::from_millis(200))
            .with_message_cache_ttl(Duration::from_secs(60));

        // Create PlumtreeMemberlist
        let pm = Arc::new(PlumtreeMemberlist::new(
            node_id.clone(),
            config,
            self.delegate.clone(),
        ));

        // Set the PM reference in the delegate
        self.delegate.set_pm(pm.clone()).await;

        // Register with the message router
        self.router.register(node_id.clone(), pm.incoming_sender()).await;

        // Create ChannelTransport for Plumtree message routing
        let (transport, transport_rx) = ChannelTransport::<NodeId>::bounded(1000);

        // Start the routing task that dispatches messages to target nodes
        let routing_handle = self.router.start_routing_task(node_id.clone(), transport_rx);
        self.routing_handle = Some(routing_handle);

        // Start the Plumtree runner with the channel transport
        let pm_clone = pm.clone();
        let runner_handle = tokio::spawn(async move {
            pm_clone.run_with_transport(transport).await;
        });
        self.runner_handle = Some(runner_handle);

        // CRITICAL: Start the incoming processor to handle messages from routing task
        let pm_incoming = pm.clone();
        let incoming_handle = tokio::spawn(async move {
            pm_incoming.run_incoming_processor().await;
        });
        self.incoming_handle = Some(incoming_handle);

        // Wrap VoidDelegate with PlumtreeNodeDelegate for automatic peer sync
        let void_delegate = memberlist::delegate::VoidDelegate::<NodeId, SocketAddr>::default();
        let plumtree_delegate = pm.wrap_delegate(void_delegate);

        // Create NetTransport options
        let mut transport_opts = NetTransportOptions::<
            NodeId,
            SocketAddrResolver<TokioRuntime>,
            TokioTcp,
        >::new(node_id.clone());
        transport_opts.add_bind_address(bind_addr);

        // Create Memberlist options (use local() preset for fast testing)
        let memberlist_opts = MemberlistOptions::local()
            .with_probe_interval(Duration::from_millis(500))
            .with_gossip_interval(Duration::from_millis(200));

        // Create Memberlist with the delegate
        let memberlist = Memberlist::with_delegate(plumtree_delegate, transport_opts, memberlist_opts)
            .await
            .map_err(|e| format!("Failed to create memberlist: {}", e))?;

        let advertise_addr = *memberlist.advertise_address();

        // Create MemberlistStack from components
        let stack = memberlist_plumtree::MemberlistStack::new(pm, memberlist, advertise_addr);

        // Join cluster if we have a seed
        if let Some(seed) = self.seed_addr {
            stack
                .join(&[seed])
                .await
                .map_err(|e| format!("Failed to join cluster: {}", e))?;
        }

        self.stack = Some(stack);
        self.is_online = true;
        self.delegate.add_event(ProtocolEvent::NodeOnline).await;

        Ok(())
    }

    /// Stop the node (disconnect from cluster)
    async fn stop(&mut self) {
        if !self.is_online {
            return;
        }

        // Unregister from router
        self.router.unregister(&self.node_id).await;

        // Abort the runner, routing, and incoming tasks
        if let Some(handle) = self.runner_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.routing_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.incoming_handle.take() {
            handle.abort();
        }

        if let Some(stack) = self.stack.take() {
            let _ = stack.leave(Duration::from_secs(1)).await;
            let _ = stack.shutdown().await;
        }

        self.delegate.clear_pm().await;
        self.is_online = false;
        self.delegate.add_event(ProtocolEvent::NodeOffline).await;
    }

    async fn broadcast(&self, text: &str) -> Result<MessageId, memberlist_plumtree::Error> {
        if !self.is_online {
            return Err(memberlist_plumtree::Error::Shutdown);
        }

        let stack = self.stack.as_ref().ok_or(memberlist_plumtree::Error::Shutdown)?;
        let msg = ChatMessage::new(format!("U{}", self.node_idx + 1), text);
        let payload = msg.encode();
        self.delegate
            .metrics
            .messages_sent
            .fetch_add(1, Ordering::SeqCst);
        let msg_id = stack.broadcast(payload.clone()).await?;
        self.delegate.on_deliver(msg_id, payload);
        Ok(msg_id)
    }

    async fn promote_all_to_eager(&self) {
        if let Some(ref stack) = self.stack {
            let topology = stack.plumtree().peers().topology();
            for peer in topology.eager.iter().chain(topology.lazy.iter()) {
                stack.plumtree().peers().promote_to_eager(peer);
            }
        }
    }

    #[allow(dead_code)]
    fn memberlist_addr(&self) -> SocketAddr {
        format!("127.0.0.1:{}", self.port).parse().unwrap()
    }
}

// ============================================================================
// Application State
// ============================================================================

/// Application state
#[derive(Clone)]
struct AppState {
    broadcast_txs: Arc<Vec<mpsc::Sender<String>>>,
    control_txs: Arc<Vec<mpsc::Sender<NodeCommand>>>,
    update_tx: broadcast::Sender<Value>,
    delegates: Arc<Vec<Arc<ChatPlumtreeDelegate>>>,
    nodes: Arc<Vec<Arc<RwLock<ChatNode>>>>,
}

#[derive(Debug, Clone)]
enum NodeCommand {
    PromoteAllEager,
    GoOffline,
    GoOnline,
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut update_rx = state.update_tx.subscribe();

    // Send initial state for all nodes
    for (i, delegate) in state.delegates.iter().enumerate() {
        let messages: Vec<_> = delegate
            .messages
            .read()
            .await
            .iter()
            .map(|m| m.to_json())
            .collect();
        let events: Vec<_> = delegate
            .events
            .read()
            .await
            .iter()
            .map(|e| e.to_json())
            .collect();
        let peers_json = delegate.get_peers_json().await;
        let online = state.nodes[i].read().await.is_online();

        let init = json!({
            "type": "init",
            "node": i,
            "messages": messages,
            "events": events,
            "metrics": delegate.metrics.to_json(),
            "peers": peers_json,
            "online": online
        });
        if sender
            .send(Message::Text(init.to_string().into()))
            .await
            .is_err()
        {
            return;
        }
    }

    // Spawn task to forward updates to WebSocket
    let send_task = tokio::spawn(async move {
        while let Ok(update) = update_rx.recv().await {
            if sender
                .send(Message::Text(update.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Handle incoming messages
    let state_clone = state.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                    handle_ws_message(&state_clone, &data).await;
                }
            }
        }
    });

    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }
}

async fn handle_ws_message(state: &AppState, data: &Value) {
    match data["type"].as_str().unwrap_or("") {
        "send" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            let text = data["text"].as_str().unwrap_or("");
            if user < state.broadcast_txs.len() && !text.is_empty() {
                let _ = state.broadcast_txs[user].send(text.to_string()).await;
            }
        }
        "toggle_online" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.nodes.len() {
                let current = state.nodes[user].read().await.is_online();
                let cmd = if current {
                    NodeCommand::GoOffline
                } else {
                    NodeCommand::GoOnline
                };
                let _ = state.control_txs[user].send(cmd).await;
            }
        }
        "promote_eager" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.control_txs.len() {
                let _ = state.control_txs[user]
                    .send(NodeCommand::PromoteAllEager)
                    .await;
            }
        }
        "reset_metrics" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.delegates.len() {
                state.delegates[user].metrics.reset();
                let _ = state.update_tx.send(json!({
                    "type": "metrics",
                    "node": user,
                    "metrics": state.delegates[user].metrics.to_json()
                }));
            }
        }
        _ => {}
    }
}

// ============================================================================
// REST API Handlers
// ============================================================================

/// GET /api/config - Returns configuration
async fn api_config() -> Json<Value> {
    Json(json!({
        "num_users": NUM_USERS,
        "version": "3.0.0",
        "transport": "MemberlistStack with real NetTransport + ChannelTransport routing"
    }))
}

/// GET /api/status - Returns status for all nodes
async fn api_status(State(state): State<AppState>) -> Json<Value> {
    let mut nodes = Vec::new();

    for (i, delegate) in state.delegates.iter().enumerate() {
        let peers_json = delegate.get_peers_json().await;
        let metrics = delegate.metrics.to_json();
        let online = state.nodes[i].read().await.is_online();

        nodes.push(json!({
            "id": i,
            "name": format!("U{}", i + 1),
            "online": online,
            "peers": peers_json,
            "metrics": metrics
        }));
    }

    Json(json!({
        "num_users": NUM_USERS,
        "nodes": nodes
    }))
}

/// GET /api/node/:id - Returns detailed state for a specific node
async fn api_node(Path(node_id): Path<usize>, State(state): State<AppState>) -> Json<Value> {
    if node_id >= state.delegates.len() {
        return Json(json!({
            "error": "Node not found",
            "node_id": node_id
        }));
    }

    let delegate = &state.delegates[node_id];
    let peers_json = delegate.get_peers_json().await;
    let metrics = delegate.metrics.to_json();
    let online = state.nodes[node_id].read().await.is_online();

    let messages: Vec<_> = delegate
        .messages
        .read()
        .await
        .iter()
        .map(|m| m.to_json())
        .collect();

    let events: Vec<_> = delegate
        .events
        .read()
        .await
        .iter()
        .map(|e| e.to_json())
        .collect();

    Json(json!({
        "id": node_id,
        "name": format!("U{}", node_id + 1),
        "online": online,
        "peers": peers_json,
        "metrics": metrics,
        "messages": messages,
        "events": events
    }))
}

/// GET /api/node/:id/peers - Returns peer tree for a specific node
async fn api_node_peers(Path(node_id): Path<usize>, State(state): State<AppState>) -> Json<Value> {
    if node_id >= state.delegates.len() {
        return Json(json!({
            "error": "Node not found",
            "node_id": node_id
        }));
    }

    let delegate = &state.delegates[node_id];
    let peers_json = delegate.get_peers_json().await;
    let online = state.nodes[node_id].read().await.is_online();

    let mut eager_with_status = Vec::new();
    let mut lazy_with_status = Vec::new();

    if let Some(eager_arr) = peers_json.get("eager").and_then(|v| v.as_array()) {
        for peer_name in eager_arr {
            if let Some(name) = peer_name.as_str() {
                eager_with_status.push(json!({
                    "name": name,
                    "online": true
                }));
            }
        }
    }

    if let Some(lazy_arr) = peers_json.get("lazy").and_then(|v| v.as_array()) {
        for peer_name in lazy_arr {
            if let Some(name) = peer_name.as_str() {
                lazy_with_status.push(json!({
                    "name": name,
                    "online": true
                }));
            }
        }
    }

    Json(json!({
        "id": node_id,
        "name": format!("U{}", node_id + 1),
        "online": online,
        "eager": eager_with_status,
        "lazy": lazy_with_status,
        "eager_count": eager_with_status.len(),
        "lazy_count": lazy_with_status.len()
    }))
}

#[tokio::main]
async fn main() {
    println!("Starting Plumtree Web Chat Demo with MemberlistStack...");
    println!("Using {} nodes with REAL networking on ports {}-{}",
        NUM_USERS, BASE_PORT, BASE_PORT + NUM_USERS as u16 - 1);
    println!();

    // Create broadcast channel for WebSocket updates
    let (update_tx, _) = broadcast::channel::<Value>(1000);

    // Create shared message router
    let router = Arc::new(MessageRouter::new());

    // Seed address is node 0
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", BASE_PORT).parse().unwrap();

    // Create nodes
    let mut nodes: Vec<Arc<RwLock<ChatNode>>> = Vec::new();
    let mut delegates = Vec::new();

    for i in 0..NUM_USERS {
        // First node (seed) has no seed_addr, others join via node 0
        let node_seed = if i == 0 { None } else { Some(seed_addr) };
        let node = ChatNode::new(i, update_tx.clone(), node_seed, router.clone());
        delegates.push(node.delegate.clone());
        nodes.push(Arc::new(RwLock::new(node)));
    }

    // Start seed node first
    println!("Starting seed node U1 on port {}...", BASE_PORT);
    if let Err(e) = nodes[0].write().await.start().await {
        eprintln!("Failed to start seed node: {}", e);
        return;
    }

    // Give seed node time to bind
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Start remaining nodes (they will join the cluster)
    for i in 1..NUM_USERS {
        print!("Starting node U{}...", i + 1);
        match nodes[i].write().await.start().await {
            Ok(_) => println!(" OK"),
            Err(e) => println!(" FAILED: {}", e),
        }
        // Small delay to avoid overwhelming the cluster
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    println!("\nAll {} nodes started!", NUM_USERS);
    println!("Waiting for SWIM gossip to propagate...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Print cluster status
    let seed_members = nodes[0].read().await.stack.as_ref()
        .map(|s| futures::executor::block_on(s.num_members()))
        .unwrap_or(0);
    println!("Cluster formed with {} members discovered by seed", seed_members);

    // Print Plumtree peer topology for first few nodes
    println!("\nPlumtree peer topology (first 5 nodes):");
    for i in 0..5.min(NUM_USERS) {
        let node_guard = nodes[i].read().await;
        if let Some(ref stack) = node_guard.stack {
            let stats = stack.plumtree().peer_stats();
            println!("  U{}: {} eager, {} lazy", i + 1, stats.eager_count, stats.lazy_count);
        }
    }

    // Create control channels
    let mut broadcast_txs = Vec::new();
    let mut control_txs = Vec::new();

    for i in 0..NUM_USERS {
        let node = nodes[i].clone();
        let (broadcast_tx, mut broadcast_rx) = mpsc::channel::<String>(100);
        let (control_tx, mut control_rx) = mpsc::channel::<NodeCommand>(100);
        broadcast_txs.push(broadcast_tx);
        control_txs.push(control_tx);

        let update_tx_clone = update_tx.clone();

        // Message handling task
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(text) = broadcast_rx.recv() => {
                        let node_guard = node.read().await;
                        if node_guard.is_online() {
                            if let Err(e) = node_guard.broadcast(&text).await {
                                eprintln!("Broadcast error: {}", e);
                            }
                        }
                    }
                    Some(cmd) = control_rx.recv() => {
                        match cmd {
                            NodeCommand::PromoteAllEager => {
                                let node_guard = node.read().await;
                                node_guard.promote_all_to_eager().await;
                            }
                            NodeCommand::GoOffline => {
                                let mut node_guard = node.write().await;
                                node_guard.stop().await;
                                let peers_json = node_guard.delegate.get_peers_json().await;
                                let _ = update_tx_clone.send(json!({
                                    "type": "state",
                                    "node": node_guard.node_idx,
                                    "online": false,
                                    "peers": peers_json
                                }));
                            }
                            NodeCommand::GoOnline => {
                                let mut node_guard = node.write().await;
                                if let Err(e) = node_guard.start().await {
                                    eprintln!("Failed to start node: {}", e);
                                }
                                let peers_json = node_guard.delegate.get_peers_json().await;
                                let _ = update_tx_clone.send(json!({
                                    "type": "state",
                                    "node": node_guard.node_idx,
                                    "online": true,
                                    "peers": peers_json
                                }));
                            }
                        }
                    }
                    else => break,
                }
            }
        });
    }

    let app_state = AppState {
        broadcast_txs: Arc::new(broadcast_txs),
        control_txs: Arc::new(control_txs),
        update_tx,
        delegates: Arc::new(delegates),
        nodes: Arc::new(nodes),
    };

    // Find the static directory
    let static_dir = find_static_dir();
    println!("\nServing static files from: {:?}", static_dir);

    // Build router
    let app = Router::new()
        .route("/api/config", get(api_config))
        .route("/api/status", get(api_status))
        .route("/api/node/:id", get(api_node))
        .route("/api/node/:id/peers", get(api_node_peers))
        .route("/ws", get(ws_handler))
        .nest_service("/", ServeDir::new(&static_dir))
        .with_state(app_state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("\nServer running on http://localhost:3000");
    println!("\nKey features demonstrated:");
    println!("  - MemberlistStack for real SWIM + Plumtree integration");
    println!("  - Real UDP/TCP networking on localhost (ports {}-{})", BASE_PORT, BASE_PORT + NUM_USERS as u16 - 1);
    println!("  - Automatic peer discovery via SWIM gossip");
    println!("  - ChannelTransport routing for Plumtree messages between local nodes");
    println!("  - Online/offline toggle with proper shutdown/restart");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

fn find_static_dir() -> PathBuf {
    let candidates = [
        PathBuf::from("examples/static"),
        PathBuf::from("./static"),
        PathBuf::from("../examples/static"),
    ];

    for path in &candidates {
        if path.exists() && path.join("index.html").exists() {
            return path.clone();
        }
    }

    PathBuf::from("examples/static")
}
