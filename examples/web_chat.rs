#![allow(clippy::type_complexity)]
#![allow(clippy::needless_range_loop)]

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
    http::header,
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
    storage::MemoryStore, CompressionConfig, IdCodec, MessageId, PeerHealthConfig,
    PeerHealthTracker, PeerStats, PeerStatus, PeerTopology, PlumtreeBridge, PlumtreeConfig,
    PlumtreeDelegate, PlumtreeDiscovery, PlumtreeNodeDelegate, SyncConfig,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use nodecraft::resolver::socket_addr::SocketAddrResolver;
use nodecraft::CheapClone;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, Ordering},
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
    Arc<ChatPlumtreeDelegate>,
    MemberlistTransport,
    StackDelegate,
>;

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

impl std::fmt::Display for DeliveryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryMethod::Gossip { round, forwarder } => {
                write!(f, "GOSSIP from {} (round {})", forwarder, round)
            }
            DeliveryMethod::Graft { forwarder } => {
                write!(f, "GRAFT from {} (tree repair)", forwarder)
            }
            DeliveryMethod::SelfBroadcast => write!(f, "LOCAL (self-broadcast)"),
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
/// Metrics reader that extracts values from Prometheus output.
///
/// Instead of manually tracking counters, this reads from the system metrics
/// recorded by the library via the `metrics` crate. The library records global
/// metrics (aggregated across all nodes in the process).
#[derive(Debug)]
struct Metrics {
    // Peer health gauges (use AtomicI64 to support decrement safely)
    peers_healthy: AtomicI64,
    peers_degraded: AtomicI64,
    peers_zombie: AtomicI64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            peers_healthy: AtomicI64::new(0),
            peers_degraded: AtomicI64::new(0),
            peers_zombie: AtomicI64::new(0),
        }
    }

    /// Increment healthy peer count
    fn inc_healthy(&self) {
        self.peers_healthy.fetch_add(1, Ordering::SeqCst);
        #[cfg(feature = "metrics")]
        memberlist_plumtree::metrics::inc_peers_healthy();
    }

    /// Decrement healthy peer count
    fn dec_healthy(&self) {
        self.peers_healthy.fetch_sub(1, Ordering::SeqCst);
        #[cfg(feature = "metrics")]
        memberlist_plumtree::metrics::dec_peers_healthy();
    }

    /// Increment degraded peer count
    fn inc_degraded(&self) {
        self.peers_degraded.fetch_add(1, Ordering::SeqCst);
        #[cfg(feature = "metrics")]
        memberlist_plumtree::metrics::inc_peers_degraded();
    }

    /// Decrement degraded peer count
    fn dec_degraded(&self) {
        self.peers_degraded.fetch_sub(1, Ordering::SeqCst);
        #[cfg(feature = "metrics")]
        memberlist_plumtree::metrics::dec_peers_degraded();
    }

    /// Increment zombie peer count
    fn inc_zombie(&self) {
        self.peers_zombie.fetch_add(1, Ordering::SeqCst);
        #[cfg(feature = "metrics")]
        memberlist_plumtree::metrics::inc_peers_zombie();
    }

    /// Decrement zombie peer count
    fn dec_zombie(&self) {
        self.peers_zombie.fetch_sub(1, Ordering::SeqCst);
        #[cfg(feature = "metrics")]
        memberlist_plumtree::metrics::dec_peers_zombie();
    }

    /// Transition a peer from one health state to another.
    #[allow(dead_code)]
    fn transition_peer_health(&self, from: Option<PeerStatus>, to: PeerStatus) {
        if let Some(old) = from {
            match old {
                PeerStatus::Healthy => self.dec_healthy(),
                PeerStatus::Degraded => self.dec_degraded(),
                PeerStatus::Zombie => self.dec_zombie(),
            }
        }
        match to {
            PeerStatus::Healthy => self.inc_healthy(),
            PeerStatus::Degraded => self.inc_degraded(),
            PeerStatus::Zombie => self.inc_zombie(),
        }
    }

    /// Remove a peer (decrement its current health state)
    #[allow(dead_code)]
    fn remove_peer(&self, current: PeerStatus) {
        match current {
            PeerStatus::Healthy => self.dec_healthy(),
            PeerStatus::Degraded => self.dec_degraded(),
            PeerStatus::Zombie => self.dec_zombie(),
        }
    }

    /// Extract metrics from Prometheus text output.
    ///
    /// Note: The library records global metrics (without node labels), so metrics
    /// are aggregated across all nodes in the process. This is appropriate for
    /// the demo where we show total cluster activity.
    fn to_json_from_prometheus(&self, prometheus_text: &str) -> Value {
        // Helper to extract a metric value (global metrics without labels)
        let get_metric = |name: &str| -> u64 {
            for line in prometheus_text.lines() {
                if line.starts_with('#') || line.is_empty() {
                    continue;
                }
                // Look for lines like: metric_name 123 (global, no labels)
                // or metric_name{} 123
                if line.starts_with(name) {
                    // Check if this is a global metric (no labels or empty labels)
                    let has_labels = line.contains('{') && !line.contains("{}");
                    if !has_labels {
                        if let Some(value_str) = line.split_whitespace().last() {
                            if let Ok(v) = value_str.parse::<f64>() {
                                return v as u64;
                            }
                        }
                    }
                }
            }
            0
        };

        // Read counters from Prometheus
        let sent = get_metric("plumtree_messages_broadcast_total");
        let received = get_metric("plumtree_messages_delivered_total");
        let grafts = get_metric("plumtree_graft_sent_total");
        let prunes = get_metric("plumtree_prune_sent_total");
        let promotions = get_metric("plumtree_peer_promotions_total");
        let demotions = get_metric("plumtree_peer_demotions_total");
        let joined = get_metric("plumtree_peer_added_total");
        let left = get_metric("plumtree_peer_removed_total");

        // Compression metrics
        let bytes_in = get_metric("plumtree_compression_bytes_in_total");
        let bytes_saved = get_metric("plumtree_compression_bytes_saved_total");

        // Priority queue gauges (from library metrics)
        let priority_critical = get_metric("plumtree_priority_critical");
        let priority_high = get_metric("plumtree_priority_high");
        let priority_normal = get_metric("plumtree_priority_normal");
        let priority_low = get_metric("plumtree_priority_low");

        // Peer health gauges (from library metrics)
        let peers_healthy = get_metric("plumtree_peers_healthy");
        let peers_degraded = get_metric("plumtree_peers_degraded");
        let peers_zombie = get_metric("plumtree_peers_zombie");

        json!({
            "sent": sent,
            "received": received,
            "grafts": grafts,
            "prunes": prunes,
            "promotions": promotions,
            "demotions": demotions,
            "joined": joined,
            "left": left,
            // Phase 1 metrics
            "compression": {
                "bytes_in": bytes_in,
                "bytes_saved": bytes_saved,
            },
            "priority_queue": {
                "critical": priority_critical,
                "high": priority_high,
                "normal": priority_normal,
                "low": priority_low,
            },
            "peer_health": {
                "healthy": peers_healthy,
                "degraded": peers_degraded,
                "zombie": peers_zombie,
            }
        })
    }

    /// Legacy method for compatibility - returns only peer health (local state)
    /// Use to_json_from_prometheus() with the Prometheus handle instead
    fn to_json(&self) -> Value {
        json!({
            "sent": 0,
            "received": 0,
            "grafts": 0,
            "prunes": 0,
            "promotions": 0,
            "demotions": 0,
            "joined": 0,
            "left": 0,
            "compression": {
                "bytes_in": 0,
                "bytes_saved": 0,
            },
            "priority_queue": {
                "critical": 0,
                "high": 0,
                "normal": 0,
                "low": 0,
            },
            "peer_health": {
                "healthy": self.peers_healthy.load(Ordering::SeqCst).max(0),
                "degraded": self.peers_degraded.load(Ordering::SeqCst).max(0),
                "zombie": self.peers_zombie.load(Ordering::SeqCst).max(0),
            }
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
    /// Reference to the PlumtreeDiscovery (set after construction)
    pm: Arc<RwLock<Option<Arc<PlumtreeDiscovery<NodeId, Arc<ChatPlumtreeDelegate>>>>>>,
    /// Peer health tracker for Phase 1 features
    #[allow(dead_code)]
    peer_health: Arc<PeerHealthTracker<NodeId>>,
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
            peer_health: Arc::new(PeerHealthTracker::new(PeerHealthConfig::default())),
        }
    }

    /// Set the PlumtreeDiscovery reference (called after construction)
    async fn set_pm(&self, pm: Arc<PlumtreeDiscovery<NodeId, Arc<ChatPlumtreeDelegate>>>) {
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
        // Message delivery is recorded by the library and read from Prometheus
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

                // Get peer topology from PlumtreeDiscovery
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
        // Metrics are recorded by the library and read from Prometheus
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
        // Metrics are recorded by the library and read from Prometheus
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
        // Metrics are recorded by the library and read from Prometheus
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
        // Metrics are recorded by the library and read from Prometheus
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
            is_online: false,
            port,
            seed_addr,
            message_store: Arc::new(MemoryStore::new(10_000)),
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
        //
        // NOTE on GRAFT visibility:
        // GRAFTs are triggered when a node receives IHave for an unknown message.
        // With ring topology (protect_ring_neighbors=true), all nodes are connected
        // via a linear eager chain, so GOSSIP reaches all nodes before IHave arrives.
        // To see GRAFTs in action:
        // 1. Toggle some nodes offline to break the eager chain
        // 2. Use the "Reset Eager" button to demote all peers to lazy
        // 3. Reduce max_peers to create sparse connectivity
        //
        // The current config uses small ihave_interval to increase GRAFT chances.
        // Configure compression - use zstd level 3 with 64 byte threshold for demo
        #[cfg(feature = "compression")]
        let compression_config = CompressionConfig::zstd(3).with_min_size(64);
        #[cfg(not(feature = "compression"))]
        let compression_config = CompressionConfig::default();

        let config = PlumtreeConfig::default()
            .with_eager_fanout(2)
            .with_max_peers(6) // Sparse connectivity for GRAFT demos
            .with_lazy_fanout(3)
            .with_ihave_interval(Duration::from_millis(10)) // Fast IHave for GRAFT demos
            .with_graft_timeout(Duration::from_millis(100)) // Retry timeout
            .with_message_cache_ttl(Duration::from_secs(60))
            // Disable ring protection for more random topology
            .with_hash_ring(true) // No ring-based eager selection
            .with_protect_ring_neighbors(true) // Allow any peer to be demoted
            .with_max_eager_peers(3) // Hard cap on eager peers
            .with_max_lazy_peers(8) // Hard cap on lazy peers
            // Enable sync for message recovery after node restarts
            .with_sync(
                SyncConfig::enabled()
                    .with_sync_interval(Duration::from_secs(5)) // Sync every 5s for demo
                    .with_sync_window(Duration::from_secs(120)), // 2 min window
            )
            // Enable compression for bandwidth savings
            .with_compression(compression_config);

        // Create PlumtreeDiscovery with persistent storage for sync recovery
        let pm = Arc::new(PlumtreeDiscovery::with_storage(
            node_id.clone(),
            config,
            self.delegate.clone(),
            self.message_store.clone(),
        ));

        // Set the PM reference in the delegate
        self.delegate.set_pm(pm.clone()).await;

        // Wrap VoidDelegate with PlumtreeNodeDelegate for automatic peer sync
        let void_delegate = memberlist::delegate::VoidDelegate::<NodeId, SocketAddr>::default();

        // Set up callbacks for rebalance events to notify the UI
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
            // Clone delegate and peer for the spawned task
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

        // Create NetTransport options
        let mut transport_opts =
            NetTransportOptions::<NodeId, SocketAddrResolver<TokioRuntime>, TokioTcp>::new(
                node_id.clone(),
            );
        transport_opts.add_bind_address(bind_addr);

        // Create Memberlist options (use local() preset for fast testing)
        let memberlist_opts = MemberlistOptions::local()
            .with_probe_interval(Duration::from_millis(500))
            .with_gossip_interval(Duration::from_millis(200));

        // Create Memberlist with the delegate
        let memberlist =
            Memberlist::with_delegate(plumtree_delegate, transport_opts, memberlist_opts)
                .await
                .map_err(|e| format!("Failed to create memberlist: {}", e))?;

        let advertise_addr = *memberlist.advertise_address();
        let bridge = PlumtreeBridge::new(pm);
        // Create MemberlistStack from components
        let stack = memberlist_plumtree::MemberlistStack::new(bridge, memberlist, advertise_addr);

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

        // Shutdown the stack (which handles PlumtreeDiscovery and Memberlist)
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

        let stack = self
            .stack
            .as_ref()
            .ok_or(memberlist_plumtree::Error::Shutdown)?;

        // Create chat message
        let msg = ChatMessage::new(format!("U{}", self.node_idx + 1), text);
        let payload = msg.encode();

        // Broadcast - compression is handled automatically inside PlumtreeDiscovery
        let msg_id = stack.broadcast(payload.clone()).await?;

        // Deliver to self (for UI display)
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

    /// Demote all eager peers to lazy.
    /// This is useful for demonstrating GRAFT flow - when all peers are lazy,
    /// the next message will trigger IHave→GRAFT→Gossip instead of direct Gossip.
    async fn demote_all_to_lazy(&self) {
        if let Some(ref stack) = self.stack {
            let topology = stack.plumtree().peers().topology();
            for peer in topology.eager.clone() {
                stack.plumtree().peers().demote_to_lazy(&peer);
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
    update_tx: broadcast::Sender<Value>,
    delegates: Arc<Vec<Arc<ChatPlumtreeDelegate>>>,
    nodes: Arc<Vec<Arc<RwLock<ChatNode>>>>,
    prometheus_handle: PrometheusHandle,
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
        if sender.send(Message::Text(init.to_string())).await.is_err() {
            return;
        }
    }

    // Spawn task to forward updates to WebSocket
    let send_task = tokio::spawn(async move {
        while let Ok(update) = update_rx.recv().await {
            if sender
                .send(Message::Text(update.to_string()))
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
                let mut node_guard = state.nodes[user].write().await;
                let is_online = node_guard.is_online();
                if is_online {
                    node_guard.stop().await;
                } else if let Err(e) = node_guard.start().await {
                    eprintln!("Failed to start node: {}", e);
                }
                let peers_json = node_guard.delegate.get_peers_json().await;
                let _ = state.update_tx.send(json!({
                    "type": "state",
                    "node": user,
                    "online": !is_online,
                    "peers": peers_json
                }));
            }
        }
        "promote_eager" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.nodes.len() {
                let node_guard = state.nodes[user].read().await;
                node_guard.promote_all_to_eager().await;
            }
        }
        "demote_lazy" => {
            // Demote all eager peers to lazy - useful for testing GRAFT flow
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.nodes.len() {
                let node_guard = state.nodes[user].read().await;
                node_guard.demote_all_to_lazy().await;
            }
        }
        "reset_metrics" => {
            // Prometheus metrics cannot be reset - just send current values
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.delegates.len() {
                let prometheus_text = state.prometheus_handle.render();
                let _ = state.update_tx.send(json!({
                    "type": "metrics",
                    "node": user,
                    "metrics": state.delegates[user].metrics.to_json_from_prometheus(&prometheus_text)
                }));
            }
        }
        "list_messages" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            let start_ts = data["start"].as_u64().unwrap_or(0);
            let end_ts = data["end"].as_u64().unwrap_or(u64::MAX);
            let limit = data["limit"].as_u64().unwrap_or(20).min(100) as usize;
            let offset = data["offset"].as_u64().unwrap_or(0) as usize;

            if user < state.nodes.len() {
                let node_guard = state.nodes[user].read().await;
                if let Some(ref stack) = node_guard.stack {
                    match stack
                        .plumtree()
                        .list_messages(start_ts, end_ts, limit, offset)
                        .await
                    {
                        Ok(page) => {
                            let messages: Vec<Value> = page
                                .messages
                                .iter()
                                .filter_map(|stored| {
                                    ChatMessage::decode(&stored.payload).map(|chat| {
                                        json!({
                                            "id": format!("{:?}", stored.id),
                                            "from": chat.from,
                                            "text": chat.text,
                                            "timestamp": chat.format_time(),
                                            "timestamp_ms": stored.timestamp,
                                        })
                                    })
                                })
                                .collect();

                            let _ = state.update_tx.send(json!({
                                "type": "stored_messages",
                                "node": user,
                                "messages": messages,
                                "has_more": page.has_more,
                                "total": page.total,
                                "pagination": { "limit": limit, "offset": offset }
                            }));
                        }
                        Err(e) => {
                            eprintln!("Failed to list messages: {}", e);
                        }
                    }
                }
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

/// GET /metrics - Prometheus scrape endpoint (text format)
async fn api_prometheus_metrics(State(state): State<AppState>) -> impl IntoResponse {
    let metrics = state.prometheus_handle.render();
    (
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        metrics,
    )
}

/// GET /api/metrics - JSON metrics for Web UI
async fn api_metrics_json(State(state): State<AppState>) -> Json<Value> {
    let prometheus_text = state.prometheus_handle.render();
    let mut metrics = parse_prometheus_metrics(&prometheus_text);

    // Aggregate gauge metrics from all nodes (the global gauges fluctuate with 50 nodes)
    let mut total_eager: usize = 0;
    let mut total_lazy: usize = 0;
    let mut total_cache: usize = 0;
    let mut total_seen_map: usize = 0;

    for node in state.nodes.iter() {
        let node_guard = node.read().await;
        if let Some(ref stack) = node_guard.stack {
            let peer_stats = stack.peer_stats();
            total_eager += peer_stats.eager_count;
            total_lazy += peer_stats.lazy_count;

            let cache_stats = stack.cache_stats();
            total_cache += cache_stats.entries;

            if let Some(seen_stats) = stack.seen_map_stats() {
                total_seen_map += seen_stats.size;
            }
        }
    }

    // Override the fluctuating global gauges with aggregated values
    if let Some(gauges) = metrics.get_mut("gauges").and_then(|v| v.as_object_mut()) {
        gauges.insert("plumtree_eager_peers".to_string(), json!(total_eager));
        gauges.insert("plumtree_lazy_peers".to_string(), json!(total_lazy));
        gauges.insert(
            "plumtree_total_peers".to_string(),
            json!(total_eager + total_lazy),
        );
        gauges.insert("plumtree_cache_size".to_string(), json!(total_cache));
        gauges.insert("plumtree_seen_map_size".to_string(), json!(total_seen_map));
    }

    Json(metrics)
}

/// Parse Prometheus text format into JSON
fn parse_prometheus_metrics(text: &str) -> Value {
    let mut counters = serde_json::Map::new();
    let mut gauges = serde_json::Map::new();
    let mut histograms = serde_json::Map::new();

    for line in text.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse "metric_name value" or "metric_name{labels} value"
        if let Some((name, value)) = parse_metric_line(line) {
            // Categorize by metric name patterns
            if name.contains("_total") {
                counters.insert(name, json!(value));
            } else if name.contains("_bucket") || name.contains("_sum") || name.contains("_count") {
                // Histogram components
                histograms.insert(name, json!(value));
            } else {
                gauges.insert(name, json!(value));
            }
        }
    }

    json!({
        "counters": counters,
        "gauges": gauges,
        "histograms": histograms
    })
}

/// Parse a single Prometheus metric line
fn parse_metric_line(line: &str) -> Option<(String, f64)> {
    // Handle metric lines like:
    // "metric_name 123"
    // "metric_name{label="value"} 123"
    let line = line.trim();

    // Find where the value starts (last space-separated token)
    let last_space = line.rfind(' ')?;
    let value_str = &line[last_space + 1..];
    let name_part = &line[..last_space];

    // Parse the value
    let value: f64 = value_str.parse().ok()?;

    // Extract metric name (without labels for simplicity, or with labels for uniqueness)
    // For base metric name: remove everything after '{'
    let name = if let Some(brace_pos) = name_part.find('{') {
        // Keep labels for histogram buckets to differentiate them
        if name_part.contains("_bucket") {
            name_part.to_string()
        } else {
            name_part[..brace_pos].to_string()
        }
    } else {
        name_part.to_string()
    };

    Some((name, value))
}

#[tokio::main]
async fn main() {
    println!("Starting Plumtree Web Chat Demo with MemberlistStack...");
    println!(
        "Using {} nodes with REAL networking on ports {}-{}",
        NUM_USERS,
        BASE_PORT,
        BASE_PORT + NUM_USERS as u16 - 1
    );
    println!();

    // Initialize Prometheus metrics recorder (must be done before any metrics are recorded)
    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    println!("Prometheus metrics recorder installed");

    // Create broadcast channel for WebSocket updates
    let (update_tx, _) = broadcast::channel::<Value>(1000);

    // Seed address is node 0
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", BASE_PORT).parse().unwrap();

    // Create nodes
    let mut nodes: Vec<Arc<RwLock<ChatNode>>> = Vec::new();
    let mut delegates = Vec::new();

    for i in 0..NUM_USERS {
        // First node (seed) has no seed_addr, others join via node 0
        let node_seed = if i == 0 { None } else { Some(seed_addr) };
        let node = ChatNode::new(i, update_tx.clone(), node_seed);
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
    for i in 0..5.min(NUM_USERS) {
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

    for i in 0..NUM_USERS {
        let node = nodes[i].clone();
        let (broadcast_tx, mut broadcast_rx) = mpsc::channel::<String>(100);
        broadcast_txs.push(broadcast_tx);

        // Message handling task (only handles broadcast)
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

    let app_state = AppState {
        broadcast_txs: Arc::new(broadcast_txs),
        update_tx,
        delegates: Arc::new(delegates),
        nodes: Arc::new(nodes),
        prometheus_handle,
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
        .route("/metrics", get(api_prometheus_metrics))
        .route("/api/metrics", get(api_metrics_json))
        .route("/ws", get(ws_handler))
        .nest_service("/", ServeDir::new(&static_dir))
        .with_state(app_state);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("\nServer running on http://localhost:3000");
    println!("Prometheus metrics at http://localhost:3000/metrics");
    println!("JSON metrics API at http://localhost:3000/api/metrics");
    println!("\nKey features demonstrated:");
    println!("  - MemberlistStack for real SWIM + Plumtree integration");
    println!(
        "  - Real UDP/TCP networking on localhost (ports {}-{})",
        BASE_PORT,
        BASE_PORT + NUM_USERS as u16 - 1
    );
    println!("  - Automatic peer discovery via SWIM gossip");
    println!("  - ChannelTransport routing for Plumtree messages between local nodes");
    println!("  - Online/offline toggle with proper shutdown/restart");
    println!("  - Prometheus metrics with /metrics endpoint");

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
