#![allow(clippy::type_complexity)]
#![allow(clippy::needless_range_loop)]

//! Web-based Chat example using PlumtreeStack with QUIC transport.
//!
//! ## Architecture: PlumtreeStack with QUIC
//!
//! This example demonstrates the clean `PlumtreeStack` abstraction for running
//! Plumtree with real QUIC networking. Each node binds to a unique port.
//!
//! Key components:
//! - `PlumtreeStack` - high-level abstraction combining PlumtreeDiscovery + QUIC
//! - `QuicTransport` - real QUIC/UDP networking
//! - `MapPeerResolver` - peer ID to address mapping
//! - Automatic background tasks (IHave scheduler, Graft timer, etc.)
//!
//! ## Features
//! - Real-time WebSocket updates
//! - Static HTML UI with peer tree visualization
//! - QUIC connection events and statistics
//! - Protocol metrics display
//! - Fault injection (node failure)
//!
//! Run with: cargo run --example web-chat-quic --features "tokio,quic,metrics"
//! Then open: http://localhost:3001

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
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use memberlist_plumtree::{
    discovery::{StaticDiscovery, StaticDiscoveryConfig},
    ConnectionEvent, MapPeerResolver, MessageId, PeerStats, PeerStatus, PeerTopology,
    PlumtreeConfig, PlumtreeDelegate, PlumtreeStack, PlumtreeStackConfig, QuicConfig,
};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, mpsc, RwLock};
use tower_http::services::ServeDir;

/// Number of users in the chat
const NUM_USERS: usize = 20;

/// Base port for QUIC (each node uses BASE_QUIC_PORT + node_idx)
const BASE_QUIC_PORT: u16 = 18000;

/// Web server port
const WEB_PORT: u16 = 3001;

// ============================================================================
// Node ID Type
// ============================================================================

/// Use u64 as the node ID type since it implements all required traits
/// (CheapClone, Eq, Ord, Hash, Debug, Display via nodecraft::Id).
type NodeId = u64;

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
    // QUIC-specific events
    QuicConnected { peer: String, rtt_ms: u64 },
    QuicDisconnected { peer: String, reason: String },
    QuicMigrated { peer: String, new_addr: String },
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
            ProtocolEvent::QuicConnected { peer, rtt_ms } => {
                json!({ "type": "quic_connect", "peer": peer, "rtt_ms": rtt_ms })
            }
            ProtocolEvent::QuicDisconnected { peer, reason } => {
                json!({ "type": "quic_disconnect", "peer": peer, "reason": reason })
            }
            ProtocolEvent::QuicMigrated { peer, new_addr } => {
                json!({ "type": "quic_migrate", "peer": peer, "new_addr": new_addr })
            }
        }
    }
}

/// Metrics counters for QUIC-specific events.
/// Protocol metrics (messages, grafts, prunes, promotions, demotions) are read from Prometheus.
#[derive(Debug)]
struct Metrics {
    // QUIC-specific metrics (not tracked by library)
    quic_connections: AtomicU64,
    quic_disconnections: AtomicU64,
    quic_migrations: AtomicU64,
    quic_bytes_sent: AtomicU64,
    quic_0rtt_sent: AtomicU64,
    // Peer health gauges (use AtomicI64 to support decrement safely)
    peers_healthy: AtomicI64,
    peers_degraded: AtomicI64,
    peers_zombie: AtomicI64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            quic_connections: AtomicU64::new(0),
            quic_disconnections: AtomicU64::new(0),
            quic_migrations: AtomicU64::new(0),
            quic_bytes_sent: AtomicU64::new(0),
            quic_0rtt_sent: AtomicU64::new(0),
            peers_healthy: AtomicI64::new(0),
            peers_degraded: AtomicI64::new(0),
            peers_zombie: AtomicI64::new(0),
        }
    }
}

impl Metrics {
    fn new() -> Self {
        Self::default()
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
    /// Decrements the old state counter and increments the new state counter.
    #[allow(dead_code)]
    fn transition_peer_health(&self, from: Option<PeerStatus>, to: PeerStatus) {
        // Decrement old state if present
        if let Some(old) = from {
            match old {
                PeerStatus::Healthy => self.dec_healthy(),
                PeerStatus::Degraded => self.dec_degraded(),
                PeerStatus::Zombie => self.dec_zombie(),
            }
        }
        // Increment new state
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

    /// Convert to JSON, reading protocol metrics from Prometheus text output.
    fn to_json_from_prometheus(&self, prometheus_text: &str) -> Value {
        // Helper to get a metric value from Prometheus text (global metrics without labels)
        let get_metric = |name: &str| -> u64 {
            for line in prometheus_text.lines() {
                if line.starts_with('#') || line.is_empty() {
                    continue;
                }
                if line.starts_with(name) {
                    // Check if this is a global metric (no labels or empty labels {})
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

        json!({
            "sent": get_metric("plumtree_messages_broadcast_total"),
            "received": get_metric("plumtree_messages_delivered_total"),
            "grafts": get_metric("plumtree_graft_sent_total"),
            "prunes": get_metric("plumtree_prune_sent_total"),
            "promotions": get_metric("plumtree_peer_promotions_total"),
            "demotions": get_metric("plumtree_peer_demotions_total"),
            "quic": {
                "connections": self.quic_connections.load(Ordering::SeqCst),
                "disconnections": self.quic_disconnections.load(Ordering::SeqCst),
                "migrations": self.quic_migrations.load(Ordering::SeqCst),
                "bytes_sent": self.quic_bytes_sent.load(Ordering::SeqCst),
                "zero_rtt_sent": self.quic_0rtt_sent.load(Ordering::SeqCst),
            },
            "peer_health": {
                "healthy": self.peers_healthy.load(Ordering::SeqCst).max(0),
                "degraded": self.peers_degraded.load(Ordering::SeqCst).max(0),
                "zombie": self.peers_zombie.load(Ordering::SeqCst).max(0),
            }
        })
    }

    /// Get QUIC-only metrics as JSON (when Prometheus text is not available)
    fn quic_only_json(&self) -> Value {
        json!({
            "quic": {
                "connections": self.quic_connections.load(Ordering::SeqCst),
                "disconnections": self.quic_disconnections.load(Ordering::SeqCst),
                "migrations": self.quic_migrations.load(Ordering::SeqCst),
                "bytes_sent": self.quic_bytes_sent.load(Ordering::SeqCst),
                "zero_rtt_sent": self.quic_0rtt_sent.load(Ordering::SeqCst),
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
        }
    }

    fn format_elapsed(&self) -> String {
        let elapsed = self.start_time.elapsed().as_secs();
        format!("{:02}:{:02}", elapsed / 60, elapsed % 60)
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
        // Note: messages_received is tracked by Prometheus (plumtree_messages_delivered_total)

        if let Some(chat_msg) = ChatMessage::decode(&payload) {
            let is_self = chat_msg.from == self.node_name;
            let pending = self.pending_context.clone();
            let messages = self.messages.clone();
            let update_tx = self.update_tx.clone();
            let node_idx = self.node_idx;
            let metrics = self.metrics.clone();

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

                let _ = update_tx.send(json!({
                    "type": "message",
                    "node": node_idx,
                    "message": received.to_json(),
                    "metrics": metrics.quic_only_json()
                }));
            });
        }
    }

    fn on_eager_promotion(&self, peer: &NodeId) {
        // Note: promotions tracked by Prometheus (plumtree_peer_promotions_total)
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", *peer + 1);
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();

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

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.quic_only_json()
            }));
        });
    }

    fn on_lazy_demotion(&self, peer: &NodeId) {
        // Note: demotions tracked by Prometheus (plumtree_peer_demotions_total)
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", *peer + 1);
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PeerDemoted { peer: peer_name };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.quic_only_json()
            }));
        });
    }

    fn on_graft_sent(&self, peer: &NodeId, message_id: &MessageId) {
        // Note: grafts_sent tracked by Prometheus (plumtree_graft_sent_total)
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", *peer + 1);
        let msg_id = message_id.to_string();
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();

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

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.quic_only_json()
            }));
        });
    }

    fn on_prune_sent(&self, peer: &NodeId) {
        // Note: prunes_sent tracked by Prometheus (plumtree_prune_sent_total)
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = format!("U{}", *peer + 1);
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PruneSent { to: peer_name };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let _ = update_tx.send(json!({
                "type": "event",
                "node": node_idx,
                "event": timestamped.to_json(),
                "metrics": metrics.quic_only_json()
            }));
        });
    }
}

// ============================================================================
// Chat Node using PlumtreeStack
// ============================================================================

/// Chat node using PlumtreeStack for QUIC-based networking.
struct ChatNodeQuic {
    node_idx: usize,
    node_id: NodeId,
    delegate: Arc<ChatPlumtreeDelegate>,
    /// The PlumtreeStack is created on start() and consumed on stop()
    stack: Option<PlumtreeStack<NodeId, Arc<ChatPlumtreeDelegate>>>,
    /// Handle for the connection event handler task
    event_handler: Option<tokio::task::JoinHandle<()>>,
    is_online: bool,
    port: u16,
    /// Shared peer resolver (kept for potential future use)
    #[allow(dead_code)]
    shared_resolver: Arc<MapPeerResolver<NodeId>>,
    update_tx: broadcast::Sender<Value>,
}

impl ChatNodeQuic {
    fn new(
        node_idx: usize,
        update_tx: broadcast::Sender<Value>,
        shared_resolver: Arc<MapPeerResolver<NodeId>>,
    ) -> Self {
        let node_id = node_idx as u64;
        let node_name = format!("U{}", node_idx + 1);
        let delegate = Arc::new(ChatPlumtreeDelegate::new(
            node_idx,
            node_name,
            update_tx.clone(),
        ));
        let port = BASE_QUIC_PORT + node_idx as u16;

        Self {
            node_idx,
            node_id,
            delegate,
            stack: None,
            event_handler: None,
            is_online: false,
            port,
            shared_resolver,
            update_tx,
        }
    }

    fn is_online(&self) -> bool {
        self.is_online
    }

    /// Start the node using PlumtreeStack
    async fn start(&mut self) -> Result<(), String> {
        if self.is_online {
            return Ok(());
        }

        let bind_addr: SocketAddr = format!("127.0.0.1:{}", self.port)
            .parse()
            .map_err(|e| format!("Invalid address: {}", e))?;

        // Build discovery config with all other nodes as seeds
        let mut discovery_config = StaticDiscoveryConfig::new();
        for i in 0..NUM_USERS {
            if i != self.node_idx {
                let peer_id = i as u64;
                let peer_addr: SocketAddr = format!("127.0.0.1:{}", BASE_QUIC_PORT + i as u16)
                    .parse()
                    .unwrap();
                discovery_config = discovery_config.with_seed(peer_id, peer_addr);
            }
        }

        let discovery = StaticDiscovery::new(discovery_config).with_local_addr(bind_addr);

        // Build PlumtreeStack config
        let config = PlumtreeStackConfig::new(self.node_id, bind_addr)
            .with_plumtree(
                PlumtreeConfig::default()
                    .with_eager_fanout(2)
                    .with_lazy_fanout(4)
                    .with_ihave_interval(Duration::from_millis(50))
                    .with_graft_timeout(Duration::from_millis(200)),
            )
            .with_quic(QuicConfig::insecure_dev())
            .with_discovery(discovery);

        // Build the stack
        let mut stack = config
            .build(self.delegate.clone())
            .await
            .map_err(|e| format!("Failed to build stack: {}", e))?;

        // Handle connection events
        if let Some(event_rx) = stack.take_event_receiver() {
            let delegate = self.delegate.clone();
            let node_idx = self.node_idx;
            let update_tx = self.update_tx.clone();

            self.event_handler = Some(tokio::spawn(async move {
                while let Ok(event) = event_rx.recv().await {
                    match event {
                        ConnectionEvent::Connected { peer, rtt, .. } => {
                            delegate
                                .metrics
                                .quic_connections
                                .fetch_add(1, Ordering::SeqCst);
                            // New peer starts as healthy
                            delegate.metrics.inc_healthy();
                            let protocol_event = ProtocolEvent::QuicConnected {
                                peer: format!("U{}", peer + 1),
                                rtt_ms: rtt.as_millis() as u64,
                            };
                            delegate.add_event(protocol_event.clone()).await;
                            let _ = update_tx.send(json!({
                                "type": "event",
                                "node": node_idx,
                                "event": TimestampedEvent {
                                    time: delegate.format_elapsed(),
                                    event: protocol_event,
                                }.to_json(),
                                "metrics": delegate.metrics.quic_only_json()
                            }));
                        }
                        ConnectionEvent::Disconnected { peer, reason } => {
                            delegate
                                .metrics
                                .quic_disconnections
                                .fetch_add(1, Ordering::SeqCst);
                            // Peer disconnected - remove from healthy count
                            delegate.metrics.dec_healthy();
                            let reason_str = format!("{:?}", reason);
                            let protocol_event = ProtocolEvent::QuicDisconnected {
                                peer: format!("U{}", peer + 1),
                                reason: reason_str,
                            };
                            delegate.add_event(protocol_event.clone()).await;
                            let _ = update_tx.send(json!({
                                "type": "event",
                                "node": node_idx,
                                "event": TimestampedEvent {
                                    time: delegate.format_elapsed(),
                                    event: protocol_event,
                                }.to_json(),
                                "metrics": delegate.metrics.quic_only_json()
                            }));
                        }
                        ConnectionEvent::Migrated { peer, new_addr, .. } => {
                            delegate
                                .metrics
                                .quic_migrations
                                .fetch_add(1, Ordering::SeqCst);
                            let protocol_event = ProtocolEvent::QuicMigrated {
                                peer: format!("U{}", peer + 1),
                                new_addr: new_addr.to_string(),
                            };
                            delegate.add_event(protocol_event.clone()).await;
                            let _ = update_tx.send(json!({
                                "type": "event",
                                "node": node_idx,
                                "event": TimestampedEvent {
                                    time: delegate.format_elapsed(),
                                    event: protocol_event,
                                }.to_json(),
                                "metrics": delegate.metrics.quic_only_json()
                            }));
                        }
                        ConnectionEvent::Reconnecting { .. } => {
                            // Don't emit event for reconnecting - too noisy
                        }
                    }
                }
            }));
        }

        self.stack = Some(stack);
        self.is_online = true;
        self.delegate.add_event(ProtocolEvent::NodeOnline).await;

        Ok(())
    }

    /// Stop the node
    async fn stop(&mut self) {
        if !self.is_online {
            return;
        }

        // Abort event handler
        if let Some(handle) = self.event_handler.take() {
            handle.abort();
        }

        // Shutdown stack
        if let Some(stack) = self.stack.take() {
            stack.shutdown().await;
        }

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

        let msg = ChatMessage::new(format!("U{}", self.node_idx + 1), text);
        let payload = msg.encode();

        // Note: messages_sent is tracked by Prometheus (plumtree_messages_broadcast_total)
        let msg_id = stack.broadcast(payload.clone()).await?;

        // Deliver to self
        self.delegate.on_deliver(msg_id, payload);

        Ok(msg_id)
    }

    fn get_topology(&self) -> PeerTopology<NodeId> {
        if let Some(ref stack) = self.stack {
            stack.plumtree().peers().topology()
        } else {
            PeerTopology::default()
        }
    }

    fn peer_stats(&self) -> PeerStats {
        if let Some(ref stack) = self.stack {
            stack.peer_stats()
        } else {
            PeerStats {
                eager_count: 0,
                lazy_count: 0,
            }
        }
    }

    async fn get_peers_json(&self) -> Value {
        let topo = self.get_topology();
        let eager: Vec<String> = topo.eager.iter().map(|p| format!("U{}", *p + 1)).collect();
        let lazy: Vec<String> = topo.lazy.iter().map(|p| format!("U{}", *p + 1)).collect();
        json!({ "eager": eager, "lazy": lazy })
    }

    async fn promote_all_to_eager(&self) {
        if let Some(ref stack) = self.stack {
            let topology = stack.plumtree().peers().topology();
            for peer in topology.lazy.iter() {
                stack.plumtree().peers().promote_to_eager(peer);
            }
        }
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
    nodes: Arc<Vec<Arc<RwLock<ChatNodeQuic>>>>,
    prometheus_handle: PrometheusHandle,
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut update_rx = state.update_tx.subscribe();

    // Send initial state for all nodes
    let prometheus_text = state.prometheus_handle.render();
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
        let node_guard = state.nodes[i].read().await;
        let peers_json = node_guard.get_peers_json().await;
        let online = node_guard.is_online();

        let init = json!({
            "type": "init",
            "node": i,
            "messages": messages,
            "events": events,
            "metrics": delegate.metrics.to_json_from_prometheus(&prometheus_text),
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
                let peers_json = node_guard.get_peers_json().await;
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
        "reset_metrics" => {
            // Note: Prometheus metrics cannot be reset. Just send current metrics.
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
        "version": "1.0.0",
        "transport": "PlumtreeStack with QUIC transport"
    }))
}

/// GET /api/status - Returns status for all nodes
async fn api_status(State(state): State<AppState>) -> Json<Value> {
    let mut nodes = Vec::new();
    let prometheus_text = state.prometheus_handle.render();

    for (i, delegate) in state.delegates.iter().enumerate() {
        let node_guard = state.nodes[i].read().await;
        let peers_json = node_guard.get_peers_json().await;
        let metrics = delegate.metrics.to_json_from_prometheus(&prometheus_text);
        let online = node_guard.is_online();

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
    let node_guard = state.nodes[node_id].read().await;
    let peers_json = node_guard.get_peers_json().await;
    let prometheus_text = state.prometheus_handle.render();
    let metrics = delegate.metrics.to_json_from_prometheus(&prometheus_text);
    let online = node_guard.is_online();

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

    let node_guard = state.nodes[node_id].read().await;
    let peers_json = node_guard.get_peers_json().await;
    let online = node_guard.is_online();

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
    let metrics = parse_prometheus_metrics(&prometheus_text);

    // Add aggregated QUIC stats
    let mut total_quic_connections: u64 = 0;
    let mut total_quic_bytes_sent: u64 = 0;

    for delegate in state.delegates.iter() {
        total_quic_connections += delegate.metrics.quic_connections.load(Ordering::SeqCst);
        total_quic_bytes_sent += delegate.metrics.quic_bytes_sent.load(Ordering::SeqCst);
    }

    let mut result = metrics;
    if let Some(gauges) = result.get_mut("gauges").and_then(|v| v.as_object_mut()) {
        gauges.insert(
            "quic_total_connections".to_string(),
            json!(total_quic_connections),
        );
        gauges.insert(
            "quic_total_bytes_sent".to_string(),
            json!(total_quic_bytes_sent),
        );
    }

    Json(result)
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

        if let Some((name, value)) = parse_metric_line(line) {
            if name.contains("_total") {
                counters.insert(name, json!(value));
            } else if name.contains("_bucket") || name.contains("_sum") || name.contains("_count") {
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
    let line = line.trim();
    let last_space = line.rfind(' ')?;
    let value_str = &line[last_space + 1..];
    let name_part = &line[..last_space];

    let value: f64 = value_str.parse().ok()?;

    let name = if let Some(brace_pos) = name_part.find('{') {
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
    println!("Starting Plumtree Web Chat Demo with PlumtreeStack (QUIC)...");
    println!(
        "Using {} nodes with QUIC networking on ports {}-{}",
        NUM_USERS,
        BASE_QUIC_PORT,
        BASE_QUIC_PORT + NUM_USERS as u16 - 1
    );
    println!();

    // Initialize Prometheus metrics recorder
    let prometheus_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");
    println!("Prometheus metrics recorder installed");

    // Create broadcast channel for WebSocket updates
    let (update_tx, _) = broadcast::channel::<Value>(1000);

    // Create shared peer resolver
    let local_addr: SocketAddr = format!("127.0.0.1:{}", BASE_QUIC_PORT).parse().unwrap();
    let shared_resolver = Arc::new(MapPeerResolver::new(local_addr));

    // Pre-populate resolver with all node addresses
    for i in 0..NUM_USERS {
        let peer_id = i as u64;
        let peer_addr: SocketAddr = format!("127.0.0.1:{}", BASE_QUIC_PORT + i as u16)
            .parse()
            .unwrap();
        shared_resolver.add_peer(peer_id, peer_addr);
    }

    // Create nodes
    let mut nodes: Vec<Arc<RwLock<ChatNodeQuic>>> = Vec::new();
    let mut delegates = Vec::new();

    for i in 0..NUM_USERS {
        let node = ChatNodeQuic::new(i, update_tx.clone(), shared_resolver.clone());
        delegates.push(node.delegate.clone());
        nodes.push(Arc::new(RwLock::new(node)));
    }

    // Start all nodes
    println!("Starting {} nodes...", NUM_USERS);
    for i in 0..NUM_USERS {
        print!("Starting node U{}...", i + 1);
        match nodes[i].write().await.start().await {
            Ok(_) => println!(" OK"),
            Err(e) => println!(" FAILED: {}", e),
        }
        // Small delay between node starts
        if i % 5 == 4 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    println!("\nAll {} nodes started!", NUM_USERS);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Print topology for first few nodes
    println!("\nPlumtree peer topology (first 5 nodes):");
    for i in 0..5.min(NUM_USERS) {
        let node_guard = nodes[i].read().await;
        let stats = node_guard.peer_stats();
        println!(
            "  U{}: {} eager, {} lazy",
            i + 1,
            stats.eager_count,
            stats.lazy_count
        );
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
    let addr = SocketAddr::from(([0, 0, 0, 0], WEB_PORT));
    println!("\nServer running on http://localhost:{}", WEB_PORT);
    println!(
        "Prometheus metrics at http://localhost:{}/metrics",
        WEB_PORT
    );
    println!("\nKey features demonstrated:");
    println!("  - PlumtreeStack for clean QUIC integration");
    println!(
        "  - Real QUIC/UDP networking on localhost (ports {}-{})",
        BASE_QUIC_PORT,
        BASE_QUIC_PORT + NUM_USERS as u16 - 1
    );
    println!("  - Connection events (connect/disconnect/migrate)");
    println!("  - TLS encryption (self-signed certificates)");
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
