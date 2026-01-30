//! Common protocol types for web chat examples.
//!
//! These types are shared between web_chat.rs (MemberlistStack) and
//! web_chat_quic.rs (PlumtreeStack).

#![allow(dead_code)]

use bytes::Bytes;
use memberlist_plumtree::MessageId;
use serde_json::{json, Value};
use std::time::Instant;

// ============================================================================
// Chat Message
// ============================================================================

/// Chat message payload with sender, text, and timestamp.
///
/// Uses a simple pipe-delimited text format: "from|timestamp|text"
#[derive(Debug, Clone)]
pub struct ChatMessage {
    pub from: String,
    pub text: String,
    pub timestamp: u64,
}

impl ChatMessage {
    pub fn new(from: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            text: text.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    pub fn encode(&self) -> Bytes {
        Bytes::from(format!("{}|{}|{}", self.from, self.timestamp, self.text))
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(data).ok()?;
        let mut parts = s.splitn(3, '|');
        Some(Self {
            from: parts.next()?.to_string(),
            timestamp: parts.next()?.parse().ok()?,
            text: parts.next()?.to_string(),
        })
    }

    pub fn format_time(&self) -> String {
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
// Delivery Method
// ============================================================================

/// How a message was delivered to this node.
#[derive(Debug, Clone)]
pub enum DeliveryMethod {
    /// Message was broadcast by this node
    SelfBroadcast,
    /// Message arrived via Gossip from an eager peer
    Gossip { round: u32, forwarder: String },
    /// Message was retrieved via Graft after receiving IHave
    Graft { forwarder: String },
}

impl std::fmt::Display for DeliveryMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeliveryMethod::SelfBroadcast => write!(f, "LOCAL (self-broadcast)"),
            DeliveryMethod::Gossip { round, forwarder } => {
                write!(f, "GOSSIP from {} (round {})", forwarder, round)
            }
            DeliveryMethod::Graft { forwarder } => {
                write!(f, "GRAFT from {} (tree repair)", forwarder)
            }
        }
    }
}

// ============================================================================
// Protocol Events
// ============================================================================

/// Protocol event for the event log.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ProtocolEvent {
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
    // QUIC-specific events
    QuicConnected { peer: String, rtt_ms: u64 },
    QuicDisconnected { peer: String, reason: String },
    QuicMigrated { peer: String, new_addr: String },
}

impl ProtocolEvent {
    pub fn to_json(&self) -> Value {
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

/// A timestamped protocol event for display.
#[derive(Debug, Clone)]
pub struct TimestampedEvent {
    pub time: String,
    pub event: ProtocolEvent,
}

impl TimestampedEvent {
    pub fn new(time: String, event: ProtocolEvent) -> Self {
        Self { time, event }
    }

    pub fn to_json(&self) -> Value {
        let mut event_json = self.event.to_json();
        if let Some(obj) = event_json.as_object_mut() {
            obj.insert("time".to_string(), json!(self.time));
        }
        event_json
    }
}

// ============================================================================
// Received Message
// ============================================================================

/// A received message with delivery metadata for UI display.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub from: String,
    pub text: String,
    pub timestamp: String,
    pub delivery: String,
    pub message_id: MessageId,
}

impl ReceivedMessage {
    pub fn to_json(&self) -> Value {
        json!({
            "from": self.from,
            "text": self.text,
            "timestamp": self.timestamp,
            "delivery": self.delivery
        })
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Metrics reader that extracts values from Prometheus output.
///
/// Instead of manually tracking counters, this reads from the system metrics.
/// Peer health metrics (healthy/degraded/zombie) are tracked by the library
/// via the `metrics` crate.
#[derive(Debug, Default)]
pub struct Metrics;

impl Metrics {
    pub fn new() -> Self {
        Self
    }

    /// Extract metrics from Prometheus text output.
    ///
    /// The library records global metrics (without node labels), so metrics
    /// are aggregated across all nodes in the process.
    pub fn to_json_from_prometheus(&self, prometheus_text: &str) -> Value {
        let get_metric = |name: &str| -> u64 {
            for line in prometheus_text.lines() {
                if line.starts_with('#') || line.is_empty() {
                    continue;
                }
                if line.starts_with(name) {
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

        // Priority queue gauges
        let priority_critical = get_metric("plumtree_priority_critical");
        let priority_high = get_metric("plumtree_priority_high");
        let priority_normal = get_metric("plumtree_priority_normal");
        let priority_low = get_metric("plumtree_priority_low");

        // Peer health gauges
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

    /// Legacy method for compatibility - returns empty metrics.
    pub fn to_json(&self) -> Value {
        json!({
            "sent": 0,
            "received": 0,
            "grafts": 0,
            "prunes": 0,
            "promotions": 0,
            "demotions": 0,
            "joined": 0,
            "left": 0,
            "compression": { "bytes_in": 0, "bytes_saved": 0 },
            "priority_queue": { "critical": 0, "high": 0, "normal": 0, "low": 0 },
            "peer_health": { "healthy": 0, "degraded": 0, "zombie": 0 }
        })
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Format elapsed time since start as "MM:SS".
pub fn format_elapsed(start_time: Instant) -> String {
    let elapsed = start_time.elapsed().as_secs();
    format!("{:02}:{:02}", elapsed / 60, elapsed % 60)
}
