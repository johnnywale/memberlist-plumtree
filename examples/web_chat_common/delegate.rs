//! Shared Plumtree delegate implementation for web chat examples.
//!
//! This module provides a generic delegate implementation that works with
//! different node ID types. Currently not used as each example has its own
//! delegate with specific behavior, but kept for future unification.

#![allow(dead_code)]

use super::types::*;
use bytes::Bytes;
use memberlist_plumtree::{MessageId, PeerStats, PeerTopology, PlumtreeDelegate};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
    time::Instant,
};
use tokio::sync::{broadcast, RwLock};

/// Trait for formatting node IDs as user-friendly names (e.g., "U1", "U2")
pub trait NodeIdFormatter: Clone + Debug + Display + Eq + Hash + Send + Sync + 'static {
    fn to_user_name(&self) -> String;
    fn from_index(idx: usize) -> Self;
}

/// Trait for accessing peer topology from the underlying stack.
/// Implemented by wrapper types that hold the stack reference.
pub trait PeerTopologyProvider<I>: Send + Sync {
    fn topology(&self) -> PeerTopology<I>;
    fn peer_stats(&self) -> PeerStats;
    fn promote_to_eager(&self, peer: &I);
    fn demote_to_lazy(&self, peer: &I);
}

/// Plumtree delegate that handles message delivery and events.
///
/// Generic over the NodeId type to work with both MemberlistStack and PlumtreeStack.
pub struct ChatPlumtreeDelegate<I: NodeIdFormatter> {
    pub node_name: String,
    pub node_idx: usize,
    pub messages: Arc<RwLock<Vec<ReceivedMessage>>>,
    pub events: Arc<RwLock<Vec<TimestampedEvent>>>,
    pub metrics: Arc<Metrics>,
    pub update_tx: broadcast::Sender<Value>,
    pub pending_context: Arc<RwLock<HashMap<MessageId, (String, u32)>>>,
    pub start_time: Instant,
    /// Topology provider (set after stack is created)
    topology_provider: Arc<RwLock<Option<Arc<dyn PeerTopologyProvider<I>>>>>,
}

impl<I: NodeIdFormatter> ChatPlumtreeDelegate<I> {
    pub fn new(node_idx: usize, node_name: String, update_tx: broadcast::Sender<Value>) -> Self {
        Self {
            node_name,
            node_idx,
            messages: Arc::new(RwLock::new(Vec::new())),
            events: Arc::new(RwLock::new(Vec::new())),
            metrics: Arc::new(Metrics::new()),
            update_tx,
            pending_context: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
            topology_provider: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the topology provider (called after stack is created)
    pub async fn set_topology_provider(&self, provider: Arc<dyn PeerTopologyProvider<I>>) {
        *self.topology_provider.write().await = Some(provider);
    }

    /// Clear the topology provider (called on shutdown)
    pub async fn clear_topology_provider(&self) {
        *self.topology_provider.write().await = None;
    }

    pub fn format_elapsed(&self) -> String {
        format_elapsed(self.start_time)
    }

    pub async fn get_peers_json(&self) -> Value {
        let topo = self.get_topology().await;
        let eager: Vec<String> = topo.eager.iter().map(|p| p.to_user_name()).collect();
        let lazy: Vec<String> = topo.lazy.iter().map(|p| p.to_user_name()).collect();
        json!({ "eager": eager, "lazy": lazy })
    }

    pub async fn get_topology(&self) -> PeerTopology<I> {
        let provider_guard = self.topology_provider.read().await;
        if let Some(ref provider) = *provider_guard {
            provider.topology()
        } else {
            PeerTopology::default()
        }
    }

    pub async fn get_peer_stats(&self) -> PeerStats {
        let provider_guard = self.topology_provider.read().await;
        if let Some(ref provider) = *provider_guard {
            provider.peer_stats()
        } else {
            PeerStats {
                eager_count: 0,
                lazy_count: 0,
            }
        }
    }

    pub async fn promote_all_to_eager(&self) {
        let provider_guard = self.topology_provider.read().await;
        if let Some(ref provider) = *provider_guard {
            let topology = provider.topology();
            for peer in topology.lazy.iter() {
                provider.promote_to_eager(peer);
            }
        }
    }

    pub async fn demote_all_to_lazy(&self) {
        let provider_guard = self.topology_provider.read().await;
        if let Some(ref provider) = *provider_guard {
            let topology = provider.topology();
            for peer in topology.eager.iter() {
                provider.demote_to_lazy(peer);
            }
        }
    }

    pub async fn add_event(&self, event: ProtocolEvent) {
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

    /// Helper to get peers JSON from topology provider
    async fn peers_json_from_provider(&self) -> Value {
        let provider_guard = self.topology_provider.read().await;
        if let Some(ref provider) = *provider_guard {
            let topo = provider.topology();
            json!({
                "eager": topo.eager.iter().map(|p| p.to_user_name()).collect::<Vec<_>>(),
                "lazy": topo.lazy.iter().map(|p| p.to_user_name()).collect::<Vec<_>>()
            })
        } else {
            json!({ "eager": [], "lazy": [] })
        }
    }
}

impl<I: NodeIdFormatter> PlumtreeDelegate<I> for ChatPlumtreeDelegate<I> {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        if let Some(chat_msg) = ChatMessage::decode(&payload) {
            let is_self = chat_msg.from == self.node_name;
            let pending = self.pending_context.clone();
            let messages = self.messages.clone();
            let update_tx = self.update_tx.clone();
            let node_idx = self.node_idx;
            let metrics = self.metrics.clone();
            let topology_provider = self.topology_provider.clone();

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
                    let provider_guard = topology_provider.read().await;
                    if let Some(ref provider) = *provider_guard {
                        let topo = provider.topology();
                        json!({
                            "eager": topo.eager.iter().map(|p| p.to_user_name()).collect::<Vec<_>>(),
                            "lazy": topo.lazy.iter().map(|p| p.to_user_name()).collect::<Vec<_>>()
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

    fn on_eager_promotion(&self, peer: &I) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = peer.to_user_name();
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let topology_provider = self.topology_provider.clone();

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
                let provider_guard = topology_provider.read().await;
                if let Some(ref provider) = *provider_guard {
                    let topo = provider.topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| p.to_user_name()).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| p.to_user_name()).collect::<Vec<_>>()
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

    fn on_lazy_demotion(&self, peer: &I) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = peer.to_user_name();
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let topology_provider = self.topology_provider.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PeerDemoted { peer: peer_name };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let peers_json = {
                let provider_guard = topology_provider.read().await;
                if let Some(ref provider) = *provider_guard {
                    let topo = provider.topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| p.to_user_name()).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| p.to_user_name()).collect::<Vec<_>>()
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

    fn on_graft_sent(&self, peer: &I, message_id: &MessageId) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = peer.to_user_name();
        let msg_id = message_id.to_string();
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let topology_provider = self.topology_provider.clone();

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
                let provider_guard = topology_provider.read().await;
                if let Some(ref provider) = *provider_guard {
                    let topo = provider.topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| p.to_user_name()).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| p.to_user_name()).collect::<Vec<_>>()
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

    fn on_prune_sent(&self, peer: &I) {
        let events = self.events.clone();
        let update_tx = self.update_tx.clone();
        let peer_name = peer.to_user_name();
        let time = self.format_elapsed();
        let node_idx = self.node_idx;
        let metrics = self.metrics.clone();
        let topology_provider = self.topology_provider.clone();

        tokio::spawn(async move {
            let event = ProtocolEvent::PruneSent { to: peer_name };
            let timestamped = TimestampedEvent { time, event };
            let mut evts = events.write().await;
            evts.push(timestamped.clone());
            if evts.len() > 100 {
                evts.remove(0);
            }

            let peers_json = {
                let provider_guard = topology_provider.read().await;
                if let Some(ref provider) = *provider_guard {
                    let topo = provider.topology();
                    json!({
                        "eager": topo.eager.iter().map(|p| p.to_user_name()).collect::<Vec<_>>(),
                        "lazy": topo.lazy.iter().map(|p| p.to_user_name()).collect::<Vec<_>>()
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
