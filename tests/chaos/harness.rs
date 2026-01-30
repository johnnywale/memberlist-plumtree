//! Test harness for chaos engineering tests.
//!
//! Provides a ChaosTestCluster that wraps multiple Plumtree instances
//! with chaos injection capabilities.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use memberlist_plumtree::testing::EnhancedChaosController;
use memberlist_plumtree::{MessageId, PlumtreeConfig};
use parking_lot::RwLock;

/// A simulated node in the chaos test cluster.
#[derive(Debug)]
pub struct ChaosNode {
    /// Node ID.
    #[allow(dead_code)]
    pub id: u64,
    /// Messages delivered to this node.
    pub delivered: RwLock<Vec<(MessageId, Bytes)>>,
    /// Eager peers.
    pub eager_peers: RwLock<Vec<u64>>,
    /// Lazy peers.
    pub lazy_peers: RwLock<Vec<u64>>,
    /// Whether the node is online.
    pub online: std::sync::atomic::AtomicBool,
}
impl ChaosNode {
    /// Create a new chaos node.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            delivered: RwLock::new(Vec::new()),
            eager_peers: RwLock::new(Vec::new()),
            lazy_peers: RwLock::new(Vec::new()),
            online: std::sync::atomic::AtomicBool::new(true),
        }
    }

    /// Add a delivered message.
    pub fn deliver(&self, id: MessageId, payload: Bytes) {
        self.delivered.write().push((id, payload));
    }

    /// Get count of delivered messages.
    #[allow(dead_code)]
    pub fn delivered_count(&self) -> usize {
        self.delivered.read().len()
    }

    /// Check if a specific message was delivered.
    pub fn has_message(&self, id: &MessageId) -> bool {
        self.delivered.read().iter().any(|(mid, _)| mid == id)
    }

    /// Take the node offline.
    pub fn go_offline(&self) {
        self.online
            .store(false, std::sync::atomic::Ordering::Release);
    }

    /// Bring the node online.
    pub fn go_online(&self) {
        self.online
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check if the node is online.
    pub fn is_online(&self) -> bool {
        self.online.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Message types for the simulated protocol.
#[derive(Debug, Clone)]
pub enum SimMessage {
    /// Gossip message with full payload.
    Gossip {
        id: MessageId,
        payload: Bytes,
        round: u32,
    },
    /// IHave announcement.
    ///
    #[allow(dead_code)]
    IHave { id: MessageId, round: u32 },
    /// Graft request.
    #[allow(dead_code)]
    Graft { id: MessageId },
    /// Prune request.
    #[allow(dead_code)]
    Prune,
}

/// A test cluster with chaos injection.
#[derive(Debug)]
pub struct ChaosTestCluster {
    /// Nodes in the cluster.
    pub nodes: HashMap<u64, Arc<ChaosNode>>,
    /// Chaos controller.
    pub chaos: EnhancedChaosController<u64, SimMessage>,
    /// Configuration.
    #[allow(dead_code)]
    pub config: PlumtreeConfig,
    /// Message counter for unique IDs.
    #[allow(dead_code)]
    message_counter: AtomicU64,
    /// In-flight messages (simulated network).
    in_flight: RwLock<Vec<(u64, u64, SimMessage, std::time::Instant)>>,
}

impl ChaosTestCluster {
    /// Create a new chaos test cluster with the given number of nodes.
    pub fn new(node_count: usize) -> Self {
        let mut nodes = HashMap::new();
        for i in 0..node_count {
            let id = i as u64;
            nodes.insert(id, Arc::new(ChaosNode::new(id)));
        }

        // Connect nodes in a ring topology for initial eager peers
        for i in 0..node_count {
            let node = nodes.get(&(i as u64)).unwrap();
            let next = ((i + 1) % node_count) as u64;
            let prev = ((i + node_count - 1) % node_count) as u64;
            node.eager_peers.write().push(next);
            node.eager_peers.write().push(prev);

            // Add some lazy peers
            for j in 2..std::cmp::min(4, node_count) {
                let lazy = ((i + j) % node_count) as u64;
                if lazy != next && lazy != prev {
                    node.lazy_peers.write().push(lazy);
                }
            }
        }

        Self {
            nodes,
            chaos: EnhancedChaosController::new(),
            config: PlumtreeConfig::default(),
            message_counter: AtomicU64::new(0),
            in_flight: RwLock::new(Vec::new()),
        }
    }

    /// Create a cluster with specific configuration.
    ///
    #[allow(dead_code)]
    pub fn with_config(node_count: usize, config: PlumtreeConfig) -> Self {
        let mut cluster = Self::new(node_count);
        cluster.config = config;
        cluster
    }

    /// Get a node by ID.
    pub fn node(&self, id: u64) -> Option<Arc<ChaosNode>> {
        self.nodes.get(&id).cloned()
    }

    /// Broadcast a message from a node.
    ///
    /// Returns the message ID.
    pub fn broadcast(&self, from: u64, payload: Bytes) -> Option<MessageId> {
        let node = self.nodes.get(&from)?;
        if !node.is_online() {
            return None;
        }

        let id = MessageId::new();
        let round = 0;

        // Deliver locally
        node.deliver(id, payload.clone());

        // Send to eager peers
        let eager_peers = node.eager_peers.read().clone();
        for peer in eager_peers {
            self.send_message(
                from,
                peer,
                SimMessage::Gossip {
                    id,
                    payload: payload.clone(),
                    round,
                },
            );
        }

        // Queue IHave for lazy peers
        let lazy_peers = node.lazy_peers.read().clone();
        for peer in lazy_peers {
            self.send_message(from, peer, SimMessage::IHave { id, round });
        }

        Some(id)
    }

    /// Send a message from one node to another.
    fn send_message(&self, from: u64, to: u64, message: SimMessage) {
        // Check chaos conditions
        if let Some(latency) = self.chaos.should_deliver(&from, &to) {
            let deliver_at = std::time::Instant::now() + latency;
            self.in_flight.write().push((from, to, message, deliver_at));
        }
        // Message dropped if should_deliver returns None
    }

    /// Process pending messages and deliver those that are ready.
    pub fn tick(&self) {
        let now = std::time::Instant::now();

        // Get messages ready for delivery
        let ready: Vec<_> = {
            let mut in_flight = self.in_flight.write();
            let mut ready = Vec::new();
            let mut remaining = Vec::new();

            for msg in in_flight.drain(..) {
                if msg.3 <= now {
                    ready.push(msg);
                } else {
                    remaining.push(msg);
                }
            }

            *in_flight = remaining;
            ready
        };

        // Deliver messages
        for (from, to, message, _) in ready {
            self.deliver_message(from, to, message);
        }

        // Also deliver any reordered messages
        for delayed in self.chaos.drain_reordered() {
            self.deliver_message(delayed.from, delayed.to, delayed.message);
        }
    }

    /// Deliver a message to a node.
    fn deliver_message(&self, from: u64, to: u64, message: SimMessage) {
        let node = match self.nodes.get(&to) {
            Some(n) if n.is_online() => n,
            _ => return,
        };

        match message {
            SimMessage::Gossip { id, payload, round } => {
                // Check if already delivered (deduplication)
                if !node.has_message(&id) {
                    node.deliver(id, payload.clone());

                    // Forward to eager peers (except sender)
                    let eager_peers = node.eager_peers.read().clone();
                    for peer in eager_peers {
                        if peer != from {
                            self.send_message(
                                to,
                                peer,
                                SimMessage::Gossip {
                                    id,
                                    payload: payload.clone(),
                                    round: round + 1,
                                },
                            );
                        }
                    }

                    // Send IHave to lazy peers (except sender)
                    let lazy_peers = node.lazy_peers.read().clone();
                    for peer in lazy_peers {
                        if peer != from {
                            self.send_message(to, peer, SimMessage::IHave { id, round });
                        }
                    }
                }
            }
            SimMessage::IHave { id, round: _ } => {
                // If we don't have the message, graft
                if !node.has_message(&id) {
                    self.send_message(to, from, SimMessage::Graft { id });
                }
            }
            SimMessage::Graft { id } => {
                // Promote sender to eager (if not already)
                let mut eager = node.eager_peers.write();
                if !eager.contains(&from) {
                    eager.push(from);
                    // Remove from lazy if present
                    node.lazy_peers.write().retain(|&p| p != from);
                }
                drop(eager); // Release lock before sending

                // Send the message back if we have it
                if let Some((_, payload)) = node
                    .delivered
                    .read()
                    .iter()
                    .find(|(mid, _)| *mid == id)
                    .cloned()
                {
                    self.send_message(
                        to,
                        from,
                        SimMessage::Gossip {
                            id,
                            payload,
                            round: 0,
                        },
                    );
                }
            }
            SimMessage::Prune => {
                // Demote sender to lazy
                node.eager_peers.write().retain(|&p| p != from);
                if !node.lazy_peers.read().contains(&from) {
                    node.lazy_peers.write().push(from);
                }
            }
        }
    }

    /// Run ticks until no more messages are in flight or timeout.
    pub fn run_until_quiet(&self, timeout: Duration) {
        let start = std::time::Instant::now();
        loop {
            self.tick();

            if self.in_flight.read().is_empty() && self.chaos.reorderer.queue_len() == 0 {
                break;
            }

            if start.elapsed() > timeout {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Count how many nodes received a specific message.
    pub fn delivery_count(&self, id: &MessageId) -> usize {
        self.nodes.values().filter(|n| n.has_message(id)).count()
    }

    /// Check if all online nodes received a message.
    pub fn all_online_received(&self, id: &MessageId) -> bool {
        self.nodes
            .values()
            .filter(|n| n.is_online())
            .all(|n| n.has_message(id))
    }

    /// Get the total number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the number of online nodes.
    pub fn online_count(&self) -> usize {
        self.nodes.values().filter(|n| n.is_online()).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chaos_cluster_creation() {
        let cluster = ChaosTestCluster::new(5);
        assert_eq!(cluster.node_count(), 5);
        assert_eq!(cluster.online_count(), 5);
    }

    #[test]
    fn test_basic_broadcast() {
        let cluster = ChaosTestCluster::new(5);

        let id = cluster.broadcast(0, Bytes::from("hello")).unwrap();

        // Run until messages are delivered
        cluster.run_until_quiet(Duration::from_secs(1));

        // All nodes should have received the message
        assert!(cluster.all_online_received(&id));
    }

    #[test]
    fn test_offline_node() {
        let cluster = ChaosTestCluster::new(5);

        // Take node 2 offline
        cluster.node(2).unwrap().go_offline();

        let id = cluster.broadcast(0, Bytes::from("hello")).unwrap();
        cluster.run_until_quiet(Duration::from_secs(1));

        // Node 2 should not have the message
        assert!(!cluster.node(2).unwrap().has_message(&id));

        // But all other online nodes should
        assert_eq!(cluster.delivery_count(&id), 4);
    }
}
