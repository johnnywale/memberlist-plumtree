//! Pub/Sub example using PlumtreeDiscovery.
//!
//! This example demonstrates a topic-based publish/subscribe system
//! built on top of PlumtreeDiscovery for efficient O(n) message broadcast.
//!
//! Features demonstrated:
//! - Topic-based message routing
//! - Message serialization/deserialization
//! - Subscriber management
//! - Statistics and monitoring
//! - Graceful shutdown
//!
//! Run with: cargo run --example pubsub

use bytes::Bytes;
use memberlist_plumtree::{
    CacheStats, MessageId, PeerStats, PlumtreeConfig, PlumtreeDelegate, PlumtreeDiscovery,
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::RwLock;

// ============================================================================
// Node Identifier (using u64 for simplicity with nodecraft::Id)
// ============================================================================

type NodeId = u64;

fn node_name(id: NodeId) -> &'static str {
    match id {
        1 => "broker-1",
        2 => "broker-2",
        3 => "broker-3",
        4 => "client-a",
        5 => "client-b",
        _ => "unknown",
    }
}

// ============================================================================
// Pub/Sub Message Types
// ============================================================================

/// Topic identifier for pub/sub routing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Topic(String);

impl Topic {
    fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A pub/sub message with topic and payload.
#[derive(Debug, Clone)]
struct PubSubMessage {
    /// Message topic for routing.
    topic: Topic,
    /// Publisher node ID.
    publisher: NodeId,
    /// Message sequence number (per publisher).
    sequence: u64,
    /// Message payload.
    payload: Vec<u8>,
    /// Timestamp (milliseconds since epoch).
    timestamp: u64,
}

impl PubSubMessage {
    fn new(topic: Topic, publisher: NodeId, sequence: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            topic,
            publisher,
            sequence,
            payload: payload.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// Encode message to bytes for transmission.
    fn encode(&self) -> Bytes {
        // Simple encoding: topic_len|topic|publisher|sequence|timestamp|payload
        let topic_bytes = self.topic.0.as_bytes();

        let mut buf = Vec::with_capacity(2 + topic_bytes.len() + 8 + 8 + 8 + self.payload.len());

        // Topic
        buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic_bytes);

        // Publisher (u64)
        buf.extend_from_slice(&self.publisher.to_be_bytes());

        // Sequence
        buf.extend_from_slice(&self.sequence.to_be_bytes());

        // Timestamp
        buf.extend_from_slice(&self.timestamp.to_be_bytes());

        // Payload
        buf.extend_from_slice(&self.payload);

        Bytes::from(buf)
    }

    /// Decode message from bytes.
    fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 4 {
            return None;
        }

        let mut pos = 0;

        // Topic
        let topic_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;
        if pos + topic_len > data.len() {
            return None;
        }
        let topic = Topic(String::from_utf8_lossy(&data[pos..pos + topic_len]).to_string());
        pos += topic_len;

        // Publisher
        if pos + 8 > data.len() {
            return None;
        }
        let publisher = u64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // Sequence
        if pos + 8 > data.len() {
            return None;
        }
        let sequence = u64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // Timestamp
        if pos + 8 > data.len() {
            return None;
        }
        let timestamp = u64::from_be_bytes(data[pos..pos + 8].try_into().ok()?);
        pos += 8;

        // Payload
        let payload = data[pos..].to_vec();

        Some(Self {
            topic,
            publisher,
            sequence,
            timestamp,
            payload,
        })
    }
}

// ============================================================================
// Pub/Sub Statistics
// ============================================================================

/// Statistics for a pub/sub node.
#[allow(dead_code)]
#[derive(Debug, Default)]
struct PubSubStats {
    messages_published: AtomicU64,
    messages_received: AtomicU64,
    messages_filtered: AtomicU64,
    bytes_published: AtomicU64,
    bytes_received: AtomicU64,
}

#[allow(dead_code)]
impl PubSubStats {
    fn record_publish(&self, size: usize) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
        self.bytes_published
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    fn record_receive(&self, size: usize) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    fn record_filtered(&self) {
        self.messages_filtered.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            messages_published: self.messages_published.load(Ordering::Relaxed),
            messages_received: self.messages_received.load(Ordering::Relaxed),
            messages_filtered: self.messages_filtered.load(Ordering::Relaxed),
            bytes_published: self.bytes_published.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct StatsSnapshot {
    messages_published: u64,
    messages_received: u64,
    messages_filtered: u64,
    bytes_published: u64,
    bytes_received: u64,
}

// ============================================================================
// Pub/Sub Delegate
// ============================================================================

/// Delegate that handles pub/sub message delivery.
#[allow(dead_code)]
struct PubSubDelegate {
    /// Node ID for this delegate.
    node_id: NodeId,
    /// Subscribed topics.
    subscriptions: RwLock<HashSet<Topic>>,
    /// Received messages (for inspection).
    messages: RwLock<Vec<PubSubMessage>>,
    /// Statistics.
    stats: Arc<PubSubStats>,
}

#[allow(dead_code)]
impl PubSubDelegate {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            subscriptions: RwLock::new(HashSet::new()),
            messages: RwLock::new(Vec::new()),
            stats: Arc::new(PubSubStats::default()),
        }
    }

    fn stats(&self) -> Arc<PubSubStats> {
        self.stats.clone()
    }

    async fn subscribe(&self, topic: Topic) {
        self.subscriptions.write().await.insert(topic);
    }

    async fn unsubscribe(&self, topic: &Topic) {
        self.subscriptions.write().await.remove(topic);
    }

    async fn is_subscribed(&self, topic: &Topic) -> bool {
        self.subscriptions.read().await.contains(topic)
    }

    async fn get_messages(&self) -> Vec<PubSubMessage> {
        self.messages.read().await.clone()
    }

    async fn store_message(&self, msg: PubSubMessage) {
        self.messages.write().await.push(msg);
    }
}

impl PlumtreeDelegate<NodeId> for PubSubDelegate {
    fn on_deliver(&self, _message_id: MessageId, payload: Bytes) {
        // Decode the pub/sub message
        let Some(msg) = PubSubMessage::decode(&payload) else {
            println!(
                "[{}] Failed to decode pub/sub message",
                node_name(self.node_id)
            );
            return;
        };

        let node_id = self.node_id;
        let stats = self.stats.clone();
        let topic = msg.topic.clone();
        let payload_len = payload.len();
        let publisher = msg.publisher;
        let sequence = msg.sequence;
        let payload_size = msg.payload.len();

        // Record stats
        stats.record_receive(payload_len);

        println!(
            "[{}] Received on '{}' from {}: {} bytes (seq: {})",
            node_name(node_id),
            topic,
            node_name(publisher),
            payload_size,
            sequence
        );
    }

    fn on_eager_promotion(&self, _peer: &NodeId) {
        // Peer promoted to eager (tree edge established)
    }

    fn on_lazy_demotion(&self, _peer: &NodeId) {
        // Peer demoted to lazy (tree edge removed)
    }

    fn on_graft_sent(&self, _peer: &NodeId, _message_id: &MessageId) {
        // Graft request sent (tree repair)
    }

    fn on_prune_sent(&self, _peer: &NodeId) {
        // Prune sent (tree optimization)
    }
}

// ============================================================================
// Pub/Sub Node
// ============================================================================

/// A pub/sub node using PlumtreeDiscovery.
struct PubSubNode {
    /// Node identifier.
    id: NodeId,
    /// PlumtreeDiscovery instance.
    memberlist: PlumtreeDiscovery<NodeId, Arc<PubSubDelegate>>,
    /// Delegate for message handling.
    delegate: Arc<PubSubDelegate>,
    /// Sequence number for published messages.
    sequence: AtomicU64,
}

#[allow(dead_code)]
impl PubSubNode {
    /// Create a new pub/sub node.
    fn new(id: NodeId, config: PlumtreeConfig) -> Self {
        let delegate = Arc::new(PubSubDelegate::new(id));
        let memberlist = PlumtreeDiscovery::new(id, config, delegate.clone());

        Self {
            id,
            memberlist,
            delegate,
            sequence: AtomicU64::new(0),
        }
    }

    /// Get the node ID.
    fn id(&self) -> NodeId {
        self.id
    }

    /// Add a peer node.
    fn add_peer(&self, peer: NodeId) {
        self.memberlist.add_peer(peer);
    }

    /// Remove a peer node.
    fn remove_peer(&self, peer: &NodeId) {
        self.memberlist.remove_peer(peer);
    }

    /// Subscribe to a topic.
    async fn subscribe(&self, topic: Topic) {
        self.delegate.subscribe(topic).await;
    }

    /// Unsubscribe from a topic.
    async fn unsubscribe(&self, topic: &Topic) {
        self.delegate.unsubscribe(topic).await;
    }

    /// Publish a message to a topic.
    async fn publish(
        &self,
        topic: Topic,
        payload: impl Into<Vec<u8>>,
    ) -> Result<MessageId, memberlist_plumtree::Error> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let msg = PubSubMessage::new(topic, self.id, sequence, payload);
        let encoded = msg.encode();

        self.delegate.stats.record_publish(encoded.len());
        self.memberlist.broadcast(encoded).await
    }

    /// Get peer statistics.
    fn peer_stats(&self) -> PeerStats {
        self.memberlist.peer_stats()
    }

    /// Get cache statistics.
    fn cache_stats(&self) -> CacheStats {
        self.memberlist.cache_stats()
    }

    /// Get pub/sub statistics.
    fn pubsub_stats(&self) -> StatsSnapshot {
        self.delegate.stats.snapshot()
    }

    /// Get received messages.
    async fn received_messages(&self) -> Vec<PubSubMessage> {
        self.delegate.get_messages().await
    }

    /// Shutdown the node.
    fn shutdown(&self) {
        self.memberlist.shutdown();
    }

    /// Check if shutdown.
    fn is_shutdown(&self) -> bool {
        self.memberlist.is_shutdown()
    }
}

// ============================================================================
// Main Example
// ============================================================================

#[tokio::main]
async fn main() {
    println!("using PlumtreeDiscovery for efficient O(n) broadcast.\n");

    // Create configuration
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(4)
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(100))
        .with_message_cache_ttl(Duration::from_secs(30))
        // New: Peer limit configuration
        .with_protect_ring_neighbors(false) // Small cluster, no need for protection
        .with_max_eager_peers(3) // Cap eager peers for this demo
        .with_max_lazy_peers(10); // Cap lazy peers

    // Create pub/sub nodes
    // IDs: 1=broker-1, 2=broker-2, 3=broker-3, 4=client-a, 5=client-b
    let nodes: Vec<PubSubNode> = vec![
        PubSubNode::new(1, config.clone()),
        PubSubNode::new(2, config.clone()),
        PubSubNode::new(3, config.clone()),
        PubSubNode::new(4, config.clone()),
        PubSubNode::new(5, config.clone()),
    ];

    println!("Created {} pub/sub nodes:", nodes.len());
    for node in &nodes {
        println!("  - {} (id: {})", node_name(node.id()), node.id());
    }
    println!();

    // Connect nodes in a mesh
    for i in 0..nodes.len() {
        for j in 0..nodes.len() {
            if i != j {
                nodes[i].add_peer(nodes[j].id());
            }
        }
    }

    println!("Connected nodes in mesh topology\n");

    // Define topics
    let topic_orders = Topic::new("orders");
    let topic_inventory = Topic::new("inventory");
    let topic_notifications = Topic::new("notifications");

    // Subscribe nodes to topics
    // Brokers subscribe to all topics
    for node in &nodes[0..3] {
        node.subscribe(topic_orders.clone()).await;
        node.subscribe(topic_inventory.clone()).await;
        node.subscribe(topic_notifications.clone()).await;
    }

    // Client A subscribes to orders and notifications
    nodes[3].subscribe(topic_orders.clone()).await;
    nodes[3].subscribe(topic_notifications.clone()).await;

    // Client B subscribes to inventory only
    nodes[4].subscribe(topic_inventory.clone()).await;

    println!("Subscription setup:");
    println!("  broker-1, broker-2, broker-3: all topics");
    println!("  client-a: orders, notifications");
    println!("  client-b: inventory\n");

    // Publish messages
    println!("Publishing messages:\n");

    // Client A publishes an order
    let msg_id = nodes[3]
        .publish(topic_orders.clone(), b"ORDER-001: Buy 100 units".to_vec())
        .await
        .unwrap();
    println!(
        "[client-a] Published to 'orders': ORDER-001 (id: {}...)",
        &msg_id.to_string()[..8]
    );

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Broker-1 publishes inventory update
    let msg_id = nodes[0]
        .publish(
            topic_inventory.clone(),
            b"INVENTORY: Widget stock = 500".to_vec(),
        )
        .await
        .unwrap();
    println!(
        "[broker-1] Published to 'inventory': stock update (id: {}...)",
        &msg_id.to_string()[..8]
    );

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Broker-2 publishes notification
    let msg_id = nodes[1]
        .publish(
            topic_notifications.clone(),
            b"ALERT: System maintenance at 02:00 UTC".to_vec(),
        )
        .await
        .unwrap();
    println!(
        "[broker-2] Published to 'notifications': maintenance alert (id: {}...)",
        &msg_id.to_string()[..8]
    );

    tokio::time::sleep(Duration::from_millis(10)).await;

    // Client B publishes inventory request
    let msg_id = nodes[4]
        .publish(
            topic_inventory.clone(),
            b"REQUEST: Check Gadget availability".to_vec(),
        )
        .await
        .unwrap();
    println!(
        "[client-b] Published to 'inventory': availability request (id: {}...)",
        &msg_id.to_string()[..8]
    );

    // Wait for message propagation
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n=== Node Statistics ===\n");

    for node in &nodes {
        let peer_stats = node.peer_stats();
        let cache_stats = node.cache_stats();
        let pubsub_stats = node.pubsub_stats();

        println!("{}:", node_name(node.id()));
        println!(
            "  Peers: {} total ({} eager, {} lazy)",
            peer_stats.total(),
            peer_stats.eager_count,
            peer_stats.lazy_count
        );
        println!(
            "  Cache: {} entries, TTL {:?}",
            cache_stats.entries, cache_stats.ttl
        );
        println!(
            "  Pub/Sub: {} published, {} received",
            pubsub_stats.messages_published, pubsub_stats.messages_received,
        );
        println!(
            "  Bytes: {} out, {} in",
            pubsub_stats.bytes_published, pubsub_stats.bytes_received
        );
        println!();
    }

    // Demonstrate dynamic subscription changes
    println!("=== Dynamic Subscription Changes ===\n");

    // Client B subscribes to notifications
    println!("[client-b] Subscribing to 'notifications'...");
    nodes[4].subscribe(topic_notifications.clone()).await;

    // Broker-3 publishes another notification
    let msg_id = nodes[2]
        .publish(
            topic_notifications.clone(),
            b"INFO: New feature released!".to_vec(),
        )
        .await
        .unwrap();
    println!(
        "[broker-3] Published to 'notifications': feature announcement (id: {}...)",
        &msg_id.to_string()[..8]
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Client A unsubscribes from orders
    println!("[client-a] Unsubscribing from 'orders'...");
    nodes[3].unsubscribe(&topic_orders).await;

    // Demonstrate peer removal
    println!("\n=== Peer Removal ===\n");

    // Remove broker-3 from all nodes (simulating node failure)
    println!("Simulating broker-3 leaving the cluster...");
    let broker3_id = nodes[2].id();
    for node in &nodes {
        if node.id() != broker3_id {
            node.remove_peer(&broker3_id);
        }
    }

    // Publish after peer removal
    let msg_id = nodes[0]
        .publish(topic_orders.clone(), b"ORDER-002: Sell 50 units".to_vec())
        .await
        .unwrap();
    println!(
        "[broker-1] Published to 'orders' after broker-3 left (id: {}...)",
        &msg_id.to_string()[..8]
    );

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Final statistics
    println!("\n=== Final Statistics ===\n");

    for node in &nodes {
        let peer_stats = node.peer_stats();
        let pubsub_stats = node.pubsub_stats();

        println!(
            "{}: {} peers, {} published, {} received",
            node_name(node.id()),
            peer_stats.total(),
            pubsub_stats.messages_published,
            pubsub_stats.messages_received
        );
    }

    // Cleanup
    println!("\n=== Shutting Down ===\n");

    for node in &nodes {
        node.shutdown();
        println!("[{}] Shutdown complete", node_name(node.id()));
    }

    println!("\n=== Pub/Sub Example Complete ===");
}
