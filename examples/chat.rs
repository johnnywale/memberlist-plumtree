//! Chat example demonstrating Plumtree multicast messaging.
//!
//! This example creates multiple nodes in a simulated network and demonstrates:
//! - Multicast message broadcasting via Plumtree spanning trees
//! - Tree optimization (eager/lazy peer management)
//! - Message deduplication
//! - Graft/Prune tree repair
//! - Rate limiting and backoff
//!
//! Run with: cargo run --example chat

use bytes::Bytes;
use memberlist_plumtree::{
    CacheStats, MessageId, PeerState, PeerStateBuilder, PeerStats, Plumtree, PlumtreeConfig,
    PlumtreeDelegate, PlumtreeHandle, PlumtreeMessage,
};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, RwLock};

/// Simple node identifier for the chat example.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct NodeId(u32);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node{}", self.0)
    }
}

/// Chat message payload.
#[derive(Debug, Clone)]
struct ChatMessage {
    from: NodeId,
    text: String,
    timestamp: u64,
}

impl ChatMessage {
    fn new(from: NodeId, text: impl Into<String>) -> Self {
        Self {
            from,
            text: text.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    fn encode(&self) -> Bytes {
        let data = format!("{}|{}|{}", self.from.0, self.timestamp, self.text);
        Bytes::from(data)
    }

    fn decode(data: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(data).ok()?;
        let mut parts = s.splitn(3, '|');
        let from = parts.next()?.parse().ok()?;
        let timestamp = parts.next()?.parse().ok()?;
        let text = parts.next()?.to_string();
        Some(Self {
            from: NodeId(from),
            timestamp,
            text,
        })
    }
}

/// Chat delegate that handles message delivery.
struct ChatDelegate {
    node_id: NodeId,
    messages: Arc<RwLock<Vec<(MessageId, ChatMessage)>>>,
    delivery_count: AtomicUsize,
    eager_promotions: AtomicUsize,
    lazy_demotions: AtomicUsize,
    grafts_sent: AtomicUsize,
    prunes_sent: AtomicUsize,
}

impl ChatDelegate {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            messages: Arc::new(RwLock::new(Vec::new())),
            delivery_count: AtomicUsize::new(0),
            eager_promotions: AtomicUsize::new(0),
            lazy_demotions: AtomicUsize::new(0),
            grafts_sent: AtomicUsize::new(0),
            prunes_sent: AtomicUsize::new(0),
        }
    }

    fn messages(&self) -> Arc<RwLock<Vec<(MessageId, ChatMessage)>>> {
        self.messages.clone()
    }

    fn stats(&self) -> DelegateStats {
        DelegateStats {
            deliveries: self.delivery_count.load(Ordering::Relaxed),
            eager_promotions: self.eager_promotions.load(Ordering::Relaxed),
            lazy_demotions: self.lazy_demotions.load(Ordering::Relaxed),
            grafts_sent: self.grafts_sent.load(Ordering::Relaxed),
            prunes_sent: self.prunes_sent.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
struct DelegateStats {
    deliveries: usize,
    eager_promotions: usize,
    lazy_demotions: usize,
    grafts_sent: usize,
    prunes_sent: usize,
}

impl PlumtreeDelegate for ChatDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        self.delivery_count.fetch_add(1, Ordering::Relaxed);
        if let Some(chat_msg) = ChatMessage::decode(&payload) {
            println!(
                "[{}] Received from {}: \"{}\"",
                self.node_id, chat_msg.from, chat_msg.text
            );
            let messages = self.messages.clone();
            // Store message (blocking in this simple example)
            tokio::spawn(async move {
                messages.write().await.push((message_id, chat_msg));
            });
        }
    }

    fn on_eager_promotion(&self, _peer: &[u8]) {
        self.eager_promotions.fetch_add(1, Ordering::Relaxed);
    }

    fn on_lazy_demotion(&self, _peer: &[u8]) {
        self.lazy_demotions.fetch_add(1, Ordering::Relaxed);
    }

    fn on_graft_sent(&self, _peer: &[u8], _message_id: &MessageId) {
        self.grafts_sent.fetch_add(1, Ordering::Relaxed);
    }

    fn on_prune_sent(&self, _peer: &[u8]) {
        self.prunes_sent.fetch_add(1, Ordering::Relaxed);
    }
}

/// Simulated network for connecting nodes.
struct SimulatedNetwork {
    /// Channels for each node to receive messages.
    channels: HashMap<NodeId, mpsc::Sender<(NodeId, PlumtreeMessage)>>,
}

impl SimulatedNetwork {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }

    fn register(&mut self, node_id: NodeId, tx: mpsc::Sender<(NodeId, PlumtreeMessage)>) {
        self.channels.insert(node_id, tx);
    }

    async fn send(&self, from: NodeId, to: NodeId, message: PlumtreeMessage) {
        if let Some(tx) = self.channels.get(&to) {
            let _ = tx.send((from, message)).await;
        }
    }
}

/// A chat node in the network.
struct ChatNode {
    id: NodeId,
    plumtree: Plumtree<NodeId, Arc<ChatDelegate>>,
    delegate: Arc<ChatDelegate>,
    rx: mpsc::Receiver<(NodeId, PlumtreeMessage)>,
}

impl ChatNode {
    fn new(id: NodeId, config: PlumtreeConfig, network: &mut SimulatedNetwork) -> Self {
        let delegate = Arc::new(ChatDelegate::new(id));
        let (plumtree, _handle) = Plumtree::new(id, config, delegate.clone());

        let (tx, rx) = mpsc::channel(1024);
        network.register(id, tx);

        Self {
            id,
            plumtree,
            delegate,
            rx,
        }
    }

    fn add_peer(&self, peer: NodeId) {
        self.plumtree.add_peer(peer);
    }

    async fn broadcast(&self, text: &str) -> Result<MessageId, memberlist_plumtree::Error> {
        let msg = ChatMessage::new(self.id, text);
        self.plumtree.broadcast(msg.encode()).await
    }

    fn peer_stats(&self) -> PeerStats {
        self.plumtree.peer_stats()
    }

    fn cache_stats(&self) -> CacheStats {
        self.plumtree.cache_stats()
    }

    fn delegate_stats(&self) -> DelegateStats {
        self.delegate.stats()
    }

    async fn messages(&self) -> Vec<(MessageId, ChatMessage)> {
        self.delegate.messages().read().await.clone()
    }
}

/// Demonstrates PeerStateBuilder usage for creating peer state with initial config.
fn demonstrate_peer_state_builder() {
    println!("\n=== PeerStateBuilder Demo ===");

    // Use the PeerStateBuilder to create peer state with initial peers
    let peer_state: PeerState<NodeId> = PeerStateBuilder::new()
        .with_eager_fanout(4)
        .with_lazy_fanout(8)
        .with_peers([NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)])
        .build();

    let stats = peer_state.stats();
    println!("Created PeerState with {} peers", stats.total());
    println!("  Eager peers: {}", stats.eager_count);
    println!("  Lazy peers: {}", stats.lazy_count);
}

/// Demonstrates rate limiter usage.
fn demonstrate_rate_limiter() {
    use memberlist_plumtree::RateLimiter;

    println!("\n=== RateLimiter Demo ===");

    // Create rate limiter with custom cleanup interval
    let limiter: RateLimiter<NodeId> =
        RateLimiter::new(5, 2.0).with_cleanup_interval(Duration::from_secs(30));

    let node = NodeId(1);

    // Check tokens for a node
    println!("Initial tokens for {}: {:.2}", node, limiter.tokens(&node));

    // Consume some tokens
    for i in 0..7 {
        let allowed = limiter.check(&node);
        println!(
            "Request {}: {}",
            i + 1,
            if allowed { "allowed" } else { "denied" }
        );
    }

    // Check remaining tokens
    println!("Remaining tokens: {:.2}", limiter.tokens(&node));

    // Reset tokens
    limiter.reset(&node);
    println!("After reset: {:.2}", limiter.tokens(&node));

    // Clear all entries
    limiter.clear();
    println!("After clear: all entries removed");

    // Demonstrate check_n for batch operations
    let limiter2: RateLimiter<NodeId> = RateLimiter::new(10, 5.0);
    let batch_allowed = limiter2.check_n(&NodeId(2), 5);
    println!(
        "Batch check (5 tokens): {}",
        if batch_allowed { "allowed" } else { "denied" }
    );
}

/// Demonstrates global rate limiter usage.
fn demonstrate_global_rate_limiter() {
    use memberlist_plumtree::GlobalRateLimiter;

    println!("\n=== GlobalRateLimiter Demo ===");

    let limiter = GlobalRateLimiter::new(10, 5.0);

    println!("Initial tokens: {:.2}", limiter.tokens());

    // Consume tokens
    for i in 0..12 {
        let allowed = limiter.check();
        println!(
            "Global request {}: {}",
            i + 1,
            if allowed { "allowed" } else { "denied" }
        );
    }

    // Check batch
    let batch_allowed = limiter.check_n(3);
    println!(
        "Batch check (3 tokens): {}",
        if batch_allowed { "allowed" } else { "denied" }
    );

    // Reset
    limiter.reset();
    println!("After reset: {:.2} tokens", limiter.tokens());
}

/// Demonstrates IHave scheduler internals.
fn demonstrate_scheduler() {
    use memberlist_plumtree::IHaveScheduler;

    println!("\n=== IHaveScheduler Demo ===");

    let scheduler = IHaveScheduler::new(Duration::from_millis(100), 16, 1000);

    println!("Scheduler configuration:");
    println!("  Interval: {:?}", scheduler.interval());
    println!("  Batch size: {}", scheduler.batch_size());
    println!("  Is shutdown: {}", scheduler.is_shutdown());

    // Access the queue
    let queue = scheduler.queue();
    println!("\nQueue state:");
    println!("  Length: {}", queue.len());
    println!("  Is empty: {}", queue.is_empty());

    // Push some items
    for i in 0..5 {
        let id = MessageId::new();
        let pushed = queue.push(id, i);
        println!("  Pushed message {}: {}", i, pushed);
    }

    println!("  Length after pushes: {}", queue.len());

    // Stop and resume
    queue.stop();
    let pushed_while_stopped = queue.push(MessageId::new(), 99);
    println!("  Push while stopped: {}", pushed_while_stopped);

    queue.resume();
    let pushed_after_resume = queue.push(MessageId::new(), 100);
    println!("  Push after resume: {}", pushed_after_resume);

    // Clear
    queue.clear();
    println!("  Length after clear: {}", queue.len());

    // Shutdown scheduler
    scheduler.shutdown();
    println!(
        "  Is shutdown after shutdown(): {}",
        scheduler.is_shutdown()
    );
}

/// Demonstrates GraftTimer with backoff.
fn demonstrate_graft_timer() {
    use memberlist_plumtree::GraftTimer;

    println!("\n=== GraftTimer Demo ===");

    // Create timer with custom backoff settings
    let timer = GraftTimer::with_backoff(
        Duration::from_millis(100), // base timeout
        Duration::from_millis(800), // max timeout
        5,                          // max retries
    );

    println!("Timer configuration:");
    println!("  Base timeout: {:?}", timer.base_timeout());
    println!("  Max retries: {}", timer.max_retries());
    println!("  Pending count: {}", timer.pending_count());

    // Expect some messages
    let msg1 = MessageId::new();
    let msg2 = MessageId::new();

    timer.expect_message(msg1, vec![1, 2, 3], 0);
    timer.expect_message_with_alternatives(
        msg2,
        vec![4, 5, 6],
        vec![vec![7, 8, 9], vec![10, 11, 12]],
        1,
    );

    println!("\nAfter expecting 2 messages:");
    println!("  Pending count: {}", timer.pending_count());

    // Get expired (none yet since timeout hasn't elapsed)
    let expired = timer.get_expired();
    println!("  Expired immediately: {}", expired.len());

    // Use the simple API
    let expired_simple = timer.get_expired_simple();
    println!("  Expired (simple API): {}", expired_simple.len());

    // Clear all pending
    timer.clear();
    println!("  Pending after clear: {}", timer.pending_count());
}

/// Demonstrates PlumtreeHandle incoming_rx usage.
async fn demonstrate_handle_incoming() {
    println!("\n=== PlumtreeHandle Demo ===");

    let delegate = Arc::new(ChatDelegate::new(NodeId(99)));
    let (plumtree, handle): (Plumtree<NodeId, _>, PlumtreeHandle<NodeId>) =
        Plumtree::new(NodeId(99), PlumtreeConfig::default(), delegate);

    // Add peers
    plumtree.add_peer(NodeId(1));
    plumtree.add_peer(NodeId(2));

    // Submit an incoming message via the handle
    let msg = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 0,
        payload: Bytes::from("test payload"),
    };

    // Submit incoming message
    handle
        .submit_incoming(NodeId(1), msg.clone())
        .await
        .unwrap();
    println!("Submitted incoming message via handle");

    // Check if handle is closed
    println!("Handle is closed: {}", handle.is_closed());

    // Shutdown closes the handle
    plumtree.shutdown();
    println!("After shutdown, handle is closed: {}", handle.is_closed());
}

#[tokio::main]
async fn main() {
    println!("=== Plumtree Chat Example ===\n");
    println!("This example demonstrates multicast messaging using Plumtree protocol.\n");

    // Demonstrate various unused APIs
    demonstrate_peer_state_builder();
    demonstrate_rate_limiter();
    demonstrate_global_rate_limiter();
    demonstrate_scheduler();
    demonstrate_graft_timer();
    demonstrate_handle_incoming().await;

    println!("\n==================================================");
    println!("=== Starting Chat Network Simulation ===");
    println!("==================================================\n");

    // Create simulated network
    let mut network = SimulatedNetwork::new();

    // Create chat configuration optimized for fast demo
    let config = PlumtreeConfig::default()
        .with_eager_fanout(2)
        .with_lazy_fanout(3)
        .with_ihave_interval(Duration::from_millis(50))
        .with_graft_timeout(Duration::from_millis(100))
        .with_message_cache_ttl(Duration::from_secs(30))
        .with_graft_rate_limit_per_second(100.0)
        .with_graft_rate_limit_burst(50)
        .with_graft_max_retries(3);

    // Create 5 chat nodes
    let num_nodes = 5;
    let mut nodes: Vec<ChatNode> = Vec::new();

    for i in 0..num_nodes {
        let node = ChatNode::new(NodeId(i), config.clone(), &mut network);
        nodes.push(node);
    }

    println!("Created {} chat nodes", num_nodes);

    // Connect nodes in a mesh (each node knows all others)
    for i in 0..num_nodes {
        for j in 0..num_nodes {
            if i != j {
                nodes[i as usize].add_peer(NodeId(j));
            }
        }
    }

    println!("Connected nodes in mesh topology\n");

    // Print initial peer state
    for node in &nodes {
        let stats = node.peer_stats();
        println!(
            "{}: {} eager, {} lazy peers",
            node.id, stats.eager_count, stats.lazy_count
        );
    }

    println!();

    // Simulate chat messages
    let messages = [
        (0, "Hello everyone!"),
        (2, "Hi Node0, how are you?"),
        (4, "Great to see the network working!"),
        (1, "Plumtree is efficient for broadcast."),
        (3, "Indeed, O(n) message complexity!"),
    ];

    let network = Arc::new(tokio::sync::RwLock::new(network));

    // Process messages for each node (run in background)
    let mut handles = Vec::new();
    for _ in 0..num_nodes {
        let mut node = nodes.remove(0);
        let _net = network.clone();

        let handle = tokio::spawn(async move {
            // Process incoming messages
            let mut processed = 0;
            loop {
                match tokio::time::timeout(Duration::from_millis(500), node.rx.recv()).await {
                    Ok(Some((from, msg))) => {
                        // Handle the message
                        if let Err(e) = node.plumtree.handle_message(from, msg).await {
                            eprintln!("[{}] Error handling message: {}", node.id, e);
                        }
                        processed += 1;
                    }
                    Ok(None) => break, // Channel closed
                    Err(_) => break,   // Timeout
                }
            }
            (node, processed)
        });
        handles.push(handle);
    }

    // Wait a bit then shutdown
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send chat messages
    println!("Sending chat messages:\n");

    // We need to get the nodes back to send messages
    // For demo purposes, let's create fresh nodes and show the concept

    let mut demo_network = SimulatedNetwork::new();
    let mut demo_nodes: Vec<ChatNode> = Vec::new();

    for i in 0..num_nodes {
        let node = ChatNode::new(NodeId(i), config.clone(), &mut demo_network);
        demo_nodes.push(node);
    }

    // Connect in mesh
    for i in 0..num_nodes {
        for j in 0..num_nodes {
            if i != j {
                demo_nodes[i as usize].add_peer(NodeId(j));
            }
        }
    }

    // Broadcast messages
    for (sender_idx, text) in &messages {
        let node = &demo_nodes[*sender_idx as usize];
        match node.broadcast(text).await {
            Ok(msg_id) => {
                println!(
                    "[{}] Broadcast: \"{}\" (id: {}...)",
                    node.id,
                    text,
                    &msg_id.to_string()[..8]
                );
            }
            Err(e) => {
                eprintln!("[{}] Failed to broadcast: {}", node.id, e);
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    println!();

    // Print final statistics
    println!("=== Final Statistics ===\n");

    for node in &demo_nodes {
        let peer_stats = node.peer_stats();
        let cache_stats = node.cache_stats();
        let delegate_stats = node.delegate_stats();

        println!("{}:", node.id);
        println!(
            "  Peers: {} eager, {} lazy",
            peer_stats.eager_count, peer_stats.lazy_count
        );
        println!(
            "  Cache: {} entries, capacity {}",
            cache_stats.entries, cache_stats.capacity
        );
        println!(
            "  Events: {} deliveries, {} promotions, {} demotions",
            delegate_stats.deliveries,
            delegate_stats.eager_promotions,
            delegate_stats.lazy_demotions
        );
        println!(
            "  Repair: {} grafts, {} prunes",
            delegate_stats.grafts_sent, delegate_stats.prunes_sent
        );
    }

    // Cleanup
    for node in &demo_nodes {
        node.plumtree.shutdown();
    }

    println!("\n=== Chat Example Complete ===");
}
