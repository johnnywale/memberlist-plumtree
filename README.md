# memberlist-plumtree

[![Crates.io](https://img.shields.io/crates/v/memberlist-plumtree.svg)](https://crates.io/crates/memberlist-plumtree)
[![Documentation](https://docs.rs/memberlist-plumtree/badge.svg)](https://docs.rs/memberlist-plumtree)
[![CI](https://github.com/johnnywale/memberlist-plumtree/actions/workflows/ci.yml/badge.svg)](https://github.com/johnnywale/memberlist-plumtree/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

Plumtree (Epidemic Broadcast Trees) implementation built on top of [memberlist](https://crates.io/crates/memberlist-core) for efficient **O(n)** message broadcast in distributed systems.

## Overview

Plumtree combines the efficiency of tree-based broadcast with the reliability of gossip protocols through a hybrid push/lazy-push approach. This makes it ideal for applications requiring reliable message dissemination across large clusters with minimal network overhead.

### Key Features

- **O(n) Message Complexity**: Messages traverse a spanning tree, eliminating redundant transmissions
- **Self-Healing**: Automatic tree repair when nodes fail or network partitions occur
- **Protocol Optimization**: Dynamic eager/lazy peer management for optimal broadcast paths
- **Rate Limiting**: Built-in per-peer rate limiting with token bucket algorithm
- **Exponential Backoff**: Configurable retry logic with exponential backoff for Graft requests
- **Runtime Agnostic**: Works with Tokio, async-std, or other async runtimes
- **Zero-Copy**: Efficient message handling using `bytes::Bytes`

## How Plumtree Works

### Key Concept: Each Node Has Its Own View

Every node maintains its own classification of peers as "eager" or "lazy". There is no global tree - the spanning tree **emerges** from each node's local decisions.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Node A's Local View                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                         [Node A]                                │
│                        /    |    \                              │
│                       /     |     \                             │
│                  eager   eager   lazy                           │
│                     /       |       \                           │
│                 [B]       [C]       [D]                         │
│                                                                 │
│  A sends GOSSIP (full message) to B and C                       │
│  A sends IHAVE (just message ID) to D                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    Node B's Local View                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                         [Node B]                                │
│                        /    |    \                              │
│                       /     |     \                             │
│                  eager   lazy    eager                          │
│                     /       |       \                           │
│                 [A]       [C]       [E]                         │
│                                                                 │
│  B has its OWN classification - different from A's view!        │
│  B sends GOSSIP to A and E, IHAVE to C                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Message Propagation Example

When Node A broadcasts a message, here's how it flows:

```
Step 1: A originates message
        A ──GOSSIP──> B (A's eager peer)
        A ──GOSSIP──> C (A's eager peer)
        A ──IHAVE───> D (A's lazy peer)

Step 2: B receives, forwards to its eager peers
        B ──GOSSIP──> E (B's eager peer)
        B ──IHAVE───> C (B's lazy peer, but C already has it)

Step 3: Tree optimization
        C received from both A and B (redundant!)
        C sends PRUNE to B → B demotes C to lazy
        Result: More efficient tree for next message
```

### Lazy Peer Behavior: Wait, Don't Pull Immediately

When a lazy peer receives IHAVE, it does **NOT** immediately request the message. Instead, it waits to see if the message arrives through its eager connections:

```
┌─────────────────────────────────────────────────────────────────┐
│           What happens when D receives IHAVE from A             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. A ──IHAVE(msg_id)──> D                                      │
│                                                                 │
│  2. D starts a timer (graft_timeout, default 500ms)             │
│     D thinks: "I'll wait to see if I get this from someone else"│
│                                                                 │
│  3a. IF D receives GOSSIP from another peer before timeout:     │
│      ✓ D cancels timer, does nothing                            │
│      (D got message through its eager connections - normal case)│
│                                                                 │
│  3b. IF timeout expires and D still doesn't have message:       │
│      ! D ──GRAFT(msg_id)──> A                                   │
│      ! A ──GOSSIP(msg)──> D   (and A promotes D to eager)       │
│      (Tree repair - D joins A's eager set for future messages)  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Example: D has its own eager peer (B)**

```
        [A]                    [B]
         │                      │
      (lazy)                (eager)
         │                      │
         └──IHAVE──> [D] <──GOSSIP──┘
                      │
              D receives from B first!
              IHAVE from A is ignored (backup not needed)
```

**Why wait instead of immediate pull?**

| Approach | Messages | Latency | Used By |
|----------|----------|---------|---------|
| Immediate pull on IHAVE | 2x messages (IHAVE + GRAFT + GOSSIP) | Higher | - |
| Wait then pull if needed | Usually just GOSSIP via eager | Lower | Plumtree |

The IHAVE/GRAFT mechanism is a **backup path**, not the primary delivery. This is what makes Plumtree achieve O(n) message complexity - most messages flow through the eager spanning tree, and lazy links only activate when needed.

### Protocol Messages

| Message | When Sent | Purpose |
|---------|-----------|---------|
| **GOSSIP** | To eager peers | Full message payload - primary delivery |
| **IHAVE** | To lazy peers | "I have message X" - backup announcement |
| **GRAFT** | When missing message | "Send me message X, add me as eager" |
| **PRUNE** | On duplicate receipt | "I got this already, demote me to lazy" |

### Why This Works

1. **Eager Push**: Full messages flow through spanning tree edges (eager peers)
2. **Lazy Push**: IHave announcements provide redundant paths for reliability
3. **Graft**: Lazy peers can request messages they missed → repairs tree
4. **Prune**: Redundant paths are removed → tree converges to efficient structure

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
memberlist-plumtree = "0.1"
```

### Feature Flags

| Feature | Description |
|---------|-------------|
| `tokio` | Enable Tokio runtime support |
| `metrics` | Enable metrics collection via the `metrics` crate |
| `serde` | Enable serialization/deserialization for config |

## Quick Start

```rust
use memberlist_plumtree::{Plumtree, PlumtreeConfig, PlumtreeDelegate, MessageId};
use bytes::Bytes;
use std::sync::Arc;

// Define a delegate to handle delivered messages
struct MyDelegate;

impl PlumtreeDelegate for MyDelegate {
    fn on_deliver(&self, msg_id: MessageId, payload: Bytes) {
        println!("Received message {}: {:?}", msg_id, payload);
    }
}

#[tokio::main]
async fn main() {
    // Create a Plumtree instance with LAN-optimized config
    let (plumtree, handle) = Plumtree::new(
        "node-1".to_string(),
        PlumtreeConfig::lan(),
        Arc::new(MyDelegate),
    );

    // Add peers discovered via memberlist
    plumtree.add_peer("node-2".to_string());
    plumtree.add_peer("node-3".to_string());

    // Broadcast a message to all nodes
    let msg_id = plumtree.broadcast(b"Hello, cluster!").await.unwrap();
    println!("Broadcast message: {}", msg_id);

    // Run background tasks (IHave scheduler, Graft timer)
    tokio::spawn({
        let pt = plumtree.clone();
        async move {
            tokio::join!(
                pt.run_ihave_scheduler(),
                pt.run_graft_timer(),
            );
        }
    });
}
```

## Configuration

### Preset Configurations

```rust
// For LAN environments (low latency, high bandwidth)
let config = PlumtreeConfig::lan();

// For WAN environments (higher latency tolerance)
let config = PlumtreeConfig::wan();

// For large clusters (1000+ nodes)
let config = PlumtreeConfig::large_cluster();
```

### Custom Configuration

```rust
use std::time::Duration;

let config = PlumtreeConfig::default()
    .with_eager_fanout(4)           // Number of eager peers
    .with_lazy_fanout(8)            // Number of lazy peers for IHave
    .with_ihave_interval(Duration::from_millis(100))
    .with_graft_timeout(Duration::from_millis(500))
    .with_message_cache_ttl(Duration::from_secs(60))
    .with_message_cache_max_size(10000)
    .with_optimization_threshold(3)
    .with_max_message_size(64 * 1024)
    .with_graft_rate_limit_per_second(10.0)
    .with_graft_rate_limit_burst(20)
    .with_graft_max_retries(5);
```

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `eager_fanout` | 3 | Number of peers receiving full messages immediately |
| `lazy_fanout` | 6 | Number of peers receiving IHave announcements |
| `ihave_interval` | 100ms | Interval for batched IHave announcements |
| `message_cache_ttl` | 60s | How long to cache messages for Graft requests |
| `message_cache_max_size` | 10000 | Maximum number of cached messages |
| `optimization_threshold` | 3 | Rounds before pruning redundant paths |
| `graft_timeout` | 500ms | Timeout before sending Graft for missing message |
| `max_message_size` | 64KB | Maximum message payload size |
| `graft_rate_limit_per_second` | 10.0 | Rate limit for Graft requests per peer |
| `graft_rate_limit_burst` | 20 | Burst capacity for Graft rate limiting |
| `graft_max_retries` | 5 | Maximum Graft retry attempts with backoff |

## Delegate Callbacks

Implement `PlumtreeDelegate` to receive protocol events:

```rust
impl PlumtreeDelegate for MyDelegate {
    // Called when a message is delivered (first time received)
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        // Process the message
    }

    // Called when a peer is promoted to eager
    fn on_eager_promotion(&self, peer: &[u8]) {
        // Peer now receives full messages
    }

    // Called when a peer is demoted to lazy
    fn on_lazy_demotion(&self, peer: &[u8]) {
        // Peer now receives only IHave announcements
    }

    // Called when a Graft is sent (tree repair)
    fn on_graft_sent(&self, peer: &[u8], message_id: &MessageId) {
        // Requesting message from peer
    }

    // Called when a Prune is sent (tree optimization)
    fn on_prune_sent(&self, peer: &[u8]) {
        // Removing redundant path
    }
}
```

## Plumtree vs PlumtreeMemberlist

This crate provides two main APIs:

### `Plumtree` - Core Protocol

The `Plumtree` struct implements the core Epidemic Broadcast Tree protocol. Use this when:

- You need fine-grained control over message handling
- You want to integrate with a custom transport layer
- You're building a custom networking stack

```rust
// Low-level API - you handle message transport
let (plumtree, handle) = Plumtree::new(node_id, config, delegate);
plumtree.handle_message(from_peer, message); // Manual message handling
```

### `PlumtreeMemberlist` - Simplified Integration

The `PlumtreeMemberlist` struct wraps `Plumtree` with a simpler API designed for integration with [memberlist](https://crates.io/crates/memberlist-core). Use this when:

- You want a simpler broadcast API without manual message handling
- You're using memberlist for cluster membership and peer discovery
- You want automatic peer management based on memberlist events

```rust
use memberlist_plumtree::{PlumtreeMemberlist, PlumtreeConfig, NoopDelegate};

// High-level API - simpler to use
let plumtree_ml = PlumtreeMemberlist::new(
    node_id,
    PlumtreeConfig::default(),
    NoopDelegate,
);

// Add peers discovered via memberlist
plumtree_ml.add_peer(peer_id);

// Broadcast - message reaches all nodes via spanning tree
let msg_id = plumtree_ml.broadcast(b"cluster message").await?;

// Access statistics
let peer_stats = plumtree_ml.peer_stats();
let cache_stats = plumtree_ml.cache_stats();
```

**Key difference**: `PlumtreeMemberlist` is a convenience wrapper that provides:
- Simplified broadcast API (just call `broadcast()`)
- Built-in statistics (peer counts, cache stats)
- Easy peer management (`add_peer`, `remove_peer`)
- Graceful shutdown handling

## Advanced Usage

### Rate Limiting

Built-in per-peer rate limiting prevents abuse:

```rust
use memberlist_plumtree::RateLimiter;

// Create rate limiter: 10 tokens, refill 5/second
let limiter: RateLimiter<String> = RateLimiter::new(10, 5.0);

// Check if request is allowed
if limiter.check(&peer_id) {
    // Process request
}

// Check batch of requests
if limiter.check_n(&peer_id, 5) {
    // Process 5 requests
}
```

### Graft Timer with Backoff

Exponential backoff for reliable message recovery:

```rust
use memberlist_plumtree::GraftTimer;
use std::time::Duration;

let timer = GraftTimer::with_backoff(
    Duration::from_millis(100),  // base timeout
    Duration::from_millis(800),  // max timeout
    5,                           // max retries
);

// Track pending message
timer.expect_message(msg_id, peer_bytes, round);

// Check for expired entries that need Graft
let expired = timer.get_expired();
for graft in expired {
    // Send Graft request to graft.peer for graft.message_id
}
```

### Scheduler Access

Direct access to IHave scheduling:

```rust
use memberlist_plumtree::IHaveScheduler;

let scheduler = IHaveScheduler::new(
    Duration::from_millis(100),  // interval
    16,                          // batch size
    1000,                        // max queue size
);

// Access the queue
let queue = scheduler.queue();
queue.push(msg_id, round);

// Pop batch for sending
let batch = scheduler.pop_batch();
```

## Examples

### Chat Example

A simple multicast chat demonstrating the core Plumtree API:

```bash
cargo run --example chat
```

This demonstrates:
- Multiple node creation and mesh topology
- Message broadcasting via `Plumtree`
- Peer state management (eager/lazy)
- Protocol statistics

### Pub/Sub Example

A topic-based publish/subscribe system using `PlumtreeMemberlist`:

```bash
cargo run --example pubsub
```

This demonstrates:
- Using `PlumtreeMemberlist` for simplified broadcasting
- Topic-based message routing
- Dynamic subscriptions (subscribe/unsubscribe at runtime)
- Message serialization/deserialization
- Statistics and monitoring
- Graceful peer removal and shutdown

**Important**: The pub/sub example uses **application-layer filtering**, not per-topic routing. This means:

```
┌─────────────────────────────────────────────────────────────────┐
│                     How Pub/Sub Works                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Publisher ──broadcast──> ALL nodes via spanning tree           │
│                              │                                  │
│                    ┌─────────┼─────────┐                        │
│                    ▼         ▼         ▼                        │
│                 Node A    Node B    Node C                      │
│                    │         │         │                        │
│              [subscribed] [not sub] [subscribed]                │
│                    │         │         │                        │
│                 PROCESS   DISCARD   PROCESS                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

- Messages are broadcast to **all nodes** via Plumtree's spanning tree
- Each node receives **every message** regardless of topic
- Topic filtering happens in the `on_deliver` callback (application layer)
- This is simple but means all nodes see all traffic

For true per-topic routing (separate spanning tree per topic), you would need to:
1. Create multiple `PlumtreeMemberlist` instances (one per topic), or
2. Implement topic-aware routing at the protocol level

The application-layer approach is suitable for:
- Small to medium clusters
- Scenarios where most nodes subscribe to most topics
- Simplicity over bandwidth optimization

## Performance Characteristics

| Aspect | Complexity |
|--------|------------|
| Message broadcast | O(n) messages |
| Tree repair | O(1) per missing message |
| Memory per node | O(cache_size) |
| Latency | O(log n) hops typical |

## References

- [Epidemic Broadcast Trees](https://www.dpss.inesc-id.pt/~ler/reports/srds07.pdf) - Original Plumtree paper by Leitão, Pereira, and Rodrigues (2007)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
