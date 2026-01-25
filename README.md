# memberlist-plumtree

[![Crates.io](https://img.shields.io/crates/v/memberlist-plumtree.svg)](https://crates.io/crates/memberlist-plumtree)
[![Documentation](https://docs.rs/memberlist-plumtree/badge.svg)](https://docs.rs/memberlist-plumtree)
[![CI](https://github.com/johnnywale/memberlist-plumtree/actions/workflows/ci.yml/badge.svg)](https://github.com/johnnywale/memberlist-plumtree/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

Plumtree (Epidemic Broadcast Trees) implementation built on top of SWIM for efficient **O(n)** message broadcast in distributed systems. Uses [memberlist](https://crates.io/crates/memberlist-core) as the SWIM implementation.

## Overview

Plumtree combines the efficiency of tree-based broadcast with the reliability of gossip protocols through a hybrid push/lazy-push approach. This makes it ideal for applications requiring reliable message dissemination across large clusters with minimal network overhead.

## Why SWIM Instead of HyParView?

The original [Plumtree paper](https://www.dpss.inesc-id.pt/~ler/reports/srds07.pdf) uses [HyParView](https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf) as the underlying peer sampling and membership protocol. HyParView maintains a small "active view" and larger "passive view" of peers, with random shuffling to keep the overlay connected.

This implementation takes a different approach by building Plumtree on top of **SWIM** (Scalable Weakly-consistent Infection-style Membership protocol) via [memberlist](https://crates.io/crates/memberlist-core).

### Comparison

| Aspect | HyParView | SWIM |
|--------|-----------|-------------------|
| **Primary Purpose** | Peer sampling for gossip | Failure detection & membership |
| **Failure Detection** | Reactive (timeout-based) | Proactive (ping/ping-req/suspect) |
| **Membership View** | Partial (active + passive views) | Full (all alive members) |
| **Scalability** | ~log(n) active view | O(1) failure detection |
| **Ecosystem** | Academic implementations | Battle-tested (HashiCorp Consul, Serf) |

### Benefits of Using SWIM

1. **Proven at Scale**: SWIM (via memberlist) powers HashiCorp's Consul and Serf, running in production clusters with thousands of nodes
2. **Fast Failure Detection**: SWIM detects failures in O(log n) time with high probability
3. **Full Membership View**: Every node knows all alive members, simplifying Plumtree's peer selection
4. **Built-in Protocol**: No need to implement a separate membership layer - SWIM handles it

### How SWIM and Plumtree Work Together

```
┌─────────────────────────────────────────────────────────────────┐
│                          SWIM Layer                              │
│                    (cluster membership)                          │
│                                                                 │
│  Responsibilities:                                              │
│  • Discover and track cluster members                           │
│  • Detect node failures via ping/ping-req/suspect               │
│  • Propagate membership changes (join/leave/fail)               │
│  • Maintain full membership list                                │
└────────────────────────────┬────────────────────────────────────┘
                             │ Membership events (NodeJoin, NodeLeave)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Plumtree Layer                            │
│              (Epidemic Broadcast Trees)                          │
│                                                                 │
│  Responsibilities:                                              │
│  • Classify peers as eager or lazy                              │
│  • Build spanning tree for O(n) message broadcast               │
│  • Self-heal tree when nodes fail                               │
│  • Optimize tree topology via Graft/Prune                       │
└─────────────────────────────────────────────────────────────────┘
```

SWIM handles **who** is in the cluster; Plumtree manages **how** messages flow between them.

## How Eager and Lazy Peers Are Maintained

Each node independently classifies its peers into two sets:

| Set | Purpose | Message Type | Default Count |
|-----|---------|--------------|---------------|
| **Eager** | Spanning tree links | GOSSIP (full payload) | 3 (`eager_fanout`) |
| **Lazy** | Backup/repair links | IHAVE (message IDs only) | 6 (`lazy_fanout`) |

### Peer Lifecycle

```
┌────────────────────────────────────────────────────────────────┐
│                    Peer Classification Flow                     │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  New peer joins (via SWIM membership event)                    │
│         │                                                      │
│         ▼                                                      │
│  ┌──────────────┐                                              │
│  │  LAZY PEER   │  ◄── All new peers start as lazy             │
│  └──────────────┘                                              │
│         │                                                      │
│         │ (receives IHAVE, timeout waiting for GOSSIP)         │
│         ▼                                                      │
│  ┌──────────────┐                                              │
│  │    GRAFT     │  ◄── "I need this message, add me as eager"  │
│  └──────────────┘                                              │
│         │                                                      │
│         ▼                                                      │
│  ┌──────────────┐                                              │
│  │  EAGER PEER  │  ◄── Now receives full messages              │
│  └──────────────┘                                              │
│         │                                                      │
│         │ (receives duplicate from multiple eager peers)       │
│         ▼                                                      │
│  ┌──────────────┐                                              │
│  │    PRUNE     │  ◄── "I already have this, demote me"        │
│  └──────────────┘                                              │
│         │                                                      │
│         ▼                                                      │
│  ┌──────────────┐                                              │
│  │  LAZY PEER   │  ◄── Back to lazy, tree optimized            │
│  └──────────────┘                                              │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Promotion (Lazy → Eager)

A lazy peer is promoted to eager when:

1. **GRAFT received**: The peer sends a GRAFT request for a missing message
2. **Topology repair**: An eager peer fails and needs replacement
3. **Initial connection**: Hash ring neighbors are auto-promoted for connectivity

### Demotion (Eager → Lazy)

An eager peer is demoted to lazy when:

1. **PRUNE received**: The peer sends a PRUNE after receiving a duplicate message
2. **Rebalancing**: Too many eager peers (above `eager_fanout` target)

### Hash Ring Topology (Optional)

When `local_id` is configured, this implementation uses a **deterministic hash ring** for peer selection:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Hash Ring Topology                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│         [Node 7]                      [Node 2]                  │
│              \                        /                         │
│               \    [Node 0 (local)]  /                          │
│                \        │           /                           │
│                 ◄───────┼──────────►                            │
│               (i-1)     │        (i+1)                          │
│          predecessor    │      successor                        │
│                         │                                       │
│     ◄───────────────────┼───────────────────►                   │
│   (i-X/4)               │               (i+X/4)                 │
│  long-range jump        │          long-range jump              │
│                                                                 │
│  Ring Position = Hash(NodeID) mod RingSize                      │
│                                                                 │
│  Protected Neighbors (always eager, never demoted):             │
│  • i±1 (adjacent) - basic ring connectivity                     │
│  • i±2 (second-nearest) - Z≥2 redundancy                        │
│                                                                 │
│  Performance Links (eager but can be demoted):                  │
│  • i±X/4 (long-range) - reduce network diameter                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Benefits of hash ring topology**:

- **Deterministic**: Same membership = same topology across restarts
- **Z≥2 Redundancy**: Adjacent neighbors protected from demotion
- **Reduced Diameter**: Long-range jumps shorten message paths
- **Churn Resilience**: Only affected neighbors need reconnection

### RTT-Based Peer Scoring

For non-ring-neighbor positions, peers are scored using a hybrid algorithm:

```
hybrid_score = (topology_score × 0.4) + (network_score × 0.6)
```

- **Topology score**: Based on hash ring distance (closer = higher)
- **Network score**: Based on RTT and reliability (faster = higher)

This creates a naturally optimized tree where messages flow through the fastest, most reliable paths while maintaining structural connectivity guarantees.

### Key Features

- **O(n) Message Complexity**: Messages traverse a spanning tree, eliminating redundant transmissions
- **Self-Healing**: Automatic tree repair when nodes fail or network partitions occur
- **Protocol Optimization**: Dynamic eager/lazy peer management for optimal broadcast paths
- **Rate Limiting**: Built-in per-peer rate limiting with token bucket algorithm
- **Exponential Backoff**: Configurable retry logic with exponential backoff for Graft requests
- **Runtime Agnostic**: Works with Tokio, async-std, or other async runtimes
- **Zero-Copy**: Efficient message handling using `bytes::Bytes`
- **RTT-Based Topology**: Peer scoring based on latency for optimal tree construction
- **Connection Pooling**: Efficient connection management with per-peer queues
- **Adaptive Batching**: Dynamic IHave batch sizing based on network conditions
- **Dynamic Cleanup Tuning**: Lock-free rate tracking with adaptive cleanup intervals
- **Lazarus Seed Recovery**: Automatic reconnection to restarted seed nodes
- **Peer Persistence**: Save known peers to disk for crash recovery
- **Anti-Entropy Sync**: Automatic message recovery after network partitions or long disconnections
- **Message Persistence**: Optional Sled-based storage for crash recovery

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

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
memberlist-plumtree = "0.1"
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tokio` | Yes | Tokio runtime support (required for `MemberlistStack::start()`) |
| `metrics` | Yes | Prometheus-compatible metrics via the `metrics` crate |
| `serde` | Yes | Serialization/deserialization for config |
| `quic` | Yes | Native QUIC transport via quinn/rustls |
| `sync` | No | Anti-entropy synchronization for message recovery |
| `storage-sled` | No | Sled persistent storage backend |

To disable default features:

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", default-features = false, features = ["tokio"] }
```

## Quick Start

```rust
use memberlist_plumtree::{
    MemberlistStack, PlumtreeDiscovery, PlumtreeConfig,
    PlumtreeNodeDelegate, PlumtreeDelegate, MessageId,
    NoopDelegate, ChannelTransport,
};
use bytes::Bytes;
use std::sync::Arc;

// Define a delegate to handle delivered messages
struct MyDelegate;

impl PlumtreeDelegate<u64> for MyDelegate {
    fn on_deliver(&self, msg_id: MessageId, payload: Bytes) {
        println!("Received message {}: {:?}", msg_id, payload);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create PlumtreeDiscovery with your delegate
    let pm = Arc::new(PlumtreeDiscovery::new(
        1u64,
        PlumtreeConfig::lan(),
        MyDelegate,
    ));

    // 2. Create delegate for auto peer sync
    let delegate = PlumtreeNodeDelegate::new(1u64, NoopDelegate, pm.clone());

    // 3. Create SWIM instance and stack (see full example for transport setup)
    // let swim = Memberlist::new(transport, delegate, options).await?;
    // let stack = MemberlistStack::new(pm, swim, advertise_addr);

    // 4. CRITICAL: Start background tasks
    // let (transport, _rx) = ChannelTransport::bounded(1024);
    // stack.start(transport);

    // 5. Join cluster and broadcast
    // stack.join(&seeds).await?;
    // stack.broadcast(b"Hello, cluster!").await?;

    Ok(())
}
```

> **Important**: For the complete working example, see [docs/getting-started.md](docs/getting-started.md).

### Using Low-Level API

If you need fine-grained control, use `Plumtree` directly:

```rust
let (plumtree, handle) = Plumtree::new(
    "node-1".to_string(),
    PlumtreeConfig::lan(),
    Arc::new(MyDelegate),
);

// Add peers manually
plumtree.add_peer("node-2".to_string());

// REQUIRED: Start background tasks
tokio::spawn({
    let pt = plumtree.clone();
    async move { pt.run_ihave_scheduler().await }
});
tokio::spawn({
    let pt = plumtree.clone();
    async move { pt.run_graft_timer().await }
});
tokio::spawn({
    let pt = plumtree.clone();
    async move { pt.run_seen_cleanup().await }
});
tokio::spawn({
    let pt = plumtree.clone();
    async move { pt.run_maintenance_loop().await }
});

// Now broadcast
let msg_id = plumtree.broadcast(b"Hello!").await?;
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
    .with_eager_fanout(4)           // Target eager peers (also forwarding limit)
    .with_lazy_fanout(8)            // Lazy peers to send IHave announcements to
    .with_ihave_interval(Duration::from_millis(100))
    .with_graft_timeout(Duration::from_millis(500))
    .with_message_cache_ttl(Duration::from_secs(60))
    .with_message_cache_max_size(10000)
    .with_optimization_threshold(3)
    .with_max_message_size(64 * 1024)
    .with_graft_rate_limit_per_second(10.0)
    .with_graft_rate_limit_burst(20)
    .with_graft_max_retries(5)
    // Peer limit and ring protection settings
    .with_hash_ring(true)           // Enable hash ring topology
    .with_protect_ring_neighbors(true)
    .with_max_protected_neighbors(4) // Protect i±1 and i±2
    .with_max_eager_peers(6)        // Hard cap on eager peers
    .with_max_lazy_peers(20);       // Hard cap on lazy peers
```

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `eager_fanout` | 3 | Target number of eager peers (and forwarding limit per message) |
| `lazy_fanout` | 6 | Number of lazy peers to send IHave announcements to per message |
| `ihave_interval` | 100ms | Interval for batched IHave announcements |
| `message_cache_ttl` | 60s | How long to cache messages for Graft requests |
| `message_cache_max_size` | 10000 | Maximum number of cached messages |
| `optimization_threshold` | 3 | Rounds before pruning redundant paths |
| `graft_timeout` | 500ms | Timeout before sending Graft for missing message |
| `max_message_size` | 64KB | Maximum message payload size |
| `graft_rate_limit_per_second` | 10.0 | Rate limit for Graft requests per peer |
| `graft_rate_limit_burst` | 20 | Burst capacity for Graft rate limiting |
| `graft_max_retries` | 5 | Maximum Graft retry attempts with backoff |
| `use_hash_ring` | false | Enable hash ring topology for deterministic peer selection |
| `protect_ring_neighbors` | true | Protect ring neighbors from demotion (requires `use_hash_ring`) |
| `max_protected_neighbors` | 4 | Number of ring neighbors to protect: 2=i±1, 4=i±1,i±2 |
| `max_eager_peers` | None | Hard cap on eager peers (rejects GRAFT when at limit) |
| `max_lazy_peers` | None | Hard cap on lazy peers (rejects new peers when at limit) |

### Peer Limit Configuration

Control the maximum number of eager and lazy peers for bandwidth and memory constraints:

```rust
use memberlist_plumtree::PlumtreeConfig;

let config = PlumtreeConfig::default()
    // Target 3 eager peers, but allow up to 5 via GRAFT
    .with_eager_fanout(3)
    .with_max_eager_peers(5)

    // Limit lazy peers to reduce IHAVE tracking overhead
    .with_max_lazy_peers(20)

    // Ring neighbor protection settings
    .with_hash_ring(true)
    .with_protect_ring_neighbors(true)
    .with_max_protected_neighbors(2);  // Only protect i±1
```

**Preset differences:**

| Preset | `protect_ring_neighbors` | `max_eager_peers` | `max_lazy_peers` |
|--------|--------------------------|-------------------|------------------|
| `default()` | true | None | None |
| `lan()` | false | None | None |
| `wan()` | true | None | None |
| `large_cluster()` | true | 8 | 50 |

## Advanced Features

### Peer Scoring (RTT-Based Topology)

Peer scoring enables latency-aware peer selection for optimal spanning tree construction:

```rust
use memberlist_plumtree::{PeerScoring, ScoringConfig};

// Peers are scored based on RTT and reliability
let scoring = PeerScoring::new(ScoringConfig::default());

// Record RTT measurement
scoring.record_rtt(&peer_id, Duration::from_millis(15));

// Record failures (affects peer ranking)
scoring.record_failure(&peer_id);

// Get best peers for eager set (sorted by score)
let best_peers = scoring.best_peers(5);
```

### Connection Pooling

Efficient connection management with per-peer message queues:

```rust
use memberlist_plumtree::{PooledTransport, PoolConfig};

let config = PoolConfig::default()
    .with_queue_size(1024)      // Per-peer queue capacity
    .with_batch_size(32)        // Messages per batch
    .with_flush_interval(Duration::from_millis(10));

let transport = PooledTransport::new(config, inner_transport);

// Messages are automatically batched and sent efficiently
transport.send(peer_id, message).await?;

// Get pool statistics
let stats = transport.stats();
println!("Messages sent: {}, Queue pressure: {:.2}%",
    stats.messages_sent, stats.queue_pressure * 100.0);
```

### Adaptive IHave Batching

Dynamic batch size adjustment based on network conditions:

```rust
use memberlist_plumtree::{AdaptiveBatcher, BatcherConfig};

let batcher = AdaptiveBatcher::new(BatcherConfig::default());

// Record network feedback
batcher.record_message();
batcher.record_graft_received();  // Successful graft
batcher.record_graft_timeout();   // Failed graft

// Get recommended batch size (adjusts based on latency/success rate)
let batch_size = batcher.recommended_batch_size();

// Scale for cluster size
batcher.set_cluster_size_hint(1000);

// Get statistics
let stats = batcher.stats();
println!("Graft success rate: {:.1}%", stats.graft_success_rate * 100.0);
```

### Dynamic Cleanup Tuning

Lock-free rate tracking with adaptive cleanup intervals:

```rust
use memberlist_plumtree::{CleanupTuner, CleanupConfig};

let tuner = CleanupTuner::new(CleanupConfig::default());

// Record messages (fully lock-free, uses atomics only)
tuner.record_message();
tuner.record_messages(100);  // Batch recording for high throughput

// Get tuned cleanup parameters based on current state
let params = tuner.get_parameters(cache_utilization, cache_ttl);
println!("Recommended interval: {:?}, batch_size: {}",
    params.interval, params.batch_size);

// Check backpressure hint
match params.backpressure_hint() {
    BackpressureHint::None => { /* Normal operation */ }
    BackpressureHint::DropSome => { /* Consider dropping low-priority messages */ }
    BackpressureHint::DropMost => { /* Drop non-critical messages */ }
    BackpressureHint::BlockNew => { /* Temporarily block new messages */ }
}

// Get statistics with trend analysis
let stats = tuner.stats();
println!("Efficiency trend: {:?}, Pressure trend: {:?}",
    stats.efficiency_trend, stats.pressure_trend);
```

### Sharded Seen Map

High-performance deduplication with configurable sharding:

```rust
// Seen map is automatically managed by Plumtree
// Get seen map statistics
if let Some(stats) = plumtree.seen_map_stats() {
    println!("Utilization: {:.1}%, Entries: {}",
        stats.utilization * 100.0, stats.size);
}
```

### QUIC Transport

Native QUIC transport for improved performance with TLS 1.3 encryption:

```rust
use memberlist_plumtree::{
    QuicTransport, QuicConfig, MapPeerResolver, TlsConfig,
};
use std::sync::Arc;

// Create peer resolver for address lookup
let resolver = Arc::new(MapPeerResolver::new(local_addr));
resolver.add_peer(2u64, "192.168.1.10:9000".parse()?);

// Development: insecure self-signed certs
let config = QuicConfig::insecure_dev();

// Production: proper TLS configuration
let config = QuicConfig::wan()
    .with_tls(TlsConfig::new()
        .with_cert_path("/path/to/server.crt")
        .with_key_path("/path/to/server.key")
        .with_ca_path("/path/to/ca.crt")
        .with_mtls(true));

// Create transport
let transport = QuicTransport::new(local_addr, config, resolver).await?;

// Use with PlumtreeDiscovery
pm.run_with_transport(transport).await;
```

**QUIC Configuration Presets:**

| Preset | Use Case |
|--------|----------|
| `QuicConfig::default()` | General purpose |
| `QuicConfig::lan()` | Low latency LAN (BBR, shorter timeouts) |
| `QuicConfig::wan()` | High latency WAN (Cubic, longer timeouts) |
| `QuicConfig::large_cluster()` | 1000+ nodes (high connection limits) |
| `QuicConfig::insecure_dev()` | Development only (self-signed certs) |

### Health Monitoring

Check protocol health for alerting and debugging:

```rust
let health = plumtree.health();

match health.status {
    HealthStatus::Healthy => println!("All systems operational"),
    HealthStatus::Degraded => println!("Warning: {}", health.message),
    HealthStatus::Unhealthy => eprintln!("Critical: {}", health.message),
}

// Detailed health information
println!("Eager peers: {}", health.peer_health.eager_count);
println!("Lazy peers: {}", health.peer_health.lazy_count);
println!("Cache utilization: {:.1}%", health.cache_health.utilization * 100.0);
println!("Pending grafts: {}", health.delivery_health.pending_grafts);
```

### Anti-Entropy Sync

Automatic message recovery after network partitions, long disconnections, or node restarts. Requires the `sync` feature.

```rust
use memberlist_plumtree::{PlumtreeConfig, SyncConfig, StorageConfig};
use std::time::Duration;

let config = PlumtreeConfig::default()
    // Enable anti-entropy sync
    .with_sync(
        SyncConfig::enabled()
            .with_sync_interval(Duration::from_secs(30))  // Sync every 30s
            .with_sync_window(Duration::from_secs(90))    // Check last 90s
            .with_max_batch_size(100)                     // Max IDs per response
    )
    // Enable storage (required for sync)
    .with_storage(
        StorageConfig::enabled()
            .with_max_messages(100_000)                   // Max stored messages
            .with_retention(Duration::from_secs(300))     // 5 minute retention
    );
```

**How it works:**

1. Nodes periodically compare state using an XOR-based hash (O(1) comparison)
2. If hashes differ, nodes exchange message IDs to identify missing messages
3. Missing messages are requested and delivered via SyncPull/SyncPush

**Use cases:**
- Late joiner needs historical messages
- Node recovers after network partition
- Node restarts and needs to catch up

### Persistent Storage (Sled)

For crash recovery, use the Sled backend to persist messages to disk. Requires the `storage-sled` feature.

```rust
use memberlist_plumtree::{PlumtreeDiscovery, StorageConfig};
use memberlist_plumtree::storage::SledStore;
use std::sync::Arc;

// Create Sled store
let store = Arc::new(SledStore::open("/var/lib/myapp/messages")?);

// Use with_storage constructor
let pm = PlumtreeDiscovery::with_storage(
    node_id,
    config,
    delegate,
    store,
);
```

See [docs/sync-persistence.md](docs/sync-persistence.md) for detailed configuration and best practices.

## Metrics

When compiled with the `metrics` feature (enabled by default), comprehensive Prometheus-compatible metrics are exposed:

### Counters
- `plumtree_messages_broadcast_total` - Total broadcasts initiated
- `plumtree_messages_delivered_total` - Total messages delivered
- `plumtree_messages_duplicate_total` - Duplicate messages received
- `plumtree_gossip_sent_total` - Gossip messages sent
- `plumtree_ihave_sent_total` - IHave messages sent
- `plumtree_graft_sent_total` - Graft messages sent
- `plumtree_prune_sent_total` - Prune messages sent
- `plumtree_graft_success_total` - Successful Graft requests
- `plumtree_graft_failed_total` - Failed Graft requests (after max retries)
- `plumtree_graft_retries_total` - Graft retry attempts
- `plumtree_peer_promotions_total` - Peers promoted to eager
- `plumtree_peer_demotions_total` - Peers demoted to lazy
- `plumtree_peer_added_total` - Peers added to topology
- `plumtree_peer_removed_total` - Peers removed from topology

### Histograms
- `plumtree_graft_latency_seconds` - Graft request to delivery latency
- `plumtree_message_size_bytes` - Message payload size distribution
- `plumtree_message_hops` - Number of hops messages travel
- `plumtree_ihave_batch_size` - IHave batch size distribution

### Gauges
- `plumtree_eager_peers` - Current eager peer count
- `plumtree_lazy_peers` - Current lazy peer count
- `plumtree_total_peers` - Total peer count
- `plumtree_cache_size` - Messages in cache
- `plumtree_seen_map_size` - Entries in deduplication map
- `plumtree_ihave_queue_size` - Pending IHave announcements
- `plumtree_pending_grafts` - Pending Graft requests

See [docs/metrics.md](docs/metrics.md) for alerting examples and Grafana dashboard queries.

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

## Plumtree vs PlumtreeDiscovery

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

### `PlumtreeDiscovery` - Simplified Integration

The `PlumtreeDiscovery` struct wraps `Plumtree` with a simpler API designed for integration with SWIM. Use this when:

- You want a simpler broadcast API without manual message handling
- You're using SWIM for cluster membership and peer discovery
- You want automatic peer management based on SWIM membership events

```rust
use memberlist_plumtree::{PlumtreeDiscovery, PlumtreeConfig, NoopDelegate};

// High-level API - simpler to use
let plumtree_ml = PlumtreeDiscovery::new(
    node_id,
    PlumtreeConfig::default(),
    NoopDelegate,
);

// Add peers discovered via SWIM
plumtree_ml.add_peer(peer_id);

// Broadcast - message reaches all nodes via spanning tree
let msg_id = plumtree_ml.broadcast(b"cluster message").await?;

// Access statistics
let peer_stats = plumtree_ml.peer_stats();
let cache_stats = plumtree_ml.cache_stats();
```

**Key difference**: `PlumtreeDiscovery` is a convenience wrapper that provides:
- Simplified broadcast API (just call `broadcast()`)
- Built-in statistics (peer counts, cache stats)
- Easy peer management (`add_peer`, `remove_peer`)
- Graceful shutdown handling

### `MemberlistStack` - Full Integration (Recommended)

The `MemberlistStack` struct provides the complete integration stack, combining Plumtree with SWIM for automatic peer discovery. **This is the recommended entry point** for production deployments:

```rust
use memberlist_plumtree::{
    MemberlistStack, PlumtreeDiscovery, PlumtreeConfig,
    PlumtreeNodeDelegate, NoopDelegate, ChannelTransport,
};
use memberlist_core::{Memberlist, Options};
use std::sync::Arc;
use std::net::SocketAddr;

// 1. Create PlumtreeDiscovery with your delegate
let pm = Arc::new(PlumtreeDiscovery::new(
    node_id,
    PlumtreeConfig::default(),
    MyDelegate,
));

// 2. Create PlumtreeNodeDelegate for auto peer sync
let delegate = PlumtreeNodeDelegate::new(
    node_id.clone(),
    NoopDelegate,  // Or your SWIM delegate
    pm.clone(),
);

// 3. Create SWIM instance (via memberlist) with transport
let memberlist = Memberlist::new(transport, delegate, Options::default()).await?;

// 4. Build the complete stack (Plumtree + SWIM)
let stack = MemberlistStack::new(pm, memberlist, advertise_addr);

// 5. CRITICAL: Start background tasks!
let (unicast_tx, _rx) = ChannelTransport::bounded(1024);
stack.start(unicast_tx);  // <-- Without this, Plumtree won't work!

// 6. Join the cluster
let seeds: Vec<SocketAddr> = vec![
    "192.168.1.10:7946".parse()?,
    "192.168.1.11:7946".parse()?,
];
stack.join(&seeds).await?;

// 7. Broadcast messages - automatically routes via spanning tree
let msg_id = stack.broadcast(b"Hello cluster!").await?;

// 8. Get cluster status
println!("Members: {}", stack.num_members().await);
println!("Peers: {:?}", stack.peer_stats());

// 9. Graceful shutdown
stack.leave(Duration::from_secs(5)).await?;
stack.shutdown().await?;
```

> **Warning**: Always call `stack.start(transport)` after creating the stack! Without it:
> - IHave messages won't be sent to lazy peers
> - Graft retry logic won't work
> - Tree self-healing will be broken

**Key features of `MemberlistStack`**:
- **Automatic peer discovery**: SWIM discovers peers without manual `add_peer()` calls
- **Simplified join API**: Just pass seed addresses, no need to deal with `MaybeResolvedAddress`
- **Integrated lifecycle**: Single struct manages both SWIM and Plumtree
- **Peer sync**: `PlumtreeNodeDelegate` automatically syncs Plumtree peers when SWIM membership changes
- **Lazarus seed recovery**: Automatic reconnection to restarted seed nodes (see below)
- **Peer persistence**: Save known peers to disk for crash recovery

### Lazarus Seed Recovery

The "Lazarus" feature solves the **Ghost Seed Problem**: when a seed node fails and restarts, other nodes have already marked it as dead and stopped probing it. Without intervention, the restarted seed remains isolated.

**How it works:**
1. Configure static seed addresses in `BridgeConfig`
2. Enable the Lazarus background task
3. The task periodically checks if seeds are in the alive set
4. Missing seeds are automatically probed and rejoined

```rust
use memberlist_plumtree::{BridgeConfig, MemberlistStack};
use std::time::Duration;
use std::path::PathBuf;

// Configure Lazarus seed recovery
let config = BridgeConfig::new()
    // Static seeds to monitor
    .with_static_seeds(vec![
        "192.168.1.100:7946".parse().unwrap(),
        "192.168.1.101:7946".parse().unwrap(),
        "192.168.1.102:7946".parse().unwrap(),
    ])
    // Enable the Lazarus background task
    .with_lazarus_enabled(true)
    // Probe interval (default: 30 seconds)
    .with_lazarus_interval(Duration::from_secs(30))
    // Optional: persist known peers for crash recovery
    .with_persistence_path(PathBuf::from("/var/lib/myapp/peers.txt"));

// After creating MemberlistStack, spawn the Lazarus task
let lazarus_handle = stack.spawn_lazarus_task(config);

// Monitor Lazarus statistics
let stats = lazarus_handle.stats();
println!("Probes sent: {}", stats.probes_sent);
println!("Reconnections: {}", stats.reconnections);
println!("Missing seeds: {}", stats.missing_seeds);

// Gracefully shutdown when done
lazarus_handle.shutdown();
```

**BridgeConfig Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `static_seeds` | `[]` | List of seed node addresses to monitor |
| `lazarus_enabled` | `false` | Enable the Lazarus background task |
| `lazarus_interval` | 30s | Interval between seed probes |
| `persistence_path` | `None` | Path to persist known peers |
| `log_changes` | `true` | Log topology changes |
| `auto_promote` | `true` | Auto-promote new peers based on fanout |

### Peer Persistence

For crash recovery, `MemberlistStack` can save known peer addresses to disk. Combined with static seeds, this provides robust bootstrap options:

```rust
use memberlist_plumtree::{persistence, BridgeConfig, MemberlistStack};

// Save current peers to file (call periodically or on shutdown)
stack.save_peers_to_file(&PathBuf::from("/var/lib/myapp/peers.txt")).await?;

// On startup, load bootstrap addresses from both sources
let config = BridgeConfig::new()
    .with_static_seeds(vec!["192.168.1.100:7946".parse().unwrap()])
    .with_persistence_path(PathBuf::from("/var/lib/myapp/peers.txt"));

// Combines static seeds + persisted peers (deduplicated)
let bootstrap_addrs = MemberlistStack::load_bootstrap_addresses(&config);

// Use bootstrap addresses to join the cluster
stack.join(&bootstrap_addrs).await?;
```

**Persistence File Format:**
```
# Comments are supported
192.168.1.100:7946
192.168.1.101:7946
192.168.1.102:7946
```

**Important:** Each node should use a **unique persistence path** to avoid conflicts:
```rust
let node_id = "node-42";
let path = PathBuf::from(format!("/var/lib/myapp/{}/peers.txt", node_id));
```

**When to use each API**:

| API | Use Case |
|-----|----------|
| `Plumtree` | Custom transport, fine-grained control |
| `PlumtreeDiscovery` | Manual peer management, testing |
| `MemberlistStack` | **Production** - full SWIM + Plumtree integration (recommended) |

## Examples

### Terminal Chat Example

An interactive terminal-based chat with fault injection for testing:

```bash
cargo run --example chat
```

**Features:**
- 20 simulated nodes with full mesh topology
- Real-time protocol metrics (Grafts, Prunes, Promotions, Demotions)
- Fault injection controls:
  - `F` - Toggle node offline/online
  - `L` - Toggle 20% packet loss
  - `E` - Promote all peers to eager (triggers prunes)
  - `R` - Reset metrics
  - `M` - Toggle metrics panel
- User switching: Tab, Shift+Tab, number keys (1-9, 0=10), PageUp/PageDown
- Color-coded event log showing protocol activity

### Web Chat Example

A web-based visualization with peer tree display:

```bash
cargo run --example web-chat
# Then open http://localhost:3000
```

**Features:**
- Real-time WebSocket updates
- Visual peer tree showing eager (green) and lazy (orange) connections
- Interactive controls: user switching, fault injection, metrics reset
- Three-panel layout: Tree visualization | Chat messages | Event log + Metrics
- Static HTML/JS frontend served from `examples/static/`

**Web UI Layout:**
```
┌─────────────────┬─────────────────────┬─────────────────┐
│   Peer Tree     │    Chat Messages    │   Event Log     │
│                 │                     │                 │
│    [U3]         │ [12:34:56] U1: Hi   │ [00:15] GOSSIP  │
│   /    \        │ [12:34:57] U2: Hey  │ [00:16] PRUNE   │
│ [U2]  [U4]      │                     │ [00:17] PROMOTE │
│  (eager) (lazy) │ > Type message...   │                 │
│                 │                     │ ┌─────────────┐ │
│ Legend:         │                     │ │ Metrics     │ │
│ ● Current       │                     │ │ Sent: 5     │ │
│ ● Eager         │                     │ │ Recv: 12    │ │
│ ● Lazy          │                     │ │ Grafts: 2   │ │
└─────────────────┴─────────────────────┴─────────────────┘
```

### Pub/Sub Example

A topic-based publish/subscribe system using `PlumtreeDiscovery`:

```bash
cargo run --example pubsub
```

This demonstrates:
- Using `PlumtreeDiscovery` for simplified broadcasting
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
1. Create multiple `PlumtreeDiscovery` instances (one per topic), or
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

### Optimized for Scale

This implementation includes several optimizations for large-scale deployments (10,000+ nodes):

- **Lock-free message rate tracking**: Atomic counters for high-throughput recording
- **Sharded deduplication**: Reduces lock contention with configurable sharding
- **RTT-based peer selection**: Optimizes tree topology for latency
- **Adaptive batching**: Adjusts batch sizes based on network conditions
- **Connection pooling**: Efficient message delivery with per-peer queues

## Roadmap

### Completed Features

- **Anti-Entropy Sync**: Automatic message recovery after partitions (Phase 3)
- **Message Persistence**: Sled backend for crash recovery (Phase 3)
- **QUIC Transport**: Native QUIC with TLS 1.3 (Phase 2)
- **RTT-Based Topology**: Latency-optimized peer selection (Phase 2)

### Phase 4: Protocol Extensions (Planned)

- **Priority-based message routing**: Support for message priorities affecting delivery order
- **Topic-based spanning trees**: Separate spanning trees per topic for efficient pub/sub
- **Compression**: Optional message compression for bandwidth reduction
- **Encryption**: End-to-end encryption support for secure clusters
- **Protocol versioning**: Backward-compatible protocol evolution
- **Cluster-aware batching**: Batch size hints based on cluster topology
- **Health-based peer selection**: Factor in peer health metrics for routing decisions

## Documentation

Comprehensive documentation is available in the [`docs/`](docs/) directory:

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Quick start guide with examples |
| [Architecture](docs/architecture.md) | System architecture and message flow |
| [Peer Topology](docs/peer-topology.md) | Eager/lazy management, hash ring, peer scoring |
| [Adaptive Features](docs/adaptive-features.md) | Dynamic batch sizing and cleanup tuning |
| [Sync & Persistence](docs/sync-persistence.md) | Anti-entropy sync, message storage, crash recovery |
| [Configuration](docs/configuration.md) | Complete configuration reference |
| [QUIC Transport](docs/quic.md) | QUIC transport setup and TLS configuration |
| [Background Tasks](docs/background-tasks.md) | Required background tasks and troubleshooting |
| [Metrics](docs/metrics.md) | Prometheus metrics reference and alerting |

API documentation is available on [docs.rs](https://docs.rs/memberlist-plumtree).

## References

- [Epidemic Broadcast Trees](https://www.dpss.inesc-id.pt/~ler/reports/srds07.pdf) - Original Plumtree paper by Leitão, Pereira, and Rodrigues (2007)

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
