# Configuration Guide

This guide covers all configuration options for memberlist-plumtree and provides recommendations for different deployment scenarios.

## PlumtreeConfig

The main configuration struct for the Plumtree protocol.

### Quick Start Presets

```rust
// General purpose (default)
let config = PlumtreeConfig::default();

// LAN deployment - low latency, aggressive optimization
let config = PlumtreeConfig::lan();

// WAN deployment - high latency tolerance, conservative
let config = PlumtreeConfig::wan();

// Large cluster (1000+ nodes) - optimized for scale
let config = PlumtreeConfig::large_cluster();
```

### Configuration Parameters

#### Peer Management

| Parameter | Default | LAN | WAN | Large Cluster | Description |
|-----------|---------|-----|-----|---------------|-------------|
| `max_peers` | None | None | None | None | Maximum peers to maintain (None = unlimited) |
| `eager_fanout` | 3 | 3 | 4 | 5 | Number of eager peers for immediate message delivery |
| `lazy_fanout` | 6 | 6 | 8 | 10 | Number of lazy peers to receive IHave announcements |
| `use_hash_ring` | false | false | false | true | Enable hash ring topology for deterministic peer selection |

**Guidelines:**
- `eager_fanout`: 3-4 is optimal for most deployments. Higher values increase redundancy but also duplicate traffic.
- `lazy_fanout`: Should be 2-3x `eager_fanout` for good tree repair coverage.
- `max_peers`: Set this to limit memory usage in very large clusters. Formula: `eager_fanout + lazy_fanout + buffer`.

```rust
let config = PlumtreeConfig::default()
    .with_eager_fanout(4)
    .with_lazy_fanout(8)
    .with_max_peers(20);
```

#### IHave Scheduling

| Parameter | Default | LAN | WAN | Large Cluster | Description |
|-----------|---------|-----|-----|---------------|-------------|
| `ihave_interval` | 100ms | 50ms | 200ms | 150ms | Interval for batching IHave messages |
| `ihave_batch_size` | 16 | 8 | 16 | 32 | Maximum IHave messages per batch |

**Guidelines:**
- Shorter intervals reduce tree repair latency but increase network overhead.
- Larger batch sizes are more efficient for high-throughput scenarios.
- For clusters with 1000+ nodes, use `ihave_batch_size: 32-64` to reduce IHave storm.

```rust
let config = PlumtreeConfig::default()
    .with_ihave_interval(Duration::from_millis(150))
    .with_ihave_batch_size(32);
```

#### Graft Timer

| Parameter | Default | LAN | WAN | Large Cluster | Description |
|-----------|---------|-----|-----|---------------|-------------|
| `graft_timeout` | 500ms | 200ms | 1000ms | 750ms | Time to wait before sending Graft after IHave |
| `graft_max_retries` | 5 | 3 | 7 | 5 | Maximum Graft retry attempts with backoff |
| `graft_rate_limit_per_second` | 10.0 | 20.0 | 5.0 | 10.0 | Per-peer Graft rate limit |
| `graft_rate_limit_burst` | 20 | 40 | 15 | 30 | Maximum Graft burst per peer |

**Guidelines:**
- `graft_timeout` should be higher than expected network RTT.
- Rate limiting prevents abuse from misbehaving peers.
- More retries increase reliability but delay failure detection.

```rust
let config = PlumtreeConfig::default()
    .with_graft_timeout(Duration::from_millis(750))
    .with_graft_max_retries(5);
```

#### Message Cache

| Parameter | Default | LAN | WAN | Large Cluster | Description |
|-----------|---------|-----|-----|---------------|-------------|
| `message_cache_ttl` | 60s | 30s | 120s | 90s | How long to cache messages for Graft responses |
| `message_cache_max_size` | 10000 | 10000 | 20000 | 50000 | Maximum messages in cache |
| `max_message_size` | 64KB | 64KB | 64KB | 64KB | Maximum payload size |

**Guidelines:**
- TTL should be long enough for tree repair to complete.
- Larger caches improve reliability but consume more memory.
- Memory usage ≈ `cache_max_size × average_message_size`.

```rust
let config = PlumtreeConfig::default()
    .with_message_cache_ttl(Duration::from_secs(90))
    .with_message_cache_max_size(50000)
    .with_max_message_size(128 * 1024);  // 128KB
```

#### Tree Optimization

| Parameter | Default | LAN | WAN | Large Cluster | Description |
|-----------|---------|-----|-----|---------------|-------------|
| `optimization_threshold` | 3 | 2 | 4 | 5 | Duplicate count before sending Prune |

**Guidelines:**
- Lower values optimize faster but may be premature during topology changes.
- Higher values are more conservative, maintaining redundant paths longer.

```rust
let config = PlumtreeConfig::default()
    .with_optimization_threshold(4);
```

#### Maintenance Loop

| Parameter | Default | LAN | WAN | Large Cluster | Description |
|-----------|---------|-----|-----|---------------|-------------|
| `maintenance_interval` | 2s | 1s | 5s | 3s | Interval for topology repair checks |
| `maintenance_jitter` | 500ms | 200ms | 1s | 750ms | Random jitter to prevent thundering herd |

**Guidelines:**
- The maintenance loop promotes lazy peers when eager count drops below `eager_fanout`.
- Jitter prevents all nodes from repairing simultaneously.
- Set to `Duration::ZERO` to disable (not recommended).

```rust
let config = PlumtreeConfig::default()
    .with_maintenance_interval(Duration::from_secs(3))
    .with_maintenance_jitter(Duration::from_millis(500));
```

## BridgeConfig

Configuration for the Plumtree-Memberlist integration bridge.

```rust
use memberlist_plumtree::BridgeConfig;

let config = BridgeConfig::new()
    .with_log_changes(true)           // Log topology changes
    .with_auto_promote(true)          // Auto-promote new peers
    .with_static_seeds(vec![          // Seed addresses
        "192.168.1.100:7946".parse().unwrap(),
        "192.168.1.101:7946".parse().unwrap(),
    ])
    .with_lazarus_enabled(true)       // Enable seed recovery
    .with_lazarus_interval(Duration::from_secs(30))
    .with_persistence_path(PathBuf::from("/var/lib/plumtree/peers.txt"));
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `log_changes` | true | Log peer join/leave events |
| `auto_promote` | true | Auto-classify new peers as eager/lazy |
| `static_seeds` | [] | Seed addresses for Lazarus recovery |
| `lazarus_enabled` | false | Enable automatic seed recovery |
| `lazarus_interval` | 30s | Interval for probing dead seeds |
| `persistence_path` | None | Path for saving known peers |

### Lazarus Seed Recovery

The Lazarus feature solves the "Ghost Seed" problem where a restarted seed remains isolated because other nodes stopped pinging it after marking it dead.

```rust
let config = BridgeConfig::new()
    .with_static_seeds(seeds)
    .with_lazarus_enabled(true)
    .with_lazarus_interval(Duration::from_secs(15));  // Faster recovery
```

### Peer Persistence

Save known peers to disk for recovery after restart:

```rust
let config = BridgeConfig::new()
    .with_persistence_path(PathBuf::from("peers.txt"));

// On shutdown, peers are saved automatically
// On startup, peers are loaded and used for bootstrapping
```

## QuicConfig

See [QUIC Transport](quic.md) for detailed QUIC configuration.

Quick reference:

```rust
use memberlist_plumtree::QuicConfig;

// Development (insecure, self-signed certs)
let config = QuicConfig::insecure_dev();

// LAN deployment
let config = QuicConfig::lan();

// WAN deployment
let config = QuicConfig::wan();

// Large cluster
let config = QuicConfig::large_cluster();
```

## Environment-Specific Recommendations

### Development/Testing

```rust
let config = PlumtreeConfig::lan()
    .with_ihave_interval(Duration::from_millis(20))  // Fast IHave for testing
    .with_graft_timeout(Duration::from_millis(100))
    .with_message_cache_ttl(Duration::from_secs(10));
```

### Production LAN

```rust
let config = PlumtreeConfig::lan()
    .with_hash_ring(true)  // Enable for better topology
    .with_maintenance_interval(Duration::from_secs(1));
```

### Production WAN

```rust
let config = PlumtreeConfig::wan()
    .with_hash_ring(true)
    .with_graft_max_retries(10)  // More retries for unreliable network
    .with_message_cache_ttl(Duration::from_secs(180));  // Longer cache
```

### Large Cluster (1000+ nodes)

```rust
let config = PlumtreeConfig::large_cluster()
    .with_ihave_batch_size(64)      // Large batches
    .with_max_peers(100)            // Limit peer connections
    .with_message_cache_max_size(100000);
```

### Memory-Constrained Environment

```rust
let config = PlumtreeConfig::default()
    .with_max_peers(20)
    .with_message_cache_max_size(1000)
    .with_message_cache_ttl(Duration::from_secs(30));
```

## Tuning Guidelines

### Latency vs Throughput

| Priority | Adjustment |
|----------|------------|
| Low latency | Lower `ihave_interval`, smaller `ihave_batch_size`, lower `graft_timeout` |
| High throughput | Higher `ihave_batch_size`, larger `message_cache_max_size` |

### Reliability vs Efficiency

| Priority | Adjustment |
|----------|------------|
| Higher reliability | Higher `eager_fanout`, more `graft_max_retries`, longer `message_cache_ttl` |
| Higher efficiency | Lower `optimization_threshold`, smaller cache |

### Memory Usage

| Component | Memory Formula |
|-----------|----------------|
| Message cache | `cache_max_size × avg_message_size` |
| Seen map | `~16 shards × capacity_per_shard × entry_size` |
| Peer state | `total_peers × peer_entry_size` |

## Monitoring Configuration

Use metrics to validate configuration:

```rust
// Check if eager fanout is maintained
let stats = plumtree.peer_stats();
if stats.eager_count < config.eager_fanout {
    warn!("Eager count below target");
}

// Check cache utilization
let cache_stats = plumtree.cache_stats();
if cache_stats.entries > config.message_cache_max_size * 90 / 100 {
    warn!("Cache near capacity");
}

// Check health
let health = plumtree.health();
if health.status.is_degraded() {
    warn!("Protocol health degraded: {}", health.message);
}
```

## See Also

- [Architecture Overview](architecture.md) - Understanding the protocol
- [Metrics Reference](metrics.md) - Monitoring and observability
- [QUIC Transport](quic.md) - QUIC-specific configuration
