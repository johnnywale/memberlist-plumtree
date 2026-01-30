# Performance Tuning Guide

Optimize memberlist-plumtree for your specific workload.

## Workload Characterization

Before tuning, understand your workload:

| Characteristic | Low | Medium | High |
|----------------|-----|--------|------|
| Message rate | <10/s | 10-100/s | >100/s |
| Message size | <1KB | 1-10KB | >10KB |
| Cluster size | <50 | 50-500 | >500 |
| Latency requirement | <1s | <100ms | <10ms |
| Reliability requirement | Best effort | At-least-once | Exactly-once |

## Tuning Parameters

### Fanout Configuration

**Goal:** Balance between reliability and overhead.

```rust
// High reliability (more redundancy)
PlumtreeConfig::default()
    .with_eager_fanout(4)
    .with_lazy_fanout(8)

// Low overhead (less redundancy)
PlumtreeConfig::default()
    .with_eager_fanout(2)
    .with_lazy_fanout(4)
```

| eager_fanout | Reliability | Network Overhead | Use Case |
|--------------|-------------|------------------|----------|
| 2 | Lower | Low | Bandwidth-constrained |
| 3 | Good | Medium | Default, most deployments |
| 4-5 | High | Higher | Critical data, large clusters |

| lazy_fanout | Recovery Speed | Network Overhead |
|-------------|----------------|------------------|
| 4 | Slower | Low |
| 6 | Balanced | Medium |
| 10+ | Faster | Higher |

### IHave Batching

**Goal:** Reduce packet overhead while maintaining low latency.

```rust
// Low latency (more packets)
PlumtreeConfig::default()
    .with_ihave_interval(Duration::from_millis(50))
    .with_ihave_batch_size(8)

// High throughput (fewer packets)
PlumtreeConfig::default()
    .with_ihave_interval(Duration::from_millis(200))
    .with_ihave_batch_size(64)
```

Trade-offs:
- Smaller batches = lower latency, more packets
- Larger batches = higher latency, fewer packets

Recommendation for large clusters:
```rust
PlumtreeConfig::large_cluster()
    .with_ihave_batch_size(32)  // Reduce IHave storm
```

### Cache Configuration

**Goal:** Balance memory usage and recovery capability.

```rust
// Memory-constrained
PlumtreeConfig::default()
    .with_message_cache_ttl(Duration::from_secs(30))
    .with_message_cache_max_size(5000)

// High recovery capability
PlumtreeConfig::default()
    .with_message_cache_ttl(Duration::from_secs(120))
    .with_message_cache_max_size(50000)
```

Formula for cache sizing:
```
cache_memory = max_size * avg_message_size
required_cache = message_rate * cache_ttl * redundancy_factor
```

### Timeout Configuration

**Goal:** Match timeouts to network characteristics.

```rust
// LAN (low latency)
PlumtreeConfig::default()
    .with_graft_timeout(Duration::from_millis(200))
    .with_graft_max_retries(3)

// WAN (high latency)
PlumtreeConfig::default()
    .with_graft_timeout(Duration::from_secs(1))
    .with_graft_max_retries(7)
```

Graft timeout should be:
```
graft_timeout > 2 * max_network_rtt + processing_time
```

### Rate Limiting

**Goal:** Prevent abuse while allowing legitimate traffic.

```rust
// Permissive (trusted network)
PlumtreeConfig::default()
    .with_graft_rate_limit_per_second(20.0)
    .with_graft_rate_limit_burst(40)

// Restrictive (untrusted/open network)
PlumtreeConfig::default()
    .with_graft_rate_limit_per_second(5.0)
    .with_graft_rate_limit_burst(10)
```

## Memory Optimization

### Reduce Cache Memory

```rust
PlumtreeConfig::default()
    .with_message_cache_ttl(Duration::from_secs(30))   // Shorter TTL
    .with_message_cache_max_size(5000)                 // Fewer messages
```

### Limit Peer Counts

```rust
PlumtreeConfig::large_cluster()
    .with_max_peers(100)          // Total peer limit
    .with_max_eager_peers(5)      // Eager peer cap
    .with_max_lazy_peers(50)      // Lazy peer cap
```

### Enable Compression

```rust
#[cfg(feature = "compression")]
PlumtreeConfig::wan()
    .with_compression(CompressionConfig::zstd(3))  // Level 3 compression
```

Compression effectiveness:
| Message Type | Typical Ratio |
|--------------|---------------|
| JSON | 3-5x |
| Protobuf | 1.5-2x |
| Binary (random) | 1x (no benefit) |

## Network Optimization

### QUIC Configuration

```rust
// Optimize for low latency
QuicConfig::default()
    .with_enable_0rtt(true)           // Fast reconnection
    .with_keep_alive_interval(Duration::from_secs(5))
    .with_idle_timeout(Duration::from_secs(30))

// Optimize for high throughput
QuicConfig::default()
    .with_max_streams_per_connection(100)
    .with_max_stream_data(1024 * 1024)  // 1MB per stream
```

### Connection Pooling

```rust
// More connections for parallel sends
PoolConfig {
    max_connections_per_peer: 8,
    idle_timeout: Duration::from_secs(60),
}

// Fewer connections to reduce overhead
PoolConfig {
    max_connections_per_peer: 2,
    idle_timeout: Duration::from_secs(30),
}
```

### Datagram vs Stream

| Message Type | Recommended | Reason |
|--------------|-------------|--------|
| Gossip (small) | Datagram | Fire-and-forget, low overhead |
| Gossip (large) | Stream | Reliability, flow control |
| Graft/Prune | Stream | Must be delivered reliably |
| IHave | Datagram | Small, loss tolerable |

## CPU Optimization

### Reduce Contention

The crate uses sharded data structures:
- Seen map: 16 shards
- Message cache: Partitioned by message ID

No additional configuration needed.

### Background Task Tuning

```rust
// Adjust cleanup frequency based on load
CleanupTuner::new()
    .with_min_interval(Duration::from_secs(5))
    .with_max_interval(Duration::from_secs(60))
```

### Delegate Implementation

```rust
impl PlumtreeDelegate<NodeId> for MyDelegate {
    fn on_deliver(&self, msg_id: MessageId, payload: Bytes) {
        // DON'T block here!
        // Spawn a task for heavy processing
        let payload = payload.clone();
        tokio::spawn(async move {
            process_heavy_message(payload).await;
        });
    }
}
```

## Benchmarking

### Message Throughput Test

```rust
#[tokio::test]
async fn benchmark_throughput() {
    let cluster = create_test_cluster(10).await;

    let start = Instant::now();
    let count = 10_000;

    for i in 0..count {
        cluster.broadcast(format!("msg{}", i).into_bytes()).await?;
    }

    let elapsed = start.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.2} msg/s", rate);
}
```

### Latency Test

```rust
#[tokio::test]
async fn benchmark_latency() {
    let cluster = create_test_cluster(10).await;
    let mut latencies = Vec::new();

    for _ in 0..1000 {
        let start = Instant::now();
        let msg_id = cluster.broadcast(b"test".to_vec()).await?;
        wait_for_delivery(&cluster, &msg_id).await;
        latencies.push(start.elapsed());
    }

    latencies.sort();
    println!("P50: {:?}", latencies[500]);
    println!("P99: {:?}", latencies[990]);
}
```

## Configuration Presets Summary

| Preset | Use Case | Key Settings |
|--------|----------|--------------|
| `default()` | General purpose | Balanced settings |
| `lan()` | Same datacenter | Low latency, aggressive |
| `wan()` | Multi-datacenter | High latency tolerance, compression |
| `large_cluster()` | 1000+ nodes | High fanout, peer caps |

## Troubleshooting Performance Issues

### High CPU Usage

1. Check delegate `on_deliver` implementation
2. Verify not logging at DEBUG level in production
3. Check message rate metrics

### High Memory Usage

1. Check `plumtree_cache_size` gauge
2. Reduce cache TTL or max size
3. Verify messages are being evicted

### High Network Usage

1. Check duplicate rate: `plumtree_messages_duplicate_total`
2. Reduce fanout settings
3. Enable compression for large messages

### Slow Message Delivery

1. Check graft latency histogram
2. Reduce IHave batch interval
3. Increase eager fanout
4. Verify network latency
