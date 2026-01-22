# Metrics Reference

memberlist-plumtree provides comprehensive Prometheus-compatible metrics for monitoring protocol health and performance.

## Enabling Metrics

Metrics require the `metrics` feature (enabled by default):

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", features = ["metrics"] }
```

Metrics are automatically initialized when creating a `Plumtree` instance.

## Metrics Overview

### Counters (Cumulative Events)

| Metric | Description | Labels |
|--------|-------------|--------|
| `plumtree_messages_broadcast_total` | Total messages broadcast by this node | - |
| `plumtree_messages_delivered_total` | Total messages delivered to application | - |
| `plumtree_messages_duplicate_total` | Total duplicate messages received | - |
| `plumtree_gossip_sent_total` | Total Gossip messages sent to eager peers | - |
| `plumtree_ihave_sent_total` | Total IHave messages sent to lazy peers | - |
| `plumtree_graft_sent_total` | Total Graft requests sent | - |
| `plumtree_prune_sent_total` | Total Prune messages sent | - |
| `plumtree_peer_promotions_total` | Total peers promoted to eager | - |
| `plumtree_peer_demotions_total` | Total peers demoted to lazy | - |
| `plumtree_peer_added_total` | Total peers added to topology | - |
| `plumtree_peer_removed_total` | Total peers removed from topology | - |
| `plumtree_graft_success_total` | Successful Graft requests (message received) | - |
| `plumtree_graft_failed_total` | Failed Graft requests (timeout after retries) | - |
| `plumtree_graft_retries_total` | Total Graft retry attempts | - |
| `plumtree_seen_map_evictions_total` | Emergency evictions from seen map | - |

### Histograms (Distributions)

| Metric | Description | Buckets |
|--------|-------------|---------|
| `plumtree_graft_latency_seconds` | Time from Graft request to message delivery | Default |
| `plumtree_message_size_bytes` | Message payload size distribution | Default |
| `plumtree_message_hops` | Number of hops (rounds) messages travel | Default |
| `plumtree_ihave_batch_size` | Size of IHave batches sent | Default |

### Gauges (Current State)

| Metric | Description |
|--------|-------------|
| `plumtree_eager_peers` | Current number of eager peers |
| `plumtree_lazy_peers` | Current number of lazy peers |
| `plumtree_total_peers` | Total peer count (eager + lazy) |
| `plumtree_cache_size` | Current messages in cache |
| `plumtree_seen_map_size` | Current entries in deduplication map |
| `plumtree_ihave_queue_size` | Pending IHave announcements |
| `plumtree_pending_grafts` | Pending Graft requests |

## Metric Interpretation

### Message Flow Health

```
# Good: Delivery rate close to broadcast rate (accounting for dedup)
rate(plumtree_messages_delivered_total[5m])

# Duplicate ratio - high values may indicate tree optimization needed
rate(plumtree_messages_duplicate_total[5m]) / rate(plumtree_messages_delivered_total[5m])
```

**Healthy indicators:**
- `delivered_total` growing steadily
- `duplicate_total` < 50% of `delivered_total`
- `gossip_sent_total` proportional to `broadcast_total × eager_peers`

### Tree Repair Health

```
# Graft success rate - should be high (>95%)
rate(plumtree_graft_success_total[5m]) /
(rate(plumtree_graft_success_total[5m]) + rate(plumtree_graft_failed_total[5m]))

# Retry rate - high values indicate network issues
rate(plumtree_graft_retries_total[5m]) / rate(plumtree_graft_sent_total[5m])
```

**Warning signs:**
- High `graft_failed_total` - indicates unreachable peers
- High `graft_retries_total` - indicates network latency or packet loss
- Growing `pending_grafts` - tree repair backlog

### Peer Topology Health

```
# Peer churn rate
rate(plumtree_peer_added_total[5m]) + rate(plumtree_peer_removed_total[5m])

# Promotion/demotion ratio - should be balanced
rate(plumtree_peer_promotions_total[5m]) / rate(plumtree_peer_demotions_total[5m])
```

**Healthy indicators:**
- `eager_peers` >= configured `eager_fanout`
- `total_peers` stable over time
- Balanced promotions and demotions

### Message Characteristics

```
# Average message size
histogram_quantile(0.5, rate(plumtree_message_size_bytes_bucket[5m]))

# Average hop count (tree depth indicator)
histogram_quantile(0.5, rate(plumtree_message_hops_bucket[5m]))
```

**Healthy indicators:**
- Hop count close to log(N) for N nodes (balanced tree)
- Message size within configured `max_message_size`

### Resource Usage

```
# Cache utilization
plumtree_cache_size / max_cache_size

# Seen map utilization
plumtree_seen_map_size / max_seen_entries
```

**Warning signs:**
- Cache near capacity - consider increasing `message_cache_max_size`
- High `seen_map_evictions_total` - memory pressure

## Alerting Examples

### Critical Alerts

```yaml
# Prometheus alerting rules

- alert: PlumtreeNoEagerPeers
  expr: plumtree_eager_peers == 0
  for: 1m
  labels:
    severity: critical
  annotations:
    summary: "No eager peers - broadcast tree broken"

- alert: PlumtreeHighGraftFailures
  expr: rate(plumtree_graft_failed_total[5m]) > 10
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: "High Graft failure rate - tree repair failing"
```

### Warning Alerts

```yaml
- alert: PlumtreeLowEagerPeers
  expr: plumtree_eager_peers < 2
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Low eager peer count - reduced redundancy"

- alert: PlumtreeHighDuplicates
  expr: rate(plumtree_messages_duplicate_total[5m]) > rate(plumtree_messages_delivered_total[5m])
  for: 10m
  labels:
    severity: warning
  annotations:
    summary: "High duplicate rate - tree may need optimization"

- alert: PlumtreeCacheNearCapacity
  expr: plumtree_cache_size > 9000  # Assuming max 10000
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Message cache near capacity"
```

## Grafana Dashboard

Example dashboard queries:

### Message Rate Panel

```
# Broadcast rate
rate(plumtree_messages_broadcast_total[1m])

# Delivery rate
rate(plumtree_messages_delivered_total[1m])

# Duplicate rate
rate(plumtree_messages_duplicate_total[1m])
```

### Peer Topology Panel

```
# Peer counts
plumtree_eager_peers
plumtree_lazy_peers
plumtree_total_peers
```

### Tree Repair Panel

```
# Graft success rate
rate(plumtree_graft_success_total[5m]) /
(rate(plumtree_graft_success_total[5m]) + rate(plumtree_graft_failed_total[5m]) + 0.001)

# Pending grafts
plumtree_pending_grafts
```

### Message Hops Heatmap

```
# Hop count distribution over time
sum(rate(plumtree_message_hops_bucket[1m])) by (le)
```

## Integration with metrics-rs

memberlist-plumtree uses the `metrics` crate. To expose metrics:

### With Prometheus Exporter

```rust
use metrics_exporter_prometheus::PrometheusBuilder;

fn main() {
    // Install Prometheus exporter
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9090))
        .install()
        .expect("failed to install Prometheus exporter");

    // Create Plumtree (metrics automatically registered)
    let (plumtree, handle) = Plumtree::new(node_id, config, delegate);

    // Metrics available at http://localhost:9090/metrics
}
```

### With Statsd

```rust
use metrics_exporter_statsd::StatsdBuilder;

fn main() {
    StatsdBuilder::new("127.0.0.1:8125")
        .install()
        .expect("failed to install Statsd exporter");

    let (plumtree, handle) = Plumtree::new(node_id, config, delegate);
}
```

## Programmatic Access

Access statistics directly via the Plumtree API:

```rust
// Peer statistics
let peer_stats = plumtree.peer_stats();
println!("Eager: {}, Lazy: {}", peer_stats.eager_count, peer_stats.lazy_count);

// Cache statistics
let cache_stats = plumtree.cache_stats();
println!("Cached messages: {}", cache_stats.entries);

// Seen map statistics
if let Some(seen_stats) = plumtree.seen_map_stats() {
    println!("Seen map utilization: {:.2}%", seen_stats.utilization * 100.0);
}

// Health report (aggregates multiple stats)
let health = plumtree.health();
println!("Status: {:?}", health.status);
```

## Best Practices

1. **Set up baseline monitoring** before deploying to production
2. **Alert on critical metrics** (no eager peers, high graft failures)
3. **Track trends** - sudden changes may indicate issues
4. **Correlate with application metrics** - message delivery timing
5. **Export to time-series database** for historical analysis

## See Also

- [Configuration Guide](configuration.md) - Tuning parameters
- [Architecture Overview](architecture.md) - Understanding metrics context
- [Background Tasks](background-tasks.md) - Task-specific metrics
