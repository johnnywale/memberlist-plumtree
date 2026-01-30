# Monitoring Guide

Comprehensive guide to monitoring memberlist-plumtree in production.

## Enabling Metrics

Metrics are available when compiled with the `metrics` feature:

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", features = ["metrics"] }
```

## Available Metrics

### Counters (Monotonically Increasing)

| Metric | Description | Labels |
|--------|-------------|--------|
| `plumtree_messages_broadcast_total` | Total broadcasts initiated | - |
| `plumtree_messages_delivered_total` | Messages delivered to application | - |
| `plumtree_messages_duplicate_total` | Duplicate messages received | - |
| `plumtree_gossip_sent_total` | Gossip messages sent | - |
| `plumtree_ihave_sent_total` | IHave messages sent | - |
| `plumtree_graft_sent_total` | Graft messages sent | - |
| `plumtree_prune_sent_total` | Prune messages sent | - |
| `plumtree_peer_promotions_total` | Peers promoted to eager | - |
| `plumtree_peer_demotions_total` | Peers demoted to lazy | - |
| `plumtree_peer_added_total` | Peers added to cluster | - |
| `plumtree_peer_removed_total` | Peers removed from cluster | - |
| `plumtree_graft_success_total` | Successful Graft requests | - |
| `plumtree_graft_failed_total` | Failed Graft requests | - |
| `plumtree_graft_retries_total` | Graft retry attempts | - |

### Histograms (Distributions)

| Metric | Description | Buckets |
|--------|-------------|---------|
| `plumtree_graft_latency_seconds` | Time from Graft to delivery | 0.01, 0.05, 0.1, 0.5, 1, 5 |
| `plumtree_message_size_bytes` | Message payload size | 100, 1K, 10K, 64K, 256K |
| `plumtree_message_hops` | Number of hops messages travel | 1, 2, 3, 5, 10 |
| `plumtree_ihave_batch_size` | Size of IHave batches | 1, 4, 8, 16, 32, 64 |

### Gauges (Current Values)

| Metric | Description |
|--------|-------------|
| `plumtree_eager_peers` | Current eager peer count |
| `plumtree_lazy_peers` | Current lazy peer count |
| `plumtree_total_peers` | Total peer count |
| `plumtree_cache_size` | Messages in cache |
| `plumtree_ihave_queue_size` | Pending IHave announcements |
| `plumtree_pending_grafts` | Pending Graft requests |

## Alert Thresholds

### Critical Alerts

| Metric | Condition | Severity | Action |
|--------|-----------|----------|--------|
| `plumtree_eager_peers` | < 1 | Critical | Node is isolated, check network |
| `plumtree_graft_failed_total` rate | > 20/min | Critical | Tree repair failing, investigate |
| `plumtree_cache_size` | > 95% capacity | Critical | Memory pressure, increase cache or reduce TTL |
| `plumtree_messages_duplicate_total` rate | > 50% | Critical | Severe topology issues |

### Warning Alerts

| Metric | Condition | Severity | Action |
|--------|-----------|----------|--------|
| `plumtree_eager_peers` | < 2 | Warning | Reduced redundancy |
| `plumtree_graft_failed_total` rate | > 5/min | Warning | Some tree repair issues |
| `plumtree_cache_size` | > 80% capacity | Warning | Monitor memory usage |
| `plumtree_messages_duplicate_total` rate | > 30% | Warning | Review fanout settings |
| `plumtree_pending_grafts` | > 100 | Warning | Graft backlog building |

### Prometheus Alert Rules

```yaml
groups:
- name: plumtree
  rules:
  - alert: PlumtreeNodeIsolated
    expr: plumtree_eager_peers < 1
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Node {{ $labels.instance }} is isolated"
      description: "Node has no eager peers for more than 1 minute"

  - alert: PlumtreeHighGraftFailures
    expr: rate(plumtree_graft_failed_total[5m]) > 0.33
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "High Graft failure rate on {{ $labels.instance }}"
      description: "Graft failure rate is {{ $value }}/min"

  - alert: PlumtreeCachePressure
    expr: plumtree_cache_size / plumtree_cache_capacity > 0.95
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "Cache near capacity on {{ $labels.instance }}"

  - alert: PlumtreeHighDuplicateRate
    expr: rate(plumtree_messages_duplicate_total[5m]) / rate(plumtree_messages_delivered_total[5m]) > 0.5
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High duplicate message rate on {{ $labels.instance }}"

  - alert: PlumtreeLowPeerCount
    expr: plumtree_total_peers < 2
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "Low peer count on {{ $labels.instance }}"
```

## Grafana Dashboard

### Overview Panel

```json
{
  "title": "Plumtree Overview",
  "panels": [
    {
      "title": "Message Rate",
      "type": "graph",
      "targets": [
        {"expr": "rate(plumtree_messages_broadcast_total[5m])", "legendFormat": "Broadcast"},
        {"expr": "rate(plumtree_messages_delivered_total[5m])", "legendFormat": "Delivered"},
        {"expr": "rate(plumtree_messages_duplicate_total[5m])", "legendFormat": "Duplicate"}
      ]
    },
    {
      "title": "Peer Counts",
      "type": "graph",
      "targets": [
        {"expr": "plumtree_eager_peers", "legendFormat": "Eager"},
        {"expr": "plumtree_lazy_peers", "legendFormat": "Lazy"},
        {"expr": "plumtree_total_peers", "legendFormat": "Total"}
      ]
    },
    {
      "title": "Graft Operations",
      "type": "graph",
      "targets": [
        {"expr": "rate(plumtree_graft_sent_total[5m])", "legendFormat": "Sent"},
        {"expr": "rate(plumtree_graft_success_total[5m])", "legendFormat": "Success"},
        {"expr": "rate(plumtree_graft_failed_total[5m])", "legendFormat": "Failed"}
      ]
    },
    {
      "title": "Graft Latency",
      "type": "heatmap",
      "targets": [
        {"expr": "rate(plumtree_graft_latency_seconds_bucket[5m])"}
      ]
    }
  ]
}
```

### Key Queries

**Message Delivery Rate**
```promql
rate(plumtree_messages_delivered_total[5m])
```

**Duplicate Rate Percentage**
```promql
100 * rate(plumtree_messages_duplicate_total[5m]) / rate(plumtree_messages_delivered_total[5m])
```

**P99 Graft Latency**
```promql
histogram_quantile(0.99, rate(plumtree_graft_latency_seconds_bucket[5m]))
```

**Cluster Health Score**
```promql
clamp_max(
  (plumtree_eager_peers / 3) *
  (1 - rate(plumtree_graft_failed_total[5m]) / (rate(plumtree_graft_sent_total[5m]) + 0.001)),
  1
)
```

## Health Check Endpoint

Implement a health check endpoint:

```rust
async fn health_check(plumtree: &PlumtreeDiscovery<NodeId, MyDelegate>) -> HealthStatus {
    let health = plumtree.health();

    // Return HTTP status based on health
    match health.status {
        HealthStatus::Healthy => StatusCode::OK,
        HealthStatus::Degraded => StatusCode::SERVICE_UNAVAILABLE,
        HealthStatus::Unhealthy => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
```

Health check criteria:
- Eager peers >= 1 (connectivity)
- Cache utilization < 90% (memory)
- Graft failure rate < 10% (tree repair)
- Message delivery working (no backlog)

## Logging

### Recommended Log Levels

| Component | Level | When to Enable |
|-----------|-------|----------------|
| Protocol | INFO | Production |
| Protocol | DEBUG | Troubleshooting |
| Network | WARN | Production |
| Network | DEBUG | Network issues |
| Metrics | INFO | Always |

### Key Log Events

```rust
// Structured logging with tracing
#[instrument(skip(payload), fields(payload_size = payload.len()))]
pub async fn broadcast(&self, payload: Bytes) -> Result<MessageId> {
    // ...
}
```

Important events to log:
- Peer join/leave
- Graft failures
- Network partitions detected
- Cache evictions
- Configuration changes

## Tracing

Enable distributed tracing for message flow:

```rust
// Each message carries trace context
let span = tracing::info_span!(
    "broadcast",
    message_id = %msg_id,
    hops = 0
);
```

Trace key operations:
- Message broadcast origin
- Hop-by-hop forwarding
- Graft/Prune decisions
- Delivery to application
