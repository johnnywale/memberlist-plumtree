# Deployment Guide

Best practices for deploying memberlist-plumtree in production environments.

## Pre-Deployment Checklist

- [ ] Review resource requirements (memory, CPU, network)
- [ ] Configure appropriate timeouts for your network
- [ ] Set up monitoring and alerting
- [ ] Plan for graceful shutdown
- [ ] Document seed node addresses
- [ ] Test failover scenarios

## Configuration Presets

### LAN Deployment (Same Datacenter)

```rust
let config = PlumtreeConfig::lan()
    .with_eager_fanout(3)
    .with_lazy_fanout(6);
```

Characteristics:
- Low latency (50ms IHave interval)
- Shorter cache TTL (30s)
- Aggressive optimization threshold (2)
- No ring neighbor protection (small clusters)

### WAN Deployment (Multiple Datacenters)

```rust
let config = PlumtreeConfig::wan()
    .with_eager_fanout(4)
    .with_lazy_fanout(8);
```

Characteristics:
- Higher latency tolerance (200ms IHave interval)
- Longer cache TTL (120s)
- Conservative optimization (threshold 4)
- Ring neighbor protection enabled
- Compression enabled (zstd level 3)

### Large Cluster (1000+ Nodes)

```rust
let config = PlumtreeConfig::large_cluster()
    .with_ihave_batch_size(64)
    .with_max_eager_peers(8)
    .with_max_lazy_peers(50);
```

Characteristics:
- Higher fanout (5 eager, 10 lazy)
- Larger IHave batches (32+)
- Hard caps on peer counts
- Ring topology protection

## Resource Requirements

### Memory

| Cluster Size | Cache Size | Estimated Memory |
|--------------|------------|------------------|
| <100 nodes | 10,000 msgs | ~50 MB |
| 100-1000 nodes | 20,000 msgs | ~100 MB |
| 1000+ nodes | 50,000 msgs | ~250 MB |

Memory formula:
```
memory = (cache_size * avg_message_size) + (total_peers * peer_state_size)
```

### CPU

- Baseline: Minimal (message forwarding is lightweight)
- Peak: During cluster churn or high broadcast rate
- Recommendation: 0.5-1 CPU core per 1000 messages/second

### Network

| Traffic Type | Formula |
|--------------|---------|
| Gossip out | `broadcast_rate * eager_fanout * avg_message_size` |
| IHave out | `broadcast_rate * lazy_fanout * 20 bytes` |
| Control | `peer_churn_rate * 100 bytes` |

## Deployment Patterns

### Single Region

```
┌─────────────────────────────────────────┐
│              Region A                    │
│  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐    │
│  │Node1│──│Node2│──│Node3│──│Node4│    │
│  └─────┘  └─────┘  └─────┘  └─────┘    │
│     Ring topology with eager links      │
└─────────────────────────────────────────┘
```

Configuration:
- Use `PlumtreeConfig::lan()` or default
- Seed nodes: 3-5 stable nodes
- Ring protection: Optional for small clusters

### Multi-Region

```
┌────────────────┐     ┌────────────────┐
│   Region A     │     │   Region B     │
│  ┌───┐  ┌───┐ │     │ ┌───┐  ┌───┐  │
│  │N1 │──│N2 │ │ WAN │ │N5 │──│N6 │  │
│  └───┘  └───┘ │─────│ └───┘  └───┘  │
│  ┌───┐  ┌───┐ │     │ ┌───┐  ┌───┐  │
│  │N3 │──│N4 │ │     │ │N7 │──│N8 │  │
│  └───┘  └───┘ │     │ └───┘  └───┘  │
└────────────────┘     └────────────────┘
```

Configuration:
- Use `PlumtreeConfig::wan()`
- Enable compression for cross-region traffic
- Configure longer graft timeouts (1-2s)
- Use zone-aware peer selection if available

### Kubernetes

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: plumtree-cluster
spec:
  serviceName: plumtree
  replicas: 5
  template:
    spec:
      containers:
      - name: plumtree
        env:
        - name: PLUMTREE_ADVERTISE_ADDR
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: PLUMTREE_SEEDS
          value: "plumtree-0.plumtree:7946,plumtree-1.plumtree:7946"
```

Considerations:
- Use StatefulSet for stable network identities
- Configure headless service for DNS discovery
- Set appropriate resource limits
- Use pod anti-affinity for availability

## Startup Sequence

1. **First Node**
   ```rust
   let stack = MemberlistStack::new(pm, memberlist, advertise_addr);
   stack.start(transport);
   // No join needed - this is the seed
   ```

2. **Subsequent Nodes**
   ```rust
   let stack = MemberlistStack::new(pm, memberlist, advertise_addr);
   stack.start(transport);
   stack.join(&seed_addresses).await?;
   ```

3. **Wait for Convergence**
   ```rust
   // Optional: wait for minimum peers
   while plumtree.peer_count() < min_peers {
       tokio::time::sleep(Duration::from_millis(100)).await;
   }
   ```

## Graceful Shutdown

```rust
// 1. Stop accepting new broadcasts
// 2. Leave the cluster gracefully
stack.leave().await?;

// 3. Wait for pending messages to drain
tokio::time::sleep(Duration::from_secs(2)).await;

// 4. Shutdown
stack.shutdown().await?;
```

Shutdown timeout recommendation: 5-10 seconds

## Rolling Updates

1. **One Node at a Time**
   - Leave cluster gracefully
   - Wait for membership to converge
   - Update and restart
   - Wait for rejoined confirmation

2. **Update Order**
   - Start with lazy peers
   - Leave seed nodes for last
   - Maintain quorum throughout

3. **Health Checks**
   ```rust
   let health = plumtree.health();
   match health.status {
       HealthStatus::Healthy => proceed_with_update(),
       _ => wait_and_retry(),
   }
   ```

## Security Considerations

### QUIC Transport (Recommended)

```rust
let config = QuicConfig::default()
    .with_mtls_verification()  // Require client certs
    .with_cert_path("certs/server.crt")
    .with_key_path("certs/server.key")
    .with_ca_path("certs/ca.crt");
```

### Network Policies

- Restrict Plumtree ports (default 7946) to cluster nodes only
- Use TLS/mTLS for all inter-node communication
- Implement rate limiting at network level

### Message Validation

```rust
impl PlumtreeDelegate<NodeId> for MyDelegate {
    fn on_deliver(&self, msg_id: MessageId, payload: Bytes) {
        // Always validate message content
        if !validate_message(&payload) {
            log::warn!("Invalid message received");
            return;
        }
        // Process valid message
    }
}
```
