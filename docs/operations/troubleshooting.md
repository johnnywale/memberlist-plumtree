# Troubleshooting Guide

Common issues and their solutions when operating memberlist-plumtree.

## Quick Diagnostics

### Check Cluster Health

```rust
let health = plumtree.health();
println!("Status: {:?}", health.status);
println!("Message: {}", health.message);
```

### Check Peer State

```rust
println!("Eager peers: {:?}", plumtree.eager_peers());
println!("Lazy peers: {:?}", plumtree.lazy_peers());
println!("Total peers: {}", plumtree.peer_count());
```

### Check Metrics

```bash
# Prometheus query for key metrics
curl -s localhost:9090/metrics | grep plumtree_
```

## Common Issues

### Message Delivery Failures

**Symptoms:**
- Messages not reaching all nodes
- High `plumtree_graft_failed_total` rate
- Nodes missing messages

**Diagnosis:**
```promql
# Check graft failure rate
rate(plumtree_graft_failed_total[5m])

# Check peer counts
plumtree_eager_peers
plumtree_lazy_peers

# Check for partitions
plumtree_messages_partitioned_total
```

**Solutions:**

1. **Network connectivity issues**
   - Verify firewall rules allow Plumtree port (default 7946)
   - Check for network partitions
   - Test connectivity between nodes

2. **Insufficient eager peers**
   - Increase `eager_fanout` configuration
   - Check if nodes are joining correctly
   - Verify memberlist connectivity

3. **Cache too small**
   - Increase `message_cache_max_size`
   - Messages may expire before Graft completes

4. **Graft timeout too short**
   - Increase `graft_timeout` for high-latency networks
   - Use WAN preset for cross-datacenter deployments

### High Duplicate Rate

**Symptoms:**
- `plumtree_messages_duplicate_total` growing rapidly
- Duplicate rate > 30%
- Excessive network traffic

**Diagnosis:**
```promql
# Duplicate rate percentage
100 * rate(plumtree_messages_duplicate_total[5m]) /
      rate(plumtree_messages_delivered_total[5m])

# Prune rate (should be non-zero if duplicates detected)
rate(plumtree_prune_sent_total[5m])
```

**Solutions:**

1. **Eager fanout too high**
   - Reduce `eager_fanout` to 3-4
   - System should self-optimize via Prune

2. **Optimization threshold too high**
   - Reduce `optimization_threshold` to send Prune earlier
   - Default of 3 is usually good

3. **Ring protection preventing optimization**
   - Check `protect_ring_neighbors` setting
   - Protected neighbors cannot be pruned

### Node Isolation

**Symptoms:**
- `plumtree_eager_peers` = 0
- Node not receiving any messages
- Health status: Unhealthy

**Diagnosis:**
```rust
let health = plumtree.health();
if health.peer_health.eager_count == 0 {
    println!("Node is isolated!");
    println!("Lazy peers: {}", health.peer_health.lazy_count);
}
```

**Solutions:**

1. **Check memberlist connectivity**
   ```rust
   // Verify memberlist sees other nodes
   let members = memberlist.members().await;
   println!("Memberlist sees {} members", members.len());
   ```

2. **Force peer promotion**
   ```rust
   // If lazy peers exist, they should eventually be promoted
   // via IHave/Graft mechanism
   ```

3. **Restart and rejoin**
   - Leave cluster gracefully
   - Rejoin with fresh seed addresses

4. **Check network configuration**
   - Verify advertise address is reachable
   - Check NAT/firewall configuration

### Memory Growth

**Symptoms:**
- RSS memory increasing over time
- `plumtree_cache_size` near capacity
- OOM events

**Diagnosis:**
```promql
# Cache utilization
plumtree_cache_size / plumtree_cache_capacity

# Message rate (correlates with cache growth)
rate(plumtree_messages_delivered_total[5m])
```

**Solutions:**

1. **Reduce cache TTL**
   ```rust
   PlumtreeConfig::default()
       .with_message_cache_ttl(Duration::from_secs(30))
   ```

2. **Reduce cache max size**
   ```rust
   PlumtreeConfig::default()
       .with_message_cache_max_size(5000)
   ```

3. **Increase cleanup frequency**
   - Cache cleanup is automatic
   - Check `CleanupTuner` settings

4. **Profile memory usage**
   - Use heaptrack or similar tools
   - Check for leaks in delegate implementation

### High Latency

**Symptoms:**
- Slow message delivery
- High `plumtree_graft_latency_seconds` p99
- User-perceived delays

**Diagnosis:**
```promql
# P99 graft latency
histogram_quantile(0.99, rate(plumtree_graft_latency_seconds_bucket[5m]))

# Message hop count
histogram_quantile(0.99, rate(plumtree_message_hops_bucket[5m]))
```

**Solutions:**

1. **Reduce IHave batching delay**
   ```rust
   PlumtreeConfig::default()
       .with_ihave_interval(Duration::from_millis(50))
   ```

2. **Increase eager fanout**
   - More eager peers = faster initial delivery
   - Trade-off: higher network overhead

3. **Enable 0-RTT for QUIC**
   - Reduces reconnection latency
   - See QUIC transport configuration

4. **Use geographic-aware peer selection**
   - Prefer low-latency peers for eager set

### Cluster Partitions

**Symptoms:**
- Subset of nodes not communicating
- Messages reaching some nodes but not others
- Split-brain behavior

**Diagnosis:**
```promql
# Check for partition indicators
plumtree_messages_partitioned_total

# Compare message counts across instances
plumtree_messages_delivered_total
```

**Solutions:**

1. **Verify network connectivity**
   - Check firewall rules
   - Test direct connectivity between partitions

2. **Wait for automatic healing**
   - Plumtree will attempt to reconnect
   - Graft mechanism repairs tree

3. **Manual intervention**
   - Restart nodes in minority partition
   - Ensure seed addresses span partitions

## Diagnostic Commands

### Dump Peer State

```rust
fn dump_peer_state(plumtree: &PlumtreeDiscovery<NodeId, MyDelegate>) {
    println!("=== Peer State ===");
    println!("Eager: {:?}", plumtree.eager_peers());
    println!("Lazy: {:?}", plumtree.lazy_peers());
    println!("Protected: {:?}", plumtree.protected_neighbors());
}
```

### Dump Health Report

```rust
fn dump_health(plumtree: &PlumtreeDiscovery<NodeId, MyDelegate>) {
    let health = plumtree.health();
    println!("=== Health Report ===");
    println!("Status: {:?}", health.status);
    println!("Peer Health: {:?}", health.peer_health);
    println!("Delivery Health: {:?}", health.delivery_health);
    println!("Cache Health: {:?}", health.cache_health);
}
```

### Test Message Delivery

```rust
async fn test_delivery(stack: &MemberlistStack<NodeId, MyDelegate>) {
    let test_payload = b"diagnostic_test_message";
    let msg_id = stack.broadcast(test_payload).await?;
    println!("Sent test message: {:?}", msg_id);

    // Check if received locally
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Verify delivery via delegate
}
```

## Log Analysis

### Key Log Patterns

**Graft Failure**
```
WARN plumtree: Graft failed for message {msg_id} after {retries} retries
```
Action: Check network connectivity to target peer

**Peer Demotion**
```
DEBUG plumtree: Demoting peer {peer} to lazy (optimization threshold reached)
```
Action: Normal behavior, no action needed

**Cache Eviction**
```
DEBUG plumtree: Evicting {count} messages from cache (TTL expired)
```
Action: Normal behavior, verify cache sizing

**Partition Detected**
```
WARN plumtree: Possible partition detected - cannot reach peers: {peers}
```
Action: Investigate network connectivity

## When to Escalate

Escalate to development team if:
- Consistent message loss despite healthy metrics
- Crash/panic in Plumtree code
- Memory leak not related to delegate implementation
- Protocol invariant violation
- Data corruption in messages

Provide:
- Full health report
- Metric snapshots for last hour
- Debug logs from affected time period
- Configuration dump
- Cluster topology
