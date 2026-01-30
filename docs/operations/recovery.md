# Disaster Recovery Guide

Procedures for recovering from various failure scenarios.

## Recovery Scenarios

### Isolated Node

**Situation:** A single node has lost connectivity to the cluster.

**Symptoms:**
- `plumtree_eager_peers` = 0
- `plumtree_lazy_peers` = 0
- Messages not being delivered

**Recovery Steps:**

1. **Verify network connectivity**
   ```bash
   # Test connectivity to seed nodes
   nc -zv seed1.example.com 7946
   nc -zv seed2.example.com 7946
   ```

2. **Check memberlist status**
   ```rust
   let members = memberlist.members().await;
   if members.is_empty() {
       println!("Memberlist has no members - rejoining required");
   }
   ```

3. **Graceful rejoin**
   ```rust
   // Leave if still connected
   stack.leave().await?;

   // Wait for state to clear
   tokio::time::sleep(Duration::from_secs(5)).await;

   // Rejoin
   stack.join(&seed_addresses).await?;
   ```

4. **If rejoin fails, restart**
   ```bash
   # Full restart
   systemctl restart plumtree
   ```

### Network Partition

**Situation:** Cluster split into two or more groups that cannot communicate.

**Symptoms:**
- Different nodes report different peer lists
- Messages delivered to subset of cluster
- `plumtree_messages_partitioned_total` increasing

**Recovery Steps:**

1. **Identify the partition**
   ```bash
   # Check peer lists from multiple nodes
   curl node1:8080/debug/peers
   curl node2:8080/debug/peers
   curl node3:8080/debug/peers
   ```

2. **Determine root cause**
   - Network switch failure
   - Firewall rule change
   - Cloud provider network issue

3. **Fix underlying network issue**

4. **Wait for automatic recovery**
   - Plumtree will automatically reconnect
   - Graft mechanism repairs the tree
   - Allow 2-5 minutes for convergence

5. **If automatic recovery fails**
   ```rust
   // Force rebalance on each node
   plumtree.rebalance_peers().await?;
   ```

### Multiple Node Failure

**Situation:** Multiple nodes fail simultaneously (e.g., zone failure).

**Symptoms:**
- Sudden drop in `plumtree_total_peers`
- Increased `plumtree_graft_failed_total`
- Message delivery delays

**Recovery Steps:**

1. **Assess remaining cluster health**
   ```promql
   # How many nodes remain?
   count(up{job="plumtree"} == 1)

   # Are remaining nodes healthy?
   min(plumtree_eager_peers)
   ```

2. **If majority survives**
   - Cluster will self-heal
   - Monitor for stabilization
   - Replace failed nodes when ready

3. **If minority survives**
   - May need manual intervention
   - Consider stopping all nodes and restarting fresh
   ```bash
   # Coordinated restart
   for node in node1 node2 node3; do
       ssh $node systemctl stop plumtree
   done
   sleep 10
   for node in node1 node2 node3; do
       ssh $node systemctl start plumtree
   done
   ```

### Total Cluster Loss

**Situation:** All nodes have failed or cluster state is corrupted.

**Recovery Steps:**

1. **Stop all nodes**
   ```bash
   for node in $(cat cluster_nodes.txt); do
       ssh $node systemctl stop plumtree
   done
   ```

2. **Clear any persistent state** (if using storage feature)
   ```bash
   for node in $(cat cluster_nodes.txt); do
       ssh $node rm -rf /var/lib/plumtree/data/*
   done
   ```

3. **Restart seed nodes first**
   ```bash
   # Start seeds
   for node in seed1 seed2 seed3; do
       ssh $node systemctl start plumtree
   done
   sleep 10
   ```

4. **Start remaining nodes**
   ```bash
   for node in $(cat non_seed_nodes.txt); do
       ssh $node systemctl start plumtree
       sleep 2  # Stagger starts
   done
   ```

5. **Verify cluster formation**
   ```bash
   # Check peer counts
   curl seed1:8080/metrics | grep plumtree_total_peers
   ```

### Message Cache Corruption

**Situation:** Cache contains corrupted or invalid messages.

**Symptoms:**
- Delegate receiving malformed messages
- Crashes during message processing
- Unexpected behavior

**Recovery Steps:**

1. **Identify affected nodes**

2. **Clear cache on affected nodes**
   ```rust
   // If API available
   plumtree.clear_cache();
   ```

3. **Or restart affected nodes**
   - Cache is in-memory, restart clears it

4. **If using persistent storage**
   ```bash
   # Stop node
   systemctl stop plumtree

   # Clear storage
   rm -rf /var/lib/plumtree/data/cache/*

   # Start node
   systemctl start plumtree
   ```

### Split Brain with Data Divergence

**Situation:** Different partitions processed different messages during split.

**Impact:**
- Some nodes received messages A, B
- Other nodes received messages C, D
- After heal, state may be inconsistent

**Recovery Steps:**

1. **Enable anti-entropy sync** (if not already)
   ```rust
   PlumtreeConfig::default()
       .with_sync(SyncConfig::enabled())
   ```

2. **Wait for sync to complete**
   - Anti-entropy will reconcile differences
   - Monitor `plumtree_sync_*` metrics

3. **If sync unavailable, application-level reconciliation**
   - Application must handle eventual consistency
   - Consider idempotent message design

## Preventive Measures

### High Availability Configuration

```rust
PlumtreeConfig::default()
    .with_eager_fanout(4)           // More redundancy
    .with_hash_ring(true)           // Deterministic topology
    .with_protect_ring_neighbors(true)  // Protect critical links
    .with_max_protected_neighbors(4)    // Z≥2 redundancy
```

### Multi-Zone Deployment

```
Zone A          Zone B          Zone C
┌─────┐         ┌─────┐         ┌─────┐
│ N1  │         │ N3  │         │ N5  │
│(seed)│────────│(seed)│────────│(seed)│
└─────┘         └─────┘         └─────┘
   │               │               │
┌─────┐         ┌─────┐         ┌─────┐
│ N2  │         │ N4  │         │ N6  │
└─────┘         └─────┘         └─────┘
```

- Distribute seed nodes across zones
- Ensure cross-zone connectivity
- Configure higher timeouts for cross-zone traffic

### Backup and State Export

```rust
// Export current peer state (for debugging)
fn export_state(plumtree: &PlumtreeDiscovery<NodeId, MyDelegate>) -> StateExport {
    StateExport {
        eager_peers: plumtree.eager_peers(),
        lazy_peers: plumtree.lazy_peers(),
        cache_size: plumtree.cache_size(),
        timestamp: Utc::now(),
    }
}
```

### Regular Health Checks

Implement automated health checks:

```rust
async fn health_check_loop(plumtree: Arc<PlumtreeDiscovery<NodeId, MyDelegate>>) {
    loop {
        let health = plumtree.health();

        if health.status == HealthStatus::Unhealthy {
            // Alert on-call
            alert_pagerduty("Plumtree node unhealthy", &health.message).await;
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
```

## Recovery Runbook Template

```markdown
## Incident: [DESCRIPTION]

### Timeline
- HH:MM - Issue detected
- HH:MM - Investigation started
- HH:MM - Root cause identified
- HH:MM - Recovery initiated
- HH:MM - Service restored

### Impact
- Duration: X minutes
- Messages affected: Y
- Nodes affected: Z

### Root Cause
[Description]

### Resolution
1. [Step 1]
2. [Step 2]
3. [Step 3]

### Prevention
- [Preventive measure 1]
- [Preventive measure 2]

### Monitoring Improvements
- [New alert 1]
- [Dashboard update 1]
```
