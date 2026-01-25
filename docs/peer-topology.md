# Peer Topology and Promotion

This document explains how memberlist-plumtree manages peer relationships, including the eager/lazy classification, hash ring topology, and the mechanisms that trigger lazy-to-eager promotion.

## Overview

Plumtree maintains two peer sets:

| Set | Purpose | Receives |
|-----|---------|----------|
| **Eager** | Spanning tree edges | Full Gossip messages |
| **Lazy** | Reliability backup | IHave announcements only |

New peers always start as **lazy**. They are promoted to **eager** through one of several mechanisms:

1. **Graft mechanism** - When a lazy peer has a message we need
2. **Rebalancing** - Background maintenance to maintain target fanout
3. **Ring neighbor protection** - Hash ring neighbors are auto-promoted

## When Lazy Peers Become Eager

### 1. Graft Mechanism (Protocol-Driven)

The most common promotion path:

```
Node A (lazy peer of Node B)
    │
    │  Broadcasts message M
    │
    ▼
Node B receives IHave(M) from Node A
    │
    │  B doesn't have message M
    │
    ▼
B promotes A to eager
B sends Graft(M) to A
    │
    ▼
A sends Gossip(M) to B
A promotes B to eager
```

**Code path:**
```rust
// In Plumtree::handle_ihave()
if !self.seen.contains(&message_id) {
    // Unknown message - promote sender to eager
    self.peers.promote_to_eager(&from);
    self.send_graft(&from, &message_id).await;
}
```

### 2. Rebalancing (Maintenance-Driven)

The maintenance loop periodically checks if eager count is below `eager_fanout`:

```rust
// In Plumtree::run_maintenance_loop()
if self.peers.eager_count() < self.config.eager_fanout {
    let result = self.peers.rebalance(self.config.eager_fanout);
    for peer in result.promoted {
        // Peer was promoted from lazy to eager
    }
}
```

**Promotion selection** uses hybrid scoring (see [Peer Scoring](#peer-scoring) below).

### 3. Hash Ring Neighbor Protection

When hash ring topology is enabled, ring neighbors (i±1, i±2) are automatically promoted:

```rust
// Ring neighbors are always promoted during rebalance
let result = peer_state.rebalance_with_scorer(eager_fanout, |peer| {
    peer_scoring.normalized_score(peer, 0.5)
});
// Ring neighbors are selected first, regardless of score
```

## Hash Ring Topology

The hash ring provides deterministic peer selection for consistent topology across all nodes.

### Ring Structure

All known peers (including local node) are sorted by stable hash to form a logical ring:

```
        ┌─────┐
    ┌───┤  A  ├───┐
    │   └─────┘   │
┌───▼───┐     ┌───▼───┐
│   F   │     │   B   │
└───┬───┘     └───┬───┘
    │             │
┌───▼───┐     ┌───▼───┐
│   E   │     │   C   │
└───┬───┘     └───┬───┘
    │   ┌─────┐   │
    └───┤  D  ├───┘
        └─────┘
```

### Ring Connections

For a node at position `i` in a ring of size `X`:

| Connection Type | Positions | Purpose |
|----------------|-----------|---------|
| **Adjacent** | `i±1` | Basic ring connectivity |
| **Second-nearest** | `i±2` | Z=2 redundancy (tolerate neighbor failure) |
| **Long-range jumps** | `i±X/4` | Reduce network diameter |

### Ring Neighbor Protection

Adjacent and second-nearest neighbors are **protected**:
- Always promoted to eager (during rebalance)
- Cannot be demoted via Prune
- Can only be removed when leaving the cluster

This guarantees Z≥2 redundancy regardless of network conditions.

### Enabling Hash Ring Topology

```rust
// Option 1: Using PeerStateBuilder
let peer_state = PeerStateBuilder::new()
    .with_local_id(my_node_id)
    .with_peers(known_peers)
    .use_hash_ring(true)
    .with_eager_fanout(3)
    .build();

// Option 2: Direct construction
let peer_state = PeerState::with_initial_peers_hash_ring(
    my_node_id,
    known_peers,
    eager_fanout,
    lazy_fanout,
);
```

## Peer Scoring

The peer scoring system tracks RTT (Round-Trip Time) and reliability to optimize the eager set for performance.

### How Scoring Works

1. **RTT Measurement**: Samples recorded via `record_peer_rtt()` (must be called by user)
2. **Exponential Moving Average**: Smooths RTT variations
3. **Failure Tracking**: Timeouts and errors tracked with exponential decay
4. **Score Calculation**:

```
base_score = 1000 / (rtt_ema + floor)^exponent
final_score = base_score / (1 + loss_rate × loss_penalty)
```

**Note**: RTT is not automatically recorded by the protocol. You must call `record_peer_rtt()` when you have RTT measurements (e.g., from your transport layer, ping responses, or application-level timing).

### Configuration

```rust
use memberlist_plumtree::ScoringConfig;

// Presets
let config = ScoringConfig::default();  // General purpose
let config = ScoringConfig::lan();      // Low latency, responsive
let config = ScoringConfig::wan();      // High latency, stable

// Custom
let config = ScoringConfig::default()
    .with_ema_alpha(0.3)                        // Responsiveness (0.0-1.0)
    .with_max_sample_age(Duration::from_secs(300))
    .with_initial_rtt_estimate(Duration::from_millis(100))
    .with_failure_decay_halflife(Duration::from_secs(1800));
```

### Recording RTT Samples

RTT samples must be recorded manually by the user. The Plumtree struct provides convenience methods:

```rust
// Via Plumtree instance (recommended)
plumtree.record_peer_rtt(&peer_id, Duration::from_millis(15));
plumtree.record_peer_failure(&peer_id);

// Or directly via PeerScoring
use memberlist_plumtree::PeerScoring;

let scoring = PeerScoring::new(ScoringConfig::default());
scoring.record_rtt(&peer_id, Duration::from_millis(15));
scoring.record_failure(&peer_id);
```

**When to record RTT:**
- After receiving a response to a request (Graft → Gossip)
- From transport-level RTT (QUIC connection stats, TCP RTT)
- From application-level ping/pong timing

### Using Scores for Rebalancing

```rust
// Rebalance with network-aware peer selection
let result = peer_state.rebalance_with_scorer(eager_fanout, |peer| {
    scoring.normalized_score(peer, 0.5)  // 0.5 = default for unknown peers
});

for peer in result.promoted {
    println!("Promoted {} (low RTT)", peer);
}

for peer in result.demoted {
    println!("Demoted {} (high RTT or failures)", peer);
}
```

### Hybrid Scoring

When rebalancing, peers are scored using a weighted combination:

```
hybrid_score = (topology_score × 0.4) + (network_score × 0.6)
```

- **Topology score**: Based on hash ring distance (closer = higher)
- **Network score**: From peer scoring (lower RTT = higher)

**Priority order during promotion:**
1. Ring neighbors (always first - structural requirement)
2. Highest hybrid score among remaining lazy peers

**Priority order during demotion:**
1. Ring neighbors are protected (never demoted)
2. Lowest hybrid score among non-protected eager peers

## Peer Statistics

```rust
// Get peer counts
let stats = peer_state.stats();
println!("Eager: {}, Lazy: {}", stats.eager_count, stats.lazy_count);

// Get topology snapshot
let topology = peer_state.topology();
println!("Eager peers: {:?}", topology.eager);
println!("Lazy peers: {:?}", topology.lazy);

// Get scoring statistics
let scoring_stats = peer_scoring.stats();
println!("Avg RTT: {:.2}ms", scoring_stats.avg_rtt_ms);
println!("Reliable peers: {:.1}%", scoring_stats.reliable_percentage);

// Get best/worst peers by score
let best = peer_scoring.best_peers(3);
let worst = peer_scoring.worst_peers(3);
```

## Demotion: Eager to Lazy

Peers are demoted from eager to lazy through:

### 1. Prune Mechanism (Protocol-Driven)

When a node receives duplicate Gossip messages above a threshold, it sends Prune:

```rust
// In Plumtree::handle_gossip()
if duplicate_count >= self.config.duplicate_threshold {
    self.send_prune(&from).await;
    self.peers.demote_to_lazy(&from);
}
```

The sender responds by demoting the pruning node to lazy.

### 2. Rebalancing with Excess Eager Peers

If eager count exceeds target (rare), rebalancing demotes the lowest-scoring peers:

```rust
let result = peer_state.rebalance_with_scorer(target_eager, |peer| score);
// result.demoted contains peers moved to lazy
```

### Protected Peers

Ring neighbors (i±1, i±2) are **never demoted**, regardless of:
- Prune requests
- Low network scores
- Rebalancing

They can only be removed when the peer leaves the cluster entirely.

## Promotion Flow Diagram

```
                    ┌─────────────────┐
                    │   New Peer      │
                    │ (add_peer)      │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   LAZY          │◄────────────────┐
                    │                 │                 │
                    └────────┬────────┘                 │
                             │                          │
         ┌───────────────────┼───────────────────┐     │
         │                   │                   │     │
         ▼                   ▼                   ▼     │
┌─────────────────┐ ┌─────────────────┐ ┌────────────┐│
│ Receive IHave   │ │   Rebalance     │ │ Ring       ││
│ for unknown msg │ │ (maintenance)   │ │ Neighbor   ││
└────────┬────────┘ └────────┬────────┘ └─────┬──────┘│
         │                   │                │       │
         │ Send Graft        │ Select by      │ Auto  │
         │                   │ hybrid score   │       │
         └───────────────────┴────────────────┘       │
                             │                        │
                             ▼                        │
                    ┌─────────────────┐               │
                    │   EAGER         │               │
                    │                 │───────────────┘
                    └─────────────────┘   Prune / Demote
                                         (if not protected)
```

## Configuration Reference

### PeerState Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `eager_fanout` | 3 | Target number of eager peers |
| `lazy_fanout` | 6 | Target number of lazy peers |
| `max_peers` | None | Maximum total peers (with eviction) |

### ScoringConfig Settings

| Parameter | Default | LAN | WAN | Description |
|-----------|---------|-----|-----|-------------|
| `ema_alpha` | 0.3 | 0.5 | 0.2 | RTT smoothing factor |
| `max_sample_age` | 5min | 3min | 10min | Stale data threshold |
| `initial_rtt_estimate` | 100ms | 10ms | 200ms | Default for new peers |
| `min_samples` | 3 | 2 | 5 | Samples before reliable |
| `loss_penalty` | 2.0 | 3.0 | 1.5 | Failure penalty multiplier |
| `failure_decay_halflife` | 30min | 15min | 60min | Failure forgiveness rate |

## Troubleshooting

### Eager Peers Below Target

**Symptom**: `plumtree_eager_peers` consistently below `eager_fanout`

**Causes**:
1. Not enough known peers
2. Maintenance loop not running
3. All peers are already eager

**Solutions**:
- Check `plumtree_total_peers` metric
- Verify `run_maintenance_loop()` is spawned
- Check logs for rebalance activity

### High Promotion/Demotion Churn

**Symptom**: Frequent peer state changes

**Causes**:
1. Unstable network causing RTT fluctuations
2. High duplicate rate triggering Prunes
3. Aggressive scoring configuration

**Solutions**:
- Increase `ema_alpha` for smoother RTT tracking
- Increase `duplicate_threshold` in PlumtreeConfig
- Use WAN presets for unstable networks

### Ring Neighbors Not Protected

**Symptom**: Ring neighbors being demoted

**Causes**:
1. Hash ring not enabled (`use_hash_ring: false`)
2. `local_id` not set

**Solutions**:
- Use `PeerState::new_with_local_id()` or builder with `use_hash_ring(true)`
- Verify `local_id` is set before adding peers

## See Also

- [Architecture Overview](architecture.md) - System design
- [Adaptive Features](adaptive-features.md) - Adaptive batcher and cleanup tuner
- [Configuration Guide](configuration.md) - Full configuration reference
- [Metrics Reference](metrics.md) - Monitoring peer topology
