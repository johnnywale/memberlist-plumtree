# Adaptive Features

memberlist-plumtree includes adaptive systems that automatically tune protocol parameters based on runtime conditions. This document covers the Adaptive Batcher and Cleanup Tuner.

## Overview

| Feature | Adjusts | Based On |
|---------|---------|----------|
| **Adaptive Batcher** | IHave batch size | Latency, Graft success, throughput |
| **Cleanup Tuner** | Cleanup interval, batch size | Cache utilization, message rate |

These features operate automatically once enabled, requiring minimal configuration.

## Adaptive Batcher

The Adaptive Batcher dynamically adjusts the IHave batch size to optimize the trade-off between:
- **Smaller batches**: Lower latency for tree repair
- **Larger batches**: Reduced packet overhead under load

### How It Works

```
                    ┌─────────────────────────────┐
                    │     Network Observations    │
                    │  - RTT measurements         │
                    │  - Graft success/failure    │
                    │  - Message throughput       │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │     Adaptive Batcher        │
                    │  - Calculate optimal size   │
                    │  - Apply momentum/hysteresis│
                    │  - Clamp to min/max         │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │  Recommended Batch Size     │
                    │  (used by IHave scheduler)  │
                    └─────────────────────────────┘
```

### Adjustment Factors

| Factor | Effect |
|--------|--------|
| **Low latency** (<10ms) | Decrease batch size (faster tree repair) |
| **High latency** (>100ms) | Increase batch size (amortize overhead) |
| **Low Graft success** (<70%) | Decrease batch size (over-batching) |
| **High throughput** (>100 msg/s) | Increase batch size (reduce packets) |
| **Large cluster** | Logarithmic increase |

### Configuration

```rust
use memberlist_plumtree::adaptive_batcher::{AdaptiveBatcher, BatcherConfig};

// Presets
let batcher = AdaptiveBatcher::new(BatcherConfig::default());
let batcher = AdaptiveBatcher::new(BatcherConfig::lan());
let batcher = AdaptiveBatcher::new(BatcherConfig::wan());
let batcher = AdaptiveBatcher::new(BatcherConfig::large_cluster());

// Custom configuration
let config = BatcherConfig::default()
    .with_min_batch_size(4)           // Floor
    .with_max_batch_size(64)          // Ceiling
    .with_target_batch_size(16)       // Starting point
    .with_momentum(0.3)               // Damping factor (0.0-1.0)
    .with_hysteresis(0.1);            // Change threshold (0.0-1.0)
```

### Configuration Presets

| Parameter | Default | LAN | WAN | Large Cluster |
|-----------|---------|-----|-----|---------------|
| `min_batch_size` | 4 | 4 | 8 | 16 |
| `max_batch_size` | 64 | 32 | 128 | 128 |
| `target_batch_size` | 16 | 8 | 32 | 48 |
| `low_latency_threshold` | 10ms | 2ms | 50ms | 20ms |
| `high_latency_threshold` | 100ms | 20ms | 200ms | 150ms |
| `min_graft_success_rate` | 70% | 80% | 60% | 65% |
| `high_throughput_threshold` | 100/s | 500/s | 50/s | 200/s |
| `momentum` | 0.3 | 0.4 | 0.25 | 0.3 |
| `hysteresis` | 0.1 | 0.15 | 0.08 | 0.1 |

### Usage

```rust
let batcher = AdaptiveBatcher::new(BatcherConfig::default());

// Record observations (automatic in Plumtree internals)
batcher.record_ihave_sent(16);
batcher.record_graft_received();  // Success
batcher.record_graft_timeout();   // Failure
batcher.record_latency(Duration::from_millis(25));
batcher.record_message();         // Atomic, lock-free

// Get recommended batch size
let batch_size = batcher.recommended_batch_size();

// Hint cluster size for scaling
batcher.set_cluster_size_hint(500);

// Check statistics
let stats = batcher.stats();
println!("Batch size: {} ({})",
    stats.current_batch_size,
    match stats.trend {
        BatchSizeTrend::Increasing => "↑",
        BatchSizeTrend::Decreasing => "↓",
        BatchSizeTrend::Stable => "→",
    }
);
println!("Graft success: {:.1}%", stats.graft_success_rate * 100.0);
println!("Adjustments: {}", stats.adjustments);
```

### Stability Features

**Momentum**: Dampens rapid changes
```
new_batch = momentum × previous + (1 - momentum) × target
```

**Hysteresis**: Prevents oscillation
```
if |change_ratio| < hysteresis_threshold:
    keep current batch size
```

**Deadband**: Ignores insufficient data
```
if total_grafts < 20:
    skip Graft success adjustment
```

## Cleanup Tuner

The Cleanup Tuner dynamically adjusts cache cleanup parameters based on memory pressure and load.

### How It Works

```
                    ┌─────────────────────────────┐
                    │     System Observations     │
                    │  - Cache utilization        │
                    │  - Message rate             │
                    │  - Cleanup duration         │
                    │  - Removal efficiency       │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │      Cleanup Tuner          │
                    │  - Calculate interval       │
                    │  - Calculate batch size     │
                    │  - Generate backpressure    │
                    └─────────────┬───────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────┐
                    │    CleanupParameters        │
                    │  - interval: Duration       │
                    │  - batch_size: usize        │
                    │  - aggressive: bool         │
                    │  - backpressure: Hint       │
                    └─────────────────────────────┘
```

### Adjustment Factors

| Factor | Effect |
|--------|--------|
| **High utilization** (>80%) | Shorter interval, aggressive cleanup |
| **Low utilization** (<30%) | Longer interval, relaxed cleanup |
| **Critical utilization** (>93%) | Backpressure hints |
| **High message rate** | Balance cleanup vs processing |
| **Slow cleanup** | Smaller batch size |
| **Low removal efficiency** | Scan more entries |

### Configuration

```rust
use memberlist_plumtree::cleanup_tuner::{CleanupTuner, CleanupConfig};

// Presets
let tuner = CleanupTuner::new(CleanupConfig::default());
let tuner = CleanupTuner::new(CleanupConfig::high_throughput());
let tuner = CleanupTuner::new(CleanupConfig::low_latency());

// Custom configuration
let config = CleanupConfig::default()
    .with_base_interval(Duration::from_secs(30))
    .with_min_interval(Duration::from_secs(5))
    .with_max_interval(Duration::from_secs(120))
    .with_high_utilization_threshold(0.8)
    .with_low_utilization_threshold(0.3)
    .with_ttl_divisor(4.0);  // Cleanup at least 4× per TTL
```

### Configuration Presets

| Parameter | Default | High Throughput | Low Latency |
|-----------|---------|-----------------|-------------|
| `base_interval` | 30s | 15s | 20s |
| `min_interval` | 5s | 2s | 5s |
| `max_interval` | 120s | 60s | 60s |
| `high_utilization_threshold` | 0.8 | 0.7 | 0.75 |
| `low_utilization_threshold` | 0.3 | 0.2 | 0.25 |
| `target_cleanup_duration` | 50ms | 25ms | 10ms |
| `high_load_message_rate` | 1000/s | 5000/s | 500/s |

### Backpressure Levels

When cache utilization is critical, the tuner provides backpressure hints:

| Level | Utilization | Recommendation |
|-------|-------------|----------------|
| `None` | <93% | Normal operation |
| `DropSome` | ≥93% | Drop lower-priority messages |
| `DropMost` | ≥96% | Drop most non-critical messages |
| `BlockNew` | ≥98% | Temporarily block new messages |

### Usage

```rust
let tuner = CleanupTuner::new(CleanupConfig::default());

// Record messages (lock-free, very fast)
tuner.record_message();
tuner.record_messages(100);  // Batch recording for high throughput

// Get cleanup parameters
let cache_utilization = 0.75;  // 75% full
let ttl = Duration::from_secs(60);
let params = tuner.get_parameters(cache_utilization, ttl);

println!("Next cleanup in: {:?}", params.interval);
println!("Batch size: {}", params.batch_size);
println!("Aggressive: {}", params.aggressive);

// Handle backpressure
match params.backpressure_hint() {
    BackpressureHint::None => { /* Normal operation */ }
    BackpressureHint::DropSome => {
        // Consider dropping lower-priority messages
    }
    BackpressureHint::DropMost => {
        // Only process critical messages
    }
    BackpressureHint::BlockNew => {
        // Temporarily stop accepting new messages
    }
}

// After cleanup completes
tuner.record_cleanup(cleanup_duration, entries_removed, &params);

// Check statistics
let stats = tuner.stats();
println!("Cleanup cycles: {}", stats.cleanup_cycles);
println!("Avg duration: {:?}", stats.avg_cleanup_duration);
println!("Removal efficiency: {:.1}%", stats.avg_removal_efficiency * 100.0);
println!("Efficiency trend: {:?}", stats.efficiency_trend);
println!("Pressure trend: {:?}", stats.pressure_trend);
```

### Efficiency Tracking

The tuner tracks removal efficiency (entries removed / entries scanned) to optimize batch sizes:

| Efficiency | Meaning | Action |
|------------|---------|--------|
| <15% | Scanning many, removing few | Increase batch size |
| 25-60% | Target range | Maintain |
| >85% | Very efficient | Decrease batch size |

Efficiency trends are tracked via EMA for smooth adjustments.

### TTL Constraint

The cleanup interval is always at least `TTL / ttl_divisor`:

```rust
// With TTL = 60s and ttl_divisor = 4
// Minimum interval = 60s / 4 = 15s
// Ensures entries are cleaned 4× per TTL period
```

## Integration Example

Both adaptive features can be integrated with Plumtree:

```rust
use memberlist_plumtree::{
    Plumtree, PlumtreeConfig,
    adaptive_batcher::{AdaptiveBatcher, BatcherConfig},
    cleanup_tuner::{CleanupTuner, CleanupConfig},
};

// Create adaptive components
let batcher = AdaptiveBatcher::new(BatcherConfig::default());
let cleanup_tuner = CleanupTuner::new(CleanupConfig::default());

// Create Plumtree with config
let config = PlumtreeConfig::default()
    .with_ihave_batch_size(batcher.recommended_batch_size());

let (plumtree, handle) = Plumtree::new(node_id, config, delegate);

// In IHave scheduler loop, use adaptive batch size:
loop {
    let batch_size = batcher.recommended_batch_size();
    // Use batch_size for IHave batching

    // Record latency observations
    if let Some(rtt) = measure_rtt() {
        batcher.record_latency(rtt);
    }
}

// In cleanup loop, use adaptive parameters:
loop {
    let cache_util = calculate_utilization();
    let ttl = config.message_cache_ttl;
    let params = cleanup_tuner.get_parameters(cache_util, ttl);

    tokio::time::sleep(params.interval).await;

    if params.aggressive {
        // More aggressive cleanup
    }

    let start = Instant::now();
    let removed = cleanup_cache(params.batch_size);
    cleanup_tuner.record_cleanup(start.elapsed(), removed, &params);
}
```

## Monitoring

### Batcher Metrics

```rust
let stats = batcher.stats();

// Track in Prometheus/metrics
gauge!("plumtree_adaptive_batch_size").set(stats.current_batch_size as f64);
counter!("plumtree_adaptive_adjustments_total").increment(stats.adjustments);
histogram!("plumtree_graft_success_rate").record(stats.graft_success_rate);
```

### Cleanup Metrics

```rust
let stats = cleanup_tuner.stats();

// Track in Prometheus/metrics
counter!("plumtree_cleanup_cycles_total").increment(stats.cleanup_cycles);
histogram!("plumtree_cleanup_duration_seconds")
    .record(stats.avg_cleanup_duration.as_secs_f64());
gauge!("plumtree_cleanup_efficiency").set(stats.avg_removal_efficiency);
```

## Troubleshooting

### Batch Size Oscillating

**Symptom**: Batch size rapidly changes up and down

**Causes**:
1. Low momentum setting
2. Low hysteresis threshold
3. Unstable network conditions

**Solutions**:
```rust
let config = BatcherConfig::default()
    .with_momentum(0.5)       // Higher = more damping
    .with_hysteresis(0.2);    // Higher = less sensitive
```

### Cleanup Too Aggressive

**Symptom**: High CPU from frequent cleanups

**Causes**:
1. Low base interval
2. Low utilization thresholds
3. Short TTL

**Solutions**:
```rust
let config = CleanupConfig::default()
    .with_base_interval(Duration::from_secs(60))
    .with_high_utilization_threshold(0.85)
    .with_ttl_divisor(2.0);  // Fewer cleanups per TTL
```

### Memory Growing Despite Cleanup

**Symptom**: `plumtree_cache_size` keeps growing

**Causes**:
1. Cleanup interval too long
2. TTL constraint preventing aggressive cleanup
3. Message rate exceeds cleanup capacity

**Solutions**:
1. Use `CleanupConfig::high_throughput()` preset
2. Reduce `message_cache_ttl` in PlumtreeConfig
3. Check backpressure hints and implement message dropping

### Graft Success Rate Low

**Symptom**: `stats.graft_success_rate` below 50%

**Causes**:
1. Network partitions
2. Peer failures
3. Overloaded peers

**Solutions**:
1. Check network connectivity
2. Review `plumtree_graft_failed_total` metric
3. Increase `graft_timeout` and `graft_max_retries`

## See Also

- [Peer Topology](peer-topology.md) - Peer scoring integration
- [Background Tasks](background-tasks.md) - IHave scheduler and cleanup tasks
- [Configuration Guide](configuration.md) - Full configuration reference
- [Metrics Reference](metrics.md) - Monitoring adaptive features
