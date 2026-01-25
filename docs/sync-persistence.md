# Anti-Entropy Sync and Persistence

This document describes the Anti-Entropy synchronization and message persistence features in memberlist-plumtree. These features enable reliable message delivery even after network partitions, long disconnections, or node restarts.

## Overview

Plumtree's gossip protocol provides reliable message delivery under normal conditions, but certain scenarios can cause message loss:

| Scenario | Problem |
|----------|---------|
| **Network Partition** | Nodes in different partitions miss each other's messages |
| **Long Disconnection** | Node disconnected longer than cache TTL misses messages |
| **Node Crash** | Restarted node has empty state, misses all prior messages |
| **Late Joiner** | New node joining cluster has no historical messages |

**Anti-Entropy Sync** solves these problems through periodic state comparison and selective message exchange.

## How It Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ANTI-ENTROPY SYNC FLOW                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Node A (Initiator)                    Node B (Responder)               │
│  ─────────────────                    ─────────────────                 │
│                                                                         │
│  1. Calculate root_hash               │                                 │
│     for time range                    │                                 │
│           │                           │                                 │
│           ▼                           │                                 │
│  ┌─────────────────┐                  │                                 │
│  │  SyncRequest    │ ──────────────▶  │                                 │
│  │  {root_hash,    │                  ▼                                 │
│  │   time_range}   │         2. Compare with local hash                 │
│  └─────────────────┘                  │                                 │
│                                       │                                 │
│                              ┌────────┴────────┐                        │
│                              │                 │                        │
│                         MATCH              MISMATCH                     │
│                              │                 │                        │
│                              ▼                 ▼                        │
│                     ┌──────────────┐  ┌──────────────┐                  │
│                     │SyncResponse  │  │SyncResponse  │                  │
│  3. Done ◀───────── │{matches:true}│  │{matches:false│ ──────▶ 3.       │
│                     └──────────────┘  │ local_ids}   │    Diff IDs      │
│                                       └──────────────┘         │        │
│                                                                ▼        │
│                                                       ┌──────────────┐  │
│  4. Receive ◀──────────────────────────────────────── │  SyncPull    │  │
│     missing IDs                                       │  {missing}   │  │
│           │                                           └──────────────┘  │
│           ▼                                                             │
│  ┌─────────────────┐                                                    │
│  │  SyncPush       │ ──────────────▶  5. Store & Deliver                │
│  │  {messages}     │                     missing messages               │
│  └─────────────────┘                                                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### XOR-Based Hash Comparison

The sync protocol uses an XOR-based hash for O(1) state comparison:

- Each message contributes `SHA256(message_id || payload)` to the root hash
- XOR is commutative: order doesn't matter
- XOR is self-inverse: `a ^ a = 0`, enabling O(1) removal
- Two nodes with identical message sets have identical root hashes

**Complexity:**
- Insert message: O(1)
- Remove message: O(1)
- Compare state: O(1)
- Collision probability: 1/2^256 (negligible)

## Configuration

### Enabling Sync

```rust
use memberlist_plumtree::{PlumtreeConfig, SyncConfig, StorageConfig};
use std::time::Duration;

let config = PlumtreeConfig::default()
    // Enable anti-entropy sync
    .with_sync(
        SyncConfig::enabled()
            .with_sync_interval(Duration::from_secs(30))  // Sync every 30s
            .with_sync_window(Duration::from_secs(90))    // Check last 90s of messages
            .with_max_batch_size(100)                     // Max IDs per response
    )
    // Enable storage for message persistence
    .with_storage(
        StorageConfig::enabled()
            .with_max_messages(100_000)                   // Max stored messages
            .with_retention(Duration::from_secs(300))     // 5 minute retention
    );
```

### SyncConfig Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enabled` | `false` | Enable anti-entropy sync |
| `sync_interval` | 30s | Time between sync rounds |
| `sync_window` | 90s | How far back to check for missing messages |
| `max_batch_size` | 100 | Maximum message IDs per sync response |

### StorageConfig Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enabled` | `false` | Enable message storage |
| `max_messages` | 100,000 | Maximum messages to store |
| `retention` | 300s | Message retention duration |
| `path` | None | Path for disk persistence (requires `storage-sled` feature) |

## Storage Backends

### In-Memory Storage (Default)

```rust
use memberlist_plumtree::{PlumtreeDiscovery, StorageConfig};

// Uses MemoryStore by default
let pm = PlumtreeDiscovery::new(
    node_id,
    config.with_storage(StorageConfig::enabled()),
    delegate,
);
```

**Characteristics:**
- Fast O(1) lookups
- LRU eviction when at capacity
- Lost on node restart
- Good for ephemeral messages

### Sled Persistent Storage

Requires the `storage-sled` feature:

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", features = ["storage-sled"] }
```

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

**Characteristics:**
- Persistent across restarts
- ACID transactions
- Efficient range queries by timestamp
- Higher latency than memory

### Custom Storage Backend

Implement the `MessageStore` trait for custom backends:

```rust
use memberlist_plumtree::storage::{MessageStore, StoredMessage};
use async_trait::async_trait;

#[async_trait]
impl MessageStore for MyStore {
    async fn insert(&self, msg: &StoredMessage) -> Result<bool, Box<dyn Error + Send + Sync>>;
    async fn get(&self, id: &MessageId) -> Result<Option<StoredMessage>, Box<dyn Error + Send + Sync>>;
    async fn contains(&self, id: &MessageId) -> Result<bool, Box<dyn Error + Send + Sync>>;
    async fn get_range(&self, start: u64, end: u64, limit: usize, offset: usize)
        -> Result<(Vec<MessageId>, bool), Box<dyn Error + Send + Sync>>;
    async fn prune(&self, older_than: u64) -> Result<usize, Box<dyn Error + Send + Sync>>;
    async fn count(&self) -> Result<usize, Box<dyn Error + Send + Sync>>;
}
```

## Background Tasks

When sync is enabled, these background tasks run automatically:

| Task | Purpose | Interval |
|------|---------|----------|
| `run_anti_entropy_sync` | Periodic sync with random peer | `sync_interval` |
| `run_storage_prune` | Remove expired messages | `retention / 2` |

These tasks are started automatically when you call `stack.start(transport)` or `pm.run_with_transport(transport)`.

## Protocol Messages

Four new message types support the sync protocol:

| Message | Direction | Purpose |
|---------|-----------|---------|
| `SyncRequest` | Initiator → Responder | "Here's my state hash for this time range" |
| `SyncResponse` | Responder → Initiator | "Match" or "Here are my message IDs" |
| `SyncPull` | Initiator → Responder | "Send me these specific messages" |
| `SyncPush` | Responder → Initiator | "Here are the requested messages" |

## Best Practices

### Tuning for Your Use Case

**High message rate, short retention:**
```rust
SyncConfig::enabled()
    .with_sync_interval(Duration::from_secs(10))  // Frequent sync
    .with_sync_window(Duration::from_secs(30))    // Short window
    .with_max_batch_size(200)                     // Larger batches

StorageConfig::enabled()
    .with_max_messages(50_000)
    .with_retention(Duration::from_secs(60))
```

**Low message rate, long retention:**
```rust
SyncConfig::enabled()
    .with_sync_interval(Duration::from_secs(60))  // Less frequent
    .with_sync_window(Duration::from_secs(300))   // Longer window
    .with_max_batch_size(50)                      // Smaller batches

StorageConfig::enabled()
    .with_max_messages(500_000)
    .with_retention(Duration::from_secs(3600))    // 1 hour
```

### Memory Considerations

- Each stored message uses ~100 bytes + payload size
- Sync state uses ~40 bytes per message (hash tracking)
- Set `max_messages` based on available memory
- Use Sled for large message volumes

### Network Considerations

- `sync_interval` affects recovery latency vs network overhead
- `max_batch_size` should fit within MTU (< 100 for safety)
- `sync_window` should be >= `retention` to catch all messages

## Monitoring

### Sync-Related Metrics

When the `metrics` feature is enabled:

| Metric | Type | Description |
|--------|------|-------------|
| `plumtree_sync_requests_sent` | Counter | Sync requests initiated |
| `plumtree_sync_responses_received` | Counter | Sync responses received |
| `plumtree_sync_messages_recovered` | Counter | Messages recovered via sync |
| `plumtree_sync_already_synced` | Counter | Sync rounds with matching hashes |
| `plumtree_storage_size` | Gauge | Current stored message count |
| `plumtree_storage_pruned` | Counter | Messages pruned by retention |

### Health Checks

```rust
let health = pm.health();

// Check sync health
if health.sync_health.last_sync_ago > Duration::from_secs(120) {
    warn!("Sync hasn't run in 2 minutes");
}

// Check storage health
if health.storage_health.utilization > 0.9 {
    warn!("Storage at 90% capacity");
}
```

## Troubleshooting

### Messages Not Syncing

1. **Check sync is enabled:**
   ```rust
   assert!(config.sync.as_ref().map(|s| s.enabled).unwrap_or(false));
   ```

2. **Check storage is enabled:**
   ```rust
   assert!(config.storage.as_ref().map(|s| s.enabled).unwrap_or(false));
   ```

3. **Check retention window:**
   Messages older than `retention` are pruned and won't sync.

4. **Check sync window:**
   Messages older than `sync_window` aren't included in sync requests.

### High Memory Usage

1. Reduce `max_messages` in StorageConfig
2. Reduce `retention` duration
3. Use Sled for disk-based storage

### Slow Sync Recovery

1. Increase `max_batch_size` for faster bulk transfer
2. Decrease `sync_interval` for more frequent checks
3. Ensure network latency is acceptable

## Feature Flags

| Feature | Description |
|---------|-------------|
| `sync` | Enable anti-entropy sync (protocol + in-memory storage) |
| `storage` | Enable storage trait and MemoryStore |
| `storage-sled` | Enable Sled persistent storage backend |

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", features = ["sync", "storage-sled"] }
```

## Limitations

1. **Not real-time:** Sync runs periodically, not on every message
2. **Window-based:** Only syncs messages within `sync_window`
3. **XOR hash:** Suitable for trusted P2P mesh; use proper Merkle tree for untrusted networks
4. **Single storage type:** MemberlistStack currently uses default (memory) storage; use PlumtreeDiscovery::with_storage for custom backends

## See Also

- [Architecture Overview](architecture.md) - Overall system design
- [Configuration Guide](configuration.md) - Full configuration reference
- [Background Tasks](background-tasks.md) - Task management and troubleshooting
