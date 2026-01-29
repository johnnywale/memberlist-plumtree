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

## Sync Strategies

The sync system uses a **pluggable strategy pattern** via the `SyncStrategy` trait. This allows you to choose the best sync mechanism for your deployment:

| Strategy | Best For | Background Task | Sync Mechanism |
|----------|----------|-----------------|----------------|
| `PlumtreeSyncStrategy` | Static discovery, custom discovery providers | Yes (runs own timer) | 4-message protocol |
| `MemberlistSyncStrategy` | Memberlist-based discovery | No (uses memberlist hooks) | Push-pull piggyback |
| `NoOpSyncStrategy` | Testing, minimal deployments | No | Disabled |

### PlumtreeSyncStrategy (Default)

The default strategy runs its own periodic sync with random peers using the 4-message protocol (Request → Response → Pull → Push).

```rust
use memberlist_plumtree::{PlumtreeDiscovery, PlumtreeConfig, NoopDelegate};

// PlumtreeSyncStrategy is used by default
let pm = PlumtreeDiscovery::new(node_id, PlumtreeConfig::default(), NoopDelegate);

// Sync runs automatically in run_with_transport()
pm.run_with_transport(transport).await;
```

**When to use:**
- Using static peer discovery (seed nodes)
- Using custom discovery providers (Consul, etcd, DNS)
- Need explicit control over sync timing
- Running without memberlist

### MemberlistSyncStrategy

Piggybacks on memberlist's existing push-pull mechanism, avoiding duplicate timers and connections.

```rust
use memberlist_plumtree::{PlumtreeDiscovery, PlumtreeConfig, NoopDelegate};
use memberlist_plumtree::sync::{MemberlistSyncStrategy, SyncHandler};
use std::time::Duration;

// Create shared sync handler
let store = Arc::new(MemoryStore::new(100_000));
let sync_handler = Arc::new(SyncHandler::new(store.clone()));

// Create channel for sync requests triggered by hash mismatch
let (sync_tx, sync_rx) = async_channel::bounded(64);

// Create memberlist strategy
let strategy = MemberlistSyncStrategy::new(
    sync_handler.clone(),
    Duration::from_secs(90),  // sync_window
    sync_tx,
);

// Create PlumtreeDiscovery with custom strategy
let pm = PlumtreeDiscovery::with_sync_strategy(
    node_id,
    config,
    delegate,
    store,
    sync_handler,  // Must be same instance!
    strategy,
);

// In your PlumtreeNodeDelegate implementation:
// fn local_state(&self) -> Bytes {
//     futures::executor::block_on(pm.sync_local_state())
// }
// fn merge_remote_state(&self, buf: &[u8]) {
//     futures::executor::block_on(pm.sync_merge_remote_state(buf, || get_peer_id()))
// }
```

**When to use:**
- Using memberlist for cluster membership
- Want to reduce network overhead (no duplicate sync traffic)
- Memberlist's 30s push-pull interval is acceptable

**How it works:**
1. During memberlist push-pull, `local_state()` returns a 48-byte header:
   - 32 bytes: XOR root hash
   - 8 bytes: timestamp
   - 8 bytes: sync window (ms)
2. `merge_remote_state()` compares hashes
3. On mismatch, queues a sync request to `sync_tx`
4. Application processes sync requests via the channel

### NoOpSyncStrategy

Disables sync entirely. Useful for testing or when sync isn't needed.

```rust
use memberlist_plumtree::sync::NoOpSyncStrategy;

let strategy = NoOpSyncStrategy::new();

let pm = PlumtreeDiscovery::with_sync_strategy(
    node_id, config, delegate, store,
    sync_handler,
    strategy,
);
```

**When to use:**
- Unit testing without sync overhead
- Ephemeral messages that don't need recovery
- Minimal resource deployments

### Strategy Comparison

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      SYNC STRATEGY COMPARISON                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PlumtreeSyncStrategy               MemberlistSyncStrategy                  │
│  ─────────────────────              ────────────────────────                │
│                                                                             │
│  ┌─────────────┐                    ┌─────────────┐                         │
│  │  Plumtree   │                    │  Plumtree   │                         │
│  │   Timer     │ ─── tick ───▶      │             │                         │
│  └─────────────┘                    └─────────────┘                         │
│        │                                   │                                │
│        ▼                                   │                                │
│  ┌─────────────┐                    ┌──────┴──────┐                         │
│  │   Random    │                    │ Memberlist  │ ◀── push-pull (30s)     │
│  │    Peer     │                    │  Delegate   │                         │
│  └─────────────┘                    └──────┬──────┘                         │
│        │                                   │                                │
│        ▼                                   ▼                                │
│  ┌─────────────┐                    ┌─────────────┐                         │
│  │ SyncRequest │                    │ local_state │ ─── 48-byte hash ───▶   │
│  │ SyncResponse│                    │merge_remote │ ◀── compare hashes      │
│  │ SyncPull    │                    └─────────────┘                         │
│  │ SyncPush    │                           │                                │
│  └─────────────┘                    on mismatch                             │
│                                            │                                │
│  Pros:                                     ▼                                │
│  • Full control                     ┌─────────────┐                         │
│  • Works without memberlist         │  sync_tx    │ ─── SyncRequest ───▶    │
│  • Configurable interval            │  channel    │                         │
│                                     └─────────────┘                         │
│  Cons:                                                                      │
│  • Extra timer/connections          Pros:                                   │
│  • More network traffic             • No extra timers                       │
│                                     • Reuses memberlist connections         │
│                                     • Less network overhead                 │
│                                                                             │
│                                     Cons:                                   │
│                                     • Requires memberlist                   │
│                                     • Fixed 30s interval                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

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

| Task | Purpose | Interval | Strategy |
|------|---------|----------|----------|
| `run_anti_entropy_sync` | Periodic sync with random peer | `sync_interval` | `PlumtreeSyncStrategy` only |
| `run_storage_prune` | Remove expired messages | `retention / 10` | All strategies |

**Note:** The `run_anti_entropy_sync` task behavior depends on the sync strategy:

- **PlumtreeSyncStrategy**: Runs the full 4-message sync protocol periodically
- **MemberlistSyncStrategy**: No-op (sync happens via memberlist's push-pull hooks)
- **NoOpSyncStrategy**: No-op (sync disabled)

These tasks are started automatically when you call `stack.start(transport)` or `pm.run_with_transport(transport)`.

You can check if your strategy needs a background task:

```rust
if pm.sync_needs_background_task() {
    // PlumtreeSyncStrategy is in use
} else {
    // MemberlistSyncStrategy or NoOpSyncStrategy
}
```

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
| `sync` | Enable anti-entropy sync (protocol + in-memory storage + all strategies) |
| `storage` | Enable storage trait and MemoryStore |
| `storage-sled` | Enable Sled persistent storage backend |

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", features = ["sync", "storage-sled"] }
```

All sync strategies (`PlumtreeSyncStrategy`, `MemberlistSyncStrategy`, `NoOpSyncStrategy`) are available when the `sync` feature is enabled.

## Limitations

1. **Not real-time:** Sync runs periodically, not on every message
2. **Window-based:** Only syncs messages within `sync_window`
3. **XOR hash:** Suitable for trusted P2P mesh; use proper Merkle tree for untrusted networks
4. **Single storage type:** MemberlistStack currently uses default (memory) storage; use PlumtreeDiscovery::with_storage for custom backends

## Implementing Custom Sync Strategies

You can implement custom sync strategies by implementing the `SyncStrategy` trait:

```rust
use memberlist_plumtree::sync::{SyncStrategy, SyncResult, SyncError};
use memberlist_plumtree::message::{MessageId, SyncMessage};
use memberlist_plumtree::storage::MessageStore;
use memberlist_plumtree::Transport;
use bytes::Bytes;

pub struct MyCustomStrategy<I, S> {
    // ... your fields
}

impl<I, S> SyncStrategy<I, S> for MyCustomStrategy<I, S>
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    S: MessageStore + 'static,
{
    /// Does this strategy need its own background task?
    fn needs_background_task(&self) -> bool {
        true  // or false if using external mechanism
    }

    /// Get local state for push-pull exchange (48 bytes: hash + timestamp + window)
    fn local_state(&self) -> impl Future<Output = Bytes> + Send {
        async { Bytes::new() }
    }

    /// Compare and potentially trigger sync on remote state
    fn merge_remote_state<F>(&self, buf: &[u8], peer_resolver: F) -> impl Future<Output = ()> + Send
    where
        F: FnOnce() -> Option<I> + Send,
    {
        async {}
    }

    /// Run background sync (called if needs_background_task() returns true)
    fn run_background_sync<T>(&self, transport: T) -> impl Future<Output = ()> + Send
    where
        T: Transport<I>,
    {
        async {}
    }

    /// Handle incoming sync messages
    fn handle_sync_message(
        &self,
        from: I,
        message: SyncMessage,
    ) -> impl Future<Output = SyncResult<Option<SyncMessage>>> + Send {
        async { Ok(None) }
    }

    /// Is sync enabled?
    fn is_enabled(&self) -> bool {
        true
    }

    /// Get current root hash
    fn root_hash(&self) -> [u8; 32] {
        [0u8; 32]
    }

    /// Record a message in sync state
    fn record_message(&self, id: MessageId, payload: &[u8]) {}

    /// Remove a message from sync state
    fn remove_message(&self, id: &MessageId) {}
}
```

## See Also

- [Architecture Overview](architecture.md) - Overall system design
- [Configuration Guide](configuration.md) - Full configuration reference
- [Background Tasks](background-tasks.md) - Task management and troubleshooting
