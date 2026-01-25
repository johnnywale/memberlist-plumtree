# Background Tasks

The Plumtree protocol requires several background tasks for correct operation. This document explains each task, when to start them, and how they interact.

## Overview

| Task | Function | Required |
|------|----------|----------|
| `run_ihave_scheduler` | Batches and sends IHave to lazy peers | Yes |
| `run_graft_timer` | Retries failed Grafts with backoff | Yes |
| `run_seen_cleanup` | Removes expired deduplication entries | Yes |
| `run_maintenance_loop` | Rebalances peer topology | Recommended |
| `run_outgoing_processor` | Routes unicast messages to transport | When using transport |
| `run_incoming_processor` | Processes messages from network | Yes |

## Starting Tasks

### Using MemberlistStack (Recommended)

The simplest approach - `start()` handles everything:

```rust
let stack = MemberlistStack::new(pm, memberlist, advertise_addr);

// This spawns all required background tasks
stack.start(transport);
```

### Using PlumtreeDiscovery

`run_with_transport()` starts all tasks with automatic unicast handling:

```rust
let pm = PlumtreeDiscovery::new(node_id, config, delegate);

// This spawns all background tasks including unicast sender
pm.run_with_transport(transport).await;
```

Or use `run()` for manual unicast handling:

```rust
// Spawns tasks except unicast sender
pm.run().await;

// You must handle unicast messages manually
let unicast_rx = pm.unicast_receiver();
while let Ok((target, data)) = unicast_rx.recv().await {
    my_transport.send_to(&target, data).await?;
}
```

### Manual Task Spawning

For fine-grained control, spawn tasks individually:

```rust
let (plumtree, handle) = Plumtree::new(node_id, config, delegate);

// IHave scheduler - REQUIRED
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_ihave_scheduler().await });

// Graft timer - REQUIRED
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_graft_timer().await });

// Seen map cleanup - REQUIRED
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_seen_cleanup().await });

// Maintenance loop - RECOMMENDED
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_maintenance_loop().await });

// Incoming processor - REQUIRED
let h = handle.clone();
tokio::spawn(async move {
    while let Some(msg) = h.next_incoming().await {
        plumtree.handle_message(msg.from, msg.message).await.ok();
    }
});
```

## Task Details

### IHave Scheduler

**Function**: Batches IHave announcements and sends them to lazy peers.

**Why it's required**: Without this task, lazy peers never receive IHave messages and can't initiate Grafts for missing messages. This breaks the lazy push mechanism.

**Configuration**:
- `ihave_interval`: How often to flush batches (default: 100ms)
- `ihave_batch_size`: Maximum messages per batch (default: 16)

**Behavior**:
1. Messages are queued when `broadcast()` or `handle_gossip()` is called
2. Batches are flushed when:
   - Batch reaches `ihave_batch_size` (immediate flush)
   - `ihave_interval` timer expires (periodic flush)
3. Each batch is sent to up to `lazy_fanout` random lazy peers

**Metrics**:
- `plumtree_ihave_sent_total` - IHave messages sent
- `plumtree_ihave_batch_size` - Batch size distribution
- `plumtree_ihave_queue_size` - Current queue depth

### Graft Timer

**Function**: Tracks expected messages and retries Grafts when they don't arrive.

**Why it's required**: Without this task, a single failed Graft results in permanent message loss. The timer ensures messages are eventually delivered through alternative paths.

**Configuration**:
- `graft_timeout`: Initial timeout before retry (default: 500ms)
- `graft_max_retries`: Maximum retry attempts (default: 5)

**Behavior**:
1. When receiving an IHave for an unknown message, a timer is started
2. If the message arrives, the timer is cancelled
3. If the timer expires without message arrival:
   - Retry counter is incremented
   - Timeout is doubled (exponential backoff)
   - Alternative peer is selected (if available)
   - New Graft is sent
4. After max retries, `on_graft_failed()` is called on the delegate

**Metrics**:
- `plumtree_graft_sent_total` - Grafts sent
- `plumtree_graft_success_total` - Successful Grafts
- `plumtree_graft_failed_total` - Failed after max retries
- `plumtree_graft_retries_total` - Retry attempts
- `plumtree_pending_grafts` - Current pending count

### Seen Map Cleanup

**Function**: Removes expired entries from the deduplication map.

**Why it's required**: Without cleanup, the seen map grows unbounded, consuming memory proportional to total messages received over time.

**Configuration**:
- `message_cache_ttl`: TTL for entries (default: 60s)
- Cleanup interval is dynamically tuned based on utilization

**Behavior**:
1. Iterates through shards one at a time (16 shards)
2. Removes entries older than `message_cache_ttl`
3. Yields between shards to avoid blocking
4. Adjusts cleanup frequency based on:
   - Cache utilization (more frequent when near capacity)
   - Message rate (less frequent under high load)

**Metrics**:
- `plumtree_seen_map_size` - Current entries
- `plumtree_seen_map_evictions_total` - Emergency evictions

### Maintenance Loop

**Function**: Periodically checks and repairs the peer topology.

**Why it's recommended**: Ensures the eager peer count stays at `eager_fanout` even when peers fail or leave silently.

**Configuration**:
- `maintenance_interval`: Check interval (default: 2s)
- `maintenance_jitter`: Random jitter to prevent thundering herd (default: 500ms)

**Behavior**:
1. Waits for `maintenance_interval + random(0, maintenance_jitter)`
2. Checks if `eager_peers < eager_fanout`
3. If repair needed, promotes lazy peers to eager using scoring
4. Uses non-blocking try_lock to avoid contention
5. Updates peer count metrics

**Metrics**:
- `plumtree_eager_peers` - Current eager count
- `plumtree_lazy_peers` - Current lazy count
- `plumtree_peer_promotions_total` - Promotions performed

**Disabling**: Set `maintenance_interval` to `Duration::ZERO` (not recommended).

### Outgoing Processor

**Function**: Routes outgoing unicast messages to the transport layer.

**Why it's needed**: Separates message generation from network I/O, allowing backpressure handling.

**Behavior**:
1. Receives `OutgoingMessage` from internal channel
2. Encodes message with `NetworkEnvelope` (includes sender ID)
3. Sends to target peer via provided transport

**When to use**: Always use when providing a transport. Not needed if handling unicast manually.

### Incoming Processor

**Function**: Processes messages received from the network.

**Why it's required**: Messages submitted via `submit_incoming()` need to be dispatched to the protocol handler.

**Behavior**:
1. Receives `IncomingMessage` from channel
2. Dispatches to `handle_message()` for protocol processing

## Task Interactions

```
                          ┌─────────────────────┐
                          │   Application       │
                          └─────────┬───────────┘
                                    │ broadcast()
                                    ▼
┌──────────────┐          ┌─────────────────────┐          ┌──────────────┐
│  IHave       │◄─────────│      Plumtree       │─────────►│  Outgoing    │
│  Scheduler   │  queue   │                     │  channel │  Processor   │
└──────┬───────┘          └─────────┬───────────┘          └──────┬───────┘
       │                            │                              │
       │ IHave                      │ Gossip/Graft                │ send_to()
       ▼                            ▼                              ▼
┌──────────────┐          ┌─────────────────────┐          ┌──────────────┐
│   Network    │◄─────────│    Graft Timer      │◄─────────│  Transport   │
│              │  retry   │                     │          │              │
└──────┬───────┘          └─────────────────────┘          └──────────────┘
       │
       │ incoming
       ▼
┌──────────────┐          ┌─────────────────────┐
│  Incoming    │─────────►│    handle_message   │
│  Processor   │ dispatch │                     │
└──────────────┘          └─────────────────────┘
                                    │
                          ┌─────────┴───────────┐
                          ▼                     ▼
                   ┌──────────────┐      ┌──────────────┐
                   │  Seen Map    │      │  Maintenance │
                   │  Cleanup     │      │  Loop        │
                   └──────────────┘      └──────────────┘
```

## Graceful Shutdown

Tasks respond to the shutdown flag:

```rust
// Signal shutdown
plumtree.shutdown();

// Tasks will exit their loops and return
// Wait for spawned tasks to complete if needed
```

Each task checks `is_shutdown()` in its main loop and exits cleanly when true.

## Troubleshooting

### Messages Not Delivered

**Symptom**: `broadcast()` succeeds but peers don't receive messages.

**Causes**:
1. IHave scheduler not running - lazy peers can't request missing messages
2. Graft timer not running - failed Grafts aren't retried
3. Outgoing processor not running - unicasts aren't sent

**Solution**: Ensure all required tasks are spawned.

### Memory Growing Unbounded

**Symptom**: Memory usage increases over time.

**Causes**:
1. Seen map cleanup not running
2. Message cache growing without eviction

**Solution**: Verify cleanup task is running, check `seen_map_size` metric.

### High CPU Usage

**Symptom**: Background tasks consuming excessive CPU.

**Causes**:
1. Very short `ihave_interval` causing frequent flushes
2. Very short `maintenance_interval`
3. High message rate overwhelming cleanup

**Solution**: Increase intervals, use larger batch sizes.

### Topology Not Self-Healing

**Symptom**: `eager_peers` stays below `eager_fanout` after node failures.

**Causes**:
1. Maintenance loop not running
2. No lazy peers to promote
3. Lock contention preventing rebalance

**Solution**: Verify maintenance loop is running, check `lazy_peers` metric.

## See Also

- [Architecture Overview](architecture.md) - Task context in the system
- [Configuration Guide](configuration.md) - Task-related parameters
- [Metrics Reference](metrics.md) - Task monitoring
