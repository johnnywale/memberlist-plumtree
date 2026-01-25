# Architecture Overview

This document describes the architecture of memberlist-plumtree, a Rust implementation of the Plumtree (Epidemic Broadcast Trees) protocol integrated with memberlist for cluster membership.

## What is Plumtree?

Plumtree is a gossip protocol that achieves O(n) message complexity while maintaining the reliability of epidemic protocols. It constructs and maintains a broadcast tree embedded in a gossip overlay network.

### Key Concepts

- **Eager Push**: Full messages are sent immediately along spanning tree edges (eager peers)
- **Lazy Push**: Message announcements (IHave) are sent to non-tree peers (lazy peers)
- **Self-Healing**: When a node receives an IHave for a missing message, it grafts the sender into its eager set, automatically repairing the tree

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application                               │
│                   (PlumtreeDelegate)                            │
└────────────────────────────┬────────────────────────────────────┘
                             │ on_deliver()
┌────────────────────────────▼────────────────────────────────────┐
│                     MemberlistStack                              │
│  (Full integration - Plumtree + Memberlist + auto peer sync)    │
├─────────────────────────────────────────────────────────────────┤
│                    PlumtreeDiscovery                           │
│      (Integration layer - message encoding/routing)             │
├─────────────────────────────────────────────────────────────────┤
│                         Plumtree                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │  PeerState  │  │ MessageCache │  │     IHaveScheduler     │  │
│  │ (eager/lazy)│  │   (TTL)      │  │  (lock-free SegQueue)  │  │
│  └─────────────┘  └──────────────┘  └────────────────────────┘  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐  │
│  │ GraftTimer  │  │ RateLimiter  │  │    seen (dedup map)    │  │
│  │  (backoff)  │  │  (per-peer)  │  │                        │  │
│  └─────────────┘  └──────────────┘  └────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │ OutgoingMessage
┌────────────────────────────▼────────────────────────────────────┐
│                    PlumtreeRunner                                │
│        (IHave scheduler + Graft timer + message processor)      │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                    Transport Layer                               │
│            (QUIC / TCP / Channel for testing)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                        Memberlist                                │
│                  (SWIM cluster membership)                       │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Plumtree (`src/plumtree.rs`)

The core protocol implementation that manages:

- **Message broadcasting**: Sends Gossip to eager peers, queues IHave for lazy peers
- **Message handling**: Processes Gossip, IHave, Graft, and Prune messages
- **Deduplication**: Sharded seen map prevents duplicate deliveries
- **Tree optimization**: Prunes redundant paths when duplicates exceed threshold

**Key methods:**
- `broadcast()` - Initiate a new message broadcast
- `handle_message()` - Process incoming protocol messages
- `run_ihave_scheduler()` - Background IHave batching
- `run_graft_timer()` - Background Graft retry logic
- `run_seen_cleanup()` - Background memory cleanup

### 2. PeerState (`src/peer_state.rs`)

Manages the eager/lazy peer classification:

- **Eager set**: Peers that receive full messages immediately
- **Lazy set**: Peers that receive IHave announcements
- **Hash ring topology**: Optional consistent hashing for balanced tree structure
- **Reservoir sampling**: Fair random peer selection

**Key features:**
- Lock-free fast paths with read locks
- Automatic rebalancing when peers join/leave
- Support for scoring-based peer selection

### 3. Message Cache (`src/message/cache.rs`)

TTL-based storage for message payloads:

- Stores messages for Graft request responses
- Automatic expiration based on configurable TTL
- Size-limited with LRU eviction

### 4. IHaveScheduler (`src/scheduler.rs`)

Batches IHave announcements for efficiency:

- Lock-free queue using crossbeam SegQueue
- Configurable batch size and interval
- "Linger" strategy: flushes immediately when batch is full

### 5. GraftTimer (`src/scheduler.rs`)

Handles Graft retry logic with exponential backoff:

- Tracks pending Graft requests with timeouts
- Rotates through alternative peers on failure
- Notifies delegate on max retries exceeded (zombie detection)

### 6. Integration Layer (`src/integration.rs`)

Bridges Plumtree with memberlist:

- **NetworkEnvelope**: Wraps messages with sender ID for routing
- **IdCodec**: Trait for encoding/decoding node IDs
- **PlumtreeNodeDelegate**: Syncs memberlist events to Plumtree topology

### 7. MemberlistStack (`src/bridge.rs`)

High-level API combining all components:

- Simplified cluster join/leave operations
- Automatic peer synchronization via PlumtreeNodeDelegate
- Lazarus task for seed node recovery
- Peer persistence for crash recovery

## Message Flow

### Broadcasting a Message

```
1. Application calls broadcast(payload)
2. Plumtree generates MessageId, caches payload
3. Gossip sent to eager peers via unicast
4. IHave queued for lazy peers
5. IHaveScheduler batches and sends IHave messages
6. Lazy peers that need the message send Graft
7. Graft triggers Gossip response with payload
```

### Receiving a Message

```
1. Network delivers Gossip/IHave/Graft/Prune
2. PlumtreeDiscovery decodes NetworkEnvelope
3. Plumtree.handle_message() dispatches by type:

   Gossip (new):
   - Deliver to application via delegate
   - Cache for potential Graft requests
   - Forward to eager peers (except sender)
   - Queue IHave for lazy peers

   Gossip (duplicate):
   - Increment receive count
   - If count > threshold: send Prune, demote sender

   IHave:
   - If message unknown: promote sender to eager, send Graft
   - Start GraftTimer for retry if no response

   Graft:
   - Promote requester to eager
   - Send cached message if available

   Prune:
   - Demote sender to lazy set
```

## Background Tasks

The protocol requires several background tasks for correct operation:

| Task | Function | Spawned By |
|------|----------|------------|
| `run_ihave_scheduler` | Batches and sends IHave to lazy peers | `run()` or `run_with_transport()` |
| `run_graft_timer` | Retries failed Grafts with backoff | `run()` or `run_with_transport()` |
| `run_seen_cleanup` | Removes expired deduplication entries | `run()` or `run_with_transport()` |
| `run_maintenance_loop` | Rebalances peer topology | `run()` or `run_with_transport()` |
| `run_outgoing_processor` | Routes unicast messages to transport | `run_with_transport()` |
| `run_incoming_processor` | Processes messages from network | `run_incoming_processor()` |

**IMPORTANT**: Always call `MemberlistStack::start()` or `PlumtreeDiscovery::run_with_transport()` to start these tasks. Without them, the protocol will not function correctly.

## Transport Abstraction

The `Transport<I>` trait allows pluggable message delivery:

```rust
pub trait Transport<I>: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn send_to(
        &self,
        target: &I,
        data: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

Built-in implementations:
- **QuicTransport**: Production QUIC transport with connection pooling
- **ChannelTransport**: In-memory channels for testing
- **NoopTransport**: Drops all messages (testing)
- **PooledTransport**: Adds connection pooling to any transport

## Performance Optimizations

### 1. Lock-Free IHave Queue
Uses crossbeam SegQueue for contention-free IHave batching.

### 2. Sharded Seen Map
16-shard deduplication map reduces lock contention. Each message operation only locks one shard.

### 3. Adaptive Batching
`AdaptiveBatcher` auto-tunes IHave batch size based on:
- Network latency
- Graft success rate
- Message throughput

### 4. RTT-Based Peer Scoring
`PeerScoring` tracks peer performance for optimal eager set selection:
- Exponential RTT smoothing
- Failure penalty
- Used during topology rebalancing

### 5. Zero-Copy Message Forwarding
Uses `Bytes::slice()` throughout for efficient payload forwarding.

## Error Handling

Errors are classified for appropriate retry behavior:

- **Transient**: Network issues, timeouts (retryable)
- **Permanent**: Invalid data, protocol violations (not retryable)

```rust
if error.is_transient() {
    // Retry with backoff
} else {
    // Log and skip
}
```

## Health Monitoring

The `health()` method returns a comprehensive health report:

```rust
let health = plumtree.health();
match health.status {
    HealthStatus::Healthy => { /* All systems nominal */ }
    HealthStatus::Degraded => { /* Warning conditions */ }
    HealthStatus::Unhealthy => { /* Critical issues */ }
}
```

Health checks include:
- Peer connectivity (eager/lazy counts)
- Cache utilization
- Pending Graft count
- Shutdown state

## Thread Safety

All core types are `Send + Sync` and designed for concurrent access:

- `Plumtree<I, D>` wraps state in `Arc<PlumtreeInner>`
- Peer state uses `RwLock` with fast-path read locks
- Seen map uses per-shard locks
- Channels use async-channel for backpressure

## See Also

- [Configuration Guide](configuration.md) - Tuning parameters
- [Metrics Reference](metrics.md) - Observability
- [QUIC Transport](quic.md) - QUIC-specific details
- [Getting Started](getting-started.md) - Quick start guide
