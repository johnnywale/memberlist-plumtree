# Getting Started

This guide walks you through setting up memberlist-plumtree for efficient broadcast in a distributed cluster.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
memberlist-plumtree = "0.1"
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tokio` | Yes | Tokio runtime support |
| `metrics` | Yes | Prometheus metrics |
| `serde` | Yes | Serialization support |
| `quic` | Yes | QUIC transport |

To disable default features:

```toml
[dependencies]
memberlist-plumtree = { version = "0.1", default-features = false, features = ["tokio"] }
```

## Quick Start

### 1. Define Your Delegate

The delegate receives delivered messages:

```rust
use memberlist_plumtree::{PlumtreeDelegate, MessageId};
use bytes::Bytes;

struct MyDelegate;

impl PlumtreeDelegate<u64> for MyDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        println!("Received: {:?}", payload);
    }
}
```

**Important**: `on_deliver` should not block. For heavy processing, spawn a background task:

```rust
impl PlumtreeDelegate<u64> for MyDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        tokio::spawn(async move {
            // Heavy processing here
            process_message(payload).await;
        });
    }
}
```

### 2. Create and Configure Plumtree

```rust
use memberlist_plumtree::{Plumtree, PlumtreeConfig};
use std::sync::Arc;

let delegate = Arc::new(MyDelegate);
let config = PlumtreeConfig::default();

let (plumtree, handle) = Plumtree::new(
    1u64,           // Node ID
    config,
    delegate,
);
```

### 3. Start Background Tasks

**Critical**: Background tasks must be started for the protocol to work:

```rust
// Spawn IHave scheduler (sends announcements to lazy peers)
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_ihave_scheduler().await });

// Spawn Graft timer (handles retry logic)
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_graft_timer().await });

// Spawn cleanup task (memory management)
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_seen_cleanup().await });

// Spawn maintenance loop (topology repair)
let pt = plumtree.clone();
tokio::spawn(async move { pt.run_maintenance_loop().await });
```

### 4. Add Peers and Broadcast

```rust
// Add peers (typically from memberlist events)
plumtree.add_peer(2u64);
plumtree.add_peer(3u64);

// Broadcast a message
let msg_id = plumtree.broadcast(b"Hello, cluster!").await?;
```

## Using MemberlistStack (Recommended)

For production use, `MemberlistStack` handles all integration automatically:

```rust
use memberlist_plumtree::{
    MemberlistStack, PlumtreeMemberlist, PlumtreeConfig,
    PlumtreeNodeDelegate, NoopDelegate, ChannelTransport,
};
use memberlist_core::{Memberlist, Options};
use std::sync::Arc;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = 1u64;
    let bind_addr: SocketAddr = "127.0.0.1:7946".parse()?;

    // 1. Create PlumtreeMemberlist with your delegate
    let pm = Arc::new(PlumtreeMemberlist::new(
        node_id,
        PlumtreeConfig::default(),
        MyDelegate,
    ));

    // 2. Create PlumtreeNodeDelegate for auto peer sync
    let delegate = PlumtreeNodeDelegate::new(
        node_id,
        NoopDelegate,  // Or your memberlist delegate
        pm.clone(),
    );

    // 3. Create memberlist with TCP transport
    let transport = /* your memberlist transport */;
    let memberlist = Memberlist::new(transport, delegate, Options::default()).await?;

    // 4. Create the stack
    let stack = MemberlistStack::new(pm, memberlist, bind_addr);

    // 5. CRITICAL: Start background tasks
    let (unicast_tx, _rx) = ChannelTransport::bounded(1024);
    stack.start(unicast_tx);

    // 6. Join the cluster
    let seeds: Vec<SocketAddr> = vec!["192.168.1.100:7946".parse()?];
    stack.join(&seeds).await?;

    // 7. Broadcast messages
    stack.broadcast(b"Hello from MemberlistStack!").await?;

    // 8. Graceful shutdown
    stack.leave(std::time::Duration::from_secs(5)).await?;
    stack.shutdown().await?;

    Ok(())
}
```

## Using QUIC Transport

For better performance, use the QUIC transport:

```rust
use memberlist_plumtree::{
    QuicTransport, QuicConfig, MapPeerResolver,
    PlumtreeMemberlist, PlumtreeConfig, NoopDelegate,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let local_addr = "127.0.0.1:9000".parse()?;

    // Create resolver for peer address lookup
    let resolver = Arc::new(MapPeerResolver::new(local_addr));

    // Add known peer addresses
    resolver.add_peer(2u64, "192.168.1.10:9000".parse()?);
    resolver.add_peer(3u64, "192.168.1.11:9000".parse()?);

    // Create QUIC transport (insecure for development)
    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(local_addr, config, resolver.clone()).await?;

    // Create PlumtreeMemberlist
    let pm = Arc::new(PlumtreeMemberlist::new(
        1u64,
        PlumtreeConfig::default(),
        MyDelegate,
    ));

    // Run with transport (starts all background tasks)
    pm.run_with_transport(transport).await;

    Ok(())
}
```

## Configuration Presets

Choose a preset based on your environment:

```rust
// General purpose (default)
let config = PlumtreeConfig::default();

// LAN deployment (low latency, aggressive optimization)
let config = PlumtreeConfig::lan();

// WAN deployment (high latency tolerance)
let config = PlumtreeConfig::wan();

// Large cluster (1000+ nodes)
let config = PlumtreeConfig::large_cluster();
```

## Implementing IdCodec

If using a custom node ID type, implement `IdCodec`:

```rust
use memberlist_plumtree::IdCodec;
use bytes::{Buf, BufMut};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct MyNodeId(String);

impl IdCodec for MyNodeId {
    fn encode_id(&self, buf: &mut impl BufMut) {
        let bytes = self.0.as_bytes();
        buf.put_u16(bytes.len() as u16);
        buf.put_slice(bytes);
    }

    fn decode_id(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 2 {
            return None;
        }
        let len = buf.get_u16() as usize;
        if buf.remaining() < len {
            return None;
        }
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        String::from_utf8(bytes).ok().map(MyNodeId)
    }

    fn encoded_id_len(&self) -> usize {
        2 + self.0.len()
    }
}
```

Built-in implementations exist for: `u8`, `u16`, `u32`, `u64`, `u128`, and `String`.

## Health Monitoring

Check protocol health:

```rust
let health = plumtree.health();

println!("Status: {:?}", health.status);
println!("Message: {}", health.message);
println!("Peers: eager={}, lazy={}",
    health.peer_health.eager_count,
    health.peer_health.lazy_count);

if health.status.is_unhealthy() {
    eprintln!("WARNING: Protocol health degraded!");
}
```

## Common Patterns

### Waiting for Cluster Formation

```rust
// Wait until we have enough peers
while plumtree.peer_stats().total_peers() < 3 {
    tokio::time::sleep(Duration::from_millis(100)).await;
}
println!("Cluster ready with {} peers", plumtree.peer_stats().total_peers());
```

### Recording Peer RTT

For optimal peer selection, record RTT when communicating:

```rust
let start = Instant::now();
// ... send message and wait for response ...
let rtt = start.elapsed();
plumtree.record_peer_rtt(&peer_id, rtt);
```

### Handling Delegate Events

```rust
impl<I: Debug> PlumtreeDelegate<I> for MyDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        // Process message
    }

    fn on_eager_promotion(&self, peer: &I) {
        println!("Peer {:?} promoted to eager", peer);
    }

    fn on_lazy_demotion(&self, peer: &I) {
        println!("Peer {:?} demoted to lazy", peer);
    }

    fn on_graft_sent(&self, peer: &I, message_id: &MessageId) {
        println!("Sent Graft to {:?} for {:?}", peer, message_id);
    }

    fn on_graft_failed(&self, message_id: &MessageId, peer: &I) {
        eprintln!("Failed to get {:?} from {:?}", message_id, peer);
    }
}
```

## Troubleshooting

### Messages Not Delivered

1. **Background tasks not started**: Ensure `start()` or `run_with_transport()` is called
2. **No eager peers**: Check `peer_stats().eager_count > 0`
3. **Network partition**: Use `health()` to check connectivity

### High Duplicate Rate

1. **Too many eager peers**: Reduce `eager_fanout` in config
2. **Network loops**: Check for bidirectional peer additions

### Memory Growth

1. **Seen map not cleaned**: Ensure `run_seen_cleanup()` is running
2. **Cache not expiring**: Check `message_cache_ttl` setting

## Next Steps

- [Architecture Overview](architecture.md) - Deep dive into internals
- [Configuration Guide](configuration.md) - Tuning parameters
- [Metrics Reference](metrics.md) - Monitoring and observability
- [QUIC Transport](quic.md) - QUIC-specific configuration
