# memberlist-plumtree Documentation

Welcome to the memberlist-plumtree documentation. This library provides a Rust implementation of the Plumtree (Epidemic Broadcast Trees) protocol integrated with memberlist for cluster membership.

## Documentation Index

### Getting Started

- **[Getting Started](getting-started.md)** - Quick start guide with examples for common use cases

### Core Concepts

- **[Architecture Overview](architecture.md)** - Deep dive into the protocol implementation, message flow, and component interactions
- **[Peer Topology](peer-topology.md)** - Eager/lazy peer management, hash ring topology, lazy-to-eager promotion, and peer scoring

### Performance

- **[Adaptive Features](adaptive-features.md)** - Dynamic IHave batch sizing and cache cleanup tuning based on runtime conditions

### Configuration

- **[Configuration Guide](configuration.md)** - Complete reference for `PlumtreeConfig`, `BridgeConfig`, and tuning recommendations for different environments

### Transport

- **[QUIC Transport](quic.md)** - Native QUIC transport configuration with TLS, connection management, and troubleshooting

### Operations

- **[Background Tasks](background-tasks.md)** - Required background tasks, how to start them, and troubleshooting
- **[Metrics Reference](metrics.md)** - Prometheus-compatible metrics for monitoring and alerting

## Quick Links

### API Entry Points

| API | Use Case |
|-----|----------|
| `MemberlistStack` | Production deployment with full SWIM-based peer discovery |
| `PlumtreeMemberlist` | Manual peer management, custom integration |
| `Plumtree` | Core protocol only, bring your own transport |

### Configuration Presets

```rust
PlumtreeConfig::default()      // General purpose
PlumtreeConfig::lan()          // Low latency LAN
PlumtreeConfig::wan()          // High latency WAN
PlumtreeConfig::large_cluster() // 1000+ nodes
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tokio` | Yes | Tokio runtime support |
| `metrics` | Yes | Prometheus metrics |
| `serde` | Yes | Serialization support |
| `quic` | Yes | QUIC transport |

## Example

```rust
use memberlist_plumtree::{
    MemberlistStack, PlumtreeMemberlist, PlumtreeConfig,
    PlumtreeNodeDelegate, NoopDelegate, ChannelTransport,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create PlumtreeMemberlist
    let pm = Arc::new(PlumtreeMemberlist::new(
        1u64,
        PlumtreeConfig::default(),
        MyDelegate,
    ));

    // Create delegate for auto peer sync
    let delegate = PlumtreeNodeDelegate::new(1u64, NoopDelegate, pm.clone());

    // Create memberlist and stack
    let memberlist = /* create memberlist with transport */;
    let stack = MemberlistStack::new(pm, memberlist, bind_addr);

    // CRITICAL: Start background tasks
    let (transport, _rx) = ChannelTransport::bounded(1024);
    stack.start(transport);

    // Join cluster and broadcast
    stack.join(&seeds).await?;
    stack.broadcast(b"Hello!").await?;

    Ok(())
}
```

## Protocol Messages

| Message | Purpose |
|---------|---------|
| **Gossip** | Full message payload, sent to eager peers |
| **IHave** | Message ID announcement, sent to lazy peers |
| **Graft** | Request to establish eager link and get missing message |
| **Prune** | Request to demote sender from eager to lazy |

## Message Flow

```
Broadcast:
  Node → Gossip → Eager Peers → Gossip → Their Eager Peers...
       → IHave → Lazy Peers

Tree Repair:
  Lazy Peer receives IHave for unknown message
       → Promote sender to Eager
       → Send Graft
       → Receive Gossip with payload

Optimization:
  Node receives duplicate Gossip (count > threshold)
       → Send Prune to duplicate sender
       → Demote sender to Lazy
```

## Support

- **Issues**: [GitHub Issues](https://github.com/johnnywale/memberlist-plumtree/issues)
- **Source**: [GitHub Repository](https://github.com/johnnywale/memberlist-plumtree)

## License

MIT OR Apache-2.0
