# memberlist-plumtree

[![Crates.io](https://img.shields.io/crates/v/memberlist-plumtree.svg)](https://crates.io/crates/memberlist-plumtree)
[![Documentation](https://docs.rs/memberlist-plumtree/badge.svg)](https://docs.rs/memberlist-plumtree)
[![CI](https://github.com/johnnywale/memberlist-plumtree/actions/workflows/ci.yml/badge.svg)](https://github.com/johnnywale/memberlist-plumtree/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

**Plumtree (Epidemic Broadcast Trees)** implementation for efficient **O(n)** message broadcast in distributed systems, built on SWIM via [memberlist](https://crates.io/crates/memberlist-core).

## Features

- **O(n) Message Complexity** - Spanning tree broadcast eliminates redundant transmissions
- **Self-Healing** - Automatic tree repair via GRAFT/PRUNE mechanism
- **QUIC Transport** - Native QUIC with TLS 1.3 encryption
- **Anti-Entropy Sync** - Automatic message recovery after partitions
- **RTT-Based Topology** - Latency-optimized peer selection
- **Prometheus Metrics** - Comprehensive observability
- **Persistent Storage** - Optional Sled backend for crash recovery

## Quick Start

```rust
use memberlist_plumtree::{
    PlumtreeStack, PlumtreeStackConfig, PlumtreeConfig,
    PlumtreeDelegate, MessageId, QuicConfig,
    discovery::{StaticDiscovery, StaticDiscoveryConfig},
};
use bytes::Bytes;
use std::sync::Arc;

struct MyDelegate;

impl PlumtreeDelegate<u64> for MyDelegate {
    fn on_deliver(&self, msg_id: MessageId, payload: Bytes) {
        println!("Received: {:?}", payload);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Install crypto provider for QUIC
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Configure discovery with seed nodes
    let discovery = StaticDiscovery::new(
        StaticDiscoveryConfig::new()
            .with_seed(1u64, "127.0.0.1:18001".parse()?)
    ).with_local_addr("127.0.0.1:18000".parse()?);

    // Build and start the stack
    let stack = PlumtreeStackConfig::new(0u64, "127.0.0.1:18000".parse()?)
        .with_plumtree(PlumtreeConfig::lan())
        .with_quic(QuicConfig::insecure_dev())
        .with_discovery(discovery)
        .build(Arc::new(MyDelegate))
        .await?;

    // Broadcast messages
    stack.broadcast(b"Hello, cluster!").await?;

    // Graceful shutdown
    stack.shutdown().await;
    Ok(())
}
```

## Installation

```toml
[dependencies]
memberlist-plumtree = "0.1"
```

### Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `tokio` | Yes | Tokio runtime support |
| `quic` | Yes | QUIC transport via quinn |
| `metrics` | Yes | Prometheus metrics |
| `sync` | No | Anti-entropy synchronization |
| `storage-sled` | No | Persistent storage backend |

## Examples

```bash
# Terminal chat with fault injection
cargo run --example chat

# Web UI with peer visualization (http://localhost:3000)
cargo run --example web-chat

# QUIC-based web chat (http://localhost:3001)
cargo run --example web-chat-quic --features "quic,metrics,compression"
```

## Documentation

Detailed documentation is available in the [`docs/`](docs/) directory:

| Document | Description |
|----------|-------------|
| [Getting Started](docs/getting-started.md) | Quick start guide |
| [Architecture](docs/architecture.md) | System design and message flow |
| [Configuration](docs/configuration.md) | Complete config reference |
| [QUIC Transport](docs/quic.md) | QUIC setup and TLS |
| [Sync & Persistence](docs/sync-persistence.md) | Anti-entropy and storage |
| [Metrics](docs/metrics.md) | Prometheus metrics reference |
| [Operations](docs/operations/) | Deployment, monitoring, troubleshooting |

API documentation: [docs.rs/memberlist-plumtree](https://docs.rs/memberlist-plumtree)

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                          SWIM Layer                              │
│              (cluster membership & failure detection)            │
└────────────────────────────┬────────────────────────────────────┘
                             │ Membership events
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Plumtree Layer                            │
│              (Epidemic Broadcast Trees)                          │
│                                                                 │
│  • Eager peers → GOSSIP (full messages)                         │
│  • Lazy peers  → IHAVE (announcements only)                     │
│  • GRAFT       → Request missing message, join eager set        │
│  • PRUNE       → Demote to lazy on duplicate receipt            │
└─────────────────────────────────────────────────────────────────┘
```

## References

- [Epidemic Broadcast Trees](https://www.dpss.inesc-id.pt/~ler/reports/srds07.pdf) - Original Plumtree paper (2007)

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
