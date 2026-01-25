# QUIC Transport

memberlist-plumtree includes a native QUIC transport implementation using quinn and rustls. QUIC offers significant advantages over TCP for gossip protocols.

## Why QUIC?

| Feature | Benefit for Plumtree |
|---------|---------------------|
| **Reduced Latency** | 0-RTT connection resumption (future) |
| **Connection Migration** | Handle IP changes without reconnection |
| **Multiplexing** | Multiple streams without head-of-line blocking |
| **Built-in Encryption** | TLS 1.3 by default |
| **Better Congestion Control** | BBR and Cubic support |

## Quick Start

### Development (Insecure)

```rust
use memberlist_plumtree::{
    QuicTransport, QuicConfig, MapPeerResolver,
    PlumtreeDiscovery, PlumtreeConfig, NoopDelegate,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let local_addr = "127.0.0.1:9000".parse()?;

    // Create peer resolver
    let resolver = Arc::new(MapPeerResolver::new(local_addr));
    resolver.add_peer(2u64, "127.0.0.1:9001".parse()?);
    resolver.add_peer(3u64, "127.0.0.1:9002".parse()?);

    // Create QUIC transport (insecure for development)
    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(local_addr, config, resolver.clone()).await?;

    // Use with Plumtree
    let pm = Arc::new(PlumtreeDiscovery::new(1u64, PlumtreeConfig::default(), NoopDelegate));
    pm.run_with_transport(transport).await;

    Ok(())
}
```

### Production (with TLS)

```rust
use memberlist_plumtree::{QuicConfig, TlsConfig};

let config = QuicConfig::wan()
    .with_tls(TlsConfig::new()
        .with_cert_path("/path/to/server.crt")
        .with_key_path("/path/to/server.key")
        .with_ca_path("/path/to/ca.crt")
        .with_mtls(true));

let transport = QuicTransport::new(bind_addr, config, resolver).await?;
```

## Configuration Presets

### Default

```rust
let config = QuicConfig::default();
```

General-purpose defaults suitable for most deployments.

### LAN

```rust
let config = QuicConfig::lan();
```

Optimized for low-latency local networks:
- Shorter timeouts (15s idle, 5s handshake)
- BBR congestion control
- Lower initial RTT estimate (10ms)
- Connection migration disabled

### WAN

```rust
let config = QuicConfig::wan();
```

Optimized for high-latency wide-area networks:
- Longer timeouts (60s idle, 30s handshake)
- Cubic congestion control
- Higher initial RTT estimate (200ms)
- Connection migration enabled

### Large Cluster

```rust
let config = QuicConfig::large_cluster();
```

Optimized for 1000+ node clusters:
- High connection limit (4096)
- Large session cache (4096)
- More concurrent streams (200)

### Insecure Development

```rust
let config = QuicConfig::insecure_dev();
```

**WARNING**: Only for local development. Uses self-signed certificates and skips verification.

## Configuration Components

### TLS Configuration

```rust
use memberlist_plumtree::TlsConfig;

let tls = TlsConfig::new()
    // Certificate paths
    .with_cert_path("/path/to/server.crt")
    .with_key_path("/path/to/server.key")
    .with_ca_path("/path/to/ca.crt")

    // Enable mutual TLS (client certificates)
    .with_mtls(true)

    // Custom ALPN protocols (default: ["plumtree/1"])
    .with_alpn_protocols(vec![b"myapp/1".to_vec()])

    // Server name for SNI (default: "localhost")
    .with_server_name("cluster.example.com");

let config = QuicConfig::default().with_tls(tls);
```

### Connection Configuration

```rust
use memberlist_plumtree::ConnectionConfig;
use std::time::Duration;

let connection = ConnectionConfig::new()
    // Idle timeout before closing
    .with_idle_timeout(Duration::from_secs(30))

    // Keep-alive ping interval
    .with_keep_alive_interval(Duration::from_secs(10))

    // Maximum concurrent connections
    .with_max_connections(512)

    // Handshake timeout
    .with_handshake_timeout(Duration::from_secs(10))

    // Retry configuration
    .with_retries(3)
    .with_retry_delay(Duration::from_secs(1));

let config = QuicConfig::default().with_connection(connection);
```

### Stream Configuration

```rust
use memberlist_plumtree::StreamConfig;

let streams = StreamConfig::new()
    // Maximum concurrent streams
    .with_max_uni_streams(100)
    .with_max_bi_streams(100)

    // Buffer sizes
    .with_recv_buffer(64 * 1024)  // 64KB
    .with_send_buffer(64 * 1024)  // 64KB

    // Maximum data per stream
    .with_max_data(1024 * 1024);  // 1MB

let config = QuicConfig::default().with_streams(streams);
```

### Congestion Control

```rust
use memberlist_plumtree::{CongestionConfig, CongestionController};

let congestion = CongestionConfig::new()
    // Controller algorithm (BBR or Cubic)
    .with_controller(CongestionController::Bbr)

    // Initial RTT estimate
    .with_initial_rtt(Duration::from_millis(100));

let config = QuicConfig::default().with_congestion(congestion);
```

**Congestion Controllers:**

| Controller | Best For |
|-----------|----------|
| `Bbr` | LAN, low-latency networks |
| `Cubic` | WAN, high-latency networks |

### Connection Migration

```rust
use memberlist_plumtree::MigrationConfig;

let migration = MigrationConfig {
    enabled: true,        // Allow IP/port changes
    validate_path: true,  // Validate new paths
};

let config = QuicConfig::default().with_migration(migration);
```

Connection migration allows peers to change their IP address or port without disconnecting. Useful for:
- Mobile devices
- DHCP lease renewals
- Load balancer changes

### Plumtree-Specific Settings

```rust
use memberlist_plumtree::{PlumtreeQuicConfig, MessagePriorities};

let plumtree = PlumtreeQuicConfig::new()
    // Enable priority-based scheduling
    .with_priority_enabled(true)

    // Custom priorities per message type
    .with_priorities(MessagePriorities {
        gossip: 2,   // High (payload delivery)
        ihave: 1,    // Normal (announcements)
        graft: 3,    // Critical (tree repair)
        prune: 0,    // Low (optimization)
    });

let config = QuicConfig::default().with_plumtree(plumtree);
```

## Peer Resolution

QUIC transport requires a peer resolver to map peer IDs to socket addresses:

```rust
use memberlist_plumtree::MapPeerResolver;

// Create resolver with local address
let resolver = Arc::new(MapPeerResolver::new("127.0.0.1:9000".parse()?));

// Add peers
resolver.add_peer(2u64, "192.168.1.10:9000".parse()?);
resolver.add_peer(3u64, "192.168.1.11:9000".parse()?);

// Remove peers
resolver.remove_peer(&2u64);

// Check peer count
let count = resolver.len();
```

### Custom Resolver

Implement `PeerResolver` for custom address resolution:

```rust
use memberlist_plumtree::PeerResolver;
use std::net::SocketAddr;

struct DnsResolver {
    domain: String,
}

impl PeerResolver<u64> for DnsResolver {
    fn resolve(&self, peer: &u64) -> Option<SocketAddr> {
        // Look up peer-{id}.{domain}
        let hostname = format!("peer-{}.{}", peer, self.domain);
        // DNS resolution logic...
        None
    }
}
```

## Connection Statistics

Monitor QUIC transport health:

```rust
let stats = transport.stats();

println!("Active connections: {}", stats.active_connections);
println!("Total connections opened: {}", stats.connections_opened);
println!("Total connections closed: {}", stats.connections_closed);
println!("Bytes sent: {}", stats.bytes_sent);
println!("Bytes received: {}", stats.bytes_received);
```

## Configuration Defaults

| Parameter | Default | LAN | WAN | Large Cluster |
|-----------|---------|-----|-----|---------------|
| `idle_timeout` | 30s | 15s | 60s | 30s |
| `keep_alive_interval` | 10s | 5s | 20s | 10s |
| `handshake_timeout` | 10s | 5s | 30s | 10s |
| `max_connections` | 256 | 256 | 256 | 4096 |
| `max_uni_streams` | 100 | 100 | 100 | 200 |
| `max_bi_streams` | 100 | 100 | 100 | 200 |
| `initial_rtt` | 100ms | 10ms | 200ms | 100ms |
| `congestion_controller` | Cubic | BBR | Cubic | Cubic |
| `migration_enabled` | false | false | true | false |

## TLS Certificate Setup

### Self-Signed (Development)

Use `insecure_dev()` preset, or generate with:

```bash
# Generate CA
openssl genrsa -out ca.key 4096
openssl req -new -x509 -days 365 -key ca.key -out ca.crt -subj "/CN=Plumtree CA"

# Generate server cert
openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"
openssl x509 -req -days 365 -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt
```

### Production (Let's Encrypt or Corporate CA)

```rust
let tls = TlsConfig::new()
    .with_cert_path("/etc/plumtree/server.crt")
    .with_key_path("/etc/plumtree/server.key")
    .with_ca_path("/etc/plumtree/ca.crt")
    .with_mtls(true)  // Require client certs
    .with_server_name("cluster.example.com");
```

## 0-RTT Early Data (Future)

**Note**: 0-RTT is not yet implemented. The configuration API is provided for future use.

When implemented, 0-RTT will allow sending data during the TLS handshake for reduced latency:

```rust
use memberlist_plumtree::ZeroRttConfig;

let zero_rtt = ZeroRttConfig::new()
    .with_enabled(true)           // Enable 0-RTT
    .with_gossip_only(true)       // Only use for idempotent Gossip
    .with_session_cache_capacity(1000);

let config = QuicConfig::default().with_zero_rtt(zero_rtt);
```

**Security Warning**: 0-RTT data is replayable. Only idempotent operations (like Gossip) should use it.

## Error Handling

QUIC transport errors include:

```rust
use memberlist_plumtree::QuicError;

match result {
    Err(QuicError::ConnectionFailed(msg)) => {
        // Handshake or connection establishment failed
    }
    Err(QuicError::SendFailed(msg)) => {
        // Failed to send data
    }
    Err(QuicError::PeerNotFound(id)) => {
        // Resolver doesn't know this peer
    }
    Err(QuicError::TlsError(msg)) => {
        // TLS configuration or handshake error
    }
    _ => {}
}
```

## Troubleshooting

### Connection Timeout

- Increase `handshake_timeout` for high-latency networks
- Check firewall allows UDP traffic on QUIC port
- Verify resolver has correct peer addresses

### High Latency

- Use `QuicConfig::lan()` for local networks
- Enable BBR congestion control
- Lower `initial_rtt` estimate if network is faster

### Connection Churn

- Increase `idle_timeout` to keep connections alive longer
- Decrease `keep_alive_interval` to prevent idle disconnects
- Enable connection migration for mobile/dynamic IPs

### Memory Usage

- Reduce `max_connections` to limit connection pool
- Reduce `session_cache_capacity` if 0-RTT is disabled
- Lower stream buffer sizes

## See Also

- [Configuration Guide](configuration.md) - General configuration
- [Architecture Overview](architecture.md) - Transport layer context
- [Metrics Reference](metrics.md) - QUIC-related metrics
