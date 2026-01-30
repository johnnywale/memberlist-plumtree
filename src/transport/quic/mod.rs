//! QUIC transport for Plumtree message delivery.
//!
//! This module provides a QUIC-based transport implementation using quinn and rustls.
//! QUIC offers several advantages over TCP:
//!
//! - **Reduced latency**: 0-RTT connection resumption
//! - **Connection migration**: Seamless handling of IP/port changes
//! - **Multiplexing**: Multiple streams per connection without head-of-line blocking
//! - **Built-in encryption**: TLS 1.3 by default
//!
//! # Quick Start
//!
//! ```ignore
//! use memberlist_plumtree::transport::quic::{QuicTransport, QuicConfig, MapPeerResolver};
//! use std::net::SocketAddr;
//!
//! // Create a resolver for peer ID to address mapping
//! let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
//! let resolver = MapPeerResolver::new(local_addr);
//! resolver.add_peer(1u64, "192.168.1.10:9000".parse().unwrap());
//!
//! // Use insecure config for development
//! let config = QuicConfig::insecure_dev();
//!
//! // Create the transport
//! let transport = QuicTransport::new(local_addr, config, resolver).await?;
//!
//! // Send a message
//! transport.send_to(&1u64, Bytes::from("hello")).await?;
//! ```
//!
//! # Configuration Presets
//!
//! - [`QuicConfig::default()`] - General-purpose defaults
//! - [`QuicConfig::lan()`] - LAN-optimized (BBR, shorter timeouts)
//! - [`QuicConfig::wan()`] - WAN-optimized (Cubic, longer timeouts)
//! - [`QuicConfig::large_cluster()`] - Large clusters (high connection limits)
//! - [`QuicConfig::insecure_dev()`] - Development only (self-signed certs)
//!
//! # 0-RTT Early Data
//!
//! 0-RTT allows sending data during the TLS handshake for reduced latency.
//!
//! ## ⚠️ Security Warning - Replay Attacks
//!
//! **0-RTT data is replayable!** When enabled, ALL messages (including Graft/Prune
//! control messages) may be sent as early data. An attacker can capture and replay
//! these packets, potentially affecting the spanning tree topology.
//!
//! **By default, 0-RTT is disabled** (safe default).
//!
//! Only enable 0-RTT if:
//! - Your network is trusted (no active attackers)
//! - The latency reduction is worth the replay risk
//! - You understand and accept the security implications
//!
//! ```ignore
//! // ⚠️ Only enable if you understand the replay risks!
//! let config = QuicConfig::default()
//!     .with_zero_rtt(ZeroRttConfig::default()
//!         .with_enabled(true)
//!         .with_session_cache_capacity(1024));
//! ```
//!
//! The transport tracks 0-RTT usage for statistics (`QuicStats::zero_rtt_sent`).
//!
//! # Stream vs Datagram Mode
//!
//! The current implementation uses **unidirectional streams** for each message.
//! This is simple and reliable but has some overhead for high-throughput scenarios.
//!
//! For gossip protocols like Plumtree, **QUIC datagrams** (`connection.send_datagram()`)
//! may be more efficient because:
//!
//! - **Lower overhead**: No stream ID negotiation or flow control per message
//! - **Better fit**: Gossip messages are typically small, unordered, and fire-and-forget
//! - **Simpler receiver**: No need to accept and manage streams
//!
//! However, datagrams have limitations:
//!
//! - **Size limit**: Typically ~1200 bytes (limited by QUIC packet size)
//! - **No ordering**: Messages may arrive out of order (acceptable for gossip)
//! - **No reliability**: Lost datagrams are not retransmitted (Plumtree handles this)
//!
//! The current stream-based approach is chosen for:
//!
//! - **Simplicity**: Works out of the box without size constraints
//! - **Reliability**: QUIC streams handle retransmission automatically
//! - **Large messages**: Supports messages larger than MTU
//!
//! Future versions may offer datagram mode as an option for small messages.
//!
//! # TLS Configuration
//!
//! For production use, configure TLS with proper certificates:
//!
//! ```ignore
//! let config = QuicConfig::default()
//!     .with_tls(TlsConfig::new()
//!         .with_cert_path("/path/to/server.crt")
//!         .with_key_path("/path/to/server.key")
//!         .with_ca_path("/path/to/ca.crt")
//!         .with_mtls(true));
//! ```
//!
//! # Peer Resolution
//!
//! The transport needs a way to map peer IDs to socket addresses. Implement
//! the [`PeerResolver`] trait or use the built-in [`MapPeerResolver`]:
//!
//! ```ignore
//! // Simple map-based resolver
//! let resolver = MapPeerResolver::new(local_addr);
//! resolver.add_peer(peer_id, peer_addr);
//!
//! // Or implement PeerResolver for custom resolution (DNS, service discovery, etc.)
//! impl PeerResolver<MyPeerId> for MyResolver {
//!     fn resolve(&self, peer: &MyPeerId) -> Option<SocketAddr> {
//!         // Custom resolution logic
//!     }
//!     fn local_addr(&self) -> SocketAddr {
//!         self.local_addr
//!     }
//! }
//! ```

mod config;
mod connection;
mod error;
mod resolver;
mod session_cache;
mod tls;
mod transport;

// Re-export main types
pub use config::{
    CongestionConfig, CongestionController, ConnectionConfig, DatagramConfig, MessagePriorities,
    MigrationConfig, PlumtreeQuicConfig, QuicConfig, StreamConfig, TlsConfig, ZeroRttConfig,
};
pub use connection::ConnectionStats;
pub use error::QuicError;
pub use resolver::{MapPeerResolver, PeerResolver};
#[allow(unused_imports)]
pub use transport::{
    CleanupTaskHandle, ConnectionEvent, DisconnectReason, IncomingConfig, IncomingHandle,
    IncomingStats, QuicStats, QuicTransport,
};

// Re-export TLS utilities (used by consumers who want lower-level control)
#[allow(unused_imports)]
pub use tls::{client_config, generate_self_signed, server_config, DEFAULT_ALPN};

// Async TLS utilities (for use in async contexts to avoid blocking)
#[allow(unused_imports)]
pub use tls::{
    client_config_with_0rtt, generate_self_signed_async, server_config_async,
    server_config_async_with_0rtt,
};

// mTLS peer verification utilities
#[allow(unused_imports)]
pub use tls::{
    extract_peer_id_from_der, generate_self_signed_with_peer_id,
    generate_self_signed_with_peer_id_async, PeerIdVerifier, PEER_ID_SAN_PREFIX,
};

// Session caching for 0-RTT (reference implementation, not currently integrated)
// Note: The transport uses rustls's built-in session cache. These types are
// provided for custom implementations or future persistent storage integration.
#[allow(unused_imports)]
pub use session_cache::{LruSessionCache, NoopSessionCache, SessionTicketStore};
