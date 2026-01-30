//! QUIC transport configuration with modular sub-configs.
//!
//! Configuration is organized into logical sub-structs:
//! - [`TlsConfig`]: TLS/certificate settings
//! - [`ConnectionConfig`]: Connection lifecycle settings
//! - [`StreamConfig`]: Stream multiplexing settings
//! - [`ZeroRttConfig`]: 0-RTT early data settings
//! - [`CongestionConfig`]: Congestion control settings
//! - [`MigrationConfig`]: Connection migration settings
//! - [`PlumtreeQuicConfig`]: Plumtree-specific optimizations
//!
//! # Presets
//!
//! Use presets for common deployment scenarios:
//!
//! ```ignore
//! use memberlist_plumtree::transport::quic::QuicConfig;
//!
//! // LAN deployment - low latency, BBR congestion control
//! let config = QuicConfig::lan();
//!
//! // WAN deployment - longer timeouts, Cubic congestion control
//! let config = QuicConfig::wan();
//!
//! // Large cluster - high connection limits
//! let config = QuicConfig::large_cluster();
//!
//! // Development only - self-signed certs, no verification
//! let config = QuicConfig::insecure_dev();
//! ```

use std::path::PathBuf;
use std::time::Duration;

/// QUIC transport configuration with modular sub-configs.
#[derive(Debug, Clone, Default)]
pub struct QuicConfig {
    /// TLS/certificate configuration.
    pub tls: TlsConfig,
    /// Connection lifecycle configuration.
    pub connection: ConnectionConfig,
    /// Stream multiplexing configuration.
    pub streams: StreamConfig,
    /// 0-RTT early data configuration.
    pub zero_rtt: ZeroRttConfig,
    /// Congestion control configuration.
    pub congestion: CongestionConfig,
    /// Connection migration configuration.
    pub migration: MigrationConfig,
    /// Datagram configuration.
    pub datagram: DatagramConfig,
    /// Plumtree-specific QUIC optimizations.
    pub plumtree: PlumtreeQuicConfig,
}

impl QuicConfig {
    /// Create a new configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// LAN-optimized configuration.
    ///
    /// - Shorter timeouts for fast failure detection
    /// - BBR congestion control for better LAN performance
    /// - Lower initial RTT estimate
    pub fn lan() -> Self {
        Self {
            connection: ConnectionConfig {
                idle_timeout: Duration::from_secs(15),
                keep_alive_interval: Duration::from_secs(5),
                handshake_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            congestion: CongestionConfig {
                controller: CongestionController::Bbr,
                initial_rtt: Duration::from_millis(10),
                ..Default::default()
            },
            migration: MigrationConfig {
                enabled: false, // Less relevant for LAN
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// WAN-optimized configuration.
    ///
    /// - Longer timeouts for high-latency links
    /// - Cubic congestion control for stable WAN performance
    /// - Higher initial RTT estimate
    pub fn wan() -> Self {
        Self {
            connection: ConnectionConfig {
                idle_timeout: Duration::from_secs(60),
                keep_alive_interval: Duration::from_secs(20),
                handshake_timeout: Duration::from_secs(30),
                ..Default::default()
            },
            congestion: CongestionConfig {
                controller: CongestionController::Cubic,
                initial_rtt: Duration::from_millis(200),
                ..Default::default()
            },
            migration: MigrationConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Large cluster configuration (1000+ nodes).
    ///
    /// - High connection limits
    /// - Large session cache for 0-RTT
    /// - Efficient stream settings
    pub fn large_cluster() -> Self {
        Self {
            connection: ConnectionConfig {
                max_connections: 4096,
                ..Default::default()
            },
            streams: StreamConfig {
                max_uni_streams: 200,
                max_bi_streams: 200,
                ..Default::default()
            },
            zero_rtt: ZeroRttConfig {
                session_cache_capacity: 4096,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// INSECURE development configuration.
    ///
    /// **WARNING**: This configuration uses self-signed certificates and
    /// skips server verification. Only use for local development and testing.
    ///
    /// # Security
    ///
    /// This is vulnerable to man-in-the-middle attacks. Never use in production.
    pub fn insecure_dev() -> Self {
        Self {
            tls: TlsConfig {
                skip_verification: true,
                mtls_enabled: false,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Builder method to set TLS configuration.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Builder method to set connection configuration.
    pub fn with_connection(mut self, connection: ConnectionConfig) -> Self {
        self.connection = connection;
        self
    }

    /// Builder method to set stream configuration.
    pub fn with_streams(mut self, streams: StreamConfig) -> Self {
        self.streams = streams;
        self
    }

    /// Builder method to set 0-RTT configuration.
    pub fn with_zero_rtt(mut self, zero_rtt: ZeroRttConfig) -> Self {
        self.zero_rtt = zero_rtt;
        self
    }

    /// Builder method to set congestion configuration.
    pub fn with_congestion(mut self, congestion: CongestionConfig) -> Self {
        self.congestion = congestion;
        self
    }

    /// Builder method to set migration configuration.
    pub fn with_migration(mut self, migration: MigrationConfig) -> Self {
        self.migration = migration;
        self
    }

    /// Builder method to set datagram configuration.
    pub fn with_datagram(mut self, datagram: DatagramConfig) -> Self {
        self.datagram = datagram;
        self
    }

    /// Builder method to set Plumtree-specific configuration.
    pub fn with_plumtree(mut self, plumtree: PlumtreeQuicConfig) -> Self {
        self.plumtree = plumtree;
        self
    }
}

/// TLS/certificate configuration.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to the server certificate file (PEM format).
    pub cert_path: Option<PathBuf>,

    /// Path to the server private key file (PEM format).
    pub key_path: Option<PathBuf>,

    /// Path to the CA certificate for peer verification.
    pub ca_path: Option<PathBuf>,

    /// Enable mutual TLS (client certificate verification).
    pub mtls_enabled: bool,

    /// ALPN protocols to advertise.
    ///
    /// Defaults to `["plumtree/1"]`.
    pub alpn_protocols: Vec<Vec<u8>>,

    /// **INSECURE**: Skip server certificate verification.
    ///
    /// Only use for development/testing. Vulnerable to MITM attacks.
    pub skip_verification: bool,

    /// Server name for TLS SNI (Server Name Indication).
    ///
    /// This is used during TLS handshake for:
    /// - SNI extension (allows server to select correct certificate)
    /// - Certificate hostname verification (unless `skip_verification` is true)
    ///
    /// For self-signed certificates, this should match the CN or SAN in the cert.
    /// Common values: `"localhost"` for development, hostname/domain for production.
    ///
    /// Default: `"localhost"` (suitable for self-signed certs).
    pub server_name: Option<String>,

    /// Expected peer ID for client certificate verification.
    ///
    /// When set and mTLS is enabled, incoming client certificates must contain
    /// a SAN entry with format `peer:<expected_peer_id>`. Certificates without
    /// a matching peer ID will be rejected.
    ///
    /// This provides MITM protection by binding application-layer peer identity
    /// to transport-layer certificate identity.
    ///
    /// Default: `None` (any valid certificate is accepted).
    pub expected_peer_id: Option<String>,

    /// Require peer ID SAN in client certificates.
    ///
    /// When true, client certificates without a `peer:*` SAN entry will be
    /// rejected, even if no specific `expected_peer_id` is set.
    ///
    /// Default: `false`.
    pub require_peer_id: bool,
}

impl TlsConfig {
    /// Create a new TLS configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set certificate path.
    pub fn with_cert_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.cert_path = Some(path.into());
        self
    }

    /// Builder method to set key path.
    pub fn with_key_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.key_path = Some(path.into());
        self
    }

    /// Builder method to set CA path.
    pub fn with_ca_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.ca_path = Some(path.into());
        self
    }

    /// Builder method to enable mTLS.
    pub fn with_mtls(mut self, enabled: bool) -> Self {
        self.mtls_enabled = enabled;
        self
    }

    /// Builder method to set ALPN protocols.
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Builder method to skip verification (INSECURE).
    pub fn with_skip_verification(mut self, skip: bool) -> Self {
        self.skip_verification = skip;
        self
    }

    /// Builder method to set server name for SNI.
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Enable mutual TLS with peer ID verification.
    ///
    /// When enabled:
    /// - Server requires client certificates
    /// - Client presents its certificate during handshake
    /// - Peer ID is extracted from certificate SAN for identity verification
    ///
    /// This provides MITM attack prevention by binding transport-layer
    /// identity to application-layer peer IDs.
    ///
    /// # Requirements
    ///
    /// Both server and client must have certificates configured via
    /// `with_cert_path()` and `with_key_path()`. Optionally, set a CA
    /// certificate via `with_ca_path()` for chain verification.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tls = TlsConfig::new()
    ///     .with_cert_path("certs/node1.crt")
    ///     .with_key_path("certs/node1.key")
    ///     .with_ca_path("certs/ca.crt")
    ///     .with_mtls_verification();
    /// ```
    pub fn with_mtls_verification(mut self) -> Self {
        self.mtls_enabled = true;
        self
    }

    /// Set the expected peer ID for client certificate verification.
    ///
    /// When set, incoming client certificates must contain a SAN entry
    /// with format `peer:<expected_peer_id>`. Certificates with a different
    /// peer ID or no peer ID SAN will be rejected.
    ///
    /// # Security
    ///
    /// This provides MITM protection by ensuring that only the expected
    /// peer can establish a connection. An attacker with a valid certificate
    /// for a different peer ID will be rejected.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tls = TlsConfig::new()
    ///     .with_cert_path("certs/server.crt")
    ///     .with_key_path("certs/server.key")
    ///     .with_ca_path("certs/ca.crt")
    ///     .with_mtls_verification()
    ///     .with_expected_peer_id("node123");
    /// ```
    pub fn with_expected_peer_id(mut self, peer_id: impl Into<String>) -> Self {
        self.expected_peer_id = Some(peer_id.into());
        self.mtls_enabled = true; // Implicitly enable mTLS
        self
    }

    /// Require peer ID SAN in client certificates.
    ///
    /// When enabled, client certificates must contain a `peer:*` SAN entry.
    /// This is useful when you want to enforce peer ID presence without
    /// verifying against a specific expected value.
    ///
    /// Note: This is automatically enforced when `expected_peer_id` is set.
    pub fn with_require_peer_id(mut self, require: bool) -> Self {
        self.require_peer_id = require;
        if require {
            self.mtls_enabled = true; // Implicitly enable mTLS
        }
        self
    }

    /// Check if custom certificates are configured.
    pub fn has_custom_certs(&self) -> bool {
        self.cert_path.is_some() && self.key_path.is_some()
    }

    /// Get server name for TLS, defaulting to "localhost".
    pub fn server_name_or_default(&self) -> &str {
        self.server_name.as_deref().unwrap_or("localhost")
    }

    /// Get the ALPN protocols, using defaults if not specified.
    pub fn alpn_protocols_or_default(&self) -> Vec<Vec<u8>> {
        if self.alpn_protocols.is_empty() {
            vec![super::tls::DEFAULT_ALPN.to_vec()]
        } else {
            self.alpn_protocols.clone()
        }
    }
}

/// Connection lifecycle configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Idle timeout before closing a connection.
    ///
    /// Default: 30 seconds.
    pub idle_timeout: Duration,

    /// Keep-alive interval to prevent idle timeout.
    ///
    /// Default: 10 seconds.
    pub keep_alive_interval: Duration,

    /// Maximum number of concurrent connections.
    ///
    /// Default: 256.
    pub max_connections: usize,

    /// Handshake timeout duration.
    ///
    /// Default: 10 seconds.
    pub handshake_timeout: Duration,

    /// Number of connection retry attempts.
    ///
    /// Default: 3.
    pub retries: u32,

    /// Delay between retry attempts.
    ///
    /// Default: 1 second.
    pub retry_delay: Duration,

    /// Timeout for acquiring a connection slot when at max capacity.
    ///
    /// When all connection slots are in use, new connection attempts will wait
    /// up to this duration for a slot to become available before failing with
    /// `MaxConnectionsReached`.
    ///
    /// Default: 5 seconds.
    pub acquire_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(10),
            max_connections: 256,
            handshake_timeout: Duration::from_secs(10),
            retries: 3,
            retry_delay: Duration::from_secs(1),
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

impl ConnectionConfig {
    /// Create a new connection configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set idle timeout.
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Builder method to set keep-alive interval.
    pub fn with_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.keep_alive_interval = interval;
        self
    }

    /// Builder method to set max connections.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Builder method to set handshake timeout.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Builder method to set retries.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Builder method to set retry delay.
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Builder method to set acquire timeout.
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }
}

/// Stream multiplexing configuration.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Maximum concurrent unidirectional streams.
    ///
    /// Default: 100.
    pub max_uni_streams: u32,

    /// Maximum concurrent bidirectional streams.
    ///
    /// Default: 100.
    pub max_bi_streams: u32,

    /// Receive buffer size per stream.
    ///
    /// Default: 64KB.
    pub recv_buffer: usize,

    /// Send buffer size per stream.
    ///
    /// Default: 64KB.
    pub send_buffer: usize,

    /// Maximum data per stream.
    ///
    /// Default: 1MB.
    pub max_data: u64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_uni_streams: 100,
            max_bi_streams: 100,
            recv_buffer: 64 * 1024, // 64KB
            send_buffer: 64 * 1024, // 64KB
            max_data: 1024 * 1024,  // 1MB
        }
    }
}

impl StreamConfig {
    /// Create a new stream configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set max unidirectional streams.
    pub fn with_max_uni_streams(mut self, max: u32) -> Self {
        self.max_uni_streams = max;
        self
    }

    /// Builder method to set max bidirectional streams.
    pub fn with_max_bi_streams(mut self, max: u32) -> Self {
        self.max_bi_streams = max;
        self
    }

    /// Builder method to set receive buffer size.
    pub fn with_recv_buffer(mut self, size: usize) -> Self {
        self.recv_buffer = size;
        self
    }

    /// Builder method to set send buffer size.
    pub fn with_send_buffer(mut self, size: usize) -> Self {
        self.send_buffer = size;
        self
    }

    /// Builder method to set max data per stream.
    pub fn with_max_data(mut self, max: u64) -> Self {
        self.max_data = max;
        self
    }
}

/// 0-RTT early data configuration.
///
/// When enabled, the transport configures TLS session resumption and early data
/// support. This allows reconnections to previously-visited servers to send data
/// before the TLS handshake completes, reducing latency.
///
/// # How It Works
///
/// 1. **First connection**: Normal TLS handshake, server sends session ticket
/// 2. **Subsequent connections**: Client uses ticket for 0-RTT data
/// 3. **Session tickets** are cached internally by rustls
///
/// # ⚠️ SECURITY WARNING - REPLAY ATTACKS ⚠️
///
/// **0-RTT data is replayable!** An attacker who captures 0-RTT packets can
/// replay them to the server. This is a fundamental property of 0-RTT, not a bug.
///
/// ## What This Means for Plumtree
///
/// When 0-RTT is enabled, **ALL messages** (including Graft and Prune) may be
/// sent as early data and are therefore **replayable**. The transport layer
/// cannot distinguish message types without parsing the application-layer
/// envelope, which requires knowing the peer ID serialization length.
///
/// ## Risk Assessment
///
/// | Message Type | Replay Impact | Risk Level |
/// |--------------|---------------|------------|
/// | **Gossip** | Duplicate delivery (handled by dedup) | LOW |
/// | **IHave** | Duplicate announcement (harmless) | LOW |
/// | **Graft** | May re-promote peer to eager set | MEDIUM |
/// | **Prune** | May re-demote peer to lazy set | MEDIUM |
///
/// **Recommendation**: Only enable 0-RTT if:
/// - Your network is trusted (no active attackers)
/// - The latency reduction is worth the replay risk
/// - You understand that tree topology may be affected by replays
///
/// # Configuration
///
/// ```ignore
/// // ⚠️ Only enable if you understand the replay risks!
/// let config = QuicConfig::default().with_zero_rtt(
///     ZeroRttConfig::default()
///         .with_enabled(true)
///         .with_session_cache_capacity(1024),
/// );
/// ```
#[derive(Debug, Clone)]
pub struct ZeroRttConfig {
    /// Enable 0-RTT early data.
    ///
    /// **⚠️ WARNING**: When enabled, ALL messages (including Graft/Prune) may
    /// be sent as replayable early data. Only enable in trusted networks where
    /// the latency benefit outweighs the replay attack risk.
    ///
    /// Default: false (safe default - no replay risk).
    pub enabled: bool,

    /// Maximum size of early data.
    ///
    /// Default: 16KB.
    pub max_early_data: u32,

    /// Number of sessions to cache for 0-RTT resumption.
    ///
    /// Default: 256.
    pub session_cache_capacity: usize,
}

impl Default for ZeroRttConfig {
    fn default() -> Self {
        Self {
            enabled: false,            // Safe default: disabled (no replay risk)
            max_early_data: 16 * 1024, // 16KB
            session_cache_capacity: 256,
        }
    }
}

impl ZeroRttConfig {
    /// Create a new 0-RTT configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to enable 0-RTT.
    ///
    /// **⚠️ WARNING**: Enabling 0-RTT makes ALL messages (including Graft/Prune)
    /// vulnerable to replay attacks. Only enable in trusted networks.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Builder method to set max early data size.
    pub fn with_max_early_data(mut self, size: u32) -> Self {
        self.max_early_data = size;
        self
    }

    /// Builder method to set session cache capacity.
    pub fn with_session_cache_capacity(mut self, capacity: usize) -> Self {
        self.session_cache_capacity = capacity;
        self
    }
}

/// Congestion control configuration.
#[derive(Debug, Clone)]
pub struct CongestionConfig {
    /// Congestion control algorithm.
    ///
    /// Default: Cubic.
    pub controller: CongestionController,

    /// Initial RTT estimate for congestion control.
    ///
    /// Default: 100ms.
    pub initial_rtt: Duration,

    /// Maximum UDP payload size.
    ///
    /// Default: 1200 bytes (conservative for path MTU).
    pub max_udp_payload: u16,
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            controller: CongestionController::Cubic,
            initial_rtt: Duration::from_millis(100),
            max_udp_payload: 1200,
        }
    }
}

impl CongestionConfig {
    /// Create a new congestion configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set congestion controller.
    pub fn with_controller(mut self, controller: CongestionController) -> Self {
        self.controller = controller;
        self
    }

    /// Builder method to set initial RTT.
    pub fn with_initial_rtt(mut self, rtt: Duration) -> Self {
        self.initial_rtt = rtt;
        self
    }

    /// Builder method to set max UDP payload.
    pub fn with_max_udp_payload(mut self, size: u16) -> Self {
        self.max_udp_payload = size;
        self
    }
}

/// Congestion control algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CongestionController {
    /// CUBIC congestion control.
    ///
    /// Traditional, stable, good for WAN links.
    /// Better for high-latency, lossy networks.
    #[default]
    Cubic,

    /// BBR congestion control.
    ///
    /// Bottleneck Bandwidth and Round-trip propagation time.
    /// Better for LAN and networks with variable latency.
    Bbr,
}

impl CongestionController {
    /// Get a string representation of the controller.
    pub fn as_str(&self) -> &'static str {
        match self {
            CongestionController::Cubic => "cubic",
            CongestionController::Bbr => "bbr",
        }
    }
}

/// Connection migration configuration.
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Enable connection migration (IP/port changes).
    ///
    /// Default: true.
    pub enabled: bool,

    /// Validate new paths before migration.
    ///
    /// Default: true.
    pub validate_path: bool,

    /// Timeout for path validation during migration.
    ///
    /// When a peer migrates to a new address, QUIC validates the new path
    /// before accepting it. This timeout controls how long to wait for
    /// path validation to complete.
    ///
    /// Default: 5 seconds.
    pub path_validation_timeout: Duration,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            validate_path: true,
            path_validation_timeout: Duration::from_secs(5),
        }
    }
}

impl MigrationConfig {
    /// Create a new migration configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to enable migration.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Builder method to set path validation.
    pub fn with_validate_path(mut self, validate: bool) -> Self {
        self.validate_path = validate;
        self
    }

    /// Builder method to set path validation timeout.
    pub fn with_path_validation_timeout(mut self, timeout: Duration) -> Self {
        self.path_validation_timeout = timeout;
        self
    }
}

/// QUIC datagram configuration.
///
/// QUIC datagrams (RFC 9221) provide a way to send unreliable data without
/// stream overhead. This is useful for gossip messages that are:
/// - Small (typically < 1200 bytes)
/// - Fire-and-forget (application handles retransmission)
/// - Latency-sensitive (no head-of-line blocking)
///
/// # When to Use Datagrams vs Streams
///
/// | Use Case | Recommended |
/// |----------|-------------|
/// | Small gossip messages | Datagrams |
/// | Large payloads (> MTU) | Streams |
/// | Control messages (Graft/Prune) | Streams (reliability needed) |
/// | High-throughput bulk data | Streams |
#[derive(Debug, Clone)]
pub struct DatagramConfig {
    /// Enable QUIC datagrams for message delivery.
    ///
    /// Default: false (use streams for compatibility).
    pub enabled: bool,

    /// Maximum datagram size.
    ///
    /// Messages larger than this will fall back to streams if `fallback_to_stream`
    /// is enabled, otherwise the send will fail.
    ///
    /// Default: 1200 bytes (conservative MTU-safe value).
    pub max_datagram_size: u16,

    /// Fall back to streams when message exceeds max_datagram_size.
    ///
    /// When true, oversized messages automatically use streams instead of failing.
    /// When false, sending a message larger than max_datagram_size returns an error.
    ///
    /// Default: true.
    pub fallback_to_stream: bool,
}

impl Default for DatagramConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_datagram_size: 1200,
            fallback_to_stream: true,
        }
    }
}

impl DatagramConfig {
    /// Create a new datagram configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to enable datagrams.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Builder method to set max datagram size.
    pub fn with_max_datagram_size(mut self, size: u16) -> Self {
        self.max_datagram_size = size;
        self
    }

    /// Builder method to set fallback behavior.
    pub fn with_fallback_to_stream(mut self, fallback: bool) -> Self {
        self.fallback_to_stream = fallback;
        self
    }
}

/// Plumtree-specific QUIC optimizations.
///
/// # Priority Streams (Future Feature)
///
/// The `priority_streams` and `priorities` fields are reserved for future implementation.
/// Currently, all messages use the same stream type. When implemented, priority streams
/// would allow different message types (Gossip, Graft, Prune, IHave) to use different
/// QUIC stream priorities, ensuring critical messages (like Graft for tree repair) are
/// delivered before less urgent messages.
#[derive(Debug, Clone)]
pub struct PlumtreeQuicConfig {
    /// Use priority streams for different message types.
    ///
    /// **Note:** This feature is not yet implemented. All messages currently use
    /// the same stream type regardless of this setting.
    ///
    /// Default: true.
    pub priority_streams: bool,

    /// Priority levels for different message types.
    ///
    /// **Note:** Reserved for future use. Not currently applied to streams.
    pub priorities: MessagePriorities,

    /// Automatically record RTT to peer scoring.
    ///
    /// Default: true.
    pub record_rtt: bool,
}

impl Default for PlumtreeQuicConfig {
    fn default() -> Self {
        Self {
            priority_streams: true,
            priorities: MessagePriorities::default(),
            record_rtt: true,
        }
    }
}

impl PlumtreeQuicConfig {
    /// Create a new Plumtree QUIC configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to enable priority streams.
    pub fn with_priority_streams(mut self, enabled: bool) -> Self {
        self.priority_streams = enabled;
        self
    }

    /// Builder method to set message priorities.
    pub fn with_priorities(mut self, priorities: MessagePriorities) -> Self {
        self.priorities = priorities;
        self
    }

    /// Builder method to enable RTT recording.
    pub fn with_record_rtt(mut self, enabled: bool) -> Self {
        self.record_rtt = enabled;
        self
    }
}

/// Priority levels for different Plumtree message types.
///
/// Higher values = higher priority.
///
/// # Future Feature
///
/// This configuration is reserved for future priority stream implementation.
/// Currently, these values are not applied to message delivery. When implemented,
/// messages with higher priority values would be scheduled for delivery before
/// lower priority messages during congestion.
#[derive(Debug, Clone)]
pub struct MessagePriorities {
    /// Priority for Gossip messages (full payload).
    ///
    /// Default: 2.
    pub gossip: i32,

    /// Priority for IHave announcements.
    ///
    /// Default: 1.
    pub ihave: i32,

    /// Priority for Graft requests (critical for tree repair).
    ///
    /// Default: 3 (highest).
    pub graft: i32,

    /// Priority for Prune messages.
    ///
    /// Default: 1.
    pub prune: i32,
}

impl Default for MessagePriorities {
    fn default() -> Self {
        Self {
            gossip: 2,
            ihave: 1,
            graft: 3, // Highest - critical for tree repair
            prune: 1,
        }
    }
}

impl MessagePriorities {
    /// Create new message priorities.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set gossip priority.
    pub fn with_gossip(mut self, priority: i32) -> Self {
        self.gossip = priority;
        self
    }

    /// Builder method to set ihave priority.
    pub fn with_ihave(mut self, priority: i32) -> Self {
        self.ihave = priority;
        self
    }

    /// Builder method to set graft priority.
    pub fn with_graft(mut self, priority: i32) -> Self {
        self.graft = priority;
        self
    }

    /// Builder method to set prune priority.
    pub fn with_prune(mut self, priority: i32) -> Self {
        self.prune = priority;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = QuicConfig::default();
        assert_eq!(config.connection.idle_timeout, Duration::from_secs(30));
        assert_eq!(config.connection.max_connections, 256);
        assert!(!config.zero_rtt.enabled); // 0-RTT disabled by default for safety
    }

    #[test]
    fn test_lan_preset() {
        let config = QuicConfig::lan();
        assert_eq!(config.connection.idle_timeout, Duration::from_secs(15));
        assert_eq!(config.congestion.controller, CongestionController::Bbr);
        assert_eq!(config.congestion.initial_rtt, Duration::from_millis(10));
        assert!(!config.migration.enabled);
    }

    #[test]
    fn test_wan_preset() {
        let config = QuicConfig::wan();
        assert_eq!(config.connection.idle_timeout, Duration::from_secs(60));
        assert_eq!(config.congestion.controller, CongestionController::Cubic);
        assert_eq!(config.congestion.initial_rtt, Duration::from_millis(200));
        assert!(config.migration.enabled);
    }

    #[test]
    fn test_large_cluster_preset() {
        let config = QuicConfig::large_cluster();
        assert_eq!(config.connection.max_connections, 4096);
        assert_eq!(config.streams.max_uni_streams, 200);
        assert_eq!(config.zero_rtt.session_cache_capacity, 4096);
    }

    #[test]
    fn test_insecure_dev_preset() {
        let config = QuicConfig::insecure_dev();
        assert!(config.tls.skip_verification);
        assert!(!config.tls.mtls_enabled);
    }

    #[test]
    fn test_builder_pattern() {
        let config = QuicConfig::default()
            .with_connection(
                ConnectionConfig::default()
                    .with_max_connections(512)
                    .with_idle_timeout(Duration::from_secs(60)),
            )
            .with_zero_rtt(ZeroRttConfig::default().with_enabled(true));

        assert_eq!(config.connection.max_connections, 512);
        assert_eq!(config.connection.idle_timeout, Duration::from_secs(60));
        assert!(config.zero_rtt.enabled);
    }

    #[test]
    fn test_tls_config() {
        let tls = TlsConfig::new()
            .with_cert_path("/path/to/cert.pem")
            .with_key_path("/path/to/key.pem")
            .with_mtls(true);

        assert!(tls.has_custom_certs());
        assert!(tls.mtls_enabled);
    }

    #[test]
    fn test_alpn_defaults() {
        let tls = TlsConfig::default();
        let alpn = tls.alpn_protocols_or_default();
        assert_eq!(alpn, vec![b"plumtree/1".to_vec()]);
    }

    #[test]
    fn test_message_priorities() {
        let priorities = MessagePriorities::default();
        assert_eq!(priorities.graft, 3); // Highest
        assert_eq!(priorities.gossip, 2);
        assert_eq!(priorities.ihave, 1);
        assert_eq!(priorities.prune, 1);
    }

    #[test]
    fn test_congestion_controller() {
        assert_eq!(CongestionController::Cubic.as_str(), "cubic");
        assert_eq!(CongestionController::Bbr.as_str(), "bbr");
        assert_eq!(CongestionController::default(), CongestionController::Cubic);
    }
}
