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
                validate_path: true,
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
            vec![b"plumtree/1".to_vec()]
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
/// # ⚠️ Not Yet Implemented
///
/// **Important**: The 0-RTT configuration is currently a **placeholder** for future
/// implementation. Setting `enabled: true` has **no effect** - all messages are sent
/// via regular 1-RTT streams regardless of this setting.
///
/// The configuration is provided to:
/// 1. Reserve the API for future 0-RTT support
/// 2. Allow applications to express intent for when support is added
/// 3. Document the security considerations upfront
///
/// Full 0-RTT support requires:
/// - Session ticket caching for connection resumption
/// - Using `connection.open_uni_early()` for early data
/// - Handling `EarlyDataRejected` errors with fallback
///
/// # Security Warning (for future implementation)
///
/// 0-RTT data is **replayable**! An attacker can capture and replay 0-RTT packets.
/// Only enable 0-RTT for idempotent operations (like Gossip messages).
///
/// **Never** send Graft or Prune control messages via 0-RTT.
#[derive(Debug, Clone)]
pub struct ZeroRttConfig {
    /// Enable 0-RTT early data.
    ///
    /// **Note**: Currently has no effect. See struct-level documentation.
    ///
    /// Default: false (safe default).
    pub enabled: bool,

    /// Maximum size of early data.
    ///
    /// Default: 16KB.
    pub max_early_data: u32,

    /// Number of sessions to cache for 0-RTT resumption.
    ///
    /// Default: 256.
    pub session_cache_capacity: usize,

    /// Only allow 0-RTT for Gossip messages (idempotent).
    ///
    /// Graft/Prune/IHave are never sent via 0-RTT when this is true.
    ///
    /// Default: true.
    pub gossip_only: bool,
}

impl Default for ZeroRttConfig {
    fn default() -> Self {
        Self {
            enabled: false,            // Safe default: disabled
            max_early_data: 16 * 1024, // 16KB
            session_cache_capacity: 256,
            gossip_only: true, // Only Gossip via 0-RTT
        }
    }
}

impl ZeroRttConfig {
    /// Create a new 0-RTT configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to enable 0-RTT.
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

    /// Builder method to set gossip-only mode.
    pub fn with_gossip_only(mut self, gossip_only: bool) -> Self {
        self.gossip_only = gossip_only;
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
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            validate_path: true,
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
}

/// Plumtree-specific QUIC optimizations.
#[derive(Debug, Clone)]
pub struct PlumtreeQuicConfig {
    /// Use priority streams for different message types.
    ///
    /// Default: true.
    pub priority_streams: bool,

    /// Priority levels for different message types.
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
        assert!(!config.zero_rtt.enabled);
        assert!(config.zero_rtt.gossip_only);
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
