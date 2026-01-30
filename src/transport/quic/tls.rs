//! TLS configuration helpers for QUIC transport.
//!
//! This module provides utilities for building TLS configurations:
//! - Loading certificates and keys from files
//! - Self-signed certificate generation for development
//! - Server and client configuration builders
//! - mTLS peer ID verification for MITM prevention

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DistinguishedName, RootCertStore, SignatureScheme};

use super::config::TlsConfig;
use super::error::QuicError;

/// Default ALPN protocol for Plumtree.
pub const DEFAULT_ALPN: &[u8] = b"plumtree/1";

/// Prefix for peer ID in certificate SAN (Subject Alternative Name).
///
/// Certificates should include a SAN entry like `peer:node123` where `node123`
/// is the peer ID that must match the expected peer ID during verification.
pub const PEER_ID_SAN_PREFIX: &str = "peer:";

// ============================================================================
// mTLS Peer ID Verification
// ============================================================================

/// Client certificate verifier that validates peer ID against certificate SAN.
///
/// This verifier implements mutual TLS (mTLS) peer identity binding to prevent
/// man-in-the-middle attacks. When enabled, incoming connections must present
/// a client certificate with a SAN entry matching the expected peer ID format.
///
/// # Certificate Requirements
///
/// Client certificates must include a Subject Alternative Name (SAN) of type
/// DNS or URI with the format `peer:<peer_id>`, where `<peer_id>` is the
/// application-layer peer identifier.
///
/// # Example Certificate Generation
///
/// Using `rcgen`:
/// ```ignore
/// let mut params = CertificateParams::new(vec!["peer:node123".to_string()]);
/// params.distinguished_name.push(DnType::CommonName, "node123");
/// let cert = Certificate::from_params(params)?;
/// ```
///
/// # Security
///
/// This verifier binds the transport-layer certificate identity to the
/// application-layer peer ID, preventing an attacker from using a valid
/// certificate to impersonate a different peer.
///
/// # Peer ID Verification
///
/// When `expected_peer_id` is set, the verifier will reject certificates that
/// don't contain a matching peer ID in their SAN entries. When `None`, any
/// valid certificate chain is accepted (logging the peer ID if present).
#[derive(Debug)]
pub struct PeerIdVerifier {
    /// Root certificates for chain validation.
    roots: Arc<RootCertStore>,
    /// Cached subjects for root hint (required for ClientCertVerifier trait).
    subjects: Vec<DistinguishedName>,
    /// Supported signature schemes for client certificates.
    supported_schemes: Vec<SignatureScheme>,
    /// Optional expected peer ID to verify against certificate SAN.
    /// When `Some`, the certificate must contain `peer:<expected_peer_id>` in SAN.
    /// When `None`, any valid certificate is accepted.
    expected_peer_id: Option<String>,
    /// Whether to require a peer ID SAN entry (even if expected_peer_id is None).
    require_peer_id: bool,
}

impl PeerIdVerifier {
    /// Create a new peer ID verifier with the given root certificates.
    pub fn new(roots: RootCertStore) -> Self {
        let subjects = roots.subjects();
        Self {
            roots: Arc::new(roots),
            subjects,
            supported_schemes: vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
            ],
            expected_peer_id: None,
            require_peer_id: false,
        }
    }

    /// Create a verifier from a CA certificate file path.
    pub fn from_ca_file(ca_path: &Path) -> Result<Self, QuicError> {
        let ca_certs = load_certificates(ca_path)?;
        let mut roots = RootCertStore::empty();
        for cert in ca_certs {
            roots
                .add(cert)
                .map_err(|e| QuicError::Certificate(e.to_string()))?;
        }
        Ok(Self::new(roots))
    }

    /// Set an expected peer ID to verify against the certificate SAN.
    ///
    /// When set, the verifier will reject certificates that don't contain
    /// a matching `peer:<expected_peer_id>` entry in their Subject Alternative Names.
    ///
    /// # Security
    ///
    /// This provides MITM protection by binding the application-layer peer ID
    /// to the transport-layer certificate identity.
    pub fn with_expected_peer_id(mut self, peer_id: impl Into<String>) -> Self {
        self.expected_peer_id = Some(peer_id.into());
        self
    }

    /// Require that certificates contain a peer ID SAN entry.
    ///
    /// When enabled, certificates without a `peer:*` SAN entry will be rejected,
    /// even if no specific expected peer ID is set.
    pub fn with_require_peer_id(mut self, require: bool) -> Self {
        self.require_peer_id = require;
        self
    }

    /// Get the expected peer ID if set.
    pub fn expected_peer_id(&self) -> Option<&str> {
        self.expected_peer_id.as_deref()
    }
}

impl ClientCertVerifier for PeerIdVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &self.subjects
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        // Verify the certificate chain using rustls's built-in verification
        let verifier = rustls::server::WebPkiClientVerifier::builder(self.roots.clone())
            .build()
            .map_err(|e| {
                rustls::Error::General(format!("failed to build WebPKI verifier: {}", e))
            })?;

        // Delegate chain verification to WebPKI
        verifier.verify_client_cert(end_entity, intermediates, now)?;

        // Extract peer ID from certificate SAN
        let extracted_peer_id = extract_peer_id_from_der(end_entity);

        // Validate peer ID based on configuration
        match (&self.expected_peer_id, &extracted_peer_id) {
            // Expected peer ID is set - must match exactly
            (Some(expected), Some(actual)) => {
                if expected != actual {
                    tracing::warn!(
                        expected = %expected,
                        actual = %actual,
                        "peer ID mismatch in client certificate"
                    );
                    return Err(rustls::Error::General(format!(
                        "peer ID mismatch: expected '{}', got '{}'",
                        expected, actual
                    )));
                }
                tracing::debug!(peer_id = %actual, "verified client certificate with matching peer ID");
            }
            // Expected peer ID is set but certificate has none
            (Some(expected), None) => {
                tracing::warn!(
                    expected = %expected,
                    "client certificate missing required peer ID SAN"
                );
                return Err(rustls::Error::General(format!(
                    "certificate missing peer ID SAN: expected '{}'",
                    expected
                )));
            }
            // No expected peer ID, but require_peer_id is set
            (None, None) if self.require_peer_id => {
                tracing::warn!("client certificate missing required peer ID SAN");
                return Err(rustls::Error::General(
                    "certificate missing required peer ID SAN".to_string(),
                ));
            }
            // No expected peer ID - log and accept
            (None, Some(peer_id)) => {
                tracing::debug!(peer_id = %peer_id, "verified client certificate with peer ID");
            }
            // No expected peer ID, no peer ID in cert - accept with warning
            (None, None) => {
                tracing::debug!("verified client certificate without peer ID SAN");
            }
        }

        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_schemes.clone()
    }

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

/// Extract peer ID from certificate SAN (Subject Alternative Name) entries.
///
/// Looks for a SAN entry with the format `peer:<peer_id>` and returns
/// the peer ID portion. Supports both DNS name and URI SAN types.
///
/// # Security
///
/// This function uses proper X.509 ASN.1 parsing via the `x509-parser` crate
/// to safely extract the peer ID, avoiding the pitfalls of string searching
/// on raw DER bytes.
///
/// # Returns
///
/// - `Some(peer_id)` if a matching SAN entry is found
/// - `None` if no peer ID SAN is present or parsing fails
pub fn extract_peer_id_from_der(cert: &CertificateDer<'_>) -> Option<String> {
    use x509_parser::prelude::*;

    // Parse the DER-encoded certificate
    let (_, x509_cert) = X509Certificate::from_der(cert.as_ref()).ok()?;

    // Look through all extensions for SubjectAlternativeName
    for ext in x509_cert.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
            for name in &san.general_names {
                match name {
                    // Check DNS names for peer ID format
                    GeneralName::DNSName(dns) => {
                        if let Some(peer_id) = dns.strip_prefix(PEER_ID_SAN_PREFIX) {
                            return Some(peer_id.to_string());
                        }
                    }
                    // Check URIs for peer ID format (e.g., "peer:node123" as URI)
                    GeneralName::URI(uri) => {
                        if let Some(peer_id) = uri.strip_prefix(PEER_ID_SAN_PREFIX) {
                            return Some(peer_id.to_string());
                        }
                    }
                    // Check RFC822 names (email-style) for peer ID format
                    GeneralName::RFC822Name(name) => {
                        if let Some(peer_id) = name.strip_prefix(PEER_ID_SAN_PREFIX) {
                            return Some(peer_id.to_string());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    None
}

/// Generate a self-signed certificate with peer ID in SAN.
///
/// This is useful for development and testing. The certificate includes:
/// - CN (Common Name) set to the peer ID
/// - SAN entry with format `peer:<peer_id>`
/// - localhost as an additional SAN for local testing
///
/// # Arguments
///
/// * `peer_id` - The peer ID to embed in the certificate
///
/// # Returns
///
/// A tuple of (certificate chain, private key)
pub fn generate_self_signed_with_peer_id(
    peer_id: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    let san_entries = vec![
        format!("{}{}", PEER_ID_SAN_PREFIX, peer_id),
        "localhost".to_string(),
    ];

    let certified_key = rcgen::generate_simple_self_signed(san_entries)
        .map_err(|e| QuicError::Certificate(format!("failed to generate cert: {}", e)))?;

    let cert_der = CertificateDer::from(certified_key.cert);
    let key_der = PrivateKeyDer::Pkcs8(certified_key.signing_key.serialize_der().into());

    Ok((vec![cert_der], key_der))
}

/// Generate a self-signed certificate with peer ID (async version).
///
/// This function offloads the CPU-intensive certificate generation to a
/// blocking thread pool, preventing it from blocking async worker threads.
pub async fn generate_self_signed_with_peer_id_async(
    peer_id: String,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    tokio::task::spawn_blocking(move || generate_self_signed_with_peer_id(&peer_id))
        .await
        .map_err(|e| QuicError::Internal(format!("spawn_blocking failed: {}", e)))?
}

/// Build a QUIC server configuration from TLS settings.
///
/// If custom certificates are provided, they will be loaded from disk.
/// Otherwise, a self-signed certificate will be generated.
///
/// **Note**: If generating self-signed certificates, this performs CPU-intensive
/// cryptographic operations. For async contexts, prefer [`server_config_async`].
///
/// This synchronous version is provided for non-async initialization scenarios.
#[allow(dead_code)]
pub fn server_config(tls: &TlsConfig) -> Result<quinn::ServerConfig, QuicError> {
    let (cert_chain, private_key) = if tls.has_custom_certs() {
        load_certs_and_key(tls)?
    } else {
        generate_self_signed()?
    };

    build_server_config(tls, cert_chain, private_key)
}

/// Build a QUIC server configuration from TLS settings (async version).
///
/// This function offloads self-signed certificate generation to a blocking
/// thread pool, preventing it from blocking async worker threads.
#[allow(dead_code)]
pub async fn server_config_async(tls: &TlsConfig) -> Result<quinn::ServerConfig, QuicError> {
    server_config_async_with_0rtt(tls, None).await
}

/// Build a QUIC server configuration with 0-RTT support (async version).
///
/// # Arguments
///
/// * `tls` - TLS configuration
/// * `max_early_data` - Maximum 0-RTT early data size (None = disabled)
///
/// This function offloads self-signed certificate generation to a blocking
/// thread pool, preventing it from blocking async worker threads.
pub async fn server_config_async_with_0rtt(
    tls: &TlsConfig,
    max_early_data: Option<u32>,
) -> Result<quinn::ServerConfig, QuicError> {
    server_config_async_full(tls, max_early_data, None).await
}

/// Build a QUIC server configuration with full QuicConfig support (async version).
///
/// This applies all configuration settings from QuicConfig to the quinn ServerConfig,
/// including stream limits, congestion control, migration settings, and datagrams.
///
/// This function offloads self-signed certificate generation to a blocking
/// thread pool, preventing it from blocking async worker threads.
pub async fn server_config_async_full(
    tls: &TlsConfig,
    max_early_data: Option<u32>,
    quic_config: Option<&super::config::QuicConfig>,
) -> Result<quinn::ServerConfig, QuicError> {
    let (cert_chain, private_key) = if tls.has_custom_certs() {
        load_certs_and_key(tls)?
    } else {
        generate_self_signed_async().await?
    };

    build_server_config_full(tls, cert_chain, private_key, max_early_data, quic_config)
}

/// Internal helper to build server config from certs.
fn build_server_config(
    tls: &TlsConfig,
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<quinn::ServerConfig, QuicError> {
    build_server_config_with_0rtt(tls, cert_chain, private_key, None)
}

/// Internal helper to build server config from certs with optional 0-RTT settings.
pub(crate) fn build_server_config_with_0rtt(
    tls: &TlsConfig,
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    max_early_data: Option<u32>,
) -> Result<quinn::ServerConfig, QuicError> {
    build_server_config_full(tls, cert_chain, private_key, max_early_data, None)
}

/// Build server config with full QuicConfig support.
///
/// This applies all configuration settings from QuicConfig to the quinn ServerConfig,
/// including stream limits, congestion control, migration settings, and datagrams.
pub(crate) fn build_server_config_full(
    tls: &TlsConfig,
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    max_early_data: Option<u32>,
    quic_config: Option<&super::config::QuicConfig>,
) -> Result<quinn::ServerConfig, QuicError> {
    let crypto = build_rustls_server_config(tls, cert_chain, private_key, max_early_data)?;

    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto)
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?,
    ));

    // Apply transport configuration
    let transport = Arc::get_mut(&mut config.transport).unwrap();
    apply_transport_config(transport, quic_config)?;

    Ok(config)
}

/// Build rustls ServerConfig with 0-RTT support.
fn build_rustls_server_config(
    tls: &TlsConfig,
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
    max_early_data: Option<u32>,
) -> Result<rustls::ServerConfig, QuicError> {
    let mut config = if tls.mtls_enabled {
        // Build with mTLS client certificate verification
        let mut verifier = if let Some(ca_path) = &tls.ca_path {
            PeerIdVerifier::from_ca_file(ca_path)?
        } else {
            // Use system roots + any custom CA
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            PeerIdVerifier::new(roots)
        };

        // Configure peer ID verification if requested
        if let Some(ref expected_peer_id) = tls.expected_peer_id {
            verifier = verifier.with_expected_peer_id(expected_peer_id);
        }
        if tls.require_peer_id {
            verifier = verifier.with_require_peer_id(true);
        }

        rustls::ServerConfig::builder()
            .with_client_cert_verifier(Arc::new(verifier))
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?
    } else {
        // No client authentication required
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?
    };

    config.alpn_protocols = tls.alpn_protocols_or_default();

    // Enable 0-RTT early data on server if configured
    if let Some(max_early) = max_early_data {
        config.max_early_data_size = max_early;
        config.send_half_rtt_data = true;
        tracing::debug!(max_early_data = max_early, "0-RTT enabled on server");
    }

    Ok(config)
}

/// Apply QuicConfig settings to quinn TransportConfig.
///
/// This configures:
/// - Connection settings (idle timeout, keep-alive)
/// - Stream limits (max uni/bi streams, buffer sizes)
/// - Congestion control (Cubic/BBR)
/// - Migration settings
/// - Datagram settings
fn apply_transport_config(
    transport: &mut quinn::TransportConfig,
    quic_config: Option<&super::config::QuicConfig>,
) -> Result<(), QuicError> {
    let config = match quic_config {
        Some(c) => c,
        None => {
            // Apply minimal defaults when no config provided
            transport.max_idle_timeout(Some(
                quinn::IdleTimeout::try_from(std::time::Duration::from_secs(30))
                    .map_err(|e| QuicError::TlsConfig(e.to_string()))?,
            ));
            return Ok(());
        }
    };

    // Connection settings
    transport.max_idle_timeout(Some(
        quinn::IdleTimeout::try_from(config.connection.idle_timeout)
            .map_err(|e| QuicError::TlsConfig(format!("invalid idle_timeout: {}", e)))?,
    ));
    transport.keep_alive_interval(Some(config.connection.keep_alive_interval));

    // Stream settings
    transport.max_concurrent_uni_streams(config.streams.max_uni_streams.into());
    transport.max_concurrent_bidi_streams(config.streams.max_bi_streams.into());
    transport.stream_receive_window(
        config
            .streams
            .recv_buffer
            .try_into()
            .unwrap_or(u32::MAX)
            .into(),
    );
    transport.send_window(config.streams.send_buffer.try_into().unwrap_or(u32::MAX) as u64);

    // Congestion control
    match config.congestion.controller {
        super::config::CongestionController::Cubic => {
            // Cubic is the default, no action needed
            tracing::trace!("using Cubic congestion control");
        }
        super::config::CongestionController::Bbr => {
            transport
                .congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
            tracing::trace!("using BBR congestion control");
        }
    }
    transport.initial_rtt(config.congestion.initial_rtt);
    transport.initial_mtu(config.congestion.max_udp_payload);

    // Migration settings
    if !config.migration.enabled {
        transport.allow_spin(false);
        // Note: quinn doesn't have a direct "disable migration" setting,
        // but we can reduce path validation timeout
    }

    // Datagram settings
    if config.datagram.enabled {
        transport.datagram_receive_buffer_size(Some(config.datagram.max_datagram_size as usize));
        transport.datagram_send_buffer_size(config.datagram.max_datagram_size as usize);
    }

    tracing::debug!(
        idle_timeout = ?config.connection.idle_timeout,
        keep_alive = ?config.connection.keep_alive_interval,
        max_uni_streams = config.streams.max_uni_streams,
        max_bi_streams = config.streams.max_bi_streams,
        congestion = config.congestion.controller.as_str(),
        migration_enabled = config.migration.enabled,
        datagram_enabled = config.datagram.enabled,
        "applied QUIC transport configuration"
    );

    Ok(())
}

/// Build a QUIC client configuration from TLS settings.
///
/// If mTLS is enabled and client certificates are provided, the client will
/// present its certificate during the TLS handshake.
#[allow(dead_code)]
pub fn client_config(tls: &TlsConfig) -> Result<quinn::ClientConfig, QuicError> {
    client_config_with_0rtt(tls, false)
}

/// Build a QUIC client configuration with optional 0-RTT support.
///
/// # Arguments
///
/// * `tls` - TLS configuration
/// * `enable_early_data` - Whether to enable 0-RTT early data
///
/// If mTLS is enabled and client certificates are provided, the client will
/// present its certificate during the TLS handshake.
pub fn client_config_with_0rtt(
    tls: &TlsConfig,
    enable_early_data: bool,
) -> Result<quinn::ClientConfig, QuicError> {
    client_config_full(tls, enable_early_data, None)
}

/// Build client config with full QuicConfig support.
///
/// This applies all configuration settings from QuicConfig to the quinn ClientConfig,
/// including stream limits, congestion control, and transport settings.
pub(crate) fn client_config_full(
    tls: &TlsConfig,
    enable_early_data: bool,
    quic_config: Option<&super::config::QuicConfig>,
) -> Result<quinn::ClientConfig, QuicError> {
    let mut crypto = if tls.skip_verification {
        insecure_client_crypto(tls)?
    } else if tls.mtls_enabled && tls.has_custom_certs() {
        mtls_client_crypto(tls)?
    } else {
        secure_client_crypto(tls)?
    };

    // Enable early data (0-RTT) if requested
    if enable_early_data {
        crypto.enable_early_data = true;
        tracing::debug!("0-RTT early data enabled on client");
    }

    // Configure session resumption for 0-RTT
    if let Some(config) = quic_config {
        if config.zero_rtt.enabled {
            // Create a session cache with the configured capacity
            let capacity = config.zero_rtt.session_cache_capacity.max(1);
            crypto.resumption = rustls::client::Resumption::in_memory_sessions(capacity);
            tracing::debug!(
                capacity = capacity,
                "session cache configured for 0-RTT resumption"
            );
        }
    }

    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
            .map_err(|e| QuicError::TlsConfig(e.to_string()))?,
    ));

    // Note: quinn::ClientConfig doesn't expose transport config directly
    // Transport settings are applied on the server side only
    // Client inherits settings from the connection/endpoint

    Ok(config)
}

/// Build a secure client crypto configuration.
fn secure_client_crypto(tls: &TlsConfig) -> Result<ClientConfig, QuicError> {
    let mut root_store = RootCertStore::empty();

    // Add system roots
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // Add custom CA if provided
    if let Some(ca_path) = &tls.ca_path {
        let ca_certs = load_certificates(ca_path)?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| QuicError::Certificate(e.to_string()))?;
        }
    }

    let mut config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // Set ALPN protocols
    config.alpn_protocols = tls.alpn_protocols_or_default();

    Ok(config)
}

/// Build a mTLS client crypto configuration with client certificate.
///
/// This configuration presents a client certificate during the TLS handshake,
/// enabling mutual TLS authentication where both client and server verify
/// each other's certificates.
fn mtls_client_crypto(tls: &TlsConfig) -> Result<ClientConfig, QuicError> {
    let mut root_store = RootCertStore::empty();

    // Add system roots
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // Add custom CA if provided
    if let Some(ca_path) = &tls.ca_path {
        let ca_certs = load_certificates(ca_path)?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| QuicError::Certificate(e.to_string()))?;
        }
    }

    // Load client certificate and key
    let (cert_chain, private_key) = load_certs_and_key(tls)?;

    let mut config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_client_auth_cert(cert_chain, private_key)
        .map_err(|e| QuicError::TlsConfig(e.to_string()))?;

    // Set ALPN protocols
    config.alpn_protocols = tls.alpn_protocols_or_default();

    Ok(config)
}

/// Build an insecure client crypto configuration.
///
/// **WARNING**: This skips server certificate verification and is
/// vulnerable to man-in-the-middle attacks. Only use for development.
fn insecure_client_crypto(tls: &TlsConfig) -> Result<ClientConfig, QuicError> {
    let mut config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureServerVerifier))
        .with_no_client_auth();

    // Set ALPN protocols
    config.alpn_protocols = tls.alpn_protocols_or_default();

    Ok(config)
}

/// Load certificates and private key from TLS configuration.
fn load_certs_and_key(
    tls: &TlsConfig,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    let cert_path = tls
        .cert_path
        .as_ref()
        .ok_or_else(|| QuicError::TlsConfig("certificate path not set".to_string()))?;

    let key_path = tls
        .key_path
        .as_ref()
        .ok_or_else(|| QuicError::TlsConfig("key path not set".to_string()))?;

    let certs = load_certificates(cert_path)?;
    let key = load_private_key(key_path)?;

    Ok((certs, key))
}

/// Load certificates from a PEM file.
fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, QuicError> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| QuicError::Certificate(format!("failed to open cert file {:?}: {}", path, e)))?
        .collect::<Result<_, _>>()
        .map_err(|e| QuicError::Certificate(format!("failed to parse certs: {}", e)))?;

    if certs.is_empty() {
        return Err(QuicError::Certificate(
            "no certificates found in file".to_string(),
        ));
    }

    Ok(certs)
}

/// Load a private key from a PEM file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, QuicError> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| {
        QuicError::Certificate(format!("failed to load private key from {:?}: {}", path, e))
    })
}

/// Generate a self-signed certificate for development.
///
/// **Note**: This function performs CPU-intensive cryptographic operations.
/// For use in async contexts, prefer [`generate_self_signed_async`] which
/// offloads the work to a blocking thread pool.
pub fn generate_self_signed(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| QuicError::Certificate(format!("failed to generate cert: {}", e)))?;

    let cert_der = CertificateDer::from(certified_key.cert);
    let key_der = PrivateKeyDer::Pkcs8(certified_key.signing_key.serialize_der().into());

    Ok((vec![cert_der], key_der))
}

/// Generate a self-signed certificate for development (async version).
///
/// This function offloads the CPU-intensive certificate generation to a
/// blocking thread pool, preventing it from blocking async worker threads.
///
/// # Example
///
/// ```ignore
/// let (certs, key) = generate_self_signed_async().await?;
/// ```
pub async fn generate_self_signed_async(
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), QuicError> {
    // Offload CPU-intensive crypto work to the blocking thread pool
    tokio::task::spawn_blocking(generate_self_signed)
        .await
        .map_err(|e| QuicError::Internal(format!("spawn_blocking failed: {}", e)))?
}

/// Server verifier that accepts any certificate.
///
/// **WARNING**: This is insecure and should only be used for development.
#[derive(Debug)]
struct InsecureServerVerifier;

impl rustls::client::danger::ServerCertVerifier for InsecureServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Install the ring crypto provider for rustls tests.
    /// This is safe to call multiple times (subsequent calls are no-ops).
    fn install_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn test_generate_self_signed() {
        let result = generate_self_signed();
        assert!(result.is_ok());

        let (certs, _key) = result.unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn test_generate_self_signed_with_peer_id() {
        let peer_id = "node123";
        let result = generate_self_signed_with_peer_id(peer_id);
        assert!(result.is_ok());

        let (certs, _key) = result.unwrap();
        assert_eq!(certs.len(), 1);

        // Verify we can extract the peer ID using proper X.509 parsing
        let extracted = extract_peer_id_from_der(&certs[0]);
        assert!(
            extracted.is_some(),
            "Should be able to extract peer ID from generated certificate"
        );
        assert_eq!(
            extracted.unwrap(),
            peer_id,
            "Extracted peer ID should match the one used during generation"
        );
    }

    #[test]
    fn test_extract_peer_id_from_der() {
        // Generate a certificate with a known peer ID
        let peer_id = "test-node-456";
        let (certs, _key) = generate_self_signed_with_peer_id(peer_id).unwrap();

        // Try to extract the peer ID using proper X.509 parsing
        let extracted = extract_peer_id_from_der(&certs[0]);

        assert!(
            extracted.is_some(),
            "Should find peer ID in certificate SAN"
        );
        assert_eq!(
            extracted.unwrap(),
            peer_id,
            "Extracted ID should exactly match the original peer ID"
        );
    }

    #[test]
    fn test_extract_peer_id_from_regular_cert() {
        // Generate a regular self-signed cert without peer ID
        let (certs, _key) = generate_self_signed().unwrap();

        // Should return None since there's no peer: prefix in the SAN
        let extracted = extract_peer_id_from_der(&certs[0]);
        assert!(
            extracted.is_none(),
            "Regular cert without peer: SAN should return None"
        );
    }

    #[test]
    fn test_server_config_self_signed() {
        install_crypto_provider();
        let tls = TlsConfig::default();
        let result = server_config(&tls);
        assert!(result.is_ok());
    }

    #[test]
    fn test_server_config_with_mtls_enabled() {
        install_crypto_provider();
        // mTLS enabled but no CA path - should use system roots
        let tls = TlsConfig {
            mtls_enabled: true,
            ..Default::default()
        };
        let result = server_config(&tls);
        assert!(result.is_ok());
    }

    #[test]
    fn test_client_config_insecure() {
        install_crypto_provider();
        let tls = TlsConfig {
            skip_verification: true,
            ..Default::default()
        };
        let result = client_config(&tls);
        assert!(result.is_ok());
    }

    #[test]
    fn test_peer_id_san_prefix() {
        assert_eq!(PEER_ID_SAN_PREFIX, "peer:");
    }

    #[test]
    fn test_alpn_default() {
        assert_eq!(DEFAULT_ALPN, b"plumtree/1");
    }

    #[test]
    fn test_alpn_protocols_or_default() {
        // Empty protocols should return default
        let tls = TlsConfig::default();
        let alpn = tls.alpn_protocols_or_default();
        assert_eq!(alpn, vec![b"plumtree/1".to_vec()]);

        // Custom protocols should be used
        let tls = TlsConfig {
            alpn_protocols: vec![b"custom/1".to_vec()],
            ..Default::default()
        };
        let alpn = tls.alpn_protocols_or_default();
        assert_eq!(alpn, vec![b"custom/1".to_vec()]);
    }

    #[test]
    fn test_tls_config_mtls_verification_builder() {
        let tls = TlsConfig::new()
            .with_cert_path("/path/to/cert.pem")
            .with_key_path("/path/to/key.pem")
            .with_ca_path("/path/to/ca.pem")
            .with_mtls_verification();

        assert!(tls.mtls_enabled);
        assert!(tls.has_custom_certs());
        assert!(tls.ca_path.is_some());
    }
}
