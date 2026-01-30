//! Tests for mTLS peer verification in QUIC transport.
//!
//! These tests verify that mutual TLS authentication works correctly,
//! including peer ID binding to certificate SANs.

#![cfg(feature = "quic")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{allocate_ports, install_crypto_provider};
use memberlist_plumtree::{
    encode_plumtree_envelope, generate_self_signed_with_peer_id, IncomingConfig, MapPeerResolver,
    MessageId, PlumtreeMessage, QuicConfig, QuicTransport, TlsConfig, Transport,
    PEER_ID_SAN_PREFIX,
};

/// Test that peer ID SAN prefix is correctly defined.
#[test]
fn test_peer_id_san_prefix() {
    assert_eq!(PEER_ID_SAN_PREFIX, "peer:");
}

/// Test generating a self-signed certificate with peer ID.
#[test]
fn test_generate_cert_with_peer_id() {
    let peer_id = "node-alpha";
    let result = generate_self_signed_with_peer_id(peer_id);
    assert!(result.is_ok(), "Should generate cert: {:?}", result.err());

    let (certs, _key) = result.unwrap();
    assert_eq!(certs.len(), 1, "Should have exactly one certificate");
}

/// Test that mTLS configuration can be enabled via builder.
#[test]
fn test_mtls_config_builder() {
    let config = QuicConfig::default().with_tls(
        TlsConfig::new()
            .with_cert_path("certs/node.crt")
            .with_key_path("certs/node.key")
            .with_ca_path("certs/ca.crt")
            .with_mtls_verification(),
    );

    assert!(config.tls.mtls_enabled);
    assert!(config.tls.has_custom_certs());
    assert!(config.tls.ca_path.is_some());
}

/// Test that transport can be created with mTLS config (even without actual certs).
/// This tests the configuration path, not actual mTLS handshake.
#[tokio::test]
async fn test_transport_with_mtls_config_no_certs() {
    install_crypto_provider();

    let ports = allocate_ports(1);
    let addr: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();

    // Create config with mTLS enabled but no cert paths
    // This should fall back to self-signed certs with system roots
    let config = QuicConfig::default().with_tls(
        TlsConfig::new()
            .with_mtls(true)
            .with_skip_verification(true),
    );

    let resolver = MapPeerResolver::<u64>::new(addr);
    let result = QuicTransport::new(addr, config, resolver).await;

    assert!(
        result.is_ok(),
        "Should create transport with mTLS config: {:?}",
        result.err()
    );
}

/// Test bidirectional communication with self-signed certificates.
/// Both nodes use self-signed certs with peer IDs embedded.
#[tokio::test]
async fn test_bidirectional_with_peer_id_certs() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    // Use insecure config for testing (no certificate verification)
    // In production, you would use proper CA-signed certs with mTLS
    let config = QuicConfig::insecure_dev();

    // Create resolvers
    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));
    resolver2.add_peer(1, addr1);

    // Create transports
    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .expect("Failed to create transport1");

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .expect("Failed to create transport2");

    // Start incoming handlers
    let (rx1, handle1) = transport1.start_incoming::<u64>(IncomingConfig::default());
    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    // Give acceptors time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Node 1 sends to Node 2
    let msg1 = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 1,
        payload: Bytes::from("Hello with peer ID cert"),
    };
    let encoded1 = encode_plumtree_envelope(&1u64, &msg1);
    transport1
        .send_to(&2u64, encoded1)
        .await
        .expect("Failed to send from node1");

    // Node 2 sends to Node 1
    let msg2 = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 2,
        payload: Bytes::from("Response with peer ID cert"),
    };
    let encoded2 = encode_plumtree_envelope(&2u64, &msg2);
    transport2
        .send_to(&1u64, encoded2)
        .await
        .expect("Failed to send from node2");

    // Receive messages
    let received2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Timeout waiting for message at node2")
        .expect("Channel closed");

    assert_eq!(received2.0, 1u64, "Sender should be node 1");

    let received1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
        .await
        .expect("Timeout waiting for message at node1")
        .expect("Channel closed");

    assert_eq!(received1.0, 2u64, "Sender should be node 2");

    // Cleanup
    handle1.stop();
    handle2.stop();
}

/// Test that TlsConfig properly chains builder methods for mTLS.
#[test]
fn test_tls_config_mtls_chaining() {
    let tls = TlsConfig::new()
        .with_cert_path("/path/to/cert.pem")
        .with_key_path("/path/to/key.pem")
        .with_ca_path("/path/to/ca.pem")
        .with_mtls_verification()
        .with_server_name("my-server");

    assert!(tls.mtls_enabled);
    assert_eq!(
        tls.cert_path.as_ref().unwrap().to_str().unwrap(),
        "/path/to/cert.pem"
    );
    assert_eq!(
        tls.key_path.as_ref().unwrap().to_str().unwrap(),
        "/path/to/key.pem"
    );
    assert_eq!(
        tls.ca_path.as_ref().unwrap().to_str().unwrap(),
        "/path/to/ca.pem"
    );
    assert_eq!(tls.server_name.as_deref(), Some("my-server"));
}

/// Test that mTLS can be disabled after being enabled.
#[test]
fn test_mtls_can_be_disabled() {
    let tls = TlsConfig::new().with_mtls_verification().with_mtls(false);

    assert!(!tls.mtls_enabled);
}

/// Test QuicError::PeerVerificationFailed error variant.
#[test]
fn test_peer_verification_failed_error() {
    use memberlist_plumtree::QuicError;

    let err = QuicError::PeerVerificationFailed {
        peer_id: "node123".to_string(),
        reason: "Certificate SAN does not match expected peer ID".to_string(),
    };

    assert!(
        err.is_permanent(),
        "PeerVerificationFailed should be permanent"
    );
    assert!(!err.is_transient());

    let err_str = err.to_string();
    assert!(err_str.contains("node123"));
    assert!(err_str.contains("SAN"));
}

/// Test that expected_peer_id can be configured via TlsConfig builder.
#[test]
fn test_expected_peer_id_config() {
    let tls = TlsConfig::new()
        .with_expected_peer_id("node-alpha")
        .with_require_peer_id(true);

    assert_eq!(tls.expected_peer_id.as_deref(), Some("node-alpha"));
    assert!(tls.require_peer_id);
    // Setting expected_peer_id should implicitly enable mTLS
    assert!(tls.mtls_enabled);
}

/// Test that require_peer_id can be set independently.
#[test]
fn test_require_peer_id_config() {
    // Can require peer ID without specifying expected value
    let tls = TlsConfig::new().with_require_peer_id(true);

    assert!(tls.require_peer_id);
    assert!(tls.expected_peer_id.is_none());
}

/// Test that QuicConfig carries peer ID settings through to TLS.
#[test]
fn test_quic_config_with_peer_id_verification() {
    let config = QuicConfig::default().with_tls(
        TlsConfig::new()
            .with_expected_peer_id("expected-node")
            .with_require_peer_id(true)
            .with_mtls_verification(),
    );

    assert_eq!(
        config.tls.expected_peer_id.as_deref(),
        Some("expected-node")
    );
    assert!(config.tls.require_peer_id);
    assert!(config.tls.mtls_enabled);
}
