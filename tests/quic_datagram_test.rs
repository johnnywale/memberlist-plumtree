//! Tests for QUIC datagram support.
//!
//! These tests verify datagram configuration and API.

#![cfg(feature = "quic")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{allocate_ports, install_crypto_provider};
use memberlist_plumtree::{
    DatagramConfig, IncomingConfig, MapPeerResolver, QuicConfig, QuicError, QuicTransport,
};

// ============================================================================
// Configuration Tests
// ============================================================================

#[test]
fn test_datagram_config_defaults() {
    let config = DatagramConfig::default();

    assert!(!config.enabled, "Datagrams should be disabled by default");
    assert_eq!(config.max_datagram_size, 1200);
    assert!(
        config.fallback_to_stream,
        "Fallback should be enabled by default"
    );
}

#[test]
fn test_datagram_config_builder() {
    let config = DatagramConfig::new()
        .with_enabled(true)
        .with_max_datagram_size(1000)
        .with_fallback_to_stream(false);

    assert!(config.enabled);
    assert_eq!(config.max_datagram_size, 1000);
    assert!(!config.fallback_to_stream);
}

#[test]
fn test_quic_config_with_datagram() {
    let config = QuicConfig::default().with_datagram(
        DatagramConfig::new()
            .with_enabled(true)
            .with_max_datagram_size(800),
    );

    assert!(config.datagram.enabled);
    assert_eq!(config.datagram.max_datagram_size, 800);
}

// ============================================================================
// Error Tests
// ============================================================================

#[test]
fn test_datagram_too_large_error() {
    let err = QuicError::DatagramTooLarge {
        size: 2000,
        max_size: 1200,
    };

    assert!(err.is_permanent(), "DatagramTooLarge should be permanent");
    assert!(!err.is_transient());

    let msg = err.to_string();
    assert!(msg.contains("2000"));
    assert!(msg.contains("1200"));
}

#[test]
fn test_datagram_send_failed_error() {
    let err = QuicError::DatagramSendFailed("connection lost".to_string());

    assert!(
        err.is_transient(),
        "DatagramSendFailed should be transient (retry may work)"
    );

    let msg = err.to_string();
    assert!(msg.contains("connection lost"));
}

#[test]
fn test_datagram_not_supported_error() {
    let err = QuicError::DatagramNotSupported;

    assert!(
        err.is_permanent(),
        "DatagramNotSupported should be permanent (use streams)"
    );

    let msg = err.to_string();
    assert!(msg.contains("not supported"));
}

// ============================================================================
// Transport API Tests
// ============================================================================

#[tokio::test]
async fn test_datagrams_enabled_check() {
    install_crypto_provider();

    let ports = allocate_ports(1);
    let addr: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();

    // Config with datagrams disabled (default)
    let config = QuicConfig::insecure_dev();
    let resolver = MapPeerResolver::<u64>::new(addr);

    let transport = QuicTransport::new(addr, config, resolver)
        .await
        .expect("Failed to create transport");

    assert!(!transport.datagrams_enabled());
    assert_eq!(transport.max_datagram_size(), 1200); // Default
}

#[tokio::test]
async fn test_datagrams_enabled_with_config() {
    install_crypto_provider();

    let ports = allocate_ports(1);
    let addr: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();

    let config = QuicConfig::insecure_dev().with_datagram(
        DatagramConfig::new()
            .with_enabled(true)
            .with_max_datagram_size(900),
    );

    let resolver = MapPeerResolver::<u64>::new(addr);

    let transport = QuicTransport::new(addr, config, resolver)
        .await
        .expect("Failed to create transport");

    assert!(transport.datagrams_enabled());
    assert_eq!(transport.max_datagram_size(), 900);
}

#[tokio::test]
async fn test_send_datagram_peer_not_found() {
    install_crypto_provider();

    let ports = allocate_ports(1);
    let addr: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();

    let config = QuicConfig::insecure_dev().with_datagram(DatagramConfig::new().with_enabled(true));

    let resolver = MapPeerResolver::<u64>::new(addr);
    // Don't add any peers

    let transport = QuicTransport::new(addr, config, resolver)
        .await
        .expect("Failed to create transport");

    // Try to send datagram to unknown peer
    let result = transport.send_datagram(&42u64, Bytes::from("hello")).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("peer not found"));
}

#[tokio::test]
async fn test_send_datagram_fallback_to_stream() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    // Configure with small datagram size to trigger fallback
    let config = QuicConfig::insecure_dev().with_datagram(
        DatagramConfig::new()
            .with_enabled(true)
            .with_max_datagram_size(10) // Very small
            .with_fallback_to_stream(true), // Enable fallback
    );

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .expect("Failed to create transport1");

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .expect("Failed to create transport2");

    // Start incoming on transport2
    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a properly encoded message larger than max_datagram_size (should fallback to stream)
    use memberlist_plumtree::{encode_plumtree_envelope, MessageId, PlumtreeMessage};

    let msg = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 1,
        payload: Bytes::from("This message payload is definitely longer than 10 bytes"),
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    let result = transport1.send_datagram(&2u64, encoded).await;

    // Should succeed via stream fallback
    assert!(
        result.is_ok(),
        "Fallback to stream should succeed: {:?}",
        result.err()
    );

    // Verify message was received
    let received = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Timeout")
        .expect("Channel closed");

    assert_eq!(received.0, 1u64); // Sender ID

    handle2.stop();
}

#[tokio::test]
async fn test_send_datagram_no_fallback_error() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    // Configure with small datagram size and NO fallback
    let config = QuicConfig::insecure_dev().with_datagram(
        DatagramConfig::new()
            .with_enabled(true)
            .with_max_datagram_size(10)
            .with_fallback_to_stream(false), // Disable fallback
    );

    let resolver = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver.add_peer(2, addr2);

    let transport = QuicTransport::with_shared_resolver(addr1, config, resolver)
        .await
        .expect("Failed to create transport");

    // Try to send message larger than max_datagram_size
    let large_message = Bytes::from("This message is way too long for the datagram limit");
    let result = transport.send_datagram(&2u64, large_message).await;

    // Should fail with DatagramTooLarge
    assert!(result.is_err());
    let err = result.unwrap_err();
    match err {
        QuicError::DatagramTooLarge { size, max_size } => {
            assert!(size > max_size);
            assert_eq!(max_size, 10);
        }
        _ => panic!("Expected DatagramTooLarge error, got: {:?}", err),
    }
}

#[tokio::test]
async fn test_send_stream_explicit() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    let config = QuicConfig::insecure_dev();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .expect("Failed to create transport1");

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .expect("Failed to create transport2");

    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Use send_stream explicitly
    use memberlist_plumtree::{encode_plumtree_envelope, MessageId, PlumtreeMessage};

    let msg = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 1,
        payload: Bytes::from("explicit stream message"),
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);

    transport1
        .send_stream(&2u64, encoded)
        .await
        .expect("send_stream should succeed");

    let received = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Timeout")
        .expect("Channel closed");

    assert_eq!(received.0, 1u64);

    handle2.stop();
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_datagram_config_max_size_zero() {
    // Edge case: zero max size
    let config = DatagramConfig::new().with_max_datagram_size(0);

    assert_eq!(config.max_datagram_size, 0);
}

#[test]
fn test_datagram_config_max_size_large() {
    // Edge case: very large max size
    let config = DatagramConfig::new().with_max_datagram_size(u16::MAX);

    assert_eq!(config.max_datagram_size, u16::MAX);
}
