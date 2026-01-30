//! Integration tests for QUIC transport.
//!
//! These tests require the `quic` feature flag.

#![cfg(feature = "quic")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{allocate_port, install_crypto_provider};
use memberlist_plumtree::{MapPeerResolver, PeerResolver, QuicConfig, QuicTransport, Transport};

/// Get an available port for testing.
fn get_test_addr() -> SocketAddr {
    format!("127.0.0.1:{}", allocate_port()).parse().unwrap()
}

#[tokio::test]
async fn test_quic_transport_creation() {
    install_crypto_provider();
    let addr = get_test_addr();
    let expected_port = addr.port();
    let resolver = MapPeerResolver::<u64>::new(addr);

    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(addr, config, resolver).await;

    assert!(
        transport.is_ok(),
        "Failed to create transport: {:?}",
        transport.err()
    );

    let transport = transport.unwrap();
    let local_addr = transport.local_addr();
    assert!(local_addr.is_ok());
    assert_eq!(local_addr.unwrap().port(), expected_port);
}

#[tokio::test]
async fn test_quic_config_presets() {
    // Test that all presets can be created
    let default = QuicConfig::default();
    assert_eq!(default.connection.idle_timeout, Duration::from_secs(30));

    let lan = QuicConfig::lan();
    assert_eq!(lan.connection.idle_timeout, Duration::from_secs(15));

    let wan = QuicConfig::wan();
    assert_eq!(wan.connection.idle_timeout, Duration::from_secs(60));

    let large = QuicConfig::large_cluster();
    assert_eq!(large.connection.max_connections, 4096);

    let dev = QuicConfig::insecure_dev();
    assert!(dev.tls.skip_verification);
}

#[tokio::test]
async fn test_quic_transport_peer_not_found() {
    install_crypto_provider();
    let addr = get_test_addr();
    let resolver = MapPeerResolver::<u64>::new(addr);
    // Don't add any peers

    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(addr, config, resolver).await.unwrap();

    // Try to send to unknown peer
    let result = transport.send_to(&42u64, Bytes::from("hello")).await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("peer not found"));
}

#[tokio::test]
async fn test_quic_transport_stats() {
    install_crypto_provider();
    let addr = get_test_addr();
    let resolver = MapPeerResolver::<u64>::new(addr);

    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(addr, config, resolver).await.unwrap();

    let stats = transport.stats().await;
    assert_eq!(stats.messages_sent, 0);
    assert_eq!(stats.messages_failed, 0);
    assert_eq!(stats.bytes_sent, 0);
}

#[tokio::test]
async fn test_quic_transport_shutdown() {
    install_crypto_provider();
    let addr = get_test_addr();
    let resolver = MapPeerResolver::<u64>::new(addr);

    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(addr, config, resolver).await.unwrap();

    // Shutdown should complete without error
    transport.shutdown().await;
}

#[tokio::test]
async fn test_resolver_operations() {
    let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let resolver = MapPeerResolver::<u64>::new(local_addr);

    // Add peers
    resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());
    resolver.add_peer(2, "192.168.1.11:9000".parse().unwrap());

    assert_eq!(resolver.peer_count(), 2);
    assert!(resolver.contains(&1));
    assert!(resolver.contains(&2));
    assert!(!resolver.contains(&3));

    // Resolve
    assert_eq!(
        resolver.resolve(&1),
        Some("192.168.1.10:9000".parse().unwrap())
    );
    assert_eq!(resolver.resolve(&3), None);

    // Update
    resolver.update_peer(1, "192.168.1.20:9000".parse().unwrap());
    assert_eq!(
        resolver.resolve(&1),
        Some("192.168.1.20:9000".parse().unwrap())
    );

    // Remove
    resolver.remove_peer(&1);
    assert!(!resolver.contains(&1));
    assert_eq!(resolver.peer_count(), 1);

    // Clear
    resolver.clear();
    assert_eq!(resolver.peer_count(), 0);
}

#[tokio::test]
async fn test_shared_resolver() {
    install_crypto_provider();
    let addr = get_test_addr();
    let resolver = Arc::new(MapPeerResolver::<u64>::new(addr));

    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::with_shared_resolver(addr, config, resolver.clone())
        .await
        .unwrap();

    // Add a peer through the shared resolver
    resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());

    // The transport's resolver should see the peer
    assert!(transport.resolver().contains(&1));
}

#[tokio::test]
async fn test_quic_transport_clone() {
    install_crypto_provider();
    let addr = get_test_addr();
    let resolver = MapPeerResolver::<u64>::new(addr);

    let config = QuicConfig::insecure_dev();
    let transport = QuicTransport::new(addr, config, resolver).await.unwrap();

    // Clone should work
    let transport2 = transport.clone();

    // Both should have the same local address
    assert_eq!(
        transport.local_addr().unwrap(),
        transport2.local_addr().unwrap()
    );
}

#[tokio::test]
async fn test_quic_config_builder() {
    use memberlist_plumtree::{ConnectionConfig, StreamConfig, ZeroRttConfig};

    let config = QuicConfig::default()
        .with_connection(
            ConnectionConfig::default()
                .with_max_connections(512)
                .with_idle_timeout(Duration::from_secs(60)),
        )
        .with_streams(StreamConfig::default().with_max_uni_streams(200))
        .with_zero_rtt(ZeroRttConfig::default().with_enabled(true));

    assert_eq!(config.connection.max_connections, 512);
    assert_eq!(config.connection.idle_timeout, Duration::from_secs(60));
    assert_eq!(config.streams.max_uni_streams, 200);
    assert!(config.zero_rtt.enabled);
}

#[tokio::test]
async fn test_quic_error_classification() {
    use memberlist_plumtree::QuicError;

    // Transient errors
    assert!(QuicError::HandshakeTimeout {
        addr: "127.0.0.1:9000".parse().unwrap(),
        timeout_ms: 5000,
    }
    .is_transient());

    assert!(QuicError::MaxConnectionsReached {
        current: 100,
        max: 100,
    }
    .is_transient());

    assert!(QuicError::EarlyDataRejected.is_transient());

    // Permanent errors
    assert!(QuicError::PeerNotFound("test".to_string()).is_permanent());
    assert!(QuicError::TlsConfig("bad config".to_string()).is_permanent());
    assert!(QuicError::Certificate("invalid".to_string()).is_permanent());
}
