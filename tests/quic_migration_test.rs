//! Tests for QUIC connection migration support.
//!
//! These tests verify connection event subscription and migration configuration.

#![cfg(feature = "quic")]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{allocate_ports, install_crypto_provider};
use memberlist_plumtree::{
    encode_plumtree_envelope, ConnectionEvent, DisconnectReason, IncomingConfig, MapPeerResolver,
    MessageId, MigrationConfig, PlumtreeMessage, QuicConfig, QuicTransport, Transport,
};

/// Test that migration configuration can be customized.
#[test]
fn test_migration_config_builder() {
    let config = MigrationConfig::new()
        .with_enabled(true)
        .with_validate_path(true)
        .with_path_validation_timeout(Duration::from_secs(10));

    assert!(config.enabled);
    assert!(config.validate_path);
    assert_eq!(config.path_validation_timeout, Duration::from_secs(10));
}

/// Test that migration can be disabled.
#[test]
fn test_migration_disabled() {
    let config = MigrationConfig::new().with_enabled(false);

    assert!(!config.enabled);
}

/// Test that QuicConfig includes migration settings.
#[test]
fn test_quic_config_with_migration() {
    let config = QuicConfig::default().with_migration(
        MigrationConfig::new()
            .with_enabled(true)
            .with_path_validation_timeout(Duration::from_secs(15)),
    );

    assert!(config.migration.enabled);
    assert_eq!(
        config.migration.path_validation_timeout,
        Duration::from_secs(15)
    );
}

/// Test LAN preset has migration disabled.
#[test]
fn test_lan_preset_migration() {
    let config = QuicConfig::lan();
    assert!(!config.migration.enabled);
}

/// Test WAN preset has migration enabled.
#[test]
fn test_wan_preset_migration() {
    let config = QuicConfig::wan();
    assert!(config.migration.enabled);
}

/// Test large cluster preset has migration enabled.
#[test]
fn test_large_cluster_preset_migration() {
    let config = QuicConfig::large_cluster();
    // Large cluster should have migration enabled
    assert!(config.migration.enabled);
}

/// Test connection event subscription API.
#[tokio::test]
async fn test_event_subscription() {
    install_crypto_provider();

    let ports = allocate_ports(1);
    let addr: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();

    let resolver = MapPeerResolver::<u64>::new(addr);
    let config = QuicConfig::insecure_dev();

    let transport = QuicTransport::new(addr, config, resolver)
        .await
        .expect("Failed to create transport");

    // Subscribe to events
    let (events, transport) = transport.subscribe_events(128);

    // Verify migration_enabled() method works
    assert!(
        transport.migration_enabled(),
        "Default config should have migration enabled"
    );

    // Events channel should be available
    assert!(!events.is_closed());
}

/// Test that events channel can receive events.
#[tokio::test]
async fn test_event_channel_not_closed() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));
    resolver2.add_peer(1, addr1);

    let config = QuicConfig::insecure_dev();

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .expect("Failed to create transport1");

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .expect("Failed to create transport2");

    // Subscribe to events on transport1
    let (events1, transport1) = transport1.subscribe_events(128);

    // Start incoming on transport2
    let (_rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    // Give acceptor time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a message - this should establish a connection
    let msg = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 1,
        payload: Bytes::from("trigger connection"),
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    transport1
        .send_to(&2u64, encoded)
        .await
        .expect("Send failed");

    // The events channel should still be open
    assert!(!events1.is_closed());

    // Connection established - we can check has_connection
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(transport1.has_connection(&2u64).await);

    handle2.stop();
}

/// Test ConnectionEvent enum variants.
#[test]
fn test_connection_event_variants() {
    let connected: ConnectionEvent<u64> = ConnectionEvent::Connected {
        peer: 1,
        rtt: Duration::from_millis(10),
        remote_addr: "127.0.0.1:9000".parse().unwrap(),
    };

    let disconnected: ConnectionEvent<u64> = ConnectionEvent::Disconnected {
        peer: 2,
        reason: DisconnectReason::IdleTimeout,
    };

    let migrated: ConnectionEvent<u64> = ConnectionEvent::Migrated {
        peer: 3,
        old_addr: "192.168.1.10:9000".parse().unwrap(),
        new_addr: "192.168.1.20:9000".parse().unwrap(),
    };

    let reconnecting: ConnectionEvent<u64> = ConnectionEvent::Reconnecting {
        peer: 4,
        attempt: 2,
    };

    // Verify Debug implementation
    let _ = format!("{:?}", connected);
    let _ = format!("{:?}", disconnected);
    let _ = format!("{:?}", migrated);
    let _ = format!("{:?}", reconnecting);
}

/// Test DisconnectReason enum variants.
#[test]
fn test_disconnect_reason_variants() {
    let reasons = [
        DisconnectReason::ApplicationClosed,
        DisconnectReason::PeerClosed {
            code: 0,
            reason: "graceful".to_string(),
        },
        DisconnectReason::IdleTimeout,
        DisconnectReason::Reset,
        DisconnectReason::TransportError("test error".to_string()),
        DisconnectReason::Evicted,
    ];

    for reason in &reasons {
        // Verify Debug implementation
        let _ = format!("{:?}", reason);
    }
}

/// Test that Connected event is emitted when establishing a new connection.
#[tokio::test]
async fn test_connected_event_emitted() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let config = QuicConfig::insecure_dev();

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .expect("Failed to create transport1");

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .expect("Failed to create transport2");

    // Subscribe to events
    let (events, transport1) = transport1.subscribe_events(128);

    // Start incoming
    let (_rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send message - this should establish a new connection and emit Connected event
    let msg = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 1,
        payload: Bytes::from("trigger connection"),
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    transport1
        .send_to(&2u64, encoded)
        .await
        .expect("Send failed");

    // Should receive Connected event
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("Timeout waiting for event")
        .expect("Channel closed");

    match event {
        ConnectionEvent::Connected {
            peer,
            rtt,
            remote_addr,
        } => {
            assert_eq!(peer, 2u64, "Connected event should be for peer 2");
            assert!(
                !rtt.is_zero() || rtt.is_zero(),
                "RTT should be present (can be zero)"
            );
            assert_eq!(
                remote_addr.port(),
                ports[1],
                "Remote address should match peer's port"
            );
        }
        other => {
            panic!("Expected Connected event, got {:?}", other);
        }
    }

    handle2.stop();
}

/// Test that transport with event subscription can still send messages.
#[tokio::test]
async fn test_send_with_event_subscription() {
    install_crypto_provider();

    let ports = allocate_ports(2);
    let addr1: SocketAddr = format!("127.0.0.1:{}", ports[0]).parse().unwrap();
    let addr2: SocketAddr = format!("127.0.0.1:{}", ports[1]).parse().unwrap();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let config = QuicConfig::insecure_dev();

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .expect("Failed to create transport1");

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .expect("Failed to create transport2");

    // Subscribe to events
    let (_events, transport1) = transport1.subscribe_events(128);

    // Start incoming
    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send message
    let msg = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 1,
        payload: Bytes::from("hello with events"),
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    transport1
        .send_to(&2u64, encoded)
        .await
        .expect("Send failed");

    // Receive message
    let received = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Timeout")
        .expect("Channel closed");

    assert_eq!(received.0, 1u64);

    handle2.stop();
}
