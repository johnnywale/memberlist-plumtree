//! Tests for bidirectional QUIC transport.
//!
//! These tests verify that two QuicTransport instances can communicate
//! bidirectionally - both sending and receiving messages.

#![cfg(all(feature = "quic", feature = "tokio"))]

use bytes::Bytes;
use memberlist_plumtree::{
    encode_plumtree_envelope, IncomingConfig, MapPeerResolver, MessageId, PlumtreeMessage,
    QuicConfig, QuicTransport, Transport,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Test that two nodes can send messages to each other over QUIC.
#[tokio::test]
async fn test_bidirectional_quic_communication() {
    // Create two nodes
    let addr1: SocketAddr = "127.0.0.1:19001".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:19002".parse().unwrap();

    // Create resolvers
    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));
    resolver2.add_peer(1, addr1);

    // Create transports with insecure dev config for testing
    let config = QuicConfig::insecure_dev();

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
        payload: Bytes::from("Hello from Node 1"),
    };
    let encoded1 = encode_plumtree_envelope(&1u64, &msg1);
    transport1
        .send_to(&2u64, encoded1)
        .await
        .expect("Failed to send from node1 to node2");

    // Node 2 sends to Node 1
    let msg2 = PlumtreeMessage::Gossip {
        id: MessageId::new(),
        round: 2,
        payload: Bytes::from("Hello from Node 2"),
    };
    let encoded2 = encode_plumtree_envelope(&2u64, &msg2);
    transport2
        .send_to(&1u64, encoded2)
        .await
        .expect("Failed to send from node2 to node1");

    // Receive message at Node 2
    let received2 = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .expect("Timeout waiting for message at node2")
        .expect("Channel closed");

    assert_eq!(received2.0, 1u64, "Sender should be node 1");
    match received2.1 {
        PlumtreeMessage::Gossip { payload, round, .. } => {
            assert_eq!(payload, Bytes::from("Hello from Node 1"));
            assert_eq!(round, 1);
        }
        _ => panic!("Expected Gossip message"),
    }

    // Receive message at Node 1
    let received1 = tokio::time::timeout(Duration::from_secs(5), rx1.recv())
        .await
        .expect("Timeout waiting for message at node1")
        .expect("Channel closed");

    assert_eq!(received1.0, 2u64, "Sender should be node 2");
    match received1.1 {
        PlumtreeMessage::Gossip { payload, round, .. } => {
            assert_eq!(payload, Bytes::from("Hello from Node 2"));
            assert_eq!(round, 2);
        }
        _ => panic!("Expected Gossip message"),
    }

    // Check stats
    let stats1 = handle1.stats();
    let stats2 = handle2.stats();

    assert_eq!(stats1.messages_received, 1, "Node1 should receive 1 message");
    assert_eq!(stats2.messages_received, 1, "Node2 should receive 1 message");

    // Cleanup
    handle1.stop();
    handle2.stop();
}

/// Test multiple messages in sequence.
#[tokio::test]
async fn test_multiple_messages() {
    let addr1: SocketAddr = "127.0.0.1:19003".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:19004".parse().unwrap();

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

    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send 10 messages from node1 to node2
    for i in 0..10 {
        let msg = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new()],
            round: i,
        };
        let encoded = encode_plumtree_envelope(&1u64, &msg);
        transport1
            .send_to(&2u64, encoded)
            .await
            .expect("Failed to send");
    }

    // Receive all messages
    let mut received_count = 0;
    for _ in 0..10 {
        match tokio::time::timeout(Duration::from_secs(5), rx2.recv()).await {
            Ok(Ok((sender, _msg))) => {
                assert_eq!(sender, 1u64);
                received_count += 1;
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    assert_eq!(received_count, 10, "Should receive all 10 messages");

    let stats = handle2.stats();
    assert_eq!(stats.messages_received, 10);

    handle2.stop();
}

/// Test IHave message type.
#[tokio::test]
async fn test_ihave_message() {
    let addr1: SocketAddr = "127.0.0.1:19005".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:19006".parse().unwrap();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let config = QuicConfig::insecure_dev();

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .unwrap();

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .unwrap();

    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg_id = MessageId::new();
    let msg = PlumtreeMessage::IHave {
        message_ids: smallvec::smallvec![msg_id],
        round: 5,
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    transport1.send_to(&2u64, encoded).await.unwrap();

    let (sender, received_msg) = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(sender, 1u64);
    match received_msg {
        PlumtreeMessage::IHave {
            message_ids,
            round,
        } => {
            assert_eq!(message_ids.len(), 1);
            assert_eq!(message_ids[0], msg_id);
            assert_eq!(round, 5);
        }
        _ => panic!("Expected IHave message"),
    }

    handle2.stop();
}

/// Test Graft message type.
#[tokio::test]
async fn test_graft_message() {
    let addr1: SocketAddr = "127.0.0.1:19007".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:19008".parse().unwrap();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let config = QuicConfig::insecure_dev();

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .unwrap();

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .unwrap();

    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg_id = MessageId::new();
    let msg = PlumtreeMessage::Graft {
        message_id: msg_id,
        round: 3,
    };
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    transport1.send_to(&2u64, encoded).await.unwrap();

    let (sender, received_msg) = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(sender, 1u64);
    match received_msg {
        PlumtreeMessage::Graft { message_id, round } => {
            assert_eq!(message_id, msg_id);
            assert_eq!(round, 3);
        }
        _ => panic!("Expected Graft message"),
    }

    handle2.stop();
}

/// Test Prune message type.
#[tokio::test]
async fn test_prune_message() {
    let addr1: SocketAddr = "127.0.0.1:19009".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:19010".parse().unwrap();

    let resolver1 = Arc::new(MapPeerResolver::<u64>::new(addr1));
    resolver1.add_peer(2, addr2);

    let resolver2 = Arc::new(MapPeerResolver::<u64>::new(addr2));

    let config = QuicConfig::insecure_dev();

    let transport1 = QuicTransport::with_shared_resolver(addr1, config.clone(), resolver1)
        .await
        .unwrap();

    let transport2 = QuicTransport::with_shared_resolver(addr2, config.clone(), resolver2)
        .await
        .unwrap();

    let (rx2, handle2) = transport2.start_incoming::<u64>(IncomingConfig::default());

    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg = PlumtreeMessage::Prune;
    let encoded = encode_plumtree_envelope(&1u64, &msg);
    transport1.send_to(&2u64, encoded).await.unwrap();

    let (sender, received_msg) = tokio::time::timeout(Duration::from_secs(5), rx2.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(sender, 1u64);
    assert!(matches!(received_msg, PlumtreeMessage::Prune));

    handle2.stop();
}
