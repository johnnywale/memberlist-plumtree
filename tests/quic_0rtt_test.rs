//! Tests for QUIC 0-RTT session caching.
//!
//! These tests verify session ticket storage and the 0-RTT configuration API.

#![cfg(feature = "quic")]

mod common;

use std::sync::Arc;

use memberlist_plumtree::{
    LruSessionCache, NoopSessionCache, QuicConfig, SessionTicketStore, ZeroRttConfig,
};

// ============================================================================
// Session Cache Tests
// ============================================================================

#[test]
fn test_lru_session_cache_basic() {
    let cache = LruSessionCache::new(100);

    // Initially empty
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);
    assert_eq!(cache.capacity(), 100);

    // Store a ticket
    let ticket = vec![1, 2, 3, 4, 5, 6, 7, 8];
    cache.store("server.example.com", ticket.clone());

    assert_eq!(cache.len(), 1);
    assert!(!cache.is_empty());

    // Retrieve it
    let retrieved = cache.get("server.example.com");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap(), ticket);

    // Non-existent key
    assert!(cache.get("other.example.com").is_none());
}

#[test]
fn test_lru_session_cache_eviction() {
    let cache = LruSessionCache::new(3);

    cache.store("a.example.com", vec![1]);
    cache.store("b.example.com", vec![2]);
    cache.store("c.example.com", vec![3]);

    assert_eq!(cache.len(), 3);

    // Adding a 4th should evict 'a' (oldest)
    cache.store("d.example.com", vec![4]);

    assert_eq!(cache.len(), 3);
    assert!(cache.get("a.example.com").is_none()); // Evicted
    assert!(cache.get("b.example.com").is_some());
    assert!(cache.get("c.example.com").is_some());
    assert!(cache.get("d.example.com").is_some());
}

#[test]
fn test_lru_session_cache_lru_ordering() {
    let cache = LruSessionCache::new(3);

    cache.store("a", vec![1]);
    cache.store("b", vec![2]);
    cache.store("c", vec![3]);

    // Access 'a' to make it recently used
    let _ = cache.get("a");

    // Add 'd' - should evict 'b' (LRU since 'a' was accessed)
    cache.store("d", vec![4]);

    assert!(cache.get("a").is_some()); // Still there
    assert!(cache.get("b").is_none()); // Evicted
    assert!(cache.get("c").is_some());
    assert!(cache.get("d").is_some());
}

#[test]
fn test_lru_session_cache_remove() {
    let cache = LruSessionCache::new(100);

    cache.store("server1", vec![1]);
    cache.store("server2", vec![2]);

    cache.remove("server1");

    assert!(cache.get("server1").is_none());
    assert!(cache.get("server2").is_some());
    assert_eq!(cache.len(), 1);
}

#[test]
fn test_lru_session_cache_clear() {
    let cache = LruSessionCache::new(100);

    cache.store("server1", vec![1]);
    cache.store("server2", vec![2]);
    cache.store("server3", vec![3]);

    cache.clear();

    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);
}

#[test]
fn test_lru_session_cache_update() {
    let cache = LruSessionCache::new(100);

    cache.store("server1", vec![1, 1, 1]);
    assert_eq!(cache.get("server1").unwrap(), vec![1, 1, 1]);

    // Update the ticket
    cache.store("server1", vec![2, 2, 2]);
    assert_eq!(cache.get("server1").unwrap(), vec![2, 2, 2]);

    // Should still be only 1 entry
    assert_eq!(cache.len(), 1);
}

#[test]
fn test_lru_session_cache_fill_ratio() {
    let cache = LruSessionCache::new(10);

    assert_eq!(cache.fill_ratio(), 0.0);

    cache.store("a", vec![1]);
    assert_eq!(cache.fill_ratio(), 0.1);

    for i in 0..9 {
        cache.store(&format!("server{}", i), vec![i as u8]);
    }
    assert_eq!(cache.fill_ratio(), 1.0);
}

#[test]
fn test_noop_session_cache() {
    let cache = NoopSessionCache::new();

    // Store does nothing
    cache.store("server", vec![1, 2, 3]);

    // Get always returns None
    assert!(cache.get("server").is_none());

    // Always empty
    assert!(cache.is_empty());
    assert_eq!(cache.len(), 0);

    // Remove and clear are no-ops
    cache.remove("server");
    cache.clear();
    assert!(cache.is_empty());
}

#[test]
fn test_arc_session_cache() {
    let cache = Arc::new(LruSessionCache::new(100));

    cache.store("server", vec![1, 2, 3]);
    assert_eq!(cache.len(), 1);

    let retrieved = cache.get("server");
    assert!(retrieved.is_some());

    // Clone the Arc
    let cache2 = cache.clone();
    assert_eq!(cache2.len(), 1);
    assert!(cache2.get("server").is_some());
}

// ============================================================================
// 0-RTT Configuration Tests
// ============================================================================

#[test]
fn test_zero_rtt_config_defaults() {
    let config = ZeroRttConfig::default();

    // 0-RTT should be disabled by default (safe - no replay risk)
    assert!(!config.enabled, "0-RTT should be disabled by default");
    assert_eq!(config.max_early_data, 16 * 1024); // 16KB
    assert_eq!(config.session_cache_capacity, 256);
}

#[test]
fn test_zero_rtt_config_builder() {
    let config = ZeroRttConfig::default()
        .with_enabled(true)
        .with_max_early_data(32 * 1024)
        .with_session_cache_capacity(1000);

    assert!(config.enabled);
    assert_eq!(config.max_early_data, 32 * 1024);
    assert_eq!(config.session_cache_capacity, 1000);
}

#[test]
fn test_zero_rtt_replay_warning() {
    // When 0-RTT is enabled, ALL messages are replayable
    // including Graft/Prune control messages
    let config = ZeroRttConfig::default().with_enabled(true);

    assert!(config.enabled);
    // User must understand that enabling 0-RTT exposes all messages to replay attacks
}

#[test]
fn test_quic_config_with_zero_rtt() {
    let config = QuicConfig::default().with_zero_rtt(
        ZeroRttConfig::default()
            .with_enabled(true)
            .with_session_cache_capacity(2048),
    );

    assert!(config.zero_rtt.enabled);
    assert_eq!(config.zero_rtt.session_cache_capacity, 2048);
}

#[test]
fn test_large_cluster_preset_zero_rtt() {
    let config = QuicConfig::large_cluster();

    // Large cluster should have larger session cache
    assert_eq!(config.zero_rtt.session_cache_capacity, 4096);
}

#[test]
fn test_session_cache_clone_creates_empty() {
    let cache = LruSessionCache::new(100);

    cache.store("server1", vec![1, 2, 3]);
    cache.store("server2", vec![4, 5, 6]);

    assert_eq!(cache.len(), 2);

    let cloned = cache.clone();

    // Clone should have same capacity but empty
    assert_eq!(cloned.capacity(), 100);
    assert_eq!(cloned.len(), 0);
}

// ============================================================================
// Integration with QUIC Transport (Configuration Only)
// ============================================================================

#[tokio::test]
async fn test_transport_with_zero_rtt_config() {
    use common::{allocate_port, install_crypto_provider};
    use memberlist_plumtree::{MapPeerResolver, QuicTransport};
    use std::net::SocketAddr;

    install_crypto_provider();

    let port = allocate_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Create config with 0-RTT enabled
    let config = QuicConfig::default().with_zero_rtt(
        ZeroRttConfig::default()
            .with_enabled(true)
            .with_session_cache_capacity(512),
    );

    let resolver = MapPeerResolver::<u64>::new(addr);

    // Transport should create successfully with 0-RTT config
    let result = QuicTransport::new(addr, config, resolver).await;

    assert!(
        result.is_ok(),
        "Transport should create with 0-RTT config: {:?}",
        result.err()
    );

    let transport = result.unwrap();
    assert!(transport.zero_rtt_enabled());
    assert_eq!(transport.session_cache_capacity(), 512);
}

/// Test that transport reports 0-RTT disabled when not configured.
#[tokio::test]
async fn test_transport_zero_rtt_disabled() {
    use common::{allocate_port, install_crypto_provider};
    use memberlist_plumtree::{MapPeerResolver, QuicTransport};
    use std::net::SocketAddr;

    install_crypto_provider();

    let port = allocate_port();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    // Default config has 0-RTT disabled
    let config = QuicConfig::default();
    let resolver = MapPeerResolver::<u64>::new(addr);

    let transport = QuicTransport::new(addr, config, resolver)
        .await
        .expect("Should create transport");

    assert!(!transport.zero_rtt_enabled());
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

#[tokio::test]
async fn test_session_cache_concurrent_access() {
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let cache = Arc::new(LruSessionCache::new(1000));
    let mut tasks = JoinSet::new();

    // Spawn writers
    for i in 0..10 {
        let cache = cache.clone();
        tasks.spawn(async move {
            for j in 0..100 {
                cache.store(&format!("server-{}-{}", i, j), vec![i as u8, j as u8]);
                tokio::task::yield_now().await;
            }
        });
    }

    // Spawn readers
    for i in 0..10 {
        let cache = cache.clone();
        tasks.spawn(async move {
            for j in 0..100 {
                let _ = cache.get(&format!("server-{}-{}", i, j));
                tokio::task::yield_now().await;
            }
        });
    }

    // Wait for all tasks
    while let Some(result) = tasks.join_next().await {
        assert!(result.is_ok());
    }

    // Cache should have items (exact count depends on eviction)
    assert!(cache.len() > 0);
    assert!(cache.len() <= 1000);
}
