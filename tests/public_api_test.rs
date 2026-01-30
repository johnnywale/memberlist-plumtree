//! Public API Contract Tests
//!
//! These tests verify that the public API contract remains stable.
//! They test API surface availability, not behavior (which is covered by other tests).

use memberlist_plumtree::*;

// =============================================================================
// Configuration API
// =============================================================================

#[test]
fn test_plumtree_config_presets_exist() {
    let _default = PlumtreeConfig::default();
    let _lan = PlumtreeConfig::lan();
    let _wan = PlumtreeConfig::wan();
    let _large = PlumtreeConfig::large_cluster();
}

#[test]
fn test_plumtree_config_builder_methods() {
    let config = PlumtreeConfig::default()
        .with_eager_fanout(4)
        .with_lazy_fanout(8)
        .with_ihave_batch_size(32)
        .with_message_cache_ttl(std::time::Duration::from_secs(120))
        .with_graft_timeout(std::time::Duration::from_millis(750));

    assert_eq!(config.eager_fanout, 4);
    assert_eq!(config.lazy_fanout, 8);
    assert_eq!(config.ihave_batch_size, 32);
}

#[test]
fn test_plumtree_config_fields_accessible() {
    let config = PlumtreeConfig::default();

    // All important fields should be publicly accessible
    let _ = config.eager_fanout;
    let _ = config.lazy_fanout;
    let _ = config.ihave_interval;
    let _ = config.message_cache_ttl;
    let _ = config.message_cache_max_size;
    let _ = config.optimization_threshold;
    let _ = config.ihave_batch_size;
    let _ = config.graft_timeout;
    let _ = config.max_message_size;
    let _ = config.graft_rate_limit_per_second;
    let _ = config.graft_rate_limit_burst;
    let _ = config.graft_max_retries;
    let _ = config.maintenance_interval;
    let _ = config.maintenance_jitter;
    let _ = config.use_hash_ring;
    let _ = config.protect_ring_neighbors;
    let _ = config.max_protected_neighbors;
}

// =============================================================================
// Error API
// =============================================================================

#[test]
fn test_error_variants_exist() {
    // Test that all error variants can be constructed
    let _shutdown = Error::Shutdown;
    let _no_peers = Error::NoPeers;

    // Test error methods
    // Shutdown is permanent - once shut down, retrying won't help
    let err = Error::Shutdown;
    assert!(err.is_shutdown());
    assert!(err.is_permanent());
    assert!(!err.is_transient());

    // NoPeers is transient - peers may join later, retry could work
    let err = Error::NoPeers;
    assert!(err.is_transient());
    assert!(!err.is_permanent());
}

#[test]
fn test_error_kind_classification() {
    use memberlist_plumtree::ErrorKind;

    // Shutdown is permanent - once shut down, retrying won't help
    let err = Error::Shutdown;
    assert!(matches!(err.kind(), ErrorKind::Permanent));

    // NoPeers is transient - peers may join later, retry could work
    let err = Error::NoPeers;
    assert!(matches!(err.kind(), ErrorKind::Transient));
}

#[test]
fn test_error_display_and_debug() {
    let err = Error::Shutdown;
    let _ = format!("{}", err);
    let _ = format!("{:?}", err);
}

// =============================================================================
// Message API
// =============================================================================

#[test]
fn test_message_id_creation() {
    let id1 = MessageId::new();
    let id2 = MessageId::new();
    // IDs should be unique (different random component)
    assert_ne!(id1, id2);
}

#[test]
fn test_message_id_encoding() {
    let id = MessageId::new();

    // Test encode/decode roundtrip
    let bytes = id.encode_to_bytes();
    let decoded = MessageId::decode_from_slice(&bytes);
    assert_eq!(decoded, Some(id));
}

#[test]
fn test_message_id_display() {
    let id = MessageId::new();
    let display = format!("{}", id);
    assert!(!display.is_empty());
}

#[test]
fn test_message_id_parts_accessible() {
    let id = MessageId::new();
    // All parts should be accessible
    let _ = id.timestamp();
    let _ = id.counter();
    let _ = id.random();
}

// =============================================================================
// Peer State API
// =============================================================================

#[test]
fn test_peer_stats_fields() {
    let stats = PeerStats {
        eager_count: 3,
        lazy_count: 6,
    };

    assert_eq!(stats.eager_count, 3);
    assert_eq!(stats.lazy_count, 6);
    assert_eq!(stats.total(), 9);
}

#[test]
fn test_add_peer_result_variants() {
    use memberlist_plumtree::AddPeerResult;

    // Test that all variants exist
    let _eager = AddPeerResult::AddedEager;
    let _lazy = AddPeerResult::AddedLazy;
    let _evicted = AddPeerResult::AddedAfterEviction;
    let _exists = AddPeerResult::AlreadyExists;
    let _limit = AddPeerResult::LimitReached;

    // Can match on variants
    let result = AddPeerResult::AddedEager;
    assert!(matches!(result, AddPeerResult::AddedEager));
}

// =============================================================================
// Health API
// =============================================================================

#[test]
fn test_health_status_variants() {
    use memberlist_plumtree::HealthStatus;

    // Test that all variants exist
    let _healthy = HealthStatus::Healthy;
    let _degraded = HealthStatus::Degraded;
    let _unhealthy = HealthStatus::Unhealthy;

    let status = HealthStatus::Healthy;
    assert!(matches!(status, HealthStatus::Healthy));
}

// =============================================================================
// Metrics API (when enabled)
// =============================================================================

#[cfg(feature = "metrics")]
#[test]
fn test_metrics_module_public() {
    use memberlist_plumtree::metrics::{init_metrics, NodeMetrics};

    // init_metrics should be callable
    init_metrics();

    // NodeMetrics should be constructible
    let metrics = NodeMetrics::new("test_node");
    assert_eq!(metrics.node_id(), "test_node");
}

// =============================================================================
// Core Types Re-exports
// =============================================================================

#[test]
fn test_core_types_reexported() {
    // These types should be available at the crate root
    let _: Option<Plumtree<u64, NoopDelegate>> = None;
    let _: Option<PlumtreeHandle<u64>> = None;
    let _: Option<PlumtreeConfig> = None;
    let _: Option<MessageId> = None;
    let _: Option<Error> = None;
    let _: Option<PeerStats> = None;
}

// =============================================================================
// Delegate Trait
// =============================================================================

/// Test delegate implementation to verify trait is implementable
struct TestDelegate;

impl PlumtreeDelegate<u64> for TestDelegate {
    fn on_deliver(&self, _message_id: MessageId, _payload: bytes::Bytes) {
        // Minimal implementation
    }
}

#[test]
fn test_plumtree_delegate_implementable() {
    let _delegate = TestDelegate;
}

#[test]
fn test_noop_delegate_exists() {
    let _delegate = NoopDelegate;
}

// =============================================================================
// IdCodec Trait
// =============================================================================

#[test]
fn test_id_codec_builtin_implementations() {
    use memberlist_plumtree::IdCodec;

    // u64 implementation
    let id: u64 = 12345;
    let mut buf = Vec::new();
    id.encode_id(&mut buf);
    assert_eq!(id.encoded_id_len(), 8);

    let mut slice = &buf[..];
    let decoded = u64::decode_id(&mut slice);
    assert_eq!(decoded, Some(id));

    // String implementation
    let id = String::from("test_node");
    let mut buf = Vec::new();
    id.encode_id(&mut buf);

    let mut slice = &buf[..];
    let decoded = String::decode_id(&mut slice);
    assert_eq!(decoded, Some(id));
}
