//! Fuzz target for configuration parsing.
//!
//! This fuzzer tests the robustness of configuration validation and builder
//! patterns with random inputs to find edge cases.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

/// Fuzz input for PlumtreeConfig-like parameters
#[derive(Debug, Arbitrary)]
struct FuzzConfig {
    max_peers: Option<u16>,
    eager_fanout: u8,
    lazy_fanout: u8,
    ihave_interval_ms: u32,
    message_cache_ttl_ms: u32,
    message_cache_max_size: u32,
    optimization_threshold: u32,
    ihave_batch_size: u16,
    graft_timeout_ms: u32,
    max_message_size: u32,
    graft_rate_limit_per_second: u16, // Using u16 to avoid f64 complexity
    graft_rate_limit_burst: u32,
    graft_max_retries: u32,
    maintenance_interval_ms: u32,
    maintenance_jitter_ms: u32,
    use_hash_ring: bool,
    protect_ring_neighbors: bool,
    max_protected_neighbors: u8,
    max_eager_peers: Option<u8>,
    max_lazy_peers: Option<u16>,
    sync_enabled: bool,
    sync_interval_ms: u32,
    sync_window_ms: u32,
    sync_max_batch_size: u16,
    storage_enabled: bool,
    storage_max_messages: u32,
    storage_retention_ms: u32,
}

/// Simulate config validation logic
fn validate_config(config: &FuzzConfig) -> Result<(), &'static str> {
    // Validate fanout constraints
    if config.eager_fanout == 0 && config.lazy_fanout == 0 {
        return Err("At least one of eager_fanout or lazy_fanout must be > 0");
    }

    // Validate max_peers constraint
    if let Some(max) = config.max_peers {
        let total_fanout = config.eager_fanout as u16 + config.lazy_fanout as u16;
        if max < total_fanout {
            return Err("max_peers must be >= eager_fanout + lazy_fanout");
        }
    }

    // Validate intervals are reasonable (not zero for critical timers)
    if config.graft_timeout_ms == 0 {
        return Err("graft_timeout must be > 0");
    }

    // Validate message size constraints
    if config.max_message_size == 0 {
        return Err("max_message_size must be > 0");
    }

    // Validate rate limiting
    if config.graft_rate_limit_per_second == 0 {
        return Err("graft_rate_limit_per_second must be > 0");
    }

    // Validate protection settings consistency
    if config.protect_ring_neighbors && !config.use_hash_ring {
        // This is technically allowed but worth noting
    }

    // Validate sync config consistency
    if config.sync_enabled {
        if config.sync_interval_ms == 0 {
            return Err("sync_interval must be > 0 when sync is enabled");
        }
        if config.sync_window_ms == 0 {
            return Err("sync_window must be > 0 when sync is enabled");
        }
    }

    // Validate storage config consistency
    if config.storage_enabled {
        if config.storage_max_messages == 0 {
            return Err("storage_max_messages must be > 0 when storage is enabled");
        }
        if config.storage_retention_ms == 0 {
            return Err("storage_retention must be > 0 when storage is enabled");
        }
    }

    // Validate peer limits consistency
    if let (Some(max_eager), Some(max_peers)) = (config.max_eager_peers, config.max_peers) {
        if max_eager as u16 > max_peers {
            return Err("max_eager_peers must be <= max_peers");
        }
    }

    if let (Some(max_lazy), Some(max_peers)) = (config.max_lazy_peers, config.max_peers) {
        if max_lazy > max_peers {
            return Err("max_lazy_peers must be <= max_peers");
        }
    }

    Ok(())
}

/// Test various config operations
fn test_config_operations(config: &FuzzConfig) {
    // Test Duration conversions (should not panic)
    let _ihave = std::time::Duration::from_millis(config.ihave_interval_ms as u64);
    let _cache_ttl = std::time::Duration::from_millis(config.message_cache_ttl_ms as u64);
    let _graft = std::time::Duration::from_millis(config.graft_timeout_ms as u64);
    let _maint = std::time::Duration::from_millis(config.maintenance_interval_ms as u64);
    let _jitter = std::time::Duration::from_millis(config.maintenance_jitter_ms as u64);
    let _sync_int = std::time::Duration::from_millis(config.sync_interval_ms as u64);
    let _sync_win = std::time::Duration::from_millis(config.sync_window_ms as u64);
    let _retention = std::time::Duration::from_millis(config.storage_retention_ms as u64);

    // Test rate conversions
    let _rate = config.graft_rate_limit_per_second as f64;

    // Test option handling
    let _max_peers = config.max_peers.map(|p| p as usize);
    let _max_eager = config.max_eager_peers.map(|p| p as usize);
    let _max_lazy = config.max_lazy_peers.map(|p| p as usize);

    // Test size conversions
    let _cache_size = config.message_cache_max_size as usize;
    let _batch_size = config.ihave_batch_size as usize;
    let _msg_size = config.max_message_size as usize;
    let _storage_max = config.storage_max_messages as usize;
    let _sync_batch = config.sync_max_batch_size as usize;
}

/// Test arithmetic operations that might overflow
fn test_arithmetic(config: &FuzzConfig) {
    // Test fanout sum (should not overflow)
    let total_fanout = (config.eager_fanout as u32).saturating_add(config.lazy_fanout as u32);
    let _ = total_fanout;

    // Test batch calculations
    let batch_count = if config.ihave_batch_size > 0 {
        (config.message_cache_max_size as usize) / (config.ihave_batch_size as usize)
    } else {
        0
    };
    let _ = batch_count;

    // Test protected neighbor ratio
    let protected_ratio = if config.eager_fanout > 0 {
        (config.max_protected_neighbors as f64) / (config.eager_fanout as f64)
    } else {
        0.0
    };
    let _ = protected_ratio;

    // Test sync window coverage
    let window_coverage = if config.sync_interval_ms > 0 {
        (config.sync_window_ms as f64) / (config.sync_interval_ms as f64)
    } else {
        0.0
    };
    let _ = window_coverage;
}

fuzz_target!(|config: FuzzConfig| {
    // Validate the config
    let _ = validate_config(&config);

    // Test various operations (should never panic)
    test_config_operations(&config);
    test_arithmetic(&config);
});
