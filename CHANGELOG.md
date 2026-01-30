# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **CI/CD Improvements**
  - Added Clippy linting to CI pipeline
  - Added rustfmt formatting check
  - Added cargo doc build validation
  - Added cargo-deny dependency audit
  - Added multi-platform testing (Ubuntu, Windows, macOS)
  - Added MSRV (1.75.0) testing
  - Added feature combination testing

- **Metrics**
  - Added `NodeMetrics` struct for per-node labeled metrics in multi-node deployments
  - Added unit tests for metrics module
  - Made `metrics` module public for external access

- **Documentation**
  - Added CHANGELOG.md
  - Added architecture diagram to lib.rs
  - Added public API contract tests

- **Testing**
  - Added `tests/public_api_test.rs` for API stability verification

### Fixed
- Fixed Clippy warnings in `sled_backend.rs`:
  - Replaced unnecessary `ok_or_else` with `ok_or`
  - Removed unnecessary reference operators in slice comparison
- Fixed broken documentation link in `MemberlistStack::new()`
- Fixed QUIC TLS tests by installing ring crypto provider

### Changed
- Suppressed intentional dead code warnings in test utilities with `#[allow(dead_code)]`

## [0.1.0] - 2024-XX-XX

### Added
- Initial release of memberlist-plumtree
- Core Plumtree protocol implementation
  - Eager push for spanning tree messages
  - Lazy push (IHave/Graft) for tree repair
  - Automatic tree optimization via Prune
- Integration with memberlist for cluster membership
- `MemberlistStack` for full Plumtree + Memberlist integration
- `PlumtreeDiscovery` for manual peer management
- `PlumtreeBridge` for local simulations

### Features
- **Protocol**
  - Configurable eager/lazy fanout
  - IHave batching with configurable interval
  - Graft timeout with exponential backoff
  - Message deduplication with TTL-based cleanup
  - Ring neighbor protection for topology stability

- **Performance**
  - RTT-based peer scoring for optimal topology
  - Adaptive IHave batch sizing
  - Connection pooling for transports
  - Lock-free IHave queue (crossbeam SegQueue)
  - Sharded seen map (16 shards) for reduced contention

- **Observability**
  - Prometheus metrics (counters, gauges, histograms)
  - Structured tracing with spans
  - Health checks with status reporting
  - Chaos testing utilities

- **Transport**
  - QUIC transport with connection pooling
  - Channel transport for testing
  - Pluggable transport trait

- **Storage & Sync**
  - Anti-entropy sync for partition recovery
  - Pluggable message storage backends
  - Sled backend implementation

### Configuration Presets
- `PlumtreeConfig::default()` - General purpose
- `PlumtreeConfig::lan()` - Low latency environments
- `PlumtreeConfig::wan()` - High latency tolerance
- `PlumtreeConfig::large_cluster()` - 1000+ nodes

[Unreleased]: https://github.com/johnnywale/memberlist-plumtree/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/johnnywale/memberlist-plumtree/releases/tag/v0.1.0
