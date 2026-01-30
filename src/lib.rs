//! # memberlist-plumtree
//!
//! Plumtree (Epidemic Broadcast Trees) implementation built on top of memberlist.
//!
//! Plumtree combines the efficiency of tree-based broadcast (O(n) messages) with
//! the reliability of gossip protocols through a hybrid push/lazy-push approach.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Application                               │
//! │                   (PlumtreeDelegate)                             │
//! └────────────────────────────┬────────────────────────────────────┘
//!                              │ on_deliver()
//! ┌────────────────────────────▼────────────────────────────────────┐
//! │                     MemberlistStack                              │
//! │  (Full integration - Plumtree + Memberlist + auto peer sync)     │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                    PlumtreeDiscovery                             │
//! │  (Integration layer - combines Plumtree with Memberlist)         │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                        Plumtree                                  │
//! │  (Core protocol - eager/lazy push, tree repair)                  │
//! ├──────────────┬──────────────┬──────────────┬────────────────────┤
//! │  PeerState   │ MessageCache │  Scheduler   │     GraftTimer     │
//! │ (Eager/Lazy) │   (TTL)      │  (IHave)     │  (Retry/Backoff)   │
//! └──────────────┴──────────────┴──────────────┴────────────────────┘
//! ```
//!
//! ## API Entry Points
//!
//! | API | Use Case |
//! |-----|----------|
//! | [`MemberlistStack`] | Production - full Plumtree + Memberlist with SWIM peer discovery |
//! | [`PlumtreeDiscovery`] | Manual peer management, custom integration |
//! | [`Plumtree`] | Core protocol only, bring your own transport |
//!
//! ## How Plumtree Works
//!
//! - **Eager Push**: Full messages are sent along spanning tree edges (eager peers)
//! - **Lazy Push**: Message announcements (IHave) are sent to non-tree peers (lazy peers)
//! - **Self-Healing**: When a node receives an IHave for a missing message, it grafts
//!   the sender into its eager set, automatically repairing the tree
//!
//! ## Example
//!
//! ```ignore
//! use memberlist_plumtree::{Plumtree, PlumtreeConfig, PlumtreeDelegate};
//! use bytes::Bytes;
//!
//! // Define a delegate to receive messages
//! struct MyDelegate;
//! impl PlumtreeDelegate for MyDelegate {
//!     fn on_deliver(&self, msg_id: MessageId, payload: Bytes) {
//!         println!("Received message: {:?}", payload);
//!     }
//! }
//!
//! // Create Plumtree instance
//! let (plumtree, handle) = Plumtree::new(
//!     node_id,
//!     PlumtreeConfig::lan(),
//!     MyDelegate,
//! );
//!
//! // Add peers (from memberlist membership)
//! plumtree.add_peer(peer_id);
//!
//! // Broadcast a message to all nodes
//! let msg_id = plumtree.broadcast(b"hello world").await?;
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]
#![allow(clippy::type_complexity)]

mod adaptive_batcher;
mod bridge;
mod cleanup_tuner;
mod compression;
mod config;
mod error;
mod health;
mod integration;
mod message;
mod peer_health;
mod peer_scoring;
mod peer_state;
mod priority;

pub mod discovery;
mod plumtree;
mod pooled_transport;
mod rate_limiter;
mod runner;
mod scheduler;
mod stack;
pub mod storage;
pub mod sync;
pub mod testing;
mod transport;

#[cfg(feature = "metrics")]
#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]
pub mod metrics;

#[cfg(test)]
mod peer_state_test;

// Re-export adaptive batcher types
pub use adaptive_batcher::{AdaptiveBatcher, BatcherConfig, BatcherStats};

// Re-export cleanup tuner types
pub use cleanup_tuner::{
    BackpressureHint, CleanupConfig, CleanupParameters, CleanupReason, CleanupStats, CleanupTuner,
    EfficiencyTrend, PressureTrend,
};

// Re-export compression types
#[cfg(feature = "compression")]
#[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
pub use compression::{compress, decompress};
pub use compression::{
    CompressionAlgorithm, CompressionConfig, CompressionError, CompressionStats,
};

// Re-export config types
pub use config::{PlumtreeConfig, StorageConfig, SyncConfig};

// Re-export error types
pub use error::{Error, ErrorKind, Result};

// Re-export health types
pub use health::{
    CacheHealth, DeliveryHealth, HealthReport, HealthReportBuilder, HealthStatus, PeerHealth,
};

// Re-export message types
pub use message::{
    CacheStats, MessageCache, MessageId, MessageTag, PlumtreeMessage, PlumtreeMessageRef,
    SyncMessage,
};

// Re-export peer health monitoring types
pub use peer_health::{
    HealthSummary, PeerHealthConfig, PeerHealthState, PeerHealthTracker, PeerStatus, ZombieAction,
};

// Re-export peer state types
pub use peer_state::{
    AddPeerResult, PeerState, PeerStateBuilder, PeerStats, PeerTopology, RebalanceResult,
    RemovePeerResult,
};

// Re-export priority types
pub use priority::{MessagePriority, PriorityConfig, PriorityQueue, PriorityQueueStats};

// Re-export peer scoring types
pub use peer_scoring::{PeerScore, PeerScoring, ScoringConfig, ScoringStats};

// Re-export core plumtree types
pub use plumtree::{
    IncomingMessage, NoopDelegate, OutgoingMessage, Plumtree, PlumtreeDelegate, PlumtreeHandle,
    SeenMapStats,
};

// Re-export runner types
pub use runner::{create_plumtree_with_channels, PlumtreeRunnerWithTransport};

// Re-export deprecated runner for backwards compatibility
#[allow(deprecated)]
pub use runner::{PlumtreeRunner, PlumtreeRunnerBuilder};

// Re-export integration types
pub use integration::{
    decode_plumtree_envelope, decode_plumtree_message, encode_plumtree_envelope,
    encode_plumtree_envelope_into, encode_plumtree_envelope_with_compression,
    encode_plumtree_message, envelope_encoded_len, is_plumtree_message, BroadcastEnvelope, IdCodec,
    MessagePage, PlumtreeDiscovery, PlumtreeEventHandler,
};

// Re-export memberlist-specific integration types (requires memberlist feature)
#[cfg(feature = "memberlist")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist")))]
pub use discovery::{DemotionCallback, PlumtreeNodeDelegate, PromotionCallback};

// Re-export scheduler types
pub use scheduler::{ExpiredGraft, GraftTimer, IHaveQueue, IHaveScheduler, PendingIHave};

// Re-export rate limiter types
pub use rate_limiter::{GlobalRateLimiter, RateLimiter};

// Re-export transport types
pub use transport::{ChannelTransport, ChannelTransportError, NoopTransport, Transport};

// Re-export QUIC transport types (requires "quic" feature)
#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use transport::quic::{
    // mTLS peer verification utilities
    extract_peer_id_from_der,
    generate_self_signed_with_peer_id,
    generate_self_signed_with_peer_id_async,
    CongestionConfig,
    CongestionController,
    ConnectionConfig,
    // Connection migration events
    ConnectionEvent,
    ConnectionStats,
    DatagramConfig,
    DisconnectReason,
    IncomingConfig,
    IncomingHandle,
    IncomingStats,
    // Session caching for 0-RTT
    LruSessionCache,
    MapPeerResolver,
    MessagePriorities,
    MigrationConfig,
    NoopSessionCache,
    PeerIdVerifier,
    PeerResolver,
    PlumtreeQuicConfig,
    QuicConfig,
    QuicError,
    QuicStats,
    QuicTransport,
    SessionTicketStore,
    StreamConfig,
    TlsConfig,
    ZeroRttConfig,
    PEER_ID_SAN_PREFIX,
};

// Re-export pooled transport types
pub use pooled_transport::{PoolConfig, PoolStats, PooledTransport, PooledTransportError};

// Re-export standalone stack types
pub use stack::PlumtreeStackConfig;

// Re-export PlumtreeStack (requires "quic" feature)
#[cfg(feature = "quic")]
#[cfg_attr(docsrs, doc(cfg(feature = "quic")))]
pub use stack::PlumtreeStack;

// Re-export scheduler failure types
pub use scheduler::FailedGraft;

// Re-export bridge types (memberlist-independent)
pub use bridge::{BridgeConfig, BridgeRouter, LazarusHandle, LazarusStats, PlumtreeBridge};

// Re-export BridgeTaskHandle (requires tokio feature)
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub use bridge::BridgeTaskHandle;

// Re-export persistence module for peer state recovery
pub use bridge::persistence;

// Re-export memberlist-specific bridge types (requires memberlist feature)
#[cfg(feature = "memberlist")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist")))]
pub use bridge::AddressExtractor;

// Re-export memberlist integration types from discovery module (requires memberlist feature)
#[cfg(feature = "memberlist")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist")))]
pub use discovery::{
    BridgeEventDelegate, MemberlistDiscovery, MemberlistDiscoveryConfig, MemberlistDiscoveryHandle,
    MemberlistStack, MemberlistStackError, PlumtreeStackBuilder,
};

/// Re-export memberlist-core types for convenience (requires memberlist feature)
#[cfg(feature = "memberlist")]
#[cfg_attr(docsrs, doc(cfg(feature = "memberlist")))]
pub mod memberlist {
    pub use memberlist_core::*;
}
