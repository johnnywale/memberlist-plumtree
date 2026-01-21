//! # memberlist-plumtree
//!
//! Plumtree (Epidemic Broadcast Trees) implementation built on top of memberlist.
//!
//! Plumtree combines the efficiency of tree-based broadcast (O(n) messages) with
//! the reliability of gossip protocols through a hybrid push/lazy-push approach.
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
mod config;
mod error;
mod health;
mod integration;
mod message;
mod peer_scoring;
mod peer_state;

mod plumtree;
mod pooled_transport;
mod rate_limiter;
mod runner;
mod scheduler;
pub mod testing;
mod transport;

#[cfg(feature = "metrics")]
mod metrics;

#[cfg(test)]
mod peer_state_test;

// Re-export adaptive batcher types
pub use adaptive_batcher::{AdaptiveBatcher, BatcherConfig, BatcherStats};

// Re-export cleanup tuner types
pub use cleanup_tuner::{
    BackpressureHint, CleanupConfig, CleanupParameters, CleanupReason, CleanupStats, CleanupTuner,
    EfficiencyTrend, PressureTrend,
};

// Re-export config types
pub use config::PlumtreeConfig;

// Re-export error types
pub use error::{Error, ErrorKind, Result};

// Re-export health types
pub use health::{
    CacheHealth, DeliveryHealth, HealthReport, HealthReportBuilder, HealthStatus, PeerHealth,
};

// Re-export message types
pub use message::{
    CacheStats, MessageCache, MessageId, MessageTag, PlumtreeMessage, PlumtreeMessageRef,
};

// Re-export peer state types
pub use peer_state::{
    AddPeerResult, PeerState, PeerStateBuilder, PeerStats, PeerTopology, RebalanceResult,
    RemovePeerResult,
};

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
    encode_plumtree_envelope_into, encode_plumtree_message, envelope_encoded_len,
    is_plumtree_message, BroadcastEnvelope, DemotionCallback, IdCodec, PlumtreeEventHandler,
    PlumtreeMemberlist, PlumtreeNodeDelegate, PromotionCallback,
};

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
    CongestionConfig, CongestionController, ConnectionConfig, ConnectionStats, MapPeerResolver,
    MessagePriorities, MigrationConfig, PeerResolver, PlumtreeQuicConfig, QuicConfig, QuicError,
    QuicStats, QuicTransport, StreamConfig, TlsConfig, ZeroRttConfig,
};

// Re-export pooled transport types
pub use pooled_transport::{PoolConfig, PoolStats, PooledTransport, PooledTransportError};

// Re-export scheduler failure types
pub use scheduler::FailedGraft;

// Re-export bridge types for Plumtree-Memberlist integration
pub use bridge::{
    AddressExtractor, BridgeConfig, BridgeEventDelegate, LazarusHandle, LazarusStats,
    MemberlistStack, MemberlistStackError, PlumtreeBridge, PlumtreeStackBuilder,
};

// Re-export persistence module for peer state recovery
pub use bridge::persistence;

/// Re-export memberlist-core types for convenience
pub mod memberlist {
    pub use memberlist_core::*;
}
