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

mod config;
mod error;
mod integration;
mod message;
mod peer_state;
mod plumtree;
mod rate_limiter;
mod runner;
mod scheduler;

#[cfg(feature = "metrics")]
mod metrics;

// Re-export config types
pub use config::PlumtreeConfig;

// Re-export error types
pub use error::{Error, Result};

// Re-export message types
pub use message::{
    CacheStats, MessageCache, MessageId, MessageTag, PlumtreeMessage, PlumtreeMessageRef,
};

// Re-export peer state types
pub use peer_state::{PeerState, PeerStateBuilder, PeerStats};

// Re-export core plumtree types
pub use plumtree::{
    IncomingMessage, NoopDelegate, OutgoingMessage, Plumtree, PlumtreeDelegate, PlumtreeHandle,
};

// Re-export runner types
pub use runner::{create_plumtree_with_channels, PlumtreeRunner, PlumtreeRunnerBuilder};

// Re-export integration types
pub use integration::{
    decode_plumtree_message, encode_plumtree_message, is_plumtree_message, PlumtreeEventHandler,
    PlumtreeMemberlist, PlumtreeNodeDelegate,
};

// Re-export scheduler types
pub use scheduler::{ExpiredGraft, GraftTimer, IHaveQueue, IHaveScheduler, PendingIHave};

// Re-export rate limiter types
pub use rate_limiter::{GlobalRateLimiter, RateLimiter};

/// Re-export memberlist-core types for convenience
pub mod memberlist {
    pub use memberlist_core::*;
}
