//! Message types and utilities for Plumtree protocol.
//!
//! This module contains:
//! - [`MessageId`] - Unique message identifiers
//! - [`PlumtreeMessage`] - Protocol message types
//! - [`MessageCache`] - Message caching for Graft requests

mod cache;
mod id;
mod types;

pub use cache::{CacheStats, MessageCache};
pub use id::MessageId;
pub use types::{MessageTag, PlumtreeMessage, PlumtreeMessageRef, SyncMessage};
// Wire-format limits: encoders must respect these or receivers reject the
// message as malformed.
pub use types::{MAX_IHAVE_BATCH, MAX_SYNC_BATCH, MAX_SYNC_PUSH_MESSAGES};
