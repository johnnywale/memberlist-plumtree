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
pub use types::{MessageTag, PlumtreeMessage, PlumtreeMessageRef};
