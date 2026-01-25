//! Storage module for message persistence.
//!
//! This module provides traits and implementations for persisting messages
//! to enable anti-entropy synchronization and crash recovery.
//!
//! # Storage Backends
//!
//! - [`MemoryStore`] - In-memory storage (default, no persistence)
//! - [`SledStore`] - Sled-based persistent storage (requires `storage-sled` feature)
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::storage::{MessageStore, MemoryStore, StoredMessage};
//!
//! let store = MemoryStore::new(10_000);
//!
//! // Store a message
//! let msg = StoredMessage {
//!     id: MessageId::new(),
//!     round: 1,
//!     payload: Bytes::from_static(b"hello"),
//!     timestamp: 1234567890,
//! };
//! store.insert(&msg).await?;
//!
//! // Retrieve messages in a time range
//! let (ids, has_more) = store.get_range(start, end, 100, 0).await?;
//! ```

mod memory;

#[cfg(feature = "storage-sled")]
mod sled_backend;

pub use memory::MemoryStore;

#[cfg(feature = "storage-sled")]
pub use sled_backend::SledStore;

use bytes::Bytes;
use std::error::Error;
use std::future::Future;

use crate::MessageId;

/// Stored message entry.
///
/// Contains all information needed to persist and retrieve a message,
/// including metadata for time-based queries and garbage collection.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    /// Unique message identifier.
    pub id: MessageId,
    /// Broadcast round number (hop count).
    pub round: u32,
    /// Message payload (uses `Bytes` for efficient cloning).
    pub payload: Bytes,
    /// Timestamp when the message was first seen (milliseconds since UNIX epoch).
    pub timestamp: u64,
}

impl StoredMessage {
    /// Create a new stored message with the current timestamp.
    pub fn new(id: MessageId, round: u32, payload: Bytes) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            id,
            round,
            payload,
            timestamp,
        }
    }

    /// Create a stored message with a specific timestamp.
    pub fn with_timestamp(id: MessageId, round: u32, payload: Bytes, timestamp: u64) -> Self {
        Self {
            id,
            round,
            payload,
            timestamp,
        }
    }
}

/// Defines how messages are persisted and retrieved.
///
/// This trait provides the interface for message storage backends.
/// Implementations must be thread-safe (`Send + Sync`).
///
/// # Type Bounds
///
/// All methods return boxed futures to support async trait implementations
/// without requiring the `async_trait` crate dependency.
pub trait MessageStore: Send + Sync {
    /// Persist a message.
    ///
    /// Returns `Ok(true)` if this was a new message, `Ok(false)` if it already existed.
    fn insert(
        &self,
        msg: &StoredMessage,
    ) -> impl Future<Output = Result<bool, Box<dyn Error + Send + Sync>>> + Send;

    /// Retrieve a specific message by ID.
    ///
    /// Returns `Ok(Some(msg))` if found, `Ok(None)` if not found.
    fn get(
        &self,
        id: &MessageId,
    ) -> impl Future<Output = Result<Option<StoredMessage>, Box<dyn Error + Send + Sync>>> + Send;

    /// Check if we have a message without fetching the payload.
    ///
    /// More efficient than `get()` when you only need to check existence.
    fn contains(
        &self,
        id: &MessageId,
    ) -> impl Future<Output = Result<bool, Box<dyn Error + Send + Sync>>> + Send;

    /// Get message IDs within a time range with pagination.
    ///
    /// # Arguments
    ///
    /// * `start` - Start timestamp (inclusive, milliseconds since UNIX epoch)
    /// * `end` - End timestamp (inclusive, milliseconds since UNIX epoch)
    /// * `limit` - Maximum number of IDs to return
    /// * `offset` - Number of IDs to skip (for pagination)
    ///
    /// # Returns
    ///
    /// A tuple of `(message_ids, has_more)` where `has_more` indicates
    /// if there are more messages beyond the current page.
    fn get_range(
        &self,
        start: u64,
        end: u64,
        limit: usize,
        offset: usize,
    ) -> impl Future<Output = Result<(Vec<MessageId>, bool), Box<dyn Error + Send + Sync>>> + Send;

    /// Remove messages older than a specific timestamp (garbage collection).
    ///
    /// # Arguments
    ///
    /// * `older_than` - Remove messages with timestamp < this value
    ///
    /// # Returns
    ///
    /// The number of messages removed.
    fn prune(
        &self,
        older_than: u64,
    ) -> impl Future<Output = Result<usize, Box<dyn Error + Send + Sync>>> + Send;

    /// Get total message count.
    fn count(&self) -> impl Future<Output = Result<usize, Box<dyn Error + Send + Sync>>> + Send;
}

/// Get the current time in milliseconds since UNIX epoch.
pub fn current_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
