//! Sync strategy trait for pluggable anti-entropy mechanisms.
//!
//! This module defines the [`SyncStrategy`] trait that allows different
//! anti-entropy sync implementations:
//!
//! - [`MemberlistSyncStrategy`]: Uses memberlist's push-pull hooks
//! - [`PlumtreeSyncStrategy`]: Uses Plumtree's 4-message protocol
//! - [`NoOpSyncStrategy`]: Disabled sync (for testing)
//!
//! # Example
//!
//! ```ignore
//! // When using memberlist discovery
//! let strategy = MemberlistSyncStrategy::new(local_id, sync_handler, config);
//! let discovery = PlumtreeDiscovery::with_sync(config, delegate, strategy);
//!
//! // When using static discovery
//! let strategy = PlumtreeSyncStrategy::new(local_id, sync_handler, peers, config);
//! let discovery = PlumtreeDiscovery::with_sync(config, delegate, strategy);
//! ```

use bytes::Bytes;
use std::future::Future;

use crate::message::SyncMessage;
use crate::storage::MessageStore;
use crate::Transport;

/// Result type for sync operations.
pub type SyncResult<T> = Result<T, SyncError>;

/// Errors that can occur during sync operations.
#[derive(Debug, Clone)]
pub enum SyncError {
    /// Sync is disabled.
    Disabled,
    /// No peers available for sync.
    NoPeers,
    /// Transport error.
    Transport(String),
    /// Encoding/decoding error.
    Encoding(String),
    /// Storage error.
    Storage(String),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Disabled => write!(f, "sync disabled"),
            SyncError::NoPeers => write!(f, "no peers available"),
            SyncError::Transport(e) => write!(f, "transport error: {}", e),
            SyncError::Encoding(e) => write!(f, "encoding error: {}", e),
            SyncError::Storage(e) => write!(f, "storage error: {}", e),
        }
    }
}

impl std::error::Error for SyncError {}

/// Anti-entropy sync strategy trait.
///
/// Implementations provide different mechanisms for recovering missed messages:
///
/// - [`MemberlistSyncStrategy`]: Piggybacks on memberlist's push-pull TCP connections.
///   Uses `local_state()` and `merge_remote_state()` hooks.
///
/// - [`PlumtreeSyncStrategy`]: Runs its own background task with periodic sync.
///   Uses Plumtree's 4-message protocol (Request/Response/Pull/Push).
///
/// - [`NoOpSyncStrategy`]: Disabled sync for testing or minimal deployments.
///
/// # Type Parameters
///
/// * `I` - Node identifier type
/// * `S` - Storage backend type implementing [`MessageStore`]
///
/// # Implementation Notes
///
/// When `needs_background_task()` returns `false`, the strategy uses memberlist's
/// push-pull mechanism via `local_state()` and `merge_remote_state()`.
///
/// When `needs_background_task()` returns `true`, the strategy runs its own
/// periodic sync via `run_background_sync()`.
///
/// [`MemberlistSyncStrategy`]: super::MemberlistSyncStrategy
/// [`PlumtreeSyncStrategy`]: super::PlumtreeSyncStrategy
/// [`NoOpSyncStrategy`]: super::NoOpSyncStrategy
pub trait SyncStrategy<I, S>: Send + Sync + 'static
where
    I: Clone + Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static,
    S: MessageStore + 'static,
{
    /// Returns true if this strategy runs its own background task.
    ///
    /// - `MemberlistSyncStrategy`: returns `false` (uses push-pull hooks)
    /// - `PlumtreeSyncStrategy`: returns `true` (runs periodic sync)
    /// - `NoOpSyncStrategy`: returns `false` (no sync)
    fn needs_background_task(&self) -> bool;

    /// Get local sync state for push-pull exchange.
    ///
    /// Called by memberlist's `local_state()` hook during push-pull sync.
    /// Returns serialized sync state to send to peer.
    ///
    /// Only meaningful when `needs_background_task()` returns `false`.
    fn local_state(&self) -> impl Future<Output = Bytes> + Send;

    /// Merge remote sync state from push-pull exchange.
    ///
    /// Called by memberlist's `merge_remote_state()` hook.
    /// Compares remote state with local and triggers recovery if needed.
    ///
    /// Only meaningful when `needs_background_task()` returns `false`.
    ///
    /// # Arguments
    ///
    /// * `buf` - Remote sync state bytes from peer's `local_state()`
    /// * `peer_resolver` - Callback to get a peer ID for follow-up requests.
    ///   Called only if sync is needed after hash comparison.
    fn merge_remote_state<F>(
        &self,
        buf: &[u8],
        peer_resolver: F,
    ) -> impl Future<Output = ()> + Send
    where
        F: FnOnce() -> Option<I> + Send;

    /// Run the background sync task.
    ///
    /// Only called when `needs_background_task()` returns `true`.
    /// Runs until shutdown is signaled.
    ///
    /// # Arguments
    ///
    /// * `transport` - Transport for sending sync messages
    fn run_background_sync<T>(&self, transport: T) -> impl Future<Output = ()> + Send
    where
        T: Transport<I>;

    /// Handle incoming sync message.
    ///
    /// Processes SyncRequest/SyncResponse/SyncPull/SyncPush messages.
    /// Returns an optional response message to send back.
    ///
    /// # Arguments
    ///
    /// * `from` - Peer that sent the message
    /// * `message` - The sync message to handle
    ///
    /// # Returns
    ///
    /// * `Ok(Some(msg))` - Response message to send back to peer
    /// * `Ok(None)` - No response needed
    /// * `Err(e)` - Error occurred during handling
    fn handle_sync_message(
        &self,
        from: I,
        message: SyncMessage,
    ) -> impl Future<Output = SyncResult<Option<SyncMessage>>> + Send;

    /// Check if sync is enabled.
    fn is_enabled(&self) -> bool;

    /// Get the current root hash for sync state comparison.
    fn root_hash(&self) -> [u8; 32];

    /// Record a message in the sync state.
    ///
    /// Should be called when a message is delivered to keep sync state updated.
    fn record_message(&self, id: crate::MessageId, payload: &[u8]);

    /// Remove a message from the sync state.
    ///
    /// Should be called when a message is pruned from storage.
    fn remove_message(&self, id: &crate::MessageId);
}
