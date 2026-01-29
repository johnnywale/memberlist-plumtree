//! Anti-entropy synchronization module.
//!
//! This module provides anti-entropy sync for Plumtree, enabling recovery
//! of missed messages after network partitions or node restarts.
//!
//! # Sync Strategies
//!
//! The module provides pluggable sync strategies via the [`SyncStrategy`] trait:
//!
//! - [`MemberlistSyncStrategy`]: Uses memberlist's push-pull hooks. Best when
//!   using memberlist for discovery - avoids duplicate timers and connections.
//!
//! - [`PlumtreeSyncStrategy`]: Uses Plumtree's 4-message protocol. Best when
//!   using static discovery or other non-memberlist discovery providers.
//!
//! - [`NoOpSyncStrategy`]: Disabled sync. For testing or minimal deployments.
//!
//! # How Anti-Entropy Sync Works
//!
//! ```text
//! Node A (Initiator)                    Node B (Responder)
//! ─────────────────                    ─────────────────
//!
//! 1. Calculate root_hash
//!    for time range
//!          │
//!          ▼
//! ┌─────────────────┐                  │
//! │  SyncRequest    │ ──────────────▶  │
//! │  {root_hash,    │                  ▼
//! │   time_range}   │         2. Compare with local hash
//! └─────────────────┘                  │
//!                             ┌────────┴────────┐
//!                             │                 │
//!                        MATCH              MISMATCH
//!                             │                 │
//!                             ▼                 ▼
//!                    ┌──────────────┐  ┌──────────────┐
//! 3. Done ◀───────── │SyncResponse  │  │SyncResponse  │ ──────▶ 3.
//!                    │{matches:true}│  │{matches:false│    Diff IDs
//!                    └──────────────┘  │ local_ids}   │         │
//!                                      └──────────────┘         ▼
//!                                                      ┌──────────────┐
//! 4. Receive ◀──────────────────────────────────────── │  SyncPull    │
//!    missing IDs                                       │  {missing}   │
//!          │                                           └──────────────┘
//!          ▼
//! ┌─────────────────┐
//! │  SyncPush       │ ──────────────▶  5. Store & Deliver
//! │  {messages}     │                     missing messages
//! └─────────────────┘
//! ```
//!
//! # Example: Using MemberlistSyncStrategy
//!
//! ```ignore
//! use memberlist_plumtree::sync::{MemberlistSyncStrategy, SyncHandler};
//! use memberlist_plumtree::storage::MemoryStore;
//!
//! // Create storage and sync handler
//! let store = Arc::new(MemoryStore::new(10_000));
//! let sync_handler = Arc::new(SyncHandler::new(store));
//!
//! // Create channel for sync requests
//! let (sync_tx, sync_rx) = async_channel::bounded(64);
//!
//! // Create memberlist sync strategy
//! let strategy = MemberlistSyncStrategy::new(
//!     sync_handler,
//!     Duration::from_secs(90),  // sync_window
//!     sync_tx,
//! );
//!
//! // Use in PlumtreeNodeDelegate's local_state/merge_remote_state
//! ```
//!
//! # Example: Using PlumtreeSyncStrategy
//!
//! ```ignore
//! use memberlist_plumtree::sync::{PlumtreeSyncStrategy, SyncHandler};
//! use memberlist_plumtree::storage::MemoryStore;
//! use memberlist_plumtree::SyncConfig;
//!
//! // Create storage and sync handler
//! let store = Arc::new(MemoryStore::new(10_000));
//! let sync_handler = Arc::new(SyncHandler::new(store));
//!
//! // Create Plumtree sync strategy
//! let strategy = PlumtreeSyncStrategy::new(
//!     local_id,
//!     sync_handler,
//!     peers,
//!     SyncConfig::enabled(),
//!     shutdown,
//! );
//!
//! // Run background sync with transport
//! strategy.run_background_sync(transport).await;
//! ```

mod handler;
mod merkle;
mod strategy;

mod memberlist_strategy;
mod noop_strategy;
mod plumtree_strategy;

// Core types
pub use handler::{SyncHandler, SyncPull, SyncPush, SyncResponse};
pub use merkle::SyncState;

// Strategy trait and implementations
pub use strategy::{SyncError, SyncResult, SyncStrategy};

pub use memberlist_strategy::MemberlistSyncStrategy;
pub use noop_strategy::NoOpSyncStrategy;
pub use plumtree_strategy::{PlumtreeSyncStrategy, PlumtreeSyncStrategyBuilder};
