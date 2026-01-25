//! Anti-entropy synchronization module.
//!
//! This module provides anti-entropy sync for Plumtree, enabling recovery
//! of missed messages after network partitions or node restarts.
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
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::sync::{SyncState, SyncHandler};
//! use memberlist_plumtree::storage::MemoryStore;
//!
//! // Create storage and sync handler
//! let store = Arc::new(MemoryStore::new(10_000));
//! let handler = SyncHandler::new(store);
//!
//! // Record messages as they arrive
//! handler.record_message(msg_id, payload.as_ref());
//!
//! // Handle sync request from peer
//! let response = handler.handle_sync_request(
//!     peer_root_hash,
//!     (time_start, time_end),
//! ).await;
//! ```

mod merkle;
mod handler;

pub use merkle::SyncState;
pub use handler::{SyncHandler, SyncResponse, SyncPull, SyncPush};
