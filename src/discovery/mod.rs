//! Pluggable peer discovery for Plumtree.
//!
//! This module provides discovery mechanisms for finding and tracking peers.
//! It enables both standalone deployment (QUIC-only) and memberlist integration.
//!
//! # Discovery Types
//!
//! - [`NoOpDiscovery`]: No automatic discovery, peers are managed manually
//! - [`StaticDiscovery`]: Discover peers from a static list of addresses
//! - [`MemberlistDiscovery`]: SWIM-based discovery using memberlist (requires `memberlist` feature)
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::discovery::{
//!     ClusterDiscovery, StaticDiscovery, StaticDiscoveryConfig,
//! };
//! use std::net::SocketAddr;
//!
//! // Using static discovery with known peer addresses
//! let config = StaticDiscoveryConfig::new()
//!     .with_seed(1u64, "192.168.1.10:9000".parse().unwrap())
//!     .with_seed(2u64, "192.168.1.11:9000".parse().unwrap())
//!     .with_probe_interval(Duration::from_secs(30));
//!
//! let discovery = StaticDiscovery::new(config);
//!
//! // Start discovery and receive events
//! let events = discovery.start().await;
//! while let Ok(event) = events.recv().await {
//!     match event {
//!         DiscoveryEvent::PeerDiscovered { id, addr } => {
//!             println!("Found peer {} at {}", id, addr);
//!         }
//!         DiscoveryEvent::PeerLost { id } => {
//!             println!("Lost peer {}", id);
//!         }
//!     }
//! }
//! ```

#[cfg(feature = "memberlist")]
mod memberlist;
mod noop;
mod r#static;
mod traits;

#[cfg(feature = "memberlist")]
pub use memberlist::{MemberlistDiscovery, MemberlistDiscoveryConfig, MemberlistDiscoveryHandle};
pub use noop::NoOpDiscovery;
pub use r#static::{StaticDiscovery, StaticDiscoveryConfig};
pub use traits::{ClusterDiscovery, DiscoveryEvent, DiscoveryHandle, SimpleDiscoveryHandle};

// Re-export memberlist delegate types for convenience (requires memberlist feature)
#[cfg(feature = "memberlist")]
pub use memberlist::{
    // Memberlist delegate traits
    AliveDelegate, ConflictDelegate, Delegate, EventDelegate, MergeDelegate, NodeDelegate,
    PingDelegate, VoidDelegate,
    // Memberlist types
    Memberlist, Meta, NodeState,
    // nodecraft types
    CheapClone, Id,
    // Plumtree-specific types
    DemotionCallback, PlumtreeNodeDelegate, PromotionCallback,
    // Bridge integration types
    BridgeEventDelegate, MemberlistStack, MemberlistStackError, PlumtreeStackBuilder,
};

// Re-export nodecraft types unconditionally (needed for IdCodec)
#[cfg(not(feature = "memberlist"))]
pub use nodecraft::{CheapClone, Id};
