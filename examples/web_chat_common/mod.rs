//! Common utilities shared between web_chat and web_chat_quic examples.
//!
//! This module provides all the shared infrastructure for both examples,
//! ensuring they have identical features and behavior. The only difference
//! between the examples is how they set up the underlying stack.
//!
//! ## Module Structure
//!
//! - `types` - Protocol types (ChatMessage, ProtocolEvent, etc.)
//! - `traits` - ChatNode, ChatDelegate traits for abstraction
//! - `delegate` - Shared delegate implementation
//! - `state` - Generic AppState
//! - `handlers` - HTTP and WebSocket handlers
//! - `router` - Router builder
//! - `server` - Server startup logic
//! - `metrics` - Prometheus parsing utilities
//! - `utils` - Constants and utilities

pub mod delegate;
pub mod handlers;
pub mod metrics;
pub mod router;
pub mod server;
pub mod state;
pub mod traits;
pub mod types;
pub mod utils;

// Re-export commonly used items
pub use types::*;
// Re-export utilities
#[allow(unused_imports)]
pub use metrics::parse_prometheus_metrics;
#[allow(unused_imports)]
pub use utils::{find_static_dir, BASE_PORT, BASE_QUIC_PORT, NUM_USERS};

// Re-export traits and state (used by both web_chat and web_chat_quic)
#[allow(unused_imports)]
pub use state::AppState;
#[allow(unused_imports)]
pub use traits::{ChatDelegate, ChatNode, WebChatConfig};

// Re-export server function (used by both web_chat and web_chat_quic)
#[allow(unused_imports)]
pub use server::{print_startup_banner, run_server};

// Re-export delegate types
#[allow(unused_imports)]
pub use delegate::{ChatPlumtreeDelegate, NodeIdFormatter, PeerTopologyProvider};
