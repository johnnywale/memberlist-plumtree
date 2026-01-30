//! Common utility functions for web chat examples.

use std::path::PathBuf;

/// Number of simulated users in the demo (used by web_chat.rs).
/// web_chat_quic.rs uses 20 nodes due to QUIC networking overhead.
#[allow(dead_code)]
pub const NUM_USERS: usize = 50;

/// Base port for memberlist (each node uses BASE_PORT + node_idx).
/// Used by web_chat.rs for memberlist/SWIM networking.
#[allow(dead_code)]
pub const BASE_PORT: u16 = 17000;

/// Base port for QUIC transport (each node uses BASE_QUIC_PORT + node_idx).
/// Used by web_chat_quic.rs for QUIC networking.
#[allow(dead_code)]
pub const BASE_QUIC_PORT: u16 = 18000;

/// Find the static directory for serving web assets.
///
/// Searches in order:
/// 1. `examples/static/`
/// 2. `./static/`
/// 3. `../examples/static/`
/// 4. Falls back to `examples/static/`
pub fn find_static_dir() -> PathBuf {
    let candidates = [
        PathBuf::from("examples/static"),
        PathBuf::from("./static"),
        PathBuf::from("../examples/static"),
    ];

    for path in &candidates {
        if path.exists() && path.join("index.html").exists() {
            return path.clone();
        }
    }

    // Default fallback
    PathBuf::from("examples/static")
}
