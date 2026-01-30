//! Shared server startup logic for web chat examples.

#![allow(dead_code)]

use std::net::SocketAddr;

use super::router::build_router;
use super::state::AppState;
use super::traits::{ChatDelegate, ChatNode, WebChatConfig};
use super::utils::find_static_dir;

/// Run the web chat server.
///
/// This function is called by both web_chat.rs and web_chat_quic.rs
/// after they have created their nodes and delegates.
pub async fn run_server<N, D>(state: AppState<N, D>)
where
    N: ChatNode,
    D: ChatDelegate,
{
    let config = state.config.clone();

    // Find the static directory
    let static_dir = find_static_dir();
    println!("\nServing static files from: {:?}", static_dir);

    // Build router
    let app = build_router(state, &static_dir);

    // Start server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.web_port));
    println!("\nServer running on http://localhost:{}", config.web_port);
    println!(
        "Prometheus metrics at http://localhost:{}/metrics",
        config.web_port
    );
    println!(
        "JSON metrics API at http://localhost:{}/api/metrics",
        config.web_port
    );
    println!("\nKey features demonstrated:");
    println!("  - {} integration", config.transport_name);
    println!(
        "  - Real networking on localhost (ports {}-{})",
        config.base_port,
        config.base_port + config.num_users as u16 - 1
    );
    println!("  - Online/offline toggle with proper shutdown/restart");
    println!("  - Prometheus metrics");
    println!("\nChaos Engineering (Jepsen-style testing):");
    println!("  GET  /api/chaos                - Get chaos status");
    println!("  POST /api/chaos/split-brain    - Create split-brain partition");
    println!("  POST /api/chaos/heal           - Heal all partitions");
    println!("  POST /api/chaos/isolate/:node  - Isolate a node");
    println!("  POST /api/chaos/loss           - Set message loss rate");
    println!("  POST /api/chaos/latency        - Set latency injection");
    println!("  POST /api/chaos/clock-skew     - Set clock skew for a node");
    println!("  POST /api/chaos/random-clock-skew - Apply random clock skew");
    println!("  POST /api/chaos/disable        - Disable all chaos effects");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// Print startup banner
pub fn print_startup_banner(config: &WebChatConfig) {
    println!("Starting Plumtree Web Chat Demo...");
    println!(
        "Using {} nodes with {} on ports {}-{}",
        config.num_users,
        config.transport_name,
        config.base_port,
        config.base_port + config.num_users as u16 - 1
    );
    println!();
}
