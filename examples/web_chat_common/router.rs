//! Shared router builder for web chat examples.

#![allow(dead_code)]

use axum::{routing::get, Router};
use std::path::Path;
use tower_http::services::ServeDir;

use super::handlers::*;
use super::state::AppState;
use super::traits::{ChatDelegate, ChatNode};

/// Build the router with all routes.
///
/// Both web_chat.rs and web_chat_quic.rs use this same router,
/// ensuring identical API behavior.
pub fn build_router<N, D>(state: AppState<N, D>, static_dir: &Path) -> Router
where
    N: ChatNode,
    D: ChatDelegate,
{
    Router::new()
        // Core API
        .route("/api/config", get(api_config::<N, D>))
        .route("/api/status", get(api_status::<N, D>))
        .route("/api/node/:id", get(api_node::<N, D>))
        .route("/api/node/:id/peers", get(api_node_peers::<N, D>))
        // Metrics
        .route("/metrics", get(api_prometheus_metrics::<N, D>))
        .route("/api/metrics", get(api_metrics_json::<N, D>))
        // Chaos engineering
        .route("/api/chaos", get(api_chaos_status::<N, D>))
        .route(
            "/api/chaos/split-brain",
            axum::routing::post(api_chaos_split_brain::<N, D>),
        )
        .route(
            "/api/chaos/heal",
            axum::routing::post(api_chaos_heal::<N, D>),
        )
        .route(
            "/api/chaos/isolate/:node",
            axum::routing::post(api_chaos_isolate::<N, D>),
        )
        .route(
            "/api/chaos/loss",
            axum::routing::post(api_chaos_loss::<N, D>),
        )
        .route(
            "/api/chaos/latency",
            axum::routing::post(api_chaos_latency::<N, D>),
        )
        .route(
            "/api/chaos/clock-skew",
            axum::routing::post(api_chaos_clock_skew::<N, D>),
        )
        .route(
            "/api/chaos/random-clock-skew",
            axum::routing::post(api_chaos_random_clock_skew::<N, D>),
        )
        .route(
            "/api/chaos/disable",
            axum::routing::post(api_chaos_disable::<N, D>),
        )
        // WebSocket
        .route("/ws", get(ws_handler::<N, D>))
        // Static files
        .nest_service("/", ServeDir::new(static_dir))
        .with_state(state)
}
