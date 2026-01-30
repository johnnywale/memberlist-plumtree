//! HTTP and WebSocket handlers shared between examples.

#![allow(dead_code)]

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::header,
    response::IntoResponse,
    Json,
};
use futures::{SinkExt, StreamExt};
use memberlist_plumtree::testing::ChaosConfig as TestChaosConfig;
use serde_json::{json, Value};
use std::time::Duration;

use super::metrics::parse_prometheus_metrics;
use super::state::AppState;
use super::traits::{ChatDelegate, ChatNode};

// ============================================================================
// WebSocket Handlers
// ============================================================================

/// WebSocket upgrade handler
pub async fn ws_handler<N, D>(
    ws: WebSocketUpgrade,
    State(state): State<AppState<N, D>>,
) -> impl IntoResponse
where
    N: ChatNode,
    D: ChatDelegate,
{
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Handle WebSocket connection
async fn handle_socket<N, D>(socket: WebSocket, state: AppState<N, D>)
where
    N: ChatNode,
    D: ChatDelegate,
{
    let (mut sender, mut receiver) = socket.split();
    let mut update_rx = state.update_tx.subscribe();

    // Send initial state for all nodes
    let prometheus_text = state.prometheus_handle.render();
    for (i, delegate) in state.delegates.iter().enumerate() {
        let messages: Vec<_> = delegate
            .messages()
            .read()
            .await
            .iter()
            .map(|m| m.to_json())
            .collect();
        let events: Vec<_> = delegate
            .events()
            .read()
            .await
            .iter()
            .map(|e| e.to_json())
            .collect();
        let peers_json = delegate.get_peers_json().await;
        let online = state.nodes[i].read().await.is_online();

        let init = json!({
            "type": "init",
            "node": i,
            "messages": messages,
            "events": events,
            "metrics": delegate.metrics().to_json_from_prometheus(&prometheus_text),
            "peers": peers_json,
            "online": online
        });
        if sender.send(Message::Text(init.to_string())).await.is_err() {
            return;
        }
    }

    // Spawn task to forward updates to WebSocket
    let send_task = tokio::spawn(async move {
        while let Ok(update) = update_rx.recv().await {
            if sender
                .send(Message::Text(update.to_string()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    // Handle incoming messages
    let state_clone = state.clone();
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Text(text) = msg {
                if let Ok(data) = serde_json::from_str::<Value>(&text) {
                    handle_ws_message(&state_clone, &data).await;
                }
            }
        }
    });

    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }
}

/// Handle incoming WebSocket message
async fn handle_ws_message<N, D>(state: &AppState<N, D>, data: &Value)
where
    N: ChatNode,
    D: ChatDelegate,
{
    let num_users = state.num_users();

    match data["type"].as_str().unwrap_or("") {
        "send" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            let text = data["text"].as_str().unwrap_or("");
            if user < state.broadcast_txs.len() && !text.is_empty() {
                let _ = state.broadcast_txs[user].send(text.to_string()).await;
            }
        }
        "toggle_online" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.nodes.len() {
                let mut node_guard = state.nodes[user].write().await;
                let is_online = node_guard.is_online();
                if is_online {
                    node_guard.stop().await;
                } else if let Err(e) = node_guard.start().await {
                    eprintln!("Failed to start node: {}", e);
                }
                let peers_json = state.delegates[user].get_peers_json().await;
                let _ = state.update_tx.send(json!({
                    "type": "state",
                    "node": user,
                    "online": !is_online,
                    "peers": peers_json
                }));
            }
        }
        "promote_eager" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.nodes.len() {
                let node_guard = state.nodes[user].read().await;
                node_guard.promote_all_to_eager().await;
            }
        }
        "demote_lazy" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.nodes.len() {
                let node_guard = state.nodes[user].read().await;
                node_guard.demote_all_to_lazy().await;
            }
        }
        "reset_metrics" => {
            let user = data["user"].as_u64().unwrap_or(0) as usize;
            if user < state.delegates.len() {
                let prometheus_text = state.prometheus_handle.render();
                let _ = state.update_tx.send(json!({
                    "type": "metrics",
                    "node": user,
                    "metrics": state.delegates[user].metrics().to_json_from_prometheus(&prometheus_text)
                }));
            }
        }
        // Chaos Engineering Commands
        "chaos_split_brain" => {
            let group_a: Vec<usize> = data["group_a"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as usize))
                        .collect()
                })
                .unwrap_or_default();
            let group_b: Vec<usize> = data["group_b"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as usize))
                        .collect()
                })
                .unwrap_or_default();

            if !group_a.is_empty() && !group_b.is_empty() {
                state.chaos.split_brain(group_a.clone(), group_b.clone());
                let _ = state.update_tx.send(json!({
                    "type": "chaos_event",
                    "event": "split_brain",
                    "group_a": group_a,
                    "group_b": group_b,
                    "stats": chaos_stats_json(&state.chaos)
                }));
            }
        }
        "chaos_heal" => {
            state.chaos.heal_split_brain();
            let _ = state.update_tx.send(json!({
                "type": "chaos_event",
                "event": "heal",
                "stats": chaos_stats_json(&state.chaos)
            }));
        }
        "chaos_isolate" => {
            let node = data["node"].as_u64().unwrap_or(0) as usize;
            if node < num_users {
                let others: Vec<usize> = (0..num_users).filter(|&i| i != node).collect();
                state.chaos.partition.isolate(node, others.clone());
                let _ = state.update_tx.send(json!({
                    "type": "chaos_event",
                    "event": "isolate",
                    "node": node,
                    "stats": chaos_stats_json(&state.chaos)
                }));
            }
        }
        "chaos_set_loss" => {
            let rate = data["rate"].as_f64().unwrap_or(0.0);
            let config = TestChaosConfig::new().with_message_loss_rate(rate);
            *state.chaos.config.write() = config;
            let _ = state.update_tx.send(json!({
                "type": "chaos_event",
                "event": "loss_rate",
                "rate": rate,
                "stats": chaos_stats_json(&state.chaos)
            }));
        }
        "chaos_set_latency" => {
            let base_ms = data["base_ms"].as_u64().unwrap_or(0);
            let jitter_ms = data["jitter_ms"].as_u64().unwrap_or(0);
            let config = TestChaosConfig::new()
                .with_latency(Duration::from_millis(base_ms))
                .with_jitter(Duration::from_millis(jitter_ms));
            *state.chaos.config.write() = config;
            let _ = state.update_tx.send(json!({
                "type": "chaos_event",
                "event": "latency",
                "base_ms": base_ms,
                "jitter_ms": jitter_ms,
                "stats": chaos_stats_json(&state.chaos)
            }));
        }
        "chaos_clock_skew" => {
            let node = data["node"].as_u64().unwrap_or(0) as usize;
            let skew_ms = data["skew_ms"].as_i64().unwrap_or(0);
            if node < num_users {
                state.chaos.clock_skew.set_skew_ms(&node, skew_ms);
                let _ = state.update_tx.send(json!({
                    "type": "chaos_event",
                    "event": "clock_skew",
                    "node": node,
                    "skew_ms": skew_ms,
                    "stats": chaos_stats_json(&state.chaos)
                }));
            }
        }
        "chaos_random_clock_skew" => {
            let max_ms = data["max_ms"].as_i64().unwrap_or(100);
            let nodes: Vec<usize> = (0..num_users).collect();
            state.chaos.apply_random_clock_skew(&nodes, max_ms);
            let _ = state.update_tx.send(json!({
                "type": "chaos_event",
                "event": "random_clock_skew",
                "max_ms": max_ms,
                "stats": chaos_stats_json(&state.chaos)
            }));
        }
        "chaos_disable" => {
            state.chaos.disable_all();
            let _ = state.update_tx.send(json!({
                "type": "chaos_event",
                "event": "disabled",
                "stats": chaos_stats_json(&state.chaos)
            }));
        }
        "chaos_stats" => {
            let _ = state.update_tx.send(json!({
                "type": "chaos_stats",
                "stats": chaos_stats_json(&state.chaos)
            }));
        }
        _ => {}
    }
}

// ============================================================================
// REST API Handlers
// ============================================================================

/// GET /api/config - Returns configuration
pub async fn api_config<N, D>(State(state): State<AppState<N, D>>) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    Json(json!({
        "num_users": state.config.num_users,
        "version": "3.0.0",
        "transport": state.config.transport_name
    }))
}

/// GET /api/status - Returns status for all nodes
pub async fn api_status<N, D>(State(state): State<AppState<N, D>>) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let mut nodes = Vec::new();
    let prometheus_text = state.prometheus_handle.render();

    for (i, delegate) in state.delegates.iter().enumerate() {
        let peers_json = delegate.get_peers_json().await;
        let metrics = delegate.metrics().to_json_from_prometheus(&prometheus_text);
        let online = state.nodes[i].read().await.is_online();

        nodes.push(json!({
            "id": i,
            "name": format!("U{}", i + 1),
            "online": online,
            "peers": peers_json,
            "metrics": metrics
        }));
    }

    Json(json!({
        "num_users": state.config.num_users,
        "nodes": nodes
    }))
}

/// GET /api/node/:id - Returns detailed state for a specific node
pub async fn api_node<N, D>(
    Path(node_id): Path<usize>,
    State(state): State<AppState<N, D>>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    if node_id >= state.delegates.len() {
        return Json(json!({
            "error": "Node not found",
            "node_id": node_id
        }));
    }

    let delegate = &state.delegates[node_id];
    let peers_json = delegate.get_peers_json().await;
    let prometheus_text = state.prometheus_handle.render();
    let metrics = delegate.metrics().to_json_from_prometheus(&prometheus_text);
    let online = state.nodes[node_id].read().await.is_online();

    let messages: Vec<_> = delegate
        .messages()
        .read()
        .await
        .iter()
        .map(|m| m.to_json())
        .collect();

    let events: Vec<_> = delegate
        .events()
        .read()
        .await
        .iter()
        .map(|e| e.to_json())
        .collect();

    Json(json!({
        "id": node_id,
        "name": format!("U{}", node_id + 1),
        "online": online,
        "peers": peers_json,
        "metrics": metrics,
        "messages": messages,
        "events": events
    }))
}

/// GET /api/node/:id/peers - Returns peer tree for a specific node
pub async fn api_node_peers<N, D>(
    Path(node_id): Path<usize>,
    State(state): State<AppState<N, D>>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    if node_id >= state.delegates.len() {
        return Json(json!({
            "error": "Node not found",
            "node_id": node_id
        }));
    }

    let delegate = &state.delegates[node_id];
    let peers_json = delegate.get_peers_json().await;
    let online = state.nodes[node_id].read().await.is_online();

    let mut eager_with_status = Vec::new();
    let mut lazy_with_status = Vec::new();

    if let Some(eager_arr) = peers_json.get("eager").and_then(|v| v.as_array()) {
        for peer_name in eager_arr {
            if let Some(name) = peer_name.as_str() {
                eager_with_status.push(json!({
                    "name": name,
                    "online": true
                }));
            }
        }
    }

    if let Some(lazy_arr) = peers_json.get("lazy").and_then(|v| v.as_array()) {
        for peer_name in lazy_arr {
            if let Some(name) = peer_name.as_str() {
                lazy_with_status.push(json!({
                    "name": name,
                    "online": true
                }));
            }
        }
    }

    Json(json!({
        "id": node_id,
        "name": format!("U{}", node_id + 1),
        "online": online,
        "eager": eager_with_status,
        "lazy": lazy_with_status,
        "eager_count": eager_with_status.len(),
        "lazy_count": lazy_with_status.len()
    }))
}

/// GET /metrics - Prometheus scrape endpoint
pub async fn api_prometheus_metrics<N, D>(State(state): State<AppState<N, D>>) -> impl IntoResponse
where
    N: ChatNode,
    D: ChatDelegate,
{
    let metrics = state.prometheus_handle.render();
    (
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        metrics,
    )
}

/// GET /api/metrics - JSON metrics for Web UI
pub async fn api_metrics_json<N, D>(State(state): State<AppState<N, D>>) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let prometheus_text = state.prometheus_handle.render();
    let metrics = parse_prometheus_metrics(&prometheus_text);
    Json(metrics)
}

// ============================================================================
// Chaos Engineering REST API
// ============================================================================

/// GET /api/chaos - Get chaos status
pub async fn api_chaos_status<N, D>(State(state): State<AppState<N, D>>) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    Json(chaos_stats_json(&state.chaos))
}

/// POST /api/chaos/split-brain - Create split-brain partition
pub async fn api_chaos_split_brain<N, D>(
    State(state): State<AppState<N, D>>,
    Json(body): Json<Value>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let group_a: Vec<usize> = body["group_a"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();
    let group_b: Vec<usize> = body["group_b"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();

    if group_a.is_empty() || group_b.is_empty() {
        return Json(json!({ "error": "Both group_a and group_b must be non-empty arrays" }));
    }

    state.chaos.split_brain(group_a.clone(), group_b.clone());

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "split_brain",
        "group_a": group_a,
        "group_b": group_b
    }));

    Json(json!({
        "status": "ok",
        "event": "split_brain",
        "group_a": group_a,
        "group_b": group_b,
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/heal - Heal all partitions
pub async fn api_chaos_heal<N, D>(State(state): State<AppState<N, D>>) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    state.chaos.heal_split_brain();

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "heal"
    }));

    Json(json!({
        "status": "ok",
        "event": "heal",
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/isolate/:node - Isolate a node
pub async fn api_chaos_isolate<N, D>(
    Path(node): Path<usize>,
    State(state): State<AppState<N, D>>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let num_users = state.num_users();
    if node >= num_users {
        return Json(json!({ "error": "Invalid node index" }));
    }

    let others: Vec<usize> = (0..num_users).filter(|&i| i != node).collect();
    state.chaos.partition.isolate(node, others);

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "isolate",
        "node": node
    }));

    Json(json!({
        "status": "ok",
        "event": "isolate",
        "node": node,
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/loss - Set message loss rate
pub async fn api_chaos_loss<N, D>(
    State(state): State<AppState<N, D>>,
    Json(body): Json<Value>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let rate = body["rate"].as_f64().unwrap_or(0.0).clamp(0.0, 1.0);

    let config = TestChaosConfig::new().with_message_loss_rate(rate);
    *state.chaos.config.write() = config;

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "loss_rate",
        "rate": rate
    }));

    Json(json!({
        "status": "ok",
        "event": "loss_rate",
        "rate": rate,
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/latency - Set latency injection
pub async fn api_chaos_latency<N, D>(
    State(state): State<AppState<N, D>>,
    Json(body): Json<Value>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let base_ms = body["base_ms"].as_u64().unwrap_or(0);
    let jitter_ms = body["jitter_ms"].as_u64().unwrap_or(0);

    let config = TestChaosConfig::new()
        .with_latency(Duration::from_millis(base_ms))
        .with_jitter(Duration::from_millis(jitter_ms));
    *state.chaos.config.write() = config;

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "latency",
        "base_ms": base_ms,
        "jitter_ms": jitter_ms
    }));

    Json(json!({
        "status": "ok",
        "event": "latency",
        "base_ms": base_ms,
        "jitter_ms": jitter_ms,
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/clock-skew - Set clock skew for a node
pub async fn api_chaos_clock_skew<N, D>(
    State(state): State<AppState<N, D>>,
    Json(body): Json<Value>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let node = body["node"].as_u64().unwrap_or(0) as usize;
    let skew_ms = body["skew_ms"].as_i64().unwrap_or(0);

    if node >= state.num_users() {
        return Json(json!({ "error": "Invalid node index" }));
    }

    state.chaos.clock_skew.set_skew_ms(&node, skew_ms);

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "clock_skew",
        "node": node,
        "skew_ms": skew_ms
    }));

    Json(json!({
        "status": "ok",
        "event": "clock_skew",
        "node": node,
        "skew_ms": skew_ms,
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/random-clock-skew - Apply random clock skew to all nodes
pub async fn api_chaos_random_clock_skew<N, D>(
    State(state): State<AppState<N, D>>,
    Json(body): Json<Value>,
) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    let max_ms = body["max_ms"].as_i64().unwrap_or(100);

    let nodes: Vec<usize> = (0..state.num_users()).collect();
    state.chaos.apply_random_clock_skew(&nodes, max_ms);

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "random_clock_skew",
        "max_ms": max_ms
    }));

    Json(json!({
        "status": "ok",
        "event": "random_clock_skew",
        "max_ms": max_ms,
        "stats": chaos_stats_json(&state.chaos)
    }))
}

/// POST /api/chaos/disable - Disable all chaos effects
pub async fn api_chaos_disable<N, D>(State(state): State<AppState<N, D>>) -> Json<Value>
where
    N: ChatNode,
    D: ChatDelegate,
{
    state.chaos.disable_all();

    let _ = state.update_tx.send(json!({
        "type": "chaos_event",
        "event": "disabled"
    }));

    Json(json!({
        "status": "ok",
        "event": "disabled",
        "stats": chaos_stats_json(&state.chaos)
    }))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Convert chaos stats to JSON
fn chaos_stats_json(
    chaos: &memberlist_plumtree::testing::EnhancedChaosController<usize, Vec<u8>>,
) -> Value {
    let stats = chaos.stats();
    let config = chaos.config.read();
    json!({
        "messages_total": stats.base.messages_total,
        "messages_dropped": stats.base.messages_dropped,
        "messages_partitioned": stats.base.messages_partitioned,
        "messages_delayed": stats.base.messages_delayed,
        "messages_reordered": stats.messages_reordered,
        "clock_skew_events": stats.clock_skew_events,
        "split_brain_events": stats.split_brain_events,
        "delivery_rate": stats.base.delivery_rate(),
        "config": {
            "enabled": config.enabled,
            "loss_rate": config.message_loss_rate,
            "base_latency_ms": config.base_latency.as_millis(),
            "jitter_ms": config.latency_jitter.as_millis()
        }
    })
}
