//! Application state shared between handlers.

#![allow(dead_code)]

use memberlist_plumtree::testing::EnhancedChaosController;
use metrics_exporter_prometheus::PrometheusHandle;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};

use super::traits::{ChatDelegate, ChatNode, WebChatConfig};

/// Application state generic over node and delegate types.
///
/// This allows the same handlers to work with both MemberlistStack
/// and PlumtreeStack backends.
pub struct AppState<N, D>
where
    N: ChatNode,
    D: ChatDelegate,
{
    /// Channels for sending broadcast requests to each node
    pub broadcast_txs: Arc<Vec<mpsc::Sender<String>>>,
    /// Channel for broadcasting updates to WebSocket clients
    pub update_tx: broadcast::Sender<Value>,
    /// Delegates for each node (hold messages, events, metrics)
    pub delegates: Arc<Vec<Arc<D>>>,
    /// Node instances
    pub nodes: Arc<Vec<Arc<RwLock<N>>>>,
    /// Prometheus metrics handle
    pub prometheus_handle: PrometheusHandle,
    /// Chaos controller for fault injection testing
    pub chaos: Arc<EnhancedChaosController<usize, Vec<u8>>>,
    /// Configuration
    pub config: WebChatConfig,
}

// Manual Clone implementation since N and D don't need to be Clone
// (we only clone the Arc wrappers)
impl<N, D> Clone for AppState<N, D>
where
    N: ChatNode,
    D: ChatDelegate,
{
    fn clone(&self) -> Self {
        Self {
            broadcast_txs: self.broadcast_txs.clone(),
            update_tx: self.update_tx.clone(),
            delegates: self.delegates.clone(),
            nodes: self.nodes.clone(),
            prometheus_handle: self.prometheus_handle.clone(),
            chaos: self.chaos.clone(),
            config: self.config.clone(),
        }
    }
}

impl<N, D> AppState<N, D>
where
    N: ChatNode,
    D: ChatDelegate,
{
    /// Create new application state
    pub fn new(
        broadcast_txs: Vec<mpsc::Sender<String>>,
        update_tx: broadcast::Sender<Value>,
        delegates: Vec<Arc<D>>,
        nodes: Vec<Arc<RwLock<N>>>,
        prometheus_handle: PrometheusHandle,
        config: WebChatConfig,
    ) -> Self {
        Self {
            broadcast_txs: Arc::new(broadcast_txs),
            update_tx,
            delegates: Arc::new(delegates),
            nodes: Arc::new(nodes),
            prometheus_handle,
            chaos: Arc::new(EnhancedChaosController::new()),
            config,
        }
    }

    /// Get number of users
    pub fn num_users(&self) -> usize {
        self.config.num_users
    }
}
