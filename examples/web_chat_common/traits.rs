//! Traits for abstracting over different stack implementations.
//!
//! These traits allow web_chat.rs and web_chat_quic.rs to share
//! all application logic while using different underlying stacks.

#![allow(dead_code)]

use memberlist_plumtree::{Error, MessageId, PeerStats};
use serde_json::Value;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::types::{Metrics, ProtocolEvent, ReceivedMessage, TimestampedEvent};

/// Trait for chat node implementations.
///
/// Both MemberlistStack-based and PlumtreeStack-based nodes implement this trait,
/// allowing shared handlers to work with either backend.
///
/// All async methods return `impl Future + Send` to ensure compatibility with
/// axum's requirement for `Send` futures in WebSocket handlers.
#[allow(dead_code)]
pub trait ChatNode: Send + Sync + 'static {
    /// Start the node and join the cluster
    fn start(&mut self) -> impl Future<Output = Result<(), String>> + Send;

    /// Stop the node and leave the cluster
    fn stop(&mut self) -> impl Future<Output = ()> + Send;

    /// Check if the node is online
    fn is_online(&self) -> bool;

    /// Broadcast a message to all nodes
    fn broadcast(&self, text: &str) -> impl Future<Output = Result<MessageId, Error>> + Send;

    /// Get peer topology as JSON (eager/lazy lists)
    fn get_peers_json(&self) -> impl Future<Output = Value> + Send;

    /// Get peer statistics
    fn peer_stats(&self) -> PeerStats;

    /// Promote all lazy peers to eager
    fn promote_all_to_eager(&self) -> impl Future<Output = ()> + Send;

    /// Demote all eager peers to lazy
    fn demote_all_to_lazy(&self) -> impl Future<Output = ()> + Send;
}

/// Trait for chat delegate implementations.
///
/// Provides access to delegate state (messages, events, metrics) for handlers.
///
/// All async methods return `impl Future + Send` to ensure compatibility with
/// axum's requirement for `Send` futures in WebSocket handlers.
pub trait ChatDelegate: Send + Sync + 'static {
    /// Get reference to received messages
    fn messages(&self) -> &Arc<RwLock<Vec<ReceivedMessage>>>;

    /// Get reference to protocol events
    fn events(&self) -> &Arc<RwLock<Vec<TimestampedEvent>>>;

    /// Get reference to metrics
    fn metrics(&self) -> &Arc<Metrics>;

    /// Get peer topology as JSON
    fn get_peers_json(&self) -> impl Future<Output = Value> + Send;

    /// Add a protocol event
    fn add_event(&self, event: ProtocolEvent) -> impl Future<Output = ()> + Send;
}

/// Configuration for web chat server.
#[derive(Clone)]
pub struct WebChatConfig {
    /// Number of users/nodes
    pub num_users: usize,
    /// Base port for node networking
    pub base_port: u16,
    /// Web server port
    pub web_port: u16,
    /// Transport description (for display)
    pub transport_name: &'static str,
}

impl WebChatConfig {
    pub fn memberlist() -> Self {
        Self {
            num_users: 50,
            base_port: 17000,
            web_port: 3000,
            transport_name: "MemberlistStack with SWIM",
        }
    }

    #[allow(dead_code)]
    pub fn quic() -> Self {
        Self {
            num_users: 20,
            base_port: 18000,
            web_port: 3001,
            transport_name: "PlumtreeStack with QUIC",
        }
    }
}
