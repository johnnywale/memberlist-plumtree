//! Common utilities for PlumtreeStack E2E tests.

#![cfg(all(feature = "quic", feature = "tokio"))]

#[path = "common/mod.rs"]
pub mod common;

use bytes::Bytes;
use common::{allocate_ports, eventually, install_crypto_provider, PortGuard};
use memberlist_plumtree::{
    discovery::{StaticDiscovery, StaticDiscoveryConfig},
    HealthReport, HealthStatus, MessageId, PeerStats, PeerTopology, PlumtreeConfig,
    PlumtreeDelegate, PlumtreeStack, PlumtreeStackConfig, QuicConfig,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// Test delegate that tracks deliveries and protocol events.
#[derive(Default)]
pub struct TestDelegate {
    pub delivered: Mutex<HashMap<MessageId, Bytes>>,
    pub duplicates: Mutex<Vec<MessageId>>,
    pub graft_failed: Mutex<Vec<(MessageId, u64)>>,
}

impl TestDelegate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn delivery_count(&self) -> usize {
        self.delivered.lock().len()
    }

    pub fn duplicate_count(&self) -> usize {
        self.duplicates.lock().len()
    }

    pub fn has_message(&self, msg_id: &MessageId) -> bool {
        self.delivered.lock().contains_key(msg_id)
    }

    #[allow(dead_code)]
    pub fn get_payload(&self, msg_id: &MessageId) -> Option<Bytes> {
        self.delivered.lock().get(msg_id).cloned()
    }
}

impl PlumtreeDelegate<u64> for TestDelegate {
    fn on_deliver(&self, message_id: MessageId, payload: Bytes) {
        use std::collections::hash_map::Entry;
        let mut delivered = self.delivered.lock();
        match delivered.entry(message_id) {
            Entry::Occupied(_) => {
                drop(delivered);
                self.duplicates.lock().push(message_id);
            }
            Entry::Vacant(e) => {
                e.insert(payload);
            }
        }
    }

    fn on_graft_failed(&self, message_id: &MessageId, peer: &u64) {
        self.graft_failed.lock().push((*message_id, *peer));
    }
}

/// A test cluster node.
pub struct TestNode {
    pub stack: PlumtreeStack<u64, Arc<TestDelegate>>,
    pub delegate: Arc<TestDelegate>,
    pub node_id: u64,
    pub addr: SocketAddr,
}

impl TestNode {
    /// Get peer stats.
    pub fn peer_stats(&self) -> PeerStats {
        self.stack.peer_stats()
    }

    /// Get peer topology (eager/lazy sets).
    #[allow(dead_code)]
    pub fn topology(&self) -> PeerTopology<u64> {
        self.stack.plumtree().peers().topology()
    }

    /// Get health report.
    pub fn health(&self) -> HealthReport {
        self.stack.plumtree().plumtree().health()
    }

    /// Check if healthy.
    pub fn is_healthy(&self) -> bool {
        self.health().status == HealthStatus::Healthy
    }

    /// Broadcast a message.
    pub async fn broadcast(
        &self,
        payload: impl Into<Bytes>,
    ) -> memberlist_plumtree::Result<MessageId> {
        self.stack.broadcast(payload).await
    }
}

/// A test cluster with multiple nodes.
pub struct TestCluster {
    pub nodes: Vec<TestNode>,
    _port_guard: PortGuard,
}

impl TestCluster {
    /// Create a new cluster with N nodes.
    pub async fn new(num_nodes: usize, config: PlumtreeConfig) -> Self {
        install_crypto_provider();

        let ports = allocate_ports(num_nodes);
        let port_guard = PortGuard::new(ports.clone());

        let addrs: Vec<SocketAddr> = ports
            .iter()
            .map(|p| format!("127.0.0.1:{}", p).parse().unwrap())
            .collect();

        let mut nodes = Vec::with_capacity(num_nodes);

        for i in 0..num_nodes {
            let node_id = i as u64;
            let bind_addr = addrs[i];

            // Build discovery with all other nodes as seeds
            let mut discovery_config = StaticDiscoveryConfig::new();
            for (j, &addr) in addrs.iter().enumerate() {
                if j != i {
                    discovery_config = discovery_config.with_seed(j as u64, addr);
                }
            }
            let discovery = StaticDiscovery::new(discovery_config).with_local_addr(bind_addr);

            let delegate = TestDelegate::new();

            let stack_config = PlumtreeStackConfig::new(node_id, bind_addr)
                .with_plumtree(config.clone())
                .with_quic(QuicConfig::insecure_dev())
                .with_discovery(discovery);

            let stack = stack_config
                .build(delegate.clone())
                .await
                .expect("Failed to build stack");

            nodes.push(TestNode {
                stack,
                delegate,
                node_id,
                addr: bind_addr,
            });
        }

        // Wait for connections to establish
        // Use longer timeout to ensure QUIC handshakes complete
        tokio::time::sleep(Duration::from_secs(2)).await;

        Self {
            nodes,
            _port_guard: port_guard,
        }
    }

    /// Get all delegates.
    #[allow(dead_code)]
    pub fn delegates(&self) -> Vec<Arc<TestDelegate>> {
        self.nodes.iter().map(|n| n.delegate.clone()).collect()
    }

    /// Wait for all nodes to receive a message.
    pub async fn wait_for_delivery(&self, msg_id: MessageId, timeout: Duration) -> bool {
        eventually(timeout, || async {
            self.nodes.iter().all(|n| n.delegate.has_message(&msg_id))
        })
        .await
        .is_ok()
    }

    /// Wait for specific nodes to receive a message.
    #[allow(dead_code)]
    pub async fn wait_for_delivery_subset(
        &self,
        msg_id: MessageId,
        node_indices: &[usize],
        timeout: Duration,
    ) -> bool {
        eventually(timeout, || async {
            node_indices
                .iter()
                .all(|&i| self.nodes[i].delegate.has_message(&msg_id))
        })
        .await
        .is_ok()
    }

    /// Check all nodes are healthy.
    #[allow(dead_code)]
    pub fn all_healthy(&self) -> bool {
        self.nodes.iter().all(|n| n.is_healthy())
    }

    /// Shutdown the cluster.
    pub async fn shutdown(self) {
        for node in self.nodes {
            node.stack.shutdown().await;
        }
        // port_guard dropped here, releasing ports
    }

    /// Remove and shutdown a specific node, returning its info.
    pub async fn kill_node(&mut self, index: usize) -> (u64, SocketAddr) {
        let node = self.nodes.remove(index);
        let id = node.node_id;
        let addr = node.addr;
        node.stack.shutdown().await;
        (id, addr)
    }

    /// Restart a node with the same ID but a new address (port).
    ///
    /// This uses a new port to avoid socket TIME_WAIT issues on Windows.
    /// The old address parameter is ignored - use `restart_node_new_port` explicitly.
    #[allow(dead_code)]
    pub async fn restart_node(
        &self,
        node_id: u64,
        _old_addr: SocketAddr,
        config: PlumtreeConfig,
    ) -> TestNode {
        // Allocate a new port to avoid TIME_WAIT issues
        let new_port = common::allocate_port();
        let new_addr: SocketAddr = format!("127.0.0.1:{}", new_port).parse().unwrap();

        // Build discovery with current cluster nodes as seeds
        let mut discovery_config = StaticDiscoveryConfig::new();
        for node in &self.nodes {
            if node.node_id != node_id {
                discovery_config = discovery_config.with_seed(node.node_id, node.addr);
            }
        }
        let discovery = StaticDiscovery::new(discovery_config).with_local_addr(new_addr);

        let delegate = TestDelegate::new();
        let stack_config = PlumtreeStackConfig::new(node_id, new_addr)
            .with_plumtree(config)
            .with_quic(QuicConfig::insecure_dev())
            .with_discovery(discovery);

        let stack = stack_config
            .build(delegate.clone())
            .await
            .expect("Failed to restart node");

        TestNode {
            stack,
            delegate,
            node_id,
            addr: new_addr,
        }
    }

    /// Add a new node to the cluster.
    #[allow(dead_code)]
    pub async fn add_node(&self, node_id: u64, config: PlumtreeConfig) -> TestNode {
        let port = common::allocate_port();
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

        // Build discovery with existing nodes as seeds
        let mut discovery_config = StaticDiscoveryConfig::new();
        for node in &self.nodes {
            discovery_config = discovery_config.with_seed(node.node_id, node.addr);
        }
        let discovery = StaticDiscovery::new(discovery_config).with_local_addr(addr);

        let delegate = TestDelegate::new();
        let stack_config = PlumtreeStackConfig::new(node_id, addr)
            .with_plumtree(config)
            .with_quic(QuicConfig::insecure_dev())
            .with_discovery(discovery);

        let stack = stack_config
            .build(delegate.clone())
            .await
            .expect("Failed to add node");

        TestNode {
            stack,
            delegate,
            node_id,
            addr,
        }
    }
}
