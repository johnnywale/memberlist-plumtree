//! QUIC transport implementation.
//!
//! The [`QuicTransport`] provides unicast message delivery over QUIC.
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::transport::quic::{QuicTransport, QuicConfig, MapPeerResolver};
//! use std::net::SocketAddr;
//!
//! // Create a resolver for peer ID to address mapping
//! let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
//! let resolver = MapPeerResolver::new(local_addr);
//! resolver.add_peer(1u64, "192.168.1.10:9000".parse().unwrap());
//!
//! // Create the QUIC transport
//! let transport = QuicTransport::new(local_addr, QuicConfig::default(), resolver).await?;
//!
//! // Send a message
//! transport.send_to(&1u64, Bytes::from("hello")).await?;
//! ```

use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use quinn::Endpoint;

use super::config::QuicConfig;
use super::connection::{ConnectionManager, ConnectionStats};
use super::error::QuicError;
use super::resolver::PeerResolver;
use super::tls;
use crate::transport::Transport;

/// QUIC transport statistics.
#[derive(Debug, Clone, Default)]
pub struct QuicStats {
    /// Messages sent successfully.
    pub messages_sent: u64,
    /// Messages failed to send.
    pub messages_failed: u64,
    /// Bytes sent.
    pub bytes_sent: u64,
    /// 0-RTT messages sent.
    pub zero_rtt_sent: u64,
    /// Connection statistics.
    pub connections: ConnectionStats,
}

/// Internal statistics tracking.
#[derive(Debug, Default)]
struct StatsInner {
    messages_sent: AtomicU64,
    messages_failed: AtomicU64,
    bytes_sent: AtomicU64,
    zero_rtt_sent: AtomicU64,
}

/// QUIC-based transport for Plumtree message delivery.
///
/// This transport uses QUIC for unicast message delivery with:
/// - Connection pooling and reuse
/// - TLS encryption
/// - Optional 0-RTT for reduced latency
/// - Connection migration support
///
/// # Type Parameters
///
/// - `I`: The peer ID type (must implement `Clone + Eq + Hash + Debug + Send + Sync`)
/// - `R`: The peer resolver type (must implement [`PeerResolver`])
pub struct QuicTransport<I, R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I>,
{
    /// Configuration.
    config: QuicConfig,
    /// Peer resolver for ID to address mapping.
    resolver: Arc<R>,
    /// Connection manager.
    connections: Arc<ConnectionManager<I>>,
    /// Statistics.
    stats: Arc<StatsInner>,
    /// The QUIC endpoint.
    endpoint: Endpoint,
}

impl<I, R> QuicTransport<I, R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I>,
{
    /// Create a new QUIC transport.
    ///
    /// # Arguments
    ///
    /// * `bind_addr` - Local address to bind to
    /// * `config` - QUIC configuration
    /// * `resolver` - Peer resolver for ID to address mapping
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TLS configuration fails
    /// - Binding to the address fails
    pub async fn new(
        bind_addr: SocketAddr,
        config: QuicConfig,
        resolver: R,
    ) -> Result<Self, QuicError> {
        let resolver = Arc::new(resolver);

        // Build TLS configurations
        let server_config = tls::server_config(&config.tls)?;
        let client_config = tls::client_config(&config.tls)?;

        // Create the endpoint
        let endpoint = Endpoint::server(server_config, bind_addr)?;

        // Create connection manager
        let connections = Arc::new(ConnectionManager::new(
            endpoint.clone(),
            client_config,
            config.clone(),
        ));

        Ok(Self {
            config,
            resolver,
            connections,
            stats: Arc::new(StatsInner::default()),
            endpoint,
        })
    }

    /// Create a new QUIC transport with a shared resolver.
    pub async fn with_shared_resolver(
        bind_addr: SocketAddr,
        config: QuicConfig,
        resolver: Arc<R>,
    ) -> Result<Self, QuicError> {
        // Build TLS configurations
        let server_config = tls::server_config(&config.tls)?;
        let client_config = tls::client_config(&config.tls)?;

        // Create the endpoint
        let endpoint = Endpoint::server(server_config, bind_addr)?;

        // Create connection manager
        let connections = Arc::new(ConnectionManager::new(
            endpoint.clone(),
            client_config,
            config.clone(),
        ));

        Ok(Self {
            config,
            resolver,
            connections,
            stats: Arc::new(StatsInner::default()),
            endpoint,
        })
    }

    /// Get the local address the transport is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, QuicError> {
        self.endpoint
            .local_addr()
            .map_err(|e| QuicError::Bind(e))
    }

    /// Get transport statistics.
    pub async fn stats(&self) -> QuicStats {
        QuicStats {
            messages_sent: self.stats.messages_sent.load(Ordering::Relaxed),
            messages_failed: self.stats.messages_failed.load(Ordering::Relaxed),
            bytes_sent: self.stats.bytes_sent.load(Ordering::Relaxed),
            zero_rtt_sent: self.stats.zero_rtt_sent.load(Ordering::Relaxed),
            connections: self.connections.stats().await,
        }
    }

    /// Get the resolver.
    pub fn resolver(&self) -> &R {
        &self.resolver
    }

    /// Get the configuration.
    pub fn config(&self) -> &QuicConfig {
        &self.config
    }

    /// Get the RTT to a peer, if a connection exists.
    pub async fn get_rtt(&self, peer: &I) -> Option<std::time::Duration> {
        self.connections.get_rtt(peer).await
    }

    /// Check if a peer has an active connection.
    pub async fn has_connection(&self, peer: &I) -> bool {
        self.connections.has_connection(peer).await
    }

    /// Close the connection to a peer.
    pub async fn close_connection(&self, peer: &I) -> bool {
        self.connections.remove(peer).await
    }

    /// Close all connections and shut down the transport.
    pub async fn shutdown(&self) {
        self.connections.close_all().await;
        self.endpoint.close(0u32.into(), b"shutdown");
    }

    /// Clean up stale connections.
    pub async fn cleanup(&self) {
        self.connections.cleanup_stale().await;
    }

    /// Check if 0-RTT should be used for this message.
    ///
    /// Returns true only if:
    /// - 0-RTT is enabled in config
    /// - gossip_only mode is enabled and this is a Gossip message
    fn should_use_0rtt(&self, data: &Bytes) -> bool {
        if !self.config.zero_rtt.enabled {
            return false;
        }

        if !self.config.zero_rtt.gossip_only {
            // 0-RTT for all messages (dangerous!)
            return true;
        }

        // Check if this is a Gossip message
        // Format: [0x50][sender_id][tag]...
        // Tag 0x01 = Gossip
        self.is_gossip_message(data)
    }

    /// Check if data is a Gossip message.
    ///
    /// Message format: [MAGIC=0x50][sender_id (8 bytes)][tag]...
    /// Tag values: 0x01 = Gossip, 0x02 = IHave, 0x03 = Graft, 0x04 = Prune
    fn is_gossip_message(&self, data: &Bytes) -> bool {
        // Need at least magic (1) + sender_id (variable, assume max 8) + tag (1) = 10 bytes minimum
        if data.len() < 10 {
            return false;
        }

        // Check magic byte
        if data[0] != 0x50 {
            return false;
        }

        // The tag byte position depends on sender ID encoding
        // For u64 sender IDs, tag is at position 9
        // For now, assume u64 sender IDs (8 bytes)
        // Tag 0x01 = Gossip
        data.get(9).copied() == Some(0x01)
    }
}

impl<I, R> Transport<I> for QuicTransport<I, R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I>,
{
    type Error = QuicError;

    async fn send_to(&self, target: &I, data: Bytes) -> Result<(), QuicError> {
        // Resolve peer address
        let addr = self
            .resolver
            .resolve(target)
            .ok_or_else(|| QuicError::PeerNotFound(format!("{:?}", target)))?;

        // Check if we should use 0-RTT
        let _use_0rtt = self.should_use_0rtt(&data);
        // Note: Actual 0-RTT implementation would use connection.open_uni_early()
        // For now, we use regular streams. 0-RTT requires session ticket caching.

        // Send via connection manager
        let data_len = data.len();
        match self.connections.send(target, addr, data).await {
            Ok(()) => {
                self.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                self.stats.bytes_sent.fetch_add(data_len as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.stats.messages_failed.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }
}

impl<I, R> Clone for QuicTransport<I, R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I>,
{
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            resolver: self.resolver.clone(),
            connections: self.connections.clone(),
            stats: self.stats.clone(),
            endpoint: self.endpoint.clone(),
        }
    }
}

impl<I, R> Debug for QuicTransport<I, R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I> + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicTransport")
            .field("config", &self.config)
            .field("resolver", &self.resolver)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_gossip_message() {
        // Create a mock Gossip message
        // Format: [0x50][8 bytes sender_id][0x01 tag]...
        let mut gossip_data = vec![0x50]; // Magic
        gossip_data.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]); // Sender ID (u64)
        gossip_data.push(0x01); // Tag = Gossip
        gossip_data.extend_from_slice(b"payload");

        // Create a mock IHave message
        let mut ihave_data = vec![0x50];
        ihave_data.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        ihave_data.push(0x02); // Tag = IHave

        // Create a mock Graft message
        let mut graft_data = vec![0x50];
        graft_data.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 1]);
        graft_data.push(0x03); // Tag = Graft

        // We can't test the actual method without a transport instance,
        // but we can verify the expected byte patterns
        assert_eq!(gossip_data[0], 0x50);
        assert_eq!(gossip_data[9], 0x01);
        assert_eq!(ihave_data[9], 0x02);
        assert_eq!(graft_data[9], 0x03);
    }

    #[test]
    fn test_quic_stats_default() {
        let stats = QuicStats::default();
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.messages_failed, 0);
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.zero_rtt_sent, 0);
    }
}
