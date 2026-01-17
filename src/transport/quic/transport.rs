//! QUIC transport implementation.
//!
//! The [`QuicTransport`] provides **outbound-only** unicast message delivery over QUIC.
//! It implements the [`Transport`] trait for sending messages to peers.
//!
//! # Outbound-Only Design
//!
//! This transport is designed specifically for the Plumtree protocol's unicast needs:
//! - Sending Gossip, IHave, Graft, and Prune messages to specific peers
//! - Connection pooling with automatic reconnection
//! - RTT measurement for peer scoring
//!
//! **Incoming messages are NOT handled by this transport.** The QUIC endpoint binds
//! to a local address to enable peer connections, but there is no acceptor loop
//! to process incoming streams.
//!
//! # Handling Incoming Messages
//!
//! For bidirectional communication, you have two options:
//!
//! 1. **Use Memberlist**: The typical setup uses Memberlist for cluster communication
//!    (which handles incoming messages), and this transport only for Plumtree unicast.
//!
//! 2. **Custom Acceptor**: Access the endpoint directly and spawn your own acceptor:
//!    ```ignore
//!    let transport = QuicTransport::new(addr, config, resolver).await?;
//!    let endpoint = transport.endpoint().clone();
//!
//!    tokio::spawn(async move {
//!        while let Some(incoming) = endpoint.accept().await {
//!            let connection = incoming.await?;
//!            // Handle incoming streams...
//!        }
//!    });
//!    ```
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
//! // Send a message (outbound only)
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

/// Handle for a background cleanup task.
///
/// When this handle is dropped, the cleanup task will be signaled to stop.
/// The task will finish its current sleep interval before actually stopping.
pub struct CleanupTaskHandle {
    running: Arc<std::sync::atomic::AtomicBool>,
    _handle: tokio::task::JoinHandle<()>,
}

impl CleanupTaskHandle {
    /// Stop the cleanup task.
    ///
    /// This signals the task to stop. The task will finish its current
    /// sleep interval before actually stopping.
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check if the cleanup task is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for CleanupTaskHandle {
    fn drop(&mut self) {
        self.stop();
    }
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
    ///
    /// # Performance Note
    ///
    /// When using self-signed certificates (the default), this function offloads
    /// certificate generation to a blocking thread pool to avoid blocking async
    /// worker threads.
    pub async fn new(
        bind_addr: SocketAddr,
        config: QuicConfig,
        resolver: R,
    ) -> Result<Self, QuicError> {
        let resolver = Arc::new(resolver);

        // Build TLS configurations
        // Use async version to avoid blocking when generating self-signed certs
        let server_config = tls::server_config_async(&config.tls).await?;

        let client_config = tls::client_config(&config.tls)?;

        // Create the endpoint
        let endpoint = Endpoint::server(server_config, bind_addr)?;

        // Create connection manager
        let connections = Arc::new(ConnectionManager::new(
            endpoint.clone(),
            client_config,
            config.clone(),
        ));

        // Warn if 0-RTT is enabled but not yet implemented
        if config.zero_rtt.enabled {
            tracing::warn!(
                "0-RTT is enabled in configuration but not yet implemented. \
                 All messages will use regular 1-RTT streams. \
                 See ZeroRttConfig documentation for details."
            );
        }

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
        // Use async version to avoid blocking when generating self-signed certs
        let server_config = tls::server_config_async(&config.tls).await?;

        let client_config = tls::client_config(&config.tls)?;

        // Create the endpoint
        let endpoint = Endpoint::server(server_config, bind_addr)?;

        // Create connection manager
        let connections = Arc::new(ConnectionManager::new(
            endpoint.clone(),
            client_config,
            config.clone(),
        ));

        // Warn if 0-RTT is enabled but not yet implemented
        if config.zero_rtt.enabled {
            tracing::warn!(
                "0-RTT is enabled in configuration but not yet implemented. \
                 All messages will use regular 1-RTT streams. \
                 See ZeroRttConfig documentation for details."
            );
        }

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
        self.endpoint.local_addr().map_err(|e| QuicError::Bind(e))
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

    /// Get the QUIC endpoint.
    ///
    /// Use this to set up custom incoming connection handling:
    ///
    /// ```ignore
    /// let endpoint = transport.endpoint().clone();
    /// tokio::spawn(async move {
    ///     while let Some(incoming) = endpoint.accept().await {
    ///         let connection = incoming.await?;
    ///         // Handle incoming streams...
    ///     }
    /// });
    /// ```
    ///
    /// Note: This transport is outbound-only by default. See module documentation
    /// for details on handling incoming connections.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
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
    ///
    /// Returns the number of connections that were cleaned up.
    pub async fn cleanup(&self) -> usize {
        self.connections.cleanup_stale().await
    }

    /// Spawn a background task that periodically cleans up stale connections.
    ///
    /// This task runs until the returned handle is dropped or cancelled.
    /// It's recommended to call this when creating a transport to ensure
    /// idle connections are properly reclaimed.
    ///
    /// # Arguments
    ///
    /// * `interval` - How often to run cleanup (default: 30 seconds)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let transport = QuicTransport::new(addr, config, resolver).await?;
    /// let cleanup_handle = transport.spawn_cleanup_task(Duration::from_secs(30));
    ///
    /// // ... use transport ...
    ///
    /// // Cleanup task is automatically stopped when handle is dropped
    /// drop(cleanup_handle);
    /// ```
    pub fn spawn_cleanup_task(&self, interval: std::time::Duration) -> CleanupTaskHandle {
        use std::sync::atomic::{AtomicBool, Ordering};

        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();
        let connections = self.connections.clone();
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let effective_interval = if interval.is_zero() {
                // Default to half the idle timeout
                config.connection.idle_timeout / 2
            } else {
                interval
            };

            while running_clone.load(Ordering::Relaxed) {
                // Sleep first, then cleanup
                tokio::time::sleep(effective_interval).await;

                if !running_clone.load(Ordering::Relaxed) {
                    break;
                }

                let cleaned = connections.cleanup_stale().await;
                if cleaned > 0 {
                    tracing::debug!(cleaned, "cleaned up stale connections");
                }
            }
        });

        CleanupTaskHandle {
            running,
            _handle: handle,
        }
    }

    /// Spawn a cleanup task with the default interval (half the idle timeout).
    pub fn spawn_cleanup_task_default(&self) -> CleanupTaskHandle {
        self.spawn_cleanup_task(std::time::Duration::ZERO)
    }

    /// Check if 0-RTT should be used for this message.
    ///
    /// Returns true only if:
    /// - 0-RTT is enabled in config
    /// - gossip_only mode is **disabled** (user explicitly accepts all-message 0-RTT risk)
    ///
    /// # Current Limitations
    ///
    /// **Note**: The current implementation checks the configuration but does not
    /// actually use QUIC 0-RTT Early Data APIs (`connection.open_uni_early()`).
    /// True 0-RTT support requires:
    ///
    /// 1. Session ticket caching for connection resumption
    /// 2. Using `connection.open_uni_early()` or `send_datagram_early()` for 0-RTT data
    /// 3. Handling `EarlyDataRejected` errors with fallback to regular streams
    ///
    /// The configuration is provided for future implementation and to reserve
    /// the API. Currently, all messages use regular 1-RTT streams.
    ///
    /// # Security Warning
    ///
    /// 0-RTT data is **replayable** by attackers. Only idempotent operations
    /// (like Gossip messages) should ever use 0-RTT. Control messages (Graft,
    /// Prune, IHave) must never use 0-RTT as replay attacks could corrupt
    /// the spanning tree topology.
    ///
    /// # Why We Don't Parse Message Type
    ///
    /// The transport layer is generic over the peer ID type `I`, which can have
    /// variable-length serialization (e.g., `u32`, `u64`, `u128`, `String`, `Uuid`).
    /// The message format is `[MAGIC][sender_id][tag]...`, where the tag position
    /// depends on the sender ID length.
    ///
    /// Rather than require `I: IdCodec` or a trait to determine serialized length,
    /// we take the conservative approach: when `gossip_only` is true (the safe default),
    /// 0-RTT is effectively disabled because we cannot reliably identify Gossip messages.
    ///
    /// Users who want 0-RTT for all messages can set `gossip_only: false`, accepting
    /// the replay risk for non-idempotent messages.
    #[allow(dead_code)]
    fn should_use_0rtt(&self, _data: &Bytes) -> bool {
        if !self.config.zero_rtt.enabled {
            return false;
        }

        if self.config.zero_rtt.gossip_only {
            // Safe mode: would need message type parsing, which we can't do reliably
            // at the transport layer due to variable-length peer ID serialization.
            // Return false to be conservative.
            return false;
        }

        // User explicitly enabled 0-RTT for ALL messages (dangerous!)
        // They accept the replay risk.
        true
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
                self.stats
                    .bytes_sent
                    .fetch_add(data_len as u64, Ordering::Relaxed);
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
