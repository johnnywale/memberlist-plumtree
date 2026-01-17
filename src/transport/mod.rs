//! Transport abstraction for Plumtree message delivery.
//!
//! This module provides the `Transport` trait that must be implemented
//! to enable proper unicast message delivery for Plumtree protocol messages.
//!
//! # Important
//!
//! Plumtree requires **unicast** (point-to-point) message delivery for correct operation:
//! - **Gossip**: Must be sent to specific eager peers
//! - **IHave**: Must be sent to specific lazy peers
//! - **Graft**: Must be sent to the peer that announced the message
//! - **Prune**: Must be sent to the peer being demoted
//!
//! Using broadcast for these messages will break the protocol.
//!
//! # Available Transports
//!
//! - [`ChannelTransport`]: Channel-based transport for testing
//! - [`NoopTransport`]: No-op transport that discards messages
//! - [`QuicTransport`](quic::QuicTransport): QUIC-based transport (requires `quic` feature)

use bytes::Bytes;
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;

#[cfg(feature = "quic")]
pub mod quic;

/// Transport trait for sending Plumtree messages.
///
/// Implementations must provide unicast message delivery to specific peers.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::Transport;
///
/// struct MyTransport {
///     memberlist: Memberlist,
/// }
///
/// impl<I: Clone + Send + Sync> Transport<I> for MyTransport {
///     async fn send_to(&self, target: &I, data: Bytes) -> Result<(), TransportError> {
///         // Send via memberlist's reliable/best-effort unicast
///         self.memberlist.send_reliable(target, data).await
///     }
/// }
/// ```
#[auto_impl::auto_impl(Box, Arc)]
pub trait Transport<I>: Send + Sync + 'static
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    /// Error type for transport operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Send a message to a specific peer (unicast).
    ///
    /// This MUST deliver the message to the specified target peer only.
    /// Broadcasting to multiple peers or random nodes will break Plumtree.
    fn send_to(
        &self,
        target: &I,
        data: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A simple channel-based transport that outputs (target, data) pairs.
///
/// Useful for testing or when you want to handle delivery externally.
#[derive(Debug, Clone)]
pub struct ChannelTransport<I> {
    tx: async_channel::Sender<(I, Bytes)>,
}

impl<I> ChannelTransport<I> {
    /// Create a new channel transport.
    pub fn new(tx: async_channel::Sender<(I, Bytes)>) -> Self {
        Self { tx }
    }

    /// Create a channel transport with a new bounded channel.
    ///
    /// Returns the transport and the receiver for (target, data) pairs.
    pub fn bounded(capacity: usize) -> (Self, async_channel::Receiver<(I, Bytes)>) {
        let (tx, rx) = async_channel::bounded(capacity);
        (Self { tx }, rx)
    }
}

/// Error type for channel transport.
#[derive(Debug, Clone)]
pub struct ChannelTransportError(pub String);

impl std::fmt::Display for ChannelTransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "channel transport error: {}", self.0)
    }
}

impl std::error::Error for ChannelTransportError {}

impl<I> Transport<I> for ChannelTransport<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Error = ChannelTransportError;

    async fn send_to(&self, target: &I, data: Bytes) -> Result<(), Self::Error> {
        self.tx
            .send((target.clone(), data))
            .await
            .map_err(|e| ChannelTransportError(e.to_string()))
    }
}

/// A no-op transport that discards all messages.
///
/// Useful for testing scenarios where message delivery doesn't matter.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTransport;

impl<I> Transport<I> for NoopTransport
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Error = std::convert::Infallible;

    async fn send_to(&self, _target: &I, _data: Bytes) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_channel_transport() {
        let (transport, rx) = ChannelTransport::<u64>::bounded(16);

        transport
            .send_to(&42u64, Bytes::from("hello"))
            .await
            .unwrap();

        let (target, data) = rx.recv().await.unwrap();
        assert_eq!(target, 42);
        assert_eq!(data, Bytes::from("hello"));
    }

    #[tokio::test]
    async fn test_noop_transport() {
        let transport = NoopTransport;
        transport
            .send_to(&42u64, Bytes::from("hello"))
            .await
            .unwrap();
    }
}
