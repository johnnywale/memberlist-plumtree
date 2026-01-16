//! Background task runner for Plumtree.
//!
//! Provides utilities for running Plumtree background tasks
//! (IHave scheduler, Graft timer, message processor).
//!
//! # Important: Unicast vs Broadcast
//!
//! Plumtree protocol messages MUST be delivered via unicast (point-to-point)
//! to specific peers. Broadcasting control messages will break the protocol.
//!
//! Use [`PlumtreeRunnerWithTransport`] with a proper [`Transport`] implementation
//! for production deployments.

use bytes::Bytes;
use std::{fmt::Debug, hash::Hash, sync::Arc};

use crate::{
    encode_plumtree_message,
    plumtree::{OutgoingMessage, Plumtree, PlumtreeDelegate, PlumtreeHandle},
    transport::Transport,
};

/// Runs all Plumtree background tasks with proper unicast transport.
///
/// This is the recommended runner for production use. It uses the [`Transport`]
/// trait to send messages to specific peers via unicast.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::{PlumtreeRunnerWithTransport, ChannelTransport};
///
/// let (transport, rx) = ChannelTransport::bounded(1024);
/// let runner = PlumtreeRunnerWithTransport::new(plumtree, handle, transport);
///
/// // Spawn the runner
/// tokio::spawn(runner.run());
///
/// // Handle outgoing messages from rx - send to specific targets
/// while let Ok((target, data)) = rx.recv().await {
///     memberlist.send_to(&target, data).await;
/// }
/// ```
pub struct PlumtreeRunnerWithTransport<I, D, T> {
    plumtree: Plumtree<I, D>,
    handle: PlumtreeHandle<I>,
    transport: Arc<T>,
}

impl<I, D, T> PlumtreeRunnerWithTransport<I, D, T>
where
    I: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
    T: Transport<I>,
{
    /// Create a new runner with the given transport.
    pub fn new(plumtree: Plumtree<I, D>, handle: PlumtreeHandle<I>, transport: T) -> Self {
        Self {
            plumtree,
            handle,
            transport: Arc::new(transport),
        }
    }

    /// Run all background tasks.
    pub async fn run(self) {
        futures::future::join4(
            self.run_ihave_scheduler(),
            self.run_graft_timer(),
            self.run_outgoing_processor(),
            self.run_seen_cleanup(),
        )
        .await;
    }

    /// Run only the IHave scheduler.
    pub async fn run_ihave_scheduler(&self) {
        self.plumtree.run_ihave_scheduler().await;
    }

    /// Run only the Graft timer.
    pub async fn run_graft_timer(&self) {
        self.plumtree.run_graft_timer().await;
    }

    /// Run the seen map cleanup task.
    pub async fn run_seen_cleanup(&self) {
        self.plumtree.run_seen_cleanup().await;
    }

    /// Run the outgoing message processor with proper unicast delivery.
    pub async fn run_outgoing_processor(&self) {
        while let Some(outgoing) = self.handle.next_outgoing().await {
            let OutgoingMessage { target, message } = outgoing;

            // Encode the message
            let encoded = encode_plumtree_message(&message);

            // Send to specific target via unicast
            if let Some(target) = target {
                if let Err(e) = self.transport.send_to(&target, encoded).await {
                    tracing::warn!("failed to send message to {:?}: {}", target, e);
                }
            } else {
                // This should not happen in normal operation - all Plumtree messages
                // after initial broadcast are unicast
                tracing::warn!("received broadcast message in transport runner - this is unexpected");
            }
        }
    }

    /// Get a reference to the Plumtree instance.
    pub fn plumtree(&self) -> &Plumtree<I, D> {
        &self.plumtree
    }

    /// Get a reference to the handle.
    pub fn handle(&self) -> &PlumtreeHandle<I> {
        &self.handle
    }

    /// Shutdown the runner.
    pub fn shutdown(&self) {
        self.plumtree.shutdown();
    }
}

/// Legacy runner that sends all messages to a single broadcast channel.
///
/// **WARNING**: This runner ignores target information and broadcasts all messages.
/// This will break Plumtree protocol correctness. Use [`PlumtreeRunnerWithTransport`]
/// for production deployments.
///
/// This runner is kept for backwards compatibility and testing scenarios where
/// message routing is handled externally.
#[deprecated(
    since = "0.2.0",
    note = "Use PlumtreeRunnerWithTransport with a proper Transport implementation"
)]
pub struct PlumtreeRunner<I, D> {
    plumtree: Plumtree<I, D>,
    handle: PlumtreeHandle<I>,
    outgoing_tx: async_channel::Sender<Bytes>,
}

#[allow(deprecated)]
impl<I, D> PlumtreeRunner<I, D>
where
    I: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    /// Create a new runner for the given Plumtree instance.
    ///
    /// The `outgoing_tx` channel is where encoded messages will be sent
    /// for transmission via memberlist's broadcast mechanism.
    pub fn new(
        plumtree: Plumtree<I, D>,
        handle: PlumtreeHandle<I>,
        outgoing_tx: async_channel::Sender<Bytes>,
    ) -> Self {
        Self {
            plumtree,
            handle,
            outgoing_tx,
        }
    }

    /// Run all background tasks.
    ///
    /// This method runs forever until the Plumtree instance is shut down.
    /// It should be spawned as a background task.
    pub async fn run(self) {
        // Run all tasks concurrently
        futures::future::join4(
            self.run_ihave_scheduler(),
            self.run_graft_timer(),
            self.run_outgoing_processor(),
            self.run_seen_cleanup(),
        )
        .await;
    }

    /// Run only the IHave scheduler.
    pub async fn run_ihave_scheduler(&self) {
        self.plumtree.run_ihave_scheduler().await;
    }

    /// Run only the Graft timer.
    pub async fn run_graft_timer(&self) {
        self.plumtree.run_graft_timer().await;
    }

    /// Run the seen map cleanup task.
    pub async fn run_seen_cleanup(&self) {
        self.plumtree.run_seen_cleanup().await;
    }

    /// Run the outgoing message processor.
    ///
    /// This encodes outgoing Plumtree messages and sends them
    /// to the memberlist broadcast channel.
    pub async fn run_outgoing_processor(&self) {
        while let Some(outgoing) = self.handle.next_outgoing().await {
            let OutgoingMessage { target: _, message } = outgoing;

            // Encode the message with Plumtree magic prefix
            let encoded = encode_plumtree_message(&message);

            // Send to memberlist broadcast queue
            if self.outgoing_tx.send(encoded).await.is_err() {
                // Channel closed, stop processing
                break;
            }
        }
    }

    /// Get a reference to the Plumtree instance.
    pub fn plumtree(&self) -> &Plumtree<I, D> {
        &self.plumtree
    }

    /// Get a reference to the handle.
    pub fn handle(&self) -> &PlumtreeHandle<I> {
        &self.handle
    }

    /// Shutdown the runner.
    pub fn shutdown(&self) {
        self.plumtree.shutdown();
    }
}

/// Builder for creating and configuring a PlumtreeRunner.
#[deprecated(
    since = "0.2.0",
    note = "Use PlumtreeRunnerWithTransport with a proper Transport implementation"
)]
pub struct PlumtreeRunnerBuilder<I, D> {
    plumtree: Option<Plumtree<I, D>>,
    handle: Option<PlumtreeHandle<I>>,
    outgoing_tx: Option<async_channel::Sender<Bytes>>,
}

#[allow(deprecated)]
impl<I, D> Default for PlumtreeRunnerBuilder<I, D>
where
    I: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    fn default() -> Self {
        Self::new()
    }
}

#[allow(deprecated)]
impl<I, D> PlumtreeRunnerBuilder<I, D>
where
    I: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            plumtree: None,
            handle: None,
            outgoing_tx: None,
        }
    }

    /// Set the Plumtree instance and handle.
    pub fn with_plumtree(mut self, plumtree: Plumtree<I, D>, handle: PlumtreeHandle<I>) -> Self {
        self.plumtree = Some(plumtree);
        self.handle = Some(handle);
        self
    }

    /// Set the outgoing message channel.
    pub fn with_outgoing_channel(mut self, tx: async_channel::Sender<Bytes>) -> Self {
        self.outgoing_tx = Some(tx);
        self
    }

    /// Build the runner.
    ///
    /// Returns `None` if required components are missing.
    pub fn build(self) -> Option<PlumtreeRunner<I, D>> {
        Some(PlumtreeRunner {
            plumtree: self.plumtree?,
            handle: self.handle?,
            outgoing_tx: self.outgoing_tx?,
        })
    }
}

/// Simple function to create a Plumtree system with channels.
///
/// Returns:
/// - `Plumtree` instance for broadcasting
/// - `PlumtreeHandle` for message I/O
/// - Sender for outgoing encoded messages (connect to memberlist)
/// - Receiver for outgoing encoded messages (for memberlist delegate)
pub fn create_plumtree_with_channels<I, D>(
    local_id: I,
    config: crate::PlumtreeConfig,
    delegate: D,
) -> (
    Plumtree<I, D>,
    PlumtreeHandle<I>,
    async_channel::Sender<Bytes>,
    async_channel::Receiver<Bytes>,
)
where
    I: Clone + Eq + Hash + Ord + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    let (plumtree, handle) = Plumtree::new(local_id, config, delegate);
    let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);

    (plumtree, handle, outgoing_tx, outgoing_rx)
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::{NoopDelegate, PlumtreeConfig};

    #[test]
    fn test_create_with_channels() {
        let (plumtree, _handle, _tx, _rx) =
            create_plumtree_with_channels(1u64, PlumtreeConfig::default(), NoopDelegate);

        assert_eq!(*plumtree.local_id(), 1u64);
    }

    #[test]
    fn test_builder() {
        let (plumtree, handle) = Plumtree::new(1u64, PlumtreeConfig::default(), NoopDelegate);
        let (tx, _rx) = async_channel::bounded(16);

        let runner = PlumtreeRunnerBuilder::new()
            .with_plumtree(plumtree, handle)
            .with_outgoing_channel(tx)
            .build();

        assert!(runner.is_some());
    }
}
