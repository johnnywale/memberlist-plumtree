//! Background task runner for Plumtree.
//!
//! Provides utilities for running Plumtree background tasks
//! (IHave scheduler, Graft timer, message processor).

use bytes::Bytes;
use std::{fmt::Debug, hash::Hash};

use crate::{
    encode_plumtree_envelope, IdCodec,
    plumtree::{OutgoingMessage, Plumtree, PlumtreeDelegate, PlumtreeHandle},
};

/// Runs all Plumtree background tasks.
///
/// This spawns tasks for:
/// - IHave scheduler (periodic lazy push announcements)
/// - Graft timer (request missing messages)
/// - Outgoing message processor (encode and queue for memberlist)
///
/// Returns a handle that can be used to stop the tasks.
pub struct PlumtreeRunner<I, D> {
    local_id: I,
    plumtree: Plumtree<I, D>,
    handle: PlumtreeHandle<I>,
    outgoing_tx: async_channel::Sender<Bytes>,
}

impl<I, D> PlumtreeRunner<I, D>
where
    I: IdCodec + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    /// Create a new runner for the given Plumtree instance.
    ///
    /// The `outgoing_tx` channel is where encoded messages will be sent
    /// for transmission via memberlist's broadcast mechanism.
    pub fn new(
        local_id: I,
        plumtree: Plumtree<I, D>,
        handle: PlumtreeHandle<I>,
        outgoing_tx: async_channel::Sender<Bytes>,
    ) -> Self {
        Self {
            local_id,
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
        futures::future::join3(
            self.run_ihave_scheduler(),
            self.run_graft_timer(),
            self.run_outgoing_processor(),
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

    /// Run the outgoing message processor.
    ///
    /// This encodes outgoing Plumtree messages and sends them
    /// to the memberlist broadcast channel.
    pub async fn run_outgoing_processor(&self) {
        while let Some(outgoing) = self.handle.next_outgoing().await {
            let OutgoingMessage { target: _, message } = outgoing;

            // Encode the message with our local ID as sender
            let encoded = encode_plumtree_envelope(&self.local_id, &message);

            // Send to memberlist broadcast queue
            if self.outgoing_tx.send(encoded).await.is_err() {
                // Channel closed, stop processing
                break;
            }
        }
    }

    /// Get the local node ID.
    pub fn local_id(&self) -> &I {
        &self.local_id
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
pub struct PlumtreeRunnerBuilder<I, D> {
    local_id: Option<I>,
    plumtree: Option<Plumtree<I, D>>,
    handle: Option<PlumtreeHandle<I>>,
    outgoing_tx: Option<async_channel::Sender<Bytes>>,
}

impl<I, D> Default for PlumtreeRunnerBuilder<I, D>
where
    I: IdCodec + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<I, D> PlumtreeRunnerBuilder<I, D>
where
    I: IdCodec + Clone + Eq + Hash + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            local_id: None,
            plumtree: None,
            handle: None,
            outgoing_tx: None,
        }
    }

    /// Set the local node ID.
    pub fn with_local_id(mut self, local_id: I) -> Self {
        self.local_id = Some(local_id);
        self
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
            local_id: self.local_id?,
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
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    D: PlumtreeDelegate<I>,
{
    let (plumtree, handle) = Plumtree::new(local_id, config, delegate);
    let (outgoing_tx, outgoing_rx) = async_channel::bounded(1024);

    (plumtree, handle, outgoing_tx, outgoing_rx)
}

#[cfg(test)]
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
            .with_local_id(1u64)
            .with_plumtree(plumtree, handle)
            .with_outgoing_channel(tx)
            .build();

        assert!(runner.is_some());
        let runner = runner.unwrap();
        assert_eq!(*runner.local_id(), 1u64);
    }

    #[test]
    fn test_builder_missing_local_id() {
        let (plumtree, handle) = Plumtree::new(1u64, PlumtreeConfig::default(), NoopDelegate);
        let (tx, _rx) = async_channel::bounded(16);

        let runner: Option<PlumtreeRunner<u64, NoopDelegate>> = PlumtreeRunnerBuilder::new()
            .with_plumtree(plumtree, handle)
            .with_outgoing_channel(tx)
            .build();

        assert!(runner.is_none());
    }
}
