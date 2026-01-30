//! No-op discovery for manual peer management.
//!
//! Use this when:
//! - Running a single-node deployment
//! - Managing peers manually via `add_peer`/`remove_peer`
//! - Using a custom discovery mechanism outside of the discovery trait

use super::traits::{ClusterDiscovery, DiscoveryEvent, SimpleDiscoveryHandle};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// No-op discovery that doesn't discover any peers.
///
/// This is useful for:
/// - Single-node deployments
/// - Manual peer management via `add_peer`/`remove_peer` APIs
/// - Testing scenarios
///
/// # Example
///
/// ```
/// use memberlist_plumtree::discovery::NoOpDiscovery;
///
/// // Create no-op discovery for single node
/// let discovery = NoOpDiscovery::<u64>::new("127.0.0.1:9000".parse().unwrap());
/// ```
#[derive(Debug, Clone)]
pub struct NoOpDiscovery<I> {
    local_addr: SocketAddr,
    _marker: PhantomData<I>,
}

impl<I> NoOpDiscovery<I> {
    /// Create a new no-op discovery.
    ///
    /// # Arguments
    ///
    /// * `local_addr` - The local node's address
    pub fn new(local_addr: SocketAddr) -> Self {
        Self {
            local_addr,
            _marker: PhantomData,
        }
    }
}

impl<I> ClusterDiscovery<I> for NoOpDiscovery<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    type Handle = SimpleDiscoveryHandle;

    async fn start(&self) -> (async_channel::Receiver<DiscoveryEvent<I>>, Self::Handle) {
        let (tx, rx) = async_channel::bounded(1);
        let running = Arc::new(AtomicBool::new(true));

        // No-op: just close the sender immediately (no peers to discover)
        drop(tx);

        (rx, SimpleDiscoveryHandle::new(running))
    }

    fn local_addr(&self) -> Option<SocketAddr> {
        Some(self.local_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::DiscoveryHandle;

    #[tokio::test]
    async fn test_noop_discovery() {
        let discovery = NoOpDiscovery::<u64>::new("127.0.0.1:9000".parse().unwrap());

        assert_eq!(
            discovery.local_addr(),
            Some("127.0.0.1:9000".parse().unwrap())
        );

        let (rx, handle) = discovery.start().await;

        // Should be running
        assert!(handle.is_running());

        // Channel should be closed immediately (no peers)
        assert!(rx.recv().await.is_err());

        // Stop
        handle.stop();
        assert!(!handle.is_running());
    }

    #[tokio::test]
    async fn test_noop_announce() {
        let discovery = NoOpDiscovery::<u64>::new("127.0.0.1:9000".parse().unwrap());

        // Announce should succeed (no-op)
        let result = discovery
            .announce(&1u64, "127.0.0.1:9001".parse().unwrap())
            .await;
        assert!(result.is_ok());
    }
}
