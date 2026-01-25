//! Core discovery traits.

use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::net::SocketAddr;

/// Events emitted by peer discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent<I> {
    /// A new peer was discovered or became reachable.
    PeerDiscovered {
        /// The peer's node ID.
        id: I,
        /// The peer's address.
        addr: SocketAddr,
    },
    /// A peer is no longer reachable.
    PeerLost {
        /// The peer's node ID.
        id: I,
    },
}

/// Handle for controlling a running discovery process.
pub trait DiscoveryHandle: Send + Sync {
    /// Check if the discovery process is still running.
    fn is_running(&self) -> bool;

    /// Stop the discovery process.
    fn stop(&self);
}

/// Trait for peer discovery mechanisms.
///
/// Implementations provide different strategies for discovering peers:
/// - Static seeds (known addresses)
/// - mDNS/DNS-SD (local network)
/// - Custom discovery (e.g., DHT, centralized registry)
///
/// # Example Implementation
///
/// ```ignore
/// use memberlist_plumtree::discovery::{ClusterDiscovery, DiscoveryEvent, DiscoveryHandle};
///
/// struct MyDiscovery {
///     // ...
/// }
///
/// impl<I> ClusterDiscovery<I> for MyDiscovery
/// where
///     I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
/// {
///     type Handle = MyDiscoveryHandle;
///
///     async fn start(
///         &self,
///     ) -> (async_channel::Receiver<DiscoveryEvent<I>>, Self::Handle) {
///         // Start background discovery tasks
///         // Return event receiver and handle
///     }
/// }
/// ```
pub trait ClusterDiscovery<I>: Send + Sync
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    /// Handle type for controlling the discovery process.
    type Handle: DiscoveryHandle;

    /// Start the discovery process.
    ///
    /// Returns:
    /// - A receiver for discovery events (peer discovered/lost)
    /// - A handle for stopping the discovery process
    ///
    /// The discovery process runs in the background until `stop()` is called
    /// on the handle or the handle is dropped.
    fn start(
        &self,
    ) -> impl Future<Output = (async_channel::Receiver<DiscoveryEvent<I>>, Self::Handle)> + Send;

    /// Announce this node to the discovery mechanism.
    ///
    /// Some discovery mechanisms (like mDNS) require active announcement.
    /// For static seeds, this is typically a no-op.
    fn announce(
        &self,
        _id: &I,
        _addr: SocketAddr,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send {
        async { Ok(()) }
    }

    /// Get the local node's address as seen by this discovery mechanism.
    ///
    /// Returns `None` if the local address is not known or not applicable.
    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }
}

/// Simple handle that uses an atomic bool for stopping.
#[derive(Debug)]
pub struct SimpleDiscoveryHandle {
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl SimpleDiscoveryHandle {
    /// Create a new simple handle.
    pub fn new(running: std::sync::Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self { running }
    }
}

impl DiscoveryHandle for SimpleDiscoveryHandle {
    fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Acquire)
    }

    fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_event() {
        let event: DiscoveryEvent<u64> = DiscoveryEvent::PeerDiscovered {
            id: 1,
            addr: "127.0.0.1:9000".parse().unwrap(),
        };

        match event {
            DiscoveryEvent::PeerDiscovered { id, addr } => {
                assert_eq!(id, 1);
                assert_eq!(addr.port(), 9000);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn test_simple_handle() {
        let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let handle = SimpleDiscoveryHandle::new(running.clone());

        assert!(handle.is_running());
        handle.stop();
        assert!(!handle.is_running());
    }
}
