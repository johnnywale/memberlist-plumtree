//! Peer address resolution for QUIC transport.
//!
//! The [`PeerResolver`] trait provides a way to map peer IDs to socket addresses.
//! This is necessary because Plumtree uses abstract peer IDs, but QUIC needs
//! concrete addresses to establish connections.
//!
//! # Implementations
//!
//! - [`MapPeerResolver`]: Simple HashMap-based resolver for testing and simple deployments
//!
//! # Custom Implementations
//!
//! For more complex scenarios (DNS-based resolution, service discovery, etc.),
//! implement the [`PeerResolver`] trait directly.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;

/// Trait for resolving peer IDs to socket addresses.
///
/// Implementations must be thread-safe and efficient, as resolution
/// may be called frequently during message sending.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::transport::quic::{PeerResolver, MapPeerResolver};
/// use std::net::SocketAddr;
///
/// // Using the built-in MapPeerResolver
/// let mut resolver = MapPeerResolver::new("127.0.0.1:9000".parse().unwrap());
/// resolver.add_peer(1u64, "192.168.1.10:9000".parse().unwrap());
/// resolver.add_peer(2u64, "192.168.1.11:9000".parse().unwrap());
///
/// assert_eq!(
///     resolver.resolve(&1u64),
///     Some("192.168.1.10:9000".parse().unwrap())
/// );
/// ```
pub trait PeerResolver<I>: Send + Sync + 'static
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    /// Resolve a peer ID to a socket address.
    ///
    /// Returns `None` if the peer is not known.
    fn resolve(&self, peer: &I) -> Option<SocketAddr>;

    /// Get the local socket address for this node.
    fn local_addr(&self) -> SocketAddr;

    /// Check if a peer is known to this resolver.
    fn contains(&self, peer: &I) -> bool {
        self.resolve(peer).is_some()
    }
}

/// Simple HashMap-based peer resolver.
///
/// Thread-safe resolver that maps peer IDs to socket addresses using an
/// internal `RwLock<HashMap>`. Suitable for testing and simple deployments.
///
/// # Example
///
/// ```ignore
/// use memberlist_plumtree::transport::quic::MapPeerResolver;
/// use std::net::SocketAddr;
///
/// let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
/// let mut resolver = MapPeerResolver::new(local_addr);
///
/// // Add peers
/// resolver.add_peer("node1".to_string(), "192.168.1.10:9000".parse().unwrap());
/// resolver.add_peer("node2".to_string(), "192.168.1.11:9000".parse().unwrap());
///
/// // Resolve peer addresses
/// assert!(resolver.resolve(&"node1".to_string()).is_some());
/// assert!(resolver.resolve(&"unknown".to_string()).is_none());
/// ```
#[derive(Debug)]
pub struct MapPeerResolver<I> {
    local_addr: SocketAddr,
    peers: RwLock<HashMap<I, SocketAddr>>,
}

impl<I> MapPeerResolver<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    /// Create a new resolver with the given local address.
    pub fn new(local_addr: SocketAddr) -> Self {
        Self {
            local_addr,
            peers: RwLock::new(HashMap::new()),
        }
    }

    /// Create a resolver with pre-populated peers.
    pub fn with_peers(local_addr: SocketAddr, peers: HashMap<I, SocketAddr>) -> Self {
        Self {
            local_addr,
            peers: RwLock::new(peers),
        }
    }

    /// Add a peer to the resolver.
    pub fn add_peer(&self, peer: I, addr: SocketAddr) {
        self.peers.write().insert(peer, addr);
    }

    /// Remove a peer from the resolver.
    pub fn remove_peer(&self, peer: &I) -> Option<SocketAddr> {
        self.peers.write().remove(peer)
    }

    /// Update a peer's address.
    pub fn update_peer(&self, peer: I, addr: SocketAddr) -> Option<SocketAddr> {
        self.peers.write().insert(peer, addr)
    }

    /// Get the number of known peers.
    pub fn peer_count(&self) -> usize {
        self.peers.read().len()
    }

    /// Get all known peer IDs.
    pub fn peer_ids(&self) -> Vec<I> {
        self.peers.read().keys().cloned().collect()
    }

    /// Clear all peers.
    pub fn clear(&self) {
        self.peers.write().clear();
    }

    /// Get a snapshot of all peer mappings.
    pub fn snapshot(&self) -> HashMap<I, SocketAddr> {
        self.peers.read().clone()
    }
}

impl<I> PeerResolver<I> for MapPeerResolver<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    fn resolve(&self, peer: &I) -> Option<SocketAddr> {
        self.peers.read().get(peer).copied()
    }

    fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl<I> Clone for MapPeerResolver<I>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            local_addr: self.local_addr,
            peers: RwLock::new(self.peers.read().clone()),
        }
    }
}

// Implement PeerResolver for Arc<R> where R: PeerResolver
impl<I, R> PeerResolver<I> for Arc<R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I>,
{
    fn resolve(&self, peer: &I) -> Option<SocketAddr> {
        (**self).resolve(peer)
    }

    fn local_addr(&self) -> SocketAddr {
        (**self).local_addr()
    }
}

// Implement PeerResolver for Box<R> where R: PeerResolver
impl<I, R> PeerResolver<I> for Box<R>
where
    I: Clone + Eq + Hash + Debug + Send + Sync + 'static,
    R: PeerResolver<I> + ?Sized,
{
    fn resolve(&self, peer: &I) -> Option<SocketAddr> {
        (**self).resolve(peer)
    }

    fn local_addr(&self) -> SocketAddr {
        (**self).local_addr()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_resolver_basic() {
        let local_addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();
        let resolver = MapPeerResolver::<u64>::new(local_addr);

        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());
        resolver.add_peer(2, "192.168.1.11:9000".parse().unwrap());

        assert_eq!(
            resolver.resolve(&1),
            Some("192.168.1.10:9000".parse().unwrap())
        );
        assert_eq!(
            resolver.resolve(&2),
            Some("192.168.1.11:9000".parse().unwrap())
        );
        assert_eq!(resolver.resolve(&3), None);
        assert_eq!(resolver.local_addr(), local_addr);
    }

    #[test]
    fn test_map_resolver_remove() {
        let resolver = MapPeerResolver::<u64>::new("127.0.0.1:9000".parse().unwrap());
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());

        assert!(resolver.contains(&1));
        let removed = resolver.remove_peer(&1);
        assert!(removed.is_some());
        assert!(!resolver.contains(&1));
    }

    #[test]
    fn test_map_resolver_update() {
        let resolver = MapPeerResolver::<u64>::new("127.0.0.1:9000".parse().unwrap());
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());

        let old = resolver.update_peer(1, "192.168.1.20:9000".parse().unwrap());
        assert_eq!(old, Some("192.168.1.10:9000".parse().unwrap()));
        assert_eq!(
            resolver.resolve(&1),
            Some("192.168.1.20:9000".parse().unwrap())
        );
    }

    #[test]
    fn test_map_resolver_with_peers() {
        let mut peers = HashMap::new();
        peers.insert(1u64, "192.168.1.10:9000".parse().unwrap());
        peers.insert(2u64, "192.168.1.11:9000".parse().unwrap());

        let resolver = MapPeerResolver::with_peers(
            "127.0.0.1:9000".parse().unwrap(),
            peers,
        );

        assert_eq!(resolver.peer_count(), 2);
        assert!(resolver.contains(&1));
        assert!(resolver.contains(&2));
    }

    #[test]
    fn test_map_resolver_snapshot() {
        let resolver = MapPeerResolver::<u64>::new("127.0.0.1:9000".parse().unwrap());
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());
        resolver.add_peer(2, "192.168.1.11:9000".parse().unwrap());

        let snapshot = resolver.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains_key(&1));
        assert!(snapshot.contains_key(&2));
    }

    #[test]
    fn test_map_resolver_clear() {
        let resolver = MapPeerResolver::<u64>::new("127.0.0.1:9000".parse().unwrap());
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());
        resolver.add_peer(2, "192.168.1.11:9000".parse().unwrap());

        assert_eq!(resolver.peer_count(), 2);
        resolver.clear();
        assert_eq!(resolver.peer_count(), 0);
    }

    #[test]
    fn test_map_resolver_clone() {
        let resolver = MapPeerResolver::<u64>::new("127.0.0.1:9000".parse().unwrap());
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());

        let cloned = resolver.clone();
        assert_eq!(resolver.peer_count(), cloned.peer_count());
        assert_eq!(resolver.resolve(&1), cloned.resolve(&1));
    }

    #[test]
    fn test_map_resolver_peer_ids() {
        let resolver = MapPeerResolver::<u64>::new("127.0.0.1:9000".parse().unwrap());
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());
        resolver.add_peer(2, "192.168.1.11:9000".parse().unwrap());

        let mut ids = resolver.peer_ids();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }

    #[test]
    fn test_arc_resolver() {
        let resolver = Arc::new(MapPeerResolver::<u64>::new(
            "127.0.0.1:9000".parse().unwrap(),
        ));
        resolver.add_peer(1, "192.168.1.10:9000".parse().unwrap());

        // Test through Arc
        let r: Arc<dyn PeerResolver<u64>> = resolver.clone();
        assert_eq!(r.resolve(&1), Some("192.168.1.10:9000".parse().unwrap()));
        assert_eq!(r.local_addr(), "127.0.0.1:9000".parse().unwrap());
    }
}
