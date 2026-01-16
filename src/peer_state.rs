//! Peer state management for Plumtree protocol.
//!
//! Manages the eager (tree) and lazy (gossip) peer sets, providing
//! efficient operations for peer selection and state transitions.

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use std::{collections::HashSet, hash::Hash, sync::Arc};

/// Manages eager and lazy peer sets for the Plumtree protocol.
///
/// Eager peers form the spanning tree and receive full messages.
/// Lazy peers receive only IHave announcements for reliability.
#[derive(Debug)]
pub struct PeerState<I> {
    inner: RwLock<PeerStateInner<I>>,
}

#[derive(Debug)]
struct PeerStateInner<I> {
    /// Peers in the eager (tree) set - receive full messages.
    eager: HashSet<I>,
    /// Peers in the lazy set - receive IHave announcements.
    lazy: HashSet<I>,
    /// Cached vector of eager peers for fast random selection.
    /// Always kept in sync with `eager` set during mutations.
    eager_vec: Vec<I>,
    /// Cached vector of lazy peers for fast random selection.
    /// Always kept in sync with `lazy` set during mutations.
    lazy_vec: Vec<I>,
}

impl<I: Clone + Eq + Hash> PeerState<I> {
    /// Create a new empty peer state.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(PeerStateInner {
                eager: HashSet::new(),
                lazy: HashSet::new(),
                eager_vec: Vec::new(),
                lazy_vec: Vec::new(),
            }),
        }
    }

    /// Create peer state with initial peers.
    ///
    /// Peers are randomly distributed between eager and lazy sets
    /// based on the configured fanout values.
    pub fn with_initial_peers(
        peers: impl IntoIterator<Item = I>,
        eager_fanout: usize,
        _lazy_fanout: usize,
    ) -> Self {
        let mut all_peers: Vec<I> = peers.into_iter().collect();
        all_peers.shuffle(&mut rand::rng());

        let eager: HashSet<I> = all_peers.iter().take(eager_fanout).cloned().collect();
        let lazy: HashSet<I> = all_peers.iter().skip(eager_fanout).cloned().collect();

        let eager_vec: Vec<I> = eager.iter().cloned().collect();
        let lazy_vec: Vec<I> = lazy.iter().cloned().collect();

        Self {
            inner: RwLock::new(PeerStateInner {
                eager,
                lazy,
                eager_vec,
                lazy_vec,
            }),
        }
    }

    /// Add a new peer to the lazy set.
    ///
    /// New peers always start as lazy. They are promoted to eager
    /// via the Graft mechanism if needed.
    pub fn add_peer(&self, peer: I) {
        let mut inner = self.inner.write();

        // Don't add if already in either set
        if inner.eager.contains(&peer) || inner.lazy.contains(&peer) {
            return;
        }

        // Update set and cache immediately to avoid write lock in hot path
        inner.lazy.insert(peer.clone());
        inner.lazy_vec.push(peer);
    }

    /// Remove a peer from all sets.
    pub fn remove_peer(&self, peer: &I) {
        let mut inner = self.inner.write();

        let removed_eager = inner.eager.remove(peer);
        let removed_lazy = inner.lazy.remove(peer);

        // Update caches immediately to avoid write lock in hot path
        if removed_eager {
            inner.eager_vec.retain(|p| p != peer);
        }
        if removed_lazy {
            inner.lazy_vec.retain(|p| p != peer);
        }
    }

    /// Promote a peer from lazy to eager set.
    ///
    /// Called when we receive a Graft or need to establish tree connection.
    pub fn promote_to_eager(&self, peer: &I) -> bool
    where
        I: Clone,
    {
        let mut inner = self.inner.write();

        // Already eager? No-op
        if inner.eager.contains(peer) {
            return false;
        }

        // Remove from lazy if present (update both set and cache)
        if inner.lazy.remove(peer) {
            inner.lazy_vec.retain(|p| p != peer);
        }

        // Add to eager (update both set and cache)
        inner.eager.insert(peer.clone());
        inner.eager_vec.push(peer.clone());

        true
    }

    /// Demote a peer from eager to lazy set.
    ///
    /// Called when we receive a Prune or detect redundant tree edge.
    pub fn demote_to_lazy(&self, peer: &I) -> bool
    where
        I: Clone,
    {
        let mut inner = self.inner.write();

        // Already lazy or not present? No-op
        if !inner.eager.contains(peer) {
            return false;
        }

        // Move from eager to lazy (update both sets and caches)
        inner.eager.remove(peer);
        inner.eager_vec.retain(|p| p != peer);
        inner.lazy.insert(peer.clone());
        inner.lazy_vec.push(peer.clone());

        true
    }

    /// Check if a peer is in the eager set.
    pub fn is_eager(&self, peer: &I) -> bool {
        self.inner.read().eager.contains(peer)
    }

    /// Check if a peer is in the lazy set.
    pub fn is_lazy(&self, peer: &I) -> bool {
        self.inner.read().lazy.contains(peer)
    }

    /// Check if a peer exists in any set.
    pub fn contains(&self, peer: &I) -> bool {
        let inner = self.inner.read();
        inner.eager.contains(peer) || inner.lazy.contains(peer)
    }

    /// Get all eager peers.
    pub fn eager_peers(&self) -> Vec<I>
    where
        I: Clone,
    {
        self.inner.read().eager.iter().cloned().collect()
    }

    /// Get all lazy peers.
    pub fn lazy_peers(&self) -> Vec<I>
    where
        I: Clone,
    {
        self.inner.read().lazy.iter().cloned().collect()
    }

    /// Get all peers (eager + lazy).
    pub fn all_peers(&self) -> Vec<I>
    where
        I: Clone,
    {
        let inner = self.inner.read();
        inner
            .eager
            .iter()
            .chain(inner.lazy.iter())
            .cloned()
            .collect()
    }

    /// Get random eager peers for message forwarding.
    ///
    /// Excludes the specified peer (usually the message sender).
    /// Uses only read lock - caches are kept up to date during mutations.
    /// Uses reservoir sampling for O(count) allocations instead of O(N).
    pub fn random_eager_except(&self, exclude: &I, count: usize) -> Vec<I>
    where
        I: Clone,
    {
        if count == 0 {
            return Vec::new();
        }

        // Read-only path - cache is always up to date
        let inner = self.inner.read();
        Self::reservoir_sample_except(&inner.eager_vec, exclude, count)
    }

    /// Reservoir sampling to select `count` random items, excluding one.
    /// O(N) scan but only O(count) allocations.
    fn reservoir_sample_except(items: &[I], exclude: &I, count: usize) -> Vec<I>
    where
        I: Clone,
    {
        use rand::Rng;

        // If count >= items.len() - 1 (excluding exclude), just return all except exclude
        // Also handle count == usize::MAX gracefully
        let effective_count = count.min(items.len());
        if effective_count == 0 {
            return Vec::new();
        }

        let mut rng = rand::rng();
        let mut reservoir: Vec<I> = Vec::with_capacity(effective_count);
        let mut seen = 0usize;

        for item in items {
            if item == exclude {
                continue;
            }

            if reservoir.len() < effective_count {
                // Fill the reservoir first
                reservoir.push(item.clone());
            } else {
                // Reservoir sampling: replace with probability count/seen
                let j = rng.random_range(0..=seen);
                if j < effective_count {
                    reservoir[j] = item.clone();
                }
            }
            seen += 1;
        }

        reservoir
    }

    /// Get random lazy peers for IHave announcements.
    ///
    /// Excludes the specified peer (usually the message sender).
    /// Uses only read lock - caches are kept up to date during mutations.
    /// Uses reservoir sampling for O(count) allocations instead of O(N).
    pub fn random_lazy_except(&self, exclude: &I, count: usize) -> Vec<I>
    where
        I: Clone,
    {
        if count == 0 {
            return Vec::new();
        }

        // Read-only path - cache is always up to date
        let inner = self.inner.read();
        Self::reservoir_sample_except(&inner.lazy_vec, exclude, count)
    }

    /// Get the number of eager peers.
    pub fn eager_count(&self) -> usize {
        self.inner.read().eager.len()
    }

    /// Get the number of lazy peers.
    pub fn lazy_count(&self) -> usize {
        self.inner.read().lazy.len()
    }

    /// Get the total number of peers.
    pub fn total_count(&self) -> usize {
        let inner = self.inner.read();
        inner.eager.len() + inner.lazy.len()
    }

    /// Clear all peers.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.eager.clear();
        inner.lazy.clear();
        inner.eager_vec.clear();
        inner.lazy_vec.clear();
    }

    /// Get statistics about peer state.
    pub fn stats(&self) -> PeerStats {
        let inner = self.inner.read();
        PeerStats {
            eager_count: inner.eager.len(),
            lazy_count: inner.lazy.len(),
        }
    }

    /// Rebalance peers to match target fanout values.
    ///
    /// Promotes or demotes peers as needed to reach target eager count.
    pub fn rebalance(&self, target_eager: usize)
    where
        I: Clone,
    {
        let mut inner = self.inner.write();

        let current_eager = inner.eager.len();

        if current_eager < target_eager {
            // Need to promote some lazy peers to eager
            let promote_count = target_eager - current_eager;
            let to_promote: Vec<I> = inner.lazy.iter().take(promote_count).cloned().collect();

            for peer in to_promote {
                inner.lazy.remove(&peer);
                inner.eager.insert(peer.clone());
                // Update caches
                inner.lazy_vec.retain(|p| *p != peer);
                inner.eager_vec.push(peer);
            }
        } else if current_eager > target_eager {
            // Need to demote some eager peers to lazy
            let demote_count = current_eager - target_eager;
            let to_demote: Vec<I> = inner.eager.iter().take(demote_count).cloned().collect();

            for peer in to_demote {
                inner.eager.remove(&peer);
                inner.lazy.insert(peer.clone());
                // Update caches
                inner.eager_vec.retain(|p| *p != peer);
                inner.lazy_vec.push(peer);
            }
        }
    }
}

impl<I: Clone + Eq + Hash> Default for PeerState<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about peer state.
#[derive(Debug, Clone, Copy)]
pub struct PeerStats {
    /// Number of eager peers.
    pub eager_count: usize,
    /// Number of lazy peers.
    pub lazy_count: usize,
}

impl PeerStats {
    /// Get total peer count.
    pub fn total(&self) -> usize {
        self.eager_count + self.lazy_count
    }
}

/// Builder for creating PeerState with specific configuration.
#[derive(Debug)]
pub struct PeerStateBuilder<I> {
    peers: Vec<I>,
    eager_fanout: usize,
    lazy_fanout: usize,
}

impl<I: Clone + Eq + Hash> PeerStateBuilder<I> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            peers: Vec::new(),
            eager_fanout: 3,
            lazy_fanout: 6,
        }
    }

    /// Add peers to initialize with.
    pub fn with_peers(mut self, peers: impl IntoIterator<Item = I>) -> Self {
        self.peers.extend(peers);
        self
    }

    /// Set the eager fanout target.
    pub fn with_eager_fanout(mut self, fanout: usize) -> Self {
        self.eager_fanout = fanout;
        self
    }

    /// Set the lazy fanout target.
    pub fn with_lazy_fanout(mut self, fanout: usize) -> Self {
        self.lazy_fanout = fanout;
        self
    }

    /// Build the PeerState.
    pub fn build(self) -> PeerState<I> {
        PeerState::with_initial_peers(self.peers, self.eager_fanout, self.lazy_fanout)
    }
}

impl<I: Clone + Eq + Hash> Default for PeerStateBuilder<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper for thread-safe peer state sharing.
pub type SharedPeerState<I> = Arc<PeerState<I>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_peer_state() {
        let state: PeerState<u64> = PeerState::new();
        assert_eq!(state.eager_count(), 0);
        assert_eq!(state.lazy_count(), 0);
    }

    #[test]
    fn test_add_peer() {
        let state: PeerState<u64> = PeerState::new();
        state.add_peer(1);
        state.add_peer(2);

        assert_eq!(state.lazy_count(), 2);
        assert_eq!(state.eager_count(), 0);
        assert!(state.is_lazy(&1));
        assert!(state.is_lazy(&2));
    }

    #[test]
    fn test_promote_to_eager() {
        let state: PeerState<u64> = PeerState::new();
        state.add_peer(1);

        assert!(state.is_lazy(&1));
        state.promote_to_eager(&1);
        assert!(state.is_eager(&1));
        assert!(!state.is_lazy(&1));
    }

    #[test]
    fn test_demote_to_lazy() {
        let state: PeerState<u64> = PeerState::new();
        state.add_peer(1);
        state.promote_to_eager(&1);

        assert!(state.is_eager(&1));
        state.demote_to_lazy(&1);
        assert!(state.is_lazy(&1));
        assert!(!state.is_eager(&1));
    }

    #[test]
    fn test_remove_peer() {
        let state: PeerState<u64> = PeerState::new();
        state.add_peer(1);
        state.add_peer(2);
        state.promote_to_eager(&1);

        state.remove_peer(&1);
        state.remove_peer(&2);

        assert_eq!(state.total_count(), 0);
    }

    #[test]
    fn test_initial_peers() {
        let peers: Vec<u64> = (1..=10).collect();
        let state = PeerState::with_initial_peers(peers, 3, 6);

        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 7);
    }

    #[test]
    fn test_random_selection() {
        let peers: Vec<u64> = (1..=10).collect();
        let state = PeerState::with_initial_peers(peers, 5, 5);

        let selected = state.random_eager_except(&1, 3);
        assert!(selected.len() <= 3);
        assert!(!selected.contains(&1));
    }

    #[test]
    fn test_rebalance() {
        let state: PeerState<u64> = PeerState::new();
        for i in 1..=10 {
            state.add_peer(i);
        }

        assert_eq!(state.eager_count(), 0);
        assert_eq!(state.lazy_count(), 10);

        state.rebalance(4);
        assert_eq!(state.eager_count(), 4);
        assert_eq!(state.lazy_count(), 6);

        state.rebalance(2);
        assert_eq!(state.eager_count(), 2);
        assert_eq!(state.lazy_count(), 8);
    }

    #[test]
    fn test_builder() {
        let state = PeerStateBuilder::new()
            .with_peers(1..=10)
            .with_eager_fanout(4)
            .with_lazy_fanout(6)
            .build();

        assert_eq!(state.eager_count(), 4);
        assert_eq!(state.lazy_count(), 6);
    }
}
