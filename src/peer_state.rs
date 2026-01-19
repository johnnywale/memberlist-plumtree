//! Peer state management for Plumtree protocol.
//!
//! Manages the eager (tree) and lazy (gossip) peer sets, providing
//! efficient operations for peer selection and state transitions.
//!
//! ## Hash Ring Topology
//!
//! This module implements a deterministic hash permutation algorithm for automatic
//! peer classification. Each node sorts **all known peer IDs** by their hash values
//! to form a stable logical hash ring, providing:
//!
//! - **Base Connectivity Layer (Z≥2 redundancy)**: Each node connects to its immediate
//!   predecessor and successor on the hash ring, plus second-nearest neighbors.
//! - **Performance Optimization Layer**: Long-range jumps (i±X/4) reduce network
//!   diameter and shorten message propagation latency.
//!
//! ## Stability Guarantees
//!
//! The hash ring is computed from a **stable membership view** (`known_peers`), not
//! from the eager/lazy subsets. This ensures:
//!
//! - Deterministic neighbor selection across all nodes
//! - Ring topology independent of promotion/demotion state
//! - Predictable healing under churn
//!
//! ## Ring Neighbor Protection
//!
//! Adjacent ring neighbors (i±1, i±2) are protected from demotion to guarantee
//! Z≥2 redundancy. They can only be removed when the peer leaves the cluster.
//!
//! ## Eviction Strategy
//!
//! When the peer limit is reached, the fingerprint hashing algorithm selects which
//! lazy peer to evict deterministically:
//!
//! ```text
//! Hash(sorted(known_peers) + New_Peer) mod |Evictable_Lazy_Set|
//! ```

use parking_lot::RwLock;
use rand::seq::SliceRandom;
use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// Fixed seed for stable hashing across Rust versions and architectures.
/// Using SipHash-1-3 with fixed keys for determinism.
const HASH_KEY_0: u64 = 0x0706050403020100;
const HASH_KEY_1: u64 = 0x0f0e0d0c0b0a0908;

/// Result of adding a peer with auto-classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddPeerResult {
    /// Peer was added to the eager set.
    AddedEager,
    /// Peer was added to the lazy set.
    AddedLazy,
    /// Peer was added after evicting another lazy peer.
    AddedAfterEviction,
    /// Peer already exists in either set.
    AlreadyExists,
    /// Peer limit reached and no evictable lazy peers.
    LimitReached,
}

/// Result of removing a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovePeerResult {
    /// Peer was removed from the eager set.
    RemovedEager,
    /// Peer was removed from the lazy set.
    RemovedLazy,
    /// Peer was not found in any set.
    NotFound,
}

impl RemovePeerResult {
    /// Returns true if the peer was in the eager set.
    pub fn was_eager(&self) -> bool {
        matches!(self, RemovePeerResult::RemovedEager)
    }

    /// Returns true if the peer was found and removed.
    pub fn was_removed(&self) -> bool {
        !matches!(self, RemovePeerResult::NotFound)
    }
}

/// Snapshot of the current peer topology.
///
/// Contains the list of eager and lazy peers at a point in time.
/// This is useful for serialization and debugging.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct PeerTopology<I> {
    /// Peers in the eager (tree) set - receive full messages.
    pub eager: Vec<I>,
    /// Peers in the lazy set - receive IHave announcements.
    pub lazy: Vec<I>,
}

impl<I> PeerTopology<I> {
    /// Create a new peer topology.
    pub fn new(eager: Vec<I>, lazy: Vec<I>) -> Self {
        Self { eager, lazy }
    }

    /// Total number of peers.
    pub fn total(&self) -> usize {
        self.eager.len() + self.lazy.len()
    }

    /// Number of eager peers.
    pub fn eager_count(&self) -> usize {
        self.eager.len()
    }

    /// Number of lazy peers.
    pub fn lazy_count(&self) -> usize {
        self.lazy.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.eager.is_empty() && self.lazy.is_empty()
    }
}

impl<I: Clone + Eq + Hash> PeerTopology<I> {
    /// Check if a peer exists in the topology.
    pub fn contains(&self, peer: &I) -> bool {
        self.eager.contains(peer) || self.lazy.contains(peer)
    }

    /// Check if a peer is eager.
    pub fn is_eager(&self, peer: &I) -> bool {
        self.eager.contains(peer)
    }

    /// Check if a peer is lazy.
    pub fn is_lazy(&self, peer: &I) -> bool {
        self.lazy.contains(peer)
    }
}

impl<I> Default for PeerTopology<I> {
    fn default() -> Self {
        Self {
            eager: Vec::new(),
            lazy: Vec::new(),
        }
    }
}

/// Manages eager and lazy peer sets for the Plumtree protocol.
///
/// Eager peers form the spanning tree and receive full messages.
/// Lazy peers receive only IHave announcements for reliability.
///
/// ## Hash Ring Topology
///
/// The peer state uses a deterministic hash ring for topology management:
///
/// - **Adjacent Links** `(i±1) mod X`: Guarantee Z=1 basic ring connectivity
/// - **Second-nearest Links** `(i±2) mod X`: Guarantee Z=2 redundancy; tolerate neighbor failures
/// - **Long-range Jumps** `(i±X/4) mod X`: Greatly reduce network diameter; improve Gossip efficiency
///
/// Where `X` is the total number of known nodes and `i` is the local node's position.
///
/// ## Ring Neighbor Protection
///
/// Adjacent neighbors (i±1, i±2) are protected from demotion to maintain Z≥2 redundancy.
/// They can only be removed when the peer leaves the cluster entirely.
#[derive(Debug)]
pub struct PeerState<I> {
    inner: RwLock<PeerStateInner<I>>,
    /// Local node ID for hash ring position computation.
    /// If None, hash ring topology features are disabled.
    local_id: Option<I>,
    /// Whether to use hash ring topology with ring neighbor protection.
    /// When false, local_id is only used for diverse peer ordering.
    use_hash_ring: bool,
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
    /// All known peers (stable membership view for ring computation).
    /// This set only grows via add_peer and shrinks via remove_peer.
    /// It is independent of eager/lazy classification.
    known_peers: HashSet<I>,
    /// Protected ring neighbors that cannot be demoted (only removed).
    /// These are the (i±1, i±2) neighbors on the hash ring.
    ring_neighbors: HashSet<I>,
    /// Cached sorted ring (invalidated on membership change).
    /// Stored as (hash, peer_id) for stable ordering with collisions.
    cached_ring: Option<Vec<I>>,
}

/// Compute a deterministic hash value for a peer ID using fixed-seed SipHash.
/// This ensures consistent ordering across Rust versions and architectures.
pub fn stable_hash<I: Hash>(id: &I) -> u64 {
    use std::hash::BuildHasher;
    // Use SipHasher with fixed keys for determinism
    let state = std::hash::BuildHasherDefault::<siphasher::sip::SipHasher13>::default();
    let mut hasher = state.build_hasher();
    id.hash(&mut hasher);
    hasher.finish()
}

/// Sort peer IDs by (hash, id) to form a stable logical hash ring.
/// The Ord bound on I ensures stable ordering even with hash collisions.
pub fn sort_by_hash_stable<I: Clone + Hash + Ord>(peers: &[I]) -> Vec<I> {
    let mut sorted: Vec<(u64, I)> = peers.iter().map(|p| (stable_hash(p), p.clone())).collect();
    sorted
        .sort_by(|(hash_a, id_a), (hash_b, id_b)| hash_a.cmp(hash_b).then_with(|| id_a.cmp(id_b)));
    sorted.into_iter().map(|(_, p)| p).collect()
}

/// Find the position of a peer in the sorted hash ring.
pub fn find_position_in_ring<I: Eq>(sorted_peers: &[I], target: &I) -> Option<usize> {
    sorted_peers.iter().position(|p| p == target)
}

/// Connection type for hash ring topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashRingConnection {
    /// Adjacent link (i±1) - basic ring connectivity
    Adjacent,
    /// Second-nearest link (i±2) - Z=2 redundancy
    SecondNearest,
    /// Long-range jump (i±X/4) - reduce network diameter
    LongRange,
}

impl<I: Clone + Eq + Hash + Ord> PeerState<I> {
    /// Create a new empty peer state without local ID.
    ///
    /// Hash ring topology features will be disabled. Use [`Self::new_with_local_id`]
    /// for deterministic hash ring-based peer classification.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(PeerStateInner {
                eager: HashSet::new(),
                lazy: HashSet::new(),
                eager_vec: Vec::new(),
                lazy_vec: Vec::new(),
                known_peers: HashSet::new(),
                ring_neighbors: HashSet::new(),
                cached_ring: None,
            }),
            local_id: None,
            use_hash_ring: false,
        }
    }

    /// Create a new empty peer state with local node ID.
    ///
    /// The local ID enables hash ring topology features:
    /// - Deterministic peer selection based on hash ring position
    /// - Adjacent and jump link connections
    /// - Ring neighbor protection (Z≥2 guarantee)
    /// - Fingerprint-based eviction that preserves topological diversity
    pub fn new_with_local_id(local_id: I) -> Self {
        Self {
            inner: RwLock::new(PeerStateInner {
                eager: HashSet::new(),
                lazy: HashSet::new(),
                eager_vec: Vec::new(),
                lazy_vec: Vec::new(),
                known_peers: HashSet::new(),
                ring_neighbors: HashSet::new(),
                cached_ring: None,
            }),
            local_id: Some(local_id),
            use_hash_ring: true, // Full hash ring features when local_id is explicitly set
        }
    }

    /// Get the local node ID if set.
    pub fn local_id(&self) -> Option<&I> {
        self.local_id.as_ref()
    }

    /// Invalidate the cached ring (called when known_peers changes).
    fn invalidate_ring_cache(inner: &mut PeerStateInner<I>) {
        inner.cached_ring = None;
    }

    /// Get or compute the sorted ring from known_peers + local_id.
    fn get_or_compute_ring(&self, inner: &PeerStateInner<I>) -> Vec<I> {
        if let Some(ref cached) = inner.cached_ring {
            return cached.clone();
        }

        let mut all: Vec<I> = inner.known_peers.iter().cloned().collect();
        if let Some(ref local) = self.local_id {
            all.push(local.clone());
        }
        sort_by_hash_stable(&all)
    }

    /// Compute ring neighbors for the local node.
    /// Returns (adjacent: i±1, second_nearest: i±2, long_range: i±X/4).
    fn compute_ring_neighbors(&self, inner: &PeerStateInner<I>) -> HashSet<I> {
        // Only compute ring neighbors when hash ring is explicitly enabled
        if !self.use_hash_ring {
            return HashSet::new();
        }

        let Some(ref local_id) = self.local_id else {
            return HashSet::new();
        };

        let sorted_ring = self.get_or_compute_ring(inner);
        let ring_size = sorted_ring.len();

        if ring_size <= 1 {
            return HashSet::new();
        }

        let Some(local_pos) = find_position_in_ring(&sorted_ring, local_id) else {
            return HashSet::new();
        };

        let mut neighbors = HashSet::new();

        // Adjacent (i±1) - always protected
        let pred = (local_pos + ring_size - 1) % ring_size;
        let succ = (local_pos + 1) % ring_size;
        if sorted_ring[pred] != *local_id {
            neighbors.insert(sorted_ring[pred].clone());
        }
        if sorted_ring[succ] != *local_id {
            neighbors.insert(sorted_ring[succ].clone());
        }

        // Second-nearest (i±2) - protected for Z≥2
        if ring_size > 3 {
            let pred2 = (local_pos + ring_size - 2) % ring_size;
            let succ2 = (local_pos + 2) % ring_size;
            if sorted_ring[pred2] != *local_id {
                neighbors.insert(sorted_ring[pred2].clone());
            }
            if sorted_ring[succ2] != *local_id {
                neighbors.insert(sorted_ring[succ2].clone());
            }
        }

        neighbors
    }

    /// Update the ring_neighbors set after membership changes.
    fn update_ring_neighbors(&self, inner: &mut PeerStateInner<I>) {
        inner.ring_neighbors = self.compute_ring_neighbors(inner);
    }

    /// Create peer state with initial peers (random distribution).
    ///
    /// Peers are randomly distributed between eager and lazy sets
    /// based on the configured fanout values.
    ///
    /// For deterministic hash ring-based distribution, use
    /// [`Self::with_initial_peers_hash_ring`] instead.
    pub fn with_initial_peers(
        peers: impl IntoIterator<Item = I>,
        eager_fanout: usize,
        _lazy_fanout: usize,
    ) -> Self {
        let mut all_peers: Vec<I> = peers.into_iter().collect();
        all_peers.shuffle(&mut rand::rng());

        let known_peers: HashSet<I> = all_peers.iter().cloned().collect();
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
                known_peers,
                ring_neighbors: HashSet::new(),
                cached_ring: None,
            }),
            local_id: None,
            use_hash_ring: false,
        }
    }

    /// Create peer state with initial peers using hash ring topology.
    ///
    /// This method implements deterministic hash ring-based peer classification:
    ///
    /// - **Adjacent links** `(i±1) mod X`: Immediate neighbors on the hash ring
    /// - **Second-nearest links** `(i±2) mod X`: Provide Z=2 redundancy
    /// - **Long-range jumps** `(i±X/4) mod X`: Reduce network diameter
    ///
    /// Ring neighbors are automatically protected from demotion.
    /// Lazy peers are selected based on ring distance to fill coverage gaps.
    ///
    /// # Arguments
    ///
    /// * `local_id` - The local node's ID for hash ring position
    /// * `peers` - All known peer IDs (excluding local)
    /// * `eager_fanout` - Target number of eager peers
    /// * `lazy_fanout` - Target number of lazy peers
    pub fn with_initial_peers_hash_ring(
        local_id: I,
        peers: impl IntoIterator<Item = I>,
        eager_fanout: usize,
        lazy_fanout: usize,
    ) -> Self {
        let all_peers: Vec<I> = peers.into_iter().collect();
        let known_peers: HashSet<I> = all_peers.iter().cloned().collect();

        if all_peers.is_empty() {
            return Self::new_with_local_id(local_id);
        }

        // Build the full ring including local node for position calculation
        let mut ring_with_local: Vec<I> = all_peers.clone();
        ring_with_local.push(local_id.clone());
        let sorted_ring = sort_by_hash_stable(&ring_with_local);

        // Find local node's position in the ring
        let local_pos = find_position_in_ring(&sorted_ring, &local_id).unwrap_or(0);
        let ring_size = sorted_ring.len();

        // Compute ring neighbors (these will be protected)
        let mut ring_neighbors: HashSet<I> = HashSet::new();
        let mut eager_candidates: Vec<I> = Vec::new();

        // 1. Adjacent links (i±1) - basic ring connectivity (always eager + protected)
        let pred = (local_pos + ring_size - 1) % ring_size;
        let succ = (local_pos + 1) % ring_size;
        if sorted_ring[pred] != local_id {
            eager_candidates.push(sorted_ring[pred].clone());
            ring_neighbors.insert(sorted_ring[pred].clone());
        }
        if sorted_ring[succ] != local_id && !eager_candidates.contains(&sorted_ring[succ]) {
            eager_candidates.push(sorted_ring[succ].clone());
            ring_neighbors.insert(sorted_ring[succ].clone());
        }

        // 2. Second-nearest links (i±2) - Z=2 redundancy (always eager + protected)
        if ring_size > 3 {
            let pred2 = (local_pos + ring_size - 2) % ring_size;
            let succ2 = (local_pos + 2) % ring_size;
            if sorted_ring[pred2] != local_id && !eager_candidates.contains(&sorted_ring[pred2]) {
                eager_candidates.push(sorted_ring[pred2].clone());
                ring_neighbors.insert(sorted_ring[pred2].clone());
            }
            if sorted_ring[succ2] != local_id && !eager_candidates.contains(&sorted_ring[succ2]) {
                eager_candidates.push(sorted_ring[succ2].clone());
                ring_neighbors.insert(sorted_ring[succ2].clone());
            }
        }

        // 3. Long-range jumps (i±X/4) - reduce network diameter (eager but not protected)
        if ring_size > 8 {
            let jump = ring_size / 4;
            let jump_pred = (local_pos + ring_size - jump) % ring_size;
            let jump_succ = (local_pos + jump) % ring_size;
            if sorted_ring[jump_pred] != local_id
                && !eager_candidates.contains(&sorted_ring[jump_pred])
            {
                eager_candidates.push(sorted_ring[jump_pred].clone());
            }
            if sorted_ring[jump_succ] != local_id
                && !eager_candidates.contains(&sorted_ring[jump_succ])
            {
                eager_candidates.push(sorted_ring[jump_succ].clone());
            }
        }

        // Take up to eager_fanout from candidates
        let eager: HashSet<I> = eager_candidates.into_iter().take(eager_fanout).collect();

        // Select lazy peers by ring distance (fill gaps in coverage)
        let max_lazy = lazy_fanout + eager_fanout.saturating_sub(eager.len());
        let mut lazy_candidates: Vec<(usize, I)> = Vec::new();

        for (idx, peer) in sorted_ring.iter().enumerate() {
            if *peer == local_id || eager.contains(peer) {
                continue;
            }
            // Calculate minimum ring distance from local
            let forward = if idx >= local_pos {
                idx - local_pos
            } else {
                ring_size - local_pos + idx
            };
            let backward = ring_size - forward;
            let distance = forward.min(backward);
            lazy_candidates.push((distance, peer.clone()));
        }

        // Sort by distance (nearest first for better coverage)
        lazy_candidates.sort_by_key(|(dist, _)| *dist);
        let lazy: HashSet<I> = lazy_candidates
            .into_iter()
            .take(max_lazy)
            .map(|(_, p)| p)
            .collect();

        let eager_vec: Vec<I> = eager.iter().cloned().collect();
        let lazy_vec: Vec<I> = lazy.iter().cloned().collect();

        Self {
            inner: RwLock::new(PeerStateInner {
                eager,
                lazy,
                eager_vec,
                lazy_vec,
                known_peers,
                ring_neighbors,
                cached_ring: Some(sorted_ring),
            }),
            local_id: Some(local_id),
            use_hash_ring: true, // Full hash ring features
        }
    }

    /// Get hash ring neighbors for the local node.
    ///
    /// Returns the peers that should be connected based on hash ring topology:
    /// - Adjacent (predecessor and successor)
    /// - Second-nearest (for Z=2 redundancy)
    /// - Long-range jumps (for reduced diameter)
    ///
    /// Returns None if local_id is not set.
    pub fn hash_ring_neighbors(&self) -> Option<Vec<(I, HashRingConnection)>> {
        let local_id = self.local_id.as_ref()?;
        let inner = self.inner.read();

        let sorted_ring = self.get_or_compute_ring(&inner);
        let ring_size = sorted_ring.len();

        if ring_size <= 1 {
            return Some(Vec::new());
        }

        let local_pos = find_position_in_ring(&sorted_ring, local_id)?;
        let mut neighbors = Vec::new();

        // Adjacent links
        let pred = (local_pos + ring_size - 1) % ring_size;
        let succ = (local_pos + 1) % ring_size;
        if sorted_ring[pred] != *local_id {
            neighbors.push((sorted_ring[pred].clone(), HashRingConnection::Adjacent));
        }
        if sorted_ring[succ] != *local_id {
            neighbors.push((sorted_ring[succ].clone(), HashRingConnection::Adjacent));
        }

        // Second-nearest links
        if ring_size > 3 {
            let pred2 = (local_pos + ring_size - 2) % ring_size;
            let succ2 = (local_pos + 2) % ring_size;
            if sorted_ring[pred2] != *local_id {
                neighbors.push((
                    sorted_ring[pred2].clone(),
                    HashRingConnection::SecondNearest,
                ));
            }
            if sorted_ring[succ2] != *local_id {
                neighbors.push((
                    sorted_ring[succ2].clone(),
                    HashRingConnection::SecondNearest,
                ));
            }
        }

        // Long-range jumps
        if ring_size > 8 {
            let jump = ring_size / 4;
            let jump_pred = (local_pos + ring_size - jump) % ring_size;
            let jump_succ = (local_pos + jump) % ring_size;
            if sorted_ring[jump_pred] != *local_id {
                neighbors.push((
                    sorted_ring[jump_pred].clone(),
                    HashRingConnection::LongRange,
                ));
            }
            if sorted_ring[jump_succ] != *local_id {
                neighbors.push((
                    sorted_ring[jump_succ].clone(),
                    HashRingConnection::LongRange,
                ));
            }
        }

        Some(neighbors)
    }

    /// Check if a peer is a protected ring neighbor.
    ///
    /// Protected neighbors cannot be demoted to lazy (only removed entirely).
    pub fn is_ring_neighbor(&self, peer: &I) -> bool {
        self.inner.read().ring_neighbors.contains(peer)
    }

    /// Get the set of protected ring neighbors.
    pub fn ring_neighbors(&self) -> HashSet<I> {
        self.inner.read().ring_neighbors.clone()
    }

    /// Add a new peer to the lazy set.
    ///
    /// New peers always start as lazy. They are promoted to eager
    /// via the Graft mechanism if needed.
    pub fn add_peer(&self, peer: I) {
        let mut inner = self.inner.write();

        // Don't add if already known
        if inner.known_peers.contains(&peer) {
            return;
        }

        // Add to known_peers and invalidate ring cache
        inner.known_peers.insert(peer.clone());
        Self::invalidate_ring_cache(&mut inner);

        // Update ring neighbors
        self.update_ring_neighbors(&mut inner);

        // Add to lazy set
        inner.lazy.insert(peer.clone());
        inner.lazy_vec.push(peer);
    }

    /// Add a new peer with automatic classification based on config limits.
    ///
    /// This method implements partial mesh topology by:
    /// 1. Checking if we're at the max_peers limit (if set)
    /// 2. Auto-classifying the peer as eager or lazy based on current counts
    ///
    /// Peers are added to eager set first (up to `eager_fanout`), then lazy.
    ///
    /// # Arguments
    ///
    /// * `peer` - The peer to add
    /// * `max_peers` - Maximum total peers allowed (None = unlimited)
    /// * `eager_fanout` - Target number of eager peers
    ///
    /// # Returns
    ///
    /// An [`AddPeerResult`] indicating the outcome of the operation.
    pub fn add_peer_auto(
        &self,
        peer: I,
        max_peers: Option<usize>,
        eager_fanout: usize,
    ) -> AddPeerResult {
        self.add_peer_auto_with_eviction(peer, max_peers, eager_fanout, true)
    }

    /// Add a new peer with automatic classification and optional eviction.
    ///
    /// When `allow_eviction` is true and the peer limit is reached, this method
    /// will evict a lazy peer to make room for the new peer. The eviction strategy
    /// is deterministic: it hashes the sorted list of all known peers plus the new
    /// peer to select which lazy peer to evict.
    ///
    /// Ring neighbors (i±1, i±2) are protected and will not be evicted.
    ///
    /// # Arguments
    ///
    /// * `peer` - The peer to add
    /// * `max_peers` - Maximum total peers allowed (None = unlimited)
    /// * `eager_fanout` - Target number of eager peers
    /// * `allow_eviction` - If true, evict a lazy peer when at capacity
    ///
    /// # Returns
    ///
    /// An [`AddPeerResult`] indicating the outcome of the operation.
    pub fn add_peer_auto_with_eviction(
        &self,
        peer: I,
        max_peers: Option<usize>,
        eager_fanout: usize,
        allow_eviction: bool,
    ) -> AddPeerResult {
        // Never add self as a peer
        if let Some(ref local_id) = self.local_id {
            if &peer == local_id {
                return AddPeerResult::AlreadyExists;
            }
        }

        let mut inner = self.inner.write();

        // Check if already exists
        if inner.known_peers.contains(&peer) {
            return AddPeerResult::AlreadyExists;
        }

        let total = inner.eager.len() + inner.lazy.len();

        // Check max_peers limit
        if let Some(max) = max_peers {
            if total >= max {
                // Find evictable lazy peers (not ring neighbors)
                let evictable: Vec<I> = inner
                    .lazy
                    .iter()
                    .filter(|p| !inner.ring_neighbors.contains(*p))
                    .cloned()
                    .collect();

                if allow_eviction && !evictable.is_empty() {
                    // Deterministic eviction: hash sorted known_peers + new_peer
                    let mut sorted_known: Vec<&I> = inner.known_peers.iter().collect();
                    sorted_known.sort();

                    let mut hasher =
                        siphasher::sip::SipHasher13::new_with_keys(HASH_KEY_0, HASH_KEY_1);
                    for known in &sorted_known {
                        (*known).hash(&mut hasher);
                    }
                    peer.hash(&mut hasher);

                    let hash = hasher.finish() as usize;
                    let evict_idx = hash % evictable.len();
                    let evicted = evictable[evict_idx].clone();

                    // Remove evicted peer from lazy
                    inner.lazy.remove(&evicted);
                    inner.lazy_vec.retain(|p| *p != evicted);
                    inner.known_peers.remove(&evicted);
                    Self::invalidate_ring_cache(&mut inner);

                    // Add new peer to known_peers
                    inner.known_peers.insert(peer.clone());

                    // Update ring neighbors after membership change
                    self.update_ring_neighbors(&mut inner);

                    // Add the new peer to lazy (temporary placement)
                    inner.lazy.insert(peer.clone());
                    inner.lazy_vec.push(peer.clone());

                    // Rebalance to ensure ring neighbors are in eager (only if hash ring enabled)
                    if self.local_id.is_some() && self.use_hash_ring {
                        self.rebalance_eager_with_ring_neighbors(&mut inner, eager_fanout);
                    }

                    return AddPeerResult::AddedAfterEviction;
                }
                return AddPeerResult::LimitReached;
            }
        }

        // Add to known_peers and invalidate cache
        inner.known_peers.insert(peer.clone());
        Self::invalidate_ring_cache(&mut inner);

        // Update ring neighbors (this recomputes based on new known_peers)
        self.update_ring_neighbors(&mut inner);

        // Hash ring-based classification when hash ring is enabled
        if self.local_id.is_some() && self.use_hash_ring {
            // Add the new peer to lazy first (temporary placement)
            inner.lazy.insert(peer.clone());
            inner.lazy_vec.push(peer.clone());

            // Rebalance: swap non-ring-neighbor eager with ring-neighbor lazy
            // This ensures ring neighbors are always in eager (up to fanout)
            self.rebalance_eager_with_ring_neighbors(&mut inner, eager_fanout);

            // Check where the peer ended up after rebalancing
            if inner.eager.contains(&peer) {
                return AddPeerResult::AddedEager;
            } else {
                return AddPeerResult::AddedLazy;
            }
        }

        // Fallback: simple first-come-first-served (no hash ring)
        if inner.eager.len() < eager_fanout {
            inner.eager.insert(peer.clone());
            inner.eager_vec.push(peer);
            AddPeerResult::AddedEager
        } else {
            inner.lazy.insert(peer.clone());
            inner.lazy_vec.push(peer);
            AddPeerResult::AddedLazy
        }
    }

    /// Rebalance eager set to prefer ring neighbors over non-ring-neighbors.
    ///
    /// This method ensures that ring neighbors are in the eager set by swapping:
    /// - Demote non-ring-neighbor eager peers to lazy
    /// - Promote ring-neighbor lazy peers to eager
    ///
    /// This is called after ring neighbors are recomputed (e.g., when peers join/leave).
    fn rebalance_eager_with_ring_neighbors(
        &self,
        inner: &mut PeerStateInner<I>,
        eager_fanout: usize,
    ) {
        // Find eager peers that are NOT ring neighbors (can be demoted)
        let demotable_eager: Vec<I> = inner
            .eager
            .iter()
            .filter(|p| !inner.ring_neighbors.contains(*p))
            .cloned()
            .collect();

        // Find lazy peers that ARE ring neighbors (should be promoted)
        let promotable_lazy: Vec<I> = inner
            .lazy
            .iter()
            .filter(|p| inner.ring_neighbors.contains(*p))
            .cloned()
            .collect();

        // Swap: demote non-ring-neighbor eager, promote ring-neighbor lazy
        let swap_count = demotable_eager.len().min(promotable_lazy.len());

        for i in 0..swap_count {
            let demote = &demotable_eager[i];
            let promote = &promotable_lazy[i];

            // Demote from eager to lazy
            inner.eager.remove(demote);
            inner.eager_vec.retain(|p| p != demote);
            inner.lazy.insert(demote.clone());
            inner.lazy_vec.push(demote.clone());

            // Promote from lazy to eager
            inner.lazy.remove(promote);
            inner.lazy_vec.retain(|p| p != promote);
            inner.eager.insert(promote.clone());
            inner.eager_vec.push(promote.clone());
        }

        // If eager is under fanout, promote remaining ring neighbors from lazy
        for neighbor in promotable_lazy.iter().skip(swap_count) {
            if inner.eager.len() >= eager_fanout {
                break;
            }
            // Move from lazy to eager
            inner.lazy.remove(neighbor);
            inner.lazy_vec.retain(|p| p != neighbor);
            inner.eager.insert(neighbor.clone());
            inner.eager_vec.push(neighbor.clone());
        }
    }

    /// Remove a peer from all sets.
    ///
    /// Returns the previous role of the peer (Eager, Lazy, or NotFound).
    /// This information can be used to trigger topology repair when an
    /// eager peer is removed.
    pub fn remove_peer(&self, peer: &I) -> RemovePeerResult {
        let mut inner = self.inner.write();

        // Remove from known_peers
        if !inner.known_peers.remove(peer) {
            return RemovePeerResult::NotFound;
        }

        // Invalidate ring cache and update neighbors
        Self::invalidate_ring_cache(&mut inner);
        inner.ring_neighbors.remove(peer);

        // Check eager first (more important for topology)
        if inner.eager.remove(peer) {
            inner.eager_vec.retain(|p| p != peer);
            // Update ring neighbors after removal
            self.update_ring_neighbors(&mut inner);
            return RemovePeerResult::RemovedEager;
        }

        // Check lazy
        if inner.lazy.remove(peer) {
            inner.lazy_vec.retain(|p| p != peer);
            // Update ring neighbors after removal
            self.update_ring_neighbors(&mut inner);
            return RemovePeerResult::RemovedLazy;
        }

        // Was in known_peers but not eager or lazy (shouldn't happen, but handle it)
        self.update_ring_neighbors(&mut inner);
        RemovePeerResult::NotFound
    }

    /// Promote a peer from lazy to eager set.
    ///
    /// Called when we receive a Graft or need to establish tree connection.
    pub fn promote_to_eager(&self, peer: &I) -> bool {
        let mut inner = self.inner.write();

        // Already eager? No-op
        if inner.eager.contains(peer) {
            return false;
        }

        // Must be in known_peers
        if !inner.known_peers.contains(peer) {
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
    ///
    /// **Note**: Ring neighbors (i±1, i±2) are protected and cannot be demoted.
    /// This method returns false for protected peers.
    pub fn demote_to_lazy(&self, peer: &I) -> bool {
        let mut inner = self.inner.write();

        // Already lazy or not present? No-op
        if !inner.eager.contains(peer) {
            return false;
        }

        // Protected ring neighbor? Cannot demote
        if inner.ring_neighbors.contains(peer) {
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
        self.inner.read().known_peers.contains(peer)
    }

    /// Get all eager peers.
    pub fn eager_peers(&self) -> Vec<I> {
        self.inner.read().eager.iter().cloned().collect()
    }

    /// Get all lazy peers.
    pub fn lazy_peers(&self) -> Vec<I> {
        self.inner.read().lazy.iter().cloned().collect()
    }

    /// Get all peers (eager + lazy).
    pub fn all_peers(&self) -> Vec<I> {
        self.inner.read().known_peers.iter().cloned().collect()
    }

    /// Get a snapshot of the current peer topology.
    ///
    /// Returns a `PeerTopology` struct containing both eager and lazy peers.
    /// This is useful for serialization, debugging, and API responses.
    pub fn topology(&self) -> PeerTopology<I> {
        let inner = self.inner.read();
        PeerTopology {
            eager: inner.eager.iter().cloned().collect(),
            lazy: inner.lazy.iter().cloned().collect(),
        }
    }

    /// Get random eager peers for message forwarding.
    ///
    /// Excludes the specified peer (usually the message sender).
    /// Uses only read lock - caches are kept up to date during mutations.
    /// Uses reservoir sampling for O(count) allocations instead of O(N).
    pub fn random_eager_except(&self, exclude: &I, count: usize) -> Vec<I> {
        if count == 0 {
            return Vec::new();
        }

        // Read-only path - cache is always up to date
        let inner = self.inner.read();
        Self::reservoir_sample_except(&inner.eager_vec, exclude, count)
    }

    /// Reservoir sampling to select `count` random items, excluding one.
    /// O(N) scan but only O(count) allocations.
    fn reservoir_sample_except(items: &[I], exclude: &I, count: usize) -> Vec<I> {
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
    pub fn random_lazy_except(&self, exclude: &I, count: usize) -> Vec<I> {
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
        self.inner.read().known_peers.len()
    }

    /// Clear all peers.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.eager.clear();
        inner.lazy.clear();
        inner.eager_vec.clear();
        inner.lazy_vec.clear();
        inner.known_peers.clear();
        inner.ring_neighbors.clear();
        inner.cached_ring = None;
    }

    /// Get statistics about peer state.
    pub fn stats(&self) -> PeerStats {
        let inner = self.inner.read();
        PeerStats {
            eager_count: inner.eager.len(),
            lazy_count: inner.lazy.len(),
        }
    }

    /// Check if topology repair is needed.
    ///
    /// Returns true if the eager peer count is below the target and there
    /// are lazy peers available for promotion.
    pub fn needs_repair(&self, target_eager: usize) -> bool {
        let inner = self.inner.read();
        inner.eager.len() < target_eager && !inner.lazy.is_empty()
    }

    /// Try to rebalance using try_write to avoid blocking.
    ///
    /// Returns true if rebalance was performed, false if lock was contended.
    /// This is useful for background maintenance tasks that should not block
    /// message forwarding operations.
    ///
    /// If `local_id` is set, uses hash ring-based selection for promotions.
    pub fn try_rebalance(&self, target_eager: usize) -> bool {
        self.try_rebalance_with_scorer(target_eager, |_| 0.5)
    }

    /// Try to rebalance using try_write with a custom scoring function.
    ///
    /// The `scorer` closure should return a value between 0.0 (worst) and 1.0 (best).
    /// This allows combining topological preference with network performance metrics.
    ///
    /// Returns true if rebalance was performed, false if lock was contended.
    pub fn try_rebalance_with_scorer<F>(&self, target_eager: usize, scorer: F) -> bool
    where
        F: Fn(&I) -> f64,
    {
        if let Some(mut inner) = self.inner.try_write() {
            let current_eager = inner.eager.len();

            if current_eager < target_eager && !inner.lazy.is_empty() {
                // Promote lazy peers to eager
                let promote_count = (target_eager - current_eager).min(inner.lazy.len());
                let to_promote =
                    self.select_peers_for_promotion_scored(&inner, promote_count, &scorer);

                for peer in to_promote {
                    inner.lazy.remove(&peer);
                    inner.eager.insert(peer.clone());
                    inner.lazy_vec.retain(|p| *p != peer);
                    inner.eager_vec.push(peer);
                }
            }
            true
        } else {
            false
        }
    }

    /// Select lazy peers to promote using hybrid scoring.
    ///
    /// Priorities:
    /// 1. Ring Neighbors (structural requirement - always promoted first)
    /// 2. Hybrid Score = (Topology proximity × weight) + (Network score × weight)
    ///
    /// The `scorer` closure provides the network performance score (0.0-1.0).
    fn select_peers_for_promotion_scored<F>(
        &self,
        inner: &PeerStateInner<I>,
        count: usize,
        scorer: &F,
    ) -> Vec<I>
    where
        F: Fn(&I) -> f64,
    {
        if count == 0 || inner.lazy.is_empty() {
            return Vec::new();
        }

        let mut selected = Vec::with_capacity(count);

        // 1. First, prioritize lazy peers that ARE ring neighbors (structural requirement)
        // These should always be promoted to maintain Z≥2 redundancy
        // Only applies when hash ring is enabled
        if self.local_id.is_some() && self.use_hash_ring {
            for peer in inner.lazy.iter() {
                if selected.len() >= count {
                    break;
                }
                if inner.ring_neighbors.contains(peer) && !selected.contains(peer) {
                    selected.push(peer.clone());
                }
            }
        }

        if selected.len() >= count {
            return selected;
        }

        // 2. For remaining slots, use hybrid scoring (topology + network performance)
        let remaining_needed = count - selected.len();

        // Collect remaining candidates (lazy peers not yet selected)
        let candidates: Vec<I> = inner
            .lazy
            .iter()
            .filter(|p| !selected.contains(p))
            .cloned()
            .collect();

        if candidates.is_empty() {
            return selected;
        }

        // Calculate hybrid scores and sort (highest score = best candidate)
        let mut scored: Vec<(f64, I)> = candidates
            .into_iter()
            .map(|peer| {
                let score = self.calculate_hybrid_score(&peer, inner, scorer);
                (score, peer)
            })
            .collect();

        // Sort by hybrid score descending (best first)
        // For tie-breaking: use a hash that includes local_id to give each node
        // a different ordering. This prevents all nodes from selecting the same
        // set of peers when scores are equal.
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    // Use stable_hash with local_id to create per-node ordering
                    let hash_a = self.peer_order_hash(&a.1, inner);
                    let hash_b = self.peer_order_hash(&b.1, inner);
                    hash_a.cmp(&hash_b)
                })
        });

        selected.extend(scored.into_iter().take(remaining_needed).map(|(_, p)| p));
        selected
    }

    /// Rebalance peers to match target fanout values.
    ///
    /// Promotes or demotes peers as needed to reach target eager count.
    /// When `local_id` is set, uses hash ring-based selection for promotions
    /// to maintain optimal topology.
    ///
    /// **Note**: Ring neighbors are protected and will not be demoted.
    pub fn rebalance(&self, target_eager: usize) {
        self.rebalance_with_scorer(target_eager, |_| 0.5)
    }

    /// Rebalance peers using a custom scoring function for network-aware optimization.
    ///
    /// The `scorer` closure should return a value between 0.0 (worst) and 1.0 (best).
    /// This allows combining topological preference with network performance metrics
    /// (e.g., RTT, reliability) from `PeerScoring`.
    ///
    /// # Hybrid Scoring
    ///
    /// Peers are scored using a weighted combination:
    /// ```text
    /// hybrid_score = (topology_score × 0.4) + (network_score × 0.6)
    /// ```
    ///
    /// - **Topology score**: Based on hash ring distance (closer = higher)
    /// - **Network score**: From the `scorer` closure (lower RTT/higher reliability = higher)
    ///
    /// # Safety Guarantees
    ///
    /// - Ring neighbors (i±1, i±2) are **never demoted** regardless of network score
    /// - Ring neighbors in lazy set are **always promoted first**
    /// - This preserves Z≥2 redundancy while optimizing performance
    ///
    /// # Example
    ///
    /// ```ignore
    /// use memberlist_plumtree::PeerScoring;
    ///
    /// let peer_scoring = PeerScoring::default();
    /// // ... record RTT samples ...
    ///
    /// // Rebalance using network performance scores
    /// peer_state.rebalance_with_scorer(3, |peer| {
    ///     peer_scoring.normalized_score(peer, 0.5)
    /// });
    /// ```
    pub fn rebalance_with_scorer<F>(&self, target_eager: usize, scorer: F)
    where
        F: Fn(&I) -> f64,
    {
        let mut inner = self.inner.write();

        let current_eager = inner.eager.len();

        if current_eager < target_eager {
            // Need to promote some lazy peers to eager
            let promote_count = target_eager - current_eager;
            let to_promote =
                self.select_peers_for_promotion_scored(&inner, promote_count, &scorer);

            for peer in to_promote {
                inner.lazy.remove(&peer);
                inner.eager.insert(peer.clone());
                // Update caches
                inner.lazy_vec.retain(|p| *p != peer);
                inner.eager_vec.push(peer);
            }
        } else if current_eager > target_eager {
            // Need to demote some eager peers to lazy
            // Ring neighbors are protected
            let demote_count = current_eager - target_eager;
            let to_demote = self.select_peers_for_demotion_scored(&inner, demote_count, &scorer);

            for peer in to_demote {
                inner.eager.remove(&peer);
                inner.lazy.insert(peer.clone());
                // Update caches
                inner.eager_vec.retain(|p| *p != peer);
                inner.lazy_vec.push(peer);
            }
        }
    }

    /// Select eager peers to demote using hybrid scoring.
    ///
    /// Peers with the LOWEST hybrid score are demoted first.
    /// Ring neighbors are always protected.
    fn select_peers_for_demotion_scored<F>(
        &self,
        inner: &PeerStateInner<I>,
        count: usize,
        scorer: &F,
    ) -> Vec<I>
    where
        F: Fn(&I) -> f64,
    {
        if count == 0 || inner.eager.is_empty() {
            return Vec::new();
        }

        // Filter out protected ring neighbors (strict rule - never demoted)
        let demotable: Vec<I> = inner
            .eager
            .iter()
            .filter(|p| !inner.ring_neighbors.contains(*p))
            .cloned()
            .collect();

        if demotable.is_empty() {
            return Vec::new();
        }

        // Calculate hybrid scores and sort (lowest score = worst candidate = demote first)
        let mut scored: Vec<(f64, I)> = demotable
            .into_iter()
            .map(|peer| {
                let score = self.calculate_hybrid_score(&peer, inner, scorer);
                (score, peer)
            })
            .collect();

        // Sort by hybrid score ascending (worst first), then by peer ID for stability
        scored.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });

        scored.into_iter().take(count).map(|(_, p)| p).collect()
    }

    /// Calculate hybrid score combining topology proximity and network performance.
    ///
    /// Score = (topology_score × WEIGHT_TOPO) + (network_score × WEIGHT_NET)
    ///
    /// - topology_score: 1.0 for immediate neighbor, decays with ring distance
    /// - network_score: From scorer closure (0.0-1.0, higher = better RTT/reliability)
    fn calculate_hybrid_score<F>(&self, peer: &I, inner: &PeerStateInner<I>, scorer: &F) -> f64
    where
        F: Fn(&I) -> f64,
    {
        // Weight configuration: Performance slightly more important than topology
        // for non-ring-neighbor peers (ring neighbors are handled separately)
        const WEIGHT_TOPO: f64 = 0.4;
        const WEIGHT_NET: f64 = 0.6;

        // 1. Get network performance score (0.0 to 1.0)
        let net_score = scorer(peer).clamp(0.0, 1.0);

        // 2. Get topological score based on ring distance
        let topo_score = if let Some(dist) = self.hash_ring_distance_internal(peer, inner) {
            let ring_size = inner.known_peers.len() + 1;
            if ring_size <= 1 {
                0.5
            } else {
                // Normalize: 1.0 for immediate neighbor (dist=1), decaying for further
                // Using inverse linear: 1.0 - (dist / (ring_size / 2))
                // Clamped to [0.0, 1.0]
                let max_dist = ring_size / 2;
                if max_dist == 0 {
                    1.0
                } else {
                    (1.0 - (dist as f64 / max_dist as f64)).clamp(0.0, 1.0)
                }
            }
        } else {
            0.5 // Default if no topology info
        };

        // 3. Weighted combination
        (topo_score * WEIGHT_TOPO) + (net_score * WEIGHT_NET)
    }

    /// Generate a deterministic ordering hash for a peer, unique per local_id.
    ///
    /// This ensures each node gets a different but deterministic ordering of peers
    /// when scores are equal, preventing all nodes from selecting the same subset.
    fn peer_order_hash(&self, peer: &I, inner: &PeerStateInner<I>) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        // Include local_id if available for per-node diversity
        if let Some(ref local_id) = self.local_id {
            local_id.hash(&mut hasher);
        } else {
            // Fallback: use a hash of known_peers count as entropy
            inner.known_peers.len().hash(&mut hasher);
        }

        // Include the peer itself
        peer.hash(&mut hasher);

        hasher.finish()
    }

    /// Calculate hash ring distance without acquiring read lock (for use when holding write lock).
    fn hash_ring_distance_internal(&self, peer: &I, inner: &PeerStateInner<I>) -> Option<usize> {
        let local_id = self.local_id.as_ref()?;

        if !inner.known_peers.contains(peer) {
            return None;
        }

        let sorted_ring = self.get_or_compute_ring(inner);
        let ring_size = sorted_ring.len();

        let local_pos = find_position_in_ring(&sorted_ring, local_id)?;
        let peer_pos = find_position_in_ring(&sorted_ring, peer)?;

        // Calculate minimum distance (can go either direction on ring)
        let forward = if peer_pos >= local_pos {
            peer_pos - local_pos
        } else {
            ring_size - local_pos + peer_pos
        };
        let backward = ring_size - forward;

        Some(forward.min(backward))
    }

    /// Promote the nearest lazy peer on the hash ring to eager.
    ///
    /// This method is used for topology repair when an eager peer fails.
    /// It selects the lazy peer closest to the local node on the hash ring
    /// to quickly restore connectivity.
    ///
    /// Returns the promoted peer if successful, None if no lazy peers available
    /// or local_id is not set (falls back to arbitrary promotion).
    pub fn promote_nearest_lazy(&self) -> Option<I> {
        let mut inner = self.inner.write();

        if inner.lazy.is_empty() {
            return None;
        }

        let peer_to_promote = if let Some(local_id) = &self.local_id {
            let sorted_ring = self.get_or_compute_ring(&inner);
            let ring_size = sorted_ring.len();

            if let Some(local_pos) = find_position_in_ring(&sorted_ring, local_id) {
                // Find nearest lazy peer
                let mut nearest: Option<I> = None;

                for offset in 1..ring_size {
                    // Check predecessor
                    let pred_pos = (local_pos + ring_size - offset) % ring_size;
                    let pred = &sorted_ring[pred_pos];
                    if inner.lazy.contains(pred) {
                        nearest = Some(pred.clone());
                        break;
                    }

                    // Check successor
                    let succ_pos = (local_pos + offset) % ring_size;
                    let succ = &sorted_ring[succ_pos];
                    if inner.lazy.contains(succ) {
                        nearest = Some(succ.clone());
                        break;
                    }
                }

                nearest
            } else {
                // Fallback: take any lazy peer
                inner.lazy.iter().next().cloned()
            }
        } else {
            // No local_id: take any lazy peer
            inner.lazy.iter().next().cloned()
        };

        if let Some(ref peer) = peer_to_promote {
            inner.lazy.remove(peer);
            inner.lazy_vec.retain(|p| p != peer);
            inner.eager.insert(peer.clone());
            inner.eager_vec.push(peer.clone());
        }

        peer_to_promote
    }

    /// Get the hash ring distance from local node to a peer.
    ///
    /// Returns the minimum number of hops on the hash ring between the local
    /// node and the given peer. Returns None if local_id is not set or peer
    /// is not known.
    ///
    /// This can be used to prioritize topology decisions.
    pub fn hash_ring_distance(&self, peer: &I) -> Option<usize> {
        let local_id = self.local_id.as_ref()?;
        let inner = self.inner.read();

        if !inner.known_peers.contains(peer) {
            return None;
        }

        let sorted_ring = self.get_or_compute_ring(&inner);
        let ring_size = sorted_ring.len();

        let local_pos = find_position_in_ring(&sorted_ring, local_id)?;
        let peer_pos = find_position_in_ring(&sorted_ring, peer)?;

        // Calculate minimum distance (can go either direction on ring)
        let forward = if peer_pos >= local_pos {
            peer_pos - local_pos
        } else {
            ring_size - local_pos + peer_pos
        };
        let backward = ring_size - forward;

        Some(forward.min(backward))
    }
}

impl<I: Clone + Eq + Hash + Ord> Default for PeerState<I> {
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
///
/// # Example
///
/// ```
/// use memberlist_plumtree::PeerStateBuilder;
///
/// // Without hash ring topology (random peer selection)
/// let state = PeerStateBuilder::<u64>::new()
///     .with_max_peers(100)
///     .with_eager_fanout(3)
///     .with_lazy_fanout(3)
///     .build();
///
/// // With hash ring topology (deterministic peer selection)
/// let state_with_ring = PeerStateBuilder::new()
///     .with_local_id(0u64)
///     .with_peers(1..100u64)
///     .with_max_peers(100)
///     .with_eager_fanout(3)
///     .with_lazy_fanout(3)
///     .build();
/// ```
#[derive(Debug)]
pub struct PeerStateBuilder<I> {
    local_id: Option<I>,
    peers: Vec<I>,
    max_peers: Option<usize>,
    eager_fanout: usize,
    lazy_fanout: usize,
    use_hash_ring: bool,
}

impl<I: Clone + Eq + Hash + Ord> PeerStateBuilder<I> {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            local_id: None,
            peers: Vec::new(),
            max_peers: None,
            eager_fanout: 3,
            lazy_fanout: 6,
            use_hash_ring: false,
        }
    }

    /// Set the local node ID.
    ///
    /// This enables hash ring topology features:
    /// - Deterministic peer selection based on hash ring position
    /// - Adjacent and jump link connections
    /// - Ring neighbor protection (Z≥2 guarantee)
    /// - Fingerprint-based eviction that preserves topological diversity
    ///
    /// When building with `use_hash_ring(true)`, the initial peer classification
    /// will use hash ring-based selection (adjacent + jump links).
    pub fn with_local_id(mut self, local_id: I) -> Self {
        self.local_id = Some(local_id);
        self
    }

    /// Enable or disable hash ring-based initial peer classification.
    ///
    /// When enabled and `local_id` is set, initial peers will be classified
    /// using hash ring topology (adjacent links + jump links as eager peers).
    ///
    /// Default is false (random classification).
    pub fn use_hash_ring(mut self, use_hash_ring: bool) -> Self {
        self.use_hash_ring = use_hash_ring;
        self
    }

    /// Add peers to initialize with.
    pub fn with_peers(mut self, peers: impl IntoIterator<Item = I>) -> Self {
        self.peers.extend(peers);
        self
    }

    /// Set the maximum number of peers.
    ///
    /// When set, limits the number of peers that can be added.
    /// Peers beyond this limit will be rejected.
    pub fn with_max_peers(mut self, max_peers: usize) -> Self {
        self.max_peers = Some(max_peers);
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
    ///
    /// If `max_peers` is set and fewer peers are provided than the limit,
    /// all provided peers are added with auto-classification (eager first).
    /// If more peers are provided than `max_peers`, only the first `max_peers`
    /// are added.
    ///
    /// When `local_id` is set and `use_hash_ring` is true, uses hash ring-based
    /// initial peer classification for optimal topology.
    pub fn build(self) -> PeerState<I> {
        let limit = self.max_peers.unwrap_or(usize::MAX);
        let peers: Vec<I> = self.peers.into_iter().take(limit).collect();

        // Use hash ring topology if local_id is set and use_hash_ring is enabled
        if let Some(local_id) = self.local_id {
            if self.use_hash_ring {
                return PeerState::with_initial_peers_hash_ring(
                    local_id,
                    peers,
                    self.eager_fanout,
                    self.lazy_fanout,
                );
            }

            // local_id set but not using hash ring for initial classification
            // This gives us diverse peer ordering without ring neighbor protection
            let state = PeerState {
                inner: RwLock::new(PeerStateInner {
                    eager: HashSet::new(),
                    lazy: HashSet::new(),
                    eager_vec: Vec::new(),
                    lazy_vec: Vec::new(),
                    known_peers: HashSet::new(),
                    ring_neighbors: HashSet::new(),
                    cached_ring: None,
                }),
                local_id: Some(local_id),
                use_hash_ring: false, // No ring neighbor protection
            };
            for peer in peers {
                state.add_peer_auto(peer, self.max_peers, self.eager_fanout);
            }
            return state;
        }

        // No local_id: use standard initialization
        let state = PeerState::new();
        for peer in peers {
            state.add_peer_auto(peer, self.max_peers, self.eager_fanout);
        }
        state
    }
}

impl<I: Clone + Eq + Hash + Ord> Default for PeerStateBuilder<I> {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapper for thread-safe peer state sharing.
pub type SharedPeerState<I> = Arc<PeerState<I>>;
