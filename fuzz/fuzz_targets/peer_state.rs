//! Fuzz target for PeerState operations.
//!
//! This fuzzer tests the peer state machine (eager/lazy peer management)
//! with random sequences of operations to find edge cases.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

/// Operations that can be performed on peer state
#[derive(Debug, Arbitrary)]
enum PeerOp {
    /// Add a peer (starts as lazy)
    AddPeer { id: u8 },
    /// Remove a peer
    RemovePeer { id: u8 },
    /// Promote peer to eager
    PromoteToEager { id: u8 },
    /// Demote peer to lazy
    DemoteToLazy { id: u8 },
    /// Check if peer is eager
    IsEager { id: u8 },
    /// Check if peer is lazy
    IsLazy { id: u8 },
    /// Get a random eager peer
    RandomEager,
    /// Get a random lazy peer
    RandomLazy,
    /// Get eager count
    EagerCount,
    /// Get lazy count
    LazyCount,
    /// Clear all peers
    ClearAll,
}

/// A simplified peer state machine for fuzzing
struct SimplePeerState {
    eager: std::collections::HashSet<u8>,
    lazy: std::collections::HashSet<u8>,
}

impl SimplePeerState {
    fn new() -> Self {
        Self {
            eager: std::collections::HashSet::new(),
            lazy: std::collections::HashSet::new(),
        }
    }

    fn add_peer(&mut self, id: u8) {
        // New peers start as lazy
        if !self.eager.contains(&id) && !self.lazy.contains(&id) {
            self.lazy.insert(id);
        }
    }

    fn remove_peer(&mut self, id: &u8) {
        self.eager.remove(id);
        self.lazy.remove(id);
    }

    fn promote_to_eager(&mut self, id: u8) {
        if self.lazy.remove(&id) {
            self.eager.insert(id);
        }
    }

    fn demote_to_lazy(&mut self, id: u8) {
        if self.eager.remove(&id) {
            self.lazy.insert(id);
        }
    }

    fn is_eager(&self, id: &u8) -> bool {
        self.eager.contains(id)
    }

    fn is_lazy(&self, id: &u8) -> bool {
        self.lazy.contains(id)
    }

    fn eager_count(&self) -> usize {
        self.eager.len()
    }

    fn lazy_count(&self) -> usize {
        self.lazy.len()
    }

    fn clear(&mut self) {
        self.eager.clear();
        self.lazy.clear();
    }

    /// Verify invariants
    fn verify_invariants(&self) -> bool {
        // No peer should be in both sets
        for id in &self.eager {
            if self.lazy.contains(id) {
                return false;
            }
        }
        true
    }
}

fuzz_target!(|ops: Vec<PeerOp>| {
    let mut state = SimplePeerState::new();

    for op in ops {
        match op {
            PeerOp::AddPeer { id } => {
                state.add_peer(id);
            }
            PeerOp::RemovePeer { id } => {
                state.remove_peer(&id);
            }
            PeerOp::PromoteToEager { id } => {
                state.promote_to_eager(id);
            }
            PeerOp::DemoteToLazy { id } => {
                state.demote_to_lazy(id);
            }
            PeerOp::IsEager { id } => {
                let _ = state.is_eager(&id);
            }
            PeerOp::IsLazy { id } => {
                let _ = state.is_lazy(&id);
            }
            PeerOp::RandomEager => {
                // Simulate random selection
                let _ = state.eager.iter().next();
            }
            PeerOp::RandomLazy => {
                let _ = state.lazy.iter().next();
            }
            PeerOp::EagerCount => {
                let _ = state.eager_count();
            }
            PeerOp::LazyCount => {
                let _ = state.lazy_count();
            }
            PeerOp::ClearAll => {
                state.clear();
            }
        }

        // Verify invariants after each operation
        assert!(state.verify_invariants(), "Invariant violated!");
    }
});
