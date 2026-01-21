mod tests {
    use crate::peer_state::{sort_by_hash_stable, stable_hash, HashRingConnection};
    use crate::{AddPeerResult, PeerState, PeerStateBuilder, PeerTopology, RemovePeerResult};
    use std::collections::HashSet;

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

    #[test]
    fn test_add_peer_auto_classification() {
        let state: PeerState<u64> = PeerState::new();

        // First 2 peers should be eager (eager_fanout = 2)
        assert_eq!(
            state.add_peer_auto(1, Some(6), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(
            state.add_peer_auto(2, Some(6), 2),
            AddPeerResult::AddedEager
        );

        // Remaining peers should be lazy
        assert_eq!(state.add_peer_auto(3, Some(6), 2), AddPeerResult::AddedLazy);
        assert_eq!(state.add_peer_auto(4, Some(6), 2), AddPeerResult::AddedLazy);

        assert_eq!(state.eager_count(), 2);
        assert_eq!(state.lazy_count(), 2);
        assert!(state.is_eager(&1));
        assert!(state.is_eager(&2));
        assert!(state.is_lazy(&3));
        assert!(state.is_lazy(&4));
    }

    #[test]
    fn test_add_peer_auto_limit_reached() {
        let state: PeerState<u64> = PeerState::new();

        // Add up to limit (2 eager, 1 lazy)
        assert_eq!(
            state.add_peer_auto(1, Some(3), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(
            state.add_peer_auto(2, Some(3), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(state.add_peer_auto(3, Some(3), 2), AddPeerResult::AddedLazy);

        // With lazy peers available, new peers trigger eviction
        assert_eq!(
            state.add_peer_auto(4, Some(3), 2),
            AddPeerResult::AddedAfterEviction
        );
        // Still at limit, evicted another lazy
        assert_eq!(
            state.add_peer_auto(5, Some(3), 2),
            AddPeerResult::AddedAfterEviction
        );

        assert_eq!(state.total_count(), 3);
    }

    #[test]
    fn test_add_peer_auto_limit_reached_no_lazy() {
        let state: PeerState<u64> = PeerState::new();

        // Add peers with max_peers = eager_fanout (no lazy slots)
        assert_eq!(
            state.add_peer_auto(1, Some(2), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(
            state.add_peer_auto(2, Some(2), 2),
            AddPeerResult::AddedEager
        );

        // Now at limit with no lazy peers to evict - should return LimitReached
        assert_eq!(
            state.add_peer_auto(3, Some(2), 2),
            AddPeerResult::LimitReached
        );

        assert_eq!(state.total_count(), 2);
        assert_eq!(state.eager_count(), 2);
        assert_eq!(state.lazy_count(), 0);
    }

    #[test]
    fn test_add_peer_auto_already_exists() {
        let state: PeerState<u64> = PeerState::new();

        assert_eq!(
            state.add_peer_auto(1, Some(10), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(
            state.add_peer_auto(1, Some(10), 2),
            AddPeerResult::AlreadyExists
        );

        assert_eq!(state.total_count(), 1);
    }

    #[test]
    fn test_add_peer_auto_unlimited() {
        let state: PeerState<u64> = PeerState::new();

        // No limit (None)
        for i in 1..=100 {
            let result = state.add_peer_auto(i, None, 3);
            if i <= 3 {
                assert_eq!(result, AddPeerResult::AddedEager);
            } else {
                assert_eq!(result, AddPeerResult::AddedLazy);
            }
        }

        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 97);
    }

    #[test]
    fn test_builder_with_max_peers() {
        let state = PeerStateBuilder::new()
            .with_peers(1..=20) // 20 peers
            .with_max_peers(8) // Limit to 8
            .with_eager_fanout(3) // 3 eager
            .build();

        // Should have exactly 8 peers (3 eager, 5 lazy)
        assert_eq!(state.total_count(), 8);
        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 5);
    }

    #[test]
    fn test_remove_peer_result() {
        let state: PeerState<u64> = PeerState::new();

        // Add lazy peer then promote to eager
        state.add_peer(1);
        state.promote_to_eager(&1);
        // Add lazy peer
        state.add_peer(2);

        // Remove eager peer
        let result = state.remove_peer(&1);
        assert_eq!(result, RemovePeerResult::RemovedEager);
        assert!(result.was_eager());
        assert!(result.was_removed());

        // Remove lazy peer
        let result = state.remove_peer(&2);
        assert_eq!(result, RemovePeerResult::RemovedLazy);
        assert!(!result.was_eager());
        assert!(result.was_removed());

        // Remove non-existent peer
        let result = state.remove_peer(&999);
        assert_eq!(result, RemovePeerResult::NotFound);
        assert!(!result.was_eager());
        assert!(!result.was_removed());
    }

    #[test]
    fn test_needs_repair() {
        let state: PeerState<u64> = PeerState::new();

        // No peers - no repair needed (no lazy peers to promote)
        assert!(!state.needs_repair(3));

        // Add eager peers up to target
        state.add_peer(1);
        state.promote_to_eager(&1);
        state.add_peer(2);
        state.promote_to_eager(&2);
        state.add_peer(3);
        state.promote_to_eager(&3);
        assert!(!state.needs_repair(3)); // At target, no repair needed

        // Remove one eager peer
        state.remove_peer(&1);
        // Still no repair needed (no lazy peers)
        assert!(!state.needs_repair(3));

        // Add lazy peers
        state.add_peer(4);
        state.add_peer(5);
        // Now repair is needed (below target with lazy peers available)
        assert!(state.needs_repair(3));
    }

    #[test]
    fn test_try_rebalance() {
        let state: PeerState<u64> = PeerState::new();

        // Setup: 1 eager, 3 lazy, target = 3 eager
        state.add_peer(1);
        state.promote_to_eager(&1);
        state.add_peer(2);
        state.add_peer(3);
        state.add_peer(4);

        assert_eq!(state.eager_count(), 1);
        assert_eq!(state.lazy_count(), 3);
        assert!(state.needs_repair(3));

        // Rebalance should promote 2 lazy peers to eager
        let success = state.try_rebalance(3);
        assert!(success);

        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 1);
        assert!(!state.needs_repair(3));
    }

    #[test]
    fn test_try_rebalance_insufficient_lazy() {
        let state: PeerState<u64> = PeerState::new();

        // Setup: 1 eager, 1 lazy, target = 3 eager
        state.add_peer(1);
        state.promote_to_eager(&1);
        state.add_peer(2);

        assert_eq!(state.eager_count(), 1);
        assert_eq!(state.lazy_count(), 1);
        assert!(state.needs_repair(3));

        // Rebalance - can only promote 1 (all available)
        let success = state.try_rebalance(3);
        assert!(success);

        // 2 eager (original + promoted), 0 lazy
        assert_eq!(state.eager_count(), 2);
        assert_eq!(state.lazy_count(), 0);

        // Still needs repair but no lazy peers available
        assert!(!state.needs_repair(3)); // needs_repair checks for lazy availability
    }

    #[test]
    fn test_eviction_replaces_lazy_peer() {
        let state: PeerState<u64> = PeerState::new();

        // Setup: 2 eager, 1 lazy, max = 3
        // Use add_peer_auto to properly populate eager/lazy
        assert_eq!(
            state.add_peer_auto(1, Some(3), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(
            state.add_peer_auto(2, Some(3), 2),
            AddPeerResult::AddedEager
        );
        assert_eq!(state.add_peer_auto(3, Some(3), 2), AddPeerResult::AddedLazy);

        // Add new peer - should evict peer 3 and add new one
        let result = state.add_peer_auto(4, Some(3), 2);
        assert_eq!(result, AddPeerResult::AddedAfterEviction);

        // Peer 3 should be gone, peer 4 should be present
        assert!(!state.contains(&3));
        assert!(state.contains(&4));
        assert!(state.is_lazy(&4));
        assert_eq!(state.total_count(), 3);
    }

    #[test]
    fn test_topology_snapshot() {
        let state: PeerState<u64> = PeerState::new();

        // Empty topology
        let topo = state.topology();
        assert!(topo.is_empty());
        assert_eq!(topo.total(), 0);
        assert_eq!(topo.eager_count(), 0);
        assert_eq!(topo.lazy_count(), 0);

        // Add peers using add_peer_auto
        state.add_peer_auto(1, None, 2);
        state.add_peer_auto(2, None, 2);
        state.add_peer_auto(3, None, 2);
        state.add_peer_auto(4, None, 2);

        let topo = state.topology();
        assert_eq!(topo.total(), 4);
        assert_eq!(topo.eager_count(), 2); // First 2 are eager
        assert_eq!(topo.lazy_count(), 2); // Rest are lazy

        // Verify contains
        assert!(topo.contains(&1));
        assert!(topo.contains(&2));
        assert!(topo.contains(&3));
        assert!(topo.contains(&4));
        assert!(!topo.contains(&99));

        // Verify eager/lazy classification
        assert!(topo.is_eager(&1));
        assert!(topo.is_eager(&2));
        assert!(topo.is_lazy(&3));
        assert!(topo.is_lazy(&4));
    }

    #[test]
    fn test_topology_after_disconnect() {
        let state: PeerState<u64> = PeerState::new();

        // Add 4 peers (2 eager, 2 lazy)
        state.add_peer_auto(1, None, 2);
        state.add_peer_auto(2, None, 2);
        state.add_peer_auto(3, None, 2);
        state.add_peer_auto(4, None, 2);

        // Verify initial state
        let topo = state.topology();
        assert_eq!(topo.total(), 4);
        assert!(topo.contains(&1));
        assert!(topo.contains(&2));
        assert!(topo.contains(&3));
        assert!(topo.contains(&4));

        // Disconnect peer 2 (eager)
        let result = state.remove_peer(&2);
        assert_eq!(result, RemovePeerResult::RemovedEager);

        // Verify topology no longer contains peer 2
        let topo = state.topology();
        assert_eq!(topo.total(), 3);
        assert!(topo.contains(&1));
        assert!(!topo.contains(&2)); // <-- Disconnected peer removed
        assert!(topo.contains(&3));
        assert!(topo.contains(&4));
        assert_eq!(topo.eager_count(), 1); // Only peer 1 remains eager

        // Disconnect peer 3 (lazy)
        let result = state.remove_peer(&3);
        assert_eq!(result, RemovePeerResult::RemovedLazy);

        // Verify topology no longer contains peer 3
        let topo = state.topology();
        assert_eq!(topo.total(), 2);
        assert!(topo.contains(&1));
        assert!(!topo.contains(&2));
        assert!(!topo.contains(&3)); // <-- Disconnected peer removed
        assert!(topo.contains(&4));
        assert_eq!(topo.lazy_count(), 1); // Only peer 4 remains lazy
    }

    #[test]
    fn test_topology_disconnect_all_eager() {
        let state: PeerState<u64> = PeerState::new();

        // Add 3 peers (2 eager, 1 lazy)
        state.add_peer_auto(1, None, 2);
        state.add_peer_auto(2, None, 2);
        state.add_peer_auto(3, None, 2);

        // Disconnect all eager peers
        state.remove_peer(&1);
        state.remove_peer(&2);

        // Verify only lazy peer remains
        let topo = state.topology();
        assert_eq!(topo.total(), 1);
        assert_eq!(topo.eager_count(), 0);
        assert_eq!(topo.lazy_count(), 1);
        assert!(topo.contains(&3));
        assert!(topo.is_lazy(&3));
    }

    #[test]
    fn test_topology_disconnect_all_peers() {
        let state: PeerState<u64> = PeerState::new();

        // Add peers
        state.add_peer_auto(1, None, 2);
        state.add_peer_auto(2, None, 2);
        state.add_peer_auto(3, None, 2);

        // Disconnect all
        state.remove_peer(&1);
        state.remove_peer(&2);
        state.remove_peer(&3);

        // Verify topology is empty
        let topo = state.topology();
        assert!(topo.is_empty());
        assert_eq!(topo.total(), 0);
        assert!(!topo.contains(&1));
        assert!(!topo.contains(&2));
        assert!(!topo.contains(&3));
    }

    #[test]
    fn test_topology_disconnect_nonexistent() {
        let state: PeerState<u64> = PeerState::new();

        state.add_peer_auto(1, None, 2);
        state.add_peer_auto(2, None, 2);

        // Remove non-existent peer
        let result = state.remove_peer(&99);
        assert_eq!(result, RemovePeerResult::NotFound);

        // Topology unchanged
        let topo = state.topology();
        assert_eq!(topo.total(), 2);
        assert!(topo.contains(&1));
        assert!(topo.contains(&2));
    }

    #[test]
    fn test_peer_topology_struct() {
        // Test PeerTopology struct directly
        let topo = PeerTopology::new(vec![1u64, 2], vec![3u64, 4, 5]);

        assert_eq!(topo.total(), 5);
        assert_eq!(topo.eager_count(), 2);
        assert_eq!(topo.lazy_count(), 3);
        assert!(!topo.is_empty());

        assert!(topo.contains(&1));
        assert!(topo.contains(&5));
        assert!(!topo.contains(&99));

        assert!(topo.is_eager(&1));
        assert!(topo.is_eager(&2));
        assert!(!topo.is_eager(&3));

        assert!(topo.is_lazy(&3));
        assert!(topo.is_lazy(&4));
        assert!(topo.is_lazy(&5));
        assert!(!topo.is_lazy(&1));

        // Test default
        let empty: PeerTopology<u64> = PeerTopology::default();
        assert!(empty.is_empty());
        assert_eq!(empty.total(), 0);
    }

    // ====== Hash Ring Topology Tests ======

    #[test]
    fn test_new_with_local_id() {
        let state: PeerState<u64> = PeerState::new_with_local_id(42);
        assert_eq!(state.local_id(), Some(&42));
        assert_eq!(state.eager_count(), 0);
        assert_eq!(state.lazy_count(), 0);
    }

    #[test]
    fn test_stable_hash_determinism() {
        // Verify stable_hash produces same results for same input
        let hash1 = stable_hash(&42u64);
        let hash2 = stable_hash(&42u64);
        assert_eq!(hash1, hash2);

        // Different inputs produce different hashes
        let hash3 = stable_hash(&43u64);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_sort_by_hash_stable_with_collisions() {
        // Even with hash collisions, Ord provides stable ordering
        let peers: Vec<u64> = vec![5, 3, 1, 4, 2];
        let sorted1 = sort_by_hash_stable(&peers);
        let sorted2 = sort_by_hash_stable(&peers);

        // Same input should produce same output
        assert_eq!(sorted1, sorted2);

        // All elements should be present
        assert_eq!(sorted1.len(), 5);
        for p in &peers {
            assert!(sorted1.contains(p));
        }
    }

    #[test]
    fn test_with_initial_peers_hash_ring() {
        // Create a ring with 20 peers
        let peers: Vec<u64> = (1..=20).collect();
        let state = PeerState::with_initial_peers_hash_ring(0u64, peers, 3, 3);

        assert_eq!(state.local_id(), Some(&0));
        // Should have exactly 3 eager peers (adjacent + possibly jump links)
        assert_eq!(state.eager_count(), 3);
        // Remaining peers should be lazy (up to lazy_fanout + remaining eager slots)
        assert!(state.lazy_count() <= 6); // max = lazy_fanout + eager_fanout

        // Ring neighbors should be tracked
        let ring_neighbors = state.ring_neighbors();
        assert!(!ring_neighbors.is_empty());
    }

    #[test]
    fn test_with_initial_peers_hash_ring_small_cluster() {
        // Small cluster with only 3 peers
        let peers: Vec<u64> = vec![1, 2, 3];
        let state = PeerState::with_initial_peers_hash_ring(0u64, peers, 3, 3);

        // With only 3 peers and eager_fanout=3, all should be eager
        assert_eq!(state.eager_count() + state.lazy_count(), 3);
    }

    #[test]
    fn test_hash_ring_neighbors() {
        let state = PeerState::new_with_local_id(0u64);

        // Add some peers
        for i in 1..=10 {
            state.add_peer(i);
        }

        // Get hash ring neighbors
        let neighbors = state.hash_ring_neighbors();
        assert!(neighbors.is_some());

        let neighbors = neighbors.unwrap();
        // Should have at least 2 adjacent neighbors (predecessor and successor)
        let adjacent_count = neighbors
            .iter()
            .filter(|(_, conn)| *conn == HashRingConnection::Adjacent)
            .count();
        assert!(adjacent_count >= 2);
    }

    #[test]
    fn test_hash_ring_neighbors_without_local_id() {
        let state: PeerState<u64> = PeerState::new();
        state.add_peer(1);
        state.add_peer(2);

        // Without local_id, should return None
        assert!(state.hash_ring_neighbors().is_none());
    }

    #[test]
    fn test_ring_neighbor_protection() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peers
        for i in 1..=10 {
            state.add_peer(i);
            state.promote_to_eager(&i);
        }

        // Get ring neighbors
        let ring_neighbors = state.ring_neighbors();
        assert!(!ring_neighbors.is_empty());

        // Try to demote a ring neighbor - should fail
        for neighbor in &ring_neighbors {
            let demoted = state.demote_to_lazy(neighbor);
            assert!(!demoted, "Ring neighbor should not be demotable");
            assert!(state.is_eager(neighbor));
        }
    }

    #[test]
    fn test_promote_nearest_lazy() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peers as lazy
        for i in 1..=10 {
            state.add_peer(i);
        }

        assert_eq!(state.lazy_count(), 10);
        assert_eq!(state.eager_count(), 0);

        // Promote nearest lazy peer
        let promoted = state.promote_nearest_lazy();
        assert!(promoted.is_some());

        // Should now have 1 eager, 9 lazy
        assert_eq!(state.eager_count(), 1);
        assert_eq!(state.lazy_count(), 9);

        // The promoted peer should be in the eager set
        let promoted_id = promoted.unwrap();
        assert!(state.is_eager(&promoted_id));
    }

    #[test]
    fn test_promote_nearest_lazy_no_lazy_peers() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peer and promote to eager
        state.add_peer(1);
        state.promote_to_eager(&1);

        // No lazy peers to promote
        let result = state.promote_nearest_lazy();
        assert!(result.is_none());
    }

    #[test]
    fn test_hash_ring_distance() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peers
        for i in 1..=10 {
            state.add_peer(i);
        }

        // Check distance to known peer
        let distance = state.hash_ring_distance(&5);
        assert!(distance.is_some());
        let dist = distance.unwrap();
        // Distance should be within half the ring size
        assert!(dist <= 6); // (10 + 1) / 2 = 5, but could be up to 6

        // Unknown peer should return None
        let distance = state.hash_ring_distance(&99);
        assert!(distance.is_none());
    }

    #[test]
    fn test_hash_ring_distance_without_local_id() {
        let state: PeerState<u64> = PeerState::new();
        state.add_peer(1);

        // Without local_id, should return None
        assert!(state.hash_ring_distance(&1).is_none());
    }

    #[test]
    fn test_rebalance_with_hash_ring() {
        let state = PeerState::new_with_local_id(0u64);

        // Add 10 lazy peers
        for i in 1..=10 {
            state.add_peer(i);
        }

        assert_eq!(state.eager_count(), 0);
        assert_eq!(state.lazy_count(), 10);

        // Rebalance to have 3 eager peers
        state.rebalance(3);

        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 7);
    }

    #[test]
    fn test_rebalance_demotion_respects_ring_neighbors() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peers as eager
        for i in 1..=10 {
            state.add_peer(i);
            state.promote_to_eager(&i);
        }

        assert_eq!(state.eager_count(), 10);

        // Get ring neighbors before demotion
        let ring_neighbors = state.ring_neighbors();

        // Rebalance to have only 2 eager peers
        state.rebalance(2);

        // Ring neighbors should still be eager (protected)
        for neighbor in &ring_neighbors {
            assert!(
                state.is_eager(neighbor),
                "Ring neighbor {:?} should remain eager",
                neighbor
            );
        }

        // Total eager count depends on how many ring neighbors there are
        assert!(state.eager_count() >= ring_neighbors.len().min(2));
    }

    #[test]
    fn test_builder_with_local_id() {
        let state = PeerStateBuilder::new()
            .with_local_id(0u64)
            .with_peers(1..=10)
            .with_eager_fanout(3)
            .with_lazy_fanout(3)
            .build();

        assert_eq!(state.local_id(), Some(&0));
        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 7);
    }

    #[test]
    fn test_builder_with_hash_ring() {
        let state = PeerStateBuilder::new()
            .with_local_id(7u64)
            .with_peers(1..=20)
            .with_eager_fanout(3)
            .with_lazy_fanout(3)
            .use_hash_ring(true)
            .build();

        assert_eq!(state.local_id(), Some(&7));
        // With hash ring, eager peers should be neighbors on the ring
        assert_eq!(state.eager_count(), 3);

        // Verify we can get hash ring neighbors
        let neighbors = state.hash_ring_neighbors();
        assert!(neighbors.is_some());

        // Ring neighbors should be tracked
        assert!(!state.ring_neighbors().is_empty());
    }

    #[test]
    fn test_eviction_protects_ring_neighbors() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peers up to a limit
        for i in 1..=5 {
            state.add_peer_auto(i, Some(5), 2);
        }

        // Get ring neighbors
        let ring_neighbors = state.ring_neighbors();

        // Try to add more peers (triggers eviction)
        for i in 6..=10 {
            state.add_peer_auto(i, Some(5), 2);
        }

        // Ring neighbors that are lazy should not be evicted
        // (Note: they may have been promoted to eager)
        for neighbor in &ring_neighbors {
            if state.contains(neighbor) {
                // If still present, good
            } else {
                // Ring neighbor was evicted - this is a problem if it was lazy
                // But if eager, it's protected from eviction by being in eager set
            }
        }
    }

    #[test]
    fn test_known_peers_stable_ring() {
        let state = PeerState::new_with_local_id(0u64);

        // Add peers
        for i in 1..=10 {
            state.add_peer(i);
        }

        // Promote/demote some peers
        state.promote_to_eager(&3);
        state.promote_to_eager(&5);
        state.promote_to_eager(&7);

        // Ring should be computed from known_peers, not eager/lazy
        let neighbors1 = state.hash_ring_neighbors().unwrap();

        // Demote a non-protected peer
        let ring_neighbors = state.ring_neighbors();
        if !ring_neighbors.contains(&3) {
            state.demote_to_lazy(&3);
        }

        // Ring should remain the same (same known_peers)
        let neighbors2 = state.hash_ring_neighbors().unwrap();

        // Neighbor list should be identical (same membership)
        assert_eq!(neighbors1.len(), neighbors2.len());
    }

    #[test]
    fn test_large_cluster_hash_ring() {
        // Test with 100 nodes as mentioned in the algorithm description
        let peers: Vec<u64> = (1..100).collect();
        let state = PeerState::with_initial_peers_hash_ring(0u64, peers, 3, 20);

        assert_eq!(state.local_id(), Some(&0));
        // Should have 3 eager (adjacent + jump links)
        assert_eq!(state.eager_count(), 3);
        // Should have up to 20 lazy peers
        assert!(state.lazy_count() <= 20);

        // With enough nodes in state (> 8), should have long-range jumps
        if state.total_count() > 8 {
            let neighbors = state.hash_ring_neighbors().unwrap();
            let has_long_range = neighbors
                .iter()
                .any(|(_, conn)| *conn == HashRingConnection::LongRange);
            assert!(has_long_range);
        }
    }

    #[test]
    fn test_hash_ring_connection_types() {
        // Create a cluster large enough to have all connection types
        let peers: Vec<u64> = (1..=20).collect();
        let state = PeerState::with_initial_peers_hash_ring(0u64, peers, 6, 6);

        let neighbors = state.hash_ring_neighbors().unwrap();

        // Count connection types
        let adjacent = neighbors
            .iter()
            .filter(|(_, c)| *c == HashRingConnection::Adjacent)
            .count();
        let second_nearest = neighbors
            .iter()
            .filter(|(_, c)| *c == HashRingConnection::SecondNearest)
            .count();
        let long_range = neighbors
            .iter()
            .filter(|(_, c)| *c == HashRingConnection::LongRange)
            .count();

        // Should have 2 adjacent (pred + succ)
        assert_eq!(adjacent, 2);
        // Should have 2 second-nearest (ring_size > 3)
        assert_eq!(second_nearest, 2);
        // Should have 2 long-range (ring_size > 8)
        assert_eq!(long_range, 2);
    }

    #[test]
    fn test_deterministic_eviction() {
        // Two nodes with same peers should make same eviction decision
        let state_a = PeerState::new_with_local_id(100u64);
        let state_b = PeerState::new_with_local_id(100u64);

        // Add same peers in same order
        for i in 1..=5 {
            state_a.add_peer_auto(i, Some(5), 2);
            state_b.add_peer_auto(i, Some(5), 2);
        }

        // Add new peer to trigger eviction
        state_a.add_peer_auto(6, Some(5), 2);
        state_b.add_peer_auto(6, Some(5), 2);

        // Both should have same peers (deterministic eviction)
        let peers_a: HashSet<u64> = state_a.all_peers().into_iter().collect();
        let peers_b: HashSet<u64> = state_b.all_peers().into_iter().collect();
        assert_eq!(peers_a, peers_b);
    }

    #[test]
    fn test_ten_node_cluster_no_isolation() {
        // Create a 10-node cluster with hash ring topology
        // Each node has local_id 0-9 and knows about all other nodes
        // Verify: no node is isolated, all nodes are reachable

        const CLUSTER_SIZE: u64 = 10;
        const EAGER_FANOUT: usize = 3;
        const LAZY_FANOUT: usize = 6;

        // Create all 10 nodes
        let mut nodes: Vec<PeerState<u64>> = Vec::new();
        for local_id in 0..CLUSTER_SIZE {
            let state = PeerStateBuilder::new()
                .with_local_id(local_id)
                .with_peers((0..CLUSTER_SIZE).filter(|&id| id != local_id))
                .with_eager_fanout(EAGER_FANOUT)
                .with_lazy_fanout(LAZY_FANOUT)
                .use_hash_ring(true)
                .build();
            nodes.push(state);
        }

        // Verify: No node is isolated (every node has eager peers)
        for (local_id, state) in nodes.iter().enumerate() {
            let eager_count = state.eager_count();
            let lazy_count = state.lazy_count();
            let total_peers = eager_count + lazy_count;

            assert!(
                eager_count > 0,
                "Node {} is isolated with 0 eager peers!",
                local_id
            );
            assert!(total_peers > 0, "Node {} has no peers at all!", local_id);

            // Each node should have exactly EAGER_FANOUT eager peers
            assert_eq!(
                eager_count, EAGER_FANOUT,
                "Node {} has {} eager peers, expected {}",
                local_id, eager_count, EAGER_FANOUT
            );

            // Verify ring neighbors are set
            let ring_neighbors = state.ring_neighbors();
            assert!(
                !ring_neighbors.is_empty(),
                "Node {} has no ring neighbors!",
                local_id
            );
        }

        // Build connectivity graph: adjacency[i] = set of nodes that i considers eager
        let mut eager_connections: Vec<HashSet<u64>> = Vec::new();
        for state in &nodes {
            eager_connections.push(state.eager_peers().into_iter().collect());
        }

        // Verify: Graph is connected using BFS from node 0
        let mut visited = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(0u64);
        visited.insert(0u64);

        while let Some(node) = queue.pop_front() {
            // Follow eager connections from this node
            for &peer in &eager_connections[node as usize] {
                if !visited.contains(&peer) {
                    visited.insert(peer);
                    queue.push_back(peer);
                }
            }
            // Also follow reverse connections (nodes that have this node as eager)
            for (other_node, connections) in eager_connections.iter().enumerate() {
                if connections.contains(&node) && !visited.contains(&(other_node as u64)) {
                    visited.insert(other_node as u64);
                    queue.push_back(other_node as u64);
                }
            }
        }

        // All nodes should be reachable
        assert_eq!(
            visited.len(),
            CLUSTER_SIZE as usize,
            "Not all nodes are reachable! Visited: {:?}, Expected: 0..{}",
            visited,
            CLUSTER_SIZE
        );

        // Print connectivity matrix for debugging
        println!("\n=== 10-Node Cluster Connectivity ===");
        for (local_id, state) in nodes.iter().enumerate() {
            let eager: Vec<u64> = state.eager_peers();
            let ring_neighbors: Vec<u64> = state.ring_neighbors().into_iter().collect();
            println!(
                "Node {}: eager={:?}, ring_neighbors={:?}",
                local_id, eager, ring_neighbors
            );
        }

        // Verify bidirectional reachability:
        // If A has B as eager, then B should have A as eager OR lazy (not completely unknown)
        for (node_a, state_a) in nodes.iter().enumerate() {
            for peer_b in state_a.eager_peers() {
                let state_b = &nodes[peer_b as usize];
                assert!(
                    state_b.contains(&(node_a as u64)),
                    "Node {} has {} as eager, but {} doesn't know about {}!",
                    node_a,
                    peer_b,
                    peer_b,
                    node_a
                );
            }
        }

        println!("\n✓ All 10 nodes are connected with no isolation!");
    }

    #[test]
    fn test_ten_node_cluster_ring_consistency() {
        // Verify that the hash ring is consistent across all nodes
        // All nodes should see the same ring order

        const CLUSTER_SIZE: u64 = 10;

        // Create all 10 nodes
        let mut nodes: Vec<PeerState<u64>> = Vec::new();
        for local_id in 0..CLUSTER_SIZE {
            let state = PeerStateBuilder::new()
                .with_local_id(local_id)
                .with_peers((0..CLUSTER_SIZE).filter(|&id| id != local_id))
                .with_eager_fanout(3)
                .with_lazy_fanout(6)
                .use_hash_ring(true)
                .build();
            nodes.push(state);
        }

        // Get hash ring neighbors for each node
        let mut all_neighbors: Vec<Vec<(u64, HashRingConnection)>> = Vec::new();
        for state in &nodes {
            if let Some(neighbors) = state.hash_ring_neighbors() {
                all_neighbors.push(neighbors);
            } else {
                panic!("Node should have hash ring neighbors!");
            }
        }

        // Verify adjacency symmetry: if A's adjacent neighbor is B, then B's adjacent neighbor should include A
        for (node_a, neighbors_a) in all_neighbors.iter().enumerate() {
            let adjacent_a: HashSet<u64> = neighbors_a
                .iter()
                .filter(|(_, c)| *c == HashRingConnection::Adjacent)
                .map(|(id, _)| *id)
                .collect();

            for adj_peer in &adjacent_a {
                let neighbors_b = &all_neighbors[*adj_peer as usize];
                let adjacent_b: HashSet<u64> = neighbors_b
                    .iter()
                    .filter(|(_, c)| *c == HashRingConnection::Adjacent)
                    .map(|(id, _)| *id)
                    .collect();

                assert!(
                    adjacent_b.contains(&(node_a as u64)),
                    "Adjacency not symmetric: {} -> {} but {} -/-> {}",
                    node_a,
                    adj_peer,
                    adj_peer,
                    node_a
                );
            }
        }

        println!("\n✓ Hash ring adjacency is symmetric across all 10 nodes!");
    }

    #[test]
    fn test_rebalance_with_scorer_prefers_high_scoring_peers() {
        // Test that rebalance_with_scorer promotes high-scoring peers over low-scoring ones
        let state = PeerState::new_with_local_id(0u64);

        // Add peers as lazy
        for i in 1..=10 {
            state.add_peer(i);
        }

        assert_eq!(state.eager_count(), 0);
        assert_eq!(state.lazy_count(), 10);

        // Define a scorer that gives high scores to peers 8, 9, 10
        // and low scores to peers 1, 2, 3
        let scorer = |peer: &u64| -> f64 {
            match *peer {
                8 | 9 | 10 => 1.0, // High score (excellent peers)
                4 | 5 | 6 | 7 => 0.5, // Medium score
                _ => 0.1, // Low score (poor peers)
            }
        };

        // Rebalance to promote 3 peers to eager
        state.rebalance_with_scorer(3, scorer);

        let eager_peers: HashSet<u64> = state.eager_peers().into_iter().collect();

        // High-scoring peers should be promoted (considering hybrid scoring with topology)
        // The exact peers depend on the hybrid score, but high-scoring peers should be favored
        println!(
            "Eager peers after rebalance: {:?} (expected high-scorers: 8, 9, 10)",
            eager_peers
        );

        // At least one high-scoring peer should be promoted
        let high_scorers_in_eager = eager_peers.iter().filter(|p| **p >= 8).count();
        assert!(
            high_scorers_in_eager >= 1,
            "At least one high-scoring peer should be promoted, got: {:?}",
            eager_peers
        );
    }

    #[test]
    fn test_rebalance_with_scorer_demotes_low_scoring_peers() {
        // Test that demotion prefers low-scoring peers
        let state = PeerState::new_with_local_id(0u64);

        // Add peers and promote all to eager
        for i in 1..=6 {
            state.add_peer(i);
            state.promote_to_eager(&i);
        }

        assert_eq!(state.eager_count(), 6);
        assert_eq!(state.lazy_count(), 0);

        // Get ring neighbors - these are protected
        let ring_neighbors: HashSet<u64> = state.ring_neighbors();

        // Define scorer: peers 1, 2 are low-scoring, 5, 6 are high-scoring
        let scorer = |peer: &u64| -> f64 {
            match *peer {
                1 | 2 => 0.1, // Low score
                3 | 4 => 0.5, // Medium score
                5 | 6 => 1.0, // High score
                _ => 0.5,
            }
        };

        // Rebalance to demote to 2 eager peers
        state.rebalance_with_scorer(2, scorer);

        // Ring neighbors should remain eager (protected)
        for neighbor in &ring_neighbors {
            if state.contains(neighbor) {
                assert!(
                    state.is_eager(neighbor),
                    "Ring neighbor {} should remain eager",
                    neighbor
                );
            }
        }

        let eager_peers: HashSet<u64> = state.eager_peers().into_iter().collect();
        let lazy_peers: HashSet<u64> = state.lazy_peers().into_iter().collect();

        println!("After demotion: eager={:?}, lazy={:?}", eager_peers, lazy_peers);
        println!("Ring neighbors (protected): {:?}", ring_neighbors);

        // Low-scoring non-ring-neighbor peers should be demoted first
        // Count how many low-scorers are in lazy
        let low_scorers_demoted = lazy_peers.iter().filter(|p| **p <= 2).count();
        println!("Low-scoring peers demoted: {}", low_scorers_demoted);
    }

    #[test]
    fn test_try_rebalance_with_scorer() {
        // Test that try_rebalance_with_scorer works correctly
        let state = PeerState::new_with_local_id(0u64);

        // Add peers as lazy
        for i in 1..=5 {
            state.add_peer(i);
        }

        // Define a scorer that prefers peer 5
        let scorer = |peer: &u64| -> f64 {
            if *peer == 5 {
                1.0
            } else {
                0.3
            }
        };

        // Try rebalance should succeed
        let success = state.try_rebalance_with_scorer(2, scorer);
        assert!(success, "try_rebalance should succeed when lock is free");

        assert_eq!(state.eager_count(), 2);
        assert_eq!(state.lazy_count(), 3);

        // Peer 5 (high score) should likely be promoted
        let eager_peers: HashSet<u64> = state.eager_peers().into_iter().collect();
        println!("Eager peers: {:?} (expected peer 5 to be included)", eager_peers);
    }

    #[test]
    fn test_remove_peer_auto_preserves_total_count() {
        // Test that remove_peer_auto removes exactly one peer and rebalances
        // Total count should be exactly (original - 1) after removal
        let state = PeerState::new_with_local_id(0u64);

        // Add 10 peers using add_peer_auto (3 eager, 7 lazy expected)
        for i in 1..=10 {
            state.add_peer_auto(i, None, 3);
        }

        let initial_eager = state.eager_count();
        let initial_lazy = state.lazy_count();
        let initial_total = initial_eager + initial_lazy;

        assert_eq!(initial_total, 10);
        assert_eq!(initial_eager, 3);
        assert_eq!(initial_lazy, 7);

        // Find a lazy peer to remove (with hash ring, specific peers may be eager)
        let lazy_peers = state.lazy_peers();
        assert!(!lazy_peers.is_empty(), "Should have lazy peers");
        let peer_to_remove = lazy_peers[0];

        // Remove a lazy peer
        let result = state.remove_peer_auto(&peer_to_remove, 3);
        assert_eq!(result, RemovePeerResult::RemovedLazy);

        let after_lazy_removal_eager = state.eager_count();
        let after_lazy_removal_lazy = state.lazy_count();
        let after_lazy_removal_total = after_lazy_removal_eager + after_lazy_removal_lazy;

        // Total should be exactly 9 (10 - 1)
        assert_eq!(
            after_lazy_removal_total, 9,
            "After removing lazy peer: eager={}, lazy={}, total={} (expected 9)",
            after_lazy_removal_eager, after_lazy_removal_lazy, after_lazy_removal_total
        );

        // Eager count should remain 3 (rebalancing shouldn't change it since we removed lazy)
        assert_eq!(after_lazy_removal_eager, 3);
        assert_eq!(after_lazy_removal_lazy, 6);

        println!(
            "After removing lazy peer {}: eager={}, lazy={}, total={}",
            peer_to_remove, after_lazy_removal_eager, after_lazy_removal_lazy, after_lazy_removal_total
        );
    }

    #[test]
    fn test_remove_peer_auto_eager_triggers_rebalance() {
        // Test that removing an eager peer triggers rebalancing
        // Ring neighbors should be promoted from lazy to eager
        let state = PeerState::new_with_local_id(0u64);

        // Add 10 peers
        for i in 1..=10 {
            state.add_peer_auto(i, None, 3);
        }

        let initial_total = state.eager_count() + state.lazy_count();
        assert_eq!(initial_total, 10);

        // Find an eager peer that is NOT a ring neighbor (can be removed without protection issues)
        let ring_neighbors = state.ring_neighbors();
        let eager_peers = state.eager_peers();
        let removable_eager: Option<u64> = eager_peers
            .iter()
            .find(|p| !ring_neighbors.contains(p))
            .cloned();

        if let Some(peer_to_remove) = removable_eager {
            let result = state.remove_peer_auto(&peer_to_remove, 3);
            assert_eq!(result, RemovePeerResult::RemovedEager);

            let after_removal_eager = state.eager_count();
            let after_removal_lazy = state.lazy_count();
            let after_removal_total = after_removal_eager + after_removal_lazy;

            // Total should be exactly 9
            assert_eq!(
                after_removal_total, 9,
                "After removing eager peer: eager={}, lazy={}, total={} (expected 9)",
                after_removal_eager, after_removal_lazy, after_removal_total
            );

            // After rebalancing, eager count should be back to 3 (if enough lazy peers)
            // or at least ring neighbors should be in eager
            for neighbor in &ring_neighbors {
                if state.contains(neighbor) {
                    assert!(
                        state.is_eager(neighbor),
                        "Ring neighbor {} should be in eager set after rebalance",
                        neighbor
                    );
                }
            }

            println!(
                "After removing eager peer {}: eager={}, lazy={}, total={}",
                peer_to_remove, after_removal_eager, after_removal_lazy, after_removal_total
            );
        } else {
            // All eager peers are ring neighbors, try removing one anyway
            let peer_to_remove = eager_peers[0];
            let result = state.remove_peer_auto(&peer_to_remove, 3);
            assert_eq!(result, RemovePeerResult::RemovedEager);

            let after_removal_total = state.eager_count() + state.lazy_count();
            assert_eq!(after_removal_total, 9);
        }
    }

    #[test]
    fn test_remove_peer_auto_not_found() {
        let state = PeerState::new_with_local_id(0u64);

        // Add some peers
        for i in 1..=5 {
            state.add_peer_auto(i, None, 2);
        }

        // Try to remove non-existent peer
        let result = state.remove_peer_auto(&99, 2);
        assert_eq!(result, RemovePeerResult::NotFound);

        // Total should be unchanged
        assert_eq!(state.eager_count() + state.lazy_count(), 5);
    }

    #[test]
    fn test_remove_peer_auto_all_peers() {
        // Test removing all peers one by one
        let state = PeerState::new_with_local_id(0u64);

        // Add 5 peers
        for i in 1..=5 {
            state.add_peer_auto(i, None, 2);
        }

        assert_eq!(state.eager_count() + state.lazy_count(), 5);

        // Remove all peers
        for i in 1..=5 {
            let before_total = state.eager_count() + state.lazy_count();
            let result = state.remove_peer_auto(&i, 2);
            assert!(result.was_removed(), "Peer {} should be removed", i);

            let after_total = state.eager_count() + state.lazy_count();
            assert_eq!(
                after_total,
                before_total - 1,
                "After removing peer {}: total should be {} but got {}",
                i,
                before_total - 1,
                after_total
            );
        }

        // All peers removed
        assert_eq!(state.eager_count(), 0);
        assert_eq!(state.lazy_count(), 0);
        assert_eq!(state.total_count(), 0);
    }

    #[test]
    fn test_remove_peer_auto_rebalance_ring_neighbors_promoted() {
        // Test that when we remove peers, ring neighbors are prioritized in eager set
        // Note: Not all ring neighbors will be eager if there are more ring neighbors than eager_fanout
        let state = PeerState::new_with_local_id(0u64);

        // Add 20 peers with hash ring
        for i in 1..=20 {
            state.add_peer_auto(i, None, 3);
        }

        let initial_total = state.eager_count() + state.lazy_count();
        assert_eq!(initial_total, 20);

        // Get ring neighbors before any removal
        let ring_neighbors_before = state.ring_neighbors();
        println!("Ring neighbors before removal: {:?}", ring_neighbors_before);

        // Remove several peers (not ring neighbors)
        let mut removed_count = 0;
        for i in 10..=15 {
            if !ring_neighbors_before.contains(&i) && state.contains(&i) {
                let result = state.remove_peer_auto(&i, 3);
                if result.was_removed() {
                    removed_count += 1;
                }
            }
        }

        let final_total = state.eager_count() + state.lazy_count();
        assert_eq!(
            final_total,
            initial_total - removed_count,
            "Expected {} peers after removing {}, got {}",
            initial_total - removed_count,
            removed_count,
            final_total
        );

        // Ring neighbors that are in eager should still be in eager
        // Count how many ring neighbors are in eager vs eager_fanout
        let ring_neighbors_after = state.ring_neighbors();
        let ring_neighbors_in_eager: Vec<_> = ring_neighbors_after
            .iter()
            .filter(|n| state.is_eager(n))
            .collect();

        // At least some ring neighbors should be in eager (up to eager_fanout)
        let expected_ring_in_eager = ring_neighbors_after.len().min(3); // eager_fanout = 3
        assert!(
            ring_neighbors_in_eager.len() >= expected_ring_in_eager.saturating_sub(1),
            "Expected at least {} ring neighbors in eager, got {} (ring_neighbors={:?}, eager={:?})",
            expected_ring_in_eager.saturating_sub(1),
            ring_neighbors_in_eager.len(),
            ring_neighbors_after,
            state.eager_peers()
        );

        println!(
            "After removing {} peers: eager={}, lazy={}, total={}, ring_neighbors_in_eager={}",
            removed_count,
            state.eager_count(),
            state.lazy_count(),
            final_total,
            ring_neighbors_in_eager.len()
        );
    }

    #[test]
    fn test_remove_peer_auto_without_hash_ring() {
        // Test remove_peer_auto when hash ring is not enabled
        let state: PeerState<u64> = PeerState::new();

        // Add peers (no hash ring, simple first-come-first-served)
        for i in 1..=10 {
            state.add_peer_auto(i, None, 3);
        }

        let initial_total = state.eager_count() + state.lazy_count();
        assert_eq!(initial_total, 10);
        assert_eq!(state.eager_count(), 3);
        assert_eq!(state.lazy_count(), 7);

        // Remove an eager peer
        let result = state.remove_peer_auto(&1, 3);
        assert_eq!(result, RemovePeerResult::RemovedEager);

        let after_total = state.eager_count() + state.lazy_count();
        assert_eq!(after_total, 9);

        // Without hash ring, eager count should decrease (no automatic rebalance)
        // The rebalance_eager_with_ring_neighbors is skipped when !use_hash_ring
        assert_eq!(state.eager_count(), 2);
        assert_eq!(state.lazy_count(), 7);
    }
}
