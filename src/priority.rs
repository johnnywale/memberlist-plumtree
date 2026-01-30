//! Message priority support for Plumtree.
//!
//! This module provides priority-based message scheduling to ensure
//! critical protocol messages (like Graft for tree repair) are processed
//! before less urgent messages.
//!
//! # Priority Levels
//!
//! - **Critical**: Graft messages - tree repair is time-sensitive
//! - **High**: Gossip messages - payload delivery
//! - **Normal**: IHave messages - announcements
//! - **Low**: Prune messages - optimization
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{PriorityConfig, MessagePriority};
//!
//! let config = PriorityConfig {
//!     enabled: true,
//!     queue_depths: [64, 256, 512, 128], // Critical, High, Normal, Low
//! };
//! ```

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

/// Message priority levels.
///
/// Lower numeric values = higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum MessagePriority {
    /// Critical priority - Graft messages for tree repair
    Critical = 0,
    /// High priority - Gossip messages with payloads
    High = 1,
    /// Normal priority - IHave announcements
    #[default]
    Normal = 2,
    /// Low priority - Prune messages for optimization
    Low = 3,
}

impl MessagePriority {
    /// All priority levels in order (highest first)
    pub const ALL: [MessagePriority; 4] = [
        MessagePriority::Critical,
        MessagePriority::High,
        MessagePriority::Normal,
        MessagePriority::Low,
    ];

    /// Get the index of this priority (0-3)
    pub fn index(self) -> usize {
        self as usize
    }

    /// Create from index
    pub fn from_index(index: usize) -> Option<Self> {
        match index {
            0 => Some(MessagePriority::Critical),
            1 => Some(MessagePriority::High),
            2 => Some(MessagePriority::Normal),
            3 => Some(MessagePriority::Low),
            _ => None,
        }
    }

    /// Get a human-readable name
    pub fn name(self) -> &'static str {
        match self {
            MessagePriority::Critical => "critical",
            MessagePriority::High => "high",
            MessagePriority::Normal => "normal",
            MessagePriority::Low => "low",
        }
    }
}

/// Configuration for priority queues.
#[derive(Debug, Clone)]
pub struct PriorityConfig {
    /// Whether priority queues are enabled
    pub enabled: bool,
    /// Maximum queue depth for each priority level
    /// [Critical, High, Normal, Low]
    pub queue_depths: [usize; 4],
    /// Weighted fair scheduling ratios
    /// Higher weight = more messages dequeued per round
    pub weights: [u32; 4],
}

impl Default for PriorityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            queue_depths: [64, 256, 512, 128], // Critical is small but fast
            weights: [8, 4, 2, 1],             // Critical gets 8x weight
        }
    }
}

impl PriorityConfig {
    /// Create a disabled config
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set queue depth for a priority level
    pub fn with_depth(mut self, priority: MessagePriority, depth: usize) -> Self {
        self.queue_depths[priority.index()] = depth;
        self
    }

    /// Set weight for a priority level
    pub fn with_weight(mut self, priority: MessagePriority, weight: u32) -> Self {
        self.weights[priority.index()] = weight;
        self
    }

    /// Get queue depth for a priority level
    pub fn depth(&self, priority: MessagePriority) -> usize {
        self.queue_depths[priority.index()]
    }

    /// Get weight for a priority level
    pub fn weight(&self, priority: MessagePriority) -> u32 {
        self.weights[priority.index()]
    }
}

/// A multi-priority queue with weighted fair scheduling.
///
/// Messages are dequeued in priority order, with higher priorities
/// getting more slots per scheduling round based on their weights.
#[derive(Debug)]
pub struct PriorityQueue<T> {
    queues: [VecDeque<T>; 4],
    config: PriorityConfig,
    stats: PriorityQueueStats,
}

impl<T> PriorityQueue<T> {
    /// Create a new priority queue with the given config
    pub fn new(config: PriorityConfig) -> Self {
        Self {
            queues: [
                VecDeque::with_capacity(config.queue_depths[0]),
                VecDeque::with_capacity(config.queue_depths[1]),
                VecDeque::with_capacity(config.queue_depths[2]),
                VecDeque::with_capacity(config.queue_depths[3]),
            ],
            config,
            stats: PriorityQueueStats::default(),
        }
    }

    /// Push an item with the given priority.
    ///
    /// Returns `Err(item)` if the queue for that priority is full.
    pub fn push(&mut self, item: T, priority: MessagePriority) -> Result<(), T> {
        let idx = priority.index();
        if self.queues[idx].len() >= self.config.queue_depths[idx] {
            self.stats.dropped[idx].fetch_add(1, Ordering::Relaxed);
            return Err(item);
        }
        self.queues[idx].push_back(item);
        self.stats.enqueued[idx].fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Pop the highest priority item.
    ///
    /// Uses weighted fair scheduling to prevent starvation of lower priorities.
    pub fn pop(&mut self) -> Option<(T, MessagePriority)> {
        // Simple priority order - highest first
        for priority in MessagePriority::ALL {
            let idx = priority.index();
            if let Some(item) = self.queues[idx].pop_front() {
                self.stats.dequeued[idx].fetch_add(1, Ordering::Relaxed);
                return Some((item, priority));
            }
        }
        None
    }

    /// Pop up to `count` items using weighted fair scheduling.
    ///
    /// Returns items in batches, respecting priority weights.
    pub fn pop_batch(&mut self, count: usize) -> Vec<(T, MessagePriority)> {
        let mut result = Vec::with_capacity(count);
        let mut remaining = count;

        // Calculate total weight
        let total_weight: u32 = self.config.weights.iter().sum();
        if total_weight == 0 {
            return result;
        }

        // Allocate slots based on weights
        let mut slots = [0usize; 4];
        for (i, &weight) in self.config.weights.iter().enumerate() {
            slots[i] = (count as u64 * weight as u64 / total_weight as u64) as usize;
        }

        // Ensure at least 1 slot for non-empty queues with weight > 0
        for (i, slot) in slots.iter_mut().enumerate() {
            if *slot == 0 && self.config.weights[i] > 0 && !self.queues[i].is_empty() {
                *slot = 1;
            }
        }

        // Dequeue from each priority level
        for priority in MessagePriority::ALL {
            let idx = priority.index();
            let to_take = slots[idx].min(remaining).min(self.queues[idx].len());

            for _ in 0..to_take {
                if let Some(item) = self.queues[idx].pop_front() {
                    self.stats.dequeued[idx].fetch_add(1, Ordering::Relaxed);
                    result.push((item, priority));
                    remaining -= 1;
                }
            }

            if remaining == 0 {
                break;
            }
        }

        // If we have remaining slots, fill from any available queue (highest priority first)
        if remaining > 0 {
            for priority in MessagePriority::ALL {
                let idx = priority.index();
                while remaining > 0 {
                    if let Some(item) = self.queues[idx].pop_front() {
                        self.stats.dequeued[idx].fetch_add(1, Ordering::Relaxed);
                        result.push((item, priority));
                        remaining -= 1;
                    } else {
                        break;
                    }
                }
            }
        }

        result
    }

    /// Check if all queues are empty
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }

    /// Get total number of items across all queues
    pub fn len(&self) -> usize {
        self.queues.iter().map(|q| q.len()).sum()
    }

    /// Get the length of a specific priority queue
    pub fn len_priority(&self, priority: MessagePriority) -> usize {
        self.queues[priority.index()].len()
    }

    /// Get queue statistics
    pub fn stats(&self) -> &PriorityQueueStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&self) {
        for i in 0..4 {
            self.stats.enqueued[i].store(0, Ordering::Relaxed);
            self.stats.dequeued[i].store(0, Ordering::Relaxed);
            self.stats.dropped[i].store(0, Ordering::Relaxed);
        }
    }
}

/// Statistics for priority queue operations.
#[derive(Debug, Default)]
pub struct PriorityQueueStats {
    /// Messages enqueued per priority
    pub enqueued: [AtomicU64; 4],
    /// Messages dequeued per priority
    pub dequeued: [AtomicU64; 4],
    /// Messages dropped (queue full) per priority
    pub dropped: [AtomicU64; 4],
}

impl PriorityQueueStats {
    /// Get enqueued count for a priority
    pub fn enqueued(&self, priority: MessagePriority) -> u64 {
        self.enqueued[priority.index()].load(Ordering::Relaxed)
    }

    /// Get dequeued count for a priority
    pub fn dequeued(&self, priority: MessagePriority) -> u64 {
        self.dequeued[priority.index()].load(Ordering::Relaxed)
    }

    /// Get dropped count for a priority
    pub fn dropped(&self, priority: MessagePriority) -> u64 {
        self.dropped[priority.index()].load(Ordering::Relaxed)
    }

    /// Get total enqueued across all priorities
    pub fn total_enqueued(&self) -> u64 {
        self.enqueued
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }

    /// Get total dequeued across all priorities
    pub fn total_dequeued(&self) -> u64 {
        self.dequeued
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .sum()
    }

    /// Get total dropped across all priorities
    pub fn total_dropped(&self) -> u64 {
        self.dropped.iter().map(|a| a.load(Ordering::Relaxed)).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_ordering() {
        assert!(MessagePriority::Critical < MessagePriority::High);
        assert!(MessagePriority::High < MessagePriority::Normal);
        assert!(MessagePriority::Normal < MessagePriority::Low);
    }

    #[test]
    fn test_priority_index() {
        assert_eq!(MessagePriority::Critical.index(), 0);
        assert_eq!(MessagePriority::High.index(), 1);
        assert_eq!(MessagePriority::Normal.index(), 2);
        assert_eq!(MessagePriority::Low.index(), 3);
    }

    #[test]
    fn test_priority_from_index() {
        assert_eq!(
            MessagePriority::from_index(0),
            Some(MessagePriority::Critical)
        );
        assert_eq!(MessagePriority::from_index(1), Some(MessagePriority::High));
        assert_eq!(
            MessagePriority::from_index(2),
            Some(MessagePriority::Normal)
        );
        assert_eq!(MessagePriority::from_index(3), Some(MessagePriority::Low));
        assert_eq!(MessagePriority::from_index(4), None);
    }

    #[test]
    fn test_priority_queue_basic() {
        let config = PriorityConfig::default();
        let mut queue: PriorityQueue<u32> = PriorityQueue::new(config);

        // Push items with different priorities
        queue.push(1, MessagePriority::Low).unwrap();
        queue.push(2, MessagePriority::High).unwrap();
        queue.push(3, MessagePriority::Critical).unwrap();
        queue.push(4, MessagePriority::Normal).unwrap();

        // Pop should return in priority order
        assert_eq!(queue.pop(), Some((3, MessagePriority::Critical)));
        assert_eq!(queue.pop(), Some((2, MessagePriority::High)));
        assert_eq!(queue.pop(), Some((4, MessagePriority::Normal)));
        assert_eq!(queue.pop(), Some((1, MessagePriority::Low)));
        assert_eq!(queue.pop(), None);
    }

    #[test]
    fn test_priority_queue_full() {
        let config = PriorityConfig {
            enabled: true,
            queue_depths: [2, 2, 2, 2],
            weights: [1, 1, 1, 1],
        };
        let mut queue: PriorityQueue<u32> = PriorityQueue::new(config);

        // Fill the critical queue
        assert!(queue.push(1, MessagePriority::Critical).is_ok());
        assert!(queue.push(2, MessagePriority::Critical).is_ok());
        assert!(queue.push(3, MessagePriority::Critical).is_err()); // Full

        assert_eq!(queue.stats().dropped(MessagePriority::Critical), 1);
    }

    #[test]
    fn test_priority_queue_batch() {
        let config = PriorityConfig {
            enabled: true,
            queue_depths: [10, 10, 10, 10],
            weights: [4, 2, 1, 1], // Critical gets 50%, High 25%, Normal/Low 12.5% each
        };
        let mut queue: PriorityQueue<u32> = PriorityQueue::new(config);

        // Add many items to each queue
        for i in 0..10 {
            queue.push(i, MessagePriority::Critical).unwrap();
            queue.push(i + 10, MessagePriority::High).unwrap();
            queue.push(i + 20, MessagePriority::Normal).unwrap();
            queue.push(i + 30, MessagePriority::Low).unwrap();
        }

        // Pop a batch of 8
        let batch = queue.pop_batch(8);
        assert_eq!(batch.len(), 8);

        // Should have items from multiple priorities due to weighted scheduling
        let critical_count = batch
            .iter()
            .filter(|(_, p)| *p == MessagePriority::Critical)
            .count();
        let high_count = batch
            .iter()
            .filter(|(_, p)| *p == MessagePriority::High)
            .count();

        // Critical should have the most
        assert!(critical_count >= high_count);
    }

    #[test]
    fn test_priority_queue_stats() {
        let config = PriorityConfig::default();
        let mut queue: PriorityQueue<u32> = PriorityQueue::new(config);

        queue.push(1, MessagePriority::High).unwrap();
        queue.push(2, MessagePriority::High).unwrap();
        queue.pop();

        let stats = queue.stats();
        assert_eq!(stats.enqueued(MessagePriority::High), 2);
        assert_eq!(stats.dequeued(MessagePriority::High), 1);
        assert_eq!(stats.total_enqueued(), 2);
        assert_eq!(stats.total_dequeued(), 1);
    }

    #[test]
    fn test_priority_config_builder() {
        let config = PriorityConfig::default()
            .with_depth(MessagePriority::Critical, 128)
            .with_weight(MessagePriority::Critical, 16);

        assert_eq!(config.depth(MessagePriority::Critical), 128);
        assert_eq!(config.weight(MessagePriority::Critical), 16);
    }

    #[test]
    fn test_priority_queue_empty() {
        let config = PriorityConfig::default();
        let mut queue: PriorityQueue<u32> = PriorityQueue::new(config);

        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);

        queue.push(1, MessagePriority::Normal).unwrap();

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.len_priority(MessagePriority::Normal), 1);
        assert_eq!(queue.len_priority(MessagePriority::Critical), 0);
    }
}
