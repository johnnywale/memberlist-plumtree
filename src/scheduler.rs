//! Background scheduler for IHave announcements.
//!
//! Batches IHave messages and sends them periodically to lazy peers,
//! using a lock-free queue for efficient producer/consumer pattern.

use crossbeam_queue::SegQueue;
use smallvec::SmallVec;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::message::MessageId;

/// Pending IHave entry in the queue.
#[derive(Debug, Clone)]
pub struct PendingIHave {
    /// Message ID to announce.
    pub message_id: MessageId,
    /// Round number for this announcement.
    pub round: u32,
}

/// Lock-free queue for pending IHave announcements.
///
/// Uses crossbeam's SegQueue for efficient concurrent access
/// without lock contention in the hot path.
#[derive(Debug)]
pub struct IHaveQueue {
    /// Lock-free queue of pending announcements.
    queue: SegQueue<PendingIHave>,
    /// Approximate length (may be slightly stale).
    len: AtomicUsize,
    /// Maximum queue size before dropping.
    max_size: usize,
    /// Flag indicating if the queue is accepting new items.
    accepting: AtomicBool,
}

impl IHaveQueue {
    /// Create a new IHave queue with the specified maximum size.
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: SegQueue::new(),
            len: AtomicUsize::new(0),
            max_size,
            accepting: AtomicBool::new(true),
        }
    }

    /// Push a new IHave announcement to the queue.
    ///
    /// Returns `true` if the item was queued, `false` if the queue is full
    /// or not accepting new items.
    pub fn push(&self, message_id: MessageId, round: u32) -> bool {
        // Check if accepting
        if !self.accepting.load(Ordering::Acquire) {
            return false;
        }

        // Check approximate size
        let current_len = self.len.load(Ordering::Relaxed);
        if current_len >= self.max_size {
            return false;
        }

        // Push to queue
        self.queue.push(PendingIHave { message_id, round });
        self.len.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Pop a batch of IHave announcements from the queue.
    ///
    /// Returns up to `max_batch` items.
    pub fn pop_batch(&self, max_batch: usize) -> SmallVec<[PendingIHave; 16]> {
        let mut batch = SmallVec::new();

        for _ in 0..max_batch {
            if let Some(item) = self.queue.pop() {
                self.len.fetch_sub(1, Ordering::Relaxed);
                batch.push(item);
            } else {
                break;
            }
        }

        batch
    }

    /// Get the approximate length of the queue.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Check if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Stop accepting new items.
    pub fn stop(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    /// Resume accepting new items.
    pub fn resume(&self) {
        self.accepting.store(true, Ordering::Release);
    }

    /// Clear all pending items from the queue.
    pub fn clear(&self) {
        while self.queue.pop().is_some() {
            self.len.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Default for IHaveQueue {
    fn default() -> Self {
        Self::new(10000)
    }
}

/// Scheduler for managing IHave announcement timing.
///
/// Tracks pending Graft timeouts and manages the IHave send interval.
#[derive(Debug)]
pub struct IHaveScheduler {
    /// Queue of pending IHave announcements.
    queue: Arc<IHaveQueue>,
    /// Interval between IHave batches.
    interval: Duration,
    /// Maximum batch size.
    batch_size: usize,
    /// Shutdown flag.
    shutdown: AtomicBool,
}

impl IHaveScheduler {
    /// Create a new IHave scheduler.
    pub fn new(interval: Duration, batch_size: usize, max_queue_size: usize) -> Self {
        Self {
            queue: Arc::new(IHaveQueue::new(max_queue_size)),
            interval,
            batch_size,
            shutdown: AtomicBool::new(false),
        }
    }

    /// Get a reference to the IHave queue.
    pub fn queue(&self) -> &Arc<IHaveQueue> {
        &self.queue
    }

    /// Get the configured interval.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Get the configured batch size.
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Check if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Request shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.queue.stop();
    }

    /// Pop a batch for sending.
    pub fn pop_batch(&self) -> SmallVec<[PendingIHave; 16]> {
        self.queue.pop_batch(self.batch_size)
    }
}

/// Tracks pending Graft requests with timeouts and exponential backoff.
///
/// When an IHave is received, we wait for the actual message.
/// If not received within timeout, we send a Graft with exponential backoff.
#[derive(Debug)]
pub struct GraftTimer {
    /// Pending message IDs waiting for content.
    pending: parking_lot::Mutex<std::collections::HashMap<MessageId, GraftEntry>>,
    /// Base timeout for expecting message after IHave.
    base_timeout: Duration,
    /// Maximum timeout after backoff.
    max_timeout: Duration,
    /// Maximum retry attempts before giving up.
    max_retries: u32,
}

#[derive(Debug, Clone)]
struct GraftEntry {
    /// When this entry was created.
    /// Reserved for future use in timeout diagnostics.
    #[allow(dead_code)]
    created: std::time::Instant,
    /// When the next retry should occur.
    next_retry: std::time::Instant,
    /// Node that sent the IHave.
    from: Vec<u8>, // Generic node ID as bytes
    /// Alternative peers to try (as serialized bytes).
    alternative_peers: Vec<Vec<u8>>,
    /// Round from the IHave.
    round: u32,
    /// Number of retry attempts made.
    retry_count: u32,
}

/// Result of checking for expired Graft timers.
#[derive(Debug, Clone)]
pub struct ExpiredGraft {
    /// Message ID that needs to be requested.
    pub message_id: MessageId,
    /// Peer to request from.
    pub peer: Vec<u8>,
    /// Round number.
    pub round: u32,
    /// Which retry attempt this is (0 = first attempt).
    pub retry_count: u32,
}

impl GraftTimer {
    /// Create a new Graft timer with default backoff settings.
    pub fn new(timeout: Duration) -> Self {
        Self {
            pending: parking_lot::Mutex::new(std::collections::HashMap::new()),
            base_timeout: timeout,
            max_timeout: timeout * 8, // Max 8x base timeout
            max_retries: 5,
        }
    }

    /// Create a new Graft timer with custom backoff settings.
    pub fn with_backoff(base_timeout: Duration, max_timeout: Duration, max_retries: u32) -> Self {
        Self {
            pending: parking_lot::Mutex::new(std::collections::HashMap::new()),
            base_timeout,
            max_timeout,
            max_retries,
        }
    }

    /// Record that we're expecting a message (received IHave).
    pub fn expect_message(&self, message_id: MessageId, from: Vec<u8>, round: u32) {
        let now = std::time::Instant::now();
        let mut pending = self.pending.lock();
        pending.entry(message_id).or_insert_with(|| GraftEntry {
            created: now,
            next_retry: now + self.base_timeout,
            from,
            alternative_peers: Vec::new(),
            round,
            retry_count: 0,
        });
    }

    /// Record that we're expecting a message with alternative peers to try.
    pub fn expect_message_with_alternatives(
        &self,
        message_id: MessageId,
        from: Vec<u8>,
        alternatives: Vec<Vec<u8>>,
        round: u32,
    ) {
        let now = std::time::Instant::now();
        let mut pending = self.pending.lock();
        pending.entry(message_id).or_insert_with(|| GraftEntry {
            created: now,
            next_retry: now + self.base_timeout,
            from,
            alternative_peers: alternatives,
            round,
            retry_count: 0,
        });
    }

    /// Mark that a message was received (cancel Graft timer).
    pub fn message_received(&self, message_id: &MessageId) {
        let mut pending = self.pending.lock();
        pending.remove(message_id);
    }

    /// Calculate backoff duration for a given retry count.
    fn calculate_backoff(&self, retry_count: u32) -> Duration {
        // Exponential backoff: base * 2^retry_count, capped at max
        let multiplier = 1u32.checked_shl(retry_count).unwrap_or(u32::MAX);
        let backoff = self.base_timeout.saturating_mul(multiplier);
        std::cmp::min(backoff, self.max_timeout)
    }

    /// Get expired entries that need Graft requests.
    ///
    /// Returns list of expired grafts with peer to try and retry info.
    /// Entries that exceed max_retries are removed.
    pub fn get_expired(&self) -> Vec<ExpiredGraft> {
        let now = std::time::Instant::now();
        let mut pending = self.pending.lock();

        let mut expired = Vec::new();
        let mut to_remove = Vec::new();

        for (id, entry) in pending.iter_mut() {
            if now >= entry.next_retry {
                // Determine which peer to try
                let peer = if entry.retry_count == 0 {
                    // First attempt: use original sender
                    entry.from.clone()
                } else {
                    // Subsequent attempts: try alternatives in round-robin
                    let alt_idx =
                        (entry.retry_count - 1) as usize % entry.alternative_peers.len().max(1);
                    if alt_idx < entry.alternative_peers.len() {
                        entry.alternative_peers[alt_idx].clone()
                    } else {
                        entry.from.clone()
                    }
                };

                expired.push(ExpiredGraft {
                    message_id: *id,
                    peer,
                    round: entry.round,
                    retry_count: entry.retry_count,
                });

                entry.retry_count += 1;

                if entry.retry_count >= self.max_retries {
                    // Max retries exceeded, give up
                    to_remove.push(*id);
                } else {
                    // Schedule next retry with backoff
                    let backoff = self.calculate_backoff(entry.retry_count);
                    entry.next_retry = now + backoff;
                }
            }
        }

        // Remove entries that exceeded max retries
        for id in to_remove {
            pending.remove(&id);
        }

        expired
    }

    /// Get expired entries (legacy API for compatibility).
    ///
    /// Returns list of (message_id, from_node, round) for expired timers.
    pub fn get_expired_simple(&self) -> Vec<(MessageId, Vec<u8>, u32)> {
        self.get_expired()
            .into_iter()
            .map(|e| (e.message_id, e.peer, e.round))
            .collect()
    }

    /// Clear all pending entries.
    pub fn clear(&self) {
        self.pending.lock().clear();
    }

    /// Get the number of pending entries.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().len()
    }

    /// Get the base timeout.
    pub fn base_timeout(&self) -> Duration {
        self.base_timeout
    }

    /// Get the max retries.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ihave_queue_push_pop() {
        let queue = IHaveQueue::new(100);

        let id = MessageId::new();
        assert!(queue.push(id, 0));

        let batch = queue.pop_batch(10);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].message_id, id);
    }

    #[test]
    fn test_ihave_queue_capacity() {
        let queue = IHaveQueue::new(3);

        for i in 0..5 {
            let pushed = queue.push(MessageId::new(), i);
            if i < 3 {
                assert!(pushed);
            } else {
                assert!(!pushed);
            }
        }

        assert_eq!(queue.len(), 3);
    }

    #[test]
    fn test_ihave_queue_batch() {
        let queue = IHaveQueue::new(100);

        for i in 0..10 {
            queue.push(MessageId::new(), i);
        }

        let batch = queue.pop_batch(5);
        assert_eq!(batch.len(), 5);
        assert_eq!(queue.len(), 5);
    }

    #[test]
    fn test_ihave_queue_stop() {
        let queue = IHaveQueue::new(100);

        assert!(queue.push(MessageId::new(), 0));
        queue.stop();
        assert!(!queue.push(MessageId::new(), 0));
        queue.resume();
        assert!(queue.push(MessageId::new(), 0));
    }

    #[test]
    fn test_graft_timer() {
        let timer = GraftTimer::new(Duration::from_millis(50));

        let id = MessageId::new();
        timer.expect_message(id, vec![1, 2, 3], 0);

        // Not expired yet
        let expired = timer.get_expired();
        assert!(expired.is_empty());

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(100));

        let expired = timer.get_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].message_id, id);
        assert_eq!(expired[0].retry_count, 0);
    }

    #[test]
    fn test_graft_timer_message_received() {
        let timer = GraftTimer::new(Duration::from_millis(50));

        let id = MessageId::new();
        timer.expect_message(id, vec![1, 2, 3], 0);
        timer.message_received(&id);

        std::thread::sleep(Duration::from_millis(100));

        let expired = timer.get_expired();
        assert!(expired.is_empty());
    }

    #[test]
    fn test_graft_timer_backoff() {
        let timer =
            GraftTimer::with_backoff(Duration::from_millis(20), Duration::from_millis(160), 3);

        let id = MessageId::new();
        timer.expect_message(id, vec![1, 2, 3], 0);

        // First expiry after base timeout
        std::thread::sleep(Duration::from_millis(30));
        let expired = timer.get_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].retry_count, 0);

        // Second expiry should be after 2x base timeout (40ms)
        std::thread::sleep(Duration::from_millis(30));
        let expired = timer.get_expired();
        assert!(expired.is_empty()); // Not yet

        std::thread::sleep(Duration::from_millis(20));
        let expired = timer.get_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].retry_count, 1);

        // Third expiry after 4x base timeout (80ms)
        std::thread::sleep(Duration::from_millis(90));
        let expired = timer.get_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].retry_count, 2);

        // After max retries, entry should be removed
        assert_eq!(timer.pending_count(), 0);
    }

    #[test]
    fn test_graft_timer_alternatives() {
        let timer =
            GraftTimer::with_backoff(Duration::from_millis(20), Duration::from_millis(200), 4);

        let id = MessageId::new();
        let primary = vec![1, 2, 3];
        let alt1 = vec![4, 5, 6];
        let alt2 = vec![7, 8, 9];
        timer.expect_message_with_alternatives(
            id,
            primary.clone(),
            vec![alt1.clone(), alt2.clone()],
            0,
        );

        // First try: primary peer
        std::thread::sleep(Duration::from_millis(30));
        let expired = timer.get_expired();
        assert_eq!(expired[0].peer, primary);

        // Second try: first alternative
        std::thread::sleep(Duration::from_millis(50));
        let expired = timer.get_expired();
        assert_eq!(expired[0].peer, alt1);

        // Third try: second alternative
        std::thread::sleep(Duration::from_millis(90));
        let expired = timer.get_expired();
        assert_eq!(expired[0].peer, alt2);

        // Fourth try: back to first alternative (round-robin)
        std::thread::sleep(Duration::from_millis(170));
        let expired = timer.get_expired();
        assert_eq!(expired[0].peer, alt1);
    }

    #[test]
    fn test_scheduler() {
        let scheduler = IHaveScheduler::new(Duration::from_millis(100), 16, 1000);

        scheduler.queue().push(MessageId::new(), 0);
        scheduler.queue().push(MessageId::new(), 1);

        let batch = scheduler.pop_batch();
        assert_eq!(batch.len(), 2);
    }
}
