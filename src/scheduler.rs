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
    /// Threshold for early flush notification.
    flush_threshold: AtomicUsize,
}

impl IHaveQueue {
    /// Create a new IHave queue with the specified maximum size.
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: SegQueue::new(),
            len: AtomicUsize::new(0),
            max_size,
            accepting: AtomicBool::new(true),
            flush_threshold: AtomicUsize::new(16), // Default batch size
        }
    }

    /// Create a new IHave queue with a specific flush threshold.
    pub fn with_flush_threshold(max_size: usize, flush_threshold: usize) -> Self {
        Self {
            queue: SegQueue::new(),
            len: AtomicUsize::new(0),
            max_size,
            accepting: AtomicBool::new(true),
            flush_threshold: AtomicUsize::new(flush_threshold),
        }
    }

    /// Set the flush threshold (batch size).
    pub fn set_flush_threshold(&self, threshold: usize) {
        self.flush_threshold.store(threshold, Ordering::Relaxed);
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

    /// Check if the queue has reached the flush threshold.
    ///
    /// This can be used to trigger early batch flush in high-throughput scenarios.
    pub fn should_flush(&self) -> bool {
        let current_len = self.len.load(Ordering::Relaxed);
        let threshold = self.flush_threshold.load(Ordering::Relaxed);
        current_len >= threshold
    }

    /// Get the current flush threshold.
    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold.load(Ordering::Relaxed)
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
            queue: Arc::new(IHaveQueue::with_flush_threshold(max_queue_size, batch_size)),
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
///
/// # Performance
///
/// Uses a dual-index structure for efficient operations:
/// - `HashMap<MessageId, GraftEntry>` for O(1) lookup by message ID (cancel on receive)
/// - `BTreeMap<Instant, HashSet<MessageId>>` for O(K) expired entry retrieval (K = expired count)
///
/// This avoids the O(N) full scan on every timer tick, which is critical under high load
/// when thousands of messages may be pending Graft.
///
/// # Type Parameters
///
/// - `I`: Peer identifier type (must be Clone + Send + Sync)
#[derive(Debug)]
pub struct GraftTimer<I> {
    /// Inner state protected by mutex.
    inner: parking_lot::Mutex<GraftTimerInner<I>>,
    /// Base timeout for expecting message after IHave.
    base_timeout: Duration,
    /// Maximum timeout after backoff.
    max_timeout: Duration,
    /// Maximum retry attempts before giving up.
    max_retries: u32,
}

/// Inner state of GraftTimer (held under lock).
#[derive(Debug)]
struct GraftTimerInner<I> {
    /// Pending message IDs waiting for content (fast lookup by ID).
    entries: std::collections::HashMap<MessageId, GraftEntry<I>>,
    /// Time-sorted index for efficient expired entry retrieval.
    /// Maps timeout instant -> set of message IDs expiring at that time.
    timeouts: std::collections::BTreeMap<std::time::Instant, std::collections::HashSet<MessageId>>,
}

impl<I> Default for GraftTimerInner<I> {
    fn default() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            timeouts: std::collections::BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct GraftEntry<I> {
    /// When this entry was created.
    /// Reserved for future use in timeout diagnostics.
    #[allow(dead_code)]
    created: std::time::Instant,
    /// When the next retry should occur.
    next_retry: std::time::Instant,
    /// Node that sent the IHave.
    from: I,
    /// Alternative peers to try.
    alternative_peers: Vec<I>,
    /// Round from the IHave.
    round: u32,
    /// Number of retry attempts made.
    retry_count: u32,
}

/// Result of checking for expired Graft timers.
#[derive(Debug, Clone)]
pub struct ExpiredGraft<I> {
    /// Message ID that needs to be requested.
    pub message_id: MessageId,
    /// Peer to request from.
    pub peer: I,
    /// Round number.
    pub round: u32,
    /// Which retry attempt this is (0 = first attempt).
    pub retry_count: u32,
}

/// Information about a failed Graft after max retries exhausted.
#[derive(Debug, Clone)]
pub struct FailedGraft<I> {
    /// Message ID that could not be retrieved.
    pub message_id: MessageId,
    /// Original peer that sent the IHave (potential zombie).
    pub original_peer: I,
    /// Total retry attempts made.
    pub total_retries: u32,
}

impl<I: Clone + Send + Sync + 'static> GraftTimer<I> {
    /// Create a new Graft timer with default backoff settings.
    pub fn new(timeout: Duration) -> Self {
        Self {
            inner: parking_lot::Mutex::new(GraftTimerInner::default()),
            base_timeout: timeout,
            max_timeout: timeout * 8, // Max 8x base timeout
            max_retries: 5,
        }
    }

    /// Create a new Graft timer with custom backoff settings.
    pub fn with_backoff(base_timeout: Duration, max_timeout: Duration, max_retries: u32) -> Self {
        Self {
            inner: parking_lot::Mutex::new(GraftTimerInner::default()),
            base_timeout,
            max_timeout,
            max_retries,
        }
    }

    /// Add a message ID to the timeout index at a given instant.
    fn add_to_timeout_index(
        inner: &mut GraftTimerInner<I>,
        timeout: std::time::Instant,
        message_id: MessageId,
    ) {
        inner
            .timeouts
            .entry(timeout)
            .or_default()
            .insert(message_id);
    }

    /// Remove a message ID from the timeout index at a given instant.
    fn remove_from_timeout_index(
        inner: &mut GraftTimerInner<I>,
        timeout: std::time::Instant,
        message_id: &MessageId,
    ) {
        if let Some(ids) = inner.timeouts.get_mut(&timeout) {
            ids.remove(message_id);
            // Clean up empty sets to avoid memory leak
            if ids.is_empty() {
                inner.timeouts.remove(&timeout);
            }
        }
    }

    /// Record that we're expecting a message (received IHave).
    pub fn expect_message(&self, message_id: MessageId, from: I, round: u32) {
        let now = std::time::Instant::now();
        let next_retry = now + self.base_timeout;
        let mut inner = self.inner.lock();

        // Only insert if not already present
        if inner.entries.contains_key(&message_id) {
            return;
        }

        inner.entries.insert(
            message_id,
            GraftEntry {
                created: now,
                next_retry,
                from,
                alternative_peers: Vec::new(),
                round,
                retry_count: 0,
            },
        );
        Self::add_to_timeout_index(&mut inner, next_retry, message_id);
    }

    /// Record that we're expecting a message with alternative peers to try.
    pub fn expect_message_with_alternatives(
        &self,
        message_id: MessageId,
        from: I,
        alternatives: Vec<I>,
        round: u32,
    ) {
        let now = std::time::Instant::now();
        let next_retry = now + self.base_timeout;
        let mut inner = self.inner.lock();

        // Only insert if not already present
        if inner.entries.contains_key(&message_id) {
            return;
        }

        inner.entries.insert(
            message_id,
            GraftEntry {
                created: now,
                next_retry,
                from,
                alternative_peers: alternatives,
                round,
                retry_count: 0,
            },
        );
        Self::add_to_timeout_index(&mut inner, next_retry, message_id);
    }

    /// Mark that a message was received (cancel Graft timer).
    ///
    /// This records a successful graft and the latency from when the entry
    /// was created to when the message was received.
    ///
    /// Returns `true` if a graft was actually pending and sent (retry_count > 0),
    /// which indicates a successful graft that the adaptive batcher should track.
    pub fn message_received(&self, message_id: &MessageId) -> bool {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.entries.remove(message_id) {
            // Also remove from timeout index
            Self::remove_from_timeout_index(&mut inner, entry.next_retry, message_id);

            // Only count as success if we actually sent a graft (retry_count > 0)
            let was_graft_sent = entry.retry_count > 0;

            // Record metrics if the feature is enabled
            #[cfg(feature = "metrics")]
            if was_graft_sent {
                crate::metrics::record_graft_success();
                let latency = entry.created.elapsed().as_secs_f64();
                crate::metrics::record_graft_latency(latency);
            }

            return was_graft_sent;
        }
        false
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
    pub fn get_expired(&self) -> Vec<ExpiredGraft<I>> {
        let (expired, _) = self.get_expired_with_failures();
        expired
    }

    /// Get expired entries and failed entries (max retries exceeded).
    ///
    /// This operation is O(K) where K is the number of expired entries,
    /// NOT O(N) where N is total pending entries.
    ///
    /// Returns:
    /// - `Vec<ExpiredGraft<I>>`: Entries that need retry
    /// - `Vec<FailedGraft<I>>`: Entries that exceeded max retries (zombie peer detection)
    pub fn get_expired_with_failures(&self) -> (Vec<ExpiredGraft<I>>, Vec<FailedGraft<I>>) {
        let now = std::time::Instant::now();
        let mut inner = self.inner.lock();

        let mut expired = Vec::new();
        let mut failed = Vec::new();

        // Collect expired timeout keys - only iterate over times <= now
        // This is O(K) where K is the number of expired time buckets
        let expired_times: Vec<std::time::Instant> = inner
            .timeouts
            .range(..=now)
            .map(|(t, _)| *t)
            .collect();

        // Collect updates to apply after processing (to avoid borrow issues)
        let mut to_reschedule: Vec<(MessageId, std::time::Instant)> = Vec::new();
        let mut to_remove: Vec<MessageId> = Vec::new();

        // Process each expired time bucket
        for timeout in expired_times {
            // Remove the entire bucket from the BTreeMap
            let Some(message_ids) = inner.timeouts.remove(&timeout) else {
                continue;
            };

            for message_id in message_ids {
                // Get the entry - it might have been removed by message_received()
                let Some(entry) = inner.entries.get_mut(&message_id) else {
                    continue;
                };

                // Verify this entry is actually expired (defensive check)
                if now < entry.next_retry {
                    // Need to re-add to timeout index at correct time
                    to_reschedule.push((message_id, entry.next_retry));
                    continue;
                }

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
                    message_id,
                    peer,
                    round: entry.round,
                    retry_count: entry.retry_count,
                });

                entry.retry_count += 1;

                if entry.retry_count >= self.max_retries {
                    // Max retries exceeded, record failure for zombie detection
                    failed.push(FailedGraft {
                        message_id,
                        original_peer: entry.from.clone(),
                        total_retries: entry.retry_count,
                    });

                    // Record failure metric
                    #[cfg(feature = "metrics")]
                    crate::metrics::record_graft_failed();

                    // Mark for removal
                    to_remove.push(message_id);
                } else {
                    // Schedule next retry with backoff
                    let backoff = self.calculate_backoff(entry.retry_count);
                    let new_timeout = now + backoff;
                    entry.next_retry = new_timeout;
                    // Mark for rescheduling
                    to_reschedule.push((message_id, new_timeout));
                }
            }
        }

        // Apply deferred updates
        for id in to_remove {
            inner.entries.remove(&id);
        }
        for (id, timeout) in to_reschedule {
            Self::add_to_timeout_index(&mut inner, timeout, id);
        }

        (expired, failed)
    }

    /// Clear all pending entries.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.entries.clear();
        inner.timeouts.clear();
    }

    /// Get the number of pending entries.
    pub fn pending_count(&self) -> usize {
        self.inner.lock().entries.len()
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
        let timer: GraftTimer<u64> = GraftTimer::new(Duration::from_millis(50));

        let id = MessageId::new();
        timer.expect_message(id, 42u64, 0);

        // Not expired yet
        let expired = timer.get_expired();
        assert!(expired.is_empty());

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(100));

        let expired = timer.get_expired();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].message_id, id);
        assert_eq!(expired[0].peer, 42u64);
        assert_eq!(expired[0].retry_count, 0);
    }

    #[test]
    fn test_graft_timer_message_received() {
        let timer: GraftTimer<u64> = GraftTimer::new(Duration::from_millis(50));

        let id = MessageId::new();
        timer.expect_message(id, 42u64, 0);
        timer.message_received(&id);

        std::thread::sleep(Duration::from_millis(100));

        let expired = timer.get_expired();
        assert!(expired.is_empty());
    }

    #[test]
    fn test_graft_timer_backoff() {
        let timer: GraftTimer<u64> =
            GraftTimer::with_backoff(Duration::from_millis(20), Duration::from_millis(160), 3);

        let id = MessageId::new();
        timer.expect_message(id, 1u64, 0);

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
        let timer: GraftTimer<u64> =
            GraftTimer::with_backoff(Duration::from_millis(20), Duration::from_millis(200), 4);

        let id = MessageId::new();
        let primary = 1u64;
        let alt1 = 2u64;
        let alt2 = 3u64;
        timer.expect_message_with_alternatives(id, primary, vec![alt1, alt2], 0);

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
