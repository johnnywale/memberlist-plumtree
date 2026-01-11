//! Message cache for storing messages for Graft requests.
//!
//! Uses a combination of HashMap and time-ordered eviction to provide
//! efficient O(1) lookups while managing memory usage.

use bytes::Bytes;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use super::MessageId;

/// Entry in the message cache.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Message payload (Arc for zero-copy sharing).
    payload: Arc<Bytes>,
    /// When this entry was inserted.
    inserted_at: Instant,
    /// Number of times this message has been accessed.
    /// Reserved for future use in cache eviction policies.
    #[allow(dead_code)]
    access_count: u32,
}

/// Thread-safe message cache with TTL-based eviction.
///
/// Messages are stored for a configurable duration to serve Graft
/// requests from nodes that missed the initial broadcast.
#[derive(Debug)]
pub struct MessageCache {
    /// Inner cache state protected by RwLock.
    inner: RwLock<CacheInner>,
    /// Time-to-live for cache entries.
    ttl: Duration,
    /// Maximum number of entries.
    max_size: usize,
}

#[derive(Debug)]
struct CacheInner {
    /// Map from message ID to cache entry.
    entries: HashMap<MessageId, CacheEntry>,
    /// Insertion order for LRU eviction (oldest first).
    insertion_order: Vec<(MessageId, Instant)>,
}

impl MessageCache {
    /// Create a new message cache with the specified TTL and max size.
    pub fn new(ttl: Duration, max_size: usize) -> Self {
        Self {
            inner: RwLock::new(CacheInner {
                entries: HashMap::with_capacity(max_size.min(1024)),
                insertion_order: Vec::with_capacity(max_size.min(1024)),
            }),
            ttl,
            max_size,
        }
    }

    /// Insert a message into the cache.
    ///
    /// If the message already exists, this is a no-op.
    /// If the cache is full, oldest entries are evicted.
    pub fn insert(&self, id: MessageId, payload: Bytes) {
        let now = Instant::now();
        let payload = Arc::new(payload);

        let mut inner = self.inner.write();

        // Check if already exists
        if inner.entries.contains_key(&id) {
            return;
        }

        // Evict expired entries first
        self.evict_expired_locked(&mut inner, now);

        // Evict oldest if still over capacity
        while inner.entries.len() >= self.max_size {
            if let Some((old_id, _)) = inner.insertion_order.first().cloned() {
                inner.entries.remove(&old_id);
                inner.insertion_order.remove(0);
            } else {
                break;
            }
        }

        // Insert new entry
        inner.entries.insert(
            id,
            CacheEntry {
                payload,
                inserted_at: now,
                access_count: 0,
            },
        );
        inner.insertion_order.push((id, now));
    }

    /// Get a message from the cache.
    ///
    /// Returns `None` if the message is not in the cache or has expired.
    /// Returns a cloned Arc to avoid holding locks during message sending.
    pub fn get(&self, id: &MessageId) -> Option<Arc<Bytes>> {
        let now = Instant::now();

        // Fast path: read lock
        {
            let inner = self.inner.read();
            if let Some(entry) = inner.entries.get(id) {
                if now.duration_since(entry.inserted_at) <= self.ttl {
                    return Some(entry.payload.clone());
                }
            }
        }

        None
    }

    /// Check if a message exists in the cache (without returning it).
    pub fn contains(&self, id: &MessageId) -> bool {
        let now = Instant::now();
        let inner = self.inner.read();

        if let Some(entry) = inner.entries.get(id) {
            now.duration_since(entry.inserted_at) <= self.ttl
        } else {
            false
        }
    }

    /// Get multiple messages from the cache.
    ///
    /// Returns a map of found message IDs to their payloads.
    pub fn get_many(&self, ids: &[MessageId]) -> HashMap<MessageId, Arc<Bytes>> {
        let now = Instant::now();
        let inner = self.inner.read();

        let mut result = HashMap::with_capacity(ids.len());
        for id in ids {
            if let Some(entry) = inner.entries.get(id) {
                if now.duration_since(entry.inserted_at) <= self.ttl {
                    result.insert(*id, entry.payload.clone());
                }
            }
        }
        result
    }

    /// Remove a message from the cache.
    pub fn remove(&self, id: &MessageId) -> Option<Arc<Bytes>> {
        let mut inner = self.inner.write();

        if let Some(entry) = inner.entries.remove(id) {
            inner.insertion_order.retain(|(i, _)| i != id);
            Some(entry.payload)
        } else {
            None
        }
    }

    /// Get the number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.inner.read().entries.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().entries.is_empty()
    }

    /// Remove all expired entries from the cache.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        let mut inner = self.inner.write();
        self.evict_expired_locked(&mut inner, now);
    }

    /// Internal method to evict expired entries while holding the lock.
    fn evict_expired_locked(&self, inner: &mut CacheInner, now: Instant) {
        // Find cutoff point in insertion order
        let cutoff = now - self.ttl;
        let mut remove_count = 0;

        for (_, inserted_at) in &inner.insertion_order {
            if *inserted_at < cutoff {
                remove_count += 1;
            } else {
                // insertion_order is sorted by time, so we can stop here
                break;
            }
        }

        if remove_count > 0 {
            // Remove expired entries
            let to_remove: Vec<_> = inner.insertion_order.drain(..remove_count).collect();
            for (id, _) in to_remove {
                inner.entries.remove(&id);
            }
        }
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.entries.clear();
        inner.insertion_order.clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read();
        CacheStats {
            entries: inner.entries.len(),
            capacity: self.max_size,
            ttl: self.ttl,
        }
    }
}

/// Statistics about the message cache.
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    /// Number of entries currently in the cache.
    pub entries: usize,
    /// Maximum capacity of the cache.
    pub capacity: usize,
    /// Time-to-live for cache entries.
    pub ttl: Duration,
}

impl Default for MessageCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(60), 10000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let cache = MessageCache::new(Duration::from_secs(60), 100);

        let id = MessageId::new();
        let payload = Bytes::from_static(b"test payload");

        cache.insert(id, payload.clone());

        let retrieved = cache.get(&id).unwrap();
        assert_eq!(&**retrieved, &payload);
    }

    #[test]
    fn test_contains() {
        let cache = MessageCache::new(Duration::from_secs(60), 100);

        let id = MessageId::new();
        assert!(!cache.contains(&id));

        cache.insert(id, Bytes::from_static(b"test"));
        assert!(cache.contains(&id));
    }

    #[test]
    fn test_duplicate_insert() {
        let cache = MessageCache::new(Duration::from_secs(60), 100);

        let id = MessageId::new();
        cache.insert(id, Bytes::from_static(b"first"));
        cache.insert(id, Bytes::from_static(b"second"));

        // Should still have the first payload
        let retrieved = cache.get(&id).unwrap();
        assert_eq!(&**retrieved, b"first");
    }

    #[test]
    fn test_capacity_eviction() {
        let cache = MessageCache::new(Duration::from_secs(60), 3);

        let ids: Vec<_> = (0..5).map(|_| MessageId::new()).collect();

        for (i, id) in ids.iter().enumerate() {
            cache.insert(*id, Bytes::from(format!("payload {}", i)));
        }

        // Only last 3 should remain
        assert_eq!(cache.len(), 3);
        assert!(!cache.contains(&ids[0]));
        assert!(!cache.contains(&ids[1]));
        assert!(cache.contains(&ids[2]));
        assert!(cache.contains(&ids[3]));
        assert!(cache.contains(&ids[4]));
    }

    #[test]
    fn test_ttl_expiration() {
        let cache = MessageCache::new(Duration::from_millis(50), 100);

        let id = MessageId::new();
        cache.insert(id, Bytes::from_static(b"test"));

        assert!(cache.contains(&id));

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(100));

        assert!(!cache.contains(&id));
    }

    #[test]
    fn test_remove() {
        let cache = MessageCache::new(Duration::from_secs(60), 100);

        let id = MessageId::new();
        cache.insert(id, Bytes::from_static(b"test"));

        let removed = cache.remove(&id);
        assert!(removed.is_some());
        assert!(!cache.contains(&id));
    }

    #[test]
    fn test_get_many() {
        let cache = MessageCache::new(Duration::from_secs(60), 100);

        let ids: Vec<_> = (0..5).map(|_| MessageId::new()).collect();
        for (i, id) in ids.iter().enumerate() {
            cache.insert(*id, Bytes::from(format!("payload {}", i)));
        }

        let result = cache.get_many(&ids[1..4]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_clear() {
        let cache = MessageCache::new(Duration::from_secs(60), 100);

        for _ in 0..10 {
            cache.insert(MessageId::new(), Bytes::from_static(b"test"));
        }

        assert_eq!(cache.len(), 10);
        cache.clear();
        assert!(cache.is_empty());
    }
}
