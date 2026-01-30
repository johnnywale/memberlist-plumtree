//! Session ticket caching for QUIC 0-RTT connection resumption.
//!
//! # ⚠️ Integration Status
//!
//! **This module is NOT integrated with the QUIC transport.**
//!
//! The transport uses rustls's built-in session cache via `Resumption::in_memory_sessions()`,
//! which handles session ticket storage internally and is sufficient for most use cases.
//!
//! ## Why This Module Exists
//!
//! This module provides a **reference implementation** for custom session storage.
//! It may be useful if you need:
//! - **Persistent storage**: Store session tickets in Redis, files, or a database
//!   so they survive process restarts
//! - **Distributed caching**: Share session tickets across multiple instances
//! - **Custom eviction policies**: Different caching strategies than LRU
//!
//! ## How to Integrate (Advanced)
//!
//! To use this with rustls, you would need to implement `rustls::client::StoresClientSessions`
//! for your custom storage and configure it in the TLS client config:
//!
//! ```ignore
//! // In tls.rs, you would change:
//! // crypto.resumption = rustls::client::Resumption::in_memory_sessions(capacity);
//!
//! // To use your custom store:
//! let custom_store = Arc::new(MySessionStore::new());
//! crypto.resumption = rustls::client::Resumption::store(custom_store);
//! ```
//!
//! # Security Warning
//!
//! 0-RTT data is **replayable**! When 0-RTT is enabled, ALL messages (including
//! Graft/Prune control messages) may be replayed by an attacker.
//!
//! # Example (Reference Implementation)
//!
//! ```
//! use memberlist_plumtree::{LruSessionCache, SessionTicketStore};
//!
//! // Create a cache with capacity for 1000 sessions
//! let cache = LruSessionCache::new(1000);
//!
//! // Store a session ticket
//! cache.store("server.example.com", vec![1, 2, 3, 4]);
//!
//! // Retrieve for 0-RTT resumption
//! if let Some(ticket) = cache.get("server.example.com") {
//!     println!("Found ticket: {:?}", ticket);
//! }
//! ```

use std::sync::Arc;

use parking_lot::RwLock;

/// Trait for session ticket storage.
///
/// Implementations must be thread-safe as they may be accessed from multiple
/// async tasks concurrently.
pub trait SessionTicketStore: Send + Sync + std::fmt::Debug {
    /// Get a session ticket for the given server name.
    ///
    /// Returns `Some(ticket)` if a valid ticket exists, `None` otherwise.
    fn get(&self, server_name: &str) -> Option<Vec<u8>>;

    /// Store a session ticket for the given server name.
    ///
    /// If a ticket already exists for this server, it will be replaced.
    fn store(&self, server_name: &str, ticket: Vec<u8>);

    /// Remove a session ticket for the given server name.
    ///
    /// Call this when a ticket is known to be invalid (e.g., server rejected it).
    fn remove(&self, server_name: &str);

    /// Clear all stored session tickets.
    fn clear(&self);

    /// Get the number of stored tickets.
    fn len(&self) -> usize;

    /// Check if the cache is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// LRU-based session ticket cache.
///
/// This cache stores session tickets in a least-recently-used (LRU) order,
/// automatically evicting the oldest entries when capacity is reached.
///
/// # Thread Safety
///
/// This implementation uses a `RwLock` to allow concurrent reads while
/// serializing writes. This is appropriate for the typical access pattern
/// where reads (checking for cached tickets) are much more common than
/// writes (storing new tickets).
#[derive(Debug)]
pub struct LruSessionCache {
    cache: RwLock<lru::LruCache<String, Vec<u8>>>,
    capacity: usize,
}

impl LruSessionCache {
    /// Create a new LRU session cache with the given capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Maximum number of session tickets to store
    pub fn new(capacity: usize) -> Self {
        // Use NonZeroUsize for LruCache
        let cap = std::num::NonZeroUsize::new(capacity.max(1)).unwrap();
        Self {
            cache: RwLock::new(lru::LruCache::new(cap)),
            capacity,
        }
    }

    /// Get the capacity of this cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get current fill ratio (0.0 to 1.0).
    pub fn fill_ratio(&self) -> f64 {
        let len = self.len();
        if self.capacity == 0 {
            return 0.0;
        }
        len as f64 / self.capacity as f64
    }
}

impl SessionTicketStore for LruSessionCache {
    fn get(&self, server_name: &str) -> Option<Vec<u8>> {
        // Note: LruCache.get() requires mutable access to update LRU order
        // We use write lock here to maintain proper LRU ordering
        let mut cache = self.cache.write();
        cache.get(server_name).cloned()
    }

    fn store(&self, server_name: &str, ticket: Vec<u8>) {
        let mut cache = self.cache.write();
        cache.put(server_name.to_string(), ticket);
    }

    fn remove(&self, server_name: &str) {
        let mut cache = self.cache.write();
        cache.pop(server_name);
    }

    fn clear(&self) {
        let mut cache = self.cache.write();
        cache.clear();
    }

    fn len(&self) -> usize {
        let cache = self.cache.read();
        cache.len()
    }
}

impl Clone for LruSessionCache {
    fn clone(&self) -> Self {
        // Clone creates a new cache with the same capacity but empty contents
        // This is intentional - session tickets shouldn't be shared across clones
        Self::new(self.capacity)
    }
}

/// A no-op session cache that doesn't store anything.
///
/// Use this when 0-RTT is disabled to avoid the overhead of cache operations.
#[derive(Debug, Clone, Default)]
pub struct NoopSessionCache;

impl NoopSessionCache {
    /// Create a new no-op session cache.
    pub fn new() -> Self {
        Self
    }
}

impl SessionTicketStore for NoopSessionCache {
    fn get(&self, _server_name: &str) -> Option<Vec<u8>> {
        None
    }

    fn store(&self, _server_name: &str, _ticket: Vec<u8>) {
        // No-op
    }

    fn remove(&self, _server_name: &str) {
        // No-op
    }

    fn clear(&self) {
        // No-op
    }

    fn len(&self) -> usize {
        0
    }
}

// Implement for Arc<T> where T: SessionTicketStore
impl<T: SessionTicketStore> SessionTicketStore for Arc<T> {
    fn get(&self, server_name: &str) -> Option<Vec<u8>> {
        (**self).get(server_name)
    }

    fn store(&self, server_name: &str, ticket: Vec<u8>) {
        (**self).store(server_name, ticket)
    }

    fn remove(&self, server_name: &str) {
        (**self).remove(server_name)
    }

    fn clear(&self) {
        (**self).clear()
    }

    fn len(&self) -> usize {
        (**self).len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_cache_basic() {
        let cache = LruSessionCache::new(10);

        // Initially empty
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);

        // Store a ticket
        cache.store("server1.example.com", vec![1, 2, 3, 4]);
        assert_eq!(cache.len(), 1);

        // Retrieve it
        let ticket = cache.get("server1.example.com");
        assert!(ticket.is_some());
        assert_eq!(ticket.unwrap(), vec![1, 2, 3, 4]);

        // Non-existent key
        assert!(cache.get("nonexistent.example.com").is_none());
    }

    #[test]
    fn test_lru_cache_eviction() {
        // Small cache
        let cache = LruSessionCache::new(3);

        cache.store("server1", vec![1]);
        cache.store("server2", vec![2]);
        cache.store("server3", vec![3]);

        assert_eq!(cache.len(), 3);

        // Adding a 4th entry should evict the oldest (server1)
        cache.store("server4", vec![4]);

        assert_eq!(cache.len(), 3);
        assert!(cache.get("server1").is_none()); // Evicted
        assert!(cache.get("server2").is_some());
        assert!(cache.get("server3").is_some());
        assert!(cache.get("server4").is_some());
    }

    #[test]
    fn test_lru_cache_lru_order() {
        let cache = LruSessionCache::new(3);

        cache.store("a", vec![1]);
        cache.store("b", vec![2]);
        cache.store("c", vec![3]);

        // Access 'a' to make it recently used
        cache.get("a");

        // Add 'd' - should evict 'b' (least recently used)
        cache.store("d", vec![4]);

        assert!(cache.get("a").is_some()); // Still there (was accessed)
        assert!(cache.get("b").is_none()); // Evicted (LRU)
        assert!(cache.get("c").is_some());
        assert!(cache.get("d").is_some());
    }

    #[test]
    fn test_lru_cache_remove() {
        let cache = LruSessionCache::new(10);

        cache.store("server1", vec![1]);
        cache.store("server2", vec![2]);

        assert_eq!(cache.len(), 2);

        cache.remove("server1");

        assert_eq!(cache.len(), 1);
        assert!(cache.get("server1").is_none());
        assert!(cache.get("server2").is_some());
    }

    #[test]
    fn test_lru_cache_clear() {
        let cache = LruSessionCache::new(10);

        cache.store("server1", vec![1]);
        cache.store("server2", vec![2]);
        cache.store("server3", vec![3]);

        assert_eq!(cache.len(), 3);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_lru_cache_update() {
        let cache = LruSessionCache::new(10);

        cache.store("server1", vec![1, 1, 1]);
        let ticket = cache.get("server1").unwrap();
        assert_eq!(ticket, vec![1, 1, 1]);

        // Update with new ticket
        cache.store("server1", vec![2, 2, 2]);
        let ticket = cache.get("server1").unwrap();
        assert_eq!(ticket, vec![2, 2, 2]);

        // Should still only have 1 entry
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_lru_cache_capacity() {
        let cache = LruSessionCache::new(100);
        assert_eq!(cache.capacity(), 100);

        let cache2 = LruSessionCache::new(0);
        // Capacity should be at least 1
        assert_eq!(cache2.capacity(), 0); // But we store 0
    }

    #[test]
    fn test_lru_cache_fill_ratio() {
        let cache = LruSessionCache::new(10);

        assert_eq!(cache.fill_ratio(), 0.0);

        cache.store("a", vec![1]);
        assert_eq!(cache.fill_ratio(), 0.1);

        cache.store("b", vec![2]);
        cache.store("c", vec![3]);
        cache.store("d", vec![4]);
        cache.store("e", vec![5]);
        assert_eq!(cache.fill_ratio(), 0.5);
    }

    #[test]
    fn test_noop_cache() {
        let cache = NoopSessionCache::new();

        // Store does nothing
        cache.store("server1", vec![1, 2, 3]);

        // Get always returns None
        assert!(cache.get("server1").is_none());

        // Always empty
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_arc_wrapper() {
        let cache = Arc::new(LruSessionCache::new(10));

        cache.store("server1", vec![1, 2, 3]);
        assert_eq!(cache.len(), 1);

        let ticket = cache.get("server1");
        assert!(ticket.is_some());
    }

    #[test]
    fn test_clone_creates_empty_cache() {
        let cache = LruSessionCache::new(100);
        cache.store("server1", vec![1, 2, 3]);
        cache.store("server2", vec![4, 5, 6]);

        assert_eq!(cache.len(), 2);

        let cloned = cache.clone();

        // Cloned cache should be empty but have same capacity
        assert_eq!(cloned.capacity(), 100);
        assert_eq!(cloned.len(), 0);
    }
}
