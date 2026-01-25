//! In-memory message storage backend.
//!
//! This is the default storage backend that keeps all messages in memory.
//! It does not provide persistence across restarts, but is useful for:
//!
//! - Development and testing
//! - Deployments where persistence is not required
//! - As a cache layer in front of a persistent backend

use super::{MessageStore, StoredMessage};
use crate::MessageId;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::error::Error;

/// In-memory message store with automatic eviction.
///
/// Messages are indexed by both ID (for O(1) lookup) and timestamp
/// (for efficient range queries and pruning).
///
/// When the store reaches capacity, the oldest messages are evicted
/// automatically. Note that if multiple messages share the same timestamp,
/// all messages at that timestamp bucket are evicted together.
pub struct MemoryStore {
    inner: RwLock<MemoryStoreInner>,
    max_size: usize,
}

struct MemoryStoreInner {
    /// Primary index: message ID -> stored message
    messages: HashMap<MessageId, StoredMessage>,
    /// Time index: timestamp -> message IDs (multiple messages can share same timestamp)
    time_index: BTreeMap<u64, Vec<MessageId>>,
}

impl MemoryStore {
    /// Create a new in-memory store with the specified maximum size.
    ///
    /// When the store exceeds `max_size` messages, the oldest messages
    /// are automatically evicted.
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: RwLock::new(MemoryStoreInner {
                messages: HashMap::new(),
                time_index: BTreeMap::new(),
            }),
            max_size,
        }
    }

    /// Get the current number of messages in the store.
    pub fn len(&self) -> usize {
        self.inner.read().messages.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().messages.is_empty()
    }

    /// Get the maximum capacity of the store.
    pub fn capacity(&self) -> usize {
        self.max_size
    }

    /// Clear all messages from the store.
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.messages.clear();
        inner.time_index.clear();
    }

    /// Get all message IDs in the store (for testing/debugging).
    pub fn all_ids(&self) -> Vec<MessageId> {
        self.inner.read().messages.keys().copied().collect()
    }

    /// Synchronous insert for use in sync contexts (e.g., delegate callbacks).
    ///
    /// This is equivalent to `insert()` but doesn't require async.
    /// Returns `Ok(true)` if the message was new, `Ok(false)` if it already existed.
    pub fn try_insert_sync(
        &self,
        msg: &StoredMessage,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let mut inner = self.inner.write();

        // Check if message already exists
        if inner.messages.contains_key(&msg.id) {
            return Ok(false);
        }

        // Evict oldest messages if at capacity
        while inner.messages.len() >= self.max_size {
            if let Some((&oldest_ts, _)) = inner.time_index.first_key_value() {
                if let Some(ids) = inner.time_index.remove(&oldest_ts) {
                    for id in ids {
                        inner.messages.remove(&id);
                    }
                }
            } else {
                break;
            }
        }

        // Insert into time index
        inner
            .time_index
            .entry(msg.timestamp)
            .or_default()
            .push(msg.id);

        // Insert into primary index
        inner.messages.insert(msg.id, msg.clone());

        Ok(true)
    }

    /// Synchronous get for use in sync contexts.
    pub fn get_sync(&self, id: &MessageId) -> Option<StoredMessage> {
        self.inner.read().messages.get(id).cloned()
    }

    /// Synchronous contains check.
    pub fn contains_sync(&self, id: &MessageId) -> bool {
        self.inner.read().messages.contains_key(id)
    }
}

impl MessageStore for MemoryStore {
    async fn insert(&self, msg: &StoredMessage) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.write();

        // Check if message already exists
        if inner.messages.contains_key(&msg.id) {
            return Ok(false);
        }

        // Evict oldest messages if at capacity
        // NOTE: If multiple messages share the same timestamp, all will be removed.
        // This is intentional - we evict by time bucket, not individual messages.
        while inner.messages.len() >= self.max_size {
            if let Some((&oldest_ts, _)) = inner.time_index.first_key_value() {
                if let Some(ids) = inner.time_index.remove(&oldest_ts) {
                    for id in ids {
                        inner.messages.remove(&id);
                    }
                }
            } else {
                break;
            }
        }

        // Insert into time index
        inner
            .time_index
            .entry(msg.timestamp)
            .or_default()
            .push(msg.id);

        // Insert into primary index
        inner.messages.insert(msg.id, msg.clone());

        Ok(true)
    }

    async fn get(
        &self,
        id: &MessageId,
    ) -> Result<Option<StoredMessage>, Box<dyn Error + Send + Sync>> {
        Ok(self.inner.read().messages.get(id).cloned())
    }

    async fn contains(&self, id: &MessageId) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Ok(self.inner.read().messages.contains_key(id))
    }

    async fn get_range(
        &self,
        start: u64,
        end: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<MessageId>, bool), Box<dyn Error + Send + Sync>> {
        let inner = self.inner.read();
        let mut result = Vec::new();
        let mut count = 0;
        let mut skipped = 0;

        // Iterate through time buckets in range
        for (_, ids) in inner.time_index.range(start..=end) {
            for id in ids {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }
                if count >= limit {
                    return Ok((result, true)); // has_more = true
                }
                result.push(*id);
                count += 1;
            }
        }

        Ok((result, false)) // has_more = false
    }

    async fn prune(&self, older_than: u64) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let mut inner = self.inner.write();
        let mut removed = 0;

        // Collect timestamps to remove
        let to_remove: Vec<u64> = inner
            .time_index
            .range(..older_than)
            .map(|(&ts, _)| ts)
            .collect();

        // Remove messages at those timestamps
        for ts in to_remove {
            if let Some(ids) = inner.time_index.remove(&ts) {
                for id in ids {
                    inner.messages.remove(&id);
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }

    async fn count(&self) -> Result<usize, Box<dyn Error + Send + Sync>> {
        Ok(self.inner.read().messages.len())
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new(100_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_message(ts: u64) -> StoredMessage {
        StoredMessage::with_timestamp(MessageId::new(), 0, Bytes::from_static(b"test"), ts)
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let store = MemoryStore::new(100);
        let msg = make_message(1000);
        let id = msg.id;

        // Insert should return true for new message
        assert!(store.insert(&msg).await.unwrap());

        // Insert same message should return false
        assert!(!store.insert(&msg).await.unwrap());

        // Get should return the message
        let retrieved = store.get(&id).await.unwrap().unwrap();
        assert_eq!(retrieved.id, id);
        assert_eq!(retrieved.timestamp, 1000);
    }

    #[tokio::test]
    async fn test_contains() {
        let store = MemoryStore::new(100);
        let msg = make_message(1000);
        let id = msg.id;

        assert!(!store.contains(&id).await.unwrap());
        store.insert(&msg).await.unwrap();
        assert!(store.contains(&id).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_range() {
        let store = MemoryStore::new(100);

        // Insert messages at different timestamps
        for ts in [100, 200, 300, 400, 500] {
            store.insert(&make_message(ts)).await.unwrap();
        }

        // Get range [200, 400]
        let (ids, has_more) = store.get_range(200, 400, 10, 0).await.unwrap();
        assert_eq!(ids.len(), 3);
        assert!(!has_more);

        // Test pagination
        let (ids, has_more) = store.get_range(100, 500, 2, 0).await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(has_more);

        // Test offset
        let (ids, has_more) = store.get_range(100, 500, 2, 2).await.unwrap();
        assert_eq!(ids.len(), 2);
        assert!(has_more);

        let (ids, has_more) = store.get_range(100, 500, 2, 4).await.unwrap();
        assert_eq!(ids.len(), 1);
        assert!(!has_more);
    }

    #[tokio::test]
    async fn test_prune() {
        let store = MemoryStore::new(100);

        // Insert messages
        for ts in [100, 200, 300, 400, 500] {
            store.insert(&make_message(ts)).await.unwrap();
        }

        assert_eq!(store.count().await.unwrap(), 5);

        // Prune messages older than 300
        let removed = store.prune(300).await.unwrap();
        assert_eq!(removed, 2); // ts=100, ts=200

        assert_eq!(store.count().await.unwrap(), 3);

        // Remaining messages should be ts >= 300
        let (ids, _) = store.get_range(0, 1000, 100, 0).await.unwrap();
        assert_eq!(ids.len(), 3);
    }

    #[tokio::test]
    async fn test_eviction_at_capacity() {
        let store = MemoryStore::new(3);

        // Insert 3 messages
        for ts in [100, 200, 300] {
            store.insert(&make_message(ts)).await.unwrap();
        }
        assert_eq!(store.count().await.unwrap(), 3);

        // Insert 4th message - should evict oldest
        store.insert(&make_message(400)).await.unwrap();
        assert_eq!(store.count().await.unwrap(), 3);

        // Message at ts=100 should be evicted
        let (ids, _) = store.get_range(100, 100, 10, 0).await.unwrap();
        assert!(ids.is_empty());

        // Messages at ts=200,300,400 should remain
        let (ids, _) = store.get_range(200, 400, 10, 0).await.unwrap();
        assert_eq!(ids.len(), 3);
    }

    #[tokio::test]
    async fn test_multiple_messages_same_timestamp() {
        let store = MemoryStore::new(100);

        // Insert multiple messages at same timestamp
        let msg1 = make_message(1000);
        let msg2 = make_message(1000);
        let msg3 = make_message(1000);

        store.insert(&msg1).await.unwrap();
        store.insert(&msg2).await.unwrap();
        store.insert(&msg3).await.unwrap();

        assert_eq!(store.count().await.unwrap(), 3);

        // All should be in range
        let (ids, _) = store.get_range(1000, 1000, 10, 0).await.unwrap();
        assert_eq!(ids.len(), 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let store = MemoryStore::new(100);

        for ts in [100, 200, 300] {
            store.insert(&make_message(ts)).await.unwrap();
        }

        assert_eq!(store.len(), 3);
        store.clear();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }
}
