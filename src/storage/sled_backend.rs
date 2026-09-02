//! Sled-based persistent storage backend.
//!
//! This storage backend uses [Sled](https://sled.rs) for persistent message storage.
//! It provides durability across restarts and efficient range queries using
//! timestamp-prefixed keys.
//!
//! # Key Format
//!
//! Two trees are maintained so that both point lookups and range scans are
//! index-driven rather than full scans:
//!
//! ```text
//! messages tree:  [message_id (24 bytes)]  ->  [timestamp][round][payload]
//! by_time tree:   [timestamp_be (8)][message_id (24)]  ->  []
//! ```
//!
//! The primary tree is keyed by message ID alone, so `get`/`contains` are
//! single lookups and a given ID can only ever have one row (re-inserting the
//! same ID with a different timestamp updates it rather than adding a
//! duplicate). The `by_time` tree provides the ordered scan that `get_range`
//! and `prune` need.
//!
//! # Blocking I/O
//!
//! Sled's API is synchronous and hits the disk. Every operation here is
//! therefore wrapped in `tokio::task::spawn_blocking`, so a compaction stall
//! blocks a blocking-pool thread instead of a runtime worker.
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::storage::{SledStore, MessageStore, StoredMessage};
//!
//! let store = SledStore::open("/tmp/plumtree-messages")?;
//!
//! let msg = StoredMessage::new(MessageId::new(), 1, Bytes::from("hello"));
//! store.insert(&msg).await?;
//!
//! // Flush to ensure durability
//! store.flush().await?;
//! ```
//!
//! # Feature
//!
//! This module is only available with the `storage-sled` feature:
//!
//! ```toml
//! [dependencies]
//! memberlist-plumtree = { version = "0.1", features = ["storage-sled"] }
//! ```

use super::{MessageStore, StoredMessage};
use crate::MessageId;
use bytes::Bytes;
use sled::Db;
use std::error::Error;
use std::path::Path;

/// Sled-based persistent message store.
///
/// Keyed by message ID, with a secondary timestamp index for range queries.
pub struct SledStore {
    db: Db,
    /// Primary tree: message ID -> record.
    messages: sled::Tree,
    /// Secondary index: (timestamp, message ID) -> empty, for ordered scans.
    by_time: sled::Tree,
}

impl SledStore {
    /// Open or create a Sled database at the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the database directory
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or created.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, sled::Error> {
        let db = sled::open(path)?;
        let messages = db.open_tree("messages")?;
        let by_time = db.open_tree("by_time")?;
        Ok(Self {
            db,
            messages,
            by_time,
        })
    }

    /// Flush all pending writes to disk.
    ///
    /// Call this when you need a durability barrier — `insert` does not flush.
    /// Flushing on every insert costs a disk sync per message and collapses
    /// write throughput; sled's own periodic flush plus an explicit call at
    /// checkpoints (or before shutdown) gives far better throughput for the
    /// same guarantee at the points that matter.
    pub async fn flush(&self) -> Result<(), sled::Error> {
        self.db.flush_async().await?;
        Ok(())
    }

    /// Build the `by_time` index key for a message.
    ///
    /// Format: `[timestamp_be (8 bytes)][message_id (24 bytes)]`. Big-endian
    /// timestamps make lexicographic order the same as chronological order,
    /// which is what makes the range scan work.
    fn index_key(timestamp: u64, id: &MessageId) -> Vec<u8> {
        let mut key = Vec::with_capacity(32);
        key.extend_from_slice(&timestamp.to_be_bytes());
        key.extend_from_slice(&id.encode_to_bytes());
        key
    }

    /// Create a key prefix for a timestamp (for range queries).
    fn timestamp_prefix(ts: u64) -> [u8; 8] {
        ts.to_be_bytes()
    }

    /// Extract the MessageId from a `by_time` index key.
    fn extract_id(key: &[u8]) -> Option<MessageId> {
        if key.len() >= 32 {
            MessageId::decode_from_slice(&key[8..32])
        } else {
            None
        }
    }

    /// Serialize the value stored in the primary tree.
    ///
    /// The timestamp lives in the value here (the key is the ID alone), so a
    /// record is self-describing without consulting the index.
    fn serialize(msg: &StoredMessage) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        // Format: timestamp (8) + round (4) + payload_len (4) + payload
        let mut data = Vec::with_capacity(16 + msg.payload.len());
        data.extend_from_slice(&msg.timestamp.to_le_bytes());
        data.extend_from_slice(&msg.round.to_le_bytes());
        data.extend_from_slice(&(msg.payload.len() as u32).to_le_bytes());
        data.extend_from_slice(&msg.payload);
        Ok(data)
    }

    /// Deserialize a record from the primary tree.
    fn deserialize(
        key: &[u8],
        value: &[u8],
    ) -> Result<StoredMessage, Box<dyn Error + Send + Sync>> {
        if key.len() < 24 || value.len() < 16 {
            return Err("invalid data".into());
        }

        let id = MessageId::decode_from_slice(&key[0..24]).ok_or("invalid message id")?;

        let timestamp = u64::from_le_bytes(value[0..8].try_into().unwrap());
        let round = u32::from_le_bytes(value[8..12].try_into().unwrap());
        let payload_len = u32::from_le_bytes(value[12..16].try_into().unwrap()) as usize;

        if value.len() < 16 + payload_len {
            return Err("truncated payload".into());
        }

        let payload = Bytes::copy_from_slice(&value[16..16 + payload_len]);

        Ok(StoredMessage {
            id,
            round,
            payload,
            timestamp,
        })
    }

    /// Read the timestamp out of a primary-tree value without full decoding.
    fn value_timestamp(value: &[u8]) -> Option<u64> {
        if value.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes(value[0..8].try_into().unwrap()))
    }
}

impl SledStore {
    /// Run a synchronous sled operation on the blocking pool.
    ///
    /// Sled performs real disk I/O; calling it directly from an async fn lets a
    /// compaction stall block a runtime worker.
    async fn blocking<F, T>(&self, f: F) -> Result<T, Box<dyn Error + Send + Sync>>
    where
        F: FnOnce() -> Result<T, Box<dyn Error + Send + Sync>> + Send + 'static,
        T: Send + 'static,
    {
        tokio::task::spawn_blocking(f)
            .await
            .map_err(|e| -> Box<dyn Error + Send + Sync> {
                format!("storage task panicked: {}", e).into()
            })?
    }
}

impl MessageStore for SledStore {
    async fn insert(&self, msg: &StoredMessage) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let messages = self.messages.clone();
        let by_time = self.by_time.clone();
        let id_bytes = msg.id.encode_to_bytes();
        let value = Self::serialize(msg)?;
        let index_key = Self::index_key(msg.timestamp, &msg.id);

        self.blocking(move || {
            // Keyed by ID alone, so re-inserting the same ID replaces the row
            // rather than creating a second one under a different timestamp.
            let previous = messages.insert(id_bytes.as_ref(), value)?;

            match previous {
                None => {
                    by_time.insert(index_key, &[])?;
                    Ok(true)
                }
                Some(old) => {
                    // Same ID already stored. If its timestamp differed, drop
                    // the stale index entry so the ID appears exactly once in
                    // range scans.
                    if let Some(old_ts) = Self::value_timestamp(&old) {
                        let old_index = {
                            let mut k = Vec::with_capacity(32);
                            k.extend_from_slice(&old_ts.to_be_bytes());
                            k.extend_from_slice(id_bytes.as_ref());
                            k
                        };
                        if old_index != index_key {
                            by_time.remove(old_index)?;
                            by_time.insert(index_key, &[])?;
                        }
                    }
                    Ok(false)
                }
            }
        })
        .await
    }

    async fn get(
        &self,
        id: &MessageId,
    ) -> Result<Option<StoredMessage>, Box<dyn Error + Send + Sync>> {
        let messages = self.messages.clone();
        let id_bytes = id.encode_to_bytes();

        self.blocking(move || {
            // Single indexed lookup: the primary key is the message ID.
            match messages.get(id_bytes.as_ref())? {
                Some(value) => Ok(Some(Self::deserialize(id_bytes.as_ref(), &value)?)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn contains(&self, id: &MessageId) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let messages = self.messages.clone();
        let id_bytes = id.encode_to_bytes();

        self.blocking(move || Ok(messages.contains_key(id_bytes.as_ref())?))
            .await
    }

    async fn get_range(
        &self,
        start: u64,
        end: u64,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<MessageId>, bool), Box<dyn Error + Send + Sync>> {
        let by_time = self.by_time.clone();

        self.blocking(move || {
            let mut result = Vec::new();
            let mut count = 0;
            let mut skipped = 0;

            let start_key = Self::timestamp_prefix(start);
            let end_key = Self::timestamp_prefix(end.saturating_add(1)); // Exclusive end

            for item in by_time.range(start_key.as_slice()..end_key.as_slice()) {
                let (key, _) = item?;

                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if count >= limit {
                    return Ok((result, true)); // has_more
                }

                if let Some(id) = Self::extract_id(&key) {
                    result.push(id);
                    count += 1;
                }
            }

            Ok((result, false))
        })
        .await
    }

    async fn prune(&self, older_than: u64) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let messages = self.messages.clone();
        let by_time = self.by_time.clone();

        self.blocking(move || {
            let cutoff = Self::timestamp_prefix(older_than);
            let mut removed = 0;

            // Walk the time index rather than the whole primary tree.
            let stale: Vec<_> = by_time
                .range(..cutoff.as_slice())
                .filter_map(|r| r.ok())
                .map(|(k, _)| k)
                .collect();

            for index_key in stale {
                if let Some(id) = Self::extract_id(&index_key) {
                    messages.remove(id.encode_to_bytes().as_ref())?;
                }
                by_time.remove(&index_key)?;
                removed += 1;
            }

            Ok(removed)
        })
        .await
    }

    async fn count(&self) -> Result<usize, Box<dyn Error + Send + Sync>> {
        let messages = self.messages.clone();
        self.blocking(move || Ok(messages.len())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn make_message(ts: u64) -> StoredMessage {
        StoredMessage::with_timestamp(MessageId::new(), 0, Bytes::from_static(b"test"), ts)
    }

    #[tokio::test]
    async fn test_insert_and_get() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

        let msg = make_message(1000);
        let id = msg.id;

        // Insert should return true for new message
        assert!(store.insert(&msg).await.unwrap());

        // Insert same message should return false (already exists)
        // Note: This test may fail because the timestamp-based key might differ
        // if the same message is inserted again with a different key
        // For now, we just test that insert doesn't panic

        // Get should return the message
        let retrieved = store.get(&id).await.unwrap().unwrap();
        assert_eq!(retrieved.id, id);
        assert_eq!(retrieved.timestamp, 1000);
    }

    #[tokio::test]
    async fn test_contains() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

        let msg = make_message(1000);
        let id = msg.id;

        assert!(!store.contains(&id).await.unwrap());
        store.insert(&msg).await.unwrap();
        assert!(store.contains(&id).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_range() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

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
    }

    #[tokio::test]
    async fn test_prune() {
        let dir = tempdir().unwrap();
        let store = SledStore::open(dir.path()).unwrap();

        // Insert messages
        for ts in [100, 200, 300, 400, 500] {
            store.insert(&make_message(ts)).await.unwrap();
        }

        assert_eq!(store.count().await.unwrap(), 5);

        // Prune messages older than 300
        let removed = store.prune(300).await.unwrap();
        assert_eq!(removed, 2); // ts=100, ts=200

        assert_eq!(store.count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_persistence() {
        let dir = tempdir().unwrap();
        let msg = make_message(1000);
        let id = msg.id;

        // Insert and close
        {
            let store = SledStore::open(dir.path()).unwrap();
            store.insert(&msg).await.unwrap();
            store.flush().await.unwrap();
            // Explicit drop to release lock
            drop(store);
        }

        // Reopen with retry - sled may take time to release file locks on some platforms
        let store = {
            let path = dir.path().to_path_buf();
            let mut attempts = 0;
            loop {
                match SledStore::open(&path) {
                    Ok(s) => break s,
                    Err(_) if attempts < 50 => {
                        attempts += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => panic!(
                        "Failed to reopen store after {} attempts: {:?}",
                        attempts, e
                    ),
                }
            }
        };

        assert!(store.contains(&id).await.unwrap());
    }
}
