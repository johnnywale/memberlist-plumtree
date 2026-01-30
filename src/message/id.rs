//! Message identifier type for Plumtree protocol.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

/// Counter for generating unique message IDs within a process
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Unique identifier for a Plumtree message.
///
/// Composed of:
/// - 8 bytes: timestamp (milliseconds since UNIX epoch)
/// - 8 bytes: node-local counter
/// - 8 bytes: random component
///
/// This provides uniqueness across nodes and time while being
/// efficient to generate and compare.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MessageId {
    timestamp: u64,
    counter: u64,
    random: u64,
}

impl MessageId {
    /// Size of the message ID in bytes when encoded.
    pub const ENCODED_SIZE: usize = 24;

    /// Create a new unique message ID.
    pub fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let random = rand::random::<u64>();

        Self {
            timestamp,
            counter,
            random,
        }
    }

    /// Create a message ID from raw components (for testing/deserialization).
    pub const fn from_parts(timestamp: u64, counter: u64, random: u64) -> Self {
        Self {
            timestamp,
            counter,
            random,
        }
    }

    /// Get the timestamp component.
    #[inline]
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get the counter component.
    #[inline]
    pub const fn counter(&self) -> u64 {
        self.counter
    }

    /// Get the random component.
    #[inline]
    pub const fn random(&self) -> u64 {
        self.random
    }

    /// Encode the message ID into bytes.
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64(self.timestamp);
        buf.put_u64(self.counter);
        buf.put_u64(self.random);
    }

    /// Encode the message ID into a new Bytes buffer.
    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::ENCODED_SIZE);
        self.encode(&mut buf);
        buf.freeze()
    }

    /// Decode a message ID from bytes.
    ///
    /// Returns `None` if the buffer is too small.
    pub fn decode(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < Self::ENCODED_SIZE {
            return None;
        }

        Some(Self {
            timestamp: buf.get_u64(),
            counter: buf.get_u64(),
            random: buf.get_u64(),
        })
    }

    /// Decode a message ID from a byte slice.
    pub fn decode_from_slice(data: &[u8]) -> Option<Self> {
        if data.len() < Self::ENCODED_SIZE {
            return None;
        }

        let mut buf = data;
        Self::decode(&mut buf)
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl Hash for MessageId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use all components for hashing
        state.write_u64(self.timestamp);
        state.write_u64(self.counter);
        state.write_u64(self.random);
    }
}

impl Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MessageId({:016x}-{:016x}-{:016x})",
            self.timestamp, self.counter, self.random
        )
    }
}

impl Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Shortened display format
        write!(
            f,
            "{:08x}{:04x}{:04x}",
            (self.timestamp & 0xFFFFFFFF) as u32,
            (self.counter & 0xFFFF) as u16,
            (self.random & 0xFFFF) as u16
        )
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            let bytes = self.encode_to_bytes();
            serializer.serialize_bytes(&bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_id_uniqueness() {
        let ids: Vec<MessageId> = (0..1000).map(|_| MessageId::new()).collect();

        // All IDs should be unique
        let mut seen = std::collections::HashSet::new();
        for id in &ids {
            assert!(seen.insert(*id), "Duplicate MessageId generated");
        }
    }

    #[test]
    fn test_message_id_encoding() {
        let id = MessageId::new();
        let encoded = id.encode_to_bytes();

        assert_eq!(encoded.len(), MessageId::ENCODED_SIZE);

        let decoded = MessageId::decode_from_slice(&encoded).unwrap();
        assert_eq!(id, decoded);
    }

    #[test]
    fn test_message_id_ordering() {
        let id1 = MessageId::from_parts(100, 0, 0);
        let id2 = MessageId::from_parts(200, 0, 0);

        assert!(id1 < id2);
    }
}
