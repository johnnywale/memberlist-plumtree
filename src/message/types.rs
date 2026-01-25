//! Plumtree protocol message types.
//!
//! # Zero-Copy Message Handling
//!
//! Messages use [`Bytes`] for payload storage, which provides zero-copy
//! semantics through reference counting:
//!
//! - **`Bytes::clone()`** is O(1) - only increments a reference count
//! - **Slicing** creates a new view without copying data
//! - **Forwarding messages** reuses the same underlying buffer
//!
//! This means forwarding a Gossip message to multiple peers only allocates
//! the envelope header; the payload bytes are shared across all recipients.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use smallvec::SmallVec;

use super::MessageId;

/// Maximum number of IHave message IDs in a single batch.
pub const MAX_IHAVE_BATCH: usize = 64;

/// Protocol message types for Plumtree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlumtreeMessage {
    /// Full message broadcast (eager push).
    ///
    /// Sent to eager peers with the complete message payload.
    Gossip {
        /// Unique message identifier.
        id: MessageId,
        /// Number of hops from the original sender.
        round: u32,
        /// Message payload.
        payload: Bytes,
    },

    /// Message announcement (lazy push).
    ///
    /// Sent to lazy peers with only message IDs, not full payloads.
    /// Recipients can request missing messages via Graft.
    IHave {
        /// List of message IDs that the sender has.
        message_ids: SmallVec<[MessageId; 8]>,
        /// Current round number.
        round: u32,
    },

    /// Request to establish eager link and get missing message.
    ///
    /// Sent when a node receives an IHave for a message it doesn't have.
    /// The recipient promotes the sender to an eager peer.
    Graft {
        /// ID of the message being requested.
        message_id: MessageId,
        /// Round at which the IHave was received.
        round: u32,
    },

    /// Request to demote to lazy peer.
    ///
    /// Sent when a node receives duplicate messages via multiple paths.
    /// Used to optimize the spanning tree by removing redundant edges.
    Prune,

    /// Anti-entropy sync message.
    ///
    /// Used for synchronization after network partitions or node restarts.
    Sync(SyncMessage),
}

/// Sync protocol messages for anti-entropy.
///
/// These messages enable recovery of missed messages by comparing
/// message state between peers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncMessage {
    /// Request state comparison.
    ///
    /// Sent to initiate sync by comparing root hashes.
    Request {
        /// XOR hash of all message hashes in the time range.
        root_hash: [u8; 32],
        /// Start of time range (milliseconds since UNIX epoch).
        time_start: u64,
        /// End of time range (milliseconds since UNIX epoch).
        time_end: u64,
    },

    /// Response with match status and IDs (paginated).
    ///
    /// If hashes match, no IDs are included. Otherwise, contains
    /// local message IDs for the requested time range.
    Response {
        /// Whether the root hashes matched.
        matches: bool,
        /// Message IDs in the time range (empty if matches=true).
        message_ids: SmallVec<[MessageId; 16]>,
        /// Whether there are more IDs beyond this response.
        has_more: bool,
    },

    /// Request specific messages.
    ///
    /// Sent after receiving a mismatch response to request missing messages.
    Pull {
        /// Message IDs to request.
        message_ids: SmallVec<[MessageId; 16]>,
    },

    /// Send requested messages.
    ///
    /// Response to a Pull request with actual message payloads.
    Push {
        /// Messages: (id, round, payload)
        messages: Vec<(MessageId, u32, Bytes)>,
    },
}

/// Message type tags for encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageTag {
    /// Gossip message tag.
    Gossip = 1,
    /// IHave message tag.
    IHave = 2,
    /// Graft message tag.
    Graft = 3,
    /// Prune message tag.
    Prune = 4,
    /// Sync Request message tag.
    SyncRequest = 5,
    /// Sync Response message tag.
    SyncResponse = 6,
    /// Sync Pull message tag.
    SyncPull = 7,
    /// Sync Push message tag.
    SyncPush = 8,
}

impl TryFrom<u8> for MessageTag {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MessageTag::Gossip),
            2 => Ok(MessageTag::IHave),
            3 => Ok(MessageTag::Graft),
            4 => Ok(MessageTag::Prune),
            5 => Ok(MessageTag::SyncRequest),
            6 => Ok(MessageTag::SyncResponse),
            7 => Ok(MessageTag::SyncPull),
            8 => Ok(MessageTag::SyncPush),
            _ => Err(value),
        }
    }
}

impl PlumtreeMessage {
    /// Encode the message into bytes.
    pub fn encode(&self, buf: &mut impl BufMut) {
        match self {
            PlumtreeMessage::Gossip { id, round, payload } => {
                buf.put_u8(MessageTag::Gossip as u8);
                id.encode(buf);
                buf.put_u32(*round);
                buf.put_u32(payload.len() as u32);
                buf.put_slice(payload);
            }
            PlumtreeMessage::IHave { message_ids, round } => {
                buf.put_u8(MessageTag::IHave as u8);
                buf.put_u32(*round);
                buf.put_u16(message_ids.len() as u16);
                for id in message_ids {
                    id.encode(buf);
                }
            }
            PlumtreeMessage::Graft { message_id, round } => {
                buf.put_u8(MessageTag::Graft as u8);
                message_id.encode(buf);
                buf.put_u32(*round);
            }
            PlumtreeMessage::Prune => {
                buf.put_u8(MessageTag::Prune as u8);
            }
            PlumtreeMessage::Sync(sync_msg) => {
                sync_msg.encode(buf);
            }
        }
    }

    /// Encode the message into a new Bytes buffer.
    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut buf);
        buf.freeze()
    }

    /// Calculate the encoded length of the message.
    pub fn encoded_len(&self) -> usize {
        match self {
            PlumtreeMessage::Gossip {
                id: _,
                round: _,
                payload,
            } => 1 + MessageId::ENCODED_SIZE + 4 + 4 + payload.len(),
            PlumtreeMessage::IHave {
                message_ids,
                round: _,
            } => 1 + 4 + 2 + (message_ids.len() * MessageId::ENCODED_SIZE),
            PlumtreeMessage::Graft { .. } => 1 + MessageId::ENCODED_SIZE + 4,
            PlumtreeMessage::Prune => 1,
            PlumtreeMessage::Sync(sync_msg) => sync_msg.encoded_len(),
        }
    }

    /// Decode a message from bytes.
    pub fn decode(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 1 {
            return None;
        }

        let tag = MessageTag::try_from(buf.get_u8()).ok()?;

        match tag {
            MessageTag::Gossip => {
                let id = MessageId::decode(buf)?;
                if buf.remaining() < 8 {
                    return None;
                }
                let round = buf.get_u32();
                let payload_len = buf.get_u32() as usize;
                if buf.remaining() < payload_len {
                    return None;
                }
                let payload = buf.copy_to_bytes(payload_len);
                Some(PlumtreeMessage::Gossip { id, round, payload })
            }
            MessageTag::IHave => {
                if buf.remaining() < 6 {
                    return None;
                }
                let round = buf.get_u32();
                let count = buf.get_u16() as usize;
                if count > MAX_IHAVE_BATCH {
                    return None;
                }
                let mut message_ids = SmallVec::with_capacity(count);
                for _ in 0..count {
                    message_ids.push(MessageId::decode(buf)?);
                }
                Some(PlumtreeMessage::IHave { message_ids, round })
            }
            MessageTag::Graft => {
                let message_id = MessageId::decode(buf)?;
                if buf.remaining() < 4 {
                    return None;
                }
                let round = buf.get_u32();
                Some(PlumtreeMessage::Graft { message_id, round })
            }
            MessageTag::Prune => Some(PlumtreeMessage::Prune),
            MessageTag::SyncRequest
            | MessageTag::SyncResponse
            | MessageTag::SyncPull
            | MessageTag::SyncPush => {
                // Re-create the tag byte for SyncMessage decode
                let sync_tag = match tag {
                    MessageTag::SyncRequest => 5u8,
                    MessageTag::SyncResponse => 6u8,
                    MessageTag::SyncPull => 7u8,
                    MessageTag::SyncPush => 8u8,
                    _ => unreachable!(),
                };
                SyncMessage::decode_with_tag(sync_tag, buf).map(PlumtreeMessage::Sync)
            }
        }
    }

    /// Decode a message from a byte slice.
    pub fn decode_from_slice(data: &[u8]) -> Option<Self> {
        let mut cursor = std::io::Cursor::new(data);
        Self::decode(&mut cursor)
    }

    /// Check if this is a Gossip message.
    pub const fn is_gossip(&self) -> bool {
        matches!(self, PlumtreeMessage::Gossip { .. })
    }

    /// Check if this is an IHave message.
    pub const fn is_ihave(&self) -> bool {
        matches!(self, PlumtreeMessage::IHave { .. })
    }

    /// Check if this is a Graft message.
    pub const fn is_graft(&self) -> bool {
        matches!(self, PlumtreeMessage::Graft { .. })
    }

    /// Check if this is a Prune message.
    pub const fn is_prune(&self) -> bool {
        matches!(self, PlumtreeMessage::Prune)
    }

    /// Check if this is a Sync message.
    pub const fn is_sync(&self) -> bool {
        matches!(self, PlumtreeMessage::Sync(_))
    }

    /// Get the message tag.
    pub const fn tag(&self) -> MessageTag {
        match self {
            PlumtreeMessage::Gossip { .. } => MessageTag::Gossip,
            PlumtreeMessage::IHave { .. } => MessageTag::IHave,
            PlumtreeMessage::Graft { .. } => MessageTag::Graft,
            PlumtreeMessage::Prune => MessageTag::Prune,
            PlumtreeMessage::Sync(sync) => sync.tag(),
        }
    }

    /// Get a human-readable type name for tracing/logging.
    pub const fn type_name(&self) -> &'static str {
        match self {
            PlumtreeMessage::Gossip { .. } => "Gossip",
            PlumtreeMessage::IHave { .. } => "IHave",
            PlumtreeMessage::Graft { .. } => "Graft",
            PlumtreeMessage::Prune => "Prune",
            PlumtreeMessage::Sync(sync) => sync.type_name(),
        }
    }
}

/// Maximum number of message IDs in a sync batch.
pub const MAX_SYNC_BATCH: usize = 100;

/// Maximum number of messages in a sync push.
pub const MAX_SYNC_PUSH_MESSAGES: usize = 50;

impl SyncMessage {
    /// Encode the sync message into bytes.
    pub fn encode(&self, buf: &mut impl BufMut) {
        match self {
            SyncMessage::Request {
                root_hash,
                time_start,
                time_end,
            } => {
                buf.put_u8(MessageTag::SyncRequest as u8);
                buf.put_slice(root_hash);
                buf.put_u64(*time_start);
                buf.put_u64(*time_end);
            }
            SyncMessage::Response {
                matches,
                message_ids,
                has_more,
            } => {
                buf.put_u8(MessageTag::SyncResponse as u8);
                buf.put_u8(if *matches { 1 } else { 0 });
                buf.put_u8(if *has_more { 1 } else { 0 });
                buf.put_u16(message_ids.len() as u16);
                for id in message_ids {
                    id.encode(buf);
                }
            }
            SyncMessage::Pull { message_ids } => {
                buf.put_u8(MessageTag::SyncPull as u8);
                buf.put_u16(message_ids.len() as u16);
                for id in message_ids {
                    id.encode(buf);
                }
            }
            SyncMessage::Push { messages } => {
                buf.put_u8(MessageTag::SyncPush as u8);
                buf.put_u16(messages.len() as u16);
                for (id, round, payload) in messages {
                    id.encode(buf);
                    buf.put_u32(*round);
                    buf.put_u32(payload.len() as u32);
                    buf.put_slice(payload);
                }
            }
        }
    }

    /// Encode the sync message into a new Bytes buffer.
    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_len());
        self.encode(&mut buf);
        buf.freeze()
    }

    /// Calculate the encoded length of the sync message.
    pub fn encoded_len(&self) -> usize {
        match self {
            SyncMessage::Request { .. } => 1 + 32 + 8 + 8, // tag + hash + start + end
            SyncMessage::Response { message_ids, .. } => {
                1 + 1 + 1 + 2 + (message_ids.len() * MessageId::ENCODED_SIZE)
            }
            SyncMessage::Pull { message_ids } => {
                1 + 2 + (message_ids.len() * MessageId::ENCODED_SIZE)
            }
            SyncMessage::Push { messages } => {
                let mut len = 1 + 2; // tag + count
                for (_, _, payload) in messages {
                    len += MessageId::ENCODED_SIZE + 4 + 4 + payload.len();
                }
                len
            }
        }
    }

    /// Decode a sync message from bytes (tag already consumed by PlumtreeMessage::decode).
    pub fn decode_with_tag(tag: u8, buf: &mut impl Buf) -> Option<Self> {
        match tag {
            5 => {
                // SyncRequest
                if buf.remaining() < 32 + 8 + 8 {
                    return None;
                }
                let mut root_hash = [0u8; 32];
                buf.copy_to_slice(&mut root_hash);
                let time_start = buf.get_u64();
                let time_end = buf.get_u64();
                Some(SyncMessage::Request {
                    root_hash,
                    time_start,
                    time_end,
                })
            }
            6 => {
                // SyncResponse
                if buf.remaining() < 4 {
                    return None;
                }
                let matches = buf.get_u8() != 0;
                let has_more = buf.get_u8() != 0;
                let count = buf.get_u16() as usize;
                if count > MAX_SYNC_BATCH {
                    return None;
                }
                let mut message_ids = SmallVec::with_capacity(count);
                for _ in 0..count {
                    message_ids.push(MessageId::decode(buf)?);
                }
                Some(SyncMessage::Response {
                    matches,
                    message_ids,
                    has_more,
                })
            }
            7 => {
                // SyncPull
                if buf.remaining() < 2 {
                    return None;
                }
                let count = buf.get_u16() as usize;
                if count > MAX_SYNC_BATCH {
                    return None;
                }
                let mut message_ids = SmallVec::with_capacity(count);
                for _ in 0..count {
                    message_ids.push(MessageId::decode(buf)?);
                }
                Some(SyncMessage::Pull { message_ids })
            }
            8 => {
                // SyncPush
                if buf.remaining() < 2 {
                    return None;
                }
                let count = buf.get_u16() as usize;
                if count > MAX_SYNC_PUSH_MESSAGES {
                    return None;
                }
                let mut messages = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = MessageId::decode(buf)?;
                    if buf.remaining() < 8 {
                        return None;
                    }
                    let round = buf.get_u32();
                    let payload_len = buf.get_u32() as usize;
                    if buf.remaining() < payload_len {
                        return None;
                    }
                    let payload = buf.copy_to_bytes(payload_len);
                    messages.push((id, round, payload));
                }
                Some(SyncMessage::Push { messages })
            }
            _ => None,
        }
    }

    /// Decode a sync message from bytes (including tag).
    pub fn decode(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 1 {
            return None;
        }
        let tag = buf.get_u8();
        Self::decode_with_tag(tag, buf)
    }

    /// Decode a sync message from a byte slice.
    pub fn decode_from_slice(data: &[u8]) -> Option<Self> {
        let mut cursor = std::io::Cursor::new(data);
        Self::decode(&mut cursor)
    }

    /// Get the message tag.
    pub const fn tag(&self) -> MessageTag {
        match self {
            SyncMessage::Request { .. } => MessageTag::SyncRequest,
            SyncMessage::Response { .. } => MessageTag::SyncResponse,
            SyncMessage::Pull { .. } => MessageTag::SyncPull,
            SyncMessage::Push { .. } => MessageTag::SyncPush,
        }
    }

    /// Get a human-readable type name.
    pub const fn type_name(&self) -> &'static str {
        match self {
            SyncMessage::Request { .. } => "SyncRequest",
            SyncMessage::Response { .. } => "SyncResponse",
            SyncMessage::Pull { .. } => "SyncPull",
            SyncMessage::Push { .. } => "SyncPush",
        }
    }

    /// Check if this is a sync request.
    pub const fn is_request(&self) -> bool {
        matches!(self, SyncMessage::Request { .. })
    }

    /// Check if this is a sync response.
    pub const fn is_response(&self) -> bool {
        matches!(self, SyncMessage::Response { .. })
    }

    /// Check if this is a sync pull.
    pub const fn is_pull(&self) -> bool {
        matches!(self, SyncMessage::Pull { .. })
    }

    /// Check if this is a sync push.
    pub const fn is_push(&self) -> bool {
        matches!(self, SyncMessage::Push { .. })
    }
}

/// Reference to a Plumtree message (for zero-copy operations).
#[derive(Debug, Clone)]
pub enum PlumtreeMessageRef<'a> {
    /// Reference to a Gossip message.
    Gossip {
        /// Unique message identifier.
        id: MessageId,
        /// Number of hops from the original sender.
        round: u32,
        /// Reference to message payload.
        payload: &'a [u8],
    },
    /// Reference to an IHave message.
    IHave {
        /// List of message IDs.
        message_ids: &'a [MessageId],
        /// Current round number.
        round: u32,
    },
    /// Reference to a Graft message.
    Graft {
        /// ID of the message being requested.
        message_id: MessageId,
        /// Round number.
        round: u32,
    },
    /// Prune message (no data).
    Prune,
}

impl<'a> PlumtreeMessageRef<'a> {
    /// Convert to an owned message.
    pub fn to_owned(&self) -> PlumtreeMessage {
        match self {
            PlumtreeMessageRef::Gossip { id, round, payload } => PlumtreeMessage::Gossip {
                id: *id,
                round: *round,
                payload: Bytes::copy_from_slice(payload),
            },
            PlumtreeMessageRef::IHave { message_ids, round } => PlumtreeMessage::IHave {
                message_ids: SmallVec::from_slice(message_ids),
                round: *round,
            },
            PlumtreeMessageRef::Graft { message_id, round } => PlumtreeMessage::Graft {
                message_id: *message_id,
                round: *round,
            },
            PlumtreeMessageRef::Prune => PlumtreeMessage::Prune,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gossip_encoding() {
        let msg = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 5,
            payload: Bytes::from_static(b"hello world"),
        };

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_ihave_encoding() {
        let msg = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new(), MessageId::new(), MessageId::new()],
            round: 10,
        };

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_graft_encoding() {
        let msg = PlumtreeMessage::Graft {
            message_id: MessageId::new(),
            round: 3,
        };

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_prune_encoding() {
        let msg = PlumtreeMessage::Prune;

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_encoded_len() {
        let gossip = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 0,
            payload: Bytes::from_static(b"test"),
        };
        assert_eq!(gossip.encoded_len(), gossip.encode_to_bytes().len());

        let ihave = PlumtreeMessage::IHave {
            message_ids: smallvec::smallvec![MessageId::new()],
            round: 0,
        };
        assert_eq!(ihave.encoded_len(), ihave.encode_to_bytes().len());

        let graft = PlumtreeMessage::Graft {
            message_id: MessageId::new(),
            round: 0,
        };
        assert_eq!(graft.encoded_len(), graft.encode_to_bytes().len());

        let prune = PlumtreeMessage::Prune;
        assert_eq!(prune.encoded_len(), prune.encode_to_bytes().len());
    }

    #[test]
    fn test_sync_request_encoding() {
        let msg = PlumtreeMessage::Sync(SyncMessage::Request {
            root_hash: [42u8; 32],
            time_start: 1000,
            time_end: 2000,
        });

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
        assert_eq!(msg.encoded_len(), encoded.len());
    }

    #[test]
    fn test_sync_response_encoding() {
        let msg = PlumtreeMessage::Sync(SyncMessage::Response {
            matches: false,
            message_ids: smallvec::smallvec![MessageId::new(), MessageId::new()],
            has_more: true,
        });

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
        assert_eq!(msg.encoded_len(), encoded.len());
    }

    #[test]
    fn test_sync_response_match_encoding() {
        let msg = PlumtreeMessage::Sync(SyncMessage::Response {
            matches: true,
            message_ids: SmallVec::new(),
            has_more: false,
        });

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_sync_pull_encoding() {
        let msg = PlumtreeMessage::Sync(SyncMessage::Pull {
            message_ids: smallvec::smallvec![MessageId::new(), MessageId::new(), MessageId::new()],
        });

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
        assert_eq!(msg.encoded_len(), encoded.len());
    }

    #[test]
    fn test_sync_push_encoding() {
        let msg = PlumtreeMessage::Sync(SyncMessage::Push {
            messages: vec![
                (MessageId::new(), 1, Bytes::from_static(b"msg1")),
                (MessageId::new(), 2, Bytes::from_static(b"msg2")),
            ],
        });

        let encoded = msg.encode_to_bytes();
        let decoded = PlumtreeMessage::decode_from_slice(&encoded).unwrap();

        assert_eq!(msg, decoded);
        assert_eq!(msg.encoded_len(), encoded.len());
    }

    #[test]
    fn test_sync_message_types() {
        let request = SyncMessage::Request {
            root_hash: [0u8; 32],
            time_start: 0,
            time_end: 0,
        };
        assert!(request.is_request());
        assert_eq!(request.tag(), MessageTag::SyncRequest);
        assert_eq!(request.type_name(), "SyncRequest");

        let response = SyncMessage::Response {
            matches: true,
            message_ids: SmallVec::new(),
            has_more: false,
        };
        assert!(response.is_response());
        assert_eq!(response.tag(), MessageTag::SyncResponse);

        let pull = SyncMessage::Pull {
            message_ids: SmallVec::new(),
        };
        assert!(pull.is_pull());
        assert_eq!(pull.tag(), MessageTag::SyncPull);

        let push = SyncMessage::Push { messages: vec![] };
        assert!(push.is_push());
        assert_eq!(push.tag(), MessageTag::SyncPush);
    }

    #[test]
    fn test_plumtree_message_is_sync() {
        let sync_msg = PlumtreeMessage::Sync(SyncMessage::Request {
            root_hash: [0u8; 32],
            time_start: 0,
            time_end: 0,
        });
        assert!(sync_msg.is_sync());

        let gossip = PlumtreeMessage::Gossip {
            id: MessageId::new(),
            round: 0,
            payload: Bytes::new(),
        };
        assert!(!gossip.is_sync());
    }
}
