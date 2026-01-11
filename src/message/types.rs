//! Plumtree protocol message types.

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
}

impl TryFrom<u8> for MessageTag {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(MessageTag::Gossip),
            2 => Ok(MessageTag::IHave),
            3 => Ok(MessageTag::Graft),
            4 => Ok(MessageTag::Prune),
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

    /// Get the message tag.
    pub const fn tag(&self) -> MessageTag {
        match self {
            PlumtreeMessage::Gossip { .. } => MessageTag::Gossip,
            PlumtreeMessage::IHave { .. } => MessageTag::IHave,
            PlumtreeMessage::Graft { .. } => MessageTag::Graft,
            PlumtreeMessage::Prune => MessageTag::Prune,
        }
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
}
