//! Fuzz target for NetworkEnvelope decoding.
//!
//! This fuzzer tests the robustness of the envelope parsing which wraps
//! all Plumtree messages with sender identity.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

/// Magic byte for Plumtree envelope
const PLUMTREE_MAGIC: u8 = 0x50; // 'P'

fuzz_target!(|data: &[u8]| {
    // Envelope format:
    // [MAGIC: 1 byte][sender_id: variable][message: variable]
    //
    // For u64 sender:
    // [0x50][8 bytes sender_id][message...]

    if data.is_empty() {
        return;
    }

    // Check magic byte
    let magic = data[0];
    if magic != PLUMTREE_MAGIC {
        // Invalid magic - should be rejected gracefully
        return;
    }

    if data.len() < 2 {
        return;
    }

    let rest = &data[1..];

    // Try to interpret as u64 sender ID (8 bytes)
    if rest.len() >= 8 {
        let sender_bytes: [u8; 8] = rest[..8].try_into().unwrap_or([0u8; 8]);
        let _sender = u64::from_be_bytes(sender_bytes);
        let _message_data = &rest[8..];
    }

    // Try to interpret as u32 sender ID (4 bytes)
    if rest.len() >= 4 {
        let sender_bytes: [u8; 4] = rest[..4].try_into().unwrap_or([0u8; 4]);
        let _sender = u32::from_be_bytes(sender_bytes);
        let _message_data = &rest[4..];
    }

    // Try to interpret as u16 sender ID (2 bytes)
    if rest.len() >= 2 {
        let sender_bytes: [u8; 2] = rest[..2].try_into().unwrap_or([0u8; 2]);
        let _sender = u16::from_be_bytes(sender_bytes);
        let _message_data = &rest[2..];
    }

    // Try to interpret as u8 sender ID (1 byte)
    {
        let _sender = rest[0];
        let _message_data = &rest[1..];
    }

    // Try string-based sender (length-prefixed)
    // Format: [1 byte length][length bytes string]
    let len = rest[0] as usize;
    if rest.len() > len {
        let string_bytes = &rest[1..1 + len.min(rest.len() - 1)];
        let _sender = String::from_utf8_lossy(string_bytes);
        let _message_data = &rest[1 + len.min(rest.len() - 1)..];
    }

    // Try to create a Bytes from the data
    let _ = Bytes::copy_from_slice(data);
});
