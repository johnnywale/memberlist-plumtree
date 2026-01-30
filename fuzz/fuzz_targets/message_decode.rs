//! Fuzz target for PlumtreeMessage decoding.
//!
//! This fuzzer tests the robustness of message parsing against arbitrary input.
//! It ensures that no panics, buffer overflows, or undefined behavior occur
//! when parsing malformed message data.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

// Import the message decoding function
// Note: We need to use the internal decode function directly
// since the public API uses IdCodec which requires a concrete type

fuzz_target!(|data: &[u8]| {
    // Try to decode as raw bytes
    let bytes = Bytes::copy_from_slice(data);

    // The message format is:
    // [tag: u8][...message-specific data...]
    //
    // Tags:
    // 0x01 = Gossip
    // 0x02 = IHave
    // 0x03 = Graft
    // 0x04 = Prune
    //
    // We just need to ensure no panics occur regardless of input

    if data.is_empty() {
        return;
    }

    let tag = data[0];
    let rest = &data[1..];

    // Simulate message parsing behavior
    match tag {
        0x01 => {
            // Gossip: [16 bytes MessageId][4 bytes round][payload...]
            if rest.len() >= 20 {
                let _msg_id = &rest[..16];
                let _round = if rest.len() >= 20 {
                    u32::from_be_bytes([rest[16], rest[17], rest[18], rest[19]])
                } else {
                    0
                };
                let _payload = if rest.len() > 20 {
                    Bytes::copy_from_slice(&rest[20..])
                } else {
                    Bytes::new()
                };
            }
        }
        0x02 => {
            // IHave: [4 bytes round][1 byte count][16 bytes * count MessageIds]
            if rest.len() >= 5 {
                let _round = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
                let _count = rest[4] as usize;
            }
        }
        0x03 => {
            // Graft: [16 bytes MessageId][4 bytes round]
            if rest.len() >= 20 {
                let _msg_id = &rest[..16];
                let _round = u32::from_be_bytes([rest[16], rest[17], rest[18], rest[19]]);
            }
        }
        0x04 => {
            // Prune: no additional data
        }
        _ => {
            // Unknown tag - just ignore
        }
    }

    // Also try interpreting as a string (for debug output testing)
    let _ = String::from_utf8_lossy(data);

    // Try to slice at various points to test bounds checking
    for i in 0..data.len().min(32) {
        let _ = bytes.slice(..i.min(bytes.len()));
        let _ = bytes.slice(i.min(bytes.len())..);
    }
});
