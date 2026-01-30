//! Message compression support for Plumtree.
//!
//! This module provides optional compression for message payloads to reduce
//! bandwidth usage. Compression is only applied when the payload exceeds
//! a configurable threshold.
//!
//! # Supported Algorithms
//!
//! - **Gzip**: Good compression ratio, widely supported
//! - **Zstd**: Better compression ratio and speed, recommended for modern deployments
//!
//! # Wire Format
//!
//! When compression is enabled, the wire format includes a flags byte:
//! ```text
//! [MAGIC:2][flags:1][sender_id:N][message:...]
//! flags: bit 0 = compressed, bits 1-2 = algorithm (0=gzip, 1=zstd)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use memberlist_plumtree::{CompressionConfig, CompressionAlgorithm, PlumtreeConfig};
//!
//! let config = PlumtreeConfig::default()
//!     .with_compression(CompressionConfig {
//!         algorithm: CompressionAlgorithm::Zstd { level: 3 },
//!         min_payload_size: 256,
//!         enabled: true,
//!     });
//! ```

use bytes::Bytes;

/// Compression algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum CompressionAlgorithm {
    /// No compression
    #[default]
    None,
    /// Gzip compression with specified level (1-9)
    Gzip {
        /// Compression level (1-9, higher = better compression, slower)
        level: u32,
    },
    /// Zstd compression with specified level (1-22, default 3)
    Zstd {
        /// Compression level (1-22, higher = better compression, slower)
        level: i32,
    },
}

impl CompressionAlgorithm {
    /// Get the algorithm ID for wire format (2 bits)
    pub fn wire_id(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip { .. } => 1,
            CompressionAlgorithm::Zstd { .. } => 2,
        }
    }

    /// Create algorithm from wire ID
    pub fn from_wire_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(CompressionAlgorithm::None),
            1 => Some(CompressionAlgorithm::Gzip { level: 6 }), // Default level for decompression
            2 => Some(CompressionAlgorithm::Zstd { level: 3 }), // Default level for decompression
            _ => None,
        }
    }
}

/// Configuration for message compression.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CompressionConfig {
    /// Whether compression is enabled
    pub enabled: bool,
    /// Compression algorithm to use
    pub algorithm: CompressionAlgorithm,
    /// Minimum payload size to trigger compression (bytes)
    /// Payloads smaller than this are sent uncompressed
    pub min_payload_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: CompressionAlgorithm::None,
            min_payload_size: 256,
        }
    }
}

impl CompressionConfig {
    /// Create a new compression config with Zstd (recommended)
    pub fn zstd(level: i32) -> Self {
        Self {
            enabled: true,
            algorithm: CompressionAlgorithm::Zstd { level },
            min_payload_size: 256,
        }
    }

    /// Create a new compression config with Gzip
    pub fn gzip(level: u32) -> Self {
        Self {
            enabled: true,
            algorithm: CompressionAlgorithm::Gzip { level },
            min_payload_size: 256,
        }
    }

    /// Set the minimum payload size for compression
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_payload_size = size;
        self
    }

    /// Check if compression should be applied to a payload of given size
    pub fn should_compress(&self, payload_size: usize) -> bool {
        self.enabled
            && !matches!(self.algorithm, CompressionAlgorithm::None)
            && payload_size >= self.min_payload_size
    }
}

/// Compression flags byte layout for wire format.
///
/// This module provides constants and functions for encoding/decoding
/// compression metadata in the wire protocol.
#[allow(dead_code)]
pub mod flags {
    /// Bit 0: compression enabled
    pub const COMPRESSED: u8 = 0b0000_0001;
    /// Bits 1-2: algorithm (0=none, 1=gzip, 2=zstd)
    pub const ALGORITHM_MASK: u8 = 0b0000_0110;
    /// Bit shift for algorithm field
    pub const ALGORITHM_SHIFT: u8 = 1;

    /// Create flags byte from compression state and algorithm ID.
    ///
    /// # Arguments
    /// * `compressed` - Whether the payload is compressed
    /// * `algorithm_id` - Algorithm identifier (0=none, 1=gzip, 2=zstd)
    ///
    /// # Returns
    /// Encoded flags byte for wire format
    #[inline]
    pub fn encode(compressed: bool, algorithm_id: u8) -> u8 {
        let mut flags = 0u8;
        if compressed {
            flags |= COMPRESSED;
            flags |= (algorithm_id << ALGORITHM_SHIFT) & ALGORITHM_MASK;
        }
        flags
    }

    /// Decode flags byte into compression state and algorithm ID.
    ///
    /// # Arguments
    /// * `flags` - Encoded flags byte from wire format
    ///
    /// # Returns
    /// Tuple of (compressed, algorithm_id)
    #[inline]
    pub fn decode(flags: u8) -> (bool, u8) {
        let compressed = (flags & COMPRESSED) != 0;
        let algorithm_id = (flags & ALGORITHM_MASK) >> ALGORITHM_SHIFT;
        (compressed, algorithm_id)
    }
}

/// Compress data using the specified algorithm.
///
/// Returns the compressed data, or the original data if compression
/// would make it larger.
#[cfg(feature = "compression")]
pub fn compress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Bytes, CompressionError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(Bytes::copy_from_slice(data)),
        CompressionAlgorithm::Gzip { level } => compress_gzip(data, level),
        CompressionAlgorithm::Zstd { level } => compress_zstd(data, level),
    }
}

/// Decompress data using the specified algorithm.
#[cfg(feature = "compression")]
pub fn decompress(data: &[u8], algorithm: CompressionAlgorithm) -> Result<Bytes, CompressionError> {
    match algorithm {
        CompressionAlgorithm::None => Ok(Bytes::copy_from_slice(data)),
        CompressionAlgorithm::Gzip { .. } => decompress_gzip(data),
        CompressionAlgorithm::Zstd { .. } => decompress_zstd(data),
    }
}

#[cfg(feature = "compression")]
fn compress_gzip(data: &[u8], level: u32) -> Result<Bytes, CompressionError> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
    encoder
        .write_all(data)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;
    let compressed = encoder
        .finish()
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;

    // Only use compressed if it's actually smaller
    if compressed.len() < data.len() {
        Ok(Bytes::from(compressed))
    } else {
        Ok(Bytes::copy_from_slice(data))
    }
}

#[cfg(feature = "compression")]
fn decompress_gzip(data: &[u8]) -> Result<Bytes, CompressionError> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| CompressionError::DecompressFailed(e.to_string()))?;
    Ok(Bytes::from(decompressed))
}

#[cfg(feature = "compression")]
fn compress_zstd(data: &[u8], level: i32) -> Result<Bytes, CompressionError> {
    let compressed = zstd::encode_all(data, level)
        .map_err(|e| CompressionError::CompressFailed(e.to_string()))?;

    // Only use compressed if it's actually smaller
    if compressed.len() < data.len() {
        Ok(Bytes::from(compressed))
    } else {
        Ok(Bytes::copy_from_slice(data))
    }
}

#[cfg(feature = "compression")]
fn decompress_zstd(data: &[u8]) -> Result<Bytes, CompressionError> {
    let decompressed =
        zstd::decode_all(data).map_err(|e| CompressionError::DecompressFailed(e.to_string()))?;
    Ok(Bytes::from(decompressed))
}

// Stub implementations when compression feature is disabled
// These provide API compatibility - data passes through unchanged
#[cfg(not(feature = "compression"))]
#[allow(dead_code)]
pub fn compress(data: &[u8], _algorithm: CompressionAlgorithm) -> Result<Bytes, CompressionError> {
    Ok(Bytes::copy_from_slice(data))
}

#[cfg(not(feature = "compression"))]
#[allow(dead_code)]
pub fn decompress(
    data: &[u8],
    _algorithm: CompressionAlgorithm,
) -> Result<Bytes, CompressionError> {
    Ok(Bytes::copy_from_slice(data))
}

/// Compression error types.
#[derive(Debug, Clone)]
pub enum CompressionError {
    /// Compression failed
    CompressFailed(String),
    /// Decompression failed
    DecompressFailed(String),
    /// Unknown algorithm
    UnknownAlgorithm(u8),
}

impl std::fmt::Display for CompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionError::CompressFailed(e) => write!(f, "compression failed: {}", e),
            CompressionError::DecompressFailed(e) => write!(f, "decompression failed: {}", e),
            CompressionError::UnknownAlgorithm(id) => {
                write!(f, "unknown compression algorithm: {}", id)
            }
        }
    }
}

impl std::error::Error for CompressionError {}

/// Compression statistics for monitoring.
#[derive(Debug, Default, Clone)]
pub struct CompressionStats {
    /// Total bytes before compression
    pub bytes_in: u64,
    /// Total bytes after compression
    pub bytes_out: u64,
    /// Number of messages compressed
    pub messages_compressed: u64,
    /// Number of messages skipped (too small or no benefit)
    pub messages_skipped: u64,
}

impl CompressionStats {
    /// Calculate compression ratio (1.0 = no compression, 0.5 = 50% reduction)
    pub fn ratio(&self) -> f64 {
        if self.bytes_in == 0 {
            1.0
        } else {
            self.bytes_out as f64 / self.bytes_in as f64
        }
    }

    /// Calculate bytes saved
    pub fn bytes_saved(&self) -> u64 {
        self.bytes_in.saturating_sub(self.bytes_out)
    }

    /// Record a compression operation
    pub fn record(&mut self, original_size: usize, compressed_size: usize) {
        self.bytes_in += original_size as u64;
        self.bytes_out += compressed_size as u64;
        if compressed_size < original_size {
            self.messages_compressed += 1;
        } else {
            self.messages_skipped += 1;
        }
    }

    /// Record a skipped compression (below threshold)
    pub fn record_skipped(&mut self, size: usize) {
        self.bytes_in += size as u64;
        self.bytes_out += size as u64;
        self.messages_skipped += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_payload_size, 256);
    }

    #[test]
    fn test_compression_config_zstd() {
        let config = CompressionConfig::zstd(3);
        assert!(config.enabled);
        assert!(matches!(
            config.algorithm,
            CompressionAlgorithm::Zstd { level: 3 }
        ));
    }

    #[test]
    fn test_compression_config_gzip() {
        let config = CompressionConfig::gzip(6);
        assert!(config.enabled);
        assert!(matches!(
            config.algorithm,
            CompressionAlgorithm::Gzip { level: 6 }
        ));
    }

    #[test]
    fn test_should_compress() {
        let config = CompressionConfig::zstd(3).with_min_size(100);

        assert!(!config.should_compress(50)); // Too small
        assert!(config.should_compress(100)); // Exactly threshold
        assert!(config.should_compress(1000)); // Large enough

        let disabled = CompressionConfig::default();
        assert!(!disabled.should_compress(1000)); // Disabled
    }

    #[test]
    fn test_flags_encode_decode() {
        // No compression
        let flags = flags::encode(false, 0);
        let (compressed, _algo) = flags::decode(flags);
        assert!(!compressed);

        // Gzip
        let flags = flags::encode(true, 1);
        let (compressed, algo) = flags::decode(flags);
        assert!(compressed);
        assert_eq!(algo, 1);

        // Zstd
        let flags = flags::encode(true, 2);
        let (compressed, algo) = flags::decode(flags);
        assert!(compressed);
        assert_eq!(algo, 2);
    }

    #[test]
    fn test_algorithm_wire_id() {
        assert_eq!(CompressionAlgorithm::None.wire_id(), 0);
        assert_eq!(CompressionAlgorithm::Gzip { level: 6 }.wire_id(), 1);
        assert_eq!(CompressionAlgorithm::Zstd { level: 3 }.wire_id(), 2);
    }

    #[test]
    fn test_algorithm_from_wire_id() {
        assert!(matches!(
            CompressionAlgorithm::from_wire_id(0),
            Some(CompressionAlgorithm::None)
        ));
        assert!(matches!(
            CompressionAlgorithm::from_wire_id(1),
            Some(CompressionAlgorithm::Gzip { .. })
        ));
        assert!(matches!(
            CompressionAlgorithm::from_wire_id(2),
            Some(CompressionAlgorithm::Zstd { .. })
        ));
        assert!(CompressionAlgorithm::from_wire_id(3).is_none());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_gzip_roundtrip() {
        // Use a larger, repetitive payload that compresses well
        let original: Vec<u8> = (0..1000)
            .flat_map(|_| b"Hello, World! ".iter().copied())
            .collect();
        let compressed = compress(&original, CompressionAlgorithm::Gzip { level: 6 }).unwrap();

        // Should actually compress (smaller than original)
        assert!(compressed.len() < original.len(), "Data should compress");

        let decompressed =
            decompress(&compressed, CompressionAlgorithm::Gzip { level: 6 }).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_ref());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_zstd_roundtrip() {
        // Use a larger, repetitive payload that compresses well
        let original: Vec<u8> = (0..1000)
            .flat_map(|_| b"Hello, World! ".iter().copied())
            .collect();
        let compressed = compress(&original, CompressionAlgorithm::Zstd { level: 3 }).unwrap();

        // Should actually compress (smaller than original)
        assert!(compressed.len() < original.len(), "Data should compress");

        let decompressed =
            decompress(&compressed, CompressionAlgorithm::Zstd { level: 3 }).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_ref());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn test_compression_benefit() {
        // Create a large repetitive payload that compresses well
        let original: Vec<u8> = (0..1000).map(|i| (i % 10) as u8).collect();

        let compressed = compress(&original, CompressionAlgorithm::Zstd { level: 3 }).unwrap();

        // Should be significantly smaller
        assert!(
            compressed.len() < original.len(),
            "Compressed {} -> {} bytes",
            original.len(),
            compressed.len()
        );
    }

    #[test]
    fn test_compression_stats() {
        let mut stats = CompressionStats::default();

        stats.record(1000, 500); // 50% compression
        assert_eq!(stats.bytes_in, 1000);
        assert_eq!(stats.bytes_out, 500);
        assert_eq!(stats.messages_compressed, 1);
        assert_eq!(stats.bytes_saved(), 500);
        assert!((stats.ratio() - 0.5).abs() < 0.001);

        stats.record(100, 100); // No benefit
        assert_eq!(stats.messages_skipped, 1);
    }
}
