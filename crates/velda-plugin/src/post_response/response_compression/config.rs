//! Declarative Configuration Schema for Response Compression.
//!
//! Enforces RFC 9110 compliant parameter bounds for minimum payload lengths,
//! codec levels, and dictionary bindings.

use super::dictionary::ZstdDictionaryConfig;
use super::negotiation::CompressionAlgorithm;

/// Configuration schema for [`super::plugin::ResponseCompressionPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseCompressionConfig {
    /// Enabled algorithms in server priority order.
    pub algorithms: Vec<CompressionAlgorithm>,
    /// Minimum payload size in bytes required to trigger compression.
    /// Tiny payloads (< 256 bytes) expand in size due to framing and Huffman overhead.
    pub min_length: usize,
    /// Gzip/Deflate compression level (1-9, default: 1 for Pareto speed/ratio sweet spot).
    pub gzip_level: u32,
    /// Brotli compression quality level (0-11, default: 2 for sub-millisecond edge latency).
    pub brotli_level: u32,
    /// Zstandard compression level (1-22, default: 1 for ultra-fast line-rate throughput).
    pub zstd_level: i32,
    /// Pre-trained Zstandard dictionary (built-in or custom Base64-decoded) for high-ratio structured JSON compression.
    pub zstd_dict: ZstdDictionaryConfig,
    /// MIME type prefixes eligible for compression.
    pub content_types: Vec<String>,
}

impl Default for ResponseCompressionConfig {
    fn default() -> Self {
        Self {
            algorithms: Vec::new(),
            min_length: 256,
            gzip_level: 1,
            brotli_level: 2,
            zstd_level: 1,
            zstd_dict: ZstdDictionaryConfig::None,
            content_types: Vec::new(),
        }
    }
}

impl ResponseCompressionConfig {
    /// Helper to borrow dictionary bytes, if configured.
    #[inline]
    pub fn zstd_dict_bytes(&self) -> Option<&[u8]> {
        self.zstd_dict.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = ResponseCompressionConfig::default();
        assert_eq!(cfg.min_length, 256);
        assert_eq!(cfg.gzip_level, 1);
        assert_eq!(cfg.brotli_level, 2);
        assert_eq!(cfg.zstd_level, 1);
        assert_eq!(cfg.zstd_dict, ZstdDictionaryConfig::None);
        assert!(cfg.algorithms.is_empty());
        assert!(cfg.content_types.is_empty());
    }
}
