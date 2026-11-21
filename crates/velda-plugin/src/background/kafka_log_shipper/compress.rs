//! Kafka Log Shipper Compression Engine (`kafka_log_shipper::compress`).
//!
//! Provides orthogonal payload compression for Kafka RecordBatch v2:
//! - [`CompressionCodec::None`]: Uncompressed raw bytes passthrough.
//! - [`CompressionCodec::Gzip`]: RFC 1952 deflate compression with fast level via `flate2`.
//! - [`CompressionCodec::Zstd`]: Zstandard streaming compression via `zstd::bulk`.

use std::io::Write;

use super::config::CompressionCodec;

/// Compresses a raw records payload according to the designated [`CompressionCodec`].
///
/// If compression fails for any reason, falls back gracefully to uncompressed bytes.
pub fn compress_records(raw: &[u8], codec: CompressionCodec) -> Vec<u8> {
    match codec {
        CompressionCodec::None => raw.to_vec(),
        CompressionCodec::Gzip => {
            let mut encoder = flate2::write::GzEncoder::new(
                Vec::with_capacity(raw.len() / 2),
                flate2::Compression::fast(),
            );
            if encoder.write_all(raw).is_ok() {
                encoder.finish().unwrap_or_else(|_| raw.to_vec())
            } else {
                raw.to_vec()
            }
        }
        CompressionCodec::Zstd => zstd::bulk::compress(raw, 1).unwrap_or_else(|_| raw.to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_records_all_codecs() {
        let sample = b"hello velda kafka compression test data 1234567890".repeat(20);

        let uncompressed = compress_records(&sample, CompressionCodec::None);
        assert_eq!(uncompressed, sample);

        let gzip_compressed = compress_records(&sample, CompressionCodec::Gzip);
        assert!(gzip_compressed.len() < sample.len());
        // RFC 1952 magic bytes 0x1f 0x8b
        assert_eq!(gzip_compressed[0], 0x1f);
        assert_eq!(gzip_compressed[1], 0x8b);

        let zstd_compressed = compress_records(&sample, CompressionCodec::Zstd);
        assert!(zstd_compressed.len() < sample.len());
        // Zstandard magic number 0xFD2FB528 (little-endian: 0x28, 0xb5, 0x2f, 0xfd)
        assert_eq!(&zstd_compressed[0..4], &[0x28, 0xb5, 0x2f, 0xfd]);
    }
}
