//! Wire Encoding Engines & Payload Compression Dispatchers.
//!
//! Provides thread-local reusable compressor contexts, scratch buffers,
//! and wire encoding for Gzip, Deflate, Brotli, and Zstandard (with optional dictionary).

use bytes::Bytes;
use std::cell::RefCell;
use std::io::Write;

use super::negotiation::CompressionAlgorithm;
use super::simd::streaming_to_bytes;

thread_local! {
    /// Reusable thread-local scratch buffer for in-place compression engines (pre-warmed to 64KB).
    static SCRATCH_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 * 1024));

    /// Reusable thread-local Zstandard bulk compressor (avoids 128KB+ context re-allocation per request).
    static ZSTD_COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
}

/// Compresses a buffer using Gzip (RFC 1952) at the specified compression level (1-9).
///
/// Utilizes `zlib-rs` AVX2 vectorization, writes to thread-local scratch buffer,
/// and streams the result directly to RAM.
pub fn encode_gzip(data: &[u8], level: u32) -> std::io::Result<Bytes> {
    let compression = flate2::Compression::new(level.clamp(1, 9));
    SCRATCH_BUF.with(|buf| {
        let mut scratch = buf.borrow_mut();
        scratch.clear();
        {
            let mut encoder = flate2::write::GzEncoder::new(&mut *scratch, compression);
            encoder.write_all(data)?;
            encoder.finish()?;
        }
        Ok(streaming_to_bytes(&scratch))
    })
}

/// Compresses a buffer using Deflate (RFC 1951) at the specified compression level (1-9).
///
/// Utilizes `zlib-rs` AVX2 vectorization, writes to thread-local scratch buffer,
/// and streams the result directly to RAM.
pub fn encode_deflate(data: &[u8], level: u32) -> std::io::Result<Bytes> {
    let compression = flate2::Compression::new(level.clamp(1, 9));
    SCRATCH_BUF.with(|buf| {
        let mut scratch = buf.borrow_mut();
        scratch.clear();
        {
            let mut encoder = flate2::write::DeflateEncoder::new(&mut *scratch, compression);
            encoder.write_all(data)?;
            encoder.finish()?;
        }
        Ok(streaming_to_bytes(&scratch))
    })
}

/// Compresses a buffer using Brotli (RFC 7932) at the specified quality level (0-11).
///
/// In-memory buffer size: uses optimal 4096-byte chunk and 22-bit sliding window.
pub fn encode_brotli(data: &[u8], level: u32) -> std::io::Result<Bytes> {
    let quality = level.clamp(0, 11);
    SCRATCH_BUF.with(|buf| {
        let mut scratch = buf.borrow_mut();
        scratch.clear();
        {
            let mut compressor = brotli::CompressorWriter::new(&mut *scratch, 4096, quality, 22);
            compressor.write_all(data)?;
            compressor.flush()?;
        }
        Ok(streaming_to_bytes(&scratch))
    })
}

/// Compresses a buffer using Zstandard (RFC 8878) at the specified level (1-22).
///
/// Reuses thread-local `zstd::bulk::Compressor` to eliminate per-request `ZSTD_CCtx`
/// allocation overhead, streaming compressed bytes directly to RAM.
pub fn encode_zstd(data: &[u8], level: i32) -> std::io::Result<Bytes> {
    let level = level.clamp(1, 22);

    // Fast-path: use thread-local reusable bulk compressor
    ZSTD_COMPRESSOR.with(|c_cell| {
        let mut c_opt = c_cell.borrow_mut();
        if c_opt.is_none() {
            *c_opt = zstd::bulk::Compressor::new(level).ok();
        }

        if let Some(compressor) = c_opt.as_mut() {
            let max_dest_len = zstd::zstd_safe::compress_bound(data.len());
            SCRATCH_BUF.with(|buf| {
                let mut scratch = buf.borrow_mut();
                if scratch.len() < max_dest_len {
                    scratch.resize(max_dest_len, 0);
                }
                match compressor.compress_to_buffer(data, &mut scratch[..max_dest_len]) {
                    Ok(written) => Ok(streaming_to_bytes(&scratch[..written])),
                    Err(_) => {
                        // Fallback to one-shot encode if bulk buffer had issue
                        let compressed = zstd::encode_all(data, level)?;
                        Ok(streaming_to_bytes(&compressed))
                    }
                }
            })
        } else {
            let compressed = zstd::encode_all(data, level)?;
            Ok(streaming_to_bytes(&compressed))
        }
    })
}

/// Compresses a buffer using Zstandard (RFC 8878) with an optional pre-trained dictionary at the specified level (1-22).
pub fn encode_zstd_with_dict(
    data: &[u8],
    level: i32,
    dict: Option<&[u8]>,
) -> std::io::Result<Bytes> {
    let level = level.clamp(1, 22);
    match dict {
        Some(d) if !d.is_empty() => SCRATCH_BUF.with(|buf| {
            let mut scratch = buf.borrow_mut();
            scratch.clear();
            {
                let mut encoder = zstd::stream::Encoder::with_dictionary(&mut *scratch, level, d)?;
                encoder.write_all(data)?;
                encoder.finish()?;
            }
            Ok(streaming_to_bytes(&scratch))
        }),
        _ => encode_zstd(data, level),
    }
}

/// Decompresses a buffer using Zstandard (RFC 8878) with an optional pre-trained dictionary.
pub fn decode_zstd_with_dict(data: &[u8], dict: Option<&[u8]>) -> std::io::Result<Bytes> {
    match dict {
        Some(d) if !d.is_empty() => {
            use std::io::Read;
            let mut decoder = zstd::stream::Decoder::with_dictionary(data, d)?;
            let mut output = Vec::with_capacity(data.len() * 2);
            decoder.read_to_end(&mut output)?;
            Ok(Bytes::from(output))
        }
        _ => {
            let decompressed = zstd::decode_all(data)?;
            Ok(Bytes::from(decompressed))
        }
    }
}

/// Dispatches compression using the negotiated algorithm with optional Zstandard dictionary.
pub fn compress_payload_with_dict(
    data: &[u8],
    algo: CompressionAlgorithm,
    gzip_level: u32,
    brotli_level: u32,
    zstd_level: i32,
    zstd_dict: Option<&[u8]>,
) -> std::io::Result<Bytes> {
    match algo {
        CompressionAlgorithm::Zstd => encode_zstd_with_dict(data, zstd_level, zstd_dict),
        CompressionAlgorithm::Brotli => encode_brotli(data, brotli_level),
        CompressionAlgorithm::Gzip => encode_gzip(data, gzip_level),
        CompressionAlgorithm::Deflate => encode_deflate(data, gzip_level),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_roundtrip_gzip() {
        let original =
            b"Velda Edge High Performance L7 API Gateway with zero-copy stream processing!"
                .repeat(20);
        let compressed = encode_gzip(&original, 6).expect("gzip encode");
        assert!(compressed.len() < original.len());

        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).expect("gzip decode");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_roundtrip_deflate() {
        let original = b"Velda Edge Deflate Wire Codec Testing Stream Buffer".repeat(30);
        let compressed = encode_deflate(&original, 6).expect("deflate encode");
        assert!(compressed.len() < original.len());

        let mut decoder = flate2::read::DeflateDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .expect("deflate decode");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_roundtrip_brotli() {
        let original = b"{\"message\":\"hello from velda-edge post_response compression plugin\",\"code\":200}".repeat(25);
        let compressed = encode_brotli(&original, 4).expect("brotli encode");
        assert!(compressed.len() < original.len());

        let mut decompressed = Vec::new();
        brotli::Decompressor::new(&compressed[..], 4096)
            .read_to_end(&mut decompressed)
            .expect("brotli decode");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_roundtrip_zstd() {
        let original =
            b"Zstandard ultra-fast compression testing with JSON payloads in RAM".repeat(25);
        let compressed = encode_zstd(&original, 3).expect("zstd encode");
        assert!(compressed.len() < original.len());

        let decompressed = zstd::decode_all(&compressed[..]).expect("zstd decode");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_roundtrip_zstd_with_dict() {
        let dict = b"{\"user_id\":1001,\"username\":\"test_user\",\"role\":\"developer\",\"timestamp\":1700000000}";
        let original = b"{\"user_id\":1001,\"username\":\"test_user\",\"role\":\"developer\",\"timestamp\":1700000000,\"data\":\"extra_fields_here\"}".repeat(10);

        let compressed = encode_zstd_with_dict(&original, 1, Some(dict)).expect("zstd with dict");
        assert!(compressed.len() < original.len());

        let decompressed =
            decode_zstd_with_dict(&compressed, Some(dict)).expect("zstd decode dict");
        assert_eq!(decompressed, original[..]);
    }
}
