//! Content-Encoding Negotiation and Wire Compression Engines.
//!
//! Standards Compliance:
//! - RFC 9110 Section 12.5.3: `Accept-Encoding` quality factor (q-value) parsing and content negotiation.
//! - RFC 9110 Section 8.4.1: `Content-Encoding` headers.
//! - RFC 1952: Gzip file format specification (via `flate2`).
//! - RFC 1951: DEFLATE compressed data format specification (via `flate2`).
//! - RFC 7932: Brotli compressed data format (via `brotli`).
//! - RFC 8878: Zstandard compression and framing (via `zstd`).

use http::header::HeaderValue;
use std::io::Write;

/// Supported response compression algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionAlgorithm {
    /// Zstandard (RFC 8878) - Ultra-fast compression/decompression with modern high throughput.
    Zstd,
    /// Brotli (RFC 7932) - Industry-leading compression ratios for web text/JSON assets.
    Brotli,
    /// Gzip (RFC 1952) - Universal industry standard, compatible with 99.9% of all clients.
    Gzip,
    /// Deflate (RFC 1951) - Raw zlib deflate stream.
    Deflate,
}

impl CompressionAlgorithm {
    /// Returns the canonical HTTP content-coding token string.
    #[inline(always)]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zstd => "zstd",
            Self::Brotli => "br",
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
        }
    }

    /// Returns the pre-computed static [`HeaderValue`] for `Content-Encoding`.
    #[inline(always)]
    pub fn as_header_value(self) -> HeaderValue {
        match self {
            Self::Zstd => HeaderValue::from_static("zstd"),
            Self::Brotli => HeaderValue::from_static("br"),
            Self::Gzip => HeaderValue::from_static("gzip"),
            Self::Deflate => HeaderValue::from_static("deflate"),
        }
    }

    /// Parses an HTTP content-coding token (case-insensitive).
    pub fn from_token(token: &str) -> Option<Self> {
        let t = token.trim();
        if t.eq_ignore_ascii_case("zstd") {
            Some(Self::Zstd)
        } else if t.eq_ignore_ascii_case("br") {
            Some(Self::Brotli)
        } else if t.eq_ignore_ascii_case("gzip") || t.eq_ignore_ascii_case("x-gzip") {
            Some(Self::Gzip)
        } else if t.eq_ignore_ascii_case("deflate") {
            Some(Self::Deflate)
        } else {
            None
        }
    }
}

/// A parsed client encoding preference with an associated q-value weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClientPreference {
    pub algorithm: Option<CompressionAlgorithm>, // None represents wildcard '*'
    pub q: f32,
}

/// Parses the RFC 9110 `Accept-Encoding` header into a list of weighted preferences.
///
/// Example: `gzip, deflate;q=0.8, br;q=1.0, zstd;q=0.9, *;q=0.1`
/// Formula: Quality values range from 0.000 to 1.000. `q=0` explicitly denotes "not acceptable".
pub fn parse_accept_encoding(header_val: &str) -> Vec<ClientPreference> {
    let mut preferences = Vec::with_capacity(4);

    for part in header_val.split(',') {
        let mut segments = part.split(';');
        let token = match segments.next() {
            Some(t) => t.trim(),
            None => continue,
        };
        if token.is_empty() {
            continue;
        }

        let mut q: f32 = 1.0;
        for param in segments {
            let param = param.trim();
            if let Some(parsed_q) = param
                .strip_prefix("q=")
                .and_then(|val_str| val_str.trim().parse::<f32>().ok())
            {
                // Clamp to valid RFC range [0.0, 1.0]
                q = parsed_q.clamp(0.0, 1.0);
            }
        }

        if token == "*" {
            preferences.push(ClientPreference { algorithm: None, q });
        } else if let Some(algo) = CompressionAlgorithm::from_token(token) {
            preferences.push(ClientPreference {
                algorithm: Some(algo),
                q,
            });
        }
    }

    preferences
}

/// Negotiates the optimal compression algorithm between downstream client requirements
/// and gateway configured preferences (RFC 9110 Section 12.5.3).
///
/// Returns `None` if the client does not accept compression (`Accept-Encoding: identity` or empty/missing).
pub fn negotiate_algorithm(
    accept_encoding_header: Option<&HeaderValue>,
    server_preferences: &[CompressionAlgorithm],
) -> Option<CompressionAlgorithm> {
    let header_str = accept_encoding_header?.to_str().ok()?;
    if header_str.trim().is_empty() {
        return None;
    }

    let client_prefs = parse_accept_encoding(header_str);
    if client_prefs.is_empty() {
        return None;
    }

    // Step 1: Check for explicitly forbidden algorithms (q=0)
    let is_forbidden = |algo: CompressionAlgorithm| -> bool {
        client_prefs
            .iter()
            .any(|p| p.algorithm == Some(algo) && p.q == 0.0)
    };

    // Step 2: Find best matching algorithm with highest client q-factor,
    // tie-broken by server priority order in `server_preferences`.
    let mut best_algo: Option<CompressionAlgorithm> = None;
    let mut best_q: f32 = 0.0;

    for &server_algo in server_preferences {
        if is_forbidden(server_algo) {
            continue;
        }

        // Direct matching preference
        let direct_match = client_prefs
            .iter()
            .find(|p| p.algorithm == Some(server_algo));
        let wildcard_match = client_prefs.iter().find(|p| p.algorithm.is_none());

        let effective_q = if let Some(p) = direct_match {
            p.q
        } else if let Some(w) = wildcard_match {
            w.q
        } else {
            0.0
        };

        if effective_q > 0.0
            && (effective_q > best_q || (effective_q == best_q && best_algo.is_none()))
        {
            best_q = effective_q;
            best_algo = Some(server_algo);
        }
    }

    best_algo
}

// ============================================================================
// Wire Encoding Engines
// ============================================================================

/// Compresses a buffer using Gzip (RFC 1952) at the specified compression level (1-9).
pub fn encode_gzip(data: &[u8], level: u32) -> std::io::Result<bytes::Bytes> {
    let compression = flate2::Compression::new(level.clamp(1, 9));
    let mut encoder =
        flate2::write::GzEncoder::new(Vec::with_capacity(data.len() / 2 + 64), compression);
    encoder.write_all(data)?;
    let result = encoder.finish()?;
    Ok(bytes::Bytes::from(result))
}

/// Compresses a buffer using Deflate (RFC 1951) at the specified compression level (1-9).
pub fn encode_deflate(data: &[u8], level: u32) -> std::io::Result<bytes::Bytes> {
    let compression = flate2::Compression::new(level.clamp(1, 9));
    let mut encoder =
        flate2::write::DeflateEncoder::new(Vec::with_capacity(data.len() / 2 + 64), compression);
    encoder.write_all(data)?;
    let result = encoder.finish()?;
    Ok(bytes::Bytes::from(result))
}

/// Compresses a buffer using Brotli (RFC 7932) at the specified quality level (0-11).
///
/// In-memory buffer size: uses optimal 4096-byte chunk and 22-bit sliding window.
pub fn encode_brotli(data: &[u8], level: u32) -> std::io::Result<bytes::Bytes> {
    let quality = level.clamp(0, 11);
    let mut output = Vec::with_capacity(data.len() / 2 + 64);
    {
        let mut compressor = brotli::CompressorWriter::new(&mut output, 4096, quality, 22);
        compressor.write_all(data)?;
        compressor.flush()?;
    }
    Ok(bytes::Bytes::from(output))
}

/// Compresses a buffer using Zstandard (RFC 8878) at the specified level (1-22).
pub fn encode_zstd(data: &[u8], level: i32) -> std::io::Result<bytes::Bytes> {
    let level = level.clamp(1, 22);
    let compressed = zstd::encode_all(data, level)?;
    Ok(bytes::Bytes::from(compressed))
}

/// Compresses a buffer using Zstandard (RFC 8878) with an optional pre-trained dictionary at the specified level (1-22).
pub fn encode_zstd_with_dict(
    data: &[u8],
    level: i32,
    dict: Option<&[u8]>,
) -> std::io::Result<bytes::Bytes> {
    let level = level.clamp(1, 22);
    match dict {
        Some(d) if !d.is_empty() => {
            let mut encoder = zstd::stream::Encoder::with_dictionary(
                Vec::with_capacity(data.len() / 2 + 64),
                level,
                d,
            )?;
            encoder.write_all(data)?;
            let result = encoder.finish()?;
            Ok(bytes::Bytes::from(result))
        }
        _ => encode_zstd(data, level),
    }
}

/// Decompresses a buffer using Zstandard (RFC 8878) with an optional pre-trained dictionary.
pub fn decode_zstd_with_dict(data: &[u8], dict: Option<&[u8]>) -> std::io::Result<bytes::Bytes> {
    match dict {
        Some(d) if !d.is_empty() => {
            use std::io::Read;
            let mut decoder = zstd::stream::Decoder::with_dictionary(data, d)?;
            let mut output = Vec::with_capacity(data.len() * 2);
            decoder.read_to_end(&mut output)?;
            Ok(bytes::Bytes::from(output))
        }
        _ => {
            let decompressed = zstd::decode_all(data)?;
            Ok(bytes::Bytes::from(decompressed))
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
) -> std::io::Result<bytes::Bytes> {
    match algo {
        CompressionAlgorithm::Zstd => encode_zstd_with_dict(data, zstd_level, zstd_dict),
        CompressionAlgorithm::Brotli => encode_brotli(data, brotli_level),
        CompressionAlgorithm::Gzip => encode_gzip(data, gzip_level),
        CompressionAlgorithm::Deflate => encode_deflate(data, gzip_level),
    }
}

/// Dispatches compression using the negotiated algorithm.
#[inline(always)]
pub fn compress_payload(
    data: &[u8],
    algo: CompressionAlgorithm,
    gzip_level: u32,
    brotli_level: u32,
    zstd_level: i32,
) -> std::io::Result<bytes::Bytes> {
    compress_payload_with_dict(data, algo, gzip_level, brotli_level, zstd_level, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_parse_accept_encoding() {
        let header = "gzip, deflate;q=0.8, br;q=1.0, zstd;q=0.5, *;q=0.1";
        let prefs = parse_accept_encoding(header);

        assert_eq!(prefs.len(), 5);
        assert_eq!(
            prefs[0],
            ClientPreference {
                algorithm: Some(CompressionAlgorithm::Gzip),
                q: 1.0
            }
        );
        assert_eq!(
            prefs[1],
            ClientPreference {
                algorithm: Some(CompressionAlgorithm::Deflate),
                q: 0.8
            }
        );
        assert_eq!(
            prefs[2],
            ClientPreference {
                algorithm: Some(CompressionAlgorithm::Brotli),
                q: 1.0
            }
        );
        assert_eq!(
            prefs[3],
            ClientPreference {
                algorithm: Some(CompressionAlgorithm::Zstd),
                q: 0.5
            }
        );
        assert_eq!(
            prefs[4],
            ClientPreference {
                algorithm: None,
                q: 0.1
            }
        );
    }

    #[test]
    fn test_negotiate_algorithm_priority_and_q_factor() {
        let server_prefs = [
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Brotli,
            CompressionAlgorithm::Gzip,
        ];

        // Case 1: Client prefers Brotli (q=1.0) over Zstd (q=0.5)
        let header1 = HeaderValue::from_static("zstd;q=0.5, br;q=1.0, gzip;q=0.8");
        let algo1 = negotiate_algorithm(Some(&header1), &server_prefs);
        assert_eq!(algo1, Some(CompressionAlgorithm::Brotli));

        // Case 2: Equal q-factor -> Server preference tie-break (Zstd preferred over Brotli)
        let header2 = HeaderValue::from_static("gzip, br, zstd");
        let algo2 = negotiate_algorithm(Some(&header2), &server_prefs);
        assert_eq!(algo2, Some(CompressionAlgorithm::Zstd));

        // Case 3: Forbidden algorithm (q=0)
        let header3 = HeaderValue::from_static("zstd;q=0, br;q=0, gzip;q=0.9");
        let algo3 = negotiate_algorithm(Some(&header3), &server_prefs);
        assert_eq!(algo3, Some(CompressionAlgorithm::Gzip));

        // Case 4: Client only supports gzip
        let header4 = HeaderValue::from_static("gzip");
        let algo4 = negotiate_algorithm(Some(&header4), &server_prefs);
        assert_eq!(algo4, Some(CompressionAlgorithm::Gzip));

        // Case 5: Missing or empty header
        assert_eq!(negotiate_algorithm(None, &server_prefs), None);
        let header_empty = HeaderValue::from_static("");
        assert_eq!(
            negotiate_algorithm(Some(&header_empty), &server_prefs),
            None
        );
    }

    #[test]
    fn test_roundtrip_gzip() {
        let original = b"Velda Edge High Performance L7 API Gateway with zero-copy stream processing and W3C request ID invariant!".repeat(20);
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
