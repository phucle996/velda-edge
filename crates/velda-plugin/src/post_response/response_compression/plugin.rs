//! Response Compression Post-Response Plugin (`response_compression`).
//!
//! Standards Compliance:
//! - RFC 9110 Section 12.5.3: Client Content Negotiation (`Accept-Encoding` quality weighting).
//! - RFC 9110 Section 8.4.1: `Content-Encoding` headers.
//! - RFC 9110 Section 12.5.5: `Vary: Accept-Encoding` injection to prevent cache poisoning across clients.
//!
//! # Architecture & Performance Invariants
//! - **Strictly Unidirectional Return Path**: Operates purely on outgoing response payloads
//!   (whether originated from upstream, served from `http_cache`, or returned via router mock).
//! - **Exact Request Workspace Borrowing**: Inspects `Accept-Encoding` from the specific
//!   in-flight request headers workspace (`req_headers`), guaranteeing zero cross-request contamination.
//! - **Pristine Cache Coordination**: Coexists transparently with `http_cache_store` and
//!   `http_cache_lookup`. Cache zones store canonical uncompressed payloads, while compression
//!   adapts each payload dynamically to the downstream client's advertised codec capabilities.
//! - **Zero Double-Compression**: Skips responses already carrying `Content-Encoding`.
//! - **Inflation Safeguard**: If compressed payload size is larger than original, the original
//!   uncompressed body is preserved to prevent bandwidth degradation.

use http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, VARY};
use http::{HeaderValue, StatusCode};
use velda_core::{Body, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use super::algorithm::{CompressionAlgorithm, compress_payload, negotiate_algorithm};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

/// Configuration schema for [`ResponseCompressionPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseCompressionConfig {
    /// Enabled algorithms in server priority order.
    pub algorithms: Vec<CompressionAlgorithm>,
    /// Minimum payload size in bytes required to trigger compression.
    /// Tiny payloads (< 256 bytes) expand in size due to framing and Huffman overhead.
    pub min_length: usize,
    /// Gzip/Deflate compression level (1-9, default: 6).
    pub gzip_level: u32,
    /// Brotli compression quality level (0-11, default: 4 for low edge latency).
    pub brotli_level: u32,
    /// Zstandard compression level (1-22, default: 3 for optimal speed/ratio).
    pub zstd_level: i32,
    /// MIME type prefixes eligible for compression.
    pub content_types: Vec<String>,
}

impl Default for ResponseCompressionConfig {
    fn default() -> Self {
        Self {
            algorithms: vec![
                CompressionAlgorithm::Zstd,
                CompressionAlgorithm::Brotli,
                CompressionAlgorithm::Gzip,
                CompressionAlgorithm::Deflate,
            ],
            min_length: 256,
            gzip_level: 6,
            brotli_level: 4,
            zstd_level: 3,
            content_types: vec![
                "application/json".to_string(),
                "text/plain".to_string(),
                "text/html".to_string(),
                "text/css".to_string(),
                "application/javascript".to_string(),
                "text/javascript".to_string(),
                "application/xml".to_string(),
                "text/xml".to_string(),
                "application/graphql+json".to_string(),
                "image/svg+xml".to_string(),
            ],
        }
    }
}

/// Post-Response Compression Plugin instance.
#[derive(Debug, Clone)]
pub struct ResponseCompressionPlugin {
    pub id: String,
    pub config: ResponseCompressionConfig,
}

impl ResponseCompressionPlugin {
    /// Constructs a new compression plugin with explicit configuration.
    pub fn new(id: impl Into<String>, config: ResponseCompressionConfig) -> Self {
        Self {
            id: id.into(),
            config,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = ResponseCompressionConfig::default();

        if config.config.is_null() {
            return Ok(Self::new(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. Parse algorithms list
        let mut algorithms = Vec::new();
        if let Some(arr) = raw.get("algorithms").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(algo) = item.as_str().and_then(CompressionAlgorithm::from_token) {
                    algorithms.push(algo);
                }
            }
        }
        if algorithms.is_empty() {
            algorithms = defaults.algorithms;
        }

        // 2. Parse min_length threshold
        let min_length = raw
            .get("min_length")
            .and_then(|v| v.as_u64())
            .map(|v| v as usize)
            .unwrap_or(defaults.min_length);

        // 3. Parse compression levels
        let gzip_level = raw
            .get("gzip_level")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(defaults.gzip_level);

        let brotli_level = raw
            .get("brotli_level")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(defaults.brotli_level);

        let zstd_level = raw
            .get("zstd_level")
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or(defaults.zstd_level);

        // 4. Parse content_types whitelist
        let mut content_types = Vec::new();
        if let Some(arr) = raw.get("content_types").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str() {
                    content_types.push(s.to_lowercase());
                }
            }
        }
        if content_types.is_empty() {
            content_types = defaults.content_types;
        }

        Ok(Self::new(
            &config.id,
            ResponseCompressionConfig {
                algorithms,
                min_length,
                gzip_level,
                brotli_level,
                zstd_level,
                content_types,
            },
        ))
    }

    /// Checks if the response Content-Type is eligible for compression.
    #[inline]
    fn is_compressible_content_type(&self, resp: &L7Response) -> bool {
        let content_type = match resp.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) {
            Some(ct) => ct.to_lowercase(),
            None => return false,
        };

        // Extract media type prefix before optional parameters (e.g. "application/json; charset=utf-8")
        let media_type = content_type.split(';').next().unwrap_or("").trim();

        self.config
            .content_types
            .iter()
            .any(|pattern| media_type.starts_with(pattern.as_str()) || media_type == pattern)
    }
}

impl PostResponsePlugin for ResponseCompressionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &http::HeaderMap, resp: &mut L7Response) {
        // Guard 1: Status Code Eligibility (RFC 9110)
        // 204 No Content, 304 Not Modified, and 1xx Informational MUST NOT contain body or be compressed.
        if resp.status == StatusCode::NO_CONTENT
            || resp.status == StatusCode::NOT_MODIFIED
            || resp.status.is_informational()
        {
            return;
        }

        // Guard 2: Already Compressed Check
        // If upstream or cache already applied Content-Encoding, never double-compress.
        if resp.headers.contains_key(CONTENT_ENCODING) {
            return;
        }

        // Guard 3: Body and Minimum Size Threshold
        let original_bytes = match &resp.body {
            Body::Bytes(bytes) => bytes,
            Body::Empty => return,
        };

        if original_bytes.len() < self.config.min_length {
            return;
        }

        // Guard 4: MIME Type Whitelist
        if !self.is_compressible_content_type(resp) {
            return;
        }

        // Guard 5: Downstream Content Negotiation (RFC 9110 § 12.5.3)
        // Strictly inspects `Accept-Encoding` from THIS specific request's borrowed workspace.
        let accept_encoding = req_headers.get(ACCEPT_ENCODING);
        let selected_algo = match negotiate_algorithm(accept_encoding, &self.config.algorithms) {
            Some(algo) => algo,
            None => return, // Client does not accept or support configured compression
        };

        // Execute In-Place Compression
        let compressed = match compress_payload(
            original_bytes,
            selected_algo,
            self.config.gzip_level,
            self.config.brotli_level,
            self.config.zstd_level,
        ) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::debug!(
                    plugin = %self.id,
                    error = %e,
                    algorithm = %selected_algo.as_str(),
                    "Failed to compress response payload; falling back to uncompressed body"
                );
                return;
            }
        };

        // Guard 6: Inflation Safeguard
        // If compressed bytes are equal to or larger than original, preserve raw body
        if compressed.len() >= original_bytes.len() {
            return;
        }

        // Apply Response Transformations
        let compressed_len = compressed.len();
        resp.body = Body::Bytes(compressed);

        // Set Content-Encoding
        resp.headers
            .insert(CONTENT_ENCODING, selected_algo.as_header_value());

        // Update Content-Length
        resp.headers
            .insert(CONTENT_LENGTH, HeaderValue::from(compressed_len));

        // Inject / Append Vary: Accept-Encoding (RFC 9110 § 12.5.5)
        if let Some(existing_vary) = resp.headers.get(VARY).and_then(|v| v.to_str().ok()) {
            if !existing_vary.to_lowercase().contains("accept-encoding") {
                let new_vary = format!("{existing_vary}, Accept-Encoding");
                if let Ok(val) = HeaderValue::from_str(&new_vary) {
                    resp.headers.insert(VARY, val);
                }
            }
        } else {
            resp.headers
                .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability {
            header: AccessLevel::Write,
            payload: AccessLevel::Write,
        }
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, StatusCode};
    use std::io::Read;
    use std::sync::Arc;
    use std::time::Duration;
    use velda_core::Body;

    #[test]
    fn test_compression_default_priority_zstd() {
        let plugin =
            ResponseCompressionPlugin::new("compress-1", ResponseCompressionConfig::default());

        let mut req_headers = HeaderMap::new();
        req_headers.insert(
            ACCEPT_ENCODING,
            HeaderValue::from_static("gzip, deflate, br, zstd"),
        );

        let payload = b"{\"users\":[{\"id\":1,\"name\":\"Alice\",\"role\":\"Admin\"},{\"id\":2,\"name\":\"Bob\",\"role\":\"User\"}]}".repeat(10);
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "zstd"); // Default priority prefers zstd
        assert_eq!(resp.headers.get(VARY).unwrap(), "Accept-Encoding");
        let compressed_len: usize = resp
            .headers
            .get(CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();

        match resp.body {
            Body::Bytes(b) => {
                assert_eq!(b.len(), compressed_len);
                assert!(b.len() < payload.len());
                let decompressed = zstd::decode_all(&b[..]).expect("valid zstd");
                assert_eq!(decompressed, payload);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_compression_brotli_explicit() {
        let config = ResponseCompressionConfig {
            algorithms: vec![CompressionAlgorithm::Brotli, CompressionAlgorithm::Gzip],
            ..Default::default()
        };
        let plugin = ResponseCompressionPlugin::new("compress-br", config);

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("br"));

        let payload = b"Velda Edge API Gateway Brotli Compression Testing Payload".repeat(15);
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain"));

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "br");
        match resp.body {
            Body::Bytes(b) => {
                let mut decompressed = Vec::new();
                brotli::Decompressor::new(&b[..], 4096)
                    .read_to_end(&mut decompressed)
                    .expect("valid brotli");
                assert_eq!(decompressed, payload);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_compression_gzip_when_only_gzip_accepted() {
        let plugin =
            ResponseCompressionPlugin::new("compress-gz", ResponseCompressionConfig::default());

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        let payload = b"{\"message\":\"hello world gzip compression\"}".repeat(20);
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
        match resp.body {
            Body::Bytes(b) => {
                let mut decoder = flate2::read::GzDecoder::new(&b[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).expect("valid gzip");
                assert_eq!(decompressed, payload);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_compression_bypass_min_length() {
        let plugin = ResponseCompressionPlugin::new(
            "compress-min",
            ResponseCompressionConfig {
                min_length: 256,
                ..Default::default()
            },
        );

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        // Only 20 bytes -> below min_length
        let tiny_payload = b"{\"status\":\"ok\"}";
        let mut resp = L7Response::from_bytes(StatusCode::OK, tiny_payload.to_vec());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        plugin.on_response(&req_headers, &mut resp);

        // Content-Encoding should NOT be set
        assert!(!resp.headers.contains_key(CONTENT_ENCODING));
        match resp.body {
            Body::Bytes(b) => assert_eq!(&b[..], tiny_payload),
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_compression_bypass_already_compressed() {
        let plugin =
            ResponseCompressionPlugin::new("compress-skip", ResponseCompressionConfig::default());

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, br"));

        let payload = b"already compressed by backend".repeat(20);
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        resp.headers
            .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));

        plugin.on_response(&req_headers, &mut resp);

        // Header and body must remain unchanged (no double-compression)
        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
        match resp.body {
            Body::Bytes(b) => assert_eq!(&b[..], &payload[..]),
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_compression_bypass_non_compressible_mime() {
        let plugin =
            ResponseCompressionPlugin::new("compress-mime", ResponseCompressionConfig::default());

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, br"));

        let payload = vec![0xFF; 500]; // Simulated PNG bytes
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.clone());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("image/png"));

        plugin.on_response(&req_headers, &mut resp);

        assert!(!resp.headers.contains_key(CONTENT_ENCODING));
    }

    #[test]
    fn test_compression_bypass_status_204_and_304() {
        let plugin =
            ResponseCompressionPlugin::new("compress-status", ResponseCompressionConfig::default());

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, br"));

        let mut resp = L7Response::empty(StatusCode::NO_CONTENT);
        plugin.on_response(&req_headers, &mut resp);
        assert!(!resp.headers.contains_key(CONTENT_ENCODING));

        let mut resp304 = L7Response::empty(StatusCode::NOT_MODIFIED);
        plugin.on_response(&req_headers, &mut resp304);
        assert!(!resp304.headers.contains_key(CONTENT_ENCODING));
    }

    #[test]
    fn test_cache_coordination_store_uncompressed_then_compress_on_return() {
        use crate::http_cache::HttpCacheZone;
        use crate::post_response::HttpCacheStoreConfig;
        use crate::post_response::HttpCacheStorePlugin;

        let zone = Arc::new(HttpCacheZone::new(
            "compress_zone",
            1024 * 1024,
            Duration::from_secs(60),
        ));
        let store_plugin = HttpCacheStorePlugin::with_zone(
            "store-1",
            HttpCacheStoreConfig::default(),
            Arc::clone(&zone),
        );
        let compress_plugin = ResponseCompressionPlugin::new(
            "compress-1",
            ResponseCompressionConfig {
                algorithms: vec![CompressionAlgorithm::Gzip],
                ..Default::default()
            },
        );

        // 1. Upstream emits 200 OK uncompressed JSON
        let original_json = b"{\"items\":[{\"id\":101,\"name\":\"MacBook Pro\"},{\"id\":102,\"name\":\"ThinkPad X1\"}]}".repeat(10);
        let mut resp = L7Response::from_bytes(StatusCode::OK, original_json.clone());
        resp.headers.insert(
            "x-velda-cache-key",
            HeaderValue::from_static("GET:api.example.com:/products"),
        );
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        resp.headers.insert(
            http::header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=60"),
        );

        let mut req_headers = HeaderMap::new();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        // 2. Sequential execution in post_response phase:
        //    Step A: http_cache_store stores the pristine uncompressed body into RAM zone
        store_plugin.on_response(&req_headers, &mut resp);
        assert_eq!(resp.headers.get("x-cache").unwrap(), "MISS");

        // Verify the cache zone has UNCOMPRESSED bytes!
        let hash = HttpCacheZone::hash_key("GET:api.example.com:/products");
        let stored_entry = zone.get(hash).expect("Must be stored in cache");
        assert_eq!(stored_entry.body, bytes::Bytes::from(original_json.clone()));
        assert!(!stored_entry.headers.contains_key(CONTENT_ENCODING));

        //    Step B: response_compression compresses the payload on the exit return path for client
        compress_plugin.on_response(&req_headers, &mut resp);
        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
        assert_eq!(resp.headers.get(VARY).unwrap(), "Accept-Encoding");

        // Downstream response body is compressed
        match resp.body {
            Body::Bytes(b) => {
                assert!(b.len() < original_json.len());
                let mut decoder = flate2::read::GzDecoder::new(&b[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).expect("valid gzip");
                assert_eq!(decompressed, original_json);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }
}
