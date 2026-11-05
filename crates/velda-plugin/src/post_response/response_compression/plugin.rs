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

use super::config::ResponseCompressionConfig;
use super::dictionary::ZstdDictionaryConfig;
use super::engine::compress_payload_with_dict;
use super::negotiation::{CompressionAlgorithm, negotiate_algorithm};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

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
        let cfg = velda_sync::validate_response_compression_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid response_compression configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut algorithms = Vec::with_capacity(cfg.algorithms.len());
        for item in &cfg.algorithms {
            let algo = CompressionAlgorithm::from_token(item).ok_or_else(|| {
                PluginError::Config(format!(
                    "Unsupported compression algorithm '{item}' in plugin '{}'",
                    config.id
                ))
            })?;
            algorithms.push(algo);
        }

        let zstd_dict = ZstdDictionaryConfig::from_optional_str(cfg.zstd_dict.as_deref());

        let content_types = cfg
            .content_types
            .into_iter()
            .map(|s| s.to_lowercase())
            .collect();

        Ok(Self::new(
            &config.id,
            ResponseCompressionConfig {
                algorithms,
                min_length: cfg.min_length_bytes,
                gzip_level: cfg.gzip_level,
                brotli_level: cfg.brotli_level,
                zstd_level: cfg.zstd_level,
                zstd_dict,
                content_types,
            },
        ))
    }

    /// Checks if the response Content-Type is eligible for compression.
    ///
    /// Executes with zero heap allocations by scanning the raw header slice case-insensitively.
    #[inline]
    fn is_compressible_content_type(&self, resp: &L7Response) -> bool {
        let content_type = match resp.headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok()) {
            Some(ct) => ct,
            None => return false,
        };

        // Extract media type prefix before optional parameters (e.g. "application/json; charset=utf-8")
        let media_type = content_type.split(';').next().unwrap_or("").trim();

        self.config.content_types.iter().any(|pattern| {
            if media_type.len() >= pattern.len() {
                media_type[..pattern.len()].eq_ignore_ascii_case(pattern)
            } else {
                false
            }
        })
    }
}

impl PostResponsePlugin for ResponseCompressionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &velda_core::HeaderWorkspace, resp: &mut L7Response) {
        // Guard 0: Explicit Declaration Invariant
        if self.config.algorithms.is_empty() || self.config.content_types.is_empty() {
            return;
        }

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
        let original_bytes = resp.body.as_slice();

        if original_bytes.is_empty() || original_bytes.len() < self.config.min_length {
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
        let compressed = match compress_payload_with_dict(
            original_bytes,
            selected_algo,
            self.config.gzip_level,
            self.config.brotli_level,
            self.config.zstd_level,
            self.config.zstd_dict.as_bytes(),
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

        // Safeguard: Content Expansion Guard
        // If compressed size is not strictly smaller than original, keep raw body to avoid bandwidth expansion.
        if compressed.len() >= original_bytes.len() {
            return;
        }

        // Apply Response Transformations
        let new_len = compressed.len();
        resp.body = Body::Bytes(compressed);

        // Update Content-Length via arithmetic HeaderWorkspace (also updates content_length register)
        resp.headers.set_u64(CONTENT_LENGTH, new_len as u64);

        // Inject Content-Encoding
        resp.headers.insert(
            CONTENT_ENCODING,
            HeaderValue::from_static(selected_algo.as_str()),
        );

        // Inject Vary: Accept-Encoding (RFC 9110 § 12.5.5)
        // Prevents downstream caching proxies from serving compressed variants to uncompressed clients.
        if let Some(existing_vary) = resp.headers.get(VARY).and_then(|v| v.to_str().ok()) {
            let has_vary = existing_vary.split(',').any(|part| {
                part.trim().eq_ignore_ascii_case("accept-encoding") || part.trim() == "*"
            });

            if !has_vary {
                let mut new_val = String::with_capacity(existing_vary.len() + 17);
                new_val.push_str(existing_vary);
                new_val.push_str(", Accept-Encoding");
                if let Ok(hv) = HeaderValue::from_str(&new_val) {
                    resp.headers.insert(VARY, hv);
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
    use super::super::dictionary::BUILTIN_COMMON_JSON_REST_DICT;
    use super::*;

    fn standard_test_config() -> ResponseCompressionConfig {
        ResponseCompressionConfig {
            algorithms: vec![
                CompressionAlgorithm::Zstd,
                CompressionAlgorithm::Brotli,
                CompressionAlgorithm::Gzip,
            ],
            min_length: 64,
            gzip_level: 1,
            brotli_level: 2,
            zstd_level: 1,
            zstd_dict: ZstdDictionaryConfig::None,
            content_types: vec!["application/json".into(), "text/".into()],
        }
    }

    #[test]
    fn test_compression_gzip_application_json() {
        let plugin = ResponseCompressionPlugin::new("compress-gzip", standard_test_config());

        let mut req_headers = velda_core::HeaderWorkspace::default();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        let original_json = b"{\"message\":\"hello world from velda-edge post_response plugin compression test\",\"status\":\"active\",\"code\":200}".repeat(5);
        let mut resp = L7Response::from_bytes(StatusCode::OK, original_json.clone());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
        assert_eq!(resp.headers.get(VARY).unwrap(), "Accept-Encoding");

        match resp.body {
            Body::Bytes(b) => {
                assert!(b.len() < original_json.len());
                let mut decoder = flate2::read::GzDecoder::new(&b[..]);
                let mut decompressed = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut decompressed).unwrap();
                assert_eq!(decompressed, original_json);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_compression_zstd_with_builtin_dict() {
        let config = ResponseCompressionConfig {
            algorithms: vec![CompressionAlgorithm::Zstd],
            zstd_dict: ZstdDictionaryConfig::Builtin(
                super::super::dictionary::BuiltinDictionary::CommonJsonRest,
            ),
            ..standard_test_config()
        };
        let plugin = ResponseCompressionPlugin::new("compress-builtin-dict", config);

        let mut req_headers = velda_core::HeaderWorkspace::default();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("zstd"));

        let payload = br#"{"status":"success","code":200,"message":"Resource retrieved successfully","data":[{"id":"usr_101","username":"dev_user","email":"dev@example.com","role":"admin","status":"active"}]}"#;
        let mut resp = L7Response::from_bytes(StatusCode::OK, payload.to_vec());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "zstd");
        match resp.body {
            Body::Bytes(b) => {
                let decompressed = super::super::engine::decode_zstd_with_dict(
                    &b,
                    Some(BUILTIN_COMMON_JSON_REST_DICT),
                )
                .expect("valid zstd builtin dict");
                assert_eq!(decompressed, payload[..]);
            }
            _ => panic!("Expected Body::Bytes"),
        }
    }

    #[test]
    fn test_from_config_strict_validation() {
        let valid_json = serde_json::json!({
            "algorithms": ["zstd", "brotli", "gzip"],
            "min_length_bytes": 256,
            "gzip_level": 1,
            "brotli_level": 2,
            "zstd_level": 1,
            "zstd_dict": null,
            "content_types": ["text/", "application/json"]
        });

        let p_cfg = PluginConfig {
            id: "comp_test".into(),
            phase: "post_response".into(),
            config: valid_json,
        };

        let plugin = ResponseCompressionPlugin::from_config(&p_cfg).unwrap();
        assert_eq!(plugin.config.algorithms.len(), 3);
        assert_eq!(plugin.config.min_length, 256);
        assert_eq!(plugin.config.gzip_level, 1);
        assert_eq!(plugin.config.brotli_level, 2);
        assert_eq!(plugin.config.zstd_level, 1);
        assert_eq!(plugin.config.zstd_dict, ZstdDictionaryConfig::None);

        // Test built-in common_json_rest dictionary resolution
        let builtin_json = serde_json::json!({
            "algorithms": ["zstd"],
            "min_length_bytes": 256,
            "gzip_level": 1,
            "brotli_level": 2,
            "zstd_level": 1,
            "zstd_dict": "common_json_rest",
            "content_types": ["application/json"]
        });
        let builtin_cfg = PluginConfig {
            id: "comp_builtin".into(),
            phase: "post_response".into(),
            config: builtin_json,
        };
        let builtin_plugin = ResponseCompressionPlugin::from_config(&builtin_cfg).unwrap();
        assert_eq!(
            builtin_plugin.config.zstd_dict.as_bytes().unwrap(),
            BUILTIN_COMMON_JSON_REST_DICT
        );
    }

    #[test]
    fn test_guards_prevent_compression() {
        let plugin = ResponseCompressionPlugin::new("compress-guard", standard_test_config());
        let mut req_headers = velda_core::HeaderWorkspace::default();
        req_headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));

        // Guard 1: Status code 204
        let mut resp = L7Response::from_bytes(StatusCode::NO_CONTENT, b"some content".to_vec());
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        plugin.on_response(&req_headers, &mut resp);
        assert!(!resp.headers.contains_key(CONTENT_ENCODING));

        // Guard 2: Already compressed
        let mut resp = L7Response::from_bytes(StatusCode::OK, b"compressed".repeat(20));
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        resp.headers
            .insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
        plugin.on_response(&req_headers, &mut resp);
        assert_eq!(resp.headers.get(CONTENT_ENCODING).unwrap(), "br");

        // Guard 4: Non-matching content type
        let mut resp = L7Response::from_bytes(StatusCode::OK, b"compressed".repeat(20));
        resp.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("image/png"));
        plugin.on_response(&req_headers, &mut resp);
        assert!(!resp.headers.contains_key(CONTENT_ENCODING));
    }
}
