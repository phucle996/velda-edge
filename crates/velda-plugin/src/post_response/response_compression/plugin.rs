//! Response Compression Post-Response Plugin (`response_compression`).
//!
//! ```text
//!   Incoming L7Response (from upstream / http_cache)
//!         │
//!         ├─► Guard 0..2: empty config | 204/304/1xx | Content-Encoding already present ──► return
//!         ├─► Guard 3..4: body.len() < min_length | non-matching Content-Type          ──► return
//!         ├─► Guard 5   : negotiate_algorithm(req_headers["accept-encoding"]) (0 alloc)──► None => return
//!         ▼
//!   compress_payload_with_dict() (thread-local SCRATCH_BUF + non-temporal streaming_to_bytes)
//!         │
//!         ├─► compressed.len() >= original.len() ──► keep uncompressed body (inflation guard)
//!         └─► replace resp.body + Content-Length + Content-Encoding + Vary: Accept-Encoding
//! ```
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

use crate::config::PluginConfig;
use http::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, VARY};
use http::{HeaderValue, StatusCode};
use velda_core::{Body, L7Response};

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
        let cfg =
            super::validation::validate_response_compression_config(&config.id, &config.config)
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
            media_type
                .get(..pattern.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(pattern))
        })
    }
}

impl PostResponsePlugin for ResponseCompressionPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
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
                const SUFFIX: &[u8] = b", Accept-Encoding";
                let eb = existing_vary.as_bytes();
                let mut stack_buf = [0u8; 512];
                let total = eb.len().saturating_add(SUFFIX.len());
                let hv = if let Some(dst) = stack_buf.get_mut(..total) {
                    let (dst_head, dst_tail) = dst.split_at_mut(eb.len());
                    dst_head.copy_from_slice(eb);
                    dst_tail.copy_from_slice(SUFFIX);
                    HeaderValue::from_bytes(dst).ok()
                } else {
                    let mut heap = String::with_capacity(eb.len() + SUFFIX.len());
                    heap.push_str(existing_vary);
                    heap.push_str(", Accept-Encoding");
                    HeaderValue::from_str(&heap).ok()
                };
                if let Some(hv) = hv {
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
