//! Cache Entry & Compressed Variants Data Model.
//!
//! Implements the Pure Payload Cache Architecture:
//! Stores only pure payload bytes (`Bytes`) and its canonical `Content-Type`.
//! Zero transient or session headers (`HeaderMap`) are persisted in cache.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use velda_core::coarse_now_sec;

/// Pre-compressed wire payload variants for a cached entry.
///
/// In accordance with Pure Payload Architecture:
/// Holds only pure compressed bytes (`Bytes`), zero headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompressedVariants {
    pub gzip: Option<Bytes>,
    pub brotli: Option<Bytes>,
    pub zstd: Option<Bytes>,
}

impl CompressedVariants {
    /// Estimated memory footprint of the compressed variants in bytes.
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        self.gzip.as_ref().map_or(0, |b| b.len())
            + self.brotli.as_ref().map_or(0, |b| b.len())
            + self.zstd.as_ref().map_or(0, |b| b.len())
    }
}

/// A single cached HTTP response entry in RAM.
///
/// In accordance with the Pure Payload Cache Architecture:
/// Only the response payload (`Bytes`) and its canonical `Content-Type` are stored.
/// Zero transient, hop-by-hop, or session headers (`HeaderMap`) are persisted in cache.
#[derive(Debug)]
pub struct CacheEntry {
    /// Canonical cache key string for debugging and diagnostics.
    pub key: Arc<str>,
    /// HTTP status code (e.g. 200 OK, 301 Moved Permanently).
    pub status: StatusCode,
    /// Preserved Content-Type header value for MIME type fidelity.
    pub content_type: Option<http::HeaderValue>,
    /// Zero-copy reference-counted uncompressed response body bytes (pure canonical payload).
    /// Optional in Lazy Mode if only compressed variants have been requested so far.
    pub raw_body: Option<Bytes>,
    /// Pre-compressed wire payload variants for zero-CPU content negotiation.
    pub variants: CompressedVariants,
    /// Whether this entry was declared public (RFC 9111 §3.5: allows serving to authenticated requests).
    pub is_public: bool,
    /// Coarse Unix timestamp in seconds when this entry was created.
    pub created_at_sec: u32,
    /// Coarse Unix timestamp in seconds when this entry expires.
    pub expires_at_sec: u32,
    /// Second-chance bit for lockless $O(1)$ Clock eviction.
    pub accessed: AtomicBool,
}

impl Clone for CacheEntry {
    fn clone(&self) -> Self {
        Self {
            key: Arc::clone(&self.key),
            status: self.status,
            content_type: self.content_type.clone(),
            raw_body: self.raw_body.clone(),
            variants: self.variants.clone(),
            is_public: self.is_public,
            created_at_sec: self.created_at_sec,
            expires_at_sec: self.expires_at_sec,
            accessed: AtomicBool::new(self.accessed.load(Ordering::Relaxed)),
        }
    }
}

impl CacheEntry {
    /// Constructs a new [`CacheEntry`] using integer coarse timestamps.
    pub fn new(
        key: impl Into<Arc<str>>,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        raw_body: Option<Bytes>,
        ttl: Duration,
    ) -> Self {
        let now = coarse_now_sec() as u32;
        let ttl_secs = ttl.as_secs().max(1) as u32;
        Self {
            key: key.into(),
            status,
            content_type,
            raw_body,
            variants: CompressedVariants::default(),
            is_public: false,
            created_at_sec: now,
            expires_at_sec: now.saturating_add(ttl_secs),
            accessed: AtomicBool::new(true),
        }
    }

    /// Convenience constructor with raw bytes.
    pub fn with_raw(
        key: impl Into<Arc<str>>,
        status: StatusCode,
        content_type: Option<http::HeaderValue>,
        raw_body: Bytes,
        ttl: Duration,
    ) -> Self {
        Self::new(key, status, content_type, Some(raw_body), ttl)
    }

    /// Attaches explicit integer timestamps (convenient for deterministic testing).
    pub fn with_timestamps(mut self, created_at_sec: u32, expires_at_sec: u32) -> Self {
        self.created_at_sec = created_at_sec;
        self.expires_at_sec = expires_at_sec;
        self
    }

    /// Attaches pre-computed compressed payload variants to this entry.
    pub fn with_variants(mut self, variants: CompressedVariants) -> Self {
        self.variants = variants;
        self
    }

    /// Attaches public availability marker according to RFC 9111 §3.5.
    pub fn with_public(mut self, is_public: bool) -> Self {
        self.is_public = is_public;
        self
    }

    /// Returns `true` if this entry has not expired (evaluated via integer coarse clock).
    #[inline(always)]
    pub fn is_fresh(&self) -> bool {
        let now = coarse_now_sec() as u32;
        now < self.expires_at_sec
    }

    /// Returns the age of this entry in seconds.
    #[inline(always)]
    pub fn age_secs(&self) -> u64 {
        let now = coarse_now_sec() as u32;
        now.saturating_sub(self.created_at_sec) as u64
    }

    /// Estimated memory footprint of this entry in bytes including all compressed variants.
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        let raw_len = self.raw_body.as_ref().map(|b| b.len()).unwrap_or(0);
        raw_len + self.variants.memory_size() + self.key.len() + 64
    }

    /// Returns the negotiated body bytes and optional content-encoding header value
    /// based on the client's `Accept-Encoding` preferences and currently cached variants (RFC 9110 §12.5.3).
    ///
    /// Implements Smart Variant Matching:
    /// 1. Hit Over Miss: If client accepts any encoding currently present in cache,
    ///    immediately serves it (avoiding upstream roundtrip).
    /// 2. Codec Hierarchy: If multiple acceptable variants are in cache, resolves by server
    ///    preference: Zstandard -> Brotli -> Gzip -> Raw Uncompressed.
    /// 3. Returns None if client does not accept any of the available variants (requiring upstream fetch).
    #[inline]
    pub fn negotiate_smart_variant(
        &self,
        accept_encoding: Option<&http::HeaderValue>,
    ) -> Option<(Bytes, Option<http::HeaderValue>)> {
        use crate::post_response::response_compression::{
            CompressionAlgorithm, negotiate_algorithm,
        };

        // Determine available algorithms currently stored in this entry
        let mut available_server_algos = Vec::with_capacity(3);
        if self.variants.zstd.is_some() {
            available_server_algos.push(CompressionAlgorithm::Zstd);
        }
        if self.variants.brotli.is_some() {
            available_server_algos.push(CompressionAlgorithm::Brotli);
        }
        if self.variants.gzip.is_some() {
            available_server_algos.push(CompressionAlgorithm::Gzip);
        }

        // 1. If client sent Accept-Encoding and we have compressed variants, negotiate among available
        if !available_server_algos.is_empty()
            && let Some(selected) = negotiate_algorithm(accept_encoding, &available_server_algos)
        {
            match selected {
                CompressionAlgorithm::Zstd => {
                    if let Some(zstd) = &self.variants.zstd {
                        return Some((zstd.clone(), Some(http::HeaderValue::from_static("zstd"))));
                    }
                }
                CompressionAlgorithm::Brotli => {
                    if let Some(br) = &self.variants.brotli {
                        return Some((br.clone(), Some(http::HeaderValue::from_static("br"))));
                    }
                }
                CompressionAlgorithm::Gzip => {
                    if let Some(gzip) = &self.variants.gzip {
                        return Some((gzip.clone(), Some(http::HeaderValue::from_static("gzip"))));
                    }
                }
                _ => {}
            }
        }

        // 2. Fallback to uncompressed raw body if available and acceptable to client
        if let Some(raw) = &self.raw_body {
            if let Some(header) = accept_encoding
                && let Ok(header_str) = header.to_str()
            {
                if crate::post_response::response_compression::is_identity_acceptable(header_str) {
                    return Some((raw.clone(), None));
                }
            } else {
                return Some((raw.clone(), None));
            }
        }

        // 3. No compatible representation available in cache for this client
        None
    }
}

/// Parameters for storing or merging a cache variant into a [`HttpCacheStorage`].
#[derive(Debug, Clone)]
pub struct StoreVariantParams<'a> {
    pub hash: u64,
    pub key: String,
    pub status: StatusCode,
    pub content_type: Option<http::HeaderValue>,
    pub ttl: Duration,
    pub encoding: Option<&'a str>,
    pub payload: Bytes,
    pub variants: Option<CompressedVariants>,
    pub is_public: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    #[test]
    fn test_cache_entry_is_public_flag() {
        let entry = CacheEntry::with_raw(
            "GET:example.com:/pub",
            StatusCode::OK,
            None,
            Bytes::from_static(b"public data"),
            Duration::from_secs(60),
        )
        .with_public(true);

        assert!(entry.is_public);
        assert!(entry.is_fresh());
    }

    #[test]
    fn test_smart_variant_matching_hit_over_miss_lazy() {
        // Simulates Lazy Mode where backend only generated Brotli variant
        let mut entry = CacheEntry::new(
            "GET:example.com:/lazy",
            StatusCode::OK,
            None,
            None, // No raw uncompressed body!
            Duration::from_secs(60),
        );
        entry.variants.brotli = Some(Bytes::from_static(b"brotli-only-data"));

        // Client supports gzip AND br -> Hit over Miss serves br instantly!
        let ae = HeaderValue::from_static("gzip, br;q=0.8");
        let res = entry.negotiate_smart_variant(Some(&ae));
        assert!(res.is_some());
        let (payload, enc) = res.unwrap();
        assert_eq!(payload.as_ref(), b"brotli-only-data");
        assert_eq!(enc.unwrap().to_str().unwrap(), "br");

        // Client supports ONLY gzip -> Cannot serve Brotli -> Returns None for upstream fetch
        let ae_gzip_only = HeaderValue::from_static("gzip");
        let res_gzip = entry.negotiate_smart_variant(Some(&ae_gzip_only));
        assert!(res_gzip.is_none());
    }
}
