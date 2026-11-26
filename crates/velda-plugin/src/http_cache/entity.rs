//! Cache Entry, Variant Negotiation & Storage Transfer Structures.
//!
//! ```text
//!   negotiate_variant_kind(variant_mask, has_raw, Accept-Encoding, preferred_encodings)
//!     ├─ Accept-Encoding: None ──► Raw (if has_raw) | None (Miss -> fill upstream)
//!     └─ Accept-Encoding: Some ──► filter preferred_encodings by variant_mask (priority order)
//!                                    ├─ match ─► compressed VariantKind (Zstd / Brotli / Gzip)
//!                                    └─ none  ─► Raw (if has_raw & identity acceptable) | None
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use velda_core::clock::coarse_sec;

use crate::post_response::response_compression::{
    CompressionAlgorithm, is_identity_acceptable, negotiate_algorithm,
};

pub const VARIANT_BIT_ZSTD: u8 = 1 << 0;
pub const VARIANT_BIT_BROTLI: u8 = 1 << 1;
pub const VARIANT_BIT_GZIP: u8 = 1 << 2;

/// Selected payload representation on a cache hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VariantKind {
    Raw,
    Zstd,
    Brotli,
    Gzip,
}

impl VariantKind {
    /// Returns the HTTP `Content-Encoding` header value for compressed variants.
    #[inline(always)]
    pub fn content_encoding_header(self) -> Option<http::HeaderValue> {
        match self {
            VariantKind::Raw => None,
            VariantKind::Zstd => Some(http::HeaderValue::from_static("zstd")),
            VariantKind::Brotli => Some(http::HeaderValue::from_static("br")),
            VariantKind::Gzip => Some(http::HeaderValue::from_static("gzip")),
        }
    }
}

/// Zero-allocation smart variant selector operating purely on `variant_mask`, `has_raw`,
/// client `Accept-Encoding`, and the configured priority-ordered `preferred_encodings`.
#[inline]
pub fn negotiate_variant_kind(
    variant_mask: u8,
    has_raw: bool,
    accept_encoding: Option<&http::HeaderValue>,
    preferred_encodings: &[CompressionAlgorithm],
) -> Option<VariantKind> {
    if let Some(ae) = accept_encoding {
        if !preferred_encodings.is_empty() && variant_mask != 0 {
            let mut stack_algos = [CompressionAlgorithm::Gzip; 3];
            let mut count = 0usize;

            for &algo in preferred_encodings {
                let bit = match algo {
                    CompressionAlgorithm::Zstd => VARIANT_BIT_ZSTD,
                    CompressionAlgorithm::Brotli => VARIANT_BIT_BROTLI,
                    CompressionAlgorithm::Gzip => VARIANT_BIT_GZIP,
                    CompressionAlgorithm::Deflate => 0,
                };
                if bit != 0
                    && (variant_mask & bit) != 0
                    && !stack_algos[..count].contains(&algo)
                    && let Some(slot) = stack_algos.get_mut(count)
                {
                    *slot = algo;
                    count += 1;
                }
            }

            if count > 0
                && let Some(available) = stack_algos.get(..count)
                && let Some(selected) = negotiate_algorithm(Some(ae), available)
            {
                match selected {
                    CompressionAlgorithm::Zstd => return Some(VariantKind::Zstd),
                    CompressionAlgorithm::Brotli => return Some(VariantKind::Brotli),
                    CompressionAlgorithm::Gzip => return Some(VariantKind::Gzip),
                    CompressionAlgorithm::Deflate => {}
                }
            }
        }

        if has_raw {
            if let Ok(header_str) = ae.to_str() {
                if is_identity_acceptable(header_str) {
                    return Some(VariantKind::Raw);
                }
            } else {
                return Some(VariantKind::Raw);
            }
        }
        return None;
    }

    // Absent `Accept-Encoding`: strictly serve plain-text (`raw`) if cached, otherwise miss.
    if has_raw {
        Some(VariantKind::Raw)
    } else {
        None
    }
}

/// Pre-compressed wire payload variants for a cached entry.
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

    /// Returns the bitmask of populated compressed variants.
    #[inline(always)]
    pub fn mask(&self) -> u8 {
        let mut mask = 0u8;
        if self.zstd.is_some() {
            mask |= VARIANT_BIT_ZSTD;
        }
        if self.brotli.is_some() {
            mask |= VARIANT_BIT_BROTLI;
        }
        if self.gzip.is_some() {
            mask |= VARIANT_BIT_GZIP;
        }
        mask
    }
}

/// Materialized view of a cached HTTP response entry.
#[derive(Debug)]
pub struct CacheEntry {
    pub key: Arc<str>,
    pub status: StatusCode,
    pub content_type: Option<http::HeaderValue>,
    pub raw_body: Option<Bytes>,
    pub variants: CompressedVariants,
    pub is_public: bool,
    pub created_at_sec: u32,
    pub expires_at_sec: u32,
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
        let now = coarse_sec() as u32;
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

    /// Attaches explicit integer timestamps.
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
        let now = coarse_sec() as u32;
        now < self.expires_at_sec
    }

    /// Returns the age of this entry in seconds.
    #[inline(always)]
    pub fn age_secs(&self) -> u64 {
        let now = coarse_sec() as u32;
        now.saturating_sub(self.created_at_sec) as u64
    }

    /// Estimated memory footprint of this entry in bytes including all compressed variants.
    #[inline(always)]
    pub fn memory_size(&self) -> usize {
        let raw_len = self.raw_body.as_ref().map_or(0, |b| b.len());
        raw_len + self.variants.memory_size() + self.key.len() + 64
    }

    /// Selects the negotiated body bytes and optional `Content-Encoding` header using stack allocation.
    #[inline]
    pub fn negotiate_smart_variant(
        &self,
        accept_encoding: Option<&http::HeaderValue>,
        preferred_encodings: &[CompressionAlgorithm],
    ) -> Option<(Bytes, Option<http::HeaderValue>)> {
        let kind = negotiate_variant_kind(
            self.variants.mask(),
            self.raw_body.is_some(),
            accept_encoding,
            preferred_encodings,
        )?;
        let bytes = match kind {
            VariantKind::Zstd => self.variants.zstd.clone()?,
            VariantKind::Brotli => self.variants.brotli.clone()?,
            VariantKind::Gzip => self.variants.gzip.clone()?,
            VariantKind::Raw => self.raw_body.clone()?,
        };
        Some((bytes, kind.content_encoding_header()))
    }
}

/// Parameters for storing or merging a zero-copy byte slice directly into the slab pool.
#[derive(Debug, Clone)]
pub struct StoreBytesParams<'a> {
    pub hash: u64,
    pub status: StatusCode,
    pub content_type: Option<http::HeaderValue>,
    pub ttl: Duration,
    pub encoding: Option<&'a str>,
    pub payload: &'a [u8],
    pub is_public: bool,
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
    fn test_negotiate_variant_without_accept_encoding_requires_raw() {
        let all_variants = VARIANT_BIT_ZSTD | VARIANT_BIT_BROTLI | VARIANT_BIT_GZIP;
        let prefs = [
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Brotli,
            CompressionAlgorithm::Gzip,
        ];

        assert_eq!(
            negotiate_variant_kind(all_variants, true, None, &prefs),
            Some(VariantKind::Raw)
        );
        // Without raw variant, missing Accept-Encoding must miss so upstream can populate plain text.
        assert_eq!(
            negotiate_variant_kind(all_variants, false, None, &prefs),
            None
        );
    }

    #[test]
    fn test_negotiate_variant_follows_configured_priority_and_whitelist() {
        let all_variants = VARIANT_BIT_ZSTD | VARIANT_BIT_BROTLI | VARIANT_BIT_GZIP;
        let ae_all = HeaderValue::from_static("gzip, br, zstd");

        // Priority: gzip > br > zstd
        assert_eq!(
            negotiate_variant_kind(
                all_variants,
                true,
                Some(&ae_all),
                &[
                    CompressionAlgorithm::Gzip,
                    CompressionAlgorithm::Brotli,
                    CompressionAlgorithm::Zstd
                ]
            ),
            Some(VariantKind::Gzip)
        );

        // Allowlist strictly filters out zstd even when cached & requested by client.
        let ae_zstd_only = HeaderValue::from_static("zstd");
        let allow_br_gzip = [CompressionAlgorithm::Brotli, CompressionAlgorithm::Gzip];
        assert_eq!(
            negotiate_variant_kind(all_variants, true, Some(&ae_zstd_only), &allow_br_gzip),
            Some(VariantKind::Raw)
        );
        assert_eq!(
            negotiate_variant_kind(all_variants, false, Some(&ae_zstd_only), &allow_br_gzip),
            None
        );

        // Empty preference list disables compressed variants and serves plain text or misses.
        assert_eq!(
            negotiate_variant_kind(all_variants, true, Some(&ae_all), &[]),
            Some(VariantKind::Raw)
        );
        assert_eq!(
            negotiate_variant_kind(all_variants, false, Some(&ae_all), &[]),
            None
        );
    }
}
