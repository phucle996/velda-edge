//! Client and Consumer Identity Key Resolution for Quota Enforcement.

use http::HeaderName;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};
use velda_core::L7Request;

/// Resolves a 64-bit hashed identifier for an incoming request based on configured key headers.
///
/// Falls back to client IP or socket address if none of the specified headers are present.
pub fn resolve_quota_key(req: &L7Request, key_headers: &[HeaderName]) -> u64 {
    let mut hasher = FxHasher::default();
    let mut matched = false;

    for name in key_headers {
        if let Some(val) = req.headers.get(name) {
            val.as_bytes().hash(&mut hasher);
            matched = true;
        }
    }

    if !matched {
        // Fallback: extract client IP
        if let Some(ip) = req.headers.get("x-forwarded-for") {
            ip.as_bytes().hash(&mut hasher);
        } else if let Some(ip) = req.headers.get("x-real-ip") {
            ip.as_bytes().hash(&mut hasher);
        } else {
            // Static fallback for completely anonymous connections
            b"anonymous".hash(&mut hasher);
        }
    }

    hasher.finish()
}
