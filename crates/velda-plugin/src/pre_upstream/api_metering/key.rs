//! Account & Tier Header Resolution for API Metering.

use http::HeaderName;
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};
use velda_core::L7Request;

/// Resolves a 64-bit hashed account identifier and plaintext tier name from incoming request headers.
pub fn resolve_account_and_tier(
    req: &L7Request,
    account_headers: &[HeaderName],
    tier_header: &HeaderName,
    default_tier: &str,
) -> (u64, String) {
    let mut hasher = FxHasher::default();
    let mut matched = false;

    for name in account_headers {
        if let Some(val) = req.headers.get(name) {
            val.as_bytes().hash(&mut hasher);
            matched = true;
            break;
        }
    }

    if !matched {
        if let Some(ip) = req.headers.get("x-forwarded-for") {
            ip.as_bytes().hash(&mut hasher);
        } else {
            b"anonymous_tenant".hash(&mut hasher);
        }
    }

    let tier_str = req
        .headers
        .get(tier_header)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_else(|| default_tier.to_ascii_lowercase());

    (hasher.finish(), tier_str)
}
