//! Keyed CSPRNG SWAR key hashing and hierarchical multi-header key extraction (`key.rs`).
//!
//! ```text
//!   L7Request ──► CompiledKeyExtractor::extract(req, &mut KeyDigestSet)
//!                     │
//!                     ├─► client_ip     ──► [hash(boot_salt, "ip:" + peer)]
//!                     ├─► route         ──► [hash(boot_salt, "rt:" + uri.path())]
//!                     ├─► header        ──► [hash(boot_salt, first_header)]
//!                     ├─► priority      ──► [hash(boot_salt, first_present_source_or_ip)]
//!                     ├─► composite     ──► [hash(boot_salt, src0 | src1 | ...)]
//!                     └─► hierarchical  ──► Cumulative cascade (up to 4 tiers):
//!                                             tier[0] = hash(salt, 0, src0)
//!                                             tier[1] = hash(salt, 1, src0|src1)
//!                                             tier[2] = hash(salt, 2, src0|src1|src2)
//!                                             tier[3] = hash(salt, 3, src0|src1|src2|src3)
//!
//!   HashDoS Defense:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ boot_salt: [u64; 2] seeded via ring::rand::SystemRandom once at startup    │
//!   │ Mix: 64-bit SWAR unaligned loads + 128-bit golden-ratio folded multiply    │
//!   │ Stack Workspace: 512B stack buffer, ZERO heap allocation on hot path       │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use ring::rand::{SecureRandom, SystemRandom};
use velda_core::L7Request;

use crate::pre_upstream::rate_limit::validation::KeyStrategy;

/// Maximum hierarchical tiers evaluated in a single request on stack with zero heap allocations.
pub const MAX_HIERARCHICAL_TIERS: usize = 4;

/// Compact 64-bit keyed key digest + fixed-size printable ASCII tag prefix (`<= 32B`)
/// passed to SoT storage adapters during background synchronization.
#[derive(Clone, Copy, Debug)]
pub struct ExtractedKey {
    pub hash: u64,
    pub tier: u8,
}

/// Zero-allocation stack output containing 1..=4 digests extracted for the current request.
#[derive(Clone, Copy, Debug)]
pub struct KeyDigestSet {
    pub items: [ExtractedKey; MAX_HIERARCHICAL_TIERS],
    pub len: usize,
}

impl KeyDigestSet {
    #[inline]
    pub fn as_slice(&self) -> &[ExtractedKey] {
        &self.items[..self.len]
    }
}

#[derive(Clone, Debug)]
enum KeySourceKind {
    ClientIp,
    Route,
    Header(String),
}

/// Pre-compiled key extractor owning a per-instance CSPRNG salt and normalized source list.
#[derive(Clone, Debug)]
pub struct CompiledKeyExtractor {
    strategy: KeyStrategy,
    sources: Vec<KeySourceKind>,
    salt: [u64; 2],
}

impl CompiledKeyExtractor {
    /// Builds a compiled key extractor with a fresh CSPRNG `boot_salt` against HashDoS.
    pub fn new(strategy: KeyStrategy, raw_headers: &[String]) -> Self {
        let sources = raw_headers
            .iter()
            .filter_map(|raw| {
                let s = raw.trim();
                if s.is_empty() {
                    return None;
                }
                if s.eq_ignore_ascii_case("client_ip") {
                    Some(KeySourceKind::ClientIp)
                } else if s.eq_ignore_ascii_case("route") {
                    Some(KeySourceKind::Route)
                } else {
                    Some(KeySourceKind::Header(s.to_ascii_lowercase()))
                }
            })
            .collect();

        Self {
            strategy,
            sources,
            salt: generate_boot_salt(),
        }
    }

    /// Extracts 1 (or up to 4 cumulative for `Hierarchical`) keyed 64-bit digests into a stack set.
    #[inline]
    pub fn extract(&self, req: &L7Request) -> KeyDigestSet {
        let mut out = KeyDigestSet {
            items: [ExtractedKey { hash: 0, tier: 0 }; MAX_HIERARCHICAL_TIERS],
            len: 0,
        };

        match self.strategy {
            KeyStrategy::ClientIp => {
                let ip = resolve_client_ip(req);
                out.items[0] = ExtractedKey {
                    hash: swar_keyed_hash(self.salt, 0, b"ip", ip.as_bytes()),
                    tier: 0,
                };
                out.len = 1;
            }
            KeyStrategy::Route => {
                let path = req.uri.path().as_bytes();
                out.items[0] = ExtractedKey {
                    hash: swar_keyed_hash(self.salt, 0, b"rt", path),
                    tier: 0,
                };
                out.len = 1;
            }
            KeyStrategy::Header => {
                for src in &self.sources {
                    if let Some(val) = lookup_source(req, src) {
                        out.items[0] = ExtractedKey {
                            hash: swar_keyed_hash(self.salt, 0, b"hd", val.as_bytes()),
                            tier: 0,
                        };
                        out.len = 1;
                        return out;
                    }
                }
                let ip = resolve_client_ip(req);
                out.items[0] = ExtractedKey {
                    hash: swar_keyed_hash(self.salt, 0, b"ip", ip.as_bytes()),
                    tier: 0,
                };
                out.len = 1;
            }
            KeyStrategy::Priority => {
                for src in &self.sources {
                    if let Some(val) = lookup_source(req, src) {
                        out.items[0] = ExtractedKey {
                            hash: swar_keyed_hash(self.salt, 0, b"pr", val.as_bytes()),
                            tier: 0,
                        };
                        out.len = 1;
                        return out;
                    }
                }
                let ip = resolve_client_ip(req);
                out.items[0] = ExtractedKey {
                    hash: swar_keyed_hash(self.salt, 0, b"ip", ip.as_bytes()),
                    tier: 0,
                };
                out.len = 1;
            }
            KeyStrategy::Composite => {
                let mut stack_buf = [0u8; 512];
                let mut pos = 0usize;
                for (i, src) in self.sources.iter().enumerate() {
                    if i > 0 && pos < stack_buf.len() {
                        stack_buf[pos] = b'|';
                        pos += 1;
                    }
                    let val = lookup_source(req, src).unwrap_or("-");
                    let take = val.len().min(stack_buf.len().saturating_sub(pos));
                    stack_buf[pos..pos + take].copy_from_slice(&val.as_bytes()[..take]);
                    pos += take;
                }
                if pos == 0 {
                    let ip = resolve_client_ip(req);
                    let take = ip.len().min(stack_buf.len());
                    stack_buf[..take].copy_from_slice(&ip.as_bytes()[..take]);
                    pos = take;
                }
                out.items[0] = ExtractedKey {
                    hash: swar_keyed_hash(self.salt, 0, b"cm", &stack_buf[..pos]),
                    tier: 0,
                };
                out.len = 1;
            }
            KeyStrategy::Hierarchical => {
                let mut stack_buf = [0u8; 512];
                let mut pos = 0usize;
                let max_tiers = self.sources.len().min(MAX_HIERARCHICAL_TIERS);
                for (tier_idx, src) in self.sources.iter().take(max_tiers).enumerate() {
                    if tier_idx > 0 && pos < stack_buf.len() {
                        stack_buf[pos] = b'|';
                        pos += 1;
                    }
                    let val = lookup_source(req, src).unwrap_or("-");
                    let take = val.len().min(stack_buf.len().saturating_sub(pos));
                    stack_buf[pos..pos + take].copy_from_slice(&val.as_bytes()[..take]);
                    pos += take;

                    out.items[tier_idx] = ExtractedKey {
                        hash: swar_keyed_hash(self.salt, tier_idx as u64, b"hr", &stack_buf[..pos]),
                        tier: tier_idx as u8,
                    };
                    out.len = tier_idx + 1;
                }
                if out.len == 0 {
                    let ip = resolve_client_ip(req);
                    out.items[0] = ExtractedKey {
                        hash: swar_keyed_hash(self.salt, 0, b"hr", ip.as_bytes()),
                        tier: 0,
                    };
                    out.len = 1;
                }
            }
        }

        out
    }
}

#[inline]
fn lookup_source<'a>(req: &'a L7Request, src: &KeySourceKind) -> Option<&'a str> {
    match src {
        KeySourceKind::ClientIp => Some(resolve_client_ip(req)),
        KeySourceKind::Route => Some(req.uri.path()),
        KeySourceKind::Header(name) => req
            .headers
            .get(name.as_str())
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty()),
    }
}

#[inline]
fn resolve_client_ip(req: &L7Request) -> &str {
    if let Some(xff) = req
        .headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        && let Some(first) = xff
            .split(',')
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    {
        return first;
    }
    if let Some(real_ip) = req
        .headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return real_ip;
    }
    "unknown-client"
}

fn generate_boot_salt() -> [u64; 2] {
    let mut raw = [0u8; 16];
    if SystemRandom::new().fill(&mut raw).is_err() {
        return [0x9e37_79b9_7f4a_7c15, 0xbf58_476d_1ce4_e5b9];
    }
    let a = u64::from_le_bytes(raw[0..8].try_into().unwrap_or([1; 8]));
    let b = u64::from_le_bytes(raw[8..16].try_into().unwrap_or([2; 8]));
    [a | 1, b | 1]
}

/// 64-bit SWAR keyed permutation mixing domain prefix, tier ordinal, and variable payload
/// without heap allocation.
#[inline]
pub fn swar_keyed_hash(salt: [u64; 2], tier: u64, tag: &[u8; 2], data: &[u8]) -> u64 {
    const P1: u64 = 0x9e37_79b1_85eb_ca87;
    const P2: u64 = 0xc2b2_ae3d_27d4_eb4f;

    let mut acc = salt[0] ^ ((tier + 1).wrapping_mul(P1)) ^ u64::from(u16::from_le_bytes(*tag));
    let (chunks, tail) = data.as_chunks::<8>();
    for c in chunks {
        let word = u64::from_le_bytes(*c);
        acc = mix128(acc ^ word, salt[1] ^ P2);
    }
    let mut tail_word = (data.len() as u64) << 56;
    for (idx, &b) in tail.iter().enumerate() {
        tail_word |= u64::from(b) << (idx * 8);
    }
    acc = mix128(acc ^ tail_word, salt[0] ^ salt[1]);
    // Ensure 0 is reserved as empty slot sentinel in ShardArena.
    let h = acc ^ (acc >> 31);
    if h == 0 { 1 } else { h }
}

#[inline(always)]
fn mix128(a: u64, b: u64) -> u64 {
    let prod = u128::from(a).wrapping_mul(u128::from(b));
    (prod as u64) ^ ((prod >> 64) as u64)
}

#[cfg(test)]
mod tests {
    use http::{Method, Uri, Version};
    use velda_core::{Body, HeaderWorkspace, L7Request};

    use super::*;

    fn req_with_headers(headers: &[(&str, &str)], path: &str) -> L7Request {
        use http::header::{HeaderName, HeaderValue};
        let mut ws = HeaderWorkspace::empty(8);
        for &(k, v) in headers {
            ws.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        L7Request::new(
            Method::GET,
            path.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            ws,
            Body::Empty,
        )
    }

    #[test]
    fn test_hierarchical_multi_header_cascading_tiers() {
        let ext = CompiledKeyExtractor::new(
            KeyStrategy::Hierarchical,
            &["x-tenant-id".into(), "x-api-key".into(), "client_ip".into()],
        );
        let r1 = req_with_headers(
            &[
                ("x-tenant-id", "acme"),
                ("x-api-key", "k1"),
                ("x-forwarded-for", "1.1.1.1"),
            ],
            "/v1/items",
        );
        let r2 = req_with_headers(
            &[
                ("x-tenant-id", "acme"),
                ("x-api-key", "k2"),
                ("x-forwarded-for", "2.2.2.2"),
            ],
            "/v1/items",
        );

        let d1 = ext.extract(&r1);
        let d2 = ext.extract(&r2);
        assert_eq!(d1.len, 3);
        assert_eq!(d2.len, 3);
        // Same tenant -> Tier 0 digest matches even when API key and IP rotate!
        assert_eq!(d1.items[0].hash, d2.items[0].hash);
        // Different API key -> Tier 1 and Tier 2 differ!
        assert_ne!(d1.items[1].hash, d2.items[1].hash);
        assert_ne!(d1.items[2].hash, d2.items[2].hash);
    }

    #[test]
    fn test_composite_distinguishes_tuples() {
        let ext = CompiledKeyExtractor::new(
            KeyStrategy::Composite,
            &["x-tenant-id".into(), "x-api-key".into()],
        );
        let r1 = req_with_headers(&[("x-tenant-id", "a"), ("x-api-key", "b")], "/");
        let r2 = req_with_headers(&[("x-tenant-id", "a"), ("x-api-key", "c")], "/");
        assert_ne!(
            ext.extract(&r1).items[0].hash,
            ext.extract(&r2).items[0].hash
        );
    }
}
