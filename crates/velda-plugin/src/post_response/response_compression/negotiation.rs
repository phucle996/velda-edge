//! RFC 9110 Content Negotiation & Accept-Encoding Parser.
//!
//! Provides zero-heap-allocation parsing of client `Accept-Encoding` quality parameters
//! and server preference resolution via stack buffers and bitmasks.

use http::HeaderValue;

/// Integer bitmasks representing client-accepted content encodings.
pub const ENCODING_GZIP: u8 = 1 << 0;
pub const ENCODING_DEFLATE: u8 = 1 << 1;
pub const ENCODING_BROTLI: u8 = 1 << 2;
pub const ENCODING_ZSTD: u8 = 1 << 3;
pub const ENCODING_STAR: u8 = 1 << 4;

/// Supported response payload compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionAlgorithm {
    /// RFC 1952 Gzip compression (`gzip`).
    Gzip,
    /// RFC 1951 Deflate compression (`deflate`).
    Deflate,
    /// RFC 7932 Brotli compression (`br`).
    Brotli,
    /// RFC 8878 Zstandard compression (`zstd`).
    Zstd,
}

impl CompressionAlgorithm {
    /// Parses an algorithm token string into a [`CompressionAlgorithm`].
    pub fn from_token(token: &str) -> Option<Self> {
        let trimmed = token.trim();
        if trimmed.eq_ignore_ascii_case("gzip") || trimmed.eq_ignore_ascii_case("x-gzip") {
            Some(Self::Gzip)
        } else if trimmed.eq_ignore_ascii_case("deflate") {
            Some(Self::Deflate)
        } else if trimmed.eq_ignore_ascii_case("br") || trimmed.eq_ignore_ascii_case("brotli") {
            Some(Self::Brotli)
        } else if trimmed.eq_ignore_ascii_case("zstd") {
            Some(Self::Zstd)
        } else {
            None
        }
    }

    /// Returns the canonical `Content-Encoding` wire header string.
    #[inline(always)]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gzip => "gzip",
            Self::Deflate => "deflate",
            Self::Brotli => "br",
            Self::Zstd => "zstd",
        }
    }

    /// Returns the single-bit mask representation for this algorithm.
    #[inline(always)]
    pub const fn bitmask(&self) -> u8 {
        match self {
            Self::Gzip => ENCODING_GZIP,
            Self::Deflate => ENCODING_DEFLATE,
            Self::Brotli => ENCODING_BROTLI,
            Self::Zstd => ENCODING_ZSTD,
        }
    }
}

/// Represents a client's preference for a specific compression algorithm with quality weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClientPreference {
    /// Advertised algorithm, or `None` if wildcard `*`.
    pub algorithm: Option<CompressionAlgorithm>,
    /// Quality weighting factor `q` (0.0 to 1.0).
    pub q: f32,
}

/// Maximum preferences stored on stack before clamping (avoids heap allocation).
pub const MAX_STACK_PREFERENCES: usize = 8;

/// Parses `Accept-Encoding` into a stack-allocated buffer without heap allocation.
///
/// Returns the number of parsed preferences written to `out`.
#[inline]
pub fn parse_accept_encoding_stack(
    header_val: &str,
    out: &mut [Option<ClientPreference>; MAX_STACK_PREFERENCES],
) -> usize {
    let mut count = 0;
    for part in header_val.split(',') {
        if count >= MAX_STACK_PREFERENCES {
            break;
        }
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
                q = parsed_q.clamp(0.0, 1.0);
            }
        }

        if token == "*" {
            out[count] = Some(ClientPreference { algorithm: None, q });
            count += 1;
        } else if let Some(algo) = CompressionAlgorithm::from_token(token) {
            out[count] = Some(ClientPreference {
                algorithm: Some(algo),
                q,
            });
            count += 1;
        }
    }
    count
}

/// Parses the RFC 9110 `Accept-Encoding` header into a list of weighted preferences.
///
/// Example: `gzip, deflate;q=0.8, br;q=1.0, zstd;q=0.9, *;q=0.1`
pub fn parse_accept_encoding(header_val: &str) -> Vec<ClientPreference> {
    let mut stack_prefs = [None; MAX_STACK_PREFERENCES];
    let count = parse_accept_encoding_stack(header_val, &mut stack_prefs);
    stack_prefs[..count].iter().filter_map(|&p| p).collect()
}

/// Checks if the `identity` (uncompressed) encoding is acceptable to the client (RFC 9110 § 12.5.3).
#[inline]
pub fn is_identity_acceptable(header_val: &str) -> bool {
    let mut prefs = [None; MAX_STACK_PREFERENCES];
    let count = parse_accept_encoding_stack(header_val, &mut prefs);

    let mut identity_q: Option<f32> = None;
    let mut wildcard_q: Option<f32> = None;

    for pref in prefs[..count].iter().flatten() {
        if pref.algorithm.is_none() {
            wildcard_q = Some(pref.q);
        }
    }

    // Direct search for "identity;q=..." token in header string
    for part in header_val.split(',') {
        let mut segments = part.split(';');
        if let Some(_tok) = segments
            .next()
            .filter(|t| t.trim().eq_ignore_ascii_case("identity"))
        {
            let mut q: f32 = 1.0;
            for param in segments {
                let param = param.trim();
                if let Some(parsed) = param
                    .strip_prefix("q=")
                    .and_then(|s| s.trim().parse::<f32>().ok())
                {
                    q = parsed.clamp(0.0, 1.0);
                }
            }
            identity_q = Some(q);
            break;
        }
    }

    if let Some(q) = identity_q {
        q > 0.0
    } else if let Some(q) = wildcard_q {
        q > 0.0
    } else {
        true
    }
}

/// Negotiates the optimal compression algorithm between downstream client requirements
/// and gateway configured preferences (RFC 9110 Section 12.5.3).
///
/// Uses zero heap allocations: parses preferences directly onto a stack-allocated buffer.
/// Returns `None` if the client does not accept compression (`Accept-Encoding: identity` or empty/missing).
pub fn negotiate_algorithm(
    accept_encoding_header: Option<&HeaderValue>,
    server_preferences: &[CompressionAlgorithm],
) -> Option<CompressionAlgorithm> {
    let header_str = accept_encoding_header?.to_str().ok()?;
    let trimmed = header_str.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Fast Path: Direct token matching for common single-encoding or standard modern browser headers
    if trimmed.eq_ignore_ascii_case("gzip") {
        return server_preferences
            .iter()
            .copied()
            .find(|&a| a == CompressionAlgorithm::Gzip);
    }
    if trimmed.eq_ignore_ascii_case("zstd") {
        return server_preferences
            .iter()
            .copied()
            .find(|&a| a == CompressionAlgorithm::Zstd);
    }
    if trimmed.eq_ignore_ascii_case("br") {
        return server_preferences
            .iter()
            .copied()
            .find(|&a| a == CompressionAlgorithm::Brotli);
    }

    // General Path: Zero-allocation stack parsing for weighted q-values
    let mut stack_prefs: [Option<ClientPreference>; MAX_STACK_PREFERENCES] =
        [None; MAX_STACK_PREFERENCES];
    let pref_count = parse_accept_encoding_stack(trimmed, &mut stack_prefs);

    if pref_count == 0 {
        return None;
    }

    let active_prefs = &stack_prefs[..pref_count];

    // Step 1: Check for explicitly forbidden algorithms (q=0)
    let is_forbidden = |algo: CompressionAlgorithm| -> bool {
        active_prefs.iter().any(|slot| {
            if let Some(p) = slot {
                p.algorithm == Some(algo) && p.q == 0.0
            } else {
                false
            }
        })
    };

    // Step 2: Find best matching algorithm with highest client q-factor,
    // tie-broken by server priority order in `server_preferences`.
    let mut best_algo: Option<CompressionAlgorithm> = None;
    let mut best_q: f32 = 0.0;

    for &server_algo in server_preferences {
        if is_forbidden(server_algo) {
            continue;
        }

        let direct_match = active_prefs
            .iter()
            .find_map(|slot| slot.filter(|p| p.algorithm == Some(server_algo)));

        let effective_q = if let Some(pref) = direct_match {
            pref.q
        } else {
            // Check wildcard `*` fallback
            let wildcard = active_prefs
                .iter()
                .find_map(|slot| slot.filter(|p| p.algorithm.is_none()));
            match wildcard {
                Some(w) => w.q,
                None => 0.0,
            }
        };

        if effective_q > best_q {
            best_q = effective_q;
            best_algo = Some(server_algo);
        }
    }

    best_algo
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_parsing() {
        assert_eq!(
            CompressionAlgorithm::from_token("gzip"),
            Some(CompressionAlgorithm::Gzip)
        );
        assert_eq!(
            CompressionAlgorithm::from_token("x-gzip"),
            Some(CompressionAlgorithm::Gzip)
        );
        assert_eq!(
            CompressionAlgorithm::from_token("deflate"),
            Some(CompressionAlgorithm::Deflate)
        );
        assert_eq!(
            CompressionAlgorithm::from_token("br"),
            Some(CompressionAlgorithm::Brotli)
        );
        assert_eq!(
            CompressionAlgorithm::from_token("brotli"),
            Some(CompressionAlgorithm::Brotli)
        );
        assert_eq!(
            CompressionAlgorithm::from_token("zstd"),
            Some(CompressionAlgorithm::Zstd)
        );
        assert_eq!(CompressionAlgorithm::from_token("lz4"), None);
    }

    #[test]
    fn test_parse_accept_encoding_quality_values() {
        let header = "gzip, deflate;q=0.5, br;q=0.9, zstd;q=1.0, *;q=0.1";
        let prefs = parse_accept_encoding(header);

        assert_eq!(prefs.len(), 5);
        assert_eq!(prefs[0].algorithm, Some(CompressionAlgorithm::Gzip));
        assert_eq!(prefs[0].q, 1.0);
        assert_eq!(prefs[1].algorithm, Some(CompressionAlgorithm::Deflate));
        assert_eq!(prefs[1].q, 0.5);
        assert_eq!(prefs[2].algorithm, Some(CompressionAlgorithm::Brotli));
        assert_eq!(prefs[2].q, 0.9);
        assert_eq!(prefs[3].algorithm, Some(CompressionAlgorithm::Zstd));
        assert_eq!(prefs[3].q, 1.0);
        assert_eq!(prefs[4].algorithm, None);
        assert_eq!(prefs[4].q, 0.1);
    }

    #[test]
    fn test_negotiate_algorithm_priority() {
        let server_prefs = [
            CompressionAlgorithm::Zstd,
            CompressionAlgorithm::Brotli,
            CompressionAlgorithm::Gzip,
        ];

        // Equal q: server priority wins (zstd)
        let header = HeaderValue::from_static("gzip, br, zstd");
        assert_eq!(
            negotiate_algorithm(Some(&header), &server_prefs),
            Some(CompressionAlgorithm::Zstd)
        );

        // Higher q wins (br has 1.0, zstd has 0.8)
        let header_q = HeaderValue::from_static("zstd;q=0.8, br;q=1.0, gzip;q=0.5");
        assert_eq!(
            negotiate_algorithm(Some(&header_q), &server_prefs),
            Some(CompressionAlgorithm::Brotli)
        );

        // Explicitly rejected algorithm (q=0)
        let header_reject = HeaderValue::from_static("zstd;q=0, br;q=0.9");
        assert_eq!(
            negotiate_algorithm(Some(&header_reject), &server_prefs),
            Some(CompressionAlgorithm::Brotli)
        );
    }

    #[test]
    fn test_is_identity_acceptable() {
        assert!(is_identity_acceptable("gzip, deflate"));
        assert!(is_identity_acceptable("gzip, identity;q=0.5"));
        assert!(!is_identity_acceptable("gzip, identity;q=0"));
        assert!(!is_identity_acceptable("*;q=0"));
    }
}
