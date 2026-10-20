//! Zero-Regex Fast Byte Scanning and URL Normalization Engine for `waf_lite`.
//!
//! Provides in-place percent-decoding, case-folding, and substring pattern matching
//! against known L7 attack signatures in single-digit nanoseconds.

/// Classification of detected threat vector for observability and forensics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreatVector {
    PathTraversal,
    SqlInjection,
    CrossSiteScripting,
    RemoteCodeExecution,
    MaliciousScanner,
    CustomPattern,
}

impl ThreatVector {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PathTraversal => "path_traversal",
            Self::SqlInjection => "sql_injection",
            Self::CrossSiteScripting => "xss",
            Self::RemoteCodeExecution => "rce",
            Self::MaliciousScanner => "malicious_scanner",
            Self::CustomPattern => "custom_pattern",
        }
    }
}

use std::sync::Arc;

use aho_corasick::{AhoCorasick, MatchKind};

use crate::error::PluginError;

/// High-performance SIMD multi-pattern Aho-Corasick matching engine for WAF Lite.
#[derive(Clone, Debug)]
pub struct WafMatcher {
    pattern_ac: Arc<AhoCorasick>,
    pattern_meta: Arc<Vec<(&'static str, ThreatVector)>>,
    ua_ac: Arc<AhoCorasick>,
    ua_names: Arc<Vec<String>>,
}

impl WafMatcher {
    /// Compiles built-in attack signatures, custom patterns, and user-agents into SIMD-accelerated automata.
    pub fn new(custom_patterns: &[String], custom_uas: &[String]) -> Result<Self, PluginError> {
        let mut patterns = Vec::new();
        let mut meta = Vec::new();

        for (pat, vector) in default_attack_signatures() {
            patterns.push(pat.to_string());
            meta.push((pat, vector));
        }

        for pat in custom_patterns {
            patterns.push(pat.clone());
            meta.push(("", ThreatVector::CustomPattern));
        }

        let pattern_ac = AhoCorasick::builder()
            .match_kind(MatchKind::Standard)
            .build(&patterns)
            .map_err(|e| {
                PluginError::Config(format!("Failed to compile WAF pattern automaton: {e}"))
            })?;

        let mut uas = Vec::new();
        let mut ua_names = Vec::new();
        for &ua in &default_malicious_user_agents() {
            uas.push(ua.to_string());
            ua_names.push(ua.to_string());
        }
        for ua in custom_uas {
            uas.push(ua.clone());
            ua_names.push(ua.clone());
        }

        let ua_ac = AhoCorasick::builder()
            .match_kind(MatchKind::Standard)
            .build(&uas)
            .map_err(|e| {
                PluginError::Config(format!("Failed to compile WAF User-Agent automaton: {e}"))
            })?;

        Ok(Self {
            pattern_ac: Arc::new(pattern_ac),
            pattern_meta: Arc::new(meta),
            ua_ac: Arc::new(ua_ac),
            ua_names: Arc::new(ua_names),
        })
    }

    /// Single-pass SIMD pattern matching across all configured signatures.
    #[inline(always)]
    pub fn check_patterns(&self, normalized_input: &str) -> Option<(&'static str, ThreatVector)> {
        if let Some(m) = self.pattern_ac.find(normalized_input.as_bytes()) {
            let (pat, vector) = self.pattern_meta[m.pattern().as_usize()];
            Some((pat, vector))
        } else {
            None
        }
    }

    /// Single-pass SIMD matching against known malicious scanner User-Agents.
    #[inline(always)]
    pub fn check_user_agent(&self, normalized_ua: &str) -> Option<&str> {
        if let Some(m) = self.ua_ac.find(normalized_ua.as_bytes()) {
            Some(&self.ua_names[m.pattern().as_usize()])
        } else {
            None
        }
    }
}

/// In-place byte decoding and ASCII lowercasing helper.
#[inline(always)]
fn decode_bytes_into(bytes: &[u8], out: &mut [u8]) -> usize {
    let mut i = 0;
    let mut o = 0;
    while i < bytes.len() && o < out.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h1 = hex_val(bytes[i + 1]);
            let h2 = hex_val(bytes[i + 2]);
            if let (Some(v1), Some(v2)) = (h1, h2) {
                let decoded_byte = (v1 << 4) | v2;
                out[o] = decoded_byte.to_ascii_lowercase();
                i += 3;
                o += 1;
                continue;
            }
        }
        out[o] = bytes[i].to_ascii_lowercase();
        i += 1;
        o += 1;
    }
    o
}

/// Normalizes and decodes input with ZERO heap allocations for inputs <= 1024 bytes.
///
/// Fast-paths lowercase ASCII without `%` directly to avoid copying any bytes.
#[inline]
pub fn with_normalized<R>(input: &str, f: impl FnOnce(&str) -> R) -> R {
    let bytes = input.as_bytes();
    let mut needs_decode = false;
    for &b in bytes {
        if b == b'%' || b.is_ascii_uppercase() {
            needs_decode = true;
            break;
        }
    }
    if !needs_decode {
        return f(input);
    }

    if bytes.len() <= 1024 {
        let mut stack_buf = [0u8; 1024];
        let len = decode_bytes_into(bytes, &mut stack_buf);
        let s = match std::str::from_utf8(&stack_buf[..len]) {
            Ok(valid) => valid,
            Err(_) => return f(&String::from_utf8_lossy(&stack_buf[..len])),
        };
        f(s)
    } else {
        let mut heap_buf = vec![0u8; bytes.len()];
        let len = decode_bytes_into(bytes, &mut heap_buf);
        let s = match std::str::from_utf8(&heap_buf[..len]) {
            Ok(valid) => valid,
            Err(_) => return f(&String::from_utf8_lossy(&heap_buf[..len])),
        };
        f(s)
    }
}

/// Fast percent-decoding and ASCII lowercasing with anti-evasion protection.
pub fn normalize_and_decode(input: &str) -> String {
    with_normalized(input, |s| s.to_string())
}

#[inline(always)]
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Default built-in attack signatures for L7 edge protection.
pub fn default_attack_signatures() -> Vec<(&'static str, ThreatVector)> {
    vec![
        // 1. Path Traversal
        ("../", ThreatVector::PathTraversal),
        ("..\\", ThreatVector::PathTraversal),
        ("/etc/passwd", ThreatVector::PathTraversal),
        ("/etc/shadow", ThreatVector::PathTraversal),
        ("/proc/self", ThreatVector::PathTraversal),
        ("boot.ini", ThreatVector::PathTraversal),
        (".env", ThreatVector::PathTraversal),
        (".git/", ThreatVector::PathTraversal),
        // 2. SQL Injection
        ("' or '1'='1", ThreatVector::SqlInjection),
        ("\" or \"1\"=\"1", ThreatVector::SqlInjection),
        ("' or 1=1", ThreatVector::SqlInjection),
        ("\" or 1=1", ThreatVector::SqlInjection),
        ("' or ''='", ThreatVector::SqlInjection),
        ("union select", ThreatVector::SqlInjection),
        ("union all select", ThreatVector::SqlInjection),
        ("; drop table", ThreatVector::SqlInjection),
        ("; exec(", ThreatVector::SqlInjection),
        ("; shutdown", ThreatVector::SqlInjection),
        ("benchmark(", ThreatVector::SqlInjection),
        ("waitfor delay", ThreatVector::SqlInjection),
        // 3. Cross-Site Scripting (XSS)
        ("<script", ThreatVector::CrossSiteScripting),
        ("</script", ThreatVector::CrossSiteScripting),
        ("javascript:", ThreatVector::CrossSiteScripting),
        ("vbscript:", ThreatVector::CrossSiteScripting),
        ("onerror=", ThreatVector::CrossSiteScripting),
        ("onload=", ThreatVector::CrossSiteScripting),
        ("onclick=", ThreatVector::CrossSiteScripting),
        ("<svg/onload", ThreatVector::CrossSiteScripting),
        ("<img src=x", ThreatVector::CrossSiteScripting),
        // 4. Remote Code Execution (RCE) / JNDI Log4j
        ("${jndi:", ThreatVector::RemoteCodeExecution),
        ("$(curl", ThreatVector::RemoteCodeExecution),
        ("$(wget", ThreatVector::RemoteCodeExecution),
        ("; /bin/sh", ThreatVector::RemoteCodeExecution),
        ("; /bin/bash", ThreatVector::RemoteCodeExecution),
        ("| /bin/sh", ThreatVector::RemoteCodeExecution),
        ("| /bin/bash", ThreatVector::RemoteCodeExecution),
    ]
}

/// Default known malicious vulnerability scanners and botnet User-Agents.
pub fn default_malicious_user_agents() -> Vec<&'static str> {
    vec![
        "sqlmap",
        "nikto",
        "acunetix",
        "masscan",
        "dirbuster",
        "gobuster",
        "wpscan",
        "nuclei",
        "nmap",
        "zgrab",
    ]
}

/// Scans a normalized string against a list of attack signatures.
#[inline]
pub fn check_patterns<'a>(
    normalized_input: &str,
    patterns: &'a [(&'static str, ThreatVector)],
    custom_patterns: &'a [String],
) -> Option<(&'a str, ThreatVector)> {
    for (pat, vector) in patterns {
        if normalized_input.contains(pat) {
            return Some((pat, *vector));
        }
    }
    for pat in custom_patterns {
        if normalized_input.contains(pat.as_str()) {
            return Some((pat.as_str(), ThreatVector::CustomPattern));
        }
    }
    None
}

/// Checks if a normalized User-Agent matches any known malicious scanner tool.
#[inline]
pub fn check_user_agent<'a>(
    normalized_ua: &str,
    builtins: &'a [&'static str],
    custom: &'a [String],
) -> Option<&'a str> {
    for &ua in builtins {
        if normalized_ua.contains(ua) {
            return Some(ua);
        }
    }
    for ua in custom {
        if normalized_ua.contains(ua.as_str()) {
            return Some(ua.as_str());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_percent_decode_and_lowercase() {
        assert_eq!(
            normalize_and_decode("%2e%2e%2fetc%2fPasswd"),
            "../etc/passwd"
        );
        assert_eq!(
            normalize_and_decode("UNION%20SELECT%20*%20FROM"),
            "union select * from"
        );
        assert_eq!(
            normalize_and_decode("%3Cscript%3Ealert(1)%3C%2Fscript%3E"),
            "<script>alert(1)</script>"
        );
        assert_eq!(
            normalize_and_decode("normal_path/resource"),
            "normal_path/resource"
        );
    }

    #[test]
    fn test_path_traversal_detection() {
        let signatures = default_attack_signatures();
        let custom = vec![];

        let raw = "/api/v1/download?file=..%2f..%2fetc%2fpasswd";
        let normalized = normalize_and_decode(raw);
        let threat = check_patterns(&normalized, &signatures, &custom);

        assert!(threat.is_some());
        let (pattern, vector) = threat.unwrap();
        assert_eq!(pattern, "../");
        assert_eq!(vector, ThreatVector::PathTraversal);
    }

    #[test]
    fn test_sql_injection_detection() {
        let signatures = default_attack_signatures();
        let custom = vec![];

        let raw = "/api/v1/users?id=1%27%20OR%201=1";
        let normalized = normalize_and_decode(raw);
        let threat = check_patterns(&normalized, &signatures, &custom);

        assert!(threat.is_some());
        let (pattern, vector) = threat.unwrap();
        assert_eq!(pattern, "' or 1=1");
        assert_eq!(vector, ThreatVector::SqlInjection);
    }

    #[test]
    fn test_xss_detection() {
        let signatures = default_attack_signatures();
        let custom = vec![];

        let raw = "/search?q=%3Cscript%3Ealert(document.cookie)%3C/script%3E";
        let normalized = normalize_and_decode(raw);
        let threat = check_patterns(&normalized, &signatures, &custom);

        assert!(threat.is_some());
        let (pattern, vector) = threat.unwrap();
        assert_eq!(pattern, "<script");
        assert_eq!(vector, ThreatVector::CrossSiteScripting);
    }

    #[test]
    fn test_malicious_user_agent_detection() {
        let builtins = default_malicious_user_agents();
        let custom = vec!["badbot".to_string()];

        let ua1 = "sqlmap/1.6#stable (https://sqlmap.org)";
        let norm1 = normalize_and_decode(ua1);
        assert_eq!(check_user_agent(&norm1, &builtins, &custom), Some("sqlmap"));

        let ua2 = "Mozilla/5.0 (compatible; BadBot/2.0)";
        let norm2 = normalize_and_decode(ua2);
        assert_eq!(check_user_agent(&norm2, &builtins, &custom), Some("badbot"));

        let ua_clean = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";
        let norm_clean = normalize_and_decode(ua_clean);
        assert!(check_user_agent(&norm_clean, &builtins, &custom).is_none());
    }
}
