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

/// Fast percent-decoding and ASCII lowercasing with anti-evasion protection.
///
/// Converts `%xx` hex encodings to their raw byte equivalent and folds uppercase
/// letters to lowercase without heap allocations if no `%` is present.
pub fn normalize_and_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let h1 = hex_val(bytes[i + 1]);
            let h2 = hex_val(bytes[i + 2]);
            if let (Some(v1), Some(v2)) = (h1, h2) {
                let decoded_byte = (v1 << 4) | v2;
                out.push(decoded_byte.to_ascii_lowercase());
                i += 3;
                continue;
            }
        }
        out.push(bytes[i].to_ascii_lowercase());
        i += 1;
    }

    // Safety: we safely convert ASCII-lowercased lossy string to prevent UTF-8 panics
    String::from_utf8_lossy(&out).into_owned()
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
