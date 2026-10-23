//! Built-in attack signatures and malicious scanner dictionary for `waf_lite`.
//!
//! Provides the canonical static knowledge base of known L7 attack patterns
//! (Path Traversal, SQL Injection, XSS, RCE, Log4j, and Framework CVEs) and
//! malicious vulnerability scanners. Zero heap allocations, zero runtime wrappers.

use http::header::HeaderValue;

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
    /// Canonical string identifier for metrics and logging.
    #[inline(always)]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::PathTraversal => "path_traversal",
            Self::SqlInjection => "sql_injection",
            Self::CrossSiteScripting => "xss",
            Self::RemoteCodeExecution => "rce",
            Self::MaliciousScanner => "malicious_scanner",
            Self::CustomPattern => "custom_pattern",
        }
    }

    /// Pre-baked static header value for zero-allocation injection in detect mode.
    #[inline(always)]
    pub const fn as_header_value(&self) -> HeaderValue {
        match self {
            Self::PathTraversal => HeaderValue::from_static("path_traversal"),
            Self::SqlInjection => HeaderValue::from_static("sql_injection"),
            Self::CrossSiteScripting => HeaderValue::from_static("xss"),
            Self::RemoteCodeExecution => HeaderValue::from_static("rce"),
            Self::MaliciousScanner => HeaderValue::from_static("malicious_scanner"),
            Self::CustomPattern => HeaderValue::from_static("custom_pattern"),
        }
    }
}

/// Canonical built-in attack signatures mapped directly to typed [`ThreatVector`].
/// Every entry is lowercase ASCII and verified free of internal substring redundancy.
pub const BUILTIN_ATTACK_SIGNATURES: &[(&str, ThreatVector)] = &[
    // ========================================================================
    // 1. Path Traversal & Sensitive System File Probes (20)
    // ========================================================================
    ("../", ThreatVector::PathTraversal),
    ("..\\", ThreatVector::PathTraversal),
    ("..;/", ThreatVector::PathTraversal), // Tomcat path traversal (CVE-2018-1305, CVE-2020-1938)
    ("..;\\", ThreatVector::PathTraversal),
    ("/etc/passwd", ThreatVector::PathTraversal),
    ("/etc/shadow", ThreatVector::PathTraversal),
    ("/etc/hosts", ThreatVector::PathTraversal),
    ("/etc/issue", ThreatVector::PathTraversal),
    ("/proc/self", ThreatVector::PathTraversal),
    ("/proc/version", ThreatVector::PathTraversal),
    ("boot.ini", ThreatVector::PathTraversal),
    ("win.ini", ThreatVector::PathTraversal),
    ("system.ini", ThreatVector::PathTraversal),
    (".env", ThreatVector::PathTraversal),
    (".git/", ThreatVector::PathTraversal),
    (".svn/", ThreatVector::PathTraversal),
    (".htaccess", ThreatVector::PathTraversal),
    ("web.config", ThreatVector::PathTraversal),
    ("id_rsa", ThreatVector::PathTraversal),
    ("id_ed25519", ThreatVector::PathTraversal),
    // ========================================================================
    // 2. SQL Injection: Standard, Parenthesized, Blind Time-Based & DB-RCE (26)
    // ========================================================================
    ("' or '1'='1", ThreatVector::SqlInjection),
    ("\" or \"1\"=\"1", ThreatVector::SqlInjection),
    ("' or 1=1", ThreatVector::SqlInjection),
    ("\" or 1=1", ThreatVector::SqlInjection),
    ("' or ''='", ThreatVector::SqlInjection),
    ("union select", ThreatVector::SqlInjection),
    ("union all select", ThreatVector::SqlInjection),
    ("union distinct select", ThreatVector::SqlInjection),
    ("union(", ThreatVector::SqlInjection),
    ("union all(", ThreatVector::SqlInjection),
    ("union distinct(", ThreatVector::SqlInjection),
    ("; drop table", ThreatVector::SqlInjection),
    ("; drop database", ThreatVector::SqlInjection),
    ("; exec(", ThreatVector::SqlInjection),
    ("; execute(", ThreatVector::SqlInjection),
    ("; shutdown", ThreatVector::SqlInjection),
    ("benchmark(", ThreatVector::SqlInjection),
    ("waitfor delay", ThreatVector::SqlInjection),
    ("sleep(", ThreatVector::SqlInjection), // Covers sleep() and pg_sleep()
    ("extractvalue(", ThreatVector::SqlInjection),
    ("updatexml(", ThreatVector::SqlInjection),
    ("xp_cmdshell", ThreatVector::SqlInjection),
    ("into outfile", ThreatVector::SqlInjection),
    ("into dumpfile", ThreatVector::SqlInjection),
    ("load_file(", ThreatVector::SqlInjection),
    ("/*!", ThreatVector::SqlInjection), // MySQL versioned comment execution
    // ========================================================================
    // 3. Cross-Site Scripting (XSS) (18)
    // ========================================================================
    ("<script", ThreatVector::CrossSiteScripting),
    ("</script", ThreatVector::CrossSiteScripting),
    ("javascript:", ThreatVector::CrossSiteScripting),
    ("vbscript:", ThreatVector::CrossSiteScripting),
    ("data:text/html", ThreatVector::CrossSiteScripting),
    ("onerror=", ThreatVector::CrossSiteScripting),
    ("onload=", ThreatVector::CrossSiteScripting),
    ("onclick=", ThreatVector::CrossSiteScripting),
    ("onmouseover=", ThreatVector::CrossSiteScripting),
    ("onfocus=", ThreatVector::CrossSiteScripting),
    ("onblur=", ThreatVector::CrossSiteScripting),
    ("<svg/onload", ThreatVector::CrossSiteScripting),
    ("<svg onload", ThreatVector::CrossSiteScripting),
    ("<img src=x", ThreatVector::CrossSiteScripting),
    ("<body onload", ThreatVector::CrossSiteScripting),
    ("<iframe src=", ThreatVector::CrossSiteScripting),
    ("document.cookie", ThreatVector::CrossSiteScripting),
    ("document.location", ThreatVector::CrossSiteScripting),
    // ========================================================================
    // 4. Remote Code Execution (RCE), Log4j & Framework CVEs (55)
    // ========================================================================
    ("${jndi:", ThreatVector::RemoteCodeExecution),
    ("}ndi:", ThreatVector::RemoteCodeExecution), // Single-level Log4j lookup evasion (${env:NaN:-j}ndi:)
    ("jndi:ldap", ThreatVector::RemoteCodeExecution),
    ("jndi:rmi", ThreatVector::RemoteCodeExecution),
    ("jndi:dns", ThreatVector::RemoteCodeExecution),
    ("jndi:iiop", ThreatVector::RemoteCodeExecution),
    ("jndi:nis", ThreatVector::RemoteCodeExecution),
    ("${${", ThreatVector::RemoteCodeExecution),
    ("${lower:", ThreatVector::RemoteCodeExecution),
    ("${upper:", ThreatVector::RemoteCodeExecution),
    ("${::-", ThreatVector::RemoteCodeExecution),
    ("${env:", ThreatVector::RemoteCodeExecution),
    ("${sys:", ThreatVector::RemoteCodeExecution),
    ("${java:", ThreatVector::RemoteCodeExecution),
    ("/bin/sh", ThreatVector::RemoteCodeExecution),
    ("/bin/bash", ThreatVector::RemoteCodeExecution),
    ("/bin/zsh", ThreatVector::RemoteCodeExecution),
    (";bash", ThreatVector::RemoteCodeExecution),
    ("; bash", ThreatVector::RemoteCodeExecution),
    ("|bash", ThreatVector::RemoteCodeExecution),
    ("| bash", ThreatVector::RemoteCodeExecution),
    ("&&bash", ThreatVector::RemoteCodeExecution),
    ("&& bash", ThreatVector::RemoteCodeExecution),
    (";zsh", ThreatVector::RemoteCodeExecution),
    ("; zsh", ThreatVector::RemoteCodeExecution),
    ("|zsh", ThreatVector::RemoteCodeExecution),
    ("| zsh", ThreatVector::RemoteCodeExecution),
    ("&&sh", ThreatVector::RemoteCodeExecution),
    ("&& sh", ThreatVector::RemoteCodeExecution),
    (";sh -", ThreatVector::RemoteCodeExecution),
    ("|sh -", ThreatVector::RemoteCodeExecution),
    ("$(curl", ThreatVector::RemoteCodeExecution),
    ("$(wget", ThreatVector::RemoteCodeExecution),
    ("$(id", ThreatVector::RemoteCodeExecution),
    ("$(whoami", ThreatVector::RemoteCodeExecution),
    ("$(cat", ThreatVector::RemoteCodeExecution),
    ("$(uname", ThreatVector::RemoteCodeExecution),
    ("`id`", ThreatVector::RemoteCodeExecution),
    ("`whoami`", ThreatVector::RemoteCodeExecution),
    ("`curl ", ThreatVector::RemoteCodeExecution),
    ("`wget ", ThreatVector::RemoteCodeExecution),
    (";whoami", ThreatVector::RemoteCodeExecution),
    ("; whoami", ThreatVector::RemoteCodeExecution),
    ("|whoami", ThreatVector::RemoteCodeExecution),
    ("| whoami", ThreatVector::RemoteCodeExecution),
    (";cat ", ThreatVector::RemoteCodeExecution),
    ("; cat ", ThreatVector::RemoteCodeExecution),
    ("|cat ", ThreatVector::RemoteCodeExecution),
    ("| cat ", ThreatVector::RemoteCodeExecution),
    ("${ifs}", ThreatVector::RemoteCodeExecution),
    ("$ifs", ThreatVector::RemoteCodeExecution),
    ("/dev/tcp/", ThreatVector::RemoteCodeExecution),
    ("/dev/udp/", ThreatVector::RemoteCodeExecution),
    ("sh -i", ThreatVector::RemoteCodeExecution), // Covers sh -i, bash -i, zsh -i
    ("nc -e", ThreatVector::RemoteCodeExecution),
    ("nc.exe", ThreatVector::RemoteCodeExecution),
    ("powershell", ThreatVector::RemoteCodeExecution), // Covers powershell and powershell.exe
    ("cmd.exe", ThreatVector::RemoteCodeExecution),    // Covers cmd.exe and cmd.exe /c
    ("certutil -urlcache", ThreatVector::RemoteCodeExecution),
    ("base64 -d", ThreatVector::RemoteCodeExecution),
    ("base64 --decode", ThreatVector::RemoteCodeExecution),
    ("<?php", ThreatVector::RemoteCodeExecution),
    ("eval(", ThreatVector::RemoteCodeExecution),
    ("passthru(", ThreatVector::RemoteCodeExecution),
    ("shell_exec(", ThreatVector::RemoteCodeExecution),
    ("system(", ThreatVector::RemoteCodeExecution),
    (
        "class.module.classloader",
        ThreatVector::RemoteCodeExecution,
    ), // Spring4Shell CVE-2022-22965
    ("#_memberaccess", ThreatVector::RemoteCodeExecution), // Apache Struts OGNL RCE
    ("#context[", ThreatVector::RemoteCodeExecution),
];

/// Canonical built-in malicious vulnerability scanner User-Agents (15).
pub const BUILTIN_MALICIOUS_USER_AGENTS: &[&str] = &[
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
    "metasploit",
    "nessus",
    "burpcollaborator",
    "openvas",
    "shodan",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dictionary_parity_with_velda_sync_and_zero_redundancy() {
        // 1. Verify exact 1:1 parity with velda_sync::BUILTIN_ATTACK_PATTERNS
        let dict_pats: Vec<&str> = BUILTIN_ATTACK_SIGNATURES.iter().map(|&(p, _)| p).collect();
        assert_eq!(
            dict_pats.as_slice(),
            velda_sync::BUILTIN_ATTACK_PATTERNS,
            "dictionary::BUILTIN_ATTACK_SIGNATURES must match velda_sync::BUILTIN_ATTACK_PATTERNS"
        );
        assert_eq!(
            BUILTIN_MALICIOUS_USER_AGENTS,
            velda_sync::BUILTIN_MALICIOUS_USER_AGENTS,
            "dictionary::BUILTIN_MALICIOUS_USER_AGENTS must match velda_sync::BUILTIN_MALICIOUS_USER_AGENTS"
        );

        // 2. Verify every pattern is non-empty, lowercase ASCII, and no pattern is a substring of another
        for (i, &(pat_i, _)) in BUILTIN_ATTACK_SIGNATURES.iter().enumerate() {
            assert!(!pat_i.is_empty());
            assert_eq!(pat_i, pat_i.to_ascii_lowercase());
            for (j, &(pat_j, _)) in BUILTIN_ATTACK_SIGNATURES.iter().enumerate() {
                if i != j {
                    assert!(
                        !pat_j.contains(pat_i),
                        "Built-in pattern '{pat_j}' is a redundant superstring of '{pat_i}'"
                    );
                }
            }
        }

        // 3. Verify every malicious User-Agent is non-empty, lowercase, and free of substring redundancy
        for (i, &ua_i) in BUILTIN_MALICIOUS_USER_AGENTS.iter().enumerate() {
            assert!(!ua_i.is_empty());
            assert_eq!(ua_i, ua_i.to_ascii_lowercase());
            for (j, &ua_j) in BUILTIN_MALICIOUS_USER_AGENTS.iter().enumerate() {
                if i != j {
                    assert!(
                        !ua_j.contains(ua_i),
                        "Built-in UA '{ua_j}' is a redundant superstring of '{ua_i}'"
                    );
                }
            }
        }
    }
}
