//! WAF Lite Configuration Schema & Semantic Validation (`phase: "pre_route"`).
//!
//! Enforces strict declarative contract for the Web Application Firewall Lite plugin.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Explicit Mode**: `mode` must be explicitly declared as `"block"` or `"detect"`.
//! - **Valid Scope**: At least one inspection vector must be active (`check_path`, `check_query`, `check_user_agent`, or `check_headers`).
//! - **Valid Header Names**: Every header in `check_headers` must conform to RFC 9110 token grammar.
//! - **Non-Empty Custom Patterns**: Custom attack and user-agent patterns cannot be empty or whitespace.
//! - **Automatic Deduplication ("Khử trùng")**: Custom patterns and user agents covered by built-in signatures
//!   or duplicated within the configuration are automatically collapsed into unique single entries and persisted into `plugins.bin`.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Canonical built-in attack pattern substrings for L7 perimeter firewall deduplication.
/// Every entry is lowercase ASCII and free of substring redundancy.
pub const BUILTIN_ATTACK_PATTERNS: &[&str] = &[
    // 1. Path Traversal & Sensitive System File Probes (20)
    "../",
    "..\\",
    "..;/",
    "..;\\",
    "/etc/passwd",
    "/etc/shadow",
    "/etc/hosts",
    "/etc/issue",
    "/proc/self",
    "/proc/version",
    "boot.ini",
    "win.ini",
    "system.ini",
    ".env",
    ".git/",
    ".svn/",
    ".htaccess",
    "web.config",
    "id_rsa",
    "id_ed25519",
    // 2. SQL Injection: Standard, Parenthesized, Blind Time-Based & DB-RCE (26)
    "' or '1'='1",
    "\" or \"1\"=\"1",
    "' or 1=1",
    "\" or 1=1",
    "' or ''='",
    "union select",
    "union all select",
    "union distinct select",
    "union(",
    "union all(",
    "union distinct(",
    "; drop table",
    "; drop database",
    "; exec(",
    "; execute(",
    "; shutdown",
    "benchmark(",
    "waitfor delay",
    "sleep(",
    "extractvalue(",
    "updatexml(",
    "xp_cmdshell",
    "into outfile",
    "into dumpfile",
    "load_file(",
    "/*!",
    // 3. Cross-Site Scripting (XSS) (18)
    "<script",
    "</script",
    "javascript:",
    "vbscript:",
    "data:text/html",
    "onerror=",
    "onload=",
    "onclick=",
    "onmouseover=",
    "onfocus=",
    "onblur=",
    "<svg/onload",
    "<svg onload",
    "<img src=x",
    "<body onload",
    "<iframe src=",
    "document.cookie",
    "document.location",
    // 4. Remote Code Execution (RCE), Log4j & Framework CVEs (55)
    "${jndi:",
    "}ndi:",
    "jndi:ldap",
    "jndi:rmi",
    "jndi:dns",
    "jndi:iiop",
    "jndi:nis",
    "${${",
    "${lower:",
    "${upper:",
    "${::-",
    "${env:",
    "${sys:",
    "${java:",
    "/bin/sh",
    "/bin/bash",
    "/bin/zsh",
    ";bash",
    "; bash",
    "|bash",
    "| bash",
    "&&bash",
    "&& bash",
    ";zsh",
    "; zsh",
    "|zsh",
    "| zsh",
    "&&sh",
    "&& sh",
    ";sh -",
    "|sh -",
    "$(curl",
    "$(wget",
    "$(id",
    "$(whoami",
    "$(cat",
    "$(uname",
    "`id`",
    "`whoami`",
    "`curl ",
    "`wget ",
    ";whoami",
    "; whoami",
    "|whoami",
    "| whoami",
    ";cat ",
    "; cat ",
    "|cat ",
    "| cat ",
    "${ifs}",
    "$ifs",
    "/dev/tcp/",
    "/dev/udp/",
    "sh -i",
    "nc -e",
    "nc.exe",
    "powershell",
    "cmd.exe",
    "certutil -urlcache",
    "base64 -d",
    "base64 --decode",
    "<?php",
    "eval(",
    "passthru(",
    "shell_exec(",
    "system(",
    "class.module.classloader",
    "#_memberaccess",
    "#context[",
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

/// Checks if a pattern is already covered by the built-in attack signatures
/// (either exact match or superstring of an existing built-in substring).
#[inline]
pub fn is_builtin_attack_pattern(pattern: &str) -> bool {
    let lower = pattern.trim().to_ascii_lowercase();
    BUILTIN_ATTACK_PATTERNS.iter().any(|&p| lower.contains(p))
}

/// Checks if a User-Agent is already covered by the built-in malicious scanner list
/// (either exact match or superstring of an existing built-in scanner substring).
#[inline]
pub fn is_builtin_user_agent(ua: &str) -> bool {
    let lower = ua.trim().to_ascii_lowercase();
    BUILTIN_MALICIOUS_USER_AGENTS
        .iter()
        .any(|&b| lower.contains(b))
}

/// Declarative configuration for the WAF Lite plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WafLiteConfig {
    /// Operational mode: "block" (rejects with 403) or "detect" (audit header injection only).
    pub mode: String,
    /// Whether to inspect request URI path for attack signatures (SQLi, path traversal, XSS).
    pub check_path: bool,
    /// Whether to inspect request query string parameters for attack signatures.
    pub check_query: bool,
    /// Whether to inspect User-Agent header against known scanners/malicious scrapers.
    pub check_user_agent: bool,
    /// List of specific request header names to inspect for malicious injection payloads.
    pub check_headers: Vec<String>,
    /// Additional custom attack pattern substrings to match and block.
    pub custom_blocked_patterns: Vec<String>,
    /// Additional custom user-agent pattern substrings to match and block.
    pub custom_blocked_user_agents: Vec<String>,
}

/// Helper to validate RFC 9110 header token grammar.
fn is_valid_header_name(h: &str) -> bool {
    let trimmed = h.trim();
    if trimmed.is_empty() {
        return false;
    }
    trimmed.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'!' | b'#'
                    | b'$'
                    | b'%'
                    | b'&'
                    | b'\''
                    | b'*'
                    | b'+'
                    | b'-'
                    | b'.'
                    | b'^'
                    | b'_'
                    | b'`'
                    | b'|'
                    | b'~'
            )
    })
}

/// Validates WAF Lite configuration strictly and automatically deduplicates custom patterns
/// against built-in signatures and internal duplicates.
pub fn validate_waf_lite_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<WafLiteConfig, SyncError> {
    let mut cfg: WafLiteConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete waf_lite config: {e}"
            ),
        })?;

    // 1. Validate & normalize Mode
    let mode_lower = cfg.mode.trim().to_ascii_lowercase();
    match mode_lower.as_str() {
        "block" | "detect" => {
            cfg.mode = mode_lower;
        }
        other => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid mode '{other}' (expected 'block' or 'detect')"
                ),
            });
        }
    }

    // 2. Validate Active Scope (at least one check must be enabled)
    let has_any_check =
        cfg.check_path || cfg.check_query || cfg.check_user_agent || !cfg.check_headers.is_empty();

    if !has_any_check {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': all inspection vectors are disabled (must enable at least one of check_path, check_query, check_user_agent, or check_headers)"
            ),
        });
    }

    // 3. Validate & Deduplicate Header Names in check_headers
    let mut seen_headers = HashSet::with_capacity(cfg.check_headers.len());
    let mut deduped_headers = Vec::with_capacity(cfg.check_headers.len());
    for header in &cfg.check_headers {
        if !is_valid_header_name(header) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid HTTP header name '{header}' in check_headers"
                ),
            });
        }
        let lower = header.trim().to_ascii_lowercase();
        if seen_headers.insert(lower.clone()) {
            deduped_headers.push(lower);
        }
    }
    cfg.check_headers = deduped_headers;

    // 4. Validate & Deduplicate Custom Blocked Patterns against built-in signatures and duplicate inputs
    let mut seen_patterns = HashSet::with_capacity(cfg.custom_blocked_patterns.len());
    let mut deduped_patterns = Vec::with_capacity(cfg.custom_blocked_patterns.len());
    for pattern in &cfg.custom_blocked_patterns {
        let trimmed = pattern.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom_blocked_patterns cannot contain empty strings"
                ),
            });
        }
        if trimmed.len() > 256 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom pattern '{trimmed}' exceeds maximum length of 256 characters"
                ),
            });
        }
        let lower = trimmed.to_ascii_lowercase();
        if is_builtin_attack_pattern(&lower) {
            tracing::info!(
                plugin_id = %plugin_id,
                pattern = %trimmed,
                "WAF Lite custom pattern is already covered by built-in dictionary; deduplicating"
            );
            continue;
        }
        if seen_patterns.insert(lower.clone()) {
            deduped_patterns.push(lower);
        }
    }
    cfg.custom_blocked_patterns = deduped_patterns;

    // 5. Validate & Deduplicate Custom Blocked User Agents against built-in scanners and duplicate inputs
    let mut seen_uas = HashSet::with_capacity(cfg.custom_blocked_user_agents.len());
    let mut deduped_uas = Vec::with_capacity(cfg.custom_blocked_user_agents.len());
    for ua in &cfg.custom_blocked_user_agents {
        let trimmed = ua.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom_blocked_user_agents cannot contain empty strings"
                ),
            });
        }
        if trimmed.len() > 256 {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': custom user agent '{trimmed}' exceeds maximum length of 256 characters"
                ),
            });
        }
        let lower = trimmed.to_ascii_lowercase();
        if is_builtin_user_agent(&lower) {
            tracing::info!(
                plugin_id = %plugin_id,
                user_agent = %trimmed,
                "WAF Lite custom user-agent is already covered by built-in dictionary; deduplicating"
            );
            continue;
        }
        if seen_uas.insert(lower.clone()) {
            deduped_uas.push(lower);
        }
    }
    cfg.custom_blocked_user_agents = deduped_uas;

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_waf_lite_config() {
        let val = json!({
            "mode": "block",
            "check_path": true,
            "check_query": true,
            "check_user_agent": true,
            "check_headers": ["X-Forwarded-Host", "Referer", "x-forwarded-host"],
            "custom_blocked_patterns": ["custom_secret_file", "wp-login.php"],
            "custom_blocked_user_agents": ["evil-bot", "custom-probe"]
        });
        let res = validate_waf_lite_config("waf_1", &val);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        assert_eq!(cfg.check_headers, vec!["x-forwarded-host", "referer"]);
        assert_eq!(cfg.custom_blocked_patterns.len(), 2);
        assert_eq!(cfg.custom_blocked_user_agents.len(), 2);
    }

    #[test]
    fn test_deduplication_of_builtin_and_repeated_patterns() {
        let val = json!({
            "mode": "block",
            "check_path": true,
            "check_query": true,
            "check_user_agent": true,
            "check_headers": [],
            "custom_blocked_patterns": [
                "../",                     // Built-in (eliminated)
                "/etc/passwd",             // Built-in (eliminated)
                "pg_sleep(",               // Covered by built-in "sleep(" (eliminated)
                "powershell.exe",          // Covered by built-in "powershell" (eliminated)
                "CUSTOM_SIGNATURE_1",
                "custom_signature_1",      // Duplicate (eliminated)
                "${jndi:",                 // Built-in (eliminated)
                "unique_signature_2"
            ],
            "custom_blocked_user_agents": [
                "sqlmap",                  // Built-in (eliminated)
                "sqlmap/1.6",              // Covered by built-in "sqlmap" (eliminated)
                "BADBOT",
                "badbot",                  // Duplicate (eliminated)
                "nikto",                   // Built-in (eliminated)
                "crawler-3"
            ]
        });

        let res = validate_waf_lite_config("waf_dedup", &val).expect("valid config");
        assert_eq!(
            res.custom_blocked_patterns,
            vec![
                "custom_signature_1".to_string(),
                "unique_signature_2".to_string()
            ]
        );
        assert_eq!(
            res.custom_blocked_user_agents,
            vec!["badbot".to_string(), "crawler-3".to_string()]
        );
    }

    #[test]
    fn test_invalid_mode_rejected() {
        let val = json!({
            "mode": "quarantine",
            "check_path": true,
            "check_query": false,
            "check_user_agent": false,
            "check_headers": [],
            "custom_blocked_patterns": [],
            "custom_blocked_user_agents": []
        });
        let res = validate_waf_lite_config("waf_bad_mode", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("invalid mode"));
    }

    #[test]
    fn test_all_checks_disabled_rejected() {
        let val = json!({
            "mode": "block",
            "check_path": false,
            "check_query": false,
            "check_user_agent": false,
            "check_headers": [],
            "custom_blocked_patterns": [],
            "custom_blocked_user_agents": []
        });
        let res = validate_waf_lite_config("waf_all_disabled", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("all inspection vectors are disabled")
        );
    }

    #[test]
    fn test_invalid_header_in_check_headers_rejected() {
        let val = json!({
            "mode": "block",
            "check_path": true,
            "check_query": false,
            "check_user_agent": false,
            "check_headers": ["Bad Header: Name"],
            "custom_blocked_patterns": [],
            "custom_blocked_user_agents": []
        });
        let res = validate_waf_lite_config("waf_bad_header", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid HTTP header name")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "mode": "block"
        });
        let res = validate_waf_lite_config("waf_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
