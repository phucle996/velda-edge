//! Prometheus Exporter Configuration Schema & Semantic Validation.
//!
//! Enforces a strict declarative contract for the Prometheus Exporter plugin.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **No Assumptions**: Configuration blocks (`scrape`, `tls`, `cors`, `auth`) are strictly mandatory.
//! - **Explicit Opt-Out**: Disabled features must be explicitly declared (`"mode": "none"` or `"enabled": false`).
//! - **Fail-Fast Invariants**: Missing fields or invalid combinations (e.g. `protocol: "https"` with `tls.mode: "none"`)
//!   cause immediate validation failure, halting artifact compilation before runtime staging.

use serde::{Deserialize, Serialize};

use crate::SyncError;

// ============================================================================
// Entity & Schema Definitions
// ============================================================================

/// Operational scrape limits for Prometheus Exporter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScrapeExporterConfig {
    /// Request timeout threshold in milliseconds (e.g. 10000ms).
    pub timeout_ms: u64,
    /// Maximum concurrent scraper connections allowed simultaneously.
    pub max_concurrency: usize,
    /// Maximum scratch buffer capacity allocated for OpenMetrics text serialization.
    pub max_buffer_size_kb: usize,
}

/// Strict TLS/mTLS configuration for Prometheus Exporter.
///
/// # Strict Declarative Invariant
/// No silent fallbacks. If disabled, `mode` must be explicitly set to `"none"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsExporterConfig {
    /// Operating mode: strictly "none", "tls", or "mtls".
    pub mode: String,
    /// Path to server certificate file (mandatory if mode != "none").
    #[serde(default)]
    pub cert_file: Option<String>,
    /// Path to private key file (mandatory if mode != "none").
    #[serde(default)]
    pub key_file: Option<String>,
    /// Path to client CA certificate bundle (mandatory if mode == "mtls").
    #[serde(default)]
    pub client_ca_file: Option<String>,
}

/// Strict Cross-Origin Resource Sharing (CORS) configuration for Prometheus Exporter.
///
/// # Strict Declarative Invariant
/// Must explicitly declare `enabled: true/false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorsExporterConfig {
    /// Whether Cross-Origin Resource Sharing (CORS) is enabled.
    pub enabled: bool,
    /// Permitted origins (e.g. `["https://grafana.internal"]`).
    #[serde(default)]
    pub allow_origins: Vec<String>,
    /// Permitted HTTP methods (e.g. `["GET", "OPTIONS"]`).
    #[serde(default)]
    pub allow_methods: Vec<String>,
    /// Permitted HTTP request headers.
    #[serde(default)]
    pub allow_headers: Vec<String>,
}

/// Strict Authentication configuration for Prometheus Exporter.
///
/// # Strict Declarative Invariant
/// Must explicitly declare `mode: "none"`, `"bearer"`, or `"basic"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthExporterConfig {
    /// Authentication mode: "none", "bearer", or "basic".
    pub mode: String,
    /// Secret bearer token (mandatory if mode == "bearer").
    #[serde(default)]
    pub token: Option<String>,
    /// Basic auth username (mandatory if mode == "basic").
    #[serde(default)]
    pub username: Option<String>,
    /// Basic auth password (mandatory if mode == "basic").
    #[serde(default)]
    pub password: Option<String>,
}

/// Production-grade declarative configuration for the Prometheus Exporter plugin.
///
/// # Strict Declarative Contract
/// Every configuration block (`scrape`, `tls`, `cors`, `auth`) is strictly mandatory.
/// Omission of any block triggers validation failure without silent assumptions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrometheusExporterConfig {
    /// Target socket address to bind and listen (e.g. "0.0.0.0:9090").
    pub listen: String,
    /// HTTP URI path for metrics endpoint (e.g. "/metrics").
    pub path: String,
    /// Application protocol family ("http" or "https").
    pub protocol: String,
    /// Operational scrape limits.
    pub scrape: ScrapeExporterConfig,
    /// Strict TLS/mTLS configuration block.
    pub tls: TlsExporterConfig,
    /// Strict CORS configuration block.
    pub cors: CorsExporterConfig,
    /// Strict Authentication configuration block.
    pub auth: AuthExporterConfig,
}

// ============================================================================
// Semantic Validation
// ============================================================================

/// Validates Prometheus Exporter configuration strictly according to production standards.
///
/// # Strict Declarative Contract
/// Rejects missing mandatory configuration blocks, invalid protocols, malformed addresses,
/// or inconsistent security modes (e.g. https with tls mode none).
pub fn validate_prometheus_exporter_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<PrometheusExporterConfig, SyncError> {
    let cfg: PrometheusExporterConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete prometheus_exporter config: {e}"
            ),
        })?;

    // 1. Validate Listen Address
    let trimmed_listen = cfg.listen.trim();
    if trimmed_listen.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!("Plugin '{plugin_id}': listen address cannot be empty"),
        });
    }
    if trimmed_listen.parse::<std::net::SocketAddr>().is_err() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': listen address '{trimmed_listen}' is not a valid SocketAddr (expected IP:PORT, e.g. 0.0.0.0:9090)"
            ),
        });
    }

    // 2. Validate Path
    let trimmed_path = cfg.path.trim();
    if !trimmed_path.starts_with('/') {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': metrics path '{trimmed_path}' must start with '/'"
            ),
        });
    }

    // 3. Validate Protocol & TLS Correlation
    let protocol = cfg.protocol.trim().to_ascii_lowercase();
    if protocol != "http" && protocol != "https" {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': protocol must be 'http' or 'https', got '{protocol}'"
            ),
        });
    }

    let tls_mode = cfg.tls.mode.trim().to_ascii_lowercase();
    match tls_mode.as_str() {
        "none" => {
            if protocol == "https" {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': protocol 'https' requires tls.mode to be 'tls' or 'mtls', got 'none'"
                    ),
                });
            }
        }
        "tls" => {
            let has_cert = cfg
                .tls
                .cert_file
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let has_key = cfg
                .tls
                .key_file
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if !has_cert || !has_key {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': tls.mode 'tls' requires both 'cert_file' and 'key_file' to be specified"
                    ),
                });
            }
        }
        "mtls" => {
            let has_cert = cfg
                .tls
                .cert_file
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let has_key = cfg
                .tls
                .key_file
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let has_ca = cfg
                .tls
                .client_ca_file
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if !has_cert || !has_key || !has_ca {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': tls.mode 'mtls' requires 'cert_file', 'key_file', and 'client_ca_file' to be specified"
                    ),
                });
            }
        }
        _ => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid tls.mode '{tls_mode}' (must be 'none', 'tls', or 'mtls')"
                ),
            });
        }
    }

    // 4. Validate CORS
    if cfg.cors.enabled && cfg.cors.allow_origins.is_empty() {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': cors is enabled but 'allow_origins' list is empty"
            ),
        });
    }

    // 5. Validate Auth
    let auth_mode = cfg.auth.mode.trim().to_ascii_lowercase();
    match auth_mode.as_str() {
        "none" => {}
        "bearer" => {
            let has_token = cfg
                .auth
                .token
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if !has_token {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': auth.mode 'bearer' requires 'token' to be specified"
                    ),
                });
            }
        }
        "basic" => {
            let has_user = cfg
                .auth
                .username
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            let has_pass = cfg
                .auth
                .password
                .as_deref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if !has_user || !has_pass {
                return Err(SyncError::Validation {
                    domain: "plugins".into(),
                    reason: format!(
                        "Plugin '{plugin_id}': auth.mode 'basic' requires both 'username' and 'password' to be specified"
                    ),
                });
            }
        }
        _ => {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid auth.mode '{auth_mode}' (must be 'none', 'bearer', or 'basic')"
                ),
            });
        }
    }

    // 6. Validate Scrape Limits
    if cfg.scrape.timeout_ms < 100 || cfg.scrape.timeout_ms > 120_000 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': scrape.timeout_ms ({}) out of valid range [100, 120000]",
                cfg.scrape.timeout_ms
            ),
        });
    }
    if cfg.scrape.max_concurrency == 0 || cfg.scrape.max_concurrency > 1024 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': scrape.max_concurrency ({}) out of valid range [1, 1024]",
                cfg.scrape.max_concurrency
            ),
        });
    }
    if cfg.scrape.max_buffer_size_kb < 8 || cfg.scrape.max_buffer_size_kb > 4096 {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': scrape.max_buffer_size_kb ({}) out of valid range [8, 4096]",
                cfg.scrape.max_buffer_size_kb
            ),
        });
    }

    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_prometheus_exporter_config() {
        let json = serde_json::json!({
            "listen": "0.0.0.0:9090",
            "path": "/metrics",
            "protocol": "http",
            "scrape": {
                "timeout_ms": 5000,
                "max_concurrency": 16,
                "max_buffer_size_kb": 256
            },
            "tls": {
                "mode": "none"
            },
            "cors": {
                "enabled": false,
                "allow_origins": [],
                "allow_methods": [],
                "allow_headers": []
            },
            "auth": {
                "mode": "none"
            }
        });

        let res = validate_prometheus_exporter_config("metrics-exp", &json);
        assert!(res.is_ok());
        let cfg = res.unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:9090");
        assert_eq!(cfg.path, "/metrics");
        assert_eq!(cfg.protocol, "http");
    }

    #[test]
    fn test_prometheus_exporter_missing_mandatory_blocks() {
        // Missing "auth" block
        let bad_json = serde_json::json!({
            "listen": "0.0.0.0:9090",
            "path": "/metrics",
            "protocol": "http",
            "scrape": {
                "timeout_ms": 5000,
                "max_concurrency": 16,
                "max_buffer_size_kb": 256
            },
            "tls": { "mode": "none" },
            "cors": { "enabled": false }
        });

        let res = validate_prometheus_exporter_config("p-test", &bad_json);
        assert!(res.is_err());
    }

    #[test]
    fn test_prometheus_exporter_tls_and_auth_variants() {
        // 1. https with tls mode none -> fail
        let mut cfg = serde_json::json!({
            "listen": "0.0.0.0:9090",
            "path": "/metrics",
            "protocol": "https",
            "scrape": { "timeout_ms": 5000, "max_concurrency": 16, "max_buffer_size_kb": 256 },
            "tls": { "mode": "none" },
            "cors": { "enabled": false },
            "auth": { "mode": "none" }
        });
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_err());

        // 2. tls mode tls without cert_file -> fail
        cfg["tls"] = serde_json::json!({ "mode": "tls", "key_file": "/tmp/key.pem" });
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_err());

        // 3. tls mode mtls without client_ca_file -> fail
        cfg["tls"] = serde_json::json!({
            "mode": "mtls",
            "cert_file": "/tmp/cert.pem",
            "key_file": "/tmp/key.pem"
        });
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_err());

        // 4. valid mtls
        cfg["tls"] = serde_json::json!({
            "mode": "mtls",
            "cert_file": "/tmp/cert.pem",
            "key_file": "/tmp/key.pem",
            "client_ca_file": "/tmp/ca.pem"
        });
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_ok());

        // 5. bearer auth without token -> fail
        cfg["auth"] = serde_json::json!({ "mode": "bearer" });
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_err());

        // 6. basic auth without password -> fail
        cfg["auth"] = serde_json::json!({ "mode": "basic", "username": "admin" });
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_err());

        // 7. invalid scrape limits
        cfg["auth"] = serde_json::json!({ "mode": "none" });
        cfg["scrape"]["timeout_ms"] = serde_json::json!(10); // < 100ms
        assert!(validate_prometheus_exporter_config("p1", &cfg).is_err());
    }
}
