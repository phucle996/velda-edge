//! High-Performance URL Rewrite Plugin (`url_rewrite`).
//!
//! # Architectural Invariants
//! 1. **Phase & Target Context**:
//!    - Evaluated in [`crate::pre_upstream`] phase on routed HTTP requests.
//!    - Modifies [`http::Uri`] (`path` and/or `query`) before request dispatch to backend upstreams.
//! 2. **Single Responsibility (URI Only)**:
//!    - Operates strictly on URI semantics. Does NOT mutate `Host` or `:authority` headers;
//!      header transformations belong to [`crate::pre_upstream::header_mutation`].
//! 3. **Reverse Permission Granting ("Cấp quyền ngược")**:
//!    - Requires `header: AccessLevel::Write` (URI is request head metadata).
//!    - Declares `payload: AccessLevel::None` (Request body remains 100% Zero-Copy streaming passthrough).
//! 4. **RFC 3986 Path Sanitization & Traversal Defense**:
//!    - Delegated to [`super::sanitizer`].
//!    - Fails-fast on traversal violations (`..` escaping root) with an immutable `400 Bad Request`.
//! 5. **Hot-Path Zero-Allocation Bypass**:
//!    - If neither path nor query string was modified, no `Uri` or `PathAndQuery` allocations occur.

use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, StatusCode, Uri};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use velda_core::{Action, L7Request, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use super::sanitizer::{is_already_normalized, normalize_and_sanitize_path};
use super::strategy::{CompiledPathStrategy, PathRewriteStrategy, QueryStrategy};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Configuration schema for [`UrlRewritePlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UrlRewriteConfig {
    /// Path rewrite rule. If `None`, path is not modified.
    #[serde(default)]
    pub path: Option<PathRewriteStrategy>,
    /// Query string handling strategy (default: [`QueryStrategy::Keep`]).
    #[serde(default)]
    pub query: QueryStrategy,
    /// Whether to enforce RFC 3986 path normalization and block directory traversal (`..`).
    /// Defaults to `true` for enterprise edge security.
    #[serde(default = "default_true")]
    pub sanitize_path: bool,
}

impl Default for UrlRewriteConfig {
    fn default() -> Self {
        Self {
            path: None,
            query: QueryStrategy::Keep,
            sanitize_path: true,
        }
    }
}

const fn default_true() -> bool {
    true
}

/// URL Rewrite Plugin instance.
pub struct UrlRewritePlugin {
    pub id: String,
    strategy: Option<CompiledPathStrategy>,
    query_strategy: QueryStrategy,
    sanitize_path: bool,
    bad_request_traversal_response: L7Response,
}

impl UrlRewritePlugin {
    /// Constructs a new `UrlRewritePlugin` with pre-compiled rules and static error responses.
    pub fn new(id: impl Into<String>, config: UrlRewriteConfig) -> Result<Self, PluginError> {
        let compiled_strategy = match &config.path {
            Some(strat) => Some(CompiledPathStrategy::compile(strat)?),
            None => None,
        };

        let bad_request_traversal_response = L7Response::from_bytes(
            StatusCode::BAD_REQUEST,
            "{\"error\":\"Path traversal attempt detected\"}",
        )
        .with_header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .with_header(CONTENT_LENGTH, HeaderValue::from_static("41"));

        Ok(Self {
            id: id.into(),
            strategy: compiled_strategy,
            query_strategy: config.query,
            sanitize_path: config.sanitize_path,
            bad_request_traversal_response,
        })
    }

    /// Instantiates plugin from compiled configuration.
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        if let Some(mode) = config
            .config
            .get("path_rewrite_mode")
            .and_then(|v| v.as_str())
        {
            let path_match = config
                .config
                .get("path_match")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let path_replacement = config
                .config
                .get("path_replacement")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let path_strat = match mode {
                "prefix" => {
                    if path_replacement.is_empty() {
                        Some(PathRewriteStrategy::StripPrefix {
                            prefix: path_match.to_string(),
                        })
                    } else {
                        Some(PathRewriteStrategy::ReplacePrefix {
                            prefix: path_match.to_string(),
                            replacement: path_replacement.to_string(),
                        })
                    }
                }
                "regex" => Some(PathRewriteStrategy::Regex {
                    pattern: path_match.to_string(),
                    replacement: path_replacement.to_string(),
                }),
                "exact" => Some(PathRewriteStrategy::Exact {
                    path: path_match.to_string(),
                    replacement: path_replacement.to_string(),
                }),
                _ => None,
            };

            let q_mode = config
                .config
                .get("query_rewrite_mode")
                .and_then(|v| v.as_str())
                .unwrap_or("keep");
            let q_replacement = config
                .config
                .get("query_replacement")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let query_strat = match q_mode {
                "strip" | "drop" => QueryStrategy::Drop,
                "append" | "replace" => QueryStrategy::Append {
                    query: q_replacement.to_string(),
                },
                _ => QueryStrategy::Keep,
            };

            let sanitize_path = config
                .config
                .get("sanitize_path")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            return Self::new(
                config.id.clone(),
                UrlRewriteConfig {
                    path: path_strat,
                    query: query_strat,
                    sanitize_path,
                },
            );
        }

        let conf: UrlRewriteConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| PluginError::Config(format!("Invalid url_rewrite config: {e}")))?;
        Self::new(config.id.clone(), conf)
    }
}

impl PreUpstreamPlugin for UrlRewritePlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        let current_path = req.uri.path();
        let current_query = req.uri.query();

        // 1. Process path transformation
        let (raw_rewritten, changed) = match &self.strategy {
            Some(strategy) => strategy.rewrite_path(current_path),
            None => (std::borrow::Cow::Borrowed(current_path), false),
        };

        // 2. Path Sanitization & RFC 3986 Normalization
        let (final_path, path_changed) = if self.sanitize_path {
            let path_to_check = raw_rewritten.as_ref();
            if is_already_normalized(path_to_check) {
                (raw_rewritten, changed)
            } else {
                match normalize_and_sanitize_path(path_to_check) {
                    Ok(normalized) => (std::borrow::Cow::Owned(normalized), true),
                    Err(_) => {
                        // Directory traversal violation: fail-fast 400 Bad Request
                        return Action::Respond(self.bad_request_traversal_response.clone());
                    }
                }
            }
        } else {
            (raw_rewritten, changed)
        };

        // 3. Process query transformation
        let (new_query, query_changed) = self.query_strategy.rewrite_query(current_query);

        // 4. Hot-path zero-allocation bypass if no transformations took place
        if !path_changed && !query_changed {
            return Action::Continue;
        }

        // 5. Rebuild URI PathAndQuery
        let path_str = final_path.as_ref();
        let new_pq_str = match &new_query {
            Some(q) if !q.is_empty() => format!("{path_str}?{q}"),
            _ => path_str.to_string(),
        };

        if let Ok(pq) = http::uri::PathAndQuery::from_maybe_shared(new_pq_str) {
            let mut parts = req.uri.clone().into_parts();
            parts.path_and_query = Some(pq);
            if let Ok(updated_uri) = Uri::from_parts(parts) {
                req.uri = updated_uri;
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        // Reverse permission: Write access on Request metadata (URI), None on Payload body
        PluginCapability::new(AccessLevel::Write, AccessLevel::None)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Version};
    use velda_core::Body;

    fn make_test_request(uri_str: &str) -> L7Request {
        L7Request::new(
            Method::GET,
            uri_str.parse().unwrap(),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        )
    }

    #[test]
    fn test_full_pipeline_strip_and_append() {
        let plugin = UrlRewritePlugin::new(
            "rw-full",
            UrlRewriteConfig {
                path: Some(PathRewriteStrategy::StripPrefix {
                    prefix: "/api/v1".into(),
                }),
                query: QueryStrategy::Append {
                    query: "source=edge".into(),
                },
                sanitize_path: true,
            },
        )
        .unwrap();

        let mut req = make_test_request("/api/v1/orders/10?type=fast");
        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));
        assert_eq!(req.uri.path(), "/orders/10");
        assert_eq!(req.uri.query(), Some("type=fast&source=edge"));
    }

    #[test]
    fn test_traversal_rejection() {
        let plugin = UrlRewritePlugin::new(
            "rw-traversal",
            UrlRewriteConfig {
                path: Some(PathRewriteStrategy::StripPrefix {
                    prefix: "/api/v1".into(),
                }),
                query: QueryStrategy::Keep,
                sanitize_path: true,
            },
        )
        .unwrap();

        let mut req = make_test_request("/api/v1/../../etc/shadow");
        let mut target: Arc<str> = Arc::from("http_upstream");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        match action {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::BAD_REQUEST);
            }
            _ => panic!("Expected Action::Respond(400 Bad Request)"),
        }
    }
}
