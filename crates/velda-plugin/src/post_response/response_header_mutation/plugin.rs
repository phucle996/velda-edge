//! Response Header Mutation Post-Response Plugin (`response_header_mutation`).
//!
//! Adds, mutates, appends, or strips HTTP response headers before downstream frame encoding:
//! - **Zero Automatic Overwrites**: Does not inject or override headers without explicit configuration.
//! - **In-Place Mutation**: Modifies [`L7Response`] headers directly on the hot path.
//! - **Pre-Compiled Static Specs**: Static header values are parsed once during configuration load.
//! - **Authoritative Variable Propagation**: Correlates Slot 0 Request ID and routing telemetry from `HeaderWorkspace`.

use std::time::{SystemTime, UNIX_EPOCH};

use http::header::{HeaderName, HeaderValue};
use velda_core::{HeaderWorkspace, L7Response};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

// ── Dynamic Response Variables & Value Specifications ────────────────

/// Dynamic runtime variables evaluated against the outgoing response and incoming request workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseDynamicVar {
    /// In-flight request ID (from response `x-request-id` or authoritative Slot 0 request ID).
    RequestId,
    /// HTTP status code as a numeric string (e.g. "200", "404").
    Status,
    /// Current UNIX epoch timestamp in seconds.
    TimestampEpoch,
}

/// Specification for response header values: static value or dynamic variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseHeaderValueSpec {
    /// Static header value pre-computed at configuration compile time.
    Static(HeaderValue),
    /// Dynamic variable resolved per response on the hot path.
    Variable(ResponseDynamicVar),
}

impl ResponseHeaderValueSpec {
    /// Parses a header value specification from a string.
    /// Supports `$$` to escape a literal `$`.
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        if let Some(rest) = s.strip_prefix('$') {
            if rest == "$" {
                return Ok(Self::Static(HeaderValue::from_static("$")));
            }
            let var = match rest {
                "request_id" => ResponseDynamicVar::RequestId,
                "status" => ResponseDynamicVar::Status,
                "timestamp_epoch" => ResponseDynamicVar::TimestampEpoch,
                _ => {
                    return Err(PluginError::Config(format!(
                        "Unknown response dynamic variable '{s}'; supported variables are: $request_id, $status, $timestamp_epoch (use '$$' to escape literal '$')"
                    )));
                }
            };
            Ok(Self::Variable(var))
        } else {
            let val = HeaderValue::from_str(s).map_err(|e| {
                PluginError::Config(format!("Invalid header value string '{s}': {e}"))
            })?;
            Ok(Self::Static(val))
        }
    }

    /// Resolves the header value against the outgoing response and incoming request workspace.
    #[inline]
    pub fn resolve(&self, resp: &L7Response, req_headers: &HeaderWorkspace) -> Option<HeaderValue> {
        match self {
            Self::Static(val) => Some(val.clone()),
            Self::Variable(var) => match var {
                ResponseDynamicVar::RequestId => Some(req_headers.request_id().clone()),
                ResponseDynamicVar::Status => match resp.status.as_u16() {
                    200 => Some(HeaderValue::from_static("200")),
                    201 => Some(HeaderValue::from_static("201")),
                    204 => Some(HeaderValue::from_static("204")),
                    301 => Some(HeaderValue::from_static("301")),
                    302 => Some(HeaderValue::from_static("302")),
                    304 => Some(HeaderValue::from_static("304")),
                    400 => Some(HeaderValue::from_static("400")),
                    401 => Some(HeaderValue::from_static("401")),
                    403 => Some(HeaderValue::from_static("403")),
                    404 => Some(HeaderValue::from_static("404")),
                    500 => Some(HeaderValue::from_static("500")),
                    502 => Some(HeaderValue::from_static("502")),
                    503 => Some(HeaderValue::from_static("503")),
                    504 => Some(HeaderValue::from_static("504")),
                    _ => HeaderValue::from_str(resp.status.as_str()).ok(),
                },
                ResponseDynamicVar::TimestampEpoch => {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let mut buf = itoa::Buffer::new();
                    HeaderValue::from_str(buf.format(now)).ok()
                }
            },
        }
    }
}

// ── Plugin Definition & Implementation ───────────────────────────────

/// Configuration schema for [`ResponseHeaderMutationPlugin`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseHeaderMutationConfig {
    /// Headers to be explicitly stripped before transmitting to the downstream client.
    pub remove: Vec<HeaderName>,
    /// Headers to be set or overwritten.
    pub set: Vec<(HeaderName, ResponseHeaderValueSpec)>,
    /// Headers to be appended to existing values (multi-value headers).
    pub add: Vec<(HeaderName, ResponseHeaderValueSpec)>,
    /// Headers to be set ONLY if the response does not already contain them.
    pub set_if_missing: Vec<(HeaderName, ResponseHeaderValueSpec)>,
}

/// Post-Response Header Mutation Plugin.
#[derive(Debug, Clone)]
pub struct ResponseHeaderMutationPlugin {
    pub id: String,
    pub config: ResponseHeaderMutationConfig,
    static_sets: Vec<(HeaderName, HeaderValue)>,
    dynamic_sets: Vec<(HeaderName, ResponseDynamicVar)>,
    static_adds: Vec<(HeaderName, HeaderValue)>,
    dynamic_adds: Vec<(HeaderName, ResponseDynamicVar)>,
    static_set_if_missing: Vec<(HeaderName, HeaderValue)>,
    dynamic_set_if_missing: Vec<(HeaderName, ResponseDynamicVar)>,
}

impl ResponseHeaderMutationPlugin {
    pub fn new(id: impl Into<String>, config: ResponseHeaderMutationConfig) -> Self {
        let mut static_sets = Vec::new();
        let mut dynamic_sets = Vec::new();
        for (name, spec) in &config.set {
            match spec {
                ResponseHeaderValueSpec::Static(val) => {
                    static_sets.push((name.clone(), val.clone()))
                }
                ResponseHeaderValueSpec::Variable(var) => dynamic_sets.push((name.clone(), *var)),
            }
        }

        let mut static_adds = Vec::new();
        let mut dynamic_adds = Vec::new();
        for (name, spec) in &config.add {
            match spec {
                ResponseHeaderValueSpec::Static(val) => {
                    static_adds.push((name.clone(), val.clone()))
                }
                ResponseHeaderValueSpec::Variable(var) => dynamic_adds.push((name.clone(), *var)),
            }
        }

        let mut static_set_if_missing = Vec::new();
        let mut dynamic_set_if_missing = Vec::new();
        for (name, spec) in &config.set_if_missing {
            match spec {
                ResponseHeaderValueSpec::Static(val) => {
                    static_set_if_missing.push((name.clone(), val.clone()))
                }
                ResponseHeaderValueSpec::Variable(var) => {
                    dynamic_set_if_missing.push((name.clone(), *var))
                }
            }
        }

        Self {
            id: id.into(),
            config,
            static_sets,
            dynamic_sets,
            static_adds,
            dynamic_adds,
            static_set_if_missing,
            dynamic_set_if_missing,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_response_header_mutation_config(&config.id, &config.config)
            .map_err(|e| {
                PluginError::Config(format!(
                    "Invalid response_header_mutation configuration for '{}': {e}",
                    config.id
                ))
            })?;

        let mut remove = Vec::with_capacity(cfg.remove_headers.len());
        for h in &cfg.remove_headers {
            let name = HeaderName::from_bytes(h.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid remove header name '{h}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            remove.push(name);
        }

        let mut set = Vec::with_capacity(cfg.set_headers.len());
        for (k, v) in &cfg.set_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid set header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(v)?;
            set.push((name, spec));
        }

        let mut add = Vec::with_capacity(cfg.append_headers.len());
        for (k, v) in &cfg.append_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid append header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(v)?;
            add.push((name, spec));
        }

        let mut set_if_missing = Vec::with_capacity(cfg.set_if_missing.len());
        for (k, v) in &cfg.set_if_missing {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!(
                    "Invalid set_if_missing header name '{k}' in plugin '{}': {e}",
                    config.id
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(v)?;
            set_if_missing.push((name, spec));
        }

        Ok(Self::new(
            &config.id,
            ResponseHeaderMutationConfig {
                remove,
                set,
                add,
                set_if_missing,
            },
        ))
    }
}

#[inline]
fn resolve_response_var_cached(
    var: ResponseDynamicVar,
    resp: &L7Response,
    req_headers: &HeaderWorkspace,
    cached_timestamp: &mut Option<HeaderValue>,
) -> Option<HeaderValue> {
    match var {
        ResponseDynamicVar::RequestId => Some(req_headers.request_id().clone()),
        ResponseDynamicVar::Status => match resp.status.as_u16() {
            200 => Some(HeaderValue::from_static("200")),
            201 => Some(HeaderValue::from_static("201")),
            204 => Some(HeaderValue::from_static("204")),
            301 => Some(HeaderValue::from_static("301")),
            302 => Some(HeaderValue::from_static("302")),
            304 => Some(HeaderValue::from_static("304")),
            400 => Some(HeaderValue::from_static("400")),
            401 => Some(HeaderValue::from_static("401")),
            403 => Some(HeaderValue::from_static("403")),
            404 => Some(HeaderValue::from_static("404")),
            500 => Some(HeaderValue::from_static("500")),
            502 => Some(HeaderValue::from_static("502")),
            503 => Some(HeaderValue::from_static("503")),
            504 => Some(HeaderValue::from_static("504")),
            _ => HeaderValue::from_str(resp.status.as_str()).ok(),
        },
        ResponseDynamicVar::TimestampEpoch => {
            if let Some(ts) = cached_timestamp.as_ref() {
                return Some(ts.clone());
            }
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut buf = itoa::Buffer::new();
            let ts = HeaderValue::from_str(buf.format(now)).ok()?;
            *cached_timestamp = Some(ts.clone());
            Some(ts)
        }
    }
}

impl PostResponsePlugin for ResponseHeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, req_headers: &HeaderWorkspace, resp: &mut L7Response) {
        // Step 1: Remove explicitly requested headers
        for name in &self.config.remove {
            resp.headers.remove(name);
        }

        // Step 2: Set or overwrite static headers (100% direct, zero enum matching)
        for (name, val) in &self.static_sets {
            resp.headers.insert(name.clone(), val.clone());
        }

        // Step 3: Append static multi-value headers
        for (name, val) in &self.static_adds {
            resp.headers.append(name.clone(), val.clone());
        }

        // Step 4: Set-if-missing static headers
        for (name, val) in &self.static_set_if_missing {
            if !resp.headers.contains_key(name) {
                resp.headers.insert(name.clone(), val.clone());
            }
        }

        // Step 5: Dynamic mutations (only if dynamic variables configured)
        if !self.dynamic_sets.is_empty()
            || !self.dynamic_adds.is_empty()
            || !self.dynamic_set_if_missing.is_empty()
        {
            let mut cached_timestamp: Option<HeaderValue> = None;

            for (name, var) in &self.dynamic_sets {
                if let Some(val) =
                    resolve_response_var_cached(*var, resp, req_headers, &mut cached_timestamp)
                {
                    resp.headers.insert(name.clone(), val);
                }
            }

            for (name, var) in &self.dynamic_adds {
                if let Some(val) =
                    resolve_response_var_cached(*var, resp, req_headers, &mut cached_timestamp)
                {
                    resp.headers.append(name.clone(), val);
                }
            }

            for (name, var) in &self.dynamic_set_if_missing {
                if !resp.headers.contains_key(name)
                    && let Some(val) =
                        resolve_response_var_cached(*var, resp, req_headers, &mut cached_timestamp)
                {
                    resp.headers.insert(name.clone(), val);
                }
            }
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, StatusCode};

    #[test]
    fn test_remove_sensitive_headers() {
        let mut config = ResponseHeaderMutationConfig::default();
        config.remove.push(HeaderName::from_static("x-powered-by"));
        config.remove.push(HeaderName::from_static("server"));

        let plugin = ResponseHeaderMutationPlugin::new("mut-1", config);

        let mut headers = HeaderMap::new();
        headers.insert("x-powered-by", HeaderValue::from_static("Express"));
        headers.insert("server", HeaderValue::from_static("Apache/2.4"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{}").with_headers(headers);
        let req_headers = HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert!(!resp.headers.contains_key("x-powered-by"));
        assert!(!resp.headers.contains_key("server"));
        assert!(resp.headers.contains_key("content-type"));
    }

    #[test]
    fn test_set_and_dynamic_variables() {
        let mut config = ResponseHeaderMutationConfig::default();
        config.set.push((
            HeaderName::from_static("server"),
            ResponseHeaderValueSpec::parse("velda-edge").unwrap(),
        ));
        config.set.push((
            HeaderName::from_static("x-request-id"),
            ResponseHeaderValueSpec::parse("$request_id").unwrap(),
        ));
        config.set.push((
            HeaderName::from_static("x-status-reflected"),
            ResponseHeaderValueSpec::parse("$status").unwrap(),
        ));

        let plugin = ResponseHeaderMutationPlugin::new("mut-set", config);

        let headers = HeaderMap::new();
        let mut resp = L7Response::from_bytes(StatusCode::CREATED, "{}").with_headers(headers);
        let req_headers = HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("server").unwrap(), "velda-edge");
        // Automatically correlated from Slot 0 authoritative request id
        assert_eq!(
            resp.headers.get("x-request-id").unwrap(),
            req_headers.request_id()
        );
        assert_eq!(resp.headers.get("x-status-reflected").unwrap(), "201");
    }

    #[test]
    fn test_set_if_missing() {
        let mut config = ResponseHeaderMutationConfig::default();
        config.set_if_missing.push((
            HeaderName::from_static("x-content-type-options"),
            ResponseHeaderValueSpec::parse("nosniff").unwrap(),
        ));
        config.set_if_missing.push((
            HeaderName::from_static("x-existing"),
            ResponseHeaderValueSpec::parse("new-val").unwrap(),
        ));

        let plugin = ResponseHeaderMutationPlugin::new("mut-sim", config);

        let mut headers = HeaderMap::new();
        headers.insert("x-existing", HeaderValue::from_static("original-val"));

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{}").with_headers(headers);
        let req_headers = HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(resp.headers.get("x-existing").unwrap(), "original-val");
    }

    #[test]
    fn test_add_multi_value_headers() {
        let mut config = ResponseHeaderMutationConfig::default();
        config.add.push((
            HeaderName::from_static("vary"),
            ResponseHeaderValueSpec::parse("User-Agent").unwrap(),
        ));

        let plugin = ResponseHeaderMutationPlugin::new("mut-add", config);

        let mut headers = HeaderMap::new();
        headers.insert("vary", HeaderValue::from_static("Accept-Encoding"));

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{}").with_headers(headers);
        let req_headers = HeaderWorkspace::empty();

        plugin.on_response(&req_headers, &mut resp);

        let vary_values: Vec<&str> = resp
            .headers
            .get_all("vary")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();

        assert_eq!(vary_values, vec!["Accept-Encoding", "User-Agent"]);
    }

    #[test]
    fn test_from_config_strict_validation() {
        let valid_json = serde_json::json!({
            "remove_headers": ["server"],
            "set_headers": {
                "server": "velda-edge",
                "x-served-by": "edge-node"
            },
            "append_headers": {
                "vary": "accept-encoding"
            },
            "set_if_missing": {
                "x-content-type-options": "nosniff"
            }
        });

        let p_cfg = PluginConfig {
            id: "res_mut".into(),
            phase: "post_response".into(),
            config: valid_json,
        };

        let plugin = ResponseHeaderMutationPlugin::from_config(&p_cfg).unwrap();
        assert_eq!(plugin.config.remove.len(), 1);
        assert_eq!(plugin.config.set.len(), 2);
        assert_eq!(plugin.config.add.len(), 1);
        assert_eq!(plugin.config.set_if_missing.len(), 1);

        let invalid_json = serde_json::json!({
            "remove_headers": ["server"]
        });
        let bad_cfg = PluginConfig {
            id: "res_mut_bad".into(),
            phase: "post_response".into(),
            config: invalid_json,
        };
        assert!(ResponseHeaderMutationPlugin::from_config(&bad_cfg).is_err());
    }
}
