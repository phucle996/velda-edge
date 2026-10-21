//! Response Header Mutation Post-Response Plugin (`response_header_mutation`).
//!
//! Adds, mutates, appends, or strips HTTP response headers before downstream frame encoding.
//! Operates strictly per explicit user configuration:
//! - **Zero Automatic Overwrites**: Does not silently inject or override any headers without configuration.
//! - **In-Place Mutation**: Modifies [`L7Response`] headers directly on the hot path.
//! - **Pre-Compiled Static Specs**: Static header values are parsed once during configuration load.

use http::header::HeaderName;
use serde_json::Value;
use velda_core::L7Response;
use velda_sync::post_sync::plugin::PluginConfig;

use super::variable::ResponseHeaderValueSpec;
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

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

/// Helper function to parse header specifications from either a JSON object or array of objects.
fn parse_specs(
    val: &Value,
    section_name: &str,
) -> Result<Vec<(HeaderName, ResponseHeaderValueSpec)>, PluginError> {
    let mut specs = Vec::new();

    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                PluginError::Config(format!("Invalid {section_name} header name '{k}': {e}"))
            })?;
            let spec_str = v.as_str().ok_or_else(|| {
                PluginError::Config(format!(
                    "{section_name} header value for '{k}' must be string"
                ))
            })?;
            let spec = ResponseHeaderValueSpec::parse(spec_str)?;
            specs.push((name, spec));
        }
    } else if let Some(arr) = val.as_array() {
        for item in arr {
            if let Some(item_obj) = item.as_object() {
                let name_str = item_obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        PluginError::Config(format!(
                            "{section_name} item must contain 'name' string field"
                        ))
                    })?;
                let val_str = item_obj
                    .get("value")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        PluginError::Config(format!(
                            "{section_name} item must contain 'value' string field"
                        ))
                    })?;

                let name = HeaderName::from_bytes(name_str.as_bytes()).map_err(|e| {
                    PluginError::Config(format!(
                        "Invalid {section_name} header name '{name_str}': {e}"
                    ))
                })?;
                let spec = ResponseHeaderValueSpec::parse(val_str)?;
                specs.push((name, spec));
            }
        }
    }

    Ok(specs)
}

/// Post-Response Header Mutation Plugin.
#[derive(Debug, Clone)]
pub struct ResponseHeaderMutationPlugin {
    pub id: String,
    pub config: ResponseHeaderMutationConfig,
}

impl ResponseHeaderMutationPlugin {
    /// Constructs a new `ResponseHeaderMutationPlugin` with explicit configuration.
    pub fn new(id: impl Into<String>, config: ResponseHeaderMutationConfig) -> Self {
        Self {
            id: id.into(),
            config,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let defaults = ResponseHeaderMutationConfig::default();

        if config.config.is_null() {
            return Ok(Self::new(&config.id, defaults));
        }

        let raw = config.config.as_object().ok_or_else(|| {
            PluginError::Config(format!("Plugin '{}' config must be an object", config.id))
        })?;

        // 1. Parse 'remove' list
        let mut remove = Vec::new();
        if let Some(rem_arr) = raw
            .get("remove")
            .or_else(|| raw.get("remove_headers"))
            .and_then(|v| v.as_array())
        {
            for item in rem_arr {
                if let Some(s) = item.as_str() {
                    let name = HeaderName::from_bytes(s.as_bytes()).map_err(|e| {
                        PluginError::Config(format!("Invalid remove header name '{s}': {e}"))
                    })?;
                    remove.push(name);
                }
            }
        }

        // 2. Parse 'set' specs
        let set = if let Some(set_val) = raw.get("set").or_else(|| raw.get("set_headers")) {
            parse_specs(set_val, "set")?
        } else {
            Vec::new()
        };

        // 3. Parse 'add' / 'append' specs
        let add = if let Some(add_val) = raw
            .get("add")
            .or_else(|| raw.get("append_headers"))
            .or_else(|| raw.get("append"))
        {
            parse_specs(add_val, "add")?
        } else {
            Vec::new()
        };

        // 4. Parse 'set_if_missing' specs
        let set_if_missing = if let Some(sim_val) = raw.get("set_if_missing") {
            parse_specs(sim_val, "set_if_missing")?
        } else {
            Vec::new()
        };

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

impl PostResponsePlugin for ResponseHeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, _req_headers: &http::HeaderMap, resp: &mut L7Response) {
        // 1. Remove explicitly requested headers
        for name in &self.config.remove {
            resp.headers.remove(name);
        }

        // 2. Set or overwrite headers
        for (name, spec) in &self.config.set {
            if let Some(val) = spec.resolve(resp) {
                resp.headers.insert(name.clone(), val);
            }
        }

        // 3. Append multi-value headers
        for (name, spec) in &self.config.add {
            if let Some(val) = spec.resolve(resp) {
                resp.headers.append(name.clone(), val);
            }
        }

        // 4. Set headers ONLY if not already present
        for (name, spec) in &self.config.set_if_missing {
            if !resp.headers.contains_key(name)
                && let Some(val) = spec.resolve(resp)
            {
                resp.headers.insert(name.clone(), val);
            }
        }
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, StatusCode};
    use velda_core::L7Response;

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

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{}");
        resp.headers = headers;
        let req_headers = HeaderMap::new();

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

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("req-999"));

        let mut resp = L7Response::from_bytes(StatusCode::CREATED, "{}");
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        assert_eq!(resp.headers.get("server").unwrap(), "velda-edge");
        assert_eq!(resp.headers.get("x-request-id").unwrap(), "req-999");
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

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{}");
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        // x-content-type-options was missing -> inserted
        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        // x-existing was already present -> NOT overwritten!
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

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{}");
        resp.headers = headers;
        let req_headers = HeaderMap::new();

        plugin.on_response(&req_headers, &mut resp);

        let vary_values: Vec<&str> = resp
            .headers
            .get_all("vary")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();

        assert_eq!(vary_values, vec!["Accept-Encoding", "User-Agent"]);
    }
}
