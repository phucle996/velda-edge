//! Request Termination Configuration Validation (`request_termination::validation`).
//!
//! ```text
//!   serde_json::Value
//!     └──► serde(deny_unknown_fields) -> RequestTerminationConfig
//!            ├── status_code in 200..=599 & valid StatusCode
//!            ├── body vs content_type parity (204/205/304 require both empty;
//!            │     non-empty body requires content_type; empty body requires empty content_type)
//!            └── headers: RFC 9110 tchar, no case-insensitive dups,
//!                  forbid framing/hop-by-hop/content-type/x-request-id, valid HeaderValue
//! ```

use std::collections::HashSet;

use http::StatusCode;
use http::header::{HeaderName, HeaderValue};

pub use super::config::RequestTerminationConfig;
use super::config::is_bodyless_status;
use crate::error::PluginError;

const MAX_HEADER_NAME_LEN: usize = 256;
const MAX_HEADER_VALUE_LEN: usize = 8192;

fn validate_header_name(plugin_id: &str, name: &str) -> Result<(), PluginError> {
    if name.is_empty() || name.len() > MAX_HEADER_NAME_LEN {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': header name '{name}' must be 1..={MAX_HEADER_NAME_LEN} bytes"
        )));
    }
    if name.starts_with([' ', '\t']) || name.ends_with([' ', '\t']) {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': header name '{name}' must not have leading or trailing whitespace"
        )));
    }
    if name.starts_with(':') {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': pseudo-header '{name}' is forbidden"
        )));
    }
    if HeaderName::from_bytes(name.as_bytes()).is_err() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': invalid RFC 9110 header name '{name}'"
        )));
    }
    Ok(())
}

fn validate_header_value(plugin_id: &str, field: &str, value: &str) -> Result<(), PluginError> {
    if value.len() > MAX_HEADER_VALUE_LEN {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': value for '{field}' exceeds {MAX_HEADER_VALUE_LEN} bytes"
        )));
    }
    if value
        .bytes()
        .any(|b| b == b'\r' || b == b'\n' || (b < 0x20 && b != b'\t') || b == 0x7F)
    {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': value for '{field}' contains illegal control characters"
        )));
    }
    if HeaderValue::from_str(value).is_err() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': invalid HTTP header value for '{field}'"
        )));
    }
    Ok(())
}

fn is_forbidden_response_header(lower_name: &str) -> bool {
    matches!(
        lower_name,
        "content-length"
            | "content-type"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "upgrade"
            | "te"
            | "trailer"
            | "x-request-id"
    )
}

/// Validates a typed [`RequestTerminationConfig`] before compiling into the hot-path plugin.
pub(crate) fn validate_config_fields(
    plugin_id: &str,
    cfg: &RequestTerminationConfig,
) -> Result<(), PluginError> {
    if !(200..=599).contains(&cfg.status_code) || StatusCode::from_u16(cfg.status_code).is_err() {
        return Err(PluginError::Config(format!(
            "Plugin '{plugin_id}': status_code ({}) must be a valid HTTP status in 200..=599",
            cfg.status_code
        )));
    }

    if is_bodyless_status(cfg.status_code) {
        if !cfg.body.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': status_code {} cannot carry a response body",
                cfg.status_code
            )));
        }
        if !cfg.content_type.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': status_code {} cannot declare content_type",
                cfg.status_code
            )));
        }
    } else if cfg.body.is_empty() {
        if !cfg.content_type.is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': content_type must be empty when body is empty"
            )));
        }
    } else {
        if cfg.content_type.trim().is_empty() {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': content_type is required when body is non-empty"
            )));
        }
        validate_header_value(plugin_id, "content_type", &cfg.content_type)?;
    }

    let mut seen_lower = HashSet::with_capacity(cfg.headers.len());
    for (name, value) in &cfg.headers {
        validate_header_name(plugin_id, name)?;
        let lower = name.to_ascii_lowercase();
        if is_forbidden_response_header(&lower) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': header '{name}' is managed by Velda Edge or forbidden"
            )));
        }
        if !seen_lower.insert(lower) {
            return Err(PluginError::Config(format!(
                "Plugin '{plugin_id}': duplicate header '{name}' (case-insensitive)"
            )));
        }
        validate_header_value(plugin_id, name, value)?;
    }

    Ok(())
}

/// Validates and parses JSON config for `request_termination`.
pub fn validate_request_termination_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<RequestTerminationConfig, PluginError> {
    let cfg: RequestTerminationConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| {
            PluginError::Config(format!(
                "Plugin '{plugin_id}' has malformed or incomplete request_termination config: {e}"
            ))
        })?;

    validate_config_fields(plugin_id, &cfg)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_request_termination() {
        let val = json!({
            "status_code": 503,
            "body": "{\"error\":\"maintenance\"}",
            "content_type": "application/json",
            "headers": {
                "Retry-After": "300"
            }
        });
        assert!(validate_request_termination_config("term_1", &val).is_ok());

        let empty_204 = json!({
            "status_code": 204,
            "body": "",
            "content_type": "",
            "headers": {}
        });
        assert!(validate_request_termination_config("term_204", &empty_204).is_ok());
    }

    #[test]
    fn test_unknown_and_missing_fields_rejected() {
        let unknown = json!({
            "status_code": 503,
            "body": "down",
            "content_type": "text/plain",
            "headers": {},
            "bypass_headers": []
        });
        assert!(validate_request_termination_config("term_unknown", &unknown).is_err());

        let missing = json!({ "status_code": 404 });
        assert!(
            validate_request_termination_config("term_missing", &missing)
                .unwrap_err()
                .to_string()
                .contains("missing field")
        );
    }

    #[test]
    fn test_invalid_status_and_bodyless_rules() {
        for bad_status in [199, 600] {
            let val = json!({
                "status_code": bad_status,
                "body": "",
                "content_type": "",
                "headers": {}
            });
            assert!(validate_request_termination_config("term_status", &val).is_err());
        }

        for status in [204, 205, 304] {
            let with_body = json!({
                "status_code": status,
                "body": "nope",
                "content_type": "text/plain",
                "headers": {}
            });
            assert!(validate_request_termination_config("term_bodyless", &with_body).is_err());
        }
    }

    #[test]
    fn test_body_content_type_parity_and_header_hygiene() {
        let missing_ct = json!({
            "status_code": 200,
            "body": "ok",
            "content_type": "",
            "headers": {}
        });
        assert!(validate_request_termination_config("term_no_ct", &missing_ct).is_err());

        let extra_ct = json!({
            "status_code": 301,
            "body": "",
            "content_type": "text/plain",
            "headers": { "location": "https://example.com" }
        });
        assert!(validate_request_termination_config("term_extra_ct", &extra_ct).is_err());

        for forbidden in [
            "content-length",
            "Content-Type",
            "transfer-encoding",
            "connection",
            "x-request-id",
        ] {
            let val = json!({
                "status_code": 403,
                "body": "denied",
                "content_type": "text/plain",
                "headers": { forbidden: "x" }
            });
            assert!(validate_request_termination_config("term_forbid", &val).is_err());
        }

        let dup_case = json!({
            "status_code": 503,
            "body": "",
            "content_type": "",
            "headers": {
                "Retry-After": "60",
                "retry-after": "120"
            }
        });
        assert!(validate_request_termination_config("term_dup", &dup_case).is_err());

        let whitespace_name = json!({
            "status_code": 403,
            "body": "",
            "content_type": "",
            "headers": { " x-foo ": "bar" }
        });
        assert!(validate_request_termination_config("term_ws", &whitespace_name).is_err());
    }
}
