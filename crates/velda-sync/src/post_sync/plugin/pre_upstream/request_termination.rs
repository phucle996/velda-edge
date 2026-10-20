//! Request Termination Configuration Schema & Semantic Validation (`phase: "pre_upstream"`).
//!
//! Enforces a strict declarative contract for short-circuiting requests with synthetic responses.
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! - **Valid HTTP Status**: `status_code` must be an RFC 9110 status code in range 200..=599.
//! - **Valid Headers**: Custom header names must conform to RFC 9110 token grammar with no CRLF in values.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the Request Termination plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestTerminationConfig {
    /// HTTP status code returned to client (200 to 599).
    pub status_code: u16,
    /// Synthetic response body payload.
    pub body: String,
    /// Response Content-Type header value (e.g. "application/json", "text/plain").
    pub content_type: String,
    /// Custom HTTP response headers injected into the response.
    pub headers: HashMap<String, String>,
}

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

/// Validates Request Termination configuration strictly according to production standards.
pub fn validate_request_termination_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<RequestTerminationConfig, SyncError> {
    let cfg: RequestTerminationConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete request_termination config: {e}"
            ),
        })?;

    // 1. Validate status_code
    if !(200..=599).contains(&cfg.status_code) {
        return Err(SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}': status_code ({}) must be between 200 and 599",
                cfg.status_code
            ),
        });
    }

    // 2. Validate custom headers
    for (hname, hval) in &cfg.headers {
        if !is_valid_header_name(hname) {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': invalid custom header name '{hname}' in headers"
                ),
            });
        }
        if hval.bytes().any(|b| b == b'\r' || b == b'\n') {
            return Err(SyncError::Validation {
                domain: "plugins".into(),
                reason: format!(
                    "Plugin '{plugin_id}': header value for '{hname}' contains CRLF control characters"
                ),
            });
        }
    }

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
            "body": "{\"error\": \"Service undergoing scheduled maintenance\"}",
            "content_type": "application/json",
            "headers": {
                "Retry-After": "300"
            }
        });
        let res = validate_request_termination_config("term_1", &val);
        assert!(res.is_ok());
    }

    #[test]
    fn test_invalid_status_code_rejected() {
        let val = json!({
            "status_code": 199,
            "body": "Bad",
            "content_type": "text/plain",
            "headers": {}
        });
        let res = validate_request_termination_config("term_bad_status", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("between 200 and 599"));
    }

    #[test]
    fn test_invalid_header_name_rejected() {
        let val = json!({
            "status_code": 403,
            "body": "Forbidden",
            "content_type": "text/plain",
            "headers": {
                "Bad: Header Name": "test"
            }
        });
        let res = validate_request_termination_config("term_bad_hdr", &val);
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("invalid custom header name")
        );
    }

    #[test]
    fn test_missing_required_fields_rejected() {
        let val = json!({
            "status_code": 404
            // Missing body, content_type, headers
        });
        let res = validate_request_termination_config("term_missing", &val);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("missing field"));
    }
}
