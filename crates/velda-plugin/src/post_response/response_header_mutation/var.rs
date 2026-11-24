//! Dynamic Response Variables and Value Specifications (`response_header_mutation::var`).
//!
//! ```text
//!   Config string ──► ResponseHeaderValueSpec::parse()
//!         ├─► "$$"                        ──► Static("$")
//!         ├─► "$request_id"               ──► Variable(RequestId)
//!         ├─► "$status"                   ──► Variable(Status)
//!         ├─► "$timestamp_epoch"          ──► Variable(TimestampEpoch)
//!         └─► literal text                ──► Static(HeaderValue)
//! ```
//!
//! Dynamic variables (`$request_id`, `$status`, `$timestamp_epoch`) are copied or formatted
//! directly into [`velda_core::buffer::header::HeaderWorkspace`]: `$request_id` clones the
//! incoming `x-request-id` header value directly without re-formatting.

use http::header::HeaderValue;

use crate::error::PluginError;

/// Dynamic runtime variables evaluated against outgoing response and incoming request workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseDynamicVar {
    /// Incoming `x-request-id` header value from `req_headers`.
    RequestId,
    /// HTTP status code as a 16-bit integer (e.g. `200`, `404`).
    Status,
    /// Coarse UNIX epoch timestamp in seconds (`req_headers.ingress_time_sec`).
    TimestampEpoch,
}

/// Specification for response header values: pre-compiled static value or dynamic variable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseHeaderValueSpec {
    /// Static header value pre-computed at configuration compile time.
    Static(HeaderValue),
    /// Dynamic variable resolved per response on the hot path as an arithmetic value.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_status_blob_integrity() {
        let test_codes = [
            StatusCode::OK,
            StatusCode::CREATED,
            StatusCode::NO_CONTENT,
            StatusCode::NOT_FOUND,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ];
        for code in test_codes {
            let hv = velda_core::buffer::header::resolve_u64_header_value(code.as_u16() as u64);
            assert_eq!(hv.to_str().unwrap(), code.as_str());
        }
    }

    #[test]
    fn test_value_spec_parsing() {
        let static_spec = ResponseHeaderValueSpec::parse("velda-edge").unwrap();
        assert_eq!(
            static_spec,
            ResponseHeaderValueSpec::Static(HeaderValue::from_static("velda-edge"))
        );

        let var_spec = ResponseHeaderValueSpec::parse("$request_id").unwrap();
        assert_eq!(
            var_spec,
            ResponseHeaderValueSpec::Variable(ResponseDynamicVar::RequestId)
        );

        let escaped = ResponseHeaderValueSpec::parse("$$").unwrap();
        assert_eq!(
            escaped,
            ResponseHeaderValueSpec::Static(HeaderValue::from_static("$"))
        );

        assert!(ResponseHeaderValueSpec::parse("$unknown_variable").is_err());
    }
}
