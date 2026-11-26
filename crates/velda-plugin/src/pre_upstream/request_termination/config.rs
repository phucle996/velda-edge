//! Request Termination Configuration Schema (`request_termination::config`).
//!
//! ```text
//!   plugins.json ("request_termination")
//!     ├── status_code  : 200..=599 (204/205/304 forbid non-empty body & content_type)
//!     ├── body         : exact payload bytes ("" => Body::Empty)
//!     ├── content_type : required MIME header when body is non-empty; "" when empty
//!     └── headers      : static custom response headers (no hop-by-hop/framing/system keys)
//! ```

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Returns `true` for HTTP status codes that RFC 9110 forbids from carrying a message body.
#[inline(always)]
pub const fn is_bodyless_status(status_code: u16) -> bool {
    matches!(status_code, 204 | 205 | 304)
}

/// Returns `true` for HTTP status codes where RFC 9112 forbids emitting `Content-Length`.
#[inline(always)]
pub const fn forbids_content_length(status_code: u16) -> bool {
    matches!(status_code, 204 | 304)
}

/// Declarative configuration for the Request Termination plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestTerminationConfig {
    /// HTTP status code returned to the client (`200..=599`).
    pub status_code: u16,
    /// Exact synthetic response body payload (`""` produces `Body::Empty`).
    pub body: String,
    /// Response `Content-Type` header value (required when `body` is non-empty, `""` when empty).
    pub content_type: String,
    /// Static custom HTTP response headers added to the synthetic response.
    pub headers: HashMap<String, String>,
}
