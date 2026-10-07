//! Upstream HTTP/2 Header Processing and RFC 9113 Sanitization.

pub use crate::headers::is_disallowed_h2_header;
pub use crate::headers::sanitize_h2_headers;
