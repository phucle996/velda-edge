//! Post-Response Execution Phase Plugins (`phase: "post_response"`).
//!
//! Evaluated on upstream responses immediately before encoding and transmitting
//! frames back to the downstream client (e.g., security response headers, compression, response body masking).
//!
//! # Execution Invariants
//! - **Response Mutability**: Plugins in this phase operate directly on outgoing [`L7Response`] headers and status.
//! - **Non-failing**: At this phase, the upstream interaction has completed. Plugins mutate headers or status in-place.

use std::sync::Arc;
use velda_core::L7Response;

pub mod cors;
pub mod security_headers;

pub use cors::CorsResponsePlugin;
pub use security_headers::SecurityHeadersPlugin;

/// Contract for plugins evaluated on outgoing upstream responses.
pub trait PostResponsePlugin: Send + Sync {
    /// Unique identifier of this plugin instance.
    fn id(&self) -> &str;

    /// Evaluated on outgoing response before downstream encoding.
    fn on_response(&self, resp: &mut L7Response);
}

/// Executes a linear chain of post-response plugins on the outgoing response.
#[inline]
pub fn run_post_response_plugins(chain: &[Arc<dyn PostResponsePlugin>], resp: &mut L7Response) {
    for plugin in chain {
        plugin.on_response(resp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_post_response_security_headers_injection() {
        let plugin = Arc::new(SecurityHeadersPlugin::new("sec-1"));
        let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin];

        let mut resp = L7Response::from_bytes(StatusCode::OK, "{\"status\":\"ok\"}");

        run_post_response_plugins(&chain, &mut resp);

        assert!(resp.headers.contains_key("strict-transport-security"));
        assert_eq!(
            resp.headers.get("x-content-type-options").unwrap(),
            "nosniff"
        );
        assert_eq!(resp.headers.get("x-frame-options").unwrap(), "DENY");
        assert_eq!(
            resp.headers.get("referrer-policy").unwrap(),
            "strict-origin-when-cross-origin"
        );
    }

    #[test]
    fn test_post_response_cors_injection() {
        let plugin = Arc::new(CorsResponsePlugin::new(
            "cors-1",
            "https://app.example.com",
            true,
        ));
        let chain: Vec<Arc<dyn PostResponsePlugin>> = vec![plugin];

        let mut resp = L7Response::empty(StatusCode::NO_CONTENT);

        run_post_response_plugins(&chain, &mut resp);

        assert_eq!(
            resp.headers.get("access-control-allow-origin").unwrap(),
            "https://app.example.com"
        );
        assert_eq!(
            resp.headers
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
        assert_eq!(resp.headers.get("vary").unwrap(), "Origin");
    }
}
