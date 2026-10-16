//! Security Headers Post-Response Plugin Skeleton (`phase: "post_response"`).
//!
//! Enforces standard production security headers on all outgoing HTTP responses
//! (e.g., HSTS, X-Content-Type-Options, X-Frame-Options, Referrer-Policy).

use http::HeaderValue;
use http::header::{
    REFERRER_POLICY, STRICT_TRANSPORT_SECURITY, X_CONTENT_TYPE_OPTIONS, X_FRAME_OPTIONS,
};
use velda_core::L7Response;

use super::PostResponsePlugin;

/// Production skeleton for injecting enterprise security headers.
#[derive(Debug, Clone)]
pub struct SecurityHeadersPlugin {
    pub id: String,
    pub hsts_max_age_secs: u64,
    pub include_subdomains: bool,
    pub frame_options: HeaderValue,
}

impl SecurityHeadersPlugin {
    /// Constructs a new security headers plugin with strict defaults.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            hsts_max_age_secs: 31_536_000, // 1 year
            include_subdomains: true,
            frame_options: HeaderValue::from_static("DENY"),
        }
    }
}

impl PostResponsePlugin for SecurityHeadersPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_response(&self, resp: &mut L7Response) {
        let hsts_val = if self.include_subdomains {
            format!("max-age={}; includeSubDomains", self.hsts_max_age_secs)
        } else {
            format!("max-age={}", self.hsts_max_age_secs)
        };

        if let Ok(val) = HeaderValue::from_str(&hsts_val) {
            resp.headers.insert(STRICT_TRANSPORT_SECURITY, val);
        }

        resp.headers
            .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));

        resp.headers
            .insert(X_FRAME_OPTIONS, self.frame_options.clone());

        resp.headers.insert(
            REFERRER_POLICY,
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        );
    }
}
