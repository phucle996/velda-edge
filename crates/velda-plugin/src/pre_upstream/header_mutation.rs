//! Header Mutation Pre-Upstream Plugin Skeleton (`phase: "pre_upstream"`).
//!
//! Adds, mutates, or strips HTTP headers before proxying requests to upstream backends
//! (e.g., adding `X-Request-Id`, passing tracing contexts, stripping sensitive hop-by-hop headers).

use http::HeaderValue;
use http::header::HeaderName;
use velda_core::{Action, L7Request};

use super::PreUpstreamPlugin;

/// Production skeleton for mutating request headers before upstream dispatch.
#[derive(Debug, Clone)]
pub struct HeaderMutationPlugin {
    pub id: String,
    pub set_headers: Vec<(HeaderName, HeaderValue)>,
    pub remove_headers: Vec<HeaderName>,
}

impl HeaderMutationPlugin {
    /// Constructs a new header mutation plugin.
    pub fn new(
        id: impl Into<String>,
        set_headers: Vec<(HeaderName, HeaderValue)>,
        remove_headers: Vec<HeaderName>,
    ) -> Self {
        Self {
            id: id.into(),
            set_headers,
            remove_headers,
        }
    }
}

impl PreUpstreamPlugin for HeaderMutationPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        for header in &self.remove_headers {
            req.headers.remove(header);
        }

        for (name, val) in &self.set_headers {
            req.headers.insert(name.clone(), val.clone());
        }

        Action::Continue
    }
}
