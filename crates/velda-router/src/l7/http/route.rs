//! HTTP Layer 7 routing rules and match requests.

use velda_core::{RouteId, UpstreamId};

/// Concrete compiled HTTP routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpRoute {
    /// Unique identifier of this route.
    pub id: RouteId,
    /// Ingress listener identifier this route attaches to.
    pub listener_id: String,
    /// Optional Host/Domain matching (exact e.g. "api.example.com", or wildcard "*.example.com").
    pub host: Option<String>,
    /// Optional path prefix (e.g. "/api/v1", "/users").
    pub path_prefix: Option<String>,
    /// Optional exact path match (e.g. "/healthz").
    pub exact_path: Option<String>,
    /// Optional HTTP method restriction (e.g. "GET", "POST").
    pub method: Option<String>,
    /// Logical upstream destination identifier.
    pub upstream_id: UpstreamId,
    /// Upstream target name as declared in configuration.
    pub upstream_name: String,
    /// Plugin hook identifiers associated with this route.
    pub plugins: Vec<String>,
}

impl HttpRoute {
    /// Creates a new HTTP route with path prefix.
    pub fn new(
        id: RouteId,
        listener_id: impl Into<String>,
        path_prefix: impl Into<String>,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            host: None,
            path_prefix: Some(path_prefix.into()),
            exact_path: None,
            method: None,
            upstream_id,
            upstream_name: upstream_name.into(),
            plugins: Vec::new(),
        }
    }

    /// Creates a new HTTP route with exact path match.
    pub fn new_exact(
        id: RouteId,
        listener_id: impl Into<String>,
        exact_path: impl Into<String>,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            host: None,
            path_prefix: None,
            exact_path: Some(exact_path.into()),
            method: None,
            upstream_id,
            upstream_name: upstream_name.into(),
            plugins: Vec::new(),
        }
    }

    /// Attaches an exact Host requirement to this route.
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Attaches an exact path requirement to this route.
    pub fn with_exact_path(mut self, path: impl Into<String>) -> Self {
        self.exact_path = Some(path.into());
        self
    }

    /// Attaches a path prefix requirement to this route.
    pub fn with_path_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.path_prefix = Some(prefix.into());
        self
    }

    /// Attaches an HTTP method requirement to this route.
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Checks if this route matches the given request host.
    #[inline]
    pub fn matches_host(&self, host: Option<&str>) -> bool {
        let Some(ref expected_host) = self.host else {
            return true; // No host restriction -> matches any host
        };

        let Some(actual_host) = host else {
            return false;
        };

        // Strip port if present: "api.example.com:8080" -> "api.example.com"
        let actual_clean = actual_host.split(':').next().unwrap_or(actual_host);

        if expected_host == "*" {
            return true;
        }

        if let Some(suffix) = expected_host.strip_prefix("*.") {
            if actual_clean.len() > suffix.len() {
                let dot_idx = actual_clean.len() - suffix.len() - 1;
                if actual_clean.as_bytes()[dot_idx] == b'.' {
                    return actual_clean[dot_idx + 1..].eq_ignore_ascii_case(suffix);
                }
            }
            false
        } else {
            expected_host.eq_ignore_ascii_case(actual_clean)
        }
    }

    /// Checks if this route matches the given HTTP method.
    #[inline]
    pub fn matches_method(&self, method: Option<&str>) -> bool {
        let Some(ref expected) = self.method else {
            return true;
        };
        let Some(actual) = method else {
            return false;
        };
        expected.eq_ignore_ascii_case(actual)
    }
}

/// Zero-allocation view of an incoming HTTP request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct HttpRouteRequest<'a> {
    /// Host / Authority header if present.
    pub host: Option<&'a str>,
    /// Request URI path (e.g. "/api/v1/users").
    pub path: &'a str,
    /// HTTP method (e.g. "GET", "POST").
    pub method: Option<&'a str>,
}

impl<'a> HttpRouteRequest<'a> {
    /// Creates a new match request with the given path.
    #[inline]
    pub fn new(path: &'a str) -> Self {
        Self {
            host: None,
            path,
            method: None,
        }
    }

    /// Attaches the request host/authority.
    #[inline]
    pub fn with_host(mut self, host: &'a str) -> Self {
        self.host = Some(host);
        self
    }

    /// Attaches the request HTTP method.
    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self
    }
}
