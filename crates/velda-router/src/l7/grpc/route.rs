//! gRPC Layer 7 routing rules and match requests.

use velda_core::{RouteId, UpstreamId};

/// Concrete compiled gRPC routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRoute {
    /// Unique identifier of this route.
    pub id: RouteId,
    /// Ingress listener identifier this route attaches to.
    pub listener_id: String,
    /// Fully qualified gRPC service name (e.g. "order.OrderService").
    pub service: String,
    /// Optional specific RPC method name (e.g. "CreateOrder"). If `None`, matches all methods.
    pub method: Option<String>,
    /// Optional authority / Host restriction (exact e.g. "grpc.example.com" or wildcard "*.example.com").
    pub authority: Option<String>,
    /// Logical upstream destination identifier.
    pub upstream_id: UpstreamId,
    /// Upstream target name as declared in configuration.
    pub upstream_name: String,
    /// Plugin hook identifiers associated with this route.
    pub plugins: Vec<String>,
}

impl GrpcRoute {
    /// Creates a new gRPC route matching a specific service.
    pub fn new(
        id: RouteId,
        listener_id: impl Into<String>,
        service: impl Into<String>,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        let mut svc = service.into();
        // Normalize leading or trailing slashes
        if svc.starts_with('/') {
            svc = svc.trim_start_matches('/').to_string();
        }
        if svc.ends_with('/') {
            svc = svc.trim_end_matches('/').to_string();
        }

        Self {
            id,
            listener_id: listener_id.into(),
            service: svc,
            method: None,
            authority: None,
            upstream_id,
            upstream_name: upstream_name.into(),
            plugins: Vec::new(),
        }
    }

    /// Attaches an RPC method restriction to this route.
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Attaches an authority requirement to this route.
    pub fn with_authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = Some(authority.into());
        self
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Checks if this route matches the given authority.
    #[inline]
    pub fn matches_authority(&self, authority: Option<&str>) -> bool {
        let Some(ref expected) = self.authority else {
            return true;
        };
        let Some(actual) = authority else {
            return false;
        };

        let actual_clean = actual.split(':').next().unwrap_or(actual);

        if expected == "*" {
            return true;
        }

        if let Some(suffix) = expected.strip_prefix("*.") {
            if actual_clean.len() > suffix.len() {
                let dot_idx = actual_clean.len() - suffix.len() - 1;
                if actual_clean.as_bytes()[dot_idx] == b'.' {
                    return actual_clean[dot_idx + 1..].eq_ignore_ascii_case(suffix);
                }
            }
            false
        } else {
            expected.eq_ignore_ascii_case(actual_clean)
        }
    }

    /// Checks if this route matches the given RPC method.
    #[inline]
    pub fn matches_method(&self, method: Option<&str>) -> bool {
        let Some(ref exp_method) = self.method else {
            return true; // No method restriction -> matches all methods in service
        };
        let Some(act_method) = method else {
            return false;
        };
        exp_method == act_method
    }
}

/// Zero-allocation view of an incoming gRPC request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct GrpcRouteRequest<'a> {
    /// Authority / :authority pseudo-header if present.
    pub authority: Option<&'a str>,
    /// Fully qualified service name (e.g. "order.OrderService").
    pub service: &'a str,
    /// Method name (e.g. "CreateOrder").
    pub method: Option<&'a str>,
}

impl<'a> GrpcRouteRequest<'a> {
    /// Creates a match request directly from service and method.
    #[inline]
    pub fn new(service: &'a str, method: Option<&'a str>) -> Self {
        Self {
            authority: None,
            service,
            method,
        }
    }

    /// Attaches authority to this match request.
    #[inline]
    pub fn with_authority(mut self, authority: &'a str) -> Self {
        self.authority = Some(authority);
        self
    }

    /// Parses a standard gRPC HTTP/2 URI path (e.g. "/package.Service/Method") into a [`GrpcRouteRequest`].
    ///
    /// Performs zero allocations.
    #[inline]
    pub fn from_path(path: &'a str, authority: Option<&'a str>) -> Option<Self> {
        let trimmed = path.strip_prefix('/')?;
        let (service, method) = trimmed.split_once('/')?;
        Some(Self {
            authority,
            service,
            method: Some(method),
        })
    }
}
