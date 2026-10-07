//! Layer 7 gRPC routing rules, match requests, and in-memory lookup tables.

use rustc_hash::FxHashMap;
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::matches_host;

/// Concrete compiled gRPC Layer 7 routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRoute {
    pub id: RouteId,
    pub listener_id: String,
    pub service: String,
    pub method: Option<String>,
    pub authority: Option<String>,
    pub upstream_id: UpstreamId,
    pub upstream_name: String,
    pub plugins: Vec<String>,
}

impl GrpcRoute {
    /// Creates a new gRPC route matching on full service name.
    pub fn new(
        id: RouteId,
        listener_id: impl Into<String>,
        service: impl Into<String>,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            service: service.into(),
            method: None,
            authority: None,
            upstream_id,
            upstream_name: upstream_name.into(),
            plugins: Vec::new(),
        }
    }

    /// Attaches an expected gRPC method name filter.
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Attaches an expected gRPC `:authority` filter (supports wildcard `*` or `*.domain`).
    pub fn with_authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = Some(authority.into());
        self
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Checks whether this route matches the given request authority view.
    #[inline]
    pub fn matches_authority(&self, authority: Option<&str>) -> bool {
        matches_host(self.authority.as_deref(), authority)
    }

    /// Checks whether this route matches the given request method view.
    #[inline]
    pub fn matches_method(&self, method: Option<&str>) -> bool {
        let Some(ref exp_method) = self.method else {
            return true;
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
    pub service: &'a str,
    pub method: Option<&'a str>,
    pub authority: Option<&'a str>,
}

impl<'a> GrpcRouteRequest<'a> {
    /// Creates a new gRPC route request view with service and optional method.
    #[inline]
    pub fn new(service: &'a str, method: Option<&'a str>) -> Self {
        Self {
            service,
            method,
            authority: None,
        }
    }

    /// Parses a gRPC request view from an HTTP/2 `:path` (e.g. `"/helloworld.Greeter/SayHello"`).
    ///
    /// Automatically strips leading slashes and any trailing query/fragment parameters.
    #[inline]
    pub fn from_path(path: &'a str, authority: Option<&'a str>) -> Option<Self> {
        let trimmed = path.strip_prefix('/')?;
        let clean_path = if let Some(idx) = trimmed.find(['?', '#']) {
            &trimmed[..idx]
        } else {
            trimmed
        };
        let mut parts = clean_path.splitn(2, '/');
        let service = parts.next()?;
        if service.is_empty() {
            return None;
        }
        let method = parts.next().filter(|m| !m.is_empty());
        Some(Self {
            service,
            method,
            authority,
        })
    }

    /// Attaches an expected method.
    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self
    }

    /// Attaches an `:authority` header value.
    #[inline]
    pub fn with_authority(mut self, authority: &'a str) -> Self {
        self.authority = Some(authority);
        self
    }
}

/// Route table for a single gRPC listener.
#[derive(Debug, Clone)]
pub struct ListenerGrpcRouter {
    service_routes: FxHashMap<String, Vec<GrpcRoute>>,
    catch_all: Vec<GrpcRoute>,
}

impl ListenerGrpcRouter {
    /// Builds a new compiled gRPC listener routing table.
    pub fn new(routes: Vec<GrpcRoute>) -> Result<Self, RouterError> {
        let mut service_map: FxHashMap<String, Vec<GrpcRoute>> = FxHashMap::default();
        let mut catch_all = Vec::new();

        for r in routes {
            if r.service == "*" || r.service.is_empty() {
                catch_all.push(r);
            } else {
                service_map.entry(r.service.clone()).or_default().push(r);
            }
        }

        for list in service_map.values_mut() {
            list.sort_by_key(|r| {
                std::cmp::Reverse(crate::host::host_specificity(r.authority.as_deref()))
            });
        }
        catch_all.sort_by_key(|r| {
            std::cmp::Reverse(crate::host::host_specificity(r.authority.as_deref()))
        });

        Ok(Self {
            service_routes: service_map,
            catch_all,
        })
    }

    /// Resolves a gRPC route for an incoming request view.
    #[inline]
    pub fn route(&self, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        if let Some(candidates) = self.service_routes.get(req.service)
            && let Some(r) = candidates
                .iter()
                .find(|r| r.matches_authority(req.authority) && r.matches_method(req.method))
        {
            return Some(r);
        }

        self.catch_all
            .iter()
            .find(|r| r.matches_authority(req.authority) && r.matches_method(req.method))
    }
}

/// Unified, thread-safe gRPC routing table coordinating all gRPC listeners.
#[derive(Debug, Default, Clone)]
pub struct GrpcRouter {
    listeners: FxHashMap<String, ListenerGrpcRouter>,
}

impl GrpcRouter {
    /// Builds a new [`GrpcRouter`] from a list of compiled [`GrpcRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = GrpcRoute>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<String, Vec<GrpcRoute>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerGrpcRouter::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves a gRPC route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the number of registered listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_routing_exact_and_catchall() {
        let routes = vec![
            GrpcRoute::new(
                RouteId::new(1),
                "grpc-in",
                "helloworld.Greeter",
                UpstreamId::new(10),
                "greeter-upstream",
            )
            .with_method("SayHello")
            .with_authority("grpc.velda.io"),
            GrpcRoute::new(
                RouteId::new(2),
                "grpc-in",
                "*",
                UpstreamId::new(20),
                "fallback-upstream",
            ),
        ];

        let router = GrpcRouter::new(routes).unwrap();

        // Exact match via from_path
        let req1 = GrpcRouteRequest::from_path(
            "/helloworld.Greeter/SayHello?debug=1",
            Some("grpc.velda.io:50051"),
        )
        .unwrap();
        assert_eq!(req1.service, "helloworld.Greeter");
        assert_eq!(req1.method, Some("SayHello"));

        let r1 = router.route("grpc-in", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "greeter-upstream");

        // Method mismatch falls back to catch-all
        let req2 = GrpcRouteRequest::from_path(
            "/helloworld.Greeter/SayGoodbye",
            Some("grpc.velda.io:50051"),
        )
        .unwrap();
        let r2 = router.route("grpc-in", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "fallback-upstream");
    }
}
