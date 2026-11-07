//! Layer 7 HTTP/1.1 routing rules, match requests, and in-memory lookup tables.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! This module intentionally maintains its own self-contained definitions for [`Http1Route`],
//! [`Http1RouteRequest`], and [`Http1Router`] rather than sharing a generic parent struct with
//! HTTP/2 and HTTP/3. Duplicating these structures across protocol boundaries is an intentional
//! architectural design choice:
//! 1. "Duplicate first. Abstract second": Preserves flat workflows and avoids accidental cross-protocol
//!    coupling between RFC 9112 (HTTP/1.1 text streaming) and binary/QUIC framing.
//! 2. Allows future HTTP/1.1-specific routing constraints (such as connection upgrade rules, hop-by-hop
//!    forwarding directives) without modifying or risking regressions in HTTP/2 or HTTP/3 pipelines.
//!
//! ### Hot/Cold Data-Oriented Separation & L1D Cache Line Alignment
//! - Full route definitions ([`Http1Route`]) contain cold metadata (`listener_id`, `upstream_name`,
//!   `plugins: Vec<String>`) exceeding 200+ bytes. Storing them directly in lookup tables pollutes L1D cache lines.
//! - The hot lookup structures ([`exact_routes`], [`prefix_routes`]) store compact, 16-byte-aligned
//!   [`Http1HotCandidate`] descriptors. During route matching, only compact matching state is fetched
//!   into L1D cache lines.
//! - Cold route metadata is accessed exactly ONCE from the listener's `routes: Vec<Http1Route>` arena
//!   after the winning candidate is identified.

use rustc_hash::FxHashMap;
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::{CompiledHost, ReverseHostLabels, host_specificity};
use crate::method::MethodMask;
use crate::path::PathPrefixMatcher;

/// Concrete compiled HTTP/1.1 Layer 7 routing rule (cold data arena).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http1Route {
    pub id: RouteId,
    pub listener_id: String,
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub exact_path: Option<String>,
    pub method: Option<String>,
    pub upstream_id: UpstreamId,
    pub upstream_name: String,
    pub plugins: Vec<String>,
    pub method_mask: MethodMask,
    pub compiled_host: CompiledHost,
}

impl Http1Route {
    /// Creates a new prefix-based HTTP/1.1 route rule.
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
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }

    /// Creates a new exact-path HTTP/1.1 route rule.
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
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }

    /// Attaches an expected host constraint (e.g. `"api.velda.io"` or `"*.velda.io"`).
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        let h = host.into();
        self.compiled_host = CompiledHost::compile(Some(&h));
        self.host = Some(h);
        self
    }

    /// Attaches an expected HTTP method constraint (e.g. `"GET"` or `"POST"`).
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        let m = method.into();
        self.method_mask = MethodMask::from_str_case_insensitive(&m);
        self.method = Some(m);
        self
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Checks whether this route matches the given parsed request host.
    #[inline]
    pub fn matches_host(&self, host: &ReverseHostLabels) -> bool {
        self.compiled_host.matches(host)
    }

    /// Checks whether this route matches the given request method mask.
    #[inline]
    pub fn matches_method(&self, req_mask: MethodMask) -> bool {
        self.method_mask.matches(req_mask)
    }

    /// Evaluates both method and host matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http1RouteRequest<'_>) -> bool {
        if !self.method_mask.matches(req.method_mask) {
            return false;
        }
        if self.compiled_host == CompiledHost::Any {
            return true;
        }
        if req.parsed_host.count > 0 || req.parsed_host.is_ip_or_unbracketed {
            self.compiled_host.matches(&req.parsed_host)
        } else if let Some(h) = req.host {
            self.compiled_host.matches(&ReverseHostLabels::parse(h))
        } else {
            false
        }
    }
}

/// Compact, 16-byte-aligned hot candidate descriptor for L1D cache line residency.
///
/// Contains ONLY the fields required during the hot matching loop, keeping cold
/// metadata (`String` URLs, `upstream_name`, `plugins`) in the listener's cold `routes` arena.
#[repr(C, align(16))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http1HotCandidate {
    pub compiled_host: CompiledHost,
    pub route_idx: u32,
    pub method_mask: MethodMask,
}

impl Http1HotCandidate {
    /// Evaluates candidate matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http1RouteRequest<'_>) -> bool {
        if !self.method_mask.matches(req.method_mask) {
            return false;
        }
        if self.compiled_host == CompiledHost::Any {
            return true;
        }
        if req.parsed_host.count > 0 || req.parsed_host.is_ip_or_unbracketed {
            self.compiled_host.matches(&req.parsed_host)
        } else if let Some(h) = req.host {
            self.compiled_host.matches(&ReverseHostLabels::parse(h))
        } else {
            false
        }
    }
}

/// Zero-allocation view of an incoming HTTP/1.1 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http1RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
    pub method_mask: MethodMask,
    pub parsed_host: ReverseHostLabels,
}

impl<'a> Http1RouteRequest<'a> {
    /// Creates a new HTTP/1.1 route request view.
    #[inline]
    pub fn new(path: &'a str) -> Self {
        Self {
            path,
            host: None,
            method: None,
            method_mask: MethodMask::ANY,
            parsed_host: ReverseHostLabels::default(),
        }
    }

    /// Attaches a host view.
    #[inline]
    pub fn with_host(mut self, host: &'a str) -> Self {
        self.host = Some(host);
        self.parsed_host = ReverseHostLabels::parse(host);
        self
    }

    /// Attaches an HTTP method view.
    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self.method_mask = MethodMask::from_str_case_insensitive(method);
        self
    }
}

/// Route table for a single HTTP/1.1 listener with Hot/Cold Data-Oriented Separation.
///
/// Aligned to 64 bytes to sit on an exact L1D cache line boundary, preventing split-line
/// header loads and false sharing between worker threads.
#[repr(C, align(64))]
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp1Router {
    routes: Vec<Http1Route>,
    exact_routes: FxHashMap<String, Vec<Http1HotCandidate>>,
    matcher: PathPrefixMatcher,
    prefix_routes: Vec<Vec<Http1HotCandidate>>,
}

impl ListenerHttp1Router {
    /// Builds a new compiled HTTP/1.1 router for a single listener.
    pub fn new(routes: Vec<Http1Route>) -> Result<Self, RouterError> {
        let mut exact_map: FxHashMap<String, Vec<Http1HotCandidate>> = FxHashMap::default();
        let mut prefix_map: FxHashMap<String, Vec<Http1HotCandidate>> = FxHashMap::default();

        for (idx, r) in routes.iter().enumerate() {
            let candidate = Http1HotCandidate {
                compiled_host: r.compiled_host.clone(),
                route_idx: idx as u32,
                method_mask: r.method_mask,
            };

            if let Some(ref exact) = r.exact_path {
                exact_map.entry(exact.clone()).or_default().push(candidate);
            } else if let Some(ref prefix) = r.path_prefix
                && !prefix.is_empty()
            {
                prefix_map
                    .entry(prefix.clone())
                    .or_default()
                    .push(candidate);
            }
        }

        for list in exact_map.values_mut() {
            list.sort_by_key(|c| {
                let r = &routes[c.route_idx as usize];
                std::cmp::Reverse(host_specificity(r.host.as_deref()))
            });
        }
        for list in prefix_map.values_mut() {
            list.sort_by_key(|c| {
                let r = &routes[c.route_idx as usize];
                std::cmp::Reverse(host_specificity(r.host.as_deref()))
            });
        }

        let mut matcher = PathPrefixMatcher::new();
        let mut prefix_routes = Vec::with_capacity(prefix_map.len());

        for (pattern, c_list) in prefix_map {
            let idx = prefix_routes.len();
            prefix_routes.push(c_list);
            matcher.insert(&pattern, idx);
        }
        matcher.compile();

        Ok(Self {
            routes,
            exact_routes: exact_map,
            matcher,
            prefix_routes,
        })
    }

    /// Resolves an HTTP/1.1 route for an incoming request view.
    ///
    /// Evaluates Tier 1 exact matches first ($O(1)$), followed by Tier 2 prefix matching via flat Slash-Block table ($O(P)$).
    /// Prefix matching uses longest-prefix-first ordering with automatic fallback to shorter prefixes when host or method filters fail.
    #[inline]
    pub fn route(&self, req: &Http1RouteRequest<'_>) -> Option<&Http1Route> {
        // Hardware temporal L1 prefetch: warm prefix table into L1D cache in parallel
        if let Some(first_node) = self.matcher.nodes().first() {
            crate::path::prefetch_l1_keep(first_node as *const _ as *const u8);
        }

        // Tier 1: Exact match lookup (O(1))
        if let Some(candidates) = self.exact_routes.get(req.path)
            && let Some(c) = candidates.iter().find(|c| c.matches_req(req))
        {
            return self.routes.get(c.route_idx as usize);
        }

        // Tier 2: Prefix match lookup via flat Slash-Block table (O(P)) with longest-prefix-first fallback
        let mut matches = [0usize; 32];
        let count = self.matcher.match_prefixes(req.path, &mut matches);

        // Iterate reverse: longest prefix match is at the end of matches buffer
        for &pattern_idx in matches[..count].iter().rev() {
            if let Some(candidates) = self.prefix_routes.get(pattern_idx)
                && let Some(c) = candidates.iter().find(|c| c.matches_req(req))
            {
                return self.routes.get(c.route_idx as usize);
            }
        }

        None
    }
}

/// Unified, thread-safe HTTP/1.1 routing table coordinating all HTTP/1.1 listeners.
#[derive(Debug, Default, Clone)]
pub struct Http1Router {
    listeners: FxHashMap<String, ListenerHttp1Router>,
}

impl Http1Router {
    /// Builds a new [`Http1Router`] from a list of compiled [`Http1Route`] rules.
    pub fn new(routes: impl IntoIterator<Item = Http1Route>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<String, Vec<Http1Route>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerHttp1Router::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/1.1 route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &Http1RouteRequest<'_>) -> Option<&Http1Route> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: &str) -> Option<&ListenerHttp1Router> {
        self.listeners.get(listener_id)
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
    fn test_http1_exact_and_prefix_routing() {
        let routes = vec![
            Http1Route::new_exact(
                RouteId::new(1),
                "listener-1",
                "/healthz",
                UpstreamId::new(10),
                "health-upstream",
            ),
            Http1Route::new(
                RouteId::new(2),
                "listener-1",
                "/api/v1",
                UpstreamId::new(20),
                "api-upstream",
            ),
        ];

        let router = Http1Router::new(routes).unwrap();

        let req1 = Http1RouteRequest::new("/healthz");
        let r1 = router.route("listener-1", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "health-upstream");

        let req2 = Http1RouteRequest::new("/api/v1/users/42");
        let r2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "api-upstream");

        let req3 = Http1RouteRequest::new("/unknown");
        assert!(router.route("listener-1", &req3).is_none());
    }

    #[test]
    fn test_http1_prefix_shadowing_fallback() {
        let routes = vec![
            Http1Route::new(
                RouteId::new(1),
                "listener-1",
                "/api/v1/internal",
                UpstreamId::new(10),
                "internal-upstream",
            )
            .with_host("internal.velda.io"),
            Http1Route::new(
                RouteId::new(2),
                "listener-1",
                "/api/v1",
                UpstreamId::new(20),
                "public-upstream",
            ),
        ];

        let router = Http1Router::new(routes).unwrap();

        // Host matches Route A
        let req1 =
            Http1RouteRequest::new("/api/v1/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route("listener-1", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "internal-upstream");

        // Host mismatch on Route A falls back to Route B
        let req2 = Http1RouteRequest::new("/api/v1/internal/data").with_host("public.velda.io");
        let r2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "public-upstream");
    }

    #[test]
    fn test_http1_exact_host_precedence_over_wildcard() {
        // Register wildcard route FIRST, then exact route SECOND
        let routes = vec![
            Http1Route::new(
                RouteId::new(1),
                "listener-1",
                "/api",
                UpstreamId::new(10),
                "wildcard-upstream",
            )
            .with_host("*.velda.io"),
            Http1Route::new(
                RouteId::new(2),
                "listener-1",
                "/api",
                UpstreamId::new(20),
                "exact-upstream",
            )
            .with_host("api.velda.io"),
        ];

        let router = Http1Router::new(routes).unwrap();

        // Exact match for api.velda.io MUST route to exact-upstream (Route 2), not wildcard-upstream (Route 1)
        let req = Http1RouteRequest::new("/api/data").with_host("api.velda.io");
        let matched = router.route("listener-1", &req).unwrap();
        assert_eq!(matched.id, RouteId::new(2));
        assert_eq!(matched.upstream_name, "exact-upstream");

        // Other subdomains route to wildcard-upstream (Route 1)
        let req2 = Http1RouteRequest::new("/api/data").with_host("billing.velda.io");
        let matched2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(matched2.id, RouteId::new(1));
        assert_eq!(matched2.upstream_name, "wildcard-upstream");
    }
}
