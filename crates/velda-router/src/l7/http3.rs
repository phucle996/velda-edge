//! Layer 7 HTTP/3 routing rules, match requests, and in-memory lookup tables.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! This module intentionally maintains its own self-contained definitions for [`Http3Route`],
//! [`Http3RouteRequest`], and [`Http3Router`] rather than sharing a generic parent struct with
//! HTTP/1.1 and HTTP/2. Duplicating these structures across protocol boundaries is an intentional
//! architectural design choice:
//! 1. "Duplicate first. Abstract second": Preserves flat workflows and avoids accidental cross-protocol
//!    coupling between RFC 9114 (HTTP/3 over QUIC datagrams) and TCP-based text/binary streaming.
//! 2. Allows future HTTP/3-specific routing constraints (such as 0-RTT early data routing, QUIC connection ID
//!    affinity, or datagram frame forwarding) without polluting HTTP/1.1 or HTTP/2.
//!
//! ### Hot/Cold Data-Oriented Separation & L1D Cache Line Alignment
//! - Full route definitions ([`Http3Route`]) contain cold metadata (`listener_id`, `upstream_name`,
//!   `plugins: Vec<String>`) exceeding 200+ bytes. Storing them directly in lookup tables pollutes L1D cache lines.
//! - The hot lookup structures ([`exact_routes`], [`prefix_routes`]) store compact, 16-byte-aligned
//!   [`Http3HotCandidate`] descriptors. During route matching, only compact matching state is fetched
//!   into L1D cache lines.
//! - Cold route metadata is accessed exactly ONCE from the listener's `routes: Vec<Http3Route>` arena
//!   after the winning candidate is identified.

use rustc_hash::FxHashMap;
use velda_core::{ListenerId, RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::{CompiledHost, ReverseHostLabels, host_specificity};
use crate::method::MethodMask;
use crate::path::PathPrefixMatcher;

/// Concrete compiled HTTP/3 Layer 7 route (12 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3Route {
    pub id: RouteId,
    pub listener_id: ListenerId,
    pub upstream_id: UpstreamId,
}

impl Http3Route {
    /// Creates a new prefix-based HTTP/3 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        path_prefix: impl Into<String>,
        upstream_id: UpstreamId,
    ) -> Http3RouteRule {
        Http3RouteRule {
            id,
            listener_id: listener_id.into(),
            host: None,
            path_prefix: Some(path_prefix.into()),
            exact_path: None,
            method: None,
            upstream_id,
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }

    /// Creates a new exact-path HTTP/3 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new_exact(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        exact_path: impl Into<String>,
        upstream_id: UpstreamId,
    ) -> Http3RouteRule {
        Http3RouteRule {
            id,
            listener_id: listener_id.into(),
            host: None,
            path_prefix: None,
            exact_path: Some(exact_path.into()),
            method: None,
            upstream_id,
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }
}

/// Builder specification for an HTTP/3 route rule consumed during router compilation.
///
/// All pattern strings (`host`, `path_prefix`, `exact_path`, `method`) are consumed and
/// dropped once compiled into [`ListenerHttp3Router`], leaving only 12-byte [`Http3Route`]s in RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3RouteRule {
    pub id: RouteId,
    pub listener_id: ListenerId,
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub exact_path: Option<String>,
    pub method: Option<String>,
    pub upstream_id: UpstreamId,
    pub method_mask: MethodMask,
    pub compiled_host: CompiledHost,
}

impl Http3RouteRule {
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

    /// Checks whether this route rule matches the given parsed request host.
    #[inline]
    pub fn matches_host(&self, host: &ReverseHostLabels) -> bool {
        self.compiled_host.matches(host)
    }

    /// Checks whether this route rule matches the given request method mask.
    #[inline]
    pub fn matches_method(&self, req_mask: MethodMask) -> bool {
        self.method_mask.matches(req_mask)
    }

    /// Evaluates both method and host matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http3RouteRequest<'_>) -> bool {
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
/// Contains ONLY the fields required during the hot matching loop, indexing into the
/// listener's 12-byte `routes: Vec<Http3Route>` table once a winner is identified.
#[repr(C, align(16))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3HotCandidate {
    pub compiled_host: CompiledHost,
    pub route_idx: u32,
    pub method_mask: MethodMask,
}

impl Http3HotCandidate {
    /// Evaluates candidate matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http3RouteRequest<'_>) -> bool {
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

/// Compact candidate descriptor for listeners with small route tables (<= 3 routes).
///
/// Eliminates FxHashMap hashing and flat Slash-Block table traversals, resolving routes
/// via linear register-unrolled evaluation in GPRs (%rax, %rcx, %rdx).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3SmallCandidate {
    pub route: Http3Route,
    pub exact_path: Option<String>,
    pub path_prefix: Option<String>,
    pub compiled_host: CompiledHost,
    pub method_mask: MethodMask,
}

impl Http3SmallCandidate {
    /// Evaluates small candidate matching against an incoming request view.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http3RouteRequest<'_>) -> bool {
        if !self.method_mask.matches(req.method_mask) {
            return false;
        }
        if self.compiled_host != CompiledHost::Any {
            let host_matches = if req.parsed_host.count > 0 || req.parsed_host.is_ip_or_unbracketed
            {
                self.compiled_host.matches(&req.parsed_host)
            } else if let Some(h) = req.host {
                self.compiled_host.matches(&ReverseHostLabels::parse(h))
            } else {
                false
            };
            if !host_matches {
                return false;
            }
        }
        if let Some(ref exact) = self.exact_path {
            if req.path != exact.as_str() {
                return false;
            }
        } else if let Some(ref prefix) = self.path_prefix
            && !req.path.starts_with(prefix.as_str())
        {
            return false;
        }
        true
    }
}

/// Zero-allocation view of an incoming HTTP/3 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http3RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
    pub method_mask: MethodMask,
    pub parsed_host: ReverseHostLabels,
}

impl<'a> Http3RouteRequest<'a> {
    /// Creates a new HTTP/3 route request view.
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

/// Function pointer signature for HTTP/3 route lookup strategies.
pub type Http3RouteFn =
    for<'a> fn(&'a ListenerHttp3Router, &Http3RouteRequest<'_>) -> Option<&'a Http3Route>;

/// Route table for a single HTTP/3 listener with Hot/Cold Data-Oriented Separation.
///
/// Aligned to 64 bytes to sit on an exact L1D cache line boundary, preventing split-line
/// header loads and false sharing between worker threads.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct ListenerHttp3Router {
    routes: Vec<Http3Route>,
    exact_routes: FxHashMap<String, Vec<Http3HotCandidate>>,
    matcher: PathPrefixMatcher,
    prefix_routes: Vec<Vec<Http3HotCandidate>>,
    small_candidates: Vec<Http3SmallCandidate>,
    route_fn: Http3RouteFn,
}

impl Default for ListenerHttp3Router {
    fn default() -> Self {
        Self {
            routes: Vec::new(),
            exact_routes: FxHashMap::default(),
            matcher: PathPrefixMatcher::default(),
            prefix_routes: Vec::new(),
            small_candidates: Vec::new(),
            route_fn: route_small,
        }
    }
}

impl ListenerHttp3Router {
    /// Builds a new compiled HTTP/3 router for a single listener.
    ///
    /// Automatically selects between Workload-Adaptive strategies:
    /// - `rules.len() <= 3`: Scalar register unrolled matching ([`route_small`]), eliminating hashing and Slash-Block overhead.
    /// - `rules.len() > 3`: SIMD/Slash-Block prefix matcher + FxHashMap exact matcher ([`route_table`]).
    pub fn new(rules: Vec<Http3RouteRule>) -> Result<Self, RouterError> {
        if rules.len() <= 3 {
            let mut sorted_rules = rules;
            sorted_rules.sort_by(|a, b| {
                let a_is_exact = a.exact_path.is_some();
                let b_is_exact = b.exact_path.is_some();
                if a_is_exact != b_is_exact {
                    return b_is_exact.cmp(&a_is_exact);
                }
                let a_prefix_len = a.path_prefix.as_ref().map_or(0, |p| p.len());
                let b_prefix_len = b.path_prefix.as_ref().map_or(0, |p| p.len());
                if a_prefix_len != b_prefix_len {
                    return b_prefix_len.cmp(&a_prefix_len);
                }
                let a_host_spec = host_specificity(a.host.as_deref());
                let b_host_spec = host_specificity(b.host.as_deref());
                b_host_spec.cmp(&a_host_spec)
            });

            let small_candidates = sorted_rules
                .into_iter()
                .map(|r| Http3SmallCandidate {
                    route: Http3Route {
                        id: r.id,
                        listener_id: r.listener_id,
                        upstream_id: r.upstream_id,
                    },
                    exact_path: r.exact_path,
                    path_prefix: r.path_prefix,
                    compiled_host: r.compiled_host,
                    method_mask: r.method_mask,
                })
                .collect();

            return Ok(Self {
                routes: Vec::new(),
                exact_routes: FxHashMap::default(),
                matcher: PathPrefixMatcher::default(),
                prefix_routes: Vec::new(),
                small_candidates,
                route_fn: route_small,
            });
        }

        let mut exact_map: FxHashMap<String, Vec<Http3HotCandidate>> = FxHashMap::default();
        let mut prefix_map: FxHashMap<String, Vec<Http3HotCandidate>> = FxHashMap::default();
        let mut routes = Vec::with_capacity(rules.len());

        for (idx, r) in rules.iter().enumerate() {
            routes.push(Http3Route {
                id: r.id,
                listener_id: r.listener_id,
                upstream_id: r.upstream_id,
            });

            let candidate = Http3HotCandidate {
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
                let r = &rules[c.route_idx as usize];
                std::cmp::Reverse(host_specificity(r.host.as_deref()))
            });
        }
        for list in prefix_map.values_mut() {
            list.sort_by_key(|c| {
                let r = &rules[c.route_idx as usize];
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
            small_candidates: Vec::new(),
            route_fn: route_table,
        })
    }

    /// Resolves an HTTP/3 route for an incoming request view.
    ///
    /// Evaluates using the pre-bound workload-adaptive function pointer:
    /// - For small listeners (<= 3 routes): executes unrolled scalar [`route_small`].
    /// - For large listeners (> 3 routes): executes exact FxHashMap + flat Slash-Block [`route_table`].
    #[inline(always)]
    pub fn route(&self, req: &Http3RouteRequest<'_>) -> Option<&Http3Route> {
        (self.route_fn)(self, req)
    }
}

#[inline]
fn route_small<'a>(
    router: &'a ListenerHttp3Router,
    req: &Http3RouteRequest<'_>,
) -> Option<&'a Http3Route> {
    for c in &router.small_candidates {
        if c.matches_req(req) {
            return Some(&c.route);
        }
    }
    None
}

#[inline]
fn route_table<'a>(
    router: &'a ListenerHttp3Router,
    req: &Http3RouteRequest<'_>,
) -> Option<&'a Http3Route> {
    // Hardware temporal L1 prefetch: warm prefix table into L1D cache in parallel
    if let Some(first_node) = router.matcher.nodes().first() {
        crate::path::prefetch_l1_keep(first_node as *const _ as *const u8);
    }

    // Tier 1: Exact match lookup (O(1))
    if let Some(candidates) = router.exact_routes.get(req.path)
        && let Some(c) = candidates.iter().find(|c| c.matches_req(req))
    {
        return router.routes.get(c.route_idx as usize);
    }

    // Tier 2: Prefix match lookup via flat Slash-Block table (O(P)) with longest-prefix-first fallback
    let mut matches = [0usize; 32];
    let count = router.matcher.match_prefixes(req.path, &mut matches);

    // Iterate reverse: longest prefix match is at the end of matches buffer
    for &pattern_idx in matches[..count].iter().rev() {
        if let Some(candidates) = router.prefix_routes.get(pattern_idx)
            && let Some(c) = candidates.iter().find(|c| c.matches_req(req))
        {
            return router.routes.get(c.route_idx as usize);
        }
    }

    None
}

/// Unified, thread-safe HTTP/3 routing table coordinating all HTTP/3 listeners.
#[derive(Debug, Default, Clone)]
pub struct Http3Router {
    listeners: FxHashMap<ListenerId, ListenerHttp3Router>,
}

impl Http3Router {
    /// Builds a new [`Http3Router`] from a list of [`Http3RouteRule`] specifications.
    pub fn new(routes: impl IntoIterator<Item = Http3RouteRule>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<ListenerId, Vec<Http3RouteRule>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerHttp3Router::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/3 route for an incoming request on a listener.
    #[inline]
    pub fn route(
        &self,
        listener_id: impl Into<ListenerId>,
        req: &Http3RouteRequest<'_>,
    ) -> Option<&Http3Route> {
        let listener_router = self.listeners.get(&listener_id.into())?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: impl Into<ListenerId>) -> Option<&ListenerHttp3Router> {
        self.listeners.get(&listener_id.into())
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
    fn test_http3_exact_and_prefix_routing() {
        let routes = vec![
            Http3Route::new_exact(
                RouteId::new(1),
                "quic-listener",
                "/healthz",
                UpstreamId::new(10),
            ),
            Http3Route::new(
                RouteId::new(2),
                "quic-listener",
                "/api/v3",
                UpstreamId::new(20),
            ),
        ];

        let router = Http3Router::new(routes).unwrap();

        let req1 = Http3RouteRequest::new("/healthz");
        let r1 = router.route("quic-listener", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_id, UpstreamId::new(10));

        let req2 = Http3RouteRequest::new("/api/v3/orders/42");
        let r2 = router.route("quic-listener", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_id, UpstreamId::new(20));

        let req3 = Http3RouteRequest::new("/unknown");
        assert!(router.route("quic-listener", &req3).is_none());
    }

    #[test]
    fn test_http3_prefix_shadowing_fallback() {
        let routes = vec![
            Http3Route::new(
                RouteId::new(1),
                "listener-1",
                "/api/v3/internal",
                UpstreamId::new(10),
            )
            .with_host("internal.velda.io"),
            Http3Route::new(
                RouteId::new(2),
                "listener-1",
                "/api/v3",
                UpstreamId::new(20),
            ),
        ];

        let router = Http3Router::new(routes).unwrap();

        let req1 =
            Http3RouteRequest::new("/api/v3/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route("listener-1", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_id, UpstreamId::new(10));

        let req2 = Http3RouteRequest::new("/api/v3/internal/data").with_host("public.velda.io");
        let r2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_id, UpstreamId::new(20));
    }
}
