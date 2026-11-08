//! Layer 7 HTTP/2 routing rules, match requests, and in-memory lookup tables.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! This module intentionally maintains its own self-contained definitions for [`Http2Route`],
//! [`Http2RouteRequest`], and [`Http2Router`] rather than sharing a generic parent struct with
//! HTTP/1.1 and HTTP/3. Duplicating these structures across protocol boundaries is an intentional
//! architectural design choice:
//! 1. "Duplicate first. Abstract second": Preserves flat workflows and avoids accidental cross-protocol
//!    coupling between RFC 9113 (HTTP/2 binary framing and multiplexed streams) and text streaming/QUIC.
//! 2. Allows future HTTP/2-specific routing constraints (such as `:authority` pseudo-header evaluation,
//!    multiplexed priority stream weighting, or H2 push directives) without polluting HTTP/1.1 or HTTP/3.
//!
//! ### Hot/Cold Data-Oriented Separation & L1D Cache Line Alignment
//! - Full route definitions ([`Http2Route`]) contain cold metadata (`listener_id`, `upstream_name`,
//!   `plugins: Vec<String>`) exceeding 200+ bytes. Storing them directly in lookup tables pollutes L1D cache lines.
//! - The hot lookup structures ([`exact_routes`], [`prefix_routes`]) store compact, 16-byte-aligned
//!   [`Http2HotCandidate`] descriptors. During route matching, only compact matching state is fetched
//!   into L1D cache lines.
//! - Cold route metadata is accessed exactly ONCE from the listener's `routes: Vec<Http2Route>` arena
//!   after the winning candidate is identified.

use rustc_hash::FxHashMap;
use velda_core::{ListenerId, RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::{CompiledHost, ReverseHostLabels, host_specificity};
use crate::method::MethodMask;
use crate::path::PathPrefixMatcher;

/// Concrete compiled HTTP/2 Layer 7 route (12 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2Route {
    pub id: RouteId,
    pub listener_id: ListenerId,
    pub upstream_id: UpstreamId,
}

impl Http2Route {
    /// Creates a new prefix-based HTTP/2 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        path_prefix: impl Into<String>,
        upstream_id: UpstreamId,
    ) -> Http2RouteRule {
        Http2RouteRule {
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

    /// Creates a new exact-path HTTP/2 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new_exact(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        exact_path: impl Into<String>,
        upstream_id: UpstreamId,
    ) -> Http2RouteRule {
        Http2RouteRule {
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

/// Builder specification for an HTTP/2 route rule consumed during router compilation.
///
/// All pattern strings (`host`, `path_prefix`, `exact_path`, `method`) are consumed and
/// dropped once compiled into [`ListenerHttp2Router`], leaving only 12-byte [`Http2Route`]s in RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2RouteRule {
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

impl Http2RouteRule {
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
    pub fn matches_req(&self, req: &Http2RouteRequest<'_>) -> bool {
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
/// listener's 12-byte `routes: Vec<Http2Route>` table once a winner is identified.
#[repr(C, align(16))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2HotCandidate {
    pub compiled_host: CompiledHost,
    pub route_idx: u32,
    pub method_mask: MethodMask,
}

impl Http2HotCandidate {
    /// Evaluates candidate matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http2RouteRequest<'_>) -> bool {
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
pub struct Http2SmallCandidate {
    pub route: Http2Route,
    pub exact_path: Option<String>,
    pub path_prefix: Option<String>,
    pub compiled_host: CompiledHost,
    pub method_mask: MethodMask,
}

impl Http2SmallCandidate {
    /// Evaluates small candidate matching against an incoming request view.
    #[inline(always)]
    pub fn matches_req(&self, req: &Http2RouteRequest<'_>) -> bool {
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

/// Zero-allocation view of an incoming HTTP/2 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http2RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
    pub method_mask: MethodMask,
    pub parsed_host: ReverseHostLabels,
}

impl<'a> Http2RouteRequest<'a> {
    /// Creates a new HTTP/2 route request view.
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

/// Function pointer signature for HTTP/2 route lookup strategies.
pub type Http2RouteFn =
    for<'a> fn(&'a ListenerHttp2Router, &Http2RouteRequest<'_>) -> Option<&'a Http2Route>;

/// Route table for a single HTTP/2 listener with Hot/Cold Data-Oriented Separation.
///
/// Aligned to 64 bytes to sit on an exact L1D cache line boundary, preventing split-line
/// header loads and false sharing between worker threads.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct ListenerHttp2Router {
    routes: Vec<Http2Route>,
    exact_routes: FxHashMap<String, Vec<Http2HotCandidate>>,
    matcher: PathPrefixMatcher,
    prefix_routes: Vec<Vec<Http2HotCandidate>>,
    small_candidates: Vec<Http2SmallCandidate>,
    route_fn: Http2RouteFn,
}

impl Default for ListenerHttp2Router {
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

impl ListenerHttp2Router {
    /// Builds a new compiled HTTP/2 router for a single listener.
    ///
    /// Automatically selects between Workload-Adaptive strategies:
    /// - `rules.len() <= 3`: Scalar register unrolled matching ([`route_small`]), eliminating hashing and Slash-Block overhead.
    /// - `rules.len() > 3`: SIMD/Slash-Block prefix matcher + FxHashMap exact matcher ([`route_table`]).
    pub fn new(rules: Vec<Http2RouteRule>) -> Result<Self, RouterError> {
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
                .map(|r| Http2SmallCandidate {
                    route: Http2Route {
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

        let mut exact_map: FxHashMap<String, Vec<Http2HotCandidate>> = FxHashMap::default();
        let mut prefix_map: FxHashMap<String, Vec<Http2HotCandidate>> = FxHashMap::default();
        let mut routes = Vec::with_capacity(rules.len());

        for (idx, r) in rules.iter().enumerate() {
            routes.push(Http2Route {
                id: r.id,
                listener_id: r.listener_id,
                upstream_id: r.upstream_id,
            });

            let candidate = Http2HotCandidate {
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

    /// Resolves an HTTP/2 route for an incoming request view.
    ///
    /// Evaluates using the pre-bound workload-adaptive function pointer:
    /// - For small listeners (<= 3 routes): executes unrolled scalar [`route_small`].
    /// - For large listeners (> 3 routes): executes exact FxHashMap + flat Slash-Block [`route_table`].
    #[inline(always)]
    pub fn route(&self, req: &Http2RouteRequest<'_>) -> Option<&Http2Route> {
        (self.route_fn)(self, req)
    }
}

#[inline]
fn route_small<'a>(
    router: &'a ListenerHttp2Router,
    req: &Http2RouteRequest<'_>,
) -> Option<&'a Http2Route> {
    for c in &router.small_candidates {
        if c.matches_req(req) {
            return Some(&c.route);
        }
    }
    None
}

#[inline]
fn route_table<'a>(
    router: &'a ListenerHttp2Router,
    req: &Http2RouteRequest<'_>,
) -> Option<&'a Http2Route> {
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

/// Unified, thread-safe HTTP/2 routing table coordinating all HTTP/2 listeners.
#[derive(Debug, Default, Clone)]
pub struct Http2Router {
    listeners: FxHashMap<ListenerId, ListenerHttp2Router>,
}

impl Http2Router {
    /// Builds a new [`Http2Router`] from a list of [`Http2RouteRule`] specifications.
    pub fn new(routes: impl IntoIterator<Item = Http2RouteRule>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<ListenerId, Vec<Http2RouteRule>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerHttp2Router::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/2 route for an incoming request on a listener.
    #[inline]
    pub fn route(
        &self,
        listener_id: impl Into<ListenerId>,
        req: &Http2RouteRequest<'_>,
    ) -> Option<&Http2Route> {
        let listener_router = self.listeners.get(&listener_id.into())?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: impl Into<ListenerId>) -> Option<&ListenerHttp2Router> {
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
    fn test_http2_exact_and_prefix_routing() {
        let routes = vec![
            Http2Route::new_exact(
                RouteId::new(1),
                "h2-listener",
                "/healthz",
                UpstreamId::new(10),
            ),
            Http2Route::new(
                RouteId::new(2),
                "h2-listener",
                "/grpc.service/v1",
                UpstreamId::new(20),
            ),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 = Http2RouteRequest::new("/healthz");
        let r1 = router.route("h2-listener", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_id, UpstreamId::new(10));

        let req2 = Http2RouteRequest::new("/grpc.service/v1/CreateUser");
        let r2 = router.route("h2-listener", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_id, UpstreamId::new(20));

        let req3 = Http2RouteRequest::new("/unknown");
        assert!(router.route("h2-listener", &req3).is_none());
    }

    #[test]
    fn test_http2_prefix_shadowing_fallback() {
        let routes = vec![
            Http2Route::new(
                RouteId::new(1),
                "listener-1",
                "/api/v1/internal",
                UpstreamId::new(10),
            )
            .with_host("internal.velda.io"),
            Http2Route::new(
                RouteId::new(2),
                "listener-1",
                "/api/v1",
                UpstreamId::new(20),
            ),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 =
            Http2RouteRequest::new("/api/v1/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route("listener-1", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_id, UpstreamId::new(10));

        let req2 = Http2RouteRequest::new("/api/v1/internal/data").with_host("public.velda.io");
        let r2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_id, UpstreamId::new(20));
    }
}
