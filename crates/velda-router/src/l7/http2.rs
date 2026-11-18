//! Layer 7 HTTP/2 routing rules, match requests, and in-memory lookup tables.
//!
//! ```text
//!                           HTTP/2 ROUTE MATCHING
//!                                     │
//!                        ┌────────────▼────────────┐
//!                        │ Ephemeral Route Active? │──[HIT]──► Ephemeral Route
//!                        └────────────┬────────────┘
//!                                     │ [NONE]
//!                        ┌────────────▼────────────┐
//!                        │ PathPrefixMatcher (16B) │
//!                        │ 4 Nodes per Cache Line  │
//!                        └────────────┬────────────┘
//!                                     │
//!                     ┌───────────────┴───────────────┐
//!                     ▼                               ▼
//!                [PATH ENDS]                   [PATH CONTINUES]
//!              Exact Leaf First              Longest Prefix Candidate
//!                     │                               │
//!                     └───────────────┬───────────────┘
//!                                     ▼
//!                         Candidate Method & Host Check
//!                                     │
//!                                ┌────┴────┐
//!                         [HIT]  ▼         ▼  [MISS]
//!                           Http2Route   None (404)
//! ```

use rustc_hash::FxHashMap;
use velda_core::{ListenerId, RouteId};

use crate::error::RouterError;
use crate::host::{CompiledHost, ReverseHostLabels, host_specificity};
use crate::method::MethodMask;
use crate::path::PathPrefixMatcher;

/// Concrete compiled HTTP/2 Layer 7 route (8 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2Route {
    pub id: RouteId,
    pub listener_id: ListenerId,
}

impl Http2Route {
    /// Creates a new prefix-based HTTP/2 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        path_prefix: impl Into<String>,
    ) -> Http2RouteRule {
        Http2RouteRule {
            id,
            listener_id: listener_id.into(),
            host: None,
            path_prefix: Some(path_prefix.into()),
            exact_path: None,
            method: None,
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
    ) -> Http2RouteRule {
        Http2RouteRule {
            id,
            listener_id: listener_id.into(),
            host: None,
            path_prefix: None,
            exact_path: Some(exact_path.into()),
            method: None,
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }
}

/// Builder specification for an HTTP/2 route rule consumed during router compilation.
///
/// All pattern strings (`host`, `path_prefix`, `exact_path`, `method`) are consumed and
/// dropped once compiled into [`ListenerHttp2Router`], leaving only 8-byte [`Http2Route`]s in RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2RouteRule {
    pub id: RouteId,
    pub listener_id: ListenerId,
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub exact_path: Option<String>,
    pub method: Option<String>,
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

/// Compact candidate descriptor indexing into `routes: Vec<Http2Route>`.
#[repr(C, align(16))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2HotCandidate {
    pub compiled_host: CompiledHost,
    pub route_idx: u32,
    pub method_mask: MethodMask,
    pub is_prefix: bool,
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

/// Ephemeral exact route registered dynamically (e.g. for ACME challenges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EphemeralHttp2Route {
    pub path: String,
    pub route: Http2Route,
    pub method_mask: MethodMask,
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

/// Route table for a single HTTP/2 listener with Hot/Cold Data-Oriented Separation.
///
/// Aligned to 64 bytes to sit on an exact L1D cache line boundary, preventing split-line
/// header loads and false sharing between worker threads.
#[repr(C, align(64))]
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp2Router {
    routes: Vec<Http2Route>,
    matcher: PathPrefixMatcher,
    pattern_routes: Vec<Vec<Http2HotCandidate>>,
    ephemeral_routes: Vec<EphemeralHttp2Route>,
}

impl ListenerHttp2Router {
    /// Builds a new compiled HTTP/2 router for a single listener.
    ///
    /// Consolidates exact and prefix matching into a single contiguous integer-block
    /// arena ([`PathPrefixMatcher`]), eliminating heap pointer dereferencing and scalar
    /// branch divergence.
    pub fn new(rules: Vec<Http2RouteRule>) -> Result<Self, RouterError> {
        let mut routes = Vec::with_capacity(rules.len());
        let mut pattern_map: FxHashMap<String, Vec<Http2HotCandidate>> = FxHashMap::default();

        for (idx, r) in rules.iter().enumerate() {
            routes.push(Http2Route {
                id: r.id,
                listener_id: r.listener_id,
            });

            let is_prefix = r.exact_path.is_none();
            let pattern = if let Some(ref exact) = r.exact_path {
                exact.clone()
            } else if let Some(ref prefix) = r.path_prefix {
                if prefix.is_empty() {
                    "/".to_string()
                } else {
                    prefix.clone()
                }
            } else {
                "/".to_string()
            };

            let candidate = Http2HotCandidate {
                compiled_host: r.compiled_host.clone(),
                route_idx: idx as u32,
                method_mask: r.method_mask,
                is_prefix,
            };

            pattern_map.entry(pattern).or_default().push(candidate);
        }

        for list in pattern_map.values_mut() {
            list.sort_by(|a, b| {
                // 1. Exact candidates take precedence over prefix candidates
                let a_exact = !a.is_prefix;
                let b_exact = !b.is_prefix;
                if a_exact != b_exact {
                    return b_exact.cmp(&a_exact);
                }
                // 2. Specific host takes precedence over wildcard host
                let r_a = &rules[a.route_idx as usize];
                let r_b = &rules[b.route_idx as usize];
                let spec_a = host_specificity(r_a.host.as_deref());
                let spec_b = host_specificity(r_b.host.as_deref());
                spec_b.cmp(&spec_a)
            });
        }

        let mut matcher = PathPrefixMatcher::new();
        let mut pattern_routes = Vec::with_capacity(pattern_map.len());

        for (pattern, c_list) in pattern_map {
            let idx = pattern_routes.len();
            let has_prefix = c_list.iter().any(|c| c.is_prefix);
            pattern_routes.push(c_list);
            matcher.insert_pattern(&pattern, idx, has_prefix);
        }
        matcher.compile();

        Ok(Self {
            routes,
            matcher,
            pattern_routes,
            ephemeral_routes: Vec::new(),
        })
    }

    /// Resolves an HTTP/2 route for an incoming request view.
    ///
    /// Evaluates using unified contiguous integer-block matching across ALU Port 0/1.
    #[inline(always)]
    pub fn route(&self, req: &Http2RouteRequest<'_>) -> Option<&Http2Route> {
        // 1. Ephemeral route fast check (e.g. ACME challenge)
        if !self.ephemeral_routes.is_empty() {
            for ep in &self.ephemeral_routes {
                if ep.path == req.path && ep.method_mask.matches(req.method_mask) {
                    return Some(&ep.route);
                }
            }
        }

        // 2. Hardware temporal prefetch
        if let Some(first_node) = self.matcher.nodes().first() {
            crate::path::prefetch_l1_keep(first_node as *const _ as *const u8);
        }

        // 3. Contiguous integer matching
        let mut matches = [crate::path::MatchedPattern::default(); 32];
        let count = self.matcher.match_path(req.path, &mut matches);

        for m in matches[..count].iter().rev() {
            if let Some(candidates) = self.pattern_routes.get(m.pattern_idx as usize) {
                for c in candidates {
                    if !m.is_exact && !c.is_prefix {
                        continue;
                    }
                    if c.matches_req(req) {
                        return self.routes.get(c.route_idx as usize);
                    }
                }
            }
        }

        None
    }

    /// Dynamically registers an ephemeral exact-path route into this listener's router.
    ///
    /// Used by background logistics plugins (such as `acme_cert_rotator`) to inject temporary in-memory
    /// endpoints (e.g. `/.well-known/acme-challenge/<token>`) directly into the router table.
    /// Guarantees zero allocation on the serving path without recompiling configuration on disk.
    pub fn insert_exact_route(
        &mut self,
        exact_path: String,
        route: Http2Route,
        method_mask: MethodMask,
    ) {
        self.ephemeral_routes
            .retain(|e| !(e.path == exact_path && e.route.id == route.id));
        self.ephemeral_routes.push(EphemeralHttp2Route {
            path: exact_path,
            route,
            method_mask,
        });
    }

    /// Dynamically removes an ephemeral exact-path route previously registered for this listener.
    ///
    /// Invoked unconditionally upon challenge completion or expiration (via RAII Drop guard)
    /// to guarantee zero memory leakage and restore pristine routing state.
    pub fn remove_exact_route(&mut self, exact_path: &str, route_id: RouteId) {
        self.ephemeral_routes
            .retain(|e| !(e.path == exact_path && e.route.id == route_id));
    }
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

    /// Dynamically registers an ephemeral exact-path route for a specific listener.
    pub fn insert_exact_route(
        &mut self,
        listener_id: impl Into<ListenerId>,
        exact_path: String,
        route: Http2Route,
        method_mask: MethodMask,
    ) {
        let lid = listener_id.into();
        self.listeners
            .entry(lid)
            .or_default()
            .insert_exact_route(exact_path, route, method_mask);
    }

    /// Dynamically removes an ephemeral exact-path route for a specific listener.
    pub fn remove_exact_route(
        &mut self,
        listener_id: impl Into<ListenerId>,
        exact_path: &str,
        route_id: RouteId,
    ) {
        let lid = listener_id.into();
        if let Some(listener) = self.listeners.get_mut(&lid) {
            listener.remove_exact_route(exact_path, route_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http2_exact_and_prefix_routing() {
        let routes = vec![
            Http2Route::new_exact(RouteId::new(1), "h2-listener", "/healthz"),
            Http2Route::new(RouteId::new(2), "h2-listener", "/grpc.service/v1"),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 = Http2RouteRequest::new("/healthz");
        let r1 = router.route("h2-listener", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));

        let req2 = Http2RouteRequest::new("/grpc.service/v1/CreateUser");
        let r2 = router.route("h2-listener", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));

        let req3 = Http2RouteRequest::new("/unknown");
        assert!(router.route("h2-listener", &req3).is_none());
    }

    #[test]
    fn test_http2_prefix_shadowing_fallback() {
        let routes = vec![
            Http2Route::new(RouteId::new(1), "listener-1", "/api/v1/internal")
                .with_host("internal.velda.io"),
            Http2Route::new(RouteId::new(2), "listener-1", "/api/v1"),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 =
            Http2RouteRequest::new("/api/v1/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route("listener-1", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));

        let req2 = Http2RouteRequest::new("/api/v1/internal/data").with_host("public.velda.io");
        let r2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
    }
}
