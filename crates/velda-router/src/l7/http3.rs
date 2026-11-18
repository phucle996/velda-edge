//! Layer 7 HTTP/3 routing rules, match requests, and in-memory lookup tables.
//!
//! ```text
//!                           HTTP/3 ROUTE MATCHING
//!                                     │
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
//!                           Http3Route   None (404)
//! ```

use rustc_hash::FxHashMap;
use velda_core::{ListenerId, RouteId};

use crate::error::RouterError;
use crate::host::{CompiledHost, ReverseHostLabels, host_specificity};
use crate::method::MethodMask;
use crate::path::PathPrefixMatcher;

/// Concrete compiled HTTP/3 Layer 7 route (8 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3Route {
    pub id: RouteId,
    pub listener_id: ListenerId,
}

impl Http3Route {
    /// Creates a new prefix-based HTTP/3 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        path_prefix: impl Into<String>,
    ) -> Http3RouteRule {
        Http3RouteRule {
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

    /// Creates a new exact-path HTTP/3 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new_exact(
        id: RouteId,
        listener_id: impl Into<ListenerId>,
        exact_path: impl Into<String>,
    ) -> Http3RouteRule {
        Http3RouteRule {
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

/// Builder specification for an HTTP/3 route rule consumed during router compilation.
///
/// All pattern strings (`host`, `path_prefix`, `exact_path`, `method`) are consumed and
/// dropped once compiled into [`ListenerHttp3Router`], leaving only 8-byte [`Http3Route`]s in RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3RouteRule {
    pub id: RouteId,
    pub listener_id: ListenerId,
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub exact_path: Option<String>,
    pub method: Option<String>,
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

/// Compact candidate descriptor indexing into `routes: Vec<Http3Route>`.
#[repr(C, align(16))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3HotCandidate {
    pub compiled_host: CompiledHost,
    pub route_idx: u32,
    pub method_mask: MethodMask,
    pub is_prefix: bool,
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

/// Route table for a single HTTP/3 listener with Hot/Cold Data-Oriented Separation.
///
/// Aligned to 64 bytes to sit on an exact L1D cache line boundary, preventing split-line
/// header loads and false sharing between worker threads.
#[repr(C, align(64))]
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp3Router {
    routes: Vec<Http3Route>,
    matcher: PathPrefixMatcher,
    pattern_routes: Vec<Vec<Http3HotCandidate>>,
}

impl ListenerHttp3Router {
    /// Builds a new compiled HTTP/3 router for a single listener.
    ///
    /// Consolidates exact and prefix matching into a single contiguous integer-block
    /// arena ([`PathPrefixMatcher`]), eliminating heap pointer dereferencing and scalar
    /// branch divergence.
    pub fn new(rules: Vec<Http3RouteRule>) -> Result<Self, RouterError> {
        let mut routes = Vec::with_capacity(rules.len());
        let mut pattern_map: FxHashMap<String, Vec<Http3HotCandidate>> = FxHashMap::default();

        for (idx, r) in rules.iter().enumerate() {
            routes.push(Http3Route {
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

            let candidate = Http3HotCandidate {
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
        })
    }

    /// Resolves an HTTP/3 route for an incoming request view.
    ///
    /// Evaluates using unified contiguous integer-block matching across ALU Port 0/1.
    #[inline(always)]
    pub fn route(&self, req: &Http3RouteRequest<'_>) -> Option<&Http3Route> {
        // 1. Hardware temporal prefetch
        if let Some(first_node) = self.matcher.nodes().first() {
            crate::path::prefetch_l1_keep(first_node as *const _ as *const u8);
        }

        // 2. Contiguous integer matching
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
            Http3Route::new_exact(RouteId::new(1), "quic-listener", "/healthz"),
            Http3Route::new(RouteId::new(2), "quic-listener", "/api/v3"),
        ];

        let router = Http3Router::new(routes).unwrap();

        let req1 = Http3RouteRequest::new("/healthz");
        let r1 = router.route("quic-listener", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));

        let req2 = Http3RouteRequest::new("/api/v3/orders/42");
        let r2 = router.route("quic-listener", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));

        let req3 = Http3RouteRequest::new("/unknown");
        assert!(router.route("quic-listener", &req3).is_none());
    }

    #[test]
    fn test_http3_prefix_shadowing_fallback() {
        let routes = vec![
            Http3Route::new(RouteId::new(1), "listener-1", "/api/v3/internal")
                .with_host("internal.velda.io"),
            Http3Route::new(RouteId::new(2), "listener-1", "/api/v3"),
        ];

        let router = Http3Router::new(routes).unwrap();

        let req1 =
            Http3RouteRequest::new("/api/v3/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route("listener-1", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));

        let req2 = Http3RouteRequest::new("/api/v3/internal/data").with_host("public.velda.io");
        let r2 = router.route("listener-1", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
    }
}
