//! Layer 7 HTTP/1.1 routing rules, match requests, and in-memory lookup tables.
//!
//! ```text
//!                          HTTP/1.1 ROUTE MATCHING
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
//!                           Http1Route   None (404)
//! ```

use crate::arena::{ARENA_NONE, ArenaRouteEntry, RouteArena};
use crate::error::RouterError;
use crate::host::{CompiledHost, HostBlocks, host_specificity};
use crate::method::MethodMask;

/// Concrete compiled HTTP/1.1 Layer 7 route (8 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http1Route {
    pub id: u16,
    pub listener_id: u32,
}

impl Http1Route {
    /// Creates a new prefix-based HTTP/1.1 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(id: u16, listener_id: u32, path_prefix: impl Into<String>) -> Http1RouteRule {
        Http1RouteRule {
            id,
            listener_id,
            host: None,
            path_prefix: Some(path_prefix.into()),
            exact_path: None,
            method: None,
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }

    /// Creates a new exact-path HTTP/1.1 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new_exact(id: u16, listener_id: u32, exact_path: impl Into<String>) -> Http1RouteRule {
        Http1RouteRule {
            id,
            listener_id,
            host: None,
            path_prefix: None,
            exact_path: Some(exact_path.into()),
            method: None,
            method_mask: MethodMask::ANY,
            compiled_host: CompiledHost::Any,
        }
    }
}

/// Builder specification for an HTTP/1.1 route rule consumed during router compilation.
///
/// All pattern strings (`host`, `path_prefix`, `exact_path`, `method`) are consumed and
/// dropped once compiled into [`ListenerHttp1Router`], leaving only 8-byte [`Http1Route`]s in RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http1RouteRule {
    pub id: u16,
    pub listener_id: u32,
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub exact_path: Option<String>,
    pub method: Option<String>,
    pub method_mask: MethodMask,
    pub compiled_host: CompiledHost,
}

impl Http1RouteRule {
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
    pub fn matches_host(&self, host: &HostBlocks) -> bool {
        self.compiled_host.matches(host)
    }

    /// Checks whether this route rule matches the given request method mask.
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
        if !req.parsed_host.is_empty() {
            self.compiled_host.matches(&req.parsed_host)
        } else if let Some(h) = req.host {
            self.compiled_host.matches(&HostBlocks::parse(h))
        } else {
            false
        }
    }
}

/// Ephemeral exact route registered dynamically (e.g. for ACME challenges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EphemeralHttp1Route {
    pub path: String,
    pub route: Http1Route,
    pub method_mask: MethodMask,
}

/// Zero-allocation view of an incoming HTTP/1.1 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http1RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
    pub method_mask: MethodMask,
    pub parsed_host: HostBlocks,
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
            parsed_host: HostBlocks::default(),
        }
    }

    /// Attaches a host view.
    #[inline]
    pub fn with_host(mut self, host: &'a str) -> Self {
        self.host = Some(host);
        self.parsed_host = HostBlocks::parse(host);
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

/// Route table for a single HTTP/1.1 listener backed by a 4KB page-aligned [`RouteArena`].
#[repr(C, align(64))]
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp1Router {
    arena: RouteArena,
    ephemeral_routes: Vec<EphemeralHttp1Route>,
}

impl ListenerHttp1Router {
    /// Builds a new compiled HTTP/1.1 router for a single listener inside a 4KB [`RouteArena`].
    pub fn new(rules: Vec<Http1RouteRule>) -> Result<Self, RouterError> {
        let listener_id = rules.first().map_or(0, |r| r.listener_id);
        let mut entries = Vec::with_capacity(rules.len());
        let mut patterns: Vec<String> = Vec::with_capacity(rules.len());

        for r in &rules {
            let pat = if let Some(ref exact) = r.exact_path {
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
            patterns.push(pat);
        }

        for (r, pat) in rules.iter().zip(patterns.iter()) {
            entries.push(ArenaRouteEntry {
                pattern: pat.as_str(),
                route_id: r.id,
                method_mask: r.method_mask.0,
                host: r.compiled_host.as_pattern(),
                specificity: host_specificity(r.host.as_deref()),
                is_prefix: r.exact_path.is_none(),
                is_catch_all: false,
            });
        }

        let arena = RouteArena::build(listener_id, entries);
        Ok(Self {
            arena,
            ephemeral_routes: Vec::new(),
        })
    }

    /// Resolves an HTTP/1.1 route for an incoming request view, returning an 8-byte [`Http1Route`] on register.
    #[inline(always)]
    pub fn route(&self, req: &Http1RouteRequest<'_>) -> Option<Http1Route> {
        // 1. Ephemeral route fast check (e.g. ACME challenge)
        if !self.ephemeral_routes.is_empty() {
            for ep in &self.ephemeral_routes {
                if ep.path == req.path && ep.method_mask.matches(req.method_mask) {
                    return Some(ep.route);
                }
            }
        }

        // 2. Contiguous 4KB arena matching
        let mut matches = [(0u16, 0u8, false); 32];
        let count = self.arena.match_path(req.path, &mut matches);
        if count == 0 {
            return None;
        }

        let host_table = self.arena.host_table();
        let listener_id = self.arena.header().listener_id;
        let mut parsed_fallback: Option<HostBlocks> = None;

        for &(cand_start, cand_count, is_exact) in matches[..count].iter().rev() {
            for c in self.arena.node_entries(cand_start, cand_count) {
                if !is_exact && (c._pad & 0x01) == 0 {
                    continue;
                }
                if !MethodMask(c.method_mask).matches(req.method_mask) {
                    continue;
                }
                if c.host_id == ARENA_NONE {
                    return Some(Http1Route {
                        id: c.route_id,
                        listener_id,
                    });
                }
                let actual_host = if !req.parsed_host.is_empty() {
                    &req.parsed_host
                } else if let Some(h) = req.host {
                    parsed_fallback.get_or_insert_with(|| HostBlocks::parse(h))
                } else {
                    continue;
                };
                if host_table.matches(c.host_id, actual_host) {
                    return Some(Http1Route {
                        id: c.route_id,
                        listener_id,
                    });
                }
            }
        }

        None
    }

    /// Dynamically registers an ephemeral exact-path route into this listener's router.
    pub fn insert_exact_route(
        &mut self,
        exact_path: String,
        route: Http1Route,
        method_mask: MethodMask,
    ) {
        self.ephemeral_routes
            .retain(|e| !(e.path == exact_path && e.route.id == route.id));
        self.ephemeral_routes.push(EphemeralHttp1Route {
            path: exact_path,
            route,
            method_mask,
        });
    }

    /// Dynamically removes an ephemeral exact-path route previously registered for this listener.
    pub fn remove_exact_route(&mut self, exact_path: &str, route_id: u16) {
        self.ephemeral_routes
            .retain(|e| !(e.path == exact_path && e.route.id == route_id));
    }
}

/// Unified, thread-safe HTTP/1.1 routing table coordinating all HTTP/1.1 listeners (0 `FxHashMap`).
#[derive(Debug, Default, Clone)]
pub struct Http1Router {
    listeners: Vec<(u32, ListenerHttp1Router)>,
}

impl Http1Router {
    /// Builds a new [`Http1Router`] from a list of [`Http1RouteRule`] specifications.
    pub fn new(routes: impl IntoIterator<Item = Http1RouteRule>) -> Result<Self, RouterError> {
        let mut all_rules: Vec<Http1RouteRule> = routes.into_iter().collect();
        all_rules.sort_by_key(|r| r.listener_id);

        let mut listeners = Vec::new();
        let mut iter = all_rules.into_iter().peekable();
        while let Some(first) = iter.next() {
            let lid = first.listener_id;
            let mut group = vec![first];
            while let Some(next) = iter.peek() {
                if next.listener_id == lid {
                    group.push(iter.next().unwrap());
                } else {
                    break;
                }
            }
            listeners.push((lid, ListenerHttp1Router::new(group)?));
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/1.1 route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: u32, req: &Http1RouteRequest<'_>) -> Option<Http1Route> {
        let listener_router = self.listener(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: u32) -> Option<&ListenerHttp1Router> {
        if self.listeners.len() <= 16 {
            self.listeners
                .iter()
                .find(|(id, _)| *id == listener_id)
                .map(|(_, r)| r)
        } else {
            self.listeners
                .binary_search_by_key(&listener_id, |(id, _)| *id)
                .ok()
                .map(|idx| &self.listeners[idx].1)
        }
    }

    /// Returns the number of registered listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }

    /// Dynamically registers an ephemeral exact-path route for a specific listener.
    pub fn insert_exact_route(
        &mut self,
        listener_id: u32,
        exact_path: String,
        route: Http1Route,
        method_mask: MethodMask,
    ) {
        match self
            .listeners
            .binary_search_by_key(&listener_id, |(id, _)| *id)
        {
            Ok(idx) => {
                self.listeners[idx]
                    .1
                    .insert_exact_route(exact_path, route, method_mask);
            }
            Err(idx) => {
                let mut lr = ListenerHttp1Router::default();
                lr.insert_exact_route(exact_path, route, method_mask);
                self.listeners.insert(idx, (listener_id, lr));
            }
        }
    }

    /// Dynamically removes an ephemeral exact-path route for a specific listener.
    pub fn remove_exact_route(&mut self, listener_id: u32, exact_path: &str, route_id: u16) {
        if let Ok(idx) = self
            .listeners
            .binary_search_by_key(&listener_id, |(id, _)| *id)
        {
            self.listeners[idx]
                .1
                .remove_exact_route(exact_path, route_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use velda_core::fnv1a_hash_32;

    #[test]
    fn test_http1_exact_and_prefix_routing() {
        let lid = fnv1a_hash_32("listener-1");
        let routes = vec![
            Http1Route::new_exact(1, lid, "/healthz"),
            Http1Route::new(2, lid, "/api/v1"),
        ];

        let router = Http1Router::new(routes).unwrap();

        let req1 = Http1RouteRequest::new("/healthz");
        let r1 = router.route(lid, &req1).unwrap();
        assert_eq!(r1.id, 1);

        let req2 = Http1RouteRequest::new("/api/v1/users/42");
        let r2 = router.route(lid, &req2).unwrap();
        assert_eq!(r2.id, 2);

        let req3 = Http1RouteRequest::new("/unknown");
        assert!(router.route(lid, &req3).is_none());
    }

    #[test]
    fn test_http1_prefix_shadowing_fallback() {
        let lid = fnv1a_hash_32("listener-1");
        let routes = vec![
            Http1Route::new(1, lid, "/api/v1/internal").with_host("internal.velda.io"),
            Http1Route::new(2, lid, "/api/v1"),
        ];

        let router = Http1Router::new(routes).unwrap();

        // Host matches Route A
        let req1 =
            Http1RouteRequest::new("/api/v1/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route(lid, &req1).unwrap();
        assert_eq!(r1.id, 1);

        // Host mismatch on Route A falls back to Route B
        let req2 = Http1RouteRequest::new("/api/v1/internal/data").with_host("public.velda.io");
        let r2 = router.route(lid, &req2).unwrap();
        assert_eq!(r2.id, 2);
    }

    #[test]
    fn test_http1_exact_host_precedence_over_wildcard() {
        let lid = fnv1a_hash_32("listener-1");
        // Register wildcard route FIRST, then exact route SECOND
        let routes = vec![
            Http1Route::new(1, lid, "/api").with_host("*.velda.io"),
            Http1Route::new(2, lid, "/api").with_host("api.velda.io"),
        ];

        let router = Http1Router::new(routes).unwrap();

        // Exact match for api.velda.io MUST route to Route 2, not Route 1
        let req = Http1RouteRequest::new("/api/data").with_host("api.velda.io");
        let matched = router.route(lid, &req).unwrap();
        assert_eq!(matched.id, 2);

        // Other subdomains route to Route 1
        let req2 = Http1RouteRequest::new("/api/data").with_host("billing.velda.io");
        let matched2 = router.route(lid, &req2).unwrap();
        assert_eq!(matched2.id, 1);
    }

    #[test]
    fn test_http1_large_table_workload_adaptive() {
        let lid = fnv1a_hash_32("listener-1");
        // > 3 routes triggers route_table (SIMD/Slash-Block + FxHashMap)
        let routes = vec![
            Http1Route::new_exact(1, lid, "/healthz"),
            Http1Route::new(2, lid, "/api/v1"),
            Http1Route::new(3, lid, "/api/v2"),
            Http1Route::new(4, lid, "/static"),
            Http1Route::new(5, lid, "/auth"),
        ];

        let router = Http1Router::new(routes).unwrap();

        let req1 = Http1RouteRequest::new("/healthz");
        assert_eq!(router.route(lid, &req1).unwrap().id, 1);

        let req2 = Http1RouteRequest::new("/api/v2/items");
        assert_eq!(router.route(lid, &req2).unwrap().id, 3);

        let req3 = Http1RouteRequest::new("/static/logo.png");
        assert_eq!(router.route(lid, &req3).unwrap().id, 4);

        let req_miss = Http1RouteRequest::new("/non-existent");
        assert!(router.route(lid, &req_miss).is_none());
    }

    #[test]
    fn test_http1_ephemeral_exact_route_lifecycle() {
        let lid = fnv1a_hash_32("listener-1");
        // Test in small listener mode (<= 3 routes)
        let mut small_router = Http1Router::new(vec![Http1Route::new(1, lid, "/api")]).unwrap();

        let acme_path = "/.well-known/acme-challenge/token123".to_string();
        let acme_route = Http1Route {
            id: 9999,
            listener_id: lid,
        };

        // Before insertion: 404 / None
        let req = Http1RouteRequest::new(&acme_path);
        assert!(small_router.route(lid, &req).is_none());

        // Insert ephemeral route
        small_router.insert_exact_route(lid, acme_path.clone(), acme_route, MethodMask::GET);

        // After insertion: matches ephemeral route immediately
        let matched = small_router.route(lid, &req).unwrap();
        assert_eq!(matched.id, 9999);

        // Existing route still matches without disruption
        let api_req = Http1RouteRequest::new("/api/v1");
        assert_eq!(small_router.route(lid, &api_req).unwrap().id, 1);

        // Remove ephemeral route (simulating RAII Drop guard cleanup)
        small_router.remove_exact_route(lid, &acme_path, 9999);

        // After removal: 404 / None restored cleanly
        assert!(small_router.route(lid, &req).is_none());
        assert_eq!(small_router.route(lid, &api_req).unwrap().id, 1);
    }
}
