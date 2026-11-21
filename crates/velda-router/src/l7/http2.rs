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

use crate::arena::{ARENA_NONE, ArenaRouteEntry, RouteArena};
use crate::error::RouterError;
use crate::host::{CompiledHost, HostBlocks, host_specificity};
use crate::method::MethodMask;

/// Concrete compiled HTTP/1.1 Layer 7 route (8 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http2Route {
    pub id: u16,
    pub listener_id: u32,
}

impl Http2Route {
    /// Creates a new prefix-based HTTP/1.1 route rule for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(id: u16, listener_id: u32, path_prefix: impl Into<String>) -> Http2RouteRule {
        Http2RouteRule {
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
    pub fn new_exact(id: u16, listener_id: u32, exact_path: impl Into<String>) -> Http2RouteRule {
        Http2RouteRule {
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
/// dropped once compiled into [`ListenerHttp2Router`], leaving only 8-byte [`Http2Route`]s in RAM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2RouteRule {
    pub id: u16,
    pub listener_id: u32,
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
    pub fn matches_req(&self, req: &Http2RouteRequest<'_>) -> bool {
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

/// Zero-allocation view of an incoming HTTP/2 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http2RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
    pub method_mask: MethodMask,
    pub parsed_host: HostBlocks,
}

impl<'a> Http2RouteRequest<'a> {
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

/// Route table for a single HTTP/2 listener backed by a 4KB page-aligned [`RouteArena`].
#[repr(C, align(64))]
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp2Router {
    arena: RouteArena,
}

impl ListenerHttp2Router {
    /// Builds a new compiled HTTP/2 router for a single listener inside a 4KB [`RouteArena`].
    pub fn new(rules: Vec<Http2RouteRule>) -> Result<Self, RouterError> {
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
        Ok(Self { arena })
    }

    /// Resolves an HTTP/2 route for an incoming request view, returning an 8-byte [`Http2Route`] on register.
    #[inline(always)]
    pub fn route(&self, req: &Http2RouteRequest<'_>) -> Option<Http2Route> {
        // Contiguous 4KB arena matching
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
                    return Some(Http2Route {
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
                    return Some(Http2Route {
                        id: c.route_id,
                        listener_id,
                    });
                }
            }
        }

        None
    }
}

/// Unified, thread-safe HTTP/1.1 routing table coordinating all HTTP/1.1 listeners (0 `FxHashMap`).
#[derive(Debug, Default, Clone)]
pub struct Http2Router {
    listeners: Vec<(u32, ListenerHttp2Router)>,
}

impl Http2Router {
    /// Builds a new [`Http2Router`] from a list of [`Http2RouteRule`] specifications.
    pub fn new(routes: impl IntoIterator<Item = Http2RouteRule>) -> Result<Self, RouterError> {
        let mut all_rules: Vec<Http2RouteRule> = routes.into_iter().collect();
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
            listeners.push((lid, ListenerHttp2Router::new(group)?));
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/1.1 route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: u32, req: &Http2RouteRequest<'_>) -> Option<Http2Route> {
        let listener_router = self.listener(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: u32) -> Option<&ListenerHttp2Router> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use velda_core::fnv1a_hash_32;

    #[test]
    fn test_http2_exact_and_prefix_routing() {
        let lid = fnv1a_hash_32("h2-listener");
        let routes = vec![
            Http2Route::new_exact(1, lid, "/healthz"),
            Http2Route::new(2, lid, "/grpc.service/v1"),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 = Http2RouteRequest::new("/healthz");
        let r1 = router.route(lid, &req1).unwrap();
        assert_eq!(r1.id, 1);

        let req2 = Http2RouteRequest::new("/grpc.service/v1/CreateUser");
        let r2 = router.route(lid, &req2).unwrap();
        assert_eq!(r2.id, 2);

        let req3 = Http2RouteRequest::new("/unknown");
        assert!(router.route(lid, &req3).is_none());
    }

    #[test]
    fn test_http2_prefix_shadowing_fallback() {
        let lid = fnv1a_hash_32("listener-1");
        let routes = vec![
            Http2Route::new(1, lid, "/api/v1/internal").with_host("internal.velda.io"),
            Http2Route::new(2, lid, "/api/v1"),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 =
            Http2RouteRequest::new("/api/v1/internal/secrets").with_host("internal.velda.io");
        let r1 = router.route(lid, &req1).unwrap();
        assert_eq!(r1.id, 1);

        let req2 = Http2RouteRequest::new("/api/v1/internal/data").with_host("public.velda.io");
        let r2 = router.route(lid, &req2).unwrap();
        assert_eq!(r2.id, 2);
    }
}
