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

use rustc_hash::FxHashMap;
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::{matches_host, matches_method};
use crate::l7::trie::PrefixTrie;

/// Concrete compiled HTTP/2 Layer 7 routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http2Route {
    pub id: RouteId,
    pub listener_id: String,
    pub host: Option<String>,
    pub path_prefix: Option<String>,
    pub exact_path: Option<String>,
    pub method: Option<String>,
    pub upstream_id: UpstreamId,
    pub upstream_name: String,
    pub plugins: Vec<String>,
}

impl Http2Route {
    /// Creates a new prefix-based HTTP/2 route rule.
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

    /// Creates a new exact-path HTTP/2 route rule.
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

    /// Attaches an expected host constraint (e.g. `"api.velda.io"` or `"*.velda.io"`).
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Attaches an expected HTTP method constraint (e.g. `"GET"` or `"POST"`).
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Checks whether this route matches the given request host.
    #[inline]
    pub fn matches_host(&self, host: Option<&str>) -> bool {
        matches_host(self.host.as_deref(), host)
    }

    /// Checks whether this route matches the given request method.
    #[inline]
    pub fn matches_method(&self, method: Option<&str>) -> bool {
        matches_method(self.method.as_deref(), method)
    }
}

/// Zero-allocation view of an incoming HTTP/2 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http2RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
}

impl<'a> Http2RouteRequest<'a> {
    /// Creates a new HTTP/2 route request view.
    #[inline]
    pub fn new(path: &'a str) -> Self {
        Self {
            path,
            host: None,
            method: None,
        }
    }

    /// Attaches a host view.
    #[inline]
    pub fn with_host(mut self, host: &'a str) -> Self {
        self.host = Some(host);
        self
    }

    /// Attaches an HTTP method view.
    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self
    }
}

/// Route table for a single HTTP/2 listener.
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp2Router {
    exact_routes: FxHashMap<String, Vec<Http2Route>>,
    trie: PrefixTrie,
    prefix_routes: Vec<Vec<Http2Route>>,
}

impl ListenerHttp2Router {
    /// Builds a new compiled HTTP/2 router for a single listener.
    pub fn new(routes: Vec<Http2Route>) -> Result<Self, RouterError> {
        let mut exact_map: FxHashMap<String, Vec<Http2Route>> = FxHashMap::default();
        let mut prefix_map: FxHashMap<String, Vec<Http2Route>> = FxHashMap::default();

        for r in routes {
            if let Some(ref exact) = r.exact_path {
                exact_map.entry(exact.clone()).or_default().push(r);
            } else if let Some(ref prefix) = r.path_prefix
                && !prefix.is_empty()
            {
                prefix_map.entry(prefix.clone()).or_default().push(r);
            }
        }

        for list in exact_map.values_mut() {
            list.sort_by_key(|r| {
                std::cmp::Reverse(crate::host::host_specificity(r.host.as_deref()))
            });
        }
        for list in prefix_map.values_mut() {
            list.sort_by_key(|r| {
                std::cmp::Reverse(crate::host::host_specificity(r.host.as_deref()))
            });
        }

        let mut trie = PrefixTrie::new();
        let mut prefix_routes = Vec::with_capacity(prefix_map.len());

        for (pattern, r_list) in prefix_map {
            let idx = prefix_routes.len();
            prefix_routes.push(r_list);
            trie.insert(&pattern, idx);
        }

        Ok(Self {
            exact_routes: exact_map,
            trie,
            prefix_routes,
        })
    }

    /// Resolves an HTTP/2 route for an incoming request view.
    ///
    /// Evaluates Tier 1 exact matches first ($O(1)$), followed by Tier 2 prefix matching via Compressed Radix Trie ($O(P)$).
    /// Prefix matching uses longest-prefix-first ordering with automatic fallback to shorter prefixes when host or method filters fail.
    #[inline]
    pub fn route(&self, req: &Http2RouteRequest<'_>) -> Option<&Http2Route> {
        // Tier 1: Exact match lookup (O(1))
        if let Some(candidates) = self.exact_routes.get(req.path)
            && let Some(r) = candidates
                .iter()
                .find(|r| r.matches_host(req.host) && r.matches_method(req.method))
        {
            return Some(r);
        }

        // Tier 2: Prefix match lookup via Radix Trie (O(P)) with longest-prefix-first fallback
        let mut matches = [0usize; 32];
        let count = self.trie.match_prefixes(req.path, &mut matches);

        // Iterate reverse: longest prefix match is at the end of matches buffer
        for &pattern_idx in matches[..count].iter().rev() {
            if let Some(candidates) = self.prefix_routes.get(pattern_idx)
                && let Some(r) = candidates
                    .iter()
                    .find(|r| r.matches_host(req.host) && r.matches_method(req.method))
            {
                return Some(r);
            }
        }

        None
    }
}

/// Unified, thread-safe HTTP/2 routing table coordinating all HTTP/2 listeners.
#[derive(Debug, Default, Clone)]
pub struct Http2Router {
    listeners: FxHashMap<String, ListenerHttp2Router>,
}

impl Http2Router {
    /// Builds a new [`Http2Router`] from a list of compiled [`Http2Route`] rules.
    pub fn new(routes: impl IntoIterator<Item = Http2Route>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<String, Vec<Http2Route>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerHttp2Router::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/2 route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &Http2RouteRequest<'_>) -> Option<&Http2Route> {
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
    fn test_http2_exact_and_prefix_routing() {
        let routes = vec![
            Http2Route::new_exact(
                RouteId::new(1),
                "listener-h2",
                "/healthz",
                UpstreamId::new(10),
                "health-upstream",
            ),
            Http2Route::new(
                RouteId::new(2),
                "listener-h2",
                "/api/v2",
                UpstreamId::new(20),
                "api-v2-upstream",
            ),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req1 = Http2RouteRequest::new("/healthz");
        let r1 = router.route("listener-h2", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "health-upstream");

        let req2 = Http2RouteRequest::new("/api/v2/items/99");
        let r2 = router.route("listener-h2", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "api-v2-upstream");
    }

    #[test]
    fn test_http2_prefix_shadowing_fallback() {
        let routes = vec![
            Http2Route::new(
                RouteId::new(1),
                "listener-h2",
                "/api/v2/admin",
                UpstreamId::new(10),
                "admin-upstream",
            )
            .with_host("admin.velda.io"),
            Http2Route::new(
                RouteId::new(2),
                "listener-h2",
                "/api/v2",
                UpstreamId::new(20),
                "public-upstream",
            ),
        ];

        let router = Http2Router::new(routes).unwrap();

        let req = Http2RouteRequest::new("/api/v2/admin/status").with_host("public.velda.io");
        let r = router.route("listener-h2", &req).unwrap();
        assert_eq!(r.id, RouteId::new(2));
        assert_eq!(r.upstream_name, "public-upstream");
    }
}
