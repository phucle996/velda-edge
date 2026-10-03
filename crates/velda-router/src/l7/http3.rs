//! Layer 7 HTTP/3 routing rules, match requests, and in-memory lookup tables.
//!
//! ### Architectural Note: Protocol Isolation Invariant (AGENTS.md §2.1 & §2.7)
//! This module intentionally maintains its own self-contained definitions for [`Http3Route`],
//! [`Http3RouteRequest`], and [`Http3Router`] rather than sharing a generic parent struct with
//! HTTP/1.1 and HTTP/2. Duplicating these structures across protocol boundaries is an intentional
//! architectural design choice:
//! 1. "Duplicate first. Abstract second": Preserves flat workflows and avoids accidental cross-protocol
//!    coupling between RFC 9114 (HTTP/3 over QUIC datagrams) and TCP-based streaming/multiplexing.
//! 2. Allows future HTTP/3-specific routing constraints (such as QUIC 0-RTT early data acceptance,
//!    datagram extension forwarding, or connection ID routing) without polluting HTTP/1.1 or HTTP/2.

use rustc_hash::FxHashMap;
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::{matches_host, matches_method};
use crate::l7::trie::PrefixTrie;

/// Concrete compiled HTTP/3 Layer 7 routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3Route {
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

impl Http3Route {
    /// Creates a new prefix-based HTTP/3 route rule.
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

    /// Creates a new exact-path HTTP/3 route rule.
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

/// Zero-allocation view of an incoming HTTP/3 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http3RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
}

impl<'a> Http3RouteRequest<'a> {
    /// Creates a new HTTP/3 route request view.
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

/// Route table for a single HTTP/3 listener.
#[derive(Debug, Clone, Default)]
pub struct ListenerHttp3Router {
    exact_routes: FxHashMap<String, Vec<Http3Route>>,
    trie: PrefixTrie,
    prefix_routes: Vec<Vec<Http3Route>>,
}

impl ListenerHttp3Router {
    /// Builds a new compiled HTTP/3 router for a single listener.
    pub fn new(routes: Vec<Http3Route>) -> Result<Self, RouterError> {
        let mut exact_map: FxHashMap<String, Vec<Http3Route>> = FxHashMap::default();
        let mut prefix_map: FxHashMap<String, Vec<Http3Route>> = FxHashMap::default();

        for r in routes {
            if let Some(ref exact) = r.exact_path {
                exact_map.entry(exact.clone()).or_default().push(r);
            } else if let Some(ref prefix) = r.path_prefix
                && !prefix.is_empty()
            {
                prefix_map.entry(prefix.clone()).or_default().push(r);
            }
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

    /// Resolves an HTTP/3 route for an incoming request view.
    ///
    /// Evaluates Tier 1 exact matches first ($O(1)$), followed by Tier 2 prefix matching via Compressed Radix Trie ($O(P)$).
    /// Prefix matching uses longest-prefix-first ordering with automatic fallback to shorter prefixes when host or method filters fail.
    #[inline]
    pub fn route(&self, req: &Http3RouteRequest<'_>) -> Option<&Http3Route> {
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

/// Unified, thread-safe HTTP/3 routing table coordinating all HTTP/3 listeners.
#[derive(Debug, Default, Clone)]
pub struct Http3Router {
    listeners: FxHashMap<String, ListenerHttp3Router>,
}

impl Http3Router {
    /// Builds a new [`Http3Router`] from a list of compiled [`Http3Route`] rules.
    pub fn new(routes: impl IntoIterator<Item = Http3Route>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<String, Vec<Http3Route>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerHttp3Router::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP/3 route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &Http3RouteRequest<'_>) -> Option<&Http3Route> {
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
    fn test_http3_exact_and_prefix_routing() {
        let routes = vec![
            Http3Route::new_exact(
                RouteId::new(1),
                "listener-h3",
                "/healthz",
                UpstreamId::new(10),
                "health-upstream",
            ),
            Http3Route::new(
                RouteId::new(2),
                "listener-h3",
                "/api/v3",
                UpstreamId::new(20),
                "api-v3-upstream",
            ),
        ];

        let router = Http3Router::new(routes).unwrap();

        let req1 = Http3RouteRequest::new("/healthz");
        let r1 = router.route("listener-h3", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "health-upstream");

        let req2 = Http3RouteRequest::new("/api/v3/quic/session");
        let r2 = router.route("listener-h3", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "api-v3-upstream");
    }

    #[test]
    fn test_http3_prefix_shadowing_fallback() {
        let routes = vec![
            Http3Route::new(
                RouteId::new(1),
                "listener-h3",
                "/api/v3/quic-vip",
                UpstreamId::new(10),
                "vip-upstream",
            )
            .with_host("vip.velda.io"),
            Http3Route::new(
                RouteId::new(2),
                "listener-h3",
                "/api/v3",
                UpstreamId::new(20),
                "public-upstream",
            ),
        ];

        let router = Http3Router::new(routes).unwrap();

        let req = Http3RouteRequest::new("/api/v3/quic-vip/connect").with_host("public.velda.io");
        let r = router.route("listener-h3", &req).unwrap();
        assert_eq!(r.id, RouteId::new(2));
        assert_eq!(r.upstream_name, "public-upstream");
    }
}
