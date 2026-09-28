//! High-performance, in-memory HTTP route lookup table.
//!
//! Uses Aho-Corasick multi-pattern search with LeftmostLongest match semantics
//! for prefix paths, combined with hash tables for exact path and listener indexing.
//! All lookups on the request serving hot path are $O(M)$ and zero-allocation.

use std::collections::HashMap;

use aho_corasick::{AhoCorasick, Anchored, Input, MatchKind, StartKind};

use super::route::{HttpRoute, HttpRouteRequest};
use crate::error::RouterError;

/// Route table for a single listener.
#[derive(Debug, Clone)]
pub struct ListenerHttpRouter {
    /// Exact path routes indexed by exact path string.
    exact_routes: HashMap<String, Vec<HttpRoute>>,
    /// Pre-compiled Aho-Corasick automaton for all prefix patterns.
    automaton: Option<AhoCorasick>,
    /// Routes corresponding to each pattern index in `automaton`.
    prefix_routes: Vec<Vec<HttpRoute>>,
}

impl ListenerHttpRouter {
    pub fn new(routes: Vec<HttpRoute>) -> Result<Self, RouterError> {
        let mut exact_map: HashMap<String, Vec<HttpRoute>> = HashMap::new();
        let mut prefix_map: HashMap<String, Vec<HttpRoute>> = HashMap::new();

        for r in routes {
            if let Some(ref exact) = r.exact_path {
                exact_map.entry(exact.clone()).or_default().push(r);
            } else if let Some(ref prefix) = r.path_prefix
                && !prefix.is_empty()
            {
                prefix_map.entry(prefix.clone()).or_default().push(r);
            }
        }

        let mut patterns = Vec::with_capacity(prefix_map.len());
        let mut prefix_routes = Vec::with_capacity(prefix_map.len());

        // Sort patterns descending by length so order is deterministic
        let mut sorted_prefixes: Vec<_> = prefix_map.into_iter().collect();
        sorted_prefixes.sort_by_key(|(a, _)| std::cmp::Reverse(a.len()));

        for (pattern, r_list) in sorted_prefixes {
            patterns.push(pattern);
            prefix_routes.push(r_list);
        }

        let automaton = if !patterns.is_empty() {
            let ac = AhoCorasick::builder()
                .match_kind(MatchKind::LeftmostLongest)
                .start_kind(StartKind::Anchored)
                .build(&patterns)
                .map_err(|e| RouterError::InvalidRoute {
                    detail: format!("failed to build Aho-Corasick automaton: {e}"),
                })?;
            Some(ac)
        } else {
            None
        };

        Ok(Self {
            exact_routes: exact_map,
            automaton,
            prefix_routes,
        })
    }

    /// Resolves an HTTP route for the given request without allocations.
    #[inline]
    pub fn route(&self, req: &HttpRouteRequest<'_>) -> Option<&HttpRoute> {
        // 1. Try exact path match first
        if let Some(candidates) = self.exact_routes.get(req.path)
            && let Some(r) = candidates
                .iter()
                .find(|r| r.matches_host(req.host) && r.matches_method(req.method))
        {
            return Some(r);
        }

        // 2. Try anchored prefix match using Aho-Corasick (strictly at byte offset 0)
        if let Some(ref ac) = self.automaton {
            let input = Input::new(req.path).anchored(Anchored::Yes);
            if let Some(mat) = ac.find(input) {
                let pattern_idx = mat.pattern().as_usize();
                if let Some(candidates) = self.prefix_routes.get(pattern_idx)
                    && let Some(r) = candidates
                        .iter()
                        .find(|r| r.matches_host(req.host) && r.matches_method(req.method))
                {
                    return Some(r);
                }
            }
        }

        // Không match là không match, không fallback route
        None
    }
}

/// Unified, thread-safe HTTP routing table coordinating all HTTP listeners.
#[derive(Debug, Default, Clone)]
pub struct HttpRouter {
    listeners: HashMap<String, ListenerHttpRouter>,
}

impl HttpRouter {
    /// Builds a new [`HttpRouter`] from a list of compiled [`HttpRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = HttpRoute>) -> Result<Self, RouterError> {
        let mut grouped: HashMap<String, Vec<HttpRoute>> = HashMap::new();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = HashMap::with_capacity(grouped.len());
        for (listener_id, list) in grouped {
            let listener_router = ListenerHttpRouter::new(list)?;
            listeners.insert(listener_id, listener_router);
        }

        Ok(Self { listeners })
    }

    /// Resolves an HTTP route for the given listener and request view.
    ///
    /// Performs zero allocations on the request serving hot path.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &HttpRouteRequest<'_>) -> Option<&HttpRoute> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the number of configured listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use velda_core::{RouteId, UpstreamId};

    #[test]
    fn test_http_router_longest_prefix_matching() {
        let routes = vec![
            HttpRoute::new(
                RouteId::new(1),
                "http-in",
                "/api",
                UpstreamId::new(10),
                "api-backend",
            ),
            HttpRoute::new(
                RouteId::new(2),
                "http-in",
                "/api/v2",
                UpstreamId::new(20),
                "v2-backend",
            ),
            HttpRoute::new(
                RouteId::new(3),
                "http-in",
                "/api/v2/users",
                UpstreamId::new(30),
                "users-backend",
            ),
            HttpRoute::new(
                RouteId::new(4),
                "http-in",
                "/",
                UpstreamId::new(40),
                "root-backend",
            ),
        ];

        let router = HttpRouter::new(routes).unwrap();

        // 1. Longest prefix matches /api/v2/users
        let req1 = HttpRouteRequest::new("/api/v2/users/42/profile");
        let r1 = router.route("http-in", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(3));
        assert_eq!(r1.upstream_name, "users-backend");

        // 2. Intermediate prefix matches /api/v2
        let req2 = HttpRouteRequest::new("/api/v2/orders");
        let r2 = router.route("http-in", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));

        // 3. Shorter prefix matches /api
        let req3 = HttpRouteRequest::new("/api/v1/legacy");
        let r3 = router.route("http-in", &req3).unwrap();
        assert_eq!(r3.id, RouteId::new(1));

        // 4. Prefix "/" matches because it was explicitly configured
        let req4 = HttpRouteRequest::new("/index.html");
        let r4 = router.route("http-in", &req4).unwrap();
        assert_eq!(r4.id, RouteId::new(4));

        // 5. Unknown listener returns None
        assert!(router.route("unknown-in", &req1).is_none());
    }

    #[test]
    fn test_http_router_no_match_returns_none() {
        let routes = vec![
            HttpRoute::new(
                RouteId::new(1),
                "http-in",
                "/api/v1",
                UpstreamId::new(10),
                "api-backend",
            ),
            HttpRoute::new_exact(
                RouteId::new(2),
                "http-in",
                "/healthz",
                UpstreamId::new(20),
                "health-backend",
            ),
        ];

        let router = HttpRouter::new(routes).unwrap();

        // Exact match works
        let req_exact = HttpRouteRequest::new("/healthz");
        assert_eq!(
            router.route("http-in", &req_exact).unwrap().id,
            RouteId::new(2)
        );

        // Prefix match works
        let req_prefix = HttpRouteRequest::new("/api/v1/users");
        assert_eq!(
            router.route("http-in", &req_prefix).unwrap().id,
            RouteId::new(1)
        );

        // Non-matching path returns None strictly (không match là không match, không fallback)
        let req_nomatch1 = HttpRouteRequest::new("/health");
        assert!(router.route("http-in", &req_nomatch1).is_none());

        let req_nomatch2 = HttpRouteRequest::new("/api/v2");
        assert!(router.route("http-in", &req_nomatch2).is_none());

        let req_nomatch3 = HttpRouteRequest::new("/static/app.js");
        assert!(router.route("http-in", &req_nomatch3).is_none());
    }

    #[test]
    fn test_http_router_host_and_method_matching() {
        let routes = vec![
            HttpRoute::new(
                RouteId::new(1),
                "http-in",
                "/users",
                UpstreamId::new(10),
                "users-get",
            )
            .with_host("api.example.com")
            .with_method("GET"),
            HttpRoute::new(
                RouteId::new(2),
                "http-in",
                "/users",
                UpstreamId::new(20),
                "users-post",
            )
            .with_host("api.example.com")
            .with_method("POST"),
            HttpRoute::new(
                RouteId::new(3),
                "http-in",
                "/users",
                UpstreamId::new(30),
                "wildcard-host",
            )
            .with_host("*.example.com"),
        ];

        let router = HttpRouter::new(routes).unwrap();

        // Match exact host + GET
        let req_get = HttpRouteRequest::new("/users")
            .with_host("api.example.com")
            .with_method("GET");
        assert_eq!(
            router.route("http-in", &req_get).unwrap().id,
            RouteId::new(1)
        );

        // Match exact host + POST
        let req_post = HttpRouteRequest::new("/users")
            .with_host("api.example.com")
            .with_method("POST");
        assert_eq!(
            router.route("http-in", &req_post).unwrap().id,
            RouteId::new(2)
        );

        // Match wildcard host
        let req_wildcard = HttpRouteRequest::new("/users")
            .with_host("partner.example.com")
            .with_method("GET");
        assert_eq!(
            router.route("http-in", &req_wildcard).unwrap().id,
            RouteId::new(3)
        );

        // Match wildcard host case-insensitively
        let req_wildcard_caps = HttpRouteRequest::new("/users")
            .with_host("PARTNER.EXAMPLE.COM")
            .with_method("GET");
        assert_eq!(
            router.route("http-in", &req_wildcard_caps).unwrap().id,
            RouteId::new(3)
        );

        // Suffix collision without dot boundary must NOT match (e.g. evilexample.com)
        let req_evil = HttpRouteRequest::new("/users").with_host("evilexample.com");
        assert!(router.route("http-in", &req_evil).is_none());

        // Naked domain without subdomain must NOT match wildcard
        let req_naked = HttpRouteRequest::new("/users").with_host("example.com");
        assert!(router.route("http-in", &req_naked).is_none());

        // Mismatched host returns None
        let req_diff = HttpRouteRequest::new("/users").with_host("other.com");
        assert!(router.route("http-in", &req_diff).is_none());
    }

    #[test]
    fn test_http_router_exact_path_precedence() {
        let routes = vec![
            HttpRoute::new(
                RouteId::new(1),
                "http-in",
                "/health",
                UpstreamId::new(10),
                "health-prefix",
            ),
            HttpRoute::new(
                RouteId::new(2),
                "http-in",
                "/health/check",
                UpstreamId::new(20),
                "health-exact",
            )
            .with_exact_path("/health/check"),
        ];

        let router = HttpRouter::new(routes).unwrap();

        let req_exact = HttpRouteRequest::new("/health/check");
        assert_eq!(
            router.route("http-in", &req_exact).unwrap().id,
            RouteId::new(2)
        );

        let req_prefix = HttpRouteRequest::new("/health/other");
        assert_eq!(
            router.route("http-in", &req_prefix).unwrap().id,
            RouteId::new(1)
        );
    }
}
