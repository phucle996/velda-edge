//! Layer 7 HTTP/1.1 routing rules, match requests, and in-memory Aho-Corasick lookup tables.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use aho_corasick::{AhoCorasick, Anchored, Input, MatchKind, StartKind};
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;

/// Concrete compiled HTTP/1.1 Layer 7 routing rule.
#[derive(Debug, Clone)]
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
    pub target_endpoints: Vec<SocketAddr>,
    rr_index: Arc<AtomicUsize>,
}

impl PartialEq for Http1Route {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.listener_id == other.listener_id
            && self.host == other.host
            && self.path_prefix == other.path_prefix
            && self.exact_path == other.exact_path
            && self.method == other.method
            && self.upstream_id == other.upstream_id
            && self.upstream_name == other.upstream_name
            && self.plugins == other.plugins
            && self.target_endpoints == other.target_endpoints
    }
}

impl Eq for Http1Route {}

impl Http1Route {
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
            target_endpoints: Vec::new(),
            rr_index: Arc::new(AtomicUsize::new(0)),
        }
    }

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
            target_endpoints: Vec::new(),
            rr_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    pub fn with_target_endpoints(mut self, endpoints: Vec<SocketAddr>) -> Self {
        self.target_endpoints = endpoints;
        self
    }

    #[inline]
    pub fn select_target(&self) -> Option<SocketAddr> {
        if self.target_endpoints.is_empty() {
            return None;
        }
        let idx = self.rr_index.fetch_add(1, Ordering::Relaxed);
        Some(self.target_endpoints[idx % self.target_endpoints.len()])
    }

    #[inline]
    pub fn matches_host(&self, host: Option<&str>) -> bool {
        let Some(ref expected) = self.host else {
            return true;
        };
        let Some(actual) = host else {
            return false;
        };

        let actual_clean = actual.split(':').next().unwrap_or(actual);

        if expected == "*" {
            return true;
        }

        if let Some(suffix) = expected.strip_prefix("*.") {
            if actual_clean.len() > suffix.len() {
                let dot_idx = actual_clean.len() - suffix.len() - 1;
                if actual_clean.as_bytes()[dot_idx] == b'.' {
                    return actual_clean[dot_idx + 1..].eq_ignore_ascii_case(suffix);
                }
            }
            false
        } else {
            expected.eq_ignore_ascii_case(actual_clean)
        }
    }

    #[inline]
    pub fn matches_method(&self, method: Option<&str>) -> bool {
        let Some(ref exp_method) = self.method else {
            return true;
        };
        let Some(act_method) = method else {
            return false;
        };
        exp_method.eq_ignore_ascii_case(act_method)
    }
}

/// Zero-allocation view of an incoming HTTP/1.1 request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct Http1RouteRequest<'a> {
    pub path: &'a str,
    pub host: Option<&'a str>,
    pub method: Option<&'a str>,
}

impl<'a> Http1RouteRequest<'a> {
    #[inline]
    pub fn new(path: &'a str) -> Self {
        Self {
            path,
            host: None,
            method: None,
        }
    }

    #[inline]
    pub fn with_host(mut self, host: &'a str) -> Self {
        self.host = Some(host);
        self
    }

    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self
    }
}

/// Route table for a single HTTP/1.1 listener.
#[derive(Debug, Clone)]
pub struct ListenerHttp1Router {
    exact_routes: HashMap<String, Vec<Http1Route>>,
    automaton: Option<AhoCorasick>,
    prefix_routes: Vec<Vec<Http1Route>>,
}

impl ListenerHttp1Router {
    pub fn new(routes: Vec<Http1Route>) -> Result<Self, RouterError> {
        let mut exact_map: HashMap<String, Vec<Http1Route>> = HashMap::new();
        let mut prefix_map: HashMap<String, Vec<Http1Route>> = HashMap::new();

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
                    detail: format!("failed to build Aho-Corasick for HTTP/1.1: {e}"),
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

    #[inline]
    pub fn route(&self, req: &Http1RouteRequest<'_>) -> Option<&Http1Route> {
        if let Some(candidates) = self.exact_routes.get(req.path)
            && let Some(r) = candidates
                .iter()
                .find(|r| r.matches_host(req.host) && r.matches_method(req.method))
        {
            return Some(r);
        }

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

        None
    }
}

/// Unified, thread-safe HTTP/1.1 routing table coordinating all HTTP/1.1 listeners.
#[derive(Debug, Default, Clone)]
pub struct Http1Router {
    listeners: HashMap<String, ListenerHttp1Router>,
}

impl Http1Router {
    pub fn new(routes: impl IntoIterator<Item = Http1Route>) -> Result<Self, RouterError> {
        let mut grouped: HashMap<String, Vec<Http1Route>> = HashMap::new();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = HashMap::with_capacity(grouped.len());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerHttp1Router::new(list)?);
        }

        Ok(Self { listeners })
    }

    #[inline]
    pub fn route(&self, listener_id: &str, req: &Http1RouteRequest<'_>) -> Option<&Http1Route> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}
