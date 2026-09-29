//! Layer 7 gRPC routing rules, match requests, and in-memory lookup tables.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;

/// Concrete compiled gRPC Layer 7 routing rule.
#[derive(Debug, Clone)]
pub struct GrpcRoute {
    pub id: RouteId,
    pub listener_id: String,
    pub service: String,
    pub method: Option<String>,
    pub authority: Option<String>,
    pub upstream_id: UpstreamId,
    pub upstream_name: String,
    pub plugins: Vec<String>,
    pub target_endpoints: Vec<SocketAddr>,
    rr_index: Arc<AtomicUsize>,
}

impl PartialEq for GrpcRoute {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.listener_id == other.listener_id
            && self.service == other.service
            && self.method == other.method
            && self.authority == other.authority
            && self.upstream_id == other.upstream_id
            && self.upstream_name == other.upstream_name
            && self.plugins == other.plugins
            && self.target_endpoints == other.target_endpoints
    }
}

impl Eq for GrpcRoute {}

impl GrpcRoute {
    pub fn new(
        id: RouteId,
        listener_id: impl Into<String>,
        service: impl Into<String>,
        upstream_id: UpstreamId,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            id,
            listener_id: listener_id.into(),
            service: service.into(),
            method: None,
            authority: None,
            upstream_id,
            upstream_name: upstream_name.into(),
            plugins: Vec::new(),
            target_endpoints: Vec::new(),
            rr_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn with_authority(mut self, authority: impl Into<String>) -> Self {
        self.authority = Some(authority.into());
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
    pub fn matches_authority(&self, authority: Option<&str>) -> bool {
        let Some(ref expected) = self.authority else {
            return true;
        };
        let Some(actual) = authority else {
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
        exp_method == act_method
    }
}

/// Zero-allocation view of an incoming gRPC request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct GrpcRouteRequest<'a> {
    pub service: &'a str,
    pub method: Option<&'a str>,
    pub authority: Option<&'a str>,
}

impl<'a> GrpcRouteRequest<'a> {
    #[inline]
    pub fn new(service: &'a str, method: Option<&'a str>) -> Self {
        Self {
            service,
            method,
            authority: None,
        }
    }

    #[inline]
    pub fn from_path(path: &'a str, authority: Option<&'a str>) -> Option<Self> {
        let trimmed = path.strip_prefix('/')?;
        let mut parts = trimmed.splitn(2, '/');
        let service = parts.next()?;
        if service.is_empty() {
            return None;
        }
        let method = parts.next().filter(|m| !m.is_empty());
        Some(Self {
            service,
            method,
            authority,
        })
    }

    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self
    }

    #[inline]
    pub fn with_authority(mut self, authority: &'a str) -> Self {
        self.authority = Some(authority);
        self
    }
}

/// Route table for a single gRPC listener.
#[derive(Debug, Clone)]
pub struct ListenerGrpcRouter {
    service_routes: HashMap<String, Vec<GrpcRoute>>,
    catch_all: Vec<GrpcRoute>,
}

impl ListenerGrpcRouter {
    pub fn new(routes: Vec<GrpcRoute>) -> Result<Self, RouterError> {
        let mut service_map: HashMap<String, Vec<GrpcRoute>> = HashMap::new();
        let mut catch_all = Vec::new();

        for r in routes {
            if r.service == "*" || r.service.is_empty() {
                catch_all.push(r);
            } else {
                service_map.entry(r.service.clone()).or_default().push(r);
            }
        }

        Ok(Self {
            service_routes: service_map,
            catch_all,
        })
    }

    #[inline]
    pub fn route(&self, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        if let Some(candidates) = self.service_routes.get(req.service)
            && let Some(r) = candidates
                .iter()
                .find(|r| r.matches_authority(req.authority) && r.matches_method(req.method))
        {
            return Some(r);
        }

        self.catch_all
            .iter()
            .find(|r| r.matches_authority(req.authority) && r.matches_method(req.method))
    }
}

/// Unified, thread-safe gRPC routing table coordinating all gRPC listeners.
#[derive(Debug, Default, Clone)]
pub struct GrpcRouter {
    listeners: HashMap<String, ListenerGrpcRouter>,
}

impl GrpcRouter {
    pub fn new(routes: impl IntoIterator<Item = GrpcRoute>) -> Result<Self, RouterError> {
        let mut grouped: HashMap<String, Vec<GrpcRoute>> = HashMap::new();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = HashMap::with_capacity(grouped.len());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerGrpcRouter::new(list)?);
        }

        Ok(Self { listeners })
    }

    #[inline]
    pub fn route(&self, listener_id: &str, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}
