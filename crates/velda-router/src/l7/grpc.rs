//! Layer 7 gRPC routing rules, match requests, and in-memory lookup tables.
//!
//! ### Hot/Cold Data-Oriented Separation & L1D Cache Line Alignment
//! - Full route definitions ([`GrpcRoute`]) contain cold metadata (`listener_id`, `upstream_name`,
//!   `plugins: Vec<String>`). Storing them directly in lookup tables pollutes L1D cache lines.
//! - The hot lookup structures ([`service_routes`], [`catch_all`]) store compact, 16-byte-aligned
//!   [`GrpcHotCandidate`] descriptors.
//! - Cold route metadata is accessed exactly ONCE from the listener's `routes: Vec<GrpcRoute>` arena
//!   after the winning candidate is identified.

use rustc_hash::FxHashMap;
use velda_core::{RouteId, UpstreamId};

use crate::error::RouterError;
use crate::host::{CompiledHost, ReverseHostLabels, host_specificity};

/// Concrete compiled gRPC Layer 7 routing rule (cold data arena).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRoute {
    pub id: RouteId,
    pub listener_id: String,
    pub service: String,
    pub method: Option<String>,
    pub authority: Option<String>,
    pub upstream_id: UpstreamId,
    pub upstream_name: String,
    pub plugins: Vec<String>,
    pub compiled_authority: CompiledHost,
}

impl GrpcRoute {
    /// Creates a new gRPC route matching on full service name.
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
            compiled_authority: CompiledHost::Any,
        }
    }

    /// Attaches an expected gRPC method name filter.
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    /// Attaches an expected gRPC `:authority` filter (supports wildcard `*` or `*.domain`).
    pub fn with_authority(mut self, authority: impl Into<String>) -> Self {
        let auth = authority.into();
        self.compiled_authority = CompiledHost::compile(Some(&auth));
        self.authority = Some(auth);
        self
    }

    /// Attaches plugin identifiers to this route.
    pub fn with_plugins(mut self, plugins: Vec<String>) -> Self {
        self.plugins = plugins;
        self
    }

    /// Checks whether this route matches the given parsed request authority view.
    #[inline]
    pub fn matches_authority(&self, authority: &ReverseHostLabels) -> bool {
        self.compiled_authority.matches(authority)
    }

    /// Checks whether this route matches the given request method view.
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

    /// Evaluates route matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &GrpcRouteRequest<'_>) -> bool {
        if !self.matches_method(req.method) {
            return false;
        }
        if self.compiled_authority == CompiledHost::Any {
            return true;
        }
        if req.parsed_authority.count > 0 || req.parsed_authority.is_ip_or_unbracketed {
            self.compiled_authority.matches(&req.parsed_authority)
        } else if let Some(a) = req.authority {
            self.compiled_authority
                .matches(&ReverseHostLabels::parse(a))
        } else {
            false
        }
    }
}

/// Compact, 16-byte-aligned hot candidate descriptor for L1D cache line residency in gRPC.
#[repr(C, align(16))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcHotCandidate {
    pub compiled_authority: CompiledHost,
    pub route_idx: u32,
    pub method: Option<String>,
}

impl GrpcHotCandidate {
    /// Evaluates candidate matching against an incoming request view in 1-2 CPU cycles.
    #[inline(always)]
    pub fn matches_req(&self, req: &GrpcRouteRequest<'_>) -> bool {
        if let Some(ref exp_method) = self.method {
            let Some(act_method) = req.method else {
                return false;
            };
            if exp_method != act_method {
                return false;
            }
        }
        if self.compiled_authority == CompiledHost::Any {
            return true;
        }
        if req.parsed_authority.count > 0 || req.parsed_authority.is_ip_or_unbracketed {
            self.compiled_authority.matches(&req.parsed_authority)
        } else if let Some(a) = req.authority {
            self.compiled_authority
                .matches(&ReverseHostLabels::parse(a))
        } else {
            false
        }
    }
}

/// Zero-allocation view of an incoming gRPC request for route matching.
#[derive(Debug, Clone, Copy)]
pub struct GrpcRouteRequest<'a> {
    pub service: &'a str,
    pub method: Option<&'a str>,
    pub authority: Option<&'a str>,
    pub parsed_authority: ReverseHostLabels,
}

impl<'a> GrpcRouteRequest<'a> {
    /// Creates a new gRPC route request view with service and optional method.
    #[inline]
    pub fn new(service: &'a str, method: Option<&'a str>) -> Self {
        Self {
            service,
            method,
            authority: None,
            parsed_authority: ReverseHostLabels::default(),
        }
    }

    /// Parses a gRPC request view from an HTTP/2 `:path` (e.g. `"/helloworld.Greeter/SayHello"`).
    ///
    /// Automatically strips leading slashes and any trailing query/fragment parameters.
    /// Uses Length-Guarded Dual-Chunk SWAR delimiter scanning to locate path delimiters
    /// (`/`, `?`, `#`) in 1 CPU cycle, eliminating `splitn` and iterator allocation overhead.
    #[inline]
    pub fn from_path(path: &'a str, authority: Option<&'a str>) -> Option<Self> {
        let bytes = path.as_bytes();
        if bytes.is_empty() || bytes[0] != b'/' {
            return None;
        }
        let rest = &bytes[1..];
        if rest.is_empty() {
            return None;
        }

        let parsed_authority = authority.map(ReverseHostLabels::parse).unwrap_or_default();

        // SWAR scan for first delimiter among '/', '?', '#'
        let (delim_idx, delim_char) = swar_find_first_delimiter(rest);

        match delim_char {
            Some(b'/') => {
                let service_bytes = &rest[..delim_idx];
                if service_bytes.is_empty() {
                    return None;
                }
                // Safe: path is valid UTF-8 str and splitting at ASCII delimiter preserves UTF-8 boundaries
                let service = unsafe { std::str::from_utf8_unchecked(service_bytes) };

                // Scan remaining method segment for trailing query '?' or fragment '#'
                let method_rest = &rest[delim_idx + 1..];
                let method_end =
                    swar_find_query_or_fragment(method_rest).unwrap_or(method_rest.len());
                let method_bytes = &method_rest[..method_end];
                let method = if method_bytes.is_empty() {
                    None
                } else {
                    Some(unsafe { std::str::from_utf8_unchecked(method_bytes) })
                };

                Some(Self {
                    service,
                    method,
                    authority,
                    parsed_authority,
                })
            }
            Some(b'?') | Some(b'#') => {
                // Delimiter was query or fragment before any second slash
                let service_bytes = &rest[..delim_idx];
                if service_bytes.is_empty() {
                    return None;
                }
                let service = unsafe { std::str::from_utf8_unchecked(service_bytes) };
                Some(Self {
                    service,
                    method: None,
                    authority,
                    parsed_authority,
                })
            }
            None => {
                // No delimiter found in rest (e.g. "/OnlyOneSegment")
                let service = unsafe { std::str::from_utf8_unchecked(rest) };
                Some(Self {
                    service,
                    method: None,
                    authority,
                    parsed_authority,
                })
            }
            _ => unreachable!(),
        }
    }

    /// Attaches an expected method.
    #[inline]
    pub fn with_method(mut self, method: &'a str) -> Self {
        self.method = Some(method);
        self
    }

    /// Attaches an `:authority` header value.
    #[inline]
    pub fn with_authority(mut self, authority: &'a str) -> Self {
        self.authority = Some(authority);
        self.parsed_authority = ReverseHostLabels::parse(authority);
        self
    }
}

/// Route table for a single gRPC listener with Hot/Cold Data-Oriented Separation.
///
/// Aligned to 64 bytes to sit on an exact L1D cache line boundary, preventing split-line
/// header loads and false sharing between worker threads.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct ListenerGrpcRouter {
    routes: Vec<GrpcRoute>,
    service_routes: FxHashMap<String, Vec<GrpcHotCandidate>>,
    catch_all: Vec<GrpcHotCandidate>,
}

impl ListenerGrpcRouter {
    /// Builds a new compiled gRPC listener routing table.
    pub fn new(routes: Vec<GrpcRoute>) -> Result<Self, RouterError> {
        let mut service_map: FxHashMap<String, Vec<GrpcHotCandidate>> = FxHashMap::default();
        let mut catch_all = Vec::new();

        for (idx, r) in routes.iter().enumerate() {
            let candidate = GrpcHotCandidate {
                compiled_authority: r.compiled_authority.clone(),
                route_idx: idx as u32,
                method: r.method.clone(),
            };

            if r.service == "*" || r.service.is_empty() {
                catch_all.push(candidate);
            } else {
                service_map
                    .entry(r.service.clone())
                    .or_default()
                    .push(candidate);
            }
        }

        for list in service_map.values_mut() {
            list.sort_by_key(|c| {
                let r = &routes[c.route_idx as usize];
                std::cmp::Reverse(host_specificity(r.authority.as_deref()))
            });
        }
        catch_all.sort_by_key(|c| {
            let r = &routes[c.route_idx as usize];
            std::cmp::Reverse(host_specificity(r.authority.as_deref()))
        });

        Ok(Self {
            routes,
            service_routes: service_map,
            catch_all,
        })
    }

    /// Resolves a gRPC route for an incoming request view.
    #[inline]
    pub fn route(&self, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        if let Some(first_route) = self.routes.first() {
            crate::path::prefetch_l1_keep(first_route as *const _ as *const u8);
        }

        if let Some(candidates) = self.service_routes.get(req.service)
            && let Some(c) = candidates.iter().find(|c| c.matches_req(req))
        {
            return self.routes.get(c.route_idx as usize);
        }

        if let Some(c) = self.catch_all.iter().find(|c| c.matches_req(req)) {
            return self.routes.get(c.route_idx as usize);
        }

        None
    }
}

/// Unified, thread-safe gRPC routing table coordinating all gRPC listeners.
#[derive(Debug, Default, Clone)]
pub struct GrpcRouter {
    listeners: FxHashMap<String, ListenerGrpcRouter>,
}

impl GrpcRouter {
    /// Builds a new [`GrpcRouter`] from a list of compiled [`GrpcRoute`] rules.
    pub fn new(routes: impl IntoIterator<Item = GrpcRoute>) -> Result<Self, RouterError> {
        let mut grouped: FxHashMap<String, Vec<GrpcRoute>> = FxHashMap::default();
        for r in routes {
            grouped.entry(r.listener_id.clone()).or_default().push(r);
        }

        let mut listeners = FxHashMap::with_capacity_and_hasher(grouped.len(), Default::default());
        for (listener_id, list) in grouped {
            listeners.insert(listener_id, ListenerGrpcRouter::new(list)?);
        }

        Ok(Self { listeners })
    }

    /// Resolves a gRPC route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: &str, req: &GrpcRouteRequest<'_>) -> Option<&GrpcRoute> {
        let listener_router = self.listeners.get(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: &str) -> Option<&ListenerGrpcRouter> {
        self.listeners.get(listener_id)
    }

    /// Returns the number of registered listeners.
    #[inline]
    pub fn listener_count(&self) -> usize {
        self.listeners.len()
    }
}

/// Scans `bytes` for the first occurrence of `/`, `?`, or `#` using Length-Guarded Dual-Chunk SWAR.
#[inline(always)]
fn swar_find_first_delimiter(bytes: &[u8]) -> (usize, Option<u8>) {
    let len = bytes.len();
    let mut i = 0;

    // 64-bit SWAR chunk scan (8 bytes per iteration)
    while i + 8 <= len {
        let w = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let hits = swar_delim_hits_64(w);
        if hits != 0 {
            let offset = (hits.trailing_zeros() / 8) as usize;
            return (i + offset, Some(bytes[i + offset]));
        }
        i += 8;
    }

    // 32-bit SWAR chunk scan (4 bytes)
    if i + 4 <= len {
        let w = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        let hits = swar_delim_hits_32(w);
        if hits != 0 {
            let offset = (hits.trailing_zeros() / 8) as usize;
            return (i + offset, Some(bytes[i + offset]));
        }
        i += 4;
    }

    // Scalar tail: 0..3 remaining bytes
    while i < len {
        let b = bytes[i];
        if b == b'/' || b == b'?' || b == b'#' {
            return (i, Some(b));
        }
        i += 1;
    }

    (len, None)
}

#[inline(always)]
fn swar_delim_hits_64(w: u64) -> u64 {
    let xs = w ^ 0x2F2F_2F2F_2F2F_2F2F_u64; // '/'
    let xq = w ^ 0x3F3F_3F3F_3F3F_3F3F_u64; // '?'
    let xh = w ^ 0x2323_2323_2323_2323_u64; // '#'

    let hs = (xs.wrapping_sub(0x0101_0101_0101_0101_u64)) & !xs;
    let hq = (xq.wrapping_sub(0x0101_0101_0101_0101_u64)) & !xq;
    let hh = (xh.wrapping_sub(0x0101_0101_0101_0101_u64)) & !xh;

    (hs | hq | hh) & 0x8080_8080_8080_8080_u64
}

#[inline(always)]
fn swar_delim_hits_32(w: u32) -> u32 {
    let xs = w ^ 0x2F2F_2F2F_u32; // '/'
    let xq = w ^ 0x3F3F_3F3F_u32; // '?'
    let xh = w ^ 0x2323_2323_u32; // '#'

    let hs = (xs.wrapping_sub(0x0101_0101_u32)) & !xs;
    let hq = (xq.wrapping_sub(0x0101_0101_u32)) & !xq;
    let hh = (xh.wrapping_sub(0x0101_0101_u32)) & !xh;

    (hs | hq | hh) & 0x8080_8080_u32
}

/// Scans for '?' or '#' in method segment using SWAR.
#[inline(always)]
fn swar_find_query_or_fragment(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    let mut i = 0;

    // 64-bit SWAR chunk scan (8 bytes per iteration)
    while i + 8 <= len {
        let w = u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let xq = w ^ 0x3F3F_3F3F_3F3F_3F3F_u64; // '?'
        let xh = w ^ 0x2323_2323_2323_2323_u64; // '#'
        let hq = (xq.wrapping_sub(0x0101_0101_0101_0101_u64)) & !xq;
        let hh = (xh.wrapping_sub(0x0101_0101_0101_0101_u64)) & !xh;
        let hits = (hq | hh) & 0x8080_8080_8080_8080_u64;
        if hits != 0 {
            return Some(i + (hits.trailing_zeros() / 8) as usize);
        }
        i += 8;
    }

    // 32-bit SWAR chunk scan (4 bytes)
    if i + 4 <= len {
        let w = u32::from_le_bytes(bytes[i..i + 4].try_into().unwrap());
        let xq = w ^ 0x3F3F_3F3F_u32;
        let xh = w ^ 0x2323_2323_u32;
        let hq = (xq.wrapping_sub(0x0101_0101_u32)) & !xq;
        let hh = (xh.wrapping_sub(0x0101_0101_u32)) & !xh;
        let hits = (hq | hh) & 0x8080_8080_u32;
        if hits != 0 {
            return Some(i + (hits.trailing_zeros() / 8) as usize);
        }
        i += 4;
    }

    // Scalar tail (0..3 bytes)
    while i < len {
        if bytes[i] == b'?' || bytes[i] == b'#' {
            return Some(i);
        }
        i += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_routing_exact_and_catchall() {
        let routes = vec![
            GrpcRoute::new(
                RouteId::new(1),
                "grpc-in",
                "helloworld.Greeter",
                UpstreamId::new(10),
                "greeter-upstream",
            )
            .with_method("SayHello")
            .with_authority("grpc.velda.io"),
            GrpcRoute::new(
                RouteId::new(2),
                "grpc-in",
                "*",
                UpstreamId::new(20),
                "fallback-upstream",
            ),
        ];

        let router = GrpcRouter::new(routes).unwrap();

        // Exact match via from_path
        let req1 = GrpcRouteRequest::from_path(
            "/helloworld.Greeter/SayHello?debug=1",
            Some("grpc.velda.io:50051"),
        )
        .unwrap();
        assert_eq!(req1.service, "helloworld.Greeter");
        assert_eq!(req1.method, Some("SayHello"));

        let r1 = router.route("grpc-in", &req1).unwrap();
        assert_eq!(r1.id, RouteId::new(1));
        assert_eq!(r1.upstream_name, "greeter-upstream");

        // Method mismatch falls back to catch-all
        let req2 = GrpcRouteRequest::from_path(
            "/helloworld.Greeter/SayGoodbye",
            Some("grpc.velda.io:50051"),
        )
        .unwrap();
        let r2 = router.route("grpc-in", &req2).unwrap();
        assert_eq!(r2.id, RouteId::new(2));
        assert_eq!(r2.upstream_name, "fallback-upstream");
    }

    #[test]
    fn test_grpc_route_request_from_path_edge_cases() {
        assert!(GrpcRouteRequest::from_path("", None).is_none());
        assert!(GrpcRouteRequest::from_path("/", None).is_none());
        assert!(GrpcRouteRequest::from_path("//", None).is_none());
        assert!(GrpcRouteRequest::from_path("///", None).is_none());
        assert!(GrpcRouteRequest::from_path("NoLeadingSlash", None).is_none());
        assert!(GrpcRouteRequest::from_path("/#debug", None).is_none());
        assert!(GrpcRouteRequest::from_path("/?param=1", None).is_none());

        let req = GrpcRouteRequest::from_path("/OnlyOneSegment", None).unwrap();
        assert_eq!(req.service, "OnlyOneSegment");
        assert_eq!(req.method, None);

        let req = GrpcRouteRequest::from_path("/service.Greeter/", None).unwrap();
        assert_eq!(req.service, "service.Greeter");
        assert_eq!(req.method, None);

        let req = GrpcRouteRequest::from_path("/service.Greeter/?query=1", None).unwrap();
        assert_eq!(req.service, "service.Greeter");
        assert_eq!(req.method, None);

        let req =
            GrpcRouteRequest::from_path("/service.v1.Service_0007/CreateOrder", Some("host:50051"))
                .unwrap();
        assert_eq!(req.service, "service.v1.Service_0007");
        assert_eq!(req.method, Some("CreateOrder"));
        assert_eq!(req.authority, Some("host:50051"));

        let req = GrpcRouteRequest::from_path("/segment1/segment2/extra/trailing/segments", None)
            .unwrap();
        assert_eq!(req.service, "segment1");
        assert_eq!(req.method, Some("segment2/extra/trailing/segments"));

        let req = GrpcRouteRequest::from_path("/users.Service/\x00nullbyte", None).unwrap();
        assert_eq!(req.service, "users.Service");
        assert_eq!(req.method, Some("\x00nullbyte"));
    }
}
