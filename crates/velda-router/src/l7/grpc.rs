//! Layer 7 gRPC routing rules, match requests, and in-memory lookup tables.
//!
//! ```text
//!                            gRPC ROUTE MATCHING
//!                                     │
//!                        ┌────────────▼────────────┐
//!                        │ Service Exact Lookup    │──[HIT]──► Check Authority/Method
//!                        └────────────┬────────────┘
//!                                     │ [MISS]
//!                        ┌────────────▼────────────┐
//!                        │ Catch-All Scan (*)      │──[HIT]──► Check Authority/Method
//!                        └────────────┬────────────┘
//!                                     │ [MISS]
//!                                     ▼
//!                                 None (404)
//! ```

use crate::arena::{ARENA_NONE, ArenaRouteEntry, RouteArena, RouteEntry};
use crate::error::RouterError;
use crate::host::{CompiledHost, HostBlocks, HostTable, host_specificity};

/// Concrete compiled gRPC Layer 7 route (8 bytes, `Copy`, zero heap strings).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcRoute {
    pub id: u16,
    pub listener_id: u32,
}

impl GrpcRoute {
    /// Creates a new gRPC route rule matching on full service name for router compilation.
    #[inline]
    #[allow(clippy::new_ret_no_self)]
    pub fn new(id: u16, listener_id: u32, service: impl Into<String>) -> GrpcRouteRule {
        GrpcRouteRule {
            id,
            listener_id,
            service: service.into(),
            method: None,
            authority: None,
            compiled_authority: CompiledHost::Any,
        }
    }
}

/// Builder specification for a gRPC route rule consumed during router compilation.
///
/// Pattern strings (`service`, `authority`) are consumed and dropped once compiled into
/// [`ListenerGrpcRouter`], leaving only 8-byte [`GrpcRoute`]s in the route table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrpcRouteRule {
    pub id: u16,
    pub listener_id: u32,
    pub service: String,
    pub method: Option<String>,
    pub authority: Option<String>,
    pub compiled_authority: CompiledHost,
}

impl GrpcRouteRule {
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

    /// Checks whether this route rule matches the given parsed request authority view.
    #[inline]
    pub fn matches_authority(&self, authority: &HostBlocks) -> bool {
        self.compiled_authority.matches(authority)
    }

    /// Checks whether this route rule matches the given request method view.
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
        if !req.parsed_authority.is_empty() {
            self.compiled_authority.matches(&req.parsed_authority)
        } else if let Some(a) = req.authority {
            self.compiled_authority.matches(&HostBlocks::parse(a))
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
    pub parsed_authority: HostBlocks,
}

impl<'a> GrpcRouteRequest<'a> {
    /// Creates a new gRPC route request view with service and optional method.
    #[inline]
    pub fn new(service: &'a str, method: Option<&'a str>) -> Self {
        Self {
            service,
            method,
            authority: None,
            parsed_authority: HostBlocks::default(),
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

        let parsed_authority = authority.map(HostBlocks::parse).unwrap_or_default();

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
        self.parsed_authority = HostBlocks::parse(authority);
        self
    }
}

#[inline(always)]
const fn grpc_method_tag(method: &[u8]) -> u16 {
    let mut h: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < method.len() {
        h ^= method[i] as u32;
        h = h.wrapping_mul(0x0100_0193);
        i += 1;
    }
    let tag = ((h >> 16) ^ h) as u16;
    if tag == 0xFFFF { 0xFFFE } else { tag }
}

/// Route table for a single gRPC listener backed by a 4KB page-aligned [`RouteArena`].
#[repr(C, align(64))]
#[derive(Debug, Clone, Default)]
pub struct ListenerGrpcRouter {
    arena: RouteArena,
}

impl ListenerGrpcRouter {
    /// Builds a new compiled gRPC listener routing table inside a 4KB [`RouteArena`].
    pub fn new(rules: Vec<GrpcRouteRule>) -> Result<Self, RouterError> {
        let listener_id = rules.first().map_or(0, |r| r.listener_id);
        let mut patterns: Vec<String> = Vec::with_capacity(rules.len());
        let mut is_catch_all = Vec::with_capacity(rules.len());

        for r in &rules {
            let catch_all = r.service == "*" || r.service.is_empty();
            is_catch_all.push(catch_all);
            if catch_all {
                patterns.push(String::new());
            } else if let Some(ref m) = r.method {
                patterns.push(format!("{}/{m}", r.service));
            } else {
                patterns.push(r.service.clone());
            }
        }

        let mut entries = Vec::with_capacity(rules.len());
        for ((r, pat), &catch_all) in rules.iter().zip(patterns.iter()).zip(is_catch_all.iter()) {
            let method_mask = if catch_all {
                r.method
                    .as_deref()
                    .map_or(0xFFFF, |m| grpc_method_tag(m.as_bytes()))
            } else {
                0xFFFF
            };
            entries.push(ArenaRouteEntry {
                pattern: pat.as_str(),
                route_id: r.id,
                method_mask,
                host: r.compiled_authority.as_pattern(),
                specificity: host_specificity(r.authority.as_deref()),
                is_prefix: false,
                is_catch_all: catch_all,
            });
        }

        Ok(Self {
            arena: RouteArena::build(listener_id, entries),
        })
    }

    #[inline(always)]
    fn match_entries(
        cands: &[RouteEntry],
        host_table: &HostTable,
        req: &GrpcRouteRequest<'_>,
        listener_id: u32,
        check_method_tag: bool,
        parsed_fallback: &mut Option<HostBlocks>,
    ) -> Option<GrpcRoute> {
        for c in cands {
            if check_method_tag && c.method_mask != 0xFFFF {
                let Some(m) = req.method else {
                    continue;
                };
                if grpc_method_tag(m.as_bytes()) != c.method_mask {
                    continue;
                }
            }
            if c.host_id == ARENA_NONE {
                return Some(GrpcRoute {
                    id: c.route_id,
                    listener_id,
                });
            }
            let actual_auth = if !req.parsed_authority.is_empty() {
                &req.parsed_authority
            } else if let Some(a) = req.authority {
                parsed_fallback.get_or_insert_with(|| HostBlocks::parse(a))
            } else {
                continue;
            };
            if host_table.matches(c.host_id, actual_auth) {
                return Some(GrpcRoute {
                    id: c.route_id,
                    listener_id,
                });
            }
        }
        None
    }

    /// Resolves a gRPC route for an incoming request view, returning an 8-byte [`GrpcRoute`] on register.
    #[inline(always)]
    pub fn route(&self, req: &GrpcRouteRequest<'_>) -> Option<GrpcRoute> {
        let host_table = self.arena.host_table();
        let listener_id = self.arena.header().listener_id;
        let mut parsed_fallback: Option<HostBlocks> = None;

        let (method_cands, svc_cands) = self.arena.match_grpc(req.service, req.method);

        if let Some((start, count)) = method_cands
            && let Some(route) = Self::match_entries(
                self.arena.node_entries(start, count),
                host_table,
                req,
                listener_id,
                false,
                &mut parsed_fallback,
            )
        {
            return Some(route);
        }

        if let Some((start, count)) = svc_cands
            && let Some(route) = Self::match_entries(
                self.arena.node_entries(start, count),
                host_table,
                req,
                listener_id,
                false,
                &mut parsed_fallback,
            )
        {
            return Some(route);
        }

        let catch_all = self.arena.catch_all_entries();
        if !catch_all.is_empty() {
            return Self::match_entries(
                catch_all,
                host_table,
                req,
                listener_id,
                true,
                &mut parsed_fallback,
            );
        }

        None
    }
}

/// Unified, thread-safe gRPC routing table coordinating all gRPC listeners (0 `FxHashMap`).
#[derive(Debug, Default, Clone)]
pub struct GrpcRouter {
    listeners: Vec<(u32, ListenerGrpcRouter)>,
}

impl GrpcRouter {
    /// Builds a new [`GrpcRouter`] from a list of [`GrpcRouteRule`] specifications.
    pub fn new(routes: impl IntoIterator<Item = GrpcRouteRule>) -> Result<Self, RouterError> {
        let mut all_rules: Vec<GrpcRouteRule> = routes.into_iter().collect();
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
            listeners.push((lid, ListenerGrpcRouter::new(group)?));
        }

        Ok(Self { listeners })
    }

    /// Resolves a gRPC route for an incoming request on a listener.
    #[inline]
    pub fn route(&self, listener_id: u32, req: &GrpcRouteRequest<'_>) -> Option<GrpcRoute> {
        let listener_router = self.listener(listener_id)?;
        listener_router.route(req)
    }

    /// Returns the compiled route table for a specific listener.
    #[inline]
    pub fn listener(&self, listener_id: u32) -> Option<&ListenerGrpcRouter> {
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

    use velda_core::fnv1a_hash_32;

    #[test]
    fn test_grpc_routing_exact_and_catchall() {
        let lid = fnv1a_hash_32("grpc-in");
        let routes = vec![
            GrpcRoute::new(1, lid, "helloworld.Greeter")
                .with_method("SayHello")
                .with_authority("grpc.velda.io"),
            GrpcRoute::new(2, lid, "*"),
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

        let r1 = router.route(lid, &req1).unwrap();
        assert_eq!(r1.id, 1);

        // Method mismatch falls back to catch-all
        let req2 = GrpcRouteRequest::from_path(
            "/helloworld.Greeter/SayGoodbye",
            Some("grpc.velda.io:50051"),
        )
        .unwrap();
        let r2 = router.route(lid, &req2).unwrap();
        assert_eq!(r2.id, 2);
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
