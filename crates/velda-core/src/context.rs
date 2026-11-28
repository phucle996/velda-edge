//! Flat Request Context & State (`velda_core::context`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                           FLAT REQUEST CONTEXT COMPOSITION                              │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  RequestContext                                                                         │
//! │    ├── l4: L4Request        (connection_id: u64, peer/local ip: u128 + port_be: u16)    │
//! │    ├── l7: L7Request        (method, uri, headers, body)                                │
//! │    └── state: RequestState  (request_id: u128, route_id: u16, upstream_id: u16,         │
//! │                              upstream_ip: u128, upstream_port_be: u16,                  │
//! │                              rejection: u8, upstream_attempts: u16, timestamps...)      │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use crate::l4::request::L4Request;
use crate::l7::request::L7Request;
use crate::lut::HttpMethod;

/// Sentinel value indicating no route has been matched (`u16::MAX`).
pub const UNMATCHED_ROUTE_ID: u16 = u16::MAX;

/// Sentinel value indicating no upstream has been assigned (`u16::MAX`).
pub const UNASSIGNED_UPSTREAM_ID: u16 = u16::MAX;

/// Canonical 32-bit FNV-1a deterministic hash for compiling string names into `u32` IDs.
///
/// Offset basis: `0x811c9dc5`, Prime: `0x01000193`.
#[inline]
pub const fn fnv1a_hash_32(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

/// Canonical 64-bit FNV-1a deterministic fingerprint ID for JA4 byte slices.
///
/// Maps a canonical 36-byte JA4 string (e.g. `t13d1516h2_8daaf6152771_e5627efa2ab1`)
/// into a non-zero `u64` identifier (`0` is reserved for unavailable / non-TLS).
#[inline]
pub const fn ja4_fingerprint_id_bytes(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x0100_0000_01b3);
        i += 1;
    }
    if hash == 0 { 1 } else { hash }
}

/// Canonical 64-bit FNV-1a deterministic fingerprint ID for JA4 string slices.
#[inline]
pub const fn ja4_fingerprint_id(ja4: &str) -> u64 {
    ja4_fingerprint_id_bytes(ja4.as_bytes())
}

/// Context associated with a single request.
///
/// `RequestContext` is the main object passed through the Velda
/// request lifecycle and plugin hooks.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// L4 connection information.
    pub l4: L4Request,

    /// L7 HTTP request.
    pub l7: L7Request,

    /// Mutable state accumulated during request processing.
    pub state: RequestState,
}

impl RequestContext {
    /// Creates a new request context.
    #[inline]
    pub fn new(l4: L4Request, l7: L7Request, request_id: u128) -> Self {
        Self {
            l4,
            l7,
            state: RequestState::new(request_id),
        }
    }
}

/// Mutable state accumulated throughout the request lifecycle.
///
/// Compact state layout targeting a single 64-byte L1 Data Cache line (`size_of <= 64`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestState {
    /// Unique 128-bit W3C TraceContext identifier for this request.
    pub request_id: u128,

    /// Ingress arrival timestamp in nanoseconds from hardware clock.
    pub ingress_time_nanos: u64,

    /// Compact 64-bit JA4 TLS ClientHello fingerprint (`0` if non-TLS / unavailable).
    pub ja4: u64,

    /// Lower 32 bits of the 40-bit non-colliding L7 wire framing traits (`wire_traits`).
    pub wire_traits_lo: u32,

    /// Client IP address encoded as RFC 4291 `u128` integer (`0` if unset).
    pub client_ip: u128,

    /// Route selected by the router (`UNMATCHED_ROUTE_ID` if unmatched).
    pub route_id: u16,

    /// Upstream selected for the request (`UNASSIGNED_UPSTREAM_ID` if unassigned).
    pub upstream_id: u16,

    /// Non-colliding 16-bit TLS ClientHello structural traits bitfield (`tls_traits`).
    pub tls_traits: u16,

    /// Canonical HTTP method for the request.
    pub method: HttpMethod,

    /// Whether the request arrived over a TLS-encrypted downstream listener.
    pub is_tls: bool,

    /// Whether routing has completed.
    pub routed: bool,

    /// Whether the request has been sent to an upstream.
    pub upstream_started: bool,

    /// Whether a response has been received from the upstream.
    pub upstream_completed: bool,

    /// High 8 bits of the 40-bit non-colliding L7 wire framing traits (`wire_traits`).
    pub wire_traits_hi: u8,
}

const _: () = assert!(std::mem::size_of::<RequestState>() == 64);

impl Default for RequestState {
    #[inline]
    fn default() -> Self {
        Self::new(0)
    }
}

impl RequestState {
    /// Creates a new request state.
    #[inline]
    pub const fn new(request_id: u128) -> Self {
        let ingress_time_nanos = crate::request_id::timestamp_nanos(request_id);
        Self {
            request_id,
            ingress_time_nanos,
            ja4: 0,
            wire_traits_lo: 0,
            client_ip: 0,
            route_id: UNMATCHED_ROUTE_ID,
            upstream_id: UNASSIGNED_UPSTREAM_ID,
            tls_traits: 0,
            method: HttpMethod::Unknown,
            is_tls: false,
            routed: false,
            upstream_started: false,
            upstream_completed: false,
            wire_traits_hi: 0,
        }
    }

    /// Returns the packed 40-bit L7 wire framing traits integer (`wire_traits_lo | (wire_traits_hi << 32)`).
    #[inline(always)]
    pub const fn wire_traits(&self) -> u64 {
        (self.wire_traits_lo as u64) | ((self.wire_traits_hi as u64) << 32)
    }

    /// Stores a packed 40-bit L7 wire framing traits integer.
    #[inline(always)]
    pub fn set_wire_traits(&mut self, wire_traits: u64) {
        self.wire_traits_lo = wire_traits as u32;
        self.wire_traits_hi = ((wire_traits >> 32) & 0xFF) as u8;
    }

    /// Builder-style setter for `tls_traits`.
    #[inline(always)]
    pub const fn with_tls_traits(mut self, tls_traits: u16) -> Self {
        self.tls_traits = tls_traits;
        self
    }

    /// Builder-style setter for packed 40-bit `wire_traits`.
    #[inline(always)]
    pub const fn with_wire_traits(mut self, wire_traits: u64) -> Self {
        self.wire_traits_lo = wire_traits as u32;
        self.wire_traits_hi = ((wire_traits >> 32) & 0xFF) as u8;
        self
    }

    /// Attaches an explicit ingress timestamp in nanoseconds.
    #[inline(always)]
    pub fn with_ingress_time(mut self, ingress_time_nanos: u64) -> Self {
        self.ingress_time_nanos = ingress_time_nanos;
        self
    }

    /// Marks whether the request arrived over an encrypted TLS listener.
    #[inline(always)]
    pub const fn with_tls(mut self, is_tls: bool) -> Self {
        self.is_tls = is_tls;
        self
    }

    /// Returns the ingress timestamp in Unix epoch seconds.
    #[inline(always)]
    pub fn ingress_time_sec(&self) -> u64 {
        self.ingress_time_nanos / 1_000_000_000
    }

    /// Stores the HTTP method in request state.
    #[inline(always)]
    pub const fn with_method(mut self, method: HttpMethod) -> Self {
        self.method = method;
        self
    }

    /// Stores the HTTP method in request state.
    #[inline(always)]
    pub fn set_method(&mut self, method: HttpMethod) {
        self.method = method;
    }

    /// Stores the route selected by the router.
    #[inline]
    pub fn set_route(&mut self, route_id: u16) {
        self.route_id = route_id;
        self.routed = true;
    }

    /// Stores the upstream selected for the request.
    #[inline]
    pub fn set_upstream(&mut self, upstream_id: u16) {
        self.upstream_id = upstream_id;
    }

    /// Marks the upstream request as started.
    #[inline]
    pub fn mark_upstream_started(&mut self) {
        self.upstream_started = true;
    }

    /// Marks the upstream request as completed.
    #[inline]
    pub fn mark_upstream_completed(&mut self) {
        self.upstream_completed = true;
    }
}
