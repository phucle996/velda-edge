//! Canonical Hot-Path State Machine & Transition LUT (`velda_core::lut::state_machine`).
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────────────────────┐
//! │                   VELDA EDGE HOT-PATH STATE MACHINE (41 STATES, 1 BYTE)                  │
//! ├──────────────────────┬─────────────────┬──────────────────┬──────────────────────────────┤
//! │ Domain               │ Value Range     │ Variants         │ FSM LUT (STATUS_SPECS[u8])   │
//! ├──────────────────────┼─────────────────┼──────────────────┼──────────────────────────────┤
//! │ Completion           │ 0               │ Completed        │ ─► on_pool_release(is_mux)   │
//! │ Core Lifecycle       │ 1..=20          │ Timeout/ACL/WAF  │     ├─► PoolKeep             │
//! │ Downstream / Parse   │ 21..=25         │ IO/TLS/Smug/Flood│     ├─► PoolStreamCanceled   │
//! │ Pool & Upstream      │ 26..=36         │ Pool/DNS/Stale/IO│     └─► PoolEvict            │
//! │ Runtime Fault        │ 37              │ InvalidConfig    │ ─► penalizes_endpoint()      │
//! │ Pool Lease States    │ 38..=40         │ Keep/Cancel/Evict│ ─► is_retryable_on_fresh()   │
//! └──────────────────────┴─────────────────┴──────────────────┴──────────────────────────────┘
//!
//!   Every Hot-Path Crate ──► Result<T, InternalStatus> ─────────► Observability & Wire Layer
//!   (sock/tls/pool/upstream/protocols/plugin/edge)                 ├─ Prometheus / OTLP / Logs
//!                                                                  └─ Protocol Wire Mapping
//! ```

use std::fmt;

use serde::{Deserialize, Serialize};

/// Canonical flat state discriminant shared across every hot-path subsystem.
///
/// Cold-path crates (`velda-sync`, bootstrap configuration, static LKG loading) retain
/// structured textual errors; every hot-path runtime subsystem (transport, security,
/// connection pool, upstream, protocol engines, plugins, observability, edge pipelines)
/// operates on this single 1-byte protocol-agnostic state machine without intermediate
/// status classes or sub-enums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum InternalStatus {
    /// 0: Request and response lifecycle completed normally.
    Completed = 0,
    /// 1: Downstream peer canceled stream or closed connection prematurely.
    ClientCanceled = 1,
    /// 2: Upstream backend failed or returned an unrecoverable gateway failure.
    UpstreamError = 2,
    /// 3: Upstream backend failed to respond within the configured request/call deadline.
    UpstreamTimeout = 3,
    /// 4: Downstream peer failed to transmit the request within the ingress timeout.
    RequestTimeout = 4,
    /// 5: No configured route or target resource matched the request.
    RouteNotFound = 5,
    /// 6: Unexpected runtime failure on the edge hot path.
    InternalError = 6,
    /// 7: Service unavailable (fault injection, degraded listener, or general unavailability).
    ServiceUnavailable = 7,
    /// 8: Malformed request line, headers, parameters, or protocol syntax.
    BadRequest = 8,
    /// 9: Authentication credential missing, expired, or invalid.
    Unauthorized = 9,
    /// 10: Request denied by access control policy.
    Forbidden = 10,
    /// 11: Operation, method, or action is not permitted.
    MethodNotAllowed = 11,
    /// 12: Request conflicts with the current resource state.
    Conflict = 12,
    /// 13: Request or message payload exceeds configured size limit.
    PayloadTooLarge = 13,
    /// 14: Request URI or path exceeds configured length limit.
    UriTooLong = 14,
    /// 15: Request media type or content encoding is not supported.
    UnsupportedMediaType = 15,
    /// 16: Request header section exceeds configured byte or count limit.
    HeaderFieldsTooLarge = 16,
    /// 17: Rate limit, quota, or memory overload shedding threshold exceeded.
    RateLimited = 17,
    /// 18: Blocked by Web Application Firewall rule inspection.
    WafBlocked = 18,
    /// 19: Blocked by IP/CIDR or TLS JA4 fingerprint filter.
    IpBlocked = 19,
    /// 20: Blocked by cross-layer bot detection heuristics.
    BotBlocked = 20,
    /// 21: Downstream socket I/O error other than orderly cancellation or timeout.
    DownstreamIo = 21,
    /// 22: Downstream transport security handshake or record layer failure.
    DownstreamTls = 22,
    /// 23: Ambiguous message framing or boundary smuggling attempt detected.
    SmugglingDetected = 23,
    /// 24: Client violated listener streaming policy.
    StreamingViolation = 24,
    /// 25: Control frame flood or rapid stream reset abuse detected.
    FloodDetected = 25,
    /// 26: No active or healthy endpoints available for the target upstream.
    NoEndpointsAvailable = 26,
    /// 27: Upstream connection pool capacity reached; no lease slot available.
    PoolExhausted = 27,
    /// 28: Timed out waiting for a connection lease from the upstream pool.
    PoolAcquisitionTimeout = 28,
    /// 29: Upstream hostname DNS resolution failed during endpoint selection.
    UpstreamDnsFailed = 29,
    /// 30: Physical transport connection attempt to the selected backend failed.
    UpstreamConnectFailed = 30,
    /// 31: Physical transport connection establishment exceeded connect timeout.
    UpstreamConnectTimeout = 31,
    /// 32: Upstream security handshake or protocol negotiation failed.
    UpstreamTlsFailed = 32,
    /// 33: Pooled persistent connection was closed by peer prior to request reuse.
    UpstreamStale = 33,
    /// 34: Upstream rejected the request cleanly prior to processing (graceful drain or stream refusal).
    UpstreamRefused = 34,
    /// 35: Socket I/O error while reading from or writing to an active upstream connection.
    UpstreamIo = 35,
    /// 36: Upstream returned malformed protocol frames or invalid wire syntax.
    UpstreamProtocolError = 36,
    /// 37: Invalid runtime configuration encountered while dispatching an upstream or pipe.
    InvalidRuntimeConfig = 37,
    /// 38: Pool lease state transition: keep connection healthy in pool (or no lease held).
    PoolKeep = 38,
    /// 39: Pool lease state transition: cancel multiplexed stream only while keeping parent connection.
    PoolStreamCanceled = 39,
    /// 40: Pool lease state transition: close and evict physical connection from pool.
    PoolEvict = 40,
}

/// Static policy specification and FSM transition row associated 1-to-1 with each [`InternalStatus`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusSpec {
    pub name: &'static str,
    pub on_pool_seq: InternalStatus,
    pub on_pool_mux: InternalStatus,
    pub penalize_endpoint: bool,
    pub retry_on_fresh: bool,
    pub close_downstream: bool,
    pub is_server_error: bool,
}

const fn spec(
    name: &'static str,
    on_pool_seq: InternalStatus,
    on_pool_mux: InternalStatus,
    penalize_endpoint: bool,
    retry_on_fresh: bool,
    close_downstream: bool,
    is_server_error: bool,
) -> StatusSpec {
    StatusSpec {
        name,
        on_pool_seq,
        on_pool_mux,
        penalize_endpoint,
        retry_on_fresh,
        close_downstream,
        is_server_error,
    }
}

/// Static lookup table mapping each `InternalStatus` discriminant (`0..41`) to its
/// lowercase `snake_case` ASCII identifier.
pub static INTERNAL_STATUS_NAMES: [&str; 41] = [
    "completed",
    "client_canceled",
    "upstream_error",
    "upstream_timeout",
    "request_timeout",
    "route_not_found",
    "internal_error",
    "service_unavailable",
    "bad_request",
    "unauthorized",
    "forbidden",
    "method_not_allowed",
    "conflict",
    "payload_too_large",
    "uri_too_long",
    "unsupported_media_type",
    "header_fields_too_large",
    "rate_limited",
    "waf_blocked",
    "ip_blocked",
    "bot_blocked",
    "downstream_io",
    "downstream_tls",
    "smuggling_detected",
    "streaming_violation",
    "flood_detected",
    "no_endpoints_available",
    "pool_exhausted",
    "pool_acquisition_timeout",
    "upstream_dns_failed",
    "upstream_connect_failed",
    "upstream_connect_timeout",
    "upstream_tls_failed",
    "upstream_stale",
    "upstream_refused",
    "upstream_io",
    "upstream_protocol_error",
    "invalid_runtime_config",
    "pool_keep",
    "pool_stream_canceled",
    "pool_evict",
];

/// Unified L1D-resident FSM transition and decision table indexed by `InternalStatus as usize` (`0..41`).
pub static STATUS_SPECS: [StatusSpec; 41] = [
    // 0: Completed
    spec(
        "completed",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 1: ClientCanceled
    spec(
        "client_canceled",
        InternalStatus::PoolEvict,
        InternalStatus::PoolStreamCanceled,
        false,
        false,
        true,
        false,
    ),
    // 2: UpstreamError
    spec(
        "upstream_error",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 3: UpstreamTimeout
    spec(
        "upstream_timeout",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 4: RequestTimeout
    spec(
        "request_timeout",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 5: RouteNotFound
    spec(
        "route_not_found",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 6: InternalError
    spec(
        "internal_error",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        false,
        false,
        true,
        true,
    ),
    // 7: ServiceUnavailable
    spec(
        "service_unavailable",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        true,
    ),
    // 8: BadRequest
    spec(
        "bad_request",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 9: Unauthorized
    spec(
        "unauthorized",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 10: Forbidden
    spec(
        "forbidden",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 11: MethodNotAllowed
    spec(
        "method_not_allowed",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 12: Conflict
    spec(
        "conflict",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 13: PayloadTooLarge
    spec(
        "payload_too_large",
        InternalStatus::PoolEvict,
        InternalStatus::PoolStreamCanceled,
        false,
        false,
        true,
        false,
    ),
    // 14: UriTooLong
    spec(
        "uri_too_long",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 15: UnsupportedMediaType
    spec(
        "unsupported_media_type",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 16: HeaderFieldsTooLarge
    spec(
        "header_fields_too_large",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 17: RateLimited
    spec(
        "rate_limited",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 18: WafBlocked
    spec(
        "waf_blocked",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 19: IpBlocked
    spec(
        "ip_blocked",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 20: BotBlocked
    spec(
        "bot_blocked",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 21: DownstreamIo
    spec(
        "downstream_io",
        InternalStatus::PoolEvict,
        InternalStatus::PoolStreamCanceled,
        false,
        false,
        true,
        false,
    ),
    // 22: DownstreamTls
    spec(
        "downstream_tls",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 23: SmugglingDetected
    spec(
        "smuggling_detected",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        true,
        false,
    ),
    // 24: StreamingViolation
    spec(
        "streaming_violation",
        InternalStatus::PoolEvict,
        InternalStatus::PoolStreamCanceled,
        false,
        false,
        true,
        false,
    ),
    // 25: FloodDetected
    spec(
        "flood_detected",
        InternalStatus::PoolEvict,
        InternalStatus::PoolStreamCanceled,
        false,
        false,
        true,
        false,
    ),
    // 26: NoEndpointsAvailable
    spec(
        "no_endpoints_available",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        true,
    ),
    // 27: PoolExhausted
    spec(
        "pool_exhausted",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        true,
    ),
    // 28: PoolAcquisitionTimeout
    spec(
        "pool_acquisition_timeout",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        true,
    ),
    // 29: UpstreamDnsFailed
    spec(
        "upstream_dns_failed",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        true,
        false,
        false,
        true,
    ),
    // 30: UpstreamConnectFailed
    spec(
        "upstream_connect_failed",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 31: UpstreamConnectTimeout
    spec(
        "upstream_connect_timeout",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 32: UpstreamTlsFailed
    spec(
        "upstream_tls_failed",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 33: UpstreamStale
    spec(
        "upstream_stale",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        false,
        true,
        false,
        true,
    ),
    // 34: UpstreamRefused
    spec(
        "upstream_refused",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        false,
        true,
        false,
        true,
    ),
    // 35: UpstreamIo
    spec(
        "upstream_io",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 36: UpstreamProtocolError
    spec(
        "upstream_protocol_error",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        true,
        false,
        false,
        true,
    ),
    // 37: InvalidRuntimeConfig
    spec(
        "invalid_runtime_config",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        true,
    ),
    // 38: PoolKeep
    spec(
        "pool_keep",
        InternalStatus::PoolKeep,
        InternalStatus::PoolKeep,
        false,
        false,
        false,
        false,
    ),
    // 39: PoolStreamCanceled
    spec(
        "pool_stream_canceled",
        InternalStatus::PoolEvict,
        InternalStatus::PoolStreamCanceled,
        false,
        false,
        true,
        false,
    ),
    // 40: PoolEvict
    spec(
        "pool_evict",
        InternalStatus::PoolEvict,
        InternalStatus::PoolEvict,
        false,
        false,
        false,
        true,
    ),
];

impl InternalStatus {
    /// Total number of distinct internal state variants.
    pub const COUNT: usize = 41;

    /// Static table containing all variants in contiguous discriminant order.
    pub const ALL: [Self; 41] = [
        Self::Completed,
        Self::ClientCanceled,
        Self::UpstreamError,
        Self::UpstreamTimeout,
        Self::RequestTimeout,
        Self::RouteNotFound,
        Self::InternalError,
        Self::ServiceUnavailable,
        Self::BadRequest,
        Self::Unauthorized,
        Self::Forbidden,
        Self::MethodNotAllowed,
        Self::Conflict,
        Self::PayloadTooLarge,
        Self::UriTooLong,
        Self::UnsupportedMediaType,
        Self::HeaderFieldsTooLarge,
        Self::RateLimited,
        Self::WafBlocked,
        Self::IpBlocked,
        Self::BotBlocked,
        Self::DownstreamIo,
        Self::DownstreamTls,
        Self::SmugglingDetected,
        Self::StreamingViolation,
        Self::FloodDetected,
        Self::NoEndpointsAvailable,
        Self::PoolExhausted,
        Self::PoolAcquisitionTimeout,
        Self::UpstreamDnsFailed,
        Self::UpstreamConnectFailed,
        Self::UpstreamConnectTimeout,
        Self::UpstreamTlsFailed,
        Self::UpstreamStale,
        Self::UpstreamRefused,
        Self::UpstreamIo,
        Self::UpstreamProtocolError,
        Self::InvalidRuntimeConfig,
        Self::PoolKeep,
        Self::PoolStreamCanceled,
        Self::PoolEvict,
    ];

    /// Converts a raw `u8` discriminant into a typed [`InternalStatus`].
    ///
    /// Returns [`InternalStatus::Completed`] if out of range `0..41`.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::Completed
        }
    }

    /// Converts a raw `u8` discriminant into `Some(InternalStatus)` if in range `0..41`.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Parses a lowercase `snake_case` internal status name at config load time.
    pub fn from_name(name: &str) -> Option<Self> {
        let trimmed = name.trim();
        for (idx, &canonical) in INTERNAL_STATUS_NAMES.iter().enumerate() {
            if trimmed.eq_ignore_ascii_case(canonical) {
                return Some(Self::ALL[idx]);
            }
        }
        None
    }

    /// Returns the static [`StatusSpec`] FSM entry via O(1) direct indexing.
    #[inline(always)]
    pub const fn spec(self) -> &'static StatusSpec {
        &STATUS_SPECS[self as usize]
    }

    /// Resolves the canonical `snake_case` string via O(1) direct indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        INTERNAL_STATUS_NAMES[self as usize]
    }

    /// Resolves the canonical `snake_case` ASCII byte slice via O(1) direct indexing.
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        INTERNAL_STATUS_NAMES[self as usize].as_bytes()
    }

    /// Transitions this state to the resulting connection-pool lease state
    /// (`PoolKeep`, `PoolStreamCanceled`, or `PoolEvict`) via `STATUS_SPECS` lookup.
    #[inline(always)]
    pub const fn on_pool_release(self, is_multiplexed: bool) -> Self {
        if is_multiplexed {
            STATUS_SPECS[self as usize].on_pool_mux
        } else {
            STATUS_SPECS[self as usize].on_pool_seq
        }
    }

    /// Returns `true` when passive health tracking / circuit breaking should penalize the backend endpoint.
    #[inline(always)]
    pub const fn penalizes_endpoint(self) -> bool {
        STATUS_SPECS[self as usize].penalize_endpoint
    }

    /// Returns `true` when a transparent 1-shot retry on a fresh connection is safe.
    #[inline(always)]
    pub const fn is_retryable_on_fresh(self) -> bool {
        STATUS_SPECS[self as usize].retry_on_fresh
    }

    /// Returns `true` when the downstream connection should be closed immediately after response dispatch.
    #[inline(always)]
    pub const fn should_close_downstream(self) -> bool {
        STATUS_SPECS[self as usize].close_downstream
    }

    /// Returns `true` when this state represents an upstream/server/pool failure (ERROR log severity).
    #[inline(always)]
    pub const fn is_server_error(self) -> bool {
        STATUS_SPECS[self as usize].is_server_error
    }

    /// Returns `true` when the request completed normally (`InternalStatus::Completed`).
    #[inline(always)]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Completed)
    }

    /// Returns `true` when processing stopped short of normal upstream completion.
    #[inline(always)]
    pub const fn is_aborted(self) -> bool {
        !matches!(self, Self::Completed)
    }
}

impl fmt::Display for InternalStatus {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

const _: () = assert!(InternalStatus::COUNT == INTERNAL_STATUS_NAMES.len());
const _: () = assert!(InternalStatus::COUNT == STATUS_SPECS.len());
const _: () = assert!(InternalStatus::COUNT == InternalStatus::ALL.len());
const _: () = assert!(std::mem::size_of::<InternalStatus>() == 1);

impl TryFrom<u8> for InternalStatus {
    type Error = ();

    #[inline(always)]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(value).ok_or(())
    }
}

impl Default for InternalStatus {
    #[inline(always)]
    fn default() -> Self {
        Self::Completed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_internal_status_dense_roundtrip() {
        for i in 0..InternalStatus::COUNT as u8 {
            let s = InternalStatus::from_u8(i);
            assert_eq!(s as u8, i);
            assert_eq!(InternalStatus::ALL[i as usize], s);
            assert_eq!(s.as_str(), INTERNAL_STATUS_NAMES[i as usize]);
            assert_eq!(s.as_str(), STATUS_SPECS[i as usize].name);
            assert_eq!(s.as_bytes(), INTERNAL_STATUS_NAMES[i as usize].as_bytes());
            assert_eq!(InternalStatus::try_from_u8(i), Some(s));
            assert_eq!(InternalStatus::from_name(s.as_str()), Some(s));
            assert_eq!(format!("{s}"), s.as_str());
        }

        assert_eq!(
            InternalStatus::from_u8(InternalStatus::COUNT as u8),
            InternalStatus::Completed
        );
        assert_eq!(InternalStatus::from_u8(255), InternalStatus::Completed);
        assert!(InternalStatus::try_from_u8(InternalStatus::COUNT as u8).is_none());
        assert!(InternalStatus::try_from_u8(255).is_none());
        assert!(InternalStatus::from_name("unknown").is_none());
    }

    #[test]
    fn test_state_machine_policy_invariants() {
        assert!(InternalStatus::Completed.is_success());
        assert!(!InternalStatus::Completed.is_aborted());
        assert_eq!(
            InternalStatus::Completed.on_pool_release(false),
            InternalStatus::PoolKeep
        );
        assert_eq!(
            InternalStatus::Completed.on_pool_release(true),
            InternalStatus::PoolKeep
        );

        // Client cancellations evict sequential connections while keeping
        // multiplexed parent connections intact via stream cancellation.
        assert_eq!(
            InternalStatus::ClientCanceled.on_pool_release(false),
            InternalStatus::PoolEvict
        );
        assert_eq!(
            InternalStatus::ClientCanceled.on_pool_release(true),
            InternalStatus::PoolStreamCanceled
        );

        // Client cancellations and edge rejections never penalize upstream endpoint health.
        for s in [
            InternalStatus::ClientCanceled,
            InternalStatus::DownstreamIo,
            InternalStatus::BadRequest,
            InternalStatus::WafBlocked,
            InternalStatus::IpBlocked,
            InternalStatus::RateLimited,
        ] {
            assert!(
                !s.penalizes_endpoint(),
                "{s} must not penalize endpoint health"
            );
            assert!(!s.is_retryable_on_fresh(), "{s} must not trigger retry");
            assert!(
                !s.is_server_error(),
                "{s} must not report server error severity"
            );
        }

        // Transient stale/refused states evict connection & allow safe retry without penalizing endpoint.
        for s in [
            InternalStatus::UpstreamStale,
            InternalStatus::UpstreamRefused,
        ] {
            assert_eq!(s.on_pool_release(false), InternalStatus::PoolEvict);
            assert_eq!(s.on_pool_release(true), InternalStatus::PoolEvict);
            assert!(!s.penalizes_endpoint());
            assert!(s.is_retryable_on_fresh());
            assert!(s.is_server_error());
        }

        // Hard upstream faults evict and degrade endpoint health.
        for s in [
            InternalStatus::UpstreamConnectFailed,
            InternalStatus::UpstreamConnectTimeout,
            InternalStatus::UpstreamTlsFailed,
            InternalStatus::UpstreamTimeout,
            InternalStatus::UpstreamIo,
            InternalStatus::UpstreamProtocolError,
            InternalStatus::UpstreamError,
        ] {
            assert_eq!(s.on_pool_release(false), InternalStatus::PoolEvict);
            assert_eq!(s.on_pool_release(true), InternalStatus::PoolEvict);
            assert!(s.penalizes_endpoint());
            assert!(!s.is_retryable_on_fresh());
            assert!(s.is_server_error());
        }
    }
}
