//! Canonical gRPC status code variants and branchless lookup tables (`velda-core::lut::grpc_status`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        GRPC_STATUS_NAMES (288 B L1D RESIDENT)                           │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ GrpcStatus            │ 0..=17        │ 18 variants    │ 288 bytes     │ Direct Index   │
//! └───────────────────────┴───────────────┴────────────────┴───────────────┴────────────────┘
//!
//!   gRPC Trailers / Ingress Error ──► GrpcStatus (u8: 0..=17)
//!   (RFC 7807 / gRPC Wire Codes)              │
//!                                             ├──────────────────────────┐
//!                                             ▼                          ▼
//!                                   GRPC_STATUS_NAMES[u8]      GRPC_STATUS_CODE_STRINGS[u8]
//!                                   ("OK", "CANCELLED", ...)   ("0", "1", "16", "17")
//! ```

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::{HeaderWorkspace, L7Response};

/// Canonical gRPC status code discriminant.
///
/// Variants 0..=16 correspond strictly to canonical gRPC status codes defined in RFC 7807 / gRPC Wire Protocol.
/// Variant 17 (`NonGrpc`) denotes that the request or pipeline was non-RPC (HTTP, Raw L4) or lacked gRPC trailers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum GrpcStatus {
    /// 0: Not an error; returned on success.
    Ok = 0,
    /// 1: The operation was cancelled (typically by caller).
    Cancelled = 1,
    /// 2: Unknown error.
    Unknown = 2,
    /// 3: Client specified an invalid argument.
    InvalidArgument = 3,
    /// 4: Deadline expired before operation could complete.
    DeadlineExceeded = 4,
    /// 5: Some requested entity was not found.
    NotFound = 5,
    /// 6: Some entity that we attempted to create already exists.
    AlreadyExists = 6,
    /// 7: The caller does not have permission to execute the specified operation.
    PermissionDenied = 7,
    /// 8: Some resource has been exhausted (quota, rate limit).
    ResourceExhausted = 8,
    /// 9: Operation was rejected because the system is not in a state required for execution.
    FailedPrecondition = 9,
    /// 10: The operation was aborted, typically due to a concurrency issue like sequencer check failures.
    Aborted = 10,
    /// 11: Operation was attempted past the valid range.
    OutOfRange = 11,
    /// 12: Operation is not implemented or not supported/enabled.
    Unimplemented = 12,
    /// 13: Internal errors. Means some invariants expected by underlying system have been broken.
    Internal = 13,
    /// 14: The service is currently unavailable. Most likely a transient condition.
    Unavailable = 14,
    /// 15: Unrecoverable data loss or corruption.
    DataLoss = 15,
    /// 16: The request does not have valid authentication credentials for the operation.
    Unauthenticated = 16,
    /// 17: Non-gRPC traffic (HTTP Web/REST, Raw L4 transport, or non-RPC pipeline).
    NonGrpc = 17,
}

/// Static lookup table mapping each `GrpcStatus` discriminant (0..17) directly to its
/// uppercase canonical ASCII name.
///
/// Resident in CPU L1 Data Cache ($18 \times 16\text{B} = 288\text{B}$).
pub static GRPC_STATUS_NAMES: [&str; 18] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
    "NON_GRPC",
];

/// Static lookup table mapping each `GrpcStatus` discriminant (0..17) directly to its
/// decimal string representation ("0", "1", ... "16", "17").
///
/// Resident in CPU L1 Data Cache ($18 \times 16\text{B} = 288\text{B}$).
pub static GRPC_STATUS_CODE_STRINGS: [&str; 18] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
    "17",
];

const GRPC_STATUS_NAME: HeaderName = HeaderName::from_static("grpc-status");
const GRPC_MESSAGE_NAME: HeaderName = HeaderName::from_static("grpc-message");
const APPLICATION_GRPC_VALUE: HeaderValue = HeaderValue::from_static("application/grpc");

/// Pre-built compile-time [`HeaderValue`] LUT mapping `GrpcStatus` (0..=17) in O(1)
/// with zero runtime byte-validation loops.
static GRPC_STATUS_HEADER_VALUES: [HeaderValue; 18] = {
    use std::mem::MaybeUninit;
    let mut arr: [MaybeUninit<HeaderValue>; 18] = [const { MaybeUninit::uninit() }; 18];
    let mut i = 0;
    while i < 18 {
        arr[i].write(HeaderValue::from_static(GRPC_STATUS_CODE_STRINGS[i]));
        i += 1;
    }
    // SAFETY: All 18 slots [0..18] are initialized above prior to transmuting.
    unsafe { std::mem::transmute::<[MaybeUninit<HeaderValue>; 18], [HeaderValue; 18]>(arr) }
};

impl GrpcStatus {
    /// Total number of distinct gRPC status variants.
    pub const COUNT: usize = 18;

    /// Static table containing all gRPC status variants in contiguous discriminant order.
    pub const ALL: [Self; 18] = [
        Self::Ok,
        Self::Cancelled,
        Self::Unknown,
        Self::InvalidArgument,
        Self::DeadlineExceeded,
        Self::NotFound,
        Self::AlreadyExists,
        Self::PermissionDenied,
        Self::ResourceExhausted,
        Self::FailedPrecondition,
        Self::Aborted,
        Self::OutOfRange,
        Self::Unimplemented,
        Self::Internal,
        Self::Unavailable,
        Self::DataLoss,
        Self::Unauthenticated,
        Self::NonGrpc,
    ];

    /// Converts a raw `u8` discriminant into a typed [`GrpcStatus`].
    ///
    /// Returns the matched variant if valid (0..=17), or [`GrpcStatus::NonGrpc`] if out of bounds.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::NonGrpc
        }
    }

    /// Converts a raw `u8` discriminant into `Some(GrpcStatus)` if valid (0..=17), or `None` if out of bounds.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Resolves the canonical uppercase ASCII status string via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        GRPC_STATUS_NAMES[self as usize]
    }

    /// Resolves the canonical uppercase ASCII status byte slice via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        GRPC_STATUS_NAMES[self as usize].as_bytes()
    }

    /// Returns the integer code value (0..=17).
    #[inline(always)]
    pub const fn code(self) -> u32 {
        self as u32
    }

    /// Returns the static decimal string representation of the integer code ("0".."17").
    #[inline(always)]
    pub const fn code_str(self) -> &'static str {
        GRPC_STATUS_CODE_STRINGS[self as usize]
    }

    /// Returns a pre-validated O(1) [`HeaderValue`] for this status code ("0".."17").
    #[inline(always)]
    pub fn code_header_value(self) -> HeaderValue {
        GRPC_STATUS_HEADER_VALUES[self as usize].clone()
    }

    /// Converts an integer code to a typed [`GrpcStatus`].
    #[inline(always)]
    pub const fn from_code(code: u32) -> Self {
        Self::from_wire_u32(code)
    }

    /// Constructs standard gRPC trailers in a [`HeaderWorkspace`] containing `grpc-status` and optional `grpc-message`.
    pub fn to_trailer_workspace(self, message: Option<&str>) -> HeaderWorkspace {
        let mut ws = HeaderWorkspace::empty(2);
        ws.insert(GRPC_STATUS_NAME, self.code_header_value());
        if let Some(val) = message.and_then(|m| HeaderValue::from_str(m).ok()) {
            ws.insert(GRPC_MESSAGE_NAME, val);
        }
        ws
    }

    /// Constructs a standard gRPC Trailers-Only [`L7Response`].
    pub fn to_l7_response(self, message: Option<&str>) -> L7Response {
        let mut resp = L7Response::empty(StatusCode::OK)
            .with_header(CONTENT_TYPE, APPLICATION_GRPC_VALUE)
            .with_header(GRPC_STATUS_NAME, self.code_header_value());

        if let Some(val) = message.and_then(|m| HeaderValue::from_str(m).ok()) {
            resp = resp.with_header(GRPC_MESSAGE_NAME, val);
        }
        resp
    }

    /// Maps an HTTP status code to a canonical [`GrpcStatus`] according to the
    /// official gRPC HTTP-to-gRPC status mapping specification.
    pub fn from_http_status(status: StatusCode) -> Self {
        match status.as_u16() {
            200 => Self::Ok,
            400 => Self::Internal,
            401 => Self::Unauthenticated,
            403 => Self::PermissionDenied,
            404 => Self::Unimplemented,
            429 | 502 | 503 | 504 => Self::Unavailable,
            _ => Self::Unknown,
        }
    }

    /// Maps a canonical [`RejectReason`] from `velda-core` to a canonical [`GrpcStatus`].
    pub fn from_reject_reason(reason: crate::RejectReason) -> Self {
        use crate::RejectReason;
        match reason {
            RejectReason::BadRequest
            | RejectReason::Conflict
            | RejectReason::UnsupportedMediaType
            | RejectReason::HeaderFieldsTooLarge
            | RejectReason::UriTooLong => Self::InvalidArgument,
            RejectReason::Unauthorized => Self::Unauthenticated,
            RejectReason::Forbidden | RejectReason::WafBlocked | RejectReason::IpBlocked => {
                Self::PermissionDenied
            }
            RejectReason::NotFound => Self::NotFound,
            RejectReason::MethodNotAllowed => Self::Unimplemented,
            RejectReason::RequestTimeout | RejectReason::GatewayTimeout => Self::DeadlineExceeded,
            RejectReason::PayloadTooLarge | RejectReason::TooManyRequests => {
                Self::ResourceExhausted
            }
            RejectReason::ClientClosedRequest => Self::Cancelled,
            RejectReason::InternalError => Self::Internal,
            RejectReason::BadGateway | RejectReason::ServiceUnavailable => Self::Unavailable,
        }
    }

    /// Maps a canonical [`GrpcStatus`] to a standard HTTP status code according to the
    /// official Google Cloud API design specification.
    pub fn to_http_status(self) -> StatusCode {
        match self {
            Self::Ok => StatusCode::OK,
            Self::Cancelled => StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            Self::Unknown | Self::NonGrpc => StatusCode::INTERNAL_SERVER_ERROR,
            Self::InvalidArgument => StatusCode::BAD_REQUEST,
            Self::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::AlreadyExists => StatusCode::CONFLICT,
            Self::PermissionDenied => StatusCode::FORBIDDEN,
            Self::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
            Self::FailedPrecondition => StatusCode::BAD_REQUEST,
            Self::Aborted => StatusCode::CONFLICT,
            Self::OutOfRange => StatusCode::BAD_REQUEST,
            Self::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
        }
    }

    /// Maps a raw wire integer status (as parsed from HTTP/2 `grpc-status` headers/trailers)
    /// into a typed [`GrpcStatus`].
    ///
    /// Values 0..=16 are preserved. Any unknown wire value outside the canonical RFC range
    /// is safely mapped to `GrpcStatus::Unknown` (2).
    #[inline(always)]
    pub const fn from_wire_u32(code: u32) -> Self {
        if code <= 16 {
            Self::ALL[code as usize]
        } else {
            Self::Unknown
        }
    }

    /// Returns `true` if this status indicates successful RPC completion (`OK`).
    #[inline(always)]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Returns `true` if this status represents an RPC client error
    /// (e.g. `InvalidArgument`, `NotFound`, `PermissionDenied`, `Unauthenticated`).
    #[inline(always)]
    pub const fn is_client_error(self) -> bool {
        matches!(
            self,
            Self::InvalidArgument
                | Self::NotFound
                | Self::AlreadyExists
                | Self::PermissionDenied
                | Self::FailedPrecondition
                | Self::OutOfRange
                | Self::Unauthenticated
        )
    }

    /// Returns `true` if this status represents an upstream or infrastructure error
    /// (e.g. `Internal`, `Unavailable`, `DataLoss`, `DeadlineExceeded`).
    #[inline(always)]
    pub const fn is_server_error(self) -> bool {
        matches!(
            self,
            Self::DeadlineExceeded
                | Self::Internal
                | Self::Unavailable
                | Self::DataLoss
                | Self::Unimplemented
        )
    }
}

const _: () = assert!(GrpcStatus::COUNT == GRPC_STATUS_NAMES.len());
const _: () = assert!(GrpcStatus::COUNT == GrpcStatus::ALL.len());
const _: () = assert!(std::mem::size_of::<GrpcStatus>() == 1);

impl TryFrom<u8> for GrpcStatus {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(v).ok_or(v)
    }
}

impl Default for GrpcStatus {
    #[inline(always)]
    fn default() -> Self {
        Self::NonGrpc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_status_dense_roundtrip() {
        for i in 0..18u8 {
            let s = GrpcStatus::from_u8(i);
            assert_eq!(s as u8, i);
            assert_eq!(GrpcStatus::ALL[i as usize], s);
            assert_eq!(GRPC_STATUS_NAMES[i as usize], s.as_str());
            assert_eq!(GrpcStatus::try_from_u8(i), Some(s));
        }
        assert_eq!(GrpcStatus::from_u8(18), GrpcStatus::NonGrpc);
        assert_eq!(GrpcStatus::from_u8(255), GrpcStatus::NonGrpc);
        assert!(GrpcStatus::try_from_u8(18).is_none());
        assert!(GrpcStatus::try_from_u8(255).is_none());
    }

    #[test]
    fn test_from_wire_u32() {
        assert_eq!(GrpcStatus::from_wire_u32(0), GrpcStatus::Ok);
        assert_eq!(GrpcStatus::from_wire_u32(14), GrpcStatus::Unavailable);
        assert_eq!(GrpcStatus::from_wire_u32(16), GrpcStatus::Unauthenticated);
        assert_eq!(GrpcStatus::from_wire_u32(999), GrpcStatus::Unknown);
        assert_eq!(GrpcStatus::from_code(14), GrpcStatus::Unavailable);
        assert_eq!(GrpcStatus::Ok.code(), 0);
        assert_eq!(GrpcStatus::Ok.code_str(), "0");
        assert_eq!(GrpcStatus::Unavailable.code_str(), "14");
    }

    #[test]
    fn test_grpc_status_http_mapping_roundtrip() {
        assert_eq!(GrpcStatus::from_http_status(StatusCode::OK), GrpcStatus::Ok);
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::BAD_REQUEST),
            GrpcStatus::Internal
        );
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::UNAUTHORIZED),
            GrpcStatus::Unauthenticated
        );
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::FORBIDDEN),
            GrpcStatus::PermissionDenied
        );
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::NOT_FOUND),
            GrpcStatus::Unimplemented
        );
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::BAD_GATEWAY),
            GrpcStatus::Unavailable
        );
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::SERVICE_UNAVAILABLE),
            GrpcStatus::Unavailable
        );
        assert_eq!(
            GrpcStatus::from_http_status(StatusCode::GATEWAY_TIMEOUT),
            GrpcStatus::Unavailable
        );

        assert_eq!(GrpcStatus::Ok.to_http_status(), StatusCode::OK);
        assert_eq!(GrpcStatus::NotFound.to_http_status(), StatusCode::NOT_FOUND);
        assert_eq!(
            GrpcStatus::Unavailable.to_http_status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn test_grpc_status_from_reject_reason() {
        use crate::RejectReason;
        assert_eq!(
            GrpcStatus::from_reject_reason(RejectReason::Forbidden),
            GrpcStatus::PermissionDenied
        );
        assert_eq!(
            GrpcStatus::from_reject_reason(RejectReason::Unauthorized),
            GrpcStatus::Unauthenticated
        );
        assert_eq!(
            GrpcStatus::from_reject_reason(RejectReason::TooManyRequests),
            GrpcStatus::ResourceExhausted
        );
        assert_eq!(
            GrpcStatus::from_reject_reason(RejectReason::BadRequest),
            GrpcStatus::InvalidArgument
        );
        assert_eq!(
            GrpcStatus::from_reject_reason(RejectReason::PayloadTooLarge),
            GrpcStatus::ResourceExhausted
        );
        assert_eq!(
            GrpcStatus::from_reject_reason(RejectReason::ServiceUnavailable),
            GrpcStatus::Unavailable
        );
    }

    #[test]
    fn test_grpc_status_to_l7_and_trailers() {
        let resp = GrpcStatus::PermissionDenied.to_l7_response(Some("access denied"));
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/grpc"
        );
        assert_eq!(resp.headers.get("grpc-status").unwrap(), "7");
        assert_eq!(resp.headers.get("grpc-message").unwrap(), "access denied");

        let trailers = GrpcStatus::ResourceExhausted.to_trailer_workspace(Some("rate limited"));
        assert_eq!(trailers.get("grpc-status").unwrap(), "8");
        assert_eq!(trailers.get("grpc-message").unwrap(), "rate limited");
    }
}
