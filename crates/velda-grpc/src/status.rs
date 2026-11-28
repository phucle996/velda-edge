//! Canonical gRPC wire status codes and `InternalStatus -> GrpcStatus` mapping (`velda_grpc::status`).
//!
//! ```text
//!   velda_core::InternalStatus (0..=20)
//!     └─► GrpcStatus::from_internal_status() ──► GrpcStatus (0..=16)
//!                                                   ├─ code_str() / code_header_value()
//!                                                   ├─ message() ("OK", "UNAVAILABLE", ...)
//!                                                   └─ to_l7_response / prepare_status_headers
//! ```

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use velda_core::{HeaderWorkspace, InternalStatus, L7Response};

/// Canonical gRPC wire status code discriminant (`0..=16`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    /// 10: The operation was aborted, typically due to a concurrency issue.
    Aborted = 10,
    /// 11: Operation was attempted past the valid range.
    OutOfRange = 11,
    /// 12: Operation is not implemented or not supported/enabled.
    Unimplemented = 12,
    /// 13: Internal error.
    Internal = 13,
    /// 14: Service is currently unavailable.
    Unavailable = 14,
    /// 15: Unrecoverable data loss or corruption.
    DataLoss = 15,
    /// 16: The request does not have valid authentication credentials.
    Unauthenticated = 16,
}

/// Canonical uppercase ASCII names indexed by `GrpcStatus as usize` (`0..=16`).
pub static GRPC_STATUS_NAMES: [&str; 17] = [
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
];

/// Canonical decimal wire strings indexed by `GrpcStatus as usize` (`0..=16`).
pub static GRPC_STATUS_CODE_STRINGS: [&str; 17] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
];

const GRPC_STATUS_NAME: HeaderName = HeaderName::from_static("grpc-status");
const GRPC_MESSAGE_NAME: HeaderName = HeaderName::from_static("grpc-message");
const APPLICATION_GRPC_VALUE: HeaderValue = HeaderValue::from_static("application/grpc");

static GRPC_STATUS_HEADER_VALUES: [HeaderValue; 17] = {
    use std::mem::MaybeUninit;
    let mut arr: [MaybeUninit<HeaderValue>; 17] = [const { MaybeUninit::uninit() }; 17];
    let mut i = 0;
    while i < 17 {
        arr[i].write(HeaderValue::from_static(GRPC_STATUS_CODE_STRINGS[i]));
        i += 1;
    }
    // SAFETY: All 17 slots [0..17] are initialized above prior to transmuting.
    unsafe { std::mem::transmute::<[MaybeUninit<HeaderValue>; 17], [HeaderValue; 17]>(arr) }
};

/// Default canonical `grpc-message` ASCII phrases mapped directly from [`InternalStatus`].
static INTERNAL_STATUS_GRPC_MESSAGES: [&str; InternalStatus::COUNT] = [
    "OK",                              // 0: Completed
    "Client Closed Request",           // 1: ClientCanceled
    "Bad Gateway",                     // 2: UpstreamError
    "Gateway Timeout",                 // 3: UpstreamTimeout
    "Request Timeout",                 // 4: RequestTimeout
    "Not Found",                       // 5: RouteNotFound
    "Internal Server Error",           // 6: InternalError
    "Service Unavailable",             // 7: ServiceUnavailable
    "Bad Request",                     // 8: BadRequest
    "Unauthorized",                    // 9: Unauthorized
    "Forbidden",                       // 10: Forbidden
    "Method Not Allowed",              // 11: MethodNotAllowed
    "Conflict",                        // 12: Conflict
    "Payload Too Large",               // 13: PayloadTooLarge
    "URI Too Long",                    // 14: UriTooLong
    "Unsupported Media Type",          // 15: UnsupportedMediaType
    "Request Header Fields Too Large", // 16: HeaderFieldsTooLarge
    "Too Many Requests",               // 17: RateLimited
    "Forbidden",                       // 18: WafBlocked
    "Forbidden",                       // 19: IpBlocked
    "Forbidden",                       // 20: BotBlocked
    "Cancelled",                       // 21: DownstreamIo
    "Cancelled",                       // 22: DownstreamTls
    "Bad Request",                     // 23: SmugglingDetected
    "Bad Request",                     // 24: StreamingViolation
    "Too Many Requests",               // 25: FloodDetected
    "Service Unavailable",             // 26: NoEndpointsAvailable
    "Service Unavailable",             // 27: PoolExhausted
    "Service Unavailable",             // 28: PoolAcquisitionTimeout
    "Bad Gateway",                     // 29: UpstreamDnsFailed
    "Bad Gateway",                     // 30: UpstreamConnectFailed
    "Gateway Timeout",                 // 31: UpstreamConnectTimeout
    "Bad Gateway",                     // 32: UpstreamTlsFailed
    "Bad Gateway",                     // 33: UpstreamStale
    "Bad Gateway",                     // 34: UpstreamRefused
    "Bad Gateway",                     // 35: UpstreamIo
    "Bad Gateway",                     // 36: UpstreamProtocolError
    "Internal Server Error",           // 37: InvalidRuntimeConfig
    "OK",                              // 38: PoolKeep
    "Cancelled",                       // 39: PoolStreamCanceled
    "Service Unavailable",             // 40: PoolEvict
];

impl GrpcStatus {
    /// Total number of canonical gRPC status codes (`0..=16`).
    pub const COUNT: usize = 17;

    /// Static table containing all canonical variants in contiguous discriminant order.
    pub const ALL: [Self; 17] = [
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
    ];

    /// Converts a raw wire integer (`0..=16`), mapping unknown values to [`GrpcStatus::Unknown`].
    #[inline(always)]
    pub const fn from_wire_u32(code: u32) -> Self {
        if code <= 16 {
            Self::ALL[code as usize]
        } else {
            Self::Unknown
        }
    }

    /// Converts a raw wire `u8` (`0..=16`), mapping unknown values to [`GrpcStatus::Unknown`].
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        Self::from_wire_u32(v as u32)
    }

    /// Converts a `u32` status code to [`GrpcStatus`].
    #[inline(always)]
    pub const fn from_code(code: u32) -> Self {
        Self::from_wire_u32(code)
    }

    /// Returns the numeric gRPC status code (`0..=16`).
    #[inline(always)]
    pub const fn code(self) -> u8 {
        self as u8
    }

    /// Returns the decimal wire string (`"0"`..`"16"`).
    #[inline(always)]
    pub const fn code_str(self) -> &'static str {
        GRPC_STATUS_CODE_STRINGS[self as usize]
    }

    /// Returns a pre-validated [`HeaderValue`] (`"0"`..`"16"`).
    #[inline(always)]
    pub fn code_header_value(self) -> HeaderValue {
        GRPC_STATUS_HEADER_VALUES[self as usize].clone()
    }

    /// Returns the uppercase canonical gRPC status name (`"OK"`, `"UNAVAILABLE"`, ...).
    #[inline(always)]
    pub const fn name(self) -> &'static str {
        GRPC_STATUS_NAMES[self as usize]
    }

    /// Maps an [`InternalStatus`] from `velda-core` into a wire [`GrpcStatus`].
    #[inline]
    pub const fn from_internal_status(status: InternalStatus) -> Self {
        match status {
            InternalStatus::Completed | InternalStatus::PoolKeep => Self::Ok,
            InternalStatus::ClientCanceled
            | InternalStatus::DownstreamIo
            | InternalStatus::DownstreamTls
            | InternalStatus::PoolStreamCanceled => Self::Cancelled,
            InternalStatus::UpstreamError
            | InternalStatus::ServiceUnavailable
            | InternalStatus::NoEndpointsAvailable
            | InternalStatus::PoolExhausted
            | InternalStatus::PoolAcquisitionTimeout
            | InternalStatus::UpstreamDnsFailed
            | InternalStatus::UpstreamConnectFailed
            | InternalStatus::UpstreamTlsFailed
            | InternalStatus::UpstreamStale
            | InternalStatus::UpstreamRefused
            | InternalStatus::UpstreamIo
            | InternalStatus::UpstreamProtocolError
            | InternalStatus::PoolEvict => Self::Unavailable,
            InternalStatus::UpstreamTimeout
            | InternalStatus::UpstreamConnectTimeout
            | InternalStatus::RequestTimeout => Self::DeadlineExceeded,
            InternalStatus::RouteNotFound => Self::NotFound,
            InternalStatus::InternalError | InternalStatus::InvalidRuntimeConfig => Self::Internal,
            InternalStatus::BadRequest
            | InternalStatus::Conflict
            | InternalStatus::UriTooLong
            | InternalStatus::UnsupportedMediaType
            | InternalStatus::HeaderFieldsTooLarge
            | InternalStatus::SmugglingDetected
            | InternalStatus::StreamingViolation => Self::InvalidArgument,
            InternalStatus::Unauthorized => Self::Unauthenticated,
            InternalStatus::Forbidden
            | InternalStatus::WafBlocked
            | InternalStatus::IpBlocked
            | InternalStatus::BotBlocked => Self::PermissionDenied,
            InternalStatus::MethodNotAllowed => Self::Unimplemented,
            InternalStatus::PayloadTooLarge
            | InternalStatus::RateLimited
            | InternalStatus::FloodDetected => Self::ResourceExhausted,
        }
    }

    /// Returns the canonical diagnostic phrase for an [`InternalStatus`].
    #[inline(always)]
    pub const fn internal_status_message(status: InternalStatus) -> &'static str {
        INTERNAL_STATUS_GRPC_MESSAGES[status as usize]
    }

    /// Maps an HTTP status code to a [`GrpcStatus`] per the gRPC HTTP-to-gRPC specification.
    #[inline]
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

    /// Constructs standard gRPC trailers in a [`HeaderWorkspace`].
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
}

const _: () = assert!(std::mem::size_of::<GrpcStatus>() == 1);
