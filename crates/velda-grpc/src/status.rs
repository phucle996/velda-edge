//! Canonical gRPC status codes and trailer metadata helpers.

use http::StatusCode;
use http::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use velda_core::L7Response;

/// Standard gRPC canonical status codes according to gRPC Core specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum GrpcStatus {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
    Unauthenticated = 16,
}

impl GrpcStatus {
    /// Returns the integer code value.
    #[inline]
    pub const fn code(self) -> u32 {
        self as u32
    }

    /// Returns the static string representation of the integer code.
    #[inline]
    pub const fn code_str(self) -> &'static str {
        match self {
            Self::Ok => "0",
            Self::Cancelled => "1",
            Self::Unknown => "2",
            Self::InvalidArgument => "3",
            Self::DeadlineExceeded => "4",
            Self::NotFound => "5",
            Self::AlreadyExists => "6",
            Self::PermissionDenied => "7",
            Self::ResourceExhausted => "8",
            Self::FailedPrecondition => "9",
            Self::Aborted => "10",
            Self::OutOfRange => "11",
            Self::Unimplemented => "12",
            Self::Internal => "13",
            Self::Unavailable => "14",
            Self::DataLoss => "15",
            Self::Unauthenticated => "16",
        }
    }

    /// Converts an integer code to a typed `GrpcStatus`.
    pub fn from_code(code: u32) -> Self {
        match code {
            0 => Self::Ok,
            1 => Self::Cancelled,
            2 => Self::Unknown,
            3 => Self::InvalidArgument,
            4 => Self::DeadlineExceeded,
            5 => Self::NotFound,
            6 => Self::AlreadyExists,
            7 => Self::PermissionDenied,
            8 => Self::ResourceExhausted,
            9 => Self::FailedPrecondition,
            10 => Self::Aborted,
            11 => Self::OutOfRange,
            12 => Self::Unimplemented,
            13 => Self::Internal,
            14 => Self::Unavailable,
            15 => Self::DataLoss,
            16 => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }

    /// Constructs standard gRPC trailers containing `grpc-status` and optional `grpc-message`.
    pub fn to_trailers(self, message: Option<&str>) -> HeaderMap {
        let mut map = HeaderMap::with_capacity(2);
        map.insert(
            HeaderName::from_static("grpc-status"),
            HeaderValue::from_static(self.code_str()),
        );
        if let Some(val) = message.and_then(|m| HeaderValue::from_str(m).ok()) {
            map.insert(HeaderName::from_static("grpc-message"), val);
        }
        map
    }

    /// Constructs a standard gRPC Trailers-Only `L7Response`.
    pub fn to_l7_response(self, message: Option<&str>) -> L7Response {
        let mut resp = L7Response::empty(StatusCode::OK)
            .with_header(CONTENT_TYPE, HeaderValue::from_static("application/grpc"))
            .with_header(
                HeaderName::from_static("grpc-status"),
                HeaderValue::from_static(self.code_str()),
            );

        if let Some(val) = message.and_then(|m| HeaderValue::from_str(m).ok()) {
            resp = resp.with_header(HeaderName::from_static("grpc-message"), val);
        }
        resp
    }
}
