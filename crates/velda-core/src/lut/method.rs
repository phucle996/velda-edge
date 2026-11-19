//! Canonical HTTP method variants and branchless lookup tables (`velda-core::lut::method`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                        HTTP_METHOD_NAMES (160 B L1D RESIDENT)                           │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ HttpMethod            │ 0..=9         │ 10 variants    │ 160 bytes     │ Direct Index   │
//! └───────────────────────┴───────────────┴────────────────┴───────────────┴────────────────┘
//!
//!   Inbound Method Bytes ──► [ Match Length & 4B Word ]
//!                                    │
//!                                    ▼
//!                           HttpMethod (u8: 0..=9)
//!                                    │
//!                                    ▼
//!                          HTTP_METHOD_NAMES[u8] ──► &'static str / &[u8]
//! ```

use serde::{Deserialize, Serialize};

/// Canonical HTTP method discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum HttpMethod {
    /// Non-HTTP or unclassified custom HTTP method.
    Unknown = 0,
    /// HTTP `GET` method (RFC 9110 §9.3.1). Safe, idempotent.
    Get = 1,
    /// HTTP `POST` method (RFC 9110 §9.3.3). Non-idempotent mutation.
    Post = 2,
    /// HTTP `PUT` method (RFC 9110 §9.3.4). Idempotent mutation.
    Put = 3,
    /// HTTP `DELETE` method (RFC 9110 §9.3.5). Idempotent mutation.
    Delete = 4,
    /// HTTP `HEAD` method (RFC 9110 §9.3.2). Safe, idempotent.
    Head = 5,
    /// HTTP `OPTIONS` method (RFC 9110 §9.3.7). Safe, idempotent.
    Options = 6,
    /// HTTP `PATCH` method (RFC 5789). Non-idempotent mutation.
    Patch = 7,
    /// HTTP `CONNECT` method (RFC 9110 §9.3.6). Tunneling.
    Connect = 8,
    /// HTTP `TRACE` method (RFC 9110 §9.3.8). Diagnostic loopback.
    Trace = 9,
}

/// Static lookup table mapping each `HttpMethod` discriminant (0..9) directly to its
/// uppercase ASCII wire representation.
///
/// Resident in CPU L1 Data Cache ($10 \times 16\text{B} = 160\text{B}$).
pub static HTTP_METHOD_NAMES: [&str; 10] = [
    "NONE", "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS", "PATCH", "CONNECT", "TRACE",
];

impl HttpMethod {
    /// Total number of distinct HTTP method variants.
    pub const COUNT: usize = 10;

    /// Static table containing all HTTP method variants in contiguous discriminant order.
    pub const ALL: [Self; 10] = [
        Self::Unknown,
        Self::Get,
        Self::Post,
        Self::Put,
        Self::Delete,
        Self::Head,
        Self::Options,
        Self::Patch,
        Self::Connect,
        Self::Trace,
    ];

    /// Converts a raw `u8` discriminant into a typed [`HttpMethod`].
    ///
    /// Returns the matched variant if valid (0..=9), or [`HttpMethod::Unknown`] if out of bounds.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::Unknown
        }
    }

    /// Converts a raw `u8` discriminant into `Some(HttpMethod)` if valid (0..=9), or `None` if out of bounds.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Resolves the canonical uppercase ASCII method string via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        HTTP_METHOD_NAMES[self as usize]
    }

    /// Resolves the canonical uppercase ASCII method byte slice via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        HTTP_METHOD_NAMES[self as usize].as_bytes()
    }

    /// Fast length-and-leading-bytes method classifier converting an ASCII byte slice into [`HttpMethod`].
    ///
    /// Avoids byte-by-byte iteration and dynamic string allocation.
    #[inline(always)]
    pub const fn from_bytes(b: &[u8]) -> Self {
        match b.len() {
            3 => match (b[0], b[1], b[2]) {
                (b'G', b'E', b'T') | (b'g', b'e', b't') => Self::Get,
                (b'P', b'U', b'T') | (b'p', b'u', b't') => Self::Put,
                _ => Self::Unknown,
            },
            4 => match (b[0], b[1], b[2], b[3]) {
                (b'P', b'O', b'S', b'T') | (b'p', b'o', b's', b't') => Self::Post,
                (b'H', b'E', b'A', b'D') | (b'h', b'e', b'a', b'd') => Self::Head,
                _ => Self::Unknown,
            },
            5 => match (b[0], b[1], b[2], b[3], b[4]) {
                (b'P', b'A', b'T', b'C', b'H') | (b'p', b'a', b't', b'c', b'h') => Self::Patch,
                (b'T', b'R', b'A', b'C', b'E') | (b't', b'r', b'a', b'c', b'e') => Self::Trace,
                _ => Self::Unknown,
            },
            6 => match (b[0], b[1], b[2], b[3], b[4], b[5]) {
                (b'D', b'E', b'L', b'E', b'T', b'E') | (b'd', b'e', b'l', b'e', b't', b'e') => {
                    Self::Delete
                }
                _ => Self::Unknown,
            },
            7 => match (b[0], b[1], b[2], b[3]) {
                (b'O', b'P', b'T', b'I') | (b'o', b'p', b't', b'i') => Self::Options,
                (b'C', b'O', b'N', b'N') | (b'c', b'o', b'n', b'n') => Self::Connect,
                _ => Self::Unknown,
            },
            _ => Self::Unknown,
        }
    }

    /// Converts a standard `http::Method` reference into [`HttpMethod`].
    #[inline(always)]
    pub fn from_http_method(m: &http::Method) -> Self {
        Self::from_bytes(m.as_str().as_bytes())
    }

    /// Returns `true` if this method is considered safe under RFC 9110 §9.2.1
    /// (`GET`, `HEAD`, `OPTIONS`, `TRACE`).
    #[inline(always)]
    pub const fn is_safe(self) -> bool {
        matches!(self, Self::Get | Self::Head | Self::Options | Self::Trace)
    }

    /// Returns `true` if this method is idempotent under RFC 9110 §9.2.2
    /// (`GET`, `HEAD`, `PUT`, `DELETE`, `OPTIONS`, `TRACE`).
    #[inline(always)]
    pub const fn is_idempotent(self) -> bool {
        matches!(
            self,
            Self::Get | Self::Head | Self::Put | Self::Delete | Self::Options | Self::Trace
        )
    }

    /// Returns `true` if this method performs a state-changing mutation on resources
    /// (`POST`, `PUT`, `DELETE`, `PATCH`).
    ///
    /// Non-idempotent and mutation methods MUST NOT be retried or processed in 0-RTT early data.
    #[inline(always)]
    pub const fn is_mutation(self) -> bool {
        matches!(self, Self::Post | Self::Put | Self::Delete | Self::Patch)
    }
}

const _: () = assert!(HttpMethod::COUNT == HTTP_METHOD_NAMES.len());
const _: () = assert!(HttpMethod::COUNT == HttpMethod::ALL.len());
const _: () = assert!(std::mem::size_of::<HttpMethod>() == 1);

impl TryFrom<u8> for HttpMethod {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(v).ok_or(v)
    }
}

impl Default for HttpMethod {
    #[inline(always)]
    fn default() -> Self {
        Self::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_method_dense_roundtrip() {
        for i in 0..10u8 {
            let m = HttpMethod::from_u8(i);
            assert_eq!(m as u8, i);
            assert_eq!(HttpMethod::ALL[i as usize], m);
            assert_eq!(HTTP_METHOD_NAMES[i as usize], m.as_str());
            assert_eq!(HttpMethod::try_from_u8(i), Some(m));
        }
        assert_eq!(HttpMethod::from_u8(10), HttpMethod::Unknown);
        assert_eq!(HttpMethod::from_u8(255), HttpMethod::Unknown);
        assert!(HttpMethod::try_from_u8(10).is_none());
        assert!(HttpMethod::try_from_u8(255).is_none());
    }

    #[test]
    fn test_from_bytes() {
        assert_eq!(HttpMethod::from_bytes(b"GET"), HttpMethod::Get);
        assert_eq!(HttpMethod::from_bytes(b"get"), HttpMethod::Get);
        assert_eq!(HttpMethod::from_bytes(b"POST"), HttpMethod::Post);
        assert_eq!(HttpMethod::from_bytes(b"PUT"), HttpMethod::Put);
        assert_eq!(HttpMethod::from_bytes(b"DELETE"), HttpMethod::Delete);
        assert_eq!(HttpMethod::from_bytes(b"HEAD"), HttpMethod::Head);
        assert_eq!(HttpMethod::from_bytes(b"OPTIONS"), HttpMethod::Options);
        assert_eq!(HttpMethod::from_bytes(b"PATCH"), HttpMethod::Patch);
        assert_eq!(HttpMethod::from_bytes(b"CONNECT"), HttpMethod::Connect);
        assert_eq!(HttpMethod::from_bytes(b"TRACE"), HttpMethod::Trace);
        assert_eq!(HttpMethod::from_bytes(b"CUSTOM"), HttpMethod::Unknown);
    }

    #[test]
    fn test_semantics() {
        assert!(HttpMethod::Get.is_safe());
        assert!(HttpMethod::Head.is_safe());
        assert!(!HttpMethod::Post.is_safe());

        assert!(HttpMethod::Get.is_idempotent());
        assert!(HttpMethod::Delete.is_idempotent());
        assert!(!HttpMethod::Post.is_idempotent());

        assert!(HttpMethod::Post.is_mutation());
        assert!(HttpMethod::Put.is_mutation());
        assert!(HttpMethod::Delete.is_mutation());
        assert!(HttpMethod::Patch.is_mutation());
        assert!(!HttpMethod::Get.is_mutation());
    }
}
