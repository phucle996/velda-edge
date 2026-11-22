//! Compile-Time Static Early Rejection Codebook & Metadata (`velda-core::lut::reject`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                       REJECT_STATIC_METAS (544 B L1D RESIDENT)                          │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ RejectReason          │ 0..=16        │ 17 variants    │ 544 bytes     │ Direct Index   │
//! └───────────────────────┴───────────────┴────────────────┴───────────────┴────────────────┘
//!
//!   Early Rejection Trigger ──► RejectReason (u8: 0..=16)
//!   (RateLimit / Auth / WAF)             │
//!                                        ▼
//!                              REJECT_STATIC_METAS[u8]
//!                                        │
//!              ┌─────────────────────────┼─────────────────────────┐
//!              ▼                         ▼                         ▼
//!     status: StatusCode       content_len_str: &str       body: &'static [u8]
//!     (400, 401, 403, 429)     ("17", "22", ...)           ("429 Too Many Requests\n")
//! ```

use http::StatusCode;

use crate::error::RejectReason;

/// Compile-time static response metadata for an early rejection reason.
#[derive(Debug, Clone, Copy)]
pub struct RejectStaticMeta {
    /// Canonical HTTP status code.
    pub status: StatusCode,
    /// Pre-formatted ASCII string literal of content length (e.g. "22").
    pub content_len_str: &'static str,
    /// Canonical RFC error message body with trailing newline.
    pub body: &'static [u8],
}

/// Constant 499 Client Closed Request status code.
const STATUS_499: StatusCode = match StatusCode::from_u16(499) {
    Ok(s) => s,
    Err(_) => panic!("Valid status code 499"),
};

/// Dense O(1) lookup table indexed directly by `RejectReason as usize` (0..18).
/// Total footprint: 19 * 32 bytes = 608 bytes (100% L1D cache-resident).
pub static REJECT_STATIC_METAS: [RejectStaticMeta; 19] = [
    // Slot 0: BadRequest
    RejectStaticMeta {
        status: StatusCode::BAD_REQUEST,
        content_len_str: "16",
        body: b"400 Bad Request\n",
    },
    // Slot 1: Unauthorized
    RejectStaticMeta {
        status: StatusCode::UNAUTHORIZED,
        content_len_str: "17",
        body: b"401 Unauthorized\n",
    },
    // Slot 2: Forbidden
    RejectStaticMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
    // Slot 3: NotFound
    RejectStaticMeta {
        status: StatusCode::NOT_FOUND,
        content_len_str: "14",
        body: b"404 Not Found\n",
    },
    // Slot 4: MethodNotAllowed
    RejectStaticMeta {
        status: StatusCode::METHOD_NOT_ALLOWED,
        content_len_str: "23",
        body: b"405 Method Not Allowed\n",
    },
    // Slot 5: RequestTimeout
    RejectStaticMeta {
        status: StatusCode::REQUEST_TIMEOUT,
        content_len_str: "20",
        body: b"408 Request Timeout\n",
    },
    // Slot 6: Conflict
    RejectStaticMeta {
        status: StatusCode::CONFLICT,
        content_len_str: "13",
        body: b"409 Conflict\n",
    },
    // Slot 7: PayloadTooLarge
    RejectStaticMeta {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        content_len_str: "22",
        body: b"413 Payload Too Large\n",
    },
    // Slot 8: UriTooLong
    RejectStaticMeta {
        status: StatusCode::URI_TOO_LONG,
        content_len_str: "17",
        body: b"414 URI Too Long\n",
    },
    // Slot 9: UnsupportedMediaType
    RejectStaticMeta {
        status: StatusCode::UNSUPPORTED_MEDIA_TYPE,
        content_len_str: "27",
        body: b"415 Unsupported Media Type\n",
    },
    // Slot 10: TooManyRequests
    RejectStaticMeta {
        status: StatusCode::TOO_MANY_REQUESTS,
        content_len_str: "22",
        body: b"429 Too Many Requests\n",
    },
    // Slot 11: HeaderFieldsTooLarge
    RejectStaticMeta {
        status: StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        content_len_str: "36",
        body: b"431 Request Header Fields Too Large\n",
    },
    // Slot 12: ClientClosedRequest (499)
    RejectStaticMeta {
        status: STATUS_499,
        content_len_str: "26",
        body: b"499 Client Closed Request\n",
    },
    // Slot 13: InternalError
    RejectStaticMeta {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        content_len_str: "26",
        body: b"500 Internal Server Error\n",
    },
    // Slot 14: BadGateway
    RejectStaticMeta {
        status: StatusCode::BAD_GATEWAY,
        content_len_str: "16",
        body: b"502 Bad Gateway\n",
    },
    // Slot 15: ServiceUnavailable
    RejectStaticMeta {
        status: StatusCode::SERVICE_UNAVAILABLE,
        content_len_str: "24",
        body: b"503 Service Unavailable\n",
    },
    // Slot 16: GatewayTimeout
    RejectStaticMeta {
        status: StatusCode::GATEWAY_TIMEOUT,
        content_len_str: "20",
        body: b"504 Gateway Timeout\n",
    },
    // Slot 17: WafBlocked (Indistinguishable from Forbidden on the wire)
    RejectStaticMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
    // Slot 18: IpBlocked (Indistinguishable from Forbidden on the wire)
    RejectStaticMeta {
        status: StatusCode::FORBIDDEN,
        content_len_str: "14",
        body: b"403 Forbidden\n",
    },
];

/// Returns the pre-compiled static response metadata for the given rejection reason in $\mathcal{O}(1)$.
#[inline(always)]
pub const fn get_reject_meta(reason: RejectReason) -> &'static RejectStaticMeta {
    &REJECT_STATIC_METAS[reason as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reject_static_metas_dense_and_exact() {
        assert_eq!(REJECT_STATIC_METAS.len(), RejectReason::COUNT);
        for i in 0..RejectReason::COUNT {
            let reason = RejectReason::from_u8(i as u8).expect("Dense discriminant");
            let meta = get_reject_meta(reason);
            assert_eq!(meta.body.len().to_string(), meta.content_len_str);
            assert!(meta.body.ends_with(b"\n"));
        }
    }
}
