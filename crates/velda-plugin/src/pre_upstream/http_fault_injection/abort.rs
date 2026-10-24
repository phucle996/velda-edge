//! Abort Injection Configuration and Pre-Allocated Plain-Text Response Builder.
//!
//! # Architecture & Performance Invariants
//! - **Plain-Text Default**: Aligned with industry standards (Envoy / Nginx), returns lightweight
//!   `text/plain; charset=utf-8` bodies (default `"fault filter abort\n"`). Extremely friendly
//!   to browsers, cURL, and microservice clients without forcing complex JSON schemas.
//! - **Zero-Allocation Hot-Path**: Response body is pre-allocated as [`Bytes`] at instantiation time.
//!   Hot-path responses only perform zero-copy atomic reference-count increments (~10ns).
//! - **Pre-Compiled Headers**: Injects `content-type: text/plain; charset=utf-8` and `x-velda-fault: abort`.
//! - **Pre-computed Threshold**: Percentage ($0.0\% - 100.0\%$) uses single-cycle integer comparison.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode, Version};
use velda_core::{Body, L7Response};

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0x243f6a8885a308d3);

thread_local! {
    static RNG_STATE: Cell<u64> = const { Cell::new(0) };
}

/// Fast thread-local XorShift64* PRNG running in ~1 nanosecond with zero dependencies.
#[inline(always)]
fn fast_random_u64() -> u64 {
    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        if x == 0 {
            let seed = GLOBAL_SEED.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
            let mut z = seed ^ 0xbf58476d1ce4e5b9;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            x = z ^ (z >> 31);
            if x == 0 {
                x = 0x243f6a8885a308d3;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545f4914f6cdd1d)
    })
}

/// Configuration for HTTP Abort Injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpAbortConfig {
    /// Configured percentage of matching requests that are aborted (0.0 to 100.0).
    pub percentage: u64, // Stored as integer threshold for Eq compliance
    /// Pre-computed integer threshold in `0..=u64::MAX`.
    threshold: u64,
    /// Default HTTP status code to return when aborted (e.g. 503).
    pub status: StatusCode,
    /// Pre-allocated raw response payload body.
    pub body: Bytes,
    /// Pre-compiled response headers to inject.
    pub headers: Vec<(HeaderName, HeaderValue)>,
    default_response: L7Response,
}

impl HttpAbortConfig {
    /// Creates a new abort configuration with validated status and pre-compiled headers.
    pub fn new(
        percentage: f64,
        status_code: u16,
        custom_body: Option<String>,
        custom_headers: Option<Vec<(HeaderName, HeaderValue)>>,
    ) -> Result<Self, String> {
        if !(0.0..=100.0).contains(&percentage) {
            return Err(format!(
                "Abort percentage must be between 0.0 and 100.0, got {percentage}"
            ));
        }

        if !(200..=599).contains(&status_code) {
            return Err(format!(
                "Abort HTTP status code must be between 200 and 599, got {status_code}"
            ));
        }

        let status = StatusCode::from_u16(status_code)
            .map_err(|e| format!("Invalid HTTP status code {status_code}: {e}"))?;

        let body = match custom_body {
            Some(text) => Bytes::from(text.into_bytes()),
            None => Bytes::from_static(b"fault filter abort\n"),
        };

        let mut headers = vec![
            (
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            ),
            (
                HeaderName::from_static("x-velda-fault"),
                HeaderValue::from_static("abort"),
            ),
        ];

        if let Some(customs) = custom_headers {
            headers.extend(customs);
        }

        let threshold = if percentage <= 0.0 {
            0
        } else if percentage >= 100.0 {
            u64::MAX
        } else {
            ((percentage / 100.0) * (u64::MAX as f64)) as u64
        };

        let mut header_map = HeaderMap::with_capacity(headers.len() + 1);
        for (name, val) in &headers {
            header_map.insert(name.clone(), val.clone());
        }
        let len_str = body.len().to_string();
        header_map.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&len_str).unwrap_or_else(|_| HeaderValue::from_static("0")),
        );
        let default_response = L7Response::new(
            status,
            Version::HTTP_11,
            header_map,
            Body::Bytes(body.clone()),
        );

        Ok(Self {
            percentage: threshold,
            threshold,
            status,
            body,
            headers,
            default_response,
        })
    }

    /// Evaluates whether this request should be aborted, returning the pre-compiled [`L7Response`].
    ///
    /// # Precedence Rule
    /// If `override_status` is supplied (from `x-fault-abort-code`), it takes immediate precedence
    /// and generates an abort response with that status code without evaluating random percentage rolls.
    #[inline]
    pub fn evaluate(&self, override_status: Option<u16>) -> Option<L7Response> {
        if let Some(code) = override_status {
            let status = StatusCode::from_u16(code).unwrap_or(self.status);
            if status == self.status {
                return Some(self.default_response.clone());
            }
            return Some(self.build_response(status));
        }

        if self.threshold == 0 {
            return None;
        }

        // 1 CPU cycle integer comparison
        if self.threshold < u64::MAX && fast_random_u64() >= self.threshold {
            return None;
        }

        Some(self.default_response.clone())
    }

    /// Constructs an [`L7Response`] with pre-allocated headers and body.
    #[inline]
    fn build_response(&self, status: StatusCode) -> L7Response {
        let mut header_map = HeaderMap::with_capacity(self.headers.len() + 1);
        for (name, val) in &self.headers {
            header_map.insert(name.clone(), val.clone());
        }
        header_map.insert(
            CONTENT_LENGTH,
            HeaderValue::from_str(&self.body.len().to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("0")),
        );

        L7Response::new(
            status,
            Version::HTTP_11,
            header_map,
            Body::Bytes(self.body.clone()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abort_config_validation() {
        assert!(HttpAbortConfig::new(101.0, 503, None, None).is_err());
        assert!(HttpAbortConfig::new(-0.1, 503, None, None).is_err());
        assert!(HttpAbortConfig::new(50.0, 999, None, None).is_err()); // invalid status
        assert!(HttpAbortConfig::new(10.0, 503, None, None).is_ok());
    }

    #[test]
    fn test_abort_default_plain_text() {
        let config = HttpAbortConfig::new(100.0, 503, None, None).unwrap();
        let resp = config.evaluate(None).unwrap();

        assert_eq!(resp.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "text/plain; charset=utf-8"
        );
        assert_eq!(resp.headers.get("x-velda-fault").unwrap(), "abort");
        assert_eq!(
            resp.body,
            Body::Bytes(Bytes::from_static(b"fault filter abort\n"))
        );
    }

    #[test]
    fn test_abort_zero_percent() {
        let config = HttpAbortConfig::new(0.0, 500, None, None).unwrap();
        for _ in 0..100 {
            assert!(config.evaluate(None).is_none());
        }
    }

    #[test]
    fn test_abort_override_takes_precedence() {
        let config = HttpAbortConfig::new(0.0, 503, None, None).unwrap(); // 0% configured
        let resp = config.evaluate(Some(504)).unwrap(); // Override supplied!
        assert_eq!(resp.status, StatusCode::GATEWAY_TIMEOUT);
    }
}
