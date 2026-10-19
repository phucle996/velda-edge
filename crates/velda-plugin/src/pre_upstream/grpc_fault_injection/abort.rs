//! gRPC Abort Injection Configuration and Canonical Trailers-Only Response Builder.
//!
//! # Architecture & Performance Invariants
//! - **Trailers-Only Specification**: Accurately adheres to the gRPC specification (RFC 9113 / gRPC over HTTP/2).
//!   Returns HTTP 200 OK with `content-type: application/grpc`, `grpc-status: <code>`, and `grpc-message: <message>`.
//!   Never returns HTTP 5xx, preventing client-side transport framing errors.
//! - **Pre-Allocated Response**: The default abort response is pre-built at instantiation time.
//!   Hot-path abort only performs a zero-copy atomic reference-count increment (~10ns).
//! - **Pre-computed Threshold**: Percentage uses a single-cycle 64-bit integer comparison.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use velda_core::L7Response;
use velda_grpc::GrpcStatus;

static GLOBAL_SEED: AtomicU64 = AtomicU64::new(0xe7037ed1a0b428db);

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
                x = 0xe7037ed1a0b428db;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545f4914f6cdd1d)
    })
}

/// Configuration for gRPC Abort Injection.
#[derive(Debug, Clone)]
pub struct GrpcAbortConfig {
    /// Configured percentage of matching requests that are aborted (0.0 to 100.0).
    pub percentage: u64,
    /// Pre-computed integer threshold in `0..=u64::MAX`.
    threshold: u64,
    /// Canonical gRPC status code (default: `14 Unavailable`).
    pub status: GrpcStatus,
    /// Error description message included in `grpc-message` trailer.
    pub message: Option<String>,
    /// Pre-compiled Trailers-Only HTTP response.
    pre_compiled_response: L7Response,
}

impl GrpcAbortConfig {
    /// Creates a new gRPC abort configuration.
    pub fn new(
        percentage: f64,
        status_code: u32,
        custom_message: Option<String>,
    ) -> Result<Self, String> {
        if !(0.0..=100.0).contains(&percentage) {
            return Err(format!(
                "Abort percentage must be between 0.0 and 100.0, got {percentage}"
            ));
        }

        if status_code > 16 {
            return Err(format!(
                "gRPC status code must be between 0 and 16, got {status_code}"
            ));
        }

        let status = GrpcStatus::from_code(status_code);
        let message = custom_message.or_else(|| Some("fault injected by edge".to_string()));

        let pre_compiled_response = status.to_l7_response(message.as_deref());

        let threshold = if percentage <= 0.0 {
            0
        } else if percentage >= 100.0 {
            u64::MAX
        } else {
            ((percentage / 100.0) * (u64::MAX as f64)) as u64
        };

        Ok(Self {
            percentage: threshold,
            threshold,
            status,
            message,
            pre_compiled_response,
        })
    }

    /// Evaluates whether this request should be aborted, returning the pre-compiled Trailers-Only [`L7Response`].
    #[inline]
    pub fn evaluate(&self, override_code: Option<u32>) -> Option<L7Response> {
        if let Some(code) = override_code {
            let status = GrpcStatus::from_code(code);
            return Some(status.to_l7_response(self.message.as_deref()));
        }

        if self.threshold == 0 {
            return None;
        }

        if self.threshold < u64::MAX && fast_random_u64() >= self.threshold {
            return None;
        }

        Some(self.pre_compiled_response.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_grpc_abort_config_validation() {
        assert!(GrpcAbortConfig::new(101.0, 14, None).is_err());
        assert!(GrpcAbortConfig::new(-1.0, 14, None).is_err());
        assert!(GrpcAbortConfig::new(50.0, 17, None).is_err()); // > 16 invalid
        assert!(GrpcAbortConfig::new(10.0, 14, None).is_ok());
    }

    #[test]
    fn test_grpc_abort_trailers_only_response() {
        let config = GrpcAbortConfig::new(100.0, 14, None).unwrap();
        let resp = config.evaluate(None).unwrap();

        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(
            resp.headers.get("content-type").unwrap(),
            "application/grpc"
        );
        assert_eq!(resp.headers.get("grpc-status").unwrap(), "14");
        assert_eq!(
            resp.headers.get("grpc-message").unwrap(),
            "fault injected by edge"
        );
    }

    #[test]
    fn test_grpc_abort_override_takes_precedence() {
        let config = GrpcAbortConfig::new(0.0, 14, None).unwrap(); // 0% configured
        let resp = config.evaluate(Some(8)).unwrap(); // Override with RESOURCE_EXHAUSTED (8)
        assert_eq!(resp.headers.get("grpc-status").unwrap(), "8");
    }
}
