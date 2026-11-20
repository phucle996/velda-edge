//! Pre-compiled canonical gRPC status sampling lookup table (`0..=16`, `NonGrpc`, + `shed`).

use velda_core::lut::GrpcStatus;

use super::state::{THRESHOLD_ALWAYS, keep_threshold, rate_to_threshold};

/// Fixed-size LUT covering canonical `GrpcStatus` discriminants `0..=16` plus slot `17` (`NonGrpc`).
pub const GRPC_LUT_LEN: usize = 18;

/// Pre-compiled gRPC observability sampling policy indexed directly by `GrpcStatus as u8`.
///
/// Slot `17` (`GrpcStatus::NonGrpc`) is permanently [`THRESHOLD_ALWAYS`] so unexpected non-RPC
/// errors on a gRPC listener are never silently sampled away.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcSampling {
    pub code: [u64; GRPC_LUT_LEN],
    pub shed: u64,
}

impl Default for GrpcSampling {
    #[inline]
    fn default() -> Self {
        Self::keep_all()
    }
}

impl GrpcSampling {
    /// Returns a fail-open policy retaining 100% of all logs and spans.
    #[inline]
    pub const fn keep_all() -> Self {
        Self {
            code: [THRESHOLD_ALWAYS; GRPC_LUT_LEN],
            shed: THRESHOLD_ALWAYS,
        }
    }

    /// Builds a policy starting from `default_rate` for all 17 canonical gRPC codes (`0..=16`),
    /// then applies explicit per-code overrides `(GrpcStatus, rate)` and optional `shed`.
    /// `GrpcStatus::NonGrpc` ignores overrides and stays fail-open.
    pub fn from_default_and_overrides(
        default_rate: f64,
        overrides: &[(GrpcStatus, f64)],
        r_shed: Option<f64>,
    ) -> Self {
        let def_thr = rate_to_threshold(default_rate);
        let mut code = [def_thr; GRPC_LUT_LEN];
        code[GrpcStatus::NonGrpc as usize] = THRESHOLD_ALWAYS;

        for &(status, rate) in overrides {
            let idx = status as usize;
            if idx < GrpcStatus::NonGrpc as usize {
                code[idx] = rate_to_threshold(rate);
            }
        }

        let shed = r_shed.map_or(
            code[GrpcStatus::ResourceExhausted as usize],
            rate_to_threshold,
        );
        Self { code, shed }
    }

    /// Evaluates whether logs & spans should be emitted for a canonical gRPC status code.
    #[inline(always)]
    pub fn keep(&self, status: GrpcStatus) -> bool {
        let idx = (status as usize).min(GRPC_LUT_LEN - 1);
        keep_threshold(self.code[idx])
    }

    /// Evaluates whether logs & spans should be emitted for an overload-shed gRPC stream.
    #[inline(always)]
    pub fn keep_shed(&self) -> bool {
        keep_threshold(self.shed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sampling::state::seed_worker;

    #[test]
    fn grpc_canonical_codes_overrides_and_nongrpc() {
        seed_worker(5);
        let s = GrpcSampling::from_default_and_overrides(
            1.0,
            &[(GrpcStatus::Ok, 0.0), (GrpcStatus::NotFound, 0.0)],
            Some(0.0),
        );

        assert!(!s.keep(GrpcStatus::Ok));
        assert!(!s.keep(GrpcStatus::NotFound));
        assert!(s.keep(GrpcStatus::Internal));
        assert!(s.keep(GrpcStatus::Unavailable));
        assert!(s.keep(GrpcStatus::ResourceExhausted));
        assert!(s.keep(GrpcStatus::NonGrpc));
        assert!(!s.keep_shed());
    }

    #[test]
    fn grpc_shed_inherits_resource_exhausted_when_omitted() {
        seed_worker(6);
        let drop_shed = GrpcSampling::from_default_and_overrides(
            1.0,
            &[(GrpcStatus::ResourceExhausted, 0.0)],
            None,
        );
        assert!(!drop_shed.keep_shed());
    }
}
