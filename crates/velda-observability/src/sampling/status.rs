//! Unified `InternalStatus` observability sampling lookup table (`0..21` + `shed`).
//!
//! ```text
//!  InternalStatus (u8: 0..21) ──► thresholds[idx] ──► Weyl Sequence Gate ──► keep(true/false)
//!  Overload Shed              ──► shed            ──► Weyl Sequence Gate ──► keep_shed()
//! ```

use velda_core::InternalStatus;

use super::state::{THRESHOLD_ALWAYS, keep_threshold, rate_to_threshold};

/// Compact POD lookup table for O(1) protocol-agnostic observability sampling keyed by [`InternalStatus`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusSampling {
    pub thresholds: [u64; InternalStatus::COUNT],
    pub shed: u64,
}

impl Default for StatusSampling {
    #[inline]
    fn default() -> Self {
        Self::keep_all()
    }
}

impl StatusSampling {
    /// Returns a fail-open policy retaining 100% of all logs and spans.
    #[inline]
    pub const fn keep_all() -> Self {
        Self {
            thresholds: [THRESHOLD_ALWAYS; InternalStatus::COUNT],
            shed: THRESHOLD_ALWAYS,
        }
    }

    /// Builds a sampling table from `default_rate`, explicit `(InternalStatus, rate)` overrides,
    /// and an optional overload `shed` rate (defaults to the `RateLimited` threshold).
    pub fn from_default_and_overrides(
        default_rate: f64,
        overrides: &[(InternalStatus, f64)],
        r_shed: Option<f64>,
    ) -> Self {
        let mut thresholds = [rate_to_threshold(default_rate); InternalStatus::COUNT];
        for &(status, rate) in overrides {
            thresholds[status as usize] = rate_to_threshold(rate);
        }
        let shed = r_shed.map_or(
            thresholds[InternalStatus::RateLimited as usize],
            rate_to_threshold,
        );
        Self { thresholds, shed }
    }

    /// Evaluates whether logs & spans should be emitted for an [`InternalStatus`].
    #[inline(always)]
    pub fn keep(&self, status: InternalStatus) -> bool {
        keep_threshold(self.thresholds[status as usize])
    }

    /// Evaluates whether logs & spans should be emitted for an overload-shed request.
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
    fn status_sampling_overrides_and_shed() {
        seed_worker(5);
        let s = StatusSampling::from_default_and_overrides(
            1.0,
            &[
                (InternalStatus::Completed, 0.0),
                (InternalStatus::RouteNotFound, 0.0),
            ],
            Some(0.0),
        );

        assert!(!s.keep(InternalStatus::Completed));
        assert!(!s.keep(InternalStatus::RouteNotFound));
        assert!(s.keep(InternalStatus::InternalError));
        assert!(s.keep(InternalStatus::ServiceUnavailable));
        assert!(s.keep(InternalStatus::RateLimited));
        assert!(!s.keep_shed());
    }

    #[test]
    fn shed_inherits_rate_limited_when_omitted() {
        seed_worker(6);
        let drop_shed = StatusSampling::from_default_and_overrides(
            1.0,
            &[(InternalStatus::RateLimited, 0.0)],
            None,
        );
        assert!(!drop_shed.keep_shed());
    }
}
