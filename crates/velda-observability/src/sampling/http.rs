//! Pre-compiled HTTP status-class sampling lookup table (`1xx`..`5xx` + overload `shed`).

use super::state::{THRESHOLD_ALWAYS, keep_threshold, rate_to_threshold};

/// Compact 88-byte POD lookup table for O(1) zero-branch HTTP observability sampling.
///
/// `class[idx]` holds the fixed-point Weyl threshold for `status_code / 100` (`1..=5`), while
/// slots `0` and `6..=9` default fail-open to [`THRESHOLD_ALWAYS`] so unexpected codes are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpSampling {
    pub class: [u64; 10],
    pub shed: u64,
}

impl Default for HttpSampling {
    #[inline]
    fn default() -> Self {
        Self::keep_all()
    }
}

impl HttpSampling {
    /// Returns a fail-open policy retaining 100% of all logs and spans.
    #[inline]
    pub const fn keep_all() -> Self {
        Self {
            class: [THRESHOLD_ALWAYS; 10],
            shed: THRESHOLD_ALWAYS,
        }
    }

    /// Compiles fractional sampling rates (`0.0..=1.0`) for each HTTP status class and optional `shed`.
    /// When `shed` is `None`, overload rejections inherit the `4xx` threshold.
    pub fn from_rates(
        r1xx: f64,
        r2xx: f64,
        r3xx: f64,
        r4xx: f64,
        r5xx: f64,
        r_shed: Option<f64>,
    ) -> Self {
        let mut class = [THRESHOLD_ALWAYS; 10];
        class[1] = rate_to_threshold(r1xx);
        class[2] = rate_to_threshold(r2xx);
        class[3] = rate_to_threshold(r3xx);
        class[4] = rate_to_threshold(r4xx);
        class[5] = rate_to_threshold(r5xx);
        let shed = r_shed.map_or(class[4], rate_to_threshold);
        Self { class, shed }
    }

    /// Evaluates whether logs & spans should be emitted for an HTTP response status code.
    #[inline(always)]
    pub fn keep(&self, status_code: u16) -> bool {
        let idx = (status_code / 100).min(9) as usize;
        keep_threshold(self.class[idx])
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
    fn http_classes_and_out_of_range_fail_open() {
        seed_worker(1);
        let s = HttpSampling::from_rates(0.0, 0.0, 0.0, 1.0, 1.0, Some(0.0));

        // 1xx..3xx dropped, 4xx..5xx always kept, shed dropped, out-of-range kept.
        for code in [100u16, 101, 200, 204, 301, 308] {
            assert!(!s.keep(code), "expected drop for status {code}");
        }
        for code in [400u16, 404, 429, 500, 503, 599] {
            assert!(s.keep(code), "expected keep for status {code}");
        }
        for code in [0u16, 99, 600, 999, u16::MAX] {
            assert!(s.keep(code), "expected fail-open for status {code}");
        }
        assert!(!s.keep_shed());
    }

    #[test]
    fn shed_inherits_4xx_when_omitted() {
        seed_worker(2);
        let keep_shed = HttpSampling::from_rates(1.0, 1.0, 1.0, 1.0, 1.0, None);
        assert!(keep_shed.keep_shed());

        let drop_shed = HttpSampling::from_rates(1.0, 1.0, 1.0, 0.0, 1.0, None);
        assert!(!drop_shed.keep_shed());
    }
}
