//! Pre-compiled Request Mirror configuration & O(1) method bitmask (`request_mirror::config`).
//!
//! ```text
//!   ignore_methods: Vec<String>
//!     └─► MethodMask::from_methods() ──► u16 register bitmask (+ rare non-standard fallback)
//!   sample_percent: f64 (0.0..=100.0)
//!     └─► compute_threshold_u64() ─────► u64 threshold for SplitMix64 `request_id::entropy < th`
//! ```

use http::Method;
use serde::{Deserialize, Serialize};

/// Declarative configuration for [`super::RequestMirrorPlugin`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestMirrorConfig {
    /// Logical upstream pool ID to receive shadowed requests.
    pub mirror_upstream: String,
    /// Percentage of matching traffic sampled for mirroring (`0.0..=100.0`).
    pub sample_percent: f64,
    /// HTTP methods excluded from mirroring (e.g. `["POST", "DELETE"]`).
    pub ignore_methods: Vec<String>,
}

impl Default for RequestMirrorConfig {
    fn default() -> Self {
        Self {
            mirror_upstream: String::new(),
            sample_percent: 100.0,
            ignore_methods: Vec::new(),
        }
    }
}

const BIT_GET: u16 = 1 << 0;
const BIT_POST: u16 = 1 << 1;
const BIT_PUT: u16 = 1 << 2;
const BIT_DELETE: u16 = 1 << 3;
const BIT_PATCH: u16 = 1 << 4;
const BIT_HEAD: u16 = 1 << 5;
const BIT_OPTIONS: u16 = 1 << 6;
const BIT_CONNECT: u16 = 1 << 7;
const BIT_TRACE: u16 = 1 << 8;

/// Pre-compiled HTTP method filter using a single `u16` register bitmask for standard methods.
#[derive(Debug, Clone, Default)]
pub(crate) struct MethodMask {
    bits: u16,
    custom: Box<[Box<str>]>,
}

impl MethodMask {
    #[inline]
    fn standard_bit(name: &[u8]) -> u16 {
        if name.eq_ignore_ascii_case(b"GET") {
            BIT_GET
        } else if name.eq_ignore_ascii_case(b"POST") {
            BIT_POST
        } else if name.eq_ignore_ascii_case(b"PUT") {
            BIT_PUT
        } else if name.eq_ignore_ascii_case(b"DELETE") {
            BIT_DELETE
        } else if name.eq_ignore_ascii_case(b"PATCH") {
            BIT_PATCH
        } else if name.eq_ignore_ascii_case(b"HEAD") {
            BIT_HEAD
        } else if name.eq_ignore_ascii_case(b"OPTIONS") {
            BIT_OPTIONS
        } else if name.eq_ignore_ascii_case(b"CONNECT") {
            BIT_CONNECT
        } else if name.eq_ignore_ascii_case(b"TRACE") {
            BIT_TRACE
        } else {
            0
        }
    }

    pub(crate) fn from_methods(methods: &[String]) -> Self {
        let mut bits = 0u16;
        let mut custom = Vec::new();
        for m in methods {
            let trimmed = m.trim();
            let bit = Self::standard_bit(trimmed.as_bytes());
            if bit != 0 {
                bits |= bit;
            } else if !trimmed.is_empty() {
                custom.push(trimmed.to_ascii_uppercase().into_boxed_str());
            }
        }
        Self {
            bits,
            custom: custom.into_boxed_slice(),
        }
    }

    #[inline(always)]
    pub(crate) fn matches(&self, method: &Method) -> bool {
        if self.bits == 0 && self.custom.is_empty() {
            return false;
        }
        let bit = if *method == Method::GET {
            BIT_GET
        } else if *method == Method::POST {
            BIT_POST
        } else if *method == Method::PUT {
            BIT_PUT
        } else if *method == Method::DELETE {
            BIT_DELETE
        } else if *method == Method::PATCH {
            BIT_PATCH
        } else if *method == Method::HEAD {
            BIT_HEAD
        } else if *method == Method::OPTIONS {
            BIT_OPTIONS
        } else if *method == Method::CONNECT {
            BIT_CONNECT
        } else if *method == Method::TRACE {
            BIT_TRACE
        } else {
            0
        };
        if bit != 0 {
            return (self.bits & bit) != 0;
        }
        let m_bytes = method.as_str().as_bytes();
        self.custom
            .iter()
            .any(|c| c.as_bytes().eq_ignore_ascii_case(m_bytes))
    }
}

/// Maps `sample_percent` (`0.0..=100.0`) onto the 64-bit unsigned integer space.
#[inline]
pub(crate) fn compute_threshold_u64(sample_percent: f64) -> u64 {
    if sample_percent <= 0.0 || sample_percent.is_nan() {
        0
    } else if sample_percent >= 100.0 {
        u64::MAX
    } else {
        let scaled = (sample_percent / 100.0) * (u64::MAX as f64);
        if scaled >= (u64::MAX as f64) {
            u64::MAX - 1
        } else {
            scaled.round() as u64
        }
    }
}
