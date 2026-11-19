//! Canonical stream completion reason variants and lookup tables (`velda-core::lut::completion`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                    COMPLETION_REASON_NAMES (80 B L1D RESIDENT)                          │
//! ├───────────────────────┬───────────────┬────────────────┬───────────────┬────────────────┤
//! │ LUT Subsystem         │ Value Range   │ Variants Count │ Memory (L1D)  │ O(1) Mechanics │
//! ├───────────────────────┼───────────────┼────────────────┼───────────────┼────────────────┤
//! │ CompletionReason      │ 0..=4         │ 5 variants     │ 80 bytes      │ Direct Index   │
//! └───────────────────────┴───────────────┴────────────────┴───────────────┴────────────────┘
//!
//!   Stream Termination Event ──► CompletionReason (u8: 0..=4)
//!   (Complete / ClientCancel /             │
//!    UpstreamError / Timeout /             ▼
//!    Rejected)                   COMPLETION_REASON_NAMES[u8] ──► &'static str / &[u8]
//! ```

use serde::{Deserialize, Serialize};

/// Canonical request/stream lifecycle completion reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompletionReason {
    /// 0: Normal, full request-response exchange completed.
    Complete = 0,
    /// 1: Client aborted, cancelled, or disconnected prematurely before response could finish.
    ClientCancel = 1,
    /// 2: Upstream backend returned an error or dropped connection.
    UpstreamError = 2,
    /// 3: Upstream response timeout or idle timeout elapsed.
    Timeout = 3,
    /// 4: Request was rejected by edge policy (rate limit, WAF, auth, early error).
    Rejected = 4,
}

/// Static lookup table mapping each `CompletionReason` discriminant (0..4) directly to its
/// lowercase snake_case ASCII name.
///
/// Resident in CPU L1 Data Cache ($5 \times 16\text{B} = 80\text{B}$).
pub static COMPLETION_REASON_NAMES: [&str; 5] = [
    "complete",
    "client_cancel",
    "upstream_error",
    "timeout",
    "rejected",
];

impl CompletionReason {
    /// Total number of distinct completion reason variants.
    pub const COUNT: usize = 5;

    /// Static table containing all completion reason variants in contiguous discriminant order.
    pub const ALL: [Self; 5] = [
        Self::Complete,
        Self::ClientCancel,
        Self::UpstreamError,
        Self::Timeout,
        Self::Rejected,
    ];

    /// Converts a raw `u8` discriminant into a typed [`CompletionReason`].
    ///
    /// Returns the matched variant if valid (0..=4), or [`CompletionReason::Complete`] if out of bounds.
    #[inline(always)]
    pub const fn from_u8(v: u8) -> Self {
        if (v as usize) < Self::COUNT {
            Self::ALL[v as usize]
        } else {
            Self::Complete
        }
    }

    /// Converts a raw `u8` discriminant into `Some(CompletionReason)` if valid (0..=4), or `None` if out of bounds.
    #[inline(always)]
    pub const fn try_from_u8(v: u8) -> Option<Self> {
        if (v as usize) < Self::COUNT {
            Some(Self::ALL[v as usize])
        } else {
            None
        }
    }

    /// Resolves the canonical snake_case string via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_str(self) -> &'static str {
        COMPLETION_REASON_NAMES[self as usize]
    }

    /// Resolves the canonical snake_case ASCII byte slice via O(1) direct array indexing.
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        COMPLETION_REASON_NAMES[self as usize].as_bytes()
    }

    /// Returns `true` if the stream completed normally without premature client or upstream abortion.
    #[inline(always)]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Complete)
    }

    /// Returns `true` if the stream terminated abnormally (client cancel, upstream error, timeout, or rejected).
    #[inline(always)]
    pub const fn is_aborted(self) -> bool {
        !self.is_success()
    }
}

const _: () = assert!(CompletionReason::COUNT == COMPLETION_REASON_NAMES.len());
const _: () = assert!(CompletionReason::COUNT == CompletionReason::ALL.len());
const _: () = assert!(std::mem::size_of::<CompletionReason>() == 1);

impl TryFrom<u8> for CompletionReason {
    type Error = u8;

    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        Self::try_from_u8(v).ok_or(v)
    }
}

impl Default for CompletionReason {
    #[inline(always)]
    fn default() -> Self {
        Self::Complete
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_reason_dense_roundtrip() {
        for i in 0..5u8 {
            let r = CompletionReason::from_u8(i);
            assert_eq!(r as u8, i);
            assert_eq!(CompletionReason::ALL[i as usize], r);
            assert_eq!(COMPLETION_REASON_NAMES[i as usize], r.as_str());
            assert_eq!(CompletionReason::try_from_u8(i), Some(r));
        }
        assert_eq!(CompletionReason::from_u8(5), CompletionReason::Complete);
        assert_eq!(CompletionReason::from_u8(255), CompletionReason::Complete);
        assert!(CompletionReason::try_from_u8(5).is_none());
        assert!(CompletionReason::try_from_u8(255).is_none());
    }

    #[test]
    fn test_predicates() {
        assert!(CompletionReason::Complete.is_success());
        assert!(!CompletionReason::ClientCancel.is_success());
        assert!(CompletionReason::UpstreamError.is_aborted());
        assert!(CompletionReason::Timeout.is_aborted());
        assert!(CompletionReason::Rejected.is_aborted());
    }
}
