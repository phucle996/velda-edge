//! Bit-packed `u32` Dual Upstream Target Register (`pre_upstream::targets`).
//!
//! ```text
//!   32-bit Routing Register (`targets: u32`)
//!   ┌──────────────────────────────────┬──────────────────────────────────┐
//!   │ Bits 31..16 (Mirror / Shadow)    │ Bits 15..0  (Primary)            │
//!   │ 0xFFFF = No mirror               │ 0xFFFF = Cancelled / Do not send │
//!   │ 0..=65534 = Dense mirror ID      │ 0..=65534 = Dense primary ID     │
//!   └──────────────────────────────────┴──────────────────────────────────┘
//!
//!   Initialization from Router : target_init(primary_id) -> 0xFFFF_0000 | primary_id
//!   Canary plugin mutation     : target_set_primary(&mut t, canary_id)  [preserves mirror bits]
//!   Request Mirror mutation    : target_set_mirror(&mut t, mirror_id)   [preserves primary bits]
//!   Short-circuit Reject/Resp  : target_cancel_all(&mut t) -> 0xFFFF_FFFF
//! ```

use velda_core::UNASSIGNED_UPSTREAM_ID;

/// 16-bit sentinel indicating no upstream is assigned for a lane (`0xFFFF`).
pub const TARGET_UNASSIGNED: u16 = UNASSIGNED_UPSTREAM_ID;

/// 32-bit sentinel indicating both primary and mirror lanes are cancelled (`0xFFFF_FFFF`).
pub const TARGETS_CANCELLED: u32 = u32::MAX;

/// Encodes an initial `u32` target word with `primary_id` and no mirror (`0xFFFF`).
#[inline(always)]
pub const fn target_init(primary_id: u16) -> u32 {
    ((TARGET_UNASSIGNED as u32) << 16) | (primary_id as u32)
}

/// Encodes an explicit `(primary_id, mirror_id)` pair into a `u32` target word.
#[inline(always)]
pub const fn target_pair(primary_id: u16, mirror_id: u16) -> u32 {
    ((mirror_id as u32) << 16) | (primary_id as u32)
}

/// Extracts the dense primary upstream ID (`None` if `0xFFFF`).
#[inline(always)]
pub const fn target_primary(targets: u32) -> Option<u16> {
    let id = (targets & 0xFFFF) as u16;
    if id == TARGET_UNASSIGNED {
        None
    } else {
        Some(id)
    }
}

/// Extracts the dense mirror upstream ID (`None` if `0xFFFF`).
#[inline(always)]
pub const fn target_mirror(targets: u32) -> Option<u16> {
    let id = (targets >> 16) as u16;
    if id == TARGET_UNASSIGNED {
        None
    } else {
        Some(id)
    }
}

/// Overwrites only the lower 16 primary bits, preserving the upper 16 mirror bits.
#[inline(always)]
pub fn target_set_primary(targets: &mut u32, primary_id: u16) {
    *targets = (*targets & 0xFFFF_0000) | (primary_id as u32);
}

/// Overwrites only the upper 16 mirror bits, preserving the lower 16 primary bits.
#[inline(always)]
pub fn target_set_mirror(targets: &mut u32, mirror_id: u16) {
    *targets = (*targets & 0x0000_FFFF) | ((mirror_id as u32) << 16);
}

/// Clears both primary and mirror lanes to `0xFFFF_FFFF`.
#[inline(always)]
pub fn target_cancel_all(targets: &mut u32) {
    *targets = TARGETS_CANCELLED;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packed_u32_targets_preserve_independent_halves() {
        // Upstream ID 0 must never collide with sentinel 0xFFFF.
        let mut t = target_init(0);
        assert_eq!(target_primary(t), Some(0));
        assert_eq!(target_mirror(t), None);

        target_set_mirror(&mut t, 7);
        assert_eq!(target_primary(t), Some(0));
        assert_eq!(target_mirror(t), Some(7));

        target_set_primary(&mut t, 2);
        assert_eq!(target_primary(t), Some(2));
        assert_eq!(target_mirror(t), Some(7));

        target_cancel_all(&mut t);
        assert_eq!(t, TARGETS_CANCELLED);
        assert_eq!(target_primary(t), None);
        assert_eq!(target_mirror(t), None);
    }
}
