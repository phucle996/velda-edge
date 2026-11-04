//! Anonymous Flat Header Slot Table for O(1) integer-indexed header storage.
//!
//! # Architecture & Invariants (SRP: Storage Only)
//! 1. **Protocol-Blind**: Contains zero protocol-specific constants or knowledge.
//!    Index semantics (which slot index represents which header) are defined exclusively
//!    by protocol crates (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`).
//! 2. **Single-Cycle Presence Test**: Bitmask `present_mask: u16` enables testing header
//!    existence in ~0.3ns using a single CPU instruction (`test` / `bt`).
//! 3. **Deterministic Array Indexing**: `get(slot: u8)` executes as `slots[slot as usize]`
//!    with direct memory indexing, zero SipHash/FxHash, zero hash collisions, zero probing.

use http::HeaderValue;

/// Maximum number of fast-path header slots supported by the 16-bit mask.
pub const MAX_HEADER_SLOTS: usize = 16;

/// Anonymous integer-indexed header slot container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderSlotTable {
    /// 16-bit mask indicating presence of each slot index (bit `i` = 1 => slot `i` present).
    pub present_mask: u16,
    /// Flat array of header values.
    pub slots: [Option<HeaderValue>; MAX_HEADER_SLOTS],
}

impl HeaderSlotTable {
    /// Creates an empty slot table.
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            present_mask: 0,
            slots: [const { None }; MAX_HEADER_SLOTS],
        }
    }

    /// Checks if a slot index is populated in ~0.3ns (single CPU bitwise instruction).
    #[inline(always)]
    pub const fn has(&self, slot: u8) -> bool {
        if (slot as usize) < MAX_HEADER_SLOTS {
            (self.present_mask & (1u16 << slot)) != 0
        } else {
            false
        }
    }

    /// Returns a reference to the header value at `slot` if present.
    #[inline(always)]
    pub fn get(&self, slot: u8) -> Option<&HeaderValue> {
        if self.has(slot) {
            self.slots[slot as usize].as_ref()
        } else {
            None
        }
    }

    /// Inserts or replaces a header value at `slot`.
    #[inline(always)]
    pub fn set(&mut self, slot: u8, value: HeaderValue) {
        if (slot as usize) < MAX_HEADER_SLOTS {
            self.present_mask |= 1u16 << slot;
            self.slots[slot as usize] = Some(value);
        }
    }

    /// Removes a header value at `slot`, returning the previous value if present.
    #[inline(always)]
    pub fn remove(&mut self, slot: u8) -> Option<HeaderValue> {
        if self.has(slot) {
            self.present_mask &= !(1u16 << slot);
            self.slots[slot as usize].take()
        } else {
            None
        }
    }

    /// Clears all slots and resets the bitmask to 0 using hardware bit manipulation.
    #[inline(always)]
    pub fn clear(&mut self) {
        let mut mask = self.present_mask;
        while mask != 0 {
            let slot = mask.trailing_zeros() as usize;
            self.slots[slot] = None;
            mask &= mask - 1;
        }
        self.present_mask = 0;
    }
}

impl Default for HeaderSlotTable {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slot_table_crud() {
        let mut table = HeaderSlotTable::new();
        assert_eq!(table.present_mask, 0);
        assert!(!table.has(5));
        assert!(table.get(5).is_none());

        let val = HeaderValue::from_static("application/json");
        table.set(5, val.clone());
        assert!(table.has(5));
        assert_eq!(table.present_mask, 1 << 5);
        assert_eq!(table.get(5).unwrap(), &val);

        let removed = table.remove(5);
        assert_eq!(removed.unwrap(), val);
        assert!(!table.has(5));
        assert_eq!(table.present_mask, 0);
    }

    #[test]
    fn test_slot_table_clear() {
        let mut table = HeaderSlotTable::new();
        table.set(1, HeaderValue::from_static("a"));
        table.set(15, HeaderValue::from_static("b"));
        assert_eq!(table.present_mask, (1 << 1) | (1 << 15));

        table.clear();
        assert_eq!(table.present_mask, 0);
        assert!(!table.has(1));
        assert!(!table.has(15));
    }
}
