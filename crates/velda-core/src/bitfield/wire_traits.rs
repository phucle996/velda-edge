//! 40-bit Non-Colliding L7 Wire Framing Bitfield Dictionary (`wire_traits`).
//!
//! ```text
//!  39..29     │ 28..27 │ 26..24 │ 23..20  │ 19..16   │ 15..13  │ 12  │ 11  │ 10   │ 9..8 │ 7..5  │ 4..0
//!  settings3  │ mframe │ mstr   │ connwin │ initwin  │ hdrtbl  │ odd │ pri │ norf │ push │ proto │ pseudo
//! ```
//!
//! Packed into `RequestState.wire_traits_lo: u32` + `wire_traits_hi: u8`. Protocol crates
//! (`velda-http1`, `velda-http2`, `velda-http3`, `velda-grpc`) compute and stamp these bits directly.

/// Canonical pseudo-header tokens for lossless wire order encoding.
pub const PSEUDO_METHOD: u8 = 0;
pub const PSEUDO_AUTHORITY: u8 = 1;
pub const PSEUDO_SCHEME: u8 = 2;
pub const PSEUDO_PATH: u8 = 3;

/// Wire traits bit offsets and masks (40-bit value inside `u64`).
pub const WIRE_PSEUDO_ORDER_MASK: u64 = 0x1F;
pub const WIRE_PROTO_SHIFT: u32 = 5;
pub const WIRE_PROTO_MASK: u64 = 0x7 << WIRE_PROTO_SHIFT;
pub const WIRE_PROTO_NONE: u64 = 0;
pub const WIRE_PROTO_H1: u64 = 1;
pub const WIRE_PROTO_H2: u64 = 2;
pub const WIRE_PROTO_H3: u64 = 3;

pub const WIRE_ENABLE_PUSH_SHIFT: u32 = 8;
pub const WIRE_ENABLE_PUSH_MASK: u64 = 0x3 << WIRE_ENABLE_PUSH_SHIFT;
pub const WIRE_FLAG_NO_RFC7540_PRIORITIES: u64 = 1 << 10;
pub const WIRE_FLAG_H2_PRIORITY_USED: u64 = 1 << 11;
pub const WIRE_FLAG_ODD_SETTINGS: u64 = 1 << 12;

pub const WIRE_HDR_TABLE_SHIFT: u32 = 13;
pub const WIRE_HDR_TABLE_MASK: u64 = 0x7 << WIRE_HDR_TABLE_SHIFT;
pub const WIRE_INIT_WIN_SHIFT: u32 = 16;
pub const WIRE_INIT_WIN_MASK: u64 = 0xF << WIRE_INIT_WIN_SHIFT;
pub const WIRE_CONN_WIN_SHIFT: u32 = 20;
pub const WIRE_CONN_WIN_MASK: u64 = 0xF << WIRE_CONN_WIN_SHIFT;
pub const WIRE_MAX_STREAMS_SHIFT: u32 = 24;
pub const WIRE_MAX_STREAMS_MASK: u64 = 0x7 << WIRE_MAX_STREAMS_SHIFT;
pub const WIRE_MAX_FRAME_SHIFT: u32 = 27;
pub const WIRE_MAX_FRAME_MASK: u64 = 0x3 << WIRE_MAX_FRAME_SHIFT;

pub const WIRE_SETTINGS_SEQ_SHIFT: u32 = 29;
pub const WIRE_SETTINGS_SEQ_MASK: u64 = 0x7FF << WIRE_SETTINGS_SEQ_SHIFT;

/// Well-known lossless 4-pseudo-header order tokens (`1..=24`), matching `encode_pseudo_order`.
pub const PSEUDO_ORDER_MASP: u8 = encode_pseudo_order(&[0, 1, 2, 3]); // Chrome / Edge / Brave
pub const PSEUDO_ORDER_MPAS: u8 = encode_pseudo_order(&[0, 3, 1, 2]); // Firefox / curl / reqwest
pub const PSEUDO_ORDER_MSPA: u8 = encode_pseudo_order(&[0, 2, 3, 1]); // Safari / WebKit
pub const PSEUDO_ORDER_MSAP: u8 = encode_pseudo_order(&[0, 2, 1, 3]); // Go net/http
pub const PSEUDO_ORDER_MAPS: u8 = encode_pseudo_order(&[0, 1, 3, 2]);
pub const PSEUDO_ORDER_OTHER: u8 = 31;

/// Encodes up to 4 pseudo-header tokens (`0=method, 1=authority, 2=scheme, 3=path`)
/// into a lossless 5-bit vocabulary code (`0..=31`, zero hash collisions).
#[inline]
pub const fn encode_pseudo_order(seq: &[u8]) -> u8 {
    if seq.len() == 4 {
        let a = seq[0];
        let b = seq[1];
        let c = seq[2];
        let d = seq[3];
        if a > 3 || b > 3 || c > 3 || d > 3 {
            return PSEUDO_ORDER_OTHER;
        }
        let bitmask = (1u8 << a) | (1u8 << b) | (1u8 << c) | (1u8 << d);
        if bitmask != 0x0F {
            return PSEUDO_ORDER_OTHER;
        }
        let d0 = ((b < a) as u8) + ((c < a) as u8) + ((d < a) as u8);
        let d1 = ((c < b) as u8) + ((d < b) as u8);
        let d2 = (d < c) as u8;
        return 1 + d0 * 6 + d1 * 2 + d2;
    }
    if seq.len() == 3 {
        return match (seq[0], seq[1], seq[2]) {
            (0, 2, 3) => 25,
            (0, 3, 2) => 26,
            (0, 1, 3) => 27,
            (0, 3, 1) => 28,
            (0, 1, 2) => 29,
            _ => PSEUDO_ORDER_OTHER,
        };
    }
    if seq.is_empty() {
        0
    } else {
        PSEUDO_ORDER_OTHER
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pseudo_order_permutations_are_bijective() {
        let mut seen = std::collections::HashSet::new();
        for a in 0..4u8 {
            for b in 0..4u8 {
                if b == a {
                    continue;
                }
                for c in 0..4u8 {
                    if c == a || c == b {
                        continue;
                    }
                    let d = 6 - a - b - c;
                    let code = encode_pseudo_order(&[a, b, c, d]);
                    assert!((1..=24).contains(&code), "code={code} out of 1..=24");
                    assert!(seen.insert(code), "duplicate code {code}");
                }
            }
        }
        assert_eq!(seen.len(), 24);
        assert_eq!(PSEUDO_ORDER_MASP, 1);
    }
}
