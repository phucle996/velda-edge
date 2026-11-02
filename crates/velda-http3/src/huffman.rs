//! Canonical RFC 7541 / RFC 9204 Huffman coding for HPACK and QPACK.
//!
//! Provides zero-alloc decoding and encoding between raw ASCII/binary octets
//! and canonical Huffman variable-length bit sequences.

/// RFC 7541 Appendix B Huffman code table entry: (code, bit_length).
#[rustfmt::skip]
pub static HUFFMAN_CODES: [(u32, u8); 257] = [
    (0x1ff8, 13), // 0
    (0x7fffd8, 23), // 1
    (0xfffffe2, 28), // 2
    (0xfffffe3, 28), // 3
    (0xfffffe4, 28), // 4
    (0xfffffe5, 28), // 5
    (0xfffffe6, 28), // 6
    (0xfffffe7, 28), // 7
    (0xfffffe8, 28), // 8
    (0xffffea, 24), // 9
    (0x3ffffffc, 30), // 10
    (0xfffffe9, 28), // 11
    (0xfffffea, 28), // 12
    (0x3ffffffd, 30), // 13
    (0xfffffeb, 28), // 14
    (0xfffffec, 28), // 15
    (0xfffffed, 28), // 16
    (0xfffffee, 28), // 17
    (0xfffffef, 28), // 18
    (0xffffff0, 28), // 19
    (0xffffff1, 28), // 20
    (0xffffff2, 28), // 21
    (0x3ffffffe, 30), // 22
    (0xffffff3, 28), // 23
    (0xffffff4, 28), // 24
    (0xffffff5, 28), // 25
    (0xffffff6, 28), // 26
    (0xffffff7, 28), // 27
    (0xffffff8, 28), // 28
    (0xffffff9, 28), // 29
    (0xffffffa, 28), // 30
    (0xffffffb, 28), // 31
    (0x14, 6), // 32
    (0x3f8, 10), // 33
    (0x3f9, 10), // 34
    (0xffa, 12), // 35
    (0x1ff9, 13), // 36
    (0x15, 6), // 37
    (0xf8, 8), // 38
    (0x7fa, 11), // 39
    (0x3fa, 10), // 40
    (0x3fb, 10), // 41
    (0xf9, 8), // 42
    (0x7fb, 11), // 43
    (0xfa, 8), // 44
    (0x16, 6), // 45
    (0x17, 6), // 46
    (0x18, 6), // 47
    (0x0, 5), // 48
    (0x1, 5), // 49
    (0x2, 5), // 50
    (0x19, 6), // 51
    (0x1a, 6), // 52
    (0x1b, 6), // 53
    (0x1c, 6), // 54
    (0x1d, 6), // 55
    (0x1e, 6), // 56
    (0x1f, 6), // 57
    (0x5c, 7), // 58
    (0xfb, 8), // 59
    (0x7ffc, 15), // 60
    (0x20, 6), // 61
    (0xffb, 12), // 62
    (0x3fc, 10), // 63
    (0x1ffa, 13), // 64
    (0x21, 6), // 65
    (0x5d, 7), // 66
    (0x5e, 7), // 67
    (0x5f, 7), // 68
    (0x60, 7), // 69
    (0x61, 7), // 70
    (0x62, 7), // 71
    (0x63, 7), // 72
    (0x64, 7), // 73
    (0x65, 7), // 74
    (0x66, 7), // 75
    (0x67, 7), // 76
    (0x68, 7), // 77
    (0x69, 7), // 78
    (0x6a, 7), // 79
    (0x6b, 7), // 80
    (0x6c, 7), // 81
    (0x6d, 7), // 82
    (0x6e, 7), // 83
    (0x6f, 7), // 84
    (0x70, 7), // 85
    (0x71, 7), // 86
    (0x72, 7), // 87
    (0xfc, 8), // 88
    (0x73, 7), // 89
    (0xfd, 8), // 90
    (0x1ffb, 13), // 91
    (0x7fff0, 19), // 92
    (0x1ffc, 13), // 93
    (0x3ffc, 14), // 94
    (0x22, 6), // 95
    (0x7ffd, 15), // 96
    (0x3, 5), // 97
    (0x23, 6), // 98
    (0x4, 5), // 99
    (0x24, 6), // 100
    (0x5, 5), // 101
    (0x25, 6), // 102
    (0x26, 6), // 103
    (0x27, 6), // 104
    (0x6, 5), // 105
    (0x74, 7), // 106
    (0x75, 7), // 107
    (0x28, 6), // 108
    (0x29, 6), // 109
    (0x2a, 6), // 110
    (0x7, 5), // 111
    (0x2b, 6), // 112
    (0x76, 7), // 113
    (0x2c, 6), // 114
    (0x8, 5), // 115
    (0x9, 5), // 116
    (0x2d, 6), // 117
    (0x77, 7), // 118
    (0x78, 7), // 119
    (0x79, 7), // 120
    (0x7a, 7), // 121
    (0x7b, 7), // 122
    (0x7ffe, 15), // 123
    (0x7fc, 11), // 124
    (0x3ffd, 14), // 125
    (0x1ffd, 13), // 126
    (0xffffffc, 28), // 127
    (0xfffe6, 20), // 128
    (0x3fffd2, 22), // 129
    (0xfffe7, 20), // 130
    (0xfffe8, 20), // 131
    (0x3fffd3, 22), // 132
    (0x3fffd4, 22), // 133
    (0x3fffd5, 22), // 134
    (0x7fffd9, 23), // 135
    (0x3fffd6, 22), // 136
    (0x7fffda, 23), // 137
    (0x7fffdb, 23), // 138
    (0x7fffdc, 23), // 139
    (0x7fffdd, 23), // 140
    (0x7fffde, 23), // 141
    (0xffffeb, 24), // 142
    (0x7fffdf, 23), // 143
    (0xffffec, 24), // 144
    (0xffffed, 24), // 145
    (0x3fffd7, 22), // 146
    (0x7fffe0, 23), // 147
    (0xffffee, 24), // 148
    (0x7fffe1, 23), // 149
    (0x7fffe2, 23), // 150
    (0x7fffe3, 23), // 151
    (0x7fffe4, 23), // 152
    (0x1fffdc, 21), // 153
    (0x3fffd8, 22), // 154
    (0x7fffe5, 23), // 155
    (0x3fffd9, 22), // 156
    (0x7fffe6, 23), // 157
    (0x7fffe7, 23), // 158
    (0xffffef, 24), // 159
    (0x3fffda, 22), // 160
    (0x1fffdd, 21), // 161
    (0xfffe9, 20), // 162
    (0x3fffdb, 22), // 163
    (0x3fffdc, 22), // 164
    (0x7fffe8, 23), // 165
    (0x7fffe9, 23), // 166
    (0x1fffde, 21), // 167
    (0x7fffea, 23), // 168
    (0x3fffdd, 22), // 169
    (0x3fffde, 22), // 170
    (0xfffff0, 24), // 171
    (0x1fffdf, 21), // 172
    (0x3fffdf, 22), // 173
    (0x7fffeb, 23), // 174
    (0x7fffec, 23), // 175
    (0x1fffe0, 21), // 176
    (0x1fffe1, 21), // 177
    (0x3fffe0, 22), // 178
    (0x1fffe2, 21), // 179
    (0x7fffed, 23), // 180
    (0x3fffe1, 22), // 181
    (0x7fffee, 23), // 182
    (0x7fffef, 23), // 183
    (0xfffea, 20), // 184
    (0x3fffe2, 22), // 185
    (0x3fffe3, 22), // 186
    (0x3fffe4, 22), // 187
    (0x7ffff0, 23), // 188
    (0x3fffe5, 22), // 189
    (0x3fffe6, 22), // 190
    (0x7ffff1, 23), // 191
    (0x3ffffe0, 26), // 192
    (0x3ffffe1, 26), // 193
    (0xfffeb, 20), // 194
    (0x7fff1, 19), // 195
    (0x3fffe7, 22), // 196
    (0x7ffff2, 23), // 197
    (0x3fffe8, 22), // 198
    (0x1ffffec, 25), // 199
    (0x3ffffe2, 26), // 200
    (0x3ffffe3, 26), // 201
    (0x3ffffe4, 26), // 202
    (0x7ffffde, 27), // 203
    (0x7ffffdf, 27), // 204
    (0x3ffffe5, 26), // 205
    (0xfffff1, 24), // 206
    (0x1ffffed, 25), // 207
    (0x7fff2, 19), // 208
    (0x1fffe3, 21), // 209
    (0x3ffffe6, 26), // 210
    (0x7ffffe0, 27), // 211
    (0x7ffffe1, 27), // 212
    (0x3ffffe7, 26), // 213
    (0x7ffffe2, 27), // 214
    (0xfffff2, 24), // 215
    (0x1fffe4, 21), // 216
    (0x1fffe5, 21), // 217
    (0x3ffffe8, 26), // 218
    (0x3ffffe9, 26), // 219
    (0xffffffd, 28), // 220
    (0x7ffffe3, 27), // 221
    (0x7ffffe4, 27), // 222
    (0x7ffffe5, 27), // 223
    (0xfffec, 20), // 224
    (0xfffff3, 24), // 225
    (0xfffed, 20), // 226
    (0x1fffe6, 21), // 227
    (0x3fffe9, 22), // 228
    (0x1fffe7, 21), // 229
    (0x1fffe8, 21), // 230
    (0x7ffff3, 23), // 231
    (0x3fffea, 22), // 232
    (0x3fffeb, 22), // 233
    (0x1ffffee, 25), // 234
    (0x1ffffef, 25), // 235
    (0xfffff4, 24), // 236
    (0xfffff5, 24), // 237
    (0x3ffffea, 26), // 238
    (0x7ffff4, 23), // 239
    (0x3ffffeb, 26), // 240
    (0x7ffffe6, 27), // 241
    (0x3ffffec, 26), // 242
    (0x3ffffed, 26), // 243
    (0x7ffffe7, 27), // 244
    (0x7ffffe8, 27), // 245
    (0x7ffffe9, 27), // 246
    (0x7ffffea, 27), // 247
    (0x7ffffeb, 27), // 248
    (0xffffffe, 28), // 249
    (0x7ffffec, 27), // 250
    (0x7ffffed, 27), // 251
    (0x7ffffee, 27), // 252
    (0x7ffffef, 27), // 253
    (0x7fffff0, 27), // 254
    (0x3ffffee, 26), // 255
    (0x3fffffff, 30), // 256
];

type EncodeHuffmanFn = fn(&[u8], &mut bytes::BytesMut);

static ENCODE_HUFFMAN_FN: std::sync::LazyLock<EncodeHuffmanFn> = std::sync::LazyLock::new(|| {
    let width = velda_core::global_hardware_topology()
        .cpu
        .register_capabilities
        .select_for_workload(velda_core::RegisterWidth::Bits64);
    match width {
        velda_core::RegisterWidth::Bits64
        | velda_core::RegisterWidth::Bits128
        | velda_core::RegisterWidth::Bits256 => encode_huffman_64,
        velda_core::RegisterWidth::Bits32 => encode_huffman_32,
    }
});

/// 64-bit register Huffman encoder: packs codes into a 64-bit bit-buffer and flushes 32 bits (4 bytes)
/// at a single instruction (`put_u32`) whenever `bits >= 32`.
fn encode_huffman_64(input: &[u8], dst: &mut bytes::BytesMut) {
    use bytes::BufMut;
    dst.reserve(input.len());

    let mut current: u64 = 0;
    let mut bits: u8 = 0;

    for &byte in input {
        let (code, len) = HUFFMAN_CODES[byte as usize];
        current = (current << len) | (code as u64);
        bits += len;

        if bits >= 32 {
            bits -= 32;
            dst.put_u32((current >> bits) as u32);
        }
    }

    while bits >= 8 {
        bits -= 8;
        dst.put_u8((current >> bits) as u8);
    }

    // Pad with 1s to octet boundary (EOS prefix)
    if bits > 0 {
        let pad = 8 - bits;
        current = (current << pad) | ((1u64 << pad) - 1);
        dst.put_u8(current as u8);
    }
}

/// Dual 32-bit register Huffman encoder for 32-bit targets.
fn encode_huffman_32(input: &[u8], dst: &mut bytes::BytesMut) {
    use bytes::BufMut;
    dst.reserve(input.len());

    let mut acc: u32 = 0;
    let mut bits: u8 = 0;

    for &byte in input {
        let (code, len) = HUFFMAN_CODES[byte as usize];
        // Since max code len is 30 bits, flush bytes first so `bits + len <= 32`
        while bits + len > 32 {
            bits -= 8;
            dst.put_u8((acc >> bits) as u8);
        }
        acc = acc.checked_shl(len as u32).unwrap_or(0) | code;
        bits += len;

        while bits >= 8 {
            bits -= 8;
            dst.put_u8((acc >> bits) as u8);
        }
    }

    if bits > 0 {
        let pad = 8 - bits;
        acc = (acc << pad) | ((1u32 << pad) - 1);
        dst.put_u8(acc as u8);
    }
}

/// Encodes raw bytes using RFC 7541 Huffman coding via the hardware-selected register function pointer.
#[inline]
pub fn encode_huffman(input: &[u8], dst: &mut bytes::BytesMut) {
    (ENCODE_HUFFMAN_FN)(input, dst)
}

#[derive(Clone, Copy)]
struct HuffmanNode {
    children: [u16; 2],
    sym: u16,
}

const NIBBLE_FLAG_EMIT: u8 = 0x01;
const NIBBLE_FLAG_FAIL: u8 = 0x02;

/// 4-byte L1-cached 4-bit nibble state transition entry.
/// Since the shortest RFC 7541 Huffman code is 5 bits, a 4-bit nibble can emit at most 1 symbol.
#[derive(Clone, Copy)]
#[repr(C)]
struct NibbleTransition {
    next_state: u16,
    sym: u8,
    flags: u8,
}

struct HuffmanDecoderAutomaton {
    /// Indexed by `[state * 16 + nibble]`.
    transitions: Vec<NibbleTransition>,
    /// True if `state` is a valid EOF padding state (0..=7 consecutive '1' bits from root).
    valid_eof: Vec<bool>,
}

static HUFFMAN_AUTOMATON: std::sync::OnceLock<HuffmanDecoderAutomaton> = std::sync::OnceLock::new();

fn get_huffman_automaton() -> &'static HuffmanDecoderAutomaton {
    HUFFMAN_AUTOMATON.get_or_init(|| {
        let mut nodes = vec![HuffmanNode {
            children: [0, 0],
            sym: 0xffff,
        }];
        for (sym, &(code, len)) in HUFFMAN_CODES.iter().enumerate() {
            let mut curr = 0;
            for i in (0..len).rev() {
                let bit = ((code >> i) & 1) as usize;
                let next = nodes[curr].children[bit];
                if next == 0 {
                    let new_idx = nodes.len() as u16;
                    nodes.push(HuffmanNode {
                        children: [0, 0],
                        sym: 0xffff,
                    });
                    nodes[curr].children[bit] = new_idx;
                    curr = new_idx as usize;
                } else {
                    curr = next as usize;
                }
            }
            nodes[curr].sym = sym as u16;
        }

        let num_states = nodes.len();
        let mut transitions = vec![
            NibbleTransition {
                next_state: 0,
                sym: 0,
                flags: NIBBLE_FLAG_FAIL,
            };
            num_states * 16
        ];

        for state in 0..num_states {
            if nodes[state].sym != 0xffff {
                continue;
            }
            for nib in 0..16usize {
                let mut curr = state;
                let mut sym = 0u8;
                let mut flags = 0u8;

                for shift in (0..4).rev() {
                    let bit = (nib >> shift) & 1;
                    let next = nodes[curr].children[bit] as usize;
                    if next == 0 {
                        flags = NIBBLE_FLAG_FAIL;
                        break;
                    }
                    curr = next;
                    if nodes[curr].sym != 0xffff {
                        let s = nodes[curr].sym;
                        if s == 256 {
                            // EOS symbol is strictly forbidden in payload
                            flags = NIBBLE_FLAG_FAIL;
                            break;
                        }
                        sym = s as u8;
                        flags |= NIBBLE_FLAG_EMIT;
                        curr = 0;
                    }
                }

                transitions[state * 16 + nib] = NibbleTransition {
                    next_state: curr as u16,
                    sym,
                    flags,
                };
            }
        }

        // Precompute valid EOF states (RFC 7541 Section 5.2: 0 to 7 consecutive '1' bits from root 0)
        let mut valid_eof = vec![false; num_states];
        let mut eof_curr = 0usize;
        valid_eof[0] = true;
        for _ in 0..7 {
            eof_curr = nodes[eof_curr].children[1] as usize;
            if eof_curr == 0 {
                break;
            }
            valid_eof[eof_curr] = true;
        }

        HuffmanDecoderAutomaton {
            transitions,
            valid_eof,
        }
    })
}

/// Decodes RFC 7541 / RFC 9204 Huffman encoded bytes into a String.
///
/// Uses a precomputed L1-cached 4-bit nibble automaton (2 transitions per byte instead of 8 bit-steps),
/// cutting branch and memory lookup overhead by 4x.
pub fn decode_huffman(input: &[u8]) -> Option<String> {
    let automaton = get_huffman_automaton();
    let table = automaton.transitions.as_slice();
    // Huffman decompressed strings typically expand by ~1.3-1.5x. Pre-allocate 1.5x capacity to eliminate reallocs.
    let expanded_cap = input.len() + (input.len() >> 1).max(16);
    let mut out = Vec::with_capacity(expanded_cap);
    let mut state = 0usize;

    for &byte in input {
        let hi_nib = (byte >> 4) as usize;
        let t_hi = table[state * 16 + hi_nib];
        if (t_hi.flags & NIBBLE_FLAG_FAIL) != 0 {
            return None;
        }
        if (t_hi.flags & NIBBLE_FLAG_EMIT) != 0 {
            out.push(t_hi.sym);
        }
        state = t_hi.next_state as usize;

        let lo_nib = (byte & 0x0f) as usize;
        let t_lo = table[state * 16 + lo_nib];
        if (t_lo.flags & NIBBLE_FLAG_FAIL) != 0 {
            return None;
        }
        if (t_lo.flags & NIBBLE_FLAG_EMIT) != 0 {
            out.push(t_lo.sym);
        }
        state = t_lo.next_state as usize;
    }

    // Verify RFC 7541 Section 5.2 EOF padding in O(1):
    // State must correspond to 0..=7 '1'-bits from root.
    if !automaton.valid_eof[state] {
        return None;
    }

    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_huffman_roundtrip_arbitrary_strings() {
        let sample_strings = [
            "custom-key",
            "custom-value",
            "application/json",
            "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            "Velda-Edge/1.0 (High-Performance QUIC L7 Engine)",
            "/api/v1/workspaces/velda/metrics?interval=60s",
        ];
        for s in sample_strings {
            let mut encoded = BytesMut::new();
            encode_huffman(s.as_bytes(), &mut encoded);
            let decoded = decode_huffman(&encoded).unwrap();
            assert_eq!(decoded, s);
        }
    }

    #[test]
    fn test_huffman_no_cache_roundtrip() {
        let text = "no-cache";
        let mut encoded = BytesMut::new();
        encode_huffman(text.as_bytes(), &mut encoded);
        assert_eq!(&encoded[..], &[0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf]);

        let decoded = decode_huffman(&encoded).unwrap();
        assert_eq!(decoded, text);
    }

    #[test]
    fn test_huffman_url_roundtrip() {
        let text = "www.example.com";
        let mut encoded = BytesMut::new();
        encode_huffman(text.as_bytes(), &mut encoded);
        assert_eq!(
            &encoded[..],
            &[
                0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff
            ]
        );

        let decoded = decode_huffman(&encoded).unwrap();
        assert_eq!(decoded, text);
    }
}
