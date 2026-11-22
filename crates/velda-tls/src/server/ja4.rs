//! Zero-Allocation JA4 TLS ClientHello Fingerprinting (`velda_tls::server::ja4`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     ZERO-ALLOCATION JA4 CLIENTHELLO EXTRACTION                          │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  Raw TCP Peek Buffer (&[u8])                                                            │
//! │    ├── TLS Record Header (5B): ContentType=0x16, RecordLen                              │
//! │    ├── Handshake Header  (4B): MsgType=0x01 (ClientHello), BodyLen                      │
//! │    ├── ClientHello Fixed     : ClientVersion (2B), Random (32B), SessionID (1B+N)       │
//! │    ├── Cipher Suites (2B+M)  ──► Stack [u16; 128] (filter GREASE, sort_unstable)        │
//! │    └── Extensions    (2B+E)  ──► Stack [u16; 128] (filter GREASE, SNI/ALPN/SigAlgs)     │
//! │                                                                                         │
//! │  Format: [ja4_a (10B)]_[ja4_b (12B SHA256)]_[ja4_c (12B SHA256)]                        │
//! │          └─► [u8; 36] on stack ──► velda_core::ja4_fingerprint_id_bytes() ──► u64       │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use sha2::{Digest, Sha256};

const HEX_LOWER: &[u8; 16] = b"0123456789abcdef";

/// Maximum cipher suites or extensions tracked on the stack per ClientHello.
const MAX_ITEMS: usize = 128;

/// Returns `true` if `val` is a TLS GREASE value (RFC 8701: `0x0a0a`, `0x1a1a`, ..., `0xfafa`).
#[inline(always)]
const fn is_grease(val: u16) -> bool {
    (val & 0x0f0f) == 0x0a0a && ((val >> 8) == (val & 0xff))
}

#[inline(always)]
fn read_u16(slice: &[u8], offset: &mut usize) -> Option<u16> {
    if *offset + 2 > slice.len() {
        return None;
    }
    let v = u16::from_be_bytes([slice[*offset], slice[*offset + 1]]);
    *offset += 2;
    Some(v)
}

#[inline(always)]
fn read_u24(slice: &[u8], offset: &mut usize) -> Option<usize> {
    if *offset + 3 > slice.len() {
        return None;
    }
    let v = ((slice[*offset] as usize) << 16)
        | ((slice[*offset + 1] as usize) << 8)
        | (slice[*offset + 2] as usize);
    *offset += 3;
    Some(v)
}

#[inline(always)]
fn map_tls_version(v: u16) -> [u8; 2] {
    match v {
        0x0304 => *b"13",
        0x0303 => *b"12",
        0x0302 => *b"11",
        0x0301 => *b"10",
        0x0300 => *b"s3",
        0x0002 => *b"s2",
        _ => *b"00",
    }
}

#[inline(always)]
fn push_hex_u16(hasher: &mut Sha256, val: u16) {
    let bytes = [
        HEX_LOWER[((val >> 12) & 0xf) as usize],
        HEX_LOWER[((val >> 8) & 0xf) as usize],
        HEX_LOWER[((val >> 4) & 0xf) as usize],
        HEX_LOWER[(val & 0xf) as usize],
    ];
    hasher.update(bytes);
}

#[inline(always)]
fn write_2digits(dst: &mut [u8], val: usize) {
    let capped = if val > 99 { 99 } else { val };
    dst[0] = b'0' + (capped / 10) as u8;
    dst[1] = b'0' + (capped % 10) as u8;
}

#[inline(always)]
fn write_sha256_prefix12(dst: &mut [u8], digest: &[u8]) {
    for i in 0..6 {
        let b = digest[i];
        dst[i * 2] = HEX_LOWER[(b >> 4) as usize];
        dst[i * 2 + 1] = HEX_LOWER[(b & 0xf) as usize];
    }
}

/// Computes the canonical 36-byte JA4 fingerprint string from a TLS `ClientHello` record.
///
/// Returns `None` if `record` is not a valid/complete TLS `ClientHello` handshake record.
/// Executes with zero heap allocations using fixed stack buffers.
pub fn compute_ja4_string(record: &[u8], is_quic: bool) -> Option<[u8; 36]> {
    let mut off = 0usize;

    // 1. TLS Record Header (when TCP TLS) or raw Handshake message (when QUIC).
    let hello_end = if !is_quic {
        if record.len() < 5 || record[0] != 0x16 {
            return None;
        }
        let rec_len = u16::from_be_bytes([record[3], record[4]]) as usize;
        off = 5;
        let end = off.checked_add(rec_len)?;
        if end > record.len() {
            return None;
        }
        end
    } else {
        record.len()
    };

    // 2. Handshake Header: MsgType = 0x01 (ClientHello) + 3-byte length
    if off >= hello_end || record[off] != 0x01 {
        return None;
    }
    off += 1;
    let hs_len = read_u24(record, &mut off)?;
    let hs_end = off.checked_add(hs_len)?;
    if hs_end > hello_end {
        return None;
    }

    // 3. ClientHello fixed fields: client_version (2B) + random (32B)
    let client_version = read_u16(record, &mut off)?;
    if off + 32 > hs_end {
        return None;
    }
    off += 32;

    // 4. Session ID (1B len + bytes)
    if off >= hs_end {
        return None;
    }
    let sid_len = record[off] as usize;
    off += 1;
    if off + sid_len > hs_end {
        return None;
    }
    off += sid_len;

    // 5. Cipher Suites (2B byte_len + u16 list)
    let cs_bytes = read_u16(record, &mut off)? as usize;
    if !cs_bytes.is_multiple_of(2) || off + cs_bytes > hs_end {
        return None;
    }
    let cs_end = off + cs_bytes;
    let mut ciphers = [0u16; MAX_ITEMS];
    let mut cipher_count = 0usize;
    while off < cs_end {
        let cs = read_u16(record, &mut off)?;
        if !is_grease(cs) && cipher_count < MAX_ITEMS {
            ciphers[cipher_count] = cs;
            cipher_count += 1;
        }
    }

    // 6. Compression Methods (1B len + bytes)
    if off >= hs_end {
        return None;
    }
    let comp_len = record[off] as usize;
    off += 1;
    if off + comp_len > hs_end {
        return None;
    }
    off += comp_len;

    // 7. Extensions (optional 2B len + extension TLV list)
    let mut total_ext_count = 0usize;
    let mut ja4c_exts = [0u16; MAX_ITEMS];
    let mut ja4c_ext_count = 0usize;
    let mut sig_algs = [0u16; MAX_ITEMS];
    let mut sig_alg_count = 0usize;

    let mut max_supported_ver: Option<u16> = None;
    let mut has_sni_domain = false;
    let mut alpn_pair = *b"00";

    if off + 2 <= hs_end {
        let exts_bytes = read_u16(record, &mut off)? as usize;
        let exts_end = off.checked_add(exts_bytes)?;
        if exts_end > hs_end {
            return None;
        }

        while off < exts_end {
            let ext_id = read_u16(record, &mut off)?;
            let ext_len = read_u16(record, &mut off)? as usize;
            let ext_end = off.checked_add(ext_len)?;
            if ext_end > exts_end {
                return None;
            }

            if !is_grease(ext_id) {
                total_ext_count += 1;

                // ja4_c excludes SNI (0x0000) and ALPN (0x0010)
                if ext_id != 0x0000 && ext_id != 0x0010 && ja4c_ext_count < MAX_ITEMS {
                    ja4c_exts[ja4c_ext_count] = ext_id;
                    ja4c_ext_count += 1;
                }

                match ext_id {
                    // SNI (0x0000): check for host_name (type 0x00)
                    0x0000 => {
                        let mut p = off;
                        if p + 2 <= ext_end {
                            let list_len = read_u16(record, &mut p)? as usize;
                            if p + list_len <= ext_end && p + 3 <= ext_end && record[p] == 0x00 {
                                has_sni_domain = true;
                            }
                        }
                    }
                    // ALPN (0x0010): extract first protocol's first and last byte
                    0x0010 => {
                        let mut p = off;
                        if p + 2 <= ext_end {
                            let list_len = read_u16(record, &mut p)? as usize;
                            if p + list_len <= ext_end && p < ext_end {
                                let proto_len = record[p] as usize;
                                p += 1;
                                if proto_len > 0 && p + proto_len <= ext_end {
                                    let first = record[p];
                                    let last = record[p + proto_len - 1];
                                    if first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric()
                                    {
                                        alpn_pair = [first, last];
                                    } else {
                                        alpn_pair = [
                                            HEX_LOWER[(first >> 4) as usize],
                                            HEX_LOWER[(last & 0xf) as usize],
                                        ];
                                    }
                                }
                            }
                        }
                    }
                    // supported_versions (0x002b): highest non-GREASE version
                    0x002b => {
                        let mut p = off;
                        if p < ext_end {
                            let vlen = record[p] as usize;
                            p += 1;
                            if p + vlen <= ext_end && vlen.is_multiple_of(2) {
                                while p < off + 1 + vlen {
                                    let v = read_u16(record, &mut p)?;
                                    if !is_grease(v) && max_supported_ver.is_none_or(|cur| v > cur)
                                    {
                                        max_supported_ver = Some(v);
                                    }
                                }
                            }
                        }
                    }
                    // signature_algorithms (0x000d): unsorted original order
                    0x000d => {
                        let mut p = off;
                        if p + 2 <= ext_end {
                            let slen = read_u16(record, &mut p)? as usize;
                            if slen.is_multiple_of(2) && p + slen <= ext_end {
                                let send = p + slen;
                                while p < send {
                                    let alg = read_u16(record, &mut p)?;
                                    if sig_alg_count < MAX_ITEMS {
                                        sig_algs[sig_alg_count] = alg;
                                        sig_alg_count += 1;
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }

            off = ext_end;
        }
    }

    // 8. Assemble ja4_a (10 bytes)
    let mut out = [0u8; 36];
    out[0] = if is_quic { b'q' } else { b't' };
    let ver = map_tls_version(max_supported_ver.unwrap_or(client_version));
    out[1] = ver[0];
    out[2] = ver[1];
    out[3] = if has_sni_domain { b'd' } else { b'i' };
    write_2digits(&mut out[4..6], cipher_count);
    write_2digits(&mut out[6..8], total_ext_count);
    out[8] = alpn_pair[0];
    out[9] = alpn_pair[1];
    out[10] = b'_';

    // 9. Assemble ja4_b (12 bytes): SHA-256 of sorted cipher suites
    if cipher_count == 0 {
        out[11..23].copy_from_slice(b"000000000000");
    } else {
        ciphers[..cipher_count].sort_unstable();
        let mut hasher = Sha256::new();
        for (i, &cs) in ciphers[..cipher_count].iter().enumerate() {
            if i > 0 {
                hasher.update(b",");
            }
            push_hex_u16(&mut hasher, cs);
        }
        let digest = hasher.finalize();
        write_sha256_prefix12(&mut out[11..23], &digest);
    }
    out[23] = b'_';

    // 10. Assemble ja4_c (12 bytes): SHA-256 of sorted extensions + '_' + unsorted sig_algs
    if ja4c_ext_count == 0 && sig_alg_count == 0 {
        out[24..36].copy_from_slice(b"000000000000");
    } else {
        ja4c_exts[..ja4c_ext_count].sort_unstable();
        let mut hasher = Sha256::new();
        for (i, &ext) in ja4c_exts[..ja4c_ext_count].iter().enumerate() {
            if i > 0 {
                hasher.update(b",");
            }
            push_hex_u16(&mut hasher, ext);
        }
        if sig_alg_count > 0 {
            hasher.update(b"_");
            for (i, &alg) in sig_algs[..sig_alg_count].iter().enumerate() {
                if i > 0 {
                    hasher.update(b",");
                }
                push_hex_u16(&mut hasher, alg);
            }
        }
        let digest = hasher.finalize();
        write_sha256_prefix12(&mut out[24..36], &digest);
    }

    Some(out)
}

/// Computes the compact 64-bit JA4 fingerprint ID from a TLS `ClientHello` record.
///
/// Returns `0` if `record` is incomplete or not a valid TLS `ClientHello`.
#[inline]
pub fn compute_ja4_u64(record: &[u8], is_quic: bool) -> u64 {
    match compute_ja4_string(record, is_quic) {
        Some(bytes) => velda_core::ja4_fingerprint_id_bytes(&bytes),
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_sample_client_hello() -> Vec<u8> {
        let mut body = Vec::new();
        // client_version: TLS 1.2 (0x0303)
        body.extend_from_slice(&0x0303u16.to_be_bytes());
        // random (32 bytes)
        body.extend_from_slice(&[0xaa; 32]);
        // session_id len = 0
        body.push(0);
        // cipher_suites: GREASE (0x0a0a), 0x1302, 0x1301
        body.extend_from_slice(&6u16.to_be_bytes());
        body.extend_from_slice(&0x0a0au16.to_be_bytes());
        body.extend_from_slice(&0x1302u16.to_be_bytes());
        body.extend_from_slice(&0x1301u16.to_be_bytes());
        // compression methods: 1 byte (null)
        body.push(1);
        body.push(0);

        // extensions
        let mut exts = Vec::new();
        // 1. GREASE extension 0x2a2a
        exts.extend_from_slice(&0x2a2au16.to_be_bytes());
        exts.extend_from_slice(&0u16.to_be_bytes());
        // 2. SNI (0x0000) for "example.com"
        let host = b"example.com";
        let sni_entry_len = 1 + 2 + host.len();
        let sni_ext_len = 2 + sni_entry_len;
        exts.extend_from_slice(&0x0000u16.to_be_bytes());
        exts.extend_from_slice(&(sni_ext_len as u16).to_be_bytes());
        exts.extend_from_slice(&(sni_entry_len as u16).to_be_bytes());
        exts.push(0x00); // host_name
        exts.extend_from_slice(&(host.len() as u16).to_be_bytes());
        exts.extend_from_slice(host);
        // 3. ALPN (0x0010): "h2"
        exts.extend_from_slice(&0x0010u16.to_be_bytes());
        exts.extend_from_slice(&5u16.to_be_bytes());
        exts.extend_from_slice(&3u16.to_be_bytes());
        exts.push(2);
        exts.extend_from_slice(b"h2");
        // 4. supported_versions (0x002b): GREASE 0x3a3a, TLS 1.3 0x0304, TLS 1.2 0x0303
        exts.extend_from_slice(&0x002bu16.to_be_bytes());
        exts.extend_from_slice(&7u16.to_be_bytes());
        exts.push(6);
        exts.extend_from_slice(&0x3a3au16.to_be_bytes());
        exts.extend_from_slice(&0x0304u16.to_be_bytes());
        exts.extend_from_slice(&0x0303u16.to_be_bytes());
        // 5. signature_algorithms (0x000d): 0x0403, 0x0804
        exts.extend_from_slice(&0x000du16.to_be_bytes());
        exts.extend_from_slice(&6u16.to_be_bytes());
        exts.extend_from_slice(&4u16.to_be_bytes());
        exts.extend_from_slice(&0x0403u16.to_be_bytes());
        exts.extend_from_slice(&0x0804u16.to_be_bytes());

        body.extend_from_slice(&(exts.len() as u16).to_be_bytes());
        body.extend_from_slice(&exts);

        // Handshake header
        let hs_len = body.len();
        let mut hs = vec![
            0x01, // ClientHello
            ((hs_len >> 16) & 0xff) as u8,
            ((hs_len >> 8) & 0xff) as u8,
            (hs_len & 0xff) as u8,
        ];
        hs.extend_from_slice(&body);

        // TLS Record header
        let mut rec = Vec::new();
        rec.push(0x16); // Handshake
        rec.extend_from_slice(&0x0301u16.to_be_bytes());
        rec.extend_from_slice(&(hs.len() as u16).to_be_bytes());
        rec.extend_from_slice(&hs);
        rec
    }

    #[test]
    fn test_ja4_extraction_with_grease_filtering() {
        let rec = build_sample_client_hello();
        let ja4 = compute_ja4_string(&rec, false).expect("valid ClientHello");
        let ja4_str = std::str::from_utf8(&ja4).unwrap();

        // Expected ja4_a: t (TCP), 13 (TLS 1.3), d (SNI present), 02 (ciphers), 04 (exts incl SNI+ALPN, excl GREASE), h2 (ALPN)
        assert!(ja4_str.starts_with("t13d0204h2_"), "got {}", ja4_str);
        assert_eq!(ja4_str.len(), 36);

        // Verify deterministic u64 matches velda_core::ja4_fingerprint_id
        let id = compute_ja4_u64(&rec, false);
        assert_ne!(id, 0);
        assert_eq!(id, velda_core::ja4_fingerprint_id(ja4_str));
    }

    #[test]
    fn test_ja4_invalid_and_truncated_inputs() {
        assert_eq!(compute_ja4_u64(&[], false), 0);
        assert_eq!(compute_ja4_u64(b"GET / HTTP/1.1\r\n", false), 0);
        let rec = build_sample_client_hello();
        assert_eq!(compute_ja4_u64(&rec[..10], false), 0);
    }
}
