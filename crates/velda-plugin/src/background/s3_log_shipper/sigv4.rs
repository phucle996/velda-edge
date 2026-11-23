//! AWS Signature Version 4 Signer (`ring::digest::SHA256` + `ring::hmac::HMAC_SHA256`).
//!
//! ```text
//!   SigV4Signer::new(access_key, secret_key, region, host_header)
//!         │
//!         ├─► daily key cache hit (YYYYMMDD == cached_date) ──► reuse kSigning
//!         ├─► daily key cache miss ──► HMAC_SHA256 chain (AWS4secret -> date -> region -> s3 -> aws4_request)
//!         └─► CanonicalRequest -> SHA256 -> StringToSign -> HMAC_SHA256 -> HTTP/1.1 PUT headers into out_head
//!
//!   Cache & Header Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ SigV4Signer { cached_date: [u8; 8], cached_signing_key: Option<hmac::Key> } │
//!   │ Output Head: PUT {uri} HTTP/1.1 | Host | x-amz-date | x-amz-content-sha256 │
//!   │              [x-amz-security-token] | Authorization | Content-Length (LUT) │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Precomputes `AWS4{secret}` prefix and caches the daily `kSigning` key (`YYYYMMDD`)
//! so same-day S3 `PUT` uploads execute only a single `HMAC_SHA256(StringToSign)`.

use ring::{digest, hmac};

const HEX: &[u8; 16] = b"0123456789abcdef";
const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

/// Percent-encodes an S3 URI path per AWS SigV4 (`A-Za-z0-9-._~/` unencoded).
pub fn encode_s3_canonical_uri(path: &str) -> String {
    let bytes = path.as_bytes();
    let needs_encode = bytes
        .iter()
        .any(|&b| !(b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/')));
    if !needs_encode {
        return path.to_string();
    }
    let mut out = String::with_capacity(path.len() + 16);
    for &b in bytes {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX_UPPER[(b >> 4) as usize] as char);
            out.push(HEX_UPPER[(b & 0x0f) as usize] as char);
        }
    }
    out
}

/// Encodes arbitrary bytes as lowercase hex into `out`.
#[inline]
pub fn write_hex_lower(bytes: &[u8], out: &mut [u8]) {
    for (i, &b) in bytes.iter().enumerate() {
        if let Some(dst) = out.get_mut(i * 2..i * 2 + 2) {
            dst[0] = HEX[(b >> 4) as usize];
            dst[1] = HEX[(b & 0x0f) as usize];
        }
    }
}

/// Converts Unix epoch seconds into `(year, month, day, hour, min, sec)` UTC.
pub fn unix_secs_to_utc_ymdhms(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sec = (secs % 60) as u32;
    let total_mins = secs / 60;
    let min = (total_mins % 60) as u32;
    let total_hours = total_mins / 60;
    let hour = (total_hours % 24) as u32;
    let days = (total_hours / 24) as i64;

    // Howard Hinnant's civil-from-days algorithm (exact Gregorian UTC, 0 allocs)
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = (y + if m <= 2 { 1 } else { 0 }) as u32;
    (year, m as u32, d as u32, hour, min, sec)
}

/// Formats `YYYYMMDDTHHMMSSZ` (16 bytes) and `YYYYMMDD` (8 bytes) from Unix seconds.
pub fn format_amz_date(unix_secs: u64) -> ([u8; 16], [u8; 8]) {
    let (y, m, d, hh, mm, ss) = unix_secs_to_utc_ymdhms(unix_secs);
    let mut full = [0u8; 16];
    full[0] = b'0' + ((y / 1000) % 10) as u8;
    full[1] = b'0' + ((y / 100) % 10) as u8;
    full[2] = b'0' + ((y / 10) % 10) as u8;
    full[3] = b'0' + (y % 10) as u8;
    full[4] = b'0' + (m / 10) as u8;
    full[5] = b'0' + (m % 10) as u8;
    full[6] = b'0' + (d / 10) as u8;
    full[7] = b'0' + (d % 10) as u8;
    full[8] = b'T';
    full[9] = b'0' + (hh / 10) as u8;
    full[10] = b'0' + (hh % 10) as u8;
    full[11] = b'0' + (mm / 10) as u8;
    full[12] = b'0' + (mm % 10) as u8;
    full[13] = b'0' + (ss / 10) as u8;
    full[14] = b'0' + (ss % 10) as u8;
    full[15] = b'Z';

    let mut date = [0u8; 8];
    date.copy_from_slice(&full[..8]);
    (full, date)
}

/// Precomputed AWS SigV4 signer with per-day `kSigning` key caching.
pub struct SigV4Signer {
    access_key_id: String,
    aws4_secret: Vec<u8>,
    region: String,
    host_header: String,
    cached_date: [u8; 8],
    cached_signing_key: Option<hmac::Key>,
}

impl SigV4Signer {
    /// Creates a new signer with precomputed `AWS4{secret}` key material.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: &str,
        region: impl Into<String>,
        host_header: impl Into<String>,
    ) -> Self {
        let mut aws4_secret = Vec::with_capacity(4 + secret_access_key.len());
        aws4_secret.extend_from_slice(b"AWS4");
        aws4_secret.extend_from_slice(secret_access_key.as_bytes());
        Self {
            access_key_id: access_key_id.into(),
            aws4_secret,
            region: region.into(),
            host_header: host_header.into(),
            cached_date: [0u8; 8],
            cached_signing_key: None,
        }
    }

    fn signing_key_for_date(&mut self, date_yyyymmdd: &[u8; 8]) -> &hmac::Key {
        if &self.cached_date != date_yyyymmdd || self.cached_signing_key.is_none() {
            let k_secret = hmac::Key::new(hmac::HMAC_SHA256, &self.aws4_secret);
            let k_date = hmac::sign(&k_secret, date_yyyymmdd);
            let k_date_key = hmac::Key::new(hmac::HMAC_SHA256, k_date.as_ref());
            let k_region = hmac::sign(&k_date_key, self.region.as_bytes());
            let k_region_key = hmac::Key::new(hmac::HMAC_SHA256, k_region.as_ref());
            let k_service = hmac::sign(&k_region_key, b"s3");
            let k_service_key = hmac::Key::new(hmac::HMAC_SHA256, k_service.as_ref());
            let k_signing = hmac::sign(&k_service_key, b"aws4_request");

            self.cached_date = *date_yyyymmdd;
            self.cached_signing_key = Some(hmac::Key::new(hmac::HMAC_SHA256, k_signing.as_ref()));
        }
        self.cached_signing_key.as_ref().unwrap()
    }

    /// Signs an S3 `PUT` request and writes the complete HTTP/1.1 request headers into `out_head`.
    pub fn write_signed_put_headers(
        &mut self,
        canonical_uri: &str,
        payload_len: usize,
        payload_sha256_hex: &[u8; 64],
        unix_secs: u64,
        session_token: Option<&str>,
        out_head: &mut Vec<u8>,
    ) {
        let (amz_date, date_short) = format_amz_date(unix_secs);
        let amz_date_str = std::str::from_utf8(&amz_date).unwrap_or("");
        let date_short_str = std::str::from_utf8(&date_short).unwrap_or("");
        let payload_hash_str = std::str::from_utf8(payload_sha256_hex).unwrap_or("");

        let (signed_headers, token_canon) = match session_token {
            Some(tok) => (
                "host;x-amz-content-sha256;x-amz-date;x-amz-security-token",
                format!("x-amz-security-token:{tok}\n"),
            ),
            None => ("host;x-amz-content-sha256;x-amz-date", String::new()),
        };

        let encoded_uri = encode_s3_canonical_uri(canonical_uri);
        let canonical_request = format!(
            "PUT\n{encoded_uri}\n\nhost:{host}\nx-amz-content-sha256:{payload_hash_str}\nx-amz-date:{amz_date_str}\n{token_canon}\n{signed_headers}\n{payload_hash_str}",
            host = self.host_header,
        );

        let canon_digest = digest::digest(&digest::SHA256, canonical_request.as_bytes());
        let mut canon_hex = [0u8; 64];
        write_hex_lower(canon_digest.as_ref(), &mut canon_hex);
        let canon_hex_str = std::str::from_utf8(&canon_hex).unwrap_or("");

        let scope = format!("{date_short_str}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date_str}\n{scope}\n{canon_hex_str}");

        let key = self.signing_key_for_date(&date_short);
        let sig = hmac::sign(key, string_to_sign.as_bytes());
        let mut sig_hex = [0u8; 64];
        write_hex_lower(sig.as_ref(), &mut sig_hex);

        out_head.clear();
        out_head.extend_from_slice(b"PUT ");
        out_head.extend_from_slice(canonical_uri.as_bytes());
        out_head.extend_from_slice(b" HTTP/1.1\r\nHost: ");
        out_head.extend_from_slice(self.host_header.as_bytes());
        out_head.extend_from_slice(b"\r\nx-amz-date: ");
        out_head.extend_from_slice(&amz_date);
        out_head.extend_from_slice(b"\r\nx-amz-content-sha256: ");
        out_head.extend_from_slice(payload_sha256_hex);
        out_head.extend_from_slice(b"\r\n");
        if let Some(tok) = session_token {
            out_head.extend_from_slice(b"x-amz-security-token: ");
            out_head.extend_from_slice(tok.as_bytes());
            out_head.extend_from_slice(b"\r\n");
        }
        out_head.extend_from_slice(b"Authorization: AWS4-HMAC-SHA256 Credential=");
        out_head.extend_from_slice(self.access_key_id.as_bytes());
        out_head.push(b'/');
        out_head.extend_from_slice(&date_short);
        out_head.push(b'/');
        out_head.extend_from_slice(self.region.as_bytes());
        out_head.extend_from_slice(b"/s3/aws4_request, SignedHeaders=");
        out_head.extend_from_slice(signed_headers.as_bytes());
        out_head.extend_from_slice(b", Signature=");
        out_head.extend_from_slice(&sig_hex);
        out_head.extend_from_slice(b"\r\nContent-Length: ");
        velda_core::lut::decimal::fast_write_u64(out_head, payload_len as u64);
        out_head.extend_from_slice(b"\r\nConnection: keep-alive\r\n\r\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unix_secs_to_utc_ymdhms() {
        // 2024-11-19 11:30:00 UTC -> 1732015800
        assert_eq!(
            unix_secs_to_utc_ymdhms(1_732_015_800),
            (2024, 11, 19, 11, 30, 0)
        );
        let (full, short) = format_amz_date(1_732_015_800);
        assert_eq!(&full, b"20241119T113000Z");
        assert_eq!(&short, b"20241119");
    }

    #[test]
    fn test_sigv4_signing_determinism_and_cache() {
        let mut signer = SigV4Signer::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
            "s3.amazonaws.com",
        );
        let payload_hash = digest::digest(&digest::SHA256, b"hello");
        let mut hash_hex = [0u8; 64];
        write_hex_lower(payload_hash.as_ref(), &mut hash_hex);

        let mut h1 = Vec::new();
        let mut h2 = Vec::new();
        signer.write_signed_put_headers(
            "/bucket/obj.jsonl",
            5,
            &hash_hex,
            1_732_015_800,
            None,
            &mut h1,
        );
        signer.write_signed_put_headers(
            "/bucket/obj.jsonl",
            5,
            &hash_hex,
            1_732_015_800,
            None,
            &mut h2,
        );
        assert_eq!(h1, h2);
        let text = std::str::from_utf8(&h1).unwrap();
        assert!(text.contains("Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20241119/us-east-1/s3/aws4_request"));
        assert!(text.contains("Content-Length: 5\r\n"));
    }
}
