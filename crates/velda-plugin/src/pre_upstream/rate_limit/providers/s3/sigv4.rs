//! AWS S3 SigV4 request signer (with daily `kSigning` cache + SIMD hex/chunk assembly)
//! (`sigv4.rs`) for `rate_limit` S3/MinIO/R2 provider.
//!
//! ```text
//!   S3SigV4Signer::new(&etp.config)
//!     ├── caches AWS4{secret} + kSigning per UTC day
//!     └── write_signed_request(method, uri_path, body, if_match, unix_secs, &mut creq_buf, &mut req_buf)
//! ```

use ring::{digest, hmac};
use velda_etp::s3::S3EtpConfig;

use crate::wire_baked::{BakedWire, format_amz_timestamps_bytes, format_hex64};

/// Pre-baked AWS S3 SigV4 signer with per-day `kSigning` key caching.
pub(crate) struct S3SigV4Signer<'a> {
    cfg: &'a S3EtpConfig,
    aws4_secret: Vec<u8>,
    cached_date: [u8; 8],
    cached_signing_key: Option<hmac::Key>,
}

impl<'a> S3SigV4Signer<'a> {
    pub(crate) fn new(cfg: &'a S3EtpConfig) -> Self {
        let mut aws4_secret = Vec::with_capacity(4 + cfg.secret_access_key.len());
        aws4_secret.extend_from_slice(b"AWS4");
        aws4_secret.extend_from_slice(cfg.secret_access_key.as_bytes());
        Self {
            cfg,
            aws4_secret,
            cached_date: [0u8; 8],
            cached_signing_key: None,
        }
    }

    fn signing_key_for_date(&mut self, date_yyyymmdd: &[u8; 8]) -> &hmac::Key {
        if &self.cached_date != date_yyyymmdd || self.cached_signing_key.is_none() {
            let k_secret = hmac::Key::new(hmac::HMAC_SHA256, &self.aws4_secret);
            let k_date = hmac::sign(&k_secret, date_yyyymmdd);
            let k_date_key = hmac::Key::new(hmac::HMAC_SHA256, k_date.as_ref());
            let k_region = hmac::sign(&k_date_key, self.cfg.region.as_bytes());
            let k_region_key = hmac::Key::new(hmac::HMAC_SHA256, k_region.as_ref());
            let k_service = hmac::sign(&k_region_key, b"s3");
            let k_service_key = hmac::Key::new(hmac::HMAC_SHA256, k_service.as_ref());
            let k_signing = hmac::sign(&k_service_key, b"aws4_request");

            self.cached_date = *date_yyyymmdd;
            self.cached_signing_key = Some(hmac::Key::new(hmac::HMAC_SHA256, k_signing.as_ref()));
        }
        self.cached_signing_key.as_ref().unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_signed_request(
        &mut self,
        method: &[u8],
        uri_path: &[u8],
        body: &[u8],
        if_match: Option<&str>,
        unix_secs: u64,
        creq_buf: &mut Vec<u8>,
        req_buf: &mut Vec<u8>,
    ) {
        let (date_stamp, amz_date) = format_amz_timestamps_bytes(unix_secs);
        let payload_digest = digest::digest(&digest::SHA256, body);
        let payload_arr: [u8; 32] = payload_digest.as_ref().try_into().unwrap();
        let mut payload_hex = [0u8; 64];
        format_hex64(&payload_arr, &mut payload_hex);

        let signed_hdrs: &[u8] = if self.cfg.session_token.is_some() {
            b"host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
        } else {
            b"host;x-amz-content-sha256;x-amz-date"
        };

        {
            let mut cw = BakedWire::cleared(creq_buf, 320);
            cw.push_chunk(method);
            cw.push_byte(b'\n');
            cw.push_chunk(uri_path);
            cw.push_chunk(b"\n\nhost:");
            cw.push_chunk(self.cfg.host_header.as_bytes());
            cw.push_chunk(b"\nx-amz-content-sha256:");
            cw.push_chunk(&payload_hex);
            cw.push_chunk(b"\nx-amz-date:");
            cw.push_chunk(&amz_date);
            if let Some(ref sts) = self.cfg.session_token {
                cw.push_chunk(b"\nx-amz-security-token:");
                cw.push_chunk(sts.as_bytes());
            }
            cw.push_chunk(b"\n\n");
            cw.push_chunk(signed_hdrs);
            cw.push_byte(b'\n');
            cw.push_chunk(&payload_hex);
        }
        let creq_digest = digest::digest(&digest::SHA256, creq_buf);
        let creq_arr: [u8; 32] = creq_digest.as_ref().try_into().unwrap();

        {
            let mut sw = BakedWire::cleared(creq_buf, 160);
            sw.push_chunk(b"AWS4-HMAC-SHA256\n");
            sw.push_chunk(&amz_date);
            sw.push_byte(b'\n');
            sw.push_chunk(&date_stamp);
            sw.push_byte(b'/');
            sw.push_chunk(self.cfg.region.as_bytes());
            sw.push_chunk(b"/s3/aws4_request\n");
            sw.push_hex64(&creq_arr);
        }

        let signing_key = self.signing_key_for_date(&date_stamp);
        let sig_tag = hmac::sign(signing_key, creq_buf);
        let sig_arr: [u8; 32] = sig_tag.as_ref().try_into().unwrap();

        let mut rw = BakedWire::cleared(req_buf, 512 + body.len());
        rw.push_chunk(method);
        rw.push_byte(b' ');
        rw.push_chunk(uri_path);
        rw.push_chunk(b" HTTP/1.1\r\nHost: ");
        rw.push_chunk(self.cfg.host_header.as_bytes());
        rw.push_chunk(b"\r\nx-amz-content-sha256: ");
        rw.push_chunk(&payload_hex);
        rw.push_chunk(b"\r\nx-amz-date: ");
        rw.push_chunk(&amz_date);
        if let Some(ref sts) = self.cfg.session_token {
            rw.push_chunk(b"\r\nx-amz-security-token: ");
            rw.push_chunk(sts.as_bytes());
        }
        if let Some(etag) = if_match {
            rw.push_chunk(b"\r\nIf-Match: ");
            rw.push_chunk(etag.as_bytes());
        }
        rw.push_chunk(b"\r\nAuthorization: AWS4-HMAC-SHA256 Credential=");
        rw.push_chunk(self.cfg.access_key_id.as_bytes());
        rw.push_byte(b'/');
        rw.push_chunk(&date_stamp);
        rw.push_byte(b'/');
        rw.push_chunk(self.cfg.region.as_bytes());
        rw.push_chunk(b"/s3/aws4_request, SignedHeaders=");
        rw.push_chunk(signed_hdrs);
        rw.push_chunk(b", Signature=");
        rw.push_hex64(&sig_arr);
        rw.push_chunk(b"\r\nConnection: keep-alive\r\nContent-Length: ");
        rw.push_u64(body.len() as u64);
        rw.push_chunk(b"\r\n\r\n");
        rw.push_chunk(body);
    }
}
