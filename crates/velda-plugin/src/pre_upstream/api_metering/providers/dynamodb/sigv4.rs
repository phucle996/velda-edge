//! AWS DynamoDB SigV4 request signer (with daily `kSigning` cache + SIMD hex/chunk assembly)
//! and HTTP/1.1 reader (`sigv4.rs`) for `api_metering`.
//!
//! ```text
//!   DynamoDbSigV4Signer::new(&etp.config)
//!     ├── pre-bakes AWS4{secret} + caches kSigning per UTC day
//!     └── write_signed_request(target, body, unix_secs, &mut creq_buf, &mut req_buf)
//!           └── BakedWire chunks + SIMD format_hex64 payload/signature + HTTP/1.1 head
//! ```

use std::io::{Error, ErrorKind};

use ring::{digest, hmac};
use velda_etp::EtpError;
use velda_etp::connect::EtpStream;
use velda_etp::dynamodb::DynamoDbEtpConfig;

use crate::wire_baked::{BakedWire, format_amz_timestamps_bytes, format_hex64};

/// Pre-baked AWS DynamoDB SigV4 signer with per-day `kSigning` cache.
pub(crate) struct DynamoDbSigV4Signer<'a> {
    cfg: &'a DynamoDbEtpConfig,
    aws4_secret: Vec<u8>,
    cached_date: [u8; 8],
    cached_signing_key: Option<hmac::Key>,
}

impl<'a> DynamoDbSigV4Signer<'a> {
    pub(crate) fn new(cfg: &'a DynamoDbEtpConfig) -> Self {
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
            let k_service = hmac::sign(&k_region_key, b"dynamodb");
            let k_service_key = hmac::Key::new(hmac::HMAC_SHA256, k_service.as_ref());
            let k_signing = hmac::sign(&k_service_key, b"aws4_request");

            self.cached_date = *date_yyyymmdd;
            self.cached_signing_key = Some(hmac::Key::new(hmac::HMAC_SHA256, k_signing.as_ref()));
        }
        self.cached_signing_key.as_ref().unwrap()
    }

    /// Writes the complete HTTP/1.1 DynamoDB request (headers + body) into `req_buf`.
    pub(crate) fn write_signed_request(
        &mut self,
        target: &[u8],
        body: &[u8],
        unix_secs: u64,
        creq_buf: &mut Vec<u8>,
        req_buf: &mut Vec<u8>,
    ) {
        if self.cfg.access_key_id.is_empty() || self.cfg.secret_access_key.is_empty() {
            let mut rw = BakedWire::cleared(req_buf, 256 + body.len());
            rw.push_chunk(b"POST / HTTP/1.1\r\nHost: ");
            rw.push_chunk(self.cfg.host_header.as_bytes());
            rw.push_chunk(b"\r\nContent-Type: application/x-amz-json-1.0\r\nX-Amz-Target: ");
            rw.push_chunk(target);
            rw.push_chunk(b"\r\nConnection: keep-alive\r\nContent-Length: ");
            rw.push_u64(body.len() as u64);
            rw.push_chunk(b"\r\n\r\n");
            rw.push_chunk(body);
            return;
        }

        let (date_stamp, amz_date) = format_amz_timestamps_bytes(unix_secs);
        let payload_digest = digest::digest(&digest::SHA256, body);
        let payload_arr: [u8; 32] = payload_digest.as_ref().try_into().unwrap();
        let mut payload_hex = [0u8; 64];
        format_hex64(&payload_arr, &mut payload_hex);

        let signed_hdrs: &[u8] = if self.cfg.session_token.is_some() {
            b"content-type;host;x-amz-date;x-amz-security-token;x-amz-target"
        } else {
            b"content-type;host;x-amz-date;x-amz-target"
        };

        // 1. Canonical request into creq_buf
        {
            let mut cw = BakedWire::cleared(creq_buf, 320);
            cw.push_chunk(b"POST\n/\n\ncontent-type:application/x-amz-json-1.0\nhost:");
            cw.push_chunk(self.cfg.host_header.as_bytes());
            cw.push_chunk(b"\nx-amz-date:");
            cw.push_chunk(&amz_date);
            if let Some(ref sts) = self.cfg.session_token {
                cw.push_chunk(b"\nx-amz-security-token:");
                cw.push_chunk(sts.as_bytes());
            }
            cw.push_chunk(b"\nx-amz-target:");
            cw.push_chunk(target);
            cw.push_chunk(b"\n\n");
            cw.push_chunk(signed_hdrs);
            cw.push_byte(b'\n');
            cw.push_chunk(&payload_hex);
        }
        let creq_digest = digest::digest(&digest::SHA256, creq_buf);
        let creq_arr: [u8; 32] = creq_digest.as_ref().try_into().unwrap();

        // 2. String-to-sign reusing creq_buf
        {
            let mut sw = BakedWire::cleared(creq_buf, 160);
            sw.push_chunk(b"AWS4-HMAC-SHA256\n");
            sw.push_chunk(&amz_date);
            sw.push_byte(b'\n');
            sw.push_chunk(&date_stamp);
            sw.push_byte(b'/');
            sw.push_chunk(self.cfg.region.as_bytes());
            sw.push_chunk(b"/dynamodb/aws4_request\n");
            sw.push_hex64(&creq_arr);
        }

        let signing_key = self.signing_key_for_date(&date_stamp);
        let sig_tag = hmac::sign(signing_key, creq_buf);
        let sig_arr: [u8; 32] = sig_tag.as_ref().try_into().unwrap();

        // 3. Complete HTTP/1.1 wire request into req_buf
        let mut rw = BakedWire::cleared(req_buf, 512 + body.len());
        rw.push_chunk(b"POST / HTTP/1.1\r\nHost: ");
        rw.push_chunk(self.cfg.host_header.as_bytes());
        rw.push_chunk(b"\r\nContent-Type: application/x-amz-json-1.0\r\nx-amz-date: ");
        rw.push_chunk(&amz_date);
        if let Some(ref sts) = self.cfg.session_token {
            rw.push_chunk(b"\r\nx-amz-security-token: ");
            rw.push_chunk(sts.as_bytes());
        }
        rw.push_chunk(b"\r\nx-amz-target: ");
        rw.push_chunk(target);
        rw.push_chunk(b"\r\nAuthorization: AWS4-HMAC-SHA256 Credential=");
        rw.push_chunk(self.cfg.access_key_id.as_bytes());
        rw.push_byte(b'/');
        rw.push_chunk(&date_stamp);
        rw.push_byte(b'/');
        rw.push_chunk(self.cfg.region.as_bytes());
        rw.push_chunk(b"/dynamodb/aws4_request, SignedHeaders=");
        rw.push_chunk(signed_hdrs);
        rw.push_chunk(b", Signature=");
        rw.push_hex64(&sig_arr);
        rw.push_chunk(b"\r\nConnection: keep-alive\r\nContent-Length: ");
        rw.push_u64(body.len() as u64);
        rw.push_chunk(b"\r\n\r\n");
        rw.push_chunk(body);
    }
}

pub(crate) async fn read_http1_response(
    stream: &mut EtpStream,
    buf: &mut Vec<u8>,
) -> Result<(u16, Vec<u8>), EtpError> {
    buf.clear();
    let mut tmp = [0u8; 1024];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::UnexpectedEof,
                "DynamoDB closed HTTP connection",
            )));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 32 * 1024 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "DynamoDB response headers > 32 KiB",
            )));
        }
    };

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    let status = match parsed.parse(&buf[..header_end]) {
        Ok(httparse::Status::Complete(_)) => parsed.code.unwrap_or(500),
        _ => 500,
    };
    let content_len = parsed
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"))
        .and_then(|h| std::str::from_utf8(h.value).ok())
        .and_then(|s| s.trim().parse::<usize>().ok())
        .unwrap_or(0);

    while buf.len() < header_end + content_len {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    Ok((
        status,
        buf[header_end..(header_end + content_len).min(buf.len())].to_vec(),
    ))
}
