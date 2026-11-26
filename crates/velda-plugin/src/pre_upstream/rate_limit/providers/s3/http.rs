//! HTTP/1.1 response + ETag header parser (`http.rs`) for `rate_limit` S3 client.
//!
//! ```text
//!   EtpStream ──► header scan ("\r\n\r\n", <= 32 KiB) ──► (status, Option<ETag>, body)
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

pub(crate) async fn read_http1_response(
    stream: &mut EtpStream,
    buf: &mut Vec<u8>,
) -> Result<(u16, Option<String>, Vec<u8>), EtpError> {
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
                "S3 closed HTTP connection unexpectedly",
            )));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 32 * 1024 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "S3 response headers exceeded 32 KiB",
            )));
        }
    };

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    let status = match parsed.parse(&buf[..header_end]) {
        Ok(httparse::Status::Complete(_)) => parsed.code.unwrap_or(500),
        _ => 500,
    };
    let etag = parsed
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("etag"))
        .and_then(|h| std::str::from_utf8(h.value).ok())
        .map(|s| s.trim().to_string());
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
        etag,
        buf[header_end..(header_end + content_len).min(buf.len())].to_vec(),
    ))
}
