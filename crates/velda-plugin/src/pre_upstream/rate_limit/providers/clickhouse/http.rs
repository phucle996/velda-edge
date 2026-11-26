//! HTTP/1.1 auth header builder & response reader (`http.rs`) for ClickHouse `rate_limit`.
//!
//! ```text
//!   format_auth_headers() ──► X-ClickHouse-User / X-ClickHouse-Key
//!   read_http1_response() ──► header scan ("\r\n\r\n", <= 32 KiB) -> (status, body)
//! ```

use std::fmt::Write as _;
use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::clickhouse::ClickHouseEtpConfig;
use velda_etp::connect::EtpStream;

pub(crate) fn format_auth_headers(cfg: &ClickHouseEtpConfig) -> String {
    let mut auth = String::new();
    if let Some(u) = &cfg.user {
        let _ = write!(auth, "X-ClickHouse-User: {u}\r\n");
    }
    if let Some(p) = &cfg.password {
        let _ = write!(auth, "X-ClickHouse-Key: {p}\r\n");
    }
    auth
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
                "ClickHouse closed HTTP connection unexpectedly",
            )));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 32 * 1024 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "ClickHouse response headers exceeded 32 KiB",
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
