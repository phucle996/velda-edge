//! HTTP/1.1 auth header builder & response reader (`http.rs`) for ClickHouse `rate_limit`.
//!
//! ```text
//!   format_auth_headers() ──► X-ClickHouse-User / X-ClickHouse-Key
//!   read_http1_response() ──► header scan ("\r\n\r\n", <= 32 KiB)
//!                               ├─► Transfer-Encoding: chunked ──► decode_chunked()
//!                               └─► Content-Length             ──► exact byte read
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
        _ => {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Malformed HTTP/1.1 response from ClickHouse",
            )));
        }
    };

    let is_chunked = parsed.headers.iter().any(|h| {
        h.name.eq_ignore_ascii_case("transfer-encoding")
            && std::str::from_utf8(h.value)
                .is_ok_and(|v| v.to_ascii_lowercase().contains("chunked"))
    });
    if is_chunked {
        loop {
            if let Some(decoded) = try_decode_chunked(&buf[header_end..])? {
                return Ok((status, decoded));
            }
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Err(EtpError::Io(Error::new(
                    ErrorKind::UnexpectedEof,
                    "ClickHouse chunked stream ended prematurely",
                )));
            }
            buf.extend_from_slice(&tmp[..n]);
        }
    }

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

fn try_decode_chunked(data: &[u8]) -> Result<Option<Vec<u8>>, EtpError> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    loop {
        let Some(rel_crlf) = data[cursor..].windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let size_line = std::str::from_utf8(&data[cursor..cursor + rel_crlf]).map_err(|_| {
            EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Invalid ClickHouse chunk size UTF-8",
            ))
        })?;
        let hex = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(hex, 16).map_err(|_| {
            EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid ClickHouse chunk size '{hex}'"),
            ))
        })?;
        cursor += rel_crlf + 2;
        if chunk_len == 0 {
            if data[cursor..].len() < 2 {
                return Ok(None);
            }
            return Ok(Some(out));
        }
        if data[cursor..].len() < chunk_len + 2 {
            return Ok(None);
        }
        out.extend_from_slice(&data[cursor..cursor + chunk_len]);
        cursor += chunk_len + 2;
    }
}

#[cfg(test)]
mod tests {
    use super::try_decode_chunked;

    #[test]
    fn test_try_decode_chunked_complete_and_partial() {
        assert_eq!(try_decode_chunked(b"0\r\n").unwrap(), None);
        assert_eq!(try_decode_chunked(b"0\r\n\r\n").unwrap(), Some(Vec::new()));
        assert_eq!(
            try_decode_chunked(b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n").unwrap(),
            Some(b"hello world".to_vec())
        );
    }
}
