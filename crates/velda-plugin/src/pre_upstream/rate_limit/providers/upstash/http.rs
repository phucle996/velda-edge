//! HTTP/1.1 wire response parser (`http.rs`) for Upstash REST `/pipeline` in `rate_limit`.
//!
//! ```text
//!   EtpStream ──► header scan ("\r\n\r\n", <= 32 KiB)
//!                   ├─► Transfer-Encoding: chunked ──► try_decode_chunked()
//!                   └─► Content-Length             ──► exact byte read
//! ```

use std::io::{Error, ErrorKind};

use velda_etp::EtpError;
use velda_etp::connect::EtpStream;

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
                "Upstash closed HTTP connection unexpectedly",
            )));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 32 * 1024 {
            return Err(EtpError::Io(Error::new(
                ErrorKind::InvalidData,
                "Upstash response headers exceeded 32 KiB",
            )));
        }
    };

    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut parsed = httparse::Response::new(&mut headers);
    let status = match parsed.parse(&buf[..header_end]) {
        Ok(httparse::Status::Complete(_)) => parsed.code.unwrap_or(500),
        _ => 500,
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
                    "Upstash chunked stream ended prematurely",
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
                "Invalid chunk size UTF-8",
            ))
        })?;
        let hex = size_line.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(hex, 16).map_err(|_| {
            EtpError::Io(Error::new(ErrorKind::InvalidData, "Invalid hex chunk size"))
        })?;
        cursor += rel_crlf + 2;
        if chunk_len == 0 {
            if data.len() < cursor + 2 {
                return Ok(None);
            }
            return Ok(Some(out));
        }
        if data.len() < cursor + chunk_len + 2 {
            return Ok(None);
        }
        out.extend_from_slice(&data[cursor..cursor + chunk_len]);
        cursor += chunk_len + 2;
    }
}
