//! RFC 9112 Wire Framing & Chunked Transfer Coding Utilities.
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Low-level byte-level wire parsing and serialization strictly defined by RFC 9112.
//! Protocol-generic between Downstream Ingress and Upstream Ingress, eliminating
//! redundant duplicate implementations across `server` and `client`.

use bytes::{BufMut, BytesMut};
use http::HeaderMap;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::Body;

use crate::error::Http1Error;

#[cold]
#[inline(never)]
pub fn cold_parse_error(msg: impl Into<String>) -> Http1Error {
    Http1Error::Parse(msg.into())
}

#[cold]
#[inline(never)]
pub fn cold_chunked_error(msg: &'static str) -> Http1Error {
    Http1Error::InvalidChunkedEncoding(msg.into())
}

#[cold]
#[inline(never)]
pub fn cold_smuggling_error(msg: &'static str) -> Http1Error {
    Http1Error::SmugglingDetected(msg.into())
}

/// Fast SIMD-accelerated search for CRLF (`\r\n`) in byte slice.
#[inline]
pub fn find_crlf(data: &[u8]) -> Option<usize> {
    memchr::memmem::find(data, b"\r\n")
}

/// Parses hex chunk size directly from ASCII byte slice without UTF-8 or String allocations.
///
/// Strictly adheres to RFC 9112 Section 7.1 (`chunk-size = 1*HEXDIG`).
/// Leading and trailing whitespace before chunk extensions are trimmed, but
/// whitespace between digits is strictly rejected to prevent smuggling evasion.
#[inline]
pub fn parse_hex_usize(bytes: &[u8]) -> Result<usize, Http1Error> {
    let mut start = 0;
    while start < bytes.len() && (bytes[start] == b' ' || bytes[start] == b'\t') {
        start += 1;
    }
    let mut end = bytes.len();
    while end > start && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\t') {
        end -= 1;
    }
    let trimmed = &bytes[start..end];
    if trimmed.is_empty() {
        return Err(cold_chunked_error("Empty chunk size line"));
    }

    let mut val: usize = 0;
    for &b in trimmed {
        let digit = match b {
            b'0'..=b'9' => (b - b'0') as usize,
            b'a'..=b'f' => (b - b'a' + 10) as usize,
            b'A'..=b'F' => (b - b'A' + 10) as usize,
            _ => {
                return Err(cold_chunked_error("Invalid hex chunk size character"));
            }
        };
        val = val
            .checked_shl(4)
            .and_then(|v| v.checked_add(digit))
            .ok_or_else(|| cold_chunked_error("Chunk size integer overflow"))?;
    }
    Ok(val)
}

/// Parses ASCII decimal digits directly from byte slice without UTF-8 validation or allocations.
///
/// Leading and trailing whitespace are trimmed, but inner whitespace is rejected.
#[inline]
pub fn parse_ascii_digits(bytes: &[u8]) -> Result<usize, Http1Error> {
    let mut start = 0;
    while start < bytes.len() && (bytes[start] == b' ' || bytes[start] == b'\t') {
        start += 1;
    }
    let mut end = bytes.len();
    while end > start && (bytes[end - 1] == b' ' || bytes[end - 1] == b'\t') {
        end -= 1;
    }
    let trimmed = &bytes[start..end];
    if trimmed.is_empty() {
        return Err(cold_parse_error("Empty Content-Length header"));
    }

    let mut val: usize = 0;
    for &b in trimmed {
        if !b.is_ascii_digit() {
            return Err(cold_parse_error("Invalid non-digit in Content-Length"));
        }
        val = val
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as usize))
            .ok_or_else(|| cold_parse_error("Content-Length integer overflow"))?;
    }
    Ok(val)
}

/// Parses a chunked HTTP body from a byte slice with memoized chunk offsets.
pub fn parse_chunked_body(
    data: &[u8],
    max_body_size: usize,
) -> Result<Option<(usize, Body)>, Http1Error> {
    let mut offset = 0;
    let mut total_body_len = 0;
    let mut memoized_chunks = [(0usize, 0usize); 16];
    let mut chunk_count = 0;
    let mut overflowed_chunks = false;

    // First pass: validate framing, check limits, and memoize chunk ranges on stack
    loop {
        let remaining = &data[offset..];
        let Some(crlf_pos) = find_crlf(remaining) else {
            return Ok(None);
        };
        let line = &remaining[..crlf_pos];
        let size_part = match memchr::memchr(b';', line) {
            Some(semi) => &line[..semi],
            None => line,
        };
        let chunk_size = parse_hex_usize(size_part)?;
        offset += crlf_pos + 2;

        if chunk_size == 0 {
            let trailer_data = &data[offset..];
            if trailer_data.starts_with(b"\r\n") {
                offset += 2;
                break;
            }
            if let Some(pos) = memchr::memmem::find(trailer_data, b"\r\n\r\n") {
                offset += pos + 4;
                break;
            }
            return Ok(None);
        }

        total_body_len += chunk_size;
        if total_body_len > max_body_size {
            return Err(Http1Error::PayloadTooLarge(total_body_len));
        }

        if data[offset..].len() < chunk_size + 2 {
            return Ok(None);
        }

        if &data[offset + chunk_size..offset + chunk_size + 2] != b"\r\n" {
            return Err(Http1Error::InvalidChunkedEncoding(
                "Missing CRLF after chunk data".into(),
            ));
        }

        if chunk_count < 16 {
            memoized_chunks[chunk_count] = (offset, chunk_size);
            chunk_count += 1;
        } else {
            overflowed_chunks = true;
        }

        offset += chunk_size + 2;
    }

    if total_body_len == 0 {
        return Ok(Some((offset, Body::Empty)));
    }

    let mut body_bytes = BytesMut::with_capacity(total_body_len);
    if !overflowed_chunks {
        for &(start, len) in &memoized_chunks[..chunk_count] {
            body_bytes.extend_from_slice(&data[start..start + len]);
        }
    } else {
        let mut read_offset = 0;
        loop {
            let remaining = &data[read_offset..];
            let crlf_pos = find_crlf(remaining).unwrap();
            let line = &remaining[..crlf_pos];
            let size_part = match memchr::memchr(b';', line) {
                Some(semi) => &line[..semi],
                None => line,
            };
            let chunk_size = parse_hex_usize(size_part).unwrap();
            read_offset += crlf_pos + 2;
            if chunk_size == 0 {
                break;
            }
            body_bytes.extend_from_slice(&data[read_offset..read_offset + chunk_size]);
            read_offset += chunk_size + 2;
        }
    }

    Ok(Some((offset, Body::Bytes(body_bytes.freeze()))))
}

/// Parsed single progressive chunk item: `(total_wire_len, chunk_payload, is_terminal)`.
pub type ParsedChunk<'a> = (usize, &'a [u8], bool);

/// Parses a single progressive chunk from an immutable byte slice.
///
/// Returns:
/// - `Ok(Some((total_wire_len, chunk_payload, is_terminal)))`:
///   A complete chunk was parsed. `total_wire_len` is the number of wire bytes
///   consumed (including chunk-size line, CRLF, payload, and trailing CRLF).
///   If `is_terminal` is true (chunk size == 0), this is the terminal chunk.
/// - `Ok(None)`: Not enough bytes in `data` to form a complete chunk.
/// - `Err(Http1Error)`: Invalid chunked framing or encoding error.
pub fn parse_single_chunk(data: &[u8]) -> Result<Option<ParsedChunk<'_>>, Http1Error> {
    if data.is_empty() {
        return Ok(None);
    }
    let Some(crlf_pos) = find_crlf(data) else {
        return Ok(None);
    };
    let line = &data[..crlf_pos];
    let size_part = match memchr::memchr(b';', line) {
        Some(semi) => &line[..semi],
        None => line,
    };
    let chunk_size = parse_hex_usize(size_part)?;
    let header_len = crlf_pos + 2;

    if chunk_size == 0 {
        let trailer_data = &data[header_len..];
        if trailer_data.starts_with(b"\r\n") {
            return Ok(Some((header_len + 2, &[], true)));
        }
        if let Some(pos) = memchr::memmem::find(trailer_data, b"\r\n\r\n") {
            return Ok(Some((header_len + pos + 4, &[], true)));
        }
        return Ok(None);
    }

    let needed = header_len + chunk_size + 2;
    if data.len() < needed {
        return Ok(None);
    }

    if &data[header_len + chunk_size..needed] != b"\r\n" {
        return Err(Http1Error::InvalidChunkedEncoding(
            "Missing CRLF after chunk data".into(),
        ));
    }

    let payload = &data[header_len..header_len + chunk_size];
    Ok(Some((needed, payload, false)))
}

/// Encodes HTTP headers into the destination buffer with a single capacity reservation.
#[inline]
pub fn encode_headers(headers: &HeaderMap, dst: &mut BytesMut) {
    if headers.is_empty() {
        return;
    }
    let mut total_len = 0;
    for (name, val) in headers {
        total_len += name.as_str().len() + val.as_bytes().len() + 4;
    }
    dst.reserve(total_len);

    for (name, val) in headers {
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }
}

/// Encodes a single chunk into `dst` formatted as `<hex_len>\r\n<data>\r\n`.
#[inline]
pub fn encode_chunk(chunk: &[u8], dst: &mut BytesMut) {
    if chunk.is_empty() {
        return;
    }
    use std::io::Write;
    let mut hex_buf = [0u8; 16];
    let mut cursor = std::io::Cursor::new(&mut hex_buf[..]);
    let _ = write!(cursor, "{:X}\r\n", chunk.len());
    let len = cursor.position() as usize;
    dst.put_slice(&hex_buf[..len]);
    dst.extend_from_slice(chunk);
    dst.put_slice(b"\r\n");
}

/// Encodes the terminal zero chunk (`0\r\n\r\n`) into `dst`.
#[inline]
pub fn encode_chunked_end(dst: &mut BytesMut) {
    dst.put_slice(b"0\r\n\r\n");
}

/// Writes a chunk of body bytes formatted as chunked transfer coding to the stream.
pub async fn send_chunk<W>(stream: &mut W, chunk: &[u8]) -> Result<(), Http1Error>
where
    W: AsyncWrite + Unpin,
{
    if chunk.is_empty() {
        return Ok(());
    }
    use std::io::Write;
    let mut header = [0u8; 32];
    let mut cursor = std::io::Cursor::new(&mut header[..]);
    write!(cursor, "{:X}\r\n", chunk.len()).map_err(Http1Error::Io)?;
    let header_len = cursor.position() as usize;

    stream.write_all(&header[..header_len]).await?;
    stream.write_all(chunk).await?;
    stream.write_all(b"\r\n").await?;
    stream.flush().await?;
    Ok(())
}

/// Writes the terminal zero chunk (`0\r\n\r\n`) to terminate a chunked stream.
pub async fn send_chunked_end<W>(stream: &mut W) -> Result<(), Http1Error>
where
    W: AsyncWrite + Unpin,
{
    stream.write_all(b"0\r\n\r\n").await?;
    stream.flush().await?;
    Ok(())
}
