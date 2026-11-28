//! Upstream HTTP/1.1 Request Wire Serializer (RFC 9112).
//!
//! Architectural Perspective (Góc nhìn Ingress):
//! Handles serializing outgoing HTTP/1.1 requests to upstream backend microservices
//! over pre-established stream connections.

use bytes::{BufMut, BytesMut};
use http::header::CONTENT_LENGTH;
use http::{Method, Uri};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::{Body, HeaderWorkspace};

use crate::outcome::Http1Outcome;
use crate::server::request::Http1ServerRequestHead;

/// Encodes an HTTP/1.1 request line (method, path and query, version).
pub fn encode_request_line(method: &Method, uri: &Uri, dst: &mut BytesMut) {
    dst.put_slice(method.as_str().as_bytes());
    dst.put_slice(b" ");
    let path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or_else(|| uri.path());
    dst.put_slice(path_and_query.as_bytes());
    dst.put_slice(b" HTTP/1.1\r\n");
}

/// Serializes HTTP/1.1 request head metadata into the destination buffer.
#[inline]
pub fn encode_request_head(
    method: &Method,
    uri: &Uri,
    headers: &HeaderWorkspace,
    body_len: Option<usize>,
    dst: &mut BytesMut,
) {
    // Fast O(1) capacity estimation: avoids redundant pass over all header keys & values
    dst.reserve(128 + headers.len() * 48);

    encode_request_line(method, uri, dst);
    for (name, val) in headers.iter() {
        dst.put_slice(name.as_str().as_bytes());
        dst.put_slice(b": ");
        dst.put_slice(val.as_bytes());
        dst.put_slice(b"\r\n");
    }

    if let Some(len) = body_len
        && !headers.contains_key(CONTENT_LENGTH)
    {
        dst.put_slice(b"content-length: ");
        let mut itoa_buf = itoa::Buffer::new();
        dst.put_slice(itoa_buf.format(len).as_bytes());
        dst.put_slice(b"\r\n");
    }

    dst.put_slice(b"\r\n");
}

/// Serializes and writes request parts directly to upstream stream using vectored I/O.
///
/// Eliminates user-space copying of body payload into `write_buf` and coalesces headers + body into a single syscall.
pub async fn send_request_parts<IO>(
    method: &Method,
    uri: &Uri,
    headers: &HeaderWorkspace,
    body: &Body,
    stream: &mut IO,
    write_buf: &mut BytesMut,
) -> Result<(), Http1Outcome>
where
    IO: AsyncWrite + Unpin,
{
    let body_len = if body.is_empty() {
        None
    } else {
        Some(body.len())
    };
    write_buf.clear();
    encode_request_head(method, uri, headers, body_len, write_buf);

    if body.is_empty() {
        stream
            .write_all(write_buf)
            .await
            .map_err(Http1Outcome::Io)?;
    } else {
        let b_slice = body.as_slice();
        if b_slice.len() <= 65536 {
            write_buf.extend_from_slice(b_slice);
            stream
                .write_all(write_buf)
                .await
                .map_err(Http1Outcome::Io)?;
            return Ok(());
        }

        let mut h_slice = &write_buf[..];
        let mut b_slice = b_slice;
        while !h_slice.is_empty() || !b_slice.is_empty() {
            let mut io_slices = [
                std::io::IoSlice::new(h_slice),
                std::io::IoSlice::new(b_slice),
            ];
            let active = if h_slice.is_empty() {
                &mut io_slices[1..]
            } else if b_slice.is_empty() {
                &mut io_slices[..1]
            } else {
                &mut io_slices[..]
            };
            let n = stream
                .write_vectored(active)
                .await
                .map_err(Http1Outcome::Io)?;
            if n == 0 {
                return Err(Http1Outcome::Io(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "failed to write whole request",
                )));
            }
            if n <= h_slice.len() {
                h_slice = &h_slice[n..];
            } else {
                let rem = n - h_slice.len();
                h_slice = &[];
                b_slice = &b_slice[rem..];
            }
        }
    }
    Ok(())
}

/// Writes an RFC 9112 chunk to an HTTP/1.1 stream using vectored I/O without copying payload bytes.
pub async fn send_chunk_vectored<IO>(stream: &mut IO, chunk: &[u8]) -> Result<(), Http1Outcome>
where
    IO: AsyncWrite + Unpin,
{
    let mut header_buf = [0u8; 32];
    let header_len = crate::wire::format_hex_crlf(chunk.len(), &mut header_buf);

    let mut h_slice = &header_buf[..header_len];
    let mut c_slice = chunk;
    let mut t_slice = &b"\r\n"[..];

    while !h_slice.is_empty() || !c_slice.is_empty() || !t_slice.is_empty() {
        let mut io_slices = [
            std::io::IoSlice::new(h_slice),
            std::io::IoSlice::new(c_slice),
            std::io::IoSlice::new(t_slice),
        ];
        let active = if !h_slice.is_empty() {
            &mut io_slices[..]
        } else if !c_slice.is_empty() {
            &mut io_slices[1..]
        } else {
            &mut io_slices[2..]
        };
        let n = stream
            .write_vectored(active)
            .await
            .map_err(Http1Outcome::Io)?;
        if n == 0 {
            return Err(Http1Outcome::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole chunk",
            )));
        }
        if n <= h_slice.len() {
            h_slice = &h_slice[n..];
        } else {
            let rem = n - h_slice.len();
            h_slice = &[];
            if rem <= c_slice.len() {
                c_slice = &c_slice[rem..];
            } else {
                let rem2 = rem - c_slice.len();
                c_slice = &[];
                t_slice = &t_slice[rem2..];
            }
        }
    }
    Ok(())
}

/// Serializes an HTTP/1.1 request head configured for chunked streaming into provided write buffer.
#[inline]
pub fn encode_request_head_chunked(head: &Http1ServerRequestHead, write_buf: &mut BytesMut) {
    write_buf.clear();
    write_buf.reserve(128 + head.headers.len() * 48);
    encode_request_line(&head.method, &head.uri, write_buf);

    let mut has_te = false;
    for (name, val) in head.headers.iter() {
        if *name == http::header::CONTENT_LENGTH {
            continue;
        }
        if *name == http::header::TRANSFER_ENCODING {
            has_te = true;
        }
        write_buf.put_slice(name.as_str().as_bytes());
        write_buf.put_slice(b": ");
        write_buf.put_slice(val.as_bytes());
        write_buf.put_slice(b"\r\n");
    }
    if !has_te {
        write_buf.put_slice(b"transfer-encoding: chunked\r\n");
    }
    write_buf.put_slice(b"\r\n");
}
