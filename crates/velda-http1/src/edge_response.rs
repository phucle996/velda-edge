//! Layer 7 HTTP/1.1 response serialization and egress writing.
//!
//! Operates on borrowed `&L7Response` references without holding or retaining
//! response business data inside the protocol engine.

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use velda_core::L7Response;

pub use crate::codec::encode_response;
use crate::error::Http1Error;

/// Serializes and writes an HTTP/1.1 response to the downstream stream.
///
/// Pure behavior: caller provides stream and response reference. Returns `true`
/// if `Connection: close` was requested, signaling the caller to close the stream.
pub async fn send_response<W>(
    stream: &mut W,
    write_buf: &mut BytesMut,
    response: &L7Response,
) -> Result<bool, Http1Error>
where
    W: AsyncWrite + Unpin,
{
    write_buf.clear();
    encode_response(response, write_buf);

    stream.write_all(write_buf).await?;
    stream.flush().await?;

    let close = response
        .headers
        .get(http::header::CONNECTION)
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.eq_ignore_ascii_case("close"));

    Ok(close)
}
