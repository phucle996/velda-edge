//! Upstream HTTP/1.1 client connector for `velda-upstream` and connection pool.

use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use velda_core::{L7Request, L7Response};

use crate::codec::{decode_response, encode_request};
use crate::error::Http1Error;

/// Active HTTP/1.1 upstream connector.
pub struct Http1UpstreamConnector;

impl Http1UpstreamConnector {
    /// Forwards an HTTP/1.1 L7Request to the target backend endpoint over cleartext TCP,
    /// returning the parsed L7Response.
    pub async fn forward_request(
        req: &L7Request,
        target: SocketAddr,
    ) -> Result<L7Response, Http1Error> {
        let mut stream = TcpStream::connect(target).await?;

        let mut write_buf = BytesMut::with_capacity(1024 + req.body.len());
        encode_request(req, &mut write_buf);

        stream.write_all(&write_buf).await?;

        let mut read_buf = BytesMut::with_capacity(4096);

        loop {
            let n = stream.read_buf(&mut read_buf).await?;
            if n == 0 {
                if let Some(resp) = decode_response(&mut read_buf)? {
                    return Ok(resp);
                }
                return Err(Http1Error::ConnectionClosed);
            }

            if let Some(resp) = decode_response(&mut read_buf)? {
                return Ok(resp);
            }
        }
    }
}
