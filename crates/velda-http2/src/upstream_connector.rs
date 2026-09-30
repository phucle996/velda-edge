//! Upstream HTTP/2 client connector for `velda-upstream` and connection pool.

use bytes::BytesMut;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use velda_core::{Body, IngressLimits, L7Request, L7Response};

use crate::error::Http2Error;

/// Active HTTP/2 upstream connector.
pub struct Http2UpstreamConnector;

impl Http2UpstreamConnector {
    /// Forwards an HTTP/2 L7Request to the target backend endpoint over cleartext TCP / H2,
    /// returning the parsed L7Response. Enforces `max_body_size` from the configured
    /// [`IngressLimits`] on the upstream response body.
    pub async fn forward_request(
        req: &L7Request,
        target: SocketAddr,
        limits: &IngressLimits,
    ) -> Result<L7Response, Http2Error> {
        let stream = TcpStream::connect(target).await?;
        let (mut client, h2_conn) = h2::client::handshake(stream).await?;

        tokio::spawn(async move {
            let _ = h2_conn.await;
        });

        let mut request = http::Request::builder()
            .method(req.method.clone())
            .uri(req.uri.clone())
            .version(http::Version::HTTP_2);

        for (k, v) in &req.headers {
            request = request.header(k, v);
        }

        let is_empty_body = !req.has_body();
        let http_req = request
            .body(())
            .map_err(|e| Http2Error::Parse(e.to_string()))?;

        let (response, mut send_stream) = client.send_request(http_req, is_empty_body)?;

        if let Body::Bytes(ref b) = req.body
            && !b.is_empty()
        {
            send_stream.send_data(b.clone(), true)?;
        }

        let (parts, mut body_stream) = response.await?.into_parts();
        let max_body = limits.max_body_size;
        let mut body_buf = BytesMut::new();

        while let Some(chunk) = body_stream.data().await {
            let data = chunk?;
            body_buf.extend_from_slice(&data);
            let _ = body_stream.flow_control().release_capacity(data.len());
            if body_buf.len() > max_body {
                return Err(Http2Error::PayloadTooLarge(body_buf.len()));
            }
        }

        let resp_body = if body_buf.is_empty() {
            Body::Empty
        } else {
            Body::Bytes(body_buf.freeze())
        };

        Ok(L7Response::new(
            parts.status,
            http::Version::HTTP_2,
            parts.headers,
            resp_body,
        ))
    }
}
