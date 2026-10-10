//! HTTP/2 Downstream Ingress Request Entity (RFC 9113 & RFC 9218).
//!
//! Represents incoming requests initiated by downstream clients across multiplexed streams.

use http::{HeaderMap, Method, Uri, Version};
use std::sync::Arc;
use velda_core::{Body, L7Request};

use super::connection::Http2FloodTracker;

/// Extensible HTTP Prioritization Scheme (RFC 9218).
///
/// Compact 2-byte representation (`#[repr(C)]`) representing stream urgency (`0..=7`)
/// and incremental concurrency preference. Default urgency is 3, incremental is false.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Http2Priority {
    /// Urgency level between 0 (highest) and 7 (lowest). Default: 3.
    pub urgency: u8,
    /// Whether incremental delivery is preferred (concurrency over serialization).
    pub incremental: bool,
}

impl Default for Http2Priority {
    #[inline]
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Http2Priority {
    /// Default HTTP priority according to RFC 9218 (urgency = 3, incremental = false).
    pub const DEFAULT: Self = Self {
        urgency: 3,
        incremental: false,
    };

    /// Parses an RFC 9218 `Priority` header value directly from raw byte slice without heap allocation.
    ///
    /// Performs an in-place linear scan with zero heap allocations and branch-friendly parsing.
    #[inline]
    pub fn from_header_bytes(bytes: &[u8]) -> Self {
        let mut urgency = 3u8;
        let mut incremental = false;

        let mut i = 0;
        let len = bytes.len();

        while i < len {
            // Skip leading whitespace / delimiters
            while i < len && (bytes[i] == b' ' || bytes[i] == b'\t' || bytes[i] == b',') {
                i += 1;
            }
            if i >= len {
                break;
            }

            // Read parameter key
            let key_start = i;
            while i < len
                && bytes[i] != b'='
                && bytes[i] != b','
                && bytes[i] != b' '
                && bytes[i] != b'\t'
            {
                i += 1;
            }
            let key = &bytes[key_start..i];

            // Skip whitespace after key
            while i < len && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }

            let mut has_val = false;
            if i < len && bytes[i] == b'=' {
                i += 1; // skip '='
                while i < len && (bytes[i] == b' ' || bytes[i] == b'\t') {
                    i += 1;
                }
                has_val = true;
            }

            if key == b"u" {
                if has_val && i < len {
                    let c = bytes[i];
                    if c.is_ascii_digit() {
                        let val = c - b'0';
                        if val <= 7 {
                            urgency = val;
                        }
                    }
                }
            } else if key == b"i" {
                if !has_val {
                    // Boolean parameter without value in Structured Fields means true
                    incremental = true;
                } else if i < len {
                    if bytes[i] == b'?' && i + 1 < len {
                        incremental = bytes[i + 1] == b'1';
                    } else if bytes[i] == b'1' {
                        incremental = true;
                    } else if bytes[i] == b'0' {
                        incremental = false;
                    }
                }
            }

            // Advance until next comma delimiter or end of input
            while i < len && bytes[i] != b',' {
                i += 1;
            }
            if i < len && bytes[i] == b',' {
                i += 1;
            }
        }

        Self {
            urgency,
            incremental,
        }
    }
}

/// Header metadata and stream identity for an incoming HTTP/2 server request.
#[derive(Debug, Clone)]
pub struct Http2ServerRequestHead {
    /// HTTP method (GET, POST, PUT, etc.).
    pub method: Method,
    /// Target URI / path.
    pub uri: Uri,
    /// Protocol version (always HTTP/2.0).
    pub version: Version,
    /// Decompressed headers received in HEADERS frame(s).
    pub headers: HeaderMap,
    /// Logical HTTP/2 stream identifier.
    pub stream_id: Option<h2::StreamId>,
    /// RFC 9218 extensible HTTP stream priority parsed in-place.
    pub priority: Http2Priority,
}

impl Http2ServerRequestHead {
    /// Creates a new [`Http2ServerRequestHead`].
    pub fn new(
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        stream_id: Option<h2::StreamId>,
    ) -> Self {
        let priority = headers
            .get("priority")
            .map(|val| Http2Priority::from_header_bytes(val.as_bytes()))
            .unwrap_or(Http2Priority::DEFAULT);

        Self {
            method,
            uri,
            version: Version::HTTP_2,
            headers,
            stream_id,
            priority,
        }
    }

    /// Enriches HTTP/2 request headers with RFC 7239 and standard proxy forwarding metadata.
    #[inline]
    pub fn enrich_forwarded_headers(
        &mut self,
        peer: std::net::SocketAddr,
        local_addr: std::net::SocketAddr,
        is_tls: bool,
    ) {
        super::header::enrich_headers(&mut self.headers, &self.uri, peer, local_addr, is_tls);
    }

    /// Returns the approximate wire size of request method, URI path, and headers.
    #[inline]
    pub fn estimated_header_bytes(&self) -> usize {
        let headers_len: usize = self
            .headers
            .iter()
            .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 4)
            .sum();
        headers_len + self.uri.path().len() + self.method.as_str().len() + 32
    }

    /// Converts this server request head into an outbound upstream [`http::Request<()>`] zero-copy by moving fields.
    #[inline]
    pub fn into_http_request(self) -> http::Request<()> {
        let mut req = http::Request::new(());
        *req.method_mut() = self.method;
        *req.uri_mut() = self.uri;
        *req.version_mut() = Version::HTTP_2;
        *req.headers_mut() = self.headers;
        req
    }
}

/// An incoming HTTP/2 server request received on an active multiplexed stream.
#[derive(Debug, Clone)]
pub struct Http2ServerRequest {
    /// Request head (method, URI, headers, stream ID).
    pub head: Http2ServerRequestHead,
    /// Request body payload received via DATA frame(s).
    pub body: Body,
}

impl Http2ServerRequest {
    /// Creates a new [`Http2ServerRequest`] from parts.
    #[inline]
    pub fn new(head: Http2ServerRequestHead, body: Body) -> Self {
        Self { head, body }
    }

    /// Deconstructs the request into its constituent head and body parts.
    #[inline]
    pub fn into_parts(self) -> (Http2ServerRequestHead, Body) {
        (self.head, self.body)
    }

    /// Converts this protocol-owned request into canonical [`L7Request`].
    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(
            self.head.method,
            self.head.uri,
            Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }

    /// Constructs an [`Http2ServerRequest`] from a canonical [`L7Request`].
    pub fn from_l7_request(req: L7Request) -> Self {
        let head = Http2ServerRequestHead::new(req.method, req.uri, req.headers, None);
        Self::new(head, req.body)
    }
}

/// Progressive stream receiver for an incoming HTTP/2 request or response body.
///
/// Reads DATA chunks incrementally with batched flow-control (`release_capacity`)
/// and maximum payload size limits without buffering the entire body into RAM.
#[derive(Debug)]
pub struct Http2StreamReceiver {
    body_stream: h2::RecvStream,
    max_body_size: usize,
    bytes_received: usize,
    unreleased_bytes: usize,
    flood_tracker: Option<Arc<Http2FloodTracker>>,
}

impl Drop for Http2StreamReceiver {
    fn drop(&mut self) {
        if self.unreleased_bytes > 0 {
            let _ = self
                .body_stream
                .flow_control()
                .release_capacity(self.unreleased_bytes);
        }
    }
}

impl Http2StreamReceiver {
    /// Creates a new [`Http2StreamReceiver`] with ingress safety limits.
    #[inline]
    pub fn new(body_stream: h2::RecvStream, max_body_size: usize) -> Self {
        Self {
            body_stream,
            max_body_size,
            bytes_received: 0,
            unreleased_bytes: 0,
            flood_tracker: None,
        }
    }

    /// Attaches an [`Http2FloodTracker`] to count payload bytes read for anti-DoS accounting.
    #[inline]
    pub fn with_flood_tracker(mut self, tracker: Arc<Http2FloodTracker>) -> Self {
        self.flood_tracker = Some(tracker);
        self
    }

    /// Asynchronously receives the next DATA chunk from the incoming stream.
    pub async fn recv_chunk(&mut self) -> Result<Option<bytes::Bytes>, crate::error::Http2Error> {
        let Some(chunk_res) = self.body_stream.data().await else {
            if self.unreleased_bytes > 0 {
                let _ = self
                    .body_stream
                    .flow_control()
                    .release_capacity(self.unreleased_bytes);
                self.unreleased_bytes = 0;
            }
            return Ok(None);
        };

        let chunk = chunk_res?;
        let len = chunk.len();
        self.bytes_received += len;
        self.unreleased_bytes += len;
        if let Some(tracker) = &self.flood_tracker {
            tracker.on_payload_read(len);
        }

        if self.bytes_received > self.max_body_size {
            let _ = self
                .body_stream
                .flow_control()
                .release_capacity(self.unreleased_bytes);
            return Err(crate::error::Http2Error::PayloadTooLarge(
                self.bytes_received,
            ));
        }

        const INGRESS_BATCH_THRESHOLD: usize = 131_072; // 128 KB
        if self.unreleased_bytes >= INGRESS_BATCH_THRESHOLD {
            let _ = self
                .body_stream
                .flow_control()
                .release_capacity(self.unreleased_bytes);
            self.unreleased_bytes = 0;
        }

        Ok(Some(chunk))
    }

    /// Returns the total number of body bytes received so far on this stream.
    #[inline]
    pub fn bytes_received(&self) -> usize {
        self.bytes_received
    }

    /// Returns whether the stream has reached the end of stream.
    #[inline]
    pub fn is_end_stream(&self) -> bool {
        self.body_stream.is_end_stream()
    }

    /// Accumulates all remaining chunks into a [`Body`] while enforcing `max_body_size`.
    pub async fn consume_all(&mut self) -> Result<Body, crate::error::Http2Error> {
        if self.is_end_stream() {
            return Ok(Body::Empty);
        }

        let Some(first_chunk) = self.recv_chunk().await? else {
            return Ok(Body::Empty);
        };

        if self.is_end_stream() {
            if self.unreleased_bytes > 0 {
                let _ = self
                    .body_stream
                    .flow_control()
                    .release_capacity(self.unreleased_bytes);
                self.unreleased_bytes = 0;
            }
            return Ok(Body::Bytes(first_chunk));
        }

        let mut body_buf = bytes::BytesMut::with_capacity(first_chunk.len() * 2);
        body_buf.extend_from_slice(&first_chunk);

        while let Some(chunk) = self.recv_chunk().await? {
            body_buf.extend_from_slice(&chunk);
        }

        if self.unreleased_bytes > 0 {
            let _ = self
                .body_stream
                .flow_control()
                .release_capacity(self.unreleased_bytes);
            self.unreleased_bytes = 0;
        }

        Ok(Body::Bytes(body_buf.freeze()))
    }
}

/// Decodes an incoming multiplexed HTTP/2 request frame into a protocol-owned [`Http2ServerRequest`].
pub async fn decode_request(
    request: http::Request<h2::RecvStream>,
    config: &crate::config::Http2Config,
) -> Result<Http2ServerRequest, crate::error::Http2Error> {
    let (parts, body_stream) = request.into_parts();
    let head = Http2ServerRequestHead::new(parts.method, parts.uri, parts.headers, None);
    let mut receiver = Http2StreamReceiver::new(body_stream, config.max_body_size);
    let body = receiver.consume_all().await?;

    Ok(Http2ServerRequest::new(head, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_priority_memory_layout() {
        assert_eq!(std::mem::size_of::<Http2Priority>(), 2);
        assert_eq!(std::mem::align_of::<Http2Priority>(), 1);
    }

    #[test]
    fn test_priority_parsing_defaults() {
        assert_eq!(
            Http2Priority::from_header_bytes(b""),
            Http2Priority::DEFAULT
        );
        assert_eq!(
            Http2Priority::from_header_bytes(b"invalid"),
            Http2Priority::DEFAULT
        );
    }

    #[test]
    fn test_priority_parsing_urgency() {
        let p = Http2Priority::from_header_bytes(b"u=0");
        assert_eq!(p.urgency, 0);
        assert!(!p.incremental);

        let p = Http2Priority::from_header_bytes(b"u=7");
        assert_eq!(p.urgency, 7);
        assert!(!p.incremental);

        // Invalid urgency > 7 falls back to default 3
        let p = Http2Priority::from_header_bytes(b"u=9");
        assert_eq!(p.urgency, 3);
    }

    #[test]
    fn test_priority_parsing_incremental() {
        let p = Http2Priority::from_header_bytes(b"i");
        assert_eq!(p.urgency, 3);
        assert!(p.incremental);

        let p = Http2Priority::from_header_bytes(b"i=?1");
        assert_eq!(p.urgency, 3);
        assert!(p.incremental);

        let p = Http2Priority::from_header_bytes(b"i=?0");
        assert_eq!(p.urgency, 3);
        assert!(!p.incremental);
    }

    #[test]
    fn test_priority_parsing_combined() {
        let p = Http2Priority::from_header_bytes(b"u=1, i");
        assert_eq!(p.urgency, 1);
        assert!(p.incremental);

        let p = Http2Priority::from_header_bytes(b"i=?1, u=2");
        assert_eq!(p.urgency, 2);
        assert!(p.incremental);

        let p = Http2Priority::from_header_bytes(b"u=5, i=?0");
        assert_eq!(p.urgency, 5);
        assert!(!p.incremental);
    }
}
