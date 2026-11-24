//! HTTP/2 Downstream Ingress Request Entity & Progressive Stream Receiver (RFC 9113 & RFC 9218).
//!
//! Represents incoming requests decoded natively into [`HeaderWorkspace`] during HPACK parsing.

use bytes::{Bytes, BytesMut};
use http::{Method, Uri, Version};
use tokio::sync::mpsc;
use velda_core::buffer::header::HeaderWorkspace;
use velda_core::{Body, L7Request};

use super::connection::{ServerBodyEvent, ServerCmd};
use crate::error::Http2Error;

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
    /// Default HTTP priority according to RFC 9218 (`urgency = 3`, `incremental = false`).
    pub const DEFAULT: Self = Self {
        urgency: 3,
        incremental: false,
    };

    /// Parses an RFC 9218 `Priority` header value directly from raw byte slice without heap allocation.
    #[inline]
    pub fn from_header_bytes(bytes: &[u8]) -> Self {
        let mut urgency = 3u8;
        let mut incremental = false;

        let mut i = 0;
        let len = bytes.len();

        while i < len {
            while i < len && (bytes[i] == b' ' || bytes[i] == b'\t' || bytes[i] == b',') {
                i += 1;
            }
            if i >= len {
                break;
            }

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

            while i < len && (bytes[i] == b' ' || bytes[i] == b'\t') {
                i += 1;
            }

            let mut has_val = false;
            if i < len && bytes[i] == b'=' {
                i += 1;
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
    pub method: Method,
    pub uri: Uri,
    pub version: Version,
    pub headers: HeaderWorkspace,
    pub stream_id: Option<u32>,
    pub priority: Http2Priority,
    pub request_id: u128,
    pub ingress_time_nanos: u64,
}

impl Http2ServerRequestHead {
    /// Creates a new [`Http2ServerRequestHead`].
    pub fn new(method: Method, uri: Uri, headers: HeaderWorkspace, stream_id: Option<u32>) -> Self {
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
            request_id: 0,
            ingress_time_nanos: 0,
        }
    }
}

/// An incoming HTTP/2 server request received on an active multiplexed stream.
#[derive(Debug, Clone)]
pub struct Http2ServerRequest {
    pub head: Http2ServerRequestHead,
    pub body: Body,
}

impl Http2ServerRequest {
    #[inline]
    pub fn new(head: Http2ServerRequestHead, body: Body) -> Self {
        Self { head, body }
    }

    pub fn into_l7_request(self) -> L7Request {
        L7Request::new(
            self.head.method,
            self.head.uri,
            Version::HTTP_2,
            self.head.headers,
            self.body,
        )
    }
}

/// Progressive stream receiver for an incoming HTTP/2 request body.
#[derive(Debug)]
pub struct Http2StreamReceiver {
    pub(crate) stream_id: u32,
    pub(crate) body_rx: mpsc::UnboundedReceiver<Result<ServerBodyEvent, Http2Error>>,
    pub(crate) cmd_tx: mpsc::UnboundedSender<ServerCmd>,
    pub(crate) max_body_size: usize,
    pub bytes_received: usize,
    pub(crate) eof: bool,
    pub(crate) trailers: Option<HeaderWorkspace>,
}

impl Http2StreamReceiver {
    #[inline(always)]
    fn record_chunk(&mut self, len: usize) -> Result<(), Http2Error> {
        self.bytes_received = self.bytes_received.saturating_add(len);
        let _ = self.cmd_tx.send(ServerCmd::ReleaseRecvCapacity {
            stream_id: self.stream_id,
            bytes: len as u32,
        });
        if self.bytes_received > self.max_body_size {
            return Err(Http2Error::PayloadTooLarge(self.bytes_received));
        }
        Ok(())
    }

    /// Asynchronously receives the next `DATA` chunk from the incoming stream.
    pub async fn recv_chunk(&mut self) -> Result<Option<Bytes>, Http2Error> {
        if self.eof {
            return Ok(None);
        }
        loop {
            match self.body_rx.recv().await {
                Some(Ok(ServerBodyEvent::Data {
                    payload,
                    end_stream,
                })) => {
                    if end_stream {
                        self.eof = true;
                    }
                    if !payload.is_empty() {
                        self.record_chunk(payload.len())?;
                        return Ok(Some(payload));
                    }
                    if self.eof {
                        return Ok(None);
                    }
                }
                Some(Ok(ServerBodyEvent::Trailers(tr))) => {
                    self.trailers = Some(tr);
                    self.eof = true;
                    return Ok(None);
                }
                Some(Err(e)) => {
                    self.eof = true;
                    return Err(e);
                }
                None => {
                    self.eof = true;
                    return Ok(None);
                }
            }
        }
    }

    /// Receives a chunk and coalesces immediately queued chunks up to `max_coalesce_bytes`.
    pub async fn recv_coalesced_chunk(
        &mut self,
        max_coalesce_bytes: usize,
    ) -> Result<Option<Bytes>, Http2Error> {
        let Some(first) = self.recv_chunk().await? else {
            return Ok(None);
        };
        if first.len() >= max_coalesce_bytes || self.eof {
            return Ok(Some(first));
        }

        let mut buf: Option<BytesMut> = None;
        while buf.as_ref().map_or(first.len(), BytesMut::len) < max_coalesce_bytes && !self.eof {
            match self.body_rx.try_recv() {
                Ok(Ok(ServerBodyEvent::Data {
                    payload,
                    end_stream,
                })) => {
                    if end_stream {
                        self.eof = true;
                    }
                    if !payload.is_empty() {
                        self.record_chunk(payload.len())?;
                        let b = buf.get_or_insert_with(|| {
                            let mut bm = BytesMut::with_capacity(
                                (first.len() + payload.len() + 16_384).min(max_coalesce_bytes),
                            );
                            bm.extend_from_slice(&first);
                            bm
                        });
                        b.extend_from_slice(&payload);
                    }
                }
                Ok(Ok(ServerBodyEvent::Trailers(tr))) => {
                    self.trailers = Some(tr);
                    self.eof = true;
                    break;
                }
                Ok(Err(e)) => {
                    self.eof = true;
                    return Err(e);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.eof = true;
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
            }
        }

        match buf {
            Some(bm) => Ok(Some(bm.freeze())),
            None => Ok(Some(first)),
        }
    }

    /// Returns whether the stream has reached the end of stream.
    #[inline]
    pub fn is_end_stream(&self) -> bool {
        self.eof
    }

    /// Takes trailing headers received with EOF, if any.
    #[inline]
    pub fn take_trailers(&mut self) -> Option<HeaderWorkspace> {
        self.trailers.take()
    }

    /// Accumulates all remaining chunks into a [`Body`] while enforcing `max_body_size`.
    pub async fn consume_all(&mut self) -> Result<Body, Http2Error> {
        self.consume_all_into_slab(None).await
    }

    /// Accumulates all remaining chunks into a pre-warmed [`PayloadSlabPool`] slab when $\le 64\text{ KB}$,
    /// falling back to heap [`BytesMut`] for larger bodies.
    pub async fn consume_all_into_slab(
        &mut self,
        slab_pool: Option<&std::sync::Arc<velda_core::buffer::slab::PayloadSlabPool>>,
    ) -> Result<Body, Http2Error> {
        let Some(first_chunk) = self.recv_chunk().await? else {
            return Ok(Body::Empty);
        };
        if self.eof {
            if let Some(pool) = slab_pool
                && let Some(mut lease) = pool.acquire_for_size(first_chunk.len())
            {
                let len = first_chunk.len();
                lease[..len].copy_from_slice(&first_chunk);
                return Ok(Body::from_slab(lease, len));
            }
            return Ok(Body::Bytes(first_chunk));
        }

        let Some(second_chunk) = self.recv_chunk().await? else {
            if let Some(pool) = slab_pool
                && let Some(mut lease) = pool.acquire_for_size(first_chunk.len())
            {
                let len = first_chunk.len();
                lease[..len].copy_from_slice(&first_chunk);
                return Ok(Body::from_slab(lease, len));
            }
            return Ok(Body::Bytes(first_chunk));
        };

        let two_len = first_chunk.len() + second_chunk.len();
        if self.eof
            && let Some(pool) = slab_pool
            && let Some(mut lease) = pool.acquire_for_size(two_len)
        {
            lease[..first_chunk.len()].copy_from_slice(&first_chunk);
            lease[first_chunk.len()..two_len].copy_from_slice(&second_chunk);
            return Ok(Body::from_slab(lease, two_len));
        }

        if !self.eof
            && two_len <= velda_core::buffer::slab::SlabClass::Large64K.byte_size()
            && let Some(pool) = slab_pool
            && let Some(mut lease) = pool.acquire(velda_core::buffer::slab::SlabClass::Large64K)
        {
            lease[..first_chunk.len()].copy_from_slice(&first_chunk);
            lease[first_chunk.len()..two_len].copy_from_slice(&second_chunk);
            drop(first_chunk);
            drop(second_chunk);
            let mut offset = two_len;

            while let Some(chunk) = self.recv_chunk().await? {
                if offset + chunk.len() <= lease.len() {
                    lease[offset..offset + chunk.len()].copy_from_slice(&chunk);
                    offset += chunk.len();
                } else {
                    let mut body_buf = BytesMut::with_capacity((offset + chunk.len()) * 2);
                    body_buf.extend_from_slice(&lease[..offset]);
                    body_buf.extend_from_slice(&chunk);
                    drop(lease);
                    while let Some(rem) = self.recv_chunk().await? {
                        body_buf.extend_from_slice(&rem);
                    }
                    return Ok(Body::Bytes(body_buf.freeze()));
                }
            }
            return Ok(Body::from_slab(lease, offset));
        }

        let mut body_buf = BytesMut::with_capacity(two_len * 2);
        body_buf.extend_from_slice(&first_chunk);
        body_buf.extend_from_slice(&second_chunk);
        while let Some(chunk) = self.recv_chunk().await? {
            body_buf.extend_from_slice(&chunk);
        }
        Ok(Body::Bytes(body_buf.freeze()))
    }
}

/// Extracts effective `Host` or authority string from request headers (`host`) or URI.
#[inline]
pub fn extract_host<'a>(headers: &'a HeaderWorkspace, uri: &'a Uri) -> Option<&'a str> {
    headers
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .or_else(|| uri.authority().map(|a| a.as_str()).or_else(|| uri.host()))
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

    #[test]
    fn test_extract_host_precedence() {
        let mut ws = HeaderWorkspace::empty(4);
        let uri: Uri = "https://authority.example.com:8443/path".parse().unwrap();
        assert_eq!(
            super::extract_host(&ws, &uri),
            Some("authority.example.com:8443")
        );

        ws.insert(
            http::header::HOST,
            http::HeaderValue::from_static("header.example.com"),
        );
        assert_eq!(super::extract_host(&ws, &uri), Some("header.example.com"));
    }
}
