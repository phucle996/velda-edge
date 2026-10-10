//! Protocol-isolated in-memory upstream tables for lock-free O(1) reads.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use super::grpc::{GrpcTcpUpstream, GrpcUdpUpstream};
use super::http::{Http1Upstream, Http2Upstream, Http3Upstream};
use super::raw::{TcpUpstream, UdpUpstream};

/// Pre-compiled single-protocol lookup table providing lock-free O(1) reads.
///
/// Pre-constructed during snapshot build; maps upstream_id -> Arc<T>.
pub struct SubUpstreamTable<T> {
    /// [PRE-COMPILED]: In-memory mapping of upstream identifiers to protocol processors.
    entries: FxHashMap<String, Arc<T>>,
}

impl<T> Clone for SubUpstreamTable<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Default for SubUpstreamTable<T> {
    fn default() -> Self {
        Self {
            entries: FxHashMap::default(),
        }
    }
}

impl<T> std::fmt::Debug for SubUpstreamTable<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubUpstreamTable")
            .field("keys", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<T> SubUpstreamTable<T> {
    /// Creates a new protocol sub-table from an owned entry map.
    pub fn new(entries: FxHashMap<String, Arc<T>>) -> Self {
        Self { entries }
    }

    /// Retrieves an upstream processor by identifier.
    #[inline]
    pub fn get(&self, id: &str) -> Option<&Arc<T>> {
        self.entries.get(id)
    }

    /// Returns the number of upstreams in this sub-table.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether this sub-table is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Upstream variant in the Layer 4 Raw family (TCP byte stream / UDP datagram).
#[derive(Clone, Debug)]
pub enum RawUpstream {
    Tcp(Arc<TcpUpstream>),
    Udp(Arc<UdpUpstream>),
}

impl RawUpstream {
    #[inline]
    pub fn as_tcp(&self) -> Option<&Arc<TcpUpstream>> {
        match self {
            Self::Tcp(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_udp(&self) -> Option<&Arc<UdpUpstream>> {
        match self {
            Self::Udp(u) => Some(u),
            _ => None,
        }
    }
}

/// Upstream variant in the Layer 7 HTTP family (HTTP/1.1, HTTP/2, HTTP/3).
#[derive(Clone, Debug)]
pub enum HttpUpstream {
    Http1(Arc<Http1Upstream>),
    Http2(Arc<Http2Upstream>),
    Http3(Arc<Http3Upstream>),
}

impl HttpUpstream {
    /// Returns the upstream unique identifier.
    #[inline]
    pub fn id(&self) -> &str {
        match self {
            Self::Http1(u) => u.id(),
            Self::Http2(u) => u.id(),
            Self::Http3(u) => u.id(),
        }
    }

    #[inline]
    pub fn as_http1(&self) -> Option<&Arc<Http1Upstream>> {
        match self {
            Self::Http1(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_http2(&self) -> Option<&Arc<Http2Upstream>> {
        match self {
            Self::Http2(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_http3(&self) -> Option<&Arc<Http3Upstream>> {
        match self {
            Self::Http3(u) => Some(u),
            _ => None,
        }
    }

    /// Returns whether this upstream employs progressive streaming (client, server, or duplex).
    #[inline]
    pub fn is_streaming(&self) -> bool {
        match self {
            Self::Http1(u) => u.strategy != velda_http1::Http1PipeStrategy::Buffered,
            Self::Http2(u) => u.strategy != velda_http2::Http2PipeStrategy::Buffered,
            Self::Http3(_) => false,
        }
    }
}

/// Upstream variant in the Layer 7 gRPC family (gRPC over TCP / gRPC over UDP).
#[derive(Clone, Debug)]
pub enum GrpcUpstream {
    Tcp(Arc<GrpcTcpUpstream>),
    Udp(Arc<GrpcUdpUpstream>),
}

impl GrpcUpstream {
    #[inline]
    pub fn as_tcp(&self) -> Option<&Arc<GrpcTcpUpstream>> {
        match self {
            Self::Tcp(u) => Some(u),
            _ => None,
        }
    }

    #[inline]
    pub fn as_udp(&self) -> Option<&Arc<GrpcUdpUpstream>> {
        match self {
            Self::Udp(u) => Some(u),
            _ => None,
        }
    }
}

/// Pre-compiled upstream table grouped strictly by ProtocolFamily.
///
/// Guaranteed zero cross-protocol lookup overhead and zero dynamic casting on the request serving hot path.
#[derive(Clone, Default, Debug)]
pub struct UpstreamTable {
    /// [PRE-COMPILED]: Protocol table for L4 Raw streams and datagrams (ProtocolFamily::Raw).
    pub raw: SubUpstreamTable<RawUpstream>,
    /// [PRE-COMPILED]: Protocol table for L7 HTTP web traffic (ProtocolFamily::Http).
    pub http: SubUpstreamTable<HttpUpstream>,
    /// [PRE-COMPILED]: Protocol table for L7 gRPC RPC endpoints (ProtocolFamily::Grpc).
    pub grpc: SubUpstreamTable<GrpcUpstream>,
}

impl UpstreamTable {
    /// Returns total number of upstreams across all protocol families.
    #[inline]
    pub fn len(&self) -> usize {
        self.raw.len() + self.http.len() + self.grpc.len()
    }

    /// Returns true if all protocol tables are empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retrieves an upstream processor in the Raw family.
    #[inline]
    pub fn get_raw(&self, id: &str) -> Option<&Arc<RawUpstream>> {
        self.raw.get(id)
    }

    /// Retrieves an upstream processor in the HTTP family.
    #[inline]
    pub fn get_http(&self, id: &str) -> Option<&Arc<HttpUpstream>> {
        self.http.get(id)
    }

    /// Retrieves an upstream processor in the gRPC family.
    #[inline]
    pub fn get_grpc(&self, id: &str) -> Option<&Arc<GrpcUpstream>> {
        self.grpc.get(id)
    }
}
