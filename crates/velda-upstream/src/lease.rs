//! RAII Backend Connection Lease and Acquisition Target parameters.
//!
//! # Core Invariants & Lifecycle
//!
//! A [`BackendLease`] represents an active, exclusive acquisition of a backend connection
//! from the underlying connection pool.
//!
//! When finished with the request:
//! - Call [`BackendLease::release`] with `reusable: true` to return healthy connections to the pool.
//! - Call [`BackendLease::release`] with `reusable: false` if connection had protocol or I/O errors.
//! - If dropped without explicit release (e.g. task cancellation, timeout, or panic),
//!   [`Drop`] will safely inspect the connection and return or close it, preventing file descriptor leaks.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use velda_connection_pool::{ConnectionKey, PoolManager};
use velda_lb::SelectionContext;

use crate::connection::BackendConnection;
use crate::health::HealthTracker;

/// Strongly typed pool manager for upstream backend connections.
pub type UpstreamPoolManager = PoolManager<ConnectionKey, Box<dyn BackendConnection>>;

/// An active, exclusive lease of a backend connection.
///
/// When finished with the request:
/// - Call [`BackendLease::release`] with `reusable: true` to return healthy connections to the pool.
/// - Call [`BackendLease::release`] with `reusable: false` if connection had protocol or I/O errors.
/// - If dropped without explicit release, RAII will safely inspect and return or close the connection.
pub struct BackendLease {
    connection: Option<Box<dyn BackendConnection>>,
    key: ConnectionKey,
    pool: Arc<UpstreamPoolManager>,
    health: Arc<HealthTracker>,
    is_draining: bool,
}

impl BackendLease {
    /// Creates a new backend lease wrapping an established connection.
    pub fn new(
        connection: Box<dyn BackendConnection>,
        key: ConnectionKey,
        pool: Arc<UpstreamPoolManager>,
        health: Arc<HealthTracker>,
        is_draining: bool,
    ) -> Self {
        Self {
            connection: Some(connection),
            key,
            pool,
            health,
            is_draining,
        }
    }

    /// Accesses the underlying backend connection.
    #[inline]
    pub fn connection(&self) -> &dyn BackendConnection {
        self.connection.as_ref().unwrap().as_ref()
    }

    /// Mutably accesses the underlying backend connection.
    #[inline]
    pub fn connection_mut(&mut self) -> &mut (dyn BackendConnection + 'static) {
        self.connection.as_mut().unwrap().as_mut()
    }

    /// Returns the target endpoint address of this connection.
    #[inline]
    pub fn endpoint(&self) -> SocketAddr {
        self.key.target_addr
    }

    /// Explicitly releases the connection back to the pool manager.
    ///
    /// - If `reusable` is true and endpoint is not draining: returns connection to pool and records health success.
    /// - If `reusable` is false or endpoint is draining: closes connection immediately.
    pub fn release(mut self, reusable: bool) {
        if let Some(conn) = self.connection.take() {
            if reusable {
                self.health.record_success(&self.key.target_addr);
            }
            self.pool
                .release(&self.key, conn, reusable, self.is_draining);
        }
    }

    /// Consumes this lease and extracts the raw underlying connection.
    pub fn into_inner(mut self) -> Option<Box<dyn BackendConnection>> {
        self.connection.take()
    }

    /// Consumes this lease and returns the underlying raw [`tokio::net::TcpStream`] if available.
    pub fn into_tcp_stream(mut self) -> Option<TcpStream> {
        self.connection.take().and_then(|c| c.into_tcp_stream())
    }
}

impl Drop for BackendLease {
    fn drop(&mut self) {
        if let Some(conn) = self.connection.take() {
            // Guard against leaked file descriptors on task cancellation / panic
            let healthy = conn.is_healthy();
            self.pool
                .release(&self.key, conn, healthy, self.is_draining);
        }
    }
}

/// Protocol and transport parameters passed into backend connection acquisition.
#[derive(Clone, Default)]
pub struct AcquireTarget<'a> {
    /// Optional protocol override (e.g. "http2", "http1", "tcp"). If None, uses upstream default protocol.
    pub protocol: Option<Arc<str>>,

    /// Optional TLS Server Name Indication (SNI).
    pub sni: Option<Arc<str>>,

    /// Optional ALPN negotiation token.
    pub alpn: Option<Arc<str>>,

    /// Contextual hints for advanced load balancing (e.g. hashing key, IP, client attributes).
    pub selection_context: SelectionContext<'a>,
}

impl<'a> AcquireTarget<'a> {
    /// Creates a default acquisition target using the upstream's configured protocol.
    pub fn default_target() -> Self {
        Self {
            protocol: None,
            sni: None,
            alpn: None,
            selection_context: SelectionContext::NONE,
        }
    }

    /// Creates an acquisition target specifying an explicit protocol override.
    pub fn new(protocol: impl Into<Arc<str>>) -> Self {
        Self {
            protocol: Some(protocol.into()),
            sni: None,
            alpn: None,
            selection_context: SelectionContext::NONE,
        }
    }

    pub fn with_sni(mut self, sni: impl Into<Arc<str>>) -> Self {
        self.sni = Some(sni.into());
        self
    }

    pub fn with_alpn(mut self, alpn: impl Into<Arc<str>>) -> Self {
        self.alpn = Some(alpn.into());
        self
    }

    pub fn with_selection_context(mut self, ctx: SelectionContext<'a>) -> Self {
        self.selection_context = ctx;
        self
    }

    pub fn with_hash_key(mut self, hash_key: u64) -> Self {
        self.selection_context.hash_key = Some(hash_key);
        self
    }
}
