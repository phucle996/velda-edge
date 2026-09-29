//! Connection identity, backend connection abstraction, and connector contract.

use std::fmt;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::TcpStream;

pub use velda_connection_pool::{ConnectionKey, PoolableResource};

use crate::error::{Result, UpstreamError};

/// Abstract contract for an acquired backend connection.
pub trait BackendConnection: PoolableResource + Send + Sync + fmt::Debug + 'static {
    /// Remote socket address of this connection.
    fn peer(&self) -> SocketAddr;

    /// Downcasts reference to `Any` for concrete backend connection extraction.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Downcasts mutable reference to `Any`.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// Downcasts boxed connection to `Box<dyn Any>`.
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any>;

    /// Consumes the boxed connection and returns the underlying raw [`TcpStream`] if available.
    fn into_tcp_stream(self: Box<Self>) -> Option<TcpStream> {
        None
    }
}

/// Generic factory capability for establishing new backend connections on pool miss.
pub trait Connector: Send + Sync + 'static {
    /// Establishes a new connection to `target`.
    fn connect(
        &self,
        target: SocketAddr,
    ) -> impl std::future::Future<Output = Result<Box<dyn BackendConnection>>> + Send;
}

/// Real TCP connector using Tokio's asynchronous TCP stream.
#[derive(Debug, Default)]
pub struct TcpConnector;

impl Connector for TcpConnector {
    async fn connect(&self, target: SocketAddr) -> Result<Box<dyn BackendConnection>> {
        let stream =
            TcpStream::connect(target)
                .await
                .map_err(|e| UpstreamError::ConnectionFailed {
                    endpoint: target,
                    reason: e.to_string(),
                })?;

        Ok(Box::new(RealTcpConnection {
            stream: Some(stream),
            peer: target,
            created_at: Instant::now(),
            last_used_at: Instant::now(),
        }))
    }
}

/// Wrapper around a live Tokio [`TcpStream`].
#[derive(Debug)]
pub struct RealTcpConnection {
    stream: Option<TcpStream>,
    peer: SocketAddr,
    created_at: Instant,
    last_used_at: Instant,
}

impl RealTcpConnection {
    /// Consumes the wrapper and returns the underlying [`TcpStream`].
    pub fn into_inner(mut self) -> Option<TcpStream> {
        self.stream.take()
    }

    /// Returns a reference to the underlying [`TcpStream`] if active.
    pub fn stream(&self) -> Option<&TcpStream> {
        self.stream.as_ref()
    }
}

impl PoolableResource for RealTcpConnection {
    fn is_healthy(&self) -> bool {
        self.stream.is_some()
    }

    fn created_at(&self) -> Instant {
        self.created_at
    }

    fn last_used_at(&self) -> Instant {
        self.last_used_at
    }

    fn touch(&mut self) {
        self.last_used_at = Instant::now();
    }

    fn touch_at(&mut self, now: Instant) {
        self.last_used_at = now;
    }

    fn close(&mut self) {
        self.stream = None;
    }
}

impl BackendConnection for RealTcpConnection {
    fn peer(&self) -> SocketAddr {
        self.peer
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }

    fn into_tcp_stream(mut self: Box<Self>) -> Option<TcpStream> {
        self.stream.take()
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub use mock::*;

#[cfg(any(test, feature = "test-utils"))]
pub mod mock {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// In-memory mock connection for unit testing.
    #[derive(Debug)]
    pub struct MockConnection {
        pub peer: SocketAddr,
        pub created_at: Instant,
        pub last_used_at: Instant,
        pub healthy: Arc<AtomicBool>,
    }

    impl MockConnection {
        pub fn new(peer: SocketAddr) -> Self {
            Self {
                peer,
                created_at: Instant::now(),
                last_used_at: Instant::now(),
                healthy: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    impl PoolableResource for MockConnection {
        fn is_healthy(&self) -> bool {
            self.healthy.load(Ordering::Acquire)
        }

        fn created_at(&self) -> Instant {
            self.created_at
        }

        fn last_used_at(&self) -> Instant {
            self.last_used_at
        }

        fn touch(&mut self) {
            self.last_used_at = Instant::now();
        }

        fn touch_at(&mut self, now: Instant) {
            self.last_used_at = now;
        }

        fn close(&mut self) {
            self.healthy.store(false, Ordering::Release);
        }
    }

    impl BackendConnection for MockConnection {
        fn peer(&self) -> SocketAddr {
            self.peer
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }

        fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
            self
        }
    }

    /// Mock connector for testing connection acquisition.
    #[derive(Debug, Default)]
    pub struct MockConnector {
        fail_addrs: std::sync::RwLock<std::collections::HashSet<SocketAddr>>,
    }

    impl MockConnector {
        pub fn new() -> Self {
            Self {
                fail_addrs: std::sync::RwLock::new(std::collections::HashSet::new()),
            }
        }

        pub fn set_failing(&self, addr: SocketAddr) {
            self.fail_addrs.write().unwrap().insert(addr);
        }

        pub fn clear_failing(&self, addr: &SocketAddr) {
            self.fail_addrs.write().unwrap().remove(addr);
        }

        pub fn clear_all(&self) {
            self.fail_addrs.write().unwrap().clear();
        }
    }

    impl Connector for MockConnector {
        async fn connect(&self, target: SocketAddr) -> Result<Box<dyn BackendConnection>> {
            if self.fail_addrs.read().unwrap().contains(&target) {
                return Err(UpstreamError::ConnectionFailed {
                    endpoint: target,
                    reason: "simulated connection refusal".into(),
                });
            }
            Ok(Box::new(MockConnection::new(target)))
        }
    }
}
