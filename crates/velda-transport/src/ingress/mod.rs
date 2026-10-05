//! Ingress subsystem managing client traffic arrival for stateful TCP and stateless UDP.

pub mod tcp;
pub mod udp;

use std::net::SocketAddr;

pub use tcp::{TcpBinding, TcpIngress};
pub use udp::{UdpBinding, UdpIngress};

use crate::error::{Result, TransportError};

/// Declarative ingress binding representing either a stateful TCP listener or stateless UDP socket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressBinding {
    /// Stateful TCP connection listener.
    Tcp(TcpBinding),
    /// Stateless UDP datagram socket.
    Udp(UdpBinding),
}

impl IngressBinding {
    /// Creates a TCP ingress binding.
    pub fn tcp(binding: TcpBinding) -> Self {
        Self::Tcp(binding)
    }

    /// Creates a UDP ingress binding.
    pub fn udp(binding: UdpBinding) -> Self {
        Self::Udp(binding)
    }

    /// Creates an ingress binding from protocol name ("tcp" or "udp").
    pub fn from_transport(
        id: impl Into<String>,
        addr: SocketAddr,
        transport_protocol: impl AsRef<str>,
        tls_enabled: bool,
    ) -> Result<Self> {
        let tp = transport_protocol.as_ref();
        let id_str = id.into();
        if tp.eq_ignore_ascii_case("tcp") {
            Ok(Self::Tcp(TcpBinding::new(id_str, addr, tls_enabled)))
        } else if tp.eq_ignore_ascii_case("udp") {
            Ok(Self::Udp(UdpBinding::new(id_str, addr, tls_enabled)))
        } else {
            Err(TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "unsupported transport protocol '{tp}' for listener '{id_str}' (must be 'tcp' or 'udp')"
                ),
            )))
        }
    }

    /// Returns the unique listener identifier.
    #[inline]
    pub fn id(&self) -> &str {
        match self {
            Self::Tcp(b) => &b.id,
            Self::Udp(b) => &b.id,
        }
    }

    /// Returns the local address to bind.
    #[inline]
    pub fn addr(&self) -> SocketAddr {
        match self {
            Self::Tcp(b) => b.addr,
            Self::Udp(b) => b.addr,
        }
    }

    /// Returns true if this binding is for TCP.
    #[inline]
    pub const fn is_tcp(&self) -> bool {
        matches!(self, Self::Tcp(_))
    }

    /// Returns true if this binding is for UDP.
    #[inline]
    pub const fn is_udp(&self) -> bool {
        matches!(self, Self::Udp(_))
    }

    /// Returns whether downstream TLS is enabled on this binding.
    #[inline]
    pub const fn tls_enabled(&self) -> bool {
        match self {
            Self::Tcp(b) => b.tls_enabled,
            Self::Udp(b) => b.tls_enabled,
        }
    }

    /// Returns a reference to the inner TCP binding if this is TCP.
    #[inline]
    pub const fn as_tcp(&self) -> Option<&TcpBinding> {
        match self {
            Self::Tcp(b) => Some(b),
            Self::Udp(_) => None,
        }
    }

    /// Returns a mutable reference to the inner TCP binding if this is TCP.
    #[inline]
    pub fn as_tcp_mut(&mut self) -> Option<&mut TcpBinding> {
        match self {
            Self::Tcp(b) => Some(b),
            Self::Udp(_) => None,
        }
    }

    /// Returns a reference to the inner UDP binding if this is UDP.
    #[inline]
    pub const fn as_udp(&self) -> Option<&UdpBinding> {
        match self {
            Self::Udp(b) => Some(b),
            Self::Tcp(_) => None,
        }
    }

    /// Returns a mutable reference to the inner UDP binding if this is UDP.
    #[inline]
    pub fn as_udp_mut(&mut self) -> Option<&mut UdpBinding> {
        match self {
            Self::Udp(b) => Some(b),
            Self::Tcp(_) => None,
        }
    }
}
