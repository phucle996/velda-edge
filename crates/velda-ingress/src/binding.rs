//! Declarative ingress binding representing stateful TCP, stateless UDP, or kernel-bypass XDP.

use std::net::SocketAddr;
pub use velda_xdp::XdpConfig;

use crate::error::{IngressError, Result};
pub use crate::sock::{TcpBinding, TcpIngress, UdpBinding, UdpIngress};
pub use crate::xdp::XdpIngress;

/// Declarative ingress binding representing stateful TCP, stateless UDP, or kernel-bypass XDP.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressBinding {
    /// Stateful TCP connection listener via OS socket stack.
    Tcp(TcpBinding),
    /// Stateless UDP datagram socket via OS socket stack.
    Udp(UdpBinding),
    /// Kernel-bypass XDP listener with companion TCP binding.
    XdpTcp(TcpBinding, XdpConfig),
    /// Kernel-bypass XDP listener with companion UDP binding.
    XdpUdp(UdpBinding, XdpConfig),
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

    /// Sets or clears XDP configuration on this binding.
    pub fn with_xdp(self, xdp: Option<XdpConfig>) -> Self {
        match (self, xdp) {
            (Self::Tcp(b), Some(x)) | (Self::XdpTcp(b, _), Some(x)) => Self::XdpTcp(b, x),
            (Self::Udp(b), Some(x)) | (Self::XdpUdp(b, _), Some(x)) => Self::XdpUdp(b, x),
            (Self::XdpTcp(b, _), None) => Self::Tcp(b),
            (Self::XdpUdp(b, _), None) => Self::Udp(b),
            (other, None) => other,
        }
    }

    /// Returns the optional XDP configuration attached to this binding.
    #[inline]
    pub fn xdp(&self) -> Option<&XdpConfig> {
        match self {
            Self::XdpTcp(_, x) | Self::XdpUdp(_, x) => Some(x),
            Self::Tcp(_) | Self::Udp(_) => None,
        }
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
            Err(IngressError::Io(std::io::Error::new(
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
            Self::Tcp(b) | Self::XdpTcp(b, _) => &b.id,
            Self::Udp(b) | Self::XdpUdp(b, _) => &b.id,
        }
    }

    /// Returns the local address to bind.
    #[inline]
    pub fn addr(&self) -> SocketAddr {
        match self {
            Self::Tcp(b) | Self::XdpTcp(b, _) => b.addr,
            Self::Udp(b) | Self::XdpUdp(b, _) => b.addr,
        }
    }

    /// Returns true if this binding is for TCP.
    #[inline]
    pub const fn is_tcp(&self) -> bool {
        matches!(self, Self::Tcp(_) | Self::XdpTcp(_, _))
    }

    /// Returns true if this binding is for UDP.
    #[inline]
    pub const fn is_udp(&self) -> bool {
        matches!(self, Self::Udp(_) | Self::XdpUdp(_, _))
    }

    /// Returns true if this binding has XDP enabled.
    #[inline]
    pub const fn is_xdp(&self) -> bool {
        matches!(self, Self::XdpTcp(_, _) | Self::XdpUdp(_, _))
    }

    /// Returns whether downstream TLS is enabled on this binding.
    #[inline]
    pub const fn tls_enabled(&self) -> bool {
        match self {
            Self::Tcp(b) | Self::XdpTcp(b, _) => b.tls_enabled,
            Self::Udp(b) | Self::XdpUdp(b, _) => b.tls_enabled,
        }
    }

    /// Returns a reference to the inner TCP binding if this is TCP.
    #[inline]
    pub const fn as_tcp(&self) -> Option<&TcpBinding> {
        match self {
            Self::Tcp(b) | Self::XdpTcp(b, _) => Some(b),
            Self::Udp(_) | Self::XdpUdp(_, _) => None,
        }
    }

    /// Returns a mutable reference to the inner TCP binding if this is TCP.
    #[inline]
    pub fn as_tcp_mut(&mut self) -> Option<&mut TcpBinding> {
        match self {
            Self::Tcp(b) | Self::XdpTcp(b, _) => Some(b),
            Self::Udp(_) | Self::XdpUdp(_, _) => None,
        }
    }

    /// Returns a reference to the inner UDP binding if this is UDP.
    #[inline]
    pub const fn as_udp(&self) -> Option<&UdpBinding> {
        match self {
            Self::Udp(b) | Self::XdpUdp(b, _) => Some(b),
            Self::Tcp(_) | Self::XdpTcp(_, _) => None,
        }
    }

    /// Returns a mutable reference to the inner UDP binding if this is UDP.
    #[inline]
    pub fn as_udp_mut(&mut self) -> Option<&mut UdpBinding> {
        match self {
            Self::Udp(b) | Self::XdpUdp(b, _) => Some(b),
            Self::Tcp(_) | Self::XdpTcp(_, _) => None,
        }
    }
}
