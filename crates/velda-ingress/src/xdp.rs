//! Kernel-bypass XDP ingress listener coordination.

use std::sync::Arc;
use velda_xdp::{XdpConfig, XdpEngineKind, XdpListener};

use crate::error::Result;
use crate::sock::{TcpBinding, TcpIngress, UdpBinding, UdpIngress};

/// Active XDP ingress listener coordinating eBPF attachment and companion sockets.
#[derive(Debug)]
pub struct XdpIngress {
    id: Arc<str>,
    xdp_listener: XdpListener,
    tcp_companion: Option<Arc<TcpIngress>>,
    udp_companion: Option<Arc<UdpIngress>>,
}

impl XdpIngress {
    /// Binds an XDP listener and any required companion socket for a TCP listener shard.
    pub fn bind_tcp_shard(
        tcp_binding: TcpBinding,
        xdp_config: XdpConfig,
        shard_id: usize,
        addr_override: Option<std::net::SocketAddr>,
    ) -> Result<Self> {
        let id: Arc<str> = Arc::from(tcp_binding.id.as_str());
        let xdp_listener = XdpListener::bind(xdp_config)?;
        let tcp_companion = match xdp_listener.config().engine_kind {
            XdpEngineKind::PassFilter | XdpEngineKind::Sockmap => {
                let ingress = TcpIngress::bind_shard(tcp_binding, shard_id, addr_override)?;
                Some(Arc::new(ingress))
            }
            XdpEngineKind::AfXdp => None,
        };
        Ok(Self {
            id,
            xdp_listener,
            tcp_companion,
            udp_companion: None,
        })
    }

    /// Binds an XDP listener and any required companion socket for a UDP socket shard.
    pub fn bind_udp_shard(
        udp_binding: UdpBinding,
        xdp_config: XdpConfig,
        shard_id: usize,
        addr_override: Option<std::net::SocketAddr>,
    ) -> Result<Self> {
        let id: Arc<str> = Arc::from(udp_binding.id.as_str());
        let xdp_listener = XdpListener::bind(xdp_config)?;
        let udp_companion = match xdp_listener.config().engine_kind {
            XdpEngineKind::PassFilter | XdpEngineKind::Sockmap => {
                let ingress = UdpIngress::bind_shard(udp_binding, shard_id, addr_override)?;
                Some(Arc::new(ingress))
            }
            XdpEngineKind::AfXdp => None,
        };
        Ok(Self {
            id,
            xdp_listener,
            tcp_companion: None,
            udp_companion,
        })
    }

    /// Listener identifier.
    #[inline]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Underlying XDP listener instance.
    #[inline]
    pub fn xdp_listener(&self) -> &XdpListener {
        &self.xdp_listener
    }

    /// Companion TCP socket ingress, if running in PassFilter or Sockmap mode.
    #[inline]
    pub fn tcp_companion(&self) -> Option<&Arc<TcpIngress>> {
        self.tcp_companion.as_ref()
    }

    /// Companion UDP socket ingress, if running in PassFilter mode.
    #[inline]
    pub fn udp_companion(&self) -> Option<&Arc<UdpIngress>> {
        self.udp_companion.as_ref()
    }
}
