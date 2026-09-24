//! Central Traffic Engine coordinating arbitrary numbers of TCP and UDP ingress listeners.

use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::connection::Connection;
use crate::error::Result;
use crate::forwarding::l7::L7Handoff;
use crate::ingress::classifier::PathKind;
use crate::ingress::listener::{IngressBinding, IngressListener};
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

/// High-performance edge traffic engine coordinating multi-port ingress bindings,
/// classification, and execution dispatching across any number of TCP and UDP ports.
#[derive(Default)]
pub struct TrafficEngine {
    tcp_listeners: Vec<IngressListener>,
    udp_listeners: Vec<(String, Arc<UdpSocket>)>,
}

impl TrafficEngine {
    /// Creates a new empty traffic engine.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a TCP ingress listener directly.
    pub fn add_tcp_listener(&mut self, listener: IngressListener) -> &mut Self {
        self.tcp_listeners.push(listener);
        self
    }

    /// Registers a UDP socket directly with a listener identifier.
    pub fn add_udp_socket(&mut self, id: impl Into<String>, socket: UdpSocket) -> &mut Self {
        self.udp_listeners.push((id.into(), Arc::new(socket)));
        self
    }

    /// Binds and registers a user-defined [`IngressBinding`].
    ///
    /// Transparently handles TCP (HTTP, HTTPS, raw TCP) and UDP listeners:
    /// - If `protocol == "udp"`, a [`UdpSocket`] is bound on the declared address.
    /// - If `protocol == "http"` or `"tcp"`, an [`IngressListener`] is bound.
    pub fn add_binding(&mut self, binding: IngressBinding) -> Result<()> {
        if binding.is_udp() {
            let socket =
                UdpSocket::bind(binding.addr, crate::udp::config::UdpSocketConfig::default())?;
            self.udp_listeners.push((binding.id, Arc::new(socket)));
            Ok(())
        } else {
            let listener = IngressListener::bind(binding)?;
            self.tcp_listeners.push(listener);
            Ok(())
        }
    }

    /// Returns the number of registered TCP listeners.
    #[inline]
    pub fn tcp_listener_count(&self) -> usize {
        self.tcp_listeners.len()
    }

    /// Returns the number of registered UDP listeners.
    #[inline]
    pub fn udp_listener_count(&self) -> usize {
        self.udp_listeners.len()
    }

    /// Starts all ingress listener accept loops (TCP) and dispatches incoming traffic
    /// to either the L4 fast path or L7 protocol handoff until shutdown is signaled.
    pub async fn run<L4H, L7H, FutL4, FutL7>(
        self,
        shutdown: watch::Receiver<bool>,
        l4_handler: L4H,
        l7_handler: L7H,
    ) -> Result<()>
    where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(L7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
    {
        self.run_with_udp(
            shutdown,
            l4_handler,
            l7_handler,
            |_id: String, _sock: Arc<UdpSocket>, _dgram: Datagram| async {},
        )
        .await
    }

    /// Starts all ingress loops across all configured TCP and UDP ports concurrently,
    /// dispatching incoming traffic to L4 TCP, L7 HTTP/TLS, or UDP handlers until shutdown.
    pub async fn run_with_udp<L4H, L7H, UdpH, FutL4, FutL7, FutUdp>(
        self,
        shutdown: watch::Receiver<bool>,
        l4_handler: L4H,
        l7_handler: L7H,
        udp_handler: UdpH,
    ) -> Result<()>
    where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(L7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
        UdpH: Fn(String, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
        FutUdp: std::future::Future<Output = ()> + Send + 'static,
    {
        tracing::info!(
            tcp_listeners = self.tcp_listeners.len(),
            udp_listeners = self.udp_listeners.len(),
            "Starting Velda Traffic Engine"
        );

        let mut tasks = JoinSet::new();

        // 1. Spawn concurrent accept loops for all configured TCP listeners
        for ingress in self.tcp_listeners {
            let ingress = Arc::new(ingress);
            let mut listener_shutdown = shutdown.clone();
            let l4_fn = l4_handler.clone();
            let l7_fn = l7_handler.clone();

            tasks.spawn(async move {
                let local_addr = ingress.local_addr();
                tracing::info!(
                    listener_id = %ingress.id(),
                    listen_addr = %local_addr,
                    path = %ingress.path(),
                    "TCP ingress loop running"
                );

                loop {
                    if *listener_shutdown.borrow() {
                        break;
                    }

                    tokio::select! {
                        _ = listener_shutdown.changed() => {
                            if *listener_shutdown.borrow() {
                                tracing::info!(
                                    listener_id = %ingress.id(),
                                    listen_addr = %local_addr,
                                    "TCP ingress loop shutting down"
                                );
                                break;
                            }
                        }
                        res = ingress.accept() => {
                            match res {
                                Ok((conn, path)) => {
                                    tracing::debug!(
                                        listener_id = %ingress.id(),
                                        conn_id = %conn.id(),
                                        path = %path,
                                        peer = %conn.peer(),
                                        "Ingress accepted connection"
                                    );
                                    match path {
                                        PathKind::L4Direct => {
                                            tokio::spawn(l4_fn(conn));
                                        }
                                        PathKind::Tls | PathKind::Http => {
                                            let handoff = L7Handoff::new(
                                                conn,
                                                path,
                                                ingress.binding().id.clone(),
                                                ingress.binding().tls_profile.clone(),
                                            );
                                            tokio::spawn(l7_fn(handoff));
                                        }
                                        PathKind::Unknown => {
                                            tokio::spawn(l4_fn(conn));
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::error!(
                                        listener_id = %ingress.id(),
                                        listen_addr = %local_addr,
                                        error = %err,
                                        "TCP accept error"
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        // 2. Spawn concurrent receive loops for all configured UDP listeners
        for (id, socket) in self.udp_listeners {
            let mut udp_shutdown = shutdown.clone();
            let udp_fn = udp_handler.clone();

            tasks.spawn(async move {
                let local_addr = socket.local_addr();
                tracing::info!(
                    listener_id = %id,
                    listen_addr = %local_addr,
                    "UDP ingress loop running"
                );

                let mut buf = [0u8; 65535];

                loop {
                    if *udp_shutdown.borrow() {
                        break;
                    }

                    tokio::select! {
                        _ = udp_shutdown.changed() => {
                            if *udp_shutdown.borrow() {
                                tracing::info!(
                                    listener_id = %id,
                                    listen_addr = %local_addr,
                                    "UDP ingress loop shutting down"
                                );
                                break;
                            }
                        }
                        res = socket.recv_from(&mut buf) => {
                            match res {
                                Ok((n, peer)) => {
                                    let dgram = Datagram::new(peer, local_addr, buf[..n].to_vec());
                                    let sock_clone = Arc::clone(&socket);
                                    let id_clone = id.clone();
                                    tokio::spawn(udp_fn(id_clone, sock_clone, dgram));
                                }
                                Err(err) => {
                                    tracing::error!(
                                        listener_id = %id,
                                        listen_addr = %local_addr,
                                        error = %err,
                                        "UDP recv error"
                                    );
                                }
                            }
                        }
                    }
                }
            });
        }

        // Wait for all listener loops across all ports to terminate cleanly upon shutdown
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                tracing::error!(error = %e, "Listener task encountered join error");
            }
        }

        tracing::info!("Velda Traffic Engine stopped gracefully across all ports");
        Ok(())
    }
}
