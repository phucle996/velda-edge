//! Traffic Engine coordinating arbitrary numbers of TCP and UDP ingress listeners.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use super::handle::EngineHandle;
use super::reconcile::reconcile_active_listeners;
use crate::connection::Connection;
use crate::error::Result;
use crate::handoff::{TcpL7Handoff, UdpL7Handoff};
use crate::ingress::classifier::PathKind;
use crate::ingress::listener::{IngressBinding, IngressListener};
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

/// High-performance edge traffic engine coordinating multi-port ingress bindings,
/// classification, and execution dispatching across any number of TCP and UDP ports.
pub struct TrafficEngine {
    initial_tcp: Vec<IngressListener>,
    initial_udp: Vec<(String, Arc<UdpSocket>, IngressBinding)>,
    initial_bindings: Vec<IngressBinding>,
    reconcile_tx: mpsc::Sender<Vec<IngressBinding>>,
    reconcile_rx: mpsc::Receiver<Vec<IngressBinding>>,
}

impl Default for TrafficEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl TrafficEngine {
    /// Creates a new empty traffic engine with an integrated declarative reconciliation channel.
    pub fn new() -> Self {
        Self::with_reconcile_capacity(32)
    }

    /// Creates a new empty traffic engine with a specified reconciliation channel buffer capacity.
    pub fn with_reconcile_capacity(capacity: usize) -> Self {
        let (reconcile_tx, reconcile_rx) = mpsc::channel(capacity.max(1));
        Self {
            initial_tcp: Vec::new(),
            initial_udp: Vec::new(),
            initial_bindings: Vec::new(),
            reconcile_tx,
            reconcile_rx,
        }
    }

    /// Creates a traffic engine sized for the given hardware CPU and memory tiers.
    pub fn for_tiers(
        cpu: velda_core::hardware::CpuTier,
        mem: velda_core::hardware::MemoryTier,
    ) -> Self {
        let cfg = super::config::EngineConfig::for_tiers(cpu, mem);
        Self::with_reconcile_capacity(cfg.reconcile_channel_capacity)
    }

    /// Returns a lightweight controller handle to dynamically submit declarative listener reconciliations.
    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            reconcile_tx: self.reconcile_tx.clone(),
        }
    }

    /// Registers a TCP ingress listener directly.
    pub fn add_tcp_listener(&mut self, listener: IngressListener) -> &mut Self {
        self.initial_tcp.push(listener);
        self
    }

    /// Registers a UDP socket directly with a listener identifier.
    pub fn add_udp_socket(&mut self, id: impl Into<String>, socket: UdpSocket) -> &mut Self {
        let id_str = id.into();
        let addr = socket.local_addr();
        let binding = IngressBinding {
            id: id_str.clone(),
            addr,
            protocol: "udp".into(),
            tls_enabled: false,
            path: PathKind::L4Direct,
            tcp_config: Default::default(),
            udp_config: socket.config().clone(),
        };
        self.initial_udp.push((id_str, Arc::new(socket), binding));
        self
    }

    /// Binds and registers an [`IngressBinding`].
    pub fn add_binding(&mut self, binding: IngressBinding) -> Result<()> {
        self.initial_bindings.push(binding);
        Ok(())
    }

    /// Returns the number of initial TCP bindings configured.
    #[inline]
    pub fn tcp_listener_count(&self) -> usize {
        self.initial_tcp.len() + self.initial_bindings.iter().filter(|b| b.is_tcp()).count()
    }

    /// Returns the number of initial UDP bindings configured.
    #[inline]
    pub fn udp_listener_count(&self) -> usize {
        self.initial_udp.len() + self.initial_bindings.iter().filter(|b| b.is_udp()).count()
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
        L7H: Fn(TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
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
    /// forwarding raw UDP traffic to `udp_handler`.
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
        L7H: Fn(TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
        UdpH: Fn(String, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
        FutUdp: std::future::Future<Output = ()> + Send + 'static,
    {
        self.run_all(
            shutdown,
            l4_handler,
            l7_handler,
            udp_handler,
            |_handoff: UdpL7Handoff| async {},
        )
        .await
    }

    /// Starts all ingress loops across all configured TCP and UDP ports concurrently,
    /// with dedicated handlers for TCP L4, TCP L7, UDP L4, and UDP L7 (HTTP/3 / QUIC).
    pub async fn run_all<L4H, L7H, UdpL4H, UdpL7H, FutL4, FutL7, FutUdpL4, FutUdpL7>(
        mut self,
        mut shutdown: watch::Receiver<bool>,
        l4_handler: L4H,
        l7_handler: L7H,
        udp_l4_handler: UdpL4H,
        udp_l7_handler: UdpL7H,
    ) -> Result<()>
    where
        L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
        FutL4: std::future::Future<Output = ()> + Send + 'static,
        L7H: Fn(TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
        FutL7: std::future::Future<Output = ()> + Send + 'static,
        UdpL4H: Fn(String, Arc<UdpSocket>, Datagram) -> FutUdpL4 + Send + Sync + Clone + 'static,
        FutUdpL4: std::future::Future<Output = ()> + Send + 'static,
        UdpL7H: Fn(UdpL7Handoff) -> FutUdpL7 + Send + Sync + Clone + 'static,
        FutUdpL7: std::future::Future<Output = ()> + Send + 'static,
    {
        tracing::info!(
            tcp_listeners = self.initial_tcp.len(),
            udp_listeners = self.initial_udp.len(),
            bindings = self.initial_bindings.len(),
            "Starting Velda Traffic Engine"
        );

        let mut tasks = JoinSet::new();
        let mut active_listeners: HashMap<String, (IngressBinding, watch::Sender<bool>)> =
            HashMap::new();

        // 1. Spawn initial pre-bound TCP listeners
        for ingress in self.initial_tcp {
            let binding = ingress.binding().clone();
            let id = binding.id.clone();
            let (tx, rx) = watch::channel(false);
            IngressListener::spawn_accept_loop(
                Arc::new(ingress),
                &mut tasks,
                rx,
                l4_handler.clone(),
                l7_handler.clone(),
            );
            active_listeners.insert(id, (binding, tx));
        }

        // 2. Spawn initial pre-bound UDP listeners
        for (_id, socket, binding) in self.initial_udp {
            let (tx, rx) = watch::channel(false);
            UdpSocket::spawn_receive_loop(
                socket,
                binding.clone(),
                &mut tasks,
                rx,
                udp_l4_handler.clone(),
                udp_l7_handler.clone(),
            );
            active_listeners.insert(binding.id.clone(), (binding, tx));
        }

        // 3. Reconcile any declared un-bound initial bindings
        if !self.initial_bindings.is_empty() {
            reconcile_active_listeners(
                &mut active_listeners,
                self.initial_bindings,
                &mut tasks,
                l4_handler.clone(),
                l7_handler.clone(),
                udp_l4_handler.clone(),
                udp_l7_handler.clone(),
            );
        }

        // 4. Event loop: Handles incoming reconciliations, task lifecycle, and global shutdown
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("Traffic Engine received global shutdown; closing all listener sockets");
                        for (_, tx) in active_listeners.values() {
                            let _ = tx.send(true);
                        }
                        break;
                    }
                }
                Some(desired) = self.reconcile_rx.recv() => {
                    reconcile_active_listeners(
                        &mut active_listeners,
                        desired,
                        &mut tasks,
                        l4_handler.clone(),
                        l7_handler.clone(),
                        udp_l4_handler.clone(),
                        udp_l7_handler.clone(),
                    );
                }
                Some(res) = tasks.join_next() => {
                    if let Err(e) = res {
                        tracing::error!(error = %e, "Listener task encountered join error");
                    }
                }
            }
        }

        // Wait for all listener tasks to cleanly exit
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                tracing::error!(error = %e, "Listener task encountered error during shutdown");
            }
        }

        tracing::info!("Velda Traffic Engine stopped gracefully across all ports");
        Ok(())
    }
}
