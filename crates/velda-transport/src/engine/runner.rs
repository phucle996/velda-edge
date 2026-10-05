//! Traffic Engine coordinating arbitrary numbers of TCP and UDP ingress listeners.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use super::reconcile::reconcile_active_listeners;
use crate::connection::Connection;
use crate::error::{Result, TransportError};
use crate::ingress::{IngressBinding, TcpIngress, UdpIngress};
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

/// Controller handle allowing supervisors to submit declarative listener reconciliations
/// to the running [`TrafficEngine`]. Keeps the internal channel type out of the public API.
#[derive(Clone, Debug)]
pub struct EngineHandle {
    reconcile_tx: mpsc::Sender<Vec<IngressBinding>>,
}

impl EngineHandle {
    /// Submits a declarative list of desired ingress bindings to the running traffic engine.
    ///
    /// The engine automatically compares desired bindings against live sockets, binding new
    /// ports, gracefully closing removed ports, and leaving identical listeners untouched.
    pub async fn reconcile(&self, desired: Vec<IngressBinding>) -> Result<()> {
        self.reconcile_tx.send(desired).await.map_err(|_| {
            TransportError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "TrafficEngine has terminated",
            ))
        })
    }
}

/// High-performance edge traffic engine coordinating multi-port ingress bindings
/// and execution dispatching across any number of stateful TCP and stateless UDP ports.
pub struct TrafficEngine {
    initial_tcp: Vec<TcpIngress>,
    initial_udp: Vec<UdpIngress>,
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

    /// Registers a pre-bound TCP ingress listener directly.
    pub fn add_tcp_ingress(&mut self, ingress: TcpIngress) -> &mut Self {
        self.initial_tcp.push(ingress);
        self
    }

    /// Registers a pre-bound UDP ingress listener directly.
    pub fn add_udp_ingress(&mut self, ingress: UdpIngress) -> &mut Self {
        self.initial_udp.push(ingress);
        self
    }

    /// Registers an [`IngressBinding`].
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

    /// Starts all ingress listener loops across all configured TCP and UDP ports concurrently,
    /// dispatching stateful TCP connections to `tcp_handler` and stateless UDP datagrams to `udp_handler`
    /// until shutdown is signaled.
    pub async fn run<TcpH, UdpH, FutTcp, FutUdp>(
        mut self,
        mut shutdown: watch::Receiver<bool>,
        tcp_handler: TcpH,
        udp_handler: UdpH,
    ) -> Result<()>
    where
        TcpH: Fn(Connection) -> FutTcp + Send + Sync + Clone + 'static,
        FutTcp: std::future::Future<Output = ()> + Send + 'static,
        UdpH: Fn(String, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
        FutUdp: std::future::Future<Output = ()> + Send + 'static,
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
            let binding = IngressBinding::Tcp(ingress.binding().clone());
            let id = binding.id().to_string();
            let (tx, rx) = watch::channel(false);
            TcpIngress::spawn_accept_loop(Arc::new(ingress), &mut tasks, rx, tcp_handler.clone());
            active_listeners.insert(id, (binding, tx));
        }

        // 2. Spawn initial pre-bound UDP listeners
        for ingress in self.initial_udp {
            let binding = IngressBinding::Udp(ingress.binding().clone());
            let id = binding.id().to_string();
            let (tx, rx) = watch::channel(false);
            UdpIngress::spawn_receive_loop(Arc::new(ingress), &mut tasks, rx, udp_handler.clone());
            active_listeners.insert(id, (binding, tx));
        }

        // 3. Reconcile any declared un-bound initial bindings
        if !self.initial_bindings.is_empty() {
            reconcile_active_listeners(
                &mut active_listeners,
                self.initial_bindings,
                &mut tasks,
                tcp_handler.clone(),
                udp_handler.clone(),
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
                        tcp_handler.clone(),
                        udp_handler.clone(),
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
