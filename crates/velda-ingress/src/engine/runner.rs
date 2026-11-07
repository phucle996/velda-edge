//! Ingress Engine coordinating arbitrary numbers of TCP, UDP, and XDP ingress listeners.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use super::reconcile::{reconcile_active_listeners, reconcile_active_listeners_shard};
use crate::binding::IngressBinding;
use crate::error::{IngressError, Result};
use crate::sock::{TcpIngress, UdpIngress};
use crate::xdp::XdpIngress;
use velda_sock::{Connection, Datagram, UdpSocket};

/// Controller handle allowing supervisors to submit declarative listener reconciliations
/// to the running [`IngressEngine`] or its partitioned worker engines. Keeps the internal channel type out of the public API.
#[derive(Clone, Debug)]
pub struct IngressHandle {
    reconcile_senders: Vec<mpsc::Sender<Vec<IngressBinding>>>,
}

/// Backwards compatibility alias for `IngressHandle`.
pub type EngineHandle = IngressHandle;

impl IngressHandle {
    /// Creates a handle bound to a single reconciliation sender.
    pub fn new(reconcile_tx: mpsc::Sender<Vec<IngressBinding>>) -> Self {
        Self {
            reconcile_senders: vec![reconcile_tx],
        }
    }

    /// Creates a handle broadcasting reconciliations across multiple worker senders.
    pub fn with_senders(senders: Vec<mpsc::Sender<Vec<IngressBinding>>>) -> Self {
        Self {
            reconcile_senders: senders,
        }
    }

    /// Submits a declarative list of desired ingress bindings to the running traffic engine(s).
    ///
    /// The engine automatically compares desired bindings against live sockets, binding new
    /// ports, gracefully closing removed ports, and leaving identical listeners untouched.
    pub async fn reconcile(&self, desired: Vec<IngressBinding>) -> Result<()> {
        for tx in &self.reconcile_senders {
            tx.send(desired.clone()).await.map_err(|_| {
                IngressError::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "IngressEngine has terminated",
                ))
            })?;
        }
        Ok(())
    }
}

/// High-performance edge traffic engine coordinating multi-port ingress bindings
/// and execution dispatching across any number of stateful TCP, stateless UDP, and XDP ports.
pub struct IngressEngine {
    initial_tcp: Vec<TcpIngress>,
    initial_udp: Vec<UdpIngress>,
    initial_bindings: Vec<IngressBinding>,
    reconcile_tx: mpsc::Sender<Vec<IngressBinding>>,
    reconcile_rx: mpsc::Receiver<Vec<IngressBinding>>,
}

/// Backwards compatibility alias for `IngressEngine`.
pub type TrafficEngine = IngressEngine;

impl Default for IngressEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl IngressEngine {
    /// Creates a new empty ingress engine with an integrated declarative reconciliation channel.
    pub fn new() -> Self {
        Self::with_reconcile_capacity(32)
    }

    /// Creates a new empty ingress engine with a specified reconciliation channel buffer capacity.
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

    /// Creates an ingress engine sized for the given hardware topology.
    pub fn for_topology(topo: &velda_core::hardware::HardwareTopology) -> Self {
        let cfg = super::config::EngineConfig::for_topology(topo);
        Self::with_reconcile_capacity(cfg.reconcile_channel_capacity)
    }

    /// Returns a lightweight controller handle to dynamically submit declarative listener reconciliations.
    pub fn handle(&self) -> IngressHandle {
        IngressHandle::new(self.reconcile_tx.clone())
    }

    /// Splits the IngressEngine into initial bindings, a broadcast controller handle,
    /// and pre-configured execution contexts for each shard.
    pub fn split_shards(
        self,
        shard_count: usize,
    ) -> (Vec<IngressBinding>, IngressHandle, Vec<ShardContext>) {
        let count = shard_count.max(1);
        let mut senders = Vec::with_capacity(count);
        let mut contexts = Vec::with_capacity(count);

        let (resolved_tx, resolved_rx) =
            watch::channel::<Option<HashMap<String, std::net::SocketAddr>>>(None);
        let resolved_publisher = Arc::new(resolved_tx);

        for shard_id in 0..count {
            let (tx, rx) = mpsc::channel(32);
            senders.push(tx);
            contexts.push(ShardContext {
                shard_id,
                total_shards: count,
                reconcile_rx: rx,
                resolved_publisher: Arc::clone(&resolved_publisher),
                resolved_subscriber: resolved_rx.clone(),
            });
        }

        let mut bindings = self.initial_bindings;
        for tcp in self.initial_tcp {
            bindings.push(IngressBinding::Tcp(tcp.binding().clone()));
        }
        for udp in self.initial_udp {
            bindings.push(IngressBinding::Udp(udp.binding().clone()));
        }

        let handle = IngressHandle::with_senders(senders);
        (bindings, handle, contexts)
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
    /// resolving specialized protocol handlers per listener via `tcp_dispatcher` and `udp_dispatcher`.
    pub async fn run<TcpD, UdpD, TcpH, UdpH, FutTcp, FutUdp>(
        mut self,
        mut shutdown: watch::Receiver<bool>,
        tcp_dispatcher: TcpD,
        udp_dispatcher: UdpD,
    ) -> Result<()>
    where
        TcpD: Fn(&str) -> TcpH + Send + Sync + Clone + 'static,
        TcpH: Fn(Connection) -> FutTcp + Send + Sync + Clone + 'static,
        FutTcp: std::future::Future<Output = ()> + Send + 'static,
        UdpD: Fn(&str) -> UdpH + Send + Sync + Clone + 'static,
        UdpH: Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
        FutUdp: std::future::Future<Output = ()> + Send + 'static,
    {
        tracing::info!(
            tcp_listeners = self.initial_tcp.len(),
            udp_listeners = self.initial_udp.len(),
            bindings = self.initial_bindings.len(),
            "Starting Velda Ingress Engine"
        );

        let mut tasks = JoinSet::new();
        let mut active_listeners: HashMap<String, (IngressBinding, watch::Sender<bool>)> =
            HashMap::new();

        // 1. Spawn initial pre-bound TCP listeners
        for ingress in self.initial_tcp {
            let binding = IngressBinding::Tcp(ingress.binding().clone());
            let id = binding.id().to_string();
            let (tx, rx) = watch::channel(false);
            let handler = tcp_dispatcher(&id);
            TcpIngress::spawn_accept_loop(Arc::new(ingress), &mut tasks, rx, handler);
            active_listeners.insert(id, (binding, tx));
        }

        // 2. Spawn initial pre-bound UDP listeners
        for ingress in self.initial_udp {
            let binding = IngressBinding::Udp(ingress.binding().clone());
            let id = binding.id().to_string();
            let (tx, rx) = watch::channel(false);
            let handler = udp_dispatcher(&id);
            UdpIngress::spawn_receive_loop(Arc::new(ingress), &mut tasks, rx, handler);
            active_listeners.insert(id, (binding, tx));
        }

        // 3. Reconcile any declared un-bound initial bindings
        if !self.initial_bindings.is_empty() {
            reconcile_active_listeners(
                &mut active_listeners,
                self.initial_bindings,
                &mut tasks,
                tcp_dispatcher.clone(),
                udp_dispatcher.clone(),
            );
        }

        // 4. Event loop: Handles incoming reconciliations, task lifecycle, and global shutdown
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("Ingress Engine received global shutdown; closing all listener sockets");
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
                        tcp_dispatcher.clone(),
                        udp_dispatcher.clone(),
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

        tracing::info!("Velda Ingress Engine stopped gracefully across all ports");
        Ok(())
    }
}

/// Execution context and cross-shard synchronization for an ingress shard.
pub struct ShardContext {
    pub shard_id: usize,
    pub total_shards: usize,
    pub reconcile_rx: mpsc::Receiver<Vec<IngressBinding>>,
    pub resolved_publisher: Arc<watch::Sender<Option<HashMap<String, std::net::SocketAddr>>>>,
    pub resolved_subscriber: watch::Receiver<Option<HashMap<String, std::net::SocketAddr>>>,
}

impl ShardContext {
    /// Creates a new shard context with all required coordination channels.
    pub fn new(
        shard_id: usize,
        total_shards: usize,
        reconcile_rx: mpsc::Receiver<Vec<IngressBinding>>,
        resolved_publisher: Arc<watch::Sender<Option<HashMap<String, std::net::SocketAddr>>>>,
        resolved_subscriber: watch::Receiver<Option<HashMap<String, std::net::SocketAddr>>>,
    ) -> Self {
        Self {
            shard_id,
            total_shards,
            reconcile_rx,
            resolved_publisher,
            resolved_subscriber,
        }
    }
}

/// Runs a dedicated ingress shard loop driving an assigned shard of active listeners.
///
/// Shard 0 dynamically resolves ephemeral ports and publishes them to the rest of the shards.
/// Shards 1..N-1 await the resolved ports and bind with SO_REUSEPORT.
pub async fn run_ingress_shard<TcpD, UdpD, TcpH, UdpH, FutTcp, FutUdp>(
    ctx: ShardContext,
    initial_bindings: Vec<IngressBinding>,
    mut shutdown: watch::Receiver<bool>,
    tcp_dispatcher: TcpD,
    udp_dispatcher: UdpD,
) -> Result<()>
where
    TcpD: Fn(&str) -> TcpH + Send + Sync + Clone + 'static,
    TcpH: Fn(Connection) -> FutTcp + Send + Sync + Clone + 'static,
    FutTcp: std::future::Future<Output = ()> + Send + 'static,
    UdpD: Fn(&str) -> UdpH + Send + Sync + Clone + 'static,
    UdpH: Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
    FutUdp: std::future::Future<Output = ()> + Send + 'static,
{
    let shard_id = ctx.shard_id;
    let total_shards = ctx.total_shards;
    let resolved_publisher = ctx.resolved_publisher;
    let mut resolved_rx = ctx.resolved_subscriber;
    let mut reconcile_rx = ctx.reconcile_rx;

    let mut tasks = JoinSet::new();
    let mut active_listeners: HashMap<String, (IngressBinding, watch::Sender<bool>)> =
        HashMap::new();

    // 1. Initial listener bind:
    // Shard 0 determines actual ephemeral ports when port == 0, publishing them via watch channel.
    // Shard 1..N-1 await the resolved map and bind to the exact same port with SO_REUSEPORT.
    let overrides: HashMap<String, std::net::SocketAddr> = if shard_id == 0 {
        let mut resolved = HashMap::new();
        for binding in &initial_bindings {
            let id = binding.id().to_string();
            match binding {
                IngressBinding::Tcp(tcp_binding) => {
                    match TcpIngress::bind_shard(tcp_binding.clone(), 0, None) {
                        Ok(ingress) => {
                            let local_addr = ingress.local_addr();
                            resolved.insert(id.clone(), local_addr);
                            let (tx, rx) = watch::channel(false);
                            let handler = tcp_dispatcher(&id);
                            TcpIngress::spawn_accept_loop(
                                Arc::new(ingress),
                                &mut tasks,
                                rx,
                                handler,
                            );
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %id,
                                error = %err,
                                "Shard 0 failed to bind initial TCP listener"
                            );
                        }
                    }
                }
                IngressBinding::Udp(udp_binding) => {
                    match UdpIngress::bind_shard(udp_binding.clone(), 0, None) {
                        Ok(ingress) => {
                            let local_addr = ingress.local_addr();
                            resolved.insert(id.clone(), local_addr);
                            let (tx, rx) = watch::channel(false);
                            let handler = udp_dispatcher(&id);
                            UdpIngress::spawn_receive_loop(
                                Arc::new(ingress),
                                &mut tasks,
                                rx,
                                handler,
                            );
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %id,
                                error = %err,
                                "Shard 0 failed to bind initial UDP socket"
                            );
                        }
                    }
                }
                IngressBinding::XdpTcp(tcp_binding, xdp_config) => {
                    match XdpIngress::bind_tcp_shard(
                        tcp_binding.clone(),
                        xdp_config.clone(),
                        0,
                        None,
                    ) {
                        Ok(ingress) => {
                            let (tx, rx) = watch::channel(false);
                            if let Some(tcp_comp) = ingress.tcp_companion() {
                                let local_addr = tcp_comp.local_addr();
                                resolved.insert(id.clone(), local_addr);
                                let handler = tcp_dispatcher(&id);
                                TcpIngress::spawn_accept_loop(
                                    Arc::clone(tcp_comp),
                                    &mut tasks,
                                    rx,
                                    handler,
                                );
                            }
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %id,
                                error = %err,
                                "Shard 0 failed to bind initial XDP TCP listener"
                            );
                        }
                    }
                }
                IngressBinding::XdpUdp(udp_binding, xdp_config) => {
                    match XdpIngress::bind_udp_shard(
                        udp_binding.clone(),
                        xdp_config.clone(),
                        0,
                        None,
                    ) {
                        Ok(ingress) => {
                            let (tx, rx) = watch::channel(false);
                            if let Some(udp_comp) = ingress.udp_companion() {
                                let local_addr = udp_comp.local_addr();
                                resolved.insert(id.clone(), local_addr);
                                let handler = udp_dispatcher(&id);
                                UdpIngress::spawn_receive_loop(
                                    Arc::clone(udp_comp),
                                    &mut tasks,
                                    rx,
                                    handler,
                                );
                            }
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %id,
                                error = %err,
                                "Shard 0 failed to bind initial XDP UDP socket"
                            );
                        }
                    }
                }
            }
        }
        let _ = resolved_publisher.send(Some(resolved.clone()));
        resolved
    } else {
        let addrs = if let Some(ref map) = *resolved_rx.borrow() {
            map.clone()
        } else {
            resolved_rx
                .wait_for(|opt| opt.is_some())
                .await
                .map(|r| r.as_ref().unwrap().clone())
                .unwrap_or_default()
        };
        for binding in &initial_bindings {
            let id = binding.id().to_string();
            let addr_override = addrs.get(&id).copied();
            match binding {
                IngressBinding::Tcp(tcp_binding) => {
                    match TcpIngress::bind_shard(tcp_binding.clone(), shard_id, addr_override) {
                        Ok(ingress) => {
                            let (tx, rx) = watch::channel(false);
                            let handler = tcp_dispatcher(&id);
                            TcpIngress::spawn_accept_loop(
                                Arc::new(ingress),
                                &mut tasks,
                                rx,
                                handler,
                            );
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard failed to bind initial TCP listener shard"
                            );
                        }
                    }
                }
                IngressBinding::Udp(udp_binding) => {
                    match UdpIngress::bind_shard(udp_binding.clone(), shard_id, addr_override) {
                        Ok(ingress) => {
                            let (tx, rx) = watch::channel(false);
                            let handler = udp_dispatcher(&id);
                            UdpIngress::spawn_receive_loop(
                                Arc::new(ingress),
                                &mut tasks,
                                rx,
                                handler,
                            );
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard failed to bind initial UDP socket shard"
                            );
                        }
                    }
                }
                IngressBinding::XdpTcp(tcp_binding, xdp_config) => {
                    match XdpIngress::bind_tcp_shard(
                        tcp_binding.clone(),
                        xdp_config.clone(),
                        shard_id,
                        addr_override,
                    ) {
                        Ok(ingress) => {
                            let (tx, rx) = watch::channel(false);
                            if let Some(tcp_comp) = ingress.tcp_companion() {
                                let handler = tcp_dispatcher(&id);
                                TcpIngress::spawn_accept_loop(
                                    Arc::clone(tcp_comp),
                                    &mut tasks,
                                    rx,
                                    handler,
                                );
                            }
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard failed to bind initial XDP TCP listener shard"
                            );
                        }
                    }
                }
                IngressBinding::XdpUdp(udp_binding, xdp_config) => {
                    match XdpIngress::bind_udp_shard(
                        udp_binding.clone(),
                        xdp_config.clone(),
                        shard_id,
                        addr_override,
                    ) {
                        Ok(ingress) => {
                            let (tx, rx) = watch::channel(false);
                            if let Some(udp_comp) = ingress.udp_companion() {
                                let handler = udp_dispatcher(&id);
                                UdpIngress::spawn_receive_loop(
                                    Arc::clone(udp_comp),
                                    &mut tasks,
                                    rx,
                                    handler,
                                );
                            }
                            active_listeners.insert(id, (binding.clone(), tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard failed to bind initial XDP UDP socket shard"
                            );
                        }
                    }
                }
            }
        }
        addrs.clone()
    };

    tracing::info!(
        shard = shard_id,
        total_shards,
        active = active_listeners.len(),
        "Ingress shard initialized and serving"
    );

    // 2. Shard event loop: Handles local task lifecycle, declarative reconciliations, and shutdown
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!(
                        shard = shard_id,
                        "Ingress shard received shutdown; closing listener sockets"
                    );
                    for (_, tx) in active_listeners.values() {
                        let _ = tx.send(true);
                    }
                    break;
                }
            }
            Some(desired) = reconcile_rx.recv() => {
                reconcile_active_listeners_shard(
                    shard_id,
                    &mut active_listeners,
                    desired,
                    &mut tasks,
                    &overrides,
                    &tcp_dispatcher,
                    &udp_dispatcher,
                );
            }
            Some(res) = tasks.join_next() => {
                if let Err(e) = res {
                    tracing::error!(
                        shard = shard_id,
                        error = %e,
                        "Ingress listener task encountered join error"
                    );
                }
            }
        }
    }

    // Wait for all local listener tasks to cleanly exit
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            tracing::error!(
                shard = shard_id,
                error = %e,
                "Ingress listener task error during shutdown"
            );
        }
    }

    tracing::info!(shard = shard_id, "Ingress shard stopped gracefully");
    Ok(())
}
