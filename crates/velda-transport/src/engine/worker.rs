//! Single-core worker ingress engine driving a dedicated shard of active listeners.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::connection::Connection;
use crate::error::Result;
use crate::ingress::{IngressBinding, TcpIngress, UdpIngress};
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

/// Dedicated single-core worker ingress engine.
///
/// Owns shard `worker_id` of all configured listeners, executing all socket accept loops,
/// datagram receive loops, and request dispatches on a dedicated, core-pinned Tokio current_thread runtime.
pub struct WorkerEngine {
    worker_id: usize,
    total_workers: usize,
    reconcile_rx: mpsc::Receiver<Vec<IngressBinding>>,
}

impl WorkerEngine {
    /// Creates a new worker engine for the given worker ID.
    pub fn new(
        worker_id: usize,
        total_workers: usize,
        reconcile_rx: mpsc::Receiver<Vec<IngressBinding>>,
    ) -> Self {
        Self {
            worker_id,
            total_workers,
            reconcile_rx,
        }
    }

    /// Returns the worker ID (shard index).
    #[inline]
    pub fn worker_id(&self) -> usize {
        self.worker_id
    }

    /// Returns the total configured worker count.
    #[inline]
    pub fn total_workers(&self) -> usize {
        self.total_workers
    }

    /// Runs this worker's shard engine until `shutdown` is signaled.
    pub async fn run<TcpD, UdpD, TcpH, UdpH, FutTcp, FutUdp>(
        mut self,
        initial_bindings: Vec<IngressBinding>,
        resolved_publisher: Arc<watch::Sender<Option<HashMap<String, std::net::SocketAddr>>>>,
        mut resolved_rx: watch::Receiver<Option<HashMap<String, std::net::SocketAddr>>>,
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
        let mut tasks = JoinSet::new();
        let mut active_listeners: HashMap<String, (IngressBinding, watch::Sender<bool>)> =
            HashMap::new();

        // 1. Initial listener bind:
        // Worker 0 determines actual ephemeral ports when port == 0, publishing them via watch channel.
        // Worker 1..N-1 await the resolved map and bind to the exact same port with SO_REUSEPORT.
        let overrides: HashMap<String, std::net::SocketAddr> = if self.worker_id == 0 {
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
                                    "Worker 0 failed to bind initial TCP listener"
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
                                    "Worker 0 failed to bind initial UDP listener"
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
                        match TcpIngress::bind_shard(
                            tcp_binding.clone(),
                            self.worker_id,
                            addr_override,
                        ) {
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
                                    worker = self.worker_id,
                                    listener_id = %id,
                                    error = %err,
                                    "Worker failed to bind initial TCP listener shard"
                                );
                            }
                        }
                    }
                    IngressBinding::Udp(udp_binding) => {
                        match UdpIngress::bind_shard(
                            udp_binding.clone(),
                            self.worker_id,
                            addr_override,
                        ) {
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
                                    worker = self.worker_id,
                                    listener_id = %id,
                                    error = %err,
                                    "Worker failed to bind initial UDP listener shard"
                                );
                            }
                        }
                    }
                }
            }
            addrs.clone()
        };

        tracing::info!(
            worker = self.worker_id,
            active = active_listeners.len(),
            "Worker ingress engine initialized and serving"
        );

        // 2. Worker event loop: Handles local task lifecycle, declarative reconciliations, and shutdown
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!(
                            worker = self.worker_id,
                            "Worker received shutdown; closing listener sockets"
                        );
                        for (_, tx) in active_listeners.values() {
                            let _ = tx.send(true);
                        }
                        break;
                    }
                }
                Some(desired) = self.reconcile_rx.recv() => {
                    self.reconcile_shard(
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
                            worker = self.worker_id,
                            error = %e,
                            "Worker listener task encountered join error"
                        );
                    }
                }
            }
        }

        // Wait for all local listener tasks to cleanly exit
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                tracing::error!(
                    worker = self.worker_id,
                    error = %e,
                    "Worker listener task error during shutdown"
                );
            }
        }

        tracing::info!(
            worker = self.worker_id,
            "Worker ingress engine stopped gracefully"
        );
        Ok(())
    }

    /// Reconciles desired bindings against this worker's active listeners.
    fn reconcile_shard<TcpD, UdpD, TcpH, UdpH, FutTcp, FutUdp>(
        &self,
        active: &mut HashMap<String, (IngressBinding, watch::Sender<bool>)>,
        desired: Vec<IngressBinding>,
        tasks: &mut JoinSet<()>,
        overrides: &HashMap<String, std::net::SocketAddr>,
        tcp_dispatcher: &TcpD,
        udp_dispatcher: &UdpD,
    ) where
        TcpD: Fn(&str) -> TcpH + Send + Sync + Clone + 'static,
        TcpH: Fn(Connection) -> FutTcp + Send + Sync + Clone + 'static,
        FutTcp: std::future::Future<Output = ()> + Send + 'static,
        UdpD: Fn(&str) -> UdpH + Send + Sync + Clone + 'static,
        UdpH: Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
        FutUdp: std::future::Future<Output = ()> + Send + 'static,
    {
        let mut desired_map: HashMap<String, IngressBinding> =
            HashMap::with_capacity(desired.len());
        for b in desired {
            desired_map.insert(b.id().to_owned(), b);
        }

        // 1. Detect and close removed or modified listeners
        active.retain(|id, (current_binding, tx)| {
            let should_keep = match desired_map.get(id) {
                Some(new_binding) => new_binding == current_binding,
                None => false,
            };
            if !should_keep {
                tracing::info!(
                    worker = self.worker_id,
                    listener_id = %id,
                    "Worker Reconcile: Closing obsolete listener shard"
                );
                let _ = tx.send(true);
                false
            } else {
                true
            }
        });

        // 2. Bind new listeners
        for (id, binding) in desired_map {
            if let Entry::Vacant(e) = active.entry(id.clone()) {
                let addr_override = overrides.get(&id).copied();
                match binding {
                    IngressBinding::Tcp(ref tcp_binding) => {
                        match TcpIngress::bind_shard(
                            tcp_binding.clone(),
                            self.worker_id,
                            addr_override,
                        ) {
                            Ok(ingress) => {
                                tracing::info!(
                                    worker = self.worker_id,
                                    listener_id = %id,
                                    addr = %ingress.local_addr(),
                                    "Worker Reconcile: Bound new TCP listener shard"
                                );
                                let (tx, rx) = watch::channel(false);
                                let handler = tcp_dispatcher(&id);
                                TcpIngress::spawn_accept_loop(
                                    Arc::new(ingress),
                                    tasks,
                                    rx,
                                    handler,
                                );
                                e.insert((binding, tx));
                            }
                            Err(err) => {
                                tracing::error!(
                                    worker = self.worker_id,
                                    listener_id = %id,
                                    error = %err,
                                    "Worker Reconcile: Failed to bind TCP listener shard"
                                );
                            }
                        }
                    }
                    IngressBinding::Udp(ref udp_binding) => {
                        match UdpIngress::bind_shard(
                            udp_binding.clone(),
                            self.worker_id,
                            addr_override,
                        ) {
                            Ok(ingress) => {
                                tracing::info!(
                                    worker = self.worker_id,
                                    listener_id = %id,
                                    addr = %ingress.local_addr(),
                                    "Worker Reconcile: Bound new UDP socket shard"
                                );
                                let (tx, rx) = watch::channel(false);
                                let handler = udp_dispatcher(&id);
                                UdpIngress::spawn_receive_loop(
                                    Arc::new(ingress),
                                    tasks,
                                    rx,
                                    handler,
                                );
                                e.insert((binding, tx));
                            }
                            Err(err) => {
                                tracing::error!(
                                    worker = self.worker_id,
                                    listener_id = %id,
                                    error = %err,
                                    "Worker Reconcile: Failed to bind UDP socket shard"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
