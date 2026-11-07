//! Declarative listener reconciliation logic.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::sock::{TcpIngress, UdpIngress};
use crate::xdp::XdpIngress;
use velda_sock::{Connection, Datagram, UdpSocket};
use velda_xdp::XdpEngineKind;

use crate::binding::IngressBinding;

/// Diffs live active sockets against desired ingress bindings:
/// - Validates candidate configurations before touching active state (LKG preservation).
/// - Closes removed or modified listeners (releasing OS ports).
/// - Binds new or modified listeners (spawning accept / receive loops).
/// - Leaves identical listeners completely untouched.
pub fn reconcile_active_listeners<TcpD, UdpD, TcpH, UdpH, FutTcp, FutUdp>(
    active: &mut HashMap<String, (IngressBinding, watch::Sender<bool>)>,
    desired: Vec<IngressBinding>,
    tasks: &mut JoinSet<()>,
    tcp_dispatcher: TcpD,
    udp_dispatcher: UdpD,
) where
    TcpD: Fn(&str) -> TcpH + Send + Sync + Clone + 'static,
    TcpH: Fn(Connection) -> FutTcp + Send + Sync + Clone + 'static,
    FutTcp: std::future::Future<Output = ()> + Send + 'static,
    UdpD: Fn(&str) -> UdpH + Send + Sync + Clone + 'static,
    UdpH: Fn(Arc<str>, Arc<UdpSocket>, Datagram) -> FutUdp + Send + Sync + Clone + 'static,
    FutUdp: std::future::Future<Output = ()> + Send + 'static,
{
    // Pre-flight LKG qualification: reject batch if any candidate config fails hardware/driver validation
    for b in &desired {
        if let Some(xdp_cfg) = b.xdp()
            && let Err(err) = velda_xdp::probe::validate_xdp_config(xdp_cfg)
        {
            tracing::error!(
                listener_id = b.id(),
                error = %err,
                "Declarative Reconcile: Pre-flight XDP validation failed; aborting to preserve active LKG listeners"
            );
            return;
        }
    }

    let mut desired_map: HashMap<String, IngressBinding> = HashMap::with_capacity(desired.len());
    for b in desired {
        desired_map.insert(b.id().to_owned(), b);
    }

    // 1. Detect and close removed or modified listeners in-place (zero vector allocation)
    active.retain(|id, (current_binding, tx)| {
        let should_keep = match desired_map.get(id) {
            Some(new_binding) => new_binding == current_binding,
            None => false,
        };
        if !should_keep {
            tracing::info!(
                listener_id = %id,
                addr = %current_binding.addr(),
                "Declarative Reconcile: Closing obsolete/modified listener"
            );
            let _ = tx.send(true);
            false
        } else {
            true
        }
    });

    // 2. Detect and bind new or modified listeners
    for (id, binding) in desired_map {
        if let Entry::Vacant(e) = active.entry(id.clone()) {
            match binding {
                IngressBinding::Tcp(ref tcp_binding) => match TcpIngress::bind(tcp_binding.clone())
                {
                    Ok(ingress) => {
                        tracing::info!(
                            listener_id = %id,
                            addr = %tcp_binding.addr,
                            shards = ingress.shard_count(),
                            "Declarative Reconcile: Bound new TCP listener shards"
                        );
                        let (tx, rx) = watch::channel(false);
                        let handler = tcp_dispatcher(&id);
                        TcpIngress::spawn_accept_loop(Arc::new(ingress), tasks, rx, handler);
                        e.insert((binding, tx));
                    }
                    Err(err) => {
                        tracing::error!(
                            listener_id = %id,
                            addr = %tcp_binding.addr,
                            error = %err,
                            "Declarative Reconcile: Failed to bind TCP listener"
                        );
                    }
                },
                IngressBinding::Udp(ref udp_binding) => match UdpIngress::bind(udp_binding.clone())
                {
                    Ok(ingress) => {
                        tracing::info!(
                            listener_id = %id,
                            addr = %udp_binding.addr,
                            shards = ingress.shard_count(),
                            "Declarative Reconcile: Bound new UDP socket shards"
                        );
                        let (tx, rx) = watch::channel(false);
                        let handler = udp_dispatcher(&id);
                        UdpIngress::spawn_receive_loop(Arc::new(ingress), tasks, rx, handler);
                        e.insert((binding, tx));
                    }
                    Err(err) => {
                        tracing::error!(
                            listener_id = %id,
                            addr = %udp_binding.addr,
                            error = %err,
                            "Declarative Reconcile: Failed to bind UDP socket"
                        );
                    }
                },
                IngressBinding::XdpTcp(ref tcp_binding, ref xdp_cfg) => {
                    match velda_xdp::XdpListener::bind(xdp_cfg.clone()) {
                        Ok(xdp_listener) => {
                            let (tx, rx) = watch::channel(false);
                            match xdp_listener.config().engine_kind {
                                XdpEngineKind::PassFilter | XdpEngineKind::Sockmap => {
                                    match TcpIngress::bind(tcp_binding.clone()) {
                                        Ok(ingress) => {
                                            tracing::info!(
                                                listener_id = %id,
                                                addr = %tcp_binding.addr,
                                                shards = ingress.shard_count(),
                                                "Declarative Reconcile: Bound companion TCP listener for XDP"
                                            );
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
                                                listener_id = %id,
                                                addr = %tcp_binding.addr,
                                                error = %err,
                                                "Declarative Reconcile: Failed to bind companion TCP listener for XDP"
                                            );
                                        }
                                    }
                                }
                                XdpEngineKind::AfXdp => {
                                    e.insert((binding, tx));
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %id,
                                addr = %tcp_binding.addr,
                                error = %err,
                                "Declarative Reconcile: Failed to bind XDP TCP listener"
                            );
                        }
                    }
                }
                IngressBinding::XdpUdp(ref udp_binding, ref xdp_cfg) => {
                    match velda_xdp::XdpListener::bind(xdp_cfg.clone()) {
                        Ok(xdp_listener) => {
                            let (tx, rx) = watch::channel(false);
                            match xdp_listener.config().engine_kind {
                                XdpEngineKind::PassFilter | XdpEngineKind::Sockmap => {
                                    match UdpIngress::bind(udp_binding.clone()) {
                                        Ok(ingress) => {
                                            tracing::info!(
                                                listener_id = %id,
                                                addr = %udp_binding.addr,
                                                shards = ingress.shard_count(),
                                                "Declarative Reconcile: Bound companion UDP socket for XDP"
                                            );
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
                                                listener_id = %id,
                                                addr = %udp_binding.addr,
                                                error = %err,
                                                "Declarative Reconcile: Failed to bind companion UDP socket for XDP"
                                            );
                                        }
                                    }
                                }
                                XdpEngineKind::AfXdp => {
                                    e.insert((binding, tx));
                                }
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                listener_id = %id,
                                addr = %udp_binding.addr,
                                error = %err,
                                "Declarative Reconcile: Failed to bind XDP UDP socket"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Reconciles desired bindings against a single shard's active listeners.
pub fn reconcile_active_listeners_shard<TcpD, UdpD, TcpH, UdpH, FutTcp, FutUdp>(
    shard_id: usize,
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
    // Pre-flight LKG qualification: reject batch if any candidate config fails hardware/driver validation
    for b in &desired {
        if let Some(xdp_cfg) = b.xdp()
            && let Err(err) = velda_xdp::probe::validate_xdp_config(xdp_cfg)
        {
            tracing::error!(
                shard = shard_id,
                listener_id = b.id(),
                error = %err,
                "Shard Reconcile: Pre-flight XDP validation failed; aborting to preserve active LKG listeners"
            );
            return;
        }
    }

    let mut desired_map: HashMap<String, IngressBinding> = HashMap::with_capacity(desired.len());
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
                shard = shard_id,
                listener_id = %id,
                "Shard Reconcile: Closing obsolete listener shard"
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
                    match TcpIngress::bind_shard(tcp_binding.clone(), shard_id, addr_override) {
                        Ok(ingress) => {
                            tracing::info!(
                                shard = shard_id,
                                listener_id = %id,
                                addr = %ingress.local_addr(),
                                "Shard Reconcile: Bound new TCP listener shard"
                            );
                            let (tx, rx) = watch::channel(false);
                            let handler = tcp_dispatcher(&id);
                            TcpIngress::spawn_accept_loop(Arc::new(ingress), tasks, rx, handler);
                            e.insert((binding, tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard Reconcile: Failed to bind TCP listener shard"
                            );
                        }
                    }
                }
                IngressBinding::Udp(ref udp_binding) => {
                    match UdpIngress::bind_shard(udp_binding.clone(), shard_id, addr_override) {
                        Ok(ingress) => {
                            tracing::info!(
                                shard = shard_id,
                                listener_id = %id,
                                addr = %ingress.local_addr(),
                                "Shard Reconcile: Bound new UDP socket shard"
                            );
                            let (tx, rx) = watch::channel(false);
                            let handler = udp_dispatcher(&id);
                            UdpIngress::spawn_receive_loop(Arc::new(ingress), tasks, rx, handler);
                            e.insert((binding, tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard Reconcile: Failed to bind UDP socket shard"
                            );
                        }
                    }
                }
                IngressBinding::XdpTcp(ref tcp_binding, ref xdp_config) => {
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
                                    tasks,
                                    rx,
                                    handler,
                                );
                            }
                            e.insert((binding, tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard Reconcile: Failed to bind XDP TCP listener shard"
                            );
                        }
                    }
                }
                IngressBinding::XdpUdp(ref udp_binding, ref xdp_config) => {
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
                                    tasks,
                                    rx,
                                    handler,
                                );
                            }
                            e.insert((binding, tx));
                        }
                        Err(err) => {
                            tracing::error!(
                                shard = shard_id,
                                listener_id = %id,
                                error = %err,
                                "Shard Reconcile: Failed to bind XDP UDP listener shard"
                            );
                        }
                    }
                }
            }
        }
    }
}
