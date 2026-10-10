//! Declarative listener reconciliation logic.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::connection::Connection;
use crate::ingress::{IngressBinding, TcpIngress, UdpIngress};
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

/// Diffs live active sockets against desired ingress bindings:
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
                IngressBinding::Tcp(ref tcp_binding) => {
                    match TcpIngress::bind(tcp_binding.clone()) {
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
                    }
                }
                IngressBinding::Udp(ref udp_binding) => {
                    match UdpIngress::bind(udp_binding.clone()) {
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
                    }
                }
            }
        }
    }
}
