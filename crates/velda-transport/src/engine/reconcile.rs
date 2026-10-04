//! Declarative listener reconciliation logic.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::connection::Connection;
use crate::handoff::{TcpL7Handoff, UdpL7Handoff};
use crate::ingress::listener::{IngressBinding, IngressListener};
use crate::udp::datagram::Datagram;
use crate::udp::socket::UdpSocket;

/// Diffs live active sockets against desired ingress bindings:
/// - Closes removed or modified listeners (releasing OS ports).
/// - Binds new or modified listeners (spawning accept loops).
/// - Leaves identical listeners completely untouched.
pub fn reconcile_active_listeners<L4H, L7H, UdpL4H, UdpL7H, FutL4, FutL7, FutUdpL4, FutUdpL7>(
    active: &mut HashMap<String, (IngressBinding, watch::Sender<bool>)>,
    desired: Vec<IngressBinding>,
    tasks: &mut JoinSet<()>,
    l4_handler: L4H,
    l7_handler: L7H,
    udp_l4_handler: UdpL4H,
    udp_l7_handler: UdpL7H,
) where
    L4H: Fn(Connection) -> FutL4 + Send + Sync + Clone + 'static,
    FutL4: std::future::Future<Output = ()> + Send + 'static,
    L7H: Fn(TcpL7Handoff) -> FutL7 + Send + Sync + Clone + 'static,
    FutL7: std::future::Future<Output = ()> + Send + 'static,
    UdpL4H: Fn(String, Arc<UdpSocket>, Datagram) -> FutUdpL4 + Send + Sync + Clone + 'static,
    FutUdpL4: std::future::Future<Output = ()> + Send + 'static,
    UdpL7H: Fn(UdpL7Handoff) -> FutUdpL7 + Send + Sync + Clone + 'static,
    FutUdpL7: std::future::Future<Output = ()> + Send + 'static,
{
    let mut desired_map: HashMap<String, IngressBinding> = HashMap::with_capacity(desired.len());
    for b in desired {
        desired_map.insert(b.id.clone(), b);
    }

    // 1. Detect removed or modified listeners
    let to_remove: Vec<String> = active
        .iter()
        .filter(|(id, (current_binding, _))| match desired_map.get(*id) {
            None => true,
            Some(new_binding) => new_binding != current_binding,
        })
        .map(|(id, _)| id.clone())
        .collect();

    for id in to_remove {
        if let Some((old_binding, tx)) = active.remove(&id) {
            tracing::info!(
                listener_id = %id,
                addr = %old_binding.addr,
                "Declarative Reconcile: Closing obsolete/modified listener"
            );
            let _ = tx.send(true);
        }
    }

    // 2. Detect and bind new or modified listeners
    for (id, binding) in desired_map {
        if let Entry::Vacant(e) = active.entry(id.clone()) {
            if binding.is_udp() {
                match UdpSocket::bind_shards(binding.addr, binding.udp_config.clone()) {
                    Ok(sockets) => {
                        tracing::info!(
                            listener_id = %id,
                            addr = %binding.addr,
                            path = %binding.path,
                            shards = sockets.len(),
                            "Declarative Reconcile: Bound new UDP socket shards"
                        );
                        let (tx, rx) = watch::channel(false);
                        for socket in sockets {
                            UdpSocket::spawn_receive_loop(
                                Arc::new(socket),
                                binding.clone(),
                                tasks,
                                rx.clone(),
                                udp_l4_handler.clone(),
                                udp_l7_handler.clone(),
                            );
                        }
                        e.insert((binding, tx));
                    }
                    Err(err) => {
                        tracing::error!(
                            listener_id = %id,
                            addr = %binding.addr,
                            error = %err,
                            "Declarative Reconcile: Failed to bind UDP socket"
                        );
                    }
                }
            } else {
                match IngressListener::bind(binding.clone()) {
                    Ok(listener) => {
                        tracing::info!(
                            listener_id = %id,
                            addr = %binding.addr,
                            "Declarative Reconcile: Bound new TCP listener"
                        );
                        let (tx, rx) = watch::channel(false);
                        IngressListener::spawn_accept_loop(
                            Arc::new(listener),
                            tasks,
                            rx,
                            l4_handler.clone(),
                            l7_handler.clone(),
                        );
                        e.insert((binding, tx));
                    }
                    Err(err) => {
                        tracing::error!(
                            listener_id = %id,
                            addr = %binding.addr,
                            error = %err,
                            "Declarative Reconcile: Failed to bind TCP listener"
                        );
                    }
                }
            }
        }
    }
}
