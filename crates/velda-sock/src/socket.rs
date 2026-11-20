//! Host Socket and Network Listener Management (`socket`).
//!
//! Provides the process-wide Single Source of Truth for all listening network sockets
//! opened across the host (Data Plane Traffic, Shadow Logistics Services, and Gateway Management).
//!
//! # Architecture & Port Isolation Mechanism
//!
//! ```text
//! ╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
//! ║                          VELDA EDGE — GLOBAL SOCKET MANAGER & PORT ISOLATION MECHANISM                           ║
//! ║                                  (crates/velda-sock/src/socket.rs: GlobalSocketManager)                          ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!                                          ┌───────────────────────────────────┐
//!                                          │       BOOTSTRAP & CONFIG          │
//!                                          │ (Config / RuntimeSnapshot Loaded) │
//!                                          └─────────────────┬─────────────────┘
//!                                                            │
//!                                                            ▼
//!                              ┌───────────────────────────────────────────────────────────┐
//!                              │       GlobalSocketManager (crates/velda-sock)             │
//!                              │   RwLock<Vec<SocketClaim>>: Process-Wide Conflict Registry│
//!                              └─────────────┬───────────────────────────────┬─────────────┘
//!                                            │                               │
//!                       [SocketRole::Traffic]│                               │[SocketRole::Shadow]
//!                       (Traffic: :80, :443) │                               │(Admin/Probe: :9091)
//!                                            │                               │
//!                                            ▼                               ▼
//!                       ┌───────────────────────────┐  atomic check  ┌───────────────────────────┐
//!                       │    register_claim()       │ ◄────────────► │    acquire_shadow_tcp()   │
//!                       │ Ingress Port Verification │  anti-conflict │ Socket Binding & Claim    │
//!                       └─────────────┬─────────────┘                └─────────────┬─────────────┘
//!                                     │                                            │
//!                                     │ SO_REUSEADDR | SO_REUSEPORT                │ SO_REUSEADDR | SO_REUSEPORT
//!                                     │ bind() + listen(4096)                      │ bind() + listen(1024)
//!                                     ▼                                            ▼
//!                       ┌───────────────────────────┐                ┌───────────────────────────┐
//!                       │   std::net::TcpListener   │                │   std::net::TcpListener   │
//!                       │  (Ingress Traffic Handle) │                │  (Shadow Dedicated Handle)│
//!                       └─────────────┬─────────────┘                └─────────────┬─────────────┘
//!                                     │                                            │
//!                   Handoff Ownership to IngressEngine              Handoff Ownership to BackgroundPlugin
//! ```

use std::sync::{OnceLock, RwLock};

use velda_core::lut::ip::is_v4_u128;

use crate::connect::{format_raw_addr, raw_to_socket2_addr, socket_addr_to_raw};

/// Operational role of a listening socket opened on the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SocketRole {
    /// Data Plane Traffic Ingress: Pinned to CPU cores, multi-worker SO_REUSEPORT (HTTP/1, HTTP/2, HTTP/3, gRPC, Raw).
    Traffic,
    /// Shadow Service: Single-listener TCP running on shadow worker thread (e.g. Prometheus Exporter, Health Probes).
    Shadow,
    /// Gateway Control / Management (e.g. UDS IPC Socket).
    Management,
}

impl std::fmt::Display for SocketRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Traffic => write!(f, "Traffic"),
            Self::Shadow => write!(f, "Shadow"),
            Self::Management => write!(f, "Management"),
        }
    }
}

/// Transport protocol for a socket binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SocketProtocol {
    Tcp,
    Udp,
    Uds,
}

impl std::fmt::Display for SocketProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp => write!(f, "TCP"),
            Self::Udp => write!(f, "UDP"),
            Self::Uds => write!(f, "UDS"),
        }
    }
}

/// Declarative claim representing an active or planned socket binding on the host.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SocketClaim {
    /// Transport protocol (TCP, UDP, UDS).
    pub protocol: SocketProtocol,
    /// Bound integer address `(ip: u128, port_be: u16)` (for TCP/UDP).
    pub addr: Option<(u128, u16)>,
    /// Path for UDS socket (for Unix Domain Sockets).
    pub path: Option<String>,
    /// Operational role of this socket.
    pub role: SocketRole,
    /// Identifier of the claiming entity (e.g. listener ID, background ID, "gateway_ipc").
    pub owner_id: String,
}

/// Error raised during socket management operations (collision, bind failure).
#[derive(Debug)]
pub enum SocketError {
    /// Two socket claims collide on the host.
    Conflict {
        existing_owner: String,
        existing_role: SocketRole,
        conflicting_owner: String,
        conflicting_role: SocketRole,
        binding: String,
    },
    /// Low-level OS I/O failure when binding the socket.
    Io(std::io::Error),
}

impl std::fmt::Display for SocketError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict {
                existing_owner,
                existing_role,
                conflicting_owner,
                conflicting_role,
                binding,
            } => {
                write!(
                    f,
                    "Socket conflict on {binding}: already claimed by {existing_owner} ({existing_role}) when claimed by {conflicting_owner} ({conflicting_role})"
                )
            }
            Self::Io(err) => write!(f, "Socket I/O error: {err}"),
        }
    }
}

impl std::error::Error for SocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SocketError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// Central host socket and port authority tracking and allocating all network listeners.
#[derive(Debug, Default)]
pub struct SocketManager {
    claims: RwLock<Vec<SocketClaim>>,
}

static GLOBAL_SOCKET_MANAGER: OnceLock<SocketManager> = OnceLock::new();

/// Returns the process-wide central SocketManager instance.
pub fn global_socket_manager() -> &'static SocketManager {
    GLOBAL_SOCKET_MANAGER.get_or_init(SocketManager::new)
}

impl SocketManager {
    /// Creates an empty socket manager.
    pub fn new() -> Self {
        Self {
            claims: RwLock::new(Vec::new()),
        }
    }

    /// Registers a Data Plane Traffic Ingress socket claim.
    pub fn register_traffic_socket(
        &self,
        ip: u128,
        port_be: u16,
        protocol: SocketProtocol,
        listener_id: impl Into<String>,
    ) -> Result<(), SocketError> {
        let listener_id = listener_id.into();
        let claim = SocketClaim {
            protocol,
            addr: Some((ip, port_be)),
            path: None,
            role: SocketRole::Traffic,
            owner_id: listener_id,
        };
        self.register_claim(claim)
    }

    /// Registers a Gateway Management Unix Domain Socket (UDS) claim.
    pub fn register_management_uds(
        &self,
        path: impl Into<String>,
        management_id: impl Into<String>,
    ) -> Result<(), SocketError> {
        let claim = SocketClaim {
            protocol: SocketProtocol::Uds,
            addr: None,
            path: Some(path.into()),
            role: SocketRole::Management,
            owner_id: management_id.into(),
        };
        self.register_claim(claim)
    }

    /// Atomically validates conflicts, performs OS `bind()`, and records a shadow TCP listener.
    ///
    /// Returns the standard library [`std::net::TcpListener`] ready for non-blocking wrapping.
    pub fn acquire_shadow_tcp(
        &self,
        ip: u128,
        port_be: u16,
        shadow_id: impl Into<String>,
    ) -> Result<std::net::TcpListener, SocketError> {
        let shadow_id = shadow_id.into();
        let mut claims = self.claims.write().unwrap();

        // Check for conflicts with existing claims
        for existing in claims.iter() {
            if existing.protocol == SocketProtocol::Tcp
                && existing.addr == Some((ip, port_be))
                && existing.owner_id != shadow_id
            {
                return Err(SocketError::Conflict {
                    existing_owner: existing.owner_id.clone(),
                    existing_role: existing.role,
                    conflicting_owner: shadow_id,
                    conflicting_role: SocketRole::Shadow,
                    binding: format_raw_addr(ip, port_be),
                });
            }
        }

        // Perform atomic OS bind with SO_REUSEADDR and SO_REUSEPORT
        let domain = if is_v4_u128(ip) {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::STREAM, None).map_err(SocketError::Io)?;
        socket.set_reuse_address(true).map_err(SocketError::Io)?;
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        unsafe {
            use std::os::unix::io::AsRawFd;
            let opt: libc::c_int = 1;
            let _ = libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &opt as *const _ as *const libc::c_void,
                std::mem::size_of_val(&opt) as libc::socklen_t,
            );
        }
        let s2_addr = raw_to_socket2_addr(ip, port_be, if is_v4_u128(ip) { 0 } else { 1 });
        socket.bind(&s2_addr).map_err(SocketError::Io)?;
        socket.listen(1024).map_err(SocketError::Io)?;
        let listener: std::net::TcpListener = socket.into();

        // Record or update claim
        if let Some(pos) = claims.iter().position(|c| {
            c.protocol == SocketProtocol::Tcp
                && c.addr == Some((ip, port_be))
                && c.owner_id == shadow_id
        }) {
            claims[pos].role = SocketRole::Shadow;
        } else {
            claims.push(SocketClaim {
                protocol: SocketProtocol::Tcp,
                addr: Some((ip, port_be)),
                path: None,
                role: SocketRole::Shadow,
                owner_id: shadow_id,
            });
        }

        Ok(listener)
    }

    /// Releases a shadow socket reservation by identifier.
    pub fn release_shadow_tcp(&self, shadow_id: &str) {
        if let Ok(mut claims) = self.claims.write() {
            claims.retain(|c| {
                !(c.role == SocketRole::Shadow
                    && c.owner_id == shadow_id
                    && c.protocol == SocketProtocol::Tcp)
            });
        }
    }

    /// Binds an OS UDP socket for a shadow logistics service, verifying against port collisions.
    pub fn acquire_shadow_udp(
        &self,
        ip: u128,
        port_be: u16,
        shadow_id: impl Into<String>,
    ) -> Result<std::net::UdpSocket, SocketError> {
        let shadow_id = shadow_id.into();
        let mut claims = self.claims.write().unwrap();

        // Check for conflicts with existing UDP claims if explicit non-zero port
        if port_be != 0 {
            for existing in claims.iter() {
                if existing.protocol == SocketProtocol::Udp
                    && existing.addr == Some((ip, port_be))
                    && existing.owner_id != shadow_id
                {
                    return Err(SocketError::Conflict {
                        existing_owner: existing.owner_id.clone(),
                        existing_role: existing.role,
                        conflicting_owner: shadow_id,
                        conflicting_role: SocketRole::Shadow,
                        binding: format_raw_addr(ip, port_be),
                    });
                }
            }
        }

        // Perform OS bind with SO_REUSEADDR and SO_REUSEPORT (if non-zero port)
        let domain = if is_v4_u128(ip) {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, None).map_err(SocketError::Io)?;
        socket.set_reuse_address(true).map_err(SocketError::Io)?;
        if port_be != 0 {
            #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
            unsafe {
                use std::os::unix::io::AsRawFd;
                let opt: libc::c_int = 1;
                let _ = libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    &opt as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&opt) as libc::socklen_t,
                );
            }
        }
        let s2_addr = raw_to_socket2_addr(ip, port_be, if is_v4_u128(ip) { 0 } else { 1 });
        socket.bind(&s2_addr).map_err(SocketError::Io)?;
        let std_socket: std::net::UdpSocket = socket.into();
        let (actual_ip, actual_port_be) = std_socket
            .local_addr()
            .map(socket_addr_to_raw)
            .unwrap_or((ip, port_be));

        // Record or update claim
        if let Some(pos) = claims.iter().position(|c| {
            c.protocol == SocketProtocol::Udp
                && c.addr == Some((actual_ip, actual_port_be))
                && c.owner_id == shadow_id
        }) {
            claims[pos].role = SocketRole::Shadow;
        } else {
            claims.push(SocketClaim {
                protocol: SocketProtocol::Udp,
                addr: Some((actual_ip, actual_port_be)),
                path: None,
                role: SocketRole::Shadow,
                owner_id: shadow_id,
            });
        }

        Ok(std_socket)
    }

    /// Releases a shadow UDP socket reservation by identifier.
    pub fn release_shadow_udp(&self, shadow_id: &str) {
        if let Ok(mut claims) = self.claims.write() {
            claims.retain(|c| {
                !(c.role == SocketRole::Shadow
                    && c.owner_id == shadow_id
                    && c.protocol == SocketProtocol::Udp)
            });
        }
    }

    fn register_claim(&self, claim: SocketClaim) -> Result<(), SocketError> {
        let mut claims = self.claims.write().unwrap();

        for existing in claims.iter() {
            let collision = match (claim.protocol, existing.protocol) {
                (SocketProtocol::Uds, SocketProtocol::Uds) => {
                    claim.path.is_some() && claim.path == existing.path
                }
                (p1, p2) if p1 == p2 => claim.addr.is_some() && claim.addr == existing.addr,
                _ => false,
            };

            if collision {
                if existing.owner_id == claim.owner_id {
                    continue;
                }
                let binding = claim
                    .addr
                    .map(|(ip, port_be)| format_raw_addr(ip, port_be))
                    .or_else(|| claim.path.clone())
                    .unwrap_or_else(|| "unknown".into());

                return Err(SocketError::Conflict {
                    existing_owner: existing.owner_id.clone(),
                    existing_role: existing.role,
                    conflicting_owner: claim.owner_id,
                    conflicting_role: claim.role,
                    binding,
                });
            }
        }

        claims.push(claim);
        Ok(())
    }

    /// Checks if a socket `(ip, port_be)` and protocol are already claimed.
    pub fn is_claimed(&self, ip: u128, port_be: u16, protocol: SocketProtocol) -> bool {
        self.claims
            .read()
            .unwrap()
            .iter()
            .any(|c| c.protocol == protocol && c.addr == Some((ip, port_be)))
    }

    /// Returns a point-in-time snapshot of all currently active socket claims.
    pub fn claims(&self) -> Vec<SocketClaim> {
        self.claims.read().unwrap().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_socket_manager_traffic_and_shadow() {
        let mgr = SocketManager::new();
        let loopback = velda_core::lut::ip::v4_to_u128(0x7F00_0001);
        let port1_be = 18080u16.to_be();
        let port2_be = 19090u16.to_be();

        mgr.register_traffic_socket(loopback, port1_be, SocketProtocol::Tcp, "listener-http")
            .unwrap();

        // Conflict when shadow tries to claim traffic port
        let res = mgr.acquire_shadow_tcp(loopback, port1_be, "prometheus_exporter");
        assert!(res.is_err());

        // Shadow successfully acquires distinct port
        let listener = mgr.acquire_shadow_tcp(loopback, port2_be, "prometheus_exporter");
        assert!(listener.is_ok());

        // Release works
        mgr.release_shadow_tcp("prometheus_exporter");
        assert_eq!(mgr.claims().len(), 1);
    }
}
