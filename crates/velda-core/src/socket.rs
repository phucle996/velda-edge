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
//! ║                                  (crates/velda-core/src/socket.rs: GlobalSocketManager)                          ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!                                          ┌───────────────────────────────────┐
//!                                          │       BOOTSTRAP & CONFIG          │
//!                                          │ (Config / RuntimeSnapshot Loaded) │
//!                                          └─────────────────┬─────────────────┘
//!                                                            │
//!                                                            ▼
//!                              ┌───────────────────────────────────────────────────────────┐
//!                              │       GlobalSocketManager (crates/velda-core)             │
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
//!                                     │                                            │
//!                                     ▼                                            ▼
//! ╔══════════════════════════════════════════════════════════╗ ╔══════════════════════════════════════════════════════════╗
//! ║        FRONTLINE TRAFFIC CONE (Top-Cone Ingress)         ║ ║       SHADOW LOGISTICS RUNNER (Background Daemon)    ║
//! ║        Owner: velda-ingress + velda-sock / velda-xdp     ║ ║       Owner: velda-plugin (health_probe)             ║
//! ╠══════════════════════════════════════════════════════════╣ ╠══════════════════════════════════════════════════════════╣
//! ║                                                          ║ ║                                                          ║
//! ║  • Multi-Shard Worker Distribution (run_ingress_shard)   ║ ║  • Dedicated Background Thread: velda-shadow-0           ║
//! ║  • 1:1 Core Pinning:                                     ║ ║  • Linux Scheduling Policy: SCHED_IDLE                   ║
//! ║    ├── Core 0: velda-worker-0 (SCHED_NORMAL, Priority 1) ║ ║    └── Yields 100% CPU when frontline worker needs it    ║
//! ║    └── Core 1: velda-worker-1 (SCHED_NORMAL, Priority 1) ║ ║                                                          ║
//! ║                                                          ║ ║  [ Sequential Inline Accept Loop — Zero Tokio Spawn ]    ║
//! ║  [ Hot-Path Request Forwarding Pipeline ]                ║ ║  loop {                                                  ║
//! ║  Downstream Client Request (:8080)                       ║ ║      select! {                                           ║
//! ║        │                                                 ║ ║          biased;                                         ║
//! ║        ▼                                                 ║ ║          _ = shutdown.changed() => break,                ║
//! ║  ┌───────────┐     ┌──────────┐     ┌────────────────┐   ║ ║          conn = listener.accept() => {                   ║
//! ║  │   Decode  │ ──► │  Router  │ ──► │ Upstream (8081)│   ║ ║              tokio::time::timeout(500ms, async {         ║
//! ║  └───────────┘     └──────────┘     └────────────────┘   ║ ║                  let mut buf = [0u8; 512]; // Stack      ║
//! ║                                                          ║ ║                  let n = stream.read(&mut buf).await;    ║
//! ║  Invariants:                                             ║ ║                  let resp = (self.eval_fn)(&buf[..n]);   ║
//! ║  - Zero JSON Parsing, Zero Disk I/O, Zero Dynamic Sniff  ║ ║                  stream.write_all(resp).await; // L1D    ║
//! ║  - Completely UNAWARE of and UNTOUCHED by Port 9091!     ║ ║              });                                         ║
//! ║                                                          ║ ║          }                                               ║
//! ║                                                          ║ ║      }                                                   ║
//! ║                                                          ║ ║  }                                                       ║
//! ╚══════════════════════════════════════════════════════════╝ ╚══════════════════════════════════════════════════════════╝
//!                                     ▲                                            ▲
//!                                     │                                            │
//!                                     └───────────────────┬────────────────────────┘
//!                                                         │
//!                                     ┌───────────────────┴───────────────────┐
//!                                     │      CLEAN LIFECYCLE DEALLOCATION     │
//!                                     │          (SIGTERM / Shutdown)         │
//!                                     ├───────────────────────────────────────┤
//!                                     │ 1. Broadcast shutdown via watch::rx   │
//!                                     │ 2. Shadow Worker closes listener      │
//!                                     │ 3. Invoke release_shadow_tcp(id)      │
//!                                     │    -> Returns clean port to Kernel    │
//!                                     └───────────────────────────────────────┘
//! ```

use std::net::SocketAddr;
use std::sync::{OnceLock, RwLock};

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
    /// Bound socket address (for TCP/UDP).
    pub address: Option<SocketAddr>,
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
        address: SocketAddr,
        protocol: SocketProtocol,
        listener_id: impl Into<String>,
    ) -> Result<(), SocketError> {
        let listener_id = listener_id.into();
        let claim = SocketClaim {
            protocol,
            address: Some(address),
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
            address: None,
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
        address: SocketAddr,
        shadow_id: impl Into<String>,
    ) -> Result<std::net::TcpListener, SocketError> {
        let shadow_id = shadow_id.into();
        let mut claims = self.claims.write().unwrap();

        // Check for conflicts with existing claims
        for existing in claims.iter() {
            if existing.protocol == SocketProtocol::Tcp
                && existing.address == Some(address)
                && existing.owner_id != shadow_id
            {
                return Err(SocketError::Conflict {
                    existing_owner: existing.owner_id.clone(),
                    existing_role: existing.role,
                    conflicting_owner: shadow_id,
                    conflicting_role: SocketRole::Shadow,
                    binding: address.to_string(),
                });
            }
        }

        // Perform atomic OS bind with SO_REUSEADDR and SO_REUSEPORT
        let domain = if address.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
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
        socket.bind(&address.into()).map_err(SocketError::Io)?;
        socket.listen(1024).map_err(SocketError::Io)?;
        let listener: std::net::TcpListener = socket.into();

        // Record or update claim
        if let Some(pos) = claims.iter().position(|c| {
            c.protocol == SocketProtocol::Tcp
                && c.address == Some(address)
                && c.owner_id == shadow_id
        }) {
            claims[pos].role = SocketRole::Shadow;
        } else {
            claims.push(SocketClaim {
                protocol: SocketProtocol::Tcp,
                address: Some(address),
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
        address: SocketAddr,
        shadow_id: impl Into<String>,
    ) -> Result<std::net::UdpSocket, SocketError> {
        let shadow_id = shadow_id.into();
        let mut claims = self.claims.write().unwrap();

        // Check for conflicts with existing UDP claims if explicit non-zero port
        if address.port() != 0 {
            for existing in claims.iter() {
                if existing.protocol == SocketProtocol::Udp
                    && existing.address == Some(address)
                    && existing.owner_id != shadow_id
                {
                    return Err(SocketError::Conflict {
                        existing_owner: existing.owner_id.clone(),
                        existing_role: existing.role,
                        conflicting_owner: shadow_id,
                        conflicting_role: SocketRole::Shadow,
                        binding: address.to_string(),
                    });
                }
            }
        }

        // Perform OS bind with SO_REUSEADDR and SO_REUSEPORT (if non-zero port)
        let domain = if address.is_ipv6() {
            socket2::Domain::IPV6
        } else {
            socket2::Domain::IPV4
        };
        let socket =
            socket2::Socket::new(domain, socket2::Type::DGRAM, None).map_err(SocketError::Io)?;
        socket.set_reuse_address(true).map_err(SocketError::Io)?;
        if address.port() != 0 {
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
        socket.bind(&address.into()).map_err(SocketError::Io)?;
        let std_socket: std::net::UdpSocket = socket.into();
        let actual_addr = std_socket.local_addr().unwrap_or(address);

        // Record or update claim
        if let Some(pos) = claims.iter().position(|c| {
            c.protocol == SocketProtocol::Udp
                && c.address == Some(actual_addr)
                && c.owner_id == shadow_id
        }) {
            claims[pos].role = SocketRole::Shadow;
        } else {
            claims.push(SocketClaim {
                protocol: SocketProtocol::Udp,
                address: Some(actual_addr),
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
                (p1, p2) if p1 == p2 => {
                    claim.address.is_some() && claim.address == existing.address
                }
                _ => false,
            };

            if collision {
                if existing.owner_id == claim.owner_id {
                    continue;
                }
                let binding = claim
                    .address
                    .map(|a| a.to_string())
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

    /// Checks if a socket address and protocol are already claimed.
    pub fn is_claimed(&self, address: SocketAddr, protocol: SocketProtocol) -> bool {
        self.claims
            .read()
            .unwrap()
            .iter()
            .any(|c| c.protocol == protocol && c.address == Some(address))
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
        let addr1: SocketAddr = "127.0.0.1:18080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:19090".parse().unwrap();

        mgr.register_traffic_socket(addr1, SocketProtocol::Tcp, "listener-http")
            .unwrap();

        // Conflict when shadow tries to claim traffic port
        let res = mgr.acquire_shadow_tcp(addr1, "prometheus_exporter");
        assert!(res.is_err());

        // Shadow successfully acquires distinct port
        let listener = mgr.acquire_shadow_tcp(addr2, "prometheus_exporter");
        assert!(listener.is_ok());

        // Release works
        mgr.release_shadow_tcp("prometheus_exporter");
        assert_eq!(mgr.claims().len(), 1);
    }
}
