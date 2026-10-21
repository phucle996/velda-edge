//! Host Socket and Network Listener Management (`socket`).
//!
//! Provides the process-wide Single Source of Truth for all listening network sockets
//! opened across the host (Data Plane Traffic, Auxiliary Services, and Gateway Management).

use std::net::SocketAddr;
use std::sync::{OnceLock, RwLock};

/// Operational role of a listening socket opened on the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SocketRole {
    /// Data Plane Traffic Ingress: Pinned to CPU cores, multi-worker SO_REUSEPORT (HTTP/1, HTTP/2, HTTP/3, gRPC, Raw).
    Traffic,
    /// Shadow Auxiliary Service: Single-listener TCP running on shadow worker thread (e.g. Prometheus Exporter, Health Probes).
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

    /// Atomically validates conflicts, performs OS `bind()`, and records an auxiliary shadow TCP listener.
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

        // Perform atomic OS bind
        let listener = std::net::TcpListener::bind(address).map_err(SocketError::Io)?;

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
            claims.retain(|c| !(c.role == SocketRole::Shadow && c.owner_id == shadow_id));
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
        let res = mgr.acquire_shadow_tcp(addr1, "prometheus");
        assert!(res.is_err());

        // Shadow successfully acquires distinct port
        let listener = mgr.acquire_shadow_tcp(addr2, "prometheus");
        assert!(listener.is_ok());

        // Release works
        mgr.release_shadow_tcp("prometheus");
        assert_eq!(mgr.claims().len(), 1);
    }
}
