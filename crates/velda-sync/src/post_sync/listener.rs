use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::SyncError;

// ============================================================================
// Binary Protocol Constants & Domain Header
// ============================================================================

pub const MAGIC_BYTES: [u8; 4] = *b"VLDA";
pub const CURRENT_FORMAT_VERSION: u32 = 1;
pub const CURRENT_COMPILER_VERSION: u32 = 1;
pub const TAG_LISTENERS: [u8; 8] = *b"LISTENER";

/// Self-describing binary header prepended to listeners.bin.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainHeader {
    pub magic: [u8; 4],
    pub format_version: u32,
    pub compiler_version: u32,
    pub schema_version: u32,
    pub domain_tag: [u8; 8],
    pub revision: u64,
    pub source_checksum: [u8; 32],
    pub payload_size: u64,
    pub payload_checksum: [u8; 32],
}

pub const DOMAIN_HEADER_SIZE: usize = std::mem::size_of::<DomainHeader>();

// ============================================================================
// Phase 1: Entity & Schema Definitions
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenersFile {
    pub schema_version: u32,
    pub listeners: Vec<ListenerConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerTransportConfig {
    pub protocol: String, // "tcp", "udp"
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerApplicationConfig {
    pub protocol: String, // "raw", "http1", "http2", "http3", "grpc"
    #[serde(default)]
    pub version: Option<String>,
    pub streaming: velda_core::StreamingMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Http1ListenerConfig {
    #[serde(default)]
    pub max_body_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_keepalive_requests: Option<u32>,
    #[serde(default)]
    pub header_read_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Http2ListenerConfig {
    #[serde(default)]
    pub max_body_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub initial_connection_window_size: Option<u32>,
    #[serde(default)]
    pub initial_stream_window_size: Option<u32>,
    #[serde(default)]
    pub max_frame_size: Option<u32>,
    #[serde(default)]
    pub enable_push: Option<bool>,
    #[serde(default)]
    pub max_consecutive_resets: Option<u32>,
    #[serde(default)]
    pub max_pending_control_frames: Option<u32>,
    #[serde(default)]
    pub max_continuation_frames: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GrpcListenerConfig {
    #[serde(default)]
    pub max_message_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub keepalive_ping_interval_ms: Option<u64>,
    #[serde(default)]
    pub keepalive_ping_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_call_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Http3ListenerConfig {
    #[serde(default)]
    pub max_body_size: Option<usize>,
    #[serde(default)]
    pub max_header_size: Option<usize>,
    #[serde(default)]
    pub max_headers: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    #[serde(default)]
    pub max_concurrent_streams: Option<u32>,
    #[serde(default)]
    pub max_concurrent_uni_streams: Option<u32>,
    #[serde(default)]
    pub max_qpack_table_capacity: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RawListenerConfig {
    #[serde(default)]
    pub copy_buffer_size: Option<usize>,
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListenerConfig {
    pub id: String,
    pub address: String, // e.g. "0.0.0.0:80" or "0.0.0.0:443"
    pub transport: ListenerTransportConfig,
    pub application: ListenerApplicationConfig,
    pub tls: ListenerTlsConfig,
    #[serde(default)]
    pub http1: Option<Http1ListenerConfig>,
    #[serde(default)]
    pub http2: Option<Http2ListenerConfig>,
    #[serde(default)]
    pub grpc: Option<GrpcListenerConfig>,
    #[serde(default)]
    pub http3: Option<Http3ListenerConfig>,
    #[serde(default)]
    pub raw: Option<RawListenerConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListenerTlsConfig {
    pub enabled: bool,
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into listener configurations.
pub fn parse_listeners(payload: &[u8]) -> Result<Vec<ListenerConfig>, SyncError> {
    let file: ListenersFile =
        serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
            domain: "listeners".into(),
            reason: e.to_string(),
        })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.listeners)
}

// ============================================================================
// Phase 3: Integrity Check & Semantic Validation (Inline Port Conflicts)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum L4Protocol {
    Tcp,
    Udp,
}

#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

#[inline]
fn ip_addresses_overlap(a: std::net::IpAddr, b: std::net::IpAddr) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (std::net::IpAddr::V4(v4_a), std::net::IpAddr::V4(v4_b)) => {
            v4_a.is_unspecified() || v4_b.is_unspecified() || v4_a == v4_b
        }
        (std::net::IpAddr::V6(v6_a), std::net::IpAddr::V6(v6_b)) => {
            v6_a.is_unspecified() || v6_b.is_unspecified() || v6_a == v6_b
        }
        (std::net::IpAddr::V4(v4), std::net::IpAddr::V6(v6)) => {
            v6.is_unspecified() || (v4.is_unspecified() && v6.to_ipv4_mapped().is_some())
        }
        (std::net::IpAddr::V6(v6), std::net::IpAddr::V4(v4)) => {
            v6.is_unspecified() || (v4.is_unspecified() && v6.to_ipv4_mapped().is_some())
        }
    }
}

#[derive(Debug, Clone)]
struct HostListeningSocket {
    proto: L4Protocol,
    ip: std::net::IpAddr,
    port: u16,
}

fn parse_proc_net_content(
    content: &str,
    proto: L4Protocol,
    is_ipv6: bool,
    out: &mut Vec<HostListeningSocket>,
) {
    for line in content.lines().skip(1) {
        let mut tokens = line.split_whitespace();
        // Token 0: sl
        let _ = tokens.next();
        // Token 1: local_address (HEX_IP:HEX_PORT)
        let Some(local_addr) = tokens.next() else {
            continue;
        };
        // Token 2: rem_address
        let _ = tokens.next();
        // Token 3: st
        let Some(state) = tokens.next() else { continue };

        let is_listening = match proto {
            L4Protocol::Tcp => state.eq_ignore_ascii_case("0A"),
            L4Protocol::Udp => state.eq_ignore_ascii_case("07"),
        };

        if !is_listening {
            continue;
        }

        let Some((hex_ip, hex_port)) = local_addr.split_once(':') else {
            continue;
        };
        let Ok(port) = u16::from_str_radix(hex_port, 16) else {
            continue;
        };

        let ip = if is_ipv6 {
            if hex_ip.len() != 32 {
                continue;
            }
            let mut octets = [0u8; 16];
            let mut valid = true;
            for i in 0..4 {
                let chunk = &hex_ip[i * 8..(i + 1) * 8];
                match u32::from_str_radix(chunk, 16) {
                    Ok(val) => {
                        let bytes = val.to_ne_bytes();
                        octets[i * 4..(i + 1) * 4].copy_from_slice(&bytes);
                    }
                    Err(_) => {
                        valid = false;
                        break;
                    }
                }
            }
            if !valid {
                continue;
            }
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(octets))
        } else {
            if hex_ip.len() != 8 {
                continue;
            }
            match u32::from_str_radix(hex_ip, 16) {
                Ok(val) => std::net::IpAddr::V4(std::net::Ipv4Addr::from(val.to_ne_bytes())),
                Err(_) => continue,
            }
        };

        out.push(HostListeningSocket { proto, ip, port });
    }
}

fn collect_host_listening_sockets() -> Vec<HostListeningSocket> {
    let mut sockets = Vec::new();
    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = fs::read_to_string("/proc/net/tcp") {
            parse_proc_net_content(&content, L4Protocol::Tcp, false, &mut sockets);
        }
        if let Ok(content) = fs::read_to_string("/proc/net/tcp6") {
            parse_proc_net_content(&content, L4Protocol::Tcp, true, &mut sockets);
        }
        if let Ok(content) = fs::read_to_string("/proc/net/udp") {
            parse_proc_net_content(&content, L4Protocol::Udp, false, &mut sockets);
        }
        if let Ok(content) = fs::read_to_string("/proc/net/udp6") {
            parse_proc_net_content(&content, L4Protocol::Udp, true, &mut sockets);
        }
    }
    sockets
}

fn probe_socket_conflict(addr: std::net::SocketAddr, proto: L4Protocol) -> bool {
    match proto {
        L4Protocol::Tcp => match std::net::TcpListener::bind(addr) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => true,
            Err(_) => false,
        },
        L4Protocol::Udp => match std::net::UdpSocket::bind(addr) {
            Ok(_) => false,
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => true,
            Err(_) => false,
        },
    }
}

/// Validates listener rules strictly without silent fallbacks:
/// 1. Schema integrity & string trimming.
/// 2. Protocol validity (transport: "tcp" or "udp"; application: "raw", "http1", "http2", "http3", or "grpc").
/// 3. TLS profile configuration completeness.
/// 4. Internal port collisions among configured listeners (preventing overlapping bindings).
/// 5. Host OS port conflict detection (verifying port availability against host services).
pub fn validate_listeners(listeners: &mut [ListenerConfig]) -> Result<(), SyncError> {
    validate_listeners_with_lkg(listeners, &[])
}

/// Validates listener rules, skipping host OS port conflict checks for ports already
/// legitimately bound by the edge data plane as declared in the current LKG artifact.
pub fn validate_listeners_with_lkg(
    listeners: &mut [ListenerConfig],
    current_lkg: &[ListenerConfig],
) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(listeners.len());
    let mut parsed_bindings: Vec<(&str, L4Protocol, std::net::SocketAddr)> =
        Vec::with_capacity(listeners.len());

    let mut lkg_bindings = HashSet::new();
    for l in current_lkg {
        if let Ok(addr) = l.address.parse::<std::net::SocketAddr>() {
            let proto = match l.transport.protocol.to_ascii_lowercase().as_str() {
                "udp" => L4Protocol::Udp,
                _ => L4Protocol::Tcp,
            };
            lkg_bindings.insert((proto, addr.port()));
        }
    }

    for listener in listeners.iter_mut() {
        trim_in_place(&mut listener.id);
        trim_in_place(&mut listener.address);
        trim_in_place(&mut listener.transport.protocol);
        listener.transport.protocol.make_ascii_lowercase();
        trim_in_place(&mut listener.application.protocol);
        listener.application.protocol.make_ascii_lowercase();
        if let Some(ref mut ver) = listener.application.version {
            trim_in_place(ver);
        }

        if listener.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: "Listener ID cannot be empty".into(),
            });
        }

        if !seen_ids.insert(listener.id.as_str()) {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!("Duplicate listener ID '{}'", listener.id),
            });
        }

        if listener.address.is_empty() {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!(
                    "Listener '{}' must specify an address (e.g. '0.0.0.0:80')",
                    listener.id
                ),
            });
        }

        let socket_addr: std::net::SocketAddr =
            listener
                .address
                .parse()
                .map_err(|e| SyncError::Validation {
                    domain: "listeners".into(),
                    reason: format!(
                        "Listener '{}' has invalid socket address '{}': {e}",
                        listener.id, listener.address
                    ),
                })?;

        if socket_addr.port() == 0 {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!(
                    "Listener '{}' has invalid port 0 in address '{}'; a valid assigned port (> 0) is strictly required",
                    listener.id, listener.address
                ),
            });
        }

        let l4_proto = match listener.transport.protocol.as_str() {
            "tcp" => L4Protocol::Tcp,
            "udp" => L4Protocol::Udp,
            other => {
                return Err(SyncError::Validation {
                    domain: "listeners".into(),
                    reason: format!(
                        "Listener '{}' has unsupported transport protocol '{other}'; must be 'tcp' or 'udp'",
                        listener.id
                    ),
                });
            }
        };

        match listener.application.protocol.as_str() {
            "http1" => {
                if listener.http2.is_some()
                    || listener.grpc.is_some()
                    || listener.http3.is_some()
                    || listener.raw.is_some()
                {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has protocol 'http1' but configures conflicting protocol blocks",
                            listener.id
                        ),
                    });
                }
                if let Some(ref h1) = listener.http1 {
                    if let Some(0) = h1.max_header_size {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_header_size 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h1.max_headers {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_headers 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h1.idle_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h1.max_keepalive_requests {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_keepalive_requests 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h1.header_read_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid header_read_timeout_ms 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                }
            }
            "http2" => {
                if listener.http1.is_some()
                    || listener.grpc.is_some()
                    || listener.http3.is_some()
                    || listener.raw.is_some()
                {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has protocol 'http2' but configures conflicting protocol blocks",
                            listener.id
                        ),
                    });
                }
                if let Some(ref h2) = listener.http2 {
                    if let Some(0) = h2.max_header_size {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_header_size 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h2.max_headers {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_headers 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h2.idle_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h2.max_concurrent_streams {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_concurrent_streams 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h2.max_consecutive_resets {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_consecutive_resets 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h2.max_pending_control_frames {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_pending_control_frames 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h2.max_continuation_frames {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_continuation_frames 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                }
            }
            "grpc" => {
                if listener.http1.is_some()
                    || listener.http2.is_some()
                    || listener.http3.is_some()
                    || listener.raw.is_some()
                {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has protocol 'grpc' but configures conflicting protocol blocks",
                            listener.id
                        ),
                    });
                }
                if let Some(ref grpc) = listener.grpc {
                    if let Some(0) = grpc.max_header_size {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_header_size 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = grpc.max_headers {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_headers 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = grpc.idle_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = grpc.max_message_size {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_message_size 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = grpc.keepalive_ping_interval_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid keepalive_ping_interval_ms 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = grpc.keepalive_ping_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid keepalive_ping_timeout_ms 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = grpc.max_call_duration_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_call_duration_ms 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                }
            }
            "http3" => {
                if listener.http1.is_some()
                    || listener.http2.is_some()
                    || listener.grpc.is_some()
                    || listener.raw.is_some()
                {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has protocol 'http3' but configures conflicting protocol blocks",
                            listener.id
                        ),
                    });
                }
                if let Some(ref h3) = listener.http3 {
                    if let Some(0) = h3.max_header_size {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_header_size 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h3.max_headers {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_headers 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h3.idle_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h3.max_concurrent_streams {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_concurrent_streams 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h3.max_concurrent_uni_streams {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_concurrent_uni_streams 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = h3.max_qpack_table_capacity {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid max_qpack_table_capacity 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                }
            }
            "raw" => {
                if listener.http1.is_some()
                    || listener.http2.is_some()
                    || listener.grpc.is_some()
                    || listener.http3.is_some()
                {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has protocol 'raw' but configures conflicting protocol blocks",
                            listener.id
                        ),
                    });
                }
                if let Some(ref raw) = listener.raw {
                    if let Some(0) = raw.copy_buffer_size {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid copy_buffer_size 0; must be > 0",
                                listener.id
                            ),
                        });
                    }
                    if let Some(0) = raw.idle_timeout_ms {
                        return Err(SyncError::Validation {
                            domain: "listeners".into(),
                            reason: format!(
                                "Listener '{}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold",
                                listener.id
                            ),
                        });
                    }
                }
            }
            other => {
                return Err(SyncError::Validation {
                    domain: "listeners".into(),
                    reason: format!(
                        "Listener '{}' has unsupported application protocol '{other}'; must be 'raw', 'http1', 'http2', 'http3', or 'grpc'",
                        listener.id
                    ),
                });
            }
        }

        // Streaming mode protocol invariant: L4 'raw' listeners cannot enable L7 streaming
        if listener.application.protocol == "raw"
            && listener.application.streaming.is_streaming_enabled()
        {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!(
                    "Listener '{}' has streaming mode '{}' enabled, but protocol 'raw' does not support L7 streaming; must be 'disable'",
                    listener.id,
                    listener.application.streaming.as_str()
                ),
            });
        }

        // ====================================================================
        // Internal Port Conflict Check
        // ====================================================================
        for &(prev_id, prev_proto, prev_addr) in &parsed_bindings {
            if prev_proto == l4_proto
                && prev_addr.port() == socket_addr.port()
                && ip_addresses_overlap(prev_addr.ip(), socket_addr.ip())
            {
                return Err(SyncError::Validation {
                    domain: "listeners".into(),
                    reason: format!(
                        "Internal port conflict: listener '{}' and listener '{}' both bind to {} ({:?})",
                        listener.id, prev_id, listener.address, l4_proto
                    ),
                });
            }
        }

        parsed_bindings.push((&listener.id, l4_proto, socket_addr));
    }

    // ========================================================================
    // Inline Host OS Port Conflict Check
    // ========================================================================
    let host_sockets = collect_host_listening_sockets();
    let has_proc_data = !host_sockets.is_empty();

    // OPTIMIZATION: Index host sockets by port into a HashMap to turn an O(N * M) nested scan
    // into an O(1) lookup per listener port.
    let mut host_sockets_by_port: HashMap<u16, Vec<HostListeningSocket>> =
        HashMap::with_capacity(host_sockets.len());
    for s in host_sockets {
        host_sockets_by_port.entry(s.port).or_default().push(s);
    }

    for &(id, proto, socket_addr) in &parsed_bindings {
        // Skip host OS port conflict check if this port is already held by the running edge gateway in LKG
        if lkg_bindings.contains(&(proto, socket_addr.port())) {
            continue;
        }

        let mut in_use = false;

        if has_proc_data {
            if let Some(matching) = host_sockets_by_port.get(&socket_addr.port()) {
                for host in matching {
                    if host.proto == proto && ip_addresses_overlap(host.ip, socket_addr.ip()) {
                        in_use = true;
                        break;
                    }
                }
            }
        } else if probe_socket_conflict(socket_addr, proto) {
            // OPTIMIZATION: Only fall back to active socket bind probe if /proc/net is unavailable
            // (e.g. non-Linux or restricted environment), avoiding hundreds of costly syscalls
            // for every free port on every validation cycle.
            in_use = true;
        }

        if in_use {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!(
                    "Host OS port conflict: listener '{}' cannot bind to {} ({:?}); port is already in use by another service on the host",
                    id, socket_addr, proto
                ),
            });
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles listener configurations into a self-describing binary artifact (listeners.bin).
pub fn compile_listeners_to_binary(
    listeners: &[ListenerConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    // OPTIMIZATION: Pre-allocate contiguous buffer with estimated capacity (header + 128B per listener)
    // to avoid multiple vector reallocations during streaming bincode serialization.
    let estimated_cap = DOMAIN_HEADER_SIZE + listeners.len().saturating_mul(128);
    let mut binary_output = Vec::with_capacity(estimated_cap);
    binary_output.resize(DOMAIN_HEADER_SIZE, 0);

    bincode::serialize_into(&mut binary_output, listeners).map_err(|e| SyncError::Compile {
        domain: "listeners".into(),
        reason: e.to_string(),
    })?;

    let payload_len = binary_output.len() - DOMAIN_HEADER_SIZE;
    let mut hasher = Sha256::new();
    hasher.update(&binary_output[DOMAIN_HEADER_SIZE..]);
    let payload_hash: [u8; 32] = hasher.finalize().into();

    let header = DomainHeader {
        magic: MAGIC_BYTES,
        format_version: CURRENT_FORMAT_VERSION,
        compiler_version: CURRENT_COMPILER_VERSION,
        schema_version: 1,
        domain_tag: TAG_LISTENERS,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "listeners".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled listeners binary artifact.
pub fn unpack_listeners_from_binary(
    bytes: &[u8],
) -> Result<(DomainHeader, Vec<ListenerConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "listeners".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_LISTENERS {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let listeners: Vec<ListenerConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "listeners".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    Ok((header, listeners))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists listeners canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_listeners(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("listeners.json");
    let bin_path = storage_dir.join("runtime").join("listeners.bin");

    for (target, content) in [(&json_path, json_bytes), (&bin_path, bin_bytes)] {
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let file_name = target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");
        let tmp_path = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(content)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, target)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn cfg(id: &str, addr: &str, transport_proto: &str, app_proto: &str) -> ListenerConfig {
        ListenerConfig {
            id: id.into(),
            address: addr.into(),
            transport: ListenerTransportConfig {
                protocol: transport_proto.into(),
            },
            application: ListenerApplicationConfig {
                protocol: app_proto.into(),
                version: None,
                streaming: velda_core::StreamingMode::DISABLED,
            },
            tls: Default::default(),
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
        }
    }

    #[test]
    fn test_parse_valid_listeners() {
        let json = r#"{
            "schema_version": 1,
            "listeners": [{
                "id": "http",
                "address": " 0.0.0.0:80 ",
                "transport": { "protocol": "TCP" },
                "application": { "protocol": "HTTP1", "version": "1.1", "streaming": [] },
                "tls": { "enabled": false },
                "http1": {
                    "max_body_size": 10485760,
                    "max_header_size": 65536,
                    "max_headers": 64,
                    "idle_timeout_ms": 30000
                }
            }]
        }"#;

        let mut listeners = parse_listeners(json.as_bytes()).unwrap();
        assert_eq!(listeners.len(), 1);
        validate_listeners(&mut listeners).unwrap();
        assert_eq!(listeners[0].address, "0.0.0.0:80");
        assert_eq!(listeners[0].transport.protocol, "tcp");
        assert_eq!(listeners[0].application.protocol, "http1");
        assert_eq!(listeners[0].application.version.as_deref(), Some("1.1"));
        assert_eq!(
            listeners[0].http1.as_ref().unwrap().max_header_size,
            Some(65536)
        );
    }

    #[test]
    fn test_omitted_protocol_block_is_valid() {
        let json = r#"{
            "schema_version": 1,
            "listeners": [{
                "id": "http",
                "address": "0.0.0.0:80",
                "transport": { "protocol": "tcp" },
                "application": { "protocol": "http1", "streaming": [] },
                "tls": { "enabled": false }
            }]
        }"#;

        let mut listeners = parse_listeners(json.as_bytes()).unwrap();
        assert_eq!(listeners.len(), 1);
        assert!(validate_listeners(&mut listeners).is_ok());
        assert!(listeners[0].http1.is_none());
    }

    #[test]
    fn test_conflicting_protocol_blocks_rejected() {
        let json = r#"{
            "schema_version": 1,
            "listeners": [{
                "id": "http",
                "address": "0.0.0.0:80",
                "transport": { "protocol": "tcp" },
                "application": { "protocol": "http1", "streaming": [] },
                "tls": { "enabled": false },
                "http2": {
                    "max_concurrent_streams": 256
                }
            }]
        }"#;

        let mut listeners = parse_listeners(json.as_bytes()).unwrap();
        assert!(validate_listeners(&mut listeners).is_err());
    }

    #[test]
    fn test_missing_streaming_fails_deserialization() {
        let json = r#"{
            "schema_version": 1,
            "listeners": [{
                "id": "http",
                "address": "0.0.0.0:80",
                "transport": { "protocol": "tcp" },
                "application": { "protocol": "http1" },
                "tls": { "enabled": false }
            }]
        }"#;

        assert!(
            parse_listeners(json.as_bytes()).is_err(),
            "Missing mandatory streaming must fail deserialization"
        );
    }

    #[test]
    fn test_unsupported_protocol_fails() {
        let mut listeners = vec![cfg(
            "bad-transport",
            "0.0.0.0:80",
            "unsupported_proto",
            "http1",
        )];
        assert!(matches!(
            validate_listeners(&mut listeners),
            Err(SyncError::Validation { .. })
        ));

        let mut listeners2 = vec![cfg("bad-app", "0.0.0.0:80", "tcp", "unsupported_app")];
        assert!(matches!(
            validate_listeners(&mut listeners2),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_duplicate_listener_id_fails() {
        let mut listeners = vec![
            cfg("http", "0.0.0.0:80", "tcp", "http1"),
            cfg("http", "0.0.0.0:8080", "tcp", "http1"),
        ];
        assert!(matches!(
            validate_listeners(&mut listeners),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_internal_port_conflict_fails() {
        let mut listeners = vec![
            cfg("http-1", "127.0.0.1:18080", "tcp", "http1"),
            cfg("tcp-1", "127.0.0.1:18080", "tcp", "raw"), // both use L4 TCP
        ];
        let err = validate_listeners(&mut listeners).unwrap_err();
        assert!(err.to_string().contains("Internal port conflict"));
    }

    #[test]
    fn test_internal_port_wildcard_overlap_fails() {
        let mut listeners = vec![
            cfg("all", "0.0.0.0:18081", "tcp", "http1"),
            cfg("loopback", "127.0.0.1:18081", "tcp", "raw"),
        ];
        let err = validate_listeners(&mut listeners).unwrap_err();
        assert!(err.to_string().contains("Internal port conflict"));
    }

    #[test]
    fn test_tcp_and_udp_coexist_on_same_port() {
        let mut listeners = vec![
            cfg("dns-tcp", "127.0.0.1:18082", "tcp", "raw"),
            cfg("dns-udp", "127.0.0.1:18082", "udp", "raw"),
        ];
        assert!(validate_listeners(&mut listeners).is_ok());
    }

    #[test]
    fn test_host_os_port_conflict_detected() {
        let host_socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = host_socket.local_addr().unwrap().port();
        let mut listeners = vec![cfg(
            "host-conflict",
            &format!("127.0.0.1:{port}"),
            "tcp",
            "raw",
        )];

        let err = validate_listeners(&mut listeners).unwrap_err();
        assert!(err.to_string().contains("Host OS port conflict"));

        drop(host_socket);
        assert!(validate_listeners(&mut listeners).is_ok());
    }

    #[test]
    fn test_listener_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let listeners = vec![cfg("http", "0.0.0.0:80", "tcp", "http1")];
        let binary = compile_listeners_to_binary(&listeners, 5, [0x11u8; 32]).unwrap();

        let (header, restored) = unpack_listeners_from_binary(&binary).unwrap();
        assert_eq!(header.revision, 5);
        assert_eq!(restored, listeners);

        persist_listeners(tmp.path(), b"{\"listeners\": []}", &binary).unwrap();
        assert!(tmp.path().join("config/listeners.json").exists());
        assert!(tmp.path().join("runtime/listeners.bin").exists());
    }

    #[test]
    fn test_listener_streaming_validation() {
        let mut invalid_raw = vec![cfg("raw-stream", "127.0.0.1:18090", "tcp", "raw")];
        invalid_raw[0].application.streaming = velda_core::StreamingMode::SERVER;
        let err = validate_listeners(&mut invalid_raw).unwrap_err();
        assert!(err.to_string().contains("does not support L7 streaming"));

        let mut valid_h2 = vec![cfg("h2-stream", "127.0.0.1:18091", "tcp", "http2")];
        valid_h2[0].application.streaming = velda_core::StreamingMode::SERVER;
        assert!(validate_listeners(&mut valid_h2).is_ok());
    }
}
