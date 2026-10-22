use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use crate::SyncError;

use super::host_port::{
    HostListeningSocket, L4Protocol, collect_host_listening_sockets, ip_addresses_overlap,
    probe_socket_conflict,
};
use super::schema::{
    GrpcListenerConfig, Http1ListenerConfig, Http2ListenerConfig, Http3ListenerConfig,
    ListenerConfig, RawListenerConfig,
};

#[inline]
pub(crate) fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

pub(crate) fn validate_http1(id: &str, h1: &Http1ListenerConfig) -> Result<(), SyncError> {
    if let Some(0) = h1.max_header_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_header_size 0; must be > 0"),
        });
    }
    if let Some(0) = h1.max_headers {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_headers 0; must be > 0"),
        });
    }
    if let Some(0) = h1.idle_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold"
            ),
        });
    }
    if let Some(0) = h1.max_keepalive_requests {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_keepalive_requests 0; must be > 0"),
        });
    }
    if let Some(0) = h1.header_read_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid header_read_timeout_ms 0; must be > 0"),
        });
    }
    if let Some(alt_svc) = h1.effective_alt_svc() {
        if alt_svc.port == 0 {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!("Listener '{id}' has invalid alt_svc port 0; must be > 0"),
            });
        }
        if alt_svc.max_age == 0 {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!("Listener '{id}' has invalid alt_svc max_age 0; must be > 0"),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_http2(id: &str, h2: &Http2ListenerConfig) -> Result<(), SyncError> {
    if let Some(0) = h2.max_header_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_header_size 0; must be > 0"),
        });
    }
    if let Some(0) = h2.max_headers {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_headers 0; must be > 0"),
        });
    }
    if let Some(0) = h2.idle_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold"
            ),
        });
    }
    if let Some(0) = h2.max_concurrent_streams {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_concurrent_streams 0; must be > 0"),
        });
    }
    if let Some(0) = h2.max_consecutive_resets {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_consecutive_resets 0; must be > 0"),
        });
    }
    if let Some(0) = h2.max_pending_control_frames {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid max_pending_control_frames 0; must be > 0"
            ),
        });
    }
    if let Some(0) = h2.max_continuation_frames {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_continuation_frames 0; must be > 0"),
        });
    }
    if let Some(stream_win) = h2.initial_stream_window_size
        && stream_win < 65_535
    {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid initial_stream_window_size {stream_win}; must be at least 65535 (RFC 9113 minimum floor)"
            ),
        });
    }
    if let Some(conn_win) = h2.initial_connection_window_size
        && conn_win < 65_535
    {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid initial_connection_window_size {conn_win}; must be at least 65535 (RFC 9113 minimum floor)"
            ),
        });
    }
    if let Some(0) = h2.max_send_buffer_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_send_buffer_size 0; must be > 0"),
        });
    }
    if let Some(alt_svc) = h2.effective_alt_svc() {
        if alt_svc.port == 0 {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!("Listener '{id}' has invalid alt_svc port 0; must be > 0"),
            });
        }
        if alt_svc.max_age == 0 {
            return Err(SyncError::Validation {
                domain: "listeners".into(),
                reason: format!("Listener '{id}' has invalid alt_svc max_age 0; must be > 0"),
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_grpc(id: &str, grpc: &GrpcListenerConfig) -> Result<(), SyncError> {
    if let Some(0) = grpc.max_header_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_header_size 0; must be > 0"),
        });
    }
    if let Some(0) = grpc.max_headers {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_headers 0; must be > 0"),
        });
    }
    if let Some(0) = grpc.idle_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold"
            ),
        });
    }
    if let Some(0) = grpc.max_message_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_message_size 0; must be > 0"),
        });
    }
    if let Some(0) = grpc.keepalive_ping_interval_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid keepalive_ping_interval_ms 0; must be > 0"
            ),
        });
    }
    if let Some(0) = grpc.keepalive_ping_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid keepalive_ping_timeout_ms 0; must be > 0"),
        });
    }
    if let Some(0) = grpc.max_call_duration_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_call_duration_ms 0; must be > 0"),
        });
    }
    Ok(())
}

pub(crate) fn validate_http3(id: &str, h3: &Http3ListenerConfig) -> Result<(), SyncError> {
    if let Some(0) = h3.max_header_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_header_size 0; must be > 0"),
        });
    }
    if let Some(0) = h3.max_headers {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_headers 0; must be > 0"),
        });
    }
    if let Some(0) = h3.idle_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold"
            ),
        });
    }
    if let Some(0) = h3.max_concurrent_streams {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_concurrent_streams 0; must be > 0"),
        });
    }
    if let Some(0) = h3.max_concurrent_uni_streams {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid max_concurrent_uni_streams 0; must be > 0"
            ),
        });
    }
    if let Some(0) = h3.max_qpack_table_capacity {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid max_qpack_table_capacity 0; must be > 0"),
        });
    }
    Ok(())
}

pub(crate) fn validate_raw(id: &str, raw: &RawListenerConfig) -> Result<(), SyncError> {
    if let Some(0) = raw.copy_buffer_size {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!("Listener '{id}' has invalid copy_buffer_size 0; must be > 0"),
        });
    }
    if let Some(0) = raw.idle_timeout_ms {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Listener '{id}' has invalid idle_timeout_ms 0; must be > 0 to prevent indefinite connection hold"
            ),
        });
    }
    Ok(())
}

/// Validates listener rules strictly without silent fallbacks:
/// 1. Schema integrity & string trimming.
/// 2. Protocol validity (transport: "tcp" or "udp"; application: "raw", "http", "http1", "http2", "http3", or "grpc").
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
    let mut parsed_bindings: Vec<(&str, L4Protocol, SocketAddr)> =
        Vec::with_capacity(listeners.len());

    let mut lkg_bindings = HashSet::new();
    for l in current_lkg {
        if let Ok(addr) = l.address.parse::<SocketAddr>() {
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

        let socket_addr: SocketAddr =
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
                    validate_http1(&listener.id, h1)?;
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
                    validate_http2(&listener.id, h2)?;
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
                    validate_grpc(&listener.id, grpc)?;
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
                    validate_http3(&listener.id, h3)?;
                }
            }
            "http" => {
                if listener.grpc.is_some() || listener.raw.is_some() {
                    return Err(SyncError::Validation {
                        domain: "listeners".into(),
                        reason: format!(
                            "Listener '{}' has protocol 'http' but configures conflicting protocol blocks (must not specify grpc or raw)",
                            listener.id
                        ),
                    });
                }
                if let Some(ref h1) = listener.http1 {
                    validate_http1(&listener.id, h1)?;
                }
                if let Some(ref h2) = listener.http2 {
                    validate_http2(&listener.id, h2)?;
                }
                if let Some(ref h3) = listener.http3 {
                    validate_http3(&listener.id, h3)?;
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
                    validate_raw(&listener.id, raw)?;
                }
            }
            other => {
                return Err(SyncError::Validation {
                    domain: "listeners".into(),
                    reason: format!(
                        "Listener '{}' has unsupported application protocol '{other}'; must be 'raw', 'http', 'http1', 'http2', 'http3', or 'grpc'",
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
