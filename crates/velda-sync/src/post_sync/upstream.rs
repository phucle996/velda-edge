use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::tls as tls_domain;
use crate::SyncError;

// ============================================================================
// Binary Protocol Constants & Domain Header
// ============================================================================

pub const MAGIC_BYTES: [u8; 4] = *b"VLDA";
pub const CURRENT_FORMAT_VERSION: u32 = 1;
pub const CURRENT_COMPILER_VERSION: u32 = 1;
pub const TAG_UPSTREAMS: [u8; 8] = *b"UPSTREAM";

/// Self-describing binary header prepended to upstreams.bin.
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
pub struct UpstreamsFile {
    pub schema_version: u32,
    pub upstreams: Vec<UpstreamConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamProtocolConfig {
    pub transport: String,   // "tcp", "udp", "quic"
    pub application: String, // "raw", "http1", "http2", "http3", "grpc"
    pub streaming: velda_core::StreamingMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamConfig {
    pub id: String,
    pub mode: String, // "dns" or "endpoints"
    pub protocol: UpstreamProtocolConfig,
    pub target: Option<DnsTarget>,
    pub resolver: Option<ResolverConfig>,
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
    pub load_balancer: LoadBalancerConfig,
    pub timeouts: UpstreamTimeouts,
    #[serde(default)]
    pub health_check: Option<HealthCheckConfig>,
    #[serde(default)]
    pub tls: Option<UpstreamTlsConfig>,
}

/// Upstream TLS configuration for secure backend connections.
/// In JSON: inline PEM strings without `sni` (sni is auto-extracted during compilation).
/// In Binary: contains auto-extracted `sni` hostnames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpstreamTlsConfig {
    #[serde(default)]
    pub ca_pem: Option<String>,
    #[serde(default)]
    pub client_cert_pem: Option<String>,
    #[serde(default)]
    pub client_key_pem: Option<String>,
    #[serde(default)]
    pub versions: Vec<String>,
    #[serde(default)]
    pub alpn: Vec<String>,
    /// Auto-extracted SNIs compiled into binary (not present in raw JSON).
    #[serde(default)]
    pub sni: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default)]
    pub passive: Option<PassiveHealthConfig>,
    #[serde(default)]
    pub active: Option<ActiveHealthConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassiveHealthConfig {
    pub enabled: bool,
    pub max_consecutive_failures: u32,
    pub cooldown_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveHealthConfig {
    pub enabled: bool,
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub unhealthy_threshold: u32,
    pub healthy_threshold: u32,
    pub check_type: String, // "http", "tcp"
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub expected_statuses: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DnsTarget {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResolverConfig {
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub nameservers: Vec<String>,
    #[serde(default)]
    pub refresh_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointConfig {
    pub address: String,
    pub weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadBalancerConfig {
    pub algorithm: String, // "round_robin", "least_connections", "random", "ip_hash"
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamTimeouts {
    pub connect_ms: u64,
    pub idle_ms: u64,
    #[serde(default)]
    pub request_ms: Option<u64>,
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into upstream configurations.
pub fn parse_upstreams(payload: &[u8]) -> Result<Vec<UpstreamConfig>, SyncError> {
    let file: UpstreamsFile =
        serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
            domain: "upstreams".into(),
            reason: e.to_string(),
        })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "upstreams".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.upstreams)
}

// ============================================================================
// Phase 3: Integrity Check & Semantic Validation
// ============================================================================

#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

/// Validates upstream configuration rules strictly without silent fallbacks.
pub fn validate_upstreams(upstreams: &mut [UpstreamConfig]) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(upstreams.len());

    for upstream in upstreams {
        trim_in_place(&mut upstream.id);
        trim_in_place(&mut upstream.mode);
        upstream.mode.make_ascii_lowercase();
        trim_in_place(&mut upstream.load_balancer.algorithm);
        upstream.load_balancer.algorithm.make_ascii_lowercase();

        // Fail fast: empty upstream ID
        if upstream.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: "Upstream ID cannot be empty".into(),
            });
        }

        // Fail fast: duplicate upstream ID
        if !seen_ids.insert(upstream.id.as_str()) {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!("Duplicate upstream ID '{}'", upstream.id),
            });
        }

        // Fail fast: empty or unsupported load_balancer algorithm (no silent fallback!)
        if upstream.load_balancer.algorithm.is_empty() {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': load_balancer algorithm cannot be empty",
                    upstream.id
                ),
            });
        }

        let is_valid_algo = matches!(
            upstream.load_balancer.algorithm.as_str(),
            "round_robin" | "least_connections" | "random" | "ip_hash"
        );

        if !is_valid_algo {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': unsupported load_balancer algorithm '{}'; must be 'round_robin', 'least_connections', 'random', or 'ip_hash'",
                    upstream.id, upstream.load_balancer.algorithm
                ),
            });
        }

        if upstream.mode == "dns" {
            let target = match &upstream.target {
                Some(t) => t,
                None => {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}' is mode 'dns' but missing required target",
                            upstream.id
                        ),
                    });
                }
            };

            if target.host.trim().is_empty() || target.port == 0 {
                return Err(SyncError::Validation {
                    domain: "upstreams".into(),
                    reason: format!(
                        "Upstream '{}' DNS target must specify valid host and non-zero port",
                        upstream.id
                    ),
                });
            }
        } else if upstream.mode == "endpoints" {
            if upstream.endpoints.is_empty() {
                return Err(SyncError::Validation {
                    domain: "upstreams".into(),
                    reason: format!(
                        "Upstream '{}' must specify at least one endpoint in 'endpoints' mode",
                        upstream.id
                    ),
                });
            }
            for ep in &upstream.endpoints {
                if ep.address.trim().is_empty() {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}' has an endpoint with empty address",
                            upstream.id
                        ),
                    });
                }
                if ep.weight == 0 {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}' endpoint '{}' weight must be greater than 0",
                            upstream.id, ep.address
                        ),
                    });
                }
            }
        } else {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}' has unsupported mode '{}' (must be 'dns' or 'endpoints')",
                    upstream.id, upstream.mode
                ),
            });
        }

        // Fail fast: strict protocol validation
        trim_in_place(&mut upstream.protocol.transport);
        trim_in_place(&mut upstream.protocol.application);
        upstream.protocol.transport.make_ascii_lowercase();
        upstream.protocol.application.make_ascii_lowercase();

        let is_valid_transport =
            matches!(upstream.protocol.transport.as_str(), "tcp" | "udp" | "quic");
        if !is_valid_transport {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': unsupported protocol transport '{}'; must be 'tcp', 'udp', or 'quic'",
                    upstream.id, upstream.protocol.transport
                ),
            });
        }

        let is_valid_app = matches!(
            upstream.protocol.application.as_str(),
            "raw" | "http1" | "http2" | "http3" | "grpc"
        );
        if !is_valid_app {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': unsupported protocol application '{}'; must be 'raw', 'http1', 'http2', 'http3', or 'grpc'",
                    upstream.id, upstream.protocol.application
                ),
            });
        }

        // Streaming mode protocol invariant: L4 'raw' upstreams cannot enable L7 streaming
        if upstream.protocol.application == "raw"
            && upstream.protocol.streaming.is_streaming_enabled()
        {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}' has streaming mode '{}' enabled, but protocol 'raw' does not support L7 streaming; must be 'disable'",
                    upstream.id,
                    upstream.protocol.streaming.as_str()
                ),
            });
        }

        match upstream.protocol.application.as_str() {
            "http1" | "http2" => {
                if upstream.protocol.transport != "tcp" {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': '{}' requires 'tcp' transport, found '{}'",
                            upstream.id, upstream.protocol.application, upstream.protocol.transport
                        ),
                    });
                }
            }
            "http3" => {
                if upstream.protocol.transport != "quic" && upstream.protocol.transport != "udp" {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': HTTP/3 requires 'quic' or 'udp' transport, found '{}'",
                            upstream.id, upstream.protocol.transport
                        ),
                    });
                }
            }
            "grpc" => {
                if upstream.protocol.transport != "tcp" && upstream.protocol.transport != "quic" {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': gRPC requires 'tcp' or 'quic' transport, found '{}'",
                            upstream.id, upstream.protocol.transport
                        ),
                    });
                }
            }
            "raw" => {
                if upstream.protocol.transport != "tcp" && upstream.protocol.transport != "udp" {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': raw application requires 'tcp' or 'udp' transport, found '{}'",
                            upstream.id, upstream.protocol.transport
                        ),
                    });
                }
            }
            _ => unreachable!(),
        }

        // Fail fast: strict timeouts validation (zero silent fallbacks)
        if upstream.timeouts.connect_ms == 0 {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': 'timeouts.connect_ms' is mandatory and must be > 0 (zero silent fallback)",
                    upstream.id
                ),
            });
        }

        if upstream.timeouts.idle_ms == 0 {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': 'timeouts.idle_ms' is mandatory and must be > 0 (zero silent fallback)",
                    upstream.id
                ),
            });
        }

        if upstream.timeouts.request_ms == Some(0) {
            return Err(SyncError::Validation {
                domain: "upstreams".into(),
                reason: format!(
                    "Upstream '{}': 'timeouts.request_ms' must be > 0 if specified",
                    upstream.id
                ),
            });
        }

        // Validation: health check options if configured
        if let Some(ref mut hc) = upstream.health_check {
            if let Some(ref mut passive) = hc.passive
                && passive.enabled
            {
                if passive.max_consecutive_failures == 0 {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': 'health_check.passive.max_consecutive_failures' must be > 0",
                            upstream.id
                        ),
                    });
                }
                if passive.cooldown_ms == 0 {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': 'health_check.passive.cooldown_ms' must be > 0",
                            upstream.id
                        ),
                    });
                }
            }

            if let Some(ref mut active) = hc.active {
                trim_in_place(&mut active.check_type);
                active.check_type.make_ascii_lowercase();

                if active.enabled {
                    if active.interval_ms == 0 {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': 'health_check.active.interval_ms' must be > 0",
                                upstream.id
                            ),
                        });
                    }
                    if active.timeout_ms == 0 {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': 'health_check.active.timeout_ms' must be > 0",
                                upstream.id
                            ),
                        });
                    }
                    if active.unhealthy_threshold == 0 {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': 'health_check.active.unhealthy_threshold' must be > 0",
                                upstream.id
                            ),
                        });
                    }
                    if active.healthy_threshold == 0 {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': 'health_check.active.healthy_threshold' must be > 0",
                                upstream.id
                            ),
                        });
                    }
                    let is_valid_check_type = matches!(active.check_type.as_str(), "http" | "tcp");
                    if !is_valid_check_type {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': unsupported health_check active check_type '{}'; must be 'http' or 'tcp'",
                                upstream.id, active.check_type
                            ),
                        });
                    }
                    if active.check_type == "http" {
                        let path = match active.path.as_mut() {
                            Some(p) => {
                                trim_in_place(p);
                                if p.is_empty() {
                                    return Err(SyncError::Validation {
                                        domain: "upstreams".into(),
                                        reason: format!(
                                            "Upstream '{}': 'health_check.active.path' cannot be empty for 'http' check",
                                            upstream.id
                                        ),
                                    });
                                }
                                p.as_str()
                            }
                            None => {
                                return Err(SyncError::Validation {
                                    domain: "upstreams".into(),
                                    reason: format!(
                                        "Upstream '{}': 'health_check.active.path' is mandatory for 'http' check (zero silent fallback)",
                                        upstream.id
                                    ),
                                });
                            }
                        };
                        if !path.starts_with('/') {
                            return Err(SyncError::Validation {
                                domain: "upstreams".into(),
                                reason: format!(
                                    "Upstream '{}': 'health_check.active.path' must start with '/'",
                                    upstream.id
                                ),
                            });
                        }

                        if active.expected_statuses.is_empty() {
                            return Err(SyncError::Validation {
                                domain: "upstreams".into(),
                                reason: format!(
                                    "Upstream '{}': 'health_check.active.expected_statuses' is mandatory for 'http' check and cannot be empty (zero silent fallback)",
                                    upstream.id
                                ),
                            });
                        }
                    }
                }
            }
        }

        // Validation: TLS configuration if configured
        if let Some(ref mut tls) = upstream.tls {
            if let Some(ref mut ca) = tls.ca_pem {
                trim_in_place(ca);
            }
            if let Some(ref mut cert) = tls.client_cert_pem {
                trim_in_place(cert);
            }
            if let Some(ref mut key) = tls.client_key_pem {
                trim_in_place(key);
            }

            if let Some(ca) = &tls.ca_pem
                && !ca.is_empty()
            {
                tls_domain::validate_ca_bundle_pem(ca)?;
            }

            // Both client_cert_pem and client_key_pem must be present if mTLS is configured
            match (&tls.client_cert_pem, &tls.client_key_pem) {
                (Some(cert), Some(key)) => {
                    if cert.is_empty() {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': client_cert_pem cannot be empty when specified",
                                upstream.id
                            ),
                        });
                    }
                    if key.is_empty() {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': client_key_pem cannot be empty when specified",
                                upstream.id
                            ),
                        });
                    }
                    let _ = tls_domain::extract_cert_snis(cert)?;
                    tls_domain::validate_private_key_pem(key)?;
                }
                (Some(_), None) => {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': specified client_cert_pem without client_key_pem",
                            upstream.id
                        ),
                    });
                }
                (None, Some(_)) => {
                    return Err(SyncError::Validation {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Upstream '{}': specified client_key_pem without client_cert_pem",
                            upstream.id
                        ),
                    });
                }
                (None, None) => {}
            }

            // Auto-extract SNI from certificate or DNS target host
            let snis =
                extract_upstream_snis(&upstream.id, &upstream.mode, upstream.target.as_ref(), tls)?;
            if snis.is_empty() {
                return Err(SyncError::Validation {
                    domain: "upstreams".into(),
                    reason: format!("Upstream '{}': TLS SNI list cannot be empty", upstream.id),
                });
            }
            tls.sni = snis;

            if tls.versions.is_empty() {
                tls.versions = vec!["tls1.2".into(), "tls1.3".into()];
            } else {
                for v in &mut tls.versions {
                    trim_in_place(v);
                    v.make_ascii_lowercase();
                    if !matches!(v.as_str(), "tls1.2" | "tls1.3" | "tlsv1.2" | "tlsv1.3") {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': Unsupported TLS version '{v}'; must be tls1.2 or tls1.3",
                                upstream.id
                            ),
                        });
                    }
                }
            }

            if tls.alpn.is_empty() {
                tls.alpn = vec!["h2".into(), "http/1.1".into()];
            } else {
                for alpn in &mut tls.alpn {
                    trim_in_place(alpn);
                    alpn.make_ascii_lowercase();
                    if alpn.is_empty() {
                        return Err(SyncError::Validation {
                            domain: "upstreams".into(),
                            reason: format!(
                                "Upstream '{}': ALPN entry cannot be empty",
                                upstream.id
                            ),
                        });
                    }
                }
            }
        }
    }

    Ok(())
}

fn extract_upstream_snis(
    id: &str,
    mode: &str,
    target: Option<&DnsTarget>,
    tls: &UpstreamTlsConfig,
) -> Result<Vec<String>, SyncError> {
    if let Some(ca) = &tls.ca_pem
        && !ca.trim().is_empty()
        && let Ok(snis) = tls_domain::extract_cert_snis(ca)
        && !snis.is_empty()
    {
        return Ok(snis);
    }

    if let Some(client_cert) = &tls.client_cert_pem
        && !client_cert.trim().is_empty()
        && let Ok(snis) = tls_domain::extract_cert_snis(client_cert)
        && !snis.is_empty()
    {
        return Ok(snis);
    }

    if mode == "dns"
        && let Some(target) = target
        && !target.host.trim().is_empty()
    {
        let h = target.host.trim().to_ascii_lowercase();
        return Ok(vec![h]);
    }

    Err(SyncError::Validation {
        domain: "upstreams".into(),
        reason: format!(
            "Upstream '{id}' has TLS enabled but failed to extract SNI (no SAN/CN in ca_pem/client_cert_pem and no valid DNS target host)"
        ),
    })
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles upstream configurations into a self-describing binary artifact (upstreams.bin).
pub fn compile_upstreams_to_binary(
    upstreams: &[UpstreamConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    // Invariant: Verify that if TLS is enabled, SNI list is populated and not empty
    for upstream in upstreams {
        if let Some(tls) = &upstream.tls {
            if tls.sni.is_empty() {
                return Err(SyncError::Compile {
                    domain: "upstreams".into(),
                    reason: format!(
                        "Upstream '{}' has TLS enabled but empty SNI list",
                        upstream.id
                    ),
                });
            }
            for sni in &tls.sni {
                if sni.trim().is_empty() {
                    return Err(SyncError::Compile {
                        domain: "upstreams".into(),
                        reason: format!("Upstream '{}' contains an empty SNI entry", upstream.id),
                    });
                }
            }
        }
    }

    // OPTIMIZATION: Pre-allocate contiguous buffer with estimated capacity (header + 192B per upstream)
    // to avoid multiple vector reallocations during streaming bincode serialization.
    let estimated_cap = DOMAIN_HEADER_SIZE + upstreams.len().saturating_mul(192);
    let mut binary_output = Vec::with_capacity(estimated_cap);
    binary_output.resize(DOMAIN_HEADER_SIZE, 0);

    bincode::serialize_into(&mut binary_output, upstreams).map_err(|e| SyncError::Compile {
        domain: "upstreams".into(),
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
        domain_tag: TAG_UPSTREAMS,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "upstreams".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled upstreams binary artifact.
pub fn unpack_upstreams_from_binary(
    bytes: &[u8],
) -> Result<(DomainHeader, Vec<UpstreamConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "upstreams".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "upstreams".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "upstreams".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_UPSTREAMS {
        return Err(SyncError::Compile {
            domain: "upstreams".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "upstreams".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "upstreams".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let upstreams: Vec<UpstreamConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "upstreams".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    // Invariant: Enforce non-empty SNI for any upstream with TLS configured
    for upstream in &upstreams {
        if let Some(tls) = &upstream.tls {
            if tls.sni.is_empty() {
                return Err(SyncError::Compile {
                    domain: "upstreams".into(),
                    reason: format!(
                        "Corrupted upstreams artifact: upstream '{}' has TLS enabled but empty SNI list",
                        upstream.id
                    ),
                });
            }
            for sni in &tls.sni {
                if sni.trim().is_empty() {
                    return Err(SyncError::Compile {
                        domain: "upstreams".into(),
                        reason: format!(
                            "Corrupted upstreams artifact: upstream '{}' contains an empty SNI entry",
                            upstream.id
                        ),
                    });
                }
            }
        }
    }

    Ok((header, upstreams))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists upstreams canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_upstreams(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("upstreams.json");
    let bin_path = storage_dir.join("runtime").join("upstreams.bin");

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

    fn mock_upstream_config(id: &str) -> UpstreamConfig {
        UpstreamConfig {
            id: id.into(),
            mode: "endpoints".into(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".into(),
                application: "http1".into(),
                streaming: velda_core::StreamingMode::DISABLED,
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "127.0.0.1:8080".into(),
                weight: 1,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: UpstreamTimeouts {
                connect_ms: 500,
                request_ms: None,
                idle_ms: 30000,
            },
            health_check: None,
            tls: None,
        }
    }

    #[test]
    fn test_parse_and_validate_upstreams() {
        let json = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": " users ",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "http1", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 2 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500, "idle_ms": 30000 }
            }]
        }"#;

        let mut upstreams = parse_upstreams(json.as_bytes()).unwrap();
        assert_eq!(upstreams.len(), 1);
        validate_upstreams(&mut upstreams).unwrap();
        assert_eq!(upstreams[0].id, "users");
        assert_eq!(upstreams[0].load_balancer.algorithm, "round_robin");
    }

    #[test]
    fn test_validation_failure_cases() {
        // Zero weight endpoint
        let mut u_zero_weight = mock_upstream_config("u1");
        u_zero_weight.endpoints[0].weight = 0;
        assert!(validate_upstreams(&mut [u_zero_weight]).is_err());

        // Unsupported LB algorithm
        let mut u_bad_lb = mock_upstream_config("u2");
        u_bad_lb.load_balancer.algorithm = "magic_balance".into();
        assert!(validate_upstreams(&mut [u_bad_lb]).is_err());

        // DNS mode missing target
        let mut u_dns_no_target = mock_upstream_config("u3");
        u_dns_no_target.mode = "dns".into();
        u_dns_no_target.target = None;
        assert!(validate_upstreams(&mut [u_dns_no_target]).is_err());

        // Zero idle timeout
        let mut u_zero_idle = mock_upstream_config("u4");
        u_zero_idle.timeouts.idle_ms = 0;
        assert!(validate_upstreams(&mut [u_zero_idle]).is_err());

        // Zero connect timeout
        let mut u_zero_connect = mock_upstream_config("u4_conn");
        u_zero_connect.timeouts.connect_ms = 0;
        assert!(validate_upstreams(&mut [u_zero_connect]).is_err());

        // Invalid transport protocol
        let mut u_bad_proto = mock_upstream_config("u5");
        u_bad_proto.protocol.transport = "invalid_transport".into();
        assert!(validate_upstreams(&mut [u_bad_proto]).is_err());
    }

    #[test]
    fn test_upstream_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let upstreams = vec![mock_upstream_config("users")];
        let json_bytes = b"{\"upstreams\": []}";
        let binary = compile_upstreams_to_binary(&upstreams, 12, [0x33u8; 32])
            .expect("Should compile upstreams");

        let (header, restored) =
            unpack_upstreams_from_binary(&binary).expect("Should unpack upstreams");
        assert_eq!(header.revision, 12);
        assert_eq!(restored, upstreams);

        persist_upstreams(tmp.path(), json_bytes, &binary).expect("Should persist upstreams");
        assert!(tmp.path().join("config/upstreams.json").exists());
        assert!(tmp.path().join("runtime/upstreams.bin").exists());
    }

    #[test]
    fn test_upstream_health_check_validation() {
        let json = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "users",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "http1", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500, "idle_ms": 30000 },
                "health_check": {
                    "passive": { "enabled": true, "max_consecutive_failures": 3, "cooldown_ms": 5000 },
                    "active": {
                        "enabled": true, "interval_ms": 2000, "timeout_ms": 500,
                        "unhealthy_threshold": 2, "healthy_threshold": 1,
                        "check_type": "http", "path": "/healthz", "expected_statuses": [200, 204]
                    }
                }
            }]
        }"#;

        let mut upstreams = parse_upstreams(json.as_bytes()).unwrap();
        validate_upstreams(&mut upstreams).unwrap();
        let hc = upstreams[0].health_check.as_ref().unwrap();
        assert_eq!(hc.passive.as_ref().unwrap().cooldown_ms, 5000);
        assert_eq!(
            hc.active.as_ref().unwrap().path.as_deref(),
            Some("/healthz")
        );
    }

    #[test]
    fn test_upstream_health_check_missing_fields_fails() {
        // Missing cooldown_ms in passive config -> deserialization failure (no silent fallback!)
        let json_missing_cooldown = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "u_hc_fail",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "raw", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500, "idle_ms": 30000 },
                "health_check": { "passive": { "enabled": true, "max_consecutive_failures": 3 } }
            }]
        }"#;
        assert!(parse_upstreams(json_missing_cooldown.as_bytes()).is_err());

        // HTTP active health check missing path -> validation failure (no silent default "/healthz"!)
        let json_missing_path = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "u_hc_no_path",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "http1", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500, "idle_ms": 30000 },
                "health_check": {
                    "active": {
                        "enabled": true, "interval_ms": 1000, "timeout_ms": 500,
                        "unhealthy_threshold": 3, "healthy_threshold": 2,
                        "check_type": "http", "expected_statuses": [200]
                    }
                }
            }]
        }"#;
        let mut upstreams = parse_upstreams(json_missing_path.as_bytes()).unwrap();
        let err = validate_upstreams(&mut upstreams).unwrap_err();
        assert!(err.to_string().contains("health_check.active.path"));
    }

    #[test]
    fn test_upstream_timeouts_mandatory_in_json() {
        // Missing entire "timeouts" object -> deserialization failure
        let json_no_timeouts = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "u_no_timeouts",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "raw", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" }
            }]
        }"#;
        assert!(parse_upstreams(json_no_timeouts.as_bytes()).is_err());

        // Missing connect_ms in timeouts -> deserialization failure
        let json_no_connect = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "u_no_connect",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "raw", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "idle_ms": 30000 }
            }]
        }"#;
        assert!(parse_upstreams(json_no_connect.as_bytes()).is_err());

        // Missing idle_ms in timeouts -> deserialization failure
        let json_no_idle = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "u_no_idle",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "raw", "streaming": [] },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500 }
            }]
        }"#;
        assert!(parse_upstreams(json_no_idle.as_bytes()).is_err());

        // Missing streaming field -> deserialization failure
        let json_no_streaming = r#"{
            "schema_version": 1,
            "upstreams": [{
                "id": "u_no_streaming",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "raw" },
                "endpoints": [{ "address": "127.0.0.1:8080", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500, "idle_ms": 30000 }
            }]
        }"#;
        assert!(parse_upstreams(json_no_streaming.as_bytes()).is_err());
    }

    #[test]
    fn test_upstream_tls_auto_extract_sni_and_compile() {
        let certified_key =
            rcgen::generate_simple_self_signed(vec!["backend.internal.svc".into()]).unwrap();
        let ca_cert = certified_key.cert.pem();

        let json = serde_json::json!({
            "schema_version": 1,
            "upstreams": [{
                "id": "secure_upstream",
                "mode": "endpoints",
                "protocol": { "transport": "tcp", "application": "http1", "streaming": [] },
                "endpoints": [{ "address": "10.0.0.1:8443", "weight": 1 }],
                "load_balancer": { "algorithm": "round_robin" },
                "timeouts": { "connect_ms": 500, "idle_ms": 30000 },
                "tls": {
                    "ca_pem": ca_cert,
                    "versions": ["tls1.3"],
                    "alpn": ["h2"]
                }
            }]
        });

        let mut upstreams = parse_upstreams(&serde_json::to_vec(&json).unwrap()).unwrap();
        assert!(
            upstreams[0].tls.as_ref().unwrap().sni.is_empty(),
            "JSON input must not specify sni"
        );

        validate_upstreams(&mut upstreams).unwrap();
        assert_eq!(
            upstreams[0].tls.as_ref().unwrap().sni,
            vec!["backend.internal.svc".to_string()]
        );

        let bin = compile_upstreams_to_binary(&upstreams, 1, [0u8; 32]).unwrap();
        let (_, unpacked) = unpack_upstreams_from_binary(&bin).unwrap();
        assert_eq!(
            unpacked[0].tls.as_ref().unwrap().sni,
            vec!["backend.internal.svc".to_string()]
        );
    }

    #[test]
    fn test_upstream_generic_http_rejected() {
        let mut u = mock_upstream_config("bad_http");
        u.protocol.application = "http".into();
        assert!(validate_upstreams(&mut [u]).is_err());
    }

    #[test]
    fn test_upstream_streaming_validation() {
        let mut invalid_raw = mock_upstream_config("raw-stream");
        invalid_raw.protocol.application = "raw".into();
        invalid_raw.protocol.streaming = velda_core::StreamingMode::SERVER;
        let err = validate_upstreams(&mut [invalid_raw]).unwrap_err();
        assert!(err.to_string().contains("does not support L7 streaming"));

        let mut valid_h2 = mock_upstream_config("h2-stream");
        valid_h2.protocol.application = "http2".into();
        valid_h2.protocol.streaming = velda_core::StreamingMode::SERVER;
        assert!(validate_upstreams(&mut [valid_h2]).is_ok());
    }
}
