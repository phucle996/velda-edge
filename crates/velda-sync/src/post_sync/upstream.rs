use std::collections::HashSet;
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
pub struct UpstreamConfig {
    pub id: String,
    pub mode: String, // "dns" or "endpoints"
    pub target: Option<DnsTarget>,
    pub resolver: Option<ResolverConfig>,
    #[serde(default)]
    pub endpoints: Vec<EndpointConfig>,
    pub load_balancer: LoadBalancerConfig,
    #[serde(default)]
    pub timeouts: UpstreamTimeouts,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpstreamTimeouts {
    pub connect_ms: Option<u64>,
    pub request_ms: Option<u64>,
    pub idle_ms: Option<u64>,
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
    }

    Ok(())
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
    // Single contiguous buffer: reserve header slot then serialize payload directly
    let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];

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

    #[test]
    fn test_parse_and_validate_upstreams() {
        let json = r#"{
            "schema_version": 1,
            "upstreams": [
                {
                    "id": " users ",
                    "mode": "endpoints",
                    "endpoints": [
                        { "address": "127.0.0.1:8080", "weight": 2 }
                    ],
                    "load_balancer": {
                        "algorithm": "round_robin"
                    }
                }
            ]
        }"#;

        let mut upstreams = parse_upstreams(json.as_bytes()).unwrap();
        assert_eq!(upstreams.len(), 1);
        validate_upstreams(&mut upstreams).unwrap();
        assert_eq!(upstreams[0].id, "users");
        assert_eq!(upstreams[0].load_balancer.algorithm, "round_robin");
    }

    #[test]
    fn test_invalid_endpoint_weight_fails() {
        let mut upstreams = vec![UpstreamConfig {
            id: "u1".into(),
            mode: "endpoints".into(),
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "127.0.0.1:8080".into(),
                weight: 0,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: Default::default(),
        }];

        assert!(matches!(
            validate_upstreams(&mut upstreams),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_unsupported_lb_algorithm_fails() {
        let mut upstreams = vec![UpstreamConfig {
            id: "u1".into(),
            mode: "endpoints".into(),
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "127.0.0.1:8080".into(),
                weight: 1,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "magic_balance".into(),
            },
            timeouts: Default::default(),
        }];

        assert!(matches!(
            validate_upstreams(&mut upstreams),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_dns_missing_target_fails() {
        let mut upstreams = vec![UpstreamConfig {
            id: "u_dns".into(),
            mode: "dns".into(),
            target: None,
            resolver: None,
            endpoints: vec![],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".into(),
            },
            timeouts: Default::default(),
        }];

        assert!(matches!(
            validate_upstreams(&mut upstreams),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_upstream_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let upstreams = vec![UpstreamConfig {
            id: "users".into(),
            mode: "endpoints".into(),
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: "10.0.0.1:8080".into(),
                weight: 5,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "least_connections".into(),
            },
            timeouts: UpstreamTimeouts {
                connect_ms: Some(2000),
                request_ms: None,
                idle_ms: Some(15000),
            },
        }];

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
}
