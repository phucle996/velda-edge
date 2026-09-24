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
pub const TAG_ROUTES: [u8; 8] = *b"ROUTES\0\0";

/// Self-describing binary header prepended to routes.bin.
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
pub struct RoutesFile {
    pub schema_version: u32,
    pub routes: Vec<RouteConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteConfig {
    pub id: String,
    pub kind: String, // "l7" or "l4"
    pub listener: String,
    #[serde(rename = "match")]
    pub match_rule: RouteMatch,
    #[serde(default)]
    pub timeouts: RouteTimeouts,
    pub upstream: String, // Logical target name; resolved dynamically at runtime
    #[serde(default)]
    pub plugins: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteMatch {
    pub host: Option<String>,
    pub path: Option<String>,
    pub path_prefix: Option<String>,
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RouteTimeouts {
    pub downstream_idle_ms: Option<u64>,
}

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into route configurations.
pub fn parse_routes(payload: &[u8]) -> Result<Vec<RouteConfig>, SyncError> {
    let file: RoutesFile = serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
        domain: "routes".into(),
        reason: e.to_string(),
    })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "routes".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.routes)
}

// ============================================================================
// Phase 3: Integrity Check & Semantic Validation
#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

/// Validates route configuration rules locally without silent fallbacks.
pub fn validate_routes(routes: &mut [RouteConfig]) -> Result<(), SyncError> {
    let mut seen_ids = HashSet::with_capacity(routes.len());

    for route in routes {
        trim_in_place(&mut route.id);
        trim_in_place(&mut route.kind);
        route.kind.make_ascii_lowercase();
        trim_in_place(&mut route.listener);
        trim_in_place(&mut route.upstream);

        if route.id.is_empty() {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: "Route ID cannot be empty".into(),
            });
        }

        if !seen_ids.insert(route.id.as_str()) {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: format!("Duplicate route ID '{}'", route.id),
            });
        }

        let is_l7 = route.kind == "l7";
        let is_l4 = route.kind == "l4";
        if !is_l7 && !is_l4 {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: format!(
                    "Route '{}' has invalid kind '{}'; must be explicitly 'l7' or 'l4'",
                    route.id, route.kind
                ),
            });
        }

        if route.listener.is_empty() {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: format!("Route '{}' must specify a non-empty listener", route.id),
            });
        }

        if route.upstream.is_empty() {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: format!(
                    "Route '{}' must specify a non-empty upstream target",
                    route.id
                ),
            });
        }

        if is_l7 {
            let has_match = route
                .match_rule
                .host
                .as_ref()
                .map(|h| !h.trim().is_empty())
                .unwrap_or(false)
                || route
                    .match_rule
                    .path
                    .as_ref()
                    .map(|p| !p.trim().is_empty())
                    .unwrap_or(false)
                || route
                    .match_rule
                    .path_prefix
                    .as_ref()
                    .map(|p| !p.trim().is_empty())
                    .unwrap_or(false);

            if !has_match {
                return Err(SyncError::Validation {
                    domain: "routes".into(),
                    reason: format!(
                        "Route '{}' is L7 but specifies no host, path, or path_prefix match rule",
                        route.id
                    ),
                });
            }
        }

        if is_l4 {
            let has_proto = route
                .match_rule
                .protocol
                .as_ref()
                .map(|p| !p.trim().is_empty())
                .unwrap_or(false);

            if !has_proto {
                return Err(SyncError::Validation {
                    domain: "routes".into(),
                    reason: format!(
                        "Route '{}' is L4 but specifies no protocol match rule",
                        route.id
                    ),
                });
            }
        }

        if let Some(prefix) = &route.match_rule.path_prefix
            && !prefix.starts_with('/')
        {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: format!(
                    "Route '{}' path_prefix '{}' must start with '/'",
                    route.id, prefix
                ),
            });
        }

        if let Some(path) = &route.match_rule.path
            && !path.starts_with('/')
        {
            return Err(SyncError::Validation {
                domain: "routes".into(),
                reason: format!("Route '{}' path '{}' must start with '/'", route.id, path),
            });
        }
    }

    Ok(())
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles route configurations into a self-describing binary artifact (routes.bin).
pub fn compile_routes_to_binary(
    routes: &[RouteConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    // Single contiguous buffer: reserve header slot then serialize payload directly
    let mut binary_output = vec![0; DOMAIN_HEADER_SIZE];

    bincode::serialize_into(&mut binary_output, routes).map_err(|e| SyncError::Compile {
        domain: "routes".into(),
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
        domain_tag: TAG_ROUTES,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "routes".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled routes binary artifact.
pub fn unpack_routes_from_binary(
    bytes: &[u8],
) -> Result<(DomainHeader, Vec<RouteConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "routes".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "routes".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "routes".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_ROUTES {
        return Err(SyncError::Compile {
            domain: "routes".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "routes".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "routes".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let routes: Vec<RouteConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "routes".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    Ok((header, routes))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists routes canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_routes(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("routes.json");
    let bin_path = storage_dir.join("runtime").join("routes.bin");

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
    fn test_parse_valid_routes() {
        let json = r#"{
            "schema_version": 1,
            "routes": [
                {
                    "id": " api ",
                    "kind": "l7",
                    "listener": "https",
                    "match": {
                        "host": "api.example.com",
                        "path_prefix": "/api"
                    },
                    "upstream": " users "
                }
            ]
        }"#;

        let mut routes = parse_routes(json.as_bytes()).unwrap();
        assert_eq!(routes.len(), 1);
        validate_routes(&mut routes).unwrap();
        assert_eq!(routes[0].id, "api");
        assert_eq!(routes[0].upstream, "users");
    }

    #[test]
    fn test_invalid_path_prefix_fails() {
        let mut routes = vec![RouteConfig {
            id: "r1".into(),
            kind: "l7".into(),
            listener: "http".into(),
            match_rule: RouteMatch {
                path_prefix: Some("invalid_path".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "users".into(),
            plugins: vec![],
        }];

        assert!(matches!(
            validate_routes(&mut routes),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_invalid_kind_fails() {
        let mut routes = vec![RouteConfig {
            id: "r1".into(),
            kind: "invalid_kind".into(),
            listener: "http".into(),
            match_rule: RouteMatch {
                host: Some("example.com".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "users".into(),
            plugins: vec![],
        }];

        assert!(matches!(
            validate_routes(&mut routes),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_l7_without_match_fails() {
        let mut routes = vec![RouteConfig {
            id: "r1".into(),
            kind: "l7".into(),
            listener: "http".into(),
            match_rule: RouteMatch::default(),
            timeouts: Default::default(),
            upstream: "users".into(),
            plugins: vec![],
        }];

        assert!(matches!(
            validate_routes(&mut routes),
            Err(SyncError::Validation { .. })
        ));
    }

    #[test]
    fn test_route_binary_roundtrip_and_persistence() {
        let tmp = tempdir().unwrap();
        let routes = vec![RouteConfig {
            id: "api".into(),
            kind: "l7".into(),
            listener: "https".into(),
            match_rule: RouteMatch {
                host: Some("api.example.com".into()),
                path_prefix: Some("/api".into()),
                ..Default::default()
            },
            timeouts: RouteTimeouts {
                downstream_idle_ms: Some(30000),
            },
            upstream: "backend".into(),
            plugins: vec!["rate-limit".into()],
        }];

        let json_bytes = b"{\"routes\": []}";
        let binary =
            compile_routes_to_binary(&routes, 42, [0x22u8; 32]).expect("Should compile routes");

        let (header, restored) = unpack_routes_from_binary(&binary).expect("Should unpack routes");
        assert_eq!(header.revision, 42);
        assert_eq!(restored, routes);

        persist_routes(tmp.path(), json_bytes, &binary).expect("Should persist routes");
        assert!(tmp.path().join("config/routes.json").exists());
        assert!(tmp.path().join("runtime/routes.bin").exists());
    }
}
