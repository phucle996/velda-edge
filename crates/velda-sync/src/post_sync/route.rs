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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct IngestRoutesFile {
    pub schema_version: u32,
    pub routes: Vec<IngestRouteConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct IngestRouteConfig {
    pub id: String,
    pub kind: String,
    pub listener: String,
    #[serde(rename = "match")]
    pub match_rule: RouteMatch,
    #[serde(default)]
    pub timeouts: RouteTimeouts,
    pub upstream: String,
    #[serde(default, deserialize_with = "deserialize_route_plugins")]
    pub plugins: Vec<String>,
}

fn deserialize_route_plugins<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct RoutePluginsVisitor;

    impl<'de> serde::de::Visitor<'de> for RoutePluginsVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str(
                "a list of plugin names or a phased object { pre_upstream: [...], post_response: [...] }",
            )
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut list = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                list.push(item);
            }
            Ok(list)
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut pre_upstream = Vec::new();
            let mut post_response = Vec::new();

            while let Some(key) = map.next_key::<String>()? {
                match key.as_str() {
                    "pre_upstream" => {
                        pre_upstream = map.next_value::<Vec<String>>()?;
                    }
                    "post_response" => {
                        post_response = map.next_value::<Vec<String>>()?;
                    }
                    _ => {
                        let _ = map.next_value::<serde_json::Value>()?;
                    }
                }
            }

            let mut result = Vec::with_capacity(pre_upstream.len() + post_response.len());
            result.extend(pre_upstream);
            result.extend(post_response);
            Ok(result)
        }
    }

    deserializer.deserialize_any(RoutePluginsVisitor)
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
    let file: IngestRoutesFile =
        serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
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

    Ok(file
        .routes
        .into_iter()
        .map(|r| RouteConfig {
            id: r.id,
            kind: r.kind,
            listener: r.listener,
            match_rule: r.match_rule,
            timeouts: r.timeouts,
            upstream: r.upstream,
            plugins: r.plugins,
        })
        .collect())
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

        // Protocol-exclusive plugin validation when route match rule specifies protocol
        if let Some(proto) = &route.match_rule.protocol {
            let proto_lower = proto.to_ascii_lowercase();
            for plugin_id in &route.plugins {
                if let Some(expected_proto) =
                    super::plugin::canonical_protocol_family_for_id(plugin_id)
                {
                    if expected_proto == "grpc" && proto_lower != "grpc" {
                        return Err(SyncError::Validation {
                            domain: "routes".into(),
                            reason: format!(
                                "Route '{}': plugin '{}' can only be applied to 'grpc' routes, found protocol '{}'",
                                route.id, plugin_id, proto
                            ),
                        });
                    }
                    if expected_proto == "http"
                        && (proto_lower == "grpc"
                            || proto_lower == "raw"
                            || proto_lower == "tcp"
                            || proto_lower == "udp")
                    {
                        return Err(SyncError::Validation {
                            domain: "routes".into(),
                            reason: format!(
                                "Route '{}': plugin '{}' cannot be applied to '{}' routes; only HTTP family is supported",
                                route.id, plugin_id, proto
                            ),
                        });
                    }
                }
            }
        }
    }

    Ok(())
}

/// Validates cross-domain protocol compatibility between routes and listeners.
///
/// Enforces that protocol-exclusive plugins (e.g. `grpc_fault_injection` strictly for gRPC,
/// `http_fault_injection`, `http_cache_lookup`, `http_cache_store` strictly for HTTP) are never attached to mismatched listeners.
pub fn validate_routes_with_listeners(
    routes: &[RouteConfig],
    listeners: &[super::listener::ListenerConfig],
) -> Result<(), SyncError> {
    use std::collections::HashMap;
    let listener_map: HashMap<&str, &str> = listeners
        .iter()
        .map(|l| (l.id.as_str(), l.application.protocol.as_str()))
        .collect();

    for route in routes {
        if let Some(&listener_proto) = listener_map.get(route.listener.as_str()) {
            for plugin_id in &route.plugins {
                if let Some(expected_proto) =
                    super::plugin::canonical_protocol_family_for_id(plugin_id)
                    && listener_proto != expected_proto
                {
                    return Err(SyncError::Validation {
                        domain: "routes".into(),
                        reason: format!(
                            "Route '{}' targets listener '{}' with application protocol '{}', but configures plugin '{}' which is strictly restricted to '{}'",
                            route.id, route.listener, listener_proto, plugin_id, expected_proto
                        ),
                    });
                }
            }
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
    // OPTIMIZATION: Pre-allocate contiguous buffer with estimated capacity (header + 128B per route)
    // to avoid multiple vector reallocations during streaming bincode serialization.
    let estimated_cap = DOMAIN_HEADER_SIZE + routes.len().saturating_mul(128);
    let mut binary_output = Vec::with_capacity(estimated_cap);
    binary_output.resize(DOMAIN_HEADER_SIZE, 0);

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
    fn test_route_binary_roundtrip() {
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

    #[test]
    fn test_parse_phased_and_flat_route_plugins() {
        let phased_json = br#"{
            "schema_version": 1,
            "routes": [
                {
                    "id": "phased-api",
                    "kind": "l7",
                    "listener": "http-in",
                    "match": { "path_prefix": "/api" },
                    "upstream": "users",
                    "plugins": {
                        "pre_upstream": ["jwt_auth", "rate_limit"],
                        "post_response": ["security_headers", "access_log"]
                    }
                },
                {
                    "id": "flat-api",
                    "kind": "l7",
                    "listener": "http-in",
                    "match": { "path_prefix": "/flat" },
                    "upstream": "users",
                    "plugins": ["jwt_auth", "rate_limit"]
                }
            ]
        }"#;

        let routes = parse_routes(phased_json).expect("phased and flat plugins must parse");
        assert_eq!(routes.len(), 2);
        assert_eq!(
            routes[0].plugins,
            vec!["jwt_auth", "rate_limit", "security_headers", "access_log"]
        );
        assert_eq!(routes[1].plugins, vec!["jwt_auth", "rate_limit"]);
    }

    #[test]
    fn test_grpc_and_http_fault_injection_protocol_compatibility() {
        use super::super::listener::{
            ListenerApplicationConfig, ListenerConfig, ListenerTransportConfig,
        };

        let listeners = vec![
            ListenerConfig {
                id: "grpc-in".into(),
                address: "0.0.0.0:50051".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "grpc".into(),
                    version: None,
                    streaming: velda_core::StreamingMode::DISABLED,
                },
                tls: Default::default(),
                http1: None,
                http2: None,
                http3: None,
                grpc: None,
                raw: None,
            },
            ListenerConfig {
                id: "http-in".into(),
                address: "0.0.0.0:80".into(),
                transport: ListenerTransportConfig {
                    protocol: "tcp".into(),
                },
                application: ListenerApplicationConfig {
                    protocol: "http".into(),
                    version: None,
                    streaming: velda_core::StreamingMode::DISABLED,
                },
                tls: Default::default(),
                http1: None,
                http2: None,
                http3: None,
                grpc: None,
                raw: None,
            },
        ];

        // 1. Valid gRPC route with grpc_fault_injection
        let valid_grpc_routes = vec![RouteConfig {
            id: "grpc-route".into(),
            kind: "l7".into(),
            listener: "grpc-in".into(),
            match_rule: RouteMatch {
                path_prefix: Some("/order.v1.OrderService".into()),
                protocol: Some("grpc".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "order-grpc".into(),
            plugins: vec!["grpc_fault_injection".into()],
        }];
        assert!(validate_routes_with_listeners(&valid_grpc_routes, &listeners).is_ok());

        // 2. Invalid: grpc_fault_injection on an HTTP listener -> MUST FAIL!
        let invalid_grpc_routes = vec![RouteConfig {
            id: "bad-http-route".into(),
            kind: "l7".into(),
            listener: "http-in".into(),
            match_rule: RouteMatch {
                path_prefix: Some("/api/v1".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "web-backend".into(),
            plugins: vec!["grpc_fault_injection".into()],
        }];
        let err = validate_routes_with_listeners(&invalid_grpc_routes, &listeners);
        assert!(err.is_err());
        assert!(
            err.unwrap_err()
                .to_string()
                .contains("strictly restricted to 'grpc'")
        );

        // 3. Invalid: http_fault_injection on a gRPC listener -> MUST FAIL!
        let invalid_http_routes = vec![RouteConfig {
            id: "bad-grpc-route".into(),
            kind: "l7".into(),
            listener: "grpc-in".into(),
            match_rule: RouteMatch {
                path_prefix: Some("/payment.v1.PaymentService".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "payment-grpc".into(),
            plugins: vec!["http_fault_injection".into()],
        }];
        let err2 = validate_routes_with_listeners(&invalid_http_routes, &listeners);
        assert!(err2.is_err());
        assert!(
            err2.unwrap_err()
                .to_string()
                .contains("strictly restricted to 'http'")
        );

        // 4. Invalid: http_cache_lookup, http_cache_store, response_header_mutation on a gRPC listener -> MUST FAIL!
        let invalid_cache_routes = vec![RouteConfig {
            id: "bad-grpc-cache-route".into(),
            kind: "l7".into(),
            listener: "grpc-in".into(),
            match_rule: RouteMatch {
                path_prefix: Some("/order.v1.OrderService".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "order-grpc".into(),
            plugins: vec![
                "http_cache_lookup".into(),
                "http_cache_store".into(),
                "response_header_mutation".into(),
            ],
        }];
        let err3 = validate_routes_with_listeners(&invalid_cache_routes, &listeners);
        assert!(err3.is_err());
        assert!(
            err3.unwrap_err()
                .to_string()
                .contains("strictly restricted to 'http'")
        );

        // 5. Invalid: request_termination on a gRPC listener -> MUST FAIL!
        let invalid_term_routes = vec![RouteConfig {
            id: "bad-grpc-term-route".into(),
            kind: "l7".into(),
            listener: "grpc-in".into(),
            match_rule: RouteMatch {
                path_prefix: Some("/order.v1.OrderService".into()),
                ..Default::default()
            },
            timeouts: Default::default(),
            upstream: "order-grpc".into(),
            plugins: vec!["request_termination".into()],
        }];
        let err4 = validate_routes_with_listeners(&invalid_term_routes, &listeners);
        assert!(err4.is_err());
        assert!(
            err4.unwrap_err()
                .to_string()
                .contains("strictly restricted to 'http'")
        );
    }
}
