//! Configuration contract for distributed quota & metering synchronization.

use serde_json::Value;
use std::net::SocketAddr;

/// Static configuration for distributed Redis synchronization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DistributedSyncConfig {
    /// Enables distributed multi-gateway synchronization via Redis.
    pub enabled: bool,
    /// Target Redis server address.
    pub redis_addr: SocketAddr,
    /// Batch reconciliation interval in milliseconds (default: 1000ms).
    pub sync_interval_ms: u64,
    /// Namespace prefix applied to Redis keys (default: "velda").
    pub key_prefix: String,
}

impl Default for DistributedSyncConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            redis_addr: SocketAddr::from(([127, 0, 0, 1], 6379)),
            sync_interval_ms: 1000,
            key_prefix: "velda".to_string(),
        }
    }
}

impl DistributedSyncConfig {
    /// Extracts distributed configuration from plugin JSON definition.
    pub fn from_json(val: Option<&Value>) -> Self {
        let val = match val {
            Some(v) => v,
            None => return Self::default(),
        };

        let enabled = val
            .get("enabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let redis_addr = val
            .get("redis_addr")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<SocketAddr>().ok())
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 6379)));

        let sync_interval_ms = val
            .get("sync_interval_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000);

        let key_prefix = val
            .get("key_prefix")
            .and_then(|v| v.as_str())
            .unwrap_or("velda")
            .to_string();

        Self {
            enabled,
            redis_addr,
            sync_interval_ms,
            key_prefix,
        }
    }
}
