//! Velda Configuration Reconciler and Domain Composition Engine (velda-sync).
//!
//! Organized around three linear stages:
//! - **Stage 1: Pre-Sync** (`pre_sync`): Candidate configuration acquisition, staging, and LKG fallback.
//! - **Stage 2: Sync** (`sync`): Delta detection, revision verification, domain delegation, and IPC dispatch.
//! - **Stage 3: Post-Sync** (`post_sync`): Domain compilers (`route`, `listener`, `upstream`, `plugin`, `tls`).

pub mod ipc;
pub mod post_sync;
pub mod pre_sync;
pub mod provider;
pub mod sync;

use serde::{Deserialize, Serialize};

pub use ipc::{IpcNotifier, SyncNotification, notify_cycle_completed};

pub use pre_sync::PreSyncStage;
pub use provider::{ControlPlaneProvider, LocalFileProvider, Provider};
pub use sync::{SyncComposition, SyncOutcome};

pub use post_sync::listener::{
    DomainHeader as ListenerDomainHeader, ListenerConfig, ListenerTlsConfig, ListenersFile,
};
pub use post_sync::plugin::{DomainHeader as PluginDomainHeader, PluginConfig, PluginsFile};
pub use post_sync::route::{
    DomainHeader as RouteDomainHeader, RouteConfig, RouteMatch, RouteTimeouts, RoutesFile,
};
pub use post_sync::tls::{
    CertificateFiles, DomainHeader as TlsDomainHeader, TlsFile, TlsProfileConfig,
};
pub use post_sync::upstream::{
    DnsTarget, DomainHeader as UpstreamDomainHeader, EndpointConfig, LoadBalancerConfig,
    ResolverConfig, UpstreamConfig, UpstreamTimeouts, UpstreamsFile,
};

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON format error in {domain}: {reason}")]
    InvalidJson { domain: String, reason: String },

    #[error("Validation error in {domain}: {reason}")]
    Validation { domain: String, reason: String },

    #[error("Compilation error in {domain}: {reason}")]
    Compile { domain: String, reason: String },

    #[error("Manifest error: {0}")]
    Manifest(String),

    #[error("Notification error: {0}")]
    Notify(String),
}

// ============================================================================
// Manifest
// ============================================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestConfig {
    pub schema_version: u32,
    pub revision: u64,
    pub configuration: ManifestFiles,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFiles {
    pub files: Vec<ManifestFileEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFileEntry {
    pub name: String,
    pub path: String,
    pub required: bool,
    #[serde(default)]
    pub revision: Option<u64>,
    #[serde(default)]
    pub hash: Option<String>,
}
