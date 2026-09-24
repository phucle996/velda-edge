//! Velda Configuration Reconciler and Domain Composition Engine (velda-sync).
//!
//! Organizes configuration processing strictly by subsystem domain owners
//! (`listener`, `route`, `upstream`, `plugin`, `tls`).
//!
//! Subsystems independently handle parse, validation, binary compilation, and persistence.
//! Composition coordinates reading manifest via Providers (`local_file`, `control_plane`),
//! delta detection, and UDS IPC notifications.

pub mod ipc;
pub mod post_sync;
pub mod provider;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use ipc::{
    IpcNotifier, IpcSignalReceiver, IpcSignalSender, SyncNotification, create_ipc_channel,
    notify_cycle_completed, run_ipc_signal_worker,
};

pub use provider::{ControlPlaneProvider, LocalFileProvider, Provider, SyncMode};

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

fn write_atomic(target: &Path, content: &[u8]) -> Result<(), SyncError> {
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
    Ok(())
}

// ============================================================================
// Composition Engine
// ============================================================================

/// Outcome of a reconciliation cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    Updated { changed_domains: Vec<String> },
    Unchanged,
}

/// Orchestrates reading manifest, delegating to domain owners, and publishing updates.
pub struct SyncComposition {
    pub provider: Provider,
    pub storage_dir: PathBuf,
    pub notifier: IpcNotifier,
    domain_revisions: HashMap<String, u64>,
    domain_checksums: HashMap<String, [u8; 32]>,
    manifest_revision: Option<u64>,
}

impl SyncComposition {
    /// Creates a composition reconciler backed by an acquisition Provider.
    pub fn new(
        provider: Provider,
        storage_dir: impl Into<PathBuf>,
        socket_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            provider,
            storage_dir: storage_dir.into(),
            notifier: IpcNotifier::new(socket_path),
            domain_revisions: HashMap::new(),
            domain_checksums: HashMap::new(),
            manifest_revision: None,
        }
    }

    /// Reconciles configuration: only executes phases for domains that actually changed.
    pub async fn reconcile(&mut self) -> Result<SyncOutcome, SyncError> {
        let (manifest, manifest_bytes) = self.provider.read_manifest().await?;

        let num_files = manifest.configuration.files.len();
        let mut changed_domains = Vec::with_capacity(num_files);
        let mut domain_revisions = HashMap::with_capacity(num_files);
        let mut domain_bins = HashMap::with_capacity(num_files);

        for file_entry in &manifest.configuration.files {
            let content = match self.provider.read_file(&file_entry.path).await {
                Ok(bytes) => bytes,
                Err(err) => {
                    if file_entry.required {
                        return Err(SyncError::Manifest(format!(
                            "Required configuration file '{}' missing at '{}': {err}",
                            file_entry.name, file_entry.path
                        )));
                    }
                    continue;
                }
            };

            let mut hasher = Sha256::new();
            hasher.update(&content);
            let checksum: [u8; 32] = hasher.finalize().into();

            let domain_rev = file_entry.revision.unwrap_or(manifest.revision);

            let is_newer = match (
                self.domain_revisions.get(&file_entry.name).copied(),
                self.domain_checksums.get(&file_entry.name),
            ) {
                (Some(curr_rev), Some(curr_hash)) => {
                    if curr_hash == &checksum {
                        false
                    } else {
                        domain_rev > curr_rev
                    }
                }
                _ => true,
            };

            if is_newer {
                // Execute domain-specific pipeline (Parse -> Validate -> Compile -> Persist)
                match file_entry.name.as_str() {
                    "listeners" => {
                        let mut listeners = post_sync::listener::parse_listeners(&content)?;
                        post_sync::listener::validate_listeners(&mut listeners)?;
                        let bin = post_sync::listener::compile_listeners_to_binary(
                            &listeners, domain_rev, checksum,
                        )?;
                        post_sync::listener::persist_listeners(&self.storage_dir, &content, &bin)?;
                    }
                    "routes" => {
                        let mut routes = post_sync::route::parse_routes(&content)?;
                        post_sync::route::validate_routes(&mut routes)?;
                        let bin = post_sync::route::compile_routes_to_binary(
                            &routes, domain_rev, checksum,
                        )?;
                        post_sync::route::persist_routes(&self.storage_dir, &content, &bin)?;
                    }
                    "upstreams" => {
                        let mut upstreams = post_sync::upstream::parse_upstreams(&content)?;
                        post_sync::upstream::validate_upstreams(&mut upstreams)?;
                        let bin = post_sync::upstream::compile_upstreams_to_binary(
                            &upstreams, domain_rev, checksum,
                        )?;
                        post_sync::upstream::persist_upstreams(&self.storage_dir, &content, &bin)?;
                    }
                    "plugins" => {
                        let mut plugins = post_sync::plugin::parse_plugins(&content)?;
                        post_sync::plugin::validate_plugins(&mut plugins)?;
                        let bin = post_sync::plugin::compile_plugins_to_binary(
                            &plugins, domain_rev, checksum,
                        )?;
                        post_sync::plugin::persist_plugins(&self.storage_dir, &content, &bin)?;
                    }
                    "tls" => {
                        let mut tls = post_sync::tls::parse_tls(&content)?;
                        post_sync::tls::validate_tls(&mut tls)?;
                        let bin =
                            post_sync::tls::compile_tls_to_binary(&tls, domain_rev, checksum)?;
                        post_sync::tls::persist_tls(&self.storage_dir, &content, &bin)?;
                    }
                    other => {
                        return Err(SyncError::Manifest(format!("Unsupported domain '{other}'")));
                    }
                }

                let bin_path_str = self
                    .storage_dir
                    .join("runtime")
                    .join(format!("{}.bin", file_entry.name))
                    .display()
                    .to_string();

                changed_domains.push(file_entry.name.clone());
                domain_revisions.insert(file_entry.name.clone(), domain_rev);
                domain_bins.insert(file_entry.name.clone(), bin_path_str);

                self.domain_revisions
                    .insert(file_entry.name.clone(), domain_rev);
                self.domain_checksums
                    .insert(file_entry.name.clone(), checksum);
            }
        }

        if changed_domains.is_empty() {
            return Ok(SyncOutcome::Unchanged);
        }

        // Persist manifest.json to LKG
        let manifest_lkg = self.storage_dir.join("config").join("manifest.json");
        write_atomic(&manifest_lkg, &manifest_bytes)?;

        // Conclude sync cycle: dispatch IPC notification to Data Plane
        let runtime_dir = self.storage_dir.join("runtime").display().to_string();
        ipc::notify_cycle_completed(
            &self.notifier,
            Some(manifest.revision),
            runtime_dir,
            changed_domains.clone(),
            domain_revisions,
            domain_bins,
        )
        .await?;

        self.manifest_revision = Some(manifest.revision);

        Ok(SyncOutcome::Updated { changed_domains })
    }
}
