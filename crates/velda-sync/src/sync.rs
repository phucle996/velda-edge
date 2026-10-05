//! Stage 2: Sync (Reconciliation & Orchestration Engine).
//!
//! Orchestrates reading candidate manifest from Pre-Sync, detecting deltas against
//! in-memory state, delegating changed domains to Post-Sync compilers, persisting durable
//! LKG manifest, and notifying Data Plane workers via UDS IPC.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::SyncError;
use crate::ipc::{self, IpcNotifier};
use crate::post_sync;
use crate::pre_sync::PreSyncStage;
use crate::provider::Provider;

/// Outcome of a reconciliation cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncOutcome {
    Updated { changed_domains: Vec<String> },
    Unchanged,
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

/// Orchestrates the 5-phase reconciliation cycle across Pre-Sync and Post-Sync stages.
pub struct SyncComposition {
    pub stage: PreSyncStage,
    pub storage_dir: PathBuf,
    pub notifier: IpcNotifier,
    domain_revisions: HashMap<String, u64>,
    domain_checksums: HashMap<String, [u8; 32]>,
    manifest_revision: Option<u64>,
    manifest_checksum: Option<[u8; 32]>,
}

impl SyncComposition {
    /// Creates a composition reconciler backed by an acquisition Provider.
    pub fn new(
        provider: Provider,
        storage_dir: impl Into<PathBuf>,
        socket_path: impl Into<PathBuf>,
    ) -> Self {
        let storage = storage_dir.into();
        let staging_dir = storage.join("staging");
        let stage = PreSyncStage::new(provider, staging_dir);
        Self::with_stage(stage, storage, socket_path)
    }

    /// Creates a composition reconciler backed by an explicit PreSyncStage.
    pub fn with_stage(
        stage: PreSyncStage,
        storage_dir: impl Into<PathBuf>,
        socket_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            stage,
            storage_dir: storage_dir.into(),
            notifier: IpcNotifier::new(socket_path),
            domain_revisions: HashMap::new(),
            domain_checksums: HashMap::new(),
            manifest_revision: None,
            manifest_checksum: None,
        }
    }

    /// Reconciles configuration: only executes phases for domains that actually changed.
    pub async fn reconcile(&mut self) -> Result<SyncOutcome, SyncError> {
        // ====================================================================
        // Phase 1: Acquire Candidate Manifest from Pre-Sync Stage
        // ====================================================================
        let (manifest, manifest_bytes) = self.stage.acquire_manifest().await?;

        let mut manifest_hasher = Sha256::new();
        manifest_hasher.update(&manifest_bytes);
        let manifest_checksum: [u8; 32] = manifest_hasher.finalize().into();

        // Fast Short-Circuit: If manifest revision and manifest SHA-256 match current state,
        // configuration is completely unchanged. Return immediately without reading or hashing
        // domain files.
        if let (Some(curr_rev), Some(curr_hash)) = (self.manifest_revision, self.manifest_checksum)
            && manifest.revision == curr_rev
            && manifest_checksum == curr_hash
        {
            return Ok(SyncOutcome::Unchanged);
        }

        let num_files = manifest.configuration.files.len();
        let mut changed_domains = Vec::with_capacity(num_files);
        let mut domain_revisions = HashMap::with_capacity(num_files);
        let mut domain_bins = HashMap::with_capacity(num_files);

        // ====================================================================
        // Phase 2: Delta & Revision Detection (Identify Changed Domains)
        // ====================================================================
        for file_entry in &manifest.configuration.files {
            let content = match self.stage.read_domain_file(&file_entry.path).await {
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
                // ============================================================
                // Phase 3: Dispatch Changed Domains to Post-Sync Compilers
                // ============================================================
                match file_entry.name.as_str() {
                    "listeners" => {
                        let mut listeners = post_sync::listener::parse_listeners(&content)?;
                        let lkg_path = self.storage_dir.join("runtime/listeners.bin");
                        let current_lkg = if lkg_path.exists() {
                            std::fs::read(&lkg_path)
                                .ok()
                                .and_then(|bytes| {
                                    post_sync::listener::unpack_listeners_from_binary(&bytes).ok()
                                })
                                .map(|(_, list)| list)
                                .unwrap_or_default()
                        } else {
                            Vec::new()
                        };
                        post_sync::listener::validate_listeners_with_lkg(
                            &mut listeners,
                            &current_lkg,
                        )?;
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
            self.manifest_revision = Some(manifest.revision);
            self.manifest_checksum = Some(manifest_checksum);
            return Ok(SyncOutcome::Unchanged);
        }

        // ====================================================================
        // Phase 4: Durable LKG Manifest Finalization
        // ====================================================================
        let manifest_lkg = self.storage_dir.join("config").join("manifest.json");
        write_atomic(&manifest_lkg, &manifest_bytes)?;

        // ====================================================================
        // Phase 5: Hot-Reload IPC Notification to Data Plane
        // ====================================================================
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
        self.manifest_checksum = Some(manifest_checksum);

        Ok(SyncOutcome::Updated { changed_domains })
    }
}
