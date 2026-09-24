//! Hot reload coordinator and atomic runtime swapping.
//!
//! When `velda-sync` broadcasts a [`SyncNotification`] over UDS, this module:
//! 1. Inspects modified domain files.
//! 2. Unpacks the changed binary artifacts (*.bin) into a candidate [`Runtime`].
//! 3. Re-uses unchanged domain state from the current runtime snapshot.
//! 4. Validates the candidate runtime.
//! 5. Performs a lock-free atomic pointer swap (`ArcSwap::store`).

use std::path::Path;
use std::sync::Arc;

use velda_sync::ipc::SyncNotification;

use crate::config::{
    EdgeError, load_listeners, load_plugins, load_routes, load_tls, load_upstreams,
};
use crate::runtime::{Runtime, SharedRuntime};

/// Summary of a successfully applied hot reload operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadOutcome {
    /// New manifest revision number.
    pub revision: u64,
    /// List of domains updated in this cycle.
    pub changed_domains: Vec<String>,
    /// Whether the listener definitions were modified (requiring socket adjustments).
    pub listeners_changed: bool,
}

/// Applies a reload notification from `velda-sync` against the active shared runtime.
pub fn apply_reload(
    shared_runtime: &SharedRuntime,
    runtime_dir: &Path,
    notif: &SyncNotification,
) -> Result<ReloadOutcome, EdgeError> {
    let current = shared_runtime.load();
    let new_revision = notif.manifest_revision.unwrap_or(current.revision + 1);

    let mut listeners = current.listeners.clone();
    let mut routes = current.routes.clone();
    let mut upstreams = current.upstreams.clone();
    let mut plugins = current.plugins.clone();
    let mut tls = current.tls.clone();

    let mut listeners_changed = false;

    for domain in &notif.changed_domains {
        match domain.as_str() {
            "listeners" => {
                listeners = load_listeners(runtime_dir)?;
                listeners_changed = true;
            }
            "routes" => {
                routes = load_routes(runtime_dir)?;
            }
            "upstreams" => {
                upstreams = load_upstreams(runtime_dir)?;
            }
            "plugins" => {
                plugins = load_plugins(runtime_dir)?;
            }
            "tls" => {
                tls = load_tls(runtime_dir)?;
            }
            other => {
                tracing::warn!(domain = %other, "Unknown domain in reload notification; skipping");
            }
        }
    }

    let candidate = Runtime {
        revision: new_revision,
        listeners,
        routes,
        upstreams,
        plugins,
        tls,
    };

    // Pre-validate that all declared listener addresses parse cleanly into IngressBindings
    candidate.active_bindings()?;

    // Lock-free atomic swap of active runtime snapshot
    shared_runtime.store(Arc::new(candidate));

    tracing::info!(
        revision = new_revision,
        changed_domains = ?notif.changed_domains,
        listeners_changed = listeners_changed,
        "Successfully reloaded runtime state via atomic swap"
    );

    Ok(ReloadOutcome {
        revision: new_revision,
        changed_domains: notif.changed_domains.clone(),
        listeners_changed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::new_shared_runtime;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use velda_sync::post_sync::listener::{ListenerConfig, compile_listeners_to_binary};

    #[test]
    fn test_apply_reload_atomic_swap() {
        let tmp = tempdir().unwrap();
        let runtime_dir = tmp.path();

        // Write a compiled listeners.bin into runtime_dir
        let listeners = vec![ListenerConfig {
            id: "http-reloaded".into(),
            address: "127.0.0.1:8080".into(),
            protocol: "http".into(),
            tls: Default::default(),
        }];
        let bin = compile_listeners_to_binary(&listeners, 10, [0u8; 32]).unwrap();
        std::fs::write(runtime_dir.join("listeners.bin"), bin).unwrap();

        let initial_runtime = Runtime::empty();
        let shared = new_shared_runtime(initial_runtime);
        assert_eq!(shared.load().revision, 0);
        assert_eq!(shared.load().listener_count(), 0);

        let notif = SyncNotification {
            manifest_revision: Some(42),
            bin_path: "runtime/listeners.bin".into(),
            changed_domains: vec!["listeners".into()],
            domain_revisions: HashMap::new(),
            domain_bins: HashMap::new(),
        };

        let outcome = apply_reload(&shared, runtime_dir, &notif).unwrap();
        assert_eq!(outcome.revision, 42);
        assert!(outcome.listeners_changed);
        assert_eq!(shared.load().revision, 42);
        assert_eq!(shared.load().listener_count(), 1);
        assert_eq!(shared.load().listeners[0].id, "http-reloaded");
    }
}
