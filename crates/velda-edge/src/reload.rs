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
use velda_transport::EngineHandle;

use crate::config::{load_listeners, load_plugins, load_routes, load_tls, load_upstreams};
use crate::error::EdgeError;
use crate::runtime::composer::build_composer;
use crate::runtime::router::build_router;
use crate::runtime::tls::compile_tls_server;
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
///
/// If listener definitions changed and an [`EngineHandle`] is provided, submits the new
/// desired ingress bindings to `velda-transport` for dynamic declarative reconciliation.
pub async fn apply_reload(
    shared_runtime: &SharedRuntime,
    runtime_dir: &Path,
    notif: &SyncNotification,
    engine_handle: Option<&EngineHandle>,
) -> Result<ReloadOutcome, EdgeError> {
    let current = shared_runtime.load();
    let new_revision = notif.manifest_revision.unwrap_or(current.revision + 1);

    let mut config = current.config.clone();
    let mut tls_server = current.tls_server.clone();

    let mut listeners_changed = false;
    let mut routes_changed = false;
    let mut upstreams_changed = false;

    for domain in &notif.changed_domains {
        match domain.as_str() {
            "listeners" => {
                config.listeners = load_listeners(runtime_dir)?;
                listeners_changed = true;
            }
            "routes" => {
                config.routes = load_routes(runtime_dir)?;
                routes_changed = true;
            }
            "upstreams" => {
                config.upstreams = load_upstreams(runtime_dir)?;
                upstreams_changed = true;
            }
            "plugins" => {
                config.plugins = load_plugins(runtime_dir)?;
            }
            "tls" => {
                config.tls = load_tls(runtime_dir)?;
                tls_server = compile_tls_server(&config.tls)?;
            }
            other => {
                tracing::warn!(domain = %other, "Unknown domain in reload notification; skipping");
            }
        }
    }

    // Recompile Composer only if listeners changed
    let composer = if listeners_changed {
        build_composer(&config.listeners)?
    } else {
        current.composer.clone()
    };

    // Recompile Router if routes, upstreams, or listeners changed
    let router = if routes_changed || upstreams_changed || listeners_changed {
        build_router(&config.routes, &config.upstreams, &config.listeners)?
    } else {
        current.router.clone()
    };

    // Recompile PipelineTable if listeners changed
    let pipelines = if listeners_changed {
        crate::runtime::pipeline::PipelineTable::build(&config.listeners)?
    } else {
        current.pipelines.clone()
    };

    // Recompile UpstreamTable if upstreams changed
    let upstreams = if upstreams_changed {
        crate::runtime::build_upstreams(&config.upstreams)
    } else {
        current.upstreams.clone()
    };

    // Recompile TlsClientEngine if upstreams changed
    let tls_client = if upstreams_changed {
        crate::runtime::tls::compile_tls_client(&config.upstreams)?
    } else {
        current.tls_client.clone()
    };

    // If listeners or TLS changed, ensure HTTP/3 pipeline engines are registered for any new listeners
    // (existing engines and their active QUIC connections are preserved without disruption!)
    if let Some(tls) = tls_server.as_ref() {
        for listener in &config.listeners {
            if listener.transport.protocol.eq_ignore_ascii_case("udp")
                && (listener.application.protocol.eq_ignore_ascii_case("http3")
                    || listener.application.protocol.eq_ignore_ascii_case("grpc"))
            {
                let _ = crate::pipeline::l7::http3::init_h3_engine(&listener.id, tls);
            }
        }
    }

    let candidate = Runtime {
        revision: new_revision,
        config,
        composer,
        router,
        pipelines,
        upstreams,
        tls_server,
        tls_client,
    };

    // Pre-validate that all declared listener addresses parse cleanly into IngressBindings
    let bindings = candidate.active_bindings()?;

    // If listeners changed, notify TrafficEngine to reconcile ports dynamically
    if let (true, Some(engine)) = (listeners_changed, engine_handle) {
        engine
            .reconcile(bindings)
            .await
            .map_err(EdgeError::Transport)?;
    }

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
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerConfig, ListenerTransportConfig,
        compile_listeners_to_binary,
    };

    #[tokio::test]
    async fn test_apply_reload_atomic_swap() {
        let tmp = tempdir().unwrap();
        let runtime_dir = tmp.path();

        // Write a compiled listeners.bin into runtime_dir
        let listeners = vec![ListenerConfig {
            id: "http-reloaded".into(),
            address: "127.0.0.1:8080".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "http1".into(),
                version: None,
            },
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

        let outcome = apply_reload(&shared, runtime_dir, &notif, None)
            .await
            .unwrap();
        assert_eq!(outcome.revision, 42);
        assert!(outcome.listeners_changed);
        assert_eq!(shared.load().revision, 42);
        assert_eq!(shared.load().listener_count(), 1);
        assert_eq!(shared.load().config.listeners[0].id, "http-reloaded");
    }

    #[tokio::test]
    async fn test_apply_reload_with_tls_compiles_composer() {
        use rcgen::generate_simple_self_signed;
        use velda_sync::post_sync::tls::{TlsConfig, compile_tls_to_binary};

        let tmp = tempdir().unwrap();
        let runtime_dir = tmp.path();

        let cert = generate_simple_self_signed(vec!["api.example.com".into()]).unwrap();
        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();

        let tls = vec![TlsConfig {
            sni: vec![],
            cert_pem,
            key_pem,
            client_ca_pem: None,
            versions: vec!["tls1.3".into()],
            alpn: vec!["h2".into()],
        }];
        let bin = compile_tls_to_binary(&tls, 1, [0u8; 32]).unwrap();
        std::fs::write(runtime_dir.join("tls.bin"), bin).unwrap();

        let initial_runtime = Runtime::empty();
        let shared = new_shared_runtime(initial_runtime);
        assert!(shared.load().config.tls.is_empty());

        let notif = SyncNotification {
            manifest_revision: Some(2),
            bin_path: "runtime/tls.bin".into(),
            changed_domains: vec!["tls".into()],
            domain_revisions: HashMap::new(),
            domain_bins: HashMap::new(),
        };

        let outcome = apply_reload(&shared, runtime_dir, &notif, None)
            .await
            .unwrap();
        assert_eq!(outcome.revision, 2);
        assert!(!outcome.listeners_changed);

        // Runtime snapshot in RAM now holds active TLS configuration and precompiled TLS server!
        assert_eq!(shared.load().config.tls.len(), 1);
        assert_eq!(shared.load().config.tls[0].sni, vec!["api.example.com"]);
        assert!(shared.load().tls_server.is_some());
    }
}
