//! Hot reload coordinator and atomic runtime swapping.
//!
//! ```text
//! +-----------------------------------------------------------------------------+
//! |                   Hot-Reload & Make-Before-Break Swap                       |
//! +-----------------------------------------------------------------------------+
//! |                                                                             |
//! |  SyncNotification (UDS) ──► apply_reload()                                  |
//! |    ├─► Unpack changed *.bin (listeners, routes, upstreams, plugins, tls, etp)
//! |    ├─► If etp changed ──► current.etp_table.reconcile_make_before_break()   |
//! |    │                        ├─ unchanged fingerprint => Arc::clone reuse    |
//! |    │                        └─ modified => build + prewarm BEFORE swap      |
//! |    ├─► Reconcile IngressEngine bindings if listeners changed                |
//! |    └─► ArcSwap::store(Arc::new(candidate))  [Old EtpLease drains safely]    |
//! +-----------------------------------------------------------------------------+
//! ```

use std::path::Path;
use std::sync::Arc;

use velda_ingress::IngressHandle;
use velda_sync::ipc::SyncNotification;

use crate::config::{
    load_etps, load_listeners, load_plugins, load_routes, load_tls, load_upstreams,
};
use crate::error::EdgeError;
use crate::profile::RuntimeProfile;
use crate::runtime::router::build_router;
use crate::runtime::tls::compile_tls_server;
use crate::runtime::{Runtime, RuntimeConfig, SharedRuntime};

static RELOAD_NOTIFIER: std::sync::OnceLock<tokio::sync::watch::Sender<u64>> =
    std::sync::OnceLock::new();

/// Returns a receiver subscribed to hot-reload revision updates.
pub fn get_reload_receiver() -> tokio::sync::watch::Receiver<u64> {
    RELOAD_NOTIFIER
        .get_or_init(|| {
            let (tx, _) = tokio::sync::watch::channel(0);
            tx
        })
        .subscribe()
}

/// Broadcasts a newly applied runtime revision to background supervisors.
pub fn notify_reload(revision: u64) {
    if let Some(tx) = RELOAD_NOTIFIER.get() {
        let _ = tx.send(revision);
    }
}

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

/// Loads the initial LKG runtime snapshot from disk artifacts.
///
/// Used during cold-start bootstrap. If LKG binary artifacts are present on disk,
/// parses and compiles them into an initial [`Runtime`] snapshot; otherwise returns an
/// empty initial runtime.
pub fn load_initial_runtime(
    runtime_dir: &Path,
    profile: &RuntimeProfile,
) -> Result<Runtime, EdgeError> {
    let has_lkg = runtime_dir.join("listeners.bin").exists()
        || runtime_dir.join("routes.bin").exists()
        || runtime_dir.join("upstreams.bin").exists()
        || runtime_dir.join("plugins.bin").exists()
        || runtime_dir.join("tls.bin").exists()
        || runtime_dir.join("etp.bin").exists();

    if !has_lkg {
        tracing::info!(
            path = %runtime_dir.display(),
            "Runtime directory does not exist yet; initializing with empty state"
        );
        return Ok(Runtime::empty());
    }

    let listeners = load_listeners(runtime_dir)?;
    let routes = load_routes(runtime_dir)?;
    let upstreams = load_upstreams(runtime_dir)?;

    velda_sync::post_sync::validate_streaming_policy(&listeners, &routes, &upstreams).map_err(
        |e| EdgeError::InvalidConfig {
            detail: e.to_string(),
        },
    )?;

    let tls = load_tls(runtime_dir)?;
    let tls_server = compile_tls_server(&tls, &profile.to_tls_server_params())?;
    let tls_client = crate::runtime::tls::compile_tls_client(&upstreams)?;
    let router = build_router(&routes, &upstreams, &listeners)?;
    let dns_config = profile.to_dns_resolver_config();
    let upstreams_table =
        crate::upstream::build_upstreams(&upstreams, tls_client.as_ref(), &dns_config);

    // Pre-initialize HTTP/3 persistent pipeline engines for declared H3 listeners
    if let Some(tls) = tls_server.as_ref() {
        for listener in &listeners {
            if listener.transport.protocol.eq_ignore_ascii_case("udp") {
                if listener.application.protocol.eq_ignore_ascii_case("http3") {
                    let _ = crate::pipeline::http3::init_h3_engine(&listener.id, tls);
                } else if listener.application.protocol.eq_ignore_ascii_case("grpc") {
                    let _ = crate::pipeline::grpc::udp::init_grpc_udp_engine(&listener.id, tls);
                }
            }
        }
    }

    let etps = load_etps(runtime_dir)?;
    let etp_entries: Vec<velda_etp::EtpEntryConfig> = etps
        .iter()
        .map(|e| {
            e.to_entry_config().map_err(|err| EdgeError::InvalidConfig {
                detail: err.to_string(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let etp_table = Arc::new(
        velda_etp::registry::build_registry(&etp_entries).map_err(|err| {
            EdgeError::InvalidConfig {
                detail: err.to_string(),
            }
        })?,
    );

    let runtime_config = RuntimeConfig {
        listeners,
        routes,
        upstreams,
        plugins: load_plugins(runtime_dir)?,
        tls,
        etps,
    };

    let topo = velda_core::hardware::global_hardware_topology();
    let dims = velda_core::buffer::adaptive::AdaptiveBufferDimensions::for_topology(topo);
    let pool_config = velda_core::buffer::pool::AdaptivePoolConfig {
        small_slabs: dims.small_baseline,
        small_max_cap: dims.small_max_cap,
        medium_slabs: dims.medium_baseline,
        medium_max_cap: dims.medium_max_cap,
        standard_slabs: dims.standard_baseline,
        standard_max_cap: dims.standard_max_cap,
        large_slabs: dims.large_baseline,
        large_max_cap: dims.large_max_cap,
        idle_ttl_secs: 15,
    };
    let buffer_pool = Arc::new(
        velda_core::buffer::pool::ShardedBufferPool::with_adaptive_limits(
            topo.available_cores,
            pool_config,
        ),
    );

    let pipelines = crate::runtime::pipeline::PipelineTable::build_with_hardware(
        &runtime_config.listeners,
        topo.available_cores,
        topo.memory_bytes,
    )?;

    let plugins = Arc::new(crate::runtime::build_plugin_table(
        &runtime_config.plugins,
        &runtime_config.routes,
        &runtime_config.upstreams,
        &runtime_config.listeners,
        &etp_table,
    )?);

    let route_to_upstream =
        crate::runtime::build_runtime_luts(&runtime_config.routes, &runtime_config.upstreams);

    Ok(Runtime {
        revision: 1,
        config: runtime_config,
        router,
        pipelines,
        upstreams: upstreams_table,
        route_to_upstream,
        plugins,
        tls_server,
        tls_client,
        overload: Arc::new(velda_core::hardware::OverloadTracker::with_config(
            profile.overload,
        )),
        buffer_pool,
        etp_table,
    })
}

/// Applies a reload notification from `velda-sync` against the active shared runtime.
///
/// If listener definitions changed and an [`IngressHandle`] is provided, submits the new
/// desired ingress bindings to `velda-ingress` for dynamic declarative reconciliation.
pub async fn apply_reload(
    shared_runtime: &SharedRuntime,
    runtime_dir: &Path,
    notif: &SyncNotification,
    engine_handle: Option<&IngressHandle>,
) -> Result<ReloadOutcome, EdgeError> {
    let current = shared_runtime.load();
    let new_revision = notif.manifest_revision.unwrap_or(current.revision + 1);

    let mut config = current.config.clone();
    let mut tls_server = current.tls_server.clone();

    let mut listeners_changed = false;
    let mut routes_changed = false;
    let mut upstreams_changed = false;
    let mut plugins_changed = false;
    let mut etps_changed = false;

    let hw = velda_core::hardware::global_hardware_topology();
    let profile = crate::profile::resolve_runtime_profile(runtime_dir, hw);

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
                plugins_changed = true;
            }
            "tls" => {
                config.tls = load_tls(runtime_dir)?;
                tls_server = compile_tls_server(&config.tls, &profile.to_tls_server_params())?;
            }
            "etp" | "etp.bin" => {
                config.etps = load_etps(runtime_dir)?;
                etps_changed = true;
            }
            other => {
                tracing::warn!(domain = %other, "Unknown domain in reload notification; skipping");
            }
        }
    }

    // Validate cross-domain streaming policy if routes, upstreams, or listeners changed
    if routes_changed || upstreams_changed || listeners_changed {
        velda_sync::post_sync::validate_streaming_policy(
            &config.listeners,
            &config.routes,
            &config.upstreams,
        )
        .map_err(|e| EdgeError::InvalidConfig {
            detail: e.to_string(),
        })?;
    }

    // Recompile Router if routes, upstreams, or listeners changed
    let router = if routes_changed || upstreams_changed || listeners_changed {
        build_router(&config.routes, &config.upstreams, &config.listeners)?
    } else {
        current.router.clone()
    };

    // Recompile PipelineTable if listeners changed
    let pipelines = if listeners_changed {
        let topo = velda_core::hardware::global_hardware_topology();
        crate::runtime::pipeline::PipelineTable::build_with_hardware(
            &config.listeners,
            topo.available_cores,
            topo.memory_bytes,
        )?
    } else {
        current.pipelines.clone()
    };

    // Recompile TlsClientEngine if upstreams changed
    let tls_client = if upstreams_changed {
        crate::runtime::tls::compile_tls_client(&config.upstreams)?
    } else {
        current.tls_client.clone()
    };

    // Recompile UpstreamTable if upstreams changed
    let upstreams = if upstreams_changed {
        let dns_config = profile.to_dns_resolver_config();
        crate::upstream::build_upstreams(&config.upstreams, tls_client.as_ref(), &dns_config)
    } else {
        current.upstreams.clone()
    };

    // Reconcile EtpRegistry via Make-Before-Break BEFORE building PluginTable so
    // background plugins wire against the newly reconciled EtpInstances.
    let etp_table = if etps_changed {
        let etp_entries: Vec<velda_etp::EtpEntryConfig> = config
            .etps
            .iter()
            .map(|e| {
                e.to_entry_config().map_err(|err| EdgeError::InvalidConfig {
                    detail: err.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reconciled = velda_etp::registry::reconcile(&current.etp_table, &etp_entries, true)
            .await
            .map_err(|err| EdgeError::InvalidConfig {
                detail: err.to_string(),
            })?;
        Arc::new(reconciled)
    } else {
        current.etp_table.clone()
    };

    // Recompile PluginTable if plugins, routes, upstreams, listeners, or ETPs changed
    let plugins = if plugins_changed
        || routes_changed
        || upstreams_changed
        || listeners_changed
        || etps_changed
    {
        Arc::new(crate::runtime::build_plugin_table(
            &config.plugins,
            &config.routes,
            &config.upstreams,
            &config.listeners,
            &etp_table,
        )?)
    } else {
        current.plugins.clone()
    };

    // If listeners or TLS changed, ensure HTTP/3 pipeline engines are registered for any new listeners
    // (existing engines and their active QUIC connections are preserved without disruption!)
    if let Some(tls) = tls_server.as_ref() {
        for listener in &config.listeners {
            if listener.transport.protocol.eq_ignore_ascii_case("udp") {
                if listener.application.protocol.eq_ignore_ascii_case("http3") {
                    let _ = crate::pipeline::http3::init_h3_engine(&listener.id, tls);
                } else if listener.application.protocol.eq_ignore_ascii_case("grpc") {
                    let _ = crate::pipeline::grpc::udp::init_grpc_udp_engine(&listener.id, tls);
                }
            }
        }
    }

    let current_rt = shared_runtime.load();
    let overload = current_rt.overload.clone();
    let buffer_pool = current_rt.buffer_pool.clone();
    let route_to_upstream = crate::runtime::build_runtime_luts(&config.routes, &config.upstreams);
    let candidate = Runtime {
        revision: new_revision,
        config,
        router,
        pipelines,
        upstreams,
        route_to_upstream,
        tls_server,
        tls_client,
        plugins,
        overload,
        buffer_pool,
        etp_table,
    };

    // Pre-validate that all declared listener addresses parse cleanly into IngressBindings
    let tcp_cfg = profile.to_tcp_listener_config();
    let udp_cfg = profile.to_udp_socket_config();
    let bindings = candidate
        .config
        .active_bindings_with_configs(Some(&tcp_cfg), Some(&udp_cfg))?;

    // If listeners changed, notify TrafficEngine to reconcile ports dynamically
    if let (true, Some(engine)) = (listeners_changed, engine_handle) {
        engine
            .reconcile(bindings)
            .await
            .map_err(EdgeError::Ingress)?;
    }

    // Lock-free atomic swap of active runtime snapshot
    shared_runtime.store(Arc::new(candidate));
    notify_reload(new_revision);

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
                streaming: velda_sync::StreamingMode::DISABLED,
            },
            tls: Default::default(),
            http1: None,
            http2: None,
            grpc: None,
            http3: None,
            raw: None,
            observability: None,
        }];
        let bin = compile_listeners_to_binary(&listeners, 10, [0u8; 32]).unwrap();
        std::fs::write(runtime_dir.join("listeners.bin"), bin).unwrap();

        let initial_runtime = Runtime::empty();
        let shared = new_shared_runtime(initial_runtime);
        assert_eq!(shared.load().revision, 0);
        assert_eq!(shared.load().config.listeners.len(), 0);

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
        assert_eq!(shared.load().config.listeners.len(), 1);
        assert_eq!(shared.load().config.listeners[0].id, "http-reloaded");
    }

    #[tokio::test]
    async fn test_apply_reload_with_tls_compiles_tls_server() {
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

    #[tokio::test]
    async fn test_apply_reload_etp_make_before_break() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use velda_etp::EtpPoolSettings;
        use velda_sync::post_sync::etp::{EtpFile, EtpSyncConfig, compile_etps};

        async fn spawn_echo() -> (u16, tokio::task::JoinHandle<()>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let handle = tokio::spawn(async move {
                while let Ok((mut stream, _)) = listener.accept().await {
                    tokio::spawn(async move {
                        let mut buf = [0u8; 64];
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => {
                                    if stream.write_all(&buf[..n]).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    });
                }
            });
            (port, handle)
        }

        let (port_unchanged, h_unchanged) = spawn_echo().await;
        let (port_v1, h_v1) = spawn_echo().await;
        let (port_v2, h_v2) = spawn_echo().await;

        let tmp = tempdir().unwrap();
        let runtime_dir = tmp.path();

        let etp_v1 = EtpFile {
            schema_version: 1,
            revision: 1,
            etps: vec![
                EtpSyncConfig {
                    id: "redis-unchanged".into(),
                    kind: "redis".into(),
                    security: velda_etp::EtpTransportSecurity::Plain,
                    pool: EtpPoolSettings {
                        pool_size: 2,
                        connect_timeout_ms: 2000,
                        idle_timeout_ms: 60_000,
                    },
                    tls: None,
                    settings: serde_json::json!({ "host": "127.0.0.1", "port": port_unchanged }),
                },
                EtpSyncConfig {
                    id: "loki-mutated".into(),
                    kind: "loki".into(),
                    security: velda_etp::EtpTransportSecurity::Plain,
                    pool: EtpPoolSettings {
                        pool_size: 2,
                        connect_timeout_ms: 2000,
                        idle_timeout_ms: 60_000,
                    },
                    tls: None,
                    settings: serde_json::json!({ "host": "127.0.0.1", "port": port_v1 }),
                },
            ],
        };
        std::fs::write(runtime_dir.join("etp.bin"), compile_etps(&etp_v1).unwrap()).unwrap();

        let shared = new_shared_runtime(Runtime::empty());
        let notif1 = SyncNotification {
            manifest_revision: Some(1),
            bin_path: "runtime/etp.bin".into(),
            changed_domains: vec!["etp".into()],
            domain_revisions: HashMap::new(),
            domain_bins: HashMap::new(),
        };
        apply_reload(&shared, runtime_dir, &notif1, None)
            .await
            .unwrap();

        let rt1 = shared.load_full();
        let unchanged_ptr_1 = Arc::as_ptr(rt1.etp_table.entries.get("redis-unchanged").unwrap());
        let mutated_ptr_1 = Arc::as_ptr(rt1.etp_table.entries.get("loki-mutated").unwrap());

        // Hold an active in-flight lease on the old loki-mutated pool before reload
        let velda_etp::EtpBackend::Loki(loki_v1) =
            &rt1.etp_table.entries.get("loki-mutated").unwrap().backend
        else {
            panic!("expected Loki backend");
        };
        let mut inflight_lease = loki_v1.acquire().await.unwrap();

        // Update loki-mutated to port_v2 while keeping redis-unchanged identical
        let etp_v2 = EtpFile {
            schema_version: 1,
            revision: 2,
            etps: vec![
                etp_v1.etps[0].clone(),
                EtpSyncConfig {
                    id: "loki-mutated".into(),
                    kind: "loki".into(),
                    security: velda_etp::EtpTransportSecurity::Plain,
                    pool: EtpPoolSettings {
                        pool_size: 2,
                        connect_timeout_ms: 2000,
                        idle_timeout_ms: 60_000,
                    },
                    tls: None,
                    settings: serde_json::json!({ "host": "127.0.0.1", "port": port_v2 }),
                },
            ],
        };
        std::fs::write(runtime_dir.join("etp.bin"), compile_etps(&etp_v2).unwrap()).unwrap();

        let notif2 = SyncNotification {
            manifest_revision: Some(2),
            bin_path: "runtime/etp.bin".into(),
            changed_domains: vec!["etp.bin".into()],
            domain_revisions: HashMap::new(),
            domain_bins: HashMap::new(),
        };
        let outcome = apply_reload(&shared, runtime_dir, &notif2, None)
            .await
            .unwrap();
        assert_eq!(outcome.revision, 2);

        let rt2 = shared.load_full();
        let unchanged_ptr_2 = Arc::as_ptr(rt2.etp_table.entries.get("redis-unchanged").unwrap());
        let mutated_ptr_2 = Arc::as_ptr(rt2.etp_table.entries.get("loki-mutated").unwrap());

        // Unchanged ETP reused same Arc instance (zero reconnect)
        assert_eq!(unchanged_ptr_1, unchanged_ptr_2);
        // Modified ETP received a newly prewarmed instance
        assert_ne!(mutated_ptr_1, mutated_ptr_2);

        // In-flight lease from rt1 finishes I/O cleanly without interruption
        let velda_etp::loki::LokiConn::Http(ref mut inflight_stream) = *inflight_lease else {
            panic!("expected HTTP LokiConn");
        };
        inflight_stream.write_all(b"inflight").await.unwrap();
        let mut buf = [0u8; 8];
        inflight_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"inflight");
        drop(inflight_lease);

        // New rt2 pool connects and exchanges I/O against port_v2
        let velda_etp::EtpBackend::Loki(loki_v2) =
            &rt2.etp_table.entries.get("loki-mutated").unwrap().backend
        else {
            panic!("expected Loki backend");
        };
        let mut new_lease = loki_v2.acquire().await.unwrap();
        let velda_etp::loki::LokiConn::Http(ref mut new_stream) = *new_lease else {
            panic!("expected HTTP LokiConn");
        };
        new_stream.write_all(b"reloaded").await.unwrap();
        new_stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"reloaded");

        h_unchanged.abort();
        h_v1.abort();
        h_v2.abort();
    }
}
