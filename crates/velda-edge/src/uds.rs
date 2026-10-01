//! Unix Domain Socket (UDS) IPC listener.
//!
//! Listens on a dedicated local socket (e.g. `velda.sock`) to receive
//! [`SyncNotification`] messages broadcasted by `velda-sync` whenever updated
//! binary artifacts (*.bin) have been published to LKG.

use std::fs;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::watch;

use velda_sync::ipc::SyncNotification;
use velda_transport::EngineHandle;

use crate::error::EdgeError;
use crate::reload::apply_reload;
use crate::runtime::SharedRuntime;

/// Starts the Unix Domain Socket IPC server loop.
///
/// Runs continuously until `shutdown` is signaled. Unlinks stale socket files
/// on start and performs clean unlinking upon termination.
pub async fn run_ipc_server(
    socket_path: PathBuf,
    runtime_dir: PathBuf,
    shared_runtime: SharedRuntime,
    engine_handle: Option<EngineHandle>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EdgeError> {
    // Ensure parent directory exists
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Clean up any stale socket file from previous abnormal termination
    if socket_path.exists() {
        let _ = fs::remove_file(&socket_path);
    }

    let listener = UnixListener::bind(&socket_path)?;
    tracing::info!(socket = %socket_path.display(), "IPC UDS server listening for sync notifications");

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("IPC UDS server received shutdown signal; stopping accept loop");
                    break;
                }
            }
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((stream, _addr)) => {
                        let shared = shared_runtime.clone();
                        let rdir = runtime_dir.clone();
                        let engine = engine_handle.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_ipc_client(stream, &shared, &rdir, engine.as_ref()).await {
                                tracing::error!(error = %e, "Failed to process IPC client connection");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "IPC UDS accept error");
                    }
                }
            }
        }
    }

    // Clean up socket file on graceful shutdown
    if socket_path.exists() {
        let _ = fs::remove_file(&socket_path);
    }

    Ok(())
}

async fn handle_ipc_client(
    stream: tokio::net::UnixStream,
    shared_runtime: &SharedRuntime,
    runtime_dir: &Path,
    engine_handle: Option<&EngineHandle>,
) -> Result<(), EdgeError> {
    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        match serde_json::from_str::<SyncNotification>(trimmed) {
            Ok(notif) => {
                tracing::debug!(changed = ?notif.changed_domains, "Received IPC sync notification");
                if let Err(e) =
                    apply_reload(shared_runtime, runtime_dir, &notif, engine_handle).await
                {
                    tracing::error!(error = %e, "Failed to apply runtime reload from IPC notification");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, raw = %trimmed, "Malformed IPC notification payload");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{Runtime, new_shared_runtime};
    use std::collections::HashMap;
    use std::time::Duration;
    use tempfile::tempdir;
    use velda_sync::ipc::send_notification;
    use velda_sync::post_sync::listener::{
        ListenerApplicationConfig, ListenerConfig, ListenerLimitsConfig, ListenerTransportConfig,
        compile_listeners_to_binary,
    };

    #[tokio::test]
    async fn test_ipc_server_receives_and_applies_reload() {
        let tmp = tempdir().unwrap();
        let socket_path = tmp.path().join("edge_test.sock");
        let runtime_dir = tmp.path().join("runtime");
        fs::create_dir_all(&runtime_dir).unwrap();

        // Write a compiled listeners.bin
        let listeners = vec![ListenerConfig {
            id: "ipc-test".into(),
            address: "127.0.0.1:9099".into(),
            transport: ListenerTransportConfig {
                protocol: "tcp".into(),
            },
            application: ListenerApplicationConfig {
                protocol: "raw".into(),
                version: None,
                streaming: velda_sync::StreamingMode::Disabled,
            },
            tls: Default::default(),
            limits: ListenerLimitsConfig::new(10 * 1024 * 1024, 64 * 1024, 64, 30_000),
        }];
        let bin = compile_listeners_to_binary(&listeners, 77, [0u8; 32]).unwrap();
        fs::write(runtime_dir.join("listeners.bin"), bin).unwrap();

        let shared = new_shared_runtime(Runtime::empty());
        assert_eq!(shared.load().revision, 0);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let s_path = socket_path.clone();
        let r_dir = runtime_dir.clone();
        let s_runtime = shared.clone();

        let server_task = tokio::spawn(async move {
            run_ipc_server(s_path, r_dir, s_runtime, None, shutdown_rx).await
        });

        // Give UDS server a moment to bind
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send notification via velda-sync client
        let notif = SyncNotification {
            manifest_revision: Some(77),
            bin_path: "runtime/listeners.bin".into(),
            changed_domains: vec!["listeners".into()],
            domain_revisions: HashMap::new(),
            domain_bins: HashMap::new(),
        };

        send_notification(&socket_path, &notif).await.unwrap();

        // Give a brief tick to process reload
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(shared.load().revision, 77);
        assert_eq!(shared.load().listener_count(), 1);
        assert_eq!(shared.load().config.listeners[0].id, "ipc-test");

        // Shut down IPC server
        shutdown_tx.send(true).unwrap();
        let _ = server_task.await;

        // Socket file should be cleaned up
        assert!(!socket_path.exists());
    }
}
