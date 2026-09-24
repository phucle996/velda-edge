//! IPC Transport Layer for Data Plane Hot Reload.
//!
//! Owns Unix Domain Socket (UDS) IPC message definitions and transport
//! (`velda.sock`), notifying Edge worker processes when compiled domain
//! binary artifacts (*.bin) have been published to LKG.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

use crate::SyncError;

/// Notification payload broadcasted to Velda Edge Data Plane workers via UDS IPC.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncNotification {
    #[serde(default)]
    pub manifest_revision: Option<u64>,
    #[serde(default)]
    pub bin_path: String,
    #[serde(default)]
    pub changed_domains: Vec<String>,
    #[serde(default)]
    pub domain_revisions: HashMap<String, u64>,
    #[serde(default)]
    pub domain_bins: HashMap<String, String>,
}

/// IPC Transport client for dispatching reload notifications to the Data Plane.
#[derive(Debug, Clone)]
pub struct IpcNotifier {
    pub socket_path: PathBuf,
}

impl IpcNotifier {
    /// Creates a new IPC transport client targeting the specified UDS socket path.
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    /// Path to the UDS socket.
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Dispatches a reload notification over the Unix Domain Socket.
    ///
    /// If the socket file does not exist (e.g. Data Plane is offline or in testing mode),
    /// this safely succeeds without error to preserve autonomous operation.
    pub async fn notify(&self, notif: &SyncNotification) -> Result<(), SyncError> {
        send_notification(&self.socket_path, notif).await
    }
}

/// Channel sender for dispatching asynchronous sync completion signals to the IPC worker.
pub type IpcSignalSender = tokio::sync::mpsc::Sender<SyncNotification>;

/// Channel receiver consumed by the IPC signal worker.
pub type IpcSignalReceiver = tokio::sync::mpsc::Receiver<SyncNotification>;

/// Creates an asynchronous unbounded/bounded signal channel for decoupling sync completion
/// from IPC transport dispatch.
pub fn create_ipc_channel(buffer_size: usize) -> (IpcSignalSender, IpcSignalReceiver) {
    tokio::sync::mpsc::channel(buffer_size)
}

/// Asynchronous background worker that receives sync completion signals from a channel
/// and dispatches notifications across the Unix Domain Socket to Edge Data Plane workers.
pub async fn run_ipc_signal_worker(mut receiver: IpcSignalReceiver, notifier: IpcNotifier) {
    while let Some(notif) = receiver.recv().await {
        if let Err(e) = notifier.notify(&notif).await {
            eprintln!("[velda-sync::ipc] Failed to deliver IPC notification: {e}");
        }
    }
}

/// Dispatches a completion notification directly to the Data Plane upon concluding a sync cycle.
pub async fn notify_cycle_completed(
    notifier: &IpcNotifier,
    manifest_revision: Option<u64>,
    bin_path: impl Into<String>,
    changed_domains: Vec<String>,
    domain_revisions: HashMap<String, u64>,
    domain_bins: HashMap<String, String>,
) -> Result<(), SyncError> {
    let notif = SyncNotification {
        manifest_revision,
        bin_path: bin_path.into(),
        changed_domains,
        domain_revisions,
        domain_bins,
    };
    notifier.notify(&notif).await
}

/// Standalone function to dispatch a reload notification to a given UDS socket path.
pub async fn send_notification(
    socket_path: &Path,
    notif: &SyncNotification,
) -> Result<(), SyncError> {
    if !socket_path.exists() {
        return Ok(());
    }

    let mut stream = UnixStream::connect(socket_path).await.map_err(|e| {
        SyncError::Notify(format!(
            "Socket connect error {}: {e}",
            socket_path.display()
        ))
    })?;

    let mut payload = serde_json::to_vec(notif).map_err(|e| SyncError::Notify(e.to_string()))?;
    payload.push(b'\n');

    stream.write_all(&payload).await?;
    stream.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn test_ipc_notification_roundtrip() {
        let tmp = tempdir().unwrap();
        let socket_path = tmp.path().join("velda_test.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();

        let notif = SyncNotification {
            manifest_revision: Some(42),
            bin_path: "/tmp/runtime/config.bin".into(),
            changed_domains: vec!["routes".into(), "upstreams".into()],
            domain_revisions: HashMap::from([("routes".into(), 42), ("upstreams".into(), 42)]),
            domain_bins: HashMap::from([("routes".into(), "/tmp/runtime/routes.bin".into())]),
        };

        let notifier = IpcNotifier::new(&socket_path);

        let notif_clone = notif.clone();
        let send_handle = tokio::spawn(async move {
            notifier.notify(&notif_clone).await.unwrap();
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();

        send_handle.await.unwrap();

        let received: SyncNotification = serde_json::from_slice(&buf).unwrap();
        assert_eq!(received, notif);
    }

    #[tokio::test]
    async fn test_ipc_non_existent_socket_is_safe() {
        let notifier = IpcNotifier::new("/tmp/non_existent_velda_test.sock");
        let notif = SyncNotification {
            manifest_revision: None,
            bin_path: "".into(),
            changed_domains: vec![],
            domain_revisions: HashMap::new(),
            domain_bins: HashMap::new(),
        };

        let res = notifier.notify(&notif).await;
        assert!(res.is_ok(), "Missing socket should be a safe no-op");
    }

    #[tokio::test]
    async fn test_ipc_signal_worker_and_cycle_completed() {
        let tmp = tempdir().unwrap();
        let socket_path = tmp.path().join("velda_signal_test.sock");

        let listener = UnixListener::bind(&socket_path).unwrap();
        let notifier = IpcNotifier::new(&socket_path);

        let (tx, rx) = create_ipc_channel(16);
        let worker_handle = tokio::spawn(run_ipc_signal_worker(rx, notifier.clone()));

        let notif = SyncNotification {
            manifest_revision: Some(100),
            bin_path: "/tmp/runtime/config.bin".into(),
            changed_domains: vec!["routes".into()],
            domain_revisions: HashMap::from([("routes".into(), 100)]),
            domain_bins: HashMap::from([("routes".into(), "/tmp/runtime/routes.bin".into())]),
        };

        tx.send(notif.clone()).await.unwrap();

        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();

        let received: SyncNotification = serde_json::from_slice(&buf).unwrap();
        assert_eq!(received, notif);

        drop(tx);
        worker_handle.await.unwrap();
    }
}
