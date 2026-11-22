//! Core-Local Per-Request Shadow Dispatch Hook (`velda-plugin::hook`).
//!
//! ```text
//!   velda-worker-{i} (SCHED_OTHER Hot Path)
//!     └─► try_dispatch_shadow(task)  [non-blocking try_send into thread-local channel]
//!           │
//!           ├─ Ok(())  ──► mpsc(1024) ──► velda-shadow-{i} (SCHED_IDLE on same CPU core)
//!           └─ Err(task) ─► Channel full or uninitialized (drops cleanly without blocking)
//! ```
//!
//! Strictly reserved for sporadic per-request fire-and-forget offloads (`request_mirror`,
//! `oauth2_introspection`). Long-running background daemons are scheduled statically across
//! cores via PBWW (`pbww() > 0` + `run_background()`), not via this hook.

use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

/// Boxed thread-safe future representation of a sporadic task offloaded to the local Shadow Worker.
pub type ShadowTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

thread_local! {
    static LOCAL_SHADOW_SENDER: std::cell::RefCell<Option<mpsc::Sender<ShadowTask>>> = const { std::cell::RefCell::new(None) };
}

/// Sets the core-local Shadow Worker task hook sender for the calling Traffic Worker thread.
pub fn set_core_shadow_sender(tx: mpsc::Sender<ShadowTask>) {
    LOCAL_SHADOW_SENDER.with(|cell| {
        *cell.borrow_mut() = Some(tx);
    });
}

/// Attempts to dispatch a one-shot task to this core's paired Shadow Worker via non-blocking `try_send`.
///
/// Returns `Ok(())` on success, or `Err(task)` if the core-local channel is full or uninitialized.
pub fn try_dispatch_shadow(task: ShadowTask) -> Result<(), ShadowTask> {
    if let Some(tx) = LOCAL_SHADOW_SENDER.with(|c| c.borrow().clone()) {
        return tx.try_send(task).map_err(|e| match e {
            mpsc::error::TrySendError::Full(t) | mpsc::error::TrySendError::Closed(t) => t,
        });
    }
    Err(task)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shadow_hook_dispatch() {
        let (tx, mut rx) = mpsc::channel(16);
        set_core_shadow_sender(tx);

        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_clone = flag.clone();

        let task = Box::pin(async move {
            flag_clone.store(true, std::sync::atomic::Ordering::Release);
        });

        assert!(try_dispatch_shadow(task).is_ok());

        if let Some(received_task) = rx.recv().await {
            received_task.await;
        }

        assert!(flag.load(std::sync::atomic::Ordering::Acquire));
    }
}
