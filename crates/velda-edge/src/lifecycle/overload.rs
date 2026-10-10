//! Background resource overload sampling and saturation monitoring.
//!
//! Spawns a background task monitoring host/container memory saturation,
//! updating the [`OverloadTracker`] state machine with hysteresis.
//! Completely decoupled from the request-serving hot path to maintain zero-IO.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use velda_core::OverloadTracker;

/// Default sampling interval for resource overload monitoring (50 ms).
pub const DEFAULT_OVERLOAD_SAMPLE_INTERVAL: Duration = Duration::from_millis(50);

/// Spawns a background task monitoring host/container memory saturation.
///
/// Samples memory usage every 50ms and updates the thread-safe [`OverloadTracker`]
/// state machine with hysteresis. Runs until the shutdown signal is received.
pub fn spawn_overload_monitor(
    tracker: Arc<OverloadTracker>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                res = shutdown.changed() => {
                    if res.is_err() || *shutdown.borrow() {
                        tracing::debug!("Overload monitor task stopping on shutdown signal");
                        break;
                    }
                }
                _ = ticker.tick() => {
                    let level = tracker.sample_and_update();
                    if level.is_critical() {
                        tracing::warn!(
                            level = ?level,
                            current_bytes = tracker.current_memory_bytes(),
                            limit_bytes = tracker.total_memory_bytes(),
                            "CRITICAL memory overload detected: shedding connections and shedding load"
                        );
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::watch;
    use velda_core::OverloadConfig;

    #[tokio::test]
    async fn test_overload_monitor_shutdown() {
        let config = OverloadConfig::default();
        let tracker = Arc::new(OverloadTracker::new(config, 1_000_000_000));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = spawn_overload_monitor(tracker, Duration::from_millis(10), shutdown_rx);

        tokio::time::sleep(Duration::from_millis(25)).await;
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("Overload monitor task must terminate cleanly upon shutdown signal")
            .unwrap();
    }
}
