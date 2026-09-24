//! Pure Metrics Capability Provider for velda-sync daemon.
//!
//! Provides thread-safe, atomic telemetry counters, gauges, and Prometheus formatting
//! for observing reconciliation cycles, delta application, and Control Plane connectivity.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use crate::SyncOutcome;

/// Snapshot of synchronization metrics at a given point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub cycles_total: u64,
    pub updates_total: u64,
    pub unchanged_total: u64,
    pub errors_total: u64,
    pub last_duration_ms: u64,
    pub manifest_revision: u64,
    pub cp_connected: bool,
}

/// In-memory atomic telemetry collector for the velda-sync agent.
#[derive(Debug, Default)]
pub struct SyncMetrics {
    cycles_total: AtomicU64,
    updates_total: AtomicU64,
    unchanged_total: AtomicU64,
    errors_total: AtomicU64,
    last_duration_ms: AtomicU64,
    manifest_revision: AtomicU64,
    cp_connected: AtomicBool,
}

impl SyncMetrics {
    /// Creates a new metrics collector with zeroed counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a successful reconciliation cycle.
    pub fn record_cycle_success(&self, outcome: &SyncOutcome, duration: Duration) {
        self.cycles_total.fetch_add(1, Ordering::Relaxed);
        self.last_duration_ms
            .store(duration.as_millis() as u64, Ordering::Relaxed);

        match outcome {
            SyncOutcome::Updated { .. } => {
                self.updates_total.fetch_add(1, Ordering::Relaxed);
            }
            SyncOutcome::Unchanged => {
                self.unchanged_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Records a failed reconciliation cycle.
    pub fn record_cycle_error(&self, duration: Duration) {
        self.cycles_total.fetch_add(1, Ordering::Relaxed);
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        self.last_duration_ms
            .store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    /// Updates the active manifest revision gauge.
    pub fn set_manifest_revision(&self, revision: u64) {
        self.manifest_revision.store(revision, Ordering::Relaxed);
    }

    /// Updates the Control Plane connectivity gauge.
    pub fn set_cp_connected(&self, connected: bool) {
        self.cp_connected.store(connected, Ordering::Relaxed);
    }

    /// Captures a point-in-time snapshot of all metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            cycles_total: self.cycles_total.load(Ordering::Relaxed),
            updates_total: self.updates_total.load(Ordering::Relaxed),
            unchanged_total: self.unchanged_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            last_duration_ms: self.last_duration_ms.load(Ordering::Relaxed),
            manifest_revision: self.manifest_revision.load(Ordering::Relaxed),
            cp_connected: self.cp_connected.load(Ordering::Relaxed),
        }
    }

    /// Renders current metrics in standard Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let snap = self.snapshot();
        let cp_val = if snap.cp_connected { 1 } else { 0 };

        format!(
            "# HELP velda_sync_reconcile_cycles_total Total number of reconciliation cycles executed.\n\
             # TYPE velda_sync_reconcile_cycles_total counter\n\
             velda_sync_reconcile_cycles_total {cycles}\n\
             # HELP velda_sync_reconcile_updates_total Total number of cycles that applied configuration updates.\n\
             # TYPE velda_sync_reconcile_updates_total counter\n\
             velda_sync_reconcile_updates_total {updates}\n\
             # HELP velda_sync_reconcile_unchanged_total Total number of cycles that resulted in no-op.\n\
             # TYPE velda_sync_reconcile_unchanged_total counter\n\
             velda_sync_reconcile_unchanged_total {unchanged}\n\
             # HELP velda_sync_reconcile_errors_total Total number of failed reconciliation cycles.\n\
             # TYPE velda_sync_reconcile_errors_total counter\n\
             velda_sync_reconcile_errors_total {errors}\n\
             # HELP velda_sync_reconcile_duration_ms Duration of the last reconciliation cycle in milliseconds.\n\
             # TYPE velda_sync_reconcile_duration_ms gauge\n\
             velda_sync_reconcile_duration_ms {duration}\n\
             # HELP velda_sync_manifest_revision Currently active manifest revision.\n\
             # TYPE velda_sync_manifest_revision gauge\n\
             velda_sync_manifest_revision {revision}\n\
             # HELP velda_sync_cp_connected Whether the remote Control Plane is reachable (1) or unreachable (0).\n\
             # TYPE velda_sync_cp_connected gauge\n\
             velda_sync_cp_connected {cp_val}\n",
            cycles = snap.cycles_total,
            updates = snap.updates_total,
            unchanged = snap.unchanged_total,
            errors = snap.errors_total,
            duration = snap.last_duration_ms,
            revision = snap.manifest_revision,
            cp_val = cp_val,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_metrics_recording_and_snapshot() {
        let metrics = SyncMetrics::new();
        metrics.set_cp_connected(true);
        metrics.set_manifest_revision(42);

        metrics.record_cycle_success(
            &SyncOutcome::Updated {
                changed_domains: vec!["routes".into()],
            },
            Duration::from_millis(15),
        );
        metrics.record_cycle_success(&SyncOutcome::Unchanged, Duration::from_millis(2));
        metrics.record_cycle_error(Duration::from_millis(50));

        let snap = metrics.snapshot();
        assert_eq!(snap.cycles_total, 3);
        assert_eq!(snap.updates_total, 1);
        assert_eq!(snap.unchanged_total, 1);
        assert_eq!(snap.errors_total, 1);
        assert_eq!(snap.last_duration_ms, 50);
        assert_eq!(snap.manifest_revision, 42);
        assert!(snap.cp_connected);

        let prom = metrics.render_prometheus();
        assert!(prom.contains("velda_sync_reconcile_cycles_total 3"));
        assert!(prom.contains("velda_sync_manifest_revision 42"));
        assert!(prom.contains("velda_sync_cp_connected 1"));
    }
}
