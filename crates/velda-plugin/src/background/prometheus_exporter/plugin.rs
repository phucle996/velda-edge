//! Prometheus Exporter Background Plugin (`prometheus_exporter::plugin`).
//!
//! Implements [`BackgroundPlugin`] for the standalone management HTTP/HTTPS metrics server.
//!
//! # Architecture & Mechanism
//!
//! ```text
//! ╔══════════════════════════════════════════════════════════════════════════════════════════════════╗
//! ║                        VELDA EDGE — PROMETHEUS EXPORTER PIPELINE                                 ║
//! ║                  (crates/velda-plugin: prometheus_exporter::plugin)                              ║
//! ╚══════════════════════════════════════════════════════════════════════════════════════════════════╝
//!
//!  [FRONTLINE WORKERS]                       [SHADOW WORKER: velda-shadow-0 (SCHED_IDLE)]
//!   velda-worker-0/1                          Management Port (:9090)
//!         │                                                │
//!         ▼ (Atomic writes)                                ▼
//!   ┌───────────┐   Lock-Free Atomic Extraction    ┌───────────────────────────────┐
//!   │ 36 Slots  │ ───────────────────────────────► │   MetricsStore::snapshot()    │
//!   │ Counters  │     (< 100ns vector reduce)      │   (36 Pipelines, 858 DPs)     │
//!   └───────────┘                                  └───────────────┬───────────────┘
//!                                                                  │
//!                                                                  ▼
//!                                                       ┌─────────────────────┐
//!                                                       │     template.rs     │
//!                                                       │ Pre-baked Skeletons │
//!                                                       │ SIMD Itoa (LUT)     │
//!                                                       │ (15,445 CPU cycles) │
//!                                                       └──────────┬──────────┘
//!                                                                  │
//!                                                                  ▼ Reused PooledBuffer
//!                                                       ┌─────────────────────┐
//!                                                       │      server.rs      │
//!                                                       │ acquire_shadow_tcp  │
//!                                                       │ mTLS + Auth + CORS  │
//!                                                       │ Concurrency Limiter │
//!                                                       └──────────┬──────────┘
//!                                                                  │
//!                                                                  ▼ HTTP/HTTPS GET /metrics
//!                                                       ┌─────────────────────────────────────┐
//!                                                       │         Prometheus Server           │
//!                                                       └─────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::watch;

use super::server::PrometheusExporter;
use crate::background::BackgroundPlugin;
use crate::error::PluginError;

/// Type alias aligning with `*Plugin` background plugin naming conventions.
pub type PrometheusExporterPlugin = PrometheusExporter;

impl BackgroundPlugin for PrometheusExporter {
    #[inline(always)]
    fn id(&self) -> &str {
        "prometheus_exporter"
    }

    fn run(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move { self.run_server(shutdown).await })
    }
}
