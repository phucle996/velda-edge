//! Prometheus Exporter Configuration (`prometheus_exporter::config`).
//!
//! # Strict Declarative Contract (Zero Silent Fallbacks)
//! All configuration blocks (`scrape`, `tls`, `cors`, `auth`) are strictly mandatory.
//! Disabled features must be explicitly declared (`mode: "none"` or `enabled: false`).

pub use velda_sync::{
    AuthExporterConfig, CorsExporterConfig, PrometheusExporterConfig, ScrapeExporterConfig,
    TlsExporterConfig,
};
