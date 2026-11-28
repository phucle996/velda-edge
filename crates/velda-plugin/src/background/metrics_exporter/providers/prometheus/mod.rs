//! Prometheus OpenMetrics Pull Scrape Provider (`metrics_exporter::providers::prometheus`).

pub mod server;
pub mod template;

pub use server::PrometheusExporter;
pub use template::{PrometheusTemplate, format_snapshot};
