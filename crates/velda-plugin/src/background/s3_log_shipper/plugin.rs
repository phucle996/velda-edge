//! S3-Compatible Background Log Shipper Plugin.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use velda_observability::{TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::client::S3HttpClient;
use super::config::S3LogShipperConfig;
use super::format::format_ndjson;

/// Background S3 / MinIO / R2 Object Storage Log Shipper.
pub struct S3LogShipperPlugin {
    pub id: String,
    pub config: S3LogShipperConfig,
    telemetry_store: Arc<TelemetryStore>,
    shipped_records: Arc<AtomicU64>,
    failed_pushes: Arc<AtomicU64>,
}

impl S3LogShipperPlugin {
    /// Constructs a new shipper bound to the global singleton [`TelemetryStore`].
    pub fn new(id: impl Into<String>, config: S3LogShipperConfig) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new shipper bound to an explicit [`TelemetryStore`].
    pub fn with_store(
        id: impl Into<String>,
        config: S3LogShipperConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_s3_log_shipper_config(&config.id, &config.config)?;
        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }

    #[inline]
    pub fn shipped_records(&self) -> u64 {
        self.shipped_records.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn failed_pushes(&self) -> u64 {
        self.failed_pushes.load(Ordering::Relaxed)
    }
}

impl BackgroundPlugin for S3LogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }

    fn weight(&self) -> u32 {
        65
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let max_batches = self.config.max_drain_batches.max(1);
            let subscriber = self.telemetry_store.subscribe(max_batches);
            let mut payload_buf: Vec<u8> = Vec::with_capacity(256 * 1024);
            let mut client: Option<S3HttpClient> = Some(S3HttpClient::new(&self.config)?);
            let mut consecutive_failures = 0u32;
            let mut cooldown_until: Option<tokio::time::Instant> = None;
            let flush_interval = Duration::from_millis(self.config.flush_interval_ms);

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(flush_interval) => {}
                    _ = shutdown.changed() => {}
                }

                let is_shutting_down = *shutdown.borrow();

                if let Some(until) = cooldown_until {
                    if tokio::time::Instant::now() < until {
                        if is_shutting_down {
                            break;
                        }
                        continue;
                    }
                    cooldown_until = None;
                    consecutive_failures = 0;
                    client = Some(S3HttpClient::new(&self.config)?);
                }

                let batches = subscriber.drain_batches(max_batches);
                if !batches.is_empty() {
                    payload_buf.clear();
                    let encoded = format_ndjson(&batches, &mut payload_buf);
                    drop(batches);

                    if encoded > 0 && !payload_buf.is_empty() {
                        if client.is_none() {
                            client = Some(S3HttpClient::new(&self.config)?);
                        }
                        let cl = client.as_mut().unwrap();
                        match cl.put_object(&payload_buf).await {
                            Ok(status) if (200..300).contains(&status) => {
                                consecutive_failures = 0;
                                self.shipped_records
                                    .fetch_add(encoded as u64, Ordering::Relaxed);
                            }
                            Ok(status) => {
                                consecutive_failures += 1;
                                self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                cl.close_connection();
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    status,
                                    records = encoded,
                                    "S3 PUT returned non-2xx status"
                                );
                            }
                            Err(e) => {
                                consecutive_failures += 1;
                                self.failed_pushes.fetch_add(1, Ordering::Relaxed);
                                cl.close_connection();
                                tracing::warn!(
                                    plugin_id = %self.id,
                                    error = %e,
                                    records = encoded,
                                    "Failed to upload log object to S3"
                                );
                            }
                        }

                        if consecutive_failures > 10 {
                            client = None;
                            cooldown_until =
                                Some(tokio::time::Instant::now() + Duration::from_secs(30));
                            consecutive_failures = 0;
                        }
                    }
                }

                if is_shutting_down {
                    break;
                }
            }

            Ok(())
        })
    }
}
