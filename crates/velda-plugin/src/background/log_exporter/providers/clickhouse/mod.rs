//! ClickHouse Access Log Provider (`log_exporter::providers::clickhouse`).

pub mod format;
pub mod wire;

use std::sync::Arc;
use std::time::Duration;

use velda_etp::clickhouse::ClickHouseEtp;
use velda_observability::TelemetryBatch;

use crate::background::log_exporter::config::{ClickHouseTargetConfig, VELDA_ACCESS_LOGS_TABLE};
use crate::error::PluginError;

use self::format::{ClickHouseFormat, format_json_each_row, format_row_binary};

pub struct ClickHouseLogProvider {
    pub etp: Arc<ClickHouseEtp>,
    pub database: String,
    pub format: ClickHouseFormat,
    pub flush_interval_ms: u64,
    pub request_timeout: Duration,
    prebaked_insert_prefix: Vec<u8>,
    ddl_request: Vec<u8>,
    head_buf: Vec<u8>,
    payload_buf: Vec<u8>,
    resp_buf: Vec<u8>,
}

impl ClickHouseLogProvider {
    pub fn new(etp: Arc<ClickHouseEtp>, cfg: &ClickHouseTargetConfig) -> Result<Self, PluginError> {
        let database = if cfg.database.trim().is_empty() {
            etp.config.database.clone()
        } else {
            cfg.database.trim().to_string()
        };
        if database.is_empty() {
            return Err(PluginError::Config(
                "ClickHouse target database resolved to empty string".into(),
            ));
        }

        let prebaked_insert_prefix = wire::bake_insert_prefix(&etp.config, &database, cfg.format);
        let ddl_request = wire::bake_ddl_request(&etp.config, &database);
        let head_cap = prebaked_insert_prefix.len() + 32;

        Ok(Self {
            etp,
            database,
            format: cfg.format,
            flush_interval_ms: cfg.flush_interval_ms.max(100),
            request_timeout: Duration::from_millis(cfg.request_timeout_ms),
            prebaked_insert_prefix,
            ddl_request,
            head_buf: Vec::with_capacity(head_cap),
            payload_buf: Vec::with_capacity(64 * 1024),
            resp_buf: Vec::with_capacity(1024),
        })
    }

    pub fn name(&self) -> &'static str {
        "clickhouse"
    }

    pub async fn prepare(&mut self) -> Result<(), PluginError> {
        match wire::ensure_table(
            &self.etp,
            &self.ddl_request,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await
        {
            Ok(status) if (200..300).contains(&status) => {
                tracing::info!(
                    database = %self.database,
                    table = %VELDA_ACCESS_LOGS_TABLE,
                    "Verified ClickHouse access logs table exists"
                );
            }
            Ok(status) => {
                tracing::warn!(
                    etp_id = %self.etp.config.id,
                    status,
                    database = %self.database,
                    "Could not ensure ClickHouse table on startup"
                );
            }
            Err(e) => {
                tracing::warn!(
                    etp_id = %self.etp.config.id,
                    error = %e,
                    database = %self.database,
                    "Could not ensure ClickHouse table on startup"
                );
            }
        }
        Ok(())
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        self.payload_buf.clear();
        let encoded_recs = match self.format {
            ClickHouseFormat::RowBinary => format_row_binary(batches, &mut self.payload_buf),
            ClickHouseFormat::JsonEachRow => format_json_each_row(batches, &mut self.payload_buf),
        };
        if encoded_recs == 0 || self.payload_buf.is_empty() {
            return Ok(0);
        }

        let status = wire::push_batch(
            &self.etp,
            &self.prebaked_insert_prefix,
            &mut self.head_buf,
            &self.payload_buf,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await
        .map_err(|e| PluginError::Http(format!("ClickHouse push failed: {e}")))?;

        if (200..300).contains(&status) {
            Ok(encoded_recs)
        } else {
            Err(PluginError::Http(format!(
                "ClickHouse returned HTTP {status} for {encoded_recs} records"
            )))
        }
    }
}
