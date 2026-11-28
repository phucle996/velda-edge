//! Local Stream / File Access Log Provider (`log_exporter::providers::local`).

pub mod format;
pub mod sink;

use velda_observability::TelemetryBatch;

use crate::background::log_exporter::config::{LocalLogTargetConfig, TargetStream};
use crate::error::PluginError;

use self::format::format_std_log_payload;
use self::sink::StdLogSink;

pub struct LocalLogProvider {
    pub config: LocalLogTargetConfig,
    pub target: TargetStream,
    sink: Option<StdLogSink>,
    out_buf: Vec<u8>,
}

impl LocalLogProvider {
    pub fn new(config: LocalLogTargetConfig) -> Self {
        let target = TargetStream::parse(&config.target);
        Self {
            config,
            target,
            sink: None,
            out_buf: Vec::with_capacity(16384),
        }
    }

    pub fn flush_interval_ms(&self) -> u64 {
        self.config.flush_interval_ms.max(50)
    }

    pub fn max_drain_batches(&self) -> usize {
        64
    }

    pub fn name(&self) -> &'static str {
        "local"
    }

    pub async fn prepare(&mut self) -> Result<(), PluginError> {
        self.sink = Some(StdLogSink::init(&self.target).await?);
        Ok(())
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        let Some(sink) = self.sink.as_mut() else {
            return Err(PluginError::Config(
                "Local log provider invoked before prepare()".into(),
            ));
        };
        let record_count = format_std_log_payload(batches, &self.config.format, &mut self.out_buf);
        if record_count > 0 && !self.out_buf.is_empty() {
            sink.write_all(&self.out_buf).await?;
        }
        Ok(record_count)
    }
}
