//! Static-Dispatch Trace Export Providers (`trace_exporter::providers`).
//!
//! ```text
//!   EtpInstance.backend (attach_etp)
//!         └─► EtpBackend::Otlp ──► EtpTraceAttachment -> OtlpTraceProvider
//! ```

pub mod otlp;

use std::sync::Arc;

use velda_etp::otlp::OtlpEtp;
use velda_etp::{EtpBackend, EtpInstance};
use velda_observability::TelemetryBatch;

use crate::error::PluginError;

use self::otlp::OtlpTraceProvider;
use super::config::TraceTargetConfig;

/// Validated, pre-bound ETP handle paired with its trace target configuration.
#[derive(Clone)]
pub enum EtpTraceAttachment {
    Otlp {
        etp: Arc<OtlpEtp>,
        config: TraceTargetConfig,
    },
}

impl EtpTraceAttachment {
    pub fn from_etp_instance(
        plugin_id: &str,
        cfg: &TraceTargetConfig,
        instance: &EtpInstance,
    ) -> Result<Self, PluginError> {
        match &instance.backend {
            EtpBackend::Otlp(pool) => Ok(Self::Otlp {
                etp: Arc::clone(pool),
                config: cfg.clone(),
            }),
            _ => Err(PluginError::Config(format!(
                "Trace plugin '{plugin_id}' requires an 'otlp' ETP for etp_id '{}', got '{}'",
                cfg.etp_id, instance.kind
            ))),
        }
    }
}

/// Owned trace provider ready to consume span records and export over `velda-etp`.
pub enum TraceProvider {
    Otlp(OtlpTraceProvider),
}

impl TraceProvider {
    pub fn from_attachment(attachment: &EtpTraceAttachment) -> Result<Self, PluginError> {
        match attachment {
            EtpTraceAttachment::Otlp { etp, config } => {
                Ok(Self::Otlp(OtlpTraceProvider::new(Arc::clone(etp), config)?))
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Otlp(_) => "otlp",
        }
    }

    pub fn flush_interval_ms(&self) -> u64 {
        match self {
            Self::Otlp(p) => p.flush_interval_ms,
        }
    }

    pub async fn flush_batches(
        &mut self,
        batches: &[TelemetryBatch],
    ) -> Result<usize, PluginError> {
        match self {
            Self::Otlp(p) => p.flush_batches(batches).await,
        }
    }
}
