//! Kafka ETP configuration parser (`KafkaEtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Kafka)
//!         │
//!         ├─► settings.brokers[] -> parse_endpoint_target() [default 9092]
//!         ├─► fallback: resolve_endpoint() if brokers[] omitted
//!         ├─► extract client_id (default "velda-edge")
//!         └─► Ok(KafkaEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | brokers: Vec<String> | resolved_brokers: Vec<(String, u16)>           │
//!   │ client_id: String | pool: EtpPoolSettings | tls: Option<EtpTlsConfig>      │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, parse_endpoint_target};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for an Apache Kafka broker pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaEtpConfig {
    pub id: String,
    pub brokers: Vec<String>,
    pub resolved_brokers: Vec<(String, u16)>,
    pub client_id: String,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl KafkaEtpConfig {
    /// Parses a [`KafkaEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Kafka {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Kafka.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "Kafka ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let tls_hint = entry.tls.is_some();
        let mut brokers = Vec::new();
        let mut resolved_brokers = Vec::new();

        if let Some(arr) = obj.get("brokers").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(raw) = item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    let target = parse_endpoint_target(raw, EtpKind::Kafka, tls_hint, &entry.id)?;
                    brokers.push(raw.to_string());
                    resolved_brokers.push(target);
                }
            }
        }

        if resolved_brokers.is_empty() {
            let (host, port) = entry.resolve_endpoint()?;
            brokers.push(format!("{host}:{port}"));
            resolved_brokers.push((host, port));
        }

        let client_id = obj
            .get("client_id")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("velda-edge")
            .to_string();

        Ok(Self {
            id: entry.id.clone(),
            brokers,
            resolved_brokers,
            client_id,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}
