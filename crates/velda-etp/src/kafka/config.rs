//! Kafka ETP configuration parser (`KafkaEtpConfig`, `KafkaSaslMechanism`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Kafka)
//!         │
//!         ├─► settings.brokers[] -> parse_endpoint_target() [default 9092]
//!         ├─► fallback: resolve_endpoint() if brokers[] omitted
//!         ├─► extract client_id (default "velda-edge")
//!         ├─► parse sasl_mechanism ("none" | "plain" | "scram_sha_256" | "scram_sha_512")
//!         │     └─► if != None: require non-empty sasl_username & sasl_password
//!         └─► Ok(KafkaEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | brokers | resolved_brokers | client_id                                │
//!   │ sasl_mechanism | sasl_username | sasl_password | pool | tls                │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, parse_endpoint_target};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// SASL authentication mechanism negotiated when opening a Kafka broker socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaSaslMechanism {
    #[default]
    None,
    Plain,
    ScramSha256,
    ScramSha512,
}

impl KafkaSaslMechanism {
    /// Strictly parses a SASL mechanism identifier.
    pub fn parse(raw: &str) -> Result<Self, EtpError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "plain" => Ok(Self::Plain),
            "scram_sha_256" | "scram-sha-256" => Ok(Self::ScramSha256),
            "scram_sha_512" | "scram-sha-512" => Ok(Self::ScramSha512),
            other => Err(EtpError::Config(format!(
                "Unsupported Kafka sasl_mechanism '{other}'; expected 'none', 'plain', 'scram_sha_256', or 'scram_sha_512'"
            ))),
        }
    }

    /// Canonical Kafka protocol SASL mechanism string.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// Tailored configuration for an Apache Kafka broker pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KafkaEtpConfig {
    pub id: String,
    pub brokers: Vec<String>,
    pub resolved_brokers: Vec<(String, u16)>,
    pub client_id: String,
    pub sasl_mechanism: KafkaSaslMechanism,
    pub sasl_username: String,
    pub sasl_password: String,
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

        let sasl_mechanism = match obj
            .get("sasl_mechanism")
            .filter(|v| !v.is_null())
            .and_then(|v| v.as_str())
        {
            Some(raw) => KafkaSaslMechanism::parse(raw)?,
            None => match obj.get("sasl_mechanism").filter(|v| !v.is_null()) {
                Some(_) => {
                    return Err(EtpError::Config(format!(
                        "Kafka ETP '{}' 'sasl_mechanism' must be a string",
                        entry.id
                    )));
                }
                None => KafkaSaslMechanism::None,
            },
        };

        let sasl_username = obj
            .get("sasl_username")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("")
            .to_string();

        let sasl_password = obj
            .get("sasl_password")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if sasl_mechanism != KafkaSaslMechanism::None
            && (sasl_username.is_empty() || sasl_password.is_empty())
        {
            return Err(EtpError::Config(format!(
                "Kafka ETP '{}' requires non-empty 'sasl_username' and 'sasl_password' when sasl_mechanism is '{}'",
                entry.id,
                sasl_mechanism.as_str()
            )));
        }

        Ok(Self {
            id: entry.id.clone(),
            brokers,
            resolved_brokers,
            client_id,
            sasl_mechanism,
            sasl_username,
            sasl_password,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn make_kafka_entry(settings: serde_json::Value) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "kafka-test".into(),
            kind: EtpKind::Kafka,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings,
        }
    }

    #[test]
    fn test_kafka_etp_sasl_config_parsing_and_validation() {
        let plain = KafkaEtpConfig::from_entry(&make_kafka_entry(serde_json::json!({
            "brokers": ["127.0.0.1:9092"],
            "client_id": "velda-edge-01",
            "sasl_mechanism": "plain",
            "sasl_username": "alice",
            "sasl_password": "secret"
        })))
        .unwrap();
        assert_eq!(plain.sasl_mechanism, KafkaSaslMechanism::Plain);
        assert_eq!(plain.sasl_username, "alice");
        assert_eq!(plain.sasl_password, "secret");
        assert_eq!(plain.client_id, "velda-edge-01");

        // Missing credentials rejected when SASL is enabled
        assert!(
            KafkaEtpConfig::from_entry(&make_kafka_entry(serde_json::json!({
                "brokers": ["127.0.0.1:9092"],
                "sasl_mechanism": "plain",
                "sasl_username": "",
                "sasl_password": "secret"
            })))
            .is_err()
        );
        assert!(
            KafkaEtpConfig::from_entry(&make_kafka_entry(serde_json::json!({
                "brokers": ["127.0.0.1:9092"],
                "sasl_mechanism": "scram_sha_256",
                "sasl_username": "alice"
            })))
            .is_err()
        );

        // Invalid mechanism rejected
        assert!(
            KafkaEtpConfig::from_entry(&make_kafka_entry(serde_json::json!({
                "brokers": ["127.0.0.1:9092"],
                "sasl_mechanism": "kerberos"
            })))
            .is_err()
        );
    }
}
