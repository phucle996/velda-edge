//! StatsD ETP configuration parser (`StatsdEtpConfig`, `StatsdFlavor`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::Statsd)
//!         │
//!         ├─► resolve_endpoint() ──► (host, port) [default 8125 UDP]
//!         ├─► StatsdFlavor::parse(flavor) ──► Standard | DogStatsd
//!         ├─► extract optional metric prefix
//!         └─► Ok(StatsdEtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | flavor: StatsdFlavor | prefix: Option<String>│
//!   │ pool: EtpPoolSettings                                                      │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings};
use crate::error::EtpError;

/// StatsD metric line format flavor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatsdFlavor {
    #[default]
    Standard,
    DogStatsd,
}

impl StatsdFlavor {
    pub fn parse(raw: &str) -> Result<Self, EtpError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "standard" | "statsd" => Ok(Self::Standard),
            "dogstatsd" | "dog_statsd" | "datadog" => Ok(Self::DogStatsd),
            other => Err(EtpError::Config(format!(
                "Unsupported StatsD flavor '{other}'; expected standard or dogstatsd"
            ))),
        }
    }
}

/// Tailored configuration for a UDP StatsD / DogStatsD exporter pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsdEtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub flavor: StatsdFlavor,
    pub prefix: Option<String>,
    pub pool: EtpPoolSettings,
}

impl StatsdEtpConfig {
    /// Parses a [`StatsdEtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::Statsd {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::Statsd.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!(
                "StatsD ETP '{}' settings must be an object",
                entry.id
            ))
        })?;

        let endpoint = obj
            .get("endpoint")
            .or_else(|| obj.get("addr"))
            .or_else(|| obj.get("address"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| format!("{host}:{port}"));

        let flavor = match obj
            .get("flavor")
            .or_else(|| obj.get("dialect"))
            .and_then(|v| v.as_str())
        {
            Some(raw) => StatsdFlavor::parse(raw)?,
            None => StatsdFlavor::Standard,
        };

        let prefix = obj
            .get("prefix")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        Ok(Self {
            id: entry.id.clone(),
            endpoint,
            host,
            port,
            flavor,
            prefix,
            pool: entry.pool.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    #[test]
    fn test_statsd_etp_parses_flavor_and_dialect_aliases() {
        let with_flavor = StatsdEtpConfig::from_entry(&EtpEntryConfig {
            id: "sd-flavor".into(),
            kind: EtpKind::Statsd,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings: serde_json::json!({
                "endpoint": "127.0.0.1:8125",
                "flavor": "dogstatsd",
                "prefix": "velda"
            }),
        })
        .unwrap();
        assert_eq!(with_flavor.flavor, StatsdFlavor::DogStatsd);
        assert_eq!(with_flavor.prefix.as_deref(), Some("velda"));

        let with_dialect = StatsdEtpConfig::from_entry(&EtpEntryConfig {
            id: "sd-dialect".into(),
            kind: EtpKind::Statsd,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings: serde_json::json!({
                "host": "127.0.0.1",
                "port": 9125,
                "dialect": "dogstatsd"
            }),
        })
        .unwrap();
        assert_eq!(with_dialect.flavor, StatsdFlavor::DogStatsd);
        assert_eq!(with_dialect.port, 9125);
        assert_eq!(with_dialect.prefix, None);
    }
}
