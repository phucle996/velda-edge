//! S3 ETP configuration parser (`S3EtpConfig`).
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::S3)
//!         │
//!         ├─► resolve_endpoint() ──► (base_host, port) [default 80 plain / 443 tls]
//!         ├─► parse force_path_style | addressing_style ("path" | "virtual_hosted")
//!         ├─► if !force_path_style && !bucket.is_empty(): host = {bucket}.{base_host}
//!         ├─► format_host_header(&host, port, is_tls) ──► host_header
//!         ├─► extract region (default "us-east-1"), bucket, keys, session_token
//!         └─► Ok(S3EtpConfig)
//!
//!   Parsed Configuration Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ id | endpoint | host | port | host_header | region | bucket                │
//!   │ access_key | secret_key | session_token | force_path_style | pool | tls    │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};

use crate::config::{EtpEntryConfig, EtpKind, EtpPoolSettings, format_host_header};
use crate::connect::EtpTlsConfig;
use crate::error::EtpError;

/// Tailored configuration for an S3-compatible object storage ETP pool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3EtpConfig {
    pub id: String,
    pub endpoint: String,
    pub host: String,
    pub port: u16,
    pub host_header: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
    pub force_path_style: bool,
    pub pool: EtpPoolSettings,
    pub tls: Option<EtpTlsConfig>,
}

impl S3EtpConfig {
    /// Parses an [`S3EtpConfig`] from a validated [`EtpEntryConfig`].
    pub fn from_entry(entry: &EtpEntryConfig) -> Result<Self, EtpError> {
        if entry.kind != EtpKind::S3 {
            return Err(EtpError::KindMismatch {
                id: entry.id.clone(),
                expected: EtpKind::S3.as_str(),
                actual: entry.kind.as_str(),
            });
        }
        entry.validate()?;
        let (base_host, port) = entry.resolve_endpoint()?;
        let obj = entry.settings.as_object().ok_or_else(|| {
            EtpError::Config(format!("S3 ETP '{}' settings must be an object", entry.id))
        })?;

        let raw_endpoint = obj
            .get("endpoint")
            .or_else(|| obj.get("url"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                let scheme = if entry.tls.is_some() { "https" } else { "http" };
                format!("{scheme}://{base_host}:{port}")
            });

        let is_tls =
            entry.tls.is_some() || raw_endpoint.to_ascii_lowercase().starts_with("https://");

        let region = obj
            .get("region")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("us-east-1")
            .to_string();

        let bucket = obj
            .get("bucket")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .unwrap_or("")
            .to_string();

        let access_key = obj
            .get("access_key")
            .or_else(|| obj.get("access_key_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let secret_key = obj
            .get("secret_key")
            .or_else(|| obj.get("secret_access_key"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let session_token = obj
            .get("session_token")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string);

        let force_path_style = if let Some(v) = obj.get("force_path_style").filter(|v| !v.is_null())
        {
            v.as_bool().ok_or_else(|| {
                EtpError::Config(format!(
                    "S3 ETP '{}' 'force_path_style' must be a boolean",
                    entry.id
                ))
            })?
        } else if let Some(v) = obj.get("addressing_style").filter(|v| !v.is_null()) {
            match v.as_str().map(str::trim) {
                Some(s) if s.eq_ignore_ascii_case("path") => true,
                Some(s) if s.eq_ignore_ascii_case("virtual_hosted") => false,
                _ => {
                    return Err(EtpError::Config(format!(
                        "S3 ETP '{}' 'addressing_style' must be 'path' or 'virtual_hosted'",
                        entry.id
                    )));
                }
            }
        } else {
            false
        };

        let host = if !force_path_style
            && !bucket.is_empty()
            && !base_host.starts_with(&format!("{bucket}."))
        {
            format!("{bucket}.{base_host}")
        } else {
            base_host
        };
        let host_header = format_host_header(&host, port, is_tls);

        Ok(Self {
            id: entry.id.clone(),
            endpoint: raw_endpoint,
            host,
            port,
            host_header,
            region,
            bucket,
            access_key,
            secret_key,
            session_token,
            force_path_style,
            pool: entry.pool.clone(),
            tls: entry.tls.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EtpPoolSettings;

    fn make_s3_entry(settings: serde_json::Value) -> EtpEntryConfig {
        EtpEntryConfig {
            id: "s3-test".into(),
            kind: EtpKind::S3,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings,
        }
    }

    #[test]
    fn test_s3_virtual_hosted_and_path_style_and_session_token() {
        let vh = S3EtpConfig::from_entry(&make_s3_entry(serde_json::json!({
            "endpoint": "https://s3.us-west-2.amazonaws.com",
            "bucket": "velda-prod",
            "region": "us-west-2",
            "access_key_id": "AKIA_VH",
            "secret_access_key": "SECRET_VH",
            "session_token": "STS_TOK_123",
            "addressing_style": "virtual_hosted"
        })))
        .unwrap();
        assert!(!vh.force_path_style);
        assert_eq!(vh.host, "velda-prod.s3.us-west-2.amazonaws.com");
        assert_eq!(vh.host_header, "velda-prod.s3.us-west-2.amazonaws.com");
        assert_eq!(vh.port, 443);
        assert_eq!(vh.access_key, "AKIA_VH");
        assert_eq!(vh.secret_key, "SECRET_VH");
        assert_eq!(vh.session_token.as_deref(), Some("STS_TOK_123"));

        // Does not double-prefix bucket when host already starts with "{bucket}."
        let already_prefixed = S3EtpConfig::from_entry(&make_s3_entry(serde_json::json!({
            "endpoint": "https://velda-prod.s3.us-west-2.amazonaws.com",
            "bucket": "velda-prod"
        })))
        .unwrap();
        assert_eq!(
            already_prefixed.host,
            "velda-prod.s3.us-west-2.amazonaws.com"
        );

        let path_bool = S3EtpConfig::from_entry(&make_s3_entry(serde_json::json!({
            "endpoint": "http://127.0.0.1:9000",
            "bucket": "velda-local",
            "force_path_style": true,
            "session_token": "   "
        })))
        .unwrap();
        assert!(path_bool.force_path_style);
        assert_eq!(path_bool.host, "127.0.0.1");
        assert_eq!(path_bool.host_header, "127.0.0.1:9000");
        assert_eq!(path_bool.session_token, None);

        let path_str = S3EtpConfig::from_entry(&make_s3_entry(serde_json::json!({
            "endpoint": "http://127.0.0.1:9000",
            "bucket": "velda-local",
            "addressing_style": "path"
        })))
        .unwrap();
        assert!(path_str.force_path_style);
        assert_eq!(path_str.host, "127.0.0.1");

        assert!(
            S3EtpConfig::from_entry(&make_s3_entry(serde_json::json!({
                "endpoint": "http://127.0.0.1:9000",
                "addressing_style": "unknown"
            })))
            .is_err()
        );
    }
}
