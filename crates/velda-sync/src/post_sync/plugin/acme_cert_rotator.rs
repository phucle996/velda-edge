//! ACME Certificate Rotator Schema & Semantic Validation (`phase: "background"`).

use serde::{Deserialize, Serialize};

use crate::SyncError;

/// Declarative configuration for the ACME Certificate Rotator plugin.
///
/// # Strict Invariant: 100% Explicit Declaration
/// Every parameter must be explicitly declared in the configuration JSON.
/// Zero defaults are permitted. Missing fields fail fast at configuration sync time.
///
/// # Trigger Model
/// The rotator runs once at bootstrap and afterwards strictly on the declared cron `schedule`
/// (there is intentionally no interval mode).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcmeCertRotatorConfig {
    /// ACME directory URL (e.g. `"https://acme-v02.api.letsencrypt.org/directory"`).
    pub directory_url: String,
    /// Account contact e-mail address.
    pub contact_email: String,
    /// Domain names to include in the certificate (first entry becomes the primary SAN).
    pub domains: Vec<String>,
    /// Renew when remaining certificate lifetime is at most this many days (1 to 89).
    pub renew_before_days: u32,
    /// Standard 5-field cron expression evaluated in UTC (`min hour day-of-month month day-of-week`).
    pub schedule: String,
    /// Directory holding the account key and per-domain LKG certificates.
    pub storage_dir: String,
}

/// Validates ACME Certificate Rotator configuration strictly according to production standards.
pub fn validate_acme_cert_rotator_config(
    plugin_id: &str,
    config_val: &serde_json::Value,
) -> Result<AcmeCertRotatorConfig, SyncError> {
    let cfg: AcmeCertRotatorConfig =
        serde_json::from_value(config_val.clone()).map_err(|e| SyncError::Validation {
            domain: "plugins".into(),
            reason: format!(
                "Plugin '{plugin_id}' has malformed or incomplete acme_cert_rotator config: {e}"
            ),
        })?;

    let fail = |reason: String| SyncError::Validation {
        domain: "plugins".into(),
        reason: format!("Plugin '{plugin_id}': {reason}"),
    };

    // 1. directory_url must be an absolute https URL (plain http is only allowed for loopback test CAs).
    let directory_url = cfg.directory_url.trim();
    let is_loopback_http = directory_url.starts_with("http://127.0.0.1")
        || directory_url.starts_with("http://localhost");
    if !directory_url.starts_with("https://") && !is_loopback_http {
        return Err(fail(format!(
            "directory_url '{directory_url}' must start with 'https://'"
        )));
    }

    // 2. contact_email: minimal structural check.
    let email = cfg.contact_email.trim();
    match email.split_once('@') {
        Some((local, host)) if !local.is_empty() && host.contains('.') => {}
        _ => {
            return Err(fail(format!(
                "contact_email '{email}' is not a valid address"
            )));
        }
    }

    // 3. domains: non-empty, LDH labels, no duplicates. Wildcards are rejected (HTTP-01 cannot validate them).
    if cfg.domains.is_empty() {
        return Err(fail("domains must contain at least one entry".into()));
    }
    let mut domains = Vec::with_capacity(cfg.domains.len());
    for d in &cfg.domains {
        let d = d.trim().to_ascii_lowercase();
        let valid = !d.is_empty()
            && d.len() <= 253
            && d.contains('.')
            && d.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && !label.starts_with('-')
                    && !label.ends_with('-')
                    && label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
            });
        if !valid {
            return Err(fail(format!(
                "domain '{d}' is invalid (wildcards are not supported with HTTP-01)"
            )));
        }
        if domains.contains(&d) {
            return Err(fail(format!("domain '{d}' is declared more than once")));
        }
        domains.push(d);
    }

    // 4. renew_before_days
    if cfg.renew_before_days == 0 || cfg.renew_before_days > 89 {
        return Err(fail(format!(
            "renew_before_days must be between 1 and 89, found {}",
            cfg.renew_before_days
        )));
    }

    // 5. schedule: exactly 5 whitespace-separated fields (full semantic parse happens in velda-plugin).
    let schedule = cfg.schedule.split_whitespace().collect::<Vec<_>>();
    if schedule.len() != 5 {
        return Err(fail(format!(
            "schedule must be a 5-field cron expression, found '{}'",
            cfg.schedule
        )));
    }

    // 6. storage_dir
    let storage_dir = cfg.storage_dir.trim();
    if storage_dir.is_empty() {
        return Err(fail("storage_dir must not be empty".into()));
    }

    Ok(AcmeCertRotatorConfig {
        directory_url: directory_url.to_string(),
        contact_email: email.to_string(),
        domains,
        renew_before_days: cfg.renew_before_days,
        schedule: schedule.join(" "),
        storage_dir: storage_dir.to_string(),
    })
}
