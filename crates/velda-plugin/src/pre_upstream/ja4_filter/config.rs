//! Configuration parsing & compilation for `ja4_filter` (`pre_upstream`).
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────────────┐
//! │                     JA4 FILTER CONFIGURATION COMPILATION                                │
//! ├─────────────────────────────────────────────────────────────────────────────────────────┤
//! │  JSON: { "deny": ["t13d1516h2_8daaf6152771_e5627efa2ab1", "0x..."] }                    │
//! │    │                                                                                    │
//! │    ▼ parse_ja4_entry() (Config Load Time — Zero Hot-Path Cost)                          │
//! │    ├── "0x..." hex ──► u64::from_str_radix(hex, 16)                                     │
//! │    └── 36-char JA4 ──► velda_core::ja4_fingerprint_id(s) ──► non-zero u64               │
//! │    │                                                                                    │
//! │    ▼ sort_unstable() + dedup()                                                          │
//! │  Box<[u64]> (Contiguous L1d-friendly sorted array)                                      │
//! └─────────────────────────────────────────────────────────────────────────────────────────┘
//! ```

use serde::{Deserialize, Serialize};
use velda_core::ja4_fingerprint_id;

use crate::error::PluginError;

/// Configuration for the `ja4_filter` pre-upstream plugin.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Ja4FilterConfig {
    /// List of denied JA4 TLS fingerprints (canonical 36-char JA4 strings or `0x`-prefixed u64 hex IDs).
    #[serde(default)]
    pub deny: Vec<String>,
}

/// Validates and compiles a single JA4 entry into a non-zero `u64` fingerprint ID.
pub fn parse_ja4_entry(raw: &str) -> Result<u64, PluginError> {
    let s = raw.trim();
    if s.is_empty() {
        return Err(PluginError::Config(
            "ja4_filter 'deny' entries must not be empty".to_string(),
        ));
    }

    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        if hex.is_empty() || hex.len() > 16 {
            return Err(PluginError::Config(format!(
                "ja4_filter invalid 64-bit hex fingerprint '{s}'"
            )));
        }
        let val = u64::from_str_radix(hex, 16).map_err(|_| {
            PluginError::Config(format!("ja4_filter invalid 64-bit hex fingerprint '{s}'"))
        })?;
        if val == 0 {
            return Err(PluginError::Config(
                "ja4_filter fingerprint ID cannot be 0 (reserved sentinel)".to_string(),
            ));
        }
        return Ok(val);
    }

    // Canonical FoxIO JA4 format: <10 chars>_<12 hex chars>_<12 hex chars> = 36 bytes
    let bytes = s.as_bytes();
    if bytes.len() != 36 || bytes[10] != b'_' || bytes[23] != b'_' {
        return Err(PluginError::Config(format!(
            "ja4_filter invalid JA4 string '{s}': expected 36-char '<ja4_a>_<ja4_b>_<ja4_c>' or '0x<hex>'"
        )));
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
    {
        return Err(PluginError::Config(format!(
            "ja4_filter invalid characters in JA4 string '{s}'"
        )));
    }

    Ok(ja4_fingerprint_id(s))
}

impl Ja4FilterConfig {
    /// Compiles and deduplicates the deny list into a sorted contiguous `Box<[u64]>`.
    pub fn compile_deny_ids(&self) -> Result<Box<[u64]>, PluginError> {
        let mut ids = Vec::with_capacity(self.deny.len());
        for entry in &self.deny {
            ids.push(parse_ja4_entry(entry)?);
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids.into_boxed_slice())
    }
}
