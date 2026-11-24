//! Canary Compiled Configuration Types (`canary::config`).
//!
//! ```text
//! CanarySchemaConfig ──► schema_to_config() ──► CanaryConfig
//!   ├─ canary_upstream : String
//!   ├─ ratio           : u32 (1..=999)
//!   ├─ rules           : Vec<HeaderRule>
//!   └─ hash_headers    : Vec<HeaderName> (non-empty)
//! ```

use http::HeaderName;

use super::validation::CanarySchemaConfig;
use crate::error::PluginError;

/// Matching operator for canary header override rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleOperator {
    Equals,
    Prefix,
    Suffix,
    Contains,
    Exists,
}

impl RuleOperator {
    /// Parses operator string (`"equals"`, `"prefix"`, `"suffix"`, `"contains"`, `"exists"`).
    pub fn parse(s: &str) -> Result<Self, PluginError> {
        match s.trim().to_ascii_lowercase().as_str() {
            "equals" => Ok(Self::Equals),
            "prefix" => Ok(Self::Prefix),
            "suffix" => Ok(Self::Suffix),
            "contains" => Ok(Self::Contains),
            "exists" => Ok(Self::Exists),
            other => Err(PluginError::Config(format!(
                "Unsupported canary rule operator '{other}'"
            ))),
        }
    }
}

/// Read-only rule matching directly on a single header inside `HeaderWorkspace`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderRule {
    pub header: HeaderName,
    pub operator: RuleOperator,
    pub pattern: Box<[u8]>,
}

/// Programmatic Canary configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryConfig {
    pub canary_upstream: String,
    /// Traffic share routed to canary in permille (`1..=999`, e.g. `5` = `0.5%`, `250` = `25%`).
    pub ratio: u32,
    pub rules: Vec<HeaderRule>,
    /// Ordered headers folded jointly for deterministic sticky hashing.
    pub hash_headers: Vec<HeaderName>,
}

pub(crate) fn schema_to_config(schema: CanarySchemaConfig) -> Result<CanaryConfig, PluginError> {
    let mut rules = Vec::with_capacity(schema.rules.len());
    for r in &schema.rules {
        let header = HeaderName::from_bytes(r.name.trim().as_bytes()).map_err(|e| {
            PluginError::Config(format!("Invalid header rule name '{}': {e}", r.name))
        })?;
        let operator = RuleOperator::parse(&r.operator)?;
        let pattern: Box<[u8]> = if matches!(operator, RuleOperator::Exists) {
            Box::new([])
        } else {
            r.value.trim().as_bytes().into()
        };
        rules.push(HeaderRule {
            header,
            operator,
            pattern,
        });
    }

    let mut hash_headers = Vec::with_capacity(schema.hash_headers.len());
    for hdr in &schema.hash_headers {
        let name = HeaderName::from_bytes(hdr.trim().as_bytes())
            .map_err(|e| PluginError::Config(format!("Invalid hash header '{hdr}': {e}")))?;
        hash_headers.push(name);
    }

    Ok(CanaryConfig {
        canary_upstream: schema.canary_upstream.trim().to_string(),
        ratio: schema.ratio,
        rules,
        hash_headers,
    })
}
