//! Plugin Semantic Validation & Phase Dispatch Engine (`velda-plugin::validation`).
//!
//! ```text
//!                         VELDA PLUGIN VALIDATION DISPATCHER
//!
//!         +-------------------------------------------------------------+
//!         |                  PluginConfig (Raw JSON)                    |
//!         +-------------------------------------------------------------+
//!                                        |
//!                                        v
//!         +-------------------------------------------------------------+
//!         |   Phase 1: General Sanity Check & Canonical Phase Binding   |
//!         |   - Trim whitespace & lowercase phase                       |
//!         |   - Check non-empty ID                                      |
//!         |   - Auto-infer missing phase from canonical registry        |
//!         +-------------------------------------------------------------+
//!                                        |
//!                                        v
//!         +-------------------------------------------------------------+
//!         |   Phase 2: Modular Semantic Validation Dispatch             |
//!         |   Dispatches to module-local `validate_*_config` by ID:     |
//!         |   - Pre-Route: ip_filter, cors_preflight, resource_limit... |
//!         |   - Pre-Upstream: api_key, jwt_auth, rate_limit, csrf...    |
//!         |   - Post-Response: cors, security_headers, compression...   |
//!         |   - Background: prometheus, health_probe, jwks_rotator...   |
//!         +-------------------------------------------------------------+
//!                                        |
//!                                        v
//!         +-------------------------------------------------------------+
//!         |   Phase 3: Normalized Config Serialization & Sync Ready     |
//!         |   Serialized back into `plugin.config` for runtime use      |
//!         +-------------------------------------------------------------+
//! ```
//!
//! # Architectural Invariants
//! - **Single Source of Truth**: Plugins own both their runtime behavior and validation logic.
//! - **Strict Declarative Contracts**: Zero silent fallbacks. Invalid configurations fail fast during sync.
//! - **Hot-Path Isolation**: Validation occurs strictly at sync / staging time, never on the serving hot path.

use crate::config::PluginConfig;
use crate::error::PluginError;

/// Returns the canonical execution phase for a known plugin identifier.
pub fn canonical_phase_for_id(id: &str) -> Option<&'static str> {
    match id {
        "api_key_auth" => Some("pre_upstream"),
        "jwt_auth" => Some("pre_upstream"),
        "hmac_auth" => Some("pre_upstream"),
        "oauth2_introspection" => Some("pre_upstream"),
        "ip_filter" => Some("pre_route"),
        "cors_preflight" => Some("pre_route"),
        "resource_limit" => Some("pre_route"),
        "waf_lite" => Some("pre_route"),
        "rate_limit" => Some("pre_upstream"),
        "header_mutation" => Some("pre_upstream"),
        "canary" => Some("pre_upstream"),
        "request_mirror" => Some("pre_upstream"),
        "url_rewrite" => Some("pre_upstream"),
        "http_cache_lookup" => Some("pre_upstream"),
        "http_fault_injection" => Some("pre_upstream"),
        "grpc_fault_injection" => Some("pre_upstream"),
        "request_termination" => Some("pre_upstream"),
        "csrf" => Some("pre_upstream"),
        "csrf_protection" => Some("pre_upstream"),
        "consumer_quota" => Some("pre_upstream"),
        "api_metering" => Some("pre_upstream"),
        "security_headers" => Some("post_response"),
        "cors" => Some("post_response"),
        "http_cache_store" => Some("post_response"),
        "response_header_mutation" => Some("post_response"),
        "response_compression" => Some("post_response"),
        "ai_token_metering" => Some("post_response"),
        "prometheus_exporter" => Some("background"),
        "health_probe" => Some("background"),
        "otlp_metrics_exporter" => Some("background"),
        "statsd_exporter" => Some("background"),
        "acme_cert_rotator" => Some("background"),
        "jwks_rotator" => Some("background"),
        "loki_log_shipper" => Some("background"),
        "std_log_shipper" => Some("background"),
        "otlp_log_shipper" => Some("background"),
        "clickhouse_log_shipper" => Some("background"),
        "otlp_trace_exporter" => Some("background"),
        _ => None,
    }
}

/// Returns the restricted protocol family for a known plugin identifier, if any.
pub fn canonical_protocol_family_for_id(id: &str) -> Option<&'static str> {
    match id {
        "grpc_fault_injection" => Some("grpc"),
        "http_fault_injection" => Some("http"),
        "cors_preflight" => Some("http"),
        "cors" => Some("http"),
        "http_cache_lookup" => Some("http"),
        "http_cache_store" => Some("http"),
        "response_header_mutation" => Some("http"),
        "response_compression" => Some("http"),
        "request_termination" => Some("http"),
        "ai_token_metering" => Some("http"),
        _ => None,
    }
}

#[inline]
fn trim_in_place(s: &mut String) {
    let trimmed_len = s.trim().len();
    if trimmed_len != s.len() {
        *s = s.trim().to_string();
    }
}

/// Validates and normalizes a single plugin configuration in place.
pub fn validate_plugin_config(plugin: &mut PluginConfig) -> Result<(), PluginError> {
    trim_in_place(&mut plugin.id);
    trim_in_place(&mut plugin.phase);
    plugin.phase.make_ascii_lowercase();

    if plugin.id.is_empty() {
        return Err(PluginError::Config("Plugin ID cannot be empty".into()));
    }

    if plugin.phase.is_empty()
        && let Some(canonical) = canonical_phase_for_id(&plugin.id)
    {
        plugin.phase = canonical.to_string();
    }

    let is_valid_phase = matches!(
        plugin.phase.as_str(),
        "pre_route" | "pre_upstream" | "post_response" | "background"
    );

    if !is_valid_phase {
        return Err(PluginError::Config(format!(
            "Plugin '{}' has invalid phase '{}' (must be pre_route, pre_upstream, post_response, or background)",
            plugin.id, plugin.phase
        )));
    }

    // Modular semantic validation dispatched per plugin ID
    match plugin.id.as_str() {
        "ip_filter" => {
            if plugin.phase != "pre_route" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_route'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_route::ip_filter::validation::validate_ip_filter_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "cors_preflight" => {
            if plugin.phase != "pre_route" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_route'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_route::cors_preflight::validation::validate_cors_preflight_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "resource_limit" => {
            if plugin.phase != "pre_route" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_route'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_route::resource_limit::validation::validate_resource_limit_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "waf_lite" => {
            if plugin.phase != "pre_route" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_route'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_route::waf_lite::validation::validate_waf_lite_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "api_key_auth" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_upstream::api_key_auth::validation::validate_api_key_auth_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "jwt_auth" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::jwt_auth::validation::validate_jwt_auth_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "rate_limit" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_upstream::rate_limit::validation::validate_rate_limit_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "canary" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::canary::validation::validate_canary_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "request_mirror" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_upstream::request_mirror::validation::validate_request_mirror_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "url_rewrite" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_upstream::url_rewrite::validation::validate_url_rewrite_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "grpc_fault_injection" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::grpc_fault_injection::validation::validate_grpc_fault_injection_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "http_fault_injection" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::http_fault_injection::validation::validate_http_fault_injection_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "request_termination" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::request_termination::validation::validate_request_termination_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "header_mutation" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized =
                crate::pre_upstream::header_mutation::validation::validate_header_mutation_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "hmac_auth" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::hmac_auth::validation::validate_hmac_auth_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "oauth2_introspection" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::oauth2_introspection::validation::validate_oauth2_introspection_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "http_cache_lookup" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::http_cache_lookup::validation::validate_http_cache_lookup_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "csrf" | "csrf_protection" => {
            if plugin.phase != "pre_upstream" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'pre_upstream'",
                    plugin.id
                )));
            }
            let normalized = crate::pre_upstream::csrf::validation::validate_csrf_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "security_headers" => {
            if plugin.phase != "post_response" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'post_response'",
                    plugin.id
                )));
            }
            let normalized = crate::post_response::security_headers::validation::validate_security_headers_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "cors" => {
            if plugin.phase != "post_response" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'post_response'",
                    plugin.id
                )));
            }
            let normalized = crate::post_response::cors::validation::validate_cors_response_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "http_cache_store" => {
            if plugin.phase != "post_response" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'post_response'",
                    plugin.id
                )));
            }
            let normalized = crate::post_response::http_cache_store::validation::validate_http_cache_store_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "response_header_mutation" => {
            if plugin.phase != "post_response" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'post_response'",
                    plugin.id
                )));
            }
            let normalized = crate::post_response::response_header_mutation::validation::validate_response_header_mutation_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "response_compression" => {
            if plugin.phase != "post_response" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'post_response'",
                    plugin.id
                )));
            }
            let normalized = crate::post_response::response_compression::validation::validate_response_compression_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "prometheus_exporter" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized = crate::background::prometheus_exporter::validation::validate_prometheus_exporter_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "health_probe" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::health_probe::validation::validate_health_probe_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "otlp_metrics_exporter" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized = crate::background::otlp_metrics_exporter::validation::validate_otlp_metrics_exporter_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "statsd_exporter" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::statsd_exporter::validation::validate_statsd_exporter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "acme_cert_rotator" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized = crate::background::acme_cert_rotator::validation::validate_acme_cert_rotator_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "jwks_rotator" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::jwks_rotator::validation::validate_jwks_rotator_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "loki_log_shipper" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::loki_log_shipper::validation::validate_loki_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "std_log_shipper" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::std_log_shipper::validation::validate_std_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "otlp_log_shipper" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::otlp_log_shipper::validation::validate_otlp_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "otlp_trace_exporter" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::otlp_trace_exporter::validate_otlp_trace_exporter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "clickhouse_log_shipper" => {
            if plugin.phase != "background" {
                return Err(PluginError::Config(format!(
                    "Plugin '{}' must specify phase 'background'",
                    plugin.id
                )));
            }
            let normalized =
                crate::background::clickhouse_log_shipper::validation::validate_clickhouse_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        _ => {}
    }

    Ok(())
}

/// Enforces cross-plugin invariants and dependencies across all active plugins.
pub fn validate_plugin_dependencies(plugins: &[PluginConfig]) -> Result<(), PluginError> {
    for plugin in plugins.iter() {
        if plugin.id == "jwks_rotator" {
            let cfg: crate::background::jwks_rotator::validation::JwksRotatorConfig =
                serde_json::from_value(plugin.config.clone()).map_err(|e| {
                    PluginError::Config(format!(
                        "Plugin '{}' failed to parse jwks_rotator config: {e}",
                        plugin.id
                    ))
                })?;
            let target_id = cfg.target_jwt_auth_id.trim();

            let target_exists = plugins.iter().any(|p| {
                p.id == target_id
                    && (p.phase == "pre_upstream"
                        || canonical_phase_for_id(&p.id) == Some("pre_upstream"))
            });

            if !target_exists {
                return Err(PluginError::Config(format!(
                    "Plugin 'jwks_rotator' requires target jwt_auth plugin '{target_id}' with phase 'pre_upstream', but it was not found in active plugins"
                )));
            }
        }
    }
    Ok(())
}
