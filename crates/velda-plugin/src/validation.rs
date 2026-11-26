//! Plugin Semantic Validation Dispatcher (`velda-plugin::validation`).
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
//!         |   Step 1: Compile-Time Plugin ID Verification               |
//!         |   - Trim whitespace on `plugin.id`                          |
//!         |   - Reject empty or unknown compiled-in plugin IDs          |
//!         +-------------------------------------------------------------+
//!                                        |
//!                                        v
//!         +-------------------------------------------------------------+
//!         |   Step 2: Modular Semantic Validation Dispatch              |
//!         |   Dispatches to module-local `validate_*_config` by ID:     |
//!         |   - Pre-Route: ip_filter, cors_preflight, resource_limit... |
//!         |   - Pre-Upstream: api_key, jwt_auth, rate_limit, csrf...    |
//!         |   - Post-Response: cors, security_headers, compression...   |
//!         |   - Background: prometheus, health_probe, acme_cert_rotator |
//!         +-------------------------------------------------------------+
//!                                        |
//!                                        v
//!         +-------------------------------------------------------------+
//!         |   Step 3: Normalized Config Serialization & Sync Ready      |
//!         |   Serialized back into `plugin.config` for runtime use      |
//!         +-------------------------------------------------------------+
//! ```
//!
//! # Architectural Invariants
//! - **Single Source of Truth**: Plugins own both their runtime behavior and validation logic.
//! - **Compile-Time Stage Binding**: Which stage a plugin runs in is fixed in Rust at compile time (`pre_route`, `pre_upstream`, `post_response`, `background`).
//! - **Strict Declarative Contracts**: Zero silent fallbacks. Invalid configurations fail fast during sync.
//! - **Hot-Path Isolation**: Validation occurs strictly at sync / staging time, never on the serving hot path.

use crate::config::PluginConfig;
use crate::error::PluginError;

/// Returns `true` if `id` is compiled into one of Velda's plugin stage modules.
pub fn is_known_plugin_id(id: &str) -> bool {
    crate::pre_route::owns_id(id)
        || crate::pre_upstream::owns_id(id)
        || crate::post_response::owns_id(id)
        || crate::background::owns_id(id)
}

/// Returns the restricted protocol family for a known plugin identifier, if any.
pub fn canonical_protocol_family_for_id(id: &str) -> Option<&'static str> {
    match id {
        "grpc_fault_injection" => Some("grpc"),
        "http_fault_injection" => Some("http"),
        "cors_preflight" => Some("http"),
        "https_redirect" => Some("http"),
        "cors" => Some("http"),
        "http_cache_lookup" => Some("http"),
        "http_cache_store" => Some("http"),
        "response_header_mutation" => Some("http"),
        "response_compression" => Some("http"),
        "request_termination" => Some("http"),
        "url_rewrite" => Some("http"),
        "openai_token_quota" => Some("http"),
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

    if plugin.id.is_empty() {
        return Err(PluginError::Config("Plugin ID cannot be empty".into()));
    }

    if !is_known_plugin_id(&plugin.id) {
        return Err(PluginError::Config(format!(
            "Unknown plugin ID '{}'",
            plugin.id
        )));
    }

    // Modular semantic validation dispatched per plugin ID
    match plugin.id.as_str() {
        "ip_filter" => {
            let normalized = crate::pre_route::ip_filter::validation::validate_ip_filter_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "cors_preflight" => {
            let normalized =
                crate::pre_route::cors_preflight::validation::validate_cors_preflight_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "resource_limit" => {
            let normalized =
                crate::pre_route::resource_limit::validation::validate_resource_limit_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "waf_lite" => {
            let normalized = crate::pre_route::waf_lite::validation::validate_waf_lite_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "method_filter" => {
            let normalized =
                crate::pre_route::method_filter::validation::validate_method_filter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "https_redirect" => {
            let normalized =
                crate::pre_route::https_redirect::validation::validate_https_redirect_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "api_key_auth" => {
            let normalized =
                crate::pre_upstream::api_key_auth::validation::validate_api_key_auth_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "jwt_auth" => {
            let normalized = crate::pre_upstream::jwt_auth::validation::validate_jwt_auth_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "rate_limit" => {
            let normalized =
                crate::pre_upstream::rate_limit::validation::validate_rate_limit_config(
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "canary" => {
            let normalized = crate::pre_upstream::canary::validation::validate_canary_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "request_mirror" => {
            let normalized =
                crate::pre_upstream::request_mirror::validation::validate_request_mirror_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "url_rewrite" => {
            let normalized =
                crate::pre_upstream::url_rewrite::validation::validate_url_rewrite_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "grpc_fault_injection" => {
            let normalized = crate::pre_upstream::grpc_fault_injection::validation::validate_grpc_fault_injection_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "http_fault_injection" => {
            let normalized = crate::pre_upstream::http_fault_injection::validation::validate_http_fault_injection_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "request_termination" => {
            let normalized = crate::pre_upstream::request_termination::validation::validate_request_termination_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "header_mutation" => {
            let normalized =
                crate::pre_upstream::header_mutation::validation::validate_header_mutation_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "hmac_auth" => {
            let normalized = crate::pre_upstream::hmac_auth::validation::validate_hmac_auth_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "oauth2_introspection" => {
            let normalized = crate::pre_upstream::oauth2_introspection::validation::validate_oauth2_introspection_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "http_cache_lookup" => {
            let normalized = crate::pre_upstream::http_cache_lookup::validation::validate_http_cache_lookup_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "csrf" | "csrf_protection" => {
            let normalized = crate::pre_upstream::csrf::validation::validate_csrf_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "ja4_filter" => {
            plugin.config =
                crate::pre_upstream::ja4_filter::validation::validate_ja4_filter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
        }
        "security_headers" => {
            let normalized = crate::post_response::security_headers::validation::validate_security_headers_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "openai_token_quota" => {
            let normalized = crate::post_response::openai_token_quota::validation::validate_openai_token_quota_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "cors" => {
            let normalized = crate::post_response::cors::validation::validate_cors_response_config(
                &plugin.id,
                &plugin.config,
            )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "http_cache_store" => {
            let normalized = crate::post_response::http_cache_store::validation::validate_http_cache_store_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "response_header_mutation" => {
            let normalized = crate::post_response::response_header_mutation::validation::validate_response_header_mutation_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "response_compression" => {
            let normalized = crate::post_response::response_compression::validation::validate_response_compression_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "prometheus_exporter" => {
            let normalized = crate::background::prometheus_exporter::validation::validate_prometheus_exporter_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "health_probe" => {
            let normalized =
                crate::background::health_probe::validation::validate_health_probe_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "otlp_metrics_exporter" => {
            let normalized = crate::background::otlp_metrics_exporter::validation::validate_otlp_metrics_exporter_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "statsd_exporter" => {
            let normalized =
                crate::background::statsd_exporter::validation::validate_statsd_exporter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "acme_cert_rotator" => {
            let normalized = crate::background::acme_cert_rotator::validation::validate_acme_cert_rotator_config(&plugin.id, &plugin.config)?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "loki_log_shipper" => {
            let normalized =
                crate::background::loki_log_shipper::validation::validate_loki_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "std_log_shipper" => {
            let normalized =
                crate::background::std_log_shipper::validation::validate_std_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "otlp_log_shipper" => {
            let normalized =
                crate::background::otlp_log_shipper::validation::validate_otlp_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "otlp_trace_exporter" => {
            let normalized =
                crate::background::otlp_trace_exporter::validate_otlp_trace_exporter_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "clickhouse_log_shipper" => {
            let normalized =
                crate::background::clickhouse_log_shipper::validation::validate_clickhouse_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "elasticsearch_log_shipper" => {
            let normalized =
                crate::background::elasticsearch_log_shipper::validation::validate_elasticsearch_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "s3_log_shipper" => {
            let normalized =
                crate::background::s3_log_shipper::validation::validate_s3_log_shipper_config(
                    &plugin.id,
                    &plugin.config,
                )?;
            plugin.config =
                serde_json::to_value(normalized).map_err(|e| PluginError::Config(e.to_string()))?;
        }
        "kafka_log_shipper" => {
            let normalized =
                crate::background::kafka_log_shipper::validation::validate_kafka_log_shipper_config(
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

fn record_zone_policy(
    zone_raw: &str,
    max_memory_mb: usize,
    key_headers: &[String],
    owner: &'static str,
    zone_budgets: &mut std::collections::HashMap<String, usize>,
    zone_key_headers: &mut std::collections::HashMap<String, (&'static str, Vec<String>)>,
) -> Result<(), PluginError> {
    let zone = zone_raw.trim().to_string();
    let entry = zone_budgets.entry(zone.clone()).or_insert(0);
    *entry = (*entry).max(max_memory_mb);

    let mut norm_keys: Vec<String> = key_headers
        .iter()
        .map(|h| h.trim().to_ascii_lowercase())
        .collect();
    norm_keys.sort_unstable();
    norm_keys.dedup();

    match zone_key_headers.get(&zone) {
        Some((prev_owner, prev_keys)) if *prev_keys != norm_keys => {
            return Err(PluginError::Config(format!(
                "HTTP cache zone '{zone}' has mismatched key_headers between {prev_owner} ({prev_keys:?}) and {owner} ({norm_keys:?})"
            )));
        }
        None => {
            zone_key_headers.insert(zone, (owner, norm_keys));
        }
        _ => {}
    }
    Ok(())
}

/// Enforces cross-plugin invariants and dependencies across all active plugins.
pub fn validate_plugin_dependencies(plugins: &[PluginConfig]) -> Result<(), PluginError> {
    let mut zone_budgets: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut zone_key_headers: std::collections::HashMap<String, (&'static str, Vec<String>)> =
        std::collections::HashMap::new();

    for plugin in plugins.iter() {
        if plugin.id == "http_cache_lookup" {
            if let Ok(cfg) = serde_json::from_value::<
                crate::pre_upstream::http_cache_lookup::validation::HttpCacheLookupConfig,
            >(plugin.config.clone())
            {
                record_zone_policy(
                    &cfg.zone,
                    cfg.max_memory_mb,
                    &cfg.key_headers,
                    "http_cache_lookup",
                    &mut zone_budgets,
                    &mut zone_key_headers,
                )?;
            }
        } else if plugin.id == "http_cache_store"
            && let Ok(cfg) = serde_json::from_value::<
                crate::post_response::http_cache_store::validation::HttpCacheStoreConfig,
            >(plugin.config.clone())
        {
            record_zone_policy(
                &cfg.zone,
                cfg.max_memory_mb,
                &cfg.key_headers,
                "http_cache_store",
                &mut zone_budgets,
                &mut zone_key_headers,
            )?;
        }
    }

    let total_cache_mb: usize = zone_budgets
        .values()
        .copied()
        .fold(0usize, usize::saturating_add);
    let max_cache_mb = crate::http_cache::max_cache_memory_mb();
    if total_cache_mb > max_cache_mb {
        return Err(PluginError::Config(format!(
            "Aggregate HTTP cache memory budget across zones ({total_cache_mb} MB) exceeds ceiling {max_cache_mb} MB ({}% of system memory)",
            crate::http_cache::CACHE_MEMORY_CEILING_PERCENT
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cache_pair(
        zone_a: &str,
        keys_a: Vec<&str>,
        zone_b: &str,
        keys_b: Vec<&str>,
    ) -> Vec<PluginConfig> {
        vec![
            PluginConfig {
                id: "http_cache_lookup".into(),
                config: json!({
                    "zone": zone_a,
                    "cacheable_methods": ["GET"],
                    "bypass_authenticated_requests": true,
                    "key_headers": keys_a,
                    "preferred_encodings": ["zstd"],
                    "max_memory_mb": 32,
                    "default_ttl_secs": 300
                }),
            },
            PluginConfig {
                id: "http_cache_store".into(),
                config: json!({
                    "zone": zone_b,
                    "cacheable_methods": ["GET"],
                    "cacheable_status": [200],
                    "max_payload_bytes": 1024,
                    "default_ttl_secs": 300,
                    "respect_cache_control": true,
                    "bypass_authenticated_requests": true,
                    "key_headers": keys_b,
                    "async_precompression": false,
                    "max_memory_mb": 32
                }),
            },
        ]
    }

    #[test]
    fn test_same_zone_key_headers_parity_enforced() {
        // Order/case differences normalize cleanly and pass.
        let matching = cache_pair(
            "api",
            vec!["X-User-ID", "X-Tenant-ID"],
            "api",
            vec!["x-tenant-id", "x-user-id"],
        );
        assert!(validate_plugin_dependencies(&matching).is_ok());

        // Mismatched key_headers on same zone must fail dependency check.
        let mismatched = cache_pair("api", vec!["x-tenant-id"], "api", vec![]);
        let err = validate_plugin_dependencies(&mismatched).unwrap_err();
        assert!(err.to_string().contains("mismatched key_headers"));

        // Different zones remain independent.
        let distinct_zones = cache_pair("api_a", vec!["x-tenant-id"], "api_b", vec![]);
        assert!(validate_plugin_dependencies(&distinct_zones).is_ok());
    }
}
