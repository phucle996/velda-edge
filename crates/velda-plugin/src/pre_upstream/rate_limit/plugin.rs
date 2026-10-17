//! Rate Limit Plugin (`rate_limit`).
//!
//! Evaluated during `pre_upstream` phase immediately before leasing backend connections.
//! Provides lock-striped in-memory token bucket / sliding window rate limiting (~15ns hot path)
//! with optional asynchronous Redis cluster batching.
//!
//! Full schema specification, allowed values, and configuration examples:
//! See [`README.md`](./README.md).

use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::IpAddr;

use http::header::RETRY_AFTER;
use http::{HeaderName, HeaderValue, StatusCode};
use velda_core::{Action, L7Request, L7Response};

use super::algorithm::{RateLimitAlgorithm, ShardedState};
use super::redis::{RedisConfig, RedisConnectionPool, TlsSource};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

/// Storage and synchronization backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RateLimitBackend {
    /// Local in-memory lock-striped concurrent state (Zero Network I/O, microsecond latency).
    #[default]
    Memory,
    /// Distributed Redis synchronization with background auxiliary batching.
    Redis,
}

impl RateLimitBackend {
    /// Parses a backend identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "redis" => Self::Redis,
            _ => Self::Memory,
        }
    }
}

/// Key resolution strategy for client identification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyStrategy {
    /// Evaluates `key_headers` in priority order; first non-empty header/IP is selected (Default).
    #[default]
    Priority,
    /// Concatenates all specified headers and client IP into a multi-dimensional compound key.
    Composite,
}

impl KeyStrategy {
    /// Parses a key strategy identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "composite" | "combine" => Self::Composite,
            _ => Self::Priority,
        }
    }
}

/// Fallback behavior when Redis is unreachable or times out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FallbackBehavior {
    /// Seamlessly fallback to local in-memory rate limiting with current quotas (Default).
    #[default]
    Memory,
    /// Fail-open: allow request through without rate limiting.
    Allow,
    /// Fail-closed: reject request immediately with 429 Too Many Requests.
    Deny,
}

impl FallbackBehavior {
    /// Parses fallback behavior identifier from configuration string.
    pub fn parse(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "allow" | "fail_open" => Self::Allow,
            "deny" | "fail_closed" => Self::Deny,
            _ => Self::Memory,
        }
    }
}

/// Complete configuration schema for [`RateLimitPlugin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitConfig {
    pub algorithm: RateLimitAlgorithm,
    pub backend: RateLimitBackend,
    pub rate_per_second: u64,
    pub burst_capacity: u64,
    pub key_strategy: KeyStrategy,
    pub key_headers: Vec<String>,
    pub fallback_behavior: FallbackBehavior,
    pub redis: RedisConfig,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            algorithm: RateLimitAlgorithm::TokenBucket,
            backend: RateLimitBackend::Memory,
            rate_per_second: 1000,
            burst_capacity: 1000,
            key_strategy: KeyStrategy::Priority,
            key_headers: vec!["client_ip".to_string()],
            fallback_behavior: FallbackBehavior::Memory,
            redis: RedisConfig::default(),
        }
    }
}

/// Production implementation of the Rate Limiter Pre-Upstream plugin.
pub struct RateLimitPlugin {
    pub id: String,
    pub config: RateLimitConfig,
    state: ShardedState,
    pub redis_pool: RedisConnectionPool,
}

impl std::fmt::Debug for RateLimitPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitPlugin")
            .field("id", &self.id)
            .field("config", &self.config)
            .field("redis_pool", &self.redis_pool)
            .finish()
    }
}

impl RateLimitPlugin {
    /// Constructs a basic rate limiter instance with default Token Bucket memory backend.
    pub fn new(id: impl Into<String>, rate_per_second: u64) -> Self {
        let config = RateLimitConfig {
            rate_per_second,
            burst_capacity: rate_per_second,
            ..RateLimitConfig::default()
        };
        Self::with_config(id, config)
    }

    /// Constructs an instance with custom configuration.
    pub fn with_config(id: impl Into<String>, config: RateLimitConfig) -> Self {
        let redis_pool = RedisConnectionPool::new(config.redis.clone());

        // Spawn auxiliary synchronization worker if configured for Redis backend
        if config.backend == RateLimitBackend::Redis {
            redis_pool.start_auxiliary_worker();
        }

        Self {
            id: id.into(),
            config,
            state: ShardedState::new(),
            redis_pool,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    ///
    /// # Strict Sentinel standard
    /// Unused or omitted parameters MUST be set to `"none"`.
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, PluginError> {
        let json = &config.config;

        // 1. Algorithm
        let algorithm = json
            .get("algorithm")
            .and_then(|v| v.as_str())
            .map(RateLimitAlgorithm::parse)
            .unwrap_or(RateLimitAlgorithm::TokenBucket);

        // 2. Backend
        let backend = json
            .get("backend")
            .and_then(|v| v.as_str())
            .map(RateLimitBackend::parse)
            .unwrap_or(RateLimitBackend::Memory);

        // 3. Rate and Burst (supports both new and legacy property names)
        let rate_per_second = json
            .get("rate_per_second")
            .or_else(|| json.get("limit_per_second"))
            .or_else(|| json.get("refill_rate_per_second"))
            .and_then(|v| v.as_u64())
            .unwrap_or(1000);

        let burst_capacity = json
            .get("burst_capacity")
            .or_else(|| json.get("capacity"))
            .and_then(|v| v.as_u64())
            .unwrap_or(rate_per_second);

        // 4. Key Strategy
        let key_strategy = json
            .get("key_strategy")
            .and_then(|v| v.as_str())
            .map(KeyStrategy::parse)
            .unwrap_or(KeyStrategy::Priority);

        // 5. Key Headers (supports array or legacy single string)
        let mut key_headers = Vec::new();
        if let Some(arr) = json.get("key_headers").and_then(|v| v.as_array()) {
            for item in arr {
                if let Some(s) = item.as_str() {
                    let trimmed = s.trim();
                    if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("none") {
                        key_headers.push(trimmed.to_string());
                    }
                }
            }
        } else if let Some(s) = json
            .get("key_headers")
            .or_else(|| json.get("key"))
            .and_then(|v| v.as_str())
        {
            let trimmed = s.trim();
            if !trimmed.is_empty() && !trimmed.eq_ignore_ascii_case("none") {
                key_headers.push(trimmed.to_string());
            }
        }

        if key_headers.is_empty() {
            key_headers.push("client_ip".to_string());
        }

        // 6. Fallback Behavior
        let fallback_behavior = json
            .get("fallback_behavior")
            .and_then(|v| v.as_str())
            .map(FallbackBehavior::parse)
            .unwrap_or(FallbackBehavior::Memory);

        // 7. Batching Configuration
        let batch_sync_interval_ms = json
            .get("batch_sync_interval_ms")
            .and_then(|v| {
                if let Some(num) = v.as_u64() {
                    Some(num)
                } else if let Some(s) = v.as_str() {
                    s.parse::<u64>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(50);

        let batch_max_size = json
            .get("batch_max_size")
            .and_then(|v| {
                if let Some(num) = v.as_u64() {
                    Some(num as usize)
                } else if let Some(s) = v.as_str() {
                    s.parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(1000);

        // 8. Redis Configuration
        let redis_url = json
            .get("redis_url")
            .and_then(|v| v.as_str())
            .and_then(|s| {
                let t = s.trim();
                if t.is_empty() || t.eq_ignore_ascii_case("none") {
                    None
                } else {
                    Some(t.to_string())
                }
            });

        let redis_username = json
            .get("redis_username")
            .and_then(|v| v.as_str())
            .and_then(|s| {
                let t = s.trim();
                if t.is_empty() || t.eq_ignore_ascii_case("none") {
                    None
                } else {
                    Some(t.to_string())
                }
            });

        let redis_password = json
            .get("redis_password")
            .and_then(|v| v.as_str())
            .and_then(|s| {
                let t = s.trim();
                if t.is_empty() || t.eq_ignore_ascii_case("none") {
                    None
                } else {
                    Some(t.to_string())
                }
            });

        let redis_tls_ca = json
            .get("redis_tls_ca")
            .and_then(|v| v.as_str())
            .map(TlsSource::parse)
            .unwrap_or(TlsSource::None);

        let redis_tls_cert = json
            .get("redis_tls_cert")
            .and_then(|v| v.as_str())
            .map(TlsSource::parse)
            .unwrap_or(TlsSource::None);

        let redis_tls_key = json
            .get("redis_tls_key")
            .and_then(|v| v.as_str())
            .map(TlsSource::parse)
            .unwrap_or(TlsSource::None);

        let redis_timeout_ms = json.get("redis_timeout_ms").and_then(|v| {
            if let Some(num) = v.as_u64() {
                Some(num)
            } else if let Some(s) = v.as_str() {
                s.parse::<u64>().ok()
            } else {
                None
            }
        });

        let redis_cfg = RedisConfig {
            url: redis_url,
            username: redis_username,
            password: redis_password,
            tls_ca: redis_tls_ca,
            tls_cert: redis_tls_cert,
            tls_key: redis_tls_key,
            timeout_ms: redis_timeout_ms,
            batch_sync_interval_ms,
            batch_max_size,
        };

        let parsed_cfg = RateLimitConfig {
            algorithm,
            backend,
            rate_per_second,
            burst_capacity,
            key_strategy,
            key_headers,
            fallback_behavior,
            redis: redis_cfg,
        };

        Ok(Self::with_config(&config.id, parsed_cfg))
    }

    /// Resolves client IP address from standard forwarding headers.
    #[inline]
    fn extract_client_ip(req: &L7Request) -> Option<IpAddr> {
        if let Some(val) = req.headers.get("x-forwarded-for")
            && let Ok(raw) = val.to_str()
        {
            let first = raw.split(',').next().unwrap_or("").trim();
            if let Ok(ip) = first.parse::<IpAddr>() {
                return Some(ip);
            }
        }

        req.headers
            .get("x-real-ip")
            .and_then(|val| val.to_str().ok())
            .and_then(|raw| raw.trim().parse::<IpAddr>().ok())
    }

    /// Resolves rate-limiting client identifier key based on [`KeyStrategy`].
    #[inline]
    fn resolve_key(&self, req: &L7Request) -> String {
        match self.config.key_strategy {
            KeyStrategy::Priority => {
                for hdr_name in &self.config.key_headers {
                    if hdr_name == "client_ip" || hdr_name == "ip" {
                        if let Some(ip) = Self::extract_client_ip(req) {
                            return format!("ip:{ip}");
                        }
                    } else if let Some(val) = req.headers.get(hdr_name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            return format!("{hdr_name}:{trimmed}");
                        }
                    }
                }
                // Fallback if none matched
                if let Some(ip) = Self::extract_client_ip(req) {
                    format!("ip:{ip}")
                } else {
                    "global:default".to_string()
                }
            }
            KeyStrategy::Composite => {
                let mut parts = Vec::with_capacity(self.config.key_headers.len());
                for hdr_name in &self.config.key_headers {
                    if hdr_name == "client_ip" || hdr_name == "ip" {
                        if let Some(ip) = Self::extract_client_ip(req) {
                            parts.push(ip.to_string());
                        } else {
                            parts.push("unknown".to_string());
                        }
                    } else if let Some(val) = req.headers.get(hdr_name)
                        && let Ok(s) = val.to_str()
                    {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            parts.push(trimmed.to_string());
                        } else {
                            parts.push("none".to_string());
                        }
                    } else {
                        parts.push("none".to_string());
                    }
                }
                parts.join(":")
            }
        }
    }

    /// Hashes the key string into a 64-bit integer for zero-alloc sharded table lookup.
    #[inline(always)]
    fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

impl PreUpstreamPlugin for RateLimitPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    /// Evaluates rate limiting on hot path prior to upstream dispatch.
    ///
    /// # Performance Contract
    /// Executes 100% in-memory with lock-striped sharding. Zero Network I/O.
    fn on_upstream_request(&self, req: &mut L7Request) -> Action {
        let key = self.resolve_key(req);
        let key_hash = Self::hash_key(&key);

        // Check backend execution mode
        let (allowed, retry_after) = match self.config.backend {
            RateLimitBackend::Memory => self.state.evaluate(
                key_hash,
                self.config.algorithm,
                self.config.rate_per_second,
                self.config.burst_capacity,
            ),
            RateLimitBackend::Redis => {
                if !self.redis_pool.is_connected() {
                    // Redis connection dropped: invoke configured fallback behavior
                    match self.config.fallback_behavior {
                        FallbackBehavior::Memory => self.state.evaluate(
                            key_hash,
                            self.config.algorithm,
                            self.config.rate_per_second,
                            self.config.burst_capacity,
                        ),
                        FallbackBehavior::Allow => (true, 0),
                        FallbackBehavior::Deny => (false, 1),
                    }
                } else {
                    // Record request in Redis batch accumulator for auxiliary synchronization
                    self.redis_pool.record_hit(key_hash);

                    // Local high-speed enforcement against cluster quota
                    self.state.evaluate(
                        key_hash,
                        self.config.algorithm,
                        self.config.rate_per_second,
                        self.config.burst_capacity,
                    )
                }
            }
        };

        if allowed {
            Action::Continue
        } else {
            let retry_str = retry_after.to_string();
            let limit_str = self.config.rate_per_second.to_string();

            Action::Respond(
                L7Response::from_bytes(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Too Many Requests: rate limit exceeded on upstream target",
                )
                .with_header(
                    RETRY_AFTER,
                    HeaderValue::from_str(&retry_str)
                        .unwrap_or_else(|_| HeaderValue::from_static("1")),
                )
                .with_header(
                    HeaderName::from_static("x-ratelimit-limit"),
                    HeaderValue::from_str(&limit_str)
                        .unwrap_or_else(|_| HeaderValue::from_static("0")),
                )
                .with_header(
                    HeaderName::from_static("x-ratelimit-remaining"),
                    HeaderValue::from_static("0"),
                )
                .with_header(
                    HeaderName::from_static("x-ratelimit-reset"),
                    HeaderValue::from_str(&retry_str)
                        .unwrap_or_else(|_| HeaderValue::from_static("1")),
                ),
            )
        }
    }

    #[inline]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    fn make_test_request(headers: HeaderMap) -> L7Request {
        L7Request::new(
            Method::GET,
            Uri::from_static("/v1/resource"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        )
    }

    #[test]
    fn test_token_bucket_rate_exhaustion() {
        // 2 requests per second with burst capacity of 2
        let plugin = RateLimitPlugin::new("rate-tb", 2);

        let mut req = make_test_request(HeaderMap::new());

        // 1st request -> PASS
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
        // 2nd request -> PASS (exhausted burst)
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
        // 3rd request -> 429 TOO_MANY_REQUESTS
        match plugin.on_upstream_request(&mut req) {
            Action::Respond(resp) => {
                assert_eq!(resp.status, StatusCode::TOO_MANY_REQUESTS);
                assert_eq!(resp.headers.get("x-ratelimit-remaining").unwrap(), "0");
                assert!(resp.headers.contains_key("retry-after"));
            }
            _ => panic!("Expected Action::Respond(429)"),
        }
    }

    #[test]
    fn test_fixed_window_algorithm() {
        let config = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 1,
            burst_capacity: 1,
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate-fw", config);
        let mut req = make_test_request(HeaderMap::new());

        // 1st request -> PASS
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
        // 2nd request in same window -> REJECT 429
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Respond(_)
        ));
    }

    #[test]
    fn test_key_strategy_priority_resolution() {
        let config = RateLimitConfig {
            key_strategy: KeyStrategy::Priority,
            key_headers: vec![
                "x-device-id".to_string(),
                "x-api-key".to_string(),
                "client_ip".to_string(),
            ],
            rate_per_second: 1,
            burst_capacity: 1,
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate-prio", config);

        // Request 1: has x-device-id
        let mut h1 = HeaderMap::new();
        h1.insert("x-device-id", HeaderValue::from_static("device-alpha"));
        let req1 = make_test_request(h1);
        assert_eq!(plugin.resolve_key(&req1), "x-device-id:device-alpha");

        // Request 2: no device-id, has x-api-key
        let mut h2 = HeaderMap::new();
        h2.insert("x-api-key", HeaderValue::from_static("secret-key-1"));
        let req2 = make_test_request(h2);
        assert_eq!(plugin.resolve_key(&req2), "x-api-key:secret-key-1");

        // Request 3: no headers, has x-forwarded-for
        let mut h3 = HeaderMap::new();
        h3.insert("x-forwarded-for", HeaderValue::from_static("192.168.1.100"));
        let req3 = make_test_request(h3);
        assert_eq!(plugin.resolve_key(&req3), "ip:192.168.1.100");
    }

    #[test]
    fn test_key_strategy_composite_resolution() {
        let config = RateLimitConfig {
            key_strategy: KeyStrategy::Composite,
            key_headers: vec![
                "client_ip".to_string(),
                "x-device-id".to_string(),
                "x-api-key".to_string(),
            ],
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate-comp", config);

        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static("10.0.0.1"));
        headers.insert("x-device-id", HeaderValue::from_static("phone-42"));
        // x-api-key is missing

        let req = make_test_request(headers);
        let key = plugin.resolve_key(&req);
        assert_eq!(key, "10.0.0.1:phone-42:none");
    }

    #[test]
    fn test_redis_fallback_to_memory_when_disconnected() {
        let config = RateLimitConfig {
            backend: RateLimitBackend::Redis,
            fallback_behavior: FallbackBehavior::Memory,
            rate_per_second: 1,
            burst_capacity: 1,
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate-redis", config);

        // Simulate Redis disconnection
        plugin.redis_pool.set_connected(false);

        let mut req = make_test_request(HeaderMap::new());
        // 1st request -> PASS via memory fallback
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
        // 2nd request -> REJECT 429 via memory fallback
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Respond(_)
        ));
    }

    #[test]
    fn test_redis_fail_open_allow() {
        let config = RateLimitConfig {
            backend: RateLimitBackend::Redis,
            fallback_behavior: FallbackBehavior::Allow,
            rate_per_second: 1,
            burst_capacity: 1,
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate-redis-allow", config);

        // Disconnect redis
        plugin.redis_pool.set_connected(false);

        let mut req = make_test_request(HeaderMap::new());
        // Should continuously pass through (fail-open)
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
        assert!(matches!(
            plugin.on_upstream_request(&mut req),
            Action::Continue
        ));
    }

    #[test]
    fn test_strict_none_sentinel_deserialization() {
        let json_cfg = serde_json::json!({
            "algorithm": "token_bucket",
            "backend": "memory",
            "rate_per_second": 500,
            "burst_capacity": 1000,
            "key_strategy": "priority",
            "key_headers": ["x-device-id", "none", "client_ip"],
            "fallback_behavior": "memory",
            "batch_sync_interval_ms": 50,
            "batch_max_size": 1000,
            "redis_url": "none",
            "redis_username": "none",
            "redis_password": "none",
            "redis_tls_ca": "none",
            "redis_tls_cert": "none",
            "redis_tls_key": "none",
            "redis_timeout_ms": "none"
        });

        let sync_cfg = velda_sync::post_sync::plugin::PluginConfig {
            id: "full-config-test".into(),
            phase: "pre_upstream".into(),
            priority: Some(100),
            config: json_cfg,
        };

        let plugin = RateLimitPlugin::from_config(&sync_cfg).expect("Should parse successfully");
        assert_eq!(plugin.config.algorithm, RateLimitAlgorithm::TokenBucket);
        assert_eq!(plugin.config.backend, RateLimitBackend::Memory);
        assert_eq!(plugin.config.rate_per_second, 500);
        assert_eq!(plugin.config.burst_capacity, 1000);
        assert_eq!(plugin.config.key_headers, vec!["x-device-id", "client_ip"]);
        assert!(plugin.config.redis.url.is_none());
        assert!(plugin.config.redis.username.is_none());
        assert!(plugin.config.redis.password.is_none());
        assert!(plugin.config.redis.tls_ca.is_none());
        assert!(plugin.config.redis.timeout_ms.is_none());
    }
}
