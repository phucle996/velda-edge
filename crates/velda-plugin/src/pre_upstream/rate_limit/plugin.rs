//! Pre-Upstream `rate_limit` plugin implementation combining Lock-Free L1 `CoreLane`
//! micro-leasing, 64-shard `CLOCK` `ShardArena`, Hierarchical Multi-Header extraction,
//! Bounded Soft Overdraft, and Shadow Worker Multi-SoT ETP synchronization.
//!
//! ```text
//!   Inbound Request (`on_upstream_request`) [Zero-IO, Zero-Alloc Hot Path]
//!         │
//!         ├─► 1. Check lock-free `sot_health` flag (`SOT_HEALTH_FAIL_OPEN` / `FAIL_CLOSED`)
//!         ├─► 2. Extract stack `KeyDigestSet` (`1` key or up to `4` hierarchical tiers)
//!         ├─► 3. Evaluate each tier against `CoreLane` -> `ShardArena` (`CLOCK` eviction)
//!         │     ├─ Hard capacity `<= burst_capacity`   ──► Continue + remaining headers
//!         │     ├─ Soft grace `<= +overdraft_percent%`  ──► Continue + remaining=0
//!         │     └─ Exceeded                            ──► Reject(429) + bypass headers
//!         │
//!         ▼ Shadow Worker (`pbww > 0`, `etp_id = Some(...)`)
//!     attach_etp(&EtpInstance) ──► RateLimitSotProvider (9 SoT ETPs)
//!     run_background(shutdown) ──► drain dirty deltas -> atomic SoT upsert -> reconcile
//! ```

use std::sync::{Arc, OnceLock};

use http::header::HeaderValue;
use velda_core::{Action, HeaderKey, L7Request, RejectReason};
use velda_etp::EtpInstance;

use crate::BackgroundTaskFuture;
use crate::capability::{AccessLevel, Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;
use crate::pre_upstream::rate_limit::arena::{
    RateLimitArena, SOT_HEALTH_FAIL_CLOSED, SOT_HEALTH_FAIL_OPEN,
};
use crate::pre_upstream::rate_limit::key::CompiledKeyExtractor;
use crate::pre_upstream::rate_limit::providers::{
    RateLimitSotProvider, from_etp_instance, run_sync_loop,
};
use crate::pre_upstream::rate_limit::validation::{
    RateLimitAlgorithm, RateLimitConfig, validate_rate_limit_config,
};

const HDR_LIMIT: HeaderKey = HeaderKey::from_static("x-ratelimit-limit");
const HDR_REMAINING: HeaderKey = HeaderKey::from_static("x-ratelimit-remaining");
const HDR_RETRY_AFTER: HeaderKey = HeaderKey::from_static("retry-after");

/// High-throughput hierarchical Pre-Upstream rate limiter with pluggable SoT storage ETPs.
pub struct RateLimitPlugin {
    pub id: String,
    pub config: RateLimitConfig,
    extractor: CompiledKeyExtractor,
    arena: Arc<RateLimitArena>,
    sot_provider: OnceLock<RateLimitSotProvider>,
    limit_header_val: HeaderValue,
}

impl std::fmt::Debug for RateLimitPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitPlugin")
            .field("id", &self.id)
            .field("etp_id", &self.config.etp_id)
            .field("rate_per_second", &self.config.rate_per_second)
            .field("burst_capacity", &self.config.burst_capacity)
            .field("overdraft_percent", &self.config.overdraft_percent)
            .field("key_strategy", &self.config.key_strategy)
            .finish()
    }
}

impl RateLimitPlugin {
    /// Constructs a local node-only fixed-quota rate limiter (`overdraft_percent = 0`).
    pub fn new(id: impl Into<String>, requests_per_sec: u64) -> Self {
        let rate = requests_per_sec.max(1);
        let config = RateLimitConfig {
            algorithm: RateLimitAlgorithm::TokenBucket,
            rate_per_second: rate,
            burst_capacity: rate,
            overdraft_percent: 0,
            pbww: 0,
            ..RateLimitConfig::default()
        };
        Self::with_config(id, config)
    }

    /// Constructs a [`RateLimitPlugin`] from an already-validated [`RateLimitConfig`].
    pub fn with_config(id: impl Into<String>, mut config: RateLimitConfig) -> Self {
        if config.etp_id.is_none() {
            config.pbww = 0;
        }
        let extractor = CompiledKeyExtractor::new(config.key_strategy, &config.key_headers);
        let arena = Arc::new(RateLimitArena::new(&config));
        let limit_header_val = HeaderValue::from_str(&config.burst_capacity.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("1000"));
        Self {
            id: id.into(),
            config,
            extractor,
            arena,
            sot_provider: OnceLock::new(),
            limit_header_val,
        }
    }

    /// Parses and compiles a [`RateLimitPlugin`] from declarative [`PluginConfig`].
    pub fn from_config(cfg: &PluginConfig) -> Result<Self, PluginError> {
        let parsed = validate_rate_limit_config(&cfg.config)?;
        Ok(Self::with_config(cfg.id.clone(), parsed))
    }

    /// Exposes the underlying bounded arena for diagnostics or testing.
    #[inline]
    pub fn arena(&self) -> &Arc<RateLimitArena> {
        &self.arena
    }
}

impl PreUpstreamPlugin for RateLimitPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _targets: &mut u32) -> Action {
        match self.arena.sot_health() {
            SOT_HEALTH_FAIL_OPEN => return Action::Continue,
            SOT_HEALTH_FAIL_CLOSED => {
                req.headers
                    .insert_bypass(HDR_RETRY_AFTER.name, HeaderValue::from_static("1"));
                req.headers
                    .insert_bypass(HDR_REMAINING.name, HeaderValue::from_static("0"));
                req.headers
                    .insert_bypass(HDR_LIMIT.name, self.limit_header_val.clone());
                return Action::Reject(RejectReason::TooManyRequests);
            }
            _ => {}
        }

        let digests = self.extractor.extract(req);
        let now_ms = self.arena.now_ms_from_ingress(req.ingress_time_nanos);
        let mut min_remaining = self.config.burst_capacity;

        for item in digests.as_slice() {
            let dec = self.arena.evaluate_key(item.hash, item.tier, now_ms);
            if !dec.allowed {
                req.headers
                    .insert_bypass_u64(HDR_RETRY_AFTER.name, dec.retry_after_secs.max(1));
                req.headers
                    .insert_bypass(HDR_REMAINING.name, HeaderValue::from_static("0"));
                req.headers
                    .insert_bypass(HDR_LIMIT.name, self.limit_header_val.clone());
                return Action::Reject(RejectReason::TooManyRequests);
            }
            if dec.remaining < min_remaining {
                min_remaining = dec.remaining;
            }
        }

        req.headers
            .insert_prehashed(&HDR_LIMIT, self.limit_header_val.clone());
        req.headers.insert_prehashed(
            &HDR_REMAINING,
            velda_core::buffer::header::resolve_u64_header_value(min_remaining),
        );
        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }

    #[inline(always)]
    fn direction(&self) -> Direction {
        Direction::Request
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        self.config.pbww
    }

    #[inline]
    fn etp_id(&self) -> Option<&str> {
        self.config.etp_id.as_deref()
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        let provider = from_etp_instance(instance)?;
        let _ = self.sot_provider.set(provider);
        Ok(())
    }

    fn run_background(
        self: Arc<Self>,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Option<BackgroundTaskFuture> {
        let provider = self.sot_provider.get()?.clone();
        let arena = Arc::clone(&self.arena);
        let config = self.config.clone();
        Some(Box::pin(async move {
            run_sync_loop(provider, arena, config, shutdown).await;
            Ok(())
        }))
    }
}

#[cfg(test)]
mod tests {
    use http::header::HeaderName;
    use http::{Method, Uri, Version};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::{Body, HeaderWorkspace};
    use velda_etp::registry::build_instance;
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};

    use super::*;
    use crate::pre_upstream::rate_limit::validation::{FallbackBehavior, KeyStrategy};

    fn make_req(headers: &[(&str, &str)], path: &str) -> L7Request {
        let mut ws = HeaderWorkspace::empty(8);
        for &(k, v) in headers {
            ws.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        L7Request::new(
            Method::GET,
            path.parse::<Uri>().unwrap(),
            Version::HTTP_11,
            ws,
            Body::Empty,
        )
    }

    #[test]
    fn test_rate_limit_from_config_rejects_legacy_and_parses_clean() {
        let legacy = PluginConfig {
            id: "rate_limit".into(),
            config: serde_json::json!({
                "backend": "memory",
                "rate_per_second": 100
            }),
        };
        assert!(RateLimitPlugin::from_config(&legacy).is_err());

        let clean = PluginConfig {
            id: "rate_limit".into(),
            config: serde_json::json!({
                "algorithm": "sliding_window",
                "rate_per_second": 200,
                "burst_capacity": 400,
                "overdraft_percent": 10,
                "key_strategy": "hierarchical",
                "key_headers": ["x-tenant-id", "x-api-key", "client_ip"]
            }),
        };
        let plugin = RateLimitPlugin::from_config(&clean).unwrap();
        assert_eq!(plugin.pbww(), 0);
        assert_eq!(plugin.etp_id(), None);
        assert_eq!(plugin.config.overdraft_percent, 10);
    }

    #[test]
    fn test_hierarchical_multi_header_blocks_rotating_ip_flood() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 3,
            burst_capacity: 3,
            overdraft_percent: 0,
            key_strategy: KeyStrategy::Hierarchical,
            key_headers: vec!["x-tenant-id".into(), "client_ip".into()],
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate_limit", cfg);

        // Tenant "acme" uses 3 rotating IPs -> all 3 succeed against tier0 tenant quota (=3)
        for i in 1..=3 {
            let ip = format!("10.0.0.{i}");
            let mut req = make_req(&[("x-tenant-id", "acme"), ("x-forwarded-for", &ip)], "/api");
            let mut upstream = 0;
            assert!(matches!(
                plugin.on_upstream_request(&mut req, &mut upstream),
                Action::Continue
            ));
        }

        // 4th request from brand new IP 10.0.0.99 still belongs to tenant "acme" -> BLOCKED!
        let mut flood_req = make_req(
            &[("x-tenant-id", "acme"), ("x-forwarded-for", "10.0.0.99")],
            "/api",
        );
        let mut upstream = 0;
        assert!(matches!(
            plugin.on_upstream_request(&mut flood_req, &mut upstream),
            Action::Reject(RejectReason::TooManyRequests)
        ));

        // Different tenant "beta" is completely isolated and unaffected!
        let mut beta_req = make_req(
            &[("x-tenant-id", "beta"), ("x-forwarded-for", "10.0.0.99")],
            "/api",
        );
        assert!(matches!(
            plugin.on_upstream_request(&mut beta_req, &mut upstream),
            Action::Continue
        ));
    }

    #[test]
    fn test_soft_overdraft_absorbs_small_grace_then_blocks_spike() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::TokenBucket,
            rate_per_second: 10,
            burst_capacity: 10,
            overdraft_percent: 20, // grace = +2 => blocks at > 12
            key_strategy: KeyStrategy::ClientIp,
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate_limit", cfg);
        let mut upstream = 0;

        for _ in 0..12 {
            let mut req = make_req(&[("x-forwarded-for", "203.0.113.5")], "/");
            assert!(matches!(
                plugin.on_upstream_request(&mut req, &mut upstream),
                Action::Continue
            ));
        }

        let mut blocked = make_req(&[("x-forwarded-for", "203.0.113.5")], "/");
        assert!(matches!(
            plugin.on_upstream_request(&mut blocked, &mut upstream),
            Action::Reject(RejectReason::TooManyRequests)
        ));
    }

    #[tokio::test]
    async fn test_redis_sot_shadow_sync_reconciles_cluster_totals_and_blocks_peer_overflow() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Mock Redis returning cluster_total = 50 on first INCRBY
        let srv = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        // Pipelined batch has SET ... EX 15 NX + INCRBY pairs; reply "+OK\r\n:50\r\n"
                        let req = String::from_utf8_lossy(&buf[..n]);
                        let pairs = req.matches("INCRBY").count();
                        let mut reply = String::new();
                        for _ in 0..pairs {
                            reply.push_str("+OK\r\n:50\r\n");
                        }
                        if sock.write_all(reply.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });

        let etp_entry = EtpEntryConfig {
            id: "redis-rl".into(),
            kind: EtpKind::Redis,
            security: velda_etp::EtpTransportSecurity::Plain,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({
                "host": "127.0.0.1",
                "port": port
            }),
        };
        let instance = build_instance(&etp_entry).unwrap();

        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            etp_id: Some("redis-rl".into()),
            rate_per_second: 10,
            burst_capacity: 10,
            overdraft_percent: 0,
            key_strategy: KeyStrategy::ClientIp,
            fallback_behavior: FallbackBehavior::Memory,
            sync_interval_ms: 25,
            pbww: 20,
            ..RateLimitConfig::default()
        };
        let plugin = Arc::new(RateLimitPlugin::with_config("rate_limit", cfg));
        plugin.attach_etp(&instance).unwrap();

        // Kafka instance must be rejected cleanly by attach_etp
        let kafka_entry = EtpEntryConfig {
            id: "kafka-bad".into(),
            kind: EtpKind::Kafka,
            security: velda_etp::EtpTransportSecurity::Plain,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings: serde_json::json!({ "brokers": ["127.0.0.1:9092"] }),
        };
        let kafka_inst = build_instance(&kafka_entry).unwrap();
        assert!(plugin.attach_etp(&kafka_inst).is_err());

        let (tx, rx) = tokio::sync::watch::channel(false);
        let bg = Arc::clone(&plugin).run_background(rx).unwrap();
        let bg_handle = tokio::spawn(bg);

        // First local request passes (used=1 <= 10)
        let mut req1 = make_req(&[("x-forwarded-for", "198.51.100.7")], "/");
        let mut upstream = 0;
        assert!(matches!(
            plugin.on_upstream_request(&mut req1, &mut upstream),
            Action::Continue
        ));

        // Wait briefly for Shadow sync tick -> Redis reports cluster total = 50 -> peer_used = 49
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;

        let mut req2 = make_req(&[("x-forwarded-for", "198.51.100.7")], "/");
        assert!(
            matches!(
                plugin.on_upstream_request(&mut req2, &mut upstream),
                Action::Reject(RejectReason::TooManyRequests)
            ),
            "peer cluster_total=50 from Redis SoT must block subsequent local requests"
        );

        let _ = tx.send(true);
        let _ = bg_handle.await;
        srv.abort();
    }

    #[test]
    fn test_ingress_time_nanos_drives_rollover_and_clamps_backward_skew() {
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::FixedWindow,
            rate_per_second: 2,
            burst_capacity: 2,
            overdraft_percent: 0,
            key_strategy: KeyStrategy::Hierarchical,
            key_headers: vec!["x-tenant-id".into(), "x-api-key".into()],
            ..RateLimitConfig::default()
        };
        let plugin = RateLimitPlugin::with_config("rate_limit", cfg);
        let mut upstream = 0;
        let t0_ns = 1_750_000_000_000_000_000u64;

        for _ in 0..2 {
            let mut req = make_req(&[("x-tenant-id", "t1"), ("x-api-key", "k1")], "/");
            req.ingress_time_nanos = t0_ns;
            assert!(matches!(
                plugin.on_upstream_request(&mut req, &mut upstream),
                Action::Continue
            ));
        }

        // Same ingress epoch exhausted -> 429
        let mut blocked = make_req(&[("x-tenant-id", "t1"), ("x-api-key", "k1")], "/");
        blocked.ingress_time_nanos = t0_ns;
        assert!(matches!(
            plugin.on_upstream_request(&mut blocked, &mut upstream),
            Action::Reject(RejectReason::TooManyRequests)
        ));

        // Backward skewed ingress timestamp (-5s) must NOT reset bucket backwards
        let mut skewed = make_req(&[("x-tenant-id", "t1"), ("x-api-key", "k1")], "/");
        skewed.ingress_time_nanos = t0_ns - 5_000_000_000;
        assert!(matches!(
            plugin.on_upstream_request(&mut skewed, &mut upstream),
            Action::Reject(RejectReason::TooManyRequests)
        ));

        // Advancing ingress_time_nanos by +1s rolls into fresh epoch cleanly without sleeping
        let mut next_epoch = make_req(&[("x-tenant-id", "t1"), ("x-api-key", "k1")], "/");
        next_epoch.ingress_time_nanos = t0_ns + 1_000_000_000;
        assert!(matches!(
            plugin.on_upstream_request(&mut next_epoch, &mut upstream),
            Action::Continue
        ));
    }
}
