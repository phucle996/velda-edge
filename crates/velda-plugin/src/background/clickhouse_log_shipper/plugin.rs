//! ClickHouse Log Shipper Background Coordinator (`clickhouse_log_shipper::plugin`).
//!
//! ```text
//!   Startup wiring (wire_plugin_etps / with_etp) attaches Arc<ClickHouseEtp> into OnceLock
//!         │
//!         ▼
//!   run(shutdown) ──► pre-bake INSERT prefix + DDL request bytes once at startup
//!         │
//!         ├─► Iteration 0: ensure_table via ClickHouseEtp::acquire() -> ClickHouseLease
//!         └─► Interval / shutdown select loop:
//!               ├─ in 30s cooldown? ──► drain & discard batches (RAM guard)
//!               └─ drain_batches(64) -> format RowBinary/JSONEachRow into payload_buf
//!                     └─► copy prebaked_insert_prefix + fast_write_u64(len) + "\r\n\r\n"
//!                           └─► etp.acquire() -> write head + payload + flush
//!                                 └─► httparse::Response + drain body
//!                                       ├─ I/O err (try 0) ─► lease.reconnect() + retry
//!                                       ├─ >= 500 / timeout ─► lease.recreate_in_background()
//!                                       └─ 2xx               ─► ++shipped_records
//!
//!   Coordinator State Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ ClickHouseLogShipperPlugin                                                 │
//!   │   id: String | config: ClickHouseLogShipperConfig (etp_id)                 │
//!   │   etp: OnceLock<Arc<ClickHouseEtp>> | telemetry_store: Arc<TelemetryStore> │
//!   │   shipped_records: Arc<AtomicU64> | failed_pushes: Arc<AtomicU64>          │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use velda_etp::clickhouse::ClickHouseEtp;
use velda_etp::{EtpBackend, EtpError, EtpInstance};
use velda_observability::{TelemetryStore, global_telemetry_store};

use crate::background::BackgroundPlugin;
use crate::capability::{Direction, PluginCapability, SupportedProtocols};
use crate::config::PluginConfig;
use crate::error::PluginError;

use super::config::{ClickHouseLogShipperConfig, VELDA_ACCESS_LOGS_TABLE};
use super::format::ClickHouseFormat;

/// Background ClickHouse Log Shipper Plugin.
pub struct ClickHouseLogShipperPlugin {
    pub id: String,
    pub config: ClickHouseLogShipperConfig,
    pub etp: OnceLock<Arc<ClickHouseEtp>>,
    pub telemetry_store: Arc<TelemetryStore>,
    pub shipped_records: Arc<AtomicU64>,
    pub failed_pushes: Arc<AtomicU64>,
}

impl ClickHouseLogShipperPlugin {
    /// Constructs a new shipper bound to the global singleton [`TelemetryStore`].
    pub fn new(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
    ) -> Result<Self, PluginError> {
        Self::with_store(id, config, Arc::clone(global_telemetry_store()))
    }

    /// Constructs a new shipper bound to an explicit [`TelemetryStore`] without an attached ETP yet.
    pub fn with_store(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        Ok(Self {
            id: id.into(),
            config,
            etp: OnceLock::new(),
            telemetry_store,
            shipped_records: Arc::new(AtomicU64::new(0)),
            failed_pushes: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Constructs a shipper pre-bound to an explicit [`ClickHouseEtp`] and [`TelemetryStore`].
    pub fn with_etp(
        id: impl Into<String>,
        config: ClickHouseLogShipperConfig,
        etp: Arc<ClickHouseEtp>,
        telemetry_store: Arc<TelemetryStore>,
    ) -> Result<Self, PluginError> {
        let plugin = Self::with_store(id, config, telemetry_store)?;
        let _ = plugin.etp.set(etp);
        Ok(plugin)
    }

    /// Instantiates and validates this plugin from declarative [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let validated =
            super::validation::validate_clickhouse_log_shipper_config(&config.id, &config.config)
                .map_err(|e| PluginError::Config(e.to_string()))?;

        Self::with_store(&config.id, validated, Arc::clone(global_telemetry_store()))
    }
}

impl BackgroundPlugin for ClickHouseLogShipperPlugin {
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        75
    }

    fn direction(&self) -> Direction {
        Direction::Background
    }

    fn capability(&self) -> PluginCapability {
        PluginCapability::default()
    }

    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::empty()
    }

    #[inline(always)]
    fn etp_id(&self) -> Option<&str> {
        Some(&self.config.etp_id)
    }

    fn attach_etp(&self, instance: &EtpInstance) -> Result<(), PluginError> {
        match &instance.backend {
            EtpBackend::ClickHouse(pool) => {
                let _ = self.etp.set(Arc::clone(pool));
                Ok(())
            }
            _ => Err(PluginError::Config(format!(
                "ClickHouse plugin '{}' requires a 'clickhouse' ETP for etp_id '{}', got '{}'",
                self.id, self.config.etp_id, instance.kind
            ))),
        }
    }

    fn run(
        self: Arc<Self>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), PluginError>> + Send>> {
        Box::pin(async move {
            let etp = self.etp.get().ok_or_else(|| {
                PluginError::Config(format!(
                    "ClickHouse plugin '{}' has no attached ClickHouseEtp for etp_id '{}'",
                    self.id, self.config.etp_id
                ))
            })?;

            let database = if self.config.database.trim().is_empty() {
                etp.config.database.as_str()
            } else {
                self.config.database.trim()
            };
            if database.is_empty() {
                return Err(PluginError::Config(
                    "ClickHouse target database resolved to empty string".to_string(),
                ));
            }

            let format = self.config.parsed_format();
            let request_timeout = Duration::from_millis(self.config.request_timeout_ms);

            // Pre-bake shared auth headers and full HTTP/1.1 prefixes once at startup
            let mut auth_headers = String::new();
            if let Some(user) = &etp.config.user {
                auth_headers.push_str("X-ClickHouse-User: ");
                auth_headers.push_str(user);
                auth_headers.push_str("\r\n");
            }
            if let Some(password) = &etp.config.password {
                auth_headers.push_str("X-ClickHouse-Key: ");
                auth_headers.push_str(password);
                auth_headers.push_str("\r\n");
            }

            let prebaked_insert_prefix = format!(
                "POST {}/?query=INSERT%20INTO%20{}.{}%20FORMAT%20{} HTTP/1.1\r\nHost: {}\r\nContent-Type: {}\r\nConnection: keep-alive\r\n{}Content-Length: ",
                etp.config.base_path,
                database,
                VELDA_ACCESS_LOGS_TABLE,
                format.as_clickhouse_format_str(),
                etp.config.host_header,
                format.content_type(),
                auth_headers,
            )
            .into_bytes();

            let ddl_body = format!(
                "CREATE TABLE IF NOT EXISTS {database}.{VELDA_ACCESS_LOGS_TABLE} (\
                    timestamp DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),\
                    request_id UInt64 CODEC(DoubleDelta, ZSTD(1)),\
                    duration_ns UInt64 CODEC(T64, ZSTD(1)),\
                    peer_ip String CODEC(ZSTD(1)),\
                    peer_port UInt16 CODEC(T64, ZSTD(1)),\
                    upstream_ip String CODEC(ZSTD(1)),\
                    upstream_port UInt16 CODEC(T64, ZSTD(1)),\
                    route_id UInt16 CODEC(T64, ZSTD(1)),\
                    status_code UInt16 CODEC(T64, ZSTD(1)),\
                    pipe LowCardinality(String) CODEC(ZSTD(1)),\
                    method LowCardinality(String) CODEC(ZSTD(1)),\
                    url String CODEC(ZSTD(3)),\
                    bytes_rx UInt32 CODEC(T64, ZSTD(1)),\
                    bytes_tx UInt32 CODEC(T64, ZSTD(1)),\
                    completion_reason LowCardinality(String) CODEC(ZSTD(1)),\
                    grpc_status LowCardinality(String) CODEC(ZSTD(1)),\
                    user_agent_family LowCardinality(String) CODEC(ZSTD(1)),\
                    user_agent_os LowCardinality(String) CODEC(ZSTD(1))\
                ) ENGINE = MergeTree() \
                PARTITION BY toYYYYMM(timestamp) \
                ORDER BY (route_id, status_code, timestamp) \
                SETTINGS index_granularity = 8192"
            );
            let mut ddl_request = format!(
                "POST {}/ HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: keep-alive\r\n{}Content-Length: ",
                etp.config.base_path,
                etp.config.host_header,
                auth_headers,
            )
            .into_bytes();
            velda_core::lut::decimal::fast_write_u64(&mut ddl_request, ddl_body.len() as u64);
            ddl_request.extend_from_slice(b"\r\n\r\n");
            ddl_request.extend_from_slice(ddl_body.as_bytes());

            let subscriber = self.telemetry_store.subscribe(256);
            let mut interval = tokio::time::interval(Duration::from_millis(
                self.config.flush_interval_ms.max(100),
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut head_buf = Vec::with_capacity(prebaked_insert_prefix.len() + 32);
            let mut payload_buf = Vec::with_capacity(64 * 1024);
            let mut resp_buf = Vec::with_capacity(1024);
            let mut ensured_table = false;
            let mut consecutive_failures: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;

            loop {
                let (is_ddl, is_shutting_down, encoded_recs) = if !ensured_table {
                    ensured_table = true;
                    (true, false, 0usize)
                } else {
                    let shutting_down = tokio::select! {
                        _ = interval.tick() => false,
                        _ = shutdown.changed() => true,
                    };

                    // 1. Cool-down state: discard incoming batches to prevent RAM buildup
                    if cooldown_until.is_some_and(|until| tokio::time::Instant::now() < until) {
                        let _ = subscriber.drain_batches(64);
                        tracing::debug!(
                            plugin_id = %self.id,
                            "ClickHouse log shipper in 30s cool-down; discarded batches to protect memory"
                        );
                        if shutting_down {
                            tracing::info!(plugin_id = %self.id, "ClickHouse log shipper shutting down");
                            break;
                        }
                        continue;
                    }

                    // 2. Cool-down expired: reset failure counter
                    if cooldown_until.is_some() {
                        cooldown_until = None;
                        consecutive_failures = 0;
                        tracing::info!(
                            plugin_id = %self.id,
                            "ClickHouse cool-down period ended, resuming batch pushes"
                        );
                    }

                    // 3. Normal drain and wire serialization
                    let batches = subscriber.drain_batches(64);
                    if batches.is_empty() {
                        if shutting_down {
                            tracing::info!(plugin_id = %self.id, "ClickHouse log shipper shutting down");
                            break;
                        }
                        continue;
                    }

                    payload_buf.clear();
                    let recs = match format {
                        ClickHouseFormat::RowBinary => {
                            super::format::format_row_binary(&batches, &mut payload_buf)
                        }
                        ClickHouseFormat::JsonEachRow => {
                            super::format::format_json_each_row(&batches, &mut payload_buf)
                        }
                    };
                    drop(batches);

                    if recs == 0 || payload_buf.is_empty() {
                        if shutting_down {
                            tracing::info!(plugin_id = %self.id, "ClickHouse log shipper shutting down");
                            break;
                        }
                        continue;
                    }

                    head_buf.clear();
                    head_buf.extend_from_slice(&prebaked_insert_prefix);
                    velda_core::lut::decimal::fast_write_u64(
                        &mut head_buf,
                        payload_buf.len() as u64,
                    );
                    head_buf.extend_from_slice(b"\r\n\r\n");

                    (false, shutting_down, recs)
                };

                let (req_head, req_body): (&[u8], &[u8]) = if is_ddl {
                    (&ddl_request, &[])
                } else {
                    (&head_buf, &payload_buf)
                };

                // 4. Borrow ClickHouseLease in-place from EtpPageArena & execute HTTP/1.1 exchange
                let http_result: Result<u16, EtpError> = match etp.acquire().await {
                    Err(err) => Err(err),
                    Ok(mut lease) => {
                        let mut outcome: Result<u16, EtpError> = Err(EtpError::Timeout {
                            id: etp.config.id.clone(),
                            timeout_ms: self.config.request_timeout_ms,
                        });

                        for attempt in 0..2 {
                            let exchange = tokio::time::timeout(request_timeout, async {
                                lease.write_all(req_head).await?;
                                if !req_body.is_empty() {
                                    lease.write_all(req_body).await?;
                                }
                                lease.flush().await?;

                                // Read and parse HTTP/1.1 response headers with httparse::Response
                                resp_buf.clear();
                                let mut tmp = [0u8; 4096];
                                let (status, hdr_len, content_length, chunked, connection_close) =
                                    loop {
                                        let n = lease.read(&mut tmp).await?;
                                        if n == 0 {
                                            return Err(EtpError::Io(std::io::Error::new(
                                                std::io::ErrorKind::UnexpectedEof,
                                                "ClickHouse closed connection while reading HTTP response headers",
                                            )));
                                        }
                                        if let Some(chunk) = tmp.get(..n) {
                                            resp_buf.extend_from_slice(chunk);
                                        }
                                        if resp_buf.len() > 32 * 1024 {
                                            return Err(EtpError::Io(std::io::Error::new(
                                                std::io::ErrorKind::InvalidData,
                                                "ClickHouse HTTP response headers exceeded 32 KiB limit",
                                            )));
                                        }

                                        let mut headers = [httparse::EMPTY_HEADER; 64];
                                        let mut resp = httparse::Response::new(&mut headers);
                                        match resp.parse(&resp_buf) {
                                            Ok(httparse::Status::Complete(hdr_len)) => {
                                                let code = resp.code.ok_or_else(|| {
                                                    EtpError::Io(std::io::Error::new(
                                                        std::io::ErrorKind::InvalidData,
                                                        "ClickHouse response missing HTTP status code",
                                                    ))
                                                })?;
                                                let mut cl: Option<usize> = None;
                                                let mut is_chunked = false;
                                                let mut is_close = false;
                                                for h in resp.headers.iter() {
                                                    if h.name.eq_ignore_ascii_case("content-length") {
                                                        if let Ok(s) = std::str::from_utf8(h.value) {
                                                            cl = s.trim().parse::<usize>().ok();
                                                        }
                                                    } else if h
                                                        .name
                                                        .eq_ignore_ascii_case("transfer-encoding")
                                                    {
                                                        if let Ok(s) = std::str::from_utf8(h.value)
                                                            && s.to_ascii_lowercase()
                                                                .contains("chunked")
                                                        {
                                                            is_chunked = true;
                                                        }
                                                    } else if h.name.eq_ignore_ascii_case("connection")
                                                        && let Ok(s) = std::str::from_utf8(h.value)
                                                        && s.trim().eq_ignore_ascii_case("close")
                                                    {
                                                        is_close = true;
                                                    }
                                                }
                                                break (code, hdr_len, cl, is_chunked, is_close);
                                            }
                                            Ok(httparse::Status::Partial) => continue,
                                            Err(err) => {
                                                return Err(EtpError::Io(std::io::Error::new(
                                                    std::io::ErrorKind::InvalidData,
                                                    format!(
                                                        "Invalid HTTP response from ClickHouse: {err}"
                                                    ),
                                                )));
                                            }
                                        }
                                    };

                                // Drain response body to preserve keep-alive stream framing
                                if chunked {
                                    let mut pos = hdr_len;
                                    loop {
                                        let crlf_rel = loop {
                                            if let Some(tail) = resp_buf.get(pos..)
                                                && let Some(rel) =
                                                    tail.windows(2).position(|w| w == b"\r\n")
                                            {
                                                break rel;
                                            }
                                            if resp_buf.len().saturating_sub(pos) > 8192 {
                                                return Err(EtpError::Io(std::io::Error::new(
                                                    std::io::ErrorKind::InvalidData,
                                                    "Chunked HTTP size line exceeded 8 KiB limit",
                                                )));
                                            }
                                            let n = lease.read(&mut tmp).await?;
                                            if n == 0 {
                                                return Err(EtpError::Io(std::io::Error::new(
                                                    std::io::ErrorKind::UnexpectedEof,
                                                    "Unexpected EOF while reading chunked HTTP body",
                                                )));
                                            }
                                            if let Some(chunk) = tmp.get(..n) {
                                                resp_buf.extend_from_slice(chunk);
                                            }
                                        };

                                        let size_slice = pos
                                            .checked_add(crlf_rel)
                                            .and_then(|end| resp_buf.get(pos..end))
                                            .ok_or_else(|| {
                                                EtpError::Io(std::io::Error::new(
                                                    std::io::ErrorKind::InvalidData,
                                                    "Invalid HTTP chunk size range",
                                                ))
                                            })?;
                                        let size_line =
                                            std::str::from_utf8(size_slice).map_err(|_| {
                                                EtpError::Io(std::io::Error::new(
                                                    std::io::ErrorKind::InvalidData,
                                                    "Invalid UTF-8 in HTTP chunk size line",
                                                ))
                                            })?;
                                        let hex_part =
                                            size_line.split(';').next().unwrap_or("").trim();
                                        let chunk_size =
                                            usize::from_str_radix(hex_part, 16).map_err(|_| {
                                                EtpError::Io(std::io::Error::new(
                                                    std::io::ErrorKind::InvalidData,
                                                    format!("Invalid HTTP chunk size '{size_line}'"),
                                                ))
                                            })?;
                                        pos += crlf_rel + 2;

                                        if chunk_size == 0 {
                                            loop {
                                                let trailer_rel = loop {
                                                    if let Some(tail) = resp_buf.get(pos..)
                                                        && let Some(rel) = tail
                                                            .windows(2)
                                                            .position(|w| w == b"\r\n")
                                                    {
                                                        break rel;
                                                    }
                                                    if resp_buf.len().saturating_sub(pos) > 8192 {
                                                        return Err(EtpError::Io(
                                                            std::io::Error::new(
                                                                std::io::ErrorKind::InvalidData,
                                                                "Chunked HTTP trailer exceeded 8 KiB limit",
                                                            ),
                                                        ));
                                                    }
                                                    let n = lease.read(&mut tmp).await?;
                                                    if n == 0 {
                                                        return Err(EtpError::Io(
                                                            std::io::Error::new(
                                                                std::io::ErrorKind::UnexpectedEof,
                                                                "Unexpected EOF while reading chunked trailer",
                                                            ),
                                                        ));
                                                    }
                                                    if let Some(chunk) = tmp.get(..n) {
                                                        resp_buf.extend_from_slice(chunk);
                                                    }
                                                };
                                                pos += trailer_rel + 2;
                                                if trailer_rel == 0 {
                                                    break;
                                                }
                                            }
                                            break;
                                        }

                                        let need = chunk_size + 2;
                                        let avail = resp_buf.len().saturating_sub(pos);
                                        if avail >= need {
                                            pos += need;
                                            if pos >= resp_buf.len() {
                                                resp_buf.clear();
                                                pos = 0;
                                            } else if pos > 4096 {
                                                let tail_len = resp_buf.len().saturating_sub(pos);
                                                for i in 0..tail_len {
                                                    if let Some(&b) = resp_buf.get(pos + i)
                                                        && let Some(dst) = resp_buf.get_mut(i)
                                                    {
                                                        *dst = b;
                                                    }
                                                }
                                                resp_buf.truncate(tail_len);
                                                pos = 0;
                                            }
                                        } else {
                                            let mut remaining = need - avail;
                                            resp_buf.clear();
                                            pos = 0;
                                            let mut sink = [0u8; 8192];
                                            while remaining > 0 {
                                                let to_read = remaining.min(sink.len());
                                                if let Some(buf) = sink.get_mut(..to_read) {
                                                    lease.read_exact(buf).await?;
                                                }
                                                remaining -= to_read;
                                            }
                                        }
                                    }
                                } else if let Some(len) = content_length
                                    && len > 0
                                {
                                    let already = resp_buf.len().saturating_sub(hdr_len);
                                    if len > already {
                                        let mut remaining = len - already;
                                        let mut sink = [0u8; 8192];
                                        while remaining > 0 {
                                            let to_read = remaining.min(sink.len());
                                            if let Some(buf) = sink.get_mut(..to_read) {
                                                lease.read_exact(buf).await?;
                                            }
                                            remaining -= to_read;
                                        }
                                    }
                                }

                                Ok((status, connection_close))
                            })
                            .await;

                            match exchange {
                                Ok(Ok((status, connection_close))) => {
                                    if connection_close || status >= 500 {
                                        lease.recreate_in_background();
                                    }
                                    outcome = Ok(status);
                                    break;
                                }
                                Ok(Err(EtpError::Io(_))) if attempt == 0 => {
                                    if let Err(err) = lease.reconnect().await {
                                        lease.recreate_in_background();
                                        outcome = Err(err);
                                        break;
                                    }
                                }
                                Ok(Err(err)) => {
                                    lease.recreate_in_background();
                                    outcome = Err(err);
                                    break;
                                }
                                Err(_) => {
                                    lease.recreate_in_background();
                                    outcome = Err(EtpError::Timeout {
                                        id: etp.config.id.clone(),
                                        timeout_ms: self.config.request_timeout_ms,
                                    });
                                    break;
                                }
                            }
                        }

                        outcome
                    }
                };

                // 5. Record result metrics & cool-down state transitions
                if is_ddl {
                    match http_result {
                        Ok(status) if (200..300).contains(&status) => {
                            tracing::info!(
                                database = %database,
                                table = %VELDA_ACCESS_LOGS_TABLE,
                                "Verified ClickHouse access logs table exists"
                            );
                        }
                        Ok(status) => {
                            tracing::warn!(
                                plugin_id = %self.id,
                                status = status,
                                database = %database,
                                "Could not ensure ClickHouse table on startup (will attempt during push)"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                plugin_id = %self.id,
                                error = %e,
                                database = %database,
                                "Could not ensure ClickHouse table on startup (will attempt during push)"
                            );
                        }
                    }
                    continue;
                }

                match http_result {
                    Ok(status) if (200..300).contains(&status) => {
                        consecutive_failures = 0;
                        self.shipped_records
                            .fetch_add(encoded_recs as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        self.failed_pushes.fetch_add(1, Ordering::Relaxed);

                        tracing::warn!(
                            plugin_id = %self.id,
                            error = %e,
                            records = encoded_recs,
                            attempt = consecutive_failures,
                            "Failed to push batch to ClickHouse (dropped without retry)"
                        );

                        if consecutive_failures > 10 {
                            tracing::warn!(
                                plugin_id = %self.id,
                                "ClickHouse shipper failed 10 consecutive times; entering 30s cool-down"
                            );
                            cooldown_until =
                                Some(tokio::time::Instant::now() + Duration::from_secs(30));
                            consecutive_failures = 0;
                        }
                    }
                    Ok(status) => {
                        consecutive_failures += 1;
                        self.failed_pushes.fetch_add(1, Ordering::Relaxed);

                        tracing::warn!(
                            plugin_id = %self.id,
                            status = status,
                            records = encoded_recs,
                            attempt = consecutive_failures,
                            "ClickHouse returned unexpected HTTP status code"
                        );

                        if consecutive_failures > 10 {
                            tracing::warn!(
                                plugin_id = %self.id,
                                "ClickHouse shipper failed 10 consecutive times; entering 30s cool-down"
                            );
                            cooldown_until =
                                Some(tokio::time::Instant::now() + Duration::from_secs(30));
                            consecutive_failures = 0;
                        }
                    }
                }

                if is_shutting_down {
                    tracing::info!(plugin_id = %self.id, "ClickHouse log shipper shutting down");
                    break;
                }
            }

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use velda_core::{CompletionReason, GrpcStatus, HttpMethod, PipeKind, UaFamily, UaOs};
    use velda_etp::{EtpEntryConfig, EtpKind, EtpPoolSettings};
    use velda_observability::TelemetryRecord;

    #[tokio::test]
    async fn test_prebaked_headers_auth_database_override_and_chunked_drain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
        let captured_c = Arc::clone(&captured);

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            // 1. DDL request
            let n1 = sock.read(&mut buf).await.unwrap();
            captured_c
                .lock()
                .await
                .push(String::from_utf8_lossy(&buf[..n1]).into_owned());
            sock.write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n2\r\nok\r\n0\r\n\r\n",
            )
            .await
            .unwrap();
            sock.flush().await.unwrap();

            // 2. Batch INSERT request over same kept-alive socket
            let n2 = sock.read(&mut buf).await.unwrap();
            captured_c
                .lock()
                .await
                .push(String::from_utf8_lossy(&buf[..n2]).into_owned());
            sock.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: keep-alive\r\n\r\ndone",
            )
            .await
            .unwrap();
            sock.flush().await.unwrap();
        });

        let entry = EtpEntryConfig {
            id: "ch-auth".into(),
            kind: EtpKind::ClickHouse,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: None,
            settings: serde_json::json!({
                "host": "127.0.0.1",
                "port": port,
                "database": "etp_default_db",
                "user": "test_user",
                "password": "test_pass"
            }),
        };
        let etp = Arc::new(ClickHouseEtp::from_entry(&entry).unwrap());
        let store = Arc::new(TelemetryStore::with_cores_and_capacity(1, 128));

        let cfg = ClickHouseLogShipperConfig {
            etp_id: "ch-auth".into(),
            database: "override_db".into(),
            format: "json_each_row".into(),
            flush_interval_ms: 100,
            max_batch_size: 100,
            request_timeout_ms: 2000,
        };
        let plugin = Arc::new(
            ClickHouseLogShipperPlugin::with_etp("ch_unit", cfg, etp, Arc::clone(&store)).unwrap(),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let p = Arc::clone(&plugin);
        let runner = tokio::spawn(async move { p.run(shutdown_rx).await });

        while store.active_subscriber_count.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let mut rec = TelemetryRecord::zeroed();
        rec.ingress_time_nanos = 1_732_000_000_000_000_000;
        rec.request_id = 42;
        rec.duration_ns = 1234;
        rec.peer_ip = 0x7f00_0001;
        rec.upstream_ip = 0x7f00_0001;
        rec.upstream_port = 8080;
        rec.route_id = 1;
        rec.status_code = 200;
        rec.pipe = PipeKind::BufferedH1H1;
        rec.method = HttpMethod::Get;
        rec.completion = CompletionReason::Complete;
        rec.grpc_status = GrpcStatus::Ok;
        rec.ua_family = UaFamily::Curl;
        rec.ua_os = UaOs::Linux;
        rec.set_url("/test");
        store.emit_on_core(0, &rec);
        store.flush_all_lanes(64);

        for _ in 0..50 {
            if plugin.shipped_records.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        assert_eq!(plugin.shipped_records.load(Ordering::Relaxed), 1);
        assert_eq!(plugin.failed_pushes.load(Ordering::Relaxed), 0);

        let _ = shutdown_tx.send(true);
        runner.await.unwrap().unwrap();
        server.await.unwrap();

        let reqs = captured.lock().await;
        assert_eq!(reqs.len(), 2);
        assert!(reqs[0].starts_with("POST / HTTP/1.1\r\n"));
        assert!(reqs[0].contains("CREATE TABLE IF NOT EXISTS override_db.velda_access_logs"));
        assert!(reqs[0].contains("X-ClickHouse-User: test_user\r\n"));
        assert!(reqs[0].contains("X-ClickHouse-Key: test_pass\r\n"));

        assert!(reqs[1].starts_with(
            "POST /?query=INSERT%20INTO%20override_db.velda_access_logs%20FORMAT%20JSONEachRow HTTP/1.1\r\n"
        ));
        assert!(reqs[1].contains(&format!("Host: 127.0.0.1:{port}\r\n")));
        assert!(reqs[1].contains("Content-Type: application/x-ndjson\r\n"));
        assert!(reqs[1].contains("X-ClickHouse-User: test_user\r\n"));
        assert!(reqs[1].contains("X-ClickHouse-Key: test_pass\r\n"));
    }
}
