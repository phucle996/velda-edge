//! Kafka Binary Wire Protocol Exchange (`kafka_log_shipper::wire`).
//!
//! ```text
//!   Startup:
//!     KafkaWireState::new(etp, &config)
//!         ├─► prebake Metadata v4 frame using etp.config.client_id
//!         └─► prebake Produce v7 prefix ([4B len][2B key][2B ver][4B cid][client_id]...[topic])
//!
//!   Runtime Operations:
//!     refresh_metadata(&mut self) / produce_records(&mut self, records)
//!         │
//!         ▼
//!     etp.acquire().await ──► KafkaLease (DerefMut -> EtpStream at Slot[idx])
//!         │
//!         ├─► attempt 0..2: write [frame] + flush; if expect_resp: read [4B BE len] + [payload]
//!         ├─► Io error (attempt 0) ─► lease.reconnect().await (in-place Slot[idx]) + retry
//!         ├─► timeout / persistent error ─► lease.recreate_in_background()
//!         └─► produce response:
//!               ├─ error_code == 0      ──► accumulate shipped records
//!               └─ error_code == 3 | 6  ──► schedule refresh_metadata() (leader failover)
//!
//!   Kafka TCP Binary Request / Response Frame Layout:
//!   ┌────────────────────────────────────────────────────────────────────────────┐
//!   │ [0..4] frame_len (i32 BE) | [4..6] api_key (i16 BE) | [6..8] api_ver       │
//!   │ [8..12] correlation_id (i32 BE) | [12..] client_id + API-specific payload  │
//!   └────────────────────────────────────────────────────────────────────────────┘
//! ```

use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use velda_etp::EtpError;
use velda_etp::kafka::KafkaEtp;
use velda_observability::TelemetryRecord;

use crate::error::PluginError;

use super::format;
use super::protocol;
use crate::background::log_exporter::config::{
    CompressionCodec, KafkaAckMode, KafkaLogFormat, KafkaTargetConfig, PartitionStrategy,
};

/// Reusable Kafka wire framing state backed by an attached [`KafkaEtp`] broker pool.
pub struct KafkaWireState {
    pub etp: Arc<KafkaEtp>,
    pub topic: String,
    pub format: KafkaLogFormat,
    pub compression: CompressionCodec,
    pub acks: KafkaAckMode,
    pub partition_strategy: PartitionStrategy,
    pub request_timeout: Duration,
    pub metadata_refresh_interval: Duration,
    pub last_metadata_refresh: tokio::time::Instant,
    pub correlation_id: AtomicI32,
    pub round_robin_counter: AtomicU64,
    pub produce_version: i16,
    pub partitions: Vec<i32>,
    pub prebaked_produce_prefix: Vec<u8>,
    pub prebaked_metadata_frame: Vec<u8>,
    pub batch_buf: Vec<u8>,
    pub req_buf: Vec<u8>,
    pub resp_buf: Vec<u8>,
}

impl KafkaWireState {
    /// Pre-bakes Metadata v4 and Produce v7 wire prefixes from `etp.config.client_id` and plugin config.
    pub fn new(etp: Arc<KafkaEtp>, cfg: &KafkaTargetConfig) -> Result<Self, PluginError> {
        let format = cfg.format;
        let compression = cfg.compression;
        let acks = cfg.acks;
        let partition_strategy = cfg.partitioner;
        let request_timeout = Duration::from_millis(cfg.request_timeout_ms);
        let metadata_refresh_interval = Duration::from_millis(cfg.metadata_refresh_ms);
        let client_id = etp.config.client_id.as_str();

        // Prebake Produce Request v7 prefix up to `partitions_count`.
        let mut prebaked_produce_prefix = Vec::with_capacity(128);
        prebaked_produce_prefix.extend_from_slice(&0i32.to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&protocol::API_KEY_PRODUCE.to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&protocol::API_VERSION_PRODUCE.to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&0i32.to_be_bytes());
        protocol::write_string(&mut prebaked_produce_prefix, client_id);
        protocol::write_nullable_string(&mut prebaked_produce_prefix, None);
        prebaked_produce_prefix.extend_from_slice(&acks.to_wire_value().to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&(cfg.request_timeout_ms as i32).to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&1i32.to_be_bytes());
        protocol::write_string(&mut prebaked_produce_prefix, &cfg.topic);

        // Prebake Metadata Request v4 frame.
        let prebaked_metadata_frame = protocol::encode_metadata_request(0, client_id, &cfg.topic);

        let now = tokio::time::Instant::now();
        let last_metadata_refresh = now.checked_sub(Duration::from_secs(3600)).unwrap_or(now);

        Ok(Self {
            etp,
            topic: cfg.topic.clone(),
            format,
            compression,
            acks,
            partition_strategy,
            request_timeout,
            metadata_refresh_interval,
            last_metadata_refresh,
            correlation_id: AtomicI32::new(1),
            round_robin_counter: AtomicU64::new(0),
            produce_version: protocol::API_VERSION_PRODUCE,
            partitions: Vec::new(),
            prebaked_produce_prefix,
            prebaked_metadata_frame,
            batch_buf: Vec::with_capacity(64 * 1024),
            req_buf: Vec::with_capacity(64 * 1024),
            resp_buf: Vec::with_capacity(4096),
        })
    }

    /// Queries Kafka broker Metadata API v4 using a pooled [`KafkaLease`] and updates `partitions`.
    pub async fn refresh_metadata(&mut self) -> Result<(), PluginError> {
        let cid = self.correlation_id.fetch_add(1, Ordering::Relaxed);
        self.req_buf.clear();
        self.req_buf
            .extend_from_slice(&self.prebaked_metadata_frame);
        if let Some(cid_slot) = self.req_buf.get_mut(8..12) {
            cid_slot.copy_from_slice(&cid.to_be_bytes());
        }

        send_kafka_frame(
            &self.etp,
            &self.req_buf,
            true,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await?;

        let meta = protocol::decode_metadata_response(&self.resp_buf)?;
        self.partitions.clear();
        for p in meta.partitions {
            if p.error_code == 0 && p.leader_id >= 0 {
                self.partitions.push(p.partition_id);
            }
        }
        self.partitions.sort_unstable();
        self.partitions.dedup();
        self.last_metadata_refresh = tokio::time::Instant::now();

        tracing::info!(
            topic = %self.topic,
            brokers = meta.brokers.len(),
            partitions = self.partitions.len(),
            "Refreshed Kafka topic partition topology"
        );
        Ok(())
    }

    /// Encodes `records` into RecordBatch v2 partitions, dispatches Produce v7 over [`KafkaLease`],
    /// and returns shipped record count (refreshing metadata on leader failover error codes `3 | 6`).
    pub async fn produce_records(
        &mut self,
        records: &[TelemetryRecord],
    ) -> Result<usize, PluginError> {
        if records.is_empty() {
            return Ok(0);
        }

        if self.partitions.is_empty()
            || self.last_metadata_refresh.elapsed() >= self.metadata_refresh_interval
        {
            self.refresh_metadata().await?;
        }

        if self.partitions.is_empty() {
            return Err(PluginError::Config(format!(
                "No active partitions available for Kafka topic '{}'",
                self.topic
            )));
        }

        let part_count = self.partitions.len();
        let mut partition_buckets: Vec<Vec<&TelemetryRecord>> = (0..part_count)
            .map(|_| Vec::with_capacity(records.len() / part_count + 1))
            .collect();

        for rec in records {
            let idx = match self.partition_strategy {
                PartitionStrategy::RoundRobin => {
                    (self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize) % part_count
                }
                PartitionStrategy::RouteId => (rec.route_id as usize) % part_count,
                PartitionStrategy::PeerIp => (rec.peer_ip as usize) % part_count,
            };
            if let Some(bucket) = partition_buckets.get_mut(idx) {
                bucket.push(rec);
            }
        }

        let cid = self.correlation_id.fetch_add(1, Ordering::Relaxed);
        self.req_buf.clear();
        self.req_buf
            .extend_from_slice(&self.prebaked_produce_prefix);
        if let Some(cid_slot) = self.req_buf.get_mut(8..12) {
            cid_slot.copy_from_slice(&cid.to_be_bytes());
        }

        let count_offset = self.req_buf.len();
        self.req_buf.extend_from_slice(&0i32.to_be_bytes());

        let mut sent_parts: Vec<(i32, usize)> = Vec::with_capacity(part_count);
        for (idx, part_recs) in partition_buckets.into_iter().enumerate() {
            if part_recs.is_empty() {
                continue;
            }
            let Some(&part_id) = self.partitions.get(idx) else {
                continue;
            };
            self.batch_buf.clear();
            format::encode_record_batch(
                &part_recs,
                self.compression,
                self.format,
                None,
                &mut self.batch_buf,
            );
            self.req_buf.extend_from_slice(&part_id.to_be_bytes());
            protocol::write_bytes(&mut self.req_buf, &self.batch_buf);
            sent_parts.push((part_id, part_recs.len()));
        }

        if sent_parts.is_empty() {
            return Ok(0);
        }

        if let Some(count_slot) = self.req_buf.get_mut(count_offset..count_offset + 4) {
            count_slot.copy_from_slice(&(sent_parts.len() as i32).to_be_bytes());
        }
        let frame_len = (self.req_buf.len().saturating_sub(4)) as i32;
        if let Some(len_slot) = self.req_buf.get_mut(0..4) {
            len_slot.copy_from_slice(&frame_len.to_be_bytes());
        }

        let expect_resp = self.acks != KafkaAckMode::None;
        send_kafka_frame(
            &self.etp,
            &self.req_buf,
            expect_resp,
            &mut self.resp_buf,
            self.request_timeout,
        )
        .await?;

        if !expect_resp {
            return Ok(records.len());
        }

        let resp = protocol::decode_produce_response(&self.resp_buf, self.produce_version)?;
        let mut total_shipped = 0usize;
        let mut need_metadata_refresh = false;

        for p in resp.partitions {
            match p.error_code {
                0 => {
                    if let Some(&(_, count)) =
                        sent_parts.iter().find(|&&(id, _)| id == p.partition_id)
                    {
                        total_shipped += count;
                    }
                }
                3 | 6 => {
                    need_metadata_refresh = true;
                    tracing::warn!(
                        partition = p.partition_id,
                        error_code = p.error_code,
                        "Kafka partition leader changed; refreshing metadata"
                    );
                }
                other => {
                    tracing::warn!(
                        partition = p.partition_id,
                        error_code = other,
                        "Kafka broker returned produce error"
                    );
                }
            }
        }

        if need_metadata_refresh {
            let _ = self.refresh_metadata().await;
        }

        if total_shipped > 0 {
            Ok(total_shipped)
        } else {
            Err(PluginError::Io(Error::new(
                ErrorKind::ConnectionReset,
                format!("Failed to produce records to Kafka topic '{}'", self.topic),
            )))
        }
    }
}

/// Sends a framed Kafka request over a pooled [`KafkaLease`] and optionally reads the `[4B len BE] + [payload]` response.
async fn send_kafka_frame(
    etp: &Arc<KafkaEtp>,
    frame: &[u8],
    expect_response: bool,
    resp_buf: &mut Vec<u8>,
    request_timeout: Duration,
) -> Result<(), PluginError> {
    let mut lease = etp
        .acquire()
        .await
        .map_err(|e| PluginError::Http(format!("Kafka ETP acquire error: {e}")))?;

    for attempt in 0..2 {
        let exchange: Result<Result<(), EtpError>, _> =
            tokio::time::timeout(request_timeout, async {
                lease.write_all(frame).await?;
                lease.flush().await?;
                if expect_response {
                    let mut len_buf = [0u8; 4];
                    lease.read_exact(&mut len_buf).await?;
                    let len = i32::from_be_bytes(len_buf);
                    if len <= 0 || len > 64 * 1024 * 1024 {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!("Invalid Kafka response frame length: {len} bytes"),
                        )
                        .into());
                    }
                    resp_buf.resize(len as usize, 0);
                    lease.read_exact(resp_buf).await?;
                }
                Ok(())
            })
            .await;

        match exchange {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(EtpError::Io(ref err)))
                if attempt == 0 && err.kind() != ErrorKind::InvalidData =>
            {
                if let Err(reconn_err) = lease.reconnect().await {
                    lease.recreate_in_background();
                    return Err(PluginError::Io(Error::other(format!(
                        "Kafka reconnect failed: {reconn_err}"
                    ))));
                }
            }
            Ok(Err(EtpError::Io(err))) => {
                lease.recreate_in_background();
                return Err(PluginError::Io(err));
            }
            Ok(Err(err)) => {
                lease.recreate_in_background();
                return Err(PluginError::Http(format!("Kafka ETP error: {err}")));
            }
            Err(_) => {
                lease.recreate_in_background();
                return Err(PluginError::Io(Error::new(
                    ErrorKind::TimedOut,
                    format!(
                        "Kafka request timed out after {} ms",
                        request_timeout.as_millis()
                    ),
                )));
            }
        }
    }

    lease.recreate_in_background();
    Err(PluginError::Io(Error::new(
        ErrorKind::ConnectionReset,
        format!(
            "Kafka request failed after reconnect retry on ETP '{}'",
            etp.config.id
        ),
    )))
}
