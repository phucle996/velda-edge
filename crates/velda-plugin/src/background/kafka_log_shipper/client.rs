//! Kafka Cluster Client & Multi-Broker Connection Manager (`kafka_log_shipper::client`).
//!
//! # Architecture
//! - Manages long-lived persistent TCP/TLS transport streams across Kafka cluster brokers.
//! - Caches partition-to-leader routing topology via Metadata API.
//! - Automatically handles partition leader failover (`NOT_LEADER_OR_FOLLOWER`).
//! - Supports Plaintext, TLS (via `velda-tls`), SASL/PLAIN, and SASL/SCRAM-SHA-256.

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use rustls::ClientConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::error::PluginError;

use super::config::{
    KafkaAckMode, KafkaLogShipperConfig, KafkaTransportSecurity, PartitionStrategy, SaslMechanism,
    generate_client_id,
};
use super::format;
use super::protocol;
use velda_observability::TelemetryRecord;

/// Unified transport stream for communicating with a Kafka broker.
pub enum TransportStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl TransportStream {
    async fn write_all(&mut self, src: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(src).await,
            Self::Tls(s) => s.write_all(src).await,
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush().await,
            Self::Tls(s) => s.flush().await,
        }
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.read_exact(buf).await.map(|_| ()),
            Self::Tls(s) => s.read_exact(buf).await.map(|_| ()),
        }
    }

    /// Sends a framed Kafka request (`[4B len BE] + [header + body]`).
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), PluginError> {
        self.write_all(frame).await.map_err(PluginError::Io)?;
        self.flush().await.map_err(PluginError::Io)?;
        Ok(())
    }

    /// Reads a framed Kafka response (`[4B len BE] + [payload]`).
    pub async fn read_frame(&mut self) -> Result<Vec<u8>, PluginError> {
        let mut len_buf = [0u8; 4];
        self.read_exact(&mut len_buf)
            .await
            .map_err(PluginError::Io)?;
        let len = i32::from_be_bytes(len_buf);
        if len <= 0 || len > 64 * 1024 * 1024 {
            return Err(PluginError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid Kafka response frame length: {len} bytes"),
            )));
        }
        let mut payload = vec![0u8; len as usize];
        self.read_exact(&mut payload)
            .await
            .map_err(PluginError::Io)?;
        Ok(payload)
    }
}

/// Routing entry for a topic partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionRoute {
    pub partition_id: i32,
    pub leader_node_id: i32,
}

/// Active transport connection associated with a broker node.
pub struct BrokerConnection {
    pub node_id: i32,
    pub stream: TransportStream,
}

/// Cluster-aware Kafka client managing broker topology and persistent connections.
pub struct KafkaClusterClient {
    pub config: KafkaLogShipperConfig,
    pub client_id: String,
    pub transport_security: KafkaTransportSecurity,
    pub tls_config: Option<Arc<ClientConfig>>,
    pub correlation_id: AtomicI32,
    pub round_robin_counter: AtomicU64,

    /// Broker catalog ordered by node_id.
    pub brokers: Vec<protocol::BrokerInfo>,
    /// Topic partition routing table ordered by partition_id.
    pub partitions: Vec<PartitionRoute>,
    /// Active persistent TCP/TLS transport connections.
    pub connections: Vec<BrokerConnection>,
    /// Last successful metadata refresh timestamp.
    pub last_metadata_refresh: tokio::time::Instant,

    // ── Prebaked Ingestion Framing (Zero Allocation per Push) ───────────────
    /// Prebaked produce request prefix up to `partitions_count` (contains client_id and topic).
    pub prebaked_produce_prefix: Vec<u8>,
    /// Prebaked metadata request frame with 4-byte length prefix.
    pub prebaked_metadata_frame: Vec<u8>,
    /// Prebaked SASL handshake frame with 4-byte length prefix.
    pub prebaked_sasl_handshake: Option<Vec<u8>>,
    /// Prebaked SASL authenticate frame with 4-byte length prefix.
    pub prebaked_sasl_auth: Option<Vec<u8>>,
}

impl KafkaClusterClient {
    /// Constructs a new cluster client with prebaked frames and auto-generated per-node `client_id`.
    pub fn new(config: &KafkaLogShipperConfig) -> Result<Self, PluginError> {
        let transport_security = config.parsed_transport_security()?;
        let tls_config =
            if transport_security.is_tls() {
                Some(config.tls.build_client_config(&[]).map_err(|e| {
                    PluginError::Tls(format!("Failed to build TLS client config: {e}"))
                })?)
            } else {
                None
            };

        let client_id = generate_client_id();

        // 1. Prebake Produce Request Prefix:
        // [4B frame length placeholder] [2B api_key=0] [2B api_version=7] [4B correlation_id placeholder] [client_id string]
        // [2B transactional_id=-1] [2B acks] [4B timeout_ms] [4B topics_count=1] [topic_name string]
        let mut prebaked_produce_prefix = Vec::with_capacity(128);
        prebaked_produce_prefix.extend_from_slice(&0i32.to_be_bytes()); // Frame length placeholder at offset 0..4
        prebaked_produce_prefix.extend_from_slice(&protocol::API_KEY_PRODUCE.to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&protocol::API_VERSION_PRODUCE.to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&0i32.to_be_bytes()); // CID stamped at offset 8..12
        protocol::write_string(&mut prebaked_produce_prefix, &client_id);
        protocol::write_nullable_string(&mut prebaked_produce_prefix, None); // transactional_id
        let wire_acks = config.parsed_acks()?.to_wire_value();
        prebaked_produce_prefix.extend_from_slice(&wire_acks.to_be_bytes());
        prebaked_produce_prefix
            .extend_from_slice(&(config.request_timeout_ms as i32).to_be_bytes());
        prebaked_produce_prefix.extend_from_slice(&1i32.to_be_bytes()); // topics_count = 1
        protocol::write_string(&mut prebaked_produce_prefix, &config.topic);

        // 2. Prebake Metadata Request Frame:
        let mut meta_body = Vec::with_capacity(64);
        protocol::write_request_header(
            &mut meta_body,
            protocol::API_KEY_METADATA,
            protocol::API_VERSION_METADATA,
            0,
            &client_id,
        );
        meta_body.extend_from_slice(&1i32.to_be_bytes());
        protocol::write_string(&mut meta_body, &config.topic);
        meta_body.push(0); // allow_auto_topic_creation = false
        let prebaked_metadata_frame = protocol::wrap_framed_request(meta_body);

        // 3. Prebake SASL frames if authentication is configured:
        let sasl = config.parsed_sasl_mechanism()?;
        let (prebaked_sasl_handshake, prebaked_sasl_auth) = if sasl.is_auth_required() {
            let mut handshake_body = Vec::with_capacity(32);
            protocol::write_request_header(
                &mut handshake_body,
                protocol::API_KEY_SASL_HANDSHAKE,
                protocol::API_VERSION_SASL_HANDSHAKE,
                0,
                &client_id,
            );
            protocol::write_string(&mut handshake_body, sasl.as_str());

            let auth_frame = if sasl == SaslMechanism::Plain {
                let auth_bytes = protocol::generate_sasl_plain_auth(
                    &config.sasl_username,
                    &config.sasl_password,
                );
                let mut auth_body = Vec::with_capacity(64 + auth_bytes.len());
                protocol::write_request_header(
                    &mut auth_body,
                    protocol::API_KEY_SASL_AUTHENTICATE,
                    protocol::API_VERSION_SASL_AUTHENTICATE,
                    0,
                    &client_id,
                );
                protocol::write_bytes(&mut auth_body, &auth_bytes);
                Some(protocol::wrap_framed_request(auth_body))
            } else {
                None
            };

            (
                Some(protocol::wrap_framed_request(handshake_body)),
                auth_frame,
            )
        } else {
            (None, None)
        };

        Ok(Self {
            config: config.clone(),
            client_id,
            transport_security,
            tls_config,
            correlation_id: AtomicI32::new(1),
            round_robin_counter: AtomicU64::new(0),
            brokers: Vec::new(),
            partitions: Vec::new(),
            connections: Vec::new(),
            last_metadata_refresh: tokio::time::Instant::now() - Duration::from_secs(3600), // Force initial refresh
            prebaked_produce_prefix,
            prebaked_metadata_frame,
            prebaked_sasl_handshake,
            prebaked_sasl_auth,
        })
    }

    /// Next unique correlation ID.
    #[inline]
    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Closes and drops all active broker connections.
    pub fn close_all_connections(&mut self) {
        self.connections.clear();
    }

    /// Establishes and authenticates a direct TCP/TLS stream to a target `host:port`.
    async fn connect_to_addr(&self, host: &str, port: i32) -> Result<TransportStream, PluginError> {
        let addr = format!("{host}:{port}");
        let connect_timeout = Duration::from_millis(self.config.connect_timeout_ms);

        let tcp = tokio::time::timeout(connect_timeout, TcpStream::connect(&addr))
            .await
            .map_err(|_| {
                PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Connection timeout to Kafka broker at {addr}"),
                ))
            })?
            .map_err(|e| {
                PluginError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to connect to Kafka broker at {addr}: {e}"),
                ))
            })?;

        let _ = tcp.set_nodelay(true);

        let mut stream = if self.transport_security.is_tls() {
            let tls_cfg = self
                .tls_config
                .as_ref()
                .ok_or_else(|| PluginError::Tls("TLS config uninitialized".into()))?;

            let tls_stream = tokio::time::timeout(
                connect_timeout,
                velda_tls::client::connect(tcp, tls_cfg, host),
            )
            .await
            .map_err(|_| {
                PluginError::Tls(format!("TLS handshake timeout to Kafka broker at {addr}"))
            })?
            .map_err(|e| {
                PluginError::Tls(format!(
                    "TLS handshake failed to Kafka broker at {addr}: {e}"
                ))
            })?;

            TransportStream::Tls(Box::new(tls_stream))
        } else {
            TransportStream::Plain(tcp)
        };

        // Execute SASL authentication if configured
        let sasl = self.config.parsed_sasl_mechanism()?;
        if sasl.is_auth_required() {
            self.authenticate_sasl(&mut stream, sasl).await?;
        }

        Ok(stream)
    }

    /// Performs SASL handshake and authentication on an open transport stream.
    async fn authenticate_sasl(
        &self,
        stream: &mut TransportStream,
        mechanism: SaslMechanism,
    ) -> Result<(), PluginError> {
        let req_timeout = Duration::from_millis(self.config.request_timeout_ms);
        let cid1 = self.next_correlation_id();

        // 1. SaslHandshake (API 17, Version 1) via prebaked frame
        let mut handshake_req = self
            .prebaked_sasl_handshake
            .as_ref()
            .ok_or_else(|| PluginError::Config("SASL handshake frame not prebaked".into()))?
            .clone();
        handshake_req[8..12].copy_from_slice(&cid1.to_be_bytes());

        tokio::time::timeout(req_timeout, stream.send_frame(&handshake_req))
            .await
            .map_err(|_| {
                PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "SASL Handshake write timeout",
                ))
            })??;

        let handshake_resp = tokio::time::timeout(req_timeout, stream.read_frame())
            .await
            .map_err(|_| {
                PluginError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "SASL Handshake read timeout",
                ))
            })??;

        let handshake_err = protocol::decode_sasl_handshake_response(&handshake_resp)?;
        if handshake_err != 0 {
            return Err(PluginError::Config(format!(
                "Kafka SASL handshake rejected for mechanism '{}' with error code {handshake_err}",
                mechanism.as_str()
            )));
        }

        // 2. SaslAuthenticate (API 36, Version 1) via prebaked frame
        let cid2 = self.next_correlation_id();
        match mechanism {
            SaslMechanism::Plain => {
                let mut auth_req = self
                    .prebaked_sasl_auth
                    .as_ref()
                    .ok_or_else(|| PluginError::Config("SASL auth frame not prebaked".into()))?
                    .clone();
                auth_req[8..12].copy_from_slice(&cid2.to_be_bytes());

                tokio::time::timeout(req_timeout, stream.send_frame(&auth_req))
                    .await
                    .map_err(|_| {
                        PluginError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "SASL Authenticate write timeout",
                        ))
                    })??;

                let auth_resp = tokio::time::timeout(req_timeout, stream.read_frame())
                    .await
                    .map_err(|_| {
                        PluginError::Io(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "SASL Authenticate read timeout",
                        ))
                    })??;

                let (auth_err, _) = protocol::decode_sasl_authenticate_response(&auth_resp)?;
                if auth_err != 0 {
                    return Err(PluginError::Config(format!(
                        "Kafka SASL/PLAIN authentication failed for user '{}' with error code {auth_err}",
                        self.config.sasl_username
                    )));
                }
            }
            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => {
                // For SCRAM: PLAIN fallback / error if broker enforces SCRAM
                return Err(PluginError::Config(format!(
                    "SASL mechanism '{}' requires SCRAM exchange provider (use SASL/PLAIN or TLS client auth)",
                    mechanism.as_str()
                )));
            }
            SaslMechanism::None => {}
        }

        Ok(())
    }

    /// Fetches and updates topic metadata and broker partition leaders.
    pub async fn refresh_metadata(&mut self) -> Result<(), PluginError> {
        let req_timeout = Duration::from_millis(self.config.request_timeout_ms);
        let cid = self.next_correlation_id();
        let mut meta_req = self.prebaked_metadata_frame.clone();
        meta_req[8..12].copy_from_slice(&cid.to_be_bytes());

        // Attempt metadata query across existing connections, then fall back to seed brokers
        let mut last_err = None;

        // Try existing connections first
        let active_node_ids: Vec<i32> = self.connections.iter().map(|c| c.node_id).collect();
        for node_id in active_node_ids {
            if let Some(conn) = self.connections.iter_mut().find(|c| c.node_id == node_id) {
                let stream = &mut conn.stream;
                let res = async {
                    stream.send_frame(&meta_req).await?;
                    let resp_frame = stream.read_frame().await?;
                    protocol::decode_metadata_response(&resp_frame)
                };

                match tokio::time::timeout(req_timeout, res).await {
                    Ok(Ok(meta)) => {
                        self.apply_metadata(meta);
                        self.last_metadata_refresh = tokio::time::Instant::now();
                        return Ok(());
                    }
                    Ok(Err(e)) => {
                        last_err = Some(e);
                        self.connections.retain(|c| c.node_id != node_id);
                    }
                    Err(_) => {
                        self.connections.retain(|c| c.node_id != node_id);
                    }
                }
            }
        }

        // Fall back to seed brokers declared in config
        for broker_str in &self.config.brokers {
            let (host, port) = parse_host_port(broker_str);
            match self.connect_to_addr(host, port).await {
                Ok(mut seed_stream) => {
                    let res = async {
                        seed_stream.send_frame(&meta_req).await?;
                        let resp_frame = seed_stream.read_frame().await?;
                        protocol::decode_metadata_response(&resp_frame)
                    };

                    match tokio::time::timeout(req_timeout, res).await {
                        Ok(Ok(meta)) => {
                            let matched_node = meta
                                .brokers
                                .iter()
                                .find(|b| b.host == host && b.port == port)
                                .map(|b| b.node_id);

                            self.apply_metadata(meta);
                            self.last_metadata_refresh = tokio::time::Instant::now();

                            if let Some(nid) = matched_node {
                                self.connections.retain(|c| c.node_id != nid);
                                self.connections.push(BrokerConnection {
                                    node_id: nid,
                                    stream: seed_stream,
                                });
                            }
                            return Ok(());
                        }
                        Ok(Err(e)) => {
                            last_err = Some(e);
                        }
                        Err(_) => {}
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            PluginError::Config(format!(
                "Failed to fetch Kafka topic metadata for '{}' from all seed brokers",
                self.config.topic
            ))
        }))
    }

    /// Applies fetched metadata to internal broker and partition routing tables.
    fn apply_metadata(&mut self, mut meta: protocol::MetadataResponse) {
        meta.brokers.sort_unstable_by_key(|b| b.node_id);
        self.brokers = meta.brokers;

        self.partitions.clear();
        for p in meta.partitions {
            if p.error_code == 0 && p.leader_id >= 0 {
                self.partitions.push(PartitionRoute {
                    partition_id: p.partition_id,
                    leader_node_id: p.leader_id,
                });
            }
        }
        self.partitions.sort_unstable_by_key(|p| p.partition_id);

        tracing::info!(
            topic = %self.config.topic,
            brokers = self.brokers.len(),
            partitions = self.partitions.len(),
            "Refreshed Kafka cluster topology and partition leaders"
        );
    }

    /// Obtains an active connection to the leader broker, connecting if necessary.
    async fn get_or_connect_leader(
        &mut self,
        leader_id: i32,
    ) -> Result<&mut TransportStream, PluginError> {
        if !self.connections.iter().any(|c| c.node_id == leader_id) {
            let broker_info = self
                .brokers
                .iter()
                .find(|b| b.node_id == leader_id)
                .ok_or_else(|| {
                    PluginError::Config(format!(
                        "Unknown Kafka broker node_id {leader_id} for topic '{}'",
                        self.config.topic
                    ))
                })?;

            let stream = self
                .connect_to_addr(&broker_info.host, broker_info.port)
                .await?;
            self.connections.push(BrokerConnection {
                node_id: leader_id,
                stream,
            });
        }

        Ok(self
            .connections
            .iter_mut()
            .find(|c| c.node_id == leader_id)
            .map(|c| &mut c.stream)
            .expect("Stream present"))
    }

    /// Groups records by partition, encodes RecordBatch v2 payloads, and produces to leader brokers.
    ///
    /// Returns total successfully produced records count.
    pub async fn push_records(
        &mut self,
        records: &[TelemetryRecord],
    ) -> Result<usize, PluginError> {
        if records.is_empty() {
            return Ok(0);
        }

        let refresh_interval = Duration::from_millis(self.config.metadata_refresh_ms);
        if self.partitions.is_empty() || self.last_metadata_refresh.elapsed() >= refresh_interval {
            self.refresh_metadata().await?;
        }

        if self.partitions.is_empty() {
            return Err(PluginError::Config(format!(
                "No active leader partitions available for Kafka topic '{}'",
                self.config.topic
            )));
        }

        let part_count = self.partitions.len();
        let partitioner = self.config.parsed_partitioner()?;
        let codec = self.config.parsed_compression()?;
        let format = self.config.parsed_format()?;
        let acks = self.config.parsed_acks()?;
        let req_timeout = Duration::from_millis(self.config.request_timeout_ms);

        // 1. Group records by target partition using flat array indexing (0 hashing overhead)
        let mut partition_buckets: Vec<Vec<&TelemetryRecord>> = (0..part_count)
            .map(|_| Vec::with_capacity(records.len() / part_count + 1))
            .collect();

        for rec in records {
            let idx = match partitioner {
                PartitionStrategy::RoundRobin => {
                    (self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize) % part_count
                }
                PartitionStrategy::RouteId => (rec.route_id as usize) % part_count,
                PartitionStrategy::PeerIp => (rec.peer_ip as usize) % part_count,
            };
            partition_buckets[idx].push(rec);
        }

        // 2. Encode RecordBatch v2 per partition, then group by leader node_id in flat vector
        struct LeaderBatch {
            leader_node_id: i32,
            batches: Vec<(i32, Vec<u8>)>,
        }

        let mut leader_batches: Vec<LeaderBatch> = Vec::with_capacity(self.brokers.len().max(1));

        for (idx, part_recs) in partition_buckets.into_iter().enumerate() {
            if part_recs.is_empty() {
                continue;
            }

            let route = self.partitions[idx];
            let part_id = route.partition_id;
            let leader_id = route.leader_node_id;
            if leader_id < 0 {
                continue;
            }

            let mut batch_buf = Vec::with_capacity(part_recs.len() * 256);
            format::encode_record_batch(&part_recs, codec, format, None, &mut batch_buf);

            if let Some(lb) = leader_batches
                .iter_mut()
                .find(|lb| lb.leader_node_id == leader_id)
            {
                lb.batches.push((part_id, batch_buf));
            } else {
                leader_batches.push(LeaderBatch {
                    leader_node_id: leader_id,
                    batches: vec![(part_id, batch_buf)],
                });
            }
        }

        let mut total_shipped = 0;
        let mut need_metadata_refresh = false;

        // 3. Dispatch produce requests to each leader broker via prebaked prefix
        for leader_batch in leader_batches {
            let leader_id = leader_batch.leader_node_id;
            let batches = leader_batch.batches;
            let cid = self.next_correlation_id();
            let total_payload_bytes: usize = batches.iter().map(|(_, b)| 8 + b.len()).sum();
            let mut produce_req =
                Vec::with_capacity(self.prebaked_produce_prefix.len() + 4 + total_payload_bytes);
            produce_req.extend_from_slice(&self.prebaked_produce_prefix);
            // Stamp correlation_id at offset 8..12 (4B frame len + 2B api_key + 2B api_ver)
            produce_req[8..12].copy_from_slice(&cid.to_be_bytes());
            // Append partitions array count:
            produce_req.extend_from_slice(&(batches.len() as i32).to_be_bytes());
            for (part_id, record_bytes) in &batches {
                produce_req.extend_from_slice(&part_id.to_be_bytes());
                protocol::write_bytes(&mut produce_req, record_bytes);
            }
            let frame_len = (produce_req.len() - 4) as i32;
            produce_req[0..4].copy_from_slice(&frame_len.to_be_bytes());

            // Connect & send
            let send_result = async {
                let stream = self.get_or_connect_leader(leader_id).await?;
                stream.send_frame(&produce_req).await?;

                if acks != KafkaAckMode::None {
                    let resp_frame = stream.read_frame().await?;
                    let resp = protocol::decode_produce_response(&resp_frame)?;
                    Ok::<Option<protocol::ProduceResponse>, PluginError>(Some(resp))
                } else {
                    Ok(None)
                }
            };

            match tokio::time::timeout(req_timeout, send_result).await {
                Ok(Ok(Some(resp))) => {
                    for p in resp.partitions {
                        match p.error_code {
                            0 => {
                                // Successful partition produce
                                let count: usize = batches
                                    .iter()
                                    .find(|(id, _)| *id == p.partition_id)
                                    .map(|_| 1)
                                    .unwrap_or(0);
                                total_shipped += count;
                            }
                            3 | 6 => {
                                // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER
                                need_metadata_refresh = true;
                                tracing::warn!(
                                    partition = p.partition_id,
                                    error_code = p.error_code,
                                    "Kafka partition leader changed; scheduled metadata refresh"
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
                }
                Ok(Ok(None)) => {
                    // acks == None: Fire-and-forget success
                    total_shipped += records.len();
                }
                Ok(Err(e)) => {
                    self.connections.retain(|c| c.node_id != leader_id);
                    need_metadata_refresh = true;
                    tracing::warn!(
                        leader_id,
                        error = %e,
                        "Failed to produce batch to Kafka broker"
                    );
                }
                Err(_) => {
                    self.connections.retain(|c| c.node_id != leader_id);
                    need_metadata_refresh = true;
                    tracing::warn!(leader_id, "Timeout producing batch to Kafka broker");
                }
            }
        }

        if need_metadata_refresh {
            let _ = self.refresh_metadata().await;
        }

        if total_shipped > 0 {
            Ok(records.len())
        } else {
            Err(PluginError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!(
                    "Failed to produce records to Kafka topic '{}'",
                    self.config.topic
                ),
            )))
        }
    }
}

/// Helper parsing `host:port` string into `(&str, i32)`.
fn parse_host_port(s: &str) -> (&str, i32) {
    if s.starts_with('[')
        && let Some(close) = s.find(']')
    {
        let host = &s[1..close];
        let port = s[close + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse::<i32>().ok())
            .unwrap_or(9092);
        return (host, port);
    }
    if let Some(colon) = s.rfind(':') {
        let host = &s[..colon];
        let port = s[colon + 1..].parse::<i32>().unwrap_or(9092);
        (host, port)
    } else {
        (s, 9092)
    }
}
