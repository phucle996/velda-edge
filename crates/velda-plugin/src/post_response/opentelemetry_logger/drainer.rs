//! Asynchronous Shadow Worker Drainer & Flusher (`opentelemetry_logger::drainer`).
//!
//! Owns non-blocking batch popping from SPSC RAM slots, non-temporal prefetching,
//! serialization, and network dispatching to the OpenTelemetry Collector via persistent HTTP/1.1 or HTTP/2 gRPC.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use http::HeaderMap;

use super::client::OtlpLogClient;
use super::client_grpc::OtlpLogsGrpcClient;
use super::proto::format_otlp_records_proto;
use super::record::{PackedOtlpRecord, format_otlp_records_json};
use super::ring_buffer::ShardedRingBuffer;

pub struct ShadowOtlpDrainer {
    protocol: String,
    endpoint: String,
    batch_size: usize,
    flush_interval: Duration,
    connect_timeout: Duration,
    request_timeout: Duration,
    service_name: String,
    custom_headers: HeaderMap,
    ring_buffer: Arc<ShardedRingBuffer>,
    shutdown: Arc<AtomicBool>,
}

impl ShadowOtlpDrainer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        protocol: String,
        endpoint: String,
        batch_size: usize,
        flush_interval: Duration,
        connect_timeout: Duration,
        request_timeout: Duration,
        service_name: String,
        custom_headers: HeaderMap,
        ring_buffer: Arc<ShardedRingBuffer>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            protocol,
            endpoint,
            batch_size,
            flush_interval,
            connect_timeout,
            request_timeout,
            service_name,
            custom_headers,
            ring_buffer,
            shutdown,
        }
    }

    /// Dispatches the drainer flusher task onto the dedicated Shadow Logistics Worker runtime.
    ///
    /// Priority 1: Dispatches onto the core-local Shadow Worker (`SCHED_IDLE`) via `try_dispatch_shadow`.
    /// Priority 2: Falls back to the current Tokio runtime only in unit test harnesses.
    pub fn dispatch_to_shadow(self) {
        let flusher_task = Box::pin(async move {
            self.run().await;
        });

        if let Err(task) = crate::hook::try_dispatch_shadow(flusher_task)
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(task);
        }
    }

    pub async fn run(self) {
        let is_grpc = self.protocol.eq_ignore_ascii_case("grpc")
            || self.endpoint.starts_with("grpc://")
            || self.endpoint.starts_with("grpcs://");

        let batch_cap = self.batch_size.max(16);
        let mut batch = vec![PackedOtlpRecord::empty(); batch_cap];
        let mut ticker = tokio::time::interval(self.flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        if is_grpc {
            let mut client = match OtlpLogsGrpcClient::new(
                &self.endpoint,
                self.connect_timeout,
                self.request_timeout,
            ) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(endpoint = %self.endpoint, error = %e, "Failed to initialize OTLP gRPC log client");
                    return;
                }
            };

            while !self.shutdown.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = ticker.tick() => {
                        let drained = self.ring_buffer.pop_all(&mut batch);
                        if drained > 0 {
                            let proto_bytes = format_otlp_records_proto(&batch[..drained], &self.service_name);
                            if let Err(e) = client.send_logs(&proto_bytes, &self.custom_headers).await {
                                tracing::warn!(error = %e, "Failed to flush OTLP logs batch to collector via gRPC");
                            }
                        }
                    }
                }
            }

            // Final shutdown flush
            let drained = self.ring_buffer.pop_all(&mut batch);
            if drained > 0 {
                let proto_bytes = format_otlp_records_proto(&batch[..drained], &self.service_name);
                let _ = client.send_logs(&proto_bytes, &self.custom_headers).await;
            }
        } else {
            let mut client = match OtlpLogClient::new(
                &self.endpoint,
                self.connect_timeout,
                self.request_timeout,
            ) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(endpoint = %self.endpoint, error = %e, "Failed to initialize OTLP HTTP log client");
                    return;
                }
            };

            let mut json_buf = String::with_capacity(32768);

            while !self.shutdown.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = ticker.tick() => {
                        let drained = self.ring_buffer.pop_all(&mut batch);
                        if drained > 0 {
                            json_buf.clear();
                            format_otlp_records_json(&batch[..drained], &self.service_name, &mut json_buf);
                            if let Err(e) = client.send_logs(json_buf.as_bytes(), &self.custom_headers).await {
                                tracing::warn!(error = %e, "Failed to flush OTLP logs batch to collector via HTTP");
                            }
                            if json_buf.capacity() > 131072 {
                                json_buf.shrink_to(32768);
                            }
                        }
                    }
                }
            }

            // Final shutdown flush
            let drained = self.ring_buffer.pop_all(&mut batch);
            if drained > 0 {
                json_buf.clear();
                format_otlp_records_json(&batch[..drained], &self.service_name, &mut json_buf);
                let _ = client
                    .send_logs(json_buf.as_bytes(), &self.custom_headers)
                    .await;
            }
        }
    }
}
