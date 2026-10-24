//! Persistent HTTP/3 QUIC State Machine Engine Container.
//!
//! Architectural Invariant:
//! Active HTTP/3 QUIC state machine engines persist across configuration reloads.
//! When routes, upstreams, or plugins reload, active client QUIC connections
//! are NOT disconnected; new incoming requests on existing connections seamlessly
//! evaluate against the latest swapped runtime snapshot.

use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use parking_lot::Mutex;
use rustc_hash::FxHashMap;
use velda_core::L7Response;
use velda_http3::Http3Engine;
use velda_http3::quinn_proto::{ConnectionHandle, StreamId};
use velda_tls::TlsServerEngine;
use velda_transport::UdpSocket;

use crate::error::EdgeError;
use crate::runtime::SharedRuntime;

/// Streaming pipeline depth (number of chunks permitted in flight per stream).
///
/// Depth 1 enforces strict O(1) flow-control backpressure: the upstream reader only fetches
/// chunk N+1 after chunk N has been accepted by Quinn or transmitted over the wire.
/// Because Quinn internally manages pacing, congestion windows, and packet flight buffers,
/// an application-layer depth of 1 prevents memory ballooning and avoids out-of-order chunk races.
pub const STREAM_PIPELINE_DEPTH: usize = 1;

/// Channel capacity for worker response messages across active streams on a single CPU core.
/// Sized to 512 messages (~32 MB peak payload capacity) to accommodate concurrency bursts without
/// excessive RAM ballooning under heavy multi-stream load.
pub const WORKER_RESPONSE_QUEUE_CAPACITY: usize = 512;

/// Batch size for draining response messages from the channel per worker tick.
/// Matches Linux GSO (`sendmmsg` / `UDP_SEGMENT`) batching limit (up to 64 datagrams).
pub const RESPONSE_BATCH_SIZE: usize = 64;

/// Minimum sleep duration when Quinn timers expire immediately or have microsecond pacing requirements.
pub const TIMER_MIN_SLEEP: Duration = Duration::from_micros(50);

/// Maximum sleep clamp to avoid sleeping past newly scheduled loss-detection deadlines.
pub const TIMER_MAX_SLEEP: Duration = Duration::from_millis(50);

/// Fallback sleep duration when no Quinn transport timers are actively registered.
pub const TIMER_IDLE_FALLBACK: Duration = Duration::from_millis(10);

/// A response or progressive streaming message queued from upstream processing
/// to be written to the HTTP/3 QUIC connection.
#[allow(clippy::large_enum_variant)]
pub enum Http3ResponseMsg {
    /// Full 1-shot HTTP/3 response (QPACK HEADERS + optional single-flight DATA frame).
    Full {
        handle: ConnectionHandle,
        stream_id: StreamId,
        response: L7Response,
    },
    /// Response HEADERS only (initiates a progressive streaming response).
    Head {
        handle: ConnectionHandle,
        stream_id: StreamId,
        status: http::StatusCode,
        headers: http::HeaderMap,
    },
    /// Progressive response DATA chunk.
    Chunk {
        handle: ConnectionHandle,
        stream_id: StreamId,
        chunk: bytes::Bytes,
        finish: bool,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    },
}

/// Downstream HTTP/3 response responder, providing uniform 1-shot and progressive streaming methods.
#[derive(Clone)]
pub struct Http3Responder {
    pub handle: ConnectionHandle,
    pub stream_id: StreamId,
    pub response_tx: tokio::sync::mpsc::Sender<Http3ResponseMsg>,
    pub request_id: Option<http::HeaderValue>,
}

impl Http3Responder {
    /// Creates a new [`Http3Responder`].
    #[inline]
    pub fn new(
        handle: ConnectionHandle,
        stream_id: StreamId,
        response_tx: tokio::sync::mpsc::Sender<Http3ResponseMsg>,
    ) -> Self {
        Self {
            handle,
            stream_id,
            response_tx,
            request_id: None,
        }
    }

    /// Sets an optional W3C Request ID header value to automatically stamp onto downstream responses.
    #[inline]
    pub fn with_request_id(mut self, request_id: Option<http::HeaderValue>) -> Self {
        self.request_id = request_id;
        self
    }

    /// Sends a 1-shot full response (buffered).
    #[inline]
    pub async fn send_response(self, mut response: L7Response) -> Result<(), EdgeError> {
        if let Some(ref req_id) = self.request_id {
            response
                .headers
                .insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
        }
        self.response_tx
            .send(Http3ResponseMsg::Full {
                handle: self.handle,
                stream_id: self.stream_id,
                response,
            })
            .await
            .map_err(|_| EdgeError::Internal("HTTP/3 worker response channel closed".into()))
    }

    /// Sends the response HEADERS frame and returns an [`Http3StreamSender`] for streaming response chunks.
    #[inline]
    pub async fn send_stream_response(
        self,
        status: http::StatusCode,
        headers: &http::HeaderMap,
    ) -> Result<Http3StreamSender, EdgeError> {
        let mut final_headers = headers.clone();
        if let Some(ref req_id) = self.request_id {
            final_headers.insert(crate::pipeline::X_REQUEST_ID, req_id.clone());
        }
        self.response_tx
            .send(Http3ResponseMsg::Head {
                handle: self.handle,
                stream_id: self.stream_id,
                status,
                headers: final_headers,
            })
            .await
            .map_err(|_| EdgeError::Internal("HTTP/3 worker response channel closed".into()))?;

        // Double-buffering: 2 permits allow 2 chunks in flight, maximizing pipelined throughput
        // while bounding peak memory to O(1) per stream.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(STREAM_PIPELINE_DEPTH));

        Ok(Http3StreamSender {
            handle: self.handle,
            stream_id: self.stream_id,
            response_tx: self.response_tx,
            semaphore,
        })
    }
}

/// Progressive stream sender for transmitting HTTP/3 response DATA chunks downstream.
pub struct Http3StreamSender {
    handle: ConnectionHandle,
    stream_id: StreamId,
    response_tx: tokio::sync::mpsc::Sender<Http3ResponseMsg>,
    semaphore: Arc<tokio::sync::Semaphore>,
}

impl Http3StreamSender {
    /// Sends a DATA frame chunk downstream with channel-driven async backpressure.
    #[inline]
    pub async fn send_chunk(&mut self, chunk: bytes::Bytes) -> Result<(), EdgeError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EdgeError::Internal("HTTP/3 stream closed".into()))?;

        self.response_tx
            .send(Http3ResponseMsg::Chunk {
                handle: self.handle,
                stream_id: self.stream_id,
                chunk,
                finish: false,
                permit: Some(permit),
            })
            .await
            .map_err(|_| EdgeError::Internal("HTTP/3 worker response channel closed".into()))
    }

    /// Explicitly closes the HTTP/3 stream by emitting the QUIC FIN bit.
    #[inline]
    pub async fn finish(self) -> Result<(), EdgeError> {
        self.response_tx
            .send(Http3ResponseMsg::Chunk {
                handle: self.handle,
                stream_id: self.stream_id,
                chunk: bytes::Bytes::new(),
                finish: true,
                permit: None,
            })
            .await
            .map_err(|_| EdgeError::Internal("HTTP/3 worker response channel closed".into()))
    }
}

/// A single per-core HTTP/3 QUIC worker state machine with direct 1:1 socket association
/// and a high-efficiency event-driven background ticker for loss recovery and ACK timer flushes.
pub struct Http3Worker {
    pub worker_id: usize,
    pub engine: Mutex<Http3Engine>,
    pub response_tx: tokio::sync::mpsc::Sender<Http3ResponseMsg>,
    response_rx: Mutex<Option<tokio::sync::mpsc::Receiver<Http3ResponseMsg>>>,
    socket: ArcSwapOption<UdpSocket>,
    ticker_started: std::sync::atomic::AtomicBool,
    pub notify: tokio::sync::Notify,
}

impl Http3Worker {
    /// Creates a new per-core worker. The background ticker is lazily spawned on first socket registration.
    pub fn new(
        worker_id: usize,
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Arc<Self> {
        let engine = Http3Engine::new_with_config(quic_cfg, h3_config);
        let (response_tx, rx) = tokio::sync::mpsc::channel(WORKER_RESPONSE_QUEUE_CAPACITY);
        Arc::new(Self {
            worker_id,
            engine: Mutex::new(engine),
            response_tx,
            response_rx: Mutex::new(Some(rx)),
            socket: ArcSwapOption::empty(),
            ticker_started: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        })
    }

    /// Registers the active UDP socket handle on the worker and starts the event-driven timeout ticker.
    #[inline]
    pub fn register_socket(self: &Arc<Self>, socket: &Arc<UdpSocket>) {
        if self
            .ticker_started
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }

        if self.socket.load().is_none() {
            self.socket.store(Some(socket.clone()));
        }

        if !self
            .ticker_started
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            let rx_opt = self.response_rx.lock().take();
            let Some(mut rx) = rx_opt else {
                return;
            };
            let worker_weak = Arc::downgrade(self);
            tokio::spawn(async move {
                let mut batch = Vec::with_capacity(RESPONSE_BATCH_SIZE);
                let mut next_timeout = {
                    let Some(worker) = worker_weak.upgrade() else {
                        return;
                    };
                    let mut engine = worker.engine.lock();
                    engine.poll_timeout()
                };

                loop {
                    // Quinn invariant: If poll_timeout is None, NO timer is active.
                    // Sleeping or polling when None causes futile wakeups and futex lock contention.
                    let sleep_opt = next_timeout.map(|t| {
                        let now = Instant::now();
                        if t <= now {
                            TIMER_MIN_SLEEP
                        } else {
                            (t - now).clamp(TIMER_MIN_SLEEP, TIMER_MAX_SLEEP)
                        }
                    });

                    tokio::select! {
                        msg = rx.recv() => {
                            match msg {
                                Some(first_msg) => {
                                    batch.push(first_msg);
                                    while batch.len() < RESPONSE_BATCH_SIZE {
                                        match rx.try_recv() {
                                            Ok(m) => batch.push(m),
                                            Err(_) => break,
                                        }
                                    }

                                    let Some(worker) = worker_weak.upgrade() else {
                                        break;
                                    };
                                    let Some(socket) = worker.socket.load_full() else {
                                        batch.clear();
                                        continue;
                                    };

                                    let mut all_outgoing = Vec::with_capacity(batch.len() * 2);
                                    let now = Instant::now();
                                    {
                                        let mut engine = worker.engine.lock();
                                        for item in batch.drain(..) {
                                            let res = match item {
                                                Http3ResponseMsg::Full {
                                                    handle,
                                                    stream_id,
                                                    response,
                                                } => engine.send_response(now, handle, stream_id, &response),
                                                Http3ResponseMsg::Head {
                                                    handle,
                                                    stream_id,
                                                    status,
                                                    headers,
                                                } => engine.send_response_head(now, handle, stream_id, status, &headers),
                                                Http3ResponseMsg::Chunk {
                                                    handle,
                                                    stream_id,
                                                    chunk,
                                                    finish,
                                                    permit,
                                                } => engine.send_data_chunk(now, handle, stream_id, &chunk, finish, permit),
                                            };
                                            match res {
                                                Ok(pkts) => {
                                                    super::downstream::H3_TOTAL_RESP_SENT
                                                        .0
                                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                    all_outgoing.extend(pkts);
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        error = %e,
                                                        "Failed to send HTTP/3 response or chunk"
                                                    );
                                                }
                                            }
                                        }

                                        // If Quinn's loss detection or PTO timer has expired, handle it immediately in the same pass
                                        if let Some(t) = engine.poll_timeout() && t <= now && engine.connection_count() > 0 {
                                            all_outgoing.extend(engine.handle_timeouts(now));
                                        }
                                        next_timeout = engine.poll_timeout();
                                    }

                                    if !all_outgoing.is_empty() {
                                        super::downstream::send_outgoing_packets(&socket, all_outgoing).await;
                                    }
                                }
                                None => break,
                            }
                        }

                        _ = async {
                            let Some(worker) = worker_weak.upgrade() else {
                                std::future::pending::<()>().await;
                                return;
                            };
                            worker.notify.notified().await;
                        } => {
                            let Some(worker) = worker_weak.upgrade() else {
                                break;
                            };
                            let now = Instant::now();
                            let outgoing = {
                                let mut engine = worker.engine.lock();
                                let pkts = if let Some(t) = engine.poll_timeout() && t <= now && engine.connection_count() > 0 {
                                    engine.handle_timeouts(now)
                                } else {
                                    Vec::new()
                                };
                                next_timeout = engine.poll_timeout();
                                pkts
                            };
                            if !outgoing.is_empty()
                                && let Some(socket) = worker.socket.load_full()
                            {
                                super::downstream::send_outgoing_packets(&socket, outgoing).await;
                            }
                        }

                        _ = async {
                            if let Some(dur) = sleep_opt {
                                tokio::time::sleep(dur).await;
                            } else {
                                tokio::time::sleep(TIMER_IDLE_FALLBACK).await;
                            }
                        } => {
                            let Some(worker) = worker_weak.upgrade() else {
                                break;
                            };
                            let Some(socket) = worker.socket.load_full() else {
                                continue;
                            };

                            let outgoing = {
                                let mut engine = worker.engine.lock();
                                let pkts = if engine.connection_count() == 0 {
                                    Vec::new()
                                } else {
                                    let now = Instant::now();
                                    engine.handle_timeouts(now)
                                };
                                next_timeout = engine.poll_timeout();
                                pkts
                            };

                            if !outgoing.is_empty() {
                                super::downstream::send_outgoing_packets(&socket, outgoing).await;
                            }
                        }
                    }
                }
            });
        }
    }
}

/// Multi-worker pool matching 1:1 with CPU cores and SO_REUSEPORT UDP socket instances.
/// Eliminates cross-core lock contention, cache thrashing, and oversharding overhead.
#[derive(Clone)]
pub struct Http3WorkerPool {
    workers: Arc<Vec<Arc<Http3Worker>>>,
}

impl Http3WorkerPool {
    /// Creates a per-core worker pool sized to an explicit worker count.
    pub fn with_worker_count(
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
        count: usize,
    ) -> Self {
        let count = count.max(1);
        let mut workers = Vec::with_capacity(count);
        for worker_id in 0..count {
            workers.push(Http3Worker::new(worker_id, quic_cfg.clone(), h3_config));
        }
        Self {
            workers: Arc::new(workers),
        }
    }

    /// Creates a per-core worker pool sized 1:1 with hardware CPU cores.
    pub fn new(
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self::with_worker_count(quic_cfg, h3_config, cores)
    }

    /// Selects the worker assigned 1:1 to the socket that received the datagram.
    /// Operates in O(1) without any IP hashing or cross-core lookups!
    #[inline]
    pub fn get_worker_for_socket(&self, socket: &UdpSocket) -> Arc<Http3Worker> {
        let idx = socket.worker_id % self.workers.len();
        self.workers[idx].clone()
    }
}

/// Global registry holding long-lived HTTP/3 QUIC worker pools indexed by listener identifier.
static H3_ENGINES: OnceLock<RwLock<FxHashMap<String, Http3WorkerPool>>> = OnceLock::new();

fn engines_table() -> &'static RwLock<FxHashMap<String, Http3WorkerPool>> {
    H3_ENGINES.get_or_init(|| RwLock::new(FxHashMap::default()))
}

/// Initializes or updates the HTTP/3 QUIC engine for the given listener.
///
/// If an engine already exists for this listener, its active connection state is PRESERVED,
/// ensuring zero-downtime and zero connection drop during configuration reloads!
pub fn init_h3_engine(listener_id: &str, tls_server: &TlsServerEngine) -> Result<(), EdgeError> {
    let mut quic_cfg = tls_server.build_quic_config().map_err(EdgeError::Tls)?;
    let h3_config = velda_http3::Http3Config::auto();
    quic_cfg.transport = Arc::new(h3_config.build_transport_config());

    let table = engines_table();
    let mut write = table.write().unwrap();
    if !write.contains_key(listener_id) {
        let pool = Http3WorkerPool::new(Arc::new(quic_cfg), h3_config);
        write.insert(listener_id.to_string(), pool);
    }
    Ok(())
}

/// Clears all active HTTP/3 engines (useful for test isolation).
pub fn clear_h3_engines() {
    let table = engines_table();
    if let Ok(mut lock) = table.write() {
        lock.clear();
    }
}

/// Retrieves all workers for the given listener, initializing if needed.
pub fn get_or_init_h3_workers(
    listener_id: &str,
    runtime: &SharedRuntime,
) -> Option<Http3WorkerPool> {
    let table = engines_table();
    if let Some(pool) = table.read().ok().and_then(|r| r.get(listener_id).cloned()) {
        return Some(pool);
    }

    let rt = runtime.load();
    let tls_server = rt.tls_server.as_ref()?;
    let _ = init_h3_engine(listener_id, tls_server);

    let table = engines_table();
    table.read().ok()?.get(listener_id).cloned()
}
