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

/// A response message queued from upstream processing to be written to the HTTP/3 QUIC connection.
pub struct Http3ResponseMsg {
    pub handle: ConnectionHandle,
    pub stream_id: StreamId,
    pub response: L7Response,
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
}

impl Http3Worker {
    /// Creates a new per-core worker. The background ticker is lazily spawned on first socket registration.
    pub fn new(
        worker_id: usize,
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Arc<Self> {
        let engine = Http3Engine::new_with_config(quic_cfg, h3_config);
        let (response_tx, rx) = tokio::sync::mpsc::channel(8192);
        Arc::new(Self {
            worker_id,
            engine: Mutex::new(engine),
            response_tx,
            response_rx: Mutex::new(Some(rx)),
            socket: ArcSwapOption::empty(),
            ticker_started: std::sync::atomic::AtomicBool::new(false),
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
                let mut batch = Vec::with_capacity(64);
                loop {
                    let (min_timeout, has_conns) = {
                        let Some(worker) = worker_weak.upgrade() else {
                            break;
                        };
                        let mut engine = worker.engine.lock();
                        (engine.poll_timeout(), engine.connection_count() > 0)
                    };

                    let sleep_dur = match min_timeout {
                        Some(t) => {
                            let now = Instant::now();
                            if t <= now {
                                Duration::from_millis(1)
                            } else {
                                (t - now).clamp(Duration::from_millis(1), Duration::from_millis(25))
                            }
                        }
                        None => {
                            if has_conns {
                                Duration::from_millis(10)
                            } else {
                                Duration::from_millis(50)
                            }
                        }
                    };

                    tokio::select! {
                        biased;

                        msg = rx.recv() => {
                            match msg {
                                Some(first_msg) => {
                                    batch.push(first_msg);
                                    while batch.len() < 64 {
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

                                    let mut all_outgoing = Vec::new();
                                    let now = Instant::now();
                                    {
                                        let mut engine = worker.engine.lock();
                                        for item in batch.drain(..) {
                                            match engine.send_response(
                                                now,
                                                item.handle,
                                                item.stream_id,
                                                &item.response,
                                            ) {
                                                Ok(pkts) => {
                                                    super::downstream::H3_TOTAL_RESP_SENT
                                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                                    all_outgoing.extend(pkts);
                                                }
                                                Err(e) => {
                                                    tracing::warn!(
                                                        stream = %item.stream_id,
                                                        error = %e,
                                                        "Failed to send HTTP/3 response"
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    if !all_outgoing.is_empty() {
                                        super::downstream::send_outgoing_packets(&socket, all_outgoing).await;
                                    }
                                }
                                None => break,
                            }
                        }

                        _ = tokio::time::sleep(sleep_dur) => {
                            let Some(worker) = worker_weak.upgrade() else {
                                break;
                            };
                            let Some(socket) = worker.socket.load_full() else {
                                continue;
                            };

                            let outgoing = {
                                let mut engine = worker.engine.lock();
                                if engine.connection_count() == 0 {
                                    Vec::new()
                                } else {
                                    let now = Instant::now();
                                    engine.handle_timeouts(now)
                                }
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
    /// Creates a per-core worker pool sized 1:1 with hardware CPU cores.
    pub fn new(
        quic_cfg: Arc<velda_http3::quinn_proto::ServerConfig>,
        h3_config: velda_http3::Http3Config,
    ) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let count = cores.max(1);

        let mut workers = Vec::with_capacity(count);
        for worker_id in 0..count {
            workers.push(Http3Worker::new(worker_id, quic_cfg.clone(), h3_config));
        }
        Self {
            workers: Arc::new(workers),
        }
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
