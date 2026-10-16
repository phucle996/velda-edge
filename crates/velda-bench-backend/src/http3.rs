//! HTTP/3 Protocol Router (Port 8443 QUIC UDP).
//!
//! Serves all 8 canonical routes over HTTP/3 QUIC streams using pure packet-driven state machine.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use bytes::Bytes;
use http::{HeaderMap, StatusCode, Version};
use tokio::net::UdpSocket;
use tokio::time::{Duration, interval};
use velda_core::{Body, L7Response};
use velda_http3::Http3Engine;
use velda_http3::quinn_proto::ServerConfig;

pub struct Http3Router;

impl Http3Router {
    pub async fn run(
        addr: SocketAddr,
        server_config: Arc<ServerConfig>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        tracing::info!(addr = %addr, "HTTP/3 Router (QUIC UDP) listening");

        let mut engine = Http3Engine::new(server_config);
        let mut buf = vec![0u8; 65535];
        let mut ticker = interval(Duration::from_millis(5));

        // Pre-allocate response static bodies
        let pong_bytes = Bytes::from_static(b"pong");
        let sse_bytes = Bytes::from_static(b"event: ping\ndata: heartbeat\n\n");
        let payload_12kb = Bytes::from(vec![b'A'; 12 * 1024]);
        let payload_1mb = Bytes::from(vec![b'B'; 1024 * 1024]);
        let payload_64kb = Bytes::from(vec![b'C'; 64 * 1024]);
        let payload_10mb = Bytes::from(vec![b'D'; 10 * 1024 * 1024]);

        loop {
            tokio::select! {
                recv_res = socket.recv_from(&mut buf) => {
                    let (len, peer) = match recv_res {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let now = Instant::now();
                    let (outgoing, requests) = engine.handle_datagram(now, peer, None, &buf[..len]);

                    for out in outgoing {
                        let _ = socket.send_to(&out.payload, out.peer).await;
                    }

                    for req_event in requests {
                        let path = req_event.request.uri.path();
                        let method = req_event.request.method;
                        let now = Instant::now();

                        let resp = if method == http::Method::POST && (path.contains("upload") || path.ends_with("/upload")) {
                            // PIPE 3: ClientStream (Upload) — 100% Real body consumption
                            let body_bytes = req_event.request.body.len();
                            let json = format!("{{\"status\":\"ok\",\"pipe\":\"client_stream_h3\",\"received_bytes\":{}}}\n", body_bytes);
                            let mut headers = HeaderMap::new();
                            headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
                            L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(Bytes::from(json)))
                        } else if method == http::Method::POST && (path.contains("duplex") || path.contains("echo")) {
                            // PIPE 4: Duplex (Echo)
                            let mut headers = HeaderMap::new();
                            headers.insert(http::header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
                            L7Response::new(StatusCode::OK, Version::HTTP_3, headers, req_event.request.body)
                        } else if path.contains("stream") || path.contains("sse") {
                            // PIPE 2: ServerStream
                            if path.ends_with("sse-ping") || path.ends_with("sse") {
                                let mut headers = HeaderMap::new();
                                headers.insert(http::header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
                                L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(sse_bytes.clone()))
                            } else if path.ends_with("64kb") {
                                let mut headers = HeaderMap::new();
                                headers.insert(http::header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
                                L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(payload_64kb.clone()))
                            } else {
                                let mut headers = HeaderMap::new();
                                headers.insert(http::header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
                                L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(payload_10mb.clone()))
                            }
                        } else {
                            // PIPE 1: Buffered
                            if path.ends_with("12kb") {
                                let mut headers = HeaderMap::new();
                                headers.insert(http::header::CONTENT_TYPE, "application/json".parse().unwrap());
                                L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(payload_12kb.clone()))
                            } else if path.ends_with("1mb") {
                                let mut headers = HeaderMap::new();
                                headers.insert(http::header::CONTENT_TYPE, "application/octet-stream".parse().unwrap());
                                L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(payload_1mb.clone()))
                            } else {
                                let mut headers = HeaderMap::new();
                                headers.insert(http::header::CONTENT_TYPE, "text/plain".parse().unwrap());
                                L7Response::new(StatusCode::OK, Version::HTTP_3, headers, Body::Bytes(pong_bytes.clone()))
                            }
                        };

                        if let Ok(out_pkts) = engine.send_response(now, req_event.handle, req_event.stream_id, &resp) {
                            for out in out_pkts {
                                let _ = socket.send_to(&out.payload, out.peer).await;
                            }
                        }
                    }
                }
                _ = ticker.tick() => {
                    let now = Instant::now();
                    let out_pkts = engine.handle_timeouts(now);
                    for out in out_pkts {
                        let _ = socket.send_to(&out.payload, out.peer).await;
                    }
                }
            }
        }
    }
}
