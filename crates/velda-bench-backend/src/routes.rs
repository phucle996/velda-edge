//! 8 Canonical Benchmark Routes across 4 Streaming Pipeline Strategies (Plain & TLS).
//!
//! 100% in-memory, zero disk I/O, deterministic real streaming backpressure:
//! - Buffered (Plain/TLS): Micro (64B ping), Medium (12KB JSON), Large (1MB RAM blob).
//! - ServerStream (Plain/TLS): SSE heartbeat, 64KB chunked, 10MB streaming file.
//! - ClientStream (Plain/TLS): Consumes 100% of incoming body frames, returns byte count.
//! - Duplex (Plain/TLS): Bidirectional real-time chunk echo.

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use std::convert::Infallible;
use std::sync::LazyLock;

pub type RespBody = BoxBody<Bytes, Infallible>;

// Pre-allocated static test payloads (zero allocation in hot path)
static PING_RESP: Bytes = Bytes::from_static(b"pong");
static SSE_PING_CHUNK: Bytes = Bytes::from_static(b"event: ping\ndata: heartbeat\n\n");
static PAYLOAD_12KB: LazyLock<Bytes> = LazyLock::new(|| {
    let mut data = String::with_capacity(12 * 1024);
    data.push_str("{\"status\":\"ok\",\"payload\":[");
    while data.len() < 12 * 1024 - 32 {
        data.push_str("{\"item\":\"benchmark_json_token_stream\"},");
    }
    data.push_str("{\"last\":true}]}");
    Bytes::from(data)
});
static PAYLOAD_1MB: LazyLock<Bytes> = LazyLock::new(|| Bytes::from(vec![0xAA; 1024 * 1024]));
static CHUNK_64KB: LazyLock<Bytes> = LazyLock::new(|| Bytes::from(vec![0x55; 64 * 1024]));
static CHUNK_8KB: LazyLock<Bytes> = LazyLock::new(|| Bytes::from(vec![0x77; 8 * 1024]));

fn full_body(b: Bytes) -> RespBody {
    Full::new(b).map_err(|e| match e {}).boxed()
}

/// Routes an incoming request to one of the 8 canonical routes based on pipe and TLS mode.
pub async fn dispatch(req: Request<Incoming>, is_tls: bool) -> Response<RespBody> {
    let path = req.uri().path();
    let method = req.method().clone();

    // ─────────────────────────────────────────────────────────────────────────
    // PIPE 3: ClientStream (Upload) — Plain & TLS
    // Consumes 100% of request body frames before responding!
    // ─────────────────────────────────────────────────────────────────────────
    if method == Method::POST && (path.contains("upload") || path.ends_with("/upload")) {
        let is_plain_route = !path.contains("/tls/") && !is_tls;
        let pipe_name = if is_plain_route {
            "client_stream_plain"
        } else {
            "client_stream_tls"
        };

        let mut body = req.into_body();
        let mut total_bytes: u64 = 0;
        while let Some(frame_res) = body.frame().await {
            if let Ok(frame) = frame_res {
                if let Some(data) = frame.data_ref() {
                    total_bytes += data.len() as u64;
                }
            } else {
                break;
            }
        }

        let resp_json = format!(
            "{{\"status\":\"ok\",\"pipe\":\"{}\",\"received_bytes\":{}}}\n",
            pipe_name, total_bytes
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(resp_json)))
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PIPE 4: Duplex (Bidirectional Echo) — Plain & TLS
    // Reads chunks as they arrive and echoes them back in real time!
    // ─────────────────────────────────────────────────────────────────────────
    if method == Method::POST && (path.contains("duplex") || path.contains("echo")) {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(16);
        let mut in_body = req.into_body();

        tokio::spawn(async move {
            while let Some(frame_res) = in_body.frame().await {
                if let Ok(frame) = frame_res
                    && let Ok(data) = frame.into_data()
                    && tx.send(Ok(Frame::data(data))).await.is_err()
                {
                    break;
                }
            }
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(StreamBody::new(stream).boxed())
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PIPE 2: ServerStream (Progressive Streaming) — Plain & TLS
    // ─────────────────────────────────────────────────────────────────────────
    if path.contains("stream") || path.contains("sse") {
        if path.ends_with("sse-ping") || path.ends_with("sse") {
            let chunk = SSE_PING_CHUNK.clone();
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(4);
            tokio::spawn(async move {
                let _ = tx.send(Ok(Frame::data(chunk))).await;
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .body(StreamBody::new(stream).boxed())
                .unwrap();
        }

        if path.ends_with("64kb") {
            // Streams 64KB as 8 x 8KB chunks
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(8);
            tokio::spawn(async move {
                for _ in 0..8 {
                    if tx.send(Ok(Frame::data(CHUNK_8KB.clone()))).await.is_err() {
                        break;
                    }
                }
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(StreamBody::new(stream).boxed())
                .unwrap();
        }

        if path.ends_with("10mb") {
            // Streams 10MB as 160 x 64KB chunks (zero disk I/O, pure memory backpressure)
            let (tx, rx) = tokio::sync::mpsc::channel::<Result<Frame<Bytes>, Infallible>>(8);
            tokio::spawn(async move {
                for _ in 0..160 {
                    if tx.send(Ok(Frame::data(CHUNK_64KB.clone()))).await.is_err() {
                        break;
                    }
                }
            });
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
            return Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(StreamBody::new(stream).boxed())
                .unwrap();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // PIPE 1: Buffered (RAM In-Memory Static Payloads) — Plain & TLS
    // ─────────────────────────────────────────────────────────────────────────
    if path.ends_with("12kb") {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(PAYLOAD_12KB.clone()))
            .unwrap();
    }

    if path.ends_with("1mb") {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(full_body(PAYLOAD_1MB.clone()))
            .unwrap();
    }

    // Default /ping or root
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain")
        .body(full_body(PING_RESP.clone()))
        .unwrap()
}
