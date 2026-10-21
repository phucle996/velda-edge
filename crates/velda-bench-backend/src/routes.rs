//! 8 Canonical Benchmark Routes across 4 Streaming Pipeline Strategies (Plain & TLS).
//!
//! 100% in-memory, zero disk I/O, deterministic real streaming backpressure:
//! - Buffered (Plain/TLS): Micro (64B ping), Medium (12KB JSON), Large (1MB RAM blob).
//! - ServerStream (Plain/TLS): SSE heartbeat, 64KB chunked, 10MB streaming file.
//! - ClientStream (Plain/TLS): Consumes 100% of incoming body frames, returns byte count.
//! - Duplex (Plain/TLS): Bidirectional real-time chunk echo.

use base64::prelude::*;
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use ring::hmac;
use std::convert::Infallible;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub type RespBody = BoxBody<Bytes, Infallible>;

// Pre-allocated static test payloads (zero allocation in hot path)
static PING_RESP: Bytes = Bytes::from_static(b"pong");
static SSE_PING_CHUNK: Bytes = Bytes::from_static(b"event: ping\ndata: heartbeat\n\n");
static JWT_SECRET: &[u8] = b"velda-secret-key-2026";
static MIRROR_COUNT: AtomicU64 = AtomicU64::new(0);

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
static COMPRESS_PAYLOAD: LazyLock<Bytes> = LazyLock::new(|| {
    let mut data = String::with_capacity(32 * 1024);
    data.push_str(
        "{\"status\":\"ok\",\"description\":\"compressible_json_stream_benchmark\",\"items\":[",
    );
    while data.len() < 32 * 1024 - 32 {
        data.push_str("{\"id\":\"velda_compression_test_chunk_2026\",\"status\":\"active\",\"padding\":\"abcdef1234567890\"},");
    }
    data.push_str("{\"end\":true}]}");
    Bytes::from(data)
});
static DYNAMIC_BUFFER: LazyLock<Bytes> = LazyLock::new(|| {
    let mut data = Vec::with_capacity(2 * 1024 * 1024);
    while data.len() < 2 * 1024 * 1024 {
        data.extend_from_slice(b"{\"id\":12345,\"status\":\"active\",\"data\":\"abcdefghijklmnopqrstuvwxyz0123456789\"}\n");
    }
    data.truncate(2 * 1024 * 1024);
    Bytes::from(data)
});

/// Signs an RFC 7519 compliant HS256 JWT for testing jwt_auth plugin.
fn generate_jwt(sub: &str, role: &str, exp_delta_secs: i64, tamper_sig: bool) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let exp = now + exp_delta_secs;

    let header_json = r#"{"alg":"HS256","typ":"JWT"}"#;
    let payload_json = format!(
        r#"{{"sub":"{}","role":"{}","iss":"velda-auth","exp":{}}}"#,
        sub, role, exp
    );

    let h_b64 = BASE64_URL_SAFE_NO_PAD.encode(header_json.as_bytes());
    let p_b64 = BASE64_URL_SAFE_NO_PAD.encode(payload_json.as_bytes());
    let signing_input = format!("{h_b64}.{p_b64}");

    let s_key = hmac::Key::new(hmac::HMAC_SHA256, JWT_SECRET);
    let mut sig = hmac::sign(&s_key, signing_input.as_bytes())
        .as_ref()
        .to_vec();
    if tamper_sig && !sig.is_empty() {
        sig[0] ^= 0xFF; // Invert first byte to invalidate signature
    }
    let s_b64 = BASE64_URL_SAFE_NO_PAD.encode(&sig);

    format!("{signing_input}.{s_b64}")
}

fn full_body(b: Bytes) -> RespBody {
    Full::new(b).map_err(|e| match e {}).boxed()
}

/// Routes an incoming request to one of the 8 canonical routes based on pipe and TLS mode.
pub async fn dispatch(req: Request<Incoming>, is_tls: bool) -> Response<RespBody> {
    let uri = req.uri();
    let path = uri.path();
    let method = req.method().clone();

    // ─────────────────────────────────────────────────────────────────────────
    // AUTH SERVICE: JWT Issuance & Key Management for jwt_auth & api_key_auth
    // ─────────────────────────────────────────────────────────────────────────
    if path == "/auth/token" || path.ends_with("/auth/token") {
        let token = generate_jwt("user_velda_996", "admin", 3600, false);
        let resp = format!(
            "{{\"token\":\"{}\",\"type\":\"Bearer\",\"expires_in\":3600}}\n",
            token
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(resp)))
            .unwrap();
    }

    if path.ends_with("/auth/token/expired") {
        let token = generate_jwt("user_velda_996", "admin", -3600, false);
        let resp = format!(
            "{{\"token\":\"{}\",\"type\":\"Bearer\",\"expired\":true}}\n",
            token
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(resp)))
            .unwrap();
    }

    if path.ends_with("/auth/token/invalid-sig") {
        let token = generate_jwt("user_velda_996", "admin", 3600, true);
        let resp = format!(
            "{{\"token\":\"{}\",\"type\":\"Bearer\",\"invalid\":true}}\n",
            token
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(resp)))
            .unwrap();
    }

    if path.ends_with("/auth/api-key") {
        let resp =
            r#"{"api_key":"velda-api-key-secret-2026","consumer_id":"consumer_enterprise_01"}"#;
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from_static(resp.as_bytes())))
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // INSPECTION & ECHO: Reflects request method, path, and all headers
    // ─────────────────────────────────────────────────────────────────────────
    if path == "/inspect" || path.ends_with("/inspect") || path.ends_with("/echo/headers") {
        let mut hdrs = serde_json::Map::new();
        for (k, v) in req.headers() {
            if let Ok(s) = v.to_str() {
                hdrs.insert(
                    k.as_str().to_string(),
                    serde_json::Value::String(s.to_string()),
                );
            }
        }
        let json = serde_json::json!({
            "status": "ok",
            "method": method.as_str(),
            "path": path,
            "headers": hdrs,
        });
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(json.to_string())))
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // RFC 9111 HTTP CACHING: Cacheable & Non-Cacheable Endpoints
    // ─────────────────────────────────────────────────────────────────────────
    if path.ends_with("/cacheable") || path.ends_with("/cache/public") {
        if req
            .headers()
            .get(header::IF_NONE_MATCH)
            .and_then(|v| v.to_str().ok())
            == Some("\"v1-cache-etag\"")
        {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header(header::ETAG, "\"v1-cache-etag\"")
                .header(header::CACHE_CONTROL, "public, max-age=60")
                .body(full_body(Bytes::new()))
                .unwrap();
        }
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ETAG, "\"v1-cache-etag\"")
            .header(header::CACHE_CONTROL, "public, max-age=60")
            .header(header::LAST_MODIFIED, "Tue, 20 Oct 2026 00:00:00 GMT")
            .body(full_body(Bytes::from_static(
                b"{\"status\":\"cached\",\"item\":\"velda-cache-data-2026\"}\n",
            )))
            .unwrap();
    }

    if path.ends_with("/cache/no-store") || path.ends_with("/no-cache") {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store, no-cache")
            .body(full_body(Bytes::from_static(
                b"{\"status\":\"not_cached\",\"item\":\"ephemeral_data\"}\n",
            )))
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // TRAFFIC MIRRORING: Shadow Target & Counter
    // ─────────────────────────────────────────────────────────────────────────
    if path == "/mirror" || path.ends_with("/mirror") || path.ends_with("/mirror/target") {
        MIRROR_COUNT.fetch_add(1, Ordering::Relaxed);
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from_static(
                b"{\"status\":\"mirrored\"}\n",
            )))
            .unwrap();
    }

    if path.ends_with("/mirror/stats") {
        let count = MIRROR_COUNT.load(Ordering::Relaxed);
        let resp = format!("{{\"mirrored_requests\":{count}}}\n");
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(Bytes::from(resp)))
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // COMPRESSIBLE LARGE PAYLOAD: For Response Compression Testing
    // ─────────────────────────────────────────────────────────────────────────
    if path.ends_with("/compress/large") || path.ends_with("/compress/json") {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(COMPRESS_PAYLOAD.clone()))
            .unwrap();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // DYNAMIC CONTINUOUS SLICE: /bytes/<N> or ?bytes=<N> (1B to 2MB)
    // Zero allocation, instant O(1) RAM slicing for continuous random production simulation
    // ─────────────────────────────────────────────────────────────────────────
    let mut dynamic_bytes: Option<usize> = None;
    if let Some(query) = uri.query() {
        for part in query.split('&') {
            if let Some(val) = part.strip_prefix("bytes=")
                && let Ok(n) = val.parse::<usize>()
            {
                dynamic_bytes = Some(n.clamp(1, 2 * 1024 * 1024));
                break;
            }
        }
    }
    if dynamic_bytes.is_none()
        && let Some(pos) = path.find("/bytes/")
    {
        let sub = &path[pos + 7..];
        let num_str = sub.split('/').next().unwrap_or(sub);
        if let Ok(n) = num_str.parse::<usize>() {
            dynamic_bytes = Some(n.clamp(1, 2 * 1024 * 1024));
        }
    }
    if let Some(n) = dynamic_bytes {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(full_body(DYNAMIC_BUFFER.slice(..n)))
            .unwrap();
    }

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
