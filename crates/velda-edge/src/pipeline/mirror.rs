//! Fire-and-Forget Verbatim Shadow Mirror Dispatcher for All L7 Protocols (`pipeline::mirror`).
//!
//! ```text
//!   Phase 4 ──► target_mask: u32 = [ mirror: u16 | primary: u16 ]
//!   Phase 5 (`downstream.rs`)
//!     ├─ primary_id (low u16)  ──► normal serving pipe (zero-copy primary path, untouched)
//!     └─ mirror_id  (high u16 != 0xFFFF && != primary_id)  [#[cold]]
//!           ├─ H3 / gRPC-UDP / Empty ──► dispatch_{http,grpc}_mirror() (verbatim L7Request)
//!           └─ H1 / H2 / gRPC-TCP    ──► attach_*_mirror() attaches tee on ingress reader ──► TeeRx mpsc(65)
//!                                                                                              │ (non-blocking)
//!                                                                                              ▼
//!                                                                                 velda-shadow-{i} (pooled acquire)
//!                                                                                   ├─ HttpUpstream::{Http1, Http2, Http3}
//!                                                                                   └─ GrpcUpstream::{Tcp, Udp}
//!   Phase 6+ ──► primary_id (u16 only; mirror response drained & discarded)
//! ```
//!
//! Enforces complete isolation from client latency and zero pipe coupling: mirrored requests are
//! cloned verbatim without injecting extra headers, borrow warm pooled upstream connections via
//! `upstream.acquire()`, and discard errors, timeouts, and response payloads.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use http::header::HeaderValue;
use http::{Method, Uri, Version};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use velda_core::{Body, HeaderWorkspace, L7Request};
use velda_grpc::config::GrpcConfig;
use velda_http1::client::header::sanitize_headers as sanitize_h1_headers;
use velda_http1::client::request::{
    encode_request_head_chunked, send_chunk_vectored, send_request_parts,
};
use velda_http1::client::response::read_response_head;
use velda_http1::config::Http1Config;
use velda_http1::server::connection::{Http1BodyTeeEvent, Http1ServerConnection};
use velda_http1::server::request::{Http1BodyFraming, Http1ServerRequestHead};
use velda_http1::server::response::is_connection_close;
use velda_http1::wire::send_chunked_end;
use velda_http2::server::request::{Http2ServerRequestHead, Http2StreamReceiver, StreamTeeEvent};

use crate::upstream::{
    GrpcUpstream, Http1Upstream, Http2Upstream, Http3Upstream, HttpUpstream, LeaseGuard,
};

/// Bounded per-operation timeout for shadow upstream I/O (`1000 ms`).
const MIRROR_TIMEOUT: Duration = Duration::from_millis(1000);

/// Maximum idle wait between consecutive chunks received from the primary worker.
const MIRROR_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Bounded stream queue capacity: 64 payload chunks + 1 reserved slot for terminal `End`/`Trailers`.
const MIRROR_STREAM_QUEUE_CAP: usize = 65;

enum TeeMsg {
    Data(Bytes),
    Trailers(HeaderWorkspace),
    End,
}

enum TeeRx {
    Http1(mpsc::Receiver<Http1BodyTeeEvent>),
    Http2(mpsc::Receiver<StreamTeeEvent>),
}

impl TeeRx {
    async fn recv(&mut self) -> Option<TeeMsg> {
        match self {
            Self::Http1(rx) => tokio::time::timeout(MIRROR_STREAM_IDLE_TIMEOUT, rx.recv())
                .await
                .ok()?
                .map(|ev| match ev {
                    Http1BodyTeeEvent::Data(b) => TeeMsg::Data(b),
                    Http1BodyTeeEvent::End => TeeMsg::End,
                }),
            Self::Http2(rx) => tokio::time::timeout(MIRROR_STREAM_IDLE_TIMEOUT, rx.recv())
                .await
                .ok()?
                .map(|ev| match ev {
                    StreamTeeEvent::Data(b) => TeeMsg::Data(b),
                    StreamTeeEvent::Trailers(t) => TeeMsg::Trailers(t),
                    StreamTeeEvent::End => TeeMsg::End,
                }),
        }
    }
}

/// Cold Phase-5 helper attaching verbatim HTTP/1.1 body/chunk mirroring at downstream ingress.
#[cold]
#[inline(never)]
pub fn attach_h1_mirror<IO: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut Http1ServerConnection<IO>,
    req_head: &Http1ServerRequestHead,
    framing: Http1BodyFraming,
    upstream: &Arc<HttpUpstream>,
) {
    if framing == Http1BodyFraming::Empty {
        dispatch_http_mirror(
            Arc::clone(upstream),
            req_head.method.clone(),
            req_head.uri.clone(),
            req_head.version,
            req_head.headers.clone(),
            Body::Empty,
        );
        return;
    }

    let (tx, rx) = mpsc::channel(MIRROR_STREAM_QUEUE_CAP);
    let task = Box::pin(stream_http_mirror(
        Arc::clone(upstream),
        req_head.method.clone(),
        req_head.uri.clone(),
        req_head.version,
        req_head.headers.clone(),
        TeeRx::Http1(rx),
    ));
    if velda_plugin::try_dispatch_shadow(task).is_ok() {
        conn.attach_body_tee(tx);
    }
}

/// Cold Phase-5 helper attaching verbatim HTTP/2 stream mirroring on `Http2StreamReceiver`.
#[cold]
#[inline(never)]
pub fn attach_h2_mirror(
    receiver: &mut Http2StreamReceiver,
    req_head: &Http2ServerRequestHead,
    upstream: &Arc<HttpUpstream>,
) {
    if receiver.is_end_stream() {
        dispatch_http_mirror(
            Arc::clone(upstream),
            req_head.method.clone(),
            req_head.uri.clone(),
            Version::HTTP_2,
            req_head.headers.clone(),
            Body::Empty,
        );
        return;
    }

    let (tx, rx) = mpsc::channel(MIRROR_STREAM_QUEUE_CAP);
    let task = Box::pin(stream_http_mirror(
        Arc::clone(upstream),
        req_head.method.clone(),
        req_head.uri.clone(),
        Version::HTTP_2,
        req_head.headers.clone(),
        TeeRx::Http2(rx),
    ));
    if velda_plugin::try_dispatch_shadow(task).is_ok() {
        receiver.attach_tee(tx);
    }
}

/// Cold Phase-5 helper attaching verbatim gRPC-TCP stream mirroring on `Http2StreamReceiver`.
#[cold]
#[inline(never)]
pub fn attach_grpc_tcp_mirror(
    receiver: &mut Http2StreamReceiver,
    req_head: &Http2ServerRequestHead,
    upstream: &Arc<GrpcUpstream>,
) {
    if receiver.is_end_stream() {
        dispatch_grpc_mirror(
            Arc::clone(upstream),
            req_head.method.clone(),
            req_head.uri.clone(),
            Version::HTTP_2,
            req_head.headers.clone(),
            Body::Empty,
        );
        return;
    }

    let (tx, rx) = mpsc::channel(MIRROR_STREAM_QUEUE_CAP);
    let task = Box::pin(stream_grpc_mirror(
        Arc::clone(upstream),
        req_head.method.clone(),
        req_head.uri.clone(),
        Version::HTTP_2,
        req_head.headers.clone(),
        TeeRx::Http2(rx),
    ));
    if velda_plugin::try_dispatch_shadow(task).is_ok() {
        receiver.attach_tee(tx);
    }
}

/// Cold Phase-5 helper offloading a buffered verbatim L7 HTTP mirror request onto `velda-shadow-{i}`.
#[cold]
#[inline(never)]
pub fn dispatch_http_mirror(
    upstream: Arc<HttpUpstream>,
    method: Method,
    uri: Uri,
    version: Version,
    headers: HeaderWorkspace,
    body: Body,
) {
    let task = Box::pin(async move {
        let _ = tokio::time::timeout(
            MIRROR_TIMEOUT,
            send_http_mirror(upstream, method, uri, version, headers, body),
        )
        .await;
    });
    let _ = velda_plugin::try_dispatch_shadow(task);
}

/// Cold Phase-5 helper offloading a buffered verbatim L7 gRPC mirror request onto `velda-shadow-{i}`.
#[cold]
#[inline(never)]
pub fn dispatch_grpc_mirror(
    upstream: Arc<GrpcUpstream>,
    method: Method,
    uri: Uri,
    version: Version,
    headers: HeaderWorkspace,
    body: Body,
) {
    let task = Box::pin(async move {
        let _ = tokio::time::timeout(
            MIRROR_TIMEOUT,
            send_grpc_mirror(upstream, method, uri, version, headers, body),
        )
        .await;
    });
    let _ = velda_plugin::try_dispatch_shadow(task);
}

#[inline]
fn prepare_h1_headers(headers: &mut HeaderWorkspace, uri: &Uri) {
    if !headers.contains_key(http::header::HOST)
        && let Some(host) = velda_http1::server::header::extract_host(headers, uri)
        && let Ok(hv) = HeaderValue::from_str(host)
    {
        headers.insert(http::header::HOST, hv);
    }
    sanitize_h1_headers(headers);
}

#[inline]
fn build_h2_uri(upstream: &Http2Upstream, headers: &HeaderWorkspace, uri: Uri) -> Uri {
    let scheme = if upstream.is_tls() { "https" } else { "http" };
    let authority = headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| uri.host())
        .unwrap_or_else(|| upstream.target_host());
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    Uri::builder()
        .scheme(scheme)
        .authority(authority)
        .path_and_query(path_and_query)
        .build()
        .unwrap_or(uri)
}

async fn drain_h1_response_body(
    lease_guard: &mut LeaseGuard,
    keep_alive: bool,
    framing: Http1BodyFraming,
    read_buf: &mut BytesMut,
    cfg: &Http1Config,
) -> bool {
    let drained = match framing {
        Http1BodyFraming::Empty => true,
        Http1BodyFraming::ContentLength(len) if len <= cfg.max_body_size => {
            while read_buf.len() < len {
                match lease_guard.read_buf(read_buf).await {
                    Ok(0) | Err(_) => return false,
                    Ok(_) => {}
                }
            }
            true
        }
        Http1BodyFraming::Chunked => loop {
            match velda_http1::wire::parse_chunked_body(read_buf, cfg.max_body_size) {
                Ok(Some(_)) => break true,
                Ok(None) => match lease_guard.read_buf(read_buf).await {
                    Ok(0) | Err(_) => break false,
                    Ok(_) => {}
                },
                Err(_) => break false,
            }
        },
        Http1BodyFraming::ContentLength(_) => false,
    };

    drained && keep_alive
}

async fn send_http_mirror(
    upstream: Arc<HttpUpstream>,
    method: Method,
    uri: Uri,
    _version: Version,
    headers: HeaderWorkspace,
    body: Body,
) {
    match upstream.as_ref() {
        HttpUpstream::Http1(u) => send_h1_mirror(u, method, uri, headers, body).await,
        HttpUpstream::Http2(u) => send_h2_mirror(u, method, uri, headers, body).await,
        HttpUpstream::Http3(u) => send_h3_mirror(u, method, uri, headers, body).await,
    }
}

async fn send_h1_mirror(
    upstream: &Arc<Http1Upstream>,
    method: Method,
    uri: Uri,
    mut headers: HeaderWorkspace,
    body: Body,
) {
    prepare_h1_headers(&mut headers, &uri);

    let Ok(lease) = upstream.acquire().await else {
        return;
    };
    let mut lease_guard = LeaseGuard::new(lease);
    let mut write_buf = BytesMut::with_capacity(512);
    let mut read_buf = BytesMut::with_capacity(1024);
    let cfg = Http1Config::auto();

    let mut head_and_framing = {
        let mut ok = send_request_parts(
            &method,
            &uri,
            &headers,
            &body,
            &mut *lease_guard,
            &mut write_buf,
        )
        .await
        .is_ok();
        let mut parsed = if ok {
            read_response_head(&mut *lease_guard, &mut read_buf, &cfg)
                .await
                .ok()
        } else {
            None
        };
        if parsed.is_none()
            && lease_guard.is_reused()
            && let Ok(mut fresh) = upstream.acquire_fresh().await
        {
            read_buf.clear();
            ok = send_request_parts(&method, &uri, &headers, &body, &mut *fresh, &mut write_buf)
                .await
                .is_ok();
            if ok {
                parsed = read_response_head(&mut *fresh, &mut read_buf, &cfg)
                    .await
                    .ok();
            }
            if parsed.is_some() {
                lease_guard.replace(fresh);
            } else {
                fresh.mark_closed();
                return;
            }
        }
        parsed
    };

    let Some((resp_head, framing)) = head_and_framing.take() else {
        return;
    };
    let keep_alive = !is_connection_close(&resp_head.headers);
    if drain_h1_response_body(&mut lease_guard, keep_alive, framing, &mut read_buf, &cfg).await {
        let _ = lease_guard.commit();
    }
}

async fn send_h2_mirror(
    upstream: &Arc<Http2Upstream>,
    method: Method,
    uri: Uri,
    headers: HeaderWorkspace,
    body: Body,
) {
    let built_uri = build_h2_uri(upstream, &headers, uri);

    let cfg = &upstream.config;
    let Ok((client, lease)) = upstream.acquire(cfg).await else {
        return;
    };
    let has_body = !body.is_empty();
    let mut _held_lease = Some(lease);
    let res = match client
        .send_request(
            method.clone(),
            built_uri.clone(),
            headers.clone(),
            !has_body,
        )
        .await
    {
        Ok(pair) => Some(pair),
        Err(ref e) if e.is_refused_or_goaway() => {
            if let Some(old_lease) = _held_lease.take() {
                old_lease.mark_goaway();
            }
            if let Ok((fresh_cli, fresh_lease)) = upstream.acquire_fresh(cfg).await {
                _held_lease = Some(fresh_lease);
                fresh_cli
                    .send_request(method, built_uri, headers, !has_body)
                    .await
                    .ok()
            } else {
                None
            }
        }
        Err(_) => None,
    };

    if let Some((resp_fut, mut send_stream)) = res {
        if has_body {
            let _ = send_stream.send_data(body.to_bytes(), true);
        }
        if let Ok(mut resp) = resp_fut.await {
            while let Ok(Some(_)) = resp.body.recv_chunk().await {}
        }
    }
}

async fn send_h3_mirror(
    upstream: &Arc<Http3Upstream>,
    method: Method,
    uri: Uri,
    mut headers: HeaderWorkspace,
    body: Body,
) {
    velda_http3::headers::sanitize_headers(&mut headers);
    let Ok((client, _lease)) = upstream.acquire(&upstream.config).await else {
        return;
    };
    let l7_req = L7Request::new(method, uri, Version::HTTP_3, headers, body);
    let _ = client.send_request(l7_req).await;
}

async fn send_grpc_mirror(
    upstream: Arc<GrpcUpstream>,
    method: Method,
    uri: Uri,
    _version: Version,
    headers: HeaderWorkspace,
    body: Body,
) {
    match upstream.as_ref() {
        GrpcUpstream::Tcp(u) => {
            let cfg = GrpcConfig::auto();
            let Ok(connector) = u.acquire(&cfg).await else {
                return;
            };
            let has_body = !body.is_empty();
            if let Ok((resp_fut, mut send_stream)) =
                connector.open_stream(method, uri, headers, !has_body).await
            {
                if has_body {
                    let _ = send_stream.send_chunk(body.to_bytes(), true).await;
                }
                if let Ok(mut resp) = resp_fut.await {
                    while let Ok(Some(_)) = resp.body.recv_chunk().await {}
                    let _ = resp.body.trailers().await;
                }
            }
        }
        GrpcUpstream::Udp(u) => {
            let cfg = GrpcConfig::auto();
            let Ok(client) = u.acquire(&cfg).await else {
                return;
            };
            let l7_req = L7Request::new(method, uri, Version::HTTP_3, headers, body);
            let _ = client.send_request(l7_req).await;
        }
        GrpcUpstream::Rdma(_) => {}
    }
}

async fn stream_http_mirror(
    upstream: Arc<HttpUpstream>,
    method: Method,
    uri: Uri,
    _version: Version,
    headers: HeaderWorkspace,
    rx: TeeRx,
) {
    match upstream.as_ref() {
        HttpUpstream::Http1(u) => stream_h1_mirror(u, method, uri, headers, rx).await,
        HttpUpstream::Http2(u) => stream_h2_mirror(u, method, uri, headers, rx).await,
        HttpUpstream::Http3(u) => stream_h3_mirror(u, method, uri, headers, rx).await,
    }
}

async fn stream_h1_mirror(
    upstream: &Arc<Http1Upstream>,
    method: Method,
    uri: Uri,
    mut headers: HeaderWorkspace,
    mut rx: TeeRx,
) {
    prepare_h1_headers(&mut headers, &uri);
    let head = Http1ServerRequestHead::new(method, uri, Version::HTTP_11, headers);
    let mut write_buf = BytesMut::with_capacity(512);
    encode_request_head_chunked(&head, &mut write_buf);

    let Ok(Ok(lease)) = tokio::time::timeout(MIRROR_TIMEOUT, upstream.acquire()).await else {
        return;
    };
    let mut lease_guard = LeaseGuard::new(lease);

    let mut head_sent = matches!(
        tokio::time::timeout(MIRROR_TIMEOUT, lease_guard.write_all(&write_buf)).await,
        Ok(Ok(()))
    );
    if !head_sent
        && lease_guard.is_reused()
        && let Ok(Ok(mut fresh)) =
            tokio::time::timeout(MIRROR_TIMEOUT, upstream.acquire_fresh()).await
    {
        head_sent = matches!(
            tokio::time::timeout(MIRROR_TIMEOUT, fresh.write_all(&write_buf)).await,
            Ok(Ok(()))
        );
        if head_sent {
            lease_guard.replace(fresh);
        } else {
            fresh.mark_closed();
            return;
        }
    }
    if !head_sent {
        return;
    }

    let mut clean_eof = false;
    while let Some(msg) = rx.recv().await {
        match msg {
            TeeMsg::Data(chunk) => {
                if !chunk.is_empty()
                    && !matches!(
                        tokio::time::timeout(
                            MIRROR_TIMEOUT,
                            send_chunk_vectored(&mut *lease_guard, &chunk),
                        )
                        .await,
                        Ok(Ok(()))
                    )
                {
                    return;
                }
            }
            TeeMsg::End | TeeMsg::Trailers(_) => {
                clean_eof = matches!(
                    tokio::time::timeout(MIRROR_TIMEOUT, send_chunked_end(&mut *lease_guard),)
                        .await,
                    Ok(Ok(()))
                );
                break;
            }
        }
    }

    if !clean_eof {
        return;
    }

    let mut read_buf = BytesMut::with_capacity(1024);
    let cfg = Http1Config::auto();
    let Ok(Ok((resp_head, framing))) = tokio::time::timeout(
        MIRROR_TIMEOUT,
        read_response_head(&mut *lease_guard, &mut read_buf, &cfg),
    )
    .await
    else {
        return;
    };
    let keep_alive = !is_connection_close(&resp_head.headers);
    if let Ok(true) = tokio::time::timeout(
        MIRROR_TIMEOUT,
        drain_h1_response_body(&mut lease_guard, keep_alive, framing, &mut read_buf, &cfg),
    )
    .await
    {
        let _ = lease_guard.commit();
    }
}

async fn stream_h2_mirror(
    upstream: &Arc<Http2Upstream>,
    method: Method,
    uri: Uri,
    headers: HeaderWorkspace,
    mut rx: TeeRx,
) {
    let built_uri = build_h2_uri(upstream, &headers, uri);
    let cfg = &upstream.config;
    let Ok(Ok((client, lease))) = tokio::time::timeout(MIRROR_TIMEOUT, upstream.acquire(cfg)).await
    else {
        return;
    };
    let mut _held_lease = Some(lease);
    let open_res = match tokio::time::timeout(
        MIRROR_TIMEOUT,
        client.send_request(method.clone(), built_uri.clone(), headers.clone(), false),
    )
    .await
    {
        Ok(Ok(pair)) => Some(pair),
        Ok(Err(ref e)) if e.is_refused_or_goaway() => {
            if let Some(old_lease) = _held_lease.take() {
                old_lease.mark_goaway();
            }
            if let Ok(Ok((fresh_cli, fresh_lease))) =
                tokio::time::timeout(MIRROR_TIMEOUT, upstream.acquire_fresh(cfg)).await
            {
                _held_lease = Some(fresh_lease);
                tokio::time::timeout(
                    MIRROR_TIMEOUT,
                    fresh_cli.send_request(method, built_uri, headers, false),
                )
                .await
                .ok()
                .and_then(Result::ok)
            } else {
                None
            }
        }
        _ => None,
    };

    let Some((resp_fut, mut send_stream)) = open_res else {
        return;
    };

    let mut clean_eof = false;
    while let Some(msg) = rx.recv().await {
        match msg {
            TeeMsg::Data(chunk) => {
                if !chunk.is_empty()
                    && !matches!(
                        tokio::time::timeout(MIRROR_TIMEOUT, send_stream.send_chunk(chunk, false))
                            .await,
                        Ok(Ok(()))
                    )
                {
                    return;
                }
            }
            TeeMsg::End | TeeMsg::Trailers(_) => {
                clean_eof = send_stream.send_data(Bytes::new(), true).is_ok();
                break;
            }
        }
    }

    if !clean_eof {
        return;
    }

    if let Ok(Ok(mut resp)) = tokio::time::timeout(MIRROR_TIMEOUT, resp_fut).await {
        let _ = tokio::time::timeout(MIRROR_TIMEOUT, async {
            while let Ok(Some(_)) = resp.body.recv_chunk().await {}
        })
        .await;
    }
}

async fn stream_h3_mirror(
    upstream: &Arc<Http3Upstream>,
    method: Method,
    uri: Uri,
    mut headers: HeaderWorkspace,
    mut rx: TeeRx,
) {
    velda_http3::headers::sanitize_headers(&mut headers);
    let Ok(Ok((client, lease))) =
        tokio::time::timeout(MIRROR_TIMEOUT, upstream.acquire(&upstream.config)).await
    else {
        return;
    };
    let mut _held_lease = Some(lease);

    let open_res = match tokio::time::timeout(
        MIRROR_TIMEOUT,
        client.open_stream(method.clone(), uri.clone(), headers.clone(), None, false),
    )
    .await
    {
        Ok(Ok(pair)) => Some(pair),
        Ok(Err(ref e)) if e.is_connection_closed() => {
            if let Some(old_lease) = _held_lease.take() {
                old_lease.mark_goaway();
            }
            if let Ok(Ok((fresh_client, fresh_lease))) =
                tokio::time::timeout(MIRROR_TIMEOUT, upstream.acquire_fresh(&upstream.config)).await
            {
                _held_lease = Some(fresh_lease);
                tokio::time::timeout(
                    MIRROR_TIMEOUT,
                    fresh_client.open_stream(method, uri, headers, None, false),
                )
                .await
                .ok()
                .and_then(Result::ok)
            } else {
                None
            }
        }
        _ => None,
    };

    let Some((sender, response_rx)) = open_res else {
        return;
    };

    let mut clean_eof = false;
    while let Some(msg) = rx.recv().await {
        match msg {
            TeeMsg::Data(chunk) => {
                if !chunk.is_empty()
                    && !matches!(
                        tokio::time::timeout(MIRROR_TIMEOUT, sender.send_chunk(chunk)).await,
                        Ok(Ok(()))
                    )
                {
                    return;
                }
            }
            TeeMsg::End | TeeMsg::Trailers(_) => {
                clean_eof = matches!(
                    tokio::time::timeout(MIRROR_TIMEOUT, sender.finish()).await,
                    Ok(Ok(()))
                );
                break;
            }
        }
    }

    if !clean_eof {
        return;
    }

    if let Ok(Ok(Ok(mut resp_stream))) = tokio::time::timeout(MIRROR_TIMEOUT, response_rx).await {
        let _ = tokio::time::timeout(MIRROR_TIMEOUT, async {
            while let Some(Ok(_)) = resp_stream.next_chunk().await {}
        })
        .await;
    }
}

async fn stream_grpc_mirror(
    upstream: Arc<GrpcUpstream>,
    method: Method,
    uri: Uri,
    _version: Version,
    headers: HeaderWorkspace,
    mut rx: TeeRx,
) {
    match upstream.as_ref() {
        GrpcUpstream::Tcp(u) => {
            let cfg = GrpcConfig::auto();
            let Ok(Ok(connector)) = tokio::time::timeout(MIRROR_TIMEOUT, u.acquire(&cfg)).await
            else {
                return;
            };
            let Ok(Ok((resp_fut, mut send_stream))) = tokio::time::timeout(
                MIRROR_TIMEOUT,
                connector.open_stream(method, uri, headers, false),
            )
            .await
            else {
                return;
            };

            let mut clean_eof = false;
            while let Some(msg) = rx.recv().await {
                match msg {
                    TeeMsg::Data(chunk) => {
                        if !chunk.is_empty()
                            && !matches!(
                                tokio::time::timeout(
                                    MIRROR_TIMEOUT,
                                    send_stream.send_chunk(chunk, false),
                                )
                                .await,
                                Ok(Ok(()))
                            )
                        {
                            return;
                        }
                    }
                    TeeMsg::Trailers(trailers) => {
                        clean_eof = send_stream.send_trailers(trailers).is_ok();
                        break;
                    }
                    TeeMsg::End => {
                        clean_eof = matches!(
                            tokio::time::timeout(
                                MIRROR_TIMEOUT,
                                send_stream.send_chunk(Bytes::new(), true),
                            )
                            .await,
                            Ok(Ok(()))
                        );
                        break;
                    }
                }
            }

            if !clean_eof {
                return;
            }

            if let Ok(Ok(mut resp)) = tokio::time::timeout(MIRROR_TIMEOUT, resp_fut).await {
                let _ = tokio::time::timeout(MIRROR_TIMEOUT, async {
                    while let Ok(Some(_)) = resp.body.recv_chunk().await {}
                    let _ = resp.body.trailers().await;
                })
                .await;
            }
        }
        GrpcUpstream::Udp(u) => {
            let mut acc = BytesMut::new();
            let mut clean_eof = false;
            while let Some(msg) = rx.recv().await {
                match msg {
                    TeeMsg::Data(chunk) => acc.extend_from_slice(&chunk),
                    TeeMsg::End | TeeMsg::Trailers(_) => {
                        clean_eof = true;
                        break;
                    }
                }
            }
            if !clean_eof {
                return;
            }
            let cfg = GrpcConfig::auto();
            let Ok(Ok(client)) = tokio::time::timeout(MIRROR_TIMEOUT, u.acquire(&cfg)).await else {
                return;
            };
            let l7_req = L7Request::new(
                method,
                uri,
                Version::HTTP_3,
                headers,
                Body::Bytes(acc.freeze()),
            );
            let _ = tokio::time::timeout(MIRROR_TIMEOUT, client.send_request(l7_req)).await;
        }
        GrpcUpstream::Rdma(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tokio::task::JoinHandle;
    use velda_sync::post_sync::upstream::{
        EndpointConfig, LoadBalancerConfig, UpstreamConfig, UpstreamProtocolConfig,
        UpstreamTimeouts as SyncTimeouts,
    };

    async fn spawn_mock_mirror_upstream(
        eof_marker: &'static [u8],
    ) -> (
        Arc<HttpUpstream>,
        oneshot::Receiver<String>,
        JoinHandle<()>,
        JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (done_tx, done_rx) = oneshot::channel::<String>();

        let srv = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req_bytes = Vec::with_capacity(1024);
            let mut tmp = [0u8; 512];
            loop {
                let n = sock.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                req_bytes.extend_from_slice(&tmp[..n]);
                if req_bytes.windows(eof_marker.len()).any(|w| w == eof_marker) {
                    break;
                }
            }
            let _ = sock
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                .await;
            let _ = done_tx.send(String::from_utf8_lossy(&req_bytes).into_owned());
        });

        let up_cfg = UpstreamConfig {
            id: "orders-staging".to_string(),
            mode: "static".to_string(),
            protocol: UpstreamProtocolConfig {
                transport: "tcp".to_string(),
                application: "http1".to_string(),
                streaming: velda_core::StreamingMode::disabled(),
            },
            target: None,
            resolver: None,
            endpoints: vec![EndpointConfig {
                address: format!("127.0.0.1:{port}"),
                weight: 1,
            }],
            load_balancer: LoadBalancerConfig {
                algorithm: "round_robin".to_string(),
            },
            timeouts: SyncTimeouts {
                connect_ms: 1000,
                idle_ms: 5000,
                request_ms: Some(1000),
            },
            health_check: None,
            tls: None,
            pool: None,
            buffer: None,
        };
        let table = crate::upstream::build_upstreams_default(&[up_cfg], None);
        let mirror_up = table.get_http(0).unwrap().clone();

        let (tx, mut rx) = mpsc::channel(8);
        velda_plugin::set_core_shadow_sender(tx);
        let shadow_runner = tokio::spawn(async move {
            if let Some(task) = rx.recv().await {
                task.await;
            }
        });

        (mirror_up, done_rx, shadow_runner, srv)
    }

    #[tokio::test]
    async fn test_dispatch_http1_mirror_delivers_verbatim_body_without_mutating_source() {
        let (mirror_up, done_rx, shadow_runner, srv) =
            spawn_mock_mirror_upstream(br#""order":42}"#).await;

        let mut primary_headers = HeaderWorkspace::empty(2);
        primary_headers.insert(
            http::header::HOST,
            HeaderValue::from_static("api.prod.internal"),
        );

        dispatch_http_mirror(
            mirror_up,
            Method::POST,
            Uri::from_static("/api/v1/orders"),
            Version::HTTP_11,
            primary_headers.clone(),
            Body::Bytes(Bytes::from_static(br#"{"order":42}"#)),
        );

        assert!(!primary_headers.contains_key("x-velda-mirror"));

        let raw_req = tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("mirror server timed out")
            .expect("channel dropped");

        assert!(raw_req.starts_with("POST /api/v1/orders HTTP/1.1\r\n"));
        assert!(!raw_req.to_ascii_lowercase().contains("x-velda-mirror:"));
        assert!(raw_req.ends_with(r#"{"order":42}"#));

        shadow_runner.abort();
        srv.abort();
    }

    #[tokio::test]
    async fn test_attach_h1_mirror_streams_verbatim_chunked_delivery() {
        use velda_http1::server::connection::{tee_chunk, tee_end};

        let (mirror_up, done_rx, shadow_runner, srv) =
            spawn_mock_mirror_upstream(b"0\r\n\r\n").await;

        let mut primary_headers = HeaderWorkspace::empty(2);
        primary_headers.insert(
            http::header::HOST,
            HeaderValue::from_static("upload.prod.internal"),
        );

        let (client_io, _peer_io) = tokio::io::duplex(64);
        let mut conn = Http1ServerConnection::new(client_io, Http1Config::auto());
        let req_head = Http1ServerRequestHead::new(
            Method::POST,
            Uri::from_static("/upload"),
            Version::HTTP_11,
            primary_headers.clone(),
        );

        attach_h1_mirror(&mut conn, &req_head, Http1BodyFraming::Chunked, &mirror_up);

        assert!(!primary_headers.contains_key("x-velda-mirror"));

        tee_chunk(&mut conn.body_tee, Bytes::from_static(b"chunk-1-"));
        tee_chunk(&mut conn.body_tee, Bytes::from_static(b"chunk-2-"));
        tee_chunk(&mut conn.body_tee, Bytes::from_static(b"chunk-3"));
        tee_end(&mut conn.body_tee);

        let raw_req = tokio::time::timeout(Duration::from_secs(2), done_rx)
            .await
            .expect("mirror server timed out")
            .expect("channel dropped");

        assert!(raw_req.starts_with("POST /upload HTTP/1.1\r\n"));
        assert!(!raw_req.to_ascii_lowercase().contains("x-velda-mirror:"));
        assert!(
            raw_req
                .to_ascii_lowercase()
                .contains("transfer-encoding: chunked\r\n")
        );
        assert!(raw_req.contains("chunk-1-"));
        assert!(raw_req.contains("chunk-2-"));
        assert!(raw_req.contains("chunk-3"));
        assert!(raw_req.ends_with("0\r\n\r\n"));

        shadow_runner.abort();
        srv.abort();
    }
}
