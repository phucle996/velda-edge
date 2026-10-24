use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use http::header::CONTENT_TYPE;
use http::{Method, StatusCode, Uri};
use quinn_proto::{ClientConfig, ServerConfig};
use velda_core::{Body, L7Response};
use velda_http3::Http3Engine;
use velda_http3::frame::{Http3Frame, decode_frame, encode_frame};

fn generate_test_crypto() -> (ServerConfig, ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.signing_key.serialize_der();

    let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der.clone())];
    let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
    );

    let mut rustls_server = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(cert_chain, private_key)
    .unwrap();
    rustls_server.alpn_protocols = vec![b"h3".to_vec()];

    let quic_server_crypto =
        quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server)).unwrap();
    let server_config = ServerConfig::with_crypto(Arc::new(quic_server_crypto));

    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(rustls::pki_types::CertificateDer::from(cert_der))
        .unwrap();

    let mut rustls_client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth();
    rustls_client.alpn_protocols = vec![b"h3".to_vec()];

    let quic_client_crypto =
        quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client)).unwrap();
    let client_config = ClientConfig::new(Arc::new(quic_client_crypto));

    (server_config, client_config)
}

async fn spawn_echo_server(
    mut engine: Http3Engine,
) -> (
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let addr = socket.local_addr().unwrap();
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();

    let s_socket = socket.clone();
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            tokio::select! {
                _ = &mut rx => break,
                res = s_socket.recv_from(&mut buf) => {
                    let (len, remote) = match res {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    let now = Instant::now();
                    let (outgoing, requests) = engine.handle_datagram(now, remote, None, &buf[..len]);
                    for out in outgoing {
                        if let Some(seg) = out.segment_size {
                            for chunk in out.payload.chunks(seg as usize) {
                                let _ = s_socket.send_to(chunk, out.peer).await;
                            }
                        } else {
                            let _ = s_socket.send_to(&out.payload, out.peer).await;
                        }
                    }
                    for req_event in requests {
                        let path = req_event.request.uri.path();
                        let resp = match path {
                            "/head" => {
                                let mut h = http::HeaderMap::new();
                                h.insert("content-length", "1024".parse().unwrap());
                                L7Response::new(StatusCode::OK, http::Version::HTTP_3, h, Body::Empty)
                            }
                            "/no-content" => {
                                L7Response::from_bytes(StatusCode::NO_CONTENT, Vec::new())
                            }
                            _ => {
                                let body = if req_event.request.body.is_empty() {
                                    Bytes::from_static(b"ok")
                                } else {
                                    req_event.request.body.to_bytes()
                                };
                                L7Response::new(
                                    StatusCode::OK,
                                    http::Version::HTTP_3,
                                    http::HeaderMap::new(),
                                    Body::Bytes(body),
                                )
                            }
                        };
                        if let Ok(resp_outgoing) = engine.send_response(now, req_event.handle, req_event.stream_id, &resp) {
                            for out in resp_outgoing {
                                if let Some(seg) = out.segment_size {
                                    for chunk in out.payload.chunks(seg as usize) {
                                        let _ = s_socket.send_to(chunk, out.peer).await;
                                    }
                                } else {
                                    let _ = s_socket.send_to(&out.payload, out.peer).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    (addr, tx, handle)
}

#[test]
fn test_http3_frame_roundtrip() {
    let frame = Http3Frame::Data(Bytes::from_static(b"quic datagram payload"));
    let mut buf = bytes::BytesMut::new();
    encode_frame(&frame, &mut buf);

    let decoded = decode_frame(&mut buf).unwrap().unwrap();
    assert_eq!(decoded, frame);
    assert!(buf.is_empty());
}

#[test]
fn test_http3_engine_adversarial_flood() {
    let (server_config, _) = generate_test_crypto();
    let mut engine = Http3Engine::new(Arc::new(server_config));
    let remote: SocketAddr = "192.0.2.1:54321".parse().unwrap();

    // Flood engine with 5,000 pseudo-random corrupted UDP packets
    for i in 0..5_000 {
        let mut corrupted = vec![0u8; (i % 512) + 1];
        for (idx, b) in corrupted.iter_mut().enumerate() {
            *b = ((i * 31 + idx * 17) & 0xff) as u8;
        }
        let now = Instant::now();
        let (outgoing, requests) = engine.handle_datagram(now, remote, None, &corrupted);
        assert_eq!(requests.len(), 0);
        let _ = outgoing;
    }

    assert_eq!(engine.connection_count(), 0);
    assert_eq!(engine.active_stream_count(), 0);
}

#[tokio::test]
async fn test_http3_roundtrip_requests() {
    let (server_config, _) = generate_test_crypto();
    let engine = Http3Engine::new(Arc::new(server_config));
    let (server_addr, shutdown_tx, server_task) = spawn_echo_server(engine).await;

    let config = velda_http3::Http3Config::auto();
    let client = velda_http3::connect(
        server_addr,
        "localhost",
        &config,
        velda_http3::default_client_config().unwrap(),
    )
    .await
    .expect("Client failed to connect to HTTP/3 server");

    // 1. Unary GET request
    let get_req = velda_core::L7Request {
        method: Method::GET,
        uri: "/get".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: velda_core::HeaderWorkspace::empty(),
        body: Body::Empty,
    };
    let get_resp = client.send_request(get_req).await.unwrap();
    assert_eq!(get_resp.status, StatusCode::OK);
    assert_eq!(get_resp.body, Body::Bytes(Bytes::from_static(b"ok")));

    // 2. POST request with body (multi-chunk streaming upload roundtrip)
    let payload = vec![0xa5u8; 65_536];
    let post_req = velda_core::L7Request {
        method: Method::POST,
        uri: "/upload".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: {
            let mut h = http::HeaderMap::new();
            h.insert(CONTENT_TYPE, "application/octet-stream".parse().unwrap());
            h.into()
        },
        body: Body::Bytes(Bytes::from(payload.clone())),
    };
    let post_resp = client.send_request(post_req).await.unwrap();
    assert_eq!(post_resp.status, StatusCode::OK);
    if let Body::Bytes(b) = post_resp.body {
        assert_eq!(&b[..], &payload[..]);
    } else {
        panic!("Expected Bytes body");
    }

    // 3. Status invariants: HEAD (no body) & 204 No Content
    let head_req = velda_core::L7Request {
        method: Method::HEAD,
        uri: "/head".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: velda_core::HeaderWorkspace::empty(),
        body: Body::Empty,
    };
    let head_resp = client.send_request(head_req).await.unwrap();
    assert_eq!(head_resp.status, StatusCode::OK);
    assert_eq!(head_resp.headers.get("content-length").unwrap(), "1024");
    assert_eq!(head_resp.body, Body::Empty);

    let no_content_req = velda_core::L7Request {
        method: Method::DELETE,
        uri: "/no-content".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: velda_core::HeaderWorkspace::empty(),
        body: Body::Empty,
    };
    let no_content_resp = client.send_request(no_content_req).await.unwrap();
    assert_eq!(no_content_resp.status, StatusCode::NO_CONTENT);
    assert_eq!(no_content_resp.body, Body::Empty);

    // 4. Multiplexed concurrent requests across shared client connection
    let mut handles = Vec::new();
    for i in 0..10 {
        let client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            let req = velda_core::L7Request {
                method: Method::POST,
                uri: format!("/concurrent-{}", i).parse::<Uri>().unwrap(),
                version: http::Version::HTTP_3,
                headers: velda_core::HeaderWorkspace::empty(),
                body: Body::Bytes(Bytes::from(format!("payload-{}", i))),
            };
            let resp = client_clone.send_request(req).await.unwrap();
            assert_eq!(resp.status, StatusCode::OK);
            if let Body::Bytes(b) = resp.body {
                assert_eq!(&b[..], format!("payload-{}", i).as_bytes());
            } else {
                panic!("Expected Bytes body");
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    client.close().await;
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}

#[tokio::test]
async fn test_http3_payload_limit_enforced() {
    let (server_config, _) = generate_test_crypto();
    let mut s_cfg = velda_http3::Http3Config::auto();
    s_cfg.max_body_size = 512;
    let engine = Http3Engine::new_with_config(Arc::new(server_config), s_cfg);
    let (server_addr, shutdown_tx, server_task) = spawn_echo_server(engine).await;

    let client_cfg = velda_http3::Http3Config::auto();
    let client = velda_http3::connect(
        server_addr,
        "localhost",
        &client_cfg,
        velda_http3::default_client_config().unwrap(),
    )
    .await
    .expect("Client failed to connect to HTTP/3 server");

    // 1. Send oversized bomb payload exceeding max_body_size
    let bomb_payload = vec![0x42u8; 128 * 1024];
    let bomb_req = velda_core::L7Request {
        method: Method::POST,
        uri: "/bomb".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: velda_core::HeaderWorkspace::empty(),
        body: Body::Bytes(Bytes::from(bomb_payload)),
    };
    let bomb_res = client.send_request(bomb_req).await;
    assert!(bomb_res.is_err(), "Oversized body must be rejected");

    // 2. Subsequent valid request over the same connection succeeds cleanly
    let valid_req = velda_core::L7Request {
        method: Method::GET,
        uri: "/valid".parse::<Uri>().unwrap(),
        version: http::Version::HTTP_3,
        headers: velda_core::HeaderWorkspace::empty(),
        body: Body::Empty,
    };
    let valid_resp = client.send_request(valid_req).await.unwrap();
    assert_eq!(valid_resp.status, StatusCode::OK);

    client.close().await;
    let _ = shutdown_tx.send(());
    let _ = server_task.await;
}
