use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};
use velda_etp::arena::{ArenaControlLine, ArenaSlot, PAGE_SIZE};
use velda_etp::connect::{ColdTargetAddr, EtpStream, sha256_u64x4};
use velda_etp::kafka::KafkaEtp;
use velda_etp::otlp::{OtlpConn, OtlpEtp};
use velda_etp::registry::{build_registry, reconcile, warm_all, warm_instance};
use velda_etp::{
    EtpBackend, EtpEntryConfig, EtpError, EtpKind, EtpPoolSettings, EtpRegistry, EtpTlsConfig,
    EtpTlsVersion, EtpTransportSecurity,
};

fn make_entry(
    id: &str,
    kind: EtpKind,
    port: u16,
    pool_size: usize,
    extra: serde_json::Value,
) -> EtpEntryConfig {
    let mut settings = serde_json::json!({
        "host": "127.0.0.1",
        "port": port,
    });
    if let (Some(base), Some(ext)) = (settings.as_object_mut(), extra.as_object()) {
        for (k, v) in ext {
            base.insert(k.clone(), v.clone());
        }
    }
    EtpEntryConfig {
        id: id.to_string(),
        kind,
        security: EtpTransportSecurity::Plain,
        pool: EtpPoolSettings {
            pool_size,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: None,
        settings,
    }
}

async fn spawn_mock_redis_server(
    expected_password: &str,
    expected_db: u16,
    drop_first_ping_count: usize,
) -> (
    u16,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepted = Arc::new(AtomicUsize::new(0));
    let pings_seen = Arc::new(AtomicUsize::new(0));
    let accepted_c = Arc::clone(&accepted);
    let pings_c = Arc::clone(&pings_seen);
    let pw = expected_password.to_string();

    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            accepted_c.fetch_add(1, Ordering::SeqCst);
            let pw = pw.clone();
            let pings_c = Arc::clone(&pings_c);
            tokio::spawn(async move {
                let (reader_half, mut writer_half) = stream.into_split();
                let mut reader = BufReader::new(reader_half);
                let mut authed = pw.is_empty();
                let mut selected_db = 0u16;

                loop {
                    let mut count_line = String::new();
                    if reader.read_line(&mut count_line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    if !count_line.starts_with('*') {
                        break;
                    }
                    let argc: usize = count_line[1..].trim().parse().unwrap_or(0);
                    let mut args = Vec::with_capacity(argc);
                    for _ in 0..argc {
                        let mut len_line = String::new();
                        if reader.read_line(&mut len_line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let len: usize = len_line[1..].trim().parse().unwrap_or(0);
                        let mut buf = vec![0u8; len + 2];
                        if reader.read_exact(&mut buf).await.is_err() {
                            return;
                        }
                        args.push(String::from_utf8_lossy(&buf[..len]).into_owned());
                    }
                    let cmd = args
                        .first()
                        .map(|s| s.to_ascii_uppercase())
                        .unwrap_or_default();
                    match cmd.as_str() {
                        "AUTH" => {
                            let ok = args.last().map(|p| p == &pw).unwrap_or(false);
                            if ok {
                                authed = true;
                                let _ = writer_half.write_all(b"+OK\r\n").await;
                            } else {
                                let _ = writer_half.write_all(b"-ERR invalid auth\r\n").await;
                            }
                        }
                        "SELECT" => {
                            let db = args.get(1).and_then(|s| s.parse::<u16>().ok());
                            if authed && db == Some(expected_db) {
                                selected_db = expected_db;
                                let _ = writer_half.write_all(b"+OK\r\n").await;
                            } else {
                                let _ = writer_half.write_all(b"-ERR bad db\r\n").await;
                            }
                        }
                        "PING" => {
                            let idx = pings_c.fetch_add(1, Ordering::SeqCst);
                            if idx < drop_first_ping_count {
                                return;
                            }
                            if authed && selected_db == expected_db {
                                let _ = writer_half.write_all(b"+PONG\r\n").await;
                            } else {
                                let _ = writer_half.write_all(b"-NOAUTH\r\n").await;
                            }
                        }
                        _ => {
                            let _ = writer_half.write_all(b"+OK\r\n").await;
                        }
                    }
                    let _ = writer_half.flush().await;
                }
            });
        }
    });

    (port, accepted, pings_seen, handle)
}

#[test]
fn test_kind_roundtrip_and_fingerprint_determinism() {
    for kind in [
        EtpKind::Redis,
        EtpKind::ClickHouse,
        EtpKind::S3,
        EtpKind::Loki,
        EtpKind::Elasticsearch,
        EtpKind::Otlp,
        EtpKind::Kafka,
        EtpKind::Statsd,
    ] {
        assert_eq!(EtpKind::from_str(kind.as_str()), Some(kind));
    }

    let a = EtpEntryConfig {
        id: "redis-1".into(),
        kind: EtpKind::Redis,
        security: EtpTransportSecurity::Plain,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({
            "host": "127.0.0.1",
            "port": 6379,
            "db": 0,
        }),
    };
    let b = EtpEntryConfig {
        id: "redis-1".into(),
        kind: EtpKind::Redis,
        security: EtpTransportSecurity::Plain,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({
            "db": 0,
            "port": 6379,
            "host": "127.0.0.1",
        }),
    };
    assert_eq!(a.fingerprint(), b.fingerprint());

    let mut c = a.clone();
    c.pool.pool_size = 8;
    assert_ne!(a.fingerprint(), c.fingerprint());
}

#[tokio::test]
async fn test_redis_prewarm_auth_select_ping_reconnect_and_recreate_in_background() {
    let (port, accepted, _pings, server_handle) = spawn_mock_redis_server("s3cr3t", 2, 1).await;

    let entry = make_entry(
        "redis-auth",
        EtpKind::Redis,
        port,
        2,
        serde_json::json!({
            "username": "velda",
            "password": "s3cr3t",
            "db": 2,
            "key_prefix": "rl:"
        }),
    );
    let registry = build_registry(&[entry]).unwrap();
    let inst = registry.entries.get("redis-auth").unwrap();
    warm_instance(inst).await.unwrap();

    let EtpBackend::Redis(redis_pool) = &inst.backend else {
        panic!("expected Redis backend");
    };
    let standalone = redis_pool.standalone().expect("expected standalone branch");
    assert_eq!(standalone.arena.ready_count(), 2);
    assert_eq!(accepted.load(Ordering::SeqCst), 2);

    {
        let mut lease = redis_pool.acquire().await.unwrap();
        assert_eq!(standalone.arena.ready_count(), 1);
        lease.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        lease.flush().await.unwrap();
        let mut pong = [0u8; 7];
        assert!(lease.read_exact(&mut pong).await.is_err());

        lease.reconnect().await.unwrap();
        assert_eq!(accepted.load(Ordering::SeqCst), 3);
        lease.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        lease.flush().await.unwrap();
        lease.read_exact(&mut pong).await.unwrap();
        assert_eq!(&pong, b"+PONG\r\n");
    }
    assert_eq!(standalone.arena.ready_count(), 2);

    {
        let mut lease = redis_pool.try_acquire().expect("warm slot available");
        assert_eq!(standalone.arena.ready_count(), 1);
        lease.recreate_in_background();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while standalone.arena.ready_count() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(standalone.arena.ready_count(), 2);
    assert_eq!(accepted.load(Ordering::SeqCst), 4);

    server_handle.abort();
}

#[tokio::test]
async fn test_clickhouse_lease_stream_io_and_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let conn_count = Arc::new(AtomicUsize::new(0));
    let conn_count_c = Arc::clone(&conn_count);

    let server = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let idx = conn_count_c.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if idx == 1 {
                    let mut tmp = [0u8; 64];
                    let _ = stream.read(&mut tmp).await;
                    return;
                }
                let mut buf = vec![0u8; 4096];
                loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    let req = String::from_utf8_lossy(&buf[..n]);
                    assert!(req.contains("GET /ping HTTP/1.1"));
                    let resp =
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok";
                    if stream.write_all(resp).await.is_err() {
                        break;
                    }
                    let _ = stream.flush().await;
                }
            });
        }
    });

    let entry = make_entry(
        "ch-logs",
        EtpKind::ClickHouse,
        port,
        1,
        serde_json::json!({
            "database": "analytics",
            "user": "writer",
            "password": "ch-secret"
        }),
    );
    let registry = build_registry(&[entry]).unwrap();
    let EtpBackend::ClickHouse(ch) = &registry.entries.get("ch-logs").unwrap().backend else {
        panic!("expected ClickHouse backend");
    };
    assert_eq!(ch.config.database, "analytics");
    assert_eq!(ch.config.user.as_deref(), Some("writer"));
    assert_eq!(ch.config.password.as_deref(), Some("ch-secret"));

    let mut lease = ch.acquire().await.unwrap();
    let raw_req = format!(
        "GET /ping HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
        ch.config.host_header
    );
    lease.write_all(raw_req.as_bytes()).await.unwrap();
    lease.flush().await.unwrap();
    let mut resp_buf = [0u8; 128];
    let n = lease.read(&mut resp_buf).await.unwrap();
    assert!(String::from_utf8_lossy(&resp_buf[..n]).starts_with("HTTP/1.1 200 OK"));

    lease.reconnect().await.unwrap();
    let dropped =
        if lease.write_all(raw_req.as_bytes()).await.is_ok() && lease.flush().await.is_ok() {
            matches!(lease.read(&mut resp_buf).await, Ok(0) | Err(_))
        } else {
            true
        };
    assert!(dropped);

    lease.reconnect().await.unwrap();
    lease.write_all(raw_req.as_bytes()).await.unwrap();
    lease.flush().await.unwrap();
    let n2 = lease.read(&mut resp_buf).await.unwrap();
    assert!(String::from_utf8_lossy(&resp_buf[..n2]).starts_with("HTTP/1.1 200 OK"));

    server.abort();
}

#[tokio::test]
async fn test_statsd_udp_warm_send_reconnect_and_recreate() {
    let server_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let port = server_sock.local_addr().unwrap().port();

    let entry = make_entry(
        "statsd-metrics",
        EtpKind::Statsd,
        port,
        2,
        serde_json::json!({
            "flavor": "dogstatsd",
            "prefix": "velda"
        }),
    );
    let registry = build_registry(&[entry]).unwrap();
    warm_all(&registry).await.unwrap();

    let EtpBackend::Statsd(statsd) = &registry.entries.get("statsd-metrics").unwrap().backend
    else {
        panic!("expected Statsd backend");
    };
    assert_eq!(statsd.arena.ready_count(), 2);

    {
        let mut lease = statsd.acquire().await.unwrap();
        let sent = lease.send(b"velda.requests:1|c").await.unwrap();
        assert_eq!(sent, b"velda.requests:1|c".len());

        let mut recv_buf = [0u8; 64];
        let (n, _) = server_sock.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..n], b"velda.requests:1|c");

        lease.reconnect().await.unwrap();
        lease.send(b"velda.reconnected:1|c").await.unwrap();
        let (n2, _) = server_sock.recv_from(&mut recv_buf).await.unwrap();
        assert_eq!(&recv_buf[..n2], b"velda.reconnected:1|c");

        lease.recreate_in_background();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while statsd.arena.ready_count() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(statsd.arena.ready_count(), 2);
}

#[tokio::test]
async fn test_make_before_break_reconciliation_preserves_unchanged_and_inflight_leases() {
    async fn spawn_echo() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 256];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if stream.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        (port, handle)
    }

    let (port_a, handle_a) = spawn_echo().await;
    let (port_b, handle_b) = spawn_echo().await;
    let (port_b2, handle_b2) = spawn_echo().await;

    let initial_configs = vec![
        make_entry("s3-archive", EtpKind::S3, port_a, 2, serde_json::json!({})),
        make_entry("loki-logs", EtpKind::Loki, port_b, 2, serde_json::json!({})),
    ];

    let reg1 = reconcile(&EtpRegistry::default(), &initial_configs, true)
        .await
        .unwrap();

    let s3_ptr_before = Arc::as_ptr(reg1.entries.get("s3-archive").unwrap());
    let loki_ptr_before = Arc::as_ptr(reg1.entries.get("loki-logs").unwrap());

    let EtpBackend::Loki(loki1) = &reg1.entries.get("loki-logs").unwrap().backend else {
        panic!("expected Loki backend");
    };
    let mut inflight_loki_lease = loki1.acquire().await.unwrap();

    let updated_configs = vec![
        make_entry("s3-archive", EtpKind::S3, port_a, 2, serde_json::json!({})),
        make_entry(
            "loki-logs",
            EtpKind::Loki,
            port_b2,
            2,
            serde_json::json!({}),
        ),
    ];
    let reg2 = reconcile(&reg1, &updated_configs, true).await.unwrap();

    assert_eq!(
        s3_ptr_before,
        Arc::as_ptr(reg2.entries.get("s3-archive").unwrap())
    );
    assert_ne!(
        loki_ptr_before,
        Arc::as_ptr(reg2.entries.get("loki-logs").unwrap())
    );

    let velda_etp::loki::LokiConn::Http(ref mut inflight_stream) = *inflight_loki_lease else {
        panic!("expected HTTP LokiConn");
    };
    inflight_stream.write_all(b"old-ok").await.unwrap();
    let mut buf = [0u8; 6];
    inflight_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"old-ok");
    drop(inflight_loki_lease);

    let EtpBackend::Loki(loki2) = &reg2.entries.get("loki-logs").unwrap().backend else {
        panic!("expected Loki backend");
    };
    let mut new_lease = loki2.acquire().await.unwrap();
    let velda_etp::loki::LokiConn::Http(ref mut new_stream) = *new_lease else {
        panic!("expected HTTP LokiConn");
    };
    new_stream.write_all(b"new-ok").await.unwrap();
    new_stream.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"new-ok");

    let s3_inst = reg2.entries.get("s3-archive").unwrap();
    assert_eq!(s3_inst.kind, EtpKind::S3);
    assert!(matches!(s3_inst.backend, EtpBackend::S3(_)));

    handle_a.abort();
    handle_b.abort();
    handle_b2.abort();
}

#[tokio::test]
async fn test_tls_stream_connect_with_custom_ca() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let mut cert_reader = std::io::BufReader::new(cert_pem.as_bytes());
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let mut key_reader = std::io::BufReader::new(key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .unwrap();

    let server_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(tcp).await.unwrap();
        let mut buf = [0u8; 5];
        tls.read_exact(&mut buf).await.unwrap();
        tls.write_all(&buf).await.unwrap();
        tls.flush().await.unwrap();
    });

    let tls_cfg = EtpTlsConfig {
        ca_cert_pem: Some(cert_pem),
        client_cert_pem: None,
        client_key_pem: None,
        server_name: Some("localhost".into()),
        insecure_skip_verify: false,
        tls_version: EtpTlsVersion::Auto,
        pinned_cert_sha256: None,
    };
    let client_cfg = tls_cfg.build_client_config().unwrap();
    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();

    let mut stream = EtpStream::connect(
        "127.0.0.1",
        port,
        Some(&(client_cfg, server_name)),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    assert!(stream.is_tls());
    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();
    let mut resp = [0u8; 5];
    stream.read_exact(&mut resp).await.unwrap();
    assert_eq!(&resp, b"hello");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_page_arena_alignment_isolation_pointer_stability_and_burst_spillover() {
    assert_eq!(std::mem::size_of::<ArenaControlLine>(), 64);
    assert_eq!(std::mem::align_of::<ArenaControlLine>(), 64);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 64];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if stream.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });

    let entry1 = make_entry("otlp-1", EtpKind::Otlp, port, 2, serde_json::json!({}));
    let entry2 = make_entry("otlp-2", EtpKind::Otlp, port, 2, serde_json::json!({}));

    let otlp1 = Arc::new(OtlpEtp::from_entry(&entry1).unwrap());
    let otlp2 = Arc::new(OtlpEtp::from_entry(&entry2).unwrap());

    assert_eq!((otlp1.arena.base.as_ptr() as usize) % PAGE_SIZE, 0);
    assert_eq!((otlp2.arena.base.as_ptr() as usize) % PAGE_SIZE, 0);

    let slot_size = std::mem::size_of::<ArenaSlot<EtpStream>>();
    let expected_required_pages = (64 + 2 * slot_size).div_ceil(PAGE_SIZE).max(1);
    assert_eq!(otlp1.arena.page_count, expected_required_pages + 1);
    assert_eq!(
        otlp1.arena.layout.size(),
        (expected_required_pages + 1) * PAGE_SIZE
    );
    assert!(otlp1.arena.total_slots > otlp1.arena.baseline_slots);

    let r1_start = otlp1.arena.base.as_ptr() as usize;
    let r1_end = r1_start + otlp1.arena.layout.size();
    let r2_start = otlp2.arena.base.as_ptr() as usize;
    let r2_end = r2_start + otlp2.arena.layout.size();
    assert!(
        r1_end <= r2_start || r2_end <= r1_start,
        "Two ETP instances must occupy disjoint page-aligned virtual memory regions"
    );

    otlp1.warm().await.unwrap();
    assert_eq!(otlp1.arena.ready_count(), 2);

    let ptr_first = {
        let lease = otlp1.acquire().await.unwrap();
        assert_eq!(lease.slot_idx, 0);
        let OtlpConn::Http(ref s) = *lease else {
            panic!("expected HTTP OtlpConn");
        };
        let p = s as *const EtpStream as usize;
        assert!(p >= r1_start && p < r1_end);
        p
    };
    let ptr_second = {
        let lease = otlp1.acquire().await.unwrap();
        assert_eq!(lease.slot_idx, 0);
        let OtlpConn::Http(ref s) = *lease else {
            panic!("expected HTTP OtlpConn");
        };
        s as *const EtpStream as usize
    };
    assert_eq!(
        ptr_first, ptr_second,
        "Re-acquiring Slot[0] via TZCNT must yield the exact same virtual address"
    );

    let l0 = otlp1.acquire().await.unwrap();
    let l1 = otlp1.acquire().await.unwrap();
    assert_eq!(l0.slot_idx, 0);
    assert_eq!(l1.slot_idx, 1);

    let mut burst = otlp1.acquire().await.unwrap();
    assert!(
        burst.slot_idx >= otlp1.arena.baseline_slots,
        "Burst lease must spill over into the +4096B spare page"
    );
    let OtlpConn::Http(ref mut burst_stream) = *burst else {
        panic!("expected HTTP OtlpConn");
    };
    let burst_ptr = burst_stream as *mut EtpStream as usize;
    assert!(burst_ptr >= r1_start && burst_ptr < r1_end);

    burst_stream.write_all(b"burst").await.unwrap();
    let mut reply = [0u8; 5];
    burst_stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"burst");

    let burst_idx = burst.slot_idx;
    drop(burst);
    let w = (burst_idx >> 6) & 1;
    let bit = 1u64 << (burst_idx & 63);
    assert_eq!(
        otlp1.arena.control().init_bitmap[w].load(Ordering::Acquire) & bit,
        0
    );
    drop(l0);
    drop(l1);
    assert_eq!(otlp1.arena.ready_count(), 2);

    server.abort();
}

#[test]
fn test_cold_target_addr_integer_ip_encoding_and_dns_fallback() {
    let v4 = ColdTargetAddr::parse("127.0.0.1", 6379);
    match v4 {
        ColdTargetAddr::Ip {
            ip_u128,
            port_be,
            addr,
        } => {
            assert!(velda_core::lut::ip::is_v4_u128(ip_u128));
            assert_eq!(port_be, 6379u16.to_be());
            assert_eq!(addr, "127.0.0.1:6379".parse().unwrap());
        }
        ColdTargetAddr::Dns { .. } => panic!("expected integer IPv4 target"),
    }

    let v6 = ColdTargetAddr::parse("[::1]", 6379);
    match v6 {
        ColdTargetAddr::Ip {
            ip_u128,
            port_be,
            addr,
        } => {
            assert!(!velda_core::lut::ip::is_v4_u128(ip_u128));
            assert_eq!(ip_u128, 1u128);
            assert_eq!(port_be, 6379u16.to_be());
            assert_eq!(addr, "[::1]:6379".parse().unwrap());
        }
        ColdTargetAddr::Dns { .. } => panic!("expected integer IPv6 target"),
    }

    let dns = ColdTargetAddr::parse("redis.internal.svc", 6379);
    assert!(matches!(dns, ColdTargetAddr::Dns { port: 6379, .. }));
}

#[tokio::test]
async fn test_tls_versions_12_13_and_session_resumption_across_pool() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let mut cert_reader = std::io::BufReader::new(cert_pem.as_bytes());
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let mut key_reader = std::io::BufReader::new(key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .unwrap();

    let server_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut tls) = acceptor.accept(tcp).await {
                    let proto = tls.get_ref().1.protocol_version().unwrap();
                    let tag = match proto {
                        rustls::ProtocolVersion::TLSv1_2 => *b"12",
                        rustls::ProtocolVersion::TLSv1_3 => *b"13",
                        _ => *b"00",
                    };
                    let _ = tls.write_all(&tag).await;
                    let _ = tls.flush().await;
                }
            });
        }
    });

    for (ver, expected_tag) in [
        (EtpTlsVersion::Tls12, b"12"),
        (EtpTlsVersion::Tls13, b"13"),
        (EtpTlsVersion::Auto, b"13"),
    ] {
        let entry = EtpEntryConfig {
            id: format!("otlp-tls-{ver:?}"),
            kind: EtpKind::Otlp,
            security: EtpTransportSecurity::Tls,
            pool: EtpPoolSettings {
                pool_size: 2,
                connect_timeout_ms: 2000,
                idle_timeout_ms: 60_000,
            },
            tls: Some(EtpTlsConfig {
                ca_cert_pem: Some(cert_pem.clone()),
                client_cert_pem: None,
                client_key_pem: None,
                server_name: Some("localhost".into()),
                insecure_skip_verify: false,
                tls_version: ver,
                pinned_cert_sha256: None,
            }),
            settings: serde_json::json!({ "host": "127.0.0.1", "port": port }),
        };

        let pool = Arc::new(OtlpEtp::from_entry(&entry).unwrap());
        let mut l1 = pool.acquire().await.unwrap();
        let mut buf = [0u8; 2];
        {
            let OtlpConn::Http(ref mut s) = *l1 else {
                panic!("expected HTTP OtlpConn");
            };
            s.read_exact(&mut buf).await.unwrap();
        }
        assert_eq!(&buf, expected_tag);

        l1.reconnect().await.unwrap();
        {
            let OtlpConn::Http(ref mut s) = *l1 else {
                panic!("expected HTTP OtlpConn");
            };
            s.read_exact(&mut buf).await.unwrap();
        }
        assert_eq!(&buf, expected_tag);
    }

    let mut e1 = make_entry("fp-tls", EtpKind::Otlp, port, 2, serde_json::json!({}));
    e1.security = EtpTransportSecurity::Tls;
    e1.tls = Some(EtpTlsConfig {
        tls_version: EtpTlsVersion::Tls12,
        ..Default::default()
    });
    let mut e2 = e1.clone();
    e2.tls.as_mut().unwrap().tls_version = EtpTlsVersion::Tls13;
    assert_ne!(e1.fingerprint(), e2.fingerprint());

    server_task.abort();
}

#[tokio::test]
async fn test_loki_es_otlp_and_kafka_direct_lease_lifecycle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<Vec<u8>>::new()));
    let captured_c = Arc::clone(&captured);

    let echo_server = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let captured_c = Arc::clone(&captured_c);
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                if let Ok(n) = stream.read(&mut buf).await
                    && n > 0
                {
                    captured_c.lock().await.push(buf[..n].to_vec());
                    let _ = stream.write_all(&buf[..n]).await;
                    let _ = stream.flush().await;
                }
            });
        }
    });

    let loki_entry = make_entry(
        "loki-fast",
        EtpKind::Loki,
        port,
        1,
        serde_json::json!({
            "tenant_id": "velda-tenant",
            "bearer_token": "tok-123"
        }),
    );
    let es_entry = make_entry(
        "es-fast",
        EtpKind::Elasticsearch,
        port,
        1,
        serde_json::json!({
            "index": "edge-logs",
            "api_key": "k-999"
        }),
    );
    let otlp_entry = make_entry(
        "otlp-fast",
        EtpKind::Otlp,
        port,
        1,
        serde_json::json!({
            "protocol": "http/json",
            "headers": { "x-otlp-key": "abc" }
        }),
    );

    let registry = build_registry(&[loki_entry, es_entry, otlp_entry]).unwrap();

    let EtpBackend::Loki(loki) = &registry.entries.get("loki-fast").unwrap().backend else {
        panic!("expected Loki backend");
    };
    assert_eq!(loki.config.tenant_id.as_deref(), Some("velda-tenant"));
    assert_eq!(loki.config.bearer_token.as_deref(), Some("tok-123"));
    let mut loki_lease = loki.acquire().await.unwrap();
    let velda_etp::loki::LokiConn::Http(ref mut loki_stream) = *loki_lease else {
        panic!("expected HTTP LokiConn");
    };
    loki_stream.write_all(b"loki-ping").await.unwrap();
    loki_stream.flush().await.unwrap();
    let mut reply = [0u8; 9];
    loki_stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"loki-ping");

    let EtpBackend::Elasticsearch(es) = &registry.entries.get("es-fast").unwrap().backend else {
        panic!("expected Elasticsearch backend");
    };
    assert_eq!(es.config.index.as_deref(), Some("edge-logs"));
    assert_eq!(es.config.api_key.as_deref(), Some("k-999"));
    let mut es_lease = es.acquire().await.unwrap();
    es_lease.write_all(b"es-ping").await.unwrap();
    es_lease.flush().await.unwrap();
    let mut reply7 = [0u8; 7];
    es_lease.read_exact(&mut reply7).await.unwrap();
    assert_eq!(&reply7, b"es-ping");

    let EtpBackend::Otlp(otlp) = &registry.entries.get("otlp-fast").unwrap().backend else {
        panic!("expected OTLP backend");
    };
    assert_eq!(otlp.config.headers.len(), 1);
    let mut otlp_lease = otlp.acquire().await.unwrap();
    let OtlpConn::Http(ref mut otlp_stream) = *otlp_lease else {
        panic!("expected HTTP OtlpConn");
    };
    otlp_stream.write_all(b"otlp-ok").await.unwrap();
    otlp_stream.flush().await.unwrap();
    otlp_stream.read_exact(&mut reply7).await.unwrap();
    assert_eq!(&reply7, b"otlp-ok");

    assert_eq!(captured.lock().await.len(), 3);
    echo_server.abort();

    let k_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let k_port = k_listener.local_addr().unwrap().port();
    let k_srv = tokio::spawn(async move {
        let (mut s, _) = k_listener.accept().await.unwrap();
        let mut buf = vec![0u8; 64];
        let n = s.read(&mut buf).await.unwrap();
        buf.truncate(n);
        buf
    });

    let kafka_entry = make_entry(
        "kafka-fast",
        EtpKind::Kafka,
        k_port,
        1,
        serde_json::json!({ "client_id": "velda-edge-test" }),
    );
    let kafka_pool = Arc::new(KafkaEtp::from_entry(&kafka_entry).unwrap());
    assert_eq!(kafka_pool.config.client_id, "velda-edge-test");
    assert_eq!(kafka_pool.blueprints.len(), 1);
    let mut k_lease = kafka_pool.acquire().await.unwrap();
    k_lease.write_all(b"kafka-direct").await.unwrap();
    k_lease.flush().await.unwrap();
    let received = k_srv.await.unwrap();
    assert_eq!(&received, b"kafka-direct");
}

#[tokio::test]
async fn test_tls_pinned_cert_sha256_u64x4_match_and_mismatch() {
    let abc_expected = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    let abc_words = sha256_u64x4(b"abc");
    let abc_cfg = EtpTlsConfig {
        pinned_cert_sha256: Some(format!("sha256:{abc_expected}")),
        ..Default::default()
    };
    assert_eq!(
        abc_cfg.parse_pinned_sha256_u64x4().unwrap(),
        Some(abc_words)
    );

    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let key_pem = cert.signing_key.serialize_pem();

    let mut cert_reader = std::io::BufReader::new(cert_pem.as_bytes());
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_reader)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let mut key_reader = std::io::BufReader::new(key_pem.as_bytes());
    let key = rustls_pemfile::private_key(&mut key_reader)
        .unwrap()
        .unwrap();

    let words = sha256_u64x4(certs[0].as_ref());
    let mut digest = [0u8; 32];
    for (i, word) in words.iter().enumerate() {
        digest[i * 8..i * 8 + 8].copy_from_slice(&word.to_le_bytes());
    }
    let hex_pin: String = digest
        .iter()
        .enumerate()
        .map(|(i, b)| {
            if i == 0 {
                format!("{b:02x}")
            } else {
                format!(":{b:02x}")
            }
        })
        .collect();

    let server_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        let (tcp1, _) = listener.accept().await.unwrap();
        let mut tls1 = acceptor.accept(tcp1).await.unwrap();
        let mut buf = [0u8; 4];
        tls1.read_exact(&mut buf).await.unwrap();
        tls1.write_all(&buf).await.unwrap();
        tls1.flush().await.unwrap();

        let (tcp2, _) = listener.accept().await.unwrap();
        let _ = acceptor.accept(tcp2).await;
    });

    let ok_tls = EtpTlsConfig {
        pinned_cert_sha256: Some(format!("sha256:{hex_pin}")),
        ..Default::default()
    };
    ok_tls.validate().unwrap();
    let ok_cfg = ok_tls.build_client_config().unwrap();
    let sni = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();

    let mut stream = EtpStream::connect(
        "127.0.0.1",
        port,
        Some(&(ok_cfg, sni.clone())),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    stream.write_all(b"ping").await.unwrap();
    stream.flush().await.unwrap();
    let mut resp = [0u8; 4];
    stream.read_exact(&mut resp).await.unwrap();
    assert_eq!(&resp, b"ping");

    let mut bad_digest = digest;
    bad_digest[0] ^= 0x01;
    let bad_hex: String = bad_digest.iter().map(|b| format!("{b:02x}")).collect();
    let bad_tls = EtpTlsConfig {
        pinned_cert_sha256: Some(bad_hex),
        ..Default::default()
    };
    let bad_cfg = bad_tls.build_client_config().unwrap();
    let err = EtpStream::connect(
        "127.0.0.1",
        port,
        Some(&(bad_cfg, sni)),
        Duration::from_secs(2),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(&err, EtpError::Tls(msg) if msg.contains("Server certificate SHA-256 integer pin mismatch")),
        "expected TLS pin mismatch error, got {err:?}"
    );

    let mut e1 = make_entry("fp-pin", EtpKind::Otlp, port, 1, serde_json::json!({}));
    e1.security = EtpTransportSecurity::Tls;
    e1.tls = Some(ok_tls);
    let mut e2 = e1.clone();
    e2.tls.as_mut().unwrap().pinned_cert_sha256 = None;
    assert_ne!(e1.fingerprint(), e2.fingerprint());

    server_task.abort();
}

#[tokio::test]
async fn test_cold_target_addr_dns_freeze_localhost() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        for tag in [b"c1", b"c2"] {
            let (mut tcp, _) = listener.accept().await.unwrap();
            tcp.write_all(tag).await.unwrap();
            tcp.flush().await.unwrap();
        }
    });

    let target = ColdTargetAddr::parse("localhost", port);
    assert_eq!(target.cached_ip_u128(), 0);

    let mut s1 = EtpStream::connect_target(&target, None, Duration::from_secs(2))
        .await
        .unwrap();
    let frozen_ip = target.cached_ip_u128();
    assert_ne!(frozen_ip, 0);
    let mut b1 = [0u8; 2];
    s1.read_exact(&mut b1).await.unwrap();
    assert_eq!(&b1, b"c1");

    let mut s2 = EtpStream::connect_target(&target, None, Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(target.cached_ip_u128(), frozen_ip);
    let mut b2 = [0u8; 2];
    s2.read_exact(&mut b2).await.unwrap();
    assert_eq!(&b2, b"c2");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_acquire_cancellation_does_not_leak_busy_bitmap() {
    use std::sync::atomic::AtomicBool;
    use velda_etp::redis::RedisEtp;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let respond_ok = Arc::new(AtomicBool::new(false));
    let respond_ok_c = Arc::clone(&respond_ok);

    let server_task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let respond_ok_c = Arc::clone(&respond_ok_c);
            tokio::spawn(async move {
                if !respond_ok_c.load(Ordering::Acquire) {
                    // Hold socket open without replying to AUTH so acquire() cancels mid-.await
                    let mut tmp = [0u8; 128];
                    let _ = stream.read(&mut tmp).await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    return;
                }
                let (reader_half, mut writer_half) = stream.into_split();
                let mut reader = BufReader::new(reader_half);
                loop {
                    let mut count_line = String::new();
                    if reader.read_line(&mut count_line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    let argc: usize = count_line[1..].trim().parse().unwrap_or(0);
                    let mut args = Vec::with_capacity(argc);
                    for _ in 0..argc {
                        let mut len_line = String::new();
                        if reader.read_line(&mut len_line).await.unwrap_or(0) == 0 {
                            return;
                        }
                        let len: usize = len_line[1..].trim().parse().unwrap_or(0);
                        let mut buf = vec![0u8; len + 2];
                        if reader.read_exact(&mut buf).await.is_err() {
                            return;
                        }
                        args.push(String::from_utf8_lossy(&buf[..len]).into_owned());
                    }
                    let cmd = args
                        .first()
                        .map(|s| s.to_ascii_uppercase())
                        .unwrap_or_default();
                    let reply = if cmd == "PING" {
                        b"+PONG\r\n".as_slice()
                    } else {
                        b"+OK\r\n".as_slice()
                    };
                    if writer_half.write_all(reply).await.is_err() {
                        break;
                    }
                    let _ = writer_half.flush().await;
                }
            });
        }
    });

    let entry = make_entry(
        "redis-cancel",
        EtpKind::Redis,
        port,
        1,
        serde_json::json!({ "password": "secret" }),
    );
    let pool = Arc::new(RedisEtp::from_entry(&entry).unwrap());

    // Cancel acquire() 20 times mid-.await after ClaimedSlotGuard has claimed Slot[0]
    for _ in 0..20 {
        let res = tokio::time::timeout(Duration::from_millis(10), pool.acquire()).await;
        assert!(res.is_err(), "expected timeout cancellation mid-handshake");
    }

    let standalone = pool.standalone().expect("standalone branch");
    assert_eq!(
        standalone.semaphore.available_permits(),
        standalone.arena.total_slots
    );
    assert_eq!(standalone.arena.baseline_active_count(), 0);

    // Enable server replies and verify immediate acquisition succeeds on Slot[0]
    respond_ok.store(true, Ordering::Release);
    let mut lease = pool.acquire().await.unwrap();
    assert_eq!(lease.slot_idx, 0);
    lease.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    lease.flush().await.unwrap();
    let mut pong = [0u8; 7];
    lease.read_exact(&mut pong).await.unwrap();
    assert_eq!(&pong, b"+PONG\r\n");

    server_task.abort();
}

#[test]
fn test_pool_size_127_still_has_spare_slot() {
    use velda_etp::arena::{EtpPageArena, MAX_BASELINE_SLOTS};

    let arena = EtpPageArena::<EtpStream>::new(MAX_BASELINE_SLOTS);
    assert_eq!(arena.baseline_slots, MAX_BASELINE_SLOTS);
    assert!(arena.total_slots > arena.baseline_slots);

    let ok_entry = make_entry(
        "ok-127",
        EtpKind::Otlp,
        4317,
        MAX_BASELINE_SLOTS,
        serde_json::json!({}),
    );
    ok_entry.validate().unwrap();

    let bad_entry = make_entry(
        "bad-128",
        EtpKind::Otlp,
        4317,
        MAX_BASELINE_SLOTS + 1,
        serde_json::json!({}),
    );
    assert!(bad_entry.validate().is_err());
}

#[tokio::test]
async fn test_vault_approle_and_auth_idp_etp_kinds() {
    for kind in [
        EtpKind::Vault,
        EtpKind::AwsSecretsManager,
        EtpKind::Keycloak,
        EtpKind::Hydra,
        EtpKind::Authelia,
        EtpKind::Okta,
        EtpKind::Auth0,
        EtpKind::EntraId,
        EtpKind::Cognito,
        EtpKind::Oidc,
    ] {
        assert_eq!(EtpKind::from_str(kind.as_str()), Some(kind));
    }

    // Cloud/SaaS IdP host inference without full URL
    let okta = EtpEntryConfig {
        id: "okta-dev".into(),
        kind: EtpKind::Okta,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "domain": "dev-12345.okta.com" }),
    };
    assert_eq!(
        okta.resolve_endpoint().unwrap(),
        ("dev-12345.okta.com".to_string(), 443)
    );

    let entra = EtpEntryConfig {
        id: "entra-corp".into(),
        kind: EtpKind::EntraId,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "tenant_id": "contoso-tenant-uuid" }),
    };
    assert_eq!(
        entra.resolve_endpoint().unwrap(),
        ("login.microsoftonline.com".to_string(), 443)
    );

    let aws_sm = EtpEntryConfig {
        id: "aws-sm".into(),
        kind: EtpKind::AwsSecretsManager,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({
            "region": "ap-southeast-1",
            "access_key_id": "AKIA_TEST",
            "secret_access_key": "SECRET_TEST"
        }),
    };
    assert_eq!(
        aws_sm.resolve_endpoint().unwrap(),
        (
            "secretsmanager.ap-southeast-1.amazonaws.com".to_string(),
            443
        )
    );

    // Mock Vault AppRole server testing login caching and invalidation
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let login_calls = Arc::new(AtomicUsize::new(0));
    let calls_c = Arc::clone(&login_calls);

    let srv = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let calls_c = Arc::clone(&calls_c);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("POST /v1/auth/approle/login") {
                    let count = calls_c.fetch_add(1, Ordering::SeqCst) + 1;
                    let body = format!(
                        r#"{{"auth":{{"client_token":"hvs.token-{count}","lease_duration":300}}}}"#
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.flush().await;
                }
            });
        }
    });

    let vault_cfg = make_entry(
        "vault-prod",
        EtpKind::Vault,
        port,
        1,
        serde_json::json!({
            "namespace": "edge",
            "auth": {
                "type": "app_role",
                "role_id": "role-1",
                "secret_id": "sec-1"
            }
        }),
    );

    let reg = build_registry(&[vault_cfg]).unwrap();
    let inst = reg.entries.get("vault-prod").unwrap();
    let EtpBackend::Vault(vault) = &inst.backend else {
        panic!("expected Vault backend");
    };

    assert_eq!(
        vault.resolve_token().await.unwrap().as_deref(),
        Some("hvs.token-1")
    );
    // Second call uses cached token (no extra HTTP call)
    assert_eq!(
        vault.resolve_token().await.unwrap().as_deref(),
        Some("hvs.token-1")
    );
    assert_eq!(login_calls.load(Ordering::SeqCst), 1);

    // Invalidating forces fresh AppRole login
    vault.invalidate_token().await;
    assert_eq!(
        vault.resolve_token().await.unwrap().as_deref(),
        Some("hvs.token-2")
    );
    assert_eq!(login_calls.load(Ordering::SeqCst), 2);

    srv.abort();
}

#[tokio::test]
async fn test_postgres_mysql_nats_dynamodb_memcached_sot_pools() {
    use velda_etp::dynamodb::DynamoDbEtp;
    use velda_etp::memcached::MemcachedEtp;
    use velda_etp::mysql::MysqlEtp;
    use velda_etp::nats::NatsEtp;
    use velda_etp::postgres::PostgresEtp;

    // 1. PostgreSQL mock server (cleartext password + ReadyForQuery)
    let pg_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_port = pg_listener.local_addr().unwrap().port();
    let pg_srv = tokio::spawn(async move {
        while let Ok((mut stream, _)) = pg_listener.accept().await {
            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).await.is_err() {
                    return;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut rest = vec![0u8; len.saturating_sub(4)];
                let _ = stream.read_exact(&mut rest).await;
                // AuthenticationCleartextPassword: 'R' | len=8 | code=3
                let mut auth_req = [0u8; 9];
                auth_req[0] = b'R';
                auth_req[1..5].copy_from_slice(&8u32.to_be_bytes());
                auth_req[5..9].copy_from_slice(&3u32.to_be_bytes());
                let _ = stream.write_all(&auth_req).await;
                // Read PasswordMessage ('p' | len | pw\0)
                let mut hdr = [0u8; 5];
                if stream.read_exact(&mut hdr).await.is_ok() {
                    let pw_len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
                    let mut pw_body = vec![0u8; pw_len.saturating_sub(4)];
                    let _ = stream.read_exact(&mut pw_body).await;
                    // Send AuthenticationOk + ReadyForQuery('Z', len=5, 'I')
                    let mut ok_buf = Vec::new();
                    ok_buf.push(b'R');
                    ok_buf.extend_from_slice(&8u32.to_be_bytes());
                    ok_buf.extend_from_slice(&0u32.to_be_bytes());
                    ok_buf.push(b'Z');
                    ok_buf.extend_from_slice(&5u32.to_be_bytes());
                    ok_buf.push(b'I');
                    let _ = stream.write_all(&ok_buf).await;
                }
            });
        }
    });

    let pg_cfg = make_entry(
        "pg-sot",
        EtpKind::Postgres,
        pg_port,
        2,
        serde_json::json!({
            "username": "velda",
            "password": "pg-secret",
            "database": "velda_rl"
        }),
    );
    let reg_pg = build_registry(&[pg_cfg]).unwrap();
    let EtpBackend::Postgres(pg_pool) = &reg_pg.entries["pg-sot"].backend else {
        panic!("expected Postgres");
    };
    pg_pool.warm().await.unwrap();
    let _pg_lease: velda_etp::postgres::PostgresLease = pg_pool.acquire().await.unwrap();

    // 2. MySQL mock server (HandshakeV10 + 0x00 OK)
    let my_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let my_port = my_listener.local_addr().unwrap().port();
    let my_srv = tokio::spawn(async move {
        while let Ok((mut stream, _)) = my_listener.accept().await {
            tokio::spawn(async move {
                let mut payload = Vec::new();
                payload.push(0x0a); // HandshakeV10
                payload.extend_from_slice(b"8.0.36\0");
                payload.extend_from_slice(&1u32.to_le_bytes()); // conn_id
                payload.extend_from_slice(&[0x22u8; 8]); // scramble part 1
                payload.push(0x00); // filler
                payload.extend_from_slice(&0xffffu16.to_le_bytes()); // cap low
                payload.push(45); // utf8mb4
                payload.extend_from_slice(&2u16.to_le_bytes()); // status
                payload.extend_from_slice(&0x0008u16.to_le_bytes()); // cap high (CLIENT_PLUGIN_AUTH)
                payload.push(21); // scramble len
                payload.extend_from_slice(&[0u8; 10]); // reserved
                payload.extend_from_slice(&[0x33u8; 12]); // scramble part 2
                payload.push(0x00);
                payload.extend_from_slice(b"mysql_native_password\0");

                let len = payload.len() as u32;
                let hdr = [(len & 0xff) as u8, ((len >> 8) & 0xff) as u8, 0, 0];
                let _ = stream.write_all(&hdr).await;
                let _ = stream.write_all(&payload).await;

                // Read HandshakeResponse41
                let mut rhdr = [0u8; 4];
                if stream.read_exact(&mut rhdr).await.is_ok() {
                    let rlen =
                        (rhdr[0] as usize) | ((rhdr[1] as usize) << 8) | ((rhdr[2] as usize) << 16);
                    let mut rbody = vec![0u8; rlen];
                    let _ = stream.read_exact(&mut rbody).await;
                    // Send OK packet (0x00, 0, 0, 2, 0, 0, 0)
                    let ok_pkt = [7u8, 0, 0, 2, 0x00, 0, 0, 2, 0, 0, 0];
                    let _ = stream.write_all(&ok_pkt).await;
                }
            });
        }
    });

    let my_cfg = make_entry(
        "mysql-sot",
        EtpKind::MySql,
        my_port,
        2,
        serde_json::json!({
            "username": "velda",
            "password": "my-secret",
            "database": "velda_rl"
        }),
    );
    let my_pool = Arc::new(MysqlEtp::from_entry(&my_cfg).unwrap());
    my_pool.warm().await.unwrap();
    let _my_lease = my_pool.acquire().await.unwrap();

    // 3. NATS mock server (INFO -> CONNECT + PING -> PONG)
    let nats_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nats_port = nats_listener.local_addr().unwrap().port();
    let nats_srv = tokio::spawn(async move {
        while let Ok((mut stream, _)) = nats_listener.accept().await {
            tokio::spawn(async move {
                let _ = stream
                    .write_all(b"INFO {\"server_id\":\"n1\",\"version\":\"2.10.0\"}\r\n")
                    .await;
                let mut buf = [0u8; 512];
                let _ = stream.read(&mut buf).await;
                let _ = stream.write_all(b"PONG\r\n").await;
            });
        }
    });

    let nats_cfg = make_entry(
        "nats-sot",
        EtpKind::Nats,
        nats_port,
        2,
        serde_json::json!({
            "token": "tok-1",
            "bucket": "rl_bucket"
        }),
    );
    let nats_pool = Arc::new(NatsEtp::from_entry(&nats_cfg).unwrap());
    nats_pool.warm().await.unwrap();
    let _nats_lease = nats_pool.acquire().await.unwrap();

    // 4. DynamoDB & Memcached config + pool instantiation
    let ddb_cfg = EtpEntryConfig {
        id: "ddb-sot".into(),
        kind: EtpKind::DynamoDb,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings {
            pool_size: 2,
            connect_timeout_ms: 500,
            idle_timeout_ms: 5000,
        },
        tls: None,
        settings: serde_json::json!({
            "region": "ap-southeast-1",
            "access_key_id": "AKIAEXAMPLE",
            "secret_access_key": "SECRETEXAMPLE"
        }),
    };
    let ddb = DynamoDbEtp::from_entry(&ddb_cfg).unwrap();
    assert_eq!(ddb.config.host, "dynamodb.ap-southeast-1.amazonaws.com");
    assert_eq!(ddb.config.port, 443);

    let mc_cfg = make_entry(
        "mc-sot",
        EtpKind::Memcached,
        11211,
        2,
        serde_json::json!({ "key_prefix": "velda:rl:" }),
    );
    let mc = MemcachedEtp::from_entry(&mc_cfg).unwrap();
    assert_eq!(mc.config.key_prefix.as_deref(), Some("velda:rl:"));

    let _ = (PostgresEtp::from_entry, EtpKind::DynamoDb);
    pg_srv.abort();
    my_srv.abort();
    nats_srv.abort();
}

#[tokio::test]
async fn test_redis_cluster_branch_discovery_routing_and_moved_override() {
    use velda_etp::redis::{RedisEtp, RedisMode, crc16_slot};

    // db > 0 must be rejected when mode == "cluster"
    let bad_cluster = make_entry(
        "redis-cluster-bad-db",
        EtpKind::Redis,
        6379,
        1,
        serde_json::json!({ "mode": "cluster", "db": 2 }),
    );
    assert!(RedisEtp::from_entry(&bad_cluster).is_err());

    let node0_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port0 = node0_listener.local_addr().unwrap().port();
    let node1_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port1 = node1_listener.local_addr().unwrap().port();

    let spawn_shard = |listener: TcpListener, id_tag: &'static str, p0: u16, p1: u16| {
        tokio::spawn(async move {
            let slots_reply = format!(
                "*2\r\n\
                 *3\r\n:0\r\n:8191\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p0}\r\n\
                 *3\r\n:8192\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:{p1}\r\n"
            );
            while let Ok((mut stream, _)) = listener.accept().await {
                let slots_reply = slots_reply.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = stream.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        let msg = String::from_utf8_lossy(&buf[..n]);
                        if msg.contains("SLOTS") {
                            let _ = stream.write_all(slots_reply.as_bytes()).await;
                        } else if msg.contains("WHOAMI") {
                            let bulk = format!("${}\r\n{id_tag}\r\n", id_tag.len());
                            let _ = stream.write_all(bulk.as_bytes()).await;
                        } else {
                            let _ = stream.write_all(b"+OK\r\n").await;
                        }
                    }
                });
            }
        })
    };

    let s0 = spawn_shard(node0_listener, "node0", port0, port1);
    let s1 = spawn_shard(node1_listener, "node1", port0, port1);

    let cluster_entry = EtpEntryConfig {
        id: "redis-cluster".into(),
        kind: EtpKind::Redis,
        security: EtpTransportSecurity::Plain,
        pool: EtpPoolSettings {
            pool_size: 2,
            connect_timeout_ms: 500,
            idle_timeout_ms: 5000,
        },
        tls: None,
        settings: serde_json::json!({
            "mode": "cluster",
            "seed_nodes": [format!("127.0.0.1:{port0}")],
            "password": "pw"
        }),
    };
    let redis_etp = Arc::new(RedisEtp::from_entry(&cluster_entry).unwrap());
    assert_eq!(redis_etp.config.mode, RedisMode::Cluster);
    assert!(redis_etp.is_cluster());
    redis_etp.warm().await.unwrap();

    // Slot 100 belongs to node0, Slot 12000 belongs to node1
    let mut l0 = redis_etp.acquire_for_slot(100).await.unwrap();
    l0.write_all(b"*1\r\n$6\r\nWHOAMI\r\n").await.unwrap();
    let mut r0 = [0u8; 11];
    l0.read_exact(&mut r0).await.unwrap();
    assert_eq!(&r0, b"$5\r\nnode0\r\n");

    let mut l1 = redis_etp.acquire_for_slot(12000).await.unwrap();
    l1.write_all(b"*1\r\n$6\r\nWHOAMI\r\n").await.unwrap();
    let mut r1 = [0u8; 11];
    l1.read_exact(&mut r1).await.unwrap();
    assert_eq!(&r1, b"$5\r\nnode1\r\n");

    // Report MOVED for slot 100 -> port1 updates SlotMap immediately
    let new_idx = redis_etp
        .report_moved(100, &format!("127.0.0.1:{port1}"))
        .unwrap();
    assert_eq!(redis_etp.cluster_node_for_slot(100), Some(new_idx));
    assert_eq!(crc16_slot(b"123456789"), 12739);

    s0.abort();
    s1.abort();
}

#[tokio::test]
async fn test_five_new_kv_etps_registry_and_auth_flows() {
    use velda_etp::aws_elasticache::AwsElastiCacheEtp;
    use velda_etp::gcp_memorystore::GcpMemorystoreEtp;

    // 1. Static password MUST be rejected for both AwsElastiCache and GcpMemorystore
    let bad_aws = make_entry(
        "aws-bad",
        EtpKind::AwsElastiCache,
        6379,
        1,
        serde_json::json!({
            "region": "us-east-1",
            "replication_group_id": "velda-rg",
            "username": "velda",
            "access_key_id": "AKIA",
            "secret_access_key": "SECRET",
            "password": "static-password-forbidden"
        }),
    );
    assert!(AwsElastiCacheEtp::from_entry(&bad_aws).is_err());

    let bad_gcp = make_entry(
        "gcp-bad",
        EtpKind::GcpMemorystore,
        6379,
        1,
        serde_json::json!({
            "iam_token_endpoint": "http://127.0.0.1:8080/token",
            "password": "static-password-forbidden"
        }),
    );
    assert!(GcpMemorystoreEtp::from_entry(&bad_gcp).is_err());

    // 2. Mock Valkey (standalone), Dragonfly, AwsElastiCache (IAM AUTH), GcpMemorystore (Metadata -> AUTH)
    let auth_log = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));
    let srv_log = Arc::clone(&auth_log);
    let resp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let resp_port = resp_listener.local_addr().unwrap().port();
    let resp_srv = tokio::spawn(async move {
        while let Ok((mut s, _)) = resp_listener.accept().await {
            let log = Arc::clone(&srv_log);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                if let Ok(n) = s.read(&mut buf).await
                    && n > 0
                {
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    log.lock().await.push(req);
                    let _ = s.write_all(b"+OK\r\n").await;
                }
            });
        }
    });

    // Mock GCE Metadata server returning short-lived access_token
    let meta_calls = Arc::new(AtomicUsize::new(0));
    let meta_calls_c = Arc::clone(&meta_calls);
    let meta_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meta_port = meta_listener.local_addr().unwrap().port();
    let meta_srv = tokio::spawn(async move {
        while let Ok((mut s, _)) = meta_listener.accept().await {
            let c = Arc::clone(&meta_calls_c);
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = s.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.contains("Metadata-Flavor: Google") {
                    c.fetch_add(1, Ordering::SeqCst);
                    let body = r#"{"access_token":"ya29.mock-gcp-iam","expires_in":3600}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.flush().await;
                }
            });
        }
    });

    let entries = vec![
        make_entry(
            "vk-main",
            EtpKind::Valkey,
            resp_port,
            1,
            serde_json::json!({ "password": "vk-pw" }),
        ),
        make_entry(
            "df-main",
            EtpKind::Dragonfly,
            resp_port,
            1,
            serde_json::json!({ "password": "df-pw" }),
        ),
        make_entry(
            "aws-main",
            EtpKind::AwsElastiCache,
            resp_port,
            1,
            serde_json::json!({
                "region": "us-east-1",
                "replication_group_id": "velda-rg",
                "username": "velda_edge",
                "access_key_id": "AKIAEXAMPLE",
                "secret_access_key": "SECRETEXAMPLE"
            }),
        ),
        make_entry(
            "gcp-main",
            EtpKind::GcpMemorystore,
            resp_port,
            1,
            serde_json::json!({
                "username": "gcp-iam-user",
                "iam_token_endpoint": format!("http://127.0.0.1:{meta_port}/token")
            }),
        ),
        EtpEntryConfig {
            id: "up-main".into(),
            kind: EtpKind::Upstash,
            security: EtpTransportSecurity::Tls,
            pool: EtpPoolSettings {
                pool_size: 1,
                connect_timeout_ms: 500,
                idle_timeout_ms: 5000,
            },
            tls: None,
            settings: serde_json::json!({
                "host": "us1-example.upstash.io",
                "token": "upstash-bearer-secret"
            }),
        },
    ];

    let reg = build_registry(&entries).unwrap();
    let EtpBackend::Valkey(vk) = &reg.entries["vk-main"].backend else {
        panic!("expected Valkey backend");
    };
    let EtpBackend::Dragonfly(df) = &reg.entries["df-main"].backend else {
        panic!("expected Dragonfly backend");
    };
    let EtpBackend::AwsElastiCache(aws) = &reg.entries["aws-main"].backend else {
        panic!("expected AwsElastiCache backend");
    };
    let EtpBackend::GcpMemorystore(gcp) = &reg.entries["gcp-main"].backend else {
        panic!("expected GcpMemorystore backend");
    };
    let EtpBackend::Upstash(up) = &reg.entries["up-main"].backend else {
        panic!("expected Upstash backend");
    };

    assert_eq!(up.config.port, 443);
    assert!(up.config.tls.is_some());
    assert_eq!(up.config.token, "upstash-bearer-secret");

    // Acquire leases to exercise live handshakes + token caching
    let _vk_l = vk.acquire().await.unwrap();
    let _df_l = df.acquire().await.unwrap();
    let _aws_l = aws.acquire().await.unwrap();
    let _gcp_l1 = gcp.acquire().await.unwrap();
    drop(_gcp_l1);
    // Warm / second connect reuses cached GCP OAuth2 token (metadata hit count stays 1)
    gcp.warm().await.unwrap();
    assert_eq!(meta_calls.load(Ordering::SeqCst), 1);

    let logs = auth_log.lock().await;
    assert!(
        logs.iter()
            .any(|s| s.contains("velda-rg/?Action=connect&User=velda_edge"))
    );
    assert!(
        logs.iter()
            .any(|s| s.contains("gcp-iam-user") && s.contains("ya29.mock-gcp-iam"))
    );

    resp_srv.abort();
    meta_srv.abort();
}

#[test]
fn test_explicit_security_validation_and_strict_host_port() {
    use velda_etp::registry::build_instance;

    // 1. Missing 'security' field must be rejected in JSON deserialization
    let missing_sec_json = serde_json::json!({
        "id": "no-sec",
        "kind": "redis",
        "settings": { "host": "127.0.0.1", "port": 6379 }
    });
    assert!(serde_json::from_value::<EtpEntryConfig>(missing_sec_json).is_err());

    // 2. Plain cannot attach 'tls'
    let plain_with_tls = EtpEntryConfig {
        id: "plain-tls".into(),
        kind: EtpKind::Redis,
        security: EtpTransportSecurity::Plain,
        pool: EtpPoolSettings::default(),
        tls: Some(EtpTlsConfig::default()),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": 6379 }),
    };
    assert!(build_instance(&plain_with_tls).is_err());

    // 3. Tls defaults to WebPKI config when tls: None, and rejects client cert/key
    let tls_auto_roots = EtpEntryConfig {
        id: "tls-default".into(),
        kind: EtpKind::Otlp,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "host": "otel.example.com" }),
    };
    let inst = build_instance(&tls_auto_roots).unwrap();
    let EtpBackend::Otlp(otlp) = &inst.backend else {
        panic!("expected Otlp");
    };
    assert_eq!(otlp.config.port, 443);
    assert!(otlp.config.tls.is_some());

    let cert = rcgen::generate_simple_self_signed(vec!["client.local".to_string()]).unwrap();
    let tls_with_client_cert = EtpEntryConfig {
        id: "tls-client-cert".into(),
        kind: EtpKind::Otlp,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings::default(),
        tls: Some(EtpTlsConfig {
            client_cert_pem: Some(cert.cert.pem()),
            client_key_pem: Some(cert.signing_key.serialize_pem()),
            ..Default::default()
        }),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": 443 }),
    };
    assert!(build_instance(&tls_with_client_cert).is_err());

    // 4. Mtls requires tls + both client_cert_pem and client_key_pem
    let mtls_missing_tls = EtpEntryConfig {
        id: "mtls-no-tls".into(),
        kind: EtpKind::Otlp,
        security: EtpTransportSecurity::Mtls,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "host": "127.0.0.1", "port": 443 }),
    };
    assert!(build_instance(&mtls_missing_tls).is_err());

    // 5. Provider-specific security matrix enforcement
    for bad_sec in [
        EtpTransportSecurity::Tls,
        EtpTransportSecurity::Mtls,
        EtpTransportSecurity::StartTls,
    ] {
        let bad_statsd = EtpEntryConfig {
            id: "statsd-bad".into(),
            kind: EtpKind::Statsd,
            security: bad_sec,
            pool: EtpPoolSettings::default(),
            tls: (bad_sec == EtpTransportSecurity::Mtls).then(|| EtpTlsConfig {
                client_cert_pem: Some(cert.cert.pem()),
                client_key_pem: Some(cert.signing_key.serialize_pem()),
                ..Default::default()
            }),
            settings: serde_json::json!({ "host": "127.0.0.1", "port": 8125 }),
        };
        assert!(build_instance(&bad_statsd).is_err());
    }

    for kind in [EtpKind::Postgres, EtpKind::MySql, EtpKind::Nats] {
        for bad_sec in [EtpTransportSecurity::Tls, EtpTransportSecurity::Mtls] {
            let bad_db = EtpEntryConfig {
                id: "db-direct-tls".into(),
                kind,
                security: bad_sec,
                pool: EtpPoolSettings::default(),
                tls: (bad_sec == EtpTransportSecurity::Mtls).then(|| EtpTlsConfig {
                    client_cert_pem: Some(cert.cert.pem()),
                    client_key_pem: Some(cert.signing_key.serialize_pem()),
                    ..Default::default()
                }),
                settings: serde_json::json!({ "host": "127.0.0.1" }),
            };
            assert!(
                build_instance(&bad_db).is_err(),
                "{kind:?} must reject direct {bad_sec}"
            );
        }
    }

    let bad_redis_starttls = EtpEntryConfig {
        id: "redis-starttls".into(),
        kind: EtpKind::Redis,
        security: EtpTransportSecurity::StartTls,
        pool: EtpPoolSettings::default(),
        tls: None,
        settings: serde_json::json!({ "host": "127.0.0.1" }),
    };
    assert!(build_instance(&bad_redis_starttls).is_err());

    // 6. URL / scheme / embedded port strictly rejected
    for bad_settings in [
        serde_json::json!({ "endpoint": "https://127.0.0.1:443" }),
        serde_json::json!({ "url": "http://127.0.0.1:4318" }),
        serde_json::json!({ "host": "https://127.0.0.1", "port": 443 }),
        serde_json::json!({ "host": "127.0.0.1:4318" }),
        serde_json::json!({ "host": "127.0.0.1/custom" }),
        serde_json::json!({ "host": "127.0.0.1", "base_path": "https://evil.local/p" }),
    ] {
        let bad = EtpEntryConfig {
            id: "url-rejected".into(),
            kind: EtpKind::Otlp,
            security: EtpTransportSecurity::Plain,
            pool: EtpPoolSettings::default(),
            tls: None,
            settings: bad_settings,
        };
        assert!(bad.validate().is_err());
    }

    // 7. Fingerprint reflects security change
    let mut fp_b = tls_auto_roots.clone();
    fp_b.security = EtpTransportSecurity::Plain;
    assert_ne!(tls_auto_roots.fingerprint(), fp_b.fingerprint());
}

#[tokio::test]
async fn test_mtls_stream_and_pool_auth_accept_and_reject_missing_cert() {
    let srv_cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let srv_cert_pem = srv_cert.cert.pem();
    let srv_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(srv_cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let srv_key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        srv_cert.signing_key.serialize_pem().as_bytes(),
    ))
    .unwrap()
    .unwrap();

    let cli_cert = rcgen::generate_simple_self_signed(vec!["velda-client".to_string()]).unwrap();
    let cli_cert_pem = cli_cert.cert.pem();
    let cli_key_pem = cli_cert.signing_key.serialize_pem();
    let cli_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(cli_cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

    let mut client_roots = rustls::RootCertStore::empty();
    for c in cli_certs {
        client_roots.add(c).unwrap();
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(client_roots),
        Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
    )
    .build()
    .unwrap();

    let server_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_client_cert_verifier(verifier)
    .with_single_cert(srv_certs, srv_key)
    .unwrap();

    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server_task = tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut tls) = acceptor.accept(tcp).await {
                    let mut buf = [0u8; 4];
                    if tls.read_exact(&mut buf).await.is_ok() {
                        let _ = tls.write_all(&buf).await;
                        let _ = tls.flush().await;
                    }
                }
            });
        }
    });

    // mTLS entry with valid client cert + key succeeds
    let mtls_entry = EtpEntryConfig {
        id: "otlp-mtls".into(),
        kind: EtpKind::Otlp,
        security: EtpTransportSecurity::Mtls,
        pool: EtpPoolSettings {
            pool_size: 2,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: Some(EtpTlsConfig {
            ca_cert_pem: Some(srv_cert_pem.clone()),
            client_cert_pem: Some(cli_cert_pem),
            client_key_pem: Some(cli_key_pem),
            server_name: Some("localhost".into()),
            ..Default::default()
        }),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": port }),
    };
    let mtls_pool = Arc::new(OtlpEtp::from_entry(&mtls_entry).unwrap());
    let mut lease = mtls_pool.acquire().await.unwrap();
    {
        let OtlpConn::Http(ref mut s) = *lease else {
            panic!("expected HTTP OtlpConn");
        };
        assert!(s.is_tls());
        s.write_all(b"mtls").await.unwrap();
        s.flush().await.unwrap();
        let mut resp = [0u8; 4];
        s.read_exact(&mut resp).await.unwrap();
        assert_eq!(&resp, b"mtls");
    }

    // Server-TLS entry WITHOUT client cert fails handshake against mTLS server
    let no_cert_entry = EtpEntryConfig {
        id: "otlp-no-cli".into(),
        kind: EtpKind::Otlp,
        security: EtpTransportSecurity::Tls,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: Some(EtpTlsConfig {
            ca_cert_pem: Some(srv_cert_pem),
            server_name: Some("localhost".into()),
            ..Default::default()
        }),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": port }),
    };
    let no_cert_pool = Arc::new(OtlpEtp::from_entry(&no_cert_entry).unwrap());
    let mut no_cert_lease = no_cert_pool.acquire().await.unwrap();
    let OtlpConn::Http(ref mut bad_s) = *no_cert_lease else {
        panic!("expected HTTP OtlpConn");
    };
    bad_s.write_all(b"ping").await.ok();
    let mut buf = [0u8; 4];
    assert!(bad_s.read_exact(&mut buf).await.is_err());

    server_task.abort();
}

#[tokio::test]
async fn test_postgres_mysql_nats_start_tls_negotiation_and_auth() {
    use velda_etp::mysql::MysqlEtp;
    use velda_etp::nats::NatsEtp;
    use velda_etp::postgres::PostgresEtp;

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_pem = cert.cert.pem();
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(cert_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        cert.signing_key.serialize_pem().as_bytes(),
    ))
    .unwrap()
    .unwrap();
    let server_cfg = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));

    // 1. Mock Postgres StartTLS (SSLRequest -> 'S' -> TLS -> StartupMessage -> ReadyForQuery)
    let pg_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let pg_port = pg_listener.local_addr().unwrap().port();
    let pg_acc = acceptor.clone();
    let pg_srv = tokio::spawn(async move {
        let (mut tcp, _) = pg_listener.accept().await.unwrap();
        let mut ssl_req = [0u8; 8];
        tcp.read_exact(&mut ssl_req).await.unwrap();
        assert_eq!(ssl_req, [0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f]);
        tcp.write_all(b"S").await.unwrap();
        tcp.flush().await.unwrap();

        let mut tls = pg_acc.accept(tcp).await.unwrap();
        let mut startup_hdr = [0u8; 4];
        tls.read_exact(&mut startup_hdr).await.unwrap();
        let startup_len = u32::from_be_bytes(startup_hdr) as usize;
        let mut rest = vec![0u8; startup_len - 4];
        tls.read_exact(&mut rest).await.unwrap();

        let mut ok = vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0];
        ok.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
        tls.write_all(&ok).await.unwrap();
        tls.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let pg_entry = EtpEntryConfig {
        id: "pg-starttls".into(),
        kind: EtpKind::Postgres,
        security: EtpTransportSecurity::StartTls,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: Some(EtpTlsConfig {
            ca_cert_pem: Some(cert_pem.clone()),
            server_name: Some("localhost".into()),
            ..Default::default()
        }),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": pg_port }),
    };
    let pg_pool = Arc::new(PostgresEtp::from_entry(&pg_entry).unwrap());
    let pg_lease = pg_pool.acquire().await.unwrap();
    assert!(pg_lease.is_tls());
    drop(pg_lease);

    // 2. Mock MySQL StartTLS (HandshakeV10 w/ CLIENT_SSL -> SSLRequest -> TLS -> HandshakeResponse41 -> OK)
    let my_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let my_port = my_listener.local_addr().unwrap().port();
    let my_acc = acceptor.clone();
    let my_srv = tokio::spawn(async move {
        let (mut tcp, _) = my_listener.accept().await.unwrap();
        let mut hs = vec![0x0a];
        hs.extend_from_slice(b"8.0.36\0");
        hs.extend_from_slice(&1u32.to_le_bytes());
        hs.extend_from_slice(&[0x11u8; 8]);
        hs.push(0);
        hs.extend_from_slice(&0xffffu16.to_le_bytes()); // includes CLIENT_SSL (0x0800)
        hs.push(45);
        hs.extend_from_slice(&2u16.to_le_bytes());
        hs.extend_from_slice(&0x0008u16.to_le_bytes());
        hs.push(21);
        hs.extend_from_slice(&[0u8; 10]);
        hs.extend_from_slice(&[0x22u8; 12]);
        hs.push(0);
        hs.extend_from_slice(b"mysql_native_password\0");

        let len = hs.len() as u32;
        let mut pkt = vec![
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
            0,
        ];
        pkt.extend_from_slice(&hs);
        tcp.write_all(&pkt).await.unwrap();
        tcp.flush().await.unwrap();

        let mut hdr = [0u8; 4];
        tcp.read_exact(&mut hdr).await.unwrap();
        assert_eq!(hdr[3], 1); // SSLRequest seq = 1
        let ssl_len = (hdr[0] as usize) | ((hdr[1] as usize) << 8) | ((hdr[2] as usize) << 16);
        assert_eq!(ssl_len, 32);
        let mut ssl_payload = [0u8; 32];
        tcp.read_exact(&mut ssl_payload).await.unwrap();
        let client_caps = u32::from_le_bytes(ssl_payload[0..4].try_into().unwrap());
        assert_ne!(client_caps & 0x0800, 0); // CLIENT_SSL set

        let mut tls = my_acc.accept(tcp).await.unwrap();
        let mut auth_hdr = [0u8; 4];
        tls.read_exact(&mut auth_hdr).await.unwrap();
        assert_eq!(auth_hdr[3], 2); // HandshakeResponse41 seq = 2
        let auth_len =
            (auth_hdr[0] as usize) | ((auth_hdr[1] as usize) << 8) | ((auth_hdr[2] as usize) << 16);
        let mut auth_body = vec![0u8; auth_len];
        tls.read_exact(&mut auth_body).await.unwrap();

        // Send MySQL OK packet (seq=3)
        let ok = [7u8, 0, 0, 3, 0x00, 0, 0, 2, 0, 0, 0];
        tls.write_all(&ok).await.unwrap();
        tls.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let my_entry = EtpEntryConfig {
        id: "my-starttls".into(),
        kind: EtpKind::MySql,
        security: EtpTransportSecurity::StartTls,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: Some(EtpTlsConfig {
            ca_cert_pem: Some(cert_pem.clone()),
            server_name: Some("localhost".into()),
            ..Default::default()
        }),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": my_port }),
    };
    let my_pool = Arc::new(MysqlEtp::from_entry(&my_entry).unwrap());
    let my_lease = my_pool.acquire().await.unwrap();
    assert!(my_lease.is_tls());
    drop(my_lease);

    // 3. Mock NATS StartTLS (INFO -> TLS upgrade -> CONNECT + PING -> PONG)
    let nats_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let nats_port = nats_listener.local_addr().unwrap().port();
    let nats_srv = tokio::spawn(async move {
        let (mut tcp, _) = nats_listener.accept().await.unwrap();
        tcp.write_all(b"INFO {\"server_id\":\"test\",\"tls_required\":true}\r\n")
            .await
            .unwrap();
        tcp.flush().await.unwrap();

        let mut tls = acceptor.accept(tcp).await.unwrap();
        let mut buf = [0u8; 512];
        let n = tls.read(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(text.starts_with("CONNECT "));
        tls.write_all(b"PONG\r\n").await.unwrap();
        tls.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let nats_entry = EtpEntryConfig {
        id: "nats-starttls".into(),
        kind: EtpKind::Nats,
        security: EtpTransportSecurity::StartTls,
        pool: EtpPoolSettings {
            pool_size: 1,
            connect_timeout_ms: 2000,
            idle_timeout_ms: 60_000,
        },
        tls: Some(EtpTlsConfig {
            ca_cert_pem: Some(cert_pem),
            server_name: Some("localhost".into()),
            ..Default::default()
        }),
        settings: serde_json::json!({ "host": "127.0.0.1", "port": nats_port }),
    };
    let nats_pool = Arc::new(NatsEtp::from_entry(&nats_entry).unwrap());
    let nats_lease = nats_pool.acquire().await.unwrap();
    assert!(nats_lease.is_tls());
    drop(nats_lease);

    pg_srv.abort();
    my_srv.abort();
    nats_srv.abort();
}
