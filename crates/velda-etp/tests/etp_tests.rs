use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};
use velda_etp::arena::{ArenaControlLine, ArenaSlot, PAGE_SIZE};
use velda_etp::connect::{ColdTargetAddr, EtpStream, sha256_u64x4};
use velda_etp::kafka::KafkaEtp;
use velda_etp::otlp::OtlpEtp;
use velda_etp::registry::{build_registry, reconcile, warm_all, warm_instance};
use velda_etp::{
    EtpBackend, EtpEntryConfig, EtpError, EtpKind, EtpPoolSettings, EtpRegistry, EtpTlsConfig,
    EtpTlsVersion,
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
    assert_eq!(redis_pool.arena.ready_count(), 2);
    assert_eq!(accepted.load(Ordering::SeqCst), 2);

    {
        let mut lease = redis_pool.acquire().await.unwrap();
        assert_eq!(redis_pool.arena.ready_count(), 1);
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
    assert_eq!(redis_pool.arena.ready_count(), 2);

    {
        let mut lease = redis_pool.try_acquire().expect("warm slot available");
        assert_eq!(redis_pool.arena.ready_count(), 1);
        lease.recreate_in_background();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while redis_pool.arena.ready_count() < 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(redis_pool.arena.ready_count(), 2);
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

    inflight_loki_lease.write_all(b"old-ok").await.unwrap();
    let mut buf = [0u8; 6];
    inflight_loki_lease.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"old-ok");
    drop(inflight_loki_lease);

    let EtpBackend::Loki(loki2) = &reg2.entries.get("loki-logs").unwrap().backend else {
        panic!("expected Loki backend");
    };
    let mut new_lease = loki2.acquire().await.unwrap();
    new_lease.write_all(b"new-ok").await.unwrap();
    new_lease.read_exact(&mut buf).await.unwrap();
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
        let p = (&*lease) as *const EtpStream as usize;
        assert!(p >= r1_start && p < r1_end);
        p
    };
    let ptr_second = {
        let lease = otlp1.acquire().await.unwrap();
        assert_eq!(lease.slot_idx, 0);
        (&*lease) as *const EtpStream as usize
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
    let burst_ptr = (&*burst) as *const EtpStream as usize;
    assert!(burst_ptr >= r1_start && burst_ptr < r1_end);

    burst.write_all(b"burst").await.unwrap();
    let mut reply = [0u8; 5];
    burst.read_exact(&mut reply).await.unwrap();
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
        l1.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, expected_tag);

        l1.reconnect().await.unwrap();
        l1.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, expected_tag);
    }

    let mut e1 = make_entry("fp-tls", EtpKind::Otlp, port, 2, serde_json::json!({}));
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
    loki_lease.write_all(b"loki-ping").await.unwrap();
    loki_lease.flush().await.unwrap();
    let mut reply = [0u8; 9];
    loki_lease.read_exact(&mut reply).await.unwrap();
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
    otlp_lease.write_all(b"otlp-ok").await.unwrap();
    otlp_lease.flush().await.unwrap();
    otlp_lease.read_exact(&mut reply7).await.unwrap();
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

    assert_eq!(pool.semaphore.available_permits(), pool.arena.total_slots);
    assert_eq!(pool.arena.baseline_active_count(), 0);

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
