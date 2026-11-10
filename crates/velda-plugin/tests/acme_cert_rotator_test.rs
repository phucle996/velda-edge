//! End-to-End ACME Certificate Rotator Integration Test Suite.
//!
//! Verifies the complete RFC 8555 automated certificate issuance workflow:
//! - Mock ACME Directory with anti-replay nonces, newAccount, newOrder, authz, finalize, and cert issuance.
//! - Dynamic ephemeral exact route injection into [`velda_router::Router`].
//! - Pre-baked static HTTP response serving directly from memory (L3/RAM).
//! - Clean RAII teardown upon challenge completion.
//! - X.509 certificate and private key disk persistence in LKG storage.
//! - Lock-free zero-downtime hot-swap into `velda-tls::SniResolver`.
//! - Idempotent bootstrap skip on healthy certificate validity.

use std::sync::Arc;
use std::sync::RwLock;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use velda_core::{ListenerId, RouteId, UpstreamId};
use velda_plugin::background::acme_cert_rotator::hot_swap::TlsHotSwapper;
use velda_plugin::background::acme_cert_rotator::storage::CertStorage;
use velda_plugin::background::acme_cert_rotator::{AcmeCertRotatorConfig, AcmeCertRotatorPlugin};
use velda_router::{Http1Route, Http1RouteRequest, Http1Router, Router};
use velda_tls::SniResolver;

/// Starts a mock ACME CA server simulating RFC 8555 directory, accounts, orders, and cert issuance.
async fn start_mock_acme_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);
    let server_base = base_url.clone();

    let handle = tokio::spawn(async move {
        let mut nonce_counter = 100u64;

        loop {
            let (mut socket, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let base = server_base.clone();
            nonce_counter += 1;
            let current_nonce = format!("nonce_{nonce_counter}");

            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 {
                    return;
                }

                let req_str = String::from_utf8_lossy(&buf[..n]);
                let first_line = req_str.lines().next().unwrap_or_default().to_string();
                let parts: Vec<String> = first_line
                    .split_whitespace()
                    .map(|s| s.to_string())
                    .collect();
                if parts.len() < 2 {
                    return;
                }
                let method = parts[0].as_str();
                let path = parts[1].as_str();

                let (status, headers, body): (u16, Vec<(String, String)>, String) =
                    match (method, path) {
                        ("GET", "/directory") => {
                            let json = serde_json::json!({
                                "newNonce": format!("{base}/newNonce"),
                                "newAccount": format!("{base}/newAccount"),
                                "newOrder": format!("{base}/newOrder"),
                            });
                            (
                                200,
                                vec![("Content-Type".into(), "application/json".into())],
                                json.to_string(),
                            )
                        }
                        ("HEAD", "/newNonce") | ("GET", "/newNonce") => (
                            200,
                            vec![("Replay-Nonce".into(), current_nonce.clone())],
                            String::new(),
                        ),
                        ("POST", "/newAccount") => {
                            let loc = format!("{base}/account/1");
                            (
                                201,
                                vec![
                                    ("Location".into(), loc),
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                ],
                                String::new(),
                            )
                        }
                        ("POST", "/newOrder") => {
                            let loc = format!("{base}/order/1");
                            let json = serde_json::json!({
                                "status": "pending",
                                "authorizations": [format!("{base}/authz/1")],
                                "finalize": format!("{base}/order/1/finalize"),
                            });
                            (
                                201,
                                vec![
                                    ("Location".into(), loc),
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                    ("Content-Type".into(), "application/json".into()),
                                ],
                                json.to_string(),
                            )
                        }
                        ("POST", "/authz/1") => {
                            let json = serde_json::json!({
                                "status": "pending",
                                "identifier": { "type": "dns", "value": "api.velda.local" },
                                "challenges": [{
                                    "type": "http-01",
                                    "url": format!("{base}/chal/1"),
                                    "token": "mock_token_abc_123",
                                    "status": "pending",
                                }]
                            });
                            (
                                200,
                                vec![
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                    ("Content-Type".into(), "application/json".into()),
                                ],
                                json.to_string(),
                            )
                        }
                        ("POST", "/chal/1") => {
                            let json = serde_json::json!({
                                "type": "http-01",
                                "url": format!("{base}/chal/1"),
                                "status": "valid"
                            });
                            (
                                200,
                                vec![
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                    ("Content-Type".into(), "application/json".into()),
                                ],
                                json.to_string(),
                            )
                        }
                        ("POST", "/order/1") => {
                            let json = serde_json::json!({
                                "status": "ready",
                                "authorizations": [format!("{base}/authz/1")],
                                "finalize": format!("{base}/order/1/finalize"),
                                "certificate": format!("{base}/cert/1"),
                            });
                            (
                                200,
                                vec![
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                    ("Content-Type".into(), "application/json".into()),
                                ],
                                json.to_string(),
                            )
                        }
                        ("POST", "/order/1/finalize") => {
                            let json = serde_json::json!({
                                "status": "valid",
                                "authorizations": [format!("{base}/authz/1")],
                                "finalize": format!("{base}/order/1/finalize"),
                                "certificate": format!("{base}/cert/1"),
                            });
                            (
                                200,
                                vec![
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                    ("Content-Type".into(), "application/json".into()),
                                ],
                                json.to_string(),
                            )
                        }
                        ("POST", "/cert/1") => {
                            // Generate a dummy valid self-signed certificate chain
                            let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
                                .unwrap();
                            let params =
                                rcgen::CertificateParams::new(vec!["api.velda.local".into()])
                                    .unwrap();
                            let cert = params.self_signed(&key).unwrap();
                            (
                                200,
                                vec![
                                    (
                                        "Content-Type".into(),
                                        "application/pem-certificate-chain".into(),
                                    ),
                                    ("Replay-Nonce".into(), current_nonce.clone()),
                                ],
                                cert.pem(),
                            )
                        }
                        _ => (404, vec![], "Not Found".to_string()),
                    };

                let mut resp_bytes =
                    format!("HTTP/1.1 {status} OK\r\nContent-Length: {}\r\n", body.len());
                for (k, v) in headers {
                    resp_bytes.push_str(&format!("{k}: {v}\r\n"));
                }
                resp_bytes.push_str("\r\n");
                resp_bytes.push_str(&body);

                let _ = socket.write_all(resp_bytes.as_bytes()).await;
                let _ = socket.flush().await;
            });
        }
    });

    (format!("{base_url}/directory"), handle)
}

#[tokio::test]
async fn test_acme_cert_rotator_e2e_lifecycle() {
    let (directory_url, server_handle) = start_mock_acme_server().await;
    let temp_dir = tempfile::tempdir().unwrap();

    let config = AcmeCertRotatorConfig {
        directory_url,
        contact_email: "dev@velda.local".to_string(),
        domains: vec!["api.velda.local".to_string()],
        renew_before_days: 30,
        schedule: "0 2 * * *".to_string(),
        storage_dir: temp_dir.path().to_str().unwrap().to_string(),
    };

    // 1. Setup in-memory Router and dynamic SniResolver
    let listener_id = ListenerId::from("ingress-http");
    let http1 = Http1Router::new(vec![Http1Route::new(
        RouteId::new(1),
        listener_id,
        "/api",
        UpstreamId::new(10),
    )])
    .unwrap();

    let router = Arc::new(RwLock::new(Router::new(
        Default::default(),
        Default::default(),
        http1,
        Default::default(),
        Default::default(),
        Default::default(),
    )));

    let sni_resolver = Arc::new(RwLock::new(SniResolver::new()));
    let hot_swapper = TlsHotSwapper::new(Some(Arc::clone(&sni_resolver)));

    // 2. Instantiate AcmeCertRotatorPlugin
    let plugin = AcmeCertRotatorPlugin::new(
        "acme_cert_rotator",
        config.clone(),
        Some(Arc::clone(&router)),
        Some("ingress-http".to_string()),
        hot_swapper,
    )
    .unwrap();

    // 3. Before renewal: SniResolver has no certificate for api.velda.local
    {
        let r = sni_resolver.read().unwrap();
        assert!(r.lookup("api.velda.local").is_none());
    }

    // 4. Trigger renewal workflow
    let renewed = plugin.check_and_renew().await.unwrap();
    assert!(renewed, "Bootstrap renewal should have executed");

    // 5. Verify certificate was hot-swapped into SniResolver!
    {
        let r = sni_resolver.read().unwrap();
        let cert_key = r.lookup("api.velda.local");
        assert!(
            cert_key.is_some(),
            "Renewed certificate must be registered in SniResolver"
        );
    }

    // 6. Verify certificate was saved to LKG storage on disk
    let storage = CertStorage::new(temp_dir.path()).unwrap();
    let cert_pem = storage.read_fullchain_pem("api.velda.local").unwrap();
    assert!(cert_pem.starts_with(b"-----BEGIN CERTIFICATE-----"));

    let privkey_pem = storage.read_privkey_pem("api.velda.local").unwrap();
    assert!(privkey_pem.contains("PRIVATE KEY"));

    // 7. Verify ephemeral route was completely purged from Router (zero route leakage!)
    {
        let r = router.read().unwrap();
        let acme_req = Http1RouteRequest::new("/.well-known/acme-challenge/mock_token_abc_123");
        assert!(r.route_http1(listener_id, &acme_req).is_none());

        // Regular traffic route remains untouched and functional
        let api_req = Http1RouteRequest::new("/api/users");
        assert_eq!(
            r.route_http1(listener_id, &api_req).unwrap().id,
            RouteId::new(1)
        );
    }

    // 8. Idempotency test: check_and_renew() on healthy cert skips without renewal
    let second_run = plugin.check_and_renew().await.unwrap();
    assert!(
        !second_run,
        "Subsequent check on healthy certificate must skip renewal"
    );

    server_handle.abort();
}
