//! End-to-End Integration Test Suite for JWT Key Rotation (`jwt_auth` + `jwks_rotator`).
//!
//! Validates:
//! 1. Inter-plugin dependency wiring via [`wire_plugin_dependencies`].
//! 2. Multi-key ingestion (up to 8 slots) and static key preservation in Slot 0.
//! 3. Zero-downtime key rotation (Key 1 -> Key 1 + Key 2 -> Key 2).
//! 4. Clamping and boundary enforcement when IdP returns > 8 keys.
//! 5. Network outage resilience (IdP 500 error preserves existing in-memory table).
//! 6. Concurrent request validation during lock-free atomic pointer swap.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::prelude::*;
use http::header::HeaderValue;
use http::{HeaderMap, Method, Uri, Version};
use ring::hmac;
use rustc_hash::FxHashMap;
use serde_json::json;
use velda_core::{Action, Body, L7Request, RejectReason};
use velda_plugin::background::BackgroundPlugin;
use velda_plugin::background::jwks_rotator::JwksRotatorPlugin;
use velda_plugin::config::PluginConfig;
use velda_plugin::factory::{LoadedPlugin, wire_plugin_dependencies};
use velda_plugin::pre_upstream::PreUpstreamPlugin;
use velda_plugin::pre_upstream::jwt_auth::JwtAuthPlugin;

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ============================================================================
// Lightweight Mock JWKS HTTP Server
// ============================================================================

struct MockJwksServer {
    addr: SocketAddr,
    jwks_json: Arc<RwLock<String>>,
    error_mode: Arc<AtomicBool>,
    #[allow(dead_code)]
    request_count: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl Drop for MockJwksServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl MockJwksServer {
    fn spawn(initial_jwks: &str) -> Self {
        let listener =
            TcpListener::bind("127.0.0.1:0").expect("Failed to bind mock JWKS TCP listener");
        let addr = listener.local_addr().unwrap();
        let jwks_json = Arc::new(RwLock::new(initial_jwks.to_string()));
        let error_mode = Arc::new(AtomicBool::new(false));
        let request_count = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let json_clone = Arc::clone(&jwks_json);
        let error_clone = Arc::clone(&error_mode);
        let count_clone = Arc::clone(&request_count);
        let shutdown_clone = Arc::clone(&shutdown);

        thread::spawn(move || {
            let _ = listener.set_nonblocking(true);

            while !shutdown_clone.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let json_ref = Arc::clone(&json_clone);
                        let error_ref = Arc::clone(&error_clone);
                        let count_ref = Arc::clone(&count_clone);
                        let shutdown_ref = Arc::clone(&shutdown_clone);

                        thread::spawn(move || {
                            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                            let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));

                            let mut buf = vec![0u8; 4096];
                            let mut total_read = 0;

                            while !shutdown_ref.load(Ordering::Relaxed) {
                                match stream.read(&mut buf[total_read..]) {
                                    Ok(0) => break,
                                    Ok(n) => {
                                        total_read += n;
                                        let mut headers = [httparse::EMPTY_HEADER; 32];
                                        let mut req = httparse::Request::new(&mut headers);
                                        if let Ok(httparse::Status::Complete(_)) =
                                            req.parse(&buf[..total_read])
                                        {
                                            count_ref.fetch_add(1, Ordering::Relaxed);

                                            let (status, body) =
                                                if error_ref.load(Ordering::Relaxed) {
                                                    (
                                                        "500 Internal Server Error",
                                                        "{\"error\":\"idp_down\"}".to_string(),
                                                    )
                                                } else {
                                                    let current = json_ref.read().unwrap().clone();
                                                    ("200 OK", current)
                                                };

                                            let resp = format!(
                                                "HTTP/1.1 {status}\r\n\
                                                 Content-Type: application/json\r\n\
                                                 Content-Length: {}\r\n\
                                                 Connection: close\r\n\
                                                 \r\n\
                                                 {body}",
                                                body.len()
                                            );
                                            let _ = stream.write_all(resp.as_bytes());
                                            let _ = stream.flush();
                                            let _ = stream.shutdown(std::net::Shutdown::Both);
                                            break;
                                        }
                                    }
                                    Err(_) => break,
                                }
                            }
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });

        Self {
            addr,
            jwks_json,
            error_mode,
            request_count,
            shutdown,
        }
    }

    fn set_jwks(&self, new_jwks: &str) {
        let mut guard = self.jwks_json.write().unwrap();
        *guard = new_jwks.to_string();
    }

    fn set_error_mode(&self, error: bool) {
        self.error_mode.store(error, Ordering::Relaxed);
    }
}

// ============================================================================
// Helper Utilities for JWT Creation & Request Building
// ============================================================================

fn make_oct_jwk(kid: &str, secret_bytes: &[u8]) -> serde_json::Value {
    let k_b64 = BASE64_URL_SAFE_NO_PAD.encode(secret_bytes);
    json!({
        "kty": "oct",
        "kid": kid,
        "use": "sig",
        "alg": "HS256",
        "k": k_b64
    })
}

fn create_jwt_hs256(kid: Option<&str>, secret_bytes: &[u8], sub: &str) -> String {
    let header_val = if let Some(k) = kid {
        json!({"alg": "HS256", "typ": "JWT", "kid": k})
    } else {
        json!({"alg": "HS256", "typ": "JWT"})
    };
    let payload_val = json!({
        "sub": sub,
        "exp": 2500000000u64
    });

    let h_b64 = BASE64_URL_SAFE_NO_PAD.encode(header_val.to_string().as_bytes());
    let p_b64 = BASE64_URL_SAFE_NO_PAD.encode(payload_val.to_string().as_bytes());
    let signing_input = format!("{h_b64}.{p_b64}");

    let key = hmac::Key::new(hmac::HMAC_SHA256, secret_bytes);
    let sig = hmac::sign(&key, signing_input.as_bytes());
    let s_b64 = BASE64_URL_SAFE_NO_PAD.encode(sig.as_ref());

    format!("{signing_input}.{s_b64}")
}

fn make_jwt_plugin_config(id: &str, secret: &str) -> PluginConfig {
    PluginConfig {
        id: id.into(),
        config: json!({
            "algorithm": "HS256",
            "secret_key": secret,
            "public_key_pem": null,
            "token_sources": [
                { "type": "header", "name": "authorization", "prefix": "Bearer " }
            ],
            "validate_exp": true,
            "clock_skew_secs": 60,
            "issuer": null,
            "audience": null,
            "claims_to_headers": {},
            "assert_claims": {},
            "cache_ttl_secs": 0
        }),
    }
}

fn make_auth_request(token: &str) -> L7Request {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    let mut req = L7Request::new(
        Method::GET,
        Uri::from_str("http://localhost/api/v1/protected").unwrap(),
        Version::HTTP_11,
        headers,
        Body::Empty,
    );
    req.ingress_time_nanos = current_epoch_secs() * 1_000_000_000;
    req
}

fn evaluate_token(plugin: &JwtAuthPlugin, token: &str) -> Action {
    let mut req = make_auth_request(token);
    let mut target_upstream = 1;
    plugin.on_upstream_request(&mut req, &mut target_upstream)
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
async fn test_jwt_jwks_rotation_e2e_flow() {
    let static_secret = b"static_super_secret_key_32_bytes_long_ok";
    let key1_secret = b"dynamic_key_one_secret_thirtytwo_bytes!";
    let key2_secret = b"dynamic_key_two_secret_thirtytwo_bytes!";

    // 1. Initial Mock IdP with Key 1
    let jwks_initial = json!({
        "keys": [make_oct_jwk("key-1", key1_secret)]
    })
    .to_string();

    let idp = MockJwksServer::spawn(&jwks_initial);
    let jwks_uri = format!("http://{}/.well-known/jwks.json", idp.addr);

    // 2. Instantiate jwt_auth plugin with static key
    let jwt_config =
        make_jwt_plugin_config("auth_jwt", std::str::from_utf8(static_secret).unwrap());
    let jwt_plugin = Arc::new(JwtAuthPlugin::from_config(&jwt_config).unwrap());

    // 3. Instantiate jwks_rotator plugin targeting auth_jwt
    let rotator_config = PluginConfig {
        id: "rotator_jwks".into(),
        config: json!({
            "target_jwt_auth_id": "auth_jwt",
            "jwks_uri": jwks_uri,
            "interval_seconds": 10,
            "timeout_seconds": 5,
            "preserve_static_key": true
        }),
    };
    let rotator_plugin = Arc::new(JwksRotatorPlugin::from_config(&rotator_config).unwrap());

    // 4. Wire inter-plugin dependencies via factory helper
    let mut available = FxHashMap::default();
    available.insert(
        "auth_jwt".to_string(),
        LoadedPlugin::PreUpstream(Arc::clone(&jwt_plugin) as Arc<dyn PreUpstreamPlugin>),
    );
    available.insert(
        "rotator_jwks".to_string(),
        LoadedPlugin::Background(Arc::clone(&rotator_plugin) as Arc<dyn BackgroundPlugin>),
    );
    wire_plugin_dependencies(&available).expect("Inter-plugin wiring must succeed");

    // Tokens
    let token_static = create_jwt_hs256(None, static_secret, "user_static");
    let token_key1 = create_jwt_hs256(Some("key-1"), key1_secret, "user_key1");
    let token_key2 = create_jwt_hs256(Some("key-2"), key2_secret, "user_key2");

    // Before rotation: static key works, key-1 fails
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_static),
        Action::Continue
    ));
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key1),
        Action::Reject(RejectReason::Unauthorized)
    ));

    // 5. Execute initial rotation step
    rotator_plugin.execute_rotation_step().await.unwrap();

    // After initial rotation:
    // - Static token STILL works (Slot 0 preserved)
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_static),
        Action::Continue
    ));

    // - Key-1 token NOW works!
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key1),
        Action::Continue
    ));

    // - Key-2 token still fails
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key2),
        Action::Reject(RejectReason::Unauthorized)
    ));

    // 6. Transition Window: IdP adds Key 2 (both Key 1 and Key 2 valid)
    let jwks_transition = json!({
        "keys": [
            make_oct_jwk("key-1", key1_secret),
            make_oct_jwk("key-2", key2_secret)
        ]
    })
    .to_string();
    idp.set_jwks(&jwks_transition);

    rotator_plugin.execute_rotation_step().await.unwrap();

    // In transition: Static, Key-1, and Key-2 ALL work concurrently!
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_static),
        Action::Continue
    ));
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key1),
        Action::Continue
    ));
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key2),
        Action::Continue
    ));

    // 7. Revocation: IdP revokes Key 1, only serves Key 2
    let jwks_revoked = json!({
        "keys": [make_oct_jwk("key-2", key2_secret)]
    })
    .to_string();
    idp.set_jwks(&jwks_revoked);

    rotator_plugin.execute_rotation_step().await.unwrap();

    // Static key STILL works (Slot 0 preserved)
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_static),
        Action::Continue
    ));

    // Key-2 works
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key2),
        Action::Continue
    ));

    // Key-1 is now evicted -> REJECTED (Unauthorized)
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_key1),
        Action::Reject(RejectReason::Unauthorized)
    ));
}

#[tokio::test]
async fn test_jwks_slot_overflow_clamping() {
    let key_secret = b"dynamic_key_secret_thirtytwo_bytes_ok!!!";

    // IdP serves 12 keys (exceeds MAX_JWKS_SLOTS = 8)
    let mut keys = Vec::new();
    for i in 1..=12 {
        keys.push(make_oct_jwk(&format!("overflow-key-{i}"), key_secret));
    }
    let jwks_json = json!({ "keys": keys }).to_string();

    let idp = MockJwksServer::spawn(&jwks_json);
    let jwks_uri = format!("http://{}/.well-known/jwks.json", idp.addr);

    let jwt_config =
        make_jwt_plugin_config("auth_jwt_overflow", "static_key_here_thirtytwo_bytes_long!");
    let jwt_plugin = Arc::new(JwtAuthPlugin::from_config(&jwt_config).unwrap());

    let rotator_config = PluginConfig {
        id: "rotator_overflow".into(),
        config: json!({
            "target_jwt_auth_id": "auth_jwt_overflow",
            "jwks_uri": jwks_uri,
            "preserve_static_key": true
        }),
    };
    let rotator_plugin = Arc::new(JwksRotatorPlugin::from_config(&rotator_config).unwrap());
    let _ = rotator_plugin.attach_shared_jwks_store(jwt_plugin.shared_jwks_store().unwrap());

    // Execute rotation: must not panic, must clamp safely to 8 slots
    rotator_plugin.execute_rotation_step().await.unwrap();

    let table = jwt_plugin.key_store.load();
    assert_eq!(table.active_count, 8); // 1 static slot + 7 dynamic keys

    // Slot 0 (static) is active
    assert_eq!(table.slots[0].is_active, 1);

    // Dynamic keys 1..=7 are active
    for i in 1..=7 {
        let token = create_jwt_hs256(
            Some(&format!("overflow-key-{i}")),
            key_secret,
            &format!("user_{i}"),
        );
        assert!(matches!(
            evaluate_token(&jwt_plugin, &token),
            Action::Continue
        ));
    }

    // Dynamic key 8 was clamped out -> REJECTED
    let token_clamped = create_jwt_hs256(Some("overflow-key-8"), key_secret, "user_clamped");
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token_clamped),
        Action::Reject(RejectReason::Unauthorized)
    ));
}

#[tokio::test]
async fn test_jwks_idp_outage_resilience() {
    let key_secret = b"resilience_secret_thirtytwo_bytes_long!";
    let jwks_initial = json!({
        "keys": [make_oct_jwk("resilient-key", key_secret)]
    })
    .to_string();

    let idp = MockJwksServer::spawn(&jwks_initial);
    let jwks_uri = format!("http://{}/.well-known/jwks.json", idp.addr);

    let jwt_config =
        make_jwt_plugin_config("auth_jwt_resilience", "resilience_static_secret_32_bytes!");
    let jwt_plugin = Arc::new(JwtAuthPlugin::from_config(&jwt_config).unwrap());

    let rotator_config = PluginConfig {
        id: "rotator_resilience".into(),
        config: json!({
            "target_jwt_auth_id": "auth_jwt_resilience",
            "jwks_uri": jwks_uri,
            "preserve_static_key": false
        }),
    };
    let rotator_plugin = Arc::new(JwksRotatorPlugin::from_config(&rotator_config).unwrap());
    let _ = rotator_plugin.attach_shared_jwks_store(jwt_plugin.shared_jwks_store().unwrap());

    // 1. Successful rotation
    rotator_plugin.execute_rotation_step().await.unwrap();

    let token = create_jwt_hs256(Some("resilient-key"), key_secret, "user_resilient");
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token),
        Action::Continue
    ));

    // 2. IdP goes down (HTTP 500)
    idp.set_error_mode(true);

    // 3. Rotation step fails
    let err = rotator_plugin.execute_rotation_step().await;
    assert!(err.is_err(), "Rotation must report network/HTTP error");

    // 4. In-memory table is PRESERVED (LKG resilience)
    // Traffic continues serving seamlessly!
    assert!(matches!(
        evaluate_token(&jwt_plugin, &token),
        Action::Continue
    ));
}

#[tokio::test]
async fn test_concurrent_request_validation_during_rotation() {
    let key_secret = b"concurrent_key_secret_thirtytwo_bytes!";
    let jwks_json = json!({
        "keys": [make_oct_jwk("concurrent-key", key_secret)]
    })
    .to_string();

    let idp = MockJwksServer::spawn(&jwks_json);
    let jwks_uri = format!("http://{}/.well-known/jwks.json", idp.addr);

    let jwt_config =
        make_jwt_plugin_config("auth_jwt_concurrent", "concurrent_static_secret_32_bytes!");
    let jwt_plugin = Arc::new(JwtAuthPlugin::from_config(&jwt_config).unwrap());

    let rotator_config = PluginConfig {
        id: "rotator_concurrent".into(),
        config: json!({
            "target_jwt_auth_id": "auth_jwt_concurrent",
            "jwks_uri": jwks_uri,
            "preserve_static_key": true
        }),
    };
    let rotator_plugin = Arc::new(JwksRotatorPlugin::from_config(&rotator_config).unwrap());
    let _ = rotator_plugin.attach_shared_jwks_store(jwt_plugin.shared_jwks_store().unwrap());

    // Initial rotation
    rotator_plugin.execute_rotation_step().await.unwrap();

    let token = create_jwt_hs256(Some("concurrent-key"), key_secret, "user_concurrent");

    // Spawn 4 concurrent worker threads serving requests
    let stop_flag = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let p = Arc::clone(&jwt_plugin);
        let tok = token.clone();
        let stop = Arc::clone(&stop_flag);

        handles.push(tokio::spawn(async move {
            let mut validated = 0;
            while !stop.load(Ordering::Relaxed) {
                let act = evaluate_token(&p, &tok);
                assert!(matches!(act, Action::Continue));
                validated += 1;
                tokio::task::yield_now().await;
            }
            validated
        }));
    }

    // Simultaneously trigger 5 rotation steps in the background
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        rotator_plugin.execute_rotation_step().await.unwrap();
    }

    stop_flag.store(true, Ordering::Relaxed);

    let mut total_requests = 0;
    for h in handles {
        total_requests += h.await.unwrap();
    }

    assert!(
        total_requests > 100,
        "Expected multiple concurrent requests to succeed during rotation, got {total_requests}"
    );
}
