//! Comprehensive End-to-End Test Suite for `oauth2_introspection` Pre-Upstream Plugin.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::header::HeaderValue;
use http::{HeaderMap, Method, StatusCode, Uri, Version};
use velda_core::{Action, Body, L7Request, L7Response};
use velda_plugin::PreUpstreamPlugin;
use velda_plugin::pre_upstream::{
    ClaimToHeaderConfig, Oauth2IntrospectionConfig, Oauth2IntrospectionPlugin,
};

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn extract_body_str(resp: &L7Response) -> String {
    String::from_utf8_lossy(resp.body.as_slice()).to_string()
}

fn test_req(method: Method, uri: &str, headers: &[(&str, &str)]) -> L7Request {
    let mut map = HeaderMap::new();
    for (k, v) in headers {
        map.insert(
            http::header::HeaderName::from_str(k).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    L7Request::new(
        method,
        Uri::from_str(uri).unwrap(),
        Version::HTTP_11,
        map,
        Body::Empty,
    )
}

/// Lightweight RAII Mock Authorization Server supporting HTTP/1.1 Keep-Alive.
struct MockIdp {
    addr: SocketAddr,
    request_count: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl Drop for MockIdp {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn spawn_mock_idp_server() -> MockIdp {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind mock IdP listener");
    let addr = listener.local_addr().unwrap();
    let request_count = Arc::new(AtomicUsize::new(0));
    let shutdown = Arc::new(AtomicBool::new(false));

    let count_clone = Arc::clone(&request_count);
    let shutdown_clone = Arc::clone(&shutdown);

    thread::spawn(move || {
        let _ = listener.set_nonblocking(true);

        while !shutdown_clone.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let count_ref = Arc::clone(&count_clone);
                    let shutdown_ref = Arc::clone(&shutdown_clone);

                    thread::spawn(move || {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(1000)));
                        let _ = stream.set_write_timeout(Some(Duration::from_millis(1000)));

                        let mut buf = vec![0u8; 4096];
                        let mut total_read = 0;

                        while !shutdown_ref.load(Ordering::Relaxed) {
                            match stream.read(&mut buf[total_read..]) {
                                Ok(0) => break,
                                Ok(n) => {
                                    total_read += n;

                                    let mut headers = [httparse::EMPTY_HEADER; 32];
                                    let mut req = httparse::Request::new(&mut headers);
                                    if let Ok(httparse::Status::Complete(body_start)) =
                                        req.parse(&buf[..total_read])
                                    {
                                        count_ref.fetch_add(1, Ordering::Relaxed);
                                        let body_bytes = &buf[body_start..total_read];
                                        let body_str = String::from_utf8_lossy(body_bytes);

                                        let json_resp = if body_str.contains("token=tok_valid_01") {
                                            let now = current_epoch_secs();
                                            format!(
                                                "{{\"active\":true,\"sub\":\"usr_9988\",\"client_id\":\"client_portal\",\"scope\":\"read orders\",\"role\":\"admin\",\"exp\":{}}}",
                                                now + 3600
                                            )
                                        } else if body_str.contains("token=tok_read_only") {
                                            "{\"active\":true,\"sub\":\"usr_1111\",\"client_id\":\"client_read\",\"scope\":\"read\"}".to_string()
                                        } else if body_str.contains("token=tok_expired_02") {
                                            "{\"active\":true,\"sub\":\"usr_2222\",\"exp\":1000}"
                                                .to_string()
                                        } else {
                                            "{\"active\":false}".to_string()
                                        };

                                        let response_str = format!(
                                            "HTTP/1.1 200 OK\r\n\
                                             Content-Type: application/json\r\n\
                                             Content-Length: {}\r\n\
                                             Connection: keep-alive\r\n\
                                             \r\n\
                                             {}",
                                            json_resp.len(),
                                            json_resp
                                        );

                                        if stream.write_all(response_str.as_bytes()).is_err() {
                                            break;
                                        }
                                        total_read = 0;
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

    MockIdp {
        addr,
        request_count,
        shutdown,
    }
}

fn build_test_plugin(
    idp_addr: SocketAddr,
    required_scopes: Vec<String>,
    fallback_behavior: &str,
    hide_credentials: bool,
) -> Oauth2IntrospectionPlugin {
    let config = Oauth2IntrospectionConfig {
        introspection_url: format!("http://{idp_addr}/oauth/introspect"),
        client_id: "test-gateway-client".to_string(),
        client_secret: "test-gateway-secret-12345".to_string(),
        auth_method: "client_secret_basic".to_string(),
        token_headers: vec!["x-access-token".to_string(), "authorization".to_string()],
        token_cookies: vec!["access_token".to_string()],
        token_query_params: vec!["token".to_string()],
        token_type_hint: "access_token".to_string(),
        cache_ttl_seconds: 300,
        negative_cache_ttl_seconds: 10,
        max_cache_entries: 1000,
        keepalive_timeout_secs: 60,
        pool_max_idle_connections: 5,
        request_timeout_ms: 1000,
        required_scopes,
        claims_to_headers: vec![
            ClaimToHeaderConfig {
                claim: "sub".to_string(),
                header: "x-user-id".to_string(),
            },
            ClaimToHeaderConfig {
                claim: "client_id".to_string(),
                header: "x-client-id".to_string(),
            },
            ClaimToHeaderConfig {
                claim: "role".to_string(),
                header: "x-user-role".to_string(),
            },
        ],
        hide_credentials,
        fallback_behavior: fallback_behavior.to_string(),
    };

    Oauth2IntrospectionPlugin::new("oauth2-test", config).unwrap()
}

#[test]
fn test_oauth2_introspection_e2e_valid_header_and_cache_hit() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(
        idp.addr,
        vec!["read".to_string(), "orders".to_string()],
        "reject",
        true,
    );
    let mut target = Arc::from("orders-upstream");

    // 1st Request: Cache Miss -> calls IdP
    let mut req1 = test_req(
        Method::GET,
        "/v1/orders",
        &[("x-access-token", "tok_valid_01")],
    );
    let action1 = plugin.on_upstream_request(&mut req1, &mut target);
    assert!(matches!(action1, Action::Continue));
    assert_eq!(idp.request_count.load(Ordering::Relaxed), 1);
    assert_eq!(req1.headers.get("x-user-id").unwrap(), "usr_9988");
    assert_eq!(req1.headers.get("x-client-id").unwrap(), "client_portal");
    assert_eq!(req1.headers.get("x-user-role").unwrap(), "admin");
    assert!(req1.headers.get("x-access-token").is_none());

    // 2nd Request: MUST HIT IN-MEMORY CACHE (sub-20ns, request_count unchanged)
    let mut req2 = test_req(
        Method::GET,
        "/v1/orders",
        &[("x-access-token", "tok_valid_01")],
    );
    let action2 = plugin.on_upstream_request(&mut req2, &mut target);
    assert!(matches!(action2, Action::Continue));
    assert_eq!(
        idp.request_count.load(Ordering::Relaxed),
        1,
        "Second request must be served from RAM cache!"
    );
    assert_eq!(req2.headers.get("x-user-id").unwrap(), "usr_9988");
}

#[test]
fn test_oauth2_introspection_e2e_authorization_bearer_extraction() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec!["read".to_string()], "reject", true);
    let mut target = Arc::from("profile-upstream");

    let mut req = test_req(
        Method::GET,
        "/v1/profile",
        &[("authorization", "Bearer tok_valid_01")],
    );
    let action = plugin.on_upstream_request(&mut req, &mut target);
    assert!(matches!(action, Action::Continue));
    assert_eq!(req.headers.get("x-user-id").unwrap(), "usr_9988");
    assert!(req.headers.get("authorization").is_none());
}

#[test]
fn test_oauth2_introspection_e2e_cookie_extraction() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec!["read".to_string()], "reject", false);
    let mut target = Arc::from("profile-upstream");

    let mut req = test_req(
        Method::GET,
        "/v1/profile",
        &[(
            "cookie",
            "session_id=xyz; access_token=tok_valid_01; theme=dark",
        )],
    );
    let action = plugin.on_upstream_request(&mut req, &mut target);
    assert!(matches!(action, Action::Continue));
    assert_eq!(req.headers.get("x-user-id").unwrap(), "usr_9988");
}

#[test]
fn test_oauth2_introspection_e2e_query_param_extraction() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec!["read".to_string()], "reject", false);
    let mut target = Arc::from("ws-upstream");

    let mut req = test_req(
        Method::GET,
        "/v1/ws?token=tok_valid_01&channel=notifications",
        &[],
    );
    let action = plugin.on_upstream_request(&mut req, &mut target);
    assert!(matches!(action, Action::Continue));
    assert_eq!(req.headers.get("x-user-id").unwrap(), "usr_9988");
}

#[test]
fn test_oauth2_introspection_e2e_missing_token_rejected() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec!["read".to_string()], "reject", true);
    let mut target = Arc::from("secure-upstream");

    let mut req = test_req(Method::GET, "/v1/secure", &[]);
    let action = plugin.on_upstream_request(&mut req, &mut target);
    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("missing_token"));
        }
        _ => panic!("Expected missing token to be rejected with 401"),
    }
}

#[test]
fn test_oauth2_introspection_e2e_revoked_token_negative_caching() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec!["read".to_string()], "reject", true);
    let mut target = Arc::from("orders-upstream");

    let mut req1 = test_req(
        Method::GET,
        "/v1/orders",
        &[("x-access-token", "tok_revoked_nonexistent")],
    );
    let action1 = plugin.on_upstream_request(&mut req1, &mut target);
    match action1 {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("invalid_token"));
        }
        _ => panic!("Expected revoked token to be rejected with 401"),
    }
    assert_eq!(idp.request_count.load(Ordering::Relaxed), 1);

    // 2nd Request: MUST HIT NEGATIVE CACHE
    let mut req2 = test_req(
        Method::GET,
        "/v1/orders",
        &[("x-access-token", "tok_revoked_nonexistent")],
    );
    let action2 = plugin.on_upstream_request(&mut req2, &mut target);
    match action2 {
        Action::Respond(resp) => assert_eq!(resp.status, StatusCode::UNAUTHORIZED),
        _ => panic!("Expected cached negative token to be rejected"),
    }
    assert_eq!(
        idp.request_count.load(Ordering::Relaxed),
        1,
        "Negative cache hit must NOT query IdP!"
    );
}

#[test]
fn test_oauth2_introspection_e2e_insufficient_scopes_forbidden() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(
        idp.addr,
        vec!["read".to_string(), "orders".to_string()],
        "reject",
        true,
    );
    let mut target = Arc::from("orders-upstream");

    let mut req = test_req(
        Method::POST,
        "/v1/orders",
        &[("x-access-token", "tok_read_only")],
    );
    let action = plugin.on_upstream_request(&mut req, &mut target);
    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::FORBIDDEN);
            assert!(extract_body_str(&resp).contains("insufficient_scope"));
        }
        _ => panic!("Expected insufficient scopes to be rejected with 403 Forbidden"),
    }
}

#[test]
fn test_oauth2_introspection_e2e_expired_token_rejected() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec![], "reject", true);
    let mut target = Arc::from("data-upstream");

    let mut req = test_req(
        Method::GET,
        "/v1/data",
        &[("x-access-token", "tok_expired_02")],
    );
    let action = plugin.on_upstream_request(&mut req, &mut target);
    match action {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::UNAUTHORIZED);
            assert!(extract_body_str(&resp).contains("token_expired"));
        }
        _ => panic!("Expected expired token to be rejected with 401"),
    }
}

#[test]
fn test_oauth2_introspection_e2e_idp_down_fallback_behavior() {
    let dummy_addr: SocketAddr = "127.0.0.1:49151".parse().unwrap();
    let mut target = Arc::from("upstream");

    // 1. Fail-Closed: fallback_behavior = "reject"
    let plugin_reject = build_test_plugin(dummy_addr, vec![], "reject", true);
    let mut req1 = test_req(
        Method::GET,
        "/v1/data",
        &[("x-access-token", "tok_some_token")],
    );
    let action1 = plugin_reject.on_upstream_request(&mut req1, &mut target);
    match action1 {
        Action::Respond(resp) => {
            assert_eq!(resp.status, StatusCode::BAD_GATEWAY);
            assert!(extract_body_str(&resp).contains("idp_unavailable"));
        }
        _ => panic!("Expected IdP failure to return 502 with fallback_behavior = reject"),
    }

    // 2. Fail-Open: fallback_behavior = "allow"
    let plugin_allow = build_test_plugin(dummy_addr, vec![], "allow", true);
    let mut req2 = test_req(
        Method::GET,
        "/v1/data",
        &[("x-access-token", "tok_some_token")],
    );
    let action2 = plugin_allow.on_upstream_request(&mut req2, &mut target);
    assert!(matches!(action2, Action::Continue));
}

#[test]
fn test_oauth2_introspection_e2e_anti_header_spoofing() {
    let idp = spawn_mock_idp_server();
    let plugin = build_test_plugin(idp.addr, vec!["read".to_string()], "reject", true);
    let mut target = Arc::from("secure-upstream");

    // Client maliciously supplies forged claims in headers
    let mut req = test_req(
        Method::GET,
        "/v1/data",
        &[
            ("x-access-token", "tok_valid_01"),
            ("x-user-id", "attacker_root"),
            ("x-user-role", "attacker_godmode"),
            ("x-client-id", "attacker_corp"),
        ],
    );

    let action = plugin.on_upstream_request(&mut req, &mut target);
    assert!(matches!(action, Action::Continue));

    // Verify forged headers were stripped and replaced with genuine IdP claims
    assert_eq!(req.headers.get("x-user-id").unwrap(), "usr_9988");
    assert_eq!(req.headers.get("x-user-role").unwrap(), "admin");
    assert_eq!(req.headers.get("x-client-id").unwrap(), "client_portal");
}
