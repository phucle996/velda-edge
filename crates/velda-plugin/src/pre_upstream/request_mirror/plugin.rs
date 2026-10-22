//! Request Mirroring (Traffic Shadowing) Pre-Upstream Plugin (`request_mirror`).
//!
//! Duplicates incoming client requests asynchronously to a designated shadow upstream
//! (e.g., staging, load testing, or experimental backend clusters) without affecting
//! the latency or outcome of the primary request.
//!
//! # Strict Architectural Invariants
//! - **Zero Client Latency Impact**: Mirror requests are enqueued via a non-blocking
//!   queue (`try_send`). Primary request forwarding never blocks or awaits mirror responses.
//! - **Failure Isolation**: Errors, timeouts, or 5xx responses from the mirror cluster are
//!   completely discarded and never propagated downstream.
//! - **Zero-Copy Payload Sharing**: Request bodies are shared using reference-counted `bytes::Bytes`,
//!   incurring 0 heap memory copying.
//! - **System Invariant Header**: Always injects `X-Velda-Mirror: true` into the shadow request,
//!   ensuring backend services recognize shadow traffic for dry-run/sandbox isolation.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, Uri};
use tokio::sync::mpsc;
use velda_core::{Action, Body, L7Request};
use velda_sync::post_sync::plugin::PluginConfig;

use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::pre_upstream::PreUpstreamPlugin;

use std::sync::RwLock;

static MIRROR_COUNTER: AtomicU64 = AtomicU64::new(0);
static MIRROR_SENDER: RwLock<Option<mpsc::Sender<MirroredRequest>>> = RwLock::new(None);

/// Initializes the global non-blocking dispatch queue for request mirroring and returns the receiver.
pub fn init_mirror_queue(capacity: usize) -> mpsc::Receiver<MirroredRequest> {
    let (tx, rx) = mpsc::channel(capacity);
    if let Ok(mut lock) = MIRROR_SENDER.write() {
        *lock = Some(tx);
    }
    rx
}

/// Returns a clone of the active global non-blocking queue sender.
pub fn get_mirror_sender() -> Option<mpsc::Sender<MirroredRequest>> {
    MIRROR_SENDER.read().ok().and_then(|lock| lock.clone())
}

const MIRROR_HEADER_NAME: HeaderName = HeaderName::from_static("x-velda-mirror");
const MIRROR_HEADER_VALUE: HeaderValue = HeaderValue::from_static("true");

/// Structured representation of a cloned request dispatched to the mirror upstream.
#[derive(Debug, Clone)]
pub struct MirroredRequest {
    /// Logical name of the target mirror upstream.
    pub target_upstream: String,
    /// HTTP method of the request.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// Cloned headers including the mandatory `x-velda-mirror: true`.
    pub headers: HeaderMap,
    /// Zero-copy shared body payload bytes.
    pub body: Bytes,
}

/// Configuration settings for the request mirror plugin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestMirrorConfig {
    /// Name of the target mirror upstream pool (e.g. `"orders-staging"`).
    pub mirror_upstream: String,
    /// Percentage of matching traffic to mirror (0 to 100).
    pub sample_percent: u32,
    /// Optional Host header replacement for the mirror request.
    pub replace_host: Option<String>,
    /// List of HTTP methods to ignore (e.g. `["DELETE"]`).
    pub ignore_methods: Vec<String>,
    /// Maximum capacity of the non-blocking mirror queue.
    pub queue_capacity: usize,
}

impl Default for RequestMirrorConfig {
    fn default() -> Self {
        Self {
            mirror_upstream: String::new(),
            sample_percent: 100,
            replace_host: None,
            ignore_methods: Vec::new(),
            queue_capacity: 65536,
        }
    }
}

/// Request Mirroring Pre-Upstream Plugin.
#[derive(Debug, Clone)]
pub struct RequestMirrorPlugin {
    pub id: String,
    pub config: RequestMirrorConfig,
    replace_host_val: Option<HeaderValue>,
    sender: Option<mpsc::Sender<MirroredRequest>>,
    mirrored_count: Arc<AtomicU64>,
    dropped_count: Arc<AtomicU64>,
}

impl RequestMirrorPlugin {
    /// Constructs a new request mirror plugin without an active queue sender (for testing/dry-run).
    pub fn new(id: impl Into<String>, config: RequestMirrorConfig) -> Result<Self, PluginError> {
        let replace_host_val = if let Some(ref host) = config.replace_host {
            Some(HeaderValue::from_str(host).map_err(|e| {
                PluginError::Config(format!("Invalid replace_host header value '{host}': {e}"))
            })?)
        } else {
            None
        };

        Ok(Self {
            id: id.into(),
            config,
            replace_host_val,
            sender: None,
            mirrored_count: Arc::new(AtomicU64::new(0)),
            dropped_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Constructs a new request mirror plugin with an active non-blocking dispatch sender.
    pub fn with_sender(
        id: impl Into<String>,
        config: RequestMirrorConfig,
        sender: mpsc::Sender<MirroredRequest>,
    ) -> Result<Self, PluginError> {
        let mut plugin = Self::new(id, config)?;
        plugin.sender = Some(sender);
        Ok(plugin)
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let cfg = velda_sync::validate_request_mirror_config(&config.id, &config.config).map_err(
            |e| {
                PluginError::Config(format!(
                    "Invalid request_mirror configuration for '{}': {e}",
                    config.id
                ))
            },
        )?;

        let sample_percent = (cfg.sample_percent.round() as u32).min(100);

        let mut plugin = Self::new(
            &config.id,
            RequestMirrorConfig {
                mirror_upstream: cfg.mirror_upstream,
                sample_percent,
                replace_host: cfg.replace_host,
                ignore_methods: cfg.ignore_methods,
                queue_capacity: cfg.queue_capacity,
            },
        )?;

        if let Some(sender) = get_mirror_sender() {
            plugin.sender = Some(sender);
        }

        Ok(plugin)
    }

    /// Total number of requests successfully enqueued for mirroring.
    #[inline]
    pub fn mirrored_count(&self) -> u64 {
        self.mirrored_count.load(Ordering::Relaxed)
    }

    /// Total number of mirror requests dropped due to queue saturation.
    #[inline]
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// Evaluates whether the request should be sampled for mirroring.
    #[inline]
    pub fn should_mirror(&self, req: &L7Request) -> bool {
        // 1. Check ignored HTTP methods
        let method_str = req.method.as_str();
        for ignored in &self.config.ignore_methods {
            if ignored.eq_ignore_ascii_case(method_str) {
                return false;
            }
        }

        // 2. Check sample percent boundaries
        if self.config.sample_percent == 0 {
            return false;
        }
        if self.config.sample_percent >= 100 {
            return true;
        }

        // 3. Sampling evaluation
        let count = MIRROR_COUNTER.fetch_add(1, Ordering::Relaxed);
        (count % 100) < (self.config.sample_percent as u64)
    }
}

impl PreUpstreamPlugin for RequestMirrorPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_upstream_request(&self, req: &mut L7Request, _target_upstream: &mut Arc<str>) -> Action {
        if self.should_mirror(req) {
            // 1. Prepare shadow headers with hardcoded mandatory X-Velda-Mirror: true
            let mut shadow_headers = req.headers.clone();
            shadow_headers.insert(MIRROR_HEADER_NAME, MIRROR_HEADER_VALUE);

            if let Some(ref host_val) = self.replace_host_val {
                shadow_headers.insert(http::header::HOST, host_val.clone());
            }

            // 2. Zero-copy body sharing via reference-counted Bytes
            let body_bytes = match &req.body {
                Body::Bytes(b) => b.clone(),
                Body::Empty => Bytes::new(),
            };

            let shadow = MirroredRequest {
                target_upstream: self.config.mirror_upstream.clone(),
                method: req.method.clone(),
                uri: req.uri.clone(),
                headers: shadow_headers,
                body: body_bytes,
            };

            // 3. Non-blocking queue dispatch (zero client latency impact)
            let sender = self.sender.as_ref().cloned().or_else(get_mirror_sender);
            if let Some(ref sender) = sender {
                match sender.try_send(shadow) {
                    Ok(_) => {
                        self.mirrored_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.dropped_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            } else {
                let target = shadow.target_upstream.clone();
                let task = Box::pin(async move {
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_millis(1000),
                        dispatch_mirror_request(target, shadow),
                    )
                    .await;
                });
                match crate::hook::try_dispatch_shadow(task) {
                    Ok(_) => {
                        self.mirrored_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.dropped_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        Action::Continue
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Read)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_L7
    }
}

/// Dispatches a replicated HTTP/1.1 request directly to the target mirror endpoint.
///
/// Discards responses and swallows errors to ensure 100% failure isolation from primary traffic.
async fn dispatch_mirror_request(target: String, req: MirroredRequest) {
    let connect_addr = if target.contains(':') {
        target.clone()
    } else {
        format!("{target}:80")
    };

    if let Ok(mut stream) = tokio::net::TcpStream::connect(&connect_addr).await {
        let mut buf = bytes::BytesMut::with_capacity(512 + req.body.len());
        use std::fmt::Write as _;
        let path = req
            .uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let _ = write!(buf, "{} {} HTTP/1.1\r\n", req.method, path);
        for (k, v) in &req.headers {
            if k == http::header::CONNECTION {
                continue;
            }
            buf.extend_from_slice(k.as_str().as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(v.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        if !req.headers.contains_key(http::header::CONTENT_LENGTH) && !req.body.is_empty() {
            let _ = write!(buf, "content-length: {}\r\n", req.body.len());
        }
        buf.extend_from_slice(b"connection: close\r\n\r\n");
        if !req.body.is_empty() {
            buf.extend_from_slice(&req.body);
        }
        use tokio::io::AsyncWriteExt;
        if stream.write_all(&buf).await.is_ok() {
            let _ = stream.flush().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Version};

    fn build_test_request(method: Method, headers: HeaderMap, body: Bytes) -> L7Request {
        L7Request::new(
            method,
            Uri::from_static("/api/v1/orders"),
            Version::HTTP_11,
            headers,
            Body::Bytes(body),
        )
    }

    #[test]
    fn test_request_mirror_always_injects_mandatory_header() {
        let (tx, mut rx) = mpsc::channel(16);
        let config = RequestMirrorConfig {
            mirror_upstream: "staging-cluster".into(),
            sample_percent: 100,
            replace_host: Some("staging.internal.net".into()),
            ignore_methods: Vec::new(),
            queue_capacity: 16,
        };

        let plugin = RequestMirrorPlugin::with_sender("mirror-1", config, tx).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-user-id", HeaderValue::from_static("usr-42"));
        let mut req = build_test_request(
            Method::POST,
            headers,
            Bytes::from_static(b"{\"item\":\"book\"}"),
        );

        let mut target = Arc::from("primary-backend");
        let action = plugin.on_upstream_request(&mut req, &mut target);
        assert!(matches!(action, Action::Continue));

        let mirrored = rx.try_recv().expect("must receive mirrored request");
        assert_eq!(mirrored.target_upstream, "staging-cluster");
        assert_eq!(mirrored.method, Method::POST);
        assert_eq!(mirrored.body, Bytes::from_static(b"{\"item\":\"book\"}"));
        assert_eq!(mirrored.headers.get("x-velda-mirror").unwrap(), "true");
        assert_eq!(
            mirrored.headers.get("host").unwrap(),
            "staging.internal.net"
        );
        assert_eq!(mirrored.headers.get("x-user-id").unwrap(), "usr-42");

        // Verify primary request was untouched regarding mirror header
        assert!(!req.headers.contains_key("x-velda-mirror"));
    }

    #[test]
    fn test_request_mirror_ignores_specified_methods() {
        let (tx, mut rx) = mpsc::channel(16);
        let config = RequestMirrorConfig {
            mirror_upstream: "staging-cluster".into(),
            sample_percent: 100,
            replace_host: None,
            ignore_methods: vec!["DELETE".into(), "PATCH".into()],
            queue_capacity: 16,
        };

        let plugin = RequestMirrorPlugin::with_sender("mirror-ignore", config, tx).unwrap();

        let mut req_del = build_test_request(Method::DELETE, HeaderMap::new(), Bytes::new());
        let mut target_del = Arc::from("primary-backend");
        plugin.on_upstream_request(&mut req_del, &mut target_del);

        assert!(rx.try_recv().is_err(), "DELETE must be ignored");

        let mut req_post = build_test_request(Method::POST, HeaderMap::new(), Bytes::new());
        let mut target_post = Arc::from("primary-backend");
        plugin.on_upstream_request(&mut req_post, &mut target_post);

        assert!(rx.try_recv().is_ok(), "POST must be mirrored");
    }

    #[test]
    fn test_request_mirror_queue_full_drops_silently() {
        // Channel with capacity 1
        let (tx, _rx) = mpsc::channel(1);
        let config = RequestMirrorConfig {
            mirror_upstream: "staging".into(),
            sample_percent: 100,
            replace_host: None,
            ignore_methods: Vec::new(),
            queue_capacity: 1,
        };

        let plugin = RequestMirrorPlugin::with_sender("mirror-drop", config, tx).unwrap();

        let mut req1 = build_test_request(Method::GET, HeaderMap::new(), Bytes::new());
        let mut target1 = Arc::from("primary-backend");
        plugin.on_upstream_request(&mut req1, &mut target1); // fills buffer (1/1)

        let mut req2 = build_test_request(Method::GET, HeaderMap::new(), Bytes::new());
        let mut target2 = Arc::from("primary-backend");
        let action = plugin.on_upstream_request(&mut req2, &mut target2); // buffer full -> dropped!

        // Primary request is unaffected (Continue)
        assert!(matches!(action, Action::Continue));
        assert_eq!(plugin.mirrored_count(), 1);
        assert_eq!(plugin.dropped_count(), 1);
    }

    #[test]
    fn test_request_mirror_from_config() {
        let json_cfg = serde_json::json!({
            "mirror_upstream": "mock-backend",
            "sample_percent": 50.0,
            "replace_host": "mock.local",
            "ignore_methods": ["DELETE"],
            "queue_capacity": 65536
        });

        let config = PluginConfig {
            id: "request_mirror".into(),
            phase: String::new(),
            config: json_cfg,
        };

        let plugin = RequestMirrorPlugin::from_config(&config).expect("valid config");
        assert_eq!(plugin.id, "request_mirror");
        assert_eq!(plugin.config.mirror_upstream, "mock-backend");
        assert_eq!(plugin.config.sample_percent, 50);
        assert_eq!(plugin.config.replace_host, Some("mock.local".into()));
        assert_eq!(plugin.config.ignore_methods, vec!["DELETE"]);

        // Empty config must fail validation
        let bad_config = PluginConfig {
            id: "request_mirror".into(),
            phase: String::new(),
            config: serde_json::json!({}),
        };
        assert!(RequestMirrorPlugin::from_config(&bad_config).is_err());
    }

    #[test]
    fn test_request_mirror_dynamic_global_sender() {
        let config = RequestMirrorConfig {
            mirror_upstream: "staging".into(),
            sample_percent: 100,
            replace_host: None,
            ignore_methods: Vec::new(),
            queue_capacity: 10,
        };

        // Plugin instantiated without sender (sender == None)
        let plugin = RequestMirrorPlugin::new("mirror-dynamic", config).unwrap();
        assert!(plugin.sender.is_none());

        // Later in lifecycle, global mirror queue is initialized
        let mut rx = init_mirror_queue(10);

        let mut req = build_test_request(Method::GET, HeaderMap::new(), Bytes::new());
        let mut target = Arc::from("prod-backend");
        let action = plugin.on_upstream_request(&mut req, &mut target);

        assert!(matches!(action, Action::Continue));
        let shadow = rx
            .try_recv()
            .expect("Should receive dynamically dispatched mirror request");
        assert_eq!(shadow.target_upstream, "staging");
    }
}
