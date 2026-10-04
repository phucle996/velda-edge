//! Shared utilities, memory tracking allocators, and test fixtures for velda-tls benchmarks.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rcgen::generate_simple_self_signed;
use rustls::ClientConfig;
use rustls::client::ClientSessionMemoryCache;
use tokio_rustls::TlsConnector;
use velda_core::hardware::{CpuTier, MemoryTier};
use velda_tls::pem::parse_ca_bundle_pem;
use velda_tls::{ServerTlsConfig, SniResolver, TlsServerEngine, TlsServerParams};

// ============================================================================
// Memory Tracking Allocator
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocSnapshot {
    pub alloc_count: u64,
    pub dealloc_count: u64,
    pub bytes_allocated: u64,
    pub bytes_deallocated: u64,
}

impl AllocSnapshot {
    pub fn net_bytes(&self) -> i64 {
        self.bytes_allocated as i64 - self.bytes_deallocated as i64
    }

    pub fn net_allocs(&self) -> i64 {
        self.alloc_count as i64 - self.dealloc_count as i64
    }
}

pub struct CountingAllocator {
    alloc_count: AtomicU64,
    dealloc_count: AtomicU64,
    bytes_allocated: AtomicU64,
    bytes_deallocated: AtomicU64,
}

impl CountingAllocator {
    pub const fn new() -> Self {
        Self {
            alloc_count: AtomicU64::new(0),
            dealloc_count: AtomicU64::new(0),
            bytes_allocated: AtomicU64::new(0),
            bytes_deallocated: AtomicU64::new(0),
        }
    }

    pub fn reset(&self) {
        self.alloc_count.store(0, Ordering::SeqCst);
        self.dealloc_count.store(0, Ordering::SeqCst);
        self.bytes_allocated.store(0, Ordering::SeqCst);
        self.bytes_deallocated.store(0, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.alloc_count.load(Ordering::SeqCst),
            self.bytes_allocated.load(Ordering::SeqCst),
        )
    }

    pub fn full_snapshot(&self) -> AllocSnapshot {
        AllocSnapshot {
            alloc_count: self.alloc_count.load(Ordering::SeqCst),
            dealloc_count: self.dealloc_count.load(Ordering::SeqCst),
            bytes_allocated: self.bytes_allocated.load(Ordering::SeqCst),
            bytes_deallocated: self.bytes_deallocated.load(Ordering::SeqCst),
        }
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size() as u64;
        self.alloc_count.fetch_add(1, Ordering::SeqCst);
        self.bytes_allocated.fetch_add(size, Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let size = layout.size() as u64;
        self.dealloc_count.fetch_add(1, Ordering::SeqCst);
        self.bytes_deallocated.fetch_add(size, Ordering::SeqCst);
        unsafe { System.dealloc(ptr, layout) }
    }
}

// ============================================================================
// Fixture Helpers
// ============================================================================

/// Generates a test self-signed certificate and private key for given Subject Alternative Names (SANs).
pub fn make_test_cert(sans: Vec<String>) -> (String, String) {
    let certified_key = generate_simple_self_signed(sans).unwrap();
    let cert_pem = certified_key.cert.pem();
    let key_pem = certified_key.signing_key.serialize_pem();
    (cert_pem, key_pem)
}

/// Builds a test `TlsServerEngine` configured for "api.example.com" and "*.example.com".
pub fn make_test_server(cpu: CpuTier, mem: MemoryTier) -> (TlsServerEngine, String) {
    let (cert_pem, key_pem) = make_test_cert(vec![
        "api.example.com".into(),
        "*.example.com".into(),
        "grpc.service.local".into(),
    ]);

    let config = ServerTlsConfig {
        sni: vec![
            "api.example.com".into(),
            "*.example.com".into(),
            "grpc.service.local".into(),
        ],
        versions: vec!["tls1.3".into()],
        alpn: vec!["h2".into(), "http/1.1".into()],
        cert_pem: cert_pem.clone(),
        key_pem,
        client_ca_pem: None,
    };

    let params = TlsServerParams::for_tiers(cpu, mem);
    let engine = TlsServerEngine::new_with_params(&[config], &params).unwrap();
    (engine, cert_pem)
}

/// Builds a client `TlsConnector` trusting the provided CA certificate and optionally enabling session cache.
pub fn make_test_client(cert_pem: &str, with_session_cache: bool) -> TlsConnector {
    let root_store = parse_ca_bundle_pem(cert_pem).unwrap();
    let mut client_config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_no_client_auth();

    if with_session_cache {
        let cache = Arc::new(ClientSessionMemoryCache::new(1024));
        client_config.resumption = rustls::client::Resumption::store(cache);
    }

    client_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    TlsConnector::from(Arc::new(client_config))
}

/// Builds a client `TlsConnector` that trusts the provided CA PEM and configures ALPN.
pub fn make_client_connector(ca_pem: &str, alpn: Option<Vec<&str>>) -> TlsConnector {
    let root_store = parse_ca_bundle_pem(ca_pem).expect("Failed to parse CA bundle");
    let mut client_config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("Valid protocol versions")
    .with_root_certificates(root_store)
    .with_no_client_auth();

    if let Some(alpns) = alpn {
        client_config.alpn_protocols = alpns.into_iter().map(|s| s.as_bytes().to_vec()).collect();
    }

    TlsConnector::from(Arc::new(client_config))
}

/// Builds an mTLS client `TlsConnector` presenting a client certificate to the server.
pub fn make_mtls_client_connector(
    ca_pem: &str,
    client_cert_pem: &str,
    client_key_pem: &str,
    alpn: Option<Vec<&str>>,
) -> TlsConnector {
    let root_store = parse_ca_bundle_pem(ca_pem).expect("Failed to parse CA bundle");
    let client_certs =
        velda_tls::pem::parse_certs_pem(client_cert_pem).expect("Failed to parse client certs");
    let client_key =
        velda_tls::pem::parse_private_key_pem(client_key_pem).expect("Failed to parse client key");

    let mut client_config = ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("Valid protocol versions")
    .with_root_certificates(root_store)
    .with_client_auth_cert(client_certs, client_key)
    .expect("Failed to set client auth cert");

    if let Some(alpns) = alpn {
        client_config.alpn_protocols = alpns.into_iter().map(|s| s.as_bytes().to_vec()).collect();
    }

    TlsConnector::from(Arc::new(client_config))
}

/// Builds an in-memory `SniResolver` populated with `count` distinct certificates.
pub fn build_test_sni_resolver(count: usize) -> SniResolver {
    let mut resolver = SniResolver::new();
    let (cert_pem, key_pem) = make_test_cert(vec!["base.test".into()]);
    let certs = velda_tls::pem::parse_certs_pem(&cert_pem).unwrap();
    let key = velda_tls::pem::parse_private_key_pem(&key_pem).unwrap();

    for i in 0..count {
        let exact_sni = format!("service_{:04}.domain.com", i);
        let wildcard_sni = format!("*.cluster_{:04}.internal", i);
        resolver
            .add_certificate(&[exact_sni, wildcard_sni], certs.clone(), key.clone_key())
            .unwrap();
    }
    resolver
}
