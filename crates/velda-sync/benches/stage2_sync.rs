//! Micro-benchmarks for Stage 2: Sync Engine (Reconciliation Loop):
//! 1. Scaling input weights from Small (100 routes) to Extreme (10,000 routes).
//! 2. Measuring Hot-Path No-Op (Unchanged loop), Delta Update, and Full Update.
//! 3. Big-O Linearity Analysis across escalating system workloads.

#[path = "common/mod.rs"]
mod common;

use std::fs;
use std::time::{Duration, Instant};

use common::{
    CountingAllocator, calculate_big_o, format_duration, generate_listeners_workload,
    generate_routes_workload, generate_upstreams_workload,
};
use tempfile::TempDir;
use velda_sync::post_sync::plugin::PluginsFile;
use velda_sync::post_sync::tls::{TlsConfig, TlsFile};
use velda_sync::provider::{LocalFileProvider, Provider};
use velda_sync::{ManifestConfig, ManifestFileEntry, ManifestFiles, SyncComposition, SyncOutcome};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator::new();

struct ReconcileBenchFixture {
    _temp: TempDir,
    composition: SyncComposition,
    config_dir: std::path::PathBuf,
    revision: u64,
    num_routes: usize,
    num_upstreams: usize,
    num_listeners: usize,
}

impl ReconcileBenchFixture {
    fn setup(num_routes: usize, num_upstreams: usize, num_listeners: usize) -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path();
        let config_dir = root.join("config");
        let storage_dir = root.join("storage");
        let socket_path = root.join("edge.sock");

        fs::create_dir_all(&config_dir).unwrap();
        fs::create_dir_all(config_dir.join("domains")).unwrap();
        fs::create_dir_all(config_dir.join("certs")).unwrap();

        // 1. Routes
        let (_, routes_json, _) = generate_routes_workload(num_routes);
        fs::write(config_dir.join("domains/routes.json"), routes_json).unwrap();

        // 2. Upstreams
        let (_, upstreams_json, _) = generate_upstreams_workload(num_upstreams);
        fs::write(config_dir.join("domains/upstreams.json"), upstreams_json).unwrap();

        // 3. Listeners
        let (_, listeners_json, _) = generate_listeners_workload(num_listeners);
        fs::write(config_dir.join("domains/listeners.json"), listeners_json).unwrap();

        // 4. Real Plugins from example/plugins.json
        let example = common::example_dir();
        let plugins_bytes = fs::read(example.join("plugins.json")).unwrap_or_else(|_| {
            serde_json::to_vec(&PluginsFile {
                schema_version: 1,
                plugins: vec![],
            })
            .unwrap()
        });
        fs::write(config_dir.join("domains/plugins.json"), plugins_bytes).unwrap();

        // 5. Real TLS from example/tls.json
        let cert_path = config_dir.join("certs/server.crt");
        let key_path = config_dir.join("certs/server.key");
        fs::write(&cert_path, "MOCK_CERT").unwrap();
        fs::write(&key_path, "MOCK_KEY").unwrap();

        let key = rcgen::generate_simple_self_signed(vec!["api.example.com".into()]).unwrap();
        let cert_pem = key.cert.pem();
        let key_pem = key.signing_key.serialize_pem();
        let tls_file = TlsFile {
            schema_version: 1,
            tls: vec![TlsConfig {
                sni: vec![],
                cert_pem,
                key_pem,
                client_ca_pem: None,
                versions: vec!["tls1.3".into()],
                alpn: vec!["h2".into(), "http/1.1".into()],
            }],
        };
        let tls_bytes = serde_json::to_vec(&tls_file).unwrap();
        fs::write(config_dir.join("domains/tls.json"), tls_bytes).unwrap();

        // Initial Manifest (Revision 1)
        let manifest = ManifestConfig {
            schema_version: 1,
            revision: 1,
            configuration: ManifestFiles {
                files: vec![
                    ManifestFileEntry {
                        name: "routes".into(),
                        path: "domains/routes.json".into(),
                        required: true,
                        revision: Some(1),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "upstreams".into(),
                        path: "domains/upstreams.json".into(),
                        required: true,
                        revision: Some(1),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "listeners".into(),
                        path: "domains/listeners.json".into(),
                        required: true,
                        revision: Some(1),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "plugins".into(),
                        path: "domains/plugins.json".into(),
                        required: false,
                        revision: Some(1),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "tls".into(),
                        path: "domains/tls.json".into(),
                        required: false,
                        revision: Some(1),
                        hash: None,
                    },
                ],
            },
        };

        fs::write(
            config_dir.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();

        let provider = Provider::LocalFile(LocalFileProvider::new(&config_dir));
        let composition = SyncComposition::new(provider, &storage_dir, &socket_path);

        Self {
            _temp: temp,
            composition,
            config_dir,
            revision: 1,
            num_routes,
            num_upstreams,
            num_listeners,
        }
    }

    fn bump_manifest_revision(&mut self, change_domain: Option<&str>) {
        self.revision += 1;
        let rev = self.revision;

        let manifest = ManifestConfig {
            schema_version: 1,
            revision: rev,
            configuration: ManifestFiles {
                files: vec![
                    ManifestFileEntry {
                        name: "routes".into(),
                        path: "domains/routes.json".into(),
                        required: true,
                        revision: Some(
                            if change_domain == Some("routes") || change_domain.is_none() {
                                rev
                            } else {
                                1
                            },
                        ),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "upstreams".into(),
                        path: "domains/upstreams.json".into(),
                        required: true,
                        revision: Some(
                            if change_domain == Some("upstreams") || change_domain.is_none() {
                                rev
                            } else {
                                1
                            },
                        ),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "listeners".into(),
                        path: "domains/listeners.json".into(),
                        required: true,
                        revision: Some(
                            if change_domain == Some("listeners") || change_domain.is_none() {
                                rev
                            } else {
                                1
                            },
                        ),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "plugins".into(),
                        path: "domains/plugins.json".into(),
                        required: false,
                        revision: Some(
                            if change_domain == Some("plugins") || change_domain.is_none() {
                                rev
                            } else {
                                1
                            },
                        ),
                        hash: None,
                    },
                    ManifestFileEntry {
                        name: "tls".into(),
                        path: "domains/tls.json".into(),
                        required: false,
                        revision: Some(
                            if change_domain == Some("tls") || change_domain.is_none() {
                                rev
                            } else {
                                1
                            },
                        ),
                        hash: None,
                    },
                ],
            },
        };

        // Mutate actual file content so checksum changes along with revision
        if change_domain == Some("routes") || change_domain.is_none() {
            let (_, routes_json, _) =
                generate_routes_workload(self.num_routes + (rev as usize % 10));
            fs::write(self.config_dir.join("domains/routes.json"), routes_json).unwrap();
        }
        if change_domain == Some("upstreams") || change_domain.is_none() {
            let (_, upstreams_json, _) =
                generate_upstreams_workload(self.num_upstreams + (rev as usize % 5));
            fs::write(
                self.config_dir.join("domains/upstreams.json"),
                upstreams_json,
            )
            .unwrap();
        }
        if change_domain == Some("listeners") || change_domain.is_none() {
            let (_, listeners_json, _) =
                generate_listeners_workload(self.num_listeners + (rev as usize % 3));
            fs::write(
                self.config_dir.join("domains/listeners.json"),
                listeners_json,
            )
            .unwrap();
        }
        if change_domain == Some("plugins") || change_domain.is_none() {
            let plugins_file = PluginsFile {
                schema_version: 1,
                plugins: vec![],
            };
            let mut bytes = serde_json::to_vec(&plugins_file).unwrap();
            bytes.push(b'\n'); // Change checksum
            bytes.extend(std::iter::repeat_n(b' ', rev as usize));
            fs::write(self.config_dir.join("domains/plugins.json"), bytes).unwrap();
        }
        if change_domain == Some("tls") || change_domain.is_none() {
            let key =
                rcgen::generate_simple_self_signed(vec![format!("api_{rev}.example.com")]).unwrap();
            let cert_pem = key.cert.pem();
            let key_pem = key.signing_key.serialize_pem();
            let tls_file = TlsFile {
                schema_version: 1,
                tls: vec![TlsConfig {
                    sni: vec![],
                    cert_pem,
                    key_pem,
                    client_ca_pem: None,
                    versions: vec!["tls1.3".into()],
                    alpn: vec!["h2".into(), "http/1.1".into()],
                }],
            };
            fs::write(
                self.config_dir.join("domains/tls.json"),
                serde_json::to_vec(&tls_file).unwrap(),
            )
            .unwrap();
        }

        fs::write(
            self.config_dir.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ReconcileScaleResult {
    routes_n: usize,
    upstreams_n: usize,
    listeners_n: usize,

    noop_duration: Duration,
    noop_allocs: u64,
    noop_bytes: u64,

    delta_duration: Duration,
    delta_allocs: u64,
    delta_bytes: u64,

    full_duration: Duration,
    full_allocs: u64,
    full_bytes: u64,
}

fn benchmark_reconcile_at_scale(
    routes_n: usize,
    upstreams_n: usize,
    listeners_n: usize,
) -> ReconcileScaleResult {
    let rt = tokio::runtime::Runtime::new().unwrap();

    rt.block_on(async {
        let mut fixture = ReconcileBenchFixture::setup(routes_n, upstreams_n, listeners_n);

        // Cold Start
        let _ = fixture.composition.reconcile().await.unwrap();

        // 1. Hot-Path No-Op
        let noop_iterations = match routes_n {
            0..=500 => 300,
            501..=2000 => 100,
            _ => 30,
        };

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..noop_iterations {
            let outcome = fixture.composition.reconcile().await.unwrap();
            assert_eq!(outcome, SyncOutcome::Unchanged);
        }
        let noop_duration = start.elapsed() / noop_iterations as u32;
        let (noop_allocs, noop_bytes) = ALLOCATOR.snapshot();

        // 2. Delta Update (Routes)
        let delta_iterations = match routes_n {
            0..=500 => 30,
            501..=2000 => 10,
            _ => 5,
        };

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..delta_iterations {
            fixture.bump_manifest_revision(Some("routes"));
            let outcome = fixture.composition.reconcile().await.unwrap();
            assert!(matches!(outcome, SyncOutcome::Updated { .. }));
        }
        let delta_duration = start.elapsed() / delta_iterations as u32;
        let (delta_allocs, delta_bytes) = ALLOCATOR.snapshot();

        // 3. Full Update (All Domains)
        let full_iterations = match routes_n {
            0..=500 => 15,
            501..=2000 => 5,
            _ => 3,
        };

        ALLOCATOR.reset();
        let start = Instant::now();
        for _ in 0..full_iterations {
            fixture.bump_manifest_revision(None);
            let outcome = fixture.composition.reconcile().await.unwrap();
            assert!(matches!(outcome, SyncOutcome::Updated { .. }));
        }
        let full_duration = start.elapsed() / full_iterations as u32;
        let (full_allocs, full_bytes) = ALLOCATOR.snapshot();

        ReconcileScaleResult {
            routes_n,
            upstreams_n,
            listeners_n,
            noop_duration,
            noop_allocs: noop_allocs / noop_iterations,
            noop_bytes: noop_bytes / noop_iterations,
            delta_duration,
            delta_allocs: delta_allocs / delta_iterations,
            delta_bytes: delta_bytes / delta_iterations,
            full_duration,
            full_allocs: full_allocs / full_iterations,
            full_bytes: full_bytes / full_iterations,
        }
    })
}

fn main() {
    println!("================================================================================");
    println!(" VELDA-SYNC BENCHMARK: STAGE 2 (SYNC RECONCILIATION ENGINE)");
    println!(" Measuring Reconciler Scaling across Escalating Input Weights [100 .. 10,000]");
    println!("================================================================================\n");

    let workloads = [
        (100, 20, 5),       // Weight 1: Small
        (500, 50, 10),      // Weight 2: Medium
        (2000, 200, 25),    // Weight 3: Heavy
        (5000, 500, 50),    // Weight 4: Extreme
        (10000, 1000, 100), // Weight 5: Ultra
    ];

    let mut results = Vec::new();

    println!("### 1. Reconcile Loop Execution Times & Allocations across Workloads\n");
    println!(
        "| {:<24} | {:<20} | {:<22} | {:<22} |",
        "Workload Weight (R,U,L)",
        "Hot-Path No-Op",
        "Delta Update (Routes)",
        "Full Update (5 Domains)"
    );
    println!("|{:-<26}|{:-<22}|{:-<24}|{:-<24}|", "", "", "", "");

    for &(r, u, l) in &workloads {
        let res = benchmark_reconcile_at_scale(r, u, l);

        println!(
            "| {:<24} | {:<10} ({:<3} allocs) | {:<10} ({:<6} allocs) | {:<10} ({:<6} allocs) |",
            format!("{} R / {} U / {} L", r, u, l),
            format_duration(res.noop_duration),
            res.noop_allocs,
            format_duration(res.delta_duration),
            res.delta_allocs,
            format_duration(res.full_duration),
            res.full_allocs,
        );

        results.push(res);
    }

    println!("\n### 2. Big-O Growth Linearity Analysis across Scaling Workloads\n");
    println!(
        "| {:<20} | {:<11} | {:<16} | {:<18} | {:<18} | {:<16} |",
        "Scale Transition",
        "Scale Ratio",
        "No-Op Growth",
        "Delta Update Growth",
        "Full Update Growth",
        "Linearity Status"
    );
    println!(
        "|{:-<22}|{:-<13}|{:-<18}|{:-<20}|{:-<20}|{:-<18}|",
        "", "", "", "", "", ""
    );

    for i in 1..results.len() {
        let prev = &results[i - 1];
        let curr = &results[i];

        let (scale, noop_growth, _) = calculate_big_o(
            prev.noop_duration,
            curr.noop_duration,
            prev.routes_n,
            curr.routes_n,
        );
        let (_, delta_growth, delta_drift) = calculate_big_o(
            prev.delta_duration,
            curr.delta_duration,
            prev.routes_n,
            curr.routes_n,
        );
        let (_, full_growth, _) = calculate_big_o(
            prev.full_duration,
            curr.full_duration,
            prev.routes_n,
            curr.routes_n,
        );

        let status = if delta_drift.abs() <= 25.0 {
            "Strict O(N) [OK]"
        } else {
            "O(N) Drift"
        };

        println!(
            "| {:<5} -> {:<6} routes | {:<11.1}x | {:<16.2}x | {:<11.2}x ({:>+4.1}%) | {:<18.2}x | {:<16} |",
            prev.routes_n,
            curr.routes_n,
            scale,
            noop_growth,
            delta_growth,
            delta_drift,
            full_growth,
            status,
        );
    }

    println!("\n================================================================================");
    println!(" STAGE 2 BENCHMARK COMPLETE");
    println!("================================================================================\n");
}
