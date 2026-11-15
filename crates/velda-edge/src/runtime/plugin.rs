//! In-memory Route Plugin Plan table and compilation dispatcher.
//!
//! Pre-compiles declarative [`PluginConfig`] definitions into typed [`LoadedPlugin`] instances,
//! and compiles route-specific [`RoutePluginPlan`] plans in RAM ahead of time.
//! Provides O(1) lock-free plan lookup on the request hot path with total bypass when empty.

use std::sync::Arc;

use rustc_hash::FxHashMap;
use velda_core::{ProtocolFamily, RouteId, StreamingMode};
use velda_plugin::{
    LoadedPlugin, RoutePluginPlan, build_route_plugin_plan, instantiate_plugin,
    wire_plugin_dependencies,
};
use velda_sync::post_sync::listener::ListenerConfig;
use velda_sync::post_sync::plugin::PluginConfig;
use velda_sync::post_sync::route::RouteConfig;

use crate::error::EdgeError;

/// Active in-memory plugin table holding pre-compiled [`RoutePluginPlan`] execution plans.
#[derive(Clone, Default)]
pub struct PluginTable {
    /// O(1) integer lookup table: RouteId -> RoutePluginPlan.
    pub routes: FxHashMap<RouteId, Arc<RoutePluginPlan>>,
    /// Active background plugins running on Shadow Worker.
    pub background_plugins: Vec<Arc<dyn velda_plugin::BackgroundPlugin>>,
    /// Pre-computed background plugins partitioned across shadow workers:
    /// Index = worker_id (0..N-1) -> List of plugins running on that shadow worker.
    pub shadow_shards: Vec<Vec<Arc<dyn velda_plugin::BackgroundPlugin>>>,
    /// Canonical empty plan singleton used for routes without any declared plugins.
    pub empty_plan: Arc<RoutePluginPlan>,
}

impl std::fmt::Debug for PluginTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginTable")
            .field("routes_count", &self.routes.len())
            .field("background_plugins_count", &self.background_plugins.len())
            .field("shadow_shards_count", &self.shadow_shards.len())
            .finish()
    }
}

impl PluginTable {
    /// Creates a new empty plugin table.
    pub fn empty() -> Self {
        Self {
            routes: FxHashMap::default(),
            background_plugins: Vec::new(),
            shadow_shards: Vec::new(),
            empty_plan: Arc::new(RoutePluginPlan::default()),
        }
    }

    /// Returns the pre-compiled plugin execution plan for the given route identifier.
    ///
    /// If the route has no plugins or route_id is unknown, returns the canonical empty plan.
    #[inline(always)]
    pub fn get_plan(&self, route_id: RouteId) -> &Arc<RoutePluginPlan> {
        self.routes.get(&route_id).unwrap_or(&self.empty_plan)
    }

    /// Returns the assigned background plugins for the specified shadow worker lane.
    ///
    /// If the worker lane index exceeds the partitioned shards, returns an empty slice.
    #[inline(always)]
    pub fn get_shadow_plugins(
        &self,
        worker_id: usize,
    ) -> &[Arc<dyn velda_plugin::BackgroundPlugin>] {
        self.shadow_shards
            .get(worker_id)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Returns `true` if the route has at least one active plugin hook.
    #[inline(always)]
    pub fn has_plugins(&self, route_id: RouteId) -> bool {
        self.routes.get(&route_id).is_some_and(|p| !p.is_empty())
    }
}

/// Pre-compiles all plugin instances and constructs the static [`PluginTable`].
pub fn build_plugin_table(
    plugins: &[PluginConfig],
    routes: &[RouteConfig],
    listeners: &[ListenerConfig],
) -> Result<PluginTable, EdgeError> {
    // 1. Instantiate all available plugins into typed LoadedPlugin instances
    let mut available_plugins = FxHashMap::default();
    let mut background_plugins = Vec::new();
    for plugin_cfg in plugins {
        let loaded = instantiate_plugin(plugin_cfg).map_err(|e| EdgeError::InvalidConfig {
            detail: format!("Failed to instantiate plugin '{}': {e}", plugin_cfg.id),
        })?;
        if let LoadedPlugin::Background(ref bg) = loaded {
            background_plugins.push(Arc::clone(bg));
        }
        available_plugins.insert(plugin_cfg.id.clone(), loaded);
    }

    // 1b. Wire inter-plugin dependencies (e.g. jwks_rotator -> jwt_auth SharedJwksStore)
    wire_plugin_dependencies(&available_plugins).map_err(|e| EdgeError::InvalidConfig {
        detail: format!("Failed to wire plugin dependencies: {e}"),
    })?;

    // 2. Host Ingress Port Authority: register Traffic Ingress listeners with SocketManager
    let socket_manager = velda_core::socket::global_socket_manager();
    for l in listeners {
        if let Ok(addr) = l.address.parse::<std::net::SocketAddr>() {
            let proto = match l.transport.protocol.to_ascii_lowercase().as_str() {
                "udp" => velda_core::socket::SocketProtocol::Udp,
                _ => velda_core::socket::SocketProtocol::Tcp,
            };
            socket_manager
                .register_traffic_socket(addr, proto, &l.id)
                .map_err(|e| EdgeError::InvalidConfig {
                    detail: format!("Ingress listener port conflict: {e}"),
                })?;
        }
    }

    // 3. Pre-index listeners by listener_id for O(1) protocol & streaming lookup
    let listener_map: FxHashMap<&str, &ListenerConfig> =
        listeners.iter().map(|l| (l.id.as_str(), l)).collect();

    // 4. Pre-compile RoutePluginPlan for each route that specifies plugins
    let mut route_plans = FxHashMap::default();
    let empty_plan = Arc::new(RoutePluginPlan::default());

    for route in routes {
        if route.plugins.is_empty() {
            continue; // Zero overhead: do not populate routes map for empty routes!
        }

        let listener = listener_map.get(route.listener.as_str()).copied();
        let (protocol_family, streaming_mode) = if let Some(l) = listener {
            let family =
                ProtocolFamily::parse(&l.application.protocol).unwrap_or(ProtocolFamily::Http);
            let streaming = l.application.streaming;
            (family, streaming)
        } else {
            (ProtocolFamily::Http, StreamingMode::DISABLED)
        };

        let plan = build_route_plugin_plan(
            &route.id,
            &route.plugins,
            &available_plugins,
            protocol_family,
            streaming_mode,
        )
        .map_err(|e| EdgeError::InvalidConfig {
            detail: format!("Failed to build plugin plan for route '{}': {e}", route.id),
        })?;

        if !plan.is_empty() {
            let route_id = route.route_id();
            route_plans.insert(route_id, Arc::new(plan));
        }
    }

    let worker_count = velda_core::hardware::global_hardware_topology()
        .worker_threads
        .max(1);
    let shadow_shards = partition_background_plugins(&background_plugins, worker_count);

    Ok(PluginTable {
        routes: route_plans,
        background_plugins,
        shadow_shards,
        empty_plan,
    })
}

/// Partitions background plugins across `worker_count` shadow worker threads using
/// a deterministic Greedy LPT (Longest Processing Time First) bin-packing algorithm
/// with Core 0 protection invariants.
pub fn partition_background_plugins(
    plugins: &[Arc<dyn velda_plugin::BackgroundPlugin>],
    worker_count: usize,
) -> Vec<Vec<Arc<dyn velda_plugin::BackgroundPlugin>>> {
    let worker_count = worker_count.max(1);
    let mut shards: Vec<Vec<Arc<dyn velda_plugin::BackgroundPlugin>>> =
        (0..worker_count).map(|_| Vec::new()).collect();

    if plugins.is_empty() {
        return shards;
    }

    if worker_count == 1 {
        shards[0] = plugins.to_vec();
        return shards;
    }

    // Multi-core partitioning (worker_count >= 2):
    // 1. Separate plugins with strict Core 0 affinity or sleep-bound characteristics:
    //    - "health_probe": Hard affinity to Core 0 (Kubernetes liveness/readiness SLA).
    //    - Sleep-bound key/cert rotators ("jwks_rotator", "acme_cert_rotator"):
    //      placed on Core 0 because they are dormant 99.9% of the time (weight <= 10).
    // 2. Heavy exporters and log shippers are sorted descending by weight and assigned
    //    greedily to the non-0 core with the minimum current accumulated load.
    let mut core_loads = vec![0u32; worker_count];
    let mut regular_plugins = Vec::new();

    for plugin in plugins {
        let id = plugin.id();
        let w = plugin.weight();
        let is_core0_affinity = id.starts_with("health_probe")
            || ((id.starts_with("jwks_rotator") || id.starts_with("acme_cert_rotator")) && w <= 10);

        if is_core0_affinity {
            shards[0].push(Arc::clone(plugin));
            core_loads[0] += w;
        } else {
            regular_plugins.push((w, Arc::clone(plugin)));
        }
    }

    // Sort regular plugins descending by weight (LPT: heaviest first)
    regular_plugins.sort_by_key(|a| std::cmp::Reverse(a.0));

    // Greedily place each regular plugin into the non-0 core with the smallest load.
    for (weight, plugin) in regular_plugins {
        let min_core = (1..worker_count)
            .min_by_key(|&c| core_loads[c])
            .unwrap_or(0);

        shards[min_core].push(plugin);
        core_loads[min_core] += weight;
    }

    shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_plugin_table() {
        let table = PluginTable::empty();
        let non_existent = RouteId::new(999);
        assert!(!table.has_plugins(non_existent));
        assert!(table.get_plan(non_existent).is_empty());
        assert!(table.background_plugins.is_empty());
        assert!(table.get_shadow_plugins(0).is_empty());
    }

    struct MockBackgroundPlugin {
        id: String,
        weight: u32,
    }

    impl velda_plugin::BackgroundPlugin for MockBackgroundPlugin {
        fn id(&self) -> &str {
            &self.id
        }

        fn weight(&self) -> u32 {
            self.weight
        }

        fn run(
            self: Arc<Self>,
            _shutdown: tokio::sync::watch::Receiver<bool>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), velda_plugin::PluginError>> + Send>,
        > {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn test_partition_background_plugins_single_core() {
        let plugins: Vec<Arc<dyn velda_plugin::BackgroundPlugin>> = vec![
            Arc::new(MockBackgroundPlugin {
                id: "health_probe".into(),
                weight: 1,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "loki_log_shipper".into(),
                weight: 80,
            }),
        ];
        let shards = partition_background_plugins(&plugins, 1);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].len(), 2);
    }

    #[test]
    fn test_partition_background_plugins_multi_core_protection() {
        let plugins: Vec<Arc<dyn velda_plugin::BackgroundPlugin>> = vec![
            Arc::new(MockBackgroundPlugin {
                id: "health_probe".into(),
                weight: 1,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "jwks_rotator".into(),
                weight: 5,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "acme_cert_rotator".into(),
                weight: 10,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "std_log_shipper".into(),
                weight: 15,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "statsd_exporter".into(),
                weight: 20,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "prometheus_exporter".into(),
                weight: 30,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "otlp_metrics_exporter".into(),
                weight: 40,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "otlp_log_shipper".into(),
                weight: 70,
            }),
            Arc::new(MockBackgroundPlugin {
                id: "loki_log_shipper".into(),
                weight: 80,
            }),
        ];

        // 4 Cores: Core 0 must only host health_probe + rotators; heavy shippers on cores 1..3
        let shards = partition_background_plugins(&plugins, 4);
        assert_eq!(shards.len(), 4);

        // Core 0 must have health_probe, jwks_rotator, acme_cert_rotator
        let core0_ids: Vec<&str> = shards[0].iter().map(|p| p.id()).collect();
        assert!(core0_ids.contains(&"health_probe"));
        assert!(core0_ids.contains(&"jwks_rotator"));
        assert!(core0_ids.contains(&"acme_cert_rotator"));
        assert!(!core0_ids.contains(&"loki_log_shipper"));
        assert!(!core0_ids.contains(&"otlp_log_shipper"));

        // loki and otlp_log_shipper must not be on the same core
        let mut loki_core = None;
        let mut otlp_log_core = None;
        for (core, shard) in shards.iter().enumerate() {
            for p in shard {
                if p.id() == "loki_log_shipper" {
                    loki_core = Some(core);
                }
                if p.id() == "otlp_log_shipper" {
                    otlp_log_core = Some(core);
                }
            }
        }
        assert_ne!(
            loki_core, otlp_log_core,
            "Loki and OTLP log shipper must be isolated on different cores"
        );
        assert!(loki_core.unwrap() > 0);
        assert!(otlp_log_core.unwrap() > 0);
    }
}
