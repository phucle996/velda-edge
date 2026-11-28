//! StatsD / DogStatsD Push Metrics Provider (`metrics_exporter::providers::statsd`).

pub mod dialect;
pub mod wire;

use std::sync::Arc;
use std::time::Duration;

use velda_etp::statsd::{StatsdEtp, StatsdFlavor};
use velda_observability::{MetricsSnapshot, MetricsStore};

use crate::background::metrics_exporter::config::StatsdMetricsPushConfig;
use crate::error::PluginError;

use self::dialect::{StatsdDialect, format_statsd_snapshot};

pub struct StatsdMetricsProvider {
    pub etp: Arc<StatsdEtp>,
    pub prefix: String,
    pub dialect: StatsdDialect,
    pub interval: Duration,
    pub max_packet_size: usize,
    packet_buffer: Vec<u8>,
    prev_snapshot: MetricsSnapshot,
}

impl StatsdMetricsProvider {
    pub fn new(etp: Arc<StatsdEtp>, cfg: &StatsdMetricsPushConfig) -> Self {
        let prefix = match cfg.prefix.trim() {
            "" => etp
                .config
                .prefix
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("velda")
                .to_string(),
            explicit => explicit.to_string(),
        };
        let dialect = match cfg.dialect {
            Some(d) => d,
            None => match etp.config.flavor {
                StatsdFlavor::Standard => StatsdDialect::Standard,
                StatsdFlavor::DogStatsd => StatsdDialect::DogStatsd,
            },
        };

        let mut packet_buffer = Vec::with_capacity(cfg.max_packet_size + 256);
        let prev_snapshot = MetricsSnapshot::zero();
        let _ = format_statsd_snapshot(
            &prev_snapshot,
            &prefix,
            dialect,
            &mut packet_buffer,
            cfg.max_packet_size,
            &mut |_| {},
        );

        Self {
            etp,
            prefix,
            dialect,
            interval: Duration::from_secs(cfg.interval_secs),
            max_packet_size: cfg.max_packet_size,
            packet_buffer,
            prev_snapshot,
        }
    }

    pub async fn push_tick(&mut self, store: &MetricsStore) -> Result<usize, PluginError> {
        let snapshot = store.snapshot();
        let delta = snapshot.delta_since(&self.prev_snapshot);

        let (_packets, metrics_sent) = wire::flush_snapshot(
            &self.etp,
            &self.prefix,
            self.dialect,
            self.max_packet_size,
            &delta,
            &mut self.packet_buffer,
        )
        .await?;

        self.prev_snapshot = snapshot;
        Ok(metrics_sent)
    }
}
