//! # Declarative XDP Ingress Listener & Core Sharding
//!
//! ## Frontline Ingress Sharding
//!
//! In Velda Edge, each core-pinned worker owns an independent [`XdpListener`] instance
//! bound to a dedicated hardware queue (`queue_id == worker_id`).
//!
//! ### Pre-Binding Validation & LKG Preservation
//! Configuration updates arriving from the Control Plane undergo strict capability qualification
//! before memory allocation. [`XdpListener::bind`] calls [`validate_xdp_config`] at the start of
//! initialization. If host capabilities do not satisfy the requested configuration, the function
//! rejects the update prior to allocating UMEM pages. This allows the transport orchestrator
//! to keep the active Last Known Good (LKG) deployment running without service interruption.

use std::sync::Arc;

use crate::bpf::XdpProgramAttachment;
use crate::config::XdpConfig;
use crate::error::Result;
use crate::pool::XdpSlabArena;
use crate::probe::{InterfaceProfile, validate_xdp_config};

/// Active XDP ingress listener bound to an interface and hardware queue shard.
#[derive(Debug)]
pub struct XdpListener {
    config: XdpConfig,
    profile: InterfaceProfile,
    arena: XdpSlabArena,
    attachment: Option<Arc<XdpProgramAttachment>>,
}

impl XdpListener {
    /// Binds an XDP listener following strict capability verification.
    ///
    /// # LKG Invariant
    /// Rejects unsupported configurations without silent degradation, enabling
    /// the orchestrator to preserve the running LKG proxy instance.
    pub fn bind(config: XdpConfig) -> Result<Self> {
        // 1. Validate environment capabilities upfront before allocating resources
        let profile = validate_xdp_config(&config)?;

        // 2. Pre-allocate contiguous UMEM Slab Arena in pinned physical RAM
        let arena = XdpSlabArena::new(&config)?;

        tracing::info!(
            interface = %config.interface,
            queue_id = config.queue_id,
            engine_kind = config.engine_kind.as_str(),
            attach_mode = config.attach_mode.as_str(),
            frame_count = config.frame_count,
            "Bound XDP listener to hardware queue shard"
        );

        Ok(Self {
            config,
            profile,
            arena,
            attachment: None,
        })
    }

    /// Validated hardware interface and driver profile.
    #[inline]
    pub fn profile(&self) -> &InterfaceProfile {
        &self.profile
    }

    /// Active listener configuration.
    #[inline]
    pub fn config(&self) -> &XdpConfig {
        &self.config
    }

    /// Underlying UMEM slab memory arena.
    #[inline]
    pub fn arena(&self) -> &XdpSlabArena {
        &self.arena
    }

    /// Attaches an eBPF program lifecycle handle to ensure clean detachment upon listener drop.
    pub fn set_attachment(&mut self, attachment: Arc<XdpProgramAttachment>) {
        self.attachment = Some(attachment);
    }
}
