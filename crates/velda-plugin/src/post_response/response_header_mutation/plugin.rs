//! Response Header Mutation Post-Response Plugin (`response_header_mutation`).
//!
//! ```text
//!   on_response(req_headers, resp)
//!     ├─► (op_mask & OP_REMOVE) != 0       ──► SIMD fingerprint filter (when remove <= 32) -> remove()
//!     ├─► (op_mask & OP_STATIC_SET) != 0   ──► insert static (HeaderName, HeaderValue)
//!     ├─► (op_mask & OP_STATIC_ADD) != 0   ──► append static (HeaderName, HeaderValue)
//!     ├─► (op_mask & OP_STATIC_SET_IF_MISSING) != 0 ──► insert if !contains_key()
//!     └─► (op_mask & OP_DYNAMIC_ANY) != 0  ──► write raw u128/u64 into HeaderWorkspace
//! ```
//!
//! Adds, mutates, appends, or strips HTTP response headers before downstream frame encoding:
//! - **Single-Instruction ALU Bitmask (`op_mask: u8`)**: Eliminates empty-vector heap dereferencing and branching.
//! - **SIMD Vector-Probed Fingerprint Filter**: Rapidly filters out non-matching removal targets when `5..=32` removals are configured.
//! - **Deferred Arithmetic Variables**: Writes `$request_id`, `$status`, and `$timestamp_epoch` directly as raw integers into [`HeaderWorkspace`].
//! - **In-Place Mutation**: Modifies [`L7Response`] headers directly on the worker hot path.

use crate::config::PluginConfig;
use http::header::{HeaderName, HeaderValue};
use velda_core::{HeaderWorkspace, L7Response};

use super::config::{
    OP_DYNAMIC_ADD, OP_DYNAMIC_ANY, OP_DYNAMIC_SET, OP_DYNAMIC_SET_IF_MISSING, OP_REMOVE,
    OP_STATIC_ADD, OP_STATIC_SET, OP_STATIC_SET_IF_MISSING, ResponseHeaderMutationConfig,
};
use super::fingerprint::{compute_header_fingerprint, fingerprint_matches_simd};
use super::var::{ResponseDynamicVar, ResponseHeaderValueSpec};
use crate::capability::{AccessLevel, PluginCapability, SupportedProtocols};
use crate::error::PluginError;
use crate::post_response::PostResponsePlugin;

/// Post-Response Header Mutation Plugin with 64-byte L1 cacheline alignment.
#[derive(Debug, Clone)]
#[repr(align(64))]
pub struct ResponseHeaderMutationPlugin {
    pub id: String,
    pub config: ResponseHeaderMutationConfig,
    /// Pre-computed 8-bit bitmask encoding active mutation phases for single CPU instruction dispatch.
    pub op_mask: u8,
    /// SIMD 32-slot fingerprint vector for parallel header removal filtering.
    pub remove_fingerprints: [u8; 32],
    /// Active number of removal fingerprints (0..=32).
    pub remove_fingerprint_len: usize,
    static_sets: Vec<(HeaderName, HeaderValue)>,
    dynamic_sets: Vec<(HeaderName, ResponseDynamicVar)>,
    static_adds: Vec<(HeaderName, HeaderValue)>,
    dynamic_adds: Vec<(HeaderName, ResponseDynamicVar)>,
    static_set_if_missing: Vec<(HeaderName, HeaderValue)>,
    dynamic_set_if_missing: Vec<(HeaderName, ResponseDynamicVar)>,
}

impl ResponseHeaderMutationPlugin {
    /// Constructs a new [`ResponseHeaderMutationPlugin`] with pre-partitioned static and dynamic rules.
    pub fn new(id: impl Into<String>, config: ResponseHeaderMutationConfig) -> Self {
        let mut op_mask = 0u8;

        if !config.remove.is_empty() {
            op_mask |= OP_REMOVE;
        }

        let mut remove_fingerprints = [0u8; 32];
        let remove_fingerprint_len = config.remove.len().min(32);
        for (i, name) in config
            .remove
            .iter()
            .take(remove_fingerprint_len)
            .enumerate()
        {
            if let Some(slot) = remove_fingerprints.get_mut(i) {
                *slot = compute_header_fingerprint(name);
            }
        }

        let mut static_sets = Vec::new();
        let mut dynamic_sets = Vec::new();
        for (name, spec) in &config.set {
            match spec {
                ResponseHeaderValueSpec::Static(val) => {
                    static_sets.push((name.clone(), val.clone()))
                }
                ResponseHeaderValueSpec::Variable(var) => dynamic_sets.push((name.clone(), *var)),
            }
        }
        if !static_sets.is_empty() {
            op_mask |= OP_STATIC_SET;
        }
        if !dynamic_sets.is_empty() {
            op_mask |= OP_DYNAMIC_SET;
        }

        let mut static_adds = Vec::new();
        let mut dynamic_adds = Vec::new();
        for (name, spec) in &config.add {
            match spec {
                ResponseHeaderValueSpec::Static(val) => {
                    static_adds.push((name.clone(), val.clone()))
                }
                ResponseHeaderValueSpec::Variable(var) => dynamic_adds.push((name.clone(), *var)),
            }
        }
        if !static_adds.is_empty() {
            op_mask |= OP_STATIC_ADD;
        }
        if !dynamic_adds.is_empty() {
            op_mask |= OP_DYNAMIC_ADD;
        }

        let mut static_set_if_missing = Vec::new();
        let mut dynamic_set_if_missing = Vec::new();
        for (name, spec) in &config.set_if_missing {
            match spec {
                ResponseHeaderValueSpec::Static(val) => {
                    static_set_if_missing.push((name.clone(), val.clone()))
                }
                ResponseHeaderValueSpec::Variable(var) => {
                    dynamic_set_if_missing.push((name.clone(), *var))
                }
            }
        }
        if !static_set_if_missing.is_empty() {
            op_mask |= OP_STATIC_SET_IF_MISSING;
        }
        if !dynamic_set_if_missing.is_empty() {
            op_mask |= OP_DYNAMIC_SET_IF_MISSING;
        }

        Self {
            id: id.into(),
            config,
            op_mask,
            remove_fingerprints,
            remove_fingerprint_len,
            static_sets,
            dynamic_sets,
            static_adds,
            dynamic_adds,
            static_set_if_missing,
            dynamic_set_if_missing,
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    pub fn from_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let parsed = ResponseHeaderMutationConfig::from_plugin_config(config)?;
        Ok(Self::new(&config.id, parsed))
    }
}

impl PostResponsePlugin for ResponseHeaderMutationPlugin {
    #[inline(always)]
    fn id(&self) -> &str {
        &self.id
    }

    #[inline(always)]
    fn pbww(&self) -> u32 {
        0
    }

    fn on_response(&self, req_headers: &HeaderWorkspace, resp: &mut L7Response) {
        // Step 1: Remove explicitly requested headers
        if (self.op_mask & OP_REMOVE) != 0 && !resp.headers.is_empty() {
            // When response contains very few headers while configured removals are 5..=32,
            // probe keys via SIMD fingerprint vector to skip full HashMap linear removal scans.
            // (Skip fingerprint filter when >32 removals are configured because `remove_fingerprints` holds 32 slots.)
            if resp.headers.len() <= 4
                && self.remove_fingerprint_len > 4
                && self.config.remove.len() <= 32
            {
                let mut any_match = false;
                for key in resp.headers.keys() {
                    let fp = compute_header_fingerprint(key);
                    if fingerprint_matches_simd(
                        &self.remove_fingerprints,
                        self.remove_fingerprint_len,
                        fp,
                    ) {
                        any_match = true;
                        break;
                    }
                }
                if any_match {
                    for name in &self.config.remove {
                        resp.headers.remove(name);
                    }
                }
            } else {
                for name in &self.config.remove {
                    resp.headers.remove(name);
                }
            }
        }

        // Step 2: Set or overwrite static headers (100% direct, zero enum matching)
        if (self.op_mask & OP_STATIC_SET) != 0 {
            for (name, val) in &self.static_sets {
                resp.headers.insert(name.clone(), val.clone());
            }
        }

        // Step 3: Append static multi-value headers
        if (self.op_mask & OP_STATIC_ADD) != 0 {
            for (name, val) in &self.static_adds {
                resp.headers.append(name.clone(), val.clone());
            }
        }

        // Step 4: Set-if-missing static headers
        if (self.op_mask & OP_STATIC_SET_IF_MISSING) != 0 {
            for (name, val) in &self.static_set_if_missing {
                if !resp.headers.contains_key(name) {
                    resp.headers.insert(name.clone(), val.clone());
                }
            }
        }

        // Step 5: Dynamic arithmetic mutations (written as raw integers into HeaderWorkspace)
        if (self.op_mask & OP_DYNAMIC_ANY) != 0 {
            if (self.op_mask & OP_DYNAMIC_SET) != 0 {
                for (name, var) in &self.dynamic_sets {
                    match *var {
                        ResponseDynamicVar::RequestId => {
                            resp.headers
                                .set_hex128(name.clone(), req_headers.request_id_raw);
                        }
                        ResponseDynamicVar::Status => {
                            resp.headers
                                .set_u64(name.clone(), resp.status.as_u16() as u64);
                        }
                        ResponseDynamicVar::TimestampEpoch => {
                            resp.headers
                                .set_u64(name.clone(), req_headers.ingress_time_sec());
                        }
                    }
                }
            }

            if (self.op_mask & OP_DYNAMIC_ADD) != 0 {
                for (name, var) in &self.dynamic_adds {
                    match *var {
                        ResponseDynamicVar::RequestId => {
                            resp.headers
                                .append_hex128(name.clone(), req_headers.request_id_raw);
                        }
                        ResponseDynamicVar::Status => {
                            resp.headers
                                .append_u64(name.clone(), resp.status.as_u16() as u64);
                        }
                        ResponseDynamicVar::TimestampEpoch => {
                            resp.headers
                                .append_u64(name.clone(), req_headers.ingress_time_sec());
                        }
                    }
                }
            }

            if (self.op_mask & OP_DYNAMIC_SET_IF_MISSING) != 0 {
                for (name, var) in &self.dynamic_set_if_missing {
                    if !resp.headers.contains_key(name) {
                        match *var {
                            ResponseDynamicVar::RequestId => {
                                resp.headers
                                    .set_hex128(name.clone(), req_headers.request_id_raw);
                            }
                            ResponseDynamicVar::Status => {
                                resp.headers
                                    .set_u64(name.clone(), resp.status.as_u16() as u64);
                            }
                            ResponseDynamicVar::TimestampEpoch => {
                                resp.headers
                                    .set_u64(name.clone(), req_headers.ingress_time_sec());
                            }
                        }
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn capability(&self) -> PluginCapability {
        PluginCapability::header_only(AccessLevel::Write)
    }

    #[inline(always)]
    fn supported_protocols(&self) -> SupportedProtocols {
        SupportedProtocols::ALL_HTTP
    }
}
