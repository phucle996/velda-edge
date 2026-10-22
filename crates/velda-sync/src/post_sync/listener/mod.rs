//! Listener domain compilation, validation, and persistence pipeline.
//!
//! Converts raw JSON listener definitions into validated, checksummed runtime binary
//! artifacts (`listeners.bin`), performing inline host OS port conflict detection.

pub mod host_port;
pub mod schema;
pub mod validate;

#[cfg(test)]
mod tests;

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::SyncError;

pub use host_port::L4Protocol;
pub use schema::*;
pub use validate::{validate_listeners, validate_listeners_with_lkg};

// ============================================================================
// Phase 2: Ingest & Parse (JSON -> Typed Struct)
// ============================================================================

/// Parses raw JSON into listener configurations.
pub fn parse_listeners(payload: &[u8]) -> Result<Vec<ListenerConfig>, SyncError> {
    let file: ListenersFile =
        serde_json::from_slice(payload).map_err(|e| SyncError::InvalidJson {
            domain: "listeners".into(),
            reason: e.to_string(),
        })?;

    if file.schema_version != 1 {
        return Err(SyncError::Validation {
            domain: "listeners".into(),
            reason: format!(
                "Unsupported schema_version {}; expected 1",
                file.schema_version
            ),
        });
    }

    Ok(file.listeners)
}

// ============================================================================
// Phase 4: Binary Compilation & Unpack (DomainHeader + Bincode)
// ============================================================================

/// Compiles listener configurations into a self-describing binary artifact (listeners.bin).
pub fn compile_listeners_to_binary(
    listeners: &[ListenerConfig],
    revision: u64,
    source_checksum: [u8; 32],
) -> Result<Vec<u8>, SyncError> {
    // OPTIMIZATION: Pre-allocate contiguous buffer with estimated capacity (header + 128B per listener)
    // to avoid multiple vector reallocations during streaming bincode serialization.
    let estimated_cap = DOMAIN_HEADER_SIZE + listeners.len().saturating_mul(128);
    let mut binary_output = Vec::with_capacity(estimated_cap);
    binary_output.resize(DOMAIN_HEADER_SIZE, 0);

    bincode::serialize_into(&mut binary_output, listeners).map_err(|e| SyncError::Compile {
        domain: "listeners".into(),
        reason: e.to_string(),
    })?;

    let payload_len = binary_output.len() - DOMAIN_HEADER_SIZE;
    let mut hasher = Sha256::new();
    hasher.update(&binary_output[DOMAIN_HEADER_SIZE..]);
    let payload_hash: [u8; 32] = hasher.finalize().into();

    let header = DomainHeader {
        magic: MAGIC_BYTES,
        format_version: CURRENT_FORMAT_VERSION,
        compiler_version: CURRENT_COMPILER_VERSION,
        schema_version: 1,
        domain_tag: TAG_LISTENERS,
        revision,
        source_checksum,
        payload_size: payload_len as u64,
        payload_checksum: payload_hash,
    };

    let header_bytes = bincode::serialize(&header).map_err(|e| SyncError::Compile {
        domain: "listeners".into(),
        reason: e.to_string(),
    })?;
    binary_output[..DOMAIN_HEADER_SIZE].copy_from_slice(&header_bytes);

    Ok(binary_output)
}

/// Reads and verifies a compiled listeners binary artifact.
pub fn unpack_listeners_from_binary(
    bytes: &[u8],
) -> Result<(DomainHeader, Vec<ListenerConfig>), SyncError> {
    if bytes.len() < DOMAIN_HEADER_SIZE {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Artifact smaller than header size".into(),
        });
    }

    let header: DomainHeader =
        bincode::deserialize(&bytes[..DOMAIN_HEADER_SIZE]).map_err(|e| SyncError::Compile {
            domain: "listeners".into(),
            reason: format!("Corrupted header: {e}"),
        })?;

    if header.magic != MAGIC_BYTES {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Invalid magic bytes".into(),
        });
    }

    if header.domain_tag != TAG_LISTENERS {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Domain tag mismatch".into(),
        });
    }

    let payload_bytes = &bytes[DOMAIN_HEADER_SIZE..];
    if payload_bytes.len() as u64 != header.payload_size {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Payload size mismatch".into(),
        });
    }

    let mut hasher = Sha256::new();
    hasher.update(payload_bytes);
    let actual_hash: [u8; 32] = hasher.finalize().into();
    if actual_hash != header.payload_checksum {
        return Err(SyncError::Compile {
            domain: "listeners".into(),
            reason: "Payload checksum mismatch".into(),
        });
    }

    let listeners: Vec<ListenerConfig> =
        bincode::deserialize(payload_bytes).map_err(|e| SyncError::Compile {
            domain: "listeners".into(),
            reason: format!("Failed to deserialize: {e}"),
        })?;

    Ok((header, listeners))
}

// ============================================================================
// Phase 5: Atomic Persistence (Durable LKG)
// ============================================================================

/// Atomically persists listeners canonical JSON and binary artifact to the LKG storage directory.
pub fn persist_listeners(
    storage_dir: &Path,
    json_bytes: &[u8],
    bin_bytes: &[u8],
) -> Result<(), SyncError> {
    let json_path = storage_dir.join("config").join("listeners.json");
    let bin_path = storage_dir.join("runtime").join("listeners.bin");

    for (target, content) in [(&json_path, json_bytes), (&bin_path, bin_bytes)] {
        let parent = target.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let file_name = target
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");
        let tmp_path = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(content)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, target)?;
    }

    Ok(())
}
