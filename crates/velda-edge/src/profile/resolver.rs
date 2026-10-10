//! Runtime profile resolution, operator overrides merging, and self-persisting write-back.

use std::fs;
use std::path::Path;

use velda_core::hardware::HardwareTopology;

use super::runtime::RuntimeProfile;

/// Recursively merges sparse operator overrides on top of hardware baseline values.
fn merge_json_values(base: &mut serde_json::Value, overrides: serde_json::Value) {
    match (base, overrides) {
        (serde_json::Value::Object(base_map), serde_json::Value::Object(override_map)) => {
            for (key, val) in override_map {
                merge_json_values(base_map.entry(key).or_insert(serde_json::Value::Null), val);
            }
        }
        (base, override_val) => {
            *base = override_val;
        }
    }
}

/// Resolves the runtime profile for the edge process:
/// 1. Reads `<runtime_dir>/runtime.json` if available.
/// 2. If fields are omitted, merges them on top of probed hardware tier defaults.
/// 3. If file does not exist, uses 100% hardware tier defaults.
/// 4. Best-effort writes back the complete merged `<runtime_dir>/runtime.json`.
pub fn resolve_runtime_profile(runtime_dir: &Path, hardware: &HardwareTopology) -> RuntimeProfile {
    let profile_path = runtime_dir.join("runtime.json");
    let mut profile = RuntimeProfile::from_hardware(hardware);
    let mut loaded_from_file = false;

    if profile_path.exists()
        && let Ok(content) = fs::read_to_string(&profile_path)
    {
        match serde_json::from_str::<serde_json::Value>(&content) {
            Ok(mut override_val) if override_val.is_object() => {
                let mut target_cpu_tier = hardware.cpu_tier();
                let mut target_mem_tier = hardware.memory_tier();

                if let Some(hw) = override_val
                    .get_mut("hardware")
                    .and_then(|h| h.as_object_mut())
                {
                    if let Some(t) = hw.get("tier").cloned()
                        && !hw.contains_key("memory_tier")
                    {
                        hw.insert("memory_tier".to_string(), t);
                    } else if let Some(mt) = hw.get("memory_tier").cloned()
                        && !hw.contains_key("tier")
                    {
                        hw.insert("tier".to_string(), mt);
                    }

                    if let Some(c) = hw.get("cpu_tier").and_then(|v| v.as_str())
                        && let Ok(tier) = c.parse::<velda_core::CpuTier>()
                    {
                        target_cpu_tier = tier;
                    }
                    if let Some(m) = hw.get("memory_tier").and_then(|v| v.as_str())
                        && let Ok(tier) = m.parse::<velda_core::MemoryTier>()
                    {
                        target_mem_tier = tier;
                    }
                }

                let base_profile =
                    RuntimeProfile::for_tiers(hardware, target_cpu_tier, target_mem_tier);
                if let Ok(mut base_val) = serde_json::to_value(&base_profile) {
                    merge_json_values(&mut base_val, override_val);
                    match serde_json::from_value::<RuntimeProfile>(base_val) {
                        Ok(merged_profile) => {
                            profile = merged_profile;
                            loaded_from_file = true;
                            tracing::info!(
                                path = %profile_path.display(),
                                cpu_tier = %profile.hardware.cpu_tier,
                                memory_tier = %profile.hardware.memory_tier,
                                io_workers = profile.transport.io_workers,
                                max_conns = profile.transport.max_active_connections,
                                "Loaded runtime profile from runtime.json with operator overrides"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                path = %profile_path.display(),
                                error = %e,
                                "Failed to deserialize merged runtime profile; falling back to hardware-probed defaults"
                            );
                        }
                    }
                }
            }
            Ok(_) => {
                tracing::warn!(
                    path = %profile_path.display(),
                    "runtime.json is not a valid JSON object; falling back to hardware-probed defaults"
                );
            }
            Err(e) => {
                tracing::warn!(
                    path = %profile_path.display(),
                    error = %e,
                    "Failed to parse runtime.json; falling back to hardware-probed defaults"
                );
            }
        }
    }

    // Best-effort atomic write-back: write to temp file then atomic rename to prevent corruption
    if let Ok(json_str) = serde_json::to_string_pretty(&profile) {
        if let Err(e) = fs::create_dir_all(runtime_dir) {
            tracing::warn!(
                path = %runtime_dir.display(),
                error = %e,
                "Failed to create runtime dir to persist runtime.json (read-only filesystem?)"
            );
        } else {
            let tmp_path = runtime_dir.join(format!("runtime.json.tmp.{}", std::process::id()));
            if let Err(e) = fs::write(&tmp_path, json_str) {
                tracing::warn!(
                    path = %tmp_path.display(),
                    error = %e,
                    "Failed to write temp runtime.json (read-only filesystem?)"
                );
            } else if let Err(e) = fs::rename(&tmp_path, &profile_path) {
                tracing::warn!(
                    from = %tmp_path.display(),
                    to = %profile_path.display(),
                    error = %e,
                    "Failed to atomically rename runtime.json"
                );
                let _ = fs::remove_file(&tmp_path);
            } else if !loaded_from_file {
                tracing::info!(
                    path = %profile_path.display(),
                    cpu_tier = %profile.hardware.cpu_tier,
                    memory_tier = %profile.hardware.memory_tier,
                    io_workers = profile.transport.io_workers,
                    max_conns = profile.transport.max_active_connections,
                    "Generated baseline runtime.json from hardware probe"
                );
            }
        }
    }

    profile
}
