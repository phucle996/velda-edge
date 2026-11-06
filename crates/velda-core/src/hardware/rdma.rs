//! RDMA hardware and subsystem probe for the Velda Edge Data Plane.
//!
//! # Problem & Architectural Intent
//! In hyperscale edge proxying and internal cluster meshes, standard kernel TCP/UDP
//! networking incurs CPU context switches, interrupt handling, and multiple memory copies
//! (RAM -> kernel sk_buff -> user space buffer). Remote Direct Memory Access (RDMA) provides:
//! 1. **Zero-Copy**: The network interface card (RNIC) reads/writes directly into process RAM.
//! 2. **Kernel Bypass**: I/O operations bypass OS kernel context switches and syscalls.
//! 3. **Sub-microsecond Latency**: Transfers execute in < 1–2 microseconds.
//!
//! # Probed Hardware Types
//! Velda Edge autonomously probes the Linux sysfs subsystem (`/sys/class/infiniband/`)
//! during cold-start bootstrap without hardcoding or vendor lock-in:
//! - **InfiniBand (IB)**: Native InfiniBand link layer, hardware credit flow control.
//! - **RoCE v2**: RDMA over Converged Ethernet (UDP encapsulated on port 4791, L3 routable).
//! - **RoCE v1**: RDMA over Ethernet L2 framing (non-routable).
//! - **iWARP**: RDMA over standard TCP/IP streams (IETF RFC 5040).
//! - **AWS EFA**: Amazon Elastic Fabric Adapter (SRD protocol for AWS EC2).
//!
//! # Invariant
//! Probing is strictly a cold-path operation. Results are cached in [`crate::hardware::HardwareTopology`].
//! If no RDMA device is present (standard VPS, dev laptops, containers without device passthrough),
//! the probe returns [`RdmaProfile::none`], triggering safe, zero-overhead fallback to standard sockets.
//!
//! Safety valve: `VELDA_RDMA=disabled` forces standard socket operation even when hardware exists.

use std::path::Path;

/// Transport/Link layer classification for RDMA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaTransportType {
    /// Native InfiniBand link layer (lossless L2 credit-based, sub-microsecond latency).
    InfiniBand,
    /// RDMA over Converged Ethernet v2 (UDP encapsulated on port 4791, L3 routable).
    RoceV2,
    /// RDMA over Converged Ethernet v1 (Ethernet L2 framing, non-routable).
    RoceV1,
    /// Internet Wide Area RDMA Protocol (TCP/IP encapsulated, routable).
    Iwarp,
    /// AWS Elastic Fabric Adapter (SRD protocol for AWS EC2 instances).
    Efa,
    /// Unrecognized or unparsed link layer.
    Unknown,
}

impl RdmaTransportType {
    /// Returns a short display name for the transport type.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::InfiniBand => "InfiniBand",
            Self::RoceV2 => "RoCE v2",
            Self::RoceV1 => "RoCE v1",
            Self::Iwarp => "iWARP",
            Self::Efa => "AWS EFA",
            Self::Unknown => "Unknown",
        }
    }
}

/// Vendor identification for RDMA hardware adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RdmaVendor {
    /// NVIDIA / Mellanox Technologies (PCI vendor `0x15b3`).
    Mellanox,
    /// Intel Corporation (PCI vendor `0x8086`).
    Intel,
    /// Broadcom Inc. (PCI vendor `0x14e4`).
    Broadcom,
    /// Amazon.com, Inc. (PCI vendor `0x1d0f` - AWS EFA).
    Amazon,
    /// Other vendor identified by PCI vendor ID.
    Other(u16),
    /// Unidentified or virtual device.
    Unknown,
}

impl RdmaVendor {
    /// Identifies vendor from 16-bit PCI vendor ID.
    pub const fn from_pci_vendor_id(id: u16) -> Self {
        match id {
            0x15b3 => Self::Mellanox,
            0x8086 => Self::Intel,
            0x14e4 => Self::Broadcom,
            0x1d0f => Self::Amazon,
            other => Self::Other(other),
        }
    }

    /// Returns a human-readable vendor name.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Mellanox => "NVIDIA/Mellanox",
            Self::Intel => "Intel",
            Self::Broadcom => "Broadcom",
            Self::Amazon => "Amazon AWS",
            Self::Other(_) => "Other",
            Self::Unknown => "Unknown",
        }
    }
}

/// Status and hardware profile of host RDMA subsystem (cold path, cached in `HardwareTopology`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RdmaProfile {
    /// Whether at least one active, supported RDMA device is present.
    pub is_available: bool,
    /// Primary detected transport type.
    pub transport: RdmaTransportType,
    /// Vendor of the primary active device.
    pub vendor: RdmaVendor,
    /// Number of probed RDMA devices in sysfs.
    pub device_count: u8,
    /// Number of active ports across all verified devices.
    pub active_ports: u8,
    /// Primary device's NUMA node affinity (-1 if non-NUMA or unspecified).
    pub numa_node: i32,
    /// Max detected link rate in Gbps across active ports (0 if unknown).
    pub max_rate_gbps: u16,
    /// Diagnostic summary for telemetry and banner.
    pub reason: &'static str,
}

impl RdmaProfile {
    /// Returns a profile representing no available RDMA hardware.
    pub const fn none(reason: &'static str) -> Self {
        Self {
            is_available: false,
            transport: RdmaTransportType::Unknown,
            vendor: RdmaVendor::Unknown,
            device_count: 0,
            active_ports: 0,
            numa_node: -1,
            max_rate_gbps: 0,
            reason,
        }
    }

    /// Formats a short summary string for logs and banner.
    /// E.g. "RoCE v2 (100 Gbps, NUMA 0, NVIDIA/Mellanox)" or "None (Standard Sockets)".
    pub fn display_summary(&self) -> String {
        if !self.is_available {
            return "None (Standard Sockets)".to_string();
        }
        let mut s = String::with_capacity(64);
        s.push_str(self.transport.as_str());

        let mut details = Vec::new();
        if self.max_rate_gbps > 0 {
            details.push(format!("{} Gbps", self.max_rate_gbps));
        }
        if self.numa_node >= 0 {
            details.push(format!("NUMA {}", self.numa_node));
        }
        if self.vendor != RdmaVendor::Unknown {
            details.push(self.vendor.as_str().to_string());
        }

        if !details.is_empty() {
            s.push_str(" (");
            s.push_str(&details.join(", "));
            s.push(')');
        }
        s
    }
}

/// Probes the host RDMA subsystem via Linux sysfs `/sys/class/infiniband`
/// (or custom path via `VELDA_RDMA_SYSFS` for testing and custom mounts).
pub fn probe_rdma() -> RdmaProfile {
    if let Ok(val) = std::env::var("VELDA_RDMA") {
        let v = val.trim().to_ascii_lowercase();
        if v == "disabled" || v == "0" || v == "false" || v == "off" || v == "none" {
            return RdmaProfile::none("disabled via VELDA_RDMA environment variable");
        }
    }
    if let Ok(sysfs_path) = std::env::var("VELDA_RDMA_SYSFS") {
        return probe_rdma_from(Path::new(&sysfs_path));
    }
    probe_rdma_from(Path::new("/sys/class/infiniband"))
}

/// Probes RDMA devices from an explicit sysfs infiniband path (enables unit testing with mock sysfs).
pub fn probe_rdma_from(sysfs_infiniband: &Path) -> RdmaProfile {
    if !sysfs_infiniband.exists() {
        return RdmaProfile::none("no /sys/class/infiniband directory found");
    }
    let entries = match std::fs::read_dir(sysfs_infiniband) {
        Ok(e) => e,
        Err(_) => return RdmaProfile::none("cannot read /sys/class/infiniband directory"),
    };

    let mut device_count = 0u8;
    let mut total_active_ports = 0u8;
    let mut best_transport = RdmaTransportType::Unknown;
    let mut best_vendor = RdmaVendor::Unknown;
    let mut best_numa = -1i32;
    let mut max_rate = 0u16;

    for entry in entries.flatten() {
        let dev_path = entry.path();
        if !dev_path.is_dir() {
            continue;
        }
        device_count = device_count.saturating_add(1);

        // Read vendor ID from <dev>/device/vendor
        let vendor = read_trim_file(&dev_path.join("device/vendor"))
            .and_then(|v| parse_hex_u16(&v))
            .map(RdmaVendor::from_pci_vendor_id)
            .unwrap_or(RdmaVendor::Unknown);

        // Read NUMA node from <dev>/device/numa_node
        let numa = read_trim_file(&dev_path.join("device/numa_node"))
            .and_then(|n| n.parse::<i32>().ok())
            .unwrap_or(-1);

        // Check node_type (e.g. "1: CA", "4: RNIC", "6: EFA")
        let node_type = read_trim_file(&dev_path.join("node_type")).unwrap_or_default();
        let is_efa =
            vendor == RdmaVendor::Amazon || node_type.contains("EFA") || node_type.starts_with('6');

        // Inspect ports in <dev>/ports
        let ports_dir = dev_path.join("ports");
        if let Ok(port_entries) = std::fs::read_dir(&ports_dir) {
            for port_entry in port_entries.flatten() {
                let port_path = port_entry.path();
                if !port_path.is_dir() {
                    continue;
                }

                // Check port state (e.g. "4: ACTIVE")
                let state_str = read_trim_file(&port_path.join("state")).unwrap_or_default();
                let is_active = state_str.starts_with('4') || state_str.contains("ACTIVE");
                if !is_active {
                    continue;
                }

                total_active_ports = total_active_ports.saturating_add(1);

                // Parse link rate (e.g. "100 Gb/sec (4X EDR)" -> 100)
                let rate_gbps = read_trim_file(&port_path.join("rate"))
                    .and_then(|r| parse_rate_gbps(&r))
                    .unwrap_or(0);
                if rate_gbps > max_rate {
                    max_rate = rate_gbps;
                }

                // Determine transport
                let transport = if is_efa {
                    RdmaTransportType::Efa
                } else {
                    let link_layer =
                        read_trim_file(&port_path.join("link_layer")).unwrap_or_default();
                    if link_layer.eq_ignore_ascii_case("infiniband") {
                        RdmaTransportType::InfiniBand
                    } else if link_layer.eq_ignore_ascii_case("ethernet") {
                        classify_roce_or_iwarp(&port_path)
                    } else {
                        RdmaTransportType::Unknown
                    }
                };

                // Prioritize first active device with recognized transport
                if (best_transport == RdmaTransportType::Unknown
                    && transport != RdmaTransportType::Unknown)
                    || (transport == RdmaTransportType::InfiniBand
                        && best_transport != RdmaTransportType::InfiniBand)
                {
                    best_transport = transport;
                    best_vendor = vendor;
                    best_numa = numa;
                }
            }
        }
    }

    if total_active_ports == 0 || best_transport == RdmaTransportType::Unknown {
        if device_count > 0 {
            return RdmaProfile {
                is_available: false,
                transport: best_transport,
                vendor: best_vendor,
                device_count,
                active_ports: 0,
                numa_node: best_numa,
                max_rate_gbps: max_rate,
                reason: "RDMA devices registered in sysfs but no active link ports",
            };
        }
        return RdmaProfile::none("no active RDMA devices found in sysfs");
    }

    RdmaProfile {
        is_available: true,
        transport: best_transport,
        vendor: best_vendor,
        device_count,
        active_ports: total_active_ports,
        numa_node: best_numa,
        max_rate_gbps: max_rate,
        reason: "hardware RDMA verified active",
    }
}

/// Classifies RoCE v2, RoCE v1, or iWARP by inspecting `<port>/gid_attrs/types/`.
fn classify_roce_or_iwarp(port_path: &Path) -> RdmaTransportType {
    let gid_types_dir = port_path.join("gid_attrs/types");
    if let Ok(entries) = std::fs::read_dir(&gid_types_dir) {
        let mut has_roce_v1 = false;
        let mut has_iwarp = false;

        for entry in entries.flatten() {
            if let Some(content) = read_trim_file(&entry.path()) {
                if content.contains("RoCE v2") {
                    return RdmaTransportType::RoceV2;
                }
                if content.contains("RoCE v1") || content.contains("IB/RoCE v1") {
                    has_roce_v1 = true;
                }
                if content.contains("iWARP") {
                    has_iwarp = true;
                }
            }
        }

        if has_roce_v1 {
            return RdmaTransportType::RoceV1;
        }
        if has_iwarp {
            return RdmaTransportType::Iwarp;
        }
    }

    // Default for Ethernet RDMA when gid_attrs is unavailable
    RdmaTransportType::RoceV2
}

#[inline]
fn read_trim_file(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[inline]
fn parse_hex_u16(s: &str) -> Option<u16> {
    let s = s.trim();
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    u16::from_str_radix(hex, 16).ok()
}

#[inline]
fn parse_rate_gbps(s: &str) -> Option<u16> {
    let trimmed = s.trim();
    let num_str = trimmed.split_whitespace().next()?;
    num_str.parse::<u16>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_rdma_none_when_directory_missing() {
        let non_existent = Path::new("/sys/class/definitely_not_existing_infiniband_path");
        let profile = probe_rdma_from(non_existent);
        assert!(!profile.is_available);
        assert_eq!(profile.transport, RdmaTransportType::Unknown);
        assert_eq!(profile.display_summary(), "None (Standard Sockets)");
    }

    #[test]
    fn test_parse_hex_u16() {
        assert_eq!(parse_hex_u16("0x15b3"), Some(0x15b3));
        assert_eq!(parse_hex_u16("0X8086"), Some(0x8086));
        assert_eq!(parse_hex_u16("14e4"), Some(0x14e4));
        assert_eq!(parse_hex_u16("invalid"), None);
    }

    #[test]
    fn test_parse_rate_gbps() {
        assert_eq!(parse_rate_gbps("100 Gb/sec (4X EDR)"), Some(100));
        assert_eq!(parse_rate_gbps("200 Gb/sec (4X HDR)"), Some(200));
        assert_eq!(parse_rate_gbps("400 Gb/sec (4X NDR)"), Some(400));
        assert_eq!(parse_rate_gbps("25 Gb/sec"), Some(25));
        assert_eq!(parse_rate_gbps("10 Gb/sec"), Some(10));
        assert_eq!(parse_rate_gbps(""), None);
    }

    #[test]
    fn test_mock_mellanox_rocev2_sysfs() {
        let test_dir =
            std::env::temp_dir().join(format!("velda_rdma_mock_mellanox_{}", std::process::id()));
        let _ = fs::remove_dir_all(&test_dir);

        let dev_dir = test_dir.join("mlx5_0");
        let port_dir = dev_dir.join("ports/1");
        let gid_dir = port_dir.join("gid_attrs/types");

        fs::create_dir_all(&gid_dir).unwrap();
        fs::create_dir_all(dev_dir.join("device")).unwrap();

        fs::write(dev_dir.join("device/vendor"), "0x15b3\n").unwrap();
        fs::write(dev_dir.join("device/numa_node"), "0\n").unwrap();
        fs::write(dev_dir.join("node_type"), "1: CA\n").unwrap();
        fs::write(port_dir.join("state"), "4: ACTIVE\n").unwrap();
        fs::write(port_dir.join("link_layer"), "Ethernet\n").unwrap();
        fs::write(port_dir.join("rate"), "100 Gb/sec (4X EDR)\n").unwrap();
        fs::write(gid_dir.join("0"), "RoCE v2\n").unwrap();

        let profile = probe_rdma_from(&test_dir);
        assert!(profile.is_available);
        assert_eq!(profile.transport, RdmaTransportType::RoceV2);
        assert_eq!(profile.vendor, RdmaVendor::Mellanox);
        assert_eq!(profile.numa_node, 0);
        assert_eq!(profile.max_rate_gbps, 100);
        assert_eq!(profile.active_ports, 1);
        assert_eq!(profile.device_count, 1);
        assert_eq!(
            profile.display_summary(),
            "RoCE v2 (100 Gbps, NUMA 0, NVIDIA/Mellanox)"
        );

        let _ = fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn test_mock_infiniband_ndr_sysfs() {
        let test_dir =
            std::env::temp_dir().join(format!("velda_rdma_mock_ib_{}", std::process::id()));
        let _ = fs::remove_dir_all(&test_dir);

        let dev_dir = test_dir.join("mlx5_0");
        let port_dir = dev_dir.join("ports/1");

        fs::create_dir_all(&port_dir).unwrap();
        fs::create_dir_all(dev_dir.join("device")).unwrap();

        fs::write(dev_dir.join("device/vendor"), "0x15b3\n").unwrap();
        fs::write(dev_dir.join("device/numa_node"), "1\n").unwrap();
        fs::write(port_dir.join("state"), "4: ACTIVE\n").unwrap();
        fs::write(port_dir.join("link_layer"), "InfiniBand\n").unwrap();
        fs::write(port_dir.join("rate"), "400 Gb/sec (4X NDR)\n").unwrap();

        let profile = probe_rdma_from(&test_dir);
        assert!(profile.is_available);
        assert_eq!(profile.transport, RdmaTransportType::InfiniBand);
        assert_eq!(profile.vendor, RdmaVendor::Mellanox);
        assert_eq!(profile.numa_node, 1);
        assert_eq!(profile.max_rate_gbps, 400);
        assert_eq!(
            profile.display_summary(),
            "InfiniBand (400 Gbps, NUMA 1, NVIDIA/Mellanox)"
        );

        let _ = fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn test_mock_aws_efa_sysfs() {
        let test_dir =
            std::env::temp_dir().join(format!("velda_rdma_mock_efa_{}", std::process::id()));
        let _ = fs::remove_dir_all(&test_dir);

        let dev_dir = test_dir.join("efa0");
        let port_dir = dev_dir.join("ports/1");

        fs::create_dir_all(&port_dir).unwrap();
        fs::create_dir_all(dev_dir.join("device")).unwrap();

        fs::write(dev_dir.join("device/vendor"), "0x1d0f\n").unwrap();
        fs::write(dev_dir.join("node_type"), "6: EFA\n").unwrap();
        fs::write(port_dir.join("state"), "4: ACTIVE\n").unwrap();
        fs::write(port_dir.join("rate"), "100 Gb/sec\n").unwrap();

        let profile = probe_rdma_from(&test_dir);
        assert!(profile.is_available);
        assert_eq!(profile.transport, RdmaTransportType::Efa);
        assert_eq!(profile.vendor, RdmaVendor::Amazon);
        assert_eq!(profile.max_rate_gbps, 100);

        let _ = fs::remove_dir_all(&test_dir);
    }

    #[test]
    fn test_mock_down_port_returns_unavailable() {
        let test_dir =
            std::env::temp_dir().join(format!("velda_rdma_mock_down_{}", std::process::id()));
        let _ = fs::remove_dir_all(&test_dir);

        let dev_dir = test_dir.join("mlx5_0");
        let port_dir = dev_dir.join("ports/1");

        fs::create_dir_all(&port_dir).unwrap();
        fs::write(port_dir.join("state"), "2: DOWN\n").unwrap();

        let profile = probe_rdma_from(&test_dir);
        assert!(!profile.is_available);
        assert_eq!(profile.active_ports, 0);

        let _ = fs::remove_dir_all(&test_dir);
    }
}
