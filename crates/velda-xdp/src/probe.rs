//! # Hardware Capability & XDP Authority Probe (Validate-Only)
//!
//! ## Fail-Fast Verification & Last Known Good (LKG) Protection
//!
//! In production edge proxy deployments:
//! - Configuring `XdpAttachMode::Drv` asserts zero-copy line-rate packet ingestion with sub-microsecond latency.
//! - Silently downgrading to `Skb` (Generic) mode when the NIC driver lacks native support masks deployment errors:
//!   the service appears healthy under light traffic, but saturates CPU and silently drops packets under peak load.
//! - This module strictly enforces a **Validate-Only** invariant:
//!   Kernel version, driver hooks, and interface queues are validated prior to resource allocation.
//!   Any deficiency immediately returns a typed error, allowing the orchestrator to abort the reload and
//!   preserve the healthy running **Last Known Good (LKG)** instance without dropping connections.

use std::ffi::CString;
use std::fs;
use std::path::Path;

use velda_core::hardware::probe_kernel;

use crate::config::{XdpAttachMode, XdpConfig, XdpEngineKind};
use crate::error::{Result, XdpError};

/// Probed hardware interface properties and driver capabilities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceProfile {
    /// Network interface name (e.g. "eth0", "lo", "enp1s0").
    pub name: String,
    /// Linux kernel interface index (`ifindex`).
    pub ifindex: u32,
    /// Hardware device driver name read from sysfs (e.g. "mlx5_core", "i40e", "virtio_net", "r8169").
    pub driver: Option<String>,
    /// Number of physical hardware RX queues.
    pub rx_queues: u32,
    /// Operational state ("up", "down").
    pub operstate: String,
    /// Maximum Transmission Unit (MTU) configured on the interface.
    pub mtu: u32,
    /// Indicates whether the driver implements `ndo_bpf` for Native Zero-Copy XDP (`Drv`).
    pub supports_native_xdp: bool,
    /// Indicates whether the device is a SmartNIC supporting bytecode offload (`Hw`).
    pub supports_hw_offload: bool,
}

/// Known Linux drivers with verified Native XDP Driver Mode (`Drv`) support.
///
/// These drivers implement the `ndo_bpf` callback in `struct net_device_ops`,
/// enabling eBPF execution directly within the NIC NAPI polling routine.
pub const KNOWN_NATIVE_XDP_DRIVERS: &[&str] = &[
    "mlx5_core",   // Mellanox ConnectX-4 / 5 / 6 / 7 (Primary datacenter target)
    "mlx4_en",     // Mellanox ConnectX-3
    "i40e",        // Intel XL710 / X710 40GbE
    "ixgbe",       // Intel 82599 10GbE
    "ice",         // Intel E810 100GbE
    "igb",         // Intel 1GbE
    "virtio_net",  // QEMU / KVM / Cloud Virtual NICs (AWS, GCP, DigitalOcean VMs)
    "ena",         // AWS Elastic Network Adapter
    "gve",         // GCP Virtual Ethernet
    "r8169",       // Realtek GbE onboard controllers (Linux >= 5.10)
    "bnxt_en",     // Broadcom NetXtreme-E
    "thunder_nic", // Marvell ThunderX
    "nfp",         // Netronome SmartNIC
];

/// SmartNIC drivers supporting eBPF execution directly on onboard ASIC hardware (`Hw`).
pub const KNOWN_HW_OFFLOAD_DRIVERS: &[&str] = &[
    "nfp", // Netronome Agilio SmartNIC
];

/// Resolves the Linux network interface index for an interface name via POSIX `if_nametoindex`.
pub fn resolve_ifindex(interface: &str) -> Result<u32> {
    let c_name =
        CString::new(interface).map_err(|_| XdpError::InterfaceNotFound(interface.to_string()))?;
    let idx = unsafe { libc::if_nametoindex(c_name.as_ptr()) };
    if idx == 0 {
        return Err(XdpError::InterfaceNotFound(interface.to_string()));
    }
    Ok(idx)
}

/// Inspects `/sys/class/net/<if>` to identify physical hardware properties without intermediate abstraction.
pub fn probe_interface(interface: &str) -> Result<InterfaceProfile> {
    let ifindex = resolve_ifindex(interface)?;
    let base_path = format!("/sys/class/net/{interface}");
    let path = Path::new(&base_path);

    if !path.exists() {
        return Err(XdpError::InterfaceNotFound(interface.to_string()));
    }

    // 1. Interface UP / DOWN operational state
    let operstate = fs::read_to_string(path.join("operstate"))
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_string();

    // 2. Hardware MTU
    let mtu = fs::read_to_string(path.join("mtu"))
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(1500);

    // 3. Driver resolution via /device/driver symlink in sysfs
    let driver = fs::read_link(path.join("device/driver"))
        .ok()
        .and_then(|p| p.file_name().map(|f| f.to_string_lossy().to_string()));

    // 4. Physical hardware RX queue count
    let mut rx_queues = 0;
    if let Ok(entries) = fs::read_dir(path.join("queues")) {
        for entry in entries.flatten() {
            if let Some(file_name) = entry.file_name().to_str()
                && file_name.starts_with("rx-")
            {
                rx_queues += 1;
            }
        }
    }
    if rx_queues == 0 {
        rx_queues = 1;
    }

    let supports_native_xdp = match &driver {
        Some(d) => KNOWN_NATIVE_XDP_DRIVERS.contains(&d.as_str()),
        None => false,
    };

    let supports_hw_offload = match &driver {
        Some(d) => KNOWN_HW_OFFLOAD_DRIVERS.contains(&d.as_str()),
        None => false,
    };

    Ok(InterfaceProfile {
        name: interface.to_string(),
        ifindex,
        driver,
        rx_queues,
        operstate,
        mtu,
        supports_native_xdp,
        supports_hw_offload,
    })
}

/// Validates host environment requirements against the requested [`XdpConfig`].
///
/// Serving as the primary LKG protection barrier, this function rejects any configuration
/// whose kernel version, permissions, or hardware drivers do not strictly satisfy requirements.
pub fn validate_xdp_config(config: &XdpConfig) -> Result<InterfaceProfile> {
    // 1. Linux Kernel version qualification
    let kernel = probe_kernel();
    if !kernel.version.is_at_least(4, 18) {
        return Err(XdpError::CapabilityMismatch {
            interface: config.interface.clone(),
            required: "Linux Kernel >= 4.18".to_string(),
            actual: format!("Linux {}", kernel.version),
            reason: "AF_XDP sockets require Linux kernel version 4.18 or higher",
        });
    }

    if config.need_wakeup && !kernel.version.is_at_least(5, 4) {
        return Err(XdpError::CapabilityMismatch {
            interface: config.interface.clone(),
            required: "Linux Kernel >= 5.4".to_string(),
            actual: format!("Linux {}", kernel.version),
            reason: "XDP_USE_NEED_WAKEUP optimization requires Linux kernel version 5.4 or higher",
        });
    }

    // 2. Interface existence and driver profile
    let profile = probe_interface(&config.interface)?;

    // 3. Queue ID boundary check: queue_id cannot exceed physical RX queue count
    if config.queue_id >= profile.rx_queues && config.interface != "lo" {
        return Err(XdpError::CapabilityMismatch {
            interface: config.interface.clone(),
            required: format!("Queue ID < {}", profile.rx_queues),
            actual: format!("Queue ID {}", config.queue_id),
            reason: "Configured RX queue ID exceeds total available hardware RX queues on the interface",
        });
    }

    // 4. Attachment mode compatibility:
    // Loopback ('lo') is a purely synthetic kernel construct lacking physical ASIC DMA rings,
    // causing native driver attachment (`XDP_FLAGS_DRV_MODE`) to fail with EOPNOTSUPP.
    match config.attach_mode {
        XdpAttachMode::Skb => {
            // Generic mode runs universally on any interface (loopback, veth, VM virtual interfaces)
        }
        XdpAttachMode::Drv => {
            if config.interface == "lo" {
                return Err(XdpError::DriverNotSupported {
                    interface: config.interface.clone(),
                    driver: "loopback".to_string(),
                    mode: "drv",
                    reason: "Loopback is a synthetic kernel interface without DMA hardware rings. Use Skb mode for loopback testing.",
                });
            }
            if !profile.supports_native_xdp {
                let driver_name = profile
                    .driver
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                return Err(XdpError::DriverNotSupported {
                    interface: config.interface.clone(),
                    driver: driver_name,
                    mode: "drv",
                    reason: "Network interface driver does not implement the ndo_bpf native XDP driver hook",
                });
            }
        }
        XdpAttachMode::Hw => {
            if !profile.supports_hw_offload {
                let driver_name = profile
                    .driver
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                return Err(XdpError::DriverNotSupported {
                    interface: config.interface.clone(),
                    driver: driver_name,
                    mode: "hw",
                    reason: "Network interface lacks SmartNIC hardware offload capability for on-chip eBPF execution",
                });
            }
        }
    }

    // 5. Zero-copy qualification: Zero-copy DMA requires Native Driver Mode
    if config.zero_copy && config.attach_mode == XdpAttachMode::Skb {
        return Err(XdpError::CapabilityMismatch {
            interface: config.interface.clone(),
            required: "Native driver mode (Drv)".to_string(),
            actual: "Generic SKB mode".to_string(),
            reason: "Zero-copy DMA requires direct NIC driver cooperation (Drv mode) and cannot be enabled on Generic Skb mode",
        });
    }

    // 6. Sockmap qualification: Linux >= 5.10 required for stable sockmap/sk_msg stream redirection
    if config.engine_kind == XdpEngineKind::Sockmap && !kernel.version.is_at_least(5, 10) {
        return Err(XdpError::CapabilityMismatch {
            interface: config.interface.clone(),
            required: "Linux Kernel >= 5.10".to_string(),
            actual: format!("Linux {}", kernel.version),
            reason: "In-kernel BPF sockmap stream splicing requires Linux kernel version 5.10 or higher for memory safety",
        });
    }

    Ok(profile)
}
