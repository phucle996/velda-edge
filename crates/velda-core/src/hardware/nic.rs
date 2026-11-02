//! NIC hardware RSS probe: decides who owns ingress flow hashing — the NIC or the kernel.
//!
//! # Problem
//! A multi-queue NIC hashes each flow (Toeplitz 4-tuple + indirection table) into an RX queue
//! whose IRQ is pinned to one CPU. If `SO_REUSEPORT` then runs a *second* software hash to pick
//! a listener socket, the packet is bounced to another core (cache miss + IPI). The fix is to let
//! the NIC's choice win via `SO_INCOMING_CPU = <core that received the packet>`.
//!
//! The opposite failure is worse: on a NIC that does **not** spread flows, `SO_INCOMING_CPU`
//! funnels every flow into the single worker that owns the IRQ core. So hardware authority is
//! granted ONLY when every condition below is proven, otherwise the kernel software hash stays in
//! charge (the safe default).
//!
//! # Proof obligations for [`IngressHashAuthority::HardwareRss`]
//! For every non-loopback interface with carrier (enslaved VFs are covered by their master):
//! 1. The interface has a physical device, `>= 2` RX queues (`/sys/class/net/<if>/queues/rx-*`).
//! 2. The RSS indirection table (`ETHTOOL_GRSSH`) exists and spreads over `>= 2` distinct rings.
//! 3. RPS is off on every RX queue (RPS would re-steer packets away from the IRQ CPU).
//! 4. The queue IRQs (own device, or the lower VF device for netvsc) are pinned to `>= 2`
//!    distinct CPUs.
//!
//! At bind time ([`ingress_steering_cpu`]) one more condition is required: the union of the RX IRQ
//! CPUs must cover **every** traffic worker core, otherwise some workers would never receive
//! packets. Any doubt (unreadable sysfs, ioctl failure, unknown driver layout) means `Software`.
//!
//! Safety valve: `VELDA_INGRESS_HASH=software` forces software hashing.

/// Maximum CPU id (exclusive) representable in [`NicProfile::rx_cpu_mask`].
pub const NIC_MAX_CPUS: usize = 1024;
const MASK_WORDS: usize = NIC_MAX_CPUS / 64;

/// Who owns the ingress flow-to-core decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngressHashAuthority {
    /// NIC hardware RSS is proven active and spreads flows over the RX IRQ CPUs.
    HardwareRss,
    /// Kernel `SO_REUSEPORT` software hash distributes flows (safe default).
    Software,
}

/// Probed NIC ingress distribution profile (cold path, cached in `HardwareTopology`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NicProfile {
    /// Which side hashes flows across cores.
    pub authority: IngressHashAuthority,
    /// Number of interfaces whose RSS was verified.
    pub verified_interfaces: u8,
    /// Smallest RX queue count among verified interfaces (0 when `Software`).
    pub rx_queues: u16,
    /// Bitmask of CPUs that receive RX queue IRQs (empty when `Software`).
    pub rx_cpu_mask: [u64; MASK_WORDS],
    /// Human-readable decision reason for logs.
    pub reason: &'static str,
}

impl NicProfile {
    /// Software-authority profile with a reason.
    pub const fn software(reason: &'static str) -> Self {
        Self {
            authority: IngressHashAuthority::Software,
            verified_interfaces: 0,
            rx_queues: 0,
            rx_cpu_mask: [0; MASK_WORDS],
            reason,
        }
    }

    /// `true` only when hardware RSS is proven AND its IRQ CPUs cover every given core.
    pub fn steers_all_cores(&self, cores: &[usize]) -> bool {
        self.authority == IngressHashAuthority::HardwareRss
            && !cores.is_empty()
            && cores
                .iter()
                .all(|&c| c < NIC_MAX_CPUS && mask_contains(&self.rx_cpu_mask, c))
    }
}

#[inline]
fn mask_contains(mask: &[u64; MASK_WORDS], cpu: usize) -> bool {
    mask[cpu / 64] & (1u64 << (cpu % 64)) != 0
}

/// Parses a kernel CPU list such as `0-3,8,10-11` into a mask. `None` on any malformed token
/// or CPU id `>= NIC_MAX_CPUS`.
pub fn parse_cpu_list(list: &str) -> Option<[u64; MASK_WORDS]> {
    let mut mask = [0u64; MASK_WORDS];
    let list = list.trim();
    if list.is_empty() {
        return None;
    }
    for part in list.split(',') {
        let (lo, hi) = match part.trim().split_once('-') {
            Some((a, b)) => (a.parse::<usize>().ok()?, b.parse::<usize>().ok()?),
            None => {
                let v = part.trim().parse::<usize>().ok()?;
                (v, v)
            }
        };
        if lo > hi || hi >= NIC_MAX_CPUS {
            return None;
        }
        for cpu in lo..=hi {
            mask[cpu / 64] |= 1u64 << (cpu % 64);
        }
    }
    Some(mask)
}

/// `true` if the RSS indirection table maps flows onto at least two distinct RX rings.
pub fn indirection_spreads(table: &[u32]) -> bool {
    match table.first() {
        Some(&first) => table.iter().any(|&e| e != first),
        None => false,
    }
}

/// `true` if an IRQ action name looks like a per-queue data IRQ (not async/cmd/mgmt/event).
pub fn is_queue_irq_name(name: &str) -> bool {
    const CONTROL: [&str; 11] = [
        "async", "cmd", "event", "mgmt", "mgmn", "pages", "ctrl", "config", "admin", "link", "err",
    ];
    let lower = name.to_ascii_lowercase();
    !CONTROL.iter().any(|c| lower.contains(c))
}

/// `true` if an `rps_cpus` hex mask (e.g. `00,00000000`) enables any CPU.
pub fn rps_enabled(mask: &str) -> bool {
    mask.trim()
        .chars()
        .any(|c| c.is_ascii_hexdigit() && c != '0')
}

/// Probes NIC RSS. Always cheap and infallible: any doubt yields `Software`.
pub fn probe_nic() -> NicProfile {
    if std::env::var("VELDA_INGRESS_HASH").is_ok_and(|v| v.eq_ignore_ascii_case("software")) {
        return NicProfile::software("forced by VELDA_INGRESS_HASH=software");
    }
    #[cfg(target_os = "linux")]
    {
        probe_linux()
    }
    #[cfg(not(target_os = "linux"))]
    {
        NicProfile::software("non-linux host")
    }
}

#[cfg(target_os = "linux")]
fn read_trim(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
}

/// Reads the RSS indirection table via `ETHTOOL_GRSSH` (unprivileged, read-only).
#[cfg(target_os = "linux")]
fn read_rss_indirection(iface: &str) -> Option<Vec<u32>> {
    #[repr(C)]
    struct IfReq {
        name: [libc::c_char; 16],
        data: *mut libc::c_void,
        _pad: [u8; 16],
    }
    const SIOCETHTOOL: libc::c_ulong = 0x8946;
    const ETHTOOL_GRSSH: u32 = 0x46;
    // struct ethtool_rxfh header: cmd, rss_context, indir_size, key_size, hfunc+rsvd8, rsvd32.
    const HEADER_WORDS: usize = 6;

    if iface.len() >= 16 {
        return None;
    }
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return None;
    }
    let mut req = IfReq {
        name: [0; 16],
        data: std::ptr::null_mut(),
        _pad: [0; 16],
    };
    for (i, b) in iface.bytes().enumerate() {
        req.name[i] = b as libc::c_char;
    }

    // Call 1: query table/key sizes.
    let mut hdr = [0u32; HEADER_WORDS];
    hdr[0] = ETHTOOL_GRSSH;
    req.data = hdr.as_mut_ptr() as *mut libc::c_void;
    let ok1 = unsafe { libc::ioctl(fd, SIOCETHTOOL, &mut req) } == 0;
    let (indir_size, key_size) = (hdr[2] as usize, hdr[3] as usize);
    if !ok1 || indir_size == 0 || indir_size > 4096 || key_size > 1024 {
        unsafe { libc::close(fd) };
        return None;
    }

    // Call 2: fetch the table.
    let mut buf = vec![0u32; HEADER_WORDS + indir_size + key_size.div_ceil(4)];
    buf[0] = ETHTOOL_GRSSH;
    buf[2] = indir_size as u32;
    buf[3] = key_size as u32;
    req.data = buf.as_mut_ptr() as *mut libc::c_void;
    let ok2 = unsafe { libc::ioctl(fd, SIOCETHTOOL, &mut req) } == 0;
    unsafe { libc::close(fd) };
    if !ok2 {
        return None;
    }
    Some(buf[HEADER_WORDS..HEADER_WORDS + indir_size].to_vec())
}

/// Returns the CPU mask of per-queue IRQs of the PCI/virtual device at `device_path`, and the
/// number of such IRQs, or `None` if the layout cannot be proven.
#[cfg(target_os = "linux")]
fn queue_irq_cpus(device_path: &str) -> Option<([u64; MASK_WORDS], usize)> {
    let mut mask = [0u64; MASK_WORDS];
    let mut count = 0usize;
    if let Ok(entries) = std::fs::read_dir(format!("{device_path}/msi_irqs")) {
        for entry in entries {
            let irq = entry.ok()?.file_name().to_string_lossy().into_owned();
            let Ok(irq_num) = irq.parse::<u32>() else {
                continue;
            };
            let irq_dir = format!("/proc/irq/{irq_num}");
            let is_queue = std::fs::read_dir(&irq_dir)
                .ok()?
                .filter_map(Result::ok)
                .any(|e| {
                    e.file_type().is_ok_and(|t| t.is_dir())
                        && is_queue_irq_name(&e.file_name().to_string_lossy())
                });
            if !is_queue {
                continue;
            }
            // Effective affinity is where the IRQ really fires; smp_affinity may list many CPUs.
            let list = read_trim(&format!("{irq_dir}/effective_affinity_list"))
                .or_else(|| read_trim(&format!("{irq_dir}/smp_affinity_list")))?;
            let cpus = parse_cpu_list(&list)?;
            for (m, c) in mask.iter_mut().zip(cpus) {
                *m |= c;
            }
            count += 1;
        }
        if count > 0 {
            return Some((mask, count));
        }
    }
    // Fallback for Hyper-V / Azure synthetic VMBus channels (`channel_vp_mapping`: `<chan>:<cpu>`)
    if let Some(vp_map) = read_trim(&format!("{device_path}/channel_vp_mapping")) {
        for line in vp_map.lines() {
            if let Some((_, cpu_str)) = line.trim().split_once(':')
                && let Ok(cpu) = cpu_str.trim().parse::<usize>()
                && cpu < NIC_MAX_CPUS
            {
                mask[cpu / 64] |= 1u64 << (cpu % 64);
                count += 1;
            }
        }
    }
    (count > 0).then_some((mask, count))
}

#[cfg(target_os = "linux")]
fn probe_linux() -> NicProfile {
    let Ok(dir) = std::fs::read_dir("/sys/class/net") else {
        return NicProfile::software("cannot read /sys/class/net");
    };

    let mut mask = [0u64; MASK_WORDS];
    let mut verified = 0u8;
    let mut min_queues = u16::MAX;

    for entry in dir.filter_map(Result::ok) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == "lo" {
            continue;
        }
        let base = format!("/sys/class/net/{name}");
        if read_trim(&format!("{base}/carrier")).as_deref() != Some("1") {
            continue; // no link: carries no traffic
        }
        // Enslaved VF (e.g. Azure mlx5 VF under netvsc): packets are delivered via the master,
        // which is verified using the VF's IRQs. The slave itself is not a delivery interface.
        if std::path::Path::new(&format!("{base}/master")).exists()
            && std::path::Path::new(&format!("{base}/device")).exists()
        {
            continue;
        }

        // Virtual interfaces (docker0, virbr0, veth*, tun*) have no hardware /device symlink;
        // skip them so local bridges do not disable RSS on physical NICs. If only virtual
        // interfaces exist (e.g. inside a container), `verified` stays 0 -> Software fallback.
        if !std::path::Path::new(&format!("{base}/device")).exists() {
            continue;
        }

        let rx_dirs: Vec<_> = std::fs::read_dir(format!("{base}/queues"))
            .map(|d| {
                d.filter_map(Result::ok)
                    .filter(|e| e.file_name().to_string_lossy().starts_with("rx-"))
                    .map(|e| e.path())
                    .collect()
            })
            .unwrap_or_default();
        if rx_dirs.len() < 2 {
            return NicProfile::software("active interface has a single RX queue");
        }
        if rx_dirs.iter().any(|q| {
            read_trim(&q.join("rps_cpus").to_string_lossy()).is_none_or(|m| rps_enabled(&m))
        }) {
            return NicProfile::software("RPS enabled or unreadable on an RX queue");
        }

        match read_rss_indirection(&name) {
            Some(table) if indirection_spreads(&table) => {}
            _ => return NicProfile::software("RSS indirection table missing or not spreading"),
        }

        // IRQ source: lower (VF) devices when present (netvsc), otherwise the NIC's own device.
        let lowers: Vec<String> = std::fs::read_dir(&base)
            .map(|d| {
                d.filter_map(Result::ok)
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.starts_with("lower_"))
                    .collect()
            })
            .unwrap_or_default();
        let device_paths: Vec<String> = if lowers.is_empty() {
            vec![format!("{base}/device")]
        } else {
            lowers
                .iter()
                .map(|l| format!("{base}/{l}/device"))
                .collect()
        };

        let mut if_mask = [0u64; MASK_WORDS];
        let mut if_irqs = 0usize;
        for path in &device_paths {
            let Some((m, n)) = queue_irq_cpus(path) else {
                return NicProfile::software("cannot prove RX queue IRQ placement");
            };
            for (a, b) in if_mask.iter_mut().zip(m) {
                *a |= b;
            }
            if_irqs += n;
        }
        let distinct_cpus: u32 = if_mask.iter().map(|w| w.count_ones()).sum();
        if distinct_cpus < 2 || if_irqs < 2 {
            return NicProfile::software("RX IRQs are not spread over multiple CPUs");
        }

        for (a, b) in mask.iter_mut().zip(if_mask) {
            *a |= b;
        }
        min_queues = min_queues.min(rx_dirs.len().min(u16::MAX as usize) as u16);
        verified = verified.saturating_add(1);
    }

    if verified == 0 {
        return NicProfile::software("no active hardware interface");
    }
    NicProfile {
        authority: IngressHashAuthority::HardwareRss,
        verified_interfaces: verified,
        rx_queues: min_queues,
        rx_cpu_mask: mask,
        reason: "hardware RSS verified on all active interfaces",
    }
}

/// Returns the CPU core to bind via `SO_INCOMING_CPU` for `worker_id`, or `None` when the kernel
/// software hash must stay in charge.
///
/// Reads the *real* physical core from the worker registry (never `worker_id % cpus`) and requires
/// hardware RSS to cover every registered traffic core, so no worker can be starved.
pub fn ingress_steering_cpu(worker_id: usize) -> Option<usize> {
    let nic = &super::global_hardware_topology().nic;
    if nic.authority != IngressHashAuthority::HardwareRss {
        return None;
    }
    let workers = crate::worker::global_worker_registry().list();
    let traffic: Vec<_> = workers
        .iter()
        .filter(|w| {
            w.role == crate::worker::WorkerRole::Traffic
                && matches!(
                    w.state,
                    crate::worker::WorkerState::Starting | crate::worker::WorkerState::Running
                )
        })
        .collect();
    let cores: Vec<usize> = traffic.iter().map(|w| w.core_id).collect();
    let mine = traffic.iter().find(|w| w.id.0 as usize == worker_id)?;
    // Every worker needs its own distinct core, otherwise SO_INCOMING_CPU would double-book one.
    let mut sorted = cores.clone();
    sorted.sort_unstable();
    sorted.dedup();
    if sorted.len() != cores.len() || !nic.steers_all_cores(&cores) {
        return None;
    }
    Some(mine.core_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_list_parsing() {
        let m = parse_cpu_list("0-3,8").unwrap();
        assert_eq!(m[0], 0b1_0000_1111);
        assert!(parse_cpu_list("").is_none());
        assert!(parse_cpu_list("3-1").is_none());
        assert!(parse_cpu_list("a").is_none());
        assert!(parse_cpu_list("2000").is_none());
    }

    #[test]
    fn indirection_spread() {
        assert!(indirection_spreads(&[0, 1, 2, 3]));
        assert!(!indirection_spreads(&[0, 0, 0, 0]));
        assert!(!indirection_spreads(&[]));
    }

    #[test]
    fn irq_names() {
        assert!(is_queue_irq_name("mlx5_comp0@pci:045d:00:02.0"));
        assert!(is_queue_irq_name("eth0-Tx-Rx-0"));
        assert!(!is_queue_irq_name("mlx5_async0@pci:045d:00:02.0"));
        assert!(!is_queue_irq_name("ena-mgmnt@pci:0000:00:05.0"));
    }

    #[test]
    fn rps_mask() {
        assert!(!rps_enabled("0"));
        assert!(!rps_enabled("00,00000000"));
        assert!(rps_enabled("00,00000008"));
    }

    #[test]
    fn software_profile_never_steers() {
        let p = NicProfile::software("test");
        assert!(!p.steers_all_cores(&[0]));
    }

    #[test]
    fn steering_requires_full_core_coverage() {
        let mut p = NicProfile::software("t");
        p.authority = IngressHashAuthority::HardwareRss;
        p.rx_cpu_mask[0] = 0b0011; // IRQs on CPU 0,1 only
        assert!(p.steers_all_cores(&[0, 1]));
        assert!(!p.steers_all_cores(&[0, 1, 2, 3]));
        assert!(!p.steers_all_cores(&[]));
    }
}
