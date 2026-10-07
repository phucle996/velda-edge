//! ASCII Hourglass startup banner for Velda Edge.

use std::io::IsTerminal;
use std::path::Path;
use velda_core::Provenance;
use velda_core::hardware::HardwareTopology;

/// Formats the startup banner with the Hourglass ASCII art on the left
/// and system/architecture metadata on the right.
pub fn format_startup_banner(
    prov: &Provenance,
    hardware: &HardwareTopology,
    worker_threads: usize,
    cpu_pinning: bool,
    runtime_dir: &Path,
    listeners: &[String],
    use_color: bool,
) -> String {
    let pin_str = if cpu_pinning { "Enabled" } else { "Disabled" };
    let runtime_dir_display = runtime_dir.display().to_string();

    let build_type = if prov.build_profile == "release"
        || (!cfg!(debug_assertions) && prov.build_profile != "dev")
    {
        "release"
    } else {
        "dev"
    };

    let ram_gb = hardware.memory.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    let cpu_tier_str = hardware.cpu.tier.as_str();
    let mem_tier_str = hardware.memory.tier.as_str();
    let accel_str = hardware.kernel.acceleration.as_str();
    let kernel_ver_str = hardware.kernel.version.to_string();

    let listeners_display = if listeners.is_empty() {
        "None (Awaiting Dynamic Sync)".to_string()
    } else if listeners.len() <= 2 {
        listeners.join(", ")
    } else {
        format!(
            "{} active ({}, {}, +{} more)",
            listeners.len(),
            listeners[0],
            listeners[1],
            listeners.len() - 2
        )
    };

    if use_color {
        let c_cyan = "\x1b[36m";
        let c_bcyan = "\x1b[1;36m";
        let c_gold = "\x1b[33m";
        let c_bgold = "\x1b[1;33m";
        let c_bmagenta = "\x1b[1;35m";
        let c_white = "\x1b[37m";
        let c_bwhite = "\x1b[1;37m";
        let c_gray = "\x1b[90m";
        let c_green = "\x1b[1;32m";
        let c_reset = "\x1b[0m";

        let left_lines = [
            format!("{c_cyan}  ┌─────────────────┐{c_reset}"),
            format!(
                "{c_cyan}  │{c_reset} \\  {c_bgold}✦{c_reset}   {c_gold}·{c_reset}   {c_bgold}✦{c_reset}  / {c_cyan}│{c_reset}"
            ),
            format!(
                "{c_cyan}  │{c_reset}  \\   {c_bgold}✦{c_reset} {c_gold}·{c_reset} {c_bgold}✦{c_reset}   /  {c_cyan}│{c_reset}"
            ),
            format!("{c_cyan}  │{c_reset}   \\    {c_gold}·{c_reset}    /   {c_cyan}│{c_reset}"),
            format!(
                "{c_cyan}  │{c_reset}    {c_bcyan}╰──>{c_reset}{c_bmagenta}◈{c_reset}{c_bcyan}<──╯{c_reset}    {c_cyan}│{c_reset}"
            ),
            format!(
                "{c_cyan}  │{c_reset}    {c_bcyan}╭──<{c_reset}{c_bmagenta}◈{c_reset}{c_bcyan}>──╮{c_reset}    {c_cyan}│{c_reset}"
            ),
            format!("{c_cyan}  │{c_reset}   /    {c_bgold}:{c_reset}    \\   {c_cyan}│{c_reset}"),
            format!(
                "{c_cyan}  │{c_reset}  /   {c_bgold}✦{c_reset} {c_bgold}:{c_reset} {c_bgold}✦{c_reset}   \\  {c_cyan}│{c_reset}"
            ),
            format!(
                "{c_cyan}  │{c_reset} /  {c_bgold}✦{c_reset} {c_gold}·{c_reset} {c_bgold}:{c_reset} {c_gold}·{c_reset} {c_bgold}✦{c_reset}  \\ {c_cyan}│{c_reset}"
            ),
            format!("{c_cyan}  └─────────────────┘{c_reset}"),
        ];

        let right_lines = [
            format!(
                "{c_bwhite}VELDA EDGE{c_reset} {c_gray}—{c_reset} {c_bcyan}High-Performance Edge Engine{c_reset}"
            ),
            format!(
                "{c_gray}GitHub       :{c_reset} {c_cyan}{}{c_reset}",
                prov.repository
            ),
            format!(
                "{c_gray}Version      :{c_reset} {c_green}v{}-{}{c_reset} {c_gray}({}){c_reset}",
                prov.version, build_type, prov.git_hash
            ),
            format!(
                "{c_gray}Platforms    :{c_reset} {c_white}Linux x86_64, aarch64 (ARM64), armv7 (ARM32){c_reset}"
            ),
            format!(
                "{c_gray}CPU Detect   :{c_reset} {c_white}{} Cores{c_reset} {c_gray}=>{c_reset} {c_bcyan}Tier: {}{c_reset} {c_gray}({} Workers, Pinning: {}){c_reset}",
                hardware.available_cores, cpu_tier_str, worker_threads, pin_str
            ),
            format!(
                "{c_gray}RAM Detect   :{c_reset} {c_white}{:.1} GB{c_reset} {c_gray}=>{c_reset} {c_bcyan}Tier: {}{c_reset}",
                ram_gb, mem_tier_str
            ),
            format!(
                "{c_gray}Kernel       :{c_reset} {c_white}Linux {}{c_reset} {c_gray}=>{c_reset} {c_bcyan}Accel: {}{c_reset}",
                kernel_ver_str, accel_str
            ),
            format!(
                "{c_gray}Runtime Dir  :{c_reset} {c_white}{}{c_reset}",
                runtime_dir_display
            ),
            format!(
                "{c_gray}Listeners    :{c_reset} {c_green}{}{c_reset}",
                listeners_display
            ),
            format!("{c_gray}Data Plane   :{c_reset} {c_green}Initializing Supervisor...{c_reset}"),
        ];

        let mut out = String::new();
        out.push('\n');
        for (left, right) in left_lines.iter().zip(right_lines.iter()) {
            out.push_str(left);
            out.push_str("   ");
            out.push_str(right);
            out.push('\n');
        }
        out
    } else {
        let left_lines = [
            "  ┌─────────────────┐",
            "  │ \\  ✦   ·   ✦  / │",
            "  │  \\   ✦ · ✦   /  │",
            "  │   \\    ·    /   │",
            "  │    ╰──>◈<──╯    │",
            "  │    ╭──<◈>──╮    │",
            "  │   /    :    \\   │",
            "  │  /   ✦ : ✦   \\  │",
            "  │ /  ✦ · : · ✦  \\ │",
            "  └─────────────────┘",
        ];

        let right_lines = [
            "VELDA EDGE — High-Performance Edge Engine".to_string(),
            format!("GitHub       : {}", prov.repository),
            format!(
                "Version      : v{}-{} ({})",
                prov.version, build_type, prov.git_hash
            ),
            "Platforms    : Linux x86_64, aarch64 (ARM64), armv7 (ARM32)".to_string(),
            format!(
                "CPU Detect   : {} Cores => Tier: {} ({} Workers, Pinning: {})",
                hardware.available_cores, cpu_tier_str, worker_threads, pin_str
            ),
            format!("RAM Detect   : {:.1} GB => Tier: {}", ram_gb, mem_tier_str),
            format!(
                "Kernel       : Linux {} => Accel: {}",
                kernel_ver_str, accel_str
            ),
            format!("Runtime Dir  : {runtime_dir_display}"),
            format!("Listeners    : {listeners_display}"),
            "Data Plane   : Initializing Supervisor...".to_string(),
        ];

        let mut out = String::new();
        out.push('\n');
        for (left, right) in left_lines.iter().zip(right_lines.iter()) {
            out.push_str(left);
            out.push_str("   ");
            out.push_str(right);
            out.push('\n');
        }
        out
    }
}

/// Print the startup banner directly to stdout.
pub fn print_startup_banner(
    prov: &Provenance,
    hardware: &HardwareTopology,
    worker_threads: usize,
    cpu_pinning: bool,
    runtime_dir: &Path,
    listeners: &[String],
) {
    let use_color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    let banner = format_startup_banner(
        prov,
        hardware,
        worker_threads,
        cpu_pinning,
        runtime_dir,
        listeners,
        use_color,
    );
    println!("{banner}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_startup_banner_format() {
        let prov = Provenance::current();
        let hardware = HardwareTopology::probe();
        let path = PathBuf::from("/etc/velda");
        let listeners = vec![
            "0.0.0.0:80 (HTTP1)".to_string(),
            "0.0.0.0:443 (HTTP2+TLS)".to_string(),
        ];

        let plain = format_startup_banner(&prov, &hardware, 8, true, &path, &listeners, false);
        assert!(plain.contains("VELDA EDGE — High-Performance Edge Engine"));
        assert!(plain.contains("GitHub       :"));
        assert!(plain.contains("CPU Detect   :"));
        assert!(plain.contains("RAM Detect   :"));
        assert!(plain.contains("Kernel       :"));
        assert!(plain.contains("Listeners    :"));
        assert!(plain.contains("0.0.0.0:80 (HTTP1)"));
        assert!(plain.contains("┌─────────────────┐"));
        assert!(plain.contains("◈"));
        assert!(plain.contains("└─────────────────┘"));
        assert!(!plain.contains("Architecture :"));
        assert!(!plain.contains("Hot-Path     :"));

        let colored = format_startup_banner(&prov, &hardware, 8, true, &path, &listeners, true);
        assert!(colored.contains("VELDA EDGE"));
        assert!(colored.contains("\x1b["));
    }
}
