use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=.git/HEAD");

    let git_hash = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "clean-release".to_string());

    let rustc_version = Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "rustc-unknown".to_string());

    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown-target".to_string());
    let profile = std::env::var("PROFILE").unwrap_or_else(|_| "release".to_string());

    println!("cargo:rustc-env=VELDA_GIT_HASH={git_hash}");
    println!("cargo:rustc-env=VELDA_RUSTC_VERSION={rustc_version}");
    println!("cargo:rustc-env=VELDA_TARGET_TRIPLE={target}");
    println!("cargo:rustc-env=VELDA_BUILD_PROFILE={profile}");
}
