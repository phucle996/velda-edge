//! Provenance and compile-time metadata for Velda Edge.
//!
//! Provides verifiable identity, build hashes, authorship, and tamper-resistant
//! watermarks embedded directly into `.rodata` binaries.

/// Raw compile-time watermark embedded in the ELF `.rodata` section.
///
/// Marked `#[used]` to prevent dead-code elimination and linker stripping (`strip = "symbols"`).
/// This constant can be recovered from any stripped binary using `strings <bin> | grep VELDA`.
#[used]
pub static VELDA_PROVENANCE_WATERMARK: &str = concat!(
    "\n@@@ VELDA PROVENANCE WATERMARK @@@\n",
    "PROJECT: Velda Edge High-Performance Edge Traffic Engine\n",
    "AUTHOR: phucle996 and Velda Edge Contributors\n",
    "REPO: https://github.com/phucle996/velda-edge\n",
    "LICENSE: Apache-2.0\n",
    "SIGNATURE: 0x564C4441 (VLDA)\n",
    "VERSION: ",
    env!("CARGO_PKG_VERSION"),
    "\n",
    "GIT_HASH: ",
    env!("VELDA_GIT_HASH"),
    "\n",
    "RUSTC: ",
    env!("VELDA_RUSTC_VERSION"),
    "\n",
    "TARGET: ",
    env!("VELDA_TARGET_TRIPLE"),
    "\n",
    "PROFILE: ",
    env!("VELDA_BUILD_PROFILE"),
    "\n",
    "@@@ END VELDA PROVENANCE WATERMARK @@@\n"
);

/// 4-byte magic signature for binary identification (`VLDA`).
pub const VELDA_MAGIC_SIGNATURE: [u8; 4] = [0x56, 0x4C, 0x44, 0x41];

/// Canonical provenance metadata describing the binary build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Provenance {
    pub name: &'static str,
    pub version: &'static str,
    pub author: &'static str,
    pub repository: &'static str,
    pub license: &'static str,
    pub git_hash: &'static str,
    pub rustc_version: &'static str,
    pub target_triple: &'static str,
    pub build_profile: &'static str,
}

impl Provenance {
    /// Return the canonical compile-time provenance of this build.
    pub const fn current() -> Self {
        Self {
            name: "Velda Edge",
            version: env!("CARGO_PKG_VERSION"),
            author: "phucle996 and Velda Edge Contributors",
            repository: "https://github.com/phucle996/velda-edge",
            license: "Apache-2.0",
            git_hash: env!("VELDA_GIT_HASH"),
            rustc_version: env!("VELDA_RUSTC_VERSION"),
            target_triple: env!("VELDA_TARGET_TRIPLE"),
            build_profile: env!("VELDA_BUILD_PROFILE"),
        }
    }

    /// Return a single-line version identifier.
    pub fn short_version(&self) -> String {
        format!("{}-v{} ({})", self.name, self.version, self.git_hash)
    }

    /// Return a human-readable banner string for CLI `--version`.
    pub fn banner(&self, component: &str) -> String {
        format!(
            "{component} v{version} ({git_hash})\n\
             Author:     {author}\n\
             Repository: {repo}\n\
             License:    {license}\n\
             Target:     {target} ({profile})\n\
             Compiler:   {rustc}",
            component = component,
            version = self.version,
            git_hash = self.git_hash,
            author = self.author,
            repo = self.repository,
            license = self.license,
            target = self.target_triple,
            profile = self.build_profile,
            rustc = self.rustc_version,
        )
    }
}

/// Ensure the linker retains the embedded watermark in `.rodata`.
#[inline(always)]
pub fn watermark() -> &'static str {
    VELDA_PROVENANCE_WATERMARK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provenance_metadata() {
        let prov = Provenance::current();
        assert_eq!(prov.name, "Velda Edge");
        assert_eq!(prov.license, "Apache-2.0");
        assert!(!prov.version.is_empty());
        assert!(!prov.git_hash.is_empty());

        let banner = prov.banner("velda-edge");
        assert!(banner.contains("velda-edge"));
        assert!(banner.contains("phucle996"));
        assert!(banner.contains("Apache-2.0"));

        let wm = watermark();
        assert!(wm.contains("VELDA PROVENANCE WATERMARK"));
        assert!(wm.contains("0x564C4441"));
    }
}
