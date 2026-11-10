//! Certificate and Account Key Storage Management.
//!
//! Owns filesystem persistence for:
//! - Account private key: `<storage_dir>/account.key`
//! - Domain certificate chains and private keys: `<storage_dir>/<primary_domain>/fullchain.pem`
//!   and `<storage_dir>/<primary_domain>/privkey.pem`.
//!
//! Guarantees atomic filesystem writes (via temporary files and atomic rename) so restarts
//! never encounter corrupted or partially written certificate files.

use std::fs;
use std::path::{Path, PathBuf};

use super::crypto::AccountKey;
use crate::error::PluginError;

/// Filesystem storage manager for ACME certificates and cryptographic materials.
pub struct CertStorage {
    storage_dir: PathBuf,
}

impl CertStorage {
    /// Creates a new storage manager rooted at the given directory, ensuring it exists.
    pub fn new(storage_dir: impl AsRef<Path>) -> Result<Self, PluginError> {
        let dir = storage_dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to create ACME storage directory '{}': {e}",
                    dir.display()
                ),
            ))
        })?;
        Ok(Self { storage_dir: dir })
    }

    /// Loads the existing account key if present, or generates a new one and persists it.
    pub fn load_or_generate_account_key(&self) -> Result<AccountKey, PluginError> {
        let key_path = self.storage_dir.join("account.key");
        if key_path.exists() {
            let pem = fs::read_to_string(&key_path).map_err(|e| {
                PluginError::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to read account key from '{}': {e}",
                        key_path.display()
                    ),
                ))
            })?;
            AccountKey::from_pem(&pem)
        } else {
            let key = AccountKey::generate()?;
            self.atomic_write(&key_path, key.to_pem().as_bytes())?;
            Ok(key)
        }
    }

    /// Reads the existing fullchain PEM certificate for the given primary domain, if present.
    pub fn read_fullchain_pem(&self, primary_domain: &str) -> Option<Vec<u8>> {
        let cert_path = self.storage_dir.join(primary_domain).join("fullchain.pem");
        fs::read(cert_path).ok()
    }

    /// Reads the existing private key PEM for the given primary domain, if present.
    pub fn read_privkey_pem(&self, primary_domain: &str) -> Option<String> {
        let key_path = self.storage_dir.join(primary_domain).join("privkey.pem");
        fs::read_to_string(key_path).ok()
    }

    /// Atomically persists a newly issued certificate fullchain and private key.
    pub fn save_certificate(
        &self,
        primary_domain: &str,
        fullchain_pem: &[u8],
        privkey_pem: &str,
    ) -> Result<(), PluginError> {
        let domain_dir = self.storage_dir.join(primary_domain);
        fs::create_dir_all(&domain_dir).map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to create domain storage directory '{}': {e}",
                    domain_dir.display()
                ),
            ))
        })?;

        let cert_path = domain_dir.join("fullchain.pem");
        let key_path = domain_dir.join("privkey.pem");

        self.atomic_write(&cert_path, fullchain_pem)?;
        self.atomic_write(&key_path, privkey_pem.as_bytes())?;

        tracing::info!(
            domain = %primary_domain,
            cert_path = %cert_path.display(),
            key_path = %key_path.display(),
            "Successfully persisted renewed ACME certificate and private key to disk"
        );

        Ok(())
    }

    /// Writes data to a temporary file in the same directory and atomically renames it.
    fn atomic_write(&self, target_path: &Path, data: &[u8]) -> Result<(), PluginError> {
        let parent = target_path.parent().unwrap_or(&self.storage_dir);
        let tmp_path = parent.join(format!(
            ".tmp_{}_{}",
            std::process::id(),
            target_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file")
        ));

        fs::write(&tmp_path, data).map_err(|e| {
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to write temporary file '{}': {e}",
                    tmp_path.display()
                ),
            ))
        })?;

        fs::rename(&tmp_path, target_path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            PluginError::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to atomically rename '{}' to '{}': {e}",
                    tmp_path.display(),
                    target_path.display()
                ),
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_lifecycle() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let storage = CertStorage::new(tmp_dir.path()).unwrap();

        // 1. Initial generation of account key
        let key1 = storage.load_or_generate_account_key().unwrap();

        // 2. Subsequent load reuses existing account key
        let key2 = storage.load_or_generate_account_key().unwrap();
        assert_eq!(key1.thumbprint(), key2.thumbprint());

        // 3. Save certificate and read back
        let domain = "api.example.com";
        assert!(storage.read_fullchain_pem(domain).is_none());

        let fake_cert = b"-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n";
        let fake_key = "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----\n";

        storage
            .save_certificate(domain, fake_cert, fake_key)
            .unwrap();

        let loaded_cert = storage.read_fullchain_pem(domain).unwrap();
        assert_eq!(loaded_cert, fake_cert);

        let loaded_key = storage.read_privkey_pem(domain).unwrap();
        assert_eq!(loaded_key, fake_key);
    }
}
