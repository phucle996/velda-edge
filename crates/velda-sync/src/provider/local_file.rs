//! Pure Local Filesystem Configuration Provider.
//!
//! Provides raw filesystem byte read capabilities from a designated configuration directory.
//! Free of business pipeline logic and schema decoding.

use std::fs;
use std::path::PathBuf;

use crate::SyncError;

/// Local filesystem configuration provider.
#[derive(Debug, Clone)]
pub struct LocalFileProvider {
    config_dir: PathBuf,
}

impl LocalFileProvider {
    /// Creates a new provider pointing to the specified directory.
    pub fn new(config_dir: impl Into<PathBuf>) -> Self {
        Self {
            config_dir: config_dir.into(),
        }
    }

    /// Reads raw bytes of a file relative to the configuration directory.
    pub async fn read_file(&self, relative_path: &str) -> Result<Vec<u8>, SyncError> {
        let file_path = self.config_dir.join(relative_path);
        if !file_path.exists() {
            return Err(SyncError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Configuration file not found: {}", file_path.display()),
            )));
        }

        let bytes = fs::read(&file_path)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_file_provider_reads_raw_bytes() {
        let tmp = tempdir().unwrap();
        let file_path = tmp.path().join("test.txt");
        fs::write(&file_path, b"hello pure provider").unwrap();

        let provider = LocalFileProvider::new(tmp.path());
        let content = provider.read_file("test.txt").await.unwrap();
        assert_eq!(content, b"hello pure provider");
    }

    #[tokio::test]
    async fn test_local_file_provider_missing_file_errors() {
        let tmp = tempdir().unwrap();
        let provider = LocalFileProvider::new(tmp.path());
        let res = provider.read_file("nonexistent.json").await;
        assert!(res.is_err());
    }
}
