//! AWS Secrets Manager External Third-Party (`velda-etp::aws_secrets_manager`) pool.
//!
//! ```text
//!   EtpEntryConfig (kind == EtpKind::AwsSecretsManager)
//!         └──► AwsSecretsManagerEtpConfig ──► AwsSecretsManagerEtp (StreamPool)
//! ```

pub mod config;
pub mod pool;

pub use config::AwsSecretsManagerEtpConfig;
pub use pool::AwsSecretsManagerEtp;
