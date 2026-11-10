//! Certificate lifetime inspection used by the bootstrap and cron renewal decision.

use crate::error::PluginError;

/// Returns the `notAfter` of the first certificate in a PEM chain as Unix seconds.
pub fn not_after_unix(fullchain_pem: &[u8]) -> Result<i64, PluginError> {
    let (_, pem) = x509_parser::pem::parse_x509_pem(fullchain_pem)
        .map_err(|e| PluginError::Config(format!("invalid certificate PEM: {e}")))?;
    let cert = pem
        .parse_x509()
        .map_err(|e| PluginError::Config(format!("invalid X.509 certificate: {e}")))?;
    Ok(cert.validity().not_after.timestamp())
}

/// Returns `true` when the certificate expires within `renew_before_days` of `now_unix`.
pub fn needs_renewal(not_after: i64, now_unix: i64, renew_before_days: u32) -> bool {
    not_after - now_unix <= i64::from(renew_before_days) * 86_400
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renewal_window() {
        let day = 86_400;
        assert!(needs_renewal(100 * day + 29 * day, 100 * day, 30));
        assert!(needs_renewal(100 * day + 30 * day, 100 * day, 30));
        assert!(!needs_renewal(100 * day + 31 * day, 100 * day, 30));
        assert!(needs_renewal(50 * day, 100 * day, 30)); // already expired
    }

    #[test]
    fn parses_generated_cert() {
        let ck = rcgen::generate_simple_self_signed(vec!["a.example.com".into()]).unwrap();
        let na = not_after_unix(ck.cert.pem().as_bytes()).unwrap();
        assert!(na > 0);
    }

    #[test]
    fn rejects_garbage() {
        assert!(not_after_unix(b"not a pem").is_err());
    }
}
