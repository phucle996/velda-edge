//! ASN.1 DER & PEM Public Key Extraction Helpers (`jwt_auth::pem`).
//!
//! ```text
//!   PEM Text ──► parse_pem_to_der() ──► Raw DER (SPKI / PKCS#1 / SEC1 / Ed25519)
//!                                         └─► extract_raw_public_key() + extract_rsa_modulus_len()
//! ```

use base64::prelude::*;

use crate::error::PluginError;

/// Parses a PEM-encoded public key into raw DER bytes.
pub fn parse_pem_to_der(pem_str: &str) -> Result<Vec<u8>, PluginError> {
    let mut in_key = false;
    let mut b64_lines = String::new();
    for line in pem_str.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("-----BEGIN") {
            in_key = true;
            continue;
        }
        if trimmed.starts_with("-----END") {
            break;
        }
        if in_key {
            b64_lines.push_str(trimmed);
        }
    }
    if b64_lines.is_empty() {
        if let Ok(der) = BASE64_STANDARD.decode(pem_str.trim()) {
            return Ok(der);
        }
        return Err(PluginError::Config(
            "Public key PEM must contain valid -----BEGIN/END----- banners or raw base64 DER"
                .into(),
        ));
    }
    BASE64_STANDARD
        .decode(&b64_lines)
        .map_err(|e| PluginError::Config(format!("Failed to base64-decode public key DER: {e}")))
}

/// Reads a single ASN.1 TLV (Tag-Length-Value) element from DER bytes.
#[inline]
pub fn read_asn1_tlv(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    if data.len() < 2 {
        return None;
    }
    let tag = data[0];
    let (len, offset) = if data[1] < 0x80 {
        (data[1] as usize, 2)
    } else {
        let len_bytes = (data[1] & 0x7f) as usize;
        if len_bytes == 0 || len_bytes > 4 || data.len() < 2 + len_bytes {
            return None;
        }
        let mut l = 0usize;
        for i in 0..len_bytes {
            l = (l << 8) | (data[2 + i] as usize);
        }
        (l, 2 + len_bytes)
    };

    if data.len() < offset + len {
        return None;
    }
    Some((tag, &data[offset..offset + len], &data[offset + len..]))
}

/// Extracts underlying raw public key bytes from X.509 `SubjectPublicKeyInfo` (SPKI),
/// PKCS#1 `RSAPublicKey`, SEC 1, or raw key structures.
pub fn extract_raw_public_key(der: &[u8], is_rsa: bool) -> Result<Vec<u8>, PluginError> {
    if der.is_empty() {
        return Err(PluginError::Config("Public key DER is empty".into()));
    }
    if !is_rsa && (der.len() == 32 || der[0] == 0x04) {
        return Ok(der.to_vec());
    }
    if der[0] != 0x30 {
        return Err(PluginError::Config(format!(
            "Invalid public key DER header: expected SEQUENCE (0x30), found 0x{:02x}",
            der[0]
        )));
    }

    let (tag, value, _) = read_asn1_tlv(der)
        .ok_or_else(|| PluginError::Config("Malformed ASN.1 SEQUENCE in public key".into()))?;
    if tag != 0x30 {
        return Err(PluginError::Config(
            "Expected SEQUENCE tag in public key".into(),
        ));
    }

    let (first_tag, _, rest) = read_asn1_tlv(value)
        .ok_or_else(|| PluginError::Config("Empty ASN.1 SEQUENCE in public key".into()))?;

    if is_rsa && first_tag == 0x02 {
        return Ok(der.to_vec());
    }

    if first_tag == 0x30 {
        let (second_tag, bitstring_val, _) = read_asn1_tlv(rest).ok_or_else(|| {
            PluginError::Config("Missing SubjectPublicKey BIT STRING in SPKI".into())
        })?;
        if second_tag != 0x03 {
            return Err(PluginError::Config(format!(
                "Expected BIT STRING (0x03) in SPKI, found 0x{:02x}",
                second_tag
            )));
        }
        if bitstring_val.is_empty() {
            return Err(PluginError::Config("Empty BIT STRING in SPKI".into()));
        }
        if bitstring_val[0] != 0 {
            return Err(PluginError::Config(format!(
                "Non-zero unused bits in SPKI BIT STRING: {}",
                bitstring_val[0]
            )));
        }
        return Ok(bitstring_val[1..].to_vec());
    }

    Ok(der.to_vec())
}

/// Extracts the RSA modulus length in bytes from a PKCS#1 `RSAPublicKey` DER sequence.
pub fn extract_rsa_modulus_len(pkcs1_der: &[u8]) -> Option<usize> {
    let (tag, value, _) = read_asn1_tlv(pkcs1_der)?;
    if tag != 0x30 {
        return None;
    }
    let (tag_n, mut val_n, _) = read_asn1_tlv(value)?;
    if tag_n != 0x02 || val_n.is_empty() {
        return None;
    }
    while val_n.len() > 1 && val_n[0] == 0x00 {
        val_n = &val_n[1..];
    }
    Some(val_n.len())
}
