//! Self-Contained HashiCorp Vault / OpenBao Key Fetcher (`jwt_auth::providers::vault`).
//!
//! ```text
//!   VaultEtp::resolve_token() + acquire() ──► StreamLease
//!         ├─► GET {path} (X-Vault-Token + optional X-Vault-Namespace)
//!         ├─► Read HTTP/1.1 response (Content-Length / chunked / EOF)
//!         ├─► HTTP 403 -> vault.invalidate_token() & retry once
//!         └─► Extract (kid, alg, VerifierKey) from JWKS / Transit / KV v2 / KV v1
//! ```

use std::fmt::Write as _;
use std::str::FromStr;
use std::time::Duration;

use base64::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velda_etp::vault::VaultEtp;

use super::super::crypto::{JwtAlgorithm, VerifierKey};
use super::super::store::{KeyEntrySpec, MAX_JWKS_SLOTS};
use crate::error::PluginError;

const DEFAULT_PATH: &str = "/v1/identity/oidc/.well-known/keys";
const MAX_RESP_BYTES: usize = 1024 * 1024;

/// Executes the complete Vault fetch + parse flow end-to-end over a borrowed [`VaultEtp`] lease.
pub async fn fetch_keys(
    vault: &VaultEtp,
    path_override: Option<&str>,
    default_alg: Option<JwtAlgorithm>,
    timeout: Duration,
) -> Result<Vec<KeyEntrySpec>, PluginError> {
    let raw_path = path_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_PATH);
    let norm_path = if raw_path.starts_with('/') {
        raw_path.to_string()
    } else {
        format!("/{raw_path}")
    };

    for attempt in 0..2 {
        let token = vault.resolve_token().await.map_err(|e| {
            PluginError::Config(format!("Vault ETP '{}' auth error: {e}", vault.config.id))
        })?;

        let mut req = format!(
            "GET {norm_path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: velda-edge/jwt-auth\r\nAccept: application/json\r\nConnection: close\r\n",
            vault.config.host_header
        );
        if let Some(ns) = vault.config.namespace.as_deref() {
            let _ = write!(req, "X-Vault-Namespace: {ns}\r\n");
        }
        if let Some(tok) = token {
            let _ = write!(req, "X-Vault-Token: {tok}\r\n");
        }
        req.push_str("\r\n");

        let mut lease = vault.acquire().await.map_err(|e| {
            PluginError::Config(format!(
                "Vault ETP '{}' acquire error: {e}",
                vault.config.id
            ))
        })?;
        let resp = send_and_read_http(&mut *lease, req.as_bytes(), timeout, &vault.config.id).await;
        lease.recreate_in_background();
        let (status, body) = resp?;

        if status == 403 && attempt == 0 {
            vault.invalidate_token().await;
            continue;
        }
        if !(200..300).contains(&status) {
            return Err(PluginError::Config(format!(
                "Vault ETP '{}' GET {norm_path} returned HTTP {status}",
                vault.config.id
            )));
        }
        return decode_vault_json(&body, default_alg, &vault.config.id);
    }

    Err(PluginError::Config(format!(
        "Vault ETP '{}' authentication failed after retry",
        vault.config.id
    )))
}

async fn send_and_read_http<S>(
    stream: &mut S,
    req: &[u8],
    timeout: Duration,
    label: &str,
) -> Result<(u16, Vec<u8>), PluginError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    tokio::time::timeout(timeout, stream.write_all(req))
        .await
        .map_err(|_| PluginError::Config(format!("Vault '{label}': write timeout")))?
        .map_err(PluginError::Io)?;
    stream.flush().await.map_err(PluginError::Io)?;

    let mut raw = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(timeout, stream.read(&mut chunk))
            .await
            .map_err(|_| PluginError::Config(format!("Vault '{label}': read timeout")))?
            .map_err(PluginError::Io)?;
        if n == 0 {
            return parse_complete_or_eof(&raw, true, label).and_then(|o| {
                o.ok_or_else(|| PluginError::Config(format!("Vault '{label}': incomplete HTTP")))
            });
        }
        raw.extend_from_slice(&chunk[..n]);
        if raw.len() > MAX_RESP_BYTES {
            return Err(PluginError::Config(format!(
                "Vault '{label}': response exceeded 1 MiB"
            )));
        }
        if let Some(done) = parse_complete_or_eof(&raw, false, label)? {
            return Ok(done);
        }
    }
}

fn parse_complete_or_eof(
    raw: &[u8],
    at_eof: bool,
    label: &str,
) -> Result<Option<(u16, Vec<u8>)>, PluginError> {
    let mut headers = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Response::new(&mut headers);
    let offset = match parsed.parse(raw) {
        Ok(httparse::Status::Complete(off)) => off,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(e) => {
            return Err(PluginError::Config(format!(
                "Vault '{label}': HTTP parse error: {e}"
            )));
        }
    };
    let status = parsed.code.unwrap_or(0);
    let mut is_chunked = false;
    let mut content_len = None;
    for h in parsed.headers.iter() {
        if h.name.eq_ignore_ascii_case("transfer-encoding")
            && std::str::from_utf8(h.value).is_ok_and(|v| {
                v.split(',')
                    .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
            })
        {
            is_chunked = true;
        } else if h.name.eq_ignore_ascii_case("content-length")
            && let Some(len) = std::str::from_utf8(h.value)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok())
        {
            content_len = Some(len);
        }
    }
    let body = &raw[offset..];
    if is_chunked {
        let mut out = Vec::new();
        let mut cur = body;
        loop {
            let Some(crlf) = cur.windows(2).position(|w| w == b"\r\n") else {
                return Ok(None);
            };
            let line = std::str::from_utf8(&cur[..crlf])
                .map_err(|_| PluginError::Config(format!("Vault '{label}': bad chunk header")))?;
            let hex_len = line.split(';').next().unwrap_or("").trim();
            let sz = usize::from_str_radix(hex_len, 16)
                .map_err(|_| PluginError::Config(format!("Vault '{label}': bad chunk size")))?;
            cur = &cur[crlf + 2..];
            if sz == 0 {
                if cur == b"\r\n" || cur.windows(4).any(|w| w == b"\r\n\r\n") || at_eof {
                    return Ok(Some((status, out)));
                }
                return Ok(None);
            }
            if cur.len() < sz + 2 {
                return Ok(None);
            }
            out.extend_from_slice(&cur[..sz]);
            cur = &cur[sz + 2..];
        }
    }
    if let Some(len) = content_len {
        return Ok((body.len() >= len).then(|| (status, body[..len].to_vec())));
    }
    Ok(at_eof.then(|| (status, body.to_vec())))
}

fn decode_vault_json(
    body: &[u8],
    default_alg: Option<JwtAlgorithm>,
    label: &str,
) -> Result<Vec<KeyEntrySpec>, PluginError> {
    let root: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| PluginError::Config(format!("Vault '{label}': JSON error: {e}")))?;

    if root.get("keys").and_then(|v| v.as_array()).is_some() {
        return parse_key_payload(&root, default_alg, label);
    }

    let Some(data) = root.get("data") else {
        return parse_key_payload(&root, default_alg, label);
    };

    // Vault Transit (`/v1/transit/keys/{name}`)
    if let Some(transit_type) = data.get("type").and_then(|v| v.as_str())
        && let Some(keys_map) = data.get("keys").and_then(|v| v.as_object())
    {
        let alg = match transit_type.to_ascii_lowercase().as_str() {
            "ed25519" => JwtAlgorithm::EdDSA,
            "ecdsa-p256" => JwtAlgorithm::ES256,
            "ecdsa-p384" => JwtAlgorithm::ES384,
            t if t.starts_with("rsa-") => default_alg
                .filter(|a| a.is_rsa())
                .unwrap_or(JwtAlgorithm::RS256),
            other => JwtAlgorithm::from_str(other)?,
        };
        let mut versions: Vec<(u64, &str, &str)> = keys_map
            .iter()
            .filter_map(|(ver, item)| {
                let pem = item.get("public_key")?.as_str()?;
                let kid = item
                    .get("kid")
                    .and_then(|v| v.as_str())
                    .unwrap_or(ver.as_str());
                Some((ver.parse::<u64>().unwrap_or(0), kid, pem))
            })
            .collect();
        versions.sort_by_key(|a| std::cmp::Reverse(a.0));
        let mut out = Vec::with_capacity(versions.len().min(MAX_JWKS_SLOTS));
        for (_, kid, pem) in versions.into_iter().take(MAX_JWKS_SLOTS) {
            out.push(KeyEntrySpec {
                raw_kid: Some(kid.to_string().into_boxed_str()),
                alg,
                key: VerifierKey::new(alg, None, Some(pem))?,
            });
        }
        return Ok(out);
    }

    // Vault KV v2 (`.data.data`) vs KV v1 (`.data`)
    if let Some(inner) = data.get("data").filter(|v| v.is_object()) {
        return parse_key_payload(inner, default_alg, label);
    }
    parse_key_payload(data, default_alg, label)
}

fn parse_key_payload(
    val: &serde_json::Value,
    default_alg: Option<JwtAlgorithm>,
    label: &str,
) -> Result<Vec<KeyEntrySpec>, PluginError> {
    if let Some(arr) = val.get("keys").and_then(|v| v.as_array()) {
        let mut out = Vec::with_capacity(arr.len().min(MAX_JWKS_SLOTS));
        for item in arr {
            if out.len() >= MAX_JWKS_SLOTS {
                break;
            }
            match parse_jwk_entry(item, default_alg) {
                Ok((kid, alg, key)) => out.push(KeyEntrySpec {
                    raw_kid: kid.map(String::into_boxed_str),
                    alg,
                    key,
                }),
                Err(err) => tracing::warn!(provider = %label, error = %err, "Skipping JWK entry"),
            }
        }
        return Ok(out);
    }

    let alg = if let Some(a) = val.get("alg").and_then(|v| v.as_str()) {
        JwtAlgorithm::from_str(a)?
    } else if let Some(d) = default_alg {
        d
    } else {
        return Err(PluginError::Config(format!(
            "Vault '{label}': secret payload requires explicit 'alg'"
        )));
    };
    let kid = val
        .get("kid")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string().into_boxed_str());
    let secret = val.get("secret_key").and_then(|v| v.as_str());
    let pem = val
        .get("public_key_pem")
        .or_else(|| val.get("public_key"))
        .and_then(|v| v.as_str());
    Ok(vec![KeyEntrySpec {
        raw_kid: kid,
        alg,
        key: VerifierKey::new(alg, secret, pem)?,
    }])
}

fn parse_jwk_entry(
    val: &serde_json::Value,
    default_alg: Option<JwtAlgorithm>,
) -> Result<(Option<String>, JwtAlgorithm, VerifierKey), PluginError> {
    if let Some(u) = val.get("use").and_then(|v| v.as_str())
        && u != "sig"
    {
        return Err(PluginError::Config(format!("Unsupported JWK use '{u}'")));
    }
    let kid = val
        .get("kid")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let kty = val
        .get("kty")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PluginError::Config("JWK missing kty".into()))?;
    let declared_alg = val
        .get("alg")
        .and_then(|v| v.as_str())
        .map(JwtAlgorithm::from_str)
        .transpose()?;

    match kty.to_uppercase().as_str() {
        "RSA" => {
            let n_raw = b64u(
                val.get("n")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| PluginError::Config("RSA JWK missing 'n'".into()))?,
            )?;
            let e_raw = b64u(
                val.get("e")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| PluginError::Config("RSA JWK missing 'e'".into()))?,
            )?;
            let n = trim_zeros(&n_raw);
            let e = trim_zeros(&e_raw);
            if n.len() < 256 {
                return Err(PluginError::Config("RSA key < 2048 bits".into()));
            }
            let alg = declared_alg.unwrap_or_else(|| {
                default_alg
                    .filter(|a| a.is_rsa())
                    .unwrap_or(JwtAlgorithm::RS256)
            });
            if !alg.is_rsa() {
                return Err(PluginError::Config(
                    "RSA key mismatches declared alg".into(),
                ));
            }
            Ok((
                kid,
                alg,
                VerifierKey::Rsa {
                    public_key_der: pkcs1_der(n, e),
                    expected_sig_len: n.len(),
                },
            ))
        }
        "EC" => {
            let crv = val.get("crv").and_then(|v| v.as_str()).unwrap_or("P-256");
            let (alg, clen, slen) = match crv {
                "P-256" => (JwtAlgorithm::ES256, 32, 64),
                "P-384" => (JwtAlgorithm::ES384, 48, 96),
                o => return Err(PluginError::Config(format!("Unsupported EC curve '{o}'"))),
            };
            if let Some(a) = declared_alg
                && a != alg
            {
                return Err(PluginError::Config("EC curve mismatches alg".into()));
            }
            let x = b64u(
                val.get("x")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| PluginError::Config("EC missing 'x'".into()))?,
            )?;
            let y = b64u(
                val.get("y")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| PluginError::Config("EC missing 'y'".into()))?,
            )?;
            if x.len() != clen || y.len() != clen {
                return Err(PluginError::Config("EC coord length mismatch".into()));
            }
            let mut pt = Vec::with_capacity(1 + clen * 2);
            pt.push(0x04);
            pt.extend_from_slice(&x);
            pt.extend_from_slice(&y);
            Ok((
                kid,
                alg,
                VerifierKey::Ecdsa {
                    public_key_raw: pt,
                    expected_sig_len: slen,
                },
            ))
        }
        "OKP" => {
            if val.get("crv").and_then(|v| v.as_str()) != Some("Ed25519") {
                return Err(PluginError::Config("Unsupported OKP curve".into()));
            }
            if let Some(a) = declared_alg
                && a != JwtAlgorithm::EdDSA
            {
                return Err(PluginError::Config("OKP Ed25519 mismatches alg".into()));
            }
            let raw: [u8; 32] = b64u(
                val.get("x")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| PluginError::Config("OKP missing 'x'".into()))?,
            )?
            .try_into()
            .map_err(|_| PluginError::Config("Ed25519 'x' must be 32B".into()))?;
            Ok((
                kid,
                JwtAlgorithm::EdDSA,
                VerifierKey::Ed25519 {
                    public_key_raw: raw,
                },
            ))
        }
        "OCT" => {
            let raw = b64u(
                val.get("k")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| PluginError::Config("oct missing 'k'".into()))?,
            )?;
            let alg = declared_alg.unwrap_or_else(|| {
                default_alg
                    .filter(|a| a.is_symmetric())
                    .unwrap_or(JwtAlgorithm::HS256)
            });
            let key = VerifierKey::from_hmac_bytes(alg, &raw)?;
            Ok((kid, alg, key))
        }
        o => Err(PluginError::Config(format!("Unsupported kty '{o}'"))),
    }
}

fn b64u(s: &str) -> Result<Vec<u8>, PluginError> {
    BASE64_URL_SAFE_NO_PAD
        .decode(s.trim().trim_end_matches('='))
        .map_err(|e| PluginError::Config(format!("Base64URL error: {e}")))
}

fn trim_zeros(mut b: &[u8]) -> &[u8] {
    while b.len() > 1 && b[0] == 0 {
        b = &b[1..];
    }
    b
}

fn der_len(len: usize, out: &mut Vec<u8>) {
    if len < 128 {
        out.push(len as u8);
    } else if len < 256 {
        out.extend_from_slice(&[0x81, len as u8]);
    } else {
        out.extend_from_slice(&[0x82, (len >> 8) as u8, (len & 0xff) as u8]);
    }
}

fn pkcs1_der(n: &[u8], e: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(n.len() + e.len() + 16);
    for raw in [n, e] {
        let trimmed = trim_zeros(raw);
        let pad = !trimmed.is_empty() && (trimmed[0] & 0x80) != 0;
        body.push(0x02);
        der_len(trimmed.len() + usize::from(pad), &mut body);
        if pad {
            body.push(0x00);
        }
        body.extend_from_slice(trimmed);
    }
    let mut out = Vec::with_capacity(body.len() + 8);
    out.push(0x30);
    der_len(body.len(), &mut out);
    out.extend_from_slice(&body);
    out
}
