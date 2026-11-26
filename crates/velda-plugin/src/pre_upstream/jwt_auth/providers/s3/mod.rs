//! Self-Contained Amazon S3 / MinIO / R2 Key Fetcher (`jwt_auth::providers::s3`).
//!
//! ```text
//!   S3Etp::acquire() ──► S3Lease
//!         ├─► AWS4-HMAC-SHA256 GET /{bucket}/{object_key} (or /{object_key})
//!         ├─► Read HTTP/1.1 response cleanly
//!         └─► Parse JWKS or `{ alg, secret_key | public_key_pem }` into KeyEntrySpec
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use base64::prelude::*;
use ring::digest::{SHA256, digest};
use ring::hmac;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velda_etp::s3::S3Etp;

use super::super::crypto::{JwtAlgorithm, VerifierKey};
use super::super::store::{KeyEntrySpec, MAX_JWKS_SLOTS};
use crate::error::PluginError;

const MAX_RESP_BYTES: usize = 1024 * 1024;

/// Executes complete AWS SigV4 GET over borrowed `S3Lease` and parses JWKS/secret keys.
pub async fn fetch_keys(
    s3: &Arc<S3Etp>,
    object_key_opt: Option<&str>,
    default_alg: Option<JwtAlgorithm>,
    timeout: Duration,
) -> Result<Vec<KeyEntrySpec>, PluginError> {
    let raw_key = object_key_opt
        .map(|s| s.trim().trim_start_matches('/'))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            PluginError::Config(format!(
                "jwt_auth using S3 ETP '{}' requires 'path' (object key)",
                s3.config.id
            ))
        })?;

    let uri_path = if s3.config.force_path_style && !s3.config.bucket.is_empty() {
        format!("/{}/{}", s3.config.bucket, raw_key)
    } else {
        format!("/{raw_key}")
    };

    let empty_sha256 = sha256_hex(b"");
    let (amz_date, date_short) = format_amz_date(velda_core::clock::coarse_sec());
    let token_canon = s3
        .config
        .session_token
        .as_deref()
        .map(|t| format!("x-amz-security-token:{t}\n"))
        .unwrap_or_default();
    let signed_headers = if s3.config.session_token.is_some() {
        "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
    } else {
        "host;x-amz-content-sha256;x-amz-date"
    };

    let canon = format!(
        "GET\n{uri_path}\n\nhost:{}\nx-amz-content-sha256:{empty_sha256}\nx-amz-date:{amz_date}\n{token_canon}\n{signed_headers}\n{empty_sha256}",
        s3.config.host_header
    );
    let scope = format!("{date_short}/{}/s3/aws4_request", s3.config.region);
    let sts = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canon.as_bytes())
    );
    let k_date = hmac_sha256(
        format!("AWS4{}", s3.config.secret_access_key).as_bytes(),
        date_short.as_bytes(),
    );
    let k_region = hmac_sha256(&k_date, s3.config.region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    let k_sign = hmac_sha256(&k_service, b"aws4_request");
    let sig = hex_lower(&hmac_sha256(&k_sign, sts.as_bytes()));

    let sec_hdr = s3
        .config
        .session_token
        .as_deref()
        .map(|t| format!("X-Amz-Security-Token: {t}\r\n"))
        .unwrap_or_default();

    let req = format!(
        "GET {uri_path} HTTP/1.1\r\nHost: {}\r\nX-Amz-Content-SHA256: {empty_sha256}\r\nX-Amz-Date: {amz_date}\r\n{sec_hdr}Authorization: AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={sig}\r\nConnection: close\r\n\r\n",
        s3.config.host_header, s3.config.access_key_id
    );

    let mut lease = s3.acquire().await.map_err(|e| {
        PluginError::Config(format!("S3 ETP '{}' acquire error: {e}", s3.config.id))
    })?;
    let res = send_and_read(&mut *lease, req.as_bytes(), timeout, &s3.config.id).await;
    lease.recreate_in_background();
    let (status, body) = res?;

    if !(200..300).contains(&status) {
        return Err(PluginError::Config(format!(
            "S3 ETP '{}' GET {uri_path} returned HTTP {status}",
            s3.config.id
        )));
    }

    let val: serde_json::Value = serde_json::from_slice(&body)
        .map_err(|e| PluginError::Config(format!("S3 ETP '{}': bad JSON: {e}", s3.config.id)))?;

    if let Some(arr) = val.get("keys").and_then(|v| v.as_array()) {
        let mut out = Vec::with_capacity(arr.len().min(MAX_JWKS_SLOTS));
        for item in arr.iter().take(MAX_JWKS_SLOTS) {
            if let Ok(entry) = parse_jwk_or_secret(item, default_alg, &s3.config.id) {
                out.push(entry);
            }
        }
        return Ok(out);
    }
    Ok(vec![parse_jwk_or_secret(&val, default_alg, &s3.config.id)?])
}

async fn send_and_read<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    req: &[u8],
    timeout: Duration,
    label: &str,
) -> Result<(u16, Vec<u8>), PluginError> {
    tokio::time::timeout(timeout, stream.write_all(req))
        .await
        .map_err(|_| PluginError::Config(format!("S3 '{label}': write timeout")))?
        .map_err(PluginError::Io)?;
    stream.flush().await.map_err(PluginError::Io)?;

    let mut raw = Vec::with_capacity(8192);
    let mut buf = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(timeout, stream.read(&mut buf))
            .await
            .map_err(|_| PluginError::Config(format!("S3 '{label}': read timeout")))?
            .map_err(PluginError::Io)?;
        if n == 0 {
            return extract_body(&raw, true, label)?
                .ok_or_else(|| PluginError::Config(format!("S3 '{label}': incomplete HTTP")));
        }
        raw.extend_from_slice(&buf[..n]);
        if raw.len() > MAX_RESP_BYTES {
            return Err(PluginError::Config(format!("S3 '{label}': >1 MiB")));
        }
        if let Some(done) = extract_body(&raw, false, label)? {
            return Ok(done);
        }
    }
}

fn extract_body(raw: &[u8], eof: bool, label: &str) -> Result<Option<(u16, Vec<u8>)>, PluginError> {
    let mut hdrs = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Response::new(&mut hdrs);
    let off = match parsed.parse(raw) {
        Ok(httparse::Status::Complete(o)) => o,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(e) => return Err(PluginError::Config(format!("S3 '{label}': parse {e}"))),
    };
    let status = parsed.code.unwrap_or(0);
    let mut chunked = false;
    let mut clen = None;
    for h in parsed.headers.iter() {
        if h.name.eq_ignore_ascii_case("transfer-encoding")
            && std::str::from_utf8(h.value)
                .is_ok_and(|v| v.to_ascii_lowercase().contains("chunked"))
        {
            chunked = true;
        } else if h.name.eq_ignore_ascii_case("content-length") {
            clen = std::str::from_utf8(h.value)
                .ok()
                .and_then(|v| v.trim().parse::<usize>().ok());
        }
    }
    let body = &raw[off..];
    if chunked {
        let mut out = Vec::new();
        let mut cur = body;
        loop {
            let Some(rel) = cur.windows(2).position(|w| w == b"\r\n") else {
                return Ok(None);
            };
            let line = std::str::from_utf8(&cur[..rel])
                .map_err(|e| PluginError::Config(format!("S3 '{label}': chunk: {e}")))?;
            let hex = line.split(';').next().unwrap_or("").trim();
            let sz = usize::from_str_radix(hex, 16)
                .map_err(|_| PluginError::Config(format!("S3 '{label}': bad chunk '{hex}'")))?;
            cur = &cur[rel + 2..];
            if sz == 0 {
                if cur.starts_with(b"\r\n") || cur.windows(4).any(|w| w == b"\r\n\r\n") {
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
    if let Some(len) = clen {
        return Ok((body.len() >= len).then(|| (status, body[..len].to_vec())));
    }
    Ok(eof.then(|| (status, body.to_vec())))
}

fn parse_jwk_or_secret(
    val: &serde_json::Value,
    default_alg: Option<JwtAlgorithm>,
    label: &str,
) -> Result<KeyEntrySpec, PluginError> {
    if let Some(kty) = val.get("kty").and_then(|v| v.as_str()) {
        if let Some(u) = val.get("use").and_then(|v| v.as_str())
            && u != "sig"
        {
            return Err(PluginError::Config(format!("Unsupported JWK use '{u}'")));
        }
        let kid = val
            .get("kid")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string().into_boxed_str());
        let decl = val
            .get("alg")
            .and_then(|x| x.as_str())
            .map(JwtAlgorithm::from_str)
            .transpose()?;
        return match kty.to_uppercase().as_str() {
            "RSA" => {
                let n_raw = decode_b64url(
                    val.get("n")
                        .and_then(|x| x.as_str())
                        .ok_or_else(|| PluginError::Config("RSA JWK missing n".into()))?,
                    label,
                )?;
                let e_raw = decode_b64url(
                    val.get("e")
                        .and_then(|x| x.as_str())
                        .ok_or_else(|| PluginError::Config("RSA JWK missing e".into()))?,
                    label,
                )?;
                let (n, e) = (trim0(&n_raw), trim0(&e_raw));
                if n.len() < 256 {
                    return Err(PluginError::Config("RSA < 2048b".into()));
                }
                let alg = decl.unwrap_or_else(|| {
                    default_alg
                        .filter(|a| a.is_rsa())
                        .unwrap_or(JwtAlgorithm::RS256)
                });
                if !alg.is_rsa() {
                    return Err(PluginError::Config("RSA alg mismatch".into()));
                }
                Ok(KeyEntrySpec {
                    raw_kid: kid,
                    alg,
                    key: VerifierKey::Rsa {
                        public_key_der: pkcs1(n, e),
                        expected_sig_len: n.len(),
                    },
                })
            }
            "EC" => {
                let crv = val.get("crv").and_then(|x| x.as_str()).unwrap_or("P-256");
                let (alg, clen, slen) = match crv {
                    "P-256" => (JwtAlgorithm::ES256, 32, 64),
                    "P-384" => (JwtAlgorithm::ES384, 48, 96),
                    o => return Err(PluginError::Config(format!("Unsupported EC curve '{o}'"))),
                };
                if decl.is_some_and(|a| a != alg) {
                    return Err(PluginError::Config("EC alg mismatch".into()));
                }
                let x = decode_b64url(
                    val.get("x")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| PluginError::Config("EC missing x".into()))?,
                    label,
                )?;
                let y = decode_b64url(
                    val.get("y")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| PluginError::Config("EC missing y".into()))?,
                    label,
                )?;
                if x.len() != clen || y.len() != clen {
                    return Err(PluginError::Config("EC coordinate length mismatch".into()));
                }
                let mut pt = Vec::with_capacity(1 + clen * 2);
                pt.push(0x04);
                pt.extend_from_slice(&x);
                pt.extend_from_slice(&y);
                Ok(KeyEntrySpec {
                    raw_kid: kid,
                    alg,
                    key: VerifierKey::Ecdsa {
                        public_key_raw: pt,
                        expected_sig_len: slen,
                    },
                })
            }
            "OKP" => {
                if val.get("crv").and_then(|x| x.as_str()) != Some("Ed25519") {
                    return Err(PluginError::Config("Unsupported OKP curve".into()));
                }
                if decl.is_some_and(|a| a != JwtAlgorithm::EdDSA) {
                    return Err(PluginError::Config("OKP alg mismatch".into()));
                }
                let raw: [u8; 32] = decode_b64url(
                    val.get("x")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| PluginError::Config("OKP missing x".into()))?,
                    label,
                )?
                .try_into()
                .map_err(|_| PluginError::Config("Ed25519 x != 32B".into()))?;
                Ok(KeyEntrySpec {
                    raw_kid: kid,
                    alg: JwtAlgorithm::EdDSA,
                    key: VerifierKey::Ed25519 {
                        public_key_raw: raw,
                    },
                })
            }
            "OCT" => {
                let raw = decode_b64url(
                    val.get("k")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| PluginError::Config("oct JWK missing k".into()))?,
                    label,
                )?;
                let alg = decl.unwrap_or_else(|| {
                    default_alg
                        .filter(|a| a.is_symmetric())
                        .unwrap_or(JwtAlgorithm::HS256)
                });
                Ok(KeyEntrySpec {
                    raw_kid: kid,
                    alg,
                    key: VerifierKey::from_hmac_bytes(alg, &raw)?,
                })
            }
            other => Err(PluginError::Config(format!(
                "Unsupported JWK kty '{other}'"
            ))),
        };
    }

    let alg = if let Some(a) = val.get("alg").and_then(|v| v.as_str()) {
        JwtAlgorithm::from_str(a)?
    } else if let Some(d) = default_alg {
        d
    } else {
        return Err(PluginError::Config(format!(
            "S3 '{label}': missing explicit 'alg'"
        )));
    };
    let kid = val
        .get("kid")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string().into_boxed_str());
    let sec = val.get("secret_key").and_then(|v| v.as_str());
    let pem = val
        .get("public_key_pem")
        .or_else(|| val.get("public_key"))
        .and_then(|v| v.as_str());
    Ok(KeyEntrySpec {
        raw_kid: kid,
        alg,
        key: VerifierKey::new(alg, sec, pem)?,
    })
}

fn decode_b64url(s: &str, label: &str) -> Result<Vec<u8>, PluginError> {
    BASE64_URL_SAFE_NO_PAD
        .decode(s.trim().trim_end_matches('='))
        .map_err(|e| PluginError::Config(format!("S3 '{label}': b64url: {e}")))
}

fn trim0(mut b: &[u8]) -> &[u8] {
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

fn pkcs1(n: &[u8], e: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(n.len() + e.len() + 16);
    for r in [n, e] {
        let t = trim0(r);
        let p = !t.is_empty() && (t[0] & 0x80) != 0;
        b.push(0x02);
        der_len(t.len() + usize::from(p), &mut b);
        if p {
            b.push(0);
        }
        b.extend_from_slice(t);
    }
    let mut d = Vec::with_capacity(b.len() + 8);
    d.push(0x30);
    der_len(b.len(), &mut d);
    d.extend_from_slice(&b);
    d
}

fn format_amz_date(unix_secs: u64) -> (String, String) {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let yr = (y + i64::from(m <= 2)) as u32;
    (
        format!("{yr:04}{m:02}{d:02}T{hh:02}{mm:02}{ss:02}Z"),
        format!("{yr:04}{m:02}{d:02}"),
    )
}

fn sha256_hex(data: &[u8]) -> String {
    hex_lower(digest(&SHA256, data).as_ref())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), data)
        .as_ref()
        .try_into()
        .expect("32B")
}

fn hex_lower(bytes: &[u8]) -> String {
    const H: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(H[(b >> 4) as usize] as char);
        s.push(H[(b & 0xF) as usize] as char);
    }
    s
}
