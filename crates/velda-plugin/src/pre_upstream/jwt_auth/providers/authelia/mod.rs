//! Self-Contained Authelia JWKS Fetch & Parse Adapter (`jwt_auth::providers::authelia`).
//!
//! ```text
//!   AutheliaEtp::acquire() ──► StreamLease
//!         ├─► GET {path | /jwks.json} w/ strict Host: host:port
//!         ├─► Read HTTP/1.1 response (Content-Length / chunked / EOF)
//!         └─► Parse RFC 7517 multi-algo JWKS (RS256 + ES256) -> Vec<KeyEntrySpec>
//! ```

use std::fmt::Write as _;
use std::str::FromStr;
use std::time::Duration;

use base64::prelude::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use velda_etp::authelia::AutheliaEtp;

use super::super::crypto::{JwtAlgorithm, VerifierKey};
use super::super::store::{KeyEntrySpec, MAX_JWKS_SLOTS};
use crate::error::PluginError;

const DEFAULT_PATH: &str = "/jwks.json";
const MAX_RESP_BYTES: usize = 1024 * 1024;

pub async fn fetch_keys(
    authelia: &AutheliaEtp,
    path_override: Option<&str>,
    default_alg: Option<JwtAlgorithm>,
    timeout: Duration,
) -> Result<Vec<KeyEntrySpec>, PluginError> {
    let raw_path = path_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_PATH);
    let path = if raw_path.starts_with('/') {
        raw_path.to_string()
    } else {
        format!("/{raw_path}")
    };

    let mut req = format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nUser-Agent: velda-edge/jwt-auth\r\nAccept: application/json\r\nConnection: close\r\n",
        authelia.config.host_header
    );
    if let Some(tok) = authelia.config.bearer_token.as_deref() {
        let _ = write!(req, "Authorization: Bearer {tok}\r\n");
    }
    for (k, v) in &authelia.config.custom_headers {
        let _ = write!(req, "{k}: {v}\r\n");
    }
    req.push_str("\r\n");

    let mut lease = authelia
        .acquire()
        .await
        .map_err(|e| PluginError::Config(format!("Authelia '{}': {e}", authelia.config.id)))?;
    let res = send_and_read(&mut *lease, req.as_bytes(), timeout, &authelia.config.id).await;
    lease.recreate_in_background();
    let (status, body) = res?;
    if status != 200 {
        return Err(PluginError::Config(format!(
            "Authelia '{}' GET {path} returned HTTP {status}",
            authelia.config.id
        )));
    }
    parse_jwks(&body, default_alg, &authelia.config.id)
}

async fn send_and_read<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    req: &[u8],
    timeout: Duration,
    label: &str,
) -> Result<(u16, Vec<u8>), PluginError> {
    tokio::time::timeout(timeout, stream.write_all(req))
        .await
        .map_err(|_| PluginError::Config(format!("Authelia '{label}': write timeout")))?
        .map_err(PluginError::Io)?;
    stream.flush().await.map_err(PluginError::Io)?;
    let mut raw = Vec::with_capacity(8192);
    let mut buf = [0u8; 4096];
    loop {
        let n = tokio::time::timeout(timeout, stream.read(&mut buf))
            .await
            .map_err(|_| PluginError::Config(format!("Authelia '{label}': read timeout")))?
            .map_err(PluginError::Io)?;
        if n == 0 {
            return parse_http(&raw, true, label)?
                .ok_or_else(|| PluginError::Config(format!("Authelia '{label}': EOF")));
        }
        raw.extend_from_slice(&buf[..n]);
        if raw.len() > MAX_RESP_BYTES {
            return Err(PluginError::Config(format!("Authelia '{label}': >1 MiB")));
        }
        if let Some(done) = parse_http(&raw, false, label)? {
            return Ok(done);
        }
    }
}

fn parse_http(raw: &[u8], eof: bool, label: &str) -> Result<Option<(u16, Vec<u8>)>, PluginError> {
    let mut hdrs = [httparse::EMPTY_HEADER; 64];
    let mut parsed = httparse::Response::new(&mut hdrs);
    let off = match parsed.parse(raw) {
        Ok(httparse::Status::Complete(o)) => o,
        Ok(httparse::Status::Partial) => return Ok(None),
        Err(e) => {
            return Err(PluginError::Config(format!(
                "Authelia '{label}': parse {e}"
            )));
        }
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
            let Some(crlf) = cur.windows(2).position(|w| w == b"\r\n") else {
                return Ok(None);
            };
            let hex = std::str::from_utf8(&cur[..crlf])
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .trim();
            let sz = usize::from_str_radix(hex, 16)
                .map_err(|_| PluginError::Config("bad chunk".into()))?;
            cur = &cur[crlf + 2..];
            if sz == 0 {
                return Ok(
                    (cur == b"\r\n" || cur.windows(4).any(|w| w == b"\r\n\r\n") || eof)
                        .then_some((status, out)),
                );
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

fn parse_jwks(
    body: &[u8],
    default_alg: Option<JwtAlgorithm>,
    label: &str,
) -> Result<Vec<KeyEntrySpec>, PluginError> {
    let root: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| PluginError::Config(format!("Authelia '{label}': JSON {e}")))?;
    let arr = root
        .get("keys")
        .and_then(|v| v.as_array())
        .ok_or_else(|| PluginError::Config(format!("Authelia '{label}': missing 'keys'")))?;
    let mut out = Vec::with_capacity(arr.len().min(MAX_JWKS_SLOTS));
    for item in arr {
        if out.len() >= MAX_JWKS_SLOTS {
            break;
        }
        match parse_jwk(item, default_alg) {
            Ok(spec) => out.push(spec),
            Err(err) => {
                tracing::warn!(provider = %label, kid = ?item.get("kid"), error = %err, "Skipping Authelia JWK entry")
            }
        }
    }
    Ok(out)
}

fn parse_jwk(
    v: &serde_json::Value,
    default_alg: Option<JwtAlgorithm>,
) -> Result<KeyEntrySpec, PluginError> {
    if let Some(u) = v.get("use").and_then(|x| x.as_str())
        && u != "sig"
    {
        return Err(PluginError::Config(format!(
            "JWK with use '{u}' unsupported"
        )));
    }
    let kid = v
        .get("kid")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string().into_boxed_str());
    let kty = v
        .get("kty")
        .and_then(|x| x.as_str())
        .ok_or_else(|| PluginError::Config("missing kty".into()))?;
    let decl = v
        .get("alg")
        .and_then(|x| x.as_str())
        .map(JwtAlgorithm::from_str)
        .transpose()?;
    match kty.to_uppercase().as_str() {
        "RSA" => {
            let n_raw = b64(v
                .get("n")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PluginError::Config("missing n".into()))?)?;
            let e_raw = b64(v
                .get("e")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PluginError::Config("missing e".into()))?)?;
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
            let crv = v.get("crv").and_then(|x| x.as_str()).unwrap_or("P-256");
            let (alg, clen, slen) = match crv {
                "P-256" => (JwtAlgorithm::ES256, 32, 64),
                "P-384" => (JwtAlgorithm::ES384, 48, 96),
                o => return Err(PluginError::Config(format!("EC {o}"))),
            };
            if decl.is_some_and(|a| a != alg) {
                return Err(PluginError::Config("EC alg mismatch".into()));
            }
            let x = b64(v
                .get("x")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PluginError::Config("missing x".into()))?)?;
            let y = b64(v
                .get("y")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PluginError::Config("missing y".into()))?)?;
            if x.len() != clen || y.len() != clen {
                return Err(PluginError::Config("EC len".into()));
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
            if v.get("crv").and_then(|x| x.as_str()) != Some("Ed25519") {
                return Err(PluginError::Config("OKP curve".into()));
            }
            if decl.is_some_and(|a| a != JwtAlgorithm::EdDSA) {
                return Err(PluginError::Config("OKP alg".into()));
            }
            let raw: [u8; 32] = b64(v
                .get("x")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PluginError::Config("missing x".into()))?)?
            .try_into()
            .map_err(|_| PluginError::Config("Ed25519 len".into()))?;
            Ok(KeyEntrySpec {
                raw_kid: kid,
                alg: JwtAlgorithm::EdDSA,
                key: VerifierKey::Ed25519 {
                    public_key_raw: raw,
                },
            })
        }
        "OCT" => {
            let raw = b64(v
                .get("k")
                .and_then(|x| x.as_str())
                .ok_or_else(|| PluginError::Config("missing k".into()))?)?;
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
        o => Err(PluginError::Config(format!("kty '{o}'"))),
    }
}

fn b64(s: &str) -> Result<Vec<u8>, PluginError> {
    BASE64_URL_SAFE_NO_PAD
        .decode(s.trim().trim_end_matches('='))
        .map_err(|e| PluginError::Config(format!("b64: {e}")))
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
