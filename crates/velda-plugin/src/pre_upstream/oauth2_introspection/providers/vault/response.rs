//! HashiCorp Vault Response Evaluator & Direct Cache Commit (`providers::vault::response`).
//!
//! ```text
//!   client::request_raw() -> VaultRawResponse
//!        ├─ HTTP 400 | 404 | (403 & !svc_tok) ──► store_negative_on_lane -> Unauthorized
//!        ├─ HTTP !2xx                         ──► ColdOutcome::Unavailable
//!        └─ HTTP 200..300:
//!             ├─ `is_oidc_introspect`         ──► Parse RFC 7662 JSON -> Direct Cache Commit
//!             └─ `/v1/auth/token/lookup*`     ──► Map `data.{entity_id, display_name, policies, ttl}`
//! ```

use std::sync::Arc;

use http::header::HeaderValue;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use velda_core::HeaderKey;
use velda_etp::vault::VaultEtp;

use super::super::super::cache::ColdOutcome;
use super::super::super::scope::check_scopes;
use super::super::IntrospectionCall;
use super::client::request_raw;

#[derive(Deserialize)]
struct VaultStandardOidcPayload {
    active: bool,
    #[serde(default)]
    exp: Option<serde_json::Value>,
    #[serde(default)]
    nbf: Option<serde_json::Value>,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Deserialize)]
struct VaultStandardLookupData {
    #[serde(default)]
    entity_id: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    policies: Option<Vec<String>>,
    #[serde(default)]
    ttl: Option<u64>,
}

#[derive(Deserialize)]
struct VaultStandardLookupEnvelope {
    data: Option<VaultStandardLookupData>,
}

#[derive(Deserialize)]
struct VaultOidcPayload {
    active: bool,
    #[serde(default)]
    exp: Option<serde_json::Value>,
    #[serde(default)]
    nbf: Option<serde_json::Value>,
    #[serde(default)]
    sub: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(flatten)]
    raw_claims: FxHashMap<String, serde_json::Value>,
}

struct NormalizedVaultClaims {
    active: bool,
    exp: Option<serde_json::Value>,
    nbf: Option<serde_json::Value>,
    sub: Option<String>,
    client_id: Option<String>,
    scope: Option<String>,
    raw_claims: FxHashMap<String, serde_json::Value>,
}

/// Fetches, decodes, evaluates, and directly caches a Vault token lookup or OIDC decision.
pub async fn execute(vault: &VaultEtp, call: &IntrospectionCall<'_>) -> (Option<u64>, ColdOutcome) {
    let Ok(raw) = request_raw(vault, call.params, call.token).await else {
        return (None, ColdOutcome::Unavailable);
    };

    let now = velda_core::clock::coarse_sec();
    if raw.status == 400 || raw.status == 404 || (raw.status == 403 && !raw.used_service_token) {
        commit_decision(call, None, &ColdOutcome::Unauthorized, now);
        return (None, ColdOutcome::Unauthorized);
    }
    if !(200..300).contains(&raw.status) {
        return (None, ColdOutcome::Unavailable);
    }

    let Some(norm) = decode_vault_response(
        &raw.payload,
        raw.is_oidc_introspect,
        call.claims_to_headers,
        now,
    ) else {
        return (None, ColdOutcome::Unavailable);
    };

    let (exp_opt, outcome) =
        evaluate_payload(&norm, call.claims_to_headers, call.required_scopes, now);
    commit_decision(call, exp_opt, &outcome, now);
    (exp_opt, outcome)
}

/// Refreshes a Vault stale entry in Shadow and updates `call.target_lane` directly.
pub async fn revalidate(vault: &VaultEtp, call: &IntrospectionCall<'_>) {
    let Ok(raw) = request_raw(vault, call.params, call.token).await else {
        call.cache
            .finish_revalidation_on_lane(call.target_lane, call.fp);
        return;
    };

    let now = velda_core::clock::coarse_sec();
    if raw.status == 400 || raw.status == 404 || (raw.status == 403 && !raw.used_service_token) {
        commit_decision(call, None, &ColdOutcome::Unauthorized, now);
        return;
    }
    if !(200..300).contains(&raw.status) {
        call.cache
            .finish_revalidation_on_lane(call.target_lane, call.fp);
        return;
    }

    let Some(norm) = decode_vault_response(
        &raw.payload,
        raw.is_oidc_introspect,
        call.claims_to_headers,
        now,
    ) else {
        call.cache
            .finish_revalidation_on_lane(call.target_lane, call.fp);
        return;
    };

    let (exp_opt, outcome) =
        evaluate_payload(&norm, call.claims_to_headers, call.required_scopes, now);
    commit_decision(call, exp_opt, &outcome, now);
}

fn decode_vault_response(
    payload: &[u8],
    is_oidc_introspect: bool,
    claims_to_headers: &[(Box<str>, HeaderKey)],
    now_sec: u64,
) -> Option<NormalizedVaultClaims> {
    let needs_extra = claims_to_headers
        .iter()
        .any(|(c, _)| !matches!(c.as_ref(), "sub" | "client_id" | "scope"));

    if is_oidc_introspect {
        if !needs_extra {
            let p = serde_json::from_slice::<VaultStandardOidcPayload>(payload).ok()?;
            return Some(NormalizedVaultClaims {
                active: p.active,
                exp: p.exp,
                nbf: p.nbf,
                sub: p.sub,
                client_id: p.client_id,
                scope: p.scope,
                raw_claims: FxHashMap::default(),
            });
        }
        let p = serde_json::from_slice::<VaultOidcPayload>(payload).ok()?;
        return Some(NormalizedVaultClaims {
            active: p.active,
            exp: p.exp,
            nbf: p.nbf,
            sub: p.sub,
            client_id: p.client_id,
            scope: p.scope,
            raw_claims: p.raw_claims,
        });
    }

    if !needs_extra {
        let env = serde_json::from_slice::<VaultStandardLookupEnvelope>(payload).ok()?;
        let data = env.data?;
        let sub = data
            .entity_id
            .filter(|s| !s.is_empty())
            .or(data.display_name);
        let scope = data
            .policies
            .filter(|arr| !arr.is_empty())
            .map(|arr| arr.join(" "))
            .filter(|s| !s.is_empty());
        let exp = data
            .ttl
            .filter(|&ttl| ttl > 0)
            .map(|ttl| serde_json::Value::from(now_sec.saturating_add(ttl)));
        return Some(NormalizedVaultClaims {
            active: true,
            exp,
            nbf: None,
            sub,
            client_id: None,
            scope,
            raw_claims: FxHashMap::default(),
        });
    }

    let v: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let data = v.get("data")?.as_object()?;

    let sub = data
        .get("entity_id")
        .and_then(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .or_else(|| data.get("display_name").and_then(|s| s.as_str()))
        .map(ToString::to_string);

    let scope = data
        .get("policies")
        .and_then(|p| p.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str())
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|s| !s.is_empty());

    let exp = data
        .get("ttl")
        .and_then(|t| t.as_u64())
        .filter(|&ttl| ttl > 0)
        .map(|ttl| serde_json::Value::from(now_sec.saturating_add(ttl)));

    let mut raw_claims = FxHashMap::default();
    for (k, val) in data {
        raw_claims.insert(k.clone(), val.clone());
    }

    Some(NormalizedVaultClaims {
        active: true,
        exp,
        nbf: None,
        sub,
        client_id: None,
        scope,
        raw_claims,
    })
}

#[inline]
fn commit_decision(
    call: &IntrospectionCall<'_>,
    exp_opt: Option<u64>,
    outcome: &ColdOutcome,
    now: u64,
) {
    match outcome {
        ColdOutcome::Allow(hdrs) => {
            call.cache.store_positive_on_lane(
                call.target_lane,
                call.fp,
                true,
                hdrs.clone(),
                exp_opt,
                now,
            );
        }
        ColdOutcome::Forbidden => {
            call.cache
                .store_positive_on_lane(call.target_lane, call.fp, false, None, exp_opt, now);
        }
        ColdOutcome::Unauthorized => {
            call.cache
                .store_negative_on_lane(call.target_lane, call.fp, now);
        }
        ColdOutcome::Unavailable => {}
    }
}

fn evaluate_payload(
    resp: &NormalizedVaultClaims,
    claims_to_headers: &[(Box<str>, HeaderKey)],
    required_scopes: &[String],
    now: u64,
) -> (Option<u64>, ColdOutcome) {
    if !resp.active {
        return (None, ColdOutcome::Unauthorized);
    }

    let Ok(exp_opt) = parse_numeric_date_opt(resp.exp.as_ref(), false) else {
        return (None, ColdOutcome::Unauthorized);
    };
    if let Some(exp) = exp_opt
        && now >= exp
    {
        return (None, ColdOutcome::Unauthorized);
    }

    let Ok(nbf_opt) = parse_numeric_date_opt(resp.nbf.as_ref(), true) else {
        return (None, ColdOutcome::Unauthorized);
    };
    if let Some(nbf) = nbf_opt
        && now < nbf
    {
        return (None, ColdOutcome::Unauthorized);
    }

    if !check_scopes(resp.scope.as_deref(), required_scopes) {
        return (exp_opt, ColdOutcome::Forbidden);
    }

    let headers = if claims_to_headers.is_empty() {
        None
    } else {
        let mut pairs = Vec::with_capacity(claims_to_headers.len());
        for (claim_path, header_key) in claims_to_headers {
            if let Some(val_str) = resolve_claim_str(resp, claim_path)
                && let Ok(hval) = HeaderValue::try_from(val_str)
            {
                pairs.push((header_key.clone(), hval));
            }
        }
        if pairs.is_empty() {
            None
        } else {
            Some(Arc::from(pairs))
        }
    };

    (exp_opt, ColdOutcome::Allow(headers))
}

fn resolve_claim_str(resp: &NormalizedVaultClaims, claim_path: &str) -> Option<String> {
    if !claim_path.contains('.') {
        match claim_path {
            "sub" => return resp.sub.clone(),
            "client_id" => return resp.client_id.clone(),
            "scope" => return resp.scope.clone(),
            _ => return resp.raw_claims.get(claim_path).and_then(value_to_header),
        }
    }
    let mut segs = claim_path.split('.');
    let first = segs.next()?;
    let mut cur = resp.raw_claims.get(first)?;
    for seg in segs {
        cur = cur.as_object()?.get(seg)?;
    }
    value_to_header(cur)
}

fn value_to_header(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(if *b { "true".into() } else { "false".into() }),
        serde_json::Value::Array(arr) => {
            let mut out = String::new();
            for item in arr {
                if let serde_json::Value::String(s) = item {
                    if !out.is_empty() {
                        out.push_str(", ");
                    }
                    out.push_str(s);
                }
            }
            if out.is_empty() { None } else { Some(out) }
        }
        _ => None,
    }
}

fn parse_numeric_date_opt(val: Option<&serde_json::Value>, ceil: bool) -> Result<Option<u64>, ()> {
    let Some(v) = val.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let n = v.as_number().ok_or(())?;
    if let Some(u) = n.as_u64() {
        return Ok(Some(u));
    }
    if let Some(i) = n.as_i64() {
        return Ok(Some(if i <= 0 { 0 } else { i as u64 }));
    }
    if let Some(f) = n.as_f64()
        && f.is_finite()
    {
        if f <= 0.0 {
            return Ok(Some(0));
        }
        if f >= u64::MAX as f64 {
            return Ok(Some(u64::MAX));
        }
        return Ok(Some(if ceil {
            f.ceil() as u64
        } else {
            f.floor() as u64
        }));
    }
    Err(())
}
