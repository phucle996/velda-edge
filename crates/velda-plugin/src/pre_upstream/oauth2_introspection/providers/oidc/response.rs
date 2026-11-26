//! Generic OIDC Response Evaluator & Direct Cache Commit (`providers::oidc::response`).
//!
//! ```text
//!   client::request_raw() -> (status, payload)
//!        ├─ HTTP !2xx        ──► ColdOutcome::Unavailable (preserve or clear SWR flag)
//!        └─ HTTP 200..300    ──► Parse RFC 7662 JSON
//!                                  ├─ active=false / expired / nbf -> store_negative_on_lane
//!                                  ├─ missing required_scopes      -> store_positive_on_lane(false)
//!                                  └─ valid + claims               -> store_positive_on_lane(true)
//! ```

use std::sync::Arc;

use http::header::HeaderValue;
use rustc_hash::FxHashMap;
use serde::Deserialize;
use velda_core::HeaderKey;
use velda_etp::oidc::OidcEtp;

use super::super::super::cache::ColdOutcome;
use super::super::super::scope::check_scopes;
use super::super::IntrospectionCall;
use super::client::request_raw;

#[derive(Deserialize)]
struct OidcResponsePayload {
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

/// Fetches, decodes, evaluates, and directly caches a generic OIDC introspection decision.
pub async fn execute(oidc: &OidcEtp, call: &IntrospectionCall<'_>) -> (Option<u64>, ColdOutcome) {
    let Ok((_path, status, payload)) = request_raw(oidc, call.params, call.token).await else {
        return (None, ColdOutcome::Unavailable);
    };
    if !(200..300).contains(&status) {
        return (None, ColdOutcome::Unavailable);
    }
    let Ok(parsed) = serde_json::from_slice::<OidcResponsePayload>(&payload) else {
        return (None, ColdOutcome::Unavailable);
    };

    let now = velda_core::clock::coarse_sec();
    let (exp_opt, outcome) =
        evaluate_payload(&parsed, call.claims_to_headers, call.required_scopes, now);
    commit_decision(call, exp_opt, &outcome, now);
    (exp_opt, outcome)
}

/// Refreshes a generic OIDC stale entry in Shadow and updates `call.target_lane` directly.
pub async fn revalidate(oidc: &OidcEtp, call: &IntrospectionCall<'_>) {
    let Ok((_path, status, payload)) = request_raw(oidc, call.params, call.token).await else {
        call.cache
            .finish_revalidation_on_lane(call.target_lane, call.fp);
        return;
    };
    if !(200..300).contains(&status) {
        call.cache
            .finish_revalidation_on_lane(call.target_lane, call.fp);
        return;
    }
    let Ok(parsed) = serde_json::from_slice::<OidcResponsePayload>(&payload) else {
        call.cache
            .finish_revalidation_on_lane(call.target_lane, call.fp);
        return;
    };

    let now = velda_core::clock::coarse_sec();
    let (exp_opt, outcome) =
        evaluate_payload(&parsed, call.claims_to_headers, call.required_scopes, now);
    commit_decision(call, exp_opt, &outcome, now);
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
    resp: &OidcResponsePayload,
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

fn resolve_claim_str(resp: &OidcResponsePayload, claim_path: &str) -> Option<String> {
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
