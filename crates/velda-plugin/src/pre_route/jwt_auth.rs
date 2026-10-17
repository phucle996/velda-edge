//! JWT Authentication Pre-Route Plugin Skeleton (`phase: "pre_route"`).
//!
//! Evaluates downstream requests before routing. Validates RFC 7519 Bearer tokens,
//! extracts authenticated claims, and fails fast with 401 Unauthorized before body reading or route evaluation.

use http::header::{AUTHORIZATION, HeaderValue};
use http::{HeaderName, StatusCode};
use velda_core::{Action, L7Request, L7Response};

use super::PreRoutePlugin;

/// Production skeleton for JWT token validation and subject extraction.
#[derive(Debug, Clone)]
pub struct JwtAuthPlugin {
    pub id: String,
    pub issuer: String,
    pub subject_header: HeaderName,
}

impl JwtAuthPlugin {
    /// Constructs a new JWT auth plugin instance.
    pub fn new(id: impl Into<String>, issuer: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            issuer: issuer.into(),
            subject_header: HeaderName::from_static("x-authenticated-user"),
        }
    }

    /// Deserializes and constructs an instance directly from a [`PluginConfig`].
    ///
    /// # Module Ownership Invariant
    /// Parsing logic, schema defaults, and header sanitization belong strictly to the
    /// plugin itself, keeping central factories razor-thin and decoupled.
    pub fn from_config(
        config: &velda_sync::post_sync::plugin::PluginConfig,
    ) -> Result<Self, crate::error::PluginError> {
        let issuer = config
            .config
            .get("issuer")
            .and_then(|v| v.as_str())
            .unwrap_or("https://auth.internal");

        let mut plugin = Self::new(&config.id, issuer);
        if let Some(subject_hdr) = config
            .config
            .get("subject_header")
            .and_then(|v| v.as_str())
            .and_then(|s| HeaderName::from_bytes(s.as_bytes()).ok())
        {
            plugin.subject_header = subject_hdr;
        }

        Ok(plugin)
    }
}

impl PreRoutePlugin for JwtAuthPlugin {
    #[inline]
    fn id(&self) -> &str {
        &self.id
    }

    fn on_request(&self, req: &mut L7Request) -> Action {
        let auth_header = match req.headers.get(&AUTHORIZATION) {
            Some(val) => match val.to_str() {
                Ok(s) => s,
                Err(_) => {
                    return Action::Respond(
                        L7Response::from_bytes(
                            StatusCode::UNAUTHORIZED,
                            "Invalid Authorization header encoding",
                        )
                        .with_header(
                            http::header::WWW_AUTHENTICATE,
                            HeaderValue::from_static("Bearer error=\"invalid_token\""),
                        ),
                    );
                }
            },
            None => {
                return Action::Respond(
                    L7Response::from_bytes(
                        StatusCode::UNAUTHORIZED,
                        "Missing Authorization Bearer token",
                    )
                    .with_header(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("Bearer realm=\"velda-edge\""),
                    ),
                );
            }
        };

        let token = match auth_header.strip_prefix("Bearer ") {
            Some(t) if !t.trim().is_empty() => t.trim(),
            _ => {
                return Action::Respond(
                    L7Response::from_bytes(
                        StatusCode::UNAUTHORIZED,
                        "Authorization header must use Bearer scheme",
                    )
                    .with_header(
                        http::header::WWW_AUTHENTICATE,
                        HeaderValue::from_static("Bearer error=\"invalid_token\""),
                    ),
                );
            }
        };

        // Basic structural validation (Header.Payload.Signature)
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Action::Respond(
                L7Response::from_bytes(
                    StatusCode::UNAUTHORIZED,
                    "Malformed JWT compact serialization",
                )
                .with_header(
                    http::header::WWW_AUTHENTICATE,
                    HeaderValue::from_static("Bearer error=\"invalid_token\""),
                ),
            );
        }

        // In production implementation, verify cryptographic signature against KeySet / JWKS cache.
        // For the skeleton, inject authenticated claims metadata downstream to avoid re-decoding.
        req.headers.insert(
            self.subject_header.clone(),
            HeaderValue::from_static("authenticated-principal"),
        );

        Action::Continue
    }

    #[inline]
    fn capability(&self) -> crate::capability::PluginCapability {
        crate::capability::PluginCapability::header_only(crate::capability::AccessLevel::Write)
    }

    #[inline]
    fn supported_protocols(&self) -> crate::capability::SupportedProtocols {
        crate::capability::SupportedProtocols::ALL_L7
    }
}
