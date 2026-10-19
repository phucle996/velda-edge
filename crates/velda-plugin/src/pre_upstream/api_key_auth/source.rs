//! API Key Token Extraction Sources and Consumer Entries.
//!
//! Provides extraction of API keys from request headers (with optional prefix, e.g. `ApiKey <token>`)
//! or URI query parameters.

use http::header::{HeaderName, HeaderValue};
use velda_core::L7Request;

/// Method and location to extract the API key token from incoming requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeySource {
    Header { name: HeaderName, prefix: String },
    Query { param: String },
}

impl KeySource {
    /// Extracts token from a single source specification.
    #[inline]
    pub fn extract<'a>(&self, req: &'a L7Request) -> Option<(&'a str, Option<HeaderName>)> {
        match self {
            Self::Header { name, prefix } => {
                if let Some(val) = req.headers.get(name)
                    && let Ok(s) = val.to_str()
                {
                    let trimmed = s.trim();
                    if prefix.is_empty() {
                        if !trimmed.is_empty() {
                            return Some((trimmed, Some(name.clone())));
                        }
                    } else if trimmed.len() > prefix.len()
                        && trimmed.is_char_boundary(prefix.len())
                        && trimmed[..prefix.len()].eq_ignore_ascii_case(prefix)
                    {
                        let token = trimmed[prefix.len()..].trim();
                        if !token.is_empty() {
                            return Some((token, Some(name.clone())));
                        }
                    }
                }
                None
            }
            Self::Query { param } => {
                if let Some(query) = req.uri.query() {
                    for pair in query.split('&') {
                        if let Some((k, v)) = pair.split_once('=')
                            && k == param
                        {
                            let token = v.trim();
                            if !token.is_empty() {
                                return Some((token, None));
                            }
                        }
                    }
                }
                None
            }
        }
    }
}

/// Extracts the raw token string from the request matching against an ordered list of sources.
#[inline]
pub fn extract_token<'a>(
    sources: &[KeySource],
    req: &'a L7Request,
) -> Option<(&'a str, Option<HeaderName>)> {
    for source in sources {
        if let Some(res) = source.extract(req) {
            return Some(res);
        }
    }
    None
}

/// Pre-compiled metadata and headers to inject for an authorized consumer.
#[derive(Clone, Debug)]
pub struct ApiKeyEntry {
    pub consumer_id: String,
    pub injected_headers: Vec<(HeaderName, HeaderValue)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method, Uri, Version};
    use velda_core::Body;

    #[test]
    fn test_extract_header_with_prefix() {
        let source = KeySource::Header {
            name: http::header::AUTHORIZATION,
            prefix: "ApiKey ".into(),
        };

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("ApiKey secret_tok_123"),
        );
        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let (token, hdr) = source.extract(&req).unwrap();
        assert_eq!(token, "secret_tok_123");
        assert_eq!(hdr, Some(http::header::AUTHORIZATION));
    }

    #[test]
    fn test_extract_query_param() {
        let source = KeySource::Query {
            param: "api_key".into(),
        };

        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/v1/items?api_key=query_secret_456&other=1"),
            Version::HTTP_11,
            HeaderMap::new(),
            Body::Empty,
        );

        let (token, hdr) = source.extract(&req).unwrap();
        assert_eq!(token, "query_secret_456");
        assert_eq!(hdr, None);
    }
}
