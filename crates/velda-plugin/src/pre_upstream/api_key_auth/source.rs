//! API Key Token Extraction Sources and Consumer Entries.
//!
//! Provides extraction of API keys from request headers (with optional prefix, e.g. `ApiKey <token>`)
//! or URI query parameters.

use http::header::{HeaderName, HeaderValue};
use velda_core::L7Request;

/// Location where a token was extracted from in an incoming request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchedSource<'s> {
    Header(&'s HeaderName),
    Query(&'s str),
}

/// Method and location to extract the API key token from incoming requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeySource {
    Header { name: HeaderName, prefix: String },
    Query { param: String },
}

impl KeySource {
    /// Extracts token from a single source specification (returns owned HeaderName for backward compatibility).
    #[inline]
    pub fn extract<'a>(&self, req: &'a L7Request) -> Option<(&'a str, Option<HeaderName>)> {
        match self.extract_ref(req) {
            Some((token, MatchedSource::Header(hdr))) => Some((token, Some(hdr.clone()))),
            Some((token, MatchedSource::Query(_))) => Some((token, None)),
            None => None,
        }
    }

    /// Fast, zero-allocation token extraction returning a reference to the matched header or query param.
    #[inline]
    pub fn extract_ref<'s, 'r>(
        &'s self,
        req: &'r L7Request,
    ) -> Option<(&'r str, MatchedSource<'s>)> {
        match self {
            Self::Header { name, prefix } => {
                for val in req.headers.get_all(name) {
                    if let Ok(s) = val.to_str() {
                        let trimmed = s.trim();
                        if prefix.is_empty() {
                            if !trimmed.is_empty() {
                                return Some((trimmed, MatchedSource::Header(name)));
                            }
                        } else {
                            let pfx_bytes = prefix.as_bytes();
                            let trimmed_bytes = trimmed.as_bytes();
                            if trimmed_bytes.len() > pfx_bytes.len()
                                && trimmed_bytes[..pfx_bytes.len()].eq_ignore_ascii_case(pfx_bytes)
                            {
                                let token = trimmed[pfx_bytes.len()..].trim();
                                if !token.is_empty() {
                                    return Some((token, MatchedSource::Header(name)));
                                }
                            }
                        }
                    }
                }
                None
            }
            Self::Query { param } => {
                if let Some(query) = req.uri.query() {
                    for pair in query.split('&') {
                        if let Some((k, v)) = pair.split_once('=')
                            && k == param.as_str()
                        {
                            let token = v.trim();
                            if !token.is_empty() {
                                return Some((token, MatchedSource::Query(param.as_str())));
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

/// Fast token extraction across sources avoiding HeaderName cloning and decoupled lifetimes.
#[inline]
pub fn extract_token_ref<'s, 'r>(
    sources: &'s [KeySource],
    req: &'r L7Request,
) -> Option<(&'r str, MatchedSource<'s>)> {
    for source in sources {
        if let Some(res) = source.extract_ref(req) {
            return Some(res);
        }
    }
    None
}

/// Pre-compiled metadata and headers to inject for an authorized consumer.
#[derive(Clone, Debug)]
pub struct ApiKeyEntry {
    pub key: String,
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
    fn test_extract_header_multiple_values() {
        let source = KeySource::Header {
            name: http::header::AUTHORIZATION,
            prefix: "ApiKey ".into(),
        };

        let mut headers = HeaderMap::new();
        headers.append(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer other_tok"),
        );
        headers.append(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("ApiKey valid_tok_456"),
        );
        let req = L7Request::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            headers,
            Body::Empty,
        );

        let (token, hdr) = source.extract(&req).unwrap();
        assert_eq!(token, "valid_tok_456");
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
