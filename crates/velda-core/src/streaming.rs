//! Directional streaming configuration and mode enumerations for L7 application protocols.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Directional streaming mode for L7 application protocols (HTTP/2, HTTP/3, gRPC).
///
/// Configures whether request payloads, response payloads, or both are streamed
/// progressively without full in-memory buffering.
///
/// Supported modes:
/// 1. `false` / `Disabled`: Full in-memory buffering for both request and response (REST standard, maximum WAF inspection).
/// 2. `"server"`: Server streaming (Request buffered for WAF/auth; Response streamed progressively for SSE, LLM tokens, large downloads).
/// 3. `"client"`: Client streaming (Request streamed for large uploads; Response buffered).
/// 4. `"duplex"`: Full-duplex bidirectional streaming (both request and response stream concurrently).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum StreamingMode {
    /// Streaming is disabled. Payloads are buffered and validated in full.
    #[default]
    Disabled = 0,
    /// Server streaming: Request is buffered and validated; Response is streamed progressively.
    Server = 1,
    /// Client streaming: Request is streamed progressively; Response is buffered.
    Client = 2,
    /// Full-Duplex bidirectional streaming: Both request and response stream concurrently.
    Duplex = 3,
}

impl StreamingMode {
    /// Parses a streaming mode from a string identifier or boolean equivalent.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "false" | "disabled" => Some(Self::Disabled),
            "server" => Some(Self::Server),
            "client" => Some(Self::Client),
            "duplex" => Some(Self::Duplex),
            _ => None,
        }
    }

    /// Returns `true` if server response streaming is enabled (`Server` or `Duplex`).
    #[inline]
    pub const fn is_server_streaming(&self) -> bool {
        matches!(self, Self::Server | Self::Duplex)
    }

    /// Returns `true` if client request streaming is enabled (`Client` or `Duplex`).
    #[inline]
    pub const fn is_client_streaming(&self) -> bool {
        matches!(self, Self::Client | Self::Duplex)
    }

    /// Returns `true` if any streaming is enabled (not `Disabled`).
    #[inline]
    pub const fn is_streaming_enabled(&self) -> bool {
        !matches!(self, Self::Disabled)
    }

    /// Returns the canonical string representation of this mode.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Server => "server",
            Self::Client => "client",
            Self::Duplex => "duplex",
        }
    }
}

impl Serialize for StreamingMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(self.as_str())
        } else {
            serializer.serialize_u8(*self as u8)
        }
    }
}

impl<'de> Deserialize<'de> for StreamingMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StreamingModeVisitor;

        impl<'de> serde::de::Visitor<'de> for StreamingModeVisitor {
            type Value = StreamingMode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter
                    .write_str("false or a streaming mode string: 'server', 'client', 'duplex'")
            }

            fn visit_bool<E>(self, value: bool) -> Result<StreamingMode, E>
            where
                E: serde::de::Error,
            {
                if !value {
                    Ok(StreamingMode::Disabled)
                } else {
                    Err(serde::de::Error::custom(
                        "boolean 'true' is ambiguous for streaming mode; specify 'server', 'client', or 'duplex'",
                    ))
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<StreamingMode, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0 => Ok(StreamingMode::Disabled),
                    1 => Ok(StreamingMode::Server),
                    2 => Ok(StreamingMode::Client),
                    3 => Ok(StreamingMode::Duplex),
                    other => Err(serde::de::Error::custom(format!(
                        "invalid streaming mode tag: {other}"
                    ))),
                }
            }

            fn visit_str<E>(self, value: &str) -> Result<StreamingMode, E>
            where
                E: serde::de::Error,
            {
                StreamingMode::parse(value).ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        "unknown streaming mode '{value}', expected one of: false, 'server', 'client', 'duplex'"
                    ))
                })
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_any(StreamingModeVisitor)
        } else {
            deserializer.deserialize_u8(StreamingModeVisitor)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_mode_parse_canonical() {
        assert_eq!(
            StreamingMode::parse("disabled"),
            Some(StreamingMode::Disabled)
        );
        assert_eq!(StreamingMode::parse("false"), Some(StreamingMode::Disabled));
        assert_eq!(StreamingMode::parse("server"), Some(StreamingMode::Server));
        assert_eq!(StreamingMode::parse("client"), Some(StreamingMode::Client));
        assert_eq!(StreamingMode::parse("duplex"), Some(StreamingMode::Duplex));

        // Unknown/legacy values rejected
        assert_eq!(StreamingMode::parse("true"), None);
        assert_eq!(StreamingMode::parse("response"), None);
        assert_eq!(StreamingMode::parse("request"), None);
        assert_eq!(StreamingMode::parse("bidirectional"), None);
        assert_eq!(StreamingMode::parse("random"), None);
    }

    #[test]
    fn test_streaming_mode_predicates() {
        assert!(!StreamingMode::Disabled.is_streaming_enabled());
        assert!(!StreamingMode::Disabled.is_server_streaming());
        assert!(!StreamingMode::Disabled.is_client_streaming());

        assert!(StreamingMode::Server.is_streaming_enabled());
        assert!(StreamingMode::Server.is_server_streaming());
        assert!(!StreamingMode::Server.is_client_streaming());

        assert!(StreamingMode::Client.is_streaming_enabled());
        assert!(!StreamingMode::Client.is_server_streaming());
        assert!(StreamingMode::Client.is_client_streaming());

        assert!(StreamingMode::Duplex.is_streaming_enabled());
        assert!(StreamingMode::Duplex.is_server_streaming());
        assert!(StreamingMode::Duplex.is_client_streaming());
    }

    #[test]
    fn test_streaming_mode_json_serde() {
        // Deserialization from JSON
        let m_false: StreamingMode = serde_json::from_str("false").unwrap();
        assert_eq!(m_false, StreamingMode::Disabled);

        let m_disabled: StreamingMode = serde_json::from_str("\"disabled\"").unwrap();
        assert_eq!(m_disabled, StreamingMode::Disabled);

        let m_server: StreamingMode = serde_json::from_str("\"server\"").unwrap();
        assert_eq!(m_server, StreamingMode::Server);

        let m_client: StreamingMode = serde_json::from_str("\"client\"").unwrap();
        assert_eq!(m_client, StreamingMode::Client);

        let m_duplex: StreamingMode = serde_json::from_str("\"duplex\"").unwrap();
        assert_eq!(m_duplex, StreamingMode::Duplex);

        // Ambiguous boolean 'true' rejected
        assert!(serde_json::from_str::<StreamingMode>("true").is_err());

        // Serialization to JSON
        assert_eq!(
            serde_json::to_string(&StreamingMode::Disabled).unwrap(),
            "\"disabled\""
        );
        assert_eq!(
            serde_json::to_string(&StreamingMode::Server).unwrap(),
            "\"server\""
        );
        assert_eq!(
            serde_json::to_string(&StreamingMode::Client).unwrap(),
            "\"client\""
        );
        assert_eq!(
            serde_json::to_string(&StreamingMode::Duplex).unwrap(),
            "\"duplex\""
        );
    }
}
