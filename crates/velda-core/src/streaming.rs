//! Directional streaming configuration for L7 application protocols.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Directional streaming mode for L7 application protocols (HTTP/2, HTTP/3, gRPC).
///
/// Configures whether request payloads, response payloads, or both are streamed
/// progressively without full in-memory buffering.
///
/// Configured via an array of directional streams in JSON:
/// 1. `[]` / `"disable"`: Full in-memory buffering for both request and response (REST standard, maximum WAF inspection).
/// 2. `["server"]`: Server streaming (Request buffered for WAF/auth; Response streamed progressively for SSE, LLM tokens, large downloads).
/// 3. `["client"]`: Client streaming (Request streamed for large uploads; Response buffered).
/// 4. `["client", "server"]`: Both client and server stream concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StreamingMode {
    /// Client-to-server request body streaming.
    pub client: bool,
    /// Server-to-client response body streaming.
    pub server: bool,
}

impl StreamingMode {
    /// Streaming is disabled: both client and server streaming are false.
    pub const DISABLED: Self = Self {
        client: false,
        server: false,
    };
    /// Server streaming enabled, client streaming disabled.
    pub const SERVER: Self = Self {
        client: false,
        server: true,
    };
    /// Client streaming enabled, server streaming disabled.
    pub const CLIENT: Self = Self {
        client: true,
        server: false,
    };
    /// Both client and server streaming enabled (full-duplex).
    pub const DUPLEX: Self = Self {
        client: true,
        server: true,
    };

    /// Creates a new `StreamingMode` with specified client and server streaming flags.
    #[inline]
    pub const fn new(client: bool, server: bool) -> Self {
        Self { client, server }
    }

    /// Creates a `StreamingMode` with both client and server streaming disabled.
    #[inline]
    pub const fn disabled() -> Self {
        Self::DISABLED
    }

    /// Creates a `StreamingMode` with only server streaming enabled.
    #[inline]
    pub const fn server_only() -> Self {
        Self::SERVER
    }

    /// Creates a `StreamingMode` with only client streaming enabled.
    #[inline]
    pub const fn client_only() -> Self {
        Self::CLIENT
    }

    /// Creates a `StreamingMode` with both client and server streaming enabled.
    #[inline]
    pub const fn both() -> Self {
        Self {
            client: true,
            server: true,
        }
    }

    /// Parses a streaming mode from a string identifier.
    ///
    /// Note: Only `"disable"`, `"server"`, and `"client"` are supported as single strings.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "disable" => Some(Self::DISABLED),
            "server" => Some(Self::SERVER),
            "client" => Some(Self::CLIENT),
            _ => None,
        }
    }

    /// Returns `true` if server response streaming is enabled.
    #[inline]
    pub const fn is_server_streaming(&self) -> bool {
        self.server
    }

    /// Returns `true` if client request streaming is enabled.
    #[inline]
    pub const fn is_client_streaming(&self) -> bool {
        self.client
    }

    /// Returns `true` if any streaming is enabled (client or server).
    #[inline]
    pub const fn is_streaming_enabled(&self) -> bool {
        self.client || self.server
    }

    /// Returns `true` if both client and server streaming are enabled concurrently.
    #[inline]
    pub const fn is_bidirectional(&self) -> bool {
        self.client && self.server
    }

    /// Returns the canonical string representation of this mode.
    #[inline]
    pub const fn as_str(&self) -> &'static str {
        match (self.client, self.server) {
            (false, false) => "disable",
            (false, true) => "server",
            (true, false) => "client",
            (true, true) => "client,server",
        }
    }
}

impl Serialize for StreamingMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            use serde::ser::SerializeSeq;
            match (self.client, self.server) {
                (false, false) => {
                    let seq = serializer.serialize_seq(Some(0))?;
                    seq.end()
                }
                (false, true) => {
                    let mut seq = serializer.serialize_seq(Some(1))?;
                    seq.serialize_element("server")?;
                    seq.end()
                }
                (true, false) => {
                    let mut seq = serializer.serialize_seq(Some(1))?;
                    seq.serialize_element("client")?;
                    seq.end()
                }
                (true, true) => {
                    let mut seq = serializer.serialize_seq(Some(2))?;
                    seq.serialize_element("client")?;
                    seq.serialize_element("server")?;
                    seq.end()
                }
            }
        } else {
            // Binary serialization: pack into 1 byte (bit 1 = client, bit 0 = server)
            let tag: u8 = ((self.client as u8) << 1) | (self.server as u8);
            serializer.serialize_u8(tag)
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
                formatter.write_str(
                    "'disable', [], or an array of streaming directions: ['server'], ['client'], ['client', 'server']",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<StreamingMode, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut has_server = false;
                let mut has_client = false;

                while let Some(item) = seq.next_element::<String>()? {
                    match item.trim().to_ascii_lowercase().as_str() {
                        "server" => has_server = true,
                        "client" => has_client = true,
                        "disable" => {}
                        other => {
                            return Err(serde::de::Error::custom(format!(
                                "unknown streaming direction '{other}', expected 'client' or 'server'",
                            )));
                        }
                    }
                }

                Ok(StreamingMode {
                    client: has_client,
                    server: has_server,
                })
            }

            fn visit_bool<E>(self, _value: bool) -> Result<StreamingMode, E>
            where
                E: serde::de::Error,
            {
                Err(serde::de::Error::custom(
                    "boolean is not supported for streaming mode; specify 'disable' or an array such as ['server'], ['client'], or ['client', 'server']",
                ))
            }

            fn visit_u64<E>(self, value: u64) -> Result<StreamingMode, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0 => Ok(StreamingMode::DISABLED),
                    1 => Ok(StreamingMode::SERVER),
                    2 => Ok(StreamingMode::CLIENT),
                    3 => Ok(StreamingMode {
                        client: true,
                        server: true,
                    }),
                    other => Err(serde::de::Error::custom(format!(
                        "invalid streaming mode tag: {other}"
                    ))),
                }
            }

            fn visit_str<E>(self, value: &str) -> Result<StreamingMode, E>
            where
                E: serde::de::Error,
            {
                match value.trim().to_ascii_lowercase().as_str() {
                    "disable" => Ok(StreamingMode::DISABLED),
                    "server" => Ok(StreamingMode::SERVER),
                    "client" => Ok(StreamingMode::CLIENT),
                    other => Err(serde::de::Error::custom(format!(
                        "unknown streaming mode '{other}', expected array ['client', 'server'], ['server'], ['client'], or 'disable'"
                    ))),
                }
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
            StreamingMode::parse("disable"),
            Some(StreamingMode::DISABLED)
        );
        assert_eq!(StreamingMode::parse("server"), Some(StreamingMode::SERVER));
        assert_eq!(StreamingMode::parse("client"), Some(StreamingMode::CLIENT));

        // Unknown/removed/invalid values rejected
        assert_eq!(StreamingMode::parse("disabled"), None);
        assert_eq!(StreamingMode::parse("false"), None);
        assert_eq!(StreamingMode::parse("true"), None);
        assert_eq!(StreamingMode::parse("response"), None);
        assert_eq!(StreamingMode::parse("request"), None);
        assert_eq!(StreamingMode::parse("random"), None);
    }

    #[test]
    fn test_streaming_mode_predicates() {
        assert!(!StreamingMode::DISABLED.is_streaming_enabled());
        assert!(!StreamingMode::DISABLED.is_server_streaming());
        assert!(!StreamingMode::DISABLED.is_client_streaming());
        assert!(!StreamingMode::DISABLED.is_bidirectional());

        assert!(StreamingMode::SERVER.is_streaming_enabled());
        assert!(StreamingMode::SERVER.is_server_streaming());
        assert!(!StreamingMode::SERVER.is_client_streaming());
        assert!(!StreamingMode::SERVER.is_bidirectional());

        assert!(StreamingMode::CLIENT.is_streaming_enabled());
        assert!(!StreamingMode::CLIENT.is_server_streaming());
        assert!(StreamingMode::CLIENT.is_client_streaming());
        assert!(!StreamingMode::CLIENT.is_bidirectional());

        let both = StreamingMode::new(true, true);
        assert!(both.is_streaming_enabled());
        assert!(both.is_server_streaming());
        assert!(both.is_client_streaming());
        assert!(both.is_bidirectional());
    }

    #[test]
    fn test_streaming_mode_json_serde() {
        // Deserialization from JSON arrays
        let m_empty: StreamingMode = serde_json::from_str("[]").unwrap();
        assert_eq!(m_empty, StreamingMode::DISABLED);

        let m_disable_arr: StreamingMode = serde_json::from_str("[\"disable\"]").unwrap();
        assert_eq!(m_disable_arr, StreamingMode::DISABLED);

        let m_server_arr: StreamingMode = serde_json::from_str("[\"server\"]").unwrap();
        assert_eq!(m_server_arr, StreamingMode::SERVER);

        let m_client_arr: StreamingMode = serde_json::from_str("[\"client\"]").unwrap();
        assert_eq!(m_client_arr, StreamingMode::CLIENT);

        let m_both_arr: StreamingMode = serde_json::from_str("[\"client\", \"server\"]").unwrap();
        assert_eq!(m_both_arr, StreamingMode::new(true, true));

        let m_both_arr_rev: StreamingMode =
            serde_json::from_str("[\"server\", \"client\"]").unwrap();
        assert_eq!(m_both_arr_rev, StreamingMode::new(true, true));

        // String forms: strictly 'disable', 'server', 'client'
        let m_disable: StreamingMode = serde_json::from_str("\"disable\"").unwrap();
        assert_eq!(m_disable, StreamingMode::DISABLED);

        let m_server: StreamingMode = serde_json::from_str("\"server\"").unwrap();
        assert_eq!(m_server, StreamingMode::SERVER);

        let m_client: StreamingMode = serde_json::from_str("\"client\"").unwrap();
        assert_eq!(m_client, StreamingMode::CLIENT);

        // Strict rejection: aliases and unknown directions
        assert!(serde_json::from_str::<StreamingMode>("\"disabled\"").is_err());
        assert!(serde_json::from_str::<StreamingMode>("[\"disabled\"]").is_err());
        assert!(serde_json::from_str::<StreamingMode>("false").is_err());
        assert!(serde_json::from_str::<StreamingMode>("true").is_err());
        assert!(serde_json::from_str::<StreamingMode>("\"false\"").is_err());
        assert!(serde_json::from_str::<StreamingMode>("[\"false\"]").is_err());
        assert!(serde_json::from_str::<StreamingMode>("[\"unknown\"]").is_err());

        // Serialization to JSON produces arrays
        assert_eq!(
            serde_json::to_string(&StreamingMode::DISABLED).unwrap(),
            "[]"
        );
        assert_eq!(
            serde_json::to_string(&StreamingMode::SERVER).unwrap(),
            "[\"server\"]"
        );
        assert_eq!(
            serde_json::to_string(&StreamingMode::CLIENT).unwrap(),
            "[\"client\"]"
        );
        assert_eq!(
            serde_json::to_string(&StreamingMode::new(true, true)).unwrap(),
            "[\"client\",\"server\"]"
        );
    }
}
