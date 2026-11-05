//! Pre-Trained RFC 8878 Zstandard Dictionaries & Configuration Models.
//!
//! Provides statically embedded built-in dictionaries (e.g. `common_json_rest.dict`)
//! as well as mechanisms to load external custom Base64-encoded dictionaries.

use bytes::Bytes;

/// Pre-trained, 16KB built-in Zstandard dictionary for common REST/JSON schemas (RFC 8878).
///
/// Contains shared tokens, keys, and structures for typical API responses
/// (`status`, `code`, `meta`, `pagination`, `data`, `id`, `created_at`, `error`, etc.).
/// Yields 2x to 3x higher compression ratio for small/medium JSON payloads (< 2KB).
pub const BUILTIN_COMMON_JSON_REST_DICT: &[u8] =
    include_bytes!("dictionaries/common_json_rest.dict");

/// Available built-in pre-trained Zstandard dictionaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinDictionary {
    /// 16KB pre-trained dictionary for common REST/JSON schemas.
    CommonJsonRest,
}

impl BuiltinDictionary {
    /// Returns the static byte slice for this built-in dictionary.
    #[inline(always)]
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            Self::CommonJsonRest => BUILTIN_COMMON_JSON_REST_DICT,
        }
    }

    /// Resolves an identifier string (case-insensitive) to a built-in dictionary if matching.
    pub fn from_name(name: &str) -> Option<Self> {
        let trimmed = name.trim();
        if trimmed.eq_ignore_ascii_case("common_json_rest")
            || trimmed.eq_ignore_ascii_case("built-in:common_json_rest")
            || trimmed.eq_ignore_ascii_case("default")
        {
            Some(Self::CommonJsonRest)
        } else {
            None
        }
    }

    /// Canonical identifier string for this built-in dictionary.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CommonJsonRest => "common_json_rest",
        }
    }
}

/// Zstandard dictionary configuration source.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ZstdDictionaryConfig {
    /// No dictionary is used.
    #[default]
    None,
    /// Use a statically embedded built-in dictionary.
    Builtin(BuiltinDictionary),
    /// Custom dictionary loaded from external configuration (e.g. Base64 payload or raw bytes).
    Custom(Bytes),
}

impl ZstdDictionaryConfig {
    /// Returns the dictionary byte slice, if any is configured.
    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::None => None,
            Self::Builtin(b) => Some(b.as_bytes()),
            Self::Custom(bytes) => Some(bytes.as_ref()),
        }
    }

    /// Whether a dictionary is configured.
    #[inline]
    pub fn is_some(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// Loads dictionary configuration from an optional string token or Base64 payload.
    pub fn from_optional_str(s: Option<&str>) -> Self {
        let raw = match s {
            Some(v) => v.trim(),
            None => return Self::None,
        };
        if raw.is_empty() || raw.eq_ignore_ascii_case("null") {
            return Self::None;
        }

        // 1. Check for built-in dictionary token
        if let Some(builtin) = BuiltinDictionary::from_name(raw) {
            return Self::Builtin(builtin);
        }

        // 2. Decode Base64 or fallback to raw bytes
        use base64::Engine;
        if let Ok(decoded) = base64::prelude::BASE64_STANDARD.decode(raw) {
            Self::Custom(Bytes::from(decoded))
        } else {
            Self::Custom(Bytes::copy_from_slice(raw.as_bytes()))
        }
    }
}

impl From<Option<Bytes>> for ZstdDictionaryConfig {
    fn from(opt: Option<Bytes>) -> Self {
        match opt {
            Some(b) => Self::Custom(b),
            None => Self::None,
        }
    }
}

impl From<BuiltinDictionary> for ZstdDictionaryConfig {
    fn from(b: BuiltinDictionary) -> Self {
        Self::Builtin(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_dictionary_integrity() {
        assert_eq!(BUILTIN_COMMON_JSON_REST_DICT.len(), 16384);
        assert_eq!(
            BuiltinDictionary::CommonJsonRest.as_bytes(),
            BUILTIN_COMMON_JSON_REST_DICT
        );
        assert_eq!(
            BuiltinDictionary::from_name("common_json_rest"),
            Some(BuiltinDictionary::CommonJsonRest)
        );
        assert_eq!(
            BuiltinDictionary::from_name("DEFAULT"),
            Some(BuiltinDictionary::CommonJsonRest)
        );
        assert_eq!(
            BuiltinDictionary::from_name("built-in:common_json_rest"),
            Some(BuiltinDictionary::CommonJsonRest)
        );
        assert_eq!(BuiltinDictionary::from_name("unknown_dict"), None);
    }

    #[test]
    fn test_dictionary_config_parsing() {
        assert_eq!(
            ZstdDictionaryConfig::from_optional_str(None),
            ZstdDictionaryConfig::None
        );
        assert_eq!(
            ZstdDictionaryConfig::from_optional_str(Some("")),
            ZstdDictionaryConfig::None
        );
        assert_eq!(
            ZstdDictionaryConfig::from_optional_str(Some("null")),
            ZstdDictionaryConfig::None
        );

        let builtin = ZstdDictionaryConfig::from_optional_str(Some("common_json_rest"));
        assert_eq!(
            builtin,
            ZstdDictionaryConfig::Builtin(BuiltinDictionary::CommonJsonRest)
        );
        assert_eq!(builtin.as_bytes(), Some(BUILTIN_COMMON_JSON_REST_DICT));

        use base64::Engine;
        let sample = b"custom test dictionary bytes";
        let b64 = base64::prelude::BASE64_STANDARD.encode(sample);
        let custom = ZstdDictionaryConfig::from_optional_str(Some(&b64));
        assert_eq!(custom.as_bytes(), Some(&sample[..]));
    }
}
