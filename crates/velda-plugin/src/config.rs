//! Declarative Plugin Configuration definition.

use serde::{Deserialize, Serialize};

/// Generic configuration container for a loaded plugin instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginConfig {
    pub id: String,
    #[serde(default)]
    pub phase: String, // "pre_route", "pre_upstream", "post_response", "background"
    #[serde(with = "json_value_bincode")]
    pub config: serde_json::Value,
}

mod json_value_bincode {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &serde_json::Value, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            value.serialize(serializer)
        } else {
            let json_str = value.to_string();
            serializer.serialize_str(&json_str)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<serde_json::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            serde_json::Value::deserialize(deserializer)
        } else {
            let s = String::deserialize(deserializer)?;
            serde_json::from_str(&s).map_err(serde::de::Error::custom)
        }
    }
}
