use std::fmt;

use crate::PluginManifest;

/// Supported human-editable manifest format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestFormat {
    /// TOML.
    Toml,
    /// JSON.
    Json,
}

/// Parsing or serialization failure with no parser implementation type exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestParseError {
    /// Input/output format.
    pub format: ManifestFormat,
    /// Human-readable parser diagnostic.
    pub message: String,
}

impl fmt::Display for ManifestParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:?} plugin manifest is invalid: {}",
            self.format, self.message
        )
    }
}

impl std::error::Error for ManifestParseError {}

/// Parses a strict TOML plugin manifest.
///
/// # Errors
///
/// Returns a format-owned diagnostic for malformed input or unknown fields.
pub fn parse_toml(input: &str) -> Result<PluginManifest, ManifestParseError> {
    toml::from_str(input).map_err(|error| ManifestParseError {
        format: ManifestFormat::Toml,
        message: error.to_string(),
    })
}

/// Parses a strict JSON plugin manifest.
///
/// # Errors
///
/// Returns a format-owned diagnostic for malformed input or unknown fields.
pub fn parse_json(input: &str) -> Result<PluginManifest, ManifestParseError> {
    serde_json::from_str(input).map_err(|error| ManifestParseError {
        format: ManifestFormat::Json,
        message: error.to_string(),
    })
}

/// Serializes a manifest to deterministic pretty TOML.
///
/// # Errors
///
/// Returns a format-owned diagnostic if the owned model cannot be represented.
pub fn to_toml(manifest: &PluginManifest) -> Result<String, ManifestParseError> {
    toml::to_string_pretty(manifest).map_err(|error| ManifestParseError {
        format: ManifestFormat::Toml,
        message: error.to_string(),
    })
}

/// Serializes a manifest to deterministic pretty JSON.
///
/// # Errors
///
/// Returns a format-owned diagnostic if the owned model cannot be represented.
pub fn to_json(manifest: &PluginManifest) -> Result<String, ManifestParseError> {
    serde_json::to_string_pretty(manifest).map_err(|error| ManifestParseError {
        format: ManifestFormat::Json,
        message: error.to_string(),
    })
}
