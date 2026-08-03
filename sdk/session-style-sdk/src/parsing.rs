use std::fmt;

use crate::SessionStyleManifest;

/// Supported human-editable style format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManifestFormat {
    /// TOML.
    Toml,
    /// JSON.
    Json,
}

/// Parsing or serialization failure with parser implementation types hidden.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestParseError {
    /// Input or output format.
    pub format: ManifestFormat,
    /// Redacted parser diagnostic.
    pub message: String,
}

impl fmt::Display for ManifestParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:?} session-style manifest is invalid: {}",
            self.format, self.message
        )
    }
}

impl std::error::Error for ManifestParseError {}

/// Parses strict TOML.
///
/// # Errors
///
/// Returns a format-owned error for malformed input or unknown fields.
pub fn parse_toml(input: &str) -> Result<SessionStyleManifest, ManifestParseError> {
    toml::from_str(input).map_err(|error| ManifestParseError {
        format: ManifestFormat::Toml,
        message: error.to_string(),
    })
}

/// Parses strict JSON.
///
/// # Errors
///
/// Returns a format-owned error for malformed input or unknown fields.
pub fn parse_json(input: &str) -> Result<SessionStyleManifest, ManifestParseError> {
    serde_json::from_str(input).map_err(|error| ManifestParseError {
        format: ManifestFormat::Json,
        message: error.to_string(),
    })
}

/// Serializes deterministic pretty TOML.
///
/// # Errors
///
/// Returns a format-owned serialization error.
pub fn to_toml(manifest: &SessionStyleManifest) -> Result<String, ManifestParseError> {
    toml::to_string_pretty(manifest).map_err(|error| ManifestParseError {
        format: ManifestFormat::Toml,
        message: error.to_string(),
    })
}

/// Serializes deterministic pretty JSON.
///
/// # Errors
///
/// Returns a format-owned serialization error.
pub fn to_json(manifest: &SessionStyleManifest) -> Result<String, ManifestParseError> {
    serde_json::to_string_pretty(manifest).map_err(|error| ManifestParseError {
        format: ManifestFormat::Json,
        message: error.to_string(),
    })
}
