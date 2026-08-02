//! Versioned plugin manifest types, parsing, and deterministic validation.
//!
//! This crate validates declarations only. It deliberately contains no plugin
//! loading, process launch, runtime state access, or plugin-host behavior.

mod model;
mod parsing;
mod validation;

pub use model::{
    AuthorityManifest, AuthorityTarget, ConfigurationSchemaMetadata, ConfigurationSchemaSource,
    Entrypoint, FailurePolicy, IsolationMode, OrderingManifest, PermissionManifest, PluginCategory,
    PluginClassification, PluginCompactionDeclaration, PluginContextTransformBoundary,
    PluginContextTransformDeclaration, PluginIdentity, PluginManifest, PluginMemoryDeclaration,
    PluginNodeExecutor, PluginObserverDelivery, PluginScope, TrustLevel,
};
pub use parsing::{ManifestFormat, ManifestParseError, parse_json, parse_toml, to_json, to_toml};
pub use validation::{
    CURRENT_MANIFEST_SCHEMA_VERSION, Diagnostic, DiagnosticSeverity, ValidatedPlugin,
    ValidationContext, ValidationReport, validate_manifest, validate_plugin_set,
};
