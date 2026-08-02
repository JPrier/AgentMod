//! Versioned plugin manifest types, parsing, and deterministic validation.
//!
//! This crate validates declarations only. It deliberately contains no plugin
//! loading, process launch, runtime state access, or plugin-host behavior.

mod model;
mod parsing;
mod validation;

pub use model::{
    AuthorityManifest, AuthorityTarget, CompactorManifest, ConfigurationSchemaMetadata,
    ConfigurationSchemaSource, ContextTransformIdempotency, ContextTransformLifecycle,
    ContextTransformManifest, Entrypoint, FailurePolicy, IsolationMode, MemoryProviderManifest,
    MemoryRetrieveManifest, MemoryWriteManifest, NodeExecutorIdempotency, NodeExecutorManifest,
    OrderingManifest, PermissionManifest, PluginCategory, PluginClassification, PluginIdentity,
    PluginManifest, PluginOperationIdempotency, PluginScope, TrustLevel,
};
pub use parsing::{ManifestFormat, ManifestParseError, parse_json, parse_toml, to_json, to_toml};
pub use validation::{
    CURRENT_MANIFEST_SCHEMA_VERSION, Diagnostic, DiagnosticSeverity, ValidatedPlugin,
    ValidationContext, ValidationReport, validate_manifest, validate_plugin_set,
};
