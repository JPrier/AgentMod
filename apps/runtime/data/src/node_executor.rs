//! Normalized runtime node-executor capability records.
//!
//! This data set contains capability metadata only. Executable behavior remains
//! in runtime logic (or behind an approved plugin invocation boundary); graph
//! logic never receives dependency implementations.

use std::{collections::BTreeSet, sync::Arc};

use agentmod_primitives::ContentHash;
use thiserror::Error;

const MAX_REGISTRATIONS: usize = 256;
const MAX_IDENTIFIER_BYTES: usize = 128;

/// Data-owned registration source.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NodeExecutorSourceData {
    /// Runtime-owned implementation assembled by the composition root.
    Runtime,
    /// Plugin-owned implementation advertised by an activated plugin.
    Plugin {
        /// Exact plugin identity allowed to provide the implementation.
        plugin_id: String,
    },
}

/// Data-owned execution boundary classification.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NodeExecutorBoundaryData {
    /// Runtime logic owns the complete implementation.
    RuntimeLogic,
    /// Execution requires an isolated plugin invocation.
    PluginHost,
}

/// Composition-root registration before data normalization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterNodeExecutorDataRecord {
    /// Stable implementation ID.
    pub id: String,
    /// Exact implementation semantic version.
    pub version: String,
    /// Semantic-version requirement for the runtime API.
    pub runtime_api: String,
    /// Serialized graph node kind supported by this implementation.
    pub node_kind: String,
    /// Business capabilities supported by this implementation.
    pub capabilities: BTreeSet<String>,
    /// Registration source.
    pub source: NodeExecutorSourceData,
    /// Process boundary used for execution.
    pub boundary: NodeExecutorBoundaryData,
    /// Whether the implementation may be selected for new work.
    pub available: bool,
    /// Hash of the exact executor declaration/configuration.
    pub declaration_hash: ContentHash,
}

/// Normalized immutable data record consumed by runtime logic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecutorDataRecord {
    /// Stable implementation ID.
    pub id: String,
    /// Exact implementation semantic version.
    pub version: String,
    /// Semantic-version requirement for the runtime API.
    pub runtime_api: String,
    /// Serialized graph node kind.
    pub node_kind: String,
    /// Sorted business capabilities.
    pub capabilities: BTreeSet<String>,
    /// Registration source.
    pub source: NodeExecutorSourceData,
    /// Process boundary used for execution.
    pub boundary: NodeExecutorBoundaryData,
    /// Whether the implementation may be selected for new work.
    pub available: bool,
    /// Hash of the exact executor declaration/configuration.
    pub declaration_hash: ContentHash,
}

/// Data-layer list request.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ListNodeExecutorsDataRequest;

/// Narrow capability data interface consumed by runtime logic.
pub trait NodeExecutorDataPort {
    /// Lists a stable snapshot of normalized registrations.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorDataError`] when the registry was not assembled
    /// or its composition-root registrations were invalid.
    fn list_node_executors(
        &self,
        request: ListNodeExecutorsDataRequest,
    ) -> Result<Vec<NodeExecutorDataRecord>, NodeExecutorDataError>;
}

/// Immutable normalized node-executor registry.
#[derive(Clone, Debug)]
pub struct RuntimeNodeExecutorData {
    records: Arc<Vec<NodeExecutorDataRecord>>,
}

impl RuntimeNodeExecutorData {
    /// Validates, normalizes, and sorts composition-root registrations.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorDataError`] for invalid fields, excessive
    /// registrations, or duplicate implementation identities.
    pub fn new(
        registrations: Vec<RegisterNodeExecutorDataRecord>,
    ) -> Result<Self, NodeExecutorDataError> {
        if registrations.is_empty() || registrations.len() > MAX_REGISTRATIONS {
            return Err(NodeExecutorDataError::InvalidRegistrationCount);
        }
        let mut records = registrations
            .into_iter()
            .map(normalize)
            .collect::<Result<Vec<_>, _>>()?;
        records.sort_by(|left, right| {
            left.node_kind
                .cmp(&right.node_kind)
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.version.cmp(&right.version))
        });
        if records.windows(2).any(|pair| {
            pair[0].node_kind == pair[1].node_kind
                && pair[0].id == pair[1].id
                && pair[0].version == pair[1].version
        }) {
            return Err(NodeExecutorDataError::DuplicateRegistration);
        }
        Ok(Self {
            records: Arc::new(records),
        })
    }

    /// Returns the exact first-party registry assembled by the native runtime
    /// composition root.
    ///
    /// Availability is only the first gate: runtime logic still validates each
    /// graph's typed semantics and rejects unsupported executor combinations.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorDataError`] only if the checked-in first-party
    /// declarations are internally inconsistent.
    pub fn native() -> Result<Self, NodeExecutorDataError> {
        const IMPLEMENTATIONS: &[(&str, &str, &[&str], bool)] = &[
            (
                "context_transform",
                "runtime.context-construction",
                &["context"],
                true,
            ),
            ("model_call", "runtime.model-request", &["model"], true),
            (
                "user_approval",
                "runtime.user-approval",
                &["approval"],
                true,
            ),
            (
                "spawn_child_agent",
                "runtime.child-spawn",
                &["agents"],
                true,
            ),
            (
                "send_child_agent_message",
                "runtime.child-message",
                &["agents"],
                true,
            ),
            ("wait_for_agents", "runtime.child-wait", &["agents"], true),
            ("join_results", "runtime.join", &["agents"], true),
            ("review", "runtime.review", &["model"], true),
            ("loop", "runtime.loop", &[], true),
            ("conditional_branch", "runtime.conditional", &[], true),
            ("parallel_branch", "runtime.parallel", &[], true),
            ("delay", "runtime.delay", &["scheduling"], true),
            ("schedule", "runtime.schedule", &["scheduling"], true),
            ("emit_event", "runtime.event-emission", &["events"], true),
            (
                "persist_artifact",
                "runtime.artifact-persistence",
                &["artifacts"],
                true,
            ),
            ("complete_turn", "runtime.turn-completion", &[], true),
            ("complete_session", "runtime.session-completion", &[], true),
            ("fail", "runtime.structured-failure", &[], true),
        ];
        let mut registrations = IMPLEMENTATIONS
            .iter()
            .map(|(node_kind, id, capabilities, available)| {
                native_registration(node_kind, id, capabilities, "1.0.0", *available, None)
            })
            .collect::<Vec<_>>();
        registrations.extend(versioned_native_registrations());
        Self::new(registrations)
    }

    /// Assembles the single immutable registry from native registrations and
    /// exact validated plugin declarations.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorDataError`] when the combined native/plugin
    /// declaration set is invalid, excessive, or contains an exact duplicate.
    pub fn native_with_plugins(
        manifests: &[crate::plugin::PluginManifestDataRecord],
    ) -> Result<Self, NodeExecutorDataError> {
        let native = Self::native()?;
        let mut registrations = native
            .records
            .iter()
            .map(|record| RegisterNodeExecutorDataRecord {
                id: record.id.clone(),
                version: record.version.clone(),
                runtime_api: record.runtime_api.clone(),
                node_kind: record.node_kind.clone(),
                capabilities: record.capabilities.clone(),
                source: record.source.clone(),
                boundary: record.boundary,
                available: record.available,
                declaration_hash: record.declaration_hash,
            })
            .collect::<Vec<_>>();
        for manifest in manifests {
            registrations.extend(manifest.node_executors.iter().map(|executor| {
                RegisterNodeExecutorDataRecord {
                    id: executor.executor_id.clone(),
                    version: executor.version.clone(),
                    runtime_api: executor.runtime_api.clone(),
                    node_kind: executor.node_kind.clone(),
                    capabilities: executor.capabilities.clone(),
                    source: NodeExecutorSourceData::Plugin {
                        plugin_id: manifest.id.clone(),
                    },
                    boundary: NodeExecutorBoundaryData::PluginHost,
                    available: true,
                    declaration_hash: executor.declaration_hash,
                }
            }));
        }
        Self::new(registrations)
    }
}

fn versioned_native_registrations() -> Vec<RegisterNodeExecutorDataRecord> {
    vec![
        native_registration(
            "tool_execution_gate",
            "runtime.tool-gate",
            &["tools"],
            "1.0.0",
            true,
            None,
        ),
        native_registration(
            "tool_execution_gate",
            "runtime.tool-gate",
            &["tools"],
            "1.1.0",
            true,
            Some(crate::tool::canonical_tool_catalog_hash()),
        ),
        native_registration(
            "model_call",
            "runtime.model-request",
            &["model"],
            "1.1.0",
            true,
            None,
        ),
        native_registration(
            "spawn_child_agent",
            "runtime.child-spawn",
            &["agents"],
            "1.1.0",
            true,
            None,
        ),
        native_registration("review", "runtime.review", &["model"], "1.1.0", true, None),
        native_registration(
            "persist_artifact",
            "runtime.artifact-persistence",
            &["artifacts"],
            "1.1.0",
            true,
            None,
        ),
    ]
}

fn native_registration(
    node_kind: &str,
    id: &str,
    capabilities: &[&str],
    version: &str,
    available: bool,
    behavior_abi: Option<ContentHash>,
) -> RegisterNodeExecutorDataRecord {
    RegisterNodeExecutorDataRecord {
        id: id.to_owned(),
        version: version.to_owned(),
        runtime_api: String::from("^1.0"),
        node_kind: node_kind.to_owned(),
        capabilities: capabilities
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect(),
        source: NodeExecutorSourceData::Runtime,
        boundary: NodeExecutorBoundaryData::RuntimeLogic,
        available,
        declaration_hash: native_declaration_hash(node_kind, id, version, behavior_abi),
    }
}

fn native_declaration_hash(
    node_kind: &str,
    id: &str,
    version: &str,
    behavior_abi: Option<ContentHash>,
) -> ContentHash {
    let mut bytes = format!("{node_kind}\0{id}\0{version}\0^1.0").into_bytes();
    if let Some(behavior_abi) = behavior_abi {
        bytes.push(0);
        bytes.extend_from_slice(b"behavior-abi:");
        bytes.extend_from_slice(behavior_abi.as_bytes());
    }
    ContentHash::digest(&bytes)
}

impl NodeExecutorDataPort for RuntimeNodeExecutorData {
    fn list_node_executors(
        &self,
        _request: ListNodeExecutorsDataRequest,
    ) -> Result<Vec<NodeExecutorDataRecord>, NodeExecutorDataError> {
        Ok(self.records.as_ref().clone())
    }
}

fn normalize(
    registration: RegisterNodeExecutorDataRecord,
) -> Result<NodeExecutorDataRecord, NodeExecutorDataError> {
    if !valid_identifier(&registration.id)
        || !valid_identifier(&registration.node_kind)
        || registration.version.trim().is_empty()
        || registration.version.len() > MAX_IDENTIFIER_BYTES
        || registration.runtime_api.trim().is_empty()
        || registration.runtime_api.len() > MAX_IDENTIFIER_BYTES
        || registration.capabilities.len() > 128
        || registration
            .capabilities
            .iter()
            .any(|value| !valid_identifier(value))
    {
        return Err(NodeExecutorDataError::InvalidRegistration);
    }
    if let NodeExecutorSourceData::Plugin { plugin_id } = &registration.source
        && !valid_identifier(plugin_id)
    {
        return Err(NodeExecutorDataError::InvalidRegistration);
    }
    if matches!(registration.source, NodeExecutorSourceData::Runtime)
        != matches!(
            registration.boundary,
            NodeExecutorBoundaryData::RuntimeLogic
        )
    {
        return Err(NodeExecutorDataError::InvalidBoundary);
    }
    Ok(NodeExecutorDataRecord {
        id: registration.id,
        version: registration.version,
        runtime_api: registration.runtime_api,
        node_kind: registration.node_kind,
        capabilities: registration.capabilities,
        source: registration.source,
        boundary: registration.boundary,
        available: registration.available,
        declaration_hash: registration.declaration_hash,
    })
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

/// Node-executor capability data failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum NodeExecutorDataError {
    /// Registry was not assembled or exceeded its hard registration bound.
    #[error("node-executor registration count is invalid")]
    InvalidRegistrationCount,
    /// A registration contains invalid or unbounded data.
    #[error("node-executor registration is invalid")]
    InvalidRegistration,
    /// The source and execution boundary are inconsistent.
    #[error("node-executor source and boundary are inconsistent")]
    InvalidBoundary,
    /// The exact implementation identity was registered more than once.
    #[error("duplicate node-executor registration")]
    DuplicateRegistration,
    /// Runtime data has no injected node-executor registry.
    #[error("node-executor registry is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_registry_is_stable_and_admits_implemented_categories() {
        let registry = RuntimeNodeExecutorData::native().expect("native registry");
        let records = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("records");
        assert_eq!(records.len(), 24);
        assert!(records.windows(2).all(|pair| {
            (
                pair[0].node_kind.as_str(),
                pair[0].id.as_str(),
                pair[0].version.as_str(),
            ) < (
                pair[1].node_kind.as_str(),
                pair[1].id.as_str(),
                pair[1].version.as_str(),
            )
        }));
        assert!(records.iter().any(|record| {
            record.node_kind == "parallel_branch"
                && record.id == "runtime.parallel"
                && record.available
        }));
        assert!(records.iter().any(|record| {
            record.node_kind == "emit_event"
                && record.id == "runtime.event-emission"
                && record.available
        }));
        for available in ["delay", "schedule"] {
            assert!(
                records
                    .iter()
                    .any(|record| { record.node_kind == available && record.available })
            );
        }
        assert!(records.iter().any(|record| {
            record.node_kind == "send_child_agent_message"
                && record.id == "runtime.child-message"
                && record.available
        }));
        for available in ["join_results", "parallel_branch"] {
            assert!(
                records
                    .iter()
                    .any(|record| { record.node_kind == available && record.available })
            );
        }
        let tool_gate = records
            .iter()
            .filter(|record| record.id == "runtime.tool-gate")
            .collect::<Vec<_>>();
        assert_eq!(tool_gate.len(), 2);
        assert_eq!(tool_gate[0].version, "1.0.0");
        assert_eq!(
            tool_gate[0].declaration_hash,
            native_declaration_hash("tool_execution_gate", "runtime.tool-gate", "1.0.0", None),
            "the historical declaration identity must remain exact"
        );
        assert_eq!(tool_gate[1].version, "1.1.0");
        assert_eq!(
            tool_gate[1].declaration_hash,
            native_declaration_hash(
                "tool_execution_gate",
                "runtime.tool-gate",
                "1.1.0",
                Some(crate::tool::canonical_tool_catalog_hash())
            )
        );
        assert_ne!(
            tool_gate[0].declaration_hash, tool_gate[1].declaration_hash,
            "alias-aware behavior must not masquerade as the historical executor"
        );
    }

    #[test]
    fn duplicate_and_mismatched_plugin_boundaries_fail_deterministically() {
        let registration = RegisterNodeExecutorDataRecord {
            id: String::from("plugin.node"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            node_kind: String::from("emit_event"),
            capabilities: BTreeSet::from([String::from("events")]),
            source: NodeExecutorSourceData::Plugin {
                plugin_id: String::from("fixture.plugin"),
            },
            boundary: NodeExecutorBoundaryData::PluginHost,
            available: true,
            declaration_hash: ContentHash::digest(b"fixture-plugin-node"),
        };
        assert_eq!(
            RuntimeNodeExecutorData::new(vec![registration.clone(), registration])
                .expect_err("duplicate"),
            NodeExecutorDataError::DuplicateRegistration
        );
        let invalid = RegisterNodeExecutorDataRecord {
            id: String::from("runtime.invalid"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^1.0"),
            node_kind: String::from("emit_event"),
            capabilities: BTreeSet::new(),
            source: NodeExecutorSourceData::Runtime,
            boundary: NodeExecutorBoundaryData::PluginHost,
            available: true,
            declaration_hash: ContentHash::digest(b"runtime-invalid"),
        };
        assert_eq!(
            RuntimeNodeExecutorData::new(vec![invalid]).expect_err("boundary"),
            NodeExecutorDataError::InvalidBoundary
        );
    }

    #[test]
    fn validated_plugin_declaration_joins_the_single_registry() {
        let declaration_hash = ContentHash::digest(b"exact-plugin-declaration");
        let manifest = crate::plugin::PluginManifestDataRecord {
            id: String::from("fixture.node"),
            version: String::from("3.0.0"),
            category: String::from("graph_node"),
            class: String::from("blocking"),
            provided_capabilities: BTreeSet::new(),
            subscribed_events: BTreeSet::new(),
            timeout_ms: 1_000,
            failure_policy: String::from("reject"),
            canonical_manifest_json: String::from("{}"),
            configuration: serde_json::json!({}),
            configuration_reference: ContentHash::digest(b"{}"),
            node_executors: vec![crate::plugin::PluginNodeExecutorDataRecord {
                plugin_version: String::from("3.0.0"),
                executor_id: String::from("fixture.echo"),
                version: String::from("2.1.0"),
                runtime_api: String::from("^1.0"),
                node_kind: String::from("model_call"),
                handler: String::from("execute_echo"),
                capabilities: BTreeSet::from([String::from("model")]),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 500,
                failure_policy: String::from("reject"),
                max_attempts: 1,
                retry_backoff_ms: 0,
                idempotent: false,
                tool_permissions: BTreeSet::new(),
                network_permissions: BTreeSet::new(),
                state_scope: String::from("invocation"),
                external_effects: false,
                declaration_hash,
            }],
            context_transforms: Vec::new(),
            memory_providers: Vec::new(),
            compactors: Vec::new(),
        };
        let registry =
            RuntimeNodeExecutorData::native_with_plugins(&[manifest]).expect("combined registry");
        let records = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("records");
        assert!(records.iter().any(|record| {
            record.id == "fixture.echo"
                && record.version == "2.1.0"
                && record.source
                    == (NodeExecutorSourceData::Plugin {
                        plugin_id: String::from("fixture.node"),
                    })
                && record.boundary == NodeExecutorBoundaryData::PluginHost
                && record.declaration_hash == declaration_hash
                && record.available
        }));
    }
}
