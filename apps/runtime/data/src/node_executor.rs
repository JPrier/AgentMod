//! Normalized runtime node-executor capability records.
//!
//! This data set contains capability metadata only. Executable behavior remains
//! in runtime logic (or behind an approved plugin invocation boundary); graph
//! logic never receives dependency implementations.

use std::{collections::BTreeSet, sync::Arc};

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
    /// Unsupported categories remain inspectable with `available = false`;
    /// compilation alone therefore cannot make them executable.
    ///
    /// The six native control-flow implementations owned by TASK-03
    /// (`runtime.child-message`, `runtime.join`, `runtime.parallel`,
    /// `runtime.delay`, `runtime.schedule`, `runtime.event-emission`) stay
    /// `available = false` until the generic dispatcher is wired into the
    /// runtime turn adapter: flipping them available before that wiring would
    /// let a structurally valid graph pass runtime-executability validation
    /// and then fail at dispatch, violating fail-closed. Their implementation
    /// contracts and mock dispatcher integration live in
    /// `agentmod-runtime-logic::node_executors`.
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
            ("tool_execution_gate", "runtime.tool-gate", &["tools"], true),
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
                false,
            ),
            ("wait_for_agents", "runtime.child-wait", &["agents"], true),
            ("join_results", "runtime.join", &["agents"], false),
            ("review", "runtime.review", &["model"], true),
            ("loop", "runtime.loop", &[], true),
            ("conditional_branch", "runtime.conditional", &[], true),
            ("parallel_branch", "runtime.parallel", &[], false),
            ("delay", "runtime.delay", &["scheduling"], false),
            ("schedule", "runtime.schedule", &["scheduling"], false),
            ("emit_event", "runtime.event-emission", &["events"], false),
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
        Self::new(
            IMPLEMENTATIONS
                .iter()
                .map(
                    |(node_kind, id, capabilities, available)| RegisterNodeExecutorDataRecord {
                        id: (*id).to_owned(),
                        version: String::from("1.0.0"),
                        runtime_api: String::from("^1.0"),
                        node_kind: (*node_kind).to_owned(),
                        capabilities: capabilities
                            .iter()
                            .map(|capability| (*capability).to_owned())
                            .collect(),
                        source: NodeExecutorSourceData::Runtime,
                        boundary: NodeExecutorBoundaryData::RuntimeLogic,
                        available: *available,
                    },
                )
                .collect(),
        )
    }
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
    fn native_registry_is_stable_and_marks_unsupported_categories() {
        let registry = RuntimeNodeExecutorData::native().expect("native registry");
        let records = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("records");
        assert_eq!(records.len(), 19);
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
                && !record.available
        }));
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
        };
        assert_eq!(
            RuntimeNodeExecutorData::new(vec![invalid]).expect_err("boundary"),
            NodeExecutorDataError::InvalidBoundary
        );
    }
}
