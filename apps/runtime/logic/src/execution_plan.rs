//! Runtime-owned execution-plan identity, migration, and inspection decisions.
//!
//! The logic layer owns compatibility and migration decisions for the
//! immutable per-node execution plan. It never parses dependency persistence
//! structs directly: it maps to and from the normalized data records through
//! the [`ExecutionPlanDataPort`] boundary. Live registry checks stay in
//! `node_executor`; this module adds the durable plan-file identity binding,
//! typed migration outcomes, and a pure inspection projection that never
//! resolves live executors.

use std::path::PathBuf;

use agentmod_primitives::ContentHash;
use agentmod_runtime_data::execution_plan::{
    EXECUTION_PLAN_RECORD_SCHEMA_VERSION, ExecutionPlanDataError, ExecutionPlanDataPort,
    ExecutionPlanFileData, ExecutionPlanIdentityData, LoadExecutionPlanDataRequest,
    LoadExecutionPlanDataResult, StoreExecutionPlanDataRequest,
};
use serde::Serialize;
use thiserror::Error;

use crate::{
    node_executor::{
        NodeExecutorCapability, RuntimeExecutabilityError, revalidate_runtime_execution_plan,
    },
    session::{SessionNodeExecutorBoundary, SessionNodeExecutorSource, SessionStyleBinding},
};

/// Logic-owned immutable execution-plan identity bound to one session style.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPlanIdentity {
    /// Canonical record schema version.
    pub schema_version: u16,
    /// Stable style ID.
    pub style_id: String,
    /// Exact semantic style version.
    pub style_version: String,
    /// Canonical style manifest content hash.
    pub style_content_hash: ContentHash,
    /// Hash of the exact compiled descriptor.
    pub compiled_style_hash: ContentHash,
    /// Compatibility-bound compiled cache key.
    pub compiled_cache_key: ContentHash,
    /// Runtime API version used during resolution.
    pub runtime_api_version: String,
    /// Plugin-set hash used during compilation.
    pub plugin_set_hash: ContentHash,
    /// Runtime capability-set hash used during compilation.
    pub capability_set_hash: ContentHash,
    /// Node-executor registry hash used during resolution.
    pub registry_hash: ContentHash,
    /// Hash of the canonical serialized execution plan.
    pub plan_hash: ContentHash,
    /// Number of compiled graph nodes covered by the plan.
    pub node_count: u64,
}

impl ExecutionPlanIdentity {
    /// Derives the exact immutable identity retained by a bound style.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionPlanLogicError::NotBound`] when the binding has no
    /// persisted plan or plan hash.
    pub fn from_binding(binding: &SessionStyleBinding) -> Result<Self, ExecutionPlanLogicError> {
        let plan = binding
            .execution_plan
            .as_ref()
            .ok_or(ExecutionPlanLogicError::NotBound)?;
        let plan_hash = binding
            .execution_plan_hash
            .ok_or(ExecutionPlanLogicError::NotBound)?;
        Ok(Self {
            schema_version: EXECUTION_PLAN_RECORD_SCHEMA_VERSION,
            style_id: binding.id.clone(),
            style_version: binding.version.clone(),
            style_content_hash: binding.content_hash,
            compiled_style_hash: binding.compiled_style_hash,
            compiled_cache_key: binding.compiled_cache_key,
            runtime_api_version: binding.runtime_api_version.clone(),
            plugin_set_hash: binding.plugin_set_hash,
            capability_set_hash: binding.capability_set_hash,
            registry_hash: plan.registry_hash,
            plan_hash,
            node_count: u64::try_from(plan.nodes.len())
                .map_err(|_| ExecutionPlanLogicError::NodeCountOverflow)?,
        })
    }

    /// Maps to the normalized data-layer identity record.
    #[must_use]
    pub fn to_data(self) -> ExecutionPlanIdentityData {
        ExecutionPlanIdentityData {
            schema_version: self.schema_version,
            style_id: self.style_id,
            style_version: self.style_version,
            style_content_hash: self.style_content_hash,
            compiled_style_hash: self.compiled_style_hash,
            compiled_cache_key: self.compiled_cache_key,
            runtime_api_version: self.runtime_api_version,
            plugin_set_hash: self.plugin_set_hash,
            capability_set_hash: self.capability_set_hash,
            registry_hash: self.registry_hash,
            plan_hash: self.plan_hash,
            node_count: self.node_count,
        }
    }

    /// Returns the first exact identity mismatch as a stable diagnostic.
    fn first_mismatch(
        &self,
        actual: &ExecutionPlanIdentityData,
    ) -> Option<ExecutionPlanMigrationDiagnostic> {
        if actual.schema_version != self.schema_version {
            return Some(diagnostic(
                "EPLAN-101",
                format!(
                    "plan file schema version {} differs from expected {}",
                    actual.schema_version, self.schema_version
                ),
            ));
        }
        if actual.style_id != self.style_id {
            return Some(diagnostic(
                "EPLAN-102",
                format!(
                    "plan file style ID `{}` differs from binding `{}`",
                    actual.style_id, self.style_id
                ),
            ));
        }
        if actual.style_version != self.style_version {
            return Some(diagnostic(
                "EPLAN-103",
                format!(
                    "plan file style version `{}` differs from binding `{}`",
                    actual.style_version, self.style_version
                ),
            ));
        }
        if actual.style_content_hash != self.style_content_hash {
            return Some(diagnostic(
                "EPLAN-104",
                "plan file style content hash differs from the binding",
            ));
        }
        if actual.compiled_style_hash != self.compiled_style_hash {
            return Some(diagnostic(
                "EPLAN-105",
                "plan file compiled style hash differs from the binding",
            ));
        }
        if actual.compiled_cache_key != self.compiled_cache_key {
            return Some(diagnostic(
                "EPLAN-106",
                "plan file compiled cache key differs from the binding",
            ));
        }
        if actual.runtime_api_version != self.runtime_api_version {
            return Some(diagnostic(
                "EPLAN-107",
                format!(
                    "plan file runtime API version `{}` differs from binding `{}`",
                    actual.runtime_api_version, self.runtime_api_version
                ),
            ));
        }
        if actual.plugin_set_hash != self.plugin_set_hash {
            return Some(diagnostic(
                "EPLAN-108",
                "plan file plugin-set hash differs from the binding",
            ));
        }
        if actual.capability_set_hash != self.capability_set_hash {
            return Some(diagnostic(
                "EPLAN-109",
                "plan file capability-set hash differs from the binding",
            ));
        }
        if actual.registry_hash != self.registry_hash {
            return Some(diagnostic(
                "EPLAN-110",
                "plan file registry hash differs from the binding",
            ));
        }
        if actual.plan_hash != self.plan_hash {
            return Some(diagnostic(
                "EPLAN-111",
                "plan file plan hash differs from the binding",
            ));
        }
        if actual.node_count != self.node_count {
            return Some(diagnostic(
                "EPLAN-112",
                format!(
                    "plan file node count {} differs from binding {}",
                    actual.node_count, self.node_count
                ),
            ));
        }
        None
    }
}

/// Typed migration outcome for an execution plan that cannot execute as-is.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutionPlanMigrationDiagnostic {
    /// Stable diagnostic code.
    pub code: String,
    /// Safe deterministic explanation.
    pub message: String,
}

/// Result of a full restart validation pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionPlanRestartOutcome {
    /// The persisted plan file, binding, and live registry agree exactly.
    Valid,
    /// The session has no compatible immutable plan and requires an explicit
    /// branch-with-recompiled-style or deliberate migration tooling.
    MigrationRequired(ExecutionPlanMigrationDiagnostic),
}

/// One normalized node-to-executor entry exposed by plan inspection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutionPlanNodeProjection {
    /// Compiled graph node ID.
    pub node_id: String,
    /// Serialized node kind.
    pub node_kind: String,
    /// Selected executor ID.
    pub executor_id: String,
    /// Exact selected executor version.
    pub executor_version: String,
    /// Executor source, including exact plugin identity.
    pub source: SessionNodeExecutorSource,
    /// Execution boundary.
    pub boundary: SessionNodeExecutorBoundary,
    /// Runtime API requirement.
    pub runtime_api_requirement: String,
    /// Capabilities required by the node.
    pub required_capabilities: Vec<String>,
    /// Exact capabilities of the selected executor.
    pub resolved_capabilities: Vec<String>,
}

/// Complete plan-identity inspection projection.
///
/// Constructed purely from canonical/session files; no live registry lookup
/// or effect dispatch is required.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutionPlanInspectionProjection {
    /// Hash of the canonical immutable plan.
    pub plan_hash: ContentHash,
    /// Hash of the registry used during initial resolution.
    pub registry_hash: ContentHash,
    /// Exact node-to-executor mapping, sorted by node ID.
    pub node_executors: Vec<ExecutionPlanNodeProjection>,
    /// Typed migration diagnostic when the plan cannot execute as-is.
    pub migration: Option<ExecutionPlanMigrationDiagnostic>,
}

/// Availability/compatibility state from a separate live validation pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPlanAvailability {
    /// Whether the live registry hash equals the persisted registry hash.
    pub registry_hash_matches: bool,
    /// Per-node availability state.
    pub nodes: Vec<ExecutionPlanNodeAvailability>,
}

/// Per-node live availability state.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "the projection reports each independent exact-identity flag for stable diagnostics"
)]
pub struct ExecutionPlanNodeAvailability {
    /// Compiled graph node ID.
    pub node_id: String,
    /// Whether the exact selected executor is still registered.
    pub executor_available: bool,
    /// Whether the exact version is still registered.
    pub version_matches: bool,
    /// Whether the exact source/plugin identity is still registered.
    pub source_matches: bool,
    /// Whether the exact execution boundary is still registered.
    pub boundary_matches: bool,
    /// Whether the exact capability set is still registered.
    pub capabilities_match: bool,
    /// Whether the registration is available and runtime-API-compatible.
    pub compatible: bool,
    /// Stable reason when the node is unavailable or incompatible.
    pub reason: Option<String>,
}

/// Logic-owned command to persist the plan file with an atomic session create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistExecutionPlanFileCommand {
    /// Session directory receiving the plan file.
    pub session_directory: PathBuf,
    /// Complete normalized plan file.
    pub file: ExecutionPlanFileData,
}

/// Builds the normalized plan file attached to session/branch/child creation.
///
/// Returns `None` for legacy bindings that never retained an execution plan;
/// those sessions require an explicit migration and must not claim a durable
/// plan file.
///
/// # Errors
///
/// Returns [`ExecutionPlanLogicError`] when the binding plan cannot be
/// serialized into the canonical record.
pub fn to_plan_file_data(
    binding: &SessionStyleBinding,
) -> Result<Option<ExecutionPlanFileData>, ExecutionPlanLogicError> {
    let Some(plan) = binding.execution_plan.as_ref() else {
        return Ok(None);
    };
    let identity = ExecutionPlanIdentity::from_binding(binding)?;
    let canonical_plan_json =
        serde_json::to_string(plan).map_err(|_| ExecutionPlanLogicError::PlanSerialization)?;
    Ok(Some(ExecutionPlanFileData {
        identity: identity.to_data(),
        canonical_plan_json,
    }))
}

/// Persists the checksummed plan file for one session through the data port.
///
/// # Errors
///
/// Returns [`ExecutionPlanLogicError`] for invalid plan identity or translated
/// data/dependency failure.
pub fn persist_execution_plan_file<D: ExecutionPlanDataPort>(
    data: &D,
    command: PersistExecutionPlanFileCommand,
) -> Result<(), ExecutionPlanLogicError> {
    data.store_execution_plan(StoreExecutionPlanDataRequest {
        session_directory: command.session_directory,
        file: command.file,
    })
    .map_err(ExecutionPlanLogicError::Data)?;
    Ok(())
}

/// Checksum-validates the persisted plan file and cross-checks it against the
/// binding identity, then delegates exact live-registry revalidation.
///
/// Fail-closed semantics:
/// - an absent plan file is a typed [`ExecutionPlanRestartOutcome::MigrationRequired`]
///   outcome (legacy session), never silent execution;
/// - a corrupt, truncated, or checksum-invalid file is a hard error;
/// - any exact identity field that differs from the binding is a hard error;
/// - any exact executor identity drift reported by the live registry is a
///   hard error (see `node_executor::revalidate_runtime_execution_plan`).
///
/// # Panics
///
/// Panics only when the binding previously passed identity construction but
/// its plan was concurrently dropped, which is a caller invariant violation.
///
/// # Errors
///
/// Returns [`ExecutionPlanLogicError`] for corrupt files, identity drift, or
/// translated data/dependency failures.
pub fn validate_persisted_execution_plan<D>(
    data: &D,
    session_directory: &std::path::Path,
    binding: &SessionStyleBinding,
) -> Result<ExecutionPlanRestartOutcome, ExecutionPlanLogicError>
where
    D: ExecutionPlanDataPort + agentmod_runtime_data::node_executor::NodeExecutorDataPort,
{
    let expected = ExecutionPlanIdentity::from_binding(binding)?;
    let loaded = data
        .load_execution_plan(LoadExecutionPlanDataRequest {
            session_directory: session_directory.to_owned(),
        })
        .map_err(ExecutionPlanLogicError::Data)?;
    let LoadExecutionPlanDataResult::Present(record) = loaded else {
        return Ok(ExecutionPlanRestartOutcome::MigrationRequired(diagnostic(
            "EPLAN-201",
            "the session has no persisted execution-plan file; branch with a recompiled style or run explicit migration tooling",
        )));
    };
    if let Some(mismatch) = expected.first_mismatch(&record.identity) {
        return Err(ExecutionPlanLogicError::IdentityDrift(mismatch));
    }
    let canonical_plan: serde_json::Value = serde_json::from_str(&record.canonical_plan_json)
        .map_err(|_| ExecutionPlanLogicError::InvalidCanonicalPlan)?;
    let binding_plan = serde_json::to_value(
        binding
            .execution_plan
            .as_ref()
            .expect("identity construction already required a persisted plan"),
    )
    .map_err(|_| ExecutionPlanLogicError::PlanSerialization)?;
    if canonical_plan != binding_plan {
        return Err(ExecutionPlanLogicError::IdentityDrift(diagnostic(
            "EPLAN-113",
            "the persisted plan file contents differ from the canonical binding plan",
        )));
    }
    revalidate_runtime_execution_plan(data, binding)
        .map_err(ExecutionPlanLogicError::Revalidate)?;
    Ok(ExecutionPlanRestartOutcome::Valid)
}

/// Validates the persisted execution plan on a live session resume.
///
/// A session that retains a checksummed plan file is validated strictly:
/// file checksum, full style/plugin/capability identity, canonical plan
/// bytes, and the exact live-registry executor identities all must agree.
/// A legacy session without a plan file falls back to the exact
/// binding-based revalidation so sessions created before the durable plan
/// file feature keep their existing fail-closed restart guarantee.
///
/// # Errors
///
/// Returns [`ExecutionPlanLogicError`] for corrupt files, identity drift, or
/// live-registry drift.
pub fn validate_session_resume_plan<D>(
    data: &D,
    session_directory: &std::path::Path,
    binding: &SessionStyleBinding,
) -> Result<(), ExecutionPlanLogicError>
where
    D: ExecutionPlanDataPort + agentmod_runtime_data::node_executor::NodeExecutorDataPort,
{
    let loaded = data
        .load_execution_plan(LoadExecutionPlanDataRequest {
            session_directory: session_directory.to_owned(),
        })
        .map_err(ExecutionPlanLogicError::Data)?;
    match loaded {
        LoadExecutionPlanDataResult::Missing => revalidate_runtime_execution_plan(data, binding)
            .map_err(ExecutionPlanLogicError::Revalidate),
        LoadExecutionPlanDataResult::Present(_) => {
            match validate_persisted_execution_plan(data, session_directory, binding)? {
                ExecutionPlanRestartOutcome::Valid => Ok(()),
                ExecutionPlanRestartOutcome::MigrationRequired(diagnostic) => {
                    Err(ExecutionPlanLogicError::MigrationRequired(diagnostic))
                }
            }
        }
    }
}

/// Reconstructs the execution-plan identity projection from canonical/session
/// files without resolving live executors or dispatching effects.
///
/// A corrupt or missing plan file yields a typed migration diagnostic inside
/// the projection rather than a hard failure.
///
/// # Errors
///
/// Returns [`ExecutionPlanLogicError`] for invalid paths or translated
/// non-corruption dependency failures.
pub fn inspect_execution_plan_file<D: ExecutionPlanDataPort>(
    data: &D,
    session_directory: &std::path::Path,
) -> Result<ExecutionPlanInspectionProjection, ExecutionPlanLogicError> {
    let loaded = match data.load_execution_plan(LoadExecutionPlanDataRequest {
        session_directory: session_directory.to_owned(),
    }) {
        Ok(loaded) => loaded,
        Err(ExecutionPlanDataError::CorruptFile { reason }) => {
            return Ok(projection_from_plan_json(
                None,
                Some(diagnostic(
                    "EPLAN-301",
                    format!("the persisted plan file is corrupt: {reason}"),
                )),
            ));
        }
        Err(error) => return Err(ExecutionPlanLogicError::Data(error)),
    };
    match loaded {
        LoadExecutionPlanDataResult::Missing => Ok(projection_from_plan_json(
            None,
            Some(diagnostic(
                "EPLAN-201",
                "the session has no persisted execution-plan file; branch with a recompiled style or run explicit migration tooling",
            )),
        )),
        LoadExecutionPlanDataResult::Present(record) => {
            let plan: serde_json::Value = serde_json::from_str(&record.canonical_plan_json)
                .map_err(|_| ExecutionPlanLogicError::InvalidCanonicalPlan)?;
            let registry_hash = plan
                .get("registry_hash")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse().ok())
                .ok_or(ExecutionPlanLogicError::InvalidCanonicalPlan)?;
            let nodes = plan
                .get("nodes")
                .and_then(serde_json::Value::as_array)
                .ok_or(ExecutionPlanLogicError::InvalidCanonicalPlan)?;
            let mut node_executors = Vec::with_capacity(nodes.len());
            for node in nodes {
                node_executors.push(node_projection(node)?);
            }
            Ok(projection_from_plan_json(
                Some(ExecutionPlanInspectionProjection {
                    plan_hash: record.identity.plan_hash,
                    registry_hash,
                    node_executors,
                    migration: None,
                }),
                None,
            ))
        }
    }
}

/// Runs a separate live validation pass over an inspection projection.
///
/// The projection itself is pure; availability is only computed when the
/// caller supplies a live capability snapshot.
#[must_use]
pub fn availability_projection(
    capabilities: &[NodeExecutorCapability],
    projection: &ExecutionPlanInspectionProjection,
) -> ExecutionPlanAvailability {
    let live_registry_hash = crate::node_executor::registry_hash_for(capabilities);
    ExecutionPlanAvailability {
        registry_hash_matches: live_registry_hash == projection.registry_hash,
        nodes: projection
            .node_executors
            .iter()
            .map(|node| node_availability(capabilities, node))
            .collect(),
    }
}

fn node_availability(
    capabilities: &[NodeExecutorCapability],
    node: &ExecutionPlanNodeProjection,
) -> ExecutionPlanNodeAvailability {
    let registered = capabilities.iter().any(|capability| {
        capability.node_kind == node.node_kind && capability.id == node.executor_id
    });
    if !registered {
        return ExecutionPlanNodeAvailability {
            node_id: node.node_id.clone(),
            executor_available: false,
            version_matches: false,
            source_matches: false,
            boundary_matches: false,
            capabilities_match: false,
            compatible: false,
            reason: Some(format!(
                "exact persisted executor `{}@{}` is not registered for `{}`",
                node.executor_id, node.executor_version, node.node_kind
            )),
        };
    }
    // The exact version is part of the selected executor identity: a newer
    // or different compatible registration must never substitute.
    let Some(registration) = capabilities.iter().find(|capability| {
        capability.node_kind == node.node_kind
            && capability.id == node.executor_id
            && capability.version == node.executor_version
    }) else {
        return ExecutionPlanNodeAvailability {
            node_id: node.node_id.clone(),
            executor_available: true,
            version_matches: false,
            source_matches: false,
            boundary_matches: false,
            capabilities_match: false,
            compatible: false,
            reason: Some(format!(
                "persisted executor `{}@{}` version is not registered",
                node.executor_id, node.executor_version
            )),
        };
    };
    let source_matches = from_projection_source(&node.source) == registration.source;
    let boundary_matches = to_capability_boundary(node.boundary) == registration.boundary;
    let capabilities_match = registration.capabilities == {
        let mut expected = node.resolved_capabilities.clone();
        expected.sort();
        expected.dedup();
        expected
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    };
    let runtime_api_compatible = registration.runtime_api == node.runtime_api_requirement
        && semver::VersionReq::parse(&registration.runtime_api).is_ok();
    let compatible = registration.available
        && source_matches
        && boundary_matches
        && capabilities_match
        && runtime_api_compatible;
    ExecutionPlanNodeAvailability {
        node_id: node.node_id.clone(),
        executor_available: true,
        version_matches: true,
        source_matches,
        boundary_matches,
        capabilities_match,
        compatible,
        reason: (!compatible).then(|| {
            format!(
                "persisted executor `{}@{}` drifted from the live registration",
                node.executor_id, node.executor_version
            )
        }),
    }
}

fn to_capability_boundary(
    boundary: SessionNodeExecutorBoundary,
) -> crate::node_executor::NodeExecutorBoundary {
    match boundary {
        SessionNodeExecutorBoundary::RuntimeLogic => {
            crate::node_executor::NodeExecutorBoundary::RuntimeLogic
        }
        SessionNodeExecutorBoundary::PluginHost => {
            crate::node_executor::NodeExecutorBoundary::PluginHost
        }
    }
}

fn from_projection_source(
    source: &SessionNodeExecutorSource,
) -> crate::node_executor::NodeExecutorSource {
    match source {
        SessionNodeExecutorSource::Runtime => crate::node_executor::NodeExecutorSource::Runtime,
        SessionNodeExecutorSource::Plugin { plugin_id } => {
            crate::node_executor::NodeExecutorSource::Plugin {
                plugin_id: plugin_id.clone(),
            }
        }
    }
}

fn node_projection(
    node: &serde_json::Value,
) -> Result<ExecutionPlanNodeProjection, ExecutionPlanLogicError> {
    let field = |name: &str| -> Result<&serde_json::Value, ExecutionPlanLogicError> {
        node.get(name)
            .ok_or(ExecutionPlanLogicError::InvalidCanonicalPlan)
    };
    let string_field = |name: &str| -> Result<String, ExecutionPlanLogicError> {
        field(name)?
            .as_str()
            .map(str::to_owned)
            .ok_or(ExecutionPlanLogicError::InvalidCanonicalPlan)
    };
    let list_field = |name: &str| -> Result<Vec<String>, ExecutionPlanLogicError> {
        field(name)?
            .as_array()
            .map(|values| {
                values
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .ok_or(ExecutionPlanLogicError::InvalidCanonicalPlan)
    };
    Ok(ExecutionPlanNodeProjection {
        node_id: string_field("node_id")?,
        node_kind: string_field("node_kind")?,
        executor_id: string_field("executor_id")?,
        executor_version: string_field("executor_version")?,
        source: serde_json::from_value(field("source")?.clone())
            .map_err(|_| ExecutionPlanLogicError::InvalidCanonicalPlan)?,
        boundary: serde_json::from_value(field("boundary")?.clone())
            .map_err(|_| ExecutionPlanLogicError::InvalidCanonicalPlan)?,
        runtime_api_requirement: string_field("runtime_api_requirement")?,
        required_capabilities: list_field("required_capabilities")?,
        resolved_capabilities: list_field("resolved_capabilities")?,
    })
}

fn projection_from_plan_json(
    projection: Option<ExecutionPlanInspectionProjection>,
    migration: Option<ExecutionPlanMigrationDiagnostic>,
) -> ExecutionPlanInspectionProjection {
    projection.unwrap_or_else(|| ExecutionPlanInspectionProjection {
        plan_hash: ContentHash::from_bytes([0; 32]),
        registry_hash: ContentHash::from_bytes([0; 32]),
        node_executors: Vec::new(),
        migration,
    })
}

fn diagnostic(code: &str, message: impl Into<String>) -> ExecutionPlanMigrationDiagnostic {
    ExecutionPlanMigrationDiagnostic {
        code: String::from(code),
        message: message.into(),
    }
}

/// Execution-plan logic failure with stable diagnostic codes.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ExecutionPlanLogicError {
    /// The binding retained no execution plan or plan hash.
    #[error("execution-plan: the session style binding has no persisted execution plan")]
    NotBound,
    /// The canonical plan could not be serialized.
    #[error("execution-plan: canonical plan serialization failed")]
    PlanSerialization,
    /// The canonical plan JSON stored in the plan file is invalid.
    #[error("execution-plan: canonical plan JSON is invalid")]
    InvalidCanonicalPlan,
    /// Node count exceeds the storage limit.
    #[error("execution-plan: plan node count overflows the persisted identity")]
    NodeCountOverflow,
    /// The persisted plan file identity drifted from the binding.
    #[error("execution-plan identity drift: {0:?}")]
    IdentityDrift(ExecutionPlanMigrationDiagnostic),
    /// The live-registry revalidation failed closed.
    #[error("execution-plan revalidation failed: {0}")]
    Revalidate(RuntimeExecutabilityError),
    /// The session requires explicit migration before another execution.
    #[error("execution-plan migration required: {0:?}")]
    MigrationRequired(ExecutionPlanMigrationDiagnostic),
    /// Translated data-layer failure.
    #[error("execution-plan data failed: {0}")]
    Data(ExecutionPlanDataError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_from_binding_round_trips_through_data_record() {
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let identity = ExecutionPlanIdentity::from_binding(&binding).expect("identity");
        let data = identity.clone().to_data();
        let restored = ExecutionPlanIdentityData {
            schema_version: data.schema_version,
            style_id: data.style_id.clone(),
            style_version: data.style_version.clone(),
            style_content_hash: data.style_content_hash,
            compiled_style_hash: data.compiled_style_hash,
            compiled_cache_key: data.compiled_cache_key,
            runtime_api_version: data.runtime_api_version.clone(),
            plugin_set_hash: data.plugin_set_hash,
            capability_set_hash: data.capability_set_hash,
            registry_hash: data.registry_hash,
            plan_hash: data.plan_hash,
            node_count: data.node_count,
        };
        assert!(identity.first_mismatch(&restored).is_none());
        let mut drifted = restored;
        drifted.plugin_set_hash = ContentHash::digest(b"other-plugins");
        let mismatch = identity.first_mismatch(&drifted).expect("drift");
        assert_eq!(mismatch.code, "EPLAN-108");
    }

    #[test]
    fn plan_file_data_matches_binding_identity_and_hash() {
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let file = to_plan_file_data(&binding)
            .expect("plan file data")
            .expect("bound plan");
        agentmod_runtime_data::execution_plan::validate_file_identity(&file)
            .expect("file identity");
        assert_eq!(
            file.identity.plan_hash,
            binding.execution_plan_hash.expect("hash")
        );
        assert_eq!(
            file.identity.node_count,
            u64::try_from(binding.execution_plan.as_ref().expect("plan").nodes.len()).unwrap()
        );
    }

    #[test]
    fn absent_binding_produces_no_plan_file() {
        let mut binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        binding.execution_plan = None;
        binding.execution_plan_hash = None;
        assert!(to_plan_file_data(&binding).expect("no plan").is_none());
    }

    #[test]
    fn inspect_missing_file_reports_migration_required() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let data = agentmod_runtime_data::local::local_runtime_data();
        let projection = inspect_execution_plan_file(&data, &session_directory).expect("inspect");
        assert!(projection.node_executors.is_empty());
        let migration = projection.migration.expect("migration diagnostic");
        assert_eq!(migration.code, "EPLAN-201");
    }

    #[test]
    fn inspect_corrupt_file_reports_corruption_diagnostic() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let mut corrupted = tempfile::NamedTempFile::new_in(&session_directory).expect("temp file");
        std::io::Write::write_all(&mut corrupted, br#"{"truncated": true"#).expect("write");
        corrupted
            .persist(session_directory.join("execution-plan.json"))
            .expect("persist corrupt file");
        let data = agentmod_runtime_data::local::local_runtime_data();
        let projection = inspect_execution_plan_file(&data, &session_directory).expect("inspect");
        let migration = projection.migration.expect("corruption diagnostic");
        assert_eq!(migration.code, "EPLAN-301");
    }

    #[test]
    fn full_restart_validation_passes_for_bound_session() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let data = agentmod_runtime_data::local::local_runtime_data_with_node_executors(
            agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("native registry"),
        );
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let file = to_plan_file_data(&binding)
            .expect("plan file data")
            .expect("bound plan");
        persist_execution_plan_file(
            &data,
            PersistExecutionPlanFileCommand {
                session_directory: session_directory.clone(),
                file,
            },
        )
        .expect("persist plan");
        let outcome = validate_persisted_execution_plan(&data, &session_directory, &binding)
            .expect("validation");
        assert_eq!(outcome, ExecutionPlanRestartOutcome::Valid);
    }

    #[test]
    fn restart_validation_fails_closed_when_plan_file_is_corrupt() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let data = agentmod_runtime_data::local::local_runtime_data_with_node_executors(
            agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("native registry"),
        );
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let file = to_plan_file_data(&binding)
            .expect("plan file data")
            .expect("bound plan");
        persist_execution_plan_file(
            &data,
            PersistExecutionPlanFileCommand {
                session_directory: session_directory.clone(),
                file,
            },
        )
        .expect("persist plan");
        let mut corrupted = tempfile::NamedTempFile::new_in(&session_directory).expect("temp file");
        std::io::Write::write_all(&mut corrupted, br#"{"truncated": true"#).expect("write");
        corrupted
            .persist(session_directory.join("execution-plan.json"))
            .expect("persist corrupt file");
        let error = validate_persisted_execution_plan(&data, &session_directory, &binding)
            .expect_err("corrupt plan must fail closed");
        assert!(
            matches!(
                error,
                ExecutionPlanLogicError::Data(ExecutionPlanDataError::CorruptFile { .. })
            ),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn restart_validation_requires_migration_when_plan_file_is_missing() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let data = agentmod_runtime_data::local::local_runtime_data_with_node_executors(
            agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("native registry"),
        );
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let outcome = validate_persisted_execution_plan(&data, &session_directory, &binding)
            .expect("typed outcome");
        match outcome {
            ExecutionPlanRestartOutcome::MigrationRequired(diagnostic) => {
                assert_eq!(diagnostic.code, "EPLAN-201");
            }
            ExecutionPlanRestartOutcome::Valid => panic!("missing plan file must not validate"),
        }
    }

    #[test]
    fn restart_validation_fails_closed_on_identity_drift() {
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let data = agentmod_runtime_data::local::local_runtime_data_with_node_executors(
            agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("native registry"),
        );
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let mut drifted = binding.clone();
        drifted.plugin_set_hash = ContentHash::digest(b"different-plugins");
        let file = to_plan_file_data(&drifted)
            .expect("plan file data")
            .expect("bound plan");
        persist_execution_plan_file(
            &data,
            PersistExecutionPlanFileCommand {
                session_directory: session_directory.clone(),
                file,
            },
        )
        .expect("persist drifted plan");
        let error = validate_persisted_execution_plan(&data, &session_directory, &binding)
            .expect_err("identity drift must fail closed");
        match error {
            ExecutionPlanLogicError::IdentityDrift(diagnostic) => {
                assert_eq!(diagnostic.code, "EPLAN-108");
            }
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn availability_projection_detects_drift_without_touching_live_state() {
        let binding = crate::style_executor::tests::binding(
            agentmod_session_style_sdk::BuiltInStyle::PersistentChat,
        );
        let file = to_plan_file_data(&binding)
            .expect("plan file data")
            .expect("bound plan");
        let directory = tempfile::tempdir().expect("temp directory");
        let session_directory = directory.path().to_owned();
        let data = agentmod_runtime_data::local::local_runtime_data_with_node_executors(
            agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("native registry"),
        );
        persist_execution_plan_file(
            &data,
            PersistExecutionPlanFileCommand {
                session_directory: session_directory.clone(),
                file,
            },
        )
        .expect("persist plan");
        let projection = inspect_execution_plan_file(&data, &session_directory).expect("inspect");
        assert!(projection.migration.is_none());
        assert!(!projection.node_executors.is_empty());
        assert!(
            projection
                .node_executors
                .iter()
                .all(|node| node.executor_id.starts_with("runtime."))
        );
        let capabilities =
            crate::node_executor::inspect_node_executor_capabilities(&data).expect("capabilities");
        let availability = availability_projection(&capabilities, &projection);
        assert!(availability.registry_hash_matches);
        assert!(
            availability
                .nodes
                .iter()
                .all(|node| node.compatible && node.reason.is_none())
        );
        // A live registry with an extra registration must not alter the
        // existing selection availability.
        let mut extra = capabilities;
        let mut registration = extra[0].clone();
        registration.id = String::from("runtime.extra-executor");
        extra.push(registration);
        let availability_extra = availability_projection(&extra, &projection);
        assert!(!availability_extra.registry_hash_matches);
        assert!(availability_extra.nodes.iter().all(|node| node.compatible));
    }
}
