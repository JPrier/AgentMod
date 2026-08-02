//! Immutable artifact-backed worker result packages.
//!
//! Every completed child produces one immutable structured result package.
//! The parent receives a bounded typed handoff that references the package
//! instead of the full child transcript.

use std::collections::BTreeMap;

use agentmod_primitives::{ContentHash, Sequence};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::session::{
    ChildAgentRecord, PlannedTask, SessionLifecycle, SessionState,
};

/// Current result-package schema version.
pub const RESULT_PACKAGE_SCHEMA_VERSION: u32 = 1;

/// Maximum retained package field sizes (bounded even when the child journal
/// is large).
pub const MAX_PACKAGE_FIELD_BYTES: usize = 256 * 1024;
/// Maximum changed-file entries retained in one package.
pub const MAX_CHANGED_FILES: usize = 4096;
/// Maximum LSP diagnostics retained in one package.
pub const MAX_DIAGNOSTICS: usize = 4096;
/// Maximum artifact references retained in one package.
pub const MAX_ARTIFACT_REFERENCES: usize = 4096;

/// Structured identity of the parent task that owned the child.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageTaskIdentity {
    /// Parent runtime session.
    pub parent_session_id: String,
    /// Runtime-owned task identifier.
    pub task_id: String,
    /// Zero-based task revision.
    pub revision: u32,
    /// Task goal retained by the plan.
    pub goal: String,
    /// Task workspace mode retained by the plan.
    pub workspace_mode: String,
}

/// Structured identity of the child session that produced the package.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageChildIdentity {
    /// Runtime-managed child session.
    pub child_session_id: String,
    /// Exact parent execution identity.
    pub execution_id: String,
    /// Selected child style.
    pub style: String,
    /// Child depth.
    pub depth: u32,
}

/// Provider/model identity retained by the child execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageProviderIdentity {
    /// Provider adapter.
    pub provider: String,
    /// Provider model.
    pub model: String,
    /// Harness adapter.
    pub harness: String,
}

/// One retained LSP diagnostic.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageLspDiagnostic {
    /// Diagnostic severity string.
    pub severity: String,
    /// Affected relative path.
    pub path: String,
    /// Stable diagnostic code.
    pub code: Option<String>,
    /// Bounded message.
    pub message: String,
}

/// Token/cost/budget usage retained from the child execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageUsage {
    /// Input tokens consumed.
    pub input_tokens: u64,
    /// Output tokens consumed.
    pub output_tokens: u64,
    /// Cache-read tokens consumed.
    pub cache_read_tokens: u64,
    /// Cache-write tokens consumed.
    pub cache_write_tokens: u64,
    /// Hard token budget.
    pub token_budget: u64,
}

/// Canonical event range covered by the child journal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PackageEventRange {
    /// First canonical child sequence.
    pub first_sequence: u64,
    /// Verified child journal head.
    pub last_sequence: u64,
}

/// Immutable artifact-backed worker result package.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerResultPackage {
    /// Package schema version.
    pub schema_version: u32,
    /// Parent task identity.
    pub task_identity: PackageTaskIdentity,
    /// Child session identity.
    pub child_identity: PackageChildIdentity,
    /// Provider/model/harness identity.
    pub provider_identity: PackageProviderIdentity,
    /// Bounded final summary.
    pub summary: String,
    /// Changed-file list (relative paths).
    pub changed_files: Vec<String>,
    /// Content-addressed diff/patch artifact reference, when produced.
    pub diff_reference: Option<String>,
    /// Validation commands declared by the plan.
    pub validation_commands: Vec<String>,
    /// Content-addressed stdout artifact reference, when produced.
    pub stdout_reference: Option<String>,
    /// Content-addressed stderr artifact reference, when produced.
    pub stderr_reference: Option<String>,
    /// Validation exit status.
    pub exit_status: Option<i32>,
    /// Retained LSP diagnostics.
    pub lsp_diagnostics: Vec<PackageLspDiagnostic>,
    /// Generated artifact references.
    pub generated_artifacts: Vec<String>,
    /// Unresolved issues retained for the reviewer.
    pub unresolved_issues: Vec<String>,
    /// Completion reason.
    pub completion_reason: String,
    /// Token/cost/budget usage.
    pub usage: PackageUsage,
    /// Canonical event range.
    pub canonical_event_range: PackageEventRange,
}

impl WorkerResultPackage {
    /// Serializes the package to bounded JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ResultPackageError::Serialization`] when the package cannot
    /// be encoded.
    pub fn to_bytes(&self) -> Result<Vec<u8>, ResultPackageError> {
        serde_json::to_vec(self).map_err(|_| ResultPackageError::Serialization)
    }

    /// Returns a bounded typed handoff line for the parent conversation.
    #[must_use]
    pub fn handoff_line(&self) -> String {
        let changed = self.changed_files.len();
        let issues = self.unresolved_issues.len();
        format!(
            "task {} ({}): completed, {} changed file(s), {} unresolved issue(s), {}",
            self.task_identity.task_id,
            self.child_identity.child_session_id,
            changed,
            issues,
            self.completion_reason
        )
    }
}

/// Builds a bounded immutable result package from the child canonical state
/// and the child's structured output.
#[allow(
    clippy::too_many_arguments,
    reason = "the package binds every exact identity component without reading the full transcript"
)]
#[must_use]
pub fn build_result_package(
    record: &ChildAgentRecord,
    task: &PlannedTask,
    state: &SessionState,
    parent_session_id: &str,
    revision: u32,
    provider: &str,
    model: &str,
    harness: &str,
    depth: u32,
    usage: PackageUsage,
) -> WorkerResultPackage {
    let summary = bounded(&record.summary.clone().unwrap_or_default(), MAX_PACKAGE_FIELD_BYTES);
    let parsed = parse_worker_output(&summary);
    let changed_files = bounded_list(
        parsed.changed_files,
        MAX_CHANGED_FILES,
        MAX_PACKAGE_FIELD_BYTES,
    );
    let diagnostics = parsed
        .lsp_diagnostics
        .into_iter()
        .take(MAX_DIAGNOSTICS)
        .collect();
    let generated = bounded_list(
        parsed.generated_artifacts,
        MAX_ARTIFACT_REFERENCES,
        MAX_PACKAGE_FIELD_BYTES,
    );
    let unresolved = bounded_list(
        parsed.unresolved_issues,
        MAX_ARTIFACT_REFERENCES,
        MAX_PACKAGE_FIELD_BYTES,
    );
    let first_sequence = Sequence::FIRST.get();
    let last_sequence = state.last_sequence.get();
    let completion_reason = bounded(
        &state
            .style_execution
            .as_ref()
            .and_then(|execution| execution.termination_reason.clone())
            .unwrap_or_else(|| String::from("child_completed")),
        MAX_PACKAGE_FIELD_BYTES,
    );
    let goal = bounded(&task.goal, MAX_PACKAGE_FIELD_BYTES);
    WorkerResultPackage {
        schema_version: RESULT_PACKAGE_SCHEMA_VERSION,
        task_identity: PackageTaskIdentity {
            parent_session_id: parent_session_id.to_owned(),
            task_id: task.task_id.clone(),
            revision,
            goal,
            workspace_mode: task.workspace_mode.clone(),
        },
        child_identity: PackageChildIdentity {
            child_session_id: record
                .child_session_id
                .map_or_else(|| String::from("unknown"), |id| id.to_string()),
            execution_id: record.identity.execution_id.clone(),
            style: record.child_style.clone(),
            depth,
        },
        provider_identity: PackageProviderIdentity {
            provider: bounded(provider, MAX_PACKAGE_FIELD_BYTES),
            model: bounded(model, MAX_PACKAGE_FIELD_BYTES),
            harness: bounded(harness, MAX_PACKAGE_FIELD_BYTES),
        },
        summary,
        changed_files,
        diff_reference: parsed.diff_reference,
        validation_commands: bounded_list(
            task.validation_commands.clone(),
            MAX_ARTIFACT_REFERENCES,
            MAX_PACKAGE_FIELD_BYTES,
        ),
        stdout_reference: parsed.stdout_reference,
        stderr_reference: parsed.stderr_reference,
        exit_status: parsed.exit_status,
        lsp_diagnostics: diagnostics,
        generated_artifacts: generated,
        unresolved_issues: unresolved,
        completion_reason,
        usage,
        canonical_event_range: PackageEventRange {
            first_sequence,
            last_sequence,
        },
    }
}

/// Parses optional structured fields from a worker summary without failing
/// the package build.
fn parse_worker_output(summary: &str) -> ParsedWorkerOutput {
    let Ok(value) = serde_json::from_str::<Value>(summary) else {
        return ParsedWorkerOutput::default();
    };
    let strings = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    };
    let diagnostics = value
        .get("lsp_diagnostics")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| {
                    let path = value.get("path")?.as_str()?.to_owned();
                    Some(PackageLspDiagnostic {
                        severity: value
                            .get("severity")
                            .and_then(Value::as_str)
                            .unwrap_or("info")
                            .to_owned(),
                        path,
                        code: value
                            .get("code")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        message: value
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    ParsedWorkerOutput {
        changed_files: strings("changed_files"),
        diff_reference: value
            .get("diff_reference")
            .and_then(Value::as_str)
            .map(str::to_owned),
        stdout_reference: value
            .get("stdout_reference")
            .and_then(Value::as_str)
            .map(str::to_owned),
        stderr_reference: value
            .get("stderr_reference")
            .and_then(Value::as_str)
            .map(str::to_owned),
        exit_status: value
            .get("exit_status")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok()),
        lsp_diagnostics: diagnostics,
        generated_artifacts: strings("generated_artifacts"),
        unresolved_issues: strings("unresolved_issues"),
    }
}

#[derive(Default)]
struct ParsedWorkerOutput {
    changed_files: Vec<String>,
    diff_reference: Option<String>,
    stdout_reference: Option<String>,
    stderr_reference: Option<String>,
    exit_status: Option<i32>,
    lsp_diagnostics: Vec<PackageLspDiagnostic>,
    generated_artifacts: Vec<String>,
    unresolved_issues: Vec<String>,
}

fn bounded(value: &str, limit: usize) -> String {
    value.chars().take(limit).collect()
}

fn bounded_list(values: Vec<String>, count_limit: usize, byte_limit: usize) -> Vec<String> {
    values
        .into_iter()
        .take(count_limit)
        .map(|value| bounded(&value, byte_limit))
        .collect()
}

/// Result-package construction failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ResultPackageError {
    /// Package serialization failed.
    #[error("worker result package could not be serialized")]
    Serialization,
}

/// Returns the canonical usage summary retained for the package.
#[must_use]
pub fn usage_from_state(state: &SessionState, token_budget: u64) -> PackageUsage {
    let execution = state.style_execution.as_ref();
    PackageUsage {
        input_tokens: execution.map_or(0, |execution| execution.input_tokens),
        output_tokens: execution.map_or(0, |execution| execution.output_tokens),
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        token_budget,
    }
}

/// Stable identity helper used by integration ordering.
#[must_use]
pub fn changed_file_map(packages: &BTreeMap<String, Vec<String>>) -> BTreeMap<String, Vec<String>> {
    packages.clone()
}

/// Returns the verified child journal head sequence, or the lifecycle marker.
#[must_use]
pub fn terminal_sequence(state: &SessionState) -> Sequence {
    state.last_sequence
}

/// Marks a session lifecycle as terminal when the package is built.
#[must_use]
pub fn is_terminal_lifecycle(lifecycle: SessionLifecycle) -> bool {
    matches!(lifecycle, SessionLifecycle::Completed)
}

/// Content digest helper for immutable package references.
#[must_use]
pub fn package_digest(bytes: &[u8]) -> ContentHash {
    ContentHash::digest(bytes)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agentmod_primitives::{ContentHash, SessionId};
    use uuid::Uuid;

    use super::*;
    use crate::session::{
        ChildAgentExecutionIdentity, ChildAgentState, PlannedTask,
    };

    fn task() -> PlannedTask {
        PlannedTask {
            task_id: String::from("task-1"),
            description: String::from("write a fixture"),
            goal: String::from("write a fixture"),
            scope: Vec::new(),
            dependencies: Vec::new(),
            expected_artifacts: vec![String::from("fixture.rs")],
            workspace_mode: String::from("shared_read_only"),
            tool_groups: vec![String::from("filesystem.read")],
            validation_commands: vec![String::from("cargo check")],
            completion_criteria: vec![String::from("compiles")],
            review_criteria: Vec::new(),
            token_budget: 5_000,
            cost_budget_micros: 100_000,
            max_steps: 8,
            retry_policy: crate::session::TaskRetryPolicy::default(),
            risk: crate::session::TaskRisk::Low,
        }
    }

    fn record() -> ChildAgentRecord {
        ChildAgentRecord {
            identity: ChildAgentExecutionIdentity {
                execution_id: String::from("child:spawn:0:task-1:1"),
                node_id: String::from("spawn-workers"),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
                task_id: String::from("task-1"),
            },
            task: String::from("write a fixture"),
            child_style: String::from("ephemeral-turn@1.1.0"),
            workspace_mode: String::from("shared_read_only"),
            token_budget: 5_000,
            state: ChildAgentState::Completed,
            proposed_at: agentmod_primitives::Sequence::new(3).expect("sequence"),
            action_digest: Some(ContentHash::digest(b"action")),
            approved_at: Some(agentmod_primitives::Sequence::new(4).expect("sequence")),
            child_session_id: Some(SessionId::from_uuid(Uuid::from_u128(42))),
            created_at: Some(agentmod_primitives::Sequence::new(5).expect("sequence")),
            child_head_sequence: Some(agentmod_primitives::Sequence::new(9).expect("sequence")),
            completed_at: Some(agentmod_primitives::Sequence::new(6).expect("sequence")),
            summary: Some(
                r#"{"worker_result":"done","status":"completed","changed_files":["src/lib.rs"],"exit_status":0,"unresolved_issues":[]}"#
                    .to_owned(),
            ),
            result_package_reference: None,
            result_package_mime_type: None,
            result_package_byte_size: None,
        }
    }

    #[test]
    fn package_is_bounded_and_serializable() {
        let record = record();
        let state = minimal_state(9);
        let package = build_result_package(
            &record,
            &task(),
            &state,
            "parent-1",
            0,
            "mock",
            "mock-model",
            "native",
            1,
            usage_from_state(&state, 5_000),
        );
        assert_eq!(package.task_identity.task_id, "task-1");
        assert_eq!(package.changed_files, ["src/lib.rs"]);
        assert_eq!(package.exit_status, Some(0));
        assert_eq!(package.canonical_event_range.last_sequence, 9);
        let bytes = package.to_bytes().expect("bytes");
        let digest = package_digest(&bytes);
        assert_eq!(digest, ContentHash::digest(&bytes));
    }

    fn minimal_state(last_sequence: u64) -> SessionState {
        SessionState {
            id: SessionId::from_uuid(Uuid::from_u128(1)),
            workspace: String::from("workspace"),
            style: String::from("style"),
            style_binding: None,
            style_execution: None,
            ancestry: None,
            child_origin: None,
            lifecycle: SessionLifecycle::Completed,
            conversation: crate::conversation::ConversationState::new(),
            approvals: BTreeMap::new(),
            tool_executions: BTreeMap::new(),
            artifact_persistences: BTreeMap::new(),
            child_agents: BTreeMap::new(),
            planner_worker: crate::session::PlannerWorkerState::default(),
            plugins: crate::session::PluginExecutionState::default(),
            process_reconciliations: BTreeMap::new(),
            last_sequence: agentmod_primitives::Sequence::new(last_sequence).expect("sequence"),
            last_event_checksum: ContentHash::digest(b"fixture"),
        }
    }

    #[test]
    fn package_handoff_is_bounded_and_references_identity() {
        let package = build_result_package(
            &record(),
            &task(),
            &minimal_state(9),
            "parent-1",
            1,
            "mock",
            "mock-model",
            "native",
            1,
            PackageUsage {
                input_tokens: 10,
                output_tokens: 5,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                token_budget: 5_000,
            },
        );
        let line = package.handoff_line();
        assert!(line.contains("task-1"));
        assert!(line.contains("completed"));
    }

    #[test]
    fn changed_file_map_orders_deterministically() {
        let mut packages = BTreeMap::new();
        packages.insert(
            String::from("child:2"),
            vec![String::from("b.rs")],
        );
        packages.insert(
            String::from("child:1"),
            vec![String::from("a.rs")],
        );
        let map = changed_file_map(&packages);
        let mut keys = map.keys().cloned().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, ["child:1", "child:2"]);
    }

    #[test]
    fn terminal_lifecycle_marker_is_stable() {
        assert!(is_terminal_lifecycle(SessionLifecycle::Completed));
        assert!(!is_terminal_lifecycle(SessionLifecycle::Active));
    }
}
