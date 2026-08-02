//! Deterministic integration over applied worker result packages.
//!
//! The integrator verifies that every expected result package exists, detects
//! overlapping and conflicting changes deterministically, records exactly what
//! was applied, and produces an integration-result artifact. Worker changes
//! are never silently dropped: a conflicting package fails the integration
//! instead of being discarded.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::result_package::WorkerResultPackage;

/// Integration-result artifact schema version.
pub const INTEGRATION_RESULT_SCHEMA_VERSION: u32 = 1;

/// Immutable integration-result artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IntegrationResultArtifact {
    /// Artifact schema version.
    pub schema_version: u32,
    /// Parent session.
    pub parent_session_id: String,
    /// Zero-based orchestration iteration.
    pub loop_iteration: u32,
    /// Deterministically ordered applied child execution IDs.
    pub applied_child_execution_ids: Vec<String>,
    /// Changed files grouped by child execution ID.
    pub changed_files: BTreeMap<String, Vec<String>>,
    /// Paths changed by more than one worker.
    pub overlapping_paths: Vec<String>,
    /// Paths changed by more than one worker with different change identity.
    pub conflicting_paths: Vec<String>,
    /// Whether the integration was applied without conflict.
    pub applied: bool,
    /// Integration validation exit status, when run.
    pub exit_status: Option<i32>,
    /// Bounded summary.
    pub summary: String,
}

impl IntegrationResultArtifact {
    /// Serializes the artifact to bounded JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns [`IntegrationError::Serialization`] when encoding fails.
    pub fn to_bytes(&self) -> Result<Vec<u8>, IntegrationError> {
        serde_json::to_vec(self).map_err(|_| IntegrationError::Serialization)
    }

    /// Returns a validation exit status for canonical retention when the
    /// integration validation produced one.
    #[must_use]
    pub fn exit_status_reference(&self) -> Option<i32> {
        self.exit_status
    }
}

/// Deterministic integration outcome before any artifact is written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrationDecision {
    /// Deterministically ordered child execution IDs whose changes apply.
    pub applied_child_execution_ids: Vec<String>,
    /// Paths changed by more than one worker.
    pub overlapping_paths: Vec<String>,
    /// Paths with conflicting change identity.
    pub conflicting_paths: Vec<String>,
    /// Whether the integration may be applied.
    pub applied: bool,
}

/// Computes the deterministic integration decision for one iteration.
///
/// Packages are ordered by child execution ID. A path changed by more than one
/// worker is overlapping; when the same path carries a different change
/// identity (diff reference or changed-file marker) it is also conflicting and
/// the integration fails closed.
#[must_use]
pub fn decide_integration(packages: &[WorkerResultPackage]) -> IntegrationDecision {
    let mut by_execution = BTreeMap::new();
    for package in packages {
        by_execution.insert(
            package.child_identity.execution_id.clone(),
            package,
        );
    }
    let applied = by_execution.keys().cloned().collect::<Vec<_>>();
    let mut changed_by_path: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut change_identity: BTreeMap<&str, String> = BTreeMap::new();
    for package in packages {
        for path in &package.changed_files {
            changed_by_path.entry(path).or_default().insert(&package.child_identity.execution_id);
            let identity = package
                .diff_reference
                .clone()
                .unwrap_or_else(|| format!("change:{}", package.child_identity.execution_id));
            change_identity
                .entry(path)
                .and_modify(|existing| {
                    if existing != &identity {
                        *existing = format!("{existing}|{identity}");
                    }
                })
                .or_insert(identity);
        }
    }
    let mut overlapping = changed_by_path
        .iter()
        .filter(|(_, owners)| owners.len() > 1)
        .map(|(path, _)| (*path).to_owned())
        .collect::<Vec<_>>();
    overlapping.sort();
    let mut conflicting = overlapping
        .iter()
        .filter(|path| change_identity.get(path.as_str()).is_some_and(|identity| identity.contains('|')))
        .cloned()
        .collect::<Vec<_>>();
    conflicting.sort();
    let applied_ok = conflicting.is_empty() && !applied.is_empty();
    IntegrationDecision {
        applied_child_execution_ids: applied,
        overlapping_paths: overlapping,
        conflicting_paths: conflicting,
        applied: applied_ok,
    }
}

/// Builds the immutable integration-result artifact from a decision.
#[must_use]
pub fn build_integration_artifact(
    parent_session_id: &str,
    loop_iteration: u32,
    packages: &[WorkerResultPackage],
    decision: &IntegrationDecision,
) -> IntegrationResultArtifact {
    let mut changed_files = BTreeMap::new();
    for package in packages {
        changed_files.insert(
            package.child_identity.execution_id.clone(),
            package.changed_files.clone(),
        );
    }
    IntegrationResultArtifact {
        schema_version: INTEGRATION_RESULT_SCHEMA_VERSION,
        parent_session_id: parent_session_id.to_owned(),
        loop_iteration,
        applied_child_execution_ids: decision.applied_child_execution_ids.clone(),
        changed_files,
        overlapping_paths: decision.overlapping_paths.clone(),
        conflicting_paths: decision.conflicting_paths.clone(),
        applied: decision.applied,
        exit_status: None,
        summary: if decision.applied {
            format!(
                "integrated {} worker package(s) with no conflicts",
                decision.applied_child_execution_ids.len()
            )
        } else {
            format!(
                "integration blocked by {} conflicting path(s)",
                decision.conflicting_paths.len()
            )
        },
    }
}

/// Integration failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum IntegrationError {
    /// Artifact serialization failed.
    #[error("integration result artifact could not be serialized")]
    Serialization,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result_package::{
        PackageChildIdentity, PackageEventRange, PackageProviderIdentity, PackageTaskIdentity,
        PackageUsage,
    };

    fn package(execution_id: &str, files: &[&str], diff: Option<&str>) -> WorkerResultPackage {
        WorkerResultPackage {
            schema_version: 1,
            task_identity: PackageTaskIdentity {
                parent_session_id: String::from("parent"),
                task_id: String::from("task"),
                revision: 0,
                goal: String::from("goal"),
                workspace_mode: String::from("shared_read_only"),
            },
            child_identity: PackageChildIdentity {
                child_session_id: String::from("child"),
                execution_id: execution_id.to_owned(),
                style: String::from("ephemeral-turn@1.1.0"),
                depth: 1,
            },
            provider_identity: PackageProviderIdentity {
                provider: String::from("mock"),
                model: String::from("mock"),
                harness: String::from("native"),
            },
            summary: String::from("done"),
            changed_files: files.iter().map(|file| (*file).to_owned()).collect(),
            diff_reference: diff.map(str::to_owned),
            validation_commands: Vec::new(),
            stdout_reference: None,
            stderr_reference: None,
            exit_status: Some(0),
            lsp_diagnostics: Vec::new(),
            generated_artifacts: Vec::new(),
            unresolved_issues: Vec::new(),
            completion_reason: String::from("child_completed"),
            usage: PackageUsage {
                input_tokens: 0,
                output_tokens: 0,
                cache_read_tokens: 0,
                cache_write_tokens: 0,
                token_budget: 1_000,
            },
            canonical_event_range: PackageEventRange {
                first_sequence: 1,
                last_sequence: 2,
            },
        }
    }

    #[test]
    fn disjoint_changes_apply_deterministically() {
        let packages = vec![
            package("child:1", &["src/a.rs"], None),
            package("child:2", &["src/b.rs"], None),
        ];
        let decision = decide_integration(&packages);
        assert!(decision.applied);
        assert_eq!(
            decision.applied_child_execution_ids,
            ["child:1", "child:2"]
        );
        assert!(decision.overlapping_paths.is_empty());
        assert!(decision.conflicting_paths.is_empty());
    }

    #[test]
    fn same_file_with_same_change_identity_is_overlapping_but_not_conflicting() {
        let packages = vec![
            package("child:1", &["src/a.rs"], Some("diff:1")),
            package("child:2", &["src/a.rs"], Some("diff:1")),
        ];
        let decision = decide_integration(&packages);
        assert!(decision.applied);
        assert_eq!(decision.overlapping_paths, ["src/a.rs"]);
        assert!(decision.conflicting_paths.is_empty());
    }

    #[test]
    fn same_file_with_different_change_identity_fails_closed() {
        let packages = vec![
            package("child:1", &["src/a.rs"], Some("diff:1")),
            package("child:2", &["src/a.rs"], Some("diff:2")),
        ];
        let decision = decide_integration(&packages);
        assert!(!decision.applied);
        assert_eq!(decision.conflicting_paths, ["src/a.rs"]);
    }

    #[test]
    fn empty_integration_never_applies() {
        let decision = decide_integration(&[]);
        assert!(!decision.applied);
        assert!(decision.applied_child_execution_ids.is_empty());
    }

    #[test]
    fn artifact_is_serializable_and_records_exact_applied_set() {
        let packages = vec![package("child:1", &["src/a.rs"], None)];
        let decision = decide_integration(&packages);
        let artifact =
            build_integration_artifact("parent", 0, &packages, &decision);
        let bytes = artifact.to_bytes().expect("bytes");
        let decoded: IntegrationResultArtifact =
            serde_json::from_slice(&bytes).expect("decode");
        assert!(decoded.applied);
        assert_eq!(decoded.loop_iteration, 0);
        assert_eq!(decoded.applied_child_execution_ids, ["child:1"]);
        assert_eq!(
            decoded.changed_files.get("child:1").expect("files"),
            &[String::from("src/a.rs")]
        );
    }
}
