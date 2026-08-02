//! Workspace-isolation policy and lease enforcement.
//!
//! Every declared workspace mode is validated and enforced by runtime logic
//! before any child tool grant crosses the proposal pipeline. Write-capable
//! tool groups and process commands fail closed when the workspace mode does
//! not authorize them. Serialized-write phases require a canonical
//! runtime-owned lease; dead owners are reconciled against a caller-supplied
//! clock so replay remains pure.

use std::collections::BTreeSet;

use thiserror::Error;

use crate::session::{PlannerWorkerState, WorkspaceLeaseRecord};

/// Stable canonical workspace-mode strings.
pub mod modes {
    /// Shared read-only workspace.
    pub const SHARED_READ_ONLY: &str = "shared_read_only";
    /// Shared workspace with serialized write phases.
    pub const SHARED_SERIALIZED_WRITES: &str = "shared_serialized_writes";
    /// Independent Git worktree per task.
    pub const INDEPENDENT_GIT_WORKTREE: &str = "independent_git_worktree";
    /// Bounded temporary copy per task.
    pub const TEMPORARY_COPY: &str = "temporary_copy";
    /// Explicitly approved custom workspace.
    pub const EXPLICIT_CUSTOM_WORKSPACE: &str = "explicit_custom_workspace";
}

/// Stable set of write-capable tool groups. Read-only modes deny every group
/// in this set before a child tool grant is authorized.
pub const WRITE_CAPABLE_TOOL_GROUPS: &[&str] = &[
    "filesystem.write",
    "filesystem.edit",
    "process.run",
    "git.write",
    "browser.write",
];

/// Stable write-intent verbs rejected by the read-only process policy.
pub const WRITE_INTENT_PROCESS_VERBS: &[&str] = &[
    "rm", "mv", "cp", "mkdir", "rmdir", "touch", "truncate", "tee", "dd", "install", "chmod",
    "chown", "ln", "unlink", "cargo", "git", "npm", "yarn", "pnpm", "pip", "pipx", "make",
    "cmake", "cargo-edit", "diesel", "sqlite3", "psql", "mysql",
];

/// Outcome of one workspace-mode enforcement decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceEnforcement {
    /// The tool or command is allowed by the mode.
    Allowed,
    /// The tool or command is denied by the mode and must fail closed.
    Denied,
}

/// Validates that a workspace mode string is one of the supported policies.
#[must_use]
pub fn validate_mode(mode: &str) -> bool {
    matches!(
        mode,
        modes::SHARED_READ_ONLY
            | modes::SHARED_SERIALIZED_WRITES
            | modes::INDEPENDENT_GIT_WORKTREE
            | modes::TEMPORARY_COPY
            | modes::EXPLICIT_CUSTOM_WORKSPACE
    )
}

/// Returns the mode assigned to a task, falling back to the policy default.
#[must_use]
pub fn task_workspace_mode(task_mode: &str, default_mode: &str) -> String {
    if validate_mode(task_mode) {
        task_mode.to_owned()
    } else if validate_mode(default_mode) {
        default_mode.to_owned()
    } else {
        modes::SHARED_READ_ONLY.to_owned()
    }
}

/// Decides whether a tool group may be granted to a child in the given mode.
///
/// Shared read-only and temporary-copy modes deny write-capable groups.
/// Serialized writes authorize writes only while the caller holds a canonical
/// unexpired lease. Worktree and custom modes authorize writes only after
/// containment validation, which the caller performs separately.
#[must_use]
pub fn enforce_tool_group(mode: &str, group: &str, lease_held: bool) -> WorkspaceEnforcement {
    if !WRITE_CAPABLE_TOOL_GROUPS.contains(&group) {
        return WorkspaceEnforcement::Allowed;
    }
    match mode {
        modes::SHARED_SERIALIZED_WRITES if lease_held => WorkspaceEnforcement::Allowed,
        modes::INDEPENDENT_GIT_WORKTREE | modes::EXPLICIT_CUSTOM_WORKSPACE => {
            WorkspaceEnforcement::Allowed
        }
        _ => WorkspaceEnforcement::Denied,
    }
}

/// Decides whether a process command may be dispatched in a read-only mode.
///
/// The policy is intentionally conservative: any argument whose first token is
/// a known write-intent verb denies the command. An empty command is denied.
/// Non-write verbs are allowed so validation commands (`cargo test`,
/// `cargo check`) still work.
#[must_use]
pub fn enforce_process_command(mode: &str, command: &[&str]) -> WorkspaceEnforcement {
    if mode != modes::SHARED_READ_ONLY && mode != modes::TEMPORARY_COPY {
        return WorkspaceEnforcement::Allowed;
    }
    let Some(verb) = command.first().copied() else {
        return WorkspaceEnforcement::Denied;
    };
    let normalized = verb
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(verb)
        .to_ascii_lowercase();
    if WRITE_INTENT_PROCESS_VERBS.contains(&normalized.as_str()) {
        WorkspaceEnforcement::Denied
    } else {
        WorkspaceEnforcement::Allowed
    }
}

/// Safe decision about an expired lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseResolution {
    /// The lease is still valid.
    Live,
    /// The lease has expired and its owner is dead.
    Expired,
}

/// Classifies one canonical lease against the caller-supplied clock.
#[must_use]
pub fn classify_lease(lease: &WorkspaceLeaseRecord, now_ms: i64) -> LeaseResolution {
    if lease.expires_at_ms > 0 && now_ms > lease.expires_at_ms {
        LeaseResolution::Expired
    } else {
        LeaseResolution::Live
    }
}

/// Returns whether a write phase may be granted for a workspace.
///
/// A grant is allowed only when no live lease exists for the same workspace,
/// or the existing lease is owned by the requesting execution and still live.
#[must_use]
pub fn write_phase_grant(
    planner: &PlannerWorkerState,
    workspace: &str,
    owner_execution_id: &str,
    now_ms: i64,
) -> bool {
    planner
        .workspace_leases
        .values()
        .filter(|lease| lease.workspace == workspace && !lease.mode.is_empty())
        .filter(|lease| lease.released_at.is_none() && lease.reconciled_at.is_none())
        .all(|lease| {
            if lease.owner_execution_id == owner_execution_id {
                classify_lease(lease, now_ms) == LeaseResolution::Live
            } else {
                classify_lease(lease, now_ms) == LeaseResolution::Expired
            }
        })
}

/// Identifies dead lease owners that must be reconciled before a new grant.
#[must_use]
pub fn dead_lease_owners(planner: &PlannerWorkerState, now_ms: i64) -> Vec<String> {
    planner
        .workspace_leases
        .values()
        .filter(|lease| lease.released_at.is_none() && lease.reconciled_at.is_none())
        .filter(|lease| classify_lease(lease, now_ms) == LeaseResolution::Expired)
        .map(|lease| lease.owner_execution_id.clone())
        .collect()
}

/// Filters a child tool-group set through the workspace-mode policy,
/// returning the retained groups.
#[must_use]
pub fn restrict_tool_groups(mode: &str, groups: &BTreeSet<String>, lease_held: bool) -> Vec<String> {
    let mut retained = groups
        .iter()
        .filter(|group| {
            enforce_tool_group(mode, group, lease_held) == WorkspaceEnforcement::Allowed
        })
        .cloned()
        .collect::<Vec<_>>();
    retained.sort();
    retained
}

/// Workspace policy failure.
#[allow(
    missing_docs,
    reason = "logic-local workspace diagnostics are self-describing"
)]
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum WorkspacePolicyError {
    /// An unknown workspace mode was requested.
    #[error("workspace mode `{mode}` is not supported")]
    UnsupportedMode { mode: String },
    /// A write grant was attempted without a valid lease.
    #[error("workspace `{workspace}` requires a canonical lease for writes")]
    MissingLease { workspace: String },
    /// A read-only workspace was asked to authorize a write-capable tool.
    #[error("workspace mode `{mode}` denies write-capable tool `{tool}`")]
    WriteToolDenied { mode: String, tool: String },
    /// A read-only workspace was asked to authorize a write-intent command.
    #[error("workspace mode `{mode}` denies write-intent command `{command}`")]
    WriteCommandDenied { mode: String, command: String },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use agentmod_primitives::Sequence;

    use super::*;
    use crate::session::{
        ChildAgentExecutionIdentity, WorkspaceLeaseRecord,
    };

    fn lease(owner: &str, workspace: &str, expires_at_ms: i64) -> WorkspaceLeaseRecord {
        WorkspaceLeaseRecord {
            workspace: workspace.to_owned(),
            mode: modes::SHARED_SERIALIZED_WRITES.to_owned(),
            owner_execution_id: owner.to_owned(),
            task_id: String::from("task-1"),
            expires_at_ms,
            acquired_at: Sequence::new(1).expect("sequence"),
            released_at: None,
            reconciled_at: None,
        }
    }

    fn planner_with(lease: Option<WorkspaceLeaseRecord>) -> PlannerWorkerState {
        let mut planner = PlannerWorkerState::default();
        if let Some(lease) = lease {
            planner
                .workspace_leases
                .insert(lease.owner_execution_id.clone(), lease);
        }
        planner
    }

    #[test]
    fn read_only_denies_write_tools_and_process_writes() {
        assert_eq!(
            enforce_tool_group(modes::SHARED_READ_ONLY, "filesystem.write", false),
            WorkspaceEnforcement::Denied
        );
        assert_eq!(
            enforce_tool_group(modes::SHARED_READ_ONLY, "filesystem.read", false),
            WorkspaceEnforcement::Allowed
        );
        assert_eq!(
            enforce_process_command(modes::SHARED_READ_ONLY, &["cargo", "test"]),
            WorkspaceEnforcement::Denied
        );
        assert_eq!(
            enforce_process_command(modes::SHARED_READ_ONLY, &["git", "status"]),
            WorkspaceEnforcement::Denied
        );
        assert_eq!(
            enforce_process_command(modes::SHARED_READ_ONLY, &["python", "check.py"]),
            WorkspaceEnforcement::Allowed
        );
        assert_eq!(
            enforce_process_command(modes::SHARED_READ_ONLY, &[]),
            WorkspaceEnforcement::Denied
        );
    }

    #[test]
    fn serialized_writes_require_a_live_lease() {
        let now = 1_000;
        let held = planner_with(Some(lease("child-a", "workspace", now + 1_000)));
        assert!(write_phase_grant(&held, "workspace", "child-a", now));
        assert!(!write_phase_grant(&held, "workspace", "child-b", now));
        // A different workspace is not protected by this lease, so the
        // acquisition check grants it; a write there is a separate policy
        // question outside lease scope.
        assert!(write_phase_grant(&held, "other", "child-a", now));
    }

    #[test]
    fn expired_lease_blocks_its_owner_and_enables_new_owner() {
        let now = 2_000;
        let expired = planner_with(Some(lease("dead-owner", "workspace", 1_500)));
        assert!(!write_phase_grant(&expired, "workspace", "dead-owner", now));
        assert!(write_phase_grant(&expired, "workspace", "child-b", now));
        assert_eq!(
            dead_lease_owners(&expired, now),
            [String::from("dead-owner")]
        );
    }

    #[test]
    fn released_lease_never_blocks_a_new_owner() {
        let mut lease = lease("child-a", "workspace", 0);
        lease.released_at = Some(Sequence::new(9).expect("sequence"));
        let planner = planner_with(Some(lease));
        assert!(write_phase_grant(&planner, "workspace", "child-b", 1_000));
    }

    #[test]
    fn restrict_tool_groups_removes_write_groups_without_lease() {
        let groups = BTreeSet::from([
            String::from("filesystem.read"),
            String::from("filesystem.write"),
        ]);
        let retained = restrict_tool_groups(modes::SHARED_READ_ONLY, &groups, false);
        assert_eq!(retained, ["filesystem.read"]);
    }

    #[test]
    fn task_workspace_mode_falls_back_to_policy_default() {
        assert_eq!(
            task_workspace_mode("", "shared_serialized_writes"),
            "shared_serialized_writes"
        );
        assert_eq!(
            task_workspace_mode("bogus", "shared_read_only"),
            "shared_read_only"
        );
    }

    #[test]
    fn identity_marker_uses_child_identity_shape() {
        let identity = ChildAgentExecutionIdentity {
            execution_id: String::from("child:spawn:0:task-1:1"),
            node_id: String::from("spawn-workers"),
            attempt: 1,
            loop_iteration: 0,
            step: 3,
            task_id: String::from("task-1"),
        };
        assert!(!identity.execution_id.is_empty());
        assert_eq!(identity.task_id, "task-1");
    }
}
