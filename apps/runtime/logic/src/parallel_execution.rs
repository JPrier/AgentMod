//! Replayable bounded parallel execution and generic join semantics.
//!
//! This module owns business state only. It derives stable identities, validates
//! branch transitions and shared writes, and produces deterministic dispatch and
//! join descriptors. Runtime orchestration remains responsible for committing
//! those descriptors as canonical events and performing any external work.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_engine::{
    JoinArtifactCollection, JoinFailurePolicy, JoinOrderingPolicy, JoinResultProjection,
    NodeConfiguration, ParallelJoinPolicy, ParallelSerializationPolicy, VariableDeclaration,
    VariableMergePolicy, VariableScope,
};
use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::node_execution::NodeWorkIdentity;

const MAX_MEMBERS: usize = 1_024;
const MAX_IDENTIFIER_BYTES: usize = 512;
const MAX_RESULT_BYTES: usize = 64 * 1_024;
const MAX_ARTIFACTS_PER_RESULT: usize = 1_024;
const MAX_FAILURE_CODE_BYTES: usize = 256;

/// One statically compiled branch target and its declared writes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParallelBranchSpec {
    /// Immutable graph-owned reference used by configured join membership.
    pub member_reference: String,
    /// Exact compiled entry node for the branch.
    pub target_node_id: String,
    /// Canonical variables the branch may write before the join.
    #[serde(default)]
    pub write_variables: BTreeSet<String>,
    /// Canonical workspace resources the branch may write before the join.
    #[serde(default)]
    pub workspace_resources: BTreeSet<String>,
}

/// Persisted binding from one graph-owned join reference to runtime identity.
///
/// Runtime orchestration commits this set verbatim at parallel initialization.
/// Recovery loads it instead of deriving a fresh mapping.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParallelMemberBinding {
    /// Immutable reference present in the join configuration.
    pub configured_reference: String,
    /// Exact compiled branch entry node.
    pub target_node_id: String,
    /// Zero-based compiled outgoing-edge order.
    pub branch_index: u32,
    /// Runtime-owned stable branch identity.
    pub branch_id: String,
}

/// Replayable lifecycle of one parallel branch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ParallelBranchState {
    /// Waiting for an available dispatch slot.
    Queued,
    /// A canonical dispatch event exists but execution has not acknowledged it.
    Dispatched,
    /// The branch acknowledged the dispatch and is executing.
    Running,
    /// The branch completed with a bounded result.
    Completed {
        /// Canonical successful-completion sequence.
        completion_sequence: u64,
        /// Bounded branch result.
        result: JoinMemberResult,
    },
    /// The branch terminally failed.
    Failed {
        /// Stable redacted failure code.
        code: String,
    },
    /// The branch was cancelled.
    Cancelled {
        /// Stable redacted cancellation code.
        code: String,
    },
}

impl ParallelBranchState {
    fn is_active(&self) -> bool {
        matches!(self, Self::Dispatched | Self::Running)
    }

    fn is_incomplete(&self) -> bool {
        matches!(self, Self::Queued | Self::Dispatched | Self::Running)
    }
}

/// One replayable branch record.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParallelBranchRecord {
    /// Stable identity derived from the owning node work and target position.
    pub branch_id: String,
    /// Stable exactly-once dispatch identity.
    pub dispatch_id: String,
    /// Zero-based order in the compiled outgoing-edge list.
    pub branch_index: u32,
    /// Exact compiled branch entry node.
    pub target_node_id: String,
    /// Current canonical lifecycle state.
    pub state: ParallelBranchState,
}

/// Canonical transition applied by replay to one branch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "transition", rename_all = "snake_case")]
pub enum ParallelBranchTransition {
    /// Commits the stable dispatch identity.
    Dispatch {
        /// Stable branch identity.
        branch_id: String,
        /// Exact derived dispatch identity.
        dispatch_id: String,
    },
    /// Records execution acknowledgement.
    Start {
        /// Stable branch identity.
        branch_id: String,
        /// Exact derived dispatch identity.
        dispatch_id: String,
    },
    /// Records successful terminal completion.
    Complete {
        /// Stable branch identity.
        branch_id: String,
        /// Exact derived dispatch identity.
        dispatch_id: String,
        /// Next canonical successful-completion sequence.
        completion_sequence: u64,
        /// Bounded result.
        result: JoinMemberResult,
    },
    /// Records terminal failure.
    Fail {
        /// Stable branch identity.
        branch_id: String,
        /// Exact derived dispatch identity.
        dispatch_id: String,
        /// Stable redacted failure code.
        code: String,
    },
    /// Records cancellation of incomplete work.
    Cancel {
        /// Stable branch identity.
        branch_id: String,
        /// Exact derived dispatch identity.
        dispatch_id: String,
        /// Stable redacted cancellation code.
        code: String,
    },
}

impl ParallelBranchTransition {
    fn branch_id(&self) -> &str {
        match self {
            Self::Dispatch { branch_id, .. }
            | Self::Start { branch_id, .. }
            | Self::Complete { branch_id, .. }
            | Self::Fail { branch_id, .. }
            | Self::Cancel { branch_id, .. } => branch_id,
        }
    }

    fn dispatch_id(&self) -> &str {
        match self {
            Self::Dispatch { dispatch_id, .. }
            | Self::Start { dispatch_id, .. }
            | Self::Complete { dispatch_id, .. }
            | Self::Fail { dispatch_id, .. }
            | Self::Cancel { dispatch_id, .. } => dispatch_id,
        }
    }
}

/// Stable dispatch proposed for later commitment by runtime orchestration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BranchDispatchDescriptor {
    /// Stable branch identity.
    pub branch_id: String,
    /// Stable exactly-once dispatch identity.
    pub dispatch_id: String,
    /// Zero-based stable dispatch order.
    pub branch_index: u32,
    /// Exact compiled branch entry node.
    pub target_node_id: String,
}

/// Deterministic dispatch readiness for the current replayed state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParallelDispatchDecision {
    /// Dispatches that fit in the currently available concurrency slots.
    pub ready: Vec<BranchDispatchDescriptor>,
    /// Whether queued branches remain after the ready dispatches.
    pub backpressured: bool,
    /// Remaining queue occupancy after the ready dispatches.
    pub queued_after_dispatch: u32,
}

/// Replayable state for one parallel node-work attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParallelExecutionState {
    /// Exact immutable node-work identity.
    work: NodeWorkIdentity,
    /// Configured upper bound for concurrent branches.
    max_parallelism: u32,
    /// Configured upper bound for waiting branches.
    max_queue_depth: u32,
    /// Exact compiled join target.
    join_target: String,
    /// Fan-out-owned readiness constraint paired with the join configuration.
    join_policy: ParallelJoinPolicy,
    /// Optional deterministic shared-resource serialization.
    serialization_policy: Option<ParallelSerializationPolicy>,
    /// Branches keyed by stable branch identity.
    branches: BTreeMap<String, ParallelBranchRecord>,
    /// Immutable configured-reference to stable-identity bindings.
    member_bindings: Vec<ParallelMemberBinding>,
}

impl ParallelExecutionState {
    /// Constructs initial queued state after validating all bounds and writes.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelExecutionError`] when configuration, identifiers,
    /// capacity, or shared-write declarations are invalid.
    pub fn new(
        work: NodeWorkIdentity,
        configuration: &NodeConfiguration,
        branch_specs: &[ParallelBranchSpec],
        variables: &[VariableDeclaration],
    ) -> Result<Self, ParallelExecutionError> {
        let NodeConfiguration::ParallelBranch {
            max_parallelism,
            max_queue_depth,
            join_target,
            join_policy,
            variable_merge_policies,
            serialization_policy,
            ..
        } = configuration
        else {
            return Err(ParallelExecutionError::WrongConfiguration {
                expected: "parallel_branch",
            });
        };
        if *max_parallelism == 0 || *max_queue_depth == 0 {
            return Err(ParallelExecutionError::InvalidCapacity);
        }
        if branch_specs.len() < 2 || branch_specs.len() > MAX_MEMBERS {
            return Err(ParallelExecutionError::InvalidMemberCount {
                count: branch_specs.len(),
            });
        }
        validate_identifier("join target", join_target)?;
        validate_parallel_writes(
            branch_specs,
            variables,
            variable_merge_policies,
            *serialization_policy,
        )?;

        let effective_parallelism = if serialization_policy.is_some() {
            1_usize
        } else {
            usize::try_from(*max_parallelism).unwrap_or(usize::MAX)
        };
        let waiting = branch_specs.len().saturating_sub(effective_parallelism);
        if waiting > usize::try_from(*max_queue_depth).unwrap_or(usize::MAX) {
            return Err(ParallelExecutionError::QueueCapacityExceeded {
                branches: branch_specs.len(),
                effective_parallelism,
                max_queue_depth: *max_queue_depth,
            });
        }

        let mut branches = BTreeMap::new();
        let mut configured_references = BTreeSet::new();
        let mut member_bindings = Vec::with_capacity(branch_specs.len());
        for (index, spec) in branch_specs.iter().enumerate() {
            validate_identifier("join member reference", &spec.member_reference)?;
            validate_identifier("branch target", &spec.target_node_id)?;
            if !configured_references.insert(spec.member_reference.clone()) {
                return Err(ParallelExecutionError::DuplicateConfiguredReference {
                    reference: spec.member_reference.clone(),
                });
            }
            let branch_index =
                u32::try_from(index).map_err(|_| ParallelExecutionError::InvalidMemberCount {
                    count: branch_specs.len(),
                })?;
            let branch_id = stable_branch_id(&work, &spec.target_node_id, branch_index)?;
            let dispatch_id = stable_dispatch_id(&work, &branch_id)?;
            member_bindings.push(ParallelMemberBinding {
                configured_reference: spec.member_reference.clone(),
                target_node_id: spec.target_node_id.clone(),
                branch_index,
                branch_id: branch_id.clone(),
            });
            let previous = branches.insert(
                branch_id.clone(),
                ParallelBranchRecord {
                    branch_id,
                    dispatch_id,
                    branch_index,
                    target_node_id: spec.target_node_id.clone(),
                    state: ParallelBranchState::Queued,
                },
            );
            if previous.is_some() {
                return Err(ParallelExecutionError::DuplicateBranchIdentity);
            }
        }

        Ok(Self {
            work,
            max_parallelism: *max_parallelism,
            max_queue_depth: *max_queue_depth,
            join_target: join_target.clone(),
            join_policy: *join_policy,
            serialization_policy: *serialization_policy,
            branches,
            member_bindings,
        })
    }

    /// Returns the exact immutable node-work identity.
    #[must_use]
    pub const fn work(&self) -> &NodeWorkIdentity {
        &self.work
    }

    /// Returns the exact compiled join target.
    #[must_use]
    pub fn join_target(&self) -> &str {
        &self.join_target
    }

    /// Returns the fan-out-owned join policy.
    #[must_use]
    pub const fn join_policy(&self) -> ParallelJoinPolicy {
        self.join_policy
    }

    /// Returns immutable persisted reference bindings.
    #[must_use]
    pub fn member_bindings(&self) -> &[ParallelMemberBinding] {
        &self.member_bindings
    }

    /// Returns replay-derived branch records keyed by stable identity.
    #[must_use]
    pub const fn branches(&self) -> &BTreeMap<String, ParallelBranchRecord> {
        &self.branches
    }

    /// Produces the next stable dispatch batch without mutating state.
    #[must_use]
    pub fn dispatch_decision(&self) -> ParallelDispatchDecision {
        let effective_limit = if self.serialization_policy.is_some() {
            1_usize
        } else {
            usize::try_from(self.max_parallelism).unwrap_or(usize::MAX)
        };
        let active = self
            .branches
            .values()
            .filter(|branch| branch.state.is_active())
            .count();
        let capacity = effective_limit.saturating_sub(active);
        let queued: Vec<_> = self
            .ordered_branches()
            .into_iter()
            .filter(|branch| matches!(branch.state, ParallelBranchState::Queued))
            .collect();
        let ready = queued
            .iter()
            .take(capacity)
            .map(|branch| BranchDispatchDescriptor {
                branch_id: branch.branch_id.clone(),
                dispatch_id: branch.dispatch_id.clone(),
                branch_index: branch.branch_index,
                target_node_id: branch.target_node_id.clone(),
            })
            .collect::<Vec<_>>();
        let queued_after_dispatch = queued.len().saturating_sub(ready.len());
        ParallelDispatchDecision {
            ready,
            backpressured: queued_after_dispatch > 0,
            queued_after_dispatch: u32::try_from(queued_after_dispatch).unwrap_or(u32::MAX),
        }
    }

    /// Applies one already-committed canonical transition during live execution
    /// or replay.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelExecutionError`] for unknown identities, substituted
    /// dispatch identities, invalid payloads, or illegal/double transitions.
    pub fn apply(
        &mut self,
        transition: ParallelBranchTransition,
    ) -> Result<(), ParallelExecutionError> {
        let expected_completion_sequence =
            matches!(&transition, ParallelBranchTransition::Complete { .. })
                .then(|| self.next_completion_sequence())
                .transpose()?;
        let branch_id = transition.branch_id().to_owned();
        let branch = self.branches.get_mut(&branch_id).ok_or_else(|| {
            ParallelExecutionError::UnknownBranch {
                branch_id: branch_id.clone(),
            }
        })?;
        if transition.dispatch_id() != branch.dispatch_id {
            return Err(ParallelExecutionError::DispatchIdentityMismatch { branch_id });
        }

        match transition {
            ParallelBranchTransition::Dispatch { .. }
                if matches!(branch.state, ParallelBranchState::Queued) =>
            {
                branch.state = ParallelBranchState::Dispatched;
            }
            ParallelBranchTransition::Start { .. }
                if matches!(branch.state, ParallelBranchState::Dispatched) =>
            {
                branch.state = ParallelBranchState::Running;
            }
            ParallelBranchTransition::Complete {
                completion_sequence,
                result,
                ..
            } if matches!(branch.state, ParallelBranchState::Running) => {
                validate_member_result(&result)?;
                let expected_completion_sequence = expected_completion_sequence
                    .ok_or(ParallelExecutionError::CompletionSequenceExhausted)?;
                if completion_sequence != expected_completion_sequence {
                    return Err(ParallelExecutionError::InvalidCompletionSequence {
                        expected: expected_completion_sequence,
                        actual: completion_sequence,
                    });
                }
                branch.state = ParallelBranchState::Completed {
                    completion_sequence,
                    result,
                };
            }
            ParallelBranchTransition::Fail { code, .. }
                if matches!(branch.state, ParallelBranchState::Running) =>
            {
                validate_code(&code)?;
                branch.state = ParallelBranchState::Failed { code };
            }
            ParallelBranchTransition::Cancel { code, .. } if branch.state.is_incomplete() => {
                validate_code(&code)?;
                branch.state = ParallelBranchState::Cancelled { code };
            }
            transition => {
                return Err(ParallelExecutionError::IllegalTransition {
                    branch_id,
                    from: state_name(&branch.state),
                    transition: transition_name(&transition),
                });
            }
        }
        Ok(())
    }

    /// Builds the next successful completion transition using the canonical
    /// completion sequence.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelExecutionError`] when the branch is unknown, not
    /// running, or the result exceeds bounds.
    pub fn completion_transition(
        &self,
        branch_id: &str,
        result: JoinMemberResult,
    ) -> Result<ParallelBranchTransition, ParallelExecutionError> {
        validate_member_result(&result)?;
        let branch =
            self.branches
                .get(branch_id)
                .ok_or_else(|| ParallelExecutionError::UnknownBranch {
                    branch_id: branch_id.to_owned(),
                })?;
        if !matches!(branch.state, ParallelBranchState::Running) {
            return Err(ParallelExecutionError::IllegalTransition {
                branch_id: branch_id.to_owned(),
                from: state_name(&branch.state),
                transition: "complete",
            });
        }
        Ok(ParallelBranchTransition::Complete {
            branch_id: branch_id.to_owned(),
            dispatch_id: branch.dispatch_id.clone(),
            completion_sequence: self.next_completion_sequence()?,
            result,
        })
    }

    /// Produces stable cancellation transitions for every incomplete branch.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelExecutionError`] when the cancellation code is not a
    /// bounded stable diagnostic.
    pub fn cancellation_transitions(
        &self,
        code: &str,
    ) -> Result<Vec<ParallelBranchTransition>, ParallelExecutionError> {
        validate_code(code)?;
        Ok(self
            .ordered_branches()
            .into_iter()
            .filter(|branch| branch.state.is_incomplete())
            .map(|branch| ParallelBranchTransition::Cancel {
                branch_id: branch.branch_id.clone(),
                dispatch_id: branch.dispatch_id.clone(),
                code: code.to_owned(),
            })
            .collect())
    }

    /// Projects current branch state into generic join-member state.
    #[must_use]
    pub fn join_members(&self) -> BTreeMap<String, JoinMemberState> {
        self.branches
            .values()
            .map(|branch| {
                let state = match &branch.state {
                    ParallelBranchState::Queued
                    | ParallelBranchState::Dispatched
                    | ParallelBranchState::Running => JoinMemberState::Pending,
                    ParallelBranchState::Completed {
                        completion_sequence,
                        result,
                    } => JoinMemberState::Completed {
                        completion_sequence: *completion_sequence,
                        result: result.clone(),
                    },
                    ParallelBranchState::Failed { code } => {
                        JoinMemberState::Failed { code: code.clone() }
                    }
                    ParallelBranchState::Cancelled { code } => {
                        JoinMemberState::Cancelled { code: code.clone() }
                    }
                };
                (branch.branch_id.clone(), state)
            })
            .collect()
    }

    /// Returns join state keyed by immutable graph-owned references using only
    /// the persisted initialization bindings.
    ///
    /// # Errors
    ///
    /// Returns [`ParallelExecutionError`] when a replayed binding is missing,
    /// duplicate, unknown, or no longer matches the retained branch record.
    pub fn bound_join_members(
        &self,
    ) -> Result<BTreeMap<String, JoinMemberState>, ParallelExecutionError> {
        bind_parallel_join_members(&self.member_bindings, &self.join_members())
    }

    fn next_completion_sequence(&self) -> Result<u64, ParallelExecutionError> {
        self.branches
            .values()
            .filter_map(|branch| match branch.state {
                ParallelBranchState::Completed {
                    completion_sequence,
                    ..
                } => Some(completion_sequence),
                _ => None,
            })
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(ParallelExecutionError::CompletionSequenceExhausted)
    }

    fn ordered_branches(&self) -> Vec<&ParallelBranchRecord> {
        let mut branches = self.branches.values().collect::<Vec<_>>();
        branches.sort_by(|left, right| {
            (left.branch_index, &left.branch_id).cmp(&(right.branch_index, &right.branch_id))
        });
        branches
    }
}

/// Bounded successful member result consumed by a generic join.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JoinMemberResult {
    /// Bounded canonical value for inline projection.
    #[serde(default)]
    pub inline_value: Option<Value>,
    /// Stable canonical node-result reference.
    #[serde(default)]
    pub node_result_reference: Option<String>,
    /// Every immutable artifact reference returned by the member.
    #[serde(default)]
    pub artifact_references: BTreeSet<String>,
    /// Subset explicitly declared for declared-only collection.
    #[serde(default)]
    pub declared_artifact_references: BTreeSet<String>,
}

/// Replay-derived generic join-member state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum JoinMemberState {
    /// The member is absent or incomplete.
    Pending,
    /// The member completed successfully.
    Completed {
        /// Canonical successful-completion sequence.
        completion_sequence: u64,
        /// Bounded member result.
        result: JoinMemberResult,
    },
    /// The member terminally failed.
    Failed {
        /// Stable redacted failure code.
        code: String,
    },
    /// The member was cancelled.
    Cancelled {
        /// Stable redacted cancellation code.
        code: String,
    },
}

/// One deterministically projected successful join member.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProjectedJoinMember {
    /// Stable member identity.
    pub member_id: String,
    /// Inline value when configured.
    #[serde(default)]
    pub inline_value: Option<Value>,
    /// Node-result reference when configured.
    #[serde(default)]
    pub node_result_reference: Option<String>,
    /// Artifact references selected by the collection policy.
    #[serde(default)]
    pub artifact_references: Vec<String>,
}

/// Deterministic successful join descriptor for later canonical commitment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JoinReadyDescriptor {
    /// Successful members in the configured stable order.
    pub results: Vec<ProjectedJoinMember>,
    /// Number of successful configured members at evaluation.
    pub success_count: u32,
    /// Failed configured members in member-ID order.
    pub failed_members: Vec<String>,
    /// Cancelled configured members in member-ID order.
    pub cancelled_members: Vec<String>,
    /// Members still absent or pending in member-ID order.
    pub missing_members: Vec<String>,
    /// Incomplete members runtime should cancel after committing readiness.
    pub cancellation_targets: Vec<String>,
}

/// Stable reason a generic join is terminally unsuccessful.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinTerminalReason {
    /// A required member failed under fail-fast semantics.
    RequiredMemberFailed,
    /// Remaining members cannot satisfy the success threshold.
    SuccessThresholdImpossible,
    /// Required members finished without satisfying wait-required semantics.
    RequiredMembersInsufficient,
    /// The durable join timeout elapsed before readiness.
    TimedOut,
}

/// Deterministic failed join descriptor for later canonical commitment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JoinFailureDescriptor {
    /// Stable terminal reason.
    pub reason: JoinTerminalReason,
    /// Successful configured member count.
    pub success_count: u32,
    /// Failed configured members in member-ID order.
    pub failed_members: Vec<String>,
    /// Cancelled configured members in member-ID order.
    pub cancelled_members: Vec<String>,
    /// Members still absent or pending in member-ID order.
    pub missing_members: Vec<String>,
    /// Incomplete members runtime should cancel after committing failure.
    pub cancellation_targets: Vec<String>,
}

/// Deterministic generic join evaluation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum JoinDecision {
    /// The join remains replayably incomplete.
    Waiting {
        /// Configured members not yet terminal.
        missing_members: Vec<String>,
        /// Current successful member count.
        success_count: u32,
    },
    /// The join is ready for one canonical completion event.
    Ready(JoinReadyDescriptor),
    /// The join is terminally unsuccessful.
    Failed(JoinFailureDescriptor),
}

/// Evaluates a configured generic join solely from replayed canonical state.
///
/// `timeout_elapsed` must itself be derived from a committed durable timeout,
/// never from querying a live clock during replay.
///
/// # Errors
///
/// Returns [`ParallelExecutionError`] when configuration, member state, or
/// projected results violate declared bounds.
pub fn evaluate_join(
    configuration: &NodeConfiguration,
    members: &BTreeMap<String, JoinMemberState>,
    timeout_elapsed: bool,
) -> Result<JoinDecision, ParallelExecutionError> {
    let NodeConfiguration::JoinResults {
        required,
        optional,
        minimum_successes,
        failure_policy,
        ordering_policy,
        cancellation_propagates,
        result_projection,
        artifact_collection,
        ..
    } = configuration
    else {
        return Err(ParallelExecutionError::WrongConfiguration {
            expected: "join_results",
        });
    };
    let JoinSnapshot {
        successful,
        failed,
        cancelled,
        missing,
        required_failed,
        required_terminal,
    } = build_join_snapshot(required, optional, *minimum_successes, members)?;
    let success_count = u32::try_from(successful.len()).unwrap_or(u32::MAX);
    let possible_successes = successful.len().saturating_add(missing.len());
    let threshold = usize::try_from(*minimum_successes).unwrap_or(usize::MAX);

    let terminal_reason = match failure_policy {
        JoinFailurePolicy::FailFast if required_failed => {
            Some(JoinTerminalReason::RequiredMemberFailed)
        }
        JoinFailurePolicy::FailFast | JoinFailurePolicy::MinimumSuccess
            if possible_successes < threshold =>
        {
            Some(JoinTerminalReason::SuccessThresholdImpossible)
        }
        JoinFailurePolicy::WaitRequired if required_terminal && successful.len() < threshold => {
            Some(JoinTerminalReason::RequiredMembersInsufficient)
        }
        _ => None,
    };
    if let Some(reason) = terminal_reason {
        return Ok(JoinDecision::Failed(failure_descriptor(
            reason,
            success_count,
            failed,
            cancelled,
            missing,
            *cancellation_propagates,
        )));
    }

    let ready = match failure_policy {
        JoinFailurePolicy::FailFast | JoinFailurePolicy::WaitRequired => {
            required_terminal && successful.len() >= threshold
        }
        JoinFailurePolicy::MinimumSuccess => successful.len() >= threshold,
    };
    if ready {
        let results = project_results(
            &successful,
            members,
            *ordering_policy,
            *result_projection,
            *artifact_collection,
        )?;
        let cancellation_targets = if *cancellation_propagates {
            missing.clone()
        } else {
            Vec::new()
        };
        return Ok(JoinDecision::Ready(JoinReadyDescriptor {
            results,
            success_count,
            failed_members: failed,
            cancelled_members: cancelled,
            missing_members: missing,
            cancellation_targets,
        }));
    }

    if timeout_elapsed {
        return Ok(JoinDecision::Failed(failure_descriptor(
            JoinTerminalReason::TimedOut,
            success_count,
            failed,
            cancelled,
            missing,
            *cancellation_propagates,
        )));
    }
    Ok(JoinDecision::Waiting {
        missing_members: missing,
        success_count,
    })
}

struct JoinSnapshot {
    successful: BTreeSet<String>,
    failed: Vec<String>,
    cancelled: Vec<String>,
    missing: Vec<String>,
    required_failed: bool,
    required_terminal: bool,
}

fn build_join_snapshot(
    required: &BTreeSet<String>,
    optional: &BTreeSet<String>,
    minimum_successes: u32,
    members: &BTreeMap<String, JoinMemberState>,
) -> Result<JoinSnapshot, ParallelExecutionError> {
    validate_join_configuration(required, optional, minimum_successes)?;
    let configured = required.union(optional).cloned().collect::<BTreeSet<_>>();
    for (member_id, state) in members {
        validate_identifier("join member", member_id)?;
        validate_member_state(state)?;
        if !configured.contains(member_id) {
            return Err(ParallelExecutionError::UnexpectedJoinMember {
                member_id: member_id.clone(),
            });
        }
    }
    let successful = configured
        .iter()
        .filter(|member| {
            matches!(
                members.get(*member),
                Some(JoinMemberState::Completed { .. })
            )
        })
        .cloned()
        .collect();
    let failed = configured
        .iter()
        .filter(|member| matches!(members.get(*member), Some(JoinMemberState::Failed { .. })))
        .cloned()
        .collect();
    let cancelled = configured
        .iter()
        .filter(|member| {
            matches!(
                members.get(*member),
                Some(JoinMemberState::Cancelled { .. })
            )
        })
        .cloned()
        .collect();
    let missing = configured
        .iter()
        .filter(|member| {
            members
                .get(*member)
                .is_none_or(|state| matches!(state, JoinMemberState::Pending))
        })
        .cloned()
        .collect();
    let required_failed = required.iter().any(|member| {
        matches!(
            members.get(member),
            Some(JoinMemberState::Failed { .. } | JoinMemberState::Cancelled { .. })
        )
    });
    let required_terminal = required.iter().all(|member| {
        members
            .get(member)
            .is_some_and(|state| !matches!(state, JoinMemberState::Pending))
    });
    Ok(JoinSnapshot {
        successful,
        failed,
        cancelled,
        missing,
        required_failed,
        required_terminal,
    })
}

/// Evaluates a parallel join from the exact binding set committed at fan-out
/// initialization.
///
/// Branch states are keyed by runtime-owned branch IDs while join membership is
/// keyed by graph-owned references. This conversion rejects unknown, missing,
/// or duplicate bindings instead of inferring them from topology.
///
/// # Errors
///
/// Returns [`ParallelExecutionError`] when the binding set, member state, or
/// join configuration does not match exactly.
pub fn evaluate_bound_parallel_join(
    parallel_configuration: &NodeConfiguration,
    join_configuration: &NodeConfiguration,
    bindings: &[ParallelMemberBinding],
    members_by_branch_id: &BTreeMap<String, JoinMemberState>,
    timeout_elapsed: bool,
) -> Result<JoinDecision, ParallelExecutionError> {
    let bound = bind_parallel_join_members(bindings, members_by_branch_id)?;
    let NodeConfiguration::ParallelBranch { join_policy, .. } = parallel_configuration else {
        return Err(ParallelExecutionError::WrongConfiguration {
            expected: "parallel_branch",
        });
    };
    let NodeConfiguration::JoinResults {
        required,
        optional,
        minimum_successes,
        failure_policy,
        ..
    } = join_configuration
    else {
        return Err(ParallelExecutionError::WrongConfiguration {
            expected: "join_results",
        });
    };
    let configured = required.union(optional).cloned().collect::<BTreeSet<_>>();
    let bound_references = bound.keys().cloned().collect::<BTreeSet<_>>();
    if configured != bound_references {
        return Err(ParallelExecutionError::JoinBindingSetMismatch);
    }
    if *join_policy == ParallelJoinPolicy::All
        && (!optional.is_empty()
            || required != &bound_references
            || usize::try_from(*minimum_successes).unwrap_or(usize::MAX) != bound_references.len())
    {
        return Err(ParallelExecutionError::ParallelJoinPolicyMismatch);
    }
    if *join_policy == ParallelJoinPolicy::MinimumSuccess
        && *failure_policy != JoinFailurePolicy::MinimumSuccess
    {
        return Err(ParallelExecutionError::ParallelJoinPolicyMismatch);
    }
    evaluate_join(join_configuration, &bound, timeout_elapsed)
}

fn bind_parallel_join_members(
    bindings: &[ParallelMemberBinding],
    members_by_branch_id: &BTreeMap<String, JoinMemberState>,
) -> Result<BTreeMap<String, JoinMemberState>, ParallelExecutionError> {
    if bindings.is_empty() || bindings.len() > MAX_MEMBERS {
        return Err(ParallelExecutionError::InvalidMemberCount {
            count: bindings.len(),
        });
    }
    let mut references = BTreeSet::new();
    let mut branch_ids = BTreeSet::new();
    let mut bound = BTreeMap::new();
    for binding in bindings {
        validate_identifier("join member reference", &binding.configured_reference)?;
        validate_identifier("branch id", &binding.branch_id)?;
        validate_identifier("branch target", &binding.target_node_id)?;
        if !references.insert(binding.configured_reference.clone()) {
            return Err(ParallelExecutionError::DuplicateConfiguredReference {
                reference: binding.configured_reference.clone(),
            });
        }
        if !branch_ids.insert(binding.branch_id.clone()) {
            return Err(ParallelExecutionError::DuplicateBoundBranch {
                branch_id: binding.branch_id.clone(),
            });
        }
        let state = members_by_branch_id
            .get(&binding.branch_id)
            .ok_or_else(|| ParallelExecutionError::MissingBoundBranch {
                branch_id: binding.branch_id.clone(),
            })?;
        validate_member_state(state)?;
        bound.insert(binding.configured_reference.clone(), state.clone());
    }
    if members_by_branch_id
        .keys()
        .any(|branch_id| !branch_ids.contains(branch_id))
    {
        return Err(ParallelExecutionError::UnexpectedBoundBranch);
    }
    Ok(bound)
}

/// Validates concurrent variable and workspace writes for a parallel fan-out.
///
/// # Errors
///
/// Returns [`ParallelExecutionError`] when a shared write lacks either an exact
/// merge policy or stable serialization.
pub fn validate_parallel_writes(
    branches: &[ParallelBranchSpec],
    variables: &[VariableDeclaration],
    configured_merge_policies: &BTreeMap<String, VariableMergePolicy>,
    serialization_policy: Option<ParallelSerializationPolicy>,
) -> Result<(), ParallelExecutionError> {
    if branches.len() > MAX_MEMBERS {
        return Err(ParallelExecutionError::InvalidMemberCount {
            count: branches.len(),
        });
    }
    let declarations = variables
        .iter()
        .map(|variable| (variable.name.as_str(), variable))
        .collect::<BTreeMap<_, _>>();
    for (name, configured) in configured_merge_policies {
        let declaration = declarations.get(name.as_str()).ok_or_else(|| {
            ParallelExecutionError::UnknownMergeVariable {
                variable: name.clone(),
            }
        })?;
        if let Some(declared) = declaration.merge_policy
            && declared != *configured
        {
            return Err(ParallelExecutionError::MergePolicyMismatch {
                variable: name.clone(),
                declared,
                configured: *configured,
            });
        }
    }
    for (left_index, left) in branches.iter().enumerate() {
        for right in branches.iter().skip(left_index + 1) {
            for variable in left.write_variables.intersection(&right.write_variables) {
                let declaration = declarations.get(variable.as_str()).ok_or_else(|| {
                    ParallelExecutionError::UnknownWrittenVariable {
                        variable: variable.clone(),
                    }
                })?;
                if declaration.scope == VariableScope::Branch {
                    continue;
                }
                let has_merge = configured_merge_policies.contains_key(variable)
                    || declaration.merge_policy.is_some();
                if !has_merge && serialization_policy.is_none() {
                    return Err(ParallelExecutionError::ConflictingVariableWrite {
                        variable: variable.clone(),
                        left_branch: left.target_node_id.clone(),
                        right_branch: right.target_node_id.clone(),
                    });
                }
            }
            if serialization_policy.is_none()
                && let Some(resource) = left
                    .workspace_resources
                    .intersection(&right.workspace_resources)
                    .next()
            {
                return Err(ParallelExecutionError::ConflictingWorkspaceWrite {
                    resource: resource.clone(),
                    left_branch: left.target_node_id.clone(),
                    right_branch: right.target_node_id.clone(),
                });
            }
        }
    }
    for branch in branches {
        for variable in &branch.write_variables {
            if !declarations.contains_key(variable.as_str()) {
                return Err(ParallelExecutionError::UnknownWrittenVariable {
                    variable: variable.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Derives a stable branch identity from exact immutable node work, target, and
/// outgoing-edge index.
///
/// # Errors
///
/// Returns [`ParallelExecutionError`] when identity material is invalid or
/// cannot be serialized canonically.
pub fn stable_branch_id(
    work: &NodeWorkIdentity,
    target_node_id: &str,
    branch_index: u32,
) -> Result<String, ParallelExecutionError> {
    validate_identifier("run id", &work.run_id)?;
    validate_identifier("node id", &work.node_id)?;
    validate_identifier("branch target", target_node_id)?;
    for branch in &work.branch_path {
        validate_identifier("parent branch id", branch)?;
    }
    let material = serde_json::to_vec(&(
        "agentmod.parallel-branch.v1",
        work,
        target_node_id,
        branch_index,
    ))
    .map_err(ParallelExecutionError::IdentitySerialization)?;
    Ok(format!(
        "branch:blake3:{}",
        ContentHash::digest(&material).to_hex()
    ))
}

fn stable_dispatch_id(
    work: &NodeWorkIdentity,
    branch_id: &str,
) -> Result<String, ParallelExecutionError> {
    let material = serde_json::to_vec(&(("agentmod.parallel-dispatch.v1"), work, branch_id))
        .map_err(ParallelExecutionError::IdentitySerialization)?;
    Ok(format!(
        "dispatch:blake3:{}",
        ContentHash::digest(&material).to_hex()
    ))
}

fn validate_join_configuration(
    required: &BTreeSet<String>,
    optional: &BTreeSet<String>,
    minimum_successes: u32,
) -> Result<(), ParallelExecutionError> {
    let member_count = required.len().saturating_add(optional.len());
    if member_count == 0
        || member_count > MAX_MEMBERS
        || required.intersection(optional).next().is_some()
        || minimum_successes == 0
        || usize::try_from(minimum_successes).unwrap_or(usize::MAX) > member_count
    {
        return Err(ParallelExecutionError::InvalidJoinConfiguration);
    }
    for member in required.iter().chain(optional) {
        validate_identifier("join member", member)?;
    }
    Ok(())
}

fn validate_member_state(state: &JoinMemberState) -> Result<(), ParallelExecutionError> {
    match state {
        JoinMemberState::Pending => Ok(()),
        JoinMemberState::Completed {
            completion_sequence,
            result,
        } => {
            if *completion_sequence == 0 {
                return Err(ParallelExecutionError::InvalidCompletionSequence {
                    expected: 1,
                    actual: 0,
                });
            }
            validate_member_result(result)
        }
        JoinMemberState::Failed { code } | JoinMemberState::Cancelled { code } => {
            validate_code(code)
        }
    }
}

fn validate_member_result(result: &JoinMemberResult) -> Result<(), ParallelExecutionError> {
    if result.artifact_references.len() > MAX_ARTIFACTS_PER_RESULT
        || !result
            .declared_artifact_references
            .is_subset(&result.artifact_references)
    {
        return Err(ParallelExecutionError::InvalidArtifactCollection);
    }
    if let Some(value) = &result.inline_value {
        let bytes =
            serde_json::to_vec(value).map_err(ParallelExecutionError::ResultSerialization)?;
        if bytes.len() > MAX_RESULT_BYTES {
            return Err(ParallelExecutionError::ResultTooLarge { bytes: bytes.len() });
        }
    }
    if let Some(reference) = &result.node_result_reference {
        validate_identifier("node result reference", reference)?;
    }
    for artifact in &result.artifact_references {
        validate_identifier("artifact reference", artifact)?;
    }
    Ok(())
}

fn project_results(
    successful: &BTreeSet<String>,
    members: &BTreeMap<String, JoinMemberState>,
    ordering: JoinOrderingPolicy,
    projection: JoinResultProjection,
    artifact_collection: JoinArtifactCollection,
) -> Result<Vec<ProjectedJoinMember>, ParallelExecutionError> {
    let mut completed = successful
        .iter()
        .filter_map(|member_id| match members.get(member_id) {
            Some(JoinMemberState::Completed {
                completion_sequence,
                result,
            }) => Some((member_id, *completion_sequence, result)),
            _ => None,
        })
        .collect::<Vec<_>>();
    match ordering {
        JoinOrderingPolicy::MemberId => {
            completed.sort_by(|left, right| left.0.cmp(right.0));
        }
        JoinOrderingPolicy::CompletionSequence => {
            completed.sort_by(|left, right| (left.1, left.0).cmp(&(right.1, right.0)));
        }
    }

    completed
        .into_iter()
        .map(|(member_id, _, result)| {
            let (inline_value, node_result_reference) = match projection {
                JoinResultProjection::Inline => (
                    Some(result.inline_value.clone().ok_or_else(|| {
                        ParallelExecutionError::MissingProjectedResult {
                            member_id: member_id.clone(),
                            projection: "inline",
                        }
                    })?),
                    None,
                ),
                JoinResultProjection::NodeReferences => (
                    None,
                    Some(result.node_result_reference.clone().ok_or_else(|| {
                        ParallelExecutionError::MissingProjectedResult {
                            member_id: member_id.clone(),
                            projection: "node_references",
                        }
                    })?),
                ),
                JoinResultProjection::ArtifactReferences => (None, None),
            };
            let artifact_references = match artifact_collection {
                JoinArtifactCollection::None => Vec::new(),
                JoinArtifactCollection::Declared => result
                    .declared_artifact_references
                    .iter()
                    .cloned()
                    .collect(),
                JoinArtifactCollection::All => result.artifact_references.iter().cloned().collect(),
            };
            Ok(ProjectedJoinMember {
                member_id: member_id.clone(),
                inline_value,
                node_result_reference,
                artifact_references,
            })
        })
        .collect()
}

fn failure_descriptor(
    reason: JoinTerminalReason,
    success_count: u32,
    failed_members: Vec<String>,
    cancelled_members: Vec<String>,
    missing_members: Vec<String>,
    cancellation_propagates: bool,
) -> JoinFailureDescriptor {
    let cancellation_targets = if cancellation_propagates {
        missing_members.clone()
    } else {
        Vec::new()
    };
    JoinFailureDescriptor {
        reason,
        success_count,
        failed_members,
        cancelled_members,
        missing_members,
        cancellation_targets,
    }
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), ParallelExecutionError> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES || value.chars().any(char::is_control)
    {
        return Err(ParallelExecutionError::InvalidIdentifier {
            field,
            value: value.to_owned(),
        });
    }
    Ok(())
}

fn validate_code(code: &str) -> Result<(), ParallelExecutionError> {
    if code.is_empty() || code.len() > MAX_FAILURE_CODE_BYTES || code.chars().any(char::is_control)
    {
        return Err(ParallelExecutionError::InvalidDiagnosticCode);
    }
    Ok(())
}

fn state_name(state: &ParallelBranchState) -> &'static str {
    match state {
        ParallelBranchState::Queued => "queued",
        ParallelBranchState::Dispatched => "dispatched",
        ParallelBranchState::Running => "running",
        ParallelBranchState::Completed { .. } => "completed",
        ParallelBranchState::Failed { .. } => "failed",
        ParallelBranchState::Cancelled { .. } => "cancelled",
    }
}

fn transition_name(transition: &ParallelBranchTransition) -> &'static str {
    match transition {
        ParallelBranchTransition::Dispatch { .. } => "dispatch",
        ParallelBranchTransition::Start { .. } => "start",
        ParallelBranchTransition::Complete { .. } => "complete",
        ParallelBranchTransition::Fail { .. } => "fail",
        ParallelBranchTransition::Cancel { .. } => "cancel",
    }
}

/// Stable parallel execution and join validation failure.
#[derive(Debug, Error)]
pub enum ParallelExecutionError {
    /// The caller supplied another node kind's configuration.
    #[error("parallel execution requires `{expected}` node configuration")]
    WrongConfiguration {
        /// Expected serialized configuration kind.
        expected: &'static str,
    },
    /// Parallel capacity was zero.
    #[error("parallel execution capacity must be positive")]
    InvalidCapacity,
    /// Branch/member cardinality was outside runtime bounds.
    #[error("parallel member count {count} is outside runtime bounds")]
    InvalidMemberCount {
        /// Observed count.
        count: usize,
    },
    /// Waiting work exceeds the declared queue.
    #[error(
        "{branches} branches require more than queue depth {max_queue_depth} at effective parallelism {effective_parallelism}"
    )]
    QueueCapacityExceeded {
        /// Total branch count.
        branches: usize,
        /// Runtime-enforced effective concurrency.
        effective_parallelism: usize,
        /// Declared maximum waiting work.
        max_queue_depth: u32,
    },
    /// A stable identity field was empty, oversized, or contained controls.
    #[error("invalid {field} identity `{value}`")]
    InvalidIdentifier {
        /// Safe field label.
        field: &'static str,
        /// Rejected value.
        value: String,
    },
    /// Canonical identity material could not be serialized.
    #[error("parallel identity serialization failed: {0}")]
    IdentitySerialization(serde_json::Error),
    /// Stable branch derivation unexpectedly collided.
    #[error("duplicate stable branch identity")]
    DuplicateBranchIdentity,
    /// Two branches claimed the same graph-owned join reference.
    #[error("duplicate configured parallel member reference `{reference}`")]
    DuplicateConfiguredReference {
        /// Duplicated reference.
        reference: String,
    },
    /// Two persisted bindings claimed the same runtime branch.
    #[error("duplicate persisted binding for branch `{branch_id}`")]
    DuplicateBoundBranch {
        /// Duplicated runtime branch identity.
        branch_id: String,
    },
    /// A persisted binding names no replayed branch state.
    #[error("persisted binding names missing branch `{branch_id}`")]
    MissingBoundBranch {
        /// Missing runtime branch identity.
        branch_id: String,
    },
    /// Replayed branch state contains a branch absent from persisted bindings.
    #[error("replayed parallel state contains an unbound branch")]
    UnexpectedBoundBranch,
    /// Persisted reference bindings do not equal immutable join membership.
    #[error("persisted parallel bindings do not match the join member set")]
    JoinBindingSetMismatch,
    /// Fan-out and join readiness constraints disagree.
    #[error("parallel fan-out join policy does not match join configuration")]
    ParallelJoinPolicyMismatch,
    /// A replay transition referenced an unknown branch.
    #[error("unknown parallel branch `{branch_id}`")]
    UnknownBranch {
        /// Referenced branch identity.
        branch_id: String,
    },
    /// A transition substituted a different dispatch identity.
    #[error("parallel branch `{branch_id}` dispatch identity does not match")]
    DispatchIdentityMismatch {
        /// Referenced branch identity.
        branch_id: String,
    },
    /// A transition was illegal from the replayed lifecycle state.
    #[error("parallel branch `{branch_id}` cannot apply `{transition}` from `{from}`")]
    IllegalTransition {
        /// Referenced branch identity.
        branch_id: String,
        /// Existing lifecycle label.
        from: &'static str,
        /// Requested transition label.
        transition: &'static str,
    },
    /// Completion sequence was not the exact next canonical value.
    #[error("completion sequence {actual} does not match expected {expected}")]
    InvalidCompletionSequence {
        /// Expected next sequence.
        expected: u64,
        /// Supplied sequence.
        actual: u64,
    },
    /// The canonical completion counter cannot advance without wrapping.
    #[error("parallel completion sequence is exhausted")]
    CompletionSequenceExhausted,
    /// A result value could not be serialized.
    #[error("join result serialization failed: {0}")]
    ResultSerialization(serde_json::Error),
    /// An inline result exceeded runtime bounds.
    #[error("join result is {bytes} bytes and exceeds runtime bounds")]
    ResultTooLarge {
        /// Canonical serialized length.
        bytes: usize,
    },
    /// Artifact declaration was oversized or was not a subset of result artifacts.
    #[error("join artifact collection is invalid")]
    InvalidArtifactCollection,
    /// A failure or cancellation code was unsafe or oversized.
    #[error("parallel diagnostic code is invalid")]
    InvalidDiagnosticCode,
    /// Join sets or threshold were inconsistent.
    #[error("join configuration has invalid members or success threshold")]
    InvalidJoinConfiguration,
    /// Replay supplied a member outside the immutable join configuration.
    #[error("unexpected join member `{member_id}`")]
    UnexpectedJoinMember {
        /// Unexpected identity.
        member_id: String,
    },
    /// A successful member lacked the configured projection value.
    #[error("join member `{member_id}` has no `{projection}` result")]
    MissingProjectedResult {
        /// Member identity.
        member_id: String,
        /// Required projection label.
        projection: &'static str,
    },
    /// A configured merge names no declared variable.
    #[error("parallel merge policy names unknown variable `{variable}`")]
    UnknownMergeVariable {
        /// Variable name.
        variable: String,
    },
    /// A branch write names no declared variable.
    #[error("parallel branch writes unknown variable `{variable}`")]
    UnknownWrittenVariable {
        /// Variable name.
        variable: String,
    },
    /// Node override and variable declaration disagree.
    #[error(
        "parallel merge policy for `{variable}` is `{configured:?}` but declaration is `{declared:?}`"
    )]
    MergePolicyMismatch {
        /// Variable name.
        variable: String,
        /// Declaration-owned policy.
        declared: VariableMergePolicy,
        /// Parallel-node override.
        configured: VariableMergePolicy,
    },
    /// Concurrent graph-variable writes lack merge/serialization.
    #[error(
        "parallel branches `{left_branch}` and `{right_branch}` conflict on variable `{variable}`"
    )]
    ConflictingVariableWrite {
        /// Variable name.
        variable: String,
        /// First compiled branch target.
        left_branch: String,
        /// Second compiled branch target.
        right_branch: String,
    },
    /// Concurrent workspace writes lack stable serialization.
    #[error(
        "parallel branches `{left_branch}` and `{right_branch}` conflict on workspace resource `{resource}`"
    )]
    ConflictingWorkspaceWrite {
        /// Workspace resource.
        resource: String,
        /// First compiled branch target.
        left_branch: String,
        /// Second compiled branch target.
        right_branch: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmod_graph_engine::{
        JoinArtifactCollection, JoinFailurePolicy, JoinOrderingPolicy, JoinResultProjection,
        ParallelJoinPolicy, SecurityClassification, VariableMutability, VariableValueType,
    };

    fn work() -> NodeWorkIdentity {
        NodeWorkIdentity {
            run_id: "run-1".to_owned(),
            node_id: "fanout".to_owned(),
            branch_path: vec!["parent-branch".to_owned()],
            attempt: 2,
            loop_iteration: 3,
            step: 4,
        }
    }

    fn parallel_configuration(max_parallelism: u32, max_queue_depth: u32) -> NodeConfiguration {
        NodeConfiguration::ParallelBranch {
            max_parallelism,
            max_queue_depth,
            join_target: "join".to_owned(),
            join_policy: ParallelJoinPolicy::MinimumSuccess,
            variable_merge_policies: BTreeMap::new(),
            serialization_policy: None,
        }
    }

    fn specs(count: usize) -> Vec<ParallelBranchSpec> {
        (0..count)
            .map(|index| ParallelBranchSpec {
                member_reference: format!("member-{index}"),
                target_node_id: format!("branch-{index}"),
                write_variables: BTreeSet::new(),
                workspace_resources: BTreeSet::new(),
            })
            .collect()
    }

    fn result(label: &str) -> JoinMemberResult {
        JoinMemberResult {
            inline_value: Some(serde_json::json!({"value": label})),
            node_result_reference: Some(format!("node-result:{label}")),
            artifact_references: [
                format!("artifact:{label}:declared"),
                format!("artifact:{label}:extra"),
            ]
            .into_iter()
            .collect(),
            declared_artifact_references: [format!("artifact:{label}:declared")]
                .into_iter()
                .collect(),
        }
    }

    fn dispatch_and_start(
        state: &mut ParallelExecutionState,
        descriptor: &BranchDispatchDescriptor,
    ) {
        state
            .apply(ParallelBranchTransition::Dispatch {
                branch_id: descriptor.branch_id.clone(),
                dispatch_id: descriptor.dispatch_id.clone(),
            })
            .expect("dispatch");
        state
            .apply(ParallelBranchTransition::Start {
                branch_id: descriptor.branch_id.clone(),
                dispatch_id: descriptor.dispatch_id.clone(),
            })
            .expect("start");
    }

    fn join_configuration(
        required: &[&str],
        optional: &[&str],
        minimum_successes: u32,
        failure_policy: JoinFailurePolicy,
        ordering_policy: JoinOrderingPolicy,
    ) -> NodeConfiguration {
        NodeConfiguration::JoinResults {
            required: required.iter().map(ToString::to_string).collect(),
            optional: optional.iter().map(ToString::to_string).collect(),
            minimum_successes,
            failure_policy,
            ordering_policy,
            timeout_ms: 5_000,
            cancellation_propagates: true,
            result_projection: JoinResultProjection::Inline,
            artifact_collection: JoinArtifactCollection::Declared,
        }
    }

    fn completed(sequence: u64, label: &str) -> JoinMemberState {
        JoinMemberState::Completed {
            completion_sequence: sequence,
            result: result(label),
        }
    }

    fn run_variable(name: &str, merge_policy: Option<VariableMergePolicy>) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            value_type: VariableValueType::List {
                item_type: Box::new(VariableValueType::String),
                max_items: 16,
            },
            scope: VariableScope::Run,
            producer: "fanout".to_owned(),
            merge_contributors: BTreeSet::new(),
            consumers: ["join".to_owned()].into_iter().collect(),
            mutability: VariableMutability::Mutable,
            merge_policy,
            max_size_bytes: 1_024,
            security_classification: SecurityClassification::Internal,
        }
    }

    #[test]
    fn identities_and_dispatch_order_are_stable_and_exact() {
        let first =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 2), &specs(4), &[])
                .expect("parallel state");
        let second =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 2), &specs(4), &[])
                .expect("parallel state");
        assert_eq!(first, second);
        let decision = first.dispatch_decision();
        assert_eq!(decision.ready.len(), 2);
        assert!(decision.backpressured);
        assert_eq!(decision.queued_after_dispatch, 2);
        assert_eq!(decision.ready[0].branch_index, 0);
        assert_eq!(decision.ready[1].branch_index, 1);
        assert!(decision.ready[0].branch_id.starts_with("branch:blake3:"));
        assert!(
            decision.ready[0]
                .dispatch_id
                .starts_with("dispatch:blake3:")
        );

        let changed = stable_branch_id(&work(), "branch-0", 1).expect("stable id");
        assert_ne!(decision.ready[0].branch_id, changed);
    }

    #[test]
    fn queue_capacity_and_backpressure_are_enforced() {
        assert!(matches!(
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(4), &[]),
            Err(ParallelExecutionError::QueueCapacityExceeded { .. })
        ));
        let mut state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 2), &specs(4), &[])
                .expect("state");
        let first = state.dispatch_decision();
        for descriptor in &first.ready {
            dispatch_and_start(&mut state, descriptor);
        }
        assert!(state.dispatch_decision().ready.is_empty());
        let completion = state
            .completion_transition(&first.ready[0].branch_id, result("one"))
            .expect("completion");
        state.apply(completion).expect("apply completion");
        let next = state.dispatch_decision();
        assert_eq!(next.ready.len(), 1);
        assert_eq!(next.ready[0].branch_index, 2);
        assert!(next.backpressured);
    }

    #[test]
    fn illegal_and_double_transitions_fail_closed() {
        let mut state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(3), &[])
                .expect("state");
        let descriptor = state.dispatch_decision().ready[0].clone();
        assert!(matches!(
            state.apply(ParallelBranchTransition::Start {
                branch_id: descriptor.branch_id.clone(),
                dispatch_id: descriptor.dispatch_id.clone(),
            }),
            Err(ParallelExecutionError::IllegalTransition { .. })
        ));
        state
            .apply(ParallelBranchTransition::Dispatch {
                branch_id: descriptor.branch_id.clone(),
                dispatch_id: descriptor.dispatch_id.clone(),
            })
            .expect("dispatch");
        assert!(matches!(
            state.apply(ParallelBranchTransition::Dispatch {
                branch_id: descriptor.branch_id.clone(),
                dispatch_id: descriptor.dispatch_id.clone(),
            }),
            Err(ParallelExecutionError::IllegalTransition { .. })
        ));
        assert!(matches!(
            state.apply(ParallelBranchTransition::Start {
                branch_id: descriptor.branch_id,
                dispatch_id: "dispatch:substituted".to_owned(),
            }),
            Err(ParallelExecutionError::DispatchIdentityMismatch { .. })
        ));
    }

    #[test]
    fn completion_sequences_are_exact_and_replayable() {
        let mut state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(3), &[])
                .expect("state");
        let descriptors = state.dispatch_decision().ready;
        for descriptor in &descriptors {
            dispatch_and_start(&mut state, descriptor);
        }
        let second = state
            .completion_transition(&descriptors[1].branch_id, result("second"))
            .expect("completion");
        assert!(matches!(
            second,
            ParallelBranchTransition::Complete {
                completion_sequence: 1,
                ..
            }
        ));
        state.apply(second.clone()).expect("apply");
        assert!(matches!(
            state.apply(second),
            Err(ParallelExecutionError::IllegalTransition { .. })
        ));
        let first = state
            .completion_transition(&descriptors[0].branch_id, result("first"))
            .expect("completion");
        assert!(matches!(
            first,
            ParallelBranchTransition::Complete {
                completion_sequence: 2,
                ..
            }
        ));
    }

    #[test]
    fn completion_sequence_overflow_fails_closed() {
        let mut state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(3), &[])
                .expect("state");
        let descriptors = state.dispatch_decision().ready;
        for descriptor in &descriptors {
            dispatch_and_start(&mut state, descriptor);
        }
        state
            .branches
            .get_mut(&descriptors[0].branch_id)
            .expect("first branch")
            .state = ParallelBranchState::Completed {
            completion_sequence: u64::MAX,
            result: result("exhausted"),
        };

        assert!(matches!(
            state.completion_transition(&descriptors[1].branch_id, result("next")),
            Err(ParallelExecutionError::CompletionSequenceExhausted)
        ));
        assert!(matches!(
            state.apply(ParallelBranchTransition::Complete {
                branch_id: descriptors[1].branch_id.clone(),
                dispatch_id: descriptors[1].dispatch_id.clone(),
                completion_sequence: u64::MAX,
                result: result("next"),
            }),
            Err(ParallelExecutionError::CompletionSequenceExhausted)
        ));
    }

    #[test]
    fn fail_fast_rejects_required_failure_and_cancels_pending() {
        let config = join_configuration(
            &["a", "b"],
            &["c"],
            2,
            JoinFailurePolicy::FailFast,
            JoinOrderingPolicy::MemberId,
        );
        let members = [
            ("a".to_owned(), completed(1, "a")),
            (
                "b".to_owned(),
                JoinMemberState::Failed {
                    code: "worker_failed".to_owned(),
                },
            ),
        ]
        .into_iter()
        .collect();
        let JoinDecision::Failed(failure) =
            evaluate_join(&config, &members, false).expect("decision")
        else {
            panic!("expected failure");
        };
        assert_eq!(failure.reason, JoinTerminalReason::RequiredMemberFailed);
        assert_eq!(failure.cancellation_targets, vec!["c"]);
    }

    #[test]
    fn wait_required_waits_then_evaluates_known_successes() {
        let config = join_configuration(
            &["a", "b"],
            &["c"],
            2,
            JoinFailurePolicy::WaitRequired,
            JoinOrderingPolicy::MemberId,
        );
        let mut members = BTreeMap::from([("a".to_owned(), completed(1, "a"))]);
        assert!(matches!(
            evaluate_join(&config, &members, false).expect("waiting"),
            JoinDecision::Waiting { .. }
        ));
        members.insert(
            "b".to_owned(),
            JoinMemberState::Failed {
                code: "failed".to_owned(),
            },
        );
        let JoinDecision::Failed(failure) =
            evaluate_join(&config, &members, false).expect("failed")
        else {
            panic!("expected failure");
        };
        assert_eq!(
            failure.reason,
            JoinTerminalReason::RequiredMembersInsufficient
        );
    }

    #[test]
    fn minimum_success_is_ready_early_and_projects_member_order() {
        let config = join_configuration(
            &["b"],
            &["a", "c"],
            2,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::MemberId,
        );
        let members = BTreeMap::from([
            ("b".to_owned(), completed(1, "b")),
            ("a".to_owned(), completed(2, "a")),
        ]);
        let JoinDecision::Ready(ready) = evaluate_join(&config, &members, false).expect("ready")
        else {
            panic!("expected ready");
        };
        assert_eq!(
            ready
                .results
                .iter()
                .map(|item| item.member_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(ready.cancellation_targets, vec!["c"]);
        assert_eq!(
            ready.results[0].artifact_references,
            vec!["artifact:a:declared"]
        );
    }

    #[test]
    fn node_reference_and_artifact_projection_policies_are_exact() {
        let mut node_reference_config = join_configuration(
            &["a"],
            &[],
            1,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::MemberId,
        );
        let NodeConfiguration::JoinResults {
            result_projection,
            artifact_collection,
            ..
        } = &mut node_reference_config
        else {
            unreachable!("join fixture");
        };
        *result_projection = JoinResultProjection::NodeReferences;
        *artifact_collection = JoinArtifactCollection::None;
        let members = BTreeMap::from([("a".to_owned(), completed(1, "a"))]);
        let JoinDecision::Ready(node_ready) =
            evaluate_join(&node_reference_config, &members, false).expect("node reference")
        else {
            panic!("expected ready");
        };
        assert_eq!(
            node_ready.results[0].node_result_reference.as_deref(),
            Some("node-result:a")
        );
        assert!(node_ready.results[0].artifact_references.is_empty());

        let mut artifact_config = node_reference_config;
        let NodeConfiguration::JoinResults {
            result_projection,
            artifact_collection,
            ..
        } = &mut artifact_config
        else {
            unreachable!("join fixture");
        };
        *result_projection = JoinResultProjection::ArtifactReferences;
        *artifact_collection = JoinArtifactCollection::All;
        let JoinDecision::Ready(artifact_ready) =
            evaluate_join(&artifact_config, &members, false).expect("artifacts")
        else {
            panic!("expected ready");
        };
        assert_eq!(
            artifact_ready.results[0].artifact_references,
            vec!["artifact:a:declared", "artifact:a:extra"]
        );
        assert!(artifact_ready.results[0].inline_value.is_none());
        assert!(artifact_ready.results[0].node_result_reference.is_none());
    }

    #[test]
    fn completion_sequence_order_and_threshold_impossibility_are_deterministic() {
        let config = join_configuration(
            &["a"],
            &["b", "c"],
            2,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::CompletionSequence,
        );
        let ready_members = BTreeMap::from([
            ("a".to_owned(), completed(2, "a")),
            ("b".to_owned(), completed(1, "b")),
        ]);
        let JoinDecision::Ready(ready) =
            evaluate_join(&config, &ready_members, false).expect("ready")
        else {
            panic!("expected ready");
        };
        assert_eq!(
            ready
                .results
                .iter()
                .map(|item| item.member_id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "a"]
        );

        let impossible_members = BTreeMap::from([
            (
                "a".to_owned(),
                JoinMemberState::Failed {
                    code: "failed".to_owned(),
                },
            ),
            (
                "b".to_owned(),
                JoinMemberState::Cancelled {
                    code: "cancelled".to_owned(),
                },
            ),
        ]);
        let JoinDecision::Failed(failure) =
            evaluate_join(&config, &impossible_members, false).expect("failure")
        else {
            panic!("expected failure");
        };
        assert_eq!(
            failure.reason,
            JoinTerminalReason::SuccessThresholdImpossible
        );
    }

    #[test]
    fn timeout_is_terminal_and_propagates_cancellation() {
        let config = join_configuration(
            &["a", "b"],
            &[],
            2,
            JoinFailurePolicy::WaitRequired,
            JoinOrderingPolicy::MemberId,
        );
        let members = BTreeMap::from([("a".to_owned(), completed(1, "a"))]);
        let JoinDecision::Failed(failure) =
            evaluate_join(&config, &members, true).expect("timeout")
        else {
            panic!("expected failure");
        };
        assert_eq!(failure.reason, JoinTerminalReason::TimedOut);
        assert_eq!(failure.cancellation_targets, vec!["b"]);
    }

    #[test]
    fn cancellation_transitions_are_stable_and_cover_incomplete_branches() {
        let mut state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(3), &[])
                .expect("state");
        let descriptors = state.dispatch_decision().ready;
        dispatch_and_start(&mut state, &descriptors[0]);
        let transitions = state
            .cancellation_transitions("parent_cancelled")
            .expect("transitions");
        assert_eq!(transitions.len(), 3);
        for transition in transitions {
            state.apply(transition).expect("cancel");
        }
        assert!(
            state
                .branches
                .values()
                .all(|branch| matches!(branch.state, ParallelBranchState::Cancelled { .. }))
        );
    }

    #[test]
    fn shared_writes_require_exact_merge_or_serialization() {
        let shared_branches = vec![
            ParallelBranchSpec {
                member_reference: "member-a".to_owned(),
                target_node_id: "a".to_owned(),
                write_variables: ["shared".to_owned()].into_iter().collect(),
                workspace_resources: ["workspace".to_owned()].into_iter().collect(),
            },
            ParallelBranchSpec {
                member_reference: "member-b".to_owned(),
                target_node_id: "b".to_owned(),
                write_variables: ["shared".to_owned()].into_iter().collect(),
                workspace_resources: ["workspace".to_owned()].into_iter().collect(),
            },
        ];
        let variables = vec![run_variable("shared", None)];
        assert!(matches!(
            validate_parallel_writes(&shared_branches, &variables, &BTreeMap::new(), None),
            Err(ParallelExecutionError::ConflictingVariableWrite { .. })
        ));
        let policies = BTreeMap::from([("shared".to_owned(), VariableMergePolicy::Append)]);
        assert!(matches!(
            validate_parallel_writes(&shared_branches, &variables, &policies, None),
            Err(ParallelExecutionError::ConflictingWorkspaceWrite { .. })
        ));
        validate_parallel_writes(
            &shared_branches,
            &variables,
            &BTreeMap::new(),
            Some(ParallelSerializationPolicy::StableBranchOrder),
        )
        .expect("serialized");
        validate_parallel_writes(
            &[
                ParallelBranchSpec {
                    member_reference: "member-a".to_owned(),
                    workspace_resources: BTreeSet::new(),
                    ..shared_branches[0].clone()
                },
                ParallelBranchSpec {
                    member_reference: "member-b".to_owned(),
                    workspace_resources: BTreeSet::new(),
                    ..shared_branches[1].clone()
                },
            ],
            &variables,
            &policies,
            None,
        )
        .expect("merged variable");
    }

    #[test]
    fn declared_merge_must_match_node_override() {
        let branches = vec![
            ParallelBranchSpec {
                member_reference: "member-a".to_owned(),
                target_node_id: "a".to_owned(),
                write_variables: ["shared".to_owned()].into_iter().collect(),
                workspace_resources: BTreeSet::new(),
            },
            ParallelBranchSpec {
                member_reference: "member-b".to_owned(),
                target_node_id: "b".to_owned(),
                write_variables: ["shared".to_owned()].into_iter().collect(),
                workspace_resources: BTreeSet::new(),
            },
        ];
        let variables = vec![run_variable("shared", Some(VariableMergePolicy::DeepMerge))];
        let policies = BTreeMap::from([("shared".to_owned(), VariableMergePolicy::Append)]);
        assert!(matches!(
            validate_parallel_writes(&branches, &variables, &policies, None),
            Err(ParallelExecutionError::MergePolicyMismatch { .. })
        ));
    }

    #[test]
    fn identical_replay_state_yields_identical_dispatch_and_join_decisions() {
        let mut state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(3), &[])
                .expect("state");
        let descriptor = state.dispatch_decision().ready[0].clone();
        dispatch_and_start(&mut state, &descriptor);
        let transition = state
            .completion_transition(&descriptor.branch_id, result("done"))
            .expect("completion");
        state.apply(transition).expect("apply");

        let bytes = serde_json::to_vec(&state).expect("serialize replay state");
        let replayed: ParallelExecutionState =
            serde_json::from_slice(&bytes).expect("deserialize replay state");
        assert_eq!(state.dispatch_decision(), replayed.dispatch_decision());

        let join = join_configuration(
            &["member-0"],
            &["member-1", "member-2"],
            1,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::CompletionSequence,
        );
        assert_eq!(
            evaluate_bound_parallel_join(
                &parallel_configuration(2, 1),
                &join,
                &state.member_bindings,
                &state.join_members(),
                false
            )
            .expect("live decision"),
            evaluate_bound_parallel_join(
                &parallel_configuration(2, 1),
                &join,
                &replayed.member_bindings,
                &replayed.join_members(),
                false
            )
            .expect("replay decision")
        );
    }

    #[test]
    fn persisted_member_bindings_fail_closed_on_substitution_or_duplicates() {
        let state =
            ParallelExecutionState::new(work(), &parallel_configuration(2, 1), &specs(3), &[])
                .expect("state");
        let join = join_configuration(
            &["member-0"],
            &["member-1", "member-2"],
            1,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::MemberId,
        );
        let mut duplicate = state.member_bindings.clone();
        duplicate[1].configured_reference = "member-0".to_owned();
        assert!(matches!(
            evaluate_bound_parallel_join(
                &parallel_configuration(2, 1),
                &join,
                &duplicate,
                &state.join_members(),
                false
            ),
            Err(ParallelExecutionError::DuplicateConfiguredReference { .. })
        ));

        let mut substituted = state.member_bindings.clone();
        substituted[0].configured_reference = "substituted".to_owned();
        assert!(matches!(
            evaluate_bound_parallel_join(
                &parallel_configuration(2, 1),
                &join,
                &substituted,
                &state.join_members(),
                false
            ),
            Err(ParallelExecutionError::JoinBindingSetMismatch)
        ));
    }

    #[test]
    fn parallel_all_policy_requires_every_binding_as_required_success() {
        let mut parallel = parallel_configuration(2, 1);
        let NodeConfiguration::ParallelBranch { join_policy, .. } = &mut parallel else {
            unreachable!("parallel fixture");
        };
        *join_policy = ParallelJoinPolicy::All;
        let state = ParallelExecutionState::new(work(), &parallel, &specs(3), &[]).expect("state");
        let mismatched = join_configuration(
            &["member-0"],
            &["member-1", "member-2"],
            1,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::MemberId,
        );
        assert!(matches!(
            evaluate_bound_parallel_join(
                &parallel,
                &mismatched,
                &state.member_bindings,
                &state.join_members(),
                false
            ),
            Err(ParallelExecutionError::ParallelJoinPolicyMismatch)
        ));

        let exact = join_configuration(
            &["member-0", "member-1", "member-2"],
            &[],
            3,
            JoinFailurePolicy::WaitRequired,
            JoinOrderingPolicy::MemberId,
        );
        assert!(matches!(
            evaluate_bound_parallel_join(
                &parallel,
                &exact,
                &state.member_bindings,
                &state.join_members(),
                false
            )
            .expect("exact policy"),
            JoinDecision::Waiting { .. }
        ));
    }

    #[test]
    fn parallel_minimum_success_policy_requires_threshold_join_semantics() {
        let parallel = parallel_configuration(2, 1);
        let state = ParallelExecutionState::new(work(), &parallel, &specs(3), &[]).expect("state");
        let incompatible = join_configuration(
            &["member-0"],
            &["member-1", "member-2"],
            1,
            JoinFailurePolicy::WaitRequired,
            JoinOrderingPolicy::MemberId,
        );
        assert!(matches!(
            evaluate_bound_parallel_join(
                &parallel,
                &incompatible,
                &state.member_bindings,
                &state.join_members(),
                false
            ),
            Err(ParallelExecutionError::ParallelJoinPolicyMismatch)
        ));

        let compatible = join_configuration(
            &["member-0"],
            &["member-1", "member-2"],
            1,
            JoinFailurePolicy::MinimumSuccess,
            JoinOrderingPolicy::MemberId,
        );
        assert!(matches!(
            evaluate_bound_parallel_join(
                &parallel,
                &compatible,
                &state.member_bindings,
                &state.join_members(),
                false
            )
            .expect("compatible threshold semantics"),
            JoinDecision::Waiting { .. }
        ));
    }
}
