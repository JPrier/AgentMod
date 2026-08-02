//! Generic runtime-logic node dispatch engine.
//!
//! A compiled style graph is executable because every node has an available
//! resolved executor identity, not because the complete topology matches one
//! of a small set of built-in adapter profiles. This module owns the generic
//! dispatch contract:
//!
//! ```text
//! current node + persisted resolved executor identity
//!         -> generic executor dispatch
//!         -> typed node outcome
//!         -> runtime validates outcome
//!         -> canonical node/variable/transition events
//! ```
//!
//! The engine is pure logic: it never mutates canonical state and never
//! fabricates external-effect completion. Node executors (runtime adapters,
//! plugin-host invocations) live behind [`NodeExecutorPort`] and return typed
//! [`NodeExecutionOutcome`] values. The dispatch reducer validates the outcome
//! and produces the canonical actions the caller commits.
//!
//! Integration seams:
//! - Task 1 (immutable execution plan): a [`NodePlan`] supplies the exact set
//!   of known node IDs; transitions to absent nodes are rejected.
//! - Task 3 (native control-node executors): new node behaviors implement
//!   [`NodeExecutorPort`] and register a [`NodeExecutorIdentity`].
//! - Task 4 (graph variables): [`NodeExecutionInput::variables`] carries
//!   canonical variable input; declared variable writes are validated against
//!   the compiled graph's write scopes.
//! - Task 7 (plugin-host transport): plugin executors resolve to
//!   [`ExecutorBoundary::PluginHost`] identities through the same dispatch
//!   path.

#[cfg(test)]
pub(crate) mod dispatch_tests;
pub mod outcome;
pub mod recovery;
pub mod reducer;
pub mod transition;

use agentmod_graph_engine::{ExecutableNode, NodeKind};
use agentmod_primitives::Sequence;
use serde_json::Value;
use thiserror::Error;

use crate::node_executor::ResolvedNodeExecutor;
use crate::style_executor::StyleExecutorError;

pub use outcome::{OutcomeCompatibility, validate_outcome_for_kind};
pub use recovery::{EffectEvidence, NodeRecoveryClass, NodeStateEvidence, classify_node};
pub use reducer::{
    NodeDispatchDecision, NodeDispatchEvent, NodeDispatchEventKind, NodeDispatchReducer,
};
pub use transition::{
    LoopState, NodePlan, TransitionError, TransitionSelection, select_transition,
};

/// Bounded canonical output produced by one completed node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedNodeOutput {
    /// Durable reference to the node result.
    pub reference: String,
    /// Durable reference to a full result artifact, when one was produced.
    #[allow(dead_code)] // exposed for composition-root consumers and tests
    pub artifact_reference: Option<String>,
    /// Executor-declared serialized byte size, when measured.
    ///
    /// `None` preserves legacy adapters that do not measure output; a declared
    /// size above [`MAX_NODE_OUTPUT_BYTES`] is rejected before transition.
    pub declared_bytes: Option<u64>,
    /// State scope names the outcome declares it wrote.
    ///
    /// Every declared write must be covered by the compiled node's declared
    /// `write_scopes`; undeclared writes are rejected by the dispatcher.
    pub variable_writes: Vec<String>,
}

impl BoundedNodeOutput {
    /// Constructs a legacy adapter output with no measured bound or writes.
    pub fn reference(reference: impl Into<String>) -> Self {
        Self {
            reference: reference.into(),
            artifact_reference: None,
            declared_bytes: None,
            variable_writes: Vec::new(),
        }
    }
}

/// Hard bound on any single node outcome's declared output.
pub const MAX_NODE_OUTPUT_BYTES: u64 = 16 * 1024 * 1024;

/// Durable continuation the node is waiting on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationEvidence {
    /// Exact continuation identity (resume-once).
    pub continuation_id: String,
    /// Stable continuation class, e.g. `approval_owned` or `schedule_owned`.
    pub resume_state: String,
    /// Whether the continuation is already terminal.
    pub terminal: bool,
}

/// Child-session state the node is waiting on.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChildSessionEvidence {
    /// Exact child identities bound to this node.
    pub child_ids: Vec<String>,
    /// Children with canonical terminal joins.
    pub joined: Vec<String>,
    /// Children still pending.
    pub pending: Vec<String>,
}

/// Parallel-branch state the node is waiting on.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParallelBranchEvidence {
    /// Exact branch identities bound to this node.
    pub branch_ids: Vec<String>,
    /// Branches with canonical terminal outcomes.
    pub completed: Vec<String>,
}

/// Scheduler evidence for delay/schedule nodes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchedulerEvidence {
    /// Exact durable occurrence identity.
    pub occurrence_id: String,
    /// Whether the claim already has a terminal worker marker.
    pub terminal: bool,
}

/// Structured reason for a requested retry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryReason {
    /// Stable redacted reason code.
    pub code: String,
    /// Safe deterministic explanation.
    pub message: String,
}

/// Structured node failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeFailure {
    /// Stable redacted failure code.
    pub code: String,
    /// Safe deterministic explanation.
    pub message: String,
    /// Durable reference to failure details when retained separately.
    pub artifact_reference: Option<String>,
}

/// Bounded typed input handed to a node executor.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NodeExecutionInput {
    /// Canonical graph variables available to the executor and transition
    /// selection (Task 4 integration seam).
    pub variables: Value,
    /// Durable reference to already committed work, when the node resumes.
    pub result_reference: Option<String>,
    /// Durable reference to a result artifact, when one exists.
    pub artifact_reference: Option<String>,
    /// Continuation evidence when the node is resumed from a durable wait.
    pub continuation: Option<ContinuationEvidence>,
    /// Child-session evidence when the node joins children.
    pub children: Option<ChildSessionEvidence>,
    /// Parallel-branch evidence when the node joins branches.
    pub parallel: Option<ParallelBranchEvidence>,
    /// Scheduler evidence when the node resumes from a durable wake.
    pub scheduler: Option<SchedulerEvidence>,
}

/// Compiled node position the engine dispatches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeCursor {
    /// Deterministic compiled node index.
    pub index: usize,
    /// Stable compiled node ID.
    pub id: String,
    /// Generic compiled node kind.
    pub kind: NodeKind,
    /// Compiled business retry limit.
    pub retry_limit: u32,
    /// Compiled static loop bound, when declared.
    pub max_iterations: Option<u32>,
    /// Compiled tool selection, when declared.
    pub tool: Option<String>,
}

impl NodeCursor {
    /// Builds an engine cursor from a compiled graph node.
    #[must_use]
    pub fn from_executable(node: &ExecutableNode) -> Self {
        Self {
            index: node.index,
            id: node.id.clone(),
            kind: node.kind,
            retry_limit: node.retry_limit,
            max_iterations: node.max_iterations,
            tool: node.tool.clone(),
        }
    }

    /// Whether this node kind terminates turn or session execution.
    #[must_use]
    pub fn is_terminal_kind(&self) -> bool {
        matches!(
            self.kind,
            NodeKind::CompleteTurn | NodeKind::CompleteSession | NodeKind::Fail
        )
    }
}

/// Execution boundary of a resolved node executor.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ExecutorBoundary {
    /// Runtime logic owns execution.
    RuntimeLogic,
    /// Isolated plugin-host invocation owns execution.
    PluginHost,
}

/// Exact resolved executor identity for one compiled node.
///
/// Dispatch is driven by this identity, never by style ID, adapter kind,
/// fixture name, node counts, edge counts, or a hard-coded tool name.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeExecutorIdentity {
    /// Compiled graph node ID.
    pub node_id: String,
    /// Serialized compiled node kind.
    pub node_kind: String,
    /// Stable implementation ID.
    pub implementation_id: String,
    /// Exact implementation version.
    pub implementation_version: String,
    /// Execution boundary.
    pub boundary: ExecutorBoundary,
}

impl NodeExecutorIdentity {
    /// Builds an identity from the runtime's exact resolution record.
    #[must_use]
    pub fn from_resolved(resolved: &ResolvedNodeExecutor) -> Self {
        Self {
            node_id: resolved.node_id.clone(),
            node_kind: resolved.node_kind.clone(),
            implementation_id: resolved.implementation_id.clone(),
            implementation_version: resolved.implementation_version.clone(),
            boundary: match resolved.boundary {
                crate::node_executor::NodeExecutorBoundary::RuntimeLogic => {
                    ExecutorBoundary::RuntimeLogic
                }
                crate::node_executor::NodeExecutorBoundary::PluginHost => {
                    ExecutorBoundary::PluginHost
                }
            },
        }
    }
}

/// Command to execute one compiled node through its resolved executor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecuteNodeCommand {
    /// Compiled node being executed.
    pub node: NodeCursor,
    /// Exact resolved executor identity.
    pub executor: NodeExecutorIdentity,
    /// Bounded typed input.
    pub input: NodeExecutionInput,
    /// One-based execution attempt.
    pub attempt: u32,
    /// Zero-based loop iteration containing this attempt.
    pub loop_iteration: u32,
    /// One-based graph step counter.
    pub step: u64,
    /// Effective hard step bound for this session execution.
    pub max_steps: u64,
}

/// Typed outcome produced by a node executor.
///
/// Executors never mutate canonical state; they return an outcome and the
/// runtime logic validates and reduces it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeExecutionOutcome {
    /// Completed with bounded typed output.
    Completed {
        /// Bounded typed output.
        output: BoundedNodeOutput,
    },
    /// Waiting on a durable continuation.
    WaitingOnContinuation {
        /// Durable continuation identity.
        evidence: ContinuationEvidence,
    },
    /// Waiting on child sessions.
    WaitingOnChildren {
        /// Child-session state.
        evidence: ChildSessionEvidence,
    },
    /// Waiting on parallel branches.
    WaitingOnParallelBranches {
        /// Parallel-branch state.
        evidence: ParallelBranchEvidence,
    },
    /// Retry requested with a structured reason.
    Retry {
        /// Structured reason.
        reason: RetryReason,
        /// Proposed next attempt (one-based).
        next_attempt: u32,
    },
    /// Failed with a structured failure.
    Failed {
        /// Structured failure.
        failure: NodeFailure,
    },
    /// Terminal turn completion.
    CompleteTurn {
        /// Bounded terminal output.
        output: BoundedNodeOutput,
    },
    /// Terminal session completion.
    CompleteSession {
        /// Bounded terminal output.
        output: BoundedNodeOutput,
    },
}

impl NodeExecutionOutcome {
    /// Stable outcome-class label used in diagnostics and traces.
    #[must_use]
    pub const fn class_name(&self) -> &'static str {
        match self {
            Self::Completed { .. } => "completed",
            Self::WaitingOnContinuation { .. } => "waiting_on_continuation",
            Self::WaitingOnChildren { .. } => "waiting_on_children",
            Self::WaitingOnParallelBranches { .. } => "waiting_on_parallel_branches",
            Self::Retry { .. } => "retry",
            Self::Failed { .. } => "failed",
            Self::CompleteTurn { .. } => "complete_turn",
            Self::CompleteSession { .. } => "complete_session",
        }
    }

    /// Output bytes declared by this outcome, when measured.
    #[must_use]
    pub const fn declared_bytes(&self) -> Option<u64> {
        match self {
            Self::Completed { output }
            | Self::CompleteTurn { output }
            | Self::CompleteSession { output } => output.declared_bytes,
            _ => None,
        }
    }
}

/// Execution boundary for one node executor.
///
/// The port is the runtime-logic seam node executors implement. Runtime
/// adapters and (in Task 7) plugin-host transports register an exact
/// [`NodeExecutorIdentity`] and produce typed [`NodeExecutionOutcome`] values.
/// Implementations must never mutate canonical state and must never fabricate
/// external-effect completion; waiting outcomes must reference durable
/// continuation/child/scheduler evidence.
pub trait NodeExecutorPort {
    /// Whether this port can execute the exact resolved identity.
    fn can_execute(&self, identity: &NodeExecutorIdentity) -> bool;

    /// Executes the node and returns a typed outcome.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError`] when the executor itself fails before it can
    /// produce a typed outcome.
    fn execute(&self, command: &ExecuteNodeCommand) -> Result<NodeExecutionOutcome, DispatchError>;
}

/// Failure while resolving, dispatching, or reducing a node outcome.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DispatchError {
    /// No executor is available for the exact resolved identity.
    #[error("no resolved executor `{implementation}` can dispatch node `{node}`")]
    NoResolvedExecutor {
        /// Compiled node ID.
        node: String,
        /// Resolved implementation ID.
        implementation: String,
    },
    /// The command violates dispatch invariants.
    #[error("invalid dispatch command for node `{node}`: {reason}")]
    InvalidCommand {
        /// Compiled node ID.
        node: String,
        /// Deterministic reason.
        reason: String,
    },
    /// The outcome is inconsistent with the compiled node kind.
    #[error("outcome `{outcome}` is inconsistent with node `{node}` of kind `{kind}`")]
    OutcomeInconsistent {
        /// Compiled node ID.
        node: String,
        /// Serialized node kind.
        kind: String,
        /// Outcome class label.
        outcome: String,
    },
    /// A retry exceeds the compiled retry budget.
    #[error("node `{node}` requested retry at attempt {attempt} beyond retry limit {retry_limit}")]
    RetryBudgetExceeded {
        /// Compiled node ID.
        node: String,
        /// Current one-based attempt.
        attempt: u32,
        /// Compiled retry limit.
        retry_limit: u32,
    },
    /// A declared output exceeds the engine bound.
    #[error("node `{node}` declared {bytes} output bytes above the {limit}-byte bound")]
    OutputExceededBounds {
        /// Compiled node ID.
        node: String,
        /// Declared output bytes.
        bytes: u64,
        /// Effective bound.
        limit: u64,
    },
    /// Transition selection rejected the outcome or graph.
    #[error("transition selection failed: {0}")]
    Transition(#[from] TransitionError),
    /// The resolved executor failed before producing a typed outcome.
    #[error("node executor failed for node `{node}`: {reason}")]
    Executor {
        /// Compiled node ID.
        node: String,
        /// Redacted reason.
        reason: String,
    },
}

/// Validates and normalizes a dispatch command.
///
/// # Errors
///
/// Returns [`DispatchError`] when counters are invalid or the identity does
/// not match the compiled node.
pub fn validate_command(command: &ExecuteNodeCommand) -> Result<(), DispatchError> {
    if command.attempt == 0 {
        return Err(DispatchError::InvalidCommand {
            node: command.node.id.clone(),
            reason: String::from("attempt must be one-based"),
        });
    }
    if command.step == 0 {
        return Err(DispatchError::InvalidCommand {
            node: command.node.id.clone(),
            reason: String::from("step must be one-based"),
        });
    }
    if command.step > command.max_steps {
        return Err(DispatchError::InvalidCommand {
            node: command.node.id.clone(),
            reason: format!(
                "step {} exceeds the {} effective step bound",
                command.step, command.max_steps
            ),
        });
    }
    if command.executor.node_id != command.node.id {
        return Err(DispatchError::InvalidCommand {
            node: command.node.id.clone(),
            reason: format!(
                "executor identity resolves node `{}` instead of `{}`",
                command.executor.node_id, command.node.id
            ),
        });
    }
    Ok(())
}

/// Dispatches one compiled node through the exact resolved executor identity.
///
/// The engine:
/// 1. validates the command invariants;
/// 2. requires the port to execute the exact identity (no fallback to style,
///    adapter, or topology heuristics);
/// 3. executes through the port;
/// 4. validates the typed outcome against the compiled node kind and bounds;
/// 5. returns the validated outcome for the caller to reduce.
///
/// # Errors
///
/// Returns [`DispatchError`] when the command is invalid, the identity is not
/// executable, the executor fails, or the outcome is inconsistent.
pub fn dispatch_node<P>(
    port: &P,
    command: &ExecuteNodeCommand,
) -> Result<NodeExecutionOutcome, DispatchError>
where
    P: NodeExecutorPort + ?Sized,
{
    validate_command(command)?;
    if !port.can_execute(&command.executor) {
        return Err(DispatchError::NoResolvedExecutor {
            node: command.node.id.clone(),
            implementation: command.executor.implementation_id.clone(),
        });
    }
    let outcome = port
        .execute(command)
        .map_err(|error| DispatchError::Executor {
            node: command.node.id.clone(),
            reason: error.to_string(),
        })?;
    if validate_outcome_for_kind(command.node.kind, &outcome) != OutcomeCompatibility::Consistent {
        return Err(DispatchError::OutcomeInconsistent {
            node: command.node.id.clone(),
            kind: serialized_kind(command.node.kind),
            outcome: outcome.class_name().to_owned(),
        });
    }
    validate_output_bounds(command, &outcome)?;
    Ok(outcome)
}

/// Validates declared output bounds for an outcome.
///
/// # Errors
///
/// Returns [`DispatchError::OutputExceededBounds`] when the outcome declares
/// more output than the engine allows.
fn validate_output_bounds(
    command: &ExecuteNodeCommand,
    outcome: &NodeExecutionOutcome,
) -> Result<(), DispatchError> {
    if let Some(bytes) = outcome
        .declared_bytes()
        .filter(|bytes| *bytes > MAX_NODE_OUTPUT_BYTES)
    {
        return Err(DispatchError::OutputExceededBounds {
            node: command.node.id.clone(),
            bytes,
            limit: MAX_NODE_OUTPUT_BYTES,
        });
    }
    Ok(())
}

/// Serializes a compiled node kind for diagnostics.
#[must_use]
pub fn serialized_kind(kind: NodeKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{kind:?}"))
}

/// Normalizes a compiled style error into the dispatch error surface.
#[must_use]
pub fn dispatch_style_error(node: &str, error: StyleExecutorError) -> DispatchError {
    match error {
        StyleExecutorError::AmbiguousTransition { node } => {
            DispatchError::Transition(TransitionError::AmbiguousTransition { node })
        }
        StyleExecutorError::MissingTransition { node } => {
            DispatchError::Transition(TransitionError::MissingTransition { node })
        }
        StyleExecutorError::ConditionEvaluation { node } => {
            DispatchError::Transition(TransitionError::ConditionEvaluation { node })
        }
        StyleExecutorError::InvalidNodeIndex(index) => {
            DispatchError::Transition(TransitionError::InvalidNodeIndex(index))
        }
        StyleExecutorError::UnknownNode(id) => {
            DispatchError::Transition(TransitionError::UnknownNode(id))
        }
        other => DispatchError::InvalidCommand {
            node: node.to_owned(),
            reason: other.to_string(),
        },
    }
}

/// Sequence adapter used by recovery classification inputs.
pub type RecoverySequence = Sequence;

#[cfg(test)]
pub(crate) mod tests {
    use agentmod_graph_engine::NodeKind;

    use super::*;
    use crate::node_execution::tests as engine_tests;

    #[test]
    fn dispatch_requires_exact_resolved_identity() {
        struct MissingPort;
        impl NodeExecutorPort for MissingPort {
            fn can_execute(&self, _identity: &NodeExecutorIdentity) -> bool {
                false
            }
            fn execute(
                &self,
                _command: &ExecuteNodeCommand,
            ) -> Result<NodeExecutionOutcome, DispatchError> {
                unreachable!("identity gate must fail first")
            }
        }
        let command = engine_tests::model_command();
        assert_eq!(
            dispatch_node(&MissingPort, &command),
            Err(DispatchError::NoResolvedExecutor {
                node: String::from("respond"),
                implementation: String::from("runtime.model_call"),
            })
        );
    }

    #[test]
    fn dispatch_rejects_command_with_zero_attempt() {
        let mut command = engine_tests::model_command();
        command.attempt = 0;
        assert!(matches!(
            dispatch_node(&engine_tests::CompletingPort::default(), &command),
            Err(DispatchError::InvalidCommand { .. })
        ));
    }

    #[test]
    fn dispatch_rejects_identity_for_another_node() {
        let mut command = engine_tests::model_command();
        command.executor.node_id = String::from("other");
        assert!(matches!(
            dispatch_node(&engine_tests::CompletingPort::default(), &command),
            Err(DispatchError::InvalidCommand { .. })
        ));
    }

    #[test]
    fn dispatch_rejects_oversized_declared_output() {
        let command = engine_tests::model_command();
        let port = engine_tests::CompletingPort {
            outcome: NodeExecutionOutcome::Completed {
                output: BoundedNodeOutput {
                    reference: String::from("huge"),
                    artifact_reference: None,
                    declared_bytes: Some(MAX_NODE_OUTPUT_BYTES + 1),
                    variable_writes: Vec::new(),
                },
            },
        };
        assert_eq!(
            dispatch_node(&port, &command).expect_err("bound"),
            DispatchError::OutputExceededBounds {
                node: String::from("respond"),
                bytes: MAX_NODE_OUTPUT_BYTES + 1,
                limit: MAX_NODE_OUTPUT_BYTES,
            }
        );
    }

    /// Shared fixture: a compiled `ModelCall` node resolved to the runtime
    /// model executor.
    pub(crate) fn model_command() -> ExecuteNodeCommand {
        ExecuteNodeCommand {
            node: NodeCursor {
                index: 0,
                id: String::from("respond"),
                kind: NodeKind::ModelCall,
                retry_limit: 0,
                max_iterations: None,
                tool: None,
            },
            executor: NodeExecutorIdentity {
                node_id: String::from("respond"),
                node_kind: String::from("model_call"),
                implementation_id: String::from("runtime.model_call"),
                implementation_version: String::from("1.0.0"),
                boundary: ExecutorBoundary::RuntimeLogic,
            },
            input: NodeExecutionInput::default(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            max_steps: 100,
        }
    }

    /// Mock port that completes with a configurable outcome.
    #[derive(Clone, Debug)]
    pub(crate) struct CompletingPort {
        pub outcome: NodeExecutionOutcome,
    }

    impl Default for CompletingPort {
        fn default() -> Self {
            Self {
                outcome: NodeExecutionOutcome::Completed {
                    output: BoundedNodeOutput::reference("model:test"),
                },
            }
        }
    }

    impl NodeExecutorPort for CompletingPort {
        fn can_execute(&self, identity: &NodeExecutorIdentity) -> bool {
            identity.implementation_id == "runtime.model_call" && identity.node_kind == "model_call"
        }

        fn execute(
            &self,
            _command: &ExecuteNodeCommand,
        ) -> Result<NodeExecutionOutcome, DispatchError> {
            Ok(self.outcome.clone())
        }
    }
}
