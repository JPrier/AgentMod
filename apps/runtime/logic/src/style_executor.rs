//! Pure interpretation of an SDK-compiled session-style graph.
//!
//! This module selects nodes and transitions only. Node adapters in turn logic
//! remain responsible for entering the existing proposal, policy, event,
//! artifact, continuation, receipt, and recovery paths.

use agentmod_graph_engine::{ExecutableNode, NodeKind};
use agentmod_primitives::ContentHash;
use agentmod_session_style_sdk::CompiledSessionStyle;
use serde_json::Value;
use thiserror::Error;

use crate::{
    node_execution::{
        DispatchError, ExecuteNodeCommand, NodeCursor, NodeExecutionInput, NodeExecutionOutcome,
        NodeExecutorIdentity, NodePlan, OutcomeCompatibility, TransitionError, dispatch_node,
        dispatch_style_error,
        transition::{LoopState, TransitionSelectionOutcome, select_transition},
        validate_outcome_for_kind,
    },
    session::SessionStyleBinding,
};

/// Runtime-owned adapter classification for one compiled graph node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StyleNodeDirective {
    ContextTransform,
    ModelCall,
    ToolExecutionGate,
    UserApproval,
    SpawnChildAgent,
    SendChildAgentMessage,
    WaitForAgents,
    JoinResults,
    Review,
    Loop,
    ConditionalBranch,
    ParallelBranch,
    Delay,
    Schedule,
    EmitEvent,
    PersistArtifact,
    CompleteTurn,
    CompleteSession,
    Fail,
}

/// Temporary topology compatibility profile.
///
/// The generic node-dispatch engine ([`crate::node_execution`]) replaced
/// topology recognition as a condition of runtime executability. These
/// classifiers are retained only as temporary compatibility diagnostics while
/// the legacy turn adapters are routed through the generic path; dispatch must
/// not be driven by this value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StyleAdapterKind {
    PersistentTurn,
    EphemeralTurn,
    ResearchLoop,
    PlannerWorkerReviewer,
    DeclarativeGraph,
}

impl StyleNodeDirective {
    /// Returns whether restart recovery must not infer that this node's
    /// externally observable work has or has not happened from graph control
    /// events alone.
    pub(crate) const fn requires_effect_evidence(self) -> bool {
        !matches!(
            self,
            Self::Loop
                | Self::ConditionalBranch
                | Self::ParallelBranch
                | Self::CompleteTurn
                | Self::CompleteSession
                | Self::Fail
        )
    }
}

/// Stable cursor into one immutable compiled graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StyleNodeCursor {
    pub index: usize,
    pub id: String,
    pub directive: StyleNodeDirective,
    pub retry_limit: u32,
    pub max_iterations: Option<u32>,
    pub tool: Option<String>,
}

impl StyleNodeCursor {
    /// Returns the raw compiled node kind for the exact graph.
    pub(crate) fn kind(&self, graph: &agentmod_graph_engine::ExecutableGraph) -> NodeKind {
        graph
            .nodes
            .get(self.index)
            .filter(|node| node.index == self.index)
            .map_or(NodeKind::Fail, |node| node.kind)
    }
}

/// Pure transition selected after a node completes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StyleTransition {
    pub from: StyleNodeCursor,
    pub to: StyleNodeCursor,
    pub label: Option<String>,
}

/// Immutable executor over the exact compiled descriptor retained by a session.
#[derive(Clone, Debug)]
pub(crate) struct CompiledStyleExecutor {
    compiled: CompiledSessionStyle,
}

impl CompiledStyleExecutor {
    /// Loads and identity-checks the retained compiled descriptor.
    pub(crate) fn from_binding(binding: &SessionStyleBinding) -> Result<Self, StyleExecutorError> {
        if ContentHash::digest(binding.compiled_style_json.as_bytes())
            != binding.compiled_style_hash
        {
            return Err(StyleExecutorError::CompiledHashMismatch);
        }
        let compiled: CompiledSessionStyle = serde_json::from_str(&binding.compiled_style_json)
            .map_err(|_| StyleExecutorError::InvalidCompiledStyle)?;
        if compiled.style_id != binding.id
            || compiled.style_version != binding.version
            || compiled.cache_key.combined_hash != binding.compiled_cache_key
            || compiled.cache_key.style_content_hash != binding.content_hash
            || compiled.cache_key.plugin_set_hash != binding.plugin_set_hash
            || compiled.cache_key.capability_set_hash != binding.capability_set_hash
        {
            return Err(StyleExecutorError::CompiledIdentityMismatch);
        }
        Ok(Self { compiled })
    }

    /// Returns the graph entry selected by the compiler.
    pub(crate) fn entry(&self) -> Result<StyleNodeCursor, StyleExecutorError> {
        self.cursor(self.compiled.graph.entry_index)
    }

    /// Returns a node by its stable ID.
    pub(crate) fn node(&self, id: &str) -> Result<StyleNodeCursor, StyleExecutorError> {
        let node = self
            .compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.id == id)
            .ok_or_else(|| StyleExecutorError::UnknownNode(id.to_owned()))?;
        Ok(cursor(node))
    }

    /// Selects the one eligible outgoing transition in compiled order.
    ///
    /// An unconditional edge is eligible. A conditional edge is eligible only
    /// when its already-parsed expression evaluates true against `variables`.
    /// Multiple eligible edges are rejected instead of depending on incidental
    /// source order.
    ///
    /// Since the generic node-dispatch engine now owns deterministic
    /// transition behavior, this method is a compatibility wrapper over
    /// [`select_transition`] using the compiled graph as the temporary
    /// execution plan and no completed outcome. Parallel fan-out (a future
    /// Task 6 surface) maps to [`StyleExecutorError::AmbiguousTransition`]
    /// exactly as the legacy single-path selector did.
    pub(crate) fn transition(
        &self,
        from_index: usize,
        variables: &Value,
    ) -> Result<Option<StyleTransition>, StyleExecutorError> {
        let from = self.cursor(from_index)?;
        let plan = self.dispatch_plan();
        let selection = select_transition(
            &self.compiled.graph,
            from_index,
            variables,
            None,
            &LoopState::default(),
            &plan,
        )
        .map_err(StyleExecutorError::from_transition)?;
        match selection {
            TransitionSelectionOutcome::Selected(selected) => Ok(Some(StyleTransition {
                from,
                to: self.cursor(selected.to.index)?,
                label: selected.label,
            })),
            TransitionSelectionOutcome::Parallel(parallel) => {
                Err(StyleExecutorError::AmbiguousTransition {
                    node: parallel.from.id,
                })
            }
            TransitionSelectionOutcome::Terminal => Ok(None),
        }
    }

    /// Returns the execution plan derived from the exact compiled graph.
    ///
    /// This is the temporary integration port until Task 1 persists the
    /// immutable execution plan; the engine consumes the same `NodePlan`
    /// contract either way.
    pub(crate) fn dispatch_plan(&self) -> NodePlan {
        NodePlan::from_graph(&self.compiled.graph)
    }

    /// Returns the compiled entry node kind.
    pub(crate) fn entry_kind(&self) -> Result<NodeKind, StyleExecutorError> {
        self.entry().map(|cursor| cursor.kind(&self.compiled.graph))
    }

    /// Returns the node kinds the runtime can dispatch at graph entry.
    ///
    /// This is the generic replacement for the legacy topology-profile entry
    /// gate: a graph is startable when its entry node has a resolved executor
    /// and a runtime adapter, regardless of the complete topology.
    pub(crate) const fn supported_entry_kinds() -> &'static [NodeKind] {
        &[
            NodeKind::ContextTransform,
            NodeKind::ModelCall,
            NodeKind::ConditionalBranch,
        ]
    }

    /// Whether the runtime owns a node adapter for the exact kind.
    ///
    /// Node behaviors not yet implemented by the runtime (Task 3 native
    /// control-node executors) are deliberately excluded: dispatch fails with
    /// a clear `NoResolvedExecutor` error instead of inventing behavior.
    pub(crate) fn supports_node_dispatch(kind: NodeKind) -> bool {
        matches!(
            kind,
            NodeKind::ContextTransform
                | NodeKind::ModelCall
                | NodeKind::ToolExecutionGate
                | NodeKind::UserApproval
                | NodeKind::SpawnChildAgent
                | NodeKind::WaitForAgents
                | NodeKind::Review
                | NodeKind::Loop
                | NodeKind::ConditionalBranch
                | NodeKind::PersistArtifact
                | NodeKind::CompleteTurn
                | NodeKind::CompleteSession
        )
    }

    /// Converts a style cursor into the engine cursor for the exact node.
    ///
    /// Exercised by focused dispatch tests; consumed by Task 3 node executors
    /// and Task 7 plugin-host transports.
    #[allow(
        dead_code,
        reason = "Task 3/7 node-executor seam; exercised by dispatch_tests"
    )]
    pub(crate) fn dispatch_cursor(
        &self,
        cursor: &StyleNodeCursor,
    ) -> Result<NodeCursor, StyleExecutorError> {
        let node = self
            .compiled
            .graph
            .nodes
            .get(cursor.index)
            .filter(|node| node.index == cursor.index)
            .ok_or(StyleExecutorError::InvalidNodeIndex(cursor.index))?;
        Ok(NodeCursor::from_executable(node))
    }

    /// Validates a typed outcome against the compiled node kind.
    ///
    /// Exercised by focused dispatch tests; consumed by Task 3 node executors.
    #[allow(
        dead_code,
        reason = "Task 3 node-executor seam; exercised by dispatch_tests"
    )]
    pub(crate) fn validate_outcome(
        &self,
        cursor: &StyleNodeCursor,
        outcome: &NodeExecutionOutcome,
    ) -> Result<(), StyleExecutorError> {
        let node = self
            .compiled
            .graph
            .nodes
            .get(cursor.index)
            .filter(|node| node.index == cursor.index)
            .ok_or(StyleExecutorError::InvalidNodeIndex(cursor.index))?;
        match validate_outcome_for_kind(node.kind, outcome) {
            OutcomeCompatibility::Consistent => Ok(()),
            OutcomeCompatibility::Inconsistent => Err(StyleExecutorError::OutcomeInconsistent {
                node: cursor.id.clone(),
                kind: crate::node_execution::serialized_kind(node.kind),
                outcome: outcome.class_name().to_owned(),
            }),
        }
    }

    /// Builds a dispatch command for the exact cursor and resolved identity.
    ///
    /// Exercised by focused dispatch tests; consumed by Task 1 persisted
    /// execution plans and Task 3 node executors.
    #[allow(
        clippy::too_many_arguments,
        reason = "a dispatch command binds the full exact node identity"
    )]
    #[allow(
        dead_code,
        reason = "Task 1/3 dispatch seam; exercised by dispatch_tests"
    )]
    pub(crate) fn dispatch_command(
        &self,
        cursor: &StyleNodeCursor,
        executor: NodeExecutorIdentity,
        input: NodeExecutionInput,
        attempt: u32,
        loop_iteration: u32,
        step: u64,
        max_steps: u64,
    ) -> Result<ExecuteNodeCommand, StyleExecutorError> {
        Ok(ExecuteNodeCommand {
            node: self.dispatch_cursor(cursor)?,
            executor,
            input,
            attempt,
            loop_iteration,
            step,
            max_steps,
        })
    }

    /// Dispatches one compiled node through the generic engine.
    ///
    /// This is the entry point Task 3 node executors (and Task 7 plugin-host
    /// transports) plug into via [`NodeExecutorPort`]. Exercised by focused
    /// dispatch tests.
    #[allow(
        clippy::too_many_arguments,
        reason = "a dispatch command binds the full exact node identity"
    )]
    #[allow(
        dead_code,
        reason = "Task 3/7 node-executor seam; exercised by dispatch_tests"
    )]
    pub(crate) fn dispatch<P>(
        &self,
        port: &P,
        cursor: &StyleNodeCursor,
        executor: NodeExecutorIdentity,
        input: NodeExecutionInput,
        attempt: u32,
        loop_iteration: u32,
        step: u64,
        max_steps: u64,
    ) -> Result<NodeExecutionOutcome, DispatchError>
    where
        P: crate::node_execution::NodeExecutorPort + ?Sized,
    {
        let command = self
            .dispatch_command(
                cursor,
                executor,
                input,
                attempt,
                loop_iteration,
                step,
                max_steps,
            )
            .map_err(|error| dispatch_style_error(&cursor.id, error))?;
        dispatch_node(port, &command)
    }

    /// Returns the retained style descriptor for node adapters and inspection.
    pub(crate) const fn compiled(&self) -> &CompiledSessionStyle {
        &self.compiled
    }

    /// Returns whether this graph is supported by the persistent turn adapter.
    ///
    /// Selection is based on the immutable compiled graph rather than a style
    /// ID, so project and plugin styles can compose the same runtime-owned
    /// lifecycle without impersonating the built-in manifest.
    pub(crate) fn supports_persistent_turn(&self) -> bool {
        let Ok(entry) = self.entry() else {
            return false;
        };
        if entry.directive != StyleNodeDirective::ModelCall {
            return false;
        }
        let Ok(Some(model_to_tools)) = self.transition(entry.index, &serde_json::json!({})) else {
            return false;
        };
        if model_to_tools.to.directive != StyleNodeDirective::ToolExecutionGate {
            return false;
        }
        let Ok(Some(to_complete)) =
            self.transition(model_to_tools.to.index, &serde_json::json!({}))
        else {
            return false;
        };
        to_complete.to.directive == StyleNodeDirective::CompleteTurn
            && self
                .transition(to_complete.to.index, &serde_json::json!({}))
                .is_ok_and(|transition| transition.is_none())
    }

    /// Classifies the exact supported turn lifecycle from compiled semantics.
    ///
    /// Temporary compatibility diagnostic only; the generic dispatch engine
    /// must not depend on this value.
    pub(crate) fn adapter_kind(&self) -> Option<StyleAdapterKind> {
        if self.supports_persistent_turn() {
            return Some(StyleAdapterKind::PersistentTurn);
        }
        if self.supports_ephemeral_turn() {
            return Some(StyleAdapterKind::EphemeralTurn);
        }
        if self.supports_research_loop() {
            return Some(StyleAdapterKind::ResearchLoop);
        }
        if self.supports_planner_worker_reviewer() {
            return Some(StyleAdapterKind::PlannerWorkerReviewer);
        }
        self.supports_declarative_graph()
            .then_some(StyleAdapterKind::DeclarativeGraph)
    }

    /// Returns whether this graph is the exact fresh-context turn lifecycle.
    fn supports_ephemeral_turn(&self) -> bool {
        if self.compiled.graph.nodes.len() != 4 || self.compiled.graph.edges.len() != 3 {
            return false;
        }
        let Ok(entry) = self.entry() else {
            return false;
        };
        if entry.directive != StyleNodeDirective::ContextTransform {
            return false;
        }
        let Ok(Some(to_model)) = self.transition(entry.index, &serde_json::json!({})) else {
            return false;
        };
        if to_model.to.directive != StyleNodeDirective::ModelCall {
            return false;
        }
        let Ok(Some(to_tools)) = self.transition(to_model.to.index, &serde_json::json!({})) else {
            return false;
        };
        if to_tools.to.directive != StyleNodeDirective::ToolExecutionGate {
            return false;
        }
        let Ok(Some(to_complete)) = self.transition(to_tools.to.index, &serde_json::json!({}))
        else {
            return false;
        };
        to_complete.to.directive == StyleNodeDirective::CompleteTurn
            && self
                .transition(to_complete.to.index, &serde_json::json!({}))
                .is_ok_and(|transition| transition.is_none())
    }

    /// Returns whether this graph is the bounded research lifecycle supported
    /// by the runtime-owned research adapter.
    fn supports_research_loop(&self) -> bool {
        if self.compiled.graph.nodes.len() != 6 || self.compiled.graph.edges.len() != 6 {
            return false;
        }
        let Ok(fresh) = self.entry() else {
            return false;
        };
        if fresh.directive != StyleNodeDirective::ContextTransform {
            return false;
        }
        let Ok(Some(to_model)) = self.transition(fresh.index, &serde_json::json!({})) else {
            return false;
        };
        if to_model.to.directive != StyleNodeDirective::ModelCall {
            return false;
        }
        let Ok(Some(to_tools)) = self.transition(to_model.to.index, &serde_json::json!({})) else {
            return false;
        };
        if to_tools.to.directive != StyleNodeDirective::ToolExecutionGate {
            return false;
        }
        let Ok(Some(to_artifact)) = self.transition(to_tools.to.index, &serde_json::json!({}))
        else {
            return false;
        };
        if to_artifact.to.directive != StyleNodeDirective::PersistArtifact {
            return false;
        }
        let Ok(Some(to_loop)) = self.transition(to_artifact.to.index, &serde_json::json!({}))
        else {
            return false;
        };
        if to_loop.to.directive != StyleNodeDirective::Loop || to_loop.to.max_iterations.is_none() {
            return false;
        }
        let Ok(Some(repeat)) = self.transition(
            to_loop.to.index,
            &serde_json::json!({"completion":{"criteria_met":false}}),
        ) else {
            return false;
        };
        let Ok(Some(complete)) = self.transition(
            to_loop.to.index,
            &serde_json::json!({"completion":{"criteria_met":true}}),
        ) else {
            return false;
        };
        repeat.to.index == fresh.index
            && complete.to.directive == StyleNodeDirective::CompleteSession
            && self
                .transition(
                    complete.to.index,
                    &serde_json::json!({"completion":{"criteria_met":true}}),
                )
                .is_ok_and(|transition| transition.is_none())
    }

    fn supports_declarative_graph(&self) -> bool {
        if self.compiled.graph.nodes.len() != 5 || self.compiled.graph.edges.len() != 6 {
            return false;
        }
        let Ok(branch) = self.entry() else {
            return false;
        };
        if branch.directive != StyleNodeDirective::ConditionalBranch {
            return false;
        }
        let Ok(Some(approval)) = self.transition(
            branch.index,
            &serde_json::json!({"request":{"requires_approval":true}}),
        ) else {
            return false;
        };
        let Ok(Some(tool)) = self.transition(
            branch.index,
            &serde_json::json!({"request":{"requires_approval":false}}),
        ) else {
            return false;
        };
        if approval.to.directive != StyleNodeDirective::UserApproval
            || tool.to.directive != StyleNodeDirective::ToolExecutionGate
            || tool.to.tool.as_deref() != Some("filesystem.read")
        {
            return false;
        }
        let Ok(Some(approved_tool)) = self.transition(approval.to.index, &serde_json::json!({}))
        else {
            return false;
        };
        if approved_tool.to.index != tool.to.index {
            return false;
        }
        let Ok(Some(loop_node)) = self.transition(tool.to.index, &serde_json::json!({})) else {
            return false;
        };
        if loop_node.to.directive != StyleNodeDirective::Loop
            || loop_node.to.max_iterations.is_none()
        {
            return false;
        }
        let Ok(Some(repeat)) = self.transition(
            loop_node.to.index,
            &serde_json::json!({"iteration":{"remaining":true}}),
        ) else {
            return false;
        };
        let Ok(Some(done)) = self.transition(
            loop_node.to.index,
            &serde_json::json!({"iteration":{"remaining":false}}),
        ) else {
            return false;
        };
        repeat.to.index == tool.to.index
            && done.to.directive == StyleNodeDirective::CompleteSession
            && self
                .transition(done.to.index, &serde_json::json!({}))
                .is_ok_and(|transition| transition.is_none())
    }

    /// Returns whether this graph is the bounded planner/worker/reviewer
    /// lifecycle supported by the runtime-owned child-session adapter.
    fn supports_planner_worker_reviewer(&self) -> bool {
        if self.compiled.graph.nodes.len() != 7 || self.compiled.graph.edges.len() != 7 {
            return false;
        }
        let Ok(plan) = self.entry() else {
            return false;
        };
        let Ok(Some(spawn)) = self.transition(plan.index, &serde_json::json!({})) else {
            return false;
        };
        let Ok(Some(wait)) = self.transition(spawn.to.index, &serde_json::json!({})) else {
            return false;
        };
        let Ok(Some(integrate)) = self.transition(wait.to.index, &serde_json::json!({})) else {
            return false;
        };
        let Ok(Some(review)) = self.transition(integrate.to.index, &serde_json::json!({})) else {
            return false;
        };
        let Ok(Some(revision)) = self.transition(review.to.index, &serde_json::json!({})) else {
            return false;
        };
        let Ok(Some(retry)) = self.transition(
            revision.to.index,
            &serde_json::json!({"review":{"approved":false}}),
        ) else {
            return false;
        };
        let Ok(Some(done)) = self.transition(
            revision.to.index,
            &serde_json::json!({"review":{"approved":true}}),
        ) else {
            return false;
        };
        plan.directive == StyleNodeDirective::ModelCall
            && spawn.to.directive == StyleNodeDirective::SpawnChildAgent
            && wait.to.directive == StyleNodeDirective::WaitForAgents
            && integrate.to.directive == StyleNodeDirective::ModelCall
            && review.to.directive == StyleNodeDirective::Review
            && revision.to.directive == StyleNodeDirective::Loop
            && revision.to.max_iterations.is_some()
            && retry.to.index == spawn.to.index
            && done.to.directive == StyleNodeDirective::CompleteSession
            && self
                .transition(done.to.index, &serde_json::json!({}))
                .is_ok_and(|transition| transition.is_none())
    }

    fn cursor(&self, index: usize) -> Result<StyleNodeCursor, StyleExecutorError> {
        self.compiled
            .graph
            .nodes
            .get(index)
            .filter(|node| node.index == index)
            .map(cursor)
            .ok_or(StyleExecutorError::InvalidNodeIndex(index))
    }
}

const fn directive(kind: NodeKind) -> StyleNodeDirective {
    match kind {
        NodeKind::ContextTransform => StyleNodeDirective::ContextTransform,
        NodeKind::ModelCall => StyleNodeDirective::ModelCall,
        NodeKind::ToolExecutionGate => StyleNodeDirective::ToolExecutionGate,
        NodeKind::UserApproval => StyleNodeDirective::UserApproval,
        NodeKind::SpawnChildAgent => StyleNodeDirective::SpawnChildAgent,
        NodeKind::SendChildAgentMessage => StyleNodeDirective::SendChildAgentMessage,
        NodeKind::WaitForAgents => StyleNodeDirective::WaitForAgents,
        NodeKind::JoinResults => StyleNodeDirective::JoinResults,
        NodeKind::Review => StyleNodeDirective::Review,
        NodeKind::Loop => StyleNodeDirective::Loop,
        NodeKind::ConditionalBranch => StyleNodeDirective::ConditionalBranch,
        NodeKind::ParallelBranch => StyleNodeDirective::ParallelBranch,
        NodeKind::Delay => StyleNodeDirective::Delay,
        NodeKind::Schedule => StyleNodeDirective::Schedule,
        NodeKind::EmitEvent => StyleNodeDirective::EmitEvent,
        NodeKind::PersistArtifact => StyleNodeDirective::PersistArtifact,
        NodeKind::CompleteTurn => StyleNodeDirective::CompleteTurn,
        NodeKind::CompleteSession => StyleNodeDirective::CompleteSession,
        NodeKind::Fail => StyleNodeDirective::Fail,
    }
}

fn cursor(node: &ExecutableNode) -> StyleNodeCursor {
    StyleNodeCursor {
        index: node.index,
        id: node.id.clone(),
        directive: directive(node.kind),
        retry_limit: node.retry_limit,
        max_iterations: node.max_iterations,
        tool: node.tool.clone(),
    }
}

/// Pure compiled-style interpretation failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StyleExecutorError {
    #[error("the retained compiled style hash does not match the session binding")]
    CompiledHashMismatch,
    #[error("the retained compiled style is invalid")]
    InvalidCompiledStyle,
    #[error("the retained compiled style identity does not match the session binding")]
    CompiledIdentityMismatch,
    #[error("compiled graph node index {0} is invalid")]
    InvalidNodeIndex(usize),
    #[error("compiled graph node `{0}` was not found")]
    UnknownNode(String),
    #[error("condition evaluation failed at graph node `{node}`")]
    ConditionEvaluation { node: String },
    #[error("graph node `{node}` has more than one eligible transition")]
    AmbiguousTransition { node: String },
    #[error("nonterminal graph node `{node}` has no eligible transition")]
    MissingTransition { node: String },
    #[error(
        "transition from node `{node}` targets `{to}`, which is absent from the execution plan"
    )]
    UnknownDestination {
        /// Compiled source node ID.
        node: String,
        /// Destination node ID absent from the plan.
        to: String,
    },
    #[error("outcome `{outcome}` is inconsistent with node `{node}` of kind `{kind}`")]
    OutcomeInconsistent {
        /// Compiled node ID.
        node: String,
        /// Serialized node kind.
        kind: String,
        /// Outcome class label.
        outcome: String,
    },
    #[error("node `{node}` declared output above the compiled bound")]
    OutputExceededBounds {
        /// Compiled node ID.
        node: String,
    },
    #[error("node `{node}` declared a write to an undeclared scope")]
    UndeclaredVariableWrite {
        /// Compiled node ID.
        node: String,
    },
    #[error("node `{node}` would repeat beyond its compiled loop bound")]
    LoopBudgetExceeded {
        /// Compiled node ID.
        node: String,
    },
}

impl StyleExecutorError {
    /// Maps a generic transition rejection into the style executor surface.
    pub(crate) fn from_transition(error: TransitionError) -> Self {
        match error {
            TransitionError::InvalidNodeIndex(index) => Self::InvalidNodeIndex(index),
            TransitionError::UnknownNode(id) => Self::UnknownNode(id),
            TransitionError::ConditionEvaluation { node } => Self::ConditionEvaluation { node },
            TransitionError::MissingTransition { node } => Self::MissingTransition { node },
            TransitionError::AmbiguousTransition { node } => Self::AmbiguousTransition { node },
            TransitionError::UnknownDestination { node, to } => {
                Self::UnknownDestination { node, to }
            }
            TransitionError::OutcomeInconsistent { node, .. } => Self::OutcomeInconsistent {
                node,
                kind: String::from("<unknown>"),
                outcome: String::from("<unknown>"),
            },
            TransitionError::UndeclaredVariableWrite { node, .. } => {
                Self::UndeclaredVariableWrite { node }
            }
            TransitionError::OutputExceededBounds { node, .. } => {
                Self::OutputExceededBounds { node }
            }
            TransitionError::LoopBudgetExceeded { node, .. } => Self::LoopBudgetExceeded { node },
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use agentmod_primitives::ContentHash;
    use agentmod_session_style_sdk::{
        BuiltInStyle, CompileContext, DecisionCapability, StyleCompilerLimits, built_in_manifest,
        compile_style, to_json,
    };
    use serde_json::json;

    use crate::{
        session::{
            SessionCompactionConfiguration, SessionMemoryConfiguration, SessionPermissionDefaults,
            SessionStyleBinding, SessionStyleBudgets, SessionStyleSource,
        },
        style_executor::{CompiledStyleExecutor, StyleExecutorError, StyleNodeDirective},
    };

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture explicitly binds every immutable session-style selection"
    )]
    pub(crate) fn binding(style: BuiltInStyle) -> SessionStyleBinding {
        let manifest = built_in_manifest(style);
        let manifest_json = to_json(&manifest).expect("manifest json");
        let plugin_set_hash = ContentHash::digest(b"plugins");
        let context = compile_context(plugin_set_hash);
        let compiled =
            compile_style(&manifest, &context, StyleCompilerLimits::default()).expect("compile");
        let compiled_json = serde_json::to_string(&compiled).expect("compiled json");
        SessionStyleBinding {
            id: compiled.style_id.clone(),
            version: compiled.style_version.clone(),
            content_hash: compiled.cache_key.style_content_hash,
            compiled_cache_key: compiled.cache_key.combined_hash,
            compiled_style_hash: ContentHash::digest(compiled_json.as_bytes()),
            source: SessionStyleSource::BuiltIn,
            source_locator: format!("built-in:{}", compiled.style_id),
            plugin_set_hash,
            capability_set_hash: compiled.cache_key.capability_set_hash,
            runtime_api_version: String::from("1.0.0"),
            configuration_json: manifest_json,
            compiled_style_json: compiled_json,
            memory: SessionMemoryConfiguration {
                provider: String::from("fixture"),
                scopes: Vec::new(),
                retrieval_timing: String::from("never"),
                query_json: String::from(
                    r#"{"source":"current_input","include_active_artifacts":false,"include_style_context":false,"max_query_bytes":16384}"#,
                ),
                max_items: 0,
                max_injected_bytes: 0,
                write_policy: String::from("never"),
                injection_location: String::from("none"),
            },
            compaction: SessionCompactionConfiguration {
                strategy: String::from("fixture"),
                trigger_tokens: None,
                reserved_context_tokens: 0,
                max_provider_projection_tokens: 0,
                preserve_unresolved_tasks: true,
                preserve_active_processes: true,
                preservation_requirements: Vec::new(),
            },
            tool_groups: Vec::new(),
            harness: String::from("native"),
            harness_version: String::from("1.0.0"),
            harness_capability_set_hash: plugin_set_hash,
            harness_required_capabilities: compiled.harness.required_capabilities.clone(),
            required_capabilities: Vec::new(),
            interceptor_order: Vec::new(),
            budgets: SessionStyleBudgets {
                max_iterations: 1,
                max_steps: 10,
                max_tokens: 10,
                max_cost_micros: 0,
                max_duration_ms: 10,
            },
            permission_defaults: SessionPermissionDefaults {
                default: String::from("ask"),
                groups: BTreeMap::new(),
            },
            child_agent_policy_json: String::from("{}"),
            retry_policy_json: String::from("{}"),
            termination_policy_json: String::from("{}"),
        }
    }

    #[test]
    fn persistent_chat_maps_the_compiled_graph_without_a_parallel_model() {
        let executor = CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::PersistentChat))
            .expect("executor");
        assert!(executor.supports_persistent_turn());
        let respond = executor.entry().expect("entry");
        assert_eq!(respond.id, "respond");
        assert_eq!(respond.directive, StyleNodeDirective::ModelCall);
        let tool = executor
            .transition(respond.index, &json!({}))
            .expect("transition")
            .expect("tool");
        assert_eq!(tool.to.id, "tool");
        assert_eq!(tool.to.directive, StyleNodeDirective::ToolExecutionGate);
        let done = executor
            .transition(tool.to.index, &json!({}))
            .expect("transition")
            .expect("done");
        assert_eq!(done.to.directive, StyleNodeDirective::CompleteTurn);
        assert_eq!(executor.transition(done.to.index, &json!({})), Ok(None));
    }

    #[test]
    fn ephemeral_turn_selects_the_fresh_context_adapter_from_compiled_semantics() {
        let executor = CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::EphemeralTurn))
            .expect("executor");
        assert_eq!(
            executor.adapter_kind(),
            Some(super::StyleAdapterKind::EphemeralTurn)
        );
        assert_eq!(
            executor.entry().expect("entry").directive,
            StyleNodeDirective::ContextTransform
        );
    }

    #[test]
    fn research_loop_uses_compiled_conditions_and_rejects_missing_variables() {
        let executor = CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::ResearchLoop))
            .expect("executor");
        assert!(!executor.supports_persistent_turn());
        assert_eq!(
            executor.adapter_kind(),
            Some(super::StyleAdapterKind::ResearchLoop)
        );
        let loop_node = executor.node("repeat").expect("loop");
        let repeat = executor
            .transition(
                loop_node.index,
                &json!({"completion":{"criteria_met":false}}),
            )
            .expect("repeat transition")
            .expect("repeat");
        assert_eq!(repeat.to.id, "fresh-context");
        let done = executor
            .transition(
                loop_node.index,
                &json!({"completion":{"criteria_met":true}}),
            )
            .expect("done transition")
            .expect("done");
        assert_eq!(done.to.id, "done");
        assert_eq!(
            executor.transition(loop_node.index, &json!({})),
            Err(StyleExecutorError::ConditionEvaluation {
                node: String::from("repeat")
            })
        );
    }

    #[test]
    fn planner_worker_reviewer_uses_the_compiled_child_and_review_loop() {
        let executor = CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::PlannerWorker))
            .expect("executor");
        assert_eq!(
            executor.adapter_kind(),
            Some(super::StyleAdapterKind::PlannerWorkerReviewer)
        );
        let plan = executor.entry().expect("plan");
        let spawn = executor
            .transition(plan.index, &json!({}))
            .expect("plan transition")
            .expect("spawn");
        assert_eq!(spawn.to.directive, StyleNodeDirective::SpawnChildAgent);
        let revision = executor.node("revision").expect("revision");
        let retry = executor
            .transition(revision.index, &json!({"review":{"approved":false}}))
            .expect("retry transition")
            .expect("spawn retry");
        assert_eq!(retry.to.id, "spawn-workers");
        let done = executor
            .transition(revision.index, &json!({"review":{"approved":true}}))
            .expect("approved transition")
            .expect("done");
        assert_eq!(done.to.directive, StyleNodeDirective::CompleteSession);
    }

    #[test]
    fn declarative_graph_uses_compiled_branch_tool_loop_and_terminal_nodes() {
        let executor =
            CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::DeclarativeGraph))
                .expect("executor");
        assert_eq!(
            executor.adapter_kind(),
            Some(super::StyleAdapterKind::DeclarativeGraph)
        );
        let branch = executor.entry().expect("branch");
        let approval = executor
            .transition(branch.index, &json!({"request":{"requires_approval":true}}))
            .expect("approval transition")
            .expect("approval");
        assert_eq!(approval.to.directive, StyleNodeDirective::UserApproval);
        let tool = executor
            .transition(
                branch.index,
                &json!({"request":{"requires_approval":false}}),
            )
            .expect("tool transition")
            .expect("tool");
        assert_eq!(tool.to.tool.as_deref(), Some("filesystem.read"));
        let repeat = executor
            .node("repeat")
            .and_then(|node| {
                executor
                    .transition(node.index, &json!({"iteration":{"remaining":true}}))
                    .map(|transition| transition.expect("repeat"))
            })
            .expect("repeat transition");
        assert_eq!(repeat.to.id, "tool");
    }

    /// Shared compile context used by every fixture binding.
    pub(crate) fn compile_context(plugin_set_hash: ContentHash) -> CompileContext {
        CompileContext {
            runtime_api_version: String::from("1.0.0"),
            plugin_set_hash,
            capabilities: [
                "agents",
                "approval",
                "artifacts",
                "context",
                "events",
                "model",
                "scheduling",
                "tools",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            tool_groups: BTreeMap::from([(
                String::from("filesystem"),
                BTreeSet::from([String::from("filesystem.read")]),
            )]),
            providers: BTreeSet::from([String::from("mock")]),
            plugins: BTreeSet::from([String::from("runtime.security")]),
            memory_providers: ["none", "file", "sqlite-fts"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            compaction_strategies: [
                "none",
                "sliding_window",
                "summary",
                "artifact_handoff",
                "tool_output_eviction",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            supported_decisions: BTreeSet::from([
                DecisionCapability::Continue,
                DecisionCapability::Replace,
                DecisionCapability::Reject,
                DecisionCapability::RequireApproval,
                DecisionCapability::Defer,
                DecisionCapability::Cancel,
                DecisionCapability::Fork,
            ]),
            graph_references: BTreeMap::new(),
        }
    }

    #[test]
    fn compiled_descriptor_tampering_fails_before_execution() {
        let mut binding = binding(BuiltInStyle::PersistentChat);
        binding.compiled_style_json.push(' ');
        assert_eq!(
            CompiledStyleExecutor::from_binding(&binding).expect_err("tampering"),
            StyleExecutorError::CompiledHashMismatch
        );
    }
}
