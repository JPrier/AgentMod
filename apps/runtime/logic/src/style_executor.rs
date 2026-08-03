//! Pure interpretation of an SDK-compiled session-style graph.
//!
//! This module selects nodes and transitions only. Node adapters in turn logic
//! remain responsible for entering the existing proposal, policy, event,
//! artifact, continuation, receipt, and recovery paths.

use agentmod_graph_engine::{ExecutableNode, NodeConfiguration};
use agentmod_primitives::ContentHash;
use agentmod_session_style_sdk::CompiledSessionStyle;
use serde_json::Value;
use thiserror::Error;

use crate::{
    node_execution::{NativeExecutorKey, native_executor_key},
    session::{
        ExecutionPlanCompilerGeneration, SessionNodeExecutorBoundary,
        SessionNodeExecutorResolution, SessionNodeExecutorSource, SessionStyleBinding,
    },
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

/// Runtime adapter selected from the exact validated compiled graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StyleAdapterKind {
    PersistentTurn,
    EphemeralTurn,
    ResearchLoop,
    PlannerWorkerReviewer,
    DeclarativeGraph,
}

/// Authoritative runtime route selected from immutable plan generation and
/// compiled graph semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionDispatchMode {
    /// Every node executes through its exact persisted executor resolution.
    Generic,
    /// Existing adapter execution and recovery semantics remain authoritative.
    Legacy(StyleAdapterKind),
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
    pub resolution: SessionNodeExecutorResolution,
    pub configuration: Option<NodeConfiguration>,
    pub retry_limit: u32,
    pub max_iterations: Option<u32>,
    pub tool: Option<String>,
    pub provider: Option<String>,
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
    resolutions: Vec<SessionNodeExecutorResolution>,
    plan_generation: Option<ExecutionPlanCompilerGeneration>,
}

impl CompiledStyleExecutor {
    /// Loads and identity-checks the retained compiled descriptor.
    pub(crate) fn from_binding(binding: &SessionStyleBinding) -> Result<Self, StyleExecutorError> {
        let compiled = Self::load_compiled(binding)?;
        let plan = binding
            .execution_plan
            .as_ref()
            .ok_or(StyleExecutorError::MissingExecutionPlan)?;
        let plan_generation = plan
            .compilation
            .compiler_generation()
            .ok_or(StyleExecutorError::UnsupportedExecutionPlanCompiler)?;
        let retained_plan_hash = binding
            .execution_plan_hash
            .ok_or(StyleExecutorError::MissingExecutionPlan)?;
        let serialized_plan =
            serde_json::to_vec(plan).map_err(|_| StyleExecutorError::InvalidExecutionPlan)?;
        if ContentHash::digest(&serialized_plan) != retained_plan_hash {
            return Err(StyleExecutorError::ExecutionPlanHashMismatch);
        }
        if plan.compilation.compiled_style_hash != binding.compiled_style_hash
            || plan.compilation.compiled_cache_key != binding.compiled_cache_key
            || plan.compilation.runtime_api_version != binding.runtime_api_version
            || plan.nodes.len() != compiled.graph.nodes.len()
        {
            return Err(StyleExecutorError::ExecutionPlanMismatch);
        }
        for node in &compiled.graph.nodes {
            let matches = plan
                .nodes
                .iter()
                .filter(|resolution| resolution.node_id == node.id)
                .collect::<Vec<_>>();
            if matches.len() != 1 {
                return Err(StyleExecutorError::ExecutionPlanMismatch);
            }
            validate_resolution(node, matches[0])?;
        }
        Ok(Self {
            compiled,
            resolutions: plan.nodes.clone(),
            plan_generation: Some(plan_generation),
        })
    }

    /// Loads only the compiled descriptor while constructing its first
    /// immutable execution plan. No cursor or dispatch operation is available
    /// through this unbound view.
    pub(crate) fn from_unbound_binding(
        binding: &SessionStyleBinding,
    ) -> Result<Self, StyleExecutorError> {
        let compiled = Self::load_compiled(binding)?;
        Ok(Self {
            compiled,
            resolutions: Vec::new(),
            plan_generation: None,
        })
    }

    /// Returns whether every node resolves to an exact implementation currently
    /// supported by generic dispatch.
    pub(crate) fn supports_generic_handler_graph(&self) -> bool {
        !self.compiled.graph.nodes.is_empty()
            && self.compiled.graph.nodes.iter().all(|node| {
                self.cursor(node.index).is_ok_and(|cursor| {
                    supported_generic_resolution(
                        node,
                        &cursor.resolution,
                        &self.compiled.allowed_plugins,
                    )
                })
            })
    }

    /// Selects dispatch exclusively from the immutable execution-plan
    /// generation and its exact persisted executor resolutions.
    ///
    /// Generation three never consults style identity, node names, graph
    /// topology, variable shape, or compatibility adapter classification.
    /// Generation two retains its frozen adapter selection for historical
    /// recovery.
    pub(crate) fn execution_dispatch_mode(
        &self,
    ) -> Result<ExecutionDispatchMode, StyleExecutorError> {
        let generation = self
            .plan_generation
            .ok_or(StyleExecutorError::MissingExecutionPlan)?;
        if generation == ExecutionPlanCompilerGeneration::V3 {
            if !self.supports_generic_handler_graph() {
                return Err(StyleExecutorError::UnsupportedGenericExecutionPlan);
            }
            return Ok(ExecutionDispatchMode::Generic);
        }
        let Some(adapter) = self.adapter_kind() else {
            return Ok(ExecutionDispatchMode::Generic);
        };
        Ok(ExecutionDispatchMode::Legacy(adapter))
    }

    /// Loads and identity-checks the retained compiled descriptor without
    /// selecting node implementations.
    fn load_compiled(
        binding: &SessionStyleBinding,
    ) -> Result<CompiledSessionStyle, StyleExecutorError> {
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
        Ok(compiled)
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
        self.cursor(node.index)
    }

    /// Selects the one eligible outgoing transition in compiled order.
    ///
    /// An unconditional edge is eligible. A conditional edge is eligible only
    /// when its already-parsed expression evaluates true against `variables`.
    /// Multiple eligible edges are rejected instead of depending on incidental
    /// source order.
    pub(crate) fn transition(
        &self,
        from_index: usize,
        variables: &Value,
    ) -> Result<Option<StyleTransition>, StyleExecutorError> {
        let from = self.cursor(from_index)?;
        let mut eligible = self
            .compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == from_index)
            .filter_map(|edge| {
                let eligible = edge
                    .condition
                    .as_ref()
                    .map_or(Ok(true), |condition| condition.evaluate(variables));
                match eligible {
                    Ok(true) => Some(Ok(edge)),
                    Ok(false) => None,
                    Err(_) => Some(Err(StyleExecutorError::ConditionEvaluation {
                        node: from.id.clone(),
                    })),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if eligible.len() > 1 {
            return Err(StyleExecutorError::AmbiguousTransition {
                node: from.id.clone(),
            });
        }
        let Some(edge) = eligible.pop() else {
            return if matches!(
                from.directive,
                StyleNodeDirective::CompleteTurn
                    | StyleNodeDirective::CompleteSession
                    | StyleNodeDirective::Fail
            ) {
                Ok(None)
            } else {
                Err(StyleExecutorError::MissingTransition {
                    node: from.id.clone(),
                })
            };
        };
        Ok(Some(StyleTransition {
            from,
            to: self.cursor(edge.to)?,
            label: edge.label.clone(),
        }))
    }

    /// Selects an exact compiled outgoing destination supplied by a
    /// runtime-validated node outcome such as a review disposition.
    pub(crate) fn transition_to(
        &self,
        from_index: usize,
        destination_node_id: &str,
    ) -> Result<StyleTransition, StyleExecutorError> {
        let from = self.cursor(from_index)?;
        let mut matching = self
            .compiled
            .graph
            .edges
            .iter()
            .filter(|edge| edge.from == from_index)
            .filter(|edge| {
                self.compiled
                    .graph
                    .nodes
                    .get(edge.to)
                    .is_some_and(|node| node.id == destination_node_id)
            });
        let edge = matching
            .next()
            .ok_or_else(|| StyleExecutorError::MissingTransition {
                node: from.id.clone(),
            })?;
        if matching.next().is_some() {
            return Err(StyleExecutorError::AmbiguousTransition {
                node: from.id.clone(),
            });
        }
        Ok(StyleTransition {
            from,
            to: self.cursor(edge.to)?,
            label: edge.label.clone(),
        })
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
    pub(crate) fn adapter_kind(&self) -> Option<StyleAdapterKind> {
        if self.resolutions.iter().any(|resolution| {
            matches!(
                (&resolution.source, resolution.boundary),
                (
                    SessionNodeExecutorSource::Plugin { .. },
                    SessionNodeExecutorBoundary::PluginHost
                )
            )
        }) {
            return None;
        }
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
        let typed_fresh_contract = matches!(
            entry.configuration.as_ref(),
            Some(NodeConfiguration::ContextTransform {
                strategy: agentmod_graph_engine::ContextTransformStrategy::Fresh
            })
        ) && matches!(
            to_model.to.configuration.as_ref(),
            Some(NodeConfiguration::ModelRequest { .. })
        ) && matches!(
            to_tools.to.configuration.as_ref(),
            Some(NodeConfiguration::ProviderToolBatchExecution { .. })
        ) && matches!(
            to_complete.to.configuration.as_ref(),
            Some(NodeConfiguration::CompleteTurn {
                cleanup: agentmod_graph_engine::CompleteTurnCleanup::DiscardProjection,
                ..
            })
        );
        let frozen_legacy_contract = matches!(
            entry.configuration.as_ref(),
            Some(NodeConfiguration::ContextTransform {
                strategy: agentmod_graph_engine::ContextTransformStrategy::PreserveHistory
            })
        ) && to_model.to.configuration.is_none()
            && matches!(
                to_tools.to.configuration.as_ref(),
                Some(NodeConfiguration::ToolExecution { .. })
            )
            && to_complete.to.configuration.is_none();
        (typed_fresh_contract || frozen_legacy_contract)
            && to_complete.to.directive == StyleNodeDirective::CompleteTurn
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
            .map(|node| self.resolved_cursor(node))
            .transpose()?
            .ok_or(StyleExecutorError::InvalidNodeIndex(index))
    }

    fn resolved_cursor(
        &self,
        node: &ExecutableNode,
    ) -> Result<StyleNodeCursor, StyleExecutorError> {
        let mut matches = self
            .resolutions
            .iter()
            .filter(|resolution| resolution.node_id == node.id);
        let resolution = matches
            .next()
            .ok_or(StyleExecutorError::MissingExecutionPlan)?;
        if matches.next().is_some() {
            return Err(StyleExecutorError::ExecutionPlanMismatch);
        }
        validate_resolution(node, resolution)?;
        cursor(node, resolution.clone())
    }
}

fn directive(
    resolution: &SessionNodeExecutorResolution,
) -> Result<StyleNodeDirective, StyleExecutorError> {
    if let Ok(key) = native_executor_key(resolution) {
        return Ok(match key {
            NativeExecutorKey::ContextConstruction => StyleNodeDirective::ContextTransform,
            NativeExecutorKey::ModelRequest => StyleNodeDirective::ModelCall,
            NativeExecutorKey::ToolGate => StyleNodeDirective::ToolExecutionGate,
            NativeExecutorKey::UserApproval => StyleNodeDirective::UserApproval,
            NativeExecutorKey::ChildSpawn => StyleNodeDirective::SpawnChildAgent,
            NativeExecutorKey::ChildMessage => StyleNodeDirective::SendChildAgentMessage,
            NativeExecutorKey::ChildWait => StyleNodeDirective::WaitForAgents,
            NativeExecutorKey::Join => StyleNodeDirective::JoinResults,
            NativeExecutorKey::Review => StyleNodeDirective::Review,
            NativeExecutorKey::Loop => StyleNodeDirective::Loop,
            NativeExecutorKey::Conditional => StyleNodeDirective::ConditionalBranch,
            NativeExecutorKey::Parallel => StyleNodeDirective::ParallelBranch,
            NativeExecutorKey::Delay => StyleNodeDirective::Delay,
            NativeExecutorKey::Schedule => StyleNodeDirective::Schedule,
            NativeExecutorKey::EventEmission => StyleNodeDirective::EmitEvent,
            NativeExecutorKey::ArtifactPersistence => StyleNodeDirective::PersistArtifact,
            NativeExecutorKey::TurnCompletion => StyleNodeDirective::CompleteTurn,
            NativeExecutorKey::SessionCompletion => StyleNodeDirective::CompleteSession,
            NativeExecutorKey::StructuredFailure => StyleNodeDirective::Fail,
        });
    }
    if !matches!(
        (
            &resolution.source,
            resolution.boundary,
            resolution.executor_declaration_hash
        ),
        (
            SessionNodeExecutorSource::Plugin { plugin_id },
            SessionNodeExecutorBoundary::PluginHost,
            declaration_hash
        ) if !plugin_id.trim().is_empty()
            && declaration_hash != ContentHash::from_bytes([0; 32])
    ) {
        return Err(StyleExecutorError::UnsupportedExecutorIdentity {
            node: resolution.node_id.clone(),
        });
    }
    directive_for_serialized_kind(&resolution.node_kind).ok_or_else(|| {
        StyleExecutorError::UnsupportedExecutorIdentity {
            node: resolution.node_id.clone(),
        }
    })
}

fn supported_generic_resolution(
    node: &ExecutableNode,
    resolution: &SessionNodeExecutorResolution,
    allowed_plugins: &[String],
) -> bool {
    if matches!(
        native_executor_key(resolution),
        Ok(NativeExecutorKey::ContextConstruction
            | NativeExecutorKey::ModelRequest
            | NativeExecutorKey::ToolGate
            | NativeExecutorKey::UserApproval
            | NativeExecutorKey::ArtifactPersistence
            | NativeExecutorKey::Conditional
            | NativeExecutorKey::Loop
            | NativeExecutorKey::ChildSpawn
            | NativeExecutorKey::ChildWait
            | NativeExecutorKey::Review
            | NativeExecutorKey::ChildMessage
            | NativeExecutorKey::EventEmission
            | NativeExecutorKey::Delay
            | NativeExecutorKey::Schedule
            | NativeExecutorKey::Parallel
            | NativeExecutorKey::Join
            | NativeExecutorKey::TurnCompletion
            | NativeExecutorKey::SessionCompletion
            | NativeExecutorKey::StructuredFailure)
    ) {
        return true;
    }
    let plugin_resolution = matches!(
        (&resolution.source, resolution.boundary),
        (
            SessionNodeExecutorSource::Plugin { plugin_id },
            SessionNodeExecutorBoundary::PluginHost
        ) if !plugin_id.trim().is_empty()
            && !resolution.executor_id.trim().is_empty()
            && !resolution.executor_version.trim().is_empty()
            && !resolution.runtime_api_requirement.trim().is_empty()
            && resolution.executor_declaration_hash != ContentHash::from_bytes([0; 32])
            && resolution
                .required_capabilities
                .iter()
                .all(|required| resolution.resolved_capabilities.contains(required))
            && directive_for_serialized_kind(&resolution.node_kind).is_some()
    );
    if !plugin_resolution {
        return false;
    }
    let SessionNodeExecutorSource::Plugin { plugin_id } = &resolution.source else {
        return false;
    };
    matches!(
        node.configuration.as_ref(),
        Some(NodeConfiguration::Plugin {
            plugin_id: configured_plugin,
            executor_id,
            executor_version,
            node_kind,
            input_schema,
            output_schema,
            configuration_reference,
            ..
        }) if configured_plugin == plugin_id
            && allowed_plugins.contains(plugin_id)
            && executor_id == &resolution.executor_id
            && executor_version == &resolution.executor_version
            && serde_json::to_value(node_kind)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .as_deref()
                == Some(resolution.node_kind.as_str())
            && !input_schema.trim().is_empty()
            && !output_schema.trim().is_empty()
            && !configuration_reference.trim().is_empty()
    )
}

fn directive_for_serialized_kind(kind: &str) -> Option<StyleNodeDirective> {
    Some(match kind {
        "context_transform" => StyleNodeDirective::ContextTransform,
        "model_call" => StyleNodeDirective::ModelCall,
        "tool_execution_gate" => StyleNodeDirective::ToolExecutionGate,
        "user_approval" => StyleNodeDirective::UserApproval,
        "spawn_child_agent" => StyleNodeDirective::SpawnChildAgent,
        "send_child_agent_message" => StyleNodeDirective::SendChildAgentMessage,
        "wait_for_agents" => StyleNodeDirective::WaitForAgents,
        "join_results" => StyleNodeDirective::JoinResults,
        "review" => StyleNodeDirective::Review,
        "loop" => StyleNodeDirective::Loop,
        "conditional_branch" => StyleNodeDirective::ConditionalBranch,
        "parallel_branch" => StyleNodeDirective::ParallelBranch,
        "delay" => StyleNodeDirective::Delay,
        "schedule" => StyleNodeDirective::Schedule,
        "emit_event" => StyleNodeDirective::EmitEvent,
        "persist_artifact" => StyleNodeDirective::PersistArtifact,
        "complete_turn" => StyleNodeDirective::CompleteTurn,
        "complete_session" => StyleNodeDirective::CompleteSession,
        "fail" => StyleNodeDirective::Fail,
        _ => return None,
    })
}

fn cursor(
    node: &ExecutableNode,
    resolution: SessionNodeExecutorResolution,
) -> Result<StyleNodeCursor, StyleExecutorError> {
    Ok(StyleNodeCursor {
        index: node.index,
        id: node.id.clone(),
        directive: directive(&resolution)?,
        resolution,
        configuration: node.configuration.clone(),
        retry_limit: node.retry_limit,
        max_iterations: node.max_iterations,
        tool: node.tool.clone(),
        provider: node.provider.clone(),
    })
}

fn validate_resolution(
    node: &ExecutableNode,
    resolution: &SessionNodeExecutorResolution,
) -> Result<(), StyleExecutorError> {
    let serialized_kind = serde_json::to_value(node.kind)
        .ok()
        .and_then(|kind| kind.as_str().map(str::to_owned))
        .ok_or(StyleExecutorError::InvalidNodeKind)?;
    let configuration_reference = ContentHash::digest(
        &serde_json::to_vec(node).map_err(|_| StyleExecutorError::InvalidNodeConfiguration)?,
    );
    if resolution.node_id != node.id
        || resolution.node_kind != serialized_kind
        || resolution.adapter_configuration_reference != configuration_reference
    {
        return Err(StyleExecutorError::NodeResolutionMismatch {
            node: node.id.clone(),
        });
    }
    directive(resolution).map(|_| ())
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
    #[error("the retained style has no immutable execution plan")]
    MissingExecutionPlan,
    #[error("the retained execution-plan compiler generation is unsupported")]
    UnsupportedExecutionPlanCompiler,
    #[error("the retained execution plan could not be serialized")]
    InvalidExecutionPlan,
    #[error("the retained execution plan hash does not match its contents")]
    ExecutionPlanHashMismatch,
    #[error("the retained execution plan does not exactly cover the compiled graph")]
    ExecutionPlanMismatch,
    #[error("compiled graph node kind could not be serialized")]
    InvalidNodeKind,
    #[error("compiled graph node configuration could not be serialized")]
    InvalidNodeConfiguration,
    #[error("persisted executor resolution does not match compiled node `{node}`")]
    NodeResolutionMismatch { node: String },
    #[error("persisted executor identity for node `{node}` has no runtime handler")]
    UnsupportedExecutorIdentity { node: String },
    #[error("generation-three execution plan contains an unsupported generic node handler")]
    UnsupportedGenericExecutionPlan,
    #[error("condition evaluation failed at graph node `{node}`")]
    ConditionEvaluation { node: String },
    #[error("graph node `{node}` has more than one eligible transition")]
    AmbiguousTransition { node: String },
    #[error("nonterminal graph node `{node}` has no eligible transition")]
    MissingTransition { node: String },
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use agentmod_primitives::ContentHash;
    use agentmod_session_style_sdk::{
        BuiltInStyle, CompileContext, DecisionCapability, SessionStyleManifest,
        StyleCompilerLimits, built_in_manifest, built_in_manifest_for_version, compile_style,
        to_json,
    };
    use serde_json::json;

    use crate::{
        session::{
            SessionCompactionConfiguration, SessionMemoryConfiguration, SessionPermissionDefaults,
            SessionStyleBinding, SessionStyleBudgets, SessionStyleSource,
        },
        style_executor::{
            CompiledStyleExecutor, ExecutionDispatchMode, StyleAdapterKind, StyleExecutorError,
            StyleNodeDirective,
        },
    };

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture explicitly binds every immutable session-style selection"
    )]
    pub(crate) fn binding(style: BuiltInStyle) -> SessionStyleBinding {
        let manifest = built_in_manifest(style);
        binding_from_manifest(&manifest)
    }

    pub(crate) fn binding_for_version(style: BuiltInStyle, version: &str) -> SessionStyleBinding {
        let manifest =
            built_in_manifest_for_version(style, version).expect("built-in style version");
        binding_from_manifest(&manifest)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture explicitly binds every immutable session-style selection"
    )]
    fn binding_from_manifest(manifest: &SessionStyleManifest) -> SessionStyleBinding {
        let manifest_json = to_json(manifest).expect("manifest json");
        let plugin_set_hash = ContentHash::digest(b"plugins");
        let context = CompileContext {
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
            tool_groups: crate::tool::canonical_tool_groups(),
            providers: BTreeSet::from([String::from("deterministic-mock"), String::from("mock")]),
            plugins: BTreeSet::from([String::from("runtime.security")]),
            context_transforms: Vec::new(),
            plugin_memory_providers: Vec::new(),
            plugin_compactors: Vec::new(),
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
        };
        let compiled =
            compile_style(manifest, &context, StyleCompilerLimits::default()).expect("compile");
        let compiled_json = serde_json::to_string(&compiled).expect("compiled json");
        let mut binding = SessionStyleBinding {
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
            execution_plan: None,
            execution_plan_hash: None,
            mcp: crate::session::SessionMcpBinding::default(),
            memory: SessionMemoryConfiguration {
                provider: String::from("fixture"),
                plugin: None,
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
                plugin: None,
                trigger_tokens: None,
                reserved_context_tokens: 0,
                max_provider_projection_tokens: 0,
                preserve_unresolved_tasks: true,
                preserve_active_processes: true,
                preservation_requirements: Vec::new(),
                summary: None,
                summary_max_bytes: 64 * 1024,
                summary_schema_version: 1,
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
        };
        crate::node_executor::bind_runtime_execution_plan(
            &agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("native node executor registry"),
            &mut binding,
        )
        .expect("bind test execution plan");
        binding
    }

    pub(crate) fn set_execution_plan_compiler(binding: &mut SessionStyleBinding, compiler: &str) {
        binding
            .execution_plan
            .as_mut()
            .expect("execution plan")
            .compilation
            .compiler = compiler.to_owned();
        binding.execution_plan_hash = Some(ContentHash::digest(
            &serde_json::to_vec(binding.execution_plan.as_ref().expect("execution plan"))
                .expect("execution plan json"),
        ));
    }

    fn generation_two_binding_for_version(
        style: BuiltInStyle,
        version: &str,
    ) -> SessionStyleBinding {
        let mut binding = binding_for_version(style, version);
        set_execution_plan_compiler(&mut binding, crate::session::EXECUTION_PLAN_COMPILER_V2);
        binding
    }

    #[test]
    fn persistent_chat_current_is_generic_and_exact_legacy_remains_adapter_backed() {
        let current = CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::PersistentChat))
            .expect("current executor");
        assert!(!current.supports_persistent_turn());
        assert_eq!(current.adapter_kind(), None);
        assert_eq!(
            current.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );
        let context = current.entry().expect("context entry");
        assert_eq!(context.id, "prepare-context");
        assert_eq!(context.directive, StyleNodeDirective::ContextTransform);
        let respond = current
            .transition(context.index, &json!({}))
            .expect("context transition")
            .expect("respond")
            .to;
        assert_eq!(respond.id, "respond");
        assert_eq!(respond.directive, StyleNodeDirective::ModelCall);
        let tool = current
            .transition(respond.index, &json!({}))
            .expect("transition")
            .expect("tool");
        assert_eq!(tool.to.id, "tool-batch");
        assert_eq!(tool.to.directive, StyleNodeDirective::ToolExecutionGate);
        let done = current
            .transition(tool.to.index, &json!({}))
            .expect("transition")
            .expect("done");
        assert_eq!(done.to.directive, StyleNodeDirective::CompleteTurn);
        assert_eq!(current.transition(done.to.index, &json!({})), Ok(None));

        let legacy = CompiledStyleExecutor::from_binding(&generation_two_binding_for_version(
            BuiltInStyle::PersistentChat,
            "1.1.0",
        ))
        .expect("legacy executor");
        assert!(legacy.supports_persistent_turn());
        assert_eq!(
            legacy.adapter_kind(),
            Some(StyleAdapterKind::PersistentTurn)
        );
        assert_eq!(
            legacy.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::PersistentTurn
            ))
        );
    }

    #[test]
    fn ephemeral_turn_selects_the_fresh_context_adapter_from_compiled_semantics() {
        let current = binding(BuiltInStyle::EphemeralTurn);
        let executor = CompiledStyleExecutor::from_binding(&current).expect("executor");
        assert_eq!(
            executor.adapter_kind(),
            Some(super::StyleAdapterKind::EphemeralTurn)
        );
        assert_eq!(
            executor.entry().expect("entry").directive,
            StyleNodeDirective::ContextTransform
        );
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );

        let legacy = CompiledStyleExecutor::from_binding(&generation_two_binding_for_version(
            BuiltInStyle::EphemeralTurn,
            "1.1.0",
        ))
        .expect("legacy executor");
        assert_eq!(
            legacy.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::EphemeralTurn
            ))
        );

        let mut generation_two = current;
        generation_two
            .execution_plan
            .as_mut()
            .expect("plan")
            .compilation
            .compiler = String::from(crate::session::EXECUTION_PLAN_COMPILER_V2);
        generation_two.execution_plan_hash = Some(ContentHash::digest(
            &serde_json::to_vec(generation_two.execution_plan.as_ref().expect("plan"))
                .expect("plan json"),
        ));
        assert_eq!(
            CompiledStyleExecutor::from_binding(&generation_two)
                .expect("generation two executor")
                .execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::EphemeralTurn
            ))
        );
    }

    #[test]
    fn research_loop_uses_compiled_conditions_and_rejects_missing_variables() {
        let executor = CompiledStyleExecutor::from_binding(&generation_two_binding_for_version(
            BuiltInStyle::ResearchLoop,
            "1.1.0",
        ))
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
    fn typed_research_graph_uses_generic_dispatch_without_topology_authority() {
        let executor = CompiledStyleExecutor::from_binding(&binding(BuiltInStyle::ResearchLoop))
            .expect("current research executor");
        assert!(executor.supports_generic_handler_graph());
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );
        let repeat = executor.node("repeat").expect("loop");
        let continued = executor
            .transition(repeat.index, &json!({"iteration":{"remaining":true}}))
            .expect("continue transition")
            .expect("continue destination");
        assert_eq!(continued.to.id, "fresh-context");
        let completed = executor
            .transition(repeat.index, &json!({"iteration":{"remaining":false}}))
            .expect("complete transition")
            .expect("complete destination");
        assert_eq!(completed.to.directive, StyleNodeDirective::CompleteSession);

        let legacy = CompiledStyleExecutor::from_binding(&generation_two_binding_for_version(
            BuiltInStyle::ResearchLoop,
            "1.1.0",
        ))
        .expect("legacy research executor");
        assert_eq!(
            legacy.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::ResearchLoop
            ))
        );
    }

    #[test]
    fn planner_worker_reviewer_uses_the_compiled_child_and_review_loop() {
        let executor = CompiledStyleExecutor::from_binding(&generation_two_binding_for_version(
            BuiltInStyle::PlannerWorker,
            "1.1.0",
        ))
        .expect("historical executor");
        assert_eq!(
            executor.adapter_kind(),
            Some(StyleAdapterKind::PlannerWorkerReviewer)
        );
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::PlannerWorkerReviewer
            ))
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

    #[test]
    fn dispatch_mode_migrates_only_typed_declarative_generation_three() {
        let current = binding(BuiltInStyle::DeclarativeGraph);
        let executor = CompiledStyleExecutor::from_binding(&current).expect("current executor");
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );

        let mut generation_two = current;
        generation_two
            .execution_plan
            .as_mut()
            .expect("plan")
            .compilation
            .compiler = String::from(crate::session::EXECUTION_PLAN_COMPILER_V2);
        generation_two.execution_plan_hash = Some(ContentHash::digest(
            &serde_json::to_vec(generation_two.execution_plan.as_ref().expect("plan"))
                .expect("plan json"),
        ));
        assert_eq!(
            CompiledStyleExecutor::from_binding(&generation_two)
                .expect("generation two executor")
                .execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::DeclarativeGraph
            ))
        );

        let legacy_manifest =
            built_in_manifest_for_version(BuiltInStyle::DeclarativeGraph, "1.1.0")
                .expect("legacy manifest");
        let mut legacy = binding_from_manifest(&legacy_manifest);
        set_execution_plan_compiler(&mut legacy, crate::session::EXECUTION_PLAN_COMPILER_V2);
        assert_eq!(
            CompiledStyleExecutor::from_binding(&legacy)
                .expect("legacy executor")
                .execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::DeclarativeGraph
            ))
        );
    }

    #[test]
    fn planner_worker_current_is_generic_and_exact_historical_version_is_legacy() {
        let current = CompiledStyleExecutor::from_binding(&binding_for_version(
            BuiltInStyle::PlannerWorker,
            "1.2.0",
        ))
        .expect("current executor");
        assert_eq!(current.adapter_kind(), None);
        assert_eq!(
            current.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );

        let historical = CompiledStyleExecutor::from_binding(&generation_two_binding_for_version(
            BuiltInStyle::PlannerWorker,
            "1.1.0",
        ))
        .expect("historical executor");
        assert_eq!(
            historical.adapter_kind(),
            Some(StyleAdapterKind::PlannerWorkerReviewer)
        );
        assert_eq!(
            historical.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::PlannerWorkerReviewer
            ))
        );
    }

    #[test]
    fn typed_generation_three_dispatch_does_not_require_adapter_classification() {
        let mut candidate = binding(BuiltInStyle::DeclarativeGraph);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&candidate.compiled_style_json).expect("compiled style");
        let mut additional_terminal = compiled
            .graph
            .nodes
            .iter()
            .find(|node| node.kind == agentmod_graph_engine::NodeKind::CompleteSession)
            .expect("terminal")
            .clone();
        additional_terminal.id = String::from("additional-terminal");
        additional_terminal.index = compiled.graph.nodes.len();
        compiled.graph.nodes.push(additional_terminal);
        candidate.compiled_style_json = serde_json::to_string(&compiled).expect("compiled json");
        candidate.compiled_style_hash =
            ContentHash::digest(candidate.compiled_style_json.as_bytes());
        candidate.execution_plan = None;
        candidate.execution_plan_hash = None;
        crate::node_executor::bind_runtime_execution_plan(
            &agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
                .expect("registry"),
            &mut candidate,
        )
        .expect("bind exact expanded plan");

        let executor = CompiledStyleExecutor::from_binding(&candidate).expect("executor");
        assert_eq!(executor.adapter_kind(), None);
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );
    }

    #[test]
    fn generation_three_recognized_legacy_shape_without_variables_is_generic() {
        let mut candidate = binding_for_version(BuiltInStyle::PersistentChat, "1.1.0");
        set_execution_plan_compiler(&mut candidate, crate::session::EXECUTION_PLAN_COMPILER_V3);

        let executor = CompiledStyleExecutor::from_binding(&candidate).expect("executor");
        assert!(executor.compiled.graph.variables.is_empty());
        assert_eq!(
            executor.adapter_kind(),
            Some(StyleAdapterKind::PersistentTurn)
        );
        assert!(executor.supports_generic_handler_graph());
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Generic)
        );
    }

    #[test]
    fn generation_three_unsupported_exact_handler_fails_closed() {
        let mut candidate = binding_for_version(BuiltInStyle::PersistentChat, "1.1.0");
        let resolution = candidate
            .execution_plan
            .as_mut()
            .expect("execution plan")
            .nodes
            .first_mut()
            .expect("resolution");
        resolution.source = crate::session::SessionNodeExecutorSource::Plugin {
            plugin_id: String::from("fixture.unsupported"),
        };
        resolution.boundary = crate::session::SessionNodeExecutorBoundary::PluginHost;
        resolution.executor_declaration_hash = ContentHash::digest(b"tampered declaration");
        set_execution_plan_compiler(&mut candidate, crate::session::EXECUTION_PLAN_COMPILER_V3);

        let executor = CompiledStyleExecutor::from_binding(&candidate).expect("executor");
        assert!(!executor.supports_generic_handler_graph());
        assert_eq!(
            executor.execution_dispatch_mode(),
            Err(StyleExecutorError::UnsupportedGenericExecutionPlan)
        );
    }

    #[test]
    fn generation_two_recognized_adapter_remains_legacy() {
        let candidate = generation_two_binding_for_version(BuiltInStyle::PersistentChat, "1.1.0");
        assert_eq!(
            candidate
                .execution_plan
                .as_ref()
                .expect("execution plan")
                .compilation
                .compiler,
            crate::session::EXECUTION_PLAN_COMPILER_V2
        );
        let executor = CompiledStyleExecutor::from_binding(&candidate).expect("executor");
        assert_eq!(
            executor.execution_dispatch_mode(),
            Ok(ExecutionDispatchMode::Legacy(
                StyleAdapterKind::PersistentTurn
            ))
        );
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
