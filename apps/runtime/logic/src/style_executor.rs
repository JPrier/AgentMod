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

use crate::session::SessionStyleBinding;

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
        if self.supports_persistent_turn() {
            return Some(StyleAdapterKind::PersistentTurn);
        }
        if self.supports_ephemeral_turn() {
            return Some(StyleAdapterKind::EphemeralTurn);
        }
        if self.supports_research_loop() {
            return Some(StyleAdapterKind::ResearchLoop);
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
        };
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
    fn compiled_descriptor_tampering_fails_before_execution() {
        let mut binding = binding(BuiltInStyle::PersistentChat);
        binding.compiled_style_json.push(' ');
        assert_eq!(
            CompiledStyleExecutor::from_binding(&binding).expect_err("tampering"),
            StyleExecutorError::CompiledHashMismatch
        );
    }
}
