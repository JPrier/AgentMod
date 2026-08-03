//! Replay-derived session-style and graph inspection.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use agentmod_expression_engine::{Expression, Operand, PathSegment};
use agentmod_graph_engine::{ExecutableEdge, ExecutableGraph};
use agentmod_session_style_sdk::CompiledSessionStyle;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    canonical_variables::{CanonicalVariableEventReducer, ConditionEligibility, VariableReader},
    conversation::ConversationEntry,
    session::{
        PluginContextOperationKind, SessionState, SessionStyleBinding, StyleExecutionControlState,
        StyleExecutionState,
    },
};

const MAX_CONDITION_DIAGNOSTIC_BYTES: usize = 1_024;

/// Logic-owned style introspection result.
#[derive(Clone, Debug, PartialEq)]
pub struct StyleIntrospectionResult {
    /// Stable structured inspection projection.
    pub value: Value,
}

/// Builds a bounded, replay-only orchestration projection.
///
/// This function never invokes a provider, tool, plugin, memory backend, or
/// graph node. Every dynamic value is derived from the supplied canonical
/// session state.
///
/// # Errors
///
/// Returns [`StyleIntrospectionError`] when the retained compiled style cannot
/// be decoded or does not match the immutable binding.
pub fn inspect_style_execution(
    state: &SessionState,
) -> Result<Option<StyleIntrospectionResult>, StyleIntrospectionError> {
    let Some(binding) = state.style_binding.as_ref() else {
        return Ok(None);
    };
    let compiled: CompiledSessionStyle = serde_json::from_str(&binding.compiled_style_json)
        .map_err(|_| StyleIntrospectionError::InvalidCompiledStyle)?;
    if compiled.style_id != binding.id
        || compiled.style_version != binding.version
        || compiled.cache_key.combined_hash != binding.compiled_cache_key
    {
        return Err(StyleIntrospectionError::CompiledIdentityMismatch);
    }

    let execution = state.style_execution.as_ref();
    let graph = execution.map_or(&compiled.graph, |value| value.graph.as_ref());
    let graph_inspection = graph_inspection(graph, execution)?;
    let remaining_budgets = remaining_budgets(binding, execution);
    let memory = memory_inspection(state, binding);
    let compaction = compaction_inspection(state, binding, execution);
    let child_agents = child_inspection(state, binding)?;
    let configuration = configuration_inspection(binding)?;

    Ok(Some(StyleIntrospectionResult {
        value: json!({
            "style": style_identity(binding),
            "harness": harness_identity(binding),
            "configuration": configuration,
            "graph": graph_inspection,
            "remaining_budgets": remaining_budgets,
            "pipeline": {
                "blocking_interceptor_order": binding.interceptor_order,
                "activated_plugin_ids": state.plugins.activated_plugin_ids,
                "plugin_lifecycle": state.plugins.lifecycle,
                "blocking_invocations": state.plugins.invocations,
            },
            "memory": memory,
            "compaction": compaction,
            "child_agents": child_agents,
            "termination_reason": execution.and_then(|value| value.termination_reason.clone()),
        }),
    }))
}

fn configuration_inspection(
    binding: &SessionStyleBinding,
) -> Result<Value, StyleIntrospectionError> {
    let retry = serde_json::from_str::<Value>(&binding.retry_policy_json)
        .map_err(|_| StyleIntrospectionError::InvalidCompiledStyle)?;
    let termination = serde_json::from_str::<Value>(&binding.termination_policy_json)
        .map_err(|_| StyleIntrospectionError::InvalidCompiledStyle)?;
    Ok(json!({
        "tool_groups": binding.tool_groups,
        "required_capabilities": binding.required_capabilities,
        "permission_defaults": binding.permission_defaults,
        "budgets": binding.budgets,
        "retry_policy": retry,
        "termination_policy": termination,
    }))
}

fn style_identity(binding: &SessionStyleBinding) -> Value {
    json!({
        "id": binding.id,
        "version": binding.version,
        "content_hash": binding.content_hash,
        "compiled_style_hash": binding.compiled_style_hash,
        "compiled_cache_key": binding.compiled_cache_key,
        "source": binding.source,
        "source_locator": binding.source_locator,
        "runtime_api_version": binding.runtime_api_version,
        "plugin_set_hash": binding.plugin_set_hash,
        "capability_set_hash": binding.capability_set_hash,
    })
}

fn harness_identity(binding: &SessionStyleBinding) -> Value {
    json!({
        "id": binding.harness,
        "version": binding.harness_version,
        "capability_set_hash": binding.harness_capability_set_hash,
        "required_capabilities": binding.harness_required_capabilities,
    })
}

fn graph_inspection(
    graph: &ExecutableGraph,
    execution: Option<&StyleExecutionState>,
) -> Result<Value, StyleIntrospectionError> {
    let canonical_variables = execution.and_then(|value| value.canonical_variables.as_deref());
    if let Some(variables) = canonical_variables {
        variables
            .validate_replayed()
            .map_err(|_| StyleIntrospectionError::InvalidCanonicalVariableState)?;
    }
    let active = execution.and_then(|value| value.active_node.as_ref());
    let current_node_id = active
        .map(|node| node.node_id.as_str())
        .or_else(|| control_node_id(execution.map(|value| &value.control)));
    let candidates = current_node_id
        .map(|node_id_value| transition_candidates(graph, node_id_value, canonical_variables))
        .transpose()?
        .unwrap_or_default();
    let known_eligible = execution
        .map(|value| known_eligible_transitions(graph, &value.control, canonical_variables))
        .transpose()?
        .unwrap_or_default();
    let variables = canonical_variables
        .map(canonical_variable_inspection)
        .transpose()?
        .unwrap_or(Value::Null);
    let nodes = graph
        .nodes
        .iter()
        .map(|node| {
            json!({
                "id": node.id,
                "kind": node.kind,
                "retry_limit": node.retry_limit,
                "max_iterations": node.max_iterations,
                "tool": node.tool,
                "provider": node.provider,
                "required_capabilities": node.required_capabilities,
            })
        })
        .collect::<Vec<_>>();
    let transitions = graph
        .edges
        .iter()
        .map(|edge| edge_json(graph, edge))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "cache_key": graph.cache_key,
        "entry_node": node_id(graph, graph.entry_index)?,
        "control": execution.map(|value| &value.control),
        "active_node": active,
        "nodes": nodes,
        "transitions": transitions,
        "completed_nodes": execution.map(|value| value.completed_nodes.clone()).unwrap_or_default(),
        "failed_nodes": execution.map(|value| value.failed_nodes.clone()).unwrap_or_default(),
        "previous_transitions": execution.map(|value| value.transitions.clone()).unwrap_or_default(),
        "next_transition_candidates": candidates,
        "next_eligible_transitions": known_eligible,
        "variables": variables,
        "loop_count": execution.map_or(0, maximum_loop_count),
        "retry_count": execution.map_or(0, retry_count),
    }))
}

fn remaining_budgets(
    binding: &SessionStyleBinding,
    execution: Option<&StyleExecutionState>,
) -> Value {
    let used_steps = execution.map_or(0, maximum_step);
    let used_tokens = execution.map_or(0, |value| {
        value.input_tokens.saturating_add(value.output_tokens)
    });
    let loop_count = execution.map_or(0, maximum_loop_count);
    json!({
        "steps": binding.budgets.max_steps.saturating_sub(used_steps),
        "tokens": binding.budgets.max_tokens.saturating_sub(used_tokens),
        "iterations": u64::from(binding.budgets.max_iterations)
            .saturating_sub(u64::from(loop_count)),
        "cost_micros": Value::Null,
        "duration_ms": Value::Null,
        "accounting": {
            "steps_used": used_steps,
            "tokens_used": used_tokens,
            "cost": "not_reported",
            "duration": "not_reconstructed",
        },
    })
}

fn memory_inspection(state: &SessionState, binding: &SessionStyleBinding) -> Value {
    let provenance = state
        .conversation
        .provider_projection()
        .iter()
        .filter_map(|entry| match entry {
            ConversationEntry::RetrievedMemory(memory) => Some(json!({
                "provider": memory.provider,
                "scope": memory.scope,
                "source": memory.source,
                "reference": memory.reference,
                "query": memory.query,
                "injection_sequence": memory.injection_sequence,
                "injection_event": memory.injection_event,
                "size_bytes": memory.size_bytes,
                "typed_provenance": memory.typed_provenance,
            })),
            _ => None,
        })
        .collect::<Vec<_>>();
    json!({
        "selection": binding.memory,
        "retrieved_provenance": provenance,
        "plugin_operations": state.plugin_context_operations.values()
            .filter(|record| record.identity.kind == PluginContextOperationKind::MemoryRetrieve)
            .collect::<Vec<_>>(),
    })
}

fn compaction_inspection(
    state: &SessionState,
    binding: &SessionStyleBinding,
    execution: Option<&StyleExecutionState>,
) -> Value {
    let history = execution
        .map(|value| {
            value
                .context_boundaries
                .iter()
                .map(|boundary| {
                    json!({
                        "identity": boundary.identity,
                        "started_phases": boundary.started_phases,
                        "completed_phases": boundary.completed_phases,
                        "last_sequence": boundary.last_sequence,
                        "completed_at": boundary.completed_at,
                        "estimated_tokens": boundary.estimated_tokens,
                        "serialized_bytes": boundary.serialized_bytes,
                        "phase_replacement_event": boundary.phase_replacement_event,
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "selection": binding.compaction,
        "history": history,
        "plugin_operations": state.plugin_context_operations.values()
            .filter(|record| record.identity.kind == PluginContextOperationKind::Compaction)
            .collect::<Vec<_>>(),
        "tokens_at_last_compaction": execution.map_or(0, |value| value.tokens_at_last_compaction),
    })
}

fn child_inspection(
    state: &SessionState,
    binding: &SessionStyleBinding,
) -> Result<Value, StyleIntrospectionError> {
    let policy = serde_json::from_str::<Value>(&binding.child_agent_policy_json)
        .map_err(|_| StyleIntrospectionError::InvalidCompiledStyle)?;
    Ok(json!({
        "policy": policy,
        "executions": state.child_agents,
        "joins": state.planner_worker.joins,
        "reviewer_findings": state.planner_worker.reviews,
    }))
}

fn maximum_step(execution: &crate::session::StyleExecutionState) -> u64 {
    execution
        .active_node
        .iter()
        .map(|node| node.step)
        .chain(execution.completed_nodes.iter().map(|node| node.step))
        .chain(execution.failed_nodes.iter().map(|node| node.step))
        .chain(
            execution
                .transitions
                .iter()
                .map(|transition| transition.step),
        )
        .max()
        .unwrap_or(0)
}

fn maximum_loop_count(execution: &crate::session::StyleExecutionState) -> u32 {
    execution
        .active_node
        .iter()
        .map(|node| node.loop_iteration)
        .chain(
            execution
                .completed_nodes
                .iter()
                .map(|node| node.loop_iteration),
        )
        .chain(
            execution
                .failed_nodes
                .iter()
                .map(|node| node.loop_iteration),
        )
        .chain(
            execution
                .transitions
                .iter()
                .map(|transition| transition.loop_iteration),
        )
        .max()
        .map_or(0, |iteration| iteration.saturating_add(1))
}

fn retry_count(execution: &crate::session::StyleExecutionState) -> u64 {
    let mut maximum_attempts = BTreeMap::<(String, u32), u32>::new();
    for (node_id, loop_iteration, attempt) in execution
        .active_node
        .iter()
        .map(|node| (node.node_id.clone(), node.loop_iteration, node.attempt))
        .chain(
            execution
                .completed_nodes
                .iter()
                .map(|node| (node.node_id.clone(), node.loop_iteration, node.attempt)),
        )
        .chain(
            execution
                .failed_nodes
                .iter()
                .map(|node| (node.node_id.clone(), node.loop_iteration, node.attempt)),
        )
    {
        maximum_attempts
            .entry((node_id, loop_iteration))
            .and_modify(|current| *current = (*current).max(attempt))
            .or_insert(attempt);
    }
    maximum_attempts
        .into_values()
        .map(|attempt| u64::from(attempt.saturating_sub(1)))
        .sum()
}

fn control_node_id(control: Option<&StyleExecutionControlState>) -> Option<&str> {
    match control? {
        StyleExecutionControlState::ReadyForEntry(cursor) => Some(&cursor.node_id),
        StyleExecutionControlState::Active(node) => Some(&node.node_id),
        StyleExecutionControlState::AwaitingTransition(node) => Some(&node.node_id),
        StyleExecutionControlState::AwaitingDestinationEntry(transition) => {
            Some(&transition.to_node_id)
        }
        StyleExecutionControlState::Terminal { .. } => None,
    }
}

fn transition_candidates(
    graph: &ExecutableGraph,
    node_id_value: &str,
    canonical_variables: Option<&CanonicalVariableEventReducer>,
) -> Result<Vec<Value>, StyleIntrospectionError> {
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == node_id_value)
        .ok_or(StyleIntrospectionError::InvalidGraphState)?;
    graph
        .edges
        .iter()
        .filter(|edge| edge.from == node.index)
        .map(|edge| {
            edge_introspection_json(
                graph,
                edge,
                canonical_variables
                    .map(|variables| classify_edge_condition(variables, node_id_value, edge)),
            )
        })
        .collect()
}

fn known_eligible_transitions(
    graph: &ExecutableGraph,
    control: &StyleExecutionControlState,
    canonical_variables: Option<&CanonicalVariableEventReducer>,
) -> Result<Vec<Value>, StyleIntrospectionError> {
    match control {
        StyleExecutionControlState::ReadyForEntry(cursor) => Ok(vec![json!({
            "kind": "enter_node",
            "to_node_id": cursor.node_id,
        })]),
        StyleExecutionControlState::AwaitingDestinationEntry(transition) => Ok(vec![json!({
            "kind": "enter_node",
            "from_node_id": transition.from_node_id,
            "to_node_id": transition.to_node_id,
        })]),
        StyleExecutionControlState::AwaitingTransition(node) => {
            let source = graph
                .nodes
                .iter()
                .find(|candidate| candidate.id == node.node_id)
                .ok_or(StyleIntrospectionError::InvalidGraphState)?;
            graph
                .edges
                .iter()
                .filter(|edge| edge.from == source.index)
                .filter_map(|edge| match canonical_variables {
                    Some(variables) => {
                        let eligibility = classify_edge_condition(variables, &source.id, edge);
                        matches!(eligibility, ConditionEligibility::Eligible)
                            .then_some((edge, Some(eligibility)))
                    }
                    None => edge.condition.is_none().then_some((edge, None)),
                })
                .map(|(edge, eligibility)| edge_introspection_json(graph, edge, eligibility))
                .collect()
        }
        StyleExecutionControlState::Active(_) | StyleExecutionControlState::Terminal { .. } => {
            Ok(Vec::new())
        }
    }
}

fn canonical_variable_inspection(
    variables: &CanonicalVariableEventReducer,
) -> Result<Value, StyleIntrospectionError> {
    let environment = variables.environment();
    let state_hash = variables
        .state_hash()
        .map_err(|_| StyleIntrospectionError::InvalidCanonicalVariableState)?;
    let declarations = environment
        .declarations()
        .iter()
        .map(|(name, declaration)| {
            let entry = environment.canonical_entries().get(name);
            let removed = variables.removed().get(name);
            let (state, version, value_hash, writer, branch_id) = match (entry, removed) {
                (Some(entry), None) => (
                    "assigned",
                    Some(entry.version),
                    Some(entry.value_hash),
                    Some(&entry.writer),
                    entry.branch_id.as_deref(),
                ),
                (None, Some(removed)) => (
                    "removed",
                    Some(removed.version),
                    Some(removed.removed_value_hash),
                    Some(&removed.writer),
                    None,
                ),
                (None, None) => ("unassigned", None, None, None, None),
                (Some(_), Some(_)) => {
                    return Err(StyleIntrospectionError::InvalidCanonicalVariableState);
                }
            };
            Ok(json!({
                "name": declaration.name,
                "type": declaration.value_type,
                "scope": declaration.scope,
                "producer": declaration.producer,
                "consumers": declaration.consumers,
                "mutability": declaration.mutability,
                "merge_policy": declaration.merge_policy,
                "max_size_bytes": declaration.max_size_bytes,
                "security_classification": declaration.security_classification,
                "state": state,
                "version": version,
                "value_hash": value_hash,
                "writer": writer,
                "branch_id": branch_id,
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "projection": "canonical",
        "run_id": variables.run_id(),
        "state_hash": state_hash,
        "declarations": declarations,
        "validation_failure_count": variables.validation_failures().len(),
    }))
}

fn classify_edge_condition(
    variables: &CanonicalVariableEventReducer,
    source_node_id: &str,
    edge: &ExecutableEdge,
) -> ConditionEligibility {
    let Some(condition) = edge.condition.as_ref() else {
        return ConditionEligibility::Eligible;
    };
    let mut roots = BTreeSet::new();
    collect_expression_roots(condition, &mut roots);
    let required_variables = roots
        .into_iter()
        .filter(|root| variables.environment().declarations().contains_key(root))
        .collect();
    variables.environment().classify_compiled_condition(
        condition,
        &VariableReader {
            node_id: source_node_id.to_owned(),
            branch_id: None,
        },
        &required_variables,
    )
}

fn collect_expression_roots(expression: &Expression, roots: &mut BTreeSet<String>) {
    match expression {
        Expression::Value(operand) => collect_operand_root(operand, roots),
        Expression::Not(inner) => collect_expression_roots(inner, roots),
        Expression::And(left, right) | Expression::Or(left, right) => {
            collect_expression_roots(left, roots);
            collect_expression_roots(right, roots);
        }
        Expression::Compare { left, right, .. } => {
            collect_operand_root(left, roots);
            collect_operand_root(right, roots);
        }
        Expression::Exists(path) => collect_path_root(path.segments(), roots),
    }
}

fn collect_operand_root(operand: &Operand, roots: &mut BTreeSet<String>) {
    if let Operand::Path(path) = operand {
        collect_path_root(path.segments(), roots);
    }
}

fn collect_path_root(path: &[PathSegment], roots: &mut BTreeSet<String>) {
    if let Some(PathSegment::Key(root)) = path.first() {
        roots.insert(root.clone());
    }
}

fn edge_introspection_json(
    graph: &ExecutableGraph,
    edge: &ExecutableEdge,
    eligibility: Option<ConditionEligibility>,
) -> Result<Value, StyleIntrospectionError> {
    let mut value = edge_json(graph, edge)?;
    if let Some(eligibility) = eligibility {
        value
            .as_object_mut()
            .expect("edge inspection is an object")
            .insert(
                String::from("eligibility"),
                condition_eligibility_json(eligibility),
            );
    }
    Ok(value)
}

fn condition_eligibility_json(eligibility: ConditionEligibility) -> Value {
    match eligibility {
        ConditionEligibility::Eligible => json!({
            "status": "eligible",
        }),
        ConditionEligibility::Ineligible => json!({
            "status": "ineligible",
        }),
        ConditionEligibility::MissingInput { path } => json!({
            "status": "missing_input",
            "path": path,
        }),
        ConditionEligibility::InvalidExpression { diagnostic } => json!({
            "status": "invalid_expression",
            "diagnostic": bounded_diagnostic(&diagnostic),
        }),
    }
}

fn bounded_diagnostic(diagnostic: &str) -> String {
    if diagnostic.len() <= MAX_CONDITION_DIAGNOSTIC_BYTES {
        return diagnostic.to_owned();
    }
    let mut end = MAX_CONDITION_DIAGNOSTIC_BYTES;
    while !diagnostic.is_char_boundary(end) {
        end -= 1;
    }
    diagnostic[..end].to_owned()
}

fn edge_json(
    graph: &ExecutableGraph,
    edge: &ExecutableEdge,
) -> Result<Value, StyleIntrospectionError> {
    Ok(json!({
        "from_node_id": node_id(graph, edge.from)?,
        "to_node_id": node_id(graph, edge.to)?,
        "label": edge.label,
        "conditional": edge.condition.is_some(),
    }))
}

fn node_id(graph: &ExecutableGraph, index: usize) -> Result<&str, StyleIntrospectionError> {
    graph
        .nodes
        .iter()
        .find(|node| node.index == index)
        .map(|node| node.id.as_str())
        .ok_or(StyleIntrospectionError::InvalidGraphState)
}

/// Style inspection projection failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StyleIntrospectionError {
    /// Retained compiled style JSON is malformed.
    #[error("retained compiled style is invalid")]
    InvalidCompiledStyle,
    /// Retained compiled identity does not match the immutable binding.
    #[error("retained compiled style identity does not match the session binding")]
    CompiledIdentityMismatch,
    /// Replay graph state references a missing compiled node.
    #[error("replay graph state is inconsistent with the compiled graph")]
    InvalidGraphState,
    /// Replay-derived canonical variable state failed integrity validation.
    #[error("replay canonical variable state is invalid")]
    InvalidCanonicalVariableState,
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use agentmod_expression_engine::{Expression, ExpressionLimits};
    use agentmod_graph_engine::{
        ExecutableEdge, ExecutableGraph, ExecutableNode, GraphBudget, GraphCacheKey,
        GraphDeclarations, NodeKind, SecurityClassification, VariableDeclaration,
        VariableMutability, VariableScope, VariableValueType,
    };
    use agentmod_primitives::ContentHash;

    use super::{graph_inspection, known_eligible_transitions, transition_candidates};
    use crate::{
        canonical_variables::{
            CanonicalVariableEventReducer, CanonicalVariableValue, VariableEnvironmentLimits,
        },
        session::{StyleExecutionControlState, StyleExecutionState, StyleNodeCompletedEvent},
    };

    fn node(index: usize, id: &str, kind: NodeKind, reads: &[&str]) -> ExecutableNode {
        ExecutableNode {
            index,
            id: id.to_owned(),
            kind,
            configuration: None,
            condition: None,
            tool: None,
            provider: None,
            required_capabilities: BTreeSet::new(),
            read_scopes: BTreeSet::new(),
            write_scopes: BTreeSet::new(),
            read_variables: reads.iter().map(|value| (*value).to_owned()).collect(),
            write_variables: BTreeSet::new(),
            retry_limit: 0,
            max_iterations: None,
        }
    }

    fn declaration(
        name: &str,
        value_type: VariableValueType,
        classification: SecurityClassification,
    ) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            value_type,
            scope: VariableScope::Run,
            producer: String::from("runtime"),
            merge_contributors: BTreeSet::new(),
            consumers: BTreeSet::from([String::from("branch")]),
            mutability: VariableMutability::Mutable,
            merge_policy: None,
            max_size_bytes: 4_096,
            security_classification: classification,
        }
    }

    fn edge(from: usize, to: usize, condition: Option<&str>, label: &str) -> ExecutableEdge {
        ExecutableEdge {
            from,
            to,
            condition: condition.map(|source| {
                Expression::parse(source, ExpressionLimits::default()).expect("condition")
            }),
            label: Some(label.to_owned()),
        }
    }

    fn graph() -> ExecutableGraph {
        let digest = ContentHash::digest(b"introspection-test");
        ExecutableGraph {
            format_version: 1,
            entry_index: 0,
            budget: GraphBudget {
                max_steps: 32,
                max_tokens: 1_024,
                max_cost_micros: 1_000,
                max_duration_ms: 60_000,
            },
            declarations: GraphDeclarations::default(),
            variables: vec![
                declaration(
                    "flag",
                    VariableValueType::Boolean,
                    SecurityClassification::Public,
                ),
                declaration(
                    "missing",
                    VariableValueType::Boolean,
                    SecurityClassification::Internal,
                ),
                declaration(
                    "secret",
                    VariableValueType::SecretReference,
                    SecurityClassification::SecretReference,
                ),
            ],
            nodes: vec![
                node(
                    0,
                    "branch",
                    NodeKind::ConditionalBranch,
                    &["flag", "missing"],
                ),
                node(1, "eligible", NodeKind::CompleteTurn, &[]),
                node(2, "ineligible", NodeKind::CompleteTurn, &[]),
                node(3, "missing_input", NodeKind::CompleteTurn, &[]),
                node(4, "invalid_expression", NodeKind::CompleteTurn, &[]),
                node(5, "unconditional", NodeKind::CompleteTurn, &[]),
            ],
            edges: vec![
                edge(0, 1, Some("flag == true"), "eligible"),
                edge(0, 2, Some("flag == false"), "ineligible"),
                edge(0, 3, Some("missing == true"), "missing_input"),
                edge(0, 4, Some("flag > \"text\""), "invalid_expression"),
                edge(0, 5, None, "unconditional"),
            ],
            cache_key: GraphCacheKey {
                graph_content_hash: digest,
                plugin_set_hash: digest,
                capability_set_hash: digest,
                runtime_api_hash: digest,
                combined_hash: digest,
            },
        }
    }

    fn canonical_variables() -> CanonicalVariableEventReducer {
        CanonicalVariableEventReducer::initialize(
            "run-introspection",
            VariableEnvironmentLimits::default(),
            graph().variables,
            [
                (String::from("flag"), CanonicalVariableValue::Boolean(true)),
                (
                    String::from("secret"),
                    CanonicalVariableValue::SecretReference(String::from(
                        "secret-store://credential",
                    )),
                ),
            ],
        )
        .expect("canonical variables")
    }

    fn completed_branch() -> StyleNodeCompletedEvent {
        StyleNodeCompletedEvent {
            node_id: String::from("branch"),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            result_reference: None,
            artifact_reference: None,
        }
    }

    fn execution(
        graph: &ExecutableGraph,
        canonical_variables: Option<CanonicalVariableEventReducer>,
    ) -> StyleExecutionState {
        StyleExecutionState {
            completed_turn_runs: Vec::new(),
            generic_model_invocations: BTreeMap::new(),
            graph: Box::new(graph.clone()),
            input_reference: None,
            execution_contract: None,
            canonical_variables: canonical_variables.map(Box::new),
            control: StyleExecutionControlState::AwaitingTransition(completed_branch()),
            active_node: None,
            active_node_entered_at: None,
            completed_nodes: vec![completed_branch()],
            emitted_user_events: Vec::new(),
            graph_schedules: BTreeMap::new(),
            child_messages: BTreeMap::new(),
            plugin_node_invocations: BTreeMap::new(),
            parallel_executions: BTreeMap::new(),
            generic_joins: BTreeMap::new(),
            failed_nodes: Vec::new(),
            transitions: Vec::new(),
            termination_reason: None,
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: 0,
            cost_micros: 0,
            cost_estimated: false,
            tokens_at_last_compaction: 0,
            context_boundaries: Vec::new(),
            latest_model_execution: None,
        }
    }

    #[test]
    fn canonical_projection_classifies_every_transition_exactly() {
        let graph = graph();
        let variables = canonical_variables();
        let candidates =
            transition_candidates(&graph, "branch", Some(&variables)).expect("candidates");
        let statuses = candidates
            .iter()
            .map(|candidate| {
                (
                    candidate["label"].as_str().expect("label"),
                    candidate["eligibility"]["status"].as_str().expect("status"),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(statuses["eligible"], "eligible");
        assert_eq!(statuses["ineligible"], "ineligible");
        assert_eq!(statuses["missing_input"], "missing_input");
        assert_eq!(statuses["invalid_expression"], "invalid_expression");
        assert_eq!(statuses["unconditional"], "eligible");
        assert_eq!(
            candidates
                .iter()
                .find(|candidate| candidate["label"] == "missing_input")
                .expect("missing candidate")["eligibility"]["path"],
            "missing"
        );

        let eligible = known_eligible_transitions(
            &graph,
            &StyleExecutionControlState::AwaitingTransition(completed_branch()),
            Some(&variables),
        )
        .expect("eligible");
        assert_eq!(
            eligible
                .iter()
                .map(|edge| edge["label"].as_str().expect("label"))
                .collect::<Vec<_>>(),
            ["eligible", "unconditional"]
        );
    }

    #[test]
    fn variable_projection_survives_restart_without_exposing_values() {
        let graph = graph();
        let execution = execution(&graph, Some(canonical_variables()));
        let before = graph_inspection(&graph, Some(&execution)).expect("inspection");
        let serialized = serde_json::to_vec(&execution).expect("serialize");
        let replayed: StyleExecutionState =
            serde_json::from_slice(&serialized).expect("deserialize");
        replayed
            .canonical_variables
            .as_deref()
            .expect("canonical variables")
            .validate_replayed()
            .expect("validate replay");
        let after = graph_inspection(&graph, Some(&replayed)).expect("inspection");

        assert_eq!(before, after);
        let encoded = serde_json::to_string(&after).expect("json");
        assert!(!encoded.contains("secret-store://credential"));
        assert!(!encoded.contains("\"value\":"));
        let secret = after["variables"]["declarations"]
            .as_array()
            .expect("declarations")
            .iter()
            .find(|declaration| declaration["name"] == "secret")
            .expect("secret metadata");
        assert_eq!(secret["security_classification"], "secret_reference");
        assert_eq!(secret["version"], 1);
        assert!(secret["value_hash"].is_string());
    }

    #[test]
    fn legacy_projection_keeps_conditional_edges_conservative() {
        let graph = graph();
        let candidates = transition_candidates(&graph, "branch", None).expect("legacy candidates");
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.get("eligibility").is_none())
        );

        let eligible = known_eligible_transitions(
            &graph,
            &StyleExecutionControlState::AwaitingTransition(completed_branch()),
            None,
        )
        .expect("legacy eligible");
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0]["label"], "unconditional");
        assert!(eligible[0].get("eligibility").is_none());
    }
}
