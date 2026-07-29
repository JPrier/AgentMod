//! Replay-derived session-style and graph inspection.

use std::collections::BTreeMap;

use agentmod_graph_engine::{ExecutableEdge, ExecutableGraph};
use agentmod_session_style_sdk::CompiledSessionStyle;
use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    conversation::ConversationEntry,
    session::{SessionState, SessionStyleBinding, StyleExecutionControlState, StyleExecutionState},
};

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
    let compaction = compaction_inspection(binding, execution);
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
    let active = execution.and_then(|value| value.active_node.as_ref());
    let current_node_id = active
        .map(|node| node.node_id.as_str())
        .or_else(|| control_node_id(execution.map(|value| &value.control)));
    let candidates = current_node_id
        .map(|node_id_value| transition_candidates(graph, node_id_value))
        .transpose()?
        .unwrap_or_default();
    let known_eligible = execution
        .map(|value| known_eligible_transitions(graph, &value.control))
        .transpose()?
        .unwrap_or_default();
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
        .history()
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
            })),
            _ => None,
        })
        .collect::<Vec<_>>();
    json!({
        "selection": binding.memory,
        "retrieved_provenance": provenance,
    })
}

fn compaction_inspection(
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
) -> Result<Vec<Value>, StyleIntrospectionError> {
    let index = graph
        .nodes
        .iter()
        .find(|node| node.id == node_id_value)
        .map(|node| node.index)
        .ok_or(StyleIntrospectionError::InvalidGraphState)?;
    graph
        .edges
        .iter()
        .filter(|edge| edge.from == index)
        .map(|edge| edge_json(graph, edge))
        .collect()
}

fn known_eligible_transitions(
    graph: &ExecutableGraph,
    control: &StyleExecutionControlState,
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
            let index = graph
                .nodes
                .iter()
                .find(|candidate| candidate.id == node.node_id)
                .map(|candidate| candidate.index)
                .ok_or(StyleIntrospectionError::InvalidGraphState)?;
            graph
                .edges
                .iter()
                .filter(|edge| edge.from == index && edge.condition.is_none())
                .map(|edge| edge_json(graph, edge))
                .collect()
        }
        StyleExecutionControlState::Active(_) | StyleExecutionControlState::Terminal { .. } => {
            Ok(Vec::new())
        }
    }
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
}
