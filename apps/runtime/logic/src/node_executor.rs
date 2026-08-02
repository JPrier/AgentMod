//! Runtime-owned node-executor resolution and executability validation.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use agentmod_graph_engine::{
    ArtifactContentSource, ChildWorkspaceConfiguration, ExecutableGraph, ExecutableNode,
    NodeConfiguration, NodeKind,
};
use agentmod_primitives::ContentHash;
use agentmod_runtime_data::node_executor::{
    ListNodeExecutorsDataRequest, NodeExecutorBoundaryData, NodeExecutorDataError,
    NodeExecutorDataPort, NodeExecutorSourceData,
};
use semver::{Version, VersionReq};
use serde::Serialize;
use thiserror::Error;

use crate::{
    node_execution::{NativeExecutorKey, native_executor_key},
    session::{
        EXECUTION_PLAN_COMPILER_V3, SessionExecutionPlan, SessionExecutionPlanCompilation,
        SessionNodeExecutorBoundary, SessionNodeExecutorResolution, SessionNodeExecutorSource,
        SessionStyleBinding,
    },
    style_executor::{CompiledStyleExecutor, StyleExecutorError},
};

/// Logic-owned executor source.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NodeExecutorSource {
    /// Runtime logic owns execution.
    Runtime,
    /// An activated plugin owns execution behind the plugin-host boundary.
    Plugin {
        /// Exact plugin identity.
        plugin_id: String,
    },
}

/// Logic-owned execution boundary.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NodeExecutorBoundary {
    /// Runtime logic implementation.
    RuntimeLogic,
    /// Isolated plugin-host invocation.
    PluginHost,
}

/// Inspectable normalized node-executor capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecutorCapability {
    /// Stable implementation ID.
    pub id: String,
    /// Exact implementation version.
    pub version: String,
    /// Runtime API version requirement.
    pub runtime_api: String,
    /// Serialized graph node kind.
    pub node_kind: String,
    /// Supported business capabilities.
    pub capabilities: BTreeSet<String>,
    /// Registration source.
    pub source: NodeExecutorSource,
    /// Execution boundary.
    pub boundary: NodeExecutorBoundary,
    /// Whether new graph executions may select the implementation.
    pub available: bool,
    /// Hash of the exact executor declaration/configuration.
    pub declaration_hash: ContentHash,
}

/// Exact implementation resolved for one compiled node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedNodeExecutor {
    /// Compiled graph node ID.
    pub node_id: String,
    /// Serialized node kind.
    pub node_kind: String,
    /// Selected implementation ID.
    pub implementation_id: String,
    /// Exact implementation version.
    pub implementation_version: String,
    /// Exact implementation source.
    pub source: NodeExecutorSource,
    /// Execution boundary.
    pub boundary: NodeExecutorBoundary,
    /// Capabilities required by the compiled node.
    pub required_capabilities: BTreeSet<String>,
    /// Exact capabilities of the selected implementation.
    pub resolved_capabilities: BTreeSet<String>,
    /// Runtime API requirement declared by the implementation.
    pub runtime_api_requirement: String,
    /// Hash of the exact selected executor declaration/configuration.
    pub executor_declaration_hash: ContentHash,
    /// Exact compiled-node adapter configuration hash.
    pub adapter_configuration_reference: ContentHash,
}

/// Stable runtime-executability diagnostic.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RuntimeExecutabilityDiagnostic {
    /// Stable diagnostic code.
    pub code: String,
    /// Compiled node ID, when the diagnostic is node-specific.
    pub node_id: Option<String>,
    /// Serialized node kind, when known.
    pub node_kind: Option<String>,
    /// Safe deterministic explanation.
    pub message: String,
}

/// Complete runtime-executability inspection result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeExecutabilityReport {
    /// Whether every node can execute now.
    pub executable: bool,
    /// Hash of the exact sorted capability registry used for resolution.
    pub registry_hash: ContentHash,
    /// Exact per-node resolutions.
    pub resolved_nodes: Vec<ResolvedNodeExecutor>,
    /// Immutable plan produced when every node resolves.
    pub execution_plan: Option<SessionExecutionPlan>,
    /// Hash of the canonical immutable plan.
    pub execution_plan_hash: Option<ContentHash>,
    /// Stable sorted diagnostics.
    pub diagnostics: Vec<RuntimeExecutabilityDiagnostic>,
}

/// Lists the normalized executor capability registry through runtime data.
///
/// # Errors
///
/// Returns [`RuntimeExecutabilityError`] when data is unavailable or contains
/// invalid semantic versions.
pub fn inspect_node_executor_capabilities<D: NodeExecutorDataPort>(
    data: &D,
) -> Result<Vec<NodeExecutorCapability>, RuntimeExecutabilityError> {
    data.list_node_executors(ListNodeExecutorsDataRequest)
        .map_err(RuntimeExecutabilityError::Data)?
        .into_iter()
        .map(|record| {
            Version::parse(&record.version).map_err(|_| {
                RuntimeExecutabilityError::InvalidRegistrationVersion {
                    implementation: record.id.clone(),
                }
            })?;
            VersionReq::parse(&record.runtime_api).map_err(|_| {
                RuntimeExecutabilityError::InvalidRuntimeApiRequirement {
                    implementation: record.id.clone(),
                }
            })?;
            Ok(NodeExecutorCapability {
                id: record.id,
                version: record.version,
                runtime_api: record.runtime_api,
                node_kind: record.node_kind,
                capabilities: record.capabilities,
                source: match record.source {
                    NodeExecutorSourceData::Runtime => NodeExecutorSource::Runtime,
                    NodeExecutorSourceData::Plugin { plugin_id } => {
                        NodeExecutorSource::Plugin { plugin_id }
                    }
                },
                boundary: match record.boundary {
                    NodeExecutorBoundaryData::RuntimeLogic => NodeExecutorBoundary::RuntimeLogic,
                    NodeExecutorBoundaryData::PluginHost => NodeExecutorBoundary::PluginHost,
                },
                available: record.available,
                declaration_hash: record.declaration_hash,
            })
        })
        .collect()
}

/// Resolves every compiled graph node against the live runtime registry.
///
/// This validation is deliberately separate from parsing/graph compilation:
/// a structurally valid graph is not necessarily executable by this runtime.
///
/// # Errors
///
/// Returns [`RuntimeExecutabilityError`] when the retained binding or registry
/// cannot be inspected. Unsupported nodes and topologies are returned as a
/// deterministic non-executable report.
pub(crate) fn inspect_runtime_executability<D: NodeExecutorDataPort>(
    data: &D,
    binding: &SessionStyleBinding,
) -> Result<RuntimeExecutabilityReport, RuntimeExecutabilityError> {
    let executor = CompiledStyleExecutor::from_unbound_binding(binding)
        .map_err(RuntimeExecutabilityError::CompiledStyle)?;
    let capabilities = inspect_node_executor_capabilities(data)?;
    let registry_hash = registry_hash(&capabilities);
    let runtime_api = Version::parse(&binding.runtime_api_version)
        .map_err(|_| RuntimeExecutabilityError::InvalidBindingRuntimeApi)?;
    let mut by_kind: BTreeMap<&str, Vec<&NodeExecutorCapability>> = BTreeMap::new();
    for capability in &capabilities {
        by_kind
            .entry(capability.node_kind.as_str())
            .or_default()
            .push(capability);
    }

    let mut resolved_nodes = Vec::new();
    let mut diagnostics = Vec::new();
    for node in &executor.compiled().graph.nodes {
        match resolve_node(node, executor.compiled(), &by_kind, &runtime_api)? {
            Ok(resolved) => resolved_nodes.push(resolved),
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
    }

    resolved_nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    diagnostics.extend(parallel_region_diagnostics(
        &executor.compiled().graph,
        &resolved_nodes,
    ));
    diagnostics.sort();
    let execution_plan = diagnostics.is_empty().then(|| SessionExecutionPlan {
        registry_hash,
        compilation: SessionExecutionPlanCompilation {
            compiler: String::from(execution_plan_compiler(binding)),
            compiled_style_hash: binding.compiled_style_hash,
            compiled_cache_key: binding.compiled_cache_key,
            runtime_api_version: binding.runtime_api_version.clone(),
        },
        nodes: resolved_nodes.iter().map(to_session_resolution).collect(),
    });
    let execution_plan_hash = execution_plan
        .as_ref()
        .map(execution_plan_hash)
        .transpose()?;
    Ok(RuntimeExecutabilityReport {
        executable: diagnostics.is_empty(),
        registry_hash,
        resolved_nodes,
        execution_plan,
        execution_plan_hash,
        diagnostics,
    })
}

/// Selects compilation provenance for a newly bound immutable execution plan.
///
/// The original built-in `1.1.0` descriptors are frozen compatibility
/// contracts whose recovery semantics are implemented by generation two.
/// Source identity is part of this decision so a user, project, plugin, or
/// inline graph cannot acquire legacy dispatch by copying a built-in ID.
/// Every other newly compiled graph is generation three and therefore must
/// execute exclusively through its exact persisted executor resolutions.
fn execution_plan_compiler(binding: &SessionStyleBinding) -> &'static str {
    if binding.source == crate::session::SessionStyleSource::BuiltIn
        && binding.version == "1.1.0"
        && matches!(
            binding.id.as_str(),
            "persistent-chat"
                | "ephemeral-turn"
                | "research-loop"
                | "planner-worker"
                | "declarative-graph"
        )
    {
        crate::session::EXECUTION_PLAN_COMPILER_V2
    } else {
        EXECUTION_PLAN_COMPILER_V3
    }
}

fn resolve_node(
    node: &agentmod_graph_engine::ExecutableNode,
    compiled: &agentmod_session_style_sdk::CompiledSessionStyle,
    by_kind: &BTreeMap<&str, Vec<&NodeExecutorCapability>>,
    runtime_api: &Version,
) -> Result<Result<ResolvedNodeExecutor, RuntimeExecutabilityDiagnostic>, RuntimeExecutabilityError>
{
    let node_kind = serialized_node_kind(node.kind)?;
    let registrations = by_kind.get(node_kind.as_str()).cloned().unwrap_or_default();
    if registrations.is_empty() {
        return Ok(Err(diagnostic(
            "NODEX001",
            Some(&node.id),
            Some(&node_kind),
            format!("no node executor is registered for `{node_kind}`"),
        )));
    }
    let allowed = registrations
        .into_iter()
        .filter(|registration| source_allowed(registration, compiled))
        .collect::<Vec<_>>();
    if allowed.is_empty() {
        return Ok(Err(diagnostic(
            "NODEX002",
            Some(&node.id),
            Some(&node_kind),
            format!("no allowed node executor is registered for `{node_kind}`"),
        )));
    }
    let preferred_version = preferred_native_executor_version(node, &node_kind);
    let compatible = allowed
        .into_iter()
        .filter(|registration| registration.available)
        .filter(|registration| {
            VersionReq::parse(&registration.runtime_api)
                .is_ok_and(|requirement| requirement.matches(runtime_api))
        })
        .filter(|registration| {
            node.required_capabilities
                .is_subset(&registration.capabilities)
        })
        .filter(|registration| {
            !matches!(registration.source, NodeExecutorSource::Runtime)
                || preferred_version.is_none_or(|version| registration.version == version)
        })
        .collect::<Vec<_>>();
    if compatible.is_empty() {
        return Ok(Err(diagnostic(
            "NODEX003",
            Some(&node.id),
            Some(&node_kind),
            format!("registered node executors for `{node_kind}` are unavailable or incompatible"),
        )));
    }
    let same_implementation_family = compatible.iter().all(|registration| {
        registration.id == compatible[0].id
            && registration.source == compatible[0].source
            && registration.boundary == compatible[0].boundary
    });
    if !same_implementation_family {
        return Ok(Err(diagnostic(
            "NODEX004",
            Some(&node.id),
            Some(&node_kind),
            format!("more than one compatible node executor can execute `{node_kind}`"),
        )));
    }
    let mut selected = compatible[0];
    let mut selected_version = Version::parse(&selected.version).map_err(|_| {
        RuntimeExecutabilityError::InvalidRegistrationVersion {
            implementation: selected.id.clone(),
        }
    })?;
    for candidate in &compatible[1..] {
        let candidate_version = Version::parse(&candidate.version).map_err(|_| {
            RuntimeExecutabilityError::InvalidRegistrationVersion {
                implementation: candidate.id.clone(),
            }
        })?;
        if candidate_version > selected_version {
            selected = *candidate;
            selected_version = candidate_version;
        }
    }
    Ok(Ok(ResolvedNodeExecutor {
        node_id: node.id.clone(),
        node_kind,
        implementation_id: selected.id.clone(),
        implementation_version: selected.version.clone(),
        source: selected.source.clone(),
        boundary: selected.boundary,
        required_capabilities: node.required_capabilities.clone(),
        resolved_capabilities: selected.capabilities.clone(),
        runtime_api_requirement: selected.runtime_api.clone(),
        executor_declaration_hash: selected.declaration_hash,
        adapter_configuration_reference: ContentHash::digest(
            &serde_json::to_vec(node)
                .map_err(|_| RuntimeExecutabilityError::InvalidNodeConfiguration)?,
        ),
    }))
}

fn preferred_native_executor_version(
    node: &ExecutableNode,
    node_kind: &str,
) -> Option<&'static str> {
    match (node_kind, node.configuration.as_ref()) {
        ("tool_execution_gate", _) => Some("1.1.0"),
        ("model_call", Some(NodeConfiguration::ModelRequest { inputs, .. })) => {
            Some(if inputs.is_empty() { "1.0.0" } else { "1.1.0" })
        }
        (
            "spawn_child_agent",
            Some(NodeConfiguration::SpawnChildAgent {
                workspace,
                artifact_reference_variables,
                ..
            }),
        ) => Some(
            if !artifact_reference_variables.is_empty()
                || matches!(
                    workspace,
                    ChildWorkspaceConfiguration::IsolatedCopy
                        | ChildWorkspaceConfiguration::BranchWorkspace { .. }
                )
            {
                "1.1.0"
            } else {
                "1.0.0"
            },
        ),
        (
            "review",
            Some(NodeConfiguration::Review {
                artifact_reference_variables,
                ..
            }),
        ) => Some(if artifact_reference_variables.is_empty() {
            "1.0.0"
        } else {
            "1.1.0"
        }),
        ("persist_artifact", Some(NodeConfiguration::PersistArtifact { content, .. })) => Some(
            if matches!(content, ArtifactContentSource::NodeResultProjection { .. }) {
                "1.1.0"
            } else {
                "1.0.0"
            },
        ),
        _ => None,
    }
}

/// Requires a compiled binding to be executable before canonical state can be
/// created or branched.
///
/// # Errors
///
/// Returns [`RuntimeExecutabilityError::Unsupported`] with deterministic
/// diagnostics when execution is not available.
pub(crate) fn bind_runtime_execution_plan<D: NodeExecutorDataPort>(
    data: &D,
    binding: &mut SessionStyleBinding,
) -> Result<RuntimeExecutabilityReport, RuntimeExecutabilityError> {
    if binding.execution_plan.is_some() || binding.execution_plan_hash.is_some() {
        revalidate_runtime_execution_plan(data, binding)?;
        return inspect_bound_execution_plan(binding);
    }
    let report = inspect_runtime_executability(data, binding)?;
    if !report.executable {
        return Err(RuntimeExecutabilityError::Unsupported {
            diagnostics: report.diagnostics,
        });
    }
    binding.execution_plan.clone_from(&report.execution_plan);
    binding.execution_plan_hash = report.execution_plan_hash;
    Ok(report)
}

/// Revalidates an exact persisted execution plan without selecting or rebinding
/// any implementation.
///
/// # Errors
///
/// Returns stable migration diagnostics when the plan, registry, graph, or
/// exact selected implementation has drifted.
#[allow(
    clippy::too_many_lines,
    reason = "exact revalidation keeps every persisted executor identity field in one fail-closed audit"
)]
pub fn revalidate_runtime_execution_plan<D: NodeExecutorDataPort>(
    data: &D,
    binding: &SessionStyleBinding,
) -> Result<(), RuntimeExecutabilityError> {
    let plan = binding.execution_plan.as_ref().ok_or_else(|| {
        unsupported(
            "NODEX101",
            None,
            None,
            "the session has no persisted node-execution plan",
        )
    })?;
    let retained_hash = binding.execution_plan_hash.ok_or_else(|| {
        unsupported(
            "NODEX101",
            None,
            None,
            "the session has no persisted node-execution plan hash",
        )
    })?;
    if plan
        .nodes
        .iter()
        .any(|node| node.executor_declaration_hash == ContentHash::from_bytes([0; 32]))
    {
        return Err(unsupported(
            "NODEX107",
            None,
            None,
            "the persisted node-execution plan predates exact executor declaration binding; branch with a recompiled style",
        ));
    }
    if execution_plan_hash(plan)? != retained_hash {
        return Err(unsupported(
            "NODEX102",
            None,
            None,
            "the persisted node-execution plan hash does not match its contents",
        ));
    }
    let executor = CompiledStyleExecutor::from_unbound_binding(binding)
        .map_err(RuntimeExecutabilityError::CompiledStyle)?;
    if plan.compilation.compiler_generation().is_none()
        || plan.compilation.compiled_style_hash != binding.compiled_style_hash
        || plan.compilation.compiled_cache_key != binding.compiled_cache_key
        || plan.compilation.runtime_api_version != binding.runtime_api_version
    {
        return Err(unsupported(
            "NODEX104",
            None,
            None,
            "the persisted node-execution plan compilation identity does not match the binding",
        ));
    }
    let capabilities = inspect_node_executor_capabilities(data)?;
    if registry_hash(&capabilities) != plan.registry_hash {
        return Err(unsupported(
            "NODEX103",
            None,
            None,
            "the live node-executor registry hash differs from the persisted registry hash",
        ));
    }
    let runtime_api = Version::parse(&binding.runtime_api_version)
        .map_err(|_| RuntimeExecutabilityError::InvalidBindingRuntimeApi)?;
    if plan.nodes.len() != executor.compiled().graph.nodes.len() {
        return Err(unsupported(
            "NODEX104",
            None,
            None,
            "the persisted node-execution plan does not cover the compiled graph exactly",
        ));
    }
    for node in &executor.compiled().graph.nodes {
        let node_kind = serialized_node_kind(node.kind)?;
        let Some(selected) = plan
            .nodes
            .iter()
            .find(|selected| selected.node_id == node.id)
        else {
            return Err(unsupported(
                "NODEX104",
                Some(&node.id),
                Some(&node_kind),
                "the compiled node is missing from the persisted execution plan",
            ));
        };
        let configuration_reference = ContentHash::digest(
            &serde_json::to_vec(node)
                .map_err(|_| RuntimeExecutabilityError::InvalidNodeConfiguration)?,
        );
        let required_capabilities = node
            .required_capabilities
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        if selected.node_kind != node_kind
            || selected.required_capabilities != required_capabilities
            || selected.adapter_configuration_reference != configuration_reference
        {
            return Err(unsupported(
                "NODEX104",
                Some(&node.id),
                Some(&node_kind),
                "the persisted node resolution does not match the compiled node",
            ));
        }
        let registration = capabilities.iter().find(|registration| {
            registration.id == selected.executor_id
                && registration.version == selected.executor_version
                && registration.node_kind == selected.node_kind
                && registration.source == from_session_source(&selected.source)
                && to_session_boundary(registration.boundary) == selected.boundary
        });
        let Some(registration) = registration else {
            return Err(unsupported(
                "NODEX105",
                Some(&node.id),
                Some(&node_kind),
                "the exact persisted node executor is unavailable",
            ));
        };
        let source = to_session_source(&registration.source);
        let boundary = to_session_boundary(registration.boundary);
        let resolved_capabilities = registration
            .capabilities
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        if !registration.available
            || registration.node_kind != node_kind
            || registration.source != from_session_source(&selected.source)
            || source != selected.source
            || boundary != selected.boundary
            || registration.runtime_api != selected.runtime_api_requirement
            || registration.declaration_hash != selected.executor_declaration_hash
            || resolved_capabilities != selected.resolved_capabilities
            || !VersionReq::parse(&registration.runtime_api)
                .is_ok_and(|requirement| requirement.matches(&runtime_api))
            || !node
                .required_capabilities
                .is_subset(&registration.capabilities)
            || !source_allowed(registration, executor.compiled())
        {
            return Err(unsupported(
                "NODEX106",
                Some(&node.id),
                Some(&node_kind),
                "the exact persisted node executor identity or capabilities have drifted",
            ));
        }
    }
    let resolved_nodes = inspect_bound_execution_plan(binding)?.resolved_nodes;
    let mut diagnostics = parallel_region_diagnostics(&executor.compiled().graph, &resolved_nodes);
    if !diagnostics.is_empty() {
        diagnostics.sort();
        return Err(RuntimeExecutabilityError::Unsupported { diagnostics });
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "parallel admission reports each exact structural and executor-identity failure at the graph compilation boundary"
)]
fn parallel_region_diagnostics(
    graph: &ExecutableGraph,
    resolutions: &[ResolvedNodeExecutor],
) -> Vec<RuntimeExecutabilityDiagnostic> {
    let nodes_by_index = graph
        .nodes
        .iter()
        .map(|node| (node.index, node))
        .collect::<BTreeMap<_, _>>();
    let nodes_by_id = graph
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    let resolutions = resolutions
        .iter()
        .map(|resolution| (resolution.node_id.as_str(), resolution))
        .collect::<BTreeMap<_, _>>();
    let mut parallels = graph
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::ParallelBranch)
        .collect::<Vec<_>>();
    parallels.sort_by(|left, right| left.id.cmp(&right.id));

    let mut diagnostics = Vec::new();
    for parallel in parallels {
        let Some(NodeConfiguration::ParallelBranch { join_target, .. }) =
            parallel.configuration.as_ref()
        else {
            diagnostics.push(parallel_association_diagnostic(
                parallel,
                "<missing>",
                "parallel_branch has no typed join association",
            ));
            continue;
        };
        let Some(join) = nodes_by_id.get(join_target.as_str()).copied() else {
            diagnostics.push(parallel_association_diagnostic(
                parallel,
                join_target,
                "configured join target is missing",
            ));
            continue;
        };
        if join.kind != NodeKind::JoinResults
            || !matches!(
                join.configuration.as_ref(),
                Some(NodeConfiguration::JoinResults { .. })
            )
        {
            diagnostics.push(parallel_association_diagnostic(
                parallel,
                join_target,
                "configured join target is not a typed join_results node",
            ));
            continue;
        }
        if resolutions
            .get(parallel.id.as_str())
            .is_some_and(|resolution| {
                selected_native_key(resolution) != Some(NativeExecutorKey::Parallel)
            })
        {
            diagnostics.push(parallel_association_diagnostic(
                parallel,
                join_target,
                "selected parallel executor is not the exact production coordinator",
            ));
            continue;
        }
        if resolutions.get(join.id.as_str()).is_some_and(|resolution| {
            selected_native_key(resolution) != Some(NativeExecutorKey::Join)
        }) {
            diagnostics.push(parallel_association_diagnostic(
                parallel,
                join_target,
                "selected join executor is not the exact production coordinator",
            ));
            continue;
        }
        let region = match derive_parallel_region(graph, parallel, join, &nodes_by_index) {
            Ok(region) => region,
            Err(detail) => {
                diagnostics.push(parallel_association_diagnostic(
                    parallel,
                    join_target,
                    detail,
                ));
                continue;
            }
        };
        for index in region {
            let Some(node) = nodes_by_index.get(&index).copied() else {
                continue;
            };
            let Some(resolution) = resolutions.get(node.id.as_str()).copied() else {
                // Normal executor resolution already emitted a more precise
                // unavailable/incompatible diagnostic for this node.
                continue;
            };
            if !supported_parallel_branch_resolution(resolution) {
                diagnostics.push(diagnostic(
                    "NODEX009",
                    Some(&node.id),
                    Some(&resolution.node_kind),
                    format!(
                        "parallel region `{}` to `{join_target}` cannot dispatch exact executor `{}@{}` for branch node `{}`",
                        parallel.id,
                        resolution.implementation_id,
                        resolution.implementation_version,
                        node.id
                    ),
                ));
            }
        }
    }
    diagnostics
}

#[allow(
    clippy::too_many_lines,
    reason = "the region derivation keeps fan-out, join membership, reachability, and overlap validation in one fail-closed structural pass"
)]
fn derive_parallel_region(
    graph: &ExecutableGraph,
    parallel: &ExecutableNode,
    join: &ExecutableNode,
    nodes_by_index: &BTreeMap<usize, &ExecutableNode>,
) -> Result<BTreeSet<usize>, &'static str> {
    if nodes_by_index.len() != graph.nodes.len()
        || nodes_by_index.get(&parallel.index).copied() != Some(parallel)
        || nodes_by_index.get(&join.index).copied() != Some(join)
    {
        return Err("compiled node indices are not unique and exact");
    }
    let mut outgoing = graph
        .edges
        .iter()
        .filter(|edge| edge.from == parallel.index)
        .collect::<Vec<_>>();
    outgoing.sort_by(|left, right| {
        left.to
            .cmp(&right.to)
            .then_with(|| left.label.cmp(&right.label))
    });
    if outgoing.len() < 2
        || outgoing
            .iter()
            .any(|edge| edge.condition.is_some() || edge.label.is_none() || edge.to == join.index)
    {
        return Err("parallel fan-out does not define at least two exact labeled branches");
    }
    let labels = outgoing
        .iter()
        .filter_map(|edge| edge.label.as_deref())
        .collect::<BTreeSet<_>>();
    if labels.len() != outgoing.len() {
        return Err("parallel fan-out member labels are not unique");
    }
    let Some(NodeConfiguration::JoinResults {
        required, optional, ..
    }) = join.configuration.as_ref()
    else {
        return Err("configured join target is not a typed join_results node");
    };
    let configured = required
        .union(optional)
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if labels != configured {
        return Err("parallel fan-out labels do not match the configured join members");
    }

    let mut all_members = BTreeSet::new();
    for branch in outgoing {
        if !nodes_by_index.contains_key(&branch.to) {
            return Err("parallel fan-out references a missing compiled node");
        }
        let mut members = BTreeSet::new();
        let mut queue = VecDeque::from([branch.to]);
        let mut reached_terminal = false;
        while let Some(index) = queue.pop_front() {
            if index == join.index {
                reached_terminal = true;
                continue;
            }
            if !members.insert(index) {
                continue;
            }
            let node = nodes_by_index
                .get(&index)
                .copied()
                .ok_or("parallel branch references a missing compiled node")?;
            let targets = graph
                .edges
                .iter()
                .filter(|edge| edge.from == node.index)
                .map(|edge| edge.to)
                .collect::<Vec<_>>();
            if targets.is_empty() {
                if matches!(
                    node.kind,
                    NodeKind::Fail | NodeKind::CompleteTurn | NodeKind::CompleteSession
                ) {
                    reached_terminal = true;
                    continue;
                }
                return Err("parallel branch does not terminate at its configured join");
            }
            queue.extend(targets);
        }
        if !reached_terminal {
            return Err("parallel branch does not terminate at its configured join");
        }
        for member in &members {
            if graph
                .edges
                .iter()
                .filter(|edge| edge.to == *member)
                .any(|edge| {
                    let fanout_entry = *member == branch.to && edge.from == parallel.index;
                    !fanout_entry && !members.contains(&edge.from)
                })
            {
                return Err("parallel branch has an incoming edge outside its exact region");
            }
        }
        if members.iter().any(|member| all_members.contains(member)) {
            return Err("parallel branch regions overlap before the configured join");
        }
        all_members.extend(members);
    }
    if graph
        .edges
        .iter()
        .filter(|edge| edge.to == join.index)
        .any(|edge| !all_members.contains(&edge.from))
    {
        return Err("configured join has an incoming edge outside its parallel region");
    }
    Ok(all_members)
}

fn supported_parallel_branch_resolution(resolution: &ResolvedNodeExecutor) -> bool {
    if matches!(
        (&resolution.source, resolution.boundary),
        (
            NodeExecutorSource::Plugin { .. },
            NodeExecutorBoundary::PluginHost
        )
    ) {
        return true;
    }
    selected_native_key(resolution).is_some_and(|key| {
        matches!(
            key,
            NativeExecutorKey::Conditional
                | NativeExecutorKey::Loop
                | NativeExecutorKey::StructuredFailure
                | NativeExecutorKey::EventEmission
                | NativeExecutorKey::Delay
                | NativeExecutorKey::Schedule
                | NativeExecutorKey::ToolGate
                | NativeExecutorKey::UserApproval
                | NativeExecutorKey::ChildSpawn
                | NativeExecutorKey::ChildMessage
                | NativeExecutorKey::ChildWait
                | NativeExecutorKey::ArtifactPersistence
        )
    })
}

fn selected_native_key(resolution: &ResolvedNodeExecutor) -> Option<NativeExecutorKey> {
    native_executor_key(&to_session_resolution(resolution)).ok()
}

fn parallel_association_diagnostic(
    parallel: &ExecutableNode,
    join_target: &str,
    detail: &str,
) -> RuntimeExecutabilityDiagnostic {
    diagnostic(
        "NODEX008",
        Some(&parallel.id),
        Some("parallel_branch"),
        format!(
            "parallel node `{}` has no exact join association with `{join_target}`: {detail}",
            parallel.id
        ),
    )
}

fn source_allowed(
    registration: &NodeExecutorCapability,
    compiled: &agentmod_session_style_sdk::CompiledSessionStyle,
) -> bool {
    match &registration.source {
        NodeExecutorSource::Runtime => true,
        NodeExecutorSource::Plugin { plugin_id } => compiled
            .allowed_plugins
            .iter()
            .any(|allowed| allowed == plugin_id),
    }
}

fn serialized_node_kind(
    kind: agentmod_graph_engine::NodeKind,
) -> Result<String, RuntimeExecutabilityError> {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(RuntimeExecutabilityError::InvalidNodeKind)
}

fn registry_hash(capabilities: &[NodeExecutorCapability]) -> ContentHash {
    let mut capabilities = capabilities.to_vec();
    capabilities.sort_by(|left, right| {
        (
            &left.node_kind,
            &left.id,
            &left.version,
            &left.source,
            left.boundary,
        )
            .cmp(&(
                &right.node_kind,
                &right.id,
                &right.version,
                &right.source,
                right.boundary,
            ))
    });
    let mut bytes = Vec::new();
    for capability in &capabilities {
        bytes.extend_from_slice(capability.node_kind.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(capability.id.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(capability.version.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(capability.runtime_api.as_bytes());
        bytes.push(u8::from(capability.available));
        bytes.extend_from_slice(capability.declaration_hash.as_bytes());
        bytes.push(match capability.boundary {
            NodeExecutorBoundary::RuntimeLogic => 0,
            NodeExecutorBoundary::PluginHost => 1,
        });
        match &capability.source {
            NodeExecutorSource::Runtime => bytes.push(0),
            NodeExecutorSource::Plugin { plugin_id } => {
                bytes.push(1);
                bytes.extend_from_slice(plugin_id.as_bytes());
                bytes.push(0);
            }
        }
        for value in &capability.capabilities {
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0);
        }
    }
    ContentHash::digest(&bytes)
}

fn execution_plan_hash(
    plan: &SessionExecutionPlan,
) -> Result<ContentHash, RuntimeExecutabilityError> {
    serde_json::to_vec(plan)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| RuntimeExecutabilityError::InvalidExecutionPlan)
}

fn to_session_resolution(resolved: &ResolvedNodeExecutor) -> SessionNodeExecutorResolution {
    SessionNodeExecutorResolution {
        node_id: resolved.node_id.clone(),
        node_kind: resolved.node_kind.clone(),
        executor_id: resolved.implementation_id.clone(),
        executor_version: resolved.implementation_version.clone(),
        source: to_session_source(&resolved.source),
        boundary: to_session_boundary(resolved.boundary),
        required_capabilities: resolved.required_capabilities.iter().cloned().collect(),
        resolved_capabilities: resolved.resolved_capabilities.iter().cloned().collect(),
        runtime_api_requirement: resolved.runtime_api_requirement.clone(),
        executor_declaration_hash: resolved.executor_declaration_hash,
        adapter_configuration_reference: resolved.adapter_configuration_reference,
    }
}

fn to_session_source(source: &NodeExecutorSource) -> SessionNodeExecutorSource {
    match source {
        NodeExecutorSource::Runtime => SessionNodeExecutorSource::Runtime,
        NodeExecutorSource::Plugin { plugin_id } => SessionNodeExecutorSource::Plugin {
            plugin_id: plugin_id.clone(),
        },
    }
}

fn from_session_source(source: &SessionNodeExecutorSource) -> NodeExecutorSource {
    match source {
        SessionNodeExecutorSource::Runtime => NodeExecutorSource::Runtime,
        SessionNodeExecutorSource::Plugin { plugin_id } => NodeExecutorSource::Plugin {
            plugin_id: plugin_id.clone(),
        },
    }
}

const fn to_session_boundary(boundary: NodeExecutorBoundary) -> SessionNodeExecutorBoundary {
    match boundary {
        NodeExecutorBoundary::RuntimeLogic => SessionNodeExecutorBoundary::RuntimeLogic,
        NodeExecutorBoundary::PluginHost => SessionNodeExecutorBoundary::PluginHost,
    }
}

fn inspect_bound_execution_plan(
    binding: &SessionStyleBinding,
) -> Result<RuntimeExecutabilityReport, RuntimeExecutabilityError> {
    let plan = binding
        .execution_plan
        .clone()
        .ok_or(RuntimeExecutabilityError::InvalidExecutionPlan)?;
    let resolved_nodes = plan
        .nodes
        .iter()
        .map(|node| ResolvedNodeExecutor {
            node_id: node.node_id.clone(),
            node_kind: node.node_kind.clone(),
            implementation_id: node.executor_id.clone(),
            implementation_version: node.executor_version.clone(),
            source: from_session_source(&node.source),
            boundary: match node.boundary {
                SessionNodeExecutorBoundary::RuntimeLogic => NodeExecutorBoundary::RuntimeLogic,
                SessionNodeExecutorBoundary::PluginHost => NodeExecutorBoundary::PluginHost,
            },
            required_capabilities: node.required_capabilities.iter().cloned().collect(),
            resolved_capabilities: node.resolved_capabilities.iter().cloned().collect(),
            runtime_api_requirement: node.runtime_api_requirement.clone(),
            executor_declaration_hash: node.executor_declaration_hash,
            adapter_configuration_reference: node.adapter_configuration_reference,
        })
        .collect();
    Ok(RuntimeExecutabilityReport {
        executable: true,
        registry_hash: plan.registry_hash,
        resolved_nodes,
        execution_plan: Some(plan),
        execution_plan_hash: binding.execution_plan_hash,
        diagnostics: Vec::new(),
    })
}

fn unsupported(
    code: &str,
    node_id: Option<&str>,
    node_kind: Option<&str>,
    message: &str,
) -> RuntimeExecutabilityError {
    RuntimeExecutabilityError::Unsupported {
        diagnostics: vec![diagnostic(code, node_id, node_kind, String::from(message))],
    }
}

fn diagnostic(
    code: &str,
    node_id: Option<&str>,
    node_kind: Option<&str>,
    message: String,
) -> RuntimeExecutabilityDiagnostic {
    RuntimeExecutabilityDiagnostic {
        code: code.to_owned(),
        node_id: node_id.map(str::to_owned),
        node_kind: node_kind.map(str::to_owned),
        message,
    }
}

/// Runtime node-executor registry or executability failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RuntimeExecutabilityError {
    /// Capability data could not be loaded.
    #[error("node-executor capability data failed: {0}")]
    Data(NodeExecutorDataError),
    /// A registration declared an invalid semantic version.
    #[error("node executor `{implementation}` has an invalid semantic version")]
    InvalidRegistrationVersion {
        /// Stable implementation ID.
        implementation: String,
    },
    /// A registration declared an invalid runtime API requirement.
    #[error("node executor `{implementation}` has an invalid runtime API requirement")]
    InvalidRuntimeApiRequirement {
        /// Stable implementation ID.
        implementation: String,
    },
    /// The immutable binding retained an invalid runtime API version.
    #[error("session binding has an invalid runtime API version")]
    InvalidBindingRuntimeApi,
    /// The compiled node kind could not be normalized.
    #[error("compiled node kind could not be normalized")]
    InvalidNodeKind,
    /// A compiled node could not be canonically serialized for binding.
    #[error("compiled node configuration could not be normalized")]
    InvalidNodeConfiguration,
    /// An immutable execution plan could not be canonically serialized.
    #[error("node-execution plan could not be normalized")]
    InvalidExecutionPlan,
    /// The retained compiled style could not be loaded.
    #[error("compiled style cannot be inspected: {0}")]
    CompiledStyle(StyleExecutorError),
    /// The graph is valid but not executable by the current registry/runtime.
    #[error("compiled style is not runtime-executable: {diagnostics:?}")]
    Unsupported {
        /// Stable sorted diagnostics.
        diagnostics: Vec<RuntimeExecutabilityDiagnostic>,
    },
}

#[cfg(test)]
mod tests {
    use agentmod_graph_engine::{ExecutableEdge, ExecutableNode, NodeConfiguration, NodeKind};
    use agentmod_runtime_data::node_executor::{
        RegisterNodeExecutorDataRecord, RuntimeNodeExecutorData,
    };
    use agentmod_runtime_data::plugin::{PluginManifestDataRecord, PluginNodeExecutorDataRecord};
    use agentmod_session_style_sdk::BuiltInStyle;

    use super::*;
    use crate::style_executor::tests::{binding, binding_for_version};

    fn registry_with_parallel_join_available() -> RuntimeNodeExecutorData {
        enable_parallel_join(&RuntimeNodeExecutorData::native().expect("native registry"))
    }

    #[test]
    fn exact_frozen_builtin_versions_bind_generation_two_and_current_versions_bind_three() {
        for style in [
            BuiltInStyle::PersistentChat,
            BuiltInStyle::EphemeralTurn,
            BuiltInStyle::ResearchLoop,
            BuiltInStyle::PlannerWorker,
            BuiltInStyle::DeclarativeGraph,
        ] {
            let legacy = binding_for_version(style, "1.1.0");
            assert_eq!(
                legacy
                    .execution_plan
                    .expect("legacy plan")
                    .compilation
                    .compiler,
                crate::session::EXECUTION_PLAN_COMPILER_V2,
                "{style:?} 1.1.0 must retain frozen adapter recovery"
            );

            let current = binding(style);
            assert_eq!(
                current
                    .execution_plan
                    .expect("current plan")
                    .compilation
                    .compiler,
                crate::session::EXECUTION_PLAN_COMPILER_V3,
                "current {style:?} must use exact generic dispatch"
            );
        }
    }

    #[test]
    fn copied_frozen_identity_outside_builtin_source_stays_generation_three() {
        let registry = RuntimeNodeExecutorData::native().expect("native registry");
        let mut copied = binding_for_version(BuiltInStyle::PersistentChat, "1.1.0");
        copied.source = crate::session::SessionStyleSource::User;
        copied.execution_plan = None;
        copied.execution_plan_hash = None;

        bind_runtime_execution_plan(&registry, &mut copied).expect("bind copied identity");

        assert_eq!(
            copied
                .execution_plan
                .expect("copied plan")
                .compilation
                .compiler,
            crate::session::EXECUTION_PLAN_COMPILER_V3
        );
    }

    fn enable_parallel_join(registry: &RuntimeNodeExecutorData) -> RuntimeNodeExecutorData {
        let registrations = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("registrations")
            .into_iter()
            .map(|record| RegisterNodeExecutorDataRecord {
                id: record.id,
                version: record.version,
                runtime_api: record.runtime_api,
                node_kind: record.node_kind.clone(),
                capabilities: record.capabilities,
                source: record.source,
                boundary: record.boundary,
                available: record.available
                    || matches!(
                        record.node_kind.as_str(),
                        "parallel_branch" | "join_results"
                    ),
                declaration_hash: record.declaration_hash,
            })
            .collect();
        RuntimeNodeExecutorData::new(registrations).expect("test registry")
    }

    fn node(index: usize, id: &str, kind: NodeKind) -> ExecutableNode {
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
            read_variables: BTreeSet::new(),
            write_variables: BTreeSet::new(),
            retry_limit: 0,
            max_iterations: None,
        }
    }

    fn parallel_configuration(join_target: &str) -> NodeConfiguration {
        serde_json::from_value(serde_json::json!({
            "type": "parallel_branch",
            "max_parallelism": 2,
            "max_queue_depth": 2,
            "join_target": join_target,
            "join_policy": "all"
        }))
        .expect("parallel configuration")
    }

    fn join_configuration(required: &[&str]) -> NodeConfiguration {
        serde_json::from_value(serde_json::json!({
            "type": "join_results",
            "required": required,
            "optional": [],
            "minimum_successes": required.len(),
            "failure_policy": "wait_required",
            "ordering_policy": "member_id",
            "timeout_ms": 1000,
            "cancellation_propagates": true,
            "result_projection": "node_references",
            "artifact_collection": "none"
        }))
        .expect("join configuration")
    }

    fn edge(from: usize, to: usize, label: Option<&str>) -> ExecutableEdge {
        ExecutableEdge {
            from,
            to,
            condition: None,
            label: label.map(str::to_owned),
        }
    }

    fn parallel_binding(left_kind: NodeKind, right_kind: NodeKind) -> SessionStyleBinding {
        let mut candidate = binding(BuiltInStyle::PersistentChat);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&candidate.compiled_style_json).expect("compiled style");
        compiled.style_id = String::from("user.renamed.parallel");
        let mut fanout = node(0, "renamed-fanout", NodeKind::ParallelBranch);
        fanout.configuration = Some(parallel_configuration("renamed-gather"));
        let mut gather = node(3, "renamed-gather", NodeKind::JoinResults);
        gather.configuration = Some(join_configuration(&["left-result", "right-result"]));
        compiled.graph.entry_index = 0;
        compiled.graph.nodes = vec![
            fanout,
            node(1, "renamed-left", left_kind),
            node(2, "renamed-right", right_kind),
            gather,
            node(4, "renamed-done", NodeKind::CompleteSession),
        ];
        compiled.graph.edges = vec![
            edge(0, 1, Some("left-result")),
            edge(0, 2, Some("right-result")),
            edge(1, 3, None),
            edge(2, 3, None),
            edge(3, 4, None),
        ];
        candidate.id.clone_from(&compiled.style_id);
        candidate.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        candidate.compiled_style_hash =
            ContentHash::digest(candidate.compiled_style_json.as_bytes());
        candidate.execution_plan = None;
        candidate.execution_plan_hash = None;
        candidate
    }

    fn nested_parallel_binding() -> SessionStyleBinding {
        let mut candidate = parallel_binding(NodeKind::ConditionalBranch, NodeKind::EmitEvent);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&candidate.compiled_style_json).expect("compiled style");
        let mut outer = node(0, "outer-fanout", NodeKind::ParallelBranch);
        outer.configuration = Some(parallel_configuration("outer-gather"));
        let mut inner = node(1, "inner-fanout", NodeKind::ParallelBranch);
        inner.configuration = Some(parallel_configuration("inner-gather"));
        let mut inner_join = node(5, "inner-gather", NodeKind::JoinResults);
        inner_join.configuration = Some(join_configuration(&["inner-a", "inner-b"]));
        let mut outer_join = node(6, "outer-gather", NodeKind::JoinResults);
        outer_join.configuration = Some(join_configuration(&["outer-left", "outer-right"]));
        compiled.graph.nodes = vec![
            outer,
            inner,
            node(2, "outer-right-node", NodeKind::EmitEvent),
            node(3, "inner-left-node", NodeKind::ConditionalBranch),
            node(4, "inner-right-node", NodeKind::PersistArtifact),
            inner_join,
            outer_join,
            node(7, "nested-done", NodeKind::CompleteSession),
        ];
        compiled.graph.edges = vec![
            edge(0, 1, Some("outer-left")),
            edge(0, 2, Some("outer-right")),
            edge(1, 3, Some("inner-a")),
            edge(1, 4, Some("inner-b")),
            edge(2, 6, None),
            edge(3, 5, None),
            edge(4, 5, None),
            edge(5, 6, None),
            edge(6, 7, None),
        ];
        candidate.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        candidate.compiled_style_hash =
            ContentHash::digest(candidate.compiled_style_json.as_bytes());
        candidate
    }

    #[test]
    fn all_live_built_in_profiles_resolve_every_node_deterministically() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        for style in [
            BuiltInStyle::PersistentChat,
            BuiltInStyle::EphemeralTurn,
            BuiltInStyle::ResearchLoop,
            BuiltInStyle::PlannerWorker,
            BuiltInStyle::DeclarativeGraph,
        ] {
            let binding = binding(style);
            let first =
                inspect_runtime_executability(&registry, &binding).expect("first inspection");
            let second =
                inspect_runtime_executability(&registry, &binding).expect("second inspection");
            assert_eq!(first, second);
            assert!(first.executable, "{style:?}: {:?}", first.diagnostics);
            let compiled: agentmod_session_style_sdk::CompiledSessionStyle =
                serde_json::from_str(&binding.compiled_style_json).expect("compiled");
            assert_eq!(first.resolved_nodes.len(), compiled.graph.nodes.len());
        }
    }

    #[test]
    fn invalid_parallel_semantics_are_rejected_but_unknown_topology_is_executable() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let mut unsupported = binding_for_version(BuiltInStyle::PersistentChat, "1.1.0");
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&unsupported.compiled_style_json).expect("compiled");
        compiled.graph.nodes[compiled.graph.entry_index].kind = NodeKind::ParallelBranch;
        unsupported.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        unsupported.compiled_style_hash =
            ContentHash::digest(unsupported.compiled_style_json.as_bytes());
        let report =
            inspect_runtime_executability(&registry, &unsupported).expect("unsupported report");
        assert!(!report.executable);
        assert_eq!(report.diagnostics[0].code, "NODEX008");

        let mut topology = binding_for_version(BuiltInStyle::PersistentChat, "1.1.0");
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&topology.compiled_style_json).expect("compiled");
        compiled
            .graph
            .nodes
            .retain(|node| node.kind != NodeKind::ToolExecutionGate);
        for (index, node) in compiled.graph.nodes.iter_mut().enumerate() {
            node.index = index;
        }
        let entry = compiled
            .graph
            .nodes
            .iter()
            .position(|node| node.kind == NodeKind::ModelCall)
            .expect("model");
        let done = compiled
            .graph
            .nodes
            .iter()
            .position(|node| node.kind == NodeKind::CompleteTurn)
            .expect("done");
        compiled.graph.entry_index = entry;
        compiled.graph.edges = vec![agentmod_graph_engine::ExecutableEdge {
            from: entry,
            to: done,
            condition: None,
            label: None,
        }];
        topology.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        topology.compiled_style_hash = ContentHash::digest(topology.compiled_style_json.as_bytes());
        let report = inspect_runtime_executability(&registry, &topology).expect("topology report");
        assert!(report.executable, "{:?}", report.diagnostics);
        assert!(report.diagnostics.is_empty());
        assert_eq!(report.resolved_nodes.len(), 2);
    }

    #[test]
    fn arbitrary_renamed_supported_parallel_region_passes_normal_admission() {
        let registry = registry_with_parallel_join_available();
        let candidate = parallel_binding(NodeKind::ConditionalBranch, NodeKind::PersistArtifact);
        let executor =
            CompiledStyleExecutor::from_unbound_binding(&candidate).expect("compiled executor");
        assert_eq!(executor.adapter_kind(), None);

        let first = inspect_runtime_executability(&registry, &candidate).expect("first inspection");
        let second =
            inspect_runtime_executability(&registry, &candidate).expect("second inspection");
        assert_eq!(first, second);
        assert!(first.executable, "{:?}", first.diagnostics);
        assert!(first.execution_plan.is_some());
        assert!(first.diagnostics.is_empty());
    }

    #[test]
    fn every_live_branch_executor_class_passes_parallel_admission() {
        let registry = registry_with_parallel_join_available();
        for kind in [
            NodeKind::SpawnChildAgent,
            NodeKind::ConditionalBranch,
            NodeKind::Loop,
            NodeKind::Fail,
            NodeKind::EmitEvent,
            NodeKind::Delay,
            NodeKind::Schedule,
            NodeKind::ToolExecutionGate,
            NodeKind::UserApproval,
            NodeKind::SendChildAgentMessage,
            NodeKind::WaitForAgents,
            NodeKind::PersistArtifact,
        ] {
            let report = inspect_runtime_executability(
                &registry,
                &parallel_binding(kind, NodeKind::EmitEvent),
            )
            .expect("supported branch inspection");
            assert!(report.executable, "{kind:?}: {:?}", report.diagnostics);
        }
    }

    #[test]
    fn exact_persisted_plan_revalidation_reapplies_parallel_admission() {
        let registry = registry_with_parallel_join_available();
        let mut candidate =
            parallel_binding(NodeKind::ConditionalBranch, NodeKind::PersistArtifact);
        bind_runtime_execution_plan(&registry, &mut candidate).expect("bind supported plan");

        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&candidate.compiled_style_json).expect("compiled");
        let left = compiled
            .graph
            .nodes
            .iter_mut()
            .find(|node| node.id == "renamed-left")
            .expect("left branch");
        left.kind = NodeKind::ModelCall;
        let left_configuration_hash =
            ContentHash::digest(&serde_json::to_vec(&*left).expect("node JSON"));
        candidate.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        candidate.compiled_style_hash =
            ContentHash::digest(candidate.compiled_style_json.as_bytes());

        let registration = inspect_node_executor_capabilities(&registry)
            .expect("capabilities")
            .into_iter()
            .find(|capability| capability.id == "runtime.model-request")
            .expect("model executor");
        let plan = candidate.execution_plan.as_mut().expect("plan");
        plan.compilation.compiled_style_hash = candidate.compiled_style_hash;
        let resolution = plan
            .nodes
            .iter_mut()
            .find(|resolution| resolution.node_id == "renamed-left")
            .expect("left resolution");
        resolution.node_kind = String::from("model_call");
        resolution.executor_id = registration.id;
        resolution.executor_version = registration.version;
        resolution.source = to_session_source(&registration.source);
        resolution.boundary = to_session_boundary(registration.boundary);
        resolution.required_capabilities.clear();
        resolution.resolved_capabilities = registration.capabilities.into_iter().collect();
        resolution.runtime_api_requirement = registration.runtime_api;
        resolution.executor_declaration_hash = registration.declaration_hash;
        resolution.adapter_configuration_reference = left_configuration_hash;
        candidate.execution_plan_hash = Some(execution_plan_hash(plan).expect("updated plan hash"));

        let error = revalidate_runtime_execution_plan(&registry, &candidate)
            .expect_err("parallel admission is revalidated");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics.len() == 1
                    && diagnostics[0].code == "NODEX009"
                    && diagnostics[0].node_id.as_deref() == Some("renamed-left")
        ));
    }

    #[test]
    fn unsupported_native_executor_classes_are_rejected_inside_parallel_regions() {
        let registry = registry_with_parallel_join_available();
        for (kind, expected_executor) in [
            (NodeKind::ContextTransform, "runtime.context-construction"),
            (NodeKind::ModelCall, "runtime.model-request"),
            (NodeKind::Review, "runtime.review"),
            (NodeKind::JoinResults, "runtime.join"),
            (NodeKind::CompleteTurn, "runtime.turn-completion"),
            (NodeKind::CompleteSession, "runtime.session-completion"),
        ] {
            let candidate = parallel_binding(kind, NodeKind::EmitEvent);
            let report = inspect_runtime_executability(&registry, &candidate).expect("inspection");
            assert!(!report.executable, "{kind:?}");
            assert!(report.execution_plan.is_none(), "{kind:?}");
            assert_eq!(report.diagnostics.len(), 1, "{kind:?}");
            let diagnostic = &report.diagnostics[0];
            assert_eq!(diagnostic.code, "NODEX009", "{kind:?}");
            assert_eq!(diagnostic.node_id.as_deref(), Some("renamed-left"));
            assert!(
                diagnostic.message.contains(expected_executor),
                "{kind:?}: {}",
                diagnostic.message
            );
        }
    }

    #[test]
    fn nested_parallel_and_join_are_rejected_by_exact_outer_region_membership() {
        let registry = registry_with_parallel_join_available();
        let report = inspect_runtime_executability(&registry, &nested_parallel_binding())
            .expect("nested inspection");
        assert!(!report.executable);
        let rejected = report
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code == "NODEX009")
            .map(|diagnostic| diagnostic.node_id.as_deref().expect("node ID"))
            .collect::<BTreeSet<_>>();
        assert_eq!(rejected, BTreeSet::from(["inner-fanout", "inner-gather"]));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one table-driven regression retains the exact accepted plugin resolution and all fail-closed tamper variants together"
    )]
    fn exact_plugin_host_executor_is_accepted_inside_parallel_region() {
        let declaration_hash = ContentHash::digest(b"parallel-plugin-declaration");
        let plugin = PluginManifestDataRecord {
            id: String::from("fixture.parallel"),
            version: String::from("1.0.0"),
            category: String::from("graph_node"),
            class: String::from("blocking"),
            provided_capabilities: BTreeSet::new(),
            subscribed_events: BTreeSet::new(),
            timeout_ms: 1_000,
            failure_policy: String::from("reject"),
            canonical_manifest_json: String::from("{}"),
            configuration: serde_json::json!({}),
            configuration_reference: ContentHash::digest(b"{}"),
            node_executors: vec![PluginNodeExecutorDataRecord {
                plugin_version: String::from("1.0.0"),
                executor_id: String::from("fixture.parallel-event"),
                version: String::from("1.0.0"),
                runtime_api: String::from("^1.0"),
                node_kind: String::from("emit_event"),
                handler: String::from("emit"),
                capabilities: BTreeSet::from([String::from("plugin.parallel")]),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 500,
                failure_policy: String::from("reject"),
                max_attempts: 1,
                retry_backoff_ms: 0,
                idempotent: true,
                tool_permissions: BTreeSet::new(),
                network_permissions: BTreeSet::new(),
                state_scope: String::from("invocation"),
                external_effects: false,
                declaration_hash,
            }],
            context_transforms: Vec::new(),
            memory_providers: Vec::new(),
            compactors: Vec::new(),
        };
        let registry = enable_parallel_join(
            &RuntimeNodeExecutorData::native_with_plugins(&[plugin]).expect("plugin registry"),
        );
        let mut candidate = parallel_binding(NodeKind::EmitEvent, NodeKind::PersistArtifact);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&candidate.compiled_style_json).expect("compiled");
        compiled
            .allowed_plugins
            .push(String::from("fixture.parallel"));
        compiled
            .graph
            .nodes
            .iter_mut()
            .find(|node| node.id == "renamed-left")
            .expect("left branch")
            .required_capabilities
            .insert(String::from("plugin.parallel"));
        candidate.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        candidate.compiled_style_hash =
            ContentHash::digest(candidate.compiled_style_json.as_bytes());

        let report =
            inspect_runtime_executability(&registry, &candidate).expect("plugin inspection");
        assert!(report.executable, "{:?}", report.diagnostics);
        assert!(report.diagnostics.is_empty());
        let plan = report.execution_plan.expect("parallel plugin plan");
        let selected = plan
            .nodes
            .iter()
            .find(|node| node.node_id == "renamed-left")
            .expect("plugin branch selection");
        assert_eq!(selected.executor_id, "fixture.parallel-event");
        assert_eq!(selected.executor_version, "1.0.0");
        assert!(matches!(
            &selected.source,
            SessionNodeExecutorSource::Plugin { plugin_id }
                if plugin_id == "fixture.parallel"
        ));
        assert_eq!(selected.boundary, SessionNodeExecutorBoundary::PluginHost);

        for drift in [
            "source",
            "boundary",
            "plugin_identity",
            "api",
            "capabilities",
        ] {
            let mut drifted = candidate.clone();
            bind_runtime_execution_plan(&registry, &mut drifted).expect("bind plugin plan");
            let plan = drifted.execution_plan.as_mut().expect("bound plan");
            let selected = plan
                .nodes
                .iter_mut()
                .find(|node| node.node_id == "renamed-left")
                .expect("plugin branch selection");
            match drift {
                "source" => selected.source = SessionNodeExecutorSource::Runtime,
                "boundary" => {
                    selected.boundary = SessionNodeExecutorBoundary::RuntimeLogic;
                }
                "plugin_identity" => {
                    selected.source = SessionNodeExecutorSource::Plugin {
                        plugin_id: String::from("fixture.substituted"),
                    };
                }
                "api" => selected.runtime_api_requirement = String::from("^9.0"),
                "capabilities" => selected
                    .resolved_capabilities
                    .push(String::from("plugin.substituted")),
                _ => unreachable!(),
            }
            drifted.execution_plan_hash =
                Some(execution_plan_hash(plan).expect("drifted plan hash"));
            let error = revalidate_runtime_execution_plan(&registry, &drifted)
                .expect_err("mixed plugin identity must fail closed");
            assert!(
                matches!(
                    error,
                    RuntimeExecutabilityError::Unsupported { ref diagnostics }
                        if diagnostics.len() == 1
                            && matches!(diagnostics[0].code.as_str(), "NODEX105" | "NODEX106")
                            && diagnostics[0].node_id.as_deref() == Some("renamed-left")
                ),
                "{drift}: {error:?}"
            );
        }
    }

    #[test]
    fn missing_or_tampered_join_association_has_one_stable_diagnostic() {
        let registry = registry_with_parallel_join_available();
        let mut missing = parallel_binding(NodeKind::ConditionalBranch, NodeKind::PersistArtifact);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&missing.compiled_style_json).expect("compiled");
        compiled
            .graph
            .nodes
            .iter_mut()
            .find(|node| node.id == "renamed-fanout")
            .expect("fanout")
            .configuration = Some(parallel_configuration("missing-gather"));
        missing.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        missing.compiled_style_hash = ContentHash::digest(missing.compiled_style_json.as_bytes());
        let report =
            inspect_runtime_executability(&registry, &missing).expect("missing join inspection");
        assert_eq!(report.diagnostics.len(), 1);
        assert_eq!(report.diagnostics[0].code, "NODEX008");
        assert_eq!(
            report.diagnostics[0].message,
            "parallel node `renamed-fanout` has no exact join association with `missing-gather`: configured join target is missing"
        );

        let mut tampered = parallel_binding(NodeKind::ConditionalBranch, NodeKind::PersistArtifact);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&tampered.compiled_style_json).expect("compiled");
        compiled
            .graph
            .nodes
            .iter_mut()
            .find(|node| node.id == "renamed-gather")
            .expect("join")
            .configuration = Some(join_configuration(&["substituted-left", "right-result"]));
        tampered.compiled_style_json =
            serde_json::to_string(&compiled).expect("compiled serialization");
        tampered.compiled_style_hash = ContentHash::digest(tampered.compiled_style_json.as_bytes());
        let report =
            inspect_runtime_executability(&registry, &tampered).expect("tampered join inspection");
        assert_eq!(report.diagnostics.len(), 1);
        assert_eq!(report.diagnostics[0].code, "NODEX008");
        assert!(
            report.diagnostics[0]
                .message
                .ends_with("parallel fan-out labels do not match the configured join members")
        );
    }

    #[test]
    fn parallel_diagnostic_order_and_json_are_stable() {
        let registry = registry_with_parallel_join_available();
        let candidate = parallel_binding(NodeKind::ModelCall, NodeKind::ContextTransform);
        let first = inspect_runtime_executability(&registry, &candidate).expect("first inspection");
        let second =
            inspect_runtime_executability(&registry, &candidate).expect("second inspection");
        assert_eq!(first.diagnostics, second.diagnostics);
        assert_eq!(
            first
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.node_id.as_deref().expect("node"))
                .collect::<Vec<_>>(),
            ["renamed-left", "renamed-right"]
        );
        assert_eq!(
            serde_json::to_string(&first.diagnostics).expect("diagnostic JSON"),
            serde_json::to_string(&second.diagnostics).expect("diagnostic JSON")
        );
    }

    #[test]
    fn allowed_plugin_executor_resolves_from_the_single_registry() {
        let declaration_hash = ContentHash::digest(b"fixture-node-declaration");
        let plugin = PluginManifestDataRecord {
            id: String::from("fixture.node"),
            version: String::from("1.0.0"),
            category: String::from("graph_node"),
            class: String::from("blocking"),
            provided_capabilities: BTreeSet::new(),
            subscribed_events: BTreeSet::new(),
            timeout_ms: 1_000,
            failure_policy: String::from("reject"),
            canonical_manifest_json: String::from("{}"),
            configuration: serde_json::json!({}),
            configuration_reference: ContentHash::digest(b"{}"),
            node_executors: vec![PluginNodeExecutorDataRecord {
                plugin_version: String::from("1.0.0"),
                executor_id: String::from("fixture.model"),
                version: String::from("2.0.0"),
                runtime_api: String::from("^1.0"),
                node_kind: String::from("model_call"),
                handler: String::from("execute_model"),
                capabilities: BTreeSet::from([String::from("model"), String::from("plugin.model")]),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 500,
                failure_policy: String::from("reject"),
                max_attempts: 1,
                retry_backoff_ms: 0,
                idempotent: false,
                tool_permissions: BTreeSet::new(),
                network_permissions: BTreeSet::new(),
                state_scope: String::from("invocation"),
                external_effects: false,
                declaration_hash,
            }],
            context_transforms: Vec::new(),
            memory_providers: Vec::new(),
            compactors: Vec::new(),
        };
        let registry =
            RuntimeNodeExecutorData::native_with_plugins(&[plugin]).expect("combined registry");
        let mut candidate = binding(BuiltInStyle::PersistentChat);
        let mut compiled: agentmod_session_style_sdk::CompiledSessionStyle =
            serde_json::from_str(&candidate.compiled_style_json).expect("compiled");
        compiled.allowed_plugins.push(String::from("fixture.node"));
        compiled
            .graph
            .nodes
            .iter_mut()
            .find(|node| node.kind == NodeKind::ModelCall)
            .expect("model")
            .required_capabilities
            .insert(String::from("plugin.model"));
        candidate.compiled_style_json = serde_json::to_string(&compiled).expect("compiled json");
        candidate.compiled_style_hash =
            ContentHash::digest(candidate.compiled_style_json.as_bytes());
        let report = inspect_runtime_executability(&registry, &candidate).expect("inspection");
        assert!(report.executable, "{:?}", report.diagnostics);
        let resolved = report
            .resolved_nodes
            .iter()
            .find(|node| node.node_kind == "model_call")
            .expect("model resolution");
        assert_eq!(resolved.implementation_id, "fixture.model");
        assert_eq!(resolved.implementation_version, "2.0.0");
        assert_eq!(
            resolved.source,
            NodeExecutorSource::Plugin {
                plugin_id: String::from("fixture.node"),
            }
        );
        assert_eq!(resolved.boundary, NodeExecutorBoundary::PluginHost);
        assert_eq!(resolved.executor_declaration_hash, declaration_hash);
    }

    #[test]
    fn persisted_plan_round_trips_and_exact_executor_revalidates() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let binding = binding(BuiltInStyle::PersistentChat);
        revalidate_runtime_execution_plan(&registry, &binding).expect("exact revalidation");
        let encoded = serde_json::to_string(&binding).expect("serialize binding");
        let decoded: SessionStyleBinding =
            serde_json::from_str(&encoded).expect("deserialize binding");
        assert_eq!(decoded, binding);
        revalidate_runtime_execution_plan(&registry, &decoded).expect("round-trip revalidation");
        assert_eq!(
            decoded.execution_plan_hash,
            decoded
                .execution_plan
                .as_ref()
                .map(execution_plan_hash)
                .transpose()
                .expect("plan hash")
        );
    }

    #[test]
    fn tool_gate_selects_alias_bound_version_and_historical_version_revalidates_exactly() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let records = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("records");
        let historical = records
            .iter()
            .find(|record| record.id == "runtime.tool-gate" && record.version == "1.0.0")
            .expect("historical tool gate");
        let current = records
            .iter()
            .find(|record| record.id == "runtime.tool-gate" && record.version == "1.1.0")
            .expect("alias-bound tool gate");
        assert_ne!(historical.declaration_hash, current.declaration_hash);

        let current_binding = binding(BuiltInStyle::PersistentChat);
        let current_resolution = current_binding
            .execution_plan
            .as_ref()
            .expect("plan")
            .nodes
            .iter()
            .find(|node| node.executor_id == "runtime.tool-gate")
            .expect("tool resolution");
        assert_eq!(current_resolution.executor_version, "1.1.0");
        assert_eq!(
            current_resolution.executor_declaration_hash,
            current.declaration_hash
        );
        revalidate_runtime_execution_plan(&registry, &current_binding)
            .expect("current restart revalidation");

        let mut historical_binding = current_binding;
        let plan = historical_binding.execution_plan.as_mut().expect("plan");
        let historical_resolution = plan
            .nodes
            .iter_mut()
            .find(|node| node.executor_id == "runtime.tool-gate")
            .expect("tool resolution");
        historical_resolution.executor_version = historical.version.clone();
        historical_resolution.executor_declaration_hash = historical.declaration_hash;
        historical_binding.execution_plan_hash =
            Some(execution_plan_hash(plan).expect("historical plan hash"));
        revalidate_runtime_execution_plan(&registry, &historical_binding)
            .expect("historical exact version remains restart-compatible");
    }

    #[test]
    fn configuration_abi_selects_new_versions_without_rebinding_historical_shapes() {
        let historical = binding_for_version(BuiltInStyle::PlannerWorker, "1.3.0");
        let historical_plan = historical.execution_plan.as_ref().expect("historical plan");
        assert!(historical_plan.nodes.iter().all(|node| {
            node.executor_version == "1.0.0"
                || (node.executor_id == "runtime.tool-gate" && node.executor_version == "1.1.0")
        }));

        let current = binding_for_version(BuiltInStyle::PlannerWorker, "1.4.0");
        let current_plan = current.execution_plan.as_ref().expect("current plan");
        let resolution = |node_id: &str| {
            current_plan
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .expect("node resolution")
        };
        assert_eq!(resolution("plan").executor_version, "1.0.0");
        assert_eq!(resolution("spawn-planner").executor_version, "1.1.0");
        assert_eq!(resolution("spawn-evidence").executor_version, "1.1.0");
        assert_eq!(resolution("integrate").executor_version, "1.1.0");
        assert_eq!(resolution("review").executor_version, "1.1.0");

        let research = binding_for_version(BuiltInStyle::ResearchLoop, "1.3.0");
        let research_plan = research.execution_plan.as_ref().expect("research plan");
        assert_eq!(
            research_plan
                .nodes
                .iter()
                .find(|node| node.node_id == "persist-evidence")
                .expect("artifact resolution")
                .executor_version,
            "1.1.0"
        );
        assert_eq!(
            research_plan
                .nodes
                .iter()
                .find(|node| node.node_id == "research")
                .expect("model resolution")
                .executor_version,
            "1.1.0"
        );
    }

    #[test]
    fn tool_catalog_abi_hash_drift_fails_restart_without_rebinding() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let binding = binding(BuiltInStyle::PersistentChat);
        let mut registrations = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("records")
            .into_iter()
            .map(|record| RegisterNodeExecutorDataRecord {
                id: record.id,
                version: record.version,
                runtime_api: record.runtime_api,
                node_kind: record.node_kind,
                capabilities: record.capabilities,
                source: record.source,
                boundary: record.boundary,
                available: record.available,
                declaration_hash: record.declaration_hash,
            })
            .collect::<Vec<_>>();
        registrations
            .iter_mut()
            .find(|record| record.id == "runtime.tool-gate" && record.version == "1.1.0")
            .expect("alias-bound tool gate")
            .declaration_hash = ContentHash::digest(b"changed-tool-catalog-abi");
        let drifted = RuntimeNodeExecutorData::new(registrations).expect("drifted registry");

        let error = revalidate_runtime_execution_plan(&drifted, &binding)
            .expect_err("tool catalog drift must fail closed");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics[0].code == "NODEX103"
        ));
        assert_eq!(
            binding
                .execution_plan
                .as_ref()
                .expect("plan")
                .nodes
                .iter()
                .find(|node| node.executor_id == "runtime.tool-gate")
                .expect("tool resolution")
                .executor_version,
            "1.1.0",
            "restart validation must not silently rebind"
        );
    }

    #[test]
    fn generation_two_and_three_revalidate_exactly_but_unknown_generation_fails() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let generation_three = binding(BuiltInStyle::PersistentChat);
        assert_eq!(
            generation_three
                .execution_plan
                .as_ref()
                .expect("plan")
                .compilation
                .compiler,
            crate::session::EXECUTION_PLAN_COMPILER_V3
        );
        revalidate_runtime_execution_plan(&registry, &generation_three).expect("generation three");

        let mut generation_two = generation_three.clone();
        generation_two
            .execution_plan
            .as_mut()
            .expect("plan")
            .compilation
            .compiler = String::from(crate::session::EXECUTION_PLAN_COMPILER_V2);
        generation_two.execution_plan_hash = Some(
            execution_plan_hash(generation_two.execution_plan.as_ref().expect("plan"))
                .expect("generation two hash"),
        );
        revalidate_runtime_execution_plan(&registry, &generation_two).expect("generation two");
        assert_eq!(
            generation_two.execution_plan.as_ref().expect("plan").nodes,
            generation_three
                .execution_plan
                .as_ref()
                .expect("plan")
                .nodes
        );

        let mut unknown = generation_three;
        unknown
            .execution_plan
            .as_mut()
            .expect("plan")
            .compilation
            .compiler = String::from("agentmod-runtime-node-plan@99");
        unknown.execution_plan_hash = Some(
            execution_plan_hash(unknown.execution_plan.as_ref().expect("plan"))
                .expect("unknown hash"),
        );
        let error =
            revalidate_runtime_execution_plan(&registry, &unknown).expect_err("unknown generation");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics[0].code == "NODEX104"
        ));
    }

    #[test]
    fn registry_drift_and_executor_version_drift_fail_without_rebinding() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let mut registry_drift = binding(BuiltInStyle::PersistentChat);
        registry_drift
            .execution_plan
            .as_mut()
            .expect("plan")
            .registry_hash = ContentHash::digest(b"different-registry");
        registry_drift.execution_plan_hash = Some(
            execution_plan_hash(registry_drift.execution_plan.as_ref().expect("plan"))
                .expect("plan hash"),
        );
        let error = revalidate_runtime_execution_plan(&registry, &registry_drift)
            .expect_err("registry drift");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics[0].code == "NODEX103"
        ));

        let mut executor_drift = binding(BuiltInStyle::PersistentChat);
        executor_drift.execution_plan.as_mut().expect("plan").nodes[0].executor_version =
            String::from("99.0.0");
        executor_drift.execution_plan_hash = Some(
            execution_plan_hash(executor_drift.execution_plan.as_ref().expect("plan"))
                .expect("plan hash"),
        );
        let error = revalidate_runtime_execution_plan(&registry, &executor_drift)
            .expect_err("executor drift");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics[0].code == "NODEX105"
        ));
        assert_eq!(
            executor_drift.execution_plan.as_ref().expect("plan").nodes[0].executor_version,
            "99.0.0"
        );
    }

    #[test]
    fn executor_declaration_drift_and_legacy_missing_hash_require_migration() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let mut declaration_drift = binding(BuiltInStyle::PersistentChat);
        declaration_drift
            .execution_plan
            .as_mut()
            .expect("plan")
            .nodes[0]
            .executor_declaration_hash = ContentHash::digest(b"substituted-declaration");
        declaration_drift.execution_plan_hash = Some(
            execution_plan_hash(declaration_drift.execution_plan.as_ref().expect("plan"))
                .expect("plan hash"),
        );
        let error = revalidate_runtime_execution_plan(&registry, &declaration_drift)
            .expect_err("declaration drift");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics[0].code == "NODEX106"
        ));

        let current = binding(BuiltInStyle::PersistentChat);
        let mut legacy_json = serde_json::to_value(&current).expect("binding json");
        let nodes = legacy_json
            .pointer_mut("/execution_plan/nodes")
            .and_then(serde_json::Value::as_array_mut)
            .expect("plan nodes");
        for node in nodes {
            node.as_object_mut()
                .expect("node")
                .remove("executor_declaration_hash");
        }
        let legacy: SessionStyleBinding =
            serde_json::from_value(legacy_json).expect("legacy compatibility decode");
        assert!(
            legacy
                .execution_plan
                .as_ref()
                .expect("plan")
                .nodes
                .iter()
                .all(|node| { node.executor_declaration_hash == ContentHash::from_bytes([0; 32]) })
        );
        let error =
            revalidate_runtime_execution_plan(&registry, &legacy).expect_err("legacy migration");
        assert!(matches!(
            error,
            RuntimeExecutabilityError::Unsupported { ref diagnostics }
                if diagnostics[0].code == "NODEX107"
        ));
    }
}
