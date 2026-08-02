//! Runtime-owned node-executor resolution and executability validation.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_primitives::ContentHash;
use agentmod_runtime_data::node_executor::{
    ListNodeExecutorsDataRequest, NodeExecutorBoundaryData, NodeExecutorDataError,
    NodeExecutorDataPort, NodeExecutorSourceData,
};
use semver::{Version, VersionReq};
use thiserror::Error;

use crate::{
    session::SessionStyleBinding,
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
    /// Execution boundary.
    pub boundary: NodeExecutorBoundary,
}

/// Stable runtime-executability diagnostic.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
    /// Whether every node and the complete topology can execute now.
    pub executable: bool,
    /// Hash of the exact sorted capability registry used for resolution.
    pub registry_hash: ContentHash,
    /// Exact per-node resolutions.
    pub resolved_nodes: Vec<ResolvedNodeExecutor>,
    /// Stable sorted diagnostics that block execution.
    pub diagnostics: Vec<RuntimeExecutabilityDiagnostic>,
    /// Stable sorted non-blocking advisories.
    pub advisory_diagnostics: Vec<RuntimeExecutabilityDiagnostic>,
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
    let executor = CompiledStyleExecutor::from_binding(binding)
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
    let mut advisory_diagnostics = Vec::new();
    for node in &executor.compiled().graph.nodes {
        match resolve_node(node, executor.compiled(), &by_kind, &runtime_api)? {
            Ok(resolved) => resolved_nodes.push(resolved),
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
    }

    // Topology classification is no longer a condition of runtime
    // executability: a graph executes because every node has an available
    // resolved executor. The legacy adapter profile is retained only as a
    // temporary compatibility advisory so operators can see that the generic
    // dispatch path will be used.
    if diagnostics.is_empty() && executor.adapter_kind().is_none() {
        advisory_diagnostics.push(diagnostic(
            "NODEX007",
            None,
            None,
            String::from(
                "all nodes resolve; this compiled topology has no legacy adapter profile and will use generic node dispatch",
            ),
        ));
    }
    diagnostics.sort();
    advisory_diagnostics.sort();
    resolved_nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(RuntimeExecutabilityReport {
        executable: diagnostics.is_empty(),
        registry_hash,
        resolved_nodes,
        diagnostics,
        advisory_diagnostics,
    })
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
        .collect::<Vec<_>>();
    match compatible.as_slice() {
        [] => Ok(Err(diagnostic(
            "NODEX003",
            Some(&node.id),
            Some(&node_kind),
            format!("registered node executors for `{node_kind}` are unavailable or incompatible"),
        ))),
        [registration] if registration.boundary == NodeExecutorBoundary::PluginHost => {
            Ok(Err(diagnostic(
                "NODEX005",
                Some(&node.id),
                Some(&node_kind),
                format!(
                    "plugin node executor `{}` is registered but plugin-node dispatch is not enabled",
                    registration.id
                ),
            )))
        }
        [registration] => Ok(Ok(ResolvedNodeExecutor {
            node_id: node.id.clone(),
            node_kind,
            implementation_id: registration.id.clone(),
            implementation_version: registration.version.clone(),
            boundary: registration.boundary,
        })),
        _ => Ok(Err(diagnostic(
            "NODEX004",
            Some(&node.id),
            Some(&node_kind),
            format!("more than one compatible node executor can execute `{node_kind}`"),
        ))),
    }
}

/// Requires a compiled binding to be executable before canonical state can be
/// created or branched.
///
/// # Errors
///
/// Returns [`RuntimeExecutabilityError::Unsupported`] with deterministic
/// diagnostics when execution is not available.
pub(crate) fn validate_runtime_executability<D: NodeExecutorDataPort>(
    data: &D,
    binding: &SessionStyleBinding,
) -> Result<RuntimeExecutabilityReport, RuntimeExecutabilityError> {
    let report = inspect_runtime_executability(data, binding)?;
    if report.executable {
        Ok(report)
    } else {
        Err(RuntimeExecutabilityError::Unsupported {
            diagnostics: report.diagnostics,
        })
    }
}

/// Converts exact resolved executor records into the generic dispatch plan.
///
/// This is the integration seam Task 1 persists as the immutable execution
/// plan: dispatch is driven by these exact identities, never by topology
/// profiles. The plan maps compiled node ID to the exact executor identity
/// chosen from the capability registry at creation time.
///
/// Exercised by focused dispatch tests; consumed by Task 1 persistence when
/// the immutable execution plan is retained in the session binding.
#[must_use]
#[allow(
    dead_code,
    reason = "Task 1 execution-plan persistence seam; exercised by dispatch_tests"
)]
pub(crate) fn dispatch_plan(
    report: &RuntimeExecutabilityReport,
) -> std::collections::BTreeMap<String, crate::node_execution::NodeExecutorIdentity> {
    report
        .resolved_nodes
        .iter()
        .map(|resolved| {
            (
                resolved.node_id.clone(),
                crate::node_execution::NodeExecutorIdentity::from_resolved(resolved),
            )
        })
        .collect()
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
    let mut bytes = Vec::new();
    for capability in capabilities {
        bytes.extend_from_slice(capability.node_kind.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(capability.id.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(capability.version.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(capability.runtime_api.as_bytes());
        bytes.push(u8::from(capability.available));
        bytes.push(match capability.boundary {
            NodeExecutorBoundary::RuntimeLogic => 0,
            NodeExecutorBoundary::PluginHost => 1,
        });
        for value in &capability.capabilities {
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0);
        }
    }
    ContentHash::digest(&bytes)
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
    use agentmod_graph_engine::NodeKind;
    use agentmod_runtime_data::node_executor::RuntimeNodeExecutorData;
    use agentmod_session_style_sdk::BuiltInStyle;

    use super::*;
    use crate::style_executor::tests::binding;

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
    fn unsupported_node_and_supported_nodes_in_unknown_topology_are_distinct() {
        let registry = RuntimeNodeExecutorData::native().expect("registry");
        let mut unsupported = binding(BuiltInStyle::PersistentChat);
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
        assert_eq!(report.diagnostics[0].code, "NODEX003");

        // An unknown topology whose nodes all resolve is executable through
        // generic node dispatch: the legacy adapter profile is no longer a
        // condition of runtime executability.
        let mut topology = binding(BuiltInStyle::PersistentChat);
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
        assert_eq!(report.resolved_nodes.len(), 2);
        assert_eq!(report.diagnostics.len(), 0);
        assert_eq!(report.advisory_diagnostics[0].code, "NODEX007");

        // The exact resolved identities form the dispatch plan: every compiled
        // node has a resolved executor without any adapter profile.
        let plan = dispatch_plan(&report);
        assert_eq!(plan.len(), 2);
        assert!(plan.contains_key("respond"));
        assert!(plan.contains_key("done"));
        let identities: Vec<_> = plan.into_values().collect();
        for identity in identities {
            assert_eq!(
                identity.boundary,
                crate::node_execution::ExecutorBoundary::RuntimeLogic
            );
        }
    }
}
