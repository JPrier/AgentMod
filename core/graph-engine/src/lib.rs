//! Versioned, deterministic compilation of generic `AgentMod` execution graphs.
//!
//! This crate validates structure and compiles inspectable execution data. It
//! deliberately assigns no runtime-specific behavior to graph node kinds.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use agentmod_expression_engine::{Expression, ExpressionLimits, ParseError};
use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Current human-editable graph format version.
pub const GRAPH_FORMAT_VERSION: u16 = 1;

/// Bounds applied while parsing and compiling an untrusted graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompilerLimits {
    /// Maximum TOML source bytes.
    pub max_source_bytes: usize,
    /// Maximum nodes.
    pub max_nodes: usize,
    /// Maximum edges.
    pub max_edges: usize,
    /// Maximum UTF-8 bytes in an identifier or declaration.
    pub max_name_bytes: usize,
    /// Maximum retry count on one node.
    pub max_retry_limit: u32,
    /// Maximum static loop iterations.
    pub max_loop_iterations: u32,
    /// Maximum graph step budget.
    pub max_steps: u64,
    /// Maximum graph token budget.
    pub max_tokens: u64,
    /// Maximum graph cost budget in micros.
    pub max_cost_micros: u64,
    /// Maximum graph duration budget in milliseconds.
    pub max_duration_ms: u64,
    /// Limits for every embedded condition.
    pub expression: ExpressionLimits,
}

impl Default for CompilerLimits {
    fn default() -> Self {
        Self {
            max_source_bytes: 1024 * 1024,
            max_nodes: 10_000,
            max_edges: 50_000,
            max_name_bytes: 256,
            max_retry_limit: 32,
            max_loop_iterations: 10_000,
            max_steps: 10_000_000,
            max_tokens: 10_000_000_000,
            max_cost_micros: 1_000_000_000_000,
            max_duration_ms: 365 * 24 * 60 * 60 * 1_000,
            expression: ExpressionLimits::default(),
        }
    }
}

/// Inputs that bind compiled graph cache entries to runtime compatibility.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphCacheInputs {
    /// Hash of the validated plugin set.
    pub plugin_set_hash: ContentHash,
    /// Runtime API version used to interpret nodes.
    pub runtime_api_version: String,
    /// Actual runtime capability set.
    pub capability_set: BTreeSet<String>,
}

/// Parsed versioned graph source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDefinition {
    /// Source format version.
    pub format_version: u16,
    /// Stable entry node ID.
    pub entry: String,
    /// Hard execution budgets.
    pub budget: GraphBudget,
    /// Capabilities and implementations the graph declares.
    #[serde(default)]
    pub declarations: GraphDeclarations,
    /// Generic node definitions.
    pub nodes: Vec<NodeDefinition>,
    /// Directed transitions.
    pub edges: Vec<EdgeDefinition>,
}

impl GraphDefinition {
    /// Parses versioned TOML while applying source-size bounds.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError`] when the source is too large or is not valid
    /// versioned graph TOML.
    pub fn parse(source: &str, limits: CompilerLimits) -> Result<Self, GraphError> {
        if source.len() > limits.max_source_bytes {
            return Err(GraphError::SourceTooLarge {
                actual: source.len(),
                maximum: limits.max_source_bytes,
            });
        }
        toml::from_str(source).map_err(|error| GraphError::InvalidToml {
            detail: error.message().to_owned(),
        })
    }
}

/// Hard execution budgets declared by a graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphBudget {
    /// Maximum node transitions.
    pub max_steps: u64,
    /// Maximum provider tokens.
    pub max_tokens: u64,
    /// Maximum provider cost in micros of the configured currency.
    pub max_cost_micros: u64,
    /// Maximum wall-clock execution duration.
    pub max_duration_ms: u64,
}

/// Graph-level implementation declarations.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphDeclarations {
    /// Capability names.
    #[serde(default)]
    pub capabilities: BTreeSet<String>,
    /// Tool names.
    #[serde(default)]
    pub tools: BTreeSet<String>,
    /// Provider names.
    #[serde(default)]
    pub providers: BTreeSet<String>,
}

/// Generic graph node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDefinition {
    /// Stable node ID.
    pub id: String,
    /// Generic node kind.
    pub kind: NodeKind,
    /// Optional condition evaluated before runtime execution.
    #[serde(default)]
    pub condition: Option<String>,
    /// Tool selected by tool-execution nodes.
    #[serde(default)]
    pub tool: Option<String>,
    /// Provider selected by model/review nodes.
    #[serde(default)]
    pub provider: Option<String>,
    /// Additional capabilities required by this node.
    #[serde(default)]
    pub required_capabilities: BTreeSet<String>,
    /// State scopes read by the node.
    #[serde(default)]
    pub read_scopes: BTreeSet<String>,
    /// State scopes proposed for writing by the node.
    #[serde(default)]
    pub write_scopes: BTreeSet<String>,
    /// Business retry limit.
    #[serde(default)]
    pub retry_limit: u32,
    /// Static iteration bound; required only for loop nodes.
    #[serde(default)]
    pub max_iterations: Option<u32>,
}

/// Generic graph node kinds.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    /// Transform provider-visible context.
    ContextTransform,
    /// Request model execution.
    ModelCall,
    /// Gate and request a tool operation.
    ToolExecutionGate,
    /// Wait for user approval.
    UserApproval,
    /// Create a child session.
    SpawnChildAgent,
    /// Send a child-session message.
    SendChildAgentMessage,
    /// Wait for child sessions.
    WaitForAgents,
    /// Join child-session results.
    JoinResults,
    /// Review structured work.
    Review,
    /// Statically bounded loop control.
    Loop,
    /// Conditional branch.
    ConditionalBranch,
    /// Parallel branch.
    ParallelBranch,
    /// Delay execution.
    Delay,
    /// Create or wait for a schedule.
    Schedule,
    /// Emit a typed runtime event.
    EmitEvent,
    /// Persist an immutable artifact.
    PersistArtifact,
    /// Complete the current turn.
    CompleteTurn,
    /// Complete the session.
    CompleteSession,
    /// Fail with a structured reason.
    Fail,
}

impl NodeKind {
    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::CompleteTurn | Self::CompleteSession | Self::Fail
        )
    }

    const fn implied_capability(self) -> Option<&'static str> {
        match self {
            Self::ContextTransform => Some("context"),
            Self::ModelCall | Self::Review => Some("model"),
            Self::ToolExecutionGate => Some("tools"),
            Self::UserApproval => Some("approval"),
            Self::SpawnChildAgent
            | Self::SendChildAgentMessage
            | Self::WaitForAgents
            | Self::JoinResults => Some("agents"),
            Self::Delay | Self::Schedule => Some("scheduling"),
            Self::EmitEvent => Some("events"),
            Self::PersistArtifact => Some("artifacts"),
            Self::Loop
            | Self::ConditionalBranch
            | Self::ParallelBranch
            | Self::CompleteTurn
            | Self::CompleteSession
            | Self::Fail => None,
        }
    }
}

/// Directed graph transition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeDefinition {
    /// Source node ID.
    pub from: String,
    /// Destination node ID.
    pub to: String,
    /// Optional constrained transition condition.
    #[serde(default)]
    pub condition: Option<String>,
    /// Optional stable inspection label.
    #[serde(default)]
    pub label: Option<String>,
}

/// Deterministic executable graph.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutableGraph {
    /// Source format version.
    pub format_version: u16,
    /// Index of the entry node.
    pub entry_index: usize,
    /// Validated hard budgets.
    pub budget: GraphBudget,
    /// Sorted declarations.
    pub declarations: GraphDeclarations,
    /// Nodes sorted by ID.
    pub nodes: Vec<ExecutableNode>,
    /// Edges sorted by source, destination, and label.
    pub edges: Vec<ExecutableEdge>,
    /// Complete deterministic cache identity.
    pub cache_key: GraphCacheKey,
}

impl ExecutableGraph {
    /// Returns deterministic JSON suitable for graph inspection and golden tests.
    ///
    /// # Errors
    ///
    /// Returns [`GraphError`] only if the fully owned executable representation
    /// cannot be serialized.
    pub fn inspect_json(&self) -> Result<String, GraphError> {
        serde_json::to_string_pretty(self).map_err(|error| GraphError::Inspection {
            detail: error.to_string(),
        })
    }
}

/// Compiled generic node.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutableNode {
    /// Deterministic node index.
    pub index: usize,
    /// Stable source ID.
    pub id: String,
    /// Generic kind.
    pub kind: NodeKind,
    /// Parsed constrained condition.
    pub condition: Option<Expression>,
    /// Declared tool.
    pub tool: Option<String>,
    /// Declared provider.
    pub provider: Option<String>,
    /// Required capabilities.
    pub required_capabilities: BTreeSet<String>,
    /// Read scopes.
    pub read_scopes: BTreeSet<String>,
    /// Proposed write scopes.
    pub write_scopes: BTreeSet<String>,
    /// Retry limit.
    pub retry_limit: u32,
    /// Static loop bound.
    pub max_iterations: Option<u32>,
}

/// Compiled directed transition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExecutableEdge {
    /// Source node index.
    pub from: usize,
    /// Destination node index.
    pub to: usize,
    /// Parsed constrained condition.
    pub condition: Option<Expression>,
    /// Optional stable label.
    pub label: Option<String>,
}

/// Cache identity with inspectable constituent hashes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct GraphCacheKey {
    /// Exact graph source hash.
    pub graph_content_hash: ContentHash,
    /// Validated plugin-set hash.
    pub plugin_set_hash: ContentHash,
    /// Sorted runtime capability-set hash.
    pub capability_set_hash: ContentHash,
    /// Runtime API version hash.
    pub runtime_api_hash: ContentHash,
    /// Hash binding all constituents.
    pub combined_hash: ContentHash,
}

/// Parses, validates, and deterministically compiles a graph.
///
/// # Errors
///
/// Returns [`GraphError`] for malformed source, structural invalidity, missing
/// declarations, unsafe parallel writes, unbounded cycles, or exceeded limits.
pub fn compile(
    source: &str,
    cache_inputs: &GraphCacheInputs,
    limits: CompilerLimits,
) -> Result<ExecutableGraph, GraphError> {
    let definition = GraphDefinition::parse(source, limits)?;
    validate_version_and_bounds(&definition, limits)?;
    validate_names(&definition, limits)?;

    let node_map = collect_nodes(&definition)?;
    validate_edges(&definition, &node_map, limits)?;
    validate_entry_and_reachability(&definition, &node_map)?;
    validate_termination(&definition, &node_map)?;
    validate_node_contracts(&definition, cache_inputs, limits)?;
    validate_cycles(&definition, &node_map)?;
    validate_parallel_writes(&definition, &node_map)?;

    let mut sorted_nodes: Vec<_> = definition.nodes.iter().collect();
    sorted_nodes.sort_by(|left, right| left.id.cmp(&right.id));
    let indices: BTreeMap<_, _> = sorted_nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (node.id.as_str(), index))
        .collect();

    let mut nodes = Vec::with_capacity(sorted_nodes.len());
    for (index, node) in sorted_nodes.into_iter().enumerate() {
        nodes.push(ExecutableNode {
            index,
            id: node.id.clone(),
            kind: node.kind,
            condition: parse_condition(node.condition.as_deref(), &node.id, limits.expression)?,
            tool: node.tool.clone(),
            provider: node.provider.clone(),
            required_capabilities: node.required_capabilities.clone(),
            read_scopes: node.read_scopes.clone(),
            write_scopes: node.write_scopes.clone(),
            retry_limit: node.retry_limit,
            max_iterations: node.max_iterations,
        });
    }

    let mut sorted_edges: Vec<_> = definition.edges.iter().collect();
    sorted_edges.sort_by(|left, right| {
        left.from
            .cmp(&right.from)
            .then_with(|| left.to.cmp(&right.to))
            .then_with(|| left.label.cmp(&right.label))
            .then_with(|| left.condition.cmp(&right.condition))
    });
    let mut edges = Vec::with_capacity(sorted_edges.len());
    for edge in sorted_edges {
        edges.push(ExecutableEdge {
            from: indices[edge.from.as_str()],
            to: indices[edge.to.as_str()],
            condition: parse_condition(
                edge.condition.as_deref(),
                &format!("{} -> {}", edge.from, edge.to),
                limits.expression,
            )?,
            label: edge.label.clone(),
        });
    }

    Ok(ExecutableGraph {
        format_version: definition.format_version,
        entry_index: indices[definition.entry.as_str()],
        budget: definition.budget,
        declarations: definition.declarations,
        nodes,
        edges,
        cache_key: build_cache_key(source, cache_inputs),
    })
}

fn validate_version_and_bounds(
    definition: &GraphDefinition,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    if definition.format_version != GRAPH_FORMAT_VERSION {
        return Err(GraphError::UnsupportedVersion {
            actual: definition.format_version,
            supported: GRAPH_FORMAT_VERSION,
        });
    }
    if definition.nodes.len() > limits.max_nodes {
        return Err(GraphError::TooManyNodes {
            actual: definition.nodes.len(),
            maximum: limits.max_nodes,
        });
    }
    if definition.edges.len() > limits.max_edges {
        return Err(GraphError::TooManyEdges {
            actual: definition.edges.len(),
            maximum: limits.max_edges,
        });
    }
    for (name, actual, maximum) in [
        ("max_steps", definition.budget.max_steps, limits.max_steps),
        (
            "max_tokens",
            definition.budget.max_tokens,
            limits.max_tokens,
        ),
        (
            "max_cost_micros",
            definition.budget.max_cost_micros,
            limits.max_cost_micros,
        ),
        (
            "max_duration_ms",
            definition.budget.max_duration_ms,
            limits.max_duration_ms,
        ),
    ] {
        if actual == 0 || actual > maximum {
            return Err(GraphError::InvalidBudget {
                name,
                actual,
                maximum,
            });
        }
    }
    Ok(())
}

fn validate_names(definition: &GraphDefinition, limits: CompilerLimits) -> Result<(), GraphError> {
    validate_name("entry", &definition.entry, limits.max_name_bytes)?;
    for node in &definition.nodes {
        validate_name("node", &node.id, limits.max_name_bytes)?;
        for value in node
            .required_capabilities
            .iter()
            .chain(&node.read_scopes)
            .chain(&node.write_scopes)
        {
            validate_name("node declaration", value, limits.max_name_bytes)?;
        }
    }
    for value in definition
        .declarations
        .capabilities
        .iter()
        .chain(&definition.declarations.tools)
        .chain(&definition.declarations.providers)
    {
        validate_name("graph declaration", value, limits.max_name_bytes)?;
    }
    Ok(())
}

fn validate_name(kind: &'static str, value: &str, maximum: usize) -> Result<(), GraphError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.:/-".contains(character))
    {
        Err(GraphError::InvalidName {
            kind,
            value: value.to_owned(),
            maximum,
        })
    } else {
        Ok(())
    }
}

fn collect_nodes(
    definition: &GraphDefinition,
) -> Result<BTreeMap<&str, &NodeDefinition>, GraphError> {
    let mut nodes = BTreeMap::new();
    for node in &definition.nodes {
        if nodes.insert(node.id.as_str(), node).is_some() {
            return Err(GraphError::DuplicateNode {
                node: node.id.clone(),
            });
        }
    }
    Ok(nodes)
}

fn validate_edges(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    let mut seen = BTreeSet::new();
    for edge in &definition.edges {
        validate_name("edge source", &edge.from, limits.max_name_bytes)?;
        validate_name("edge destination", &edge.to, limits.max_name_bytes)?;
        if !nodes.contains_key(edge.from.as_str()) {
            return Err(GraphError::UnknownEdgeNode {
                edge: format!("{} -> {}", edge.from, edge.to),
                node: edge.from.clone(),
            });
        }
        if !nodes.contains_key(edge.to.as_str()) {
            return Err(GraphError::UnknownEdgeNode {
                edge: format!("{} -> {}", edge.from, edge.to),
                node: edge.to.clone(),
            });
        }
        let identity = (
            edge.from.as_str(),
            edge.to.as_str(),
            edge.label.as_deref(),
            edge.condition.as_deref(),
        );
        if !seen.insert(identity) {
            return Err(GraphError::DuplicateEdge {
                edge: format!("{} -> {}", edge.from, edge.to),
            });
        }
    }
    Ok(())
}

fn adjacency(definition: &GraphDefinition) -> BTreeMap<&str, Vec<&str>> {
    let mut result = BTreeMap::<_, Vec<_>>::new();
    for node in &definition.nodes {
        result.entry(node.id.as_str()).or_default();
    }
    for edge in &definition.edges {
        result
            .entry(edge.from.as_str())
            .or_default()
            .push(edge.to.as_str());
    }
    for targets in result.values_mut() {
        targets.sort_unstable();
        targets.dedup();
    }
    result
}

fn reverse_adjacency(definition: &GraphDefinition) -> BTreeMap<&str, Vec<&str>> {
    let mut result = BTreeMap::<_, Vec<_>>::new();
    for node in &definition.nodes {
        result.entry(node.id.as_str()).or_default();
    }
    for edge in &definition.edges {
        result
            .entry(edge.to.as_str())
            .or_default()
            .push(edge.from.as_str());
    }
    result
}

fn reachable_from<'a>(
    start: &'a str,
    adjacency: &BTreeMap<&'a str, Vec<&'a str>>,
) -> BTreeSet<&'a str> {
    let mut reached = BTreeSet::new();
    let mut queue = VecDeque::from([start]);
    while let Some(node) = queue.pop_front() {
        if !reached.insert(node) {
            continue;
        }
        if let Some(targets) = adjacency.get(node) {
            queue.extend(targets.iter().copied());
        }
    }
    reached
}

fn validate_entry_and_reachability(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    if !nodes.contains_key(definition.entry.as_str()) {
        return Err(GraphError::UnknownEntry {
            entry: definition.entry.clone(),
        });
    }
    let graph = adjacency(definition);
    let reached = reachable_from(&definition.entry, &graph);
    let unreachable: Vec<_> = nodes
        .keys()
        .filter(|node| !reached.contains(**node))
        .map(|node| (*node).to_owned())
        .collect();
    if unreachable.is_empty() {
        Ok(())
    } else {
        Err(GraphError::UnreachableNodes { nodes: unreachable })
    }
}

fn validate_termination(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    let terminals: Vec<_> = nodes
        .values()
        .filter(|node| node.kind.is_terminal())
        .map(|node| node.id.as_str())
        .collect();
    if terminals.is_empty() {
        return Err(GraphError::MissingTermination);
    }
    let graph = adjacency(definition);
    for terminal in &terminals {
        if graph
            .get(terminal)
            .is_some_and(|targets| !targets.is_empty())
        {
            return Err(GraphError::TerminalHasOutgoingEdge {
                node: (*terminal).to_owned(),
            });
        }
    }
    let reverse = reverse_adjacency(definition);
    let mut can_terminate = BTreeSet::new();
    for terminal in terminals {
        can_terminate.extend(reachable_from(terminal, &reverse));
    }
    let trapped: Vec<_> = nodes
        .keys()
        .filter(|node| !can_terminate.contains(**node))
        .map(|node| (*node).to_owned())
        .collect();
    if trapped.is_empty() {
        Ok(())
    } else {
        Err(GraphError::NoTerminationPath { nodes: trapped })
    }
}

fn validate_node_contracts(
    definition: &GraphDefinition,
    cache_inputs: &GraphCacheInputs,
    limits: CompilerLimits,
) -> Result<(), GraphError> {
    for capability in &definition.declarations.capabilities {
        if !cache_inputs.capability_set.contains(capability) {
            return Err(GraphError::RuntimeCapabilityUnavailable {
                capability: capability.clone(),
            });
        }
    }
    for node in &definition.nodes {
        if node.retry_limit > limits.max_retry_limit {
            return Err(GraphError::RetryLimitExceeded {
                node: node.id.clone(),
                actual: node.retry_limit,
                maximum: limits.max_retry_limit,
            });
        }
        match (node.kind, node.max_iterations) {
            (NodeKind::Loop, Some(value)) if value > 0 && value <= limits.max_loop_iterations => {}
            (NodeKind::Loop, value) => {
                return Err(GraphError::InvalidLoopBound {
                    node: node.id.clone(),
                    actual: value,
                    maximum: limits.max_loop_iterations,
                });
            }
            (_, Some(_)) => {
                return Err(GraphError::LoopBoundOnNonLoop {
                    node: node.id.clone(),
                });
            }
            (_, None) => {}
        }
        let required = node
            .required_capabilities
            .iter()
            .map(String::as_str)
            .chain(node.kind.implied_capability());
        for capability in required {
            if !definition.declarations.capabilities.contains(capability) {
                return Err(GraphError::UndeclaredCapability {
                    node: node.id.clone(),
                    capability: capability.to_owned(),
                });
            }
        }
        match node.kind {
            NodeKind::ToolExecutionGate => {
                let tool = node
                    .tool
                    .as_deref()
                    .ok_or_else(|| GraphError::MissingTool {
                        node: node.id.clone(),
                    })?;
                if !definition.declarations.tools.contains(tool) {
                    return Err(GraphError::UndeclaredTool {
                        node: node.id.clone(),
                        tool: tool.to_owned(),
                    });
                }
            }
            NodeKind::ModelCall | NodeKind::Review => {
                let provider =
                    node.provider
                        .as_deref()
                        .ok_or_else(|| GraphError::MissingProvider {
                            node: node.id.clone(),
                        })?;
                if !definition.declarations.providers.contains(provider) {
                    return Err(GraphError::UndeclaredProvider {
                        node: node.id.clone(),
                        provider: provider.to_owned(),
                    });
                }
            }
            _ => {}
        }
        parse_condition(node.condition.as_deref(), &node.id, limits.expression)?;
    }
    for edge in &definition.edges {
        parse_condition(
            edge.condition.as_deref(),
            &format!("{} -> {}", edge.from, edge.to),
            limits.expression,
        )?;
    }
    Ok(())
}

fn parse_condition(
    source: Option<&str>,
    owner: &str,
    limits: ExpressionLimits,
) -> Result<Option<Expression>, GraphError> {
    source
        .map(|condition| {
            Expression::parse(condition, limits).map_err(|error| GraphError::InvalidCondition {
                owner: owner.to_owned(),
                error,
            })
        })
        .transpose()
}

fn validate_cycles(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    let graph = adjacency(definition);
    let loop_nodes: BTreeSet<_> = nodes
        .values()
        .filter(|node| node.kind == NodeKind::Loop)
        .map(|node| node.id.as_str())
        .collect();
    let mut incoming: BTreeMap<_, usize> = nodes
        .keys()
        .copied()
        .filter(|node| !loop_nodes.contains(node))
        .map(|node| (node, 0))
        .collect();
    for (source, targets) in &graph {
        if loop_nodes.contains(source) {
            continue;
        }
        for target in targets {
            if !loop_nodes.contains(target) {
                *incoming
                    .get_mut(target)
                    .expect("validated target exists in non-loop node map") += 1;
            }
        }
    }
    let mut ready: BTreeSet<_> = incoming
        .iter()
        .filter_map(|(node, count)| (*count == 0).then_some(*node))
        .collect();
    let mut processed = 0;
    while let Some(node) = ready.pop_first() {
        processed += 1;
        for target in graph.get(node).into_iter().flatten().copied() {
            if loop_nodes.contains(target) {
                continue;
            }
            let count = incoming
                .get_mut(target)
                .expect("validated target exists in non-loop node map");
            *count -= 1;
            if *count == 0 {
                ready.insert(target);
            }
        }
    }
    if processed == incoming.len() {
        Ok(())
    } else {
        Err(GraphError::IllegalCycle {
            nodes: incoming
                .into_iter()
                .filter_map(|(node, count)| (count > 0).then_some(node.to_owned()))
                .collect(),
        })
    }
}

fn validate_parallel_writes(
    definition: &GraphDefinition,
    nodes: &BTreeMap<&str, &NodeDefinition>,
) -> Result<(), GraphError> {
    let graph = adjacency(definition);
    for parallel in nodes
        .values()
        .filter(|node| node.kind == NodeKind::ParallelBranch)
    {
        let branches = graph.get(parallel.id.as_str()).cloned().unwrap_or_default();
        if branches.len() < 2 {
            return Err(GraphError::ParallelNeedsBranches {
                node: parallel.id.clone(),
            });
        }
        let join = common_join(&branches, &graph, nodes).ok_or_else(|| {
            GraphError::ParallelMissingJoin {
                node: parallel.id.clone(),
            }
        })?;
        let scopes: Vec<_> = branches
            .iter()
            .map(|branch| branch_write_scopes(branch, join, &graph, nodes))
            .collect();
        for left in 0..branches.len() {
            for right in (left + 1)..branches.len() {
                if let Some(scope) = scopes[left].intersection(&scopes[right]).next() {
                    return Err(GraphError::ConflictingParallelWrites {
                        node: parallel.id.clone(),
                        scope: (*scope).to_owned(),
                        branches: vec![branches[left].to_owned(), branches[right].to_owned()],
                    });
                }
            }
        }
    }
    Ok(())
}

fn common_join<'a>(
    branches: &[&'a str],
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
) -> Option<&'a str> {
    let mut common: Option<BTreeSet<&str>> = None;
    for branch in branches {
        let reachable = reachable_from(branch, graph);
        let joins: BTreeSet<_> = reachable
            .into_iter()
            .filter(|node| {
                nodes
                    .get(node)
                    .is_some_and(|item| item.kind == NodeKind::JoinResults)
            })
            .collect();
        common = Some(match common {
            None => joins,
            Some(current) => current.intersection(&joins).copied().collect(),
        });
    }
    common?.into_iter().next()
}

fn branch_write_scopes<'a>(
    branch: &'a str,
    join: &str,
    graph: &BTreeMap<&'a str, Vec<&'a str>>,
    nodes: &BTreeMap<&'a str, &'a NodeDefinition>,
) -> BTreeSet<&'a str> {
    let mut scopes = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut queue = VecDeque::from([branch]);
    while let Some(node) = queue.pop_front() {
        if node == join || !visited.insert(node) {
            continue;
        }
        if let Some(definition) = nodes.get(node) {
            scopes.extend(definition.write_scopes.iter().map(String::as_str));
        }
        if let Some(targets) = graph.get(node) {
            queue.extend(targets.iter().copied());
        }
    }
    scopes
}

fn build_cache_key(source: &str, inputs: &GraphCacheInputs) -> GraphCacheKey {
    let graph_content_hash = ContentHash::digest(source.as_bytes());
    let plugin_set_hash = inputs.plugin_set_hash;
    let capability_set_hash = ContentHash::digest(&encode_strings(
        inputs.capability_set.iter().map(String::as_str),
    ));
    let runtime_api_hash = ContentHash::digest(inputs.runtime_api_version.as_bytes());
    let mut combined = Vec::with_capacity(128);
    combined.extend_from_slice(graph_content_hash.as_bytes());
    combined.extend_from_slice(plugin_set_hash.as_bytes());
    combined.extend_from_slice(capability_set_hash.as_bytes());
    combined.extend_from_slice(runtime_api_hash.as_bytes());
    GraphCacheKey {
        graph_content_hash,
        plugin_set_hash,
        capability_set_hash,
        runtime_api_hash,
        combined_hash: ContentHash::digest(&combined),
    }
}

fn encode_strings<'a>(values: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let mut encoded = Vec::new();
    for value in values {
        encoded.extend_from_slice(&(value.len() as u64).to_be_bytes());
        encoded.extend_from_slice(value.as_bytes());
    }
    encoded
}

/// Deterministic graph rejection.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphError {
    /// Source exceeds the configured byte bound.
    #[error("graph source is {actual} bytes; maximum is {maximum}")]
    SourceTooLarge {
        /// Actual bytes.
        actual: usize,
        /// Maximum bytes.
        maximum: usize,
    },
    /// TOML syntax or schema is invalid.
    #[error("invalid graph TOML: {detail}")]
    InvalidToml {
        /// Parser-owned stable message.
        detail: String,
    },
    /// Source format is unsupported.
    #[error("graph format version {actual} is unsupported; expected {supported}")]
    UnsupportedVersion {
        /// Actual version.
        actual: u16,
        /// Supported version.
        supported: u16,
    },
    /// Node bound exceeded.
    #[error("graph has {actual} nodes; maximum is {maximum}")]
    TooManyNodes {
        /// Actual count.
        actual: usize,
        /// Maximum count.
        maximum: usize,
    },
    /// Edge bound exceeded.
    #[error("graph has {actual} edges; maximum is {maximum}")]
    TooManyEdges {
        /// Actual count.
        actual: usize,
        /// Maximum count.
        maximum: usize,
    },
    /// Budget is zero or exceeds compiler policy.
    #[error("budget `{name}` is {actual}; valid range is 1..={maximum}")]
    InvalidBudget {
        /// Budget field.
        name: &'static str,
        /// Actual value.
        actual: u64,
        /// Maximum.
        maximum: u64,
    },
    /// Identifier or declaration is invalid.
    #[error("invalid {kind} `{value}`; maximum is {maximum} ASCII name bytes")]
    InvalidName {
        /// Name category.
        kind: &'static str,
        /// Invalid value.
        value: String,
        /// Maximum bytes.
        maximum: usize,
    },
    /// Duplicate node ID.
    #[error("duplicate node `{node}`")]
    DuplicateNode {
        /// Duplicate ID.
        node: String,
    },
    /// Entry does not exist.
    #[error("entry node `{entry}` does not exist")]
    UnknownEntry {
        /// Entry ID.
        entry: String,
    },
    /// Edge references an unknown node.
    #[error("edge `{edge}` references unknown node `{node}`")]
    UnknownEdgeNode {
        /// Edge label.
        edge: String,
        /// Unknown node.
        node: String,
    },
    /// Exact duplicate edge.
    #[error("duplicate edge `{edge}`")]
    DuplicateEdge {
        /// Edge label.
        edge: String,
    },
    /// Nodes cannot be reached from entry.
    #[error("unreachable nodes: {nodes:?}")]
    UnreachableNodes {
        /// Sorted IDs.
        nodes: Vec<String>,
    },
    /// No terminal node exists.
    #[error("graph has no complete_turn, complete_session, or fail node")]
    MissingTermination,
    /// Terminal node has an outgoing transition.
    #[error("terminal node `{node}` has an outgoing edge")]
    TerminalHasOutgoingEdge {
        /// Terminal ID.
        node: String,
    },
    /// Reachable nodes cannot reach termination.
    #[error("nodes have no termination path: {nodes:?}")]
    NoTerminationPath {
        /// Sorted IDs.
        nodes: Vec<String>,
    },
    /// Node retry limit exceeds compiler policy.
    #[error("node `{node}` retry limit {actual} exceeds {maximum}")]
    RetryLimitExceeded {
        /// Node ID.
        node: String,
        /// Actual limit.
        actual: u32,
        /// Maximum.
        maximum: u32,
    },
    /// Loop is missing a valid static bound.
    #[error("loop `{node}` has invalid bound {actual:?}; valid range is 1..={maximum}")]
    InvalidLoopBound {
        /// Loop ID.
        node: String,
        /// Actual optional value.
        actual: Option<u32>,
        /// Maximum.
        maximum: u32,
    },
    /// Non-loop node declares a loop bound.
    #[error("non-loop node `{node}` declares max_iterations")]
    LoopBoundOnNonLoop {
        /// Node ID.
        node: String,
    },
    /// Node requires a capability omitted from declarations.
    #[error("node `{node}` requires undeclared capability `{capability}`")]
    UndeclaredCapability {
        /// Node ID.
        node: String,
        /// Capability.
        capability: String,
    },
    /// Graph declares a capability absent from the runtime cache context.
    #[error("runtime does not provide declared capability `{capability}`")]
    RuntimeCapabilityUnavailable {
        /// Missing capability.
        capability: String,
    },
    /// Tool node supplies no tool.
    #[error("tool node `{node}` does not select a tool")]
    MissingTool {
        /// Node ID.
        node: String,
    },
    /// Selected tool was not declared.
    #[error("node `{node}` selects undeclared tool `{tool}`")]
    UndeclaredTool {
        /// Node ID.
        node: String,
        /// Tool name.
        tool: String,
    },
    /// Model/review node supplies no provider.
    #[error("model node `{node}` does not select a provider")]
    MissingProvider {
        /// Node ID.
        node: String,
    },
    /// Selected provider was not declared.
    #[error("node `{node}` selects undeclared provider `{provider}`")]
    UndeclaredProvider {
        /// Node ID.
        node: String,
        /// Provider name.
        provider: String,
    },
    /// Embedded condition is invalid.
    #[error("invalid condition on `{owner}`: {error}")]
    InvalidCondition {
        /// Node or edge owner.
        owner: String,
        /// Expression parse failure.
        error: ParseError,
    },
    /// A cycle exists that does not traverse a bounded loop node.
    #[error("cycle does not traverse a bounded loop node: {nodes:?}")]
    IllegalCycle {
        /// Deterministic cycle nodes.
        nodes: Vec<String>,
    },
    /// Parallel node has fewer than two branches.
    #[error("parallel node `{node}` requires at least two outgoing branches")]
    ParallelNeedsBranches {
        /// Parallel node.
        node: String,
    },
    /// Parallel branches have no common join.
    #[error("parallel node `{node}` has no common join_results node")]
    ParallelMissingJoin {
        /// Parallel node.
        node: String,
    },
    /// Parallel branches propose writes to the same scope.
    #[error("parallel node `{node}` branches {branches:?} both write `{scope}`")]
    ConflictingParallelWrites {
        /// Parallel node.
        node: String,
        /// Conflicting scope.
        scope: String,
        /// Conflicting branch entry IDs.
        branches: Vec<String>,
    },
    /// Inspection serialization failed.
    #[error("graph inspection serialization failed: {detail}")]
    Inspection {
        /// Serialization diagnostic.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
format_version = 1
entry = "plan"

[budget]
max_steps = 100
max_tokens = 10000
max_cost_micros = 500000
max_duration_ms = 60000

[declarations]
capabilities = ["model", "tools"]
tools = ["filesystem.read"]
providers = ["mock"]

[[nodes]]
id = "plan"
kind = "model_call"
provider = "mock"
condition = "session.ready == true"
retry_limit = 2

[[nodes]]
id = "read"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "plan"
to = "read"
condition = "model.tool_requested"

[[edges]]
from = "read"
to = "done"
"#;

    fn cache_inputs() -> GraphCacheInputs {
        GraphCacheInputs {
            plugin_set_hash: ContentHash::digest(b"plugins"),
            runtime_api_version: "1.0".into(),
            capability_set: ["model".to_owned(), "tools".to_owned()]
                .into_iter()
                .collect(),
        }
    }

    fn compile_valid(source: &str) -> Result<ExecutableGraph, GraphError> {
        compile(source, &cache_inputs(), CompilerLimits::default())
    }

    #[test]
    fn valid_graph_compiles_to_stable_sorted_representation() {
        let graph = compile_valid(VALID).expect("compile");
        assert_eq!(
            graph
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["done", "plan", "read"]
        );
        assert_eq!(graph.entry_index, 1);
        let first = graph.inspect_json().expect("inspection");
        let second = compile_valid(VALID)
            .expect("compile again")
            .inspect_json()
            .expect("inspection");
        assert_eq!(first, second);
        assert!(first.contains(r#""graph_content_hash""#));
        assert!(first.contains(r#""expression": "compare""#));
    }

    #[test]
    fn cache_key_changes_for_each_compatibility_input() {
        let baseline = compile_valid(VALID).expect("baseline").cache_key;
        let changed_source = compile_valid(&VALID.replace("max_steps = 100", "max_steps = 101"))
            .expect("source")
            .cache_key;
        assert_ne!(baseline.combined_hash, changed_source.combined_hash);

        let mut inputs = cache_inputs();
        inputs.plugin_set_hash = ContentHash::digest(b"other plugins");
        assert_ne!(
            baseline.combined_hash,
            compile(VALID, &inputs, CompilerLimits::default())
                .expect("plugins")
                .cache_key
                .combined_hash
        );

        let mut inputs = cache_inputs();
        inputs.runtime_api_version = "1.1".into();
        assert_ne!(
            baseline.combined_hash,
            compile(VALID, &inputs, CompilerLimits::default())
                .expect("runtime")
                .cache_key
                .combined_hash
        );

        let mut inputs = cache_inputs();
        inputs.capability_set.insert("extra".into());
        assert_ne!(
            baseline.combined_hash,
            compile(VALID, &inputs, CompilerLimits::default())
                .expect("capabilities")
                .cache_key
                .combined_hash
        );
    }

    #[test]
    fn rejects_version_and_size_bounds() {
        assert!(matches!(
            compile_valid(&VALID.replace("format_version = 1", "format_version = 2")),
            Err(GraphError::UnsupportedVersion { .. })
        ));
        let limits = CompilerLimits {
            max_source_bytes: 4,
            ..CompilerLimits::default()
        };
        assert!(matches!(
            compile(VALID, &cache_inputs(), limits),
            Err(GraphError::SourceTooLarge { .. })
        ));
    }

    #[test]
    fn rejects_duplicate_and_unknown_node_families() {
        let duplicate = VALID.replace(
            "[[nodes]]\nid = \"done\"",
            "[[nodes]]\nid = \"plan\"\nkind = \"complete_session\"\n\n[[nodes]]\nid = \"done\"",
        );
        assert!(matches!(
            compile_valid(&duplicate),
            Err(GraphError::DuplicateNode { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("entry = \"plan\"", "entry = \"missing\"")),
            Err(GraphError::UnknownEntry { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("to = \"read\"", "to = \"missing\"")),
            Err(GraphError::UnknownEdgeNode { .. })
        ));
    }

    #[test]
    fn rejects_unreachable_and_termination_families() {
        let unreachable = VALID.replace(
            "[[nodes]]\nid = \"done\"",
            "[[nodes]]\nid = \"orphan\"\nkind = \"complete_turn\"\n\n[[nodes]]\nid = \"done\"",
        );
        assert!(matches!(
            compile_valid(&unreachable),
            Err(GraphError::UnreachableNodes { .. })
        ));
        let no_terminal = VALID.replace("kind = \"complete_session\"", "kind = \"emit_event\"");
        assert!(matches!(
            compile_valid(&no_terminal),
            Err(GraphError::MissingTermination)
        ));
        let terminal_edge = format!("{VALID}\n[[edges]]\nfrom = \"done\"\nto = \"plan\"\n");
        assert!(matches!(
            compile_valid(&terminal_edge),
            Err(GraphError::TerminalHasOutgoingEdge { .. })
        ));
    }

    #[test]
    fn rejects_retry_and_budget_families() {
        assert!(matches!(
            compile_valid(&VALID.replace("retry_limit = 2", "retry_limit = 99")),
            Err(GraphError::RetryLimitExceeded { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("max_steps = 100", "max_steps = 0")),
            Err(GraphError::InvalidBudget { .. })
        ));
    }

    #[test]
    fn rejects_capability_tool_and_provider_families() {
        let capability = VALID.replace(
            "condition = \"session.ready == true\"",
            "required_capabilities = [\"browser\"]\ncondition = \"session.ready == true\"",
        );
        assert!(matches!(
            compile_valid(&capability),
            Err(GraphError::UndeclaredCapability { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("tool = \"filesystem.read\"", "tool = \"other\"")),
            Err(GraphError::UndeclaredTool { .. })
        ));
        assert!(matches!(
            compile_valid(&VALID.replace("provider = \"mock\"", "provider = \"other\"")),
            Err(GraphError::UndeclaredProvider { .. })
        ));
        let mut inputs = cache_inputs();
        inputs.capability_set.remove("model");
        assert!(matches!(
            compile(VALID, &inputs, CompilerLimits::default()),
            Err(GraphError::RuntimeCapabilityUnavailable { .. })
        ));
    }

    #[test]
    fn rejects_invalid_embedded_condition() {
        assert!(matches!(
            compile_valid(&VALID.replace("session.ready == true", "session.ready = true")),
            Err(GraphError::InvalidCondition { .. })
        ));
    }

    #[test]
    fn cycles_require_a_statically_bounded_loop_node() {
        let illegal = VALID.replace(
            "[[edges]]\nfrom = \"read\"\nto = \"done\"",
            "[[edges]]\nfrom = \"read\"\nto = \"plan\"\n\n[[edges]]\nfrom = \"read\"\nto = \"done\"",
        );
        assert!(matches!(
            compile_valid(&illegal),
            Err(GraphError::IllegalCycle { .. })
        ));

        let bounded = r#"
format_version = 1
entry = "loop"
[budget]
max_steps = 100
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[[nodes]]
id = "loop"
kind = "loop"
max_iterations = 3
[[nodes]]
id = "work"
kind = "conditional_branch"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "loop"
to = "work"
[[edges]]
from = "work"
to = "loop"
[[edges]]
from = "loop"
to = "done"
"#;
        assert!(compile_valid(bounded).is_ok());
        assert!(matches!(
            compile_valid(&bounded.replace("max_iterations = 3", "max_iterations = 0")),
            Err(GraphError::InvalidLoopBound { .. })
        ));
    }

    #[test]
    fn rejects_conflicting_parallel_write_scopes() {
        let parallel = r#"
format_version = 1
entry = "parallel"
[budget]
max_steps = 100
max_tokens = 1
max_cost_micros = 1
max_duration_ms = 1
[declarations]
capabilities = ["agents"]
[[nodes]]
id = "parallel"
kind = "parallel_branch"
[[nodes]]
id = "left"
kind = "wait_for_agents"
write_scopes = ["workspace"]
[[nodes]]
id = "right"
kind = "wait_for_agents"
write_scopes = ["workspace"]
[[nodes]]
id = "join"
kind = "join_results"
[[nodes]]
id = "done"
kind = "complete_session"
[[edges]]
from = "parallel"
to = "left"
[[edges]]
from = "parallel"
to = "right"
[[edges]]
from = "left"
to = "join"
[[edges]]
from = "right"
to = "join"
[[edges]]
from = "join"
to = "done"
"#;
        let mut inputs = cache_inputs();
        inputs.capability_set.insert("agents".into());
        assert!(matches!(
            compile(parallel, &inputs, CompilerLimits::default()),
            Err(GraphError::ConflictingParallelWrites { .. })
        ));
        assert!(
            compile(
                &parallel.replacen(
                    "write_scopes = [\"workspace\"]",
                    "write_scopes = [\"context\"]",
                    1
                ),
                &inputs,
                CompilerLimits::default()
            )
            .is_ok()
        );
    }
}
