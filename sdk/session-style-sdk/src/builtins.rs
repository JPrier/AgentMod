use std::collections::BTreeMap;

use agentmod_graph_engine::{CompilerLimits, GraphDefinition};

use crate::{
    ApprovalDecision, ApprovalDefaults, BuiltInStyle, ChildAgentLimits, CompactionSelection,
    CompactionStrategy, DecisionCapability, ExecutionBudgets, GraphSource, InterceptorDeclaration,
    MemoryScope, MemorySelection, RetryPolicy, SessionStyleManifest, StyleIdentity, StyleKind,
    TerminationOutcome, TerminationPolicy, TopLevelSelection,
};

/// Constructs one of the five required built-in semantic descriptors.
#[must_use]
pub fn built_in_manifest(style: BuiltInStyle) -> SessionStyleManifest {
    let parts = built_in_parts(style);
    let retry_attempts = if matches!(
        style,
        BuiltInStyle::ResearchLoop | BuiltInStyle::PlannerWorker
    ) {
        4
    } else {
        2
    };
    SessionStyleManifest {
        schema_version: 1,
        identity: StyleIdentity {
            id: parts.id.to_owned(),
            version: "1.0.0".to_owned(),
            runtime_api: "^1.0".to_owned(),
        },
        kind: StyleKind::BuiltIn,
        built_in_semantic: Some(style),
        graph: GraphSource::Inline {
            source: parts.graph.to_owned(),
        },
        interceptors: vec![default_interceptor()],
        required_capabilities: parts.capabilities.into_iter().map(str::to_owned).collect(),
        allowed_tool_groups: parts.tool_groups.into_iter().map(str::to_owned).collect(),
        allowed_providers: if parts.graph.contains("model_call") || parts.graph.contains("review") {
            vec!["mock".to_owned()]
        } else {
            Vec::new()
        },
        allowed_plugins: vec!["runtime.security".to_owned()],
        memory: parts.memory,
        compaction: parts.compaction,
        approvals: ApprovalDefaults {
            default: ApprovalDecision::Ask,
            groups: BTreeMap::from([("filesystem.read".to_owned(), ApprovalDecision::Allow)]),
        },
        budgets: ExecutionBudgets {
            max_iterations: 32,
            max_steps: 1_000,
            max_tokens: 1_000_000,
            max_cost_micros: 100_000_000,
            max_duration_ms: 3_600_000,
        },
        child_agents: parts.children,
        retry: RetryPolicy {
            max_attempts: retry_attempts,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
            retryable_failures: vec![
                "provider.rate_limit".to_owned(),
                "provider.unavailable".to_owned(),
            ],
        },
        termination: TerminationPolicy {
            allowed_outcomes: parts.outcomes,
            on_hard_limit: TerminationOutcome::Fail,
            require_explicit_terminal_node: true,
        },
        selection: TopLevelSelection {
            requires_explicit_selection: true,
            model_may_select: false,
        },
    }
}

struct BuiltInParts {
    id: &'static str,
    graph: &'static str,
    capabilities: Vec<&'static str>,
    tool_groups: Vec<&'static str>,
    memory: MemorySelection,
    compaction: CompactionSelection,
    children: ChildAgentLimits,
    outcomes: Vec<TerminationOutcome>,
}

fn built_in_parts(style: BuiltInStyle) -> BuiltInParts {
    let tuple = match style {
        BuiltInStyle::PersistentChat => (
            "persistent-chat",
            PERSISTENT_CHAT_GRAPH,
            vec!["agents", "approval", "model", "tools"],
            vec!["filesystem"],
            file_memory(),
            summary_compaction(),
            bounded_children(8, 2, 2, 50_000),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        BuiltInStyle::EphemeralTurn => (
            "ephemeral-turn",
            EPHEMERAL_TURN_GRAPH,
            vec!["approval", "context", "model"],
            Vec::new(),
            no_memory(),
            artifact_compaction(),
            no_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        BuiltInStyle::ResearchLoop => (
            "research-loop",
            RESEARCH_LOOP_GRAPH,
            vec!["agents", "approval", "artifacts", "model"],
            Vec::new(),
            file_memory(),
            artifact_compaction(),
            bounded_children(4, 2, 1, 50_000),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        BuiltInStyle::PlannerWorker => (
            "planner-worker",
            PLANNER_WORKER_GRAPH,
            vec!["agents", "approval", "model"],
            Vec::new(),
            file_memory(),
            summary_compaction(),
            bounded_children(16, 4, 2, 100_000),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        BuiltInStyle::DeclarativeGraph => (
            "declarative-graph",
            DECLARATIVE_GRAPH,
            vec!["approval", "events"],
            Vec::new(),
            no_memory(),
            no_compaction(),
            no_children(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
    };
    BuiltInParts {
        id: tuple.0,
        graph: tuple.1,
        capabilities: tuple.2,
        tool_groups: tuple.3,
        memory: tuple.4,
        compaction: tuple.5,
        children: tuple.6,
        outcomes: tuple.7,
    }
}

/// Constructs the declarative-graph built-in around user-supplied graph TOML.
///
/// Graph declarations seed the style capability, provider, and tool-group
/// allowlists when the source is syntactically valid. Full safety validation is
/// still performed by [`crate::compile_style`].
#[must_use]
pub fn declarative_graph_manifest(source: impl Into<String>) -> SessionStyleManifest {
    let source = source.into();
    let mut manifest = built_in_manifest(BuiltInStyle::DeclarativeGraph);
    if let Ok(definition) = GraphDefinition::parse(&source, CompilerLimits::default()) {
        manifest.required_capabilities = definition.declarations.capabilities.into_iter().collect();
        if !manifest
            .required_capabilities
            .iter()
            .any(|capability| capability == "approval")
        {
            manifest.required_capabilities.push("approval".to_owned());
        }
        manifest.required_capabilities.sort();
        manifest.allowed_providers = definition.declarations.providers.into_iter().collect();
        manifest.allowed_tool_groups = definition
            .declarations
            .tools
            .iter()
            .filter_map(|tool| tool.split_once('.').map(|(group, _)| group.to_owned()))
            .collect();
        manifest.allowed_tool_groups.sort();
        manifest.allowed_tool_groups.dedup();
    }
    manifest.graph = GraphSource::Inline { source };
    manifest
}

fn default_interceptor() -> InterceptorDeclaration {
    InterceptorDeclaration {
        id: "runtime-style-policy".to_owned(),
        owner: "runtime.security".to_owned(),
        event: "action.proposed".to_owned(),
        stage: 10,
        priority: 100,
        before: Vec::new(),
        after: Vec::new(),
        supported_decisions: vec![
            DecisionCapability::Continue,
            DecisionCapability::Replace,
            DecisionCapability::Reject,
            DecisionCapability::RequireApproval,
            DecisionCapability::Cancel,
        ],
        required_capabilities: vec!["approval".to_owned()],
    }
}

fn file_memory() -> MemorySelection {
    MemorySelection {
        provider: "file".to_owned(),
        scopes: vec![MemoryScope::Session, MemoryScope::Project],
        max_items: 32,
        max_injected_bytes: 256 * 1024,
    }
}

fn no_memory() -> MemorySelection {
    MemorySelection {
        provider: "none".to_owned(),
        scopes: Vec::new(),
        max_items: 0,
        max_injected_bytes: 0,
    }
}

fn summary_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::Summary,
        trigger_tokens: Some(750_000),
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
    }
}

fn artifact_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::ArtifactHandoff,
        trigger_tokens: Some(750_000),
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
    }
}

fn no_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::None,
        trigger_tokens: None,
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
    }
}

const fn no_children() -> ChildAgentLimits {
    ChildAgentLimits {
        max_children: 0,
        max_concurrent: 0,
        max_depth: 0,
        per_child_token_budget: 0,
    }
}

const fn bounded_children(
    max_children: u32,
    max_concurrent: u32,
    max_depth: u16,
    per_child_token_budget: u64,
) -> ChildAgentLimits {
    ChildAgentLimits {
        max_children,
        max_concurrent,
        max_depth,
        per_child_token_budget,
    }
}

const PERSISTENT_CHAT_GRAPH: &str = r#"
format_version = 1
entry = "respond"

[budget]
max_steps = 100
max_tokens = 250000
max_cost_micros = 25000000
max_duration_ms = 900000

[declarations]
capabilities = ["model", "tools"]
tools = ["filesystem.read"]
providers = ["mock"]

[[nodes]]
id = "respond"
kind = "model_call"
provider = "mock"
retry_limit = 1

[[nodes]]
id = "tool"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

[[nodes]]
id = "done"
kind = "complete_turn"

[[edges]]
from = "respond"
to = "tool"

[[edges]]
from = "tool"
to = "done"
"#;

const EPHEMERAL_TURN_GRAPH: &str = r#"
format_version = 1
entry = "fresh-context"

[budget]
max_steps = 50
max_tokens = 250000
max_cost_micros = 25000000
max_duration_ms = 900000

[declarations]
capabilities = ["context", "model"]
providers = ["mock"]

[[nodes]]
id = "fresh-context"
kind = "context_transform"

[[nodes]]
id = "respond"
kind = "model_call"
provider = "mock"
retry_limit = 1

[[nodes]]
id = "done"
kind = "complete_turn"

[[edges]]
from = "fresh-context"
to = "respond"

[[edges]]
from = "respond"
to = "done"
"#;

const RESEARCH_LOOP_GRAPH: &str = r#"
format_version = 1
entry = "research"

[budget]
max_steps = 500
max_tokens = 750000
max_cost_micros = 75000000
max_duration_ms = 2700000

[declarations]
capabilities = ["artifacts", "model"]
providers = ["mock"]

[[nodes]]
id = "research"
kind = "model_call"
provider = "mock"
retry_limit = 2

[[nodes]]
id = "persist"
kind = "persist_artifact"

[[nodes]]
id = "repeat"
kind = "loop"
max_iterations = 16

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "research"
to = "persist"

[[edges]]
from = "persist"
to = "repeat"

[[edges]]
from = "repeat"
to = "research"
condition = "iteration.remaining == true"

[[edges]]
from = "repeat"
to = "done"
condition = "iteration.remaining == false"
"#;

const PLANNER_WORKER_GRAPH: &str = r#"
format_version = 1
entry = "plan"

[budget]
max_steps = 750
max_tokens = 900000
max_cost_micros = 90000000
max_duration_ms = 3300000

[declarations]
capabilities = ["agents", "model"]
providers = ["mock"]

[[nodes]]
id = "plan"
kind = "model_call"
provider = "mock"
retry_limit = 2

[[nodes]]
id = "spawn-workers"
kind = "spawn_child_agent"

[[nodes]]
id = "wait-workers"
kind = "wait_for_agents"

[[nodes]]
id = "integrate"
kind = "model_call"
provider = "mock"
retry_limit = 2

[[nodes]]
id = "review"
kind = "review"
provider = "mock"
retry_limit = 2

[[nodes]]
id = "revision"
kind = "loop"
max_iterations = 8

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "plan"
to = "spawn-workers"

[[edges]]
from = "spawn-workers"
to = "wait-workers"

[[edges]]
from = "wait-workers"
to = "integrate"

[[edges]]
from = "integrate"
to = "review"

[[edges]]
from = "review"
to = "revision"

[[edges]]
from = "revision"
to = "spawn-workers"
condition = "review.approved == false"

[[edges]]
from = "revision"
to = "done"
condition = "review.approved == true"
"#;

const DECLARATIVE_GRAPH: &str = r#"
format_version = 1
entry = "emit"

[budget]
max_steps = 10
max_tokens = 1000
max_cost_micros = 1000
max_duration_ms = 10000

[declarations]
capabilities = ["events"]

[[nodes]]
id = "emit"
kind = "emit_event"

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "emit"
to = "done"
"#;
