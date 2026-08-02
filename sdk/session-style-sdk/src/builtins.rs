use std::collections::BTreeMap;

use agentmod_graph_engine::{CompilerLimits, GraphDefinition};

use crate::{
    ApprovalDecision, ApprovalDefaults, BuiltInStyle, ChildAgentLimits, ChildCancellationBehavior,
    ChildJoinBehavior, ChildMemoryAccess, ChildWorkspaceMode, CompactionPreservationRequirement,
    CompactionSelection, CompactionStrategy, DecisionCapability, ExecutionBudgets, GraphSource,
    HarnessSelection, InterceptorDeclaration, MemoryInjectionLocation, MemoryQueryConstruction,
    MemoryQuerySource, MemoryRetrievalTiming, MemoryScope, MemorySelection, MemoryWritePolicy,
    RetryPolicy, SessionStyleManifest, StyleIdentity, StyleKind, TerminationOutcome,
    TerminationPolicy, TopLevelSelection,
};

fn default_summary_max_bytes() -> u32 {
    64 * 1024
}

fn default_summary_schema_version() -> u16 {
    1
}

/// Constructs one of the five required built-in semantic descriptors.
#[must_use]
pub fn built_in_manifest(style: BuiltInStyle) -> SessionStyleManifest {
    let parts = built_in_parts(style);
    manifest_from_parts(style, parts)
}

/// Constructs a built-in semantic descriptor only when the requested version
/// exactly matches the version shipped by this SDK.
///
/// This exact-match constructor prevents callers loading persisted session
/// identities from silently substituting a newer built-in descriptor. Callers
/// should surface `None` as an unavailable-style or migration-required error.
#[must_use]
pub fn built_in_manifest_for_version(
    style: BuiltInStyle,
    version: &str,
) -> Option<SessionStyleManifest> {
    let parts = built_in_parts(style);
    (parts.version == version).then(|| manifest_from_parts(style, parts))
}

fn manifest_from_parts(style: BuiltInStyle, parts: BuiltInParts) -> SessionStyleManifest {
    let retry_attempts = if matches!(
        style,
        BuiltInStyle::ResearchLoop | BuiltInStyle::PlannerWorker
    ) {
        4
    } else {
        2
    };
    let harness_requires_tools = !parts.tool_groups.is_empty();
    SessionStyleManifest {
        schema_version: 1,
        identity: StyleIdentity {
            id: parts.id.to_owned(),
            version: parts.version.to_owned(),
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
        harness: HarnessSelection {
            id: String::from("native"),
            required_capabilities: {
                let mut capabilities = vec![
                    String::from("cancellation"),
                    String::from("streaming"),
                    String::from("structured_context_replacement"),
                    String::from("token_usage"),
                ];
                if harness_requires_tools {
                    capabilities.push(String::from("tool_calls"));
                }
                capabilities
            },
        },
        memory: parts.memory,
        compaction: parts.compaction,
        approvals: ApprovalDefaults {
            default: ApprovalDecision::Ask,
            groups: BTreeMap::from([("filesystem.read".to_owned(), ApprovalDecision::Allow)]),
        },
        budgets: ExecutionBudgets {
            max_iterations: parts.max_iterations,
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
    version: &'static str,
    max_iterations: u32,
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
            "1.1.0",
            32,
            PERSISTENT_CHAT_GRAPH,
            vec!["agents", "approval", "model", "tools"],
            vec!["filesystem"],
            file_memory(),
            summary_compaction(),
            persistent_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        BuiltInStyle::EphemeralTurn => (
            "ephemeral-turn",
            "1.1.0",
            32,
            EPHEMERAL_TURN_GRAPH,
            vec!["approval", "context", "model", "tools"],
            vec!["filesystem"],
            no_memory(),
            no_compaction(),
            no_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        BuiltInStyle::ResearchLoop => (
            "research-loop",
            "1.1.0",
            16,
            RESEARCH_LOOP_GRAPH,
            vec!["approval", "artifacts", "context", "model", "tools"],
            vec!["filesystem"],
            research_memory(),
            no_compaction(),
            no_children(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        BuiltInStyle::PlannerWorker => (
            "planner-worker",
            "1.1.0",
            32,
            PLANNER_WORKER_GRAPH,
            vec!["agents", "approval", "model"],
            Vec::new(),
            planner_memory(),
            summary_compaction(),
            planner_children(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        BuiltInStyle::DeclarativeGraph => (
            "declarative-graph",
            "1.1.0",
            3,
            DECLARATIVE_GRAPH,
            vec!["approval", "tools"],
            vec!["filesystem"],
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
        version: tuple.1,
        max_iterations: tuple.2,
        graph: tuple.3,
        capabilities: tuple.4,
        tool_groups: tuple.5,
        memory: tuple.6,
        compaction: tuple.7,
        children: tuple.8,
        outcomes: tuple.9,
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
        retrieval_timing: MemoryRetrievalTiming::TurnStart,
        query: MemoryQueryConstruction {
            source: MemoryQuerySource::CurrentInput,
            include_active_artifacts: true,
            include_style_context: true,
            max_query_bytes: 16 * 1024,
        },
        max_items: 32,
        max_injected_bytes: 256 * 1024,
        write_policy: MemoryWritePolicy::TurnCompletion,
        injection_location: MemoryInjectionLocation::BeforeCurrentInput,
        auto_write: None,
    }
}

fn research_memory() -> MemorySelection {
    MemorySelection {
        provider: "file".to_owned(),
        scopes: vec![MemoryScope::Session, MemoryScope::Project],
        retrieval_timing: MemoryRetrievalTiming::IterationStart,
        query: MemoryQueryConstruction {
            source: MemoryQuerySource::SessionGoal,
            include_active_artifacts: true,
            include_style_context: true,
            max_query_bytes: 32 * 1024,
        },
        max_items: 64,
        max_injected_bytes: 512 * 1024,
        write_policy: MemoryWritePolicy::IterationCompletion,
        injection_location: MemoryInjectionLocation::BeforeCurrentInput,
        auto_write: None,
    }
}

fn planner_memory() -> MemorySelection {
    MemorySelection {
        provider: "file".to_owned(),
        scopes: vec![MemoryScope::Session, MemoryScope::Project],
        retrieval_timing: MemoryRetrievalTiming::BeforeModelRequest,
        query: MemoryQueryConstruction {
            source: MemoryQuerySource::CurrentInputAndGoal,
            include_active_artifacts: true,
            include_style_context: true,
            max_query_bytes: 32 * 1024,
        },
        max_items: 64,
        max_injected_bytes: 512 * 1024,
        write_policy: MemoryWritePolicy::SessionCompletion,
        injection_location: MemoryInjectionLocation::BeforeCurrentInput,
        auto_write: None,
    }
}

fn no_memory() -> MemorySelection {
    MemorySelection {
        provider: "none".to_owned(),
        scopes: Vec::new(),
        retrieval_timing: MemoryRetrievalTiming::Never,
        query: MemoryQueryConstruction::default(),
        max_items: 0,
        max_injected_bytes: 0,
        write_policy: MemoryWritePolicy::Never,
        injection_location: MemoryInjectionLocation::None,
        auto_write: None,
    }
}

fn summary_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::Summary,
        trigger_tokens: Some(750_000),
        reserved_context_tokens: 32_000,
        max_provider_projection_tokens: 250_000,
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
        preservation_requirements: required_projection_records(),
        summary: None,
        summary_max_bytes: default_summary_max_bytes(),
        summary_schema_version: default_summary_schema_version(),
    }
}

fn no_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::None,
        trigger_tokens: None,
        reserved_context_tokens: 0,
        max_provider_projection_tokens: 0,
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
        preservation_requirements: required_projection_records(),
        summary: None,
        summary_max_bytes: default_summary_max_bytes(),
        summary_schema_version: default_summary_schema_version(),
    }
}

fn required_projection_records() -> Vec<CompactionPreservationRequirement> {
    vec![
        CompactionPreservationRequirement::SystemInstructions,
        CompactionPreservationRequirement::CurrentInput,
        CompactionPreservationRequirement::PendingControlState,
        CompactionPreservationRequirement::ArtifactReferences,
        CompactionPreservationRequirement::MemoryProvenance,
        CompactionPreservationRequirement::ActiveGraphState,
        CompactionPreservationRequirement::ToolCallCorrelation,
    ]
}

fn no_children() -> ChildAgentLimits {
    ChildAgentLimits {
        max_children: 0,
        max_concurrent: 0,
        max_depth: 0,
        per_child_token_budget: 0,
        child_style: None,
        workspace_mode: None,
        custom_workspace: None,
        inherit_provider: None,
        inherit_model: None,
        context_budget_tokens: None,
        per_child_cost_budget_micros: None,
        tool_groups: Vec::new(),
        memory_access: None,
        join_behavior: None,
        cancellation_behavior: None,
        reviewer_max_attempts: None,
    }
}

fn planner_children() -> ChildAgentLimits {
    ChildAgentLimits {
        max_children: 16,
        max_concurrent: 4,
        max_depth: 2,
        per_child_token_budget: 100_000,
        child_style: Some(String::from("ephemeral-turn@1.1.0")),
        workspace_mode: Some(ChildWorkspaceMode::SharedReadOnly),
        custom_workspace: None,
        inherit_provider: Some(true),
        inherit_model: Some(true),
        context_budget_tokens: Some(64_000),
        per_child_cost_budget_micros: Some(10_000_000),
        tool_groups: Vec::new(),
        memory_access: Some(ChildMemoryAccess::None),
        join_behavior: Some(ChildJoinBehavior::All),
        cancellation_behavior: Some(ChildCancellationBehavior::Cascade),
        reviewer_max_attempts: Some(8),
    }
}

fn persistent_children() -> ChildAgentLimits {
    ChildAgentLimits {
        max_children: 8,
        max_concurrent: 2,
        max_depth: 2,
        per_child_token_budget: 50_000,
        child_style: Some(String::from("ephemeral-turn@1.1.0")),
        workspace_mode: Some(ChildWorkspaceMode::SharedReadOnly),
        custom_workspace: None,
        inherit_provider: Some(true),
        inherit_model: Some(true),
        context_budget_tokens: Some(32_000),
        per_child_cost_budget_micros: Some(5_000_000),
        tool_groups: vec![String::from("filesystem")],
        memory_access: Some(ChildMemoryAccess::ReadOnly),
        join_behavior: Some(ChildJoinBehavior::All),
        cancellation_behavior: Some(ChildCancellationBehavior::Cascade),
        reviewer_max_attempts: Some(4),
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
capabilities = ["context", "model", "tools"]
tools = ["filesystem.read"]
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
id = "tool"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

[[nodes]]
id = "done"
kind = "complete_turn"

[[edges]]
from = "fresh-context"
to = "respond"

[[edges]]
from = "respond"
to = "tool"

[[edges]]
from = "tool"
to = "done"
"#;

const RESEARCH_LOOP_GRAPH: &str = r#"
format_version = 1
entry = "fresh-context"

[budget]
max_steps = 500
max_tokens = 750000
max_cost_micros = 75000000
max_duration_ms = 2700000

[declarations]
capabilities = ["artifacts", "context", "model", "tools"]
tools = ["filesystem.read"]
providers = ["mock"]

[[nodes]]
id = "fresh-context"
kind = "context_transform"

[[nodes]]
id = "research"
kind = "model_call"
provider = "mock"
retry_limit = 2

[[nodes]]
id = "tool"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

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
from = "fresh-context"
to = "research"

[[edges]]
from = "research"
to = "tool"

[[edges]]
from = "tool"
to = "persist"

[[edges]]
from = "persist"
to = "repeat"

[[edges]]
from = "repeat"
to = "fresh-context"
condition = "completion.criteria_met == false"
label = "continue"

[[edges]]
from = "repeat"
to = "done"
condition = "completion.criteria_met == true"
label = "complete"
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
entry = "branch"

[budget]
max_steps = 64
max_tokens = 1000
max_cost_micros = 1000
max_duration_ms = 10000

[declarations]
capabilities = ["approval", "tools"]
tools = ["filesystem.read"]

[[nodes]]
id = "branch"
kind = "conditional_branch"

[[nodes]]
id = "approval"
kind = "user_approval"

[[nodes]]
id = "tool"
kind = "tool_execution_gate"
tool = "filesystem.read"
read_scopes = ["workspace"]

[[nodes]]
id = "repeat"
kind = "loop"
max_iterations = 3

[[nodes]]
id = "done"
kind = "complete_session"

[[edges]]
from = "branch"
to = "approval"
condition = "request.requires_approval == true"
label = "require-approval"

[[edges]]
from = "branch"
to = "tool"
condition = "request.requires_approval == false"
label = "skip-approval"

[[edges]]
from = "approval"
to = "tool"

[[edges]]
from = "tool"
to = "repeat"

[[edges]]
from = "repeat"
to = "tool"
condition = "iteration.remaining == true"
label = "continue"

[[edges]]
from = "repeat"
to = "done"
condition = "iteration.remaining == false"
label = "complete"
"#;
