use std::collections::BTreeMap;

use agentmod_graph_engine::{CompilerLimits, GraphDefinition};

use crate::{
    ApprovalDecision, ApprovalDefaults, BuiltInStyle, ChildAgentLimits, ChildCancellationBehavior,
    ChildJoinBehavior, ChildMemoryAccess, ChildWorkspaceMergePolicy, ChildWorkspaceMode,
    CompactionPreservationRequirement, CompactionSelection, CompactionStrategy, DecisionCapability,
    ExecutionBudgets, GraphSource, HarnessSelection, InterceptorDeclaration,
    MemoryInjectionLocation, MemoryQueryConstruction, MemoryQuerySource, MemoryRetrievalTiming,
    MemoryScope, MemorySelection, MemoryWritePolicy, RetryPolicy, SessionStyleManifest,
    StyleIdentity, StyleKind, TerminationOutcome, TerminationPolicy, TopLevelSelection,
};

/// Constructs one of the five required built-in semantic descriptors.
///
/// # Panics
///
/// Panics only if the SDK's exhaustive built-in version table violates its
/// internal invariant by declaring no version for a [`BuiltInStyle`].
#[must_use]
pub fn built_in_manifest(style: BuiltInStyle) -> SessionStyleManifest {
    let version = built_in_versions(style)
        .last()
        .copied()
        .expect("every built-in style ships at least one version");
    built_in_manifest_for_version(style, version)
        .expect("the latest declared built-in version has an exact descriptor")
}

/// Returns every exact semantic version of a built-in style shipped by this SDK.
///
/// The returned versions are ordered from oldest to newest. Runtime discovery
/// uses this list to expose historical descriptors without inventing a fallback
/// or maintaining a second version catalog.
#[must_use]
pub const fn built_in_versions(style: BuiltInStyle) -> &'static [&'static str] {
    match style {
        BuiltInStyle::PlannerWorker => &["1.1.0", "1.2.0", "1.3.0", "1.4.0"],
        BuiltInStyle::ResearchLoop => &["1.1.0", "1.2.0", "1.3.0"],
        BuiltInStyle::PersistentChat
        | BuiltInStyle::EphemeralTurn
        | BuiltInStyle::DeclarativeGraph => &["1.1.0", "1.2.0"],
    }
}

/// Constructs a built-in semantic descriptor only when the requested version
/// exactly matches a version shipped by this SDK.
///
/// This exact-match constructor prevents callers loading persisted session
/// identities from silently substituting a newer built-in descriptor. Callers
/// should surface `None` as an unavailable-style or migration-required error.
#[must_use]
pub fn built_in_manifest_for_version(
    style: BuiltInStyle,
    version: &str,
) -> Option<SessionStyleManifest> {
    built_in_parts_for_version(style, version).map(|parts| manifest_from_parts(style, parts))
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
        allowed_providers: if parts.graph.contains("provider = \"deterministic-mock\"") {
            vec!["deterministic-mock".to_owned()]
        } else if parts.graph.contains("model_call") || parts.graph.contains("review") {
            vec!["mock".to_owned()]
        } else {
            Vec::new()
        },
        allowed_plugins: vec!["runtime.security".to_owned()],
        context_transforms: Vec::new(),
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

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive immutable built-in version table is kept in one compiler-checked match"
)]
fn built_in_parts_for_version(style: BuiltInStyle, version: &str) -> Option<BuiltInParts> {
    let tuple = match (style, version) {
        (BuiltInStyle::PersistentChat, "1.1.0") => (
            "persistent-chat",
            "1.1.0",
            32,
            PERSISTENT_CHAT_GRAPH_1_1,
            vec!["agents", "approval", "model", "tools"],
            vec!["filesystem"],
            file_memory(),
            summary_compaction(),
            persistent_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        (BuiltInStyle::PersistentChat, "1.2.0") => (
            "persistent-chat",
            "1.2.0",
            32,
            PERSISTENT_CHAT_GRAPH,
            vec!["agents", "approval", "context", "model", "tools"],
            vec![
                "browser",
                "filesystem",
                "git",
                "lsp",
                "mcp",
                "process",
                "web",
            ],
            file_memory(),
            summary_compaction(),
            persistent_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        (BuiltInStyle::EphemeralTurn, "1.1.0") => (
            "ephemeral-turn",
            "1.1.0",
            32,
            EPHEMERAL_TURN_GRAPH_1_1,
            vec!["approval", "context", "model", "tools"],
            vec!["filesystem"],
            no_memory(),
            no_compaction(),
            no_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        (BuiltInStyle::EphemeralTurn, "1.2.0") => (
            "ephemeral-turn",
            "1.2.0",
            32,
            EPHEMERAL_TURN_GRAPH,
            vec!["approval", "context", "model", "tools"],
            vec!["filesystem"],
            no_memory(),
            no_compaction(),
            no_children(),
            vec![TerminationOutcome::CompleteTurn, TerminationOutcome::Fail],
        ),
        (BuiltInStyle::ResearchLoop, "1.1.0") => (
            "research-loop",
            "1.1.0",
            16,
            RESEARCH_LOOP_GRAPH_1_1,
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
        (BuiltInStyle::ResearchLoop, "1.2.0") => (
            "research-loop",
            "1.2.0",
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
        (BuiltInStyle::ResearchLoop, "1.3.0") => (
            "research-loop",
            "1.3.0",
            16,
            include_str!("research_loop_1_3.toml"),
            vec!["approval", "artifacts", "context", "model", "tools"],
            vec!["filesystem", "git", "process"],
            research_memory(),
            no_compaction(),
            no_children(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        (BuiltInStyle::PlannerWorker, "1.1.0") => (
            "planner-worker",
            "1.1.0",
            32,
            PLANNER_WORKER_GRAPH_1_1,
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
        (BuiltInStyle::PlannerWorker, "1.2.0") => (
            "planner-worker",
            "1.2.0",
            4,
            PLANNER_WORKER_GRAPH,
            vec!["agents", "approval", "artifacts", "model"],
            Vec::new(),
            planner_memory(),
            summary_compaction(),
            planner_children_1_2(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        (BuiltInStyle::PlannerWorker, "1.3.0") => (
            "planner-worker",
            "1.3.0",
            4,
            include_str!("planner_worker_1_3.toml"),
            vec!["agents", "approval", "artifacts", "model"],
            Vec::new(),
            planner_memory(),
            summary_compaction(),
            planner_children_1_2(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        (BuiltInStyle::PlannerWorker, "1.4.0") => (
            "planner-worker",
            "1.4.0",
            4,
            include_str!("planner_worker_1_4.toml"),
            vec!["agents", "approval", "artifacts", "model", "tools"],
            vec!["filesystem", "git", "process"],
            planner_memory(),
            summary_compaction(),
            planner_children_1_4(),
            vec![
                TerminationOutcome::CompleteSession,
                TerminationOutcome::Fail,
            ],
        ),
        (BuiltInStyle::DeclarativeGraph, "1.1.0") => (
            "declarative-graph",
            "1.1.0",
            3,
            DECLARATIVE_GRAPH_1_1,
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
        (BuiltInStyle::DeclarativeGraph, "1.2.0") => (
            "declarative-graph",
            "1.2.0",
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
        _ => return None,
    };
    Some(BuiltInParts {
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
    })
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
        plugin: None,
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
    }
}

fn research_memory() -> MemorySelection {
    MemorySelection {
        provider: "file".to_owned(),
        plugin: None,
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
    }
}

fn planner_memory() -> MemorySelection {
    MemorySelection {
        provider: "file".to_owned(),
        plugin: None,
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
    }
}

fn no_memory() -> MemorySelection {
    MemorySelection {
        provider: "none".to_owned(),
        plugin: None,
        scopes: Vec::new(),
        retrieval_timing: MemoryRetrievalTiming::Never,
        query: MemoryQueryConstruction::default(),
        max_items: 0,
        max_injected_bytes: 0,
        write_policy: MemoryWritePolicy::Never,
        injection_location: MemoryInjectionLocation::None,
    }
}

fn summary_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::Summary,
        plugin: None,
        trigger_tokens: Some(750_000),
        reserved_context_tokens: 32_000,
        max_provider_projection_tokens: 250_000,
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
        preservation_requirements: required_projection_records(),
        summary: None,
        summary_max_bytes: 64 * 1024,
        summary_schema_version: 1,
    }
}

fn no_compaction() -> CompactionSelection {
    CompactionSelection {
        strategy: CompactionStrategy::None,
        plugin: None,
        trigger_tokens: None,
        reserved_context_tokens: 0,
        max_provider_projection_tokens: 0,
        preserve_unresolved_tasks: true,
        preserve_active_processes: true,
        preservation_requirements: required_projection_records(),
        summary: None,
        summary_max_bytes: 64 * 1024,
        summary_schema_version: 1,
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
        workspace_merge_policy: None,
        custom_workspace: None,
        inherit_provider: None,
        inherit_model: None,
        inherit_mcp: None,
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
        workspace_merge_policy: None,
        custom_workspace: None,
        inherit_provider: Some(true),
        inherit_model: Some(true),
        inherit_mcp: None,
        context_budget_tokens: Some(64_000),
        per_child_cost_budget_micros: Some(10_000_000),
        tool_groups: Vec::new(),
        memory_access: Some(ChildMemoryAccess::None),
        join_behavior: Some(ChildJoinBehavior::All),
        cancellation_behavior: Some(ChildCancellationBehavior::Cascade),
        reviewer_max_attempts: Some(8),
    }
}

fn planner_children_1_2() -> ChildAgentLimits {
    ChildAgentLimits {
        max_children: 8,
        max_concurrent: 2,
        max_depth: 2,
        per_child_token_budget: 100_000,
        child_style: Some(String::from("ephemeral-turn@1.2.0")),
        workspace_mode: Some(ChildWorkspaceMode::SharedReadOnly),
        workspace_merge_policy: None,
        custom_workspace: None,
        inherit_provider: Some(true),
        inherit_model: Some(true),
        inherit_mcp: None,
        context_budget_tokens: Some(64_000),
        per_child_cost_budget_micros: Some(10_000_000),
        tool_groups: Vec::new(),
        memory_access: Some(ChildMemoryAccess::None),
        join_behavior: Some(ChildJoinBehavior::All),
        cancellation_behavior: Some(ChildCancellationBehavior::Cascade),
        reviewer_max_attempts: Some(2),
    }
}

fn planner_children_1_4() -> ChildAgentLimits {
    ChildAgentLimits {
        max_children: 8,
        max_concurrent: 2,
        max_depth: 2,
        per_child_token_budget: 100_000,
        child_style: Some(String::from("research-loop@1.3.0")),
        workspace_mode: Some(ChildWorkspaceMode::BranchWorkspace),
        workspace_merge_policy: Some(ChildWorkspaceMergePolicy::ManualReview),
        custom_workspace: None,
        inherit_provider: Some(true),
        inherit_model: Some(true),
        inherit_mcp: None,
        context_budget_tokens: Some(64_000),
        per_child_cost_budget_micros: Some(10_000_000),
        tool_groups: vec![
            String::from("filesystem"),
            String::from("git"),
            String::from("process"),
        ],
        memory_access: Some(ChildMemoryAccess::None),
        join_behavior: Some(ChildJoinBehavior::All),
        cancellation_behavior: Some(ChildCancellationBehavior::Cascade),
        reviewer_max_attempts: Some(2),
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
        workspace_merge_policy: None,
        custom_workspace: None,
        inherit_provider: Some(true),
        inherit_model: Some(true),
        inherit_mcp: None,
        context_budget_tokens: Some(32_000),
        per_child_cost_budget_micros: Some(5_000_000),
        tool_groups: vec![String::from("filesystem")],
        memory_access: Some(ChildMemoryAccess::ReadOnly),
        join_behavior: Some(ChildJoinBehavior::All),
        cancellation_behavior: Some(ChildCancellationBehavior::Cascade),
        reviewer_max_attempts: Some(4),
    }
}

const PERSISTENT_CHAT_GRAPH_1_1: &str = r#"
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

const PERSISTENT_CHAT_GRAPH: &str = r#"
format_version = 1
entry = "prepare-context"

[budget]
max_steps = 100
max_tokens = 250000
max_cost_micros = 25000000
max_duration_ms = 900000

[declarations]
capabilities = ["context", "model", "tools"]
tools = [
    "browser.start",
    "browser.navigate",
    "browser.inspect",
    "browser.screenshot",
    "browser.click",
    "browser.type",
    "browser.submit",
    "browser.download",
    "browser.close",
    "filesystem.read",
    "filesystem.list",
    "filesystem.glob",
    "filesystem.grep",
    "filesystem.write",
    "filesystem.edit",
    "filesystem.apply_patch",
    "git.discover",
    "git.status",
    "git.diff",
    "git.changed_files",
    "git.branch",
    "git.dirty",
    "git.worktree_create",
    "git.worktree_cleanup",
    "git.checkpoint_create",
    "git.checkpoint_restore",
    "git.export_patch",
    "lsp.project_root",
    "lsp.diagnostics",
    "lsp.document_symbols",
    "lsp.workspace_symbols",
    "lsp.definition",
    "lsp.references",
    "lsp.hover",
    "lsp.signature_help",
    "lsp.rename",
    "lsp.formatting",
    "lsp.code_actions",
    "mcp.server.list",
    "mcp.capabilities",
    "mcp.invoke",
    "process.run",
    "process.start",
    "process.run_pty",
    "process.start_pty",
    "process.read",
    "process.input",
    "process.resize",
    "process.wait",
    "process.interrupt",
    "process.kill",
    "process.detach",
    "process.reattach",
    "process.list",
    "http.request",
    "web.fetch",
    "web.search",
]
providers = ["deterministic-mock"]

[[variables]]
name = "model_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "respond"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "respond"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "turn_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tool-batch"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[nodes]]
id = "prepare-context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "preserve_history" }

[[nodes]]
id = "respond"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["model_disposition", "model_result"]
retry_limit = 1
configuration = { type = "model_request", disposition_output = "model_disposition", result_output = "model_result" }

[[nodes]]
id = "tool-batch"
kind = "tool_execution_gate"
read_variables = ["model_disposition", "model_result"]
write_variables = ["turn_result"]
read_scopes = ["workspace"]
[nodes.configuration]
type = "provider_tool_batch_execution"
request_reference_variable = "model_result"
disposition_variable = "model_disposition"
maximum_calls = 32
allowed_tools = [
    "browser.start",
    "browser.navigate",
    "browser.inspect",
    "browser.screenshot",
    "browser.click",
    "browser.type",
    "browser.submit",
    "browser.download",
    "browser.close",
    "filesystem.read",
    "filesystem.list",
    "filesystem.glob",
    "filesystem.grep",
    "filesystem.write",
    "filesystem.edit",
    "filesystem.apply_patch",
    "git.discover",
    "git.status",
    "git.diff",
    "git.changed_files",
    "git.branch",
    "git.dirty",
    "git.worktree_create",
    "git.worktree_cleanup",
    "git.checkpoint_create",
    "git.checkpoint_restore",
    "git.export_patch",
    "lsp.project_root",
    "lsp.diagnostics",
    "lsp.document_symbols",
    "lsp.workspace_symbols",
    "lsp.definition",
    "lsp.references",
    "lsp.hover",
    "lsp.signature_help",
    "lsp.rename",
    "lsp.formatting",
    "lsp.code_actions",
    "mcp.server.list",
    "mcp.capabilities",
    "mcp.invoke",
    "process.run",
    "process.start",
    "process.run_pty",
    "process.start_pty",
    "process.read",
    "process.input",
    "process.resize",
    "process.wait",
    "process.interrupt",
    "process.kill",
    "process.detach",
    "process.reattach",
    "process.list",
    "http.request",
    "web.fetch",
    "web.search",
]

[[nodes]]
id = "done"
kind = "complete_turn"
read_variables = ["turn_result"]
configuration = { type = "complete_turn", result_reference_variable = "turn_result", cleanup = "preserve_projection" }

[[edges]]
from = "prepare-context"
to = "respond"

[[edges]]
from = "respond"
to = "tool-batch"

[[edges]]
from = "tool-batch"
to = "done"
"#;

const EPHEMERAL_TURN_GRAPH_1_1: &str = r#"
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
providers = ["deterministic-mock"]

[[variables]]
name = "model_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "respond"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "respond"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "turn_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tool-batch"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[nodes]]
id = "fresh-context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "fresh" }

[[nodes]]
id = "respond"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["model_disposition", "model_result"]
retry_limit = 1
configuration = { type = "model_request", disposition_output = "model_disposition", result_output = "model_result" }

[[nodes]]
id = "tool-batch"
kind = "tool_execution_gate"
read_variables = ["model_disposition", "model_result"]
write_variables = ["turn_result"]
read_scopes = ["workspace"]
configuration = { type = "provider_tool_batch_execution", request_reference_variable = "model_result", disposition_variable = "model_disposition", maximum_calls = 32, allowed_tools = ["filesystem.read"] }

[[nodes]]
id = "done"
kind = "complete_turn"
read_variables = ["turn_result"]
configuration = { type = "complete_turn", result_reference_variable = "turn_result", cleanup = "discard_projection" }

[[edges]]
from = "fresh-context"
to = "respond"

[[edges]]
from = "respond"
to = "tool-batch"

[[edges]]
from = "tool-batch"
to = "done"
"#;

const RESEARCH_LOOP_GRAPH_1_1: &str = r#"
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
providers = ["deterministic-mock"]

[[variables]]
name = "model_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "research"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "model_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "research"
consumers = ["tool-batch"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "research_receipt"
type = { kind = "node_result_reference" }
scope = "run"
producer = "tool-batch"
consumers = ["persist"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "receipt_artifact"
type = { kind = "artifact_reference" }
scope = "run"
producer = "persist"
consumers = ["done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "confidential"

[[variables]]
name = "iteration"
type = { kind = "map", value_type = { kind = "boolean" }, max_entries = 1 }
scope = "run"
producer = "repeat"
consumers = ["repeat"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "fresh-context"
kind = "context_transform"
configuration = { type = "context_transform", strategy = "fresh" }

[[nodes]]
id = "research"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["model_disposition", "model_result"]
retry_limit = 2
configuration = { type = "model_request", disposition_output = "model_disposition", result_output = "model_result" }

[[nodes]]
id = "tool-batch"
kind = "tool_execution_gate"
read_variables = ["model_disposition", "model_result"]
write_variables = ["research_receipt"]
read_scopes = ["workspace"]
configuration = { type = "provider_tool_batch_execution", request_reference_variable = "model_result", disposition_variable = "model_disposition", maximum_calls = 32, allowed_tools = ["filesystem.read"] }

[[nodes]]
id = "persist"
kind = "persist_artifact"
read_variables = ["research_receipt"]
write_variables = ["receipt_artifact"]
configuration = { type = "persist_artifact", content = { kind = "provider_result_text", reference_variable = "research_receipt" }, mime_type = "text/markdown", security = "private", retention = "session" }

[[nodes]]
id = "repeat"
kind = "loop"
read_variables = ["iteration"]
write_variables = ["iteration"]
max_iterations = 3

[[nodes]]
id = "done"
kind = "complete_session"
read_variables = ["receipt_artifact"]

[[edges]]
from = "fresh-context"
to = "research"

[[edges]]
from = "research"
to = "tool-batch"

[[edges]]
from = "tool-batch"
to = "persist"

[[edges]]
from = "persist"
to = "repeat"

[[edges]]
from = "repeat"
to = "fresh-context"
condition = "iteration.remaining == true"
label = "continue"

[[edges]]
from = "repeat"
to = "done"
condition = "iteration.remaining == false"
label = "complete"
"#;

const PLANNER_WORKER_GRAPH_1_1: &str = r#"
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

const PLANNER_WORKER_GRAPH: &str = r#"
format_version = 1
entry = "plan"

[budget]
max_steps = 750
max_tokens = 900000
max_cost_micros = 90000000
max_duration_ms = 3300000

[declarations]
capabilities = ["agents", "artifacts", "model"]
providers = ["deterministic-mock"]

[[variables]]
name = "plan_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "plan"
consumers = ["plan-route"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "plan_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "plan"
consumers = ["plan-route"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "planner_task"
type = { kind = "string" }
scope = "run"
producer = "plan"
consumers = ["spawn-planner"]
mutability = "mutable"
max_size_bytes = 8192
security_classification = "internal"

[[variables]]
name = "evidence_task"
type = { kind = "string" }
scope = "run"
producer = "plan"
consumers = ["spawn-evidence"]
mutability = "mutable"
max_size_bytes = 8192
security_classification = "internal"

[[variables]]
name = "planner_child"
type = { kind = "child_id" }
scope = "run"
producer = "spawn-planner"
consumers = ["wait-planner", "join-workers"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[variables]]
name = "evidence_child"
type = { kind = "child_id" }
scope = "run"
producer = "spawn-evidence"
consumers = ["wait-evidence", "join-workers"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[variables]]
name = "joined_results"
type = { kind = "node_result_reference" }
scope = "run"
producer = "join-workers"
consumers = ["integrate"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "integration_disposition"
type = { kind = "enum", values = ["response_complete", "tool_requests"] }
scope = "run"
producer = "integrate"
consumers = ["integration-route"]
mutability = "mutable"
max_size_bytes = 64
security_classification = "internal"

[[variables]]
name = "integration_result"
type = { kind = "node_result_reference" }
scope = "run"
producer = "integrate"
consumers = ["persist-integration", "review", "done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "internal"

[[variables]]
name = "integration_artifact"
type = { kind = "artifact_reference" }
scope = "run"
producer = "persist-integration"
consumers = ["review", "done"]
mutability = "mutable"
max_size_bytes = 512
security_classification = "confidential"

[[variables]]
name = "iteration"
type = { kind = "map", value_type = { kind = "boolean" }, max_entries = 1 }
scope = "run"
producer = "revision"
consumers = ["revision"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "plan"
kind = "model_call"
provider = "deterministic-mock"
write_variables = ["plan_disposition", "plan_result", "planner_task", "evidence_task"]
retry_limit = 2
configuration = { type = "model_request", disposition_output = "plan_disposition", result_output = "plan_result", provider_options = { mock_planner_phase = "plan" }, json_outputs = { planner_task = "/tasks/0/description", evidence_task = "/tasks/1/description" } }

[[nodes]]
id = "plan-route"
kind = "conditional_branch"
read_variables = ["plan_disposition", "plan_result"]

[[nodes]]
id = "spawn-planner"
kind = "spawn_child_agent"
read_variables = ["planner_task"]
write_variables = ["planner_child"]
configuration = { type = "spawn_child_agent", task_input = { kind = "variable", variable = "planner_task" }, task_id_prefix = "planner-task", child_style = "ephemeral-turn@1.2.0", tool_groups = [], maximum_children = 1, maximum_depth = 2, token_budget = 100000, context_budget_tokens = 64000, cost_budget_micros = 10000000, workspace = { mode = "shared_read_only" }, artifact_references = [], security_classification = "internal", approval_required = true }

[[nodes]]
id = "spawn-evidence"
kind = "spawn_child_agent"
read_variables = ["evidence_task"]
write_variables = ["evidence_child"]
configuration = { type = "spawn_child_agent", task_input = { kind = "variable", variable = "evidence_task" }, task_id_prefix = "evidence-task", child_style = "ephemeral-turn@1.2.0", tool_groups = [], maximum_children = 1, maximum_depth = 2, token_budget = 100000, context_budget_tokens = 64000, cost_budget_micros = 10000000, workspace = { mode = "shared_read_only" }, artifact_references = [], security_classification = "internal", approval_required = true }

[[nodes]]
id = "wait-fanout"
kind = "parallel_branch"
configuration = { type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "join-workers", join_policy = "all" }

[[nodes]]
id = "wait-planner"
kind = "wait_for_agents"
read_variables = ["planner_child"]
configuration = { type = "wait_for_agents", children = { kind = "variable", variable = "planner_child" }, maximum_children = 1, minimum_successes = 1, timeout_ms = 600000, cancellation = "cascade" }

[[nodes]]
id = "wait-evidence"
kind = "wait_for_agents"
read_variables = ["evidence_child"]
configuration = { type = "wait_for_agents", children = { kind = "variable", variable = "evidence_child" }, maximum_children = 1, minimum_successes = 1, timeout_ms = 600000, cancellation = "cascade" }

[[nodes]]
id = "join-workers"
kind = "join_results"
read_variables = ["planner_child", "evidence_child"]
write_variables = ["joined_results"]
configuration = { type = "join_results", required = ["planner", "evidence"], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 600000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "all" }

[[nodes]]
id = "integrate"
kind = "model_call"
provider = "deterministic-mock"
read_variables = ["joined_results"]
write_variables = ["integration_disposition", "integration_result"]
retry_limit = 2
configuration = { type = "model_request", disposition_output = "integration_disposition", result_output = "integration_result", provider_options = { mock_planner_phase = "integrate" } }

[[nodes]]
id = "integration-route"
kind = "conditional_branch"
read_variables = ["integration_disposition"]

[[nodes]]
id = "persist-integration"
kind = "persist_artifact"
read_variables = ["integration_result"]
write_variables = ["integration_artifact"]
configuration = { type = "persist_artifact", content = { kind = "provider_result_text", reference_variable = "integration_result" }, mime_type = "text/markdown", security = "private", retention = "session" }

[[nodes]]
id = "review"
kind = "review"
provider = "deterministic-mock"
read_variables = ["integration_result", "integration_artifact"]
retry_limit = 2
configuration = { type = "review", input = { kind = "variable", variable = "integration_result" }, artifact_references = [], result_schema = { maximum_findings = 16, maximum_finding_bytes = 1024, maximum_rejections = 2, require_artifact_evidence = false }, routes = { approved = "done", revision = "revision", failure = "structured-failure" }, maximum_revisions = 2 }

[[nodes]]
id = "revision"
kind = "loop"
read_variables = ["iteration"]
write_variables = ["iteration"]
max_iterations = 2

[[nodes]]
id = "done"
kind = "complete_session"
read_variables = ["integration_result", "integration_artifact"]

[[nodes]]
id = "structured-failure"
kind = "fail"

[[edges]]
from = "plan"
to = "plan-route"

[[edges]]
from = "plan-route"
to = "spawn-planner"
condition = "plan_disposition == \"response_complete\""
label = "planned"

[[edges]]
from = "plan-route"
to = "structured-failure"
condition = "plan_disposition == \"tool_requests\""
label = "unsupported-plan-tool-request"

[[edges]]
from = "spawn-planner"
to = "spawn-evidence"

[[edges]]
from = "spawn-evidence"
to = "wait-fanout"

[[edges]]
from = "wait-fanout"
to = "wait-planner"
label = "planner"

[[edges]]
from = "wait-fanout"
to = "wait-evidence"
label = "evidence"

[[edges]]
from = "wait-planner"
to = "join-workers"

[[edges]]
from = "wait-evidence"
to = "join-workers"

[[edges]]
from = "join-workers"
to = "integrate"

[[edges]]
from = "integrate"
to = "integration-route"

[[edges]]
from = "integration-route"
to = "persist-integration"
condition = "integration_disposition == \"response_complete\""
label = "integrated"

[[edges]]
from = "integration-route"
to = "structured-failure"
condition = "integration_disposition == \"tool_requests\""
label = "unsupported-tool-request"

[[edges]]
from = "persist-integration"
to = "review"

[[edges]]
from = "review"
to = "done"

[[edges]]
from = "review"
to = "revision"

[[edges]]
from = "review"
to = "structured-failure"

[[edges]]
from = "revision"
to = "spawn-planner"
condition = "iteration.remaining == true"
label = "revise"

[[edges]]
from = "revision"
to = "structured-failure"
condition = "iteration.remaining == false"
label = "revision-limit"
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

[[variables]]
name = "request"
type = { kind = "map", value_type = { kind = "boolean" }, max_entries = 1 }
scope = "run"
producer = "runtime"
consumers = ["branch"]
mutability = "immutable"
max_size_bytes = 128
security_classification = "internal"

[[variables]]
name = "tool_arguments"
type = { kind = "map", value_type = { kind = "string" }, max_entries = 8 }
scope = "run"
producer = "runtime"
consumers = ["tool"]
mutability = "immutable"
max_size_bytes = 4096
security_classification = "internal"

[[variables]]
name = "iteration"
type = { kind = "map", value_type = { kind = "boolean" }, max_entries = 1 }
scope = "run"
producer = "repeat"
consumers = ["repeat"]
mutability = "mutable"
max_size_bytes = 128
security_classification = "internal"

[[nodes]]
id = "branch"
kind = "conditional_branch"
read_variables = ["request"]

[[nodes]]
id = "approval"
kind = "user_approval"
configuration = { type = "user_approval", action_summary = { kind = "static", value = "declarative graph requested user approval" } }

[[nodes]]
id = "tool"
kind = "tool_execution_gate"
configuration = { type = "tool_execution", arguments = { kind = "variable", variable = "tool_arguments" } }
tool = "filesystem.read"
read_scopes = ["workspace"]
read_variables = ["tool_arguments"]

[[nodes]]
id = "repeat"
kind = "loop"
read_variables = ["iteration"]
write_variables = ["iteration"]
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

const DECLARATIVE_GRAPH_1_1: &str = r#"
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
