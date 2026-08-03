//! Isolated plugin-node process proof, invoked by the cross-platform E2E scripts.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_event_pipeline::BlockingPipelineBuilder;
use agentmod_primitives::{CausationId, ContentHash, Sequence, SessionId, Version};
use agentmod_runtime_data::{
    RuntimeData,
    cancellation::{
        ClearRuntimeCancellationDataCommand, RequestRuntimeCancellationDataCommand,
        RuntimeCancellationControlDataPort, RuntimeCancellationData,
    },
    identity::{AllocateEventIdentityDataRequest, EventIdentityDataPort},
    node_executor::RuntimeNodeExecutorData,
    plugin::{
        ActivatePluginsDataRequest, InvokePluginContextTransformDataRequest,
        PluginCatalogDataRecord, PluginContextTransformDataRecord,
        PluginContextTransformLifecycleData, PluginDataError, PluginDataPort,
        PluginInvocationCancellationTargetDataRecord, RuntimePluginData, compile_plugin_catalog,
    },
    plugin_receipt::RuntimePluginNodeReceiptData,
    plugin_receipt::{PluginInvocationReceiptDataIdentity, PluginNodeReceiptDataPort},
};
use agentmod_runtime_dependency::{
    DependencyError, DependencyStorageHealthRequest, DependencyStorageHealthResponse,
    LocalRuntimeDependencies, RuntimeDependencyPort,
    cancellation::RuntimeCancellationDependency,
    continuation::{
        ContinuationDependencyError, ContinuationDependencyPort, DependencyContinuationRecord,
        DependencyCreateContinuationRequest, DependencyTransitionContinuationRequest,
        DependencyTransitionContinuationResponse, FileContinuationDependency,
    },
    harness::{
        DependencyCommand, DependencyEventStream, DependencyReply, HarnessDependencyError,
        HarnessDependencyPort,
    },
    harness_registry::{
        DependencyHarnessDescriptor, HarnessRegistryDependencyError, HarnessRegistryDependencyPort,
    },
    identity::{
        DependencyAllocateEventIdentityRequest, DependencyEventIdentity,
        EventIdentityDependencyError, EventIdentityDependencyPort,
    },
    journal::{
        DependencyAppendJournalRequest, DependencyAppendJournalResponse,
        DependencyRecoverJournalRequest, DependencyRecoverJournalResponse,
        DependencyScanJournalRequest, DependencyScanJournalResponse, JournalDependencyError,
        JournalDependencyPort,
    },
    plugin::{
        DependencyPluginManifestSource, ProcessPluginDependency, ProcessPluginDependencyConfig,
        RuntimePluginDependencyPort,
    },
    plugin_receipt::FilePluginNodeReceiptDependency,
    registry::{
        ChildMessageDependencyError, DependencyAppendChildMessageRequest,
        DependencyChildMessageReceipt, DependencyCreateBranchRequest,
        DependencyCreateChildSessionRequest, DependencyCreateSessionRequest,
        DependencyCreatedSession, DependencyListSessionsRequest, DependencyPrepareSessionRequest,
        DependencyPreparedSession, DependencySessionMetadata, SessionCatalogDependencyError,
        SessionCatalogDependencyPort,
    },
    scheduler::{
        DependencyRuntimeSchedule, DependencyScheduleStoreResult, DependencyScheduledExecution,
        RuntimeSchedulerDependencyError, RuntimeSchedulerDependencyPort,
    },
    style::{
        DependencyStyleCacheLoadRequest, DependencyStyleCacheRecord,
        DependencyStyleCacheStoreRequest, DependencyStyleDiscovery,
        DependencyStyleDiscoveryRequest, SessionStyleDependencyError, SessionStyleDependencyPort,
    },
    tool::{
        DependencyCancelToolRequest, DependencyToolCommand, DependencyToolEvent,
        DependencyToolReceipt, ToolHostDependencyError, ToolHostDependencyPort,
    },
};
use agentmod_runtime_logic::{
    RuntimeLogic,
    action::ConsequentialAction,
    harness::ProviderExecutionPolicy,
    node_execution::NodeWorkIdentity,
    node_executor::{
        NodeExecutorBoundary, NodeExecutorSource, ResolvedNodeExecutor,
        inspect_node_executor_capabilities,
    },
    permission::{PermissionEffect, PermissionMatcher, PermissionPolicy, PermissionRule},
    persistence::{
        CommitDurability, CommitSessionEventCommand, LoadSessionCommand, SessionPersistenceLogic,
        SessionPersistenceLogicPort,
    },
    plugin::{
        ExecutePluginNodeCommand, LoadPluginNodeStateCommand, PersistPluginNodeStateCommand,
        PluginCompositionLogic, PluginInvocationCancellationTarget, PluginNodeExecutionError,
        PluginNodeExecutorLogicPort, PluginNodeStatePersistenceError,
        PluginNodeStatePersistenceLogicPort, PluginNodeStateReadError,
        PluginNodeStateReadLogicPort, PluginNodeStateScope, plugin_invocation_cancellation_target,
        plugin_node_state_persistence_digests, plugin_node_state_persistence_request_hash,
        plugin_node_state_read_digests, plugin_node_state_read_request_hash,
        plugin_node_state_value_hash,
    },
    plugin_context_turn::{
        DrivePluginContextTransformCommand, PluginContextTransformTurnError,
        PluginContextTransformTurnPort, ProductionPluginContextTransformTurn,
    },
    plugin_turn::{
        AuthorizePluginTurnCommand, DrivePluginTurnCommand, PluginNodeInvocationPolicy,
        PluginTurnAuthorization, PluginTurnAuthorizationError, PluginTurnAuthorizationPort,
        PluginTurnOutcome, ProductionPluginNodeTurnPort, ProductionPluginTurnRuntime,
    },
    registry::{CreateSessionCommand, SessionRegistryLogicPort},
    session::{
        ContextBoundaryCompletedEvent, ContextBoundaryIdentity, ContextBoundaryOrigin,
        ContextBoundaryStartedEvent, ContextPhaseCompletedEvent, ContextPhaseIdentity,
        ContextPhaseStartedEvent, PluginNodeInvocationState, PluginSetActivatedEvent,
        RuntimeCommittedEvent, SessionNodeExecutorBoundary, SessionNodeExecutorSource,
        StyleExecutionContract, StyleExecutionInitializedEvent, StyleNodeEnteredEvent,
    },
    style::{
        InspectStyleCommand, SessionStyleLogicPort, StyleContextTransformDescriptor,
        StyleEnvironment,
    },
    turn::{
        ApprovalTurnLogicPort, ResolveTurnApprovalCommand, RunTurnCommand, TurnLogic, TurnLogicPort,
    },
};
use agentmod_runtime_service::RuntimeStyleServiceConfig;
use agentmod_session_style_sdk::{
    BuiltInStyle, CompiledSessionStyle, ContextTransformLifecycle, ContextTransformSelection,
    GraphSource, StyleKind, built_in_manifest,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use tempfile::TempDir;

fn executable(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} must point to a built executable for this ignored process test")
    }))
}

fn executor_json(
    id: &str,
    handler: &str,
    capability: &str,
    timeout_ms: u64,
    external_effects: bool,
) -> Value {
    json!({
        "executor_id": id,
        "version": "1.0.0",
        "runtime_api": "^1.0",
        "node_kind": "model_call",
        "handler": handler,
        "capabilities": ["model", capability],
        "input_schema": "{\"type\":\"object\"}",
        "output_schema": "{\"type\":\"object\"}",
        "timeout_ms": timeout_ms,
        "failure_policy": {"kind":"reject"},
        "idempotency": "non_idempotent",
        "required_permissions": {
            "tools":["filesystem.read"],
            "network":["api.example"]
        },
        "state_scope": "session",
        "external_effects": external_effects,
    })
}

fn manifest(worker: &str) -> String {
    serde_json::to_string(&json!({
        "schema_version": 1,
        "identity": {
            "id": "fixture.node",
            "version": "1.0.0",
            "runtime_api": "^1.0",
        },
        "category": "graph_node",
        "scope": "session",
        "classification": "blocking",
        "entrypoint": {"kind":"process","program":worker,"args":[]},
        "trust": "approved_third_party",
        "isolation": "process",
        "required_capabilities": [],
        "provided_capabilities": [
            "model",
            "plugin.success",
            "plugin.graph",
            "plugin.graph_action",
            "plugin.graph_invalid",
            "plugin.invalid",
            "plugin.timeout",
            "plugin.unavailable"
        ],
        "subscribed_events": [],
        "authorities": {"read":["invocation_state"],"proposed_write":[]},
        "permissions": {
            "tools":["filesystem.read"],
            "network":["api.example"]
        },
        "ordering": {"stage":0,"priority":0,"before":[],"after":[]},
        "configuration": {
            "schema_id":"fixture.node.config",
            "schema_version":1,
            "required":false,
            "source":{
                "kind":"inline_json",
                "document":"{\"type\":\"object\",\"additionalProperties\":false}"
            }
        },
        "failure_policy":{"kind":"reject"},
        "timeout_ms":1000,
        "state_migration_version":1,
        "node_executors":[
            executor_json("fixture.graph", "graph_success", "plugin.graph", 500, false),
            executor_json(
                "fixture.graph.action",
                "graph_action",
                "plugin.graph_action",
                500,
                true
            ),
            executor_json(
                "fixture.graph.invalid_transition",
                "graph_success",
                "plugin.graph_invalid",
                500,
                false
            ),
            executor_json("fixture.success", "execute_echo", "plugin.success", 500, false),
            executor_json("fixture.invalid", "invalid_output", "plugin.invalid", 500, false),
            executor_json("fixture.timeout", "timeout_effect", "plugin.timeout", 50, true),
            executor_json(
                "fixture.unavailable",
                "execute_echo",
                "plugin.unavailable",
                500,
                true
            )
        ]
    }))
    .expect("manifest JSON")
}

fn context_transform_json(id: &str, handler: &str, timeout_ms: u64) -> Value {
    json!({
        "transform_id": id,
        "version": "1.0.0",
        "runtime_api": "^1.0",
        "handler": handler,
        "lifecycle": "before_model_request",
        "capabilities": ["context.redaction"],
        "input_schema": "{\"type\":\"object\",\"required\":[\"projection\"]}",
        "output_schema": "{\"type\":\"array\"}",
        "timeout_ms": timeout_ms,
        "failure_policy": {"kind":"reject"},
        "idempotency": "idempotent",
        "required_permissions": {"tools":[],"network":[]},
        "state_scope": "model_call",
        "external_effects": false,
    })
}

fn context_manifest(worker: &str) -> String {
    serde_json::to_string(&json!({
        "schema_version": 1,
        "identity": {
            "id": "fixture.context",
            "version": "1.0.0",
            "runtime_api": "^1.0",
        },
        "category": "context_transform",
        "scope": "session",
        "classification": "blocking",
        "entrypoint": {"kind":"process","program":worker,"args":[]},
        "trust": "approved_third_party",
        "isolation": "process",
        "required_capabilities": [],
        "provided_capabilities": ["context.redaction"],
        "subscribed_events": [],
        "authorities": {"read":["invocation_state"],"proposed_write":[]},
        "permissions": {"tools":[],"network":[]},
        "ordering": {"stage":0,"priority":0,"before":[],"after":[]},
        "configuration": {
            "schema_id":"fixture.context.config",
            "schema_version":1,
            "required":false,
            "source":{
                "kind":"inline_json",
                "document":"{\"type\":\"object\",\"additionalProperties\":false}"
            }
        },
        "failure_policy":{"kind":"reject"},
        "timeout_ms":1000,
        "state_migration_version":1,
        "context_transforms":[
            context_transform_json("fixture.redact", "redact_projection", 500),
            context_transform_json("fixture.invalid", "invalid_transform_response", 500),
            context_transform_json("fixture.timeout", "timeout_transform", 50)
        ]
    }))
    .expect("context-transform manifest JSON")
}

fn context_transform_request(
    declaration: &PluginContextTransformDataRecord,
    invocation_id: &str,
    configuration_reference: ContentHash,
) -> InvokePluginContextTransformDataRequest {
    let input = json!({
        "projection": [
            {"role": "user", "content": "bounded private context"}
        ]
    });
    let readable_state = json!({"classification":"private"});
    let request_hash = serde_json::to_vec(&(
        "agentmod.plugin.context-transform.request.v1",
        "fixture.context",
        invocation_id,
        &declaration.transform_id,
        &declaration.version,
        "before_model_request",
        &declaration.handler,
        declaration.timeout_ms,
        configuration_reference,
        &input,
        &readable_state,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .expect("context transform request hash");
    let target = plugin_invocation_cancellation_target(
        "context-process-session",
        "context-process-run",
        "fixture.context",
        "1.0.0",
        invocation_id,
        &declaration.transform_id,
        declaration.declaration_hash,
        request_hash,
    )
    .expect("context transform cancellation target");
    InvokePluginContextTransformDataRequest {
        cancellation_target: PluginInvocationCancellationTargetDataRecord {
            session_id: target.session_id,
            run_id: target.run_id,
            plugin_id: target.plugin_id,
            plugin_version: target.plugin_version,
            invocation_id: target.invocation_id,
            invocation_digest: target.invocation_digest,
            operation_id: target.operation_id,
            declaration_hash: target.declaration_hash,
            request_hash: target.request_hash,
        },
        session_id: String::from("context-process-session"),
        plugin_id: String::from("fixture.context"),
        invocation_id: invocation_id.to_owned(),
        transform_id: declaration.transform_id.clone(),
        transform_version: declaration.version.clone(),
        declaration_hash: declaration.declaration_hash,
        timeout_ms: declaration.timeout_ms,
        configuration_reference,
        lifecycle: PluginContextTransformLifecycleData::BeforeModelRequest,
        handler: declaration.handler.clone(),
        input,
        readable_state,
        cancellation_id: format!("cancel-{invocation_id}"),
    }
}

fn write_context_transform_style(
    root: &Path,
    declaration: &PluginContextTransformDataRecord,
    configuration_reference: ContentHash,
) -> String {
    let style_id = String::from("process-context-transform");
    let mut style = built_in_manifest(BuiltInStyle::PersistentChat);
    style.identity.id.clone_from(&style_id);
    style.identity.runtime_api = String::from("^1.0");
    style.kind = StyleKind::Custom;
    style.built_in_semantic = None;
    style.interceptors.clear();
    style.allowed_plugins = vec![String::from("fixture.context")];
    style.context_transforms = vec![ContextTransformSelection {
        plugin_id: String::from("fixture.context"),
        transform_id: declaration.transform_id.clone(),
        version: declaration.version.clone(),
        declaration_hash: declaration.declaration_hash,
        lifecycle: ContextTransformLifecycle::BeforeModelRequest,
        configuration_reference,
    }];
    if !style
        .allowed_providers
        .iter()
        .any(|provider| provider == "mock")
    {
        style.allowed_providers.push(String::from("mock"));
    }
    fs::create_dir_all(root).expect("context style root");
    fs::write(
        root.join(format!("{style_id}.json")),
        serde_json::to_vec(&style).expect("context style JSON"),
    )
    .expect("write context style");
    style_id
}

fn command(
    record: &agentmod_runtime_data::plugin::PluginNodeExecutorDataRecord,
    step: u64,
) -> ExecutePluginNodeCommand {
    let configuration = ContentHash::digest(format!("configuration-{step}").as_bytes());
    ExecutePluginNodeCommand {
        session_id: String::from("01900000-0000-7000-8000-000000000001"),
        work: NodeWorkIdentity {
            run_id: String::from("process-plugin-run"),
            node_id: format!("node-{step}"),
            branch_path: vec![String::from("process-proof")],
            attempt: 1,
            loop_iteration: 0,
            step,
        },
        executor: ResolvedNodeExecutor {
            node_id: format!("node-{step}"),
            node_kind: record.node_kind.clone(),
            implementation_id: record.executor_id.clone(),
            implementation_version: record.version.clone(),
            source: NodeExecutorSource::Plugin {
                plugin_id: String::from("fixture.node"),
            },
            boundary: NodeExecutorBoundary::PluginHost,
            required_capabilities: record.capabilities.clone(),
            resolved_capabilities: record.capabilities.clone(),
            runtime_api_requirement: record.runtime_api.clone(),
            executor_declaration_hash: record.declaration_hash,
            adapter_configuration_reference: configuration,
        },
        adapter_configuration_reference: configuration,
        input: json!({"value":step}),
        readable_state: json!({"classification":"internal"}),
        cancellation_id: format!("cancel-{step}"),
    }
}

#[derive(Clone, Debug)]
struct ProcessRuntimeDependencies {
    local: LocalRuntimeDependencies,
    continuations: FileContinuationDependency,
}

impl ProcessRuntimeDependencies {
    fn new(sessions_root: PathBuf) -> Self {
        Self {
            local: LocalRuntimeDependencies,
            continuations: FileContinuationDependency::new(sessions_root),
        }
    }
}

impl RuntimeDependencyPort for ProcessRuntimeDependencies {
    fn check_storage(
        &self,
        request: DependencyStorageHealthRequest,
    ) -> Result<DependencyStorageHealthResponse, DependencyError> {
        self.local.check_storage(request)
    }
}

impl agentmod_runtime_dependency::execution_plan::ExecutionPlanDependencyPort
    for ProcessRuntimeDependencies
{
    fn store_execution_plan(
        &self,
        request: agentmod_runtime_dependency::execution_plan::DependencyStoreExecutionPlanRequest,
    ) -> Result<
        agentmod_runtime_dependency::execution_plan::DependencyStoreExecutionPlanResponse,
        agentmod_runtime_dependency::execution_plan::ExecutionPlanDependencyError,
    > {
        agentmod_runtime_dependency::execution_plan::LocalExecutionPlanDependency
            .store_execution_plan(request)
    }

    fn load_execution_plan(
        &self,
        request: agentmod_runtime_dependency::execution_plan::DependencyLoadExecutionPlanRequest,
    ) -> Result<
        agentmod_runtime_dependency::execution_plan::DependencyLoadExecutionPlanResult,
        agentmod_runtime_dependency::execution_plan::ExecutionPlanDependencyError,
    > {
        agentmod_runtime_dependency::execution_plan::LocalExecutionPlanDependency
            .load_execution_plan(request)
    }
}

impl SessionStyleDependencyPort for ProcessRuntimeDependencies {
    fn discover_session_styles(
        &self,
        request: DependencyStyleDiscoveryRequest,
    ) -> Result<DependencyStyleDiscovery, SessionStyleDependencyError> {
        self.local.discover_session_styles(request)
    }

    fn load_session_style_cache(
        &self,
        request: DependencyStyleCacheLoadRequest,
    ) -> Result<Option<DependencyStyleCacheRecord>, SessionStyleDependencyError> {
        self.local.load_session_style_cache(request)
    }

    fn store_session_style_cache(
        &self,
        request: DependencyStyleCacheStoreRequest,
    ) -> Result<(), SessionStyleDependencyError> {
        self.local.store_session_style_cache(request)
    }
}

impl SessionCatalogDependencyPort for ProcessRuntimeDependencies {
    fn prepare_session(
        &self,
        request: DependencyPrepareSessionRequest,
    ) -> Result<DependencyPreparedSession, SessionCatalogDependencyError> {
        self.local.prepare_session(request)
    }

    fn create_session(
        &self,
        request: DependencyCreateSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        self.local.create_session(request)
    }

    fn create_branch(
        &self,
        request: DependencyCreateBranchRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        self.local.create_branch(request)
    }

    fn create_child_session(
        &self,
        request: DependencyCreateChildSessionRequest,
    ) -> Result<DependencyCreatedSession, SessionCatalogDependencyError> {
        self.local.create_child_session(request)
    }

    fn append_child_message(
        &self,
        request: DependencyAppendChildMessageRequest,
    ) -> Result<DependencyChildMessageReceipt, ChildMessageDependencyError> {
        self.local.append_child_message(request)
    }

    fn list_sessions(
        &self,
        request: DependencyListSessionsRequest,
    ) -> Result<Vec<DependencySessionMetadata>, SessionCatalogDependencyError> {
        self.local.list_sessions(request)
    }
}

impl JournalDependencyPort for ProcessRuntimeDependencies {
    fn append(
        &self,
        request: DependencyAppendJournalRequest,
    ) -> Result<DependencyAppendJournalResponse, JournalDependencyError> {
        self.local.append(request)
    }

    fn scan(
        &self,
        request: DependencyScanJournalRequest,
    ) -> Result<DependencyScanJournalResponse, JournalDependencyError> {
        self.local.scan(request)
    }

    fn recover_tail(
        &self,
        request: DependencyRecoverJournalRequest,
    ) -> Result<DependencyRecoverJournalResponse, JournalDependencyError> {
        self.local.recover_tail(request)
    }
}

impl EventIdentityDependencyPort for ProcessRuntimeDependencies {
    fn allocate_event_identity(
        &self,
        request: DependencyAllocateEventIdentityRequest,
    ) -> Result<DependencyEventIdentity, EventIdentityDependencyError> {
        self.local.allocate_event_identity(request)
    }
}

impl HarnessRegistryDependencyPort for ProcessRuntimeDependencies {
    fn list_harnesses(
        &self,
    ) -> Result<Vec<DependencyHarnessDescriptor>, HarnessRegistryDependencyError> {
        self.local.list_harnesses()
    }
}

impl ContinuationDependencyPort for ProcessRuntimeDependencies {
    fn create_continuation(
        &self,
        request: DependencyCreateContinuationRequest,
    ) -> Result<(), ContinuationDependencyError> {
        self.continuations.create_continuation(request)
    }

    fn load_continuation(
        &self,
        session_id: &str,
        id: &str,
    ) -> Result<DependencyContinuationRecord, ContinuationDependencyError> {
        self.continuations.load_continuation(session_id, id)
    }

    fn transition_continuation(
        &self,
        request: DependencyTransitionContinuationRequest,
    ) -> Result<DependencyTransitionContinuationResponse, ContinuationDependencyError> {
        self.continuations.transition_continuation(request)
    }

    fn list_continuations(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyContinuationRecord>, ContinuationDependencyError> {
        self.continuations.list_continuations(limit)
    }
}

#[async_trait]
impl HarnessDependencyPort for ProcessRuntimeDependencies {
    async fn exchange(
        &self,
        _command: DependencyCommand,
    ) -> Result<DependencyReply, HarnessDependencyError> {
        Err(HarnessDependencyError::Unavailable)
    }

    async fn exchange_events(
        &self,
        _command: DependencyCommand,
    ) -> Result<DependencyEventStream, HarnessDependencyError> {
        Err(HarnessDependencyError::Unavailable)
    }

    async fn shutdown(&self) {}
}

#[async_trait]
impl ToolHostDependencyPort for ProcessRuntimeDependencies {
    async fn execute(
        &self,
        command: DependencyToolCommand,
    ) -> Result<Vec<DependencyToolEvent>, ToolHostDependencyError> {
        if command.tool != "filesystem.read" {
            return Err(ToolHostDependencyError::Unavailable);
        }
        Ok(vec![
            DependencyToolEvent::Started {
                call_id: command.call_id.clone(),
            },
            DependencyToolEvent::Completed {
                call_id: command.call_id,
                result: json!({"content":"fixture plugin action result"}),
                artifact: None,
                truncated: false,
            },
        ])
    }

    async fn cancel(
        &self,
        _request: DependencyCancelToolRequest,
    ) -> Result<bool, ToolHostDependencyError> {
        Ok(false)
    }

    fn list_receipts(&self) -> Result<Vec<DependencyToolReceipt>, ToolHostDependencyError> {
        Ok(Vec::new())
    }

    async fn shutdown(&self) {}
}

impl RuntimeSchedulerDependencyPort for ProcessRuntimeDependencies {
    fn upsert(
        &self,
        schedule: DependencyRuntimeSchedule,
    ) -> Result<DependencyScheduleStoreResult, RuntimeSchedulerDependencyError> {
        self.local.upsert(schedule)
    }

    fn remove(&self, schedule_id: &str) -> Result<bool, RuntimeSchedulerDependencyError> {
        self.local.remove(schedule_id)
    }

    fn list(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyRuntimeSchedule>, RuntimeSchedulerDependencyError> {
        self.local.list(limit)
    }

    fn claim_due(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.local.claim_due(limit)
    }

    fn list_pending_executions(
        &self,
        limit: u32,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.local.list_pending_executions(limit)
    }

    fn fire_runtime_event(
        &self,
        source_session_id: &str,
        event_id: &str,
        event_type: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.local
            .fire_runtime_event(source_session_id, event_id, event_type)
    }

    fn fire_process_output(
        &self,
        source_session_id: &str,
        output_id: &str,
        process_id: &str,
        output: &str,
    ) -> Result<Vec<DependencyScheduledExecution>, RuntimeSchedulerDependencyError> {
        self.local
            .fire_process_output(source_session_id, output_id, process_id, output)
    }

    fn complete_execution(
        &self,
        execution_id: &str,
        succeeded: bool,
    ) -> Result<bool, RuntimeSchedulerDependencyError> {
        self.local.complete_execution(execution_id, succeeded)
    }
}

type ProcessRuntimeData = RuntimeData<ProcessRuntimeDependencies>;

fn turn_policy(user_effect: PermissionEffect) -> ProviderExecutionPolicy {
    let pipeline = || {
        Arc::new(
            BlockingPipelineBuilder::<agentmod_runtime_logic::action::ActionProposal>::new()
                .compile()
                .expect("empty pipeline"),
        )
    };
    ProviderExecutionPolicy {
        style_pipeline: pipeline(),
        plugin_pipeline: pipeline(),
        user_policy: PermissionPolicy::new("user", Vec::new(), user_effect, "plugin graph policy"),
        mandatory_policy: PermissionPolicy::new(
            "mandatory",
            Vec::new(),
            PermissionEffect::Allow,
            "mandatory allow",
        ),
    }
}

fn allow_turn_policy() -> ProviderExecutionPolicy {
    turn_policy(PermissionEffect::Allow)
}

fn plugin_action_approval_policy() -> ProviderExecutionPolicy {
    let mut policy = turn_policy(PermissionEffect::Allow);
    policy.user_policy = PermissionPolicy::new(
        "plugin-action-user",
        vec![PermissionRule {
            id: String::from("ask-plugin-tools"),
            priority: 100,
            matcher: PermissionMatcher {
                action: Some(String::from("tool_call")),
                tool: Some(String::from("filesystem.read")),
                ..PermissionMatcher::default()
            },
            effect: PermissionEffect::Ask,
            reason: String::from("approve plugin-proposed tool action"),
        }],
        PermissionEffect::Allow,
        "allow non-tool actions",
    );
    policy
}

#[derive(Default)]
struct StrictPluginAuthorization {
    calls: AtomicU64,
    observed: Mutex<Vec<AuthorizePluginTurnCommand>>,
}

#[async_trait]
impl PluginTurnAuthorizationPort for StrictPluginAuthorization {
    async fn authorize_plugin_turn(
        &self,
        command: AuthorizePluginTurnCommand,
    ) -> Result<PluginTurnAuthorization, PluginTurnAuthorizationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            command.proposal.digest().expect("proposal digest"),
            command.action_digest
        );
        assert_eq!(command.declaration_hash, command.policy.declaration_hash);
        assert_eq!(
            command.executor_source,
            SessionNodeExecutorSource::Plugin {
                plugin_id: String::from("fixture.node"),
            }
        );
        assert_eq!(
            command.policy.required_permissions,
            vec![
                String::from("network.api.example"),
                String::from("tool.filesystem.read"),
            ]
        );
        let ConsequentialAction::PluginNodeInvocation(action) = &command.proposal.action else {
            panic!("exact plugin-node action");
        };
        assert_eq!(action.plugin_id, "fixture.node");
        assert_eq!(action.executor_id, command.identity.executor.executor_id);
        assert_eq!(
            action.executor_version,
            command.identity.executor.executor_version
        );
        assert_eq!(
            action.declaration_hash,
            command.identity.executor.executor_declaration_hash
        );
        assert_eq!(action.invocation_digest, command.identity.invocation_digest);
        self.observed.lock().expect("observed").push(command);
        Ok(PluginTurnAuthorization {
            authorization_digest: ContentHash::digest(b"production-plugin-process-authorization"),
        })
    }
}

struct ProductionProcessFixture {
    _root: TempDir,
    sessions_root: PathBuf,
    worker: PathBuf,
    catalog: PluginCatalogDataRecord,
    registry: RuntimeNodeExecutorData,
    process_config: ProcessPluginDependencyConfig,
    cancellations: RuntimeCancellationDependency,
    data: ProcessRuntimeData,
    process: Arc<ProcessPluginDependency>,
    session_id: SessionId,
    session_directory: PathBuf,
    command: DrivePluginTurnCommand,
}

fn process_data(
    sessions_root: &Path,
    catalog: &PluginCatalogDataRecord,
    registry: &RuntimeNodeExecutorData,
    process_config: &ProcessPluginDependencyConfig,
    cancellations: &RuntimeCancellationDependency,
) -> (ProcessRuntimeData, Arc<ProcessPluginDependency>) {
    let process =
        Arc::new(ProcessPluginDependency::new(process_config.clone()).expect("process dependency"));
    let data = RuntimeData::new(ProcessRuntimeDependencies::new(sessions_root.to_owned()))
        .with_node_executors(registry.clone())
        .with_plugins(RuntimePluginData::new(
            process.clone(),
            catalog.manifests.clone(),
        ))
        .with_plugin_node_receipts(RuntimePluginNodeReceiptData::new(Arc::new(
            FilePluginNodeReceiptDependency::new(sessions_root.to_owned()).expect("receipt store"),
        )))
        .with_runtime_cancellations(RuntimeCancellationData::new(Arc::new(
            cancellations.clone(),
        )));
    (data, process)
}

fn style_environment(
    style_root: PathBuf,
    catalog: &PluginCatalogDataRecord,
    capability: &str,
) -> StyleEnvironment {
    let mut environment =
        RuntimeStyleServiceConfig::native(&style_root.join("sessions")).logic_environment(None);
    environment.plugin_set_hash = catalog.plugin_set_hash.to_hex();
    environment.user_style_root = Some(style_root);
    environment.project_style_root = None;
    environment.plugin_style_roots.clear();
    environment.cache_root = None;
    environment.capabilities.insert(capability.to_owned());
    environment.plugins.insert(String::from("fixture.node"));
    environment
}

#[allow(
    clippy::too_many_lines,
    reason = "the process fixture spells out both direct and graph-shaped immutable plugin styles"
)]
fn write_plugin_style(root: &Path, executor_id: &str, capability: &str) -> String {
    let style_id = format!("process-{}", executor_id.replace('.', "-"));
    let graph_fixture = matches!(
        executor_id,
        "fixture.graph" | "fixture.graph.action" | "fixture.graph.invalid_transition"
    );
    let mut style = built_in_manifest(if graph_fixture {
        BuiltInStyle::EphemeralTurn
    } else {
        BuiltInStyle::PersistentChat
    });
    style.identity.id.clone_from(&style_id);
    style.identity.runtime_api = String::from("^1.0");
    style.kind = StyleKind::Custom;
    style.built_in_semantic = None;
    style.interceptors.clear();
    style.allowed_plugins = vec![String::from("fixture.node")];
    style.required_capabilities.push(capability.to_owned());
    if graph_fixture {
        if !style
            .allowed_providers
            .iter()
            .any(|provider| provider == "mock")
        {
            style.allowed_providers.push(String::from("mock"));
        }
        style.required_capabilities.push(String::from("context"));
        style.required_capabilities.sort();
        style.required_capabilities.dedup();
    }
    let GraphSource::Inline { source } = &mut style.graph else {
        panic!("built-in inline graph");
    };
    if graph_fixture {
        let terminal_node = if executor_id == "fixture.graph.invalid_transition" {
            "different_done"
        } else {
            "renamed_done"
        };
        *source = format!(
            r#"
format_version = 1
entry = "runtime_context"

[budget]
max_steps = 1000
max_tokens = 1000000
max_cost_micros = 100000000
max_duration_ms = 3600000

[declarations]
capabilities = ["context", "model", "{capability}"]
providers = ["mock"]
plugins = ["fixture.node"]

[[nodes]]
id = "runtime_context"
kind = "context_transform"
configuration = {{ type = "context_transform", strategy = "preserve_history" }}

[[nodes]]
id = "renamed_plugin"
kind = "model_call"
provider = "mock"
required_capabilities = ["{capability}"]
configuration = {{ type = "plugin", plugin_id = "fixture.node", executor_id = "{executor_id}", executor_version = "1.0.0", node_kind = "model_call", input_schema = "{executor_id}.input", output_schema = "{executor_id}.output", configuration_reference = "{executor_id}.config", input = {{ value = "renamed" }} }}

[[nodes]]
id = "{terminal_node}"
kind = "complete_turn"

[[edges]]
from = "runtime_context"
to = "renamed_plugin"

[[edges]]
from = "renamed_plugin"
to = "{terminal_node}"
"#
        );
    } else {
        if !style
            .allowed_providers
            .iter()
            .any(|provider| provider == "mock")
        {
            style.allowed_providers.push(String::from("mock"));
        }
        *source = format!(
            r#"
format_version = 1
entry = "plugin_entry"

[budget]
max_steps = 100
max_tokens = 250000
max_cost_micros = 25000000
max_duration_ms = 900000

[declarations]
capabilities = ["model", "{capability}"]
providers = ["mock"]
plugins = ["fixture.node"]

[[nodes]]
id = "plugin_entry"
kind = "model_call"
provider = "mock"
required_capabilities = ["{capability}"]
configuration = {{ type = "plugin", plugin_id = "fixture.node", executor_id = "{executor_id}", executor_version = "1.0.0", node_kind = "model_call", input_schema = "{executor_id}.input", output_schema = "{executor_id}.output", configuration_reference = "{executor_id}.config", input = {{ value = "direct" }} }}

[[nodes]]
id = "done"
kind = "complete_turn"

[[edges]]
from = "plugin_entry"
to = "done"
"#
        );
    }
    fs::create_dir_all(root).expect("style root");
    fs::write(
        root.join(format!("{style_id}.json")),
        serde_json::to_vec(&style).expect("style json"),
    )
    .expect("write style");
    style_id
}

fn append_canonical(
    data: &ProcessRuntimeData,
    session_id: SessionId,
    session_directory: &Path,
    payload: RuntimeCommittedEvent,
) {
    let persistence = SessionPersistenceLogic::new(data.clone());
    let head = persistence
        .load_session(LoadSessionCommand {
            session_directory: session_directory.to_owned(),
            expected_session_id: session_id,
        })
        .expect("load head");
    let identity = data
        .allocate_event_identity(AllocateEventIdentityDataRequest)
        .expect("identity");
    let event = EventEnvelope::seal(
        EventMetadata {
            event_id: identity.event_id,
            scope: EventScope::Session(session_id),
            sequence: head
                .state
                .last_sequence
                .checked_next()
                .expect("next sequence"),
            timestamp: identity.timestamp,
            event_type: payload.event_type().to_owned(),
            event_version: Version::new(1, 0),
            correlation_id: identity.correlation_id,
            causation_id: CausationId::from_uuid(head.last_event_id.into_uuid()),
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: Vec::new(),
            classification: EventClassification::Committed,
        },
        payload,
    )
    .expect("seal");
    persistence
        .commit_event(CommitSessionEventCommand {
            session_directory: session_directory.to_owned(),
            event,
            durability: CommitDurability::Full,
        })
        .expect("commit");
}

#[allow(
    clippy::too_many_lines,
    reason = "the process fixture spells out the exact ordered context-boundary journal before isolated dispatch"
)]
fn seed_context_transform_phase(
    data: &ProcessRuntimeData,
    session_id: SessionId,
    session_directory: &Path,
    binding: &agentmod_runtime_logic::session::SessionStyleBinding,
    compiled: &CompiledSessionStyle,
) -> DrivePluginContextTransformCommand {
    let entry_node_id = compiled.graph.nodes[compiled.graph.entry_index].id.clone();
    let execution_plan = binding.execution_plan.as_ref().expect("execution plan");
    let contract = StyleExecutionContract {
        style_binding_hash: ContentHash::digest(
            &serde_json::to_vec(binding).expect("binding serialization"),
        ),
        execution_plan_hash: binding.execution_plan_hash.expect("execution plan hash"),
        registry_hash: execution_plan.registry_hash,
        node_executors: execution_plan.nodes.clone(),
        initial_node_id: entry_node_id.clone(),
        initial_variables_json: String::from("{}"),
        invocation_provider: Some(String::from("mock")),
        invocation_model: Some(String::from("mock-model")),
        invocation_options_json: None,
        initial_budgets: binding.budgets,
        run_id: format!("style-run:{session_id}"),
    };
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
            plugin_ids: vec![String::from("fixture.context")],
            plugin_set_hash: binding.plugin_set_hash,
        }),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
            StyleExecutionInitializedEvent {
                graph: Box::new(compiled.graph.clone()),
                input_reference: None,
                execution_contract: Some(Box::new(contract)),
            },
        )),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
            node_id: entry_node_id.clone(),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
        }),
    );
    let request_hash = ContentHash::digest(b"context-process-request");
    let turn_boundary = ContextBoundaryIdentity {
        node_id: entry_node_id.clone(),
        boundary: String::from("turn_start"),
        run_id: String::from("context-process-run"),
        origin: ContextBoundaryOrigin::UserTurn,
        request_hash,
        source_head: Sequence::new(4).expect("turn source head"),
    };
    let turn_memory = ContextPhaseIdentity {
        boundary: turn_boundary.clone(),
        phase: String::from("memory"),
    };
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
            identity: turn_boundary.clone(),
        }),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
            identity: turn_memory.clone(),
        }),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
            identity: turn_memory,
        }),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextBoundaryCompleted(ContextBoundaryCompletedEvent {
            identity: turn_boundary,
            projection_hash: ContentHash::digest(b"[]"),
            estimated_tokens: 0,
            serialized_bytes: 2,
        }),
    );
    let before_boundary = ContextBoundaryIdentity {
        node_id: entry_node_id,
        boundary: String::from("before_model_request"),
        run_id: String::from("context-process-run"),
        origin: ContextBoundaryOrigin::UserTurn,
        request_hash,
        source_head: Sequence::new(8).expect("before-model source head"),
    };
    let before_memory = ContextPhaseIdentity {
        boundary: before_boundary.clone(),
        phase: String::from("memory"),
    };
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextBoundaryStarted(ContextBoundaryStartedEvent {
            identity: before_boundary.clone(),
        }),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
            identity: before_memory.clone(),
        }),
    );
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextPhaseCompleted(ContextPhaseCompletedEvent {
            identity: before_memory,
        }),
    );
    let phase = ContextPhaseIdentity {
        boundary: before_boundary,
        phase: String::from("plugin_context_transform:0"),
    };
    append_canonical(
        data,
        session_id,
        session_directory,
        RuntimeCommittedEvent::ContextPhaseStarted(ContextPhaseStartedEvent {
            identity: phase.clone(),
        }),
    );
    DrivePluginContextTransformCommand {
        session_id,
        session_directory: session_directory.to_owned(),
        phase,
        ordinal: 0,
        cancellation_id: String::from("context-process-cancel"),
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the fixture retains one complete immutable style, registry, journal, plugin-host, receipt, and cancellation composition for process reconstruction"
)]
async fn production_fixture(executor_id: &str) -> ProductionProcessFixture {
    let root = TempDir::new().expect("temporary root");
    let sessions_root = root.path().join("sessions");
    let worker_source = executable("AGENTMOD_TEST_PLUGIN_WORKER");
    let worker = root
        .path()
        .join(worker_source.file_name().expect("worker name"));
    fs::copy(worker_source, &worker).expect("copy isolated worker");
    let source = DependencyPluginManifestSource {
        locator: String::from("fixture-node.json"),
        format: String::from("json"),
        contents: manifest(&worker.to_string_lossy()),
    };
    let catalog = compile_plugin_catalog(&[source], "1.0.0", Vec::new()).expect("catalog");
    let registry =
        RuntimeNodeExecutorData::native_with_plugins(&catalog.manifests).expect("single registry");
    let process_config = ProcessPluginDependencyConfig {
        program: executable("AGENTMOD_TEST_PLUGIN_HOST")
            .to_string_lossy()
            .into_owned(),
        arguments: Vec::new(),
        owner_id: String::from("agentmod-runtime-production-plugin-test"),
        runtime_api_version: String::from("1.0.0"),
        sessions_root: sessions_root.clone(),
        executable_roots: vec![root.path().to_owned()],
        authorization_key: ProcessPluginDependency::derive_authorization_key(
            b"production-plugin-node-process-test",
        ),
        maximum_frame_bytes: 1024 * 1024,
        request_timeout: Duration::from_secs(5),
    };
    let cancellations = RuntimeCancellationDependency::default();
    let (data, process) = process_data(
        &sessions_root,
        &catalog,
        &registry,
        &process_config,
        &cancellations,
    );
    let declaration = catalog.manifests[0]
        .node_executors
        .iter()
        .find(|declaration| declaration.executor_id == executor_id)
        .expect("executor declaration")
        .clone();
    let style_root = root.path().join("styles");
    let style_id = write_plugin_style(
        &style_root,
        executor_id,
        declaration
            .capabilities
            .iter()
            .find(|capability| capability.starts_with("plugin."))
            .expect("plugin capability"),
    );
    let environment = style_environment(
        style_root,
        &catalog,
        declaration
            .capabilities
            .iter()
            .find(|capability| capability.starts_with("plugin."))
            .expect("plugin capability"),
    );
    let style_logic = RuntimeLogic::new(data.clone());
    let binding = style_logic
        .resolve_style(InspectStyleCommand {
            selector: style_id.clone(),
            environment: environment.clone(),
        })
        .unwrap_or_else(|error| {
            let inspection = style_logic
                .inspect_style(InspectStyleCommand {
                    selector: style_id,
                    environment,
                })
                .ok();
            panic!("resolve plugin style: {error:?}; inspection: {inspection:?}");
        })
        .binding;
    let workspace = root.path().join("workspace");
    fs::create_dir_all(&workspace).expect("workspace");
    let created = RuntimeLogic::new(data.clone())
        .create_session(CreateSessionCommand {
            sessions_root: sessions_root.clone(),
            workspace,
            style_binding: binding,
            mcp_servers: Vec::new(),
        })
        .expect("create session");
    data.activate_plugins(ActivatePluginsDataRequest {
        session_id: created.session_id.to_string(),
        plugin_ids: vec![String::from("fixture.node")],
        runtime_api_version: String::from("1.0.0"),
        capabilities: BTreeSet::new(),
        cancellation_id: format!("activate-{executor_id}"),
    })
    .await
    .expect("activate exact plugin");
    let persistence = SessionPersistenceLogic::new(data.clone());
    let created_state = persistence
        .load_session(LoadSessionCommand {
            session_directory: created.session_directory.clone(),
            expected_session_id: created.session_id,
        })
        .expect("created session");
    assert_generation_three_plan_identity(&created_state.state);
    let binding = created_state.state.style_binding.expect("style binding");
    let compiled: CompiledSessionStyle =
        serde_json::from_str(&binding.compiled_style_json).expect("compiled style");
    let entry_node_id = compiled.graph.nodes[compiled.graph.entry_index].id.clone();
    let graph_fixture = matches!(
        executor_id,
        "fixture.graph" | "fixture.graph.action" | "fixture.graph.invalid_transition"
    );
    let plugin_node_id = if graph_fixture {
        String::from("renamed_plugin")
    } else {
        entry_node_id.clone()
    };
    let execution_plan = binding.execution_plan.as_ref().expect("execution plan");
    let executor = execution_plan
        .nodes
        .iter()
        .find(|resolution| resolution.node_id == plugin_node_id)
        .expect("plugin resolution")
        .clone();
    assert_eq!(executor.executor_id, executor_id);
    assert_eq!(
        executor.source,
        SessionNodeExecutorSource::Plugin {
            plugin_id: String::from("fixture.node"),
        }
    );
    assert_eq!(executor.boundary, SessionNodeExecutorBoundary::PluginHost);
    append_canonical(
        &data,
        created.session_id,
        &created.session_directory,
        RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
            plugin_ids: vec![String::from("fixture.node")],
            plugin_set_hash: binding.plugin_set_hash,
        }),
    );
    let run_id = format!("style-run:{}", created.session_id);
    let contract = StyleExecutionContract {
        style_binding_hash: ContentHash::digest(
            &serde_json::to_vec(&binding).expect("binding hash"),
        ),
        execution_plan_hash: binding.execution_plan_hash.expect("plan hash"),
        registry_hash: execution_plan.registry_hash,
        node_executors: execution_plan.nodes.clone(),
        initial_node_id: entry_node_id.clone(),
        initial_variables_json: String::from("{}"),
        invocation_provider: Some(String::from("mock")),
        invocation_model: Some(String::from("mock-model")),
        invocation_options_json: None,
        initial_budgets: binding.budgets,
        run_id: run_id.clone(),
    };
    if !graph_fixture {
        append_canonical(
            &data,
            created.session_id,
            &created.session_directory,
            RuntimeCommittedEvent::StyleExecutionInitialized(Box::new(
                StyleExecutionInitializedEvent {
                    graph: Box::new(compiled.graph),
                    input_reference: None,
                    execution_contract: Some(Box::new(contract)),
                },
            )),
        );
        append_canonical(
            &data,
            created.session_id,
            &created.session_directory,
            RuntimeCommittedEvent::StyleNodeEntered(StyleNodeEnteredEvent {
                node_id: entry_node_id.clone(),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            }),
        );
    }
    let required_permissions = declaration
        .network_permissions
        .iter()
        .map(|permission| format!("network.{permission}"))
        .chain(
            declaration
                .tool_permissions
                .iter()
                .map(|permission| format!("tool.{permission}")),
        )
        .collect();
    ProductionProcessFixture {
        _root: root,
        sessions_root,
        worker,
        catalog,
        registry,
        process_config,
        cancellations,
        data,
        process,
        session_id: created.session_id,
        session_directory: created.session_directory,
        command: DrivePluginTurnCommand {
            session_id: created.session_id,
            work: NodeWorkIdentity {
                run_id,
                node_id: plugin_node_id,
                branch_path: Vec::new(),
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            },
            executor,
            input: json!({"value":executor_id}),
            readable_state: json!({"classification":"internal"}),
            cancellation_id: format!("cancel-{executor_id}"),
            policy: PluginNodeInvocationPolicy {
                declaration_hash: declaration.declaration_hash,
                idempotent: declaration.idempotent,
                external_effects: declaration.external_effects,
                max_attempts: declaration.max_attempts,
                required_permissions,
            },
        },
    }
}

fn invocation_count(session_directory: &Path) -> usize {
    fs::read_to_string(session_directory.join("fixture-node-invocations.log"))
        .map(|contents| contents.lines().count())
        .unwrap_or(0)
}

fn receipt_count(session_directory: &Path) -> usize {
    fs::read_dir(
        session_directory
            .join("artifacts")
            .join("plugin-node-receipts"),
    )
    .map(|entries| entries.filter_map(Result::ok).count())
    .unwrap_or(0)
}

fn loaded_state(
    data: &ProcessRuntimeData,
    session_id: SessionId,
    session_directory: &Path,
) -> agentmod_runtime_logic::session::SessionState {
    let state = SessionPersistenceLogic::new(data.clone())
        .load_session(LoadSessionCommand {
            session_directory: session_directory.to_owned(),
            expected_session_id: session_id,
        })
        .expect("load state")
        .state;
    assert_generation_three_plan_identity(&state);
    state
}

fn assert_generation_three_plan_identity(state: &agentmod_runtime_logic::session::SessionState) {
    let binding = state.style_binding.as_ref().expect("style binding");
    let plan = binding.execution_plan.as_ref().expect("execution plan");
    assert_eq!(
        plan.compilation.compiler, "agentmod-runtime-node-plan@3",
        "plugin process session must retain the generation-3 compiler"
    );
    assert_eq!(
        plan.compilation.compiled_style_hash, binding.compiled_style_hash,
        "plugin process session must retain the exact compiled style"
    );
    assert_eq!(
        plan.compilation.compiled_cache_key, binding.compiled_cache_key,
        "plugin process session must retain the exact compiled cache key"
    );
    let plan_hash = binding.execution_plan_hash.expect("execution plan hash");
    if let Some(contract) = state
        .style_execution
        .as_ref()
        .and_then(|execution| execution.execution_contract.as_deref())
    {
        assert_eq!(
            contract.execution_plan_hash, plan_hash,
            "plugin process execution must not rebind its plan"
        );
        assert_eq!(
            contract.registry_hash, plan.registry_hash,
            "plugin process execution must retain its compiled registry"
        );
        assert_eq!(
            contract.node_executors, plan.nodes,
            "plugin process execution must retain every exact executor resolution"
        );
    }
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
async fn context_transform_uses_exact_isolated_protocol_and_fails_closed() {
    let root = TempDir::new().expect("temporary root");
    let sessions_root = root.path().join("sessions");
    let worker_source = executable("AGENTMOD_TEST_PLUGIN_WORKER");
    let worker = root
        .path()
        .join(worker_source.file_name().expect("worker name"));
    fs::copy(worker_source, &worker).expect("copy isolated worker");
    let catalog = compile_plugin_catalog(
        &[DependencyPluginManifestSource {
            locator: String::from("fixture-context.json"),
            format: String::from("json"),
            contents: context_manifest(&worker.to_string_lossy()),
        }],
        "1.0.0",
        Vec::new(),
    )
    .expect("context-transform catalog");
    let process = Arc::new(
        ProcessPluginDependency::new(ProcessPluginDependencyConfig {
            program: executable("AGENTMOD_TEST_PLUGIN_HOST")
                .to_string_lossy()
                .into_owned(),
            arguments: Vec::new(),
            owner_id: String::from("agentmod-runtime-context-process-test"),
            runtime_api_version: String::from("1.0.0"),
            sessions_root,
            executable_roots: vec![root.path().to_owned()],
            authorization_key: ProcessPluginDependency::derive_authorization_key(
                b"context-transform-process-test",
            ),
            maximum_frame_bytes: 1024 * 1024,
            request_timeout: Duration::from_secs(5),
        })
        .expect("process dependency"),
    );
    let data = RuntimePluginData::new(process.clone(), catalog.manifests.clone());
    data.activate_plugins(ActivatePluginsDataRequest {
        session_id: String::from("context-process-session"),
        plugin_ids: vec![String::from("fixture.context")],
        runtime_api_version: String::from("1.0.0"),
        capabilities: BTreeSet::from([String::from("context.redaction")]),
        cancellation_id: String::from("activate-context"),
    })
    .await
    .expect("activate exact context plugin");
    let declarations = &catalog.manifests[0].context_transforms;
    let configuration_reference = catalog.manifests[0].configuration_reference;

    let success = declarations
        .iter()
        .find(|declaration| declaration.transform_id == "fixture.redact")
        .expect("success declaration");
    let proposal = data
        .invoke_context_transform(context_transform_request(
            success,
            "context-success-1",
            configuration_reference,
        ))
        .await
        .expect("isolated context transform");
    assert_eq!(
        proposal.replacement,
        json!([{"role":"user","content":"bounded private context"}])
    );
    assert_eq!(proposal.attempts, 1);

    for (transform_id, invocation_id) in [
        ("fixture.invalid", "context-invalid-1"),
        ("fixture.timeout", "context-timeout-1"),
    ] {
        let declaration = declarations
            .iter()
            .find(|declaration| declaration.transform_id == transform_id)
            .expect("failure declaration");
        assert!(matches!(
            data.invoke_context_transform(context_transform_request(
                declaration,
                invocation_id,
                configuration_reference,
            ))
            .await,
            Err(PluginDataError::AmbiguousContextTransform {
                plugin_id,
                transform_id: failed_transform,
                invocation_id: failed_invocation,
            }) if plugin_id == "fixture.context"
                && failed_transform == transform_id
                && failed_invocation == invocation_id
        ));
    }

    process.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
#[allow(
    clippy::too_many_lines,
    reason = "the ignored process proof keeps immutable style selection, isolated invocation, durable receipt restart, and declaration-loss crash cut together"
)]
async fn production_context_transform_turn_recovers_receipt_and_fails_closed_on_declaration_loss() {
    let root = TempDir::new().expect("temporary root");
    let sessions_root = root.path().join("sessions");
    let worker_source = executable("AGENTMOD_TEST_PLUGIN_WORKER");
    let worker = root
        .path()
        .join(worker_source.file_name().expect("worker name"));
    fs::copy(worker_source, &worker).expect("copy isolated worker");
    let catalog = compile_plugin_catalog(
        &[DependencyPluginManifestSource {
            locator: String::from("fixture-context.json"),
            format: String::from("json"),
            contents: context_manifest(&worker.to_string_lossy()),
        }],
        "1.0.0",
        Vec::new(),
    )
    .expect("context-transform catalog");
    let declaration = catalog.manifests[0]
        .context_transforms
        .iter()
        .find(|declaration| declaration.transform_id == "fixture.redact")
        .expect("context declaration")
        .clone();
    let registry =
        RuntimeNodeExecutorData::native_with_plugins(&catalog.manifests).expect("single registry");
    let process_config = ProcessPluginDependencyConfig {
        program: executable("AGENTMOD_TEST_PLUGIN_HOST")
            .to_string_lossy()
            .into_owned(),
        arguments: Vec::new(),
        owner_id: String::from("agentmod-runtime-context-turn-process-test"),
        runtime_api_version: String::from("1.0.0"),
        sessions_root: sessions_root.clone(),
        executable_roots: vec![root.path().to_owned()],
        authorization_key: ProcessPluginDependency::derive_authorization_key(
            b"context-transform-turn-process-test",
        ),
        maximum_frame_bytes: 1024 * 1024,
        request_timeout: Duration::from_secs(5),
    };
    let cancellations = RuntimeCancellationDependency::default();
    let (data, process) = process_data(
        &sessions_root,
        &catalog,
        &registry,
        &process_config,
        &cancellations,
    );
    let style_root = root.path().join("context-styles");
    let configuration_reference = catalog.manifests[0].configuration_reference;
    let style_id =
        write_context_transform_style(&style_root, &declaration, configuration_reference);
    let mut environment = style_environment(style_root, &catalog, "context.redaction");
    environment.plugins = BTreeSet::from([String::from("fixture.context")]);
    environment.context_transforms = vec![StyleContextTransformDescriptor {
        plugin_id: String::from("fixture.context"),
        transform_id: declaration.transform_id.clone(),
        version: declaration.version.clone(),
        declaration_hash: declaration.declaration_hash.to_hex(),
        lifecycle: String::from("before_model_request"),
    }];
    let binding = RuntimeLogic::new(data.clone())
        .resolve_style(InspectStyleCommand {
            selector: style_id,
            environment,
        })
        .expect("resolve exact context style")
        .binding;
    let compiled: CompiledSessionStyle =
        serde_json::from_str(&binding.compiled_style_json).expect("compiled context style");
    assert_eq!(
        compiled.context_transforms,
        vec![ContextTransformSelection {
            plugin_id: String::from("fixture.context"),
            transform_id: declaration.transform_id.clone(),
            version: declaration.version.clone(),
            declaration_hash: declaration.declaration_hash,
            lifecycle: ContextTransformLifecycle::BeforeModelRequest,
            configuration_reference,
        }]
    );

    let first_workspace = root.path().join("workspace-first");
    let unavailable_workspace = root.path().join("workspace-unavailable");
    fs::create_dir_all(&first_workspace).expect("first workspace");
    fs::create_dir_all(&unavailable_workspace).expect("unavailable workspace");
    let first = RuntimeLogic::new(data.clone())
        .create_session(CreateSessionCommand {
            sessions_root: sessions_root.clone(),
            workspace: first_workspace,
            style_binding: binding.clone(),
            mcp_servers: Vec::new(),
        })
        .expect("create first context session");
    let unavailable = RuntimeLogic::new(data.clone())
        .create_session(CreateSessionCommand {
            sessions_root: sessions_root.clone(),
            workspace: unavailable_workspace,
            style_binding: binding.clone(),
            mcp_servers: Vec::new(),
        })
        .expect("create declaration-loss session");
    let first_binding = SessionPersistenceLogic::new(data.clone())
        .load_session(LoadSessionCommand {
            session_directory: first.session_directory.clone(),
            expected_session_id: first.session_id,
        })
        .expect("load first session")
        .state
        .style_binding
        .expect("persisted first binding");
    let unavailable_binding = SessionPersistenceLogic::new(data.clone())
        .load_session(LoadSessionCommand {
            session_directory: unavailable.session_directory.clone(),
            expected_session_id: unavailable.session_id,
        })
        .expect("load unavailable session")
        .state
        .style_binding
        .expect("persisted unavailable binding");
    data.activate_plugins(ActivatePluginsDataRequest {
        session_id: first.session_id.to_string(),
        plugin_ids: vec![String::from("fixture.context")],
        runtime_api_version: String::from("1.0.0"),
        capabilities: BTreeSet::from([String::from("context.redaction")]),
        cancellation_id: String::from("activate-context-turn"),
    })
    .await
    .expect("activate exact context plugin");
    let first_command = seed_context_transform_phase(
        &data,
        first.session_id,
        &first.session_directory,
        &first_binding,
        &compiled,
    );
    let unavailable_command = seed_context_transform_phase(
        &data,
        unavailable.session_id,
        &unavailable.session_directory,
        &unavailable_binding,
        &compiled,
    );
    let completed = ProductionPluginContextTransformTurn::new(data.clone())
        .drive_plugin_context_transform(first_command.clone())
        .await
        .expect("isolated production context transform");
    assert!(completed.replacement.is_empty());
    let stored = data
        .load_plugin_invocation_receipt(PluginInvocationReceiptDataIdentity {
            session_id: first.session_id,
            invocation_id: completed.identity.invocation_id.clone(),
        })
        .expect("load terminal context receipt")
        .expect("terminal context receipt");
    assert!(stored.receipt_json.contains("\"outcome\":\"completed\""));
    process.shutdown().await;

    let unavailable_process =
        Arc::new(ProcessPluginDependency::new(process_config).expect("restart process dependency"));
    let restarted: ProcessRuntimeData =
        RuntimeData::new(ProcessRuntimeDependencies::new(sessions_root.clone()))
            .with_node_executors(registry)
            .with_plugins(RuntimePluginData::new(
                unavailable_process.clone(),
                Vec::new(),
            ))
            .with_plugin_node_receipts(RuntimePluginNodeReceiptData::new(Arc::new(
                FilePluginNodeReceiptDependency::new(sessions_root)
                    .expect("restarted receipt store"),
            )));
    let replayed = ProductionPluginContextTransformTurn::new(restarted.clone())
        .drive_plugin_context_transform(first_command)
        .await
        .expect("restart from exact terminal receipt");
    assert_eq!(replayed.replacement, completed.replacement);
    assert_eq!(replayed.replacement_hash, completed.replacement_hash);

    assert!(matches!(
        ProductionPluginContextTransformTurn::new(restarted.clone())
            .drive_plugin_context_transform(unavailable_command.clone())
            .await,
        Err(PluginContextTransformTurnError::PluginData(
            PluginDataError::Invalid
        ))
    ));
    assert!(matches!(
        ProductionPluginContextTransformTurn::new(restarted)
            .drive_plugin_context_transform(unavailable_command)
            .await,
        Err(PluginContextTransformTurnError::AmbiguousFailClosed)
    ));
    unavailable_process.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
#[allow(
    clippy::too_many_lines,
    reason = "the ignored process scenario keeps host restart, exact state receipt, and no-rebinding assertions in one lifecycle"
)]
async fn plugin_node_state_receipt_survives_host_restart_without_rebinding() {
    let fixture = production_fixture("fixture.graph").await;
    let declaration = fixture.catalog.manifests[0]
        .node_executors
        .iter()
        .find(|declaration| declaration.executor_id == "fixture.graph")
        .expect("executor declaration");
    let state = json!({"cursor": 1});
    let mut command = PersistPluginNodeStateCommand {
        cancellation_target: PluginInvocationCancellationTarget {
            session_id: fixture.session_id.to_string(),
            run_id: String::from("state-process-run"),
            plugin_id: String::from("fixture.node"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("plugin-node:state-process-proof"),
            invocation_digest: ContentHash::digest(b"state-process-invocation"),
            operation_id: declaration.executor_id.clone(),
            declaration_hash: declaration.declaration_hash,
            request_hash: ContentHash::digest(b"state-process-write-request"),
        },
        session_id: fixture.session_id.to_string(),
        plugin_id: String::from("fixture.node"),
        invocation_id: String::from("plugin-node:state-process-proof"),
        invocation_digest: ContentHash::digest(b"state-process-invocation"),
        executor_id: declaration.executor_id.clone(),
        executor_version: declaration.version.clone(),
        executor_declaration_hash: declaration.declaration_hash,
        configuration_reference: fixture.catalog.manifests[0].configuration_reference,
        state_scope: PluginNodeStateScope::Session,
        prior_generation: 0,
        prior_state_hash: None,
        state_hash: plugin_node_state_value_hash(&state).expect("state hash"),
        state,
        action_digest: ContentHash::digest(b"pending-action"),
        authorization_digest: ContentHash::digest(b"pending-authorization"),
        nonce: String::from("state-process-nonce"),
        cancellation_id: String::from("state-process-cancel"),
        idempotency_key: String::from("state-process-write-1"),
    };
    let write_request_hash =
        plugin_node_state_persistence_request_hash(&command).expect("state write request hash");
    command.cancellation_target = plugin_invocation_cancellation_target(
        &fixture.session_id.to_string(),
        "state-process-run",
        "fixture.node",
        "1.0.0",
        "plugin-node:state-process-proof",
        &format!("{}:state-write", declaration.executor_id),
        declaration.declaration_hash,
        write_request_hash,
    )
    .expect("state write cancellation target");
    let (action_digest, authorization_digest) =
        plugin_node_state_persistence_digests(&command).expect("state identities");
    command.action_digest = action_digest;
    command.authorization_digest = authorization_digest;
    let first = PluginCompositionLogic::new(fixture.data.clone())
        .persist_plugin_node_state(command.clone())
        .await
        .expect("first durable state receipt");
    assert_eq!(first.generation, 1);
    assert!(!first.replayed);

    fixture.process.shutdown().await;
    let restarted = Arc::new(
        ProcessPluginDependency::new(fixture.process_config.clone()).expect("restarted dependency"),
    );
    let restarted_data =
        RuntimePluginData::new(restarted.clone(), fixture.catalog.manifests.clone());
    restarted_data
        .activate_plugins(ActivatePluginsDataRequest {
            session_id: fixture.session_id.to_string(),
            plugin_ids: vec![String::from("fixture.node")],
            runtime_api_version: String::from("1.0.0"),
            capabilities: BTreeSet::new(),
            cancellation_id: String::from("state-process-reactivate"),
        })
        .await
        .expect("reactivate plugin after host restart");
    let replay = PluginCompositionLogic::new(restarted_data)
        .persist_plugin_node_state(command.clone())
        .await
        .expect("reconciled durable state receipt");
    assert!(replay.replayed);
    assert_eq!(first.receipt_id, replay.receipt_id);
    assert_eq!(first.receipt_digest, replay.receipt_digest);

    let restarted_data =
        RuntimePluginData::new(restarted.clone(), fixture.catalog.manifests.clone());
    restarted_data
        .activate_plugins(ActivatePluginsDataRequest {
            session_id: fixture.session_id.to_string(),
            plugin_ids: vec![String::from("fixture.node")],
            runtime_api_version: String::from("1.0.0"),
            capabilities: BTreeSet::new(),
            cancellation_id: String::from("state-process-reactivate-errors"),
        })
        .await
        .expect("reuse active host");
    let logic = PluginCompositionLogic::new(restarted_data);
    let mut read_command = LoadPluginNodeStateCommand {
        cancellation_target: PluginInvocationCancellationTarget {
            session_id: fixture.session_id.to_string(),
            run_id: String::from("state-process-run"),
            plugin_id: String::from("fixture.node"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("plugin-node:later-state-process-proof"),
            invocation_digest: ContentHash::digest(b"later-state-process-invocation"),
            operation_id: declaration.executor_id.clone(),
            declaration_hash: declaration.declaration_hash,
            request_hash: ContentHash::digest(b"state-process-read-request"),
        },
        session_id: fixture.session_id.to_string(),
        plugin_id: String::from("fixture.node"),
        invocation_id: String::from("plugin-node:later-state-process-proof"),
        invocation_digest: ContentHash::digest(b"later-state-process-invocation"),
        executor_id: declaration.executor_id.clone(),
        executor_version: declaration.version.clone(),
        executor_declaration_hash: declaration.declaration_hash,
        configuration_reference: fixture.catalog.manifests[0].configuration_reference,
        state_scope: PluginNodeStateScope::Session,
        expected_generation: first.generation,
        expected_state_hash: first.state_hash,
        action_digest: ContentHash::digest(b"pending-read-action"),
        authorization_digest: ContentHash::digest(b"pending-read-authorization"),
        nonce: String::from("state-process-read-nonce"),
        cancellation_id: String::from("state-process-read-cancel"),
        idempotency_key: String::from("state-process-read-1"),
    };
    let read_request_hash =
        plugin_node_state_read_request_hash(&read_command).expect("state read request hash");
    read_command.cancellation_target = plugin_invocation_cancellation_target(
        &fixture.session_id.to_string(),
        "state-process-run",
        "fixture.node",
        "1.0.0",
        "plugin-node:later-state-process-proof",
        &format!("{}:state-read", declaration.executor_id),
        declaration.declaration_hash,
        read_request_hash,
    )
    .expect("state read cancellation target");
    let (read_action, read_authorization) =
        plugin_node_state_read_digests(&read_command).expect("read identities");
    read_command.action_digest = read_action;
    read_command.authorization_digest = read_authorization;
    let loaded = logic
        .load_plugin_node_state(read_command.clone())
        .await
        .expect("load prior session state after host restart");
    assert_eq!(loaded.state, json!({"cursor": 1}));
    assert_eq!(loaded.receipt.generation, first.generation);
    assert_eq!(loaded.receipt.state_hash, first.state_hash);
    assert!(!loaded.receipt.replayed);
    let replayed_read = logic
        .load_plugin_node_state(read_command.clone())
        .await
        .expect("reconcile exact state read");
    assert!(replayed_read.receipt.replayed);
    assert_eq!(
        replayed_read.receipt.receipt_digest,
        loaded.receipt.receipt_digest
    );
    let mut substituted_read = read_command;
    substituted_read.expected_state_hash = ContentHash::digest(b"substituted-state");
    let substituted_read_hash = plugin_node_state_read_request_hash(&substituted_read)
        .expect("substituted state read request hash");
    substituted_read.cancellation_target = plugin_invocation_cancellation_target(
        &fixture.session_id.to_string(),
        "state-process-run",
        "fixture.node",
        "1.0.0",
        "plugin-node:later-state-process-proof",
        &format!("{}:state-read", declaration.executor_id),
        declaration.declaration_hash,
        substituted_read_hash,
    )
    .expect("substituted state read cancellation target");
    let (read_action, read_authorization) =
        plugin_node_state_read_digests(&substituted_read).expect("substituted read identities");
    substituted_read.action_digest = read_action;
    substituted_read.authorization_digest = read_authorization;
    assert_eq!(
        logic.load_plugin_node_state(substituted_read).await,
        Err(PluginNodeStateReadError::StaleGeneration)
    );

    let mut stale_command = command.clone();
    stale_command.idempotency_key = String::from("state-process-write-2");
    let stale_request_hash = plugin_node_state_persistence_request_hash(&stale_command)
        .expect("stale state write request hash");
    stale_command.cancellation_target = plugin_invocation_cancellation_target(
        &fixture.session_id.to_string(),
        "state-process-run",
        "fixture.node",
        "1.0.0",
        "plugin-node:state-process-proof",
        &format!("{}:state-write", declaration.executor_id),
        declaration.declaration_hash,
        stale_request_hash,
    )
    .expect("stale state write cancellation target");
    let (action, authorization) =
        plugin_node_state_persistence_digests(&stale_command).expect("stale identities");
    stale_command.action_digest = action;
    stale_command.authorization_digest = authorization;
    assert_eq!(
        logic.persist_plugin_node_state(stale_command).await,
        Err(PluginNodeStatePersistenceError::StaleGeneration)
    );

    let mut conflict = command;
    conflict.state = json!({"cursor": 2});
    conflict.state_hash =
        plugin_node_state_value_hash(&conflict.state).expect("changed state hash");
    let conflict_request_hash = plugin_node_state_persistence_request_hash(&conflict)
        .expect("conflicting state write request hash");
    conflict.cancellation_target = plugin_invocation_cancellation_target(
        &fixture.session_id.to_string(),
        "state-process-run",
        "fixture.node",
        "1.0.0",
        "plugin-node:state-process-proof",
        &format!("{}:state-write", declaration.executor_id),
        declaration.declaration_hash,
        conflict_request_hash,
    )
    .expect("conflicting state write cancellation target");
    let (action, authorization) =
        plugin_node_state_persistence_digests(&conflict).expect("conflict identities");
    conflict.action_digest = action;
    conflict.authorization_digest = authorization;
    assert_eq!(
        logic.persist_plugin_node_state(conflict).await,
        Err(PluginNodeStatePersistenceError::Conflict)
    );
    restarted.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
#[allow(
    clippy::too_many_lines,
    reason = "the direct process proof keeps success and each fail-closed host outcome against one activated isolated plugin"
)]
async fn isolated_plugin_node_success_invalid_timeout_and_unavailability() {
    let temporary = TempDir::new().expect("temporary root");
    let worker_source = executable("AGENTMOD_TEST_PLUGIN_WORKER");
    let worker_name = worker_source.file_name().expect("worker file name");
    let worker = temporary.path().join(worker_name);
    fs::copy(&worker_source, &worker).expect("copy isolated worker");
    let host = executable("AGENTMOD_TEST_PLUGIN_HOST");
    let source = DependencyPluginManifestSource {
        locator: String::from("fixture-node.json"),
        format: String::from("json"),
        contents: manifest(&worker.to_string_lossy()),
    };
    let catalog = compile_plugin_catalog(&[source], "1.0.0", Vec::new()).expect("catalog");
    let process_config = ProcessPluginDependencyConfig {
        program: host.to_string_lossy().into_owned(),
        arguments: Vec::new(),
        owner_id: String::from("agentmod-runtime-test"),
        runtime_api_version: String::from("1.0.0"),
        sessions_root: temporary.path().join("sessions"),
        executable_roots: vec![temporary.path().to_path_buf()],
        authorization_key: ProcessPluginDependency::derive_authorization_key(
            b"plugin-node-process-test",
        ),
        maximum_frame_bytes: 1024 * 1024,
        request_timeout: Duration::from_secs(5),
    };
    let mut mismatch_config = process_config.clone();
    mismatch_config.runtime_api_version = String::from("0.1.0");
    let mismatch_process =
        Arc::new(ProcessPluginDependency::new(mismatch_config).expect("mismatch process"));
    let mismatch_data = RuntimePluginData::new(mismatch_process.clone(), catalog.manifests.clone());
    assert_eq!(
        mismatch_data
            .activate_plugins(ActivatePluginsDataRequest {
                session_id: String::from("01900000-0000-7000-8000-000000000000"),
                plugin_ids: vec![String::from("fixture.node")],
                runtime_api_version: String::from("1.0.0"),
                capabilities: BTreeSet::new(),
                cancellation_id: String::from("activate-runtime-api-mismatch"),
            })
            .await,
        Err(PluginDataError::Rejected {
            operation: String::from("negotiate"),
            code: String::from("operation_failed"),
            retryable: true,
        })
    );
    mismatch_process.shutdown().await;

    let process =
        Arc::new(ProcessPluginDependency::new(process_config).expect("process dependency"));
    let data = RuntimePluginData::new(process.clone(), catalog.manifests.clone());
    data.activate_plugins(ActivatePluginsDataRequest {
        session_id: String::from("01900000-0000-7000-8000-000000000001"),
        plugin_ids: vec![String::from("fixture.node")],
        runtime_api_version: String::from("1.0.0"),
        capabilities: BTreeSet::new(),
        cancellation_id: String::from("activate-fixture-node"),
    })
    .await
    .expect("activate exact plugin");
    let registry =
        RuntimeNodeExecutorData::native_with_plugins(&catalog.manifests).expect("single registry");
    let capabilities = inspect_node_executor_capabilities(&registry).expect("capabilities");
    assert_eq!(
        capabilities
            .iter()
            .filter(|capability| {
                capability.source
                    == NodeExecutorSource::Plugin {
                        plugin_id: String::from("fixture.node"),
                    }
            })
            .count(),
        7
    );
    let declarations = &catalog.manifests[0].node_executors;
    let declaration = |id: &str| {
        declarations
            .iter()
            .find(|record| record.executor_id == id)
            .expect("declaration")
    };
    let logic = PluginCompositionLogic::new(data);

    let success = logic
        .execute_plugin_node(command(declaration("fixture.success"), 1))
        .await
        .expect("isolated success");
    assert_eq!(success.output["fixture"], true);
    assert_eq!(success.attempts, 1);

    let invalid = logic
        .execute_plugin_node(command(declaration("fixture.invalid"), 2))
        .await
        .expect_err("invalid schema output");
    assert_eq!(invalid, PluginNodeExecutionError::InvalidOutcome);

    let timeout = logic
        .execute_plugin_node(command(declaration("fixture.timeout"), 3))
        .await
        .expect_err("ambiguous timeout");
    assert!(matches!(
        timeout,
        PluginNodeExecutionError::Ambiguous {
            ref plugin_id,
            ref executor_id,
            ..
        } if plugin_id == "fixture.node" && executor_id == "fixture.timeout"
    ));

    fs::remove_file(&worker).expect("remove temporary worker after activation");
    let unavailable = logic
        .execute_plugin_node(command(declaration("fixture.unavailable"), 4))
        .await
        .expect_err("ambiguous unavailable worker");
    assert!(matches!(
        unavailable,
        PluginNodeExecutionError::Ambiguous {
            ref executor_id,
            ..
        } if executor_id == "fixture.unavailable"
    ));
    process.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
#[allow(
    clippy::too_many_lines,
    reason = "the mixed process scenario keeps runtime/plugin ordering and exact once-only assertions together"
)]
async fn arbitrary_graph_c_mixes_runtime_and_plugin_executors_once() {
    let fixture = production_fixture("fixture.graph").await;
    let logic = TurnLogic::new(fixture.data.clone(), allow_turn_policy())
        .with_plugins(Arc::new(PluginCompositionLogic::new(fixture.data.clone())))
        .with_plugin_node_runtime(Arc::new(ProductionPluginNodeTurnPort::new(
            fixture.data.clone(),
        )));
    let result = logic
        .run_turn(RunTurnCommand {
            sessions_root: fixture.sessions_root.clone(),
            session_id: fixture.session_id.to_string(),
            prompt: String::from("execute the renamed plugin graph"),
            provider: String::from("mock"),
            model: String::from("mock-model"),
            options: json!({}),
            cancellation_id: String::from("renamed-plugin-graph-turn"),
        })
        .await
        .expect("generic plugin graph turn");
    assert!(result.awaiting_continuation.is_none());
    let loaded = SessionPersistenceLogic::new(fixture.data.clone())
        .load_session(LoadSessionCommand {
            session_directory: fixture.session_directory.clone(),
            expected_session_id: fixture.session_id,
        })
        .expect("load completed graph");
    let execution = loaded.state.style_execution.expect("style execution");
    let contract = execution
        .execution_contract
        .as_deref()
        .expect("immutable execution contract");
    let context_executor = contract
        .node_executors
        .iter()
        .find(|resolution| resolution.node_id == "runtime_context")
        .expect("runtime context resolution");
    assert_eq!(context_executor.executor_id, "runtime.context-construction");
    assert_eq!(context_executor.source, SessionNodeExecutorSource::Runtime);
    assert_eq!(
        context_executor.boundary,
        SessionNodeExecutorBoundary::RuntimeLogic
    );
    let plugin_executor = contract
        .node_executors
        .iter()
        .find(|resolution| resolution.node_id == "renamed_plugin")
        .expect("plugin resolution");
    assert_eq!(plugin_executor.executor_id, "fixture.graph");
    assert_eq!(
        plugin_executor.source,
        SessionNodeExecutorSource::Plugin {
            plugin_id: String::from("fixture.node"),
        }
    );
    assert_eq!(
        plugin_executor.boundary,
        SessionNodeExecutorBoundary::PluginHost
    );
    assert_eq!(
        execution
            .completed_nodes
            .iter()
            .map(|completed| completed.node_id.as_str())
            .collect::<Vec<_>>(),
        ["runtime_context", "renamed_plugin", "renamed_done"]
    );
    let invocations = execution
        .plugin_node_invocations
        .values()
        .collect::<Vec<_>>();
    let [invocation] = invocations.as_slice() else {
        panic!("one exact plugin invocation")
    };
    assert_eq!(invocation.state, PluginNodeInvocationState::Completed);
    let application = invocation
        .outcome_application
        .as_deref()
        .expect("validated application");
    assert!(application.budget_charge.is_some());
    assert!(application.actions.is_empty());
    let preserved = application
        .preserved_state
        .as_ref()
        .expect("hash-only plugin state receipt");
    assert_eq!(
        preserved.state_hash,
        agentmod_runtime_logic::plugin::plugin_node_state_value_hash(
            &json!({"cursor":1,"status":"ready"})
        )
        .expect("state hash")
    );
    assert_eq!(
        preserved.state_scope,
        agentmod_runtime_logic::session::PluginNodeStateScope::Session
    );
    assert_eq!(invocation_count(&fixture.session_directory), 1);
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
#[allow(
    clippy::too_many_lines,
    reason = "the process proof keeps plugin invocation, canonical action proposal, durable approval restart, downstream receipt, post-action budget/state, and no-redispatch assertions adjacent"
)]
async fn plugin_proposed_tool_action_approval_restart_is_exactly_once() {
    let fixture = production_fixture("fixture.graph.action").await;
    let logic = || {
        TurnLogic::new(fixture.data.clone(), plugin_action_approval_policy())
            .with_plugins(Arc::new(PluginCompositionLogic::new(fixture.data.clone())))
            .with_plugin_node_runtime(Arc::new(ProductionPluginNodeTurnPort::new(
                fixture.data.clone(),
            )))
    };
    let command = || RunTurnCommand {
        sessions_root: fixture.sessions_root.clone(),
        session_id: fixture.session_id.to_string(),
        prompt: String::from("approve the plugin proposed tool action"),
        provider: String::from("mock"),
        model: String::from("mock-model"),
        options: json!({}),
        cancellation_id: String::from("plugin-action-approval"),
    };

    let waiting = logic()
        .run_turn(command())
        .await
        .expect("plugin tool action waits for approval");
    let continuation_id = waiting
        .awaiting_continuation
        .expect("plugin action approval continuation");
    assert_eq!(invocation_count(&fixture.session_directory), 1);
    let proposed = loaded_state(
        &fixture.data,
        fixture.session_id,
        &fixture.session_directory,
    );
    let invocation = proposed
        .style_execution
        .as_ref()
        .and_then(|execution| execution.plugin_node_invocations.values().next())
        .expect("plugin invocation");
    let application = invocation
        .outcome_application
        .as_deref()
        .expect("validated plugin outcome");
    assert!(application.budget_charge.is_none());
    let [action] = application.actions.as_slice() else {
        panic!("one exact plugin action")
    };
    assert!(action.terminal.is_none());
    let call_id = match &action.proposed.runtime_proposal {
        agentmod_runtime_logic::session::PluginNodeRuntimeActionProposal::ToolCall {
            call_id,
            tool,
            group,
            ..
        } => {
            assert_eq!(tool, "filesystem.read");
            assert_eq!(group, "filesystem");
            call_id.clone()
        }
        agentmod_runtime_logic::session::PluginNodeRuntimeActionProposal::NetworkRequest {
            ..
        } => panic!("typed tool action"),
    };
    assert!(call_id.starts_with("plugin-action:"));

    let recovered = logic()
        .run_turn(command())
        .await
        .expect("restart reconstructs plugin action approval");
    assert_eq!(
        recovered.awaiting_continuation.as_deref(),
        Some(continuation_id.as_str())
    );
    assert_eq!(invocation_count(&fixture.session_directory), 1);

    let resolved = logic()
        .resolve_turn_approval(ResolveTurnApprovalCommand {
            sessions_root: fixture.sessions_root.clone(),
            session_id: fixture.session_id.to_string(),
            continuation_id: continuation_id.clone(),
            approved: true,
            resume_after_resolution: true,
        })
        .await
        .expect("approved plugin action resumes");
    assert!(resolved.awaiting_continuation.is_none());
    logic()
        .resolve_turn_approval(ResolveTurnApprovalCommand {
            sessions_root: fixture.sessions_root.clone(),
            session_id: fixture.session_id.to_string(),
            continuation_id,
            approved: true,
            resume_after_resolution: true,
        })
        .await
        .expect("duplicate plugin action approval is idempotent");

    let completed = loaded_state(
        &fixture.data,
        fixture.session_id,
        &fixture.session_directory,
    );
    let execution = completed.style_execution.as_ref().expect("execution");
    let invocation = execution
        .plugin_node_invocations
        .values()
        .next()
        .expect("plugin invocation");
    let application = invocation
        .outcome_application
        .as_deref()
        .expect("applied plugin outcome");
    let [action] = application.actions.as_slice() else {
        panic!("one exact action")
    };
    assert!(matches!(
        action.terminal,
        Some(agentmod_runtime_logic::session::PluginNodeActionTerminalRecord::Applied(_))
    ));
    assert!(
        action.terminal_at.expect("action terminal")
            < application.budget_charged_at.expect("budget charged"),
        "budget charge must follow terminal action receipt"
    );
    assert!(
        application.state_failure.is_none(),
        "state preservation failed: {:?}",
        application.state_failure
    );
    assert!(
        application
            .state_preserved_at
            .expect("state preservation terminal")
            > application.budget_charged_at.expect("budget charged")
    );
    assert_eq!(
        completed
            .tool_executions
            .get(&call_id)
            .map(|record| record.state),
        Some(agentmod_runtime_logic::session::ToolExecutionState::Terminal)
    );
    assert_eq!(invocation_count(&fixture.session_directory), 1);
    fixture.process.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
async fn invalid_plugin_graph_outcome_is_rejected_once_and_never_redispatched() {
    let fixture = production_fixture("fixture.graph.invalid_transition").await;
    let logic = || {
        TurnLogic::new(fixture.data.clone(), allow_turn_policy())
            .with_plugins(Arc::new(PluginCompositionLogic::new(fixture.data.clone())))
            .with_plugin_node_runtime(Arc::new(ProductionPluginNodeTurnPort::new(
                fixture.data.clone(),
            )))
    };
    let command = || RunTurnCommand {
        sessions_root: fixture.sessions_root.clone(),
        session_id: fixture.session_id.to_string(),
        prompt: String::from("reject the invalid plugin outcome"),
        provider: String::from("mock"),
        model: String::from("mock-model"),
        options: json!({}),
        cancellation_id: String::from("invalid-plugin-graph-turn"),
    };

    let first = logic()
        .run_turn(command())
        .await
        .expect("invalid plugin proposal is canonically rejected");
    assert!(first.awaiting_continuation.is_none());
    let rejected = loaded_state(
        &fixture.data,
        fixture.session_id,
        &fixture.session_directory,
    );
    let invocation = rejected
        .style_execution
        .as_ref()
        .and_then(|execution| execution.plugin_node_invocations.values().next())
        .expect("rejected plugin invocation");
    assert_eq!(invocation.state, PluginNodeInvocationState::Completed);
    assert_eq!(
        invocation.failure_code.as_deref(),
        Some("invalid_transition")
    );
    assert!(invocation.diagnostic.is_some());
    assert!(invocation.outcome_application.is_none());
    assert_eq!(invocation_count(&fixture.session_directory), 1);

    let _ = logic().run_turn(command()).await;
    assert_eq!(
        invocation_count(&fixture.session_directory),
        1,
        "restart must not invoke the plugin after canonical rejection"
    );
    let replayed = loaded_state(
        &fixture.data,
        fixture.session_id,
        &fixture.session_directory,
    );
    let replayed_invocation = replayed
        .style_execution
        .as_ref()
        .and_then(|execution| execution.plugin_node_invocations.values().next())
        .expect("replayed rejected invocation");
    assert_eq!(
        replayed_invocation.failure_code.as_deref(),
        Some("invalid_transition")
    );

    fixture.process.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
async fn plugin_graph_approval_restart_and_denial_are_durable_and_effect_safe() {
    Box::pin(prove_plugin_graph_approval_restart()).await;
    Box::pin(prove_plugin_graph_denial_is_effect_safe()).await;
}

#[allow(
    clippy::too_many_lines,
    reason = "the helper keeps proposal, restart, approval, duplicate resolution, and canonical receipt evidence adjacent"
)]
async fn prove_plugin_graph_approval_restart() {
    let approved = production_fixture("fixture.graph").await;
    let approved_logic = || {
        TurnLogic::new(approved.data.clone(), turn_policy(PermissionEffect::Ask))
            .with_plugins(Arc::new(PluginCompositionLogic::new(approved.data.clone())))
            .with_plugin_node_runtime(Arc::new(ProductionPluginNodeTurnPort::new(
                approved.data.clone(),
            )))
    };
    let turn_command = || RunTurnCommand {
        sessions_root: approved.sessions_root.clone(),
        session_id: approved.session_id.to_string(),
        prompt: String::from("approve the renamed plugin graph"),
        provider: String::from("mock"),
        model: String::from("mock-model"),
        options: json!({}),
        cancellation_id: String::from("renamed-plugin-graph-approval"),
    };
    let waiting = approved_logic()
        .run_turn(turn_command())
        .await
        .expect("plugin approval proposal");
    let continuation_id = waiting
        .awaiting_continuation
        .expect("plugin approval continuation");
    assert_eq!(invocation_count(&approved.session_directory), 0);
    let proposed = loaded_state(
        &approved.data,
        approved.session_id,
        &approved.session_directory,
    );
    assert_eq!(
        proposed
            .style_execution
            .as_ref()
            .and_then(|execution| execution.plugin_node_invocations.values().next())
            .expect("proposed invocation")
            .state,
        PluginNodeInvocationState::Proposed
    );

    let recovered = approved_logic()
        .run_turn(turn_command())
        .await
        .expect("restart reconstructs approval");
    assert_eq!(
        recovered.awaiting_continuation.as_deref(),
        Some(continuation_id.as_str())
    );
    assert_eq!(invocation_count(&approved.session_directory), 0);
    let resolved = approved_logic()
        .resolve_turn_approval(ResolveTurnApprovalCommand {
            sessions_root: approved.sessions_root.clone(),
            session_id: approved.session_id.to_string(),
            continuation_id: continuation_id.clone(),
            approved: true,
            resume_after_resolution: true,
        })
        .await
        .expect("approved plugin node resumes");
    assert!(resolved.awaiting_continuation.is_none());
    approved_logic()
        .resolve_turn_approval(ResolveTurnApprovalCommand {
            sessions_root: approved.sessions_root.clone(),
            session_id: approved.session_id.to_string(),
            continuation_id,
            approved: true,
            resume_after_resolution: true,
        })
        .await
        .expect("duplicate approval is idempotent");
    assert_eq!(invocation_count(&approved.session_directory), 1);
    let completed = loaded_state(
        &approved.data,
        approved.session_id,
        &approved.session_directory,
    );
    let completed_invocations = &completed
        .style_execution
        .as_ref()
        .expect("completed execution")
        .plugin_node_invocations;
    assert_eq!(
        completed_invocations.len(),
        1,
        "duplicate approval must not create another invocation: {completed_invocations:?}"
    );
    assert_eq!(
        completed_invocations
            .values()
            .next()
            .expect("completed invocation")
            .state,
        PluginNodeInvocationState::Completed,
        "duplicate approval must preserve the completed receipt: {completed_invocations:?}"
    );
    approved.process.shutdown().await;
}

async fn prove_plugin_graph_denial_is_effect_safe() {
    let denied = production_fixture("fixture.graph").await;
    let denied_logic = || {
        TurnLogic::new(denied.data.clone(), turn_policy(PermissionEffect::Ask))
            .with_plugins(Arc::new(PluginCompositionLogic::new(denied.data.clone())))
            .with_plugin_node_runtime(Arc::new(ProductionPluginNodeTurnPort::new(
                denied.data.clone(),
            )))
    };
    let waiting = denied_logic()
        .run_turn(RunTurnCommand {
            sessions_root: denied.sessions_root.clone(),
            session_id: denied.session_id.to_string(),
            prompt: String::from("deny the renamed plugin graph"),
            provider: String::from("mock"),
            model: String::from("mock-model"),
            options: json!({}),
            cancellation_id: String::from("renamed-plugin-graph-denial"),
        })
        .await
        .expect("plugin denial proposal");
    let denial_id = waiting
        .awaiting_continuation
        .expect("plugin denial continuation");
    assert_eq!(invocation_count(&denied.session_directory), 0);
    denied_logic()
        .resolve_turn_approval(ResolveTurnApprovalCommand {
            sessions_root: denied.sessions_root.clone(),
            session_id: denied.session_id.to_string(),
            continuation_id: denial_id.clone(),
            approved: false,
            resume_after_resolution: true,
        })
        .await
        .expect("plugin denial is canonical");
    denied_logic()
        .resolve_turn_approval(ResolveTurnApprovalCommand {
            sessions_root: denied.sessions_root.clone(),
            session_id: denied.session_id.to_string(),
            continuation_id: denial_id,
            approved: false,
            resume_after_resolution: true,
        })
        .await
        .expect("duplicate denial is idempotent");
    assert_eq!(invocation_count(&denied.session_directory), 0);
    let failed = loaded_state(&denied.data, denied.session_id, &denied.session_directory);
    assert_eq!(
        failed
            .style_execution
            .as_ref()
            .and_then(|execution| execution.plugin_node_invocations.values().next())
            .expect("denied invocation")
            .state,
        PluginNodeInvocationState::Failed
    );
    denied.process.shutdown().await;
}

#[derive(Clone, Copy)]
enum ExpectedProductionOutcome {
    Completed,
    Ambiguous,
}

#[allow(
    clippy::too_many_lines,
    reason = "the helper keeps first execution, canonical receipt assertions, full reconstruction, and no-redispatch evidence adjacent"
)]
async fn prove_production_outcome(
    executor_id: &str,
    expected: ExpectedProductionOutcome,
    remove_worker_before_drive: bool,
) {
    let fixture = production_fixture(executor_id).await;
    if remove_worker_before_drive {
        fs::remove_file(&fixture.worker).expect("remove worker after session creation");
    }
    let authorization = Arc::new(StrictPluginAuthorization::default());
    let first = ProductionPluginTurnRuntime::new(
        fixture.data.clone(),
        fixture.session_id,
        fixture.session_directory.clone(),
    )
    .coordinator(authorization.clone())
    .drive(fixture.command.clone())
    .await
    .expect("production plugin turn");
    match (expected, &first.outcome) {
        (
            ExpectedProductionOutcome::Completed,
            PluginTurnOutcome::ProposalPendingValidation { proposal, .. },
        ) => {
            assert_eq!(proposal.output["fixture"], true);
            assert_eq!(proposal.output["executor_id"], executor_id);
            assert_eq!(proposal.output["input"], json!({"value":executor_id}));
        }
        (ExpectedProductionOutcome::Ambiguous, PluginTurnOutcome::AmbiguousFailClosed { .. }) => {}
        _ => panic!(
            "unexpected production plugin outcome for {executor_id}: {:?}",
            first.outcome
        ),
    }
    assert_eq!(authorization.calls.load(Ordering::SeqCst), 1);
    assert_eq!(authorization.observed.lock().expect("observed").len(), 1);
    assert_eq!(receipt_count(&fixture.session_directory), 1);
    let before = loaded_state(
        &fixture.data,
        fixture.session_id,
        &fixture.session_directory,
    );
    let record = before
        .style_execution
        .as_ref()
        .expect("execution")
        .plugin_node_invocations
        .values()
        .next()
        .expect("invocation");
    assert_eq!(record.proposed_at, Sequence::new(5).expect("sequence"));
    assert_eq!(
        record.authorized_at,
        Some(Sequence::new(6).expect("sequence"))
    );
    assert_eq!(
        record.dispatched_at,
        Some(Sequence::new(7).expect("sequence"))
    );
    assert_eq!(
        record.terminal_at,
        Some(Sequence::new(8).expect("sequence"))
    );
    assert_eq!(
        record.state,
        match expected {
            ExpectedProductionOutcome::Completed => PluginNodeInvocationState::Completed,
            ExpectedProductionOutcome::Ambiguous => PluginNodeInvocationState::Ambiguous,
        }
    );
    let first_worker_count = invocation_count(&fixture.session_directory);
    if remove_worker_before_drive {
        assert_eq!(first_worker_count, 0);
    } else {
        assert_eq!(first_worker_count, 1);
    }
    fixture.process.shutdown().await;

    let (reconstructed_data, reconstructed_process) = process_data(
        &fixture.sessions_root,
        &fixture.catalog,
        &fixture.registry,
        &fixture.process_config,
        &fixture.cancellations,
    );
    let replay_authorization = Arc::new(StrictPluginAuthorization::default());
    let replayed = ProductionPluginTurnRuntime::new(
        reconstructed_data.clone(),
        fixture.session_id,
        fixture.session_directory.clone(),
    )
    .coordinator(replay_authorization.clone())
    .drive(fixture.command)
    .await
    .expect("reconstructed plugin turn");
    assert_eq!(replayed, first);
    assert_eq!(replay_authorization.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        loaded_state(
            &reconstructed_data,
            fixture.session_id,
            &fixture.session_directory,
        ),
        before
    );
    assert_eq!(
        invocation_count(&fixture.session_directory),
        first_worker_count
    );
    assert_eq!(receipt_count(&fixture.session_directory), 1);
    reconstructed_process.shutdown().await;
}

#[tokio::test]
#[ignore = "run through tests/e2e/plugin_node_executor.{ps1,sh} after building process binaries"]
async fn production_coordinator_persists_receipts_and_never_redispatches_after_reconstruction() {
    prove_production_outcome(
        "fixture.success",
        ExpectedProductionOutcome::Completed,
        false,
    )
    .await;
    prove_production_outcome(
        "fixture.invalid",
        ExpectedProductionOutcome::Ambiguous,
        false,
    )
    .await;
    prove_production_outcome(
        "fixture.timeout",
        ExpectedProductionOutcome::Ambiguous,
        false,
    )
    .await;
    prove_production_outcome(
        "fixture.unavailable",
        ExpectedProductionOutcome::Ambiguous,
        true,
    )
    .await;

    let fixture = production_fixture("fixture.success").await;
    fixture
        .data
        .request_runtime_cancellation(RequestRuntimeCancellationDataCommand {
            cancellation_id: fixture.command.cancellation_id.clone(),
        })
        .expect("request cancellation");
    let authorization = Arc::new(StrictPluginAuthorization::default());
    let cancelled = ProductionPluginTurnRuntime::new(
        fixture.data.clone(),
        fixture.session_id,
        fixture.session_directory.clone(),
    )
    .coordinator(authorization.clone())
    .drive(fixture.command.clone())
    .await
    .expect("cancelled production turn");
    assert!(matches!(
        cancelled.outcome,
        PluginTurnOutcome::Failed { ref code } if code == "cancelled_before_authorization"
    ));
    assert_eq!(authorization.calls.load(Ordering::SeqCst), 0);
    assert_eq!(invocation_count(&fixture.session_directory), 0);
    assert_eq!(receipt_count(&fixture.session_directory), 0);
    assert!(
        fixture
            .data
            .clear_runtime_cancellation(ClearRuntimeCancellationDataCommand {
                cancellation_id: fixture.command.cancellation_id.clone(),
            })
            .expect("clear cancellation")
    );
    let before = loaded_state(
        &fixture.data,
        fixture.session_id,
        &fixture.session_directory,
    );
    fixture.process.shutdown().await;
    let (reconstructed_data, reconstructed_process) = process_data(
        &fixture.sessions_root,
        &fixture.catalog,
        &fixture.registry,
        &fixture.process_config,
        &fixture.cancellations,
    );
    let replay_authorization = Arc::new(StrictPluginAuthorization::default());
    let replayed = ProductionPluginTurnRuntime::new(
        reconstructed_data.clone(),
        fixture.session_id,
        fixture.session_directory.clone(),
    )
    .coordinator(replay_authorization.clone())
    .drive(fixture.command)
    .await
    .expect("replay cancellation");
    assert_eq!(replayed, cancelled);
    assert_eq!(replay_authorization.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        loaded_state(
            &reconstructed_data,
            fixture.session_id,
            &fixture.session_directory,
        ),
        before
    );
    assert_eq!(invocation_count(&fixture.session_directory), 0);
    reconstructed_process.shutdown().await;
}
