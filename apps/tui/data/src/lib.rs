//! TUI business datasets and explicit dependency normalization.
#![allow(
    missing_docs,
    reason = "data-local frontend records are boundary-specific"
)]
#![allow(
    clippy::missing_errors_doc,
    reason = "the data port exposes one documented closed error taxonomy"
)]

use agentmod_primitives::{CancellationId, Sequence, SessionId};
use agentmod_tui_dependency::{
    DependencyArtifactResource, DependencyAttachment, DependencyAttachmentKind,
    DependencyBranchSessionRequest, DependencyChildResource, DependencyCreateSessionRequest,
    DependencyMcpOAuthAction, DependencyMcpOAuthRequest, DependencyMcpOAuthResponse,
    DependencyPluginLifecycleAction, DependencyPluginLifecycleRequest, DependencyProcessResource,
    DependencySchedule, DependencySchedulePayload, DependencyScheduleTrigger,
    DependencySessionBudgetSelection, DependencySessionEventStream, DependencyStyleAvailability,
    DependencyStyleInspection, DependencyStyleSourceKind, DependencyTurnEvent,
    DependencyTurnStream, DependencyTurnStreamItem, TuiDependencyError, TuiRuntimeDependencyPort,
};
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeHealthDataRecord {
    pub ready: bool,
    pub version: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachmentDataKind {
    Image,
    Audio,
    Blob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachmentDataRecord {
    pub identity: String,
    pub name: String,
    pub uri: String,
    pub mime_type: String,
    pub kind: AttachmentDataKind,
    pub data_base64: String,
    pub byte_size: u64,
}

/// Data-owned optional session budget selection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionBudgetDataRequest {
    pub max_iterations: Option<u32>,
    pub max_steps: Option<u64>,
    pub max_tokens: Option<u64>,
    pub max_cost_micros: Option<u64>,
    pub max_duration_ms: Option<u64>,
}

/// Data-owned complete session creation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionDataRequest {
    pub workspace: String,
    pub style: String,
    pub harness: Option<String>,
    pub memory: Option<String>,
    pub compaction: Option<String>,
    pub budgets: Option<SessionBudgetDataRequest>,
}

/// Data-owned style provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleDataSourceKind {
    BuiltIn,
    User,
    Project,
    Plugin,
    Inline,
}

/// Data-owned style selection availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StyleDataAvailability {
    Available,
    Disabled,
    Invalid,
    Incompatible,
    Conflict,
}

/// Data-owned bounded style catalog row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleDataRecord {
    pub id: String,
    pub version: String,
    pub source: StyleDataSourceKind,
    pub availability: StyleDataAvailability,
    pub style_content_hash: String,
    pub compiled_cache_key: String,
    pub required_capabilities: Vec<String>,
}

/// Data-owned style diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StyleDiagnosticDataRecord {
    pub code: String,
    pub path: String,
    pub message: String,
    pub help: String,
}

/// Data-owned complete style inspection.
#[derive(Clone, Debug, PartialEq)]
pub struct StyleInspectionDataRecord {
    pub summary: StyleDataRecord,
    pub source_locator: String,
    pub manifest: Value,
    pub compiled: Option<Value>,
    pub diagnostics: Vec<StyleDiagnosticDataRecord>,
}

/// Data-owned harness descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessDataRecord {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub capability_set_hash: String,
    pub availability: String,
}

/// Data-owned style-selectable component catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionComponentDataRecord {
    pub memory_providers: Vec<String>,
    pub compaction_strategies: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionDataRecord {
    pub id: SessionId,
    pub workspace: String,
    pub style: String,
    pub sequence: Sequence,
    pub state: String,
}

/// Data-owned atomic branch request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionDataRequest {
    pub parent_session_id: SessionId,
    pub at: Sequence,
    pub style: Option<String>,
}

/// Data-owned atomic branch record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchSessionDataRecord {
    pub session_id: SessionId,
    pub parent_session_id: SessionId,
    pub fork_sequence: Sequence,
    pub child_head_sequence: Sequence,
}

/// Data-owned plugin lifecycle action.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginLifecycleDataAction {
    Disable,
    Enable,
    Quarantine,
    Unquarantine,
}

/// Data-owned plugin lifecycle request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginLifecycleDataRequest {
    pub session_id: SessionId,
    pub plugin_id: String,
    pub action: PluginLifecycleDataAction,
    pub reason_code: Option<String>,
    pub cancellation_id: CancellationId,
}

/// Data-owned canonical plugin lifecycle result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginLifecycleDataRecord {
    pub session_id: SessionId,
    pub plugin_id: String,
    pub plugin_version: String,
    pub state: String,
    pub committed_sequence: Sequence,
    pub replayed: bool,
}

/// Data-owned MCP OAuth management action.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpOAuthDataAction {
    Begin,
    Status,
    Cancel { transaction_id: String },
}

/// Data-owned exact MCP OAuth management request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpOAuthDataRequest {
    pub session_id: SessionId,
    pub server_id: String,
    pub action: McpOAuthDataAction,
    pub cancellation_id: CancellationId,
}

/// Data-owned bounded MCP OAuth result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpOAuthDataRecord {
    Started {
        server_id: String,
        transaction_id: String,
        authorization_url: String,
        authorization_url_hash: String,
        expires_at_ms: i64,
    },
    Status {
        server_id: String,
        status: String,
        transaction_id: Option<String>,
        expires_at_ms: Option<i64>,
        scopes: Vec<String>,
        status_hash: String,
    },
}

/// Data-owned replay-only artifact row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactResourceDataRecord {
    pub execution_id: String,
    pub node_id: String,
    pub state: String,
    pub mime_type: String,
    pub byte_size: u64,
    pub artifact_reference: Option<String>,
}

/// Data-owned replay-only child row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildResourceDataRecord {
    pub execution_id: String,
    pub task_id: String,
    pub state: String,
    pub child_style: String,
    pub workspace_mode: String,
    pub child_session_id: Option<String>,
    pub summary: Option<String>,
}

/// Data-owned replay-only process reconciliation row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessResourceDataRecord {
    pub call_id: String,
    pub process_id: String,
    pub status: Option<String>,
    pub started_at: u64,
    pub completed_at: Option<u64>,
}

/// Data-owned bounded canonical runtime-resource projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeResourcesDataRecord {
    pub artifacts: Vec<ArtifactResourceDataRecord>,
    pub children: Vec<ChildResourceDataRecord>,
    pub processes: Vec<ProcessResourceDataRecord>,
}

/// Data-owned schedule trigger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleDataTrigger {
    AtMillis(i64),
    Interval {
        starts_at_ms: i64,
        every_ms: u64,
    },
    RuntimeEvent {
        event_type: String,
    },
    ProcessOutput {
        process_id: String,
        contains: String,
    },
}

/// Data-owned schedule payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleDataPayload {
    Prompt { prompt: String },
    Continuation { continuation_id: String },
    GraphTrigger { run_id: String, node_id: String },
}

/// Data-owned durable schedule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleDataRecord {
    pub schedule_id: String,
    pub session_id: SessionId,
    pub idempotency_id: String,
    pub style: String,
    pub workspace: String,
    pub permission_policy: String,
    pub provider: String,
    pub model: String,
    pub token_budget: u64,
    pub cost_budget_micros: u64,
    pub trigger: ScheduleDataTrigger,
    pub payload: ScheduleDataPayload,
    pub active: bool,
}

/// Data-owned schedule store result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduleStoreDataRecord {
    pub schedule_id: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventDataRecord {
    pub sequence: Sequence,
    pub event_type: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SessionEventPageDataRecord {
    pub events: Vec<SessionEventDataRecord>,
    pub head_sequence: Sequence,
    pub cursor: Option<Sequence>,
    pub has_more: bool,
}

/// Data-owned bounded stream of newly committed canonical events.
pub struct SessionEventDataStream {
    dependency: DependencySessionEventStream,
}

impl SessionEventDataStream {
    #[must_use]
    pub fn try_next(&self) -> Option<Result<SessionEventDataRecord, TuiDataError>> {
        self.dependency.try_next().map(|value| {
            value
                .map(|event| SessionEventDataRecord {
                    sequence: event.sequence,
                    event_type: event.event_type,
                    payload: event.payload,
                })
                .map_err(map_error)
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnDataEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
        input_tokens: u64,
        output_tokens: u64,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnDataStreamItem {
    Event {
        event: TurnDataEvent,
        committed_sequence: Sequence,
    },
    Complete {
        first_sequence: Sequence,
        last_sequence: Sequence,
        awaiting_continuation: Option<String>,
    },
}

pub struct TurnDataStream {
    dependency: DependencyTurnStream,
}

impl TurnDataStream {
    #[must_use]
    pub fn try_next(&self) -> Option<Result<TurnDataStreamItem, TuiDataError>> {
        self.dependency.try_next().map(|value| {
            value
                .map(map_stream_item)
                .map_err(|error| TuiDataError::Dependency(error.to_string()))
        })
    }
}

pub trait TuiDataPort {
    fn load_attachment(
        &self,
        _workspace: String,
        _path: String,
    ) -> Result<AttachmentDataRecord, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "attachment loading unavailable",
        )))
    }
    fn runtime_health(&self) -> Result<RuntimeHealthDataRecord, TuiDataError>;
    fn list_styles(&self) -> Result<Vec<StyleDataRecord>, TuiDataError>;
    fn inspect_style(&self, _selector: String) -> Result<StyleInspectionDataRecord, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "style inspection unavailable",
        )))
    }
    fn list_harnesses(&self) -> Result<Vec<HarnessDataRecord>, TuiDataError> {
        Ok(Vec::new())
    }
    fn list_session_components(&self) -> Result<SessionComponentDataRecord, TuiDataError> {
        Ok(SessionComponentDataRecord {
            memory_providers: Vec::new(),
            compaction_strategies: Vec::new(),
        })
    }
    fn list_sessions(&self, limit: u32) -> Result<Vec<SessionDataRecord>, TuiDataError>;
    fn inspect_session(&self, _session_id: SessionId) -> Result<Value, TuiDataError> {
        Ok(Value::Null)
    }
    fn inspect_runtime_resources(
        &self,
        _session_id: SessionId,
    ) -> Result<RuntimeResourcesDataRecord, TuiDataError> {
        Ok(RuntimeResourcesDataRecord {
            artifacts: Vec::new(),
            children: Vec::new(),
            processes: Vec::new(),
        })
    }
    fn create_session(&self, workspace: String, style: String) -> Result<SessionId, TuiDataError>;
    fn create_session_with_harness(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
    ) -> Result<SessionId, TuiDataError> {
        let _ = harness;
        self.create_session(workspace, style)
    }
    fn create_session_with_components(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
        memory: Option<String>,
        compaction: Option<String>,
    ) -> Result<SessionId, TuiDataError> {
        let _ = (memory, compaction);
        self.create_session_with_harness(workspace, style, harness)
    }
    fn create_session_with_configuration(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<SessionId, TuiDataError> {
        let _ = request.budgets;
        self.create_session_with_components(
            request.workspace,
            request.style,
            request.harness,
            request.memory,
            request.compaction,
        )
    }
    fn branch_session(
        &self,
        request: BranchSessionDataRequest,
    ) -> Result<BranchSessionDataRecord, TuiDataError>;
    fn change_plugin_lifecycle(
        &self,
        _request: PluginLifecycleDataRequest,
    ) -> Result<PluginLifecycleDataRecord, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "plugin lifecycle management unavailable",
        )))
    }
    fn manage_mcp_oauth(
        &self,
        _request: McpOAuthDataRequest,
    ) -> Result<McpOAuthDataRecord, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "MCP OAuth management unavailable",
        )))
    }
    fn upsert_schedule(
        &self,
        _schedule: ScheduleDataRecord,
    ) -> Result<ScheduleStoreDataRecord, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "schedule management unavailable",
        )))
    }
    fn list_schedules(&self, _limit: u32) -> Result<Vec<ScheduleDataRecord>, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "schedule management unavailable",
        )))
    }
    fn remove_schedule(&self, _schedule_id: &str) -> Result<bool, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "schedule management unavailable",
        )))
    }
    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<SessionEventPageDataRecord, TuiDataError>;
    fn start_session_subscription(
        &self,
        _session_id: SessionId,
        _after: Option<Sequence>,
    ) -> Result<SessionEventDataStream, TuiDataError> {
        Err(TuiDataError::Dependency(String::from(
            "live event subscription unavailable",
        )))
    }
    fn start_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        provider: String,
        model: String,
        options: Value,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataStream, TuiDataError>;
    fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<TurnDataEvent>, TuiDataError>;
    fn cancel(&self, cancellation_id: CancellationId, reason: String) -> Result<(), TuiDataError>;
}

#[derive(Clone, Debug)]
pub struct TuiData<D> {
    dependency: D,
}

impl<D> TuiData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D: TuiRuntimeDependencyPort> TuiDataPort for TuiData<D> {
    fn load_attachment(
        &self,
        workspace: String,
        path: String,
    ) -> Result<AttachmentDataRecord, TuiDataError> {
        self.dependency
            .load_attachment(workspace, path)
            .map(map_attachment)
            .map_err(map_error)
    }
    fn runtime_health(&self) -> Result<RuntimeHealthDataRecord, TuiDataError> {
        self.dependency
            .health()
            .map(|value| RuntimeHealthDataRecord {
                ready: value.status == "ok",
                version: value.version,
            })
            .map_err(map_error)
    }

    fn list_styles(&self) -> Result<Vec<StyleDataRecord>, TuiDataError> {
        self.dependency
            .list_styles()
            .map(|styles| {
                styles
                    .into_iter()
                    .map(|style| StyleDataRecord {
                        id: style.id,
                        version: style.version,
                        source: match style.source {
                            DependencyStyleSourceKind::BuiltIn => StyleDataSourceKind::BuiltIn,
                            DependencyStyleSourceKind::User => StyleDataSourceKind::User,
                            DependencyStyleSourceKind::Project => StyleDataSourceKind::Project,
                            DependencyStyleSourceKind::Plugin => StyleDataSourceKind::Plugin,
                            DependencyStyleSourceKind::Inline => StyleDataSourceKind::Inline,
                        },
                        availability: match style.availability {
                            DependencyStyleAvailability::Available => {
                                StyleDataAvailability::Available
                            }
                            DependencyStyleAvailability::Disabled => {
                                StyleDataAvailability::Disabled
                            }
                            DependencyStyleAvailability::Invalid => StyleDataAvailability::Invalid,
                            DependencyStyleAvailability::Incompatible => {
                                StyleDataAvailability::Incompatible
                            }
                            DependencyStyleAvailability::Conflict => {
                                StyleDataAvailability::Conflict
                            }
                        },
                        style_content_hash: style.style_content_hash,
                        compiled_cache_key: style.compiled_cache_key,
                        required_capabilities: style.required_capabilities,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn inspect_style(&self, selector: String) -> Result<StyleInspectionDataRecord, TuiDataError> {
        self.dependency
            .inspect_style(selector)
            .map(map_style_inspection)
            .map_err(map_error)
    }

    fn list_harnesses(&self) -> Result<Vec<HarnessDataRecord>, TuiDataError> {
        self.dependency
            .list_harnesses()
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| HarnessDataRecord {
                        id: value.id,
                        version: value.version,
                        capabilities: value.capabilities,
                        capability_set_hash: value.capability_set_hash,
                        availability: value.availability,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn list_session_components(&self) -> Result<SessionComponentDataRecord, TuiDataError> {
        self.dependency
            .list_session_components()
            .map(|value| SessionComponentDataRecord {
                memory_providers: value.memory_providers,
                compaction_strategies: value.compaction_strategies,
            })
            .map_err(map_error)
    }

    fn list_sessions(&self, limit: u32) -> Result<Vec<SessionDataRecord>, TuiDataError> {
        self.dependency
            .list_sessions(limit)
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| SessionDataRecord {
                        id: value.id,
                        workspace: value.workspace,
                        style: value.style,
                        sequence: value.sequence,
                        state: value.state,
                    })
                    .collect()
            })
            .map_err(map_error)
    }

    fn inspect_session(&self, session_id: SessionId) -> Result<Value, TuiDataError> {
        self.dependency
            .inspect_session(session_id)
            .map_err(map_error)
    }

    fn inspect_runtime_resources(
        &self,
        session_id: SessionId,
    ) -> Result<RuntimeResourcesDataRecord, TuiDataError> {
        self.dependency
            .inspect_runtime_resources(session_id)
            .map(|resources| RuntimeResourcesDataRecord {
                artifacts: resources
                    .artifacts
                    .into_iter()
                    .map(map_artifact_resource)
                    .collect(),
                children: resources
                    .children
                    .into_iter()
                    .map(map_child_resource)
                    .collect(),
                processes: resources
                    .processes
                    .into_iter()
                    .map(map_process_resource)
                    .collect(),
            })
            .map_err(map_error)
    }

    fn create_session(&self, workspace: String, style: String) -> Result<SessionId, TuiDataError> {
        self.dependency
            .create_session(workspace, style)
            .map_err(map_error)
    }

    fn create_session_with_harness(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
    ) -> Result<SessionId, TuiDataError> {
        self.dependency
            .create_session_with_harness(workspace, style, harness)
            .map_err(map_error)
    }

    fn create_session_with_components(
        &self,
        workspace: String,
        style: String,
        harness: Option<String>,
        memory: Option<String>,
        compaction: Option<String>,
    ) -> Result<SessionId, TuiDataError> {
        self.dependency
            .create_session_with_components(workspace, style, harness, memory, compaction)
            .map_err(map_error)
    }

    fn create_session_with_configuration(
        &self,
        request: CreateSessionDataRequest,
    ) -> Result<SessionId, TuiDataError> {
        self.dependency
            .create_session_with_configuration(DependencyCreateSessionRequest {
                workspace: request.workspace,
                style: request.style,
                harness: request.harness,
                memory: request.memory,
                compaction: request.compaction,
                budgets: request
                    .budgets
                    .map(|budgets| DependencySessionBudgetSelection {
                        max_iterations: budgets.max_iterations,
                        max_steps: budgets.max_steps,
                        max_tokens: budgets.max_tokens,
                        max_cost_micros: budgets.max_cost_micros,
                        max_duration_ms: budgets.max_duration_ms,
                    }),
            })
            .map_err(map_error)
    }

    fn branch_session(
        &self,
        request: BranchSessionDataRequest,
    ) -> Result<BranchSessionDataRecord, TuiDataError> {
        self.dependency
            .branch_session(DependencyBranchSessionRequest {
                parent_session_id: request.parent_session_id,
                at: request.at,
                style: request.style,
            })
            .map(|response| BranchSessionDataRecord {
                session_id: response.session_id,
                parent_session_id: response.parent_session_id,
                fork_sequence: response.fork_sequence,
                child_head_sequence: response.child_head_sequence,
            })
            .map_err(map_error)
    }

    fn change_plugin_lifecycle(
        &self,
        request: PluginLifecycleDataRequest,
    ) -> Result<PluginLifecycleDataRecord, TuiDataError> {
        self.dependency
            .change_plugin_lifecycle(DependencyPluginLifecycleRequest {
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                action: match request.action {
                    PluginLifecycleDataAction::Disable => DependencyPluginLifecycleAction::Disable,
                    PluginLifecycleDataAction::Enable => DependencyPluginLifecycleAction::Enable,
                    PluginLifecycleDataAction::Quarantine => {
                        DependencyPluginLifecycleAction::Quarantine
                    }
                    PluginLifecycleDataAction::Unquarantine => {
                        DependencyPluginLifecycleAction::Unquarantine
                    }
                },
                reason_code: request.reason_code,
                cancellation_id: request.cancellation_id,
            })
            .map(|response| PluginLifecycleDataRecord {
                session_id: response.session_id,
                plugin_id: response.plugin_id,
                plugin_version: response.plugin_version,
                state: response.state,
                committed_sequence: response.committed_sequence,
                replayed: response.replayed,
            })
            .map_err(map_error)
    }

    fn manage_mcp_oauth(
        &self,
        request: McpOAuthDataRequest,
    ) -> Result<McpOAuthDataRecord, TuiDataError> {
        self.dependency
            .manage_mcp_oauth(DependencyMcpOAuthRequest {
                session_id: request.session_id,
                server_id: request.server_id,
                action: match request.action {
                    McpOAuthDataAction::Begin => DependencyMcpOAuthAction::Begin,
                    McpOAuthDataAction::Status => DependencyMcpOAuthAction::Status,
                    McpOAuthDataAction::Cancel { transaction_id } => {
                        DependencyMcpOAuthAction::Cancel { transaction_id }
                    }
                },
                cancellation_id: request.cancellation_id,
            })
            .map(|response| match response {
                DependencyMcpOAuthResponse::Started {
                    server_id,
                    transaction_id,
                    authorization_url,
                    authorization_url_hash,
                    expires_at_ms,
                } => McpOAuthDataRecord::Started {
                    server_id,
                    transaction_id,
                    authorization_url,
                    authorization_url_hash,
                    expires_at_ms,
                },
                DependencyMcpOAuthResponse::Status {
                    server_id,
                    status,
                    transaction_id,
                    expires_at_ms,
                    scopes,
                    status_hash,
                } => McpOAuthDataRecord::Status {
                    server_id,
                    status,
                    transaction_id,
                    expires_at_ms,
                    scopes,
                    status_hash,
                },
            })
            .map_err(map_error)
    }

    fn upsert_schedule(
        &self,
        schedule: ScheduleDataRecord,
    ) -> Result<ScheduleStoreDataRecord, TuiDataError> {
        self.dependency
            .upsert_schedule(to_dependency_schedule(schedule))
            .map(|response| ScheduleStoreDataRecord {
                schedule_id: response.schedule_id,
                replayed: response.replayed,
            })
            .map_err(map_error)
    }

    fn list_schedules(&self, limit: u32) -> Result<Vec<ScheduleDataRecord>, TuiDataError> {
        self.dependency
            .list_schedules(limit)
            .map(|schedules| {
                schedules
                    .into_iter()
                    .map(from_dependency_schedule)
                    .collect()
            })
            .map_err(map_error)
    }

    fn remove_schedule(&self, schedule_id: &str) -> Result<bool, TuiDataError> {
        self.dependency
            .remove_schedule(schedule_id)
            .map_err(map_error)
    }

    fn session_events(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
        limit: u32,
    ) -> Result<SessionEventPageDataRecord, TuiDataError> {
        self.dependency
            .session_events(session_id, after, limit)
            .map(|value| SessionEventPageDataRecord {
                events: value
                    .events
                    .into_iter()
                    .map(|event| SessionEventDataRecord {
                        sequence: event.sequence,
                        event_type: event.event_type,
                        payload: event.payload,
                    })
                    .collect(),
                head_sequence: value.head_sequence,
                cursor: value.last_delivered_sequence,
                has_more: value.has_more,
            })
            .map_err(map_error)
    }

    fn start_session_subscription(
        &self,
        session_id: SessionId,
        after: Option<Sequence>,
    ) -> Result<SessionEventDataStream, TuiDataError> {
        self.dependency
            .start_session_subscription(session_id, after)
            .map(|dependency| SessionEventDataStream { dependency })
            .map_err(map_error)
    }

    fn start_turn(
        &self,
        session_id: SessionId,
        prompt: String,
        provider: String,
        model: String,
        options: Value,
        cancellation_id: CancellationId,
    ) -> Result<TurnDataStream, TuiDataError> {
        self.dependency
            .start_turn(
                session_id,
                prompt,
                provider,
                model,
                options,
                cancellation_id,
            )
            .map(|dependency| TurnDataStream { dependency })
            .map_err(map_error)
    }

    fn resolve_approval(
        &self,
        session_id: SessionId,
        continuation_id: String,
        approved: bool,
    ) -> Result<Vec<TurnDataEvent>, TuiDataError> {
        self.dependency
            .resolve_approval(session_id, continuation_id, approved)
            .map(|values| values.into_iter().map(map_event).collect())
            .map_err(map_error)
    }

    fn cancel(&self, cancellation_id: CancellationId, reason: String) -> Result<(), TuiDataError> {
        self.dependency
            .cancel(cancellation_id, reason)
            .map_err(map_error)
    }
}

fn map_stream_item(value: DependencyTurnStreamItem) -> TurnDataStreamItem {
    match value {
        DependencyTurnStreamItem::Event {
            event,
            committed_sequence,
        } => TurnDataStreamItem::Event {
            event: map_event(event),
            committed_sequence,
        },
        DependencyTurnStreamItem::Complete {
            first_committed_sequence,
            last_committed_sequence,
            awaiting_continuation,
        } => TurnDataStreamItem::Complete {
            first_sequence: first_committed_sequence,
            last_sequence: last_committed_sequence,
            awaiting_continuation,
        },
    }
}

fn map_artifact_resource(value: DependencyArtifactResource) -> ArtifactResourceDataRecord {
    ArtifactResourceDataRecord {
        execution_id: value.execution_id,
        node_id: value.node_id,
        state: value.state,
        mime_type: value.mime_type,
        byte_size: value.byte_size,
        artifact_reference: value.artifact_reference,
    }
}

fn map_child_resource(value: DependencyChildResource) -> ChildResourceDataRecord {
    ChildResourceDataRecord {
        execution_id: value.execution_id,
        task_id: value.task_id,
        state: value.state,
        child_style: value.child_style,
        workspace_mode: value.workspace_mode,
        child_session_id: value.child_session_id,
        summary: value.summary,
    }
}

fn map_process_resource(value: DependencyProcessResource) -> ProcessResourceDataRecord {
    ProcessResourceDataRecord {
        call_id: value.call_id,
        process_id: value.process_id,
        status: value.status,
        started_at: value.started_at,
        completed_at: value.completed_at,
    }
}

fn to_dependency_schedule(schedule: ScheduleDataRecord) -> DependencySchedule {
    DependencySchedule {
        schedule_id: schedule.schedule_id,
        session_id: schedule.session_id,
        idempotency_id: schedule.idempotency_id,
        style: schedule.style,
        workspace: schedule.workspace,
        permission_policy: schedule.permission_policy,
        provider: schedule.provider,
        model: schedule.model,
        token_budget: schedule.token_budget,
        cost_budget_micros: schedule.cost_budget_micros,
        trigger: match schedule.trigger {
            ScheduleDataTrigger::AtMillis(value) => DependencyScheduleTrigger::AtMillis(value),
            ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => DependencyScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            ScheduleDataTrigger::RuntimeEvent { event_type } => {
                DependencyScheduleTrigger::RuntimeEvent { event_type }
            }
            ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            } => DependencyScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match schedule.payload {
            ScheduleDataPayload::Prompt { prompt } => DependencySchedulePayload::Prompt { prompt },
            ScheduleDataPayload::Continuation { continuation_id } => {
                DependencySchedulePayload::Continuation { continuation_id }
            }
            ScheduleDataPayload::GraphTrigger { run_id, node_id } => {
                DependencySchedulePayload::GraphTrigger { run_id, node_id }
            }
        },
        active: schedule.active,
    }
}

fn from_dependency_schedule(schedule: DependencySchedule) -> ScheduleDataRecord {
    ScheduleDataRecord {
        schedule_id: schedule.schedule_id,
        session_id: schedule.session_id,
        idempotency_id: schedule.idempotency_id,
        style: schedule.style,
        workspace: schedule.workspace,
        permission_policy: schedule.permission_policy,
        provider: schedule.provider,
        model: schedule.model,
        token_budget: schedule.token_budget,
        cost_budget_micros: schedule.cost_budget_micros,
        trigger: match schedule.trigger {
            DependencyScheduleTrigger::AtMillis(value) => ScheduleDataTrigger::AtMillis(value),
            DependencyScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } => ScheduleDataTrigger::Interval {
                starts_at_ms,
                every_ms,
            },
            DependencyScheduleTrigger::RuntimeEvent { event_type } => {
                ScheduleDataTrigger::RuntimeEvent { event_type }
            }
            DependencyScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } => ScheduleDataTrigger::ProcessOutput {
                process_id,
                contains,
            },
        },
        payload: match schedule.payload {
            DependencySchedulePayload::Prompt { prompt } => ScheduleDataPayload::Prompt { prompt },
            DependencySchedulePayload::Continuation { continuation_id } => {
                ScheduleDataPayload::Continuation { continuation_id }
            }
            DependencySchedulePayload::GraphTrigger { run_id, node_id } => {
                ScheduleDataPayload::GraphTrigger { run_id, node_id }
            }
        },
        active: schedule.active,
    }
}

fn map_style_inspection(value: DependencyStyleInspection) -> StyleInspectionDataRecord {
    StyleInspectionDataRecord {
        summary: StyleDataRecord {
            id: value.summary.id,
            version: value.summary.version,
            source: match value.summary.source {
                DependencyStyleSourceKind::BuiltIn => StyleDataSourceKind::BuiltIn,
                DependencyStyleSourceKind::User => StyleDataSourceKind::User,
                DependencyStyleSourceKind::Project => StyleDataSourceKind::Project,
                DependencyStyleSourceKind::Plugin => StyleDataSourceKind::Plugin,
                DependencyStyleSourceKind::Inline => StyleDataSourceKind::Inline,
            },
            availability: match value.summary.availability {
                DependencyStyleAvailability::Available => StyleDataAvailability::Available,
                DependencyStyleAvailability::Disabled => StyleDataAvailability::Disabled,
                DependencyStyleAvailability::Invalid => StyleDataAvailability::Invalid,
                DependencyStyleAvailability::Incompatible => StyleDataAvailability::Incompatible,
                DependencyStyleAvailability::Conflict => StyleDataAvailability::Conflict,
            },
            style_content_hash: value.summary.style_content_hash,
            compiled_cache_key: value.summary.compiled_cache_key,
            required_capabilities: value.summary.required_capabilities,
        },
        source_locator: value.source_locator,
        manifest: value.manifest,
        compiled: value.compiled,
        diagnostics: value
            .diagnostics
            .into_iter()
            .map(|diagnostic| StyleDiagnosticDataRecord {
                code: diagnostic.code,
                path: diagnostic.path,
                message: diagnostic.message,
                help: diagnostic.help,
            })
            .collect(),
    }
}

fn map_attachment(value: DependencyAttachment) -> AttachmentDataRecord {
    AttachmentDataRecord {
        identity: value.identity,
        name: value.name,
        uri: value.uri,
        mime_type: value.mime_type,
        kind: match value.kind {
            DependencyAttachmentKind::Image => AttachmentDataKind::Image,
            DependencyAttachmentKind::Audio => AttachmentDataKind::Audio,
            DependencyAttachmentKind::Blob => AttachmentDataKind::Blob,
        },
        data_base64: value.data_base64,
        byte_size: value.byte_size,
    }
}

fn map_event(value: DependencyTurnEvent) -> TurnDataEvent {
    match value {
        DependencyTurnEvent::Started => TurnDataEvent::Started,
        DependencyTurnEvent::Text(value) => TurnDataEvent::Text(value),
        DependencyTurnEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => TurnDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        DependencyTurnEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => TurnDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        DependencyTurnEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
            ..
        } => TurnDataEvent::Completed {
            reason,
            input_tokens,
            output_tokens,
        },
        DependencyTurnEvent::Cancelled => TurnDataEvent::Cancelled,
        DependencyTurnEvent::Failed {
            code,
            message,
            retryable,
        } => TurnDataEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "map_err consumes the lower-layer error at this explicit boundary"
)]
fn map_error(error: TuiDependencyError) -> TuiDataError {
    TuiDataError::Dependency(error.to_string())
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TuiDataError {
    #[error("TUI runtime dependency failed: {0}")]
    Dependency(String),
}

#[cfg(test)]
mod attachment_tests {
    use agentmod_tui_dependency::{DependencyAttachment, DependencyAttachmentKind};

    use super::{AttachmentDataKind, map_attachment};

    #[test]
    fn dependency_attachment_is_explicitly_mapped_to_data_owned_types() {
        let mapped = map_attachment(DependencyAttachment {
            identity: String::from("/workspace/pixel.png"),
            name: String::from("pixel.png"),
            uri: String::from("file:///workspace/pixel.png"),
            mime_type: String::from("image/png"),
            kind: DependencyAttachmentKind::Image,
            data_base64: String::from("iVBORw=="),
            byte_size: 4,
        });
        assert_eq!(mapped.kind, AttachmentDataKind::Image);
        assert_eq!(mapped.name, "pixel.png");
        assert_eq!(mapped.data_base64, "iVBORw==");
        assert_eq!(mapped.byte_size, 4);
    }
}
