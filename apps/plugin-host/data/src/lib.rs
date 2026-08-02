//! Plugin datasets and dependency normalization.
#![allow(
    missing_docs,
    reason = "layer-local mapping records mirror individually documented protocol and dependency fields"
)]

use std::collections::BTreeSet;

use agentmod_plugin_host_dependency as dependency;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Data-owned plugin class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginClassData {
    Blocking,
    Observer,
    Tool,
    GraphNode,
    Memory,
    Compaction,
    ContextTransform,
    Extension,
}

/// Data-owned graph node executor declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecutorData {
    pub executor_id: String,
    pub version: String,
    pub node_kind: String,
    pub runtime_api: String,
    pub required_capabilities: BTreeSet<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub idempotent: bool,
    pub external_effect: bool,
    pub read_authority: BTreeSet<String>,
    pub state_scope: String,
}

/// Data-owned memory declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryDeclarationData {
    pub scopes: BTreeSet<String>,
    pub capabilities: BTreeSet<String>,
    pub bounded_bytes: u64,
}

/// Data-owned compaction declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionDeclarationData {
    pub strategy_id: String,
    pub idempotent: bool,
    pub bounded_bytes: u64,
}

/// Data-owned context transform boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextTransformBoundaryData {
    BeforeMemoryRetrieval,
    AfterMemoryRetrieval,
    BeforeCompaction,
    AfterCompaction,
    BeforeProviderProjection,
    BeforeTurnCompletion,
}

/// Data-owned context transform declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextTransformData {
    pub transform_id: String,
    pub boundary: ContextTransformBoundaryData,
    pub stage: u16,
    pub priority: i32,
    pub before: BTreeSet<String>,
    pub after: BTreeSet<String>,
}

/// Data-owned observer delivery semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserverDeliveryData {
    BestEffort,
    AtMostOnce,
    AtLeastOnce {
        max_attempts: u8,
        retry_backoff_ms: u64,
    },
}

/// Data-owned manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestData {
    pub schema_version: u16,
    pub id: String,
    pub version: String,
    pub runtime_api: String,
    pub category: String,
    pub scope: String,
    pub class: PluginClassData,
    pub program: String,
    pub arguments: Vec<String>,
    pub required_capabilities: BTreeSet<String>,
    pub provided_capabilities: BTreeSet<String>,
    pub subscribed_events: BTreeSet<String>,
    pub read_authority: BTreeSet<String>,
    pub proposed_write_authority: BTreeSet<String>,
    pub tool_permissions: BTreeSet<String>,
    pub network_permissions: BTreeSet<String>,
    pub after: BTreeSet<String>,
    pub before: BTreeSet<String>,
    pub stage: u16,
    pub priority: i32,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub state_migration_version: u32,
    pub schema_id: String,
    pub schema_version_number: u32,
    pub schema_required: bool,
    pub schema_json: String,
    pub node_executors: Vec<NodeExecutorData>,
    pub memory: Option<MemoryDeclarationData>,
    pub compaction: Option<CompactionDeclarationData>,
    pub context_transforms: Vec<ContextTransformData>,
    pub observer_delivery: ObserverDeliveryData,
}

/// Data-owned authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationData {
    pub owner_id: String,
    pub session_id: String,
    pub call_id: String,
    pub normalized_digest: String,
    pub grant: String,
    pub cancellation_id: String,
}

/// Invocation.
#[derive(Clone, Debug)]
pub struct InvocationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub operation: String,
    pub kind: String,
    pub payload: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}

/// Graph node execution.
#[derive(Clone, Debug)]
pub struct NodeExecutionData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub executor_id: String,
    pub node_id: String,
    pub node_kind: String,
    pub input: Value,
    pub variables: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}

/// Memory operation.
#[derive(Clone, Debug)]
pub struct MemoryOperationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub scope: String,
    pub query: String,
    pub limit: usize,
    pub entries: Vec<MemoryItemData>,
    pub authorization: AuthorizationData,
}

/// Memory item.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryItemData {
    pub reference: String,
    pub content: String,
    pub score: Option<f64>,
    pub created_at_ms: i64,
}

/// Compaction proposal.
#[derive(Clone, Debug)]
pub struct CompactionData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub source_range_start: u64,
    pub source_range_end: u64,
    pub source_range_hash: String,
    pub current_entries: Value,
    pub proposal: Value,
    pub authorization: AuthorizationData,
}

/// Context transform.
#[derive(Clone, Debug)]
pub struct ContextTransformOperationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub boundary: ContextTransformBoundaryData,
    pub payload: Value,
    pub authorization: AuthorizationData,
}

/// Observation.
#[derive(Clone, Debug)]
pub struct ObservationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub event_range_start: u64,
    pub event_range_end: u64,
    pub authorization: AuthorizationData,
}

/// State change.
#[derive(Clone, Debug)]
pub struct StateChangeData {
    pub plugin_id: String,
    pub reason: Option<String>,
    pub authorization: AuthorizationData,
}

/// Decision.
#[derive(Clone, Debug, PartialEq)]
pub enum DecisionData {
    Continue(Value),
    Replace(Value),
    Reject(String),
    ToolResult(Value),
    NodeResult(Value),
}

/// Memory result.
#[derive(Clone, Debug, PartialEq)]
pub enum MemoryResultData {
    Describe {
        scopes: BTreeSet<String>,
        capabilities: BTreeSet<String>,
        bounded_bytes: u64,
    },
    Retrieve {
        items: Vec<MemoryItemData>,
    },
    Commit {
        retained: bool,
        references: Vec<String>,
    },
    Health {
        healthy: bool,
        item_count: u64,
        retained_bytes: u64,
    },
}

/// Audit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditData {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}

/// Load result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadData {
    pub plugin_id: String,
    pub state_version: u32,
    pub attempts: u8,
}

/// Observation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationResultData {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
}

/// Durable delivery record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryRecordData {
    pub delivery_id: String,
    pub plugin_id: String,
    pub handler: String,
    pub event_type: String,
    pub event_range_start: u64,
    pub event_range_end: u64,
    pub attempts: u8,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub next_retry_at_ms: i64,
    pub terminal: Option<String>,
}

/// Health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthData {
    pub loaded: usize,
    pub running: usize,
    pub observer_dropped: u64,
    pub pending_deliveries: usize,
}

/// Data contract.
#[async_trait]
pub trait PluginDataPort: Send + Sync {
    async fn negotiate(
        &self,
        protocol: u16,
        api: String,
        capabilities: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDataError>;
    async fn validate_set(
        &self,
        manifests: Vec<ManifestData>,
    ) -> Result<Vec<String>, PluginDataError>;
    async fn load(
        &self,
        manifest: ManifestData,
        configuration: Value,
        authorization: AuthorizationData,
    ) -> Result<LoadData, PluginDataError>;
    async fn invoke(&self, request: InvocationData) -> Result<(DecisionData, u8), PluginDataError>;
    async fn execute_node(
        &self,
        request: NodeExecutionData,
    ) -> Result<(Value, u8), PluginDataError>;
    async fn memory(
        &self,
        operation: String,
        request: MemoryOperationData,
    ) -> Result<(MemoryResultData, u8), PluginDataError>;
    async fn compaction_propose(
        &self,
        request: CompactionData,
    ) -> Result<(Value, u64, u8), PluginDataError>;
    async fn context_transform(
        &self,
        request: ContextTransformOperationData,
    ) -> Result<(Value, u8), PluginDataError>;
    async fn observe(
        &self,
        request: ObservationData,
    ) -> Result<ObservationResultData, PluginDataError>;
    async fn cancel(&self, invocation_id: String) -> Result<(), PluginDataError>;
    async fn state_change(
        &self,
        request: StateChangeData,
        quarantine: bool,
    ) -> Result<AuditData, PluginDataError>;
    async fn reload(&self, request: StateChangeData) -> Result<AuditData, PluginDataError>;
    async fn unquarantine(&self, request: StateChangeData) -> Result<AuditData, PluginDataError>;
    async fn health(&self) -> HealthData;
    async fn audits(&self) -> Vec<AuditData>;
    async fn deliveries(&self) -> Vec<DeliveryRecordData>;
    async fn active_invocations(&self) -> usize;
    async fn pending_deliveries(&self) -> usize;
    async fn flush(&self) -> Result<(), PluginDataError>;
}

/// Data implementation.
#[derive(Clone)]
pub struct PluginData<D> {
    dependency: D,
}
impl<D> PluginData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: dependency::PluginDependencyPort> PluginDataPort for PluginData<D> {
    async fn negotiate(
        &self,
        p: u16,
        a: String,
        c: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDataError> {
        self.dependency.negotiate(p, a, c).await.map_err(map_error)
    }
    async fn validate_set(&self, m: Vec<ManifestData>) -> Result<Vec<String>, PluginDataError> {
        self.dependency
            .validate_set(m.into_iter().map(map_manifest).collect())
            .await
            .map_err(map_error)
    }
    async fn load(
        &self,
        m: ManifestData,
        c: Value,
        a: AuthorizationData,
    ) -> Result<LoadData, PluginDataError> {
        let value = self
            .dependency
            .load(dependency::DependencyLoadRequest {
                manifest: map_manifest(m),
                configuration: c,
                authorization: map_auth(a),
            })
            .await
            .map_err(map_error)?;
        Ok(LoadData {
            plugin_id: value.plugin_id,
            state_version: value.state_version,
            attempts: value.attempts,
        })
    }
    async fn invoke(&self, r: InvocationData) -> Result<(DecisionData, u8), PluginDataError> {
        let (d, a) = self
            .dependency
            .invoke(dependency::DependencyInvocationRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                operation: r.operation,
                kind: r.kind,
                payload: r.payload,
                readable_state: r.readable_state,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            match d {
                dependency::DependencyDecision::Continue(v) => DecisionData::Continue(v),
                dependency::DependencyDecision::Replace(v) => DecisionData::Replace(v),
                dependency::DependencyDecision::Reject(v) => DecisionData::Reject(v),
                dependency::DependencyDecision::ToolResult(v) => DecisionData::ToolResult(v),
                dependency::DependencyDecision::NodeResult(v) => DecisionData::NodeResult(v),
            },
            a,
        ))
    }
    async fn execute_node(&self, r: NodeExecutionData) -> Result<(Value, u8), PluginDataError> {
        self.dependency
            .execute_node(dependency::DependencyNodeExecutionRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                executor_id: r.executor_id,
                node_id: r.node_id,
                node_kind: r.node_kind,
                input: r.input,
                variables: r.variables,
                readable_state: r.readable_state,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)
    }
    async fn memory(
        &self,
        operation: String,
        r: MemoryOperationData,
    ) -> Result<(MemoryResultData, u8), PluginDataError> {
        let (result, attempts) = self
            .dependency
            .memory(
                operation,
                dependency::DependencyMemoryRequest {
                    plugin_id: r.plugin_id,
                    invocation_id: r.invocation_id,
                    scope: r.scope,
                    query: r.query,
                    limit: r.limit,
                    entries: r
                        .entries
                        .into_iter()
                        .map(|item| dependency::DependencyMemoryItem {
                            reference: item.reference,
                            content: item.content,
                            score: item.score,
                            created_at_ms: item.created_at_ms,
                        })
                        .collect(),
                    authorization: map_auth(r.authorization),
                },
            )
            .await
            .map_err(map_error)?;
        let result = match result {
            dependency::DependencyMemoryResult::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            } => MemoryResultData::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            },
            dependency::DependencyMemoryResult::Retrieve { items } => MemoryResultData::Retrieve {
                items: items
                    .into_iter()
                    .map(|item| MemoryItemData {
                        reference: item.reference,
                        content: item.content,
                        score: item.score,
                        created_at_ms: item.created_at_ms,
                    })
                    .collect(),
            },
            dependency::DependencyMemoryResult::Commit {
                retained,
                references,
            } => MemoryResultData::Commit {
                retained,
                references,
            },
            dependency::DependencyMemoryResult::Health {
                healthy,
                item_count,
                retained_bytes,
            } => MemoryResultData::Health {
                healthy,
                item_count,
                retained_bytes,
            },
        };
        Ok((result, attempts))
    }
    async fn compaction_propose(
        &self,
        r: CompactionData,
    ) -> Result<(Value, u64, u8), PluginDataError> {
        self.dependency
            .compaction_propose(dependency::DependencyCompactionRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                source_range_start: r.source_range_start,
                source_range_end: r.source_range_end,
                source_range_hash: r.source_range_hash,
                current_entries: r.current_entries,
                proposal: r.proposal,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)
    }
    async fn context_transform(
        &self,
        r: ContextTransformOperationData,
    ) -> Result<(Value, u8), PluginDataError> {
        self.dependency
            .context_transform(dependency::DependencyContextTransformRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                transform_id: r.transform_id,
                boundary: map_boundary(r.boundary),
                payload: r.payload,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)
    }
    async fn observe(&self, r: ObservationData) -> Result<ObservationResultData, PluginDataError> {
        let v = self
            .dependency
            .observe(dependency::DependencyObservationRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                event_type: r.event_type,
                event: r.event,
                event_range_start: r.event_range_start,
                event_range_end: r.event_range_end,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(ObservationResultData {
            accepted: v.accepted,
            queue_depth: v.queue_depth,
            dropped: v.dropped,
        })
    }
    async fn cancel(&self, i: String) -> Result<(), PluginDataError> {
        self.dependency.cancel(i).await.map_err(map_error)
    }
    async fn state_change(
        &self,
        r: StateChangeData,
        q: bool,
    ) -> Result<AuditData, PluginDataError> {
        let request = dependency::DependencyStateChangeRequest {
            plugin_id: r.plugin_id,
            reason: r.reason,
            authorization: map_auth(r.authorization),
        };
        let v = if q {
            self.dependency.quarantine(request).await
        } else {
            self.dependency.disable(request).await
        }
        .map_err(map_error)?;
        Ok(map_audit(v))
    }
    async fn reload(&self, r: StateChangeData) -> Result<AuditData, PluginDataError> {
        self.dependency
            .reload(dependency::DependencyStateChangeRequest {
                plugin_id: r.plugin_id,
                reason: r.reason,
                authorization: map_auth(r.authorization),
            })
            .await
            .map(map_audit)
            .map_err(map_error)
    }
    async fn unquarantine(&self, r: StateChangeData) -> Result<AuditData, PluginDataError> {
        self.dependency
            .unquarantine(dependency::DependencyStateChangeRequest {
                plugin_id: r.plugin_id,
                reason: r.reason,
                authorization: map_auth(r.authorization),
            })
            .await
            .map(map_audit)
            .map_err(map_error)
    }
    async fn health(&self) -> HealthData {
        let v = self.dependency.health().await;
        HealthData {
            loaded: v.loaded,
            running: v.running,
            observer_dropped: v.observer_dropped,
            pending_deliveries: v.pending_deliveries,
        }
    }
    async fn audits(&self) -> Vec<AuditData> {
        self.dependency
            .audits()
            .await
            .into_iter()
            .map(map_audit)
            .collect()
    }
    async fn deliveries(&self) -> Vec<DeliveryRecordData> {
        self.dependency
            .deliveries()
            .await
            .into_iter()
            .map(|record| DeliveryRecordData {
                delivery_id: record.delivery_id,
                plugin_id: record.plugin_id,
                handler: record.handler,
                event_type: record.event_type,
                event_range_start: record.event_range_start,
                event_range_end: record.event_range_end,
                attempts: record.attempts,
                max_attempts: record.max_attempts,
                retry_backoff_ms: record.retry_backoff_ms,
                next_retry_at_ms: record.next_retry_at_ms,
                terminal: record.terminal,
            })
            .collect()
    }
    async fn active_invocations(&self) -> usize {
        self.dependency.active_invocations().await
    }
    async fn pending_deliveries(&self) -> usize {
        self.dependency.pending_deliveries().await
    }
    async fn flush(&self) -> Result<(), PluginDataError> {
        self.dependency.flush().await.map_err(map_error)
    }
}

fn map_boundary(v: ContextTransformBoundaryData) -> dependency::DependencyContextTransformBoundary {
    match v {
        ContextTransformBoundaryData::BeforeMemoryRetrieval => {
            dependency::DependencyContextTransformBoundary::BeforeMemoryRetrieval
        }
        ContextTransformBoundaryData::AfterMemoryRetrieval => {
            dependency::DependencyContextTransformBoundary::AfterMemoryRetrieval
        }
        ContextTransformBoundaryData::BeforeCompaction => {
            dependency::DependencyContextTransformBoundary::BeforeCompaction
        }
        ContextTransformBoundaryData::AfterCompaction => {
            dependency::DependencyContextTransformBoundary::AfterCompaction
        }
        ContextTransformBoundaryData::BeforeProviderProjection => {
            dependency::DependencyContextTransformBoundary::BeforeProviderProjection
        }
        ContextTransformBoundaryData::BeforeTurnCompletion => {
            dependency::DependencyContextTransformBoundary::BeforeTurnCompletion
        }
    }
}

fn map_manifest(v: ManifestData) -> dependency::DependencyManifest {
    dependency::DependencyManifest {
        schema_version: v.schema_version,
        id: v.id,
        version: v.version,
        runtime_api: v.runtime_api,
        category: v.category,
        scope: v.scope,
        class: match v.class {
            PluginClassData::Blocking => dependency::DependencyPluginClass::Blocking,
            PluginClassData::Observer => dependency::DependencyPluginClass::Observer,
            PluginClassData::Tool => dependency::DependencyPluginClass::Tool,
            PluginClassData::GraphNode => dependency::DependencyPluginClass::GraphNode,
            PluginClassData::Memory => dependency::DependencyPluginClass::Memory,
            PluginClassData::Compaction => dependency::DependencyPluginClass::Compaction,
            PluginClassData::ContextTransform => {
                dependency::DependencyPluginClass::ContextTransform
            }
            PluginClassData::Extension => dependency::DependencyPluginClass::Extension,
        },
        entrypoint: dependency::DependencyEntrypoint {
            program: v.program,
            arguments: v.arguments,
        },
        required_capabilities: v.required_capabilities,
        provided_capabilities: v.provided_capabilities,
        subscribed_events: v.subscribed_events,
        read_authority: v.read_authority,
        proposed_write_authority: v.proposed_write_authority,
        tool_permissions: v.tool_permissions,
        network_permissions: v.network_permissions,
        after: v.after,
        before: v.before,
        stage: v.stage,
        priority: v.priority,
        timeout_ms: v.timeout_ms,
        failure_policy: v.failure_policy,
        max_attempts: v.max_attempts,
        retry_backoff_ms: v.retry_backoff_ms,
        state_migration_version: v.state_migration_version,
        configuration_schema: dependency::DependencyConfigurationSchema {
            id: v.schema_id,
            version: v.schema_version_number,
            required: v.schema_required,
            inline_json: v.schema_json,
        },
        node_executors: v
            .node_executors
            .into_iter()
            .map(|executor| dependency::DependencyNodeExecutor {
                executor_id: executor.executor_id,
                version: executor.version,
                node_kind: executor.node_kind,
                runtime_api: executor.runtime_api,
                required_capabilities: executor.required_capabilities,
                input_schema: executor.input_schema,
                output_schema: executor.output_schema,
                timeout_ms: executor.timeout_ms,
                failure_policy: executor.failure_policy,
                idempotent: executor.idempotent,
                external_effect: executor.external_effect,
                read_authority: executor.read_authority,
                state_scope: executor.state_scope,
            })
            .collect(),
        memory: v
            .memory
            .map(|memory| dependency::DependencyMemoryDeclaration {
                scopes: memory.scopes,
                capabilities: memory.capabilities,
                bounded_bytes: memory.bounded_bytes,
            }),
        compaction: v
            .compaction
            .map(|compaction| dependency::DependencyCompactionDeclaration {
                strategy_id: compaction.strategy_id,
                idempotent: compaction.idempotent,
                bounded_bytes: compaction.bounded_bytes,
            }),
        context_transforms: v
            .context_transforms
            .into_iter()
            .map(|transform| dependency::DependencyContextTransform {
                transform_id: transform.transform_id,
                boundary: map_boundary(transform.boundary),
                stage: transform.stage,
                priority: transform.priority,
                before: transform.before,
                after: transform.after,
            })
            .collect(),
        observer_delivery: match v.observer_delivery {
            ObserverDeliveryData::BestEffort => dependency::DependencyObserverDelivery::BestEffort,
            ObserverDeliveryData::AtMostOnce => dependency::DependencyObserverDelivery::AtMostOnce,
            ObserverDeliveryData::AtLeastOnce {
                max_attempts,
                retry_backoff_ms,
            } => dependency::DependencyObserverDelivery::AtLeastOnce {
                max_attempts,
                retry_backoff_ms,
            },
        },
    }
}
fn map_auth(v: AuthorizationData) -> dependency::DependencyAuthorization {
    dependency::DependencyAuthorization {
        owner_id: v.owner_id,
        session_id: v.session_id,
        call_id: v.call_id,
        normalized_digest: v.normalized_digest,
        grant: v.grant,
        cancellation_id: v.cancellation_id,
    }
}
fn map_audit(v: dependency::DependencyAudit) -> AuditData {
    AuditData {
        plugin_id: v.plugin_id,
        invocation_id: v.invocation_id,
        operation: v.operation,
        outcome: v.outcome,
        attempts: v.attempts,
    }
}
#[allow(clippy::needless_pass_by_value)]
fn map_error(v: dependency::PluginDependencyError) -> PluginDataError {
    match v {
        dependency::PluginDependencyError::Invalid
        | dependency::PluginDependencyError::Validation(_)
        | dependency::PluginDependencyError::Configuration => PluginDataError::Invalid,
        dependency::PluginDependencyError::Authorization
        | dependency::PluginDependencyError::Replay => PluginDataError::Authorization,
        dependency::PluginDependencyError::NotLoaded
        | dependency::PluginDependencyError::Inactive => PluginDataError::Unavailable,
        dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
        _ => PluginDataError::External,
    }
}
/// Data error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginDataError {
    #[error("invalid plugin data")]
    Invalid,
    #[error("plugin authorization denied")]
    Authorization,
    #[error("plugin unavailable")]
    Unavailable,
    #[error("plugin cancelled")]
    Cancelled,
    #[error("plugin dependency failed")]
    External,
}
