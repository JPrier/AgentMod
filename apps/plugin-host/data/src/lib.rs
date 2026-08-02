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
    Extension,
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
    pub context_transforms: Vec<ContextTransformData>,
    pub memory_providers: Vec<MemoryProviderData>,
    pub compactors: Vec<CompactorData>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationIdempotencyData {
    Idempotent,
    NonIdempotent,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationDeclarationData {
    pub handler: String,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: OperationIdempotencyData,
    pub tool_permissions: Vec<String>,
    pub network_permissions: Vec<String>,
    pub state_scope: String,
    pub external_effects: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryProviderData {
    pub provider_id: String,
    pub version: String,
    pub runtime_api: String,
    pub capabilities: Vec<String>,
    pub retrieve: OperationDeclarationData,
    pub write: Option<OperationDeclarationData>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactorData {
    pub compactor_id: String,
    pub version: String,
    pub runtime_api: String,
    pub handler: String,
    pub capabilities: Vec<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: OperationIdempotencyData,
    pub tool_permissions: Vec<String>,
    pub network_permissions: Vec<String>,
    pub state_scope: String,
    pub external_effects: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextTransformLifecycleData {
    BeforeModelRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextTransformData {
    pub transform_id: String,
    pub version: String,
    pub runtime_api: String,
    pub handler: String,
    pub lifecycle: ContextTransformLifecycleData,
    pub capabilities: BTreeSet<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotent: bool,
    pub tool_permissions: BTreeSet<String>,
    pub network_permissions: BTreeSet<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeExecutorData {
    pub executor_id: String,
    pub version: String,
    pub runtime_api: String,
    pub node_kind: String,
    pub handler: String,
    pub capabilities: BTreeSet<String>,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotent: bool,
    pub tool_permissions: BTreeSet<String>,
    pub network_permissions: BTreeSet<String>,
    pub state_scope: String,
    pub external_effects: bool,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationCancellationTargetData {
    pub session_id: String,
    pub run_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub operation_id: String,
    pub declaration_hash: String,
    pub request_hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationCancellationStatusData {
    Signalled,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationCancellationReceiptData {
    pub target: InvocationCancellationTargetData,
    pub reason_code: String,
    pub action_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
    pub status: InvocationCancellationStatusData,
    pub receipt_id: String,
    pub receipt_digest: String,
}

#[derive(Clone, Debug)]
pub struct CancelInvocationData {
    pub target: InvocationCancellationTargetData,
    pub reason_code: String,
    pub action_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: AuthorizationData,
}

/// Invocation.
#[derive(Clone, Debug)]
pub struct InvocationData {
    pub cancellation_target: Option<InvocationCancellationTargetData>,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub executor_id: Option<String>,
    pub executor_version: Option<String>,
    pub timeout_ms: Option<u64>,
    pub configuration_reference: Option<String>,
    pub operation: String,
    pub kind: String,
    pub payload: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}

#[derive(Clone, Debug)]
pub struct ContextTransformInvocationData {
    pub cancellation_target: InvocationCancellationTargetData,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub transform_version: String,
    pub timeout_ms: u64,
    pub configuration_reference: String,
    pub lifecycle: ContextTransformLifecycleData,
    pub handler: String,
    pub input: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextTransformProposalData {
    pub replacement: Value,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationBindingData {
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub operation_id: String,
    pub session_id: String,
    pub run_id: String,
    pub node_id: Option<String>,
    pub declaration_hash: String,
    pub configuration_reference: String,
    pub request_hash: String,
    pub idempotency_key: String,
    pub attempt: u8,
}
#[derive(Clone, Debug)]
pub struct MemoryRetrieveData {
    pub binding: OperationBindingData,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: OperationIdempotencyData,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRetrieveProposalData {
    pub binding: OperationBindingData,
    pub provider_id: String,
    pub provider_version: String,
    pub items: Value,
}
#[derive(Clone, Debug)]
pub struct MemoryWriteData {
    pub binding: OperationBindingData,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: OperationIdempotencyData,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryWriteReceiptData {
    pub binding: OperationBindingData,
    pub provider_id: String,
    pub provider_version: String,
    pub provider_record_id: String,
    pub value_hash: String,
    pub receipt: Value,
}
#[derive(Clone, Debug)]
pub struct CompactionData {
    pub binding: OperationBindingData,
    pub compactor_id: String,
    pub compactor_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: OperationIdempotencyData,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CompactionProposalData {
    pub binding: OperationBindingData,
    pub compactor_id: String,
    pub compactor_version: String,
    pub replacement: Value,
    pub replacement_hash: String,
    pub preserved_references: Value,
    pub preserved_artifacts: Value,
}
/// Observation.
#[derive(Clone, Debug)]
pub struct ObservationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub authorization: AuthorizationData,
}
/// State change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateChangeActionData {
    Disable,
    Enable,
    Quarantine,
    Unquarantine,
}

/// State change.
#[derive(Clone, Debug)]
pub struct StateChangeData {
    pub plugin_id: String,
    pub plugin_version: String,
    pub configuration_reference: String,
    pub action: StateChangeActionData,
    pub reason: Option<String>,
    pub authorization: AuthorizationData,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeStateScopeData {
    Invocation,
    ModelCall,
    Turn,
    Session,
    Project,
    User,
    Runtime,
}
#[derive(Clone, Debug)]
pub struct PersistNodeStateData {
    pub cancellation_target: InvocationCancellationTargetData,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub configuration_reference: String,
    pub state_scope: NodeStateScopeData,
    pub prior_generation: u64,
    pub prior_state_hash: Option<String>,
    pub state: Value,
    pub state_hash: String,
    pub action_digest: String,
    pub authorization_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: AuthorizationData,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStateReceiptData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub state_scope: NodeStateScopeData,
    pub prior_generation: u64,
    pub generation: u64,
    pub state_hash: String,
    pub action_digest: String,
    pub authorization_digest: String,
    pub idempotency_key: String,
    pub receipt_id: String,
    pub receipt_digest: String,
    pub replayed: bool,
}
#[derive(Clone, Debug)]
pub struct LoadNodeStateData {
    pub cancellation_target: InvocationCancellationTargetData,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub configuration_reference: String,
    pub state_scope: NodeStateScopeData,
    pub expected_generation: u64,
    pub expected_state_hash: String,
    pub action_digest: String,
    pub authorization_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: AuthorizationData,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStateReadReceiptData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub state_scope: NodeStateScopeData,
    pub generation: u64,
    pub state_hash: String,
    pub action_digest: String,
    pub authorization_digest: String,
    pub idempotency_key: String,
    pub receipt_id: String,
    pub receipt_digest: String,
    pub replayed: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct LoadedNodeStateData {
    pub state: Value,
    pub receipt: NodeStateReadReceiptData,
}
/// Decision.
#[derive(Clone, Debug, PartialEq)]
pub enum DecisionData {
    Continue(Value),
    Replace(Value),
    Reject(String),
    ToolResult(Value),
    NodeOutcome(NodeOutcomeData),
}
#[derive(Clone, Debug, PartialEq)]
pub struct NodeActionData {
    pub kind: String,
    pub payload: Value,
}
#[derive(Clone, Debug, PartialEq)]
pub struct NodeOutcomeData {
    pub output: Value,
    pub preserved_state: Value,
    pub proposed_actions: Vec<NodeActionData>,
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
    pub status: ObserverDeliveryStatusData,
    pub request_hash: String,
    pub receipt_id: String,
    pub receipt_digest: String,
    pub replayed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverDeliveryStatusData {
    Completed,
    Rejected,
    Failed,
    Ambiguous,
}
/// Health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthData {
    pub loaded: usize,
    pub running: usize,
    pub observer_pending: u64,
    pub observer_dropped: u64,
    pub state_flushed: bool,
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
    async fn invoke_context_transform(
        &self,
        request: ContextTransformInvocationData,
    ) -> Result<(ContextTransformProposalData, u8), PluginDataError>;
    async fn invoke_memory_retrieve(
        &self,
        request: MemoryRetrieveData,
    ) -> Result<(MemoryRetrieveProposalData, u8), PluginDataError>;
    async fn invoke_memory_write(
        &self,
        request: MemoryWriteData,
    ) -> Result<(MemoryWriteReceiptData, u8), PluginDataError>;
    async fn invoke_compaction(
        &self,
        request: CompactionData,
    ) -> Result<(CompactionProposalData, u8), PluginDataError>;
    async fn observe(
        &self,
        request: ObservationData,
    ) -> Result<ObservationResultData, PluginDataError>;
    async fn persist_node_state(
        &self,
        request: PersistNodeStateData,
    ) -> Result<NodeStateReceiptData, PluginDataError>;
    async fn load_node_state(
        &self,
        request: LoadNodeStateData,
    ) -> Result<LoadedNodeStateData, PluginDataError>;
    async fn cancel_invocation(
        &self,
        request: CancelInvocationData,
    ) -> Result<InvocationCancellationReceiptData, PluginDataError>;
    async fn state_change(&self, request: StateChangeData) -> Result<AuditData, PluginDataError>;
    async fn health(&self) -> HealthData;
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
                cancellation_target: r
                    .cancellation_target
                    .map(map_cancellation_target)
                    .transpose()?,
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                executor_id: r.executor_id,
                executor_version: r.executor_version,
                timeout_ms: r.timeout_ms,
                configuration_reference: r
                    .configuration_reference
                    .as_deref()
                    .map(parse_hash)
                    .transpose()?,
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
                dependency::DependencyDecision::NodeOutcome(v) => {
                    DecisionData::NodeOutcome(NodeOutcomeData {
                        output: v.output,
                        preserved_state: v.preserved_state,
                        proposed_actions: v
                            .proposed_actions
                            .into_iter()
                            .map(|action| NodeActionData {
                                kind: action.kind,
                                payload: action.payload,
                            })
                            .collect(),
                    })
                }
            },
            a,
        ))
    }

    async fn invoke_context_transform(
        &self,
        request: ContextTransformInvocationData,
    ) -> Result<(ContextTransformProposalData, u8), PluginDataError> {
        let (proposal, attempts) = self
            .dependency
            .invoke_context_transform(dependency::DependencyContextTransformRequest {
                cancellation_target: map_cancellation_target(request.cancellation_target)?,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                transform_id: request.transform_id,
                transform_version: request.transform_version,
                timeout_ms: request.timeout_ms,
                configuration_reference: parse_hash(&request.configuration_reference)?,
                lifecycle: match request.lifecycle {
                    ContextTransformLifecycleData::BeforeModelRequest => {
                        dependency::DependencyContextTransformLifecycle::BeforeModelRequest
                    }
                },
                handler: request.handler,
                input: request.input,
                readable_state: request.readable_state,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            ContextTransformProposalData {
                replacement: proposal.replacement,
            },
            attempts,
        ))
    }
    async fn invoke_memory_retrieve(
        &self,
        request: MemoryRetrieveData,
    ) -> Result<(MemoryRetrieveProposalData, u8), PluginDataError> {
        let (proposal, attempts) = self
            .dependency
            .invoke_memory_retrieve(dependency::DependencyMemoryRetrieveRequest {
                binding: map_binding(request.binding)?,
                provider_id: request.provider_id,
                provider_version: request.provider_version,
                handler: request.handler,
                timeout_ms: request.timeout_ms,
                idempotency: map_idempotency(request.idempotency),
                request: request.request,
                readable_state: request.readable_state,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            MemoryRetrieveProposalData {
                binding: unmap_binding(proposal.binding),
                provider_id: proposal.provider_id,
                provider_version: proposal.provider_version,
                items: proposal.items,
            },
            attempts,
        ))
    }
    async fn invoke_memory_write(
        &self,
        request: MemoryWriteData,
    ) -> Result<(MemoryWriteReceiptData, u8), PluginDataError> {
        let (receipt, attempts) = self
            .dependency
            .invoke_memory_write(dependency::DependencyMemoryWriteRequest {
                binding: map_binding(request.binding)?,
                provider_id: request.provider_id,
                provider_version: request.provider_version,
                handler: request.handler,
                timeout_ms: request.timeout_ms,
                idempotency: map_idempotency(request.idempotency),
                request: request.request,
                readable_state: request.readable_state,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            MemoryWriteReceiptData {
                binding: unmap_binding(receipt.binding),
                provider_id: receipt.provider_id,
                provider_version: receipt.provider_version,
                provider_record_id: receipt.provider_record_id,
                value_hash: receipt.value_hash.to_hex(),
                receipt: receipt.receipt,
            },
            attempts,
        ))
    }
    async fn invoke_compaction(
        &self,
        request: CompactionData,
    ) -> Result<(CompactionProposalData, u8), PluginDataError> {
        let (proposal, attempts) = self
            .dependency
            .invoke_compaction(dependency::DependencyCompactionRequest {
                binding: map_binding(request.binding)?,
                compactor_id: request.compactor_id,
                compactor_version: request.compactor_version,
                handler: request.handler,
                timeout_ms: request.timeout_ms,
                idempotency: map_idempotency(request.idempotency),
                request: request.request,
                readable_state: request.readable_state,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            CompactionProposalData {
                binding: unmap_binding(proposal.binding),
                compactor_id: proposal.compactor_id,
                compactor_version: proposal.compactor_version,
                replacement: proposal.replacement,
                replacement_hash: proposal.replacement_hash.to_hex(),
                preserved_references: proposal.preserved_references,
                preserved_artifacts: proposal.preserved_artifacts,
            },
            attempts,
        ))
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
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(ObservationResultData {
            accepted: v.accepted,
            queue_depth: v.queue_depth,
            dropped: v.dropped,
            status: match v.status {
                dependency::DependencyObserverDeliveryStatus::Completed => {
                    ObserverDeliveryStatusData::Completed
                }
                dependency::DependencyObserverDeliveryStatus::Rejected => {
                    ObserverDeliveryStatusData::Rejected
                }
                dependency::DependencyObserverDeliveryStatus::Failed => {
                    ObserverDeliveryStatusData::Failed
                }
                dependency::DependencyObserverDeliveryStatus::Ambiguous => {
                    ObserverDeliveryStatusData::Ambiguous
                }
            },
            request_hash: v.request_hash.to_hex(),
            receipt_id: v.receipt_id,
            receipt_digest: v.receipt_digest.to_hex(),
            replayed: v.replayed,
        })
    }
    async fn persist_node_state(
        &self,
        r: PersistNodeStateData,
    ) -> Result<NodeStateReceiptData, PluginDataError> {
        let receipt = self
            .dependency
            .persist_node_state(dependency::DependencyPersistNodeStateRequest {
                cancellation_target: map_cancellation_target(r.cancellation_target)?,
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                invocation_digest: parse_hash(&r.invocation_digest)?,
                executor_id: r.executor_id,
                executor_version: r.executor_version,
                executor_declaration_hash: parse_hash(&r.executor_declaration_hash)?,
                configuration_reference: parse_hash(&r.configuration_reference)?,
                state_scope: map_node_state_scope(r.state_scope),
                prior_generation: r.prior_generation,
                prior_state_hash: r.prior_state_hash.as_deref().map(parse_hash).transpose()?,
                state: r.state,
                state_hash: parse_hash(&r.state_hash)?,
                action_digest: parse_hash(&r.action_digest)?,
                authorization_digest: parse_hash(&r.authorization_digest)?,
                nonce: r.nonce,
                idempotency_key: r.idempotency_key,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(NodeStateReceiptData {
            plugin_id: receipt.plugin_id,
            invocation_id: receipt.invocation_id,
            invocation_digest: receipt.invocation_digest.to_hex(),
            executor_id: receipt.executor_id,
            executor_version: receipt.executor_version,
            executor_declaration_hash: receipt.executor_declaration_hash.to_hex(),
            state_scope: unmap_node_state_scope(receipt.state_scope),
            prior_generation: receipt.prior_generation,
            generation: receipt.generation,
            state_hash: receipt.state_hash.to_hex(),
            action_digest: receipt.action_digest.to_hex(),
            authorization_digest: receipt.authorization_digest.to_hex(),
            idempotency_key: receipt.idempotency_key,
            receipt_id: receipt.receipt_id,
            receipt_digest: receipt.receipt_digest.to_hex(),
            replayed: receipt.replayed,
        })
    }
    async fn load_node_state(
        &self,
        r: LoadNodeStateData,
    ) -> Result<LoadedNodeStateData, PluginDataError> {
        let loaded = self
            .dependency
            .load_node_state(dependency::DependencyLoadNodeStateRequest {
                cancellation_target: map_cancellation_target(r.cancellation_target)?,
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                invocation_digest: parse_hash(&r.invocation_digest)?,
                executor_id: r.executor_id,
                executor_version: r.executor_version,
                executor_declaration_hash: parse_hash(&r.executor_declaration_hash)?,
                configuration_reference: parse_hash(&r.configuration_reference)?,
                state_scope: map_node_state_scope(r.state_scope),
                expected_generation: r.expected_generation,
                expected_state_hash: parse_hash(&r.expected_state_hash)?,
                action_digest: parse_hash(&r.action_digest)?,
                authorization_digest: parse_hash(&r.authorization_digest)?,
                nonce: r.nonce,
                idempotency_key: r.idempotency_key,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(LoadedNodeStateData {
            state: loaded.state,
            receipt: NodeStateReadReceiptData {
                plugin_id: loaded.receipt.plugin_id,
                invocation_id: loaded.receipt.invocation_id,
                invocation_digest: loaded.receipt.invocation_digest.to_hex(),
                executor_id: loaded.receipt.executor_id,
                executor_version: loaded.receipt.executor_version,
                executor_declaration_hash: loaded.receipt.executor_declaration_hash.to_hex(),
                state_scope: unmap_node_state_scope(loaded.receipt.state_scope),
                generation: loaded.receipt.generation,
                state_hash: loaded.receipt.state_hash.to_hex(),
                action_digest: loaded.receipt.action_digest.to_hex(),
                authorization_digest: loaded.receipt.authorization_digest.to_hex(),
                idempotency_key: loaded.receipt.idempotency_key,
                receipt_id: loaded.receipt.receipt_id,
                receipt_digest: loaded.receipt.receipt_digest.to_hex(),
                replayed: loaded.receipt.replayed,
            },
        })
    }
    async fn cancel_invocation(
        &self,
        request: CancelInvocationData,
    ) -> Result<InvocationCancellationReceiptData, PluginDataError> {
        let receipt = self
            .dependency
            .cancel_invocation(dependency::DependencyCancelInvocationRequest {
                target: map_cancellation_target(request.target)?,
                reason_code: request.reason_code,
                action_digest: parse_hash(&request.action_digest)?,
                nonce: request.nonce,
                idempotency_key: request.idempotency_key,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(InvocationCancellationReceiptData {
            target: unmap_cancellation_target(receipt.target),
            reason_code: receipt.reason_code,
            action_digest: receipt.action_digest.to_hex(),
            nonce: receipt.nonce,
            idempotency_key: receipt.idempotency_key,
            cancellation_id: receipt.cancellation_id,
            status: match receipt.status {
                dependency::DependencyInvocationCancellationStatus::Signalled => {
                    InvocationCancellationStatusData::Signalled
                }
                dependency::DependencyInvocationCancellationStatus::AlreadyTerminal => {
                    InvocationCancellationStatusData::AlreadyTerminal
                }
            },
            receipt_id: receipt.receipt_id,
            receipt_digest: receipt.receipt_digest.to_hex(),
        })
    }
    async fn state_change(&self, r: StateChangeData) -> Result<AuditData, PluginDataError> {
        let action = r.action;
        let request = dependency::DependencyStateChangeRequest {
            plugin_id: r.plugin_id,
            plugin_version: r.plugin_version,
            configuration_reference: r
                .configuration_reference
                .parse()
                .map_err(|_| PluginDataError::Invalid)?,
            reason: r.reason,
            authorization: map_auth(r.authorization),
        };
        let v = match action {
            StateChangeActionData::Disable => self.dependency.disable(request).await,
            StateChangeActionData::Enable => self.dependency.enable(request).await,
            StateChangeActionData::Quarantine => self.dependency.quarantine(request).await,
            StateChangeActionData::Unquarantine => self.dependency.unquarantine(request).await,
        }
        .map_err(map_error)?;
        Ok(map_audit(v))
    }
    async fn health(&self) -> HealthData {
        let v = self.dependency.health().await;
        HealthData {
            loaded: v.loaded,
            running: v.running,
            observer_pending: v.observer_pending,
            observer_dropped: v.observer_dropped,
            state_flushed: v.state_flushed,
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
            .map(map_node_executor)
            .collect(),
        context_transforms: v
            .context_transforms
            .into_iter()
            .map(map_context_transform)
            .collect(),
        memory_providers: v
            .memory_providers
            .into_iter()
            .map(map_memory_provider)
            .collect(),
        compactors: v.compactors.into_iter().map(map_compactor).collect(),
    }
}
fn map_node_executor(executor: NodeExecutorData) -> dependency::DependencyNodeExecutorDeclaration {
    dependency::DependencyNodeExecutorDeclaration {
        executor_id: executor.executor_id,
        version: executor.version,
        runtime_api: executor.runtime_api,
        node_kind: executor.node_kind,
        handler: executor.handler,
        capabilities: executor.capabilities,
        input_schema: executor.input_schema,
        output_schema: executor.output_schema,
        timeout_ms: executor.timeout_ms,
        failure_policy: executor.failure_policy,
        max_attempts: executor.max_attempts,
        retry_backoff_ms: executor.retry_backoff_ms,
        idempotency: if executor.idempotent {
            dependency::DependencyNodeExecutorIdempotency::Idempotent
        } else {
            dependency::DependencyNodeExecutorIdempotency::NonIdempotent
        },
        tool_permissions: executor.tool_permissions,
        network_permissions: executor.network_permissions,
        state_scope: executor.state_scope,
        external_effects: executor.external_effects,
    }
}
fn map_context_transform(
    transform: ContextTransformData,
) -> dependency::DependencyContextTransformDeclaration {
    dependency::DependencyContextTransformDeclaration {
        transform_id: transform.transform_id,
        version: transform.version,
        runtime_api: transform.runtime_api,
        handler: transform.handler,
        lifecycle: match transform.lifecycle {
            ContextTransformLifecycleData::BeforeModelRequest => {
                dependency::DependencyContextTransformLifecycle::BeforeModelRequest
            }
        },
        capabilities: transform.capabilities,
        input_schema: transform.input_schema,
        output_schema: transform.output_schema,
        timeout_ms: transform.timeout_ms,
        failure_policy: transform.failure_policy,
        max_attempts: transform.max_attempts,
        retry_backoff_ms: transform.retry_backoff_ms,
        idempotency: if transform.idempotent {
            dependency::DependencyContextTransformIdempotency::Idempotent
        } else {
            dependency::DependencyContextTransformIdempotency::NonIdempotent
        },
        tool_permissions: transform.tool_permissions,
        network_permissions: transform.network_permissions,
        state_scope: transform.state_scope,
        external_effects: transform.external_effects,
    }
}
fn map_memory_provider(
    provider: MemoryProviderData,
) -> dependency::DependencyMemoryProviderDeclaration {
    dependency::DependencyMemoryProviderDeclaration {
        provider_id: provider.provider_id,
        version: provider.version,
        runtime_api: provider.runtime_api,
        capabilities: provider.capabilities,
        retrieve: map_operation_declaration(provider.retrieve),
        write: provider.write.map(map_operation_declaration),
    }
}
fn map_compactor(compactor: CompactorData) -> dependency::DependencyCompactorDeclaration {
    dependency::DependencyCompactorDeclaration {
        compactor_id: compactor.compactor_id,
        version: compactor.version,
        runtime_api: compactor.runtime_api,
        handler: compactor.handler,
        capabilities: compactor.capabilities,
        input_schema: compactor.input_schema,
        output_schema: compactor.output_schema,
        timeout_ms: compactor.timeout_ms,
        failure_policy: compactor.failure_policy,
        max_attempts: compactor.max_attempts,
        retry_backoff_ms: compactor.retry_backoff_ms,
        idempotency: map_idempotency(compactor.idempotency),
        tool_permissions: compactor.tool_permissions,
        network_permissions: compactor.network_permissions,
        state_scope: compactor.state_scope,
        external_effects: compactor.external_effects,
    }
}
fn map_operation_declaration(
    operation: OperationDeclarationData,
) -> dependency::DependencyOperationDeclaration {
    dependency::DependencyOperationDeclaration {
        handler: operation.handler,
        input_schema: operation.input_schema,
        output_schema: operation.output_schema,
        timeout_ms: operation.timeout_ms,
        failure_policy: operation.failure_policy,
        max_attempts: operation.max_attempts,
        retry_backoff_ms: operation.retry_backoff_ms,
        idempotency: map_idempotency(operation.idempotency),
        tool_permissions: operation.tool_permissions,
        network_permissions: operation.network_permissions,
        state_scope: operation.state_scope,
        external_effects: operation.external_effects,
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
fn parse_hash(value: &str) -> Result<agentmod_primitives::ContentHash, PluginDataError> {
    value.parse().map_err(|_| PluginDataError::Invalid)
}
const fn map_idempotency(
    idempotency: OperationIdempotencyData,
) -> dependency::DependencyOperationIdempotency {
    match idempotency {
        OperationIdempotencyData::Idempotent => {
            dependency::DependencyOperationIdempotency::Idempotent
        }
        OperationIdempotencyData::NonIdempotent => {
            dependency::DependencyOperationIdempotency::NonIdempotent
        }
    }
}
fn map_binding(
    binding: OperationBindingData,
) -> Result<dependency::DependencyOperationBinding, PluginDataError> {
    Ok(dependency::DependencyOperationBinding {
        plugin_id: binding.plugin_id,
        plugin_version: binding.plugin_version,
        invocation_id: binding.invocation_id,
        operation_id: binding.operation_id,
        session_id: binding.session_id,
        run_id: binding.run_id,
        node_id: binding.node_id,
        declaration_hash: parse_hash(&binding.declaration_hash)?,
        configuration_reference: parse_hash(&binding.configuration_reference)?,
        request_hash: parse_hash(&binding.request_hash)?,
        idempotency_key: binding.idempotency_key,
        attempt: binding.attempt,
    })
}
fn unmap_binding(binding: dependency::DependencyOperationBinding) -> OperationBindingData {
    OperationBindingData {
        plugin_id: binding.plugin_id,
        plugin_version: binding.plugin_version,
        invocation_id: binding.invocation_id,
        operation_id: binding.operation_id,
        session_id: binding.session_id,
        run_id: binding.run_id,
        node_id: binding.node_id,
        declaration_hash: binding.declaration_hash.to_hex(),
        configuration_reference: binding.configuration_reference.to_hex(),
        request_hash: binding.request_hash.to_hex(),
        idempotency_key: binding.idempotency_key,
        attempt: binding.attempt,
    }
}
fn map_cancellation_target(
    target: InvocationCancellationTargetData,
) -> Result<dependency::DependencyInvocationCancellationTarget, PluginDataError> {
    Ok(dependency::DependencyInvocationCancellationTarget {
        session_id: target.session_id,
        run_id: target.run_id,
        plugin_id: target.plugin_id,
        plugin_version: target.plugin_version,
        invocation_id: target.invocation_id,
        invocation_digest: parse_hash(&target.invocation_digest)?,
        operation_id: target.operation_id,
        declaration_hash: parse_hash(&target.declaration_hash)?,
        request_hash: parse_hash(&target.request_hash)?,
    })
}
fn unmap_cancellation_target(
    target: dependency::DependencyInvocationCancellationTarget,
) -> InvocationCancellationTargetData {
    InvocationCancellationTargetData {
        session_id: target.session_id,
        run_id: target.run_id,
        plugin_id: target.plugin_id,
        plugin_version: target.plugin_version,
        invocation_id: target.invocation_id,
        invocation_digest: target.invocation_digest.to_hex(),
        operation_id: target.operation_id,
        declaration_hash: target.declaration_hash.to_hex(),
        request_hash: target.request_hash.to_hex(),
    }
}
const fn map_node_state_scope(
    scope: NodeStateScopeData,
) -> dependency::DependencyPluginNodeStateScope {
    match scope {
        NodeStateScopeData::Invocation => dependency::DependencyPluginNodeStateScope::Invocation,
        NodeStateScopeData::ModelCall => dependency::DependencyPluginNodeStateScope::ModelCall,
        NodeStateScopeData::Turn => dependency::DependencyPluginNodeStateScope::Turn,
        NodeStateScopeData::Session => dependency::DependencyPluginNodeStateScope::Session,
        NodeStateScopeData::Project => dependency::DependencyPluginNodeStateScope::Project,
        NodeStateScopeData::User => dependency::DependencyPluginNodeStateScope::User,
        NodeStateScopeData::Runtime => dependency::DependencyPluginNodeStateScope::Runtime,
    }
}
const fn unmap_node_state_scope(
    scope: dependency::DependencyPluginNodeStateScope,
) -> NodeStateScopeData {
    match scope {
        dependency::DependencyPluginNodeStateScope::Invocation => NodeStateScopeData::Invocation,
        dependency::DependencyPluginNodeStateScope::ModelCall => NodeStateScopeData::ModelCall,
        dependency::DependencyPluginNodeStateScope::Turn => NodeStateScopeData::Turn,
        dependency::DependencyPluginNodeStateScope::Session => NodeStateScopeData::Session,
        dependency::DependencyPluginNodeStateScope::Project => NodeStateScopeData::Project,
        dependency::DependencyPluginNodeStateScope::User => NodeStateScopeData::User,
        dependency::DependencyPluginNodeStateScope::Runtime => NodeStateScopeData::Runtime,
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
        | dependency::PluginDependencyError::Configuration
        | dependency::PluginDependencyError::ConfigurationDrift => PluginDataError::Invalid,
        dependency::PluginDependencyError::Authorization
        | dependency::PluginDependencyError::Replay => PluginDataError::Authorization,
        dependency::PluginDependencyError::NotLoaded
        | dependency::PluginDependencyError::Inactive => PluginDataError::Unavailable,
        dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
        dependency::PluginDependencyError::StaleStateGeneration => {
            PluginDataError::StaleStateGeneration
        }
        dependency::PluginDependencyError::StateConflict => PluginDataError::StateConflict,
        dependency::PluginDependencyError::CancellationTargetMismatch
        | dependency::PluginDependencyError::IdempotencyConflict => {
            PluginDataError::CancellationConflict
        }
        dependency::PluginDependencyError::Timeout
        | dependency::PluginDependencyError::Crashed
        | dependency::PluginDependencyError::Process
        | dependency::PluginDependencyError::MalformedResponse
        | dependency::PluginDependencyError::ResponseTooLarge
        | dependency::PluginDependencyError::External
        | dependency::PluginDependencyError::Ambiguous => PluginDataError::Ambiguous,
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
    #[error("plugin execution may have completed without a terminal receipt")]
    Ambiguous,
    #[error("plugin-node state generation is stale")]
    StaleStateGeneration,
    #[error("plugin-node state conflicts with an existing receipt")]
    StateConflict,
    #[error("plugin cancellation identity conflicts with active or persisted state")]
    CancellationConflict,
    #[error("plugin dependency failed")]
    External,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_binding_hashes_are_parsed_and_round_trip_exactly() {
        let binding = OperationBindingData {
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("invoke-1"),
            operation_id: String::from("retrieve-1"),
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: None,
            declaration_hash: "11".repeat(32),
            configuration_reference: "22".repeat(32),
            request_hash: "33".repeat(32),
            idempotency_key: String::from("memory-key"),
            attempt: 1,
        };
        let dependency = map_binding(binding.clone()).expect("valid binding");
        assert_eq!(unmap_binding(dependency), binding);

        let mut invalid = binding;
        invalid.request_hash = String::from("invalid");
        assert_eq!(map_binding(invalid), Err(PluginDataError::Invalid));
    }
}
