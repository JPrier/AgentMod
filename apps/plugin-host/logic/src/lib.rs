//! Plugin activation, invocation, observation, and state policy.
#![allow(
    missing_docs,
    reason = "layer-local mapping records have self-describing fields"
)]

use agentmod_plugin_host_data as data;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginClass {
    Blocking,
    Observer,
    Tool,
    Extension,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestCommand {
    pub schema_version: u16,
    pub id: String,
    pub version: String,
    pub runtime_api: String,
    pub category: String,
    pub scope: String,
    pub class: PluginClass,
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
    pub node_executors: Vec<NodeExecutorCommand>,
    pub context_transforms: Vec<ContextTransformDeclarationCommand>,
    pub memory_providers: Vec<MemoryProviderDeclarationCommand>,
    pub compactors: Vec<CompactorDeclarationCommand>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationIdempotency {
    Idempotent,
    NonIdempotent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationDeclarationCommand {
    pub handler: String,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub idempotency: OperationIdempotency,
    pub tool_permissions: Vec<String>,
    pub network_permissions: Vec<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryProviderDeclarationCommand {
    pub provider_id: String,
    pub version: String,
    pub runtime_api: String,
    pub capabilities: Vec<String>,
    pub retrieve: OperationDeclarationCommand,
    pub write: Option<OperationDeclarationCommand>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactorDeclarationCommand {
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
    pub idempotency: OperationIdempotency,
    pub tool_permissions: Vec<String>,
    pub network_permissions: Vec<String>,
    pub state_scope: String,
    pub external_effects: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextTransformLifecycle {
    BeforeModelRequest,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextTransformDeclarationCommand {
    pub transform_id: String,
    pub version: String,
    pub runtime_api: String,
    pub handler: String,
    pub lifecycle: ContextTransformLifecycle,
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
pub struct NodeExecutorCommand {
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authorization {
    pub owner_id: String,
    pub session_id: String,
    pub call_id: String,
    pub normalized_digest: String,
    pub grant: String,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationCancellationTarget {
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
pub enum InvocationCancellationStatus {
    Signalled,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationCancellationReceipt {
    pub target: InvocationCancellationTarget,
    pub reason_code: String,
    pub action_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
    pub status: InvocationCancellationStatus,
    pub receipt_id: String,
    pub receipt_digest: String,
}

#[derive(Clone, Debug)]
pub struct CancelInvocationCommand {
    pub target: InvocationCancellationTarget,
    pub reason_code: String,
    pub action_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: Authorization,
}
#[derive(Clone, Debug)]
pub struct InvocationCommand {
    pub cancellation_target: Option<InvocationCancellationTarget>,
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
    pub authorization: Authorization,
}

#[derive(Clone, Debug)]
pub struct ContextTransformCommand {
    pub cancellation_target: InvocationCancellationTarget,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub transform_version: String,
    pub timeout_ms: u64,
    pub configuration_reference: String,
    pub lifecycle: ContextTransformLifecycle,
    pub handler: String,
    pub input: Value,
    pub readable_state: Value,
    pub authorization: Authorization,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContextTransformProposal {
    pub replacement: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationBinding {
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
pub struct MemoryRetrieveCommand {
    pub binding: OperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: OperationIdempotency,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: Authorization,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRetrieveProposal {
    pub binding: OperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub items: Value,
}

#[derive(Clone, Debug)]
pub struct MemoryWriteCommand {
    pub binding: OperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: OperationIdempotency,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: Authorization,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MemoryWriteReceiptProposal {
    pub binding: OperationBinding,
    pub provider_id: String,
    pub provider_version: String,
    pub provider_record_id: String,
    pub value_hash: String,
    pub receipt: Value,
}

#[derive(Clone, Debug)]
pub struct CompactionCommand {
    pub binding: OperationBinding,
    pub compactor_id: String,
    pub compactor_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub idempotency: OperationIdempotency,
    pub request: Value,
    pub readable_state: Value,
    pub authorization: Authorization,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompactionProposal {
    pub binding: OperationBinding,
    pub compactor_id: String,
    pub compactor_version: String,
    pub replacement: Value,
    pub replacement_hash: String,
    pub preserved_references: Value,
    pub preserved_artifacts: Value,
}
#[derive(Clone, Debug)]
pub struct ObservationCommand {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub authorization: Authorization,
}
#[derive(Clone, Debug)]
pub struct StateChangeCommand {
    pub plugin_id: String,
    pub plugin_version: String,
    pub configuration_reference: String,
    pub action: StateChangeAction,
    pub reason: Option<String>,
    pub authorization: Authorization,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateChangeAction {
    Disable,
    Enable,
    Quarantine,
    Unquarantine,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeStateScope {
    Invocation,
    ModelCall,
    Turn,
    Session,
    Project,
    User,
    Runtime,
}
#[derive(Clone, Debug)]
pub struct PersistNodeStateCommand {
    pub cancellation_target: InvocationCancellationTarget,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub configuration_reference: String,
    pub state_scope: NodeStateScope,
    pub prior_generation: u64,
    pub prior_state_hash: Option<String>,
    pub state: Value,
    pub state_hash: String,
    pub action_digest: String,
    pub authorization_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: Authorization,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStateReceipt {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub state_scope: NodeStateScope,
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
pub struct LoadNodeStateCommand {
    pub cancellation_target: InvocationCancellationTarget,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub configuration_reference: String,
    pub state_scope: NodeStateScope,
    pub expected_generation: u64,
    pub expected_state_hash: String,
    pub action_digest: String,
    pub authorization_digest: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub authorization: Authorization,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStateReadReceipt {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: String,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: String,
    pub state_scope: NodeStateScope,
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
pub struct LoadedNodeState {
    pub state: Value,
    pub receipt: NodeStateReadReceipt,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    Continue(Value),
    Replace(Value),
    Reject(String),
    ToolResult(Value),
    NodeOutcome(NodeOutcome),
}
#[derive(Clone, Debug, PartialEq)]
pub struct NodeActionProposal {
    pub kind: String,
    pub payload: Value,
}
#[derive(Clone, Debug, PartialEq)]
pub struct NodeOutcome {
    pub output: Value,
    pub preserved_state: Value,
    pub proposed_actions: Vec<NodeActionProposal>,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Audit {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadResult {
    pub plugin_id: String,
    pub state_version: u32,
    pub audit: Audit,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationResult {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
    pub status: ObserverDeliveryStatus,
    pub request_hash: String,
    pub receipt_id: String,
    pub receipt_digest: String,
    pub replayed: bool,
    pub audit: Audit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObserverDeliveryStatus {
    Completed,
    Rejected,
    Failed,
    Ambiguous,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Health {
    pub loaded: usize,
    pub running: usize,
    pub observer_pending: u64,
    pub observer_dropped: u64,
    pub state_flushed: bool,
}

#[async_trait]
pub trait PluginLogicPort: Send + Sync {
    async fn negotiate(
        &self,
        p: u16,
        a: String,
        c: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginLogicError>;
    async fn validate_set(&self, m: Vec<ManifestCommand>) -> Result<Vec<String>, PluginLogicError>;
    async fn load(
        &self,
        m: ManifestCommand,
        c: Value,
        a: Authorization,
    ) -> Result<LoadResult, PluginLogicError>;
    async fn invoke(&self, r: InvocationCommand) -> Result<(Decision, Audit), PluginLogicError>;
    async fn invoke_context_transform(
        &self,
        command: ContextTransformCommand,
    ) -> Result<(ContextTransformProposal, Audit), PluginLogicError>;
    async fn invoke_memory_retrieve(
        &self,
        command: MemoryRetrieveCommand,
    ) -> Result<(MemoryRetrieveProposal, Audit), PluginLogicError>;
    async fn invoke_memory_write(
        &self,
        command: MemoryWriteCommand,
    ) -> Result<(MemoryWriteReceiptProposal, Audit), PluginLogicError>;
    async fn invoke_compaction(
        &self,
        command: CompactionCommand,
    ) -> Result<(CompactionProposal, Audit), PluginLogicError>;
    async fn observe(&self, r: ObservationCommand) -> Result<ObservationResult, PluginLogicError>;
    async fn persist_node_state(
        &self,
        request: PersistNodeStateCommand,
    ) -> Result<NodeStateReceipt, PluginLogicError>;
    async fn load_node_state(
        &self,
        request: LoadNodeStateCommand,
    ) -> Result<LoadedNodeState, PluginLogicError>;
    async fn cancel_invocation(
        &self,
        command: CancelInvocationCommand,
    ) -> Result<InvocationCancellationReceipt, PluginLogicError>;
    async fn state_change(&self, r: StateChangeCommand) -> Result<Audit, PluginLogicError>;
    async fn health(&self) -> Health;
}
#[derive(Clone)]
pub struct PluginLogic<D> {
    data: D,
}
impl<D> PluginLogic<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}
#[async_trait]
impl<D: data::PluginDataPort> PluginLogicPort for PluginLogic<D> {
    async fn negotiate(
        &self,
        p: u16,
        a: String,
        c: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginLogicError> {
        self.data.negotiate(p, a, c).await.map_err(map_error)
    }
    async fn validate_set(&self, m: Vec<ManifestCommand>) -> Result<Vec<String>, PluginLogicError> {
        if m.is_empty() {
            return Err(PluginLogicError::Invalid);
        }
        self.data
            .validate_set(m.into_iter().map(map_manifest).collect())
            .await
            .map_err(map_error)
    }
    async fn load(
        &self,
        m: ManifestCommand,
        c: Value,
        a: Authorization,
    ) -> Result<LoadResult, PluginLogicError> {
        validate_id(&m.id)?;
        validate_auth(&a)?;
        let v = self
            .data
            .load(map_manifest(m), c, map_auth(a))
            .await
            .map_err(map_error)?;
        let audit = Audit {
            plugin_id: v.plugin_id.clone(),
            invocation_id: None,
            operation: "load".into(),
            outcome: "loaded".into(),
            attempts: v.attempts,
        };
        Ok(LoadResult {
            plugin_id: v.plugin_id,
            state_version: v.state_version,
            audit,
        })
    }
    async fn invoke(&self, r: InvocationCommand) -> Result<(Decision, Audit), PluginLogicError> {
        validate_id(&r.plugin_id)?;
        validate_auth(&r.authorization)?;
        let invocation = r.invocation_id.clone();
        let plugin = r.plugin_id.clone();
        let operation = r.operation.clone();
        let (v, attempts) = self
            .data
            .invoke(data::InvocationData {
                cancellation_target: r.cancellation_target.map(map_cancellation_target),
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                executor_id: r.executor_id,
                executor_version: r.executor_version,
                timeout_ms: r.timeout_ms,
                configuration_reference: r.configuration_reference,
                operation: r.operation,
                kind: r.kind,
                payload: r.payload,
                readable_state: r.readable_state,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        let decision = match v {
            data::DecisionData::Continue(v) => Decision::Continue(v),
            data::DecisionData::Replace(v) => Decision::Replace(v),
            data::DecisionData::Reject(v) => Decision::Reject(v),
            data::DecisionData::ToolResult(v) => Decision::ToolResult(v),
            data::DecisionData::NodeOutcome(v) => Decision::NodeOutcome(NodeOutcome {
                output: v.output,
                preserved_state: v.preserved_state,
                proposed_actions: v
                    .proposed_actions
                    .into_iter()
                    .map(|action| NodeActionProposal {
                        kind: action.kind,
                        payload: action.payload,
                    })
                    .collect(),
            }),
        };
        Ok((
            decision,
            Audit {
                plugin_id: plugin,
                invocation_id: Some(invocation),
                operation,
                outcome: "completed".into(),
                attempts,
            },
        ))
    }

    async fn invoke_context_transform(
        &self,
        command: ContextTransformCommand,
    ) -> Result<(ContextTransformProposal, Audit), PluginLogicError> {
        validate_id(&command.plugin_id)?;
        validate_id(&command.transform_id)?;
        validate_id(&command.transform_version)?;
        validate_id(&command.handler)?;
        if command.invocation_id.is_empty()
            || command.invocation_id.len() > 256
            || !command
                .invocation_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b".:_-".contains(&byte))
        {
            return Err(PluginLogicError::Invalid);
        }
        validate_auth(&command.authorization)?;
        let plugin_id = command.plugin_id.clone();
        let invocation_id = command.invocation_id.clone();
        let (proposal, attempts) = self
            .data
            .invoke_context_transform(data::ContextTransformInvocationData {
                cancellation_target: map_cancellation_target(command.cancellation_target),
                plugin_id: command.plugin_id,
                invocation_id: command.invocation_id,
                transform_id: command.transform_id,
                transform_version: command.transform_version,
                timeout_ms: command.timeout_ms,
                configuration_reference: command.configuration_reference,
                lifecycle: match command.lifecycle {
                    ContextTransformLifecycle::BeforeModelRequest => {
                        data::ContextTransformLifecycleData::BeforeModelRequest
                    }
                },
                handler: command.handler,
                input: command.input,
                readable_state: command.readable_state,
                authorization: map_auth(command.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            ContextTransformProposal {
                replacement: proposal.replacement,
            },
            Audit {
                plugin_id,
                invocation_id: Some(invocation_id),
                operation: String::from("context_transform"),
                outcome: String::from("completed"),
                attempts,
            },
        ))
    }

    async fn invoke_memory_retrieve(
        &self,
        command: MemoryRetrieveCommand,
    ) -> Result<(MemoryRetrieveProposal, Audit), PluginLogicError> {
        validate_operation_command(
            &command.binding,
            &command.provider_id,
            &command.provider_version,
            &command.handler,
            command.idempotency,
            true,
            &command.authorization,
        )?;
        let plugin_id = command.binding.plugin_id.clone();
        let invocation_id = command.binding.invocation_id.clone();
        let (proposal, worker_attempts) = self
            .data
            .invoke_memory_retrieve(data::MemoryRetrieveData {
                binding: map_binding(command.binding),
                provider_id: command.provider_id,
                provider_version: command.provider_version,
                handler: command.handler,
                timeout_ms: command.timeout_ms,
                idempotency: map_idempotency(command.idempotency),
                request: command.request,
                readable_state: command.readable_state,
                authorization: map_auth(command.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            MemoryRetrieveProposal {
                binding: unmap_binding(proposal.binding),
                provider_id: proposal.provider_id,
                provider_version: proposal.provider_version,
                items: proposal.items,
            },
            operation_audit(plugin_id, invocation_id, "memory_retrieve", worker_attempts),
        ))
    }

    async fn invoke_memory_write(
        &self,
        command: MemoryWriteCommand,
    ) -> Result<(MemoryWriteReceiptProposal, Audit), PluginLogicError> {
        validate_operation_command(
            &command.binding,
            &command.provider_id,
            &command.provider_version,
            &command.handler,
            command.idempotency,
            false,
            &command.authorization,
        )?;
        if command.idempotency == OperationIdempotency::NonIdempotent
            && command.binding.attempt != 1
        {
            return Err(PluginLogicError::Invalid);
        }
        let plugin_id = command.binding.plugin_id.clone();
        let invocation_id = command.binding.invocation_id.clone();
        let (receipt, worker_attempts) = self
            .data
            .invoke_memory_write(data::MemoryWriteData {
                binding: map_binding(command.binding),
                provider_id: command.provider_id,
                provider_version: command.provider_version,
                handler: command.handler,
                timeout_ms: command.timeout_ms,
                idempotency: map_idempotency(command.idempotency),
                request: command.request,
                readable_state: command.readable_state,
                authorization: map_auth(command.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            MemoryWriteReceiptProposal {
                binding: unmap_binding(receipt.binding),
                provider_id: receipt.provider_id,
                provider_version: receipt.provider_version,
                provider_record_id: receipt.provider_record_id,
                value_hash: receipt.value_hash,
                receipt: receipt.receipt,
            },
            operation_audit(plugin_id, invocation_id, "memory_write", worker_attempts),
        ))
    }

    async fn invoke_compaction(
        &self,
        command: CompactionCommand,
    ) -> Result<(CompactionProposal, Audit), PluginLogicError> {
        validate_operation_command(
            &command.binding,
            &command.compactor_id,
            &command.compactor_version,
            &command.handler,
            command.idempotency,
            true,
            &command.authorization,
        )?;
        let plugin_id = command.binding.plugin_id.clone();
        let invocation_id = command.binding.invocation_id.clone();
        let (proposal, worker_attempts) = self
            .data
            .invoke_compaction(data::CompactionData {
                binding: map_binding(command.binding),
                compactor_id: command.compactor_id,
                compactor_version: command.compactor_version,
                handler: command.handler,
                timeout_ms: command.timeout_ms,
                idempotency: map_idempotency(command.idempotency),
                request: command.request,
                readable_state: command.readable_state,
                authorization: map_auth(command.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            CompactionProposal {
                binding: unmap_binding(proposal.binding),
                compactor_id: proposal.compactor_id,
                compactor_version: proposal.compactor_version,
                replacement: proposal.replacement,
                replacement_hash: proposal.replacement_hash,
                preserved_references: proposal.preserved_references,
                preserved_artifacts: proposal.preserved_artifacts,
            },
            operation_audit(plugin_id, invocation_id, "compaction", worker_attempts),
        ))
    }
    async fn observe(&self, r: ObservationCommand) -> Result<ObservationResult, PluginLogicError> {
        validate_id(&r.plugin_id)?;
        validate_auth(&r.authorization)?;
        let plugin = r.plugin_id.clone();
        let invocation = r.invocation_id.clone();
        let v = self
            .data
            .observe(data::ObservationData {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                event_type: r.event_type,
                event: r.event,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(ObservationResult {
            accepted: v.accepted,
            queue_depth: v.queue_depth,
            dropped: v.dropped,
            status: match v.status {
                data::ObserverDeliveryStatusData::Completed => ObserverDeliveryStatus::Completed,
                data::ObserverDeliveryStatusData::Rejected => ObserverDeliveryStatus::Rejected,
                data::ObserverDeliveryStatusData::Failed => ObserverDeliveryStatus::Failed,
                data::ObserverDeliveryStatusData::Ambiguous => ObserverDeliveryStatus::Ambiguous,
            },
            request_hash: v.request_hash,
            receipt_id: v.receipt_id,
            receipt_digest: v.receipt_digest,
            replayed: v.replayed,
            audit: Audit {
                plugin_id: plugin,
                invocation_id: Some(invocation),
                operation: "observe".into(),
                outcome: match v.status {
                    data::ObserverDeliveryStatusData::Completed => "completed",
                    data::ObserverDeliveryStatusData::Rejected => "rejected",
                    data::ObserverDeliveryStatusData::Failed => "failed",
                    data::ObserverDeliveryStatusData::Ambiguous => "ambiguous",
                }
                .into(),
                attempts: 1,
            },
        })
    }
    async fn persist_node_state(
        &self,
        request: PersistNodeStateCommand,
    ) -> Result<NodeStateReceipt, PluginLogicError> {
        validate_id(&request.plugin_id)?;
        validate_id(&request.executor_id)?;
        validate_id(&request.executor_version)?;
        validate_id(&request.nonce)?;
        validate_id(&request.idempotency_key)?;
        validate_auth(&request.authorization)?;
        if request.invocation_id.is_empty()
            || request.invocation_id.len() > 256
            || [
                request.invocation_digest.as_str(),
                request.executor_declaration_hash.as_str(),
                request.configuration_reference.as_str(),
                request.state_hash.as_str(),
                request.action_digest.as_str(),
                request.authorization_digest.as_str(),
            ]
            .iter()
            .any(|hash| hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || request.prior_state_hash.as_ref().is_some_and(|hash| {
                hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            || serde_json::to_vec(&request.state)
                .map_err(|_| PluginLogicError::Invalid)?
                .len()
                > 1024 * 1024
        {
            return Err(PluginLogicError::Invalid);
        }
        let receipt = self
            .data
            .persist_node_state(data::PersistNodeStateData {
                cancellation_target: map_cancellation_target(request.cancellation_target),
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                executor_declaration_hash: request.executor_declaration_hash,
                configuration_reference: request.configuration_reference,
                state_scope: map_node_state_scope(request.state_scope),
                prior_generation: request.prior_generation,
                prior_state_hash: request.prior_state_hash,
                state: request.state,
                state_hash: request.state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                nonce: request.nonce,
                idempotency_key: request.idempotency_key,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(NodeStateReceipt {
            plugin_id: receipt.plugin_id,
            invocation_id: receipt.invocation_id,
            invocation_digest: receipt.invocation_digest,
            executor_id: receipt.executor_id,
            executor_version: receipt.executor_version,
            executor_declaration_hash: receipt.executor_declaration_hash,
            state_scope: unmap_node_state_scope(receipt.state_scope),
            prior_generation: receipt.prior_generation,
            generation: receipt.generation,
            state_hash: receipt.state_hash,
            action_digest: receipt.action_digest,
            authorization_digest: receipt.authorization_digest,
            idempotency_key: receipt.idempotency_key,
            receipt_id: receipt.receipt_id,
            receipt_digest: receipt.receipt_digest,
            replayed: receipt.replayed,
        })
    }
    async fn load_node_state(
        &self,
        request: LoadNodeStateCommand,
    ) -> Result<LoadedNodeState, PluginLogicError> {
        validate_id(&request.plugin_id)?;
        validate_id(&request.executor_id)?;
        validate_id(&request.executor_version)?;
        validate_id(&request.nonce)?;
        validate_id(&request.idempotency_key)?;
        validate_auth(&request.authorization)?;
        if !matches!(
            request.state_scope,
            NodeStateScope::Invocation | NodeStateScope::Session
        ) || request.expected_generation == 0
            || request.invocation_id.is_empty()
            || request.invocation_id.len() > 256
            || [
                request.invocation_digest.as_str(),
                request.executor_declaration_hash.as_str(),
                request.configuration_reference.as_str(),
                request.expected_state_hash.as_str(),
                request.action_digest.as_str(),
                request.authorization_digest.as_str(),
            ]
            .iter()
            .any(|hash| hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(PluginLogicError::Invalid);
        }
        let loaded = self
            .data
            .load_node_state(data::LoadNodeStateData {
                cancellation_target: map_cancellation_target(request.cancellation_target),
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                executor_declaration_hash: request.executor_declaration_hash,
                configuration_reference: request.configuration_reference,
                state_scope: map_node_state_scope(request.state_scope),
                expected_generation: request.expected_generation,
                expected_state_hash: request.expected_state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                nonce: request.nonce,
                idempotency_key: request.idempotency_key,
                authorization: map_auth(request.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(LoadedNodeState {
            state: loaded.state,
            receipt: NodeStateReadReceipt {
                plugin_id: loaded.receipt.plugin_id,
                invocation_id: loaded.receipt.invocation_id,
                invocation_digest: loaded.receipt.invocation_digest,
                executor_id: loaded.receipt.executor_id,
                executor_version: loaded.receipt.executor_version,
                executor_declaration_hash: loaded.receipt.executor_declaration_hash,
                state_scope: unmap_node_state_scope(loaded.receipt.state_scope),
                generation: loaded.receipt.generation,
                state_hash: loaded.receipt.state_hash,
                action_digest: loaded.receipt.action_digest,
                authorization_digest: loaded.receipt.authorization_digest,
                idempotency_key: loaded.receipt.idempotency_key,
                receipt_id: loaded.receipt.receipt_id,
                receipt_digest: loaded.receipt.receipt_digest,
                replayed: loaded.receipt.replayed,
            },
        })
    }
    async fn cancel_invocation(
        &self,
        command: CancelInvocationCommand,
    ) -> Result<InvocationCancellationReceipt, PluginLogicError> {
        validate_cancellation_command(&command)?;
        let receipt = self
            .data
            .cancel_invocation(data::CancelInvocationData {
                target: map_cancellation_target(command.target),
                reason_code: command.reason_code,
                action_digest: command.action_digest,
                nonce: command.nonce,
                idempotency_key: command.idempotency_key,
                authorization: map_auth(command.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(InvocationCancellationReceipt {
            target: unmap_cancellation_target(receipt.target),
            reason_code: receipt.reason_code,
            action_digest: receipt.action_digest,
            nonce: receipt.nonce,
            idempotency_key: receipt.idempotency_key,
            cancellation_id: receipt.cancellation_id,
            status: match receipt.status {
                data::InvocationCancellationStatusData::Signalled => {
                    InvocationCancellationStatus::Signalled
                }
                data::InvocationCancellationStatusData::AlreadyTerminal => {
                    InvocationCancellationStatus::AlreadyTerminal
                }
            },
            receipt_id: receipt.receipt_id,
            receipt_digest: receipt.receipt_digest,
        })
    }
    async fn state_change(&self, r: StateChangeCommand) -> Result<Audit, PluginLogicError> {
        validate_id(&r.plugin_id)?;
        validate_auth(&r.authorization)?;
        self.data
            .state_change(data::StateChangeData {
                plugin_id: r.plugin_id,
                plugin_version: r.plugin_version,
                configuration_reference: r.configuration_reference,
                action: match r.action {
                    StateChangeAction::Disable => data::StateChangeActionData::Disable,
                    StateChangeAction::Enable => data::StateChangeActionData::Enable,
                    StateChangeAction::Quarantine => data::StateChangeActionData::Quarantine,
                    StateChangeAction::Unquarantine => data::StateChangeActionData::Unquarantine,
                },
                reason: r.reason,
                authorization: map_auth(r.authorization),
            })
            .await
            .map(map_audit)
            .map_err(map_error)
    }
    async fn health(&self) -> Health {
        let v = self.data.health().await;
        Health {
            loaded: v.loaded,
            running: v.running,
            observer_pending: v.observer_pending,
            observer_dropped: v.observer_dropped,
            state_flushed: v.state_flushed,
        }
    }
}
fn validate_id(v: &str) -> Result<(), PluginLogicError> {
    if v.is_empty()
        || v.len() > 128
        || !v
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
    {
        Err(PluginLogicError::Invalid)
    } else {
        Ok(())
    }
}

fn validate_operation_identity(v: &str) -> Result<(), PluginLogicError> {
    if !v.contains(':') {
        return validate_id(v);
    }
    if v.len() > 128 {
        return Err(PluginLogicError::Invalid);
    }
    let mut segments = v.split(':');
    let prefix = segments.next().ok_or(PluginLogicError::Invalid)?;
    let digest = segments.next().ok_or(PluginLogicError::Invalid)?;
    if segments.next().is_some()
        || validate_id(prefix).is_err()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(PluginLogicError::Invalid);
    }
    Ok(())
}

fn validate_operation_run_id(v: &str) -> Result<(), PluginLogicError> {
    if !v.contains(':') {
        return validate_id(v);
    }
    if v.len() > 256 {
        return Err(PluginLogicError::Invalid);
    }
    let segments = v.split(':').collect::<Vec<_>>();
    let valid = match segments.as_slice() {
        ["style-run", session_id] => valid_canonical_uuid(session_id),
        ["style-turn", session_id, sequence, state_hash, request_hash] => {
            valid_canonical_uuid(session_id)
                && valid_canonical_u64(sequence)
                && valid_lower_hex_digest(state_hash)
                && valid_lower_hex_digest(request_hash)
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(PluginLogicError::Invalid)
    }
}

fn valid_canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

fn valid_canonical_u64(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value
            .parse::<u64>()
            .is_ok_and(|parsed| parsed.to_string() == value)
}

fn valid_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_auth(v: &Authorization) -> Result<(), PluginLogicError> {
    if v.owner_id.is_empty()
        || v.session_id.is_empty()
        || v.call_id.is_empty()
        || v.normalized_digest.len() != 64
        || v.grant.is_empty()
        || v.cancellation_id.is_empty()
    {
        Err(PluginLogicError::Invalid)
    } else {
        Ok(())
    }
}
fn validate_cancellation_command(
    command: &CancelInvocationCommand,
) -> Result<(), PluginLogicError> {
    for value in [
        command.target.session_id.as_str(),
        command.target.run_id.as_str(),
        command.target.plugin_id.as_str(),
        command.target.plugin_version.as_str(),
        command.target.invocation_id.as_str(),
        command.target.operation_id.as_str(),
        command.reason_code.as_str(),
        command.nonce.as_str(),
        command.idempotency_key.as_str(),
    ] {
        if value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte))
        {
            return Err(PluginLogicError::Invalid);
        }
    }
    for digest in [
        command.target.invocation_digest.as_str(),
        command.target.declaration_hash.as_str(),
        command.target.request_hash.as_str(),
        command.action_digest.as_str(),
    ] {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(PluginLogicError::Invalid);
        }
    }
    if command.authorization.session_id != command.target.session_id
        || command.authorization.normalized_digest != command.action_digest
    {
        return Err(PluginLogicError::Authorization);
    }
    validate_auth(&command.authorization)
}

fn map_cancellation_target(
    target: InvocationCancellationTarget,
) -> data::InvocationCancellationTargetData {
    data::InvocationCancellationTargetData {
        session_id: target.session_id,
        run_id: target.run_id,
        plugin_id: target.plugin_id,
        plugin_version: target.plugin_version,
        invocation_id: target.invocation_id,
        invocation_digest: target.invocation_digest,
        operation_id: target.operation_id,
        declaration_hash: target.declaration_hash,
        request_hash: target.request_hash,
    }
}

fn unmap_cancellation_target(
    target: data::InvocationCancellationTargetData,
) -> InvocationCancellationTarget {
    InvocationCancellationTarget {
        session_id: target.session_id,
        run_id: target.run_id,
        plugin_id: target.plugin_id,
        plugin_version: target.plugin_version,
        invocation_id: target.invocation_id,
        invocation_digest: target.invocation_digest,
        operation_id: target.operation_id,
        declaration_hash: target.declaration_hash,
        request_hash: target.request_hash,
    }
}
fn validate_operation_command(
    binding: &OperationBinding,
    implementation_id: &str,
    implementation_version: &str,
    handler: &str,
    idempotency: OperationIdempotency,
    must_be_pure_idempotent: bool,
    authorization: &Authorization,
) -> Result<(), PluginLogicError> {
    for value in [
        binding.plugin_id.as_str(),
        binding.plugin_version.as_str(),
        binding.operation_id.as_str(),
        binding.session_id.as_str(),
        implementation_id,
        implementation_version,
        handler,
    ] {
        validate_id(value)?;
    }
    validate_operation_run_id(&binding.run_id)?;
    validate_operation_identity(&binding.invocation_id)?;
    validate_operation_identity(&binding.idempotency_key)?;
    if binding
        .node_id
        .as_deref()
        .is_some_and(|node_id| validate_id(node_id).is_err())
        || binding.attempt == 0
        || binding.attempt > 16
        || [
            binding.declaration_hash.as_str(),
            binding.configuration_reference.as_str(),
            binding.request_hash.as_str(),
        ]
        .iter()
        .any(|hash| hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
        || (must_be_pure_idempotent && idempotency != OperationIdempotency::Idempotent)
    {
        return Err(PluginLogicError::Invalid);
    }
    validate_auth(authorization)
}
fn operation_audit(
    plugin_id: String,
    invocation_id: String,
    operation: &str,
    attempts: u8,
) -> Audit {
    Audit {
        plugin_id,
        invocation_id: Some(invocation_id),
        operation: operation.to_owned(),
        outcome: String::from("completed"),
        attempts,
    }
}
const fn map_idempotency(idempotency: OperationIdempotency) -> data::OperationIdempotencyData {
    match idempotency {
        OperationIdempotency::Idempotent => data::OperationIdempotencyData::Idempotent,
        OperationIdempotency::NonIdempotent => data::OperationIdempotencyData::NonIdempotent,
    }
}
fn map_binding(binding: OperationBinding) -> data::OperationBindingData {
    data::OperationBindingData {
        plugin_id: binding.plugin_id,
        plugin_version: binding.plugin_version,
        invocation_id: binding.invocation_id,
        operation_id: binding.operation_id,
        session_id: binding.session_id,
        run_id: binding.run_id,
        node_id: binding.node_id,
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        request_hash: binding.request_hash,
        idempotency_key: binding.idempotency_key,
        attempt: binding.attempt,
    }
}
fn unmap_binding(binding: data::OperationBindingData) -> OperationBinding {
    OperationBinding {
        plugin_id: binding.plugin_id,
        plugin_version: binding.plugin_version,
        invocation_id: binding.invocation_id,
        operation_id: binding.operation_id,
        session_id: binding.session_id,
        run_id: binding.run_id,
        node_id: binding.node_id,
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        request_hash: binding.request_hash,
        idempotency_key: binding.idempotency_key,
        attempt: binding.attempt,
    }
}
fn map_manifest(v: ManifestCommand) -> data::ManifestData {
    data::ManifestData {
        schema_version: v.schema_version,
        id: v.id,
        version: v.version,
        runtime_api: v.runtime_api,
        category: v.category,
        scope: v.scope,
        class: match v.class {
            PluginClass::Blocking => data::PluginClassData::Blocking,
            PluginClass::Observer => data::PluginClassData::Observer,
            PluginClass::Tool => data::PluginClassData::Tool,
            PluginClass::Extension => data::PluginClassData::Extension,
        },
        program: v.program,
        arguments: v.arguments,
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
        schema_id: v.schema_id,
        schema_version_number: v.schema_version_number,
        schema_required: v.schema_required,
        schema_json: v.schema_json,
        node_executors: v
            .node_executors
            .into_iter()
            .map(|executor| data::NodeExecutorData {
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
                idempotent: executor.idempotent,
                tool_permissions: executor.tool_permissions,
                network_permissions: executor.network_permissions,
                state_scope: executor.state_scope,
                external_effects: executor.external_effects,
            })
            .collect(),
        context_transforms: v
            .context_transforms
            .into_iter()
            .map(|transform| data::ContextTransformData {
                transform_id: transform.transform_id,
                version: transform.version,
                runtime_api: transform.runtime_api,
                handler: transform.handler,
                lifecycle: match transform.lifecycle {
                    ContextTransformLifecycle::BeforeModelRequest => {
                        data::ContextTransformLifecycleData::BeforeModelRequest
                    }
                },
                capabilities: transform.capabilities,
                input_schema: transform.input_schema,
                output_schema: transform.output_schema,
                timeout_ms: transform.timeout_ms,
                failure_policy: transform.failure_policy,
                max_attempts: transform.max_attempts,
                retry_backoff_ms: transform.retry_backoff_ms,
                idempotent: transform.idempotent,
                tool_permissions: transform.tool_permissions,
                network_permissions: transform.network_permissions,
                state_scope: transform.state_scope,
                external_effects: transform.external_effects,
            })
            .collect(),
        memory_providers: v
            .memory_providers
            .into_iter()
            .map(map_memory_provider)
            .collect(),
        compactors: v.compactors.into_iter().map(map_compactor).collect(),
    }
}
fn map_memory_provider(provider: MemoryProviderDeclarationCommand) -> data::MemoryProviderData {
    data::MemoryProviderData {
        provider_id: provider.provider_id,
        version: provider.version,
        runtime_api: provider.runtime_api,
        capabilities: provider.capabilities,
        retrieve: map_operation_declaration(provider.retrieve),
        write: provider.write.map(map_operation_declaration),
    }
}
fn map_compactor(compactor: CompactorDeclarationCommand) -> data::CompactorData {
    data::CompactorData {
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
    operation: OperationDeclarationCommand,
) -> data::OperationDeclarationData {
    data::OperationDeclarationData {
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
fn map_auth(v: Authorization) -> data::AuthorizationData {
    data::AuthorizationData {
        owner_id: v.owner_id,
        session_id: v.session_id,
        call_id: v.call_id,
        normalized_digest: v.normalized_digest,
        grant: v.grant,
        cancellation_id: v.cancellation_id,
    }
}
const fn map_node_state_scope(scope: NodeStateScope) -> data::NodeStateScopeData {
    match scope {
        NodeStateScope::Invocation => data::NodeStateScopeData::Invocation,
        NodeStateScope::ModelCall => data::NodeStateScopeData::ModelCall,
        NodeStateScope::Turn => data::NodeStateScopeData::Turn,
        NodeStateScope::Session => data::NodeStateScopeData::Session,
        NodeStateScope::Project => data::NodeStateScopeData::Project,
        NodeStateScope::User => data::NodeStateScopeData::User,
        NodeStateScope::Runtime => data::NodeStateScopeData::Runtime,
    }
}
const fn unmap_node_state_scope(scope: data::NodeStateScopeData) -> NodeStateScope {
    match scope {
        data::NodeStateScopeData::Invocation => NodeStateScope::Invocation,
        data::NodeStateScopeData::ModelCall => NodeStateScope::ModelCall,
        data::NodeStateScopeData::Turn => NodeStateScope::Turn,
        data::NodeStateScopeData::Session => NodeStateScope::Session,
        data::NodeStateScopeData::Project => NodeStateScope::Project,
        data::NodeStateScopeData::User => NodeStateScope::User,
        data::NodeStateScopeData::Runtime => NodeStateScope::Runtime,
    }
}
fn map_audit(v: data::AuditData) -> Audit {
    Audit {
        plugin_id: v.plugin_id,
        invocation_id: v.invocation_id,
        operation: v.operation,
        outcome: v.outcome,
        attempts: v.attempts,
    }
}
#[allow(clippy::needless_pass_by_value)]
fn map_error(v: data::PluginDataError) -> PluginLogicError {
    match v {
        data::PluginDataError::Invalid => PluginLogicError::Invalid,
        data::PluginDataError::Authorization => PluginLogicError::Authorization,
        data::PluginDataError::Unavailable => PluginLogicError::Unavailable,
        data::PluginDataError::Cancelled => PluginLogicError::Cancelled,
        data::PluginDataError::Ambiguous => PluginLogicError::Ambiguous,
        data::PluginDataError::StaleStateGeneration => PluginLogicError::StaleStateGeneration,
        data::PluginDataError::StateConflict => PluginLogicError::StateConflict,
        data::PluginDataError::CancellationConflict => PluginLogicError::CancellationConflict,
        data::PluginDataError::External => PluginLogicError::Operation,
    }
}
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginLogicError {
    #[error("invalid plugin command")]
    Invalid,
    #[error("plugin authorization denied")]
    Authorization,
    #[error("plugin unavailable")]
    Unavailable,
    #[error("plugin cancelled")]
    Cancelled,
    #[error("plugin execution is ambiguous")]
    Ambiguous,
    #[error("plugin-node state generation is stale")]
    StaleStateGeneration,
    #[error("plugin-node state conflicts with an existing receipt")]
    StateConflict,
    #[error("plugin cancellation identity conflicts with active or persisted state")]
    CancellationConflict,
    #[error("plugin operation failed")]
    Operation,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> OperationBinding {
        OperationBinding {
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("1.0.0"),
            invocation_id: String::from("invoke-1"),
            operation_id: String::from("retrieve-1"),
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: Some(String::from("memory-node")),
            declaration_hash: "11".repeat(32),
            configuration_reference: "22".repeat(32),
            request_hash: "33".repeat(32),
            idempotency_key: String::from("memory-key"),
            attempt: 1,
        }
    }

    fn authorization() -> Authorization {
        Authorization {
            owner_id: String::from("owner"),
            session_id: String::from("session-1"),
            call_id: String::from("call-1"),
            normalized_digest: "44".repeat(32),
            grant: String::from("grant"),
            cancellation_id: String::from("cancel-1"),
        }
    }

    #[test]
    fn pure_memory_operations_require_idempotent_exact_bounded_identity() {
        assert_eq!(
            validate_operation_command(
                &binding(),
                "memory.provider",
                "1.0.0",
                "retrieve",
                OperationIdempotency::Idempotent,
                true,
                &authorization(),
            ),
            Ok(())
        );
        assert_eq!(
            validate_operation_command(
                &binding(),
                "memory.provider",
                "1.0.0",
                "retrieve",
                OperationIdempotency::NonIdempotent,
                true,
                &authorization(),
            ),
            Err(PluginLogicError::Invalid)
        );
        let mut invalid = binding();
        invalid.declaration_hash = String::from("not-a-hash");
        assert_eq!(
            validate_operation_command(
                &invalid,
                "memory.provider",
                "1.0.0",
                "retrieve",
                OperationIdempotency::Idempotent,
                true,
                &authorization(),
            ),
            Err(PluginLogicError::Invalid)
        );
    }

    #[test]
    fn operation_identity_accepts_only_plain_or_single_digest_scoped_ids() {
        assert_eq!(validate_operation_identity("invoke-1"), Ok(()));
        assert_eq!(
            validate_operation_identity(&format!(
                "plugin-automatic-memory-write:{}",
                "ab".repeat(32)
            )),
            Ok(())
        );
        assert_eq!(
            validate_operation_identity(&format!(
                "plugin-context-operation-once:{}",
                "01".repeat(32)
            )),
            Ok(())
        );

        for invalid in [
            String::new(),
            format!("plugin/context:{}", "ab".repeat(32)),
            format!("plugin\ncontext:{}", "ab".repeat(32)),
            format!("plugin-context::{}", "ab".repeat(32)),
            String::from("plugin-context:"),
            format!("plugin-context:{}", "ab".repeat(31)),
            format!("plugin-context:{}", "AB".repeat(32)),
            format!("plugin-context:{}", "ag".repeat(32)),
            format!("{}:{}", "p".repeat(64), "ab".repeat(32)),
        ] {
            assert_eq!(
                validate_operation_identity(&invalid),
                Err(PluginLogicError::Invalid),
                "{invalid:?} must be rejected"
            );
        }
    }

    #[test]
    fn operation_command_accepts_runtime_digest_scoped_receipt_identities() {
        let mut scoped = binding();
        scoped.invocation_id = format!("plugin-automatic-memory-write:{}", "ab".repeat(32));
        scoped.idempotency_key = format!("plugin-automatic-memory-write-once:{}", "cd".repeat(32));

        assert_eq!(
            validate_operation_command(
                &scoped,
                "memory.provider",
                "1.0.0",
                "write",
                OperationIdempotency::Idempotent,
                true,
                &authorization(),
            ),
            Ok(())
        );
    }

    #[test]
    fn operation_run_id_accepts_only_exact_runtime_owned_composites() {
        let session = "019fb6d7-3026-7d93-842f-71ff151836d7";
        let state_hash = "ab".repeat(32);
        let request_hash = "01".repeat(32);
        assert_eq!(
            validate_operation_run_id(&format!("style-run:{session}")),
            Ok(())
        );
        assert_eq!(
            validate_operation_run_id(&format!(
                "style-turn:{session}:2:{state_hash}:{request_hash}"
            )),
            Ok(())
        );
        assert_eq!(
            validate_operation_run_id(&format!(
                "style-turn:{session}:{}:{state_hash}:{request_hash}",
                u64::MAX
            )),
            Ok(())
        );

        for invalid in [
            String::new(),
            String::from("style-run:"),
            format!("style-run::{session}"),
            format!("style-run:{session}:"),
            format!("style-turn:{session}:2:{state_hash}"),
            format!("style-turn::{session}:2:{state_hash}:{request_hash}"),
            format!("style-turn:{session}::2:{state_hash}:{request_hash}"),
            format!("style-turn:{session}:0:{state_hash}:{request_hash}:extra"),
            format!("style-turn:{session}:18446744073709551616:{state_hash}:{request_hash}"),
            format!("style-turn:{session}:00:{state_hash}:{request_hash}"),
            format!("style-turn:{session}:02:{state_hash}:{request_hash}"),
            format!("style-turn:{session}:+2:{state_hash}:{request_hash}"),
            format!("style-turn:{session}:2:{}:{request_hash}", "a".repeat(63)),
            format!("style-turn:{session}:2:{}:{request_hash}", "a".repeat(65)),
            format!("style-turn:{session}:2:{}:{request_hash}", "AB".repeat(32)),
            format!("style-turn:{session}:2:{state_hash}:{}", "0".repeat(63)),
            format!("style-turn:{session}:2:{state_hash}:{}", "0".repeat(65)),
            format!("style-turn:{session}:2:{state_hash}:{}", "ag".repeat(32)),
            format!("style-turn:{session}:2:{state_hash}:{request_hash}:extra"),
            format!("other-run:{session}"),
            format!("style-run:{session}/child"),
            format!("style-run:{session}/../child"),
            format!("style-run:../{session}"),
            format!("style-run:{session}\n"),
            format!("style-run:{session}\0"),
            String::from("style-run:019fb6d7-3026-7d93-842f-71ff151836d"),
            String::from("style-run:019fb6d7-3026-7d93-842f-71ff151836d70"),
            String::from("style-run:019fb6d7_3026-7d93-842f-71ff151836d7"),
            format!(
                "style-turn:019FB6D7-3026-7D93-842F-71FF151836D7:2:{state_hash}:{request_hash}"
            ),
            format!(
                "style-turn:{session}:2:{state_hash}:{request_hash}{}",
                "x".repeat(257)
            ),
        ] {
            assert_eq!(
                validate_operation_run_id(&invalid),
                Err(PluginLogicError::Invalid),
                "{invalid:?} must be rejected"
            );
        }
    }
}
