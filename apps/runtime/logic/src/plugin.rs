//! Style-selected plugin activation and blocking-pipeline composition.
#![allow(
    missing_docs,
    reason = "logic-local plugin commands and records remain boundary-specific"
)]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use agentmod_event_pipeline::{
    BlockingInterceptor, BlockingPipeline, BlockingPipelineBuilder, Decision, FailurePolicy,
    InterceptorError, InterceptorRegistration, OrderingSpec,
};
use agentmod_primitives::ContentHash;
use agentmod_runtime_data::plugin::{
    ActivatePluginsDataRequest, ActivatedPluginDataRecord, InvokePluginDataRequest,
    InvokePluginNodeExecutorDataRequest, LoadPluginNodeStateDataRequest,
    LoadedPluginNodeStateDataRecord, PersistPluginNodeStateDataRequest, PluginDataError,
    PluginDataPort, PluginDecisionDataRecord, PluginInvocationCancellationTargetDataRecord,
    PluginNodeStateReadReceiptDataRecord, PluginNodeStateReceiptDataRecord,
    PluginNodeStateScopeData,
};
use agentmod_session_style_sdk::{
    CompiledSessionStyle, DecisionCapability, InterceptorDeclaration,
};
use async_trait::async_trait;
use serde_json::json;
use thiserror::Error;

use crate::action::ActionProposal;
use crate::node_execution::NodeWorkIdentity;
use crate::node_executor::{NodeExecutorBoundary, NodeExecutorSource, ResolvedNodeExecutor};
use crate::plugin_observer::{
    ObserverDeclaration, ObserverDeliveryCoordinator, ObserverDeliveryError,
    PreparedObserverDelivery,
};
use crate::session::{RuntimeCommittedEvent, SessionState};

const MAX_PLUGIN_NODE_VALUE_BYTES: usize = 1024 * 1024;
const MAX_PLUGIN_NODE_ACTIONS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct PluginInvocationCancellationTarget {
    pub session_id: String,
    pub run_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub operation_id: String,
    pub declaration_hash: ContentHash,
    pub request_hash: ContentHash,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PluginNodeStateScope {
    Invocation,
    ModelCall,
    Turn,
    Session,
    Project,
    User,
    Runtime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PersistPluginNodeStateCommand {
    pub cancellation_target: PluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub state_scope: PluginNodeStateScope,
    pub prior_generation: u64,
    pub prior_state_hash: Option<ContentHash>,
    pub state: serde_json::Value,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub nonce: String,
    pub cancellation_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeStatePersistenceReceipt {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub state_scope: PluginNodeStateScope,
    pub prior_generation: u64,
    pub generation: u64,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub idempotency_key: String,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
    pub replayed: bool,
}

/// Hashes a bounded plugin-node state value exactly as the persistence
/// boundary validates it.
///
/// # Errors
///
/// Returns [`PluginNodeStatePersistenceError::InvalidState`] when the value
/// cannot be encoded or exceeds the plugin-node value bound.
pub fn plugin_node_state_value_hash(
    value: &serde_json::Value,
) -> Result<ContentHash, PluginNodeStatePersistenceError> {
    let encoded =
        serde_json::to_vec(value).map_err(|_| PluginNodeStatePersistenceError::InvalidState)?;
    if encoded.len() > MAX_PLUGIN_NODE_VALUE_BYTES {
        return Err(PluginNodeStatePersistenceError::InvalidState);
    }
    Ok(ContentHash::digest(&encoded))
}

/// Hashes the complete versioned state-persistence request authenticated by
/// the invocation cancellation target.
///
/// # Errors
///
/// Returns [`PluginNodeStatePersistenceError::InvalidCommand`] when the
/// complete request identity cannot be encoded.
pub fn plugin_node_state_persistence_request_hash(
    command: &PersistPluginNodeStateCommand,
) -> Result<ContentHash, PluginNodeStatePersistenceError> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-state.persist.request.v1",
        &command.plugin_id,
        &command.invocation_id,
        command.invocation_digest,
        &command.executor_id,
        &command.executor_version,
        command.executor_declaration_hash,
        command.configuration_reference,
        plugin_node_state_scope_name(command.state_scope),
        command.prior_generation,
        command.prior_state_hash,
        &command.state,
        command.state_hash,
        &command.idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginNodeStatePersistenceError::InvalidCommand)
}

/// Hashes the complete versioned state-read request authenticated by the
/// invocation cancellation target.
///
/// # Errors
///
/// Returns [`PluginNodeStateReadError::InvalidCommand`] when the complete
/// request identity cannot be encoded.
pub fn plugin_node_state_read_request_hash(
    command: &LoadPluginNodeStateCommand,
) -> Result<ContentHash, PluginNodeStateReadError> {
    serde_json::to_vec(&(
        "agentmod.plugin.node-state.load.request.v1",
        &command.plugin_id,
        &command.invocation_id,
        command.invocation_digest,
        &command.executor_id,
        &command.executor_version,
        command.executor_declaration_hash,
        command.configuration_reference,
        plugin_node_state_scope_name(command.state_scope),
        command.expected_generation,
        command.expected_state_hash,
        &command.idempotency_key,
    ))
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginNodeStateReadError::InvalidCommand)
}

/// Derives exact action and authorization digests for a state CAS command.
///
/// The keyed grant remains dependency-owned; this digest pair is the bounded
/// identity that the dependency must place inside that grant.
///
/// # Errors
///
/// Returns [`PluginNodeStatePersistenceError::InvalidCommand`] if the identity
/// cannot be encoded.
pub fn plugin_node_state_persistence_digests(
    command: &PersistPluginNodeStateCommand,
) -> Result<(ContentHash, ContentHash), PluginNodeStatePersistenceError> {
    let action = serde_json::to_vec(&(
        &command.session_id,
        &command.plugin_id,
        &command.invocation_id,
        command.invocation_digest,
        &command.executor_id,
        &command.executor_version,
        command.executor_declaration_hash,
        command.configuration_reference,
        command.state_scope,
        command.prior_generation,
        command.prior_state_hash,
        command.state_hash,
        &command.idempotency_key,
    ))
    .map_err(|_| PluginNodeStatePersistenceError::InvalidCommand)?;
    let action_digest = ContentHash::digest(&action);
    let authorization = serde_json::to_vec(&(
        &command.cancellation_target,
        action_digest,
        &command.nonce,
        &command.cancellation_id,
        &command.idempotency_key,
    ))
    .map_err(|_| PluginNodeStatePersistenceError::InvalidCommand)?;
    Ok((action_digest, ContentHash::digest(&authorization)))
}

/// Hashes the exact terminal receipt identity.
///
/// # Errors
///
/// Returns [`PluginNodeStatePersistenceError::InvalidReceipt`] when encoding
/// fails.
pub fn plugin_node_state_persistence_receipt_digest(
    receipt: &PluginNodeStatePersistenceReceipt,
) -> Result<ContentHash, PluginNodeStatePersistenceError> {
    serde_json::to_vec(&(
        &receipt.plugin_id,
        &receipt.invocation_id,
        receipt.invocation_digest,
        &receipt.executor_id,
        &receipt.executor_version,
        receipt.executor_declaration_hash,
        receipt.state_scope,
        receipt.prior_generation,
        receipt.generation,
        receipt.state_hash,
        receipt.action_digest,
        receipt.authorization_digest,
        &receipt.idempotency_key,
        &receipt.receipt_id,
    ))
    .map(|encoded| ContentHash::digest(&encoded))
    .map_err(|_| PluginNodeStatePersistenceError::InvalidReceipt)
}

#[async_trait]
pub trait PluginNodeStatePersistenceLogicPort: Send + Sync {
    async fn persist_plugin_node_state(
        &self,
        command: PersistPluginNodeStateCommand,
    ) -> Result<PluginNodeStatePersistenceReceipt, PluginNodeStatePersistenceError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct LoadPluginNodeStateCommand {
    pub cancellation_target: PluginInvocationCancellationTarget,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub state_scope: PluginNodeStateScope,
    pub expected_generation: u64,
    pub expected_state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub nonce: String,
    pub cancellation_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeStateReadReceipt {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub state_scope: PluginNodeStateScope,
    pub generation: u64,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub idempotency_key: String,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LoadedPluginNodeState {
    pub state: serde_json::Value,
    pub receipt: PluginNodeStateReadReceipt,
}

/// Derives exact action and authorization digests for a state-read command.
///
/// # Errors
///
/// Returns [`PluginNodeStateReadError::InvalidCommand`] when encoding fails.
pub fn plugin_node_state_read_digests(
    command: &LoadPluginNodeStateCommand,
) -> Result<(ContentHash, ContentHash), PluginNodeStateReadError> {
    let action = serde_json::to_vec(&(
        &command.session_id,
        &command.plugin_id,
        &command.invocation_id,
        command.invocation_digest,
        &command.executor_id,
        &command.executor_version,
        command.executor_declaration_hash,
        command.configuration_reference,
        command.state_scope,
        command.expected_generation,
        command.expected_state_hash,
        &command.idempotency_key,
    ))
    .map_err(|_| PluginNodeStateReadError::InvalidCommand)?;
    let action_digest = ContentHash::digest(&action);
    let authorization = serde_json::to_vec(&(
        &command.cancellation_target,
        action_digest,
        &command.nonce,
        &command.cancellation_id,
        &command.idempotency_key,
    ))
    .map_err(|_| PluginNodeStateReadError::InvalidCommand)?;
    Ok((action_digest, ContentHash::digest(&authorization)))
}

/// Hashes the immutable terminal state-read receipt identity.
///
/// # Errors
///
/// Returns [`PluginNodeStateReadError::InvalidReceipt`] when encoding fails.
pub fn plugin_node_state_read_receipt_digest(
    receipt: &PluginNodeStateReadReceipt,
) -> Result<ContentHash, PluginNodeStateReadError> {
    serde_json::to_vec(&(
        &receipt.plugin_id,
        &receipt.invocation_id,
        receipt.invocation_digest,
        &receipt.executor_id,
        &receipt.executor_version,
        receipt.executor_declaration_hash,
        receipt.state_scope,
        receipt.generation,
        receipt.state_hash,
        receipt.action_digest,
        receipt.authorization_digest,
        &receipt.idempotency_key,
        &receipt.receipt_id,
    ))
    .map(|encoded| ContentHash::digest(&encoded))
    .map_err(|_| PluginNodeStateReadError::InvalidReceipt)
}

#[async_trait]
pub trait PluginNodeStateReadLogicPort: Send + Sync {
    async fn load_plugin_node_state(
        &self,
        command: LoadPluginNodeStateCommand,
    ) -> Result<LoadedPluginNodeState, PluginNodeStateReadError>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutePluginNodeCommand {
    pub session_id: String,
    pub work: NodeWorkIdentity,
    pub executor: ResolvedNodeExecutor,
    pub adapter_configuration_reference: ContentHash,
    pub input: serde_json::Value,
    pub readable_state: serde_json::Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginNodeActionProposal {
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginNodeExecutionProposal {
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub output: serde_json::Value,
    pub preserved_state: serde_json::Value,
    pub proposed_actions: Vec<PluginNodeActionProposal>,
    pub attempts: u8,
}

/// Derives the stable digest-backed identity for one exact plugin-node command.
///
/// # Errors
///
/// Returns [`PluginNodeExecutionError::InvalidOutcome`] when the bounded
/// command identity cannot be serialized.
pub fn plugin_node_invocation_identity(
    command: &ExecutePluginNodeCommand,
) -> Result<(String, ContentHash), PluginNodeExecutionError> {
    let invocation_bytes = serde_json::to_vec(&serde_json::json!({
        "session_id": command.session_id,
        "work": command.work,
        "plugin_id": match &command.executor.source {
            NodeExecutorSource::Plugin { plugin_id } => plugin_id,
            NodeExecutorSource::Runtime => {
                return Err(PluginNodeExecutionError::InvalidResolution);
            }
        },
        "executor_id": command.executor.implementation_id,
        "executor_version": command.executor.implementation_version,
        "node_kind": command.executor.node_kind,
        "executor_declaration_hash": command.executor.executor_declaration_hash,
        "adapter_configuration_reference": command.adapter_configuration_reference,
        "input": command.input,
        "readable_state": command.readable_state,
    }))
    .map_err(|_| PluginNodeExecutionError::InvalidOutcome)?;
    let invocation_digest = ContentHash::digest(&invocation_bytes);
    Ok((
        format!("plugin-node:{}", invocation_digest.to_hex()),
        invocation_digest,
    ))
}

/// Constructs the complete domain-separated identity used for authenticated
/// cancellation of one exact plugin invocation.
///
/// # Errors
///
/// Returns a serialization error if deterministic identity encoding fails.
#[allow(clippy::too_many_arguments)]
pub fn plugin_invocation_cancellation_target(
    session_id: &str,
    run_id: &str,
    plugin_id: &str,
    plugin_version: &str,
    invocation_id: &str,
    operation_id: &str,
    declaration_hash: ContentHash,
    request_hash: ContentHash,
) -> Result<PluginInvocationCancellationTarget, serde_json::Error> {
    let invocation_digest = serde_json::to_vec(&(
        "agentmod.plugin.invocation.identity.v1",
        session_id,
        run_id,
        plugin_id,
        plugin_version,
        invocation_id,
        operation_id,
        declaration_hash,
        request_hash,
    ))
    .map(|bytes| ContentHash::digest(&bytes))?;
    Ok(PluginInvocationCancellationTarget {
        session_id: session_id.to_owned(),
        run_id: run_id.to_owned(),
        plugin_id: plugin_id.to_owned(),
        plugin_version: plugin_version.to_owned(),
        invocation_id: invocation_id.to_owned(),
        invocation_digest,
        operation_id: operation_id.to_owned(),
        declaration_hash,
        request_hash,
    })
}

pub(crate) fn map_cancellation_target(
    target: &PluginInvocationCancellationTarget,
) -> PluginInvocationCancellationTargetDataRecord {
    PluginInvocationCancellationTargetDataRecord {
        session_id: target.session_id.clone(),
        run_id: target.run_id.clone(),
        plugin_id: target.plugin_id.clone(),
        plugin_version: target.plugin_version.clone(),
        invocation_id: target.invocation_id.clone(),
        invocation_digest: target.invocation_digest,
        operation_id: target.operation_id.clone(),
        declaration_hash: target.declaration_hash,
        request_hash: target.request_hash,
    }
}

#[async_trait]
pub trait PluginNodeExecutorLogicPort: Send + Sync {
    async fn execute_plugin_node(
        &self,
        command: ExecutePluginNodeCommand,
    ) -> Result<PluginNodeExecutionProposal, PluginNodeExecutionError>;
}

#[async_trait]
#[allow(
    clippy::too_many_lines,
    reason = "the runtime validation gate audits the complete plugin-node proposal before returning it to orchestration"
)]
impl<D> PluginNodeExecutorLogicPort for PluginCompositionLogic<D>
where
    D: Clone + PluginDataPort + Send + Sync + 'static,
{
    async fn execute_plugin_node(
        &self,
        command: ExecutePluginNodeCommand,
    ) -> Result<PluginNodeExecutionProposal, PluginNodeExecutionError> {
        if command.work.node_id != command.executor.node_id
            || command.executor.boundary != NodeExecutorBoundary::PluginHost
            || command.adapter_configuration_reference
                != command.executor.adapter_configuration_reference
        {
            return Err(PluginNodeExecutionError::InvalidResolution);
        }
        let NodeExecutorSource::Plugin { plugin_id } = &command.executor.source else {
            return Err(PluginNodeExecutionError::InvalidResolution);
        };
        let declaration = self
            .data
            .node_executor_declaration(
                plugin_id,
                &command.executor.implementation_id,
                &command.executor.implementation_version,
                &command.executor.node_kind,
            )
            .map_err(PluginNodeExecutionError::Data)?;
        if declaration.declaration_hash != command.executor.executor_declaration_hash {
            return Err(PluginNodeExecutionError::InvalidResolution);
        }
        let plugin_configuration_reference = self
            .data
            .plugin_configuration_reference(plugin_id)
            .map_err(PluginNodeExecutionError::Data)?;
        validate_bounded_value(&command.input)?;
        validate_bounded_value(&command.readable_state)?;
        validate_json_schema(&declaration.input_schema, &command.input)?;
        let (invocation_id, invocation_digest) = plugin_node_invocation_identity(&command)?;
        let request_hash = serde_json::to_vec(&(
            "agentmod.plugin.node-executor.request.v1",
            plugin_id,
            &invocation_id,
            &command.executor.implementation_id,
            &command.executor.implementation_version,
            &command.executor.node_kind,
            &declaration.handler,
            declaration.timeout_ms,
            plugin_configuration_reference,
            &command.input,
            &command.readable_state,
        ))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginNodeExecutionError::InvalidOutcome)?;
        let cancellation_target = plugin_invocation_cancellation_target(
            &command.session_id,
            &command.work.run_id,
            plugin_id,
            &declaration.plugin_version,
            &invocation_id,
            &command.executor.implementation_id,
            declaration.declaration_hash,
            request_hash,
        )
        .map_err(|_| PluginNodeExecutionError::InvalidOutcome)?;
        let outcome = self
            .data
            .invoke_node_executor(InvokePluginNodeExecutorDataRequest {
                cancellation_target: map_cancellation_target(&cancellation_target),
                session_id: command.session_id,
                plugin_id: plugin_id.clone(),
                invocation_id: invocation_id.clone(),
                executor_id: command.executor.implementation_id,
                executor_version: command.executor.implementation_version,
                timeout_ms: declaration.timeout_ms,
                configuration_reference: plugin_configuration_reference,
                node_kind: command.executor.node_kind,
                input: command.input,
                readable_state: command.readable_state,
                cancellation_id: command.cancellation_id,
            })
            .await
            .map_err(|error| match error {
                PluginDataError::Ambiguous {
                    plugin_id,
                    executor_id,
                } => PluginNodeExecutionError::Ambiguous {
                    plugin_id,
                    executor_id,
                    invocation_id: invocation_id.clone(),
                    invocation_digest,
                },
                other => PluginNodeExecutionError::Data(other),
            })?;
        validate_json_schema(&declaration.output_schema, &outcome.output)?;
        validate_bounded_value(&outcome.output)?;
        validate_bounded_value(&outcome.preserved_state)?;
        if outcome.proposed_actions.len() > MAX_PLUGIN_NODE_ACTIONS
            || (!declaration.external_effects && !outcome.proposed_actions.is_empty())
        {
            return Err(PluginNodeExecutionError::InvalidOutcome);
        }
        let proposed_actions = outcome
            .proposed_actions
            .into_iter()
            .map(|action| {
                if action.kind.is_empty()
                    || action.kind.len() > 128
                    || !action.kind.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || b"._:-".contains(&byte)
                    })
                {
                    return Err(PluginNodeExecutionError::InvalidOutcome);
                }
                validate_bounded_value(&action.payload)?;
                Ok(PluginNodeActionProposal {
                    kind: action.kind,
                    payload: action.payload,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PluginNodeExecutionProposal {
            invocation_id,
            invocation_digest,
            output: outcome.output,
            preserved_state: outcome.preserved_state,
            proposed_actions,
            attempts: outcome.attempts,
        })
    }
}

fn validate_bounded_value(value: &serde_json::Value) -> Result<(), PluginNodeExecutionError> {
    if serde_json::to_vec(value)
        .map_err(|_| PluginNodeExecutionError::InvalidOutcome)?
        .len()
        > MAX_PLUGIN_NODE_VALUE_BYTES
    {
        return Err(PluginNodeExecutionError::InvalidOutcome);
    }
    Ok(())
}

fn validate_json_schema(
    schema: &str,
    value: &serde_json::Value,
) -> Result<(), PluginNodeExecutionError> {
    let schema: serde_json::Value =
        serde_json::from_str(schema).map_err(|_| PluginNodeExecutionError::InvalidDeclaration)?;
    let expected = schema.get("type").and_then(serde_json::Value::as_str);
    let valid = match expected {
        None => true,
        Some("null") => value.is_null(),
        Some("boolean") => value.is_boolean(),
        Some("number") => value.is_number(),
        Some("integer") => value.as_i64().is_some() || value.as_u64().is_some(),
        Some("string") => value.is_string(),
        Some("array") => value.is_array(),
        Some("object") => value.is_object(),
        Some(_) => false,
    };
    if !valid {
        return Err(PluginNodeExecutionError::InvalidOutcome);
    }
    if let (Some(required), Some(object)) = (
        schema.get("required").and_then(serde_json::Value::as_array),
        value.as_object(),
    ) && required
        .iter()
        .filter_map(serde_json::Value::as_str)
        .any(|field| !object.contains_key(field))
    {
        return Err(PluginNodeExecutionError::InvalidOutcome);
    }
    Ok(())
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginNodeExecutionError {
    #[error("persisted executor identity is not a plugin-host resolution")]
    InvalidResolution,
    #[error("plugin node executor declaration is invalid")]
    InvalidDeclaration,
    #[error("plugin node executor returned an invalid or unbounded proposal")]
    InvalidOutcome,
    #[error("plugin node data operation failed: {0}")]
    Data(PluginDataError),
    #[error(
        "plugin node invocation `{invocation_id}` is ambiguous for `{plugin_id}` executor `{executor_id}`"
    )]
    Ambiguous {
        plugin_id: String,
        executor_id: String,
        invocation_id: String,
        invocation_digest: ContentHash,
    },
}

#[derive(Clone, Debug)]
pub struct ComposePluginPipelineCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub compiled_style: CompiledSessionStyle,
    pub runtime_api_version: String,
}

#[derive(Clone)]
pub struct ComposedPluginPipeline {
    pub pipeline: Arc<BlockingPipeline<ActionProposal>>,
    pub activated_plugin_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommittedPluginEvent {
    pub event_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct ObserveCommittedPluginEventsCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub compiled_style: CompiledSessionStyle,
    pub runtime_api_version: String,
    pub events: Vec<CommittedPluginEvent>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PluginObservationSummary {
    pub enqueued: u64,
    pub dropped: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PlannedObserverDelivery {
    pub proposed: RuntimeCommittedEvent,
    pub prepared: PreparedObserverDelivery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeardownPluginHostCommand {
    pub session_id: String,
    pub active_continuations: usize,
    pub pending_observer_deliveries: usize,
}

#[async_trait]
pub trait PluginCompositionLogicPort: Send + Sync {
    /// Activates exact style-selected plugins and compiles their blocking order.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCompositionError`] when activation, compatibility, or
    /// deterministic ordering validation fails.
    async fn compose_pipeline(
        &self,
        command: ComposePluginPipelineCommand,
    ) -> Result<ComposedPluginPipeline, PluginCompositionError>;

    /// Plans exact canonical delivery identities without invoking observers.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCompositionError`] when live declarations cannot be
    /// revalidated or a bounded delivery identity cannot be constructed.
    async fn plan_observer_deliveries(
        &self,
        _command: ObserveCommittedPluginEventsCommand,
    ) -> Result<Vec<PlannedObserverDelivery>, PluginCompositionError> {
        Err(PluginCompositionError::ObserverCoordination)
    }

    /// Constructs dispatch intent for one exact replayed observer proposal.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCompositionError`] unless replay contains the exact
    /// proposed delivery identity.
    fn observer_dispatch_intent(
        &self,
        _state: &SessionState,
        _prepared: &PreparedObserverDelivery,
    ) -> Result<RuntimeCommittedEvent, PluginCompositionError> {
        Err(PluginCompositionError::ObserverCoordination)
    }

    /// Reconciles one exact replayed dispatch with a terminal host receipt.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCompositionError`] when host I/O fails or the exact
    /// terminal receipt cannot be validated.
    async fn reconcile_observer_receipt(
        &self,
        _state: &SessionState,
        _prepared: &PreparedObserverDelivery,
    ) -> Result<RuntimeCommittedEvent, PluginCompositionError> {
        Err(PluginCompositionError::ObserverCoordination)
    }

    /// Tears down a plugin host only after every runtime and host idle gate.
    ///
    /// # Errors
    ///
    /// Returns [`PluginCompositionError`] when health/flush validation or
    /// transport teardown fails.
    async fn teardown_host_if_idle(
        &self,
        _command: TeardownPluginHostCommand,
    ) -> Result<bool, PluginCompositionError> {
        Ok(false)
    }
}

#[derive(Clone)]
pub struct PluginCompositionLogic<D> {
    data: D,
}

impl<D> PluginCompositionLogic<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

#[async_trait]
impl<D> PluginNodeStatePersistenceLogicPort for PluginCompositionLogic<D>
where
    D: Clone + PluginDataPort + Send + Sync + 'static,
{
    async fn persist_plugin_node_state(
        &self,
        command: PersistPluginNodeStateCommand,
    ) -> Result<PluginNodeStatePersistenceReceipt, PluginNodeStatePersistenceError> {
        validate_state_persistence_command(&command)?;
        let receipt = self
            .data
            .persist_plugin_node_state(PersistPluginNodeStateDataRequest {
                cancellation_target: map_cancellation_target(&command.cancellation_target),
                session_id: command.session_id.clone(),
                plugin_id: command.plugin_id.clone(),
                invocation_id: command.invocation_id.clone(),
                invocation_digest: command.invocation_digest,
                executor_id: command.executor_id.clone(),
                executor_version: command.executor_version.clone(),
                executor_declaration_hash: command.executor_declaration_hash,
                configuration_reference: command.configuration_reference,
                state_scope: map_logic_state_scope(command.state_scope),
                prior_generation: command.prior_generation,
                prior_state_hash: command.prior_state_hash,
                state: command.state.clone(),
                state_hash: command.state_hash,
                action_digest: command.action_digest,
                authorization_digest: command.authorization_digest,
                nonce: command.nonce.clone(),
                cancellation_id: command.cancellation_id.clone(),
                idempotency_key: command.idempotency_key.clone(),
            })
            .await
            .map_err(|error| map_state_persistence_data_error(&command, error))?;
        let receipt = map_state_receipt(receipt);
        validate_logic_state_receipt(&command, &receipt)?;
        Ok(receipt)
    }
}

#[async_trait]
impl<D> PluginNodeStateReadLogicPort for PluginCompositionLogic<D>
where
    D: Clone + PluginDataPort + Send + Sync + 'static,
{
    async fn load_plugin_node_state(
        &self,
        command: LoadPluginNodeStateCommand,
    ) -> Result<LoadedPluginNodeState, PluginNodeStateReadError> {
        validate_state_read_command(&command)?;
        let loaded = self
            .data
            .load_plugin_node_state(LoadPluginNodeStateDataRequest {
                cancellation_target: map_cancellation_target(&command.cancellation_target),
                session_id: command.session_id.clone(),
                plugin_id: command.plugin_id.clone(),
                invocation_id: command.invocation_id.clone(),
                invocation_digest: command.invocation_digest,
                executor_id: command.executor_id.clone(),
                executor_version: command.executor_version.clone(),
                executor_declaration_hash: command.executor_declaration_hash,
                configuration_reference: command.configuration_reference,
                state_scope: map_logic_state_scope(command.state_scope),
                expected_generation: command.expected_generation,
                expected_state_hash: command.expected_state_hash,
                action_digest: command.action_digest,
                authorization_digest: command.authorization_digest,
                nonce: command.nonce.clone(),
                cancellation_id: command.cancellation_id.clone(),
                idempotency_key: command.idempotency_key.clone(),
            })
            .await
            .map_err(|error| map_state_read_data_error(&command, error))?;
        map_loaded_state(&command, loaded)
    }
}

fn validate_state_read_command(
    command: &LoadPluginNodeStateCommand,
) -> Result<(), PluginNodeStateReadError> {
    if !matches!(
        command.state_scope,
        PluginNodeStateScope::Invocation | PluginNodeStateScope::Session
    ) {
        return Err(PluginNodeStateReadError::UnsupportedScope);
    }
    let request_hash = plugin_node_state_read_request_hash(command)?;
    if command.expected_generation == 0
        || !valid_state_cancellation_target(
            &command.cancellation_target,
            &command.session_id,
            &command.plugin_id,
            &command.invocation_id,
            &format!("{}:state-read", command.executor_id),
            command.executor_declaration_hash,
            request_hash,
        )
        || [
            command.session_id.as_str(),
            command.plugin_id.as_str(),
            command.invocation_id.as_str(),
            command.executor_id.as_str(),
            command.executor_version.as_str(),
            command.nonce.as_str(),
            command.cancellation_id.as_str(),
            command.idempotency_key.as_str(),
        ]
        .iter()
        .any(|value| {
            value.is_empty()
                || value.len() > 256
                || !value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
        })
    {
        return Err(PluginNodeStateReadError::InvalidCommand);
    }
    let (action_digest, authorization_digest) = plugin_node_state_read_digests(command)?;
    if command.action_digest != action_digest
        || command.authorization_digest != authorization_digest
    {
        return Err(PluginNodeStateReadError::InvalidDigest);
    }
    Ok(())
}

fn map_loaded_state(
    command: &LoadPluginNodeStateCommand,
    loaded: LoadedPluginNodeStateDataRecord,
) -> Result<LoadedPluginNodeState, PluginNodeStateReadError> {
    if plugin_node_state_value_hash(&loaded.state)
        .map_err(|_| PluginNodeStateReadError::InvalidState)?
        != command.expected_state_hash
    {
        return Err(PluginNodeStateReadError::InvalidState);
    }
    let receipt = map_state_read_receipt(loaded.receipt);
    if receipt.plugin_id != command.plugin_id
        || receipt.invocation_id != command.invocation_id
        || receipt.invocation_digest != command.invocation_digest
        || receipt.executor_id != command.executor_id
        || receipt.executor_version != command.executor_version
        || receipt.executor_declaration_hash != command.executor_declaration_hash
        || receipt.state_scope != command.state_scope
        || receipt.generation != command.expected_generation
        || receipt.state_hash != command.expected_state_hash
        || receipt.action_digest != command.action_digest
        || receipt.authorization_digest != command.authorization_digest
        || receipt.idempotency_key != command.idempotency_key
        || receipt.receipt_id.is_empty()
        || plugin_node_state_read_receipt_digest(&receipt)? != receipt.receipt_digest
    {
        return Err(PluginNodeStateReadError::InvalidReceipt);
    }
    Ok(LoadedPluginNodeState {
        state: loaded.state,
        receipt,
    })
}

fn map_state_read_receipt(
    receipt: PluginNodeStateReadReceiptDataRecord,
) -> PluginNodeStateReadReceipt {
    PluginNodeStateReadReceipt {
        plugin_id: receipt.plugin_id,
        invocation_id: receipt.invocation_id,
        invocation_digest: receipt.invocation_digest,
        executor_id: receipt.executor_id,
        executor_version: receipt.executor_version,
        executor_declaration_hash: receipt.executor_declaration_hash,
        state_scope: unmap_logic_state_scope(receipt.state_scope),
        generation: receipt.generation,
        state_hash: receipt.state_hash,
        action_digest: receipt.action_digest,
        authorization_digest: receipt.authorization_digest,
        idempotency_key: receipt.idempotency_key,
        receipt_id: receipt.receipt_id,
        receipt_digest: receipt.receipt_digest,
        replayed: receipt.replayed,
    }
}

fn map_state_read_data_error(
    command: &LoadPluginNodeStateCommand,
    error: PluginDataError,
) -> PluginNodeStateReadError {
    match error {
        PluginDataError::StateReadUnsupported => PluginNodeStateReadError::Unsupported,
        PluginDataError::UnsupportedStateScope => PluginNodeStateReadError::UnsupportedScope,
        PluginDataError::StaleStateGeneration => PluginNodeStateReadError::StaleGeneration,
        PluginDataError::StateConflict => PluginNodeStateReadError::Conflict,
        PluginDataError::Cancelled => PluginNodeStateReadError::Cancelled,
        PluginDataError::AmbiguousStateRead { .. } => PluginNodeStateReadError::Ambiguous {
            plugin_id: command.plugin_id.clone(),
            invocation_id: command.invocation_id.clone(),
            idempotency_key: command.idempotency_key.clone(),
        },
        other => PluginNodeStateReadError::Data(other),
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginNodeStateReadError {
    #[error("plugin-node state read command is invalid")]
    InvalidCommand,
    #[error("plugin-node state read action or authorization digest is invalid")]
    InvalidDigest,
    #[error("plugin-node state read returned invalid or unbounded state")]
    InvalidState,
    #[error("plugin-node state read receipt identity is invalid")]
    InvalidReceipt,
    #[error("plugin-host protocol has no authenticated plugin-node state read")]
    Unsupported,
    #[error("plugin-node state scope lacks an exact canonical read identity")]
    UnsupportedScope,
    #[error("plugin-node state generation or hash is stale")]
    StaleGeneration,
    #[error("plugin-node state read conflicts with a prior idempotent request")]
    Conflict,
    #[error("plugin-node state read was cancelled")]
    Cancelled,
    #[error(
        "plugin-node state read is ambiguous for `{plugin_id}` invocation `{invocation_id}` idempotency `{idempotency_key}`"
    )]
    Ambiguous {
        plugin_id: String,
        invocation_id: String,
        idempotency_key: String,
    },
    #[error("plugin-node state data operation failed: {0}")]
    Data(PluginDataError),
}

fn validate_state_persistence_command(
    command: &PersistPluginNodeStateCommand,
) -> Result<(), PluginNodeStatePersistenceError> {
    if !matches!(
        command.state_scope,
        PluginNodeStateScope::Invocation | PluginNodeStateScope::Session
    ) {
        return Err(PluginNodeStatePersistenceError::UnsupportedScope);
    }
    let request_hash = plugin_node_state_persistence_request_hash(command)?;
    if !valid_state_cancellation_target(
        &command.cancellation_target,
        &command.session_id,
        &command.plugin_id,
        &command.invocation_id,
        &format!("{}:state-write", command.executor_id),
        command.executor_declaration_hash,
        request_hash,
    ) || [
        command.session_id.as_str(),
        command.plugin_id.as_str(),
        command.invocation_id.as_str(),
        command.executor_id.as_str(),
        command.executor_version.as_str(),
        command.nonce.as_str(),
        command.cancellation_id.as_str(),
        command.idempotency_key.as_str(),
    ]
    .iter()
    .any(|value| {
        value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    }) {
        return Err(PluginNodeStatePersistenceError::InvalidCommand);
    }
    if plugin_node_state_value_hash(&command.state)? != command.state_hash {
        return Err(PluginNodeStatePersistenceError::InvalidState);
    }
    let (action_digest, authorization_digest) = plugin_node_state_persistence_digests(command)?;
    if command.action_digest != action_digest
        || command.authorization_digest != authorization_digest
    {
        return Err(PluginNodeStatePersistenceError::InvalidDigest);
    }
    Ok(())
}

const fn plugin_node_state_scope_name(scope: PluginNodeStateScope) -> &'static str {
    match scope {
        PluginNodeStateScope::Invocation => "invocation",
        PluginNodeStateScope::ModelCall => "model_call",
        PluginNodeStateScope::Turn => "turn",
        PluginNodeStateScope::Session => "session",
        PluginNodeStateScope::Project => "project",
        PluginNodeStateScope::User => "user",
        PluginNodeStateScope::Runtime => "runtime",
    }
}

#[allow(clippy::too_many_arguments)]
fn valid_state_cancellation_target(
    target: &PluginInvocationCancellationTarget,
    session_id: &str,
    plugin_id: &str,
    invocation_id: &str,
    operation_id: &str,
    declaration_hash: ContentHash,
    request_hash: ContentHash,
) -> bool {
    !target.plugin_version.is_empty()
        && target.session_id == session_id
        && target.plugin_id == plugin_id
        && target.invocation_id == invocation_id
        && target.operation_id == operation_id
        && target.declaration_hash == declaration_hash
        && target.request_hash == request_hash
        && plugin_invocation_cancellation_target(
            &target.session_id,
            &target.run_id,
            &target.plugin_id,
            &target.plugin_version,
            &target.invocation_id,
            &target.operation_id,
            target.declaration_hash,
            target.request_hash,
        )
        .is_ok_and(|expected| expected.invocation_digest == target.invocation_digest)
}

const fn map_logic_state_scope(scope: PluginNodeStateScope) -> PluginNodeStateScopeData {
    match scope {
        PluginNodeStateScope::Invocation => PluginNodeStateScopeData::Invocation,
        PluginNodeStateScope::ModelCall => PluginNodeStateScopeData::ModelCall,
        PluginNodeStateScope::Turn => PluginNodeStateScopeData::Turn,
        PluginNodeStateScope::Session => PluginNodeStateScopeData::Session,
        PluginNodeStateScope::Project => PluginNodeStateScopeData::Project,
        PluginNodeStateScope::User => PluginNodeStateScopeData::User,
        PluginNodeStateScope::Runtime => PluginNodeStateScopeData::Runtime,
    }
}

const fn unmap_logic_state_scope(scope: PluginNodeStateScopeData) -> PluginNodeStateScope {
    match scope {
        PluginNodeStateScopeData::Invocation => PluginNodeStateScope::Invocation,
        PluginNodeStateScopeData::ModelCall => PluginNodeStateScope::ModelCall,
        PluginNodeStateScopeData::Turn => PluginNodeStateScope::Turn,
        PluginNodeStateScopeData::Session => PluginNodeStateScope::Session,
        PluginNodeStateScopeData::Project => PluginNodeStateScope::Project,
        PluginNodeStateScopeData::User => PluginNodeStateScope::User,
        PluginNodeStateScopeData::Runtime => PluginNodeStateScope::Runtime,
    }
}

fn map_state_receipt(
    receipt: PluginNodeStateReceiptDataRecord,
) -> PluginNodeStatePersistenceReceipt {
    PluginNodeStatePersistenceReceipt {
        plugin_id: receipt.plugin_id,
        invocation_id: receipt.invocation_id,
        invocation_digest: receipt.invocation_digest,
        executor_id: receipt.executor_id,
        executor_version: receipt.executor_version,
        executor_declaration_hash: receipt.executor_declaration_hash,
        state_scope: unmap_logic_state_scope(receipt.state_scope),
        prior_generation: receipt.prior_generation,
        generation: receipt.generation,
        state_hash: receipt.state_hash,
        action_digest: receipt.action_digest,
        authorization_digest: receipt.authorization_digest,
        idempotency_key: receipt.idempotency_key,
        receipt_id: receipt.receipt_id,
        receipt_digest: receipt.receipt_digest,
        replayed: receipt.replayed,
    }
}

fn validate_logic_state_receipt(
    command: &PersistPluginNodeStateCommand,
    receipt: &PluginNodeStatePersistenceReceipt,
) -> Result<(), PluginNodeStatePersistenceError> {
    if receipt.plugin_id != command.plugin_id
        || receipt.invocation_id != command.invocation_id
        || receipt.invocation_digest != command.invocation_digest
        || receipt.executor_id != command.executor_id
        || receipt.executor_version != command.executor_version
        || receipt.executor_declaration_hash != command.executor_declaration_hash
        || receipt.state_scope != command.state_scope
        || receipt.prior_generation != command.prior_generation
        || receipt.generation != command.prior_generation.saturating_add(1)
        || receipt.state_hash != command.state_hash
        || receipt.action_digest != command.action_digest
        || receipt.authorization_digest != command.authorization_digest
        || receipt.idempotency_key != command.idempotency_key
        || receipt.receipt_id.is_empty()
        || plugin_node_state_persistence_receipt_digest(receipt)? != receipt.receipt_digest
    {
        return Err(PluginNodeStatePersistenceError::InvalidReceipt);
    }
    Ok(())
}

fn map_state_persistence_data_error(
    command: &PersistPluginNodeStateCommand,
    error: PluginDataError,
) -> PluginNodeStatePersistenceError {
    match error {
        PluginDataError::StatePersistenceUnsupported => {
            PluginNodeStatePersistenceError::Unsupported
        }
        PluginDataError::UnsupportedStateScope => PluginNodeStatePersistenceError::UnsupportedScope,
        PluginDataError::StaleStateGeneration => PluginNodeStatePersistenceError::StaleGeneration,
        PluginDataError::StateConflict => PluginNodeStatePersistenceError::Conflict,
        PluginDataError::Cancelled => PluginNodeStatePersistenceError::Cancelled,
        PluginDataError::AmbiguousStatePersistence { .. } => {
            PluginNodeStatePersistenceError::Ambiguous {
                plugin_id: command.plugin_id.clone(),
                invocation_id: command.invocation_id.clone(),
                idempotency_key: command.idempotency_key.clone(),
            }
        }
        other => PluginNodeStatePersistenceError::Data(other),
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginNodeStatePersistenceError {
    #[error("plugin-node state persistence command is invalid")]
    InvalidCommand,
    #[error("plugin-node preserved state is invalid or unbounded")]
    InvalidState,
    #[error("plugin-node state action or authorization digest is invalid")]
    InvalidDigest,
    #[error("plugin-node state receipt identity is invalid")]
    InvalidReceipt,
    #[error("plugin-host protocol has no durable plugin-node state receipt")]
    Unsupported,
    #[error("plugin-node state scope lacks an exact canonical persistence identity")]
    UnsupportedScope,
    #[error("plugin-node state generation is stale")]
    StaleGeneration,
    #[error("plugin-node state conflicts with a prior idempotent write")]
    Conflict,
    #[error("plugin-node state persistence was cancelled")]
    Cancelled,
    #[error(
        "plugin-node state persistence is ambiguous for `{plugin_id}` invocation `{invocation_id}` idempotency `{idempotency_key}`"
    )]
    Ambiguous {
        plugin_id: String,
        invocation_id: String,
        idempotency_key: String,
    },
    #[error("plugin-node state data operation failed: {0}")]
    Data(PluginDataError),
}

#[async_trait]
impl<D> PluginCompositionLogicPort for PluginCompositionLogic<D>
where
    D: Clone + PluginDataPort + Send + Sync + 'static,
{
    async fn compose_pipeline(
        &self,
        command: ComposePluginPipelineCommand,
    ) -> Result<ComposedPluginPipeline, PluginCompositionError> {
        let declarations = command
            .compiled_style
            .interceptors
            .iter()
            .filter(|declaration| !runtime_owned(&declaration.owner))
            .cloned()
            .collect::<Vec<_>>();
        for declaration in &declarations {
            if declaration.supported_decisions.iter().any(|decision| {
                !matches!(
                    decision,
                    DecisionCapability::Continue
                        | DecisionCapability::Replace
                        | DecisionCapability::Reject
                )
            }) {
                return Err(PluginCompositionError::UnsupportedDecision);
            }
        }
        let activated = self
            .data
            .activate_plugins(ActivatePluginsDataRequest {
                session_id: command.session_id.clone(),
                plugin_ids: command.compiled_style.allowed_plugins.clone(),
                runtime_api_version: command.runtime_api_version,
                capabilities: command
                    .compiled_style
                    .required_capabilities
                    .iter()
                    .cloned()
                    .collect(),
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginCompositionError::Data)?;
        let activated_plugin_ids = activated.plugin_ids;
        let plugins: BTreeMap<_, _> = activated
            .plugins
            .into_iter()
            .map(|plugin| (plugin.id.clone(), plugin))
            .collect();
        let mut builder = BlockingPipelineBuilder::new();
        for declaration in declarations {
            let plugin = plugins
                .get(&declaration.owner)
                .ok_or(PluginCompositionError::Unavailable)?
                .clone();
            if plugin.class != "blocking"
                || !plugin.subscribed_events.contains(&declaration.event)
                || plugin.timeout_ms == 0
            {
                return Err(PluginCompositionError::Incompatible);
            }
            builder.register(InterceptorRegistration::new(
                ordering(&declaration),
                Duration::from_millis(plugin.timeout_ms),
                failure_policy(&plugin)?,
                Arc::new(RuntimePluginInterceptor {
                    data: self.data.clone(),
                    session_id: command.session_id.clone(),
                    run_id: command.cancellation_id.clone(),
                    cancellation_id: command.cancellation_id.clone(),
                    plugin_id: plugin.id,
                    plugin_version: plugin.version,
                    declaration_hash: ContentHash::digest(
                        &serde_json::to_vec(&declaration)
                            .map_err(|_| PluginCompositionError::Incompatible)?,
                    ),
                    declaration,
                }),
            ));
        }
        let pipeline = builder
            .compile()
            .map(Arc::new)
            .map_err(|_| PluginCompositionError::Ordering)?;
        Ok(ComposedPluginPipeline {
            pipeline,
            activated_plugin_ids,
        })
    }

    async fn plan_observer_deliveries(
        &self,
        command: ObserveCommittedPluginEventsCommand,
    ) -> Result<Vec<PlannedObserverDelivery>, PluginCompositionError> {
        if command.events.is_empty() {
            return Ok(Vec::new());
        }
        let activated = self
            .data
            .activate_plugins(ActivatePluginsDataRequest {
                session_id: command.session_id.clone(),
                plugin_ids: command.compiled_style.allowed_plugins.clone(),
                runtime_api_version: command.runtime_api_version,
                capabilities: command
                    .compiled_style
                    .required_capabilities
                    .iter()
                    .cloned()
                    .collect(),
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginCompositionError::Data)?;
        let observers = activated
            .plugins
            .into_iter()
            .filter(|plugin| plugin.class == "observer")
            .collect::<Vec<_>>();
        let coordinator = ObserverDeliveryCoordinator::new(self.data.clone());
        let mut planned = Vec::new();
        for event in &command.events {
            for observer in observers
                .iter()
                .filter(|plugin| plugin.subscribed_events.contains(&event.event_type))
            {
                let (proposed, prepared) = coordinator
                    .propose(
                        command.session_id.clone(),
                        command.cancellation_id.clone(),
                        ObserverDeclaration {
                            plugin_id: observer.id.clone(),
                            plugin_version: observer.version.clone(),
                            handler: format!("observe:{}", event.event_type),
                            declaration_hash: observer.declaration_hash,
                            configuration_reference: observer.configuration_reference,
                        },
                        event,
                    )
                    .map_err(PluginCompositionError::Observer)?;
                planned.push(PlannedObserverDelivery { proposed, prepared });
            }
        }
        Ok(planned)
    }

    fn observer_dispatch_intent(
        &self,
        state: &SessionState,
        prepared: &PreparedObserverDelivery,
    ) -> Result<RuntimeCommittedEvent, PluginCompositionError> {
        ObserverDeliveryCoordinator::new(self.data.clone())
            .dispatch_intent(state, prepared)
            .map_err(PluginCompositionError::Observer)
    }

    async fn reconcile_observer_receipt(
        &self,
        state: &SessionState,
        prepared: &PreparedObserverDelivery,
    ) -> Result<RuntimeCommittedEvent, PluginCompositionError> {
        ObserverDeliveryCoordinator::new(self.data.clone())
            .reconcile_exact_receipt(state, prepared)
            .await
            .map_err(PluginCompositionError::Observer)
    }

    async fn teardown_host_if_idle(
        &self,
        command: TeardownPluginHostCommand,
    ) -> Result<bool, PluginCompositionError> {
        self.data
            .teardown_host_if_idle(
                agentmod_runtime_data::plugin::TeardownPluginHostDataRequest {
                    session_id: command.session_id,
                    active_continuations: command.active_continuations,
                    pending_observer_deliveries: command.pending_observer_deliveries,
                },
            )
            .await
            .map_err(PluginCompositionError::Data)
    }
}

struct RuntimePluginInterceptor<D> {
    data: D,
    session_id: String,
    run_id: String,
    cancellation_id: String,
    plugin_id: String,
    plugin_version: String,
    declaration_hash: ContentHash,
    declaration: InterceptorDeclaration,
}

#[async_trait]
impl<D> BlockingInterceptor<ActionProposal> for RuntimePluginInterceptor<D>
where
    D: PluginDataPort + Send + Sync,
{
    async fn intercept(
        &self,
        proposal: ActionProposal,
    ) -> Result<Decision<ActionProposal>, InterceptorError> {
        if self.declaration.event != "action.proposed"
            && self.declaration.event != format!("{}.proposed", proposal.action.kind())
        {
            return Ok(Decision::Continue(proposal));
        }
        let value = serde_json::to_value(&proposal)
            .map_err(|_| InterceptorError::new("plugin proposal serialization failed"))?;
        let invocation_id = uuid::Uuid::now_v7().to_string();
        let readable_state = json!({
            "session_id": self.session_id,
            "style": proposal.style,
            "workspace": proposal.workspace,
        });
        let request_hash = serde_json::to_vec(&(
            "agentmod.plugin.interceptor.request.v1",
            &self.plugin_id,
            &invocation_id,
            &self.declaration.id,
            &self.declaration.event,
            &value,
            &readable_state,
        ))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| InterceptorError::new("plugin proposal hashing failed"))?;
        let cancellation_target = plugin_invocation_cancellation_target(
            &self.session_id,
            &self.run_id,
            &self.plugin_id,
            &self.plugin_version,
            &invocation_id,
            &self.declaration.id,
            self.declaration_hash,
            request_hash,
        )
        .map_err(|_| InterceptorError::new("plugin cancellation identity failed"))?;
        let decision = self
            .data
            .invoke_plugin(InvokePluginDataRequest {
                cancellation_target: map_cancellation_target(&cancellation_target),
                session_id: self.session_id.clone(),
                plugin_id: self.plugin_id.clone(),
                invocation_id,
                handler: self.declaration.id.clone(),
                proposal_type: self.declaration.event.clone(),
                proposal: value,
                readable_state,
                cancellation_id: self.cancellation_id.clone(),
            })
            .await
            .map_err(|error| InterceptorError::new(error.to_string()))?;
        match decision {
            PluginDecisionDataRecord::Continue(value) => {
                let returned = decode_proposal(value)?;
                validate_identity(&proposal, &returned)?;
                Ok(Decision::Continue(returned))
            }
            PluginDecisionDataRecord::Replace(value) => {
                let returned = decode_proposal(value)?;
                validate_identity(&proposal, &returned)?;
                Ok(Decision::Replace(returned))
            }
            PluginDecisionDataRecord::Reject(reason) => Ok(Decision::Reject { reason }),
        }
    }
}

fn decode_proposal(value: serde_json::Value) -> Result<ActionProposal, InterceptorError> {
    serde_json::from_value(value)
        .map_err(|_| InterceptorError::new("plugin returned an invalid typed proposal"))
}

fn validate_identity(
    original: &ActionProposal,
    returned: &ActionProposal,
) -> Result<(), InterceptorError> {
    if original.id != returned.id
        || original.style != returned.style
        || original.workspace != returned.workspace
    {
        return Err(InterceptorError::new(
            "plugin changed immutable proposal identity or scope",
        ));
    }
    Ok(())
}

fn runtime_owned(owner: &str) -> bool {
    owner == "runtime" || owner.starts_with("runtime.")
}

fn ordering(declaration: &InterceptorDeclaration) -> OrderingSpec {
    let mut ordering = OrderingSpec::new(declaration.id.as_str(), declaration.owner.as_str())
        .with_stage(declaration.stage)
        .with_priority(declaration.priority);
    for before in &declaration.before {
        ordering = ordering.before(before.as_str());
    }
    for after in &declaration.after {
        ordering = ordering.after(after.as_str());
    }
    ordering
}

fn failure_policy(
    plugin: &ActivatedPluginDataRecord,
) -> Result<FailurePolicy, PluginCompositionError> {
    match plugin.failure_policy.as_str() {
        "reject" => Ok(FailurePolicy::Reject),
        "cancel" => Ok(FailurePolicy::Cancel),
        "continue" => Ok(FailurePolicy::ContinueUnchanged),
        "retry" | "disable" => Ok(FailurePolicy::Abort),
        _ => Err(PluginCompositionError::Incompatible),
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginCompositionError {
    #[error("plugin data operation failed: {0}")]
    Data(PluginDataError),
    #[error("style-selected plugin is unavailable")]
    Unavailable,
    #[error("style-selected plugin is incompatible with its declaration")]
    Incompatible,
    #[error("plugin interceptor requests an unsupported decision")]
    UnsupportedDecision,
    #[error("plugin interceptor ordering is invalid")]
    Ordering,
    #[error("canonical observer coordination is required")]
    ObserverCoordination,
    #[error("canonical observer coordination failed: {0}")]
    Observer(ObserverDeliveryError),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    };

    use agentmod_event_pipeline::{ActionCapabilities, ExecutionOutcome};
    use agentmod_primitives::ContentHash;
    use agentmod_runtime_data::plugin::{
        ActivatedPluginsDataRecord, InvokePluginNodeExecutorDataRequest,
        LoadPluginNodeStateDataRequest, LoadedPluginNodeStateDataRecord, ObservePluginDataRequest,
        PersistPluginNodeStateDataRequest, PluginNodeExecutorDataRecord,
        PluginNodeOutcomeDataRecord, PluginNodeStateReadReceiptDataRecord,
        PluginNodeStateReceiptDataRecord, PluginObservationDataRecord,
    };
    use agentmod_session_style_sdk::{
        BuiltInStyle, CompileContext, DecisionCapability, InterceptorDeclaration,
        StyleCompilerLimits, built_in_manifest_for_version, compile_style,
    };
    use serde_json::json;

    use crate::action::{ConsequentialAction, ProposalId, ToolCallAction};

    use super::*;

    #[derive(Clone, Debug, Default)]
    enum StateFixtureMode {
        #[default]
        Success,
        Substitute,
        Error(PluginDataError),
    }

    #[derive(Clone, Default)]
    struct FixtureData {
        invocations: Arc<Mutex<Vec<InvokePluginDataRequest>>>,
        observations: Arc<Mutex<Vec<ObservePluginDataRequest>>>,
        node_invocations: Arc<Mutex<Vec<InvokePluginNodeExecutorDataRequest>>>,
        state_persistences: Arc<Mutex<Vec<PersistPluginNodeStateDataRequest>>>,
        state_reads: Arc<Mutex<Vec<LoadPluginNodeStateDataRequest>>>,
        state_mode: Arc<Mutex<StateFixtureMode>>,
    }

    #[async_trait]
    impl PluginDataPort for FixtureData {
        fn plugin_configuration_reference(
            &self,
            plugin_id: &str,
        ) -> Result<ContentHash, PluginDataError> {
            if plugin_id != "fixture.node" {
                return Err(PluginDataError::Invalid);
            }
            Ok(ContentHash::digest(b"plugin-configuration"))
        }

        fn node_executor_declaration(
            &self,
            plugin_id: &str,
            executor_id: &str,
            executor_version: &str,
            node_kind: &str,
        ) -> Result<PluginNodeExecutorDataRecord, PluginDataError> {
            if (plugin_id, executor_id, executor_version, node_kind)
                != ("fixture.node", "fixture.echo", "2.1.0", "model_call")
            {
                return Err(PluginDataError::Invalid);
            }
            Ok(PluginNodeExecutorDataRecord {
                plugin_version: String::from("1.0.0"),
                executor_id: executor_id.into(),
                version: executor_version.into(),
                runtime_api: "^1.0".into(),
                node_kind: node_kind.into(),
                handler: "execute_echo".into(),
                capabilities: BTreeSet::from(["model".into(), "node.echo".into()]),
                input_schema: r#"{"type":"object","required":["value"]}"#.into(),
                output_schema: r#"{"type":"object","required":["echo"]}"#.into(),
                timeout_ms: 500,
                failure_policy: "reject".into(),
                max_attempts: 1,
                retry_backoff_ms: 0,
                idempotent: false,
                tool_permissions: BTreeSet::new(),
                network_permissions: BTreeSet::new(),
                state_scope: "invocation".into(),
                external_effects: false,
                declaration_hash: ContentHash::digest(b"fixture-node-declaration"),
            })
        }

        async fn activate_plugins(
            &self,
            request: ActivatePluginsDataRequest,
        ) -> Result<ActivatedPluginsDataRecord, PluginDataError> {
            Ok(ActivatedPluginsDataRecord {
                plugin_ids: request.plugin_ids.clone(),
                plugins: request
                    .plugin_ids
                    .into_iter()
                    .map(|id| ActivatedPluginDataRecord {
                        declaration_hash: ContentHash::digest(id.as_bytes()),
                        configuration_reference: ContentHash::digest(b"fixture-config"),
                        class: if id == "fixture.observer" {
                            String::from("observer")
                        } else {
                            String::from("blocking")
                        },
                        subscribed_events: if id == "fixture.observer" {
                            BTreeSet::from([String::from("tool.execution_completed")])
                        } else {
                            BTreeSet::from([String::from("action.proposed")])
                        },
                        timeout_ms: 1_000,
                        failure_policy: if id == "fixture.observer" {
                            String::from("continue")
                        } else {
                            String::from("reject")
                        },
                        version: String::from("1.0.0"),
                        id,
                    })
                    .collect(),
            })
        }

        async fn invoke_plugin(
            &self,
            request: InvokePluginDataRequest,
        ) -> Result<PluginDecisionDataRecord, PluginDataError> {
            self.invocations
                .lock()
                .expect("invocations")
                .push(request.clone());
            let mut proposal: ActionProposal =
                serde_json::from_value(request.proposal).expect("typed proposal");
            let ConsequentialAction::ToolCall(action) = &mut proposal.action else {
                panic!("tool call")
            };
            action.arguments = json!({"path":"rewritten.txt"});
            Ok(PluginDecisionDataRecord::Replace(
                serde_json::to_value(proposal).expect("proposal json"),
            ))
        }

        async fn observe_event(
            &self,
            request: agentmod_runtime_data::plugin::ObservePluginDataRequest,
        ) -> Result<PluginObservationDataRecord, PluginDataError> {
            self.observations
                .lock()
                .expect("observations")
                .push(request);
            Ok(PluginObservationDataRecord {
                accepted: true,
                queue_depth: 0,
                dropped: 0,
                status:
                    agentmod_runtime_data::plugin::PluginObserverDeliveryStatusDataRecord::Completed,
                request_hash: "0".repeat(64),
                receipt_id: String::from("observer-receipt"),
                receipt_digest: "0".repeat(64),
                replayed: false,
            })
        }

        async fn invoke_node_executor(
            &self,
            request: InvokePluginNodeExecutorDataRequest,
        ) -> Result<PluginNodeOutcomeDataRecord, PluginDataError> {
            self.node_invocations
                .lock()
                .expect("node invocations")
                .push(request);
            Ok(PluginNodeOutcomeDataRecord {
                output: json!({"echo":true}),
                preserved_state: json!({"cursor":1}),
                proposed_actions: Vec::new(),
                attempts: 1,
            })
        }

        async fn persist_plugin_node_state(
            &self,
            request: PersistPluginNodeStateDataRequest,
        ) -> Result<PluginNodeStateReceiptDataRecord, PluginDataError> {
            self.state_persistences
                .lock()
                .expect("state persistences")
                .push(request.clone());
            let mode = self.state_mode.lock().expect("state mode").clone();
            if let StateFixtureMode::Error(error) = mode {
                return Err(error);
            }
            let mut receipt = PluginNodeStatePersistenceReceipt {
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                executor_declaration_hash: request.executor_declaration_hash,
                state_scope: unmap_logic_state_scope(request.state_scope),
                prior_generation: request.prior_generation,
                generation: request.prior_generation + 1,
                state_hash: request.state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                idempotency_key: request.idempotency_key,
                receipt_id: String::from("state-receipt-1"),
                receipt_digest: ContentHash::digest(b"pending"),
                replayed: false,
            };
            if matches!(mode, StateFixtureMode::Substitute) {
                receipt.plugin_id = String::from("substituted.plugin");
            }
            receipt.receipt_digest =
                plugin_node_state_persistence_receipt_digest(&receipt).expect("receipt digest");
            Ok(PluginNodeStateReceiptDataRecord {
                plugin_id: receipt.plugin_id,
                invocation_id: receipt.invocation_id,
                invocation_digest: receipt.invocation_digest,
                executor_id: receipt.executor_id,
                executor_version: receipt.executor_version,
                executor_declaration_hash: receipt.executor_declaration_hash,
                state_scope: map_logic_state_scope(receipt.state_scope),
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

        async fn load_plugin_node_state(
            &self,
            request: LoadPluginNodeStateDataRequest,
        ) -> Result<LoadedPluginNodeStateDataRecord, PluginDataError> {
            self.state_reads
                .lock()
                .expect("state reads")
                .push(request.clone());
            let state = json!({"cursor": 1});
            let mut receipt = PluginNodeStateReadReceipt {
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                executor_declaration_hash: request.executor_declaration_hash,
                state_scope: unmap_logic_state_scope(request.state_scope),
                generation: request.expected_generation,
                state_hash: request.expected_state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                idempotency_key: request.idempotency_key,
                receipt_id: String::from("state-read-receipt-1"),
                receipt_digest: ContentHash::digest(b"pending"),
                replayed: false,
            };
            receipt.receipt_digest =
                plugin_node_state_read_receipt_digest(&receipt).expect("read receipt digest");
            Ok(LoadedPluginNodeStateDataRecord {
                state,
                receipt: PluginNodeStateReadReceiptDataRecord {
                    plugin_id: receipt.plugin_id,
                    invocation_id: receipt.invocation_id,
                    invocation_digest: receipt.invocation_digest,
                    executor_id: receipt.executor_id,
                    executor_version: receipt.executor_version,
                    executor_declaration_hash: receipt.executor_declaration_hash,
                    state_scope: map_logic_state_scope(receipt.state_scope),
                    generation: receipt.generation,
                    state_hash: receipt.state_hash,
                    action_digest: receipt.action_digest,
                    authorization_digest: receipt.authorization_digest,
                    idempotency_key: receipt.idempotency_key,
                    receipt_id: receipt.receipt_id,
                    receipt_digest: receipt.receipt_digest,
                    replayed: receipt.replayed,
                },
            })
        }
    }

    fn compiled_style() -> CompiledSessionStyle {
        let mut manifest = built_in_manifest_for_version(BuiltInStyle::PersistentChat, "1.1.0")
            .expect("frozen persistent manifest");
        manifest.allowed_plugins = vec![String::from("fixture.rewriter")];
        manifest.interceptors = vec![InterceptorDeclaration {
            id: String::from("rewrite-tool"),
            owner: String::from("fixture.rewriter"),
            event: String::from("action.proposed"),
            stage: 10,
            priority: 5,
            before: Vec::new(),
            after: Vec::new(),
            supported_decisions: vec![
                DecisionCapability::Continue,
                DecisionCapability::Replace,
                DecisionCapability::Reject,
            ],
            required_capabilities: Vec::new(),
        }];
        compile_style(
            &manifest,
            &CompileContext {
                runtime_api_version: String::from("1.0.0"),
                plugin_set_hash: ContentHash::digest(b"fixture"),
                capabilities: [
                    "agents",
                    "approval",
                    "artifacts",
                    "context",
                    "continuations",
                    "events",
                    "model",
                    "scheduling",
                    "tools",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                tool_groups: BTreeMap::from([
                    (
                        String::from("filesystem"),
                        BTreeSet::from([String::from("filesystem.read")]),
                    ),
                    (
                        String::from("process"),
                        BTreeSet::from([String::from("process.run")]),
                    ),
                ]),
                providers: BTreeSet::from([String::from("mock")]),
                plugins: BTreeSet::from([String::from("fixture.rewriter")]),
                context_transforms: Vec::new(),
                plugin_memory_providers: Vec::new(),
                plugin_compactors: Vec::new(),
                memory_providers: ["none", "file", "sqlite-fts"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                compaction_strategies: BTreeSet::from([
                    String::from("artifact_handoff"),
                    String::from("none"),
                    String::from("sliding_window"),
                    String::from("summary"),
                    String::from("tool_output_eviction"),
                ]),
                supported_decisions: BTreeSet::from([
                    DecisionCapability::Continue,
                    DecisionCapability::Replace,
                    DecisionCapability::Reject,
                    DecisionCapability::RequireApproval,
                    DecisionCapability::Defer,
                    DecisionCapability::Cancel,
                    DecisionCapability::Fork,
                ]),
                graph_references: BTreeMap::new(),
            },
            StyleCompilerLimits::default(),
        )
        .expect("compiled style")
    }

    #[tokio::test]
    async fn style_selected_plugin_rewrites_typed_proposal_in_compiled_order() {
        let data = FixtureData::default();
        let pipeline = PluginCompositionLogic::new(data.clone())
            .compose_pipeline(ComposePluginPipelineCommand {
                session_id: String::from("01900000-0000-7000-8000-000000000001"),
                cancellation_id: String::from("01900000-0000-7000-8000-000000000002"),
                compiled_style: compiled_style(),
                runtime_api_version: String::from("1.0.0"),
            })
            .await
            .expect("pipeline")
            .pipeline;
        let proposal = ActionProposal {
            id: ProposalId(String::from("proposal-1")),
            action: ConsequentialAction::ToolCall(ToolCallAction {
                tool: String::from("filesystem.read"),
                group: String::from("filesystem"),
                arguments: json!({"path":"original.txt"}),
                source: None,
            }),
            style: String::from("persistent-chat"),
            workspace: String::from("repo"),
            origin: String::from("runtime"),
        };
        let report = pipeline.execute(proposal, ActionCapabilities::all()).await;
        assert!(matches!(
            report.steps[0].result,
            agentmod_event_pipeline::ExecutionStepResult::Decision(Decision::Replace(_))
        ));
        let ExecutionOutcome::Decision(Decision::Continue(rewritten)) = report.outcome else {
            panic!("transformed continuation")
        };
        let ConsequentialAction::ToolCall(action) = rewritten.action else {
            panic!("tool")
        };
        assert_eq!(action.arguments, json!({"path":"rewritten.txt"}));
        assert_eq!(data.invocations.lock().expect("invocations").len(), 1);
    }

    #[tokio::test]
    async fn observer_receives_only_matching_committed_events() {
        let data = FixtureData::default();
        let mut style = compiled_style();
        style.allowed_plugins.push(String::from("fixture.observer"));
        let plans = PluginCompositionLogic::new(data.clone())
            .plan_observer_deliveries(ObserveCommittedPluginEventsCommand {
                session_id: String::from("01900000-0000-7000-8000-000000000001"),
                cancellation_id: String::from("observer-range-2"),
                compiled_style: style,
                runtime_api_version: String::from("0.1.0"),
                events: vec![
                    CommittedPluginEvent {
                        event_id: String::from("01900000-0000-7000-8000-000000000010"),
                        sequence: 1,
                        event_type: String::from("model.request_started"),
                        payload: json!({"event":"model_request_started"}),
                    },
                    CommittedPluginEvent {
                        event_id: String::from("01900000-0000-7000-8000-000000000011"),
                        sequence: 2,
                        event_type: String::from("tool.execution_completed"),
                        payload: json!({"event":"tool_execution_completed"}),
                    },
                ],
            })
            .await
            .expect("observer planning");
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].prepared.event_type, "tool.execution_completed");
        assert!(matches!(
            plans[0].proposed,
            RuntimeCommittedEvent::PluginObserverDeliveryProposed(_)
        ));
        assert!(
            data.observations.lock().expect("observations").is_empty(),
            "planning must not dispatch the observer"
        );
    }

    #[tokio::test]
    async fn plugin_node_invocation_binds_full_work_and_exact_declaration() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        let configuration_reference = ContentHash::digest(b"node-configuration");
        let command = ExecutePluginNodeCommand {
            session_id: String::from("01900000-0000-7000-8000-000000000001"),
            work: NodeWorkIdentity {
                run_id: String::from("run-1"),
                node_id: String::from("model"),
                branch_path: vec![String::from("branch-b")],
                attempt: 2,
                loop_iteration: 3,
                step: 7,
            },
            executor: ResolvedNodeExecutor {
                node_id: String::from("model"),
                node_kind: String::from("model_call"),
                implementation_id: String::from("fixture.echo"),
                implementation_version: String::from("2.1.0"),
                source: NodeExecutorSource::Plugin {
                    plugin_id: String::from("fixture.node"),
                },
                boundary: NodeExecutorBoundary::PluginHost,
                required_capabilities: BTreeSet::from([String::from("node.echo")]),
                resolved_capabilities: BTreeSet::from([
                    String::from("model"),
                    String::from("node.echo"),
                ]),
                runtime_api_requirement: String::from("^1.0"),
                executor_declaration_hash: ContentHash::digest(b"fixture-node-declaration"),
                adapter_configuration_reference: configuration_reference,
            },
            adapter_configuration_reference: configuration_reference,
            input: json!({"value":42}),
            readable_state: json!({"session":{"classification":"internal"}}),
            cancellation_id: String::from("cancel-node-1"),
        };
        let first = logic
            .execute_plugin_node(command.clone())
            .await
            .expect("plugin node");
        let second = logic
            .execute_plugin_node(command)
            .await
            .expect("same exact work");
        assert_eq!(first.invocation_digest, second.invocation_digest);
        assert_eq!(first.invocation_id, second.invocation_id);
        assert_eq!(first.output, json!({"echo":true}));
        assert_eq!(first.preserved_state, json!({"cursor":1}));
        let invocations = data.node_invocations.lock().expect("node invocations");
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[0].executor_id, "fixture.echo");
        assert_eq!(invocations[0].executor_version, "2.1.0");
        assert_eq!(
            invocations[0].configuration_reference,
            ContentHash::digest(b"plugin-configuration")
        );
        assert_ne!(
            invocations[0].configuration_reference,
            configuration_reference
        );
    }

    fn state_persistence_command() -> PersistPluginNodeStateCommand {
        let state = json!({"cursor": 2});
        let state_hash = plugin_node_state_value_hash(&state).expect("state hash");
        let executor_declaration_hash = ContentHash::digest(b"fixture-node-declaration");
        let cancellation_target = plugin_invocation_cancellation_target(
            "session-1",
            "run-1",
            "fixture.node",
            "1.0.0",
            "plugin-node:invocation",
            "fixture.echo:state-write",
            executor_declaration_hash,
            state_hash,
        )
        .expect("cancellation target");
        let mut command = PersistPluginNodeStateCommand {
            cancellation_target,
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.node"),
            invocation_id: String::from("plugin-node:invocation"),
            invocation_digest: ContentHash::digest(b"invocation"),
            executor_id: String::from("fixture.echo"),
            executor_version: String::from("2.1.0"),
            executor_declaration_hash,
            configuration_reference: ContentHash::digest(b"node-configuration"),
            state_scope: PluginNodeStateScope::Invocation,
            prior_generation: 1,
            prior_state_hash: Some(ContentHash::digest(b"prior")),
            state_hash,
            state,
            action_digest: ContentHash::digest(b"pending-action"),
            authorization_digest: ContentHash::digest(b"pending-authorization"),
            nonce: String::from("nonce-1"),
            cancellation_id: String::from("cancel-1"),
            idempotency_key: String::from("state-write-1"),
        };
        let request_hash =
            plugin_node_state_persistence_request_hash(&command).expect("request hash");
        command.cancellation_target = plugin_invocation_cancellation_target(
            "session-1",
            "run-1",
            "fixture.node",
            "1.0.0",
            "plugin-node:invocation",
            "fixture.echo:state-write",
            executor_declaration_hash,
            request_hash,
        )
        .expect("cancellation target");
        let (action, authorization) =
            plugin_node_state_persistence_digests(&command).expect("digests");
        command.action_digest = action;
        command.authorization_digest = authorization;
        command
    }

    fn state_read_command() -> LoadPluginNodeStateCommand {
        let state = json!({"cursor": 1});
        let expected_state_hash = plugin_node_state_value_hash(&state).expect("state hash");
        let executor_declaration_hash = ContentHash::digest(b"fixture-node-declaration");
        let mut command = LoadPluginNodeStateCommand {
            cancellation_target: plugin_invocation_cancellation_target(
                "session-1",
                "run-1",
                "fixture.node",
                "1.0.0",
                "plugin-node:later-invocation",
                "fixture.echo:state-read",
                executor_declaration_hash,
                ContentHash::from_bytes([0; 32]),
            )
            .expect("provisional cancellation target"),
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.node"),
            invocation_id: String::from("plugin-node:later-invocation"),
            invocation_digest: ContentHash::digest(b"later-invocation"),
            executor_id: String::from("fixture.echo"),
            executor_version: String::from("2.1.0"),
            executor_declaration_hash,
            configuration_reference: ContentHash::digest(b"node-configuration"),
            state_scope: PluginNodeStateScope::Session,
            expected_generation: 1,
            expected_state_hash,
            action_digest: ContentHash::digest(b"pending-read-action"),
            authorization_digest: ContentHash::digest(b"pending-read-authorization"),
            nonce: String::from("read-nonce-1"),
            cancellation_id: String::from("read-cancel-1"),
            idempotency_key: String::from("state-read-1"),
        };
        let request_hash =
            plugin_node_state_read_request_hash(&command).expect("state-read request hash");
        command.cancellation_target = plugin_invocation_cancellation_target(
            "session-1",
            "run-1",
            "fixture.node",
            "1.0.0",
            "plugin-node:later-invocation",
            "fixture.echo:state-read",
            executor_declaration_hash,
            request_hash,
        )
        .expect("cancellation target");
        let (action, authorization) =
            plugin_node_state_read_digests(&command).expect("read digests");
        command.action_digest = action;
        command.authorization_digest = authorization;
        command
    }

    #[tokio::test]
    async fn state_read_validates_exact_identity_and_keeps_raw_value_outside_receipt() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        let loaded = logic
            .load_plugin_node_state(state_read_command())
            .await
            .expect("loaded state");
        assert_eq!(loaded.state, json!({"cursor": 1}));
        assert_eq!(loaded.receipt.generation, 1);
        assert_eq!(data.state_reads.lock().expect("state reads").len(), 1);
        assert!(!format!("{:?}", loaded.receipt).contains("cursor"));

        let mut invalid = state_read_command();
        invalid.expected_state_hash = ContentHash::digest(b"substituted");
        assert_eq!(
            logic.load_plugin_node_state(invalid).await,
            Err(PluginNodeStateReadError::InvalidCommand)
        );
        assert_eq!(
            data.state_reads.lock().expect("state reads").len(),
            1,
            "a request-hash substitution invalidates the authenticated cancellation target before \
             the data boundary"
        );

        for scope in [
            PluginNodeStateScope::ModelCall,
            PluginNodeStateScope::Turn,
            PluginNodeStateScope::Project,
            PluginNodeStateScope::User,
            PluginNodeStateScope::Runtime,
        ] {
            let mut unsupported = state_read_command();
            unsupported.state_scope = scope;
            assert_eq!(
                logic.load_plugin_node_state(unsupported).await,
                Err(PluginNodeStateReadError::UnsupportedScope)
            );
        }
        assert_eq!(
            data.state_reads.lock().expect("state reads").len(),
            1,
            "unsupported scopes must fail before the data boundary"
        );
    }

    #[tokio::test]
    async fn state_persistence_validates_exact_command_and_terminal_receipt() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        let command = state_persistence_command();
        let first = logic
            .persist_plugin_node_state(command.clone())
            .await
            .expect("terminal receipt");
        let second = logic
            .persist_plugin_node_state(command)
            .await
            .expect("idempotent terminal receipt");
        assert_eq!(first, second);
        assert_eq!(first.generation, 2);
        assert_eq!(
            data.state_persistences
                .lock()
                .expect("state persistences")
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn state_persistence_rejects_state_digest_and_receipt_substitution() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        let mut invalid = state_persistence_command();
        invalid.state_hash = ContentHash::digest(b"substituted");
        assert_eq!(
            logic.persist_plugin_node_state(invalid).await,
            Err(PluginNodeStatePersistenceError::InvalidCommand)
        );
        assert!(
            data.state_persistences
                .lock()
                .expect("state persistences")
                .is_empty()
        );

        *data.state_mode.lock().expect("state mode") = StateFixtureMode::Substitute;
        assert_eq!(
            logic
                .persist_plugin_node_state(state_persistence_command())
                .await,
            Err(PluginNodeStatePersistenceError::InvalidReceipt)
        );
    }

    #[tokio::test]
    async fn state_persistence_cancellation_target_binds_complete_request() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        let mut substituted = state_persistence_command();
        let state_hash = substituted.state_hash;
        substituted.configuration_reference = ContentHash::digest(b"substituted-configuration");
        let (action, authorization) =
            plugin_node_state_persistence_digests(&substituted).expect("substituted digests");
        substituted.action_digest = action;
        substituted.authorization_digest = authorization;
        assert_eq!(substituted.state_hash, state_hash);
        assert_eq!(
            logic.persist_plugin_node_state(substituted).await,
            Err(PluginNodeStatePersistenceError::InvalidCommand)
        );
        assert!(
            data.state_persistences
                .lock()
                .expect("state persistences")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn state_persistence_rejects_every_scope_without_a_canonical_identity() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        for scope in [
            PluginNodeStateScope::ModelCall,
            PluginNodeStateScope::Turn,
            PluginNodeStateScope::Project,
            PluginNodeStateScope::User,
            PluginNodeStateScope::Runtime,
        ] {
            let mut command = state_persistence_command();
            command.state_scope = scope;
            assert_eq!(
                logic.persist_plugin_node_state(command).await,
                Err(PluginNodeStatePersistenceError::UnsupportedScope),
                "scope {scope:?} must fail before the data boundary"
            );
        }
        assert!(
            data.state_persistences
                .lock()
                .expect("state persistences")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn state_persistence_maps_stale_cancel_unsupported_and_ambiguous_stably() {
        let data = FixtureData::default();
        let logic = PluginCompositionLogic::new(data.clone());
        for (source, expected) in [
            (
                PluginDataError::StaleStateGeneration,
                PluginNodeStatePersistenceError::StaleGeneration,
            ),
            (
                PluginDataError::StateConflict,
                PluginNodeStatePersistenceError::Conflict,
            ),
            (
                PluginDataError::Cancelled,
                PluginNodeStatePersistenceError::Cancelled,
            ),
            (
                PluginDataError::StatePersistenceUnsupported,
                PluginNodeStatePersistenceError::Unsupported,
            ),
        ] {
            *data.state_mode.lock().expect("state mode") = StateFixtureMode::Error(source);
            assert_eq!(
                logic
                    .persist_plugin_node_state(state_persistence_command())
                    .await,
                Err(expected)
            );
        }
        *data.state_mode.lock().expect("state mode") =
            StateFixtureMode::Error(PluginDataError::AmbiguousStatePersistence {
                plugin_id: String::from("ignored"),
                invocation_id: String::from("ignored"),
                idempotency_key: String::from("ignored"),
            });
        assert!(matches!(
            logic
                .persist_plugin_node_state(state_persistence_command())
                .await,
            Err(PluginNodeStatePersistenceError::Ambiguous {
                plugin_id,
                invocation_id,
                idempotency_key,
            }) if plugin_id == "fixture.node"
                && invocation_id == "plugin-node:invocation"
                && idempotency_key == "state-write-1"
        ));
    }
}
