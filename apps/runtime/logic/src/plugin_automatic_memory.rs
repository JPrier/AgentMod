//! One-shot, receipt-backed execution for plugin-provided automatic memory writes.

use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::{Arc, Mutex},
};

use agentmod_primitives::{ContentHash, SessionId};
use agentmod_runtime_data::{
    plugin::{
        PluginArtifactReferenceDataRecord, PluginCanonicalReferenceDataRecord,
        PluginCanonicalReferenceKindData, PluginDataError, PluginDataPort, PluginMemoryScopeData,
        PluginMemoryWriteBoundaryData, PluginMemoryWriteInputDataRecord,
        PluginOperationBindingDataRecord, PluginSecurityClassificationData,
        WritePluginMemoryDataRequest,
    },
    plugin_receipt::{
        PluginInvocationReceiptDataIdentity, PluginNodeReceiptDataPort,
        StorePluginInvocationReceiptDataRequest,
    },
};
use async_trait::async_trait;
use semver::{Version as SemanticVersion, VersionReq};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::Mutex as AsyncMutex;

use crate::session::{
    AutomaticMemoryWriteIdentity, AutomaticMemoryWriteState, SealedPluginContextReceipt,
    SessionState,
};

const MAX_RECEIPT_BYTES: usize = 256 * 1024;
const MAX_IN_FLIGHT_INVOCATIONS: usize = 128;
const ZERO_HASH: ContentHash = ContentHash::from_bytes([0; 32]);

#[derive(Debug, Default)]
struct DispatchGateState {
    claimed: bool,
}

type DispatchGate = AsyncMutex<DispatchGateState>;
type DispatchGateMap = BTreeMap<(SessionId, String), Arc<DispatchGate>>;

/// Exact runtime-owned invocation supplied after replay validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparePluginAutomaticMemoryWriteCommand {
    /// Canonical owning session.
    pub session_id: SessionId,
    /// Exact replay-owned automatic-memory identity.
    pub identity: AutomaticMemoryWriteIdentity,
    /// Bounded content retained by the canonical outbox.
    pub content: String,
    /// Stable cancellation identity for the isolated invocation.
    pub cancellation_id: String,
    /// Digest of the exact approved consequential action.
    pub action_digest: ContentHash,
    /// Runtime API version bound by the immutable session style.
    pub runtime_api_version: String,
}

/// Opaque preflight ticket. Only this module can construct the exact data request.
#[derive(Debug)]
pub struct PreparedPluginAutomaticMemoryWrite {
    identity: AutomaticMemoryWriteIdentity,
    action_digest: ContentHash,
    request: Option<WritePluginMemoryDataRequest>,
    output_schema: String,
    ticket_count: Arc<Mutex<usize>>,
}

impl Drop for PreparedPluginAutomaticMemoryWrite {
    fn drop(&mut self) {
        if let Ok(mut count) = self.ticket_count.lock() {
            *count = count.saturating_sub(1);
        }
    }
}

/// A terminal plugin response durably sealed before journal completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedPluginAutomaticMemoryWrite {
    /// Provider-owned immutable result reference.
    pub reference: String,
    /// Whether the plugin reports retaining the value.
    pub retained: bool,
    /// Hash of the exact accepted value.
    pub value_hash: ContentHash,
    /// Durable tamper-evident receipt reference.
    pub terminal_receipt: SealedPluginContextReceipt,
}

/// Result of crossing the non-idempotent plugin boundary once.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginAutomaticMemoryWriteOutcome {
    /// A sealed terminal response is ready for canonical completion.
    Completed(CompletedPluginAutomaticMemoryWrite),
    /// The non-idempotent effect cannot be proven terminal.
    Ambiguous {
        /// Stable bounded ambiguity classification.
        code: String,
        /// Optional sealed diagnostic evidence.
        terminal_receipt: Option<SealedPluginContextReceipt>,
    },
}

/// Runtime-logic-owned port consumed by turn finalization.
#[async_trait]
pub trait PluginAutomaticMemoryWriteTurnPort: Send + Sync {
    /// Validates immutable selection and returns a one-use preflight ticket.
    ///
    /// # Errors
    ///
    /// Returns a stable validation, receipt, or in-flight classification when
    /// the exact persisted plugin selection cannot be prepared safely.
    fn prepare(
        &self,
        command: PreparePluginAutomaticMemoryWriteCommand,
    ) -> Result<PreparedPluginAutomaticMemoryWrite, PluginAutomaticMemoryWriteError>;

    /// Loads an exact terminal receipt without redispatching the plugin effect.
    ///
    /// # Errors
    ///
    /// Returns a stable validation or receipt classification when recovery
    /// cannot prove the exact persisted invocation terminal.
    fn recover_terminal_receipt(
        &self,
        command: &PreparePluginAutomaticMemoryWriteCommand,
    ) -> Result<Option<CompletedPluginAutomaticMemoryWrite>, PluginAutomaticMemoryWriteError>;

    /// Crosses the boundary once after verifying replayed dispatch evidence.
    async fn invoke_once(
        &self,
        state: &SessionState,
        prepared: PreparedPluginAutomaticMemoryWrite,
    ) -> Result<PluginAutomaticMemoryWriteOutcome, PluginAutomaticMemoryWriteError>;
}

/// Production coordinator over runtime data and the shared durable plugin receipt store.
#[derive(Clone, Debug)]
pub struct ProductionPluginAutomaticMemoryWriteTurn<D> {
    data: D,
    ticket_count: Arc<Mutex<usize>>,
    dispatch_gates: Arc<AsyncMutex<DispatchGateMap>>,
}

impl<D> ProductionPluginAutomaticMemoryWriteTurn<D> {
    /// Creates a production coordinator over runtime data.
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            ticket_count: Arc::new(Mutex::new(0)),
            dispatch_gates: Arc::new(AsyncMutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl<D> PluginAutomaticMemoryWriteTurnPort for ProductionPluginAutomaticMemoryWriteTurn<D>
where
    D: Clone + Send + Sync + PluginDataPort + PluginNodeReceiptDataPort + 'static,
{
    #[allow(
        clippy::too_many_lines,
        reason = "preflight validates the complete immutable declaration and constructs one exact hashed data request"
    )]
    fn prepare(
        &self,
        command: PreparePluginAutomaticMemoryWriteCommand,
    ) -> Result<PreparedPluginAutomaticMemoryWrite, PluginAutomaticMemoryWriteError> {
        validate_command(&command)?;
        let plugin = command
            .identity
            .plugin
            .as_ref()
            .ok_or(PluginAutomaticMemoryWriteError::InvalidBinding)?;
        let declaration = self
            .data
            .memory_provider_declaration(
                &plugin.plugin_id,
                &plugin.provider_id,
                &plugin.provider_version,
            )
            .map_err(|_| PluginAutomaticMemoryWriteError::InvalidBinding)?;
        let write = declaration
            .write
            .ok_or(PluginAutomaticMemoryWriteError::InvalidBinding)?;
        let runtime = SemanticVersion::parse(&command.runtime_api_version)
            .map_err(|_| PluginAutomaticMemoryWriteError::InvalidBinding)?;
        if declaration.provider_id != plugin.provider_id
            || declaration.version != plugin.provider_version
            || declaration.declaration_hash != plugin.declaration_hash
            || SemanticVersion::parse(&declaration.version).is_err()
            || VersionReq::parse(&declaration.runtime_api)
                .map_or(true, |requirement| !requirement.matches(&runtime))
            || declaration.capabilities.iter().any(|capability| {
                capability.trim().is_empty()
                    || capability.len() > 128
                    || capability.chars().any(char::is_whitespace)
            })
            || write.handler.trim().is_empty()
            || write.handler.len() > 256
            || serde_json::from_str::<Value>(&write.input_schema).is_err()
            || serde_json::from_str::<Value>(&write.output_schema).is_err()
            || write.timeout_ms == 0
            || write.max_attempts != 1
            || write.retry_backoff_ms != 0
            || !matches!(
                write.failure_policy.as_str(),
                "reject" | "cancel" | "disable" | "continue"
            )
            || !matches!(
                write.state_scope.as_str(),
                "invocation" | "model_call" | "turn" | "session" | "project" | "user"
            )
            || write.tool_permissions.iter().any(|permission| {
                permission.trim().is_empty()
                    || permission.len() > 256
                    || permission.chars().any(char::is_control)
            })
            || write.network_permissions.iter().any(|permission| {
                permission.trim().is_empty()
                    || permission.len() > 256
                    || permission.chars().any(char::is_control)
            })
            || write.idempotent
            || !write.external_effects
            || plugin.attempt != 1
        {
            return Err(PluginAutomaticMemoryWriteError::InvalidBinding);
        }
        let mut request = WritePluginMemoryDataRequest {
            binding: PluginOperationBindingDataRecord {
                plugin_id: plugin.plugin_id.clone(),
                plugin_version: plugin.plugin_version.clone(),
                invocation_id: plugin.invocation_id.clone(),
                operation_id: plugin.provider_id.clone(),
                session_id: command.session_id.to_string(),
                run_id: command.identity.run_id.clone(),
                node_id: None,
                declaration_hash: plugin.declaration_hash,
                configuration_reference: plugin.configuration_reference,
                request_hash: ZERO_HASH,
                idempotency_key: plugin.idempotency_key.clone(),
                attempt: plugin.attempt,
            },
            provider_id: plugin.provider_id.clone(),
            provider_version: plugin.provider_version.clone(),
            handler: write.handler,
            timeout_ms: write.timeout_ms,
            input: PluginMemoryWriteInputDataRecord {
                scope: map_scope(&command.identity.scope)?,
                boundary: map_boundary(&command.identity.policy)?,
                value: Value::String(command.content),
                value_hash: plugin.value_hash,
                artifacts: Vec::new(),
                references: Vec::new(),
                security_classification: map_security(&plugin.security_classification)?,
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: command.cancellation_id,
        };
        request.binding.request_hash = plugin_memory_write_request_hash(&request)?;
        let schema_input = json!({
            "scope": memory_scope_name(request.input.scope),
            "boundary": memory_boundary_name(request.input.boundary),
            "value": request.input.value.clone(),
            "value_hash": request.input.value_hash,
            "artifacts": [],
            "references": [],
            "security_classification": security_name(request.input.security_classification),
            "parameters": request.input.parameters.clone(),
        });
        validate_json_schema(&write.input_schema, &schema_input)?;
        if self
            .data
            .load_plugin_invocation_receipt(PluginInvocationReceiptDataIdentity {
                session_id: command.session_id,
                invocation_id: plugin.invocation_id.clone(),
            })
            .map_err(|_| PluginAutomaticMemoryWriteError::ReceiptUnavailable)?
            .is_some()
        {
            return Err(PluginAutomaticMemoryWriteError::TerminalReceiptExists);
        }
        let mut ticket_count = self
            .ticket_count
            .lock()
            .map_err(|_| PluginAutomaticMemoryWriteError::InvocationInFlight)?;
        if *ticket_count >= MAX_IN_FLIGHT_INVOCATIONS {
            return Err(PluginAutomaticMemoryWriteError::InvocationInFlight);
        }
        *ticket_count += 1;
        drop(ticket_count);
        Ok(PreparedPluginAutomaticMemoryWrite {
            identity: command.identity,
            action_digest: command.action_digest,
            request: Some(request),
            output_schema: write.output_schema,
            ticket_count: self.ticket_count.clone(),
        })
    }

    fn recover_terminal_receipt(
        &self,
        command: &PreparePluginAutomaticMemoryWriteCommand,
    ) -> Result<Option<CompletedPluginAutomaticMemoryWrite>, PluginAutomaticMemoryWriteError> {
        validate_command(command)?;
        let plugin = command
            .identity
            .plugin
            .as_ref()
            .ok_or(PluginAutomaticMemoryWriteError::InvalidBinding)?;
        let Some(stored) = self
            .data
            .load_plugin_invocation_receipt(PluginInvocationReceiptDataIdentity {
                session_id: command.session_id,
                invocation_id: plugin.invocation_id.clone(),
            })
            .map_err(|_| PluginAutomaticMemoryWriteError::ReceiptUnavailable)?
        else {
            return Ok(None);
        };
        let receipt: AutomaticPluginMemoryTerminalReceipt =
            serde_json::from_str(&stored.receipt_json)
                .map_err(|_| PluginAutomaticMemoryWriteError::InvalidReceipt)?;
        receipt.validate(&command.identity)?;
        Ok(Some(receipt.completed()))
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the one-shot boundary keeps gate ownership, exact receipt recovery, dispatch, validation, and durable sealing adjacent"
    )]
    async fn invoke_once(
        &self,
        state: &SessionState,
        mut prepared: PreparedPluginAutomaticMemoryWrite,
    ) -> Result<PluginAutomaticMemoryWriteOutcome, PluginAutomaticMemoryWriteError> {
        let Some(record) = state
            .automatic_memory_writes
            .get(&prepared.identity.write_id)
        else {
            return Err(PluginAutomaticMemoryWriteError::DispatchNotCommitted);
        };
        let request = prepared
            .request
            .as_ref()
            .ok_or(PluginAutomaticMemoryWriteError::DispatchNotCommitted)?;
        let selected = state
            .style_binding
            .as_ref()
            .and_then(|binding| binding.memory.plugin.as_ref());
        let plugin_identity = prepared.identity.plugin.as_ref();
        if state.id.to_string() != request.binding.session_id
            || record.identity != prepared.identity
            || record.state != AutomaticMemoryWriteState::Dispatched
            || record.action_digest != Some(prepared.action_digest)
            || record.dispatched_at.is_none()
            || record.completed_at.is_some()
            || !state
                .plugins
                .activated_plugin_ids
                .contains(&request.binding.plugin_id)
            || selected
                .zip(plugin_identity)
                .is_none_or(|(selected, plugin)| {
                    selected.plugin_id != plugin.plugin_id
                        || selected.plugin_version != plugin.plugin_version
                        || selected.provider_id != plugin.provider_id
                        || selected.provider_version != plugin.provider_version
                        || selected.declaration_hash != plugin.declaration_hash
                        || selected.configuration_reference != plugin.configuration_reference
                })
        {
            return Err(PluginAutomaticMemoryWriteError::DispatchNotCommitted);
        }
        let gate_key = (state.id, request.binding.invocation_id.clone());
        let dispatch_gate = {
            let mut gates = self.dispatch_gates.lock().await;
            gates
                .entry(gate_key.clone())
                .or_insert_with(|| Arc::new(DispatchGate::new(DispatchGateState::default())))
                .clone()
        };
        let mut dispatch_guard = dispatch_gate.lock().await;
        if let Some(completed) =
            self.recover_terminal_receipt(&PreparePluginAutomaticMemoryWriteCommand {
                session_id: state.id,
                identity: prepared.identity.clone(),
                content: record.content.clone(),
                cancellation_id: request.binding.run_id.clone(),
                action_digest: prepared.action_digest,
                runtime_api_version: state
                    .style_binding
                    .as_ref()
                    .map_or_else(String::new, |binding| binding.runtime_api_version.clone()),
            })?
        {
            drop(dispatch_guard);
            let mut gates = self.dispatch_gates.lock().await;
            if gates
                .get(&gate_key)
                .is_some_and(|current| Arc::ptr_eq(current, &dispatch_gate))
            {
                gates.remove(&gate_key);
            }
            return Ok(PluginAutomaticMemoryWriteOutcome::Completed(completed));
        }
        if dispatch_guard.claimed {
            return Err(PluginAutomaticMemoryWriteError::AmbiguousFailClosed);
        }
        dispatch_guard.claimed = true;
        let plugin = prepared
            .identity
            .plugin
            .as_ref()
            .expect("prepared plugin write retains plugin identity");
        let session_id = SessionId::from_str(&request.binding.session_id)
            .expect("prepared plugin write retains canonical session");
        let expected_binding = request.binding.clone();
        let expected_provider_id = request.provider_id.clone();
        let expected_provider_version = request.provider_version.clone();
        let expected_value_hash = request.input.value_hash;
        let request = prepared
            .request
            .take()
            .ok_or(PluginAutomaticMemoryWriteError::DispatchNotCommitted)?;
        let raw = match self.data.write_memory(request).await {
            Ok(raw) => raw,
            Err(error) => {
                return Ok(PluginAutomaticMemoryWriteOutcome::Ambiguous {
                    code: plugin_error_code(&error).to_owned(),
                    terminal_receipt: None,
                });
            }
        };
        if raw.binding != expected_binding
            || raw.provider_id != expected_provider_id
            || raw.provider_version != expected_provider_version
            || raw.value_hash != expected_value_hash
        {
            return Ok(PluginAutomaticMemoryWriteOutcome::Ambiguous {
                code: String::from("plugin_memory_write_terminal_identity_mismatch"),
                terminal_receipt: None,
            });
        }
        if validate_json_schema(&prepared.output_schema, &raw.receipt).is_err() {
            return Ok(PluginAutomaticMemoryWriteOutcome::Ambiguous {
                code: String::from("plugin_memory_write_invalid_output_schema"),
                terminal_receipt: None,
            });
        }
        let Ok(receipt) = AutomaticPluginMemoryTerminalReceipt::seal(
            &prepared.identity,
            raw.provider_record_id,
            raw.value_hash,
            raw.receipt,
        ) else {
            return Ok(PluginAutomaticMemoryWriteOutcome::Ambiguous {
                code: String::from("plugin_memory_write_invalid_terminal_response"),
                terminal_receipt: None,
            });
        };
        let receipt_json = match serde_json::to_string(&receipt) {
            Ok(json) if json.len() <= MAX_RECEIPT_BYTES => json,
            _ => {
                return Ok(PluginAutomaticMemoryWriteOutcome::Ambiguous {
                    code: String::from("plugin_memory_write_terminal_receipt_too_large"),
                    terminal_receipt: None,
                });
            }
        };
        let stored =
            self.data
                .store_plugin_invocation_receipt(StorePluginInvocationReceiptDataRequest {
                    identity: PluginInvocationReceiptDataIdentity {
                        session_id,
                        invocation_id: plugin.invocation_id.clone(),
                    },
                    receipt_json: receipt_json.clone(),
                });
        let outcome = match stored {
            Ok(stored) if stored.receipt_json == receipt_json => {
                PluginAutomaticMemoryWriteOutcome::Completed(receipt.completed())
            }
            _ => PluginAutomaticMemoryWriteOutcome::Ambiguous {
                code: String::from("plugin_memory_write_terminal_receipt_unavailable"),
                terminal_receipt: None,
            },
        };
        if matches!(outcome, PluginAutomaticMemoryWriteOutcome::Completed(_)) {
            drop(dispatch_guard);
            let mut gates = self.dispatch_gates.lock().await;
            if gates
                .get(&gate_key)
                .is_some_and(|current| Arc::ptr_eq(current, &dispatch_gate))
            {
                gates.remove(&gate_key);
            }
        }
        Ok(outcome)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct AutomaticPluginMemoryTerminalReceipt {
    invocation_id: String,
    invocation_digest: ContentHash,
    plugin_id: String,
    plugin_version: String,
    provider_id: String,
    provider_version: String,
    provider_record_id: String,
    value_hash: ContentHash,
    provider_receipt: Value,
    receipt_hash: ContentHash,
}

impl AutomaticPluginMemoryTerminalReceipt {
    fn seal(
        identity: &AutomaticMemoryWriteIdentity,
        provider_record_id: String,
        value_hash: ContentHash,
        provider_receipt: Value,
    ) -> Result<Self, PluginAutomaticMemoryWriteError> {
        let plugin = identity
            .plugin
            .as_ref()
            .ok_or(PluginAutomaticMemoryWriteError::InvalidBinding)?;
        let mut receipt = Self {
            invocation_id: plugin.invocation_id.clone(),
            invocation_digest: plugin.invocation_digest,
            plugin_id: plugin.plugin_id.clone(),
            plugin_version: plugin.plugin_version.clone(),
            provider_id: plugin.provider_id.clone(),
            provider_version: plugin.provider_version.clone(),
            provider_record_id,
            value_hash,
            provider_receipt,
            receipt_hash: ZERO_HASH,
        };
        receipt.receipt_hash = receipt.expected_hash(identity)?;
        receipt.validate(identity)?;
        Ok(receipt)
    }

    fn expected_hash(
        &self,
        identity: &AutomaticMemoryWriteIdentity,
    ) -> Result<ContentHash, PluginAutomaticMemoryWriteError> {
        let mut normalized = self.clone();
        normalized.receipt_hash = ZERO_HASH;
        serde_json::to_vec(&(
            "agentmod.plugin-automatic-memory-write-terminal-receipt.v1",
            identity,
            normalized,
        ))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginAutomaticMemoryWriteError::InvalidReceipt)
    }

    fn validate(
        &self,
        identity: &AutomaticMemoryWriteIdentity,
    ) -> Result<(), PluginAutomaticMemoryWriteError> {
        let plugin = identity
            .plugin
            .as_ref()
            .ok_or(PluginAutomaticMemoryWriteError::InvalidBinding)?;
        if self.invocation_id != plugin.invocation_id
            || self.invocation_digest != plugin.invocation_digest
            || self.plugin_id != plugin.plugin_id
            || self.plugin_version != plugin.plugin_version
            || self.provider_id != plugin.provider_id
            || self.provider_version != plugin.provider_version
            || self.value_hash != plugin.value_hash
            || self.provider_record_id.trim().is_empty()
            || self.provider_record_id.len() > 512
            || self.receipt_hash == ZERO_HASH
            || self.receipt_hash != self.expected_hash(identity)?
        {
            return Err(PluginAutomaticMemoryWriteError::InvalidReceipt);
        }
        Ok(())
    }

    fn completed(&self) -> CompletedPluginAutomaticMemoryWrite {
        CompletedPluginAutomaticMemoryWrite {
            reference: self.provider_record_id.clone(),
            retained: true,
            value_hash: self.value_hash,
            terminal_receipt: SealedPluginContextReceipt {
                receipt_hash: self.receipt_hash,
                receipt_reference: format!("plugin-receipt:{}", self.receipt_hash),
            },
        }
    }
}

fn validate_command(
    command: &PreparePluginAutomaticMemoryWriteCommand,
) -> Result<(), PluginAutomaticMemoryWriteError> {
    let plugin = command
        .identity
        .plugin
        .as_ref()
        .ok_or(PluginAutomaticMemoryWriteError::InvalidBinding)?;
    let empty_hash = ContentHash::digest(b"[]");
    let typed_value_hash = serde_json::to_vec(&Value::String(command.content.clone()))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| PluginAutomaticMemoryWriteError::InvalidBinding)?;
    if command.identity.content_hash != ContentHash::digest(command.content.as_bytes())
        || command.identity.byte_size != u64::try_from(command.content.len()).unwrap_or(u64::MAX)
        || plugin.value_hash != typed_value_hash
        || plugin.artifact_references_hash != empty_hash
        || plugin.references_hash != empty_hash
        || plugin.attempt != 1
        || SemanticVersion::parse(&command.runtime_api_version).is_err()
    {
        return Err(PluginAutomaticMemoryWriteError::InvalidBinding);
    }
    Ok(())
}

fn map_scope(scope: &str) -> Result<PluginMemoryScopeData, PluginAutomaticMemoryWriteError> {
    if scope.starts_with("session:") {
        Ok(PluginMemoryScopeData::Session)
    } else if scope.starts_with("project:") {
        Ok(PluginMemoryScopeData::Project)
    } else if scope.starts_with("user:") {
        Ok(PluginMemoryScopeData::User)
    } else if scope == "runtime" {
        Ok(PluginMemoryScopeData::Runtime)
    } else {
        Err(PluginAutomaticMemoryWriteError::InvalidBinding)
    }
}

fn map_boundary(
    policy: &str,
) -> Result<PluginMemoryWriteBoundaryData, PluginAutomaticMemoryWriteError> {
    match policy {
        "turn_completion" => Ok(PluginMemoryWriteBoundaryData::TurnCompletion),
        "iteration_completion" => Ok(PluginMemoryWriteBoundaryData::IterationCompletion),
        "session_completion" => Ok(PluginMemoryWriteBoundaryData::SessionCompletion),
        _ => Err(PluginAutomaticMemoryWriteError::InvalidBinding),
    }
}

fn map_security(
    security: &str,
) -> Result<PluginSecurityClassificationData, PluginAutomaticMemoryWriteError> {
    match security {
        "public" => Ok(PluginSecurityClassificationData::Public),
        "standard" => Ok(PluginSecurityClassificationData::Internal),
        "private" => Ok(PluginSecurityClassificationData::Private),
        "confidential" => Ok(PluginSecurityClassificationData::Confidential),
        _ => Err(PluginAutomaticMemoryWriteError::InvalidBinding),
    }
}

#[derive(Serialize)]
struct HashedPluginArtifactReference<'a> {
    artifact_id: &'a str,
    content_hash: &'a ContentHash,
    media_type: &'a str,
    size_bytes: u64,
    security_classification: &'static str,
}

#[derive(Serialize)]
struct HashedPluginCanonicalReference<'a> {
    kind: &'static str,
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_hash: Option<&'a ContentHash>,
}

#[derive(Serialize)]
struct HashedPluginMemoryWriteRequest<'a> {
    schema: &'static str,
    plugin_id: &'a str,
    plugin_version: &'a str,
    invocation_id: &'a str,
    operation_id: &'a str,
    session_id: &'a str,
    run_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<&'a str>,
    declaration_hash: ContentHash,
    configuration_reference: ContentHash,
    idempotency_key: &'a str,
    attempt: u8,
    provider_id: &'a str,
    provider_version: &'a str,
    handler: &'a str,
    timeout_ms: u64,
    idempotency: &'static str,
    scope: &'static str,
    boundary: &'static str,
    value: &'a Value,
    value_hash: ContentHash,
    artifacts: Vec<HashedPluginArtifactReference<'a>>,
    references: Vec<HashedPluginCanonicalReference<'a>>,
    security_classification: &'static str,
    parameters: &'a Value,
    readable_state: &'a Value,
}

fn plugin_memory_write_request_hash(
    request: &WritePluginMemoryDataRequest,
) -> Result<ContentHash, PluginAutomaticMemoryWriteError> {
    let binding = &request.binding;
    let input = &request.input;
    let artifacts = input
        .artifacts
        .iter()
        .map(hashed_artifact_reference)
        .collect::<Vec<_>>();
    let references = input
        .references
        .iter()
        .map(hashed_canonical_reference)
        .collect::<Vec<_>>();
    serde_json::to_vec(&HashedPluginMemoryWriteRequest {
        schema: "agentmod.plugin.memory-write.request.v2",
        plugin_id: &binding.plugin_id,
        plugin_version: &binding.plugin_version,
        invocation_id: &binding.invocation_id,
        operation_id: &binding.operation_id,
        session_id: &binding.session_id,
        run_id: &binding.run_id,
        node_id: binding.node_id.as_deref(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        idempotency_key: &binding.idempotency_key,
        attempt: binding.attempt,
        provider_id: &request.provider_id,
        provider_version: &request.provider_version,
        handler: &request.handler,
        timeout_ms: request.timeout_ms,
        idempotency: "non_idempotent",
        scope: memory_scope_name(input.scope),
        boundary: memory_boundary_name(input.boundary),
        value: &input.value,
        value_hash: input.value_hash,
        artifacts,
        references,
        security_classification: security_name(input.security_classification),
        parameters: &input.parameters,
        readable_state: &request.readable_state,
    })
    .map(|bytes| ContentHash::digest(&bytes))
    .map_err(|_| PluginAutomaticMemoryWriteError::InvalidBinding)
}

fn hashed_artifact_reference(
    reference: &PluginArtifactReferenceDataRecord,
) -> HashedPluginArtifactReference<'_> {
    HashedPluginArtifactReference {
        artifact_id: &reference.artifact_id,
        content_hash: &reference.content_hash,
        media_type: &reference.media_type,
        size_bytes: reference.size_bytes,
        security_classification: security_name(reference.security_classification),
    }
}

fn hashed_canonical_reference(
    reference: &PluginCanonicalReferenceDataRecord,
) -> HashedPluginCanonicalReference<'_> {
    HashedPluginCanonicalReference {
        kind: match reference.kind {
            PluginCanonicalReferenceKindData::Artifact => "artifact",
            PluginCanonicalReferenceKindData::NodeResult => "node_result",
            PluginCanonicalReferenceKindData::ToolResult => "tool_result",
            PluginCanonicalReferenceKindData::ApprovalResult => "approval_result",
            PluginCanonicalReferenceKindData::Continuation => "continuation",
            PluginCanonicalReferenceKindData::ChildSession => "child_session",
        },
        id: &reference.id,
        content_hash: reference.content_hash.as_ref(),
    }
}

const fn memory_scope_name(scope: PluginMemoryScopeData) -> &'static str {
    match scope {
        PluginMemoryScopeData::Session => "session",
        PluginMemoryScopeData::Project => "project",
        PluginMemoryScopeData::User => "user",
        PluginMemoryScopeData::Runtime => "runtime",
    }
}

const fn memory_boundary_name(boundary: PluginMemoryWriteBoundaryData) -> &'static str {
    match boundary {
        PluginMemoryWriteBoundaryData::Explicit => "explicit",
        PluginMemoryWriteBoundaryData::TurnCompletion => "turn_completion",
        PluginMemoryWriteBoundaryData::IterationCompletion => "iteration_completion",
        PluginMemoryWriteBoundaryData::SessionCompletion => "session_completion",
    }
}

const fn security_name(security: PluginSecurityClassificationData) -> &'static str {
    match security {
        PluginSecurityClassificationData::Public => "public",
        PluginSecurityClassificationData::Internal => "internal",
        PluginSecurityClassificationData::Private => "private",
        PluginSecurityClassificationData::Confidential => "confidential",
    }
}

fn plugin_error_code(error: &PluginDataError) -> &'static str {
    match error {
        PluginDataError::AmbiguousMemoryWrite { .. } => "plugin_memory_write_ambiguous",
        PluginDataError::Unavailable => "plugin_memory_write_unavailable",
        PluginDataError::Inactive => "plugin_memory_write_inactive",
        PluginDataError::Invalid => "plugin_memory_write_invalid",
        PluginDataError::Rejected { .. } => "plugin_memory_write_rejected",
        PluginDataError::MemoryOperationUnsupported => "plugin_memory_write_unsupported",
        PluginDataError::Cancelled => "plugin_memory_write_cancelled",
        _ => "plugin_memory_write_failed_without_terminal_receipt",
    }
}

fn validate_json_schema(
    schema_json: &str,
    value: &Value,
) -> Result<(), PluginAutomaticMemoryWriteError> {
    let schema: Value = serde_json::from_str(schema_json)
        .map_err(|_| PluginAutomaticMemoryWriteError::InvalidBinding)?;
    validate_schema_value(&schema, value)
}

fn validate_schema_value(
    schema: &Value,
    value: &Value,
) -> Result<(), PluginAutomaticMemoryWriteError> {
    if let Some(expected) = schema.get("type").and_then(Value::as_str) {
        let matches = match expected {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => false,
        };
        if !matches {
            return Err(PluginAutomaticMemoryWriteError::InvalidBinding);
        }
    }
    if let Some(allowed) = schema.get("enum").and_then(Value::as_array)
        && !allowed.contains(value)
    {
        return Err(PluginAutomaticMemoryWriteError::InvalidBinding);
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array)
            && required
                .iter()
                .filter_map(Value::as_str)
                .any(|field| !object.contains_key(field))
        {
            return Err(PluginAutomaticMemoryWriteError::InvalidBinding);
        }
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (field, property_schema) in properties {
                if let Some(field_value) = object.get(field) {
                    validate_schema_value(property_schema, field_value)?;
                }
            }
        }
    }
    if let (Some(items), Some(values)) = (schema.get("items"), value.as_array()) {
        for item in values {
            validate_schema_value(items, item)?;
        }
    }
    Ok(())
}

/// Stable production coordinator failures before effect dispatch or during receipt recovery.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PluginAutomaticMemoryWriteError {
    /// Immutable selection or declaration validation failed.
    #[error("plugin automatic-memory binding is invalid")]
    InvalidBinding,
    /// Durable receipt storage could not be read.
    #[error("plugin automatic-memory terminal receipt is unavailable")]
    ReceiptUnavailable,
    /// Stored terminal receipt failed exact validation.
    #[error("plugin automatic-memory terminal receipt is invalid")]
    InvalidReceipt,
    /// The exact one-shot invocation already has an outstanding ticket.
    #[error("plugin automatic-memory invocation already has an in-flight ticket")]
    InvocationInFlight,
    /// Replay does not prove the exact approved invocation was dispatched.
    #[error("plugin automatic-memory dispatch is not canonically committed")]
    DispatchNotCommitted,
    /// A terminal receipt already proves the one-shot invocation completed.
    #[error("plugin automatic-memory invocation already has a terminal receipt")]
    TerminalReceiptExists,
    /// A prior call crossed or may have crossed the non-idempotent boundary.
    #[error("plugin automatic-memory invocation is already claimed and must fail closed")]
    AmbiguousFailClosed,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use agentmod_event_model::{
        EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
    };
    use agentmod_primitives::{
        CausationId, CorrelationId, EventId, Sequence, TimestampMillis, Version,
    };
    use agentmod_runtime_data::{
        plugin::{
            ActivatePluginsDataRequest, ActivatedPluginsDataRecord, InvokePluginDataRequest,
            ObservePluginDataRequest, PluginDecisionDataRecord, PluginMemoryOperationDataRecord,
            PluginMemoryProviderDataRecord, PluginMemoryWriteReceiptDataRecord,
            PluginObservationDataRecord,
        },
        plugin_receipt::{
            PluginNodeReceiptDataError, PluginNodeReceiptDataIdentity, PluginNodeReceiptDataRecord,
            StorePluginNodeReceiptDataRequest,
        },
    };
    use agentmod_session_style_sdk::BuiltInStyle;
    use uuid::Uuid;

    use crate::{
        session::{
            AutomaticMemoryWriteApprovedEvent, AutomaticMemoryWriteDispatchedEvent,
            AutomaticMemoryWriteIdentity, AutomaticMemoryWriteProposedEvent,
            PluginSetActivatedEvent, RuntimeCommittedEvent, SessionCreatedEvent,
            SessionPluginMemoryConfiguration, SessionState, automatic_memory_write_id,
            plugin_automatic_memory_action_proposal, plugin_automatic_memory_write_identity,
            replay,
        },
        style_executor::tests::binding,
    };

    use super::*;

    #[derive(Clone)]
    struct MockPluginData {
        declaration: PluginMemoryProviderDataRecord,
        write_calls: Arc<AtomicU64>,
        receipt: Arc<Mutex<Option<PluginNodeReceiptDataRecord>>>,
        block_write: Arc<AtomicBool>,
        substitute_response: Arc<AtomicBool>,
        write_started: Arc<tokio::sync::Notify>,
        write_release: Arc<tokio::sync::Notify>,
    }

    impl MockPluginData {
        fn new(declaration_hash: ContentHash) -> Self {
            Self {
                declaration: PluginMemoryProviderDataRecord {
                    provider_id: String::from("fixture.memory"),
                    version: String::from("1.0.0"),
                    runtime_api: String::from("^1.0"),
                    capabilities: BTreeSet::from([String::from("memory.write")]),
                    retrieve: PluginMemoryOperationDataRecord {
                        handler: String::from("retrieve"),
                        input_schema: String::from(r#"{"type":"object"}"#),
                        output_schema: String::from(r#"{"type":"object"}"#),
                        timeout_ms: 1_000,
                        failure_policy: String::from("reject"),
                        max_attempts: 1,
                        retry_backoff_ms: 0,
                        idempotent: true,
                        tool_permissions: BTreeSet::new(),
                        network_permissions: BTreeSet::new(),
                        state_scope: String::from("session"),
                        external_effects: false,
                    },
                    write: Some(PluginMemoryOperationDataRecord {
                        handler: String::from("write"),
                        input_schema: String::from(
                            r#"{"type":"object","required":["scope","value","value_hash"]}"#,
                        ),
                        output_schema: String::from(r#"{"type":"object","required":["stored"]}"#),
                        timeout_ms: 1_000,
                        failure_policy: String::from("reject"),
                        max_attempts: 1,
                        retry_backoff_ms: 0,
                        idempotent: false,
                        tool_permissions: BTreeSet::new(),
                        network_permissions: BTreeSet::new(),
                        state_scope: String::from("session"),
                        external_effects: true,
                    }),
                    declaration_hash,
                },
                write_calls: Arc::new(AtomicU64::new(0)),
                receipt: Arc::new(Mutex::new(None)),
                block_write: Arc::new(AtomicBool::new(false)),
                substitute_response: Arc::new(AtomicBool::new(false)),
                write_started: Arc::new(tokio::sync::Notify::new()),
                write_release: Arc::new(tokio::sync::Notify::new()),
            }
        }
    }

    #[async_trait]
    impl PluginDataPort for MockPluginData {
        fn memory_provider_declaration(
            &self,
            plugin_id: &str,
            provider_id: &str,
            provider_version: &str,
        ) -> Result<PluginMemoryProviderDataRecord, PluginDataError> {
            if plugin_id == "fixture.plugin"
                && provider_id == self.declaration.provider_id
                && provider_version == self.declaration.version
            {
                Ok(self.declaration.clone())
            } else {
                Err(PluginDataError::Invalid)
            }
        }

        async fn activate_plugins(
            &self,
            _request: ActivatePluginsDataRequest,
        ) -> Result<ActivatedPluginsDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn invoke_plugin(
            &self,
            _request: InvokePluginDataRequest,
        ) -> Result<PluginDecisionDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn observe_event(
            &self,
            _request: ObservePluginDataRequest,
        ) -> Result<PluginObservationDataRecord, PluginDataError> {
            Err(PluginDataError::Invalid)
        }

        async fn write_memory(
            &self,
            request: WritePluginMemoryDataRequest,
        ) -> Result<PluginMemoryWriteReceiptDataRecord, PluginDataError> {
            self.write_calls.fetch_add(1, Ordering::Relaxed);
            if self.block_write.load(Ordering::Relaxed) {
                self.write_started.notify_one();
                self.write_release.notified().await;
            }
            Ok(PluginMemoryWriteReceiptDataRecord {
                binding: request.binding,
                provider_id: request.provider_id,
                provider_version: if self.substitute_response.load(Ordering::Relaxed) {
                    String::from("9.9.9-substituted")
                } else {
                    request.provider_version
                },
                provider_record_id: String::from("plugin-memory:record:1"),
                value_hash: request.input.value_hash,
                receipt: json!({"stored": true}),
            })
        }
    }

    impl PluginNodeReceiptDataPort for MockPluginData {
        fn load_plugin_node_receipt(
            &self,
            identity: PluginNodeReceiptDataIdentity,
        ) -> Result<Option<PluginNodeReceiptDataRecord>, PluginNodeReceiptDataError> {
            Ok(self
                .receipt
                .lock()
                .expect("receipt")
                .clone()
                .filter(|receipt| receipt.identity == identity))
        }

        fn store_plugin_node_receipt(
            &self,
            request: StorePluginNodeReceiptDataRequest,
        ) -> Result<PluginNodeReceiptDataRecord, PluginNodeReceiptDataError> {
            let record = PluginNodeReceiptDataRecord {
                identity: request.identity,
                receipt_json: request.receipt_json,
            };
            let mut stored = self.receipt.lock().expect("receipt");
            if let Some(existing) = stored.as_ref() {
                if existing != &record {
                    return Err(PluginNodeReceiptDataError::Conflict);
                }
                return Ok(existing.clone());
            }
            *stored = Some(record.clone());
            Ok(record)
        }
    }

    fn session_id() -> SessionId {
        SessionId::from_uuid(Uuid::from_u128(0x5151))
    }

    fn envelope(
        sequence: u64,
        payload: RuntimeCommittedEvent,
    ) -> EventEnvelope<RuntimeCommittedEvent> {
        EventEnvelope::seal(
            EventMetadata {
                event_id: EventId::from_uuid(Uuid::from_u128(100 + u128::from(sequence))),
                scope: EventScope::Session(session_id()),
                sequence: Sequence::new(sequence).expect("sequence"),
                timestamp: TimestampMillis::new(i64::try_from(sequence).expect("timestamp")),
                event_type: payload.event_type().to_owned(),
                event_version: Version::new(1, 0),
                correlation_id: CorrelationId::from_uuid(Uuid::from_u128(0x6161)),
                causation_id: CausationId::from_uuid(Uuid::from_u128(0x7171)),
                parent_graph_node_id: None,
                classification: EventClassification::Committed,
                origin: EventOrigin {
                    subsystem: String::from("runtime"),
                    plugin: None,
                },
                schema_version: Version::new(1, 0),
                artifacts: Vec::new(),
            },
            payload,
        )
        .expect("event")
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the fixture spells out the complete immutable plugin selection and canonical dispatch history"
    )]
    fn dispatched_fixture() -> (
        SessionState,
        PreparePluginAutomaticMemoryWriteCommand,
        ContentHash,
    ) {
        let declaration_hash = ContentHash::digest(b"fixture write declaration");
        let mut style = binding(BuiltInStyle::PersistentChat);
        style.memory.provider = String::from("fixture.memory");
        style.memory.plugin = Some(SessionPluginMemoryConfiguration {
            plugin_id: String::from("fixture.plugin"),
            plugin_version: String::from("2.0.0"),
            provider_id: String::from("fixture.memory"),
            provider_version: String::from("1.0.0"),
            declaration_hash,
            configuration_reference: ContentHash::digest(b"fixture write configuration"),
        });
        style.memory.scopes = vec![String::from("session")];
        style.memory.write_policy = String::from("turn_completion");
        let activated = replay(&[
            envelope(
                1,
                RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
                    workspace: String::from("fixture"),
                    style: style.id.clone(),
                    style_binding: Some(Box::new(style.clone())),
                }),
            ),
            envelope(
                2,
                RuntimeCommittedEvent::PluginSetActivated(PluginSetActivatedEvent {
                    plugin_ids: vec![String::from("fixture.plugin")],
                    plugin_set_hash: style.plugin_set_hash,
                }),
            ),
        ])
        .expect("activated");
        let content = String::from("bounded plugin memory");
        let mut identity = AutomaticMemoryWriteIdentity {
            write_id: String::new(),
            run_id: String::from("run-1"),
            request_hash: ContentHash::digest(b"request"),
            session_completion: None,
            iteration_completion: None,
            policy: String::from("turn_completion"),
            provider: String::from("fixture.memory"),
            scope: format!("session:{}", session_id()),
            source: String::from("runtime.automatic_memory:turn_completion:run-1"),
            content_hash: ContentHash::digest(content.as_bytes()),
            byte_size: u64::try_from(content.len()).expect("bytes"),
            created_at_millis: 10,
            plugin: None,
        };
        identity.plugin = Some(
            plugin_automatic_memory_write_identity(
                &activated,
                &identity,
                ContentHash::digest(
                    &serde_json::to_vec(&Value::String(content.clone()))
                        .expect("typed plugin memory value"),
                ),
                ContentHash::digest(b"[]"),
                ContentHash::digest(b"[]"),
                String::from("standard"),
            )
            .expect("plugin identity"),
        );
        identity.write_id =
            automatic_memory_write_id(session_id(), &identity).expect("write identity");
        let proposed = crate::session::reduce(
            Some(activated),
            &envelope(
                3,
                RuntimeCommittedEvent::AutomaticMemoryWriteProposed(Box::new(
                    AutomaticMemoryWriteProposedEvent {
                        identity: identity.clone(),
                        content: content.clone(),
                    },
                )),
            ),
        )
        .expect("proposed");
        let action_digest = plugin_automatic_memory_action_proposal(
            &proposed,
            &identity,
            identity.plugin.as_ref().expect("plugin"),
        )
        .expect("action")
        .digest()
        .expect("digest");
        let approved = crate::session::reduce(
            Some(proposed),
            &envelope(
                4,
                RuntimeCommittedEvent::AutomaticMemoryWriteApproved(Box::new(
                    AutomaticMemoryWriteApprovedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                )),
            ),
        )
        .expect("approved");
        let dispatched = crate::session::reduce(
            Some(approved),
            &envelope(
                5,
                RuntimeCommittedEvent::AutomaticMemoryWriteDispatched(Box::new(
                    AutomaticMemoryWriteDispatchedEvent {
                        identity: identity.clone(),
                        action_digest,
                    },
                )),
            ),
        )
        .expect("dispatched");
        (
            dispatched,
            PreparePluginAutomaticMemoryWriteCommand {
                session_id: session_id(),
                identity,
                content,
                cancellation_id: String::from("cancel-1"),
                action_digest,
                runtime_api_version: String::from("1.0.0"),
            },
            declaration_hash,
        )
    }

    #[tokio::test]
    async fn predispatch_invocation_is_rejected_without_crossing_data() {
        let (dispatched, command, declaration_hash) = dispatched_fixture();
        let data = MockPluginData::new(declaration_hash);
        let coordinator = ProductionPluginAutomaticMemoryWriteTurn::new(data.clone());
        let ticket = coordinator.prepare(command).expect("ticket");
        let mut before_dispatch = dispatched;
        before_dispatch
            .automatic_memory_writes
            .get_mut(&ticket.identity.write_id)
            .expect("record")
            .state = AutomaticMemoryWriteState::Approved;
        assert_eq!(
            coordinator.invoke_once(&before_dispatch, ticket).await,
            Err(PluginAutomaticMemoryWriteError::DispatchNotCommitted)
        );
        assert_eq!(data.write_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the semantic-hash regression mutates every request field independently"
    )]
    fn complete_request_hash_binds_configuration_bounds_and_full_input() {
        let (_dispatched, command, declaration_hash) = dispatched_fixture();
        let data = MockPluginData::new(declaration_hash);
        let coordinator = ProductionPluginAutomaticMemoryWriteTurn::new(data);
        let ticket = coordinator.prepare(command.clone()).expect("ticket");
        let request = ticket.request.as_ref().expect("prepared request");
        let expected = request.binding.request_hash;
        assert_eq!(
            expected.to_hex(),
            "42b1678cc8c13d8283edf538c9acad93d6fbb85a6a296a41b9573ef5b8b25a0d"
        );
        assert_ne!(expected, command.identity.request_hash);

        macro_rules! assert_hash_change {
            ($change:expr) => {{
                let mut changed = request.clone();
                $change(&mut changed);
                assert_ne!(
                    plugin_memory_write_request_hash(&changed).expect("changed hash"),
                    expected
                );
            }};
        }
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .plugin_id
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .plugin_version
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .invocation_id
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .operation_id
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .session_id
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .run_id
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.binding.node_id = Some(String::from("other-node"));
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.binding.declaration_hash = ContentHash::digest(b"other declaration");
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.binding.configuration_reference = ContentHash::digest(b"other configuration");
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .binding
            .idempotency_key
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.binding.attempt += 1;
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .provider_id
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .provider_version
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| request
            .handler
            .push_str("-other"));
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.timeout_ms += 1;
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.scope = PluginMemoryScopeData::Project;
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.boundary = PluginMemoryWriteBoundaryData::SessionCompletion;
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.value = json!("other value");
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.value_hash = ContentHash::digest(b"other value hash");
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.artifacts = vec![PluginArtifactReferenceDataRecord {
                artifact_id: String::from("artifact:other"),
                content_hash: ContentHash::digest(b"artifact"),
                media_type: String::from("application/json"),
                size_bytes: 8,
                security_classification: PluginSecurityClassificationData::Private,
            }];
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.references = vec![PluginCanonicalReferenceDataRecord {
                kind: PluginCanonicalReferenceKindData::NodeResult,
                id: String::from("node-result:other"),
                content_hash: Some(ContentHash::digest(b"node result")),
            }];
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.security_classification = PluginSecurityClassificationData::Confidential;
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.input.parameters = json!({"changed": true});
        });
        assert_hash_change!(|request: &mut WritePluginMemoryDataRequest| {
            request.readable_state = json!({"changed": true});
        });
    }

    #[test]
    fn input_schema_validates_the_exact_canonical_plugin_payload() {
        let (_dispatched, command, declaration_hash) = dispatched_fixture();
        let mut data = MockPluginData::new(declaration_hash);
        data.declaration
            .write
            .as_mut()
            .expect("write declaration")
            .input_schema = String::from(
            r#"{
                "type": "object",
                "required": ["scope", "boundary", "security_classification"],
                "properties": {
                    "scope": {"enum": ["session"]},
                    "boundary": {"enum": ["turn_completion"]},
                    "security_classification": {"enum": ["internal"]}
                }
            }"#,
        );
        let coordinator = ProductionPluginAutomaticMemoryWriteTurn::new(data);
        let ticket = coordinator
            .prepare(command)
            .expect("canonical data-boundary payload satisfies the declared schema");
        let request = ticket.request.as_ref().expect("prepared request");
        assert_eq!(request.input.scope, PluginMemoryScopeData::Session);
        assert_eq!(
            request.input.security_classification,
            PluginSecurityClassificationData::Internal
        );
    }

    #[tokio::test]
    async fn cloned_coordinators_share_one_shot_gate_and_receipt() {
        let (dispatched, command, declaration_hash) = dispatched_fixture();
        let data = MockPluginData::new(declaration_hash);
        let first = ProductionPluginAutomaticMemoryWriteTurn::new(data.clone());
        let second = first.clone();
        let first_ticket = first.prepare(command.clone()).expect("first ticket");
        let second_ticket = second
            .prepare(command.clone())
            .expect("second coordinator ticket");
        let (left, right) = tokio::join!(
            first.invoke_once(&dispatched, first_ticket),
            second.invoke_once(&dispatched, second_ticket)
        );
        assert!(matches!(
            left,
            Ok(PluginAutomaticMemoryWriteOutcome::Completed(_))
        ));
        assert!(matches!(
            right,
            Ok(PluginAutomaticMemoryWriteOutcome::Completed(_))
        ));
        assert_eq!(data.write_calls.load(Ordering::Relaxed), 1);
        assert!(matches!(
            first.prepare(command),
            Err(PluginAutomaticMemoryWriteError::TerminalReceiptExists)
        ));
    }

    #[tokio::test]
    async fn cancelled_live_call_retains_claim_and_never_redispatches() {
        let (dispatched, command, declaration_hash) = dispatched_fixture();
        let data = MockPluginData::new(declaration_hash);
        data.block_write.store(true, Ordering::Relaxed);
        let coordinator = ProductionPluginAutomaticMemoryWriteTurn::new(data.clone());
        let cancelled_ticket = coordinator
            .prepare(command.clone())
            .expect("cancelled ticket");
        let retry_ticket = coordinator.prepare(command).expect("retry ticket");
        let cancelled_coordinator = coordinator.clone();
        let cancelled_state = dispatched.clone();
        let call = tokio::spawn(async move {
            cancelled_coordinator
                .invoke_once(&cancelled_state, cancelled_ticket)
                .await
        });
        data.write_started.notified().await;
        call.abort();
        assert!(call.await.expect_err("call was cancelled").is_cancelled());

        assert_eq!(
            coordinator.invoke_once(&dispatched, retry_ticket).await,
            Err(PluginAutomaticMemoryWriteError::AmbiguousFailClosed)
        );
        assert_eq!(data.write_calls.load(Ordering::Relaxed), 1);
        assert!(
            data.receipt.lock().expect("receipt").is_none(),
            "cancellation before a terminal response cannot fabricate a receipt"
        );
    }

    #[tokio::test]
    async fn substituted_terminal_identity_is_ambiguous_and_retains_claim() {
        let (dispatched, command, declaration_hash) = dispatched_fixture();
        let data = MockPluginData::new(declaration_hash);
        data.substitute_response.store(true, Ordering::Relaxed);
        let coordinator = ProductionPluginAutomaticMemoryWriteTurn::new(data.clone());
        let first = coordinator.prepare(command.clone()).expect("first ticket");
        let retry = coordinator.prepare(command).expect("retry ticket");
        assert!(matches!(
            coordinator.invoke_once(&dispatched, first).await,
            Ok(PluginAutomaticMemoryWriteOutcome::Ambiguous { code, .. })
                if code == "plugin_memory_write_terminal_identity_mismatch"
        ));
        assert_eq!(
            coordinator.invoke_once(&dispatched, retry).await,
            Err(PluginAutomaticMemoryWriteError::AmbiguousFailClosed)
        );
        assert_eq!(data.write_calls.load(Ordering::Relaxed), 1);
        assert!(data.receipt.lock().expect("receipt").is_none());
    }
}
