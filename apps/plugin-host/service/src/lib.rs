//! Plugin protocol endpoints and explicit wire-to-logic mappings.
#![allow(
    missing_docs,
    reason = "wire mapping types are documented by the protocol crate"
)]

use agentmod_plugin_host_logic as logic;
use agentmod_plugin_protocol as protocol;
use thiserror::Error;

#[derive(Clone)]
pub struct PluginHostService<L> {
    logic: L,
}
impl<L> PluginHostService<L> {
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}
impl<L: logic::PluginLogicPort> PluginHostService<L> {
    pub async fn handle(&self, command: protocol::PluginCommand) -> protocol::PluginResponse {
        let consequential_write =
            matches!(&command, protocol::PluginCommand::InvokeMemoryWrite { .. });
        let response =
            self.execute(command)
                .await
                .unwrap_or_else(|error| protocol::PluginResponse::Failed {
                    code: error.code().into(),
                    message: "plugin request was rejected".into(),
                    retryable: error == PluginServiceError::Operation,
                });
        validate_service_response(response, consequential_write)
    }
    #[allow(
        clippy::too_many_lines,
        reason = "all versioned protocol-to-logic mappings remain explicit at one endpoint boundary"
    )]
    async fn execute(
        &self,
        c: protocol::PluginCommand,
    ) -> Result<protocol::PluginResponse, PluginServiceError> {
        c.validate_contract()
            .map_err(|_| PluginServiceError::Invalid)?;
        Ok(match c {
            protocol::PluginCommand::Negotiate {
                protocol_version,
                runtime_api_version,
                capabilities,
            } => {
                let (p, a, c) = self
                    .logic
                    .negotiate(protocol_version, runtime_api_version, capabilities)
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::Negotiated {
                    protocol_version: p,
                    runtime_api_version: a,
                    capabilities: c,
                }
            }
            protocol::PluginCommand::ValidateSet { manifests } => {
                protocol::PluginResponse::SetValidated {
                    plugin_ids: self
                        .logic
                        .validate_set(manifests.into_iter().map(map_manifest).collect())
                        .await
                        .map_err(map_error)?,
                }
            }
            protocol::PluginCommand::Load {
                manifest,
                configuration,
                authorization,
            } => {
                let v = self
                    .logic
                    .load(
                        map_manifest(*manifest),
                        configuration,
                        map_auth(authorization),
                    )
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::Loaded {
                    plugin_id: v.plugin_id,
                    state_version: v.state_version,
                    audit: map_audit(v.audit),
                }
            }
            protocol::PluginCommand::Intercept {
                cancellation_target,
                plugin_id,
                invocation_id,
                handler,
                proposal_type,
                proposal,
                readable_state,
                authorization,
            } => {
                let (d, a) = self
                    .logic
                    .invoke(logic::InvocationCommand {
                        cancellation_target: Some(map_cancellation_target(cancellation_target)),
                        plugin_id,
                        invocation_id,
                        handler,
                        executor_id: None,
                        executor_version: None,
                        timeout_ms: None,
                        configuration_reference: None,
                        operation: "intercept".into(),
                        kind: proposal_type,
                        payload: proposal,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                match d {
                    logic::Decision::Continue(proposal) => protocol::PluginResponse::Continue {
                        proposal,
                        audit: map_audit(a),
                    },
                    logic::Decision::Replace(proposal) => protocol::PluginResponse::Replace {
                        proposal,
                        audit: map_audit(a),
                    },
                    logic::Decision::Reject(reason) => protocol::PluginResponse::Reject {
                        reason,
                        audit: map_audit(a),
                    },
                    logic::Decision::ToolResult(_) | logic::Decision::NodeOutcome(_) => {
                        return Err(PluginServiceError::Invalid);
                    }
                }
            }
            protocol::PluginCommand::InvokeTool {
                plugin_id,
                invocation_id,
                tool,
                arguments,
                readable_state,
                authorization,
            } => {
                let (d, a) = self
                    .logic
                    .invoke(logic::InvocationCommand {
                        cancellation_target: None,
                        plugin_id,
                        invocation_id,
                        handler: tool,
                        executor_id: None,
                        executor_version: None,
                        timeout_ms: None,
                        configuration_reference: None,
                        operation: "tool".into(),
                        kind: "tool".into(),
                        payload: arguments,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                if let logic::Decision::ToolResult(value) = d {
                    protocol::PluginResponse::ToolResult {
                        value,
                        audit: map_audit(a),
                    }
                } else {
                    return Err(PluginServiceError::Invalid);
                }
            }
            protocol::PluginCommand::InvokeNodeExecutor {
                cancellation_target,
                plugin_id,
                invocation_id,
                executor_id,
                executor_version,
                node_kind,
                handler,
                timeout_ms,
                configuration_reference,
                input,
                readable_state,
                authorization,
            } => {
                let (decision, audit) = self
                    .logic
                    .invoke(logic::InvocationCommand {
                        cancellation_target: Some(map_cancellation_target(cancellation_target)),
                        plugin_id,
                        invocation_id,
                        handler,
                        executor_id: Some(executor_id),
                        executor_version: Some(executor_version),
                        timeout_ms: Some(timeout_ms),
                        configuration_reference: Some(configuration_reference),
                        operation: "node_executor".into(),
                        kind: node_kind,
                        payload: input,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                let logic::Decision::NodeOutcome(outcome) = decision else {
                    return Err(PluginServiceError::Invalid);
                };
                protocol::PluginResponse::NodeOutcome {
                    proposal: protocol::PluginNodeOutcomeProposal {
                        output: outcome.output,
                        preserved_state: outcome.preserved_state,
                        proposed_actions: outcome
                            .proposed_actions
                            .into_iter()
                            .map(|action| protocol::PluginNodeActionProposal {
                                kind: action.kind,
                                payload: action.payload,
                            })
                            .collect(),
                    },
                    audit: map_audit(audit),
                }
            }
            protocol::PluginCommand::InvokeContextTransform {
                cancellation_target,
                plugin_id,
                invocation_id,
                transform_id,
                transform_version,
                lifecycle,
                handler,
                timeout_ms,
                configuration_reference,
                input,
                readable_state,
                authorization,
            } => {
                let (proposal, audit) = self
                    .logic
                    .invoke_context_transform(logic::ContextTransformCommand {
                        cancellation_target: map_cancellation_target(cancellation_target),
                        plugin_id,
                        invocation_id,
                        transform_id,
                        transform_version,
                        timeout_ms,
                        configuration_reference,
                        lifecycle: match lifecycle {
                            protocol::ContextTransformLifecycle::BeforeModelRequest => {
                                logic::ContextTransformLifecycle::BeforeModelRequest
                            }
                        },
                        handler,
                        input,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::ContextTransformProposal {
                    proposal: protocol::PluginContextTransformProposal {
                        replacement: proposal.replacement,
                    },
                    audit: map_audit(audit),
                }
            }
            protocol::PluginCommand::InvokeMemoryRetrieve {
                binding,
                provider_id,
                provider_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                authorization,
            } => {
                let (proposal, audit) = self
                    .logic
                    .invoke_memory_retrieve(logic::MemoryRetrieveCommand {
                        binding: map_binding(binding),
                        provider_id,
                        provider_version,
                        handler,
                        timeout_ms,
                        idempotency: map_idempotency(idempotency),
                        request: serde_json::to_value(request)
                            .map_err(|_| PluginServiceError::Invalid)?,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::MemoryRetrieved {
                    proposal: protocol::PluginMemoryRetrieveProposal {
                        binding: unmap_binding(proposal.binding)?,
                        provider_id: proposal.provider_id,
                        provider_version: proposal.provider_version,
                        items: serde_json::from_value(proposal.items)
                            .map_err(|_| PluginServiceError::Invalid)?,
                    },
                    audit: map_audit(audit),
                }
            }
            protocol::PluginCommand::InvokeMemoryWrite {
                binding,
                provider_id,
                provider_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                authorization,
            } => {
                let (receipt, audit) = self
                    .logic
                    .invoke_memory_write(logic::MemoryWriteCommand {
                        binding: map_binding(binding),
                        provider_id,
                        provider_version,
                        handler,
                        timeout_ms,
                        idempotency: map_idempotency(idempotency),
                        request: serde_json::to_value(request)
                            .map_err(|_| PluginServiceError::Invalid)?,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::MemoryWritten {
                    receipt: protocol::PluginMemoryWriteReceiptProposal {
                        binding: unmap_binding(receipt.binding)
                            .map_err(|_| PluginServiceError::Ambiguous)?,
                        provider_id: receipt.provider_id,
                        provider_version: receipt.provider_version,
                        provider_record_id: receipt.provider_record_id,
                        value_hash: receipt
                            .value_hash
                            .parse()
                            .map_err(|_| PluginServiceError::Ambiguous)?,
                        receipt: receipt.receipt,
                    },
                    audit: map_audit(audit),
                }
            }
            protocol::PluginCommand::InvokeCompaction {
                binding,
                compactor_id,
                compactor_version,
                handler,
                timeout_ms,
                idempotency,
                request,
                readable_state,
                authorization,
            } => {
                let (proposal, audit) = self
                    .logic
                    .invoke_compaction(logic::CompactionCommand {
                        binding: map_binding(binding),
                        compactor_id,
                        compactor_version,
                        handler,
                        timeout_ms,
                        idempotency: map_idempotency(idempotency),
                        request: serde_json::to_value(request)
                            .map_err(|_| PluginServiceError::Invalid)?,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::CompactionProposed {
                    proposal: protocol::PluginCompactionProposal {
                        binding: unmap_binding(proposal.binding)?,
                        compactor_id: proposal.compactor_id,
                        compactor_version: proposal.compactor_version,
                        replacement: proposal.replacement,
                        replacement_hash: proposal
                            .replacement_hash
                            .parse()
                            .map_err(|_| PluginServiceError::Invalid)?,
                        preserved_references: serde_json::from_value(proposal.preserved_references)
                            .map_err(|_| PluginServiceError::Invalid)?,
                        preserved_artifacts: serde_json::from_value(proposal.preserved_artifacts)
                            .map_err(|_| PluginServiceError::Invalid)?,
                    },
                    audit: map_audit(audit),
                }
            }
            protocol::PluginCommand::PersistNodeState {
                cancellation_target,
                plugin_id,
                invocation_id,
                invocation_digest,
                executor_id,
                executor_version,
                executor_declaration_hash,
                configuration_reference,
                state_scope,
                prior_generation,
                prior_state_hash,
                state,
                state_hash,
                action_digest,
                authorization_digest,
                nonce,
                idempotency_key,
                authorization,
            } => {
                let receipt = self
                    .logic
                    .persist_node_state(logic::PersistNodeStateCommand {
                        cancellation_target: map_cancellation_target(cancellation_target),
                        plugin_id,
                        invocation_id,
                        invocation_digest,
                        executor_id,
                        executor_version,
                        executor_declaration_hash,
                        configuration_reference,
                        state_scope: map_node_state_scope(state_scope),
                        prior_generation,
                        prior_state_hash,
                        state,
                        state_hash,
                        action_digest,
                        authorization_digest,
                        nonce,
                        idempotency_key,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                let audit = protocol::PluginAudit {
                    plugin_id: receipt.plugin_id.clone(),
                    invocation_id: Some(receipt.invocation_id.clone()),
                    operation: String::from("persist_node_state"),
                    outcome: if receipt.replayed {
                        String::from("reconciled")
                    } else {
                        String::from("committed")
                    },
                    attempts: 1,
                };
                protocol::PluginResponse::NodeStatePersisted {
                    receipt: Box::new(protocol::PluginNodeStateReceipt {
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
                    }),
                    audit,
                }
            }
            protocol::PluginCommand::LoadNodeState {
                cancellation_target,
                plugin_id,
                invocation_id,
                invocation_digest,
                executor_id,
                executor_version,
                executor_declaration_hash,
                configuration_reference,
                state_scope,
                expected_generation,
                expected_state_hash,
                action_digest,
                authorization_digest,
                nonce,
                idempotency_key,
                authorization,
            } => {
                let loaded = self
                    .logic
                    .load_node_state(logic::LoadNodeStateCommand {
                        cancellation_target: map_cancellation_target(cancellation_target),
                        plugin_id,
                        invocation_id,
                        invocation_digest,
                        executor_id,
                        executor_version,
                        executor_declaration_hash,
                        configuration_reference,
                        state_scope: map_node_state_scope(state_scope),
                        expected_generation,
                        expected_state_hash,
                        action_digest,
                        authorization_digest,
                        nonce,
                        idempotency_key,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                let audit = protocol::PluginAudit {
                    plugin_id: loaded.receipt.plugin_id.clone(),
                    invocation_id: Some(loaded.receipt.invocation_id.clone()),
                    operation: String::from("load_node_state"),
                    outcome: if loaded.receipt.replayed {
                        String::from("reconciled")
                    } else {
                        String::from("loaded")
                    },
                    attempts: 1,
                };
                protocol::PluginResponse::NodeStateLoaded {
                    state: loaded.state,
                    receipt: Box::new(protocol::PluginNodeStateReadReceipt {
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
                    }),
                    audit,
                }
            }
            protocol::PluginCommand::Observe {
                plugin_id,
                invocation_id,
                handler,
                event_type,
                event,
                authorization,
            } => {
                let v = self
                    .logic
                    .observe(logic::ObservationCommand {
                        plugin_id,
                        invocation_id,
                        handler,
                        event_type,
                        event,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::Observation {
                    accepted: v.accepted,
                    queue_depth: v.queue_depth,
                    dropped: v.dropped,
                    status: match v.status {
                        logic::ObserverDeliveryStatus::Completed => {
                            protocol::PluginObserverDeliveryStatus::Completed
                        }
                        logic::ObserverDeliveryStatus::Rejected => {
                            protocol::PluginObserverDeliveryStatus::Rejected
                        }
                        logic::ObserverDeliveryStatus::Failed => {
                            protocol::PluginObserverDeliveryStatus::Failed
                        }
                        logic::ObserverDeliveryStatus::Ambiguous => {
                            protocol::PluginObserverDeliveryStatus::Ambiguous
                        }
                    },
                    request_hash: v.request_hash,
                    receipt_id: v.receipt_id,
                    receipt_digest: v.receipt_digest,
                    replayed: v.replayed,
                    audit: map_audit(v.audit),
                }
            }
            protocol::PluginCommand::CancelInvocation {
                target,
                reason_code,
                action_digest,
                nonce,
                idempotency_key,
                authorization,
            } => {
                let receipt = self
                    .logic
                    .cancel_invocation(logic::CancelInvocationCommand {
                        target: map_cancellation_target(target),
                        reason_code,
                        action_digest: action_digest.to_hex(),
                        nonce,
                        idempotency_key,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::InvocationCancelled {
                    receipt: Box::new(protocol::PluginInvocationCancellationReceipt {
                        target: unmap_cancellation_target(receipt.target)?,
                        reason_code: receipt.reason_code,
                        action_digest: receipt
                            .action_digest
                            .parse()
                            .map_err(|_| PluginServiceError::Invalid)?,
                        nonce: receipt.nonce,
                        idempotency_key: receipt.idempotency_key,
                        cancellation_id: receipt.cancellation_id,
                        status: match receipt.status {
                            logic::InvocationCancellationStatus::Signalled => {
                                protocol::PluginInvocationCancellationStatus::Signalled
                            }
                            logic::InvocationCancellationStatus::AlreadyTerminal => {
                                protocol::PluginInvocationCancellationStatus::AlreadyTerminal
                            }
                        },
                        receipt_id: receipt.receipt_id,
                        receipt_digest: receipt
                            .receipt_digest
                            .parse()
                            .map_err(|_| PluginServiceError::Invalid)?,
                    }),
                }
            }
            protocol::PluginCommand::Disable {
                plugin_id,
                plugin_version,
                configuration_reference,
                authorization,
            } => {
                let a = self
                    .logic
                    .state_change(logic::StateChangeCommand {
                        plugin_id: plugin_id.clone(),
                        plugin_version,
                        configuration_reference,
                        action: logic::StateChangeAction::Disable,
                        reason: None,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "disabled".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Enable {
                plugin_id,
                plugin_version,
                configuration_reference,
                authorization,
            } => {
                let a = self
                    .logic
                    .state_change(logic::StateChangeCommand {
                        plugin_id: plugin_id.clone(),
                        plugin_version,
                        configuration_reference,
                        action: logic::StateChangeAction::Enable,
                        reason: None,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "active".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Quarantine {
                plugin_id,
                plugin_version,
                configuration_reference,
                reason_code,
                authorization,
            } => {
                let a = self
                    .logic
                    .state_change(logic::StateChangeCommand {
                        plugin_id: plugin_id.clone(),
                        plugin_version,
                        configuration_reference,
                        action: logic::StateChangeAction::Quarantine,
                        reason: Some(reason_code),
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "quarantined".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Unquarantine {
                plugin_id,
                plugin_version,
                configuration_reference,
                authorization,
            } => {
                let a = self
                    .logic
                    .state_change(logic::StateChangeCommand {
                        plugin_id: plugin_id.clone(),
                        plugin_version,
                        configuration_reference,
                        action: logic::StateChangeAction::Unquarantine,
                        reason: None,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "active".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Health => {
                let h = self.logic.health().await;
                protocol::PluginResponse::Health {
                    loaded: h.loaded,
                    running: h.running,
                    observer_pending: h.observer_pending,
                    observer_dropped: h.observer_dropped,
                    state_flushed: h.state_flushed,
                }
            }
        })
    }
}

fn validate_service_response(
    response: protocol::PluginResponse,
    consequential_write: bool,
) -> protocol::PluginResponse {
    if response.validate_contract().is_ok() {
        response
    } else {
        protocol::PluginResponse::Failed {
            code: String::from(if consequential_write {
                "ambiguous_execution"
            } else {
                "plugin.service.invalid"
            }),
            message: String::from("plugin request was rejected"),
            retryable: false,
        }
    }
}
fn map_auth(v: protocol::PluginAuthorization) -> logic::Authorization {
    logic::Authorization {
        owner_id: v.owner_id,
        session_id: v.session_id,
        call_id: v.call_id,
        normalized_digest: v.normalized_digest,
        grant: v.grant,
        cancellation_id: v.cancellation_id,
    }
}
fn map_cancellation_target(
    target: protocol::PluginInvocationCancellationTarget,
) -> logic::InvocationCancellationTarget {
    logic::InvocationCancellationTarget {
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
fn unmap_cancellation_target(
    target: logic::InvocationCancellationTarget,
) -> Result<protocol::PluginInvocationCancellationTarget, PluginServiceError> {
    Ok(protocol::PluginInvocationCancellationTarget {
        session_id: target.session_id,
        run_id: target.run_id,
        plugin_id: target.plugin_id,
        plugin_version: target.plugin_version,
        invocation_id: target.invocation_id,
        invocation_digest: target
            .invocation_digest
            .parse()
            .map_err(|_| PluginServiceError::Invalid)?,
        operation_id: target.operation_id,
        declaration_hash: target
            .declaration_hash
            .parse()
            .map_err(|_| PluginServiceError::Invalid)?,
        request_hash: target
            .request_hash
            .parse()
            .map_err(|_| PluginServiceError::Invalid)?,
    })
}
const fn map_node_state_scope(scope: protocol::PluginNodeStateScope) -> logic::NodeStateScope {
    match scope {
        protocol::PluginNodeStateScope::Invocation => logic::NodeStateScope::Invocation,
        protocol::PluginNodeStateScope::ModelCall => logic::NodeStateScope::ModelCall,
        protocol::PluginNodeStateScope::Turn => logic::NodeStateScope::Turn,
        protocol::PluginNodeStateScope::Session => logic::NodeStateScope::Session,
        protocol::PluginNodeStateScope::Project => logic::NodeStateScope::Project,
        protocol::PluginNodeStateScope::User => logic::NodeStateScope::User,
        protocol::PluginNodeStateScope::Runtime => logic::NodeStateScope::Runtime,
    }
}
const fn unmap_node_state_scope(scope: logic::NodeStateScope) -> protocol::PluginNodeStateScope {
    match scope {
        logic::NodeStateScope::Invocation => protocol::PluginNodeStateScope::Invocation,
        logic::NodeStateScope::ModelCall => protocol::PluginNodeStateScope::ModelCall,
        logic::NodeStateScope::Turn => protocol::PluginNodeStateScope::Turn,
        logic::NodeStateScope::Session => protocol::PluginNodeStateScope::Session,
        logic::NodeStateScope::Project => protocol::PluginNodeStateScope::Project,
        logic::NodeStateScope::User => protocol::PluginNodeStateScope::User,
        logic::NodeStateScope::Runtime => protocol::PluginNodeStateScope::Runtime,
    }
}
fn map_manifest(v: protocol::PluginManifest) -> logic::ManifestCommand {
    logic::ManifestCommand {
        schema_version: v.schema_version,
        id: v.id,
        version: v.version,
        runtime_api: v.runtime_api,
        category: v.category,
        scope: v.scope,
        class: match v.class {
            protocol::PluginClass::Blocking => logic::PluginClass::Blocking,
            protocol::PluginClass::Observer => logic::PluginClass::Observer,
            protocol::PluginClass::Tool => logic::PluginClass::Tool,
            protocol::PluginClass::Extension => logic::PluginClass::Extension,
        },
        program: v.entrypoint.program,
        arguments: v.entrypoint.arguments,
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
        schema_id: v.configuration_schema.id,
        schema_version_number: v.configuration_schema.version,
        schema_required: v.configuration_schema.required,
        schema_json: v.configuration_schema.inline_json,
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
fn map_node_executor(
    executor: protocol::PluginNodeExecutorDeclaration,
) -> logic::NodeExecutorCommand {
    logic::NodeExecutorCommand {
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
        idempotent: matches!(
            executor.idempotency,
            protocol::NodeExecutorIdempotency::Idempotent
        ),
        tool_permissions: executor.tool_permissions,
        network_permissions: executor.network_permissions,
        state_scope: executor.state_scope,
        external_effects: executor.external_effects,
    }
}
fn map_context_transform(
    transform: protocol::PluginContextTransformDeclaration,
) -> logic::ContextTransformDeclarationCommand {
    logic::ContextTransformDeclarationCommand {
        transform_id: transform.transform_id,
        version: transform.version,
        runtime_api: transform.runtime_api,
        handler: transform.handler,
        lifecycle: match transform.lifecycle {
            protocol::ContextTransformLifecycle::BeforeModelRequest => {
                logic::ContextTransformLifecycle::BeforeModelRequest
            }
        },
        capabilities: transform.capabilities,
        input_schema: transform.input_schema,
        output_schema: transform.output_schema,
        timeout_ms: transform.timeout_ms,
        failure_policy: transform.failure_policy,
        max_attempts: transform.max_attempts,
        retry_backoff_ms: transform.retry_backoff_ms,
        idempotent: matches!(
            transform.idempotency,
            protocol::ContextTransformIdempotency::Idempotent
        ),
        tool_permissions: transform.tool_permissions,
        network_permissions: transform.network_permissions,
        state_scope: transform.state_scope,
        external_effects: transform.external_effects,
    }
}
fn map_memory_provider(
    provider: protocol::PluginMemoryProviderDeclaration,
) -> logic::MemoryProviderDeclarationCommand {
    logic::MemoryProviderDeclarationCommand {
        provider_id: provider.provider_id,
        version: provider.version,
        runtime_api: provider.runtime_api,
        capabilities: provider.capabilities,
        retrieve: map_operation_declaration(provider.retrieve),
        write: provider.write.map(map_operation_declaration),
    }
}
fn map_compactor(
    compactor: protocol::PluginCompactorDeclaration,
) -> logic::CompactorDeclarationCommand {
    let (failure_policy, max_attempts, retry_backoff_ms) = map_failure(&compactor.failure_policy);
    logic::CompactorDeclarationCommand {
        compactor_id: compactor.compactor_id,
        version: compactor.version,
        runtime_api: compactor.runtime_api,
        handler: compactor.handler,
        capabilities: compactor.capabilities,
        input_schema: compactor.input_schema,
        output_schema: compactor.output_schema,
        timeout_ms: compactor.timeout_ms,
        failure_policy,
        max_attempts,
        retry_backoff_ms,
        idempotency: map_idempotency(compactor.idempotency),
        tool_permissions: compactor.required_permissions.tools,
        network_permissions: compactor.required_permissions.network,
        state_scope: map_operation_scope(compactor.state_scope),
        external_effects: compactor.external_effects,
    }
}
fn map_operation_declaration<T>(operation: T) -> logic::OperationDeclarationCommand
where
    T: IntoOperationDeclaration,
{
    operation.into_operation_declaration()
}

trait IntoOperationDeclaration {
    fn into_operation_declaration(self) -> logic::OperationDeclarationCommand;
}

impl IntoOperationDeclaration for protocol::PluginMemoryRetrieveDeclaration {
    fn into_operation_declaration(self) -> logic::OperationDeclarationCommand {
        let failure_policy = &self.failure_policy;
        operation_declaration(
            self.handler,
            self.input_schema,
            self.output_schema,
            self.timeout_ms,
            failure_policy,
            self.idempotency,
            self.required_permissions,
            self.state_scope,
            self.external_effects,
        )
    }
}

impl IntoOperationDeclaration for protocol::PluginMemoryWriteDeclaration {
    fn into_operation_declaration(self) -> logic::OperationDeclarationCommand {
        let failure_policy = &self.failure_policy;
        operation_declaration(
            self.handler,
            self.input_schema,
            self.output_schema,
            self.timeout_ms,
            failure_policy,
            self.idempotency,
            self.required_permissions,
            self.state_scope,
            self.external_effects,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn operation_declaration(
    handler: String,
    input_schema: String,
    output_schema: String,
    timeout_ms: u64,
    failure_policy: &protocol::PluginOperationFailurePolicy,
    idempotency: protocol::PluginOperationIdempotency,
    permissions: protocol::PluginOperationPermissions,
    state_scope: protocol::PluginOperationStateScope,
    external_effects: bool,
) -> logic::OperationDeclarationCommand {
    let (failure_policy, max_attempts, retry_backoff_ms) = map_failure(failure_policy);
    logic::OperationDeclarationCommand {
        handler,
        input_schema,
        output_schema,
        timeout_ms,
        failure_policy,
        max_attempts,
        retry_backoff_ms,
        idempotency: map_idempotency(idempotency),
        tool_permissions: permissions.tools,
        network_permissions: permissions.network,
        state_scope: map_operation_scope(state_scope),
        external_effects,
    }
}
fn map_failure(policy: &protocol::PluginOperationFailurePolicy) -> (String, u8, u64) {
    match policy {
        protocol::PluginOperationFailurePolicy::Reject => (String::from("reject"), 1, 0),
        protocol::PluginOperationFailurePolicy::Cancel => (String::from("cancel"), 1, 0),
        protocol::PluginOperationFailurePolicy::Disable => (String::from("disable"), 1, 0),
        protocol::PluginOperationFailurePolicy::Continue => (String::from("continue"), 1, 0),
        protocol::PluginOperationFailurePolicy::Retry {
            max_attempts,
            backoff_ms,
        } => (String::from("retry"), *max_attempts, *backoff_ms),
    }
}
const fn map_idempotency(
    idempotency: protocol::PluginOperationIdempotency,
) -> logic::OperationIdempotency {
    match idempotency {
        protocol::PluginOperationIdempotency::Idempotent => logic::OperationIdempotency::Idempotent,
        protocol::PluginOperationIdempotency::NonIdempotent => {
            logic::OperationIdempotency::NonIdempotent
        }
    }
}
fn map_operation_scope(scope: protocol::PluginOperationStateScope) -> String {
    String::from(match scope {
        protocol::PluginOperationStateScope::Invocation => "invocation",
        protocol::PluginOperationStateScope::ModelCall => "model_call",
        protocol::PluginOperationStateScope::Turn => "turn",
        protocol::PluginOperationStateScope::Session => "session",
        protocol::PluginOperationStateScope::Project => "project",
        protocol::PluginOperationStateScope::User => "user",
        protocol::PluginOperationStateScope::Runtime => "runtime",
    })
}
fn map_binding(binding: protocol::PluginOperationBinding) -> logic::OperationBinding {
    logic::OperationBinding {
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
fn unmap_binding(
    binding: logic::OperationBinding,
) -> Result<protocol::PluginOperationBinding, PluginServiceError> {
    Ok(protocol::PluginOperationBinding {
        plugin_id: binding.plugin_id,
        plugin_version: binding.plugin_version,
        invocation_id: binding.invocation_id,
        operation_id: binding.operation_id,
        session_id: binding.session_id,
        run_id: binding.run_id,
        node_id: binding.node_id,
        declaration_hash: binding
            .declaration_hash
            .parse()
            .map_err(|_| PluginServiceError::Invalid)?,
        configuration_reference: binding
            .configuration_reference
            .parse()
            .map_err(|_| PluginServiceError::Invalid)?,
        request_hash: binding
            .request_hash
            .parse()
            .map_err(|_| PluginServiceError::Invalid)?,
        idempotency_key: binding.idempotency_key,
        attempt: binding.attempt,
    })
}
fn map_audit(v: logic::Audit) -> protocol::PluginAudit {
    protocol::PluginAudit {
        plugin_id: v.plugin_id,
        invocation_id: v.invocation_id,
        operation: v.operation,
        outcome: v.outcome,
        attempts: v.attempts,
    }
}
#[allow(clippy::needless_pass_by_value)]
fn map_error(v: logic::PluginLogicError) -> PluginServiceError {
    match v {
        logic::PluginLogicError::Invalid => PluginServiceError::Invalid,
        logic::PluginLogicError::Authorization => PluginServiceError::Authorization,
        logic::PluginLogicError::Cancelled => PluginServiceError::Cancelled,
        logic::PluginLogicError::Ambiguous => PluginServiceError::Ambiguous,
        logic::PluginLogicError::StaleStateGeneration => PluginServiceError::StaleStateGeneration,
        logic::PluginLogicError::StateConflict => PluginServiceError::StateConflict,
        logic::PluginLogicError::CancellationConflict => PluginServiceError::CancellationConflict,
        _ => PluginServiceError::Operation,
    }
}
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PluginServiceError {
    #[error("invalid plugin request")]
    Invalid,
    #[error("plugin authorization denied")]
    Authorization,
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
impl PluginServiceError {
    const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid_request",
            Self::Authorization => "authorization_denied",
            Self::Cancelled => "cancelled",
            Self::Ambiguous => "ambiguous_execution",
            Self::StaleStateGeneration => "stale_state_generation",
            Self::StateConflict => "state_conflict",
            Self::CancellationConflict => "cancellation_conflict",
            Self::Operation => "operation_failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_v6_memory_declaration_maps_without_recovery_semantic_loss() {
        let mapped = map_operation_declaration(protocol::PluginMemoryWriteDeclaration {
            handler: String::from("write"),
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(r#"{"type":"object"}"#),
            timeout_ms: 250,
            failure_policy: protocol::PluginOperationFailurePolicy::Retry {
                max_attempts: 3,
                backoff_ms: 7,
            },
            idempotency: protocol::PluginOperationIdempotency::Idempotent,
            required_permissions: protocol::PluginOperationPermissions {
                tools: vec![String::from("memory.write")],
                network: vec![String::from("memory.example")],
            },
            state_scope: protocol::PluginOperationStateScope::Session,
            external_effects: true,
        });
        assert_eq!(mapped.handler, "write");
        assert_eq!(mapped.failure_policy, "retry");
        assert_eq!(mapped.max_attempts, 3);
        assert_eq!(mapped.retry_backoff_ms, 7);
        assert_eq!(mapped.idempotency, logic::OperationIdempotency::Idempotent);
        assert_eq!(mapped.state_scope, "session");
        assert!(mapped.external_effects);
    }

    #[test]
    fn invalid_post_dispatch_write_response_is_always_ambiguous() {
        let response = validate_service_response(
            protocol::PluginResponse::MemoryWritten {
                receipt: protocol::PluginMemoryWriteReceiptProposal {
                    binding: protocol::PluginOperationBinding {
                        plugin_id: String::from("fixture.memory"),
                        plugin_version: String::from("1.0.0"),
                        invocation_id: String::from("invocation-1"),
                        operation_id: String::from("operation-1"),
                        session_id: String::from("session-1"),
                        run_id: String::from("run-1"),
                        node_id: None,
                        declaration_hash: "11".repeat(32).parse().expect("declaration hash"),
                        configuration_reference: "22"
                            .repeat(32)
                            .parse()
                            .expect("configuration hash"),
                        request_hash: "33".repeat(32).parse().expect("request hash"),
                        idempotency_key: String::from("key-1"),
                        attempt: 1,
                    },
                    provider_id: String::from("fixture.provider"),
                    provider_version: String::from("1.0.0"),
                    provider_record_id: String::new(),
                    value_hash: "44".repeat(32).parse().expect("value hash"),
                    receipt: serde_json::json!({"accepted":true}),
                },
                audit: protocol::PluginAudit {
                    plugin_id: String::from("fixture.memory"),
                    invocation_id: Some(String::from("invocation-1")),
                    operation: String::from("memory_write"),
                    outcome: String::from("completed"),
                    attempts: 1,
                },
            },
            true,
        );
        assert!(matches!(
            response,
            protocol::PluginResponse::Failed {
                ref code,
                retryable: false,
                ..
            } if code == "ambiguous_execution"
        ));
    }
}
