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
        self.execute(command)
            .await
            .unwrap_or_else(|error| protocol::PluginResponse::Failed {
                code: error.code().into(),
                message: "plugin request was rejected".into(),
                retryable: error == PluginServiceError::Operation,
            })
    }
    #[allow(
        clippy::too_many_lines,
        reason = "all versioned protocol-to-logic mappings remain explicit at one endpoint boundary"
    )]
    async fn execute(
        &self,
        c: protocol::PluginCommand,
    ) -> Result<protocol::PluginResponse, PluginServiceError> {
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
                        plugin_id,
                        invocation_id,
                        handler,
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
                    logic::Decision::ToolResult(_) => return Err(PluginServiceError::Invalid),
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
                        plugin_id,
                        invocation_id,
                        handler: tool,
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
                    audit: map_audit(v.audit),
                }
            }
            protocol::PluginCommand::Cancel { invocation_id } => {
                self.logic.cancel(invocation_id).await.map_err(map_error)?;
                protocol::PluginResponse::Health {
                    loaded: 0,
                    running: 0,
                    observer_dropped: 0,
                }
            }
            protocol::PluginCommand::Disable {
                plugin_id,
                authorization,
            } => {
                let a = self
                    .logic
                    .state_change(
                        logic::StateChangeCommand {
                            plugin_id: plugin_id.clone(),
                            reason: None,
                            authorization: map_auth(authorization),
                        },
                        false,
                    )
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "disabled".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Quarantine {
                plugin_id,
                reason_code,
                authorization,
            } => {
                let a = self
                    .logic
                    .state_change(
                        logic::StateChangeCommand {
                            plugin_id: plugin_id.clone(),
                            reason: Some(reason_code),
                            authorization: map_auth(authorization),
                        },
                        true,
                    )
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "quarantined".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Health => {
                let h = self.logic.health().await;
                protocol::PluginResponse::Health {
                    loaded: h.loaded,
                    running: h.running,
                    observer_dropped: h.observer_dropped,
                }
            }
        })
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
    }
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
    #[error("plugin operation failed")]
    Operation,
}
impl PluginServiceError {
    const fn code(self) -> &'static str {
        match self {
            Self::Invalid => "invalid_request",
            Self::Authorization => "authorization_denied",
            Self::Cancelled => "cancelled",
            Self::Operation => "operation_failed",
        }
    }
}
