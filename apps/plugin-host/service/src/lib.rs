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
    /// Number of currently running plugin invocations.
    #[must_use]
    pub async fn active_invocations(&self) -> usize {
        self.logic.active_invocations().await
    }

    /// Number of pending (non-terminal) durable deliveries.
    #[must_use]
    pub async fn pending_deliveries(&self) -> usize {
        self.logic.pending_deliveries().await
    }

    /// Flushes durable delivery state.
    ///
    /// # Errors
    ///
    /// Returns a service error when the flush fails.
    pub async fn flush(&self) -> Result<(), PluginServiceError> {
        self.logic.flush().await.map_err(map_error)
    }

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
                    logic::Decision::ToolResult(_) => return Err(PluginServiceError::Invalid),
                    logic::Decision::NodeResult(_) => return Err(PluginServiceError::Invalid),
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
            protocol::PluginCommand::ExecuteNode {
                plugin_id,
                invocation_id,
                executor_id,
                node_id,
                node_kind,
                input,
                variables,
                readable_state,
                authorization,
            } => {
                let (value, a) = self
                    .logic
                    .execute_node(logic::NodeExecutionCommand {
                        plugin_id,
                        invocation_id,
                        executor_id,
                        node_id,
                        node_kind,
                        input,
                        variables,
                        readable_state,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::NodeResult {
                    value,
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::MemoryDescribe {
                plugin_id,
                invocation_id,
                authorization,
            } => {
                let (result, a) = self
                    .logic
                    .memory(
                        "describe".into(),
                        logic::MemoryOperationCommand {
                            plugin_id,
                            invocation_id,
                            scope: String::new(),
                            query: String::new(),
                            limit: 0,
                            entries: Vec::new(),
                            authorization: map_auth(authorization),
                        },
                    )
                    .await
                    .map_err(map_error)?;
                let logic::MemoryResult::Describe {
                    scopes,
                    capabilities,
                    bounded_bytes,
                } = result
                else {
                    return Err(PluginServiceError::Invalid);
                };
                protocol::PluginResponse::MemoryDescribed {
                    scopes,
                    capabilities,
                    bounded_bytes,
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::MemoryRetrieve {
                plugin_id,
                invocation_id,
                scope,
                query,
                limit,
                authorization,
            } => {
                let (result, a) = self
                    .logic
                    .memory(
                        "retrieve".into(),
                        logic::MemoryOperationCommand {
                            plugin_id,
                            invocation_id,
                            scope,
                            query,
                            limit,
                            entries: Vec::new(),
                            authorization: map_auth(authorization),
                        },
                    )
                    .await
                    .map_err(map_error)?;
                let logic::MemoryResult::Retrieve { items } = result else {
                    return Err(PluginServiceError::Invalid);
                };
                protocol::PluginResponse::MemoryRetrieved {
                    items: items
                        .into_iter()
                        .map(|item| protocol::PluginMemoryItem {
                            reference: item.reference,
                            content: item.content,
                            score: item.score,
                            created_at_ms: item.created_at_ms,
                        })
                        .collect(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::MemoryCommitWrite {
                plugin_id,
                invocation_id,
                scope,
                entries,
                authorization,
            } => {
                let (result, a) = self
                    .logic
                    .memory(
                        "commit_write".into(),
                        logic::MemoryOperationCommand {
                            plugin_id,
                            invocation_id,
                            scope,
                            query: String::new(),
                            limit: 0,
                            entries: entries
                                .into_iter()
                                .map(|item| logic::MemoryItem {
                                    reference: item.reference,
                                    content: item.content,
                                    score: item.score,
                                    created_at_ms: item.created_at_ms,
                                })
                                .collect(),
                            authorization: map_auth(authorization),
                        },
                    )
                    .await
                    .map_err(map_error)?;
                let logic::MemoryResult::Commit {
                    retained,
                    references,
                } = result
                else {
                    return Err(PluginServiceError::Invalid);
                };
                protocol::PluginResponse::MemoryWriteCommitted {
                    retained,
                    references,
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::MemoryHealth {
                plugin_id,
                invocation_id,
                authorization,
            } => {
                let (result, a) = self
                    .logic
                    .memory(
                        "health".into(),
                        logic::MemoryOperationCommand {
                            plugin_id,
                            invocation_id,
                            scope: String::new(),
                            query: String::new(),
                            limit: 0,
                            entries: Vec::new(),
                            authorization: map_auth(authorization),
                        },
                    )
                    .await
                    .map_err(map_error)?;
                let logic::MemoryResult::Health {
                    healthy,
                    item_count,
                    retained_bytes,
                } = result
                else {
                    return Err(PluginServiceError::Invalid);
                };
                protocol::PluginResponse::MemoryHealthResult {
                    healthy,
                    item_count,
                    retained_bytes,
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::CompactionPropose {
                plugin_id,
                invocation_id,
                source_range_start,
                source_range_end,
                source_range_hash,
                current_entries,
                proposal,
                authorization,
            } => {
                let (replacement, size_bytes, a) = self
                    .logic
                    .compaction_propose(logic::CompactionCommand {
                        plugin_id,
                        invocation_id,
                        source_range_start,
                        source_range_end,
                        source_range_hash,
                        current_entries,
                        proposal,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::CompactionProposalAccepted {
                    replacement,
                    size_bytes,
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::ContextTransform {
                plugin_id,
                invocation_id,
                transform_id,
                boundary,
                payload,
                authorization,
            } => {
                let (value, a) = self
                    .logic
                    .context_transform(logic::ContextTransformCommand {
                        plugin_id,
                        invocation_id,
                        transform_id,
                        boundary: map_boundary(boundary),
                        payload,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::TransformResult {
                    value,
                    audit: map_audit(a),
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
                        event_range_start: 0,
                        event_range_end: 0,
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
                self.logic
                    .cancel(invocation_id.clone())
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::Cancelled {
                    invocation_id,
                    audit: protocol::PluginAudit {
                        plugin_id: String::new(),
                        invocation_id: None,
                        operation: "cancel".into(),
                        outcome: protocol::audit_outcome::CANCELLED.into(),
                        attempts: 1,
                    },
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
            protocol::PluginCommand::Reload {
                plugin_id,
                authorization,
            } => {
                let a = self
                    .logic
                    .reload(logic::StateChangeCommand {
                        plugin_id: plugin_id.clone(),
                        reason: None,
                        authorization: map_auth(authorization),
                    })
                    .await
                    .map_err(map_error)?;
                protocol::PluginResponse::StateChanged {
                    plugin_id,
                    state: "reloaded".into(),
                    audit: map_audit(a),
                }
            }
            protocol::PluginCommand::Unquarantine {
                plugin_id,
                authorization,
            } => {
                let a = self
                    .logic
                    .unquarantine(logic::StateChangeCommand {
                        plugin_id: plugin_id.clone(),
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
            protocol::PluginCommand::AuditList {
                since_invocation_id,
                limit,
            } => {
                let audits = self.logic.audits().await;
                let limit = usize::from(limit.min(1024));
                let mut filtered = audits
                    .into_iter()
                    .filter(|audit| {
                        since_invocation_id.as_ref().is_none_or(|cursor| {
                            audit
                                .invocation_id
                                .as_ref()
                                .is_none_or(|id| id.as_str() > cursor.as_str())
                        })
                    })
                    .collect::<Vec<_>>();
                let truncated = filtered.len() > limit;
                filtered.truncate(limit);
                protocol::PluginResponse::AuditListed {
                    audits: filtered.into_iter().map(map_audit).collect(),
                    truncated,
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
fn map_boundary(v: protocol::PluginContextTransformBoundary) -> logic::ContextTransformBoundary {
    match v {
        protocol::PluginContextTransformBoundary::BeforeMemoryRetrieval => {
            logic::ContextTransformBoundary::BeforeMemoryRetrieval
        }
        protocol::PluginContextTransformBoundary::AfterMemoryRetrieval => {
            logic::ContextTransformBoundary::AfterMemoryRetrieval
        }
        protocol::PluginContextTransformBoundary::BeforeCompaction => {
            logic::ContextTransformBoundary::BeforeCompaction
        }
        protocol::PluginContextTransformBoundary::AfterCompaction => {
            logic::ContextTransformBoundary::AfterCompaction
        }
        protocol::PluginContextTransformBoundary::BeforeProviderProjection => {
            logic::ContextTransformBoundary::BeforeProviderProjection
        }
        protocol::PluginContextTransformBoundary::BeforeTurnCompletion => {
            logic::ContextTransformBoundary::BeforeTurnCompletion
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
            protocol::PluginClass::GraphNode => logic::PluginClass::GraphNode,
            protocol::PluginClass::Memory => logic::PluginClass::Memory,
            protocol::PluginClass::Compaction => logic::PluginClass::Compaction,
            protocol::PluginClass::ContextTransform => logic::PluginClass::ContextTransform,
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
            .map(|executor| logic::NodeExecutorDeclaration {
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
        memory: v.memory.map(|memory| logic::MemoryDeclaration {
            scopes: memory.scopes,
            capabilities: memory.capabilities,
            bounded_bytes: memory.bounded_bytes,
        }),
        compaction: v.compaction.map(|compaction| logic::CompactionDeclaration {
            strategy_id: compaction.strategy_id,
            idempotent: compaction.idempotent,
            bounded_bytes: compaction.bounded_bytes,
        }),
        context_transforms: v
            .context_transforms
            .into_iter()
            .map(|transform| logic::ContextTransformDeclaration {
                transform_id: transform.transform_id,
                boundary: map_boundary(transform.boundary),
                stage: transform.stage,
                priority: transform.priority,
                before: transform.before,
                after: transform.after,
            })
            .collect(),
        observer_delivery: match v.observer_delivery {
            protocol::PluginObserverDelivery::BestEffort => logic::ObserverDelivery::BestEffort,
            protocol::PluginObserverDelivery::AtMostOnce => logic::ObserverDelivery::AtMostOnce,
            protocol::PluginObserverDelivery::AtLeastOnce {
                max_attempts,
                retry_backoff_ms,
            } => logic::ObserverDelivery::AtLeastOnce {
                max_attempts,
                retry_backoff_ms,
            },
        },
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
