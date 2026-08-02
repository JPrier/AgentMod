//! Runtime plugin management, node execution, context transform, memory, and
//! compaction adapters below the frontend layer.
#![allow(
    missing_docs,
    reason = "logic-local plugin management records remain boundary-specific"
)]

use std::collections::{BTreeMap, BTreeSet};

use agentmod_event_pipeline::{OrderingSpec, compile_order};
use agentmod_runtime_data::plugin::{
    ExecutePluginNodeDataRequest, PluginCompactionDataRequest, PluginContextTransformDataRequest,
    PluginDataError, PluginDataPort, PluginHealthDataRecord, PluginMemoryDataRequest,
    PluginMemoryDataResult, PluginStateChangeDataRequest,
};

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Canonical audit outcome codes mirrored from the wire vocabulary.
pub mod audit_outcome {
    pub const PROPOSED: &str = "proposed";
    pub const STARTED: &str = "started";
    pub const COMPLETED: &str = "completed";
    pub const REJECTED_BY_PLUGIN: &str = "rejected_by_plugin";
    pub const REJECTED_BY_RUNTIME: &str = "rejected_by_runtime";
    pub const TIMED_OUT: &str = "timed_out";
    pub const CANCELLED: &str = "cancelled";
    pub const CRASHED: &str = "crashed";
    pub const INVALID_RESPONSE: &str = "invalid_response";
    pub const QUARANTINED: &str = "quarantined";
    pub const OBSERVER_DELIVERY_ATTEMPTED: &str = "observer_delivery_attempted";
    pub const OBSERVER_DELIVERY_COMPLETED: &str = "observer_delivery_completed";
    pub const OBSERVER_DELIVERY_FAILED: &str = "observer_delivery_failed";
    pub const OBSERVER_DELIVERY_DROPPED: &str = "observer_delivery_dropped";
}

const MAX_SCHEMA_BYTES: usize = 65_536;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_TRANSFORMS: usize = 64;
const MAX_MEMORY_ENTRIES: usize = 128;

/// One canonical plugin audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginAuditRecord {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}

/// Inspectable plugin lifecycle projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginLifecycleProjection {
    pub plugin_id: String,
    pub class: String,
    pub category: String,
    pub version: String,
    pub status: String,
    pub node_executors: Vec<String>,
    pub memory_scopes: BTreeSet<String>,
    pub compaction_strategy: Option<String>,
    pub context_transforms: Vec<String>,
    pub observer_delivery: String,
    pub timeout_ms: u64,
}

/// Health projection for one session's plugin host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginHostHealthProjection {
    pub loaded: usize,
    pub running: usize,
    pub observer_dropped: u64,
    pub pending_deliveries: usize,
}

/// Consequential lifecycle operations require policy approval before any host
/// state change. The composition root wires this gate to the runtime
/// permission chain.
#[async_trait]
pub trait LifecyclePolicyGate: Send + Sync {
    /// Approves a consequential plugin lifecycle operation.
    async fn approve(
        &self,
        operation: &str,
        session_id: &str,
        plugin_id: &str,
    ) -> Result<(), PluginManagementError>;
}

/// A gate that rejects nothing (used by tests and trusted management).
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAllLifecyclePolicyGate;

#[async_trait]
impl LifecyclePolicyGate for AllowAllLifecyclePolicyGate {
    async fn approve(
        &self,
        _operation: &str,
        _session_id: &str,
        _plugin_id: &str,
    ) -> Result<(), PluginManagementError> {
        Ok(())
    }
}

/// Resolved plugin node executor for one node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPluginNodeExecutor {
    pub plugin_id: String,
    pub executor_id: String,
    pub version: String,
    pub node_kind: String,
    pub input_schema: String,
    pub output_schema: String,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub idempotent: bool,
    pub external_effect: bool,
    pub state_scope: String,
}

/// Command to execute one plugin-provided graph node.
#[derive(Clone, Debug, PartialEq)]
pub struct ExecutePluginNodeCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub plugin_id: String,
    pub executor_id: String,
    pub node_id: String,
    pub node_kind: String,
    pub input: Value,
    pub variables: Value,
    pub readable_state: Value,
}

/// Validated plugin node result.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginNodeResult {
    pub value: Value,
    pub output_schema_checked: bool,
    pub audit: PluginAuditRecord,
}

/// One compiled context transform in execution order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledContextTransform {
    pub plugin_id: String,
    pub transform_id: String,
    pub boundary: String,
    pub stage: u16,
    pub priority: i32,
}

/// Compiled context transform pipeline for one lifecycle boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ContextTransformPipeline {
    pub transforms: Vec<CompiledContextTransform>,
}

/// Command to run the compiled context transform pipeline at one boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct RunContextTransformsCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub boundary: String,
    pub payload: Value,
    pub transforms: ContextTransformPipeline,
}

/// Result of running the pipeline.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextTransformResult {
    pub value: Value,
    pub applied: Vec<String>,
    pub audits: Vec<PluginAuditRecord>,
}

/// Command for a plugin memory operation.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub plugin_id: String,
    pub scope: String,
    pub query: String,
    pub limit: usize,
    pub entries: Vec<PluginMemoryEntry>,
    /// Approved by the proposal/policy pipeline before any commit.
    pub write_authorized: bool,
}

/// Plugin memory entry.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryEntry {
    pub reference: String,
    pub content: String,
    pub score: Option<f64>,
    pub created_at_ms: i64,
}

/// Result of a plugin memory operation.
#[derive(Clone, Debug, PartialEq)]
pub enum PluginMemoryResult {
    Describe {
        scopes: BTreeSet<String>,
        capabilities: BTreeSet<String>,
        bounded_bytes: u64,
    },
    Retrieve {
        items: Vec<PluginMemoryEntry>,
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

/// Command to propose a plugin compaction replacement.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub plugin_id: String,
    pub source_range_start: u64,
    pub source_range_end: u64,
    pub source_range_hash: String,
    pub current_entries: Value,
    pub proposal: Value,
}

/// Accepted plugin compaction replacement.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionResult {
    pub replacement: Value,
    pub size_bytes: u64,
    pub audit: PluginAuditRecord,
}

/// Narrow plugin management and adapter interface.
#[async_trait]
pub trait PluginManagementLogicPort: Send + Sync {
    async fn list_plugins(
        &self,
        session_id: String,
    ) -> Result<Vec<PluginLifecycleProjection>, PluginManagementError>;

    async fn inspect_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginLifecycleProjection, PluginManagementError>;

    async fn disable_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginAuditRecord, PluginManagementError>;

    async fn quarantine_plugin(
        &self,
        session_id: String,
        plugin_id: String,
        reason: String,
    ) -> Result<PluginAuditRecord, PluginManagementError>;

    async fn unquarantine_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginAuditRecord, PluginManagementError>;

    async fn reload_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginAuditRecord, PluginManagementError>;

    async fn host_health(
        &self,
        session_id: String,
    ) -> Result<PluginHostHealthProjection, PluginManagementError>;

    async fn host_audits(
        &self,
        session_id: String,
    ) -> Result<Vec<PluginAuditRecord>, PluginManagementError>;

    async fn execute_node(
        &self,
        command: ExecutePluginNodeCommand,
    ) -> Result<PluginNodeResult, PluginManagementError>;

    async fn run_context_transforms(
        &self,
        command: RunContextTransformsCommand,
    ) -> Result<ContextTransformResult, PluginManagementError>;

    async fn plugin_memory(
        &self,
        operation: String,
        command: PluginMemoryCommand,
    ) -> Result<PluginMemoryResult, PluginManagementError>;

    async fn plugin_compaction(
        &self,
        command: PluginCompactionCommand,
    ) -> Result<PluginCompactionResult, PluginManagementError>;
}

/// Management coordinator over data plus a mandatory lifecycle policy gate.
#[derive(Clone)]
pub struct PluginManagementLogic<D, G> {
    data: D,
    gate: G,
}

impl<D, G> PluginManagementLogic<D, G> {
    #[must_use]
    pub const fn new(data: D, gate: G) -> Self {
        Self { data, gate }
    }
}

#[async_trait]
impl<D, G> PluginManagementLogicPort for PluginManagementLogic<D, G>
where
    D: PluginDataPort + Send + Sync + 'static,
    G: LifecyclePolicyGate + Send + Sync + 'static,
{
    async fn list_plugins(
        &self,
        session_id: String,
    ) -> Result<Vec<PluginLifecycleProjection>, PluginManagementError> {
        let manifests = plugin_manifests(&self.data, &session_id).await?;
        let health = self
            .data
            .plugin_health(session_id)
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(manifests
            .into_iter()
            .map(|manifest| lifecycle_projection(manifest, &health))
            .collect())
    }

    async fn inspect_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginLifecycleProjection, PluginManagementError> {
        let manifest = self
            .data
            .manifest(&plugin_id)
            .ok_or(PluginManagementError::Unavailable)?;
        let health = self
            .data
            .plugin_health(session_id)
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(lifecycle_projection(manifest, &health))
    }

    async fn disable_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginAuditRecord, PluginManagementError> {
        self.gate
            .approve("disable", &session_id, &plugin_id)
            .await?;
        let audit = self
            .data
            .plugin_state_change(
                "disable",
                PluginStateChangeDataRequest {
                    session_id,
                    plugin_id,
                    reason: None,
                    cancellation_id: uuid::Uuid::now_v7().to_string(),
                },
            )
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(PluginAuditRecord {
            plugin_id: audit.plugin_id,
            invocation_id: audit.invocation_id,
            operation: audit.operation,
            outcome: audit.outcome,
            attempts: audit.attempts,
        })
    }

    async fn quarantine_plugin(
        &self,
        session_id: String,
        plugin_id: String,
        reason: String,
    ) -> Result<PluginAuditRecord, PluginManagementError> {
        self.gate
            .approve("quarantine", &session_id, &plugin_id)
            .await?;
        let audit = self
            .data
            .plugin_state_change(
                "quarantine",
                PluginStateChangeDataRequest {
                    session_id,
                    plugin_id,
                    reason: Some(reason),
                    cancellation_id: uuid::Uuid::now_v7().to_string(),
                },
            )
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(PluginAuditRecord {
            plugin_id: audit.plugin_id,
            invocation_id: audit.invocation_id,
            operation: audit.operation,
            outcome: audit.outcome,
            attempts: audit.attempts,
        })
    }

    async fn unquarantine_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginAuditRecord, PluginManagementError> {
        self.gate
            .approve("unquarantine", &session_id, &plugin_id)
            .await?;
        let audit = self
            .data
            .plugin_state_change(
                "unquarantine",
                PluginStateChangeDataRequest {
                    session_id,
                    plugin_id,
                    reason: None,
                    cancellation_id: uuid::Uuid::now_v7().to_string(),
                },
            )
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(PluginAuditRecord {
            plugin_id: audit.plugin_id,
            invocation_id: audit.invocation_id,
            operation: audit.operation,
            outcome: audit.outcome,
            attempts: audit.attempts,
        })
    }

    async fn reload_plugin(
        &self,
        session_id: String,
        plugin_id: String,
    ) -> Result<PluginAuditRecord, PluginManagementError> {
        self.gate.approve("reload", &session_id, &plugin_id).await?;
        let audit = self
            .data
            .plugin_state_change(
                "reload",
                PluginStateChangeDataRequest {
                    session_id,
                    plugin_id,
                    reason: None,
                    cancellation_id: uuid::Uuid::now_v7().to_string(),
                },
            )
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(PluginAuditRecord {
            plugin_id: audit.plugin_id,
            invocation_id: audit.invocation_id,
            operation: audit.operation,
            outcome: audit.outcome,
            attempts: audit.attempts,
        })
    }

    async fn host_health(
        &self,
        session_id: String,
    ) -> Result<PluginHostHealthProjection, PluginManagementError> {
        let health = self
            .data
            .plugin_health(session_id)
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(PluginHostHealthProjection {
            loaded: health.loaded,
            running: health.running,
            observer_dropped: health.observer_dropped,
            pending_deliveries: health.pending_deliveries,
        })
    }

    async fn host_audits(
        &self,
        session_id: String,
    ) -> Result<Vec<PluginAuditRecord>, PluginManagementError> {
        let audits = self
            .data
            .plugin_audits(session_id)
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(audits
            .into_iter()
            .map(|audit| PluginAuditRecord {
                plugin_id: audit.plugin_id,
                invocation_id: audit.invocation_id,
                operation: audit.operation,
                outcome: audit.outcome,
                attempts: audit.attempts,
            })
            .collect())
    }

    async fn execute_node(
        &self,
        command: ExecutePluginNodeCommand,
    ) -> Result<PluginNodeResult, PluginManagementError> {
        if serde_json::to_vec(&command.input)
            .map(|bytes| bytes.len() > MAX_PAYLOAD_BYTES)
            .unwrap_or(true)
        {
            return Err(PluginManagementError::Invalid);
        }
        let manifest = self
            .data
            .manifest(&command.plugin_id)
            .ok_or(PluginManagementError::Unavailable)?;
        let executor = manifest
            .node_executors
            .iter()
            .find(|executor| {
                executor.executor_id == command.executor_id
                    && executor.node_kind == command.node_kind
            })
            .ok_or(PluginManagementError::Invalid)?;
        validate_schema(&executor.input_schema, &command.input)?;
        if !executor.external_effect && command.input.get("effect").is_some() {
            return Err(PluginManagementError::UndeclaredEffect);
        }
        if !executor.idempotent && command.variables.get("retry").is_some() {
            return Err(PluginManagementError::AmbiguousRetry);
        }
        let started = PluginAuditRecord {
            plugin_id: command.plugin_id.clone(),
            invocation_id: Some(command.node_id.clone()),
            operation: "execute_node".to_owned(),
            outcome: audit_outcome::STARTED.to_owned(),
            attempts: 0,
        };
        let (value, attempts) = match self
            .data
            .execute_plugin_node(ExecutePluginNodeDataRequest {
                session_id: command.session_id.clone(),
                plugin_id: command.plugin_id.clone(),
                invocation_id: format!("node:{}:{}", command.node_id, command.executor_id),
                executor_id: command.executor_id.clone(),
                node_id: command.node_id.clone(),
                node_kind: command.node_kind.clone(),
                input: command.input,
                variables: command.variables,
                readable_state: command.readable_state,
                cancellation_id: command.cancellation_id,
            })
            .await
        {
            Ok(result) => result,
            Err(error) => {
                return Err(PluginManagementError::NodeFailed {
                    started: Box::new(started),
                    error,
                });
            }
        };
        validate_schema(&executor.output_schema, &value).map_err(|error| {
            PluginManagementError::NodeOutputInvalid {
                audit: PluginAuditRecord {
                    plugin_id: command.plugin_id.clone(),
                    invocation_id: Some(command.node_id.clone()),
                    operation: "execute_node".to_owned(),
                    outcome: audit_outcome::REJECTED_BY_RUNTIME.to_owned(),
                    attempts,
                },
                error: Box::new(error),
            }
        })?;
        validate_node_identity(&command.node_id, &command.node_kind, &value)?;
        Ok(PluginNodeResult {
            value,
            output_schema_checked: true,
            audit: PluginAuditRecord {
                plugin_id: command.plugin_id,
                invocation_id: Some(command.node_id),
                operation: "execute_node".to_owned(),
                outcome: audit_outcome::COMPLETED.to_owned(),
                attempts,
            },
        })
    }

    async fn run_context_transforms(
        &self,
        command: RunContextTransformsCommand,
    ) -> Result<ContextTransformResult, PluginManagementError> {
        let mut value = command.payload;
        let mut applied = Vec::new();
        let mut audits = Vec::new();
        for transform in &command.transforms.transforms {
            if transform.boundary != command.boundary {
                continue;
            }
            let manifest = self
                .data
                .manifest(&transform.plugin_id)
                .ok_or(PluginManagementError::Unavailable)?;
            if !manifest
                .context_transforms
                .iter()
                .any(|declared| declared.transform_id == transform.transform_id)
            {
                return Err(PluginManagementError::UndeclaredTransform);
            }
            if transform_protected_keys(&value).is_some() {
                return Err(PluginManagementError::TransformProtectedKeys);
            }
            let (transformed, attempts) = self
                .data
                .plugin_context_transform(PluginContextTransformDataRequest {
                    session_id: command.session_id.clone(),
                    plugin_id: transform.plugin_id.clone(),
                    invocation_id: format!(
                        "transform:{}:{}:{}",
                        command.boundary, transform.transform_id, command.cancellation_id
                    ),
                    transform_id: transform.transform_id.clone(),
                    boundary: transform.boundary.clone(),
                    payload: value.clone(),
                    cancellation_id: command.cancellation_id.clone(),
                })
                .await
                .map_err(PluginManagementError::Data)?;
            if transform_protected_keys(&transformed).is_some() {
                return Err(PluginManagementError::TransformProtectedKeys);
            }
            value = transformed;
            applied.push(transform.transform_id.clone());
            audits.push(PluginAuditRecord {
                plugin_id: transform.plugin_id.clone(),
                invocation_id: Some(transform.transform_id.clone()),
                operation: "context_transform".to_owned(),
                outcome: audit_outcome::COMPLETED.to_owned(),
                attempts,
            });
        }
        Ok(ContextTransformResult {
            value,
            applied,
            audits,
        })
    }

    async fn plugin_memory(
        &self,
        operation: String,
        command: PluginMemoryCommand,
    ) -> Result<PluginMemoryResult, PluginManagementError> {
        if operation == "commit_write" && !command.write_authorized {
            return Err(PluginManagementError::MemoryWriteUnapproved);
        }
        if command.entries.len() > MAX_MEMORY_ENTRIES
            || command
                .entries
                .iter()
                .any(|entry| entry.content.len() > MAX_PAYLOAD_BYTES)
        {
            return Err(PluginManagementError::Invalid);
        }
        let (result, _attempts) = self
            .data
            .plugin_memory(
                operation.clone(),
                PluginMemoryDataRequest {
                    session_id: command.session_id.clone(),
                    plugin_id: command.plugin_id.clone(),
                    invocation_id: format!("memory:{}:{}", operation, command.cancellation_id),
                    scope: command.scope,
                    query: command.query,
                    limit: command.limit,
                    entries: command
                        .entries
                        .into_iter()
                        .map(
                            |entry| agentmod_runtime_data::plugin::PluginMemoryItemDataRecord {
                                reference: entry.reference,
                                content: entry.content,
                                score: entry.score,
                                created_at_ms: entry.created_at_ms,
                            },
                        )
                        .collect(),
                    cancellation_id: command.cancellation_id,
                },
            )
            .await
            .map_err(PluginManagementError::Data)?;
        Ok(match result {
            PluginMemoryDataResult::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            } => PluginMemoryResult::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            },
            PluginMemoryDataResult::Retrieve { items } => PluginMemoryResult::Retrieve {
                items: items
                    .into_iter()
                    .map(|item| PluginMemoryEntry {
                        reference: item.reference,
                        content: item.content,
                        score: item.score,
                        created_at_ms: item.created_at_ms,
                    })
                    .collect(),
            },
            PluginMemoryDataResult::Commit {
                retained,
                references,
            } => PluginMemoryResult::Commit {
                retained,
                references,
            },
            PluginMemoryDataResult::Health {
                healthy,
                item_count,
                retained_bytes,
            } => PluginMemoryResult::Health {
                healthy,
                item_count,
                retained_bytes,
            },
        })
    }

    async fn plugin_compaction(
        &self,
        command: PluginCompactionCommand,
    ) -> Result<PluginCompactionResult, PluginManagementError> {
        let manifest = self
            .data
            .manifest(&command.plugin_id)
            .ok_or(PluginManagementError::Unavailable)?;
        let compaction = manifest
            .compaction
            .as_ref()
            .ok_or(PluginManagementError::Invalid)?;
        if command.source_range_start > command.source_range_end {
            return Err(PluginManagementError::Invalid);
        }
        let (replacement, size_bytes, attempts) = self
            .data
            .plugin_compaction_propose(PluginCompactionDataRequest {
                session_id: command.session_id.clone(),
                plugin_id: command.plugin_id.clone(),
                invocation_id: format!("compaction:{}", command.cancellation_id),
                source_range_start: command.source_range_start,
                source_range_end: command.source_range_end,
                source_range_hash: command.source_range_hash.clone(),
                current_entries: command.current_entries,
                proposal: command.proposal,
                cancellation_id: command.cancellation_id,
            })
            .await
            .map_err(PluginManagementError::Data)?;
        if size_bytes > compaction.bounded_bytes {
            return Err(PluginManagementError::CompactionBoundExceeded);
        }
        if !compaction.idempotent && command.source_range_hash.is_empty() {
            return Err(PluginManagementError::AmbiguousRetry);
        }
        Ok(PluginCompactionResult {
            replacement,
            size_bytes,
            audit: PluginAuditRecord {
                plugin_id: command.plugin_id,
                invocation_id: None,
                operation: "compaction_propose".to_owned(),
                outcome: audit_outcome::COMPLETED.to_owned(),
                attempts,
            },
        })
    }
}

/// Compiles the ordered context transform pipeline for a style from its
/// allowed context-transform plugins. Ordering constraints reference transform
/// IDs within the selected set; unknown references are ignored so a style may
/// include only a subset of a plugin's declared transforms.
pub fn compile_context_transform_pipeline(
    manifests: &[PluginManifestView],
) -> Result<ContextTransformPipeline, PluginManagementError> {
    let mut transforms = Vec::new();
    for manifest in manifests {
        for transform in &manifest.context_transforms {
            transforms.push(transform.clone());
        }
    }
    if transforms.len() > MAX_TRANSFORMS {
        return Err(PluginManagementError::Invalid);
    }
    let mut by_key: BTreeMap<String, ContextTransformDeclarationView> = BTreeMap::new();
    let mut by_id: BTreeMap<String, String> = BTreeMap::new();
    for transform in &transforms {
        let key = format!("{}:{}", transform.plugin_id, transform.transform_id);
        by_key
            .entry(key.clone())
            .or_insert_with(|| transform.clone());
        by_id
            .entry(transform.transform_id.clone())
            .or_insert_with(|| key.clone());
    }
    let mut specifications = Vec::new();
    for transform in &transforms {
        let key = format!("{}:{}", transform.plugin_id, transform.transform_id);
        let mut specification = OrderingSpec::new(key.as_str(), transform.transform_id.as_str())
            .with_stage(transform.stage)
            .with_priority(transform.priority);
        for before in &transform.before {
            if let Some(target) = by_id.get(before) {
                specification = specification.before(target.as_str());
            }
        }
        for after in &transform.after {
            if let Some(target) = by_id.get(after) {
                specification = specification.after(target.as_str());
            }
        }
        specifications.push(specification);
    }
    let ordered = compile_order(&specifications).map_err(|_| PluginManagementError::Ordering)?;
    let mut pipeline = ContextTransformPipeline::default();
    for handler in ordered.handlers() {
        if let Some(transform) = by_key.get(handler.as_str()) {
            pipeline.transforms.push(CompiledContextTransform {
                plugin_id: transform.plugin_id.clone(),
                transform_id: transform.transform_id.clone(),
                boundary: transform.boundary.clone(),
                stage: transform.stage,
                priority: transform.priority,
            });
        }
    }
    Ok(pipeline)
}

/// Minimal manifest view used for pipeline compilation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginManifestView {
    pub plugin_id: String,
    pub context_transforms: Vec<ContextTransformDeclarationView>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextTransformDeclarationView {
    pub plugin_id: String,
    pub transform_id: String,
    pub boundary: String,
    pub stage: u16,
    pub priority: i32,
    pub before: BTreeSet<String>,
    pub after: BTreeSet<String>,
}

impl ContextTransformDeclarationView {
    #[must_use]
    pub fn new(
        plugin_id: String,
        transform_id: String,
        boundary: String,
        stage: u16,
        priority: i32,
        before: BTreeSet<String>,
        after: BTreeSet<String>,
    ) -> Self {
        Self {
            plugin_id,
            transform_id,
            boundary,
            stage,
            priority,
            before,
            after,
        }
    }
}

async fn plugin_manifests<D: PluginDataPort>(
    data: &D,
    _session_id: &str,
) -> Result<Vec<agentmod_runtime_data::plugin::PluginManifestDataRecord>, PluginManagementError> {
    Ok(data
        .plugin_ids()
        .into_iter()
        .filter_map(|id| data.manifest(&id))
        .collect())
}

fn lifecycle_projection(
    manifest: agentmod_runtime_data::plugin::PluginManifestDataRecord,
    health: &PluginHealthDataRecord,
) -> PluginLifecycleProjection {
    PluginLifecycleProjection {
        plugin_id: manifest.id,
        class: manifest.class,
        category: manifest.category,
        version: manifest.version,
        status: if health.loaded > 0 {
            String::from("active")
        } else {
            String::from("loaded")
        },
        node_executors: manifest
            .node_executors
            .iter()
            .map(|executor| executor.executor_id.clone())
            .collect(),
        memory_scopes: manifest
            .memory
            .as_ref()
            .map(|memory| memory.scopes.clone())
            .unwrap_or_default(),
        compaction_strategy: manifest
            .compaction
            .as_ref()
            .map(|compaction| compaction.strategy_id.clone()),
        context_transforms: manifest
            .context_transforms
            .iter()
            .map(|transform| transform.transform_id.clone())
            .collect(),
        observer_delivery: manifest.observer_delivery,
        timeout_ms: manifest.timeout_ms,
    }
}

fn validate_schema(schema: &str, value: &Value) -> Result<(), PluginManagementError> {
    if schema.is_empty() || schema.len() > MAX_SCHEMA_BYTES {
        return Err(PluginManagementError::InvalidSchema);
    }
    let document: Value =
        serde_json::from_str(schema).map_err(|_| PluginManagementError::InvalidSchema)?;
    if !document.is_object() {
        return Err(PluginManagementError::InvalidSchema);
    }
    if let Some(required) = document.get("required").and_then(Value::as_array) {
        for field in required.iter().filter_map(Value::as_str) {
            if value.get(field).is_none() {
                return Err(PluginManagementError::SchemaMismatch);
            }
        }
    }
    if document.get("additionalProperties") == Some(&Value::Bool(false))
        && let (Some(properties), Some(object)) = (
            document.get("properties").and_then(Value::as_object),
            value.as_object(),
        )
        && object.keys().any(|key| !properties.contains_key(key))
    {
        return Err(PluginManagementError::SchemaMismatch);
    }
    Ok(())
}

fn validate_node_identity(
    node_id: &str,
    node_kind: &str,
    value: &Value,
) -> Result<(), PluginManagementError> {
    if value.get("node_id").and_then(Value::as_str) != Some(node_id)
        || value.get("node_kind").and_then(Value::as_str) != Some(node_kind)
    {
        return Err(PluginManagementError::NodeIdentityMismatch);
    }
    if value.get("workspace").is_some() || value.get("session_id").is_some() {
        return Err(PluginManagementError::ProtectedIdentity);
    }
    Ok(())
}

fn transform_protected_keys(value: &Value) -> Option<&'static str> {
    if value.get("history").is_some()
        || value.get("workspace").is_some()
        || value.get("session_id").is_some()
        || value.get("identity").is_some()
        || value.get("secrets").is_some()
    {
        Some("transform attempted to modify protected context keys")
    } else {
        None
    }
}

/// Management, adapter, or policy failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginManagementError {
    #[error("plugin data operation failed: {0}")]
    Data(PluginDataError),
    #[error("plugin is unavailable or not activated")]
    Unavailable,
    #[error("plugin request is invalid")]
    Invalid,
    #[error("plugin schema is invalid or unbounded")]
    InvalidSchema,
    #[error("plugin value does not match its declared schema")]
    SchemaMismatch,
    #[error("plugin node output does not match its declared schema: {error}")]
    NodeOutputInvalid {
        audit: PluginAuditRecord,
        error: Box<PluginManagementError>,
    },
    #[error("plugin node output changed run or node identity")]
    NodeIdentityMismatch,
    #[error("plugin node output changed canonical identity or workspace")]
    ProtectedIdentity,
    #[error("plugin node requested an undeclared external effect")]
    UndeclaredEffect,
    #[error("ambiguous non-idempotent retry is not permitted")]
    AmbiguousRetry,
    #[error("plugin node invocation failed: {error}")]
    NodeFailed {
        started: Box<PluginAuditRecord>,
        error: PluginDataError,
    },
    #[error("plugin context transform is undeclared")]
    UndeclaredTransform,
    #[error("plugin context transform modified protected context keys")]
    TransformProtectedKeys,
    #[error("plugin context transform ordering is invalid")]
    Ordering,
    #[error("plugin memory write was not approved by the policy pipeline")]
    MemoryWriteUnapproved,
    #[error("plugin compaction replacement exceeds its declared bound")]
    CompactionBoundExceeded,
    #[error("consequential lifecycle action was not approved")]
    PolicyDenied,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use agentmod_runtime_data::plugin::{
        PluginCompactionDataRecord, PluginContextTransformDataRecord, PluginManifestDataRecord,
        PluginMemoryDataRecord, PluginNodeExecutorDataRecord,
    };
    use serde_json::json;

    use super::*;

    fn manifest_with(plugin_id: &str, category: &str) -> PluginManifestDataRecord {
        PluginManifestDataRecord {
            id: plugin_id.to_owned(),
            version: String::from("1.0.0"),
            class: String::from("blocking"),
            category: category.to_owned(),
            subscribed_events: BTreeSet::new(),
            timeout_ms: 1_000,
            failure_policy: String::from("reject"),
            canonical_manifest_json: String::new(),
            configuration: Value::Null,
            node_executors: if category == "graph_node" {
                vec![PluginNodeExecutorDataRecord {
                    executor_id: String::from("fixture.node"),
                    version: String::from("1.0.0"),
                    node_kind: String::from("emit_event"),
                    runtime_api: String::from("^1.0"),
                    required_capabilities: BTreeSet::from([String::from("events")]),
                    input_schema: String::from(r#"{"type":"object"}"#),
                    output_schema: String::from(r#"{"type":"object"}"#),
                    timeout_ms: 500,
                    failure_policy: String::from("reject"),
                    idempotent: true,
                    external_effect: false,
                    read_authority: BTreeSet::from([String::from("session_state")]),
                    state_scope: String::from("plugin_state"),
                }]
            } else {
                Vec::new()
            },
            memory: (category == "memory").then(|| PluginMemoryDataRecord {
                scopes: BTreeSet::from([String::from("session")]),
                capabilities: BTreeSet::from([String::from("write")]),
                bounded_bytes: 1024,
            }),
            compaction: (category == "compaction").then(|| PluginCompactionDataRecord {
                strategy_id: String::from("fixture.summary"),
                idempotent: true,
                bounded_bytes: 2048,
            }),
            context_transforms: if category == "context_transform" {
                vec![PluginContextTransformDataRecord {
                    transform_id: String::from("fixture.anonymize"),
                    boundary: String::from("before_provider_projection"),
                    stage: 10,
                    priority: 5,
                    before: BTreeSet::new(),
                    after: BTreeSet::new(),
                }]
            } else {
                Vec::new()
            },
            observer_delivery: String::from("best_effort"),
        }
    }

    #[test]
    fn transform_pipeline_orders_by_stage_priority_and_constraints() {
        let first = PluginManifestView {
            plugin_id: String::from("fixture.transform-a"),
            context_transforms: vec![ContextTransformDeclarationView::new(
                String::from("fixture.transform-a"),
                String::from("alpha"),
                String::from("before_provider_projection"),
                20,
                0,
                BTreeSet::new(),
                BTreeSet::from([String::from("beta")]),
            )],
        };
        let second = PluginManifestView {
            plugin_id: String::from("fixture.transform-b"),
            context_transforms: vec![ContextTransformDeclarationView::new(
                String::from("fixture.transform-b"),
                String::from("beta"),
                String::from("before_provider_projection"),
                10,
                0,
                BTreeSet::new(),
                BTreeSet::new(),
            )],
        };
        let pipeline =
            compile_context_transform_pipeline(&[first, second]).expect("compiled pipeline");
        assert_eq!(pipeline.transforms.len(), 2);
        assert_eq!(pipeline.transforms[0].transform_id, "beta");
        assert_eq!(pipeline.transforms[1].transform_id, "alpha");
    }

    #[test]
    fn transform_pipeline_ignores_unknown_constraints() {
        let manifest = PluginManifestView {
            plugin_id: String::from("fixture.transform"),
            context_transforms: vec![ContextTransformDeclarationView::new(
                String::from("fixture.transform"),
                String::from("solo"),
                String::from("before_turn_completion"),
                5,
                1,
                BTreeSet::from([String::from("missing.transform")]),
                BTreeSet::new(),
            )],
        };
        let pipeline = compile_context_transform_pipeline(&[manifest]).expect("pipeline");
        assert_eq!(pipeline.transforms.len(), 1);
        assert_eq!(pipeline.transforms[0].transform_id, "solo");
    }

    #[test]
    fn schema_validation_enforces_required_and_additional_properties() {
        let schema = r#"{"type":"object","required":["id"],"additionalProperties":false}"#;
        assert_eq!(
            validate_schema(schema, &json!({"other": 1})),
            Err(PluginManagementError::SchemaMismatch)
        );
        assert!(validate_schema(schema, &json!({"id": 1})).is_ok());
        assert_eq!(
            validate_schema("not-json", &json!({})),
            Err(PluginManagementError::InvalidSchema)
        );
    }

    #[test]
    fn node_identity_validation_rejects_canonical_scope_changes() {
        assert_eq!(
            validate_node_identity(
                "node-1",
                "emit_event",
                &json!({"node_id":"node-1","node_kind":"emit_event","workspace":"other"}),
            ),
            Err(PluginManagementError::ProtectedIdentity)
        );
        assert_eq!(
            validate_node_identity(
                "node-1",
                "emit_event",
                &json!({"node_id":"node-2","node_kind":"emit_event"}),
            ),
            Err(PluginManagementError::NodeIdentityMismatch)
        );
        assert!(
            validate_node_identity(
                "node-1",
                "emit_event",
                &json!({"node_id":"node-1","node_kind":"emit_event","ok":true}),
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn memory_write_requires_policy_approval() {
        #[derive(Clone)]
        struct NoData;
        #[async_trait]
        impl PluginDataPort for NoData {
            async fn activate_plugins(
                &self,
                _request: agentmod_runtime_data::plugin::ActivatePluginsDataRequest,
            ) -> Result<agentmod_runtime_data::plugin::ActivatedPluginsDataRecord, PluginDataError>
            {
                Err(PluginDataError::Unavailable)
            }

            async fn invoke_plugin(
                &self,
                _request: agentmod_runtime_data::plugin::InvokePluginDataRequest,
            ) -> Result<agentmod_runtime_data::plugin::PluginDecisionDataRecord, PluginDataError>
            {
                Err(PluginDataError::Unavailable)
            }

            async fn observe_event(
                &self,
                _request: agentmod_runtime_data::plugin::ObservePluginDataRequest,
            ) -> Result<agentmod_runtime_data::plugin::PluginObservationDataRecord, PluginDataError>
            {
                Err(PluginDataError::Unavailable)
            }

            async fn execute_plugin_node(
                &self,
                _request: agentmod_runtime_data::plugin::ExecutePluginNodeDataRequest,
            ) -> Result<(serde_json::Value, u8), PluginDataError> {
                Err(PluginDataError::Unavailable)
            }

            async fn plugin_memory(
                &self,
                _operation: String,
                _request: PluginMemoryDataRequest,
            ) -> Result<(PluginMemoryDataResult, u8), PluginDataError> {
                Err(PluginDataError::Unavailable)
            }

            async fn plugin_compaction_propose(
                &self,
                _request: agentmod_runtime_data::plugin::PluginCompactionDataRequest,
            ) -> Result<(serde_json::Value, u64, u8), PluginDataError> {
                Err(PluginDataError::Unavailable)
            }

            async fn plugin_context_transform(
                &self,
                _request: agentmod_runtime_data::plugin::PluginContextTransformDataRequest,
            ) -> Result<(serde_json::Value, u8), PluginDataError> {
                Err(PluginDataError::Unavailable)
            }

            async fn plugin_state_change(
                &self,
                _operation: &str,
                _request: agentmod_runtime_data::plugin::PluginStateChangeDataRequest,
            ) -> Result<agentmod_runtime_data::plugin::PluginAuditDataRecord, PluginDataError>
            {
                Err(PluginDataError::Unavailable)
            }

            async fn plugin_audits(
                &self,
                _session_id: String,
            ) -> Result<Vec<agentmod_runtime_data::plugin::PluginAuditDataRecord>, PluginDataError>
            {
                Err(PluginDataError::Unavailable)
            }

            async fn plugin_health(
                &self,
                _session_id: String,
            ) -> Result<agentmod_runtime_data::plugin::PluginHealthDataRecord, PluginDataError>
            {
                Err(PluginDataError::Unavailable)
            }
        }
        let management = PluginManagementLogic::new(NoData, AllowAllLifecyclePolicyGate);
        let result = management
            .plugin_memory(
                "commit_write".to_owned(),
                PluginMemoryCommand {
                    session_id: String::from("session"),
                    cancellation_id: String::from("cancel"),
                    plugin_id: String::from("fixture.memory"),
                    scope: String::from("session"),
                    query: String::new(),
                    limit: 0,
                    entries: Vec::new(),
                    write_authorized: false,
                },
            )
            .await;
        assert_eq!(result, Err(PluginManagementError::MemoryWriteUnapproved));
    }

    #[test]
    fn lifecycle_projection_exposes_declared_capabilities() {
        let health = PluginHealthDataRecord {
            loaded: 1,
            running: 0,
            observer_dropped: 0,
            pending_deliveries: 0,
        };
        let projection =
            lifecycle_projection(manifest_with("fixture.graph", "graph_node"), &health);
        assert_eq!(projection.node_executors, vec!["fixture.node"]);
        let memory = lifecycle_projection(manifest_with("fixture.mem", "memory"), &health);
        assert!(memory.memory_scopes.contains("session"));
        let compaction = lifecycle_projection(manifest_with("fixture.comp", "compaction"), &health);
        assert_eq!(
            compaction.compaction_strategy.as_deref(),
            Some("fixture.summary")
        );
        let transform = lifecycle_projection(
            manifest_with("fixture.transform", "context_transform"),
            &health,
        );
        assert_eq!(transform.context_transforms, vec!["fixture.anonymize"]);
    }
}
