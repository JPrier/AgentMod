//! Runtime-owned plugin catalog and isolated-host routing datasets.
#![allow(
    missing_docs,
    reason = "data-local plugin records remain distinct from dependency and logic types"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use agentmod_plugin_sdk as sdk;
use agentmod_primitives::ContentHash;
use agentmod_runtime_dependency::plugin as dependency;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PluginNodeExecutorDataRecord {
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PluginMemoryDataRecord {
    pub scopes: BTreeSet<String>,
    pub capabilities: BTreeSet<String>,
    pub bounded_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PluginCompactionDataRecord {
    pub strategy_id: String,
    pub idempotent: bool,
    pub bounded_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct PluginContextTransformDataRecord {
    pub transform_id: String,
    pub boundary: String,
    pub stage: u16,
    pub priority: i32,
    pub before: BTreeSet<String>,
    pub after: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginManifestDataRecord {
    pub id: String,
    pub version: String,
    pub class: String,
    pub category: String,
    pub subscribed_events: BTreeSet<String>,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub canonical_manifest_json: String,
    pub configuration: Value,
    pub node_executors: Vec<PluginNodeExecutorDataRecord>,
    pub memory: Option<PluginMemoryDataRecord>,
    pub compaction: Option<PluginCompactionDataRecord>,
    pub context_transforms: Vec<PluginContextTransformDataRecord>,
    pub observer_delivery: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginCatalogDataRecord {
    pub manifests: Vec<PluginManifestDataRecord>,
    pub plugin_set_hash: ContentHash,
}

/// Compiles exact dependency sources through the plugin SDK into data records.
///
/// # Errors
///
/// Returns [`PluginDataError::Invalid`] when parsing, set validation, runtime
/// compatibility, or data normalization fails.
pub fn compile_plugin_catalog(
    sources: &[dependency::DependencyPluginManifestSource],
    runtime_api_version: &str,
    available_capabilities: Vec<String>,
) -> Result<PluginCatalogDataRecord, PluginDataError> {
    let manifests = sources
        .iter()
        .map(|source| {
            match source.format.as_str() {
                "toml" => sdk::parse_toml(&source.contents),
                "json" => sdk::parse_json(&source.contents),
                _ => return Err(PluginDataError::Invalid),
            }
            .map_err(|_| PluginDataError::Invalid)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| PluginDataError::Invalid)?;
    let mut context = sdk::ValidationContext::new(runtime_api_version);
    context.available_capabilities = available_capabilities;
    let validated =
        sdk::validate_plugin_set(&manifests, &context).map_err(|_| PluginDataError::Invalid)?;
    let mut records = validated
        .into_iter()
        .map(|plugin| map_sdk_manifest(plugin.into_manifest()))
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by(|left, right| left.id.cmp(&right.id));
    let hash_bytes = records
        .iter()
        .flat_map(|record| {
            [
                record.id.as_bytes(),
                b"\0",
                record.version.as_bytes(),
                b"\0",
                record.canonical_manifest_json.as_bytes(),
                b"\0",
            ]
            .concat()
        })
        .collect::<Vec<_>>();
    Ok(PluginCatalogDataRecord {
        manifests: records,
        plugin_set_hash: ContentHash::digest(&hash_bytes),
    })
}

fn map_sdk_manifest(
    manifest: sdk::PluginManifest,
) -> Result<PluginManifestDataRecord, PluginDataError> {
    let (program, arguments) = match manifest.entrypoint {
        sdk::Entrypoint::Process { program, args } => (program, args),
        sdk::Entrypoint::RustBuiltin { .. } | sdk::Entrypoint::WasiComponent { .. } => {
            return Err(PluginDataError::Invalid);
        }
    };
    let schema_json = match manifest.configuration.source {
        sdk::ConfigurationSchemaSource::InlineJson { document } => document,
        sdk::ConfigurationSchemaSource::File { .. } => return Err(PluginDataError::Invalid),
    };
    let (failure_policy, max_attempts, retry_backoff_ms) = match manifest.failure_policy {
        sdk::FailurePolicy::Reject => (String::from("reject"), 1, 0),
        sdk::FailurePolicy::Cancel => (String::from("cancel"), 1, 0),
        sdk::FailurePolicy::Disable => (String::from("disable"), 1, 0),
        sdk::FailurePolicy::Continue => (String::from("continue"), 1, 0),
        sdk::FailurePolicy::Retry {
            max_attempts,
            backoff_ms,
        } => (String::from("retry"), max_attempts, backoff_ms),
    };
    let category = enum_name(manifest.category)?;
    let scope = enum_name(manifest.scope)?;
    let class = enum_name(manifest.classification)?;
    let read_authority = enum_names(manifest.authorities.read)?;
    let proposed_write_authority = enum_names(manifest.authorities.proposed_write)?;
    let observer_delivery_wire = match manifest.observer_delivery {
        sdk::PluginObserverDelivery::BestEffort => {
            serde_json::json!({ "mode": "best_effort" })
        }
        sdk::PluginObserverDelivery::AtMostOnce => {
            serde_json::json!({ "mode": "at_most_once" })
        }
        sdk::PluginObserverDelivery::AtLeastOnce {
            max_attempts,
            retry_backoff_ms,
        } => serde_json::json!({
            "mode": "at_least_once",
            "max_attempts": max_attempts,
            "retry_backoff_ms": retry_backoff_ms,
        }),
    };
    let wire_manifest = serde_json::json!({
        "schema_version": manifest.schema_version,
        "id": manifest.identity.id,
        "version": manifest.identity.version,
        "runtime_api": manifest.identity.runtime_api,
        "category": category.clone(),
        "scope": scope,
        "class": class,
        "entrypoint": {
            "program": program,
            "arguments": arguments,
        },
        "required_capabilities": manifest.required_capabilities,
        "provided_capabilities": manifest.provided_capabilities,
        "subscribed_events": manifest.subscribed_events,
        "read_authority": read_authority,
        "proposed_write_authority": proposed_write_authority,
        "tool_permissions": manifest.permissions.tools,
        "network_permissions": manifest.permissions.network,
        "after": manifest.ordering.after,
        "before": manifest.ordering.before,
        "stage": manifest.ordering.stage,
        "priority": manifest.ordering.priority,
        "timeout_ms": manifest.timeout_ms,
        "failure_policy": failure_policy,
        "max_attempts": max_attempts,
        "retry_backoff_ms": retry_backoff_ms,
        "state_migration_version": manifest.state_migration_version,
        "configuration_schema": {
            "id": manifest.configuration.schema_id,
            "version": manifest.configuration.schema_version,
            "required": manifest.configuration.required,
            "inline_json": schema_json,
        },
        "node_executors": manifest.node_executors.iter().map(|executor| {
            serde_json::json!({
                "executor_id": executor.executor_id,
                "version": executor.version,
                "node_kind": executor.node_kind,
                "runtime_api": executor.runtime_api,
                "required_capabilities": executor.required_capabilities,
                "input_schema": executor.input_schema,
                "output_schema": executor.output_schema,
                "timeout_ms": executor.timeout_ms,
                "failure_policy": executor.failure_policy,
                "idempotent": executor.idempotent,
                "external_effect": executor.external_effect,
                "read_authority": executor.read_authority,
                "state_scope": executor.state_scope,
            })
        }).collect::<Vec<_>>(),
        "memory": manifest.memory.as_ref().map(|memory| {
            serde_json::json!({
                "scopes": memory.scopes,
                "capabilities": memory.capabilities,
                "bounded_bytes": memory.bounded_bytes,
            })
        }),
        "compaction": manifest.compaction.as_ref().map(|compaction| {
            serde_json::json!({
                "strategy_id": compaction.strategy_id,
                "idempotent": compaction.idempotent,
                "bounded_bytes": compaction.bounded_bytes,
            })
        }),
        "context_transforms": manifest.context_transforms.iter().map(|transform| {
            serde_json::json!({
                "transform_id": transform.transform_id,
                "boundary": enum_name(transform.boundary).expect("boundary"),
                "stage": transform.stage,
                "priority": transform.priority,
                "before": transform.before,
                "after": transform.after,
            })
        }).collect::<Vec<_>>(),
        "observer_delivery": observer_delivery_wire,
    });
    let id = wire_manifest
        .get("id")
        .and_then(Value::as_str)
        .ok_or(PluginDataError::Invalid)?
        .to_owned();
    let version = wire_manifest
        .get("version")
        .and_then(Value::as_str)
        .ok_or(PluginDataError::Invalid)?
        .to_owned();
    let subscribed_events = wire_manifest
        .get("subscribed_events")
        .and_then(Value::as_array)
        .ok_or(PluginDataError::Invalid)?
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let class = wire_manifest
        .get("class")
        .and_then(Value::as_str)
        .ok_or(PluginDataError::Invalid)?
        .to_owned();
    let node_executors = wire_manifest
        .get("node_executors")
        .and_then(Value::as_array)
        .ok_or(PluginDataError::Invalid)?
        .iter()
        .map(|value| serde_json::from_value(value.clone()).map_err(|_| PluginDataError::Invalid))
        .collect::<Result<Vec<PluginNodeExecutorDataRecord>, _>>()?;
    let memory = wire_manifest
        .get("memory")
        .cloned()
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value).map_err(|_| PluginDataError::Invalid))
        .transpose()?;
    let compaction = wire_manifest
        .get("compaction")
        .cloned()
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value).map_err(|_| PluginDataError::Invalid))
        .transpose()?;
    let context_transforms = wire_manifest
        .get("context_transforms")
        .and_then(Value::as_array)
        .ok_or(PluginDataError::Invalid)?
        .iter()
        .map(|value| serde_json::from_value(value.clone()).map_err(|_| PluginDataError::Invalid))
        .collect::<Result<Vec<PluginContextTransformDataRecord>, _>>()?;
    let observer_delivery = wire_manifest
        .get("observer_delivery")
        .and_then(|value| value.get("mode"))
        .and_then(Value::as_str)
        .ok_or(PluginDataError::Invalid)?
        .to_owned();
    Ok(PluginManifestDataRecord {
        id,
        version,
        class,
        category: category.clone(),
        subscribed_events,
        timeout_ms: manifest.timeout_ms,
        failure_policy,
        canonical_manifest_json: serde_json::to_string(&wire_manifest)
            .map_err(|_| PluginDataError::Invalid)?,
        configuration: Value::Object(serde_json::Map::new()),
        node_executors,
        memory,
        compaction,
        context_transforms,
        observer_delivery,
    })
}

fn enum_name<T: serde::Serialize>(value: T) -> Result<String, PluginDataError> {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or(PluginDataError::Invalid)
}

fn enum_names<T: serde::Serialize>(values: Vec<T>) -> Result<Vec<String>, PluginDataError> {
    values.into_iter().map(enum_name).collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivatePluginsDataRequest {
    pub session_id: String,
    pub plugin_ids: Vec<String>,
    pub runtime_api_version: String,
    pub capabilities: BTreeSet<String>,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivatedPluginsDataRecord {
    pub plugin_ids: Vec<String>,
    pub plugins: Vec<ActivatedPluginDataRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivatedPluginDataRecord {
    pub id: String,
    pub class: String,
    pub subscribed_events: BTreeSet<String>,
    pub timeout_ms: u64,
    pub failure_policy: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InvokePluginDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub proposal_type: String,
    pub proposal: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PluginDecisionDataRecord {
    Continue(Value),
    Replace(Value),
    Reject(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObservePluginDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub event_range_start: u64,
    pub event_range_end: u64,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginObservationDataRecord {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutePluginNodeDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub executor_id: String,
    pub node_id: String,
    pub node_kind: String,
    pub input: Value,
    pub variables: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub scope: String,
    pub query: String,
    pub limit: usize,
    pub entries: Vec<PluginMemoryItemDataRecord>,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryItemDataRecord {
    pub reference: String,
    pub content: String,
    pub score: Option<f64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PluginMemoryDataResult {
    Describe {
        scopes: BTreeSet<String>,
        capabilities: BTreeSet<String>,
        bounded_bytes: u64,
    },
    Retrieve {
        items: Vec<PluginMemoryItemDataRecord>,
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

#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub source_range_start: u64,
    pub source_range_end: u64,
    pub source_range_hash: String,
    pub current_entries: Value,
    pub proposal: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginContextTransformDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub boundary: String,
    pub payload: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginStateChangeDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub reason: Option<String>,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginAuditDataRecord {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginHealthDataRecord {
    pub loaded: usize,
    pub running: usize,
    pub observer_dropped: u64,
    pub pending_deliveries: usize,
}

#[async_trait]
pub trait PluginDataPort: Send + Sync {
    /// Returns all catalog plugin IDs (default: none when unimplemented).
    fn plugin_ids(&self) -> Vec<String> {
        Vec::new()
    }

    /// Returns the manifest record for a plugin ID.
    fn manifest(&self, plugin_id: &str) -> Option<PluginManifestDataRecord> {
        let _ = plugin_id;
        None
    }

    async fn activate_plugins(
        &self,
        request: ActivatePluginsDataRequest,
    ) -> Result<ActivatedPluginsDataRecord, PluginDataError>;

    async fn invoke_plugin(
        &self,
        request: InvokePluginDataRequest,
    ) -> Result<PluginDecisionDataRecord, PluginDataError>;

    async fn observe_event(
        &self,
        request: ObservePluginDataRequest,
    ) -> Result<PluginObservationDataRecord, PluginDataError>;

    async fn execute_plugin_node(
        &self,
        request: ExecutePluginNodeDataRequest,
    ) -> Result<(Value, u8), PluginDataError>;

    async fn plugin_memory(
        &self,
        operation: String,
        request: PluginMemoryDataRequest,
    ) -> Result<(PluginMemoryDataResult, u8), PluginDataError>;

    async fn plugin_compaction_propose(
        &self,
        request: PluginCompactionDataRequest,
    ) -> Result<(Value, u64, u8), PluginDataError>;

    async fn plugin_context_transform(
        &self,
        request: PluginContextTransformDataRequest,
    ) -> Result<(Value, u8), PluginDataError>;

    async fn plugin_state_change(
        &self,
        operation: &str,
        request: PluginStateChangeDataRequest,
    ) -> Result<PluginAuditDataRecord, PluginDataError>;

    async fn plugin_audits(
        &self,
        session_id: String,
    ) -> Result<Vec<PluginAuditDataRecord>, PluginDataError>;

    async fn plugin_health(
        &self,
        session_id: String,
    ) -> Result<PluginHealthDataRecord, PluginDataError>;
}

#[derive(Clone)]
pub struct RuntimePluginData {
    dependency: Arc<dyn dependency::RuntimePluginDependencyPort>,
    manifests: Arc<BTreeMap<String, PluginManifestDataRecord>>,
    activated: Arc<Mutex<BTreeMap<String, BTreeSet<String>>>>,
}

impl std::fmt::Debug for RuntimePluginData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimePluginData")
            .field("manifest_count", &self.manifests.len())
            .finish_non_exhaustive()
    }
}

impl RuntimePluginData {
    #[must_use]
    pub fn new(
        dependency: Arc<dyn dependency::RuntimePluginDependencyPort>,
        manifests: Vec<PluginManifestDataRecord>,
    ) -> Self {
        Self {
            dependency,
            manifests: Arc::new(
                manifests
                    .into_iter()
                    .map(|manifest| (manifest.id.clone(), manifest))
                    .collect(),
            ),
            activated: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Returns the manifest record for a plugin ID.
    #[must_use]
    pub fn manifest(&self, plugin_id: &str) -> Option<PluginManifestDataRecord> {
        self.manifests.get(plugin_id).cloned()
    }

    /// Returns all catalog plugin IDs in stable order.
    #[must_use]
    pub fn plugin_ids(&self) -> Vec<String> {
        self.manifests.keys().cloned().collect()
    }

    /// Converts catalog graph-node plugins into node-executor registrations
    /// consumed by the runtime node-executor registry at the composition root.
    #[must_use]
    pub fn node_executor_registrations(
        &self,
    ) -> Vec<crate::node_executor::RegisterNodeExecutorDataRecord> {
        let mut registrations = Vec::new();
        for manifest in self.manifests.values() {
            for executor in &manifest.node_executors {
                registrations.push(crate::node_executor::RegisterNodeExecutorDataRecord {
                    id: format!("plugin.{}", executor.executor_id),
                    version: executor.version.clone(),
                    runtime_api: executor.runtime_api.clone(),
                    node_kind: executor.node_kind.clone(),
                    capabilities: executor.required_capabilities.clone(),
                    source: crate::node_executor::NodeExecutorSourceData::Plugin {
                        plugin_id: manifest.id.clone(),
                    },
                    boundary: crate::node_executor::NodeExecutorBoundaryData::PluginHost,
                    available: true,
                });
            }
        }
        registrations
    }

    fn selected(
        &self,
        plugin_ids: &[String],
    ) -> Result<Vec<PluginManifestDataRecord>, PluginDataError> {
        let mut unique = BTreeSet::new();
        plugin_ids
            .iter()
            .map(|id| {
                if !unique.insert(id.clone()) {
                    return Err(PluginDataError::Invalid);
                }
                self.manifests
                    .get(id)
                    .cloned()
                    .ok_or(PluginDataError::Unavailable)
            })
            .collect()
    }
}

#[async_trait]
impl PluginDataPort for RuntimePluginData {
    fn plugin_ids(&self) -> Vec<String> {
        self.plugin_ids()
    }

    fn manifest(&self, plugin_id: &str) -> Option<PluginManifestDataRecord> {
        self.manifest(plugin_id)
    }

    async fn activate_plugins(
        &self,
        request: ActivatePluginsDataRequest,
    ) -> Result<ActivatedPluginsDataRecord, PluginDataError> {
        let selected = self.selected(&request.plugin_ids)?;
        self.dependency
            .negotiate(
                request.session_id.clone(),
                request.runtime_api_version,
                request.capabilities,
            )
            .await
            .map_err(|error| map_operation_error("negotiate", error))?;
        let order = self
            .dependency
            .validate_set(
                request.session_id.clone(),
                selected
                    .iter()
                    .map(|manifest| manifest.canonical_manifest_json.clone())
                    .collect(),
            )
            .await
            .map_err(|error| map_operation_error("validate_set", error))?;
        let already_active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .cloned()
            .unwrap_or_default();
        for manifest in &selected {
            if already_active.contains(&manifest.id) {
                continue;
            }
            let loaded = self
                .dependency
                .load(dependency::DependencyPluginLoadRequest {
                    session_id: request.session_id.clone(),
                    manifest_json: manifest.canonical_manifest_json.clone(),
                    configuration: manifest.configuration.clone(),
                    cancellation_id: request.cancellation_id.clone(),
                })
                .await
                .map_err(|error| map_operation_error("load", error))?;
            if loaded.plugin_id != manifest.id {
                return Err(PluginDataError::Invalid);
            }
        }
        self.activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .entry(request.session_id)
            .or_default()
            .extend(order.iter().cloned());
        let plugins = order
            .iter()
            .map(|id| {
                self.manifests
                    .get(id)
                    .map(|manifest| ActivatedPluginDataRecord {
                        id: manifest.id.clone(),
                        class: manifest.class.clone(),
                        subscribed_events: manifest.subscribed_events.clone(),
                        timeout_ms: manifest.timeout_ms,
                        failure_policy: manifest.failure_policy.clone(),
                    })
                    .ok_or(PluginDataError::Invalid)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ActivatedPluginsDataRecord {
            plugin_ids: order,
            plugins,
        })
    }

    async fn invoke_plugin(
        &self,
        request: InvokePluginDataRequest,
    ) -> Result<PluginDecisionDataRecord, PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let (decision, _) = self
            .dependency
            .invoke(dependency::DependencyPluginInvocationRequest {
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                handler: request.handler,
                kind: request.proposal_type,
                payload: request.proposal,
                readable_state: request.readable_state,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("invoke", error))?;
        Ok(match decision {
            dependency::DependencyPluginDecision::Continue(value) => {
                PluginDecisionDataRecord::Continue(value)
            }
            dependency::DependencyPluginDecision::Replace(value) => {
                PluginDecisionDataRecord::Replace(value)
            }
            dependency::DependencyPluginDecision::Reject(reason) => {
                PluginDecisionDataRecord::Reject(reason)
            }
        })
    }

    async fn observe_event(
        &self,
        request: ObservePluginDataRequest,
    ) -> Result<PluginObservationDataRecord, PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let result = self
            .dependency
            .observe(dependency::DependencyPluginObservationRequest {
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                handler: request.handler,
                event_type: request.event_type,
                event: request.event,
                event_range_start: request.event_range_start,
                event_range_end: request.event_range_end,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("observe", error))?;
        Ok(PluginObservationDataRecord {
            accepted: result.accepted,
            queue_depth: result.queue_depth,
            dropped: result.dropped,
        })
    }

    async fn execute_plugin_node(
        &self,
        request: ExecutePluginNodeDataRequest,
    ) -> Result<(Value, u8), PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        self.dependency
            .execute_node(dependency::DependencyPluginNodeExecutionRequest {
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                executor_id: request.executor_id,
                node_id: request.node_id,
                node_kind: request.node_kind,
                input: request.input,
                variables: request.variables,
                readable_state: request.readable_state,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("execute_node", error))
    }

    async fn plugin_memory(
        &self,
        operation: String,
        request: PluginMemoryDataRequest,
    ) -> Result<(PluginMemoryDataResult, u8), PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let (result, attempts) = self
            .dependency
            .memory(
                operation,
                dependency::DependencyPluginMemoryRequest {
                    session_id: request.session_id,
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    scope: request.scope,
                    query: request.query,
                    limit: request.limit,
                    entries: request
                        .entries
                        .into_iter()
                        .map(|item| dependency::DependencyPluginMemoryItem {
                            reference: item.reference,
                            content: item.content,
                            score: item.score,
                            created_at_ms: item.created_at_ms,
                        })
                        .collect(),
                    cancellation_id: request.cancellation_id,
                },
            )
            .await
            .map_err(|error| map_operation_error("memory", error))?;
        let result = match result {
            dependency::DependencyPluginMemoryResult::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            } => PluginMemoryDataResult::Describe {
                scopes,
                capabilities,
                bounded_bytes,
            },
            dependency::DependencyPluginMemoryResult::Retrieve { items } => {
                PluginMemoryDataResult::Retrieve {
                    items: items
                        .into_iter()
                        .map(|item| PluginMemoryItemDataRecord {
                            reference: item.reference,
                            content: item.content,
                            score: item.score,
                            created_at_ms: item.created_at_ms,
                        })
                        .collect(),
                }
            }
            dependency::DependencyPluginMemoryResult::Commit {
                retained,
                references,
            } => PluginMemoryDataResult::Commit {
                retained,
                references,
            },
            dependency::DependencyPluginMemoryResult::Health {
                healthy,
                item_count,
                retained_bytes,
            } => PluginMemoryDataResult::Health {
                healthy,
                item_count,
                retained_bytes,
            },
        };
        Ok((result, attempts))
    }

    async fn plugin_compaction_propose(
        &self,
        request: PluginCompactionDataRequest,
    ) -> Result<(Value, u64, u8), PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        self.dependency
            .compaction_propose(dependency::DependencyPluginCompactionRequest {
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                source_range_start: request.source_range_start,
                source_range_end: request.source_range_end,
                source_range_hash: request.source_range_hash,
                current_entries: request.current_entries,
                proposal: request.proposal,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("compaction_propose", error))
    }

    async fn plugin_context_transform(
        &self,
        request: PluginContextTransformDataRequest,
    ) -> Result<(Value, u8), PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let boundary = match request.boundary.as_str() {
            "before_memory_retrieval" => {
                dependency::DependencyPluginContextTransformBoundary::BeforeMemoryRetrieval
            }
            "after_memory_retrieval" => {
                dependency::DependencyPluginContextTransformBoundary::AfterMemoryRetrieval
            }
            "before_compaction" => {
                dependency::DependencyPluginContextTransformBoundary::BeforeCompaction
            }
            "after_compaction" => {
                dependency::DependencyPluginContextTransformBoundary::AfterCompaction
            }
            "before_provider_projection" => {
                dependency::DependencyPluginContextTransformBoundary::BeforeProviderProjection
            }
            "before_turn_completion" => {
                dependency::DependencyPluginContextTransformBoundary::BeforeTurnCompletion
            }
            _ => return Err(PluginDataError::Invalid),
        };
        self.dependency
            .context_transform(dependency::DependencyPluginContextTransformRequest {
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                transform_id: request.transform_id,
                boundary,
                payload: request.payload,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("context_transform", error))
    }

    async fn plugin_state_change(
        &self,
        operation: &str,
        request: PluginStateChangeDataRequest,
    ) -> Result<PluginAuditDataRecord, PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let audit = self
            .dependency
            .state_change(
                operation,
                dependency::DependencyPluginStateChangeRequest {
                    session_id: request.session_id,
                    plugin_id: request.plugin_id,
                    reason: request.reason,
                    cancellation_id: request.cancellation_id,
                },
            )
            .await
            .map_err(|error| map_operation_error("state_change", error))?;
        Ok(PluginAuditDataRecord {
            plugin_id: audit.plugin_id,
            invocation_id: audit.invocation_id,
            operation: audit.operation,
            outcome: audit.outcome,
            attempts: audit.attempts,
        })
    }

    async fn plugin_audits(
        &self,
        session_id: String,
    ) -> Result<Vec<PluginAuditDataRecord>, PluginDataError> {
        let audits = self
            .dependency
            .audits(session_id)
            .await
            .map_err(|error| map_operation_error("audits", error))?;
        Ok(audits
            .into_iter()
            .map(|audit| PluginAuditDataRecord {
                plugin_id: audit.plugin_id,
                invocation_id: audit.invocation_id,
                operation: audit.operation,
                outcome: audit.outcome,
                attempts: audit.attempts,
            })
            .collect())
    }

    async fn plugin_health(
        &self,
        session_id: String,
    ) -> Result<PluginHealthDataRecord, PluginDataError> {
        let health = self
            .dependency
            .health(session_id)
            .await
            .map_err(|error| map_operation_error("health", error))?;
        Ok(PluginHealthDataRecord {
            loaded: health.loaded,
            running: health.running,
            observer_dropped: health.observer_dropped,
            pending_deliveries: health.pending_deliveries,
        })
    }
}

fn map_error(operation: &str, error: dependency::PluginDependencyError) -> PluginDataError {
    match error {
        dependency::PluginDependencyError::InvalidConfiguration
        | dependency::PluginDependencyError::InvalidRequest
        | dependency::PluginDependencyError::FrameTooLarge
        | dependency::PluginDependencyError::InvalidResponse
        | dependency::PluginDependencyError::Authorization => PluginDataError::Invalid,
        dependency::PluginDependencyError::Rejected { code, retryable } => {
            PluginDataError::Rejected {
                operation: operation.to_owned(),
                code,
                retryable,
            }
        }
        dependency::PluginDependencyError::Unavailable
        | dependency::PluginDependencyError::Timeout
        | dependency::PluginDependencyError::Incompatible
        | dependency::PluginDependencyError::Clock => PluginDataError::Unavailable,
    }
}

fn map_operation_error(
    operation: &str,
    error: dependency::PluginDependencyError,
) -> PluginDataError {
    map_error(operation, error)
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginDataError {
    #[error("plugin data request is invalid")]
    Invalid,
    #[error("plugin data dependency is unavailable")]
    Unavailable,
    #[error("plugin is not active for the session")]
    Inactive,
    #[error("plugin host rejected `{operation}` with `{code}`")]
    Rejected {
        operation: String,
        code: String,
        retryable: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(contents: String) -> dependency::DependencyPluginManifestSource {
        dependency::DependencyPluginManifestSource {
            locator: String::from("fixture.toml"),
            format: String::from("toml"),
            contents,
        }
    }

    fn observer(proposed_write: &str) -> String {
        format!(
            r#"
schema_version = 1
category = "observer"
scope = "session"
classification = "observer"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = ["tool.execution_completed"]
timeout_ms = 1000
state_migration_version = 1

[identity]
id = "fixture.observer"
version = "1.0.0"
runtime_api = "^0.1"

[entrypoint]
kind = "process"
program = "fixture-worker"
args = []

[authorities]
read = ["session_state"]
proposed_write = {proposed_write}

[permissions]
tools = []
network = []

[ordering]
stage = 100
priority = 0
before = []
after = []

[configuration]
schema_id = "fixture.observer.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{{"type":"object","additionalProperties":false}}'

[failure_policy]
kind = "continue"
"#
        )
    }

    fn graph_node() -> String {
        r#"
schema_version = 1
category = "graph_node"
scope = "session"
classification = "blocking"
trust = "approved_third_party"
isolation = "process"
required_capabilities = ["events"]
provided_capabilities = []
subscribed_events = []
timeout_ms = 1000
state_migration_version = 1

[identity]
id = "fixture.graph-node"
version = "1.0.0"
runtime_api = "^0.1"

[entrypoint]
kind = "process"
program = "fixture-worker"
args = []

[authorities]
read = ["session_state"]
proposed_write = []

[permissions]
tools = []
network = []

[ordering]
stage = 100
priority = 0
before = []
after = []

[configuration]
schema_id = "fixture.graph-node.config"
schema_version = 1
required = false

[configuration.source]
kind = "inline_json"
document = '{"type":"object","additionalProperties":false}'

[failure_policy]
kind = "reject"

[[node_executors]]
executor_id = "fixture.node"
version = "1.0.0"
node_kind = "emit_event"
runtime_api = "^1.0"
required_capabilities = ["events"]
input_schema = '{"type":"object"}'
output_schema = '{"type":"object"}'
timeout_ms = 500
failure_policy = "reject"
idempotent = true
external_effect = false
read_authority = ["session_state"]
state_scope = "plugin_state"
"#
        .to_owned()
    }

    #[test]
    fn catalog_compiles_observer_without_canonical_write_authority() {
        let catalog = compile_plugin_catalog(
            &[source(observer("[]"))],
            "0.1.0",
            vec![String::from("events")],
        )
        .expect("valid observer catalog");
        assert_eq!(catalog.manifests.len(), 1);
        assert_eq!(catalog.manifests[0].class, "observer");
        assert_ne!(catalog.plugin_set_hash, ContentHash::digest(b""));
    }

    #[test]
    fn catalog_rejects_observer_canonical_write_request() {
        assert_eq!(
            compile_plugin_catalog(
                &[source(observer("[\"canonical_state\"]"))],
                "0.1.0",
                vec![String::from("events")],
            ),
            Err(PluginDataError::Invalid)
        );
    }

    #[test]
    fn catalog_compiles_graph_node_executor_declaration() {
        let catalog = compile_plugin_catalog(
            &[source(graph_node())],
            "0.1.0",
            vec![String::from("events")],
        )
        .expect("valid graph node catalog");
        assert_eq!(catalog.manifests.len(), 1);
        assert_eq!(catalog.manifests[0].class, "blocking");
        assert_eq!(catalog.manifests[0].node_executors.len(), 1);
        assert_eq!(
            catalog.manifests[0].node_executors[0].executor_id,
            "fixture.node"
        );
        assert!(catalog.manifests[0].node_executors[0].idempotent);
        let wire: Value = serde_json::from_str(&catalog.manifests[0].canonical_manifest_json)
            .expect("wire manifest");
        assert_eq!(wire["node_executors"][0]["node_kind"], "emit_event");
    }
}
