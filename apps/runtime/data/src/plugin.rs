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
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginManifestDataRecord {
    pub id: String,
    pub version: String,
    pub category: String,
    pub class: String,
    pub provided_capabilities: BTreeSet<String>,
    pub subscribed_events: BTreeSet<String>,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub canonical_manifest_json: String,
    pub configuration: Value,
    pub configuration_reference: ContentHash,
    pub node_executors: Vec<PluginNodeExecutorDataRecord>,
    pub context_transforms: Vec<PluginContextTransformDataRecord>,
    pub memory_providers: Vec<PluginMemoryProviderDataRecord>,
    pub compactors: Vec<PluginCompactorDataRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryProviderDataRecord {
    pub provider_id: String,
    pub version: String,
    pub runtime_api: String,
    pub capabilities: BTreeSet<String>,
    pub retrieve: PluginMemoryOperationDataRecord,
    pub write: Option<PluginMemoryOperationDataRecord>,
    pub declaration_hash: ContentHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginMemoryOperationDataRecord {
    pub handler: String,
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
pub struct PluginCompactorDataRecord {
    pub compactor_id: String,
    pub version: String,
    pub runtime_api: String,
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
    pub declaration_hash: ContentHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginContextTransformDataRecord {
    pub plugin_version: String,
    pub transform_id: String,
    pub version: String,
    pub runtime_api: String,
    pub handler: String,
    pub lifecycle: String,
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
    pub declaration_hash: ContentHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeExecutorDataRecord {
    pub plugin_version: String,
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
    pub declaration_hash: ContentHash,
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

#[allow(
    clippy::too_many_lines,
    reason = "the plugin protocol boundary maps every validated manifest field explicitly"
)]
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
    let provided_capabilities = manifest.provided_capabilities.iter().cloned().collect();
    let read_authority = enum_names(manifest.authorities.read)?;
    let proposed_write_authority = enum_names(manifest.authorities.proposed_write)?;
    let node_executors = manifest
        .node_executors
        .iter()
        .map(|executor| map_sdk_node_executor(&manifest.identity.version, executor))
        .collect::<Result<Vec<_>, _>>()?;
    let mut context_transforms = manifest
        .context_transforms
        .iter()
        .map(|transform| map_sdk_context_transform(&manifest.identity.version, transform))
        .collect::<Result<Vec<_>, _>>()?;
    context_transforms.sort_by(|left, right| {
        left.transform_id
            .cmp(&right.transform_id)
            .then_with(|| left.version.cmp(&right.version))
    });
    let mut memory_providers = manifest
        .memory_providers
        .iter()
        .map(map_sdk_memory_provider)
        .collect::<Result<Vec<_>, _>>()?;
    memory_providers.sort_by(|left, right| {
        left.provider_id
            .cmp(&right.provider_id)
            .then_with(|| left.version.cmp(&right.version))
    });
    let mut compactors = manifest
        .compactors
        .iter()
        .map(map_sdk_compactor)
        .collect::<Result<Vec<_>, _>>()?;
    compactors.sort_by(|left, right| {
        left.compactor_id
            .cmp(&right.compactor_id)
            .then_with(|| left.version.cmp(&right.version))
    });
    let wire_node_executors = node_executors
        .iter()
        .map(|executor| {
            serde_json::json!({
                "executor_id": executor.executor_id,
                "version": executor.version,
                "runtime_api": executor.runtime_api,
                "node_kind": executor.node_kind,
                "handler": executor.handler,
                "capabilities": executor.capabilities,
                "input_schema": executor.input_schema,
                "output_schema": executor.output_schema,
                "timeout_ms": executor.timeout_ms,
                "failure_policy": executor.failure_policy,
                "max_attempts": executor.max_attempts,
                "retry_backoff_ms": executor.retry_backoff_ms,
                "idempotency": if executor.idempotent { "idempotent" } else { "non_idempotent" },
                "tool_permissions": executor.tool_permissions,
                "network_permissions": executor.network_permissions,
                "state_scope": executor.state_scope,
                "external_effects": executor.external_effects,
            })
        })
        .collect::<Vec<_>>();
    let wire_context_transforms = context_transforms
        .iter()
        .map(|transform| {
            serde_json::json!({
                "transform_id": transform.transform_id,
                "version": transform.version,
                "runtime_api": transform.runtime_api,
                "handler": transform.handler,
                "lifecycle": transform.lifecycle,
                "capabilities": transform.capabilities,
                "input_schema": transform.input_schema,
                "output_schema": transform.output_schema,
                "timeout_ms": transform.timeout_ms,
                "failure_policy": transform.failure_policy,
                "max_attempts": transform.max_attempts,
                "retry_backoff_ms": transform.retry_backoff_ms,
                "idempotency": if transform.idempotent { "idempotent" } else { "non_idempotent" },
                "tool_permissions": transform.tool_permissions,
                "network_permissions": transform.network_permissions,
                "state_scope": transform.state_scope,
                "external_effects": transform.external_effects,
            })
        })
        .collect::<Vec<_>>();
    let mut wire_manifest = serde_json::json!({
        "schema_version": manifest.schema_version,
        "id": manifest.identity.id,
        "version": manifest.identity.version,
        "runtime_api": manifest.identity.runtime_api,
        "category": category,
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
        "node_executors": wire_node_executors,
    });
    if !wire_context_transforms.is_empty() {
        wire_manifest
            .as_object_mut()
            .ok_or(PluginDataError::Invalid)?
            .insert(
                String::from("context_transforms"),
                Value::Array(wire_context_transforms),
            );
    }
    if !manifest.memory_providers.is_empty() {
        wire_manifest
            .as_object_mut()
            .ok_or(PluginDataError::Invalid)?
            .insert(
                String::from("memory_providers"),
                serde_json::to_value(&manifest.memory_providers)
                    .map_err(|_| PluginDataError::Invalid)?,
            );
    }
    if !manifest.compactors.is_empty() {
        wire_manifest
            .as_object_mut()
            .ok_or(PluginDataError::Invalid)?
            .insert(
                String::from("compactors"),
                serde_json::to_value(&manifest.compactors).map_err(|_| PluginDataError::Invalid)?,
            );
    }
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
    let configuration = Value::Object(serde_json::Map::new());
    let configuration_reference = ContentHash::digest(
        &serde_json::to_vec(&configuration).map_err(|_| PluginDataError::Invalid)?,
    );
    Ok(PluginManifestDataRecord {
        id,
        version,
        category,
        class,
        provided_capabilities,
        subscribed_events,
        timeout_ms: manifest.timeout_ms,
        failure_policy,
        canonical_manifest_json: serde_json::to_string(&wire_manifest)
            .map_err(|_| PluginDataError::Invalid)?,
        configuration,
        configuration_reference,
        node_executors,
        context_transforms,
        memory_providers,
        compactors,
    })
}

fn map_sdk_memory_provider(
    provider: &sdk::MemoryProviderManifest,
) -> Result<PluginMemoryProviderDataRecord, PluginDataError> {
    Ok(PluginMemoryProviderDataRecord {
        provider_id: provider.provider_id.clone(),
        version: provider.version.clone(),
        runtime_api: provider.runtime_api.clone(),
        capabilities: provider.capabilities.iter().cloned().collect(),
        retrieve: map_sdk_memory_operation(
            &provider.retrieve.handler,
            &provider.retrieve.input_schema,
            &provider.retrieve.output_schema,
            provider.retrieve.timeout_ms,
            &provider.retrieve.failure_policy,
            provider.retrieve.idempotency,
            &provider.retrieve.required_permissions,
            provider.retrieve.state_scope,
            provider.retrieve.external_effects,
        )?,
        write: provider
            .write
            .as_ref()
            .map(|write| {
                map_sdk_memory_operation(
                    &write.handler,
                    &write.input_schema,
                    &write.output_schema,
                    write.timeout_ms,
                    &write.failure_policy,
                    write.idempotency,
                    &write.required_permissions,
                    write.state_scope,
                    write.external_effects,
                )
            })
            .transpose()?,
        declaration_hash: ContentHash::digest(
            &provider
                .declaration_hash_input()
                .map_err(|_| PluginDataError::Invalid)?,
        ),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the data boundary copies every validated operation field explicitly"
)]
fn map_sdk_memory_operation(
    handler: &str,
    input_schema: &str,
    output_schema: &str,
    timeout_ms: u64,
    failure_policy: &sdk::FailurePolicy,
    idempotency: sdk::PluginOperationIdempotency,
    permissions: &sdk::PermissionManifest,
    state_scope: sdk::PluginScope,
    external_effects: bool,
) -> Result<PluginMemoryOperationDataRecord, PluginDataError> {
    let (failure_policy, max_attempts, retry_backoff_ms) =
        normalized_failure_policy(failure_policy);
    Ok(PluginMemoryOperationDataRecord {
        handler: handler.to_owned(),
        input_schema: input_schema.to_owned(),
        output_schema: output_schema.to_owned(),
        timeout_ms,
        failure_policy: failure_policy.to_owned(),
        max_attempts,
        retry_backoff_ms,
        idempotent: idempotency == sdk::PluginOperationIdempotency::Idempotent,
        tool_permissions: permissions.tools.iter().cloned().collect(),
        network_permissions: permissions.network.iter().cloned().collect(),
        state_scope: enum_name(state_scope)?,
        external_effects,
    })
}

fn map_sdk_compactor(
    compactor: &sdk::CompactorManifest,
) -> Result<PluginCompactorDataRecord, PluginDataError> {
    let (failure_policy, max_attempts, retry_backoff_ms) =
        normalized_failure_policy(&compactor.failure_policy);
    Ok(PluginCompactorDataRecord {
        compactor_id: compactor.compactor_id.clone(),
        version: compactor.version.clone(),
        runtime_api: compactor.runtime_api.clone(),
        handler: compactor.handler.clone(),
        capabilities: compactor.capabilities.iter().cloned().collect(),
        input_schema: compactor.input_schema.clone(),
        output_schema: compactor.output_schema.clone(),
        timeout_ms: compactor.timeout_ms,
        failure_policy: failure_policy.to_owned(),
        max_attempts,
        retry_backoff_ms,
        idempotent: compactor.idempotency == sdk::PluginOperationIdempotency::Idempotent,
        tool_permissions: compactor
            .required_permissions
            .tools
            .iter()
            .cloned()
            .collect(),
        network_permissions: compactor
            .required_permissions
            .network
            .iter()
            .cloned()
            .collect(),
        state_scope: enum_name(compactor.state_scope)?,
        external_effects: compactor.external_effects,
        declaration_hash: ContentHash::digest(
            &compactor
                .declaration_hash_input()
                .map_err(|_| PluginDataError::Invalid)?,
        ),
    })
}

const fn normalized_failure_policy(policy: &sdk::FailurePolicy) -> (&'static str, u8, u64) {
    match policy {
        sdk::FailurePolicy::Reject => ("reject", 1, 0),
        sdk::FailurePolicy::Cancel => ("cancel", 1, 0),
        sdk::FailurePolicy::Disable => ("disable", 1, 0),
        sdk::FailurePolicy::Continue => ("continue", 1, 0),
        sdk::FailurePolicy::Retry {
            max_attempts,
            backoff_ms,
        } => ("retry", *max_attempts, *backoff_ms),
    }
}

fn map_sdk_context_transform(
    plugin_version: &str,
    transform: &sdk::ContextTransformManifest,
) -> Result<PluginContextTransformDataRecord, PluginDataError> {
    let (failure_policy, max_attempts, retry_backoff_ms) = match &transform.failure_policy {
        sdk::FailurePolicy::Reject => ("reject", 1, 0),
        sdk::FailurePolicy::Cancel => ("cancel", 1, 0),
        sdk::FailurePolicy::Disable => ("disable", 1, 0),
        sdk::FailurePolicy::Continue => ("continue", 1, 0),
        sdk::FailurePolicy::Retry {
            max_attempts,
            backoff_ms,
        } => ("retry", *max_attempts, *backoff_ms),
    };
    let canonical = serde_json::to_vec(transform).map_err(|_| PluginDataError::Invalid)?;
    Ok(PluginContextTransformDataRecord {
        plugin_version: plugin_version.to_owned(),
        transform_id: transform.transform_id.clone(),
        version: transform.version.clone(),
        runtime_api: transform.runtime_api.clone(),
        handler: transform.handler.clone(),
        lifecycle: enum_name(transform.lifecycle)?,
        capabilities: transform.capabilities.iter().cloned().collect(),
        input_schema: transform.input_schema.clone(),
        output_schema: transform.output_schema.clone(),
        timeout_ms: transform.timeout_ms,
        failure_policy: failure_policy.to_owned(),
        max_attempts,
        retry_backoff_ms,
        idempotent: transform.idempotency == sdk::ContextTransformIdempotency::Idempotent,
        tool_permissions: transform
            .required_permissions
            .tools
            .iter()
            .cloned()
            .collect(),
        network_permissions: transform
            .required_permissions
            .network
            .iter()
            .cloned()
            .collect(),
        state_scope: enum_name(transform.state_scope)?,
        external_effects: transform.external_effects,
        declaration_hash: ContentHash::digest(&canonical),
    })
}

fn map_sdk_node_executor(
    plugin_version: &str,
    executor: &sdk::NodeExecutorManifest,
) -> Result<PluginNodeExecutorDataRecord, PluginDataError> {
    let (failure_policy, max_attempts, retry_backoff_ms) = match &executor.failure_policy {
        sdk::FailurePolicy::Reject => ("reject", 1, 0),
        sdk::FailurePolicy::Cancel => ("cancel", 1, 0),
        sdk::FailurePolicy::Disable => ("disable", 1, 0),
        sdk::FailurePolicy::Continue => ("continue", 1, 0),
        sdk::FailurePolicy::Retry {
            max_attempts,
            backoff_ms,
        } => ("retry", *max_attempts, *backoff_ms),
    };
    let state_scope = enum_name(executor.state_scope)?;
    let canonical = serde_json::to_vec(executor).map_err(|_| PluginDataError::Invalid)?;
    Ok(PluginNodeExecutorDataRecord {
        plugin_version: plugin_version.to_owned(),
        executor_id: executor.executor_id.clone(),
        version: executor.version.clone(),
        runtime_api: executor.runtime_api.clone(),
        node_kind: executor.node_kind.clone(),
        handler: executor.handler.clone(),
        capabilities: executor.capabilities.iter().cloned().collect(),
        input_schema: executor.input_schema.clone(),
        output_schema: executor.output_schema.clone(),
        timeout_ms: executor.timeout_ms,
        failure_policy: failure_policy.to_owned(),
        max_attempts,
        retry_backoff_ms,
        idempotent: executor.idempotency == sdk::NodeExecutorIdempotency::Idempotent,
        tool_permissions: executor
            .required_permissions
            .tools
            .iter()
            .cloned()
            .collect(),
        network_permissions: executor
            .required_permissions
            .network
            .iter()
            .cloned()
            .collect(),
        state_scope,
        external_effects: executor.external_effects,
        declaration_hash: ContentHash::digest(&canonical),
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
    pub version: String,
    pub declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub class: String,
    pub subscribed_events: BTreeSet<String>,
    pub timeout_ms: u64,
    pub failure_policy: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginLifecycleActionData {
    Disable,
    Enable,
    Quarantine,
    Unquarantine,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangePluginLifecycleDataRequest {
    pub session_id: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub configuration_reference: ContentHash,
    pub action: PluginLifecycleActionData,
    pub reason_code: Option<String>,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginLifecycleDataRecord {
    pub plugin_id: String,
    pub state: String,
    pub audit_operation: String,
    pub audit_outcome: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InvokePluginDataRequest {
    pub cancellation_target: PluginInvocationCancellationTargetDataRecord,
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
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginObservationDataRecord {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
    pub status: PluginObserverDeliveryStatusDataRecord,
    pub request_hash: String,
    pub receipt_id: String,
    pub receipt_digest: String,
    pub replayed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TeardownPluginHostDataRequest {
    pub session_id: String,
    pub active_continuations: usize,
    pub pending_observer_deliveries: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginObserverDeliveryStatusDataRecord {
    Completed,
    Rejected,
    Failed,
    Ambiguous,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InvokePluginNodeExecutorDataRequest {
    pub cancellation_target: PluginInvocationCancellationTargetDataRecord,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub executor_id: String,
    pub executor_version: String,
    pub timeout_ms: u64,
    pub configuration_reference: ContentHash,
    pub node_kind: String,
    pub input: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginNodeActionProposalDataRecord {
    pub kind: String,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginNodeOutcomeDataRecord {
    pub output: Value,
    pub preserved_state: Value,
    pub proposed_actions: Vec<PluginNodeActionProposalDataRecord>,
    pub attempts: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginContextTransformLifecycleData {
    BeforeModelRequest,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InvokePluginContextTransformDataRequest {
    pub cancellation_target: PluginInvocationCancellationTargetDataRecord,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub transform_id: String,
    pub transform_version: String,
    pub declaration_hash: ContentHash,
    pub timeout_ms: u64,
    pub configuration_reference: ContentHash,
    pub lifecycle: PluginContextTransformLifecycleData,
    pub handler: String,
    pub input: Value,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginContextTransformProposalDataRecord {
    pub replacement: Value,
    pub attempts: u8,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PluginMemoryScopeData {
    Session,
    Project,
    User,
    Runtime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginSecurityClassificationData {
    Public,
    Internal,
    Private,
    Confidential,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginCanonicalReferenceKindData {
    Artifact,
    NodeResult,
    ToolResult,
    ApprovalResult,
    Continuation,
    ChildSession,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginCanonicalReferenceDataRecord {
    pub kind: PluginCanonicalReferenceKindData,
    pub id: String,
    pub content_hash: Option<ContentHash>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginArtifactReferenceDataRecord {
    pub artifact_id: String,
    pub content_hash: ContentHash,
    pub media_type: String,
    pub size_bytes: u64,
    pub security_classification: PluginSecurityClassificationData,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginOperationBindingDataRecord {
    pub plugin_id: String,
    pub plugin_version: String,
    pub invocation_id: String,
    pub operation_id: String,
    pub session_id: String,
    pub run_id: String,
    pub node_id: Option<String>,
    pub declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub request_hash: ContentHash,
    pub idempotency_key: String,
    pub attempt: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginInvocationCancellationTargetDataRecord {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginInvocationCancellationDataStatus {
    Signalled,
    AlreadyTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CancelPluginNodeInvocationDataRequest {
    pub target: PluginInvocationCancellationTargetDataRecord,
    pub reason_code: String,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginInvocationCancellationDataResult {
    pub target: PluginInvocationCancellationTargetDataRecord,
    pub reason_code: String,
    pub action_digest: ContentHash,
    pub nonce: String,
    pub idempotency_key: String,
    pub cancellation_id: String,
    pub status: PluginInvocationCancellationDataStatus,
    pub receipt_id: String,
    pub receipt_digest: ContentHash,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryRetrieveInputDataRecord {
    pub query: String,
    pub scopes: BTreeSet<PluginMemoryScopeData>,
    pub max_items: u32,
    pub max_bytes: u64,
    pub artifacts: Vec<PluginArtifactReferenceDataRecord>,
    pub references: Vec<PluginCanonicalReferenceDataRecord>,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetrievePluginMemoryDataRequest {
    pub binding: PluginOperationBindingDataRecord,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub timeout_ms: u64,
    pub input: PluginMemoryRetrieveInputDataRecord,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryItemProposalDataRecord {
    pub item_id: String,
    pub scope: PluginMemoryScopeData,
    pub value: Value,
    pub value_hash: ContentHash,
    pub artifacts: Vec<PluginArtifactReferenceDataRecord>,
    pub references: Vec<PluginCanonicalReferenceDataRecord>,
    pub security_classification: PluginSecurityClassificationData,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryRetrieveProposalDataRecord {
    pub binding: PluginOperationBindingDataRecord,
    pub provider_id: String,
    pub provider_version: String,
    pub items: Vec<PluginMemoryItemProposalDataRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginMemoryWriteBoundaryData {
    Explicit,
    TurnCompletion,
    IterationCompletion,
    SessionCompletion,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryWriteInputDataRecord {
    pub scope: PluginMemoryScopeData,
    pub boundary: PluginMemoryWriteBoundaryData,
    pub value: Value,
    pub value_hash: ContentHash,
    pub artifacts: Vec<PluginArtifactReferenceDataRecord>,
    pub references: Vec<PluginCanonicalReferenceDataRecord>,
    pub security_classification: PluginSecurityClassificationData,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WritePluginMemoryDataRequest {
    pub binding: PluginOperationBindingDataRecord,
    pub provider_id: String,
    pub provider_version: String,
    pub handler: String,
    pub timeout_ms: u64,
    pub input: PluginMemoryWriteInputDataRecord,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginMemoryWriteReceiptDataRecord {
    pub binding: PluginOperationBindingDataRecord,
    pub provider_id: String,
    pub provider_version: String,
    pub provider_record_id: String,
    pub value_hash: ContentHash,
    pub receipt: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionInputDataRecord {
    pub projection: Value,
    pub projection_hash: ContentHash,
    pub required_references: Vec<PluginCanonicalReferenceDataRecord>,
    pub required_artifacts: Vec<PluginArtifactReferenceDataRecord>,
    pub preservation_requirements: BTreeSet<String>,
    pub max_replacement_bytes: u64,
    pub max_projection_tokens: u64,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompactPluginContextDataRequest {
    pub binding: PluginOperationBindingDataRecord,
    pub compactor_id: String,
    pub compactor_version: String,
    pub handler: String,
    pub max_attempts: u8,
    pub retry_backoff_ms: u64,
    pub timeout_ms: u64,
    pub input: PluginCompactionInputDataRecord,
    pub readable_state: Value,
    pub cancellation_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PluginCompactionProposalDataRecord {
    pub binding: PluginOperationBindingDataRecord,
    pub compactor_id: String,
    pub compactor_version: String,
    pub replacement: Value,
    pub replacement_hash: ContentHash,
    pub preserved_references: Vec<PluginCanonicalReferenceDataRecord>,
    pub preserved_artifacts: Vec<PluginArtifactReferenceDataRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginNodeStateScopeData {
    Invocation,
    ModelCall,
    Turn,
    Session,
    Project,
    User,
    Runtime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PersistPluginNodeStateDataRequest {
    pub cancellation_target: PluginInvocationCancellationTargetDataRecord,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub state_scope: PluginNodeStateScopeData,
    pub prior_generation: u64,
    pub prior_state_hash: Option<ContentHash>,
    pub state: Value,
    pub state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub nonce: String,
    pub cancellation_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeStateReceiptDataRecord {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub state_scope: PluginNodeStateScopeData,
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

#[derive(Clone, Debug, PartialEq)]
pub struct LoadPluginNodeStateDataRequest {
    pub cancellation_target: PluginInvocationCancellationTargetDataRecord,
    pub session_id: String,
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub configuration_reference: ContentHash,
    pub state_scope: PluginNodeStateScopeData,
    pub expected_generation: u64,
    pub expected_state_hash: ContentHash,
    pub action_digest: ContentHash,
    pub authorization_digest: ContentHash,
    pub nonce: String,
    pub cancellation_id: String,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeStateReadReceiptDataRecord {
    pub plugin_id: String,
    pub invocation_id: String,
    pub invocation_digest: ContentHash,
    pub executor_id: String,
    pub executor_version: String,
    pub executor_declaration_hash: ContentHash,
    pub state_scope: PluginNodeStateScopeData,
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
pub struct LoadedPluginNodeStateDataRecord {
    pub state: Value,
    pub receipt: PluginNodeStateReadReceiptDataRecord,
}

#[async_trait]
pub trait PluginDataPort: Send + Sync {
    /// Returns the exact catalog version for one plugin.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDataError`] when the plugin is absent.
    fn plugin_version(&self, _plugin_id: &str) -> Result<String, PluginDataError> {
        Err(PluginDataError::Unavailable)
    }

    /// Returns the immutable configuration reference used to activate one plugin.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDataError`] when the plugin is absent.
    fn plugin_configuration_reference(
        &self,
        _plugin_id: &str,
    ) -> Result<ContentHash, PluginDataError> {
        Err(PluginDataError::Unavailable)
    }

    /// Loads one exact validated context-transform declaration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDataError`] when the exact plugin/transform identity is
    /// absent from the authoritative plugin catalog.
    fn context_transform_declaration(
        &self,
        _plugin_id: &str,
        _transform_id: &str,
        _transform_version: &str,
    ) -> Result<PluginContextTransformDataRecord, PluginDataError> {
        Err(PluginDataError::Invalid)
    }

    /// Loads one exact validated node-executor declaration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDataError`] when the plugin/executor identity is not in
    /// the validated catalog.
    fn node_executor_declaration(
        &self,
        _plugin_id: &str,
        _executor_id: &str,
        _executor_version: &str,
        _node_kind: &str,
    ) -> Result<PluginNodeExecutorDataRecord, PluginDataError> {
        Err(PluginDataError::Invalid)
    }

    /// Loads one exact immutable memory-provider declaration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDataError`] when the identity is absent.
    fn memory_provider_declaration(
        &self,
        _plugin_id: &str,
        _provider_id: &str,
        _provider_version: &str,
    ) -> Result<PluginMemoryProviderDataRecord, PluginDataError> {
        Err(PluginDataError::Invalid)
    }

    /// Loads one exact immutable compactor declaration.
    ///
    /// # Errors
    ///
    /// Returns [`PluginDataError`] when the identity is absent.
    fn compactor_declaration(
        &self,
        _plugin_id: &str,
        _compactor_id: &str,
        _compactor_version: &str,
    ) -> Result<PluginCompactorDataRecord, PluginDataError> {
        Err(PluginDataError::Invalid)
    }

    async fn activate_plugins(
        &self,
        request: ActivatePluginsDataRequest,
    ) -> Result<ActivatedPluginsDataRecord, PluginDataError>;

    async fn change_plugin_lifecycle(
        &self,
        _request: ChangePluginLifecycleDataRequest,
    ) -> Result<PluginLifecycleDataRecord, PluginDataError> {
        Err(PluginDataError::Unavailable)
    }

    async fn invoke_plugin(
        &self,
        request: InvokePluginDataRequest,
    ) -> Result<PluginDecisionDataRecord, PluginDataError>;

    async fn observe_event(
        &self,
        request: ObservePluginDataRequest,
    ) -> Result<PluginObservationDataRecord, PluginDataError>;

    async fn teardown_host_if_idle(
        &self,
        _request: TeardownPluginHostDataRequest,
    ) -> Result<bool, PluginDataError> {
        Ok(false)
    }

    async fn invoke_node_executor(
        &self,
        _request: InvokePluginNodeExecutorDataRequest,
    ) -> Result<PluginNodeOutcomeDataRecord, PluginDataError> {
        Err(PluginDataError::Invalid)
    }

    async fn cancel_node_invocation(
        &self,
        _request: CancelPluginNodeInvocationDataRequest,
    ) -> Result<PluginInvocationCancellationDataResult, PluginDataError> {
        Err(PluginDataError::Unavailable)
    }

    async fn invoke_context_transform(
        &self,
        _request: InvokePluginContextTransformDataRequest,
    ) -> Result<PluginContextTransformProposalDataRecord, PluginDataError> {
        Err(PluginDataError::Invalid)
    }

    async fn retrieve_memory(
        &self,
        _request: RetrievePluginMemoryDataRequest,
    ) -> Result<PluginMemoryRetrieveProposalDataRecord, PluginDataError> {
        Err(PluginDataError::MemoryOperationUnsupported)
    }

    async fn write_memory(
        &self,
        _request: WritePluginMemoryDataRequest,
    ) -> Result<PluginMemoryWriteReceiptDataRecord, PluginDataError> {
        Err(PluginDataError::MemoryOperationUnsupported)
    }

    async fn compact_context(
        &self,
        _request: CompactPluginContextDataRequest,
    ) -> Result<PluginCompactionProposalDataRecord, PluginDataError> {
        Err(PluginDataError::MemoryOperationUnsupported)
    }

    async fn persist_plugin_node_state(
        &self,
        _request: PersistPluginNodeStateDataRequest,
    ) -> Result<PluginNodeStateReceiptDataRecord, PluginDataError> {
        Err(PluginDataError::StatePersistenceUnsupported)
    }

    async fn load_plugin_node_state(
        &self,
        _request: LoadPluginNodeStateDataRequest,
    ) -> Result<LoadedPluginNodeStateDataRecord, PluginDataError> {
        Err(PluginDataError::StateReadUnsupported)
    }
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

    fn validate_active_operation_binding(
        &self,
        binding: &PluginOperationBindingDataRecord,
    ) -> Result<(), PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&binding.session_id)
            .is_some_and(|plugins| plugins.contains(&binding.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let manifest = self
            .manifests
            .get(&binding.plugin_id)
            .ok_or(PluginDataError::Invalid)?;
        if manifest.version != binding.plugin_version
            || binding.attempt == 0
            || binding.configuration_reference == ContentHash::from_bytes([0; 32])
        {
            return Err(PluginDataError::Invalid);
        }
        Ok(())
    }
}

#[async_trait]
impl PluginDataPort for RuntimePluginData {
    fn plugin_version(&self, plugin_id: &str) -> Result<String, PluginDataError> {
        self.manifests
            .get(plugin_id)
            .map(|manifest| manifest.version.clone())
            .ok_or(PluginDataError::Unavailable)
    }

    fn plugin_configuration_reference(
        &self,
        plugin_id: &str,
    ) -> Result<ContentHash, PluginDataError> {
        self.manifests
            .get(plugin_id)
            .map(|manifest| manifest.configuration_reference)
            .ok_or(PluginDataError::Unavailable)
    }

    fn context_transform_declaration(
        &self,
        plugin_id: &str,
        transform_id: &str,
        transform_version: &str,
    ) -> Result<PluginContextTransformDataRecord, PluginDataError> {
        self.manifests
            .get(plugin_id)
            .and_then(|manifest| {
                manifest.context_transforms.iter().find(|transform| {
                    transform.transform_id == transform_id && transform.version == transform_version
                })
            })
            .cloned()
            .ok_or(PluginDataError::Invalid)
    }

    fn memory_provider_declaration(
        &self,
        plugin_id: &str,
        provider_id: &str,
        provider_version: &str,
    ) -> Result<PluginMemoryProviderDataRecord, PluginDataError> {
        self.manifests
            .get(plugin_id)
            .and_then(|manifest| {
                manifest.memory_providers.iter().find(|provider| {
                    provider.provider_id == provider_id && provider.version == provider_version
                })
            })
            .cloned()
            .ok_or(PluginDataError::Invalid)
    }

    fn compactor_declaration(
        &self,
        plugin_id: &str,
        compactor_id: &str,
        compactor_version: &str,
    ) -> Result<PluginCompactorDataRecord, PluginDataError> {
        self.manifests
            .get(plugin_id)
            .and_then(|manifest| {
                manifest.compactors.iter().find(|compactor| {
                    compactor.compactor_id == compactor_id && compactor.version == compactor_version
                })
            })
            .cloned()
            .ok_or(PluginDataError::Invalid)
    }

    fn node_executor_declaration(
        &self,
        plugin_id: &str,
        executor_id: &str,
        executor_version: &str,
        node_kind: &str,
    ) -> Result<PluginNodeExecutorDataRecord, PluginDataError> {
        self.manifests
            .get(plugin_id)
            .and_then(|manifest| {
                manifest.node_executors.iter().find(|executor| {
                    executor.executor_id == executor_id
                        && executor.version == executor_version
                        && executor.node_kind == node_kind
                })
            })
            .cloned()
            .ok_or(PluginDataError::Invalid)
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
                        version: manifest.version.clone(),
                        declaration_hash: ContentHash::digest(
                            manifest.canonical_manifest_json.as_bytes(),
                        ),
                        configuration_reference: manifest.configuration_reference,
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

    async fn change_plugin_lifecycle(
        &self,
        request: ChangePluginLifecycleDataRequest,
    ) -> Result<PluginLifecycleDataRecord, PluginDataError> {
        let manifest = self
            .manifests
            .get(&request.plugin_id)
            .ok_or(PluginDataError::Unavailable)?;
        if manifest.version != request.plugin_version
            || manifest.configuration_reference != request.configuration_reference
        {
            return Err(PluginDataError::Unavailable);
        }
        let activating = matches!(
            request.action,
            PluginLifecycleActionData::Enable | PluginLifecycleActionData::Unquarantine
        );
        let changed = self
            .dependency
            .change_plugin_lifecycle(dependency::DependencyPluginLifecycleRequest {
                session_id: request.session_id.clone(),
                plugin_id: request.plugin_id.clone(),
                plugin_version: request.plugin_version,
                configuration_reference: request.configuration_reference,
                action: match request.action {
                    PluginLifecycleActionData::Disable => {
                        dependency::DependencyPluginLifecycleAction::Disable
                    }
                    PluginLifecycleActionData::Enable => {
                        dependency::DependencyPluginLifecycleAction::Enable
                    }
                    PluginLifecycleActionData::Quarantine => {
                        dependency::DependencyPluginLifecycleAction::Quarantine
                    }
                    PluginLifecycleActionData::Unquarantine => {
                        dependency::DependencyPluginLifecycleAction::Unquarantine
                    }
                },
                reason_code: request.reason_code,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("change_plugin_lifecycle", error))?;
        let mut activated = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?;
        let session_plugins = activated.entry(request.session_id).or_default();
        if activating {
            session_plugins.insert(request.plugin_id);
        } else {
            session_plugins.remove(&request.plugin_id);
        }
        Ok(PluginLifecycleDataRecord {
            plugin_id: changed.plugin_id,
            state: changed.state,
            audit_operation: changed.audit_operation,
            audit_outcome: changed.audit_outcome,
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
                cancellation_target: map_cancellation_target(&request.cancellation_target),
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
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| map_operation_error("observe", error))?;
        Ok(PluginObservationDataRecord {
            accepted: result.accepted,
            queue_depth: result.queue_depth,
            dropped: result.dropped,
            status: match result.status {
                dependency::DependencyPluginObserverDeliveryStatus::Completed => {
                    PluginObserverDeliveryStatusDataRecord::Completed
                }
                dependency::DependencyPluginObserverDeliveryStatus::Rejected => {
                    PluginObserverDeliveryStatusDataRecord::Rejected
                }
                dependency::DependencyPluginObserverDeliveryStatus::Failed => {
                    PluginObserverDeliveryStatusDataRecord::Failed
                }
                dependency::DependencyPluginObserverDeliveryStatus::Ambiguous => {
                    PluginObserverDeliveryStatusDataRecord::Ambiguous
                }
            },
            request_hash: result.request_hash,
            receipt_id: result.receipt_id,
            receipt_digest: result.receipt_digest,
            replayed: result.replayed,
        })
    }

    async fn teardown_host_if_idle(
        &self,
        request: TeardownPluginHostDataRequest,
    ) -> Result<bool, PluginDataError> {
        self.dependency
            .teardown_session_if_idle(
                &request.session_id,
                request.active_continuations,
                request.pending_observer_deliveries,
            )
            .await
            .map_err(|error| map_operation_error("teardown_if_idle", error))
    }

    async fn invoke_node_executor(
        &self,
        request: InvokePluginNodeExecutorDataRequest,
    ) -> Result<PluginNodeOutcomeDataRecord, PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let manifest = self
            .manifests
            .get(&request.plugin_id)
            .ok_or(PluginDataError::Invalid)?;
        let declaration = manifest
            .node_executors
            .iter()
            .find(|executor| {
                executor.executor_id == request.executor_id
                    && executor.version == request.executor_version
                    && executor.node_kind == request.node_kind
            })
            .ok_or(PluginDataError::Invalid)?;
        let result = self
            .dependency
            .invoke_node_executor(dependency::DependencyPluginNodeInvocationRequest {
                cancellation_target: map_cancellation_target(&request.cancellation_target),
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                timeout_ms: request.timeout_ms,
                configuration_reference: request.configuration_reference,
                node_kind: request.node_kind,
                handler: declaration.handler.clone(),
                input: request.input,
                readable_state: request.readable_state,
                cancellation_id: request.cancellation_id,
            })
            .await
            .map_err(|error| {
                let ambiguous_transport = matches!(
                    &error,
                    dependency::PluginDependencyError::Timeout
                        | dependency::PluginDependencyError::Unavailable
                        | dependency::PluginDependencyError::InvalidResponse
                ) || matches!(
                    &error,
                    dependency::PluginDependencyError::Rejected { code, .. }
                        if code == "ambiguous_execution"
                );
                if declaration.external_effects && !declaration.idempotent && ambiguous_transport {
                    PluginDataError::Ambiguous {
                        plugin_id: manifest.id.clone(),
                        executor_id: declaration.executor_id.clone(),
                    }
                } else {
                    map_operation_error("invoke_node_executor", error)
                }
            })?;
        Ok(PluginNodeOutcomeDataRecord {
            output: result.output,
            preserved_state: result.preserved_state,
            proposed_actions: result
                .proposed_actions
                .into_iter()
                .map(|action| PluginNodeActionProposalDataRecord {
                    kind: action.kind,
                    payload: action.payload,
                })
                .collect(),
            attempts: result.attempts,
        })
    }

    async fn cancel_node_invocation(
        &self,
        request: CancelPluginNodeInvocationDataRequest,
    ) -> Result<PluginInvocationCancellationDataResult, PluginDataError> {
        validate_cancellation_target(self, &request)?;
        let result = self
            .dependency
            .cancel_plugin_invocation(dependency::DependencyCancelPluginInvocationRequest {
                target: map_cancellation_target(&request.target),
                reason_code: request.reason_code.clone(),
                nonce: request.nonce.clone(),
                idempotency_key: request.idempotency_key.clone(),
                cancellation_id: request.cancellation_id.clone(),
            })
            .await
            .map_err(|error| map_cancellation_error(&request, &error))?;
        let mapped = PluginInvocationCancellationDataResult {
            target: unmap_cancellation_target(result.target),
            reason_code: result.reason_code,
            action_digest: result.action_digest,
            nonce: result.nonce,
            idempotency_key: result.idempotency_key,
            cancellation_id: result.cancellation_id,
            status: match result.status {
                dependency::DependencyPluginInvocationCancellationStatus::Signalled => {
                    PluginInvocationCancellationDataStatus::Signalled
                }
                dependency::DependencyPluginInvocationCancellationStatus::AlreadyTerminal => {
                    PluginInvocationCancellationDataStatus::AlreadyTerminal
                }
            },
            receipt_id: result.receipt_id,
            receipt_digest: result.receipt_digest,
        };
        if mapped.target != request.target
            || mapped.reason_code != request.reason_code
            || mapped.nonce != request.nonce
            || mapped.idempotency_key != request.idempotency_key
            || mapped.cancellation_id != request.cancellation_id
            || mapped.receipt_id.is_empty()
            || mapped.action_digest == ContentHash::from_bytes([0; 32])
            || mapped.receipt_digest == ContentHash::from_bytes([0; 32])
        {
            return Err(PluginDataError::Ambiguous {
                plugin_id: request.target.plugin_id,
                executor_id: request.target.operation_id,
            });
        }
        Ok(mapped)
    }

    async fn invoke_context_transform(
        &self,
        request: InvokePluginContextTransformDataRequest,
    ) -> Result<PluginContextTransformProposalDataRecord, PluginDataError> {
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let declaration = self.context_transform_declaration(
            &request.plugin_id,
            &request.transform_id,
            &request.transform_version,
        )?;
        let lifecycle = match request.lifecycle {
            PluginContextTransformLifecycleData::BeforeModelRequest
                if declaration.lifecycle == "before_model_request" =>
            {
                dependency::DependencyPluginContextTransformLifecycle::BeforeModelRequest
            }
            PluginContextTransformLifecycleData::BeforeModelRequest => {
                return Err(PluginDataError::Invalid);
            }
        };
        if declaration.declaration_hash != request.declaration_hash
            || declaration.handler != request.handler
            || !declaration.idempotent
            || declaration.external_effects
        {
            return Err(PluginDataError::Invalid);
        }
        let plugin_id = request.plugin_id.clone();
        let invocation_id = request.invocation_id.clone();
        let transform_id = request.transform_id.clone();
        let result = self
            .dependency
            .invoke_context_transform(
                dependency::DependencyPluginContextTransformInvocationRequest {
                    cancellation_target: map_cancellation_target(&request.cancellation_target),
                    session_id: request.session_id,
                    plugin_id: request.plugin_id,
                    invocation_id: request.invocation_id,
                    transform_id: request.transform_id,
                    transform_version: request.transform_version,
                    timeout_ms: request.timeout_ms,
                    configuration_reference: request.configuration_reference,
                    lifecycle,
                    handler: request.handler,
                    input: request.input,
                    readable_state: request.readable_state,
                    cancellation_id: request.cancellation_id,
                },
            )
            .await
            .map_err(|error| {
                if matches!(
                    error,
                    dependency::PluginDependencyError::Timeout
                        | dependency::PluginDependencyError::Unavailable
                        | dependency::PluginDependencyError::InvalidResponse
                        | dependency::PluginDependencyError::AmbiguousContextTransform
                ) {
                    PluginDataError::AmbiguousContextTransform {
                        plugin_id,
                        transform_id,
                        invocation_id,
                    }
                } else {
                    map_operation_error("invoke_context_transform", error)
                }
            })?;
        Ok(PluginContextTransformProposalDataRecord {
            replacement: result.replacement,
            attempts: result.attempts,
        })
    }

    async fn retrieve_memory(
        &self,
        request: RetrievePluginMemoryDataRequest,
    ) -> Result<PluginMemoryRetrieveProposalDataRecord, PluginDataError> {
        self.validate_active_operation_binding(&request.binding)?;
        let declaration = self.memory_provider_declaration(
            &request.binding.plugin_id,
            &request.provider_id,
            &request.provider_version,
        )?;
        if declaration.declaration_hash != request.binding.declaration_hash
            || declaration.retrieve.handler != request.handler
            || !declaration.retrieve.idempotent
            || declaration.retrieve.external_effects
            || request.max_attempts == 0
            || request.max_attempts > declaration.retrieve.max_attempts
            || request.retry_backoff_ms != declaration.retrieve.retry_backoff_ms
            || request.timeout_ms != declaration.retrieve.timeout_ms
        {
            return Err(PluginDataError::Invalid);
        }
        let expected = request.clone();
        let proposal = self
            .dependency
            .retrieve_memory(map_retrieve_request(request))
            .await
            .map_err(|error| map_operation_error("retrieve_memory", error))?;
        if proposal.binding != map_binding(&expected.binding)
            || proposal.provider_id != expected.provider_id
            || proposal.provider_version != expected.provider_version
        {
            return Err(PluginDataError::Invalid);
        }
        Ok(unmap_retrieve_proposal(proposal))
    }

    async fn write_memory(
        &self,
        request: WritePluginMemoryDataRequest,
    ) -> Result<PluginMemoryWriteReceiptDataRecord, PluginDataError> {
        self.validate_active_operation_binding(&request.binding)?;
        let declaration = self.memory_provider_declaration(
            &request.binding.plugin_id,
            &request.provider_id,
            &request.provider_version,
        )?;
        let write = declaration.write.ok_or(PluginDataError::Invalid)?;
        if declaration.declaration_hash != request.binding.declaration_hash
            || write.handler != request.handler
            || write.idempotent
            || !write.external_effects
            || request.binding.attempt != 1
            || request.timeout_ms != write.timeout_ms
        {
            return Err(PluginDataError::Invalid);
        }
        let ambiguity = PluginDataError::AmbiguousMemoryWrite {
            plugin_id: request.binding.plugin_id.clone(),
            provider_id: request.provider_id.clone(),
            invocation_id: request.binding.invocation_id.clone(),
            idempotency_key: request.binding.idempotency_key.clone(),
        };
        let expected = request.clone();
        let receipt = self
            .dependency
            .write_memory(map_write_request(request))
            .await
            .map_err(|error| match error {
                dependency::PluginDependencyError::AmbiguousMemoryWrite
                | dependency::PluginDependencyError::Timeout
                | dependency::PluginDependencyError::Unavailable
                | dependency::PluginDependencyError::InvalidResponse => ambiguity,
                other => map_operation_error("write_memory", other),
            })?;
        if receipt.binding != map_binding(&expected.binding)
            || receipt.provider_id != expected.provider_id
            || receipt.provider_version != expected.provider_version
            || receipt.value_hash != expected.input.value_hash
        {
            return Err(PluginDataError::AmbiguousMemoryWrite {
                plugin_id: expected.binding.plugin_id,
                provider_id: expected.provider_id,
                invocation_id: expected.binding.invocation_id,
                idempotency_key: expected.binding.idempotency_key,
            });
        }
        Ok(unmap_write_receipt(receipt))
    }

    async fn compact_context(
        &self,
        request: CompactPluginContextDataRequest,
    ) -> Result<PluginCompactionProposalDataRecord, PluginDataError> {
        self.validate_active_operation_binding(&request.binding)?;
        let declaration = self.compactor_declaration(
            &request.binding.plugin_id,
            &request.compactor_id,
            &request.compactor_version,
        )?;
        if declaration.declaration_hash != request.binding.declaration_hash
            || declaration.handler != request.handler
            || !declaration.idempotent
            || declaration.external_effects
            || request.max_attempts == 0
            || request.max_attempts > declaration.max_attempts
            || request.retry_backoff_ms != declaration.retry_backoff_ms
            || request.timeout_ms != declaration.timeout_ms
        {
            return Err(PluginDataError::Invalid);
        }
        let expected = request.clone();
        let proposal = self
            .dependency
            .compact_context(map_compaction_request(request))
            .await
            .map_err(|error| map_operation_error("compact_context", error))?;
        if proposal.binding != map_binding(&expected.binding)
            || proposal.compactor_id != expected.compactor_id
            || proposal.compactor_version != expected.compactor_version
        {
            return Err(PluginDataError::Invalid);
        }
        Ok(unmap_compaction_proposal(proposal))
    }

    async fn persist_plugin_node_state(
        &self,
        request: PersistPluginNodeStateDataRequest,
    ) -> Result<PluginNodeStateReceiptDataRecord, PluginDataError> {
        if !matches!(
            request.state_scope,
            PluginNodeStateScopeData::Invocation | PluginNodeStateScopeData::Session
        ) {
            return Err(PluginDataError::UnsupportedStateScope);
        }
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let declaration = self
            .manifests
            .get(&request.plugin_id)
            .and_then(|manifest| {
                manifest.node_executors.iter().find(|executor| {
                    executor.executor_id == request.executor_id
                        && executor.version == request.executor_version
                        && executor.declaration_hash == request.executor_declaration_hash
                })
            })
            .ok_or(PluginDataError::Invalid)?;
        if declaration.state_scope != state_scope_name(request.state_scope) {
            return Err(PluginDataError::Invalid);
        }
        let expected = request.clone();
        let receipt = self
            .dependency
            .persist_plugin_node_state(dependency::DependencyPersistPluginNodeStateRequest {
                cancellation_target: map_cancellation_target(&request.cancellation_target),
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                executor_declaration_hash: request.executor_declaration_hash,
                configuration_reference: request.configuration_reference,
                state_scope: map_state_scope(request.state_scope),
                prior_generation: request.prior_generation,
                prior_state_hash: request.prior_state_hash,
                state: request.state,
                state_hash: request.state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                nonce: request.nonce,
                cancellation_id: request.cancellation_id,
                idempotency_key: request.idempotency_key,
            })
            .await
            .map_err(|error| map_state_persistence_error(&expected, error))?;
        validate_state_receipt(&expected, &receipt)?;
        Ok(PluginNodeStateReceiptDataRecord {
            plugin_id: receipt.plugin_id,
            invocation_id: receipt.invocation_id,
            invocation_digest: receipt.invocation_digest,
            executor_id: receipt.executor_id,
            executor_version: receipt.executor_version,
            executor_declaration_hash: receipt.executor_declaration_hash,
            state_scope: unmap_state_scope(receipt.state_scope),
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
        if !matches!(
            request.state_scope,
            PluginNodeStateScopeData::Invocation | PluginNodeStateScopeData::Session
        ) || request.expected_generation == 0
        {
            return Err(PluginDataError::UnsupportedStateScope);
        }
        let active = self
            .activated
            .lock()
            .map_err(|_| PluginDataError::Unavailable)?
            .get(&request.session_id)
            .is_some_and(|plugins| plugins.contains(&request.plugin_id));
        if !active {
            return Err(PluginDataError::Inactive);
        }
        let declaration = self
            .manifests
            .get(&request.plugin_id)
            .and_then(|manifest| {
                manifest.node_executors.iter().find(|executor| {
                    executor.executor_id == request.executor_id
                        && executor.version == request.executor_version
                        && executor.declaration_hash == request.executor_declaration_hash
                })
            })
            .ok_or(PluginDataError::Invalid)?;
        if declaration.state_scope != state_scope_name(request.state_scope) {
            return Err(PluginDataError::Invalid);
        }
        let expected = request.clone();
        let loaded = self
            .dependency
            .load_plugin_node_state(dependency::DependencyLoadPluginNodeStateRequest {
                cancellation_target: map_cancellation_target(&request.cancellation_target),
                session_id: request.session_id,
                plugin_id: request.plugin_id,
                invocation_id: request.invocation_id,
                invocation_digest: request.invocation_digest,
                executor_id: request.executor_id,
                executor_version: request.executor_version,
                executor_declaration_hash: request.executor_declaration_hash,
                configuration_reference: request.configuration_reference,
                state_scope: map_state_scope(request.state_scope),
                expected_generation: request.expected_generation,
                expected_state_hash: request.expected_state_hash,
                action_digest: request.action_digest,
                authorization_digest: request.authorization_digest,
                nonce: request.nonce,
                cancellation_id: request.cancellation_id,
                idempotency_key: request.idempotency_key,
            })
            .await
            .map_err(|error| map_state_read_error(&expected, error))?;
        validate_state_read_receipt(&expected, &loaded)?;
        Ok(LoadedPluginNodeStateDataRecord {
            state: loaded.state,
            receipt: PluginNodeStateReadReceiptDataRecord {
                plugin_id: loaded.receipt.plugin_id,
                invocation_id: loaded.receipt.invocation_id,
                invocation_digest: loaded.receipt.invocation_digest,
                executor_id: loaded.receipt.executor_id,
                executor_version: loaded.receipt.executor_version,
                executor_declaration_hash: loaded.receipt.executor_declaration_hash,
                state_scope: unmap_state_scope(loaded.receipt.state_scope),
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
}

fn map_binding(
    binding: &PluginOperationBindingDataRecord,
) -> dependency::DependencyPluginOperationBinding {
    dependency::DependencyPluginOperationBinding {
        plugin_id: binding.plugin_id.clone(),
        plugin_version: binding.plugin_version.clone(),
        invocation_id: binding.invocation_id.clone(),
        operation_id: binding.operation_id.clone(),
        session_id: binding.session_id.clone(),
        run_id: binding.run_id.clone(),
        node_id: binding.node_id.clone(),
        declaration_hash: binding.declaration_hash,
        configuration_reference: binding.configuration_reference,
        request_hash: binding.request_hash,
        idempotency_key: binding.idempotency_key.clone(),
        attempt: binding.attempt,
    }
}

fn map_cancellation_target(
    target: &PluginInvocationCancellationTargetDataRecord,
) -> dependency::DependencyPluginInvocationCancellationTarget {
    dependency::DependencyPluginInvocationCancellationTarget {
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

fn unmap_cancellation_target(
    target: dependency::DependencyPluginInvocationCancellationTarget,
) -> PluginInvocationCancellationTargetDataRecord {
    PluginInvocationCancellationTargetDataRecord {
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

fn validate_cancellation_target(
    data: &RuntimePluginData,
    request: &CancelPluginNodeInvocationDataRequest,
) -> Result<(), PluginDataError> {
    let target = &request.target;
    let manifest = data
        .manifests
        .get(&target.plugin_id)
        .ok_or(PluginDataError::Invalid)?;
    let declaration = manifest
        .node_executors
        .iter()
        .find(|declaration| {
            declaration.executor_id == target.operation_id
                && declaration.declaration_hash == target.declaration_hash
        })
        .ok_or(PluginDataError::Invalid)?;
    let expected_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            "agentmod.plugin.invocation.identity.v1",
            &target.session_id,
            &target.run_id,
            &target.plugin_id,
            &target.plugin_version,
            &target.invocation_id,
            &target.operation_id,
            target.declaration_hash,
            target.request_hash,
        ))
        .map_err(|_| PluginDataError::Invalid)?,
    );
    if target.session_id.is_empty()
        || target.run_id.is_empty()
        || target.invocation_id.is_empty()
        || manifest.version != target.plugin_version
        || declaration.plugin_version != target.plugin_version
        || target.invocation_digest != expected_digest
    {
        return Err(PluginDataError::Invalid);
    }
    Ok(())
}

#[cfg(test)]
fn test_cancellation_target_data(
    plugin_id: &str,
    plugin_version: &str,
    invocation_id: &str,
    operation_id: &str,
    declaration_hash: ContentHash,
    request_hash: ContentHash,
) -> PluginInvocationCancellationTargetDataRecord {
    let invocation_digest = ContentHash::digest(
        &serde_json::to_vec(&(
            "agentmod.plugin.invocation.identity.v1",
            "session-1",
            "run-1",
            plugin_id,
            plugin_version,
            invocation_id,
            operation_id,
            declaration_hash,
            request_hash,
        ))
        .expect("cancellation target"),
    );
    PluginInvocationCancellationTargetDataRecord {
        session_id: String::from("session-1"),
        run_id: String::from("run-1"),
        plugin_id: plugin_id.to_owned(),
        plugin_version: plugin_version.to_owned(),
        invocation_id: invocation_id.to_owned(),
        invocation_digest,
        operation_id: operation_id.to_owned(),
        declaration_hash,
        request_hash,
    }
}

fn unmap_binding(
    binding: dependency::DependencyPluginOperationBinding,
) -> PluginOperationBindingDataRecord {
    PluginOperationBindingDataRecord {
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

const fn map_scope(scope: PluginMemoryScopeData) -> dependency::DependencyPluginMemoryScope {
    match scope {
        PluginMemoryScopeData::Session => dependency::DependencyPluginMemoryScope::Session,
        PluginMemoryScopeData::Project => dependency::DependencyPluginMemoryScope::Project,
        PluginMemoryScopeData::User => dependency::DependencyPluginMemoryScope::User,
        PluginMemoryScopeData::Runtime => dependency::DependencyPluginMemoryScope::Runtime,
    }
}

const fn unmap_scope(scope: dependency::DependencyPluginMemoryScope) -> PluginMemoryScopeData {
    match scope {
        dependency::DependencyPluginMemoryScope::Session => PluginMemoryScopeData::Session,
        dependency::DependencyPluginMemoryScope::Project => PluginMemoryScopeData::Project,
        dependency::DependencyPluginMemoryScope::User => PluginMemoryScopeData::User,
        dependency::DependencyPluginMemoryScope::Runtime => PluginMemoryScopeData::Runtime,
    }
}

const fn map_security(
    value: PluginSecurityClassificationData,
) -> dependency::DependencyPluginSecurityClassification {
    match value {
        PluginSecurityClassificationData::Public => {
            dependency::DependencyPluginSecurityClassification::Public
        }
        PluginSecurityClassificationData::Internal => {
            dependency::DependencyPluginSecurityClassification::Internal
        }
        PluginSecurityClassificationData::Private => {
            dependency::DependencyPluginSecurityClassification::Private
        }
        PluginSecurityClassificationData::Confidential => {
            dependency::DependencyPluginSecurityClassification::Confidential
        }
    }
}

const fn unmap_security(
    value: dependency::DependencyPluginSecurityClassification,
) -> PluginSecurityClassificationData {
    match value {
        dependency::DependencyPluginSecurityClassification::Public => {
            PluginSecurityClassificationData::Public
        }
        dependency::DependencyPluginSecurityClassification::Internal => {
            PluginSecurityClassificationData::Internal
        }
        dependency::DependencyPluginSecurityClassification::Private => {
            PluginSecurityClassificationData::Private
        }
        dependency::DependencyPluginSecurityClassification::Confidential => {
            PluginSecurityClassificationData::Confidential
        }
    }
}

fn map_reference(
    value: PluginCanonicalReferenceDataRecord,
) -> dependency::DependencyPluginCanonicalReference {
    dependency::DependencyPluginCanonicalReference {
        kind: match value.kind {
            PluginCanonicalReferenceKindData::Artifact => {
                dependency::DependencyPluginCanonicalReferenceKind::Artifact
            }
            PluginCanonicalReferenceKindData::NodeResult => {
                dependency::DependencyPluginCanonicalReferenceKind::NodeResult
            }
            PluginCanonicalReferenceKindData::ToolResult => {
                dependency::DependencyPluginCanonicalReferenceKind::ToolResult
            }
            PluginCanonicalReferenceKindData::ApprovalResult => {
                dependency::DependencyPluginCanonicalReferenceKind::ApprovalResult
            }
            PluginCanonicalReferenceKindData::Continuation => {
                dependency::DependencyPluginCanonicalReferenceKind::Continuation
            }
            PluginCanonicalReferenceKindData::ChildSession => {
                dependency::DependencyPluginCanonicalReferenceKind::ChildSession
            }
        },
        id: value.id,
        content_hash: value.content_hash,
    }
}

fn unmap_reference(
    value: dependency::DependencyPluginCanonicalReference,
) -> PluginCanonicalReferenceDataRecord {
    PluginCanonicalReferenceDataRecord {
        kind: match value.kind {
            dependency::DependencyPluginCanonicalReferenceKind::Artifact => {
                PluginCanonicalReferenceKindData::Artifact
            }
            dependency::DependencyPluginCanonicalReferenceKind::NodeResult => {
                PluginCanonicalReferenceKindData::NodeResult
            }
            dependency::DependencyPluginCanonicalReferenceKind::ToolResult => {
                PluginCanonicalReferenceKindData::ToolResult
            }
            dependency::DependencyPluginCanonicalReferenceKind::ApprovalResult => {
                PluginCanonicalReferenceKindData::ApprovalResult
            }
            dependency::DependencyPluginCanonicalReferenceKind::Continuation => {
                PluginCanonicalReferenceKindData::Continuation
            }
            dependency::DependencyPluginCanonicalReferenceKind::ChildSession => {
                PluginCanonicalReferenceKindData::ChildSession
            }
        },
        id: value.id,
        content_hash: value.content_hash,
    }
}

fn map_artifact(
    value: PluginArtifactReferenceDataRecord,
) -> dependency::DependencyPluginArtifactReference {
    dependency::DependencyPluginArtifactReference {
        artifact_id: value.artifact_id,
        content_hash: value.content_hash,
        media_type: value.media_type,
        size_bytes: value.size_bytes,
        security_classification: map_security(value.security_classification),
    }
}

fn unmap_artifact(
    value: dependency::DependencyPluginArtifactReference,
) -> PluginArtifactReferenceDataRecord {
    PluginArtifactReferenceDataRecord {
        artifact_id: value.artifact_id,
        content_hash: value.content_hash,
        media_type: value.media_type,
        size_bytes: value.size_bytes,
        security_classification: unmap_security(value.security_classification),
    }
}

fn map_retrieve_request(
    request: RetrievePluginMemoryDataRequest,
) -> dependency::DependencyPluginMemoryRetrieveRequest {
    dependency::DependencyPluginMemoryRetrieveRequest {
        binding: map_binding(&request.binding),
        provider_id: request.provider_id,
        provider_version: request.provider_version,
        handler: request.handler,
        max_attempts: request.max_attempts,
        retry_backoff: std::time::Duration::from_millis(request.retry_backoff_ms),
        timeout: std::time::Duration::from_millis(request.timeout_ms),
        input: dependency::DependencyPluginMemoryRetrieveInput {
            query: request.input.query,
            scopes: request.input.scopes.into_iter().map(map_scope).collect(),
            max_items: request.input.max_items,
            max_bytes: request.input.max_bytes,
            artifacts: request
                .input
                .artifacts
                .into_iter()
                .map(map_artifact)
                .collect(),
            references: request
                .input
                .references
                .into_iter()
                .map(map_reference)
                .collect(),
            parameters: request.input.parameters,
        },
        readable_state: request.readable_state,
        cancellation_id: request.cancellation_id,
    }
}

/// Hashes one exact data-owned plugin memory-retrieval request using the
/// dependency protocol boundary's canonical representation.
///
/// # Errors
///
/// Returns [`PluginDataError::Invalid`] when the bounded request cannot be
/// represented by the dependency protocol.
pub fn plugin_memory_retrieve_request_hash(
    request: &RetrievePluginMemoryDataRequest,
) -> Result<ContentHash, PluginDataError> {
    dependency::plugin_memory_retrieve_request_hash(&map_retrieve_request(request.clone()))
        .map_err(|_| PluginDataError::Invalid)
}

fn unmap_retrieve_proposal(
    proposal: dependency::DependencyPluginMemoryRetrieveProposal,
) -> PluginMemoryRetrieveProposalDataRecord {
    PluginMemoryRetrieveProposalDataRecord {
        binding: unmap_binding(proposal.binding),
        provider_id: proposal.provider_id,
        provider_version: proposal.provider_version,
        items: proposal
            .items
            .into_iter()
            .map(|item| PluginMemoryItemProposalDataRecord {
                item_id: item.item_id,
                scope: unmap_scope(item.scope),
                value: item.value,
                value_hash: item.value_hash,
                artifacts: item.artifacts.into_iter().map(unmap_artifact).collect(),
                references: item.references.into_iter().map(unmap_reference).collect(),
                security_classification: unmap_security(item.security_classification),
                metadata: item.metadata,
            })
            .collect(),
    }
}

fn map_write_request(
    request: WritePluginMemoryDataRequest,
) -> dependency::DependencyPluginMemoryWriteRequest {
    dependency::DependencyPluginMemoryWriteRequest {
        binding: map_binding(&request.binding),
        provider_id: request.provider_id,
        provider_version: request.provider_version,
        handler: request.handler,
        timeout: std::time::Duration::from_millis(request.timeout_ms),
        input: dependency::DependencyPluginMemoryWriteInput {
            scope: map_scope(request.input.scope),
            boundary: match request.input.boundary {
                PluginMemoryWriteBoundaryData::Explicit => {
                    dependency::DependencyPluginMemoryWriteBoundary::Explicit
                }
                PluginMemoryWriteBoundaryData::TurnCompletion => {
                    dependency::DependencyPluginMemoryWriteBoundary::TurnCompletion
                }
                PluginMemoryWriteBoundaryData::IterationCompletion => {
                    dependency::DependencyPluginMemoryWriteBoundary::IterationCompletion
                }
                PluginMemoryWriteBoundaryData::SessionCompletion => {
                    dependency::DependencyPluginMemoryWriteBoundary::SessionCompletion
                }
            },
            value: request.input.value,
            value_hash: request.input.value_hash,
            artifacts: request
                .input
                .artifacts
                .into_iter()
                .map(map_artifact)
                .collect(),
            references: request
                .input
                .references
                .into_iter()
                .map(map_reference)
                .collect(),
            security_classification: map_security(request.input.security_classification),
            parameters: request.input.parameters,
        },
        readable_state: request.readable_state,
        cancellation_id: request.cancellation_id,
    }
}

fn unmap_write_receipt(
    receipt: dependency::DependencyPluginMemoryWriteReceipt,
) -> PluginMemoryWriteReceiptDataRecord {
    PluginMemoryWriteReceiptDataRecord {
        binding: unmap_binding(receipt.binding),
        provider_id: receipt.provider_id,
        provider_version: receipt.provider_version,
        provider_record_id: receipt.provider_record_id,
        value_hash: receipt.value_hash,
        receipt: receipt.receipt,
    }
}

fn map_compaction_request(
    request: CompactPluginContextDataRequest,
) -> dependency::DependencyPluginCompactionRequest {
    dependency::DependencyPluginCompactionRequest {
        binding: map_binding(&request.binding),
        compactor_id: request.compactor_id,
        compactor_version: request.compactor_version,
        handler: request.handler,
        max_attempts: request.max_attempts,
        retry_backoff: std::time::Duration::from_millis(request.retry_backoff_ms),
        timeout: std::time::Duration::from_millis(request.timeout_ms),
        input: dependency::DependencyPluginCompactionInput {
            projection: request.input.projection,
            projection_hash: request.input.projection_hash,
            required_references: request
                .input
                .required_references
                .into_iter()
                .map(map_reference)
                .collect(),
            required_artifacts: request
                .input
                .required_artifacts
                .into_iter()
                .map(map_artifact)
                .collect(),
            preservation_requirements: request.input.preservation_requirements,
            max_replacement_bytes: request.input.max_replacement_bytes,
            max_projection_tokens: request.input.max_projection_tokens,
            parameters: request.input.parameters,
        },
        readable_state: request.readable_state,
        cancellation_id: request.cancellation_id,
    }
}

/// Hashes one exact data-owned plugin compaction request using the dependency
/// protocol boundary's canonical representation.
///
/// # Errors
///
/// Returns [`PluginDataError::Invalid`] when the bounded request cannot be
/// represented by the dependency protocol.
pub fn plugin_compaction_request_hash(
    request: &CompactPluginContextDataRequest,
) -> Result<ContentHash, PluginDataError> {
    dependency::plugin_compaction_request_hash(&map_compaction_request(request.clone()))
        .map_err(|_| PluginDataError::Invalid)
}

fn unmap_compaction_proposal(
    proposal: dependency::DependencyPluginCompactionProposal,
) -> PluginCompactionProposalDataRecord {
    PluginCompactionProposalDataRecord {
        binding: unmap_binding(proposal.binding),
        compactor_id: proposal.compactor_id,
        compactor_version: proposal.compactor_version,
        replacement: proposal.replacement,
        replacement_hash: proposal.replacement_hash,
        preserved_references: proposal
            .preserved_references
            .into_iter()
            .map(unmap_reference)
            .collect(),
        preserved_artifacts: proposal
            .preserved_artifacts
            .into_iter()
            .map(unmap_artifact)
            .collect(),
    }
}

const fn map_state_scope(
    scope: PluginNodeStateScopeData,
) -> dependency::DependencyPluginNodeStateScope {
    match scope {
        PluginNodeStateScopeData::Invocation => {
            dependency::DependencyPluginNodeStateScope::Invocation
        }
        PluginNodeStateScopeData::ModelCall => {
            dependency::DependencyPluginNodeStateScope::ModelCall
        }
        PluginNodeStateScopeData::Turn => dependency::DependencyPluginNodeStateScope::Turn,
        PluginNodeStateScopeData::Session => dependency::DependencyPluginNodeStateScope::Session,
        PluginNodeStateScopeData::Project => dependency::DependencyPluginNodeStateScope::Project,
        PluginNodeStateScopeData::User => dependency::DependencyPluginNodeStateScope::User,
        PluginNodeStateScopeData::Runtime => dependency::DependencyPluginNodeStateScope::Runtime,
    }
}

const fn unmap_state_scope(
    scope: dependency::DependencyPluginNodeStateScope,
) -> PluginNodeStateScopeData {
    match scope {
        dependency::DependencyPluginNodeStateScope::Invocation => {
            PluginNodeStateScopeData::Invocation
        }
        dependency::DependencyPluginNodeStateScope::ModelCall => {
            PluginNodeStateScopeData::ModelCall
        }
        dependency::DependencyPluginNodeStateScope::Turn => PluginNodeStateScopeData::Turn,
        dependency::DependencyPluginNodeStateScope::Session => PluginNodeStateScopeData::Session,
        dependency::DependencyPluginNodeStateScope::Project => PluginNodeStateScopeData::Project,
        dependency::DependencyPluginNodeStateScope::User => PluginNodeStateScopeData::User,
        dependency::DependencyPluginNodeStateScope::Runtime => PluginNodeStateScopeData::Runtime,
    }
}

const fn state_scope_name(scope: PluginNodeStateScopeData) -> &'static str {
    match scope {
        PluginNodeStateScopeData::Invocation => "invocation",
        PluginNodeStateScopeData::ModelCall => "model_call",
        PluginNodeStateScopeData::Turn => "turn",
        PluginNodeStateScopeData::Session => "session",
        PluginNodeStateScopeData::Project => "project",
        PluginNodeStateScopeData::User => "user",
        PluginNodeStateScopeData::Runtime => "runtime",
    }
}

fn validate_state_receipt(
    request: &PersistPluginNodeStateDataRequest,
    receipt: &dependency::DependencyPluginNodeStateReceipt,
) -> Result<(), PluginDataError> {
    if receipt.plugin_id != request.plugin_id
        || receipt.invocation_id != request.invocation_id
        || receipt.invocation_digest != request.invocation_digest
        || receipt.executor_id != request.executor_id
        || receipt.executor_version != request.executor_version
        || receipt.executor_declaration_hash != request.executor_declaration_hash
        || receipt.state_scope != map_state_scope(request.state_scope)
        || receipt.prior_generation != request.prior_generation
        || receipt.generation != request.prior_generation.saturating_add(1)
        || receipt.state_hash != request.state_hash
        || receipt.action_digest != request.action_digest
        || receipt.authorization_digest != request.authorization_digest
        || receipt.idempotency_key != request.idempotency_key
        || receipt.receipt_id.is_empty()
        || dependency::plugin_node_state_receipt_digest(receipt)
            .map_err(|_| PluginDataError::Invalid)?
            != receipt.receipt_digest
    {
        return Err(PluginDataError::Invalid);
    }
    Ok(())
}

fn validate_state_read_receipt(
    request: &LoadPluginNodeStateDataRequest,
    loaded: &dependency::DependencyLoadedPluginNodeState,
) -> Result<(), PluginDataError> {
    let receipt = &loaded.receipt;
    let state_hash = serde_json::to_vec(&loaded.state)
        .map(|encoded| ContentHash::digest(&encoded))
        .map_err(|_| PluginDataError::Invalid)?;
    if receipt.plugin_id != request.plugin_id
        || receipt.invocation_id != request.invocation_id
        || receipt.invocation_digest != request.invocation_digest
        || receipt.executor_id != request.executor_id
        || receipt.executor_version != request.executor_version
        || receipt.executor_declaration_hash != request.executor_declaration_hash
        || receipt.state_scope != map_state_scope(request.state_scope)
        || receipt.generation != request.expected_generation
        || receipt.state_hash != request.expected_state_hash
        || state_hash != request.expected_state_hash
        || receipt.action_digest != request.action_digest
        || receipt.authorization_digest != request.authorization_digest
        || receipt.idempotency_key != request.idempotency_key
        || receipt.receipt_id.is_empty()
        || dependency::plugin_node_state_read_receipt_digest(receipt)
            .map_err(|_| PluginDataError::Invalid)?
            != receipt.receipt_digest
    {
        return Err(PluginDataError::Invalid);
    }
    Ok(())
}

fn map_state_persistence_error(
    request: &PersistPluginNodeStateDataRequest,
    error: dependency::PluginDependencyError,
) -> PluginDataError {
    match error {
        dependency::PluginDependencyError::StatePersistenceUnsupported => {
            PluginDataError::StatePersistenceUnsupported
        }
        dependency::PluginDependencyError::StaleStateGeneration => {
            PluginDataError::StaleStateGeneration
        }
        dependency::PluginDependencyError::StateConflict => PluginDataError::StateConflict,
        dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
        dependency::PluginDependencyError::AmbiguousStatePersistence
        | dependency::PluginDependencyError::Timeout
        | dependency::PluginDependencyError::Unavailable
        | dependency::PluginDependencyError::InvalidResponse => {
            PluginDataError::AmbiguousStatePersistence {
                plugin_id: request.plugin_id.clone(),
                invocation_id: request.invocation_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
            }
        }
        other => map_operation_error("persist_plugin_node_state", other),
    }
}

fn map_state_read_error(
    request: &LoadPluginNodeStateDataRequest,
    error: dependency::PluginDependencyError,
) -> PluginDataError {
    match error {
        dependency::PluginDependencyError::StateReadUnsupported => {
            PluginDataError::StateReadUnsupported
        }
        dependency::PluginDependencyError::MemoryOperationUnsupported => {
            PluginDataError::MemoryOperationUnsupported
        }
        dependency::PluginDependencyError::ContextTransformUnsupported => {
            PluginDataError::Unavailable
        }
        dependency::PluginDependencyError::StaleStateGeneration => {
            PluginDataError::StaleStateGeneration
        }
        dependency::PluginDependencyError::StateConflict => PluginDataError::StateConflict,
        dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
        dependency::PluginDependencyError::AmbiguousStateRead
        | dependency::PluginDependencyError::Timeout
        | dependency::PluginDependencyError::Unavailable
        | dependency::PluginDependencyError::InvalidResponse => {
            PluginDataError::AmbiguousStateRead {
                plugin_id: request.plugin_id.clone(),
                invocation_id: request.invocation_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
            }
        }
        other => map_operation_error("load_plugin_node_state", other),
    }
}

fn map_error(operation: &str, error: dependency::PluginDependencyError) -> PluginDataError {
    match error {
        dependency::PluginDependencyError::InvalidConfiguration
        | dependency::PluginDependencyError::InvalidRequest
        | dependency::PluginDependencyError::FrameTooLarge
        | dependency::PluginDependencyError::InvalidResponse
        | dependency::PluginDependencyError::Authorization => PluginDataError::Invalid,
        dependency::PluginDependencyError::StatePersistenceUnsupported => {
            PluginDataError::StatePersistenceUnsupported
        }
        dependency::PluginDependencyError::StateReadUnsupported => {
            PluginDataError::StateReadUnsupported
        }
        dependency::PluginDependencyError::MemoryOperationUnsupported => {
            PluginDataError::MemoryOperationUnsupported
        }
        dependency::PluginDependencyError::StaleStateGeneration => {
            PluginDataError::StaleStateGeneration
        }
        dependency::PluginDependencyError::StateConflict => PluginDataError::StateConflict,
        dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
        dependency::PluginDependencyError::Rejected { code, retryable } => {
            PluginDataError::Rejected {
                operation: operation.to_owned(),
                code,
                retryable,
            }
        }
        dependency::PluginDependencyError::ContextTransformUnsupported
        | dependency::PluginDependencyError::CancellationUnsupported
        | dependency::PluginDependencyError::LifecycleManagementUnsupported
        | dependency::PluginDependencyError::PendingRequestLimit
        | dependency::PluginDependencyError::AmbiguousStatePersistence
        | dependency::PluginDependencyError::AmbiguousStateRead
        | dependency::PluginDependencyError::AmbiguousContextTransform
        | dependency::PluginDependencyError::AmbiguousMemoryWrite
        | dependency::PluginDependencyError::Unavailable
        | dependency::PluginDependencyError::Timeout
        | dependency::PluginDependencyError::Clock => PluginDataError::Unavailable,
    }
}

fn map_operation_error(
    operation: &str,
    error: dependency::PluginDependencyError,
) -> PluginDataError {
    map_error(operation, error)
}

fn map_cancellation_error(
    request: &CancelPluginNodeInvocationDataRequest,
    error: &dependency::PluginDependencyError,
) -> PluginDataError {
    match error {
        dependency::PluginDependencyError::InvalidConfiguration
        | dependency::PluginDependencyError::InvalidRequest
        | dependency::PluginDependencyError::FrameTooLarge => PluginDataError::Invalid,
        _ => PluginDataError::Ambiguous {
            plugin_id: request.target.plugin_id.clone(),
            executor_id: request.target.operation_id.clone(),
        },
    }
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
    #[error("plugin node execution is ambiguous for `{plugin_id}` executor `{executor_id}`")]
    Ambiguous {
        plugin_id: String,
        executor_id: String,
    },
    #[error(
        "plugin context-transform execution is ambiguous for `{plugin_id}` transform `{transform_id}` invocation `{invocation_id}`"
    )]
    AmbiguousContextTransform {
        plugin_id: String,
        transform_id: String,
        invocation_id: String,
    },
    #[error("plugin-host protocol has no memory or compaction invocation")]
    MemoryOperationUnsupported,
    #[error(
        "approved plugin memory write is ambiguous for `{plugin_id}` provider `{provider_id}` invocation `{invocation_id}` idempotency `{idempotency_key}`"
    )]
    AmbiguousMemoryWrite {
        plugin_id: String,
        provider_id: String,
        invocation_id: String,
        idempotency_key: String,
    },
    #[error("plugin-host protocol has no durable plugin-node state receipt")]
    StatePersistenceUnsupported,
    #[error("plugin-host protocol has no authenticated plugin-node state read")]
    StateReadUnsupported,
    #[error("plugin-node state scope lacks an exact canonical persistence identity")]
    UnsupportedStateScope,
    #[error("plugin-node state generation is stale")]
    StaleStateGeneration,
    #[error("plugin-node state conflicts with a prior idempotent write")]
    StateConflict,
    #[error("plugin-node state persistence was cancelled")]
    Cancelled,
    #[error(
        "plugin-node state persistence is ambiguous for `{plugin_id}` invocation `{invocation_id}` idempotency `{idempotency_key}`"
    )]
    AmbiguousStatePersistence {
        plugin_id: String,
        invocation_id: String,
        idempotency_key: String,
    },
    #[error(
        "plugin-node state read is ambiguous for `{plugin_id}` invocation `{invocation_id}` idempotency `{idempotency_key}`"
    )]
    AmbiguousStateRead {
        plugin_id: String,
        invocation_id: String,
        idempotency_key: String,
    },
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;

    const AUTOMATIC_MEMORY_TOML: &str =
        include_str!("../../../../tests/fixtures/plugins/automatic-memory.toml");
    const PLUGIN_CONTEXT_TOML: &str =
        include_str!("../../../../tests/fixtures/plugins/plugin-context.toml");
    const PLUGIN_COMPACTION_TOML: &str =
        include_str!("../../../../tests/fixtures/plugins/plugin-compaction.toml");

    fn source(contents: String) -> dependency::DependencyPluginManifestSource {
        dependency::DependencyPluginManifestSource {
            locator: String::from("fixture.toml"),
            format: String::from("toml"),
            contents,
        }
    }

    #[test]
    fn automatic_memory_process_fixture_is_valid_and_declaration_hashes_are_stable() {
        let manifest = AUTOMATIC_MEMORY_TOML
            .replace("__PLUGIN_PROGRAM__", "fixture-worker")
            .replace("__PLUGIN_ARGS__", "[]");
        let catalog =
            compile_plugin_catalog(&[source(manifest)], "1.0.0", Vec::new()).expect("catalog");
        let hashes = catalog.manifests[0]
            .memory_providers
            .iter()
            .map(|provider| {
                (
                    provider.provider_id.as_str(),
                    provider.declaration_hash.to_hex(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            hashes,
            BTreeMap::from([
                (
                    "fixture.memory.invalid",
                    String::from(
                        "572f03ae7b5fde6771c40521785c287a722da29a2814a0c228e320eaa94aca66",
                    ),
                ),
                (
                    "fixture.memory.success",
                    String::from(
                        "2e4f6dc7fa1e3ad211c32148bacb5c208c99ed726e021866ac31699742240266",
                    ),
                ),
                (
                    "fixture.memory.timeout",
                    String::from(
                        "6025dd4db5a87e2d72055147bc5f9022ab1ffd8850d034be47d98384d08ad338",
                    ),
                ),
            ])
        );
    }

    #[test]
    fn plugin_context_process_fixtures_are_valid_and_declaration_hashes_are_stable() {
        let manifest = PLUGIN_CONTEXT_TOML
            .replace("__PLUGIN_PROGRAM__", "fixture-worker")
            .replace("__PLUGIN_ARGS__", "[]");
        let compactor = PLUGIN_COMPACTION_TOML
            .replace("__PLUGIN_PROGRAM__", "fixture-worker")
            .replace("__PLUGIN_ARGS__", "[]");
        let catalog =
            compile_plugin_catalog(&[source(manifest), source(compactor)], "1.0.0", Vec::new())
                .expect("catalog");
        let providers = catalog
            .manifests
            .iter()
            .flat_map(|manifest| &manifest.memory_providers)
            .map(|provider| {
                (
                    provider.provider_id.as_str(),
                    provider.declaration_hash.to_hex(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let compactors = catalog
            .manifests
            .iter()
            .flat_map(|manifest| &manifest.compactors)
            .map(|compactor| {
                (
                    compactor.compactor_id.as_str(),
                    compactor.declaration_hash.to_hex(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            providers,
            BTreeMap::from([
                (
                    "fixture.context-memory.invalid",
                    String::from(
                        "a4028f30754d127c743f064045462ffce0ca7daae1d590834d1979a284010cc6"
                    ),
                ),
                (
                    "fixture.context-memory.success",
                    String::from(
                        "2b72f4cd63fe66f74672098d981ba2e38d1d2b1ba3e51f9df178966d7885fc40"
                    ),
                ),
                (
                    "fixture.context-memory.timeout",
                    String::from(
                        "2e59bf93479578db0e76f547b37b02a6a7dc6d44c99c9a3157ad8993def50f96"
                    ),
                ),
            ])
        );
        assert_eq!(
            compactors,
            BTreeMap::from([
                (
                    "fixture.context-compactor.invalid",
                    String::from(
                        "6135a6b4689d5cf3a7d23a406da04ba9731e2ef2c8873ffe97351352c0e9d1a2"
                    ),
                ),
                (
                    "fixture.context-compactor.success",
                    String::from(
                        "e5c9320e9582f147a37f6f1f7fd0726c83e4f1c996bf8c52d8a32ad037723938"
                    ),
                ),
                (
                    "fixture.context-compactor.timeout",
                    String::from(
                        "4dc2c55dd5d476170166aabc04684f9e09c27a8974c0b2eee1abd9b34817af33"
                    ),
                ),
            ])
        );
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

    fn context_transform(handler: &str) -> String {
        let mut manifest = sdk::parse_toml(&observer("[]")).expect("base plugin manifest");
        manifest.identity.id = String::from("fixture.context");
        manifest.category = sdk::PluginCategory::ContextTransform;
        manifest.classification = sdk::PluginClassification::Blocking;
        manifest.failure_policy = sdk::FailurePolicy::Reject;
        manifest.required_capabilities.clear();
        manifest.provided_capabilities = vec![String::from("context.redaction")];
        manifest.subscribed_events.clear();
        manifest.context_transforms = vec![sdk::ContextTransformManifest {
            transform_id: String::from("fixture.redact"),
            version: String::from("1.0.0"),
            runtime_api: String::from("^0.1"),
            handler: handler.to_owned(),
            lifecycle: sdk::ContextTransformLifecycle::BeforeModelRequest,
            capabilities: vec![String::from("context.redaction")],
            input_schema: String::from(r#"{"type":"object","required":["projection"]}"#),
            output_schema: String::from(r#"{"type":"object","required":["replacement"]}"#),
            timeout_ms: 500,
            failure_policy: sdk::FailurePolicy::Reject,
            idempotency: sdk::ContextTransformIdempotency::Idempotent,
            required_permissions: sdk::PermissionManifest::default(),
            state_scope: sdk::PluginScope::ModelCall,
            external_effects: false,
        }];
        sdk::to_toml(&manifest).expect("context-transform TOML")
    }

    fn memory_provider(handler: &str) -> String {
        let mut manifest = sdk::parse_toml(&observer("[]")).expect("base plugin manifest");
        manifest.identity.id = String::from("fixture.memory");
        manifest.category = sdk::PluginCategory::Memory;
        manifest.classification = sdk::PluginClassification::Blocking;
        manifest.failure_policy = sdk::FailurePolicy::Reject;
        manifest.required_capabilities.clear();
        manifest.provided_capabilities = vec![String::from("memory.fixture")];
        manifest.subscribed_events.clear();
        manifest.memory_providers = vec![sdk::MemoryProviderManifest {
            provider_id: String::from("fixture.semantic"),
            version: String::from("1.4.0"),
            runtime_api: String::from("^0.1"),
            capabilities: vec![String::from("memory.fixture")],
            retrieve: sdk::MemoryRetrieveManifest {
                handler: handler.to_owned(),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 500,
                failure_policy: sdk::FailurePolicy::Retry {
                    max_attempts: 2,
                    backoff_ms: 10,
                },
                idempotency: sdk::PluginOperationIdempotency::Idempotent,
                required_permissions: sdk::PermissionManifest::default(),
                state_scope: sdk::PluginScope::Session,
                external_effects: false,
            },
            write: Some(sdk::MemoryWriteManifest {
                handler: String::from("write_memory"),
                input_schema: String::from(r#"{"type":"object"}"#),
                output_schema: String::from(r#"{"type":"object"}"#),
                timeout_ms: 500,
                failure_policy: sdk::FailurePolicy::Reject,
                idempotency: sdk::PluginOperationIdempotency::NonIdempotent,
                required_permissions: sdk::PermissionManifest::default(),
                state_scope: sdk::PluginScope::Session,
                external_effects: true,
            }),
        }];
        sdk::to_toml(&manifest).expect("memory-provider TOML")
    }

    fn compactor(handler: &str) -> String {
        let mut manifest = sdk::parse_toml(&observer("[]")).expect("base plugin manifest");
        manifest.identity.id = String::from("fixture.compaction");
        manifest.category = sdk::PluginCategory::Compaction;
        manifest.classification = sdk::PluginClassification::Blocking;
        manifest.failure_policy = sdk::FailurePolicy::Reject;
        manifest.required_capabilities.clear();
        manifest.provided_capabilities = vec![String::from("compaction.fixture")];
        manifest.subscribed_events.clear();
        manifest.compactors = vec![sdk::CompactorManifest {
            compactor_id: String::from("fixture.summary"),
            version: String::from("2.0.0"),
            runtime_api: String::from("^0.1"),
            handler: handler.to_owned(),
            capabilities: vec![String::from("compaction.fixture")],
            input_schema: String::from(r#"{"type":"object"}"#),
            output_schema: String::from(r#"{"type":"object"}"#),
            timeout_ms: 600,
            failure_policy: sdk::FailurePolicy::Retry {
                max_attempts: 2,
                backoff_ms: 10,
            },
            idempotency: sdk::PluginOperationIdempotency::Idempotent,
            required_permissions: sdk::PermissionManifest::default(),
            state_scope: sdk::PluginScope::Session,
            external_effects: false,
        }];
        sdk::to_toml(&manifest).expect("compactor TOML")
    }

    #[derive(Clone)]
    struct StateDependency {
        result:
            Result<dependency::DependencyPluginNodeStateReceipt, dependency::PluginDependencyError>,
        read_result:
            Result<dependency::DependencyLoadedPluginNodeState, dependency::PluginDependencyError>,
        context_result: Result<
            dependency::DependencyPluginContextTransformProposal,
            dependency::PluginDependencyError,
        >,
        context_requests:
            Arc<Mutex<Vec<dependency::DependencyPluginContextTransformInvocationRequest>>>,
    }

    #[derive(Clone, Default)]
    struct LifecycleDependency {
        requests: Arc<Mutex<Vec<dependency::DependencyPluginLifecycleRequest>>>,
    }

    #[derive(Clone)]
    struct CancellationDependency {
        requests: Arc<Mutex<Vec<dependency::DependencyCancelPluginInvocationRequest>>>,
        status: dependency::DependencyPluginInvocationCancellationStatus,
        fail: Option<dependency::PluginDependencyError>,
    }

    #[async_trait]
    impl dependency::RuntimePluginDependencyPort for CancellationDependency {
        async fn negotiate(
            &self,
            _session_id: String,
            _runtime_api_version: String,
            capabilities: BTreeSet<String>,
        ) -> Result<BTreeSet<String>, dependency::PluginDependencyError> {
            Ok(capabilities)
        }

        async fn validate_set(
            &self,
            _session_id: String,
            _manifests_json: Vec<String>,
        ) -> Result<Vec<String>, dependency::PluginDependencyError> {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn load(
            &self,
            _request: dependency::DependencyPluginLoadRequest,
        ) -> Result<dependency::DependencyPluginLoadResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke(
            &self,
            _request: dependency::DependencyPluginInvocationRequest,
        ) -> Result<(dependency::DependencyPluginDecision, u8), dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn observe(
            &self,
            _request: dependency::DependencyPluginObservationRequest,
        ) -> Result<dependency::DependencyPluginObservationResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke_node_executor(
            &self,
            _request: dependency::DependencyPluginNodeInvocationRequest,
        ) -> Result<dependency::DependencyPluginNodeOutcome, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn cancel_plugin_invocation(
            &self,
            request: dependency::DependencyCancelPluginInvocationRequest,
        ) -> Result<
            dependency::DependencyPluginInvocationCancellationReceipt,
            dependency::PluginDependencyError,
        > {
            self.requests
                .lock()
                .expect("cancellation requests")
                .push(request.clone());
            if let Some(error) = &self.fail {
                return Err(error.clone());
            }
            Ok(dependency::DependencyPluginInvocationCancellationReceipt {
                target: request.target,
                reason_code: request.reason_code,
                action_digest: ContentHash::digest(b"cancel action"),
                nonce: request.nonce,
                idempotency_key: request.idempotency_key,
                cancellation_id: request.cancellation_id,
                status: self.status,
                receipt_id: String::from("cancel-receipt-1"),
                receipt_digest: ContentHash::digest(b"cancel receipt"),
            })
        }

        async fn shutdown(&self) {}
    }

    #[async_trait]
    impl dependency::RuntimePluginDependencyPort for LifecycleDependency {
        async fn negotiate(
            &self,
            _session_id: String,
            _runtime_api_version: String,
            capabilities: BTreeSet<String>,
        ) -> Result<BTreeSet<String>, dependency::PluginDependencyError> {
            Ok(capabilities)
        }

        async fn validate_set(
            &self,
            _session_id: String,
            _manifests_json: Vec<String>,
        ) -> Result<Vec<String>, dependency::PluginDependencyError> {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn load(
            &self,
            _request: dependency::DependencyPluginLoadRequest,
        ) -> Result<dependency::DependencyPluginLoadResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke(
            &self,
            _request: dependency::DependencyPluginInvocationRequest,
        ) -> Result<(dependency::DependencyPluginDecision, u8), dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn observe(
            &self,
            _request: dependency::DependencyPluginObservationRequest,
        ) -> Result<dependency::DependencyPluginObservationResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke_node_executor(
            &self,
            _request: dependency::DependencyPluginNodeInvocationRequest,
        ) -> Result<dependency::DependencyPluginNodeOutcome, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn change_plugin_lifecycle(
            &self,
            request: dependency::DependencyPluginLifecycleRequest,
        ) -> Result<dependency::DependencyPluginLifecycleResult, dependency::PluginDependencyError>
        {
            self.requests
                .lock()
                .expect("lifecycle requests")
                .push(request.clone());
            let (state, operation, outcome) = match request.action {
                dependency::DependencyPluginLifecycleAction::Disable => {
                    ("disabled", "disable", "disabled")
                }
                dependency::DependencyPluginLifecycleAction::Enable => {
                    ("active", "enable", "active")
                }
                dependency::DependencyPluginLifecycleAction::Quarantine => (
                    "quarantined",
                    "quarantine",
                    request.reason_code.as_deref().unwrap_or("quarantined"),
                ),
                dependency::DependencyPluginLifecycleAction::Unquarantine => {
                    ("active", "unquarantine", "active")
                }
            };
            Ok(dependency::DependencyPluginLifecycleResult {
                plugin_id: request.plugin_id,
                state: state.to_owned(),
                audit_operation: operation.to_owned(),
                audit_outcome: outcome.to_owned(),
            })
        }

        async fn shutdown(&self) {}
    }

    #[async_trait]
    impl dependency::RuntimePluginDependencyPort for StateDependency {
        async fn negotiate(
            &self,
            _session_id: String,
            _runtime_api_version: String,
            capabilities: BTreeSet<String>,
        ) -> Result<BTreeSet<String>, dependency::PluginDependencyError> {
            Ok(capabilities)
        }

        async fn validate_set(
            &self,
            _session_id: String,
            _manifests_json: Vec<String>,
        ) -> Result<Vec<String>, dependency::PluginDependencyError> {
            Ok(vec![String::from("fixture.plugin")])
        }

        async fn load(
            &self,
            _request: dependency::DependencyPluginLoadRequest,
        ) -> Result<dependency::DependencyPluginLoadResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke(
            &self,
            _request: dependency::DependencyPluginInvocationRequest,
        ) -> Result<(dependency::DependencyPluginDecision, u8), dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn observe(
            &self,
            _request: dependency::DependencyPluginObservationRequest,
        ) -> Result<dependency::DependencyPluginObservationResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke_node_executor(
            &self,
            _request: dependency::DependencyPluginNodeInvocationRequest,
        ) -> Result<dependency::DependencyPluginNodeOutcome, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke_context_transform(
            &self,
            request: dependency::DependencyPluginContextTransformInvocationRequest,
        ) -> Result<
            dependency::DependencyPluginContextTransformProposal,
            dependency::PluginDependencyError,
        > {
            self.context_requests
                .lock()
                .expect("context request projection")
                .push(request);
            self.context_result.clone()
        }

        async fn persist_plugin_node_state(
            &self,
            _request: dependency::DependencyPersistPluginNodeStateRequest,
        ) -> Result<dependency::DependencyPluginNodeStateReceipt, dependency::PluginDependencyError>
        {
            self.result.clone()
        }

        async fn load_plugin_node_state(
            &self,
            _request: dependency::DependencyLoadPluginNodeStateRequest,
        ) -> Result<dependency::DependencyLoadedPluginNodeState, dependency::PluginDependencyError>
        {
            self.read_result.clone()
        }

        async fn shutdown(&self) {}
    }

    #[derive(Clone, Default)]
    struct MemoryDependency {
        retrieve_requests: Arc<Mutex<Vec<dependency::DependencyPluginMemoryRetrieveRequest>>>,
        write_requests: Arc<Mutex<Vec<dependency::DependencyPluginMemoryWriteRequest>>>,
        compaction_requests: Arc<Mutex<Vec<dependency::DependencyPluginCompactionRequest>>>,
        ambiguous_write: bool,
    }

    #[async_trait]
    impl dependency::RuntimePluginDependencyPort for MemoryDependency {
        async fn negotiate(
            &self,
            _session_id: String,
            _runtime_api_version: String,
            capabilities: BTreeSet<String>,
        ) -> Result<BTreeSet<String>, dependency::PluginDependencyError> {
            Ok(capabilities)
        }

        async fn validate_set(
            &self,
            _session_id: String,
            _manifests_json: Vec<String>,
        ) -> Result<Vec<String>, dependency::PluginDependencyError> {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn load(
            &self,
            _request: dependency::DependencyPluginLoadRequest,
        ) -> Result<dependency::DependencyPluginLoadResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke(
            &self,
            _request: dependency::DependencyPluginInvocationRequest,
        ) -> Result<(dependency::DependencyPluginDecision, u8), dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn observe(
            &self,
            _request: dependency::DependencyPluginObservationRequest,
        ) -> Result<dependency::DependencyPluginObservationResult, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn invoke_node_executor(
            &self,
            _request: dependency::DependencyPluginNodeInvocationRequest,
        ) -> Result<dependency::DependencyPluginNodeOutcome, dependency::PluginDependencyError>
        {
            Err(dependency::PluginDependencyError::InvalidRequest)
        }

        async fn retrieve_memory(
            &self,
            request: dependency::DependencyPluginMemoryRetrieveRequest,
        ) -> Result<
            dependency::DependencyPluginMemoryRetrieveProposal,
            dependency::PluginDependencyError,
        > {
            self.retrieve_requests
                .lock()
                .expect("retrieve requests")
                .push(request.clone());
            Ok(dependency::DependencyPluginMemoryRetrieveProposal {
                binding: request.binding,
                provider_id: request.provider_id,
                provider_version: request.provider_version,
                items: Vec::new(),
            })
        }

        async fn write_memory(
            &self,
            request: dependency::DependencyPluginMemoryWriteRequest,
        ) -> Result<dependency::DependencyPluginMemoryWriteReceipt, dependency::PluginDependencyError>
        {
            self.write_requests
                .lock()
                .expect("write requests")
                .push(request.clone());
            if self.ambiguous_write {
                return Err(dependency::PluginDependencyError::AmbiguousMemoryWrite);
            }
            Ok(dependency::DependencyPluginMemoryWriteReceipt {
                binding: request.binding,
                provider_id: request.provider_id,
                provider_version: request.provider_version,
                provider_record_id: String::from("record-1"),
                value_hash: request.input.value_hash,
                receipt: json!({"accepted": true}),
            })
        }

        async fn compact_context(
            &self,
            request: dependency::DependencyPluginCompactionRequest,
        ) -> Result<dependency::DependencyPluginCompactionProposal, dependency::PluginDependencyError>
        {
            self.compaction_requests
                .lock()
                .expect("compaction requests")
                .push(request.clone());
            let replacement = json!({"summary": "bounded"});
            Ok(dependency::DependencyPluginCompactionProposal {
                binding: request.binding,
                compactor_id: request.compactor_id,
                compactor_version: request.compactor_version,
                replacement_hash: ContentHash::digest(
                    &serde_json::to_vec(&replacement).expect("replacement"),
                ),
                replacement,
                preserved_references: request.input.required_references,
                preserved_artifacts: request.input.required_artifacts,
            })
        }

        async fn shutdown(&self) {}
    }

    fn state_request() -> PersistPluginNodeStateDataRequest {
        let state = json!({"cursor": 2});
        let state_hash =
            ContentHash::digest(&serde_json::to_vec(&state).expect("bounded fixture state"));
        let declaration_hash = ContentHash::digest(b"declaration");
        PersistPluginNodeStateDataRequest {
            cancellation_target: test_cancellation_target_data(
                "fixture.plugin",
                "1.0.0",
                "plugin-node:invocation",
                "fixture.executor:state-write",
                declaration_hash,
                state_hash,
            ),
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("plugin-node:invocation"),
            invocation_digest: ContentHash::digest(b"invocation"),
            executor_id: String::from("fixture.executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: declaration_hash,
            configuration_reference: ContentHash::digest(b"configuration"),
            state_scope: PluginNodeStateScopeData::Invocation,
            prior_generation: 1,
            prior_state_hash: Some(ContentHash::digest(b"prior")),
            state_hash,
            state,
            action_digest: ContentHash::digest(b"action"),
            authorization_digest: ContentHash::digest(b"authorization"),
            nonce: String::from("nonce-1"),
            cancellation_id: String::from("cancel-1"),
            idempotency_key: String::from("state-write-1"),
        }
    }

    fn state_receipt(
        request: &PersistPluginNodeStateDataRequest,
    ) -> dependency::DependencyPluginNodeStateReceipt {
        let mut receipt = dependency::DependencyPluginNodeStateReceipt {
            plugin_id: request.plugin_id.clone(),
            invocation_id: request.invocation_id.clone(),
            invocation_digest: request.invocation_digest,
            executor_id: request.executor_id.clone(),
            executor_version: request.executor_version.clone(),
            executor_declaration_hash: request.executor_declaration_hash,
            state_scope: map_state_scope(request.state_scope),
            prior_generation: request.prior_generation,
            generation: request.prior_generation + 1,
            state_hash: request.state_hash,
            action_digest: request.action_digest,
            authorization_digest: request.authorization_digest,
            idempotency_key: request.idempotency_key.clone(),
            receipt_id: String::from("state-receipt-1"),
            receipt_digest: ContentHash::digest(b"pending"),
            replayed: false,
        };
        receipt.receipt_digest =
            dependency::plugin_node_state_receipt_digest(&receipt).expect("receipt digest");
        receipt
    }

    fn state_read_request() -> LoadPluginNodeStateDataRequest {
        let declaration_hash = ContentHash::digest(b"declaration");
        let expected_state_hash = ContentHash::digest(
            &serde_json::to_vec(&json!({"cursor": 2})).expect("bounded fixture state"),
        );
        LoadPluginNodeStateDataRequest {
            cancellation_target: test_cancellation_target_data(
                "fixture.plugin",
                "1.0.0",
                "plugin-node:invocation",
                "fixture.executor:state-read",
                declaration_hash,
                expected_state_hash,
            ),
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("plugin-node:invocation"),
            invocation_digest: ContentHash::digest(b"invocation"),
            executor_id: String::from("fixture.executor"),
            executor_version: String::from("1.0.0"),
            executor_declaration_hash: declaration_hash,
            configuration_reference: ContentHash::digest(b"configuration"),
            state_scope: PluginNodeStateScopeData::Invocation,
            expected_generation: 2,
            expected_state_hash,
            action_digest: ContentHash::digest(b"read-action"),
            authorization_digest: ContentHash::digest(b"read-authorization"),
            nonce: String::from("read-nonce-1"),
            cancellation_id: String::from("read-cancel-1"),
            idempotency_key: String::from("state-read-1"),
        }
    }

    fn state_read_result(
        request: &LoadPluginNodeStateDataRequest,
    ) -> dependency::DependencyLoadedPluginNodeState {
        let mut receipt = dependency::DependencyPluginNodeStateReadReceipt {
            plugin_id: request.plugin_id.clone(),
            invocation_id: request.invocation_id.clone(),
            invocation_digest: request.invocation_digest,
            executor_id: request.executor_id.clone(),
            executor_version: request.executor_version.clone(),
            executor_declaration_hash: request.executor_declaration_hash,
            state_scope: map_state_scope(request.state_scope),
            generation: request.expected_generation,
            state_hash: request.expected_state_hash,
            action_digest: request.action_digest,
            authorization_digest: request.authorization_digest,
            idempotency_key: request.idempotency_key.clone(),
            receipt_id: String::from("state-read-receipt-1"),
            receipt_digest: ContentHash::digest(b"pending"),
            replayed: false,
        };
        receipt.receipt_digest =
            dependency::plugin_node_state_read_receipt_digest(&receipt).expect("receipt digest");
        dependency::DependencyLoadedPluginNodeState {
            state: json!({"cursor": 2}),
            receipt,
        }
    }

    fn runtime_state_data(
        result: Result<
            dependency::DependencyPluginNodeStateReceipt,
            dependency::PluginDependencyError,
        >,
    ) -> RuntimePluginData {
        runtime_state_data_with_read(
            result,
            Err(dependency::PluginDependencyError::StateReadUnsupported),
        )
    }

    fn runtime_state_data_with_read(
        result: Result<
            dependency::DependencyPluginNodeStateReceipt,
            dependency::PluginDependencyError,
        >,
        read_result: Result<
            dependency::DependencyLoadedPluginNodeState,
            dependency::PluginDependencyError,
        >,
    ) -> RuntimePluginData {
        let data = RuntimePluginData::new(
            Arc::new(StateDependency {
                result,
                read_result,
                context_result: Err(dependency::PluginDependencyError::ContextTransformUnsupported),
                context_requests: Arc::new(Mutex::new(Vec::new())),
            }),
            vec![PluginManifestDataRecord {
                id: String::from("fixture.plugin"),
                version: String::from("1.0.0"),
                category: String::from("graph_node"),
                class: String::from("blocking"),
                provided_capabilities: BTreeSet::new(),
                subscribed_events: BTreeSet::new(),
                timeout_ms: 1_000,
                failure_policy: String::from("reject"),
                canonical_manifest_json: String::from("{}"),
                configuration: json!({}),
                configuration_reference: ContentHash::digest(b"{}"),
                node_executors: vec![PluginNodeExecutorDataRecord {
                    plugin_version: String::from("1.0.0"),
                    executor_id: String::from("fixture.executor"),
                    version: String::from("1.0.0"),
                    runtime_api: String::from("^1.0"),
                    node_kind: String::from("plugin_fixture"),
                    handler: String::from("execute"),
                    capabilities: BTreeSet::new(),
                    input_schema: String::from(r#"{"type":"object"}"#),
                    output_schema: String::from(r#"{"type":"object"}"#),
                    timeout_ms: 1_000,
                    failure_policy: String::from("reject"),
                    max_attempts: 1,
                    retry_backoff_ms: 0,
                    idempotent: true,
                    tool_permissions: BTreeSet::new(),
                    network_permissions: BTreeSet::new(),
                    state_scope: String::from("invocation"),
                    external_effects: false,
                    declaration_hash: ContentHash::digest(b"declaration"),
                }],
                context_transforms: Vec::new(),
                memory_providers: Vec::new(),
                compactors: Vec::new(),
            }],
        );
        data.activated
            .lock()
            .expect("activation projection")
            .insert(
                String::from("session-1"),
                BTreeSet::from([String::from("fixture.plugin")]),
            );
        data
    }

    fn runtime_cancellation_data(
        status: dependency::DependencyPluginInvocationCancellationStatus,
        fail: Option<dependency::PluginDependencyError>,
    ) -> (RuntimePluginData, Arc<CancellationDependency>) {
        let mock_dependency = Arc::new(CancellationDependency {
            requests: Arc::new(Mutex::new(Vec::new())),
            status,
            fail,
        });
        let seed = runtime_state_data(Err(
            dependency::PluginDependencyError::StatePersistenceUnsupported,
        ));
        let data = RuntimePluginData::new(
            mock_dependency.clone(),
            seed.manifests.values().cloned().collect(),
        );
        (data, mock_dependency)
    }

    fn cancellation_data_request() -> CancelPluginNodeInvocationDataRequest {
        CancelPluginNodeInvocationDataRequest {
            target: test_cancellation_target_data(
                "fixture.plugin",
                "1.0.0",
                "plugin-node:fixture",
                "fixture.executor",
                ContentHash::digest(b"declaration"),
                ContentHash::digest(b"request"),
            ),
            reason_code: String::from("parallel_branch_cancelled"),
            nonce: String::from("nonce-1"),
            idempotency_key: String::from("cancel-once-1"),
            cancellation_id: String::from("cancel-1"),
        }
    }

    #[tokio::test]
    async fn cancellation_data_maps_exact_request_and_receipt() {
        for (dependency_status, expected_status) in [
            (
                dependency::DependencyPluginInvocationCancellationStatus::Signalled,
                PluginInvocationCancellationDataStatus::Signalled,
            ),
            (
                dependency::DependencyPluginInvocationCancellationStatus::AlreadyTerminal,
                PluginInvocationCancellationDataStatus::AlreadyTerminal,
            ),
        ] {
            let (data, dependency) = runtime_cancellation_data(dependency_status, None);
            let request = cancellation_data_request();
            let result = data
                .cancel_node_invocation(request.clone())
                .await
                .expect("cancellation receipt");
            assert_eq!(result.target, request.target);
            assert_eq!(result.reason_code, request.reason_code);
            assert_eq!(result.nonce, request.nonce);
            assert_eq!(result.idempotency_key, request.idempotency_key);
            assert_eq!(result.cancellation_id, request.cancellation_id);
            assert_eq!(result.status, expected_status);
            assert_ne!(result.action_digest, ContentHash::from_bytes([0; 32]));
            assert_ne!(result.receipt_digest, ContentHash::from_bytes([0; 32]));
            assert_eq!(dependency.requests.lock().expect("requests").len(), 1);
        }
    }

    #[tokio::test]
    async fn cancellation_data_rejects_target_substitution_before_dependency() {
        let (data, dependency) = runtime_cancellation_data(
            dependency::DependencyPluginInvocationCancellationStatus::Signalled,
            None,
        );
        for mutate in 0..4 {
            let mut request = cancellation_data_request();
            match mutate {
                0 => request.target.session_id = String::new(),
                1 => request.target.plugin_version = String::from("9.9.9"),
                2 => request.target.operation_id = String::from("other.executor"),
                _ => request.target.request_hash = ContentHash::digest(b"substituted"),
            }
            assert_eq!(
                data.cancel_node_invocation(request).await,
                Err(PluginDataError::Invalid)
            );
        }
        assert!(dependency.requests.lock().expect("requests").is_empty());
    }

    #[tokio::test]
    async fn cancellation_data_preserves_timeout_as_no_receipt() {
        let (data, dependency) = runtime_cancellation_data(
            dependency::DependencyPluginInvocationCancellationStatus::Signalled,
            Some(dependency::PluginDependencyError::Timeout),
        );
        assert_eq!(
            data.cancel_node_invocation(cancellation_data_request())
                .await,
            Err(PluginDataError::Ambiguous {
                plugin_id: String::from("fixture.plugin"),
                executor_id: String::from("fixture.executor"),
            })
        );
        assert_eq!(dependency.requests.lock().expect("requests").len(), 1);
    }

    fn runtime_context_data(
        result: Result<
            dependency::DependencyPluginContextTransformProposal,
            dependency::PluginDependencyError,
        >,
    ) -> (
        RuntimePluginData,
        PluginContextTransformDataRecord,
        Arc<Mutex<Vec<dependency::DependencyPluginContextTransformInvocationRequest>>>,
    ) {
        let catalog = compile_plugin_catalog(
            &[source(context_transform("redact_projection"))],
            "0.1.0",
            Vec::new(),
        )
        .expect("context-transform catalog");
        let declaration = catalog.manifests[0].context_transforms[0].clone();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let data = RuntimePluginData::new(
            Arc::new(StateDependency {
                result: Err(dependency::PluginDependencyError::StatePersistenceUnsupported),
                read_result: Err(dependency::PluginDependencyError::StateReadUnsupported),
                context_result: result,
                context_requests: requests.clone(),
            }),
            catalog.manifests,
        );
        data.activated
            .lock()
            .expect("activation projection")
            .insert(
                String::from("session-1"),
                BTreeSet::from([String::from("fixture.context")]),
            );
        (data, declaration, requests)
    }

    fn context_request(
        declaration: &PluginContextTransformDataRecord,
    ) -> InvokePluginContextTransformDataRequest {
        let input = json!({"projection": [{"role": "user", "content": "secret"}]});
        let input_hash = ContentHash::digest(&serde_json::to_vec(&input).expect("context input"));
        InvokePluginContextTransformDataRequest {
            cancellation_target: test_cancellation_target_data(
                "fixture.context",
                &declaration.plugin_version,
                "session-1:run-1:model-1",
                &declaration.transform_id,
                declaration.declaration_hash,
                input_hash,
            ),
            session_id: String::from("session-1"),
            plugin_id: String::from("fixture.context"),
            invocation_id: String::from("session-1:run-1:model-1"),
            transform_id: declaration.transform_id.clone(),
            transform_version: declaration.version.clone(),
            declaration_hash: declaration.declaration_hash,
            timeout_ms: declaration.timeout_ms,
            configuration_reference: ContentHash::digest(b"context configuration"),
            lifecycle: PluginContextTransformLifecycleData::BeforeModelRequest,
            handler: declaration.handler.clone(),
            input,
            readable_state: json!({"classification": "private"}),
            cancellation_id: String::from("cancel-1"),
        }
    }

    fn runtime_memory_data(ambiguous_write: bool) -> (RuntimePluginData, Arc<MemoryDependency>) {
        let catalog = compile_plugin_catalog(
            &[
                source(memory_provider("retrieve_memory")),
                source(compactor("compact_projection")),
            ],
            "0.1.0",
            Vec::new(),
        )
        .expect("memory catalog");
        let dependency = Arc::new(MemoryDependency {
            ambiguous_write,
            ..MemoryDependency::default()
        });
        let data = RuntimePluginData::new(dependency.clone(), catalog.manifests);
        data.activated
            .lock()
            .expect("activation projection")
            .insert(
                String::from("session-1"),
                BTreeSet::from([
                    String::from("fixture.memory"),
                    String::from("fixture.compaction"),
                ]),
            );
        (data, dependency)
    }

    fn operation_binding(
        manifest: &PluginManifestDataRecord,
        declaration_hash: ContentHash,
    ) -> PluginOperationBindingDataRecord {
        PluginOperationBindingDataRecord {
            plugin_id: manifest.id.clone(),
            plugin_version: manifest.version.clone(),
            invocation_id: String::from("session-1:run-1:operation-1"),
            operation_id: String::from("operation-1"),
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: Some(String::from("node-1")),
            declaration_hash,
            configuration_reference: ContentHash::digest(
                &serde_json::to_vec(&manifest.configuration).expect("configuration"),
            ),
            request_hash: ContentHash::digest(b"typed request"),
            idempotency_key: String::from("idempotency-1"),
            attempt: 1,
        }
    }

    #[tokio::test]
    async fn memory_and_compaction_data_map_exact_catalog_bound_requests() {
        let (data, dependency) = runtime_memory_data(false);
        let memory_manifest = data
            .manifests
            .get("fixture.memory")
            .expect("memory manifest");
        let provider = &memory_manifest.memory_providers[0];
        let retrieve = RetrievePluginMemoryDataRequest {
            binding: operation_binding(memory_manifest, provider.declaration_hash),
            provider_id: provider.provider_id.clone(),
            provider_version: provider.version.clone(),
            handler: provider.retrieve.handler.clone(),
            max_attempts: provider.retrieve.max_attempts,
            retry_backoff_ms: provider.retrieve.retry_backoff_ms,
            timeout_ms: provider.retrieve.timeout_ms,
            input: PluginMemoryRetrieveInputDataRecord {
                query: String::from("current goal"),
                scopes: BTreeSet::from([PluginMemoryScopeData::Session]),
                max_items: 4,
                max_bytes: 4096,
                artifacts: Vec::new(),
                references: vec![PluginCanonicalReferenceDataRecord {
                    kind: PluginCanonicalReferenceKindData::NodeResult,
                    id: String::from("node-result-1"),
                    content_hash: Some(ContentHash::digest(b"node result")),
                }],
                parameters: json!({"ranking": "semantic"}),
            },
            readable_state: json!({"session": "session-1"}),
            cancellation_id: String::from("cancel-1"),
        };
        let retrieved = data
            .retrieve_memory(retrieve.clone())
            .await
            .expect("retrieve proposal");
        assert_eq!(retrieved.binding, retrieve.binding);
        {
            let dispatched = dependency
                .retrieve_requests
                .lock()
                .expect("retrieve requests");
            assert_eq!(dispatched.len(), 1);
            assert_eq!(dispatched[0].provider_id, retrieve.provider_id);
            assert_eq!(dispatched[0].input.query, retrieve.input.query);
            assert_eq!(dispatched[0].input.references.len(), 1);
        }

        let compaction_manifest = data
            .manifests
            .get("fixture.compaction")
            .expect("compaction manifest");
        let compactor = &compaction_manifest.compactors[0];
        let projection = json!([{"role": "user", "content": "bounded"}]);
        let compact = CompactPluginContextDataRequest {
            binding: operation_binding(compaction_manifest, compactor.declaration_hash),
            compactor_id: compactor.compactor_id.clone(),
            compactor_version: compactor.version.clone(),
            handler: compactor.handler.clone(),
            max_attempts: compactor.max_attempts,
            retry_backoff_ms: compactor.retry_backoff_ms,
            timeout_ms: compactor.timeout_ms,
            input: PluginCompactionInputDataRecord {
                projection_hash: ContentHash::digest(
                    &serde_json::to_vec(&projection).expect("projection"),
                ),
                projection,
                required_references: Vec::new(),
                required_artifacts: Vec::new(),
                preservation_requirements: BTreeSet::from([String::from("user_intent")]),
                max_replacement_bytes: 4096,
                max_projection_tokens: 256,
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: String::from("cancel-2"),
        };
        let proposal = data
            .compact_context(compact.clone())
            .await
            .expect("compaction proposal");
        assert_eq!(proposal.binding, compact.binding);
        assert_eq!(
            dependency
                .compaction_requests
                .lock()
                .expect("compaction requests")[0]
                .input
                .preservation_requirements,
            compact.input.preservation_requirements
        );
    }

    #[tokio::test]
    async fn approved_memory_write_maps_ambiguous_transport_fail_closed() {
        let (data, dependency) = runtime_memory_data(true);
        let manifest = data
            .manifests
            .get("fixture.memory")
            .expect("memory manifest");
        let provider = &manifest.memory_providers[0];
        let value = json!({"fact": "approved"});
        let request = WritePluginMemoryDataRequest {
            binding: operation_binding(manifest, provider.declaration_hash),
            provider_id: provider.provider_id.clone(),
            provider_version: provider.version.clone(),
            handler: provider
                .write
                .as_ref()
                .expect("write declaration")
                .handler
                .clone(),
            timeout_ms: provider
                .write
                .as_ref()
                .expect("write declaration")
                .timeout_ms,
            input: PluginMemoryWriteInputDataRecord {
                scope: PluginMemoryScopeData::Session,
                boundary: PluginMemoryWriteBoundaryData::IterationCompletion,
                value_hash: ContentHash::digest(&serde_json::to_vec(&value).expect("write value")),
                value,
                artifacts: Vec::new(),
                references: Vec::new(),
                security_classification: PluginSecurityClassificationData::Private,
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: String::from("cancel-write"),
        };
        assert!(matches!(
            data.write_memory(request.clone()).await,
            Err(PluginDataError::AmbiguousMemoryWrite {
                plugin_id,
                provider_id,
                invocation_id,
                idempotency_key,
            }) if plugin_id == "fixture.memory"
                && provider_id == "fixture.semantic"
                && invocation_id == request.binding.invocation_id
                && idempotency_key == request.binding.idempotency_key
        ));
        assert_eq!(
            dependency
                .write_requests
                .lock()
                .expect("write requests")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn memory_dispatch_rejects_declaration_substitution() {
        let (data, dependency) = runtime_memory_data(false);
        let manifest = data
            .manifests
            .get("fixture.memory")
            .expect("memory manifest");
        let provider = &manifest.memory_providers[0];
        let mut request = RetrievePluginMemoryDataRequest {
            binding: operation_binding(manifest, provider.declaration_hash),
            provider_id: provider.provider_id.clone(),
            provider_version: provider.version.clone(),
            handler: provider.retrieve.handler.clone(),
            max_attempts: provider.retrieve.max_attempts,
            retry_backoff_ms: provider.retrieve.retry_backoff_ms,
            timeout_ms: provider.retrieve.timeout_ms,
            input: PluginMemoryRetrieveInputDataRecord {
                query: String::from("query"),
                scopes: BTreeSet::from([PluginMemoryScopeData::Session]),
                max_items: 1,
                max_bytes: 1024,
                artifacts: Vec::new(),
                references: Vec::new(),
                parameters: json!({}),
            },
            readable_state: json!({}),
            cancellation_id: String::from("cancel"),
        };
        request.binding.declaration_hash = ContentHash::digest(b"substituted");
        assert_eq!(
            data.retrieve_memory(request).await,
            Err(PluginDataError::Invalid)
        );
        assert!(
            dependency
                .retrieve_requests
                .lock()
                .expect("retrieve requests")
                .is_empty()
        );
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
        assert!(
            !catalog.manifests[0]
                .canonical_manifest_json
                .contains("\"context_transforms\"")
        );
        assert_ne!(catalog.plugin_set_hash, ContentHash::digest(b""));
    }

    #[tokio::test]
    async fn lifecycle_change_maps_exactly_and_removes_session_activation() {
        let catalog = compile_plugin_catalog(
            &[source(observer("[]"))],
            "0.1.0",
            vec![String::from("events")],
        )
        .expect("catalog");
        let dependency = Arc::new(LifecycleDependency::default());
        let data = RuntimePluginData::new(dependency.clone(), catalog.manifests);
        let plugin_version = data
            .plugin_version("fixture.observer")
            .expect("plugin version");
        let configuration_reference = data
            .plugin_configuration_reference("fixture.observer")
            .expect("configuration reference");
        data.activated.lock().expect("activated").insert(
            String::from("session-1"),
            BTreeSet::from([String::from("fixture.observer")]),
        );
        let changed = data
            .change_plugin_lifecycle(ChangePluginLifecycleDataRequest {
                session_id: String::from("session-1"),
                plugin_id: String::from("fixture.observer"),
                plugin_version: plugin_version.clone(),
                configuration_reference,
                action: PluginLifecycleActionData::Quarantine,
                reason_code: Some(String::from("integrity_failure")),
                cancellation_id: String::from("lifecycle-cancellation-1"),
            })
            .await
            .expect("quarantine");
        assert_eq!(changed.plugin_id, "fixture.observer");
        assert_eq!(changed.state, "quarantined");
        assert_eq!(changed.audit_operation, "quarantine");
        assert_eq!(changed.audit_outcome, "integrity_failure");
        assert!(
            !data
                .activated
                .lock()
                .expect("activated")
                .get("session-1")
                .expect("session activation")
                .contains("fixture.observer")
        );
        assert_eq!(
            dependency.requests.lock().expect("requests").as_slice(),
            &[dependency::DependencyPluginLifecycleRequest {
                session_id: String::from("session-1"),
                plugin_id: String::from("fixture.observer"),
                plugin_version,
                configuration_reference,
                action: dependency::DependencyPluginLifecycleAction::Quarantine,
                reason_code: Some(String::from("integrity_failure")),
                cancellation_id: String::from("lifecycle-cancellation-1"),
            }]
        );
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
    fn catalog_normalizes_and_hashes_exact_context_transform_declarations() {
        let manifest_source = context_transform("redact_projection");
        let parsed = sdk::parse_toml(&manifest_source).expect("serialized context transform");
        sdk::validate_manifest(&parsed, &sdk::ValidationContext::new("0.1.0"))
            .expect("validated context transform");
        map_sdk_manifest(parsed).expect("normalized context transform");
        let first = compile_plugin_catalog(&[source(manifest_source)], "0.1.0", Vec::new())
            .expect("context-transform catalog");
        let manifest = &first.manifests[0];
        assert_eq!(manifest.category, "context_transform");
        assert_eq!(manifest.context_transforms.len(), 1);
        let declaration = &manifest.context_transforms[0];
        assert_eq!(declaration.transform_id, "fixture.redact");
        assert_eq!(declaration.version, "1.0.0");
        assert_eq!(declaration.lifecycle, "before_model_request");
        assert!(declaration.idempotent);
        assert!(!declaration.external_effects);
        assert_ne!(declaration.declaration_hash, ContentHash::digest(b""));
        assert!(
            manifest
                .canonical_manifest_json
                .contains("\"context_transforms\"")
        );

        let data = RuntimePluginData::new(
            Arc::new(StateDependency {
                result: Err(dependency::PluginDependencyError::StatePersistenceUnsupported),
                read_result: Err(dependency::PluginDependencyError::StateReadUnsupported),
                context_result: Err(dependency::PluginDependencyError::ContextTransformUnsupported),
                context_requests: Arc::new(Mutex::new(Vec::new())),
            }),
            first.manifests.clone(),
        );
        assert_eq!(
            data.context_transform_declaration("fixture.context", "fixture.redact", "1.0.0")
                .expect("exact declaration"),
            declaration.clone()
        );
        assert_eq!(
            data.context_transform_declaration("fixture.context", "fixture.redact", "2.0.0"),
            Err(PluginDataError::Invalid)
        );

        let changed = compile_plugin_catalog(
            &[source(context_transform("redact_projection_v2"))],
            "0.1.0",
            Vec::new(),
        )
        .expect("changed context-transform catalog");
        assert_ne!(first.plugin_set_hash, changed.plugin_set_hash);
        assert_ne!(
            declaration.declaration_hash,
            changed.manifests[0].context_transforms[0].declaration_hash
        );
    }

    #[test]
    fn catalog_normalizes_and_hashes_exact_memory_and_compactor_declarations() {
        let first = compile_plugin_catalog(
            &[
                source(memory_provider("retrieve_memory")),
                source(compactor("compact_projection")),
            ],
            "0.1.0",
            Vec::new(),
        )
        .expect("memory and compactor catalog");
        let memory = first
            .manifests
            .iter()
            .find(|manifest| manifest.id == "fixture.memory")
            .expect("memory plugin");
        assert_eq!(
            memory.provided_capabilities,
            BTreeSet::from([String::from("memory.fixture")])
        );
        let provider = &memory.memory_providers[0];
        assert_eq!(provider.provider_id, "fixture.semantic");
        assert_eq!(provider.version, "1.4.0");
        assert_eq!(provider.retrieve.failure_policy, "retry");
        assert_eq!(provider.retrieve.max_attempts, 2);
        assert!(provider.retrieve.idempotent);
        assert!(provider.write.is_some());
        assert_ne!(provider.declaration_hash, ContentHash::digest(b""));
        assert!(
            memory
                .canonical_manifest_json
                .contains("\"memory_providers\"")
        );

        let compaction = first
            .manifests
            .iter()
            .find(|manifest| manifest.id == "fixture.compaction")
            .expect("compaction plugin");
        let declaration = &compaction.compactors[0];
        assert_eq!(declaration.compactor_id, "fixture.summary");
        assert_eq!(declaration.version, "2.0.0");
        assert!(declaration.idempotent);
        assert_ne!(declaration.declaration_hash, ContentHash::digest(b""));
        assert!(
            compaction
                .canonical_manifest_json
                .contains("\"compactors\"")
        );

        let changed = compile_plugin_catalog(
            &[
                source(memory_provider("retrieve_memory_v2")),
                source(compactor("compact_projection")),
            ],
            "0.1.0",
            Vec::new(),
        )
        .expect("changed memory catalog");
        let changed_memory = changed
            .manifests
            .iter()
            .find(|manifest| manifest.id == "fixture.memory")
            .expect("changed memory plugin");
        assert_ne!(first.plugin_set_hash, changed.plugin_set_hash);
        assert_ne!(
            provider.declaration_hash,
            changed_memory.memory_providers[0].declaration_hash
        );
    }

    #[tokio::test]
    async fn context_transform_dispatches_only_the_exact_catalog_declaration() {
        let expected_replacement = json!([{"role": "user", "content": "[redacted]"}]);
        let (data, declaration, requests) =
            runtime_context_data(Ok(dependency::DependencyPluginContextTransformProposal {
                replacement: expected_replacement.clone(),
                attempts: 1,
            }));
        let request = context_request(&declaration);
        let result = data
            .invoke_context_transform(request.clone())
            .await
            .expect("exact transform proposal");

        assert_eq!(result.replacement, expected_replacement);
        assert_eq!(result.attempts, 1);
        let dispatched = requests.lock().expect("context request projection");
        assert_eq!(dispatched.len(), 1);
        assert_eq!(dispatched[0].plugin_id, request.plugin_id);
        assert_eq!(dispatched[0].transform_id, request.transform_id);
        assert_eq!(dispatched[0].transform_version, request.transform_version);
        assert_eq!(dispatched[0].handler, request.handler);
    }

    #[tokio::test]
    async fn context_transform_rejects_declaration_substitution_before_dispatch() {
        let (data, declaration, requests) =
            runtime_context_data(Ok(dependency::DependencyPluginContextTransformProposal {
                replacement: json!([]),
                attempts: 1,
            }));
        let mut request = context_request(&declaration);
        request.declaration_hash = ContentHash::digest(b"substituted declaration");

        assert_eq!(
            data.invoke_context_transform(request).await,
            Err(PluginDataError::Invalid)
        );
        assert!(
            requests
                .lock()
                .expect("context request projection")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn context_transform_maps_uncertain_transport_to_ambiguous_state() {
        for dependency_error in [
            dependency::PluginDependencyError::Timeout,
            dependency::PluginDependencyError::Unavailable,
            dependency::PluginDependencyError::InvalidResponse,
            dependency::PluginDependencyError::AmbiguousContextTransform,
        ] {
            let (data, declaration, _) = runtime_context_data(Err(dependency_error));
            assert!(matches!(
                data.invoke_context_transform(context_request(&declaration))
                    .await,
                Err(PluginDataError::AmbiguousContextTransform {
                    plugin_id,
                    transform_id,
                    invocation_id,
                }) if plugin_id == "fixture.context"
                    && transform_id == "fixture.redact"
                    && invocation_id == "session-1:run-1:model-1"
            ));
        }
    }

    #[tokio::test]
    async fn state_persistence_maps_exact_receipt_and_idempotent_replay() {
        let request = state_request();
        let mut terminal = state_receipt(&request);
        terminal.replayed = true;
        let data = runtime_state_data(Ok(terminal));
        let first = data
            .persist_plugin_node_state(request.clone())
            .await
            .expect("terminal receipt");
        let second = data
            .persist_plugin_node_state(request)
            .await
            .expect("same terminal receipt");
        assert_eq!(first, second);
        assert!(first.replayed);
        assert_eq!(first.generation, 2);
    }

    #[tokio::test]
    async fn state_read_maps_exact_value_and_receipt_and_rejects_substitution() {
        let request = state_read_request();
        let exact = state_read_result(&request);
        let data = runtime_state_data_with_read(
            Err(dependency::PluginDependencyError::StatePersistenceUnsupported),
            Ok(exact.clone()),
        );
        let loaded = data
            .load_plugin_node_state(request.clone())
            .await
            .expect("exact loaded state");
        assert_eq!(loaded.state, json!({"cursor": 2}));
        assert_eq!(loaded.receipt.generation, 2);

        let mut substituted = exact;
        substituted.state = json!({"cursor": 3});
        assert_eq!(
            runtime_state_data_with_read(
                Err(dependency::PluginDependencyError::StatePersistenceUnsupported),
                Ok(substituted),
            )
            .load_plugin_node_state(request)
            .await,
            Err(PluginDataError::Invalid)
        );
        for dependency_error in [
            dependency::PluginDependencyError::StateReadUnsupported,
            dependency::PluginDependencyError::StaleStateGeneration,
            dependency::PluginDependencyError::StateConflict,
            dependency::PluginDependencyError::Cancelled,
        ] {
            let expected = match dependency_error {
                dependency::PluginDependencyError::StateReadUnsupported => {
                    PluginDataError::StateReadUnsupported
                }
                dependency::PluginDependencyError::StaleStateGeneration => {
                    PluginDataError::StaleStateGeneration
                }
                dependency::PluginDependencyError::StateConflict => PluginDataError::StateConflict,
                dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
                _ => unreachable!("bounded fixture error"),
            };
            assert_eq!(
                runtime_state_data_with_read(
                    Err(dependency::PluginDependencyError::StatePersistenceUnsupported),
                    Err(dependency_error),
                )
                .load_plugin_node_state(state_read_request())
                .await,
                Err(expected)
            );
        }
        assert!(matches!(
            runtime_state_data_with_read(
                Err(dependency::PluginDependencyError::StatePersistenceUnsupported),
                Err(dependency::PluginDependencyError::AmbiguousStateRead),
            )
            .load_plugin_node_state(state_read_request())
            .await,
            Err(PluginDataError::AmbiguousStateRead { .. })
        ));
    }

    #[tokio::test]
    async fn state_persistence_rejects_receipt_substitution() {
        let request = state_request();
        let mut terminal = state_receipt(&request);
        terminal.plugin_id = String::from("substituted.plugin");
        terminal.receipt_digest =
            dependency::plugin_node_state_receipt_digest(&terminal).expect("receipt digest");
        assert_eq!(
            runtime_state_data(Ok(terminal))
                .persist_plugin_node_state(request)
                .await,
            Err(PluginDataError::Invalid)
        );
    }

    #[tokio::test]
    async fn state_persistence_rejects_every_scope_without_a_canonical_identity() {
        for scope in [
            PluginNodeStateScopeData::ModelCall,
            PluginNodeStateScopeData::Turn,
            PluginNodeStateScopeData::Project,
            PluginNodeStateScopeData::User,
            PluginNodeStateScopeData::Runtime,
        ] {
            let mut request = state_request();
            request.state_scope = scope;
            assert_eq!(
                runtime_state_data(Err(dependency::PluginDependencyError::InvalidRequest))
                    .persist_plugin_node_state(request)
                    .await,
                Err(PluginDataError::UnsupportedStateScope),
                "scope {scope:?} must fail before dependency dispatch"
            );
        }
    }

    #[tokio::test]
    async fn state_persistence_maps_stale_cancel_timeout_and_ambiguous_fail_closed() {
        let cases = [
            (
                dependency::PluginDependencyError::StaleStateGeneration,
                PluginDataError::StaleStateGeneration,
            ),
            (
                dependency::PluginDependencyError::Cancelled,
                PluginDataError::Cancelled,
            ),
        ];
        for (dependency_error, expected) in cases {
            assert_eq!(
                runtime_state_data(Err(dependency_error))
                    .persist_plugin_node_state(state_request())
                    .await,
                Err(expected)
            );
        }
        for dependency_error in [
            dependency::PluginDependencyError::Timeout,
            dependency::PluginDependencyError::AmbiguousStatePersistence,
        ] {
            assert!(matches!(
                runtime_state_data(Err(dependency_error))
                    .persist_plugin_node_state(state_request())
                    .await,
                Err(PluginDataError::AmbiguousStatePersistence {
                    plugin_id,
                    invocation_id,
                    idempotency_key,
                }) if plugin_id == "fixture.plugin"
                    && invocation_id == "plugin-node:invocation"
                    && idempotency_key == "state-write-1"
            ));
        }
    }
}
