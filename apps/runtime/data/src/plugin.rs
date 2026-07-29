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
    pub class: String,
    pub subscribed_events: BTreeSet<String>,
    pub timeout_ms: u64,
    pub failure_policy: String,
    pub canonical_manifest_json: String,
    pub configuration: Value,
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
    let wire_manifest = serde_json::json!({
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
    Ok(PluginManifestDataRecord {
        id,
        version,
        class,
        subscribed_events,
        timeout_ms: manifest.timeout_ms,
        failure_policy,
        canonical_manifest_json: serde_json::to_string(&wire_manifest)
            .map_err(|_| PluginDataError::Invalid)?,
        configuration: Value::Object(serde_json::Map::new()),
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
    pub cancellation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginObservationDataRecord {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
}

#[async_trait]
pub trait PluginDataPort: Send + Sync {
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
}

#[async_trait]
impl PluginDataPort for RuntimePluginData {
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
}
