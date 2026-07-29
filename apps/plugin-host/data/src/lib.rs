//! Plugin datasets and dependency normalization.
#![allow(
    missing_docs,
    reason = "layer-local mapping records mirror individually documented protocol and dependency fields"
)]

use std::collections::BTreeSet;

use agentmod_plugin_host_dependency as dependency;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Data-owned plugin class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginClassData {
    Blocking,
    Observer,
    Tool,
    Extension,
}

/// Data-owned manifest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestData {
    pub schema_version: u16,
    pub id: String,
    pub version: String,
    pub runtime_api: String,
    pub category: String,
    pub scope: String,
    pub class: PluginClassData,
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
}

/// Data-owned authorization.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationData {
    pub owner_id: String,
    pub session_id: String,
    pub call_id: String,
    pub normalized_digest: String,
    pub grant: String,
    pub cancellation_id: String,
}

/// Invocation.
#[derive(Clone, Debug)]
pub struct InvocationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub operation: String,
    pub kind: String,
    pub payload: Value,
    pub readable_state: Value,
    pub authorization: AuthorizationData,
}
/// Observation.
#[derive(Clone, Debug)]
pub struct ObservationData {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub authorization: AuthorizationData,
}
/// State change.
#[derive(Clone, Debug)]
pub struct StateChangeData {
    pub plugin_id: String,
    pub reason: Option<String>,
    pub authorization: AuthorizationData,
}
/// Decision.
#[derive(Clone, Debug, PartialEq)]
pub enum DecisionData {
    Continue(Value),
    Replace(Value),
    Reject(String),
    ToolResult(Value),
}
/// Audit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditData {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}
/// Load result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadData {
    pub plugin_id: String,
    pub state_version: u32,
    pub attempts: u8,
}
/// Observation result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationResultData {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
}
/// Health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthData {
    pub loaded: usize,
    pub running: usize,
    pub observer_dropped: u64,
}

/// Data contract.
#[async_trait]
pub trait PluginDataPort: Send + Sync {
    async fn negotiate(
        &self,
        protocol: u16,
        api: String,
        capabilities: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDataError>;
    async fn validate_set(
        &self,
        manifests: Vec<ManifestData>,
    ) -> Result<Vec<String>, PluginDataError>;
    async fn load(
        &self,
        manifest: ManifestData,
        configuration: Value,
        authorization: AuthorizationData,
    ) -> Result<LoadData, PluginDataError>;
    async fn invoke(&self, request: InvocationData) -> Result<(DecisionData, u8), PluginDataError>;
    async fn observe(
        &self,
        request: ObservationData,
    ) -> Result<ObservationResultData, PluginDataError>;
    async fn cancel(&self, invocation_id: String) -> Result<(), PluginDataError>;
    async fn state_change(
        &self,
        request: StateChangeData,
        quarantine: bool,
    ) -> Result<AuditData, PluginDataError>;
    async fn health(&self) -> HealthData;
}

/// Data implementation.
#[derive(Clone)]
pub struct PluginData<D> {
    dependency: D,
}
impl<D> PluginData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: dependency::PluginDependencyPort> PluginDataPort for PluginData<D> {
    async fn negotiate(
        &self,
        p: u16,
        a: String,
        c: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginDataError> {
        self.dependency.negotiate(p, a, c).await.map_err(map_error)
    }
    async fn validate_set(&self, m: Vec<ManifestData>) -> Result<Vec<String>, PluginDataError> {
        self.dependency
            .validate_set(m.into_iter().map(map_manifest).collect())
            .await
            .map_err(map_error)
    }
    async fn load(
        &self,
        m: ManifestData,
        c: Value,
        a: AuthorizationData,
    ) -> Result<LoadData, PluginDataError> {
        let value = self
            .dependency
            .load(dependency::DependencyLoadRequest {
                manifest: map_manifest(m),
                configuration: c,
                authorization: map_auth(a),
            })
            .await
            .map_err(map_error)?;
        Ok(LoadData {
            plugin_id: value.plugin_id,
            state_version: value.state_version,
            attempts: value.attempts,
        })
    }
    async fn invoke(&self, r: InvocationData) -> Result<(DecisionData, u8), PluginDataError> {
        let (d, a) = self
            .dependency
            .invoke(dependency::DependencyInvocationRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                operation: r.operation,
                kind: r.kind,
                payload: r.payload,
                readable_state: r.readable_state,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok((
            match d {
                dependency::DependencyDecision::Continue(v) => DecisionData::Continue(v),
                dependency::DependencyDecision::Replace(v) => DecisionData::Replace(v),
                dependency::DependencyDecision::Reject(v) => DecisionData::Reject(v),
                dependency::DependencyDecision::ToolResult(v) => DecisionData::ToolResult(v),
            },
            a,
        ))
    }
    async fn observe(&self, r: ObservationData) -> Result<ObservationResultData, PluginDataError> {
        let v = self
            .dependency
            .observe(dependency::DependencyObservationRequest {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                event_type: r.event_type,
                event: r.event,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(ObservationResultData {
            accepted: v.accepted,
            queue_depth: v.queue_depth,
            dropped: v.dropped,
        })
    }
    async fn cancel(&self, i: String) -> Result<(), PluginDataError> {
        self.dependency.cancel(i).await.map_err(map_error)
    }
    async fn state_change(
        &self,
        r: StateChangeData,
        q: bool,
    ) -> Result<AuditData, PluginDataError> {
        let request = dependency::DependencyStateChangeRequest {
            plugin_id: r.plugin_id,
            reason: r.reason,
            authorization: map_auth(r.authorization),
        };
        let v = if q {
            self.dependency.quarantine(request).await
        } else {
            self.dependency.disable(request).await
        }
        .map_err(map_error)?;
        Ok(map_audit(v))
    }
    async fn health(&self) -> HealthData {
        let v = self.dependency.health().await;
        HealthData {
            loaded: v.loaded,
            running: v.running,
            observer_dropped: v.observer_dropped,
        }
    }
}

fn map_manifest(v: ManifestData) -> dependency::DependencyManifest {
    dependency::DependencyManifest {
        schema_version: v.schema_version,
        id: v.id,
        version: v.version,
        runtime_api: v.runtime_api,
        category: v.category,
        scope: v.scope,
        class: match v.class {
            PluginClassData::Blocking => dependency::DependencyPluginClass::Blocking,
            PluginClassData::Observer => dependency::DependencyPluginClass::Observer,
            PluginClassData::Tool => dependency::DependencyPluginClass::Tool,
            PluginClassData::Extension => dependency::DependencyPluginClass::Extension,
        },
        entrypoint: dependency::DependencyEntrypoint {
            program: v.program,
            arguments: v.arguments,
        },
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
        configuration_schema: dependency::DependencyConfigurationSchema {
            id: v.schema_id,
            version: v.schema_version_number,
            required: v.schema_required,
            inline_json: v.schema_json,
        },
    }
}
fn map_auth(v: AuthorizationData) -> dependency::DependencyAuthorization {
    dependency::DependencyAuthorization {
        owner_id: v.owner_id,
        session_id: v.session_id,
        call_id: v.call_id,
        normalized_digest: v.normalized_digest,
        grant: v.grant,
        cancellation_id: v.cancellation_id,
    }
}
fn map_audit(v: dependency::DependencyAudit) -> AuditData {
    AuditData {
        plugin_id: v.plugin_id,
        invocation_id: v.invocation_id,
        operation: v.operation,
        outcome: v.outcome,
        attempts: v.attempts,
    }
}
#[allow(clippy::needless_pass_by_value)]
fn map_error(v: dependency::PluginDependencyError) -> PluginDataError {
    match v {
        dependency::PluginDependencyError::Invalid
        | dependency::PluginDependencyError::Validation(_)
        | dependency::PluginDependencyError::Configuration => PluginDataError::Invalid,
        dependency::PluginDependencyError::Authorization
        | dependency::PluginDependencyError::Replay => PluginDataError::Authorization,
        dependency::PluginDependencyError::NotLoaded
        | dependency::PluginDependencyError::Inactive => PluginDataError::Unavailable,
        dependency::PluginDependencyError::Cancelled => PluginDataError::Cancelled,
        _ => PluginDataError::External,
    }
}
/// Data error.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginDataError {
    #[error("invalid plugin data")]
    Invalid,
    #[error("plugin authorization denied")]
    Authorization,
    #[error("plugin unavailable")]
    Unavailable,
    #[error("plugin cancelled")]
    Cancelled,
    #[error("plugin dependency failed")]
    External,
}
