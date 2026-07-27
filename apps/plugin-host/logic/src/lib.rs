//! Plugin activation, invocation, observation, and state policy.
#![allow(
    missing_docs,
    reason = "layer-local mapping records have self-describing fields"
)]

use agentmod_plugin_host_data as data;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PluginClass {
    Blocking,
    Observer,
    Tool,
    Extension,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestCommand {
    pub schema_version: u16,
    pub id: String,
    pub version: String,
    pub runtime_api: String,
    pub category: String,
    pub scope: String,
    pub class: PluginClass,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authorization {
    pub owner_id: String,
    pub session_id: String,
    pub call_id: String,
    pub normalized_digest: String,
    pub grant: String,
    pub cancellation_id: String,
}
#[derive(Clone, Debug)]
pub struct InvocationCommand {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub kind: String,
    pub payload: Value,
    pub readable_state: Value,
    pub authorization: Authorization,
}
#[derive(Clone, Debug)]
pub struct ObservationCommand {
    pub plugin_id: String,
    pub invocation_id: String,
    pub handler: String,
    pub event_type: String,
    pub event: Value,
    pub authorization: Authorization,
}
#[derive(Clone, Debug)]
pub struct StateChangeCommand {
    pub plugin_id: String,
    pub reason: Option<String>,
    pub authorization: Authorization,
}
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    Continue(Value),
    Replace(Value),
    Reject(String),
    ToolResult(Value),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Audit {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadResult {
    pub plugin_id: String,
    pub state_version: u32,
    pub audit: Audit,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationResult {
    pub accepted: bool,
    pub queue_depth: usize,
    pub dropped: u64,
    pub audit: Audit,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Health {
    pub loaded: usize,
    pub running: usize,
    pub observer_dropped: u64,
}

#[async_trait]
pub trait PluginLogicPort: Send + Sync {
    async fn negotiate(
        &self,
        p: u16,
        a: String,
        c: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginLogicError>;
    async fn validate_set(&self, m: Vec<ManifestCommand>) -> Result<Vec<String>, PluginLogicError>;
    async fn load(
        &self,
        m: ManifestCommand,
        c: Value,
        a: Authorization,
    ) -> Result<LoadResult, PluginLogicError>;
    async fn invoke(&self, r: InvocationCommand) -> Result<(Decision, Audit), PluginLogicError>;
    async fn observe(&self, r: ObservationCommand) -> Result<ObservationResult, PluginLogicError>;
    async fn cancel(&self, i: String) -> Result<(), PluginLogicError>;
    async fn state_change(&self, r: StateChangeCommand, q: bool)
    -> Result<Audit, PluginLogicError>;
    async fn health(&self) -> Health;
}
#[derive(Clone)]
pub struct PluginLogic<D> {
    data: D,
}
impl<D> PluginLogic<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}
#[async_trait]
impl<D: data::PluginDataPort> PluginLogicPort for PluginLogic<D> {
    async fn negotiate(
        &self,
        p: u16,
        a: String,
        c: BTreeSet<String>,
    ) -> Result<(u16, String, BTreeSet<String>), PluginLogicError> {
        self.data.negotiate(p, a, c).await.map_err(map_error)
    }
    async fn validate_set(&self, m: Vec<ManifestCommand>) -> Result<Vec<String>, PluginLogicError> {
        if m.is_empty() {
            return Err(PluginLogicError::Invalid);
        }
        self.data
            .validate_set(m.into_iter().map(map_manifest).collect())
            .await
            .map_err(map_error)
    }
    async fn load(
        &self,
        m: ManifestCommand,
        c: Value,
        a: Authorization,
    ) -> Result<LoadResult, PluginLogicError> {
        validate_id(&m.id)?;
        validate_auth(&a)?;
        let v = self
            .data
            .load(map_manifest(m), c, map_auth(a))
            .await
            .map_err(map_error)?;
        let audit = Audit {
            plugin_id: v.plugin_id.clone(),
            invocation_id: None,
            operation: "load".into(),
            outcome: "loaded".into(),
            attempts: v.attempts,
        };
        Ok(LoadResult {
            plugin_id: v.plugin_id,
            state_version: v.state_version,
            audit,
        })
    }
    async fn invoke(&self, r: InvocationCommand) -> Result<(Decision, Audit), PluginLogicError> {
        validate_id(&r.plugin_id)?;
        validate_auth(&r.authorization)?;
        let invocation = r.invocation_id.clone();
        let plugin = r.plugin_id.clone();
        let (v, attempts) = self
            .data
            .invoke(data::InvocationData {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                kind: r.kind,
                payload: r.payload,
                readable_state: r.readable_state,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        let decision = match v {
            data::DecisionData::Continue(v) => Decision::Continue(v),
            data::DecisionData::Replace(v) => Decision::Replace(v),
            data::DecisionData::Reject(v) => Decision::Reject(v),
            data::DecisionData::ToolResult(v) => Decision::ToolResult(v),
        };
        Ok((
            decision,
            Audit {
                plugin_id: plugin,
                invocation_id: Some(invocation),
                operation: "invoke".into(),
                outcome: "completed".into(),
                attempts,
            },
        ))
    }
    async fn observe(&self, r: ObservationCommand) -> Result<ObservationResult, PluginLogicError> {
        validate_id(&r.plugin_id)?;
        validate_auth(&r.authorization)?;
        let plugin = r.plugin_id.clone();
        let invocation = r.invocation_id.clone();
        let v = self
            .data
            .observe(data::ObservationData {
                plugin_id: r.plugin_id,
                invocation_id: r.invocation_id,
                handler: r.handler,
                event_type: r.event_type,
                event: r.event,
                authorization: map_auth(r.authorization),
            })
            .await
            .map_err(map_error)?;
        Ok(ObservationResult {
            accepted: v.accepted,
            queue_depth: v.queue_depth,
            dropped: v.dropped,
            audit: Audit {
                plugin_id: plugin,
                invocation_id: Some(invocation),
                operation: "observe".into(),
                outcome: if v.accepted { "accepted" } else { "dropped" }.into(),
                attempts: 1,
            },
        })
    }
    async fn cancel(&self, i: String) -> Result<(), PluginLogicError> {
        if i.is_empty() {
            return Err(PluginLogicError::Invalid);
        }
        self.data.cancel(i).await.map_err(map_error)
    }
    async fn state_change(
        &self,
        r: StateChangeCommand,
        q: bool,
    ) -> Result<Audit, PluginLogicError> {
        validate_id(&r.plugin_id)?;
        validate_auth(&r.authorization)?;
        self.data
            .state_change(
                data::StateChangeData {
                    plugin_id: r.plugin_id,
                    reason: r.reason,
                    authorization: map_auth(r.authorization),
                },
                q,
            )
            .await
            .map(map_audit)
            .map_err(map_error)
    }
    async fn health(&self) -> Health {
        let v = self.data.health().await;
        Health {
            loaded: v.loaded,
            running: v.running,
            observer_dropped: v.observer_dropped,
        }
    }
}
fn validate_id(v: &str) -> Result<(), PluginLogicError> {
    if v.is_empty()
        || v.len() > 128
        || !v
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
    {
        Err(PluginLogicError::Invalid)
    } else {
        Ok(())
    }
}
fn validate_auth(v: &Authorization) -> Result<(), PluginLogicError> {
    if v.owner_id.is_empty()
        || v.session_id.is_empty()
        || v.call_id.is_empty()
        || v.normalized_digest.len() != 64
        || v.grant.is_empty()
        || v.cancellation_id.is_empty()
    {
        Err(PluginLogicError::Invalid)
    } else {
        Ok(())
    }
}
fn map_manifest(v: ManifestCommand) -> data::ManifestData {
    data::ManifestData {
        schema_version: v.schema_version,
        id: v.id,
        version: v.version,
        runtime_api: v.runtime_api,
        category: v.category,
        scope: v.scope,
        class: match v.class {
            PluginClass::Blocking => data::PluginClassData::Blocking,
            PluginClass::Observer => data::PluginClassData::Observer,
            PluginClass::Tool => data::PluginClassData::Tool,
            PluginClass::Extension => data::PluginClassData::Extension,
        },
        program: v.program,
        arguments: v.arguments,
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
        schema_id: v.schema_id,
        schema_version_number: v.schema_version_number,
        schema_required: v.schema_required,
        schema_json: v.schema_json,
    }
}
fn map_auth(v: Authorization) -> data::AuthorizationData {
    data::AuthorizationData {
        owner_id: v.owner_id,
        session_id: v.session_id,
        call_id: v.call_id,
        normalized_digest: v.normalized_digest,
        grant: v.grant,
        cancellation_id: v.cancellation_id,
    }
}
fn map_audit(v: data::AuditData) -> Audit {
    Audit {
        plugin_id: v.plugin_id,
        invocation_id: v.invocation_id,
        operation: v.operation,
        outcome: v.outcome,
        attempts: v.attempts,
    }
}
#[allow(clippy::needless_pass_by_value)]
fn map_error(v: data::PluginDataError) -> PluginLogicError {
    match v {
        data::PluginDataError::Invalid => PluginLogicError::Invalid,
        data::PluginDataError::Authorization => PluginLogicError::Authorization,
        data::PluginDataError::Unavailable => PluginLogicError::Unavailable,
        data::PluginDataError::Cancelled => PluginLogicError::Cancelled,
        data::PluginDataError::External => PluginLogicError::Operation,
    }
}
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PluginLogicError {
    #[error("invalid plugin command")]
    Invalid,
    #[error("plugin authorization denied")]
    Authorization,
    #[error("plugin unavailable")]
    Unavailable,
    #[error("plugin cancelled")]
    Cancelled,
    #[error("plugin operation failed")]
    Operation,
}
