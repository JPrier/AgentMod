//! Session creation and dormant-catalog business use cases.

use std::{fmt, path::PathBuf};

use agentmod_event_model::{
    EventClassification, EventEnvelope, EventMetadata, EventOrigin, EventScope,
};
use agentmod_primitives::{ContentHash, Sequence, SessionId, Version};
use agentmod_runtime_data::node_executor::NodeExecutorDataPort;
use agentmod_runtime_data::registry::{
    CreateSessionDataRequest, ListSessionsDataRequest, PrepareSessionDataRequest,
    PreparedSessionDataRecord, SessionRegistryDataError, SessionRegistryDataPort,
};
use thiserror::Error;
use url::Url;

use crate::{
    node_executor::{RuntimeExecutabilityError, bind_runtime_execution_plan},
    session::{
        RuntimeCommittedEvent, SessionCreatedEvent, SessionMcpBinding, SessionMcpSecretBinding,
        SessionMcpServerBinding, SessionMcpTransportBinding, SessionStyleBinding,
    },
};

const MAX_SESSION_LIST: usize = 1_000;
const MAX_STYLE_LENGTH: usize = 128;
const MAX_MCP_SERVERS: usize = 32;
const MAX_MCP_ARGUMENTS: usize = 128;
const MAX_MCP_KEY_VALUES: usize = 64;
const MAX_MCP_FIELD_BYTES: usize = 8 * 1024;

/// Logic-owned per-session MCP declaration.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct SessionMcpServerDeclaration {
    /// ACP display name.
    pub name: String,
    /// Exact transport declaration.
    pub transport: SessionMcpTransportDeclaration,
}

/// Logic-owned MCP transport declaration.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum SessionMcpTransportDeclaration {
    /// Stdio child server.
    Stdio {
        /// Absolute executable.
        program: String,
        /// Exact argument vector.
        arguments: Vec<String>,
        /// Ordered environment entries.
        environment: Vec<SessionMcpSensitiveEntry>,
    },
    /// Streamable HTTP or legacy SSE endpoint.
    StreamableHttp {
        /// Secure or loopback URL.
        url: String,
        /// Whether ACP declared the legacy SSE transport.
        legacy_sse: bool,
        /// Ordered headers.
        headers: Vec<SessionMcpSensitiveEntry>,
    },
}

/// Sensitive MCP environment/header entry with redacted diagnostics.
#[derive(Clone, Eq, PartialEq, serde::Serialize)]
pub struct SessionMcpSensitiveEntry {
    /// Field name.
    pub name: String,
    /// Exact transient value.
    pub value: String,
}

impl fmt::Debug for SessionMcpSensitiveEntry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionMcpSensitiveEntry")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Exact transient serialized MCP configuration passed only to data.
#[derive(Clone, Eq, PartialEq)]
pub struct SensitiveSessionMcpConfiguration(pub String);

impl fmt::Debug for SensitiveSessionMcpConfiguration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitiveSessionMcpConfiguration(<redacted>)")
    }
}

/// Logic-owned create command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionCommand {
    /// Sessions root selected by bootstrap configuration.
    pub sessions_root: PathBuf,
    /// User-selected workspace path.
    pub workspace: PathBuf,
    /// Immutable selected and compiled style.
    pub style_binding: SessionStyleBinding,
    /// Optional exact per-session MCP declarations.
    pub mcp_servers: Vec<SessionMcpServerDeclaration>,
}

/// Logic-owned create result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateSessionResult {
    /// New session identifier.
    pub session_id: SessionId,
    /// Durable directory.
    pub session_directory: PathBuf,
}

/// Logic-owned list command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListSessionsCommand {
    /// Sessions root.
    pub sessions_root: PathBuf,
    /// Caller-requested maximum.
    pub limit: usize,
}

/// Logic-owned summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionSummaryResult {
    /// Session ID.
    pub id: SessionId,
    /// Workspace display label.
    pub workspace_label: String,
    /// Explicit style.
    pub style: String,
    /// Last known sequence.
    pub sequence: Sequence,
    /// Lifecycle label.
    pub state: String,
}

/// Narrow session registry use-case interface.
pub trait SessionRegistryLogicPort {
    /// Creates a complete initial durable session.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryLogicError`] for invalid business input or
    /// failed durable creation.
    fn create_session(
        &self,
        command: CreateSessionCommand,
    ) -> Result<CreateSessionResult, SessionRegistryLogicError>;

    /// Lists dormant metadata without loading conversations.
    ///
    /// # Errors
    ///
    /// Returns [`SessionRegistryLogicError`] for invalid configuration or data.
    fn list_sessions(
        &self,
        command: ListSessionsCommand,
    ) -> Result<Vec<SessionSummaryResult>, SessionRegistryLogicError>;
}

impl<D> SessionRegistryLogicPort for super::RuntimeLogic<D>
where
    D: SessionRegistryDataPort + NodeExecutorDataPort,
{
    fn create_session(
        &self,
        mut command: CreateSessionCommand,
    ) -> Result<CreateSessionResult, SessionRegistryLogicError> {
        validate_create(&command)?;
        bind_runtime_execution_plan(&self.data, &mut command.style_binding)
            .map_err(SessionRegistryLogicError::RuntimeExecutability)?;
        let prepared = self
            .data
            .prepare(PrepareSessionDataRequest {
                workspace: command.workspace,
            })
            .map_err(SessionRegistryLogicError::Data)?;
        let mcp_configuration = bind_session_mcp(
            prepared.session_id,
            &mut command.style_binding,
            &command.mcp_servers,
        )?;
        let event = initial_event(&prepared, &command.style_binding)?;
        let event_json = serde_json::to_vec(&event)
            .map_err(|_| SessionRegistryLogicError::InitialEventSerialization)?;
        let style_binding_json = serde_json::to_string(&command.style_binding)
            .map_err(|_| SessionRegistryLogicError::InitialEventSerialization)?;
        let execution_plan = crate::execution_plan::to_plan_file_data(&command.style_binding)
            .map_err(SessionRegistryLogicError::ExecutionPlan)?;
        let session_id = prepared.session_id;
        let created = self
            .data
            .create(CreateSessionDataRequest {
                sessions_root: command.sessions_root,
                prepared,
                style: command.style_binding.id.clone(),
                style_binding_json,
                style_manifest_json: command.style_binding.configuration_json,
                compiled_style_json: command.style_binding.compiled_style_json,
                initial_event_json: event_json,
                mcp_configuration: mcp_configuration.map(|value| value.0.into()),
                execution_plan,
            })
            .map_err(SessionRegistryLogicError::Data)?;
        Ok(CreateSessionResult {
            session_id,
            session_directory: created.session_directory,
        })
    }

    fn list_sessions(
        &self,
        command: ListSessionsCommand,
    ) -> Result<Vec<SessionSummaryResult>, SessionRegistryLogicError> {
        if command.sessions_root.as_os_str().is_empty() {
            return Err(SessionRegistryLogicError::InvalidSessionsRoot);
        }
        let limit = command.limit.min(MAX_SESSION_LIST);
        self.data
            .list(ListSessionsDataRequest {
                sessions_root: command.sessions_root,
                limit,
            })
            .map_err(SessionRegistryLogicError::Data)?
            .into_iter()
            .map(|record| {
                Ok(SessionSummaryResult {
                    id: record.id,
                    workspace_label: record.workspace,
                    style: record.style,
                    sequence: Sequence::new(record.sequence)
                        .map_err(|_| SessionRegistryLogicError::InvalidSequence)?,
                    state: record.state,
                })
            })
            .collect()
    }
}

fn bind_session_mcp(
    session_id: SessionId,
    binding: &mut SessionStyleBinding,
    servers: &[SessionMcpServerDeclaration],
) -> Result<Option<SensitiveSessionMcpConfiguration>, SessionRegistryLogicError> {
    validate_mcp_servers(servers)?;
    if servers.is_empty() {
        binding.mcp = SessionMcpBinding::default();
        return Ok(None);
    }
    let canonical = serde_json::to_vec(servers)
        .map_err(|_| SessionRegistryLogicError::InvalidMcpConfiguration)?;
    let declaration_hash = ContentHash::digest(&canonical);
    let mut sanitized = Vec::with_capacity(servers.len());
    let mut bootstrap = Vec::with_capacity(servers.len());
    for (server_index, server) in servers.iter().enumerate() {
        let id = stable_mcp_server_id(session_id, server_index, &server.name);
        let (transport, bootstrap_transport) = match &server.transport {
            SessionMcpTransportDeclaration::Stdio {
                program,
                arguments,
                environment,
            } => {
                let secrets = secret_bindings(session_id, &id, "env", environment);
                (
                    SessionMcpTransportBinding::Stdio {
                        program: program.clone(),
                        arguments: arguments.clone(),
                        environment: secrets,
                    },
                    serde_json::json!({
                        "transport": "stdio",
                        "program": program,
                        "arguments": arguments,
                        "environment": environment.iter().map(|entry| {
                            (entry.name.clone(), entry.value.clone())
                        }).collect::<std::collections::BTreeMap<_, _>>(),
                    }),
                )
            }
            SessionMcpTransportDeclaration::StreamableHttp {
                url,
                legacy_sse,
                headers,
            } => {
                let secrets = secret_bindings(session_id, &id, "header", headers);
                (
                    SessionMcpTransportBinding::StreamableHttp {
                        url: url.clone(),
                        legacy_sse: *legacy_sse,
                        headers: secrets,
                    },
                    serde_json::json!({
                        "transport": if *legacy_sse {
                            "legacy_sse"
                        } else {
                            "streamable_http"
                        },
                        "url": url,
                        "headers": headers.iter().map(|entry| {
                            (entry.name.clone(), entry.value.clone())
                        }).collect::<std::collections::BTreeMap<_, _>>(),
                    }),
                )
            }
        };
        sanitized.push(SessionMcpServerBinding {
            id: id.clone(),
            display_name: server.name.clone(),
            transport,
        });
        let mut bootstrap_server = bootstrap_transport
            .as_object()
            .cloned()
            .ok_or(SessionRegistryLogicError::InvalidMcpConfiguration)?;
        bootstrap_server.insert(String::from("id"), serde_json::Value::String(id));
        bootstrap_server.insert(
            String::from("display_name"),
            serde_json::Value::String(server.name.clone()),
        );
        bootstrap_server.insert(String::from("active"), serde_json::Value::Bool(true));
        bootstrap.push(serde_json::Value::Object(bootstrap_server));
    }
    let configuration_reference = format!("session-mcp:blake3:{}", declaration_hash.to_hex());
    binding.mcp = SessionMcpBinding {
        schema_version: 1,
        declaration_hash,
        configuration_reference: Some(configuration_reference),
        servers: sanitized,
    };
    serde_json::to_string(&serde_json::json!({
        "schema_version": 1,
        "session_id": session_id,
        "declaration_hash": declaration_hash,
        "servers": bootstrap,
    }))
    .map(SensitiveSessionMcpConfiguration)
    .map(Some)
    .map_err(|_| SessionRegistryLogicError::InvalidMcpConfiguration)
}

fn stable_mcp_server_id(session_id: SessionId, server_index: usize, name: &str) -> String {
    if name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return name.to_owned();
    }
    let id_hash = ContentHash::digest(format!("{session_id}\0{server_index}\0{name}").as_bytes());
    format!("acp-{}", &id_hash.to_hex()[..16])
}

fn secret_bindings(
    session_id: SessionId,
    server_id: &str,
    kind: &str,
    entries: &[SessionMcpSensitiveEntry],
) -> Vec<SessionMcpSecretBinding> {
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| SessionMcpSecretBinding {
            name: entry.name.clone(),
            secret_reference: format!(
                "secret-ref:session-mcp:{session_id}:{server_id}:{kind}:{index}"
            ),
            value_hash: ContentHash::digest(entry.value.as_bytes()),
        })
        .collect()
}

fn validate_mcp_servers(
    servers: &[SessionMcpServerDeclaration],
) -> Result<(), SessionRegistryLogicError> {
    if servers.len() > MAX_MCP_SERVERS {
        return Err(SessionRegistryLogicError::InvalidMcpConfiguration);
    }
    let mut names = std::collections::BTreeSet::new();
    for server in servers {
        if server.name.trim().is_empty()
            || server.name.len() > 128
            || server.name.chars().any(char::is_control)
            || !names.insert(server.name.to_ascii_lowercase())
        {
            return Err(SessionRegistryLogicError::InvalidMcpConfiguration);
        }
        match &server.transport {
            SessionMcpTransportDeclaration::Stdio {
                program,
                arguments,
                environment,
            } => {
                if !std::path::Path::new(program).is_absolute()
                    || program.len() > MAX_MCP_FIELD_BYTES
                    || program.contains('\0')
                    || arguments.len() > MAX_MCP_ARGUMENTS
                    || arguments
                        .iter()
                        .any(|value| value.len() > MAX_MCP_FIELD_BYTES || value.contains('\0'))
                {
                    return Err(SessionRegistryLogicError::InvalidMcpConfiguration);
                }
                validate_mcp_entries(environment, true)?;
            }
            SessionMcpTransportDeclaration::StreamableHttp { url, headers, .. } => {
                let parsed = Url::parse(url)
                    .map_err(|_| SessionRegistryLogicError::InvalidMcpConfiguration)?;
                let host = parsed
                    .host_str()
                    .ok_or(SessionRegistryLogicError::InvalidMcpConfiguration)?;
                if url.len() > MAX_MCP_FIELD_BYTES
                    || !(parsed.scheme() == "https"
                        || (parsed.scheme() == "http"
                            && matches!(host, "localhost" | "127.0.0.1" | "::1")))
                    || parsed.username() != ""
                    || parsed.password().is_some()
                {
                    return Err(SessionRegistryLogicError::InvalidMcpConfiguration);
                }
                validate_mcp_entries(headers, false)?;
            }
        }
    }
    Ok(())
}

fn validate_mcp_entries(
    entries: &[SessionMcpSensitiveEntry],
    environment: bool,
) -> Result<(), SessionRegistryLogicError> {
    if entries.len() > MAX_MCP_KEY_VALUES {
        return Err(SessionRegistryLogicError::InvalidMcpConfiguration);
    }
    let mut names = std::collections::BTreeSet::new();
    for entry in entries {
        let valid_name = !entry.name.is_empty()
            && entry.name.len() <= 128
            && if environment {
                !entry.name.contains(['=', '\0'])
            } else {
                entry
                    .name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            };
        if !valid_name
            || entry.value.len() > MAX_MCP_FIELD_BYTES
            || entry.value.contains('\0')
            || !names.insert(entry.name.to_ascii_lowercase())
        {
            return Err(SessionRegistryLogicError::InvalidMcpConfiguration);
        }
    }
    Ok(())
}

fn validate_create(command: &CreateSessionCommand) -> Result<(), SessionRegistryLogicError> {
    if command.sessions_root.as_os_str().is_empty() {
        return Err(SessionRegistryLogicError::InvalidSessionsRoot);
    }
    if command.workspace.as_os_str().is_empty() {
        return Err(SessionRegistryLogicError::InvalidWorkspace);
    }
    if command.style_binding.id.is_empty()
        || command.style_binding.id.len() > MAX_STYLE_LENGTH
        || !command
            .style_binding
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(SessionRegistryLogicError::InvalidStyle);
    }
    if command.style_binding.version.trim().is_empty()
        || command.style_binding.harness.trim().is_empty()
        || command.style_binding.harness_version.trim().is_empty()
        || command.style_binding.runtime_api_version.trim().is_empty()
        || command.style_binding.source_locator.trim().is_empty()
        || command.style_binding.configuration_json.is_empty()
        || command.style_binding.compiled_style_json.is_empty()
        || command.style_binding.content_hash
            != ContentHash::digest(command.style_binding.configuration_json.as_bytes())
        || command.style_binding.compiled_style_hash
            != ContentHash::digest(command.style_binding.compiled_style_json.as_bytes())
    {
        return Err(SessionRegistryLogicError::InvalidStyleBinding);
    }
    Ok(())
}

fn initial_event(
    prepared: &PreparedSessionDataRecord,
    style: &SessionStyleBinding,
) -> Result<EventEnvelope<RuntimeCommittedEvent>, SessionRegistryLogicError> {
    EventEnvelope::seal(
        EventMetadata {
            event_id: prepared.event_id,
            scope: EventScope::Session(prepared.session_id),
            sequence: Sequence::FIRST,
            timestamp: prepared.timestamp,
            event_type: String::from("session.created"),
            event_version: Version::new(1, 0),
            correlation_id: prepared.correlation_id,
            causation_id: prepared.causation_id,
            parent_graph_node_id: None,
            origin: EventOrigin {
                subsystem: String::from("runtime"),
                plugin: None,
            },
            schema_version: Version::new(1, 0),
            artifacts: vec![],
            classification: EventClassification::Committed,
        },
        RuntimeCommittedEvent::SessionCreated(SessionCreatedEvent {
            workspace: prepared.normalized_workspace.to_string_lossy().into_owned(),
            style: style.id.clone(),
            style_binding: Some(Box::new(style.clone())),
        }),
    )
    .map_err(|_| SessionRegistryLogicError::InitialEventSerialization)
}

/// Session registry business failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SessionRegistryLogicError {
    /// Configured storage root is empty.
    #[error("sessions root is invalid")]
    InvalidSessionsRoot,
    /// Workspace selection is empty.
    #[error("workspace selection is invalid")]
    InvalidWorkspace,
    /// Style ID has unsafe syntax or length.
    #[error("session style identifier is invalid")]
    InvalidStyle,
    /// Selected style binding is incomplete or inconsistent.
    #[error("session style binding is invalid")]
    InvalidStyleBinding,
    /// Per-session MCP declarations are invalid or cannot be encoded safely.
    #[error("per-session MCP configuration is invalid")]
    InvalidMcpConfiguration,
    /// The compiled style is valid but cannot execute in this runtime.
    #[error("session style is not runtime-executable: {0}")]
    RuntimeExecutability(RuntimeExecutabilityError),
    /// Data operation failed.
    #[error("session registry data failed: {0}")]
    Data(SessionRegistryDataError),
    /// Initial event could not be serialized.
    #[error("initial session event could not be serialized")]
    InitialEventSerialization,
    /// Data returned sequence zero.
    #[error("session registry returned an invalid sequence")]
    InvalidSequence,
    /// The immutable node-execution plan file could not be prepared.
    #[error("session execution plan logic failed: {0}")]
    ExecutionPlan(crate::execution_plan::ExecutionPlanLogicError),
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use agentmod_primitives::ContentHash;
    use agentmod_runtime_data::{
        fixture_file::{
            CreateFixtureDirectoryDataRequest, FixtureFileDataPort, ReadFixtureFileDataRequest,
            WriteFixtureFileDataRequest,
        },
        journal::{JournalEventDataPort, ScanEventsDataRequest},
        local::local_runtime_data_with_node_executors,
        node_executor::RuntimeNodeExecutorData,
    };
    use agentmod_session_style_sdk::{StyleKind, declarative_graph_manifest, to_json};

    use super::*;
    use crate::{
        RuntimeLogic,
        node_executor::{bind_runtime_execution_plan, revalidate_runtime_execution_plan},
        session::{RuntimeCommittedEvent, SessionStyleBinding, SessionStyleSource},
        style::{
            ChildStyleMemoryAccess, InspectStyleCommand, SelectChildStyleCommand,
            SessionStyleLogicError, SessionStyleLogicPort, StyleDecisionCapability,
            StyleEnvironment, StyleHarnessDescriptor, ValidateStyleBindingCommand,
        },
        style_executor::CompiledStyleExecutor,
    };

    const USER_PARALLEL_GRAPH: &str = r#"
format_version = 1
entry = "fanout"

[budget]
max_steps = 20
max_tokens = 100
max_cost_micros = 100
max_duration_ms = 1000

[declarations]
capabilities = ["agents"]

[[nodes]]
id = "fanout"
kind = "parallel_branch"
configuration = { type = "parallel_branch", max_parallelism = 2, max_queue_depth = 2, join_target = "gather", join_policy = "all" }

[[nodes]]
id = "left-check"
kind = "conditional_branch"

[[nodes]]
id = "right-check"
kind = "conditional_branch"

[[nodes]]
id = "gather"
kind = "join_results"
configuration = { type = "join_results", required = ["left-result", "right-result"], optional = [], minimum_successes = 2, failure_policy = "wait_required", ordering_policy = "member_id", timeout_ms = 1000, cancellation_propagates = true, result_projection = "node_references", artifact_collection = "none" }

[[nodes]]
id = "finished"
kind = "complete_session"

[[edges]]
from = "fanout"
to = "left-check"
label = "left-result"

[[edges]]
from = "fanout"
to = "right-check"
label = "right-result"

[[edges]]
from = "left-check"
to = "gather"

[[edges]]
from = "right-check"
to = "gather"

[[edges]]
from = "gather"
to = "finished"
"#;

    fn parallel_join_registry() -> RuntimeNodeExecutorData {
        RuntimeNodeExecutorData::native().expect("native executor registry")
    }

    fn style_environment(user_style_root: &std::path::Path) -> StyleEnvironment {
        StyleEnvironment {
            runtime_api_version: String::from("1.0.0"),
            plugin_set_hash: ContentHash::digest(b"admission-plugin-set").to_hex(),
            user_style_root: Some(user_style_root.to_owned()),
            project_style_root: None,
            plugin_style_roots: Vec::new(),
            cache_root: None,
            capabilities: BTreeSet::from([String::from("agents"), String::from("approval")]),
            tool_groups: BTreeMap::new(),
            providers: BTreeSet::new(),
            plugins: BTreeSet::from([String::from("runtime.security")]),
            context_transforms: Vec::new(),
            plugin_memory_providers: Vec::new(),
            plugin_compactors: Vec::new(),
            memory_providers: BTreeSet::from([String::from("none")]),
            compaction_strategies: BTreeSet::from([String::from("none")]),
            supported_decisions: BTreeSet::from([
                StyleDecisionCapability::Continue,
                StyleDecisionCapability::Replace,
                StyleDecisionCapability::Reject,
                StyleDecisionCapability::RequireApproval,
                StyleDecisionCapability::Defer,
                StyleDecisionCapability::Cancel,
                StyleDecisionCapability::Fork,
            ]),
            graph_references: BTreeMap::new(),
            harnesses: BTreeMap::from([(
                String::from("native"),
                StyleHarnessDescriptor {
                    version: String::from("1.0.0"),
                    capabilities: [
                        "cancellation",
                        "streaming",
                        "structured_context_replacement",
                        "token_usage",
                        "tool_calls",
                    ]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                    available: true,
                },
            )]),
        }
    }

    fn user_manifest() -> String {
        let mut manifest = declarative_graph_manifest(USER_PARALLEL_GRAPH);
        manifest.identity.id = String::from("user-parallel-admission");
        manifest.identity.version = String::from("7.3.1");
        manifest.identity.runtime_api = String::from("^1.0");
        manifest.kind = StyleKind::Custom;
        manifest.built_in_semantic = None;
        to_json(&manifest).expect("canonical user manifest")
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one admission fixture keeps user discovery, native rejection, durable creation, and restart revalidation visible as a single production-path proof"
    )]
    fn user_parallel_join_normal_admission_persists_and_revalidates_exact_native_plan() {
        let root = tempfile::tempdir().expect("test root");
        let workspace = tempfile::tempdir().expect("workspace");
        let user_styles = root.path().join("user-styles");
        let registry = parallel_join_registry();
        let data = local_runtime_data_with_node_executors(registry.clone());
        data.create_fixture_directory(CreateFixtureDirectoryDataRequest {
            directory: user_styles.clone(),
            recursive: false,
        })
        .expect("user style root");
        data.write_fixture_file(WriteFixtureFileDataRequest {
            file: user_styles.join("user-parallel-admission.json"),
            bytes: user_manifest().into_bytes(),
        })
        .expect("user style manifest");
        let environment = style_environment(&user_styles);
        let logic = RuntimeLogic::new(data.clone());
        let resolved = logic
            .resolve_style(InspectStyleCommand {
                selector: String::from("user-parallel-admission@7.3.1"),
                environment,
            })
            .expect("resolve user style");

        assert_eq!(resolved.binding.source, SessionStyleSource::User);
        assert_eq!(
            std::path::Path::new(&resolved.binding.source_locator)
                .file_name()
                .and_then(std::ffi::OsStr::to_str),
            Some("user-parallel-admission.json")
        );
        assert!(resolved.binding.execution_plan.is_none());
        assert_eq!(
            CompiledStyleExecutor::from_unbound_binding(&resolved.binding)
                .expect("compiled user graph")
                .adapter_kind(),
            None,
            "admission must not depend on a built-in adapter profile"
        );
        let expected_registry_hash =
            crate::node_executor::inspect_runtime_executability(&registry, &resolved.binding)
                .expect("inspect resolved user graph")
                .registry_hash;

        let created = logic
            .create_session(CreateSessionCommand {
                sessions_root: root.path().join("sessions"),
                workspace: workspace.path().to_owned(),
                style_binding: resolved.binding,
                mcp_servers: Vec::new(),
            })
            .expect("normal session creation");
        let style_lock: serde_json::Value = serde_json::from_slice(
            &data
                .read_fixture_file(ReadFixtureFileDataRequest {
                    file: created.session_directory.join("style.lock"),
                })
                .expect("style lock"),
        )
        .expect("style lock JSON");
        let persisted: SessionStyleBinding =
            serde_json::from_value(style_lock["binding"].clone()).expect("persisted binding");
        let plan = persisted
            .execution_plan
            .as_ref()
            .expect("persisted execution plan");
        let plan_hash = persisted
            .execution_plan_hash
            .expect("persisted execution-plan hash");
        assert_eq!(
            ContentHash::digest(&serde_json::to_vec(plan).expect("execution plan JSON")),
            plan_hash
        );
        assert_eq!(plan.registry_hash, expected_registry_hash);
        assert_eq!(
            plan.nodes
                .iter()
                .map(|node| {
                    (
                        node.node_id.as_str(),
                        node.executor_id.as_str(),
                        node.executor_version.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            [
                ("fanout", "runtime.parallel", "1.0.0"),
                ("finished", "runtime.session-completion", "1.0.0"),
                ("gather", "runtime.join", "1.0.0"),
                ("left-check", "runtime.conditional", "1.0.0"),
                ("right-check", "runtime.conditional", "1.0.0"),
            ]
        );

        let journal = data
            .scan_events(ScanEventsDataRequest {
                session_directory: created.session_directory.clone(),
            })
            .expect("canonical journal");
        assert_eq!(journal.events.len(), 1);
        let created_event: RuntimeCommittedEvent =
            serde_json::from_value(journal.events[0].event.payload.clone())
                .expect("session-created event");
        let RuntimeCommittedEvent::SessionCreated(created_payload) = created_event else {
            panic!("first event was not session.created");
        };
        assert_eq!(
            created_payload.style_binding.as_deref(),
            Some(&persisted),
            "the canonical initial event and immutable style lock must bind the same plan"
        );

        let restarted = local_runtime_data_with_node_executors(registry.clone());
        revalidate_runtime_execution_plan(&restarted, &persisted)
            .expect("restart exact-plan revalidation");
        let native_restart = local_runtime_data_with_node_executors(
            RuntimeNodeExecutorData::native().expect("native executor registry"),
        );
        revalidate_runtime_execution_plan(&native_restart, &persisted)
            .expect("native restart retains the exact implementation");
    }

    #[test]
    fn parent_restricted_child_binding_revalidates_exact_descriptor_and_plan() {
        let root = tempfile::tempdir().expect("test root");
        let user_styles = root.path().join("user-styles");
        let registry = parallel_join_registry();
        let data = local_runtime_data_with_node_executors(registry);
        data.create_fixture_directory(CreateFixtureDirectoryDataRequest {
            directory: user_styles.clone(),
            recursive: false,
        })
        .expect("user style root");
        data.write_fixture_file(WriteFixtureFileDataRequest {
            file: user_styles.join("user-parallel-admission.json"),
            bytes: user_manifest().into_bytes(),
        })
        .expect("user style manifest");
        let environment = style_environment(&user_styles);
        let logic = RuntimeLogic::new(data.clone());
        let resolved = logic
            .resolve_style(InspectStyleCommand {
                selector: String::from("user-parallel-admission@7.3.1"),
                environment: environment.clone(),
            })
            .expect("resolve child style");
        let mut retained = logic
            .select_child_style(SelectChildStyleCommand {
                binding: resolved.binding,
                tool_groups: BTreeSet::new(),
                memory_access: ChildStyleMemoryAccess::ReadWrite,
                inherited_provider: None,
                max_tokens: Some(50),
                max_cost_micros: Some(50),
                environment: environment.clone(),
            })
            .expect("compile parent restrictions")
            .binding;
        bind_runtime_execution_plan(&data, &mut retained).expect("bind exact child plan");

        let retained_compiled: serde_json::Value =
            serde_json::from_str(&retained.compiled_style_json).expect("compiled descriptor");
        assert_eq!(retained.budgets.max_tokens, 50);
        assert_eq!(retained.budgets.max_cost_micros, 50);
        assert_eq!(retained_compiled["budgets"]["max_tokens"], 50);
        assert_eq!(retained_compiled["budgets"]["max_cost_micros"], 50);
        assert_eq!(
            retained_compiled["graph"]["budget"]["max_tokens"],
            serde_json::json!(50)
        );
        assert!(retained.execution_plan.is_some());

        let restarted: SessionStyleBinding = serde_json::from_str(
            &serde_json::to_string(&retained).expect("retained binding serialization"),
        )
        .expect("retained binding restart");
        logic
            .validate_style_binding(ValidateStyleBindingCommand {
                binding: restarted.clone(),
                environment: environment.clone(),
            })
            .expect("exact overridden child binding revalidation");

        let mut version_drift = restarted;
        version_drift
            .execution_plan
            .as_mut()
            .expect("execution plan")
            .nodes
            .first_mut()
            .expect("executor")
            .executor_version = String::from("9.0.0");
        version_drift.execution_plan_hash = Some(ContentHash::digest(
            &serde_json::to_vec(
                version_drift
                    .execution_plan
                    .as_ref()
                    .expect("execution plan"),
            )
            .expect("plan serialization"),
        ));
        assert!(matches!(
            logic.validate_style_binding(ValidateStyleBindingCommand {
                binding: version_drift,
                environment,
            }),
            Err(SessionStyleLogicError::BindingIncompatible { .. })
        ));
    }
}
