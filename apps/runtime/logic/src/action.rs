//! Typed consequential-action proposals and digest binding.

use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Runtime-logic proposal identifier.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProposalId(pub String);

/// Structured filesystem write proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilesystemWriteAction {
    /// Normalized workspace-relative or approved-root-relative path.
    pub path: String,
    /// Hash expected before mutation.
    pub expected_hash: Option<ContentHash>,
    /// Hash of proposed content; content itself may live in an artifact.
    pub content_hash: ContentHash,
    /// Whether an existing target may be replaced.
    pub overwrite: bool,
}

/// Structured process start proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProcessStartAction {
    /// Executable after dependency-independent normalization.
    pub executable: String,
    /// Exact argument vector.
    pub arguments: Vec<String>,
    /// Normalized working-directory selection.
    pub working_directory: String,
    /// Environment variable names only; secret values remain references.
    pub environment_names: Vec<String>,
    /// Secret reference names requested by the process.
    pub secret_references: Vec<String>,
}

/// Structured network request proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HttpRequestAction {
    /// Uppercase HTTP method.
    pub method: String,
    /// Normalized URL.
    pub url: String,
    /// Header names; sensitive values remain dependency-side references.
    pub header_names: Vec<String>,
    /// Optional body artifact/hash.
    pub body_hash: Option<ContentHash>,
}

/// Structured provider request proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelRequestAction {
    /// Harness registry ID authorized to execute the request.
    pub harness: String,
    /// Provider adapter ID.
    pub provider: String,
    /// Provider model ID.
    pub model: String,
    /// Approved conversation projection content hash.
    pub projection_hash: ContentHash,
    /// Provider-specific options already constrained by runtime configuration.
    pub options: Value,
}

/// Structured tool call proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolCallAction {
    /// Stable tool ID.
    pub tool: String,
    /// Capability group.
    pub group: String,
    /// Validated arguments.
    pub arguments: Value,
    /// Optional originating plugin/MCP server.
    pub source: Option<String>,
}

/// Structured immutable artifact persistence proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPersistenceAction {
    /// Hash of the exact bytes to persist.
    pub content_hash: ContentHash,
    /// Valid media type.
    pub mime_type: String,
    /// Exact byte count.
    pub byte_size: u64,
    /// Exact security handling classification.
    pub security: ArtifactPersistenceSecurity,
    /// Exact retention contract.
    pub retention: ArtifactPersistenceRetention,
}

/// Security classification bound into an artifact-persistence action digest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactPersistenceSecurity {
    /// Ordinary workspace content.
    Standard,
    /// User-private content.
    Private,
    /// Secret-bearing content.
    Secret,
}

/// Retention contract bound into an artifact-persistence action digest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArtifactPersistenceRetention {
    /// Retain until explicit removal policy acts.
    Permanent,
    /// Retain with the owning session.
    Session,
    /// Retain until this exact portable Unix timestamp in milliseconds.
    UntilUnixMilliseconds {
        /// Exact expiration timestamp.
        expires_at_millis: i64,
    },
}

/// Structured canonical child-session message delivery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentMessageAction {
    /// Exact runtime-managed child session.
    pub child_session_id: String,
    /// Stable business identity of the bounded message.
    pub message_identity: ContentHash,
    /// Hash of the canonical typed message body.
    pub payload_hash: ContentHash,
    /// Hash of the ordered immutable artifact references.
    pub artifact_references_hash: ContentHash,
    /// Stable information-flow classification.
    pub security_classification: String,
}

/// Structured cancellation proposal for an exact set of owned child sessions.
///
/// The action authorizes cancellation dispatch but does not perform it. The
/// caller must dispatch the exact sorted child set through the runtime's
/// child-session boundary after the hash-bound proposal is accepted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildAgentCancellationAction {
    /// Exact parent session that owns every selected child.
    pub parent_session_id: String,
    /// Immutable style-run identity.
    pub run_id: String,
    /// Generic graph node requesting cancellation.
    pub node_id: String,
    /// Stable branch coordinates for the requesting work item.
    pub branch_path: Vec<String>,
    /// Current bounded node attempt.
    pub attempt: u32,
    /// Current bounded loop iteration.
    pub loop_iteration: u32,
    /// Current canonical graph step.
    pub step: u64,
    /// Immutable execution plan selected for this run.
    pub execution_plan_hash: ContentHash,
    /// Exact adapter configuration for this node.
    pub configuration_hash: ContentHash,
    /// Hash of the canonical child-state projection used by the node.
    pub projection_hash: ContentHash,
    /// Hash of the bounded cancellation reason.
    pub reason_hash: ContentHash,
    /// Exact child sessions in ascending canonical identifier order.
    pub child_session_ids: Vec<String>,
}

/// Structured isolated plugin-node invocation proposal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginNodeInvocationAction {
    /// Exact activated plugin selected by the immutable execution plan.
    pub plugin_id: String,
    /// Exact executor ID selected by the immutable execution plan.
    pub executor_id: String,
    /// Exact executor version selected by the immutable execution plan.
    pub executor_version: String,
    /// Stable invocation identity derived from the complete node work.
    pub invocation_id: String,
    /// Digest binding the complete bounded invocation request.
    pub invocation_digest: ContentHash,
    /// Hash of the exact validated plugin executor declaration.
    pub declaration_hash: ContentHash,
    /// Whether the declaration permits the isolated plugin to request effects.
    pub external_effects: bool,
    /// Exact declared permission names, in canonical declaration order.
    pub required_permissions: Vec<String>,
}

/// Exact immutable fields shared by consequential plugin context operations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginContextOperationActionIdentity {
    /// Exact activated plugin.
    pub plugin_id: String,
    /// Exact activated plugin version.
    pub plugin_version: String,
    /// Exact selected implementation within the plugin.
    pub implementation_id: String,
    /// Exact selected implementation version.
    pub implementation_version: String,
    /// Hash of the authoritative implementation declaration.
    pub declaration_hash: ContentHash,
    /// Hash of the immutable implementation configuration.
    pub configuration_reference: ContentHash,
    /// Hash of the complete bounded request.
    pub request_hash: ContentHash,
    /// Hash of the explicitly scoped readable state.
    pub readable_state_hash: ContentHash,
    /// Digest of the complete isolated invocation identity.
    pub invocation_digest: ContentHash,
    /// Stable digest-backed invocation identifier.
    pub invocation_id: String,
    /// Stable one-attempt idempotency key.
    pub idempotency_key: String,
    /// Exact isolated-worker attempt. Plugin context operations permit one.
    pub attempt: u8,
}

/// Consequential construction of provider context through plugin memory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginMemoryRetrieveAction {
    /// Exact immutable operation identity.
    pub identity: PluginContextOperationActionIdentity,
    /// Hash of the exact canonical query and retrieval limits.
    pub retrieval_contract_hash: ContentHash,
}

/// Consequential non-idempotent plugin memory write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginMemoryWriteAction {
    /// Exact immutable operation identity.
    pub identity: PluginContextOperationActionIdentity,
    /// Hash of the exact bounded typed value.
    pub value_hash: ContentHash,
    /// Hash of ordered immutable artifact references.
    pub artifact_references_hash: ContentHash,
    /// Hash of ordered canonical non-artifact references.
    pub references_hash: ContentHash,
    /// Stable information-flow classification.
    pub security_classification: String,
}

/// Consequential provider-context compaction through an exact plugin.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginCompactionAction {
    /// Exact immutable operation identity.
    pub identity: PluginContextOperationActionIdentity,
    /// Hash of the current provider projection.
    pub projection_hash: ContentHash,
    /// Hash of the exact required preservation contract.
    pub preservation_contract_hash: ContentHash,
}

/// Every consequential runtime action class.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", content = "details", rename_all = "snake_case")]
pub enum ConsequentialAction {
    /// Construct provider-visible context.
    ContextConstruction {
        /// Stable context strategy.
        strategy: String,
    },
    /// Replace provider-visible structured context.
    ContextReplacement {
        /// Replacement projection hash.
        projection_hash: ContentHash,
    },
    /// Call a model provider.
    ModelRequest(ModelRequestAction),
    /// Retry a failed model request.
    ModelRetry {
        /// Original proposal.
        original_proposal: ProposalId,
        /// Retry attempt.
        attempt: u32,
    },
    /// Change provider/model.
    ProviderSwitch {
        /// Provider adapter ID.
        provider: String,
        /// Model ID.
        model: String,
    },
    /// Execute a normalized tool call.
    ToolCall(ToolCallAction),
    /// Start a supervised process.
    ProcessStart(ProcessStartAction),
    /// Send bytes to a supervised process.
    ProcessInput {
        /// Runtime process ID.
        process_id: String,
        /// Hash of exact bytes.
        input_hash: ContentHash,
    },
    /// Write/replace a file.
    FilesystemWrite(FilesystemWriteAction),
    /// Execute an HTTP operation.
    HttpRequest(HttpRequestAction),
    /// Execute web search.
    WebSearch {
        /// Search query.
        query: String,
        /// Result bound.
        result_count: u32,
    },
    /// Persist a memory record.
    MemoryWrite {
        /// Exact selected provider.
        provider: String,
        /// Normalized memory scope.
        scope: String,
        /// Runtime-owned provenance label.
        source: String,
        /// Compiled automatic write boundary.
        policy: String,
        /// Exact turn/run identity.
        run_id: String,
        /// Exact provider/model/options/input request.
        request_hash: ContentHash,
        /// Hash of bounded content.
        content_hash: ContentHash,
        /// Exact UTF-8 byte count.
        byte_size: u64,
        /// Runtime-recorded timestamp reused during recovery.
        created_at_millis: i64,
    },
    /// Construct provider context through an exact plugin memory provider.
    PluginMemoryRetrieve(Box<PluginMemoryRetrieveAction>),
    /// Persist a typed value through an exact non-idempotent plugin provider.
    PluginMemoryWrite(Box<PluginMemoryWriteAction>),
    /// Persist approved bytes as an immutable content-addressed artifact.
    ArtifactPersistence(ArtifactPersistenceAction),
    /// Compact provider context.
    Compaction {
        /// Stable strategy.
        strategy: String,
    },
    /// Compact provider context through an exact plugin implementation.
    PluginCompaction(Box<PluginCompactionAction>),
    /// Create a child session.
    ChildAgentCreation {
        /// Child style.
        style: String,
        /// Workspace mode.
        workspace_mode: String,
        /// Token budget.
        token_budget: u64,
        /// Exact inherited provider selected by the parent, when enabled.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_provider: Option<String>,
        /// Exact inherited model selected by the parent, when enabled.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_model: Option<String>,
        /// Canonical hash of the exact inherited MCP binding, when enabled.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        inherited_mcp_binding_hash: Option<ContentHash>,
    },
    /// Deliver a canonical typed event to an owned child session.
    ChildAgentMessage(ChildAgentMessageAction),
    /// Authorize cancellation of an exact set of owned child sessions.
    ChildAgentCancellation(ChildAgentCancellationAction),
    /// Change plugin activation/configuration/state.
    PluginStateChange {
        /// Plugin ID.
        plugin: String,
        /// Stable operation.
        operation: String,
    },
    /// Invoke an exact plugin-provided graph-node executor.
    PluginNodeInvocation(PluginNodeInvocationAction),
    /// Resume a durable continuation.
    ContinuationResume {
        /// Continuation rendered at this logic boundary.
        continuation: String,
    },
    /// Create a durable schedule.
    ScheduleCreation {
        /// Stable schedule expression.
        schedule: String,
        /// Session style.
        style: String,
    },
    /// Restore a Git/runtime checkpoint.
    CheckpointRestoration {
        /// Checkpoint ID.
        checkpoint: String,
    },
}

impl ConsequentialAction {
    /// Stable action type used by permission matching and event names.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ContextConstruction { .. } | Self::PluginMemoryRetrieve(_) => {
                "context_construction"
            }
            Self::ContextReplacement { .. } => "context_replacement",
            Self::ModelRequest(_) => "model_request",
            Self::ModelRetry { .. } => "model_retry",
            Self::ProviderSwitch { .. } => "provider_switch",
            Self::ToolCall(_) => "tool_call",
            Self::ProcessStart(_) => "process_start",
            Self::ProcessInput { .. } => "process_input",
            Self::FilesystemWrite(_) => "filesystem_write",
            Self::HttpRequest(_) => "http_request",
            Self::WebSearch { .. } => "web_search",
            Self::MemoryWrite { .. } | Self::PluginMemoryWrite(_) => "memory_write",
            Self::ArtifactPersistence(_) => "artifact_persistence",
            Self::Compaction { .. } | Self::PluginCompaction(_) => "compaction",
            Self::ChildAgentCreation { .. } => "child_agent_creation",
            Self::ChildAgentMessage(_) => "child_agent_message",
            Self::ChildAgentCancellation(_) => "child_agent_cancellation",
            Self::PluginStateChange { .. } => "plugin_state_change",
            Self::PluginNodeInvocation(_) => "plugin_node_invocation",
            Self::ContinuationResume { .. } => "continuation_resume",
            Self::ScheduleCreation { .. } => "schedule_creation",
            Self::CheckpointRestoration { .. } => "checkpoint_restoration",
        }
    }
}

/// Original or interceptor-modified proposal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ActionProposal {
    /// Stable proposal ID.
    pub id: ProposalId,
    /// Typed proposed side effect.
    pub action: ConsequentialAction,
    /// Explicit session style.
    pub style: String,
    /// Normalized workspace label.
    pub workspace: String,
    /// Originating subsystem/plugin, for audit and policy.
    pub origin: String,
}

impl ActionProposal {
    /// Computes the digest to which a dependency authorization grant must be bound.
    ///
    /// # Errors
    ///
    /// Returns [`ActionProposalError`] if deterministic JSON serialization fails.
    pub fn digest(&self) -> Result<ContentHash, ActionProposalError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|error| ActionProposalError::Serialization(error.to_string()))
    }
}

/// Proposal construction failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ActionProposalError {
    /// Typed action could not be serialized for digest binding.
    #[error("action proposal serialization failed: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proposal(path: &str) -> ActionProposal {
        ActionProposal {
            id: ProposalId("proposal-1".into()),
            action: ConsequentialAction::FilesystemWrite(FilesystemWriteAction {
                path: path.into(),
                expected_hash: None,
                content_hash: ContentHash::digest(b"content"),
                overwrite: false,
            }),
            style: "persistent-chat".into(),
            workspace: "fixture".into(),
            origin: "runtime".into(),
        }
    }

    #[test]
    fn digest_binds_exact_modified_action() {
        assert_ne!(
            proposal("safe.txt").digest().expect("digest"),
            proposal("other.txt").digest().expect("digest")
        );
        assert_eq!(
            proposal("safe.txt").digest().expect("digest"),
            proposal("safe.txt").digest().expect("digest")
        );
    }

    #[test]
    fn plugin_node_digest_binds_exact_executor_and_declaration() {
        let proposal = |executor_version: &str, declaration_hash: ContentHash| ActionProposal {
            id: ProposalId("plugin-node:invoke-1".into()),
            action: ConsequentialAction::PluginNodeInvocation(PluginNodeInvocationAction {
                plugin_id: "fixture.plugin".into(),
                executor_id: "fixture.transform".into(),
                executor_version: executor_version.into(),
                invocation_id: "invoke-1".into(),
                invocation_digest: ContentHash::digest(b"complete invocation"),
                declaration_hash,
                external_effects: false,
                required_permissions: vec!["artifact.read".into()],
            }),
            style: "user-graph".into(),
            workspace: "fixture".into(),
            origin: "plugin:fixture.plugin".into(),
        };
        let exact = proposal("1.0.0", ContentHash::digest(b"declaration-v1"));
        assert_eq!(
            exact.digest().expect("digest"),
            proposal("1.0.0", ContentHash::digest(b"declaration-v1"))
                .digest()
                .expect("same digest")
        );
        assert_ne!(
            exact.digest().expect("digest"),
            proposal("1.0.1", ContentHash::digest(b"declaration-v1"))
                .digest()
                .expect("version digest")
        );
        assert_ne!(
            exact.digest().expect("digest"),
            proposal("1.0.0", ContentHash::digest(b"substituted"))
                .digest()
                .expect("declaration digest")
        );
    }

    fn plugin_context_action_identity() -> PluginContextOperationActionIdentity {
        PluginContextOperationActionIdentity {
            plugin_id: String::from("fixture.context"),
            plugin_version: String::from("2.0.0"),
            implementation_id: String::from("fixture.memory"),
            implementation_version: String::from("1.0.0"),
            declaration_hash: ContentHash::digest(b"declaration"),
            configuration_reference: ContentHash::digest(b"configuration"),
            request_hash: ContentHash::digest(b"request"),
            readable_state_hash: ContentHash::digest(b"readable state"),
            invocation_digest: ContentHash::digest(b"invocation"),
            invocation_id: String::from("plugin-context-operation:1"),
            idempotency_key: String::from("plugin-context-operation-once:1"),
            attempt: 1,
        }
    }

    #[test]
    fn plugin_context_actions_map_to_existing_permission_groups_and_bind_exact_fields() {
        let proposal = |action| ActionProposal {
            id: ProposalId(String::from("plugin-context:1")),
            action,
            style: String::from("user-graph"),
            workspace: String::from("fixture"),
            origin: String::from("plugin:fixture.context"),
        };
        let retrieve = proposal(ConsequentialAction::PluginMemoryRetrieve(Box::new(
            PluginMemoryRetrieveAction {
                identity: plugin_context_action_identity(),
                retrieval_contract_hash: ContentHash::digest(b"retrieval contract"),
            },
        )));
        let write = proposal(ConsequentialAction::PluginMemoryWrite(Box::new(
            PluginMemoryWriteAction {
                identity: plugin_context_action_identity(),
                value_hash: ContentHash::digest(b"value"),
                artifact_references_hash: ContentHash::digest(b"artifacts"),
                references_hash: ContentHash::digest(b"references"),
                security_classification: String::from("private"),
            },
        )));
        let compact = proposal(ConsequentialAction::PluginCompaction(Box::new(
            PluginCompactionAction {
                identity: plugin_context_action_identity(),
                projection_hash: ContentHash::digest(b"projection"),
                preservation_contract_hash: ContentHash::digest(b"preservation"),
            },
        )));
        assert_eq!(retrieve.action.kind(), "context_construction");
        assert_eq!(write.action.kind(), "memory_write");
        assert_eq!(compact.action.kind(), "compaction");
        let mut substituted = plugin_context_action_identity();
        substituted.attempt = 2;
        let substituted = proposal(ConsequentialAction::PluginMemoryRetrieve(Box::new(
            PluginMemoryRetrieveAction {
                identity: substituted,
                retrieval_contract_hash: ContentHash::digest(b"retrieval contract"),
            },
        )));
        assert_ne!(
            retrieve.digest().expect("retrieve digest"),
            substituted.digest().expect("substituted digest")
        );
    }
}
