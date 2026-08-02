//! Native `emit_event` node executor.
//!
//! Emits only declared user-space event namespaces. The graph supplies a
//! declared event type, a bounded typed payload, approved artifact
//! references, and non-secret metadata; the runtime constructs sequence,
//! timestamp, correlation, causation, origin, and integrity metadata at the
//! canonical journal boundary. Runtime-owned event categories (provider
//! completion, tool completion, permission decisions, scheduler claims,
//! lifecycle events, security audit events, and their peers) cannot be
//! forged.

use agentmod_primitives::ContentHash;

use crate::node_executors::{
    EmitEventConfig, ExecutorPhaseResult, MAX_ARTIFACT_REFERENCES, MAX_EMITTED_EVENT_PAYLOAD_BYTES,
    MAX_EVENT_METADATA_BYTES, NativeNodeExecutor, NodeExecutorConfig, NodeExecutorError,
    NodeExecutorInput, NodeExecutorKind, NodeExecutorOutcome, NodeExecutorStep,
    RUNTIME_OWNED_EVENT_PREFIXES,
    events::{NodeExecutorEventPayload, UserEventEmittedEvent},
    state::NodeExecutorState,
};

/// Object keys whose presence marks payload/metadata as secret-bearing.
const SECRET_KEY_NAMES: &[&str] = &[
    "secret",
    "token",
    "password",
    "passwd",
    "api_key",
    "apikey",
    "authorization",
    "access_key",
    "private_key",
];

/// Native constrained event-emission executor.
#[derive(Clone, Debug, Default)]
pub struct EmitEventExecutor;

impl EmitEventExecutor {
    fn config(input: &NodeExecutorInput) -> Result<&EmitEventConfig, NodeExecutorError> {
        let NodeExecutorConfig::EmitEvent(config) = &input.config else {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        };
        Ok(config)
    }

    fn validate(input: &NodeExecutorInput) -> Result<(), NodeExecutorError> {
        if input.executor_kind != NodeExecutorKind::EmitEvent
            || input.session_id.trim().is_empty()
            || input.run_id.trim().is_empty()
            || input.node_id.trim().is_empty()
            || input.attempt == 0
        {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        }
        let config = Self::config(input)?;
        validate_namespace(&config.namespace)?;
        if config.event_type.trim().is_empty()
            || config.event_type.len() > 256
            || !config
                .event_type
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(NodeExecutorError::InvalidInput {
                reason: String::from("emitted event type is invalid"),
            });
        }
        if config.payload_json.is_empty()
            || config.payload_json.len() > MAX_EMITTED_EVENT_PAYLOAD_BYTES
            || config.artifact_references.len() > MAX_ARTIFACT_REFERENCES
            || config.metadata_json.len() > MAX_EVENT_METADATA_BYTES
        {
            return Err(NodeExecutorError::BoundExceeded {
                detail: String::from("emitted event payload, metadata, or artifact bound exceeded"),
            });
        }
        if contains_secret_key(&config.payload_json)? || contains_secret_key(&config.metadata_json)?
        {
            return Err(NodeExecutorError::InvalidInput {
                reason: String::from("emitted event payload or metadata contains secret keys"),
            });
        }
        Ok(())
    }
}

/// Validates a declared user-space namespace and rejects runtime-owned
/// categories.
fn validate_namespace(namespace: &str) -> Result<(), NodeExecutorError> {
    if namespace.trim().is_empty()
        || namespace.len() > 128
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(NodeExecutorError::InvalidInput {
            reason: String::from("emitted event namespace is invalid"),
        });
    }
    if RUNTIME_OWNED_EVENT_PREFIXES
        .iter()
        .any(|prefix| namespace.starts_with(prefix))
    {
        return Err(NodeExecutorError::InvalidInput {
            reason: format!("emitted event namespace `{namespace}` is runtime-owned"),
        });
    }
    Ok(())
}

/// Rejects payloads and metadata carrying obvious secret keys.
fn contains_secret_key(json: &str) -> Result<bool, NodeExecutorError> {
    let value: serde_json::Value =
        serde_json::from_str(json).map_err(|error| NodeExecutorError::InvalidInput {
            reason: format!("emitted event content is not canonical JSON: {error}"),
        })?;
    Ok(contains_secret_in_value(&value))
}

fn contains_secret_in_value(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => object.iter().any(|(key, value)| {
            let secret_key = SECRET_KEY_NAMES.iter().any(|secret| {
                key.eq_ignore_ascii_case(secret)
                    || key
                        .to_ascii_lowercase()
                        .contains(&secret.to_ascii_lowercase())
            });
            secret_key || contains_secret_in_value(value)
        }),
        serde_json::Value::Array(values) => values.iter().any(contains_secret_in_value),
        _ => false,
    }
}

impl NativeNodeExecutor for EmitEventExecutor {
    fn kind(&self) -> NodeExecutorKind {
        NodeExecutorKind::EmitEvent
    }

    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError> {
        Self::validate(input)?;
        let config = Self::config(input)?;
        let identity = input.identity();
        let emission_id = identity.emission_id(&config.namespace, &config.event_type);

        // Duplicate suppression: an identical emission is a replay no-op.
        let payload_hash = ContentHash::digest(config.payload_json.as_bytes());
        if state.emitted_events.iter().any(|record| {
            record.namespace == config.namespace
                && record.event_type == config.event_type
                && record.payload_hash == payload_hash
                && record.node_id == input.node_id
                && record.step == input.step
        }) {
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                step: NodeExecutorStep {
                    events: Vec::new(),
                    transition_variables: serde_json::json!({
                        "emitted": {
                            "namespace": config.namespace,
                            "type": config.event_type,
                            "duplicate": true,
                        }
                    }),
                },
            }));
        }

        // Cancellation of an emission is a plain cancelled node.
        if input.cancel_requested {
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events: Vec::new(),
                    transition_variables: serde_json::json!({
                        "emitted": {"state": "cancelled"}
                    }),
                },
                classification: crate::node_executors::NodeExecutorFailureClassification::Cancelled,
                reason: String::from("cancelled before emission"),
            }));
        }

        let emitted = NodeExecutorEventPayload::UserEventEmitted(UserEventEmittedEvent {
            emission_id,
            node_id: input.node_id.clone(),
            run_id: input.run_id.clone(),
            attempt: input.attempt,
            loop_iteration: input.loop_iteration,
            step: input.step,
            namespace: config.namespace.clone(),
            event_type: config.event_type.clone(),
            sequence: 0, // assigned by the runtime journal committer
            payload_json: config.payload_json.clone(),
            payload_hash,
            artifact_references: config.artifact_references.clone(),
            metadata_json: config.metadata_json.clone(),
            correlation_id: String::new(), // assigned by the runtime committer
            causation_id: String::new(),   // assigned by the runtime committer
        });
        Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
            step: NodeExecutorStep {
                events: vec![emitted],
                transition_variables: serde_json::json!({
                    "emitted": {
                        "namespace": config.namespace,
                        "type": config.event_type,
                        "count": state.emitted_events.len() + 1,
                    }
                }),
            },
        }))
    }

    fn finalize(
        &self,
        _input: &NodeExecutorInput,
        _state: &NodeExecutorState,
        _receipt: &crate::node_executors::NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError> {
        // Event emission never requests external effects.
        Err(NodeExecutorError::InvalidTransition {
            detail: String::from("event emission executor requested no effect"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_executors::{NodeExecutorClock, NodeExecutorEffectReceipt};

    fn input(namespace: &str, event_type: &str, payload: &str) -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("emit"),
            attempt: 1,
            loop_iteration: 0,
            step: 8,
            executor_kind: NodeExecutorKind::EmitEvent,
            config: NodeExecutorConfig::EmitEvent(EmitEventConfig {
                namespace: namespace.to_owned(),
                event_type: event_type.to_owned(),
                payload_json: payload.to_owned(),
                artifact_references: Vec::new(),
                metadata_json: String::from(r#"{"source":"graph"}"#),
            }),
            clock: NodeExecutorClock {
                now_ms: 1_700_000_000_000,
            },
            participant_outcomes: Vec::new(),
            wake_claim: None,
            cancel_requested: false,
            remove_requested: false,
        }
    }

    #[test]
    fn user_space_event_emits_with_declared_namespace() {
        let executor = EmitEventExecutor;
        let state = NodeExecutorState::default();
        let input = input("project", "progress", r#"{"percent":42}"#);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete { step }) = phase else {
            panic!("expected complete");
        };
        assert_eq!(step.events.len(), 1);
        assert_eq!(step.events[0].event_type(), "event.user_emitted");
        let NodeExecutorEventPayload::UserEventEmitted(emitted) = &step.events[0] else {
            panic!("expected emitted event");
        };
        assert_eq!(emitted.namespace, "project");
        assert_eq!(emitted.event_type, "progress");
        assert_eq!(
            emitted.payload_hash,
            ContentHash::digest(br#"{"percent":42}"#)
        );
    }

    #[test]
    fn runtime_owned_namespaces_are_rejected() {
        let executor = EmitEventExecutor;
        let state = NodeExecutorState::default();
        for namespace in [
            "provider.completion",
            "tool.completed",
            "permission.decision",
            "scheduler.claim",
            "lifecycle.changed",
            "security.audit",
            "child_agent.completed",
        ] {
            let input = input(namespace, "fake", r"{}");
            assert!(
                matches!(
                    executor.prepare(&input, &state).expect_err("forged"),
                    NodeExecutorError::InvalidInput { .. }
                ),
                "namespace {namespace} must be rejected"
            );
        }
    }

    #[test]
    fn secret_bearing_payload_is_rejected() {
        let executor = EmitEventExecutor;
        let state = NodeExecutorState::default();
        let input = input(
            "project",
            "result",
            r#"{"api_key":"sk-live-1234","value":1}"#,
        );
        assert!(matches!(
            executor.prepare(&input, &state).expect_err("secret"),
            NodeExecutorError::InvalidInput { .. }
        ));
    }

    #[test]
    fn identical_emission_is_a_replay_noop() {
        let executor = EmitEventExecutor;
        let mut state = NodeExecutorState::default();
        let input = input("project", "progress", r#"{"percent":42}"#);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete { step }) = phase else {
            panic!("expected complete");
        };
        state.apply(&step.events[0], 50).expect("commit");
        let phase = executor.prepare(&input, &state).expect("replay");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete { step }) = phase else {
            panic!("expected replay complete");
        };
        assert!(step.events.is_empty());
        assert_eq!(step.transition_variables["emitted"]["duplicate"], true);
    }

    #[test]
    fn finalize_is_never_valid_for_emission() {
        let executor = EmitEventExecutor;
        let input = input("project", "progress", r"{}");
        let state = NodeExecutorState::default();
        assert!(matches!(
            executor
                .finalize(&input, &state, &NodeExecutorEffectReceipt::DelayCreated)
                .expect_err("no effect"),
            NodeExecutorError::InvalidTransition { .. }
        ));
    }
}
