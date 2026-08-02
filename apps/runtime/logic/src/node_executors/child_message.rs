//! Native `send_child_agent_message` node executor.
//!
//! Delivers bounded typed messages to an exact child session with exact
//! parent/child identity, child lifecycle validation through the delivery
//! port, message sequence and idempotency keys, security classification,
//! cancellation and expiration, canonical proposed/delivered/rejected events,
//! and replay/duplicate suppression. The message is never converted into a
//! provider-visible parent user message; the child style decides projection.

use agentmod_primitives::ContentHash;

use crate::node_executors::{
    ChildMessageConfig, ExecutorPhaseResult, MAX_ARTIFACT_REFERENCES, MAX_CHILD_MESSAGE_BYTES,
    NativeNodeExecutor, NodeExecutorConfig, NodeExecutorEffect, NodeExecutorEffectReceipt,
    NodeExecutorError, NodeExecutorFailureClassification, NodeExecutorInput, NodeExecutorKind,
    NodeExecutorOutcome, NodeExecutorStep,
    events::{
        ChildMessageDeliveredEvent, ChildMessageProposedEvent, ChildMessageRejectedEvent,
        NodeExecutorEventPayload,
    },
    ports::DeliverChildMessageCommand,
    state::{ChildMessageState, NodeExecutorState},
};

/// Stable reason used when the child boundary rejected delivery.
const REASON_CHILD_UNAVAILABLE: &str = "child_unavailable";
/// Stable reason used when the message expired before delivery.
const REASON_EXPIRED: &str = "expired";
/// Stable reason used when the node was cancelled before delivery.
const REASON_CANCELLED: &str = "cancelled";

/// Native child-agent message executor.
#[derive(Clone, Debug, Default)]
pub struct ChildMessageExecutor;

impl ChildMessageExecutor {
    fn config(input: &NodeExecutorInput) -> Result<&ChildMessageConfig, NodeExecutorError> {
        let NodeExecutorConfig::ChildMessage(config) = &input.config else {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        };
        Ok(config)
    }

    fn validate(input: &NodeExecutorInput) -> Result<(), NodeExecutorError> {
        if input.executor_kind != NodeExecutorKind::ChildMessage
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
        if config.child_session_id.trim().is_empty()
            || config.child_session_id.len() > MAX_CHILD_MESSAGE_BYTES
            || config.idempotency_key.trim().is_empty()
            || config.content.is_empty()
            || config.content.len() > MAX_CHILD_MESSAGE_BYTES
            || config.artifact_references.len() > MAX_ARTIFACT_REFERENCES
        {
            return Err(NodeExecutorError::InvalidInput {
                reason: String::from("child message identity or bounds are invalid"),
            });
        }
        Ok(())
    }

    /// Returns the next deterministic per-child message sequence.
    fn next_sequence(state: &NodeExecutorState, child_session_id: &str) -> u64 {
        state
            .child_messages
            .values()
            .filter(|record| record.child_session_id == child_session_id)
            .map(|record| record.sequence)
            .max()
            .unwrap_or(0)
            + 1
    }

    /// Locates the canonical proposed record for this input after proposal
    /// events were committed, using the idempotency key and exact child.
    fn proposed_record<'a>(
        state: &'a NodeExecutorState,
        config: &ChildMessageConfig,
    ) -> Option<&'a crate::node_executors::state::ChildMessageRecord> {
        state.child_messages.values().find(|record| {
            record.idempotency_key == config.idempotency_key
                && record.child_session_id == config.child_session_id
        })
    }
}

impl NativeNodeExecutor for ChildMessageExecutor {
    fn kind(&self) -> NodeExecutorKind {
        NodeExecutorKind::ChildMessage
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the child-message state machine keeps identity, duplicate suppression, expiry, cancellation, and recovery adjacent"
    )]
    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError> {
        Self::validate(input)?;
        let config = Self::config(input)?;
        let identity = input.identity();
        let sequence = Self::next_sequence(state, &config.child_session_id);
        let message_id = identity.message_id(&config.child_session_id, sequence);

        // Duplicate suppression: an existing identical key resolves to its
        // canonical terminal outcome without re-delivery.
        if let Some(existing) = Self::proposed_record(state, config) {
            if existing.content_hash != ContentHash::digest(config.content.as_bytes()) {
                return Err(NodeExecutorError::InvalidTransition {
                    detail: format!(
                        "idempotency key `{}` was reused with conflicting content",
                        config.idempotency_key
                    ),
                });
            }
            let existing = existing.clone();
            return match existing.state {
                ChildMessageState::Delivered => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "child_message": {
                                    "state": "delivered",
                                    "message_id": existing.message_id,
                                    "sequence": existing.sequence,
                                }
                            }),
                        },
                    }))
                }
                ChildMessageState::Rejected => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "child_message": {
                                    "state": "rejected",
                                    "message_id": existing.message_id,
                                }
                            }),
                        },
                        classification: NodeExecutorFailureClassification::Failed,
                        reason: existing
                            .rejection_reason
                            .clone()
                            .unwrap_or_else(|| String::from("rejected")),
                    }))
                }
                // A proposed message is an interrupted delivery: re-dispatch
                // through the idempotent create-once delivery boundary.
                ChildMessageState::Proposed => Ok(ExecutorPhaseResult::Effect {
                    events: Vec::new(),
                    effect: NodeExecutorEffect::DeliverChildMessage(DeliverChildMessageCommand {
                        parent_session_id: existing.parent_session_id,
                        child_session_id: existing.child_session_id,
                        message_id: existing.message_id,
                        idempotency_key: existing.idempotency_key,
                        sequence: existing.sequence,
                        content: existing.content,
                        content_hash: existing.content_hash,
                        classification: existing.classification,
                        expires_at_ms: existing.expires_at_ms,
                    }),
                }),
            };
        }

        // Cancellation before any delivery intent is a plain cancelled node.
        if input.cancel_requested {
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events: Vec::new(),
                    transition_variables: serde_json::json!({
                        "child_message": {"state": "cancelled"}
                    }),
                },
                classification: NodeExecutorFailureClassification::Cancelled,
                reason: String::from(REASON_CANCELLED),
            }));
        }

        let content_hash = ContentHash::digest(config.content.as_bytes());
        let proposed = NodeExecutorEventPayload::ChildMessageProposed(ChildMessageProposedEvent {
            message_id: message_id.clone(),
            idempotency_key: config.idempotency_key.clone(),
            parent_session_id: input.session_id.clone(),
            child_session_id: config.child_session_id.clone(),
            sequence,
            node_id: input.node_id.clone(),
            run_id: input.run_id.clone(),
            attempt: input.attempt,
            loop_iteration: input.loop_iteration,
            step: input.step,
            content: config.content.clone(),
            content_hash,
            artifact_references: config.artifact_references.clone(),
            classification: config.classification,
            expires_at_ms: config.expires_at_ms,
        });

        // Expiration before delivery resolves to a canonical rejection.
        if config
            .expires_at_ms
            .is_some_and(|expires_at| input.clock.now_ms >= expires_at)
        {
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events: vec![
                        proposed,
                        NodeExecutorEventPayload::ChildMessageRejected(ChildMessageRejectedEvent {
                            message_id,
                            child_session_id: config.child_session_id.clone(),
                            reason: String::from(REASON_EXPIRED),
                            detail: String::from("message expired before delivery"),
                        }),
                    ],
                    transition_variables: serde_json::json!({
                        "child_message": {"state": "expired"}
                    }),
                },
                classification: NodeExecutorFailureClassification::Expired,
                reason: String::from(REASON_EXPIRED),
            }));
        }

        // Consequential delivery goes through the idempotent child boundary.
        Ok(ExecutorPhaseResult::Effect {
            events: vec![proposed],
            effect: NodeExecutorEffect::DeliverChildMessage(DeliverChildMessageCommand {
                parent_session_id: input.session_id.clone(),
                child_session_id: config.child_session_id.clone(),
                message_id,
                idempotency_key: config.idempotency_key.clone(),
                sequence,
                content: config.content.clone(),
                content_hash,
                classification: config.classification,
                expires_at_ms: config.expires_at_ms,
            }),
        })
    }

    fn finalize(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
        receipt: &NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError> {
        let config = Self::config(input)?;
        let NodeExecutorEffectReceipt::ChildMessage(result) = receipt else {
            return Err(NodeExecutorError::InvalidTransition {
                detail: String::from("child message expected a delivery receipt"),
            });
        };
        let record = Self::proposed_record(state, config).ok_or(NodeExecutorError::Ambiguous {
            detail: String::from("child message receipt arrived without a canonical proposal"),
        })?;
        let message_id = record.message_id.clone();
        if result.delivered {
            Ok(NodeExecutorOutcome::Complete {
                step: NodeExecutorStep {
                    events: vec![NodeExecutorEventPayload::ChildMessageDelivered(
                        ChildMessageDeliveredEvent {
                            message_id: message_id.clone(),
                            child_session_id: config.child_session_id.clone(),
                            receipt: result.summary.clone(),
                        },
                    )],
                    transition_variables: serde_json::json!({
                        "child_message": {
                            "state": "delivered",
                            "message_id": message_id,
                        }
                    }),
                },
            })
        } else {
            Ok(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events: vec![NodeExecutorEventPayload::ChildMessageRejected(
                        ChildMessageRejectedEvent {
                            message_id,
                            child_session_id: config.child_session_id.clone(),
                            reason: result
                                .rejection_reason
                                .clone()
                                .unwrap_or_else(|| REASON_CHILD_UNAVAILABLE.to_owned()),
                            detail: result.summary.clone(),
                        },
                    )],
                    transition_variables: serde_json::json!({
                        "child_message": {"state": "rejected"}
                    }),
                },
                classification: NodeExecutorFailureClassification::Failed,
                reason: result
                    .rejection_reason
                    .clone()
                    .unwrap_or_else(|| REASON_CHILD_UNAVAILABLE.to_owned()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_executors::{
        ChildMessageConfig, NodeExecutorClock,
        events::ChildMessageClassification,
        ports::{
            ChildLifecycleView, ChildMessageReceipt, ChildSessionMessagePort,
            ChildSessionMessagePortError, NodeExecutorPorts,
        },
    };

    struct RecordingChildPort {
        deliveries: std::sync::Mutex<Vec<DeliverChildMessageCommand>>,
        delivered: bool,
    }

    impl ChildSessionMessagePort for RecordingChildPort {
        fn deliver_child_message(
            &self,
            command: DeliverChildMessageCommand,
        ) -> Result<ChildMessageReceipt, ChildSessionMessagePortError> {
            self.deliveries.lock().expect("lock").push(command);
            Ok(ChildMessageReceipt {
                delivered: self.delivered,
                summary: String::from("delivered"),
                rejection_reason: None,
            })
        }

        fn child_lifecycle(
            &self,
            _child_session_id: &str,
        ) -> Result<ChildLifecycleView, ChildSessionMessagePortError> {
            Ok(ChildLifecycleView {
                exists: true,
                active: true,
            })
        }
    }

    struct Ports {
        child: RecordingChildPort,
    }

    impl NodeExecutorPorts for Ports {
        fn child_messages(&self) -> &dyn ChildSessionMessagePort {
            &self.child
        }
        fn schedules(&self) -> &dyn crate::node_executors::ports::GraphSchedulePort {
            unreachable!("not used by child message tests")
        }
        fn delays(&self) -> &dyn crate::node_executors::ports::DurableDelayPort {
            unreachable!("not used by child message tests")
        }
    }

    fn input(content: &str) -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("parent-1"),
            run_id: String::from("run-1"),
            node_id: String::from("message"),
            attempt: 1,
            loop_iteration: 0,
            step: 4,
            executor_kind: NodeExecutorKind::ChildMessage,
            config: NodeExecutorConfig::ChildMessage(ChildMessageConfig {
                child_session_id: String::from("child-1"),
                idempotency_key: String::from("key-1"),
                content: content.to_owned(),
                artifact_references: Vec::new(),
                classification: ChildMessageClassification::Instruction,
                expires_at_ms: None,
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
    fn fresh_message_proposes_then_delivers_with_exact_identity() {
        let executor = ChildMessageExecutor;
        let mut state = NodeExecutorState::default();
        let ports = Ports {
            child: RecordingChildPort {
                deliveries: std::sync::Mutex::new(Vec::new()),
                delivered: true,
            },
        };
        let input = input("hello child");
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, effect } = phase else {
            panic!("expected delivery effect");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "child_agent.message_proposed");
        let NodeExecutorEffect::DeliverChildMessage(command) = &effect else {
            panic!("expected deliver command");
        };
        assert_eq!(command.child_session_id, "child-1");
        assert_eq!(command.idempotency_key, "key-1");
        // The dispatcher commits proposal events before performing the effect.
        for event in &events {
            state.apply(event, 10).expect("commit proposal");
        }
        let receipt = ports
            .child_messages()
            .deliver_child_message(command.clone())
            .expect("deliver");
        let outcome = executor
            .finalize(
                &input,
                &state,
                &NodeExecutorEffectReceipt::ChildMessage(receipt),
            )
            .expect("finalize");
        let NodeExecutorOutcome::Complete { step } = outcome else {
            panic!("expected complete");
        };
        assert_eq!(step.events.len(), 1);
        assert_eq!(step.events[0].event_type(), "child_agent.message_delivered");
        assert_eq!(
            step.transition_variables["child_message"]["state"],
            "delivered"
        );
    }

    #[test]
    fn expired_message_resolves_to_rejection_without_delivery() {
        let executor = ChildMessageExecutor;
        let state = NodeExecutorState::default();
        let mut input = input("hello child");
        input.clock = NodeExecutorClock {
            now_ms: 2_000_000_000_000,
        };
        match &mut input.config {
            NodeExecutorConfig::ChildMessage(config) => {
                config.expires_at_ms = Some(1_900_000_000_000);
            }
            _ => unreachable!(),
        }
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(outcome) = phase else {
            panic!("expected terminal outcome");
        };
        let NodeExecutorOutcome::Failed {
            step,
            classification,
            ..
        } = outcome
        else {
            panic!("expected failure");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Expired);
        assert_eq!(step.events.len(), 2);
        assert_eq!(step.events[1].event_type(), "child_agent.message_rejected");
    }

    #[test]
    fn cancelled_message_never_proposes_delivery() {
        let executor = ChildMessageExecutor;
        let state = NodeExecutorState::default();
        let mut input = input("hello child");
        input.cancel_requested = true;
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed { classification, .. }) = phase
        else {
            panic!("expected cancellation failure");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Cancelled);
    }

    #[test]
    fn conflicting_idempotency_key_fails_closed() {
        let executor = ChildMessageExecutor;
        let mut state = NodeExecutorState::default();
        let identity = input("hello child").identity();
        let proposed = NodeExecutorEventPayload::ChildMessageProposed(ChildMessageProposedEvent {
            message_id: identity.message_id("child-1", 1),
            idempotency_key: String::from("key-1"),
            parent_session_id: String::from("parent-1"),
            child_session_id: String::from("child-1"),
            sequence: 1,
            node_id: String::from("message"),
            run_id: String::from("run-1"),
            attempt: 1,
            loop_iteration: 0,
            step: 4,
            content: String::from("hello child"),
            content_hash: ContentHash::digest(b"hello child"),
            artifact_references: Vec::new(),
            classification: ChildMessageClassification::Instruction,
            expires_at_ms: None,
        });
        state.apply(&proposed, 10).expect("propose");
        let mut conflicting = input("different content");
        conflicting.config = NodeExecutorConfig::ChildMessage(ChildMessageConfig {
            child_session_id: String::from("child-1"),
            idempotency_key: String::from("key-1"),
            content: String::from("different content"),
            artifact_references: Vec::new(),
            classification: ChildMessageClassification::Instruction,
            expires_at_ms: None,
        });
        assert!(matches!(
            executor
                .prepare(&conflicting, &state)
                .expect_err("conflict"),
            NodeExecutorError::InvalidTransition { .. }
        ));
    }

    #[test]
    fn proposed_recovery_redelivers_through_idempotent_boundary() {
        let executor = ChildMessageExecutor;
        let mut state = NodeExecutorState::default();
        let identity = input("hello child").identity();
        let proposed = NodeExecutorEventPayload::ChildMessageProposed(ChildMessageProposedEvent {
            message_id: identity.message_id("child-1", 1),
            idempotency_key: String::from("key-1"),
            parent_session_id: String::from("parent-1"),
            child_session_id: String::from("child-1"),
            sequence: 1,
            node_id: String::from("message"),
            run_id: String::from("run-1"),
            attempt: 1,
            loop_iteration: 0,
            step: 4,
            content: String::from("hello child"),
            content_hash: ContentHash::digest(b"hello child"),
            artifact_references: Vec::new(),
            classification: ChildMessageClassification::Instruction,
            expires_at_ms: None,
        });
        state.apply(&proposed, 10).expect("propose");
        let phase = executor
            .prepare(&input("hello child"), &state)
            .expect("prepare");
        let ExecutorPhaseResult::Effect { events, effect } = phase else {
            panic!("expected redelivery effect");
        };
        assert!(events.is_empty());
        assert!(matches!(effect, NodeExecutorEffect::DeliverChildMessage(_)));
    }
}
