//! Native `delay` node executor.
//!
//! Uses the existing scheduler and continuation systems through the durable
//! delay port. A delay node resolves and canonically records the exact wake
//! time once, creates a durable continuation bound to session/run/node/
//! transition, survives runtime restart, resumes exactly once, supports
//! cancellation and expiration, and avoids duplicate scheduler occurrences.
//! The continuation identity is deterministic so the create-once port is
//! idempotent across restart; the wake claim is resume-once.

use crate::node_executors::{
    DelayConfig, ExecutorPhaseResult, MAX_DELAY_MILLIS, NativeNodeExecutor, NodeExecutorConfig,
    NodeExecutorEffect, NodeExecutorEffectReceipt, NodeExecutorError,
    NodeExecutorFailureClassification, NodeExecutorInput, NodeExecutorKind, NodeExecutorOutcome,
    NodeExecutorStep,
    events::{
        DelayCancelledEvent, DelayExpiredEvent, DelayResumedEvent, DelayScheduledEvent,
        NodeExecutorEventPayload,
    },
    ports::{
        CancelDelayContinuationCommand, ClaimDelayWakeCommand, CreateDelayContinuationCommand,
    },
    state::{DelayState, NodeExecutorState},
};

/// Native durable delay executor.
#[derive(Clone, Debug, Default)]
pub struct DelayExecutor;

impl DelayExecutor {
    fn config(input: &NodeExecutorInput) -> Result<&DelayConfig, NodeExecutorError> {
        let NodeExecutorConfig::Delay(config) = &input.config else {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        };
        Ok(config)
    }

    fn validate(input: &NodeExecutorInput) -> Result<(), NodeExecutorError> {
        if input.executor_kind != NodeExecutorKind::Delay
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
        if config.duration_ms <= 0 || config.duration_ms > MAX_DELAY_MILLIS {
            return Err(NodeExecutorError::BoundExceeded {
                detail: String::from("delay duration is outside its hard bound"),
            });
        }
        Ok(())
    }
}

impl NativeNodeExecutor for DelayExecutor {
    fn kind(&self) -> NodeExecutorKind {
        NodeExecutorKind::Delay
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the durable-delay state machine keeps scheduling, restart recovery, wake claims, expiry, and cancellation adjacent"
    )]
    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError> {
        Self::validate(input)?;
        let config = Self::config(input)?;
        let identity = input.identity();
        let delay_id = identity.delay_id();

        // Replay: terminal delays resolve to their stored outcome.
        if let Some(delay) = state.delays.get(&delay_id)
            && delay.is_terminal()
        {
            return match delay.state {
                DelayState::Resumed => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "delay": {"state": "resumed", "wake_time_ms": delay.wake_time_ms}
                            }),
                        },
                    }))
                }
                DelayState::Cancelled => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "delay": {"state": "cancelled"}
                            }),
                        },
                        classification: NodeExecutorFailureClassification::Cancelled,
                        reason: delay
                            .terminal_reason
                            .clone()
                            .unwrap_or_else(|| String::from("cancelled")),
                    }))
                }
                DelayState::Expired => Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                    step: NodeExecutorStep {
                        events: Vec::new(),
                        transition_variables: serde_json::json!({
                            "delay": {"state": "expired"}
                        }),
                    },
                    classification: NodeExecutorFailureClassification::Expired,
                    reason: delay
                        .terminal_reason
                        .clone()
                        .unwrap_or_else(|| String::from("expired")),
                })),
                DelayState::Pending => unreachable!("terminal check excludes pending"),
            };
        }

        // First entry: resolve the exact wake time once and record it before
        // the idempotent continuation creation.
        if !state.delays.contains_key(&delay_id) {
            if input.cancel_requested {
                return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                    step: NodeExecutorStep {
                        events: Vec::new(),
                        transition_variables: serde_json::json!({
                            "delay": {"state": "cancelled"}
                        }),
                    },
                    classification: NodeExecutorFailureClassification::Cancelled,
                    reason: String::from("cancelled before scheduling"),
                }));
            }
            // An already-expired delay never schedules a continuation.
            if config
                .expires_at_ms
                .is_some_and(|expires_at| input.clock.now_ms >= expires_at)
            {
                return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                    step: NodeExecutorStep {
                        events: Vec::new(),
                        transition_variables: serde_json::json!({
                            "delay": {"state": "expired"}
                        }),
                    },
                    classification: NodeExecutorFailureClassification::Expired,
                    reason: String::from("delay expired before scheduling"),
                }));
            }
            let wake_time_ms = input.clock.now_ms.checked_add(config.duration_ms).ok_or(
                NodeExecutorError::BoundExceeded {
                    detail: String::from("delay wake time overflow"),
                },
            )?;
            let scheduled = NodeExecutorEventPayload::DelayScheduled(DelayScheduledEvent {
                delay_id: delay_id.clone(),
                node_id: input.node_id.clone(),
                run_id: input.run_id.clone(),
                attempt: input.attempt,
                loop_iteration: input.loop_iteration,
                step: input.step,
                session_id: input.session_id.clone(),
                wake_time_ms,
                continuation_id: identity.delay_continuation_id(),
                expires_at_ms: config.expires_at_ms,
            });
            let effect =
                NodeExecutorEffect::CreateDelayContinuation(CreateDelayContinuationCommand {
                    session_id: input.session_id.clone(),
                    continuation_id: identity.delay_continuation_id(),
                    wake_time_ms,
                    expires_at_ms: config.expires_at_ms,
                    node_id: input.node_id.clone(),
                });
            return Ok(ExecutorPhaseResult::Effect {
                events: vec![scheduled],
                effect,
            });
        }

        // Recovery: a pending delay with a wake claim resumes exactly once.
        let delay = state
            .delays
            .get(&delay_id)
            .ok_or(NodeExecutorError::Ambiguous {
                detail: String::from("delay state missing after scheduling"),
            })?
            .clone();
        if let Some(claim) = &input.wake_claim {
            if claim.continuation_id != delay.continuation_id
                || claim.wake_time_ms != delay.wake_time_ms
            {
                return Err(NodeExecutorError::InvalidTransition {
                    detail: String::from("delay wake claim does not match canonical state"),
                });
            }
            if delay.state != DelayState::Pending {
                // Another claim already won; the canonical state tells the truth.
                return match delay.state {
                    DelayState::Resumed => {
                        Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                            step: NodeExecutorStep {
                                events: Vec::new(),
                                transition_variables: serde_json::json!({
                                    "delay": {"state": "resumed"}
                                }),
                            },
                        }))
                    }
                    _ => Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "delay": {"state": "terminal"}
                            }),
                        },
                        classification: NodeExecutorFailureClassification::Cancelled,
                        reason: String::from("delay already terminal"),
                    })),
                };
            }
            return Ok(ExecutorPhaseResult::Effect {
                events: Vec::new(),
                effect: NodeExecutorEffect::ClaimDelayWake(ClaimDelayWakeCommand {
                    session_id: delay.session_id.clone(),
                    continuation_id: delay.continuation_id.clone(),
                    wake_time_ms: delay.wake_time_ms,
                }),
            });
        }

        // Cancellation of a pending delay cancels the durable continuation.
        if input.cancel_requested {
            return Ok(ExecutorPhaseResult::Effect {
                events: Vec::new(),
                effect: NodeExecutorEffect::CancelDelayContinuation(
                    CancelDelayContinuationCommand {
                        session_id: delay.session_id.clone(),
                        continuation_id: delay.continuation_id.clone(),
                    },
                ),
            });
        }

        // Expiration of a pending delay resolves to a canonical expiry without
        // dispatching a wake.
        if delay
            .expires_at_ms
            .is_some_and(|expires_at| input.clock.now_ms >= expires_at)
        {
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events: vec![NodeExecutorEventPayload::DelayExpired(DelayExpiredEvent {
                        delay_id: delay_id.clone(),
                        reason: String::from("delay expired before its wake"),
                    })],
                    transition_variables: serde_json::json!({
                        "delay": {"state": "expired"}
                    }),
                },
                classification: NodeExecutorFailureClassification::Expired,
                reason: String::from("expired"),
            }));
        }

        Ok(ExecutorPhaseResult::Await {
            events: Vec::new(),
            reason: format!("delay pending until {}", delay.wake_time_ms),
        })
    }

    fn finalize(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
        receipt: &NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError> {
        let identity = input.identity();
        let delay_id = identity.delay_id();
        let delay = state
            .delays
            .get(&delay_id)
            .ok_or(NodeExecutorError::Ambiguous {
                detail: String::from("delay state missing at finalize"),
            })?
            .clone();
        match receipt {
            NodeExecutorEffectReceipt::DelayCreated => {
                // The continuation exists; the node now awaits its wake.
                Ok(NodeExecutorOutcome::Awaiting {
                    step: NodeExecutorStep {
                        events: Vec::new(),
                        transition_variables: serde_json::json!({
                            "delay": {
                                "state": "pending",
                                "wake_time_ms": delay.wake_time_ms,
                            }
                        }),
                    },
                    reason: format!("delay pending until {}", delay.wake_time_ms),
                })
            }
            NodeExecutorEffectReceipt::DelayWake(result) => {
                if result.transitioned {
                    Ok(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: vec![NodeExecutorEventPayload::DelayResumed(
                                DelayResumedEvent {
                                    delay_id: delay_id.clone(),
                                    wake_time_ms: delay.wake_time_ms,
                                    proof: result.proof.clone(),
                                },
                            )],
                            transition_variables: serde_json::json!({
                                "delay": {
                                    "state": "resumed",
                                    "wake_time_ms": delay.wake_time_ms,
                                }
                            }),
                        },
                    })
                } else {
                    // Resume-once: another claim won; canonical state decides.
                    Ok(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "delay": {"state": "resumed"}
                            }),
                        },
                    })
                }
            }
            NodeExecutorEffectReceipt::DelayCancelled(removed) => {
                let reason = if *removed {
                    String::from("cancelled")
                } else {
                    String::from("cancelled (continuation already terminal)")
                };
                Ok(NodeExecutorOutcome::Failed {
                    step: NodeExecutorStep {
                        events: vec![NodeExecutorEventPayload::DelayCancelled(
                            DelayCancelledEvent {
                                delay_id: delay_id.clone(),
                                reason: reason.clone(),
                            },
                        )],
                        transition_variables: serde_json::json!({
                            "delay": {"state": "cancelled"}
                        }),
                    },
                    classification: NodeExecutorFailureClassification::Cancelled,
                    reason,
                })
            }
            _ => Err(NodeExecutorError::InvalidTransition {
                detail: String::from("delay executor received an unexpected receipt"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_executors::{
        NodeExecutorClock, NodeExecutorEffectReceipt, ports::DelayWakeResult,
    };

    fn input(duration_ms: i64) -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("wait"),
            attempt: 1,
            loop_iteration: 0,
            step: 5,
            executor_kind: NodeExecutorKind::Delay,
            config: NodeExecutorConfig::Delay(DelayConfig {
                duration_ms,
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
    fn delay_records_wake_time_once_then_resumes_exactly_once() {
        let executor = DelayExecutor;
        let mut state = NodeExecutorState::default();
        let input = input(5_000);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, effect } = phase else {
            panic!("expected create effect");
        };
        assert_eq!(events[0].event_type(), "delay.scheduled");
        let NodeExecutorEventPayload::DelayScheduled(scheduled) = &events[0] else {
            panic!("expected scheduled event");
        };
        assert_eq!(scheduled.wake_time_ms, 1_700_000_005_000);
        assert!(matches!(
            effect,
            NodeExecutorEffect::CreateDelayContinuation(_)
        ));
        // The dispatcher commits the scheduled event before the effect.
        state.apply(&events[0], 20).expect("commit schedule");
        let outcome = executor
            .finalize(&input, &state, &NodeExecutorEffectReceipt::DelayCreated)
            .expect("finalize create");
        let NodeExecutorOutcome::Awaiting { .. } = outcome else {
            panic!("expected awaiting after creation");
        };
        // Re-enter with a wake claim: resume exactly once.
        let mut claimed = input.clone();
        claimed.wake_claim = Some(ClaimDelayWakeCommand {
            session_id: String::from("session-1"),
            continuation_id: claimed.identity().delay_continuation_id(),
            wake_time_ms: 1_700_000_005_000,
        });
        let phase = executor.prepare(&claimed, &state).expect("prepare claim");
        let ExecutorPhaseResult::Effect { effect, .. } = phase else {
            panic!("expected claim effect");
        };
        assert!(matches!(effect, NodeExecutorEffect::ClaimDelayWake(_)));
        let outcome = executor
            .finalize(
                &claimed,
                &state,
                &NodeExecutorEffectReceipt::DelayWake(DelayWakeResult {
                    transitioned: true,
                    proof: String::from("scheduler.claim"),
                }),
            )
            .expect("finalize claim");
        let NodeExecutorOutcome::Complete { step } = outcome else {
            panic!("expected resume");
        };
        assert_eq!(step.events[0].event_type(), "delay.resumed");
        state.apply(&step.events[0], 21).expect("commit resume");
        // A second claim resolves to the canonical resumed state without
        // emitting another resume event.
        let phase = executor.prepare(&claimed, &state).expect("replay");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete { step }) = phase else {
            panic!("expected terminal replay");
        };
        assert!(step.events.is_empty());
    }

    #[test]
    fn pending_delay_expiry_resolves_without_wake_dispatch() {
        let executor = DelayExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(5_000);
        match &mut input.config {
            NodeExecutorConfig::Delay(config) => {
                config.expires_at_ms = Some(1_700_000_003_000);
            }
            _ => unreachable!(),
        }
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, .. } = phase else {
            panic!("expected create effect");
        };
        state.apply(&events[0], 20).expect("commit schedule");
        input.clock = NodeExecutorClock {
            now_ms: 1_700_000_004_000,
        };
        let phase = executor.prepare(&input, &state).expect("prepare expiry");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
            classification,
            step,
            ..
        }) = phase
        else {
            panic!("expected expiry");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Expired);
        assert_eq!(step.events[0].event_type(), "delay.expired");
    }

    #[test]
    fn pending_delay_cancellation_cancels_the_continuation() {
        let executor = DelayExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(5_000);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, .. } = phase else {
            panic!("expected create effect");
        };
        state.apply(&events[0], 20).expect("commit schedule");
        input.cancel_requested = true;
        let phase = executor.prepare(&input, &state).expect("prepare cancel");
        let ExecutorPhaseResult::Effect { effect, .. } = phase else {
            panic!("expected cancel effect");
        };
        assert!(matches!(
            effect,
            NodeExecutorEffect::CancelDelayContinuation(_)
        ));
        let outcome = executor
            .finalize(
                &input,
                &state,
                &NodeExecutorEffectReceipt::DelayCancelled(true),
            )
            .expect("finalize cancel");
        let NodeExecutorOutcome::Failed {
            classification,
            step,
            ..
        } = outcome
        else {
            panic!("expected cancellation");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Cancelled);
        assert_eq!(step.events[0].event_type(), "delay.cancelled");
    }
}
