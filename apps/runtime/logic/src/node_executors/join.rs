//! Native `join_results` node executor.
//!
//! Implements a generic join over child sessions/branches with configuration
//! for required and optional participants, minimum successful results,
//! allowed failures, timeout, ordering, result projection, and artifact
//! collection. Canonical join state reconstructs expected/completed/failed/
//! cancelled/missing participants, readiness, terminal classification, and
//! exact collected result references. Cancellation propagates through the
//! node, and ambiguous replay positions fail closed without redispatch.

use crate::node_executors::{
    ExecutorPhaseResult, JoinConfig, MAX_JOIN_PARTICIPANTS, NativeNodeExecutor, NodeExecutorConfig,
    NodeExecutorEffectReceipt, NodeExecutorError, NodeExecutorFailureClassification,
    NodeExecutorInput, NodeExecutorKind, NodeExecutorOutcome, NodeExecutorStep, ParticipantOutcome,
    events::{
        JoinInitializedEvent, JoinParticipantCancelledEvent, JoinParticipantCompletedEvent,
        JoinParticipantFailedEvent, JoinReleasedEvent, JoinTerminalState, NodeExecutorEventPayload,
    },
    state::{JoinState, NodeExecutorState},
};

/// Native generic join executor.
#[derive(Clone, Debug, Default)]
pub struct JoinExecutor;

impl JoinExecutor {
    fn config(input: &NodeExecutorInput) -> Result<&JoinConfig, NodeExecutorError> {
        let NodeExecutorConfig::Join(config) = &input.config else {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        };
        Ok(config)
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "participant counts are bounded far below u32::MAX"
    )]
    fn validate(input: &NodeExecutorInput) -> Result<(), NodeExecutorError> {
        if input.executor_kind != NodeExecutorKind::Join
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
        if config.required_participants.len() + config.optional_participants.len()
            > MAX_JOIN_PARTICIPANTS
            || config.min_success
                > u32::try_from(config.required_participants.len()).unwrap_or(u32::MAX)
        {
            return Err(NodeExecutorError::BoundExceeded {
                detail: String::from("join participant or minimum-success bound exceeded"),
            });
        }
        let mut seen = std::collections::BTreeSet::new();
        for participant in config
            .required_participants
            .iter()
            .chain(config.optional_participants.iter())
        {
            if participant.trim().is_empty() || !seen.insert(participant) {
                return Err(NodeExecutorError::InvalidInput {
                    reason: String::from("join participants must be unique and non-empty"),
                });
            }
        }
        Ok(())
    }

    /// Folds caller-derived participant outcomes into canonical events.
    fn fold_outcomes(input: &NodeExecutorInput, join_id: &str) -> Vec<NodeExecutorEventPayload> {
        input
            .participant_outcomes
            .iter()
            .map(|outcome| match outcome {
                ParticipantOutcome::Completed {
                    participant,
                    result_references,
                    result_bytes,
                } => NodeExecutorEventPayload::JoinParticipantCompleted(
                    JoinParticipantCompletedEvent {
                        join_id: join_id.to_owned(),
                        participant_execution_id: participant.clone(),
                        result_references: result_references.clone(),
                        result_bytes: *result_bytes,
                    },
                ),
                ParticipantOutcome::Failed {
                    participant,
                    reason,
                } => NodeExecutorEventPayload::JoinParticipantFailed(JoinParticipantFailedEvent {
                    join_id: join_id.to_owned(),
                    participant_execution_id: participant.clone(),
                    reason: reason.clone(),
                }),
                ParticipantOutcome::Cancelled {
                    participant,
                    reason,
                } => NodeExecutorEventPayload::JoinParticipantCancelled(
                    JoinParticipantCancelledEvent {
                        join_id: join_id.to_owned(),
                        participant_execution_id: participant.clone(),
                        reason: reason.clone(),
                    },
                ),
            })
            .collect()
    }

    /// Deterministic readiness decision from the reconstructed join state.
    fn readiness(
        join: &crate::node_executors::state::JoinRecord,
        now_ms: i64,
        cancel_requested: bool,
    ) -> Readiness {
        if cancel_requested {
            return Readiness::Terminal(JoinTerminalState::Cancelled, "cancelled".to_owned());
        }
        if let Some(timeout) = join.timeout_ms {
            let elapsed = now_ms.saturating_sub(join.initialized_at_ms);
            let timeout = i64::try_from(timeout).unwrap_or(i64::MAX);
            if elapsed >= timeout
                && join.completed_participants.len() < join.expected_participants.len()
            {
                return Readiness::Terminal(JoinTerminalState::TimedOut, "join timeout".to_owned());
            }
        }
        let failures = join.failed_participants.len() + join.cancelled_participants.len();
        if failures > join.allowed_failures as usize {
            return Readiness::Terminal(
                JoinTerminalState::Failed,
                format!(
                    "{failures} failures exceed allowed {}",
                    join.allowed_failures
                ),
            );
        }
        let all_required_resolved = join.expected_participants.iter().all(|participant| {
            join.completed_participants.contains(participant)
                || join.failed_participants.contains(participant)
                || join.cancelled_participants.contains(participant)
        });
        if join.completed_participants.len() >= join.min_success as usize && all_required_resolved {
            return Readiness::Terminal(JoinTerminalState::Success, "join complete".to_owned());
        }
        Readiness::Awaiting
    }

    /// Exact collected result references retained by the reducer.
    fn collected_references(join: &crate::node_executors::state::JoinRecord) -> Vec<String> {
        // The reducer retains per-participant references in canonical
        // completion order; the ordering policy selects how the integration
        // projection presents them, and the references themselves are exact.
        let _ = join.ordering;
        join.collected_result_references.clone()
    }
}

/// Deterministic join readiness decision.
enum Readiness {
    /// The join is not terminal yet.
    Awaiting,
    /// The join releases with the given terminal state and reason.
    Terminal(JoinTerminalState, String),
}

impl NativeNodeExecutor for JoinExecutor {
    fn kind(&self) -> NodeExecutorKind {
        NodeExecutorKind::Join
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the generic-join state machine keeps initialization, outcome folding, readiness, and release adjacent"
    )]
    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError> {
        Self::validate(input)?;
        let config = Self::config(input)?;
        let identity = input.identity();
        let join_id = identity.join_id();

        // Replay: a terminal join resolves to its stored outcome.
        if let Some(join) = state.joins.get(&join_id)
            && join.is_terminal()
        {
            let terminal = join.state;
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                step: NodeExecutorStep {
                    events: Vec::new(),
                    transition_variables: serde_json::json!({
                        "join": {
                            "state": match terminal {
                                JoinState::Success => "success",
                                JoinState::Failed => "failed",
                                JoinState::TimedOut => "timed_out",
                                JoinState::Cancelled => "cancelled",
                                JoinState::Awaiting => "awaiting",
                            },
                            "collected": join.collected_result_references,
                        }
                    }),
                },
            }));
        }

        let mut events = Vec::new();
        if !state.joins.contains_key(&join_id) {
            events.push(NodeExecutorEventPayload::JoinInitialized(
                JoinInitializedEvent {
                    join_id: join_id.clone(),
                    node_id: input.node_id.clone(),
                    run_id: input.run_id.clone(),
                    attempt: input.attempt,
                    loop_iteration: input.loop_iteration,
                    step: input.step,
                    initialized_at_ms: input.clock.now_ms,
                    expected_participants: config.required_participants.clone(),
                    optional_participants: config.optional_participants.clone(),
                    min_success: config.min_success,
                    allowed_failures: config.allowed_failures,
                    timeout_ms: config.timeout_ms,
                    ordering: config.ordering,
                    result_projection: config.result_projection,
                    artifact_collection: config.artifact_collection,
                },
            ));
        }

        // Fold caller-derived participant outcomes into canonical events.
        let folded = Self::fold_outcomes(input, &join_id);
        events.extend(folded);

        // Reconstruct the join after the generated events to compute
        // readiness deterministically.
        let mut projected = state.clone();
        for (index, event) in events.iter().enumerate() {
            projected
                .apply(event, u64::MAX - index as u64)
                .map_err(|error| NodeExecutorError::InvalidTransition {
                    detail: format!("join reducer rejected folded outcome: {error}"),
                })?;
        }
        let join = projected
            .joins
            .get(&join_id)
            .ok_or(NodeExecutorError::Ambiguous {
                detail: String::from("join state is missing after initialization"),
            })?;

        match Self::readiness(join, input.clock.now_ms, input.cancel_requested) {
            Readiness::Terminal(terminal_state, reason) => {
                let state = match terminal_state {
                    JoinTerminalState::Success => "success",
                    JoinTerminalState::Failed => "failed",
                    JoinTerminalState::TimedOut => "timed_out",
                    JoinTerminalState::Cancelled => "cancelled",
                };
                events.push(NodeExecutorEventPayload::JoinReleased(JoinReleasedEvent {
                    join_id: join_id.clone(),
                    state: terminal_state,
                    collected_result_references: Self::collected_references(join),
                    missing_participants: join.missing().into_iter().collect(),
                    reason: reason.clone(),
                }));
                let classification = match terminal_state {
                    JoinTerminalState::Success => None,
                    JoinTerminalState::Failed => Some(NodeExecutorFailureClassification::Failed),
                    JoinTerminalState::TimedOut => {
                        Some(NodeExecutorFailureClassification::TimedOut)
                    }
                    JoinTerminalState::Cancelled => {
                        Some(NodeExecutorFailureClassification::Cancelled)
                    }
                };
                let step = NodeExecutorStep {
                    events,
                    transition_variables: serde_json::json!({
                        "join": {"state": state, "reason": reason}
                    }),
                };
                Ok(ExecutorPhaseResult::Done(match classification {
                    Some(classification) => NodeExecutorOutcome::Failed {
                        step,
                        classification,
                        reason,
                    },
                    None => NodeExecutorOutcome::Complete { step },
                }))
            }
            Readiness::Awaiting => Ok(ExecutorPhaseResult::Await {
                events,
                reason: format!(
                    "awaiting {} of {} participants",
                    join.completed_participants.len(),
                    join.expected_participants.len()
                ),
            }),
        }
    }

    fn finalize(
        &self,
        _input: &NodeExecutorInput,
        _state: &NodeExecutorState,
        _receipt: &NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError> {
        // Joins never request external effects.
        Err(NodeExecutorError::InvalidTransition {
            detail: String::from("join executor requested no effect"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_executors::{
        NodeExecutorClock,
        events::{JoinArtifactCollection, JoinOrdering, JoinProjection},
    };

    fn input(required: &[&str], min_success: u32, allowed_failures: u32) -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("join"),
            attempt: 1,
            loop_iteration: 0,
            step: 6,
            executor_kind: NodeExecutorKind::Join,
            config: NodeExecutorConfig::Join(JoinConfig {
                required_participants: required.iter().map(|value| (*value).to_owned()).collect(),
                optional_participants: Vec::new(),
                min_success,
                allowed_failures,
                timeout_ms: None,
                ordering: JoinOrdering::DeclarationOrder,
                result_projection: JoinProjection::TypedResults,
                artifact_collection: JoinArtifactCollection::Bounded,
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

    fn completed(participant: &str) -> ParticipantOutcome {
        ParticipantOutcome::Completed {
            participant: participant.to_owned(),
            result_references: vec![format!("{participant}:result")],
            result_bytes: 8,
        }
    }

    #[test]
    fn join_awaits_then_releases_success_exactly_when_ready() {
        let executor = JoinExecutor;
        let state = NodeExecutorState::default();
        let mut input = input(&["child-1", "child-2"], 2, 0);

        let phase = executor.prepare(&input, &state).expect("first prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        assert_eq!(events[0].event_type(), "child_agent.join_initialized");
        // Commit the init, then re-enter with one completion: still awaiting.
        let mut state = state.clone();
        state.apply(&events[0], 10).expect("init commit");
        input.participant_outcomes = vec![completed("child-1")];
        let phase = executor.prepare(&input, &state).expect("second prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].event_type(),
            "child_agent.join_participant_completed"
        );
        // Commit the completion and re-enter with the second: releases success.
        for event in &events {
            state.apply(event, 11).expect("completion commit");
        }
        input.participant_outcomes = vec![completed("child-2")];
        let phase = executor.prepare(&input, &state).expect("third prepare");
        let ExecutorPhaseResult::Done(outcome) = phase else {
            panic!("expected done");
        };
        let NodeExecutorOutcome::Complete { step } = outcome else {
            panic!("expected success completion");
        };
        assert_eq!(step.events.len(), 2);
        assert_eq!(
            step.events[0].event_type(),
            "child_agent.join_participant_completed"
        );
        assert_eq!(step.events[1].event_type(), "child_agent.join_released");
        assert_eq!(step.transition_variables["join"]["state"], "success");
    }

    #[test]
    fn join_failure_exceeding_allowance_releases_failed() {
        let executor = JoinExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(&["child-1", "child-2"], 2, 1);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        for event in &events {
            state.apply(event, 10).expect("init commit");
        }
        input.participant_outcomes = vec![
            ParticipantOutcome::Failed {
                participant: String::from("child-1"),
                reason: String::from("tool failed"),
            },
            ParticipantOutcome::Failed {
                participant: String::from("child-2"),
                reason: String::from("tool failed"),
            },
        ];
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
            classification,
            step,
            ..
        }) = phase
        else {
            panic!("expected failure");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Failed);
        assert_eq!(step.transition_variables["join"]["state"], "failed");
    }

    #[test]
    fn join_cancellation_propagates_to_a_released_cancellation() {
        let executor = JoinExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(&["child-1"], 1, 0);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        for event in &events {
            state.apply(event, 10).expect("init commit");
        }
        input.cancel_requested = true;
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed { classification, .. }) = phase
        else {
            panic!("expected cancellation");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Cancelled);
    }

    #[test]
    fn join_timeout_releases_timed_out_with_missing_participants() {
        let executor = JoinExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(&["child-1", "child-2"], 2, 0);
        input.config = NodeExecutorConfig::Join(JoinConfig {
            required_participants: vec![String::from("child-1"), String::from("child-2")],
            optional_participants: Vec::new(),
            min_success: 2,
            allowed_failures: 0,
            timeout_ms: Some(1_000),
            ordering: JoinOrdering::DeclarationOrder,
            result_projection: JoinProjection::TypedResults,
            artifact_collection: JoinArtifactCollection::None,
        });
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        for event in &events {
            state.apply(event, 10).expect("init commit");
        }
        // Advance the clock past the timeout and re-enter with one completion.
        input.clock = NodeExecutorClock {
            now_ms: 1_700_000_001_500,
        };
        input.participant_outcomes = vec![completed("child-1")];
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
            classification,
            step,
            ..
        }) = phase
        else {
            panic!("expected timeout");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::TimedOut);
        let released = step.events.last().expect("released");
        let NodeExecutorEventPayload::JoinReleased(released) = released else {
            panic!("expected join release");
        };
        assert_eq!(released.state, JoinTerminalState::TimedOut);
        assert_eq!(released.missing_participants, ["child-2"]);
    }
}
