//! Native `schedule` node executor.
//!
//! Supports graph-owned creation of one-time schedules, recurring schedules
//! where permitted, runtime-event triggers, exact process-output triggers,
//! and continuation wakeups. Schedule creation is consequential: it traverses
//! the normal proposal -> policy -> dispatch -> result path, binds the exact
//! style, workspace, permission, provider, model, and budget information, and
//! commits canonical proposed/created/rejected/removed events. The schedule
//! identity is deterministic from the idempotency key so the create-once
//! scheduler port is replay-safe.

use crate::node_executors::{
    ExecutorPhaseResult, MAX_SCHEDULE_BINDING_BYTES, NativeNodeExecutor, NodeExecutorConfig,
    NodeExecutorEffect, NodeExecutorEffectReceipt, NodeExecutorError,
    NodeExecutorFailureClassification, NodeExecutorInput, NodeExecutorKind, NodeExecutorOutcome,
    NodeExecutorStep, ScheduleConfig,
    events::{
        GraphScheduleCreatedEvent, GraphScheduleProposedEvent, GraphScheduleRemovedEvent,
        NodeExecutorEventPayload,
    },
    ports::UpsertGraphScheduleCommand,
    state::{GraphScheduleState, NodeExecutorState},
};

/// Native graph-owned schedule executor.
#[derive(Clone, Debug, Default)]
pub struct ScheduleExecutor;

impl ScheduleExecutor {
    fn config(input: &NodeExecutorInput) -> Result<&ScheduleConfig, NodeExecutorError> {
        let NodeExecutorConfig::Schedule(config) = &input.config else {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        };
        Ok(config)
    }

    fn validate(input: &NodeExecutorInput) -> Result<(), NodeExecutorError> {
        if input.executor_kind != NodeExecutorKind::Schedule
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
        if config.idempotency_key.trim().is_empty()
            || config.style.trim().is_empty()
            || config.workspace.trim().is_empty()
            || config.permission_policy.trim().is_empty()
            || config.provider.trim().is_empty()
            || config.model.trim().is_empty()
            || config.token_budget == 0
        {
            return Err(NodeExecutorError::InvalidInput {
                reason: String::from("schedule bindings are incomplete"),
            });
        }
        for binding in [
            &config.style,
            &config.workspace,
            &config.permission_policy,
            &config.provider,
            &config.model,
        ] {
            if binding.len() > MAX_SCHEDULE_BINDING_BYTES {
                return Err(NodeExecutorError::BoundExceeded {
                    detail: String::from("schedule binding exceeds its hard bound"),
                });
            }
        }
        match &config.trigger {
            crate::node_executors::events::GraphScheduleTrigger::AtMillis { wake_time_ms }
                if *wake_time_ms >= 0 => {}
            crate::node_executors::events::GraphScheduleTrigger::Interval {
                starts_at_ms,
                every_ms,
            } if *starts_at_ms >= 0
                && *every_ms >= crate::node_executors::MIN_RECURRING_INTERVAL_MS => {}
            crate::node_executors::events::GraphScheduleTrigger::RuntimeEvent { event_type }
                if !event_type.trim().is_empty() && event_type.len() <= 256 => {}
            crate::node_executors::events::GraphScheduleTrigger::ProcessOutput {
                process_id,
                contains,
            } if !process_id.trim().is_empty()
                && !contains.is_empty()
                && contains.len() <= 4_096 => {}
            crate::node_executors::events::GraphScheduleTrigger::Continuation {
                continuation_id,
            } if !continuation_id.trim().is_empty() => {}
            _ => {
                return Err(NodeExecutorError::InvalidInput {
                    reason: String::from("schedule trigger is invalid"),
                });
            }
        }
        Ok(())
    }
}

impl NativeNodeExecutor for ScheduleExecutor {
    fn kind(&self) -> NodeExecutorKind {
        NodeExecutorKind::Schedule
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the graph-owned schedule state machine keeps removal, replay resolution, and proposal creation adjacent"
    )]
    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError> {
        Self::validate(input)?;
        let config = Self::config(input)?;
        let identity = input.identity();
        let schedule_id = identity.schedule_id(&config.idempotency_key);

        // Schedule removal targets an active schedule owned by this node.
        if input.remove_requested {
            match state.schedules.get(&schedule_id) {
                Some(schedule) if schedule.state == GraphScheduleState::Active => {
                    return Ok(ExecutorPhaseResult::Effect {
                        events: Vec::new(),
                        effect: NodeExecutorEffect::RemoveSchedule { schedule_id },
                    });
                }
                Some(schedule) if schedule.state == GraphScheduleState::Removed => {
                    return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "schedule": {"state": "removed", "schedule_id": schedule_id}
                            }),
                        },
                    }));
                }
                _ => {
                    return Err(NodeExecutorError::InvalidInput {
                        reason: String::from("cannot remove a schedule that is not active"),
                    });
                }
            }
        }

        // Replay: an existing schedule resolves to its canonical lifecycle.
        if let Some(schedule) = state.schedules.get(&schedule_id) {
            return match schedule.state {
                GraphScheduleState::Active => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "schedule": {"state": "active", "schedule_id": schedule_id}
                            }),
                        },
                    }))
                }
                GraphScheduleState::Rejected => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "schedule": {"state": "rejected"}
                            }),
                        },
                        classification: NodeExecutorFailureClassification::Rejected,
                        reason: schedule
                            .terminal_reason
                            .clone()
                            .unwrap_or_else(|| String::from("policy rejected schedule")),
                    }))
                }
                GraphScheduleState::Removed => {
                    Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "schedule": {"state": "removed", "schedule_id": schedule_id}
                            }),
                        },
                    }))
                }
                // Proposed: an interrupted proposal resumes through the
                // idempotent create-once scheduler port.
                GraphScheduleState::Proposed => {
                    let schedule = schedule.clone();
                    Ok(ExecutorPhaseResult::Effect {
                        events: Vec::new(),
                        effect: NodeExecutorEffect::UpsertSchedule(UpsertGraphScheduleCommand {
                            schedule_id: schedule.schedule_id,
                            session_id: schedule.session_id,
                            idempotency_key: schedule.idempotency_key,
                            trigger: schedule.trigger,
                            style: schedule.style,
                            workspace: schedule.workspace,
                            permission_policy: schedule.permission_policy,
                            provider: schedule.provider,
                            model: schedule.model,
                            token_budget: schedule.token_budget,
                            cost_budget_micros: schedule.cost_budget_micros,
                        }),
                    })
                }
            };
        }

        // Cancellation before creation is a plain cancelled node.
        if input.cancel_requested {
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events: Vec::new(),
                    transition_variables: serde_json::json!({
                        "schedule": {"state": "cancelled"}
                    }),
                },
                classification: NodeExecutorFailureClassification::Cancelled,
                reason: String::from("cancelled before schedule creation"),
            }));
        }

        // Schedule removal is requested for an active schedule owned by this
        // node identity; the dispatch event precedes the removal effect.
        if input.remove_requested {
            return Ok(ExecutorPhaseResult::Effect {
                events: Vec::new(),
                effect: NodeExecutorEffect::RemoveSchedule { schedule_id },
            });
        }

        // Consequential creation: proposal event first, then the idempotent
        // scheduler upsert after policy approval.
        let proposed =
            NodeExecutorEventPayload::GraphScheduleProposed(GraphScheduleProposedEvent {
                schedule_id: schedule_id.clone(),
                node_id: input.node_id.clone(),
                run_id: input.run_id.clone(),
                attempt: input.attempt,
                loop_iteration: input.loop_iteration,
                step: input.step,
                session_id: input.session_id.clone(),
                idempotency_key: config.idempotency_key.clone(),
                trigger: config.trigger.clone(),
                style: config.style.clone(),
                workspace: config.workspace.clone(),
                permission_policy: config.permission_policy.clone(),
                provider: config.provider.clone(),
                model: config.model.clone(),
                token_budget: config.token_budget,
                cost_budget_micros: config.cost_budget_micros,
            });
        let effect = NodeExecutorEffect::UpsertSchedule(UpsertGraphScheduleCommand {
            schedule_id: schedule_id.clone(),
            session_id: input.session_id.clone(),
            idempotency_key: config.idempotency_key.clone(),
            trigger: config.trigger.clone(),
            style: config.style.clone(),
            workspace: config.workspace.clone(),
            permission_policy: config.permission_policy.clone(),
            provider: config.provider.clone(),
            model: config.model.clone(),
            token_budget: config.token_budget,
            cost_budget_micros: config.cost_budget_micros,
        });
        Ok(ExecutorPhaseResult::Effect {
            events: vec![proposed],
            effect,
        })
    }

    fn finalize(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
        receipt: &NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError> {
        let config = Self::config(input)?;
        let identity = input.identity();
        let schedule_id = identity.schedule_id(&config.idempotency_key);
        let schedule = state
            .schedules
            .get(&schedule_id)
            .ok_or(NodeExecutorError::Ambiguous {
                detail: String::from("schedule state missing at finalize"),
            })?
            .clone();
        match receipt {
            NodeExecutorEffectReceipt::Schedule(store) => {
                let created =
                    NodeExecutorEventPayload::GraphScheduleCreated(GraphScheduleCreatedEvent {
                        schedule_id: schedule.schedule_id.clone(),
                        node_id: schedule.node_id.clone(),
                        run_id: schedule.run_id.clone(),
                        attempt: schedule.attempt,
                        loop_iteration: schedule.loop_iteration,
                        step: schedule.step,
                        session_id: schedule.session_id.clone(),
                        idempotency_key: schedule.idempotency_key.clone(),
                        trigger: schedule.trigger.clone(),
                        style: schedule.style.clone(),
                        workspace: schedule.workspace.clone(),
                        permission_policy: schedule.permission_policy.clone(),
                        provider: schedule.provider.clone(),
                        model: schedule.model.clone(),
                        token_budget: schedule.token_budget,
                        cost_budget_micros: schedule.cost_budget_micros,
                    });
                Ok(NodeExecutorOutcome::Complete {
                    step: NodeExecutorStep {
                        events: vec![created],
                        transition_variables: serde_json::json!({
                            "schedule": {
                                "state": "active",
                                "schedule_id": schedule.schedule_id,
                                "replayed": store.replayed,
                            }
                        }),
                    },
                })
            }
            NodeExecutorEffectReceipt::ScheduleRemoved(removed) => {
                let reason = if *removed {
                    String::from("removed")
                } else {
                    String::from("removed (schedule already terminal)")
                };
                Ok(NodeExecutorOutcome::Complete {
                    step: NodeExecutorStep {
                        events: vec![NodeExecutorEventPayload::GraphScheduleRemoved(
                            GraphScheduleRemovedEvent {
                                schedule_id: schedule.schedule_id.clone(),
                                node_id: schedule.node_id.clone(),
                                reason: reason.clone(),
                            },
                        )],
                        transition_variables: serde_json::json!({
                            "schedule": {"state": "removed", "schedule_id": schedule.schedule_id}
                        }),
                    },
                })
            }
            _ => Err(NodeExecutorError::InvalidTransition {
                detail: String::from("schedule executor received an unexpected receipt"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_executors::{
        NodeExecutorClock, NodeExecutorEffectReceipt, events::GraphScheduleTrigger,
        ports::ScheduleStoreReceipt,
    };

    fn input() -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("schedule"),
            attempt: 1,
            loop_iteration: 0,
            step: 7,
            executor_kind: NodeExecutorKind::Schedule,
            config: NodeExecutorConfig::Schedule(ScheduleConfig {
                idempotency_key: String::from("key-1"),
                trigger: GraphScheduleTrigger::AtMillis {
                    wake_time_ms: 1_700_000_000_000,
                },
                style: String::from("persistent-chat@1.1.0"),
                workspace: String::from("workspace"),
                permission_policy: String::from("ask"),
                provider: String::from("mock"),
                model: String::from("fixture"),
                token_budget: 1_000,
                cost_budget_micros: 100,
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
    fn schedule_proposal_effect_creation_binds_exact_information() {
        let executor = ScheduleExecutor;
        let state = NodeExecutorState::default();
        let input = input();
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, effect } = phase else {
            panic!("expected upsert effect");
        };
        assert_eq!(events[0].event_type(), "schedule.graph_proposed");
        let NodeExecutorEventPayload::GraphScheduleProposed(proposed) = &events[0] else {
            panic!("expected proposal");
        };
        assert_eq!(proposed.style, "persistent-chat@1.1.0");
        assert_eq!(proposed.provider, "mock");
        assert_eq!(proposed.model, "fixture");
        assert_eq!(proposed.token_budget, 1_000);
        let NodeExecutorEffect::UpsertSchedule(command) = &effect else {
            panic!("expected upsert");
        };
        assert_eq!(command.schedule_id, input.identity().schedule_id("key-1"));
    }

    #[test]
    fn schedule_created_after_policy_approval_and_removal_path() {
        let executor = ScheduleExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input();
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, .. } = phase else {
            panic!("expected upsert effect");
        };
        state.apply(&events[0], 30).expect("commit proposal");
        let outcome = executor
            .finalize(
                &input,
                &state,
                &NodeExecutorEffectReceipt::Schedule(ScheduleStoreReceipt {
                    schedule_id: input.identity().schedule_id("key-1"),
                    replayed: false,
                }),
            )
            .expect("finalize create");
        let NodeExecutorOutcome::Complete { step } = outcome else {
            panic!("expected creation");
        };
        assert_eq!(step.events[0].event_type(), "schedule.graph_created");
        state.apply(&step.events[0], 31).expect("commit created");
        // Removal is requested on a later re-entry.
        input.remove_requested = true;
        let phase = executor.prepare(&input, &state).expect("prepare removal");
        let ExecutorPhaseResult::Effect { effect, .. } = phase else {
            panic!("expected removal effect");
        };
        assert!(matches!(effect, NodeExecutorEffect::RemoveSchedule { .. }));
        let outcome = executor
            .finalize(
                &input,
                &state,
                &NodeExecutorEffectReceipt::ScheduleRemoved(true),
            )
            .expect("finalize removal");
        let NodeExecutorOutcome::Complete { step } = outcome else {
            panic!("expected removal");
        };
        assert_eq!(step.events[0].event_type(), "schedule.graph_removed");
    }

    #[test]
    fn invalid_recurring_interval_is_rejected() {
        let executor = ScheduleExecutor;
        let state = NodeExecutorState::default();
        let mut input = input();
        match &mut input.config {
            NodeExecutorConfig::Schedule(config) => {
                config.trigger = GraphScheduleTrigger::Interval {
                    starts_at_ms: 1_700_000_000_000,
                    every_ms: 500, // below the minimum recurring interval
                };
            }
            _ => unreachable!(),
        }
        assert!(matches!(
            executor.prepare(&input, &state).expect_err("interval"),
            NodeExecutorError::InvalidInput { .. }
        ));
    }

    #[test]
    fn proposed_recovery_redoes_idempotent_upsert() {
        let executor = ScheduleExecutor;
        let mut state = NodeExecutorState::default();
        let input = input();
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Effect { events, .. } = phase else {
            panic!("expected upsert effect");
        };
        state.apply(&events[0], 30).expect("commit proposal");
        // Restart: the proposal is canonical but the upsert has no created
        // event; the create-once upsert is replayed safely.
        let phase = executor.prepare(&input, &state).expect("recovery prepare");
        let ExecutorPhaseResult::Effect { events, effect } = phase else {
            panic!("expected recovery upsert");
        };
        assert!(events.is_empty());
        assert!(matches!(effect, NodeExecutorEffect::UpsertSchedule(_)));
    }
}
