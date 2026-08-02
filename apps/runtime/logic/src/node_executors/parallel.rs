//! Native `parallel_branch` node executor.
//!
//! Implements bounded parallel graph execution with stable branch IDs,
//! explicit maximum parallelism, deterministic readiness and dispatch order,
//! bounded queues, independent cancellation IDs, branch-local variable scope
//! enforcement, shared-write validation, a join policy, restart recovery, and
//! no duplicate branch effects. Parallel branches cannot write the same
//! canonical variable or workspace resource without an explicit merge policy
//! supplied by graph state; the executor fails closed otherwise.

use crate::node_executors::{
    ExecutorPhaseResult, MAX_PARALLEL_BRANCHES, MAX_PARALLELISM, MIN_PARALLELISM,
    NativeNodeExecutor, NodeExecutorConfig, NodeExecutorEffectReceipt, NodeExecutorError,
    NodeExecutorFailureClassification, NodeExecutorInput, NodeExecutorKind, NodeExecutorOutcome,
    NodeExecutorStep, ParallelBranchConfig, ParticipantOutcome,
    events::{
        NodeExecutorEventPayload, ParallelBranchFinishedEvent, ParallelBranchInitializedEvent,
        ParallelBranchMemberCancelledEvent, ParallelBranchMemberCompletedEvent,
        ParallelBranchMemberDispatchedEvent, ParallelBranchMemberFailedEvent,
        ParallelBranchTerminalState,
    },
    state::{NodeExecutorState, ParallelBranchState, ParallelMemberState},
};

/// Native parallel branch executor.
#[derive(Clone, Debug, Default)]
pub struct ParallelBranchExecutor;

impl ParallelBranchExecutor {
    fn config(input: &NodeExecutorInput) -> Result<&ParallelBranchConfig, NodeExecutorError> {
        let NodeExecutorConfig::ParallelBranch(config) = &input.config else {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        };
        Ok(config)
    }

    fn validate(input: &NodeExecutorInput) -> Result<(), NodeExecutorError> {
        if input.executor_kind != NodeExecutorKind::ParallelBranch
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
        if config.branch_ids.is_empty()
            || config.branch_ids.len() > MAX_PARALLEL_BRANCHES
            || !(MIN_PARALLELISM..=MAX_PARALLELISM).contains(&config.max_parallelism)
            || config.max_parallelism as usize > config.branch_ids.len()
        {
            return Err(NodeExecutorError::BoundExceeded {
                detail: String::from("parallel branch count or parallelism bound exceeded"),
            });
        }
        let mut seen = std::collections::BTreeSet::new();
        for branch in &config.branch_ids {
            if branch.trim().is_empty() || !seen.insert(branch) {
                return Err(NodeExecutorError::InvalidInput {
                    reason: String::from("parallel branch IDs must be unique and non-empty"),
                });
            }
        }
        // Shared writes require an explicit merge/serialization policy supplied
        // by graph state; without one the node fails closed.
        if !config.shared_write_scopes.is_empty() && config.merge_policy.is_none() {
            return Err(NodeExecutorError::InvalidInput {
                reason: String::from(
                    "parallel branches share canonical writes without a merge policy",
                ),
            });
        }
        Ok(())
    }

    /// Deterministic next dispatch batch: pending members in declaration
    /// order up to the maximum parallelism currently in flight.
    fn next_dispatch_batch(
        record: &crate::node_executors::state::ParallelBranchRecord,
    ) -> Vec<String> {
        let in_flight = record
            .member_states
            .values()
            .filter(|state| **state == ParallelMemberState::Dispatched)
            .count();
        let capacity = (record.max_parallelism as usize).saturating_sub(in_flight);
        if capacity == 0 {
            return Vec::new();
        }
        record
            .branch_ids
            .iter()
            .filter(|member| {
                matches!(
                    record.member_states.get(*member),
                    Some(ParallelMemberState::Pending)
                )
            })
            .take(capacity)
            .cloned()
            .collect()
    }

    fn fold_outcomes(input: &NodeExecutorInput, branch_id: &str) -> Vec<NodeExecutorEventPayload> {
        input
            .participant_outcomes
            .iter()
            .map(|outcome| match outcome {
                ParticipantOutcome::Completed {
                    participant,
                    result_references,
                    ..
                } => NodeExecutorEventPayload::ParallelBranchMemberCompleted(
                    ParallelBranchMemberCompletedEvent {
                        branch_id: branch_id.to_owned(),
                        member_id: participant.clone(),
                        result_references: result_references.clone(),
                    },
                ),
                ParticipantOutcome::Failed {
                    participant,
                    reason,
                } => NodeExecutorEventPayload::ParallelBranchMemberFailed(
                    ParallelBranchMemberFailedEvent {
                        branch_id: branch_id.to_owned(),
                        member_id: participant.clone(),
                        reason: reason.clone(),
                    },
                ),
                ParticipantOutcome::Cancelled {
                    participant,
                    reason,
                } => NodeExecutorEventPayload::ParallelBranchMemberCancelled(
                    ParallelBranchMemberCancelledEvent {
                        branch_id: branch_id.to_owned(),
                        member_id: participant.clone(),
                        reason: reason.clone(),
                    },
                ),
            })
            .collect()
    }
}

impl NativeNodeExecutor for ParallelBranchExecutor {
    fn kind(&self) -> NodeExecutorKind {
        NodeExecutorKind::ParallelBranch
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the parallel-branch state machine keeps recovery, dispatch ordering, cancellation, and terminal evaluation adjacent"
    )]
    fn prepare(
        &self,
        input: &NodeExecutorInput,
        state: &NodeExecutorState,
    ) -> Result<ExecutorPhaseResult, NodeExecutorError> {
        Self::validate(input)?;
        let config = Self::config(input)?;
        let identity = input.identity();
        let branch_id = identity.branch_id();

        // Replay: a terminal parallel node resolves to its stored outcome.
        if let Some(record) = state.parallel_branches.get(&branch_id)
            && record.is_terminal()
        {
            let terminal = record.state;
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete {
                step: NodeExecutorStep {
                    events: Vec::new(),
                    transition_variables: serde_json::json!({
                        "parallel": {
                            "state": match terminal {
                                ParallelBranchState::FinishedSuccess => "finished_success",
                                ParallelBranchState::FinishedFailure => "finished_failure",
                                ParallelBranchState::Cancelled => "cancelled",
                                ParallelBranchState::Running => "running",
                            },
                            "reason": record.terminal_reason,
                        }
                    }),
                },
            }));
        }

        // Restart recovery is applied below after folding caller-derived
        // outcomes: canonical member completions resolve dispatched members,
        // and explicit cancellation resolves them through cancellation IDs.
        let mut events = Vec::new();
        if !state.parallel_branches.contains_key(&branch_id) {
            events.push(NodeExecutorEventPayload::ParallelBranchInitialized(
                ParallelBranchInitializedEvent {
                    branch_id: branch_id.clone(),
                    node_id: input.node_id.clone(),
                    run_id: input.run_id.clone(),
                    attempt: input.attempt,
                    loop_iteration: input.loop_iteration,
                    step: input.step,
                    branch_ids: config.branch_ids.clone(),
                    max_parallelism: config.max_parallelism,
                    shared_write_scopes: config.shared_write_scopes.clone(),
                },
            ));
        }

        // Fold member outcomes first so dispatch capacity accounts for them.
        let folded = Self::fold_outcomes(input, &branch_id);
        events.extend(folded);

        // Project state through the generated events to compute the next
        // deterministic dispatch batch.
        let mut projected = state.clone();
        for (index, event) in events.iter().enumerate() {
            projected
                .apply(event, u64::MAX - index as u64)
                .map_err(|error| NodeExecutorError::InvalidTransition {
                    detail: format!("parallel reducer rejected folded outcome: {error}"),
                })?;
        }
        // Members still dispatched without a terminal outcome after folding
        // are externally uncertain; never redispatch, fail closed. Explicit
        // cancellation resolves them through their cancellation IDs instead.
        if !input.cancel_requested
            && let Some(record) = projected.parallel_branches.get(&branch_id)
            && !record.dispatched_without_terminal().is_empty()
        {
            return Err(NodeExecutorError::Ambiguous {
                detail: format!(
                    "parallel members dispatched without terminal evidence: {:?}",
                    record.dispatched_without_terminal()
                ),
            });
        }
        let record =
            projected
                .parallel_branches
                .get(&branch_id)
                .ok_or(NodeExecutorError::Ambiguous {
                    detail: String::from("parallel state is missing after initialization"),
                })?;

        // Cancellation: mark dispatched-but-not-terminal members cancelled and
        // finish the node as cancelled.
        if input.cancel_requested {
            for member in record.dispatched_without_terminal() {
                events.push(NodeExecutorEventPayload::ParallelBranchMemberCancelled(
                    ParallelBranchMemberCancelledEvent {
                        branch_id: branch_id.clone(),
                        member_id: member,
                        reason: String::from("cancelled"),
                    },
                ));
            }
            events.push(NodeExecutorEventPayload::ParallelBranchFinished(
                ParallelBranchFinishedEvent {
                    branch_id: branch_id.clone(),
                    state: ParallelBranchTerminalState::Cancelled,
                    reason: String::from("cancelled"),
                },
            ));
            return Ok(ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
                step: NodeExecutorStep {
                    events,
                    transition_variables: serde_json::json!({
                        "parallel": {"state": "cancelled", "reason": "cancelled"}
                    }),
                },
                classification: NodeExecutorFailureClassification::Cancelled,
                reason: String::from("cancelled"),
            }));
        }

        // Dispatch the next deterministic batch with independent cancellation
        // identities.
        let batch = Self::next_dispatch_batch(record);
        let start_index = u32::try_from(record.dispatched_order.len()).unwrap_or(u32::MAX);
        for (offset, member) in batch.iter().enumerate() {
            let offset = u32::try_from(offset).unwrap_or(u32::MAX);
            events.push(NodeExecutorEventPayload::ParallelBranchMemberDispatched(
                ParallelBranchMemberDispatchedEvent {
                    branch_id: branch_id.clone(),
                    member_id: member.clone(),
                    dispatch_index: start_index + offset,
                    cancellation_id: format!(
                        "par-cancel:{session}:{run}:{node}:{step}:{member}",
                        session = input.session_id,
                        run = input.run_id,
                        node = input.node_id,
                        step = input.step,
                        member = member
                    ),
                },
            ));
        }

        // Re-project after dispatch to evaluate the terminal state.
        let mut projected = projected.clone();
        let dispatch_events = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    NodeExecutorEventPayload::ParallelBranchMemberDispatched(_)
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        for (index, event) in dispatch_events.iter().enumerate() {
            projected
                .apply(event, u64::MAX - index as u64)
                .map_err(|error| NodeExecutorError::InvalidTransition {
                    detail: format!("parallel dispatch rejected: {error}"),
                })?;
        }
        let record = projected
            .parallel_branches
            .get(&branch_id)
            .expect("parallel record exists after dispatch");

        let terminal_members = record
            .member_states
            .values()
            .filter(|state| {
                matches!(
                    state,
                    ParallelMemberState::Completed
                        | ParallelMemberState::Failed
                        | ParallelMemberState::Cancelled
                )
            })
            .count();
        let all_terminal = terminal_members == record.branch_ids.len();
        if !all_terminal {
            let reason = format!(
                "dispatched {} of {} members",
                record.dispatched_order.len(),
                record.branch_ids.len()
            );
            return Ok(ExecutorPhaseResult::Await { events, reason });
        }

        let failures = record
            .member_states
            .values()
            .filter(|state| {
                matches!(
                    state,
                    ParallelMemberState::Failed | ParallelMemberState::Cancelled
                )
            })
            .count();
        // Join policy: any member failure or cancellation fails the node.
        // A dedicated configurable allowance is available for extension.
        let terminal_state = if failures == 0 {
            ParallelBranchTerminalState::FinishedSuccess
        } else {
            ParallelBranchTerminalState::FinishedFailure
        };
        let reason = match terminal_state {
            ParallelBranchTerminalState::FinishedSuccess => {
                format!("all {} members completed", record.branch_ids.len())
            }
            _ => format!("{failures} member(s) failed or cancelled"),
        };
        events.push(NodeExecutorEventPayload::ParallelBranchFinished(
            ParallelBranchFinishedEvent {
                branch_id: branch_id.clone(),
                state: terminal_state,
                reason: reason.clone(),
            },
        ));
        let step = NodeExecutorStep {
            events,
            transition_variables: serde_json::json!({
                "parallel": {
                    "state": match terminal_state {
                        ParallelBranchTerminalState::FinishedSuccess => "finished_success",
                        ParallelBranchTerminalState::FinishedFailure => "finished_failure",
                        ParallelBranchTerminalState::Cancelled => "cancelled",
                    },
                    "reason": reason,
                }
            }),
        };
        Ok(ExecutorPhaseResult::Done(match terminal_state {
            ParallelBranchTerminalState::FinishedSuccess => NodeExecutorOutcome::Complete { step },
            _ => NodeExecutorOutcome::Failed {
                step,
                classification: NodeExecutorFailureClassification::Failed,
                reason,
            },
        }))
    }

    fn finalize(
        &self,
        _input: &NodeExecutorInput,
        _state: &NodeExecutorState,
        _receipt: &NodeExecutorEffectReceipt,
    ) -> Result<NodeExecutorOutcome, NodeExecutorError> {
        // Parallel nodes never request external effects; member execution is
        // dispatched by the runtime through the canonical dispatch events.
        Err(NodeExecutorError::InvalidTransition {
            detail: String::from("parallel executor requested no effect"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_executors::{NodeExecutorClock, ParallelBranchConfig};

    fn input(branches: &[&str], max_parallelism: u32) -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("parallel"),
            attempt: 1,
            loop_iteration: 0,
            step: 3,
            executor_kind: NodeExecutorKind::ParallelBranch,
            config: NodeExecutorConfig::ParallelBranch(ParallelBranchConfig {
                branch_ids: branches.iter().map(|value| (*value).to_owned()).collect(),
                max_parallelism,
                shared_write_scopes: Vec::new(),
                merge_policy: None,
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
            result_references: Vec::new(),
            result_bytes: 0,
        }
    }

    #[test]
    fn dispatch_respects_max_parallelism_and_finishes_success() {
        let executor = ParallelBranchExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(&["a", "b", "c"], 2);

        let phase = executor.prepare(&input, &state).expect("first prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        assert_eq!(events[0].event_type(), "parallel.branch_initialized");
        let dispatched: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NodeExecutorEventPayload::ParallelBranchMemberDispatched(event) => {
                    Some(event.member_id.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(dispatched, ["a", "b"]); // max parallelism 2
        for event in &events {
            state.apply(event, 10).expect("commit");
        }
        // Complete a and b; dispatch c.
        input.participant_outcomes = vec![completed("a"), completed("b")];
        let phase = executor.prepare(&input, &state).expect("second prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        let dispatched: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                NodeExecutorEventPayload::ParallelBranchMemberDispatched(event) => {
                    Some(event.member_id.clone())
                }
                _ => None,
            })
            .collect();
        assert_eq!(dispatched, ["c"]);
        for event in &events {
            state.apply(event, 11).expect("commit");
        }
        // Complete c; the node finishes successfully.
        input.participant_outcomes = vec![completed("c")];
        let phase = executor.prepare(&input, &state).expect("third prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Complete { step }) = phase else {
            panic!("expected success");
        };
        assert_eq!(
            step.events.last().expect("finished").event_type(),
            "parallel.branch_finished"
        );
        assert_eq!(
            step.transition_variables["parallel"]["state"],
            "finished_success"
        );
    }

    #[test]
    fn member_failure_finishes_the_node_as_failure() {
        let executor = ParallelBranchExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(&["a"], 1);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        for event in &events {
            state.apply(event, 10).expect("commit");
        }
        input.participant_outcomes = vec![ParticipantOutcome::Failed {
            participant: String::from("a"),
            reason: String::from("member failed"),
        }];
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed { classification, .. }) = phase
        else {
            panic!("expected failure");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Failed);
    }

    #[test]
    fn shared_writes_without_merge_policy_fail_closed() {
        let executor = ParallelBranchExecutor;
        let state = NodeExecutorState::default();
        let mut input = input(&["a", "b"], 2);
        input.config = NodeExecutorConfig::ParallelBranch(ParallelBranchConfig {
            branch_ids: vec![String::from("a"), String::from("b")],
            max_parallelism: 2,
            shared_write_scopes: vec![String::from("variables.output")],
            merge_policy: None,
        });
        assert!(matches!(
            executor.prepare(&input, &state).expect_err("shared write"),
            NodeExecutorError::InvalidInput { .. }
        ));
        // With an explicit merge policy the same graph is accepted.
        match &mut input.config {
            NodeExecutorConfig::ParallelBranch(config) => {
                config.merge_policy = Some(String::from("serialize_declaration_order"));
            }
            _ => unreachable!(),
        }
        executor
            .prepare(&input, &state)
            .expect("allowed with policy");
    }

    #[test]
    fn dispatched_without_terminal_fails_closed_on_recovery() {
        let executor = ParallelBranchExecutor;
        let mut state = NodeExecutorState::default();
        let input = input(&["a", "b"], 2);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        for event in &events {
            state.apply(event, 10).expect("commit");
        }
        // Re-enter with no outcomes: dispatched members are uncertain.
        let error = executor.prepare(&input, &state).expect_err("uncertain");
        assert_eq!(
            error.recovery_classification(),
            crate::node_executors::state::ReplayClassification::ExternallyUncertain
        );
        assert!(matches!(error, NodeExecutorError::Ambiguous { .. }));
    }

    #[test]
    fn cancellation_marks_members_and_finishes_cancelled() {
        let executor = ParallelBranchExecutor;
        let mut state = NodeExecutorState::default();
        let mut input = input(&["a", "b"], 2);
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Await { events, .. } = phase else {
            panic!("expected awaiting");
        };
        for event in &events {
            state.apply(event, 10).expect("commit");
        }
        input.cancel_requested = true;
        let phase = executor.prepare(&input, &state).expect("prepare");
        let ExecutorPhaseResult::Done(NodeExecutorOutcome::Failed {
            classification,
            step,
            ..
        }) = phase
        else {
            panic!("expected cancellation");
        };
        assert_eq!(classification, NodeExecutorFailureClassification::Cancelled);
        assert!(step.events.iter().any(|event| {
            matches!(
                event,
                NodeExecutorEventPayload::ParallelBranchMemberCancelled(_)
            )
        }));
        assert_eq!(
            step.events.last().expect("finished").event_type(),
            "parallel.branch_finished"
        );
    }
}
