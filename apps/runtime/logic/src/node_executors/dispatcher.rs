//! Generic native node dispatch: the mock-integration surface for Task 2's
//! generic dispatcher contract.
//!
//! This dispatcher mirrors Task 2's capability resolution
//! (`agentmod_runtime_data::node_executor`): each compiled node resolves to
//! exactly one native executor, which is then driven through its
//! prepare/effect/finalize phases. A committed-event store applies canonical
//! payloads through the pure reducer (the same reducer the session reducer
//! delegates to), so replay positions, recovery cuts, and duplicate
//! suppression are exercised without a real journal. The composition root
//! binds real ports and the session journal in place of these mocks.

use std::sync::{Arc, Mutex};

use crate::node_executors::{
    NativeNodeDispatcher, NodeExecutorEffect, NodeExecutorEffectReceipt, NodeExecutorError,
    NodeExecutorInput, NodeExecutorOutcome, NodeExecutorPorts, NodeExecutorStep,
    events::{GraphScheduleRejectedEvent, NodeExecutorEventPayload},
    state::{NodeExecutorState, ReplayClassification},
};

/// Committed-event store consumed by the generic dispatcher.
pub trait NodeExecutorCommitter: Send + Sync {
    /// Commits canonical event payloads in order.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorError`] when a payload violates the reducer
    /// invariants.
    fn commit(&self, events: &[NodeExecutorEventPayload]) -> Result<(), NodeExecutorError>;

    /// Returns the current reconstructed state.
    fn state(&self) -> NodeExecutorState;
}

/// In-memory committed-event store using the pure reducer.
#[derive(Debug)]
pub struct InMemoryNodeExecutorCommitter {
    state: Mutex<NodeExecutorState>,
    next_sequence: Mutex<u64>,
}

impl InMemoryNodeExecutorCommitter {
    /// Constructs an empty store.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            state: Mutex::new(NodeExecutorState::default()),
            next_sequence: Mutex::new(1),
        }
    }
}

impl NodeExecutorCommitter for InMemoryNodeExecutorCommitter {
    fn commit(&self, events: &[NodeExecutorEventPayload]) -> Result<(), NodeExecutorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| NodeExecutorError::PortFailure { port: "committer" })?;
        for event in events {
            let sequence = {
                let mut next = self
                    .next_sequence
                    .lock()
                    .map_err(|_| NodeExecutorError::PortFailure { port: "committer" })?;
                let current = *next;
                *next += 1;
                current
            };
            state
                .apply(event, sequence)
                .map_err(|error| NodeExecutorError::InvalidTransition {
                    detail: format!("committed event rejected by reducer: {error}"),
                })?;
        }
        Ok(())
    }

    fn state(&self) -> NodeExecutorState {
        self.state
            .lock()
            .map(|state| state.clone())
            .unwrap_or_default()
    }
}

/// Policy decision over one proposed consequential effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeExecutorPolicyDecision {
    /// The proposal may proceed to its idempotent effect.
    Approve,
    /// The proposal is denied and becomes a canonical rejection.
    Deny,
}

/// Policy seam for consequential node proposals (schedule creation).
pub trait NodeExecutorPolicy: Send + Sync {
    /// Decides one proposed effect.
    fn decide(&self, proposal: &NodeExecutorEventPayload) -> NodeExecutorPolicyDecision;
}

/// Default policy: every consequential proposal is approved.
#[derive(Clone, Copy, Debug, Default)]
pub struct ApproveAllPolicy;

impl NodeExecutorPolicy for ApproveAllPolicy {
    fn decide(&self, _proposal: &NodeExecutorEventPayload) -> NodeExecutorPolicyDecision {
        NodeExecutorPolicyDecision::Approve
    }
}

/// Test policy that denies graph-schedule proposals.
#[derive(Clone, Copy, Debug, Default)]
pub struct DenySchedulePolicy;

impl NodeExecutorPolicy for DenySchedulePolicy {
    fn decide(&self, proposal: &NodeExecutorEventPayload) -> NodeExecutorPolicyDecision {
        if matches!(proposal, NodeExecutorEventPayload::GraphScheduleProposed(_)) {
            NodeExecutorPolicyDecision::Deny
        } else {
            NodeExecutorPolicyDecision::Approve
        }
    }
}

/// Outcome of one generic dispatch.
#[derive(Clone, Debug, PartialEq)]
pub enum DispatchOutcome {
    /// The node reached a terminal outcome; its events are committed.
    Done(NodeExecutorOutcome),
    /// The node awaits an external wake; its events are committed.
    Awaiting {
        /// Stable awaiting classification.
        reason: String,
    },
}

/// The generic native node dispatcher used for mock integration.
pub struct GenericNodeDispatcher {
    /// Executor resolution mirroring Task 2's capability registry.
    pub executors: NativeNodeDispatcher,
    /// Committed-event store.
    pub committer: Arc<dyn NodeExecutorCommitter>,
    /// External capability ports.
    pub ports: Arc<dyn NodeExecutorPorts>,
    /// Consequential proposal policy.
    pub policy: Arc<dyn NodeExecutorPolicy>,
}

impl GenericNodeDispatcher {
    /// Resolves and drives one node to a terminal or awaiting outcome.
    ///
    /// # Errors
    ///
    /// Returns [`NodeExecutorError`] when the node cannot execute: identity
    /// mismatch, invalid input, an invalid replay transition, or an
    /// externally uncertain replay position (never redispatched).
    pub fn dispatch(
        &self,
        input: &NodeExecutorInput,
    ) -> Result<DispatchOutcome, NodeExecutorError> {
        let executor =
            self.executors
                .resolve(input.executor_kind)
                .ok_or(NodeExecutorError::InvalidInput {
                    reason: format!(
                        "no native executor for `{}`",
                        input.executor_kind.implementation_id()
                    ),
                })?;
        if executor.kind() != input.executor_kind {
            return Err(NodeExecutorError::IdentityMismatch {
                node_id: input.node_id.clone(),
            });
        }

        // Replay gate: ambiguous positions fail closed before any effect. A
        // caller that supplies canonical participant outcomes is resolving an
        // in-flight parallel/join node, so the gate yields to the executor's
        // own stricter validation of those outcomes.
        let state = self.committer.state();
        match state.classify_replay(&input.identity()) {
            ReplayClassification::Consistent
            | ReplayClassification::SafeToProceed
            | ReplayClassification::InvalidTransition => {}
            ReplayClassification::ExternallyUncertain if !input.participant_outcomes.is_empty() => {
            }
            ReplayClassification::ExternallyUncertain => {
                return Err(NodeExecutorError::Ambiguous {
                    detail: format!(
                        "replay position for node `{}` is externally uncertain",
                        input.node_id
                    ),
                });
            }
        }

        let state = self.committer.state();
        let phase = executor.prepare(input, &state)?;
        match phase {
            crate::node_executors::ExecutorPhaseResult::Done(outcome) => {
                let events = outcome_events(&outcome);
                self.committer.commit(&events)?;
                Ok(DispatchOutcome::Done(outcome))
            }
            crate::node_executors::ExecutorPhaseResult::Await { events, reason } => {
                self.committer.commit(&events)?;
                Ok(DispatchOutcome::Awaiting { reason })
            }
            crate::node_executors::ExecutorPhaseResult::Effect { events, effect } => {
                self.committer.commit(&events)?;
                // Consequential proposals pass through the policy seam before
                // the idempotent effect is performed.
                if let Some(proposal) = events.last()
                    && self.policy.decide(proposal) == NodeExecutorPolicyDecision::Deny
                {
                    let rejection = rejection_event(input, proposal)?;
                    self.committer.commit(&[rejection])?;
                    let reason = String::from("policy denied consequential schedule creation");
                    return Ok(DispatchOutcome::Done(NodeExecutorOutcome::Failed {
                        step: NodeExecutorStep {
                            events: Vec::new(),
                            transition_variables: serde_json::json!({
                                "schedule": {"state": "rejected", "reason": reason}
                            }),
                        },
                        classification:
                            crate::node_executors::NodeExecutorFailureClassification::Rejected,
                        reason,
                    }));
                }
                let receipt = self.perform_effect(&effect)?;
                let outcome = executor.finalize(input, &self.committer.state(), &receipt)?;
                let events = outcome_events(&outcome);
                self.committer.commit(&events)?;
                match outcome {
                    NodeExecutorOutcome::Awaiting { step, reason } => {
                        let _ = step;
                        Ok(DispatchOutcome::Awaiting { reason })
                    }
                    outcome => Ok(DispatchOutcome::Done(outcome)),
                }
            }
        }
    }

    fn perform_effect(
        &self,
        effect: &NodeExecutorEffect,
    ) -> Result<NodeExecutorEffectReceipt, NodeExecutorError> {
        match effect {
            NodeExecutorEffect::DeliverChildMessage(command) => self
                .ports
                .child_messages()
                .deliver_child_message(command.clone())
                .map(NodeExecutorEffectReceipt::ChildMessage)
                .map_err(|_| NodeExecutorError::PortFailure { port: "child" }),
            NodeExecutorEffect::UpsertSchedule(command) => self
                .ports
                .schedules()
                .upsert_schedule(command.clone())
                .map(NodeExecutorEffectReceipt::Schedule)
                .map_err(|_| NodeExecutorError::PortFailure { port: "schedule" }),
            NodeExecutorEffect::RemoveSchedule { schedule_id } => self
                .ports
                .schedules()
                .remove_schedule(schedule_id)
                .map(NodeExecutorEffectReceipt::ScheduleRemoved)
                .map_err(|_| NodeExecutorError::PortFailure { port: "schedule" }),
            NodeExecutorEffect::CreateDelayContinuation(command) => self
                .ports
                .delays()
                .create_delay_continuation(command.clone())
                .map(|()| NodeExecutorEffectReceipt::DelayCreated)
                .map_err(|_| NodeExecutorError::PortFailure { port: "delay" }),
            NodeExecutorEffect::ClaimDelayWake(command) => self
                .ports
                .delays()
                .claim_delay_wake(command.clone())
                .map(NodeExecutorEffectReceipt::DelayWake)
                .map_err(|_| NodeExecutorError::PortFailure { port: "delay" }),
            NodeExecutorEffect::CancelDelayContinuation(command) => self
                .ports
                .delays()
                .cancel_delay_continuation(command.clone())
                .map(NodeExecutorEffectReceipt::DelayCancelled)
                .map_err(|_| NodeExecutorError::PortFailure { port: "delay" }),
        }
    }
}

fn outcome_events(outcome: &NodeExecutorOutcome) -> Vec<NodeExecutorEventPayload> {
    match outcome {
        NodeExecutorOutcome::Complete { step }
        | NodeExecutorOutcome::Awaiting { step, .. }
        | NodeExecutorOutcome::Failed { step, .. } => step.events.clone(),
    }
}

/// Synthesizes the canonical rejection for a denied proposal.
fn rejection_event(
    input: &NodeExecutorInput,
    proposal: &NodeExecutorEventPayload,
) -> Result<NodeExecutorEventPayload, NodeExecutorError> {
    let NodeExecutorEventPayload::GraphScheduleProposed(proposed) = proposal else {
        return Err(NodeExecutorError::InvalidTransition {
            detail: String::from("policy denied a proposal without a rejection path"),
        });
    };
    Ok(NodeExecutorEventPayload::GraphScheduleRejected(
        GraphScheduleRejectedEvent {
            schedule_id: proposed.schedule_id.clone(),
            node_id: input.node_id.clone(),
            reason: String::from("policy denied schedule creation"),
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use agentmod_primitives::ContentHash;

    use super::*;
    use crate::node_executors::{
        ChildMessageConfig, DelayConfig, EmitEventConfig, JoinConfig, NodeExecutorClock,
        NodeExecutorConfig, NodeExecutorKind, ParallelBranchConfig, ParticipantOutcome,
        ScheduleConfig,
        events::{
            ChildMessageClassification, GraphScheduleTrigger, JoinArtifactCollection, JoinOrdering,
            JoinProjection,
        },
        ports::{
            CancelDelayContinuationCommand, ChildLifecycleView, ChildMessageReceipt,
            ChildSessionMessagePort, ChildSessionMessagePortError, ClaimDelayWakeCommand,
            CreateDelayContinuationCommand, DelayWakeResult, DeliverChildMessageCommand,
            DurableDelayPort, DurableDelayPortError, GraphSchedulePort, GraphSchedulePortError,
            NodeExecutorPorts, ScheduleStoreReceipt, UpsertGraphScheduleCommand,
        },
    };

    /// Deterministic mock ports recording every call.
    #[derive(Default)]
    struct MockPorts {
        delivered: std::sync::Mutex<Vec<DeliverChildMessageCommand>>,
        upserts: std::sync::Mutex<BTreeMap<String, UpsertGraphScheduleCommand>>,
        continuations: std::sync::Mutex<BTreeMap<String, CreateDelayContinuationCommand>>,
        claimed: std::sync::Mutex<Vec<ClaimDelayWakeCommand>>,
    }

    impl NodeExecutorPorts for MockPorts {
        fn child_messages(&self) -> &dyn ChildSessionMessagePort {
            self
        }
        fn schedules(&self) -> &dyn GraphSchedulePort {
            self
        }
        fn delays(&self) -> &dyn DurableDelayPort {
            self
        }
    }

    impl ChildSessionMessagePort for MockPorts {
        fn deliver_child_message(
            &self,
            command: DeliverChildMessageCommand,
        ) -> Result<ChildMessageReceipt, ChildSessionMessagePortError> {
            let mut deliveries = self.delivered.lock().expect("lock");
            let delivered = deliveries
                .iter()
                .any(|previous| previous.message_id == command.message_id);
            // Create-once by message identity: a replay returns the same
            // terminal receipt without a duplicate child-side effect.
            if !delivered {
                deliveries.push(command.clone());
            }
            Ok(ChildMessageReceipt {
                delivered: true,
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

    impl GraphSchedulePort for MockPorts {
        fn upsert_schedule(
            &self,
            command: UpsertGraphScheduleCommand,
        ) -> Result<ScheduleStoreReceipt, GraphSchedulePortError> {
            let mut upserts = self.upserts.lock().expect("lock");
            let replayed = upserts.contains_key(&command.schedule_id);
            upserts.insert(command.schedule_id.clone(), command);
            Ok(ScheduleStoreReceipt {
                schedule_id: upserts.len().to_string(),
                replayed,
            })
        }

        fn remove_schedule(&self, _schedule_id: &str) -> Result<bool, GraphSchedulePortError> {
            Ok(true)
        }
    }

    impl DurableDelayPort for MockPorts {
        fn create_delay_continuation(
            &self,
            command: CreateDelayContinuationCommand,
        ) -> Result<(), DurableDelayPortError> {
            let mut continuations = self.continuations.lock().expect("lock");
            continuations
                .entry(command.continuation_id.clone())
                .or_insert(command);
            Ok(())
        }

        fn claim_delay_wake(
            &self,
            command: ClaimDelayWakeCommand,
        ) -> Result<DelayWakeResult, DurableDelayPortError> {
            let mut claimed = self.claimed.lock().expect("lock");
            let transitioned = !claimed
                .iter()
                .any(|previous| previous.continuation_id == command.continuation_id);
            claimed.push(command);
            Ok(DelayWakeResult {
                transitioned,
                proof: String::from("scheduler.claim"),
            })
        }

        fn cancel_delay_continuation(
            &self,
            _command: CancelDelayContinuationCommand,
        ) -> Result<bool, DurableDelayPortError> {
            Ok(true)
        }
    }

    fn dispatcher(ports: Arc<MockPorts>) -> GenericNodeDispatcher {
        GenericNodeDispatcher {
            executors: NativeNodeDispatcher::native(),
            committer: Arc::new(InMemoryNodeExecutorCommitter::empty()),
            ports,
            policy: Arc::new(ApproveAllPolicy),
        }
    }

    fn base_input() -> NodeExecutorInput {
        NodeExecutorInput {
            session_id: String::from("session-1"),
            run_id: String::from("run-1"),
            node_id: String::from("node"),
            attempt: 1,
            loop_iteration: 0,
            step: 1,
            executor_kind: NodeExecutorKind::ChildMessage,
            config: NodeExecutorConfig::ChildMessage(ChildMessageConfig {
                child_session_id: String::from("child-1"),
                idempotency_key: String::from("key-1"),
                content: String::from("hello"),
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
    fn task2_registry_records_are_consistent_with_native_executors() {
        use agentmod_runtime_data::node_executor::{
            ListNodeExecutorsDataRequest, NodeExecutorDataPort,
        };
        let registry = agentmod_runtime_data::node_executor::RuntimeNodeExecutorData::native()
            .expect("native registry");
        let records = registry
            .list_node_executors(ListNodeExecutorsDataRequest)
            .expect("records");
        for kind in [
            NodeExecutorKind::ChildMessage,
            NodeExecutorKind::Join,
            NodeExecutorKind::ParallelBranch,
            NodeExecutorKind::Delay,
            NodeExecutorKind::Schedule,
            NodeExecutorKind::EmitEvent,
        ] {
            let record = records
                .iter()
                .find(|record| {
                    record.node_kind == kind.serialized_node_kind()
                        && record.id == kind.implementation_id()
                })
                .expect("registry record");
            assert_eq!(record.version, kind.implementation_version());
            assert_eq!(
                record.capabilities.iter().cloned().collect::<Vec<_>>(),
                kind.capabilities()
            );
            // The six categories remain non-selected until the runtime
            // dispatcher is wired; graphs using them still fail closed.
            assert!(!record.available);
        }
    }

    #[test]
    fn child_message_dispatch_delivers_once_and_survives_recovery_cut() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let input = base_input();
        let first = dispatch.dispatch(&input).expect("first dispatch");
        assert!(matches!(first, DispatchOutcome::Done(_)));
        assert_eq!(ports.delivered.lock().expect("lock").len(), 1);
        // Recovery cut: the proposal is committed but delivery outcome is not
        // canonical; the create-once boundary redelivers without duplication.
        let second = dispatch.dispatch(&input).expect("recovery dispatch");
        assert!(matches!(second, DispatchOutcome::Done(_)));
        assert_eq!(ports.delivered.lock().expect("lock").len(), 1);
        let state = dispatch.committer.state();
        let record = state.child_messages.values().next().expect("record");
        assert_eq!(
            record.state,
            crate::node_executors::state::ChildMessageState::Delivered
        );
    }

    #[test]
    fn schedule_dispatch_traverses_proposal_policy_effect_and_result() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::Schedule;
        input.node_id = String::from("schedule");
        input.config = NodeExecutorConfig::Schedule(ScheduleConfig {
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
        });
        let outcome = dispatch.dispatch(&input).expect("dispatch");
        assert!(matches!(outcome, DispatchOutcome::Done(_)));
        {
            let upserts = ports.upserts.lock().expect("lock");
            assert_eq!(upserts.len(), 1);
        }
        // Recovery cut after the proposal: the create-once upsert replays.
        let outcome = dispatch.dispatch(&input).expect("recovery dispatch");
        assert!(matches!(outcome, DispatchOutcome::Done(_)));
        {
            let upserts = ports.upserts.lock().expect("lock");
            assert_eq!(upserts.len(), 1);
        }
        let state = dispatch.committer.state();
        let schedule = state.schedules.values().next().expect("schedule record");
        assert_eq!(
            schedule.state,
            crate::node_executors::state::GraphScheduleState::Active
        );
    }

    #[test]
    fn denied_schedule_proposal_commits_rejection_without_effect() {
        let ports = Arc::new(MockPorts::default());
        let mut dispatch = dispatcher(ports.clone());
        dispatch.policy = Arc::new(DenySchedulePolicy);
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::Schedule;
        input.node_id = String::from("schedule");
        input.config = NodeExecutorConfig::Schedule(ScheduleConfig {
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
        });
        let outcome = dispatch.dispatch(&input).expect("dispatch");
        let DispatchOutcome::Done(NodeExecutorOutcome::Failed { classification, .. }) = outcome
        else {
            panic!("expected policy rejection");
        };
        assert_eq!(
            classification,
            crate::node_executors::NodeExecutorFailureClassification::Rejected
        );
        assert!(ports.upserts.lock().expect("lock").is_empty());
        let state = dispatch.committer.state();
        assert_eq!(
            state.schedules.values().next().expect("schedule").state,
            crate::node_executors::state::GraphScheduleState::Rejected
        );
    }

    #[test]
    fn delay_dispatch_schedules_then_resumes_exactly_once_across_restart() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::Delay;
        input.node_id = String::from("wait");
        input.config = NodeExecutorConfig::Delay(DelayConfig {
            duration_ms: 5_000,
            expires_at_ms: None,
        });
        let outcome = dispatch.dispatch(&input).expect("schedule dispatch");
        assert!(matches!(outcome, DispatchOutcome::Awaiting { .. }));
        assert_eq!(ports.continuations.lock().expect("lock").len(), 1);
        // Restart cut: a pending delay re-enters with the scheduler wake claim.
        input.wake_claim = Some(ClaimDelayWakeCommand {
            session_id: String::from("session-1"),
            continuation_id: input.identity().delay_continuation_id(),
            wake_time_ms: 1_700_000_005_000,
        });
        let outcome = dispatch.dispatch(&input).expect("wake dispatch");
        assert!(matches!(outcome, DispatchOutcome::Done(_)));
        assert_eq!(ports.claimed.lock().expect("lock").len(), 1);
        let state = dispatch.committer.state();
        assert_eq!(
            state.delays.values().next().expect("delay").state,
            crate::node_executors::state::DelayState::Resumed
        );
        // A duplicate wake claim is a no-op: resume exactly once and never
        // re-claims the transition.
        let outcome = dispatch.dispatch(&input).expect("duplicate wake");
        assert!(matches!(outcome, DispatchOutcome::Done(_)));
        assert_eq!(ports.claimed.lock().expect("lock").len(), 1);
        let state = dispatch.committer.state();
        assert_eq!(
            state.delays.values().next().expect("delay").state,
            crate::node_executors::state::DelayState::Resumed
        );
    }

    #[test]
    fn join_dispatch_awaits_then_releases_with_exact_results() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::Join;
        input.node_id = String::from("join");
        input.config = NodeExecutorConfig::Join(JoinConfig {
            required_participants: vec![String::from("child-1"), String::from("child-2")],
            optional_participants: Vec::new(),
            min_success: 2,
            allowed_failures: 0,
            timeout_ms: None,
            ordering: JoinOrdering::DeclarationOrder,
            result_projection: JoinProjection::TypedResults,
            artifact_collection: JoinArtifactCollection::Bounded,
        });
        let outcome = dispatch.dispatch(&input).expect("initial dispatch");
        assert!(matches!(outcome, DispatchOutcome::Awaiting { .. }));
        // Re-enter with one completion: still awaiting.
        input.participant_outcomes = vec![ParticipantOutcome::Completed {
            participant: String::from("child-1"),
            result_references: vec![String::from("child-1:result")],
            result_bytes: 8,
        }];
        let outcome = dispatch.dispatch(&input).expect("partial dispatch");
        assert!(matches!(outcome, DispatchOutcome::Awaiting { .. }));
        // Re-enter with the second: releases success with exact references.
        input.participant_outcomes = vec![ParticipantOutcome::Completed {
            participant: String::from("child-2"),
            result_references: vec![String::from("child-2:result")],
            result_bytes: 8,
        }];
        let outcome = dispatch.dispatch(&input).expect("final dispatch");
        let DispatchOutcome::Done(NodeExecutorOutcome::Complete { step }) = outcome else {
            panic!("expected join success");
        };
        assert_eq!(step.transition_variables["join"]["state"], "success");
        let state = dispatch.committer.state();
        let join = state.joins.values().next().expect("join");
        assert_eq!(
            join.collected_result_references,
            ["child-1:result", "child-2:result"]
        );
    }

    #[test]
    fn parallel_dispatch_bounds_concurrency_and_rejects_uncertain_recovery() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::ParallelBranch;
        input.node_id = String::from("parallel");
        input.config = NodeExecutorConfig::ParallelBranch(ParallelBranchConfig {
            branch_ids: vec![String::from("a"), String::from("b"), String::from("c")],
            max_parallelism: 2,
            shared_write_scopes: Vec::new(),
            merge_policy: None,
        });
        let outcome = dispatch.dispatch(&input).expect("initial dispatch");
        let DispatchOutcome::Awaiting { .. } = outcome else {
            panic!("expected awaiting");
        };
        let state = dispatch.committer.state();
        let record = state.parallel_branches.values().next().expect("record");
        assert_eq!(record.dispatched_order, ["a", "b"]);
        // Recovery cut: dispatched members without terminal evidence fail
        // closed and are never redispatched.
        let input_unchanged = input.clone();
        let error = dispatch
            .dispatch(&input_unchanged)
            .expect_err("uncertain recovery");
        assert_eq!(
            error.recovery_classification(),
            ReplayClassification::ExternallyUncertain
        );
        // Complete a and b; dispatch c.
        input.participant_outcomes = vec![
            ParticipantOutcome::Completed {
                participant: String::from("a"),
                result_references: Vec::new(),
                result_bytes: 0,
            },
            ParticipantOutcome::Completed {
                participant: String::from("b"),
                result_references: Vec::new(),
                result_bytes: 0,
            },
        ];
        let outcome = dispatch.dispatch(&input).expect("second dispatch");
        assert!(matches!(outcome, DispatchOutcome::Awaiting { .. }));
        let state = dispatch.committer.state();
        let record = state.parallel_branches.values().next().expect("record");
        assert_eq!(record.dispatched_order, ["a", "b", "c"]);
        // Complete c; the node finishes successfully.
        input.participant_outcomes = vec![ParticipantOutcome::Completed {
            participant: String::from("c"),
            result_references: Vec::new(),
            result_bytes: 0,
        }];
        let outcome = dispatch.dispatch(&input).expect("final dispatch");
        let DispatchOutcome::Done(NodeExecutorOutcome::Complete { step }) = outcome else {
            panic!("expected parallel success");
        };
        assert_eq!(
            step.transition_variables["parallel"]["state"],
            "finished_success"
        );
    }

    #[test]
    fn event_emission_dispatch_emits_once_and_suppresses_duplicates() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::EmitEvent;
        input.node_id = String::from("emit");
        input.config = NodeExecutorConfig::EmitEvent(EmitEventConfig {
            namespace: String::from("project"),
            event_type: String::from("progress"),
            payload_json: String::from(r#"{"percent":42}"#),
            artifact_references: Vec::new(),
            metadata_json: String::from(r"{}"),
        });
        let outcome = dispatch.dispatch(&input).expect("emit");
        let DispatchOutcome::Done(NodeExecutorOutcome::Complete { step }) = outcome else {
            panic!("expected emission");
        };
        assert_eq!(step.events[0].event_type(), "event.user_emitted");
        let outcome = dispatch.dispatch(&input).expect("duplicate");
        let DispatchOutcome::Done(NodeExecutorOutcome::Complete { step }) = outcome else {
            panic!("expected duplicate suppression");
        };
        assert!(step.events.is_empty());
        let state = dispatch.committer.state();
        assert_eq!(state.emitted_events.len(), 1);
        assert_eq!(
            state.emitted_events[0].payload_hash,
            ContentHash::digest(br#"{"percent":42}"#)
        );
    }

    #[test]
    fn identity_mismatch_fails_before_any_effect() {
        let ports = Arc::new(MockPorts::default());
        let dispatch = dispatcher(ports.clone());
        let mut input = base_input();
        input.executor_kind = NodeExecutorKind::Join; // config still child message
        let error = dispatch.dispatch(&input).expect_err("mismatch");
        assert!(matches!(error, NodeExecutorError::IdentityMismatch { .. }));
        assert!(ports.delivered.lock().expect("lock").is_empty());
    }
}
