//! Runtime-owned canonical graph-state and budget reducer for one session.
//!
//! This module is the runtime logic seam between the pure
//! `agentmod-graph-state` core and generic dispatch. It owns the session-bound
//! canonical projection: graph variables (typed, scoped, merge-safe) and the
//! execution-budget ledger (known/estimated/unknown semantics, pricing
//! provenance, child rollup, exact restart reconstruction). It is a pure
//! reducer: every mutation returns canonical events for the caller to journal,
//! and replay of those events reconstructs an identical projection without
//! calling any external system.
//!
//! Wiring this projection into the turn executor (check-before-dispatch and
//! commit-after-completion) belongs to the generic dispatch workstream; the
//! narrow ports implemented here are the consumption surface for that work.

use agentmod_expression_engine::Expression;
use agentmod_graph_state::budget::{BudgetLedger, BudgetLimits};
use agentmod_graph_state::declare::{DeclarationSet, VariableScope};
use agentmod_graph_state::event::{BudgetEvent, GraphStateEvent};
use agentmod_graph_state::expression::ConditionVerdict;
use agentmod_graph_state::port::{BudgetReadPort, GraphStateReadPort};
use agentmod_graph_state::reduce::GraphStateReducer;
use agentmod_graph_state::state::{GraphState, GraphStateError, ReadOutcome};
use agentmod_primitives::{SessionId, TimestampMillis};
use serde_json::Value;
use thiserror::Error;

use crate::session::SessionStyleBudgets;

/// Events produced by initialization of a session graph projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionGraphInitializationEvents {
    /// Canonical graph-state initialization event.
    pub graph: GraphStateEvent,
    /// Canonical budget initialization event.
    pub budget: BudgetEvent,
}

/// Session-bound canonical graph variables and budget ledger.
#[derive(Clone, Debug)]
pub struct SessionGraphState {
    session_id: SessionId,
    state: GraphState,
    ledger: BudgetLedger,
    applied: u64,
}

impl SessionGraphState {
    /// Initializes the projection from declarations and budget limits.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateLogicError`] when declarations or limits are
    /// invalid for the session.
    pub fn initialize(
        session_id: SessionId,
        declarations: DeclarationSet,
        limits: BudgetLimits,
        recorded_at: TimestampMillis,
        wall_clock_enabled: bool,
    ) -> Result<(Self, SessionGraphInitializationEvents), GraphStateLogicError> {
        let (state, graph_events) = GraphState::new(session_id, declarations)?;
        let (ledger, budget_events) =
            BudgetLedger::initialize(session_id, limits, recorded_at, wall_clock_enabled);
        let graph = graph_events
            .into_iter()
            .next()
            .ok_or(GraphStateLogicError::MissingInitialization)?;
        let budget = budget_events
            .into_iter()
            .next()
            .ok_or(GraphStateLogicError::MissingInitialization)?;
        let projection = Self {
            session_id,
            state,
            ledger,
            applied: 2,
        };
        Ok((
            projection,
            SessionGraphInitializationEvents { graph, budget },
        ))
    }

    /// Maps immutable style budgets onto canonical ledger limits.
    ///
    /// `max_steps` becomes the style-step ceiling, `max_tokens` the total
    /// token ceiling, `max_cost_micros` the provider-cost ceiling, and
    /// `max_duration_ms` the explicitly selected wall-clock ceiling.
    #[must_use]
    pub fn limits_from_style(budgets: &SessionStyleBudgets) -> BudgetLimits {
        BudgetLimits {
            max_style_steps: Some(budgets.max_steps),
            max_iterations: Some(u64::from(budgets.max_iterations)),
            max_total_tokens: Some(budgets.max_tokens),
            max_provider_cost_micros: Some(budgets.max_cost_micros),
            max_elapsed_wall_clock_ms: Some(budgets.max_duration_ms),
            ..BudgetLimits::default()
        }
    }

    /// Returns whether the style wall-clock ceiling is explicitly selected.
    #[must_use]
    pub const fn style_wall_clock_enabled(budgets: &SessionStyleBudgets) -> bool {
        budgets.max_duration_ms > 0
    }

    /// Applies one committed graph-state event to the projection.
    ///
    /// Initialization is owned by [`Self::initialize`]; re-initialization is
    /// rejected. Events are validated exactly and the projection fails closed
    /// on tampering.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateLogicError`] when the event cannot be applied
    /// exactly (tampering or ordering violation).
    pub fn apply_graph_event(&mut self, event: &GraphStateEvent) -> Result<(), GraphStateLogicError> {
        if matches!(event, GraphStateEvent::VariablesInitialized { .. }) {
            return Err(GraphStateLogicError::RepeatedInitialization);
        }
        self.state.apply_event(event)?;
        self.applied = self.applied.saturating_add(1);
        Ok(())
    }

    /// Applies one committed budget event to the projection.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateLogicError`] when the event cannot be applied
    /// exactly.
    pub fn apply_budget_event(&mut self, event: &BudgetEvent) -> Result<(), GraphStateLogicError> {
        self.ledger.apply(event)?;
        self.applied = self.applied.saturating_add(1);
        Ok(())
    }

    /// Reconstructs the projection from its complete event streams.
    ///
    /// The first graph event must be `VariablesInitialized` and the first
    /// budget event `BudgetsInitialized` for the exact session; remaining
    /// events are applied in order. The reconstructed projection equals the
    /// live projection that produced the events.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateLogicError`] when the streams are empty, the
    /// session does not match, or any event cannot be applied exactly.
    pub fn reconstruct(
        session_id: SessionId,
        graph_events: &[GraphStateEvent],
        budget_events: &[BudgetEvent],
    ) -> Result<Self, GraphStateLogicError> {
        let mut reducer = GraphStateReducer::new(session_id);
        for event in graph_events {
            reducer.apply(event)?;
        }
        let init = budget_events
            .first()
            .ok_or(GraphStateLogicError::MissingInitialization)?;
        let ledger = BudgetLedger::reconstruct(session_id, init, &budget_events[1..])?;
        Ok(Self {
            session_id,
            state: reducer.state().clone(),
            ledger,
            applied: u64::try_from(graph_events.len() + budget_events.len())
                .unwrap_or(u64::MAX),
        })
    }

    /// Returns the canonical graph variable state.
    #[must_use]
    pub const fn state(&self) -> &GraphState {
        &self.state
    }

    /// Returns the canonical budget ledger.
    #[must_use]
    pub const fn ledger(&self) -> &BudgetLedger {
        &self.ledger
    }

    /// Returns the number of events applied to this projection (audit).
    #[must_use]
    pub const fn applied(&self) -> u64 {
        self.applied
    }

    /// Returns the session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl GraphStateReadPort for SessionGraphState {
    fn declarations(&self) -> &DeclarationSet {
        self.state.declarations()
    }

    fn read(&self, name: &str, scope: &VariableScope) -> Result<ReadOutcome<'_>, GraphStateError> {
        self.state.read(name, scope)
    }

    fn verdict(&self, expression: &Expression, scope: &VariableScope) -> ConditionVerdict {
        let counters = self.ledger.budget_environment();
        agentmod_graph_state::expression::evaluate_condition(&self.state, &counters, expression, scope)
    }

    fn environment(&self, scope: &VariableScope) -> Value {
        self.state.environment(scope)
    }
}

impl BudgetReadPort for SessionGraphState {
    fn budget(&self) -> &BudgetLedger {
        &self.ledger
    }

    fn budget_environment(&self) -> Value {
        self.ledger.budget_environment()
    }
}

/// Narrow logic-owned port consumed by generic dispatch.
pub trait RuntimeGraphStatePort: GraphStateReadPort + BudgetReadPort {}

impl<T> RuntimeGraphStatePort for T where T: GraphStateReadPort + BudgetReadPort {}

/// Session graph-state logic failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphStateLogicError {
    /// The event stream lacks its initialization event.
    #[error("session graph state requires initialization events")]
    MissingInitialization,
    /// Graph-state event application failed.
    #[error("graph-state event rejected: {0}")]
    GraphState(#[from] GraphStateError),
    /// Graph-state replay failed.
    #[error("graph-state replay rejected: {0}")]
    Replay(#[from] agentmod_graph_state::reduce::ReducerError),
    /// Budget event application failed.
    #[error("budget event rejected: {0}")]
    Budget(#[from] agentmod_graph_state::budget::BudgetError),
    /// A second initialization event was applied to a live projection.
    #[error("session graph state rejects repeated initialization")]
    RepeatedInitialization,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use agentmod_graph_state::budget::{
        BudgetDimension, BudgetLimits, UsageEvidence, UsageKind,
    };
    use agentmod_graph_state::declare::{
        MutabilityPolicy, SecurityClassification, VariableDeclaration, VariableType,
    };
    use agentmod_graph_state::state::AssignmentSource;
    use agentmod_graph_state::value::GraphValue;

    use super::*;

    fn session() -> SessionId {
        SessionId::from_uuid(uuid::Uuid::nil())
    }

    fn declaration(name: &str) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            r#type: VariableType::UnsignedInteger { min: 0, max: 1_000 },
            scope: VariableScope::Run,
            producers: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: MutabilityPolicy::Assignable,
            max_serialized_bytes: 512,
            classification: SecurityClassification::SessionInternal,
            merge_policy: agentmod_graph_state::declare::MergePolicy::RejectConflict,
            default: None,
        }
    }

    fn declarations() -> DeclarationSet {
        let mut set = DeclarationSet::new();
        set.insert(declaration("steps")).expect("declared");
        set
    }

    fn limits() -> BudgetLimits {
        BudgetLimits {
            max_style_steps: Some(10),
            max_model_requests: Some(2),
            ..BudgetLimits::default()
        }
    }

    fn initialize() -> (SessionGraphState, SessionGraphInitializationEvents) {
        SessionGraphState::initialize(
            session(),
            declarations(),
            limits(),
            TimestampMillis::new(1_700_000_000_000),
            false,
        )
        .expect("initialize")
    }

    #[test]
    fn projection_commits_assignments_and_replays_exactly() {
        let (mut live, init) = initialize();
        let mut graph_events = vec![init.graph];
        let assignment = live
            .state
            .assign(
                "steps",
                GraphValue::UnsignedInteger(4),
                &AssignmentSource::Runtime,
                &VariableScope::Run,
                None,
            )
            .expect("assign");
        graph_events.extend(assignment.clone());
        let mut budget_events = vec![init.budget];
        budget_events.push(
            live.ledger
                .commit(
                    &UsageEvidence::new(
                        BudgetDimension::ModelRequests,
                        1,
                        UsageKind::Reported,
                        None,
                    ),
                    TimestampMillis::new(1_700_000_000_001),
                )
                .expect("commit"),
        );

        // Restart path: reconstruct from the complete event streams.
        let reconstructed =
            SessionGraphState::reconstruct(session(), &graph_events, &budget_events)
                .expect("reconstruct");
        assert_eq!(reconstructed.state(), live.state());
        assert_eq!(reconstructed.ledger(), live.ledger());

        // Live path: a freshly initialized projection applies the committed
        // events incrementally and reaches the same state.
        let (mut applied_projection, _) = initialize();
        for event in &assignment {
            applied_projection
                .apply_graph_event(event)
                .expect("apply graph event");
        }
        assert_eq!(applied_projection.state(), live.state());
        assert_eq!(
            applied_projection
                .read("steps", &VariableScope::Run)
                .expect("read"),
            ReadOutcome::Value(&GraphValue::UnsignedInteger(4))
        );
    }

    #[test]
    fn generic_dispatch_consumes_the_narrow_runtime_port() {
        let (projection, _) = initialize();
        let port: &dyn RuntimeGraphStatePort = &projection;
        assert_eq!(port.declarations().len(), 1);
        assert_eq!(
            port.read("steps", &VariableScope::Run).expect("read"),
            ReadOutcome::Unassigned
        );
        assert_eq!(
            port.budget().remaining(BudgetDimension::ModelRequests),
            2
        );
        let expression =
            Expression::parse("counters.model_requests.remaining >= 1", agentmod_expression_engine::ExpressionLimits::default())
                .expect("parse");
        assert_eq!(
            port.verdict(&expression, &VariableScope::Run),
            ConditionVerdict::Eligible
        );
    }

    #[test]
    fn style_budgets_map_onto_canonical_limits() {
        let budgets = SessionStyleBudgets {
            max_iterations: 8,
            max_steps: 250,
            max_tokens: 500_000,
            max_cost_micros: 50_000_000,
            max_duration_ms: 1_800_000,
        };
        let limits = SessionGraphState::limits_from_style(&budgets);
        assert_eq!(limits.max_style_steps, Some(250));
        assert_eq!(limits.max_iterations, Some(8));
        assert_eq!(limits.max_total_tokens, Some(500_000));
        assert_eq!(limits.max_provider_cost_micros, Some(50_000_000));
        assert_eq!(limits.max_elapsed_wall_clock_ms, Some(1_800_000));
        assert!(SessionGraphState::style_wall_clock_enabled(&budgets));
    }

    #[test]
    fn reconstruct_requires_matching_session() {
        let other = SessionId::from_uuid(uuid::Uuid::nil());
        assert!(matches!(
            SessionGraphState::reconstruct(other, &[], &[]),
            Err(GraphStateLogicError::MissingInitialization)
        ));
    }
}
