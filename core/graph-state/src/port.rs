//! Narrow read ports for generic dispatch.
//!
//! Generic dispatch consumes graph state exclusively through these ports:
//! typed declarations, validated reads, deterministic condition verdicts, and
//! the canonical budget projection. No external SDK or frontend type crosses
//! the port boundary.

use agentmod_expression_engine::Expression;
use serde_json::Value;

use crate::{
    budget::BudgetLedger,
    declare::{DeclarationSet, VariableScope},
    expression::ConditionVerdict,
    state::{GraphState, GraphStateError, ReadOutcome},
};

/// Narrow read-only port for canonical graph variables.
pub trait GraphStateReadPort {
    /// Returns the declaration set.
    fn declarations(&self) -> &DeclarationSet;

    /// Reads one variable from a scope.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError`] for undeclared reads or unknown scopes.
    fn read(&self, name: &str, scope: &VariableScope) -> Result<ReadOutcome<'_>, GraphStateError>;

    /// Evaluates one condition against canonical variables deterministically.
    fn verdict(&self, expression: &Expression, scope: &VariableScope) -> ConditionVerdict;

    /// Returns the deterministic variable environment for a scope.
    fn environment(&self, scope: &VariableScope) -> Value;
}

impl GraphStateReadPort for GraphState {
    fn declarations(&self) -> &DeclarationSet {
        GraphState::declarations(self)
    }

    fn read(&self, name: &str, scope: &VariableScope) -> Result<ReadOutcome<'_>, GraphStateError> {
        GraphState::read(self, name, scope)
    }

    fn verdict(&self, expression: &Expression, scope: &VariableScope) -> ConditionVerdict {
        let counters = Value::Object(serde_json::Map::new());
        crate::expression::evaluate_condition(self, &counters, expression, scope)
    }

    fn environment(&self, scope: &VariableScope) -> Value {
        GraphState::environment(self, scope)
    }
}

/// Narrow read-only port for canonical budget accounting.
pub trait BudgetReadPort {
    /// Returns the canonical budget ledger.
    fn budget(&self) -> &BudgetLedger;

    /// Returns the deterministic budget counters environment.
    fn budget_environment(&self) -> Value;
}

impl BudgetReadPort for BudgetLedger {
    fn budget(&self) -> &BudgetLedger {
        self
    }

    fn budget_environment(&self) -> Value {
        BudgetLedger::budget_environment(self)
    }
}

/// Composite execution graph state consumed by generic dispatch.
///
/// Owns the canonical variable state and the budget ledger for one session.
#[derive(Clone, Debug)]
pub struct ExecutionGraphState {
    state: GraphState,
    ledger: BudgetLedger,
}

impl ExecutionGraphState {
    /// Creates the composite from its two canonical halves.
    #[must_use]
    pub const fn new(state: GraphState, ledger: BudgetLedger) -> Self {
        Self { state, ledger }
    }

    /// Returns the variable state.
    #[must_use]
    pub const fn state(&self) -> &GraphState {
        &self.state
    }

    /// Returns the budget ledger.
    #[must_use]
    pub const fn ledger(&self) -> &BudgetLedger {
        &self.ledger
    }
}

impl GraphStateReadPort for ExecutionGraphState {
    fn declarations(&self) -> &DeclarationSet {
        self.state.declarations()
    }

    fn read(&self, name: &str, scope: &VariableScope) -> Result<ReadOutcome<'_>, GraphStateError> {
        self.state.read(name, scope)
    }

    fn verdict(&self, expression: &Expression, scope: &VariableScope) -> ConditionVerdict {
        let counters = self.ledger.budget_environment();
        crate::expression::evaluate_condition(&self.state, &counters, expression, scope)
    }

    fn environment(&self, scope: &VariableScope) -> Value {
        self.state.environment(scope)
    }
}

impl BudgetReadPort for ExecutionGraphState {
    fn budget(&self) -> &BudgetLedger {
        &self.ledger
    }

    fn budget_environment(&self) -> Value {
        self.ledger.budget_environment()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use agentmod_expression_engine::{Expression, ExpressionLimits};
    use agentmod_primitives::{SessionId, TimestampMillis};

    use super::*;
    use crate::{
        budget::BudgetLimits,
        declare::{MutabilityPolicy, SecurityClassification, VariableDeclaration, VariableType},
        state::AssignmentSource,
        value::GraphValue,
    };

    fn declaration(name: &str) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            r#type: VariableType::UnsignedInteger { min: 0, max: 100 },
            scope: VariableScope::Run,
            producers: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: MutabilityPolicy::Assignable,
            max_serialized_bytes: 512,
            classification: SecurityClassification::SessionInternal,
            merge_policy: crate::declare::MergePolicy::RejectConflict,
            default: None,
        }
    }

    #[test]
    fn dispatch_consumes_state_through_the_narrow_ports() {
        let mut set = crate::declare::DeclarationSet::new();
        set.insert(declaration("steps")).expect("declared");
        let session = SessionId::from_uuid(uuid::Uuid::nil());
        let (state, _) = GraphState::new(session, set).expect("state");
        let (ledger, _) = BudgetLedger::initialize(
            session,
            BudgetLimits {
                max_model_requests: Some(2),
                ..BudgetLimits::default()
            },
            TimestampMillis::new(1_700_000_000_000),
            false,
        );
        let composite = ExecutionGraphState::new(state, ledger);
        let variables: &dyn GraphStateReadPort = &composite;
        let budget: &dyn BudgetReadPort = &composite;
        assert_eq!(variables.declarations().len(), 1);
        assert_eq!(
            variables.read("steps", &VariableScope::Run).expect("read"),
            ReadOutcome::Unassigned
        );
        // Rebound through a mutable state for assignment, then rebuild.
        let mut state = composite.state().clone();
        let _ = state.assign(
            "steps",
            GraphValue::UnsignedInteger(4),
            &AssignmentSource::Runtime,
            &VariableScope::Run,
            None,
        );
        let composite = ExecutionGraphState::new(state, composite.ledger().clone());
        let variables: &dyn GraphStateReadPort = &composite;
        let expression = Expression::parse(
            "steps >= 3 && counters.model_requests.remaining >= 1",
            ExpressionLimits::default(),
        )
        .expect("parse");
        assert_eq!(
            variables.verdict(&expression, &VariableScope::Run),
            ConditionVerdict::Eligible
        );
        assert_eq!(variables.environment(&VariableScope::Run)["steps"], 4);
        assert_eq!(
            budget.budget_environment()["counters"]["model_requests"]["remaining"],
            2
        );
    }
}
