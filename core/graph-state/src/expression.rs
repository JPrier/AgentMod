//! Deterministic condition evaluation from canonical graph state.
//!
//! Conditions evaluate only against canonical graph variables and canonical
//! budget counters. The environment is built from sorted sources, so results
//! never depend on incidental JSON object ordering or live external state.
//! Outcomes are stable: eligible, ineligible, missing required input, or
//! invalid expression/type.

use agentmod_expression_engine::{EnvironmentPath, EvaluationError, Expression, Operand};
use serde_json::Value;

use crate::{declare::VariableScope, state::GraphState};

/// Root namespace for canonical budget counters in conditions.
pub const COUNTERS_ROOT: &str = "counters";

/// Stable condition outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionVerdict {
    /// The condition is true and every required input is present.
    Eligible,
    /// The condition is false with every required input present.
    Ineligible,
    /// Declared variables referenced by the condition are unassigned.
    MissingRequiredInput {
        /// Declared names, sorted and deduplicated.
        missing: Vec<String>,
    },
    /// The condition references an undeclared variable, unknown counter, or
    /// produces a type error.
    InvalidExpression {
        /// Deterministic diagnostic.
        reason: String,
    },
}

impl ConditionVerdict {
    /// Returns whether the verdict is a definite boolean outcome.
    #[must_use]
    pub const fn is_definite(&self) -> bool {
        matches!(self, Self::Eligible | Self::Ineligible)
    }
}

/// Evaluates a condition against canonical state and counters.
///
/// # Errors
///
/// No errors escape; every failure is a stable [`ConditionVerdict`].
#[must_use]
pub fn evaluate_condition(
    state: &GraphState,
    counters: &Value,
    expression: &Expression,
    scope: &VariableScope,
) -> ConditionVerdict {
    let paths = collect_paths(expression);
    let mut missing = Vec::new();
    for path in &paths {
        let Some(root) = path.segments().first() else {
            return ConditionVerdict::InvalidExpression {
                reason: "empty environment path".to_owned(),
            };
        };
        let root_name = match root {
            agentmod_expression_engine::PathSegment::Key(key) => key,
            agentmod_expression_engine::PathSegment::Index(_) => {
                return ConditionVerdict::InvalidExpression {
                    reason: "path starts with an array index".to_owned(),
                };
            }
        };
        if root_name == COUNTERS_ROOT {
            // Counter paths must resolve within the canonical projection.
            if lookup(counters, path).is_none() {
                return ConditionVerdict::InvalidExpression {
                    reason: format!("unknown counter path `{path}`"),
                };
            }
            continue;
        }
        let Some(declaration) = state.declaration(root_name) else {
            return ConditionVerdict::InvalidExpression {
                reason: format!("condition references undeclared variable `{root_name}`"),
            };
        };
        let _ = declaration;
        let assigned = matches!(
            state.read(root_name, scope),
            Ok(crate::state::ReadOutcome::Value(_) | crate::state::ReadOutcome::Null)
        );
        if !assigned {
            missing.push(root_name.to_owned());
        }
    }
    missing.sort_unstable();
    missing.dedup();
    if !missing.is_empty() {
        return ConditionVerdict::MissingRequiredInput { missing };
    }

    let environment = merge_environment(state.environment(scope), counters);
    match expression.evaluate(&environment) {
        Ok(true) => ConditionVerdict::Eligible,
        Ok(false) => ConditionVerdict::Ineligible,
        Err(EvaluationError::MissingPath { path }) => ConditionVerdict::MissingRequiredInput {
            missing: vec![path],
        },
        Err(error) => ConditionVerdict::InvalidExpression {
            reason: error.to_string(),
        },
    }
}

/// Collects every environment path referenced by an expression.
#[must_use]
pub fn collect_paths(expression: &Expression) -> Vec<&EnvironmentPath> {
    let mut paths = Vec::new();
    collect_paths_into(expression, &mut paths);
    paths
}

fn collect_paths_into<'a>(expression: &'a Expression, paths: &mut Vec<&'a EnvironmentPath>) {
    match expression {
        Expression::Value(operand) => operand_paths(operand, paths),
        Expression::Not(inner) => collect_paths_into(inner, paths),
        Expression::And(left, right) | Expression::Or(left, right) => {
            collect_paths_into(left, paths);
            collect_paths_into(right, paths);
        }
        Expression::Compare { left, right, .. } => {
            operand_paths(left, paths);
            operand_paths(right, paths);
        }
        Expression::Exists(path) => paths.push(path),
    }
}

fn operand_paths<'a>(operand: &'a Operand, paths: &mut Vec<&'a EnvironmentPath>) {
    if let Operand::Path(path) = operand {
        paths.push(path);
    }
}

/// Looks a path up inside a JSON-like value.
#[must_use]
pub fn lookup<'a>(environment: &'a Value, path: &EnvironmentPath) -> Option<&'a Value> {
    let mut current = environment;
    for segment in path.segments() {
        current = match segment {
            agentmod_expression_engine::PathSegment::Key(key) => current.as_object()?.get(key)?,
            agentmod_expression_engine::PathSegment::Index(index) => {
                current.as_array()?.get(*index)?
            }
        };
    }
    Some(current)
}

/// Merges the variable environment and the counters namespace.
#[must_use]
pub fn merge_environment(variables: Value, counters: &Value) -> Value {
    let mut environment = match variables {
        Value::Object(fields) => fields,
        _ => serde_json::Map::new(),
    };
    if let Some(counters) = counters.get(COUNTERS_ROOT) {
        environment.insert(COUNTERS_ROOT.to_owned(), counters.clone());
    }
    Value::Object(environment)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use agentmod_expression_engine::{Expression, ExpressionLimits};
    use agentmod_primitives::{SessionId, TimestampMillis};

    use super::*;
    use crate::{
        budget::{BudgetLedger, BudgetLimits},
        declare::{
            DeclarationSet, MutabilityPolicy, SecurityClassification, VariableDeclaration,
            VariableScope, VariableType,
        },
        state::AssignmentSource,
        value::GraphValue,
    };

    fn declarations() -> DeclarationSet {
        let mut set = DeclarationSet::new();
        set.insert(VariableDeclaration {
            name: "ready".into(),
            r#type: VariableType::Boolean,
            scope: VariableScope::Run,
            producers: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: MutabilityPolicy::Assignable,
            max_serialized_bytes: 512,
            classification: SecurityClassification::Public,
            merge_policy: crate::declare::MergePolicy::RejectConflict,
            default: Some(GraphValue::Boolean(false)),
        })
        .expect("declared");
        set.insert(VariableDeclaration {
            name: "count".into(),
            r#type: VariableType::UnsignedInteger { min: 0, max: 100 },
            scope: VariableScope::Run,
            producers: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: MutabilityPolicy::Assignable,
            max_serialized_bytes: 512,
            classification: SecurityClassification::SessionInternal,
            merge_policy: crate::declare::MergePolicy::RejectConflict,
            default: None,
        })
        .expect("declared");
        set
    }

    fn state() -> GraphState {
        GraphState::new(SessionId::from_uuid(uuid::Uuid::nil()), declarations())
            .expect("state")
            .0
    }

    fn counters() -> Value {
        let (ledger, _) = BudgetLedger::initialize(
            SessionId::from_uuid(uuid::Uuid::nil()),
            BudgetLimits {
                max_model_requests: Some(2),
                ..BudgetLimits::default()
            },
            TimestampMillis::new(1_700_000_000_000),
            false,
        );
        ledger.budget_environment()
    }

    fn parse(source: &str) -> Expression {
        Expression::parse(source, ExpressionLimits::default()).expect("valid expression")
    }

    #[test]
    fn outcomes_are_eligible_ineligible_missing_and_invalid() {
        let state = state();
        let counters = counters();

        assert_eq!(
            evaluate_condition(
                &state,
                &counters,
                &parse("ready == true"),
                &VariableScope::Run
            ),
            ConditionVerdict::Ineligible
        );
        assert_eq!(
            evaluate_condition(
                &state,
                &counters,
                &parse("ready == false"),
                &VariableScope::Run
            ),
            ConditionVerdict::Eligible
        );
        // `count` is declared but unassigned: missing required input.
        assert_eq!(
            evaluate_condition(&state, &counters, &parse("count >= 1"), &VariableScope::Run),
            ConditionVerdict::MissingRequiredInput {
                missing: vec!["count".to_owned()]
            }
        );
        // `unknown` is undeclared: invalid expression.
        assert!(matches!(
            evaluate_condition(
                &state,
                &counters,
                &parse("unknown == true"),
                &VariableScope::Run
            ),
            ConditionVerdict::InvalidExpression { .. }
        ));
        // Type mismatch is invalid.
        assert_eq!(
            evaluate_condition(
                &state,
                &counters,
                &parse("count == true"),
                &VariableScope::Run
            ),
            ConditionVerdict::MissingRequiredInput {
                missing: vec!["count".to_owned()]
            }
        );
    }

    #[test]
    fn counters_are_canonical_inputs() {
        let state = state();
        let counters = counters();
        assert_eq!(
            evaluate_condition(
                &state,
                &counters,
                &parse("counters.model_requests.remaining >= 1"),
                &VariableScope::Run,
            ),
            ConditionVerdict::Eligible
        );
        assert!(matches!(
            evaluate_condition(
                &state,
                &counters,
                &parse("counters.missing_dimension.used == 0"),
                &VariableScope::Run,
            ),
            ConditionVerdict::InvalidExpression { .. }
        ));
    }

    #[test]
    fn assignment_order_does_not_change_verdicts() {
        let mut forward = state();
        let mut reverse = state();
        let assign = |state: &mut GraphState, node: &str| {
            let _ = state.assign(
                "count",
                GraphValue::UnsignedInteger(7),
                &AssignmentSource::Node {
                    node_id: node.to_owned(),
                },
                &VariableScope::Run,
                None,
            );
        };
        assign(&mut forward, "left");
        assign(&mut forward, "right");
        assign(&mut reverse, "right");
        assign(&mut reverse, "left");
        let counters = counters();
        for source in ["count >= 1", "count == 7", "count <= 10"] {
            assert_eq!(
                evaluate_condition(&forward, &counters, &parse(source), &VariableScope::Run,),
                evaluate_condition(&reverse, &counters, &parse(source), &VariableScope::Run,),
                "verdict must not depend on assignment order for {source}"
            );
        }
    }

    #[test]
    fn environment_projection_is_sorted_and_stable() {
        let mut state = state();
        let _ = state.assign(
            "count",
            GraphValue::UnsignedInteger(3),
            &AssignmentSource::Runtime,
            &VariableScope::Run,
            None,
        );
        let environment = state.environment(&VariableScope::Run);
        let bytes = serde_json::to_vec(&environment).expect("serialize");
        assert_eq!(serde_json::to_vec(&environment).expect("again"), bytes);
    }
}
