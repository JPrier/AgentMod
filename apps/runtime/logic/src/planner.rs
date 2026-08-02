//! Structured planner-output parsing and validation.
//!
//! A planner model response is rejected unless every task carries a complete,
//! bounded, internally consistent schema and the plan as a whole satisfies
//! parent limits. Nothing in this module touches effects: the validated task
//! set is committed canonically before any child is created.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_session_style_sdk::ChildWorkspaceMode;
use serde_json::Value;
use thiserror::Error;

use crate::session::{PlannedTask, TaskRetryPolicy, TaskRisk};

/// Hard bounds for planner output parsing.
pub(crate) const MAX_TASK_TEXT_LEN: usize = 64 * 1024;
pub(crate) const MAX_ENTRIES_PER_TASK: usize = 64;
pub(crate) const MAX_DEPENDENCIES_PER_TASK: usize = 64;
pub(crate) const MAX_RETRYABLE_FAILURES: usize = 32;

/// Planner-visible context used to validate a candidate plan.
#[derive(Clone, Debug)]
pub struct PlannerValidationContext {
    /// Hard child count ceiling from the child-agent policy.
    pub max_children: u32,
    /// Hard concurrent ceiling from the child-agent policy.
    pub max_concurrent: u32,
    /// Parent session token ceiling.
    pub parent_max_tokens: u64,
    /// Parent session cost ceiling in micro-dollars.
    pub parent_max_cost_micros: u64,
    /// Parent session step ceiling.
    pub parent_max_steps: u64,
    /// Tool groups available to the parent compiled style.
    pub available_tool_groups: BTreeSet<String>,
    /// Child style selectors available to this runtime.
    pub available_styles: BTreeSet<String>,
    /// Harness identifiers available to this runtime.
    pub available_harnesses: BTreeSet<String>,
    /// Default child style selector from the child-agent policy.
    pub default_child_style: String,
    /// Default child workspace mode from the child-agent policy.
    pub default_workspace_mode: String,
    /// Default child token budget from the child-agent policy.
    pub default_token_budget: u64,
    /// Default child cost budget from the child-agent policy.
    pub default_cost_budget_micros: u64,
    /// Default child step budget from the parent session.
    pub default_max_steps: u64,
}

/// Parses and fully validates the structured planner output.
///
/// # Errors
///
/// Returns [`PlannerValidationError`] for invalid JSON, malformed or
/// unbounded tasks, duplicate or cyclic dependencies, unavailable tools,
/// styles, or harnesses, invalid workspace policies, or task totals that
/// exceed parent limits.
pub(crate) fn parse_and_validate_plan(
    text: &str,
    context: &PlannerValidationContext,
) -> Result<Vec<PlannedTask>, PlannerValidationError> {
    if text.trim().is_empty() || text.len() > 256 * 1024 {
        return Err(PlannerValidationError::InvalidJson);
    }
    let value: Value =
        serde_json::from_str(text).map_err(|_| PlannerValidationError::InvalidJson)?;
    let tasks = value
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or(PlannerValidationError::InvalidJson)?;
    if tasks.len() < 2 {
        return Err(PlannerValidationError::TooFewTasks);
    }
    if u32::try_from(tasks.len()).map_or(true, |count| count > context.max_children) {
        return Err(PlannerValidationError::TooManyTasks {
            count: tasks.len(),
            limit: context.max_children,
        });
    }
    let mut parsed = BTreeMap::new();
    for task in tasks {
        let parsed_task = parse_task(task, context)?;
        if parsed
            .insert(parsed_task.task_id.clone(), parsed_task)
            .is_some()
        {
            return Err(PlannerValidationError::DuplicateTaskId);
        }
    }
    validate_dependencies(&parsed)?;
    validate_totals(&parsed, context)?;
    Ok(parsed.into_values().collect())
}

fn parse_task(
    task: &Value,
    context: &PlannerValidationContext,
) -> Result<PlannedTask, PlannerValidationError> {
    let task_id = bounded_text(task, "task_id")
        .or_else(|| bounded_text(task, "id"))
        .ok_or(PlannerValidationError::InvalidTaskId)?;
    let description = bounded_text(task, "description").ok_or(PlannerValidationError::InvalidTask)?;
    let goal = bounded_text(task, "goal").unwrap_or_else(|| description.clone());
    let scope = bounded_string_list(task, "scope", MAX_ENTRIES_PER_TASK)?;
    let dependencies = bounded_string_list(task, "dependencies", MAX_DEPENDENCIES_PER_TASK)?;
    let expected_artifacts = bounded_string_list(task, "expected_artifacts", MAX_ENTRIES_PER_TASK)?;
    let workspace_mode = task
        .get("workspace_mode")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 64)
        .unwrap_or(&context.default_workspace_mode)
        .to_owned();
    validate_workspace_mode(&workspace_mode, task)?;
    let tool_groups = bounded_string_list(task, "tool_groups", MAX_ENTRIES_PER_TASK)?;
    for group in &tool_groups {
        if !context.available_tool_groups.contains(group) {
            return Err(PlannerValidationError::UnavailableToolGroup {
                task_id: task_id.clone(),
                group: group.clone(),
            });
        }
    }
    if let Some(style_selector) = task
        .get("style")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        && !context.available_styles.contains(style_selector)
    {
        return Err(PlannerValidationError::UnavailableStyle {
            task_id: task_id.clone(),
            style: style_selector.to_owned(),
        });
    }
    if let Some(harness) = task
        .get("harness")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        && !context.available_harnesses.contains(harness)
    {
        return Err(PlannerValidationError::UnavailableHarness {
            task_id: task_id.clone(),
            harness: harness.to_owned(),
        });
    }
    let validation_commands =
        bounded_string_list(task, "validation_commands", MAX_ENTRIES_PER_TASK)?;
    let completion_criteria =
        bounded_string_list(task, "completion_criteria", MAX_ENTRIES_PER_TASK)?;
    let review_criteria = bounded_string_list(task, "review_criteria", MAX_ENTRIES_PER_TASK)?;
    let token_budget = explicit_u64(task, "token_budget")
        .unwrap_or(context.default_token_budget);
    let cost_budget_micros = explicit_u64(task, "cost_budget_micros")
        .unwrap_or(context.default_cost_budget_micros);
    let max_steps = explicit_u32(task, "max_steps")
        .unwrap_or(u32::try_from(context.default_max_steps).unwrap_or(u32::MAX));
    let retry_policy = parse_retry_policy(task)?;
    let risk = task
        .get("risk")
        .and_then(Value::as_str)
        .map(parse_risk)
        .unwrap_or_default();
    if token_budget == 0 || cost_budget_micros == 0 || max_steps == 0 {
        return Err(PlannerValidationError::UnboundedTask {
            task_id: task_id.clone(),
        });
    }
    Ok(PlannedTask {
        task_id,
        description,
        goal,
        scope,
        dependencies,
        expected_artifacts,
        workspace_mode,
        tool_groups,
        validation_commands,
        completion_criteria,
        review_criteria,
        token_budget,
        cost_budget_micros,
        max_steps,
        retry_policy,
        risk,
    })
}

fn parse_retry_policy(task: &Value) -> Result<TaskRetryPolicy, PlannerValidationError> {
    let Some(policy) = task.get("retry_policy") else {
        return Ok(TaskRetryPolicy::default());
    };
    if !policy.is_object() {
        return Err(PlannerValidationError::InvalidRetryPolicy);
    }
    let max_attempts: u32 = policy
        .get("max_attempts")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0 && *value <= 16)
        .unwrap_or(2)
        .try_into()
        .map_err(|_| PlannerValidationError::InvalidRetryPolicy)?;
    let retryable_failures = policy
        .get("retryable_failures")
        .and_then(Value::as_array)
        .map_or_else(Vec::new, |values| {
            values
                .iter()
                .filter_map(|value| {
                    value
                        .as_str()
                        .filter(|value| !value.trim().is_empty() && value.len() <= 128)
                        .map(str::to_owned)
                })
                .collect()
        });
    if retryable_failures.len() > MAX_RETRYABLE_FAILURES {
        return Err(PlannerValidationError::InvalidRetryPolicy);
    }
    Ok(TaskRetryPolicy {
        max_attempts,
        retryable_failures,
    })
}

fn parse_risk(value: &str) -> TaskRisk {
    match value.to_ascii_lowercase().as_str() {
        "high" => TaskRisk::High,
        "medium" | "moderate" => TaskRisk::Medium,
        _ => TaskRisk::Low,
    }
}

fn validate_workspace_mode(
    mode: &str,
    task: &Value,
) -> Result<(), PlannerValidationError> {
    let supported = matches!(
        mode,
        "shared_read_only"
            | "shared_serialized_writes"
            | "independent_git_worktree"
            | "temporary_copy"
            | "explicit_custom_workspace"
    );
    if !supported {
        return Err(PlannerValidationError::InvalidWorkspaceMode {
            task_id: bounded_text(task, "task_id")
                .or_else(|| bounded_text(task, "id"))
                .unwrap_or_default(),
            mode: mode.to_owned(),
        });
    }
    if mode == "explicit_custom_workspace"
        && task
            .get("custom_workspace")
            .and_then(Value::as_object)
            .is_none()
    {
        return Err(PlannerValidationError::InvalidWorkspaceMode {
            task_id: bounded_text(task, "task_id")
                .or_else(|| bounded_text(task, "id"))
                .unwrap_or_default(),
            mode: mode.to_owned(),
        });
    }
    Ok(())
}

fn validate_dependencies(
    tasks: &BTreeMap<String, PlannedTask>,
) -> Result<(), PlannerValidationError> {
    for task in tasks.values() {
        let mut visited = BTreeSet::new();
        let mut stack = Vec::new();
        visit_dependencies(tasks, &task.task_id, &mut visited, &mut stack)?;
    }
    Ok(())
}

fn visit_dependencies(
    tasks: &BTreeMap<String, PlannedTask>,
    task_id: &str,
    visited: &mut BTreeSet<String>,
    stack: &mut Vec<String>,
) -> Result<(), PlannerValidationError> {
    if stack.iter().any(|candidate| candidate == task_id) {
        return Err(PlannerValidationError::CyclicDependency {
            task_id: task_id.to_owned(),
        });
    }
    if !visited.insert(task_id.to_owned()) {
        return Ok(());
    }
    stack.push(task_id.to_owned());
    let task = tasks.get(task_id).ok_or(PlannerValidationError::MissingDependency {
        task_id: task_id.to_owned(),
    })?;
    for dependency in &task.dependencies {
        if !tasks.contains_key(dependency) {
            return Err(PlannerValidationError::MissingDependency {
                task_id: dependency.clone(),
            });
        }
        visit_dependencies(tasks, dependency, visited, stack)?;
    }
    stack.pop();
    Ok(())
}

fn validate_totals(
    tasks: &BTreeMap<String, PlannedTask>,
    context: &PlannerValidationContext,
) -> Result<(), PlannerValidationError> {
    let tokens = tasks
        .values()
        .try_fold(0_u64, |total, task| total.checked_add(task.token_budget))
        .ok_or(PlannerValidationError::TotalBudgetOverflow)?;
    let cost = tasks
        .values()
        .try_fold(0_u64, |total, task| {
            total.checked_add(task.cost_budget_micros)
        })
        .ok_or(PlannerValidationError::TotalBudgetOverflow)?;
    let steps = tasks
        .values()
        .try_fold(0_u32, |total, task| total.checked_add(task.max_steps))
        .ok_or(PlannerValidationError::TotalBudgetOverflow)?;
    if tokens > context.parent_max_tokens {
        return Err(PlannerValidationError::TotalTokenBudgetExceeded {
            total: tokens,
            limit: context.parent_max_tokens,
        });
    }
    if cost > context.parent_max_cost_micros {
        return Err(PlannerValidationError::TotalCostBudgetExceeded {
            total: cost,
            limit: context.parent_max_cost_micros,
        });
    }
    if u64::from(steps) > context.parent_max_steps {
        return Err(PlannerValidationError::TotalStepBudgetExceeded {
            total: steps,
            limit: context.parent_max_steps,
        });
    }
    if u64::from(context.max_concurrent) == 0
        || context.max_concurrent > context.max_children
    {
        return Err(PlannerValidationError::InvalidConcurrencyLimit);
    }
    Ok(())
}

fn bounded_text(task: &Value, key: &str) -> Option<String> {
    task.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= MAX_TASK_TEXT_LEN)
        .map(str::to_owned)
}

fn bounded_string_list(
    task: &Value,
    key: &str,
    limit: usize,
) -> Result<Vec<String>, PlannerValidationError> {
    let Some(values) = task.get(key) else {
        return Ok(Vec::new());
    };
    let Some(values) = values.as_array() else {
        return Err(PlannerValidationError::InvalidTask);
    };
    if values.len() > limit {
        return Err(PlannerValidationError::InvalidTask);
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty() && value.len() <= MAX_TASK_TEXT_LEN)
                .map(str::to_owned)
                .ok_or(PlannerValidationError::InvalidTask)
        })
        .collect()
}

/// Reads an explicit bounded unsigned integer, preserving explicit zero so
/// the unbounded-task rejection can distinguish it from a missing value.
fn explicit_u64(task: &Value, key: &str) -> Option<u64> {
    task.get(key)
        .and_then(Value::as_u64)
        .filter(|value| *value <= 1_000_000_000_000)
}

fn explicit_u32(task: &Value, key: &str) -> Option<u32> {
    task.get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value <= 1_000_000)
}

/// Returns the canonical workspace-mode string for a style selection.
#[must_use]
pub fn workspace_mode_string(mode: ChildWorkspaceMode) -> String {
    match mode {
        ChildWorkspaceMode::SharedReadOnly => String::from("shared_read_only"),
        ChildWorkspaceMode::SharedSerializedWrites => String::from("shared_serialized_writes"),
        ChildWorkspaceMode::IndependentGitWorktree => String::from("independent_git_worktree"),
        ChildWorkspaceMode::TemporaryCopy => String::from("temporary_copy"),
        ChildWorkspaceMode::ExplicitCustomWorkspace => String::from("explicit_custom_workspace"),
    }
}

/// Structured planner-output validation failure.
#[allow(
    missing_docs,
    reason = "logic-local validation diagnostics are self-describing"
)]
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PlannerValidationError {
    /// Planner output is not valid bounded JSON.
    #[error("planner output is not valid bounded JSON")]
    InvalidJson,
    /// The plan contains fewer than two tasks.
    #[error("planner output must contain at least two tasks")]
    TooFewTasks,
    /// The plan exceeds the child-count ceiling.
    #[error("planner output contains {count} tasks exceeding the {limit} limit")]
    TooManyTasks {
        /// Task count that exceeded the ceiling.
        count: usize,
        /// Parent child-count ceiling.
        limit: u32,
    },
    /// A task has an invalid or missing identifier.
    #[error("planner task has an invalid identifier")]
    InvalidTaskId,
    /// A task record is malformed or unbounded.
    #[error("planner task record is malformed or unbounded")]
    InvalidTask,
    /// Two tasks share the same identifier.
    #[error("planner output contains duplicate task identifiers")]
    DuplicateTaskId,
    /// A task references a dependency that does not exist.
    #[error("planner task references a missing dependency `{task_id}`")]
    MissingDependency {
        /// Referenced dependency identifier.
        task_id: String,
    },
    /// Task dependencies form a cycle.
    #[error("planner task dependencies form a cycle at `{task_id}`")]
    CyclicDependency {
        /// Task identifier where the cycle was detected.
        task_id: String,
    },
    /// A task requests a tool group unavailable to the parent style.
    #[error("planner task `{task_id}` requests unavailable tool group `{group}`")]
    UnavailableToolGroup { task_id: String, group: String },
    /// A task requests a style unavailable to this runtime.
    #[error("planner task `{task_id}` requests unavailable style `{style}`")]
    UnavailableStyle { task_id: String, style: String },
    /// A task requests a harness unavailable to this runtime.
    #[error("planner task `{task_id}` requests unavailable harness `{harness}`")]
    UnavailableHarness { task_id: String, harness: String },
    /// A task declares an unsupported or incomplete workspace policy.
    #[error("planner task `{task_id}` has invalid workspace policy `{mode}`")]
    InvalidWorkspaceMode { task_id: String, mode: String },
    /// A task is missing required bounded budgets.
    #[error("planner task `{task_id}` is unbounded")]
    UnboundedTask { task_id: String },
    /// Task totals overflow.
    #[error("planner task totals overflow parent limits")]
    TotalBudgetOverflow,
    /// Task token totals exceed the parent ceiling.
    #[error("planner token totals {total} exceed the parent limit {limit}")]
    TotalTokenBudgetExceeded { total: u64, limit: u64 },
    /// Task cost totals exceed the parent ceiling.
    #[error("planner cost totals {total} exceed the parent limit {limit}")]
    TotalCostBudgetExceeded { total: u64, limit: u64 },
    /// Task step totals exceed the parent ceiling.
    #[error("planner step totals {total} exceed the parent limit {limit}")]
    TotalStepBudgetExceeded { total: u32, limit: u64 },
    /// The concurrency ceiling is invalid.
    #[error("planner concurrency ceiling is invalid")]
    InvalidConcurrencyLimit,
    /// Task retry policy is malformed.
    #[error("planner task retry policy is malformed")]
    InvalidRetryPolicy,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn context() -> PlannerValidationContext {
        PlannerValidationContext {
            max_children: 16,
            max_concurrent: 4,
            parent_max_tokens: 1_000_000,
            parent_max_cost_micros: 100_000_000,
            parent_max_steps: 1_000,
            available_tool_groups: BTreeSet::from([
                String::from("filesystem.read"),
                String::from("filesystem.write"),
            ]),
            available_styles: BTreeSet::from([String::from("ephemeral-turn@1.1.0")]),
            available_harnesses: BTreeSet::from([String::from("native")]),
            default_child_style: String::from("ephemeral-turn@1.1.0"),
            default_workspace_mode: String::from("shared_read_only"),
            default_token_budget: 100_000,
            default_cost_budget_micros: 10_000_000,
            default_max_steps: 64,
        }
    }

    fn plan(tasks: &str) -> String {
        format!(r#"{{"tasks":{tasks}}}"#)
    }

    fn valid_tasks() -> &'static str {
        r#"[{"task_id":"task-1","description":"read the runtime","goal":"read the runtime","dependencies":[],"workspace_mode":"shared_read_only","tool_groups":["filesystem.read"],"validation_commands":["cargo check"],"completion_criteria":["journal valid"],"review_criteria":["no writes"],"token_budget":5000,"cost_budget_micros":100000,"max_steps":8,"risk":"low"},{"task_id":"task-2","description":"write a fixture","goal":"write a fixture","dependencies":["task-1"],"workspace_mode":"shared_serialized_writes","tool_groups":["filesystem.read","filesystem.write"],"validation_commands":["cargo test"],"completion_criteria":["test passes"],"review_criteria":["diff bounded"],"token_budget":5000,"cost_budget_micros":100000,"max_steps":8,"risk":"medium"}]"#
    }

    #[test]
    fn valid_plan_with_dependencies_parses() {
        let parsed = parse_and_validate_plan(&plan(valid_tasks()), &context()).expect("plan");
        assert_eq!(parsed.len(), 2);
        let second = parsed.iter().find(|task| task.task_id == "task-2").unwrap();
        assert_eq!(second.dependencies, ["task-1"]);
        assert_eq!(second.risk, TaskRisk::Medium);
        assert_eq!(second.workspace_mode, "shared_serialized_writes");
        assert_eq!(second.tool_groups, ["filesystem.read", "filesystem.write"]);
    }

    #[test]
    fn duplicate_ids_are_rejected() {
        let tasks = r#"[{"task_id":"task-1","description":"a"},{"task_id":"task-1","description":"b"}]"#;
        assert_eq!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("duplicate"),
            PlannerValidationError::DuplicateTaskId
        );
    }

    #[test]
    fn missing_dependency_is_rejected() {
        let tasks = r#"[{"task_id":"task-1","description":"a","dependencies":["ghost"]},{"task_id":"task-2","description":"b"}]"#;
        assert_eq!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("missing"),
            PlannerValidationError::MissingDependency {
                task_id: String::from("ghost")
            }
        );
    }

    #[test]
    fn cyclic_dependency_is_rejected() {
        let tasks = r#"[{"task_id":"task-1","description":"a","dependencies":["task-2"]},{"task_id":"task-2","description":"b","dependencies":["task-1"]}]"#;
        assert_eq!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("cycle"),
            PlannerValidationError::CyclicDependency {
                task_id: String::from("task-1")
            }
        );
    }

    #[test]
    fn unavailable_tool_group_is_rejected() {
        let tasks = r#"[{"task_id":"task-1","description":"a","tool_groups":["git.write"]},{"task_id":"task-2","description":"b"}]"#;
        assert_eq!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("tool"),
            PlannerValidationError::UnavailableToolGroup {
                task_id: String::from("task-1"),
                group: String::from("git.write"),
            }
        );
    }

    #[test]
    fn invalid_workspace_policy_is_rejected() {
        let tasks = r#"[{"task_id":"task-1","description":"a","workspace_mode":"unbounded"},{"task_id":"task-2","description":"b"}]"#;
        assert_eq!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("mode"),
            PlannerValidationError::InvalidWorkspaceMode {
                task_id: String::from("task-1"),
                mode: String::from("unbounded"),
            }
        );
    }

    #[test]
    fn custom_workspace_requires_explicit_configuration() {
        let tasks = r#"[{"task_id":"task-1","description":"a","workspace_mode":"explicit_custom_workspace"},{"task_id":"task-2","description":"b"}]"#;
        assert!(matches!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("custom"),
            PlannerValidationError::InvalidWorkspaceMode { .. }
        ));
    }

    #[test]
    fn unbounded_task_is_rejected() {
        let tasks = r#"[{"task_id":"task-1","description":"a","token_budget":0},{"task_id":"task-2","description":"b"}]"#;
        assert_eq!(
            parse_and_validate_plan(&plan(tasks), &context()).expect_err("unbounded"),
            PlannerValidationError::UnboundedTask {
                task_id: String::from("task-1")
            }
        );
    }

    #[test]
    fn total_token_budget_is_enforced() {
        let mut context = context();
        context.parent_max_tokens = 5_000;
        assert_eq!(
            parse_and_validate_plan(&plan(valid_tasks()), &context).expect_err("total"),
            PlannerValidationError::TotalTokenBudgetExceeded {
                total: 10_000,
                limit: 5_000,
            }
        );
    }

    #[test]
    fn too_many_tasks_is_rejected() {
        let mut context = context();
        context.max_children = 1;
        assert_eq!(
            parse_and_validate_plan(&plan(valid_tasks()), &context).expect_err("count"),
            PlannerValidationError::TooManyTasks {
                count: 2,
                limit: 1,
            }
        );
    }

    #[test]
    fn defaulted_budgets_and_modes_are_applied() {
        let tasks = r#"[{"task_id":"task-1","description":"a"},{"task_id":"task-2","description":"b","risk":"high"}]"#;
        let parsed = parse_and_validate_plan(&plan(tasks), &context()).expect("plan");
        assert!(parsed.iter().all(|task| {
            task.token_budget == 100_000
                && task.cost_budget_micros == 10_000_000
                && task.max_steps == 64
                && task.workspace_mode == "shared_read_only"
        }));
        assert!(parsed
            .iter()
            .any(|task| task.risk == TaskRisk::High));
    }
}
