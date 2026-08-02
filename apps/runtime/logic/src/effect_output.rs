//! Pure, declaration-driven projection of bounded node effect results.
//!
//! The projector owns no effect, persistence, or graph-transition authority.
//! It converts runtime-supplied result slots and explicitly named ordinary
//! values into one typed transition object. Slot selection depends only on
//! compiled write declarations and value types, never node or variable names.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_engine::{
    SecurityClassification, VariableDeclaration, VariableScope, VariableValueType,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::canonical_variables::{
    BranchWriteContext, CanonicalApprovalResult, CanonicalVariableEnvironment,
    VariableEnvironmentLimits, VariableWriter, canonical_value_from_json,
};

const MAX_EFFECT_OUTPUT_FIELDS: usize = 256;
const MAX_CHILD_RESULTS: usize = 256;
const MAX_EFFECT_OUTPUT_BYTES: usize = 1024 * 1024;

/// Bounded values supplied by one already-authorized runtime effect.
///
/// Ordinary fields are keyed by exact declared variable name. Every other
/// field is a single runtime-owned typed slot.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EffectResultSlots {
    /// Explicit ordinary transition values by declared variable name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ordinary: BTreeMap<String, Value>,
    /// Exact node-result receipt reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_result_reference: Option<String>,
    /// Exact tool-result receipt reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_result_reference: Option<String>,
    /// Runtime-owned approval disposition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_result: Option<CanonicalApprovalResult>,
    /// Immutable artifact reference.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_reference: Option<String>,
    /// Singular child-session result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_id: Option<String>,
    /// Plural child-session result in canonical runtime order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_ids: Option<Vec<String>>,
    /// Runtime-recorded Unix timestamp in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_millis: Option<i64>,
    /// Runtime-recorded duration in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_millis: Option<u64>,
}

/// Runtime value that must be retained canonically for deterministic replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RecordedRuntimeValue {
    /// Exact runtime-selected timestamp.
    TimestampMillis(i64),
    /// Exact runtime-selected duration.
    DurationMillis(u64),
}

/// Pure effect-output projection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectedEffectOutput {
    /// Exact JSON object used by graph transitions and variable coordination.
    pub transition_variables: Value,
    /// Runtime time values that replay must reuse rather than resolve again.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub recorded_runtime_values: BTreeMap<String, RecordedRuntimeValue>,
}

/// Projects one effect result against the exact compiled write contract.
///
/// # Errors
///
/// Returns [`EffectOutputError`] for undeclared or misowned writes, missing,
/// extra, duplicated, ambiguous, prohibited, incompatible, or oversized
/// values.
pub fn project_effect_output(
    node_id: &str,
    write_variables: &BTreeSet<String>,
    declarations: &[VariableDeclaration],
    slots: EffectResultSlots,
) -> Result<ProjectedEffectOutput, EffectOutputError> {
    project_effect_output_in_context(node_id, write_variables, declarations, None, slots)
}

/// Projects one effect result produced inside an exact parallel branch.
///
/// Shared run/session values remain complete branch contributions; this pure
/// projector does not apply their merge policy. Branch-scoped values retain
/// and validate the supplied branch identity.
///
/// # Errors
///
/// Returns [`EffectOutputError`] under the same fail-closed rules as
/// [`project_effect_output`].
pub fn project_branch_effect_output(
    node_id: &str,
    write_variables: &BTreeSet<String>,
    declarations: &[VariableDeclaration],
    branch: &BranchWriteContext,
    slots: EffectResultSlots,
) -> Result<ProjectedEffectOutput, EffectOutputError> {
    project_effect_output_in_context(node_id, write_variables, declarations, Some(branch), slots)
}

#[allow(
    clippy::too_many_lines,
    reason = "fail-closed declaration and effect-slot consumption stays adjacent for auditability"
)]
fn project_effect_output_in_context(
    node_id: &str,
    write_variables: &BTreeSet<String>,
    declarations: &[VariableDeclaration],
    branch: Option<&BranchWriteContext>,
    slots: EffectResultSlots,
) -> Result<ProjectedEffectOutput, EffectOutputError> {
    if node_id.is_empty() || node_id.chars().any(char::is_control) {
        return Err(EffectOutputError::InvalidNodeIdentity);
    }
    if write_variables.len() > MAX_EFFECT_OUTPUT_FIELDS
        || slots.ordinary.len() > MAX_EFFECT_OUTPUT_FIELDS
        || slots
            .child_ids
            .as_ref()
            .is_some_and(|children| children.is_empty() || children.len() > MAX_CHILD_RESULTS)
    {
        return Err(EffectOutputError::BoundExceeded);
    }
    if slots.child_id.is_some() && slots.child_ids.is_some() {
        return Err(EffectOutputError::AmbiguousChildSlots);
    }

    let mut declarations_by_name = BTreeMap::new();
    for declaration in declarations {
        if declarations_by_name
            .insert(declaration.name.as_str(), declaration)
            .is_some()
        {
            return Err(EffectOutputError::DuplicateDeclaration(
                declaration.name.clone(),
            ));
        }
    }

    let mut slot_consumers = BTreeMap::<EffectSlot, Vec<String>>::new();
    let mut selected_declarations = Vec::with_capacity(write_variables.len());
    for variable in write_variables {
        let declaration = declarations_by_name
            .get(variable.as_str())
            .copied()
            .ok_or_else(|| EffectOutputError::UndeclaredWrite(variable.clone()))?;
        let parallel_merge_write = branch.is_some()
            && matches!(
                declaration.scope,
                VariableScope::Run | VariableScope::Session
            )
            && declaration.merge_policy.is_some();
        if declaration.producer != node_id {
            if parallel_merge_write {
                if !declaration.merge_contributors.contains(node_id) {
                    return Err(EffectOutputError::InvalidValue {
                        variable: variable.clone(),
                        detail: String::from("unauthorized parallel merge contributor"),
                    });
                }
            } else {
                return Err(EffectOutputError::WrongProducer {
                    variable: variable.clone(),
                    expected: declaration.producer.clone(),
                    actual: node_id.to_owned(),
                });
            }
        }
        if declaration.security_classification == SecurityClassification::SecretReference {
            return Err(EffectOutputError::ProhibitedType(variable.clone()));
        }
        if let Some(slot) = effect_slot(&declaration.value_type)
            .map_err(|()| EffectOutputError::ProhibitedType(variable.clone()))?
        {
            slot_consumers
                .entry(slot)
                .or_default()
                .push(variable.clone());
        }
        selected_declarations.push(declaration.clone());
    }
    for (slot, consumers) in &slot_consumers {
        if consumers.len() > 1 {
            return Err(EffectOutputError::DuplicateSlotConsumers {
                slot: slot.name(),
                variables: consumers.clone(),
            });
        }
    }
    if slot_consumers.contains_key(&EffectSlot::ChildId)
        && slot_consumers.contains_key(&EffectSlot::ChildIds)
    {
        return Err(EffectOutputError::AmbiguousChildSlots);
    }

    let EffectResultSlots {
        mut ordinary,
        node_result_reference,
        tool_result_reference,
        approval_result,
        artifact_reference,
        child_id,
        child_ids,
        timestamp_millis,
        duration_millis,
    } = slots;
    let mut supplied = BTreeMap::new();
    insert_slot(
        &mut supplied,
        EffectSlot::NodeResult,
        node_result_reference.map(Value::String),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::ToolResult,
        tool_result_reference.map(Value::String),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::ApprovalResult,
        approval_result.map(|approval| Value::String(approval_text(approval).to_owned())),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::ArtifactReference,
        artifact_reference.map(Value::String),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::ChildId,
        child_id.map(Value::String),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::ChildIds,
        child_ids.map(|children| Value::Array(children.into_iter().map(Value::String).collect())),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::Timestamp,
        timestamp_millis.map(|value| Value::Number(value.into())),
    );
    insert_slot(
        &mut supplied,
        EffectSlot::Duration,
        duration_millis.map(|value| Value::Number(value.into())),
    );

    let mut environment = CanonicalVariableEnvironment::new(
        selected_declarations,
        VariableEnvironmentLimits::default(),
    )
    .map_err(|error| EffectOutputError::InvalidDeclaration(error.to_string()))?;
    let mut output = Map::new();
    let mut recorded_runtime_values = BTreeMap::new();
    for variable in write_variables {
        let declaration = declarations_by_name
            .get(variable.as_str())
            .copied()
            .expect("write declarations were resolved above");
        let slot = effect_slot(&declaration.value_type).expect("effect type was classified above");
        let value = match slot {
            Some(slot) => supplied
                .remove(&slot)
                .ok_or_else(|| EffectOutputError::MissingSlot {
                    variable: variable.clone(),
                    slot: slot.name(),
                })?,
            None => ordinary
                .remove(variable)
                .ok_or_else(|| EffectOutputError::MissingOrdinaryField(variable.clone()))?,
        };
        let canonical =
            canonical_value_from_json(&value, &declaration.value_type).map_err(|error| {
                EffectOutputError::InvalidValue {
                    variable: variable.clone(),
                    detail: error.to_string(),
                }
            })?;
        let writer_branch = if declaration.scope == VariableScope::Branch {
            branch.cloned()
        } else {
            None
        };
        let writer = match slot {
            Some(_) => VariableWriter::RuntimeRecorded {
                node_id: node_id.to_owned(),
                branch: writer_branch,
            },
            None => VariableWriter::Node {
                node_id: node_id.to_owned(),
                branch: writer_branch,
            },
        };
        if matches!(
            declaration.scope,
            VariableScope::Run | VariableScope::Session
        ) && declaration.merge_policy.is_some()
        {
            if declaration.producer != node_id && !declaration.merge_contributors.contains(node_id)
            {
                return Err(EffectOutputError::InvalidValue {
                    variable: variable.clone(),
                    detail: String::from("unauthorized parallel merge contributor"),
                });
            }
            environment
                .validate_parallel_contribution(variable, &canonical)
                .map_err(|error| EffectOutputError::InvalidValue {
                    variable: variable.clone(),
                    detail: error.to_string(),
                })?;
        } else {
            environment
                .assign(variable, writer, None, canonical)
                .map_err(|error| EffectOutputError::InvalidValue {
                    variable: variable.clone(),
                    detail: error.to_string(),
                })?;
        }
        match slot {
            Some(EffectSlot::Timestamp) => {
                let timestamp = value
                    .as_i64()
                    .expect("timestamp slot was constructed from i64");
                recorded_runtime_values.insert(
                    variable.clone(),
                    RecordedRuntimeValue::TimestampMillis(timestamp),
                );
            }
            Some(EffectSlot::Duration) => {
                let duration = value
                    .as_u64()
                    .expect("duration slot was constructed from u64");
                recorded_runtime_values.insert(
                    variable.clone(),
                    RecordedRuntimeValue::DurationMillis(duration),
                );
            }
            _ => {}
        }
        output.insert(variable.clone(), value);
    }
    if let Some(extra) = ordinary.into_keys().next() {
        return Err(EffectOutputError::ExtraOrdinaryField(extra));
    }
    if let Some((slot, _)) = supplied.into_iter().next() {
        return Err(EffectOutputError::ExtraSlot(slot.name()));
    }
    let transition_variables = Value::Object(output);
    if serde_json::to_vec(&transition_variables)
        .map_or(true, |bytes| bytes.len() > MAX_EFFECT_OUTPUT_BYTES)
    {
        return Err(EffectOutputError::BoundExceeded);
    }
    Ok(ProjectedEffectOutput {
        transition_variables,
        recorded_runtime_values,
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum EffectSlot {
    NodeResult,
    ToolResult,
    ApprovalResult,
    ArtifactReference,
    ChildId,
    ChildIds,
    Timestamp,
    Duration,
}

impl EffectSlot {
    const fn name(self) -> &'static str {
        match self {
            Self::NodeResult => "node_result_reference",
            Self::ToolResult => "tool_result_reference",
            Self::ApprovalResult => "approval_result",
            Self::ArtifactReference => "artifact_reference",
            Self::ChildId => "child_id",
            Self::ChildIds => "child_ids",
            Self::Timestamp => "timestamp",
            Self::Duration => "duration",
        }
    }
}

fn effect_slot(value_type: &VariableValueType) -> Result<Option<EffectSlot>, ()> {
    match value_type {
        VariableValueType::Boolean
        | VariableValueType::Integer
        | VariableValueType::Decimal
        | VariableValueType::String
        | VariableValueType::Enum { .. } => Ok(None),
        VariableValueType::List { item_type, .. } if **item_type == VariableValueType::ChildId => {
            Ok(Some(EffectSlot::ChildIds))
        }
        VariableValueType::List { item_type, .. }
        | VariableValueType::Map {
            value_type: item_type,
            ..
        } if ordinary_type(item_type) => Ok(None),
        VariableValueType::ArtifactReference => Ok(Some(EffectSlot::ArtifactReference)),
        VariableValueType::ChildId => Ok(Some(EffectSlot::ChildId)),
        VariableValueType::ToolResultReference => Ok(Some(EffectSlot::ToolResult)),
        VariableValueType::ApprovalResult => Ok(Some(EffectSlot::ApprovalResult)),
        VariableValueType::NodeResultReference => Ok(Some(EffectSlot::NodeResult)),
        VariableValueType::Timestamp => Ok(Some(EffectSlot::Timestamp)),
        VariableValueType::Duration => Ok(Some(EffectSlot::Duration)),
        VariableValueType::SessionId
        | VariableValueType::TaskId
        | VariableValueType::SecretReference
        | VariableValueType::List { .. }
        | VariableValueType::Map { .. } => Err(()),
    }
}

fn ordinary_type(value_type: &VariableValueType) -> bool {
    match value_type {
        VariableValueType::Boolean
        | VariableValueType::Integer
        | VariableValueType::Decimal
        | VariableValueType::String
        | VariableValueType::Enum { .. } => true,
        VariableValueType::List { item_type, .. } => ordinary_type(item_type),
        VariableValueType::Map { value_type, .. } => ordinary_type(value_type),
        VariableValueType::SessionId
        | VariableValueType::ChildId
        | VariableValueType::TaskId
        | VariableValueType::ArtifactReference
        | VariableValueType::SecretReference
        | VariableValueType::ToolResultReference
        | VariableValueType::ApprovalResult
        | VariableValueType::NodeResultReference
        | VariableValueType::Timestamp
        | VariableValueType::Duration => false,
    }
}

fn insert_slot(supplied: &mut BTreeMap<EffectSlot, Value>, slot: EffectSlot, value: Option<Value>) {
    if let Some(value) = value {
        supplied.insert(slot, value);
    }
}

const fn approval_text(value: CanonicalApprovalResult) -> &'static str {
    match value {
        CanonicalApprovalResult::Approved => "approved",
        CanonicalApprovalResult::Denied => "denied",
        CanonicalApprovalResult::Cancelled => "cancelled",
        CanonicalApprovalResult::Expired => "expired",
    }
}

/// Stable pure projection rejection.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum EffectOutputError {
    /// Producing node identity is empty or contains control characters.
    #[error("effect-output node identity is invalid")]
    InvalidNodeIdentity,
    /// Declaration or result collection exceeds a hard projector bound.
    #[error("effect-output projection exceeds its hard bound")]
    BoundExceeded,
    /// Input declarations contain the same variable more than once.
    #[error("effect-output declaration `{0}` is duplicated")]
    DuplicateDeclaration(String),
    /// A declared write has no matching declaration.
    #[error("effect-output write `{0}` is undeclared")]
    UndeclaredWrite(String),
    /// The exact node does not own a declared write.
    #[error("effect-output variable `{variable}` belongs to `{expected}`, not producer `{actual}`")]
    WrongProducer {
        /// Variable identity.
        variable: String,
        /// Declared producer.
        expected: String,
        /// Actual node.
        actual: String,
    },
    /// Secret, handle, or nested runtime-owned slot type is prohibited.
    #[error("effect-output variable `{0}` has a prohibited type")]
    ProhibitedType(String),
    /// More than one write consumes a single runtime result slot.
    #[error("effect-output variables {variables:?} duplicate slot `{slot}`")]
    DuplicateSlotConsumers {
        /// Stable slot name.
        slot: &'static str,
        /// Conflicting variables.
        variables: Vec<String>,
    },
    /// Singular and plural child results cannot be projected together.
    #[error("effect-output child result is ambiguous")]
    AmbiguousChildSlots,
    /// Required ordinary value is absent.
    #[error("effect-output ordinary field `{0}` is missing")]
    MissingOrdinaryField(String),
    /// Required runtime-owned slot is absent.
    #[error("effect-output variable `{variable}` is missing slot `{slot}`")]
    MissingSlot {
        /// Variable identity.
        variable: String,
        /// Stable slot name.
        slot: &'static str,
    },
    /// Supplied ordinary field has no declared write consumer.
    #[error("effect-output ordinary field `{0}` is extra")]
    ExtraOrdinaryField(String),
    /// Supplied runtime-owned slot has no declared write consumer.
    #[error("effect-output slot `{0}` is extra")]
    ExtraSlot(&'static str),
    /// A declaration cannot initialize the pure validation environment.
    #[error("effect-output declaration is invalid: {0}")]
    InvalidDeclaration(String),
    /// Supplied value does not match its exact declaration.
    #[error("effect-output variable `{variable}` is invalid: {detail}")]
    InvalidValue {
        /// Variable identity.
        variable: String,
        /// Stable canonical-variable validation detail.
        detail: String,
    },
}

#[cfg(test)]
mod tests {
    use agentmod_graph_engine::{
        SecurityClassification, VariableMergePolicy, VariableMutability, VariableScope,
    };
    use serde_json::json;

    use super::*;

    fn declaration(name: &str, value_type: VariableValueType) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            value_type,
            scope: VariableScope::Run,
            producer: String::from("effect"),
            merge_contributors: BTreeSet::new(),
            consumers: BTreeSet::from([String::from("next")]),
            mutability: VariableMutability::Immutable,
            merge_policy: None,
            max_size_bytes: 4_096,
            security_classification: SecurityClassification::Internal,
        }
    }

    fn project(
        declarations: &[VariableDeclaration],
        slots: EffectResultSlots,
    ) -> Result<ProjectedEffectOutput, EffectOutputError> {
        let writes = declarations
            .iter()
            .map(|declaration| declaration.name.clone())
            .collect();
        project_effect_output("effect", &writes, declarations, slots)
    }

    #[test]
    fn projects_every_runtime_owned_reference_slot() {
        for (declaration, slots, expected) in [
            (
                declaration("tool", VariableValueType::ToolResultReference),
                EffectResultSlots {
                    tool_result_reference: Some(String::from("tool-result:1")),
                    ..EffectResultSlots::default()
                },
                json!({"tool": "tool-result:1"}),
            ),
            (
                declaration("artifact", VariableValueType::ArtifactReference),
                EffectResultSlots {
                    artifact_reference: Some(String::from("artifact:blake3:1234")),
                    ..EffectResultSlots::default()
                },
                json!({"artifact": "artifact:blake3:1234"}),
            ),
            (
                declaration("child", VariableValueType::ChildId),
                EffectResultSlots {
                    child_id: Some(String::from("child-1")),
                    ..EffectResultSlots::default()
                },
                json!({"child": "child-1"}),
            ),
            (
                declaration("node", VariableValueType::NodeResultReference),
                EffectResultSlots {
                    node_result_reference: Some(String::from("node-result:1")),
                    ..EffectResultSlots::default()
                },
                json!({"node": "node-result:1"}),
            ),
        ] {
            assert_eq!(
                project(&[declaration], slots)
                    .expect("projection")
                    .transition_variables,
                expected
            );
        }
    }

    #[test]
    fn projects_approval_children_and_runtime_recorded_time() {
        let declarations = vec![
            declaration("approval", VariableValueType::ApprovalResult),
            declaration(
                "children",
                VariableValueType::List {
                    item_type: Box::new(VariableValueType::ChildId),
                    max_items: 4,
                },
            ),
            declaration("duration", VariableValueType::Duration),
            declaration("timestamp", VariableValueType::Timestamp),
        ];
        let output = project(
            &declarations,
            EffectResultSlots {
                approval_result: Some(CanonicalApprovalResult::Approved),
                child_ids: Some(vec![String::from("child-a"), String::from("child-b")]),
                timestamp_millis: Some(1_234),
                duration_millis: Some(25),
                ..EffectResultSlots::default()
            },
        )
        .expect("projection");
        assert_eq!(
            output.transition_variables,
            json!({
                "approval": "approved",
                "children": ["child-a", "child-b"],
                "duration": 25,
                "timestamp": 1234
            })
        );
        assert_eq!(
            output.recorded_runtime_values,
            BTreeMap::from([
                (
                    String::from("duration"),
                    RecordedRuntimeValue::DurationMillis(25)
                ),
                (
                    String::from("timestamp"),
                    RecordedRuntimeValue::TimestampMillis(1_234)
                ),
            ])
        );
    }

    #[test]
    fn mixed_ordinary_values_are_exactly_named_and_typed() {
        let declarations = vec![
            declaration("count", VariableValueType::Integer),
            declaration(
                "labels",
                VariableValueType::List {
                    item_type: Box::new(VariableValueType::String),
                    max_items: 3,
                },
            ),
        ];
        let output = project(
            &declarations,
            EffectResultSlots {
                ordinary: BTreeMap::from([
                    (String::from("count"), json!(3)),
                    (String::from("labels"), json!(["a", "b"])),
                ]),
                ..EffectResultSlots::default()
            },
        )
        .expect("ordinary projection");
        assert_eq!(
            output.transition_variables,
            json!({"count": 3, "labels": ["a", "b"]})
        );
    }

    #[test]
    fn legacy_no_write_projection_omits_available_effect_results() {
        let output = project_effect_output(
            "effect",
            &BTreeSet::new(),
            &[],
            EffectResultSlots::default(),
        )
        .expect("empty legacy projection");
        assert_eq!(output.transition_variables, json!({}));
        assert!(output.recorded_runtime_values.is_empty());

        assert!(matches!(
            project_effect_output(
                "effect",
                &BTreeSet::new(),
                &[],
                EffectResultSlots {
                    node_result_reference: Some(String::from("available-but-undeclared")),
                    ..EffectResultSlots::default()
                }
            ),
            Err(EffectOutputError::ExtraSlot("node_result_reference"))
        ));
    }

    #[test]
    fn missing_extra_duplicate_and_ambiguous_slots_fail_closed() {
        let tool = declaration("tool", VariableValueType::ToolResultReference);
        assert!(matches!(
            project(std::slice::from_ref(&tool), EffectResultSlots::default()),
            Err(EffectOutputError::MissingSlot { .. })
        ));
        assert!(matches!(
            project(
                std::slice::from_ref(&tool),
                EffectResultSlots {
                    tool_result_reference: Some(String::from("tool:1")),
                    artifact_reference: Some(String::from("artifact:1")),
                    ..EffectResultSlots::default()
                }
            ),
            Err(EffectOutputError::ExtraSlot("artifact_reference"))
        ));
        let mut second = tool;
        second.name = String::from("other-tool");
        assert!(matches!(
            project(
                &[
                    declaration("tool", VariableValueType::ToolResultReference),
                    second
                ],
                EffectResultSlots::default()
            ),
            Err(EffectOutputError::DuplicateSlotConsumers { .. })
        ));
        assert!(matches!(
            project(
                &[declaration("child", VariableValueType::ChildId)],
                EffectResultSlots {
                    child_id: Some(String::from("one")),
                    child_ids: Some(vec![String::from("one")]),
                    ..EffectResultSlots::default()
                }
            ),
            Err(EffectOutputError::AmbiguousChildSlots)
        ));
    }

    #[test]
    fn wrong_producer_type_and_bounds_are_rejected() {
        let mut wrong = declaration("count", VariableValueType::Integer);
        wrong.producer = String::from("other");
        assert!(matches!(
            project(
                &[wrong],
                EffectResultSlots {
                    ordinary: BTreeMap::from([(String::from("count"), json!(1))]),
                    ..EffectResultSlots::default()
                }
            ),
            Err(EffectOutputError::WrongProducer { .. })
        ));
        assert!(matches!(
            project(
                &[declaration("count", VariableValueType::Integer)],
                EffectResultSlots {
                    ordinary: BTreeMap::from([(String::from("count"), json!("wrong"))]),
                    ..EffectResultSlots::default()
                }
            ),
            Err(EffectOutputError::InvalidValue { .. })
        ));
        assert!(matches!(
            project(
                &[declaration("secret", VariableValueType::SecretReference)],
                EffectResultSlots::default()
            ),
            Err(EffectOutputError::ProhibitedType(_))
        ));
        assert!(matches!(
            project_effect_output(
                "effect",
                &BTreeSet::new(),
                &[],
                EffectResultSlots {
                    ordinary: (0..=MAX_EFFECT_OUTPUT_FIELDS)
                        .map(|index| (format!("field-{index}"), Value::Null))
                        .collect(),
                    ..EffectResultSlots::default()
                }
            ),
            Err(EffectOutputError::BoundExceeded)
        ));
    }

    #[test]
    fn serialization_is_restart_deterministic() {
        let declarations = vec![
            declaration("result", VariableValueType::NodeResultReference),
            declaration("timestamp", VariableValueType::Timestamp),
        ];
        let slots = EffectResultSlots {
            node_result_reference: Some(String::from("node-result:stable")),
            timestamp_millis: Some(42),
            ..EffectResultSlots::default()
        };
        let first = project(&declarations, slots.clone()).expect("first");
        let second = project(&declarations, slots).expect("second");
        assert_eq!(first, second);
        assert_eq!(
            serde_json::to_vec(&first).expect("first JSON"),
            serde_json::to_vec(&second).expect("second JSON")
        );
    }

    #[test]
    fn parallel_merge_declaration_remains_a_projection_not_an_implicit_merge() {
        let mut shared = declaration(
            "shared",
            VariableValueType::List {
                item_type: Box::new(VariableValueType::String),
                max_items: 4,
            },
        );
        shared.mutability = VariableMutability::Mutable;
        shared.merge_policy = Some(VariableMergePolicy::Append);
        let output = project(
            &[shared],
            EffectResultSlots {
                ordinary: BTreeMap::from([(String::from("shared"), json!(["branch-result"]))]),
                ..EffectResultSlots::default()
            },
        )
        .expect("branch contribution");
        assert_eq!(
            output.transition_variables,
            json!({"shared": ["branch-result"]})
        );
    }

    #[test]
    fn runtime_owned_effect_slot_accepts_only_explicit_parallel_contributors() {
        let mut shared = declaration("shared", VariableValueType::ToolResultReference);
        shared.producer = String::from("left_tool");
        shared.merge_contributors = BTreeSet::from([String::from("right_tool")]);
        shared.mutability = VariableMutability::Mutable;
        shared.merge_policy = Some(VariableMergePolicy::FirstBranch);
        let writes = BTreeSet::from([String::from("shared")]);
        let output = project_branch_effect_output(
            "right_tool",
            &writes,
            &[shared.clone()],
            &BranchWriteContext {
                branch_id: String::from("branch:right"),
                stable_order: 1,
                serialized_shared_write: false,
            },
            EffectResultSlots {
                tool_result_reference: Some(String::from("tool-result:right")),
                ..EffectResultSlots::default()
            },
        )
        .expect("explicit runtime-owned contribution");
        assert_eq!(
            output.transition_variables,
            json!({"shared": "tool-result:right"})
        );
        assert!(matches!(
            project_branch_effect_output(
                "unrelated_tool",
                &writes,
                &[shared],
                &BranchWriteContext {
                    branch_id: String::from("branch:unrelated"),
                    stable_order: 2,
                    serialized_shared_write: false,
                },
                EffectResultSlots {
                    tool_result_reference: Some(String::from("tool-result:unrelated")),
                    ..EffectResultSlots::default()
                },
            ),
            Err(EffectOutputError::InvalidValue { .. })
        ));
    }

    #[test]
    fn branch_scoped_output_requires_and_retains_explicit_branch_context() {
        let mut local = declaration("local", VariableValueType::String);
        local.scope = VariableScope::Branch;
        let writes = BTreeSet::from([String::from("local")]);
        let slots = EffectResultSlots {
            ordinary: BTreeMap::from([(String::from("local"), json!("branch-value"))]),
            ..EffectResultSlots::default()
        };
        assert!(matches!(
            project_effect_output("effect", &writes, &[local.clone()], slots.clone()),
            Err(EffectOutputError::InvalidValue { .. })
        ));
        let output = project_branch_effect_output(
            "effect",
            &writes,
            &[local],
            &BranchWriteContext {
                branch_id: String::from("branch:stable"),
                stable_order: 1,
                serialized_shared_write: false,
            },
            slots,
        )
        .expect("branch projection");
        assert_eq!(
            output.transition_variables,
            json!({"local": "branch-value"})
        );
    }
}
