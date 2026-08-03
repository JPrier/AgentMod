//! Pure canonical typed-variable state for generic graph execution.
//!
//! This module owns no persistence or effect boundary. It validates graph-engine
//! declarations, applies versioned writes, deterministically merges stable
//! branch results, and builds bounded expression environments suitable for
//! later canonical event integration.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_expression_engine::{EvaluationError, Expression, ExpressionLimits};
use agentmod_graph_engine::{
    SecurityClassification, VariableDeclaration, VariableMergePolicy, VariableMutability,
    VariableScope, VariableValueType,
};
use agentmod_primitives::ContentHash;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

const RUNTIME_PRODUCER: &str = "runtime";

/// Global resource limits applied in addition to each graph declaration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableEnvironmentLimits {
    /// Maximum declaration count.
    pub max_variables: usize,
    /// Maximum recursive value depth.
    pub max_value_depth: usize,
    /// Maximum total list elements and map entries in one value.
    pub max_value_items: usize,
    /// Maximum UTF-8 map-key size.
    pub max_map_key_bytes: usize,
    /// Maximum UTF-8 reference size.
    pub max_reference_bytes: usize,
    /// Maximum canonical decimal size.
    pub max_decimal_bytes: usize,
    /// Maximum aggregate serialized live-value size.
    pub max_environment_bytes: usize,
}

impl Default for VariableEnvironmentLimits {
    fn default() -> Self {
        Self {
            max_variables: 1_024,
            max_value_depth: 32,
            max_value_items: 8_192,
            max_map_key_bytes: 256,
            max_reference_bytes: 4 * 1024,
            max_decimal_bytes: 128,
            max_environment_bytes: 1024 * 1024,
        }
    }
}

/// Closed canonical approval result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalApprovalResult {
    /// User or policy approved.
    Approved,
    /// User or policy denied.
    Denied,
    /// Approval was cancelled.
    Cancelled,
    /// Approval expired.
    Expired,
}

/// A bounded value whose representation contains no external handles or floats.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CanonicalVariableValue {
    /// Boolean.
    Boolean(bool),
    /// Signed 64-bit integer.
    Integer(i64),
    /// Normalized base-ten string without exponent notation.
    Decimal(String),
    /// UTF-8 text.
    String(String),
    /// Closed declaration-owned tag.
    Enum(String),
    /// Homogeneous list.
    List(Vec<Self>),
    /// Homogeneous map with lexicographically ordered keys.
    Map(BTreeMap<String, Self>),
    /// Session identity.
    SessionId(String),
    /// Child-session identity.
    ChildId(String),
    /// Task identity.
    TaskId(String),
    /// Immutable artifact identity.
    ArtifactReference(String),
    /// Opaque secret-store reference, never secret bytes.
    SecretReference(String),
    /// Tool-result identity.
    ToolResultReference(String),
    /// Approval result.
    ApprovalResult(CanonicalApprovalResult),
    /// Node-result identity.
    NodeResultReference(String),
    /// Runtime-recorded Unix timestamp in milliseconds.
    TimestampMillis(i64),
    /// Runtime-recorded duration in milliseconds.
    DurationMillis(u64),
}

impl CanonicalVariableValue {
    /// Constructs a normalized decimal.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError::InvalidDecimal`] for exponent
    /// notation, signs without digits, or other non-decimal syntax.
    pub fn decimal(value: &str) -> Result<Self, VariableEnvironmentError> {
        Ok(Self::Decimal(canonical_decimal(value)?))
    }

    pub(crate) fn expression_value(&self) -> Value {
        match self {
            Self::Boolean(value) => Value::Bool(*value),
            Self::Integer(value) | Self::TimestampMillis(value) => Value::Number((*value).into()),
            Self::Decimal(value)
            | Self::String(value)
            | Self::Enum(value)
            | Self::SessionId(value)
            | Self::ChildId(value)
            | Self::TaskId(value)
            | Self::ArtifactReference(value)
            | Self::SecretReference(value)
            | Self::ToolResultReference(value)
            | Self::NodeResultReference(value) => Value::String(value.clone()),
            Self::List(values) => Value::Array(values.iter().map(Self::expression_value).collect()),
            Self::Map(values) => Value::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), value.expression_value()))
                    .collect(),
            ),
            Self::ApprovalResult(value) => Value::String(
                match value {
                    CanonicalApprovalResult::Approved => "approved",
                    CanonicalApprovalResult::Denied => "denied",
                    CanonicalApprovalResult::Cancelled => "cancelled",
                    CanonicalApprovalResult::Expired => "expired",
                }
                .to_owned(),
            ),
            Self::DurationMillis(value) => Value::Number((*value).into()),
        }
    }
}

/// Converts a bounded JSON initialization value into its exact declared
/// canonical variable representation.
///
/// This helper is intentionally pure so session initialization can reconstruct
/// legacy `initial_variables_json` without querying live components.
///
/// # Errors
///
/// Returns [`VariableEnvironmentError`] when the JSON shape or scalar cannot be
/// represented by the declaration type.
pub fn canonical_value_from_json(
    value: &Value,
    value_type: &VariableValueType,
) -> Result<CanonicalVariableValue, VariableEnvironmentError> {
    match value_type {
        VariableValueType::Boolean => value
            .as_bool()
            .map(CanonicalVariableValue::Boolean)
            .ok_or(VariableEnvironmentError::TypeMismatch),
        VariableValueType::Integer => value
            .as_i64()
            .map(CanonicalVariableValue::Integer)
            .ok_or(VariableEnvironmentError::TypeMismatch),
        VariableValueType::Decimal => value
            .as_str()
            .ok_or(VariableEnvironmentError::TypeMismatch)
            .and_then(CanonicalVariableValue::decimal),
        VariableValueType::String => string_value(value, CanonicalVariableValue::String),
        VariableValueType::Enum { .. } => string_value(value, CanonicalVariableValue::Enum),
        VariableValueType::List { item_type, .. } => value
            .as_array()
            .ok_or(VariableEnvironmentError::TypeMismatch)?
            .iter()
            .map(|item| canonical_value_from_json(item, item_type))
            .collect::<Result<Vec<_>, _>>()
            .map(CanonicalVariableValue::List),
        VariableValueType::Map { value_type, .. } => value
            .as_object()
            .ok_or(VariableEnvironmentError::TypeMismatch)?
            .iter()
            .map(|(key, value)| {
                canonical_value_from_json(value, value_type).map(|value| (key.clone(), value))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map(CanonicalVariableValue::Map),
        VariableValueType::SessionId => string_value(value, CanonicalVariableValue::SessionId),
        VariableValueType::ChildId => string_value(value, CanonicalVariableValue::ChildId),
        VariableValueType::TaskId => string_value(value, CanonicalVariableValue::TaskId),
        VariableValueType::ArtifactReference => {
            string_value(value, CanonicalVariableValue::ArtifactReference)
        }
        VariableValueType::SecretReference => {
            string_value(value, CanonicalVariableValue::SecretReference)
        }
        VariableValueType::ToolResultReference => {
            string_value(value, CanonicalVariableValue::ToolResultReference)
        }
        VariableValueType::ApprovalResult => match value.as_str() {
            Some("approved") => Ok(CanonicalVariableValue::ApprovalResult(
                CanonicalApprovalResult::Approved,
            )),
            Some("denied") => Ok(CanonicalVariableValue::ApprovalResult(
                CanonicalApprovalResult::Denied,
            )),
            Some("cancelled") => Ok(CanonicalVariableValue::ApprovalResult(
                CanonicalApprovalResult::Cancelled,
            )),
            Some("expired") => Ok(CanonicalVariableValue::ApprovalResult(
                CanonicalApprovalResult::Expired,
            )),
            _ => Err(VariableEnvironmentError::TypeMismatch),
        },
        VariableValueType::NodeResultReference => {
            string_value(value, CanonicalVariableValue::NodeResultReference)
        }
        VariableValueType::Timestamp => value
            .as_i64()
            .map(CanonicalVariableValue::TimestampMillis)
            .ok_or(VariableEnvironmentError::TypeMismatch),
        VariableValueType::Duration => value
            .as_u64()
            .map(CanonicalVariableValue::DurationMillis)
            .ok_or(VariableEnvironmentError::TypeMismatch),
    }
}

fn string_value(
    value: &Value,
    constructor: impl FnOnce(String) -> CanonicalVariableValue,
) -> Result<CanonicalVariableValue, VariableEnvironmentError> {
    value
        .as_str()
        .map(str::to_owned)
        .map(constructor)
        .ok_or(VariableEnvironmentError::TypeMismatch)
}

/// Stable identity of a graph writer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VariableWriter {
    /// Runtime-owned recorded input.
    Runtime,
    /// Runtime-recorded timestamp or duration attributed to the exact graph
    /// node whose output contract requested it.
    RuntimeRecorded {
        /// Compiled node ID.
        node_id: String,
        /// Branch context when execution is parallel.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchWriteContext>,
    },
    /// Exact graph node, optionally executing inside one branch.
    Node {
        /// Compiled node ID.
        node_id: String,
        /// Branch context when execution is parallel.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        branch: Option<BranchWriteContext>,
    },
}

impl VariableWriter {
    fn node_id(&self) -> &str {
        match self {
            Self::Runtime => RUNTIME_PRODUCER,
            Self::RuntimeRecorded { node_id, .. } | Self::Node { node_id, .. } => node_id,
        }
    }

    fn branch(&self) -> Option<&BranchWriteContext> {
        match self {
            Self::Runtime => None,
            Self::RuntimeRecorded { branch, .. } | Self::Node { branch, .. } => branch.as_ref(),
        }
    }
}

/// Exact branch context attached to a direct write.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BranchWriteContext {
    /// Stable branch identity.
    pub branch_id: String,
    /// Stable dispatch order.
    pub stable_order: u32,
    /// Whether runtime serialized this write against other branches.
    pub serialized_shared_write: bool,
}

/// Exact node/branch identity used to authorize a read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableReader {
    /// Compiled consuming node ID.
    pub node_id: String,
    /// Active branch identity for branch-scoped values.
    pub branch_id: Option<String>,
}

/// One branch contribution to a deterministic shared-variable merge.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BranchVariableValue {
    /// Stable branch identity.
    pub branch_id: String,
    /// Stable branch dispatch order.
    pub stable_order: u32,
    /// Complete declared value contributed by this branch.
    pub value: CanonicalVariableValue,
}

/// Canonical live entry reconstructed by replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalVariableEntry {
    /// Validated value.
    pub value: CanonicalVariableValue,
    /// Strictly positive assignment version.
    pub version: u64,
    /// Hash of the canonical serialized value.
    pub value_hash: ContentHash,
    /// Writer of the current version.
    pub writer: VariableWriter,
    /// Branch scope retained for access validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_id: Option<String>,
}

/// Pure declaration-bound canonical graph environment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalVariableEnvironment {
    declarations: BTreeMap<String, VariableDeclaration>,
    entries: BTreeMap<String, CanonicalVariableEntry>,
    limits: VariableEnvironmentLimits,
}

impl CanonicalVariableEnvironment {
    /// Validates declarations and creates an empty environment.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for duplicate, invalid, unsafe, or
    /// globally unbounded declarations.
    pub fn new(
        declarations: impl IntoIterator<Item = VariableDeclaration>,
        limits: VariableEnvironmentLimits,
    ) -> Result<Self, VariableEnvironmentError> {
        validate_limits(limits)?;
        let mut by_name = BTreeMap::new();
        for declaration in declarations {
            validate_declaration(&declaration, limits)?;
            let name = declaration.name.clone();
            if by_name.insert(name.clone(), declaration).is_some() {
                return Err(VariableEnvironmentError::DuplicateDeclaration(name));
            }
        }
        if by_name.len() > limits.max_variables {
            return Err(VariableEnvironmentError::VariableLimitExceeded);
        }
        Ok(Self {
            declarations: by_name,
            entries: BTreeMap::new(),
            limits,
        })
    }

    /// Returns immutable declarations in canonical name order.
    #[must_use]
    pub const fn declarations(&self) -> &BTreeMap<String, VariableDeclaration> {
        &self.declarations
    }

    /// Returns live entries in canonical name order without bypassing read
    /// authorization for graph nodes.
    #[must_use]
    pub const fn canonical_entries(&self) -> &BTreeMap<String, CanonicalVariableEntry> {
        &self.entries
    }

    /// Returns live values in canonical name order.
    ///
    /// This is an explicit alias for [`Self::canonical_entries`] used by
    /// orchestration projections that treat the entries as the replayed value
    /// set; it does not bypass node read authorization.
    #[must_use]
    pub const fn values(&self) -> &BTreeMap<String, CanonicalVariableEntry> {
        &self.entries
    }

    /// Validates one bounded contribution for a declared shared parallel
    /// variable without installing it into the live environment.
    ///
    /// # Errors
    ///
    /// Returns a declaration, scope, merge-policy, type, security, or bound
    /// failure. The join remains the only authorized writer of the eventual
    /// merged value.
    pub(crate) fn validate_parallel_contribution(
        &self,
        name: &str,
        value: &CanonicalVariableValue,
    ) -> Result<(), VariableEnvironmentError> {
        let declaration = self.declaration(name)?;
        if !matches!(
            declaration.scope,
            VariableScope::Run | VariableScope::Session
        ) || declaration.merge_policy.is_none()
        {
            return Err(VariableEnvironmentError::InvalidBranchWrite);
        }
        self.validate_value(
            declaration,
            &VariableWriter::Node {
                node_id: declaration.producer.clone(),
                branch: None,
            },
            value,
        )
    }

    /// Hashes the exact declarations, limits, versions, writers, and values in
    /// canonical map order.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError::Serialization`] if the canonical
    /// environment representation cannot be serialized.
    pub fn state_hash(&self) -> Result<ContentHash, VariableEnvironmentError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|_| VariableEnvironmentError::Serialization)
    }

    /// Assigns one variable using optimistic version comparison.
    ///
    /// `expected_version` must be absent for the first write and must equal the
    /// current version for every mutable rewrite.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for declaration, writer, scope,
    /// mutability, version, type, security, or resource-limit violations.
    pub fn assign(
        &mut self,
        name: &str,
        writer: VariableWriter,
        expected_version: Option<u64>,
        value: CanonicalVariableValue,
    ) -> Result<&CanonicalVariableEntry, VariableEnvironmentError> {
        let declaration = self.declaration(name)?.clone();
        authorize_writer(&declaration, &writer)?;
        let branch_id = validate_direct_write_scope(&declaration, &writer)?;
        self.validate_value(&declaration, &writer, &value)?;
        self.install(
            name,
            declaration.mutability,
            writer,
            branch_id,
            expected_version,
            value,
        )
    }

    /// Deterministically merges complete branch values using the declaration's
    /// exact merge policy.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for unauthorized writers, absent or
    /// incompatible merge policy, duplicate branches, conflicts, version
    /// mismatch, invalid values, or exceeded bounds.
    pub fn merge_branches(
        &mut self,
        name: &str,
        writer: VariableWriter,
        expected_version: Option<u64>,
        mut branches: Vec<BranchVariableValue>,
    ) -> Result<&CanonicalVariableEntry, VariableEnvironmentError> {
        let declaration = self.declaration(name)?.clone();
        if writer.branch().is_some() {
            return Err(VariableEnvironmentError::InvalidBranchWrite);
        }
        if declaration.scope == VariableScope::Branch {
            return Err(VariableEnvironmentError::InvalidBranchWrite);
        }
        let policy = declaration
            .merge_policy
            .ok_or(VariableEnvironmentError::MissingMergePolicy)?;
        if branches.is_empty() {
            return Err(VariableEnvironmentError::EmptyMerge);
        }
        branches.sort_by(|left, right| {
            (left.stable_order, left.branch_id.as_str())
                .cmp(&(right.stable_order, right.branch_id.as_str()))
        });
        let mut ids = BTreeSet::new();
        if branches
            .iter()
            .any(|branch| branch.branch_id.is_empty() || !ids.insert(branch.branch_id.as_str()))
        {
            return Err(VariableEnvironmentError::DuplicateBranch);
        }
        for branch in &branches {
            self.validate_value(&declaration, &writer, &branch.value)?;
        }
        let value = merge_values(policy, branches)?;
        self.validate_value(&declaration, &writer, &value)?;
        self.install(
            name,
            declaration.mutability,
            writer,
            None,
            expected_version,
            value,
        )
    }

    /// Reads one variable as an exact declared consumer.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] when the variable is undeclared,
    /// unassigned, unauthorized, or outside the reader's branch scope.
    pub fn read(
        &self,
        name: &str,
        reader: &VariableReader,
    ) -> Result<&CanonicalVariableEntry, VariableEnvironmentError> {
        let declaration = self.declaration(name)?;
        if !declaration.consumers.contains(&reader.node_id) {
            return Err(VariableEnvironmentError::UnauthorizedReader {
                variable: name.to_owned(),
                reader: reader.node_id.clone(),
            });
        }
        let entry = self
            .entries
            .get(name)
            .ok_or_else(|| VariableEnvironmentError::MissingValue(name.to_owned()))?;
        if declaration.scope == VariableScope::Branch
            && entry.branch_id.as_deref() != reader.branch_id.as_deref()
        {
            return Err(VariableEnvironmentError::BranchScopeMismatch);
        }
        Ok(entry)
    }

    /// Revalidates deserialized state, including every retained value hash.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] if replayed declarations or entries
    /// could not have been produced by this kernel.
    pub fn validate_replayed(&self) -> Result<(), VariableEnvironmentError> {
        validate_limits(self.limits)?;
        if self.declarations.len() > self.limits.max_variables {
            return Err(VariableEnvironmentError::VariableLimitExceeded);
        }
        for (name, declaration) in &self.declarations {
            if name != &declaration.name {
                return Err(VariableEnvironmentError::DeclarationKeyMismatch);
            }
            validate_declaration(declaration, self.limits)?;
        }
        for (name, entry) in &self.entries {
            let declaration = self.declaration(name)?;
            if entry.version == 0 {
                return Err(VariableEnvironmentError::InvalidVersion);
            }
            authorize_replayed_writer(declaration, &entry.writer)?;
            let expected_branch = validate_direct_write_scope_replayed(
                declaration,
                &entry.writer,
                entry.branch_id.as_deref(),
            )?;
            if expected_branch.as_deref() != entry.branch_id.as_deref() {
                return Err(VariableEnvironmentError::BranchScopeMismatch);
            }
            self.validate_value(declaration, &entry.writer, &entry.value)?;
            if value_hash(&entry.value)? != entry.value_hash {
                return Err(VariableEnvironmentError::ValueHashMismatch(name.clone()));
            }
        }
        self.validate_environment_size()
    }

    /// Evaluates a deterministic expression from exactly declared inputs.
    #[must_use]
    pub fn classify_condition(
        &self,
        expression_source: &str,
        reader: &VariableReader,
        required_variables: &BTreeSet<String>,
        limits: ExpressionLimits,
    ) -> ConditionEligibility {
        let expression = match Expression::parse(expression_source, limits) {
            Ok(expression) => expression,
            Err(error) => {
                return ConditionEligibility::InvalidExpression {
                    diagnostic: error.to_string(),
                };
            }
        };
        self.classify_compiled_condition(&expression, reader, required_variables)
    }

    /// Evaluates an already compiled deterministic expression from exactly
    /// declared inputs.
    ///
    /// This is the replay/introspection path: it consumes the immutable AST
    /// retained by the compiled graph and never reparses or queries live
    /// components.
    #[must_use]
    pub fn classify_compiled_condition(
        &self,
        expression: &Expression,
        reader: &VariableReader,
        required_variables: &BTreeSet<String>,
    ) -> ConditionEligibility {
        let mut object = Map::new();
        for name in required_variables {
            match self.read(name, reader) {
                Ok(entry) => {
                    object.insert(name.clone(), entry.value.expression_value());
                }
                Err(VariableEnvironmentError::MissingValue(_)) => {
                    return ConditionEligibility::MissingInput { path: name.clone() };
                }
                Err(error) => {
                    return ConditionEligibility::InvalidExpression {
                        diagnostic: error.to_string(),
                    };
                }
            }
        }
        match expression.evaluate(&Value::Object(object)) {
            Ok(true) => ConditionEligibility::Eligible,
            Ok(false) => ConditionEligibility::Ineligible,
            Err(EvaluationError::MissingPath { path }) => {
                ConditionEligibility::MissingInput { path }
            }
            Err(error) => ConditionEligibility::InvalidExpression {
                diagnostic: error.to_string(),
            },
        }
    }

    /// Builds the exact bounded JSON environment used by compiled transition
    /// expressions after enforcing every declaration consumer and branch scope.
    ///
    /// Secret-classified variables can contribute only opaque
    /// [`CanonicalVariableValue::SecretReference`] strings; inline secret
    /// material cannot enter this environment because assignment validation
    /// rejects it.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] when a requested input is
    /// undeclared, unassigned, unauthorized, or outside the reader's branch.
    pub fn transition_environment(
        &self,
        reader: &VariableReader,
        required_variables: &BTreeSet<String>,
    ) -> Result<Value, VariableEnvironmentError> {
        let mut object = Map::new();
        for name in required_variables {
            let entry = self.read(name, reader)?;
            object.insert(name.clone(), entry.value.expression_value());
        }
        Ok(Value::Object(object))
    }

    /// Builds the exact pre-execution input environment for one node.
    ///
    /// An unassigned variable that the same node produces is omitted: such a
    /// declaration can be required by an outgoing condition that runs only
    /// after the node commits its output. If that variable already has a
    /// canonical value, it remains an input so mutable read-modify-write nodes
    /// retain their prior-state semantics.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for every undeclared,
    /// unauthorized, wrong-branch, or missing non-output input.
    pub fn node_input_environment(
        &self,
        reader: &VariableReader,
        read_variables: &BTreeSet<String>,
        write_variables: &BTreeSet<String>,
    ) -> Result<Value, VariableEnvironmentError> {
        let mut object = Map::new();
        for name in read_variables {
            match self.read(name, reader) {
                Ok(entry) => {
                    object.insert(name.clone(), entry.value.expression_value());
                }
                Err(VariableEnvironmentError::MissingValue(variable))
                    if write_variables.contains(name) =>
                {
                    debug_assert_eq!(variable, *name);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(Value::Object(object))
    }

    fn declaration(&self, name: &str) -> Result<&VariableDeclaration, VariableEnvironmentError> {
        self.declarations
            .get(name)
            .ok_or_else(|| VariableEnvironmentError::UndeclaredVariable(name.to_owned()))
    }

    fn validate_value(
        &self,
        declaration: &VariableDeclaration,
        writer: &VariableWriter,
        value: &CanonicalVariableValue,
    ) -> Result<(), VariableEnvironmentError> {
        validate_value_type(
            value,
            &declaration.value_type,
            declaration.security_classification,
            writer,
            self.limits,
            1,
            &mut 0,
        )?;
        let bytes = canonical_value_bytes(value)?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > declaration.max_size_bytes {
            return Err(VariableEnvironmentError::DeclaredSizeExceeded {
                variable: declaration.name.clone(),
            });
        }
        Ok(())
    }

    fn install(
        &mut self,
        name: &str,
        mutability: VariableMutability,
        writer: VariableWriter,
        branch_id: Option<String>,
        expected_version: Option<u64>,
        value: CanonicalVariableValue,
    ) -> Result<&CanonicalVariableEntry, VariableEnvironmentError> {
        let next_version = match self.entries.get(name) {
            None if expected_version.is_none() => 1,
            Some(_) if mutability == VariableMutability::Immutable => {
                return Err(VariableEnvironmentError::ImmutableReassignment);
            }
            Some(existing) if expected_version == Some(existing.version) => existing
                .version
                .checked_add(1)
                .ok_or(VariableEnvironmentError::VersionOverflow)?,
            _ => return Err(VariableEnvironmentError::VersionMismatch),
        };
        let prior = self.entries.insert(
            name.to_owned(),
            CanonicalVariableEntry {
                value_hash: value_hash(&value)?,
                value,
                version: next_version,
                writer,
                branch_id,
            },
        );
        if let Err(error) = self.validate_environment_size() {
            match prior {
                Some(prior) => {
                    self.entries.insert(name.to_owned(), prior);
                }
                None => {
                    self.entries.remove(name);
                }
            }
            return Err(error);
        }
        self.entries
            .get(name)
            .ok_or(VariableEnvironmentError::InternalInvariant)
    }

    fn validate_environment_size(&self) -> Result<(), VariableEnvironmentError> {
        let total = self.entries.values().try_fold(0_usize, |total, entry| {
            total
                .checked_add(canonical_value_bytes(&entry.value)?.len())
                .ok_or(VariableEnvironmentError::EnvironmentSizeExceeded)
        })?;
        if total > self.limits.max_environment_bytes {
            Err(VariableEnvironmentError::EnvironmentSizeExceeded)
        } else {
            Ok(())
        }
    }
}

/// Exact introspection result for one condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConditionEligibility {
    /// Expression evaluates true.
    Eligible,
    /// Expression evaluates false.
    Ineligible,
    /// A declared value or nested expression path is absent.
    MissingInput {
        /// Stable missing variable/path.
        path: String,
    },
    /// Declaration access, syntax, or type evaluation is invalid.
    InvalidExpression {
        /// Stable bounded diagnostic.
        diagnostic: String,
    },
}

/// Common immutable binding carried by every canonical variable event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalVariableEventBinding {
    /// Exact graph run.
    pub run_id: String,
    /// Runtime or exact graph node responsible for the operation.
    pub node_id: String,
    /// Declared variable name.
    pub variable: String,
    /// Version before the operation, absent before a first assignment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_version: Option<u64>,
    /// Version after the operation. Validation failures retain the prior
    /// version because they do not mutate live state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_version: Option<u64>,
    /// Exact assigned/merged/removed value hash, or attempted-operation hash
    /// for a validation failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_hash: Option<ContentHash>,
    /// Every artifact reference recursively present in the value or attempt.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub artifact_references: BTreeSet<String>,
}

/// Canonical variable declaration event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableDeclaredEvent {
    /// Common run/node/version binding.
    pub binding: CanonicalVariableEventBinding,
    /// Exact graph declaration.
    pub declaration: VariableDeclaration,
    /// Hash of the canonical declaration bytes.
    pub declaration_hash: ContentHash,
}

/// Canonical successful assignment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableAssignedEvent {
    /// Common run/node/version/value binding.
    pub binding: CanonicalVariableEventBinding,
    /// Exact authorized writer.
    pub writer: VariableWriter,
    /// Exact value.
    pub value: CanonicalVariableValue,
}

/// Canonical deterministic branch merge.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableMergedEvent {
    /// Common run/node/version/value binding.
    pub binding: CanonicalVariableEventBinding,
    /// Exact authorized merge coordinator.
    pub writer: VariableWriter,
    /// Exact declaration-owned merge policy.
    pub policy: VariableMergePolicy,
    /// Complete branch inputs in canonical stable order.
    pub branches: Vec<BranchVariableValue>,
    /// Exact merged result.
    pub value: CanonicalVariableValue,
}

/// Canonical removal from live node/branch scope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableRemovedEvent {
    /// Common run/node/version/removed-value binding.
    pub binding: CanonicalVariableEventBinding,
    /// Exact authorized writer closing the scope.
    pub writer: VariableWriter,
}

/// Stable validation-failure category safe for canonical diagnostics.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VariableValidationFailureCode {
    /// Declaration is duplicate or invalid.
    InvalidDeclaration,
    /// Variable is absent or undeclared.
    UnknownVariable,
    /// Reader/writer/scope/branch access is invalid.
    AccessDenied,
    /// Type, enum, decimal, reference, or runtime-recorded value is invalid.
    InvalidValue,
    /// Security classification was violated.
    SecurityViolation,
    /// Mutability or version compare-and-set failed.
    VersionConflict,
    /// Size, depth, item, collection, or environment bound was exceeded.
    ResourceLimit,
    /// Merge policy/input/conflict is invalid.
    MergeConflict,
    /// Removal is not allowed for this live scope.
    RemovalNotAllowed,
    /// Canonical serialization or internal invariant failed.
    Internal,
}

/// Complete bounded operation retained when validation fails.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum VariableValidationAttempt {
    /// Declaration attempt.
    Declare {
        /// Exact declaration.
        declaration: VariableDeclaration,
    },
    /// Assignment attempt.
    Assign {
        /// Variable name.
        variable: String,
        /// Exact writer.
        writer: VariableWriter,
        /// Optimistic prior version.
        expected_version: Option<u64>,
        /// Attempted value.
        value: CanonicalVariableValue,
    },
    /// Merge attempt.
    Merge {
        /// Variable name.
        variable: String,
        /// Exact merge coordinator.
        writer: VariableWriter,
        /// Optimistic prior version.
        expected_version: Option<u64>,
        /// Complete branch inputs.
        branches: Vec<BranchVariableValue>,
    },
    /// Removal attempt.
    Remove {
        /// Variable name.
        variable: String,
        /// Exact writer closing scope.
        writer: VariableWriter,
        /// Exact current version.
        expected_version: u64,
    },
}

impl VariableValidationAttempt {
    fn variable(&self) -> &str {
        match self {
            Self::Declare { declaration } => &declaration.name,
            Self::Assign { variable, .. }
            | Self::Merge { variable, .. }
            | Self::Remove { variable, .. } => variable,
        }
    }
}

/// Canonical failed-validation event. Applying it never mutates live values.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableValidationFailedEvent {
    /// Common run/node/current-version/attempt binding.
    pub binding: CanonicalVariableEventBinding,
    /// Complete bounded attempted operation.
    pub attempt: VariableValidationAttempt,
    /// Stable expected failure category.
    pub code: VariableValidationFailureCode,
}

/// Pure canonical variable event ready to embed in a runtime envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum CanonicalVariableEvent {
    /// Declaration became canonical.
    Declared(Box<VariableDeclaredEvent>),
    /// Assignment became canonical.
    Assigned(Box<VariableAssignedEvent>),
    /// Branch merge became canonical.
    Merged(Box<VariableMergedEvent>),
    /// Node/branch-scoped value left live scope.
    Removed(Box<VariableRemovedEvent>),
    /// Attempt was canonically rejected without mutation.
    ValidationFailed(Box<VariableValidationFailedEvent>),
}

impl CanonicalVariableEvent {
    /// Stable user-space event type for later runtime-envelope mapping.
    #[must_use]
    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::Declared(_) => "graph.variable_declared",
            Self::Assigned(_) => "graph.variable_assigned",
            Self::Merged(_) => "graph.variable_merged",
            Self::Removed(_) => "graph.variable_removed",
            Self::ValidationFailed(_) => "graph.variable_validation_failed",
        }
    }

    /// Returns the common immutable binding.
    #[must_use]
    pub fn binding(&self) -> &CanonicalVariableEventBinding {
        match self {
            Self::Declared(event) => &event.binding,
            Self::Assigned(event) => &event.binding,
            Self::Merged(event) => &event.binding,
            Self::Removed(event) => &event.binding,
            Self::ValidationFailed(event) => &event.binding,
        }
    }
}

/// Terminal removal tombstone retained for replay validation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemovedVariableRecord {
    /// Version assigned to removal.
    pub version: u64,
    /// Hash of the removed value.
    pub removed_value_hash: ContentHash,
    /// Exact removal writer.
    pub writer: VariableWriter,
}

/// Result of applying one pure canonical variable event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VariableEventApplyOutcome {
    /// Declaration or live state changed.
    Mutated,
    /// Failure was independently reproduced and retained without live mutation.
    ValidationFailed(VariableValidationFailureCode),
}

/// Explicit audit state for one immutable-contract initial assignment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InitialAssignmentAuditState {
    /// The value is seeded in the live environment but its journal audit event
    /// has not yet been reduced.
    Pending,
    /// The exact seed assignment audit event has been reduced.
    Observed,
}

/// Pure replay reducer for canonical variable events.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalVariableEventReducer {
    run_id: String,
    environment: CanonicalVariableEnvironment,
    declaration_hashes: BTreeMap<String, ContentHash>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    observed_declarations: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    initial_audit_schema_version: u8,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    seeded_initial_assignment_hashes: BTreeMap<String, ContentHash>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    observed_initial_assignment_hashes: BTreeMap<String, ContentHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    initial_assignment_audit_state_hash: Option<ContentHash>,
    removed: BTreeMap<String, RemovedVariableRecord>,
    validation_failures: Vec<VariableValidationFailedEvent>,
}

const MAX_CANONICAL_VARIABLE_FAILURES: usize = 1_024;

impl CanonicalVariableEventReducer {
    /// Creates an empty event-reduced environment for one exact graph run.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for an invalid run identity or
    /// environment limits.
    pub fn new(
        run_id: impl Into<String>,
        limits: VariableEnvironmentLimits,
    ) -> Result<Self, VariableEnvironmentError> {
        let run_id = run_id.into();
        validate_event_identity(&run_id, RUNTIME_PRODUCER)?;
        Ok(Self {
            run_id,
            environment: CanonicalVariableEnvironment::new([], limits)?,
            declaration_hashes: BTreeMap::new(),
            observed_declarations: BTreeSet::new(),
            initial_audit_schema_version: 1,
            seeded_initial_assignment_hashes: BTreeMap::new(),
            observed_initial_assignment_hashes: BTreeMap::new(),
            initial_assignment_audit_state_hash: Some(initial_assignment_audit_state_hash(
                &BTreeMap::new(),
                &BTreeMap::new(),
            )?),
            removed: BTreeMap::new(),
            validation_failures: Vec::new(),
        })
    }

    /// Seeds the immutable graph-declaration and initial-value projection
    /// carried by style initialization. Declaration events remain observable
    /// exactly once afterward as explicit journal audit records.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for invalid declarations, unknown
    /// initial values, non-runtime producers, or invalid typed values.
    pub fn initialize(
        run_id: impl Into<String>,
        limits: VariableEnvironmentLimits,
        declarations: impl IntoIterator<Item = VariableDeclaration>,
        initial_values: impl IntoIterator<Item = (String, CanonicalVariableValue)>,
    ) -> Result<Self, VariableEnvironmentError> {
        let mut reducer = Self::new(run_id, limits)?;
        for declaration in declarations {
            if reducer.environment.declarations.len() >= limits.max_variables {
                return Err(VariableEnvironmentError::VariableLimitExceeded);
            }
            validate_declaration(&declaration, limits)?;
            let name = declaration.name.clone();
            if reducer
                .environment
                .declarations
                .insert(name.clone(), declaration.clone())
                .is_some()
            {
                return Err(VariableEnvironmentError::DuplicateDeclaration(name));
            }
            reducer
                .declaration_hashes
                .insert(declaration.name.clone(), declaration_hash(&declaration)?);
        }
        for (name, value) in initial_values {
            let entry = reducer
                .environment
                .assign(&name, VariableWriter::Runtime, None, value)?;
            reducer
                .seeded_initial_assignment_hashes
                .insert(name, entry.value_hash);
        }
        reducer.refresh_initial_assignment_audit_state_hash()?;
        reducer.validate_replayed()?;
        Ok(reducer)
    }

    /// Returns the exact graph run identity.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Returns the replay-owned live variable environment.
    #[must_use]
    pub const fn environment(&self) -> &CanonicalVariableEnvironment {
        &self.environment
    }

    /// Returns terminal node/branch-scope removal tombstones.
    #[must_use]
    pub const fn removed(&self) -> &BTreeMap<String, RemovedVariableRecord> {
        &self.removed
    }

    /// Returns canonically reproduced validation failures.
    #[must_use]
    pub fn validation_failures(&self) -> &[VariableValidationFailedEvent] {
        &self.validation_failures
    }

    /// Reports whether the declaration's explicit canonical audit event has
    /// already been reduced.
    #[must_use]
    pub fn declaration_was_observed(&self, name: &str) -> bool {
        self.observed_declarations.contains(name)
    }

    /// Returns the exact audit state for an immutable-contract seed.
    ///
    /// `None` means the value is not a seed in this projection. A supplied
    /// value that differs from a retained seed also returns `None`; callers
    /// requiring that exact seed must treat absence as a conflict.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] when the value cannot be hashed.
    pub fn initial_assignment_audit_state(
        &self,
        name: &str,
        value: &CanonicalVariableValue,
    ) -> Result<Option<InitialAssignmentAuditState>, VariableEnvironmentError> {
        let hash = value_hash(value)?;
        if self.seeded_initial_assignment_hashes.get(name) != Some(&hash) {
            return Ok(None);
        }
        Ok(Some(
            if self.observed_initial_assignment_hashes.get(name) == Some(&hash) {
                InitialAssignmentAuditState::Observed
            } else {
                InitialAssignmentAuditState::Pending
            },
        ))
    }

    /// Prepares either the exact successful event or an independently
    /// reproducible validation-failure event without mutating reducer state.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] only when event identity or
    /// canonical serialization itself is invalid. Ordinary declaration,
    /// access, value, version, merge, and removal failures become
    /// [`CanonicalVariableEvent::ValidationFailed`].
    pub fn prepare_event(
        &self,
        node_id: &str,
        attempt: VariableValidationAttempt,
    ) -> Result<CanonicalVariableEvent, VariableEnvironmentError> {
        validate_event_identity(&self.run_id, node_id)?;
        match self.prepare_success(node_id, &attempt) {
            Ok(event) => Ok(event),
            Err(error) => Ok(CanonicalVariableEvent::ValidationFailed(Box::new(
                self.prepare_failure(node_id, attempt, &error)?,
            ))),
        }
    }

    /// Applies one event only after independently preparing and comparing the
    /// exact expected event from current replay state.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for run/node/binding tampering,
    /// changed values, hashes, versions, artifacts, policies, attempts, or a
    /// failure that cannot be reproduced.
    pub fn apply(
        &mut self,
        event: CanonicalVariableEvent,
    ) -> Result<VariableEventApplyOutcome, VariableEnvironmentError> {
        let binding = event.binding();
        validate_event_identity(&binding.run_id, &binding.node_id)?;
        if binding.run_id != self.run_id {
            return Err(VariableEnvironmentError::EventRunMismatch);
        }
        let attempt = attempt_from_event(&event);
        let expected = self.prepare_event(&binding.node_id, attempt)?;
        if expected != event {
            return Err(VariableEnvironmentError::EventMismatch);
        }
        let mut next = self.clone();
        let outcome = next.apply_verified(event)?;
        *self = next;
        Ok(outcome)
    }

    /// Revalidates a deserialized reducer snapshot without querying live
    /// components.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError`] for invalid environment state,
    /// declaration hashes, tombstones, failure bindings, or resource bounds.
    pub fn validate_replayed(&self) -> Result<(), VariableEnvironmentError> {
        validate_event_identity(&self.run_id, RUNTIME_PRODUCER)?;
        self.environment.validate_replayed()?;
        if self.declaration_hashes.len() != self.environment.declarations.len() {
            return Err(VariableEnvironmentError::DeclarationHashMismatch);
        }
        if !self
            .observed_declarations
            .iter()
            .all(|name| self.environment.declarations.contains_key(name))
        {
            return Err(VariableEnvironmentError::DeclarationKeyMismatch);
        }
        for (name, declaration) in &self.environment.declarations {
            if self.declaration_hashes.get(name) != Some(&declaration_hash(declaration)?) {
                return Err(VariableEnvironmentError::DeclarationHashMismatch);
            }
        }
        self.validate_initial_assignment_audit_state()?;
        for (name, removed) in &self.removed {
            let declaration = self.environment.declaration(name)?;
            if self.environment.entries.contains_key(name)
                || !matches!(
                    declaration.scope,
                    VariableScope::Node | VariableScope::Branch
                )
                || removed.version == 0
            {
                return Err(VariableEnvironmentError::InvalidRemoval);
            }
            authorize_replayed_writer(declaration, &removed.writer)?;
            if declaration.scope == VariableScope::Branch {
                validate_direct_write_scope(declaration, &removed.writer)?;
            }
        }
        if self.validation_failures.len() > MAX_CANONICAL_VARIABLE_FAILURES {
            return Err(VariableEnvironmentError::FailureLimitExceeded);
        }
        for failure in &self.validation_failures {
            validate_failure_static(&self.run_id, failure)?;
        }
        Ok(())
    }

    /// Hashes the complete replay-owned reducer snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`VariableEnvironmentError::Serialization`] if canonical
    /// serialization fails.
    pub fn state_hash(&self) -> Result<ContentHash, VariableEnvironmentError> {
        serde_json::to_vec(self)
            .map(|bytes| ContentHash::digest(&bytes))
            .map_err(|_| VariableEnvironmentError::Serialization)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive preparer keeps every public event variant bound to the same replay snapshot and failure conversion"
    )]
    fn prepare_success(
        &self,
        node_id: &str,
        attempt: &VariableValidationAttempt,
    ) -> Result<CanonicalVariableEvent, VariableEnvironmentError> {
        match attempt {
            VariableValidationAttempt::Declare { declaration } => {
                if node_id != RUNTIME_PRODUCER {
                    return Err(VariableEnvironmentError::UnauthorizedWriter {
                        variable: declaration.name.clone(),
                        writer: node_id.to_owned(),
                    });
                }
                if let Some(existing) = self.environment.declarations.get(&declaration.name) {
                    if existing != declaration
                        || self.observed_declarations.contains(&declaration.name)
                    {
                        return Err(VariableEnvironmentError::DuplicateDeclaration(
                            declaration.name.clone(),
                        ));
                    }
                } else {
                    if self.environment.declarations.len() >= self.environment.limits.max_variables
                    {
                        return Err(VariableEnvironmentError::VariableLimitExceeded);
                    }
                    validate_declaration(declaration, self.environment.limits)?;
                }
                let declaration_hash = declaration_hash(declaration)?;
                Ok(CanonicalVariableEvent::Declared(Box::new(
                    VariableDeclaredEvent {
                        binding: self.binding(
                            node_id,
                            &declaration.name,
                            None,
                            None,
                            None,
                            BTreeSet::new(),
                        ),
                        declaration: declaration.clone(),
                        declaration_hash,
                    },
                )))
            }
            VariableValidationAttempt::Assign {
                variable,
                writer,
                expected_version,
                value,
            } => {
                self.ensure_not_removed(variable)?;
                if node_id == RUNTIME_PRODUCER
                    && *writer == VariableWriter::Runtime
                    && expected_version.is_none()
                    && self.initial_assignment_audit_state(variable, value)?
                        == Some(InitialAssignmentAuditState::Pending)
                {
                    let entry =
                        self.environment.entries.get(variable).ok_or_else(|| {
                            VariableEnvironmentError::MissingValue(variable.clone())
                        })?;
                    if entry.version != 1
                        || entry.writer != VariableWriter::Runtime
                        || entry.value != *value
                    {
                        return Err(VariableEnvironmentError::EventMismatch);
                    }
                    return Ok(CanonicalVariableEvent::Assigned(Box::new(
                        VariableAssignedEvent {
                            binding: self.binding(
                                node_id,
                                variable,
                                None,
                                Some(1),
                                Some(entry.value_hash),
                                artifact_references(value),
                            ),
                            writer: writer.clone(),
                            value: value.clone(),
                        },
                    )));
                }
                let prior = self
                    .environment
                    .entries
                    .get(variable)
                    .map(|entry| entry.version);
                let mut environment = self.environment.clone();
                let entry = environment
                    .assign(variable, writer.clone(), *expected_version, value.clone())?
                    .clone();
                Ok(CanonicalVariableEvent::Assigned(Box::new(
                    VariableAssignedEvent {
                        binding: self.binding(
                            node_id,
                            variable,
                            prior,
                            Some(entry.version),
                            Some(entry.value_hash),
                            artifact_references(value),
                        ),
                        writer: writer.clone(),
                        value: value.clone(),
                    },
                )))
            }
            VariableValidationAttempt::Merge {
                variable,
                writer,
                expected_version,
                branches,
            } => {
                self.ensure_not_removed(variable)?;
                let declaration = self.environment.declaration(variable)?;
                let policy = declaration
                    .merge_policy
                    .ok_or(VariableEnvironmentError::MissingMergePolicy)?;
                let prior = self
                    .environment
                    .entries
                    .get(variable)
                    .map(|entry| entry.version);
                let mut canonical_branches = branches.clone();
                sort_branches(&mut canonical_branches);
                let mut environment = self.environment.clone();
                let entry = environment
                    .merge_branches(
                        variable,
                        writer.clone(),
                        *expected_version,
                        canonical_branches.clone(),
                    )?
                    .clone();
                Ok(CanonicalVariableEvent::Merged(Box::new(
                    VariableMergedEvent {
                        binding: self.binding(
                            node_id,
                            variable,
                            prior,
                            Some(entry.version),
                            Some(entry.value_hash),
                            artifact_references(&entry.value),
                        ),
                        writer: writer.clone(),
                        policy,
                        branches: canonical_branches,
                        value: entry.value,
                    },
                )))
            }
            VariableValidationAttempt::Remove {
                variable,
                writer,
                expected_version,
            } => self.prepare_removal(node_id, variable, writer, *expected_version),
        }
    }

    fn prepare_removal(
        &self,
        node_id: &str,
        variable: &str,
        writer: &VariableWriter,
        expected_version: u64,
    ) -> Result<CanonicalVariableEvent, VariableEnvironmentError> {
        self.ensure_not_removed(variable)?;
        let declaration = self.environment.declaration(variable)?;
        if !matches!(
            declaration.scope,
            VariableScope::Node | VariableScope::Branch
        ) {
            return Err(VariableEnvironmentError::InvalidRemoval);
        }
        authorize_writer(declaration, writer)?;
        let entry = self
            .environment
            .entries
            .get(variable)
            .ok_or_else(|| VariableEnvironmentError::MissingValue(variable.to_owned()))?;
        if entry.version != expected_version {
            return Err(VariableEnvironmentError::VersionMismatch);
        }
        let retained_branch =
            validate_direct_write_scope_replayed(declaration, writer, entry.branch_id.as_deref())?;
        if retained_branch.as_deref() != entry.branch_id.as_deref() {
            return Err(VariableEnvironmentError::BranchScopeMismatch);
        }
        let next = entry
            .version
            .checked_add(1)
            .ok_or(VariableEnvironmentError::VersionOverflow)?;
        Ok(CanonicalVariableEvent::Removed(Box::new(
            VariableRemovedEvent {
                binding: self.binding(
                    node_id,
                    variable,
                    Some(entry.version),
                    Some(next),
                    Some(entry.value_hash),
                    artifact_references(&entry.value),
                ),
                writer: writer.clone(),
            },
        )))
    }

    fn prepare_failure(
        &self,
        node_id: &str,
        attempt: VariableValidationAttempt,
        error: &VariableEnvironmentError,
    ) -> Result<VariableValidationFailedEvent, VariableEnvironmentError> {
        let variable = attempt.variable().to_owned();
        let current_version = self.current_version(&variable);
        let artifacts = attempt_artifact_references(&attempt);
        let attempt_hash = ContentHash::digest(
            &serde_json::to_vec(&attempt).map_err(|_| VariableEnvironmentError::Serialization)?,
        );
        Ok(VariableValidationFailedEvent {
            binding: self.binding(
                node_id,
                &variable,
                current_version,
                current_version,
                Some(attempt_hash),
                artifacts,
            ),
            attempt,
            code: failure_code(error),
        })
    }

    fn apply_verified(
        &mut self,
        event: CanonicalVariableEvent,
    ) -> Result<VariableEventApplyOutcome, VariableEnvironmentError> {
        match event {
            CanonicalVariableEvent::Declared(event) => {
                self.environment
                    .declarations
                    .entry(event.declaration.name.clone())
                    .or_insert_with(|| event.declaration.clone());
                self.declaration_hashes
                    .entry(event.declaration.name.clone())
                    .or_insert(event.declaration_hash);
                self.observed_declarations
                    .insert(event.declaration.name.clone());
                Ok(VariableEventApplyOutcome::Mutated)
            }
            CanonicalVariableEvent::Assigned(event) => {
                let seed_hash = self
                    .seeded_initial_assignment_hashes
                    .get(&event.binding.variable)
                    .copied();
                if event.writer == VariableWriter::Runtime
                    && event.binding.prior_version.is_none()
                    && event.binding.new_version == Some(1)
                    && seed_hash == event.binding.value_hash
                    && !self
                        .observed_initial_assignment_hashes
                        .contains_key(&event.binding.variable)
                {
                    let observed_hash = seed_hash.ok_or(VariableEnvironmentError::EventMismatch)?;
                    self.observed_initial_assignment_hashes
                        .insert(event.binding.variable, observed_hash);
                    self.refresh_initial_assignment_audit_state_hash()?;
                } else {
                    let variable = event.binding.variable;
                    let writer = event.writer;
                    let prior_version = event.binding.prior_version;
                    let assigned = self.environment.assign(
                        &variable,
                        writer.clone(),
                        prior_version,
                        event.value,
                    )?;
                    let assigned_hash = assigned.value_hash;
                    if writer == VariableWriter::Runtime && prior_version.is_none() {
                        self.seeded_initial_assignment_hashes
                            .insert(variable.clone(), assigned_hash);
                        self.observed_initial_assignment_hashes
                            .insert(variable, assigned_hash);
                        self.refresh_initial_assignment_audit_state_hash()?;
                    }
                }
                Ok(VariableEventApplyOutcome::Mutated)
            }
            CanonicalVariableEvent::Merged(event) => {
                self.environment.merge_branches(
                    &event.binding.variable,
                    event.writer,
                    event.binding.prior_version,
                    event.branches,
                )?;
                Ok(VariableEventApplyOutcome::Mutated)
            }
            CanonicalVariableEvent::Removed(event) => {
                let entry = self
                    .environment
                    .entries
                    .remove(&event.binding.variable)
                    .ok_or_else(|| {
                        VariableEnvironmentError::MissingValue(event.binding.variable.clone())
                    })?;
                self.removed.insert(
                    event.binding.variable.clone(),
                    RemovedVariableRecord {
                        version: event
                            .binding
                            .new_version
                            .ok_or(VariableEnvironmentError::EventMismatch)?,
                        removed_value_hash: entry.value_hash,
                        writer: event.writer,
                    },
                );
                Ok(VariableEventApplyOutcome::Mutated)
            }
            CanonicalVariableEvent::ValidationFailed(event) => {
                if self.validation_failures.len() >= MAX_CANONICAL_VARIABLE_FAILURES {
                    return Err(VariableEnvironmentError::FailureLimitExceeded);
                }
                let code = event.code;
                self.validation_failures.push(*event);
                Ok(VariableEventApplyOutcome::ValidationFailed(code))
            }
        }
    }

    fn binding(
        &self,
        node_id: &str,
        variable: &str,
        prior_version: Option<u64>,
        new_version: Option<u64>,
        value_hash: Option<ContentHash>,
        artifact_references: BTreeSet<String>,
    ) -> CanonicalVariableEventBinding {
        CanonicalVariableEventBinding {
            run_id: self.run_id.clone(),
            node_id: node_id.to_owned(),
            variable: variable.to_owned(),
            prior_version,
            new_version,
            value_hash,
            artifact_references,
        }
    }

    fn current_version(&self, variable: &str) -> Option<u64> {
        self.environment
            .entries
            .get(variable)
            .map(|entry| entry.version)
            .or_else(|| self.removed.get(variable).map(|removed| removed.version))
    }

    fn ensure_not_removed(&self, variable: &str) -> Result<(), VariableEnvironmentError> {
        if self.removed.contains_key(variable) {
            Err(VariableEnvironmentError::InvalidRemoval)
        } else {
            Ok(())
        }
    }

    fn validate_initial_assignment_audit_state(&self) -> Result<(), VariableEnvironmentError> {
        if self.initial_audit_schema_version == 0 {
            if !self.seeded_initial_assignment_hashes.is_empty()
                || !self.observed_initial_assignment_hashes.is_empty()
                || self.initial_assignment_audit_state_hash.is_some()
            {
                return Err(VariableEnvironmentError::InitialAuditStateMismatch);
            }
            return Ok(());
        }
        if self.initial_audit_schema_version != 1
            || self.initial_assignment_audit_state_hash
                != Some(initial_assignment_audit_state_hash(
                    &self.seeded_initial_assignment_hashes,
                    &self.observed_initial_assignment_hashes,
                )?)
            || !self
                .observed_initial_assignment_hashes
                .iter()
                .all(|(name, hash)| self.seeded_initial_assignment_hashes.get(name) == Some(hash))
        {
            return Err(VariableEnvironmentError::InitialAuditStateMismatch);
        }
        for (name, hash) in &self.seeded_initial_assignment_hashes {
            let declaration = self.environment.declaration(name)?;
            if declaration.producer != RUNTIME_PRODUCER {
                return Err(VariableEnvironmentError::InitialAuditStateMismatch);
            }
            if !self.observed_initial_assignment_hashes.contains_key(name) {
                let entry = self
                    .environment
                    .entries
                    .get(name)
                    .ok_or(VariableEnvironmentError::InitialAuditStateMismatch)?;
                if entry.version != 1
                    || entry.writer != VariableWriter::Runtime
                    || entry.value_hash != *hash
                {
                    return Err(VariableEnvironmentError::InitialAuditStateMismatch);
                }
            }
        }
        let runtime_entries = self
            .environment
            .entries
            .iter()
            .filter(|(_, entry)| entry.writer == VariableWriter::Runtime)
            .map(|(name, _)| name)
            .collect::<BTreeSet<_>>();
        if runtime_entries
            .iter()
            .any(|name| !self.seeded_initial_assignment_hashes.contains_key(*name))
        {
            return Err(VariableEnvironmentError::InitialAuditStateMismatch);
        }
        Ok(())
    }

    fn refresh_initial_assignment_audit_state_hash(
        &mut self,
    ) -> Result<(), VariableEnvironmentError> {
        self.initial_assignment_audit_state_hash = Some(initial_assignment_audit_state_hash(
            &self.seeded_initial_assignment_hashes,
            &self.observed_initial_assignment_hashes,
        )?);
        Ok(())
    }
}

/// Stable pure variable-kernel failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum VariableEnvironmentError {
    /// Invalid global limits.
    #[error("variable environment limits are invalid")]
    InvalidLimits,
    /// Too many declarations.
    #[error("variable declaration limit exceeded")]
    VariableLimitExceeded,
    /// Duplicate declaration.
    #[error("duplicate variable declaration `{0}`")]
    DuplicateDeclaration(String),
    /// Empty or invalid declaration.
    #[error("invalid variable declaration `{0}`")]
    InvalidDeclaration(String),
    /// Declaration type is too deep.
    #[error("variable declaration type exceeds maximum depth")]
    DeclarationDepthExceeded,
    /// Declaration map key and embedded name differ after replay.
    #[error("variable declaration key mismatch")]
    DeclarationKeyMismatch,
    /// Variable is not declared.
    #[error("variable `{0}` is undeclared")]
    UndeclaredVariable(String),
    /// Variable has no value.
    #[error("variable `{0}` has no canonical value")]
    MissingValue(String),
    /// Writer does not match declaration producer.
    #[error("writer `{writer}` cannot produce variable `{variable}`")]
    UnauthorizedWriter {
        /// Variable name.
        variable: String,
        /// Supplied writer.
        writer: String,
    },
    /// Reader is not a declared consumer.
    #[error("reader `{reader}` cannot consume variable `{variable}`")]
    UnauthorizedReader {
        /// Variable name.
        variable: String,
        /// Supplied reader.
        reader: String,
    },
    /// Branch-scoped read/write mismatch.
    #[error("variable branch scope does not match")]
    BranchScopeMismatch,
    /// Unsafe direct parallel shared write.
    #[error("parallel shared write requires merge or explicit serialization")]
    InvalidBranchWrite,
    /// Missing merge policy.
    #[error("variable does not declare a merge policy")]
    MissingMergePolicy,
    /// No branch values.
    #[error("branch merge has no values")]
    EmptyMerge,
    /// Duplicate or empty branch identity.
    #[error("branch merge contains duplicate or empty identity")]
    DuplicateBranch,
    /// Merge inputs conflict.
    #[error("branch values conflict under deep merge")]
    MergeConflict,
    /// Merge policy and value shape disagree.
    #[error("branch values are incompatible with merge policy")]
    InvalidMergeValue,
    /// Immutable reassignment.
    #[error("immutable variable cannot be reassigned")]
    ImmutableReassignment,
    /// Version compare-and-set failed.
    #[error("variable version mismatch")]
    VersionMismatch,
    /// Version overflow.
    #[error("variable version overflow")]
    VersionOverflow,
    /// Zero replayed version.
    #[error("replayed variable version is invalid")]
    InvalidVersion,
    /// Value does not match its declared type.
    #[error("variable value does not match its declared type")]
    TypeMismatch,
    /// Invalid canonical decimal.
    #[error("decimal is not in canonical base-ten form")]
    InvalidDecimal,
    /// Enum tag is not declared.
    #[error("enum tag is not declared")]
    InvalidEnum,
    /// Inline secret or misclassified secret reference.
    #[error("variable value violates its security classification")]
    SecurityViolation,
    /// Runtime-recorded type was supplied by a graph node.
    #[error("timestamp and duration values must be recorded by runtime")]
    RuntimeRecordedValueRequired,
    /// Empty, oversized, or control-bearing reference.
    #[error("variable reference is invalid")]
    InvalidReference,
    /// Value depth exceeded.
    #[error("variable value exceeds maximum depth")]
    ValueDepthExceeded,
    /// Value item count exceeded.
    #[error("variable value exceeds maximum item count")]
    ValueItemLimitExceeded,
    /// List/map type bound exceeded.
    #[error("variable value exceeds its declared collection bound")]
    CollectionBoundExceeded,
    /// Invalid map key.
    #[error("variable map key is invalid")]
    InvalidMapKey,
    /// Per-declaration byte limit exceeded.
    #[error("variable `{variable}` exceeds its declared byte limit")]
    DeclaredSizeExceeded {
        /// Variable name.
        variable: String,
    },
    /// Aggregate live environment byte limit exceeded.
    #[error("canonical variable environment exceeds its byte limit")]
    EnvironmentSizeExceeded,
    /// Stored hash differs.
    #[error("variable `{0}` value hash does not match")]
    ValueHashMismatch(String),
    /// Seeded and observed initialization-audit state is internally inconsistent.
    #[error("canonical variable initialization audit state does not match")]
    InitialAuditStateMismatch,
    /// Canonical serialization failed.
    #[error("canonical variable serialization failed")]
    Serialization,
    /// Internal post-insert invariant failed.
    #[error("canonical variable internal invariant failed")]
    InternalInvariant,
    /// Event run identity differs from reducer run.
    #[error("canonical variable event run does not match")]
    EventRunMismatch,
    /// Event differs from the exact operation prepared from replay state.
    #[error("canonical variable event does not match replay state")]
    EventMismatch,
    /// Declaration hash differs from canonical bytes.
    #[error("canonical variable declaration hash does not match")]
    DeclarationHashMismatch,
    /// Removal is invalid for the declaration, scope, or replay state.
    #[error("canonical variable removal is invalid")]
    InvalidRemoval,
    /// Too many validation failures were retained.
    #[error("canonical variable validation-failure limit exceeded")]
    FailureLimitExceeded,
    /// Run or node identity is empty, oversized, or control-bearing.
    #[error("canonical variable event identity is invalid")]
    InvalidEventIdentity,
}

fn attempt_from_event(event: &CanonicalVariableEvent) -> VariableValidationAttempt {
    match event {
        CanonicalVariableEvent::Declared(event) => VariableValidationAttempt::Declare {
            declaration: event.declaration.clone(),
        },
        CanonicalVariableEvent::Assigned(event) => VariableValidationAttempt::Assign {
            variable: event.binding.variable.clone(),
            writer: event.writer.clone(),
            expected_version: event.binding.prior_version,
            value: event.value.clone(),
        },
        CanonicalVariableEvent::Merged(event) => VariableValidationAttempt::Merge {
            variable: event.binding.variable.clone(),
            writer: event.writer.clone(),
            expected_version: event.binding.prior_version,
            branches: event.branches.clone(),
        },
        CanonicalVariableEvent::Removed(event) => VariableValidationAttempt::Remove {
            variable: event.binding.variable.clone(),
            writer: event.writer.clone(),
            expected_version: event.binding.prior_version.unwrap_or_default(),
        },
        CanonicalVariableEvent::ValidationFailed(event) => event.attempt.clone(),
    }
}

fn validate_event_identity(run_id: &str, node_id: &str) -> Result<(), VariableEnvironmentError> {
    if run_id.is_empty()
        || run_id.len() > 256
        || run_id.chars().any(char::is_control)
        || node_id.is_empty()
        || node_id.len() > 256
        || node_id.chars().any(char::is_control)
    {
        Err(VariableEnvironmentError::InvalidEventIdentity)
    } else {
        Ok(())
    }
}

fn declaration_hash(
    declaration: &VariableDeclaration,
) -> Result<ContentHash, VariableEnvironmentError> {
    serde_json::to_vec(declaration)
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| VariableEnvironmentError::Serialization)
}

fn sort_branches(branches: &mut [BranchVariableValue]) {
    branches.sort_by(|left, right| {
        (left.stable_order, left.branch_id.as_str())
            .cmp(&(right.stable_order, right.branch_id.as_str()))
    });
}

pub(crate) fn artifact_references(value: &CanonicalVariableValue) -> BTreeSet<String> {
    let mut references = BTreeSet::new();
    collect_artifact_references(value, &mut references);
    references
}

fn collect_artifact_references(value: &CanonicalVariableValue, references: &mut BTreeSet<String>) {
    match value {
        CanonicalVariableValue::ArtifactReference(reference) => {
            references.insert(reference.clone());
        }
        CanonicalVariableValue::List(values) => {
            for value in values {
                collect_artifact_references(value, references);
            }
        }
        CanonicalVariableValue::Map(values) => {
            for value in values.values() {
                collect_artifact_references(value, references);
            }
        }
        _ => {}
    }
}

fn attempt_artifact_references(attempt: &VariableValidationAttempt) -> BTreeSet<String> {
    match attempt {
        VariableValidationAttempt::Declare { .. } | VariableValidationAttempt::Remove { .. } => {
            BTreeSet::new()
        }
        VariableValidationAttempt::Assign { value, .. } => artifact_references(value),
        VariableValidationAttempt::Merge { branches, .. } => branches
            .iter()
            .flat_map(|branch| artifact_references(&branch.value))
            .collect(),
    }
}

fn failure_code(error: &VariableEnvironmentError) -> VariableValidationFailureCode {
    match error {
        VariableEnvironmentError::DuplicateDeclaration(_)
        | VariableEnvironmentError::InvalidDeclaration(_)
        | VariableEnvironmentError::DeclarationDepthExceeded
        | VariableEnvironmentError::DeclarationKeyMismatch
        | VariableEnvironmentError::DeclarationHashMismatch => {
            VariableValidationFailureCode::InvalidDeclaration
        }
        VariableEnvironmentError::UndeclaredVariable(_)
        | VariableEnvironmentError::MissingValue(_) => {
            VariableValidationFailureCode::UnknownVariable
        }
        VariableEnvironmentError::UnauthorizedWriter { .. }
        | VariableEnvironmentError::UnauthorizedReader { .. }
        | VariableEnvironmentError::BranchScopeMismatch
        | VariableEnvironmentError::InvalidBranchWrite
        | VariableEnvironmentError::InvalidEventIdentity
        | VariableEnvironmentError::EventRunMismatch => VariableValidationFailureCode::AccessDenied,
        VariableEnvironmentError::TypeMismatch
        | VariableEnvironmentError::InvalidDecimal
        | VariableEnvironmentError::InvalidEnum
        | VariableEnvironmentError::RuntimeRecordedValueRequired
        | VariableEnvironmentError::InvalidReference
        | VariableEnvironmentError::InvalidMapKey => VariableValidationFailureCode::InvalidValue,
        VariableEnvironmentError::SecurityViolation => {
            VariableValidationFailureCode::SecurityViolation
        }
        VariableEnvironmentError::ImmutableReassignment
        | VariableEnvironmentError::VersionMismatch
        | VariableEnvironmentError::VersionOverflow
        | VariableEnvironmentError::InvalidVersion => {
            VariableValidationFailureCode::VersionConflict
        }
        VariableEnvironmentError::InvalidLimits
        | VariableEnvironmentError::VariableLimitExceeded
        | VariableEnvironmentError::ValueDepthExceeded
        | VariableEnvironmentError::ValueItemLimitExceeded
        | VariableEnvironmentError::CollectionBoundExceeded
        | VariableEnvironmentError::DeclaredSizeExceeded { .. }
        | VariableEnvironmentError::EnvironmentSizeExceeded
        | VariableEnvironmentError::FailureLimitExceeded => {
            VariableValidationFailureCode::ResourceLimit
        }
        VariableEnvironmentError::MissingMergePolicy
        | VariableEnvironmentError::EmptyMerge
        | VariableEnvironmentError::DuplicateBranch
        | VariableEnvironmentError::MergeConflict
        | VariableEnvironmentError::InvalidMergeValue => {
            VariableValidationFailureCode::MergeConflict
        }
        VariableEnvironmentError::InvalidRemoval => {
            VariableValidationFailureCode::RemovalNotAllowed
        }
        VariableEnvironmentError::ValueHashMismatch(_)
        | VariableEnvironmentError::InitialAuditStateMismatch
        | VariableEnvironmentError::Serialization
        | VariableEnvironmentError::InternalInvariant
        | VariableEnvironmentError::EventMismatch => VariableValidationFailureCode::Internal,
    }
}

fn validate_failure_static(
    run_id: &str,
    failure: &VariableValidationFailedEvent,
) -> Result<(), VariableEnvironmentError> {
    validate_event_identity(&failure.binding.run_id, &failure.binding.node_id)?;
    if failure.binding.run_id != run_id
        || failure.binding.variable != failure.attempt.variable()
        || failure.binding.prior_version != failure.binding.new_version
        || failure.binding.artifact_references != attempt_artifact_references(&failure.attempt)
    {
        return Err(VariableEnvironmentError::EventMismatch);
    }
    let expected_hash = ContentHash::digest(
        &serde_json::to_vec(&failure.attempt)
            .map_err(|_| VariableEnvironmentError::Serialization)?,
    );
    if failure.binding.value_hash != Some(expected_hash) {
        return Err(VariableEnvironmentError::EventMismatch);
    }
    Ok(())
}

fn validate_limits(limits: VariableEnvironmentLimits) -> Result<(), VariableEnvironmentError> {
    if limits.max_variables == 0
        || limits.max_value_depth == 0
        || limits.max_value_items == 0
        || limits.max_map_key_bytes == 0
        || limits.max_reference_bytes == 0
        || limits.max_decimal_bytes == 0
        || limits.max_environment_bytes == 0
    {
        Err(VariableEnvironmentError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn validate_declaration(
    declaration: &VariableDeclaration,
    limits: VariableEnvironmentLimits,
) -> Result<(), VariableEnvironmentError> {
    if declaration.name.is_empty()
        || declaration.name.len() > 256
        || declaration.name.chars().any(char::is_control)
        || declaration.producer.is_empty()
        || declaration.max_size_bytes == 0
        || usize::try_from(declaration.max_size_bytes)
            .map_or(true, |size| size > limits.max_environment_bytes)
    {
        return Err(VariableEnvironmentError::InvalidDeclaration(
            declaration.name.clone(),
        ));
    }
    validate_declared_type(&declaration.value_type, limits, 1)?;
    let secret_type = declaration.value_type == VariableValueType::SecretReference;
    let secret_class =
        declaration.security_classification == SecurityClassification::SecretReference;
    if secret_type != secret_class {
        return Err(VariableEnvironmentError::SecurityViolation);
    }
    let merge_compatible = match declaration.merge_policy {
        None | Some(VariableMergePolicy::FirstBranch) => true,
        Some(VariableMergePolicy::Append | VariableMergePolicy::Union) => {
            matches!(declaration.value_type, VariableValueType::List { .. })
        }
        Some(VariableMergePolicy::DeepMerge) => {
            matches!(declaration.value_type, VariableValueType::Map { .. })
        }
    };
    if !merge_compatible
        || (declaration.merge_policy.is_some()
            && declaration.mutability != VariableMutability::Mutable)
    {
        return Err(VariableEnvironmentError::InvalidDeclaration(
            declaration.name.clone(),
        ));
    }
    Ok(())
}

fn validate_declared_type(
    value_type: &VariableValueType,
    limits: VariableEnvironmentLimits,
    depth: usize,
) -> Result<(), VariableEnvironmentError> {
    if depth > limits.max_value_depth {
        return Err(VariableEnvironmentError::DeclarationDepthExceeded);
    }
    match value_type {
        VariableValueType::Enum { values } if values.is_empty() => Err(
            VariableEnvironmentError::InvalidDeclaration(String::from("empty enum")),
        ),
        VariableValueType::Enum { values }
            if values
                .iter()
                .any(|value| value.is_empty() || value.chars().any(char::is_control)) =>
        {
            Err(VariableEnvironmentError::InvalidDeclaration(String::from(
                "invalid enum",
            )))
        }
        VariableValueType::List {
            item_type,
            max_items,
        } => {
            if *max_items == 0
                || usize::try_from(*max_items).map_or(true, |count| count > limits.max_value_items)
            {
                return Err(VariableEnvironmentError::InvalidDeclaration(String::from(
                    "invalid list bound",
                )));
            }
            validate_declared_type(item_type, limits, depth + 1)
        }
        VariableValueType::Map {
            value_type,
            max_entries,
        } => {
            if *max_entries == 0
                || usize::try_from(*max_entries)
                    .map_or(true, |count| count > limits.max_value_items)
            {
                return Err(VariableEnvironmentError::InvalidDeclaration(String::from(
                    "invalid map bound",
                )));
            }
            validate_declared_type(value_type, limits, depth + 1)
        }
        _ => Ok(()),
    }
}

fn authorize_writer(
    declaration: &VariableDeclaration,
    writer: &VariableWriter,
) -> Result<(), VariableEnvironmentError> {
    if declaration.producer == writer.node_id() {
        Ok(())
    } else {
        Err(VariableEnvironmentError::UnauthorizedWriter {
            variable: declaration.name.clone(),
            writer: writer.node_id().to_owned(),
        })
    }
}

fn authorize_replayed_writer(
    declaration: &VariableDeclaration,
    writer: &VariableWriter,
) -> Result<(), VariableEnvironmentError> {
    if declaration.producer == writer.node_id()
        || (declaration.merge_policy.is_some()
            && matches!(writer, VariableWriter::Node { branch: None, .. }))
    {
        Ok(())
    } else {
        Err(VariableEnvironmentError::UnauthorizedWriter {
            variable: declaration.name.clone(),
            writer: writer.node_id().to_owned(),
        })
    }
}

fn validate_direct_write_scope(
    declaration: &VariableDeclaration,
    writer: &VariableWriter,
) -> Result<Option<String>, VariableEnvironmentError> {
    match (declaration.scope, writer.branch()) {
        (VariableScope::Branch, Some(branch)) if !branch.branch_id.is_empty() => {
            Ok(Some(branch.branch_id.clone()))
        }
        (VariableScope::Branch, _) => Err(VariableEnvironmentError::BranchScopeMismatch),
        (VariableScope::Run | VariableScope::Session, Some(branch))
            if declaration.merge_policy.is_some() || !branch.serialized_shared_write =>
        {
            Err(VariableEnvironmentError::InvalidBranchWrite)
        }
        _ => Ok(None),
    }
}

fn validate_direct_write_scope_replayed(
    declaration: &VariableDeclaration,
    writer: &VariableWriter,
    retained_branch_id: Option<&str>,
) -> Result<Option<String>, VariableEnvironmentError> {
    let branch = validate_direct_write_scope(declaration, writer)?;
    if declaration.scope != VariableScope::Branch && retained_branch_id.is_some() {
        return Err(VariableEnvironmentError::BranchScopeMismatch);
    }
    Ok(branch)
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "recursive validation carries the exact declaration, security, writer, and global counters"
)]
fn validate_value_type(
    value: &CanonicalVariableValue,
    expected: &VariableValueType,
    classification: SecurityClassification,
    writer: &VariableWriter,
    limits: VariableEnvironmentLimits,
    depth: usize,
    item_count: &mut usize,
) -> Result<(), VariableEnvironmentError> {
    if depth > limits.max_value_depth {
        return Err(VariableEnvironmentError::ValueDepthExceeded);
    }
    if classification == SecurityClassification::SecretReference
        && !matches!(value, CanonicalVariableValue::SecretReference(_))
    {
        return Err(VariableEnvironmentError::SecurityViolation);
    }
    if classification != SecurityClassification::SecretReference
        && matches!(value, CanonicalVariableValue::SecretReference(_))
    {
        return Err(VariableEnvironmentError::SecurityViolation);
    }
    match (value, expected) {
        (CanonicalVariableValue::Boolean(_), VariableValueType::Boolean)
        | (CanonicalVariableValue::Integer(_), VariableValueType::Integer)
        | (CanonicalVariableValue::String(_), VariableValueType::String)
        | (CanonicalVariableValue::ApprovalResult(_), VariableValueType::ApprovalResult) => Ok(()),
        (CanonicalVariableValue::Decimal(value), VariableValueType::Decimal) => {
            if value.len() > limits.max_decimal_bytes || canonical_decimal(value)? != *value {
                Err(VariableEnvironmentError::InvalidDecimal)
            } else {
                Ok(())
            }
        }
        (CanonicalVariableValue::Enum(value), VariableValueType::Enum { values }) => {
            if values.contains(value) {
                Ok(())
            } else {
                Err(VariableEnvironmentError::InvalidEnum)
            }
        }
        (
            CanonicalVariableValue::List(values),
            VariableValueType::List {
                item_type,
                max_items,
            },
        ) => {
            if values.len() > usize::try_from(*max_items).unwrap_or(usize::MAX) {
                return Err(VariableEnvironmentError::CollectionBoundExceeded);
            }
            add_items(item_count, values.len(), limits)?;
            for value in values {
                validate_value_type(
                    value,
                    item_type,
                    classification,
                    writer,
                    limits,
                    depth + 1,
                    item_count,
                )?;
            }
            Ok(())
        }
        (
            CanonicalVariableValue::Map(values),
            VariableValueType::Map {
                value_type,
                max_entries,
            },
        ) => {
            if values.len() > usize::try_from(*max_entries).unwrap_or(usize::MAX) {
                return Err(VariableEnvironmentError::CollectionBoundExceeded);
            }
            add_items(item_count, values.len(), limits)?;
            for (key, value) in values {
                if key.is_empty()
                    || key.len() > limits.max_map_key_bytes
                    || key.chars().any(char::is_control)
                {
                    return Err(VariableEnvironmentError::InvalidMapKey);
                }
                validate_value_type(
                    value,
                    value_type,
                    classification,
                    writer,
                    limits,
                    depth + 1,
                    item_count,
                )?;
            }
            Ok(())
        }
        (CanonicalVariableValue::SessionId(value), VariableValueType::SessionId)
        | (CanonicalVariableValue::ChildId(value), VariableValueType::ChildId)
        | (CanonicalVariableValue::TaskId(value), VariableValueType::TaskId)
        | (
            CanonicalVariableValue::ArtifactReference(value),
            VariableValueType::ArtifactReference,
        )
        | (CanonicalVariableValue::SecretReference(value), VariableValueType::SecretReference)
        | (
            CanonicalVariableValue::ToolResultReference(value),
            VariableValueType::ToolResultReference,
        )
        | (
            CanonicalVariableValue::NodeResultReference(value),
            VariableValueType::NodeResultReference,
        ) => validate_reference(value, limits),
        (CanonicalVariableValue::TimestampMillis(_), VariableValueType::Timestamp)
        | (CanonicalVariableValue::DurationMillis(_), VariableValueType::Duration) => {
            if matches!(
                writer,
                VariableWriter::Runtime | VariableWriter::RuntimeRecorded { .. }
            ) {
                Ok(())
            } else {
                Err(VariableEnvironmentError::RuntimeRecordedValueRequired)
            }
        }
        _ => Err(VariableEnvironmentError::TypeMismatch),
    }
}

fn add_items(
    current: &mut usize,
    added: usize,
    limits: VariableEnvironmentLimits,
) -> Result<(), VariableEnvironmentError> {
    *current = current
        .checked_add(added)
        .ok_or(VariableEnvironmentError::ValueItemLimitExceeded)?;
    if *current > limits.max_value_items {
        Err(VariableEnvironmentError::ValueItemLimitExceeded)
    } else {
        Ok(())
    }
}

fn validate_reference(
    value: &str,
    limits: VariableEnvironmentLimits,
) -> Result<(), VariableEnvironmentError> {
    if value.is_empty()
        || value.len() > limits.max_reference_bytes
        || value.chars().any(char::is_control)
    {
        Err(VariableEnvironmentError::InvalidReference)
    } else {
        Ok(())
    }
}

fn canonical_decimal(input: &str) -> Result<String, VariableEnvironmentError> {
    if input.is_empty() || input.len() > 128 || input.contains(['e', 'E', '+']) {
        return Err(VariableEnvironmentError::InvalidDecimal);
    }
    let (negative, unsigned) = input
        .strip_prefix('-')
        .map_or((false, input), |value| (true, value));
    let mut parts = unsigned.split('.');
    let integer = parts.next().unwrap_or_default();
    let fraction = parts.next();
    if parts.next().is_some()
        || integer.is_empty()
        || !integer.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.is_some_and(|value| {
            value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit())
        })
    {
        return Err(VariableEnvironmentError::InvalidDecimal);
    }
    let integer = integer.trim_start_matches('0');
    let integer = if integer.is_empty() { "0" } else { integer };
    let fraction = fraction.unwrap_or_default().trim_end_matches('0');
    let zero = integer == "0" && fraction.is_empty();
    let sign = if negative && !zero { "-" } else { "" };
    if fraction.is_empty() {
        Ok(format!("{sign}{integer}"))
    } else {
        Ok(format!("{sign}{integer}.{fraction}"))
    }
}

fn merge_values(
    policy: VariableMergePolicy,
    branches: Vec<BranchVariableValue>,
) -> Result<CanonicalVariableValue, VariableEnvironmentError> {
    match policy {
        VariableMergePolicy::Append => {
            let mut merged = Vec::new();
            for branch in branches {
                let CanonicalVariableValue::List(mut values) = branch.value else {
                    return Err(VariableEnvironmentError::InvalidMergeValue);
                };
                merged.append(&mut values);
            }
            Ok(CanonicalVariableValue::List(merged))
        }
        VariableMergePolicy::Union => {
            let mut merged = BTreeSet::new();
            for branch in branches {
                let CanonicalVariableValue::List(values) = branch.value else {
                    return Err(VariableEnvironmentError::InvalidMergeValue);
                };
                merged.extend(values);
            }
            Ok(CanonicalVariableValue::List(merged.into_iter().collect()))
        }
        VariableMergePolicy::DeepMerge => {
            let mut merged = BTreeMap::new();
            for branch in branches {
                let CanonicalVariableValue::Map(values) = branch.value else {
                    return Err(VariableEnvironmentError::InvalidMergeValue);
                };
                deep_merge(&mut merged, values)?;
            }
            Ok(CanonicalVariableValue::Map(merged))
        }
        VariableMergePolicy::FirstBranch => branches
            .into_iter()
            .next()
            .map(|branch| branch.value)
            .ok_or(VariableEnvironmentError::EmptyMerge),
    }
}

pub(crate) fn merge_branch_contributions(
    policy: VariableMergePolicy,
    branches: Vec<BranchVariableValue>,
) -> Result<CanonicalVariableValue, VariableEnvironmentError> {
    merge_values(policy, branches)
}

fn deep_merge(
    target: &mut BTreeMap<String, CanonicalVariableValue>,
    source: BTreeMap<String, CanonicalVariableValue>,
) -> Result<(), VariableEnvironmentError> {
    for (key, value) in source {
        match (target.get_mut(&key), value) {
            (None, value) => {
                target.insert(key, value);
            }
            (
                Some(CanonicalVariableValue::Map(target_map)),
                CanonicalVariableValue::Map(source_map),
            ) => deep_merge(target_map, source_map)?,
            (Some(existing), value) if *existing == value => {}
            (Some(_), _) => return Err(VariableEnvironmentError::MergeConflict),
        }
    }
    Ok(())
}

fn canonical_value_bytes(
    value: &CanonicalVariableValue,
) -> Result<Vec<u8>, VariableEnvironmentError> {
    serde_json::to_vec(value).map_err(|_| VariableEnvironmentError::Serialization)
}

fn value_hash(value: &CanonicalVariableValue) -> Result<ContentHash, VariableEnvironmentError> {
    Ok(ContentHash::digest(&canonical_value_bytes(value)?))
}

fn initial_assignment_audit_state_hash(
    seeded: &BTreeMap<String, ContentHash>,
    observed: &BTreeMap<String, ContentHash>,
) -> Result<ContentHash, VariableEnvironmentError> {
    serde_json::to_vec(&(seeded, observed))
        .map(|bytes| ContentHash::digest(&bytes))
        .map_err(|_| VariableEnvironmentError::Serialization)
}

#[allow(
    clippy::trivially_copy_pass_by_ref,
    reason = "serde skip_serializing_if requires a shared-reference predicate"
)]
const fn is_zero_u8(value: &u8) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration(
        name: &str,
        value_type: VariableValueType,
        producer: &str,
    ) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            value_type,
            scope: VariableScope::Run,
            producer: producer.to_owned(),
            merge_contributors: BTreeSet::new(),
            consumers: [String::from("consumer")].into_iter().collect(),
            mutability: VariableMutability::Mutable,
            merge_policy: None,
            max_size_bytes: 4_096,
            security_classification: SecurityClassification::Internal,
        }
    }

    fn node_writer() -> VariableWriter {
        VariableWriter::Node {
            node_id: String::from("producer"),
            branch: None,
        }
    }

    fn reader() -> VariableReader {
        VariableReader {
            node_id: String::from("consumer"),
            branch_id: None,
        }
    }

    fn branch_value(id: &str, order: u32, value: CanonicalVariableValue) -> BranchVariableValue {
        BranchVariableValue {
            branch_id: id.to_owned(),
            stable_order: order,
            value,
        }
    }

    #[test]
    fn node_input_omits_only_unassigned_self_output_and_retains_existing_value() {
        let mut environment = CanonicalVariableEnvironment::new(
            [
                declaration("self_output", VariableValueType::String, "consumer"),
                declaration("required_input", VariableValueType::String, "producer"),
            ],
            VariableEnvironmentLimits::default(),
        )
        .expect("environment");
        let reads = BTreeSet::from([String::from("required_input"), String::from("self_output")]);
        let writes = BTreeSet::from([String::from("self_output")]);
        assert!(matches!(
            environment.node_input_environment(&reader(), &reads, &writes),
            Err(VariableEnvironmentError::MissingValue(name)) if name == "required_input"
        ));

        environment
            .assign(
                "required_input",
                node_writer(),
                None,
                CanonicalVariableValue::String(String::from("ready")),
            )
            .expect("required input");
        assert_eq!(
            environment
                .node_input_environment(&reader(), &reads, &writes)
                .expect("unassigned self output is not an invocation input"),
            serde_json::json!({"required_input":"ready"})
        );

        environment
            .assign(
                "self_output",
                VariableWriter::Node {
                    node_id: String::from("consumer"),
                    branch: None,
                },
                None,
                CanonicalVariableValue::String(String::from("prior")),
            )
            .expect("prior self output");
        assert_eq!(
            environment
                .node_input_environment(&reader(), &reads, &writes)
                .expect("existing mutable output remains an input"),
            serde_json::json!({
                "required_input":"ready",
                "self_output":"prior"
            })
        );
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one table-driven restart test covers every scalar and reference variant"
    )]
    fn every_declared_scalar_and_reference_type_round_trips_and_revalidates() {
        let enum_values = [String::from("ready"), String::from("waiting")]
            .into_iter()
            .collect();
        let cases = vec![
            (
                "boolean",
                VariableValueType::Boolean,
                CanonicalVariableValue::Boolean(true),
            ),
            (
                "integer",
                VariableValueType::Integer,
                CanonicalVariableValue::Integer(-7),
            ),
            (
                "decimal",
                VariableValueType::Decimal,
                CanonicalVariableValue::decimal("001.2500").expect("decimal"),
            ),
            (
                "string",
                VariableValueType::String,
                CanonicalVariableValue::String(String::from("bounded")),
            ),
            (
                "enum",
                VariableValueType::Enum {
                    values: enum_values,
                },
                CanonicalVariableValue::Enum(String::from("ready")),
            ),
            (
                "session",
                VariableValueType::SessionId,
                CanonicalVariableValue::SessionId(String::from(
                    "00000000-0000-0000-0000-000000000001",
                )),
            ),
            (
                "child",
                VariableValueType::ChildId,
                CanonicalVariableValue::ChildId(String::from("child:1")),
            ),
            (
                "task",
                VariableValueType::TaskId,
                CanonicalVariableValue::TaskId(String::from("task:1")),
            ),
            (
                "artifact",
                VariableValueType::ArtifactReference,
                CanonicalVariableValue::ArtifactReference(String::from("blake3:abc")),
            ),
            (
                "tool",
                VariableValueType::ToolResultReference,
                CanonicalVariableValue::ToolResultReference(String::from("tool-result:1")),
            ),
            (
                "approval",
                VariableValueType::ApprovalResult,
                CanonicalVariableValue::ApprovalResult(CanonicalApprovalResult::Approved),
            ),
            (
                "node",
                VariableValueType::NodeResultReference,
                CanonicalVariableValue::NodeResultReference(String::from("node-result:1")),
            ),
        ];
        let declarations = cases
            .iter()
            .map(|(name, value_type, _)| declaration(name, value_type.clone(), "producer"))
            .chain([
                declaration("timestamp", VariableValueType::Timestamp, RUNTIME_PRODUCER),
                declaration("duration", VariableValueType::Duration, RUNTIME_PRODUCER),
                {
                    let mut secret =
                        declaration("secret", VariableValueType::SecretReference, "producer");
                    secret.security_classification = SecurityClassification::SecretReference;
                    secret
                },
            ])
            .collect::<Vec<_>>();
        let mut environment =
            CanonicalVariableEnvironment::new(declarations, VariableEnvironmentLimits::default())
                .expect("environment");
        for (name, _, value) in cases {
            environment
                .assign(name, node_writer(), None, value)
                .expect("scalar assignment");
        }
        environment
            .assign(
                "timestamp",
                VariableWriter::Runtime,
                None,
                CanonicalVariableValue::TimestampMillis(42),
            )
            .expect("timestamp");
        environment
            .assign(
                "duration",
                VariableWriter::Runtime,
                None,
                CanonicalVariableValue::DurationMillis(15),
            )
            .expect("duration");
        environment
            .assign(
                "secret",
                node_writer(),
                None,
                CanonicalVariableValue::SecretReference(String::from("vault:item:version")),
            )
            .expect("opaque secret reference");

        let bytes = serde_json::to_vec(&environment).expect("serialize");
        let recovered: CanonicalVariableEnvironment =
            serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(recovered, environment);
        assert_eq!(
            recovered.state_hash().expect("recovered hash"),
            environment.state_hash().expect("original hash")
        );
        recovered.validate_replayed().expect("replay validation");
        assert_eq!(
            recovered.canonical_entries()["decimal"].value,
            CanonicalVariableValue::Decimal(String::from("1.25"))
        );
    }

    #[test]
    fn nested_lists_maps_and_all_bounds_are_enforced() {
        let mut list = declaration(
            "items",
            VariableValueType::List {
                item_type: Box::new(VariableValueType::Map {
                    value_type: Box::new(VariableValueType::Integer),
                    max_entries: 2,
                }),
                max_items: 2,
            },
            "producer",
        );
        list.max_size_bytes = 256;
        let mut environment = CanonicalVariableEnvironment::new(
            [list],
            VariableEnvironmentLimits {
                max_value_items: 4,
                ..VariableEnvironmentLimits::default()
            },
        )
        .expect("environment");
        environment
            .assign(
                "items",
                node_writer(),
                None,
                CanonicalVariableValue::List(vec![CanonicalVariableValue::Map(
                    [(String::from("count"), CanonicalVariableValue::Integer(1))]
                        .into_iter()
                        .collect(),
                )]),
            )
            .expect("nested value");
        assert_eq!(
            environment.assign(
                "items",
                node_writer(),
                Some(1),
                CanonicalVariableValue::List(vec![
                    CanonicalVariableValue::Map(BTreeMap::new()),
                    CanonicalVariableValue::Map(BTreeMap::new()),
                    CanonicalVariableValue::Map(BTreeMap::new()),
                ]),
            ),
            Err(VariableEnvironmentError::CollectionBoundExceeded)
        );
        assert_eq!(
            environment.assign(
                "items",
                node_writer(),
                Some(1),
                CanonicalVariableValue::List(vec![CanonicalVariableValue::Map(
                    [(String::new(), CanonicalVariableValue::Integer(1))]
                        .into_iter()
                        .collect()
                )]),
            ),
            Err(VariableEnvironmentError::InvalidMapKey)
        );
    }

    #[test]
    fn declaration_access_mutability_and_version_cas_fail_closed() {
        let mut immutable = declaration("answer", VariableValueType::Integer, "producer");
        immutable.mutability = VariableMutability::Immutable;
        let mut environment =
            CanonicalVariableEnvironment::new([immutable], VariableEnvironmentLimits::default())
                .expect("environment");
        assert!(matches!(
            environment.assign(
                "answer",
                VariableWriter::Node {
                    node_id: String::from("other"),
                    branch: None,
                },
                None,
                CanonicalVariableValue::Integer(1)
            ),
            Err(VariableEnvironmentError::UnauthorizedWriter { .. })
        ));
        environment
            .assign(
                "answer",
                node_writer(),
                None,
                CanonicalVariableValue::Integer(1),
            )
            .expect("first assignment");
        assert_eq!(
            environment.assign(
                "answer",
                node_writer(),
                Some(1),
                CanonicalVariableValue::Integer(2)
            ),
            Err(VariableEnvironmentError::ImmutableReassignment)
        );
        assert!(matches!(
            environment.read(
                "answer",
                &VariableReader {
                    node_id: String::from("other"),
                    branch_id: None
                }
            ),
            Err(VariableEnvironmentError::UnauthorizedReader { .. })
        ));
        assert_eq!(
            environment.assign(
                "missing",
                node_writer(),
                None,
                CanonicalVariableValue::Integer(1)
            ),
            Err(VariableEnvironmentError::UndeclaredVariable(String::from(
                "missing"
            )))
        );

        let mut mutable = CanonicalVariableEnvironment::new(
            [declaration(
                "counter",
                VariableValueType::Integer,
                "producer",
            )],
            VariableEnvironmentLimits::default(),
        )
        .expect("mutable");
        mutable
            .assign(
                "counter",
                node_writer(),
                None,
                CanonicalVariableValue::Integer(1),
            )
            .expect("first");
        assert_eq!(
            mutable.assign(
                "counter",
                node_writer(),
                Some(9),
                CanonicalVariableValue::Integer(2)
            ),
            Err(VariableEnvironmentError::VersionMismatch)
        );
        assert_eq!(
            mutable
                .assign(
                    "counter",
                    node_writer(),
                    Some(1),
                    CanonicalVariableValue::Integer(2)
                )
                .expect("cas")
                .version,
            2
        );
    }

    #[test]
    fn secrets_and_runtime_recorded_values_cannot_be_forged() {
        let mut secret = declaration("secret", VariableValueType::SecretReference, "producer");
        secret.security_classification = SecurityClassification::SecretReference;
        let timestamp = declaration("time", VariableValueType::Timestamp, "producer");
        let mut environment = CanonicalVariableEnvironment::new(
            [secret, timestamp],
            VariableEnvironmentLimits::default(),
        )
        .expect("environment");
        assert_eq!(
            environment.assign(
                "secret",
                node_writer(),
                None,
                CanonicalVariableValue::String(String::from("inline secret"))
            ),
            Err(VariableEnvironmentError::SecurityViolation)
        );
        assert_eq!(
            environment.assign(
                "time",
                node_writer(),
                None,
                CanonicalVariableValue::TimestampMillis(1)
            ),
            Err(VariableEnvironmentError::RuntimeRecordedValueRequired)
        );
        assert_eq!(
            CanonicalVariableValue::decimal("1e3"),
            Err(VariableEnvironmentError::InvalidDecimal)
        );
    }

    #[test]
    fn type_enum_reference_and_byte_limits_reject_without_partial_mutation() {
        let mut tag = declaration(
            "tag",
            VariableValueType::Enum {
                values: [String::from("known")].into_iter().collect(),
            },
            "producer",
        );
        tag.max_size_bytes = 64;
        let mut reference = declaration("reference", VariableValueType::TaskId, "producer");
        reference.max_size_bytes = 80;
        let mut first = declaration("first", VariableValueType::String, "producer");
        first.max_size_bytes = 80;
        let mut second = declaration("second", VariableValueType::String, "producer");
        second.max_size_bytes = 80;
        let mut environment = CanonicalVariableEnvironment::new(
            [tag, reference, first, second],
            VariableEnvironmentLimits {
                max_environment_bytes: 120,
                ..VariableEnvironmentLimits::default()
            },
        )
        .expect("environment");
        assert_eq!(
            environment.assign(
                "tag",
                node_writer(),
                None,
                CanonicalVariableValue::Enum(String::from("unknown"))
            ),
            Err(VariableEnvironmentError::InvalidEnum)
        );
        assert_eq!(
            environment.assign(
                "tag",
                node_writer(),
                None,
                CanonicalVariableValue::Integer(1)
            ),
            Err(VariableEnvironmentError::TypeMismatch)
        );
        assert_eq!(
            environment.assign(
                "reference",
                node_writer(),
                None,
                CanonicalVariableValue::TaskId(String::from("bad\nreference"))
            ),
            Err(VariableEnvironmentError::InvalidReference)
        );
        environment
            .assign(
                "first",
                node_writer(),
                None,
                CanonicalVariableValue::String("a".repeat(45)),
            )
            .expect("first bounded value");
        assert_eq!(
            environment.assign(
                "second",
                node_writer(),
                None,
                CanonicalVariableValue::String("b".repeat(45))
            ),
            Err(VariableEnvironmentError::EnvironmentSizeExceeded)
        );
        assert!(!environment.canonical_entries().contains_key("second"));
    }

    #[test]
    fn shared_branch_writes_require_merge_or_explicit_serialization() {
        let shared_declaration = declaration("shared", VariableValueType::String, "producer");
        let mut environment = CanonicalVariableEnvironment::new(
            [shared_declaration],
            VariableEnvironmentLimits::default(),
        )
        .expect("environment");
        let parallel = |serialized_shared_write| VariableWriter::Node {
            node_id: String::from("producer"),
            branch: Some(BranchWriteContext {
                branch_id: String::from("branch-a"),
                stable_order: 0,
                serialized_shared_write,
            }),
        };
        assert_eq!(
            environment.assign(
                "shared",
                parallel(false),
                None,
                CanonicalVariableValue::String(String::from("unsafe"))
            ),
            Err(VariableEnvironmentError::InvalidBranchWrite)
        );
        environment
            .assign(
                "shared",
                parallel(true),
                None,
                CanonicalVariableValue::String(String::from("serialized")),
            )
            .expect("serialized shared write");

        let mut branch = declaration("local", VariableValueType::String, "producer");
        branch.scope = VariableScope::Branch;
        let mut branch_environment =
            CanonicalVariableEnvironment::new([branch], VariableEnvironmentLimits::default())
                .expect("branch environment");
        branch_environment
            .assign(
                "local",
                parallel(false),
                None,
                CanonicalVariableValue::String(String::from("local")),
            )
            .expect("branch write");
        assert_eq!(
            branch_environment.read(
                "local",
                &VariableReader {
                    node_id: String::from("consumer"),
                    branch_id: Some(String::from("branch-b"))
                }
            ),
            Err(VariableEnvironmentError::BranchScopeMismatch)
        );
    }

    #[test]
    fn append_and_first_branch_use_stable_order_not_input_order() {
        let mut appended = declaration(
            "values",
            VariableValueType::List {
                item_type: Box::new(VariableValueType::Integer),
                max_items: 8,
            },
            "producer",
        );
        appended.merge_policy = Some(VariableMergePolicy::Append);
        let mut first = declaration("first", VariableValueType::String, "producer");
        first.merge_policy = Some(VariableMergePolicy::FirstBranch);
        let mut environment = CanonicalVariableEnvironment::new(
            [appended, first],
            VariableEnvironmentLimits::default(),
        )
        .expect("environment");
        let values = vec![
            branch_value(
                "b",
                1,
                CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(2)]),
            ),
            branch_value(
                "a",
                0,
                CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(1)]),
            ),
        ];
        assert_eq!(
            environment
                .merge_branches("values", node_writer(), None, values)
                .expect("append")
                .value,
            CanonicalVariableValue::List(vec![
                CanonicalVariableValue::Integer(1),
                CanonicalVariableValue::Integer(2)
            ])
        );
        assert_eq!(
            environment
                .merge_branches(
                    "first",
                    node_writer(),
                    None,
                    vec![
                        branch_value(
                            "later",
                            2,
                            CanonicalVariableValue::String(String::from("later"))
                        ),
                        branch_value(
                            "first",
                            1,
                            CanonicalVariableValue::String(String::from("first"))
                        )
                    ]
                )
                .expect("first branch")
                .value,
            CanonicalVariableValue::String(String::from("first"))
        );
    }

    #[test]
    fn replay_accepts_join_owned_merge_but_direct_assignment_remains_producer_only() {
        let mut shared = declaration("shared", VariableValueType::String, "left");
        shared.merge_policy = Some(VariableMergePolicy::FirstBranch);
        shared.merge_contributors.insert(String::from("right"));
        let mut environment =
            CanonicalVariableEnvironment::new([shared], VariableEnvironmentLimits::default())
                .expect("environment");
        let join = VariableWriter::Node {
            node_id: String::from("join"),
            branch: None,
        };
        assert!(matches!(
            environment.assign(
                "shared",
                join.clone(),
                None,
                CanonicalVariableValue::String(String::from("forged")),
            ),
            Err(VariableEnvironmentError::UnauthorizedWriter { .. })
        ));
        environment
            .merge_branches(
                "shared",
                join,
                None,
                vec![
                    branch_value(
                        "left",
                        0,
                        CanonicalVariableValue::String(String::from("left")),
                    ),
                    branch_value(
                        "right",
                        1,
                        CanonicalVariableValue::String(String::from("right")),
                    ),
                ],
            )
            .expect("join merge");
        environment
            .validate_replayed()
            .expect("join-owned merge replays");
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "union ordering and recursive deep-merge success/conflict share one declaration-bound fixture"
    )]
    fn union_is_canonically_sorted_and_deep_merge_rejects_conflicts() {
        let mut union = declaration(
            "union",
            VariableValueType::List {
                item_type: Box::new(VariableValueType::String),
                max_items: 8,
            },
            "producer",
        );
        union.merge_policy = Some(VariableMergePolicy::Union);
        let mut deep = declaration(
            "deep",
            VariableValueType::Map {
                value_type: Box::new(VariableValueType::Map {
                    value_type: Box::new(VariableValueType::String),
                    max_entries: 4,
                }),
                max_entries: 4,
            },
            "producer",
        );
        deep.merge_policy = Some(VariableMergePolicy::DeepMerge);
        let mut environment =
            CanonicalVariableEnvironment::new([union, deep], VariableEnvironmentLimits::default())
                .expect("environment");
        assert_eq!(
            environment
                .merge_branches(
                    "union",
                    node_writer(),
                    None,
                    vec![
                        branch_value(
                            "b",
                            1,
                            CanonicalVariableValue::List(vec![
                                CanonicalVariableValue::String(String::from("z")),
                                CanonicalVariableValue::String(String::from("a")),
                            ])
                        ),
                        branch_value(
                            "a",
                            0,
                            CanonicalVariableValue::List(vec![CanonicalVariableValue::String(
                                String::from("z")
                            )])
                        )
                    ]
                )
                .expect("union")
                .value,
            CanonicalVariableValue::List(vec![
                CanonicalVariableValue::String(String::from("a")),
                CanonicalVariableValue::String(String::from("z"))
            ])
        );
        let nested = |key: &str, value: &str| {
            CanonicalVariableValue::Map(
                [(
                    String::from("group"),
                    CanonicalVariableValue::Map(
                        [(key.to_owned(), CanonicalVariableValue::String(value.into()))]
                            .into_iter()
                            .collect(),
                    ),
                )]
                .into_iter()
                .collect(),
            )
        };
        assert_eq!(
            environment
                .merge_branches(
                    "deep",
                    node_writer(),
                    None,
                    vec![
                        branch_value("a", 0, nested("left", "one")),
                        branch_value("b", 1, nested("right", "two"))
                    ]
                )
                .expect("recursive deep merge")
                .value,
            CanonicalVariableValue::Map(
                [(
                    String::from("group"),
                    CanonicalVariableValue::Map(
                        [
                            (
                                String::from("left"),
                                CanonicalVariableValue::String(String::from("one"))
                            ),
                            (
                                String::from("right"),
                                CanonicalVariableValue::String(String::from("two"))
                            )
                        ]
                        .into_iter()
                        .collect()
                    )
                )]
                .into_iter()
                .collect()
            )
        );
        assert_eq!(
            environment.merge_branches(
                "deep",
                node_writer(),
                Some(1),
                vec![
                    branch_value("a", 0, nested("same", "one")),
                    branch_value("b", 1, nested("same", "two"))
                ]
            ),
            Err(VariableEnvironmentError::MergeConflict)
        );
    }

    #[test]
    fn condition_classification_is_exact_and_deterministic() {
        let declarations = [
            declaration("ready", VariableValueType::Boolean, "producer"),
            declaration("count", VariableValueType::Integer, "producer"),
            declaration("missing", VariableValueType::String, "producer"),
        ];
        let mut environment =
            CanonicalVariableEnvironment::new(declarations, VariableEnvironmentLimits::default())
                .expect("environment");
        environment
            .assign(
                "ready",
                node_writer(),
                None,
                CanonicalVariableValue::Boolean(true),
            )
            .expect("ready");
        environment
            .assign(
                "count",
                node_writer(),
                None,
                CanonicalVariableValue::Integer(2),
            )
            .expect("count");
        let required = [String::from("ready"), String::from("count")]
            .into_iter()
            .collect();
        assert_eq!(
            environment.classify_condition(
                "ready && count >= 2",
                &reader(),
                &required,
                ExpressionLimits::default()
            ),
            ConditionEligibility::Eligible
        );
        assert_eq!(
            environment.classify_condition(
                "count < 2",
                &reader(),
                &required,
                ExpressionLimits::default()
            ),
            ConditionEligibility::Ineligible
        );
        assert_eq!(
            environment.classify_condition(
                "missing == \"x\"",
                &reader(),
                &[String::from("missing")].into_iter().collect(),
                ExpressionLimits::default()
            ),
            ConditionEligibility::MissingInput {
                path: String::from("missing")
            }
        );
        assert!(matches!(
            environment.classify_condition(
                "ready ==",
                &reader(),
                &required,
                ExpressionLimits::default()
            ),
            ConditionEligibility::InvalidExpression { .. }
        ));
        assert_eq!(
            environment.classify_condition(
                "unknown == true",
                &reader(),
                &required,
                ExpressionLimits::default()
            ),
            ConditionEligibility::MissingInput {
                path: String::from("unknown")
            }
        );
    }

    fn apply_attempt(
        reducer: &mut CanonicalVariableEventReducer,
        node_id: &str,
        attempt: VariableValidationAttempt,
    ) -> CanonicalVariableEvent {
        let event = reducer
            .prepare_event(node_id, attempt)
            .expect("prepare canonical event");
        reducer.apply(event.clone()).expect("apply canonical event");
        event
    }

    /// Ported concept from `audit/task-04`: deterministic replay under random
    /// assignment order. The converged reducer is a pure function of its
    /// canonical events, so replaying the exact same journal from a fresh
    /// reducer must reconstruct byte-identical state no matter how many
    /// declarations, assignments, and merges preceded it.
    fn apply_random_sequence(
        reducer: &mut CanonicalVariableEventReducer,
        names: &[String],
        seed: u64,
    ) -> Vec<CanonicalVariableEvent> {
        let mut events = Vec::new();
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = |bound: usize| -> usize {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) as usize % bound
        };
        for name in names {
            events.push(apply_attempt(
                reducer,
                RUNTIME_PRODUCER,
                VariableValidationAttempt::Declare {
                    declaration: declaration(
                        name,
                        VariableValueType::String,
                        RUNTIME_PRODUCER,
                    ),
                },
            ));
        }
        let rounds = 3 + next(5);
        for _ in 0..rounds {
            let name = &names[next(names.len())];
            let value =
                CanonicalVariableValue::String(format!("value-{}-{}", seed, next(1000)));
            events.push(apply_attempt(
                reducer,
                RUNTIME_PRODUCER,
                VariableValidationAttempt::Assign {
                    variable: name.clone(),
                    writer: node_writer(),
                    expected_version: None,
                    value,
                },
            ));
        }
        events
    }

    #[test]
    fn replay_reconstructs_identical_state_under_random_assignment_order() {
        use proptest::prelude::*;
        proptest!(|(seed in any::<u64>(), names in prop::collection::vec(proptest::string::string_regex("v[0-9]").unwrap(), 1..6))| {
            let mut names = names;
            names.sort();
            names.dedup();
            let mut live = CanonicalVariableEventReducer::new(
                "run:variables:prop",
                VariableEnvironmentLimits::default(),
            )
            .expect("live reducer");
            let events = apply_random_sequence(&mut live, &names, seed);

            let mut replay = CanonicalVariableEventReducer::new(
                "run:variables:prop",
                VariableEnvironmentLimits::default(),
            )
            .expect("replay reducer");
            for event in &events {
                replay.apply(event.clone()).expect("replay event");
            }
            let replay_hash = replay.state_hash().expect("replay hash");
            let live_hash = live.state_hash().expect("live hash");
            prop_assert!(
                replay_hash == live_hash,
                "replay diverged from live state under seed {}",
                seed
            );

            // A serialized snapshot survives restart and revalidates.
            let bytes = serde_json::to_vec(&live).expect("serialize reducer");
            let recovered: CanonicalVariableEventReducer =
                serde_json::from_slice(&bytes).expect("deserialize reducer");
            recovered.validate_replayed().expect("validate recovered");
            prop_assert_eq!(
                recovered.state_hash().expect("recovered hash"),
                live.state_hash().expect("live hash")
            );
        });
    }

    #[test]
    fn event_reducer_replays_declaration_assignment_artifacts_and_rejects_tampering() {
        let mut reducer = CanonicalVariableEventReducer::new(
            "run:variables:1",
            VariableEnvironmentLimits::default(),
        )
        .expect("reducer");
        let declaration = declaration("artifact", VariableValueType::ArtifactReference, "producer");
        let declared = apply_attempt(
            &mut reducer,
            RUNTIME_PRODUCER,
            VariableValidationAttempt::Declare {
                declaration: declaration.clone(),
            },
        );
        assert_eq!(declared.event_type(), "graph.variable_declared");
        let assigned = apply_attempt(
            &mut reducer,
            "producer",
            VariableValidationAttempt::Assign {
                variable: String::from("artifact"),
                writer: node_writer(),
                expected_version: None,
                value: CanonicalVariableValue::ArtifactReference(String::from("blake3:artifact")),
            },
        );
        assert_eq!(assigned.event_type(), "graph.variable_assigned");
        assert_eq!(assigned.binding().prior_version, None);
        assert_eq!(assigned.binding().new_version, Some(1));
        assert_eq!(
            assigned.binding().artifact_references,
            [String::from("blake3:artifact")].into_iter().collect()
        );

        let bytes = serde_json::to_vec(&reducer).expect("serialize reducer");
        let recovered: CanonicalVariableEventReducer =
            serde_json::from_slice(&bytes).expect("deserialize reducer");
        recovered.validate_replayed().expect("validate recovered");
        assert_eq!(
            recovered.state_hash().expect("recovered hash"),
            reducer.state_hash().expect("live hash")
        );

        let mut replay = CanonicalVariableEventReducer::new(
            "run:variables:1",
            VariableEnvironmentLimits::default(),
        )
        .expect("replay");
        replay.apply(declared).expect("replay declaration");
        let mut tampered = assigned.clone();
        let CanonicalVariableEvent::Assigned(event) = &mut tampered else {
            panic!("assigned event");
        };
        event.binding.value_hash = Some(ContentHash::digest(b"tampered"));
        assert_eq!(
            replay.apply(tampered),
            Err(VariableEnvironmentError::EventMismatch)
        );
        assert!(replay.environment().canonical_entries().is_empty());
        replay.apply(assigned).expect("exact assignment");
        assert_eq!(
            replay.state_hash().expect("replay hash"),
            reducer.state_hash().expect("source hash")
        );
    }

    #[test]
    fn merge_and_validation_failure_events_are_deterministic_and_non_mutating() {
        let mut reducer = CanonicalVariableEventReducer::new(
            "run:variables:merge",
            VariableEnvironmentLimits::default(),
        )
        .expect("reducer");
        let mut declaration = declaration(
            "items",
            VariableValueType::List {
                item_type: Box::new(VariableValueType::Integer),
                max_items: 8,
            },
            "producer",
        );
        declaration.merge_policy = Some(VariableMergePolicy::Append);
        apply_attempt(
            &mut reducer,
            RUNTIME_PRODUCER,
            VariableValidationAttempt::Declare { declaration },
        );
        let merged = apply_attempt(
            &mut reducer,
            "producer",
            VariableValidationAttempt::Merge {
                variable: String::from("items"),
                writer: node_writer(),
                expected_version: None,
                branches: vec![
                    branch_value(
                        "later",
                        1,
                        CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(2)]),
                    ),
                    branch_value(
                        "first",
                        0,
                        CanonicalVariableValue::List(vec![CanonicalVariableValue::Integer(1)]),
                    ),
                ],
            },
        );
        let CanonicalVariableEvent::Merged(merged_event) = merged else {
            panic!("merged event");
        };
        assert_eq!(merged_event.policy, VariableMergePolicy::Append);
        assert_eq!(merged_event.branches[0].branch_id, "first");
        assert_eq!(
            reducer.environment().classify_condition(
                "exists(items[1])",
                &reader(),
                &[String::from("items")].into_iter().collect(),
                ExpressionLimits::default()
            ),
            ConditionEligibility::Eligible
        );

        let environment_hash = reducer
            .environment()
            .state_hash()
            .expect("environment hash");
        let failed = reducer
            .prepare_event(
                "producer",
                VariableValidationAttempt::Assign {
                    variable: String::from("items"),
                    writer: node_writer(),
                    expected_version: None,
                    value: CanonicalVariableValue::List(vec![]),
                },
            )
            .expect("prepare failure");
        let CanonicalVariableEvent::ValidationFailed(failure) = &failed else {
            panic!("validation failure");
        };
        assert_eq!(failure.code, VariableValidationFailureCode::VersionConflict);
        assert_eq!(
            reducer.apply(failed),
            Ok(VariableEventApplyOutcome::ValidationFailed(
                VariableValidationFailureCode::VersionConflict
            ))
        );
        assert_eq!(
            reducer.environment().state_hash().expect("after failure"),
            environment_hash,
            "a validation failure cannot mutate live variables"
        );
        let recovered: CanonicalVariableEventReducer =
            serde_json::from_slice(&serde_json::to_vec(&reducer).expect("serialize"))
                .expect("deserialize");
        recovered
            .validate_replayed()
            .expect("failure survives restart validation");
    }

    #[test]
    fn removal_is_versioned_scope_bounded_and_terminal() {
        let mut reducer = CanonicalVariableEventReducer::new(
            "run:variables:remove",
            VariableEnvironmentLimits::default(),
        )
        .expect("reducer");
        let mut local = declaration("local", VariableValueType::ArtifactReference, "producer");
        local.scope = VariableScope::Node;
        local.consumers = [String::from("producer")].into_iter().collect();
        apply_attempt(
            &mut reducer,
            RUNTIME_PRODUCER,
            VariableValidationAttempt::Declare { declaration: local },
        );
        apply_attempt(
            &mut reducer,
            "producer",
            VariableValidationAttempt::Assign {
                variable: String::from("local"),
                writer: node_writer(),
                expected_version: None,
                value: CanonicalVariableValue::ArtifactReference(String::from("artifact:local")),
            },
        );
        let removed = apply_attempt(
            &mut reducer,
            "producer",
            VariableValidationAttempt::Remove {
                variable: String::from("local"),
                writer: node_writer(),
                expected_version: 1,
            },
        );
        assert_eq!(removed.event_type(), "graph.variable_removed");
        assert_eq!(removed.binding().prior_version, Some(1));
        assert_eq!(removed.binding().new_version, Some(2));
        assert_eq!(
            removed.binding().artifact_references,
            [String::from("artifact:local")].into_iter().collect()
        );
        assert!(
            !reducer
                .environment()
                .canonical_entries()
                .contains_key("local")
        );
        assert_eq!(reducer.removed()["local"].version, 2);

        let retry = reducer
            .prepare_event(
                "producer",
                VariableValidationAttempt::Assign {
                    variable: String::from("local"),
                    writer: node_writer(),
                    expected_version: Some(2),
                    value: CanonicalVariableValue::ArtifactReference(String::from(
                        "artifact:replacement",
                    )),
                },
            )
            .expect("terminal removal failure");
        assert!(matches!(
            retry,
            CanonicalVariableEvent::ValidationFailed(ref failure)
                if failure.code == VariableValidationFailureCode::RemovalNotAllowed
        ));

        let mut run_scoped = declaration("run_value", VariableValueType::String, "producer");
        run_scoped.scope = VariableScope::Run;
        apply_attempt(
            &mut reducer,
            RUNTIME_PRODUCER,
            VariableValidationAttempt::Declare {
                declaration: run_scoped,
            },
        );
        apply_attempt(
            &mut reducer,
            "producer",
            VariableValidationAttempt::Assign {
                variable: String::from("run_value"),
                writer: node_writer(),
                expected_version: None,
                value: CanonicalVariableValue::String(String::from("retained")),
            },
        );
        let invalid_remove = reducer
            .prepare_event(
                "producer",
                VariableValidationAttempt::Remove {
                    variable: String::from("run_value"),
                    writer: node_writer(),
                    expected_version: 1,
                },
            )
            .expect("scope failure");
        assert!(matches!(
            invalid_remove,
            CanonicalVariableEvent::ValidationFailed(ref failure)
                if failure.code == VariableValidationFailureCode::RemovalNotAllowed
        ));
    }

    #[test]
    fn replay_rejects_hash_tampering() {
        let mut environment = CanonicalVariableEnvironment::new(
            [declaration("value", VariableValueType::String, "producer")],
            VariableEnvironmentLimits::default(),
        )
        .expect("environment");
        environment
            .assign(
                "value",
                node_writer(),
                None,
                CanonicalVariableValue::String(String::from("original")),
            )
            .expect("assignment");
        environment.entries.get_mut("value").expect("entry").value =
            CanonicalVariableValue::String(String::from("tampered"));
        assert_eq!(
            environment.validate_replayed(),
            Err(VariableEnvironmentError::ValueHashMismatch(String::from(
                "value"
            )))
        );
    }
}
