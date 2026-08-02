//! Variable declarations: stable names, types, scopes, producers, consumers,
//! mutability, size, classification, merge policy, and defaults.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::value::{Decimal, GraphValue, GraphValueError, MAX_DECIMAL_SCALE};

/// Canonical variable type with explicit bounds.
///
/// Null is accepted only under [`VariableType::Optional`]; every other type
/// validates values against its declared bounds before they may be committed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum VariableType {
    /// Nullable wrapper; accepts null or the inner type.
    Optional(Box<Self>),
    /// Boolean.
    Boolean,
    /// Signed integer in the inclusive declared range.
    SignedInteger {
        /// Inclusive lower bound.
        min: i64,
        /// Inclusive upper bound.
        max: i64,
    },
    /// Unsigned integer in the inclusive declared range.
    UnsignedInteger {
        /// Inclusive lower bound.
        min: u64,
        /// Inclusive upper bound.
        max: u64,
    },
    /// Fixed-point decimal in the inclusive declared range.
    Decimal {
        /// Inclusive lower bound.
        min: Decimal,
        /// Inclusive upper bound.
        max: Decimal,
    },
    /// UTF-8 string no larger than the declared bound.
    String {
        /// Maximum UTF-8 bytes.
        max_bytes: usize,
    },
    /// Closed tag set.
    EnumTag {
        /// Allowed tags.
        tags: BTreeSet<String>,
    },
    /// Ordered list with a bounded length and a declared element type.
    List {
        /// Element type.
        element: Box<Self>,
        /// Maximum element count.
        max_len: usize,
    },
    /// Object with string keys and a declared value type.
    Map {
        /// Value type.
        value: Box<Self>,
        /// Maximum field count.
        max_len: usize,
    },
    /// Opaque session identifier.
    SessionId,
    /// Opaque child-session identifier.
    ChildSessionId,
    /// Runtime-owned task identity.
    TaskId,
    /// Compiled graph node identity.
    NodeId,
    /// Opaque continuation identity.
    ContinuationId,
    /// Reference to an immutable artifact.
    ArtifactReference,
    /// Reference to a completed tool result.
    ToolResultReference,
    /// Reference to a joined child result.
    ChildResultReference,
    /// Approval decision.
    ApprovalDecision,
    /// Approved secret reference only; plaintext is rejected.
    Secret,
    /// Canonical timestamp.
    Timestamp,
    /// Canonical duration in milliseconds.
    Duration,
}

impl VariableType {
    /// Validates the declaration bounds themselves.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareError::InvalidTypeBounds`] when a declared range is
    /// empty, a length bound is zero, or a decimal bound is malformed.
    pub fn validate(&self) -> Result<(), DeclareError> {
        match self {
            Self::Optional(inner) => inner.validate(),
            Self::SignedInteger { min, max } => {
                if min > max {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "signed integer minimum exceeds maximum".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }
            Self::UnsignedInteger { min, max } => {
                if min > max {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "unsigned integer minimum exceeds maximum".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }
            Self::Decimal { min, max } => {
                min.validate()?;
                max.validate()?;
                if min > max {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "decimal minimum exceeds maximum".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }
            Self::String { max_bytes } => {
                if *max_bytes == 0 {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "string bound must be at least one byte".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }
            Self::EnumTag { tags } => {
                if tags.is_empty() {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "enum tag set must not be empty".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }
            Self::List { element, max_len } => {
                if *max_len == 0 {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "list length bound must be at least one".to_owned(),
                    })
                } else {
                    element.validate()
                }
            }
            Self::Map { value, max_len } => {
                if *max_len == 0 {
                    Err(DeclareError::InvalidTypeBounds {
                        detail: "map length bound must be at least one".to_owned(),
                    })
                } else {
                    value.validate()
                }
            }
            Self::Boolean
            | Self::SessionId
            | Self::ChildSessionId
            | Self::TaskId
            | Self::NodeId
            | Self::ContinuationId
            | Self::ArtifactReference
            | Self::ToolResultReference
            | Self::ChildResultReference
            | Self::ApprovalDecision
            | Self::Secret
            | Self::Timestamp
            | Self::Duration => Ok(()),
        }
    }

    /// Returns whether the value conforms to this type and its bounds.
    #[must_use]
    pub fn accepts(&self, value: &GraphValue) -> bool {
        match self {
            Self::Optional(inner) => matches!(value, GraphValue::Null) || inner.accepts(value),
            Self::Boolean => matches!(value, GraphValue::Boolean(_)),
            Self::SignedInteger { min, max } => {
                matches!(value, GraphValue::SignedInteger(value) if *min <= *value && *value <= *max)
            }
            Self::UnsignedInteger { min, max } => {
                matches!(value, GraphValue::UnsignedInteger(value) if *min <= *value && *value <= *max)
            }
            Self::Decimal { min, max } => {
                matches!(value, GraphValue::Decimal(value) if *min <= *value && *value <= *max)
            }
            Self::String { max_bytes } => matches!(
                value,
                GraphValue::String(value) if value.len() <= *max_bytes
            ),
            Self::EnumTag { tags } => {
                matches!(value, GraphValue::EnumTag(tag) if tags.contains(tag))
            }
            Self::List { element, max_len } => matches!(
                value,
                GraphValue::List(values)
                    if values.len() <= *max_len && values.iter().all(|item| element.accepts(item))
            ),
            Self::Map {
                value: value_type,
                max_len,
            } => matches!(
                value,
                GraphValue::Map(fields)
                    if fields.len() <= *max_len
                        && fields.values().all(|item| value_type.accepts(item))
            ),
            Self::SessionId => matches!(value, GraphValue::SessionId(_)),
            Self::ChildSessionId => matches!(value, GraphValue::ChildSessionId(_)),
            Self::TaskId => matches!(value, GraphValue::TaskId(_)),
            Self::NodeId => matches!(value, GraphValue::NodeId(_)),
            Self::ContinuationId => matches!(value, GraphValue::ContinuationId(_)),
            Self::ArtifactReference => matches!(value, GraphValue::ArtifactReference(_)),
            Self::ToolResultReference => matches!(value, GraphValue::ToolResultReference(_)),
            Self::ChildResultReference => matches!(value, GraphValue::ChildResultReference(_)),
            Self::ApprovalDecision => matches!(value, GraphValue::ApprovalDecision(_)),
            Self::Secret => value.is_secret_reference(),
            Self::Timestamp => matches!(value, GraphValue::Timestamp(_)),
            Self::Duration => matches!(value, GraphValue::DurationMillis(_)),
        }
    }

    /// Returns whether this type is the list/map container expected by a merge
    /// policy; used to machine-validate parallel write safety.
    #[must_use]
    pub const fn is_list(&self) -> bool {
        matches!(self, Self::List { .. })
    }

    /// Returns whether this type is the map container expected by a merge
    /// policy.
    #[must_use]
    pub const fn is_map(&self) -> bool {
        matches!(self, Self::Map { .. })
    }
}

/// Scope that owns a variable within a style run.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum VariableScope {
    /// Owned by the whole run; visible to every node.
    Run,
    /// Owned by one branch-local scope.
    Branch {
        /// Stable branch identifier.
        branch_id: String,
    },
    /// Owned by one node's execution.
    Node {
        /// Stable node identifier.
        node_id: String,
    },
}

impl VariableScope {
    /// Returns a stable textual identity for events and diagnostics.
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Run => "run",
            Self::Branch { branch_id } => branch_id,
            Self::Node { node_id } => node_id,
        }
    }
}

impl fmt::Display for VariableScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Run => formatter.write_str("run"),
            Self::Branch { branch_id } => write!(formatter, "branch:{branch_id}"),
            Self::Node { node_id } => write!(formatter, "node:{node_id}"),
        }
    }
}

/// Mutability and versioning policy for one variable.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutabilityPolicy {
    /// Write once per scope lifetime; later writes are rejected.
    Immutable,
    /// Overwrite allowed; every accepted assignment advances the version.
    Assignable,
    /// Versioned; every accepted assignment advances the version and prior
    /// versions remain reconstructable from the canonical journal.
    Versioned,
}

/// Security classification of one variable.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityClassification {
    /// Visible to providers and frontends.
    Public,
    /// Visible only inside the runtime session.
    SessionInternal,
    /// Never plaintext; represented by approved secret references.
    Secret,
}

/// Deterministic merge policy for variables with multiple writers.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "policy", content = "value", rename_all = "snake_case")]
pub enum MergePolicy {
    /// Reject parallel writes that both modify the variable.
    RejectConflict,
    /// Explicit deterministic last-writer ordering.
    LastWriter {
        /// Deterministic ordering used to select the winner.
        ordering: LastWriterOrdering,
    },
    /// Append contributor lists in deterministic contributor order.
    ListAppend,
    /// Union contributor lists, deduplicated by canonical bytes.
    SetUnion,
    /// Merge object fields; conflicting keys with differing values reject.
    ObjectFieldMerge,
}

/// Deterministic ordering for last-writer merges.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LastWriterOrdering {
    /// Lexicographic branch identity; highest identity wins.
    BranchLexical,
    /// Lexicographic node identity; highest identity wins.
    NodeLexical,
}

/// Policy applied when a branch-local scope is created.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BranchScopePolicy {
    /// Starts from declared defaults; parent writes are invisible except for
    /// immutable shared reads.
    Isolated,
    /// Snapshots parent values at creation; writes become branch-local.
    CopyOnWrite,
}

/// One variable declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VariableDeclaration {
    /// Stable variable name.
    pub name: String,
    /// Typed value contract.
    pub r#type: VariableType,
    /// Owning scope.
    pub scope: VariableScope,
    /// Declared producer node IDs; empty means runtime-owned.
    #[serde(default)]
    pub producers: BTreeSet<String>,
    /// Declared consumer node IDs.
    #[serde(default)]
    pub consumers: BTreeSet<String>,
    /// Mutability and versioning policy.
    #[serde(default = "default_mutability")]
    pub mutability: MutabilityPolicy,
    /// Maximum canonical serialized bytes.
    pub max_serialized_bytes: usize,
    /// Security classification.
    #[serde(default = "default_classification")]
    pub classification: SecurityClassification,
    /// Merge policy where parallel writes are possible.
    #[serde(default = "default_merge_policy")]
    pub merge_policy: MergePolicy,
    /// Optional default value, validated against the type.
    #[serde(default)]
    pub default: Option<GraphValue>,
}

const fn default_mutability() -> MutabilityPolicy {
    MutabilityPolicy::Assignable
}

const fn default_classification() -> SecurityClassification {
    SecurityClassification::SessionInternal
}

const fn default_merge_policy() -> MergePolicy {
    MergePolicy::RejectConflict
}

impl VariableDeclaration {
    /// Validates the declaration in isolation.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareError`] when the name is invalid, the type bounds are
    /// invalid, the default does not conform, or the merge policy is
    /// incompatible with the declared type.
    pub fn validate(&self) -> Result<(), DeclareError> {
        validate_name(&self.name)?;
        self.r#type.validate()?;
        if self.max_serialized_bytes == 0 {
            return Err(DeclareError::InvalidSizeBound {
                name: self.name.clone(),
            });
        }
        if let Some(default) = &self.default {
            if !self.r#type.accepts(default) {
                return Err(DeclareError::DefaultTypeMismatch {
                    name: self.name.clone(),
                });
            }
            if default.serialized_bytes() > self.max_serialized_bytes {
                return Err(DeclareError::DefaultTooLarge {
                    name: self.name.clone(),
                });
            }
        }
        if matches!(self.classification, SecurityClassification::Secret)
            && !secret_compatible_type(&self.r#type)
        {
            return Err(DeclareError::InvalidTypeBounds {
                detail: format!(
                    "secret-classified variable `{}` must declare a secret-reference type",
                    self.name
                ),
            });
        }
        if !merge_policy_compatible(self) {
            return Err(DeclareError::MergePolicyTypeMismatch {
                name: self.name.clone(),
                policy: format!("{:?}", self.merge_policy),
            });
        }
        Ok(())
    }
}

/// Validates an identifier/name against the canonical character set.
///
/// # Errors
///
/// Returns [`DeclareError::InvalidName`] when the name is empty, too long, or
/// contains unsupported characters.
pub fn validate_name(value: &str) -> Result<(), DeclareError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "_.:/-".contains(character))
    {
        Err(DeclareError::InvalidName {
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

/// Returns whether a merge policy is compatible with the declared type.
#[must_use]
pub fn merge_policy_compatible(declaration: &VariableDeclaration) -> bool {
    match declaration.merge_policy {
        MergePolicy::ListAppend | MergePolicy::SetUnion => declaration.r#type.is_list(),
        MergePolicy::ObjectFieldMerge => declaration.r#type.is_map(),
        MergePolicy::RejectConflict | MergePolicy::LastWriter { .. } => true,
    }
}

/// Returns whether a type admits only approved secret references.
#[must_use]
pub fn secret_compatible_type(r#type: &VariableType) -> bool {
    match r#type {
        VariableType::Secret => true,
        VariableType::Optional(inner) => secret_compatible_type(inner),
        _ => false,
    }
}

/// Ordered set of declarations keyed by stable name.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DeclarationSet {
    declarations: BTreeMap<String, VariableDeclaration>,
}

impl DeclarationSet {
    /// Creates an empty declaration set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            declarations: BTreeMap::new(),
        }
    }

    /// Inserts and validates a declaration.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareError`] when the declaration is invalid or a variable
    /// with the same name already exists.
    pub fn insert(&mut self, declaration: VariableDeclaration) -> Result<(), DeclareError> {
        declaration.validate()?;
        if self.declarations.contains_key(&declaration.name) {
            return Err(DeclareError::DuplicateName {
                name: declaration.name,
            });
        }
        self.declarations
            .insert(declaration.name.clone(), declaration);
        Ok(())
    }

    /// Validates every producer/consumer reference against the node universe.
    ///
    /// # Errors
    ///
    /// Returns [`DeclareError::UnknownNodeReference`] when a producer or
    /// consumer references a node that does not exist in `node_ids`.
    pub fn validate_nodes(&self, node_ids: &BTreeSet<String>) -> Result<(), DeclareError> {
        for declaration in self.declarations.values() {
            let mut references: Vec<&str> = declaration
                .producers
                .iter()
                .map(String::as_str)
                .chain(declaration.consumers.iter().map(String::as_str))
                .collect();
            if let VariableScope::Node { node_id } = &declaration.scope {
                references.push(node_id);
            }
            for reference in references {
                if !node_ids.contains(reference) {
                    return Err(DeclareError::UnknownNodeReference {
                        name: declaration.name.clone(),
                        reference: reference.to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    /// Returns the declaration for `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&VariableDeclaration> {
        self.declarations.get(name)
    }

    /// Returns whether a variable is declared.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.declarations.contains_key(name)
    }

    /// Returns the number of declarations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.declarations.len()
    }

    /// Returns whether the set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.declarations.is_empty()
    }

    /// Iterates declarations in stable name order.
    pub fn iter(&self) -> impl Iterator<Item = &VariableDeclaration> {
        self.declarations.values()
    }

    /// Returns all declared names in stable order.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.declarations.keys().map(String::as_str).collect()
    }
}

/// Declaration validation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DeclareError {
    /// The name is empty, too long, or contains unsupported characters.
    #[error("invalid variable name `{value}`")]
    InvalidName {
        /// Invalid name.
        value: String,
    },
    /// A variable with the same name is already declared.
    #[error("duplicate variable declaration `{name}`")]
    DuplicateName {
        /// Duplicate name.
        name: String,
    },
    /// A declared type range or bound is invalid.
    #[error("invalid type bounds: {detail}")]
    InvalidTypeBounds {
        /// Deterministic diagnostic.
        detail: String,
    },
    /// The serialized-size bound must be positive.
    #[error("variable `{name}` has a zero serialized-size bound")]
    InvalidSizeBound {
        /// Variable name.
        name: String,
    },
    /// A default value does not conform to the declared type.
    #[error("default for `{name}` does not conform to its declared type")]
    DefaultTypeMismatch {
        /// Variable name.
        name: String,
    },
    /// A default value exceeds the declared serialized-size bound.
    #[error("default for `{name}` exceeds its declared size bound")]
    DefaultTooLarge {
        /// Variable name.
        name: String,
    },
    /// The merge policy is incompatible with the declared type.
    #[error("merge policy `{policy}` is incompatible with the declared type of `{name}`")]
    MergePolicyTypeMismatch {
        /// Variable name.
        name: String,
        /// Policy diagnostic.
        policy: String,
    },
    /// A producer or consumer references an unknown node.
    #[error("variable `{name}` references unknown node `{reference}`")]
    UnknownNodeReference {
        /// Variable name.
        name: String,
        /// Unknown node identity.
        reference: String,
    },
}

/// Validates a default value against a type without declaring it.
///
/// # Errors
///
/// Returns [`DeclareError::InvalidTypeBounds`] when the type itself is
/// malformed or [`DeclareError::DefaultTypeMismatch`] when the value does not
/// conform.
pub fn validate_default(r#type: &VariableType, value: &GraphValue) -> Result<(), DeclareError> {
    r#type.validate()?;
    if r#type.accepts(value) {
        Ok(())
    } else {
        Err(DeclareError::DefaultTypeMismatch {
            name: "(unbound)".to_owned(),
        })
    }
}

impl From<GraphValueError> for DeclareError {
    fn from(error: GraphValueError) -> Self {
        Self::InvalidTypeBounds {
            detail: error.to_string(),
        }
    }
}

/// Maximum canonical serialized bytes for a graph value (1 MiB ceiling).
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;

/// Returns the canonical maximum scale used by decimal declarations.
#[must_use]
pub const fn max_decimal_scale() -> u8 {
    MAX_DECIMAL_SCALE
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::SecretReference;

    fn declaration(name: &str) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            r#type: VariableType::Boolean,
            scope: VariableScope::Run,
            producers: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: MutabilityPolicy::Assignable,
            max_serialized_bytes: 512,
            classification: SecurityClassification::SessionInternal,
            merge_policy: MergePolicy::RejectConflict,
            default: Some(GraphValue::Boolean(false)),
        }
    }

    #[test]
    fn declaration_set_rejects_duplicates_and_bad_defaults() {
        let mut set = DeclarationSet::new();
        set.insert(declaration("ready")).expect("valid");
        assert_eq!(
            set.insert(declaration("ready")),
            Err(DeclareError::DuplicateName {
                name: "ready".to_owned(),
            })
        );
        let mut bad = declaration("count");
        bad.r#type = VariableType::SignedInteger { min: 0, max: 10 };
        bad.default = Some(GraphValue::String("x".into()));
        assert_eq!(
            set.insert(bad),
            Err(DeclareError::DefaultTypeMismatch {
                name: "count".to_owned()
            })
        );
    }

    #[test]
    fn type_bounds_are_checked() {
        let mut count_decl = declaration("count");
        count_decl.r#type = VariableType::SignedInteger { min: 10, max: 1 };
        assert!(matches!(
            count_decl.validate(),
            Err(DeclareError::InvalidTypeBounds { .. })
        ));
        let mut tag_decl = declaration("tag");
        tag_decl.r#type = VariableType::EnumTag {
            tags: BTreeSet::new(),
        };
        assert!(matches!(
            tag_decl.validate(),
            Err(DeclareError::InvalidTypeBounds { .. })
        ));
    }

    #[test]
    fn merge_policies_require_matching_containers() {
        let mut list = declaration("items");
        list.default = None;
        list.r#type = VariableType::List {
            element: Box::new(VariableType::String { max_bytes: 64 }),
            max_len: 100,
        };
        list.merge_policy = MergePolicy::SetUnion;
        assert_eq!(list.validate(), Ok(()));
        list.merge_policy = MergePolicy::ObjectFieldMerge;
        assert!(matches!(
            list.validate(),
            Err(DeclareError::MergePolicyTypeMismatch { .. })
        ));
    }

    #[test]
    fn secret_declarations_reject_plaintext_defaults() {
        let mut secret = declaration("api_key");
        secret.r#type = VariableType::Secret;
        secret.classification = SecurityClassification::Secret;
        secret.default = Some(GraphValue::String("plaintext".into()));
        assert!(matches!(
            secret.validate(),
            Err(DeclareError::DefaultTypeMismatch { .. })
        ));
        secret.default = Some(GraphValue::SecretReference(
            SecretReference::new("secret:0001".into()).expect("valid"),
        ));
        assert_eq!(secret.validate(), Ok(()));
    }

    #[test]
    fn node_references_are_validated() {
        let mut set = DeclarationSet::new();
        let mut declaration = declaration("out");
        declaration.producers = ["plan".into()].into_iter().collect();
        set.insert(declaration).expect("valid");
        let nodes: BTreeSet<_> = ["plan".into(), "done".into()].into_iter().collect();
        assert_eq!(set.validate_nodes(&nodes), Ok(()));
        assert!(matches!(
            set.validate_nodes(&BTreeSet::from(["done".to_owned()])),
            Err(DeclareError::UnknownNodeReference { .. })
        ));
    }

    #[test]
    fn accepts_checks_every_value_shape() {
        let r#type = VariableType::List {
            element: Box::new(VariableType::UnsignedInteger { min: 0, max: 10 }),
            max_len: 3,
        };
        assert!(r#type.accepts(&GraphValue::List(vec![
            GraphValue::UnsignedInteger(1),
            GraphValue::UnsignedInteger(10),
        ])));
        assert!(!r#type.accepts(&GraphValue::List(vec![GraphValue::UnsignedInteger(11)])));
        assert!(!r#type.accepts(&GraphValue::List(vec![
            GraphValue::UnsignedInteger(1),
            GraphValue::UnsignedInteger(2),
            GraphValue::UnsignedInteger(3),
            GraphValue::UnsignedInteger(4)
        ])));
        assert!(VariableType::Optional(Box::new(VariableType::Boolean)).accepts(&GraphValue::Null));
        assert!(VariableType::Secret.accepts(&GraphValue::SecretReference(
            SecretReference::new("secret:0001".into()).expect("valid")
        )));
        assert!(!VariableType::Secret.accepts(&GraphValue::String("x".into())));
    }
}
