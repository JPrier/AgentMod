//! Bounded canonical graph values.
//!
//! Values are fully owned, deterministic, and replay-safe. Container ordering
//! is canonical (`BTreeMap` for objects, declared order for lists), so hashing
//! and serialization never depend on incidental map iteration order.

use std::collections::BTreeMap;

use agentmod_event_model::ArtifactReference;
use agentmod_primitives::{ContinuationId, SessionId, TimestampMillis};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum fixed-point scale for [`Decimal`] values.
pub const MAX_DECIMAL_SCALE: u8 = 12;

/// Bounded fixed-point decimal value.
///
/// `unscaled * 10^-scale`; scale is at most [`MAX_DECIMAL_SCALE`]. Fixed point
/// keeps ordering and hashing fully deterministic without floating point.
/// Equality and ordering are value-based: `10` at scale `1` equals `100` at
/// scale `2`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct Decimal {
    /// Scaled integer value.
    pub unscaled: i64,
    /// Decimal scale.
    pub scale: u8,
}

impl Decimal {
    /// Creates a decimal value, rejecting an out-of-range scale.
    ///
    /// # Errors
    ///
    /// Returns [`GraphValueError::InvalidDecimalScale`] when the scale exceeds
    /// [`MAX_DECIMAL_SCALE`].
    pub const fn new(unscaled: i64, scale: u8) -> Result<Self, GraphValueError> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(GraphValueError::InvalidDecimalScale {
                actual: scale,
                maximum: MAX_DECIMAL_SCALE,
            });
        }
        Ok(Self { unscaled, scale })
    }

    /// Validates that the scale is within the canonical bound.
    ///
    /// # Errors
    ///
    /// Returns [`GraphValueError::InvalidDecimalScale`] for an out-of-range
    /// scale; used by the reducer on deserialized values.
    pub const fn validate(self) -> Result<(), GraphValueError> {
        if self.scale > MAX_DECIMAL_SCALE {
            Err(GraphValueError::InvalidDecimalScale {
                actual: self.scale,
                maximum: MAX_DECIMAL_SCALE,
            })
        } else {
            Ok(())
        }
    }

    /// Returns the value scaled to `scale` as `i128`, or an error when the
    /// exact representation cannot be materialized.
    ///
    /// # Errors
    ///
    /// Returns [`GraphValueError::DecimalOverflow`] when the scaled magnitude
    /// exceeds `i128` or `scale` exceeds [`MAX_DECIMAL_SCALE`].
    pub fn scaled_to(self, scale: u8) -> Result<i128, GraphValueError> {
        if scale > MAX_DECIMAL_SCALE || self.scale > MAX_DECIMAL_SCALE {
            return Err(GraphValueError::InvalidDecimalScale {
                actual: scale.max(self.scale),
                maximum: MAX_DECIMAL_SCALE,
            });
        }
        let Some(power) = 10i128.checked_pow(u32::from(scale - self.scale.min(scale))) else {
            return Err(GraphValueError::DecimalOverflow);
        };
        if self.scale <= scale {
            match i128::from(self.unscaled).checked_mul(power) {
                Some(scaled) => Ok(scaled),
                None => Err(GraphValueError::DecimalOverflow),
            }
        } else {
            // Truncating division is exact: the source scale is larger.
            Ok(i128::from(self.unscaled) / power)
        }
    }

    /// Compares two decimals exactly.
    #[must_use]
    pub fn compare(self, other: Self) -> std::cmp::Ordering {
        let scale = self.scale.max(other.scale);
        match (self.scaled_to(scale), other.scaled_to(scale)) {
            (Ok(left), Ok(right)) => left.cmp(&right),
            // Unreachable for validated values; keep total determinism.
            _ => self.unscaled.cmp(&other.unscaled),
        }
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.compare(*other)
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Decimal {}

impl std::hash::Hash for Decimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash the value normalized to the canonical maximum scale so equal
        // values always produce equal hashes.
        match self.scaled_to(MAX_DECIMAL_SCALE) {
            Ok(normalized) => state.write_i128(normalized),
            // Unreachable for validated values; keep total determinism.
            Err(_) => state.write_i64(self.unscaled),
        }
    }
}

/// Canonical approval decision outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalDecision {
    /// The consequential action was approved.
    Approved,
    /// The consequential action was rejected.
    Rejected,
    /// The approval was cancelled before resolution.
    Cancelled,
}

/// Opaque reference to an approved secret.
///
/// Secret values are never represented as plaintext in canonical graph state;
/// declarations classified [`SecurityClassification::Secret`] only accept this
/// reference type.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SecretReference(String);

impl SecretReference {
    /// Maximum UTF-8 bytes in one secret reference.
    pub const MAX_BYTES: usize = 1024;

    /// Wraps a validated secret reference.
    ///
    /// # Errors
    ///
    /// Returns [`GraphValueError::SecretReferenceTooLong`] when the reference
    /// exceeds [`Self::MAX_BYTES`] or is empty.
    pub fn new(value: String) -> Result<Self, GraphValueError> {
        if value.is_empty() || value.len() > Self::MAX_BYTES {
            return Err(GraphValueError::SecretReferenceTooLong {
                actual: value.len(),
                maximum: Self::MAX_BYTES,
            });
        }
        Ok(Self(value))
    }

    /// Returns the opaque reference bytes.
    #[must_use]
    pub const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

/// Typed canonical graph value.
///
/// Large values are represented as immutable [`GraphValue::ArtifactReference`]
/// entries; secret values are represented by [`GraphValue::SecretReference`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum GraphValue {
    /// Canonical null; only valid where the declaration is optional.
    Null,
    /// Boolean.
    Boolean(bool),
    /// Signed integer within declared bounds.
    SignedInteger(i64),
    /// Unsigned integer within declared bounds.
    UnsignedInteger(u64),
    /// Bounded fixed-point decimal.
    Decimal(Decimal),
    /// UTF-8 string within the declared byte bound.
    String(String),
    /// Tag from the declared closed tag set.
    EnumTag(String),
    /// Ordered list of typed elements.
    List(Vec<GraphValue>),
    /// Object with string keys and a declared value type.
    Map(BTreeMap<String, GraphValue>),
    /// Opaque session identifier.
    SessionId(SessionId),
    /// Opaque child-session identifier (children are sessions).
    ChildSessionId(SessionId),
    /// Runtime-owned task identity.
    TaskId(String),
    /// Compiled graph node identity.
    NodeId(String),
    /// Opaque continuation identity.
    ContinuationId(ContinuationId),
    /// Reference to an immutable artifact.
    ArtifactReference(ArtifactReference),
    /// Reference to a completed tool result.
    ToolResultReference(String),
    /// Reference to a joined child result.
    ChildResultReference(String),
    /// Approval decision outcome.
    ApprovalDecision(ApprovalDecision),
    /// Approved secret reference; never plaintext.
    SecretReference(SecretReference),
    /// Canonical wall-clock timestamp.
    Timestamp(TimestampMillis),
    /// Canonical duration in milliseconds.
    DurationMillis(u64),
}

impl GraphValue {
    /// Returns the deterministic canonical JSON byte length of this value.
    #[must_use]
    pub fn serialized_bytes(&self) -> usize {
        canonical_value_bytes(self).len()
    }

    /// Returns a stable human-oriented type label.
    #[must_use]
    pub const fn type_label(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Boolean(_) => "boolean",
            Self::SignedInteger(_) => "signed_integer",
            Self::UnsignedInteger(_) => "unsigned_integer",
            Self::Decimal(_) => "decimal",
            Self::String(_) => "string",
            Self::EnumTag(_) => "enum_tag",
            Self::List(_) => "list",
            Self::Map(_) => "map",
            Self::SessionId(_) => "session_id",
            Self::ChildSessionId(_) => "child_session_id",
            Self::TaskId(_) => "task_id",
            Self::NodeId(_) => "node_id",
            Self::ContinuationId(_) => "continuation_id",
            Self::ArtifactReference(_) => "artifact_reference",
            Self::ToolResultReference(_) => "tool_result_reference",
            Self::ChildResultReference(_) => "child_result_reference",
            Self::ApprovalDecision(_) => "approval_decision",
            Self::SecretReference(_) => "secret_reference",
            Self::Timestamp(_) => "timestamp",
            Self::DurationMillis(_) => "duration_millis",
        }
    }

    /// Returns whether the value can be referenced as plaintext.
    ///
    /// Secret-declared variables require a [`GraphValue::SecretReference`] and
    /// reject every other representation, so plaintext secrets never enter
    /// canonical state.
    #[must_use]
    pub const fn is_secret_reference(&self) -> bool {
        matches!(self, Self::SecretReference(_))
    }

    /// Returns the artifact reference, when this value is an artifact-backed
    /// large value.
    #[must_use]
    pub const fn artifact_reference(&self) -> Option<&ArtifactReference> {
        match self {
            Self::ArtifactReference(reference) => Some(reference),
            _ => None,
        }
    }

    /// Returns the list elements, when this value is a list.
    #[must_use]
    pub const fn as_list(&self) -> Option<&Vec<Self>> {
        match self {
            Self::List(values) => Some(values),
            _ => None,
        }
    }

    /// Returns the object fields, when this value is a map.
    #[must_use]
    pub const fn as_map(&self) -> Option<&BTreeMap<String, Self>> {
        match self {
            Self::Map(values) => Some(values),
            _ => None,
        }
    }
}

/// Returns the canonical JSON bytes of a value.
///
/// Every container is ordered deterministically (`BTreeMap` keys, declared
/// list order), so these bytes are stable across processes and rebuilds.
///
/// # Panics
///
/// Panics only if the fully owned value cannot be serialized, which is a
/// programming error in the serialized model.
#[must_use]
pub fn canonical_value_bytes(value: &GraphValue) -> Vec<u8> {
    // Serialization of the fully owned value is total for the supported set;
    // a failure would indicate a programming error in the serialized model.
    serde_json::to_vec(value).expect("canonical graph value serializes")
}

/// Canonical graph-value failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphValueError {
    /// Decimal scale exceeds the canonical bound.
    #[error("decimal scale {actual} exceeds the canonical maximum {maximum}")]
    InvalidDecimalScale {
        /// Actual scale.
        actual: u8,
        /// Maximum scale.
        maximum: u8,
    },
    /// Decimal scaling overflows the exact `i128` representation.
    #[error("decimal value cannot be represented exactly at the required scale")]
    DecimalOverflow,
    /// Secret reference is empty or exceeds the byte bound.
    #[error("secret reference is {actual} bytes; valid range is 1..={maximum}")]
    SecretReferenceTooLong {
        /// Actual bytes.
        actual: usize,
        /// Maximum bytes.
        maximum: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_ordering_is_exact_and_scale_aware() {
        let small = Decimal::new(1, 2).expect("valid");
        let large = Decimal::new(99, 2).expect("valid");
        assert!(small < large);
        assert!(Decimal::new(10, 1).expect("valid") == Decimal::new(100, 2).expect("valid"));
        assert!(Decimal::new(-1, 1).expect("valid") < Decimal::new(-1, 2).expect("valid"));
    }

    #[test]
    fn decimal_scale_is_bounded_and_validated() {
        assert_eq!(
            Decimal::new(1, MAX_DECIMAL_SCALE + 1),
            Err(GraphValueError::InvalidDecimalScale {
                actual: MAX_DECIMAL_SCALE + 1,
                maximum: MAX_DECIMAL_SCALE,
            })
        );
        assert_eq!(
            Decimal::new(1, MAX_DECIMAL_SCALE)
                .expect("valid")
                .validate(),
            Ok(())
        );
    }

    #[test]
    fn secret_references_are_opaque_and_bounded() {
        assert_eq!(
            SecretReference::new(String::new()),
            Err(GraphValueError::SecretReferenceTooLong {
                actual: 0,
                maximum: SecretReference::MAX_BYTES,
            })
        );
        let reference = SecretReference::new("secret:0001".into()).expect("valid");
        assert_eq!(reference.as_str(), "secret:0001");
    }

    #[test]
    fn canonical_bytes_are_sorted_and_stable() {
        let mut map = BTreeMap::new();
        map.insert("z".to_owned(), GraphValue::Boolean(true));
        map.insert("a".to_owned(), GraphValue::UnsignedInteger(1));
        let value = GraphValue::Map(map);
        let first = canonical_value_bytes(&value);
        let mut other = BTreeMap::new();
        other.insert("a".to_owned(), GraphValue::UnsignedInteger(1));
        other.insert("z".to_owned(), GraphValue::Boolean(true));
        assert_eq!(first, canonical_value_bytes(&GraphValue::Map(other)));
        assert!(GraphValue::Map(BTreeMap::new()).serialized_bytes() > 0);
    }

    #[test]
    fn artifact_and_secret_values_report_their_shape() {
        let artifact = GraphValue::ArtifactReference(ArtifactReference {
            id: agentmod_primitives::ArtifactId::from_uuid(uuid::Uuid::nil()),
            content_hash: agentmod_primitives::ContentHash::digest(b"x"),
        });
        assert!(artifact.artifact_reference().is_some());
        assert!(!artifact.is_secret_reference());
        let secret =
            GraphValue::SecretReference(SecretReference::new("secret:0001".into()).expect("valid"));
        assert!(secret.is_secret_reference());
        assert!(!GraphValue::Null.is_secret_reference());
    }
}
