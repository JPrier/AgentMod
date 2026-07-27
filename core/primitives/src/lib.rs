//! Universally stable identifiers and scalar primitives.
//!
//! This crate intentionally contains no business requests, records, provider types,
//! tool types, persistence models, or layer errors.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

macro_rules! opaque_uuid {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Wraps a UUID produced by an injected dependency.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the portable UUID value.
            #[must_use]
            pub const fn into_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

opaque_uuid!(EventId, "Opaque canonical event identifier.");
opaque_uuid!(SessionId, "Opaque canonical session identifier.");
opaque_uuid!(ArtifactId, "Opaque immutable artifact identifier.");
opaque_uuid!(CorrelationId, "Opaque correlation identifier.");
opaque_uuid!(CausationId, "Opaque causation identifier.");
opaque_uuid!(ContinuationId, "Opaque resumable-continuation identifier.");
opaque_uuid!(
    CancellationId,
    "Opaque cross-protocol cancellation identifier."
);
opaque_uuid!(RequestId, "Opaque cross-protocol request identifier.");
opaque_uuid!(IdempotencyId, "Opaque idempotency identifier.");

/// A strictly positive event sequence number.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Sequence(u64);

impl Sequence {
    /// First valid sequence in a journal.
    pub const FIRST: Self = Self(1);

    /// Validates a raw sequence.
    ///
    /// # Errors
    ///
    /// Returns [`PrimitiveError::ZeroSequence`] when `value` is zero.
    pub fn new(value: u64) -> Result<Self, PrimitiveError> {
        if value == 0 {
            Err(PrimitiveError::ZeroSequence)
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the raw sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the following sequence, or an overflow error.
    ///
    /// # Errors
    ///
    /// Returns [`PrimitiveError::SequenceOverflow`] at `u64::MAX`.
    pub fn checked_next(self) -> Result<Self, PrimitiveError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(PrimitiveError::SequenceOverflow)
    }
}

/// Portable Unix timestamp in milliseconds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct TimestampMillis(i64);

impl TimestampMillis {
    /// Wraps a timestamp supplied by an injected clock dependency.
    #[must_use]
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns milliseconds since the Unix epoch.
    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Number of bytes, kept distinct from arbitrary integers at API boundaries.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct ByteCount(u64);

impl ByteCount {
    /// Creates a byte count.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw count.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Semantic protocol or schema version.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    /// Breaking version component.
    pub major: u16,
    /// Additive version component.
    pub minor: u16,
}

impl Version {
    /// Creates a version.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Returns whether the major versions can communicate.
    #[must_use]
    pub const fn is_compatible_with(self, other: Self) -> bool {
        self.major == other.major
    }
}

/// A BLAKE3 content hash.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentHash([u8; 32]);

impl ContentHash {
    /// Hashes immutable content.
    #[must_use]
    pub fn digest(content: &[u8]) -> Self {
        Self(*blake3::hash(content).as_bytes())
    }

    /// Builds a hash from its exact bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact hash bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns a lowercase hexadecimal representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        blake3::Hash::from_bytes(self.0).to_hex().to_string()
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ContentHash")
            .field(&self.to_hex())
            .finish()
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl FromStr for ContentHash {
    type Err = PrimitiveError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = blake3::Hash::from_hex(value)
            .map_err(|_| PrimitiveError::InvalidContentHash(value.to_owned()))?;
        Ok(Self(*parsed.as_bytes()))
    }
}

impl Serialize for ContentHash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

/// Primitive validation failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum PrimitiveError {
    /// Sequence zero is reserved as an invalid/unset sentinel.
    #[error("event sequence must be greater than zero")]
    ZeroSequence,
    /// No sequence exists after `u64::MAX`.
    #[error("event sequence overflow")]
    SequenceOverflow,
    /// A content hash was not exactly valid BLAKE3 hexadecimal.
    #[error("invalid BLAKE3 content hash: {0}")]
    InvalidContentHash(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_hash_json_round_trip_is_hex() {
        let hash = ContentHash::digest(b"agentmod");
        let json = serde_json::to_string(&hash).expect("hash serializes");
        assert_eq!(json.len(), 66);
        assert_eq!(
            serde_json::from_str::<ContentHash>(&json).expect("hash parses"),
            hash
        );
    }

    #[test]
    fn sequence_is_positive_and_checked() {
        assert_eq!(Sequence::new(0), Err(PrimitiveError::ZeroSequence));
        assert_eq!(Sequence::FIRST.checked_next().expect("next").get(), 2);
        assert_eq!(
            Sequence::new(u64::MAX).expect("valid max").checked_next(),
            Err(PrimitiveError::SequenceOverflow)
        );
    }

    #[test]
    fn version_compatibility_uses_major_component() {
        assert!(Version::new(1, 0).is_compatible_with(Version::new(1, 9)));
        assert!(!Version::new(1, 9).is_compatible_with(Version::new(2, 0)));
    }
}
