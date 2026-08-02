//! Service mapping for bounded immutable artifact range access.

use std::{path::PathBuf, str::FromStr};

use agentmod_primitives::ContentHash;
use agentmod_runtime_logic::artifact::{
    ArtifactReadError, ArtifactReadLogicPort, ReadArtifactRangeCommand,
};
use thiserror::Error;

/// Service-owned artifact range request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceReadArtifactRangeRequest {
    /// Session-scoped artifact store root selected by composition.
    pub store_root: PathBuf,
    /// Exact immutable portable reference.
    pub artifact_reference: String,
    /// Lowercase canonical full-object digest.
    pub expected_content_hash: String,
    /// Canonically expected full-object size.
    pub expected_byte_size: u64,
    /// Zero-based byte offset.
    pub offset: u64,
    /// Exact bounded byte count.
    pub length: u64,
}

/// Service-owned artifact range response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceReadArtifactRangeResponse {
    /// Exact requested bytes.
    pub bytes: Vec<u8>,
    /// Verified full-object byte size.
    pub artifact_bytes: u64,
    /// Verified lowercase full-object digest.
    pub content_hash: String,
}

/// Runtime artifact endpoint service over an injected logic boundary.
#[derive(Clone, Debug)]
pub struct ArtifactService<L> {
    logic: L,
}

impl<L> ArtifactService<L> {
    /// Creates an artifact service over injected logic.
    #[must_use]
    pub const fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L: ArtifactReadLogicPort> ArtifactService<L> {
    /// Reads one exact bounded range through the runtime business boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactServiceError`] for invalid service input or a
    /// fail-closed logic result.
    pub fn read_range(
        &self,
        request: ServiceReadArtifactRangeRequest,
    ) -> Result<ServiceReadArtifactRangeResponse, ArtifactServiceError> {
        let expected_content_hash = ContentHash::from_str(&request.expected_content_hash)
            .map_err(|_| ArtifactServiceError::Invalid)?;
        let result = self
            .logic
            .read_artifact_range(ReadArtifactRangeCommand {
                store_root: request.store_root,
                artifact_reference: request.artifact_reference,
                expected_content_hash,
                expected_byte_size: request.expected_byte_size,
                offset: request.offset,
                length: request.length,
            })
            .map_err(ArtifactServiceError::Logic)?;
        Ok(ServiceReadArtifactRangeResponse {
            bytes: result.bytes,
            artifact_bytes: result.artifact_bytes,
            content_hash: result.content_hash.to_hex(),
        })
    }
}

/// Artifact service failure.
#[derive(Debug, Error)]
pub enum ArtifactServiceError {
    /// Service input is malformed.
    #[error("artifact service request is invalid")]
    Invalid,
    /// Runtime business validation or storage failed.
    #[error("artifact service operation failed: {0}")]
    Logic(#[source] ArtifactReadError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use agentmod_runtime_logic::artifact::ReadArtifactRangeResult;

    use super::*;

    #[derive(Default)]
    struct MockArtifactLogic {
        command: RefCell<Option<ReadArtifactRangeCommand>>,
    }

    impl ArtifactReadLogicPort for MockArtifactLogic {
        fn read_artifact_range(
            &self,
            command: ReadArtifactRangeCommand,
        ) -> Result<ReadArtifactRangeResult, ArtifactReadError> {
            let hash = command.expected_content_hash;
            self.command.replace(Some(command));
            Ok(ReadArtifactRangeResult {
                bytes: b"range".to_vec(),
                artifact_bytes: 11,
                content_hash: hash,
            })
        }
    }

    #[test]
    fn maps_service_request_into_logic_owned_exact_range_contract() {
        let logic = MockArtifactLogic::default();
        let service = ArtifactService::new(logic);
        let hash = ContentHash::digest(b"hello world");
        let response = service
            .read_range(ServiceReadArtifactRangeRequest {
                store_root: PathBuf::from("session/artifacts"),
                artifact_reference: format!("artifact:blake3:{hash}"),
                expected_content_hash: hash.to_hex(),
                expected_byte_size: 11,
                offset: 2,
                length: 5,
            })
            .expect("service range");

        assert_eq!(response.bytes, b"range");
        assert_eq!(response.artifact_bytes, 11);
        assert_eq!(response.content_hash, hash.to_hex());
        assert_eq!(
            service.logic.command.borrow().as_ref(),
            Some(&ReadArtifactRangeCommand {
                store_root: PathBuf::from("session/artifacts"),
                artifact_reference: format!("artifact:blake3:{hash}"),
                expected_content_hash: hash,
                expected_byte_size: 11,
                offset: 2,
                length: 5,
            })
        );
    }

    #[test]
    fn rejects_malformed_hash_before_calling_logic() {
        let service = ArtifactService::new(MockArtifactLogic::default());
        assert!(matches!(
            service.read_range(ServiceReadArtifactRangeRequest {
                store_root: PathBuf::from("session/artifacts"),
                artifact_reference: String::from("artifact:blake3:not-a-hash"),
                expected_content_hash: String::from("not-a-hash"),
                expected_byte_size: 1,
                offset: 0,
                length: 1,
            }),
            Err(ArtifactServiceError::Invalid)
        ));
        assert!(service.logic.command.borrow().is_none());
    }
}
