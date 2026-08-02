//! Data-owned durable provider-completion receipt boundary.
#![allow(
    missing_docs,
    reason = "data-local provider receipt records are intentionally boundary-specific"
)]

use std::sync::Arc;

use agentmod_primitives::SessionId;
use agentmod_runtime_dependency::provider_completion_receipt::{
    DependencyProviderCompletionReceiptIdentity, DependencyStoreProviderCompletionReceiptRequest,
    ProviderCompletionReceiptDependencyError, ProviderCompletionReceiptDependencyPort,
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCompletionReceiptDataIdentity {
    pub session_id: SessionId,
    pub invocation_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreProviderCompletionReceiptDataRequest {
    pub identity: ProviderCompletionReceiptDataIdentity,
    pub receipt_json: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCompletionReceiptDataRecord {
    pub identity: ProviderCompletionReceiptDataIdentity,
    pub receipt_json: String,
}

pub trait ProviderCompletionReceiptDataPort: Send + Sync {
    /// Loads one exact verified provider-completion receipt when present.
    ///
    /// # Errors
    ///
    /// Returns a classified data error for invalid identity, corruption, or
    /// dependency unavailability.
    fn load_provider_completion_receipt(
        &self,
        identity: ProviderCompletionReceiptDataIdentity,
    ) -> Result<Option<ProviderCompletionReceiptDataRecord>, ProviderCompletionReceiptDataError>;

    /// Durably stores one exact provider-completion receipt.
    ///
    /// Exact duplicates are idempotent and substitutions conflict.
    ///
    /// # Errors
    ///
    /// Returns a classified data error for invalid identity, oversized or
    /// corrupt content, conflict, or dependency unavailability.
    fn store_provider_completion_receipt(
        &self,
        request: StoreProviderCompletionReceiptDataRequest,
    ) -> Result<ProviderCompletionReceiptDataRecord, ProviderCompletionReceiptDataError>;
}

#[derive(Clone)]
pub struct RuntimeProviderCompletionReceiptData {
    dependency: Arc<dyn ProviderCompletionReceiptDependencyPort>,
}

impl std::fmt::Debug for RuntimeProviderCompletionReceiptData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeProviderCompletionReceiptData")
            .finish_non_exhaustive()
    }
}

impl RuntimeProviderCompletionReceiptData {
    #[must_use]
    pub fn new(dependency: Arc<dyn ProviderCompletionReceiptDependencyPort>) -> Self {
        Self { dependency }
    }
}

impl ProviderCompletionReceiptDataPort for RuntimeProviderCompletionReceiptData {
    fn load_provider_completion_receipt(
        &self,
        identity: ProviderCompletionReceiptDataIdentity,
    ) -> Result<Option<ProviderCompletionReceiptDataRecord>, ProviderCompletionReceiptDataError>
    {
        self.dependency
            .load_provider_completion_receipt(&to_dependency_identity(&identity))
            .map_err(map_error)?
            .map(|record| {
                String::from_utf8(record.receipt_bytes)
                    .map(|receipt_json| ProviderCompletionReceiptDataRecord {
                        identity,
                        receipt_json,
                    })
                    .map_err(|_| ProviderCompletionReceiptDataError::Corrupt)
            })
            .transpose()
    }

    fn store_provider_completion_receipt(
        &self,
        request: StoreProviderCompletionReceiptDataRequest,
    ) -> Result<ProviderCompletionReceiptDataRecord, ProviderCompletionReceiptDataError> {
        let identity = request.identity;
        let record = self
            .dependency
            .store_provider_completion_receipt(DependencyStoreProviderCompletionReceiptRequest {
                identity: to_dependency_identity(&identity),
                receipt_bytes: request.receipt_json.into_bytes(),
            })
            .map_err(map_error)?;
        Ok(ProviderCompletionReceiptDataRecord {
            identity,
            receipt_json: String::from_utf8(record.receipt_bytes)
                .map_err(|_| ProviderCompletionReceiptDataError::Corrupt)?,
        })
    }
}

fn to_dependency_identity(
    identity: &ProviderCompletionReceiptDataIdentity,
) -> DependencyProviderCompletionReceiptIdentity {
    DependencyProviderCompletionReceiptIdentity {
        session_id: identity.session_id.to_string(),
        invocation_id: format!(
            "provider-completion:{}",
            agentmod_primitives::ContentHash::digest(identity.invocation_id.as_bytes()).to_hex()
        ),
    }
}

fn map_error(
    error: ProviderCompletionReceiptDependencyError,
) -> ProviderCompletionReceiptDataError {
    match error {
        ProviderCompletionReceiptDependencyError::InvalidRequest => {
            ProviderCompletionReceiptDataError::Invalid
        }
        ProviderCompletionReceiptDependencyError::TooLarge => {
            ProviderCompletionReceiptDataError::TooLarge
        }
        ProviderCompletionReceiptDependencyError::Storage => {
            ProviderCompletionReceiptDataError::Unavailable
        }
        ProviderCompletionReceiptDependencyError::Corrupt => {
            ProviderCompletionReceiptDataError::Corrupt
        }
        ProviderCompletionReceiptDependencyError::Conflict => {
            ProviderCompletionReceiptDataError::Conflict
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProviderCompletionReceiptDataError {
    #[error("provider receipt data request is invalid")]
    Invalid,
    #[error("provider receipt data exceeds its byte bound")]
    TooLarge,
    #[error("provider receipt data is unavailable")]
    Unavailable,
    #[error("provider receipt data is corrupt")]
    Corrupt,
    #[error("provider receipt data conflicts with existing content")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use agentmod_runtime_dependency::provider_completion_receipt::FileProviderCompletionReceiptDependency;

    use super::*;

    #[test]
    fn data_boundary_maps_exact_identity_and_verified_json() {
        let root = tempfile::tempdir().expect("root");
        let dependency =
            FileProviderCompletionReceiptDependency::new(root.path().to_owned()).expect("store");
        let data = RuntimeProviderCompletionReceiptData::new(Arc::new(dependency));
        let identity = ProviderCompletionReceiptDataIdentity {
            session_id: SessionId::from_str("00000000-0000-0000-0000-000000000001")
                .expect("session"),
            invocation_id: String::from("generic-provider-completion:phase-1"),
        };
        let request = StoreProviderCompletionReceiptDataRequest {
            identity: identity.clone(),
            receipt_json: String::from(r#"{"schema_version":1,"reason":"stop"}"#),
        };
        let stored = data
            .store_provider_completion_receipt(request.clone())
            .expect("store");
        assert_eq!(
            stored,
            ProviderCompletionReceiptDataRecord {
                identity: identity.clone(),
                receipt_json: request.receipt_json,
            }
        );
        assert_eq!(
            data.load_provider_completion_receipt(identity)
                .expect("load"),
            Some(stored)
        );
    }
}
