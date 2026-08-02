//! Data-owned durable isolated-plugin terminal receipt boundary.

use std::sync::Arc;

use agentmod_primitives::SessionId;
use agentmod_runtime_dependency::plugin_receipt::{
    DependencyPluginNodeReceiptIdentity, DependencyStorePluginNodeReceiptRequest,
    PluginNodeReceiptDependencyError, PluginNodeReceiptDependencyPort,
};
use thiserror::Error;

/// Data-owned exact receipt identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeReceiptDataIdentity {
    /// Canonical session.
    pub session_id: SessionId,
    /// Digest-backed invocation identity.
    pub invocation_id: String,
}

/// Data-owned exact identity for any isolated plugin invocation.
///
/// This alias keeps the original plugin-node API source-compatible while
/// allowing other plugin operations to share the same durable, tamper-evident
/// receipt store.
pub type PluginInvocationReceiptDataIdentity = PluginNodeReceiptDataIdentity;

/// Data-owned request to store one terminal receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorePluginNodeReceiptDataRequest {
    /// Exact scoped identity.
    pub identity: PluginNodeReceiptDataIdentity,
    /// Complete logic-owned receipt representation.
    pub receipt_json: String,
}

/// Data-owned request to store one isolated-plugin terminal receipt.
pub type StorePluginInvocationReceiptDataRequest = StorePluginNodeReceiptDataRequest;

/// Data-owned verified terminal receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginNodeReceiptDataRecord {
    /// Exact scoped identity.
    pub identity: PluginNodeReceiptDataIdentity,
    /// Complete verified logic-owned receipt representation.
    pub receipt_json: String,
}

/// Data-owned verified terminal receipt for any isolated plugin invocation.
pub type PluginInvocationReceiptDataRecord = PluginNodeReceiptDataRecord;

/// Narrow data port consumed by runtime plugin coordination.
pub trait PluginNodeReceiptDataPort: Send + Sync {
    /// Loads one exact verified receipt.
    ///
    /// # Errors
    ///
    /// Returns a data-owned failure for invalid identity, corruption, or
    /// unavailable storage.
    fn load_plugin_node_receipt(
        &self,
        identity: PluginNodeReceiptDataIdentity,
    ) -> Result<Option<PluginNodeReceiptDataRecord>, PluginNodeReceiptDataError>;

    /// Stores one exact receipt atomically and idempotently.
    ///
    /// # Errors
    ///
    /// Returns a data-owned failure for invalid, conflicting, corrupt,
    /// oversized, or unavailable storage.
    fn store_plugin_node_receipt(
        &self,
        request: StorePluginNodeReceiptDataRequest,
    ) -> Result<PluginNodeReceiptDataRecord, PluginNodeReceiptDataError>;

    /// Loads one exact terminal receipt for an isolated plugin operation.
    ///
    /// # Errors
    ///
    /// Returns the same classified data failure as the compatibility
    /// plugin-node method.
    fn load_plugin_invocation_receipt(
        &self,
        identity: PluginInvocationReceiptDataIdentity,
    ) -> Result<Option<PluginInvocationReceiptDataRecord>, PluginNodeReceiptDataError> {
        self.load_plugin_node_receipt(identity)
    }

    /// Stores one exact isolated-plugin terminal receipt atomically and
    /// idempotently.
    ///
    /// # Errors
    ///
    /// Returns the same classified data failure as the compatibility
    /// plugin-node method.
    fn store_plugin_invocation_receipt(
        &self,
        request: StorePluginInvocationReceiptDataRequest,
    ) -> Result<PluginInvocationReceiptDataRecord, PluginNodeReceiptDataError> {
        self.store_plugin_node_receipt(request)
    }
}

/// Explicit data router over an injected receipt dependency.
#[derive(Clone)]
pub struct RuntimePluginNodeReceiptData {
    dependency: Arc<dyn PluginNodeReceiptDependencyPort>,
}

impl std::fmt::Debug for RuntimePluginNodeReceiptData {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimePluginNodeReceiptData")
            .finish_non_exhaustive()
    }
}

impl RuntimePluginNodeReceiptData {
    /// Creates the data router.
    #[must_use]
    pub fn new(dependency: Arc<dyn PluginNodeReceiptDependencyPort>) -> Self {
        Self { dependency }
    }
}

impl PluginNodeReceiptDataPort for RuntimePluginNodeReceiptData {
    fn load_plugin_node_receipt(
        &self,
        identity: PluginNodeReceiptDataIdentity,
    ) -> Result<Option<PluginNodeReceiptDataRecord>, PluginNodeReceiptDataError> {
        let dependency_identity = to_dependency_identity(&identity);
        self.dependency
            .load_plugin_node_receipt(&dependency_identity)
            .map_err(map_dependency_error)?
            .map(|record| {
                String::from_utf8(record.receipt_bytes)
                    .map(|receipt_json| PluginNodeReceiptDataRecord {
                        identity,
                        receipt_json,
                    })
                    .map_err(|_| PluginNodeReceiptDataError::Corrupt)
            })
            .transpose()
    }

    fn store_plugin_node_receipt(
        &self,
        request: StorePluginNodeReceiptDataRequest,
    ) -> Result<PluginNodeReceiptDataRecord, PluginNodeReceiptDataError> {
        let identity = request.identity;
        let record = self
            .dependency
            .store_plugin_node_receipt(DependencyStorePluginNodeReceiptRequest {
                identity: to_dependency_identity(&identity),
                receipt_bytes: request.receipt_json.into_bytes(),
            })
            .map_err(map_dependency_error)?;
        let receipt_json = String::from_utf8(record.receipt_bytes)
            .map_err(|_| PluginNodeReceiptDataError::Corrupt)?;
        Ok(PluginNodeReceiptDataRecord {
            identity,
            receipt_json,
        })
    }
}

fn to_dependency_identity(
    identity: &PluginNodeReceiptDataIdentity,
) -> DependencyPluginNodeReceiptIdentity {
    DependencyPluginNodeReceiptIdentity {
        session_id: identity.session_id.to_string(),
        invocation_id: identity.invocation_id.clone(),
    }
}

fn map_dependency_error(error: PluginNodeReceiptDependencyError) -> PluginNodeReceiptDataError {
    match error {
        PluginNodeReceiptDependencyError::InvalidRequest => PluginNodeReceiptDataError::Invalid,
        PluginNodeReceiptDependencyError::TooLarge => PluginNodeReceiptDataError::TooLarge,
        PluginNodeReceiptDependencyError::Storage => PluginNodeReceiptDataError::Unavailable,
        PluginNodeReceiptDependencyError::Corrupt => PluginNodeReceiptDataError::Corrupt,
        PluginNodeReceiptDependencyError::Conflict => PluginNodeReceiptDataError::Conflict,
    }
}

/// Stable data-owned receipt failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PluginNodeReceiptDataError {
    /// Identity or request was invalid.
    #[error("plugin-node receipt data request is invalid")]
    Invalid,
    /// Receipt exceeded its bound.
    #[error("plugin-node receipt data exceeds its byte bound")]
    TooLarge,
    /// Receipt storage was unavailable.
    #[error("plugin-node receipt data is unavailable")]
    Unavailable,
    /// Stored receipt was corrupt.
    #[error("plugin-node receipt data is corrupt")]
    Corrupt,
    /// Existing exact identity contains different receipt content.
    #[error("plugin-node receipt data conflicts with existing content")]
    Conflict,
}
