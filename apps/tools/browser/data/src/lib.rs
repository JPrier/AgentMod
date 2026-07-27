//! Browser datasets and dependency normalization.

use agentmod_browser_host_dependency::{
    BrowserDependencyError, BrowserDependencyPort, DependencyAuthorization,
    DependencyBrowserAction, DependencyBrowserRequest,
};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Data-owned authorization.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowserDataAuthorization {
    /// Tool call.
    pub call_id: String,
    /// Exact action.
    pub action: String,
    /// Canonical digest.
    pub normalized_digest: String,
    /// Signed grant.
    pub grant: String,
    /// Expanded arguments.
    pub arguments: Value,
    /// Cancellation.
    pub cancellation_id: String,
}

/// Data-owned operation.
#[derive(Clone, Debug, PartialEq)]
pub enum BrowserDataAction {
    /// Start.
    Start,
    /// Navigate.
    Navigate {
        /// Destination.
        url: String,
    },
    /// Inspect.
    Inspect {
        /// Inline bound.
        maximum_bytes: usize,
    },
    /// Screenshot.
    Screenshot,
    /// Click.
    Click {
        /// CSS selector.
        selector: String,
    },
    /// Type.
    Type {
        /// CSS selector.
        selector: String,
        /// Replacement text.
        text: String,
    },
    /// Submit.
    Submit {
        /// CSS selector.
        selector: String,
    },
    /// Download.
    Download {
        /// Download URL.
        url: String,
        /// Byte bound.
        maximum_bytes: usize,
    },
    /// Close.
    Close,
}

/// Data request.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowserDataRequest {
    /// Authorization.
    pub authorization: BrowserDataAuthorization,
    /// Operation.
    pub action: BrowserDataAction,
}

/// Stable data record.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowserDataRecord {
    /// Bounded output.
    pub result: Value,
    /// Artifact.
    pub artifact: Option<String>,
    /// Projection flag.
    pub truncated: bool,
}

/// Data port consumed by logic.
#[async_trait]
pub trait BrowserDataPort: Send + Sync {
    /// Executes one business-facing data operation.
    async fn execute(
        &self,
        request: BrowserDataRequest,
    ) -> Result<BrowserDataRecord, BrowserDataError>;
    /// Cancels.
    async fn cancel(&self, cancellation_id: &str) -> Result<(), BrowserDataError>;
    /// Health dataset.
    async fn health(&self) -> Result<Value, BrowserDataError>;
    /// Shutdown.
    async fn shutdown(&self);
}

/// Data implementation.
#[derive(Clone)]
pub struct BrowserData<D> {
    dependency: D,
}

impl<D> BrowserData<D> {
    /// Injects dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

#[async_trait]
impl<D: BrowserDependencyPort> BrowserDataPort for BrowserData<D> {
    async fn execute(
        &self,
        request: BrowserDataRequest,
    ) -> Result<BrowserDataRecord, BrowserDataError> {
        let value = self
            .dependency
            .execute(DependencyBrowserRequest {
                authorization: map_authorization(request.authorization),
                action: match request.action {
                    BrowserDataAction::Start => DependencyBrowserAction::Start,
                    BrowserDataAction::Navigate { url } => {
                        DependencyBrowserAction::Navigate { url }
                    }
                    BrowserDataAction::Inspect { maximum_bytes } => {
                        DependencyBrowserAction::Inspect { maximum_bytes }
                    }
                    BrowserDataAction::Screenshot => DependencyBrowserAction::Screenshot,
                    BrowserDataAction::Click { selector } => {
                        DependencyBrowserAction::Click { selector }
                    }
                    BrowserDataAction::Type { selector, text } => {
                        DependencyBrowserAction::Type { selector, text }
                    }
                    BrowserDataAction::Submit { selector } => {
                        DependencyBrowserAction::Submit { selector }
                    }
                    BrowserDataAction::Download { url, maximum_bytes } => {
                        DependencyBrowserAction::Download { url, maximum_bytes }
                    }
                    BrowserDataAction::Close => DependencyBrowserAction::Close,
                },
            })
            .await
            .map_err(map_error)?;
        Ok(BrowserDataRecord {
            result: value.result,
            artifact: value.artifact,
            truncated: value.truncated,
        })
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<(), BrowserDataError> {
        self.dependency
            .cancel(cancellation_id)
            .await
            .map_err(map_error)
    }

    async fn health(&self) -> Result<Value, BrowserDataError> {
        self.dependency.health().await.map_err(map_error)
    }

    async fn shutdown(&self) {
        self.dependency.shutdown().await;
    }
}

fn map_authorization(value: BrowserDataAuthorization) -> DependencyAuthorization {
    DependencyAuthorization {
        call_id: value.call_id,
        action: value.action,
        normalized_digest: value.normalized_digest,
        grant: value.grant,
        arguments: value.arguments,
        cancellation_id: value.cancellation_id,
    }
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "dependency errors are deliberately consumed at the data boundary"
)]
const fn map_error(error: BrowserDependencyError) -> BrowserDataError {
    match error {
        BrowserDependencyError::InvalidRequest | BrowserDependencyError::Configuration => {
            BrowserDataError::Invalid
        }
        BrowserDependencyError::Authorization
        | BrowserDependencyError::AuthorizationReplay
        | BrowserDependencyError::NetworkPolicy => BrowserDataError::Denied,
        BrowserDependencyError::Cancelled => BrowserDataError::Cancelled,
        BrowserDependencyError::NoSession => BrowserDataError::NoSession,
        BrowserDependencyError::TooLarge => BrowserDataError::TooLarge,
        BrowserDependencyError::Transport
        | BrowserDependencyError::Remote
        | BrowserDependencyError::Protocol
        | BrowserDependencyError::Artifact
        | BrowserDependencyError::DuplicateCancellation
        | BrowserDependencyError::UnknownCancellation => BrowserDataError::Unavailable,
    }
}

/// Data-layer failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BrowserDataError {
    /// Invalid.
    #[error("invalid browser data request")]
    Invalid,
    /// Policy or authorization denial.
    #[error("browser data request denied")]
    Denied,
    /// No active session.
    #[error("browser session is not active")]
    NoSession,
    /// Too large.
    #[error("browser data exceeded its bound")]
    TooLarge,
    /// Cancelled.
    #[error("browser data request cancelled")]
    Cancelled,
    /// Dependency unavailable.
    #[error("browser data unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_browser_host_dependency::{
        BrowserDependencyError, BrowserDependencyPort, DependencyBrowserRequest,
        DependencyBrowserResponse,
    };
    use async_trait::async_trait;
    use serde_json::{Value, json};

    use super::{
        BrowserData, BrowserDataAction, BrowserDataAuthorization, BrowserDataPort,
        BrowserDataRequest,
    };

    #[derive(Clone)]
    struct MockDependency;

    #[async_trait]
    impl BrowserDependencyPort for MockDependency {
        async fn execute(
            &self,
            request: DependencyBrowserRequest,
        ) -> Result<DependencyBrowserResponse, BrowserDependencyError> {
            assert_eq!(request.authorization.action, "browser.inspect");
            match request.action {
                agentmod_browser_host_dependency::DependencyBrowserAction::Inspect {
                    maximum_bytes,
                } => assert_eq!(maximum_bytes, 42),
                other => panic!("unexpected mapping: {other:?}"),
            }
            Ok(DependencyBrowserResponse {
                result: json!({"html":"rendered"}),
                artifact: None,
                truncated: true,
            })
        }

        async fn cancel(&self, _: &str) -> Result<(), BrowserDependencyError> {
            Ok(())
        }

        async fn health(&self) -> Result<Value, BrowserDependencyError> {
            Ok(json!({"healthy":true}))
        }

        async fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn data_maps_owned_records_across_the_dependency_boundary() {
        let value = BrowserData::new(MockDependency)
            .execute(BrowserDataRequest {
                authorization: BrowserDataAuthorization {
                    call_id: "call".to_owned(),
                    action: "browser.inspect".to_owned(),
                    normalized_digest: "11".repeat(32),
                    grant: "grant".to_owned(),
                    arguments: json!({"maximum_bytes":42}),
                    cancellation_id: "cancel".to_owned(),
                },
                action: BrowserDataAction::Inspect { maximum_bytes: 42 },
            })
            .await
            .expect("record");
        assert_eq!(value.result["html"], "rendered");
        assert!(value.truncated);
    }
}
