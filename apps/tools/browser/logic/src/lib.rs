//! Browser lifecycle, validation, and interaction business rules.

use agentmod_browser_host_data::{
    BrowserDataAction, BrowserDataAuthorization, BrowserDataError, BrowserDataPort,
    BrowserDataRequest,
};
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Logic-owned authorization.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowserAuthorization {
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

/// Logic command.
#[derive(Clone, Debug, PartialEq)]
pub enum BrowserCommand {
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

/// Logic request.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowserLogicRequest {
    /// Authorization.
    pub authorization: BrowserAuthorization,
    /// Command.
    pub command: BrowserCommand,
}

/// Logic result.
#[derive(Clone, Debug, PartialEq)]
pub struct BrowserResult {
    /// Bounded output.
    pub result: Value,
    /// Artifact ID.
    pub artifact: Option<String>,
    /// Projection flag.
    pub truncated: bool,
}

/// Business interface.
#[async_trait]
pub trait BrowserLogicPort: Send + Sync {
    /// Executes.
    async fn execute(
        &self,
        request: BrowserLogicRequest,
    ) -> Result<BrowserResult, BrowserLogicError>;
    /// Cancels.
    async fn cancel(&self, cancellation_id: &str) -> Result<(), BrowserLogicError>;
    /// Health.
    async fn health(&self) -> Result<Value, BrowserLogicError>;
    /// Shutdown.
    async fn shutdown(&self);
}

/// Logic implementation.
#[derive(Clone)]
pub struct BrowserLogic<D> {
    data: D,
    maximum_inline_bytes: usize,
    maximum_artifact_bytes: usize,
}

impl<D> BrowserLogic<D> {
    /// Constructs bounded logic.
    ///
    /// # Errors
    ///
    /// Rejects zero bounds.
    pub fn new(
        data: D,
        maximum_inline_bytes: usize,
        maximum_artifact_bytes: usize,
    ) -> Result<Self, BrowserLogicError> {
        if maximum_inline_bytes == 0 || maximum_artifact_bytes == 0 {
            return Err(BrowserLogicError::Configuration);
        }
        Ok(Self {
            data,
            maximum_inline_bytes,
            maximum_artifact_bytes,
        })
    }
}

#[async_trait]
impl<D: BrowserDataPort> BrowserLogicPort for BrowserLogic<D> {
    async fn execute(
        &self,
        request: BrowserLogicRequest,
    ) -> Result<BrowserResult, BrowserLogicError> {
        validate_authorization(&request.authorization)?;
        validate_command(
            &request.command,
            self.maximum_inline_bytes,
            self.maximum_artifact_bytes,
        )?;
        let value = self
            .data
            .execute(BrowserDataRequest {
                authorization: map_authorization(request.authorization),
                action: match request.command {
                    BrowserCommand::Start => BrowserDataAction::Start,
                    BrowserCommand::Navigate { url } => BrowserDataAction::Navigate { url },
                    BrowserCommand::Inspect { maximum_bytes } => {
                        BrowserDataAction::Inspect { maximum_bytes }
                    }
                    BrowserCommand::Screenshot => BrowserDataAction::Screenshot,
                    BrowserCommand::Click { selector } => BrowserDataAction::Click { selector },
                    BrowserCommand::Type { selector, text } => {
                        BrowserDataAction::Type { selector, text }
                    }
                    BrowserCommand::Submit { selector } => BrowserDataAction::Submit { selector },
                    BrowserCommand::Download { url, maximum_bytes } => {
                        BrowserDataAction::Download { url, maximum_bytes }
                    }
                    BrowserCommand::Close => BrowserDataAction::Close,
                },
            })
            .await
            .map_err(map_error)?;
        Ok(BrowserResult {
            result: value.result,
            artifact: value.artifact,
            truncated: value.truncated,
        })
    }

    async fn cancel(&self, cancellation_id: &str) -> Result<(), BrowserLogicError> {
        if cancellation_id.trim().is_empty() {
            return Err(BrowserLogicError::Invalid);
        }
        self.data.cancel(cancellation_id).await.map_err(map_error)
    }

    async fn health(&self) -> Result<Value, BrowserLogicError> {
        self.data.health().await.map_err(map_error)
    }

    async fn shutdown(&self) {
        self.data.shutdown().await;
    }
}

fn validate_authorization(value: &BrowserAuthorization) -> Result<(), BrowserLogicError> {
    if value.call_id.trim().is_empty()
        || value.action.trim().is_empty()
        || value.normalized_digest.len() != 64
        || value.grant.trim().is_empty()
        || !value.arguments.is_object()
        || value.cancellation_id.trim().is_empty()
    {
        Err(BrowserLogicError::Invalid)
    } else {
        Ok(())
    }
}

fn validate_command(
    command: &BrowserCommand,
    maximum_inline: usize,
    maximum_artifact: usize,
) -> Result<(), BrowserLogicError> {
    match command {
        BrowserCommand::Navigate { url } if url.is_empty() => Err(BrowserLogicError::Invalid),
        BrowserCommand::Inspect { maximum_bytes }
            if *maximum_bytes == 0 || *maximum_bytes > maximum_inline =>
        {
            Err(BrowserLogicError::Invalid)
        }
        BrowserCommand::Click { selector } | BrowserCommand::Submit { selector }
            if selector.trim().is_empty() || selector.len() > 4096 =>
        {
            Err(BrowserLogicError::Invalid)
        }
        BrowserCommand::Type { selector, text }
            if selector.trim().is_empty() || selector.len() > 4096 || text.len() > 64 * 1024 =>
        {
            Err(BrowserLogicError::Invalid)
        }
        BrowserCommand::Download { url, maximum_bytes }
            if url.is_empty() || *maximum_bytes == 0 || *maximum_bytes > maximum_artifact =>
        {
            Err(BrowserLogicError::Invalid)
        }
        _ => Ok(()),
    }
}

fn map_authorization(value: BrowserAuthorization) -> BrowserDataAuthorization {
    BrowserDataAuthorization {
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
    reason = "data errors are deliberately consumed at the logic boundary"
)]
const fn map_error(error: BrowserDataError) -> BrowserLogicError {
    match error {
        BrowserDataError::Invalid => BrowserLogicError::Invalid,
        BrowserDataError::Denied => BrowserLogicError::Denied,
        BrowserDataError::NoSession => BrowserLogicError::NoSession,
        BrowserDataError::TooLarge => BrowserLogicError::TooLarge,
        BrowserDataError::Cancelled => BrowserLogicError::Cancelled,
        BrowserDataError::Unavailable => BrowserLogicError::Unavailable,
    }
}

/// Logic failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BrowserLogicError {
    /// Invalid composition.
    #[error("invalid browser logic configuration")]
    Configuration,
    /// Invalid command.
    #[error("invalid browser command")]
    Invalid,
    /// Denied.
    #[error("browser command denied")]
    Denied,
    /// No session.
    #[error("browser session is not active")]
    NoSession,
    /// Too large.
    #[error("browser output exceeded its bound")]
    TooLarge,
    /// Cancelled.
    #[error("browser command cancelled")]
    Cancelled,
    /// Unavailable.
    #[error("browser operation unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use agentmod_browser_host_data::{
        BrowserDataError, BrowserDataPort, BrowserDataRecord, BrowserDataRequest,
    };
    use async_trait::async_trait;
    use serde_json::{Value, json};

    use super::{
        BrowserAuthorization, BrowserCommand, BrowserLogic, BrowserLogicPort, BrowserLogicRequest,
    };

    #[derive(Clone)]
    struct MockData;

    #[async_trait]
    impl BrowserDataPort for MockData {
        async fn execute(
            &self,
            request: BrowserDataRequest,
        ) -> Result<BrowserDataRecord, BrowserDataError> {
            assert_eq!(request.authorization.action, "browser.click");
            assert!(matches!(
                request.action,
                agentmod_browser_host_data::BrowserDataAction::Click { ref selector }
                    if selector == "#save"
            ));
            Ok(BrowserDataRecord {
                result: json!({"clicked":true}),
                artifact: None,
                truncated: false,
            })
        }

        async fn cancel(&self, _: &str) -> Result<(), BrowserDataError> {
            Ok(())
        }

        async fn health(&self) -> Result<Value, BrowserDataError> {
            Ok(json!({"healthy":true}))
        }

        async fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn logic_validates_and_maps_without_external_dependencies() {
        let logic = BrowserLogic::new(MockData, 1024, 4096).expect("logic");
        let value = logic
            .execute(BrowserLogicRequest {
                authorization: BrowserAuthorization {
                    call_id: "call".to_owned(),
                    action: "browser.click".to_owned(),
                    normalized_digest: "11".repeat(32),
                    grant: "grant".to_owned(),
                    arguments: json!({"selector":"#save"}),
                    cancellation_id: "cancel".to_owned(),
                },
                command: BrowserCommand::Click {
                    selector: "#save".to_owned(),
                },
            })
            .await
            .expect("result");
        assert_eq!(value.result["clicked"], true);
    }

    #[tokio::test]
    async fn logic_rejects_oversized_inspection_before_data() {
        let logic = BrowserLogic::new(MockData, 8, 16).expect("logic");
        let error = logic
            .execute(BrowserLogicRequest {
                authorization: BrowserAuthorization {
                    call_id: "call".to_owned(),
                    action: "browser.inspect".to_owned(),
                    normalized_digest: "11".repeat(32),
                    grant: "grant".to_owned(),
                    arguments: json!({"maximum_bytes":9}),
                    cancellation_id: "cancel".to_owned(),
                },
                command: BrowserCommand::Inspect { maximum_bytes: 9 },
            })
            .await
            .expect_err("bound");
        assert_eq!(error, super::BrowserLogicError::Invalid);
    }
}
