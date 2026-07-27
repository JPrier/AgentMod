//! External dependency contracts and implementations for the native harness.

pub mod execution;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use thiserror::Error;

/// Dependency-owned request for the configured provider catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCatalogProbeRequest {
    /// Whether providers that are currently unavailable should be returned.
    pub include_unavailable: bool,
}

/// Dependency-owned provider catalog entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyProviderRecord {
    /// Adapter-local provider key.
    pub provider_key: String,
    /// Whether the provider adapter can accept work.
    pub ready: bool,
    /// Provider features discovered by the adapter.
    pub capabilities: BTreeSet<String>,
}

/// Dependency-owned response from a provider catalog probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCatalogProbeResponse {
    /// Provider entries in deterministic key order.
    pub providers: Vec<DependencyProviderRecord>,
}

/// Failure reported by a provider-catalog adapter.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DependencyError {
    /// The configured provider catalog cannot be read.
    #[error("provider catalog is unavailable: {detail}")]
    CatalogUnavailable {
        /// Redacted adapter diagnostic.
        detail: String,
    },
    /// Bootstrap authorization key was malformed.
    #[error("harness authorization key is invalid")]
    InvalidAuthorizationKey,
}

/// Narrow external dependency used to inspect provider readiness.
pub trait ProviderCatalogDependency {
    /// Reads the configured providers without performing a model request.
    ///
    /// # Errors
    ///
    /// Returns [`DependencyError`] when the external catalog cannot be read.
    fn probe_catalog(
        &self,
        request: ProviderCatalogProbeRequest,
    ) -> Result<ProviderCatalogProbeResponse, DependencyError>;
}

/// Deterministic built-in catalog used for local health checks and tests.
#[derive(Clone, Debug)]
pub struct StaticProviderCatalogDependency {
    providers: Vec<DependencyProviderRecord>,
    grant_validation: GrantValidation,
}

#[derive(Clone, Debug)]
enum GrantValidation {
    Development,
    Secure {
        key: [u8; 32],
        uses: Arc<Mutex<BTreeMap<uuid::Uuid, u8>>>,
    },
}

impl StaticProviderCatalogDependency {
    /// Creates the deterministic built-in provider catalog.
    #[must_use]
    pub fn built_in() -> Self {
        Self {
            providers: vec![DependencyProviderRecord {
                provider_key: "deterministic-mock".to_owned(),
                ready: true,
                capabilities: BTreeSet::from([
                    "cancellation".to_owned(),
                    "streaming".to_owned(),
                    "tool_calls".to_owned(),
                ]),
            }],
            grant_validation: GrantValidation::Development,
        }
    }

    /// Creates the built-in catalog with mandatory keyed grant validation.
    #[must_use]
    pub fn secure(authorization_key: [u8; 32]) -> Self {
        let mut value = Self::built_in();
        value.grant_validation = GrantValidation::Secure {
            key: authorization_key,
            uses: Arc::new(Mutex::new(BTreeMap::new())),
        };
        value
    }

    pub(crate) fn validate_grant(
        &self,
        grant: &str,
        resumed: bool,
    ) -> Result<(), execution::ProviderExecutionDependencyError> {
        let GrantValidation::Secure { key, uses } = &self.grant_validation else {
            return if grant == "grant" {
                Ok(())
            } else {
                Err(execution::ProviderExecutionDependencyError::InvalidRequest(
                    "development authorization grant is invalid".into(),
                ))
            };
        };
        execution::validate_runtime_grant(grant, key, uses, resumed)
    }
}

impl Default for StaticProviderCatalogDependency {
    fn default() -> Self {
        Self::built_in()
    }
}

/// Parses an exact 32-byte hexadecimal bootstrap key.
///
/// # Errors
///
/// Returns [`DependencyError::InvalidAuthorizationKey`] for malformed input.
pub fn parse_authorization_key(value: &str) -> Result<[u8; 32], DependencyError> {
    if value.len() != 64 {
        return Err(DependencyError::InvalidAuthorizationKey);
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text =
            std::str::from_utf8(chunk).map_err(|_| DependencyError::InvalidAuthorizationKey)?;
        key[index] =
            u8::from_str_radix(text, 16).map_err(|_| DependencyError::InvalidAuthorizationKey)?;
    }
    Ok(key)
}

impl ProviderCatalogDependency for StaticProviderCatalogDependency {
    fn probe_catalog(
        &self,
        request: ProviderCatalogProbeRequest,
    ) -> Result<ProviderCatalogProbeResponse, DependencyError> {
        let mut providers: Vec<_> = self
            .providers
            .iter()
            .filter(|provider| request.include_unavailable || provider.ready)
            .cloned()
            .collect();
        providers.sort_by(|left, right| left.provider_key.cmp(&right.provider_key));
        Ok(ProviderCatalogProbeResponse { providers })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_catalog_is_ready_and_deterministic() {
        let dependency = StaticProviderCatalogDependency::built_in();

        let first = dependency
            .probe_catalog(ProviderCatalogProbeRequest {
                include_unavailable: true,
            })
            .expect("built-in catalog is readable");
        let second = dependency
            .probe_catalog(ProviderCatalogProbeRequest {
                include_unavailable: true,
            })
            .expect("built-in catalog remains readable");

        assert_eq!(first, second);
        assert_eq!(first.providers.len(), 1);
        assert_eq!(first.providers[0].provider_key, "deterministic-mock");
        assert!(first.providers[0].ready);
        assert_eq!(
            first.providers[0].capabilities,
            BTreeSet::from([
                "cancellation".to_owned(),
                "streaming".to_owned(),
                "tool_calls".to_owned(),
            ])
        );
    }
}
