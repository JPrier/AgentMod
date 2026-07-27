//! Business-facing provider health datasets for the native harness.

pub mod execution;

use std::collections::BTreeSet;

use agentmod_harness_dependency::{
    DependencyError, ProviderCatalogDependency, ProviderCatalogProbeRequest,
    ProviderCatalogProbeResponse,
};
use thiserror::Error;

/// Data-owned query for harness provider health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessHealthDataQuery {
    /// Capabilities the caller needs the ready provider set to expose.
    pub required_capabilities: BTreeSet<String>,
}

/// Data-owned aggregate of provider catalog health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessHealthRecord {
    /// Number of configured providers.
    pub configured_provider_count: u32,
    /// Number of providers currently able to accept work.
    pub ready_provider_count: u32,
    /// Union of capabilities exposed by ready providers.
    pub available_capabilities: BTreeSet<String>,
    /// Requested capabilities absent from all ready providers.
    pub missing_capabilities: BTreeSet<String>,
}

/// Failure while assembling harness business data.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DataError {
    /// Provider catalog data could not be obtained.
    #[error("provider health data is unavailable: {detail}")]
    ProviderHealthUnavailable {
        /// Redacted dependency diagnostic.
        detail: String,
    },
    /// Provider counts exceeded the stable data representation.
    #[error("provider catalog contains too many entries")]
    ProviderCountOverflow,
}

/// Business-facing provider health data interface.
pub trait HarnessHealthData {
    /// Builds a normalized provider health record.
    ///
    /// # Errors
    ///
    /// Returns [`DataError`] when provider records cannot be read or counted.
    fn read_health(&self, query: HarnessHealthDataQuery) -> Result<HarnessHealthRecord, DataError>;
}

/// Provider health data assembler backed by one catalog dependency.
#[derive(Clone, Debug)]
pub struct HarnessHealthDataStore<D> {
    dependency: D,
}

impl<D> HarnessHealthDataStore<D> {
    /// Injects the provider catalog dependency.
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}

impl<D> HarnessHealthData for HarnessHealthDataStore<D>
where
    D: ProviderCatalogDependency,
{
    fn read_health(&self, query: HarnessHealthDataQuery) -> Result<HarnessHealthRecord, DataError> {
        let dependency_request = to_dependency_request(&query);
        let dependency_response = self
            .dependency
            .probe_catalog(dependency_request)
            .map_err(map_dependency_error)?;
        to_data_record(query, dependency_response)
    }
}

fn to_dependency_request(_query: &HarnessHealthDataQuery) -> ProviderCatalogProbeRequest {
    ProviderCatalogProbeRequest {
        include_unavailable: true,
    }
}

fn to_data_record(
    query: HarnessHealthDataQuery,
    response: ProviderCatalogProbeResponse,
) -> Result<HarnessHealthRecord, DataError> {
    let configured_provider_count =
        u32::try_from(response.providers.len()).map_err(|_| DataError::ProviderCountOverflow)?;
    let ready_provider_count = u32::try_from(
        response
            .providers
            .iter()
            .filter(|provider| provider.ready)
            .count(),
    )
    .map_err(|_| DataError::ProviderCountOverflow)?;
    let available_capabilities = response
        .providers
        .into_iter()
        .filter(|provider| provider.ready)
        .flat_map(|provider| provider.capabilities)
        .collect::<BTreeSet<_>>();
    let missing_capabilities = query
        .required_capabilities
        .into_iter()
        .filter(|capability| !available_capabilities.contains(capability))
        .collect();

    Ok(HarnessHealthRecord {
        configured_provider_count,
        ready_provider_count,
        available_capabilities,
        missing_capabilities,
    })
}

fn map_dependency_error(error: DependencyError) -> DataError {
    match error {
        DependencyError::CatalogUnavailable { detail } => {
            DataError::ProviderHealthUnavailable { detail }
        }
        DependencyError::InvalidAuthorizationKey => DataError::ProviderHealthUnavailable {
            detail: String::from("harness authorization is unavailable"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use agentmod_harness_dependency::DependencyProviderRecord;

    use super::*;

    struct MockProviderCatalog {
        requests: Mutex<Vec<ProviderCatalogProbeRequest>>,
        response: Result<ProviderCatalogProbeResponse, DependencyError>,
    }

    impl ProviderCatalogDependency for MockProviderCatalog {
        fn probe_catalog(
            &self,
            request: ProviderCatalogProbeRequest,
        ) -> Result<ProviderCatalogProbeResponse, DependencyError> {
            self.requests
                .lock()
                .expect("request lock is not poisoned")
                .push(request);
            self.response.clone()
        }
    }

    #[test]
    fn maps_query_and_aggregates_only_ready_provider_capabilities() {
        let dependency = MockProviderCatalog {
            requests: Mutex::new(Vec::new()),
            response: Ok(ProviderCatalogProbeResponse {
                providers: vec![
                    DependencyProviderRecord {
                        provider_key: "ready".to_owned(),
                        ready: true,
                        capabilities: BTreeSet::from(["streaming".to_owned()]),
                    },
                    DependencyProviderRecord {
                        provider_key: "offline".to_owned(),
                        ready: false,
                        capabilities: BTreeSet::from(["images".to_owned()]),
                    },
                ],
            }),
        };
        let store = HarnessHealthDataStore::new(dependency);

        let record = store
            .read_health(HarnessHealthDataQuery {
                required_capabilities: BTreeSet::from([
                    "images".to_owned(),
                    "streaming".to_owned(),
                ]),
            })
            .expect("mock catalog succeeds");

        assert_eq!(record.configured_provider_count, 2);
        assert_eq!(record.ready_provider_count, 1);
        assert_eq!(
            record.available_capabilities,
            BTreeSet::from(["streaming".to_owned()])
        );
        assert_eq!(
            record.missing_capabilities,
            BTreeSet::from(["images".to_owned()])
        );
        assert_eq!(
            store
                .dependency
                .requests
                .lock()
                .expect("request lock is not poisoned")
                .as_slice(),
            &[ProviderCatalogProbeRequest {
                include_unavailable: true
            }]
        );
    }

    #[test]
    fn translates_dependency_failure() {
        let store = HarnessHealthDataStore::new(MockProviderCatalog {
            requests: Mutex::new(Vec::new()),
            response: Err(DependencyError::CatalogUnavailable {
                detail: "fixture unavailable".to_owned(),
            }),
        });

        assert_eq!(
            store.read_health(HarnessHealthDataQuery {
                required_capabilities: BTreeSet::new(),
            }),
            Err(DataError::ProviderHealthUnavailable {
                detail: "fixture unavailable".to_owned(),
            })
        );
    }
}
