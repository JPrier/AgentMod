//! Provider-independent health behavior for the native harness.

pub mod execution;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use agentmod_harness_data::{
    DataError, HarnessCatalogData, HarnessCatalogRecord, HarnessHealthData, HarnessHealthDataQuery,
    HarnessHealthRecord,
};
use thiserror::Error;

/// Logic-owned command for evaluating harness health.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectHarnessHealthCommand {
    /// Provider capabilities required by this caller.
    pub required_capabilities: Vec<String>,
}

/// Business health classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessHealthStatus {
    /// At least one provider is ready and all requested capabilities exist.
    Ready,
    /// A provider is ready, but one or more requested capabilities are absent.
    Degraded,
    /// No provider is currently ready.
    Unavailable,
}

/// Logic-owned result of a harness health evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessHealthResult {
    /// Business health classification.
    pub status: HarnessHealthStatus,
    /// Providers configured in the catalog.
    pub configured_provider_count: u32,
    /// Providers currently ready.
    pub ready_provider_count: u32,
    /// Capabilities currently available.
    pub available_capabilities: Vec<String>,
    /// Required capabilities currently unavailable.
    pub missing_capabilities: Vec<String>,
}

/// Business failure while evaluating harness health.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum LogicError {
    /// A capability name was empty or not normalized.
    #[error("invalid required capability `{capability}`")]
    InvalidCapability {
        /// Invalid capability text.
        capability: String,
    },
    /// Required health data could not be assembled.
    #[error("harness health data is unavailable: {detail}")]
    HealthDataUnavailable {
        /// Redacted data-layer diagnostic.
        detail: String,
    },
}

/// Harness health business interface exposed to service.
pub trait HarnessHealthLogic {
    /// Evaluates provider readiness and requested capabilities.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] for invalid capability names or unavailable health data.
    fn inspect_health(
        &self,
        command: InspectHarnessHealthCommand,
    ) -> Result<HarnessHealthResult, LogicError>;
}

/// Logic-owned detailed provider/model catalog entry.
#[allow(
    clippy::struct_excessive_bools,
    reason = "capability flags are the catalog contract"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicCatalogRecord {
    /// Stable provider key.
    pub provider_key: String,
    /// Adapter version.
    pub version: String,
    /// Discoverable model IDs in stable order.
    pub model_ids: Vec<String>,
    /// Sorted capability names.
    pub capabilities: Vec<String>,
    /// Known context limit in tokens.
    pub context_limit: Option<u64>,
    /// Tool-call support.
    pub tool_support: bool,
    /// Image input support.
    pub image_support: bool,
    /// Structured-output support.
    pub structured_output_support: bool,
    /// Streaming support.
    pub streaming_support: bool,
    /// Pricing-record source.
    pub pricing_source: String,
    /// Whether the provider can accept work now.
    pub ready: bool,
}

/// Harness provider/model catalog business interface exposed to service.
pub trait HarnessCatalogLogic {
    /// Reads the bounded detailed provider/model catalog.
    ///
    /// # Errors
    ///
    /// Returns [`LogicError`] when catalog data is unavailable.
    fn inspect_catalog(
        &self,
        include_unavailable: bool,
    ) -> Result<Vec<LogicCatalogRecord>, LogicError>;
}

/// Provider-independent harness health use case.
#[derive(Clone, Debug)]
pub struct HarnessHealthManager<D> {
    data: D,
    pub(crate) pending: Arc<Mutex<BTreeMap<String, execution::PendingProviderExecution>>>,
}

impl<D> HarnessHealthManager<D> {
    /// Injects the business-facing health data implementation.
    #[must_use]
    pub fn new(data: D) -> Self {
        Self {
            data,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl<D> HarnessHealthLogic for HarnessHealthManager<D>
where
    D: HarnessHealthData,
{
    fn inspect_health(
        &self,
        command: InspectHarnessHealthCommand,
    ) -> Result<HarnessHealthResult, LogicError> {
        let data_query = to_data_query(command)?;
        let record = self.data.read_health(data_query).map_err(map_data_error)?;
        Ok(to_logic_result(record))
    }
}

impl<D> HarnessCatalogLogic for HarnessHealthManager<D>
where
    D: HarnessCatalogData,
{
    fn inspect_catalog(
        &self,
        include_unavailable: bool,
    ) -> Result<Vec<LogicCatalogRecord>, LogicError> {
        let records = self
            .data
            .read_catalog(include_unavailable)
            .map_err(map_data_error)?;
        Ok(records.into_iter().map(to_logic_catalog_record).collect())
    }
}

fn to_logic_catalog_record(record: HarnessCatalogRecord) -> LogicCatalogRecord {
    LogicCatalogRecord {
        provider_key: record.provider_key,
        version: record.version,
        model_ids: record.model_ids,
        capabilities: record.capabilities.into_iter().collect(),
        context_limit: record.context_limit,
        tool_support: record.tool_support,
        image_support: record.image_support,
        structured_output_support: record.structured_output_support,
        streaming_support: record.streaming_support,
        pricing_source: record.pricing_source,
        ready: record.ready,
    }
}

fn to_data_query(
    command: InspectHarnessHealthCommand,
) -> Result<HarnessHealthDataQuery, LogicError> {
    let mut required_capabilities = BTreeSet::new();
    for capability in command.required_capabilities {
        let normalized = capability.trim().to_ascii_lowercase();
        if normalized.is_empty()
            || normalized
                .chars()
                .any(|character| !(character.is_ascii_alphanumeric() || character == '_'))
        {
            return Err(LogicError::InvalidCapability { capability });
        }
        required_capabilities.insert(normalized);
    }
    Ok(HarnessHealthDataQuery {
        required_capabilities,
    })
}

fn to_logic_result(record: HarnessHealthRecord) -> HarnessHealthResult {
    let status = if record.ready_provider_count == 0 {
        HarnessHealthStatus::Unavailable
    } else if record.missing_capabilities.is_empty() {
        HarnessHealthStatus::Ready
    } else {
        HarnessHealthStatus::Degraded
    };
    HarnessHealthResult {
        status,
        configured_provider_count: record.configured_provider_count,
        ready_provider_count: record.ready_provider_count,
        available_capabilities: record.available_capabilities.into_iter().collect(),
        missing_capabilities: record.missing_capabilities.into_iter().collect(),
    }
}

fn map_data_error(error: DataError) -> LogicError {
    match error {
        DataError::ProviderHealthUnavailable { detail } => {
            LogicError::HealthDataUnavailable { detail }
        }
        DataError::ProviderCountOverflow => LogicError::HealthDataUnavailable {
            detail: "provider count exceeds supported range".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockHealthData {
        queries: Mutex<Vec<HarnessHealthDataQuery>>,
        response: Result<HarnessHealthRecord, DataError>,
    }

    impl HarnessHealthData for MockHealthData {
        fn read_health(
            &self,
            query: HarnessHealthDataQuery,
        ) -> Result<HarnessHealthRecord, DataError> {
            self.queries
                .lock()
                .expect("query lock is not poisoned")
                .push(query);
            self.response.clone()
        }
    }

    #[test]
    fn normalizes_command_and_classifies_degraded_health() {
        let data = MockHealthData {
            queries: Mutex::new(Vec::new()),
            response: Ok(HarnessHealthRecord {
                configured_provider_count: 2,
                ready_provider_count: 1,
                available_capabilities: BTreeSet::from(["streaming".to_owned()]),
                missing_capabilities: BTreeSet::from(["images".to_owned()]),
            }),
        };
        let manager = HarnessHealthManager::new(data);

        let result = manager
            .inspect_health(InspectHarnessHealthCommand {
                required_capabilities: vec![
                    " Streaming ".to_owned(),
                    "images".to_owned(),
                    "streaming".to_owned(),
                ],
            })
            .expect("mock data succeeds");

        assert_eq!(result.status, HarnessHealthStatus::Degraded);
        assert_eq!(
            manager
                .data
                .queries
                .lock()
                .expect("query lock is not poisoned")
                .as_slice(),
            &[HarnessHealthDataQuery {
                required_capabilities: BTreeSet::from([
                    "images".to_owned(),
                    "streaming".to_owned(),
                ])
            }]
        );
    }

    #[test]
    fn rejects_invalid_capability_without_calling_data() {
        let manager = HarnessHealthManager::new(MockHealthData {
            queries: Mutex::new(Vec::new()),
            response: Ok(HarnessHealthRecord {
                configured_provider_count: 0,
                ready_provider_count: 0,
                available_capabilities: BTreeSet::new(),
                missing_capabilities: BTreeSet::new(),
            }),
        });

        assert_eq!(
            manager.inspect_health(InspectHarnessHealthCommand {
                required_capabilities: vec!["tool-calls".to_owned()],
            }),
            Err(LogicError::InvalidCapability {
                capability: "tool-calls".to_owned(),
            })
        );
        assert!(
            manager
                .data
                .queries
                .lock()
                .expect("query lock is not poisoned")
                .is_empty()
        );
    }

    #[test]
    fn translates_data_failure() {
        let manager = HarnessHealthManager::new(MockHealthData {
            queries: Mutex::new(Vec::new()),
            response: Err(DataError::ProviderHealthUnavailable {
                detail: "fixture failure".to_owned(),
            }),
        });

        assert_eq!(
            manager.inspect_health(InspectHarnessHealthCommand {
                required_capabilities: Vec::new(),
            }),
            Err(LogicError::HealthDataUnavailable {
                detail: "fixture failure".to_owned(),
            })
        );
    }
}
