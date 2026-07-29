//! Data-owned harness catalog records.
#![allow(
    missing_docs,
    reason = "data-local harness catalog records are intentionally boundary-specific"
)]

use agentmod_runtime_dependency::harness_registry as dependency;
use thiserror::Error;

/// Data-layer harness descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessCatalogRecord {
    pub id: String,
    pub version: String,
    pub capabilities: Vec<String>,
    pub available: bool,
}

/// Harness catalog boundary exposed to runtime logic.
pub trait HarnessCatalogDataPort {
    /// Lists normalized harness descriptors.
    ///
    /// # Errors
    ///
    /// Returns a data error when the dependency catalog is unavailable or
    /// contains malformed records.
    fn list_harnesses(&self) -> Result<Vec<HarnessCatalogRecord>, HarnessCatalogDataError>;
}

impl<D> HarnessCatalogDataPort for super::RuntimeData<D>
where
    D: dependency::HarnessRegistryDependencyPort,
{
    fn list_harnesses(&self) -> Result<Vec<HarnessCatalogRecord>, HarnessCatalogDataError> {
        self.dependency
            .list_harnesses()
            .map_err(|_| HarnessCatalogDataError::Unavailable)
            .map(|records| {
                records
                    .into_iter()
                    .map(|record| HarnessCatalogRecord {
                        id: record.id,
                        version: record.version,
                        capabilities: record.capabilities,
                        available: record.available,
                    })
                    .collect()
            })
    }
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HarnessCatalogDataError {
    #[error("harness catalog is unavailable")]
    Unavailable,
}
