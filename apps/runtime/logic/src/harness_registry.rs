//! Harness catalog inspection and compatibility business rules.
#![allow(
    missing_docs,
    reason = "logic-local harness registry records are intentionally boundary-specific"
)]

use std::collections::BTreeSet;

use agentmod_primitives::ContentHash;
use agentmod_runtime_data::harness_registry::{
    HarnessCatalogDataError, HarnessCatalogDataPort, HarnessCatalogRecord,
};
use thiserror::Error;

/// Logic-owned harness descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessDescriptor {
    pub id: String,
    pub version: String,
    pub capabilities: BTreeSet<String>,
    pub capability_set_hash: ContentHash,
    pub availability: HarnessAvailability,
}

/// Harness activation state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessAvailability {
    Available,
    Disabled,
}

/// Harness registry business boundary.
pub trait HarnessRegistryLogicPort {
    /// Lists descriptors in stable registry order.
    ///
    /// # Errors
    ///
    /// Returns a catalog or descriptor-validation error.
    fn list_harnesses(&self) -> Result<Vec<HarnessDescriptor>, HarnessRegistryLogicError>;

    /// Resolves one exact harness ID.
    ///
    /// # Errors
    ///
    /// Returns an invalid-ID, missing-adapter, catalog, or descriptor error.
    fn inspect_harness(&self, id: &str) -> Result<HarnessDescriptor, HarnessRegistryLogicError>;
}

impl<D> HarnessRegistryLogicPort for super::RuntimeLogic<D>
where
    D: HarnessCatalogDataPort,
{
    fn list_harnesses(&self) -> Result<Vec<HarnessDescriptor>, HarnessRegistryLogicError> {
        self.data
            .list_harnesses()
            .map_err(HarnessRegistryLogicError::Data)?
            .into_iter()
            .map(map_descriptor)
            .collect()
    }

    fn inspect_harness(&self, id: &str) -> Result<HarnessDescriptor, HarnessRegistryLogicError> {
        if id.trim().is_empty() || id.len() > 128 {
            return Err(HarnessRegistryLogicError::InvalidId);
        }
        self.list_harnesses()?
            .into_iter()
            .find(|descriptor| descriptor.id == id)
            .ok_or_else(|| HarnessRegistryLogicError::NotFound(id.to_owned()))
    }
}

fn map_descriptor(
    record: HarnessCatalogRecord,
) -> Result<HarnessDescriptor, HarnessRegistryLogicError> {
    if record.id.is_empty() || record.version.is_empty() {
        return Err(HarnessRegistryLogicError::InvalidData);
    }
    let capabilities = record.capabilities.into_iter().collect::<BTreeSet<_>>();
    let canonical = capabilities.iter().cloned().collect::<Vec<_>>();
    Ok(HarnessDescriptor {
        id: record.id,
        version: record.version,
        capability_set_hash: ContentHash::digest(
            &serde_json::to_vec(&canonical).map_err(|_| HarnessRegistryLogicError::InvalidData)?,
        ),
        capabilities,
        availability: if record.available {
            HarnessAvailability::Available
        } else {
            HarnessAvailability::Disabled
        },
    })
}

#[derive(Debug, Eq, Error, PartialEq)]
pub enum HarnessRegistryLogicError {
    #[error("harness catalog failed: {0}")]
    Data(HarnessCatalogDataError),
    #[error("harness ID is invalid")]
    InvalidId,
    #[error("harness `{0}` was not found")]
    NotFound(String),
    #[error("harness catalog returned invalid data")]
    InvalidData,
}
