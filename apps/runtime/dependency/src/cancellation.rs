//! Runtime-owned cancellation state independent of process existence.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use thiserror::Error;

const MAX_CANCELLATIONS: usize = 16_384;

/// Narrow dependency port for explicit runtime cancellation state.
pub trait RuntimeCancellationDependencyPort: Send + Sync {
    /// Records one exact cancellation idempotently.
    ///
    /// # Errors
    ///
    /// Returns a classified error for invalid identity, capacity, or
    /// unavailable state.
    fn request_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<(), RuntimeCancellationDependencyError>;

    /// Clears one exact terminal cancellation identity.
    ///
    /// # Errors
    ///
    /// Returns a classified error for invalid identity or unavailable state.
    fn clear_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<bool, RuntimeCancellationDependencyError>;

    /// Reports whether the exact runtime cancellation identity was requested.
    ///
    /// # Errors
    ///
    /// Returns a classified error for invalid identity or unavailable state.
    fn cancellation_requested(
        &self,
        cancellation_id: &str,
    ) -> Result<bool, RuntimeCancellationDependencyError>;
}

/// Shared runtime cancellation registry composed independently of worker
/// process lifecycle.
#[derive(Clone, Debug, Default)]
pub struct RuntimeCancellationDependency {
    requested: Arc<Mutex<BTreeSet<String>>>,
}

impl RuntimeCancellationDependency {
    /// Records one exact cancellation idempotently.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe identity, poisoned state, or exceeded
    /// registry bound.
    pub fn request(&self, cancellation_id: &str) -> Result<(), RuntimeCancellationDependencyError> {
        validate_id(cancellation_id)?;
        let mut requested = self
            .requested
            .lock()
            .map_err(|_| RuntimeCancellationDependencyError::Unavailable)?;
        if requested.len() >= MAX_CANCELLATIONS && !requested.contains(cancellation_id) {
            return Err(RuntimeCancellationDependencyError::Capacity);
        }
        requested.insert(cancellation_id.to_owned());
        Ok(())
    }

    /// Removes an exact terminal cancellation identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsafe identity or poisoned state.
    pub fn clear(&self, cancellation_id: &str) -> Result<bool, RuntimeCancellationDependencyError> {
        validate_id(cancellation_id)?;
        self.requested
            .lock()
            .map_err(|_| RuntimeCancellationDependencyError::Unavailable)
            .map(|mut requested| requested.remove(cancellation_id))
    }
}

impl RuntimeCancellationDependencyPort for RuntimeCancellationDependency {
    fn request_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<(), RuntimeCancellationDependencyError> {
        self.request(cancellation_id)
    }

    fn clear_cancellation(
        &self,
        cancellation_id: &str,
    ) -> Result<bool, RuntimeCancellationDependencyError> {
        self.clear(cancellation_id)
    }

    fn cancellation_requested(
        &self,
        cancellation_id: &str,
    ) -> Result<bool, RuntimeCancellationDependencyError> {
        validate_id(cancellation_id)?;
        self.requested
            .lock()
            .map_err(|_| RuntimeCancellationDependencyError::Unavailable)
            .map(|requested| requested.contains(cancellation_id))
    }
}

fn validate_id(value: &str) -> Result<(), RuntimeCancellationDependencyError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(RuntimeCancellationDependencyError::InvalidRequest);
    }
    Ok(())
}

/// Stable cancellation dependency failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RuntimeCancellationDependencyError {
    /// Cancellation identity was unsafe.
    #[error("runtime cancellation identity is invalid")]
    InvalidRequest,
    /// Registry capacity was exhausted.
    #[error("runtime cancellation registry capacity was exhausted")]
    Capacity,
    /// Registry state was unavailable.
    #[error("runtime cancellation registry is unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_request_is_independent_of_process_lifecycle() {
        let source = RuntimeCancellationDependency::default();
        let query = source.clone();
        assert!(
            !query
                .cancellation_requested("plugin-turn:1")
                .expect("query")
        );
        source.request("plugin-turn:1").expect("request");
        assert!(
            query
                .cancellation_requested("plugin-turn:1")
                .expect("query")
        );
        assert!(source.clear("plugin-turn:1").expect("clear"));
        assert!(
            !query
                .cancellation_requested("plugin-turn:1")
                .expect("query")
        );
    }
}
