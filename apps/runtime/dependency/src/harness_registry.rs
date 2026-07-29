//! Dependency-owned harness descriptors and adapter routing.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::{Mutex, mpsc};

use crate::harness::{
    DependencyCommand, DependencyEventStream, DependencyReply, HarnessDependencyError,
    HarnessDependencyPort,
};

/// Dependency-bound harness descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencyHarnessDescriptor {
    /// Stable registry ID.
    pub id: String,
    /// Adapter semantic version.
    pub version: String,
    /// Sorted capability IDs.
    pub capabilities: Vec<String>,
    /// Whether the adapter may accept new sessions.
    pub available: bool,
}

/// Dependency catalog boundary consumed by runtime data.
pub trait HarnessRegistryDependencyPort: Send + Sync {
    /// Returns all bounded descriptors in stable ID order.
    ///
    /// # Errors
    ///
    /// Returns a registry error when configured descriptors cannot be read.
    fn list_harnesses(
        &self,
    ) -> Result<Vec<DependencyHarnessDescriptor>, HarnessRegistryDependencyError>;
}

/// Immutable registry of explicitly injected harness adapters.
#[derive(Clone)]
pub struct HarnessRegistryDependency {
    descriptors: Arc<BTreeMap<String, DependencyHarnessDescriptor>>,
    adapters: Arc<BTreeMap<String, Arc<dyn HarnessDependencyPort>>>,
    active_cancellations: Arc<Mutex<BTreeMap<String, String>>>,
}

impl HarnessRegistryDependency {
    /// Builds a registry after validating descriptor and adapter identity.
    ///
    /// # Errors
    ///
    /// Returns [`HarnessRegistryDependencyError::InvalidRegistry`] when the
    /// registry is empty, oversized, duplicated, or contains malformed fields.
    pub fn new(
        entries: Vec<(DependencyHarnessDescriptor, Arc<dyn HarnessDependencyPort>)>,
    ) -> Result<Self, HarnessRegistryDependencyError> {
        if entries.is_empty() || entries.len() > 64 {
            return Err(HarnessRegistryDependencyError::InvalidRegistry);
        }
        let mut descriptors = BTreeMap::new();
        let mut adapters = BTreeMap::new();
        for (mut descriptor, adapter) in entries {
            descriptor.capabilities.sort();
            descriptor.capabilities.dedup();
            if descriptor.id.trim().is_empty()
                || descriptor.version.trim().is_empty()
                || descriptor
                    .capabilities
                    .iter()
                    .any(|value| value.trim().is_empty())
                || descriptors
                    .insert(descriptor.id.clone(), descriptor.clone())
                    .is_some()
            {
                return Err(HarnessRegistryDependencyError::InvalidRegistry);
            }
            adapters.insert(descriptor.id.clone(), adapter);
        }
        Ok(Self {
            descriptors: Arc::new(descriptors),
            adapters: Arc::new(adapters),
            active_cancellations: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    async fn routed_harness_id(&self, command: &DependencyCommand) -> String {
        if let DependencyCommand::Cancel {
            harness_id,
            cancellation_id,
        } = command
            && harness_id.is_empty()
        {
            return self
                .active_cancellations
                .lock()
                .await
                .get(cancellation_id)
                .cloned()
                .unwrap_or_default();
        }
        command.harness_id().to_owned()
    }

    fn adapter_by_id(
        &self,
        id: &str,
    ) -> Result<Arc<dyn HarnessDependencyPort>, HarnessDependencyError> {
        let descriptor = self
            .descriptors
            .get(id)
            .filter(|descriptor| descriptor.available)
            .ok_or(HarnessDependencyError::Unavailable)?;
        self.adapters
            .get(&descriptor.id)
            .cloned()
            .ok_or(HarnessDependencyError::Unavailable)
    }
}

impl HarnessRegistryDependencyPort for HarnessRegistryDependency {
    fn list_harnesses(
        &self,
    ) -> Result<Vec<DependencyHarnessDescriptor>, HarnessRegistryDependencyError> {
        Ok(self.descriptors.values().cloned().collect())
    }
}

impl HarnessRegistryDependencyPort for crate::LocalRuntimeDependencies {
    fn list_harnesses(
        &self,
    ) -> Result<Vec<DependencyHarnessDescriptor>, HarnessRegistryDependencyError> {
        Ok(vec![
            DependencyHarnessDescriptor {
                id: String::from("fixture"),
                version: String::from("1.0.0"),
                capabilities: [
                    "cancellation",
                    "streaming",
                    "structured_context_replacement",
                    "structured_output",
                    "token_usage",
                    "tool_calls",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                available: true,
            },
            DependencyHarnessDescriptor {
                id: String::from("native"),
                version: String::from("1.0.0"),
                capabilities: [
                    "cancellation",
                    "cost_metadata",
                    "fine_grained_proposal_boundaries",
                    "images",
                    "multiple_tool_calls",
                    "provider_switching",
                    "streaming",
                    "structured_context_replacement",
                    "structured_output",
                    "token_usage",
                    "tool_calls",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                available: true,
            },
        ])
    }
}

#[async_trait]
impl HarnessDependencyPort for HarnessRegistryDependency {
    async fn exchange(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyReply, HarnessDependencyError> {
        let id = self.routed_harness_id(&command).await;
        self.adapter_by_id(&id)?.exchange(command).await
    }

    async fn exchange_events(
        &self,
        command: DependencyCommand,
    ) -> Result<DependencyEventStream, HarnessDependencyError> {
        let harness_id = self.routed_harness_id(&command).await;
        let cancellation_id = match &command {
            DependencyCommand::Execute {
                cancellation_id, ..
            } => Some(cancellation_id.clone()),
            _ => None,
        };
        let mut stream = self
            .adapter_by_id(&harness_id)?
            .exchange_events(command)
            .await?;
        let Some(cancellation_id) = cancellation_id else {
            return Ok(stream);
        };
        self.active_cancellations
            .lock()
            .await
            .insert(cancellation_id.clone(), harness_id);
        let routes = self.active_cancellations.clone();
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                let terminal = matches!(
                    &event,
                    Ok(crate::harness::DependencyEvent::Completed { .. }
                        | crate::harness::DependencyEvent::Cancelled
                        | crate::harness::DependencyEvent::Failed { .. })
                        | Err(_)
                );
                if sender.send(event).await.is_err() || terminal {
                    break;
                }
            }
            routes.lock().await.remove(&cancellation_id);
        });
        Ok(DependencyEventStream::from_receiver(receiver))
    }

    async fn shutdown(&self) {
        for adapter in self.adapters.values() {
            adapter.shutdown().await;
        }
    }
}

/// Registry construction or catalog failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HarnessRegistryDependencyError {
    /// Registry descriptors are empty, duplicate, oversized, or malformed.
    #[error("invalid harness registry")]
    InvalidRegistry,
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    struct FixtureAdapter {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl HarnessDependencyPort for FixtureAdapter {
        async fn exchange(
            &self,
            _command: DependencyCommand,
        ) -> Result<DependencyReply, HarnessDependencyError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(DependencyReply::Health {
                status: String::from("ok"),
                ready: 1,
                capabilities: Vec::new(),
            })
        }

        async fn exchange_events(
            &self,
            _command: DependencyCommand,
        ) -> Result<DependencyEventStream, HarnessDependencyError> {
            Err(HarnessDependencyError::InvalidRequest)
        }

        async fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn routes_each_command_to_the_explicit_adapter_identity() {
        let native = Arc::new(FixtureAdapter {
            calls: AtomicUsize::new(0),
        });
        let fixture = Arc::new(FixtureAdapter {
            calls: AtomicUsize::new(0),
        });
        let registry = HarnessRegistryDependency::new(vec![
            (
                DependencyHarnessDescriptor {
                    id: String::from("fixture"),
                    version: String::from("1.0.0"),
                    capabilities: vec![String::from("streaming")],
                    available: true,
                },
                fixture.clone(),
            ),
            (
                DependencyHarnessDescriptor {
                    id: String::from("native"),
                    version: String::from("1.0.0"),
                    capabilities: vec![String::from("streaming")],
                    available: true,
                },
                native.clone(),
            ),
        ])
        .expect("registry");

        registry
            .exchange(DependencyCommand::Health {
                harness_id: String::from("fixture"),
            })
            .await
            .expect("fixture health");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
        assert_eq!(native.calls.load(Ordering::SeqCst), 0);
        assert!(
            registry
                .exchange(DependencyCommand::Health {
                    harness_id: String::from("missing"),
                })
                .await
                .is_err()
        );
    }
}
