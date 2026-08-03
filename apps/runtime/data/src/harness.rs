//! Business-facing harness operation datasets.
#![allow(
    missing_docs,
    reason = "data-local transport records are self-describing"
)]
use agentmod_runtime_dependency::harness as dependency;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq)]
pub enum HarnessDataEntry {
    System(String),
    User(String),
    Assistant(String),
    ToolCall {
        call_id: String,
        tool: String,
        arguments: Value,
    },
    ToolResult {
        call_id: String,
        content: String,
        truncated: bool,
    },
    Summary {
        text: String,
        start: u64,
        end: u64,
    },
    Metadata {
        key: String,
        value: Value,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub enum HarnessDataDecision {
    Continue,
    Replace(Vec<HarnessDataEntry>),
    Reject(String),
    Cancel(String),
}
#[derive(Clone, Debug, PartialEq)]
pub enum HarnessDataCommand {
    Execute {
        harness_id: String,
        session_id: String,
        provider: String,
        model: String,
        entries: Vec<HarnessDataEntry>,
        options: Value,
        grant: String,
        cancellation_id: String,
    },
    Continue {
        harness_id: String,
        continuation_id: String,
        decision: HarnessDataDecision,
    },
    Cancel {
        harness_id: String,
        cancellation_id: String,
    },
    Health {
        harness_id: String,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub enum HarnessDataEvent {
    Started,
    Text(String),
    ToolDelta {
        call_id: String,
        name: String,
        arguments: String,
    },
    ToolProposed {
        continuation_id: String,
        call_id: String,
        tool: String,
        arguments: Value,
    },
    Completed {
        reason: String,
        input_tokens: u64,
        output_tokens: u64,
        reasoning_tokens: u64,
        estimated: bool,
        cost_micros: u64,
    },
    Cancelled,
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}
#[derive(Clone, Debug, PartialEq)]
pub enum HarnessDataReply {
    Health {
        status: String,
        ready: u32,
        capabilities: Vec<String>,
    },
    Events(Vec<HarnessDataEvent>),
    Failed {
        code: String,
        message: String,
        retryable: bool,
    },
}

pub struct HarnessDataEventStream {
    receiver: mpsc::Receiver<Result<HarnessDataEvent, HarnessDataError>>,
}

impl HarnessDataEventStream {
    pub async fn next(&mut self) -> Option<Result<HarnessDataEvent, HarnessDataError>> {
        self.receiver.recv().await
    }

    #[must_use]
    pub fn from_events(events: Vec<HarnessDataEvent>) -> Self {
        let (sender, receiver) = mpsc::channel(16);
        tokio::spawn(async move {
            for event in events {
                if sender.send(Ok(event)).await.is_err() {
                    break;
                }
            }
        });
        Self { receiver }
    }
}
#[async_trait]
pub trait HarnessDataPort: Send + Sync {
    async fn exchange(
        &self,
        command: HarnessDataCommand,
    ) -> Result<HarnessDataReply, HarnessDataError>;
    async fn exchange_events(
        &self,
        command: HarnessDataCommand,
    ) -> Result<HarnessDataEventStream, HarnessDataError>;
}
#[derive(Clone)]
pub struct HarnessData<D> {
    dependency: D,
}
impl<D> HarnessData<D> {
    #[must_use]
    pub const fn new(dependency: D) -> Self {
        Self { dependency }
    }
}
#[async_trait]
impl<D: dependency::HarnessDependencyPort> HarnessDataPort for HarnessData<D> {
    async fn exchange(&self, c: HarnessDataCommand) -> Result<HarnessDataReply, HarnessDataError> {
        self.dependency
            .exchange(map_command(c))
            .await
            .map(map_reply)
            .map_err(|_| HarnessDataError::Unavailable)
    }

    async fn exchange_events(
        &self,
        command: HarnessDataCommand,
    ) -> Result<HarnessDataEventStream, HarnessDataError> {
        self.dependency
            .exchange_events(map_command(command))
            .await
            .map(map_stream)
            .map_err(|_| HarnessDataError::Unavailable)
    }
}

#[async_trait]
impl<D: dependency::HarnessDependencyPort> HarnessDataPort for super::RuntimeData<D> {
    async fn exchange(
        &self,
        command: HarnessDataCommand,
    ) -> Result<HarnessDataReply, HarnessDataError> {
        self.dependency
            .exchange(map_command(command))
            .await
            .map(map_reply)
            .map_err(|_| HarnessDataError::Unavailable)
    }

    async fn exchange_events(
        &self,
        command: HarnessDataCommand,
    ) -> Result<HarnessDataEventStream, HarnessDataError> {
        self.dependency
            .exchange_events(map_command(command))
            .await
            .map(map_stream)
            .map_err(|_| HarnessDataError::Unavailable)
    }
}
fn map_command(v: HarnessDataCommand) -> dependency::DependencyCommand {
    match v {
        HarnessDataCommand::Health { harness_id } => {
            dependency::DependencyCommand::Health { harness_id }
        }
        HarnessDataCommand::Cancel {
            harness_id,
            cancellation_id,
        } => dependency::DependencyCommand::Cancel {
            harness_id,
            cancellation_id,
        },
        HarnessDataCommand::Continue {
            harness_id,
            continuation_id,
            decision,
        } => dependency::DependencyCommand::Continue {
            harness_id,
            continuation_id,
            decision: match decision {
                HarnessDataDecision::Continue => dependency::DependencyDecision::Continue,
                HarnessDataDecision::Replace(v) => {
                    dependency::DependencyDecision::Replace(v.into_iter().map(map_entry).collect())
                }
                HarnessDataDecision::Reject(v) => dependency::DependencyDecision::Reject(v),
                HarnessDataDecision::Cancel(v) => dependency::DependencyDecision::Cancel(v),
            },
        },
        HarnessDataCommand::Execute {
            harness_id,
            session_id,
            provider,
            model,
            entries,
            options,
            grant,
            cancellation_id,
        } => dependency::DependencyCommand::Execute {
            harness_id,
            session_id,
            provider,
            model,
            entries: entries.into_iter().map(map_entry).collect(),
            options,
            grant,
            cancellation_id,
        },
    }
}
fn map_entry(v: HarnessDataEntry) -> dependency::DependencyEntry {
    match v {
        HarnessDataEntry::System(v) => dependency::DependencyEntry::System(v),
        HarnessDataEntry::User(v) => dependency::DependencyEntry::User(v),
        HarnessDataEntry::Assistant(v) => dependency::DependencyEntry::Assistant(v),
        HarnessDataEntry::ToolCall {
            call_id,
            tool,
            arguments,
        } => dependency::DependencyEntry::ToolCall {
            call_id,
            tool,
            arguments,
        },
        HarnessDataEntry::ToolResult {
            call_id,
            content,
            truncated,
        } => dependency::DependencyEntry::ToolResult {
            call_id,
            content,
            truncated,
        },
        HarnessDataEntry::Summary { text, start, end } => {
            dependency::DependencyEntry::Summary { text, start, end }
        }
        HarnessDataEntry::Metadata { key, value } => {
            dependency::DependencyEntry::Metadata { key, value }
        }
    }
}
fn map_reply(v: dependency::DependencyReply) -> HarnessDataReply {
    match v {
        dependency::DependencyReply::Health {
            status,
            ready,
            capabilities,
        } => HarnessDataReply::Health {
            status,
            ready,
            capabilities,
        },
        dependency::DependencyReply::Failed {
            code,
            message,
            retryable,
        } => HarnessDataReply::Failed {
            code,
            message,
            retryable,
        },
        dependency::DependencyReply::Events(v) => {
            HarnessDataReply::Events(v.into_iter().map(map_event).collect())
        }
    }
}

fn map_event(event: dependency::DependencyEvent) -> HarnessDataEvent {
    match event {
        dependency::DependencyEvent::Started => HarnessDataEvent::Started,
        dependency::DependencyEvent::Text(value) => HarnessDataEvent::Text(value),
        dependency::DependencyEvent::ToolDelta {
            call_id,
            name,
            arguments,
        } => HarnessDataEvent::ToolDelta {
            call_id,
            name,
            arguments,
        },
        dependency::DependencyEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        } => HarnessDataEvent::ToolProposed {
            continuation_id,
            call_id,
            tool,
            arguments,
        },
        dependency::DependencyEvent::Completed { reason, usage } => HarnessDataEvent::Completed {
            reason,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            estimated: usage.estimated,
            cost_micros: usage.cost.as_ref().map_or(0, |cost| {
                cost.input_cost_micros
                    .saturating_add(cost.output_cost_micros)
                    .saturating_add(cost.cache_read_cost_micros)
                    .saturating_add(cost.cache_write_cost_micros)
            }),
        },
        dependency::DependencyEvent::Cancelled => HarnessDataEvent::Cancelled,
        dependency::DependencyEvent::Failed {
            code,
            message,
            retryable,
        } => HarnessDataEvent::Failed {
            code,
            message,
            retryable,
        },
    }
}

fn map_stream(mut dependency: dependency::DependencyEventStream) -> HarnessDataEventStream {
    let (sender, receiver) = mpsc::channel(16);
    tokio::spawn(async move {
        while let Some(event) = dependency.next().await {
            let mapped = event
                .map(map_event)
                .map_err(|_| HarnessDataError::Unavailable);
            if sender.send(mapped).await.is_err() {
                break;
            }
        }
    });
    HarnessDataEventStream { receiver }
}
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum HarnessDataError {
    #[error("harness data unavailable")]
    Unavailable,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct MockDependency {
        commands: Mutex<Vec<dependency::DependencyCommand>>,
    }

    #[async_trait]
    impl dependency::HarnessDependencyPort for MockDependency {
        async fn exchange(
            &self,
            command: dependency::DependencyCommand,
        ) -> Result<dependency::DependencyReply, dependency::HarnessDependencyError> {
            self.commands.lock().expect("commands").push(command);
            Ok(dependency::DependencyReply::Events(vec![
                dependency::DependencyEvent::Text("mapped".into()),
                dependency::DependencyEvent::Completed {
                    reason: "stop".into(),
                    usage: dependency::DependencyUsage {
                        input_tokens: 3,
                        output_tokens: 1,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                        reasoning_tokens: 0,
                        estimated: false,
                        cost: None,
                    },
                },
            ]))
        }

        async fn exchange_events(
            &self,
            _command: dependency::DependencyCommand,
        ) -> Result<dependency::DependencyEventStream, dependency::HarnessDependencyError> {
            Err(dependency::HarnessDependencyError::InvalidRequest)
        }

        async fn shutdown(&self) {}
    }

    #[tokio::test]
    async fn maps_business_dataset_to_dependency_and_normalizes_reply() {
        let data = HarnessData::new(MockDependency {
            commands: Mutex::new(Vec::new()),
        });
        let response = data
            .exchange(HarnessDataCommand::Execute {
                harness_id: String::from("native"),
                session_id: "session".into(),
                provider: "provider".into(),
                model: "model".into(),
                entries: vec![HarnessDataEntry::User("hello".into())],
                options: serde_json::json!({}),
                grant: "grant".into(),
                cancellation_id: "cancel".into(),
            })
            .await
            .expect("mapped exchange");
        assert_eq!(
            response,
            HarnessDataReply::Events(vec![
                HarnessDataEvent::Text("mapped".into()),
                HarnessDataEvent::Completed {
                    reason: "stop".into(),
                    input_tokens: 3,
                    output_tokens: 1,
                    reasoning_tokens: 0,
                    estimated: false,
                    cost_micros: 0,
                }
            ])
        );
        let commands = data.dependency.commands.lock().expect("commands");
        assert!(matches!(
            commands.as_slice(),
            [dependency::DependencyCommand::Execute { entries, .. }]
                if matches!(entries.as_slice(), [dependency::DependencyEntry::User(value)] if value == "hello")
        ));
    }
}
