//! Style-selected plugin activation and blocking-pipeline composition.
#![allow(
    missing_docs,
    reason = "logic-local plugin commands and records remain boundary-specific"
)]

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use agentmod_event_pipeline::{
    BlockingInterceptor, BlockingPipeline, BlockingPipelineBuilder, Decision, FailurePolicy,
    InterceptorError, InterceptorRegistration, OrderingSpec,
};
use agentmod_runtime_data::plugin::{
    ActivatePluginsDataRequest, ActivatedPluginDataRecord, InvokePluginDataRequest,
    ObservePluginDataRequest, PluginDataError, PluginDataPort, PluginDecisionDataRecord,
};
use agentmod_session_style_sdk::{
    CompiledSessionStyle, DecisionCapability, InterceptorDeclaration,
};
use async_trait::async_trait;
use serde_json::json;
use thiserror::Error;

use crate::action::ActionProposal;

#[derive(Clone, Debug)]
pub struct ComposePluginPipelineCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub compiled_style: CompiledSessionStyle,
    pub runtime_api_version: String,
}

#[derive(Clone)]
pub struct ComposedPluginPipeline {
    pub pipeline: Arc<BlockingPipeline<ActionProposal>>,
    pub activated_plugin_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommittedPluginEvent {
    pub event_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct ObserveCommittedPluginEventsCommand {
    pub session_id: String,
    pub cancellation_id: String,
    pub compiled_style: CompiledSessionStyle,
    pub runtime_api_version: String,
    pub events: Vec<CommittedPluginEvent>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PluginObservationSummary {
    pub enqueued: u64,
    pub dropped: u64,
    /// Per-delivery canonical audit records (attempted/dropped).
    pub audits: Vec<PluginAuditRecord>,
}

/// Canonical plugin delivery audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginAuditRecord {
    pub plugin_id: String,
    pub invocation_id: Option<String>,
    pub operation: String,
    pub outcome: String,
    pub attempts: u8,
}

/// Event types that are never deliverable to observers (prevents canonical
/// audit recursion).
const NON_DELIVERABLE_EVENT_TYPES: &[&str] = &["plugin.audit_recorded"];

fn deliverable(event_type: &str) -> bool {
    !NON_DELIVERABLE_EVENT_TYPES.contains(&event_type)
}

#[async_trait]
pub trait PluginCompositionLogicPort: Send + Sync {
    async fn compose_pipeline(
        &self,
        command: ComposePluginPipelineCommand,
    ) -> Result<ComposedPluginPipeline, PluginCompositionError>;

    async fn observe_committed_events(
        &self,
        command: ObserveCommittedPluginEventsCommand,
    ) -> Result<PluginObservationSummary, PluginCompositionError>;
}

#[derive(Clone)]
pub struct PluginCompositionLogic<D> {
    data: D,
}

impl<D> PluginCompositionLogic<D> {
    #[must_use]
    pub const fn new(data: D) -> Self {
        Self { data }
    }
}

#[async_trait]
impl<D> PluginCompositionLogicPort for PluginCompositionLogic<D>
where
    D: Clone + PluginDataPort + Send + Sync + 'static,
{
    async fn compose_pipeline(
        &self,
        command: ComposePluginPipelineCommand,
    ) -> Result<ComposedPluginPipeline, PluginCompositionError> {
        let declarations = command
            .compiled_style
            .interceptors
            .iter()
            .filter(|declaration| !runtime_owned(&declaration.owner))
            .cloned()
            .collect::<Vec<_>>();
        for declaration in &declarations {
            if declaration.supported_decisions.iter().any(|decision| {
                !matches!(
                    decision,
                    DecisionCapability::Continue
                        | DecisionCapability::Replace
                        | DecisionCapability::Reject
                )
            }) {
                return Err(PluginCompositionError::UnsupportedDecision);
            }
        }
        let activated = self
            .data
            .activate_plugins(ActivatePluginsDataRequest {
                session_id: command.session_id.clone(),
                plugin_ids: command.compiled_style.allowed_plugins.clone(),
                runtime_api_version: command.runtime_api_version,
                capabilities: command
                    .compiled_style
                    .required_capabilities
                    .iter()
                    .cloned()
                    .collect(),
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginCompositionError::Data)?;
        let activated_plugin_ids = activated.plugin_ids;
        let plugins: BTreeMap<_, _> = activated
            .plugins
            .into_iter()
            .map(|plugin| (plugin.id.clone(), plugin))
            .collect();
        let mut builder = BlockingPipelineBuilder::new();
        for declaration in declarations {
            let plugin = plugins
                .get(&declaration.owner)
                .ok_or(PluginCompositionError::Unavailable)?
                .clone();
            if plugin.class != "blocking"
                || !plugin.subscribed_events.contains(&declaration.event)
                || plugin.timeout_ms == 0
            {
                return Err(PluginCompositionError::Incompatible);
            }
            builder.register(InterceptorRegistration::new(
                ordering(&declaration),
                Duration::from_millis(plugin.timeout_ms),
                failure_policy(&plugin)?,
                Arc::new(RuntimePluginInterceptor {
                    data: self.data.clone(),
                    session_id: command.session_id.clone(),
                    cancellation_id: command.cancellation_id.clone(),
                    plugin_id: plugin.id,
                    declaration,
                }),
            ));
        }
        let pipeline = builder
            .compile()
            .map(Arc::new)
            .map_err(|_| PluginCompositionError::Ordering)?;
        Ok(ComposedPluginPipeline {
            pipeline,
            activated_plugin_ids,
        })
    }

    async fn observe_committed_events(
        &self,
        command: ObserveCommittedPluginEventsCommand,
    ) -> Result<PluginObservationSummary, PluginCompositionError> {
        if command.events.is_empty() {
            return Ok(PluginObservationSummary::default());
        }
        let activated = self
            .data
            .activate_plugins(ActivatePluginsDataRequest {
                session_id: command.session_id.clone(),
                plugin_ids: command.compiled_style.allowed_plugins.clone(),
                runtime_api_version: command.runtime_api_version,
                capabilities: command
                    .compiled_style
                    .required_capabilities
                    .iter()
                    .cloned()
                    .collect(),
                cancellation_id: command.cancellation_id.clone(),
            })
            .await
            .map_err(PluginCompositionError::Data)?;
        let observers = activated
            .plugins
            .into_iter()
            .filter(|plugin| plugin.class == "observer")
            .collect::<Vec<_>>();
        let mut summary = PluginObservationSummary::default();
        for event in command.events {
            for observer in observers.iter().filter(|plugin| {
                deliverable(&event.event_type)
                    && plugin.subscribed_events.contains(&event.event_type)
            }) {
                let result = self
                    .data
                    .observe_event(ObservePluginDataRequest {
                        session_id: command.session_id.clone(),
                        plugin_id: observer.id.clone(),
                        invocation_id: format!("observer-{}-{}", event.event_id, observer.id),
                        handler: format!("observe:{}", event.event_type),
                        event_type: event.event_type.clone(),
                        event: json!({
                            "event_id": event.event_id,
                            "sequence": event.sequence,
                            "event_type": event.event_type,
                            "payload": event.payload,
                        }),
                        event_range_start: event.sequence,
                        event_range_end: event.sequence,
                        cancellation_id: command.cancellation_id.clone(),
                    })
                    .await
                    .map_err(PluginCompositionError::Data)?;
                summary.audits.push(PluginAuditRecord {
                    plugin_id: observer.id.clone(),
                    invocation_id: Some(format!("observer-{}-{}", event.event_id, observer.id)),
                    operation: String::from("observe"),
                    outcome: if result.accepted {
                        String::from("observer_delivery_attempted")
                    } else {
                        String::from("observer_delivery_dropped")
                    },
                    attempts: 1,
                });
                if result.accepted {
                    summary.enqueued = summary.enqueued.saturating_add(1);
                } else {
                    summary.dropped = summary.dropped.saturating_add(result.dropped);
                }
            }
        }
        Ok(summary)
    }
}

struct RuntimePluginInterceptor<D> {
    data: D,
    session_id: String,
    cancellation_id: String,
    plugin_id: String,
    declaration: InterceptorDeclaration,
}

#[async_trait]
impl<D> BlockingInterceptor<ActionProposal> for RuntimePluginInterceptor<D>
where
    D: PluginDataPort + Send + Sync,
{
    async fn intercept(
        &self,
        proposal: ActionProposal,
    ) -> Result<Decision<ActionProposal>, InterceptorError> {
        if self.declaration.event != "action.proposed"
            && self.declaration.event != format!("{}.proposed", proposal.action.kind())
        {
            return Ok(Decision::Continue(proposal));
        }
        let value = serde_json::to_value(&proposal)
            .map_err(|_| InterceptorError::new("plugin proposal serialization failed"))?;
        let invocation_id = uuid::Uuid::now_v7().to_string();
        let decision = self
            .data
            .invoke_plugin(InvokePluginDataRequest {
                session_id: self.session_id.clone(),
                plugin_id: self.plugin_id.clone(),
                invocation_id,
                handler: self.declaration.id.clone(),
                proposal_type: self.declaration.event.clone(),
                proposal: value,
                readable_state: json!({
                    "session_id": self.session_id,
                    "style": proposal.style,
                    "workspace": proposal.workspace,
                }),
                cancellation_id: self.cancellation_id.clone(),
            })
            .await
            .map_err(|error| InterceptorError::new(error.to_string()))?;
        match decision {
            PluginDecisionDataRecord::Continue(value) => {
                let returned = decode_proposal(value)?;
                validate_identity(&proposal, &returned)?;
                Ok(Decision::Continue(returned))
            }
            PluginDecisionDataRecord::Replace(value) => {
                let returned = decode_proposal(value)?;
                validate_identity(&proposal, &returned)?;
                Ok(Decision::Replace(returned))
            }
            PluginDecisionDataRecord::Reject(reason) => Ok(Decision::Reject { reason }),
        }
    }
}

fn decode_proposal(value: serde_json::Value) -> Result<ActionProposal, InterceptorError> {
    serde_json::from_value(value)
        .map_err(|_| InterceptorError::new("plugin returned an invalid typed proposal"))
}

fn validate_identity(
    original: &ActionProposal,
    returned: &ActionProposal,
) -> Result<(), InterceptorError> {
    if original.id != returned.id
        || original.style != returned.style
        || original.workspace != returned.workspace
    {
        return Err(InterceptorError::new(
            "plugin changed immutable proposal identity or scope",
        ));
    }
    Ok(())
}

fn runtime_owned(owner: &str) -> bool {
    owner == "runtime" || owner.starts_with("runtime.")
}

fn ordering(declaration: &InterceptorDeclaration) -> OrderingSpec {
    let mut ordering = OrderingSpec::new(declaration.id.as_str(), declaration.owner.as_str())
        .with_stage(declaration.stage)
        .with_priority(declaration.priority);
    for before in &declaration.before {
        ordering = ordering.before(before.as_str());
    }
    for after in &declaration.after {
        ordering = ordering.after(after.as_str());
    }
    ordering
}

fn failure_policy(
    plugin: &ActivatedPluginDataRecord,
) -> Result<FailurePolicy, PluginCompositionError> {
    match plugin.failure_policy.as_str() {
        "reject" => Ok(FailurePolicy::Reject),
        "cancel" => Ok(FailurePolicy::Cancel),
        "continue" => Ok(FailurePolicy::ContinueUnchanged),
        "retry" | "disable" => Ok(FailurePolicy::Abort),
        _ => Err(PluginCompositionError::Incompatible),
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PluginCompositionError {
    #[error("plugin data operation failed: {0}")]
    Data(PluginDataError),
    #[error("style-selected plugin is unavailable")]
    Unavailable,
    #[error("style-selected plugin is incompatible with its declaration")]
    Incompatible,
    #[error("plugin interceptor requests an unsupported decision")]
    UnsupportedDecision,
    #[error("plugin interceptor ordering is invalid")]
    Ordering,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{Arc, Mutex},
    };

    use agentmod_event_pipeline::{ActionCapabilities, ExecutionOutcome};
    use agentmod_primitives::ContentHash;
    use agentmod_runtime_data::plugin::{ActivatedPluginsDataRecord, PluginObservationDataRecord};
    use agentmod_session_style_sdk::{
        BuiltInStyle, CompileContext, DecisionCapability, InterceptorDeclaration,
        StyleCompilerLimits, built_in_manifest, compile_style,
    };
    use serde_json::json;

    use crate::action::{ConsequentialAction, ProposalId, ToolCallAction};

    use super::*;

    #[derive(Clone, Default)]
    struct FixtureData {
        invocations: Arc<Mutex<Vec<InvokePluginDataRequest>>>,
        observations: Arc<Mutex<Vec<ObservePluginDataRequest>>>,
    }

    #[async_trait]
    impl PluginDataPort for FixtureData {
        async fn activate_plugins(
            &self,
            request: ActivatePluginsDataRequest,
        ) -> Result<ActivatedPluginsDataRecord, PluginDataError> {
            Ok(ActivatedPluginsDataRecord {
                plugin_ids: request.plugin_ids.clone(),
                plugins: request
                    .plugin_ids
                    .into_iter()
                    .map(|id| ActivatedPluginDataRecord {
                        class: if id == "fixture.observer" {
                            String::from("observer")
                        } else {
                            String::from("blocking")
                        },
                        subscribed_events: if id == "fixture.observer" {
                            BTreeSet::from([String::from("tool.execution_completed")])
                        } else {
                            BTreeSet::from([String::from("action.proposed")])
                        },
                        timeout_ms: 1_000,
                        failure_policy: if id == "fixture.observer" {
                            String::from("continue")
                        } else {
                            String::from("reject")
                        },
                        id,
                    })
                    .collect(),
            })
        }

        async fn invoke_plugin(
            &self,
            request: InvokePluginDataRequest,
        ) -> Result<PluginDecisionDataRecord, PluginDataError> {
            self.invocations
                .lock()
                .expect("invocations")
                .push(request.clone());
            let mut proposal: ActionProposal =
                serde_json::from_value(request.proposal).expect("typed proposal");
            let ConsequentialAction::ToolCall(action) = &mut proposal.action else {
                panic!("tool call")
            };
            action.arguments = json!({"path":"rewritten.txt"});
            Ok(PluginDecisionDataRecord::Replace(
                serde_json::to_value(proposal).expect("proposal json"),
            ))
        }

        async fn observe_event(
            &self,
            request: agentmod_runtime_data::plugin::ObservePluginDataRequest,
        ) -> Result<PluginObservationDataRecord, PluginDataError> {
            self.observations
                .lock()
                .expect("observations")
                .push(request);
            Ok(PluginObservationDataRecord {
                accepted: true,
                queue_depth: 0,
                dropped: 0,
            })
        }

        async fn execute_plugin_node(
            &self,
            _request: agentmod_runtime_data::plugin::ExecutePluginNodeDataRequest,
        ) -> Result<(serde_json::Value, u8), PluginDataError> {
            Err(PluginDataError::Unavailable)
        }

        async fn plugin_memory(
            &self,
            _operation: String,
            _request: agentmod_runtime_data::plugin::PluginMemoryDataRequest,
        ) -> Result<(agentmod_runtime_data::plugin::PluginMemoryDataResult, u8), PluginDataError>
        {
            Err(PluginDataError::Unavailable)
        }

        async fn plugin_compaction_propose(
            &self,
            _request: agentmod_runtime_data::plugin::PluginCompactionDataRequest,
        ) -> Result<(serde_json::Value, u64, u8), PluginDataError> {
            Err(PluginDataError::Unavailable)
        }

        async fn plugin_context_transform(
            &self,
            _request: agentmod_runtime_data::plugin::PluginContextTransformDataRequest,
        ) -> Result<(serde_json::Value, u8), PluginDataError> {
            Err(PluginDataError::Unavailable)
        }

        async fn plugin_state_change(
            &self,
            _operation: &str,
            _request: agentmod_runtime_data::plugin::PluginStateChangeDataRequest,
        ) -> Result<agentmod_runtime_data::plugin::PluginAuditDataRecord, PluginDataError> {
            Err(PluginDataError::Unavailable)
        }

        async fn plugin_audits(
            &self,
            _session_id: String,
        ) -> Result<Vec<agentmod_runtime_data::plugin::PluginAuditDataRecord>, PluginDataError>
        {
            Err(PluginDataError::Unavailable)
        }

        async fn plugin_health(
            &self,
            _session_id: String,
        ) -> Result<agentmod_runtime_data::plugin::PluginHealthDataRecord, PluginDataError>
        {
            Err(PluginDataError::Unavailable)
        }
    }

    fn compiled_style() -> CompiledSessionStyle {
        let mut manifest = built_in_manifest(BuiltInStyle::PersistentChat);
        manifest.allowed_plugins = vec![String::from("fixture.rewriter")];
        manifest.interceptors = vec![InterceptorDeclaration {
            id: String::from("rewrite-tool"),
            owner: String::from("fixture.rewriter"),
            event: String::from("action.proposed"),
            stage: 10,
            priority: 5,
            before: Vec::new(),
            after: Vec::new(),
            supported_decisions: vec![
                DecisionCapability::Continue,
                DecisionCapability::Replace,
                DecisionCapability::Reject,
            ],
            required_capabilities: Vec::new(),
        }];
        compile_style(
            &manifest,
            &CompileContext {
                runtime_api_version: String::from("1.0.0"),
                plugin_set_hash: ContentHash::digest(b"fixture"),
                capabilities: [
                    "agents",
                    "approval",
                    "artifacts",
                    "context",
                    "continuations",
                    "events",
                    "model",
                    "scheduling",
                    "tools",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
                tool_groups: BTreeMap::from([
                    (
                        String::from("filesystem"),
                        BTreeSet::from([String::from("filesystem.read")]),
                    ),
                    (
                        String::from("process"),
                        BTreeSet::from([String::from("process.run")]),
                    ),
                ]),
                providers: BTreeSet::from([String::from("mock")]),
                plugins: BTreeSet::from([String::from("fixture.rewriter")]),
                memory_providers: ["none", "file", "sqlite-fts"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                compaction_strategies: BTreeSet::from([
                    String::from("artifact_handoff"),
                    String::from("none"),
                    String::from("sliding_window"),
                    String::from("summary"),
                    String::from("tool_output_eviction"),
                ]),
                supported_decisions: BTreeSet::from([
                    DecisionCapability::Continue,
                    DecisionCapability::Replace,
                    DecisionCapability::Reject,
                    DecisionCapability::RequireApproval,
                    DecisionCapability::Defer,
                    DecisionCapability::Cancel,
                    DecisionCapability::Fork,
                ]),
                graph_references: BTreeMap::new(),
            },
            StyleCompilerLimits::default(),
        )
        .expect("compiled style")
    }

    #[tokio::test]
    async fn style_selected_plugin_rewrites_typed_proposal_in_compiled_order() {
        let data = FixtureData::default();
        let pipeline = PluginCompositionLogic::new(data.clone())
            .compose_pipeline(ComposePluginPipelineCommand {
                session_id: String::from("01900000-0000-7000-8000-000000000001"),
                cancellation_id: String::from("01900000-0000-7000-8000-000000000002"),
                compiled_style: compiled_style(),
                runtime_api_version: String::from("1.0.0"),
            })
            .await
            .expect("pipeline")
            .pipeline;
        let proposal = ActionProposal {
            id: ProposalId(String::from("proposal-1")),
            action: ConsequentialAction::ToolCall(ToolCallAction {
                tool: String::from("filesystem.read"),
                group: String::from("filesystem"),
                arguments: json!({"path":"original.txt"}),
                source: None,
            }),
            style: String::from("persistent-chat"),
            workspace: String::from("repo"),
            origin: String::from("runtime"),
        };
        let report = pipeline.execute(proposal, ActionCapabilities::all()).await;
        assert!(matches!(
            report.steps[0].result,
            agentmod_event_pipeline::ExecutionStepResult::Decision(Decision::Replace(_))
        ));
        let ExecutionOutcome::Decision(Decision::Continue(rewritten)) = report.outcome else {
            panic!("transformed continuation")
        };
        let ConsequentialAction::ToolCall(action) = rewritten.action else {
            panic!("tool")
        };
        assert_eq!(action.arguments, json!({"path":"rewritten.txt"}));
        assert_eq!(data.invocations.lock().expect("invocations").len(), 1);
    }

    #[tokio::test]
    async fn observer_receives_only_matching_committed_events() {
        let data = FixtureData::default();
        let mut style = compiled_style();
        style.allowed_plugins.push(String::from("fixture.observer"));
        let summary = PluginCompositionLogic::new(data.clone())
            .observe_committed_events(ObserveCommittedPluginEventsCommand {
                session_id: String::from("01900000-0000-7000-8000-000000000001"),
                cancellation_id: String::from("observer-range-2"),
                compiled_style: style,
                runtime_api_version: String::from("0.1.0"),
                events: vec![
                    CommittedPluginEvent {
                        event_id: String::from("01900000-0000-7000-8000-000000000010"),
                        sequence: 1,
                        event_type: String::from("model.request_started"),
                        payload: json!({"event":"model_request_started"}),
                    },
                    CommittedPluginEvent {
                        event_id: String::from("01900000-0000-7000-8000-000000000011"),
                        sequence: 2,
                        event_type: String::from("tool.execution_completed"),
                        payload: json!({"event":"tool_execution_completed"}),
                    },
                ],
            })
            .await
            .expect("observer delivery");
        assert_eq!(summary.enqueued, 1);
        assert_eq!(summary.dropped, 0);
        let observations = data.observations.lock().expect("observations");
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].event_type, "tool.execution_completed");
    }
}
