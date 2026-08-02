//! Production authorization for exact immutable plugin-node invocations.
//!
//! This adapter composes the existing style/plugin interception order with
//! user and mandatory policy. It intentionally returns only a digest and never
//! exposes a grant or crosses the plugin execution boundary.

use std::sync::Arc;

use agentmod_event_pipeline::{ActionCapabilities, BlockingPipeline};
use agentmod_primitives::ContentHash;
use async_trait::async_trait;
use serde::Serialize;

use crate::{
    action::{ActionProposal, ConsequentialAction, PluginNodeInvocationAction},
    interception::{InterceptionOutcome, InterceptorAuditResult, intercept_action},
    permission::{PermissionEffect, PermissionPolicy, revalidate_mandatory_after_approval},
    plugin_turn::{
        AuthorizePluginTurnCommand, PluginTurnAuthorization, PluginTurnAuthorizationError,
        PluginTurnAuthorizationPort,
    },
    session::SessionNodeExecutorSource,
};

/// Production policy adapter for one session's immutable interception contract.
#[derive(Clone)]
pub struct ProductionPluginTurnAuthorization {
    style_pipeline: Arc<BlockingPipeline<ActionProposal>>,
    plugin_pipeline: Arc<BlockingPipeline<ActionProposal>>,
    capabilities: ActionCapabilities,
    user_policy: PermissionPolicy,
    mandatory_policy: PermissionPolicy,
}

impl ProductionPluginTurnAuthorization {
    /// Creates the adapter from the already compiled session interception and
    /// policy contract.
    #[must_use]
    pub const fn new(
        style_pipeline: Arc<BlockingPipeline<ActionProposal>>,
        plugin_pipeline: Arc<BlockingPipeline<ActionProposal>>,
        capabilities: ActionCapabilities,
        user_policy: PermissionPolicy,
        mandatory_policy: PermissionPolicy,
    ) -> Self {
        Self {
            style_pipeline,
            plugin_pipeline,
            capabilities,
            user_policy,
            mandatory_policy,
        }
    }
}

/// Authorization adapter used only after the runtime has loaded an exact
/// resolved durable approval continuation. It revalidates the mandatory policy
/// and refuses any invocation/action substitution.
#[derive(Clone)]
pub struct ApprovedPluginTurnAuthorization {
    expected_identity: crate::session::PluginNodeInvocationIdentity,
    expected_action_digest: ContentHash,
    mandatory_policy: PermissionPolicy,
}

impl ApprovedPluginTurnAuthorization {
    /// Binds an approval to one exact immutable invocation and action digest.
    #[must_use]
    pub const fn new(
        expected_identity: crate::session::PluginNodeInvocationIdentity,
        expected_action_digest: ContentHash,
        mandatory_policy: PermissionPolicy,
    ) -> Self {
        Self {
            expected_identity,
            expected_action_digest,
            mandatory_policy,
        }
    }
}

#[async_trait]
impl PluginTurnAuthorizationPort for ApprovedPluginTurnAuthorization {
    async fn authorize_plugin_turn(
        &self,
        command: AuthorizePluginTurnCommand,
    ) -> Result<PluginTurnAuthorization, PluginTurnAuthorizationError> {
        validate_exact_proposal(&command)?;
        if command.identity != self.expected_identity
            || command.action_digest != self.expected_action_digest
        {
            return Err(PluginTurnAuthorizationError::InvalidProposal);
        }
        let permission =
            revalidate_mandatory_after_approval(&command.proposal, &self.mandatory_policy);
        if permission.effect == PermissionEffect::Deny {
            return Err(PluginTurnAuthorizationError::Denied {
                reason: permission.reason,
            });
        }
        let bytes = serde_json::to_vec(&AuthorizationDigestMaterial {
            action_digest: command.action_digest,
            declaration_hash: command.declaration_hash,
            permission: &permission,
        })
        .map_err(|_| PluginTurnAuthorizationError::FailedClosed {
            code: String::from("authorization_digest_failed"),
        })?;
        Ok(PluginTurnAuthorization {
            authorization_digest: ContentHash::digest(&bytes),
        })
    }
}

#[derive(Serialize)]
struct AuthorizationDigestMaterial<'a> {
    action_digest: ContentHash,
    declaration_hash: ContentHash,
    permission: &'a crate::permission::PermissionDecision,
}

#[async_trait]
impl PluginTurnAuthorizationPort for ProductionPluginTurnAuthorization {
    async fn authorize_plugin_turn(
        &self,
        command: AuthorizePluginTurnCommand,
    ) -> Result<PluginTurnAuthorization, PluginTurnAuthorizationError> {
        validate_exact_proposal(&command)?;
        let result = intercept_action(
            command.proposal.clone(),
            &self.style_pipeline,
            &self.plugin_pipeline,
            self.capabilities,
            &self.user_policy,
            &self.mandatory_policy,
        )
        .await;
        if result.audit.iter().any(|step| {
            matches!(
                step.result,
                InterceptorAuditResult::Continue { replaced: true, .. }
            )
        }) {
            return Err(PluginTurnAuthorizationError::ReplacementRejected);
        }
        match result.outcome {
            InterceptionOutcome::Approved {
                executable,
                permission,
            } => {
                if executable != command.proposal
                    || executable.digest().ok() != Some(command.action_digest)
                {
                    return Err(PluginTurnAuthorizationError::ReplacementRejected);
                }
                let material = AuthorizationDigestMaterial {
                    action_digest: command.action_digest,
                    declaration_hash: command.declaration_hash,
                    permission: &permission,
                };
                let bytes = serde_json::to_vec(&material).map_err(|_| {
                    PluginTurnAuthorizationError::FailedClosed {
                        code: String::from("authorization_digest_failed"),
                    }
                })?;
                Ok(PluginTurnAuthorization {
                    authorization_digest: ContentHash::digest(&bytes),
                })
            }
            InterceptionOutcome::RequireApproval {
                proposal,
                reason,
                continuation,
            } if proposal == command.proposal => {
                Err(PluginTurnAuthorizationError::ApprovalRequired {
                    proposal: Box::new(proposal),
                    reason,
                    continuation,
                })
            }
            InterceptionOutcome::RequireApproval { .. } => {
                Err(PluginTurnAuthorizationError::ReplacementRejected)
            }
            InterceptionOutcome::Rejected { reason } => {
                Err(PluginTurnAuthorizationError::Denied { reason })
            }
            InterceptionOutcome::Deferred { .. } => {
                Err(PluginTurnAuthorizationError::FailedClosed {
                    code: String::from("interceptor_deferred"),
                })
            }
            InterceptionOutcome::Cancelled { .. } => {
                Err(PluginTurnAuthorizationError::FailedClosed {
                    code: String::from("interceptor_cancelled"),
                })
            }
            InterceptionOutcome::Forked { .. } => Err(PluginTurnAuthorizationError::FailedClosed {
                code: String::from("interceptor_forked"),
            }),
            InterceptionOutcome::Aborted { .. } => {
                Err(PluginTurnAuthorizationError::FailedClosed {
                    code: String::from("interceptor_aborted"),
                })
            }
        }
    }
}

fn validate_exact_proposal(
    command: &AuthorizePluginTurnCommand,
) -> Result<(), PluginTurnAuthorizationError> {
    if command.proposal.digest().ok() != Some(command.action_digest)
        || command.declaration_hash != command.policy.declaration_hash
    {
        return Err(PluginTurnAuthorizationError::InvalidProposal);
    }
    let SessionNodeExecutorSource::Plugin { plugin_id } = &command.executor_source else {
        return Err(PluginTurnAuthorizationError::InvalidProposal);
    };
    let ConsequentialAction::PluginNodeInvocation(action) = &command.proposal.action else {
        return Err(PluginTurnAuthorizationError::InvalidProposal);
    };
    if !matches_exact_invocation(command, action, plugin_id) {
        return Err(PluginTurnAuthorizationError::InvalidProposal);
    }
    Ok(())
}

fn matches_exact_invocation(
    command: &AuthorizePluginTurnCommand,
    action: &PluginNodeInvocationAction,
    plugin_id: &str,
) -> bool {
    let mut required_permissions = command.policy.required_permissions.clone();
    required_permissions.sort();
    required_permissions.dedup();
    action.plugin_id == plugin_id
        && action.plugin_id == command.identity.plugin_id
        && action.executor_id == command.identity.executor.executor_id
        && action.executor_version == command.identity.executor.executor_version
        && action.invocation_id == command.identity.invocation_id
        && action.invocation_digest == command.identity.invocation_digest
        && action.declaration_hash == command.declaration_hash
        && action.declaration_hash == command.identity.executor.executor_declaration_hash
        && action.external_effects == command.policy.external_effects
        && action.required_permissions == required_permissions
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use agentmod_event_pipeline::{
        BlockingInterceptor, BlockingPipelineBuilder, Decision, FailurePolicy, InterceptorError,
        InterceptorRegistration, OrderingSpec,
    };
    use agentmod_primitives::{EventId, SessionId};
    use uuid::Uuid;

    use crate::{
        action::ProposalId,
        node_execution::NodeWorkIdentity,
        permission::PermissionEffect,
        plugin_turn::PluginNodeInvocationPolicy,
        session::{
            PluginNodeInvocationIdentity, SessionNodeExecutorBoundary,
            SessionNodeExecutorResolution,
        },
    };

    use super::*;

    struct Replace;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for Replace {
        async fn intercept(
            &self,
            mut proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            proposal.origin.push_str(".replaced");
            Ok(Decision::Replace(proposal))
        }
    }

    fn pipeline(
        interceptor: Option<Arc<dyn BlockingInterceptor<ActionProposal>>>,
    ) -> Arc<BlockingPipeline<ActionProposal>> {
        let mut builder = BlockingPipelineBuilder::new();
        if let Some(interceptor) = interceptor {
            builder.register(InterceptorRegistration::new(
                OrderingSpec::new("fixture", "runtime"),
                Duration::from_secs(1),
                FailurePolicy::Abort,
                interceptor,
            ));
        }
        Arc::new(builder.compile().expect("pipeline"))
    }

    fn policy(effect: PermissionEffect) -> PermissionPolicy {
        PermissionPolicy::new("fixture", vec![], effect, "fixture policy")
    }

    fn command() -> AuthorizePluginTurnCommand {
        let declaration_hash = ContentHash::digest(b"declaration");
        let executor = SessionNodeExecutorResolution {
            node_id: String::from("plugin"),
            node_kind: String::from("model_call"),
            executor_id: String::from("fixture.echo"),
            executor_version: String::from("1.0.0"),
            source: SessionNodeExecutorSource::Plugin {
                plugin_id: String::from("fixture.plugin"),
            },
            boundary: SessionNodeExecutorBoundary::PluginHost,
            required_capabilities: vec![String::from("model")],
            resolved_capabilities: vec![String::from("model")],
            runtime_api_requirement: String::from("^1"),
            executor_declaration_hash: declaration_hash,
            adapter_configuration_reference: ContentHash::digest(b"configuration"),
        };
        let identity = PluginNodeInvocationIdentity {
            work: NodeWorkIdentity {
                run_id: String::from("run"),
                node_id: String::from("plugin"),
                branch_path: vec![],
                attempt: 1,
                loop_iteration: 0,
                step: 1,
            },
            executor: executor.clone(),
            configuration_hash: executor.adapter_configuration_reference,
            plugin_id: String::from("fixture.plugin"),
            invocation_id: String::from("invocation"),
            invocation_digest: ContentHash::digest(b"invocation"),
            input_hash: ContentHash::digest(b"input"),
            readable_state_hash: ContentHash::digest(b"state"),
            causation_event_id: EventId::from_uuid(Uuid::from_u128(2)),
        };
        let proposal = ActionProposal {
            id: ProposalId(String::from("plugin-node:invocation")),
            action: ConsequentialAction::PluginNodeInvocation(PluginNodeInvocationAction {
                plugin_id: identity.plugin_id.clone(),
                executor_id: executor.executor_id.clone(),
                executor_version: executor.executor_version.clone(),
                invocation_id: identity.invocation_id.clone(),
                invocation_digest: identity.invocation_digest,
                declaration_hash,
                external_effects: false,
                required_permissions: vec![],
            }),
            style: String::from("user-graph"),
            workspace: String::from("fixture"),
            origin: String::from("plugin:fixture.plugin"),
        };
        AuthorizePluginTurnCommand {
            session_id: SessionId::from_uuid(Uuid::from_u128(1)),
            identity,
            policy: PluginNodeInvocationPolicy {
                declaration_hash,
                idempotent: true,
                external_effects: false,
                max_attempts: 1,
                required_permissions: vec![],
            },
            action_digest: proposal.digest().expect("digest"),
            proposal,
            executor_source: executor.source,
            declaration_hash,
        }
    }

    #[tokio::test]
    async fn exact_unchanged_proposal_is_authorized() {
        let adapter = ProductionPluginTurnAuthorization::new(
            pipeline(None),
            pipeline(None),
            ActionCapabilities::all(),
            policy(PermissionEffect::Allow),
            policy(PermissionEffect::Allow),
        );
        let result = adapter
            .authorize_plugin_turn(command())
            .await
            .expect("authorized");
        assert_ne!(
            result.authorization_digest,
            ContentHash::from_bytes([0; 32])
        );
    }

    #[tokio::test]
    async fn replacement_ask_and_deny_fail_with_typed_results() {
        let replacement = ProductionPluginTurnAuthorization::new(
            pipeline(Some(Arc::new(Replace))),
            pipeline(None),
            ActionCapabilities::all(),
            policy(PermissionEffect::Allow),
            policy(PermissionEffect::Allow),
        );
        assert_eq!(
            replacement
                .authorize_plugin_turn(command())
                .await
                .expect_err("replacement"),
            PluginTurnAuthorizationError::ReplacementRejected
        );

        let ask = ProductionPluginTurnAuthorization::new(
            pipeline(None),
            pipeline(None),
            ActionCapabilities::all(),
            policy(PermissionEffect::Ask),
            policy(PermissionEffect::Allow),
        );
        assert!(matches!(
            ask.authorize_plugin_turn(command()).await,
            Err(PluginTurnAuthorizationError::ApprovalRequired { .. })
        ));

        let deny = ProductionPluginTurnAuthorization::new(
            pipeline(None),
            pipeline(None),
            ActionCapabilities::all(),
            policy(PermissionEffect::Allow),
            policy(PermissionEffect::Deny),
        );
        assert!(matches!(
            deny.authorize_plugin_turn(command()).await,
            Err(PluginTurnAuthorizationError::Denied { .. })
        ));
    }

    #[tokio::test]
    async fn substituted_invocation_is_rejected_before_interception() {
        let adapter = ProductionPluginTurnAuthorization::new(
            pipeline(None),
            pipeline(None),
            ActionCapabilities::all(),
            policy(PermissionEffect::Allow),
            policy(PermissionEffect::Allow),
        );
        let mut command = command();
        let ConsequentialAction::PluginNodeInvocation(action) = &mut command.proposal.action else {
            panic!("plugin invocation")
        };
        action.executor_version = String::from("2.0.0");
        command.action_digest = command.proposal.digest().expect("digest");
        assert_eq!(
            adapter
                .authorize_plugin_turn(command)
                .await
                .expect_err("invalid"),
            PluginTurnAuthorizationError::InvalidProposal
        );
    }
}
