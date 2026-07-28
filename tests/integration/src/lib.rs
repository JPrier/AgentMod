//! Cross-layer acceptance fixtures.

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use agentmod_event_pipeline::{
        ActionCapabilities, BlockingInterceptor, BlockingPipelineBuilder, Decision, FailurePolicy,
        InterceptorError, InterceptorRegistration, OrderingSpec,
    };
    use agentmod_filesystem_host_data::FilesystemData;
    use agentmod_filesystem_host_dependency::{
        DEFAULT_MAX_FILE_BYTES, DependencyRequest, FilesystemAuthorizationConfig,
        FilesystemDependencyConfig, NativeFilesystem, WriteMode, WriteRequest,
        canonical_operation_digest,
    };
    use agentmod_filesystem_host_logic::FilesystemLogic;
    use agentmod_filesystem_host_service::FilesystemService;
    use agentmod_primitives::{ContinuationId, SessionId, TimestampMillis};
    use agentmod_protocol_support::authorization::{
        AuthorizationClaims, AuthorizationKey, seal_authorization,
    };
    use agentmod_runtime_data::RuntimeData;
    use agentmod_runtime_data::continuation::ContinuationData;
    use agentmod_runtime_dependency::{
        LocalRuntimeDependencies, continuation::FileContinuationDependency,
    };
    use agentmod_runtime_logic::continuation::{
        ContinuationLogic, ContinuationLogicPort, ContinuationPayload, ContinuationWakeCondition,
        CreateContinuationCommand,
    };
    use agentmod_runtime_protocol::{RuntimeRequest, RuntimeResponse};
    use agentmod_runtime_service::{
        RuntimeService, RuntimeServiceConfig, continuation::ContinuationService,
    };
    use agentmod_tool_protocol::{ToolHostCommand, ToolHostEvent};
    use async_trait::async_trait;
    use serde_json::json;
    use uuid::Uuid;

    use agentmod_runtime_logic::{
        RuntimeLogic,
        action::{ActionProposal, ConsequentialAction, FilesystemWriteAction, ProposalId},
        interception::{InterceptionOutcome, intercept_action},
        permission::{PermissionEffect, PermissionMatcher, PermissionPolicy, PermissionRule},
    };

    #[test]
    fn runtime_endpoint_creates_and_lists_a_complete_durable_session() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let service = RuntimeService::new(
            RuntimeLogic::new(RuntimeData::new(LocalRuntimeDependencies)),
            RuntimeServiceConfig {
                session_root: storage.path().join("sessions"),
                version: String::from("test"),
            },
        );
        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
            })
            .expect("create through complete layer chain")
        else {
            panic!("created response")
        };
        let session_directory = storage.path().join("sessions").join(session_id.to_string());
        for required in [
            "metadata.json",
            "events.jsonl",
            "style.json",
            "style.lock",
            "workspace.json",
            "continuations",
            "snapshots",
            "artifacts",
            "process-logs",
            "branches",
        ] {
            assert!(session_directory.join(required).exists(), "{required}");
        }
        let RuntimeResponse::Sessions { sessions } = service
            .handle_wire(&RuntimeRequest::ListSessions { limit: 10 })
            .expect("list through complete layer chain")
        else {
            panic!("sessions response")
        };
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, session_id);
        assert_eq!(sessions[0].style, "persistent-chat");
        assert_eq!(sessions[0].sequence.get(), 1);
    }

    #[test]
    fn runtime_replay_and_branch_use_fresh_child_history() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let service = RuntimeService::new(
            RuntimeLogic::new(RuntimeData::new(LocalRuntimeDependencies)),
            RuntimeServiceConfig {
                session_root: storage.path().join("sessions"),
                version: String::from("test"),
            },
        );
        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
            })
            .expect("create parent")
        else {
            panic!("created response")
        };
        let RuntimeResponse::SessionBranched {
            session_id: child_id,
            parent_session_id,
            fork_sequence,
            child_head_sequence,
        } = service
            .handle_wire(&RuntimeRequest::BranchSession {
                session_id,
                at: agentmod_primitives::Sequence::FIRST,
                style: Some(String::from("ephemeral-turn")),
            })
            .expect("branch")
        else {
            panic!("branch response")
        };
        assert_eq!(parent_session_id, session_id);
        assert_eq!(fork_sequence, agentmod_primitives::Sequence::FIRST);
        assert_eq!(child_head_sequence.get(), 2);

        let RuntimeResponse::SessionInspected {
            head_sequence,
            state,
            ..
        } = service
            .handle_wire(&RuntimeRequest::InspectSession {
                session_id: child_id,
                at: None,
            })
            .expect("inspect child")
        else {
            panic!("inspection response")
        };
        assert_eq!(head_sequence.get(), 2);
        assert_eq!(state["style"], "ephemeral-turn");
        assert_eq!(
            state["ancestry"]["parent_session_id"],
            session_id.to_string()
        );
        assert_eq!(state["ancestry"]["fork_sequence"], 1);

        let RuntimeResponse::SessionInspected { head_sequence, .. } = service
            .handle_wire(&RuntimeRequest::ReplaySession {
                session_id,
                at: Some(agentmod_primitives::Sequence::FIRST),
            })
            .expect("replay parent")
        else {
            panic!("inspection response")
        };
        assert_eq!(head_sequence, agentmod_primitives::Sequence::FIRST);
    }

    #[test]
    fn durable_approval_survives_restart_and_resumes_once() {
        let directory = tempfile::tempdir().expect("temp directory");
        let id = ContinuationId::from_uuid(Uuid::from_u128(7));
        let session_id = SessionId::from_uuid(Uuid::from_u128(8));

        let create_logic = ContinuationLogic::new(ContinuationData::new(
            FileContinuationDependency::new(directory.path().into()),
        ));
        create_logic
            .create_continuation(CreateContinuationCommand {
                session_id: session_id.to_string(),
                id,
                wake_condition: ContinuationWakeCondition::Manual,
                payload: ContinuationPayload::Opaque(String::from("integration-fixture")),
                expires_at: None,
            })
            .expect("durably create before approval");

        let restarted_service = ContinuationService::new(ContinuationLogic::new(
            ContinuationData::new(FileContinuationDependency::new(directory.path().into())),
        ));
        let wire_request = RuntimeRequest::ResolveApproval {
            session_id,
            continuation_id: id.to_string(),
            approved: true,
            resume_after_resolution: true,
        };
        assert_eq!(
            restarted_service
                .handle_wire(&wire_request)
                .expect("first approval"),
            RuntimeResponse::ApprovalResolved {
                transitioned: true,
                events: Vec::new(),
                last_committed_sequence: None,
                awaiting_continuation: None,
            }
        );

        let restarted_again = ContinuationService::new(ContinuationLogic::new(
            ContinuationData::new(FileContinuationDependency::new(directory.path().into())),
        ));
        assert_eq!(
            restarted_again
                .handle_wire(&wire_request)
                .expect("idempotent duplicate"),
            RuntimeResponse::ApprovalResolved {
                transitioned: false,
                events: Vec::new(),
                last_committed_sequence: None,
                awaiting_continuation: None,
            }
        );
    }

    struct ReplaceWritePath;

    #[async_trait]
    impl BlockingInterceptor<ActionProposal> for ReplaceWritePath {
        async fn intercept(
            &self,
            mut proposal: ActionProposal,
        ) -> Result<Decision<ActionProposal>, InterceptorError> {
            let ConsequentialAction::FilesystemWrite(write) = &mut proposal.action else {
                return Ok(Decision::Continue(proposal));
            };
            write.path = "safe.txt".into();
            Ok(Decision::Replace(proposal))
        }
    }

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the acceptance regression keeps the complete proposal-to-side-effect evidence explicit"
    )]
    async fn pre_tool_modification_executes_only_modified_filesystem_action() {
        let directory = tempfile::tempdir().expect("temp directory");
        let authorization_key = Arc::new(AuthorizationKey::from_bytes([9; 32]));
        let dependency = NativeFilesystem::new(
            FilesystemDependencyConfig::new(
                vec![directory.path().into()],
                Vec::new(),
                DEFAULT_MAX_FILE_BYTES,
            )
            .expect("filesystem config")
            .with_authorization(FilesystemAuthorizationConfig {
                owner: String::from("integration-owner"),
                session: String::from("integration-session"),
                key: Arc::clone(&authorization_key),
            }),
        );
        let filesystem =
            FilesystemService::new(FilesystemLogic::new(FilesystemData::new(dependency)));

        let original = ActionProposal {
            id: ProposalId("proposal-write-1".into()),
            action: ConsequentialAction::FilesystemWrite(FilesystemWriteAction {
                path: "unsafe.txt".into(),
                expected_hash: None,
                content_hash: agentmod_primitives::ContentHash::digest(b"approved content"),
                overwrite: false,
            }),
            style: "persistent-chat".into(),
            workspace: directory.path().display().to_string(),
            origin: "runtime".into(),
        };
        let mut style_builder = BlockingPipelineBuilder::new();
        style_builder.register(InterceptorRegistration::new(
            OrderingSpec::new("rewrite-path", "built-in-style"),
            Duration::from_secs(1),
            FailurePolicy::Abort,
            Arc::new(ReplaceWritePath),
        ));
        let style = style_builder.compile().expect("style pipeline");
        let plugins = BlockingPipelineBuilder::<ActionProposal>::new()
            .compile()
            .expect("empty plugin pipeline");
        let allow = PermissionPolicy::new(
            "allow-fixture",
            vec![PermissionRule {
                id: "allow-write".into(),
                priority: 1,
                matcher: PermissionMatcher {
                    action: Some("filesystem_write".into()),
                    ..PermissionMatcher::default()
                },
                effect: PermissionEffect::Allow,
                reason: "fixture approval".into(),
            }],
            PermissionEffect::Deny,
            "deny unmatched",
        );
        let intercepted = intercept_action(
            original,
            &style,
            &plugins,
            ActionCapabilities::all(),
            &allow,
            &allow,
        )
        .await;
        assert_eq!(intercepted.original.action.kind(), "filesystem_write");
        assert_eq!(intercepted.audit.len(), 1);
        let InterceptionOutcome::Approved { executable, .. } = intercepted.outcome else {
            panic!("modified proposal must be approved")
        };
        let ConsequentialAction::FilesystemWrite(write) = &executable.action else {
            panic!("filesystem action")
        };
        assert_eq!(write.path, "safe.txt");

        let call_id = String::from("call-write-1");
        let host_digest = canonical_operation_digest(&DependencyRequest::Write(WriteRequest {
            path: write.path.clone(),
            content: b"approved content".to_vec(),
            mode: WriteMode::Create,
            expected_hash: None,
            overwrite: false,
            create_parents: false,
        }))
        .expect("host canonical operation");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let now = i64::try_from(now).expect("clock fits");
        let grant = seal_authorization(
            &AuthorizationClaims {
                owner: String::from("integration-owner"),
                session: String::from("integration-session"),
                call_id: call_id.clone(),
                action: String::from("filesystem.write"),
                normalized_digest: host_digest,
                issued_at: TimestampMillis::new(now - 1),
                expires_at: TimestampMillis::new(now + 30_000),
                nonce: String::from("integration-write-once"),
            },
            authorization_key.as_ref(),
        )
        .expect("grant");
        let events = filesystem.handle_wire(ToolHostCommand::Execute {
            call_id,
            tool: "filesystem.write".into(),
            arguments: json!({
                "path": write.path,
                "content": "approved content",
                "mode": "create",
                "overwrite": false,
                "create_parents": false
            }),
            normalized_digest: host_digest.to_string(),
            authorization_grant: grant,
            cancellation_id: agentmod_primitives::CancellationId::from_uuid(Uuid::from_u128(9)),
        });
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ToolHostEvent::Completed { .. }))
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("safe.txt")).expect("safe output"),
            "approved content"
        );
        assert!(!directory.path().join("unsafe.txt").exists());
    }
}
