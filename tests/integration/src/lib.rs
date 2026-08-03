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
    use agentmod_runtime_data::continuation::ContinuationData;
    use agentmod_runtime_data::{RuntimeData, node_executor::RuntimeNodeExecutorData};
    use agentmod_runtime_dependency::{
        LocalRuntimeDependencies, continuation::FileContinuationDependency,
    };
    use agentmod_runtime_logic::continuation::{
        ContinuationLogic, ContinuationLogicPort, ContinuationPayload, ContinuationWakeCondition,
        CreateContinuationCommand,
    };
    use agentmod_runtime_protocol::{
        RuntimeExecutionBudgetOverrides, RuntimeRequest, RuntimeResponse,
    };
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

    fn runtime_data() -> RuntimeData<LocalRuntimeDependencies> {
        RuntimeData::new(LocalRuntimeDependencies).with_node_executors(
            RuntimeNodeExecutorData::native().expect("native node-executor registry"),
        )
    }

    fn runtime_logic() -> RuntimeLogic<RuntimeData<LocalRuntimeDependencies>> {
        RuntimeLogic::new(runtime_data())
    }

    fn assert_exact_builtin_style_catalog(
        styles: &[agentmod_runtime_protocol::RuntimeStyleSummary],
    ) {
        assert_eq!(
            styles
                .iter()
                .map(|style| (style.id.as_str(), style.version.as_str()))
                .collect::<Vec<_>>(),
            [
                ("declarative-graph", "1.1.0"),
                ("declarative-graph", "1.2.0"),
                ("ephemeral-turn", "1.1.0"),
                ("ephemeral-turn", "1.2.0"),
                ("persistent-chat", "1.1.0"),
                ("persistent-chat", "1.2.0"),
                ("planner-worker", "1.1.0"),
                ("planner-worker", "1.2.0"),
                ("planner-worker", "1.3.0"),
                ("planner-worker", "1.4.0"),
                ("research-loop", "1.1.0"),
                ("research-loop", "1.2.0"),
                ("research-loop", "1.3.0"),
            ]
        );
        assert!(styles.iter().all(|style| {
            style.availability == agentmod_runtime_protocol::RuntimeStyleAvailability::Available
                && style.source == agentmod_runtime_protocol::RuntimeStyleSourceKind::BuiltIn
                && style.style_content_hash.len() == 64
                && style.compiled_cache_key.len() == 64
        }));
    }

    #[test]
    fn runtime_endpoint_creates_and_lists_a_complete_durable_session() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: storage.path().join("sessions"),
                version: String::from("test"),
                styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(
                    &storage.path().join("sessions"),
                ),
            },
        );
        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
                harness: Some(String::from("native")),
                memory: None,
                compaction: None,
                budgets: None,
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
            "execution-plan.json",
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
    fn session_style_registry_binds_exact_styles_and_fails_closed_when_disabled() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions_root = storage.path().join("sessions");
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: sessions_root.clone(),
                version: String::from("test"),
                styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(&sessions_root),
            },
        );

        let RuntimeResponse::Styles { styles } = service
            .handle_wire(&RuntimeRequest::ListStyles)
            .expect("list built-in styles")
        else {
            panic!("styles response")
        };
        assert_exact_builtin_style_catalog(&styles);

        let create = |style: &str| {
            let RuntimeResponse::SessionCreated { session_id } = service
                .handle_wire(&RuntimeRequest::CreateSession {
                    workspace: workspace.path().display().to_string(),
                    style: style.to_owned(),
                    harness: None,
                    memory: None,
                    compaction: None,
                    budgets: None,
                })
                .expect("create style-bound session")
            else {
                panic!("created response")
            };
            session_id
        };
        let persistent = create("persistent-chat");
        let ephemeral = create("ephemeral-turn@1.1.0");

        for (session_id, expected_style, expected_version) in [
            (persistent, "persistent-chat", "1.2.0"),
            (ephemeral, "ephemeral-turn", "1.1.0"),
        ] {
            let RuntimeResponse::SessionInspected { state, .. } = service
                .handle_wire(&RuntimeRequest::InspectSession {
                    session_id,
                    at: None,
                })
                .expect("inspect style-bound session")
            else {
                panic!("inspection response")
            };
            assert_eq!(state["style_binding"]["id"], expected_style);
            assert_eq!(state["style_binding"]["version"], expected_version);
            assert_eq!(state["style_binding"]["harness"], "native");
            assert_eq!(state["style_compatibility"]["status"], "compatible");
            for key in [
                "content_hash",
                "compiled_cache_key",
                "compiled_style_hash",
                "plugin_set_hash",
                "capability_set_hash",
            ] {
                assert!(
                    state["style_binding"][key]
                        .as_str()
                        .is_some_and(|value| value.len() == 64),
                    "{key}"
                );
            }
        }

        let disabled_root = storage.path().join("styles").join("user");
        std::fs::create_dir_all(&disabled_root).expect("create user style root");
        std::fs::write(disabled_root.join("persistent-chat.disabled"), b"disabled")
            .expect("disable exact style");

        let error = service
            .validate_session_style_compatibility(persistent)
            .expect_err("disabled persisted style must fail closed");
        assert!(error.to_string().contains("disabled"));
        service
            .validate_session_style_compatibility(ephemeral)
            .expect("unrelated persisted style remains compatible");

        let RuntimeResponse::SessionInspected { state, .. } = service
            .handle_wire(&RuntimeRequest::InspectSession {
                session_id: persistent,
                at: None,
            })
            .expect("incompatible inspection remains available")
        else {
            panic!("inspection response")
        };
        assert_eq!(state["style_binding"]["id"], "persistent-chat");
        assert_eq!(state["style_compatibility"]["status"], "incompatible");
        assert!(
            state["style_compatibility"]["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains("disabled"))
        );
    }

    #[test]
    fn harness_registry_selects_adapters_and_rejects_missing_capabilities() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions_root = storage.path().join("sessions");
        let mut styles =
            agentmod_runtime_service::RuntimeStyleServiceConfig::native(&sessions_root);
        styles.plugin_style_roots.push(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("fixtures")
                .join("styles"),
        );
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: sessions_root.clone(),
                version: String::from("test"),
                styles,
            },
        );

        let RuntimeResponse::Harnesses { harnesses } = service
            .handle_wire(&RuntimeRequest::ListHarnesses)
            .expect("list harnesses")
        else {
            panic!("harness list response")
        };
        assert_eq!(
            harnesses
                .iter()
                .map(|harness| harness.id.as_str())
                .collect::<Vec<_>>(),
            vec!["fixture", "native"]
        );
        let fixture = harnesses
            .iter()
            .find(|harness| harness.id == "fixture")
            .expect("fixture harness");
        assert_eq!(fixture.availability, "available");
        assert!(fixture.capabilities.contains(&String::from("tool_calls")));
        assert!(!fixture.capabilities.contains(&String::from("images")));
        assert_eq!(fixture.capability_set_hash.len(), 64);

        let RuntimeResponse::HarnessInspected { harness } = service
            .handle_wire(&RuntimeRequest::InspectHarness {
                id: String::from("fixture"),
            })
            .expect("inspect fixture harness")
        else {
            panic!("harness inspection response")
        };
        assert_eq!(harness, *fixture);

        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
                harness: Some(String::from("fixture")),
                memory: None,
                compaction: None,
                budgets: None,
            })
            .expect("select compatible fixture harness")
        else {
            panic!("created response")
        };
        let RuntimeResponse::SessionInspected { state, .. } = service
            .handle_wire(&RuntimeRequest::InspectSession {
                session_id,
                at: None,
            })
            .expect("inspect fixture-bound session")
        else {
            panic!("inspection response")
        };
        assert_eq!(state["style_binding"]["harness"], "fixture");
        assert_eq!(state["style_binding"]["harness_version"], "1.0.0");
        assert_eq!(
            state["style_binding"]["harness_capability_set_hash"],
            fixture.capability_set_hash
        );
        assert_initial_style_introspection(&state);

        let error = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("fixture-harness-incompatible"),
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
            })
            .expect_err("fixture cannot satisfy the style's image requirement");
        assert!(error.to_string().contains("images"), "{error}");
    }

    fn assert_initial_style_introspection(state: &serde_json::Value) {
        let introspection = &state["style_introspection"];
        assert_eq!(introspection["style"]["id"], "persistent-chat");
        assert_eq!(introspection["harness"]["id"], "fixture");
        assert_eq!(introspection["graph"]["entry_node"], "prepare-context");
        assert_eq!(
            introspection["remaining_budgets"]["steps"],
            state["style_binding"]["budgets"]["max_steps"]
        );
        assert_eq!(
            introspection["pipeline"]["blocking_interceptor_order"],
            state["style_binding"]["interceptor_order"]
        );
        assert_eq!(
            introspection["memory"]["selection"]["provider"],
            state["style_binding"]["memory"]["provider"]
        );
        for collection in ["nodes", "transitions"] {
            assert!(
                introspection["graph"][collection]
                    .as_array()
                    .is_some_and(|values| !values.is_empty()),
                "{collection}"
            );
        }
    }

    #[test]
    fn runtime_replay_and_branch_use_fresh_child_history() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: storage.path().join("sessions"),
                version: String::from("test"),
                styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(
                    &storage.path().join("sessions"),
                ),
            },
        );
        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
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
    #[allow(
        clippy::too_many_lines,
        reason = "one vertical acceptance test proves component and budget binding plus restart failures"
    )]
    fn session_component_overrides_recompile_and_validate_the_exact_binding() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let config = RuntimeServiceConfig {
            session_root: storage.path().join("sessions"),
            version: String::from("test"),
            styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(
                &storage.path().join("sessions"),
            ),
        };
        let service = RuntimeService::new(runtime_logic(), config.clone());
        let RuntimeResponse::SessionComponents {
            memory_providers,
            compaction_strategies,
        } = service
            .handle_wire(&RuntimeRequest::ListSessionComponents)
            .expect("list components")
        else {
            panic!("component response")
        };
        assert!(memory_providers.contains(&String::from("sqlite-fts")));
        assert!(compaction_strategies.contains(&String::from("sliding_window")));
        let create = |memory, compaction| {
            let RuntimeResponse::SessionCreated { session_id } = service
                .handle_wire(&RuntimeRequest::CreateSession {
                    workspace: workspace.path().display().to_string(),
                    style: String::from("ephemeral-turn"),
                    harness: None,
                    memory,
                    compaction,
                    budgets: None,
                })
                .expect("create component-selected session")
            else {
                panic!("created response")
            };
            session_id
        };
        let default_id = create(None, None);
        let selected_id = create(
            Some(String::from("sqlite-fts")),
            Some(String::from("sliding_window")),
        );
        let RuntimeResponse::SessionCreated {
            session_id: budget_id,
        } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("ephemeral-turn"),
                harness: None,
                memory: Some(String::from("sqlite-fts")),
                compaction: Some(String::from("sliding_window")),
                budgets: Some(RuntimeExecutionBudgetOverrides {
                    max_iterations: Some(3),
                    max_steps: Some(40),
                    max_tokens: Some(100_000),
                    max_cost_micros: Some(1_000_000),
                    max_duration_ms: Some(60_000),
                }),
            })
            .expect("create budget-selected session")
        else {
            panic!("created response")
        };
        let inspect = |session_id| {
            let RuntimeResponse::SessionInspected { state, .. } = service
                .handle_wire(&RuntimeRequest::InspectSession {
                    session_id,
                    at: None,
                })
                .expect("inspect")
            else {
                panic!("inspection response")
            };
            state
        };
        let default = inspect(default_id);
        let selected = inspect(selected_id);
        let budgeted = inspect(budget_id);
        assert_eq!(default["style_binding"]["memory"]["provider"], "none");
        assert_eq!(
            selected["style_binding"]["memory"]["provider"],
            "sqlite-fts"
        );
        assert_eq!(
            selected["style_binding"]["compaction"]["strategy"],
            "sliding_window"
        );
        assert_ne!(
            selected["style_binding"]["compiled_cache_key"],
            default["style_binding"]["compiled_cache_key"]
        );
        assert_eq!(
            budgeted["style_binding"]["budgets"],
            serde_json::json!({
                "max_iterations": 3,
                "max_steps": 40,
                "max_tokens": 100_000,
                "max_cost_micros": 1_000_000,
                "max_duration_ms": 60_000,
            })
        );
        let compiled: serde_json::Value = serde_json::from_str(
            budgeted["style_binding"]["compiled_style_json"]
                .as_str()
                .expect("compiled JSON"),
        )
        .expect("parse compiled JSON");
        assert_eq!(compiled["graph"]["budget"]["max_tokens"], 100_000);
        assert_ne!(
            budgeted["style_binding"]["compiled_cache_key"],
            selected["style_binding"]["compiled_cache_key"]
        );

        let restarted = RuntimeService::new(runtime_logic(), config);
        restarted
            .validate_session_style_compatibility(selected_id)
            .expect("exact selected binding survives restart");
        restarted
            .validate_session_style_compatibility(budget_id)
            .expect("exact budget-selected binding survives restart");
        let error = restarted
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("ephemeral-turn"),
                harness: None,
                memory: Some(String::from("missing-memory")),
                compaction: None,
                budgets: None,
            })
            .expect_err("unavailable memory must fail");
        assert!(error.to_string().contains("STYLE017"), "{error}");
        let error = restarted
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("ephemeral-turn"),
                harness: None,
                memory: None,
                compaction: None,
                budgets: Some(RuntimeExecutionBudgetOverrides {
                    max_iterations: None,
                    max_steps: Some(0),
                    max_tokens: None,
                    max_cost_micros: None,
                    max_duration_ms: None,
                }),
            })
            .expect_err("zero budget must fail");
        assert!(error.to_string().contains("STYLE020"), "{error}");
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
    #[test]
    fn created_session_retains_immutable_plan_file_and_restart_validates_exactly() {
        use agentmod_runtime_logic::{
            execution_plan::{ExecutionPlanRestartOutcome, validate_session_resume_plan},
            persistence::SessionPersistenceLogicPort,
        };

        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions_root = storage.path().join("sessions");
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: sessions_root.clone(),
                version: String::from("test"),
                styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(&sessions_root),
            },
        );
        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
            })
            .expect("create session")
        else {
            panic!("created response")
        };
        let session_directory = sessions_root.join(session_id.to_string());
        assert!(session_directory.join("execution-plan.json").exists());

        // The persisted plan must survive a fresh data stack restart and
        // validate exactly against the live registry.
        let restarted = runtime_data();
        let loaded =
            agentmod_runtime_logic::persistence::SessionPersistenceLogic::new(restarted.clone())
                .load_session(agentmod_runtime_logic::persistence::LoadSessionCommand {
                    session_directory: session_directory.clone(),
                    expected_session_id: session_id,
                })
                .expect("load persisted session");
        let binding = loaded.state.style_binding.as_ref().expect("style binding");
        let outcome = agentmod_runtime_logic::execution_plan::validate_persisted_execution_plan(
            &restarted,
            &session_directory,
            binding,
        )
        .expect("restart validation");
        assert_eq!(outcome, ExecutionPlanRestartOutcome::Valid);
        validate_session_resume_plan(&restarted, &session_directory, binding)
            .expect("resume validation");

        // A truncated plan file must fail closed with a stable corruption code.
        let path = session_directory.join("execution-plan.json");
        let bytes = std::fs::read(&path).expect("read plan file");
        std::fs::write(&path, &bytes[..bytes.len() / 2]).expect("truncate");
        let error = agentmod_runtime_logic::execution_plan::validate_persisted_execution_plan(
            &restarted,
            &session_directory,
            binding,
        )
        .expect_err("corrupt plan must fail closed");
        assert!(
            matches!(
                error,
                agentmod_runtime_logic::execution_plan::ExecutionPlanLogicError::Data(
                    agentmod_runtime_data::execution_plan::ExecutionPlanDataError::CorruptFile { .. }
                )
            ),
            "unexpected error {error:?}"
        );
    }

    #[test]
    fn branch_with_recompiled_style_creates_distinct_plan_and_leaves_parent_unchanged() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions_root = storage.path().join("sessions");
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: sessions_root.clone(),
                version: String::from("test"),
                styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(&sessions_root),
            },
        );
        let create = |style: &str| {
            let RuntimeResponse::SessionCreated { session_id } = service
                .handle_wire(&RuntimeRequest::CreateSession {
                    workspace: workspace.path().display().to_string(),
                    style: style.to_owned(),
                    harness: None,
                    memory: None,
                    compaction: None,
                    budgets: None,
                })
                .expect("create session")
            else {
                panic!("created response")
            };
            session_id
        };
        let parent = create("persistent-chat");
        let parent_directory = sessions_root.join(parent.to_string());
        let parent_plan =
            std::fs::read(parent_directory.join("execution-plan.json")).expect("parent plan file");
        assert!(!parent_plan.is_empty());

        // Branch with a recompiled style; the child must retain its own plan
        // file and the parent directory must stay byte-identical.
        let RuntimeResponse::SessionBranched {
            session_id: child, ..
        } = service
            .handle_wire(&RuntimeRequest::BranchSession {
                session_id: parent,
                at: agentmod_primitives::Sequence::FIRST,
                style: Some(String::from("ephemeral-turn")),
            })
            .expect("branch with recompiled style")
        else {
            panic!("branched response")
        };
        let child_directory = sessions_root.join(child.to_string());
        let child_plan =
            std::fs::read(child_directory.join("execution-plan.json")).expect("child plan file");
        assert!(!child_plan.is_empty());
        assert_ne!(parent_plan, child_plan, "child plan must be distinct");
        assert_eq!(
            std::fs::read(parent_directory.join("execution-plan.json")).expect("parent plan"),
            parent_plan,
            "parent plan must remain unchanged"
        );
    }

    #[test]
    fn plan_inspection_projection_replays_identity_without_live_registry() {
        let storage = tempfile::tempdir().expect("storage");
        let workspace = tempfile::tempdir().expect("workspace");
        let sessions_root = storage.path().join("sessions");
        let service = RuntimeService::new(
            runtime_logic(),
            RuntimeServiceConfig {
                session_root: sessions_root.clone(),
                version: String::from("test"),
                styles: agentmod_runtime_service::RuntimeStyleServiceConfig::native(&sessions_root),
            },
        );
        let RuntimeResponse::SessionCreated { session_id } = service
            .handle_wire(&RuntimeRequest::CreateSession {
                workspace: workspace.path().display().to_string(),
                style: String::from("persistent-chat"),
                harness: None,
                memory: None,
                compaction: None,
                budgets: None,
            })
            .expect("create session")
        else {
            panic!("created response")
        };
        let session_directory = sessions_root.join(session_id.to_string());

        // Pure replay: inspect with a data stack that has NO live registry.
        let replay_data = RuntimeData::new(LocalRuntimeDependencies);
        let projection = agentmod_runtime_logic::execution_plan::inspect_execution_plan_file(
            &replay_data,
            &session_directory,
        )
        .expect("pure plan inspection");
        assert!(projection.migration.is_none());
        assert_eq!(
            projection.plan_hash.to_hex().len(),
            64,
            "plan hash must be exact"
        );
        assert!(!projection.node_executors.is_empty());
        assert!(
            projection
                .node_executors
                .iter()
                .all(|node| { !node.executor_id.is_empty() && !node.executor_version.is_empty() })
        );

        // The separate live availability pass reports state without changing
        // the pure projection.
        let capabilities =
            agentmod_runtime_logic::node_executor::inspect_node_executor_capabilities(
                &runtime_data(),
            )
            .expect("live capabilities");
        let availability = agentmod_runtime_logic::execution_plan::availability_projection(
            &capabilities,
            &projection,
        );
        assert!(availability.registry_hash_matches);
        assert!(availability.nodes.iter().all(|node| node.compatible));
    }
}
