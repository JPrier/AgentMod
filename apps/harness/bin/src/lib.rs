//! Composition root for the `AgentMod` native harness.

use agentmod_harness_data::HarnessHealthDataStore;
use agentmod_harness_dependency::CompositeProviderCatalogDependency;
use agentmod_harness_logic::HarnessHealthManager;
use agentmod_harness_service::HarnessService;

/// Fully assembled first-party harness service.
pub type DefaultHarnessService = HarnessService<HarnessHealthManager<HarnessHealthDataStore<
    CompositeProviderCatalogDependency,
>>>;

/// Assembles dependency → data → logic → service for the harness.
#[must_use]
pub fn build_service() -> DefaultHarnessService {
    let dependency = CompositeProviderCatalogDependency::development();
    let data = HarnessHealthDataStore::new(dependency);
    let logic = HarnessHealthManager::new(data);
    HarnessService::new(logic)
}

/// Assembles the production harness with keyed runtime grant validation.
#[must_use]
pub fn build_secure_service(authorization_key: [u8; 32]) -> DefaultHarnessService {
    let dependency = CompositeProviderCatalogDependency::secure(authorization_key);
    let data = HarnessHealthDataStore::new(dependency);
    let logic = HarnessHealthManager::new(data);
    HarnessService::new(logic)
}

#[cfg(test)]
mod tests {
    use agentmod_harness_protocol::{
        HarnessCommand, HarnessContinuationDecision, HarnessEvent, ProjectedEntry,
    };
    use agentmod_harness_service::{ServiceHealthStatus, ServiceResponse};

    use super::*;

    #[tokio::test]
    async fn composition_root_assembles_a_healthy_vertical_slice() {
        let service = build_service();

        let ServiceResponse::Health(response) = service
            .handle_wire_command(&HarnessCommand::Health)
            .expect("built-in service reports health")
        else {
            panic!("health command returned a non-health response")
        };

        assert_eq!(response.status, ServiceHealthStatus::Ok);
        assert_eq!(response.ready_provider_count, 1);
        assert!(response.configured_provider_count >= 1);
        assert_eq!(
            response.capabilities,
            vec![
                "cancellation".to_owned(),
                "streaming".to_owned(),
                "tool_calls".to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn composition_root_reports_the_bounded_catalog() {
        let service = build_service();
        let ServiceResponse::Catalog(providers) = service
            .handle_wire_command(&HarnessCommand::Catalog)
            .expect("catalog")
        else {
            panic!("catalog command returned a non-catalog response")
        };
        assert!(
            providers
                .iter()
                .any(|provider| provider.id == "deterministic-mock" && provider.available)
        );
        assert!(providers.iter().any(|provider| provider.id == "openrouter"));
    }

    #[tokio::test]
    async fn composition_root_executes_mock_provider_through_all_layers() {
        let service = build_service();
        let events = service
            .execute_wire(&HarnessCommand::Execute {
                session_id: "018f6f83-7b80-7000-8000-000000000001"
                    .parse()
                    .expect("session ID"),
                provider: "deterministic-mock".into(),
                model: "mock-model".into(),
                entries: vec![ProjectedEntry::User {
                    text: "hello".into(),
                }],
                options: serde_json::json!({
                    "mock_scenario": "streaming_text",
                    "mock_text": "done"
                }),
                authorization_grant: "grant".into(),
                cancellation_id: "018f6f83-7b80-7000-8000-000000000002"
                    .parse()
                    .expect("cancellation ID"),
            })
            .await
            .expect("provider execution");
        assert!(matches!(events.first(), Some(HarnessEvent::Started)));
        assert!(matches!(
            events.last(),
            Some(HarnessEvent::Completed { .. })
        ));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, HarnessEvent::TextDelta { .. }))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn tool_proposal_waits_for_explicit_runtime_continuation_once() {
        let service = build_service();
        let session_id = "018f6f83-7b80-7000-8000-000000000001"
            .parse()
            .expect("session ID");
        let cancellation_id = "018f6f83-7b80-7000-8000-000000000002"
            .parse()
            .expect("cancellation ID");
        let events = service
            .execute_wire(&HarnessCommand::Execute {
                session_id,
                provider: "deterministic-mock".into(),
                model: "mock-model".into(),
                entries: vec![ProjectedEntry::User {
                    text: "read the file".into(),
                }],
                options: serde_json::json!({"mock_scenario": "one_tool_call"}),
                authorization_grant: "grant".into(),
                cancellation_id,
            })
            .await
            .expect("initial provider request");
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, HarnessEvent::Completed { .. }))
        );
        let continuation_id = events
            .iter()
            .find_map(|event| {
                if let HarnessEvent::ToolCallProposed {
                    continuation_id, ..
                } = event
                {
                    Some(*continuation_id)
                } else {
                    None
                }
            })
            .expect("tool proposal continuation");

        let command = HarnessCommand::Continue {
            continuation_id,
            decision: HarnessContinuationDecision::ReplaceContext {
                entries: vec![
                    ProjectedEntry::User {
                        text: "read the file".into(),
                    },
                    ProjectedEntry::ToolResult {
                        call_id: "call-1".into(),
                        content: "bounded tool result".into(),
                        truncated: false,
                    },
                ],
            },
        };
        let resumed = service
            .continue_wire(&command)
            .await
            .expect("approved fresh request");
        assert!(matches!(resumed.first(), Some(HarnessEvent::Started)));
        assert!(matches!(
            resumed.last(),
            Some(HarnessEvent::Completed { .. })
        ));
        assert!(
            service.continue_wire(&command).await.is_err(),
            "continuation must resolve exactly once"
        );
    }
}
