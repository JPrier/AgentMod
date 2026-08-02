//! Composition root for the independent `AgentMod` harness fixture.

use agentmod_harness_fixture_data::FixtureDataStore;
use agentmod_harness_fixture_dependency::FixtureProviderCatalogDependency;
use agentmod_harness_fixture_logic::FixtureLogicManager;
use agentmod_harness_fixture_service::FixtureService;

/// Fully assembled independent fixture harness service.
pub type DefaultFixtureHarnessService = FixtureService<FixtureLogicManager<FixtureDataStore<
    FixtureProviderCatalogDependency,
>>>;

/// Assembles dependency → data → logic → service for the fixture.
#[must_use]
pub fn build_service() -> DefaultFixtureHarnessService {
    let dependency = FixtureProviderCatalogDependency::development();
    let data = FixtureDataStore::new(dependency);
    let logic = FixtureLogicManager::new(data);
    FixtureService::new(logic)
}

/// Assembles the production fixture with keyed runtime grant validation.
#[must_use]
pub fn build_secure_service(authorization_key: [u8; 32]) -> DefaultFixtureHarnessService {
    let dependency = FixtureProviderCatalogDependency::secure(authorization_key);
    let data = FixtureDataStore::new(dependency);
    let logic = FixtureLogicManager::new(data);
    FixtureService::new(logic)
}

#[cfg(test)]
mod tests {
    use agentmod_harness_fixture_dependency::{
        FIXTURE_HARNESS_ID, FIXTURE_HARNESS_VERSION, FIXTURE_MODEL, FIXTURE_PROVIDER,
    };
    use agentmod_harness_protocol::{HarnessCommand, HarnessEvent, ProjectedEntry};
    use agentmod_harness_protocol::CatalogProvider;

    use super::*;

    #[tokio::test]
    async fn composition_root_reports_distinct_identity_and_capabilities() {
        let service = build_service();
        match service.handle_wire_command(&HarnessCommand::Catalog).await {
            Ok(agentmod_harness_fixture_service::FixtureServiceReply::Catalog(providers)) => {
                let provider: &CatalogProvider = &providers[0];
                assert_eq!(provider.id, FIXTURE_HARNESS_ID);
                assert_eq!(provider.version, FIXTURE_HARNESS_VERSION);
                assert_eq!(provider.models, [FIXTURE_MODEL]);
                assert!(!provider.image_support);
                assert!(!provider.structured_output_support);
                assert!(provider.streaming_support);
                assert!(provider.tool_support);
            }
            other => panic!("unexpected catalog reply: {other:?}"),
        }
        match service.handle_wire_command(&HarnessCommand::Health).await {
            Ok(agentmod_harness_fixture_service::FixtureServiceReply::Health(health)) => {
                assert_eq!(health.ready_provider_count, 1);
                assert!(!health.capabilities.iter().any(|c| c == "images"));
            }
            other => panic!("unexpected health reply: {other:?}"),
        }
    }

    #[tokio::test]
    async fn composition_root_executes_deterministic_scenarios() {
        let service = build_service();
        let events = service
            .execute_wire(&HarnessCommand::Execute {
                session_id: "018f6f83-7b80-7000-8000-000000000001"
                    .parse()
                    .expect("session ID"),
                provider: FIXTURE_PROVIDER.into(),
                model: FIXTURE_MODEL.into(),
                entries: vec![ProjectedEntry::User {
                    text: "hello".into(),
                }],
                options: serde_json::json!({
                    "fixture_scenario": "streaming_text",
                    "fixture_text": "independent"
                }),
                authorization_grant: "grant".into(),
                cancellation_id: "018f6f83-7b80-7000-8000-000000000002"
                    .parse()
                    .expect("cancellation ID"),
            })
            .await
            .expect("provider execution");
        assert!(matches!(events.first(), Some(HarnessEvent::Started)));
        assert!(matches!(events.last(), Some(HarnessEvent::Completed { .. })));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, HarnessEvent::TextDelta { .. }))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn image_inputs_are_rejected_by_the_fixture() {
        let service = build_service();
        let events = service
            .execute_wire(&HarnessCommand::Execute {
                session_id: "018f6f83-7b80-7000-8000-000000000001"
                    .parse()
                    .expect("session ID"),
                provider: FIXTURE_PROVIDER.into(),
                model: FIXTURE_MODEL.into(),
                entries: vec![ProjectedEntry::Image {
                    media_type: "image/png".into(),
                    data_base64: "aGVsbG8=".into(),
                }],
                options: serde_json::json!({"fixture_scenario": "text"}),
                authorization_grant: "grant".into(),
                cancellation_id: "018f6f83-7b80-7000-8000-000000000002"
                    .parse()
                    .expect("cancellation ID"),
            })
            .await
            .expect("provider execution");
        assert!(matches!(
            events.last(),
            Some(HarnessEvent::Failed {
                code,
                retryable: false,
                ..
            }) if code == "unsupported_capability"
        ));
    }
}
