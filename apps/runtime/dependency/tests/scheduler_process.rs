//! Focused real-process proof for runtime-owned graph schedule payloads.

use std::path::PathBuf;

use agentmod_runtime_dependency::scheduler::{
    DependencyRuntimeSchedule, DependencySchedulePayload, DependencyScheduleTrigger,
    ProcessSchedulerDependency, ProcessSchedulerDependencyConfig, RuntimeSchedulerDependencyPort,
};
use tempfile::TempDir;

fn executable() -> PathBuf {
    PathBuf::from(
        std::env::var("AGENTMOD_TEST_SCHEDULER")
            .expect("AGENTMOD_TEST_SCHEDULER must name a built scheduler executable"),
    )
}

#[test]
#[ignore = "requires the isolated scheduler process binary"]
fn durable_graph_delay_schedule_round_trips_through_real_worker() {
    let root = TempDir::new().expect("scheduler root");
    let dependency = ProcessSchedulerDependency::new(ProcessSchedulerDependencyConfig {
        program: executable().to_string_lossy().into_owned(),
        arguments: Vec::new(),
        state_root: root.path().join("state"),
        authentication_token: ProcessSchedulerDependency::generate_authentication_token(),
        maximum_frame_bytes: 1024 * 1024,
    })
    .expect("scheduler negotiation");
    let schedule = DependencyRuntimeSchedule {
        schedule_id: format!("graph-schedule:{}", "b".repeat(64)),
        session_id: String::from("019fb28b-a6ce-7dd3-b22f-f6fdab88c3c0"),
        idempotency_id: "b".repeat(64),
        style: String::from("user-graph-a"),
        workspace: root.path().join("workspace").to_string_lossy().into_owned(),
        permission_policy: String::from(r#"{"default":"allow","groups":{}}"#),
        provider: String::from("deterministic-mock"),
        model: String::from("mock-model"),
        token_budget: 10_000,
        cost_budget_micros: 1_000_000,
        trigger: DependencyScheduleTrigger::AtMillis(1_785_406_939_305),
        payload: DependencySchedulePayload::Continuation {
            continuation_id: String::from("7a5f7586-4f3d-7b58-17ba-4d88864ece0c"),
        },
        active: true,
    };

    let stored = dependency
        .upsert(schedule.clone())
        .expect("store exact graph delay schedule");
    assert_eq!(stored.schedule_id, schedule.schedule_id);
    assert!(!stored.replayed);
    assert_eq!(
        dependency.list(10).expect("list stored schedule"),
        vec![schedule]
    );
}
