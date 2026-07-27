//! Acceptance, golden-format, and parser property tests.

use agentmod_plugin_sdk::{
    Entrypoint, FailurePolicy, IsolationMode, PluginCategory, PluginClassification, PluginManifest,
    PluginScope, TrustLevel, ValidationContext, parse_json, parse_toml, to_json, to_toml,
    validate_manifest, validate_plugin_set,
};
use proptest::prelude::*;

const GOLDEN_TOML: &str = include_str!("golden/authorized-interceptor.toml");
const GOLDEN_JSON: &str = include_str!("golden/authorized-interceptor.json");

fn context() -> ValidationContext {
    ValidationContext {
        runtime_api_version: "1.2.0".to_owned(),
        available_capabilities: vec!["runtime.events".to_owned()],
        maximum_timeout_ms: 30_000,
    }
}

fn manifest() -> PluginManifest {
    parse_toml(GOLDEN_TOML).expect("golden manifest must parse")
}

fn codes(error: &agentmod_plugin_sdk::ValidationReport) -> Vec<&'static str> {
    error
        .diagnostics()
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect()
}

#[test]
fn golden_toml_and_json_are_equivalent_and_round_trip() {
    let from_toml = parse_toml(GOLDEN_TOML).expect("TOML golden must parse");
    let from_json = parse_json(GOLDEN_JSON).expect("JSON golden must parse");
    assert_eq!(from_toml, from_json);

    let encoded_toml = to_toml(&from_toml).expect("manifest must serialize as TOML");
    let encoded_json = to_json(&from_json).expect("manifest must serialize as JSON");
    assert_eq!(
        parse_toml(&encoded_toml).expect("serialized TOML must parse"),
        from_toml
    );
    assert_eq!(
        parse_json(&encoded_json).expect("serialized JSON must parse"),
        from_json
    );
    assert_eq!(encoded_json.trim(), GOLDEN_JSON.trim());
}

#[test]
fn authorized_blocking_interceptor_is_accepted() {
    let validated = validate_manifest(&manifest(), &context()).expect("manifest must be valid");
    assert_eq!(validated.manifest().identity.id, "agentmod.audit");
}

#[test]
fn observer_requesting_canonical_write_is_rejected_at_stable_path() {
    let mut candidate = manifest();
    candidate.category = PluginCategory::Observer;
    candidate.classification = PluginClassification::Observer;
    candidate.scope = PluginScope::Runtime;
    candidate.entrypoint = Entrypoint::Process {
        program: "agentmod-observer".to_owned(),
        args: Vec::new(),
    };
    candidate.trust = TrustLevel::ApprovedThirdParty;
    candidate.isolation = IsolationMode::Process;
    candidate.failure_policy = FailurePolicy::Continue;

    let report = validate_manifest(&candidate, &context()).expect_err("write must be rejected");
    assert!(codes(&report).contains(&"PLUG016"));
    let diagnostic = report
        .diagnostics()
        .iter()
        .find(|diagnostic| diagnostic.code == "PLUG016")
        .expect("observer authority diagnostic");
    assert_eq!(
        diagnostic.path,
        "plugins[agentmod.audit].authorities.proposed_write[0]"
    );
}

#[test]
fn missing_capability_prevents_activation() {
    let mut candidate = manifest();
    candidate
        .required_capabilities
        .push("runtime.nonexistent".to_owned());

    let report = validate_manifest(&candidate, &context()).expect_err("capability is missing");
    assert!(codes(&report).contains(&"PLUG012"));
}

#[test]
fn ordering_cycle_is_rejected_by_event_pipeline_compiler() {
    let mut alpha = manifest();
    alpha.identity.id = "plugin.alpha".to_owned();
    alpha.provided_capabilities.clear();
    alpha.ordering.before = vec!["plugin.beta".to_owned()];

    let mut beta = manifest();
    beta.identity.id = "plugin.beta".to_owned();
    beta.provided_capabilities.clear();
    beta.ordering.before = vec!["plugin.alpha".to_owned()];

    let report =
        validate_plugin_set(&[alpha, beta], &context()).expect_err("cycle must be rejected");
    assert_eq!(codes(&report), vec!["PLUG022"]);
    assert_eq!(report.diagnostics()[0].path, "plugins.ordering");
}

#[test]
fn missing_ordering_dependency_is_rejected() {
    let mut candidate = manifest();
    candidate.ordering.after = vec!["plugin.absent".to_owned()];

    let report = validate_plugin_set(&[candidate], &context()).expect_err("dependency is missing");
    assert_eq!(codes(&report), vec!["PLUG021"]);
    assert_eq!(
        report.diagnostics()[0].path,
        "plugins[agentmod.audit].ordering.after"
    );
}

#[test]
fn out_of_process_plugins_require_process_entrypoints() {
    let mut candidate = manifest();
    candidate.isolation = IsolationMode::Process;

    let report = validate_manifest(&candidate, &context()).expect_err("entrypoint must mismatch");
    assert!(codes(&report).contains(&"PLUG007"));
}

#[test]
fn unknown_fields_are_rejected_in_both_formats() {
    let json = GOLDEN_JSON.replacen(
        "\"schema_version\": 1,",
        "\"schema_version\": 1, \"unknown\": true,",
        1,
    );
    assert!(parse_json(&json).is_err());

    let toml = format!("unknown = true\n{GOLDEN_TOML}");
    assert!(parse_toml(&toml).is_err());
}

#[test]
fn diagnostics_have_deterministic_code_path_message_order() {
    let mut candidate = manifest();
    candidate.schema_version = 9;
    candidate.identity.id = "--INVALID".to_owned();
    candidate.timeout_ms = 0;
    candidate.state_migration_version = 0;

    let first = validate_manifest(&candidate, &context()).expect_err("manifest must be invalid");
    let second = validate_manifest(&candidate, &context()).expect_err("manifest must be invalid");
    assert_eq!(first, second);
    assert!(first.diagnostics().windows(2).all(|pair| {
        (
            pair[0].code,
            pair[0].path.as_str(),
            pair[0].message.as_str(),
        ) <= (
            pair[1].code,
            pair[1].path.as_str(),
            pair[1].message.as_str(),
        )
    }));
}

#[test]
fn duplicate_capabilities_and_subscriptions_are_rejected() {
    let mut candidate = manifest();
    candidate
        .required_capabilities
        .push("runtime.events".to_owned());
    candidate
        .subscribed_events
        .push("tool.call.proposed".to_owned());

    let report = validate_manifest(&candidate, &context()).expect_err("duplicates must fail");
    assert_eq!(codes(&report), vec!["PLUG011", "PLUG014"]);
}

proptest! {
    #[test]
    fn parsers_never_panic_on_arbitrary_bounded_text(input in ".{0,4096}") {
        let _ = parse_toml(&input);
        let _ = parse_json(&input);
    }

    #[test]
    fn timeout_validation_matches_documented_bounds(timeout_ms in any::<u64>()) {
        let mut candidate = manifest();
        candidate.timeout_ms = timeout_ms;
        let result = validate_manifest(&candidate, &context());
        let timeout_error = result
            .as_ref()
            .err()
            .is_some_and(|report| codes(report).contains(&"PLUG009"));
        prop_assert_eq!(timeout_error, timeout_ms == 0 || timeout_ms > 30_000);
    }
}
