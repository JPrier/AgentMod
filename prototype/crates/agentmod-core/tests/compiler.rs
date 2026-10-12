//! The compiler (compiler.md §5–§7): one test per row of the §6 check table,
//! every §11 example, wildcard expansion, per-emitter supply walks, runtime
//! breach records, and revision determinism.

mod common;

use std::collections::BTreeMap;

use agentmod_core::compiler::{
    Compilation, Diagnostic, Revision, Severity, compile, compile_definition,
};
use agentmod_core::kernel::{InitialEvent, InvocationResult, InvokeRequest, StartRequest};
use agentmod_core::manifest::{DeploymentConfig, Manifest, SourceRef};
use agentmod_core::projection::project;
use agentmod_core::record::Body;
use common::{S, Sim, codes, definition, deployment, k, opt};
use serde_json::{Value, json};

/// Compile one definition `d` over these manifests.
fn build(ms: &Value, d: Value) -> (Compilation, DeploymentConfig, BTreeMap<String, Manifest>) {
    let ms = common::manifests(ms.clone());
    let cfg = deployment(&ms, json!({ "d": d }));
    (compile(&cfg, &ms), cfg, ms)
}

fn rev(c: &Compilation) -> &Revision {
    &c.definitions["d"]
}

fn find<'a>(c: &'a Compilation, code: &str) -> Vec<&'a Diagnostic> {
    c.diagnostics.iter().filter(|d| d.code == code).collect()
}

fn one<'a>(c: &'a Compilation, code: &str) -> &'a Diagnostic {
    let f = find(c, code);
    assert_eq!(
        f.len(),
        1,
        "expected one `{code}`, got {:#?}",
        c.diagnostics
    );
    f[0]
}

/// A root that starts things: a frontend whose deferred `go` starts sessions.
fn ui() -> Value {
    json!({ "subscribe": [{ "event": "session-started", "mode": "async" }], "emit": [{ "event": "go", "deferred": true, "keys": { "text": k(S) } }], "capabilities": ["deferred-publish"], "sink": true })
}

fn base_def(plugins: Value) -> Value {
    json!({ "runtime": "0.1", "phases": ["guard", "main"], "plugins": plugins })
}

// ----------------------------------------------------------------------------
// §6, row by row.
// ----------------------------------------------------------------------------

#[test]
fn row_supply_is_checked_per_emitter_with_key_paths() {
    // Two emitters of `x`; one falls short, one has the wrong shape inside a nested object.
    let ms = json!({
        "ui": ui(),
        "a": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "x", "keys": { "id": k(S), "meta": { "shape": "object", "keys": { "n": k("integer") } } } }] },
        "b": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "x", "keys": { "meta": { "shape": "object", "keys": { "n": k("string") } } } }] },
        "c": { "subscribe": [{ "event": "x", "mode": "async", "receives": { "id": k(S), "meta": { "shape": "object", "keys": { "n": k("number") } } } }], "sink": true }
    });
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "a": {}, "b": {}, "c": {} })),
    );
    assert!(!c.ok);
    let starved = one(&c, "starved-consumer");
    assert_eq!(
        (starved.event.as_deref(), starved.key.as_deref()),
        (Some("x"), Some("id"))
    );
    assert_eq!(starved.plugins, vec!["b", "c"]);
    assert!(starved.message.contains("`b → c`"));
    let shape = one(&c, "shape-mismatch");
    assert_eq!(shape.key.as_deref(), Some("meta.n"));
    assert_eq!(shape.plugins, vec!["b", "c"]);
    // `a` satisfies `c` (integer ⊆ number): no diagnostic names it.
    assert!(
        c.diagnostics
            .iter()
            .filter(|d| d.severity == Severity::Error)
            .all(|d| !d.plugins.contains(&"a".to_owned()))
    );
}

#[test]
fn row_self_subscription_including_through_a_family() {
    let ms = json!({
        "ui": ui(),
        "loop": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "tool-call/read", "mode": "async" }], "emit": [{ "event": "tool-call/*" }] },
        "echo": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "go" }] }
    });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "loop": {}, "echo": {} })));
    let d = find(&c, "self-subscription");
    assert_eq!(d.len(), 2, "{d:#?}");
    assert!(
        d.iter()
            .any(|x| x.plugins == ["loop"] && x.message.contains("through `tool-call/*`"))
    );
    assert!(d.iter().any(|x| x.plugins == ["echo"]));
}

#[test]
fn row_blocking_subscriber_without_a_phase() {
    let ms = json!({ "ui": ui(), "g": { "subscribe": [{ "event": "go", "mode": "blocking", "actions": ["veto"] }] } });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "g": {} })));
    let d = one(&c, "missing-phase");
    assert_eq!(
        (d.event.as_deref(), d.plugins.as_slice()),
        (Some("go"), &["g".to_owned()][..])
    );
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "g": { "phase": "nope" } })));
    one(&c, "unknown-phase");
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "g": { "phase": "main", "phases": { "other": "guard" } } })),
    );
    one(&c, "stale-phase");
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "g": { "phases": { "go": "guard" } } })),
    );
    assert!(c.ok, "a per-event phase suffices: {:#?}", c.diagnostics);
}

#[test]
fn row_observable_ties_name_the_pair_the_event_and_the_reason() {
    let sub = |extra: Value| -> Value {
        let mut s = json!({ "event": "go", "mode": "blocking", "receives": { "text": k(S) } });
        s.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({ "subscribe": [s] })
    };
    let cases = [
        (
            json!({ "returns": { "text": k(S) } }),
            json!({}),
            "which the other receives",
        ),
        (
            json!({ "returns": { "tag": k(S) } }),
            json!({ "returns": { "tag": k(S) } }),
            "both return `tag`",
        ),
        (
            json!({ "actions": ["contribute-context"] }),
            json!({ "actions": ["contribute-context"] }),
            "both contribute context",
        ),
        (json!({ "actions": ["veto"] }), json!({}), "can veto"),
    ];
    for (a, b, why) in cases {
        let ms = json!({ "ui": ui(), "a": sub(a), "b": sub(b) });
        let (c, ..) = build(
            &ms,
            base_def(json!({ "ui": {}, "a": { "phase": "main" }, "b": { "phase": "main" } })),
        );
        let d = one(&c, "observable-tie");
        assert_eq!(d.plugins, vec!["a", "b"]);
        assert_eq!(d.event.as_deref(), Some("go"));
        assert!(d.message.contains(why), "{why}: {}", d.message);
        // A tie-break orders them.
        let mut def =
            base_def(json!({ "ui": {}, "a": { "phase": "main" }, "b": { "phase": "main" } }));
        def["order"] = json!({ "go": { "main": ["b", "a"] } });
        let (c, ..) = build(&ms, def);
        assert!(c.ok, "{:#?}", c.diagnostics);
        let r = &rev(&c).graph;
        let route = r.routes.iter().find(|x| x.event == "go").unwrap();
        let order: Vec<&str> = route
            .blocking
            .iter()
            .map(|i| r.edge(*i).to.as_str())
            .collect();
        assert_eq!(order, vec!["b", "a"]);
    }
    // No observable interaction: a fixed order by name, shown in the report.
    let ms = json!({ "ui": ui(), "a": sub(json!({})), "b": sub(json!({})) });
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "a": { "phase": "main" }, "b": { "phase": "main" } })),
    );
    assert!(c.ok, "{:#?}", c.diagnostics);
    assert_eq!(
        rev(&c).report.fixed_orders,
        vec!["go [main]: a before b (no observable tie; by name)"]
    );
    // A tie-break naming a plugin that is not there is stale.
    let mut def = base_def(json!({ "ui": {}, "a": { "phase": "main" }, "b": { "phase": "main" } }));
    def["order"] = json!({ "go": { "main": ["a", "ghost"] } });
    let (c, ..) = build(&ms, def);
    one(&c, "stale-tie-break");
}

#[test]
fn row_fragments_with_conflicting_phase_lists() {
    let ms = common::manifests(json!({ "ui": ui() }));
    let mut cfg = deployment(
        &ms,
        json!({ "d": { "runtime": "0.1", "include": ["a", "b"], "plugins": { "ui": {} } } }),
    );
    cfg.fragments.insert(
        "a".into(),
        definition(json!({ "phases": ["guard", "main"] })),
    );
    cfg.fragments.insert(
        "b".into(),
        definition(json!({ "phases": ["main", "guard"] })),
    );
    cfg.sources.insert(
        "fragments.b.phases".into(),
        SourceRef {
            file: "frag.toml".into(),
            line: 7,
        },
    );
    let c = compile(&cfg, &ms);
    let d = one(&c, "conflicting-phases");
    assert!(
        d.message.contains("`fragments.b`") && d.message.contains("`fragments.a`"),
        "{}",
        d.message
    );
    assert_eq!(
        d.source.as_ref().map(|s| (s.file.as_str(), s.line)),
        Some(("frag.toml", 7)),
        "traces to the fragment and line"
    );
    // Agreeing fragments flatten into one definition.
    cfg.fragments.insert(
        "b".into(),
        definition(json!({ "phases": ["guard", "main"], "plugins": { "ui": {} } })),
    );
    let c = compile(&cfg, &ms);
    assert!(c.ok, "{:#?}", c.diagnostics);
    assert_eq!(c.definitions["d"].definition.phases, vec!["guard", "main"]);
}

fn family_ms() -> Value {
    let h = |ev: &str| json!({ "subscribe": [{ "event": ev, "mode": "async" }], "emit": [{ "event": "tool-result" }] });
    json!({
        "ui": ui(),
        "loop": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "tool-result", "mode": "async" }], "emit": [{ "event": "tool-call/*", "keys": { "call_id": k(S) } }] },
        "local": h("tool-call/read_file"),
        "vm": h("tool-call/read_file"),
        "gh": h("tool-call/mcp__gh__issues"),
        "watch": { "subscribe": [{ "event": "tool-call/mcp__gh__*", "mode": "async" }], "sink": true },
        "audit": { "subscribe": [{ "event": "tool-call/*", "mode": "async" }], "sink": true }
    })
}

/// §3.3/§3.4: a family emit resolves into the concrete members the
/// definition's subscribers name; family and sub-family subscriptions only
/// observe; two subscribers of one member are not an error.
#[test]
fn row_families_resolve_and_a_member_may_have_several_subscribers() {
    let plugins =
        json!({ "ui": {}, "loop": {}, "local": {}, "vm": {}, "gh": {}, "watch": {}, "audit": {} });
    let (c, ..) = build(&family_ms(), base_def(plugins));
    assert!(c.ok, "{:#?}", c.diagnostics);
    let r = rev(&c);
    assert_eq!(
        r.report.families["tool-call/*"]["loop"],
        vec!["tool-call/mcp__gh__issues", "tool-call/read_file"]
    );
    let info = one(&c, "resolved-family");
    assert_eq!(info.severity, Severity::Info);
    assert!(
        info.message.contains("resolved to 2 members"),
        "{}",
        info.message
    );
    // The graph holds only concrete events.
    assert!(r.graph.edges.iter().all(|e| !e.event.contains('*')));
    assert!(r.graph.routes.iter().all(|x| !x.event.contains('*')));
    let to = |ev: &str| -> Vec<&str> {
        r.graph
            .edges
            .iter()
            .filter(|e| e.event == ev)
            .map(|e| e.to.as_str())
            .collect()
    };
    // Both subscribers of read_file run; observers see every member.
    assert_eq!(to("tool-call/read_file"), vec!["audit", "local", "vm"]);
    assert_eq!(
        to("tool-call/mcp__gh__issues"),
        vec!["audit", "gh", "watch"]
    );
    assert!(find(&c, "missing-handler-pick").is_empty());
}

#[test]
fn row_returns_that_change_a_supplied_keys_shape() {
    let ms = json!({
        "ui": ui(),
        "t": { "subscribe": [{ "event": "go", "mode": "blocking", "returns": { "text": k("integer") } }] },
        "c": { "subscribe": [{ "event": "go", "mode": "async", "receives": { "text": k(S) } }], "sink": true }
    });
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "t": { "phase": "main" }, "c": {} })),
    );
    let d = one(&c, "returns-shape-conflict");
    assert_eq!(
        (d.key.as_deref(), d.plugins.as_slice()),
        (Some("text"), &["t".to_owned(), "ui".to_owned()][..])
    );
}

#[test]
fn row_undeclared_actions_and_capabilities() {
    let ms = json!({
        "ui": ui(),
        "a": { "subscribe": [{ "event": "go", "mode": "async", "actions": ["veto"] }], "sink": true },
        "b": { "subscribe": [{ "event": "go", "mode": "async", "actions": ["explode"], "returns": { "x": k(S) } }], "sink": true },
        "c": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "later", "deferred": true }, { "event": "now", "lane": "interrupt" }], "outbound": [{ "event": "out" }] }
    });
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "a": {}, "b": {}, "c": {} })),
    );
    assert_eq!(
        find(&c, "invalid-action").len(),
        2,
        "async veto, async returns"
    );
    one(&c, "unknown-action");
    let caps: Vec<&str> = find(&c, "missing-capability")
        .iter()
        .map(|d| d.message.as_str())
        .collect();
    assert!(caps.iter().any(|m| m.contains("deferred-publish")));
    assert!(caps.iter().any(|m| m.contains("interrupt capability")));
    assert!(caps.iter().any(|m| m.contains("cross-session")));
}

#[test]
fn row_a_plugin_that_never_fires_is_an_error() {
    let ms = json!({ "ui": ui(), "deaf": { "subscribe": [{ "event": "nobody-emits-this", "mode": "async" }], "sink": true } });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "deaf": {} })));
    let d = one(&c, "never-fires");
    assert_eq!(d.plugins, vec!["deaf"]);
    assert_eq!(d.severity, Severity::Error);
}

#[test]
fn row_an_unfed_subscription_is_a_warning() {
    let ms = json!({ "ui": ui(), "p": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "answer", "mode": "async" }], "sink": true } });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "p": {} })));
    assert!(c.ok);
    let d = one(&c, "unfed-subscription");
    assert_eq!(
        (d.severity, d.event.as_deref()),
        (Severity::Warning, Some("answer"))
    );
}

#[test]
fn row_a_plugin_with_no_effect_is_a_warning_unless_a_sink() {
    let ms = json!({ "ui": ui(), "idle": { "subscribe": [{ "event": "go", "mode": "async" }] }, "screen": { "subscribe": [{ "event": "go", "mode": "async" }], "sink": true } });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "idle": {}, "screen": {} })));
    let d = one(&c, "no-effect");
    assert_eq!(
        (d.severity, d.plugins.as_slice()),
        (Severity::Warning, &["idle".to_owned()][..])
    );
}

#[test]
fn row_an_unreachable_island_is_a_warning() {
    let ms = json!({
        "ui": ui(),
        "p": { "subscribe": [{ "event": "b", "mode": "async" }], "emit": [{ "event": "a" }] },
        "q": { "subscribe": [{ "event": "a", "mode": "async" }], "emit": [{ "event": "b" }] }
    });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "p": {}, "q": {} })));
    let d = one(&c, "unreachable-island");
    assert_eq!(
        (d.severity, d.plugins.as_slice()),
        (Severity::Warning, &["p".to_owned(), "q".to_owned()][..])
    );
    assert!(
        find(&c, "never-fires").is_empty(),
        "they do feed each other"
    );
}

#[test]
fn row_an_interrupt_only_cycle_is_a_warning() {
    let ms = json!({
        "ui": ui(),
        "p": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "b", "mode": "async" }], "emit": [{ "event": "a", "lane": "interrupt" }], "capabilities": ["interrupt"] },
        "q": { "subscribe": [{ "event": "a", "mode": "async" }], "emit": [{ "event": "b", "lane": "interrupt" }], "capabilities": ["interrupt"] }
    });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "p": {}, "q": {} })));
    let d = one(&c, "interrupt-cycle");
    assert_eq!(d.plugins, vec!["p", "q"]);
    assert!(
        rev(&c)
            .report
            .interrupt
            .iter()
            .any(|x| x == "p: a (interrupt lane)")
    );
}

#[test]
fn row_unconsumed_emits_and_unrequested_keys_are_not_reported() {
    let ms = json!({
        "ui": ui(),
        "p": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "nobody-listens", "keys": { "x": k(S) } }], "sink": true }
    });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "p": {} })));
    assert!(c.ok);
    assert!(
        c.diagnostics.iter().all(|d| d.severity == Severity::Info),
        "{:#?}",
        c.diagnostics
    );
}

#[test]
fn row_info_sections_cycles_capabilities_interrupt_unlisted_and_surface() {
    let ms = json!({
        "ui": { "subscribe": [{ "event": "session-started", "mode": "async" }], "emit": [{ "event": "go", "deferred": true, "keys": { "text": k(S) } }], "capabilities": ["deferred-publish", "start-session", "interrupt"], "sink": true },
        "p": { "subscribe": [{ "event": "go", "mode": "async", "unlisted_keys": true }, { "event": "b", "mode": "async" }], "emit": [{ "event": "a" }] },
        "q": { "subscribe": [{ "event": "a", "mode": "async" }], "emit": [{ "event": "b" }], "outbound": [{ "event": "done" }], "capabilities": ["cross-session"] }
    });
    let mut d = base_def(json!({ "ui": {}, "p": {}, "q": {} }));
    d["inbound"] = json!({ "reply": { "text": k(S) } });
    let (c, ..) = build(&ms, d);
    assert!(c.ok, "{:#?}", c.diagnostics);
    let r = &rev(&c).report;
    assert_eq!(r.cycles, vec![vec!["p".to_owned(), "q".to_owned()]]);
    assert_eq!(r.capabilities["cross-session"], vec!["q"]);
    assert!(
        r.interrupt
            .contains(&"ui: holds the interrupt capability".to_owned())
    );
    assert_eq!(r.unlisted_keys, vec!["p: go"]);
    assert_eq!(r.external_surface["inbound"], vec!["reply"]);
    assert_eq!(r.external_surface["outbound"], vec!["q: done"]);
    assert_eq!(r.external_surface["deferred-publish"], vec!["ui: go"]);
    for code in [
        "cycle",
        "capability",
        "interrupt",
        "unlisted-keys",
        "external-surface",
    ] {
        assert!(
            find(&c, code).iter().all(|d| d.severity == Severity::Info)
                && !find(&c, code).is_empty(),
            "{code}"
        );
    }
}

#[test]
fn deny_warnings_promotes_warnings_to_errors() {
    let ms = json!({ "ui": ui(), "p": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "answer", "mode": "async" }], "sink": true } });
    let mut d = base_def(json!({ "ui": {}, "p": {} }));
    d["deny_warnings"] = json!(true);
    let (c, ..) = build(&ms, d);
    assert!(!c.ok);
    assert_eq!(one(&c, "unfed-subscription").severity, Severity::Error);
}

#[test]
fn definitions_name_their_runtime_and_slots_are_typed() {
    let ms = json!({ "ui": ui(), "p": { "subscribe": [{ "event": "go", "mode": "async", "receives": { "slots": { "context": k("object"), "notes": k("array<string>") } } }], "sink": true } });
    let (c, ..) = build(&ms, json!({ "plugins": { "ui": {}, "p": {} } }));
    one(&c, "missing-runtime");
    let (c, ..) = build(
        &ms,
        json!({ "runtime": "9.9", "plugins": { "ui": {}, "p": {} } }),
    );
    one(&c, "unknown-runtime");
    let (c, ..) = build(
        &ms,
        json!({ "runtime": "0.1", "plugins": { "ui": {}, "p": {} } }),
    );
    assert_eq!(
        one(&c, "shape-mismatch").key.as_deref(),
        Some("slots.context")
    );
    one(&c, "unknown-slot");
    let (c, ..) = build(
        &ms,
        json!({ "runtime": "0.1", "plugins": { "ui": {}, "p": {} }, "slots": { "context": k("object"), "notes": k("array<string>") } }),
    );
    assert!(
        c.ok,
        "a definition may replace the default slot: {:#?}",
        c.diagnostics
    );
}

#[test]
fn diagnostics_trace_to_the_definition_source_location() {
    let ms = common::manifests(
        json!({ "ui": ui(), "g": { "subscribe": [{ "event": "go", "mode": "blocking", "actions": ["veto"] }] } }),
    );
    let mut cfg = deployment(&ms, json!({ "d": base_def(json!({ "ui": {}, "g": {} })) }));
    cfg.sources.insert(
        "definitions.d".into(),
        SourceRef {
            file: "agentmod.toml".into(),
            line: 10,
        },
    );
    cfg.sources.insert(
        "definitions.d.plugins.g".into(),
        SourceRef {
            file: "agentmod.toml".into(),
            line: 14,
        },
    );
    let c = compile(&cfg, &ms);
    let d = one(&c, "missing-phase");
    assert_eq!(d.definition.as_deref(), Some("d"));
    assert_eq!(d.path.as_deref(), Some("definitions.d.plugins.g"));
    assert_eq!(
        d.source,
        Some(SourceRef {
            file: "agentmod.toml".into(),
            line: 14
        })
    );
    assert_eq!(
        d.location(),
        "agentmod.toml:14 / definitions.d.plugins.g / go / g"
    );
}

// ----------------------------------------------------------------------------
// Wildcard expansion and supply walks.
// ----------------------------------------------------------------------------

#[test]
fn wildcard_expansion_excludes_own_and_wildcard_emits() {
    let ms = json!({
        "ui": ui(),
        "n": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "foo" }] },
        "w1": { "subscribe": [{ "event": "*", "mode": "async" }], "emit": [{ "event": "foo" }] },
        "w2": { "subscribe": [{ "event": "*", "mode": "async" }], "emit": [{ "event": "bar" }] }
    });
    let (c, ..) = build(
        &ms,
        base_def(json!({ "ui": {}, "n": {}, "w1": {}, "w2": {} })),
    );
    let g = &rev(&c).graph;
    let has = |from: &str, ev: &str, to: &str| {
        g.edges
            .iter()
            .any(|e| e.from == from && e.event == ev && e.to == to)
    };
    assert!(has("n", "foo", "w2"), "N --foo--> W2 exists");
    assert!(!has("w1", "foo", "w2"), "W1 --foo--> W2 does not");
    assert!(
        !has("w1", "foo", "w1") && !has("w2", "bar", "w2"),
        "no wildcard receives its own emits"
    );
    assert!(has("ui", "go", "w1") && has("core", "session-started", "w2"));
}

#[test]
fn supply_walks_through_earlier_returns_per_emitter() {
    let ms = json!({
        "ui": ui(),
        "other": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "msg", "keys": { "text": k(S) } }] },
        "src": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "msg", "keys": { "text": k(S) } }] },
        "tagger": { "subscribe": [{ "event": "msg", "mode": "blocking", "returns": { "tag": k(S), "maybe": opt("bool") } }] },
        "early": { "subscribe": [{ "event": "msg", "mode": "blocking", "receives": { "tag": opt(S) } }], "sink": true },
        "late": { "subscribe": [{ "event": "msg", "mode": "blocking", "receives": { "tag": k(S) } }], "sink": true },
        "watch": { "subscribe": [{ "event": "msg", "mode": "async", "receives": { "tag": k(S), "maybe": opt("bool") } }], "sink": true }
    });
    let mut d = json!({ "runtime": "0.1", "phases": ["a", "b", "c"], "plugins": { "ui": {}, "other": {}, "src": {}, "tagger": { "phase": "b" }, "early": { "phase": "a" }, "late": { "phase": "c" }, "watch": {} } });
    let (c, ..) = build(&ms, d.clone());
    assert!(c.ok, "{:#?}", c.diagnostics);
    // The supply each edge carries: `late` and `watch` see the returned `tag`.
    let g = &rev(&c).graph;
    for from in ["src", "other"] {
        let edge = |to: &str| {
            g.edges
                .iter()
                .find(|e| e.from == from && e.to == to)
                .unwrap()
        };
        assert!(!edge("early").supply.contains_key("tag"));
        assert!(!edge("late").supply["tag"].optional);
        assert!(edge("watch").supply["maybe"].optional);
        let p = edge("late").position.clone().unwrap();
        assert_eq!((p.phase.as_str(), p.chain), ("c", 2));
    }
    // Move the tagger after `late`: `late` is starved, on every emitter's edge.
    d["plugins"]["tagger"] = json!({ "phase": "c" });
    d["plugins"]["late"] = json!({ "phase": "b" });
    let (c, ..) = build(&ms, d);
    let starved = find(&c, "starved-consumer");
    assert_eq!(
        starved
            .iter()
            .map(|x| x.plugins.clone())
            .collect::<Vec<_>>(),
        vec![
            vec!["late".to_owned(), "other".to_owned()],
            vec!["late".to_owned(), "src".to_owned()]
        ]
    );
}

// ----------------------------------------------------------------------------
// Revisions.
// ----------------------------------------------------------------------------

#[test]
fn revisions_are_deterministic_and_content_addressed() {
    let ms_v = json!({ "ui": ui(), "p": { "subscribe": [{ "event": "go", "mode": "async" }], "sink": true } });
    let (a, cfg, ms) = build(&ms_v, base_def(json!({ "ui": {}, "p": {} })));
    let (b, ..) = build(&ms_v, base_def(json!({ "ui": {}, "p": {} })));
    assert_eq!(
        serde_json::to_string(&a).unwrap(),
        serde_json::to_string(&b).unwrap(),
        "byte for byte"
    );
    // Source locations and descriptions do not change the revision; config does.
    let mut moved = cfg.clone();
    moved.sources.insert(
        "definitions.d".into(),
        SourceRef {
            file: "x.toml".into(),
            line: 99,
        },
    );
    moved.definitions.get_mut("d").unwrap().description = "renamed".into();
    assert_eq!(
        compile(&moved, &ms).definitions["d"].revision,
        rev(&a).revision
    );
    let mut changed = cfg.clone();
    changed.plugins.get_mut("p").unwrap().config = json!({ "v": 2 });
    assert_ne!(
        compile(&changed, &ms).definitions["d"].revision,
        rev(&a).revision
    );
    // Compiling the definition alone gives the same revision.
    assert_eq!(
        compile_definition("d", &cfg.definitions["d"], &cfg, &ms).revision,
        rev(&a).revision
    );
}

// ----------------------------------------------------------------------------
// §11, every example.
// ----------------------------------------------------------------------------

fn chat_loop_ms() -> Value {
    json!({
        "web-ui": { "subscribe": [{ "event": "session-started", "mode": "async" }], "emit": [{ "event": "user-message", "deferred": true, "keys": { "text": k(S) } }], "capabilities": ["deferred-publish", "observe"], "sink": true },
        "chat-context": {
            "subscribe": [
                { "event": "user-message", "mode": "blocking", "actions": ["contribute-context"], "receives": { "text": k(S) } },
                { "event": "model-response", "mode": "blocking", "actions": ["contribute-context"], "receives": { "text": k(S) }, "returns": { "messages": opt("array<object>") } },
                { "event": "tool-result", "mode": "blocking", "actions": ["contribute-context"], "receives": { "call_id": k(S), "content": k(S) } }
            ],
            "emit": [
                { "event": "model-request", "keys": { "model": k(S), "messages": k("array<object>") } },
                { "event": "tool-call/*", "keys": { "call_id": k(S), "args": k("object") } }
            ]
        },
        "openrouter": { "subscribe": [{ "event": "model-request", "mode": "async", "receives": { "model": k(S), "messages": k("array<object>") } }], "emit": [{ "event": "model-response", "keys": { "text": k(S) } }] },
        "file-tools": { "subscribe": [{ "event": "tool-call/read_file", "mode": "async", "receives": { "call_id": k(S), "args": k("object") } }], "emit": [{ "event": "tool-result", "keys": { "call_id": k(S), "content": k(S) } }] }
    })
}

fn chat_loop_def() -> Value {
    json!({ "runtime": "0.1", "phases": ["loop"], "plugins": { "web-ui": {}, "chat-context": { "phase": "loop" }, "openrouter": {}, "file-tools": {} } })
}

#[test]
fn example_accepted_chat_loop() {
    let (c, ..) = build(&chat_loop_ms(), chat_loop_def());
    assert!(c.ok, "{:#?}", c.diagnostics);
    let r = rev(&c);
    // web-ui watches the session through `observe`, so it is not in the loop.
    assert_eq!(
        r.report.cycles,
        vec![vec![
            "chat-context".to_owned(),
            "file-tools".to_owned(),
            "openrouter".to_owned(),
        ]]
    );
    assert!(
        find(&c, "cycle")
            .iter()
            .all(|d| d.severity == Severity::Info)
    );
    // chat-context's `tool-call/*` resolves to read_file, which file-tools subscribes to.
    assert_eq!(
        r.report.families["tool-call/*"]["chat-context"],
        vec!["tool-call/read_file"]
    );
    let read = r
        .graph
        .routes
        .iter()
        .find(|x| x.event == "tool-call/read_file")
        .unwrap();
    assert_eq!(read.emit, "tool-call/*");
    assert_eq!(
        read.asyncs
            .iter()
            .map(|i| r.graph.edge(*i).to.as_str())
            .collect::<Vec<_>>(),
        vec!["file-tools"]
    );
}

#[test]
fn example_rejected_emitter_that_falls_short() {
    let ms = json!({
        "ui": ui(),
        "shell": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "tool-result", "keys": { "content": k(S), "exit_code": k("integer") } }] },
        "mcp-bridge": { "subscribe": [{ "event": "go", "mode": "async" }], "emit": [{ "event": "tool-result", "keys": { "content": k(S) } }] },
        "retry": { "subscribe": [{ "event": "tool-result", "mode": "async", "receives": { "exit_code": k("integer") } }], "sink": true }
    });
    let d = base_def(json!({ "ui": {}, "shell": {}, "mcp-bridge": {}, "retry": {} }));
    let (c, ..) = build(&ms, d.clone());
    let e = one(&c, "starved-consumer");
    assert_eq!(
        (e.key.as_deref(), e.plugins.as_slice()),
        (
            Some("exit_code"),
            &["mcp-bridge".to_owned(), "retry".to_owned()][..]
        )
    );
    assert!(e.message.contains("`mcp-bridge → retry`"));
    // Fix: make it optional in `retry`.
    let mut fixed = ms.clone();
    fixed["retry"]["subscribe"][0]["receives"]["exit_code"] = opt("integer");
    assert!(build(&fixed, d.clone()).0.ok);
    // Fix: supply it from `mcp-bridge`.
    let mut fixed = ms;
    fixed["mcp-bridge"]["emit"][0]["keys"]["exit_code"] = k("integer");
    assert!(build(&fixed, d).0.ok);
}

#[test]
fn example_rejected_observable_tie() {
    let gate = |name: &str, veto: bool| json!({ "subscribe": [{ "event": "tool-call/*", "mode": "blocking", "actions": if veto { json!(["veto"]) } else { json!([]) } }], "emit": [{ "event": format!("{name}-noted") }] });
    let mut ms = chat_loop_ms();
    ms["policy-gate"] = gate("policy", true);
    ms["redactor"] = gate("redactor", true);
    let mut d = chat_loop_def();
    d["phases"] = json!(["guard", "loop"]);
    d["plugins"]["policy-gate"] = json!({ "phase": "guard" });
    d["plugins"]["redactor"] = json!({ "phase": "guard" });
    let (c, ..) = build(&ms, d.clone());
    let ties = find(&c, "observable-tie");
    assert!(!ties.is_empty());
    assert!(
        ties.iter()
            .all(|t| t.plugins == ["policy-gate", "redactor"] && t.message.contains("can veto"))
    );
    assert!(
        ties[0]
            .message
            .contains("`order.\"tool-call\".guard = [\"policy-gate\", \"redactor\"]`"),
        "{}",
        ties[0].message
    );
    d["order"] = json!({ "tool-call": { "guard": ["policy-gate", "redactor"] } });
    assert!(build(&ms, d.clone()).0.ok);
    // Neither vetoes nor touches the other's keys: compiles, with a fixed order in the report.
    ms["policy-gate"] = gate("policy", false);
    ms["redactor"] = gate("redactor", false);
    d.as_object_mut().unwrap().remove("order");
    let (c, ..) = build(&ms, d);
    assert!(c.ok, "{:#?}", c.diagnostics);
    assert!(
        rev(&c)
            .report
            .fixed_orders
            .iter()
            .any(|f| f.contains("policy-gate before redactor"))
    );
}

#[test]
fn example_rejected_self_subscription() {
    let ms = json!({ "ui": ui(), "summariser": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "context-updated", "mode": "async" }], "emit": [{ "event": "context-updated" }] } });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "summariser": {} })));
    assert_eq!(one(&c, "self-subscription").plugins, vec!["summariser"]);
    // Fix: emit a different event and let another plugin react to it.
    let ms = json!({
        "ui": ui(),
        "summariser": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "context-updated", "mode": "async" }], "emit": [{ "event": "summary-ready" }] },
        "editor": { "subscribe": [{ "event": "summary-ready", "mode": "async" }], "emit": [{ "event": "context-updated" }] }
    });
    assert!(
        build(
            &ms,
            base_def(json!({ "ui": {}, "summariser": {}, "editor": {} }))
        )
        .0
        .ok
    );
}

#[test]
fn example_accepted_then_failed_at_runtime_with_a_breach_record() {
    let mut ms = chat_loop_ms();
    ms["web-ui"]["capabilities"] = json!(["deferred-publish", "start-session"]);
    let (c, ..) = build(&ms, chat_loop_def());
    assert!(c.ok, "{:#?}", c.diagnostics);
    let revision = rev(&c).revision.clone();
    let mut sim = Sim::new(c);
    let sid = sim.start(
        "web-ui",
        "d",
        Some(("user-message", json!({ "text": "hi" }))),
    );
    sim.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| match (
        p,
        req.event.event_name.as_str(),
    ) {
        ("chat-context", "user-message") => {
            s.emit(
                p,
                &req.invocation_id,
                "model-request",
                json!({ "model": "m", "messages": [] }),
            )
            .unwrap();
            InvocationResult::default()
        }
        ("openrouter", _) => {
            s.emit(
                p,
                &req.invocation_id,
                "model-response",
                json!({ "text": "ok" }),
            )
            .unwrap();
            InvocationResult::default()
        }
        // Declared `returns.messages` as array<object>, but returns a string.
        ("chat-context", "model-response") => InvocationResult {
            returns: Some(json!({ "messages": "oops" }).as_object().unwrap().clone()),
            ..Default::default()
        },
        _ => InvocationResult::default(),
    });
    let view = project(sim.log(&sid));
    let mr = view
        .events
        .iter()
        .find(|e| e.event_name == "model-response")
        .unwrap();
    assert_eq!(mr.status, "failed");
    // The record: session, revision, event and invocation ids, plugin stamp,
    // the exact declaration violated, and the value observed.
    let rec = sim
        .log(&sid)
        .iter()
        .find(|r| matches!(r.body, Body::Breach { .. }))
        .unwrap();
    let Body::Breach {
        event_id,
        invocation_id,
        plugin,
        stamp,
        revision: r,
        declaration,
        observed,
        reason,
        reaction,
    } = &rec.body
    else {
        unreachable!()
    };
    assert_eq!(rec.session_id, sid);
    assert_eq!(r, &revision);
    assert_eq!(event_id.as_deref(), Some(mr.event_id.as_str()));
    assert_eq!(
        invocation_id.as_deref(),
        Some(mr.invocations[0].invocation_id.as_str())
    );
    assert_eq!(plugin, "chat-context");
    assert_eq!(stamp.binary, "bin-chat-context");
    assert_eq!(declaration["kind"], "returns");
    assert_eq!(declaration["subscription"], "model-response");
    assert_eq!(declaration["key"], "messages");
    assert_eq!(
        declaration["shape"],
        json!({ "shape": "array<object>", "optional": true })
    );
    assert_eq!(observed, &json!("oops"));
    assert!(reason.contains("contradicting its `returns`"));
    assert_eq!(reaction, "invocation-failed");
}

#[test]
fn example_accepted_with_a_warning_until_inbound_is_declared() {
    let ms = json!({ "ui": ui(), "ask-parent": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "answer", "mode": "async", "receives": { "text": k(S) } }], "sink": true } });
    let mut d = base_def(json!({ "ui": {}, "ask-parent": {} }));
    let (c, ..) = build(&ms, d.clone());
    assert!(c.ok);
    let w = one(&c, "unfed-subscription");
    assert_eq!(
        (w.event.as_deref(), w.plugins.as_slice()),
        (Some("answer"), &["ask-parent".to_owned()][..])
    );
    d["inbound"] = json!({ "answer": { "text": k(S) } });
    let (c, ..) = build(&ms, d);
    assert!(find(&c, "unfed-subscription").is_empty());
    let g = &rev(&c).graph;
    assert!(
        g.edges
            .iter()
            .any(|e| e.from == "core" && e.event == "answer" && e.to == "ask-parent")
    );
}

#[test]
fn example_rejected_dynamic_definition() {
    // An orchestrator starts a session from a definition it wrote moments ago;
    // that definition has a starved consumer.
    let ms = json!({
        "ui": { "subscribe": [{ "event": "session-started", "mode": "async" }], "emit": [{ "event": "go", "deferred": true, "keys": { "text": k(S) } }], "capabilities": ["deferred-publish", "start-session"], "sink": true },
        "orchestrator": { "subscribe": [{ "event": "go", "mode": "async" }, { "event": "session-start-failed", "mode": "async", "receives": { "definition": k(S), "diagnostics": k("array<object>") } }], "capabilities": ["start-session"], "sink": true },
        "needy": { "subscribe": [{ "event": "go", "mode": "async", "receives": { "missing": k(S) } }], "sink": true }
    });
    let (c, ..) = build(&ms, base_def(json!({ "ui": {}, "orchestrator": {} })));
    assert!(c.ok, "{:#?}", c.diagnostics);
    let mut sim = Sim::new(c);
    let parent = sim.start("ui", "d", None);
    sim.run(&mut |_: &mut Sim, _: &str, _: &InvokeRequest| InvocationResult::default());
    let cite = sim.first_invocation(&parent, "ui");
    sim.publish(agentmod_core::kernel::PublishRequest {
        plugin: "ui".into(),
        invocation_id: None,
        cite: Some(cite),
        event_name: "go".into(),
        payload: json!({ "text": "x" }),
        ui: None,
        lane: None,
        target_session: None,
    })
    .unwrap();
    let mut outcome = None;
    sim.run(&mut |s: &mut Sim, p: &str, req: &InvokeRequest| {
        if p == "orchestrator" && req.event.event_name == "go" {
            let spec =
                definition(json!({ "runtime": "0.1", "plugins": { "ui": {}, "needy": {} } }));
            let now = s.tick();
            let out = s.k.start_session(
                &StartRequest {
                    plugin: "orchestrator".into(),
                    definition: String::new(),
                    spec: Some(spec),
                    invocation_id: Some(req.invocation_id.clone()),
                    initial: Some(InitialEvent {
                        event_name: "go".into(),
                        payload: json!({ "text": "y" }),
                        ui: None,
                    }),
                    fork_from: None,
                },
                now,
            );
            s.take(out.effects.clone());
            outcome = Some(out);
        }
        InvocationResult::default()
    });
    let out = outcome.unwrap();
    assert!(out.session_id.is_none(), "no session is created");
    assert!(codes(&out.diagnostics).contains(&"starved-consumer"));
    // The orchestrator receives the diagnostics as an event.
    let failed = sim.payloads(&parent, "session-start-failed");
    assert_eq!(failed.len(), 1);
    assert!(
        failed[0]["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["code"] == "starved-consumer" && d["key"] == "missing")
    );
    let view = project(sim.log(&parent));
    let ev = view
        .events
        .iter()
        .find(|e| e.event_name == "session-start-failed")
        .unwrap();
    assert!(ev.invocations.iter().any(|i| i.plugin == "orchestrator"));
    assert_eq!(sim.logs.len(), 1, "only the parent's log exists");
    // A well-formed definition written the same way starts, under its own revision.
    let good = definition(json!({ "runtime": "0.1", "plugins": { "ui": {}, "orchestrator": {} } }));
    let now = sim.tick();
    let ok = sim.k.start_session(
        &StartRequest {
            plugin: "orchestrator".into(),
            definition: String::new(),
            spec: Some(good),
            invocation_id: None,
            initial: None,
            fork_from: None,
        },
        now,
    );
    assert!(ok.session_id.is_some(), "{:?}", ok.error);
    sim.take(ok.effects);
    assert_eq!(sim.saved.len(), 1, "the new revision is persisted");
    assert_eq!(Some(sim.saved[0].revision.clone()), ok.revision);
    let _: Value = json!(ms);
}

#[test]
fn embedded_workflows_are_rejected_explicitly_until_supported() {
    // Not implemented in this prototype (see the PR): an explicit error, never a silent default.
    let ms = json!({ "ui": ui() });
    let msv = common::manifests(ms);
    let mut cfg = deployment(
        &msv,
        json!({
            "d": { "runtime": "0.1", "plugins": { "ui": {}, "research": { "workflow": "researcher" } } },
            "researcher": { "runtime": "0.1", "plugins": { "ui": {} }, "interface": { "receives": { "go": { "text": k(S) } } } }
        }),
    );
    cfg.sources.insert(
        "definitions.d.plugins.research".into(),
        SourceRef {
            file: "agentmod.toml".into(),
            line: 3,
        },
    );
    let c = compile(&cfg, &msv);
    let d = one(&c, "unsupported-workflow");
    assert_eq!(d.plugins, vec!["research"]);
    assert_eq!(d.source.as_ref().map(|s| s.line), Some(3));
}
