//! State-contract tests: undeclared reads/writes, type/size/secret bounds,
//! producer authority, scopes, versions, and deterministic merges.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_graph_state::declare::{
    BranchScopePolicy, DeclarationSet, LastWriterOrdering, MergePolicy, MutabilityPolicy,
    SecurityClassification, VariableDeclaration, VariableScope, VariableType,
};
use agentmod_graph_state::event::GraphStateEvent;
use agentmod_graph_state::state::{
    AssignmentSource, GraphState, GraphStateError, MergeContribution, ReadOutcome, RejectionReason,
};
use agentmod_graph_state::value::{GraphValue, SecretReference};
use agentmod_primitives::SessionId;

fn session() -> SessionId {
    SessionId::from_uuid(uuid::Uuid::nil())
}

fn declaration(name: &str, r#type: VariableType) -> VariableDeclaration {
    VariableDeclaration {
        name: name.to_owned(),
        r#type,
        scope: VariableScope::Run,
        producers: BTreeSet::new(),
        consumers: BTreeSet::new(),
        mutability: MutabilityPolicy::Assignable,
        max_serialized_bytes: 512,
        classification: SecurityClassification::SessionInternal,
        merge_policy: MergePolicy::RejectConflict,
        default: None,
    }
}

fn runtime() -> AssignmentSource {
    AssignmentSource::Runtime
}

fn node(id: &str) -> AssignmentSource {
    AssignmentSource::Node {
        node_id: id.to_owned(),
    }
}

fn base_declarations() -> DeclarationSet {
    let mut set = DeclarationSet::new();
    set.insert(declaration(
        "counter",
        VariableType::UnsignedInteger { min: 0, max: 100 },
    ))
    .expect("declared");
    set.insert(declaration("ready", VariableType::Boolean))
        .expect("declared");
    set
}

#[test]
fn undeclared_reads_and_writes_are_rejected() {
    let (state, _) = GraphState::new(session(), base_declarations()).expect("state");
    assert_eq!(
        state.read("ghost", &VariableScope::Run),
        Err(GraphStateError::UndeclaredRead {
            name: "ghost".into()
        })
    );
    let mut state = state;
    assert_eq!(
        state.assign(
            "ghost",
            GraphValue::Boolean(true),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::UndeclaredWrite {
            name: "ghost".into()
        })
    );
    let reason = state.record_rejection(
        "ghost",
        &VariableScope::Run,
        None,
        RejectionReason::Undeclared,
    );
    assert_eq!(
        reason,
        GraphStateEvent::VariableValidationRejected {
            name: "ghost".into(),
            scope: VariableScope::Run,
            node: None,
            reason: "undeclared".into(),
        }
    );
}

#[test]
fn type_and_size_and_secret_contracts_are_enforced() {
    let mut set = base_declarations();
    let mut secret = declaration("api_key", VariableType::Secret);
    secret.classification = SecurityClassification::Secret;
    set.insert(secret).expect("declared");
    // Type-valid but larger than the serialized-size bound: exercises the
    // size contract rather than the type contract.
    let mut small = declaration("tiny", VariableType::String { max_bytes: 64 });
    small.max_serialized_bytes = 16;
    set.insert(small).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");

    assert_eq!(
        state.assign(
            "counter",
            GraphValue::Boolean(true),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::TypeMismatch {
            name: "counter".into(),
            expected: format!("{:?}", VariableType::UnsignedInteger { min: 0, max: 100 }),
            actual: "boolean",
        })
    );
    assert!(matches!(
        state.assign(
            "counter",
            GraphValue::UnsignedInteger(101),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::TypeMismatch { .. })
    ));
    assert!(matches!(
        state.assign(
            "tiny",
            GraphValue::String("way-too-long".into()),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::SizeExceeded { .. })
    ));
    assert_eq!(
        state.assign(
            "api_key",
            GraphValue::String("plaintext".into()),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::SecretPlaintext {
            name: "api_key".into()
        })
    );
    assert!(
        state
            .assign(
                "api_key",
                GraphValue::SecretReference(
                    SecretReference::new("secret:0001".into()).expect("valid")
                ),
                &runtime(),
                &VariableScope::Run,
                None,
            )
            .is_ok()
    );
}

#[test]
fn immutable_variables_are_write_once_and_versions_advance() {
    let mut set = base_declarations();
    let mut fixed = declaration("fixed", VariableType::Boolean);
    fixed.mutability = MutabilityPolicy::Immutable;
    set.insert(fixed).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    state
        .assign(
            "fixed",
            GraphValue::Boolean(true),
            &runtime(),
            &VariableScope::Run,
            None,
        )
        .expect("first write");
    assert_eq!(
        state.assign(
            "fixed",
            GraphValue::Boolean(false),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::ImmutableWrite {
            name: "fixed".into(),
            scope: VariableScope::Run,
        })
    );
    let events = state
        .assign(
            "counter",
            GraphValue::UnsignedInteger(1),
            &runtime(),
            &VariableScope::Run,
            Some("run-1"),
        )
        .expect("assign");
    assert!(matches!(
        events.as_slice(),
        [GraphStateEvent::VariableAssigned {
            prior_version: 0,
            version: 1,
            ..
        }]
    ));
    let events = state
        .assign(
            "counter",
            GraphValue::UnsignedInteger(2),
            &runtime(),
            &VariableScope::Run,
            Some("run-1"),
        )
        .expect("assign");
    assert!(matches!(
        events.as_slice(),
        [GraphStateEvent::VariableAssigned {
            prior_version: 1,
            version: 2,
            ..
        }]
    ));
    assert_eq!(state.version("counter", &VariableScope::Run), 2);
}

#[test]
fn producer_authority_rejects_undeclared_writers() {
    let mut set = base_declarations();
    let mut produced = declaration("out", VariableType::Boolean);
    produced.producers = ["plan".into()].into_iter().collect();
    set.insert(produced).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    assert_eq!(
        state.assign(
            "out",
            GraphValue::Boolean(true),
            &node("done"),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::NotProducer {
            name: "out".into(),
            node: "done".into(),
        })
    );
    assert_eq!(
        state.assign(
            "out",
            GraphValue::Boolean(true),
            &runtime(),
            &VariableScope::Run,
            None,
        ),
        Err(GraphStateError::NotProducer {
            name: "out".into(),
            node: "runtime".into(),
        })
    );
    assert!(
        state
            .assign(
                "out",
                GraphValue::Boolean(true),
                &node("plan"),
                &VariableScope::Run,
                None,
            )
            .is_ok()
    );
}

#[test]
fn node_scoped_variables_require_their_owning_node() {
    let mut set = DeclarationSet::new();
    let mut local = declaration("local", VariableType::Boolean);
    local.scope = VariableScope::Node {
        node_id: "plan".into(),
    };
    set.insert(local).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    assert_eq!(
        state.assign(
            "local",
            GraphValue::Boolean(true),
            &node("done"),
            &VariableScope::Node {
                node_id: "plan".into()
            },
            None,
        ),
        Err(GraphStateError::NotOwner {
            name: "local".into(),
            node: "done".into(),
            scope: VariableScope::Node {
                node_id: "plan".into()
            },
        })
    );
    assert!(
        state
            .assign(
                "local",
                GraphValue::Boolean(true),
                &node("plan"),
                &VariableScope::Node {
                    node_id: "plan".into()
                },
                None,
            )
            .is_ok()
    );
    assert_eq!(
        state.environment(&VariableScope::Run),
        serde_json::json!({})
    );
}

#[test]
fn branch_scopes_merge_obligations_close_deterministically() {
    let mut set = base_declarations();
    let mut notes = declaration(
        "notes",
        VariableType::List {
            element: Box::new(VariableType::String { max_bytes: 64 }),
            max_len: 10,
        },
    );
    notes.merge_policy = MergePolicy::SetUnion;
    set.insert(notes).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");

    state
        .create_branch_scope("left", BranchScopePolicy::Isolated)
        .expect("branch");
    state
        .assign(
            "notes",
            GraphValue::List(vec![GraphValue::String("a".into())]),
            &runtime(),
            &VariableScope::Branch {
                branch_id: "left".into(),
            },
            None,
        )
        .expect("branch write");
    assert_eq!(
        state.read("notes", &VariableScope::Run).expect("read"),
        ReadOutcome::Unassigned
    );
    assert!(matches!(
        state.close_branch_scope("left"),
        Err(GraphStateError::UnmergedBranchWrites {
            branch_id: ref id,
            ..
        }) if id == "left"
    ));
    state
        .merge_parallel(
            "notes",
            vec![MergeContribution {
                branch_id: "left".into(),
                node_id: None,
                value: GraphValue::List(vec![GraphValue::String("a".into())]),
            }],
        )
        .expect("merge");
    state.close_branch_scope("left").expect("close");
    assert_eq!(
        state.read("notes", &VariableScope::Run).expect("read"),
        ReadOutcome::Value(&GraphValue::List(vec![GraphValue::String("a".into())]))
    );
}

#[test]
fn parallel_merge_policies_are_deterministic() {
    let mut set = DeclarationSet::new();
    set.insert(declaration("shared", VariableType::Boolean))
        .expect("declared");
    let mut list = declaration(
        "acc",
        VariableType::List {
            element: Box::new(VariableType::String { max_bytes: 64 }),
            max_len: 10,
        },
    );
    list.merge_policy = MergePolicy::ListAppend;
    set.insert(list).expect("declared");
    let mut map = declaration(
        "obj",
        VariableType::Map {
            value: Box::new(VariableType::UnsignedInteger { min: 0, max: 100 }),
            max_len: 10,
        },
    );
    map.merge_policy = MergePolicy::ObjectFieldMerge;
    set.insert(map).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    for branch in ["b1", "b2"] {
        state
            .create_branch_scope(branch, BranchScopePolicy::Isolated)
            .expect("branch");
    }
    assert!(matches!(
        state.merge_parallel(
            "shared",
            vec![
                MergeContribution {
                    branch_id: "b1".into(),
                    node_id: None,
                    value: GraphValue::Boolean(true),
                },
                MergeContribution {
                    branch_id: "b2".into(),
                    node_id: None,
                    value: GraphValue::Boolean(false),
                },
            ],
        ),
        Err(GraphStateError::ConflictRejected { .. })
    ));
    state
        .merge_parallel(
            "acc",
            vec![
                MergeContribution {
                    branch_id: "b2".into(),
                    node_id: None,
                    value: GraphValue::List(vec![GraphValue::String("from-b2".into())]),
                },
                MergeContribution {
                    branch_id: "b1".into(),
                    node_id: None,
                    value: GraphValue::List(vec![GraphValue::String("from-b1".into())]),
                },
            ],
        )
        .expect("append");
    assert_eq!(
        state.read("acc", &VariableScope::Run).expect("read"),
        ReadOutcome::Value(&GraphValue::List(vec![
            GraphValue::String("from-b1".into()),
            GraphValue::String("from-b2".into()),
        ]))
    );
    let mut fields_b1 = BTreeMap::new();
    fields_b1.insert("a".to_owned(), GraphValue::UnsignedInteger(1));
    let mut fields_b2 = BTreeMap::new();
    fields_b2.insert("a".to_owned(), GraphValue::UnsignedInteger(2));
    assert!(matches!(
        state.merge_parallel(
            "obj",
            vec![
                MergeContribution {
                    branch_id: "b1".into(),
                    node_id: None,
                    value: GraphValue::Map(fields_b1),
                },
                MergeContribution {
                    branch_id: "b2".into(),
                    node_id: None,
                    value: GraphValue::Map(fields_b2),
                },
            ],
        ),
        Err(GraphStateError::FieldConflict { key, .. }) if key == "a"
    ));
}

#[test]
fn last_writer_uses_declared_deterministic_ordering() {
    let mut set = DeclarationSet::new();
    let mut decision = declaration("decision", VariableType::Boolean);
    decision.merge_policy = MergePolicy::LastWriter {
        ordering: LastWriterOrdering::BranchLexical,
    };
    set.insert(decision).expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    state
        .create_branch_scope("a", BranchScopePolicy::Isolated)
        .expect("branch a");
    state
        .create_branch_scope("b", BranchScopePolicy::Isolated)
        .expect("branch b");
    state
        .merge_parallel(
            "decision",
            vec![
                MergeContribution {
                    branch_id: "a".into(),
                    node_id: None,
                    value: GraphValue::Boolean(false),
                },
                MergeContribution {
                    branch_id: "b".into(),
                    node_id: None,
                    value: GraphValue::Boolean(true),
                },
            ],
        )
        .expect("last writer");
    assert_eq!(
        state.read("decision", &VariableScope::Run).expect("read"),
        ReadOutcome::Value(&GraphValue::Boolean(true))
    );
}

#[test]
fn defaults_apply_at_initialization_and_null_is_optional_only() {
    let mut set = DeclarationSet::new();
    let mut with_default = declaration(
        "mode",
        VariableType::EnumTag {
            tags: ["fast".into(), "safe".into()].into_iter().collect(),
        },
    );
    with_default.default = Some(GraphValue::EnumTag("safe".into()));
    set.insert(with_default).expect("declared");
    set.insert(declaration(
        "note",
        VariableType::Optional(Box::new(VariableType::String { max_bytes: 64 })),
    ))
    .expect("declared");
    let (mut state, _) = GraphState::new(session(), set).expect("state");
    assert_eq!(
        state.read("mode", &VariableScope::Run).expect("read"),
        ReadOutcome::Value(&GraphValue::EnumTag("safe".into()))
    );
    assert!(
        state
            .assign(
                "note",
                GraphValue::Null,
                &runtime(),
                &VariableScope::Run,
                None
            )
            .is_ok()
    );
    assert_eq!(
        state.read("note", &VariableScope::Run).expect("read"),
        ReadOutcome::Null
    );
}
