//! Native dependency integration tests using the deterministic fixture server.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use agentmod_lsp_host_dependency::{
    AuthorizationConfig, DependencyOperation, DependencyPosition, DependencyRange,
    DependencyRequest, DependencyResponse, LspDependencyConfig, LspDependencyPort,
    NativeLspDependency, ServerDefinition, issue_authorization_grant, operation_digest,
};
use tempfile::TempDir;

struct Fixture {
    _temp: TempDir,
    root: PathBuf,
    document: PathBuf,
    authorization: AuthorizationConfig,
}

fn make_fixture() -> Fixture {
    let temp = tempfile::tempdir().expect("tempdir");
    let root = temp.path().to_path_buf();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname='fixture'\nversion='0.1.0'\n",
    )
    .expect("manifest");
    fs::create_dir(root.join("src")).expect("src");
    let document = root.join("src/main.rs");
    fs::write(&document, "fn main() {}\n").expect("document");
    Fixture {
        _temp: temp,
        root,
        document,
        authorization: AuthorizationConfig {
            owner: "owner".into(),
            session: "session".into(),
            key: [7; 32],
            maximum_lifetime: Duration::from_secs(60),
        },
    }
}

fn make_dependency(
    fixture: &Fixture,
    environment: BTreeMap<String, String>,
) -> NativeLspDependency {
    let server = ServerDefinition {
        id: "fixture".into(),
        command: PathBuf::from(env!("CARGO_BIN_EXE_agentmod-lsp-fixture")),
        arguments: Vec::new(),
        extensions: BTreeSet::from([".rs".into()]),
        language_id: "rust".into(),
        environment,
    };
    let config = LspDependencyConfig::new(
        fixture.root.clone(),
        vec![server],
        1024 * 1024,
        Duration::from_secs(3),
    )
    .expect("config")
    .with_authorization(fixture.authorization.clone());
    NativeLspDependency::new(config)
}

fn execute(
    dependency: &NativeLspDependency,
    authorization: &AuthorizationConfig,
    nonce: usize,
    operation: DependencyOperation,
) -> Result<DependencyResponse, agentmod_lsp_host_dependency::DependencyError> {
    let digest = operation_digest(&operation).expect("digest");
    let call_id = format!("call-{nonce}");
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_secs()
        + 30;
    let grant = issue_authorization_grant(
        authorization,
        &call_id,
        &digest,
        &format!("nonce-{nonce}"),
        expiry,
    );
    dependency.execute(DependencyRequest::Execute {
        cancellation_key: format!("cancel-{nonce}"),
        call_id,
        normalized_digest: digest,
        authorization_grant: grant,
        operation,
    })
}

#[test]
#[allow(clippy::too_many_lines)]
fn fixture_supports_all_required_operations_and_lifecycle() {
    let fixture = make_fixture();
    let dependency = make_dependency(&fixture, BTreeMap::new());
    let document = fixture.document.display().to_string();
    let position = DependencyPosition {
        line: 0,
        character: 1,
    };
    let range = DependencyRange {
        start: DependencyPosition {
            line: 0,
            character: 0,
        },
        end: DependencyPosition {
            line: 0,
            character: 2,
        },
    };
    let operations = vec![
        DependencyOperation::ProjectRoot {
            path: document.clone(),
        },
        DependencyOperation::Diagnostics {
            document: document.clone(),
        },
        DependencyOperation::DocumentSymbols {
            document: document.clone(),
        },
        DependencyOperation::WorkspaceSymbols {
            query: "main".into(),
        },
        DependencyOperation::Definition {
            document: document.clone(),
            position,
        },
        DependencyOperation::References {
            document: document.clone(),
            position,
            include_declaration: true,
        },
        DependencyOperation::Hover {
            document: document.clone(),
            position,
        },
        DependencyOperation::SignatureHelp {
            document: document.clone(),
            position,
        },
        DependencyOperation::Rename {
            document: document.clone(),
            position,
            new_name: "renamed".into(),
        },
        DependencyOperation::Formatting {
            document: document.clone(),
            tab_size: 4,
            insert_spaces: true,
        },
        DependencyOperation::CodeActions {
            document,
            range,
            diagnostics: vec!["fixture".into()],
        },
    ];
    let mut responses = Vec::new();
    for (index, operation) in operations.into_iter().enumerate() {
        responses.push(
            execute(&dependency, &fixture.authorization, index, operation).expect("operation"),
        );
    }
    assert!(matches!(
        responses[0],
        DependencyResponse::ProjectRoot { .. }
    ));
    assert!(matches!(responses[1], DependencyResponse::Diagnostics(ref v) if v.len() == 1));
    assert!(matches!(responses[2], DependencyResponse::Symbols(ref v) if v.len() == 1));
    assert!(matches!(responses[3], DependencyResponse::Symbols(ref v) if v.len() == 1));
    assert!(matches!(responses[4], DependencyResponse::Locations(ref v) if v.len() == 1));
    assert!(matches!(responses[5], DependencyResponse::Locations(ref v) if v.len() == 1));
    assert!(matches!(responses[6], DependencyResponse::Hover(Some(_))));
    assert!(matches!(
        responses[7],
        DependencyResponse::Signature(Some(_))
    ));
    assert!(matches!(responses[8], DependencyResponse::WorkspaceEdit(_)));
    assert!(matches!(responses[9], DependencyResponse::TextEdits(ref v) if v.len() == 1));
    assert!(matches!(responses[10], DependencyResponse::CodeActions(ref v) if v.len() == 1));
    assert!(matches!(
        dependency
            .execute(DependencyRequest::Health { document: None })
            .expect("health"),
        DependencyResponse::Health {
            restart_count: 0,
            ..
        }
    ));
    assert_eq!(
        dependency
            .execute(DependencyRequest::Shutdown)
            .expect("shutdown"),
        DependencyResponse::Shutdown
    );
}

#[test]
fn authorization_is_deny_by_default_and_single_use() {
    let fixture = make_fixture();
    let config = LspDependencyConfig::new(
        fixture.root.clone(),
        Vec::new(),
        1024,
        Duration::from_secs(1),
    )
    .expect("config");
    let denied = NativeLspDependency::new(config);
    let operation = DependencyOperation::ProjectRoot {
        path: fixture.document.display().to_string(),
    };
    assert!(execute(&denied, &fixture.authorization, 1, operation).is_err());

    let dependency = make_dependency(&fixture, BTreeMap::new());
    let operation = DependencyOperation::ProjectRoot {
        path: fixture.document.display().to_string(),
    };
    let digest = operation_digest(&operation).expect("digest");
    let call_id = "one-time".to_owned();
    let grant = issue_authorization_grant(
        &fixture.authorization,
        &call_id,
        &digest,
        "same-nonce",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_secs()
            + 30,
    );
    let request = || DependencyRequest::Execute {
        cancellation_key: "cancel".into(),
        call_id: call_id.clone(),
        normalized_digest: digest.clone(),
        authorization_grant: grant.clone(),
        operation: operation.clone(),
    };
    dependency.execute(request()).expect("first");
    assert!(dependency.execute(request()).is_err());
}

#[test]
fn cancellation_and_one_restart_are_bounded() {
    let fixture = make_fixture();
    let dependency = Arc::new(make_dependency(&fixture, BTreeMap::new()));
    let operation = DependencyOperation::WorkspaceSymbols {
        query: "slow".into(),
    };
    let digest = operation_digest(&operation).expect("digest");
    let grant = issue_authorization_grant(
        &fixture.authorization,
        "slow-call",
        &digest,
        "slow-nonce",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_secs()
            + 30,
    );
    let worker = {
        let dependency = Arc::clone(&dependency);
        thread::spawn(move || {
            dependency.execute(DependencyRequest::Execute {
                cancellation_key: "slow-cancel".into(),
                call_id: "slow-call".into(),
                normalized_digest: digest,
                authorization_grant: grant,
                operation,
            })
        })
    };
    thread::sleep(Duration::from_millis(100));
    assert_eq!(
        dependency
            .execute(DependencyRequest::Cancel {
                cancellation_key: "slow-cancel".into()
            })
            .expect("cancel"),
        DependencyResponse::Cancelled { active: true }
    );
    assert!(worker.join().expect("join").is_err());

    let restart_fixture = make_fixture();
    let marker = restart_fixture.root.join("crash.marker");
    let restart = make_dependency(
        &restart_fixture,
        BTreeMap::from([(
            "AGENTMOD_LSP_FIXTURE_CRASH_MARKER".into(),
            marker.display().to_string(),
        )]),
    );
    assert!(matches!(
        execute(
            &restart,
            &restart_fixture.authorization,
            77,
            DependencyOperation::WorkspaceSymbols {
                query: "crash-once".into()
            }
        )
        .expect("restart succeeds"),
        DependencyResponse::Symbols(_)
    ));
    assert!(matches!(
        restart
            .execute(DependencyRequest::Health { document: None })
            .expect("health"),
        DependencyResponse::Health {
            restart_count: 1,
            ..
        }
    ));
}
