//! Explicit process-test fixture for seeding checksum-protected file memory.
//!
//! The normal test suite never writes external state: the one ignored test
//! requires the E2E driver to provide every input through environment
//! variables and is invoked only by the style-context process tests.

use std::path::PathBuf;

use agentmod_runtime_dependency::memory::{
    DependencyMemoryWriteRequest, FileMemoryDependency, MemoryDependencyPort,
    SqliteFtsMemoryDependency,
};

#[test]
#[ignore = "invoked explicitly by the style-context process E2E"]
fn seed_file_memory_for_process_e2e() {
    let path = PathBuf::from(required("AGENTMOD_TEST_MEMORY_PATH"));
    let provider = required("AGENTMOD_TEST_MEMORY_PROVIDER");
    let scope = required("AGENTMOD_TEST_MEMORY_SCOPE");
    let source = required("AGENTMOD_TEST_MEMORY_SOURCE");
    let content = required("AGENTMOD_TEST_MEMORY_CONTENT");
    let created_at_millis = required("AGENTMOD_TEST_MEMORY_CREATED_AT_MS")
        .parse()
        .expect("AGENTMOD_TEST_MEMORY_CREATED_AT_MS must be an i64");

    let request = DependencyMemoryWriteRequest {
        scope,
        source,
        content,
        created_at_millis,
        deduplication_key: None,
    };
    let response = match provider.as_str() {
        "file" => FileMemoryDependency::new(path).write(request),
        "sqlite-fts" => SqliteFtsMemoryDependency::new(path).write(request),
        other => panic!("unsupported E2E memory provider: {other}"),
    }
    .expect("seed first-party process-E2E memory");
    assert!(response.retained);
    assert!(!response.id.is_empty());
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set by the E2E driver"))
}
