use std::collections::BTreeSet;
use std::path::PathBuf;

#[test]
fn failing_fixture_emits_stable_diagnostic_codes() {
    let manifest = fixture_root().join("Cargo.toml");
    let report = xtask::architecture::validate_manifest(&manifest)
        .expect("intentional-violations fixture must be inspectable");
    let codes: BTreeSet<_> = report
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect();

    let expected = BTreeSet::from([
        xtask::architecture::MISSING_LAYER_METADATA,
        xtask::architecture::MISSING_PROCESS_METADATA,
        xtask::architecture::SKIPPED_LAYER,
        xtask::architecture::UPWARD_LAYER,
        xtask::architecture::CROSS_PROCESS_INTERNAL,
        xtask::architecture::PROTOCOL_IN_DOMAIN_LAYER,
        xtask::architecture::EXTERNAL_API_ABOVE_DEPENDENCY,
        xtask::architecture::CROSS_LAYER_TYPE_ALIAS,
        xtask::architecture::CROSS_LAYER_REEXPORT,
        xtask::architecture::UPWARD_CALLBACK,
        xtask::architecture::PATH_METADATA_MISMATCH,
        xtask::architecture::INCOMPLETE_PROCESS_LAYERS,
    ]);
    assert_eq!(codes, expected);
}

#[test]
fn diagnostics_are_sorted_and_readable() {
    let manifest = fixture_root().join("Cargo.toml");
    let report = xtask::architecture::validate_manifest(&manifest)
        .expect("intentional-violations fixture must be inspectable");
    let codes: Vec<_> = report
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code)
        .collect();
    assert!(codes.windows(2).all(|pair| pair[0] <= pair[1]));

    for diagnostic in report.diagnostics {
        let rendered = diagnostic.to_string();
        assert!(rendered.starts_with(&format!("error[{}]", diagnostic.code)));
        assert!(rendered.contains("\n  help: "));
        assert!(!diagnostic.message.is_empty());
        assert!(!diagnostic.help.is_empty());
    }
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("architecture")
        .join("intentional-violations")
}
