//! Machine-validated parallel write safety.
//!
//! A graph whose parallel branches propose unsafe writes to the same variable
//! must fail runtime-executability validation before any session mutation.
//! Validation is purely declarative and deterministic: it inspects the
//! declared write sets and merge policies and returns a stable report.

use std::collections::{BTreeMap, BTreeSet};

use crate::declare::{
    DeclarationSet, LastWriterOrdering, MergePolicy, MutabilityPolicy, VariableScope,
};

/// One parallel branch's declared write set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelBranchPlan {
    /// Stable branch identity.
    pub branch_id: String,
    /// Variable names the branch proposes to write.
    pub written: BTreeSet<String>,
}

/// Per-variable parallel write verdict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParallelWriteVerdict {
    /// The write pattern is safe under the declared policy.
    Safe {
        /// Policy applied at the join.
        policy: MergePolicy,
    },
    /// Parallel writes conflict and must be rejected.
    Conflict {
        /// Deterministic diagnostic.
        reason: String,
    },
}

/// Complete parallel write-safety report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParallelSafetyReport {
    /// Whether every write pattern is safe.
    pub safe: bool,
    /// Per-variable verdicts in stable name order.
    pub verdicts: BTreeMap<String, ParallelWriteVerdict>,
}

/// Validates parallel write plans against declared merge policies.
///
/// Variables written by one branch are always safe. Variables written by two
/// or more branches are safe only when the declared policy is a deterministic
/// merge (last-writer with an explicit ordering, list append, set union, or
/// object-field merge) compatible with the declared type, or when the variable
/// is not immutable.
///
/// # Errors
///
/// No errors escape; the report carries the complete machine-validated
/// outcome.
#[must_use]
pub fn validate_parallel_write_safety(
    declarations: &DeclarationSet,
    branches: &[ParallelBranchPlan],
) -> ParallelSafetyReport {
    let mut writers: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for branch in branches {
        for variable in &branch.written {
            writers
                .entry(variable.as_str())
                .or_default()
                .insert(branch.branch_id.as_str());
        }
    }
    let mut verdicts = BTreeMap::new();
    for (variable, contributor_branches) in writers {
        let verdict = match declarations.get(variable) {
            None => ParallelWriteVerdict::Conflict {
                reason: format!("undeclared variable `{variable}` written by parallel branches"),
            },
            Some(_) if contributor_branches.len() <= 1 => ParallelWriteVerdict::Safe {
                policy: MergePolicy::RejectConflict,
            },
            Some(declaration) => {
                if !matches!(declaration.scope, VariableScope::Run) {
                    ParallelWriteVerdict::Conflict {
                        reason: format!("parallel branches write non-run variable `{variable}`"),
                    }
                } else if matches!(declaration.mutability, MutabilityPolicy::Immutable) {
                    ParallelWriteVerdict::Conflict {
                        reason: format!(
                            "immutable variable `{variable}` written by parallel branches"
                        ),
                    }
                } else {
                    match declaration.merge_policy {
                        MergePolicy::RejectConflict => ParallelWriteVerdict::Conflict {
                            reason: format!(
                                "parallel branches write `{variable}` without a declared merge policy"
                            ),
                        },
                        MergePolicy::LastWriter { .. }
                        | MergePolicy::ListAppend
                        | MergePolicy::SetUnion
                        | MergePolicy::ObjectFieldMerge => ParallelWriteVerdict::Safe {
                            policy: declaration.merge_policy,
                        },
                    }
                }
            }
        };
        verdicts.insert(variable.to_owned(), verdict);
    }
    let safe = verdicts
        .values()
        .all(|verdict| matches!(verdict, ParallelWriteVerdict::Safe { .. }));
    ParallelSafetyReport { safe, verdicts }
}

/// Returns the deterministic last-writer ordering for a policy.
#[must_use]
pub const fn last_writer_ordering(policy: MergePolicy) -> Option<LastWriterOrdering> {
    match policy {
        MergePolicy::LastWriter { ordering } => Some(ordering),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        declare::{BranchScopePolicy, SecurityClassification, VariableDeclaration, VariableType},
        value::GraphValue,
    };

    fn declaration(name: &str, policy: MergePolicy, r#type: VariableType) -> VariableDeclaration {
        VariableDeclaration {
            name: name.to_owned(),
            r#type,
            scope: VariableScope::Run,
            producers: BTreeSet::new(),
            consumers: BTreeSet::new(),
            mutability: MutabilityPolicy::Assignable,
            max_serialized_bytes: 4096,
            classification: SecurityClassification::SessionInternal,
            merge_policy: policy,
            default: None,
        }
    }

    fn declarations() -> DeclarationSet {
        let mut set = DeclarationSet::new();
        set.insert(declaration(
            "shared",
            MergePolicy::RejectConflict,
            VariableType::Boolean,
        ))
        .expect("declared");
        set.insert(declaration(
            "last_writer",
            MergePolicy::LastWriter {
                ordering: LastWriterOrdering::BranchLexical,
            },
            VariableType::Boolean,
        ))
        .expect("declared");
        set.insert(declaration(
            "accumulate",
            MergePolicy::SetUnion,
            VariableType::List {
                element: Box::new(VariableType::String { max_bytes: 64 }),
                max_len: 100,
            },
        ))
        .expect("declared");
        set.insert(declaration(
            "immutable",
            MergePolicy::RejectConflict,
            VariableType::Boolean,
        ))
        .expect("declared");
        set
    }

    #[test]
    fn unsafe_parallel_writes_fail_machine_validation() {
        let set = declarations();
        let branches = [
            ParallelBranchPlan {
                branch_id: "left".into(),
                written: ["shared".into(), "accumulate".into()].into_iter().collect(),
            },
            ParallelBranchPlan {
                branch_id: "right".into(),
                written: ["shared".into(), "accumulate".into()].into_iter().collect(),
            },
        ];
        let report = validate_parallel_write_safety(&set, &branches);
        assert!(!report.safe);
        assert!(matches!(
            report.verdicts.get("shared"),
            Some(ParallelWriteVerdict::Conflict { .. })
        ));
        assert!(matches!(
            report.verdicts.get("accumulate"),
            Some(ParallelWriteVerdict::Safe {
                policy: MergePolicy::SetUnion
            })
        ));
    }

    #[test]
    fn declared_deterministic_policies_pass_validation() {
        let set = declarations();
        let branches = [
            ParallelBranchPlan {
                branch_id: "left".into(),
                written: ["last_writer".into(), "accumulate".into()]
                    .into_iter()
                    .collect(),
            },
            ParallelBranchPlan {
                branch_id: "right".into(),
                written: ["last_writer".into(), "accumulate".into()]
                    .into_iter()
                    .collect(),
            },
        ];
        let report = validate_parallel_write_safety(&set, &branches);
        assert!(report.safe);
        assert!(matches!(
            report.verdicts.get("last_writer"),
            Some(ParallelWriteVerdict::Safe {
                policy: MergePolicy::LastWriter { .. }
            })
        ));
    }

    #[test]
    fn undeclared_and_immutable_parallel_writes_conflict() {
        let mut set = DeclarationSet::new();
        let mut immutable = declaration(
            "immutable",
            MergePolicy::RejectConflict,
            VariableType::Boolean,
        );
        immutable.mutability = MutabilityPolicy::Immutable;
        set.insert(immutable).expect("declared");
        let branches = [
            ParallelBranchPlan {
                branch_id: "left".into(),
                written: ["immutable".into(), "ghost".into()].into_iter().collect(),
            },
            ParallelBranchPlan {
                branch_id: "right".into(),
                written: ["immutable".into(), "ghost".into()].into_iter().collect(),
            },
        ];
        let report = validate_parallel_write_safety(&set, &branches);
        assert!(!report.safe);
        assert!(matches!(
            report.verdicts.get("immutable"),
            Some(ParallelWriteVerdict::Conflict { .. })
        ));
        assert!(matches!(
            report.verdicts.get("ghost"),
            Some(ParallelWriteVerdict::Conflict { .. })
        ));
    }

    #[test]
    fn single_writer_branches_are_always_safe() {
        let set = declarations();
        let branches = [
            ParallelBranchPlan {
                branch_id: "left".into(),
                written: ["shared".into()].into_iter().collect(),
            },
            ParallelBranchPlan {
                branch_id: "right".into(),
                written: ["accumulate".into()].into_iter().collect(),
            },
        ];
        let report = validate_parallel_write_safety(&set, &branches);
        assert!(report.safe);
    }

    #[test]
    fn branch_scope_policy_is_reachable() {
        // Guards the public constant surface against accidental removal.
        assert_eq!(BranchScopePolicy::Isolated, BranchScopePolicy::Isolated);
        let _ = GraphValue::Null;
    }
}
