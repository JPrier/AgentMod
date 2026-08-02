//! Canonical graph variable state: scopes, reads, writes, and merge semantics.
//!
//! State is a pure function of committed events. Undeclared reads and writes
//! are rejected, values are validated against their declarations before any
//! mutation, and parallel merges follow the declared deterministic policy.

use std::collections::{BTreeMap, BTreeSet};

use agentmod_event_model::ArtifactReference;
use agentmod_primitives::{ContentHash, SessionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    declare::{
        BranchScopePolicy, DeclarationSet, LastWriterOrdering, MergePolicy, MutabilityPolicy,
        SecurityClassification, VariableDeclaration, VariableScope,
    },
    event::GraphStateEvent,
    value::{GraphValue, canonical_value_bytes},
};

/// Who performed an assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssignmentSource {
    /// A compiled graph node.
    Node {
        /// Stable node identity.
        node_id: String,
    },
    /// The runtime itself (runtime-owned counters, bindings, defaults).
    Runtime,
}

/// Outcome of a canonical read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadOutcome<'a> {
    /// Assigned value.
    Value(&'a GraphValue),
    /// Assigned canonical null.
    Null,
    /// Declared but not assigned in the requested scope.
    Unassigned,
}

impl ReadOutcome<'_> {
    /// Returns whether the variable is present in the requested scope.
    #[must_use]
    pub const fn present(&self) -> bool {
        !matches!(self, Self::Unassigned)
    }
}

/// One merged branch contribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeContribution {
    /// Contributor branch identity.
    pub branch_id: String,
    /// Contributing node, when node-produced.
    pub node_id: Option<String>,
    /// Exact contributed value.
    pub value: GraphValue,
}

/// One stored variable entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VariableEntry {
    /// Declared name.
    pub name: String,
    /// Current value; `None` when unassigned.
    pub value: Option<GraphValue>,
    /// Hash of the exact canonical value bytes.
    pub value_hash: Option<ContentHash>,
    /// Artifact reference when the value is artifact-backed.
    pub artifact_reference: Option<ArtifactReference>,
    /// Producing node, when node-produced.
    pub producer_node: Option<String>,
    /// Deterministically ordered contributor identities after a merge.
    pub merged_from: Vec<String>,
}

impl VariableEntry {
    fn assigned(name: &str, value: GraphValue, producer_node: Option<&str>) -> Self {
        let value_hash = ContentHash::digest(&canonical_value_bytes(&value));
        let artifact_reference = value.artifact_reference().cloned();
        Self {
            name: name.to_owned(),
            value: Some(value),
            value_hash: Some(value_hash),
            artifact_reference,
            producer_node: producer_node.map(str::to_owned),
            merged_from: Vec::new(),
        }
    }
}

/// One branch-local scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BranchScope {
    /// Stable branch identity.
    pub branch_id: String,
    /// Creation policy.
    pub policy: BranchScopePolicy,
    /// Branch-local entries.
    pub variables: BTreeMap<String, VariableEntry>,
    /// Run-scoped variables written inside the branch (merge obligations).
    pub written_run: BTreeSet<String>,
    /// Branch-scoped variables declared by the graph.
    pub declared_branch: BTreeSet<String>,
    /// Whether the scope was closed.
    pub closed: bool,
}

/// Canonical graph variable state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphState {
    session_id: SessionId,
    declarations: DeclarationSet,
    run: BTreeMap<String, VariableEntry>,
    branches: BTreeMap<String, BranchScope>,
    nodes: BTreeMap<String, BTreeMap<String, VariableEntry>>,
    versions: BTreeMap<ScopeKey, u64>,
    rejected: u64,
    empty: BTreeMap<String, VariableEntry>,
}

/// Version counter key: one counter per variable per scope.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ScopeKey {
    /// Owning scope.
    pub scope: VariableScope,
    /// Variable name.
    pub name: String,
}

impl GraphState {
    /// Creates an uninitialized empty state for reducer bootstrap.
    #[must_use]
    pub fn empty(session_id: SessionId) -> Self {
        Self {
            session_id,
            declarations: DeclarationSet::new(),
            run: BTreeMap::new(),
            branches: BTreeMap::new(),
            nodes: BTreeMap::new(),
            versions: BTreeMap::new(),
            rejected: 0,
            empty: BTreeMap::new(),
        }
    }

    /// Initializes state from a validated declaration set, applying defaults.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError::InvalidDeclarations`] when a declaration is
    /// malformed or its default cannot be applied.
    pub fn new(
        session_id: SessionId,
        declarations: DeclarationSet,
    ) -> Result<(Self, Vec<GraphStateEvent>), GraphStateError> {
        let mut state = Self {
            session_id,
            declarations,
            run: BTreeMap::new(),
            branches: BTreeMap::new(),
            nodes: BTreeMap::new(),
            versions: BTreeMap::new(),
            rejected: 0,
            empty: BTreeMap::new(),
        };
        let mut events = Vec::new();
        let declarations_vec = state.declarations.iter().cloned().collect::<Vec<_>>();
        for declaration in &declarations_vec {
            if let Some(default) = &declaration.default {
                state.apply_default(declaration, default)?;
            }
        }
        let declarations_hash =
            ContentHash::digest(&canonical_declarations_bytes(&declarations_vec));
        events.push(GraphStateEvent::VariablesInitialized {
            session_id,
            declarations_hash,
            declarations: declarations_vec,
        });
        Ok((state, events))
    }

    fn apply_default(
        &mut self,
        declaration: &VariableDeclaration,
        default: &GraphValue,
    ) -> Result<(), GraphStateError> {
        validate_value_for(declaration, default)?;
        let entry = VariableEntry::assigned(&declaration.name, default.clone(), None);
        let store = self.scope_store(&declaration.scope)?;
        store.insert(declaration.name.clone(), entry);
        let key = ScopeKey {
            scope: declaration.scope.clone(),
            name: declaration.name.clone(),
        };
        self.versions.insert(key, 1);
        Ok(())
    }

    /// Returns the session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns the declaration set.
    #[must_use]
    pub const fn declarations(&self) -> &DeclarationSet {
        &self.declarations
    }

    /// Returns the declaration for a name.
    #[must_use]
    pub fn declaration(&self, name: &str) -> Option<&VariableDeclaration> {
        self.declarations.get(name)
    }

    /// Reads a variable from a scope.
    ///
    /// Undeclared reads are rejected. Branch reads fall back to immutable
    /// shared run values; every other combination returns exactly the stored
    /// value, null, or unassigned.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError::UndeclaredRead`] or
    /// [`GraphStateError::UnknownScope`] when the variable or scope is absent.
    pub fn read<'a>(
        &'a self,
        name: &str,
        scope: &VariableScope,
    ) -> Result<ReadOutcome<'a>, GraphStateError> {
        let declaration =
            self.declarations
                .get(name)
                .ok_or_else(|| GraphStateError::UndeclaredRead {
                    name: name.to_owned(),
                })?;
        let entry =
            match scope {
                VariableScope::Run => self.run.get(name),
                VariableScope::Branch { branch_id } => {
                    let branch = self.branches.get(branch_id).ok_or_else(|| {
                        GraphStateError::UnknownScope {
                            scope: scope.clone(),
                        }
                    })?;
                    match branch.variables.get(name) {
                        Some(entry) => Some(entry),
                        None => self.run.get(name).filter(|_| {
                            matches!(declaration.mutability, MutabilityPolicy::Immutable)
                        }),
                    }
                }
                VariableScope::Node { node_id } => self
                    .nodes
                    .get(node_id)
                    .and_then(|variables| variables.get(name)),
            };
        match entry {
            Some(entry) => match entry.value.as_ref() {
                Some(GraphValue::Null) => Ok(ReadOutcome::Null),
                Some(value) => Ok(ReadOutcome::Value(value)),
                None => Ok(ReadOutcome::Unassigned),
            },
            None => Ok(ReadOutcome::Unassigned),
        }
    }

    /// Returns the current version of a variable in a scope.
    #[must_use]
    pub fn version(&self, name: &str, scope: &VariableScope) -> u64 {
        self.versions
            .get(&ScopeKey {
                scope: scope.clone(),
                name: name.to_owned(),
            })
            .copied()
            .unwrap_or(0)
    }

    /// Assigns a validated value in a scope, producing committed events.
    ///
    /// The write is rejected before any mutation when the variable is
    /// undeclared, the value violates its type/size/classification contract,
    /// the producer is not a declared producer, or the variable is immutable
    /// and already assigned.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError`] without mutating state.
    pub fn assign(
        &mut self,
        name: &str,
        value: GraphValue,
        producer: &AssignmentSource,
        scope: &VariableScope,
        style_run: Option<&str>,
    ) -> Result<Vec<GraphStateEvent>, GraphStateError> {
        let declaration =
            self.declarations
                .get(name)
                .ok_or_else(|| GraphStateError::UndeclaredWrite {
                    name: name.to_owned(),
                })?;
        validate_value_for(declaration, &value)?;
        Self::validate_producer(declaration, producer, scope)?;
        self.validate_scope_exists(scope)?;
        let entry = self.scope_entry(scope)?.get(name);
        if matches!(declaration.mutability, MutabilityPolicy::Immutable) && entry.is_some() {
            return Err(GraphStateError::ImmutableWrite {
                name: name.to_owned(),
                scope: scope.clone(),
            });
        }
        let prior_version = self.version(name, scope);
        let version = prior_version + 1;
        let producer_node = match producer {
            AssignmentSource::Node { node_id } => Some(node_id.clone()),
            AssignmentSource::Runtime => None,
        };
        let value_hash = ContentHash::digest(&canonical_value_bytes(&value));
        let artifact_reference = value.artifact_reference().cloned();
        self.scope_store(scope)?.insert(
            name.to_owned(),
            VariableEntry::assigned(name, value.clone(), producer_node.as_deref()),
        );
        if let Some(branch) = self.branch_mut(scope) {
            branch.written_run.insert(name.to_owned());
        }
        self.versions.insert(
            ScopeKey {
                scope: scope.clone(),
                name: name.to_owned(),
            },
            version,
        );
        Ok(vec![GraphStateEvent::VariableAssigned {
            name: name.to_owned(),
            scope: scope.clone(),
            style_run: style_run.map(str::to_owned),
            producer_node,
            prior_version,
            version,
            value,
            value_hash,
            artifact_reference,
        }])
    }

    /// Records a rejected assignment as a canonical audit event.
    ///
    /// The event carries no state change; it exists so rejection is replayable.
    #[must_use]
    pub fn record_rejection(
        &self,
        name: &str,
        scope: &VariableScope,
        producer: Option<&AssignmentSource>,
        reason: RejectionReason,
    ) -> GraphStateEvent {
        GraphStateEvent::VariableValidationRejected {
            name: name.to_owned(),
            scope: scope.clone(),
            node: match producer {
                Some(AssignmentSource::Node { node_id }) => Some(node_id.clone()),
                _ => None,
            },
            reason: reason.to_string(),
        }
    }

    /// Creates a branch-local scope.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError::DuplicateScope`] when the branch already
    /// exists or [`GraphStateError::InvalidDeclarations`] when the graph
    /// declares branch-scoped variables for a missing branch.
    pub fn create_branch_scope(
        &mut self,
        branch_id: &str,
        policy: BranchScopePolicy,
    ) -> Result<Vec<GraphStateEvent>, GraphStateError> {
        if self.branches.contains_key(branch_id) {
            return Err(GraphStateError::DuplicateScope {
                scope: VariableScope::Branch {
                    branch_id: branch_id.to_owned(),
                },
            });
        }
        let declared_branch: BTreeSet<_> = self
            .declarations
            .iter()
            .filter(|declaration| {
                matches!(&declaration.scope, VariableScope::Branch { branch_id: declared }
                    if declared == branch_id)
            })
            .map(|declaration| declaration.name.clone())
            .collect();
        let variables = if matches!(policy, BranchScopePolicy::CopyOnWrite) {
            self.run.clone()
        } else {
            BTreeMap::new()
        };
        self.branches.insert(
            branch_id.to_owned(),
            BranchScope {
                branch_id: branch_id.to_owned(),
                policy,
                variables,
                written_run: BTreeSet::new(),
                declared_branch,
                closed: false,
            },
        );
        Ok(vec![GraphStateEvent::BranchScopeCreated {
            branch_id: branch_id.to_owned(),
            policy,
        }])
    }

    /// Merges deterministic branch contributions into a run variable.
    ///
    /// Contributors are sorted by branch identity, then the declared merge
    /// policy decides: reject conflicts, deterministic last-writer, list
    /// append, set union, or object-field merge. The merged value is
    /// re-validated against the declaration before the event is produced.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError`] without mutating state when the variable is
    /// not run-scoped, a contributor scope is missing, or the merge conflicts.
    pub fn merge_parallel(
        &mut self,
        name: &str,
        contributions: Vec<MergeContribution>,
    ) -> Result<Vec<GraphStateEvent>, GraphStateError> {
        let declaration =
            self.declarations
                .get(name)
                .ok_or_else(|| GraphStateError::UndeclaredWrite {
                    name: name.to_owned(),
                })?;
        if !matches!(declaration.scope, VariableScope::Run) {
            return Err(GraphStateError::ScopeNotMergeable {
                name: name.to_owned(),
            });
        }
        let mut contributions = contributions;
        if contributions.is_empty() {
            return Err(GraphStateError::EmptyMerge {
                name: name.to_owned(),
            });
        }
        contributions.sort_by(|left, right| left.branch_id.cmp(&right.branch_id));
        for contribution in &contributions {
            let branch = self.branches.get(&contribution.branch_id).ok_or_else(|| {
                GraphStateError::UnknownScope {
                    scope: VariableScope::Branch {
                        branch_id: contribution.branch_id.clone(),
                    },
                }
            })?;
            if branch.closed {
                return Err(GraphStateError::ClosedScope {
                    scope: VariableScope::Branch {
                        branch_id: contribution.branch_id.clone(),
                    },
                });
            }
            validate_value_for(declaration, &contribution.value)?;
        }
        let merged = merge_values(declaration, &contributions)?;
        validate_value_for(declaration, &merged)?;
        let prior_version = self.version(name, &VariableScope::Run);
        let version = prior_version + 1;
        let value_hash = ContentHash::digest(&canonical_value_bytes(&merged));
        let merged_from = contributions
            .iter()
            .map(|contribution| contribution.branch_id.clone())
            .collect::<Vec<_>>();
        let mut entry = VariableEntry::assigned(
            name,
            merged.clone(),
            contributions
                .iter()
                .find_map(|contribution| contribution.node_id.as_deref()),
        );
        entry.merged_from.clone_from(&merged_from);
        self.run.insert(name.to_owned(), entry);
        self.versions.insert(
            ScopeKey {
                scope: VariableScope::Run,
                name: name.to_owned(),
            },
            version,
        );
        for contribution in &contributions {
            if let Some(branch) = self.branches.get_mut(&contribution.branch_id) {
                branch.written_run.remove(name);
            }
        }
        Ok(vec![GraphStateEvent::VariableMerged {
            name: name.to_owned(),
            scope: VariableScope::Run,
            policy: declaration.merge_policy,
            contributors: merged_from,
            version,
            value: merged,
            value_hash,
        }])
    }

    /// Closes a branch-local scope after its run writes were merged.
    ///
    /// Branch-scoped variables are dropped; run-scoped writes must already be
    /// merged through [`Self::merge_parallel`], otherwise the close fails
    /// closed with the outstanding variables.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError`] when the branch is unknown, already closed,
    /// or still owns unmerged run-scoped writes.
    pub fn close_branch_scope(
        &mut self,
        branch_id: &str,
    ) -> Result<Vec<GraphStateEvent>, GraphStateError> {
        let branch =
            self.branches
                .get_mut(branch_id)
                .ok_or_else(|| GraphStateError::UnknownScope {
                    scope: VariableScope::Branch {
                        branch_id: branch_id.to_owned(),
                    },
                })?;
        if branch.closed {
            return Err(GraphStateError::ClosedScope {
                scope: VariableScope::Branch {
                    branch_id: branch_id.to_owned(),
                },
            });
        }
        let outstanding: Vec<_> = branch.written_run.iter().cloned().collect();
        if !outstanding.is_empty() {
            return Err(GraphStateError::UnmergedBranchWrites {
                branch_id: branch_id.to_owned(),
                variables: outstanding,
            });
        }
        branch.closed = true;
        self.branches.remove(branch_id);
        Ok(vec![GraphStateEvent::BranchScopeClosed {
            branch_id: branch_id.to_owned(),
        }])
    }

    /// Returns a deterministic environment for condition evaluation.
    ///
    /// Only assigned declared variables are projected; unassigned variables
    /// are absent, so conditions can distinguish missing required input from
    /// an ineligible value. Objects are sorted by declared name.
    #[must_use]
    pub fn environment(&self, scope: &VariableScope) -> serde_json::Value {
        let mut root = serde_json::Map::new();
        match scope {
            VariableScope::Run => {
                for (name, entry) in &self.run {
                    if let Some(value) = &entry.value {
                        root.insert(name.clone(), graph_value_to_json(value));
                    }
                }
            }
            VariableScope::Branch { branch_id } => {
                if let Some(branch) = self.branches.get(branch_id) {
                    for (name, entry) in &branch.variables {
                        if let Some(value) = &entry.value {
                            root.insert(name.clone(), graph_value_to_json(value));
                        }
                    }
                }
                for (name, entry) in &self.run {
                    if root.contains_key(name) {
                        continue;
                    }
                    let Some(declaration) = self.declarations.get(name) else {
                        continue;
                    };
                    if matches!(declaration.mutability, MutabilityPolicy::Immutable)
                        && let Some(value) = &entry.value
                    {
                        root.insert(name.clone(), graph_value_to_json(value));
                    }
                }
            }
            VariableScope::Node { node_id } => {
                if let Some(variables) = self.nodes.get(node_id) {
                    for (name, entry) in variables {
                        if let Some(value) = &entry.value {
                            root.insert(name.clone(), graph_value_to_json(value));
                        }
                    }
                }
            }
        }
        serde_json::Value::Object(root)
    }

    /// Returns the number of recorded rejection events (audit counter).
    #[must_use]
    pub fn rejected_count(&self) -> u64 {
        self.rejected
    }

    /// Applies a canonical event during replay.
    ///
    /// # Errors
    ///
    /// Returns [`GraphStateError::InconsistentEvent`] when the event cannot be
    /// applied exactly to the current state.
    // The reducer is deliberately one exhaustive match: each arm mirrors the
    // committing operation exactly, and splitting it would hide ordering bugs.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn apply_event(&mut self, event: &GraphStateEvent) -> Result<(), GraphStateError> {
        match event {
            GraphStateEvent::VariablesInitialized {
                session_id,
                declarations,
                ..
            } => {
                if *session_id != self.session_id {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: "initialization session mismatch".to_owned(),
                    });
                }
                let mut set = DeclarationSet::new();
                for declaration in declarations.clone() {
                    set.insert(declaration).map_err(|error| {
                        GraphStateError::InconsistentEvent {
                            detail: error.to_string(),
                        }
                    })?;
                }
                let rebuilt = Self::new(self.session_id, set)
                    .map_err(|_| GraphStateError::InconsistentEvent {
                        detail: "initialization replay failed".to_owned(),
                    })?
                    .0;
                *self = rebuilt;
                Ok(())
            }
            GraphStateEvent::VariableAssigned {
                name,
                scope,
                producer_node,
                prior_version,
                version,
                value,
                value_hash,
                ..
            } => {
                let declaration = self.declarations.get(name).ok_or_else(|| {
                    GraphStateError::InconsistentEvent {
                        detail: format!("assign to undeclared variable `{name}`"),
                    }
                })?;
                validate_value_for(declaration, value).map_err(|error| {
                    GraphStateError::InconsistentEvent {
                        detail: error.to_string(),
                    }
                })?;
                self.validate_scope_exists(scope)?;
                let actual_hash = ContentHash::digest(&canonical_value_bytes(value));
                if &actual_hash != value_hash {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("value hash mismatch for `{name}`"),
                    });
                }
                let expected_version = self.version(name, scope) + 1;
                if *version != expected_version || *prior_version != expected_version - 1 {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("version mismatch for `{name}`"),
                    });
                }
                if self.scope_entry(scope)?.contains_key(name)
                    && matches!(declaration.mutability, MutabilityPolicy::Immutable)
                {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("immutable `{name}` assigned twice"),
                    });
                }
                let entry = VariableEntry::assigned(name, value.clone(), producer_node.as_deref());
                self.scope_store(scope)?.insert(name.clone(), entry);
                if let Some(branch) = self.branch_mut(scope) {
                    branch.written_run.insert(name.clone());
                }
                self.versions.insert(
                    ScopeKey {
                        scope: scope.clone(),
                        name: name.clone(),
                    },
                    *version,
                );
                Ok(())
            }
            GraphStateEvent::VariableValidationRejected { .. } => {
                self.rejected = self.rejected.saturating_add(1);
                Ok(())
            }
            GraphStateEvent::BranchScopeCreated { branch_id, policy } => {
                if self.branches.contains_key(branch_id) {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("duplicate branch scope `{branch_id}`"),
                    });
                }
                self.create_branch_scope(branch_id, *policy).map_err(|_| {
                    GraphStateError::InconsistentEvent {
                        detail: format!("branch scope creation replay failed for `{branch_id}`"),
                    }
                })?;
                Ok(())
            }
            GraphStateEvent::BranchScopeClosed { branch_id } => {
                self.close_branch_scope(branch_id).map_err(|_| {
                    GraphStateError::InconsistentEvent {
                        detail: format!("branch scope close replay failed for `{branch_id}`"),
                    }
                })?;
                Ok(())
            }
            GraphStateEvent::VariableMerged {
                name,
                scope,
                policy,
                contributors,
                version,
                value,
                value_hash,
            } => {
                if !matches!(scope, VariableScope::Run) {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("merge target `{name}` is not run-scoped"),
                    });
                }
                let declaration = self.declarations.get(name).ok_or_else(|| {
                    GraphStateError::InconsistentEvent {
                        detail: format!("merge of undeclared variable `{name}`"),
                    }
                })?;
                if declaration.merge_policy != *policy {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("merge policy mismatch for `{name}`"),
                    });
                }
                validate_value_for(declaration, value).map_err(|error| {
                    GraphStateError::InconsistentEvent {
                        detail: error.to_string(),
                    }
                })?;
                let actual_hash = ContentHash::digest(&canonical_value_bytes(value));
                if &actual_hash != value_hash {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("merged value hash mismatch for `{name}`"),
                    });
                }
                let expected_version = self.version(name, &VariableScope::Run) + 1;
                if *version != expected_version {
                    return Err(GraphStateError::InconsistentEvent {
                        detail: format!("merged version mismatch for `{name}`"),
                    });
                }
                let mut entry = VariableEntry::assigned(name, value.clone(), None);
                entry.merged_from.clone_from(contributors);
                self.run.insert(name.clone(), entry);
                self.versions.insert(
                    ScopeKey {
                        scope: VariableScope::Run,
                        name: name.clone(),
                    },
                    *version,
                );
                for branch_id in contributors {
                    if let Some(branch) = self.branches.get_mut(branch_id) {
                        branch.written_run.remove(name);
                    }
                }
                Ok(())
            }
        }
    }

    /// Validates producer authority without using session state.
    fn validate_producer(
        declaration: &VariableDeclaration,
        producer: &AssignmentSource,
        scope: &VariableScope,
    ) -> Result<(), GraphStateError> {
        let node_id = match producer {
            AssignmentSource::Node { node_id } => node_id,
            AssignmentSource::Runtime => {
                if declaration.producers.is_empty() {
                    return Ok(());
                }
                return Err(GraphStateError::NotProducer {
                    name: declaration.name.clone(),
                    node: "runtime".to_owned(),
                });
            }
        };
        if let VariableScope::Node { node_id: owner } = scope
            && node_id != owner
        {
            return Err(GraphStateError::NotOwner {
                name: declaration.name.clone(),
                node: node_id.clone(),
                scope: scope.clone(),
            });
        }
        if !declaration.producers.is_empty() && !declaration.producers.contains(node_id) {
            return Err(GraphStateError::NotProducer {
                name: declaration.name.clone(),
                node: node_id.clone(),
            });
        }
        Ok(())
    }

    fn validate_scope_exists(&self, scope: &VariableScope) -> Result<(), GraphStateError> {
        match scope {
            VariableScope::Branch { branch_id } => {
                if self.branches.contains_key(branch_id) {
                    Ok(())
                } else {
                    Err(GraphStateError::UnknownScope {
                        scope: scope.clone(),
                    })
                }
            }
            _ => Ok(()),
        }
    }

    fn scope_entry(
        &self,
        scope: &VariableScope,
    ) -> Result<&BTreeMap<String, VariableEntry>, GraphStateError> {
        match scope {
            VariableScope::Run => Ok(&self.run),
            VariableScope::Branch { branch_id } => self
                .branches
                .get(branch_id)
                .map(|branch| &branch.variables)
                .ok_or_else(|| GraphStateError::UnknownScope {
                    scope: scope.clone(),
                }),
            VariableScope::Node { node_id } => Ok(self.nodes.get(node_id).unwrap_or(&self.empty)),
        }
    }

    fn scope_store(
        &mut self,
        scope: &VariableScope,
    ) -> Result<&mut BTreeMap<String, VariableEntry>, GraphStateError> {
        match scope {
            VariableScope::Run => Ok(&mut self.run),
            VariableScope::Branch { branch_id } => self
                .branches
                .get_mut(branch_id)
                .map(|branch| &mut branch.variables)
                .ok_or_else(|| GraphStateError::UnknownScope {
                    scope: scope.clone(),
                }),
            VariableScope::Node { node_id } => Ok(self.nodes.entry(node_id.clone()).or_default()),
        }
    }

    fn branch_mut(&mut self, scope: &VariableScope) -> Option<&mut BranchScope> {
        match scope {
            VariableScope::Branch { branch_id } => self.branches.get_mut(branch_id),
            _ => None,
        }
    }
}

/// Stable rejection reason recorded for audit.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionReason {
    /// Undeclared variable.
    Undeclared,
    /// Value violates the declared type or bounds.
    TypeMismatch,
    /// Value exceeds the declared serialized-size bound.
    SizeExceeded,
    /// Plaintext value for a secret-classified variable.
    SecretPlaintext,
    /// Writer is not a declared producer.
    NotProducer,
    /// Writer is not the owning node of a node-scoped variable.
    NotOwner,
    /// Immutable variable written twice.
    ImmutableWrite,
    /// Parallel merge conflicts.
    ConflictRejected,
    /// Object-field merge found differing values for one key.
    FieldConflict,
}

impl std::fmt::Display for RejectionReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Undeclared => "undeclared",
            Self::TypeMismatch => "type_mismatch",
            Self::SizeExceeded => "size_exceeded",
            Self::SecretPlaintext => "secret_plaintext",
            Self::NotProducer => "not_producer",
            Self::NotOwner => "not_owner",
            Self::ImmutableWrite => "immutable_write",
            Self::ConflictRejected => "conflict_rejected",
            Self::FieldConflict => "field_conflict",
        })
    }
}

/// Validates a value against its declaration contract.
///
/// # Errors
///
/// Returns [`GraphStateError`] when the type, size, or classification is
/// violated.
pub fn validate_value_for(
    declaration: &VariableDeclaration,
    value: &GraphValue,
) -> Result<(), GraphStateError> {
    if !declaration.r#type.accepts(value) {
        return Err(GraphStateError::TypeMismatch {
            name: declaration.name.clone(),
            expected: format!("{:?}", declaration.r#type),
            actual: value.type_label(),
        });
    }
    if value.serialized_bytes() > declaration.max_serialized_bytes {
        return Err(GraphStateError::SizeExceeded {
            name: declaration.name.clone(),
            actual: value.serialized_bytes(),
            maximum: declaration.max_serialized_bytes,
        });
    }
    if matches!(declaration.classification, SecurityClassification::Secret)
        && !value.is_secret_reference()
    {
        return Err(GraphStateError::SecretPlaintext {
            name: declaration.name.clone(),
        });
    }
    Ok(())
}

/// Applies the declared merge policy to deterministic contributions.
///
/// # Errors
///
/// Returns [`GraphStateError`] when the policy rejects the contributor set.
fn merge_values(
    declaration: &VariableDeclaration,
    contributions: &[MergeContribution],
) -> Result<GraphValue, GraphStateError> {
    let values = contributions.iter().map(|item| &item.value);
    match declaration.merge_policy {
        MergePolicy::RejectConflict => {
            if contributions.len() > 1 {
                return Err(GraphStateError::ConflictRejected {
                    name: declaration.name.clone(),
                    branches: contributions
                        .iter()
                        .map(|item| item.branch_id.clone())
                        .collect(),
                });
            }
            Ok(contributions
                .first()
                .map_or(GraphValue::Null, |item| item.value.clone()))
        }
        MergePolicy::LastWriter { ordering } => {
            let winner = match ordering {
                LastWriterOrdering::BranchLexical => contributions
                    .iter()
                    .max_by(|left, right| left.branch_id.cmp(&right.branch_id)),
                LastWriterOrdering::NodeLexical => contributions.iter().max_by(|left, right| {
                    left.node_id
                        .cmp(&right.node_id)
                        .then_with(|| left.branch_id.cmp(&right.branch_id))
                }),
            }
            .expect("contributors is non-empty by construction");
            Ok(winner.value.clone())
        }
        MergePolicy::ListAppend => {
            let mut merged = Vec::new();
            for value in values {
                let list = value
                    .as_list()
                    .ok_or_else(|| GraphStateError::MergePolicyMismatch {
                        name: declaration.name.clone(),
                        policy: "list_append".to_owned(),
                    })?;
                merged.extend(list.iter().cloned());
            }
            Ok(GraphValue::List(merged))
        }
        MergePolicy::SetUnion => {
            let mut unique = BTreeMap::new();
            for value in values {
                let list = value
                    .as_list()
                    .ok_or_else(|| GraphStateError::MergePolicyMismatch {
                        name: declaration.name.clone(),
                        policy: "set_union".to_owned(),
                    })?;
                for element in list {
                    unique.insert(canonical_value_bytes(element), element.clone());
                }
            }
            Ok(GraphValue::List(unique.into_values().collect()))
        }
        MergePolicy::ObjectFieldMerge => {
            let mut merged: BTreeMap<String, GraphValue> = BTreeMap::new();
            for value in values {
                let map = value
                    .as_map()
                    .ok_or_else(|| GraphStateError::MergePolicyMismatch {
                        name: declaration.name.clone(),
                        policy: "object_field_merge".to_owned(),
                    })?;
                for (key, field) in map {
                    match merged.get(key) {
                        Some(existing) if existing != field => {
                            return Err(GraphStateError::FieldConflict {
                                name: declaration.name.clone(),
                                key: key.clone(),
                            });
                        }
                        _ => {
                            merged.insert(key.clone(), field.clone());
                        }
                    }
                }
            }
            Ok(GraphValue::Map(merged))
        }
    }
}

/// Projects a graph value into JSON for condition evaluation.
#[must_use]
pub fn graph_value_to_json(value: &GraphValue) -> serde_json::Value {
    match value {
        GraphValue::Null => serde_json::Value::Null,
        GraphValue::Boolean(value) => serde_json::Value::Bool(*value),
        GraphValue::SignedInteger(..)
        | GraphValue::UnsignedInteger(..)
        | GraphValue::Timestamp(..)
        | GraphValue::DurationMillis(..) => serde_json::Value::Number(scalar_number(value)),
        GraphValue::Decimal(value) => {
            // Project fixed-point decimals as a stable scaled integer plus
            // scale; the expression language has no float constants.
            serde_json::json!({ "unscaled": value.unscaled, "scale": value.scale })
        }
        GraphValue::String(value)
        | GraphValue::EnumTag(value)
        | GraphValue::TaskId(value)
        | GraphValue::NodeId(value)
        | GraphValue::ToolResultReference(value)
        | GraphValue::ChildResultReference(value) => serde_json::Value::String(value.clone()),
        GraphValue::List(values) => {
            serde_json::Value::Array(values.iter().map(graph_value_to_json).collect())
        }
        GraphValue::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), graph_value_to_json(value)))
                .collect(),
        ),
        GraphValue::SessionId(value) | GraphValue::ChildSessionId(value) => {
            serde_json::Value::String(value.to_string())
        }
        GraphValue::ContinuationId(value) => serde_json::Value::String(format!("{value}")),
        GraphValue::ArtifactReference(reference) => {
            serde_json::json!({ "artifact_id": reference.id.to_string(), "content_hash": reference.content_hash.to_hex() })
        }
        GraphValue::ApprovalDecision(decision) => {
            serde_json::Value::String(format!("{decision:?}").to_ascii_lowercase())
        }
        GraphValue::SecretReference(reference) => {
            serde_json::Value::String(reference.as_str().to_owned())
        }
    }
}

/// Projects an integer-like scalar into a JSON number.
///
/// Each arm converts a distinct canonical scalar type; the bodies are
/// intentionally parallel.
#[allow(clippy::match_same_arms)]
fn scalar_number(value: &GraphValue) -> serde_json::Number {
    match value {
        GraphValue::SignedInteger(value) => serde_json::Number::from(*value),
        GraphValue::UnsignedInteger(value) => serde_json::Number::from(*value),
        GraphValue::Timestamp(value) => serde_json::Number::from(value.get()),
        GraphValue::DurationMillis(value) => serde_json::Number::from(*value),
        // Unreachable: the caller matches only scalar variants.
        _ => serde_json::Number::from(0),
    }
}

/// Canonical declaration-set bytes for identity hashing.
///
/// # Panics
///
/// Panics only if the fully owned declarations cannot be serialized, which is
/// a programming error in the serialized model.
#[must_use]
pub fn canonical_declarations_bytes(declarations: &[VariableDeclaration]) -> Vec<u8> {
    serde_json::to_vec(declarations).expect("declarations serialize")
}

/// Canonical graph-state failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum GraphStateError {
    /// Read of an undeclared variable.
    #[error("read of undeclared variable `{name}`")]
    UndeclaredRead {
        /// Variable name.
        name: String,
    },
    /// Write to an undeclared variable.
    #[error("write to undeclared variable `{name}`")]
    UndeclaredWrite {
        /// Variable name.
        name: String,
    },
    /// The scope does not exist.
    #[error("unknown scope {scope}")]
    UnknownScope {
        /// Missing scope.
        scope: VariableScope,
    },
    /// The scope is already closed.
    #[error("scope {scope} is closed")]
    ClosedScope {
        /// Closed scope.
        scope: VariableScope,
    },
    /// A scope was created twice.
    #[error("duplicate scope {scope}")]
    DuplicateScope {
        /// Duplicate scope.
        scope: VariableScope,
    },
    /// A value violates the declared type or bounds.
    #[error("value for `{name}` has type {actual}; expected {expected}")]
    TypeMismatch {
        /// Variable name.
        name: String,
        /// Expected type diagnostic.
        expected: String,
        /// Actual type label.
        actual: &'static str,
    },
    /// A value exceeds the declared serialized-size bound.
    #[error("value for `{name}` is {actual} bytes; maximum is {maximum}")]
    SizeExceeded {
        /// Variable name.
        name: String,
        /// Actual bytes.
        actual: usize,
        /// Maximum bytes.
        maximum: usize,
    },
    /// A plaintext value was written to a secret-classified variable.
    #[error("secret variable `{name}` rejects plaintext values")]
    SecretPlaintext {
        /// Variable name.
        name: String,
    },
    /// The writer is not a declared producer.
    #[error("node `{node}` is not a declared producer of `{name}`")]
    NotProducer {
        /// Variable name.
        name: String,
        /// Writer identity.
        node: String,
    },
    /// The writer is not the owner of a node-scoped variable.
    #[error("node `{node}` does not own node-scoped variable `{name}` in {scope}")]
    NotOwner {
        /// Variable name.
        name: String,
        /// Writer identity.
        node: String,
        /// Owning scope.
        scope: VariableScope,
    },
    /// An immutable variable was written twice.
    #[error("immutable variable `{name}` is already assigned in {scope}")]
    ImmutableWrite {
        /// Variable name.
        name: String,
        /// Target scope.
        scope: VariableScope,
    },
    /// A parallel merge was rejected by policy.
    #[error("parallel merge of `{name}` rejected: contributors {branches:?}")]
    ConflictRejected {
        /// Variable name.
        name: String,
        /// Contributor branches.
        branches: Vec<String>,
    },
    /// An object-field merge found differing values for one key.
    #[error("object-field merge of `{name}` conflicts on key `{key}`")]
    FieldConflict {
        /// Variable name.
        name: String,
        /// Conflicting key.
        key: String,
    },
    /// A merge policy does not match the contributor values.
    #[error("merge policy `{policy}` does not match values of `{name}`")]
    MergePolicyMismatch {
        /// Variable name.
        name: String,
        /// Policy diagnostic.
        policy: String,
    },
    /// Only run-scoped variables are mergeable into the run scope.
    #[error("variable `{name}` is not run-scoped and cannot be merged")]
    ScopeNotMergeable {
        /// Variable name.
        name: String,
    },
    /// A merge requires at least one contributor.
    #[error("merge of `{name}` requires at least one contributor")]
    EmptyMerge {
        /// Variable name.
        name: String,
    },
    /// A branch close found run-scoped writes that were never merged.
    #[error("branch `{branch_id}` closes with unmerged run writes: {variables:?}")]
    UnmergedBranchWrites {
        /// Branch identity.
        branch_id: String,
        /// Outstanding variables.
        variables: Vec<String>,
    },
    /// Declarations are malformed.
    #[error("invalid declarations: {detail}")]
    InvalidDeclarations {
        /// Deterministic diagnostic.
        detail: String,
    },
    /// Replay event is inconsistent with the current state.
    #[error("inconsistent replay event: {detail}")]
    InconsistentEvent {
        /// Deterministic diagnostic.
        detail: String,
    },
}

impl GraphStateError {
    /// Returns the stable rejection reason for audit events, when applicable.
    #[must_use]
    pub const fn rejection_reason(&self) -> Option<RejectionReason> {
        match self {
            Self::UndeclaredRead { .. } | Self::UndeclaredWrite { .. } => {
                Some(RejectionReason::Undeclared)
            }
            Self::TypeMismatch { .. } => Some(RejectionReason::TypeMismatch),
            Self::SizeExceeded { .. } => Some(RejectionReason::SizeExceeded),
            Self::SecretPlaintext { .. } => Some(RejectionReason::SecretPlaintext),
            Self::NotProducer { .. } => Some(RejectionReason::NotProducer),
            Self::NotOwner { .. } => Some(RejectionReason::NotOwner),
            Self::ImmutableWrite { .. } => Some(RejectionReason::ImmutableWrite),
            Self::ConflictRejected { .. } => Some(RejectionReason::ConflictRejected),
            Self::FieldConflict { .. } => Some(RejectionReason::FieldConflict),
            _ => None,
        }
    }
}
