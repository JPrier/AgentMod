use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Stable identifier for a blocking handler within one compiled pipeline.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HandlerId(String);

impl HandlerId {
    /// Creates a handler identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HandlerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<&str> for HandlerId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Stable identifier for the plugin or built-in component owning a handler.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PluginId(String);

impl PluginId {
    /// Creates a plugin identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PluginId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl From<&str> for PluginId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Declarative ordering information for one handler.
///
/// Explicit `before` and `after` edges take precedence over the deterministic
/// ready-queue tie-break: stage ascending, priority descending, plugin
/// identifier ascending, then handler identifier ascending.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderingSpec {
    /// Unique handler identifier.
    pub handler: HandlerId,
    /// Owning plugin or built-in component.
    pub plugin: PluginId,
    /// Broad execution stage; lower stages are preferred.
    pub stage: u16,
    /// Priority within a stage; higher priorities are preferred.
    pub priority: i32,
    /// Handlers which must run after this handler.
    pub before: Vec<HandlerId>,
    /// Handlers which must run before this handler.
    pub after: Vec<HandlerId>,
}

impl OrderingSpec {
    /// Creates an ordering specification with no explicit edges.
    #[must_use]
    pub fn new(handler: impl Into<HandlerId>, plugin: impl Into<PluginId>) -> Self {
        Self {
            handler: handler.into(),
            plugin: plugin.into(),
            stage: 0,
            priority: 0,
            before: Vec::new(),
            after: Vec::new(),
        }
    }

    /// Sets the broad execution stage.
    #[must_use]
    pub const fn with_stage(mut self, stage: u16) -> Self {
        self.stage = stage;
        self
    }

    /// Sets the priority, with larger values executing first when otherwise tied.
    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Adds a handler which must execute after this handler.
    #[must_use]
    pub fn before(mut self, handler: impl Into<HandlerId>) -> Self {
        self.before.push(handler.into());
        self
    }

    /// Adds a handler which must execute before this handler.
    #[must_use]
    pub fn after(mut self, handler: impl Into<HandlerId>) -> Self {
        self.after.push(handler.into());
        self
    }
}

/// Successfully compiled deterministic handler order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledOrder {
    handlers: Vec<HandlerId>,
}

impl CompiledOrder {
    /// Returns handler identifiers in execution order.
    #[must_use]
    pub fn handlers(&self) -> &[HandlerId] {
        &self.handlers
    }
}

/// One readable pipeline compilation problem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompileDiagnostic {
    /// More than one registration used the same handler identifier.
    DuplicateHandler {
        /// Duplicated identifier.
        handler: HandlerId,
        /// Number of registrations using the identifier.
        registrations: usize,
    },
    /// A `before` or `after` constraint named an unregistered handler.
    MissingDependency {
        /// Handler declaring the constraint.
        handler: HandlerId,
        /// Missing target.
        missing: HandlerId,
        /// Constraint direction.
        relation: &'static str,
    },
    /// Explicit constraints contain a directed cycle.
    OrderingCycle {
        /// Readable closed cycle path, with the first identifier repeated last.
        path: Vec<HandlerId>,
    },
}

impl CompileDiagnostic {
    /// Returns a stable machine-readable diagnostic code.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::DuplicateHandler { .. } => "PIPE001",
            Self::MissingDependency { .. } => "PIPE002",
            Self::OrderingCycle { .. } => "PIPE003",
        }
    }
}

impl fmt::Display for CompileDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateHandler {
                handler,
                registrations,
            } => write!(
                formatter,
                "error[{}]: handler `{handler}` is registered {registrations} times",
                self.code()
            ),
            Self::MissingDependency {
                handler,
                missing,
                relation,
            } => write!(
                formatter,
                "error[{}]: handler `{handler}` declares `{relation}` constraint on missing handler `{missing}`",
                self.code()
            ),
            Self::OrderingCycle { path } => {
                let path = path
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" -> ");
                write!(
                    formatter,
                    "error[{}]: handler ordering contains cycle: {path}",
                    self.code()
                )
            }
        }
    }
}

/// Collection of all diagnostics found while compiling a pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompileError {
    diagnostics: Vec<CompileDiagnostic>,
}

impl CompileError {
    /// Returns diagnostics in stable code and identifier order.
    #[must_use]
    pub fn diagnostics(&self) -> &[CompileDiagnostic] {
        &self.diagnostics
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, diagnostic) in self.diagnostics.iter().enumerate() {
            if index > 0 {
                formatter.write_str("\n")?;
            }
            diagnostic.fmt(formatter)?;
        }
        Ok(())
    }
}

impl std::error::Error for CompileError {}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReadyKey {
    stage: u16,
    priority: Reverse<i32>,
    plugin: PluginId,
    handler: HandlerId,
}

/// Compiles registration-independent deterministic handler ordering.
///
/// # Errors
///
/// Returns every duplicate and missing dependency discovered, or a readable
/// cycle after the declaration set is otherwise valid.
pub fn compile_order(specifications: &[OrderingSpec]) -> Result<CompiledOrder, CompileError> {
    let mut diagnostics = Vec::new();
    let mut counts = BTreeMap::<HandlerId, usize>::new();
    for specification in specifications {
        *counts.entry(specification.handler.clone()).or_default() += 1;
    }
    for (handler, registrations) in counts.iter().filter(|(_, count)| **count > 1) {
        diagnostics.push(CompileDiagnostic::DuplicateHandler {
            handler: handler.clone(),
            registrations: *registrations,
        });
    }

    let known: BTreeSet<_> = counts.keys().cloned().collect();
    for specification in specifications {
        for missing in specification
            .before
            .iter()
            .filter(|candidate| !known.contains(*candidate))
        {
            diagnostics.push(CompileDiagnostic::MissingDependency {
                handler: specification.handler.clone(),
                missing: missing.clone(),
                relation: "before",
            });
        }
        for missing in specification
            .after
            .iter()
            .filter(|candidate| !known.contains(*candidate))
        {
            diagnostics.push(CompileDiagnostic::MissingDependency {
                handler: specification.handler.clone(),
                missing: missing.clone(),
                relation: "after",
            });
        }
    }
    if !diagnostics.is_empty() {
        sort_diagnostics(&mut diagnostics);
        return Err(CompileError { diagnostics });
    }

    let specifications_by_id: BTreeMap<_, _> = specifications
        .iter()
        .map(|specification| (specification.handler.clone(), specification))
        .collect();
    let mut outgoing: BTreeMap<HandlerId, BTreeSet<HandlerId>> = known
        .iter()
        .cloned()
        .map(|handler| (handler, BTreeSet::new()))
        .collect();
    let mut incoming: BTreeMap<HandlerId, usize> =
        known.iter().cloned().map(|handler| (handler, 0)).collect();

    for specification in specifications {
        for target in &specification.before {
            add_edge(&specification.handler, target, &mut outgoing, &mut incoming);
        }
        for source in &specification.after {
            add_edge(source, &specification.handler, &mut outgoing, &mut incoming);
        }
    }

    let mut ready = BTreeSet::new();
    for (handler, count) in &incoming {
        if *count == 0
            && let Some(specification) = specifications_by_id.get(handler)
        {
            ready.insert(ready_key(specification));
        }
    }

    let mut handlers = Vec::with_capacity(specifications.len());
    while let Some(next) = ready.pop_first() {
        let handler = next.handler;
        handlers.push(handler.clone());
        for target in outgoing.get(&handler).cloned().unwrap_or_default() {
            if let Some(count) = incoming.get_mut(&target) {
                *count -= 1;
                if *count == 0
                    && let Some(specification) = specifications_by_id.get(&target)
                {
                    ready.insert(ready_key(specification));
                }
            }
        }
    }

    if handlers.len() != specifications.len() {
        let unresolved: BTreeSet<_> = incoming
            .iter()
            .filter(|(_, count)| **count > 0)
            .map(|(handler, _)| handler.clone())
            .collect();
        let path = find_cycle(&outgoing, &unresolved);
        return Err(CompileError {
            diagnostics: vec![CompileDiagnostic::OrderingCycle { path }],
        });
    }

    Ok(CompiledOrder { handlers })
}

fn add_edge(
    source: &HandlerId,
    target: &HandlerId,
    outgoing: &mut BTreeMap<HandlerId, BTreeSet<HandlerId>>,
    incoming: &mut BTreeMap<HandlerId, usize>,
) {
    if let Some(targets) = outgoing.get_mut(source)
        && targets.insert(target.clone())
        && let Some(count) = incoming.get_mut(target)
    {
        *count += 1;
    }
}

fn ready_key(specification: &OrderingSpec) -> ReadyKey {
    ReadyKey {
        stage: specification.stage,
        priority: Reverse(specification.priority),
        plugin: specification.plugin.clone(),
        handler: specification.handler.clone(),
    }
}

fn find_cycle(
    outgoing: &BTreeMap<HandlerId, BTreeSet<HandlerId>>,
    unresolved: &BTreeSet<HandlerId>,
) -> Vec<HandlerId> {
    fn visit(
        node: &HandlerId,
        outgoing: &BTreeMap<HandlerId, BTreeSet<HandlerId>>,
        unresolved: &BTreeSet<HandlerId>,
        visiting: &mut Vec<HandlerId>,
        visited: &mut BTreeSet<HandlerId>,
    ) -> Option<Vec<HandlerId>> {
        if let Some(index) = visiting.iter().position(|candidate| candidate == node) {
            let mut cycle = visiting[index..].to_vec();
            cycle.push(node.clone());
            return Some(cycle);
        }
        if !visited.insert(node.clone()) {
            return None;
        }
        visiting.push(node.clone());
        if let Some(targets) = outgoing.get(node) {
            for target in targets {
                if unresolved.contains(target)
                    && let Some(cycle) = visit(target, outgoing, unresolved, visiting, visited)
                {
                    return Some(cycle);
                }
            }
        }
        visiting.pop();
        None
    }

    let mut visited = BTreeSet::new();
    for node in unresolved {
        if let Some(cycle) = visit(node, outgoing, unresolved, &mut Vec::new(), &mut visited) {
            return cycle;
        }
    }
    unresolved.iter().cloned().collect()
}

fn sort_diagnostics(diagnostics: &mut [CompileDiagnostic]) {
    diagnostics.sort_by_key(|diagnostic| (diagnostic.code(), diagnostic.to_string()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_permutations_compile_identically() {
        let registrations = vec![
            OrderingSpec::new("security", "runtime")
                .with_stage(2)
                .with_priority(100),
            OrderingSpec::new("plugin-b", "bravo")
                .with_stage(1)
                .with_priority(5),
            OrderingSpec::new("plugin-a", "alpha")
                .with_stage(1)
                .with_priority(5),
            OrderingSpec::new("early", "runtime").with_stage(0),
        ];
        let expected = ["early", "plugin-a", "plugin-b", "security"];
        for permutation in permutations(&registrations) {
            let compiled = compile_order(&permutation).expect("permutation must compile");
            let actual: Vec<_> = compiled.handlers().iter().map(HandlerId::as_str).collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn explicit_edges_override_tie_breaks() {
        let specifications = [
            OrderingSpec::new("late-stage", "zulu")
                .with_stage(50)
                .before("early-stage"),
            OrderingSpec::new("early-stage", "alpha").with_stage(0),
        ];
        let compiled = compile_order(&specifications).expect("edge set must compile");
        let actual: Vec<_> = compiled.handlers().iter().map(HandlerId::as_str).collect();
        assert_eq!(actual, ["late-stage", "early-stage"]);
    }

    #[test]
    fn reports_duplicates_and_missing_dependencies_together() {
        let error = compile_order(&[
            OrderingSpec::new("same", "a").before("absent"),
            OrderingSpec::new("same", "b"),
        ])
        .expect_err("invalid declarations must fail");
        let codes: Vec<_> = error
            .diagnostics()
            .iter()
            .map(CompileDiagnostic::code)
            .collect();
        assert_eq!(codes, ["PIPE001", "PIPE002"]);
        assert!(error.to_string().contains("absent"));
    }

    #[test]
    fn reports_a_closed_cycle_path() {
        let error = compile_order(&[
            OrderingSpec::new("a", "p").before("b"),
            OrderingSpec::new("b", "p").before("c"),
            OrderingSpec::new("c", "p").before("a"),
        ])
        .expect_err("cycle must fail");
        let [CompileDiagnostic::OrderingCycle { path }] = error.diagnostics() else {
            panic!("expected exactly one cycle diagnostic");
        };
        assert_eq!(path.first(), path.last());
        assert_eq!(path.len(), 4);
        assert!(error.to_string().contains("a -> b -> c -> a"));
    }

    fn permutations<T: Clone>(values: &[T]) -> Vec<Vec<T>> {
        fn generate<T: Clone>(remaining: &[T], prefix: &mut Vec<T>, output: &mut Vec<Vec<T>>) {
            if remaining.is_empty() {
                output.push(prefix.clone());
                return;
            }
            for index in 0..remaining.len() {
                let mut next = remaining.to_owned();
                let value = next.remove(index);
                prefix.push(value);
                generate(&next, prefix, output);
                prefix.pop();
            }
        }

        let mut output = Vec::new();
        generate(values, &mut Vec::new(), &mut output);
        output
    }
}
