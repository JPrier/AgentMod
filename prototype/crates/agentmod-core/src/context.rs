//! Context assembly: an ordered, invocation-attributed fold over contributions.
//!
//! Every change is an appended record, so every prior state is a free
//! checkpoint: restoration folds back to an earlier state *as a new record*.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::record::{Body, Outcome, Record};
use crate::types::{ContextItem, ContextOp};

/// Folded context of one session plus the checkpoints restoration needs.
#[derive(Debug, Clone)]
pub struct ContextState {
    items: Arc<Vec<ContextItem>>,
    /// (sequence, state after that record) for every change, ascending.
    history: Vec<(u64, Arc<Vec<ContextItem>>)>,
}

impl Default for ContextState {
    fn default() -> Self {
        let empty = Arc::new(Vec::new());
        Self {
            items: empty.clone(),
            history: vec![(0, empty)],
        }
    }
}

impl ContextState {
    #[must_use]
    pub fn items(&self) -> &[ContextItem] {
        &self.items
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<Vec<ContextItem>> {
        self.items.clone()
    }

    /// Context as it stood after the record at `sequence`.
    #[must_use]
    pub fn at(&self, sequence: u64) -> Arc<Vec<ContextItem>> {
        let idx = self.history.partition_point(|(s, _)| *s <= sequence);
        self.history[idx.saturating_sub(1)].1.clone()
    }

    /// Replace the whole context (fork seeding).
    pub fn seed(&mut self, sequence: u64, items: Vec<ContextItem>) {
        self.items = Arc::new(items);
        self.history.push((sequence, self.items.clone()));
    }

    /// Apply one invocation's contributions, recorded at `sequence`.
    pub fn apply(&mut self, sequence: u64, invocation_id: &str, plugin: &str, ops: &[ContextOp]) {
        if ops.is_empty() {
            return;
        }
        let mut items: Vec<ContextItem> = (*self.items).clone();
        let mut added = 0usize;
        for op in ops {
            match op {
                ContextOp::Add { slot, value } => {
                    items.push(ContextItem {
                        id: format!("{invocation_id}#{added}"),
                        slot: slot.clone(),
                        value: value.clone(),
                        plugin: plugin.to_owned(),
                        invocation_id: invocation_id.to_owned(),
                    });
                    added += 1;
                }
                ContextOp::Replace { id, value } => {
                    if let Some(item) = items.iter_mut().find(|i| &i.id == id) {
                        item.value = value.clone();
                        item.plugin = plugin.to_owned();
                        item.invocation_id = invocation_id.to_owned();
                    }
                }
                ContextOp::Remove { id } => items.retain(|i| &i.id != id),
                ContextOp::ClearSlot { slot } => items.retain(|i| &i.slot != slot),
                ContextOp::Restore { to_sequence } => {
                    items = (*self.at(*to_sequence)).clone();
                }
            }
        }
        self.items = Arc::new(items);
        self.history.push((sequence, self.items.clone()));
    }
}

/// Fold a session's records into its context as of `up_to` (inclusive).
#[must_use]
pub fn fold(records: &[Record], up_to: Option<u64>) -> ContextState {
    let mut state = ContextState::default();
    let mut plugins: BTreeMap<&str, &str> = BTreeMap::new();
    for r in records {
        if up_to.is_some_and(|u| r.sequence > u) {
            break;
        }
        match &r.body {
            Body::InvocationStarted {
                invocation_id,
                plugin,
                ..
            } => {
                plugins.insert(invocation_id, plugin);
            }
            Body::InvocationCompleted {
                invocation_id,
                outcome: Outcome::Ok,
                contributions,
                ..
            } => {
                let plugin = plugins.get(invocation_id.as_str()).copied().unwrap_or("?");
                state.apply(r.sequence, invocation_id, plugin, contributions);
            }
            Body::ContextSeeded { items, .. } => state.seed(r.sequence, items.clone()),
            _ => {}
        }
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn add(slot: &str, v: i64) -> ContextOp {
        ContextOp::Add {
            slot: slot.into(),
            value: json!(v),
        }
    }

    #[test]
    fn add_replace_remove_clear() {
        let mut c = ContextState::default();
        c.apply(1, "s/i1", "p", &[add("m", 1), add("m", 2), add("t", 3)]);
        assert_eq!(c.items().len(), 3);
        assert_eq!(c.items()[1].id, "s/i1#1");
        c.apply(
            2,
            "s/i2",
            "q",
            &[ContextOp::Replace {
                id: "s/i1#0".into(),
                value: json!(10),
            }],
        );
        assert_eq!(c.items()[0].value, json!(10));
        assert_eq!(c.items()[0].plugin, "q");
        c.apply(
            3,
            "s/i3",
            "q",
            &[
                ContextOp::Remove {
                    id: "s/i1#1".into(),
                },
                ContextOp::Remove {
                    id: "missing".into(),
                },
            ],
        );
        assert_eq!(c.items().len(), 2);
        c.apply(4, "s/i4", "q", &[ContextOp::ClearSlot { slot: "m".into() }]);
        assert_eq!(c.items().len(), 1);
        assert_eq!(c.items()[0].slot, "t");
    }

    #[test]
    fn restore_is_reversible() {
        let mut c = ContextState::default();
        c.apply(1, "s/i1", "p", &[add("m", 1)]);
        c.apply(5, "s/i2", "p", &[add("m", 2)]);
        c.apply(9, "s/i3", "p", &[ContextOp::Restore { to_sequence: 3 }]);
        assert_eq!(c.items().len(), 1);
        // Restore the pre-revert state: nothing was lost.
        c.apply(12, "s/i4", "p", &[ContextOp::Restore { to_sequence: 8 }]);
        assert_eq!(c.items().len(), 2);
        assert_eq!(c.at(0).len(), 0);
        assert_eq!(c.at(4).len(), 1);
        assert_eq!(c.at(100).len(), 2);
    }

    #[test]
    fn ops_after_restore_apply_in_order() {
        let mut c = ContextState::default();
        c.apply(1, "s/i1", "p", &[add("m", 1), add("m", 2)]);
        c.apply(
            2,
            "s/i2",
            "p",
            &[ContextOp::Restore { to_sequence: 0 }, add("m", 3)],
        );
        assert_eq!(c.items().len(), 1);
        assert_eq!(c.items()[0].value, json!(3));
    }
}
