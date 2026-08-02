//! Canonical execution-budget accounting.
//!
//! Every budget dimension distinguishes provider-reported values, estimates,
//! and explicit unknowns. Usage is committed only after a completed action
//! with exact evidence; checks gate the next consequential dispatch. Cost
//! calculations bind a pricing-record version/timestamp and model/provider
//! identity, and an unknown price remains unknown rather than zero.

use std::collections::BTreeMap;
use std::fmt;

use agentmod_primitives::{ContentHash, SessionId, TimestampMillis};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::event::BudgetEvent;

/// One accounting dimension.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetDimension {
    /// Completed style steps.
    StyleSteps,
    /// Completed model requests.
    ModelRequests,
    /// Completed tool calls.
    ToolCalls,
    /// Completed style iterations.
    Iterations,
    /// Performed retries.
    Retries,
    /// Spawned child sessions (cumulative).
    ChildSessions,
    /// Provider-reported input tokens.
    InputTokens,
    /// Provider-reported output tokens.
    OutputTokens,
    /// Total tokens (input + output).
    TotalTokens,
    /// Provider cost in configured currency micros.
    ProviderCostMicros,
    /// Active provider duration in milliseconds.
    ActiveProviderDurationMs,
    /// Active tool duration in milliseconds.
    ActiveToolDurationMs,
    /// Elapsed wall-clock ceiling in milliseconds.
    ElapsedWallClockMs,
}

impl BudgetDimension {
    /// Returns the stable counters key used by condition environments.
    #[must_use]
    pub const fn counters_key(self) -> &'static str {
        match self {
            Self::StyleSteps => "style_steps",
            Self::ModelRequests => "model_requests",
            Self::ToolCalls => "tool_calls",
            Self::Iterations => "iterations",
            Self::Retries => "retries",
            Self::ChildSessions => "child_sessions",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
            Self::TotalTokens => "total_tokens",
            Self::ProviderCostMicros => "provider_cost_micros",
            Self::ActiveProviderDurationMs => "provider_duration_ms",
            Self::ActiveToolDurationMs => "tool_duration_ms",
            Self::ElapsedWallClockMs => "wall_clock_ms",
        }
    }

    /// Returns whether this dimension is cumulative (not a gauge).
    #[must_use]
    pub const fn is_cumulative(self) -> bool {
        true
    }
}

/// Whether a committed usage value is provider-reported or estimated.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageKind {
    /// Provider-reported exact value.
    Reported,
    /// Estimated value with provenance.
    Estimated,
}

/// Immutable limits bound at execution initialization.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetLimits {
    /// Maximum completed style steps.
    pub max_style_steps: Option<u64>,
    /// Maximum completed model requests.
    pub max_model_requests: Option<u64>,
    /// Maximum completed tool calls.
    pub max_tool_calls: Option<u64>,
    /// Maximum style iterations.
    pub max_iterations: Option<u64>,
    /// Maximum retries.
    pub max_retries: Option<u64>,
    /// Maximum cumulative child sessions.
    pub max_child_sessions: Option<u64>,
    /// Maximum concurrent children.
    pub max_concurrent_children: Option<u64>,
    /// Maximum input tokens.
    pub max_input_tokens: Option<u64>,
    /// Maximum output tokens.
    pub max_output_tokens: Option<u64>,
    /// Maximum total tokens.
    pub max_total_tokens: Option<u64>,
    /// Maximum provider cost in configured currency micros.
    pub max_provider_cost_micros: Option<u64>,
    /// Maximum active provider duration.
    pub max_active_provider_duration_ms: Option<u64>,
    /// Maximum active tool duration.
    pub max_active_tool_duration_ms: Option<u64>,
    /// Maximum elapsed wall clock; only tracked when explicitly selected.
    pub max_elapsed_wall_clock_ms: Option<u64>,
}

impl BudgetLimits {
    /// Returns the declared ceiling for a cumulative dimension.
    #[must_use]
    pub const fn limit_for(self, dimension: BudgetDimension) -> Option<u64> {
        match dimension {
            BudgetDimension::StyleSteps => self.max_style_steps,
            BudgetDimension::ModelRequests => self.max_model_requests,
            BudgetDimension::ToolCalls => self.max_tool_calls,
            BudgetDimension::Iterations => self.max_iterations,
            BudgetDimension::Retries => self.max_retries,
            BudgetDimension::ChildSessions => self.max_child_sessions,
            BudgetDimension::InputTokens => self.max_input_tokens,
            BudgetDimension::OutputTokens => self.max_output_tokens,
            BudgetDimension::TotalTokens => self.max_total_tokens,
            BudgetDimension::ProviderCostMicros => self.max_provider_cost_micros,
            BudgetDimension::ActiveProviderDurationMs => self.max_active_provider_duration_ms,
            BudgetDimension::ActiveToolDurationMs => self.max_active_tool_duration_ms,
            BudgetDimension::ElapsedWallClockMs => self.max_elapsed_wall_clock_ms,
        }
    }
}

/// Pricing identity bound to every cost calculation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PricingBinding {
    /// Model identity.
    pub model: String,
    /// Provider identity.
    pub provider: String,
    /// Exact pricing-record version used for the calculation.
    pub pricing_record_version: String,
    /// Timestamp of the pricing record.
    pub recorded_at: TimestampMillis,
}

/// Exact usage evidence for one completed action.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UsageEvidence {
    /// Accounting dimension.
    pub dimension: BudgetDimension,
    /// Exact positive delta.
    pub delta: u64,
    /// Value class.
    pub kind: UsageKind,
    /// Required for cost dimensions; binds pricing-record identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing: Option<PricingBinding>,
    /// Hash of the canonical evidence bytes proving the action completed.
    pub evidence_hash: ContentHash,
}

impl UsageEvidence {
    /// Creates evidence whose hash binds the exact dimension/delta/kind.
    ///
    /// # Panics
    ///
    /// Panics only if the fully owned evidence cannot be serialized, which is
    /// a programming error in the serialized model.
    #[must_use]
    pub fn new(
        dimension: BudgetDimension,
        delta: u64,
        kind: UsageKind,
        pricing: Option<PricingBinding>,
    ) -> Self {
        let evidence_hash = ContentHash::digest(
            &serde_json::to_vec(&(dimension, delta, kind, pricing.as_ref()))
                .expect("evidence serializes"),
        );
        Self {
            dimension,
            delta,
            kind,
            pricing,
            evidence_hash,
        }
    }

    /// Returns whether a cost dimension carries its required pricing binding.
    #[must_use]
    pub const fn has_pricing_for_cost(&self) -> bool {
        !matches!(self.dimension, BudgetDimension::ProviderCostMicros) || self.pricing.is_some()
    }
}

/// Complete child budget report rolled up per explicit policy.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChildBudgetReport {
    /// Exact usage committed by the child session.
    #[serde(default)]
    pub contributions: Vec<UsageEvidence>,
}

/// Policy controlling child usage rollup.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "policy", content = "value", rename_all = "snake_case")]
pub enum RollupPolicy {
    /// Roll up every reported and estimated contribution exactly.
    Full,
    /// Roll up contributions, capping each dimension delta.
    Bounded {
        /// Maximum delta rolled up per dimension.
        max_delta: u64,
    },
    /// Roll up nothing.
    None,
}

/// One cumulative dimension cell.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetCell {
    /// Dimension identity.
    pub dimension: BudgetDimension,
    /// Provider-reported cumulative usage.
    pub used_reported: u64,
    /// Estimated cumulative usage.
    pub used_estimated: u64,
    /// Declared ceiling.
    pub limit: Option<u64>,
    /// Whether a reported value was explicitly unknown.
    pub unknown_reported: bool,
    /// Whether an estimated value was explicitly unknown.
    pub unknown_estimated: bool,
    /// Count of blocked pre-dispatch checks (audit).
    pub blocked_requests: u64,
    /// Hash of the last committed evidence.
    pub last_evidence: Option<ContentHash>,
}

impl BudgetCell {
    /// Returns the conservative remaining amount (reported + estimated).
    #[must_use]
    pub fn remaining_conservative(&self) -> u64 {
        self.limit.map_or(u64::MAX, |limit| {
            limit.saturating_sub(self.used_reported.saturating_add(self.used_estimated))
        })
    }

    /// Returns the provider-reported remaining amount.
    #[must_use]
    pub fn remaining_reported(&self) -> u64 {
        self.limit
            .map_or(u64::MAX, |limit| limit.saturating_sub(self.used_reported))
    }

    /// Returns whether the given usage class is explicitly unknown.
    #[must_use]
    pub const fn is_unknown(&self, kind: UsageKind) -> bool {
        match kind {
            UsageKind::Reported => self.unknown_reported,
            UsageKind::Estimated => self.unknown_estimated,
        }
    }
}

/// Concurrent-children gauge.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConcurrentGauge {
    /// Current open children.
    pub current: u64,
    /// Observed peak.
    pub peak: u64,
    /// Declared ceiling.
    pub limit: Option<u64>,
}

/// Pre-dispatch decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetDecision {
    /// Dispatch may proceed; the conservative remaining amount is returned.
    Allowed {
        /// Conservative remaining amount for the dimension.
        remaining: u64,
    },
    /// Dispatch is blocked before any mutation.
    Blocked {
        /// Conservative remaining amount for the dimension.
        remaining: u64,
        /// Requested delta.
        requested: u64,
    },
}

impl BudgetDecision {
    /// Returns whether dispatch is allowed.
    #[must_use]
    pub const fn allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }
}

/// Canonical budget ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetLedger {
    session_id: SessionId,
    limits: BudgetLimits,
    recorded_at: TimestampMillis,
    wall_clock_enabled: bool,
    cells: BTreeMap<BudgetDimension, BudgetCell>,
    gauge: ConcurrentGauge,
}

impl BudgetLedger {
    /// Initializes a ledger with immutable limits.
    #[must_use]
    pub fn initialize(
        session_id: SessionId,
        limits: BudgetLimits,
        recorded_at: TimestampMillis,
        wall_clock_enabled: bool,
    ) -> (Self, Vec<BudgetEvent>) {
        let cells = limits
            .map_cells()
            .into_iter()
            .map(|(dimension, limit)| {
                (
                    dimension,
                    BudgetCell {
                        dimension,
                        used_reported: 0,
                        used_estimated: 0,
                        limit,
                        unknown_reported: false,
                        unknown_estimated: false,
                        blocked_requests: 0,
                        last_evidence: None,
                    },
                )
            })
            .collect();
        let gauge = ConcurrentGauge {
            current: 0,
            peak: 0,
            limit: limits.max_concurrent_children,
        };
        let ledger = Self {
            session_id,
            limits: limits.clone(),
            recorded_at,
            wall_clock_enabled,
            cells,
            gauge,
        };
        let events = vec![BudgetEvent::BudgetsInitialized {
            session_id,
            limits,
            recorded_at,
            wall_clock_enabled,
        }];
        (ledger, events)
    }

    /// Reconstructs a ledger from its initialization event and subsequent
    /// budget events, reproducing the exact remaining amounts.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when the init event does not match the session,
    /// or any event cannot be applied exactly.
    pub fn reconstruct(
        session_id: SessionId,
        init: &BudgetEvent,
        events: &[BudgetEvent],
    ) -> Result<Self, BudgetError> {
        let BudgetEvent::BudgetsInitialized {
            session_id: init_session,
            limits,
            recorded_at,
            wall_clock_enabled,
        } = init
        else {
            return Err(BudgetError::ExpectedInitialization);
        };
        if *init_session != session_id {
            return Err(BudgetError::SessionMismatch);
        }
        let (mut ledger, _) = Self::initialize(
            *init_session,
            limits.clone(),
            *recorded_at,
            *wall_clock_enabled,
        );
        for event in events {
            ledger.apply(event)?;
        }
        Ok(ledger)
    }

    /// Applies one committed budget event.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when the event cannot be applied exactly.
    pub fn apply(&mut self, event: &BudgetEvent) -> Result<(), BudgetError> {
        match event {
            BudgetEvent::BudgetsInitialized { .. } => Err(BudgetError::RepeatedInitialization),
            BudgetEvent::BudgetCommitted {
                dimension,
                delta,
                kind,
                evidence_hash,
                pricing,
                recorded_at: _,
            } => {
                let cell = self.cell_mut(*dimension)?;
                let used = match kind {
                    UsageKind::Reported => &mut cell.used_reported,
                    UsageKind::Estimated => &mut cell.used_estimated,
                };
                *used = used
                    .checked_add(*delta)
                    .ok_or(BudgetError::LedgerOverflow {
                        dimension: *dimension,
                    })?;
                cell.last_evidence = Some(*evidence_hash);
                let _ = pricing;
                Ok(())
            }
            BudgetEvent::BudgetMarkedUnknown {
                dimension,
                kind,
                recorded_at: _,
            } => {
                let cell = self.cell_mut(*dimension)?;
                match kind {
                    UsageKind::Reported => cell.unknown_reported = true,
                    UsageKind::Estimated => cell.unknown_estimated = true,
                }
                Ok(())
            }
            BudgetEvent::BudgetCheckBlocked {
                dimension,
                requested,
                remaining,
                recorded_at: _,
            } => {
                let cell = self.cell_mut(*dimension)?;
                if cell.remaining_conservative() != *remaining {
                    return Err(BudgetError::InconsistentBlockedEvent {
                        dimension: *dimension,
                    });
                }
                cell.blocked_requests += 1;
                let _ = requested;
                Ok(())
            }
            BudgetEvent::ConcurrentChildrenChanged {
                delta,
                current,
                peak,
                limit,
                recorded_at: _,
            } => {
                let expected_current = self
                    .gauge
                    .current
                    .checked_add_signed(*delta)
                    .ok_or(BudgetError::GaugeUnderflow)?;
                if expected_current != *current {
                    return Err(BudgetError::InconsistentGauge {
                        expected: expected_current,
                        actual: *current,
                    });
                }
                if self.gauge.limit != *limit {
                    return Err(BudgetError::InconsistentGaugeLimit {
                        expected: self.gauge.limit,
                        actual: *limit,
                    });
                }
                self.gauge.current = *current;
                self.gauge.peak = self.gauge.peak.max(*peak);
                Ok(())
            }
            BudgetEvent::ChildUsageRolledUp { .. } => Ok(()),
        }
    }

    /// Checks whether the next consequential dispatch may proceed.
    ///
    /// The returned event is a `BudgetCheckBlocked` record when blocked; the
    /// caller must journal it and must not dispatch.
    #[must_use]
    pub fn check(&mut self, dimension: BudgetDimension, requested: u64) -> BudgetDecision {
        let Some(cell) = self.cells.get(&dimension) else {
            return BudgetDecision::Blocked {
                remaining: 0,
                requested,
            };
        };
        let remaining = cell.remaining_conservative();
        if requested == 0 || remaining >= requested {
            BudgetDecision::Allowed { remaining }
        } else {
            if let Some(cell) = self.cells.get_mut(&dimension) {
                cell.blocked_requests += 1;
            }
            BudgetDecision::Blocked {
                remaining,
                requested,
            }
        }
    }

    /// Records a pre-dispatch blocked check as a canonical event.
    #[must_use]
    pub fn record_blocked(
        &self,
        dimension: BudgetDimension,
        requested: u64,
        recorded_at: TimestampMillis,
    ) -> BudgetEvent {
        let remaining = self
            .cell(dimension)
            .map_or(0, BudgetCell::remaining_conservative);
        BudgetEvent::BudgetCheckBlocked {
            dimension,
            requested,
            remaining,
            recorded_at,
        }
    }

    /// Commits exact usage after a completed action.
    ///
    /// A completed action may consume the final budget; the next pre-dispatch
    /// check is what prevents further dispatch.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] for zero deltas, missing pricing bindings on
    /// cost dimensions, disabled wall-clock tracking, or overflow.
    pub fn commit(
        &mut self,
        evidence: &UsageEvidence,
        recorded_at: TimestampMillis,
    ) -> Result<BudgetEvent, BudgetError> {
        if evidence.delta == 0 {
            return Err(BudgetError::ZeroDelta {
                dimension: evidence.dimension,
            });
        }
        self.require_tracked(evidence.dimension)?;
        if matches!(evidence.dimension, BudgetDimension::ProviderCostMicros)
            && evidence.pricing.is_none()
        {
            return Err(BudgetError::MissingPricingBinding {
                dimension: evidence.dimension,
            });
        }
        let cell = self.cell_mut(evidence.dimension)?;
        let used = match evidence.kind {
            UsageKind::Reported => &mut cell.used_reported,
            UsageKind::Estimated => &mut cell.used_estimated,
        };
        *used = used
            .checked_add(evidence.delta)
            .ok_or(BudgetError::LedgerOverflow {
                dimension: evidence.dimension,
            })?;
        cell.last_evidence = Some(evidence.evidence_hash);
        Ok(BudgetEvent::BudgetCommitted {
            dimension: evidence.dimension,
            delta: evidence.delta,
            kind: evidence.kind,
            evidence_hash: evidence.evidence_hash,
            pricing: evidence.pricing.clone(),
            recorded_at,
        })
    }

    /// Marks a dimension's usage explicitly unknown; never recorded as zero.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when the dimension is not tracked.
    pub fn mark_unknown(
        &mut self,
        dimension: BudgetDimension,
        kind: UsageKind,
        recorded_at: TimestampMillis,
    ) -> Result<BudgetEvent, BudgetError> {
        self.require_tracked(dimension)?;
        let cell = self.cell_mut(dimension)?;
        match kind {
            UsageKind::Reported => cell.unknown_reported = true,
            UsageKind::Estimated => cell.unknown_estimated = true,
        }
        Ok(BudgetEvent::BudgetMarkedUnknown {
            dimension,
            kind,
            recorded_at,
        })
    }

    /// Opens one concurrent child, enforcing the ceiling before mutation.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::ConcurrentLimitReached`] when the ceiling is
    /// already reached; no state is mutated.
    pub fn open_child(
        &mut self,
        recorded_at: TimestampMillis,
    ) -> Result<Vec<BudgetEvent>, BudgetError> {
        if self
            .gauge
            .limit
            .is_some_and(|limit| self.gauge.current >= limit)
        {
            return Err(BudgetError::ConcurrentLimitReached {
                current: self.gauge.current,
                limit: self.gauge.limit,
            });
        }
        let previous = self.gauge.current;
        self.gauge.current = previous + 1;
        self.gauge.peak = self.gauge.peak.max(self.gauge.current);
        Ok(vec![BudgetEvent::ConcurrentChildrenChanged {
            delta: 1,
            current: self.gauge.current,
            peak: self.gauge.peak,
            limit: self.gauge.limit,
            recorded_at,
        }])
    }

    /// Closes one concurrent child.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::GaugeUnderflow`] when no child is open.
    pub fn close_child(
        &mut self,
        recorded_at: TimestampMillis,
    ) -> Result<Vec<BudgetEvent>, BudgetError> {
        if self.gauge.current == 0 {
            return Err(BudgetError::GaugeUnderflow);
        }
        self.gauge.current -= 1;
        Ok(vec![BudgetEvent::ConcurrentChildrenChanged {
            delta: -1,
            current: self.gauge.current,
            peak: self.gauge.peak,
            limit: self.gauge.limit,
            recorded_at,
        }])
    }

    /// Rolls up a child session's usage per the explicit policy.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError`] when any contribution cannot be committed.
    pub fn roll_up_child(
        &mut self,
        child_session: SessionId,
        report: &ChildBudgetReport,
        policy: RollupPolicy,
        recorded_at: TimestampMillis,
    ) -> Result<Vec<BudgetEvent>, BudgetError> {
        let mut contributions = report.contributions.clone();
        contributions.sort_by(|left, right| {
            left.dimension
                .cmp(&right.dimension)
                .then_with(|| left.kind.cmp(&right.kind))
        });
        let mut events = Vec::new();
        let mut dimensions = Vec::new();
        match policy {
            RollupPolicy::None => {}
            RollupPolicy::Full => {
                for contribution in &contributions {
                    events.push(self.commit(contribution, recorded_at)?);
                    dimensions.push(contribution.dimension);
                }
            }
            RollupPolicy::Bounded { max_delta } => {
                for contribution in &contributions {
                    let mut capped = contribution.clone();
                    capped.delta = capped.delta.min(max_delta);
                    events.push(self.commit(&capped, recorded_at)?);
                    dimensions.push(contribution.dimension);
                }
            }
        }
        events.push(BudgetEvent::ChildUsageRolledUp {
            child_session,
            policy,
            dimensions,
            recorded_at,
        });
        Ok(events)
    }

    /// Returns the conservative remaining amount for a dimension.
    #[must_use]
    pub fn remaining(&self, dimension: BudgetDimension) -> u64 {
        self.cell(dimension)
            .map_or(0, BudgetCell::remaining_conservative)
    }

    /// Returns the provider-reported remaining amount for a dimension.
    #[must_use]
    pub fn remaining_reported(&self, dimension: BudgetDimension) -> u64 {
        self.cell(dimension)
            .map_or(0, BudgetCell::remaining_reported)
    }

    /// Returns whether a dimension's usage class is explicitly unknown.
    #[must_use]
    pub fn is_unknown(&self, dimension: BudgetDimension, kind: UsageKind) -> bool {
        matches!(
            self.cells.get(&dimension),
            Some(cell) if cell.is_unknown(kind)
        )
    }

    /// Returns the cumulative cell for a dimension, when tracked.
    ///
    /// # Errors
    ///
    /// Returns [`BudgetError::DimensionNotTracked`] when the dimension is not
    /// tracked by this ledger.
    pub fn cell(&self, dimension: BudgetDimension) -> Result<&BudgetCell, BudgetError> {
        self.cells
            .get(&dimension)
            .ok_or(BudgetError::DimensionNotTracked { dimension })
    }

    /// Returns the concurrent-children gauge.
    #[must_use]
    pub const fn gauge(&self) -> &ConcurrentGauge {
        &self.gauge
    }

    /// Returns the immutable limits.
    #[must_use]
    pub fn limits(&self) -> &BudgetLimits {
        &self.limits
    }

    /// Returns the session identity.
    #[must_use]
    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Returns whether the wall-clock ceiling is explicitly selected.
    #[must_use]
    pub const fn wall_clock_enabled(&self) -> bool {
        self.wall_clock_enabled
    }

    /// Iterates tracked cells in stable dimension order.
    pub fn cells(&self) -> impl Iterator<Item = &BudgetCell> {
        self.cells.values()
    }

    /// Returns a deterministic counters environment for conditions.
    ///
    /// Every value is derived only from canonical ledger state, so the
    /// projection is stable across restarts and rebuilds.
    #[must_use]
    pub fn budget_environment(&self) -> Value {
        let mut counters = Map::new();
        for cell in self.cells.values() {
            let mut entry = Map::new();
            entry.insert(
                "used".to_owned(),
                Value::from(cell.used_reported.saturating_add(cell.used_estimated)),
            );
            entry.insert(
                "remaining".to_owned(),
                Value::from(cell.remaining_conservative()),
            );
            if let Some(limit) = cell.limit {
                entry.insert("limit".to_owned(), Value::from(limit));
            }
            if cell.is_unknown(UsageKind::Reported) {
                entry.insert("unknown".to_owned(), Value::Bool(true));
            }
            counters.insert(
                cell.dimension.counters_key().to_owned(),
                Value::Object(entry),
            );
        }
        let mut gauge = Map::new();
        gauge.insert("current".to_owned(), Value::from(self.gauge.current));
        gauge.insert("peak".to_owned(), Value::from(self.gauge.peak));
        if let Some(limit) = self.gauge.limit {
            gauge.insert("limit".to_owned(), Value::from(limit));
        }
        counters.insert("concurrent_children".to_owned(), Value::Object(gauge));
        let mut root = Map::new();
        root.insert("counters".to_owned(), Value::Object(counters));
        Value::Object(root)
    }

    fn require_tracked(&self, dimension: BudgetDimension) -> Result<(), BudgetError> {
        if matches!(dimension, BudgetDimension::ElapsedWallClockMs) && !self.wall_clock_enabled {
            return Err(BudgetError::WallClockDisabled);
        }
        if dimension.is_cumulative() && self.cells.contains_key(&dimension) {
            Ok(())
        } else {
            Err(BudgetError::DimensionNotTracked { dimension })
        }
    }

    fn cell_mut(&mut self, dimension: BudgetDimension) -> Result<&mut BudgetCell, BudgetError> {
        self.require_tracked(dimension)?;
        self.cells
            .get_mut(&dimension)
            .ok_or(BudgetError::DimensionNotTracked { dimension })
    }
}

impl BudgetLimits {
    fn map_cells(&self) -> Vec<(BudgetDimension, Option<u64>)> {
        [
            (BudgetDimension::StyleSteps, self.max_style_steps),
            (BudgetDimension::ModelRequests, self.max_model_requests),
            (BudgetDimension::ToolCalls, self.max_tool_calls),
            (BudgetDimension::Iterations, self.max_iterations),
            (BudgetDimension::Retries, self.max_retries),
            (BudgetDimension::ChildSessions, self.max_child_sessions),
            (BudgetDimension::InputTokens, self.max_input_tokens),
            (BudgetDimension::OutputTokens, self.max_output_tokens),
            (BudgetDimension::TotalTokens, self.max_total_tokens),
            (
                BudgetDimension::ProviderCostMicros,
                self.max_provider_cost_micros,
            ),
            (
                BudgetDimension::ActiveProviderDurationMs,
                self.max_active_provider_duration_ms,
            ),
            (
                BudgetDimension::ActiveToolDurationMs,
                self.max_active_tool_duration_ms,
            ),
            (
                BudgetDimension::ElapsedWallClockMs,
                self.max_elapsed_wall_clock_ms,
            ),
        ]
        .into_iter()
        .collect()
    }
}

impl fmt::Display for BudgetDimension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.counters_key())
    }
}

/// Budget accounting failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BudgetError {
    /// A commit delta must be positive.
    #[error("budget commit for {dimension} must have a positive delta")]
    ZeroDelta {
        /// Dimension.
        dimension: BudgetDimension,
    },
    /// A cost dimension requires a pricing binding.
    #[error("cost dimension {dimension} requires a pricing binding")]
    MissingPricingBinding {
        /// Dimension.
        dimension: BudgetDimension,
    },
    /// The dimension is not tracked by this ledger.
    #[error("budget dimension {dimension} is not tracked")]
    DimensionNotTracked {
        /// Dimension.
        dimension: BudgetDimension,
    },
    /// The wall-clock ceiling is not explicitly selected.
    #[error("elapsed wall-clock budget is not explicitly selected")]
    WallClockDisabled,
    /// Cumulative usage overflowed.
    #[error("budget ledger overflow on {dimension}")]
    LedgerOverflow {
        /// Dimension.
        dimension: BudgetDimension,
    },
    /// A gauge closed below zero.
    #[error("budget gauge underflow")]
    GaugeUnderflow,
    /// The concurrent-children ceiling is reached.
    #[error("concurrent children limit reached: {current}/{limit:?}")]
    ConcurrentLimitReached {
        /// Current gauge.
        current: u64,
        /// Declared ceiling.
        limit: Option<u64>,
    },
    /// Reconstruction received a non-initialization event first.
    #[error("budget reconstruction requires a budgets-initialized event")]
    ExpectedInitialization,
    /// Reconstruction session does not match.
    #[error("budget reconstruction session mismatch")]
    SessionMismatch,
    /// Initialization appeared more than once.
    #[error("budget initialization event repeated")]
    RepeatedInitialization,
    /// A blocked event does not match the current remaining amount.
    #[error("budget blocked event is inconsistent for {dimension}")]
    InconsistentBlockedEvent {
        /// Dimension.
        dimension: BudgetDimension,
    },
    /// A gauge event does not follow from the current gauge.
    #[error("budget gauge event is inconsistent: expected {expected}, actual {actual}")]
    InconsistentGauge {
        /// Expected current.
        expected: u64,
        /// Event current.
        actual: u64,
    },
    /// A gauge event changes the declared limit.
    #[error(
        "budget gauge event changes the declared limit: expected {expected:?}, actual {actual:?}"
    )]
    InconsistentGaugeLimit {
        /// Expected limit.
        expected: Option<u64>,
        /// Event limit.
        actual: Option<u64>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::BudgetEvent;

    fn session() -> SessionId {
        SessionId::from_uuid(uuid::Uuid::nil())
    }

    fn limits() -> BudgetLimits {
        BudgetLimits {
            max_model_requests: Some(2),
            max_input_tokens: Some(100),
            max_output_tokens: Some(100),
            max_total_tokens: Some(200),
            max_provider_cost_micros: Some(1_000),
            max_concurrent_children: Some(2),
            ..BudgetLimits::default()
        }
    }

    fn ledger() -> BudgetLedger {
        BudgetLedger::initialize(
            session(),
            limits(),
            TimestampMillis::new(1_700_000_000_000),
            false,
        )
        .0
    }

    fn pricing() -> PricingBinding {
        PricingBinding {
            model: "mock".into(),
            provider: "fixture".into(),
            pricing_record_version: "1.0".into(),
            recorded_at: TimestampMillis::new(1_700_000_000_000),
        }
    }

    #[test]
    fn final_action_consumes_budget_and_next_check_is_blocked() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        assert_eq!(
            ledger.check(BudgetDimension::ModelRequests, 1),
            BudgetDecision::Allowed { remaining: 2 }
        );
        let evidence =
            UsageEvidence::new(BudgetDimension::ModelRequests, 1, UsageKind::Reported, None);
        ledger.commit(&evidence, at).expect("commit");
        assert_eq!(
            ledger.check(BudgetDimension::ModelRequests, 1),
            BudgetDecision::Allowed { remaining: 1 }
        );
        ledger.commit(&evidence, at).expect("commit final");
        assert_eq!(
            ledger.check(BudgetDimension::ModelRequests, 1),
            BudgetDecision::Blocked {
                remaining: 0,
                requested: 1
            }
        );
        assert_eq!(ledger.remaining(BudgetDimension::ModelRequests), 0);
    }

    #[test]
    fn oversize_request_is_blocked_without_mutation() {
        let mut ledger = ledger();
        assert_eq!(
            ledger.check(BudgetDimension::ModelRequests, 3),
            BudgetDecision::Blocked {
                remaining: 2,
                requested: 3
            }
        );
        // The blocked check is audit-only; usage is unchanged.
        assert_eq!(ledger.remaining(BudgetDimension::ModelRequests), 2);
    }

    #[test]
    fn every_token_boundary_is_exact() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        let evidence =
            UsageEvidence::new(BudgetDimension::TotalTokens, 200, UsageKind::Reported, None);
        assert!(matches!(
            ledger.check(BudgetDimension::TotalTokens, 200),
            BudgetDecision::Allowed { remaining: 200 }
        ));
        ledger.commit(&evidence, at).expect("commit final");
        assert_eq!(ledger.remaining(BudgetDimension::TotalTokens), 0);
        assert_eq!(
            ledger.check(BudgetDimension::TotalTokens, 1),
            BudgetDecision::Blocked {
                remaining: 0,
                requested: 1
            }
        );
    }

    #[test]
    fn unknown_cost_remains_unknown_never_zero() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        ledger
            .mark_unknown(BudgetDimension::ProviderCostMicros, UsageKind::Reported, at)
            .expect("marked unknown");
        assert!(ledger.is_unknown(BudgetDimension::ProviderCostMicros, UsageKind::Reported));
        assert_eq!(ledger.remaining(BudgetDimension::ProviderCostMicros), 1_000);
        assert_eq!(
            ledger
                .cell(BudgetDimension::ProviderCostMicros)
                .expect("cell")
                .used_reported,
            0
        );
    }

    #[test]
    fn cost_requires_a_pricing_binding() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        let without = UsageEvidence::new(
            BudgetDimension::ProviderCostMicros,
            100,
            UsageKind::Reported,
            None,
        );
        assert_eq!(
            ledger.commit(&without, at),
            Err(BudgetError::MissingPricingBinding {
                dimension: BudgetDimension::ProviderCostMicros
            })
        );
        let with = UsageEvidence::new(
            BudgetDimension::ProviderCostMicros,
            100,
            UsageKind::Reported,
            Some(pricing()),
        );
        ledger.commit(&with, at).expect("commit with pricing");
        assert_eq!(ledger.remaining(BudgetDimension::ProviderCostMicros), 900);
    }

    #[test]
    fn estimated_usage_counts_conservatively() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        let estimated =
            UsageEvidence::new(BudgetDimension::InputTokens, 90, UsageKind::Estimated, None);
        ledger.commit(&estimated, at).expect("commit estimate");
        assert_eq!(ledger.remaining(BudgetDimension::InputTokens), 10);
        let reported =
            UsageEvidence::new(BudgetDimension::InputTokens, 10, UsageKind::Reported, None);
        assert_eq!(
            ledger.check(BudgetDimension::InputTokens, 10),
            BudgetDecision::Allowed { remaining: 10 }
        );
        ledger.commit(&reported, at).expect("commit reported");
        assert_eq!(ledger.remaining(BudgetDimension::InputTokens), 0);
    }

    #[test]
    fn concurrent_children_gauge_enforces_ceiling_and_peaks() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        ledger.open_child(at).expect("open");
        ledger.open_child(at).expect("open");
        assert_eq!(ledger.gauge().current, 2);
        assert_eq!(ledger.gauge().peak, 2);
        assert_eq!(
            ledger.open_child(at),
            Err(BudgetError::ConcurrentLimitReached {
                current: 2,
                limit: Some(2)
            })
        );
        ledger.close_child(at).expect("close");
        ledger.open_child(at).expect("reopen");
        assert_eq!(ledger.gauge().peak, 2);
        ledger.close_child(at).expect("close");
        ledger.close_child(at).expect("close");
        assert_eq!(ledger.close_child(at), Err(BudgetError::GaugeUnderflow));
    }

    #[test]
    fn child_rollup_follows_explicit_policy() {
        let mut full_ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        let child = SessionId::from_uuid(uuid::Uuid::nil());
        let report = ChildBudgetReport {
            contributions: vec![
                UsageEvidence::new(BudgetDimension::ModelRequests, 1, UsageKind::Reported, None),
                UsageEvidence::new(BudgetDimension::InputTokens, 60, UsageKind::Reported, None),
            ],
        };
        full_ledger
            .roll_up_child(child, &report, RollupPolicy::Full, at)
            .expect("full rollup");
        assert_eq!(full_ledger.remaining(BudgetDimension::ModelRequests), 1);
        assert_eq!(full_ledger.remaining(BudgetDimension::InputTokens), 40);

        let mut bounded_ledger = ledger();
        bounded_ledger
            .roll_up_child(child, &report, RollupPolicy::Bounded { max_delta: 50 }, at)
            .expect("bounded rollup");
        assert_eq!(bounded_ledger.remaining(BudgetDimension::InputTokens), 50);

        let mut none_ledger = ledger();
        none_ledger
            .roll_up_child(child, &report, RollupPolicy::None, at)
            .expect("no rollup");
        assert_eq!(none_ledger.remaining(BudgetDimension::InputTokens), 100);
    }

    #[test]
    fn restart_reconstruction_reproduces_exact_remaining() {
        let mut original = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        let mut events = Vec::new();
        let evidence =
            UsageEvidence::new(BudgetDimension::ModelRequests, 1, UsageKind::Reported, None);
        events.push(original.commit(&evidence, at).expect("commit"));
        events.push(
            original
                .mark_unknown(BudgetDimension::ProviderCostMicros, UsageKind::Reported, at)
                .expect("unknown"),
        );
        let estimated = UsageEvidence::new(
            BudgetDimension::TotalTokens,
            150,
            UsageKind::Estimated,
            None,
        );
        events.push(original.commit(&estimated, at).expect("commit"));
        let blocked = original.check(BudgetDimension::ModelRequests, 2);
        assert_eq!(
            blocked,
            BudgetDecision::Blocked {
                remaining: 1,
                requested: 2
            }
        );
        events.push(original.record_blocked(
            BudgetDimension::ModelRequests,
            1,
            TimestampMillis::new(1_700_000_000_002),
        ));

        let init = BudgetEvent::BudgetsInitialized {
            session_id: session(),
            limits: limits(),
            recorded_at: TimestampMillis::new(1_700_000_000_000),
            wall_clock_enabled: false,
        };
        let rebuilt = BudgetLedger::reconstruct(session(), &init, &events).expect("reconstruct");
        assert_eq!(rebuilt, original);
        assert_eq!(rebuilt.remaining(BudgetDimension::ModelRequests), 1);
        assert_eq!(rebuilt.remaining(BudgetDimension::TotalTokens), 50);
        assert!(rebuilt.is_unknown(BudgetDimension::ProviderCostMicros, UsageKind::Reported));
    }

    #[test]
    fn wall_clock_is_only_tracked_when_explicitly_selected() {
        let mut ledger = ledger();
        let at = TimestampMillis::new(1_700_000_000_001);
        assert_eq!(
            ledger.commit(
                &UsageEvidence::new(
                    BudgetDimension::ElapsedWallClockMs,
                    5,
                    UsageKind::Reported,
                    None,
                ),
                at,
            ),
            Err(BudgetError::WallClockDisabled)
        );
        let (mut enabled, _) = BudgetLedger::initialize(
            session(),
            BudgetLimits {
                max_elapsed_wall_clock_ms: Some(10),
                ..BudgetLimits::default()
            },
            TimestampMillis::new(1_700_000_000_000),
            true,
        );
        enabled
            .commit(
                &UsageEvidence::new(
                    BudgetDimension::ElapsedWallClockMs,
                    10,
                    UsageKind::Reported,
                    None,
                ),
                at,
            )
            .expect("commit wall clock");
        assert_eq!(enabled.remaining(BudgetDimension::ElapsedWallClockMs), 0);
    }

    #[test]
    fn counters_environment_is_deterministic() {
        let ledger = ledger();
        let environment = ledger.budget_environment();
        let serialized = serde_json::to_string(&environment).expect("serialize");
        assert!(serialized.contains(r#""style_steps""#));
        assert!(serialized.contains(r#""concurrent_children""#));
        assert_eq!(
            serde_json::to_string(&environment).expect("again"),
            serialized
        );
    }
}
