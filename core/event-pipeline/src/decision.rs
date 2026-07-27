use std::fmt;

/// Opaque continuation identifier supplied by the embedding runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContinuationId(pub String);

/// User-facing approval information without transport-specific fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalRequest {
    /// Human-readable action summary.
    pub summary: String,
}

/// Deterministic wake condition interpreted by the embedding runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WakeCondition {
    /// Wake after a portable timestamp represented as Unix milliseconds.
    AtUnixMilliseconds(i64),
    /// Wake when the named committed event is observed.
    CommittedEvent(String),
    /// Wake only through an explicit continuation request.
    Explicit,
}

/// Policy used to join forked proposals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinPolicy {
    /// Require all branches.
    All,
    /// Accept any completed branch.
    Any,
    /// Accept the first successful branch.
    FirstSuccess,
}

/// Typed result returned by a blocking interceptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Decision<T> {
    /// Continue with the supplied proposal.
    Continue(T),
    /// Replace the proposal and continue through remaining interceptors.
    Replace(T),
    /// Reject the proposal without executing it.
    Reject {
        /// Human-readable reason safe for the proposal caller.
        reason: String,
    },
    /// Pause for explicit approval.
    RequireApproval {
        /// Approval information.
        request: ApprovalRequest,
        /// Durable continuation to resolve.
        continuation: ContinuationId,
    },
    /// Pause until a deterministic wake condition occurs.
    Defer {
        /// Durable continuation to resume.
        continuation: ContinuationId,
        /// Required wake condition.
        wake_condition: WakeCondition,
    },
    /// Cancel the proposal.
    Cancel {
        /// Human-readable cancellation reason.
        reason: String,
    },
    /// Fork into independently evaluated proposal branches.
    Fork {
        /// Branch inputs.
        branches: Vec<T>,
        /// Join behavior.
        join: JoinPolicy,
    },
}

impl<T> Decision<T> {
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::Continue(_) => "continue",
            Self::Replace(_) => "replace",
            Self::Reject { .. } => "reject",
            Self::RequireApproval { .. } => "require-approval",
            Self::Defer { .. } => "defer",
            Self::Cancel { .. } => "cancel",
            Self::Fork { .. } => "fork",
        }
    }
}

/// Optional decision capability supported by an action and session style.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecisionCapability {
    /// Proposal replacement.
    Replace,
    /// Durable user approval.
    Approval,
    /// Deferred continuation.
    Defer,
    /// Explicit cancellation.
    Cancel,
    /// Proposal forking.
    Fork,
}

impl DecisionCapability {
    const fn bit(self) -> u8 {
        match self {
            Self::Replace => 1 << 0,
            Self::Approval => 1 << 1,
            Self::Defer => 1 << 2,
            Self::Cancel => 1 << 3,
            Self::Fork => 1 << 4,
        }
    }
}

/// Compact set of decision capabilities supported by one action and session style.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActionCapabilities {
    supported: u8,
}

impl ActionCapabilities {
    /// Capabilities supporting every decision variant.
    #[must_use]
    pub const fn all() -> Self {
        Self {
            supported: DecisionCapability::Replace.bit()
                | DecisionCapability::Approval.bit()
                | DecisionCapability::Defer.bit()
                | DecisionCapability::Cancel.bit()
                | DecisionCapability::Fork.bit(),
        }
    }

    /// Capabilities supporting only continue and reject.
    #[must_use]
    pub const fn minimal() -> Self {
        Self { supported: 0 }
    }

    /// Adds one supported decision capability.
    #[must_use]
    pub const fn with(mut self, capability: DecisionCapability) -> Self {
        self.supported |= capability.bit();
        self
    }

    /// Reports whether an optional decision capability is supported.
    #[must_use]
    pub const fn supports(self, capability: DecisionCapability) -> bool {
        self.supported & capability.bit() != 0
    }

    /// Validates a decision against action capabilities.
    ///
    /// # Errors
    ///
    /// Returns an error when the action does not support the decision variant.
    pub fn validate<T>(&self, decision: &Decision<T>) -> Result<(), DecisionCapabilityError> {
        let supported = match decision {
            Decision::Continue(_) | Decision::Reject { .. } => true,
            Decision::Replace(_) => self.supports(DecisionCapability::Replace),
            Decision::RequireApproval { .. } => self.supports(DecisionCapability::Approval),
            Decision::Defer { .. } => self.supports(DecisionCapability::Defer),
            Decision::Cancel { .. } => self.supports(DecisionCapability::Cancel),
            Decision::Fork { .. } => self.supports(DecisionCapability::Fork),
        };
        if supported {
            Ok(())
        } else {
            Err(DecisionCapabilityError {
                decision: decision.kind(),
            })
        }
    }
}

/// Error returned when a handler chooses an unsupported decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecisionCapabilityError {
    decision: &'static str,
}

impl DecisionCapabilityError {
    /// Returns the unsupported decision name.
    #[must_use]
    pub const fn decision(&self) -> &'static str {
        self.decision
    }
}

impl fmt::Display for DecisionCapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "decision `{}` is not supported by this action",
            self.decision
        )
    }
}

impl std::error::Error for DecisionCapabilityError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_each_decision_capability() {
        let minimal = ActionCapabilities::minimal();
        assert!(minimal.validate(&Decision::Continue(1_u8)).is_ok());
        assert!(
            minimal
                .validate::<u8>(&Decision::Reject {
                    reason: "policy".into()
                })
                .is_ok()
        );
        assert_eq!(
            minimal
                .validate(&Decision::Replace(2_u8))
                .expect_err("replace is unsupported")
                .decision(),
            "replace"
        );
        assert_eq!(
            minimal
                .validate::<u8>(&Decision::RequireApproval {
                    request: ApprovalRequest {
                        summary: "write".into()
                    },
                    continuation: ContinuationId("c1".into()),
                })
                .expect_err("approval is unsupported")
                .decision(),
            "require-approval"
        );
        assert_eq!(
            minimal
                .validate::<u8>(&Decision::Defer {
                    continuation: ContinuationId("c2".into()),
                    wake_condition: WakeCondition::Explicit,
                })
                .expect_err("defer is unsupported")
                .decision(),
            "defer"
        );
        assert_eq!(
            minimal
                .validate::<u8>(&Decision::Cancel {
                    reason: "stop".into()
                })
                .expect_err("cancel is unsupported")
                .decision(),
            "cancel"
        );
        assert_eq!(
            minimal
                .validate(&Decision::Fork {
                    branches: vec![1_u8, 2],
                    join: JoinPolicy::All,
                })
                .expect_err("fork is unsupported")
                .decision(),
            "fork"
        );
    }
}
