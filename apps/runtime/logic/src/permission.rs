//! Deterministic allow/ask/deny policy evaluation with mandatory-policy precedence.

use serde::{Deserialize, Serialize};

use crate::action::{ActionProposal, ConsequentialAction};

/// Permission outcome.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionEffect {
    /// Execute without interactive approval.
    Allow,
    /// Create a durable approval continuation.
    Ask,
    /// Never execute.
    Deny,
}

/// Stable matcher fields. `None` means the field is unconstrained.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct PermissionMatcher {
    /// Consequential action kind.
    pub action: Option<String>,
    /// Tool ID.
    pub tool: Option<String>,
    /// Tool group.
    pub tool_group: Option<String>,
    /// Plugin or MCP source.
    pub source: Option<String>,
    /// Canonical path prefix after dependency-independent normalization.
    pub path_prefix: Option<String>,
    /// Executable.
    pub executable: Option<String>,
    /// Domain exact/suffix pattern.
    pub domain: Option<String>,
    /// HTTP method.
    pub http_method: Option<String>,
    /// Provider ID.
    pub provider: Option<String>,
    /// Model ID.
    pub model: Option<String>,
    /// Session style.
    pub style: Option<String>,
    /// Workspace.
    pub workspace: Option<String>,
}

/// Ordered user or mandatory permission rule.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PermissionRule {
    /// Stable rule ID shown in audit events.
    pub id: String,
    /// Higher values evaluate first.
    pub priority: i32,
    /// Match conditions.
    pub matcher: PermissionMatcher,
    /// Outcome.
    pub effect: PermissionEffect,
    /// Safe explanation.
    pub reason: String,
}

/// Named policy with an explicit default.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PermissionPolicy {
    /// Stable policy ID.
    pub id: String,
    /// Ordered rules; construction normalizes them deterministically.
    pub rules: Vec<PermissionRule>,
    /// Outcome when no rule matches.
    pub default_effect: PermissionEffect,
    /// Safe default explanation.
    pub default_reason: String,
}

impl PermissionPolicy {
    /// Sorts a policy by priority descending, then stable rule ID.
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        mut rules: Vec<PermissionRule>,
        default_effect: PermissionEffect,
        default_reason: impl Into<String>,
    ) -> Self {
        rules.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.id.cmp(&right.id))
        });
        Self {
            id: id.into(),
            rules,
            default_effect,
            default_reason: default_reason.into(),
        }
    }

    fn evaluate(&self, proposal: &ActionProposal) -> PolicyMatch {
        self.rules
            .iter()
            .find(|rule| rule.matcher.matches(proposal))
            .map_or_else(
                || PolicyMatch {
                    policy_id: self.id.clone(),
                    rule_id: None,
                    effect: self.default_effect,
                    reason: self.default_reason.clone(),
                },
                |rule| PolicyMatch {
                    policy_id: self.id.clone(),
                    rule_id: Some(rule.id.clone()),
                    effect: rule.effect,
                    reason: rule.reason.clone(),
                },
            )
    }
}

impl PermissionMatcher {
    fn matches(&self, proposal: &ActionProposal) -> bool {
        optional_eq(self.action.as_deref(), proposal.action.kind())
            && optional_eq(self.style.as_deref(), &proposal.style)
            && optional_eq(self.workspace.as_deref(), &proposal.workspace)
            && self.matches_action(&proposal.action)
    }

    fn matches_action(&self, action: &ConsequentialAction) -> bool {
        match action {
            ConsequentialAction::ToolCall(tool) => {
                optional_eq(self.tool.as_deref(), &tool.tool)
                    && optional_eq(self.tool_group.as_deref(), &tool.group)
                    && optional_eq(self.source.as_deref(), tool.source.as_deref().unwrap_or(""))
                    && action_unrelated_fields_empty(self, ActionFieldGroup::Tool)
            }
            ConsequentialAction::FilesystemWrite(write) => {
                optional_prefix(self.path_prefix.as_deref(), &write.path)
                    && action_unrelated_fields_empty(self, ActionFieldGroup::Path)
            }
            ConsequentialAction::ProcessStart(process) => {
                optional_eq(self.executable.as_deref(), &process.executable)
                    && action_unrelated_fields_empty(self, ActionFieldGroup::Process)
            }
            ConsequentialAction::HttpRequest(request) => {
                let domain = extract_domain(&request.url);
                optional_domain(self.domain.as_deref(), domain)
                    && optional_eq(self.http_method.as_deref(), &request.method)
                    && action_unrelated_fields_empty(self, ActionFieldGroup::Http)
            }
            ConsequentialAction::ModelRequest(request) => {
                optional_eq(self.provider.as_deref(), &request.provider)
                    && optional_eq(self.model.as_deref(), &request.model)
                    && action_unrelated_fields_empty(self, ActionFieldGroup::Provider)
            }
            ConsequentialAction::ProviderSwitch { provider, model } => {
                optional_eq(self.provider.as_deref(), provider)
                    && optional_eq(self.model.as_deref(), model)
                    && action_unrelated_fields_empty(self, ActionFieldGroup::Provider)
            }
            _ => action_unrelated_fields_empty(self, ActionFieldGroup::None),
        }
    }
}

#[derive(Clone, Copy)]
enum ActionFieldGroup {
    None,
    Tool,
    Path,
    Process,
    Http,
    Provider,
}

fn action_unrelated_fields_empty(matcher: &PermissionMatcher, group: ActionFieldGroup) -> bool {
    (matches!(group, ActionFieldGroup::Tool)
        || (matcher.tool.is_none() && matcher.tool_group.is_none() && matcher.source.is_none()))
        && (matches!(group, ActionFieldGroup::Path) || matcher.path_prefix.is_none())
        && (matches!(group, ActionFieldGroup::Process) || matcher.executable.is_none())
        && (matches!(group, ActionFieldGroup::Http)
            || (matcher.domain.is_none() && matcher.http_method.is_none()))
        && (matches!(group, ActionFieldGroup::Provider)
            || (matcher.provider.is_none() && matcher.model.is_none()))
}

fn optional_eq(expected: Option<&str>, actual: &str) -> bool {
    expected.is_none_or(|expected| expected.eq_ignore_ascii_case(actual))
}

fn optional_prefix(expected: Option<&str>, actual: &str) -> bool {
    expected.is_none_or(|expected| {
        let expected = expected.trim_end_matches(['/', '\\']);
        actual == expected
            || actual
                .strip_prefix(expected)
                .is_some_and(|suffix| suffix.starts_with(['/', '\\']))
    })
}

fn optional_domain(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected.is_none_or(|expected| {
        actual.is_some_and(|actual| {
            let expected = expected.to_ascii_lowercase();
            let actual = actual.to_ascii_lowercase();
            if let Some(suffix) = expected.strip_prefix("*.") {
                actual != suffix && actual.ends_with(&format!(".{suffix}"))
            } else {
                actual == expected
            }
        })
    })
}

fn extract_domain(url: &str) -> Option<&str> {
    let after_scheme = url.split_once("://")?.1;
    let authority = after_scheme.split(['/', '?', '#']).next()?;
    let without_user = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if without_user.starts_with('[') {
        without_user
            .split_once(']')
            .map(|(host, _)| host.trim_start_matches('['))
    } else {
        Some(without_user.split(':').next().unwrap_or(without_user))
    }
}

/// One auditable policy evaluation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PolicyMatch {
    /// Policy ID.
    pub policy_id: String,
    /// Matching rule or `None` for the default.
    pub rule_id: Option<String>,
    /// Rule/default outcome.
    pub effect: PermissionEffect,
    /// Safe explanation.
    pub reason: String,
}

/// Final permission result after user then mandatory policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PermissionDecision {
    /// Final outcome.
    pub effect: PermissionEffect,
    /// User-policy evaluation first, mandatory evaluation second.
    pub trace: Vec<PolicyMatch>,
    /// Safe final explanation.
    pub reason: String,
}

/// Applies user policy and then mandatory native security policy.
///
/// Mandatory deny always wins. Mandatory ask upgrades user allow. User deny/ask cannot
/// be relaxed by mandatory allow.
#[must_use]
pub fn evaluate_permissions(
    proposal: &ActionProposal,
    user: &PermissionPolicy,
    mandatory: &PermissionPolicy,
) -> PermissionDecision {
    let user_match = user.evaluate(proposal);
    let mandatory_match = mandatory.evaluate(proposal);
    let (effect, reason) = match (user_match.effect, mandatory_match.effect) {
        (_, PermissionEffect::Deny) => (PermissionEffect::Deny, mandatory_match.reason.clone()),
        (PermissionEffect::Deny, _) => (PermissionEffect::Deny, user_match.reason.clone()),
        (_, PermissionEffect::Ask) => (PermissionEffect::Ask, mandatory_match.reason.clone()),
        (PermissionEffect::Ask, _) => (PermissionEffect::Ask, user_match.reason.clone()),
        (PermissionEffect::Allow, PermissionEffect::Allow) => {
            (PermissionEffect::Allow, mandatory_match.reason.clone())
        }
    };
    PermissionDecision {
        effect,
        trace: vec![user_match, mandatory_match],
        reason,
    }
}

/// Revalidates mandatory policy immediately before an already user-approved
/// action resumes.
///
/// A new mandatory deny blocks execution. `Ask` is satisfied by the durable
/// approval being resolved, while `Allow` remains allowed.
#[must_use]
pub fn revalidate_mandatory_after_approval(
    proposal: &ActionProposal,
    mandatory: &PermissionPolicy,
) -> PermissionDecision {
    let mandatory_match = mandatory.evaluate(proposal);
    let effect = if mandatory_match.effect == PermissionEffect::Deny {
        PermissionEffect::Deny
    } else {
        PermissionEffect::Allow
    };
    PermissionDecision {
        effect,
        reason: mandatory_match.reason.clone(),
        trace: vec![mandatory_match],
    }
}

#[cfg(test)]
mod tests {
    use agentmod_primitives::ContentHash;

    use crate::action::{FilesystemWriteAction, HttpRequestAction, ProposalId};

    use super::*;

    fn file(path: &str) -> ActionProposal {
        ActionProposal {
            id: ProposalId("p1".into()),
            action: ConsequentialAction::FilesystemWrite(FilesystemWriteAction {
                path: path.into(),
                expected_hash: None,
                content_hash: ContentHash::digest(b"x"),
                overwrite: false,
            }),
            style: "persistent-chat".into(),
            workspace: "repo".into(),
            origin: "runtime".into(),
        }
    }

    fn policy(id: &str, effect: PermissionEffect, matcher: PermissionMatcher) -> PermissionPolicy {
        PermissionPolicy::new(
            id,
            vec![PermissionRule {
                id: format!("{id}-rule"),
                priority: 0,
                matcher,
                effect,
                reason: format!("{id} decision"),
            }],
            PermissionEffect::Ask,
            "default ask",
        )
    }

    #[test]
    fn mandatory_deny_overrides_user_allow_and_runs_second() {
        let user = policy(
            "user",
            PermissionEffect::Allow,
            PermissionMatcher {
                action: Some("filesystem_write".into()),
                ..PermissionMatcher::default()
            },
        );
        let mandatory = policy(
            "mandatory",
            PermissionEffect::Deny,
            PermissionMatcher {
                path_prefix: Some("secrets".into()),
                ..PermissionMatcher::default()
            },
        );
        let decision = evaluate_permissions(&file("secrets/token"), &user, &mandatory);
        assert_eq!(decision.effect, PermissionEffect::Deny);
        assert_eq!(decision.trace[0].policy_id, "user");
        assert_eq!(decision.trace[1].policy_id, "mandatory");
        assert_eq!(decision.reason, "mandatory decision");
    }

    #[test]
    fn rule_order_is_priority_then_id() {
        let policy = PermissionPolicy::new(
            "ordered",
            vec![
                PermissionRule {
                    id: "z".into(),
                    priority: 1,
                    matcher: PermissionMatcher::default(),
                    effect: PermissionEffect::Allow,
                    reason: "z".into(),
                },
                PermissionRule {
                    id: "a".into(),
                    priority: 1,
                    matcher: PermissionMatcher::default(),
                    effect: PermissionEffect::Deny,
                    reason: "a".into(),
                },
            ],
            PermissionEffect::Ask,
            "default",
        );
        assert_eq!(
            policy.evaluate(&file("src/lib.rs")).rule_id.as_deref(),
            Some("a")
        );
    }

    #[test]
    fn domain_suffix_does_not_match_apex_or_lookalike() {
        let matcher = PermissionMatcher {
            domain: Some("*.example.com".into()),
            http_method: Some("GET".into()),
            ..PermissionMatcher::default()
        };
        let proposal = |url: &str| ActionProposal {
            id: ProposalId("p".into()),
            action: ConsequentialAction::HttpRequest(HttpRequestAction {
                method: "GET".into(),
                url: url.into(),
                header_names: vec![],
                body_hash: None,
            }),
            style: "persistent-chat".into(),
            workspace: "repo".into(),
            origin: "runtime".into(),
        };
        assert!(matcher.matches(&proposal("https://api.example.com/v1")));
        assert!(!matcher.matches(&proposal("https://example.com")));
        assert!(!matcher.matches(&proposal("https://example.com.evil.test")));
    }
}
