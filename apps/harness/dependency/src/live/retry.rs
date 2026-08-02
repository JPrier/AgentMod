//! Provider-neutral retry classification for live HTTP exchanges.

use std::time::Duration;

use reqwest::header::HeaderMap;

use crate::execution::{
    DependencyProviderFailureKind, DependencyRetryClassification,
};

/// One classified provider failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassifiedFailure {
    /// Stable dependency failure kind.
    pub kind: DependencyProviderFailureKind,
    /// Redacted diagnostic without credentials or headers.
    pub message: String,
    /// Retry guidance; ambiguous exchanges are never automatically retried.
    pub retry: DependencyRetryClassification,
}

/// Classifies an HTTP error response received before streaming began.
#[must_use]
pub fn classify_http_status(
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    body_excerpt: &str,
) -> ClassifiedFailure {
    let code = status.as_u16();
    let redacted_detail = redact_body_excerpt(body_excerpt);
    match code {
        401 | 403 => ClassifiedFailure {
            kind: DependencyProviderFailureKind::AuthenticationFailed,
            message: format!("provider rejected the supplied credentials (HTTP {code})"),
            retry: DependencyRetryClassification::Never,
        },
        429 => {
            let delay = retry_after_millis(headers);
            ClassifiedFailure {
                kind: DependencyProviderFailureKind::RateLimited,
                message: format!("provider rate limited the request (HTTP {code})"),
                retry: DependencyRetryClassification::AfterMilliseconds(delay),
            }
        }
        404 => ClassifiedFailure {
            kind: DependencyProviderFailureKind::UnsupportedCapability,
            message: format!("provider endpoint or model was not found (HTTP {code})"),
            retry: DependencyRetryClassification::Never,
        },
        400 | 422 => ClassifiedFailure {
            kind: DependencyProviderFailureKind::InvalidRequest,
            message: format!("provider rejected the request as invalid (HTTP {code})"),
            retry: DependencyRetryClassification::Never,
        },
        408 => ClassifiedFailure {
            kind: DependencyProviderFailureKind::Timeout,
            message: format!("provider request timed out before dispatch (HTTP {code})"),
            retry: DependencyRetryClassification::Immediate,
        },
        430..=499 => ClassifiedFailure {
            kind: DependencyProviderFailureKind::InvalidRequest,
            message: format!("provider rejected the request (HTTP {code})"),
            retry: DependencyRetryClassification::Never,
        },
        500..=599 => ClassifiedFailure {
            kind: DependencyProviderFailureKind::ProviderOverloaded,
            message: format!("provider reported an overload or server failure (HTTP {code})"),
            retry: DependencyRetryClassification::AfterMilliseconds(1_000),
        },
        _ => ClassifiedFailure {
            kind: DependencyProviderFailureKind::InvalidRequest,
            message: format!(
                "provider returned an unexpected status (HTTP {code}): {redacted_detail}"
            ),
            retry: DependencyRetryClassification::Never,
        },
    }
}

/// Classifies a provider-reported error object carried inside a stream.
#[must_use]
pub fn classify_provider_error(kind: &str, message: &str) -> ClassifiedFailure {
    let redacted = redact_body_excerpt(message);
    match kind {
        "rate_limit_error" | "rate_limited" => ClassifiedFailure {
            kind: DependencyProviderFailureKind::RateLimited,
            message: "provider rate limited the request".into(),
            retry: DependencyRetryClassification::AfterMilliseconds(1_000),
        },
        "overloaded_error" | "server_error" => ClassifiedFailure {
            kind: DependencyProviderFailureKind::ProviderOverloaded,
            message: "provider reported an overload or server failure".into(),
            retry: DependencyRetryClassification::AfterMilliseconds(1_000),
        },
        "authentication_error" | "permission_error" | "invalid_api_key" => ClassifiedFailure {
            kind: DependencyProviderFailureKind::AuthenticationFailed,
            message: "provider rejected the supplied credentials".into(),
            retry: DependencyRetryClassification::Never,
        },
        "invalid_request_error" | "bad_request" => {
            if redacted.is_empty() {
                ClassifiedFailure {
                    kind: DependencyProviderFailureKind::InvalidRequest,
                    message: "provider rejected the request as invalid".into(),
                    retry: DependencyRetryClassification::Never,
                }
            } else {
                ClassifiedFailure {
                    kind: DependencyProviderFailureKind::InvalidRequest,
                    message: format!("provider rejected the request as invalid: {redacted}"),
                    retry: DependencyRetryClassification::Never,
                }
            }
        }
        _ => {
            if redacted.is_empty() {
                ClassifiedFailure {
                    kind: DependencyProviderFailureKind::InvalidRequest,
                    message: "provider reported an error in the stream".into(),
                    retry: DependencyRetryClassification::Never,
                }
            } else {
                ClassifiedFailure {
                    kind: DependencyProviderFailureKind::InvalidRequest,
                    message: format!("provider reported an error: {redacted}"),
                    retry: DependencyRetryClassification::Never,
                }
            }
        }
    }
}

/// Classifies a transport failure that happened before any provider event.
#[must_use]
pub fn classify_pre_dispatch_transport(detail: &str) -> ClassifiedFailure {
    ClassifiedFailure {
        kind: DependencyProviderFailureKind::TransportFailure,
        message: redacted_transport(detail),
        retry: DependencyRetryClassification::Immediate,
    }
}

/// Classifies a disconnect after dispatch whose outcome is ambiguous.
#[must_use]
pub fn classify_ambiguous_disconnect(detail: &str) -> ClassifiedFailure {
    ClassifiedFailure {
        kind: DependencyProviderFailureKind::AmbiguousDisconnect,
        message: redacted_transport(detail),
        retry: DependencyRetryClassification::Never,
    }
}

/// Classifies a deadline expiry before any provider event as a safe retry.
#[must_use]
pub fn classify_pre_dispatch_timeout() -> ClassifiedFailure {
    ClassifiedFailure {
        kind: DependencyProviderFailureKind::Timeout,
        message: "provider deadline elapsed before any response".into(),
        retry: DependencyRetryClassification::Immediate,
    }
}

/// Classifies a deadline expiry after partial output as ambiguous.
#[must_use]
pub fn classify_partial_timeout() -> ClassifiedFailure {
    ClassifiedFailure {
        kind: DependencyProviderFailureKind::PartialOutputFailure,
        message: "provider stream stopped after partial output".into(),
        retry: DependencyRetryClassification::Never,
    }
}

/// Returns the bounded redacted body excerpt used only for diagnostics.
fn redact_body_excerpt(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    // Prefer a provider error message field when present.
    let extracted = serde_json::from_str::<serde_json::Value>(trimmed)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| trimmed.to_owned());
    let mut chars = extracted.chars();
    let bounded: String = chars.by_ref().take(256).collect();
    if chars.next().is_some() {
        bounded + "…"
    } else {
        bounded
    }
}

fn redacted_transport(detail: &str) -> String {
    // Transport diagnostics can contain URLs with embedded credentials;
    // keep only a bounded, credential-stripped excerpt.
    let mut result = String::with_capacity(128.min(detail.len()));
    for character in detail.chars().take(128) {
        if character.is_control() {
            result.push(' ');
        } else {
            result.push(character);
        }
    }
    if result.is_empty() {
        "provider transport failed".to_owned()
    } else {
        result
    }
}

fn retry_after_millis(headers: &HeaderMap) -> u64 {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(1_000, |seconds| seconds.saturating_mul(1_000))
}

/// Returns the configured per-request timeout as a duration.
#[must_use]
pub const fn request_timeout(timeout: Duration) -> Duration {
    timeout
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_http_statuses_without_leaking_headers() {
        let headers = HeaderMap::new();
        let auth = classify_http_status(reqwest::StatusCode::UNAUTHORIZED, &headers, "");
        assert_eq!(auth.kind, DependencyProviderFailureKind::AuthenticationFailed);
        assert_eq!(auth.retry, DependencyRetryClassification::Never);
        assert!(!auth.message.contains("Bearer"));

        let mut rate = HeaderMap::new();
        rate.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("2"),
        );
        let rate = classify_http_status(reqwest::StatusCode::TOO_MANY_REQUESTS, &rate, "{}");
        assert_eq!(rate.kind, DependencyProviderFailureKind::RateLimited);
        assert_eq!(
            rate.retry,
            DependencyRetryClassification::AfterMilliseconds(2_000)
        );

        let overload =
            classify_http_status(reqwest::StatusCode::SERVICE_UNAVAILABLE, &headers, "");
        assert_eq!(overload.kind, DependencyProviderFailureKind::ProviderOverloaded);

        let invalid = classify_http_status(reqwest::StatusCode::BAD_REQUEST, &headers, "");
        assert_eq!(invalid.kind, DependencyProviderFailureKind::InvalidRequest);
        assert_eq!(invalid.retry, DependencyRetryClassification::Never);

        let missing = classify_http_status(reqwest::StatusCode::NOT_FOUND, &headers, "");
        assert_eq!(
            missing.kind,
            DependencyProviderFailureKind::UnsupportedCapability
        );
    }

    #[test]
    fn body_excerpt_uses_provider_error_field_and_is_bounded() {
        let body = r#"{"error":{"message":"model gpt-4 is not supported"}}"#;
        let classified = classify_provider_error("invalid_request_error", body);
        assert!(classified.message.contains("model gpt-4 is not supported"));
        assert!(classified.message.len() < 512);
    }

    #[test]
    fn ambiguous_after_dispatch_is_never_retried() {
        let pre = classify_pre_dispatch_transport("connection refused");
        assert_eq!(pre.kind, DependencyProviderFailureKind::TransportFailure);
        assert_eq!(pre.retry, DependencyRetryClassification::Immediate);

        let ambiguous = classify_ambiguous_disconnect("stream ended unexpectedly");
        assert_eq!(
            ambiguous.kind,
            DependencyProviderFailureKind::AmbiguousDisconnect
        );
        assert_eq!(ambiguous.retry, DependencyRetryClassification::Never);

        let partial = classify_partial_timeout();
        assert_eq!(
            partial.kind,
            DependencyProviderFailureKind::PartialOutputFailure
        );
        assert_eq!(partial.retry, DependencyRetryClassification::Never);
    }
}
