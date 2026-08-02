//! Independent provider and keyed-grant dependency boundary.

use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

use agentmod_harness_protocol::{ProjectedEntry, Usage};
use serde_json::Value;
use uuid::Uuid;

const MAX_ENTRIES: usize = 256;
const MAX_OPTIONS: usize = 64;
const MAX_TEXT_BYTES: usize = 16 * 1024;
const MAX_GRANT_LIFETIME_MS: u128 = 300_000;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ExecuteRequest {
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) entries: Vec<ProjectedEntry>,
    pub(crate) options: Value,
    pub(crate) authorization_grant: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProviderEvent {
    Started,
    Text(String),
    Completed { finish_reason: String, usage: Usage },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DependencyError {
    Authorization,
    InvalidRequest,
    Clock,
}

#[derive(Debug)]
pub(crate) struct IndependentProviderDependency {
    grants: GrantValidator,
}

impl IndependentProviderDependency {
    pub(crate) const fn new(key: [u8; 32]) -> Self {
        Self {
            grants: GrantValidator::new(key),
        }
    }

    pub(crate) fn execute(
        &mut self,
        request: &ExecuteRequest,
    ) -> Result<Vec<ProviderEvent>, DependencyError> {
        self.grants.validate(&request.authorization_grant)?;
        if !matches!(request.provider.as_str(), "deterministic-mock" | "mock")
            || request.model.trim().is_empty()
            || request.entries.len() > MAX_ENTRIES
        {
            return Err(DependencyError::InvalidRequest);
        }
        let options = request
            .options
            .as_object()
            .ok_or(DependencyError::InvalidRequest)?;
        if options.len() > MAX_OPTIONS
            || options
                .get("mock_scenario")
                .and_then(Value::as_str)
                .is_some_and(|scenario| scenario != "streaming_text")
        {
            return Err(DependencyError::InvalidRequest);
        }
        let requested_text = options
            .get("mock_text")
            .and_then(Value::as_str)
            .unwrap_or("deterministic response");
        if requested_text.len() > MAX_TEXT_BYTES {
            return Err(DependencyError::InvalidRequest);
        }
        let text = format!("independent-harness:{requested_text}");
        let input_tokens =
            u64::try_from(request.entries.len()).map_err(|_| DependencyError::InvalidRequest)?;
        let output_tokens = u64::try_from(text.split_whitespace().count())
            .map_err(|_| DependencyError::InvalidRequest)?;
        Ok(vec![
            ProviderEvent::Started,
            ProviderEvent::Text(text),
            ProviderEvent::Completed {
                finish_reason: String::from("stop"),
                usage: Usage {
                    input_tokens,
                    output_tokens,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                },
            },
        ])
    }
}

#[derive(Debug)]
struct GrantValidator {
    key: [u8; 32],
    used_nonces: BTreeSet<Uuid>,
}

impl GrantValidator {
    const fn new(key: [u8; 32]) -> Self {
        Self {
            key,
            used_nonces: BTreeSet::new(),
        }
    }

    fn validate(&mut self, grant: &str) -> Result<(), DependencyError> {
        let fields = grant.split('.').collect::<Vec<_>>();
        if fields.len() != 5
            || fields[0] != "v1"
            || !is_lower_hex(fields[3], 64)
            || !is_lower_hex(fields[4], 64)
        {
            return Err(DependencyError::Authorization);
        }
        let expires = fields[1]
            .parse::<u128>()
            .map_err(|_| DependencyError::Authorization)?;
        let nonce = fields[2]
            .parse::<Uuid>()
            .map_err(|_| DependencyError::Authorization)?;
        let now = now_millis()?;
        if expires < now || expires.saturating_sub(now) > MAX_GRANT_LIFETIME_MS {
            return Err(DependencyError::Authorization);
        }
        let payload = fields[..4].join(".");
        let expected = blake3::keyed_hash(&self.key, payload.as_bytes())
            .to_hex()
            .to_string();
        if !constant_time_equal(expected.as_bytes(), fields[4].as_bytes())
            || !self.used_nonces.insert(nonce)
        {
            return Err(DependencyError::Authorization);
        }
        Ok(())
    }
}

fn now_millis() -> Result<u128, DependencyError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|_| DependencyError::Clock)
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let maximum = left.len().max(right.len());
    for index in 0..maximum {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

pub(crate) fn parse_key(value: &str) -> Result<[u8; 32], DependencyError> {
    if !is_lower_hex(value, 64) {
        return Err(DependencyError::Authorization);
    }
    let mut key = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).map_err(|_| DependencyError::Authorization)?;
        key[index] = u8::from_str_radix(text, 16).map_err(|_| DependencyError::Authorization)?;
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(key: &[u8; 32], nonce: Uuid, expires: u128) -> String {
        let binding = "ab".repeat(32);
        let payload = format!("v1.{expires}.{nonce}.{binding}");
        let signature = blake3::keyed_hash(key, payload.as_bytes());
        format!("{payload}.{}", signature.to_hex())
    }

    fn request(grant: String) -> ExecuteRequest {
        ExecuteRequest {
            provider: String::from("mock"),
            model: String::from("fixture-model"),
            entries: vec![ProjectedEntry::User {
                text: String::from("hello"),
            }],
            options: serde_json::json!({
                "mock_scenario": "streaming_text",
                "mock_text": "proof",
            }),
            authorization_grant: grant,
        }
    }

    #[test]
    fn keyed_grant_is_verified_once_at_the_dependency_boundary() {
        let key = [7_u8; 32];
        let exact = grant(
            &key,
            Uuid::from_u128(1),
            now_millis().expect("clock") + 60_000,
        );
        let mut dependency = IndependentProviderDependency::new(key);
        let events = dependency
            .execute(&request(exact.clone()))
            .expect("exact grant");
        assert_eq!(
            events,
            vec![
                ProviderEvent::Started,
                ProviderEvent::Text(String::from("independent-harness:proof")),
                ProviderEvent::Completed {
                    finish_reason: String::from("stop"),
                    usage: Usage {
                        input_tokens: 1,
                        output_tokens: 1,
                        cache_read_tokens: 0,
                        cache_write_tokens: 0,
                    },
                },
            ]
        );
        assert_eq!(
            dependency.execute(&request(exact)),
            Err(DependencyError::Authorization)
        );
    }

    #[test]
    fn tampered_expired_and_wrong_key_grants_fail_closed() {
        let key = [9_u8; 32];
        let now = now_millis().expect("clock");
        let exact = grant(&key, Uuid::from_u128(2), now + 60_000);
        let mut tampered = exact.clone();
        tampered.push('0');
        let mut dependency = IndependentProviderDependency::new(key);
        assert_eq!(
            dependency.execute(&request(tampered)),
            Err(DependencyError::Authorization)
        );
        assert_eq!(
            dependency.execute(&request(grant(&key, Uuid::from_u128(3), now - 1))),
            Err(DependencyError::Authorization)
        );
        assert_eq!(
            IndependentProviderDependency::new([8_u8; 32]).execute(&request(exact)),
            Err(DependencyError::Authorization)
        );
    }

    #[test]
    fn key_parser_and_request_bounds_are_exact() {
        assert_eq!(parse_key(&"07".repeat(32)), Ok([7_u8; 32]));
        assert_eq!(
            parse_key(&"GG".repeat(32)),
            Err(DependencyError::Authorization)
        );

        let key = [5_u8; 32];
        let mut value = request(grant(
            &key,
            Uuid::from_u128(4),
            now_millis().expect("clock") + 60_000,
        ));
        value.provider = String::from("not-declared");
        assert_eq!(
            IndependentProviderDependency::new(key).execute(&value),
            Err(DependencyError::InvalidRequest)
        );
    }
}
