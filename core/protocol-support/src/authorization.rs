//! Short-lived keyed authorization grants for consequential protocol requests.
//!
//! Grant verification is only one enforcement step. A host dependency must also
//! record the verified nonce atomically and reject replay before performing a
//! side effect.

use std::fmt;

use agentmod_primitives::{ContentHash, TimestampMillis};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const TOKEN_VERSION: &str = "v1";
const KEY_BYTES: usize = 32;
const MAX_PAYLOAD_BYTES: usize = 4096;
const MAX_CLAIM_TEXT_BYTES: usize = 512;
const MAX_LIFETIME_MILLIS: i64 = 5 * 60 * 1000;
const CLOCK_SKEW_MILLIS: i64 = 30 * 1000;

/// Signed claims binding one exact action to one local owner/session/call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationClaims {
    /// Local authenticated connection owner.
    pub owner: String,
    /// Runtime session whose policy approved the action.
    pub session: String,
    /// Exact protocol call identifier.
    pub call_id: String,
    /// Stable tool/action name.
    pub action: String,
    /// Digest of exact normalized arguments.
    pub normalized_digest: ContentHash,
    /// Grant issue time.
    pub issued_at: TimestampMillis,
    /// Hard expiry.
    pub expires_at: TimestampMillis,
    /// Unique single-use nonce.
    pub nonce: String,
}

/// Expected request binding supplied independently from decoded claims.
#[derive(Clone, Copy, Debug)]
pub struct ExpectedAuthorization<'a> {
    /// Authenticated connection owner.
    pub owner: &'a str,
    /// Selected runtime session.
    pub session: &'a str,
    /// Received call ID.
    pub call_id: &'a str,
    /// Received normalized action/tool name.
    pub action: &'a str,
    /// Recomputed canonical argument digest.
    pub normalized_digest: ContentHash,
}

/// Secret key used only by local runtime and host authorization dependencies.
pub struct AuthorizationKey([u8; KEY_BYTES]);

impl AuthorizationKey {
    /// Creates a key from exact bytes loaded through a secret dependency.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; KEY_BYTES]) -> Self {
        Self(bytes)
    }

    /// Decodes a 64-character hexadecimal secret reference value.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationError::InvalidKey`] without exposing key content.
    pub fn from_hex(value: &str) -> Result<Self, AuthorizationError> {
        let bytes = decode_hex(value, KEY_BYTES).map_err(|()| AuthorizationError::InvalidKey)?;
        let key: [u8; KEY_BYTES] = bytes
            .try_into()
            .map_err(|_| AuthorizationError::InvalidKey)?;
        Ok(Self(key))
    }

    fn bytes(&self) -> &[u8; KEY_BYTES] {
        &self.0
    }
}

impl fmt::Debug for AuthorizationKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizationKey([REDACTED])")
    }
}

impl Drop for AuthorizationKey {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Creates a portable keyed grant string.
///
/// # Errors
///
/// Returns [`AuthorizationError`] for invalid claim bounds or serialization.
pub fn seal_authorization(
    claims: &AuthorizationClaims,
    key: &AuthorizationKey,
) -> Result<String, AuthorizationError> {
    validate_claims(claims)?;
    let payload = serde_json::to_vec(claims)
        .map_err(|error| AuthorizationError::Encoding(error.to_string()))?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(AuthorizationError::PayloadTooLarge);
    }
    let mac = blake3::keyed_hash(key.bytes(), &payload);
    Ok(format!(
        "{TOKEN_VERSION}.{}.{}",
        encode_hex(&payload),
        mac.to_hex()
    ))
}

/// Verifies authenticity, bounds, time, and exact request binding.
///
/// The returned nonce must be atomically consumed by the host dependency before
/// any side effect.
///
/// # Errors
///
/// Returns a stable [`AuthorizationError`] without including claims or secrets.
pub fn verify_authorization(
    token: &str,
    key: &AuthorizationKey,
    expected: ExpectedAuthorization<'_>,
    now: TimestampMillis,
) -> Result<AuthorizationClaims, AuthorizationError> {
    if token.len() > MAX_PAYLOAD_BYTES * 2 + 80 {
        return Err(AuthorizationError::PayloadTooLarge);
    }
    let mut segments = token.split('.');
    if segments.next() != Some(TOKEN_VERSION) {
        return Err(AuthorizationError::InvalidToken);
    }
    let payload_hex = segments.next().ok_or(AuthorizationError::InvalidToken)?;
    let mac_hex = segments.next().ok_or(AuthorizationError::InvalidToken)?;
    if segments.next().is_some() {
        return Err(AuthorizationError::InvalidToken);
    }
    let payload = decode_hex(payload_hex, MAX_PAYLOAD_BYTES)
        .map_err(|()| AuthorizationError::InvalidToken)?;
    let supplied_mac = decode_hex(mac_hex, 32).map_err(|()| AuthorizationError::InvalidToken)?;
    if supplied_mac.len() != 32 {
        return Err(AuthorizationError::InvalidToken);
    }
    let expected_mac = blake3::keyed_hash(key.bytes(), &payload);
    if !constant_time_equal(expected_mac.as_bytes(), &supplied_mac) {
        return Err(AuthorizationError::InvalidMac);
    }
    let claims: AuthorizationClaims =
        serde_json::from_slice(&payload).map_err(|_| AuthorizationError::InvalidToken)?;
    validate_claims(&claims)?;
    validate_time(&claims, now)?;
    if claims.owner != expected.owner
        || claims.session != expected.session
        || claims.call_id != expected.call_id
        || claims.action != expected.action
        || claims.normalized_digest != expected.normalized_digest
    {
        return Err(AuthorizationError::BindingMismatch);
    }
    Ok(claims)
}

fn validate_claims(claims: &AuthorizationClaims) -> Result<(), AuthorizationError> {
    for value in [
        &claims.owner,
        &claims.session,
        &claims.call_id,
        &claims.action,
        &claims.nonce,
    ] {
        if value.is_empty()
            || value.len() > MAX_CLAIM_TEXT_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(AuthorizationError::InvalidClaims);
        }
    }
    if claims.expires_at.get() <= claims.issued_at.get()
        || claims.expires_at.get() - claims.issued_at.get() > MAX_LIFETIME_MILLIS
    {
        return Err(AuthorizationError::InvalidLifetime);
    }
    Ok(())
}

fn validate_time(
    claims: &AuthorizationClaims,
    now: TimestampMillis,
) -> Result<(), AuthorizationError> {
    if now.get() > claims.expires_at.get() {
        return Err(AuthorizationError::Expired);
    }
    if claims.issued_at.get() > now.get().saturating_add(CLOCK_SKEW_MILLIS) {
        return Err(AuthorizationError::NotYetValid);
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(value: &str, maximum_bytes: usize) -> Result<Vec<u8>, ()> {
    if !value.len().is_multiple_of(2) || value.len() / 2 > maximum_bytes {
        return Err(());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or(())?;
            let low = hex_nibble(pair[1]).ok_or(())?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

/// Grant creation or verification failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AuthorizationError {
    /// Configured key is absent or malformed.
    #[error("authorization key is invalid")]
    InvalidKey,
    /// Claim text or bounds are invalid.
    #[error("authorization claims are invalid")]
    InvalidClaims,
    /// Issue/expiry relationship exceeds the bounded lifetime.
    #[error("authorization grant lifetime is invalid")]
    InvalidLifetime,
    /// Encoded token exceeds its hard bound.
    #[error("authorization grant payload is too large")]
    PayloadTooLarge,
    /// Encoding failed.
    #[error("authorization grant encoding failed: {0}")]
    Encoding(String),
    /// Token format or payload is invalid.
    #[error("authorization grant is invalid")]
    InvalidToken,
    /// Keyed authentication failed.
    #[error("authorization grant authentication failed")]
    InvalidMac,
    /// Grant expired.
    #[error("authorization grant expired")]
    Expired,
    /// Grant issue time is unreasonably in the future.
    #[error("authorization grant is not yet valid")]
    NotYetValid,
    /// Claims do not bind the received request.
    #[error("authorization grant does not match this request")]
    BindingMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> AuthorizationKey {
        AuthorizationKey::from_bytes([7; 32])
    }

    fn claims() -> AuthorizationClaims {
        AuthorizationClaims {
            owner: "local-user".into(),
            session: "session-1".into(),
            call_id: "call-1".into(),
            action: "filesystem.write".into(),
            normalized_digest: ContentHash::digest(b"normalized"),
            issued_at: TimestampMillis::new(1_000),
            expires_at: TimestampMillis::new(2_000),
            nonce: "nonce-1".into(),
        }
    }

    fn expected(claims: &AuthorizationClaims) -> ExpectedAuthorization<'_> {
        ExpectedAuthorization {
            owner: &claims.owner,
            session: &claims.session,
            call_id: &claims.call_id,
            action: &claims.action,
            normalized_digest: claims.normalized_digest,
        }
    }

    #[test]
    fn valid_token_round_trips_without_exposing_key() {
        let claims = claims();
        let token = seal_authorization(&claims, &key()).expect("seal");
        assert_eq!(
            verify_authorization(
                &token,
                &key(),
                expected(&claims),
                TimestampMillis::new(1_500)
            )
            .expect("verify"),
            claims
        );
        assert_eq!(format!("{:?}", key()), "AuthorizationKey([REDACTED])");
    }

    #[test]
    fn tamper_expiry_and_binding_are_rejected() {
        let claims = claims();
        let token = seal_authorization(&claims, &key()).expect("seal");
        let mut tampered = token.into_bytes();
        let index = tampered.len() / 2;
        tampered[index] = if tampered[index] == b'a' { b'b' } else { b'a' };
        assert!(matches!(
            verify_authorization(
                std::str::from_utf8(&tampered).expect("utf8"),
                &key(),
                expected(&claims),
                TimestampMillis::new(1_500)
            ),
            Err(AuthorizationError::InvalidMac | AuthorizationError::InvalidToken)
        ));

        let token = seal_authorization(&claims, &key()).expect("seal");
        assert_eq!(
            verify_authorization(
                &token,
                &key(),
                expected(&claims),
                TimestampMillis::new(2_001)
            ),
            Err(AuthorizationError::Expired)
        );
        let wrong = ExpectedAuthorization {
            call_id: "other",
            ..expected(&claims)
        };
        assert_eq!(
            verify_authorization(&token, &key(), wrong, TimestampMillis::new(1_500)),
            Err(AuthorizationError::BindingMismatch)
        );
    }

    #[test]
    fn key_parser_requires_exact_secret_length() {
        assert!(AuthorizationKey::from_hex(&"01".repeat(32)).is_ok());
        assert!(matches!(
            AuthorizationKey::from_hex("abcd"),
            Err(AuthorizationError::InvalidKey)
        ));
    }
}
