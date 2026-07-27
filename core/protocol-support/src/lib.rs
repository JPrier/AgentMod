//! Transport-independent framing, negotiation, and error envelopes.

pub mod authorization;

use std::collections::BTreeSet;

use agentmod_primitives::{
    CancellationId, CausationId, CorrelationId, IdempotencyId, RequestId, Version,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Stable prefix identifying `AgentMod` frames.
pub const MAGIC: [u8; 4] = *b"AMOD";

/// Default maximum decoded frame body.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Type of protocol frame.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameKind {
    /// Initial version/capability negotiation.
    Handshake,
    /// A request expecting a response or stream.
    Request,
    /// A unary response.
    Response,
    /// One item in a bounded stream.
    StreamItem,
    /// Successful or failed stream completion.
    StreamEnd,
    /// Request or stream cancellation.
    Cancel,
    /// Receiver grants additional stream credits.
    WindowUpdate,
    /// Liveness probe or acknowledgement.
    Heartbeat,
    /// Structured transport/protocol error.
    Error,
}

/// Shared header carried by every protocol family.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FrameHeader {
    /// Protocol family, such as `runtime` or `tool`.
    pub family: String,
    /// Wire schema version.
    pub version: Version,
    /// Frame semantic kind.
    pub kind: FrameKind,
    /// Per-operation request ID.
    pub request_id: RequestId,
    /// Optional stream sequence local to the request.
    pub stream_sequence: Option<u64>,
    /// Cross-process correlation.
    pub correlation_id: CorrelationId,
    /// Direct cross-process cause.
    pub causation_id: CausationId,
    /// Retry/reconnect deduplication key.
    pub idempotency_id: IdempotencyId,
    /// Optional target cancellation token.
    pub cancellation_id: Option<CancellationId>,
}

/// Typed bounded wire frame.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WireFrame<T> {
    /// Common frame metadata.
    pub header: FrameHeader,
    /// Protocol-family payload.
    pub payload: T,
}

/// Initial negotiation request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Handshake {
    /// Versions accepted by the caller, highest preference first.
    pub supported_versions: Vec<Version>,
    /// Named optional capabilities.
    pub capabilities: BTreeSet<String>,
    /// Runtime-generated proof of local authorization.
    pub authorization_token: String,
}

/// Negotiated protocol parameters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Negotiated {
    /// Selected compatible version.
    pub version: Version,
    /// Capabilities supported by both peers.
    pub capabilities: BTreeSet<String>,
}

/// Negotiates a shared major version and capability intersection.
///
/// # Errors
///
/// Returns [`ProtocolError::IncompatibleVersion`] when no offered major version is
/// shared.
pub fn negotiate(
    local_versions: &[Version],
    local_capabilities: &BTreeSet<String>,
    remote: &Handshake,
) -> Result<Negotiated, ProtocolError> {
    let version = local_versions
        .iter()
        .copied()
        .find(|local| {
            remote
                .supported_versions
                .iter()
                .any(|candidate| local.is_compatible_with(*candidate))
        })
        .ok_or_else(|| ProtocolError::IncompatibleVersion {
            local: local_versions.to_vec(),
            remote: remote.supported_versions.clone(),
        })?;

    let capabilities = local_capabilities
        .intersection(&remote.capabilities)
        .cloned()
        .collect();
    Ok(Negotiated {
        version,
        capabilities,
    })
}

/// Structured protocol error safe to return to a peer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ErrorEnvelope {
    /// Stable machine-readable code.
    pub code: String,
    /// Redacted human-readable explanation.
    pub message: String,
    /// Whether repeating with the same idempotency key may succeed.
    pub retryable: bool,
}

/// Encodes one frame as a 4-byte big-endian length followed by CBOR.
///
/// # Errors
///
/// Returns [`ProtocolError`] when CBOR encoding fails or the body exceeds `maximum`.
pub fn encode_frame<T: Serialize>(
    frame: &WireFrame<T>,
    maximum: usize,
) -> Result<Vec<u8>, ProtocolError> {
    let mut body = Vec::new();
    ciborium::into_writer(frame, &mut body)
        .map_err(|error| ProtocolError::Encode(error.to_string()))?;
    if body.len() > maximum {
        return Err(ProtocolError::FrameTooLarge {
            actual: body.len(),
            maximum,
        });
    }
    let length = u32::try_from(body.len()).map_err(|_| ProtocolError::FrameTooLarge {
        actual: body.len(),
        maximum: u32::MAX as usize,
    })?;
    let mut encoded = Vec::with_capacity(8 + body.len());
    encoded.extend_from_slice(&MAGIC);
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

/// Decodes one complete bounded frame.
///
/// # Errors
///
/// Returns [`ProtocolError`] for a malformed header, mismatched or excessive length,
/// invalid magic, or invalid CBOR body.
pub fn decode_frame<T: DeserializeOwned>(
    encoded: &[u8],
    maximum: usize,
) -> Result<WireFrame<T>, ProtocolError> {
    if encoded.len() < 8 {
        return Err(ProtocolError::TruncatedHeader);
    }
    if encoded[..4] != MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let declared = u32::from_be_bytes(
        encoded[4..8]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedHeader)?,
    ) as usize;
    if declared > maximum {
        return Err(ProtocolError::FrameTooLarge {
            actual: declared,
            maximum,
        });
    }
    let actual = encoded.len() - 8;
    if actual != declared {
        return Err(ProtocolError::LengthMismatch { declared, actual });
    }
    ciborium::from_reader(&encoded[8..]).map_err(|error| ProtocolError::Decode(error.to_string()))
}

/// Reads exactly one bounded frame from an asynchronous transport.
///
/// The declared body length is checked before allocation.
///
/// # Errors
///
/// Returns [`ProtocolError`] for transport truncation/failure or malformed framing.
pub async fn read_frame<R, T>(reader: &mut R, maximum: usize) -> Result<WireFrame<T>, ProtocolError>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0_u8; 8];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| ProtocolError::Io(error.to_string()))?;
    if header[..4] != MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }
    let declared = u32::from_be_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| ProtocolError::TruncatedHeader)?,
    ) as usize;
    if declared > maximum {
        return Err(ProtocolError::FrameTooLarge {
            actual: declared,
            maximum,
        });
    }
    let mut body = vec![0_u8; declared];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|error| ProtocolError::Io(error.to_string()))?;
    let mut encoded = Vec::with_capacity(8 + declared);
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(&body);
    decode_frame(&encoded, maximum)
}

/// Writes and flushes exactly one bounded frame to an asynchronous transport.
///
/// # Errors
///
/// Returns [`ProtocolError`] when encoding, writing, or flushing fails.
pub async fn write_frame<W, T>(
    writer: &mut W,
    frame: &WireFrame<T>,
    maximum: usize,
) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let encoded = encode_frame(frame, maximum)?;
    writer
        .write_all(&encoded)
        .await
        .map_err(|error| ProtocolError::Io(error.to_string()))?;
    writer
        .flush()
        .await
        .map_err(|error| ProtocolError::Io(error.to_string()))
}

/// Framing and negotiation failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProtocolError {
    /// No compatible major protocol version exists.
    #[error("no compatible protocol version (local {local:?}, remote {remote:?})")]
    IncompatibleVersion {
        /// Versions offered by the receiver.
        local: Vec<Version>,
        /// Versions offered by the caller.
        remote: Vec<Version>,
    },
    /// Serialized body exceeded its configured bound.
    #[error("frame has {actual} bytes, maximum is {maximum}")]
    FrameTooLarge {
        /// Body bytes observed or declared.
        actual: usize,
        /// Configured bound.
        maximum: usize,
    },
    /// Fewer than eight header bytes were supplied.
    #[error("truncated frame header")]
    TruncatedHeader,
    /// Magic prefix is not an `AgentMod` frame.
    #[error("invalid frame magic")]
    InvalidMagic,
    /// Declared and actual lengths differ.
    #[error("frame length mismatch: declared {declared}, actual {actual}")]
    LengthMismatch {
        /// Header length.
        declared: usize,
        /// Available body bytes.
        actual: usize,
    },
    /// CBOR encoding failed.
    #[error("CBOR encoding failed: {0}")]
    Encode(String),
    /// CBOR decoding failed.
    #[error("CBOR decoding failed: {0}")]
    Decode(String),
    /// Asynchronous transport read/write failed.
    #[error("protocol transport I/O failed: {0}")]
    Io(String),
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use agentmod_primitives::{CausationId, CorrelationId, IdempotencyId, RequestId};
    use uuid::Uuid;

    use super::*;

    fn uuid(value: &str) -> Uuid {
        Uuid::from_str(value).expect("fixture UUID")
    }

    #[test]
    fn bounded_frame_round_trip() {
        let frame = WireFrame {
            header: FrameHeader {
                family: "runtime".into(),
                version: Version::new(1, 0),
                kind: FrameKind::Request,
                request_id: RequestId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000001")),
                stream_sequence: None,
                correlation_id: CorrelationId::from_uuid(uuid(
                    "018f6f83-7b80-7000-8000-000000000002",
                )),
                causation_id: CausationId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000003")),
                idempotency_id: IdempotencyId::from_uuid(uuid(
                    "018f6f83-7b80-7000-8000-000000000004",
                )),
                cancellation_id: None,
            },
            payload: String::from("health"),
        };
        let bytes = encode_frame(&frame, 1024).expect("encodes");
        assert_eq!(
            decode_frame::<String>(&bytes, 1024).expect("decodes"),
            frame
        );
    }

    #[test]
    fn negotiation_intersects_capabilities() {
        let local = BTreeSet::from(["streaming".into(), "branching".into()]);
        let remote = Handshake {
            supported_versions: vec![Version::new(1, 4)],
            capabilities: BTreeSet::from(["streaming".into(), "other".into()]),
            authorization_token: "fixture".into(),
        };
        assert_eq!(
            negotiate(&[Version::new(1, 1)], &local, &remote)
                .expect("compatible")
                .capabilities,
            BTreeSet::from(["streaming".into()])
        );
    }

    #[test]
    fn oversized_declared_frame_is_rejected_before_decode() {
        let mut bytes = Vec::from(MAGIC);
        bytes.extend_from_slice(&(1025_u32).to_be_bytes());
        assert_eq!(
            decode_frame::<String>(&bytes, 1024),
            Err(ProtocolError::FrameTooLarge {
                actual: 1025,
                maximum: 1024
            })
        );
    }

    #[tokio::test]
    async fn asynchronous_transport_round_trip_is_bounded() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let frame = WireFrame {
            header: FrameHeader {
                family: "runtime".into(),
                version: Version::new(1, 0),
                kind: FrameKind::Heartbeat,
                request_id: RequestId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000011")),
                stream_sequence: None,
                correlation_id: CorrelationId::from_uuid(uuid(
                    "018f6f83-7b80-7000-8000-000000000012",
                )),
                causation_id: CausationId::from_uuid(uuid("018f6f83-7b80-7000-8000-000000000013")),
                idempotency_id: IdempotencyId::from_uuid(uuid(
                    "018f6f83-7b80-7000-8000-000000000014",
                )),
                cancellation_id: None,
            },
            payload: String::from("ping"),
        };
        let expected = frame.clone();
        let writer = tokio::spawn(async move {
            write_frame(&mut client, &frame, 1024).await.expect("write");
        });
        assert_eq!(
            read_frame::<_, String>(&mut server, 1024)
                .await
                .expect("read"),
            expected
        );
        writer.await.expect("writer joins");
    }

    #[tokio::test]
    async fn asynchronous_reader_rejects_length_before_body_allocation() {
        let (mut client, mut server) = tokio::io::duplex(32);
        client.write_all(&MAGIC).await.expect("magic");
        client
            .write_all(&(2048_u32).to_be_bytes())
            .await
            .expect("length");
        assert_eq!(
            read_frame::<_, String>(&mut server, 1024).await,
            Err(ProtocolError::FrameTooLarge {
                actual: 2048,
                maximum: 1024
            })
        );
    }
}
