//! The live stream subsystem: provider output in flight, outside the log.
//!
//! A provider's token deltas are *not* canonical events. They pass through
//! here instead of the event pipeline:
//!
//! ```text
//! provider adapter (plugin) ── normalized frames ──► StreamHub
//!     ├── live, coalesced frames → attached clients (each with its own cursor
//!     │   and a byte-bounded queue; a client that falls behind gets
//!     │   `resync-required` and re-hydrates from a snapshot)
//!     ├── compact recovery ops → the host's recovery store (segments, then a
//!     │   compacted snapshot; never cumulative copies), dropped at finalization
//!     └── materialized partial state → snapshots for reconnecting clients
//! ```
//!
//! The provider plugin still publishes exactly one canonical `model-response`
//! (assembled text, tool calls, normalized usage, model identity) when it
//! finishes, so canonical history and replay are unchanged by streaming.
//!
//! Three kinds of state, kept apart on purpose:
//! * canonical semantic state — the session log (never written from here);
//! * recoverable partial state — [`Materialized`] streams and [`RecoveryOp`]s;
//! * ephemeral presentation state — [`LiveMsg`]s in client queues.
//!
//! Like the kernel this module is sans-I/O and reads no clock: hosts pass
//! `now` and execute the returned [`StreamOutput`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::session_of;

/// Tunables (configurable per deployment: `[runtime.streaming]`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSettings {
    /// Flush a coalesced delta run after this long (ms).
    #[serde(default = "d_flush_ms")]
    pub flush_ms: u64,
    /// …or once it holds this many bytes.
    #[serde(default = "d_flush_bytes")]
    pub flush_bytes: usize,
    /// Per-client live queue bound; past it the client must resync.
    #[serde(default = "d_client_bytes")]
    pub client_queue_bytes: usize,
    /// Materialized text kept per stream for snapshots (the canonical response
    /// is unaffected); beyond it the snapshot is marked truncated.
    #[serde(default = "d_stream_bytes")]
    pub max_stream_bytes: usize,
    /// Recovery segments written before they are compacted into one snapshot.
    #[serde(default = "d_compact")]
    pub compact_after_segments: u32,
}

fn d_flush_ms() -> u64 {
    33
}
fn d_flush_bytes() -> usize {
    512
}
fn d_client_bytes() -> usize {
    256 * 1024
}
fn d_stream_bytes() -> usize {
    4 * 1024 * 1024
}
fn d_compact() -> u32 {
    64
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            flush_ms: d_flush_ms(),
            flush_bytes: d_flush_bytes(),
            client_queue_bytes: d_client_bytes(),
            max_stream_bytes: d_stream_bytes(),
            compact_after_segments: d_compact(),
        }
    }
}

/// What a content block carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BlockKind {
    Text,
    Reasoning,
    ToolCall,
}

/// A normalized provider frame (provider specifics stay in the adapter).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Frame {
    /// Starts an attempt. Retries and provider/model fallback open a *new*
    /// attempt; the previous attempt's partial output is superseded, never
    /// concatenated.
    Open {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
        /// `retry`, `fallback`, … for attempts after the first.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    BlockStart {
        block: u32,
        kind: BlockKind,
        /// Tool name (tool-call blocks).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_id: Option<String>,
    },
    TextDelta {
        block: u32,
        text: String,
    },
    ReasoningDelta {
        block: u32,
        text: String,
    },
    /// Partial tool arguments: presentation only — nothing executes them.
    ToolArgsDelta {
        block: u32,
        text: String,
    },
    Metadata {
        data: Value,
    },
    /// Usage as the provider reported it (already normalized by the adapter).
    Usage {
        usage: Value,
    },
    BlockEnd {
        block: u32,
    },
    /// The serving model or provider changed within an attempt.
    Boundary {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider: Option<String>,
    },
    /// The attempt failed (possibly after HTTP 200). Terminal only when the
    /// adapter gives up; a retry opens a new attempt.
    Error {
        message: String,
        #[serde(default)]
        retryable: bool,
    },
    Cancelled {
        reason: String,
    },
    Complete {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish_reason: Option<String>,
    },
}

impl Frame {
    fn delta(&self) -> Option<(u32, BlockKind, &str)> {
        match self {
            Frame::TextDelta { block, text } => Some((*block, BlockKind::Text, text)),
            Frame::ReasoningDelta { block, text } => Some((*block, BlockKind::Reasoning, text)),
            Frame::ToolArgsDelta { block, text } => Some((*block, BlockKind::ToolCall, text)),
            _ => None,
        }
    }

    fn terminal(&self) -> Option<StreamStatus> {
        match self {
            Frame::Complete { .. } => Some(StreamStatus::Complete),
            Frame::Error {
                retryable: false, ..
            } => Some(StreamStatus::Failed),
            Frame::Cancelled { .. } => Some(StreamStatus::Cancelled),
            _ => None,
        }
    }
}

/// A frame as a provider plugin sends it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InFrame {
    /// Attempt identity, unique within the stream (`<attempt>.<n>`).
    pub attempt: String,
    /// Provider sequence, when the provider numbers its events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pseq: Option<u64>,
    #[serde(flatten)]
    pub frame: Frame,
}

/// Stream lifecycle (monotonic: once terminal, never changes again).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamStatus {
    Open,
    Complete,
    Failed,
    Cancelled,
    /// The runtime stopped while it was open (restored from recovery state);
    /// the invocation's retry opens a new attempt.
    Interrupted,
}

impl StreamStatus {
    fn is_terminal(self) -> bool {
        !matches!(self, StreamStatus::Open | StreamStatus::Interrupted)
    }
}

/// One content block's materialized state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockState {
    pub block: u32,
    pub kind: BlockKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    pub text: String,
    #[serde(default)]
    pub closed: bool,
}

/// Diagnostics counted per stream (reported in the canonical response summary).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamStats {
    /// Normalized frames accepted.
    pub frames: u64,
    /// Coalesced live frames produced.
    pub live_frames: u64,
    pub delta_bytes: u64,
    pub attempts: u32,
    /// Frames after a terminal state, or from a superseded attempt (ignored).
    pub late_frames: u64,
    /// Frames whose provider sequence was already seen (ignored).
    pub duplicate_frames: u64,
    /// Provider sequence numbers skipped.
    pub missing_frames: u64,
    /// Structurally invalid frames (delta for an unknown block, …; ignored).
    pub invalid_frames: u64,
    pub recovery_writes: u64,
    pub recovery_bytes: u64,
}

/// Recoverable partial state of one stream; also the hydration snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Materialized {
    pub session_id: String,
    pub stream_id: String,
    pub attempt: String,
    /// AgentMod-assigned sequence of the last frame this state includes.
    pub seq: u64,
    pub status: StreamStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub blocks: Vec<BlockState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
    #[serde(default)]
    pub stats: StreamStats,
}

impl Materialized {
    fn bytes(&self) -> usize {
        self.blocks.iter().map(|b| b.text.len()).sum()
    }
}

/// What travels to clients.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum LiveMsg {
    /// One normalized frame, or a coalesced run of deltas (`from_seq..=seq`).
    Frame {
        session_id: String,
        stream_id: String,
        attempt: String,
        seq: u64,
        from_seq: u64,
        frame: Frame,
    },
    /// Hydrate from this state; apply later frames with `seq` greater.
    Snapshot { state: Box<Materialized> },
    /// The stream ended and its invocation completed: render the canonical
    /// events from the log instead (`outcome`: complete, failed, cancelled, …).
    Finalized {
        session_id: String,
        stream_id: String,
        outcome: StreamStatus,
    },
    /// The client fell behind its queue bound: frames were dropped. Fetch a
    /// snapshot (`StreamHub::snapshots`) and continue from its sequence.
    ResyncRequired {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        dropped_bytes: usize,
    },
}

impl LiveMsg {
    /// Approximate wire size, for byte-bounded queues.
    #[must_use]
    pub fn size(&self) -> usize {
        match self {
            LiveMsg::Frame { frame, .. } => {
                96 + match frame {
                    Frame::TextDelta { text, .. }
                    | Frame::ReasoningDelta { text, .. }
                    | Frame::ToolArgsDelta { text, .. } => text.len(),
                    Frame::Usage { usage } => usage.to_string().len(),
                    Frame::Metadata { data } => data.to_string().len(),
                    Frame::Error { message, .. } => message.len(),
                    _ => 32,
                }
            }
            LiveMsg::Snapshot { state } => 128 + state.bytes(),
            _ => 96,
        }
    }

    fn session(&self) -> Option<&str> {
        match self {
            LiveMsg::Frame { session_id, .. } | LiveMsg::Finalized { session_id, .. } => {
                Some(session_id)
            }
            LiveMsg::Snapshot { state } => Some(&state.session_id),
            LiveMsg::ResyncRequired { session_id, .. } => session_id.as_deref(),
        }
    }
}

/// An operation on the host's recovery store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
pub enum RecoveryOp {
    /// Append one compact segment (a coalesced delta run or a structural frame).
    Append {
        stream_id: String,
        session_id: String,
        segment: Value,
    },
    /// Replace everything stored for the stream with this snapshot (compaction).
    Snapshot { state: Box<Materialized> },
    /// The stream is finalized: its canonical events are in the log.
    Discard { stream_id: String },
}

/// What the host must do after a hub call.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamOutput {
    pub recovery: Vec<RecoveryOp>,
    /// Clients with new messages to drain.
    pub ready: Vec<u64>,
}

#[derive(Debug, Clone)]
struct Pending {
    block: u32,
    kind: BlockKind,
    text: String,
    from_seq: u64,
    to_seq: u64,
    started: u64,
}

#[derive(Debug, Clone)]
struct Stream {
    state: Materialized,
    superseded: BTreeSet<String>,
    last_pseq: Option<u64>,
    pending: Option<Pending>,
    segments: u32,
    /// Blocks whose first delta already went out (leading-edge flush).
    led: BTreeSet<u32>,
}

#[derive(Debug, Clone, Default)]
struct Client {
    session: Option<String>,
    queue: VecDeque<LiveMsg>,
    bytes: usize,
    /// Dropping frames until the client re-hydrates.
    behind: bool,
}

/// Hub-wide counters (labels are kinds, never session or stream ids).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HubCounters {
    pub streams_opened: u64,
    pub frames: u64,
    pub live_frames: u64,
    pub deliveries: u64,
    pub delivered_bytes: u64,
    pub resyncs: u64,
    pub dropped_bytes: u64,
    pub recovery_writes: u64,
    pub recovery_bytes: u64,
    pub late_frames: u64,
    pub duplicate_frames: u64,
    pub invalid_frames: u64,
}

/// The stream controller and broker.
#[derive(Debug, Default)]
pub struct StreamHub {
    pub settings: StreamSettings,
    streams: BTreeMap<String, Stream>,
    clients: BTreeMap<u64, Client>,
    next_client: u64,
    pub counters: HubCounters,
}

/// Result of ingesting a provider batch (also the plugin's `stream` reply).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestResult {
    pub seq: u64,
    pub status: Option<StreamStatus>,
    pub stats: StreamStats,
}

impl StreamHub {
    #[must_use]
    pub fn new(settings: StreamSettings) -> Self {
        Self {
            settings,
            next_client: 1,
            ..Default::default()
        }
    }

    // ------------------------------------------------------------------
    // Ingestion.
    // ------------------------------------------------------------------

    /// Ingest normalized frames for `stream_id` (an invocation id; the session
    /// is derived from it). Invalid, duplicate, and late frames are counted and
    /// ignored; they never reach clients or recovery.
    pub fn ingest(
        &mut self,
        stream_id: &str,
        frames: Vec<InFrame>,
        now: u64,
        out: &mut StreamOutput,
    ) -> IngestResult {
        let session_id = session_of(stream_id).unwrap_or_default().to_owned();
        for f in frames {
            self.counters.frames += 1;
            self.ingest_one(&session_id, stream_id, f, now, out);
        }
        self.poll_stream(stream_id, now, out);
        let st = self.streams.get(stream_id);
        IngestResult {
            seq: st.map_or(0, |s| s.state.seq),
            status: st.map(|s| s.state.status),
            stats: st.map(|s| s.state.stats.clone()).unwrap_or_default(),
        }
    }

    fn ingest_one(
        &mut self,
        session_id: &str,
        stream_id: &str,
        f: InFrame,
        now: u64,
        out: &mut StreamOutput,
    ) {
        let opening = matches!(f.frame, Frame::Open { .. });
        if !self.streams.contains_key(stream_id) {
            if !opening {
                self.counters.invalid_frames += 1;
                return;
            }
            self.counters.streams_opened += 1;
            self.streams.insert(
                stream_id.to_owned(),
                Stream {
                    state: Materialized {
                        session_id: session_id.to_owned(),
                        stream_id: stream_id.to_owned(),
                        attempt: String::new(),
                        seq: 0,
                        status: StreamStatus::Open,
                        model: None,
                        provider: None,
                        blocks: Vec::new(),
                        usage: None,
                        error: None,
                        truncated: false,
                        stats: StreamStats::default(),
                    },
                    superseded: BTreeSet::new(),
                    last_pseq: None,
                    pending: None,
                    segments: 0,
                    led: BTreeSet::new(),
                },
            );
        }
        let s = self.streams.get_mut(stream_id).expect("stream");
        // Terminal states are monotonic: anything after them is late.
        if s.state.status.is_terminal() || s.superseded.contains(&f.attempt) {
            s.state.stats.late_frames += 1;
            self.counters.late_frames += 1;
            return;
        }
        if f.attempt != s.state.attempt {
            if !opening {
                s.state.stats.invalid_frames += 1;
                self.counters.invalid_frames += 1;
                return;
            }
            // A new attempt supersedes the previous one entirely.
            self.flush(stream_id, out);
            let s = self.streams.get_mut(stream_id).expect("stream");
            if !s.state.attempt.is_empty() {
                s.superseded.insert(s.state.attempt.clone());
            }
            s.state.attempt.clone_from(&f.attempt);
            s.state.blocks.clear();
            s.led.clear();
            s.state.usage = None;
            s.state.error = None;
            s.state.truncated = false;
            s.state.status = StreamStatus::Open;
            s.state.stats.attempts += 1;
            s.last_pseq = None;
        } else if opening {
            // Re-opening the current attempt: a duplicate.
            s.state.stats.duplicate_frames += 1;
            self.counters.duplicate_frames += 1;
            return;
        }
        let s = self.streams.get_mut(stream_id).expect("stream");
        if let Some(p) = f.pseq {
            if let Some(last) = s.last_pseq {
                if p <= last {
                    s.state.stats.duplicate_frames += 1;
                    self.counters.duplicate_frames += 1;
                    return;
                }
                s.state.stats.missing_frames += p - last - 1;
            }
            s.last_pseq = Some(p);
        }
        // Structural validation against the materialized blocks.
        let valid = match &f.frame {
            Frame::BlockStart { block, .. } => !s.state.blocks.iter().any(|b| b.block == *block),
            Frame::BlockEnd { block } => s
                .state
                .blocks
                .iter()
                .any(|b| b.block == *block && !b.closed),
            other => other.delta().is_none_or(|(block, kind, _)| {
                s.state
                    .blocks
                    .iter()
                    .any(|b| b.block == block && b.kind == kind && !b.closed)
            }),
        };
        if !valid {
            s.state.stats.invalid_frames += 1;
            self.counters.invalid_frames += 1;
            return;
        }
        s.state.seq += 1;
        s.state.stats.frames += 1;
        let seq = s.state.seq;
        if let Some((block, kind, text)) = f.frame.delta() {
            s.state.stats.delta_bytes += text.len() as u64;
            let room = self
                .settings
                .max_stream_bytes
                .saturating_sub(s.state.bytes());
            if let Some(b) = s.state.blocks.iter_mut().find(|b| b.block == block) {
                if text.len() <= room {
                    b.text.push_str(text);
                } else {
                    s.state.truncated = true;
                }
            }
            // Coalesce adjacent compatible deltas (same attempt and block).
            let compatible = s
                .pending
                .as_ref()
                .is_some_and(|p| p.block == block && p.kind == kind);
            if !compatible {
                self.flush(stream_id, out);
            }
            let s = self.streams.get_mut(stream_id).expect("stream");
            // The first delta of a block goes out at once (first-token
            // latency); the rest coalesce.
            let leading = s.led.insert(block);
            let p = s.pending.get_or_insert_with(|| Pending {
                block,
                kind,
                text: String::new(),
                from_seq: seq,
                to_seq: seq,
                started: now,
            });
            p.text.push_str(text);
            p.to_seq = seq;
            if leading || p.text.len() >= self.settings.flush_bytes {
                self.flush(stream_id, out);
            }
            return;
        }
        // Every non-delta frame is a boundary: flush, apply, forward.
        self.flush(stream_id, out);
        let s = self.streams.get_mut(stream_id).expect("stream");
        match &f.frame {
            Frame::Open {
                model, provider, ..
            } => {
                s.state.model.clone_from(model);
                s.state.provider.clone_from(provider);
            }
            Frame::Boundary { model, provider } => {
                if model.is_some() {
                    s.state.model.clone_from(model);
                }
                if provider.is_some() {
                    s.state.provider.clone_from(provider);
                }
            }
            Frame::BlockStart {
                block,
                kind,
                name,
                call_id,
            } => s.state.blocks.push(BlockState {
                block: *block,
                kind: *kind,
                name: name.clone(),
                call_id: call_id.clone(),
                text: String::new(),
                closed: false,
            }),
            Frame::BlockEnd { block } => {
                if let Some(b) = s.state.blocks.iter_mut().find(|b| b.block == *block) {
                    b.closed = true;
                }
            }
            Frame::Usage { usage } => s.state.usage = Some(usage.clone()),
            Frame::Error { message, .. } => s.state.error = Some(message.clone()),
            _ => {}
        }
        if let Some(t) = f.frame.terminal() {
            s.state.status = t;
        }
        let attempt = s.state.attempt.clone();
        self.emit(session_id, stream_id, &attempt, seq, seq, f.frame, out);
    }

    /// Emit one live frame: to recovery (as a compact segment) and to clients.
    #[allow(clippy::too_many_arguments)]
    fn emit(
        &mut self,
        session_id: &str,
        stream_id: &str,
        attempt: &str,
        from_seq: u64,
        seq: u64,
        frame: Frame,
        out: &mut StreamOutput,
    ) {
        let compact_after = self.settings.compact_after_segments;
        let s = self.streams.get_mut(stream_id).expect("stream");
        s.state.stats.live_frames += 1;
        self.counters.live_frames += 1;
        s.segments += 1;
        let op = if s.segments > compact_after {
            s.segments = 0;
            RecoveryOp::Snapshot {
                state: Box::new(s.state.clone()),
            }
        } else {
            RecoveryOp::Append {
                stream_id: stream_id.to_owned(),
                session_id: session_id.to_owned(),
                segment: serde_json::json!({ "attempt": attempt, "seq": seq, "frame": frame }),
            }
        };
        let bytes = serde_json::to_string(&op).map_or(0, |t| t.len()) as u64;
        s.state.stats.recovery_writes += 1;
        s.state.stats.recovery_bytes += bytes;
        self.counters.recovery_writes += 1;
        self.counters.recovery_bytes += bytes;
        out.recovery.push(op);
        let msg = LiveMsg::Frame {
            session_id: session_id.to_owned(),
            stream_id: stream_id.to_owned(),
            attempt: attempt.to_owned(),
            seq,
            from_seq,
            frame,
        };
        self.broadcast(&msg, out);
    }

    fn flush(&mut self, stream_id: &str, out: &mut StreamOutput) {
        let Some(s) = self.streams.get_mut(stream_id) else {
            return;
        };
        let Some(p) = s.pending.take() else {
            return;
        };
        let (session, attempt) = (s.state.session_id.clone(), s.state.attempt.clone());
        let frame = match p.kind {
            BlockKind::Text => Frame::TextDelta {
                block: p.block,
                text: p.text,
            },
            BlockKind::Reasoning => Frame::ReasoningDelta {
                block: p.block,
                text: p.text,
            },
            BlockKind::ToolCall => Frame::ToolArgsDelta {
                block: p.block,
                text: p.text,
            },
        };
        self.emit(
            &session, stream_id, &attempt, p.from_seq, p.to_seq, frame, out,
        );
    }

    fn poll_stream(&mut self, stream_id: &str, now: u64, out: &mut StreamOutput) {
        let due = self
            .streams
            .get(stream_id)
            .and_then(|s| s.pending.as_ref())
            .is_some_and(|p| now.saturating_sub(p.started) >= self.settings.flush_ms);
        if due {
            self.flush(stream_id, out);
        }
    }

    /// Flush coalesced runs that have waited `flush_ms` (call at the deadline).
    pub fn poll(&mut self, now: u64, out: &mut StreamOutput) {
        let ids: Vec<String> = self.streams.keys().cloned().collect();
        for id in ids {
            self.poll_stream(&id, now, out);
        }
    }

    /// When the host should call [`StreamHub::poll`] next.
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        self.streams
            .values()
            .filter_map(|s| s.pending.as_ref())
            .map(|p| p.started + self.settings.flush_ms)
            .min()
    }

    /// The stream's invocation completed (or was cancelled, or its plugin
    /// died): close it monotonically, tell clients to render the canonical
    /// events, and drop its recovery state.
    pub fn finalize(
        &mut self,
        stream_id: &str,
        outcome: StreamStatus,
        out: &mut StreamOutput,
    ) -> Option<Materialized> {
        self.flush(stream_id, out);
        let mut s = self.streams.remove(stream_id)?;
        if !s.state.status.is_terminal() {
            s.state.status = outcome;
        }
        out.recovery.push(RecoveryOp::Discard {
            stream_id: stream_id.to_owned(),
        });
        let msg = LiveMsg::Finalized {
            session_id: s.state.session_id.clone(),
            stream_id: stream_id.to_owned(),
            outcome: s.state.status,
        };
        self.broadcast(&msg, out);
        Some(s.state)
    }

    /// An invocation was retried (its plugin restarted): the open attempt is
    /// interrupted; the retry will open a new one.
    pub fn interrupt(&mut self, stream_id: &str, out: &mut StreamOutput) {
        self.flush(stream_id, out);
        if let Some(s) = self.streams.get_mut(stream_id)
            && !s.state.status.is_terminal()
        {
            s.state.status = StreamStatus::Interrupted;
            if !s.state.attempt.is_empty() {
                s.superseded.insert(s.state.attempt.clone());
            }
        }
    }

    /// Restore partial state from the recovery store after a runtime restart.
    pub fn restore(&mut self, mut state: Materialized) {
        if !state.status.is_terminal() {
            state.status = StreamStatus::Interrupted;
        }
        let mut superseded = BTreeSet::new();
        if !state.attempt.is_empty() {
            superseded.insert(state.attempt.clone());
        }
        self.streams.insert(
            state.stream_id.clone(),
            Stream {
                state,
                superseded,
                last_pseq: None,
                pending: None,
                segments: 0,
                led: BTreeSet::new(),
            },
        );
    }

    /// Fold recovery records (as written) back into a materialized state.
    #[must_use]
    pub fn replay_recovery(ops: &[RecoveryOp], settings: &StreamSettings) -> Option<Materialized> {
        let mut hub = StreamHub::new(settings.clone());
        let mut sink = StreamOutput::default();
        let mut id = None;
        for op in ops {
            match op {
                RecoveryOp::Snapshot { state } => {
                    id = Some(state.stream_id.clone());
                    let mut st = (**state).clone();
                    st.status = StreamStatus::Open;
                    hub.streams.insert(
                        st.stream_id.clone(),
                        Stream {
                            state: st,
                            superseded: BTreeSet::new(),
                            last_pseq: None,
                            pending: None,
                            segments: 0,
                            led: BTreeSet::new(),
                        },
                    );
                }
                RecoveryOp::Append {
                    stream_id, segment, ..
                } => {
                    id = Some(stream_id.clone());
                    let Ok(frame) = serde_json::from_value::<Frame>(segment["frame"].clone())
                    else {
                        continue;
                    };
                    let attempt = segment["attempt"].as_str().unwrap_or_default().to_owned();
                    let seq = segment["seq"].as_u64().unwrap_or(0);
                    hub.ingest(
                        stream_id,
                        vec![InFrame {
                            attempt,
                            pseq: None,
                            frame,
                        }],
                        0,
                        &mut sink,
                    );
                    if let Some(s) = hub.streams.get_mut(stream_id) {
                        s.state.seq = s.state.seq.max(seq);
                    }
                    hub.flush(stream_id, &mut sink);
                }
                RecoveryOp::Discard { .. } => return None,
            }
        }
        id.and_then(|id| hub.streams.remove(&id)).map(|s| s.state)
    }

    // ------------------------------------------------------------------
    // Broker.
    // ------------------------------------------------------------------

    fn broadcast(&mut self, msg: &LiveMsg, out: &mut StreamOutput) {
        let size = msg.size();
        let cap = self.settings.client_queue_bytes;
        for (id, c) in &mut self.clients {
            if c.session
                .as_deref()
                .is_some_and(|s| Some(s) != msg.session())
            {
                continue;
            }
            if c.behind {
                self.counters.dropped_bytes += size as u64;
                continue;
            }
            if c.bytes + size > cap {
                // Too far behind: drop what is queued and ask for a resync.
                let dropped = c.bytes + size;
                c.queue.clear();
                c.bytes = 0;
                c.behind = true;
                self.counters.resyncs += 1;
                self.counters.dropped_bytes += dropped as u64;
                let r = LiveMsg::ResyncRequired {
                    session_id: c.session.clone(),
                    dropped_bytes: dropped,
                };
                c.bytes += r.size();
                c.queue.push_back(r);
            } else {
                c.bytes += size;
                c.queue.push_back(msg.clone());
            }
            if !out.ready.contains(id) {
                out.ready.push(*id);
            }
        }
    }

    /// Attach a client (optionally to one session). Returns its id and the
    /// current snapshots in one step, so no frame can fall between hydration
    /// and subscription: every later message has a greater `seq`.
    pub fn attach(&mut self, session: Option<&str>) -> (u64, Vec<Materialized>) {
        let id = self.next_client.max(1);
        self.next_client = id + 1;
        self.clients.insert(
            id,
            Client {
                session: session.map(str::to_owned),
                ..Default::default()
            },
        );
        (id, self.snapshots(session))
    }

    pub fn detach(&mut self, client: u64) {
        // Detaching never cancels generation: execution owns the stream.
        self.clients.remove(&client);
    }

    /// Current partial state of every open stream (optionally one session's).
    #[must_use]
    pub fn snapshots(&self, session: Option<&str>) -> Vec<Materialized> {
        self.streams
            .values()
            .filter(|s| session.is_none_or(|x| s.state.session_id == x))
            .map(|s| s.state.clone())
            .collect()
    }

    /// The client re-hydrated after `resync-required`: deliver again. Returns
    /// the snapshots to hydrate from (atomic with resuming delivery).
    pub fn resync(&mut self, client: u64) -> Vec<Materialized> {
        let session = match self.clients.get_mut(&client) {
            Some(c) => {
                c.behind = false;
                c.queue.clear();
                c.bytes = 0;
                c.session.clone()
            }
            None => return Vec::new(),
        };
        self.snapshots(session.as_deref())
    }

    /// Take up to `max_bytes` of a client's queued messages (at least one).
    pub fn drain(&mut self, client: u64, max_bytes: usize) -> Vec<LiveMsg> {
        let Some(c) = self.clients.get_mut(&client) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        let mut taken = 0;
        while let Some(m) = c.queue.front() {
            let n = m.size();
            if !out.is_empty() && taken + n > max_bytes {
                break;
            }
            taken += n;
            c.bytes = c.bytes.saturating_sub(n);
            out.push(c.queue.pop_front().expect("front"));
        }
        self.counters.deliveries += out.len() as u64;
        self.counters.delivered_bytes += taken as u64;
        out
    }

    /// Bytes queued for a client.
    #[must_use]
    pub fn queued(&self, client: u64) -> usize {
        self.clients.get(&client).map_or(0, |c| c.bytes)
    }

    #[must_use]
    pub fn clients(&self) -> Vec<u64> {
        self.clients.keys().copied().collect()
    }

    #[must_use]
    pub fn is_open(&self, stream_id: &str) -> bool {
        self.streams.contains_key(stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn f(attempt: &str, frame: Frame) -> InFrame {
        InFrame {
            attempt: attempt.into(),
            pseq: None,
            frame,
        }
    }
    fn open(a: &str) -> InFrame {
        f(
            a,
            Frame::Open {
                model: Some("m".into()),
                provider: Some("p".into()),
                reason: None,
            },
        )
    }
    fn start(a: &str, block: u32, kind: BlockKind) -> InFrame {
        f(
            a,
            Frame::BlockStart {
                block,
                kind,
                name: None,
                call_id: None,
            },
        )
    }
    fn text(a: &str, block: u32, t: &str) -> InFrame {
        f(
            a,
            Frame::TextDelta {
                block,
                text: t.into(),
            },
        )
    }
    fn hub() -> StreamHub {
        StreamHub::new(StreamSettings::default())
    }
    fn frames(msgs: &[LiveMsg]) -> Vec<&Frame> {
        msgs.iter()
            .filter_map(|m| match m {
                LiveMsg::Frame { frame, .. } => Some(frame),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn deltas_coalesce_by_bytes_and_time_but_never_across_blocks() {
        let mut h = hub();
        let (c, snap) = h.attach(None);
        assert!(snap.is_empty());
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![open("a"), start("a", 0, BlockKind::Reasoning)],
            0,
            &mut out,
        );
        // 100 small reasoning deltas within the window, then a text block.
        let mut batch: Vec<InFrame> = (0..100)
            .map(|_| {
                f(
                    "a",
                    Frame::ReasoningDelta {
                        block: 0,
                        text: "r".into(),
                    },
                )
            })
            .collect();
        batch.push(start("a", 1, BlockKind::Text));
        batch.extend((0..10).map(|_| text("a", 1, "hello ")));
        h.ingest("s1/i2", batch, 5, &mut out);
        // Each block's first delta goes out at once (first-token latency);
        // the rest of the run coalesces, and the text run is still pending.
        let got = h.drain(c, usize::MAX);
        let kinds: Vec<&Frame> = frames(&got);
        assert!(matches!(kinds[0], Frame::Open { .. }));
        assert!(matches!(kinds[2], Frame::ReasoningDelta { text, .. } if text.len() == 1));
        assert!(matches!(kinds[3], Frame::ReasoningDelta { text, .. } if text.len() == 99));
        assert!(matches!(kinds[4], Frame::BlockStart { block: 1, .. }));
        assert!(matches!(kinds[5], Frame::TextDelta { text, .. } if text == "hello "));
        assert_eq!(kinds.len(), 6);
        // Time flushes the pending run.
        assert_eq!(h.next_deadline(), Some(5 + 33));
        h.poll(40, &mut out);
        let got = h.drain(c, usize::MAX);
        assert!(
            matches!(&got[0], LiveMsg::Frame { frame: Frame::TextDelta { text, .. }, seq, from_seq, .. } if text.len() == 54 && *seq - *from_seq == 8)
        );
        // Size flushes too.
        h.ingest("s1/i2", vec![text("a", 1, &"x".repeat(600))], 41, &mut out);
        assert_eq!(h.drain(c, usize::MAX).len(), 1);
        // 113 normalized frames became 8 live frames.
        let st = &h.snapshots(None)[0];
        assert_eq!(st.stats.frames, 114);
        assert_eq!(st.stats.live_frames, 8);
        assert_eq!(st.blocks[1].text.len(), 660);
    }

    #[test]
    fn retry_opens_a_new_attempt_and_never_concatenates() {
        let mut h = hub();
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![
                open("1.1"),
                start("1.1", 0, BlockKind::Text),
                text("1.1", 0, "partial ans"),
            ],
            0,
            &mut out,
        );
        h.ingest(
            "s1/i2",
            vec![f(
                "1.1",
                Frame::Error {
                    message: "upstream reset".into(),
                    retryable: true,
                },
            )],
            1,
            &mut out,
        );
        assert_eq!(h.snapshots(None)[0].status, StreamStatus::Open);
        h.ingest(
            "s1/i2",
            vec![
                InFrame {
                    attempt: "1.2".into(),
                    pseq: None,
                    frame: Frame::Open {
                        model: Some("m2".into()),
                        provider: None,
                        reason: Some("retry".into()),
                    },
                },
                start("1.2", 0, BlockKind::Text),
                text("1.2", 0, "full answer"),
            ],
            2,
            &mut out,
        );
        // A late frame from the superseded attempt is ignored and counted.
        let r = h.ingest("s1/i2", vec![text("1.1", 0, " LATE")], 3, &mut out);
        let st = &h.snapshots(None)[0];
        assert_eq!(st.blocks[0].text, "full answer");
        assert_eq!(st.attempt, "1.2");
        assert_eq!(st.model.as_deref(), Some("m2"));
        assert_eq!(r.stats.attempts, 2);
        assert_eq!(r.stats.late_frames, 1);
    }

    #[test]
    fn terminal_states_are_monotonic_and_late_frames_are_observed() {
        let mut h = hub();
        let (c, _) = h.attach(Some("s1"));
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![
                open("a"),
                start("a", 0, BlockKind::Text),
                text("a", 0, "done"),
            ],
            0,
            &mut out,
        );
        let r = h.ingest(
            "s1/i2",
            vec![
                f(
                    "a",
                    Frame::Complete {
                        finish_reason: Some("stop".into()),
                    },
                ),
                f(
                    "a",
                    Frame::Cancelled {
                        reason: "race".into(),
                    },
                ),
                text("a", 0, "after"),
            ],
            1,
            &mut out,
        );
        assert_eq!(r.status, Some(StreamStatus::Complete));
        assert_eq!(r.stats.late_frames, 2);
        // Finalization keeps the first terminal state and clears recovery.
        out.recovery.clear();
        let fin = h
            .finalize("s1/i2", StreamStatus::Cancelled, &mut out)
            .unwrap();
        assert_eq!(fin.status, StreamStatus::Complete);
        assert!(matches!(out.recovery[0], RecoveryOp::Discard { .. }));
        let msgs = h.drain(c, usize::MAX);
        assert!(matches!(
            msgs.last(),
            Some(LiveMsg::Finalized {
                outcome: StreamStatus::Complete,
                ..
            })
        ));
        assert!(h.snapshots(None).is_empty());
        // Frames for a finalized stream re-open nothing.
        h.ingest("s1/i2", vec![text("a", 0, "zombie")], 2, &mut out);
        assert!(h.snapshots(None).is_empty());
    }

    #[test]
    fn duplicate_missing_and_invalid_frames_are_counted_not_applied() {
        let mut h = hub();
        let mut out = StreamOutput::default();
        let p = |n: u64, fr: InFrame| InFrame {
            pseq: Some(n),
            ..fr
        };
        let r = h.ingest(
            "s1/i2",
            vec![
                p(1, open("a")),
                p(2, start("a", 0, BlockKind::Text)),
                p(3, text("a", 0, "ab")),
                p(3, text("a", 0, "ab")), // duplicate by provider seq (not by text)
                p(4, text("a", 0, "ab")), // same text, new seq: applied
                p(7, text("a", 0, "c")),  // 5 and 6 missing
                p(8, text("a", 9, "?")),  // delta for an unknown block
                p(
                    9,
                    f(
                        "a",
                        Frame::ToolArgsDelta {
                            block: 0,
                            text: "{".into(),
                        },
                    ),
                ), // wrong kind
            ],
            0,
            &mut out,
        );
        h.poll(100, &mut out);
        let st = &h.snapshots(None)[0];
        assert_eq!(st.blocks[0].text, "ababc");
        assert_eq!(r.stats.duplicate_frames, 1);
        assert_eq!(r.stats.missing_frames, 2);
        assert_eq!(r.stats.invalid_frames, 2);
        // A frame before any open is invalid.
        h.ingest("s1/i9", vec![text("a", 0, "x")], 0, &mut out);
        assert!(!h.is_open("s1/i9"));
    }

    #[test]
    fn tool_arguments_accumulate_per_block_as_presentation_only() {
        let mut h = hub();
        let mut out = StreamOutput::default();
        let tool = |b: u32, name: &str| {
            f(
                "a",
                Frame::BlockStart {
                    block: b,
                    kind: BlockKind::ToolCall,
                    name: Some(name.into()),
                    call_id: Some(format!("c{b}")),
                },
            )
        };
        let args = |b: u32, t: &str| {
            f(
                "a",
                Frame::ToolArgsDelta {
                    block: b,
                    text: t.into(),
                },
            )
        };
        h.ingest(
            "s1/i2",
            vec![
                open("a"),
                tool(0, "read_file"),
                args(0, "{\"pa"),
                tool(1, "shell"),
                args(1, "{\"command\":"),
                args(0, "th\":\"a\"}"),
                f("a", Frame::BlockEnd { block: 0 }),
                args(1, "\"make\"}"),
            ],
            0,
            &mut out,
        );
        let st = &h.snapshots(None)[0];
        assert_eq!(st.blocks[0].text, "{\"path\":\"a\"}");
        assert!(st.blocks[0].closed);
        assert_eq!(st.blocks[1].text, "{\"command\":\"make\"}");
        assert!(!st.blocks[1].closed);
        // Deltas after the block ended are invalid.
        let r = h.ingest("s1/i2", vec![args(0, "x")], 1, &mut out);
        assert_eq!(r.stats.invalid_frames, 1);
    }

    #[test]
    fn a_slow_client_is_bounded_and_resyncs_from_a_snapshot() {
        let mut h = StreamHub::new(StreamSettings {
            client_queue_bytes: 2_000,
            flush_bytes: 100,
            ..Default::default()
        });
        let (fast, _) = h.attach(None);
        let (slow, _) = h.attach(None);
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![open("a"), start("a", 0, BlockKind::Text)],
            0,
            &mut out,
        );
        for i in 0..100 {
            h.ingest("s1/i2", vec![text("a", 0, &"y".repeat(100))], i, &mut out);
            h.drain(fast, usize::MAX); // the fast client keeps up
        }
        // The slow client never drained: its queue stayed bounded.
        assert!(h.queued(slow) <= 2_000);
        let msgs = h.drain(slow, usize::MAX);
        assert!(matches!(msgs.last(), Some(LiveMsg::ResyncRequired { .. })));
        assert!(h.counters.resyncs >= 1);
        // While behind, nothing else is queued for it.
        h.ingest("s1/i2", vec![text("a", 0, "z")], 200, &mut out);
        h.poll(1_000, &mut out);
        assert!(h.drain(slow, usize::MAX).is_empty());
        // Resync: the snapshot carries all text so far and the cursor.
        let snaps = h.resync(slow);
        assert_eq!(snaps[0].blocks[0].text.len(), 100 * 100 + 1);
        let cursor = snaps[0].seq;
        h.ingest("s1/i2", vec![text("a", 0, "!")], 2_000, &mut out);
        h.poll(3_000, &mut out);
        let more = h.drain(slow, usize::MAX);
        assert!(matches!(&more[0], LiveMsg::Frame { from_seq, .. } if *from_seq == cursor + 1));
    }

    #[test]
    fn a_second_client_joins_mid_stream_and_a_disconnect_cancels_nothing() {
        let mut h = hub();
        let (a, _) = h.attach(Some("s1"));
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![
                open("a"),
                start("a", 0, BlockKind::Text),
                text("a", 0, "first "),
            ],
            0,
            &mut out,
        );
        h.poll(100, &mut out);
        // Attach is atomic with the snapshot: the cursor is the snapshot's seq.
        let (b, snap) = h.attach(Some("s1"));
        assert_eq!(snap[0].blocks[0].text, "first ");
        let cursor = snap[0].seq;
        h.detach(a);
        h.ingest("s1/i2", vec![text("a", 0, "second")], 101, &mut out);
        h.poll(200, &mut out);
        let got = h.drain(b, usize::MAX);
        assert!(
            matches!(&got[0], LiveMsg::Frame { from_seq, frame: Frame::TextDelta { text, .. }, .. } if *from_seq == cursor + 1 && text == "second")
        );
        assert_eq!(h.snapshots(Some("s1"))[0].status, StreamStatus::Open);
        // Another session's client sees nothing.
        let (other, snap) = h.attach(Some("s2"));
        assert!(snap.is_empty());
        h.ingest("s1/i2", vec![text("a", 0, "!")], 300, &mut out);
        h.poll(400, &mut out);
        assert!(h.drain(other, usize::MAX).is_empty());
    }

    #[test]
    fn recovery_is_segments_then_compacted_snapshots_never_cumulative_copies() {
        let settings = StreamSettings {
            flush_bytes: 1,
            compact_after_segments: 8,
            ..Default::default()
        };
        let mut h = StreamHub::new(settings.clone());
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![open("a"), start("a", 0, BlockKind::Text)],
            0,
            &mut out,
        );
        let word = "Hello";
        for ch in word.chars().cycle().take(40) {
            h.ingest("s1/i2", vec![text("a", 0, &ch.to_string())], 0, &mut out);
        }
        // Each segment carries only its own delta; snapshots appear periodically.
        let mut bytes_of_text_in_appends = 0;
        let mut snapshots = 0;
        for op in &out.recovery {
            match op {
                RecoveryOp::Append { segment, .. } => {
                    if let Some(t) = segment["frame"]["text"].as_str() {
                        assert_eq!(t.len(), 1, "a segment holds one delta, not the text so far");
                        bytes_of_text_in_appends += t.len();
                    }
                }
                RecoveryOp::Snapshot { .. } => snapshots += 1,
                RecoveryOp::Discard { .. } => {}
            }
        }
        assert!(snapshots >= 4);
        assert!(bytes_of_text_in_appends < 40);
        // Replaying what was written gives back the exact partial state.
        let restored = StreamHub::replay_recovery(&out.recovery, &settings).unwrap();
        let live = &h.snapshots(None)[0];
        assert_eq!(restored.blocks[0].text, live.blocks[0].text);
        assert_eq!(restored.seq, live.seq);
        // After a crash the restored stream is interrupted, and the retry's
        // new attempt supersedes it.
        let mut h2 = StreamHub::new(settings);
        h2.restore(restored);
        assert_eq!(h2.snapshots(None)[0].status, StreamStatus::Interrupted);
        let mut out2 = StreamOutput::default();
        h2.ingest(
            "s1/i2",
            vec![
                open("b"),
                start("b", 0, BlockKind::Text),
                text("b", 0, "new"),
            ],
            0,
            &mut out2,
        );
        h2.poll(100, &mut out2);
        assert_eq!(h2.snapshots(None)[0].blocks[0].text, "new");
        assert_eq!(h2.snapshots(None)[0].status, StreamStatus::Open);
    }

    #[test]
    fn arbitrary_batch_splits_produce_identical_state() {
        // The same frames ingested in different batch shapes and timings give
        // the same materialized text and block structure.
        let all: Vec<InFrame> = {
            let mut v = vec![open("a"), start("a", 0, BlockKind::Text)];
            for w in "the quick brown fox jumps over the lazy dog ".split_inclusive(' ') {
                v.push(text("a", 0, w));
            }
            v.push(f(
                "a",
                Frame::Usage {
                    usage: json!({ "input_tokens": 3 }),
                },
            ));
            v.push(f(
                "a",
                Frame::Complete {
                    finish_reason: None,
                },
            ));
            v
        };
        let reference = {
            let mut h = hub();
            let mut o = StreamOutput::default();
            h.ingest("s1/i2", all.clone(), 0, &mut o);
            h.snapshots(None)[0].clone()
        };
        for split in 1..all.len() {
            let mut h = hub();
            let mut o = StreamOutput::default();
            for (i, chunk) in all.chunks(split).enumerate() {
                h.ingest("s1/i2", chunk.to_vec(), (i as u64) * 7, &mut o);
            }
            let st = &h.snapshots(None)[0];
            assert_eq!(st.blocks, reference.blocks, "split {split}");
            assert_eq!(st.seq, reference.seq);
            assert_eq!(st.usage, reference.usage);
            assert_eq!(st.status, StreamStatus::Complete);
        }
    }

    #[test]
    fn large_responses_are_bounded_in_materialized_state() {
        let mut h = StreamHub::new(StreamSettings {
            max_stream_bytes: 10_000,
            ..Default::default()
        });
        let mut out = StreamOutput::default();
        h.ingest(
            "s1/i2",
            vec![open("a"), start("a", 0, BlockKind::Text)],
            0,
            &mut out,
        );
        for i in 0..1000 {
            h.ingest("s1/i2", vec![text("a", 0, &"z".repeat(100))], i, &mut out);
        }
        let st = &h.snapshots(None)[0];
        assert!(st.blocks[0].text.len() <= 10_000);
        assert!(st.truncated);
        assert_eq!(st.stats.delta_bytes, 100_000);
    }
}
