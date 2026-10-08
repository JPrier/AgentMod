// Client-side live stream state (presentation only; never history).
//
// Ingestion and rendering are separate. Messages are applied as they arrive —
// also in a background tab, where requestAnimationFrame does not run — into a
// materialized model per stream; the page renders that model at most once per
// animation frame, and a tab that becomes visible renders the *latest* state
// rather than replaying every delta it missed.
//
// Hydration is race-free: the client is subscribed before it asks for a
// snapshot; messages arriving meanwhile are buffered, and after the snapshot
// only those with a greater sequence apply. A gap in sequence (or a
// `resync-required` from the server) triggers a fresh snapshot.

export class StreamStore {
  /**
   * @param {object} [o]
   * @param {() => void} [o.onChange]   something changed (schedule a render)
   * @param {() => void} [o.onResync]   the store needs a fresh snapshot
   */
  constructor({ onChange = () => {}, onResync = () => {} } = {}) {
    this.streams = new Map();
    this.onChange = onChange;
    this.onResync = onResync;
    this.hydrating = false;
    this.buffer = [];
    this.stats = { messages: 0, frames: 0, duplicates: 0, gaps: 0, late: 0, resyncs: 0, renders: 0, hydrations: 0 };
  }

  /** Start buffering: a snapshot request is in flight. */
  beginHydrate() {
    this.hydrating = true;
    this.buffer = [];
  }

  /** Install snapshots (Materialized states), then apply buffered newer messages. */
  hydrate(snapshots = []) {
    this.stats.hydrations++;
    for (const st of snapshots) {
      this.streams.set(st.stream_id, {
        session_id: st.session_id,
        stream_id: st.stream_id,
        attempt: st.attempt,
        seq: st.seq,
        status: st.status,
        model: st.model,
        blocks: new Map((st.blocks || []).map((b) => [b.block, { ...b }])),
        usage: st.usage,
        error: st.error,
        finalized: false,
      });
    }
    const pending = this.buffer;
    this.hydrating = false;
    this.buffer = [];
    for (const m of pending) this.apply(m);
    this.onChange();
  }

  applyAll(messages) {
    for (const m of messages) this.apply(m);
    this.onChange();
  }

  apply(m) {
    this.stats.messages++;
    if (this.hydrating) {
      this.buffer.push(m);
      return;
    }
    if (m.type === 'resync-required') {
      this.stats.resyncs++;
      this.onResync();
      return;
    }
    if (m.type === 'snapshot') {
      this.hydrate([m.state]);
      return;
    }
    if (m.type === 'finalized') {
      const s = this.streams.get(m.stream_id);
      // Unknown (already pruned after its canonical response arrived): nothing to do.
      if (s) {
        s.status = m.outcome;
        s.finalized = true;
      }
      return;
    }
    if (m.type !== 'frame') return;
    const f = m.frame;
    let s = this.streams.get(m.stream_id);
    if (!s || (m.attempt !== s.attempt && f.type === 'open')) {
      if (s?.finalized) return;
      if (!s && f.type !== 'open' && m.from_seq !== 1) {
        // Joined mid-stream without a snapshot: get one.
        this.stats.gaps++;
        this.onResync();
        return;
      }
      s = { session_id: m.session_id, stream_id: m.stream_id, attempt: m.attempt, seq: s?.seq ?? 0, status: 'open', blocks: new Map(), finalized: false };
      this.streams.set(m.stream_id, s);
    }
    if (s.finalized || m.attempt !== s.attempt) {
      this.stats.late++;
      return;
    }
    if (m.seq <= s.seq) {
      this.stats.duplicates++;
      return;
    }
    if (m.from_seq > s.seq + 1 && f.type !== 'open') {
      this.stats.gaps++;
      this.onResync();
      return;
    }
    s.seq = m.seq;
    this.stats.frames++;
    switch (f.type) {
      case 'open':
        s.model = f.model;
        s.blocks = new Map();
        s.status = 'open';
        break;
      case 'block-start':
        s.blocks.set(f.block, { block: f.block, kind: f.kind, name: f.name, call_id: f.call_id, text: '', closed: false });
        break;
      case 'text-delta':
      case 'reasoning-delta':
      case 'tool-args-delta': {
        const b = s.blocks.get(f.block);
        if (b) b.text += f.text;
        break;
      }
      case 'block-end': {
        const b = s.blocks.get(f.block);
        if (b) b.closed = true;
        break;
      }
      case 'usage':
        s.usage = f.usage;
        break;
      case 'boundary':
        if (f.model) s.model = f.model;
        break;
      case 'error':
        s.error = f.message;
        if (!f.retryable) s.status = 'failed';
        break;
      case 'cancelled':
        s.status = 'cancelled';
        break;
      case 'complete':
        s.status = 'complete';
        break;
      default:
    }
  }

  /** Streams of one session still worth showing (not yet replaced by canonical events). */
  live(sessionId, { canonical = new Set() } = {}) {
    // Shown until the canonical events that replace it are in the view (no flicker).
    return [...this.streams.values()].filter((s) => s.session_id === sessionId && !canonical.has(s.stream_id));
  }

  /** Forget streams whose canonical response is now in the log. */
  prune(canonical) {
    for (const id of canonical) this.streams.delete(id);
  }
}

/**
 * Render scheduling decoupled from ingestion: at most one render per animation
 * frame while visible; none while hidden (ingestion continues); one render of
 * the latest state when the page becomes visible again.
 */
export function renderScheduler(render, { win = globalThis } = {}) {
  let queued = false;
  const doc = win.document;
  const visible = () => !doc || doc.visibilityState !== 'hidden';
  const raf = win.requestAnimationFrame ? (cb) => win.requestAnimationFrame(cb) : (cb) => setTimeout(cb, 16);
  const run = () => {
    queued = false;
    render();
  };
  doc?.addEventListener?.('visibilitychange', () => {
    if (visible()) {
      queued = false;
      render();
    }
  });
  return () => {
    if (queued || !visible()) return;
    queued = true;
    raf(run);
  };
}
