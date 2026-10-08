// Incremental Server-Sent Events parser (the provider-facing byte parser).
//
// Bytes arrive in arbitrary splits: a UTF-8 sequence, a line, or an event may
// straddle chunks; lines may end in LF, CRLF, or CR (even split between the CR
// and the LF). Events are separated by a blank line; several `data:` lines join
// with "\n"; `:` lines are comments (keep-alives such as OpenRouter's
// ": OPENROUTER PROCESSING"). The buffer is bounded: an event larger than
// `maxEventBytes` is an error rather than unbounded memory growth.

export class SseError extends Error {}

export class SseParser {
  /**
   * @param {object} [o]
   * @param {number} [o.maxEventBytes]  bound on one event (default 4 MiB)
   */
  constructor({ maxEventBytes = 4 * 1024 * 1024 } = {}) {
    this.decoder = new TextDecoder('utf-8');
    this.buf = '';
    this.pendingCR = false;
    this.data = [];
    this.event = '';
    this.id = null;
    this.size = 0;
    this.maxEventBytes = maxEventBytes;
    /** Raw byte chunks seen (transport events). */
    this.chunks = 0;
  }

  /** Feed bytes (Uint8Array) or text; returns the complete events parsed. */
  push(chunk) {
    this.chunks++;
    const text = typeof chunk === 'string' ? chunk : this.decoder.decode(chunk, { stream: true });
    return this.feed(text);
  }

  /** End of stream: flush a trailing partial event (a malformed final frame). */
  end() {
    const out = this.feed(this.decoder.decode());
    if (this.buf.length) {
      this.line(this.buf, out);
      this.buf = '';
    }
    const partial = this.data.length > 0;
    if (partial) out.push({ event: this.event || 'message', data: this.data.join('\n'), id: this.id, partial: true });
    this.data = [];
    return out;
  }

  feed(text) {
    const out = [];
    let s = text;
    if (this.pendingCR) {
      // A CR ended the previous chunk; a leading LF belongs to the same break.
      if (s.startsWith('\n')) s = s.slice(1);
      this.pendingCR = false;
    }
    this.buf += s;
    for (;;) {
      const m = /\r\n|\r|\n/.exec(this.buf);
      if (!m) break;
      if (m[0] === '\r' && m.index === this.buf.length - 1) {
        // Could be the first half of CRLF split across chunks.
        this.line(this.buf.slice(0, m.index), out);
        this.buf = '';
        this.pendingCR = true;
        break;
      }
      this.line(this.buf.slice(0, m.index), out);
      this.buf = this.buf.slice(m.index + m[0].length);
    }
    if (this.buf.length + this.size > this.maxEventBytes) throw new SseError(`SSE event exceeds ${this.maxEventBytes} bytes`);
    return out;
  }

  line(l, out) {
    if (l === '') {
      if (this.data.length) out.push({ event: this.event || 'message', data: this.data.join('\n'), id: this.id });
      this.data = [];
      this.event = '';
      this.size = 0;
      return;
    }
    if (l.startsWith(':')) return; // comment / keep-alive
    const i = l.indexOf(':');
    const field = i < 0 ? l : l.slice(0, i);
    let value = i < 0 ? '' : l.slice(i + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'data') {
      this.data.push(value);
      this.size += value.length;
      if (this.size > this.maxEventBytes) throw new SseError(`SSE event exceeds ${this.maxEventBytes} bytes`);
    } else if (field === 'event') this.event = value;
    else if (field === 'id') this.id = value;
  }
}
