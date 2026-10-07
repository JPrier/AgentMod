// A deliberately small Markdown renderer for assistant text (escape-first).

const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);

function inline(s) {
  return esc(s)
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/(^|[^*])\*([^*\s][^*]*)\*/g, '$1<em>$2</em>')
    .replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
}

export function markdown(src) {
  const lines = String(src ?? '').split('\n');
  const out = [];
  let list = null;
  let para = [];
  const flush = () => {
    if (para.length) out.push(`<p>${inline(para.join(' '))}</p>`);
    para = [];
    if (list) out.push(`</${list}>`);
    list = null;
  };
  for (const line of lines) {
    const t = line.trim();
    let m;
    if (!t) { flush(); continue; }
    if ((m = t.match(/^>\s?(.*)/))) { flush(); out.push(`<blockquote>${inline(m[1])}</blockquote>`); continue; }
    if ((m = t.match(/^[-*]\s+(.*)/)) || (m = t.match(/^\d+[.)]\s+(.*)/))) {
      const kind = /^\d/.test(t) ? 'ol' : 'ul';
      if (para.length) { out.push(`<p>${inline(para.join(' '))}</p>`); para = []; }
      if (list !== kind) { if (list) out.push(`</${list}>`); out.push(`<${kind}>`); list = kind; }
      out.push(`<li>${inline(m[1])}</li>`);
      continue;
    }
    if (list) { out.push(`</${list}>`); list = null; }
    para.push(t);
  }
  flush();
  return out.join('');
}

export { esc };
