// OpenAI-compatible streaming mock standing in for openrouter.ai in tests
// (no network, no real key). Usage: node mock-openrouter.mjs [port] — key is "test-key".
// It picks tools by keyword, like a (very) small model would.
import http from 'node:http';
http.createServer(async (req, res) => {
  res.setHeader('access-control-allow-origin', '*');
  res.setHeader('access-control-allow-headers', '*');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const ok = req.headers.authorization === 'Bearer test-key';
  if (req.method === 'GET' && req.url.endsWith('/models')) {
    // Shape of https://openrouter.ai/api/v1/models (public; no key needed).
    const model = (id, name, tools, prompt) => ({ id, name, context_length: 128000, pricing: { prompt, completion: String(Number(prompt) * 4) }, supported_parameters: tools ? ['tools', 'temperature'] : ['temperature'] });
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ data: [
      model('openai/gpt-4o-mini', 'OpenAI: GPT-4o-mini', true, '0.00000015'),
      model('mock/tool-model', 'Mock: Tool Model', true, '0'),
      model('mock/chat-only', 'Mock: Chat Only', false, '0.000001'),
    ] }));
  }
  if (req.method === 'GET' && req.url.endsWith('/key')) { res.writeHead(ok ? 200 : 401, { 'content-type': 'application/json' }); return res.end(ok ? '{"data":{"label":"test"}}' : '{"error":{"message":"bad key"}}'); }
  let body = ''; for await (const c of req) body += c;
  const j = JSON.parse(body || '{}');
  console.error('MOCK', JSON.stringify({ title: req.headers['x-title'], model: j.model, tools: (j.tools || []).map((t) => t.function.name), roles: j.messages.map((m) => m.role) }));
  if (!ok) { res.writeHead(401); return res.end('{"error":{"message":"bad key"}}'); }
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const send = (o) => res.write(`data: ${JSON.stringify(o)}\n\n`);
  const last = j.messages.at(-1);
  const tools = new Set((j.tools || []).map((t) => t.function.name));
  const call = (name, args) => send({ model: j.model, choices: [{ delta: { tool_calls: [{ index: 0, id: `call_${Date.now()}`, function: { name, arguments: JSON.stringify(args) } }] } }] });
  const t = String(last.content || '');
  let text;
  const direct = last.role === 'user' && t.match(/^tool (\w+) (\{[\s\S]*\})$/);
  if (direct && tools.has(direct[1])) call(direct[1], JSON.parse(direct[2])); // "tool <name> <json args>": call it verbatim
  else if (last.role === 'user' && /time/i.test(t) && tools.has('clock')) call('clock', {});
  else if (last.role === 'user' && /calculate/i.test(t) && tools.has('calc')) call('calc', { expression: t.replace(/.*calculate\s*/i, '') });
  else if (last.role === 'user' && /^delegate/i.test(t) && tools.has('delegate')) call('delegate', { task: t.replace(/^delegate:?\s*/i, '') });
  else if (last.role === 'user' && /remember/i.test(t) && tools.has('remember')) call('remember', { note: t.replace(/.*remember( that)?\s*/i, '') });
  else text = last.role === 'tool' ? `The tool returned: ${String(last.content).slice(0, 80)}` : `Mock OpenRouter (${j.model}) replying to: ${t.slice(0, 60)}. This reply is deliberately a little long so that it streams for a while, word by word, giving the test time to stop or crash it mid-stream.`;
  if (text) for (const w of text.split(/(?<= )/)) { send({ model: j.model, choices: [{ delta: { content: w } }] }); await new Promise((r) => setTimeout(r, Number(process.env.MOCK_DELAY_MS || 40))); }
  send({ model: j.model, choices: [{ delta: {}, finish_reason: 'stop' }] });
  res.end('data: [DONE]\n\n');
}).listen(Number(process.argv[2] || 8765), () => console.error('mock OpenRouter listening'));
