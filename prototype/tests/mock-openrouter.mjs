// OpenAI-compatible streaming mock standing in for openrouter.ai in tests
// (no network, no real key). Usage: node mock-openrouter.mjs [port] — key is "test-key".
// It picks tools by keyword, like a (very) small model would, or follows a
// scripted scenario ("script:<name>" in the user's message) for harness tests.
import http from 'node:http';

const lastOut = (outs) => outs[outs.length - 1] || '';
const SCRIPTS = {
  // A coding task: plan, look, search, read, patch, test, server, finish.
  fix: (step, outs) => [
    () => ({ calls: [['update_plan', { items: [{ text: 'find the bug', status: 'in_progress' }, { text: 'fix and test', status: 'pending' }] }], ['list_dir', {}]] }),
    () => ({ calls: [['search_text', { query: 'function add' }]] }),
    () => ({ calls: [['read_file', { path: 'calc.js' }]] }),
    () => ({ calls: [['apply_patch', { changes: [{ action: 'update', path: 'calc.js', edits: [{ old_text: 'return a - b;', new_text: 'return a + b;' }] }] }]] }),
    () => ({ calls: [['shell', { command: 'node test.js' }]] }),
    () => ({ calls: [['process', { action: 'start', name: 'server', command: 'node -e "require(\'http\').createServer((q,r)=>r.end(\'ok\')).listen(0,function(){console.log(\'listening on \'+this.address().port)})"' }]] }),
    () => ({ calls: [['process', /listening on \d+/.test(lastOut(outs)) ? { action: 'status', id: (lastOut(outs).match(/p[0-9a-f]{10}/) || [])[0] } : { action: 'wait', id: (lastOut(outs).match(/p[0-9a-f]{10}/) || [])[0], until: 'listening on \\d+', timeout_seconds: 10 }], ['update_plan', { items: [{ text: 'find the bug', status: 'completed' }, { text: 'fix and test', status: 'completed' }] }]] }),
    () => ({ text: `Fixed add() in calc.js; node test.js passes (exit 0). ${/listening on \d+/.test(outs.join('\n')) ? 'Server is listening.' : ''}` }),
  ][step]?.(),
  // A network command needs approval under the default policy.
  net: (step) => [() => ({ calls: [['shell', { command: 'curl -sS --max-time 2 http://127.0.0.1:9/ || echo curl-ran' }]] }), () => ({ text: 'network step done' })][step]?.(),
  // Ask the user, then use the answer.
  ask: (step, outs) => [() => ({ calls: [['ask_user', { question: 'Which color?', options: ['red', 'blue'] }]] }), () => ({ text: `You chose: ${lastOut(outs).replace('The user answered: ', '')}` })][step]?.(),
  // Delegate to an isolated child, then adopt its changes.
  delegate: (step, outs) => [
    () => ({ calls: [['tool_search', { query: 'delegate adopt' }]] }),
    () => ({ calls: [['delegate', { task: 'script:child create the greeting file', workspace: 'isolated' }]] }),
    () => ({ calls: [['adopt_changes', { session: (lastOut(outs).match(/Child (s\d+)/) || [])[1] }]] }),
    () => ({ text: `adopted: ${lastOut(outs).split('\n')[0]}` }),
  ][step]?.(),
  child: (step) => [
    () => ({ calls: [['apply_patch', { changes: [{ action: 'create', path: 'greeting.txt', content: 'hello from the child\n' }] }]] }),
    () => ({ calls: [['shell', { command: 'cat greeting.txt' }]] }),
    () => ({ text: 'child done: created greeting.txt' }),
  ][step]?.(),
  // In a branch: show what the workspace looks like there.
  peek: (step, outs) => [() => ({ calls: [['shell', { command: 'cat calc.js; pwd' }]] }), () => ({ text: `branch sees: ${lastOut(outs).replace(/\s+/g, ' ').slice(0, 300)}` })][step]?.(),
  // Delegate a read-only child that then tries to write.
  readonlychild: (step) => [() => ({ calls: [['tool_search', { query: 'select:delegate' }]] }), () => ({ calls: [['delegate', { task: 'script:forbidden', read_only: true }]] }), () => ({ text: 'child reported' })][step]?.(),
  // Try a tool the child may not use.
  forbidden: (step) => [() => ({ calls: [['apply_patch', { changes: [{ action: 'create', path: 'nope.txt', content: 'x' }] }]] }), () => ({ text: 'tried' })][step]?.(),
  // Start a long process (for restart reconciliation).
  long: (step, outs) => [() => ({ calls: [['process', { action: 'start', name: 'ticker', command: 'while true; do echo tick; sleep 1; done' }]] }), () => ({ text: `started ${(lastOut(outs).match(/p[0-9a-f]{10}/) || [])[0]}` })][step]?.(),
  procs: (step) => [() => ({ calls: [['process', { action: 'list' }]] }), () => ({ text: 'listed' })][step]?.(),
  // One assistant turn, four parallel calls: a read, a slow command, a search
  // with a file as its path (actionable error), and a network command that
  // needs approval. Results arrive in any order; the loop asks again once.
  batch: (step, outs) => [
    () => ({ calls: [['read_file', { path: 'calc.js' }], ['shell', { command: 'sleep 1; echo slow-done' }], ['search_text', { query: 'add', path: 'calc.js' }], ['shell', { command: 'wget -q -T 1 http://127.0.0.1:9/ || echo wget-ran' }]] }),
    () => ({ text: `batch done: ${outs.length} results` }),
  ][step]?.(),
  // Parallel calls still running when the user hard-stops the session.
  slowbatch: (step) => [() => ({ calls: [['shell', { command: 'sleep 20' }], ['read_file', { path: 'calc.js' }]] }), () => ({ text: 'should not happen' })][step]?.(),
  // A tool call with malformed JSON arguments and an unknown tool.
  bad: (step) => [() => ({ calls: [['no_such_tool', {}]] }), () => ({ text: 'recovered' })][step]?.(),
};
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
  const last = [...j.messages].reverse().find((m) => m.role !== 'system') || j.messages.at(-1);
  const tools = new Set((j.tools || []).map((t) => t.function.name));
  const call = (name, args) => send({ model: j.model, choices: [{ delta: { tool_calls: [{ index: 0, id: `call_${Date.now()}`, function: { name, arguments: JSON.stringify(args) } }] } }] });
  const t = String(last.content || '');
  let text;
  // Scripted scenarios: "script:<name>" in the latest user message selects a
  // script; the step is the number of assistant turns since that message.
  const msgs = j.messages;
  const lastUser = msgs.map((m, i) => (m.role === 'user' && typeof m.content === 'string' && /script:\w+/.test(m.content) ? i : -1)).filter((i) => i >= 0).pop();
  if (lastUser !== undefined) {
    const name = msgs[lastUser].content.match(/script:(\w+)/)[1];
    const step = msgs.slice(lastUser + 1).filter((m) => m.role === 'assistant').length;
    const toolOut = msgs.slice(lastUser + 1).filter((m) => m.role === 'tool').map((m) => String(m.content));
    const out = SCRIPTS[name]?.(step, toolOut, tools, msgs[lastUser].content);
    if (out) {
      console.error('MOCK script', name, step, JSON.stringify(out).slice(0, 200));
      if (out.calls) {
        out.calls.forEach(([fn, args], i) => send({ model: j.model, choices: [{ delta: { tool_calls: [{ index: i, id: `call_${name}_${step}_${i}_${Date.now()}`, function: { name: fn, arguments: JSON.stringify(args) } }] } }] }));
        send({ model: j.model, choices: [{ delta: {}, finish_reason: 'tool_calls' }], usage: { prompt_tokens: 1000 + step, completion_tokens: 20, prompt_tokens_details: { cached_tokens: 800 }, cost: 0.0001 } });
        return res.end('data: [DONE]\n\n');
      }
      text = out.text;
    }
  }
  const direct = last.role === 'user' && t.match(/^tool (\w+) (\{[\s\S]*\})$/);
  if (text) { /* scripted */ }
  else if (direct && tools.has(direct[1])) call(direct[1], JSON.parse(direct[2])); // "tool <name> <json args>": call it verbatim
  else if (last.role === 'user' && /time/i.test(t) && tools.has('clock')) call('clock', {});
  else if (last.role === 'user' && /calculate/i.test(t) && tools.has('calc')) call('calc', { expression: t.replace(/.*calculate\s*/i, '') });
  else if (last.role === 'user' && /^delegate/i.test(t) && tools.has('delegate')) call('delegate', { task: t.replace(/^delegate:?\s*/i, '') });
  else if (last.role === 'user' && /remember/i.test(t) && tools.has('remember')) call('remember', { note: t.replace(/.*remember( that)?\s*/i, '') });
  else text = last.role === 'tool' ? `The tool returned: ${String(last.content).slice(0, 80)}` : `Mock OpenRouter (${j.model}) replying to: ${t.slice(0, 60)}. This reply is deliberately a little long so that it streams for a while, word by word, giving the test time to stop or crash it mid-stream.`;
  if (text) for (const w of text.split(/(?<= )/)) { send({ model: j.model, choices: [{ delta: { content: w } }] }); await new Promise((r) => setTimeout(r, Number(process.env.MOCK_DELAY_MS || 40))); }
  send({ model: j.model, choices: [{ delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 900, completion_tokens: 30, prompt_tokens_details: { cached_tokens: 600 }, cost: 0.0001 } });
  res.end('data: [DONE]\n\n');
}).listen(Number(process.argv[2] || 8765), () => console.error('mock OpenRouter listening'));
