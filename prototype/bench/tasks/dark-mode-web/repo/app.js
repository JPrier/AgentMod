// Notes app. `addNote` is exported for tests (no browser needed).
function addNote(list, text) {
  const t = String(text || '').trim();
  if (!t) return list;
  return [...list, { text: t, at: list.length }];
}

if (typeof document !== 'undefined') {
  let notes = [];
  const ul = document.getElementById('notes');
  document.getElementById('add').addEventListener('submit', (e) => {
    e.preventDefault();
    const input = document.getElementById('note');
    notes = addNote(notes, input.value);
    input.value = '';
    ul.replaceChildren(...notes.map((n) => Object.assign(document.createElement('li'), { textContent: n.text })));
  });
}

if (typeof module !== 'undefined') module.exports = { addNote };
