const { addNote } = require('../app.js');
const fail = (m) => { console.error('FAIL', m); process.exit(1); };
if (addNote([], '  hi ').length !== 1 || addNote([], '  hi ')[0].text !== 'hi') fail('add trims');
if (addNote([{ text: 'a', at: 0 }], '').length !== 1) fail('empty ignored');
console.log('ok');
