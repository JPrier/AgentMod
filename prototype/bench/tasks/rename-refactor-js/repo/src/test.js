const { greet } = require('./greet');
const { report } = require('./report');
const fail = (m) => { console.error('FAIL', m); process.exit(1); };
if (greet(1) !== 'Hello, Ada!') fail('greet');
if (greet(9) !== 'Hello, stranger!') fail('stranger');
if (report([1, 2, 3]) !== 'Ada, Linus, ?') fail('report');
console.log('ok');
