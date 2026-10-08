const { add, mul } = require('./calc');
let failed = 0;
const check = (name, got, want) => { if (got !== want) { console.error(`FAIL ${name}: got ${got}, want ${want}`); failed++; } else console.log(`ok ${name}`); };
check('add', add(2, 3), 5);
check('mul', mul(2, 3), 6);
process.exit(failed ? 1 : 0);
