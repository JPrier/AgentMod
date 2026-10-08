const { getUsr } = require('./users');
function greet(id) {
  const u = getUsr(id);
  return u ? `Hello, ${u.name}!` : 'Hello, stranger!';
}
module.exports = { greet };
