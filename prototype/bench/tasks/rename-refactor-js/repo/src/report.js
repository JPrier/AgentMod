const users = require('./users');
function report(ids) {
  return ids.map((id) => users.getUsr(id)?.name ?? '?').join(', ');
}
module.exports = { report };
