// Glob patterns for search_files and include/exclude filters.
//
//   *      any run of characters except '/'
//   **     any run of path segments (including none)
//   ?      one character except '/'
//   [abc]  a character class;  {a,b}  alternatives
//
// A pattern without '/' matches a file name anywhere ("*.rs" == "**/*.rs").

export function globToRegExp(glob) {
  let g = String(glob).trim().replace(/^\.\//, '');
  if (!g.includes('/')) g = `**/${g}`;
  let re = '';
  let i = 0;
  let braces = 0;
  while (i < g.length) {
    const c = g[i];
    if (c === '*') {
      if (g[i + 1] === '*') {
        // '**/' matches zero or more whole segments; a trailing '**' matches the rest.
        if (g[i + 2] === '/') {
          re += '(?:[^/]*/)*';
          i += 3;
        } else {
          re += '.*';
          i += 2;
        }
      } else {
        re += '[^/]*';
        i += 1;
      }
    } else if (c === '?') {
      re += '[^/]';
      i += 1;
    } else if (c === '[') {
      const end = g.indexOf(']', i + 1);
      if (end < 0) { re += '\\['; i += 1; continue; }
      let cls = g.slice(i + 1, end).replace(/\\/g, '\\\\');
      if (cls.startsWith('!')) cls = `^${cls.slice(1)}`;
      re += `[${cls}]`;
      i = end + 1;
    } else if (c === '{') {
      braces += 1;
      re += '(?:';
      i += 1;
    } else if (c === '}' && braces > 0) {
      braces -= 1;
      re += ')';
      i += 1;
    } else if (c === ',' && braces > 0) {
      re += '|';
      i += 1;
    } else {
      re += c.replace(/[.+^$()|\\]/g, '\\$&');
      i += 1;
    }
  }
  // "src/**" also matches "src" itself; "dir/" matches everything under dir.
  if (re.endsWith('/')) re += '.*';
  return new RegExp(`^${re}$`);
}

/** Does relative path `p` match any of `globs`? */
export function matchesAny(p, globs) {
  return globs.some((g) => globToRegExp(g).test(p));
}
