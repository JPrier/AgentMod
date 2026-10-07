#!/usr/bin/env bash
# Assemble the static site (GitHub Pages): UI + WASM kernel + JS plugins.
# Run scripts/build-wasm.sh first so ui/pkg exists.
set -euo pipefail
cd "$(dirname "$0")/.."
test -f ui/pkg/agentmod_wasm.js || { echo "ui/pkg missing: run scripts/build-wasm.sh" >&2; exit 1; }
rm -rf dist
mkdir -p dist
cp -R ui/. dist/
mkdir -p dist/plugins
for d in plugins/*/; do
  name=$(basename "$d")
  # Worker-capable plugins only (JS); native-only plugins are disabled in the browser.
  if ls "$d"*.js >/dev/null 2>&1; then cp -R "$d" "dist/plugins/$name"; fi
done
cp plugins/package.json dist/plugins/
touch dist/.nojekyll
echo "site assembled in dist/ ($(du -sh dist | cut -f1))"
