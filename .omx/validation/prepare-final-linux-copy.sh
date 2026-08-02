#!/usr/bin/env bash
set -euo pipefail

source_root=/mnt/c/Users/jkpri/AgentMod
evidence_root="$source_root/.omx/validation"
snapshot_id=${1:-initial}
if [[ ! "$snapshot_id" =~ ^[a-z0-9]+$ ]]; then
  echo "invalid Linux final-copy snapshot id: $snapshot_id" >&2
  exit 2
fi
if [[ "$snapshot_id" == initial ]]; then
  status_path="$evidence_root/2026-07-31-linux-final-source-status.txt"
  root_path="$evidence_root/2026-07-31-linux-final-copy-root.txt"
else
  status_path="$evidence_root/2026-07-31-linux-final-$snapshot_id-source-status.txt"
  root_path="$evidence_root/2026-07-31-linux-final-$snapshot_id-copy-root.txt"
fi

if [[ -e "$status_path" || -e "$root_path" ]]; then
  echo "refusing to overwrite existing Linux final-copy evidence" >&2
  exit 2
fi

git -C "$source_root" status --porcelain=v1 >"$status_path"
status_count=$(wc -l <"$status_path" | tr -d ' ')
status_sha=$(sha256sum "$status_path" | cut -d ' ' -f 1)
source_head=$(git -C "$source_root" rev-parse HEAD)
copy_root=$(mktemp -d "/tmp/agentmod-linux-final-20260731-$snapshot_id.XXXXXX")

rsync -a \
  --exclude=.git/ \
  --exclude=target/ \
  --exclude=.omx/validation/ \
  "$source_root/" "$copy_root/"
mkdir -p "$copy_root/.cargo-target"
printf '%s\n' "$copy_root" >"$root_path"

printf 'ISOLATED_COPY_ROOT=%s\n' "$copy_root"
printf 'SOURCE_HEAD=%s\n' "$source_head"
printf 'SOURCE_DIRTY_ENTRIES=%s\n' "$status_count"
printf 'SOURCE_STATUS_SHA256=%s\n' "$status_sha"
printf 'COPY_BYTES=%s\n' "$(du -sb "$copy_root" | cut -f 1)"
date --iso-8601=seconds --utc
rustc -Vv
cargo -Vv
uname -a
sed -n '1,8p' /etc/os-release
lscpu | sed -n '1,18p'
