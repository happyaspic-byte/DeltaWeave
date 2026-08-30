#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
target="${1:-x86_64-pc-windows-msvc}"
dest_dir="$repository_root/apps/deltaweave-gui/src-tauri/binaries"
dest="$dest_dir/deltaweave-daemon-${target}.exe"
mkdir -p "$dest_dir"

candidates=(
  "$repository_root/target/${target}/release/deltaweave-daemon.exe"
  "$repository_root/target/${target}/release/deltaweave-daemon"
  "$repository_root/target/release/deltaweave-daemon.exe"
  "$repository_root/target/release/deltaweave-daemon"
)

src=""
for candidate in "${candidates[@]}"; do
  if [[ -f "$candidate" ]]; then
    src="$candidate"
    break
  fi
done

if [[ -z "$src" ]]; then
  echo "missing release daemon for $target" >&2
  exit 1
fi

cp "$src" "$dest"
size="$(wc -c < "$dest")"
if [[ "$size" -le 1024 ]]; then
  echo "invalid sidecar $dest ($size bytes)" >&2
  exit 1
fi

echo "$dest"
