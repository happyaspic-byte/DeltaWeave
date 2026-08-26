#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
assets_dir="$repo_dir/docs/assets"
work_dir="$(mktemp -d)"

command -v convert >/dev/null || {
  echo "ImageMagick 'convert' is required" >&2
  exit 1
}
command -v identify >/dev/null || {
  echo "ImageMagick 'identify' is required" >&2
  exit 1
}
command -v fc-match >/dev/null || {
  echo "fontconfig 'fc-match' is required" >&2
  exit 1
}

font="${DELTAWEAVE_DOC_FONT:-$(fc-match --format='%{file}\n' 'DejaVu Sans Mono' | sed -n '1p')}"
test -f "$font" || {
  echo "a readable monospace font is required" >&2
  exit 1
}

mkdir -p "$assets_dir"

render_terminal() {
  local output="$1"
  local section="$2"
  local body="$3"
  local point_size="${4:-24}"
  local line_spacing="${5:-8}"
  local header="${6:-DeltaWeave v0.2.0  |  verified CLI output}"

  convert -size 1280x720 xc:'#07111f' \
    -fill '#111c2d' -stroke '#26364c' -strokewidth 2 \
    -draw 'roundrectangle 32,32 1248,688 24,24' \
    -fill '#ff5f57' -stroke none -draw 'circle 72,72 80,72' \
    -fill '#febc2e' -draw 'circle 100,72 108,72' \
    -fill '#28c840' -draw 'circle 128,72 136,72' \
    -font "$font" -pointsize 22 -fill '#8fa6c2' \
    -annotate +170+80 "$header" \
    -pointsize 23 -fill '#2dd4bf' -annotate +72+132 "$section" \
    -pointsize "$point_size" -fill '#e6edf7' -interline-spacing "$line_spacing" \
    -annotate +72+184 "$body" \
    -strip "$output"
}

# Transfer sequence: the Windows values and peer-accept lines come from the
# v0.2.0 release workflow's packaged-binary self-test.
render_terminal "$work_dir/transfer-before.png" \
  '전 (BEFORE)  |  Start isolated package verification' \
  $'PS> .\\\\deltaweave.exe self-test\n\nTemporary source, receiver, chunk store, and index directories\nare created automatically. User synchronization folders are not touched.\n\nChecks queued:\n  - authenticated QUIC transfer\n  - FastCDC/BLAKE3 integrity\n  - delta reuse\n  - rename, tombstone, and restart recovery'

render_terminal "$work_dir/transfer-during.png" \
  '중 (DURING)  |  Actual Windows release execution' \
  $'03:36:16.316  INFO router.accept\n  me=0d8cae8543  alpn="deltaweave/sync/1"\n  remote=fe99ed78e7\n  accepted DeltaWeave peer                 [full transfer]\n\n03:36:16.617  INFO router.accept\n  me=0d8cae8543  alpn="deltaweave/sync/1"\n  remote=fe99ed78e7\n  accepted DeltaWeave peer                 [delta transfer]\n\nEvery received chunk is verified before materialization.' \
  21 6

render_terminal "$work_dir/transfer-after.png" \
  '후 (AFTER)  |  Actual Windows JSON output' \
  $'{\n  "architecture": "x86_64",\n  "operating_system": "windows",\n  "first_transfer_bytes": 4194304,\n  "second_transfer_bytes": 257800,\n  "reused_extents": 16,\n  "index_rename_detected": true,\n  "index_restart_verified": true,\n  "index_tombstones": 2,\n  "status": "pass"\n}'

render_terminal "$work_dir/transfer-result.png" \
  '결과 (RESULT)  |  Verification summary' \
  $'PASS\n\n  Encrypted peer connection ............. verified\n  Chunk and whole-file integrity ......... verified\n  Initial transfer ....................... 4,194,304 B\n  Delta transfer .........................   257,800 B\n  Transfer reduction .....................     93.85%\n  Reused extents .........................         16\n  Rename / tombstone / restart ........... verified\n  Temporary test data .................... cleaned'

# Local-index sequence: rendered from a real three-file scan and native watcher
# event generated with the v0.2.0 release binary.
render_terminal "$work_dir/index-before.png" \
  '전 (BEFORE)  |  Prepare an isolated test folder' \
  $'PS> $Root = "C:\\\\DeltaWeave-Test\\\\root"\nPS> $Private = "C:\\\\DeltaWeave-Test\\\\private"\nPS> .\\\\deltaweave.exe scan `\n>>   --root $Root `\n>>   --state "$Private\\\\index.redb" `\n>>   --identity "$Private\\\\node.key"\n\nPrivate state is deliberately outside the indexed folder.' \
  21 6

render_terminal "$work_dir/index-during.png" \
  '중 (DURING)  |  Native watcher is active' \
  $'{\n  "event": "initial_scan",\n  "report": {\n    "generation": 1,\n    "live_records": 0,\n    "issues": []\n  },\n  "root": "C:\\\\\\\\DeltaWeave-Test\\\\\\\\root",\n  "status": "watching",\n  "watcher_error": null\n}\n\nPS> New-Item "$Root\\\\new-file.txt"' \
  21 6

render_terminal "$work_dir/index-after.png" \
  '후 (AFTER)  |  Actual native watcher event' \
  $'{\n  "event": "watch_scan",\n  "native_events": 2,\n  "report": {\n    "generation": 2,\n    "live_records": 1,\n    "files_hashed": 1,\n    "changes": [\n      { "kind": "created", "path": "new-file.txt" }\n    ],\n    "issues": []\n  },\n  "rescan_required": false,\n  "watcher_degraded": false\n}' \
  20 5

render_terminal "$work_dir/index-result.png" \
  '결과 (RESULT)  |  Authoritative scan is clean' \
  $'{\n  "generation": 1,\n  "live_records": 5,\n  "tombstones": 0,\n  "files_hashed": 3,\n  "retries_queued": 0,\n  "changes": [\n    { "kind": "created", "path": "Documents/report.pdf" },\n    { "kind": "created", "path": "Photos/IMG_001.jpg" }\n  ],\n  "collisions": [],\n  "issues": []\n}' \
  21 5

render_terminal "$work_dir/synology-self-test.png" \
  'Synology ARM64 release package  |  verified binary result' \
  $'admin@synology:~$ ./deltaweave self-test\n{\n  "architecture": "aarch64",\n  "operating_system": "linux",\n  "first_transfer_bytes": 4194304,\n  "second_transfer_bytes": 257800,\n  "reused_extents": 16,\n  "index_rename_detected": true,\n  "index_restart_verified": true,\n  "index_tombstones": 2,\n  "status": "pass"\n}'

# Bidirectional sequence: values come from an actual v0.3.0 sync-once CLI run using
# two independent roots, persistent identities, the direct-only server, and protocol v2.
render_terminal "$work_dir/sync-before.png" \
  '전 (BEFORE)  |  Independent Windows and NAS folders' \
  $'WINDOWS ROOT                         NAS ROOT\n  windows-only.txt   7,886 B           nas-only.txt   3,139 B\n  shared.txt         1,080 B           shared.txt     1,080 B\n\nPS> .\\deltaweave.exe sync-once `\n>> --root C:\\DeltaWeave-Sync `\n>> --peer <NAS_ENDPOINT_ID> `\n>> --direct <NAS_IP:UDP_PORT> --direct-only\n\nBoth private state roots and identities are outside synchronized data.' \
  20 6 'DeltaWeave v0.3.0  |  verified sync-once output'

render_terminal "$work_dir/sync-during.png" \
  '중 (DURING)  |  Initial bidirectional reconciliation' \
  $'{\n  "status": "pass",\n  "merkle_queries": 3,\n  "local_actions": 2,\n  "remote_actions": 2,\n  "pulled_bytes": 3139,\n  "pushed_bytes": 8966,\n  "conflicts": []\n}\n\nWindows now has nas-only.txt.\nNAS now has windows-only.txt.\nA final snapshot proves both Merkle roots are identical.' \
  21 5 'DeltaWeave v0.3.0  |  verified sync-once output'

render_terminal "$work_dir/sync-after.png" \
  '후 (AFTER)  |  Concurrent edits preserve both contents' \
  $'{\n  "status": "pass",\n  "merkle_queries": 2,\n  "local_actions": 2,\n  "remote_actions": 2,\n  "conflicts": [{\n    "path": "shared.txt",\n    "conflict_path": "shared.conflict-7caef0037876.txt",\n    "reason": "concurrent_edit"\n  }]\n}\n\nBoth peers contain the canonical file and the same verified conflict copy.' \
  20 4 'DeltaWeave v0.3.0  |  verified sync-once output'

render_terminal "$work_dir/sync-result.png" \
  '결과 (RESULT)  |  Delete propagation and idempotent retry' \
  $'DELETE PASS\n  local_actions ........ 0\n  remote_actions ....... 1\n  pushed_bytes ......... 0\n  windows-only.txt is absent on both peers\n\nNO-CHANGE RETRY PASS\n  merkle_queries ....... 1\n  local_actions ........ 0\n  remote_actions ....... 0\n  pulled / pushed ...... 0 B / 0 B\n  verified roots ....... identical' \
  22 7 'DeltaWeave v0.3.0  |  verified sync-once output'

cp "$work_dir/transfer-before.png" "$assets_dir/usage-01-before.png"
cp "$work_dir/transfer-during.png" "$assets_dir/usage-02-during.png"
cp "$work_dir/transfer-after.png" "$assets_dir/usage-03-after.png"
cp "$work_dir/transfer-result.png" "$assets_dir/usage-04-result.png"
cp "$work_dir/transfer-after.png" "$assets_dir/deltaweave-self-test.png"
cp "$work_dir/index-before.png" "$assets_dir/index-01-before.png"
cp "$work_dir/index-during.png" "$assets_dir/index-02-during.png"
cp "$work_dir/index-after.png" "$assets_dir/index-03-after.png"
cp "$work_dir/index-result.png" "$assets_dir/index-04-result.png"
cp "$work_dir/index-after.png" "$assets_dir/deltaweave-index-watch.png"
cp "$work_dir/synology-self-test.png" "$assets_dir/deltaweave-synology-self-test.png"
cp "$work_dir/sync-before.png" "$assets_dir/sync-01-before.png"
cp "$work_dir/sync-during.png" "$assets_dir/sync-02-during.png"
cp "$work_dir/sync-after.png" "$assets_dir/sync-03-after.png"
cp "$work_dir/sync-result.png" "$assets_dir/sync-04-result.png"

convert \
  -delay 150 "$work_dir/transfer-before.png" \
  -delay 190 "$work_dir/transfer-during.png" \
  -delay 190 "$work_dir/transfer-after.png" \
  -delay 240 "$work_dir/transfer-result.png" \
  -loop 0 -colors 128 -layers Optimize \
  "$assets_dir/deltaweave-quickstart.gif"

convert \
  -delay 150 "$work_dir/index-before.png" \
  -delay 170 "$work_dir/index-during.png" \
  -delay 190 "$work_dir/index-after.png" \
  -delay 240 "$work_dir/index-result.png" \
  -loop 0 -colors 128 -layers Optimize \
  "$assets_dir/deltaweave-index-lifecycle.gif"

convert \
  -delay 170 "$work_dir/sync-before.png" \
  -delay 190 "$work_dir/sync-during.png" \
  -delay 210 "$work_dir/sync-after.png" \
  -delay 260 "$work_dir/sync-result.png" \
  -loop 0 -colors 128 -layers Optimize \
  "$assets_dir/deltaweave-sync-lifecycle.gif"

identify \
  "$assets_dir/deltaweave-self-test.png" \
  "$assets_dir/deltaweave-index-watch.png" \
  "$assets_dir/deltaweave-synology-self-test.png" \
  "$assets_dir/deltaweave-quickstart.gif" \
  "$assets_dir/deltaweave-index-lifecycle.gif" \
  "$assets_dir/deltaweave-sync-lifecycle.gif"
