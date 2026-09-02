#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
size_bytes="${DELTAWEAVE_TEST_SIZE_BYTES:-104857600}"
remote_path="loopback/p2p-loopback-100mb.bin"
keep_work_dir="${DELTAWEAVE_KEEP_WORK_DIR:-0}"
server_pid=""
server_stderr=""

if [[ ! "$size_bytes" =~ ^[1-9][0-9]*$ ]]; then
  echo 'DELTAWEAVE_TEST_SIZE_BYTES must be a positive integer' >&2
  exit 2
fi

for dependency in dd python3 sha256sum; do
  command -v "$dependency" >/dev/null || {
    echo "$dependency is required" >&2
    exit 1
  }
done

if [[ -n "${DELTAWEAVE_WORK_DIR:-}" ]]; then
  work_dir="$DELTAWEAVE_WORK_DIR"
  if [[ -e "$work_dir" ]] && [[ -n "$(find "$work_dir" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
    echo "DELTAWEAVE_WORK_DIR must be empty: $work_dir" >&2
    exit 2
  fi
  mkdir -p "$work_dir"
else
  work_dir="$(mktemp -d)"
fi

cleanup() {
  local status=$?
  trap - EXIT
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if (( status != 0 )) && [[ -n "$server_stderr" ]] && [[ -s "$server_stderr" ]]; then
    echo 'Receiver stderr:' >&2
    cat "$server_stderr" >&2
  fi
  if [[ "$keep_work_dir" == "1" ]]; then
    echo "Work directory retained: $work_dir"
  else
    rm -rf "$work_dir"
  fi
  exit "$status"
}
trap cleanup EXIT

if [[ -n "${DELTAWEAVE_BIN:-}" ]]; then
  deltaweave="$DELTAWEAVE_BIN"
else
  command -v cargo >/dev/null || {
    echo 'cargo is required when DELTAWEAVE_BIN is not set' >&2
    exit 1
  }
  echo '[1/6] Building DeltaWeave CLI'
  (
    cd "$repository_root"
    cargo build --locked -p deltaweave
  )
  target_dir="${CARGO_TARGET_DIR:-$repository_root/target}"
  if [[ "$target_dir" != /* ]]; then
    target_dir="$repository_root/$target_dir"
  fi
  deltaweave="$target_dir/debug/deltaweave"
fi

if [[ ! -x "$deltaweave" ]]; then
  echo "DeltaWeave executable is not available: $deltaweave" >&2
  exit 1
fi

source_dir="$work_dir/source"
received_dir="$work_dir/received"
private_dir="$work_dir/private"
source_file="$source_dir/p2p-loopback-100mb.bin"
received_file="$received_dir/$remote_path"
sender_identity="$private_dir/sender.key"
receiver_identity="$private_dir/receiver.key"
receiver_state="$private_dir/receiver-state"
server_stdout="$work_dir/receiver.stdout"
server_stderr="$work_dir/receiver.stderr"
push_receipt="$work_dir/push-receipt.json"

mkdir -p "$source_dir" "$received_dir" "$private_dir"

echo '[2/6] Generating source payload'
dd if=/dev/urandom of="$source_file" bs="$size_bytes" count=1 status=none
actual_source_size="$(wc -c <"$source_file" | tr -d '[:space:]')"
if [[ "$actual_source_size" != "$size_bytes" ]]; then
  echo "Source size mismatch: expected $size_bytes bytes, got $actual_source_size" >&2
  exit 1
fi
source_hash="$(sha256sum "$source_file" | awk '{print $1}')"

echo '[3/6] Creating loopback peer identities'
sender_init="$work_dir/sender-init.json"
receiver_init="$work_dir/receiver-init.json"
"$deltaweave" init --identity "$sender_identity" >"$sender_init"
"$deltaweave" init --identity "$receiver_identity" >"$receiver_init"
sender_endpoint="$(python3 - "$sender_init" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    print(json.load(stream)["endpoint_id"])
PY
)"

echo '[4/6] Starting direct-only receiver on loopback'
"$deltaweave" serve \
  --root "$received_dir" \
  --state "$receiver_state" \
  --identity "$receiver_identity" \
  --allow-peer "$sender_endpoint" \
  --bind 127.0.0.1:0 \
  --direct-only \
  >"$server_stdout" 2>"$server_stderr" &
server_pid=$!

server_json=""
deadline=$((SECONDS + 30))
while ! server_json="$(python3 - "$server_stdout" 2>/dev/null <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    value = json.load(stream)
if value.get("status") != "ready":
    raise SystemExit(1)
addresses = value.get("direct_addresses", [])
if not addresses:
    raise SystemExit(1)
print(value["endpoint_id"])
print(addresses[0])
PY
)"; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    wait "$server_pid" || true
    echo 'Receiver exited before becoming ready' >&2
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo 'Timed out waiting for the receiver to become ready' >&2
    exit 1
  fi
  sleep 0.1
done
mapfile -t server_info <<<"$server_json"
receiver_endpoint="${server_info[0]}"
receiver_address="${server_info[1]}"

echo '[5/6] Sending payload over authenticated P2P loopback'
"$deltaweave" push "$source_file" \
  --remote-path "$remote_path" \
  --peer "$receiver_endpoint" \
  --direct "$receiver_address" \
  --identity "$sender_identity" \
  --direct-only \
  >"$push_receipt"

python3 - "$push_receipt" "$size_bytes" "$remote_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    receipt = json.load(stream)
expected_size = int(sys.argv[2])
expected_path = sys.argv[3]
if receipt.get("transferred_bytes") != expected_size:
    raise SystemExit(
        f"receipt transferred_bytes mismatch: expected {expected_size}, "
        f"got {receipt.get('transferred_bytes')}"
    )
if receipt.get("path") != expected_path:
    raise SystemExit(
        f"receipt path mismatch: expected {expected_path!r}, got {receipt.get('path')!r}"
    )
PY

if [[ ! -f "$received_file" ]]; then
  echo "Received file is missing: $received_file" >&2
  exit 1
fi
received_size="$(wc -c <"$received_file" | tr -d '[:space:]')"
if [[ "$received_size" != "$size_bytes" ]]; then
  echo "Received size mismatch: expected $size_bytes bytes, got $received_size" >&2
  exit 1
fi

echo '[6/6] Verifying end-to-end SHA-256 digest'
received_hash="$(sha256sum "$received_file" | awk '{print $1}')"
printf 'Source SHA-256:   %s\n' "$source_hash"
printf 'Received SHA-256: %s\n' "$received_hash"
if [[ "$received_hash" != "$source_hash" ]]; then
  echo 'SHA-256 mismatch after P2P loopback transfer' >&2
  exit 1
fi

printf 'Verified bytes: %s\n' "$received_size"
echo 'P2P loopback transfer and SHA-256 verification: PASS'
