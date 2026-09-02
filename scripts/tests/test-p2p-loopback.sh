#!/usr/bin/env bash
set -euo pipefail

test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

mock_bin="$test_root/deltaweave"
command_log="$test_root/commands.log"
server_root_file="$test_root/server-root"
server_pid_file="$test_root/server.pid"
server_stopped_file="$test_root/server.stopped"
work_dir="$test_root/work"
output_file="$test_root/output.log"

cat >"$mock_bin" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >>"$MOCK_COMMAND_LOG"
command="$1"
shift

case "$command" in
  init)
    identity=""
    while (( $# > 0 )); do
      case "$1" in
        --identity)
          identity="$2"
          shift 2
          ;;
        *)
          exit 2
          ;;
      esac
    done
    mkdir -p "$(dirname "$identity")"
    : >"$identity"
    case "$identity" in
      *sender*) endpoint_id="sender-endpoint" ;;
      *) endpoint_id="receiver-endpoint" ;;
    esac
    printf '{\n  "created": true,\n  "endpoint_id": "%s",\n  "identity_file": "%s"\n}\n' \
      "$endpoint_id" "$identity"
    ;;
  serve)
    root=""
    while (( $# > 0 )); do
      case "$1" in
        --root)
          root="$2"
          shift 2
          ;;
        --state|--identity|--allow-peer|--bind)
          shift 2
          ;;
        --direct-only)
          shift
          ;;
        *)
          exit 2
          ;;
      esac
    done
    printf '%s\n' "$root" >"$MOCK_SERVER_ROOT_FILE"
    printf '%s\n' "$$" >"$MOCK_SERVER_PID_FILE"
    trap 'printf stopped >"$MOCK_SERVER_STOPPED_FILE"; exit 0' TERM INT
    printf '{\n  "status": "ready",\n  "endpoint_id": "receiver-endpoint",\n  "direct_addresses": [\n    "127.0.0.1:41234"\n  ],\n  "relay_urls": []\n}\n'
    while :; do sleep 1 & wait $!; done
    ;;
  push)
    if [[ "${MOCK_PUSH_FAIL:-0}" == "1" ]]; then
      exit 23
    fi
    source="$1"
    shift
    remote_path=""
    while (( $# > 0 )); do
      case "$1" in
        --remote-path)
          remote_path="$2"
          shift 2
          ;;
        --peer|--direct|--identity)
          shift 2
          ;;
        --direct-only)
          shift
          ;;
        *)
          exit 2
          ;;
      esac
    done
    root="$(cat "$MOCK_SERVER_ROOT_FILE")"
    mkdir -p "$root/$(dirname "$remote_path")"
    cp "$source" "$root/$remote_path"
    size="$(wc -c <"$source" | tr -d '[:space:]')"
    printf '{\n  "file_hash": "mock",\n  "manifest_hash": "mock",\n  "transferred_bytes": %s,\n  "reused_extents": 0,\n  "path": "%s"\n}\n' \
      "$size" "$remote_path"
    ;;
  *)
    exit 2
    ;;
esac
MOCK
chmod +x "$mock_bin"

MOCK_COMMAND_LOG="$command_log" \
MOCK_SERVER_ROOT_FILE="$server_root_file" \
MOCK_SERVER_PID_FILE="$server_pid_file" \
MOCK_SERVER_STOPPED_FILE="$server_stopped_file" \
DELTAWEAVE_BIN="$mock_bin" \
DELTAWEAVE_TEST_SIZE_BYTES=1048576 \
DELTAWEAVE_WORK_DIR="$work_dir" \
DELTAWEAVE_KEEP_WORK_DIR=1 \
  "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/test-p2p-loopback.sh" \
  >"$output_file"

source_file="$work_dir/source/p2p-loopback-100mb.bin"
received_file="$work_dir/received/loopback/p2p-loopback-100mb.bin"
source_size="$(wc -c <"$source_file" | tr -d '[:space:]')"
received_size="$(wc -c <"$received_file" | tr -d '[:space:]')"
source_hash="$(sha256sum "$source_file" | awk '{print $1}')"
received_hash="$(sha256sum "$received_file" | awk '{print $1}')"

[[ "$source_size" == "1048576" ]]
[[ "$received_size" == "$source_size" ]]
[[ "$received_hash" == "$source_hash" ]]
grep -Fq 'serve --root' "$command_log"
grep -Fq -- '--allow-peer sender-endpoint' "$command_log"
grep -Fq -- '--bind 127.0.0.1:0 --direct-only' "$command_log"
grep -Fq 'push ' "$command_log"
grep -Fq -- '--peer receiver-endpoint' "$command_log"
grep -Fq -- '--direct 127.0.0.1:41234' "$command_log"
grep -Fq -- '--remote-path loopback/p2p-loopback-100mb.bin' "$command_log"
push_command="$(grep '^push ' "$command_log")"
[[ "$push_command" == *' --direct-only' ]]
grep -Fq "Source SHA-256:   $source_hash" "$output_file"
grep -Fq "Received SHA-256: $received_hash" "$output_file"
grep -Fq 'P2P loopback transfer and SHA-256 verification: PASS' "$output_file"

failure_work_dir="$test_root/failure-work"
failure_output="$test_root/failure-output.log"
: >"$server_stopped_file"
if MOCK_COMMAND_LOG="$command_log" \
  MOCK_SERVER_ROOT_FILE="$server_root_file" \
  MOCK_SERVER_PID_FILE="$server_pid_file" \
  MOCK_SERVER_STOPPED_FILE="$server_stopped_file" \
  MOCK_PUSH_FAIL=1 \
  DELTAWEAVE_BIN="$mock_bin" \
  DELTAWEAVE_TEST_SIZE_BYTES=1048576 \
  DELTAWEAVE_WORK_DIR="$failure_work_dir" \
  "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/scripts/test-p2p-loopback.sh" \
  >"$failure_output" 2>&1; then
  echo 'expected failed push to return nonzero' >&2
  exit 1
fi

for _ in {1..50}; do
  [[ -s "$server_stopped_file" ]] && break
  sleep 0.02
done
[[ -s "$server_stopped_file" ]]
[[ ! -e "$failure_work_dir" ]]
server_pid="$(cat "$server_pid_file")"
if kill -0 "$server_pid" 2>/dev/null; then
  echo "receiver process survived failed transfer: $server_pid" >&2
  exit 1
fi

printf 'test-p2p-loopback: PASS\n'
