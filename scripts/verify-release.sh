#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

command -v cargo >/dev/null

echo '[1/8] rustfmt'
cargo fmt --all -- --check

echo '[2/8] clippy'
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

echo '[3/8] tests'
cargo test --locked --workspace --all-targets --all-features

echo '[4/8] rustdoc'
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps --all-features

echo '[5/8] packaged CLI self-test'
self_test_output="$(
  RUST_LOG="${DELTAWEAVE_VERIFY_RUST_LOG:-warn,netwatch=error}" \
    cargo run --locked -p deltaweave -- self-test
)"
grep -Fq '"status": "pass"' <<< "$self_test_output"
grep -Fq '"sync_bidirectional_verified": true' <<< "$self_test_output"
grep -Fq '"sync_delete_verified": true' <<< "$self_test_output"
grep -Fq '"sync_restart_actions": 0' <<< "$self_test_output"
printf '%s\n' "$self_test_output"

echo '[6/8] documentation media and shell syntax'
bash -n scripts/render-doc-visuals.sh
test -s docs/assets/deltaweave-hero.webp
test -s docs/assets/deltaweave-quickstart.gif
test -s docs/assets/deltaweave-index-lifecycle.gif
test -s docs/assets/deltaweave-sync-lifecycle.gif
test -s docs/assets/portainer-flow.svg

echo '[7/8] patch hygiene'
git diff --check

echo '[8/8] Portainer Compose'
if command -v docker >/dev/null && docker compose version >/dev/null 2>&1; then
  DELTAWEAVE_ALLOWED_PEER=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  DELTAWEAVE_DATA_DIR=/tmp/deltaweave-verify \
  PUID=65532 \
  PGID=65532 \
    docker compose -f deploy/portainer/compose.yml config --quiet
else
  echo 'SKIP: Docker Compose is unavailable; GitHub Container CI performs this gate.'
fi

echo 'DeltaWeave release verification: PASS'
