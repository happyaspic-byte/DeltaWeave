#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

seed="${DELTAWEAVE_FAULT_SEED:-424242}"
workspace="${1:-}"
arguments=(fault-test --seed "$seed")
if [[ -n "$workspace" ]]; then
  arguments+=(--workspace "$workspace")
fi
if [[ "${DELTAWEAVE_FORCE_FAILURE:-0}" == "1" ]]; then
  arguments+=(--force-failure)
fi

exec cargo run --locked -p deltaweave -- "${arguments[@]}"
