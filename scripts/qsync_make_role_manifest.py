#!/usr/bin/env python3
"""Combine platform artifact provenance for the F role drivers.

The Windows and Linux executables are intentionally allowed to have different
hashes.  Every role descriptor must still bind to the selected source and
workflow SHA; this file does not accept a source-only assertion.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any


SOURCE_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def fail(error_class: str) -> None:
    print(f"QSYNC_F_MANIFEST|ok=false|error_class={error_class}")
    raise SystemExit(1)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        fail("binary_missing")
    return digest.hexdigest()


def required_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or not path.is_file():
        fail("binary_missing")
    return path


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        fail("manifest_invalid")
    if not isinstance(value, dict):
        fail("manifest_invalid")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="create role-specific F artifact manifest")
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--windows-manifest", required=True, type=Path)
    parser.add_argument("--windows-binary", required=True, type=Path)
    parser.add_argument("--linux-binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not SOURCE_SHA_RE.fullmatch(args.source_sha):
        fail("source_mismatch")
    windows_manifest = load_json(required_path(str(args.windows_manifest)))
    if windows_manifest.get("status") != "pass":
        fail("manifest_invalid")
    if windows_manifest.get("source_sha") != args.source_sha or windows_manifest.get("workflow_sha") != args.source_sha:
        fail("source_mismatch")
    windows_artifact = windows_manifest.get("artifact")
    if not isinstance(windows_artifact, dict):
        fail("manifest_invalid")
    windows_hash = windows_artifact.get("sha256")
    windows_size = windows_artifact.get("size_bytes")
    if not isinstance(windows_hash, str) or not SHA256_RE.fullmatch(windows_hash):
        fail("manifest_invalid")
    if not isinstance(windows_size, int) or windows_size <= 0:
        fail("manifest_invalid")
    windows_binary = required_path(str(args.windows_binary))
    if sha256(windows_binary) != windows_hash or windows_binary.stat().st_size != windows_size:
        fail("binary_hash_mismatch")
    linux_binary = required_path(str(args.linux_binary))
    linux_hash = sha256(linux_binary)
    linux_size = linux_binary.stat().st_size
    if linux_size <= 0:
        fail("binary_missing")
    manifest = {
        "status": "pass",
        "source_sha": args.source_sha,
        "workflow_sha": args.source_sha,
        "artifacts": {
            "owner": {
                "source_sha": args.source_sha,
                "workflow_sha": args.source_sha,
                "target": "linux",
                "sha256": linux_hash,
                "size_bytes": linux_size,
            },
            "rw_provider": {
                "source_sha": args.source_sha,
                "workflow_sha": args.source_sha,
                "target": "windows",
                "sha256": windows_hash,
                "size_bytes": windows_size,
            },
            "ro_consumer": {
                "source_sha": args.source_sha,
                "workflow_sha": args.source_sha,
                "target": "linux",
                "sha256": linux_hash,
                "size_bytes": linux_size,
            },
        },
    }
    try:
        if not args.output.is_absolute() or args.output.exists():
            fail("config_invalid")
        args.output.parent.mkdir(parents=False, exist_ok=True)
        args.output.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        os.chmod(args.output, 0o600)
    except SystemExit:
        raise
    except OSError:
        fail("config_invalid")
    print(
        "QSYNC_F_MANIFEST|ok=true|source_sha="
        + args.source_sha
        + "|linux_sha256="
        + linux_hash
        + "|windows_sha256="
        + windows_hash
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
