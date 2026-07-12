#!/usr/bin/env python3
"""Fail when active release documentation drifts from repository automation."""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require_snippets(path: str, snippets: tuple[str, ...]) -> list[str]:
    text = (ROOT / path).read_text(encoding="utf-8")
    normalized = " ".join(text.split())
    return [
        f"{path}: missing `{snippet}`"
        for snippet in snippets
        if " ".join(snippet.split()) not in normalized
    ]


def check_dependency_versions() -> list[str]:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    version = manifest["package"]["version"]
    expected_minor = ".".join(version.split(".")[:2])
    errors: list[str] = []
    pattern = re.compile(r'trine-kv\s*=\s*\{[^\n]*version\s*=\s*"([^"]+)"')

    for path in ("README.md", "docs/usage.md", "docs/platform-io.md"):
        text = (ROOT / path).read_text(encoding="utf-8")
        versions = pattern.findall(text)
        if not versions:
            errors.append(f"{path}: no active trine-kv dependency example found")
            continue
        for documented in versions:
            if documented != expected_minor:
                errors.append(
                    f"{path}: dependency example uses {documented}; expected {expected_minor} "
                    f"for crate {version}"
                )

    release = (ROOT / "docs/release.md").read_text(encoding="utf-8")
    if f"`{expected_minor}.x`" not in release:
        errors.append(
            f"docs/release.md: current release line must be `{expected_minor}.x`"
        )
    return errors


def check_release_contract() -> list[str]:
    common_gate = (
        "python3 scripts/check_docs_drift.py",
        "cargo fmt --check",
        "cargo clippy --all-targets --all-features -- -D warnings",
        "cargo test --all-targets --all-features",
        "cargo check --target wasm32-unknown-unknown --lib",
        "cargo check --target wasm32-wasip1 --lib",
        "cargo test --target wasm32-wasip1 --lib wasi_persistent",
        "cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings",
        "cargo test --target wasm32-unknown-unknown --test browser_persistent_wasm",
        "cargo run --example quickstart",
        "cargo run --example sync_quickstart",
        "cargo run --example platform_io",
        "cargo run --example platform_io --features platform-io",
        "cargo run --example platform_io --features platform-io-native",
        "cargo run --example read_versions",
        "cargo run --example user_store",
        "cargo run --example event_index",
        "cargo package --list",
        "cargo package --locked",
    )
    maturity_gate = (
        "production_maturity forced_process_exit_recovery",
        "production_maturity concurrent_mixed_load_soak_reopens_cleanly",
    )
    errors: list[str] = []

    for path in (".github/workflows/ci.yml", ".github/workflows/publish.yml"):
        errors.extend(require_snippets(path, common_gate))
    errors.extend(require_snippets(".github/workflows/publish.yml", maturity_gate))

    release_contract = common_gate + maturity_gate + (
        ".github/workflows/production-evidence.yml",
        "Linux, macOS, and Windows",
        "Windows Platform I/O",
        "macOS Platform I/O",
        "cargo publish --dry-run --locked",
    )
    errors.extend(require_snippets("docs/release.md", release_contract))
    errors.extend(
        require_snippets(
            "docs/production-readiness.md",
            (".github/workflows/production-evidence.yml", "Linux, macOS, and Windows"),
        )
    )
    return errors


def main() -> int:
    errors = check_dependency_versions() + check_release_contract()
    if errors:
        print("documentation drift detected:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("documentation and release automation are aligned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
