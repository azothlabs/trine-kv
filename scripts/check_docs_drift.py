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


def check_local_markdown_links(path: str) -> list[str]:
    source = ROOT / path
    text = source.read_text(encoding="utf-8")
    links = re.findall(r"\[[^\]]+\]\(([^)]+)\)", text)
    errors: list[str] = []
    for link in links:
        if link.startswith(("http://", "https://", "#")):
            continue
        target = link.split("#", 1)[0]
        if target and not (source.parent / target).exists():
            errors.append(f"{path}: local link target does not exist: {link}")
    return errors


def check_release_contract() -> list[str]:
    lockfile = tomllib.loads((ROOT / "Cargo.lock").read_text(encoding="utf-8"))
    wasm_bindgen_version = next(
        package["version"]
        for package in lockfile["package"]
        if package["name"] == "wasm-bindgen"
    )
    common_gate = (
        "python3 scripts/check_docs_drift.py",
        "cargo fmt --check",
        "cargo clippy --all-targets --all-features -- -D warnings",
        "cargo test --all-targets --all-features",
        "cargo check --target wasm32-unknown-unknown --lib",
        "cargo check --target wasm32-wasip1 --lib",
        "cargo test --target wasm32-wasip1 --lib wasi_persistent",
        "cargo clippy --target wasm32-unknown-unknown --lib -- -D warnings",
        (
            "cargo test --target wasm32-unknown-unknown "
            "--test browser_persistent_wasm "
            "--test browser_dedicated_worker_wasm "
            "--test browser_shared_worker_wasm"
        ),
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
    destructive_gate = ("cargo test -q destructive_ --lib -- --test-threads=1",)
    wasi_warning_gate = (
        'RUSTFLAGS="-D warnings" cargo check --target wasm32-wasip1 --lib',
    )
    wasm_runner_install = (
        "rustup toolchain install 1.88.0 --profile minimal",
        f"cargo +1.88.0 install wasm-bindgen-cli --version {wasm_bindgen_version} --locked",
    )
    package_exclusions = (
        "benches|docs|tests",
        "crate package contains repository-only files",
    )
    errors: list[str] = []

    for path in (".github/workflows/ci.yml", ".github/workflows/publish.yml"):
        errors.extend(require_snippets(path, common_gate))
        errors.extend(require_snippets(path, wasi_warning_gate))
        errors.extend(require_snippets(path, wasm_runner_install))
        errors.extend(require_snippets(path, package_exclusions))
    errors.extend(
        require_snippets(
            "docs/release.md",
            (f"wasm-bindgen-cli {wasm_bindgen_version}", "Rust 1.88", "Rust 1.85"),
        )
    )
    errors.extend(require_snippets("docs/release.md", wasi_warning_gate))

    production = (ROOT / ".github/workflows/production-evidence.yml").read_text(
        encoding="utf-8"
    )
    matrix_report = "production-maturity-${{ matrix.os }}"
    if production.count(matrix_report) != 4:
        errors.append(
            ".github/workflows/production-evidence.yml: maturity report and artifact "
            "names must consistently use `matrix.os`"
        )
    if "production-maturity-${{ runner.os }}" in production:
        errors.append(
            ".github/workflows/production-evidence.yml: `runner.os` is unavailable in "
            "job-level report environment expressions"
        )
    errors.extend(require_snippets(".github/workflows/publish.yml", maturity_gate))
    for path in (
        ".github/workflows/production-evidence.yml",
        ".github/workflows/publish.yml",
        "docs/production-readiness.md",
        "docs/release.md",
    ):
        errors.extend(require_snippets(path, destructive_gate))

    publish = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    version_step = publish.partition("- name: Verify requested version")[2].partition(
        "- name: Check documentation drift"
    )[0]
    for snippet in ("cargo_version=", "CHANGELOG.md is missing an entry"):
        if snippet not in version_step:
            errors.append(
                ".github/workflows/publish.yml: Verify requested version step must contain "
                f"`{snippet}`"
            )
    if not re.search(
        r"- name: Check documentation drift\s+run: python3 scripts/check_docs_drift\.py",
        publish,
    ):
        errors.append(
            ".github/workflows/publish.yml: documentation drift step has no direct run command"
        )

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
    errors.extend(
        require_snippets(
            "README.md",
            (
                "Production Status",
                "pre-`1.0`",
                "docs/production-readiness.md",
                ".github/workflows/production-evidence.yml",
                "Linux, macOS, and Windows",
                "sudden power loss",
                "disk-full behavior",
                "online backup",
                "deterministic I/O failure injection",
                "python3 scripts/check_docs_drift.py",
            ),
        )
    )
    return errors


def main() -> int:
    errors = (
        check_dependency_versions()
        + check_release_contract()
        + check_local_markdown_links("README.md")
    )
    if errors:
        print("documentation drift detected:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print("documentation and release automation are aligned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
