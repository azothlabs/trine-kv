#!/usr/bin/env python3
"""Compare paired Trine benchmark summaries from the same runner."""

from __future__ import annotations

import argparse
import csv
import io
import sys
from dataclasses import dataclass
from pathlib import Path


REQUIRED_ROWS = (
    "single-key put",
    "batch write",
    "random get",
    "bounded range scan",
    "prefix scan",
    "WAL replay",
    "flush throughput",
    "compaction throughput",
    "separated blob values",
    "cold table read-only",
)


@dataclass(frozen=True)
class Comparison:
    name: str
    baseline_us: int
    current_us: int
    delta_us: int
    delta_percent: float
    regressed: bool


def read_summary(path: Path) -> dict[str, int]:
    lines = path.read_text(encoding="utf-8").splitlines()
    try:
        header_index = next(
            index for index, line in enumerate(lines) if line.startswith("group,name,runs,")
        )
    except StopIteration as error:
        raise ValueError(f"{path} does not contain a grouped benchmark CSV header") from error

    reader = csv.DictReader(io.StringIO("\n".join(lines[header_index:])))
    rows: dict[str, int] = {}
    for row in reader:
        name = row.get("name")
        elapsed = row.get("elapsed_us_median")
        if name and elapsed:
            rows[name] = int(elapsed)
    return rows


def compare(
    baseline: dict[str, int],
    current: dict[str, int],
    required_rows: tuple[str, ...],
    max_regression_percent: float,
    absolute_noise_us: int,
) -> list[Comparison]:
    missing = [
        name for name in required_rows if name not in baseline or name not in current
    ]
    if missing:
        raise ValueError(f"missing required benchmark rows: {', '.join(missing)}")

    comparisons = []
    for name in required_rows:
        baseline_us = baseline[name]
        current_us = current[name]
        delta_us = current_us - baseline_us
        if baseline_us == 0:
            delta_percent = 0.0 if current_us == 0 else float("inf")
        else:
            delta_percent = delta_us * 100.0 / baseline_us
        regressed = (
            delta_us > absolute_noise_us
            and delta_percent > max_regression_percent
        )
        comparisons.append(
            Comparison(
                name=name,
                baseline_us=baseline_us,
                current_us=current_us,
                delta_us=delta_us,
                delta_percent=delta_percent,
                regressed=regressed,
            )
        )
    return comparisons


def markdown_report(
    comparisons: list[Comparison],
    max_regression_percent: float,
    absolute_noise_us: int,
) -> str:
    lines = [
        "# Trine KV paired performance comparison",
        "",
        (
            f"Gate: regression must exceed both {max_regression_percent:.1f}% "
            f"and {absolute_noise_us} us."
        ),
        "",
        "| benchmark | baseline median us | current median us | delta | result |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for item in comparisons:
        result = "FAIL" if item.regressed else "pass"
        lines.append(
            f"| {item.name} | {item.baseline_us} | {item.current_us} | "
            f"{item.delta_us:+} ({item.delta_percent:+.1f}%) | {result} |"
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-regression-percent", type=float, default=20.0)
    parser.add_argument("--absolute-noise-us", type=int, default=500)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        comparisons = compare(
            read_summary(args.baseline),
            read_summary(args.current),
            REQUIRED_ROWS,
            args.max_regression_percent,
            args.absolute_noise_us,
        )
    except (OSError, ValueError) as error:
        print(f"benchmark comparison error: {error}", file=sys.stderr)
        return 2

    report = markdown_report(
        comparisons, args.max_regression_percent, args.absolute_noise_us
    )
    print(report, end="")
    if args.output:
        args.output.write_text(report, encoding="utf-8")
    return 1 if any(item.regressed for item in comparisons) else 0


if __name__ == "__main__":
    raise SystemExit(main())
