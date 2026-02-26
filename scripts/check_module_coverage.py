#!/usr/bin/env python3
"""Enforce per-module coverage thresholds from a pytest-cov JSON report."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class ModuleCoverageRule:
    module_path: str
    threshold: float


def _parse_rule(raw: str, *, default_threshold: float) -> ModuleCoverageRule:
    token = str(raw or "").strip()
    if not token:
        raise ValueError("module rule cannot be empty")
    if "=" in token:
        module_path, threshold_raw = token.split("=", 1)
        return ModuleCoverageRule(module_path=module_path.strip(), threshold=float(threshold_raw))
    return ModuleCoverageRule(module_path=token, threshold=float(default_threshold))


def _normalize_module_key(path: str) -> str:
    return str(path or "").replace("\\", "/").strip()


def _resolve_summary(files: dict[str, Any], module_path: str) -> dict[str, Any] | None:
    want = _normalize_module_key(module_path)
    if not want:
        return None

    direct = files.get(want)
    if isinstance(direct, dict):
        return direct.get("summary") if isinstance(direct.get("summary"), dict) else None

    for key, value in files.items():
        normalized = _normalize_module_key(str(key))
        if normalized == want or normalized.endswith("/" + want):
            if isinstance(value, dict) and isinstance(value.get("summary"), dict):
                return value["summary"]
    return None


def _percent_covered(summary: dict[str, Any]) -> float:
    if "percent_covered" in summary:
        return float(summary.get("percent_covered", 0.0))
    if "percent_covered_display" in summary:
        return float(summary.get("percent_covered_display", 0.0))
    covered = float(summary.get("covered_lines", 0) or 0)
    total = float(summary.get("num_statements", 0) or 0)
    if total <= 0:
        return 0.0
    return (covered / total) * 100.0


def run_check(*, coverage_json: Path, rules: list[ModuleCoverageRule]) -> tuple[bool, list[str]]:
    payload = json.loads(coverage_json.read_text(encoding="utf-8"))
    files = payload.get("files")
    if not isinstance(files, dict):
        raise ValueError("coverage JSON is missing the 'files' mapping")

    ok = True
    lines: list[str] = []
    for rule in rules:
        summary = _resolve_summary(files, rule.module_path)
        if not isinstance(summary, dict):
            ok = False
            lines.append(f"FAIL {rule.module_path}: missing from coverage report")
            continue
        percent = _percent_covered(summary)
        status = "PASS" if percent >= rule.threshold else "FAIL"
        if status == "FAIL":
            ok = False
        lines.append(f"{status} {rule.module_path}: {percent:.2f}% (threshold {rule.threshold:.2f}%)")
    return ok, lines


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check per-module coverage thresholds from coverage JSON")
    parser.add_argument(
        "--coverage-json",
        type=Path,
        default=Path("tmp/coverage.json"),
        help="Path to pytest-cov JSON output (default: tmp/coverage.json)",
    )
    parser.add_argument(
        "--default-threshold",
        type=float,
        default=85.0,
        help="Default threshold for module rules without '=threshold'",
    )
    parser.add_argument(
        "--module",
        action="append",
        default=[],
        help="Module rule in the form src/foo.py or src/foo.py=90",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    module_tokens = list(args.module or [])
    if not module_tokens:
        module_tokens = ["src/quality_gate.py=85", "src/mockup_generator.py=85"]

    try:
        rules = [_parse_rule(raw, default_threshold=args.default_threshold) for raw in module_tokens]
    except Exception as exc:
        print(f"Invalid module rule: {exc}", file=sys.stderr)
        return 2

    if not args.coverage_json.exists():
        print(f"Coverage JSON not found: {args.coverage_json}", file=sys.stderr)
        return 2

    try:
        passed, lines = run_check(coverage_json=args.coverage_json, rules=rules)
    except Exception as exc:
        print(f"Coverage check failed to run: {exc}", file=sys.stderr)
        return 2

    for line in lines:
        print(line)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
