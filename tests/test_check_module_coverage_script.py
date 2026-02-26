from __future__ import annotations

import json
from pathlib import Path

from scripts import check_module_coverage as cmc


def _write_report(path: Path, files: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"files": files}), encoding="utf-8")


def test_parse_rule_supports_explicit_and_default_threshold():
    explicit = cmc._parse_rule("src/mockup_generator.py=91.5", default_threshold=85.0)
    assert explicit.module_path == "src/mockup_generator.py"
    assert explicit.threshold == 91.5

    defaulted = cmc._parse_rule("src/quality_gate.py", default_threshold=87.0)
    assert defaulted.module_path == "src/quality_gate.py"
    assert defaulted.threshold == 87.0


def test_run_check_passes_when_modules_meet_thresholds(tmp_path: Path):
    report = tmp_path / "coverage.json"
    _write_report(
        report,
        {
            "src/quality_gate.py": {"summary": {"percent_covered": 90.2}},
            "src/mockup_generator.py": {"summary": {"percent_covered": 88.0}},
        },
    )

    ok, lines = cmc.run_check(
        coverage_json=report,
        rules=[
            cmc.ModuleCoverageRule("src/quality_gate.py", 85.0),
            cmc.ModuleCoverageRule("src/mockup_generator.py", 85.0),
        ],
    )
    assert ok is True
    assert all(line.startswith("PASS ") for line in lines)


def test_run_check_fails_for_low_or_missing_modules(tmp_path: Path):
    report = tmp_path / "coverage.json"
    _write_report(
        report,
        {
            "src/quality_gate.py": {"summary": {"percent_covered": 82.0}},
        },
    )

    ok, lines = cmc.run_check(
        coverage_json=report,
        rules=[
            cmc.ModuleCoverageRule("src/quality_gate.py", 85.0),
            cmc.ModuleCoverageRule("src/mockup_generator.py", 85.0),
        ],
    )
    assert ok is False
    assert "FAIL src/quality_gate.py" in lines[0]
    assert "missing from coverage report" in lines[1]


def test_main_uses_default_rules(tmp_path: Path):
    report = tmp_path / "coverage.json"
    _write_report(
        report,
        {
            "src/quality_gate.py": {"summary": {"percent_covered": 85.0}},
            "src/mockup_generator.py": {"summary": {"percent_covered": 85.0}},
        },
    )
    assert cmc.main(["--coverage-json", str(report)]) == 0
