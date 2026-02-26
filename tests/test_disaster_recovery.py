from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from src import disaster_recovery as dr


def _runtime(tmp_path: Path):
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "Output Covers"
    config_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(
        project_root=tmp_path,
        config_dir=config_dir,
        data_dir=data_dir,
        output_dir=output_dir,
        catalog_id="classics",
    )


def test_create_snapshot_and_validate(tmp_path: Path):
    runtime = _runtime(tmp_path)
    (runtime.config_dir / "book_catalog.json").write_text(json.dumps([{"number": 1}]), encoding="utf-8")
    (runtime.data_dir / "winner_selections.json").write_text(json.dumps({"1": 2}), encoding="utf-8")
    (runtime.output_dir / "Book One" / "Variant-2").mkdir(parents=True, exist_ok=True)
    (runtime.output_dir / "Book One" / "Variant-2" / "cover.jpg").write_bytes(b"jpg-bytes")

    summary = dr.create_snapshot(runtime=runtime, snapshot_root=tmp_path / "snapshots")
    assert summary.snapshot_dir.exists()
    assert summary.files_copied >= 2

    validate_ok = dr.validate_snapshot(summary.snapshot_dir)
    assert validate_ok["ok"] is True
    assert validate_ok["checked"] >= 1

    # Tamper one file and ensure validation fails.
    target = summary.snapshot_dir / "config" / "book_catalog.json"
    target.write_text("{}", encoding="utf-8")
    validate_bad = dr.validate_snapshot(summary.snapshot_dir)
    assert validate_bad["ok"] is False
    assert validate_bad["mismatches"] >= 1


def test_restore_snapshot_force_and_skip(tmp_path: Path):
    runtime = _runtime(tmp_path)
    cfg_file = runtime.config_dir / "settings.json"
    data_file = runtime.data_dir / "jobs.sqlite3"
    cfg_file.write_text(json.dumps({"a": 1}), encoding="utf-8")
    data_file.write_bytes(b"sqlite")

    summary = dr.create_snapshot(runtime=runtime, snapshot_root=tmp_path / "snapshots")
    snap = summary.snapshot_dir

    # Remove file and restore.
    cfg_file.unlink()
    result = dr.restore_snapshot(snapshot_dir=snap, runtime=runtime, force=False)
    assert result["ok"] is True
    assert cfg_file.exists()

    # Existing files are skipped unless force=True.
    result_skip = dr.restore_snapshot(snapshot_dir=snap, runtime=runtime, force=False)
    assert result_skip["skipped_existing"] >= 1
    result_force = dr.restore_snapshot(snapshot_dir=snap, runtime=runtime, force=True)
    assert result_force["restored_files"] >= 1


def test_snapshot_helpers_and_cli_main(tmp_path: Path, monkeypatch, capsys):
    runtime = _runtime(tmp_path)
    assert dr._should_copy_file(Path("a.json")) is True
    assert dr._should_copy_file(Path("a.bin")) is False

    # invalid validate input format
    bad = tmp_path / "bad_snapshot"
    bad.mkdir(parents=True, exist_ok=True)
    (bad / "manifest.json").write_text("{}", encoding="utf-8")
    res = dr.validate_snapshot(bad)
    assert res["ok"] is True  # empty manifest items means nothing to mismatch

    # main snapshot branch
    monkeypatch.setattr(dr.config, "get_config", lambda: runtime)
    monkeypatch.setattr(
        dr.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(command="snapshot", snapshot_root=tmp_path / "snaps"),
    )
    assert dr.main() == 0
    assert "snapshot_dir" in capsys.readouterr().out


def test_snapshot_source_filters_and_output_index(tmp_path: Path):
    runtime = _runtime(tmp_path)
    missing_runtime = SimpleNamespace(
        config_dir=tmp_path / "missing-config",
        data_dir=tmp_path / "missing-data",
    )
    assert dr._iter_snapshot_sources(missing_runtime) == []

    keep_cfg = runtime.config_dir / "keep.json"
    skip_cfg_dir = runtime.config_dir / "nested"
    skip_cfg_dir.mkdir(parents=True, exist_ok=True)
    keep_cfg.write_text("{}", encoding="utf-8")
    (skip_cfg_dir / "skip.bin").write_bytes(b"bin")
    (runtime.data_dir / "snapshots" / "old.json").parent.mkdir(parents=True, exist_ok=True)
    (runtime.data_dir / "snapshots" / "old.json").write_text("{}", encoding="utf-8")
    (runtime.data_dir / "notes.txt").write_text("note", encoding="utf-8")
    rows = dr._iter_snapshot_sources(runtime)
    rels = {str(path.relative_to(base)) for _, base, path in rows}
    assert "keep.json" in rels
    assert "notes.txt" in rels
    assert "snapshots/old.json" not in rels
    assert "nested/skip.bin" not in rels

    # Output index should ignore non-image/export files.
    (runtime.output_dir / "Book One" / "Variant-1").mkdir(parents=True, exist_ok=True)
    (runtime.output_dir / "Book One" / "Variant-1" / "cover.jpg").write_bytes(b"jpg")
    (runtime.output_dir / "Book One" / "Variant-1" / "notes.txt").write_text("skip", encoding="utf-8")
    output_index = dr._build_output_index(runtime.output_dir)
    assert output_index["total_files"] == 1


def test_create_snapshot_collision_and_validate_edge_cases(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    (runtime.config_dir / "book_catalog.json").write_text("[]", encoding="utf-8")

    # Force snapshot id collision.
    collision_root = tmp_path / "snapshots"
    existing = collision_root / "20260222-000000"
    existing.mkdir(parents=True, exist_ok=True)

    class _FixedDateTime:
        @staticmethod
        def now(_tz=None):  # type: ignore[no-untyped-def]
            class _Value:
                def strftime(self, _fmt):  # type: ignore[no-untyped-def]
                    return "20260222-000000"

            return _Value()

    monkeypatch.setattr(dr, "datetime", _FixedDateTime)
    try:
        raised = False
        try:
            dr.create_snapshot(runtime=runtime, snapshot_root=collision_root)
        except FileExistsError:
            raised = True
        assert raised is True
    finally:
        monkeypatch.undo()

    invalid_manifest = tmp_path / "invalid_manifest"
    invalid_manifest.mkdir(parents=True, exist_ok=True)
    (invalid_manifest / "manifest.json").write_text(json.dumps("bad"), encoding="utf-8")
    bad_result = dr.validate_snapshot(invalid_manifest)
    assert bad_result["ok"] is False
    assert bad_result["error"] == "Invalid manifest format"

    partial_manifest = tmp_path / "partial_manifest"
    partial_manifest.mkdir(parents=True, exist_ok=True)
    (partial_manifest / "manifest.json").write_text(
        json.dumps({"items": ["not-dict", {"path": "", "sha256": ""}, {"path": "missing.bin", "sha256": "abc"}]}),
        encoding="utf-8",
    )
    mismatch_result = dr.validate_snapshot(partial_manifest)
    assert mismatch_result["ok"] is False
    assert mismatch_result["checked"] == 1
    assert mismatch_result["mismatches"] == 1


def test_restore_snapshot_missing_scope_and_main_validate_restore(tmp_path: Path, monkeypatch):
    runtime = _runtime(tmp_path)
    snapshot_dir = tmp_path / "snapshot"
    (snapshot_dir / "config" / "nested").mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "config" / "nested" / "settings.json").write_text("{}", encoding="utf-8")
    # data scope intentionally missing to hit "continue" branch.
    restore_result = dr.restore_snapshot(snapshot_dir=snapshot_dir, runtime=runtime, force=False)
    assert restore_result["ok"] is True
    assert restore_result["config_files"] == 1

    monkeypatch.setattr(dr.config, "get_config", lambda: runtime)
    (snapshot_dir / "manifest.json").write_text(json.dumps("bad"), encoding="utf-8")
    monkeypatch.setattr(
        dr.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(command="validate", snapshot=snapshot_dir),
    )
    assert dr.main() == 1

    monkeypatch.setattr(
        dr.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(command="restore", snapshot=snapshot_dir, force=True),
    )
    assert dr.main() == 0

    monkeypatch.setattr(
        dr.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(command="unknown"),
    )
    assert dr.main() == 1
