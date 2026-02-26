from __future__ import annotations

from pathlib import Path

import pytest

from src import safe_json


def test_load_json_missing_file_returns_default(tmp_path: Path):
    path = tmp_path / "missing.json"
    default = {"items": []}
    assert safe_json.load_json(path, default) == default


def test_atomic_write_json_round_trip(tmp_path: Path):
    path = tmp_path / "payload.json"
    payload = {"a": 1, "b": ["x", "y"]}
    safe_json.atomic_write_json(path, payload)
    assert path.exists()
    assert safe_json.load_json(path, {}) == payload


def test_load_json_corrupt_returns_default(tmp_path: Path):
    path = tmp_path / "broken.json"
    path.write_text("{bad json", encoding="utf-8")
    assert safe_json.load_json(path, {"ok": False}) == {"ok": False}


def test_update_json_mutates_and_persists(tmp_path: Path):
    path = tmp_path / "data.json"
    safe_json.atomic_write_json(path, {"count": 1})

    def mutator(payload):
        payload["count"] = payload.get("count", 0) + 1
        return payload

    updated = safe_json.update_json(path, mutator, default={"count": 0})
    assert updated["count"] == 2
    assert safe_json.load_json(path, {})["count"] == 2


def test_atomic_write_many_json(tmp_path: Path):
    one = tmp_path / "one.json"
    two = tmp_path / "two.json"
    safe_json.atomic_write_many_json(
        [
            (one, {"value": 1}),
            (two, {"value": 2}),
        ]
    )
    assert safe_json.load_json(one, {})["value"] == 1
    assert safe_json.load_json(two, {})["value"] == 2


def test_atomic_write_many_json_cleanup_ignores_remove_errors(tmp_path: Path, monkeypatch):
    one = tmp_path / "one.json"
    two = tmp_path / "two.json"

    original_replace = safe_json.os.replace
    original_remove = safe_json.os.remove

    calls = {"replace": 0, "remove": 0}

    def _replace(src, dst):  # type: ignore[no-untyped-def]
        calls["replace"] += 1
        if calls["replace"] == 1:
            raise RuntimeError("replace failed")
        return original_replace(src, dst)

    def _remove(path):  # type: ignore[no-untyped-def]
        calls["remove"] += 1
        raise OSError("remove failed")

    monkeypatch.setattr(safe_json.os, "replace", _replace)
    monkeypatch.setattr(safe_json.os, "remove", _remove)

    with pytest.raises(RuntimeError):
        safe_json.atomic_write_many_json([(one, {"value": 1}), (two, {"value": 2})])

    assert calls["remove"] >= 1
    # Restore explicitly to keep this test isolated if monkeypatch teardown order changes.
    monkeypatch.setattr(safe_json.os, "replace", original_replace)
    monkeypatch.setattr(safe_json.os, "remove", original_remove)


def test_atomic_write_many_json_rolls_back_existing_files_on_partial_failure(tmp_path: Path, monkeypatch):
    one = tmp_path / "one.json"
    two = tmp_path / "two.json"
    safe_json.atomic_write_json(one, {"value": "old-one"})
    safe_json.atomic_write_json(two, {"value": "old-two"})

    original_replace = safe_json.os.replace
    calls = {"commit_replaces": 0}

    def _replace(src, dst):  # type: ignore[no-untyped-def]
        src_path = str(src)
        dst_path = str(dst)
        if src_path.endswith(".tmp") and dst_path.endswith("two.json"):
            calls["commit_replaces"] += 1
            raise RuntimeError("simulated commit failure on second file")
        return original_replace(src, dst)

    monkeypatch.setattr(safe_json.os, "replace", _replace)
    with pytest.raises(RuntimeError, match="simulated commit failure"):
        safe_json.atomic_write_many_json(
            [
                (one, {"value": "new-one"}),
                (two, {"value": "new-two"}),
            ]
        )

    assert safe_json.load_json(one, {})["value"] == "old-one"
    assert safe_json.load_json(two, {})["value"] == "old-two"
    assert calls["commit_replaces"] == 1


def test_atomic_write_many_json_rolls_back_new_file_on_partial_failure(tmp_path: Path, monkeypatch):
    one = tmp_path / "one.json"
    two = tmp_path / "two.json"
    safe_json.atomic_write_json(one, {"value": "old-one"})
    assert not two.exists()

    original_replace = safe_json.os.replace

    def _replace(src, dst):  # type: ignore[no-untyped-def]
        src_path = str(src)
        dst_path = str(dst)
        if src_path.endswith(".tmp") and dst_path.endswith("two.json"):
            raise RuntimeError("simulated commit failure on new file")
        return original_replace(src, dst)

    monkeypatch.setattr(safe_json.os, "replace", _replace)
    with pytest.raises(RuntimeError, match="simulated commit failure"):
        safe_json.atomic_write_many_json(
            [
                (one, {"value": "new-one"}),
                (two, {"value": "new-two"}),
            ]
        )

    assert safe_json.load_json(one, {})["value"] == "old-one"
    assert not two.exists()
