from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from src import archiver


def _make_variant_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "file.jpg").write_bytes(b"data")


def test_archive_and_undo_flow(tmp_path: Path):
    output_root = tmp_path / "Output Covers"
    book_dir = output_root / "1. Test Book"
    _make_variant_folder(book_dir / "Variant-1")
    _make_variant_folder(book_dir / "Variant-2")
    _make_variant_folder(book_dir / "Variant-3")

    selections_path = tmp_path / "selections.json"
    selections_path.write_text(json.dumps({"1": 2}), encoding="utf-8")
    archive_log = tmp_path / "archive_log.json"

    summary = archiver.archive_non_winners(
        output_root=output_root,
        selections_path=selections_path,
        archive_log_path=archive_log,
    )
    assert summary["moved_variants"] == 2
    assert (output_root / "1. Test Book" / "Variant-2").exists()
    assert not (output_root / "1. Test Book" / "Variant-1").exists()
    assert (output_root / "Archive" / "1. Test Book" / "Variant-1").exists()

    restored = archiver.undo_archive(output_root=output_root, archive_log_path=archive_log)
    assert restored["restored_variants"] == 2
    assert (output_root / "1. Test Book" / "Variant-1").exists()
    assert (output_root / "1. Test Book" / "Variant-3").exists()


def test_archive_helpers(tmp_path: Path):
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{not-json", encoding="utf-8")

    assert archiver._load_selections(bad_json) == {}
    assert archiver._load_archive_log(bad_json) == {"operations": []}
    assert archiver._parse_book_number("12. Something") == 12
    assert archiver._parse_book_number("nope") is None
    assert archiver._parse_variant_number("Variant-5") == 5
    assert archiver._parse_variant_number("Bad") is None
    assert "T" in archiver._utc_now()


def test_archive_empty_and_undo_missing_operation(tmp_path: Path):
    output_root = tmp_path / "Output Covers"
    output_root.mkdir(parents=True, exist_ok=True)
    selections_path = tmp_path / "selections.json"
    selections_path.write_text(json.dumps({}), encoding="utf-8")
    archive_log = tmp_path / "archive_log.json"

    summary = archiver.archive_non_winners(
        output_root=output_root,
        selections_path=selections_path,
        archive_log_path=archive_log,
    )
    assert summary["moved_variants"] == 0

    missing = archiver.undo_archive(
        output_root=output_root,
        archive_log_path=archive_log,
        operation_id="does-not-exist",
    )
    assert missing["restored_variants"] == 0


def test_archive_replaces_existing_target_and_undo_overwrites_destination(tmp_path: Path):
    output_root = tmp_path / "Output Covers"
    book_dir = output_root / "2. Test Book"
    _make_variant_folder(book_dir / "Variant-1")
    _make_variant_folder(book_dir / "Variant-2")

    selections_path = tmp_path / "selections.json"
    selections_path.write_text(json.dumps({"2": 2}), encoding="utf-8")
    archive_log = tmp_path / "archive_log.json"

    existing_target = output_root / "Archive" / "2. Test Book" / "Variant-1"
    _make_variant_folder(existing_target)
    (existing_target / "stale.txt").write_text("stale", encoding="utf-8")

    summary = archiver.archive_non_winners(
        output_root=output_root,
        selections_path=selections_path,
        archive_log_path=archive_log,
    )
    assert summary["moved_variants"] == 1
    assert not (book_dir / "Variant-1").exists()
    assert (existing_target / "file.jpg").exists()
    assert not (existing_target / "stale.txt").exists()

    # Recreate destination so undo exercises overwrite path.
    _make_variant_folder(book_dir / "Variant-1")
    restored = archiver.undo_archive(output_root=output_root, archive_log_path=archive_log)
    assert restored["restored_variants"] == 1
    assert (book_dir / "Variant-1" / "file.jpg").exists()


def test_archive_skips_invalid_book_dirs_and_unselected_books(tmp_path: Path):
    output_root = tmp_path / "Output Covers"
    _make_variant_folder(output_root / "not-a-book" / "Variant-1")
    _make_variant_folder(output_root / "4. Test Book" / "Variant-1")
    _make_variant_folder(output_root / "4. Test Book" / "Variant-2")

    selections_path = tmp_path / "selections.json"
    selections_path.write_text(json.dumps({}), encoding="utf-8")
    archive_log = tmp_path / "archive_log.json"

    summary = archiver.archive_non_winners(
        output_root=output_root,
        selections_path=selections_path,
        archive_log_path=archive_log,
    )
    assert summary["moved_variants"] == 0
    assert (output_root / "4. Test Book" / "Variant-1").exists()
    assert (output_root / "4. Test Book" / "Variant-2").exists()


def test_undo_missing_log_and_missing_source_entries(tmp_path: Path):
    output_root = tmp_path / "Output Covers"
    archive_log = tmp_path / "archive_log.json"

    no_ops = archiver.undo_archive(output_root=output_root, archive_log_path=archive_log)
    assert no_ops["restored_variants"] == 0

    payload = {
        "operations": [
            {
                "operation_id": "op-1",
                "timestamp": archiver._utc_now(),
                "moves": [{"from": str(output_root / "1. A" / "Variant-1"), "to": str(output_root / "Archive" / "1. A" / "Variant-1")}],
            }
        ]
    }
    archive_log.parent.mkdir(parents=True, exist_ok=True)
    archive_log.write_text(json.dumps(payload), encoding="utf-8")
    result = archiver.undo_archive(output_root=output_root, archive_log_path=archive_log, operation_id="op-1")
    assert result["restored_variants"] == 0


def test_load_helpers_with_missing_and_nondict_payloads(tmp_path: Path):
    missing = tmp_path / "missing.json"
    assert archiver._load_selections(missing) == {}
    assert archiver._load_archive_log(missing) == {"operations": []}

    non_dict = tmp_path / "non_dict.json"
    non_dict.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    assert archiver._load_selections(non_dict) == {}
    assert archiver._load_archive_log(non_dict) == {"operations": []}


def test_main_archive_and_undo_paths(monkeypatch, tmp_path: Path):
    args = SimpleNamespace(
        output_root=tmp_path / "Output Covers",
        selections=tmp_path / "selections.json",
        archive_log=tmp_path / "archive_log.json",
        undo=False,
        operation_id=None,
    )
    monkeypatch.setattr(archiver.argparse.ArgumentParser, "parse_args", lambda self: args)
    monkeypatch.setattr(archiver, "archive_non_winners", lambda **_kwargs: {"moved_variants": 2})
    assert archiver.main() == 0

    args.undo = True
    monkeypatch.setattr(archiver, "undo_archive", lambda **_kwargs: {"restored_variants": 1})
    assert archiver.main() == 0
