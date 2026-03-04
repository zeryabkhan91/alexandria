from __future__ import annotations

import os
from pathlib import Path
import time

from src import drive_manager


def _seed_file(path: Path, content: bytes = b"x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_push_to_local_mirror_and_status(tmp_path: Path):
    input_root = tmp_path / "Input Covers"
    output_root = tmp_path / "Output Covers"
    exports_root = tmp_path / "exports"
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")
    mirror = tmp_path / "drive"
    drive_id = f"local:{mirror}"

    _seed_file(input_root / "1. Book/cover.jpg", b"input")
    _seed_file(output_root / "1. Book/Variant-1/cover.jpg", b"winner")
    _seed_file(output_root / "Mockups/1. Book/standing_front.jpg", b"mock")
    _seed_file(exports_root / "amazon/classics/1/01_main_cover.jpg", b"exp")

    pushed = drive_manager.push_to_drive(
        output_root=output_root,
        input_root=input_root,
        exports_root=exports_root,
        drive_folder_id=drive_id,
        credentials_path=credentials,
        selected_relative_files=["1. Book/Variant-1/cover.jpg"],
    )
    assert pushed["mode"] == "local_mirror"
    assert pushed["uploaded"] >= 1

    status = drive_manager.get_status(
        output_root=output_root,
        input_root=input_root,
        exports_root=exports_root,
        drive_folder_id=drive_id,
        credentials_path=credentials,
        last_sync={"mode": "push"},
    )
    assert status["connection"] == "connected"
    assert "pending_changes" in status


def test_pull_from_local_mirror(tmp_path: Path):
    input_root = tmp_path / "Input Covers"
    mirror = tmp_path / "mirror"
    source = mirror / "Alexandria Covers" / "Input Covers" / "2. Book" / "cover.jpg"
    _seed_file(source, b"from_drive")

    pulled = drive_manager.pull_from_drive(
        input_root=input_root,
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "credentials.json",
    )
    assert pulled["downloaded"] == 1
    assert (input_root / "2. Book" / "cover.jpg").exists()


def test_push_to_google_api_path_uses_selected_files(tmp_path: Path, monkeypatch):
    input_root = tmp_path / "Input Covers"
    output_root = tmp_path / "Output Covers"
    exports_root = tmp_path / "exports"
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")

    selected = output_root / "1. Book" / "Variant-1" / "cover.jpg"
    _seed_file(selected, b"winner")
    _seed_file(exports_root / "amazon/classics/1/01_main_cover.jpg", b"exp")

    captured: dict[str, object] = {}

    def _fake_sync_to_drive(**kwargs):
        captured.update(kwargs)
        return {
            "mode": "google_api",
            "uploaded": 1,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "progress": [{"file": "1. Book/Variant-1/cover.jpg", "status": "uploaded"}],
        }

    monkeypatch.setattr(drive_manager.gdrive_sync, "sync_to_drive", _fake_sync_to_drive)
    sync_state_path = tmp_path / "gdrive_sync_state_demo.json"

    payload = drive_manager.push_to_drive(
        output_root=output_root,
        input_root=input_root,
        exports_root=exports_root,
        drive_folder_id="remote-folder-id",
        credentials_path=credentials,
        sync_state_path=sync_state_path,
        selected_relative_files=["1. Book/Variant-1/cover.jpg", "missing/file.jpg"],
    )
    assert payload["mode"] == "google_api"
    assert payload["direction"] == "push"
    assert payload["uploaded"] == 1
    files = captured.get("files")
    assert isinstance(files, list)
    assert len(files) == 1
    assert files[0] == selected.resolve()
    assert captured.get("sync_state_path") == sync_state_path


def test_pull_from_google_api_returns_explicit_not_supported_error(tmp_path: Path):
    pulled = drive_manager.pull_from_drive(
        input_root=tmp_path / "Input Covers",
        drive_folder_id="remote-folder-id",
        credentials_path=tmp_path / "credentials.json",
    )
    assert pulled["mode"] == "google_api"
    assert pulled["direction"] == "pull"
    assert pulled["failed"] == 1
    assert "only supported" in pulled["errors"][0]["error"].lower()


def test_sync_bidirectional_aggregates_pull_and_push(tmp_path: Path):
    input_root = tmp_path / "Input Covers"
    output_root = tmp_path / "Output Covers"
    exports_root = tmp_path / "exports"
    mirror = tmp_path / "drive"
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")
    drive_id = f"local:{mirror}"

    _seed_file(mirror / "Alexandria Covers" / "Input Covers" / "2. Book" / "cover.jpg", b"from-drive")
    _seed_file(output_root / "1. Book" / "Variant-1" / "cover.jpg", b"winner")
    _seed_file(exports_root / "web/classics/manifest.json", b"{}")

    sync_state_path = tmp_path / "gdrive_sync_state_demo.json"
    payload = drive_manager.sync_bidirectional(
        output_root=output_root,
        input_root=input_root,
        exports_root=exports_root,
        drive_folder_id=drive_id,
        credentials_path=credentials,
        sync_state_path=sync_state_path,
    )
    assert payload["direction"] == "bidirectional"
    assert payload["downloaded"] >= 1
    assert payload["uploaded"] >= 1
    assert payload["failed"] == 0
    assert (input_root / "2. Book" / "cover.jpg").exists()


def test_sync_bidirectional_forwards_sync_state_path(tmp_path: Path, monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        drive_manager,
        "pull_from_drive",
        lambda **_kwargs: {
            "mode": "google_api",
            "direction": "pull",
            "downloaded": 0,
            "skipped": 0,
            "conflicts": 0,
            "failed": 0,
            "errors": [],
        },
    )

    def _fake_push_to_drive(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return {
            "mode": "google_api",
            "direction": "push",
            "uploaded": 1,
            "downloaded": 0,
            "skipped": 0,
            "conflicts": 0,
            "failed": 0,
            "errors": [],
            "changes": [],
        }

    monkeypatch.setattr(drive_manager, "push_to_drive", _fake_push_to_drive)
    sync_state = tmp_path / "gdrive_sync_state_demo.json"

    payload = drive_manager.sync_bidirectional(
        output_root=tmp_path / "Output Covers",
        input_root=tmp_path / "Input Covers",
        exports_root=tmp_path / "exports",
        drive_folder_id="remote-folder-id",
        credentials_path=tmp_path / "credentials.json",
        sync_state_path=sync_state,
    )

    assert payload["direction"] == "bidirectional"
    assert captured.get("sync_state_path") == sync_state


def test_pull_from_local_mirror_conflict_keeps_local_when_newer(tmp_path: Path):
    input_root = tmp_path / "Input Covers"
    mirror = tmp_path / "mirror"
    src = mirror / "Alexandria Covers" / "Input Covers" / "4. Book" / "cover.jpg"
    dst = input_root / "4. Book" / "cover.jpg"
    _seed_file(src, b"drive")
    _seed_file(dst, b"local")

    now = time.time()
    os.utime(src, (now - 30, now - 30))
    os.utime(dst, (now, now))

    payload = drive_manager.pull_from_drive(
        input_root=input_root,
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "credentials.json",
    )
    assert payload["downloaded"] == 0
    assert payload["conflicts"] == 1
    assert payload["skipped"] == 1
    assert any(change.get("status") == "conflict_keep_local" for change in payload["changes"])


def test_get_status_google_api_error_path(tmp_path: Path, monkeypatch):
    def _boom(**_kwargs):
        raise RuntimeError("drive offline")

    monkeypatch.setattr(drive_manager.gdrive_sync, "get_sync_status", _boom)

    status = drive_manager.get_status(
        output_root=tmp_path / "Output Covers",
        input_root=tmp_path / "Input Covers",
        exports_root=tmp_path / "exports",
        drive_folder_id="remote-folder-id",
        credentials_path=tmp_path / "credentials.json",
        last_sync={"mode": "push"},
    )
    assert status["mode"] == "google_api"
    assert status["connection"] == "error"
    assert status["status"]["mode"] == "unavailable"


def test_safe_resolved_child_blocks_path_traversal(tmp_path: Path):
    output_root = tmp_path / "Output Covers"
    output_root.mkdir(parents=True, exist_ok=True)
    outside = tmp_path / "outside.jpg"
    outside.write_bytes(b"x")

    allowed = drive_manager._safe_resolved_child(output_root, "Book/cover.jpg")
    blocked = drive_manager._safe_resolved_child(output_root, "../outside.jpg")
    empty = drive_manager._safe_resolved_child(output_root, "")

    assert allowed is not None
    assert blocked is None
    assert empty is None


def test_copy_newer_status_paths_and_error_branch(tmp_path: Path, monkeypatch):
    summary = drive_manager.SyncSummary(mode="local_mirror", direction="push", errors=[], changes=[])

    src = tmp_path / "src.jpg"
    dst = tmp_path / "dst.jpg"
    src.write_bytes(b"a")
    dst.write_bytes(b"a")
    now = time.time()
    os.utime(src, (now, now))
    os.utime(dst, (now, now))
    drive_manager._copy_newer(src=src, dst=dst, summary=summary, rel="same.jpg")
    assert any(change.get("status") == "skipped_unchanged" for change in summary.changes)

    src.write_bytes(b"old")
    dst.write_bytes(b"newer")
    os.utime(src, (now - 50, now - 50))
    os.utime(dst, (now, now))
    drive_manager._copy_newer(src=src, dst=dst, summary=summary, rel="older.jpg")
    assert any(change.get("status") == "conflict_keep_destination" for change in summary.changes)

    src.write_bytes(b"new")
    os.utime(src, (now + 50, now + 50))
    drive_manager._copy_newer(src=src, dst=dst, summary=summary, rel="newer.jpg")
    assert any(change.get("status") == "conflict_keep_source" for change in summary.changes)

    broken_src = tmp_path / "broken.jpg"
    broken_dst = tmp_path / "broken-dst.jpg"
    broken_src.write_bytes(b"x")

    def _boom(_src, _dst):  # type: ignore[no-untyped-def]
        raise OSError("copy failed")

    monkeypatch.setattr(drive_manager.shutil, "copy2", _boom)
    drive_manager._copy_newer(src=broken_src, dst=broken_dst, summary=summary, rel="broken.jpg")
    assert summary.failed >= 1
    assert any(error.get("file") == "broken.jpg" for error in summary.errors)


def test_pending_count_detects_missing_and_changed_files(tmp_path: Path):
    src_root = tmp_path / "src"
    dst_root = tmp_path / "dst"
    _seed_file(src_root / "a.jpg", b"1")
    _seed_file(src_root / "b.jpg", b"2")
    _seed_file(dst_root / "a.jpg", b"1")

    now = time.time()
    os.utime(src_root / "a.jpg", (now + 5, now + 5))
    os.utime(dst_root / "a.jpg", (now, now))
    pending = drive_manager._pending_count(src_root=src_root, dst_root=dst_root)
    assert pending == 2


def test_push_to_google_api_filters_traversal_and_missing_selected_files(tmp_path: Path, monkeypatch):
    input_root = tmp_path / "Input Covers"
    output_root = tmp_path / "Output Covers"
    exports_root = tmp_path / "exports"
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")

    selected = output_root / "1. Book" / "Variant-1" / "cover.jpg"
    _seed_file(selected, b"winner")
    outside = tmp_path / "outside.jpg"
    _seed_file(outside, b"outside")

    captured: dict[str, object] = {}

    def _fake_sync_to_drive(**kwargs):
        captured.update(kwargs)
        return {
            "mode": "google_api",
            "uploaded": 1,
            "skipped": 0,
            "failed": 0,
            "errors": [],
            "progress": [],
        }

    monkeypatch.setattr(drive_manager.gdrive_sync, "sync_to_drive", _fake_sync_to_drive)
    payload = drive_manager.push_to_drive(
        output_root=output_root,
        input_root=input_root,
        exports_root=exports_root,
        drive_folder_id="remote-folder-id",
        credentials_path=credentials,
        selected_relative_files=[
            "1. Book/Variant-1/cover.jpg",
            "../outside.jpg",
            "",
            "missing.jpg",
        ],
    )
    assert payload["uploaded"] == 1
    files = captured.get("files")
    assert isinstance(files, list)
    assert files == [selected.resolve()]


def test_push_local_default_handles_outside_iter_file_and_ignored_prefixes(tmp_path: Path, monkeypatch):
    input_root = tmp_path / "Input Covers"
    output_root = tmp_path / "Output Covers"
    exports_root = tmp_path / "exports"
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")
    drive_id = f"local:{tmp_path / 'mirror'}"

    valid = output_root / "1. Book" / "Variant-1" / "cover.jpg"
    ignored = output_root / "Archive" / "old.jpg"
    outside = tmp_path / "outside.txt"
    _seed_file(valid, b"v")
    _seed_file(ignored, b"i")
    _seed_file(outside, b"o")
    _seed_file(output_root / "Social" / "s.jpg", b"s")
    _seed_file(output_root / "Mockups" / "m.jpg", b"m")
    _seed_file(output_root / "catalog.pdf", b"pdf")
    _seed_file(exports_root / "web" / "manifest.json", b"{}")
    _seed_file(input_root / "Book" / "cover.jpg", b"in")

    def _fake_iter(root: Path):  # type: ignore[no-untyped-def]
        if root == output_root:
            return [outside, valid, ignored]
        if root == output_root / "Social":
            return [output_root / "Social" / "s.jpg"]
        if root == output_root / "Mockups":
            return [output_root / "Mockups" / "m.jpg"]
        if root == exports_root:
            return [exports_root / "web" / "manifest.json"]
        if root == input_root:
            return [input_root / "Book" / "cover.jpg"]
        return []

    monkeypatch.setattr(drive_manager, "_iter_files", _fake_iter)
    payload = drive_manager.push_to_drive(
        output_root=output_root,
        input_root=input_root,
        exports_root=exports_root,
        drive_folder_id=drive_id,
        credentials_path=credentials,
        selected_relative_files=None,
    )
    changes = payload.get("changes", [])
    assert any(change.get("file", "").startswith("Winners/1. Book/") for change in changes)
    assert any(change.get("file", "").startswith("Social Cards/") for change in changes)
    assert any(change.get("file", "").startswith("Mockups/") for change in changes)
    assert any(change.get("file", "").startswith("Catalogs/") for change in changes)
    assert any(change.get("file", "").startswith("Exports/") for change in changes)
    assert not any("Archive/" in str(change.get("file", "")) for change in changes)


def test_pull_local_skip_unchanged_conflict_keep_drive_and_copy_error(tmp_path: Path, monkeypatch):
    input_root = tmp_path / "Input Covers"
    mirror = tmp_path / "mirror"
    source_root = mirror / "Alexandria Covers" / "Input Covers"
    unchanged_src = source_root / "same.jpg"
    unchanged_dst = input_root / "same.jpg"
    newer_drive_src = source_root / "drive_newer.jpg"
    newer_drive_dst = input_root / "drive_newer.jpg"
    error_src = source_root / "error.jpg"

    _seed_file(unchanged_src, b"same")
    _seed_file(unchanged_dst, b"same")
    now = time.time()
    os.utime(unchanged_src, (now, now))
    os.utime(unchanged_dst, (now + 0.2, now + 0.2))

    _seed_file(newer_drive_src, b"new")
    _seed_file(newer_drive_dst, b"old")
    os.utime(newer_drive_src, (now + 10, now + 10))
    os.utime(newer_drive_dst, (now, now))

    _seed_file(error_src, b"err")

    real_copy2 = drive_manager.shutil.copy2

    def _copy_with_error(src, dst):  # type: ignore[no-untyped-def]
        if Path(src).name == "error.jpg":
            raise OSError("cannot copy")
        return real_copy2(src, dst)

    monkeypatch.setattr(drive_manager.shutil, "copy2", _copy_with_error)
    payload = drive_manager.pull_from_drive(
        input_root=input_root,
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "credentials.json",
    )
    assert payload["skipped"] >= 1
    assert payload["conflicts"] >= 1
    assert payload["failed"] >= 1
    assert any(change.get("status") == "skipped_unchanged" for change in payload["changes"])
    assert any(change.get("status") == "conflict_keep_drive" for change in payload["changes"])


def test_list_input_covers_local_mirror_maps_catalog(tmp_path: Path):
    mirror = tmp_path / "mirror"
    drive_id = f"local:{mirror}"
    input_root = mirror / "Alexandria Covers" / "Input Covers"
    _seed_file(input_root / "1. A Room with a View" / "cover.jpg", b"img")
    _seed_file(input_root / "2. Moby Dick" / "cover.jpg", b"img")

    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text(
        '[{"number": 1, "title": "A Room with a View"}, {"number": 2, "title": "Moby Dick"}]',
        encoding="utf-8",
    )

    payload = drive_manager.list_input_covers(
        drive_folder_id=drive_id,
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    assert payload["mode"] == "local_mirror"
    assert payload["count"] == 2
    assert payload["covers"][0]["book_number"] == 1
    assert payload["covers"][1]["book_number"] == 2


def test_list_input_covers_google_api_uses_input_subfolder(monkeypatch, tmp_path: Path):
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text(
        '[{"number": 1, "title": "A Room with a View"}, {"number": 2, "title": "Moby Dick"}]',
        encoding="utf-8",
    )

    class _FakeFilesApi:
        def __init__(self) -> None:
            self._last_q = ""

        def list(self, **kwargs):  # type: ignore[no-untyped-def]
            self._last_q = str(kwargs.get("q", ""))
            return self

        def execute(self):  # type: ignore[no-untyped-def]
            if "mimeType='application/vnd.google-apps.folder'" in self._last_q:
                return {"files": [{"id": "input-folder", "name": "Input Covers"}]}
            if "'input-folder' in parents" in self._last_q:
                return {
                    "files": [
                        {"id": "folder-1", "name": "1. A Room with a View", "mimeType": "application/vnd.google-apps.folder"},
                        {"id": "file-2", "name": "2. Moby Dick.jpg", "mimeType": "image/jpeg", "size": "1234"},
                        {"id": "skip", "name": "notes.txt", "mimeType": "text/plain"},
                    ]
                }
            return {"files": []}

    class _FakeService:
        def __init__(self) -> None:
            self._files_api = _FakeFilesApi()

        def files(self):  # type: ignore[no-untyped-def]
            return self._files_api

    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: _FakeService())
    payload = drive_manager.list_input_covers(
        drive_folder_id="drive-root-id",
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    assert payload["mode"] == "google_api"
    assert payload["input_folder_id"] == "input-folder"
    assert payload["count"] == 2
    assert payload["covers"][0]["book_number"] == 1
    assert payload["covers"][1]["book_number"] == 2


def test_list_input_covers_google_api_error_returns_unavailable(monkeypatch, tmp_path: Path):
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(
        drive_manager.gdrive_sync,
        "authenticate",
        lambda _path: (_ for _ in ()).throw(RuntimeError("drive offline")),
    )

    payload = drive_manager.list_input_covers(
        drive_folder_id="drive-root-id",
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    assert payload["mode"] == "unavailable"
    assert payload["count"] == 0
    assert "drive offline" in str(payload.get("error", ""))


def test_list_input_covers_google_api_reuses_cached_drive_listing(monkeypatch, tmp_path: Path):
    drive_manager.clear_drive_cover_cache()
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text('[{"number": 1, "title": "A Room with a View"}]', encoding="utf-8")

    calls = {"iter": 0}
    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: object())

    def _fake_iter_drive_cover_entries(**_kwargs):  # type: ignore[no-untyped-def]
        calls["iter"] += 1
        return (
            [
                {
                    "id": "folder-1",
                    "name": "1. A Room with a View",
                    "kind": "folder",
                    "book_number": 1,
                }
            ],
            "input-folder-id",
        )

    monkeypatch.setattr(drive_manager, "_iter_drive_cover_entries", _fake_iter_drive_cover_entries)

    first = drive_manager.list_input_covers(
        drive_folder_id="drive-root-id",
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    second = drive_manager.list_input_covers(
        drive_folder_id="drive-root-id",
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    drive_manager.clear_drive_cover_cache()

    assert first["mode"] == "google_api"
    assert second["mode"] == "google_api"
    assert first["count"] == 1
    assert second["count"] == 1
    assert calls["iter"] == 1


def test_clear_drive_cover_cache_forces_refetch(monkeypatch, tmp_path: Path):
    drive_manager.clear_drive_cover_cache()
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text('[{"number": 1, "title": "A Room with a View"}]', encoding="utf-8")

    calls = {"iter": 0}
    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: object())

    def _fake_iter_drive_cover_entries(**_kwargs):  # type: ignore[no-untyped-def]
        calls["iter"] += 1
        return (
            [
                {
                    "id": "folder-1",
                    "name": "1. A Room with a View",
                    "kind": "folder",
                    "book_number": 1,
                }
            ],
            "input-folder-id",
        )

    monkeypatch.setattr(drive_manager, "_iter_drive_cover_entries", _fake_iter_drive_cover_entries)
    drive_manager.list_input_covers(
        drive_folder_id="drive-root-id",
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    drive_manager.clear_drive_cover_cache()
    drive_manager.list_input_covers(
        drive_folder_id="drive-root-id",
        input_folder_id="",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        limit=100,
    )
    drive_manager.clear_drive_cover_cache()
    assert calls["iter"] == 2


def test_resolve_book_mapping_uses_fuzzy_title_fallback():
    title_by_book = {2: "Moby Dick"}
    book_by_title = {drive_manager._normalize_title_token("Moby Dick"): 2}
    book, title = drive_manager._resolve_book_mapping(
        name="Moby Dik",
        title_by_book=title_by_book,
        book_by_title=book_by_title,
    )
    assert book == 2
    assert title == "Moby Dick"


def test_ensure_local_input_cover_selected_drive_cover_mismatch_falls_back_to_requested_book(monkeypatch, tmp_path: Path):
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text(
        '[{"number": 1, "folder_name": "1. Book", "title": "Book One"}, {"number": 2, "folder_name": "2. Book", "title": "Book Two"}]',
        encoding="utf-8",
    )
    input_root = tmp_path / "Input Covers"

    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: object())
    monkeypatch.setattr(
        drive_manager,
        "_iter_drive_cover_entries",
        lambda **_kwargs: (
            [
                {"id": "cover-2", "book_number": 2, "kind": "folder", "name": "2. Book Two"},
                {"id": "cover-1", "book_number": 1, "kind": "folder", "name": "1. Book One"},
            ],
            "input-folder-id",
        ),
    )
    monkeypatch.setattr(
        drive_manager,
        "_pick_drive_image_from_folder",
        lambda **_kwargs: {"id": "image-1", "name": "cover.jpg"},
    )
    monkeypatch.setattr(drive_manager, "_pick_drive_pdf_from_folder", lambda **_kwargs: None)
    monkeypatch.setattr(drive_manager, "_download_drive_file_bytes", lambda **_kwargs: b"image")

    payload = drive_manager.ensure_local_input_cover(
        drive_folder_id="drive-root-id",
        input_folder_id="input-folder-id",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        input_root=input_root,
        book_number=1,
        selected_cover_id="cover-2",
    )
    assert payload["ok"] is True
    assert payload["downloaded"] is True
    assert str(payload.get("path", "")).endswith("cover_from_drive.jpg")


def test_ensure_local_input_cover_missing_drive_book_includes_title(monkeypatch, tmp_path: Path):
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text(
        '[{"number": 1, "folder_name": "1. Book", "title": "Book One"}]',
        encoding="utf-8",
    )
    input_root = tmp_path / "Input Covers"

    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: object())
    monkeypatch.setattr(
        drive_manager,
        "_iter_drive_cover_entries",
        lambda **_kwargs: ([], "input-folder-id"),
    )

    payload = drive_manager.ensure_local_input_cover(
        drive_folder_id="drive-root-id",
        input_folder_id="input-folder-id",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        input_root=input_root,
        book_number=1,
    )
    assert payload["ok"] is False
    assert "book #1 'book one'" in str(payload.get("error", "")).lower()


def test_ensure_local_input_cover_selected_drive_cover_missing_falls_back(monkeypatch, tmp_path: Path):
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text(
        '[{"number": 1, "folder_name": "1. Book", "title": "Book One"}]',
        encoding="utf-8",
    )
    input_root = tmp_path / "Input Covers"

    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: object())
    monkeypatch.setattr(
        drive_manager,
        "_iter_drive_cover_entries",
        lambda **_kwargs: (
            [{"id": "cover-1", "book_number": 1, "kind": "folder", "name": "1. Book One"}],
            "input-folder-id",
        ),
    )
    monkeypatch.setattr(
        drive_manager,
        "_pick_drive_image_from_folder",
        lambda **_kwargs: {"id": "image-1", "name": "cover.jpg"},
    )
    monkeypatch.setattr(drive_manager, "_pick_drive_pdf_from_folder", lambda **_kwargs: None)
    monkeypatch.setattr(drive_manager, "_download_drive_file_bytes", lambda **_kwargs: b"image")

    payload = drive_manager.ensure_local_input_cover(
        drive_folder_id="drive-root-id",
        input_folder_id="input-folder-id",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        input_root=input_root,
        book_number=1,
        selected_cover_id="missing-cover-id",
    )
    assert payload["ok"] is True
    assert payload["downloaded"] is True


def test_ensure_local_input_cover_selected_drive_cover_ignores_existing_local_when_selected(monkeypatch, tmp_path: Path):
    catalog_path = tmp_path / "book_catalog.json"
    catalog_path.write_text(
        '[{"number": 1, "folder_name": "1. Book", "title": "Book One"}, {"number": 2, "folder_name": "2. Book", "title": "Book Two"}]',
        encoding="utf-8",
    )
    input_root = tmp_path / "Input Covers"
    _seed_file(input_root / "1. Book" / "existing_local.jpg", b"local")

    monkeypatch.setattr(drive_manager.gdrive_sync, "authenticate", lambda _path: object())
    monkeypatch.setattr(
        drive_manager,
        "_iter_drive_cover_entries",
        lambda **_kwargs: (
            [{"id": "cover-2", "book_number": 2, "kind": "folder", "name": "2. Book Two"}],
            "input-folder-id",
        ),
    )

    payload = drive_manager.ensure_local_input_cover(
        drive_folder_id="drive-root-id",
        input_folder_id="input-folder-id",
        credentials_path=tmp_path / "credentials.json",
        catalog_path=catalog_path,
        input_root=input_root,
        book_number=1,
        selected_cover_id="cover-2",
    )
    assert payload["ok"] is False
    assert "no cover found in google drive for book #1" in str(payload.get("error", "")).lower()
