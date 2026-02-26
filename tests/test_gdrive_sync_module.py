from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from src import gdrive_sync as gs


def _write(path: Path, data: bytes = b"x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def test_authenticate_requires_google_dependencies(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("GOOGLE_CREDENTIALS_JSON", raising=False)
    monkeypatch.setattr(gs, "GOOGLE_API_AVAILABLE", False)
    with pytest.raises(RuntimeError):
        gs.authenticate(tmp_path / "credentials.json")


def test_authenticate_missing_credentials_file(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("GOOGLE_CREDENTIALS_JSON", raising=False)
    monkeypatch.setattr(gs, "GOOGLE_API_AVAILABLE", True)
    with pytest.raises(FileNotFoundError):
        gs.authenticate(tmp_path / "missing-credentials.json")


def test_local_mirror_sync_and_status(tmp_path: Path):
    local = tmp_path / "Output Covers"
    mirror = tmp_path / "Mirror"
    _write(local / "1. Book" / "Variant-1" / "file.jpg", b"abc")
    _write(local / "1. Book" / "Variant-1" / "file.pdf", b"def")

    events: list[dict] = []
    summary1 = gs.sync_to_drive(
        local_output_dir=local,
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "unused.json",
        incremental=True,
        progress_callback=events.append,
    )
    assert summary1["mode"] == "local_mirror"
    assert summary1["uploaded"] == 2
    assert summary1["failed"] == 0
    assert len(events) == 2
    assert (mirror / "1. Book" / "Variant-1" / "file.jpg").exists()

    # Incremental rerun should skip equal-size files.
    summary2 = gs.sync_to_drive(
        local_output_dir=local,
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "unused.json",
        incremental=True,
    )
    assert summary2["skipped"] == 2

    status = gs.get_sync_status(f"local:{mirror}", tmp_path / "unused.json")
    assert status["mode"] == "local_mirror"
    assert status["file_count"] >= 2


def test_sync_selected_to_drive_filters_subset(tmp_path: Path):
    local = tmp_path / "Output Covers"
    mirror = tmp_path / "Mirror"
    _write(local / "1. Book" / "Variant-1" / "keep.jpg", b"1")
    _write(local / "1. Book" / "Variant-1" / "skip.jpg", b"2")

    summary = gs.sync_selected_to_drive(
        local_output_dir=local,
        relative_paths=["1. Book/Variant-1/keep.jpg", "missing.jpg"],
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "unused.json",
    )
    assert summary["uploaded"] == 1
    assert (mirror / "1. Book" / "Variant-1" / "keep.jpg").exists()
    assert not (mirror / "1. Book" / "Variant-1" / "skip.jpg").exists()


def test_resolve_files_and_state_helpers(tmp_path: Path, monkeypatch):
    root = tmp_path / "Output Covers"
    _write(root / "a.jpg", b"a")
    _write(root / "b.pdf", b"b")
    outside = tmp_path / "outside.txt"
    outside.write_text("x", encoding="utf-8")

    files = gs._resolve_files(root, [root / "a.jpg", root / "a.jpg", outside, Path("missing")])
    assert files == [(root / "a.jpg").resolve()]

    monkeypatch.setattr(gs, "SYNC_STATE_PATH", tmp_path / "state.json")
    assert gs._load_sync_state() == {}
    gs.SYNC_STATE_PATH.write_text("{bad-json", encoding="utf-8")
    assert gs._load_sync_state() == {}

    gs._save_sync_state({"hello": "world"})
    assert gs._load_sync_state() == {"hello": "world"}

    assert gs._book_number_from_folder("12. Something") == 12
    assert gs._book_number_from_folder("abc") is None
    assert gs._escape_query("O'Hara") == "O\\'Hara"


def test_sync_selected_to_drive_ignores_blank_tokens(tmp_path: Path, monkeypatch):
    local = tmp_path / "Output Covers"
    mirror = tmp_path / "Mirror"
    _write(local / "1. Book" / "Variant-1" / "keep.jpg", b"1")

    captured: dict[str, object] = {}

    def _fake_sync_to_drive(**kwargs):
        captured.update(kwargs)
        return {"failed": 0}

    monkeypatch.setattr(gs, "sync_to_drive", _fake_sync_to_drive)
    gs.sync_selected_to_drive(
        local_output_dir=local,
        relative_paths=["", " ", "/", "1. Book/Variant-1/keep.jpg"],
        drive_folder_id=f"local:{mirror}",
        credentials_path=tmp_path / "unused.json",
    )
    files = captured["files"]
    assert isinstance(files, list)
    assert len(files) == 1


def test_sync_to_drive_google_mode_missing_local_dir_raises(tmp_path: Path, monkeypatch):
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(gs, "authenticate", lambda _path: object())

    with pytest.raises(FileNotFoundError):
        gs.sync_to_drive(
            local_output_dir=tmp_path / "missing-output",
            drive_folder_id="drive-root",
            credentials_path=credentials,
        )


def test_winner_file_collection_and_staging_tree(tmp_path: Path):
    output = tmp_path / "Output Covers"
    book_dir = output / "3. Title"
    _write(book_dir / "Variant-2" / "A.jpg", b"1")
    _write(book_dir / "Variant-2" / "A.pdf", b"2")
    _write(book_dir / "Variant-2" / "A.ai", b"3")
    _write(book_dir / "Variant-1" / "B.jpg", b"4")

    selections = tmp_path / "winner_selections.json"
    selections.write_text(json.dumps({"selections": {"3": 2}}), encoding="utf-8")

    winners = gs._load_winner_map(selections)
    assert winners == {3: 2}

    files = gs._winner_files_for_book(book_dir, winner_variant=2)
    assert len(files) == 3

    staging_root, meta = gs._build_winners_sync_tree(output_dir=output, selections_path=selections)
    try:
        assert meta["staged_books"] == 1
        assert meta["staged_files"] == 3
        assert (staging_root / "3. Title" / "A.jpg").exists()
    finally:
        # Keep tests from leaking temp staging dirs.
        if staging_root.exists():
            for p in sorted(staging_root.rglob("*"), reverse=True):
                if p.is_file():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            staging_root.rmdir()


def test_ensure_remote_folder_and_upload_file_paths(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(gs, "MediaFileUpload", lambda path, mimetype=None, resumable=False: SimpleNamespace(path=path), raising=False)

    class FakeRequest:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):  # type: ignore[no-untyped-def]
            return self._payload

    class FakeFilesApi:
        def __init__(self):
            self.list_payload = {"files": []}
            self.created = []
            self.updated = []

        def list(self, **_kwargs):  # type: ignore[no-untyped-def]
            return FakeRequest(self.list_payload)

        def create(self, **kwargs):  # type: ignore[no-untyped-def]
            self.created.append(kwargs)
            return FakeRequest({"id": "new-id"})

        def update(self, **kwargs):  # type: ignore[no-untyped-def]
            self.updated.append(kwargs)
            return FakeRequest({"id": kwargs.get("fileId", "updated-id")})

    class FakeService:
        def __init__(self):
            self._files = FakeFilesApi()

        def files(self):  # type: ignore[no-untyped-def]
            return self._files

    service = FakeService()
    cache: dict[tuple[str, str], str] = {}
    folder_id = gs._ensure_remote_folder(
        service=service,
        parent_id="root",
        folder_name="Books",
        cache=cache,
    )
    assert folder_id == "new-id"
    assert cache[("root", "Books")] == "new-id"

    # Existing folder path and cache-hit path.
    service._files.list_payload = {"files": [{"id": "existing-id", "name": "Books"}]}
    cache_existing: dict[tuple[str, str], str] = {}
    existing_id = gs._ensure_remote_folder(
        service=service,
        parent_id="parent",
        folder_name="Books",
        cache=cache_existing,
    )
    assert existing_id == "existing-id"
    assert gs._ensure_remote_folder(service=service, parent_id="parent", folder_name="Books", cache=cache_existing) == "existing-id"

    local_file = tmp_path / "file.jpg"
    local_file.write_bytes(b"abcd")
    state: dict[str, dict] = {}

    # Existing remote with same size -> skipped
    service._files.list_payload = {"files": [{"id": "f1", "name": "file.jpg", "size": str(local_file.stat().st_size)}]}
    status = gs._upload_file(service=service, parent_id="root", local_path=local_file, incremental=True, state=state)
    assert status == "skipped"

    # Existing remote with different size -> uploaded via update
    service._files.list_payload = {"files": [{"id": "f1", "name": "file.jpg", "size": "999"}]}
    status = gs._upload_file(service=service, parent_id="root", local_path=local_file, incremental=True, state=state)
    assert status == "uploaded"
    assert service._files.updated

    # Missing remote -> uploaded via create
    service._files.list_payload = {"files": []}
    status = gs._upload_file(service=service, parent_id="root", local_path=local_file, incremental=False, state=state)
    assert status == "uploaded"
    assert service._files.created


def test_authenticate_service_account_and_oauth(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("GOOGLE_CREDENTIALS_JSON", raising=False)
    monkeypatch.setattr(gs, "GOOGLE_API_AVAILABLE", True)

    cred_path = tmp_path / "credentials.json"
    cred_path.write_text(json.dumps({"type": "service_account"}), encoding="utf-8")

    class _Creds:
        @staticmethod
        def from_service_account_file(path, scopes=None):  # type: ignore[no-untyped-def]
            return {"path": path, "scopes": scopes}

    monkeypatch.setattr(gs, "service_account", SimpleNamespace(Credentials=_Creds), raising=False)
    monkeypatch.setattr(gs, "build", lambda _api, _ver, credentials=None: {"credentials": credentials}, raising=False)

    svc = gs.authenticate(cred_path)
    assert "credentials" in svc

    oauth_path = tmp_path / "oauth.json"
    oauth_path.write_text(json.dumps({"installed": {"client_id": "abc"}}), encoding="utf-8")

    class _Flow:
        @staticmethod
        def from_client_secrets_file(path, scopes):  # type: ignore[no-untyped-def]
            class _Inner:
                def run_local_server(self, port=0):  # type: ignore[no-untyped-def]
                    return {"oauth": True, "path": path, "scopes": scopes, "port": port}

            return _Inner()

    monkeypatch.setattr(gs, "InstalledAppFlow", _Flow, raising=False)
    svc2 = gs.authenticate(oauth_path)
    assert svc2["credentials"]["oauth"] is True


def test_authenticate_prefers_google_credentials_json_env(monkeypatch):
    monkeypatch.setattr(gs, "GOOGLE_API_AVAILABLE", True)
    monkeypatch.setenv(
        "GOOGLE_CREDENTIALS_JSON",
        json.dumps(
            {
                "type": "service_account",
                "project_id": "demo-project",
                "private_key_id": "abc123",
                "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
                "client_email": "demo@example.iam.gserviceaccount.com",
                "client_id": "1234567890",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        ),
    )

    class _Creds:
        @staticmethod
        def from_service_account_info(info, scopes=None):  # type: ignore[no-untyped-def]
            return {"info_type": info.get("type"), "scopes": scopes}

    monkeypatch.setattr(gs, "service_account", SimpleNamespace(Credentials=_Creds), raising=False)
    monkeypatch.setattr(gs, "build", lambda _api, _ver, credentials=None: {"credentials": credentials}, raising=False)
    service = gs.authenticate(None)
    assert service["credentials"]["info_type"] == "service_account"


def test_sync_to_google_api_path_and_get_status(tmp_path: Path, monkeypatch):
    local = tmp_path / "Output Covers"
    _write(local / "1. Book" / "Variant-1" / "file.jpg", b"abc")
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")

    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):  # type: ignore[no-untyped-def]
            return self._payload

    class _Service:
        def __init__(self):
            self.items: list[dict] = []
            self._next = 1

        def files(self):  # type: ignore[no-untyped-def]
            return self

        def list(self, q=None, fields=None, pageSize=None):  # type: ignore[no-untyped-def]
            if q and "mimeType='application/vnd.google-apps.folder'" in q:
                return _Request({"files": []})
            if q and "name='file.jpg'" in q:
                return _Request({"files": []})
            if q and "'drive-root' in parents and trashed=false" in q:
                return _Request({"files": self.items})
            return _Request({"files": []})

        def create(self, body=None, media_body=None, fields=None):  # type: ignore[no-untyped-def]
            item_id = f"id-{self._next}"
            self._next += 1
            mime = (body or {}).get("mimeType", "application/octet-stream")
            name = (body or {}).get("name", "")
            self.items.append({"id": item_id, "name": name, "mimeType": mime})
            return _Request({"id": item_id})

        def update(self, fileId=None, media_body=None):  # type: ignore[no-untyped-def]
            return _Request({"id": fileId})

    service = _Service()
    monkeypatch.setattr(gs, "authenticate", lambda _path: service)
    monkeypatch.setattr(gs, "MediaFileUpload", lambda path, mimetype=None, resumable=False: {"path": path}, raising=False)

    summary = gs.sync_to_drive(
        local_output_dir=local,
        drive_folder_id="drive-root",
        credentials_path=credentials,
        incremental=True,
    )
    assert summary["mode"] == "google_api"
    assert summary["uploaded"] == 1
    assert summary["failed"] == 0

    status = gs.get_sync_status("drive-root", credentials)
    assert status["mode"] == "google_api"
    assert status["item_count"] >= 1


def test_sync_to_google_api_progress_callback_called(tmp_path: Path, monkeypatch):
    local = tmp_path / "Output Covers"
    _write(local / "1. Book" / "Variant-1" / "file.jpg", b"abc")
    credentials = tmp_path / "credentials.json"
    credentials.write_text("{}", encoding="utf-8")

    class _Request:
        def __init__(self, payload):
            self._payload = payload

        def execute(self):  # type: ignore[no-untyped-def]
            return self._payload

    class _Service:
        def files(self):  # type: ignore[no-untyped-def]
            return self

        def list(self, q=None, fields=None, pageSize=None):  # type: ignore[no-untyped-def]
            if q and "mimeType='application/vnd.google-apps.folder'" in q:
                return _Request({"files": [{"id": "folder-id"}]})
            return _Request({"files": []})

        def create(self, body=None, media_body=None, fields=None):  # type: ignore[no-untyped-def]
            return _Request({"id": "new-id"})

        def update(self, fileId=None, media_body=None):  # type: ignore[no-untyped-def]
            return _Request({"id": fileId})

    monkeypatch.setattr(gs, "authenticate", lambda _path: _Service())
    monkeypatch.setattr(gs, "MediaFileUpload", lambda path, mimetype=None, resumable=False: {"path": path}, raising=False)

    events: list[dict] = []
    summary = gs.sync_to_drive(
        local_output_dir=local,
        drive_folder_id="drive-root",
        credentials_path=credentials,
        incremental=True,
        progress_callback=events.append,
    )
    assert summary["uploaded"] == 1
    assert len(events) == 1


def test_main_status_winners_only_and_selected_files(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        output_dir=tmp_path / "Output Covers",
        gdrive_output_folder_id="local:" + str(tmp_path / "Mirror"),
        google_credentials_path="",
        config_dir=tmp_path / "config",
    )
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(gs.config, "get_config", lambda _catalog: runtime)

    # status path
    args_status = SimpleNamespace(
        catalog="classics",
        local_output_dir=None,
        input=None,
        drive_folder_id=None,
        credentials_path=None,
        selected_files=None,
        winners_only=False,
        selections=tmp_path / "winner_selections.json",
        no_incremental=False,
        status=True,
    )
    monkeypatch.setattr(gs.argparse.ArgumentParser, "parse_args", lambda self: args_status)
    monkeypatch.setattr(gs, "get_sync_status", lambda *_args, **_kwargs: {"ok": True})
    assert gs.main() == 0

    # winners-only missing selections -> error
    args_winners = SimpleNamespace(**args_status.__dict__)
    args_winners.status = False
    args_winners.winners_only = True
    args_winners.selections = tmp_path / "missing.json"
    monkeypatch.setattr(gs.argparse.ArgumentParser, "parse_args", lambda self: args_winners)
    assert gs.main() == 1

    # selected-files path
    _write(runtime.output_dir / "1. Book" / "Variant-1" / "file.jpg", b"abc")
    selected_path = tmp_path / "selected.json"
    selected_path.write_text(json.dumps(["1. Book/Variant-1/file.jpg"]), encoding="utf-8")
    args_selected = SimpleNamespace(**args_status.__dict__)
    args_selected.status = False
    args_selected.selected_files = selected_path
    monkeypatch.setattr(gs.argparse.ArgumentParser, "parse_args", lambda self: args_selected)
    monkeypatch.setattr(gs, "sync_to_drive", lambda **_kwargs: {"failed": 0})
    assert gs.main() == 0


def test_winner_map_and_tree_edge_cases(tmp_path: Path):
    # Raw selections not a dict.
    raw_bad = tmp_path / "raw_bad.json"
    raw_bad.write_text(json.dumps({"selections": [1, 2, 3]}), encoding="utf-8")
    assert gs._load_winner_map(raw_bad) == {}

    # Mixed keys and dict/non-dict winner values.
    mixed = tmp_path / "mixed.json"
    mixed.write_text(
        json.dumps({"selections": {"bad-key": 1, "4": {"winner": 2}, "5": {"winner": 0}, "6": 3}}),
        encoding="utf-8",
    )
    assert gs._load_winner_map(mixed) == {4: 2, 6: 3}

    output = tmp_path / "Output Covers"
    (output / "Book No Number").mkdir(parents=True, exist_ok=True)  # skipped: no numeric prefix
    (output / "7. Winner Missing Files").mkdir(parents=True, exist_ok=True)  # skipped: winner chosen but no files
    (output / "8. No Winner").mkdir(parents=True, exist_ok=True)  # skipped: no selection

    selections = tmp_path / "winners.json"
    selections.write_text(json.dumps({"selections": {"7": 1}}), encoding="utf-8")

    staging_root, meta = gs._build_winners_sync_tree(output_dir=output, selections_path=selections)
    try:
        assert meta["staged_books"] == 0
        assert meta["staged_files"] == 0
        assert meta["skipped_books"] == [7]
    finally:
        if staging_root.exists():
            for p in sorted(staging_root.rglob("*"), reverse=True):
                if p.is_file():
                    p.unlink()
                elif p.is_dir():
                    p.rmdir()
            staging_root.rmdir()


def test_winner_files_fallback_to_book_root(tmp_path: Path):
    book_dir = tmp_path / "3. Root Files"
    _write(book_dir / "winner.jpg", b"img")
    _write(book_dir / "winner.pdf", b"pdf")
    _write(book_dir / "ignore.txt", b"txt")
    files = gs._winner_files_for_book(book_dir, winner_variant=9)
    assert {p.name for p in files} == {"winner.jpg", "winner.pdf"}


def test_main_winners_only_success_sets_staging_inputs(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        output_dir=tmp_path / "Output Covers",
        gdrive_output_folder_id="drive-root",
        google_credentials_path="",
        config_dir=tmp_path / "config",
    )
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)
    _write(runtime.output_dir / "2. Book" / "Variant-1" / "file.jpg", b"abc")

    selections = tmp_path / "winner_selections.json"
    selections.write_text(json.dumps({"selections": {"2": 1}}), encoding="utf-8")
    creds = runtime.config_dir / "credentials.json"
    creds.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(gs.config, "get_config", lambda _catalog: runtime)

    args = SimpleNamespace(
        catalog="classics",
        local_output_dir=None,
        input=None,
        drive_folder_id=None,
        credentials_path=None,
        selected_files=None,
        winners_only=True,
        selections=selections,
        no_incremental=False,
        status=False,
    )
    monkeypatch.setattr(gs.argparse.ArgumentParser, "parse_args", lambda self: args)

    captured: dict[str, object] = {}

    def _fake_sync_to_drive(**kwargs):
        captured.update(kwargs)
        return {"failed": 0}

    monkeypatch.setattr(gs, "sync_to_drive", _fake_sync_to_drive)

    assert gs.main() == 0
    assert captured.get("files") is None
    sync_root = captured.get("local_output_dir")
    assert isinstance(sync_root, Path)
    assert sync_root != runtime.output_dir
    assert not sync_root.exists()


def test_main_uses_catalog_scoped_sync_state_path(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        catalog_id="demo",
        data_dir=tmp_path,
        output_dir=tmp_path / "Output Covers",
        gdrive_output_folder_id="drive-root",
        google_credentials_path="",
        config_dir=tmp_path / "config",
    )
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)
    _write(runtime.output_dir / "1. Book" / "Variant-1" / "file.jpg", b"abc")

    monkeypatch.setattr(gs.config, "get_config", lambda _catalog: runtime)

    args = SimpleNamespace(
        catalog="demo",
        local_output_dir=None,
        input=None,
        drive_folder_id=None,
        credentials_path=None,
        selected_files=None,
        winners_only=False,
        selections=tmp_path / "winner_selections.json",
        no_incremental=False,
        status=False,
    )
    monkeypatch.setattr(gs.argparse.ArgumentParser, "parse_args", lambda self: args)

    captured: dict[str, object] = {}

    def _fake_sync_to_drive(**kwargs):
        captured.update(kwargs)
        return {"failed": 0}

    monkeypatch.setattr(gs, "sync_to_drive", _fake_sync_to_drive)

    assert gs.main() == 0
    assert captured.get("sync_state_path") == (tmp_path / "gdrive_sync_state_demo.json")
