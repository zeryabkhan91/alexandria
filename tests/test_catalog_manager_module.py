from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.catalog_manager import CatalogManager
from src import safe_json
from src import catalog_manager as cm


def _manager(tmp_path: Path) -> CatalogManager:
    catalogs_path = tmp_path / "config" / "catalogs.json"
    return CatalogManager(catalogs_path=catalogs_path, project_root=tmp_path)


def test_list_catalogs_bootstraps_default_when_missing(tmp_path: Path):
    manager = _manager(tmp_path)
    rows = manager.list_catalogs()
    assert rows
    assert rows[0].catalog_id == "classics"


def test_create_catalog_creates_config_files(tmp_path: Path):
    manager = _manager(tmp_path)
    created = manager.create_catalog(
        name="Science Fiction",
        description="SF classics",
        input_dir="Input SF",
        output_dir="Output SF",
        config_dir="config",
    )
    assert created.catalog_id == "science-fiction"
    assert (tmp_path / "config" / "book_catalog_science-fiction.json").exists()
    assert (tmp_path / "config" / "book_prompts_science-fiction.json").exists()


def test_create_catalog_duplicate_raises(tmp_path: Path):
    manager = _manager(tmp_path)
    manager.create_catalog(name="Horror")
    with pytest.raises(ValueError):
        manager.create_catalog(name="Horror")


def test_update_archive_activate_catalog(tmp_path: Path):
    manager = _manager(tmp_path)
    manager.create_catalog(name="Mystery")
    updated = manager.update_catalog("mystery", {"description": "Updated", "status": "active"})
    assert updated.description == "Updated"
    archived = manager.archive_catalog("mystery")
    assert archived.status == "archived"
    active = manager.activate_catalog("mystery")
    assert active.status == "active"


def test_clone_catalog_copies_settings(tmp_path: Path):
    manager = _manager(tmp_path)
    source = manager.create_catalog(name="Poetry")
    manager.update_settings(source.catalog_id, {"variants_per_book": 12, "quality_threshold": 0.77})
    clone = manager.clone_catalog(source.catalog_id, new_id="poetry-clone", name="Poetry Clone")
    settings = manager.get_settings(clone.catalog_id)
    assert clone.catalog_id == "poetry-clone"
    assert settings["variants_per_book"] == 12
    assert settings["quality_threshold"] == 0.77


def test_set_default_catalog(tmp_path: Path):
    manager = _manager(tmp_path)
    manager.create_catalog(name="Children")
    new_default = manager.set_default_catalog("children")
    assert new_default == "children"
    assert manager.get_default_catalog_id() == "children"


def test_get_and_update_settings_merge_defaults(tmp_path: Path):
    manager = _manager(tmp_path)
    manager.create_catalog(name="Thriller")
    settings = manager.update_settings("thriller", {"variants_per_book": 20})
    assert settings["variants_per_book"] == 20
    resolved = manager.get_settings("thriller")
    assert resolved["variants_per_book"] == 20
    assert "default_provider" in resolved


def test_import_books_detects_new_and_skips_existing(tmp_path: Path):
    manager = _manager(tmp_path)
    catalog = manager.create_catalog(name="Import Test", input_dir="Input Covers", output_dir="Output Covers")
    input_dir = tmp_path / "Input Covers"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "1. Existing Title - Existing Author").mkdir()
    (input_dir / "2. New Title - New Author").mkdir()
    (input_dir / "README").mkdir()

    catalog_file = tmp_path / "config" / f"book_catalog_{catalog.catalog_id}.json"
    safe_json.atomic_write_json(
        catalog_file,
        [
            {
                "number": 1,
                "title": "Existing Title",
                "author": "Existing Author",
                "folder_name": "1. Existing Title - Existing Author",
            }
        ],
    )

    summary = manager.import_books(catalog.catalog_id)
    assert summary["imported"] == 1
    assert summary["skipped"] == 1
    payload = json.loads(catalog_file.read_text(encoding="utf-8"))
    assert len(payload) == 2


def test_stats_for_catalog_tracks_processed_and_winners(tmp_path: Path):
    manager = _manager(tmp_path)
    catalog = manager.create_catalog(name="Stats Test", output_dir="Output Covers")
    catalog_file = tmp_path / "config" / f"book_catalog_{catalog.catalog_id}.json"
    safe_json.atomic_write_json(
        catalog_file,
        [
            {"number": 1, "title": "A", "author": "B", "folder_name": "1. A - B"},
            {"number": 2, "title": "C", "author": "D", "folder_name": "2. C - D"},
        ],
    )
    (tmp_path / "Output Covers" / "1. A - B").mkdir(parents=True, exist_ok=True)
    winners_path = tmp_path / "data" / f"winner_selections_{catalog.catalog_id}.json"
    winners_path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(winners_path, {"selections": {"1": {"winner": 2}}})

    stats = manager.stats_for_catalog(catalog.catalog_id)
    assert stats["book_count"] == 2
    assert stats["processed_count"] == 1
    assert stats["winner_count"] == 1


def test_export_catalog_bundle_contains_books_and_stats(tmp_path: Path):
    manager = _manager(tmp_path)
    catalog = manager.create_catalog(name="Export Test")
    catalog_file = tmp_path / "config" / f"book_catalog_{catalog.catalog_id}.json"
    safe_json.atomic_write_json(catalog_file, [{"number": 1, "title": "T", "author": "A", "folder_name": "1. T - A"}])
    bundle = manager.export_catalog_bundle(catalog.catalog_id)
    assert bundle["catalog"]["id"] == catalog.catalog_id
    assert isinstance(bundle["books"], list)
    assert "stats" in bundle


def test_load_supports_map_catalog_format(tmp_path: Path):
    catalogs_path = tmp_path / "config" / "catalogs.json"
    catalogs_path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(
        catalogs_path,
        {
            "catalogs": {
                "classic-literature": {
                    "name": "Classic Literature",
                    "description": "Sample",
                    "status": "active",
                    "input_dir": "Input Covers",
                    "output_dir": "Output Covers",
                    "config_dir": "config",
                }
            },
            "default_catalog": "classic-literature",
        },
    )
    manager = CatalogManager(catalogs_path=catalogs_path, project_root=tmp_path)
    catalog = manager.get_catalog("classic-literature")
    assert catalog.catalog_id == "classic-literature"
    assert manager.get_default_catalog_id() == "classic-literature"


def test_catalog_manager_helper_and_error_branches(tmp_path: Path, monkeypatch):
    # _parse_folder em-dash and unknown-author branches.
    n1, t1, a1 = cm._parse_folder("7. Title — Author")
    assert (n1, t1, a1) == (7, "Title", "Author")
    n2, t2, a2 = cm._parse_folder("8. Untagged")
    assert (n2, t2, a2) == (8, "Untagged", "Unknown")

    # dataclass to_dict branch.
    catalog_dc = cm.Catalog(
        catalog_id="x",
        name="X",
        description="",
        book_count=0,
        created_at="now",
        updated_at="now",
        status="active",
        settings={},
        input_dir="Input Covers",
        output_dir="Output Covers",
        config_dir="config",
    )
    assert catalog_dc.to_dict()["catalog_id"] == "x"

    manager = _manager(tmp_path)

    # _load with non-dict payload.
    monkeypatch.setattr(cm.safe_json, "load_json", lambda *_args, **_kwargs: "invalid")
    loaded = manager._load()
    assert isinstance(loaded["catalogs"], dict)

    # _load list format with invalid rows.
    monkeypatch.setattr(
        cm.safe_json,
        "load_json",
        lambda *_args, **_kwargs: {
            "catalogs": ["bad", {"name": "missing-id"}, {"id": "Sci Fi", "name": "Sci Fi"}],
            "default_catalog": "missing",
        },
    )
    list_loaded = manager._load()
    assert "sci-fi" in list_loaded["catalogs"]

    # _save with invalid catalogs type.
    captured = {}
    monkeypatch.setattr(cm.safe_json, "atomic_write_json", lambda _path, payload: captured.update(payload))
    manager._save({"catalogs": "bad", "default_catalog": "classics"})
    assert captured["catalogs"] == {}

    # absolute path resolution and winner path branch for classics.
    abs_path = manager._resolve_project_path("/tmp", project_root=tmp_path)
    assert str(abs_path) == "/tmp"
    assert str(manager._winner_path("classics")).endswith("data/winner_selections.json")

    # list/get/set-default branches when rows are invalid.
    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": "bad", "default_catalog": "classics"})
    assert manager.list_catalogs() == []
    with pytest.raises(KeyError):
        manager.get_catalog("missing")
    with pytest.raises(KeyError):
        manager.set_default_catalog("missing")


def test_catalog_manager_crud_and_import_edge_branches(tmp_path: Path, monkeypatch):
    manager = _manager(tmp_path)

    # create_catalog with invalid rows payload + empty default_catalog.
    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": "bad", "default_catalog": ""})
    saved = {}
    monkeypatch.setattr(manager, "_save", lambda payload: saved.update(payload))
    monkeypatch.setattr(manager, "_ensure_catalog_files", lambda _row: None)
    created = manager.create_catalog(name="Alpha")
    assert created.catalog_id == "alpha"
    assert saved["default_catalog"] == "alpha"

    # update_catalog with invalid/missing rows.
    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": "bad", "default_catalog": "classics"})
    with pytest.raises(KeyError):
        manager.update_catalog("alpha", {"name": "A"})

    # clone_catalog invalid rows and duplicate id.
    monkeypatch.setattr(manager, "get_catalog", lambda _cid: created)
    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": "bad", "default_catalog": "classics"})
    monkeypatch.setattr(manager, "_ensure_catalog_files", lambda _row: None)
    clone = manager.clone_catalog("alpha", new_id="alpha-clone")
    assert clone.catalog_id == "alpha-clone"

    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": {"alpha-clone": {"id": "alpha-clone"}}, "default_catalog": "classics"})
    with pytest.raises(ValueError):
        manager.clone_catalog("alpha", new_id="alpha-clone")

    # import_books with invalid rows/missing catalog/input-dir.
    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": "bad", "default_catalog": "classics"})
    with pytest.raises(KeyError):
        manager.import_books("missing")

    monkeypatch.setattr(
        manager,
        "_load",
        lambda: {
            "catalogs": {"alpha": {"id": "alpha", "input_dir": "missing-input", "catalog_file": "config/book_catalog_alpha.json", "prompts_file": "config/book_prompts_alpha.json"}},
            "default_catalog": "alpha",
        },
    )
    with pytest.raises(FileNotFoundError):
        manager.import_books("alpha")

    # export_catalog_bundle with invalid rows and missing catalog.
    monkeypatch.setattr(manager, "_load", lambda: {"catalogs": "bad", "default_catalog": "classics"})
    with pytest.raises(KeyError):
        manager.export_catalog_bundle("missing")


def test_stats_for_catalog_copy_suffix_and_empty_folder_name(tmp_path: Path, monkeypatch):
    manager = _manager(tmp_path)
    catalog = manager.create_catalog(name="Stats Edge", output_dir="Output Covers")
    monkeypatch.setattr(manager, "get_catalog", lambda _cid: catalog)
    monkeypatch.setattr(
        manager,
        "_load",
        lambda: {"catalogs": {catalog.catalog_id: {"id": catalog.catalog_id}}, "default_catalog": catalog.catalog_id},
    )
    monkeypatch.setattr(
        manager,
        "_read_catalog_books",
        lambda _row: [
            {"number": 1, "folder_name": "1. Book copy"},
            {"number": 2, "folder_name": ""},
        ],
    )

    (tmp_path / "Output Covers" / "1. Book").mkdir(parents=True, exist_ok=True)
    stats = manager.stats_for_catalog(catalog.catalog_id)
    assert stats["processed_count"] == 1
