from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from scripts import archive_non_winners
from scripts import auto_select_winners
from scripts import export_winners
from scripts import generate_catalog
from scripts import import_catalog
from scripts import prepare_print_delivery
from scripts import regenerate_weak
from src import config
from src import gdrive_sync
from src import mockup_generator
from src import social_card_generator


def test_auto_select_winners_default_output_path_uses_config_helper(tmp_path: Path):
    runtime = SimpleNamespace(catalog_id="demo", data_dir=tmp_path)
    assert auto_select_winners._default_output_path(runtime) == (tmp_path / "winner_selections_demo.json")


def test_export_winners_main_uses_catalog_scoped_default_selection_path(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        catalog_id="demo",
        data_dir=tmp_path,
        output_dir=tmp_path / "Output Covers",
        book_catalog_path=tmp_path / "book_catalog_demo.json",
    )
    captured: dict[str, Path] = {}

    def _fake_export_winners(**kwargs):  # type: ignore[no-untyped-def]
        captured["selections"] = kwargs["selections_path"]
        return {"ok": True}

    monkeypatch.setattr(export_winners.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(export_winners, "export_winners", _fake_export_winners)
    monkeypatch.setattr(sys, "argv", ["export_winners.py", "--catalog", "demo"])
    assert export_winners.main() == 0
    assert captured["selections"] == (tmp_path / "winner_selections_demo.json")


def test_archive_non_winners_main_uses_catalog_scoped_defaults(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        catalog_id="demo",
        data_dir=tmp_path,
        output_dir=tmp_path / "Output Covers",
    )
    captured: dict[str, Path] = {}

    def _fake_archive_non_winners(**kwargs):  # type: ignore[no-untyped-def]
        captured["selections"] = kwargs["selections_path"]
        captured["log"] = kwargs["log_path"]
        return {"ok": True}

    monkeypatch.setattr(archive_non_winners.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(archive_non_winners, "archive_non_winners", _fake_archive_non_winners)
    monkeypatch.setattr(sys, "argv", ["archive_non_winners.py", "--catalog", "demo"])
    assert archive_non_winners.main() == 0
    assert captured["selections"] == (tmp_path / "winner_selections_demo.json")
    assert captured["log"] == (tmp_path / "archive_log_demo.json")


def test_generate_catalog_main_uses_catalog_scoped_default_selection_path(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        catalog_id="demo",
        data_dir=tmp_path,
        output_dir=tmp_path / "Output Covers",
        book_catalog_path=tmp_path / "book_catalog_demo.json",
        prompts_path=tmp_path / "book_prompts_demo.json",
    )
    captured: dict[str, Path] = {}

    def _fake_generate_catalog(**kwargs):  # type: ignore[no-untyped-def]
        captured["selections"] = kwargs["selections"]
        return tmp_path / "out.pdf"

    monkeypatch.setattr(generate_catalog.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(generate_catalog, "generate_catalog", _fake_generate_catalog)
    monkeypatch.setattr(sys, "argv", ["generate_catalog.py", "--catalog", "demo"])
    assert generate_catalog.main() == 0
    assert captured["selections"] == (tmp_path / "winner_selections_demo.json")


def test_prepare_print_delivery_main_uses_catalog_scoped_default_selection_path(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(catalog_id="demo", data_dir=tmp_path)
    captured: dict[str, Path] = {}

    def _fake_prepare_print_delivery(**kwargs):  # type: ignore[no-untyped-def]
        captured["catalog_id"] = kwargs["catalog_id"]
        captured["selections"] = kwargs["selections"]
        return {"preflight_failures": []}

    monkeypatch.setattr(prepare_print_delivery.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(prepare_print_delivery, "prepare_print_delivery", _fake_prepare_print_delivery)
    monkeypatch.setattr(
        sys,
        "argv",
        ["prepare_print_delivery.py", "--catalog", "demo", "--output", str(tmp_path / "delivery")],
    )
    assert prepare_print_delivery.main() == 0
    assert captured["catalog_id"] == "demo"
    assert captured["selections"] == (tmp_path / "winner_selections_demo.json")


def test_gdrive_sync_main_winners_only_uses_catalog_scoped_default_selection_path(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        catalog_id="demo",
        output_dir=tmp_path / "Output Covers",
        gdrive_output_folder_id="folder-id",
        google_credentials_path="",
        config_dir=tmp_path / "config",
        data_dir=tmp_path,
    )
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.config_dir.mkdir(parents=True, exist_ok=True)
    selections_path = tmp_path / "winner_selections_demo.json"
    selections_path.write_text(json.dumps({"selections": {}}), encoding="utf-8")

    captured: dict[str, Path] = {}

    def _fake_build_winners_sync_tree(*, output_dir, selections_path):  # type: ignore[no-untyped-def]
        captured["selections"] = selections_path
        staging = tmp_path / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        return staging, {"staged_books": 0}

    monkeypatch.setattr(gdrive_sync.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(gdrive_sync, "_build_winners_sync_tree", _fake_build_winners_sync_tree)
    monkeypatch.setattr(gdrive_sync, "sync_to_drive", lambda **_kwargs: {"status": "ok"})
    monkeypatch.setattr(sys, "argv", ["gdrive_sync.py", "--catalog", "demo", "--winners-only"])
    assert gdrive_sync.main() == 0
    assert captured["selections"] == selections_path


def test_mockup_and_social_main_use_catalog_scoped_default_selection_path(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(catalog_id="demo", data_dir=tmp_path)
    captured: dict[str, str] = {}

    monkeypatch.setattr(mockup_generator.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(
        mockup_generator,
        "generate_all_mockups",
        lambda **kwargs: (captured.__setitem__("mockups", kwargs["selections_path"]) or {"ok": True}),
    )
    monkeypatch.setattr(sys, "argv", ["mockup_generator.py", "--catalog", "demo"])
    assert mockup_generator.main() == 0
    assert captured["mockups"].endswith("winner_selections_demo.json")

    monkeypatch.setattr(social_card_generator.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(
        social_card_generator,
        "generate_social_cards",
        lambda **kwargs: (captured.__setitem__("social", kwargs["selections_path"]) or {"ok": True}),
    )
    monkeypatch.setattr(sys, "argv", ["social_card_generator.py", "--catalog", "demo"])
    assert social_card_generator.main() == 0
    assert captured["social"].endswith("winner_selections_demo.json")


def test_import_catalog_writes_catalog_scoped_regions_without_default_overwrite(tmp_path: Path, monkeypatch):
    input_dir = tmp_path / "Input Demo"
    output_dir = tmp_path / "Output Demo"
    config_dir = tmp_path / "config"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    config_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / "1. Demo Title - Demo Author").mkdir()

    monkeypatch.setattr(import_catalog.config, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(import_catalog.config, "CATALOGS_PATH", config_dir / "catalogs.json")
    monkeypatch.setattr(import_catalog.config, "_load_catalogs_payload", lambda: {"catalogs": []})
    monkeypatch.setattr(import_catalog.prompt_generator, "generate_all_prompts", lambda **_kwargs: [{"number": 1}])
    monkeypatch.setattr(import_catalog.prompt_generator, "save_prompts", lambda *_a, **_k: None)

    captured: dict[str, Path] = {}

    def _fake_analyze_all_covers(input_dir, *, template_id, regions_path):  # type: ignore[no-untyped-def]
        captured["regions_path"] = regions_path
        return {"cover_count": 1, "covers": []}

    monkeypatch.setattr(import_catalog.cover_analyzer, "analyze_all_covers", _fake_analyze_all_covers)
    result = import_catalog.import_catalog(
        name="Demo",
        catalog_id="demo",
        input_dir=input_dir,
        output_dir=output_dir,
        cover_style="navy_gold_medallion",
    )

    assert captured["regions_path"] == (config_dir / "cover_regions_demo.json")
    assert result["regions_file"].endswith("cover_regions_demo.json")


def test_regenerate_weak_main_configures_catalog_scoped_paths(tmp_path: Path, monkeypatch):
    runtime = SimpleNamespace(
        catalog_id="demo",
        data_dir=tmp_path / "data",
        tmp_dir=tmp_path / "tmp",
        output_dir=tmp_path / "Output Covers",
        book_catalog_path=tmp_path / "config" / "book_catalog_demo.json",
    )
    runtime.data_dir.mkdir(parents=True, exist_ok=True)
    runtime.tmp_dir.mkdir(parents=True, exist_ok=True)
    runtime.output_dir.mkdir(parents=True, exist_ok=True)
    runtime.book_catalog_path.parent.mkdir(parents=True, exist_ok=True)

    captured: dict[str, Path] = {}

    def _fake_regenerate_weak_books(**_kwargs):  # type: ignore[no-untyped-def]
        captured["winners"] = regenerate_weak.WINNER_SELECTIONS_PATH
        captured["catalog"] = regenerate_weak.CATALOG_PATH
        captured["output"] = regenerate_weak.OUTPUT_DIR
        return {"ok": True}

    monkeypatch.setattr(regenerate_weak.config, "get_config", lambda *_a, **_k: runtime)
    monkeypatch.setattr(regenerate_weak, "regenerate_weak_books", _fake_regenerate_weak_books)
    monkeypatch.setattr(sys, "argv", ["regenerate_weak.py", "--catalog", "demo"])
    assert regenerate_weak.main() == 0
    assert captured["winners"] == (runtime.data_dir / "winner_selections_demo.json")
    assert captured["catalog"] == runtime.book_catalog_path
    assert captured["output"] == runtime.output_dir
