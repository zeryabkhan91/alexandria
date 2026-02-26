from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

from scripts import migrate_to_sqlite
from src import config


def _runtime(tmp_path: Path) -> config.Config:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "Output Covers"
    input_dir = tmp_path / "Input Covers"
    tmp_dir = tmp_path / "tmp"
    for path in (config_dir, data_dir, output_dir, input_dir, tmp_dir):
        path.mkdir(parents=True, exist_ok=True)

    (config_dir / "book_catalog.json").write_text(
        json.dumps(
            [
                {
                    "number": 1,
                    "title": "Book 1",
                    "author": "Author 1",
                    "genre": "fiction",
                    "folder_name": "1. Book 1 - Author 1",
                }
            ]
        ),
        encoding="utf-8",
    )
    (config_dir / "book_prompts.json").write_text("{}", encoding="utf-8")
    (config_dir / "prompt_library.json").write_text(json.dumps({"prompts": [], "style_anchors": []}), encoding="utf-8")

    (data_dir / "winner_selections.json").write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")
    (data_dir / "quality_scores.json").write_text(
        json.dumps({"scores": [{"book_number": 1, "variant_id": 1, "overall_score": 0.8, "model": "m1"}]}),
        encoding="utf-8",
    )
    (data_dir / "generation_history.json").write_text(
        json.dumps({"items": [{"book_number": 1, "model": "m1", "status": "success"}]}),
        encoding="utf-8",
    )
    (data_dir / "audit_log.json").write_text(json.dumps({"items": [{"action": "test", "catalog_id": "classics"}]}), encoding="utf-8")

    runtime = config.get_config()
    return replace(
        runtime,
        catalog_id="classics",
        config_dir=config_dir,
        data_dir=data_dir,
        output_dir=output_dir,
        input_dir=input_dir,
        tmp_dir=tmp_dir,
        book_catalog_path=config_dir / "book_catalog.json",
        prompts_path=config_dir / "book_prompts.json",
        prompt_library_path=config_dir / "prompt_library.json",
    )


def test_migrate_to_sqlite_from_json(tmp_path: Path):
    runtime = _runtime(tmp_path)
    db_path = tmp_path / "alexandria.db"
    summary = migrate_to_sqlite.migrate_to_sqlite(
        catalog_id="classics",
        db_path=db_path,
        runtime=runtime,
    )
    assert summary["ok"] is True
    assert summary["counts"]["books"] == 1
    assert summary["counts"]["variants"] >= 1
