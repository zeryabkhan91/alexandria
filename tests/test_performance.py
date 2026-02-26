from __future__ import annotations

from dataclasses import replace
import json
import time
from pathlib import Path

import pytest

from src import config
from src import repository


pytestmark = pytest.mark.performance


def _build_runtime(tmp_path: Path, count: int = 2500) -> config.Config:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "Output Covers"
    input_dir = tmp_path / "Input Covers"
    tmp_dir = tmp_path / "tmp"
    for path in (config_dir, data_dir, output_dir, input_dir, tmp_dir):
        path.mkdir(parents=True, exist_ok=True)

    catalog = []
    for i in range(1, count + 1):
        catalog.append(
            {
                "number": i,
                "title": f"Synthetic Title {i}",
                "author": f"Synthetic Author {i}",
                "genre": "synthetic",
                "folder_name": f"{i}. Synthetic Title {i} - Synthetic Author",
            }
        )
    catalog_path = config_dir / "book_catalog.json"
    catalog_path.write_text(json.dumps(catalog), encoding="utf-8")
    prompts_path = config_dir / "book_prompts.json"
    prompts_path.write_text("{}", encoding="utf-8")
    library_path = config_dir / "prompt_library.json"
    library_path.write_text(json.dumps({"prompts": [], "style_anchors": []}), encoding="utf-8")

    runtime = config.get_config()
    return replace(
        runtime,
        catalog_id="classics",
        book_catalog_path=catalog_path,
        prompts_path=prompts_path,
        prompt_library_path=library_path,
        config_dir=config_dir,
        data_dir=data_dir,
        output_dir=output_dir,
        input_dir=input_dir,
        tmp_dir=tmp_dir,
        use_sqlite=False,
    )


def test_list_2500_books_under_500ms(tmp_path: Path):
    runtime = _build_runtime(tmp_path, count=2500)
    repo = repository.JsonBookRepository(runtime)
    start = time.perf_counter()
    rows, total = repo.list_books(catalog_id="classics", limit=25, offset=0, filters={}, sort="book_number", order="asc")
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert total == 2500
    assert len(rows) == 25
    assert elapsed_ms < 500


def test_search_2500_books_under_300ms(tmp_path: Path):
    runtime = _build_runtime(tmp_path, count=2500)
    repo = repository.JsonBookRepository(runtime)
    start = time.perf_counter()
    rows, total = repo.list_books(catalog_id="classics", limit=25, offset=0, filters={"search": "Title 2450"}, sort="book_number", order="asc")
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert total >= 1
    assert rows
    assert elapsed_ms < 300


def test_generation_history_pagination(tmp_path: Path):
    runtime = _build_runtime(tmp_path, count=100)
    history_path = runtime.data_dir / "generation_history.json"
    entries = [
        {"book_number": (i % 100) + 1, "model": "model", "status": "success", "timestamp": f"2026-02-20T00:{i%60:02d}:00+00:00"}
        for i in range(10_000)
    ]
    history_path.write_text(json.dumps({"items": entries}), encoding="utf-8")

    repo = repository.JsonBookRepository(runtime)
    page1, total = repo.list_generation_history(catalog_id="classics", limit=50, offset=0, filters={})
    page2, _ = repo.list_generation_history(catalog_id="classics", limit=50, offset=50, filters={})
    assert total == 10_000
    assert len(page1) == 50
    assert len(page2) == 50
    assert page1 != page2
