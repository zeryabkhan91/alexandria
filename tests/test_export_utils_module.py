from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from src import export_utils


def test_default_isbn_and_quality_lookup_paths(tmp_path: Path):
    assert export_utils.default_isbn(7) == "BOOK00007"
    assert export_utils._safe_int("x", 9) == 9
    assert export_utils._safe_float("x", 1.5) == 1.5
    assert export_utils._find_first_jpg(tmp_path / "missing") is None

    quality_path = tmp_path / "quality.json"
    assert export_utils._quality_lookup(quality_path) == {}
    quality_path.write_text("{bad", encoding="utf-8")
    assert export_utils._quality_lookup(quality_path) == {}
    quality_path.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 1, "overall_score": 0.5},
                    {"book_number": 1, "variant_id": 1, "overall_score": 0.8},
                    {"book_number": 0, "variant_id": 1, "overall_score": 0.9},
                    "bad-row",
                ]
            }
        ),
        encoding="utf-8",
    )
    lookup = export_utils._quality_lookup(quality_path)
    assert lookup[(1, 1)] == 0.8


def test_load_winner_books_happy_path_and_defaults(tmp_path: Path, monkeypatch):
    catalog_path = tmp_path / "book_catalog.json"
    output_root = tmp_path / "Output Covers"
    selections_path = tmp_path / "winner_selections.json"
    quality_path = tmp_path / "quality_scores.json"

    catalog_path.write_text(
        json.dumps(
            [
                {"number": 1, "title": "A", "author": "B", "isbn": "", "page_count": 0},
                {"number": 2, "title": "C", "author": "D", "isbn": "ISBN-2", "page_count": 222},
            ]
        ),
        encoding="utf-8",
    )
    quality_path.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 3, "overall_score": 0.77},
                    {"book_number": 2, "variant_id": 1, "overall_score": 0.66},
                ]
            }
        ),
        encoding="utf-8",
    )

    # Book 1 has cover image; book 2 will be skipped due missing cover file.
    variant1 = output_root / "1. Folder" / "Variant-3"
    variant1.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (100, 100), (20, 30, 40)).save(variant1 / "cover.jpg", format="JPEG")

    records = {
        1: SimpleNamespace(title="Title 1", author="Author 1", folder_name="1. Folder"),
        2: SimpleNamespace(title="Title 2", author="Author 2", folder_name="2. Folder"),
    }
    winner_map = {1: 3, 2: 1, 3: 1}
    monkeypatch.setattr(export_utils.mockup_generator, "load_book_records", lambda _catalog: records)
    monkeypatch.setattr(export_utils.mockup_generator, "load_winner_map", lambda _sel: winner_map)

    winners = export_utils.load_winner_books(
        catalog_path=catalog_path,
        output_root=output_root,
        selections_path=selections_path,
        quality_path=quality_path,
    )
    assert sorted(winners.keys()) == [1]
    book = winners[1]
    assert book.isbn == export_utils.default_isbn(1)
    assert book.page_count == 320
    assert book.quality_score == 0.77


def test_load_winner_books_invalid_catalog_and_no_quality_path(tmp_path: Path, monkeypatch):
    catalog_path = tmp_path / "book_catalog.json"
    output_root = tmp_path / "Output Covers"
    selections_path = tmp_path / "winner_selections.json"
    catalog_path.write_text("{bad", encoding="utf-8")

    variant = output_root / "1. Folder" / "Variant-1"
    variant.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (80, 80), (10, 20, 30)).save(variant / "cover.jpg", format="JPEG")

    monkeypatch.setattr(
        export_utils.mockup_generator,
        "load_book_records",
        lambda _catalog: {1: SimpleNamespace(title="Title", author="Author", folder_name="1. Folder")},
    )
    monkeypatch.setattr(export_utils.mockup_generator, "load_winner_map", lambda _sel: {1: 1})

    winners = export_utils.load_winner_books(
        catalog_path=catalog_path,
        output_root=output_root,
        selections_path=selections_path,
        quality_path=None,
    )
    assert winners[1].quality_score == 0.0


def test_crop_cover_regions_and_ensure_rgb_jpeg(tmp_path: Path):
    cover = Image.new("RGB", (3784, 2777), (30, 40, 50))
    front, spine, back, detail = export_utils.crop_cover_regions(cover)
    assert front.width > 0 and front.height == cover.height
    assert spine.width > 0 and spine.height == cover.height
    assert back.width > 0 and back.height == cover.height
    assert detail.width > 0 and detail.height > 0

    target = tmp_path / "out" / "img.jpg"
    export_utils.ensure_rgb_jpeg(front, quality=200, dpi=(72, 72), destination=target)
    assert target.exists()
