from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from PIL import Image, ImageDraw
import pytest

from src import export_amazon
from src import export_ingram
from src import export_social
from src import export_web


def _fixture(tmp_path: Path) -> dict[str, Path]:
    catalog_path = tmp_path / "book_catalog.json"
    output_root = tmp_path / "Output Covers"
    selections_path = tmp_path / "winner_selections.json"
    quality_path = tmp_path / "quality_scores.json"
    exports_root = tmp_path / "exports"

    row = {
        "number": 1,
        "title": "Test Title",
        "author": "Test Author",
        "genre": "fiction",
        "folder_name": "1. Test Title - Test Author",
        "isbn": "TEST-ISBN-1",
        "page_count": 320,
    }
    catalog_path.write_text(json.dumps([row]), encoding="utf-8")
    selections_path.write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")
    quality_path.write_text(json.dumps({"scores": [{"book_number": 1, "variant_id": 1, "overall_score": 0.88}]}), encoding="utf-8")

    variant_dir = output_root / row["folder_name"] / "Variant-1"
    variant_dir.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (1200, 800), (30, 60, 120)).save(variant_dir / "cover.jpg", format="JPEG", quality=90)
    return {
        "catalog_path": catalog_path,
        "output_root": output_root,
        "selections_path": selections_path,
        "quality_path": quality_path,
        "exports_root": exports_root,
    }


def test_export_amazon_catalog(tmp_path: Path):
    fx = _fixture(tmp_path)
    summary = export_amazon.export_catalog(
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
    )
    assert summary["books_exported"] == 1
    out_dir = fx["exports_root"] / "amazon" / "classics" / "1"
    assert out_dir.exists()
    assert len(list(out_dir.glob("*.jpg"))) >= 7


def test_export_amazon_font_and_resize_edge_paths(monkeypatch):
    monkeypatch.setattr(export_amazon.ImageFont, "truetype", lambda *_a, **_k: (_ for _ in ()).throw(OSError("missing")))
    monkeypatch.setattr(export_amazon.ImageFont, "load_default", export_amazon.ImageFont.load_default_imagefont)
    font = export_amazon._load_font(20)
    assert font is not None

    valid = Image.new("RGB", (800, 1000), (0, 0, 0))
    assert export_amazon._ensure_kdp_size(valid) is valid

    extreme = Image.new("RGB", (20000, 100), (0, 0, 0))
    resized = export_amazon._ensure_kdp_size(extreme)
    assert resized.size[0] <= 10000
    assert resized.size[1] >= 625


def test_export_amazon_missing_winner_and_catalog_error_collection(tmp_path: Path):
    fx = _fixture(tmp_path)

    with pytest.raises(ValueError):
        export_amazon.export_book(
            book_number=999,
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
        )

    summary = export_amazon.export_catalog(
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        books=[1, 2],
    )
    assert summary["books_requested"] == 2
    assert summary["books_exported"] == 1
    assert len(summary["errors"]) == 1


def test_export_social_platform_dimensions(tmp_path: Path):
    fx = _fixture(tmp_path)
    summary = export_social.export_book(
        book_number=1,
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        platforms=["instagram", "facebook"],
    )
    assert summary["file_count"] >= 5
    image_path = fx["exports_root"] / summary["files"][0]
    with Image.open(image_path) as im:
        assert im.width in {1080, 1200}
        assert im.height in {1080, 1350, 1920, 630}


def test_export_web_manifest(tmp_path: Path):
    fx = _fixture(tmp_path)
    summary = export_web.export_catalog(
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
    )
    assert summary["books_exported"] == 1
    manifest_path = Path(summary["manifest_path"])
    assert manifest_path.exists()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["count"] == 1


def test_export_ingram_handles_missing_reportlab(tmp_path: Path):
    fx = _fixture(tmp_path)
    if export_ingram.canvas is None:
        summary = export_ingram.export_catalog(
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
        )
        assert summary["books_exported"] == 0
        assert summary["errors"]
    else:
        summary = export_ingram.export_catalog(
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
        )
        assert summary["books_exported"] == 1


def test_export_ingram_runtime_and_missing_winner_paths(tmp_path: Path, monkeypatch):
    fx = _fixture(tmp_path)

    monkeypatch.setattr(export_ingram, "canvas", None)
    monkeypatch.setattr(export_ingram, "inch", None)
    with pytest.raises(RuntimeError):
        export_ingram.export_book(
            book_number=1,
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
        )

    monkeypatch.setattr(export_ingram, "canvas", SimpleNamespace(Canvas=object))
    monkeypatch.setattr(export_ingram, "inch", 72.0)
    with pytest.raises(ValueError):
        export_ingram.export_book(
            book_number=999,
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
        )


def test_export_ingram_catalog_collects_errors(tmp_path: Path):
    fx = _fixture(tmp_path)
    summary = export_ingram.export_catalog(
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        books=[999],
    )
    assert summary["books_requested"] == 1
    assert summary["books_exported"] == 0
    assert len(summary["errors"]) == 1


def test_export_social_normalize_platform_inputs():
    all_from_none = export_social._normalize_platforms(None)
    all_from_string = export_social._normalize_platforms("all")
    subset_from_csv = export_social._normalize_platforms("instagram,facebook,invalid")
    subset_from_list = export_social._normalize_platforms([" instagram ", "facebook", "facebook", "bad"])

    assert all_from_none == sorted(export_social.SOCIAL_SPECS.keys())
    assert all_from_string == sorted(export_social.SOCIAL_SPECS.keys())
    assert subset_from_csv == ["instagram", "facebook"]
    assert subset_from_list == ["facebook", "instagram"]


def test_export_social_wrap_and_font_fallback(monkeypatch):
    monkeypatch.setattr(export_social.ImageFont, "truetype", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("missing")))
    monkeypatch.setattr(export_social.ImageFont, "load_default", export_social.ImageFont.load_default_imagefont)
    font = export_social._load_font(24)
    assert font is not None

    image = Image.new("RGB", (400, 400), (0, 0, 0))
    draw = ImageDraw.Draw(image)
    lines = export_social._wrap_text(
        draw,
        "This is a long sentence that should wrap into multiple lines for testing.",
        font=font,
        max_width=120,
    )
    assert len(lines) >= 2


def test_export_social_book_and_catalog_error_paths(tmp_path: Path):
    fx = _fixture(tmp_path)

    with pytest.raises(ValueError):
        export_social.export_book(
            book_number=999,
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
            platforms=["instagram"],
        )

    with pytest.raises(ValueError):
        export_social.export_book(
            book_number=1,
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
            platforms="invalid_platform",
        )

    summary = export_social.export_catalog(
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        books=[1, 2],
        platforms=["instagram"],
    )
    assert summary["books_requested"] == 2
    assert summary["books_exported"] == 1
    assert len(summary["errors"]) == 1


def test_export_web_resize_mockups_and_error_paths(tmp_path: Path):
    fx = _fixture(tmp_path)

    # _resize_longest keeps small images unchanged by size.
    tiny = Image.new("RGB", (100, 60), (20, 20, 20))
    resized = export_web._resize_longest(tiny, 200)
    assert resized.size == tiny.size

    # _find_mockups returns at most 6 entries and empty for missing folder.
    folder_name = "1. Test Title - Test Author"
    for idx in range(8):
        mockup = fx["output_root"] / "Mockups" / folder_name / f"m{idx}.jpg"
        mockup.parent.mkdir(parents=True, exist_ok=True)
        Image.new("RGB", (200, 120), (10, 10, 10)).save(mockup, format="JPEG")
    assert len(export_web._find_mockups(fx["output_root"], folder_name)) == 6
    assert export_web._find_mockups(fx["output_root"], "missing-folder") == []

    with pytest.raises(ValueError):
        export_web.export_book(
            book_number=999,
            catalog_id="classics",
            catalog_path=fx["catalog_path"],
            output_root=fx["output_root"],
            selections_path=fx["selections_path"],
            quality_path=fx["quality_path"],
            exports_root=fx["exports_root"],
        )

    summary = export_web.export_catalog(
        catalog_id="classics",
        catalog_path=fx["catalog_path"],
        output_root=fx["output_root"],
        selections_path=fx["selections_path"],
        quality_path=fx["quality_path"],
        exports_root=fx["exports_root"],
        books=[1, 2],
    )
    assert summary["books_requested"] == 2
    assert summary["books_exported"] == 1
    assert len(summary["errors"]) == 1
