from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image

from src import cover_compositor as cc


def _make_rgb(path: Path, color=(20, 30, 50), size=(700, 500)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=color).save(path, format="JPEG")


def _make_rgba(path: Path, color=(220, 180, 120, 255), size=(300, 300)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGBA", size, color=color).save(path, format="PNG")


def test_region_and_parser_helpers():
    region = cc._region_from_dict(
        {
            "center_x": 200,
            "center_y": 150,
            "radius": 80,
            "frame_bbox": [100, 50, 300, 250],
            "region_type": "rectangle",
            "rect_bbox": [120, 90, 250, 220],
            "mask_path": "x.png",
        }
    )
    assert region.region_type == "rectangle"
    assert region.rect_bbox == (120, 90, 250, 220)
    assert cc._parse_variant("variant_3") == 3
    assert cc._parse_variant("bad") == 0
    assert cc._parse_books("1,3-4") == [1, 3, 4]


def test_circle_and_rect_masks():
    circle = cc._build_circle_feather_mask(width=200, height=200, center_x=100, center_y=100, radius=50, feather_px=10)
    rect = cc._build_rect_feather_mask(width=200, height=200, bbox=(40, 50, 160, 170), feather_px=8)
    assert circle.mode == "L"
    assert rect.mode == "L"
    c = np.array(circle)
    r = np.array(rect)
    assert c[100, 100] > c[0, 0]
    assert r[60, 60] > r[0, 0]


def test_load_custom_mask_fallback_and_resize(tmp_path: Path):
    fallback = cc._load_custom_mask("missing/path.png", (40, 60))
    assert fallback.size == (40, 60)
    assert np.array(fallback).max() == 255

    mask_src = tmp_path / "mask.png"
    Image.new("L", (20, 20), 128).save(mask_src, format="PNG")
    resized = cc._load_custom_mask(str(mask_src), (50, 70))
    assert resized.size == (50, 70)


def test_composite_single_circle_and_rectangle(tmp_path: Path):
    cover = tmp_path / "cover.jpg"
    ill = tmp_path / "ill.png"
    _make_rgb(cover, size=(700, 500))
    _make_rgba(ill, size=(300, 300))

    circle_out = tmp_path / "circle.jpg"
    region_circle = {"center_x": 350, "center_y": 250, "radius": 120, "frame_bbox": [200, 100, 500, 400], "region_type": "circle"}
    cc.composite_single(cover, ill, region_circle, circle_out)
    assert circle_out.exists()
    assert circle_out.with_suffix(".jpg.validation.json").exists()
    with Image.open(circle_out) as img:
        assert img.size == (700, 500)

    rect_out = tmp_path / "rect.jpg"
    region_rect = {
        "center_x": 300,
        "center_y": 220,
        "radius": 100,
        "frame_bbox": [180, 80, 500, 380],
        "region_type": "rectangle",
        "rect_bbox": [220, 120, 480, 360],
    }
    cc.composite_single(cover, ill, region_rect, rect_out)
    assert rect_out.exists()


def test_generate_fit_overlay_and_color_match(tmp_path: Path):
    cover = tmp_path / "cover.jpg"
    ill = tmp_path / "ill.png"
    _make_rgb(cover, size=(700, 500))
    _make_rgba(ill, size=(120, 120))

    region = cc._region_from_dict({"center_x": 350, "center_y": 250, "radius": 100, "frame_bbox": [250, 150, 450, 350], "region_type": "circle"})
    matched = cc._color_match_illustration(Image.open(cover), Image.open(ill).convert("RGBA"), region)
    assert matched.mode == "RGBA"

    overlay_out = tmp_path / "fit_overlay.png"
    cc.generate_fit_overlay(cover, {"center_x": 350, "center_y": 250, "radius": 100, "frame_bbox": [250, 150, 450, 350]}, overlay_out)
    assert overlay_out.exists()


def test_find_cover_collect_generated_and_batch(tmp_path: Path):
    input_dir = tmp_path / "Input Covers"
    generated_dir = tmp_path / "tmp" / "generated"
    output_dir = tmp_path / "tmp" / "composited"
    regions_path = tmp_path / "regions.json"
    catalog_path = tmp_path / "catalog.json"

    book_folder = input_dir / "1. Test Book"
    _make_rgb(book_folder / "cover.jpg", size=(3784, 2777))
    _make_rgba(generated_dir / "1" / "model_a" / "variant_1.png", size=(512, 512))
    _make_rgba(generated_dir / "1" / "variant_2.png", size=(512, 512))

    catalog_path.write_text(json.dumps([{"number": 1, "folder_name": "1. Test Book"}]), encoding="utf-8")
    regions_payload = {"covers": [{"cover_id": 1, "center_x": 2864, "center_y": 1620, "radius": 500, "frame_bbox": [2200, 900, 3400, 2200], "region_type": "circle"}]}
    regions_path.write_text(json.dumps(regions_payload), encoding="utf-8")

    assert cc._find_cover_jpg(input_dir, 1, catalog_path=catalog_path).exists()
    collected = cc._collect_generated_for_book(generated_dir, 1)
    assert len(collected) >= 2

    summary = cc.batch_composite(
        input_dir=input_dir,
        generated_dir=generated_dir,
        output_dir=output_dir,
        regions_path=regions_path,
        book_numbers=[1],
        catalog_path=catalog_path,
    )
    assert summary["processed_books"] == 1
    assert summary["failed_books"] == 0
    assert (output_dir / "1" / "composite_validation.json").exists()


def test_composite_single_with_custom_mask(tmp_path: Path):
    cover = tmp_path / "cover.jpg"
    ill = tmp_path / "ill.png"
    custom_mask = tmp_path / "mask.png"
    _make_rgb(cover, size=(700, 500))
    _make_rgba(ill, size=(256, 256))
    Image.new("L", (700, 500), 180).save(custom_mask, format="PNG")

    out = tmp_path / "custom.jpg"
    region = {
        "center_x": 350,
        "center_y": 250,
        "radius": 120,
        "frame_bbox": [180, 80, 520, 420],
        "region_type": "custom_mask",
        "mask_path": str(custom_mask),
    }
    cc.composite_single(cover, ill, region, out)
    assert out.exists()


def test_generate_fit_overlay_rectangle_path(tmp_path: Path):
    cover = tmp_path / "cover.jpg"
    _make_rgb(cover, size=(700, 500))
    out = tmp_path / "overlay.png"
    cc.generate_fit_overlay(
        cover,
        {
            "center_x": 300,
            "center_y": 200,
            "radius": 80,
            "frame_bbox": [150, 100, 450, 340],
            "region_type": "rectangle",
            "rect_bbox": [180, 130, 420, 300],
        },
        out,
    )
    assert out.exists()


def test_composite_all_variants_raises_without_generated_images(tmp_path: Path):
    input_dir = tmp_path / "Input Covers"
    generated_dir = tmp_path / "tmp" / "generated"
    output_dir = tmp_path / "tmp" / "composited"
    catalog_path = tmp_path / "catalog.json"
    book_folder = input_dir / "3. Test Book"
    _make_rgb(book_folder / "cover.jpg", size=(3784, 2777))
    (generated_dir / "3").mkdir(parents=True, exist_ok=True)
    catalog_path.write_text(json.dumps([{"number": 3, "folder_name": "3. Test Book"}]), encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        cc.composite_all_variants(
            book_number=3,
            input_dir=input_dir,
            generated_dir=generated_dir,
            output_dir=output_dir,
            regions={"covers": [{"cover_id": 3, "center_x": 200, "center_y": 200, "radius": 100, "frame_bbox": [100, 100, 300, 300]}]},
            catalog_path=catalog_path,
        )


def test_find_cover_jpg_error_paths(tmp_path: Path):
    input_dir = tmp_path / "Input Covers"
    catalog_path = tmp_path / "catalog.json"
    catalog_path.write_text(json.dumps([{"number": 4, "folder_name": "4. Missing Folder"}]), encoding="utf-8")

    with pytest.raises(KeyError):
        cc._find_cover_jpg(input_dir, 9, catalog_path=catalog_path)
    with pytest.raises(FileNotFoundError):
        cc._find_cover_jpg(input_dir, 4, catalog_path=catalog_path)

    folder = input_dir / "4. Missing Folder"
    folder.mkdir(parents=True, exist_ok=True)
    with pytest.raises(FileNotFoundError):
        cc._find_cover_jpg(input_dir, 4, catalog_path=catalog_path)


def test_collect_generated_handles_missing_and_history_dir(tmp_path: Path):
    generated_dir = tmp_path / "generated"
    assert cc._collect_generated_for_book(generated_dir, 5) == []

    base = generated_dir / "5"
    _make_rgba(base / "history" / "variant_1.png")
    _make_rgba(base / "model_a" / "variant_1.png")
    _make_rgba(base / "model_a" / "variant_1.png")
    rows = cc._collect_generated_for_book(generated_dir, 5)
    assert len(rows) == 1
    assert rows[0]["model"] == "model_a"


def test_color_match_returns_original_when_ring_empty(tmp_path: Path):
    cover = Image.new("RGB", (20, 20), (20, 30, 40))
    illustration = Image.new("RGBA", (10, 10), (100, 120, 140, 255))
    region = cc.Region(center_x=10, center_y=10, radius=9999, frame_bbox=(0, 0, 20, 20))
    matched = cc._color_match_illustration(cover, illustration, region)
    assert matched is illustration


def test_main_book_and_batch_paths(monkeypatch, tmp_path: Path):
    regions_path = tmp_path / "regions.json"
    regions_path.write_text(json.dumps({"covers": []}), encoding="utf-8")

    book_args = SimpleNamespace(
        input_dir=tmp_path / "Input Covers",
        generated_dir=tmp_path / "generated",
        output_dir=tmp_path / "out",
        regions_path=regions_path,
        catalog_path=tmp_path / "catalog.json",
        book=10,
        books=None,
        max_books=20,
    )
    monkeypatch.setattr(cc.argparse.ArgumentParser, "parse_args", lambda self: book_args)
    monkeypatch.setattr(cc, "composite_all_variants", lambda **_kwargs: [Path("a"), Path("b")])
    assert cc.main() == 0

    batch_args = SimpleNamespace(**book_args.__dict__)
    batch_args.book = None
    batch_args.books = "1,2-3"
    monkeypatch.setattr(cc.argparse.ArgumentParser, "parse_args", lambda self: batch_args)
    monkeypatch.setattr(cc, "batch_composite", lambda **_kwargs: {"processed_books": 3})
    assert cc.main() == 0


def test_validate_composite_output_detects_alignment_and_bleed(tmp_path: Path):
    cover = Image.new("RGB", (300, 300), (20, 30, 40))
    composite = cover.copy()
    composite_arr = np.array(composite, dtype=np.uint8)
    composite_arr[30:110, 30:110] = np.array([240, 220, 180], dtype=np.uint8)
    composite = Image.fromarray(composite_arr, mode="RGB")
    out = tmp_path / "out.jpg"
    composite.save(out, format="JPEG", dpi=(300, 300))

    region = cc.Region(center_x=220, center_y=220, radius=60, frame_bbox=(160, 160, 280, 280))
    validation = cc.validate_composite_output(
        cover=cover,
        composited=composite,
        region=region,
        output_path=out,
    )
    assert validation.alignment_ok is False
    assert validation.valid is False
