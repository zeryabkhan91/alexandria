from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from PIL import Image, ImageDraw

from scripts import visual_qa as vqa


def _make_image(path: Path, *, size: tuple[int, int] = (400, 400), color: tuple[int, int, int] = (12, 30, 70)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", size, color)
    if path.suffix.lower() == ".png":
        image.save(path, format="PNG")
    else:
        image.save(path, format="JPEG", quality=95)


def test_verify_composite_passes_for_center_only_changes(tmp_path: Path) -> None:
    original = tmp_path / "original.png"
    composite = tmp_path / "composite.png"
    output_dir = tmp_path / "qa_output"

    _make_image(original)
    _make_image(composite)
    with Image.open(composite).convert("RGB") as image:
        draw = ImageDraw.Draw(image)
        draw.ellipse((140, 140, 260, 260), fill=(220, 200, 170))
        image.save(composite, format="PNG")

    report = vqa.verify_composite(
        original_path=original,
        composite_path=composite,
        book_number=1,
        center_x=200,
        center_y=200,
        radius=120,
        output_dir=output_dir,
        golden_dir=output_dir / "golden",
    )

    assert report["passed"] is True
    assert (output_dir / "qa_001.json").exists()


def test_verify_composite_flags_outer_bleed(tmp_path: Path) -> None:
    original = tmp_path / "original.png"
    composite = tmp_path / "composite.png"
    output_dir = tmp_path / "qa_output"

    _make_image(original)
    _make_image(composite)
    with Image.open(composite).convert("RGB") as image:
        draw = ImageDraw.Draw(image)
        draw.ellipse((140, 140, 260, 260), fill=(220, 200, 170))
        draw.rectangle((0, 0, 40, 40), fill=(240, 240, 240))
        image.save(composite, format="PNG")

    report = vqa.verify_composite(
        original_path=original,
        composite_path=composite,
        book_number=2,
        center_x=200,
        center_y=200,
        radius=120,
        output_dir=output_dir,
        golden_dir=output_dir / "golden",
    )

    assert report["passed"] is False
    assert "art_containment" in report["failed_checks"]


def test_run_batch_verification_writes_summary_report(tmp_path: Path) -> None:
    input_dir = tmp_path / "Input Covers"
    composited_dir = tmp_path / "tmp" / "composited"
    output_dir = tmp_path / "qa_output"

    source_dir = input_dir / "3. Sample Book copy"
    original = source_dir / "source.jpg"
    composite = composited_dir / "3" / "provider_a" / "variant_1.jpg"

    _make_image(original)
    _make_image(composite)
    with Image.open(composite).convert("RGB") as image:
        draw = ImageDraw.Draw(image)
        draw.ellipse((140, 140, 260, 260), fill=(210, 190, 150))
        image.save(composite, format="JPEG", quality=95)

    payload = vqa.run_batch_verification(
        input_covers_dir=input_dir,
        composited_dir=composited_dir,
        output_dir=output_dir,
        golden_dir=output_dir / "golden",
        catalog=[{"number": 3, "title": "Sample Book", "folder_name": "3. Sample Book copy"}],
        book_numbers=[3],
    )

    summary = payload.get("summary", {})
    assert int(summary.get("total", 0)) == 1
    assert int(summary.get("verified", 0)) == 1
    assert (output_dir / "qa_report.json").exists()
    data = json.loads((output_dir / "qa_report.json").read_text(encoding="utf-8"))
    assert isinstance(data.get("results"), list)
    assert len(data.get("results", [])) == 1


def test_verify_composite_replacement_frame_mode_accepts_clean_navy_outer_band(tmp_path: Path, monkeypatch) -> None:
    original = tmp_path / "original.png"
    composite = tmp_path / "composite.png"
    output_dir = tmp_path / "qa_output"

    _make_image(original)
    _make_image(composite)
    with Image.open(composite).convert("RGB") as image:
        draw = ImageDraw.Draw(image)
        draw.ellipse((120, 120, 280, 280), fill=(220, 200, 170))
        image.save(composite, format="PNG")

    monkeypatch.setattr(vqa.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        vqa.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(art_clip_radius=60, radius_scale=1.0),
    )
    monkeypatch.setattr(
        vqa.replacement_frame,
        "ensure_replacement_frame_assets",
        lambda: {"overlay_width": 180, "overlay_height": 180, "overlay_outer_radius": 90.0, "overlay_bbox_x1": 0, "overlay_bbox_y1": 0, "overlay_bbox_x2": 180, "overlay_bbox_y2": 180},
    )

    report = vqa.verify_composite(
        original_path=original,
        composite_path=composite,
        book_number=4,
        center_x=200,
        center_y=200,
        radius=120,
        output_dir=output_dir,
        golden_dir=output_dir / "golden",
        compositor_mode="replacement_frame",
        replacement_metrics={
            "source_anchor_box": [110, 110, 290, 290],
            "overlay_anchor_box_unscaled": [0, 0, 180, 180],
            "overlay_anchor_box_scaled": [110, 110, 290, 290],
            "final_scale": 1.0,
            "anchor_error_left_px": 0.0,
            "anchor_error_top_px": 0.0,
            "anchor_error_right_px": 0.0,
            "anchor_error_bottom_px": 0.0,
            "anchor_error_max_px": 0.0,
            "navy_band_max_px": 3.0,
            "clear_bbox": [107, 107, 293, 293],
        },
    )

    assert report["passed"] is True
    assert report["compositor_mode"] == "replacement_frame"
    assert report["metrics"]["anchor_error_max_px"] == 0.0
    assert report["metrics"]["navy_band_max_px"] == 3.0


def test_verify_composite_replacement_frame_mode_flags_gold_outer_band(tmp_path: Path, monkeypatch) -> None:
    original = tmp_path / "original.png"
    composite = tmp_path / "composite.png"
    output_dir = tmp_path / "qa_output"

    _make_image(original)
    _make_image(composite)
    with Image.open(composite).convert("RGB") as image:
        draw = ImageDraw.Draw(image)
        draw.ellipse((120, 120, 280, 280), fill=(220, 200, 170))
        draw.ellipse((72, 72, 328, 328), outline=(180, 145, 88), width=20)
        image.save(composite, format="PNG")

    monkeypatch.setattr(vqa.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        vqa.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(art_clip_radius=60, radius_scale=1.0),
    )
    monkeypatch.setattr(
        vqa.replacement_frame,
        "ensure_replacement_frame_assets",
        lambda: {"overlay_width": 180, "overlay_height": 180, "overlay_outer_radius": 90.0, "overlay_bbox_x1": 0, "overlay_bbox_y1": 0, "overlay_bbox_x2": 180, "overlay_bbox_y2": 180},
    )

    report = vqa.verify_composite(
        original_path=original,
        composite_path=composite,
        book_number=5,
        center_x=200,
        center_y=200,
        radius=120,
        output_dir=output_dir,
        golden_dir=output_dir / "golden",
        compositor_mode="replacement_frame",
        replacement_metrics={
            "source_anchor_box": [110, 110, 290, 290],
            "overlay_anchor_box_unscaled": [0, 0, 180, 180],
            "overlay_anchor_box_scaled": [124, 129, 304, 309],
            "final_scale": 1.0,
            "anchor_error_left_px": 14.0,
            "anchor_error_top_px": 19.0,
            "anchor_error_right_px": 14.0,
            "anchor_error_bottom_px": 19.0,
            "anchor_error_max_px": 19.0,
            "navy_band_max_px": 12.0,
            "clear_bbox": [98, 98, 316, 321],
        },
    )

    assert report["passed"] is False
    assert "replacement_anchor_max" in report["failed_checks"] or "replacement_navy_band" in report["failed_checks"] or "replacement_outer_band_clean" in report["failed_checks"] or "replacement_outer_band_not_gold" in report["failed_checks"]
