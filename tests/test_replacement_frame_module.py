from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
from PIL import Image, ImageDraw

from src import replacement_frame as rf


def _build_overlay() -> tuple[Image.Image, dict[str, object]]:
    overlay = Image.new("RGBA", (120, 120), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay, "RGBA")
    draw.ellipse((0, 0, 119, 119), fill=(190, 150, 95, 255))
    draw.ellipse((28, 28, 91, 91), fill=(0, 0, 0, 0))
    hole_mask = np.zeros((120, 120), dtype=bool)
    hole_mask[28:92, 28:92] = True
    meta = {
        "scale": 1.0,
        "auto_scale": 1.0,
        "auto_dx": -10,
        "auto_dy": -10,
        "final_scale": 1.0,
        "final_dx": -10,
        "final_dy": -10,
        "overlay_width": 120,
        "overlay_height": 120,
        "overlay_outer_radius": 59.5,
        "legacy_outer_radius": 80,
        "overlay_outer_radius_unscaled": 59.5,
        "overlay_outer_radius_scaled": 80.0,
        "outer_fit_scale": round(80.0 / 59.5, 6),
        "outer_radius_error_px": 0.0,
        "moat_band_width_px": 3.0,
        "navy_band_max_px": 3.0,
        "hole_radius": 32,
        "hole_bbox_x1": 28,
        "hole_bbox_y1": 28,
        "hole_bbox_x2": 92,
        "hole_bbox_y2": 92,
        "source_anchor_box": [70, 70, 230, 230],
        "overlay_anchor_box_unscaled": [0, 0, 120, 120],
        "overlay_anchor_box_scaled": [70, 70, 230, 230],
        "anchor_error_left_px": 0.0,
        "anchor_error_top_px": 0.0,
        "anchor_error_right_px": 0.0,
        "anchor_error_bottom_px": 0.0,
        "anchor_error_max_px": 0.0,
        "hole_mask": hole_mask,
        "center_x": 150,
        "center_y": 150,
        "erase_radius": 60,
        "paste_x": 90,
        "paste_y": 90,
    }
    return overlay, meta


def test_apply_replacement_frame_composite_uses_fixed_navy_fill_and_large_clear_radius(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(rf.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        rf.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(art_clip_radius=50, radius_scale=1.0),
    )
    monkeypatch.setattr(rf, "_target_anchor_box_for_standard_cover", lambda **_kwargs: ((70, 70, 230, 230), "source_silhouette"))
    monkeypatch.setattr(rf, "_compute_registered_overlay", lambda **_kwargs: _build_overlay())

    art_path = tmp_path / "art.png"
    Image.new("RGB", (100, 100), (20, 180, 80)).save(art_path, format="PNG")
    base = Image.new("RGB", (300, 300), (170, 120, 75))

    result, details = rf.apply_replacement_frame_composite(
        image=base,
        ai_art_path=art_path,
        center_x=150,
        center_y=150,
        cover_size=(300, 300),
        frame_bbox=(70, 70, 230, 230),
        geometry_source="cover_region",
    )

    assert details["applied"] is True
    assert details["replacement_frame_mode"] == "single_frame_standard_medallion"
    assert details["fill_policy"] == "fixed_standard_navy"
    assert tuple(details["fill_rgb"]) == rf.STANDARD_NAVY_FILL_RGB
    assert int(details["legacy_outer_radius"]) == 80
    assert int(details["clear_radius"]) >= 60
    assert abs(float(details["overlay_outer_radius_scaled"]) - 80.0) <= 0.001
    assert float(details["outer_radius_error_px"]) <= 0.001
    assert float(details["moat_band_width_px"]) == 3.0
    assert float(details["anchor_error_max_px"]) == 0.0
    assert details["source_anchor_box"] == [70, 70, 230, 230]
    assert details["geometry_source"] == "cover_region"
    assert result.getpixel((88, 150)) == rf.STANDARD_NAVY_FILL_RGB


def test_apply_replacement_frame_composite_places_art_in_hole_center(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(rf.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        rf.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(art_clip_radius=50, radius_scale=1.0),
    )
    monkeypatch.setattr(rf, "_target_anchor_box_for_standard_cover", lambda **_kwargs: ((70, 70, 230, 230), "source_silhouette"))
    monkeypatch.setattr(rf, "_compute_registered_overlay", lambda **_kwargs: _build_overlay())

    art = Image.new("RGB", (100, 100), (10, 200, 40))
    draw = ImageDraw.Draw(art)
    draw.rectangle((30, 30, 70, 70), fill=(250, 30, 30))
    art_path = tmp_path / "art.png"
    art.save(art_path, format="PNG")

    result, details = rf.apply_replacement_frame_composite(
        image=Image.new("RGB", (300, 300), (10, 10, 10)),
        ai_art_path=art_path,
        center_x=150,
        center_y=150,
        cover_size=(300, 300),
        frame_bbox=(70, 70, 230, 230),
        geometry_source="template_geometry",
    )

    center_pixel = result.getpixel((150, 150))
    assert center_pixel[0] > 200
    assert details["placement_center"] == [150, 150]
    assert details["hole_bbox"] == [28, 28, 92, 92]
    assert round(float(details["outer_fit_scale"]), 6) == round(80.0 / 59.5, 6)
    assert details["overlay_anchor_box_scaled"] == [70, 70, 230, 230]
