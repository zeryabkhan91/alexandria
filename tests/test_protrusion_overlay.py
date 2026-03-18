from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from PIL import Image

from src import protrusion_overlay as po


def test_apply_shared_protrusion_overlay_centers_and_masks_black_background(tmp_path: Path, monkeypatch) -> None:
    overlay_path = tmp_path / "frame.png"
    overlay = Image.new("RGB", (20, 40), (0, 0, 0))
    for y in range(1, 9):
        for x in range(8, 12):
            overlay.putpixel((x, y), (250, 220, 40))
    for y in range(27, 35):
        for x in range(6, 14):
            overlay.putpixel((x, y), (250, 220, 40))
    overlay.save(overlay_path, format="PNG")

    monkeypatch.setattr(po.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        po.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(center_x=50, center_y=50, frame_hole_radius=20, art_clip_radius=20, radius_scale=1.0),
    )
    po._load_overlay_rgba.cache_clear()

    base = Image.new("RGB", (100, 100), (20, 30, 80))
    result, details = po.apply_shared_protrusion_overlay(
        image=base,
        center_x=50,
        center_y=50,
        cover_size=(100, 100),
        overlay_path=overlay_path,
    )

    assert details["applied"] is True
    assert [component["name"] for component in details["components"]] == ["top", "bottom"]
    assert details["paste_x"] == 40
    assert details["paste_y"] == 30
    assert details["components"][0]["paste_x"] == 48
    assert details["components"][0]["paste_y"] == 31
    assert details["components"][1]["paste_x"] == 46
    assert details["components"][1]["paste_y"] == 57
    assert result.getpixel((50, 35))[0] > 200
    assert result.getpixel((50, 60))[0] > 200
    assert result.getpixel((40, 40)) == (20, 30, 80)
