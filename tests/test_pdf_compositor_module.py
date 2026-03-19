from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from PIL import Image, ImageDraw

from src import pdf_compositor as pc


def _save_jpg(path: Path, image: Image.Image) -> None:
    image.save(path, format="JPEG", quality=100, subsampling=0)


def _configure_fake_im0(
    monkeypatch, *, mapped_left: int = 10, mapped_top: int = 10, mapped_size: int = 80, mapped_cx: float = 50.0, mapped_cy: float = 50.0
) -> None:
    monkeypatch.setattr(pc, "EXPECTED_JPG_SIZE", (100, 100))
    monkeypatch.setattr(
        pc,
        "_extract_im0_transform",
        lambda _pdf: {
            "im0_w": mapped_size,
            "im0_h": mapped_size,
            "cm_a": mapped_size,
            "cm_d": mapped_size,
            "cm_tx": mapped_left,
            "cm_ty": mapped_top,
            "page_w_pts": 100,
            "page_h_pts": 100,
        },
    )
    monkeypatch.setattr(
        pc,
        "_im0_to_jpg_mapping",
        lambda _transform, _jpg_w, _jpg_h: {
            "im0_left": mapped_left,
            "im0_top": mapped_top,
            "im0_w_jpg": mapped_size,
            "im0_h_jpg": mapped_size,
            "im0_cx": mapped_cx,
            "im0_cy": mapped_cy,
            "im0_to_jpg_scale_x": 1.0,
            "im0_to_jpg_scale_y": 1.0,
            "scale_x": 1.0,
            "scale_y": 1.0,
        },
    )


def test_composite_cover_pdf_radial_blend(tmp_path: Path, monkeypatch) -> None:
    _configure_fake_im0(monkeypatch)

    source_pdf = tmp_path / "source.pdf"
    source_pdf.write_bytes(b"%PDF-FAKE")
    source_jpg = tmp_path / "source.jpg"
    base = Image.new("RGB", (100, 100), (18, 28, 90))
    draw = ImageDraw.Draw(base)
    draw.ellipse((15, 15, 85, 85), fill=(165, 52, 46))
    _save_jpg(source_jpg, base)

    ai_art = Image.new("RGB", (80, 80), (40, 210, 70))
    ai_art_path = tmp_path / "art.png"
    ai_art.save(ai_art_path)

    result = pc.composite_cover_pdf(
        source_pdf_path=str(source_pdf),
        ai_art_path=str(ai_art_path),
        output_pdf_path=str(tmp_path / "out.pdf"),
        output_jpg_path=str(tmp_path / "out.jpg"),
        source_jpg_path=str(source_jpg),
    )

    assert result["success"] is True
    assert "center_x" in result
    assert "center_y" in result
    assert "valid" in result
    assert "issues" in result
    assert "validation_metrics" in result


def test_composite_cover_pdf_standard_geometry_fills_shared_radius(tmp_path: Path, monkeypatch) -> None:
    _configure_fake_im0(monkeypatch)
    monkeypatch.setattr(pc.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        pc.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(center_x=50, center_y=50, frame_hole_radius=24, art_clip_radius=30),
    )
    monkeypatch.setattr(pc.replacement_frame, "is_active_for_size", lambda _size: True)
    captured: dict[str, object] = {}

    def _fake_replacement(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        out = kwargs["image"].copy()
        out.putpixel((50, 50), (40, 210, 70))
        return out, {
            "applied": True,
            "replacement_frame_mode": "single_frame_standard_medallion",
            "clear_radius": 30,
            "hole_radius": 24,
            "overlay_width": 40,
            "overlay_height": 40,
            "paste_x": 30,
            "paste_y": 30,
            "fill_policy": "fixed_standard_navy",
            "fill_rgb": (26, 39, 68),
            "legacy_outer_radius": 30,
            "overlay_outer_radius_scaled": 30.0,
            "outer_fit_scale": 1.25,
            "outer_radius_error_px": 0.0,
            "moat_band_width_px": 6.0,
            "derived_rgba_path": "derived.png",
        }

    monkeypatch.setattr(pc.replacement_frame, "apply_replacement_frame_composite", _fake_replacement)
    monkeypatch.setattr(
        pc.replacement_frame,
        "compute_outside_change_metrics",
        lambda **_kwargs: {"outside_changed_pct": 0.0, "outside_mean_delta": 0.0},
    )

    source_pdf = tmp_path / "source.pdf"
    source_pdf.write_bytes(b"%PDF-FAKE")
    source_jpg = tmp_path / "source.jpg"
    base = Image.new("RGB", (100, 100), (18, 28, 90))
    draw = ImageDraw.Draw(base)
    draw.ellipse((20, 20, 80, 80), fill=(165, 52, 46))
    _save_jpg(source_jpg, base)

    ai_art = Image.new("RGB", (80, 80), (40, 210, 70))
    ai_art_path = tmp_path / "art.png"
    ai_art.save(ai_art_path)

    out_jpg = tmp_path / "out.jpg"
    result = pc.composite_cover_pdf(
        source_pdf_path=str(source_pdf),
        ai_art_path=str(ai_art_path),
        output_pdf_path=str(tmp_path / "out.pdf"),
        output_jpg_path=str(out_jpg),
        source_jpg_path=str(source_jpg),
        regions_path=tmp_path / "missing_regions.json",
    )

    assert result["placement_source"] == "template_geometry"
    assert result["overlay_source"] == "replacement_frame_overlay"
    assert result["replacement_frame"]["fill_policy"] == "fixed_standard_navy"
    assert result["replacement_frame"]["outer_radius_error_px"] == 0.0
    assert captured["frame_bbox"] is None
    assert captured["geometry_source"] == "template_geometry"


def test_composite_cover_pdf_applies_protrusion_overlay(tmp_path: Path, monkeypatch) -> None:
    _configure_fake_im0(monkeypatch)
    monkeypatch.setattr(pc.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(
        pc.frame_geometry,
        "resolve_standard_medallion_geometry",
        lambda _size: SimpleNamespace(center_x=50, center_y=50, frame_hole_radius=24, art_clip_radius=30),
    )
    monkeypatch.setattr(pc.replacement_frame, "is_active_for_size", lambda _size: False)

    calls: list[dict[str, int]] = []

    def _fake_overlay(*, image, center_x, center_y, cover_size, overlay_path=pc.protrusion_overlay.SHARED_PROTRUSION_OVERLAY_PATH):
        calls.append({"center_x": int(center_x), "center_y": int(center_y), "width": int(cover_size[0]), "height": int(cover_size[1])})
        out = image.copy()
        out.putpixel((50, 5), (250, 220, 40))
        return out, {"applied": True, "reason": "test", "overlay_width": 10, "overlay_height": 10, "paste_x": 45, "paste_y": 0}

    monkeypatch.setattr(pc.protrusion_overlay, "apply_shared_protrusion_overlay", _fake_overlay)

    source_pdf = tmp_path / "source.pdf"
    source_pdf.write_bytes(b"%PDF-FAKE")
    source_jpg = tmp_path / "source.jpg"
    _save_jpg(source_jpg, Image.new("RGB", (100, 100), (18, 28, 90)))
    ai_art_path = tmp_path / "art.png"
    Image.new("RGB", (80, 80), (40, 210, 70)).save(ai_art_path)

    out_jpg = tmp_path / "out.jpg"
    result = pc.composite_cover_pdf(
        source_pdf_path=str(source_pdf),
        ai_art_path=str(ai_art_path),
        output_pdf_path=str(tmp_path / "out.pdf"),
        output_jpg_path=str(out_jpg),
        source_jpg_path=str(source_jpg),
        regions_path=tmp_path / "missing_regions.json",
    )

    assert calls == [{"center_x": 50, "center_y": 50, "width": 100, "height": 100}]
    assert result["protrusion_overlay"]["applied"] is True
    with Image.open(out_jpg) as out:
        assert out.convert("RGB").getpixel((50, 5))[0] > 200
