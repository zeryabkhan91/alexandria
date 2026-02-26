from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image

from src import cover_analyzer as ca


def _make_cover(path: Path, color=(26, 39, 68), size=(3784, 2777)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color=color).save(path, format="JPEG")


def test_parse_cover_id_and_sorting(tmp_path: Path):
    folder_a = tmp_path / "2. Book Two"
    folder_b = tmp_path / "1. Book One"
    folder_a.mkdir(parents=True, exist_ok=True)
    folder_b.mkdir(parents=True, exist_ok=True)
    _make_cover(folder_a / "a.jpg")
    _make_cover(folder_b / "b.jpg")

    assert ca._parse_cover_id("12. Name") == 12
    assert ca._parse_cover_id("bad") == 0
    folders = ca._sorted_cover_folders(tmp_path)
    assert [p.name for p in folders] == ["1. Book One", "2. Book Two"]
    jpgs = ca._sorted_cover_jpgs(tmp_path)
    assert len(jpgs) == 2


def test_cover_template_resolution(monkeypatch):
    monkeypatch.setattr(
        ca.config,
        "load_cover_templates",
        lambda: {"templates": [{"id": "x", "region_type": "rectangle", "defaults": {"x": 1, "y": 2, "width": 3, "height": 4}}]},
    )
    assert ca._cover_template("x")["id"] == "x"
    assert ca._cover_template("missing")["id"] == "x"


def test_cover_template_fallback_default_and_skip_non_dict(monkeypatch):
    monkeypatch.setattr(ca.config, "load_cover_templates", lambda: {"templates": ["bad-row"]})
    fallback = ca._cover_template("missing")
    assert fallback["id"] == "navy_gold_medallion"


def test_make_circle_and_rectangle_region():
    circle = ca._make_circle_region(3784, 2777, {"id": "circle", "defaults": {"center_x": 2864, "center_y": 1620, "radius": 500}})
    assert circle.region_type == "circle"
    assert circle.radius > 0
    assert len(circle.frame_bbox) == 4

    rect = ca._make_rectangle_region(
        3784,
        2777,
        {"id": "rect", "region_type": "rectangle", "defaults": {"x": 100, "y": 120, "width": 500, "height": 600}},
    )
    assert rect.region_type == "rectangle"
    assert rect.rect_bbox is not None
    assert rect.radius > 0
    assert rect.to_dict()["rect_bbox"] == list(rect.rect_bbox)


def test_compute_confidence_for_non_circle_region():
    rect_region = ca.CoverRegion(
        center_x=10,
        center_y=10,
        radius=5,
        frame_bbox=(0, 0, 20, 20),
        confidence=0.0,
        region_type="rectangle",
        rect_bbox=(0, 0, 20, 20),
    )
    rgb = np.zeros((30, 30, 3), dtype=np.uint8)
    assert ca._compute_confidence(rgb, rect_region) == 0.95


def test_analyze_cover_happy_path_and_size_error(tmp_path: Path, monkeypatch):
    cover = tmp_path / "1. Book" / "cover.jpg"
    _make_cover(cover)
    monkeypatch.setattr(
        ca.config,
        "load_cover_templates",
        lambda: {"templates": [{"id": "navy_gold_medallion", "region_type": "circle", "defaults": {"center_x": 2864, "center_y": 1620, "radius": 500}}]},
    )
    region = ca.analyze_cover(cover)
    assert region.center_x > 0
    assert 0.0 <= region.confidence <= 1.0

    bad_cover = tmp_path / "bad.jpg"
    _make_cover(bad_cover, size=(500, 500))
    try:
        ca.analyze_cover(bad_cover)
        assert False, "expected ValueError for invalid size"
    except ValueError:
        pass

    try:
        ca.analyze_cover(tmp_path / "missing.jpg")
        assert False, "expected FileNotFoundError for missing file"
    except FileNotFoundError:
        pass


def test_analyze_cover_rectangle_template_branch(tmp_path: Path, monkeypatch):
    cover = tmp_path / "1. Book" / "cover.jpg"
    _make_cover(cover)
    monkeypatch.setattr(
        ca.config,
        "load_cover_templates",
        lambda: {
            "templates": [
                {"id": "rect", "region_type": "rectangle", "defaults": {"x": 100, "y": 200, "width": 300, "height": 400}}
            ]
        },
    )
    region = ca.analyze_cover(cover, template_id="rect")
    assert region.region_type == "rectangle"
    assert region.rect_bbox is not None


def test_analyze_all_covers_and_masks(tmp_path: Path, monkeypatch):
    _make_cover(tmp_path / "1. A" / "a.jpg")
    _make_cover(tmp_path / "2. B" / "b.jpg")
    regions_out = tmp_path / "regions.json"
    debug_out = tmp_path / "debug"
    monkeypatch.setattr(ca, "DEFAULT_REGIONS_JSON", regions_out)
    monkeypatch.setattr(
        ca.config,
        "load_cover_templates",
        lambda: {"templates": [{"id": "navy_gold_medallion", "region_type": "circle", "defaults": {"center_x": 2864, "center_y": 1620, "radius": 500}}]},
    )

    payload = ca.analyze_all_covers(tmp_path, template_id="navy_gold_medallion")
    assert payload["cover_count"] == 2
    assert regions_out.exists()
    loaded = json.loads(regions_out.read_text(encoding="utf-8"))
    assert loaded["cover_count"] == 2

    consensus = ca.CoverRegion(**payload["consensus_region"])
    mask = ca.generate_compositing_mask(consensus, ca.EXPECTED_COVER_SIZE)
    assert mask.shape[2] == 4
    assert mask.dtype == np.uint8

    rect_region = ca.CoverRegion(
        center_x=100,
        center_y=100,
        radius=50,
        frame_bbox=(10, 10, 120, 120),
        confidence=0.95,
        region_type="rectangle",
        rect_bbox=(10, 20, 70, 80),
    )
    rect_mask = ca.generate_compositing_mask(rect_region, (200, 200))
    assert rect_mask[25, 15, 3] == 255

    ca.save_debug_overlays(tmp_path, consensus, debug_out, count=2)
    assert len(list(debug_out.glob("debug_overlay_*.png"))) >= 2


def test_analyze_all_covers_raises_when_missing(tmp_path: Path):
    try:
        ca.analyze_all_covers(tmp_path)
        assert False, "expected FileNotFoundError"
    except FileNotFoundError:
        pass


def test_save_debug_overlays_rectangle_branch(tmp_path: Path):
    _make_cover(tmp_path / "1. A" / "a.jpg")
    debug_out = tmp_path / "debug"
    rect = ca.CoverRegion(
        center_x=100,
        center_y=100,
        radius=50,
        frame_bbox=(20, 20, 180, 180),
        confidence=0.95,
        region_type="rectangle",
        rect_bbox=(30, 40, 140, 180),
    )
    ca.save_debug_overlays(tmp_path, rect, debug_out, count=1)
    assert (debug_out / "debug_overlay_001.png").exists()


def test_write_mask_png(tmp_path: Path):
    path = tmp_path / "mask.png"
    mask = np.zeros((10, 10, 4), dtype=np.uint8)
    mask[:, :, 3] = 255
    ca._write_mask_png(mask, path)
    assert path.exists()


def test_main_wires_analysis_mask_and_debug(monkeypatch, tmp_path: Path):
    args = SimpleNamespace(
        input_dir=tmp_path / "Input Covers",
        template_id="navy_gold_medallion",
        regions_path=tmp_path / "regions.json",
        mask_path=tmp_path / "mask.png",
        debug_dir=tmp_path / "debug",
        debug_count=2,
    )
    monkeypatch.setattr(ca.argparse.ArgumentParser, "parse_args", lambda self: args)

    payload = {
        "consensus_region": {
            "center_x": 100,
            "center_y": 100,
            "radius": 50,
            "frame_bbox": [10, 10, 190, 190],
            "confidence": 0.95,
            "region_type": "circle",
            "rect_bbox": None,
            "template_id": "navy_gold_medallion",
            "compositing": "raster_first",
        }
    }
    calls: dict[str, object] = {}

    def _fake_analyze_all_covers(input_dir, *, template_id="navy_gold_medallion", regions_path=None):  # type: ignore[no-untyped-def]
        calls["analyze"] = (input_dir, template_id, regions_path)
        return payload

    monkeypatch.setattr(ca, "analyze_all_covers", _fake_analyze_all_covers)
    monkeypatch.setattr(ca, "_write_mask_png", lambda mask, mask_path: calls.setdefault("mask_path", mask_path))
    monkeypatch.setattr(
        ca,
        "save_debug_overlays",
        lambda input_dir, consensus, debug_dir, count=5: calls.setdefault("debug", (input_dir, debug_dir, count)),
    )

    assert ca.main() == 0
    assert calls["analyze"] == (args.input_dir, args.template_id, args.regions_path)
    assert calls["mask_path"] == args.mask_path
    assert calls["debug"] == (args.input_dir, args.debug_dir, args.debug_count)


@pytest.mark.filterwarnings("ignore:'src.cover_analyzer' found in sys.modules:RuntimeWarning")
def test_module_main_entrypoint_runs(monkeypatch, tmp_path: Path):
    input_dir = tmp_path / "Input Covers"
    _make_cover(input_dir / "1. A" / "a.jpg")
    regions_path = tmp_path / "regions.json"
    mask_path = tmp_path / "mask.png"
    debug_dir = tmp_path / "debug"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "cover_analyzer",
            "--input-dir",
            str(input_dir),
            "--regions-path",
            str(regions_path),
            "--mask-path",
            str(mask_path),
            "--debug-dir",
            str(debug_dir),
            "--debug-count",
            "1",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_module("src.cover_analyzer", run_name="__main__", alter_sys=True)
    assert exc.value.code == 0
    assert regions_path.exists()
    assert mask_path.exists()
