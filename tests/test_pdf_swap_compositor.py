from __future__ import annotations

import numpy as np
import pytest

from src import pdf_swap_compositor as psc
from src import pdf_compositor as pc


def test_detect_blend_radius_from_smask_is_data_driven():
    smask = np.full((21, 21), 255, dtype=np.uint8)
    yy, xx = np.ogrid[:21, :21]
    dist = np.sqrt((xx - 10) ** 2 + (yy - 10) ** 2)
    smask[dist >= 8] = 200

    detected = psc.detect_blend_radius_from_smask(smask)
    assert detected >= 20
    assert detected != psc.DEFAULT_BLEND_RADIUS


def test_build_art_mask_fades_before_preserved_ring():
    mask = psc._build_art_mask(width=21, height=21, inner_radius=4, outer_radius=8)

    assert mask[10, 10] == pytest.approx(1.0)
    assert mask[10, 17] == pytest.approx(0.25, abs=0.05)
    assert mask[0, 0] == pytest.approx(0.0)


def test_resolve_target_radii_uses_shared_template_geometry(monkeypatch: pytest.MonkeyPatch, tmp_path):
    source_pdf = tmp_path / "source.pdf"
    source_pdf.write_bytes(b"%PDF-FAKE")

    monkeypatch.setattr(psc.frame_geometry, "is_standard_medallion_cover", lambda _size: True)
    monkeypatch.setattr(psc.frame_geometry, "template_geometry_to_im0", lambda _mapping, _size: (910, 1010))
    monkeypatch.setattr(pc, "_extract_im0_transform", lambda _pdf: {"stub": True})
    monkeypatch.setattr(
        pc,
        "_im0_to_jpg_mapping",
        lambda _transform, _w, _h: {
            "im0_cx": 123.0,
            "im0_cy": 456.0,
            "im0_to_jpg_scale_x": 0.57,
            "im0_to_jpg_scale_y": 0.57,
        },
    )

    inner, outer = psc._resolve_target_radii(
        source_pdf_path=source_pdf,
        smask_arr=np.full((41, 41), 255, dtype=np.uint8),
        expected_output_size=(3784, 2777),
        requested_outer_radius=None,
        feather_px=20,
    )

    assert inner == 910
    assert outer == 910
