"""Shared protrusion-only overlay applied after medallion art placement."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

try:
    from src import config
    from src import frame_geometry
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import frame_geometry  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

SHARED_PROTRUSION_OVERLAY_PATH = config.CONFIG_DIR / "frame_overlays" / "Frame_514868_frame.png"
BACKGROUND_THRESHOLD = 4.0
COMPONENT_ROW_GAP_PX = 12
COMPONENT_MIN_ROWS = 8


@lru_cache(maxsize=4)
def _load_overlay_rgba(path_str: str) -> Image.Image | None:
    path = Path(path_str)
    if not path.exists():
        return None
    try:
        with Image.open(path) as source:
            rgb = source.convert("RGB")
    except Exception:
        logger.warning("Failed to open protrusion overlay at %s", path)
        return None

    arr = np.asarray(rgb, dtype=np.uint8)
    intensity = arr.max(axis=2).astype(np.float32)
    alpha = np.where(intensity > BACKGROUND_THRESHOLD, 255, 0).astype(np.uint8)
    if int(alpha.max()) <= 5:
        logger.warning("Protrusion overlay alpha is trivial at %s", path)
        return None
    rgba = np.dstack([arr, alpha])
    return Image.fromarray(rgba, mode="RGBA")


def _extract_overlay_components(overlay: Image.Image) -> list[dict[str, Any]]:
    alpha = np.asarray(overlay.getchannel("A"), dtype=np.uint8)
    row_has = np.any(alpha > 0, axis=1)
    row_indices = np.where(row_has)[0].tolist()
    if not row_indices:
        return []

    row_groups: list[tuple[int, int]] = []
    start = row_indices[0]
    prev = row_indices[0]
    for row in row_indices[1:]:
        if int(row) - int(prev) <= COMPONENT_ROW_GAP_PX:
            prev = row
            continue
        row_groups.append((int(start), int(prev)))
        start = int(row)
        prev = int(row)
    row_groups.append((int(start), int(prev)))

    components: list[dict[str, Any]] = []
    for top, bottom in row_groups:
        if (bottom - top + 1) < COMPONENT_MIN_ROWS:
            continue
        local_alpha = alpha[top : bottom + 1, :]
        col_has = np.any(local_alpha > 0, axis=0)
        col_indices = np.where(col_has)[0].tolist()
        if not col_indices:
            continue
        left = int(col_indices[0])
        right = int(col_indices[-1]) + 1
        crop = overlay.crop((left, top, right, bottom + 1))
        components.append(
            {
                "overlay": crop,
                "bbox": (left, top, right, bottom + 1),
            }
        )
    return components


def apply_shared_protrusion_overlay(
    *,
    image: Image.Image,
    center_x: int,
    center_y: int,
    cover_size: tuple[int, int] | None = None,
    overlay_path: Path = SHARED_PROTRUSION_OVERLAY_PATH,
) -> tuple[Image.Image, dict[str, Any]]:
    """Composite the shared protruding leaf/floral motifs over the medallion art."""

    target_size = tuple(int(v) for v in (cover_size or image.size))
    details: dict[str, Any] = {
        "applied": False,
        "reason": "",
        "path": str(overlay_path),
        "requested_center_x": int(center_x),
        "requested_center_y": int(center_y),
        "applied_center_x": int(center_x),
        "applied_center_y": int(center_y),
        "cover_size": target_size,
        "overlay_width": 0,
        "overlay_height": 0,
        "paste_x": 0,
        "paste_y": 0,
        "scale": 0.0,
        "components": [],
    }

    if not frame_geometry.is_standard_medallion_cover(target_size):
        details["reason"] = "non_standard_cover"
        logger.info(
            "Shared protrusion overlay skipped: reason=%s cover_size=%s center=(%d,%d)",
            details["reason"],
            target_size,
            int(center_x),
            int(center_y),
        )
        return image, details

    overlay = _load_overlay_rgba(str(overlay_path))
    if overlay is None:
        details["reason"] = "overlay_missing_or_invalid"
        logger.warning(
            "Shared protrusion overlay skipped: reason=%s path=%s center=(%d,%d)",
            details["reason"],
            overlay_path,
            int(center_x),
            int(center_y),
        )
        return image, details

    geometry = frame_geometry.resolve_standard_medallion_geometry(target_size)
    scale = float(getattr(geometry, "radius_scale", 1.0) or 1.0)
    applied_center_x = int(center_x) if int(center_x) > 0 else int(getattr(geometry, "center_x", 0) or 0)
    applied_center_y = int(center_y) if int(center_y) > 0 else int(getattr(geometry, "center_y", 0) or 0)
    scaled_overlay = overlay
    if scale != 1.0:
        scaled_overlay = overlay.resize(
            (
                max(1, int(round(overlay.width * scale))),
                max(1, int(round(overlay.height * scale))),
            ),
            Image.LANCZOS,
        )
    components = _extract_overlay_components(scaled_overlay)
    if not components:
        details["reason"] = "overlay_components_missing"
        logger.warning(
            "Shared protrusion overlay skipped: reason=%s path=%s requested_center=(%d,%d)",
            details["reason"],
            overlay_path,
            int(center_x),
            int(center_y),
        )
        return image, details

    base_mode = image.mode
    composited = image.convert("RGBA")
    paste_x = int(round(applied_center_x - (scaled_overlay.width / 2.0)))
    paste_y = int(round(applied_center_y - (scaled_overlay.height / 2.0)))
    composited.alpha_composite(scaled_overlay, dest=(paste_x, paste_y))
    component_details: list[dict[str, Any]] = []
    for index, component in enumerate(components):
        left, top, right, bottom = component["bbox"]
        name = "top" if index == 0 else "bottom"
        component_details.append(
            {
                "name": name,
                "overlay_width": int(right - left),
                "overlay_height": int(bottom - top),
                "paste_x": int(paste_x + left),
                "paste_y": int(paste_y + top),
            }
        )

    details.update(
        {
            "applied": True,
            "reason": "applied",
            "applied_center_x": int(applied_center_x),
            "applied_center_y": int(applied_center_y),
            "overlay_width": int(scaled_overlay.width),
            "overlay_height": int(scaled_overlay.height),
            "paste_x": int(paste_x),
            "paste_y": int(paste_y),
            "scale": round(scale, 6),
            "components": component_details,
        }
    )
    logger.info(
        "Shared protrusion overlay applied: path=%s requested_center=(%d,%d) applied_center=(%d,%d) cover_size=%s overlay_size=%dx%d scale=%.6f components=%s",
        overlay_path,
        int(center_x),
        int(center_y),
        int(applied_center_x),
        int(applied_center_y),
        target_size,
        int(scaled_overlay.width),
        int(scaled_overlay.height),
        float(scale),
        component_details,
    )
    if base_mode == "RGBA":
        return composited, details
    return composited.convert(base_mode), details
