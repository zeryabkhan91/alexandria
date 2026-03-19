"""Shared full-frame replacement helper for standard medallion covers."""

from __future__ import annotations

import math
import os
from collections import deque
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageOps

try:
    from src import config
    from src import frame_geometry
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import frame_geometry  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

ENABLE_REPLACEMENT_FRAME = True
REPLACEMENT_FRAME_SOURCE_PATH = config.CONFIG_DIR / "frame_overlays" / "Untitled__4_frame.png"
DERIVED_DIR = config.CONFIG_DIR / "frame_overlays" / "_derived"
DERIVED_RGBA_PATH = DERIVED_DIR / "Untitled__4_frame_rgba.png"
DERIVED_METRICS_PATH = DERIVED_DIR / "Untitled__4_frame_metrics.json"
REGISTRATION_OVERRIDES_PATH = config.CONFIG_DIR / "medallion_registration_overrides.json"

BACKGROUND_THRESHOLD = 6.0
ALPHA_THRESHOLD = 8
INNER_HOLE_PERCENTILE = 10.0
INNER_HOLE_MARGIN_PX = 1
ERASE_MARGIN_PX = 8
STANDARD_FRAME_CLEAR_PADDING_PX = 95
STANDARD_FRAME_CLEAR_EXTRA_MARGIN_PX = 6
MOAT_BAND_MAX_WIDTH_PX = 12
SOURCE_ANCHOR_DIFF_THRESHOLD = 34.0
SOURCE_ANCHOR_BRIGHTNESS_MIN = 72.0
SOURCE_ANCHOR_ROI_PAD_PX = 120
SOURCE_ANCHOR_NEARBY_SEARCH_PX = 320
SOURCE_ANCHOR_MIN_SCALE = 0.85
SOURCE_ANCHOR_MAX_SCALE = 1.75
ANCHOR_SCALE_WEIGHT_X = 1.0
ANCHOR_SCALE_WEIGHT_Y = 2.0
ANCHOR_BOX_TRIM_X_PERCENT = 1.5
ANCHOR_BOX_TRIM_Y_PERCENT = 0.5
SILHOUETTE_CLEAR_PADDING_PX = 3
AI_EDGE_TRIM_RATIO = 0.08
AI_UNIFORM_MARGIN_MAX_TRIM_RATIO = 0.22
AI_UNIFORM_MARGIN_COLOR_TOL = 26.0
AI_UNIFORM_MARGIN_STD_MAX = 22.0
AI_UNIFORM_MARGIN_MATCH_RATIO = 0.92
HOLE_MASK_FEATHER_PX = 2.5
STANDARD_NAVY_FILL_RGB = (26, 39, 68)


def is_active_for_size(size: tuple[int, int]) -> bool:
    return bool(ENABLE_REPLACEMENT_FRAME and frame_geometry.is_standard_medallion_cover(size))


def _trim_uniform_margins(image: Image.Image) -> Image.Image:
    rgb = image.convert("RGB")
    arr = np.asarray(rgb, dtype=np.float32)
    if arr.ndim != 3 or arr.shape[2] != 3:
        return rgb
    h, w = int(arr.shape[0]), int(arr.shape[1])
    if h < 64 or w < 64:
        return rgb

    patch = max(4, min(h, w) // 40)
    corners = np.concatenate(
        [
            arr[:patch, :patch].reshape(-1, 3),
            arr[:patch, w - patch :].reshape(-1, 3),
            arr[h - patch :, :patch].reshape(-1, 3),
            arr[h - patch :, w - patch :].reshape(-1, 3),
        ],
        axis=0,
    )
    corner_color = np.median(corners, axis=0)

    def _line_matches(line: np.ndarray) -> bool:
        if line.size == 0:
            return False
        diff = np.abs(line - corner_color).mean(axis=1)
        match_ratio = float(np.mean(diff <= AI_UNIFORM_MARGIN_COLOR_TOL))
        std_mean = float(np.std(line, axis=0).mean())
        return match_ratio >= AI_UNIFORM_MARGIN_MATCH_RATIO and std_mean <= AI_UNIFORM_MARGIN_STD_MAX

    max_trim_x = max(0, int(round(w * AI_UNIFORM_MARGIN_MAX_TRIM_RATIO)))
    max_trim_y = max(0, int(round(h * AI_UNIFORM_MARGIN_MAX_TRIM_RATIO)))

    left = 0
    while left < max_trim_x and _line_matches(arr[:, left, :]):
        left += 1
    right = 0
    while right < max_trim_x and _line_matches(arr[:, w - 1 - right, :]):
        right += 1
    top = 0
    while top < max_trim_y and _line_matches(arr[top, :, :]):
        top += 1
    bottom = 0
    while bottom < max_trim_y and _line_matches(arr[h - 1 - bottom, :, :]):
        bottom += 1

    if (left + right + top + bottom) <= 0:
        return rgb
    new_w = w - left - right
    new_h = h - top - bottom
    if new_w < max(64, int(w * 0.55)) or new_h < max(64, int(h * 0.55)):
        return rgb
    return rgb.crop((left, top, w - right, h - bottom))


def _derive_rgba_from_source(source_path: Path) -> Image.Image:
    with Image.open(source_path) as source:
        if "A" in source.getbands():
            rgba = source.convert("RGBA")
            alpha = np.asarray(rgba.getchannel("A"), dtype=np.uint8)
            if int(alpha.max()) > ALPHA_THRESHOLD and int(alpha.min()) < 250:
                return rgba
        rgb = source.convert("RGB")

    arr = np.asarray(rgb, dtype=np.uint8)
    intensity = arr.max(axis=2).astype(np.float32)
    alpha = np.where(intensity > BACKGROUND_THRESHOLD, 255, 0).astype(np.uint8)
    rgba_arr = np.dstack([arr, alpha])
    return Image.fromarray(rgba_arr, mode="RGBA")


def _find_transparent_seed(transparent: np.ndarray) -> tuple[int, int] | None:
    height, width = transparent.shape
    center_x = int(round((width - 1) / 2.0))
    center_y = int(round((height - 1) / 2.0))
    if bool(transparent[center_y, center_x]):
        return (center_y, center_x)
    max_radius = max(height, width)
    for radius in range(1, max_radius):
        y1 = max(0, center_y - radius)
        y2 = min(height - 1, center_y + radius)
        x1 = max(0, center_x - radius)
        x2 = min(width - 1, center_x + radius)
        for y in range(y1, y2 + 1):
            for x in range(x1, x2 + 1):
                if bool(transparent[y, x]):
                    return (y, x)
    return None


def _extract_center_hole_mask(alpha: np.ndarray) -> np.ndarray:
    transparent = alpha <= ALPHA_THRESHOLD
    seed = _find_transparent_seed(transparent)
    if seed is None:
        raise ValueError("Replacement frame has no transparent center hole")
    height, width = transparent.shape
    hole = np.zeros((height, width), dtype=bool)
    queue: deque[tuple[int, int]] = deque([seed])
    while queue:
        y, x = queue.popleft()
        if y < 0 or x < 0 or y >= height or x >= width:
            continue
        if hole[y, x] or not bool(transparent[y, x]):
            continue
        hole[y, x] = True
        queue.append((y - 1, x))
        queue.append((y + 1, x))
        queue.append((y, x - 1))
        queue.append((y, x + 1))
    if not np.any(hole):
        raise ValueError("Replacement frame hole mask extraction failed")
    return hole


def _mask_bbox(mask: np.ndarray) -> tuple[int, int, int, int]:
    ys, xs = np.where(mask)
    if ys.size <= 0 or xs.size <= 0:
        raise ValueError("Mask is empty")
    return (
        int(xs.min()),
        int(ys.min()),
        int(xs.max()) + 1,
        int(ys.max()) + 1,
    )


def _mask_percentile_box(
    mask: np.ndarray,
    *,
    trim_x_percent: float = ANCHOR_BOX_TRIM_X_PERCENT,
    trim_y_percent: float = ANCHOR_BOX_TRIM_Y_PERCENT,
) -> tuple[int, int, int, int]:
    ys, xs = np.where(mask)
    if ys.size <= 0 or xs.size <= 0:
        raise ValueError("Mask is empty")
    x1 = int(math.floor(float(np.percentile(xs.astype(np.float32), float(trim_x_percent)))))
    x2 = int(math.ceil(float(np.percentile(xs.astype(np.float32), float(100.0 - trim_x_percent))))) + 1
    y1 = int(math.floor(float(np.percentile(ys.astype(np.float32), float(trim_y_percent)))))
    y2 = int(math.ceil(float(np.percentile(ys.astype(np.float32), float(100.0 - trim_y_percent))))) + 1
    return (x1, y1, max(x1 + 1, x2), max(y1 + 1, y2))


def _bbox_center(box: tuple[int, int, int, int]) -> tuple[float, float]:
    return ((float(box[0]) + float(box[2])) / 2.0, (float(box[1]) + float(box[3])) / 2.0)


def _find_true_near_seed(mask: np.ndarray, *, seed_y: int, seed_x: int, max_radius: int) -> tuple[int, int] | None:
    height, width = mask.shape
    sy = int(np.clip(seed_y, 0, max(0, height - 1)))
    sx = int(np.clip(seed_x, 0, max(0, width - 1)))
    if bool(mask[sy, sx]):
        return sy, sx
    for radius in range(1, max(1, int(max_radius)) + 1):
        y1 = max(0, sy - radius)
        y2 = min(height - 1, sy + radius)
        x1 = max(0, sx - radius)
        x2 = min(width - 1, sx + radius)
        for y in range(y1, y2 + 1):
            for x in range(x1, x2 + 1):
                if bool(mask[y, x]):
                    return y, x
    return None


def _connected_component_from_seed(mask: np.ndarray, *, seed_y: int, seed_x: int) -> np.ndarray:
    height, width = mask.shape
    component = np.zeros((height, width), dtype=bool)
    seed = _find_true_near_seed(mask, seed_y=seed_y, seed_x=seed_x, max_radius=SOURCE_ANCHOR_NEARBY_SEARCH_PX)
    if seed is None:
        return component
    queue: deque[tuple[int, int]] = deque([seed])
    while queue:
        y, x = queue.popleft()
        if y < 0 or x < 0 or y >= height or x >= width:
            continue
        if component[y, x] or not bool(mask[y, x]):
            continue
        component[y, x] = True
        queue.append((y - 1, x))
        queue.append((y + 1, x))
        queue.append((y, x - 1))
        queue.append((y, x + 1))
    return component


def _analyze_overlay_alpha(alpha: np.ndarray, *, hole_mask: np.ndarray | None = None) -> dict[str, int]:
    height, width = alpha.shape
    center_x = (width - 1) / 2.0
    center_y = (height - 1) / 2.0

    yy, xx = np.ogrid[:height, :width]
    dist = np.sqrt((xx - center_x) ** 2 + (yy - center_y) ** 2)
    opaque = alpha > ALPHA_THRESHOLD
    if not np.any(opaque):
        raise ValueError("Replacement frame alpha is empty")
    overlay_bbox = _mask_bbox(opaque)
    anchor_bbox = _mask_percentile_box(opaque)

    opaque_dists = dist[opaque]
    outer_radius = float(opaque_dists.max())
    erase_radius = int(math.ceil(outer_radius)) + ERASE_MARGIN_PX

    hole = hole_mask if hole_mask is not None else _extract_center_hole_mask(alpha)
    hole_ys, hole_xs = np.where(hole)
    hole_bbox = (
        int(hole_xs.min()),
        int(hole_ys.min()),
        int(hole_xs.max()) + 1,
        int(hole_ys.max()) + 1,
    )
    hole_width = int(hole_bbox[2] - hole_bbox[0])
    hole_height = int(hole_bbox[3] - hole_bbox[1])
    hole_dists = dist[hole]
    hole_radius = int(math.ceil(float(hole_dists.max())))

    max_radius = int(min(center_x, center_y))
    first_hits: list[int] = []
    for angle in np.linspace(0.0, math.pi * 2.0, 720, endpoint=False):
        cos_a = float(math.cos(float(angle)))
        sin_a = float(math.sin(float(angle)))
        for radius in range(max_radius + 1):
            px = int(np.clip(round(center_x + (cos_a * radius)), 0, width - 1))
            py = int(np.clip(round(center_y + (sin_a * radius)), 0, height - 1))
            if int(alpha[py, px]) > ALPHA_THRESHOLD:
                first_hits.append(radius)
                break

    if not first_hits:
        conservative_hole_radius = max(20, max_radius - ERASE_MARGIN_PX)
    else:
        conservative_hole_radius = int(np.floor(np.percentile(np.asarray(first_hits, dtype=np.float32), INNER_HOLE_PERCENTILE)))
        conservative_hole_radius = max(20, conservative_hole_radius - INNER_HOLE_MARGIN_PX)

    return {
        "overlay_width": int(width),
        "overlay_height": int(height),
        "overlay_outer_radius": round(float(outer_radius), 4),
        "overlay_bbox_x1": int(overlay_bbox[0]),
        "overlay_bbox_y1": int(overlay_bbox[1]),
        "overlay_bbox_x2": int(overlay_bbox[2]),
        "overlay_bbox_y2": int(overlay_bbox[3]),
        "anchor_bbox_x1": int(anchor_bbox[0]),
        "anchor_bbox_y1": int(anchor_bbox[1]),
        "anchor_bbox_x2": int(anchor_bbox[2]),
        "anchor_bbox_y2": int(anchor_bbox[3]),
        "hole_radius": int(max(hole_radius, conservative_hole_radius)),
        "hole_bbox_x1": int(hole_bbox[0]),
        "hole_bbox_y1": int(hole_bbox[1]),
        "hole_bbox_x2": int(hole_bbox[2]),
        "hole_bbox_y2": int(hole_bbox[3]),
        "hole_width": int(hole_width),
        "hole_height": int(hole_height),
        "erase_radius": int(max(hole_radius + 4, erase_radius)),
    }


@lru_cache(maxsize=1)
def _load_registration_overrides() -> dict[str, Any]:
    payload = safe_json.load_json(REGISTRATION_OVERRIDES_PATH, {})
    return payload if isinstance(payload, dict) else {}


def _resolve_registration_override(*, book_number: int | None, template_id: str = "") -> tuple[dict[str, Any], str]:
    payload = _load_registration_overrides()
    merged: dict[str, Any] = {"scale_adjust": 0.0, "dx": 0, "dy": 0, "approved": False}
    source = ""

    def _is_effective(entry: dict[str, Any]) -> bool:
        return bool(
            float(entry.get("scale_adjust", 0.0) or 0.0)
            or int(entry.get("dx", 0) or 0)
            or int(entry.get("dy", 0) or 0)
        )

    template_defaults = payload.get("template_defaults", {})
    if isinstance(template_defaults, dict) and template_id:
        template_override = template_defaults.get(str(template_id), {})
        if isinstance(template_override, dict) and template_override:
            merged.update(template_override)
            if _is_effective(template_override):
                source = f"template:{template_id}"

    book_overrides = payload.get("book_overrides", {})
    if isinstance(book_overrides, dict) and book_number is not None:
        book_override = book_overrides.get(str(int(book_number)), {})
        if isinstance(book_override, dict) and book_override:
            merged.update(book_override)
            if _is_effective(book_override):
                source = f"book:{int(book_number)}"

    return merged, source


def _target_anchor_box_for_standard_cover(
    *,
    image: Image.Image,
    center_x: int,
    center_y: int,
    frame_bbox: tuple[int, int, int, int] | None,
    fill_rgb: tuple[int, int, int],
    cover_size: tuple[int, int],
) -> tuple[tuple[int, int, int, int], str]:
    fallback_radius = _legacy_outer_radius_for_standard_cover(
        cover_size=cover_size,
        center_x=center_x,
        center_y=center_y,
        frame_bbox=frame_bbox,
    )
    if frame_bbox is not None and len(frame_bbox) == 4:
        fallback_box = tuple(int(v) for v in frame_bbox)
    else:
        fallback_box = (
            int(center_x - fallback_radius),
            int(center_y - fallback_radius),
            int(center_x + fallback_radius),
            int(center_y + fallback_radius),
        )

    x1, y1, x2, y2 = [int(v) for v in fallback_box]
    pad = int(SOURCE_ANCHOR_ROI_PAD_PX)
    rx1 = max(0, x1 - pad)
    ry1 = max(0, y1 - pad)
    rx2 = min(int(image.width), x2 + pad)
    ry2 = min(int(image.height), y2 + pad)

    roi = np.asarray(image.convert("RGB").crop((rx1, ry1, rx2, ry2)), dtype=np.float32)
    fill = np.asarray(fill_rgb, dtype=np.float32)
    diff = np.abs(roi - fill).mean(axis=2)
    bright = roi.max(axis=2)
    mask = (diff >= float(SOURCE_ANCHOR_DIFF_THRESHOLD)) & (bright >= float(SOURCE_ANCHOR_BRIGHTNESS_MIN))
    local_x1 = int(x1 - rx1)
    local_y1 = int(y1 - ry1)
    local_x2 = int(x2 - rx1)
    local_y2 = int(y2 - ry1)
    seed_margin = 12
    seeds = [
        (max(0, local_y1 + seed_margin), int(center_x - rx1)),
        (min(mask.shape[0] - 1, local_y2 - seed_margin), int(center_x - rx1)),
        (int(center_y - ry1), max(0, local_x1 + seed_margin)),
        (int(center_y - ry1), min(mask.shape[1] - 1, local_x2 - seed_margin)),
    ]
    component = np.zeros_like(mask, dtype=bool)
    for seed_y, seed_x in seeds:
        local_component = _connected_component_from_seed(mask, seed_y=seed_y, seed_x=seed_x)
        if np.any(local_component):
            component |= local_component

    if not np.any(component):
        return fallback_box, "frame_bbox_fallback"

    detected = _mask_percentile_box(component)
    source_box = (
        int(rx1 + detected[0]),
        int(ry1 + detected[1]),
        int(rx1 + detected[2]),
        int(ry1 + detected[3]),
    )
    fallback_w = max(1, int(fallback_box[2] - fallback_box[0]))
    fallback_h = max(1, int(fallback_box[3] - fallback_box[1]))
    detected_w = max(1, int(source_box[2] - source_box[0]))
    detected_h = max(1, int(source_box[3] - source_box[1]))
    scale_w = float(detected_w) / float(fallback_w)
    scale_h = float(detected_h) / float(fallback_h)
    if not (
        SOURCE_ANCHOR_MIN_SCALE <= scale_w <= SOURCE_ANCHOR_MAX_SCALE
        and SOURCE_ANCHOR_MIN_SCALE <= scale_h <= SOURCE_ANCHOR_MAX_SCALE
    ):
        return fallback_box, "frame_bbox_fallback"
    return source_box, "source_silhouette"


def _load_cached_assets() -> dict[str, Any] | None:
    if not DERIVED_RGBA_PATH.exists() or not DERIVED_METRICS_PATH.exists():
        return None
    try:
        with Image.open(DERIVED_RGBA_PATH) as stored:
            rgba = stored.convert("RGBA")
        payload = safe_json.load_json(DERIVED_METRICS_PATH, {})
        if not isinstance(payload, dict):
            return None
        alpha = np.asarray(rgba.getchannel("A"), dtype=np.uint8)
        metrics = _analyze_overlay_alpha(alpha)
        return {
            "source_path": str(REPLACEMENT_FRAME_SOURCE_PATH),
            "rgba_path": str(DERIVED_RGBA_PATH),
            **{str(k): v for k, v in payload.items() if str(k) not in {"source_path", "rgba_path"}},
            **metrics,
        }
    except Exception:
        logger.warning("Replacement frame cache invalid; regenerating derived assets at %s", DERIVED_RGBA_PATH)
        return None


@lru_cache(maxsize=1)
def ensure_replacement_frame_assets() -> dict[str, Any]:
    if not REPLACEMENT_FRAME_SOURCE_PATH.exists():
        raise FileNotFoundError(f"Replacement frame source missing: {REPLACEMENT_FRAME_SOURCE_PATH}")

    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    cached = _load_cached_assets()
    if cached is not None:
        return cached
    rgba = _derive_rgba_from_source(REPLACEMENT_FRAME_SOURCE_PATH)
    alpha = np.asarray(rgba.getchannel("A"), dtype=np.uint8)
    metrics = _analyze_overlay_alpha(alpha)
    temp_rgba_path = DERIVED_DIR / f"{DERIVED_RGBA_PATH.stem}.{os.getpid()}.tmp.png"
    rgba.save(temp_rgba_path, format="PNG")
    os.replace(temp_rgba_path, DERIVED_RGBA_PATH)
    payload = {
        "source_path": str(REPLACEMENT_FRAME_SOURCE_PATH),
        "rgba_path": str(DERIVED_RGBA_PATH),
        **metrics,
    }
    safe_json.atomic_write_json(DERIVED_METRICS_PATH, payload)
    logger.info(
        "Replacement frame assets ensured: source=%s rgba=%s overlay_size=%dx%d outer_radius=%.4f hole_radius=%d erase_radius=%d",
        REPLACEMENT_FRAME_SOURCE_PATH,
        DERIVED_RGBA_PATH,
        int(metrics["overlay_width"]),
        int(metrics["overlay_height"]),
        float(metrics["overlay_outer_radius"]),
        int(metrics["hole_radius"]),
        int(metrics["erase_radius"]),
    )
    return payload


def _legacy_outer_radius_for_standard_cover(
    *,
    cover_size: tuple[int, int],
    center_x: int,
    center_y: int,
    frame_bbox: tuple[int, int, int, int] | None,
) -> int:
    geometry = frame_geometry.resolve_standard_medallion_geometry(cover_size)
    scale = float(getattr(geometry, "radius_scale", 1.0) or 1.0)
    if frame_bbox is not None and len(frame_bbox) == 4:
        x1, y1, x2, y2 = [int(v) for v in frame_bbox]
        return int(
            math.ceil(
                max(
                    abs(int(x1) - int(center_x)),
                    abs(int(x2) - int(center_x)),
                    abs(int(y1) - int(center_y)),
                    abs(int(y2) - int(center_y)),
                )
            )
        )
    return int(round(float(geometry.art_clip_radius) + (STANDARD_FRAME_CLEAR_PADDING_PX * scale)))


def _scale_overlay_image(scale: float) -> tuple[Image.Image, dict[str, Any]]:
    with Image.open(DERIVED_RGBA_PATH) as stored:
        overlay = stored.convert("RGBA")
    applied_scale = float(scale)
    if abs(applied_scale - 1.0) > 1e-6:
        overlay = overlay.resize(
            (
                max(1, int(round(overlay.width * applied_scale))),
                max(1, int(round(overlay.height * applied_scale))),
            ),
            Image.LANCZOS,
        )
    alpha = np.asarray(overlay.getchannel("A"), dtype=np.uint8)
    hole_mask = _extract_center_hole_mask(alpha)
    overlay_metrics = _analyze_overlay_alpha(alpha, hole_mask=hole_mask)
    return overlay, {
        "scale": float(applied_scale),
        **overlay_metrics,
        "hole_mask": hole_mask,
    }


def _solve_anchor_scale(
    *,
    source_anchor_box: tuple[int, int, int, int],
    overlay_anchor_box: tuple[int, int, int, int],
) -> float:
    target_w = max(1.0, float(source_anchor_box[2] - source_anchor_box[0]))
    target_h = max(1.0, float(source_anchor_box[3] - source_anchor_box[1]))
    overlay_w = max(1.0, float(overlay_anchor_box[2] - overlay_anchor_box[0]))
    overlay_h = max(1.0, float(overlay_anchor_box[3] - overlay_anchor_box[1]))
    numerator = (ANCHOR_SCALE_WEIGHT_X * overlay_w * target_w) + (ANCHOR_SCALE_WEIGHT_Y * overlay_h * target_h)
    denominator = (ANCHOR_SCALE_WEIGHT_X * overlay_w * overlay_w) + (ANCHOR_SCALE_WEIGHT_Y * overlay_h * overlay_h)
    return max(0.01, float(numerator / max(1e-6, denominator)))


def _compute_registered_overlay(
    *,
    source_anchor_box: tuple[int, int, int, int],
    base_center_x: int,
    base_center_y: int,
    scale_adjust: float = 0.0,
    dx: int = 0,
    dy: int = 0,
) -> tuple[Image.Image, dict[str, Any]]:
    payload = ensure_replacement_frame_assets()
    overlay_anchor_box_unscaled = (
        int(payload.get("anchor_bbox_x1", payload.get("overlay_bbox_x1", 0))),
        int(payload.get("anchor_bbox_y1", payload.get("overlay_bbox_y1", 0))),
        int(payload.get("anchor_bbox_x2", payload.get("overlay_bbox_x2", payload.get("overlay_width", 0)))),
        int(payload.get("anchor_bbox_y2", payload.get("overlay_bbox_y2", payload.get("overlay_height", 0)))),
    )
    auto_scale = _solve_anchor_scale(
        source_anchor_box=source_anchor_box,
        overlay_anchor_box=overlay_anchor_box_unscaled,
    )
    final_scale = float(auto_scale) * (1.0 + float(scale_adjust))
    overlay, overlay_meta = _scale_overlay_image(final_scale)
    overlay_anchor_box_scaled = (
        int(overlay_meta.get("anchor_bbox_x1", overlay_meta["overlay_bbox_x1"])),
        int(overlay_meta.get("anchor_bbox_y1", overlay_meta["overlay_bbox_y1"])),
        int(overlay_meta.get("anchor_bbox_x2", overlay_meta["overlay_bbox_x2"])),
        int(overlay_meta.get("anchor_bbox_y2", overlay_meta["overlay_bbox_y2"])),
    )
    target_cx, target_cy = _bbox_center(source_anchor_box)
    overlay_cx, overlay_cy = _bbox_center(overlay_anchor_box_scaled)
    paste_x = int(round(target_cx - overlay_cx)) + int(dx)
    paste_y = int(round(target_cy - overlay_cy)) + int(dy)
    centered_paste_x = int(round(base_center_x - (overlay.width / 2.0)))
    centered_paste_y = int(round(base_center_y - (overlay.height / 2.0)))
    final_anchor_box = (
        int(paste_x + overlay_anchor_box_scaled[0]),
        int(paste_y + overlay_anchor_box_scaled[1]),
        int(paste_x + overlay_anchor_box_scaled[2]),
        int(paste_y + overlay_anchor_box_scaled[3]),
    )
    clear_padding = int(SILHOUETTE_CLEAR_PADDING_PX)
    return overlay, {
        **overlay_meta,
        "auto_scale": round(float(auto_scale), 6),
        "auto_dx": int(round(target_cx - overlay_cx)) - int(centered_paste_x),
        "auto_dy": int(round(target_cy - overlay_cy)) - int(centered_paste_y),
        "final_scale": round(float(final_scale), 6),
        "final_dx": int(paste_x - centered_paste_x),
        "final_dy": int(paste_y - centered_paste_y),
        "paste_x": int(paste_x),
        "paste_y": int(paste_y),
        "source_anchor_box": [int(v) for v in source_anchor_box],
        "overlay_anchor_box_unscaled": [int(v) for v in overlay_anchor_box_unscaled],
        "overlay_anchor_box_scaled": [int(v) for v in final_anchor_box],
        "anchor_error_left_px": round(abs(float(final_anchor_box[0] - source_anchor_box[0])), 4),
        "anchor_error_top_px": round(abs(float(final_anchor_box[1] - source_anchor_box[1])), 4),
        "anchor_error_right_px": round(abs(float(final_anchor_box[2] - source_anchor_box[2])), 4),
        "anchor_error_bottom_px": round(abs(float(final_anchor_box[3] - source_anchor_box[3])), 4),
        "anchor_error_max_px": round(
            max(
                abs(float(final_anchor_box[0] - source_anchor_box[0])),
                abs(float(final_anchor_box[1] - source_anchor_box[1])),
                abs(float(final_anchor_box[2] - source_anchor_box[2])),
                abs(float(final_anchor_box[3] - source_anchor_box[3])),
            ),
            4,
        ),
        "legacy_outer_radius": int(
            math.ceil(
                max(
                    abs(float(source_anchor_box[0] - base_center_x)),
                    abs(float(source_anchor_box[2] - base_center_x)),
                    abs(float(source_anchor_box[1] - base_center_y)),
                    abs(float(source_anchor_box[3] - base_center_y)),
                )
            )
        ),
        "overlay_outer_radius_unscaled": float(payload.get("overlay_outer_radius", 0.0) or 0.0),
        "overlay_outer_radius_scaled": float(overlay_meta["overlay_outer_radius"]),
        "outer_fit_scale": round(float(final_scale), 6),
        "outer_radius_error_px": round(
            abs(
                float(overlay_meta["overlay_outer_radius"])
                - float(
                    math.ceil(
                        max(
                            abs(float(source_anchor_box[0] - base_center_x)),
                            abs(float(source_anchor_box[2] - base_center_x)),
                            abs(float(source_anchor_box[1] - base_center_y)),
                            abs(float(source_anchor_box[3] - base_center_y)),
                        )
                    )
                )
            ),
            4,
        ),
        "moat_band_width_px": float(clear_padding),
        "navy_band_max_px": float(clear_padding),
    }


def _standard_fill_rgb() -> tuple[int, int, int]:
    return tuple(int(v) for v in STANDARD_NAVY_FILL_RGB)


def _clear_radius_for_standard_cover(
    *,
    legacy_outer_radius: int,
) -> int:
    return max(20, int(legacy_outer_radius) + STANDARD_FRAME_CLEAR_EXTRA_MARGIN_PX)


def _load_prepared_art(*, ai_art_path: Path, size: tuple[int, int], fill_rgb: tuple[int, int, int]) -> Image.Image:
    target_w = max(2, int(size[0]))
    target_h = max(2, int(size[1]))
    with Image.open(ai_art_path) as source:
        prepared = _trim_uniform_margins(source)
        if AI_EDGE_TRIM_RATIO > 0:
            src_w, src_h = prepared.size
            trim_x = int(round(src_w * AI_EDGE_TRIM_RATIO / 2.0))
            trim_y = int(round(src_h * AI_EDGE_TRIM_RATIO / 2.0))
            if (src_w - 2 * trim_x) >= 64 and (src_h - 2 * trim_y) >= 64:
                prepared = prepared.crop((trim_x, trim_y, src_w - trim_x, src_h - trim_y))
        fitted = ImageOps.fit(
            prepared.convert("RGBA"),
            (target_w, target_h),
            method=Image.LANCZOS,
            centering=(0.5, 0.5),
        )
    art_bg = Image.new("RGBA", (target_w, target_h), (*fill_rgb, 255))
    art_bg.alpha_composite(fitted)
    return art_bg


def _circle_mask(*, size: tuple[int, int], center_x: int, center_y: int, radius: int) -> Image.Image:
    mask = Image.new("L", size, 0)
    draw = ImageDraw.Draw(mask)
    r = max(2, int(radius))
    draw.ellipse((center_x - r, center_y - r, center_x + r, center_y + r), fill=255)
    return mask


def compute_outside_change_metrics(
    *,
    original_rgb: Image.Image,
    composited_rgb: Image.Image,
    center_x: int,
    center_y: int,
    guard_radius: int,
) -> dict[str, float]:
    original_arr = np.asarray(original_rgb.convert("RGB"), dtype=np.int16)
    composited_arr = np.asarray(composited_rgb.convert("RGB"), dtype=np.int16)
    height, width = original_arr.shape[:2]
    yy, xx = np.ogrid[:height, :width]
    dist = np.sqrt((xx - float(center_x)) ** 2 + (yy - float(center_y)) ** 2)
    outside = dist >= float(max(20, int(guard_radius)))
    if not np.any(outside):
        return {"outside_changed_pct": 0.0, "outside_mean_delta": 0.0}
    diff = np.abs(composited_arr - original_arr).max(axis=2)
    ring = diff[outside]
    if ring.size <= 0:
        return {"outside_changed_pct": 0.0, "outside_mean_delta": 0.0}
    changed_pct = 100.0 * float(np.sum(ring > 0.0)) / float(ring.size)
    return {
        "outside_changed_pct": round(changed_pct, 4),
        "outside_mean_delta": round(float(ring.mean()), 4),
    }


def _dilated_mask(mask: Image.Image, *, padding_px: int) -> Image.Image:
    grow = max(0, int(padding_px))
    if grow <= 0:
        return mask
    size = max(3, (grow * 2) + 1)
    if size % 2 == 0:
        size += 1
    return mask.filter(ImageFilter.MaxFilter(size=size))


def apply_replacement_frame_composite(
    *,
    image: Image.Image,
    ai_art_path: Path,
    center_x: int,
    center_y: int,
    cover_size: tuple[int, int] | None = None,
    frame_bbox: tuple[int, int, int, int] | None = None,
    geometry_source: str = "",
    book_number: int | None = None,
    template_id: str = "",
) -> tuple[Image.Image, dict[str, Any]]:
    target_size = tuple(int(v) for v in (cover_size or image.size))
    details: dict[str, Any] = {
        "applied": False,
        "reason": "",
        "replacement_frame_mode": "",
        "frame_asset": str(REPLACEMENT_FRAME_SOURCE_PATH),
        "source_path": str(REPLACEMENT_FRAME_SOURCE_PATH),
        "derived_rgba_path": str(DERIVED_RGBA_PATH),
        "geometry_source": str(geometry_source or ""),
        "requested_center_x": int(center_x),
        "requested_center_y": int(center_y),
        "applied_center_x": int(center_x),
        "applied_center_y": int(center_y),
        "overlay_width": 0,
        "overlay_height": 0,
        "paste_x": 0,
        "paste_y": 0,
        "hole_radius": 0,
        "erase_radius": 0,
        "clear_radius": 0,
        "clear_bbox": [],
        "source_anchor_box": [],
        "source_anchor_source": "",
        "overlay_anchor_box_unscaled": [],
        "overlay_anchor_box_scaled": [],
        "scale": 0.0,
        "auto_scale": 0.0,
        "auto_dx": 0,
        "auto_dy": 0,
        "final_scale": 0.0,
        "final_dx": 0,
        "final_dy": 0,
        "legacy_outer_radius": 0,
        "overlay_outer_radius_unscaled": 0.0,
        "overlay_outer_radius_scaled": 0.0,
        "outer_fit_scale": 0.0,
        "outer_radius_error_px": 0.0,
        "moat_band_width_px": 0.0,
        "anchor_error_left_px": 0.0,
        "anchor_error_top_px": 0.0,
        "anchor_error_right_px": 0.0,
        "anchor_error_bottom_px": 0.0,
        "anchor_error_max_px": 0.0,
        "navy_band_max_px": 0.0,
        "fill_policy": "",
        "fill_rgb": (),
        "override_applied": False,
        "override_source": "",
        "hole_bbox": [],
        "placement_center": [int(center_x), int(center_y)],
    }

    if not is_active_for_size(target_size):
        details["reason"] = "disabled_or_non_standard"
        return image, details

    applied_center_x = int(center_x)
    applied_center_y = int(center_y)
    fill_rgb = _standard_fill_rgb()
    source_anchor_box, source_anchor_source = _target_anchor_box_for_standard_cover(
        image=image,
        center_x=applied_center_x,
        center_y=applied_center_y,
        frame_bbox=frame_bbox,
        fill_rgb=fill_rgb,
        cover_size=target_size,
    )
    override_payload, override_source = _resolve_registration_override(book_number=book_number, template_id=template_id)
    scale_adjust = float(override_payload.get("scale_adjust", 0.0) or 0.0)
    override_dx = int(override_payload.get("dx", 0) or 0)
    override_dy = int(override_payload.get("dy", 0) or 0)
    overlay, overlay_meta = _compute_registered_overlay(
        source_anchor_box=source_anchor_box,
        base_center_x=applied_center_x,
        base_center_y=applied_center_y,
        scale_adjust=scale_adjust,
        dx=override_dx,
        dy=override_dy,
    )
    hole_radius = int(overlay_meta["hole_radius"])
    erase_radius = int(max(overlay_meta["erase_radius"], hole_radius + 8))
    hole_mask = np.asarray(overlay_meta["hole_mask"], dtype=bool)
    hole_bbox = (
        int(overlay_meta["hole_bbox_x1"]),
        int(overlay_meta["hole_bbox_y1"]),
        int(overlay_meta["hole_bbox_x2"]),
        int(overlay_meta["hole_bbox_y2"]),
    )
    paste_x = int(overlay_meta["paste_x"])
    paste_y = int(overlay_meta["paste_y"])

    result = image.convert("RGBA").copy()
    overlay_alpha = overlay.getchannel("A")
    clear_mask = Image.new("L", target_size, 0)
    clear_mask.paste(overlay_alpha, (paste_x, paste_y))
    clear_mask = _dilated_mask(clear_mask, padding_px=SILHOUETTE_CLEAR_PADDING_PX)
    clear_arr = np.asarray(clear_mask, dtype=np.uint8) > 0
    clear_bbox = list(_mask_bbox(clear_arr)) if np.any(clear_arr) else [0, 0, 0, 0]
    clear_radius = max(
        20,
        int(
            math.ceil(
                max(
                    abs(float(clear_bbox[0] - applied_center_x)),
                    abs(float(clear_bbox[2] - applied_center_x)),
                    abs(float(clear_bbox[1] - applied_center_y)),
                    abs(float(clear_bbox[3] - applied_center_y)),
                )
            )
        ),
    )
    fill_layer = Image.new("RGBA", target_size, (*fill_rgb, 255))
    result = Image.composite(fill_layer, result, clear_mask)

    art = _load_prepared_art(
        ai_art_path=Path(ai_art_path),
        size=(int(hole_bbox[2] - hole_bbox[0]), int(hole_bbox[3] - hole_bbox[1])),
        fill_rgb=fill_rgb,
    )
    local_art = Image.new("RGBA", overlay.size, (*fill_rgb, 255))
    local_art.alpha_composite(art, dest=(int(hole_bbox[0]), int(hole_bbox[1])))
    hole_mask_img = Image.fromarray((hole_mask.astype(np.uint8) * 255), mode="L").filter(
        ImageFilter.GaussianBlur(radius=HOLE_MASK_FEATHER_PX)
    )
    local_art.putalpha(hole_mask_img)

    local_canvas = Image.new("RGBA", overlay.size, (0, 0, 0, 0))
    local_canvas.alpha_composite(local_art)
    local_canvas.alpha_composite(overlay)
    result.alpha_composite(local_canvas, dest=(paste_x, paste_y))

    details.update(
        {
            "applied": True,
            "reason": "applied",
            "replacement_frame_mode": "single_frame_standard_medallion",
            "applied_center_x": int(applied_center_x),
            "applied_center_y": int(applied_center_y),
            "overlay_width": int(overlay.width),
            "overlay_height": int(overlay.height),
            "paste_x": int(paste_x),
            "paste_y": int(paste_y),
            "source_anchor_box": [int(v) for v in source_anchor_box],
            "source_anchor_source": str(source_anchor_source),
            "overlay_anchor_box_unscaled": [int(v) for v in overlay_meta.get("overlay_anchor_box_unscaled", [])],
            "overlay_anchor_box_scaled": [int(v) for v in overlay_meta.get("overlay_anchor_box_scaled", [])],
            "hole_radius": int(hole_radius),
            "hole_bbox": [int(v) for v in hole_bbox],
            "erase_radius": int(erase_radius),
            "clear_radius": int(clear_radius),
            "clear_bbox": [int(v) for v in clear_bbox],
            "scale": float(overlay_meta["final_scale"]),
            "auto_scale": float(overlay_meta["auto_scale"]),
            "auto_dx": int(overlay_meta["auto_dx"]),
            "auto_dy": int(overlay_meta["auto_dy"]),
            "final_scale": float(overlay_meta["final_scale"]),
            "final_dx": int(overlay_meta["final_dx"]),
            "final_dy": int(overlay_meta["final_dy"]),
            "legacy_outer_radius": int(overlay_meta["legacy_outer_radius"]),
            "overlay_outer_radius_unscaled": float(overlay_meta["overlay_outer_radius_unscaled"]),
            "overlay_outer_radius_scaled": float(overlay_meta["overlay_outer_radius_scaled"]),
            "outer_fit_scale": float(overlay_meta["outer_fit_scale"]),
            "outer_radius_error_px": float(overlay_meta["outer_radius_error_px"]),
            "moat_band_width_px": float(overlay_meta["moat_band_width_px"]),
            "anchor_error_left_px": float(overlay_meta["anchor_error_left_px"]),
            "anchor_error_top_px": float(overlay_meta["anchor_error_top_px"]),
            "anchor_error_right_px": float(overlay_meta["anchor_error_right_px"]),
            "anchor_error_bottom_px": float(overlay_meta["anchor_error_bottom_px"]),
            "anchor_error_max_px": float(overlay_meta["anchor_error_max_px"]),
            "navy_band_max_px": float(overlay_meta["navy_band_max_px"]),
            "fill_policy": "fixed_standard_navy",
            "fill_rgb": tuple(int(v) for v in fill_rgb),
            "override_applied": bool(override_source),
            "override_source": str(override_source),
            "placement_center": [int(applied_center_x), int(applied_center_y)],
            "geometry_source": str(geometry_source or "template_geometry"),
        }
    )
    logger.info(
        "Replacement frame composite applied: source=%s derived=%s center=(%d,%d) overlay_size=%dx%d paste=(%d,%d) hole_radius=%d hole_bbox=%s erase_radius=%d clear_radius=%d clear_bbox=%s source_anchor_box=%s source_anchor_source=%s overlay_anchor_box_scaled=%s final_scale=%.6f final_dx=%d final_dy=%d anchor_error_max_px=%.4f navy_band_max_px=%.4f fill_policy=%s fill_rgb=%s geometry_source=%s override_source=%s",
        REPLACEMENT_FRAME_SOURCE_PATH,
        DERIVED_RGBA_PATH,
        int(applied_center_x),
        int(applied_center_y),
        int(overlay.width),
        int(overlay.height),
        int(paste_x),
        int(paste_y),
        int(hole_radius),
        tuple(int(v) for v in hole_bbox),
        int(erase_radius),
        int(clear_radius),
        tuple(int(v) for v in clear_bbox),
        tuple(int(v) for v in source_anchor_box),
        str(source_anchor_source),
        tuple(int(v) for v in overlay_meta.get("overlay_anchor_box_scaled", [])),
        float(overlay_meta["final_scale"]),
        int(overlay_meta["final_dx"]),
        int(overlay_meta["final_dy"]),
        float(overlay_meta["anchor_error_max_px"]),
        float(overlay_meta["navy_band_max_px"]),
        "fixed_standard_navy",
        tuple(int(v) for v in fill_rgb),
        str(geometry_source or "template_geometry"),
        str(override_source),
    )
    return result.convert("RGB"), details
