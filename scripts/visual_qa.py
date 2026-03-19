#!/usr/bin/env python3
"""Structural visual QA for compositor output."""

from __future__ import annotations

import argparse
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src import config
    from src import frame_geometry
    from src import replacement_frame
    from src import safe_json
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import frame_geometry  # type: ignore
    import replacement_frame  # type: ignore
    import safe_json  # type: ignore


CENTER_X = 2864
CENTER_Y = 1620
RADIUS = 500

FRAME_CHANGED_THRESHOLD_PCT = 1.0
FRAME_MEAN_DELTA_THRESHOLD = 3.0
FRAME_PIXEL_DELTA_THRESHOLD = 3.0

OUTER_CHANGED_THRESHOLD_PCT = 0.5
OUTER_PIXEL_DELTA_THRESHOLD = 5.0

INNER_CHANGED_MIN_PCT = 20.0
INNER_PIXEL_DELTA_THRESHOLD = 10.0

GOLDEN_MEAN_DELTA_THRESHOLD = 15.0
STANDARD_NAVY_RGB = np.array([26.0, 39.0, 68.0], dtype=np.float32)
NAVY_BAND_DELTA_THRESHOLD = 38.0
NAVY_BAND_MATCH_THRESHOLD = 0.72
GOLD_BAND_RATIO_THRESHOLD = 0.18
ANCHOR_ERROR_EDGE_THRESHOLD_PX = 4.0
ANCHOR_ERROR_MAX_THRESHOLD_PX = 4.0
NAVY_BAND_MAX_THRESHOLD_PX = 4.0


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_project_relative(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(resolved)


def _load_json(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def _resolve_compositor_mode(composite_file: Path, requested_mode: str = "auto") -> str:
    mode = str(requested_mode or "auto").strip().lower()
    if mode and mode != "auto":
        return mode

    direct_validation = composite_file.with_suffix(composite_file.suffix + ".validation.json")
    if direct_validation.exists():
        payload = _load_json(direct_validation)
        if bool(dict(payload.get("replacement_frame", {})).get("applied", False)):
            return "replacement_frame"
        direct_mode = str(payload.get("compositor_mode", "")).strip().lower()
        if direct_mode:
            return direct_mode

    parent = composite_file.parent
    report_path = parent / "composite_validation.json"
    if not report_path.exists():
        report_path = parent.parent / "composite_validation.json"
    if report_path.exists():
        report = _load_json(report_path)
        for row in report.get("items", []) if isinstance(report.get("items"), list) else []:
            if not isinstance(row, dict):
                continue
            output_path = Path(str(row.get("output_path", "")).strip())
            try:
                matches = output_path.resolve() == composite_file.resolve()
            except Exception:
                matches = str(output_path) == str(composite_file)
            if not matches:
                continue
            if str(row.get("overlay_source", "")).strip().lower() == "replacement_frame_overlay":
                return "replacement_frame"
            row_mode = str(row.get("mode", "")).strip().lower()
            if row_mode:
                return row_mode

    return "legacy"


def _resolve_replacement_payload_from_artifacts(composite_file: Path) -> dict[str, Any]:
    direct_validation = composite_file.with_suffix(composite_file.suffix + ".validation.json")
    if direct_validation.exists():
        payload = _load_json(direct_validation)
        repl = payload.get("replacement_frame", {})
        return repl if isinstance(repl, dict) else {}

    parent = composite_file.parent
    report_path = parent / "composite_validation.json"
    if not report_path.exists():
        report_path = parent.parent / "composite_validation.json"
    if report_path.exists():
        report = _load_json(report_path)
        for row in report.get("items", []) if isinstance(report.get("items"), list) else []:
            if not isinstance(row, dict):
                continue
            output_path = Path(str(row.get("output_path", "")).strip())
            try:
                matches = output_path.resolve() == composite_file.resolve()
            except Exception:
                matches = str(output_path) == str(composite_file)
            if not matches:
                continue
            repl = row.get("replacement_frame", {})
            return repl if isinstance(repl, dict) else {}
    return {}


def _legacy_outer_radius_for_book(*, book_number: int, center_x: int, center_y: int, width: int, height: int) -> int:
    regions_path = config.cover_regions_path()
    payload = safe_json.load_json(regions_path, {})
    if isinstance(payload, dict):
        for row in payload.get("covers", []) if isinstance(payload.get("covers"), list) else []:
            if not isinstance(row, dict):
                continue
            if int(row.get("cover_id", 0) or 0) != int(book_number):
                continue
            bbox = row.get("frame_bbox")
            if isinstance(bbox, (list, tuple)) and len(bbox) == 4:
                x1, y1, x2, y2 = [int(v) for v in bbox]
                return int(
                    math.ceil(
                        max(
                            abs(x1 - int(center_x)),
                            abs(x2 - int(center_x)),
                            abs(y1 - int(center_y)),
                            abs(y2 - int(center_y)),
                        )
                    )
                )
    geometry = frame_geometry.resolve_standard_medallion_geometry((width, height))
    scale = float(getattr(geometry, "radius_scale", 1.0) or 1.0)
    return int(round(float(geometry.art_clip_radius) + (replacement_frame.STANDARD_FRAME_CLEAR_PADDING_PX * scale)))


def _find_original_image(folder: Path) -> Path | None:
    if not folder.exists() or not folder.is_dir():
        return None
    candidates = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg"}])
    return candidates[0] if candidates else None


def _find_latest_composite_for_book(composited_dir: Path, book_number: int) -> Path | None:
    book_dir = composited_dir / str(int(book_number))
    if not book_dir.exists() or not book_dir.is_dir():
        return None
    variants = [p for p in book_dir.rglob("variant_*.jpg") if p.is_file()]
    if not variants:
        return None
    variants.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return variants[0]


def _build_catalog_rows(catalog: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in catalog:
        if not isinstance(item, dict):
            continue
        number = _safe_int(item.get("number"), 0)
        if number <= 0:
            continue
        rows.append(
            {
                "number": number,
                "title": str(item.get("title", f"Book {number}")),
                "folder_name": str(item.get("folder_name", "")).strip(),
            }
        )
    rows.sort(key=lambda row: int(row["number"]))
    return rows


def _distance_grid(height: int, width: int, center_x: int, center_y: int) -> np.ndarray:
    cx = int(np.clip(int(center_x), 0, max(0, width - 1)))
    cy = int(np.clip(int(center_y), 0, max(0, height - 1)))
    yy, xx = np.ogrid[:height, :width]
    return np.sqrt((xx - cx) ** 2 + (yy - cy) ** 2)


def _replacement_outer_band_metrics(
    *,
    composite_arr: np.ndarray,
    composite_file: Path,
    book_number: int,
    width: int,
    height: int,
    center_x: int,
    center_y: int,
    replacement_metrics: dict[str, Any] | None = None,
) -> dict[str, float] | None:
    if not frame_geometry.is_standard_medallion_cover((width, height)):
        return None

    resolved_metrics = dict(replacement_metrics or {})
    if not resolved_metrics:
        resolved_metrics = _resolve_replacement_payload_from_artifacts(composite_file)

    assets = replacement_frame.ensure_replacement_frame_assets()
    fallback_radius = float(
        resolved_metrics.get(
            "legacy_outer_radius",
            _legacy_outer_radius_for_book(
                book_number=book_number,
                center_x=center_x,
                center_y=center_y,
                width=width,
                height=height,
            ),
        )
        or 0.0
    )
    source_anchor_box = resolved_metrics.get("source_anchor_box", [])
    if not isinstance(source_anchor_box, (list, tuple)) or len(source_anchor_box) != 4:
        source_anchor_box = [
            int(center_x - fallback_radius),
            int(center_y - fallback_radius),
            int(center_x + fallback_radius),
            int(center_y + fallback_radius),
        ]
    overlay_anchor_box_unscaled = resolved_metrics.get("overlay_anchor_box_unscaled", [])
    if not isinstance(overlay_anchor_box_unscaled, (list, tuple)) or len(overlay_anchor_box_unscaled) != 4:
        overlay_anchor_box_unscaled = [
            int(assets.get("overlay_bbox_x1", 0)),
            int(assets.get("overlay_bbox_y1", 0)),
            int(assets.get("overlay_bbox_x2", assets.get("overlay_width", 0))),
            int(assets.get("overlay_bbox_y2", assets.get("overlay_height", 0))),
        ]
    overlay_anchor_box_scaled = resolved_metrics.get("overlay_anchor_box_scaled", [])
    if not isinstance(overlay_anchor_box_scaled, (list, tuple)) or len(overlay_anchor_box_scaled) != 4:
        final_scale = float(resolved_metrics.get("final_scale", resolved_metrics.get("outer_fit_scale", 1.0)) or 1.0)
        overlay_anchor_box_scaled = [
            int(round(float(overlay_anchor_box_unscaled[0]) * final_scale)),
            int(round(float(overlay_anchor_box_unscaled[1]) * final_scale)),
            int(round(float(overlay_anchor_box_unscaled[2]) * final_scale)),
            int(round(float(overlay_anchor_box_unscaled[3]) * final_scale)),
        ]
    navy_band_max_px = float(
        resolved_metrics.get(
            "navy_band_max_px",
            resolved_metrics.get("moat_band_width_px", replacement_frame.SILHOUETTE_CLEAR_PADDING_PX),
        )
        or 0.0
    )
    clear_bbox = resolved_metrics.get("clear_bbox", [])
    if not isinstance(clear_bbox, (list, tuple)) or len(clear_bbox) != 4:
        clear_bbox = [
            int(source_anchor_box[0] - navy_band_max_px),
            int(source_anchor_box[1] - navy_band_max_px),
            int(source_anchor_box[2] + navy_band_max_px),
            int(source_anchor_box[3] + navy_band_max_px),
        ]
    anchor_error_left_px = float(
        resolved_metrics.get("anchor_error_left_px", abs(float(overlay_anchor_box_scaled[0]) - float(source_anchor_box[0]))) or 0.0
    )
    anchor_error_top_px = float(
        resolved_metrics.get("anchor_error_top_px", abs(float(overlay_anchor_box_scaled[1]) - float(source_anchor_box[1]))) or 0.0
    )
    anchor_error_right_px = float(
        resolved_metrics.get("anchor_error_right_px", abs(float(overlay_anchor_box_scaled[2]) - float(source_anchor_box[2]))) or 0.0
    )
    anchor_error_bottom_px = float(
        resolved_metrics.get("anchor_error_bottom_px", abs(float(overlay_anchor_box_scaled[3]) - float(source_anchor_box[3]))) or 0.0
    )
    anchor_error_max_px = float(
        resolved_metrics.get(
            "anchor_error_max_px",
            max(anchor_error_left_px, anchor_error_top_px, anchor_error_right_px, anchor_error_bottom_px),
        )
        or 0.0
    )
    band = np.zeros((height, width), dtype=bool)
    cx1, cy1, cx2, cy2 = [max(0, int(v)) for v in clear_bbox]
    ox1, oy1, ox2, oy2 = [max(0, int(v)) for v in overlay_anchor_box_scaled]
    cx2 = min(width, cx2)
    cy2 = min(height, cy2)
    ox2 = min(width, ox2)
    oy2 = min(height, oy2)
    if cx2 > cx1 and cy2 > cy1:
        band[cy1:cy2, cx1:cx2] = True
    if ox2 > ox1 and oy2 > oy1:
        band[oy1:oy2, ox1:ox2] = False
    if not np.any(band):
        band = None

    navy_match_ratio = 1.0
    gold_ratio = 0.0
    if band is not None and np.any(band):
        pixels = composite_arr[band]
        if pixels.size > 0:
            deltas = np.abs(pixels - STANDARD_NAVY_RGB).mean(axis=1)
            navy_match_ratio = float(np.mean(deltas <= NAVY_BAND_DELTA_THRESHOLD))
            gold_mask = (
                (pixels[:, 0] >= 85.0)
                & (pixels[:, 1] >= 70.0)
                & (pixels[:, 0] >= pixels[:, 1])
                & (pixels[:, 1] >= pixels[:, 2])
                & ((pixels[:, 0] - pixels[:, 2]) >= 30.0)
            )
            gold_ratio = float(np.mean(gold_mask))

    return {
        "outer_band_navy_match_ratio": round(navy_match_ratio, 4),
        "outer_band_gold_ratio": round(gold_ratio, 4),
        "source_anchor_box": [int(v) for v in source_anchor_box],
        "overlay_anchor_box_unscaled": [int(v) for v in overlay_anchor_box_unscaled],
        "overlay_anchor_box_scaled": [int(v) for v in overlay_anchor_box_scaled],
        "anchor_error_left_px": round(anchor_error_left_px, 4),
        "anchor_error_top_px": round(anchor_error_top_px, 4),
        "anchor_error_right_px": round(anchor_error_right_px, 4),
        "anchor_error_bottom_px": round(anchor_error_bottom_px, 4),
        "anchor_error_max_px": round(anchor_error_max_px, 4),
        "navy_band_max_px": round(navy_band_max_px, 4),
    }


def _report_check(
    name: str,
    passed: bool,
    *,
    value: str,
    threshold: str,
    detail: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "value": value,
        "threshold": threshold,
        "detail": detail,
    }


def verify_composite(
    *,
    original_path: str | Path,
    composite_path: str | Path,
    book_number: int,
    book_title: str = "",
    center_x: int = CENTER_X,
    center_y: int = CENTER_Y,
    radius: int = RADIUS,
    golden_dir: str | Path = "qa_output/golden",
    output_dir: str | Path = "qa_output",
    compositor_mode: str = "auto",
    replacement_metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run structural visual QA checks on one composite and persist a JSON report."""
    original_file = Path(original_path)
    composite_file = Path(composite_path)
    original = Image.open(original_file).convert("RGB")
    composite = Image.open(composite_file).convert("RGB")
    resolved_mode = _resolve_compositor_mode(composite_file, requested_mode=compositor_mode)

    same_dimensions = original.size == composite.size
    composite_for_analysis = composite if same_dimensions else composite.resize(original.size, Image.LANCZOS)

    original_arr = np.array(original, dtype=np.float32)
    composite_arr = np.array(composite_for_analysis, dtype=np.float32)
    max_diff = np.abs(original_arr - composite_arr).max(axis=2)
    height, width = original_arr.shape[:2]
    dist = _distance_grid(height, width, center_x, center_y)

    checks: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {}

    frame_mask = (dist >= max(0, int(radius) - 20)) & (dist <= int(radius) + 120)
    frame_values = max_diff[frame_mask]
    frame_changed_pct = 0.0 if frame_values.size == 0 else float((frame_values > FRAME_PIXEL_DELTA_THRESHOLD).sum() / frame_values.size * 100.0)
    frame_mean_delta = 0.0 if frame_values.size == 0 else float(frame_values.mean())
    metrics["frame_changed_pct"] = round(frame_changed_pct, 4)
    metrics["frame_mean_delta"] = round(frame_mean_delta, 4)
    if resolved_mode == "replacement_frame":
        checks.append(
            _report_check(
                "frame_ring_integrity",
                True,
                value=f"mode={resolved_mode}, {frame_changed_pct:.2f}% changed, delta {frame_mean_delta:.2f}",
                threshold="replacement-frame mode allows ring replacement",
                detail="Original ring-integrity preservation is not required when the frame is intentionally replaced.",
            )
        )
    else:
        checks.append(
            _report_check(
                "frame_ring_integrity",
                frame_changed_pct < FRAME_CHANGED_THRESHOLD_PCT and frame_mean_delta < FRAME_MEAN_DELTA_THRESHOLD,
                value=f"{frame_changed_pct:.2f}% changed, delta {frame_mean_delta:.2f}",
                threshold=f"<{FRAME_CHANGED_THRESHOLD_PCT:.1f}% changed and delta <{FRAME_MEAN_DELTA_THRESHOLD:.1f}",
                detail="Ornamental frame ring must remain intact.",
            )
        )

    outer_mask = dist > int(radius) + 150
    if resolved_mode == "replacement_frame" and isinstance(replacement_metrics, dict):
        source_anchor_box = replacement_metrics.get("source_anchor_box", [])
        if isinstance(source_anchor_box, (list, tuple)) and len(source_anchor_box) == 4:
            pad = 32
            sx1 = max(0, int(source_anchor_box[0]) - pad)
            sy1 = max(0, int(source_anchor_box[1]) - pad)
            sx2 = min(width, int(source_anchor_box[2]) + pad)
            sy2 = min(height, int(source_anchor_box[3]) + pad)
            outer_mask = np.ones((height, width), dtype=bool)
            outer_mask[sy1:sy2, sx1:sx2] = False
    outer_values = max_diff[outer_mask]
    outer_changed_pct = 0.0 if outer_values.size == 0 else float((outer_values > OUTER_PIXEL_DELTA_THRESHOLD).sum() / outer_values.size * 100.0)
    metrics["outer_changed_pct"] = round(outer_changed_pct, 4)
    checks.append(
        _report_check(
            "art_containment",
            outer_changed_pct < OUTER_CHANGED_THRESHOLD_PCT,
            value=f"{outer_changed_pct:.2f}% outer pixels changed",
            threshold=f"<{OUTER_CHANGED_THRESHOLD_PCT:.1f}% outer pixels changed",
            detail="Generated art should not bleed into outer cover/frame area.",
        )
    )

    inner_mask = dist < max(1, int(radius) - 100)
    inner_values = max_diff[inner_mask]
    inner_changed_pct = 0.0 if inner_values.size == 0 else float((inner_values > INNER_PIXEL_DELTA_THRESHOLD).sum() / inner_values.size * 100.0)
    metrics["inner_changed_pct"] = round(inner_changed_pct, 4)
    checks.append(
        _report_check(
            "art_presence",
            inner_changed_pct > INNER_CHANGED_MIN_PCT,
            value=f"{inner_changed_pct:.2f}% inner pixels changed",
            threshold=f">{INNER_CHANGED_MIN_PCT:.1f}% inner pixels changed",
            detail="Medallion center must contain newly generated art.",
        )
    )

    checks.append(
        _report_check(
            "dimensions_match",
            same_dimensions,
            value=f"original={original.size}, composite={composite.size}",
            threshold="dimensions must match exactly",
            detail="Composite dimensions must match template dimensions.",
        )
    )

    art_zone_mask = dist < max(1, int(radius) - 20)
    changed_pixels = (max_diff > INNER_PIXEL_DELTA_THRESHOLD) & art_zone_mask
    if np.any(changed_pixels):
        ys, xs = np.where(changed_pixels)
        box_w = int(xs.max() - xs.min() + 1)
        box_h = int(ys.max() - ys.min() + 1)
        aspect_ratio = float(box_w) / float(max(1, box_h))
    else:
        box_w = 0
        box_h = 0
        aspect_ratio = 0.0
    metrics["changed_bbox_width"] = box_w
    metrics["changed_bbox_height"] = box_h
    metrics["changed_bbox_aspect_ratio"] = round(aspect_ratio, 4)
    checks.append(
        _report_check(
            "aspect_ratio_sanity",
            bool(np.any(changed_pixels)) and 0.55 <= aspect_ratio <= 1.8,
            value=f"bbox={box_w}x{box_h}, ratio={aspect_ratio:.3f}",
            threshold="changed-art bbox ratio in [0.55, 1.80]",
            detail="Changed medallion art footprint should not be heavily stretched.",
        )
    )

    metrics["compositor_mode"] = resolved_mode
    if resolved_mode == "replacement_frame":
        replacement_metrics = _replacement_outer_band_metrics(
            composite_arr=composite_arr,
            composite_file=composite_file,
            book_number=book_number,
            width=width,
            height=height,
            center_x=center_x,
            center_y=center_y,
            replacement_metrics=replacement_metrics,
        )
        if replacement_metrics is None:
            checks.append(
                _report_check(
                    "replacement_outer_band_clean",
                    True,
                    value="not_applicable",
                    threshold="N/A",
                    detail="Replacement-frame outer-band cleanliness check only runs for standard medallion geometry.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_outer_band_not_gold",
                    True,
                    value="not_applicable",
                    threshold="N/A",
                    detail="Replacement-frame outer-band residue check only runs for standard medallion geometry.",
                )
            )
        else:
            metrics.update(replacement_metrics)
            navy_match_ratio = float(replacement_metrics["outer_band_navy_match_ratio"])
            gold_ratio = float(replacement_metrics["outer_band_gold_ratio"])
            checks.append(
                _report_check(
                    "replacement_anchor_left",
                    float(replacement_metrics["anchor_error_left_px"]) <= ANCHOR_ERROR_EDGE_THRESHOLD_PX,
                    value=f"left anchor error={float(replacement_metrics['anchor_error_left_px']):.3f}px",
                    threshold=f"<={ANCHOR_ERROR_EDGE_THRESHOLD_PX:.1f}px",
                    detail="Replacement frame left edge should align to the source medallion footprint.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_anchor_top",
                    float(replacement_metrics["anchor_error_top_px"]) <= ANCHOR_ERROR_EDGE_THRESHOLD_PX,
                    value=f"top anchor error={float(replacement_metrics['anchor_error_top_px']):.3f}px",
                    threshold=f"<={ANCHOR_ERROR_EDGE_THRESHOLD_PX:.1f}px",
                    detail="Replacement frame top crest should align to the source medallion footprint.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_anchor_right",
                    float(replacement_metrics["anchor_error_right_px"]) <= ANCHOR_ERROR_EDGE_THRESHOLD_PX,
                    value=f"right anchor error={float(replacement_metrics['anchor_error_right_px']):.3f}px",
                    threshold=f"<={ANCHOR_ERROR_EDGE_THRESHOLD_PX:.1f}px",
                    detail="Replacement frame right edge should align to the source medallion footprint.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_anchor_bottom",
                    float(replacement_metrics["anchor_error_bottom_px"]) <= ANCHOR_ERROR_EDGE_THRESHOLD_PX,
                    value=f"bottom anchor error={float(replacement_metrics['anchor_error_bottom_px']):.3f}px",
                    threshold=f"<={ANCHOR_ERROR_EDGE_THRESHOLD_PX:.1f}px",
                    detail="Replacement frame bottom flower should align to the source medallion footprint.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_anchor_max",
                    float(replacement_metrics["anchor_error_max_px"]) <= ANCHOR_ERROR_MAX_THRESHOLD_PX,
                    value=f"max anchor error={float(replacement_metrics['anchor_error_max_px']):.3f}px",
                    threshold=f"<={ANCHOR_ERROR_MAX_THRESHOLD_PX:.1f}px",
                    detail="Maximum anchor mismatch across the replacement frame must stay within signoff tolerance.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_navy_band",
                    float(replacement_metrics["navy_band_max_px"]) <= NAVY_BAND_MAX_THRESHOLD_PX,
                    value=f"navy band={float(replacement_metrics['navy_band_max_px']):.3f}px",
                    threshold=f"<={NAVY_BAND_MAX_THRESHOLD_PX:.1f}px",
                    detail="Visible navy band between the replacement frame and cleared source area must remain effectively invisible.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_outer_band_clean",
                    navy_match_ratio >= NAVY_BAND_MATCH_THRESHOLD,
                    value=f"navy match={navy_match_ratio:.3f}",
                    threshold=f">={NAVY_BAND_MATCH_THRESHOLD:.2f}",
                    detail="Band between cleared source medallion and replacement frame should stay navy, not haloed.",
                )
            )
            checks.append(
                _report_check(
                    "replacement_outer_band_not_gold",
                    gold_ratio <= GOLD_BAND_RATIO_THRESHOLD,
                    value=f"gold ratio={gold_ratio:.3f}",
                    threshold=f"<={GOLD_BAND_RATIO_THRESHOLD:.2f}",
                    detail="Outer band should not retain visible gold source-frame residue or brown halos.",
                )
            )

    golden_path = Path(golden_dir) / f"golden_{int(book_number):03d}.jpg"
    if golden_path.exists():
        golden = Image.open(golden_path).convert("RGB")
        if golden.size != original.size:
            golden = golden.resize(original.size, Image.LANCZOS)
        golden_arr = np.array(golden, dtype=np.float32)
        golden_delta = float(np.abs(golden_arr - composite_arr).mean())
        metrics["golden_mean_delta"] = round(golden_delta, 4)
        checks.append(
            _report_check(
                "golden_reference",
                golden_delta < GOLDEN_MEAN_DELTA_THRESHOLD,
                value=f"mean delta={golden_delta:.2f}",
                threshold=f"mean delta <{GOLDEN_MEAN_DELTA_THRESHOLD:.1f}",
                detail="Comparison against known-good golden reference.",
            )
        )
    else:
        checks.append(
            _report_check(
                "golden_reference",
                True,
                value="no golden reference available",
                threshold="N/A",
                detail="Human review required until a golden reference is stored.",
            )
        )

    passed = all(bool(check.get("passed")) for check in checks)
    failed_checks = [str(check.get("name")) for check in checks if not bool(check.get("passed"))]
    generated_at = datetime.now(timezone.utc).isoformat()

    report = {
        "book_number": int(book_number),
        "book_title": str(book_title or f"Book {int(book_number)}"),
        "passed": bool(passed),
        "failed_checks": failed_checks,
        "checks": checks,
        "metrics": metrics,
        "compositor_mode": resolved_mode,
        "generated_at": generated_at,
        "original_path": _to_project_relative(original_file),
        "composite_path": _to_project_relative(composite_file),
        "golden_path": _to_project_relative(golden_path) if golden_path.exists() else "",
    }

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / f"qa_{int(book_number):03d}.json"
    safe_json.atomic_write_json(report_path, report)
    report["report_path"] = _to_project_relative(report_path)
    return report


def run_batch_verification(
    *,
    input_covers_dir: str | Path,
    composited_dir: str | Path,
    output_dir: str | Path = "qa_output",
    golden_dir: str | Path = "qa_output/golden",
    catalog: list[dict[str, Any]] | None = None,
    book_numbers: list[int] | None = None,
    compositor_mode: str = "auto",
) -> dict[str, Any]:
    """Run structural QA for a set of books and persist a batch report."""
    input_root = Path(input_covers_dir)
    composited_root = Path(composited_dir)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    golden_root = Path(golden_dir)
    golden_root.mkdir(parents=True, exist_ok=True)

    catalog_rows = _build_catalog_rows(catalog or [])
    if not catalog_rows:
        discovered_numbers = sorted(
            {_safe_int(path.name, 0) for path in composited_root.iterdir() if path.is_dir() and path.name.isdigit()}
        )
        catalog_rows = [{"number": n, "title": f"Book {n}", "folder_name": ""} for n in discovered_numbers if n > 0]

    wanted = {int(n) for n in (book_numbers or []) if int(n) > 0}
    if wanted:
        catalog_rows = [row for row in catalog_rows if int(row.get("number", 0)) in wanted]

    results: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    generated_at = datetime.now(timezone.utc).isoformat()

    for row in catalog_rows:
        book_number = int(row["number"])
        book_title = str(row["title"])
        folder_name = str(row.get("folder_name", "")).strip()

        original = _find_original_image(input_root / folder_name) if folder_name else None
        if original is None:
            fallback_dirs = sorted([p for p in input_root.glob(f"{book_number}.*") if p.is_dir()])
            for candidate in fallback_dirs:
                original = _find_original_image(candidate)
                if original is not None:
                    break

        composite = _find_latest_composite_for_book(composited_root, book_number)
        if original is None or composite is None:
            missing.append(
                {
                    "book_number": book_number,
                    "book_title": book_title,
                    "missing_original": original is None,
                    "missing_composite": composite is None,
                }
            )
            continue

        report = verify_composite(
            original_path=original,
            composite_path=composite,
            book_number=book_number,
            book_title=book_title,
            output_dir=out_dir,
            golden_dir=golden_root,
            compositor_mode=compositor_mode,
        )
        results.append(report)

    results.sort(
        key=lambda row: (
            0 if not bool(row.get("passed")) else 1,
            -float((row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}).get("frame_changed_pct", 0.0)),
            _safe_int(row.get("book_number"), 0),
        )
    )

    summary = {
        "total": len(catalog_rows),
        "verified": len(results),
        "passed": sum(1 for row in results if bool(row.get("passed"))),
        "failed": sum(1 for row in results if not bool(row.get("passed"))),
        "not_compared": len(missing),
    }

    payload = {
        "generated_at": generated_at,
        "results": results,
        "missing": missing,
        "summary": summary,
    }
    safe_json.atomic_write_json(out_dir / "qa_report.json", payload)
    return payload


def _load_catalog_for_runtime(runtime: config.Config) -> list[dict[str, Any]]:
    payload = safe_json.load_json(runtime.book_catalog_path, [])
    return payload if isinstance(payload, list) else []


def _parse_selected_books(*, book: int | None, books_csv: str) -> list[int]:
    selected: set[int] = set()
    if book is not None and int(book) > 0:
        selected.add(int(book))
    for token in str(books_csv or "").split(","):
        token = token.strip()
        if token.isdigit() and int(token) > 0:
            selected.add(int(token))
    return sorted(selected)


def _print_summary(payload: dict[str, Any]) -> None:
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    print(
        "Structural QA summary: "
        f"total={_safe_int(summary.get('total'))} "
        f"verified={_safe_int(summary.get('verified'))} "
        f"passed={_safe_int(summary.get('passed'))} "
        f"failed={_safe_int(summary.get('failed'))} "
        f"not_compared={_safe_int(summary.get('not_compared'))}"
    )
    for row in payload.get("results", []):
        if not isinstance(row, dict) or bool(row.get("passed")):
            continue
        checks = ",".join([str(item) for item in row.get("failed_checks", []) if str(item).strip()])
        report_path = str(row.get("report_path", ""))
        print(f"FAIL book={_safe_int(row.get('book_number'))} checks={checks} report={report_path}")


def _run_cli() -> int:
    parser = argparse.ArgumentParser(description="Run structural visual QA checks")
    parser.add_argument("--catalog", default=config.DEFAULT_CATALOG_ID, help="Catalog id")
    parser.add_argument("--book", type=int, help="Single book number")
    parser.add_argument("--books", default="", help="Comma-separated book numbers")
    parser.add_argument("--all", action="store_true", help="Run QA for all books")
    parser.add_argument("--output-dir", default="qa_output", help="Directory for QA JSON output")
    parser.add_argument("--golden-dir", default="qa_output/golden", help="Directory containing golden references")
    parser.add_argument("--input-covers-dir", default="", help="Optional override path for original covers")
    parser.add_argument("--composited-dir", default="", help="Optional override path for composited covers")
    args = parser.parse_args()

    runtime = config.get_config(args.catalog)
    selected_books = _parse_selected_books(book=args.book, books_csv=args.books)
    if not selected_books and not args.all:
        args.all = True

    input_covers_dir = Path(args.input_covers_dir).expanduser() if str(args.input_covers_dir).strip() else runtime.input_dir
    composited_dir = Path(args.composited_dir).expanduser() if str(args.composited_dir).strip() else runtime.tmp_dir / "composited"
    output_dir = Path(args.output_dir).expanduser()
    golden_dir = Path(args.golden_dir).expanduser()

    payload = run_batch_verification(
        input_covers_dir=input_covers_dir,
        composited_dir=composited_dir,
        output_dir=output_dir,
        golden_dir=golden_dir,
        catalog=_load_catalog_for_runtime(runtime),
        book_numbers=selected_books if selected_books else None,
    )
    _print_summary(payload)
    failed = _safe_int((payload.get("summary", {}) if isinstance(payload, dict) else {}).get("failed"), 0)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    raise SystemExit(_run_cli())
