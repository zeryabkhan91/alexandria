"""Phase 1A — Cover analysis for configurable template region types."""

from __future__ import annotations

import argparse
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw

try:
    from src import config
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

# Legacy template defaults (navy/gold medallion).
TEMPLATE_CENTER_X = 2864
TEMPLATE_CENTER_Y = 1620
TEMPLATE_RADIUS = 500
TEMPLATE_FRAME_PADDING = 95
BASE_COVER_SIZE = (3784, 2777)  # (width, height)
EXPECTED_COVER_SIZE = BASE_COVER_SIZE
NAVY_RGB = np.array([26.0, 39.0, 68.0], dtype=np.float32)

DEFAULT_REGIONS_JSON = Path("config/cover_regions.json")
DEFAULT_MASK_PNG = Path("config/compositing_mask.png")
DEFAULT_DEBUG_DIR = Path("config/debug_overlays")


@dataclass
class CoverRegion:
    """Detected illustration region definition."""

    center_x: int
    center_y: int
    radius: int
    frame_bbox: tuple[int, int, int, int]
    confidence: float
    region_type: str = "circle"
    rect_bbox: tuple[int, int, int, int] | None = None
    template_id: str = "navy_gold_medallion"
    compositing: str = "raster_first"

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["frame_bbox"] = list(self.frame_bbox)
        if self.rect_bbox is not None:
            data["rect_bbox"] = list(self.rect_bbox)
        return data


def _parse_cover_id(folder_name: str) -> int:
    prefix = folder_name.split(".", 1)[0].strip()
    try:
        return int(prefix)
    except ValueError:
        return 0


def _sorted_cover_folders(input_dir: Path) -> list[Path]:
    folders = [path for path in input_dir.iterdir() if path.is_dir()]
    return sorted(folders, key=lambda path: (_parse_cover_id(path.name), path.name))


def _sorted_cover_jpgs(input_dir: Path) -> list[Path]:
    jpgs: list[Path] = []
    for folder in _sorted_cover_folders(input_dir):
        candidates = sorted(folder.glob("*.jpg"))
        if candidates:
            jpgs.append(candidates[0])
    return jpgs


def _rgb_to_hsv(rgb: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    arr = rgb.astype(np.float32) / 255.0
    red, green, blue = arr[..., 0], arr[..., 1], arr[..., 2]
    cmax = np.max(arr, axis=-1)
    cmin = np.min(arr, axis=-1)
    delta = cmax - cmin

    hue = np.zeros_like(cmax)
    mask = delta > 1e-6

    idx = mask & (cmax == red)
    hue[idx] = ((green[idx] - blue[idx]) / delta[idx]) % 6.0
    idx = mask & (cmax == green)
    hue[idx] = ((blue[idx] - red[idx]) / delta[idx]) + 2.0
    idx = mask & (cmax == blue)
    hue[idx] = ((red[idx] - green[idx]) / delta[idx]) + 4.0
    hue *= 60.0

    sat = np.zeros_like(cmax)
    nz = cmax > 1e-6
    sat[nz] = delta[nz] / cmax[nz]
    val = cmax
    return hue, sat, val


def _clip01(value: float) -> float:
    return float(max(0.0, min(1.0, value)))


def _cover_template(template_id: str) -> dict[str, Any]:
    payload = config.load_cover_templates()
    templates = payload.get("templates", []) if isinstance(payload, dict) else []
    for row in templates:
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip().lower() == template_id.strip().lower():
            return row
    for row in templates:
        if isinstance(row, dict):
            return row
    return {
        "id": "navy_gold_medallion",
        "region_type": "circle",
        "compositing": "raster_first",
        "defaults": {
            "center_x": TEMPLATE_CENTER_X,
            "center_y": TEMPLATE_CENTER_Y,
            "radius": TEMPLATE_RADIUS,
            "frame_padding": TEMPLATE_FRAME_PADDING,
        },
    }


def _make_circle_region(width: int, height: int, template_row: dict[str, Any]) -> CoverRegion:
    defaults = template_row.get("defaults", {}) if isinstance(template_row, dict) else {}
    base_center_x = int(defaults.get("center_x", TEMPLATE_CENTER_X) or TEMPLATE_CENTER_X)
    base_center_y = int(defaults.get("center_y", TEMPLATE_CENTER_Y) or TEMPLATE_CENTER_Y)
    base_radius = int(defaults.get("radius", TEMPLATE_RADIUS) or TEMPLATE_RADIUS)
    frame_padding = int(defaults.get("frame_padding", TEMPLATE_FRAME_PADDING) or TEMPLATE_FRAME_PADDING)

    width_scale = width / BASE_COVER_SIZE[0]
    height_scale = height / BASE_COVER_SIZE[1]
    radius = int(round(base_radius * min(width_scale, height_scale)))

    right_margin = BASE_COVER_SIZE[0] - base_center_x
    center_x = int(round(width - (right_margin * width_scale)))
    center_y = int(round(base_center_y * height_scale))

    frame_radius = radius + frame_padding
    x1 = max(0, center_x - frame_radius)
    y1 = max(0, center_y - frame_radius)
    x2 = min(width - 1, center_x + frame_radius)
    y2 = min(height - 1, center_y + frame_radius)

    return CoverRegion(
        center_x=center_x,
        center_y=center_y,
        radius=radius,
        frame_bbox=(x1, y1, x2, y2),
        confidence=0.0,
        region_type="circle",
        rect_bbox=None,
        template_id=str(template_row.get("id", "navy_gold_medallion")),
        compositing=str(template_row.get("compositing", "raster_first")),
    )


def _make_rectangle_region(width: int, height: int, template_row: dict[str, Any]) -> CoverRegion:
    defaults = template_row.get("defaults", {}) if isinstance(template_row, dict) else {}
    x = int(defaults.get("x", 2080) or 2080)
    y = int(defaults.get("y", 500) or 500)
    w = int(defaults.get("width", 1550) or 1550)
    h = int(defaults.get("height", 1850) or 1850)

    width_scale = width / BASE_COVER_SIZE[0]
    height_scale = height / BASE_COVER_SIZE[1]

    rx1 = max(0, int(round(x * width_scale)))
    ry1 = max(0, int(round(y * height_scale)))
    rx2 = min(width - 1, int(round((x + w) * width_scale)))
    ry2 = min(height - 1, int(round((y + h) * height_scale)))

    center_x = int((rx1 + rx2) / 2)
    center_y = int((ry1 + ry2) / 2)
    radius = max(1, int(min(rx2 - rx1, ry2 - ry1) / 2))

    return CoverRegion(
        center_x=center_x,
        center_y=center_y,
        radius=radius,
        frame_bbox=(rx1, ry1, rx2, ry2),
        confidence=0.95,
        region_type="rectangle",
        rect_bbox=(rx1, ry1, rx2, ry2),
        template_id=str(template_row.get("id", "full_bleed")),
        compositing=str(template_row.get("compositing", "layer_under_text")),
    )


def _compute_confidence(rgb: np.ndarray, region: CoverRegion) -> float:
    if region.region_type != "circle":
        return 0.95

    height, width = rgb.shape[:2]
    yy, xx = np.ogrid[:height, :width]
    dist = np.sqrt((xx - region.center_x) ** 2 + (yy - region.center_y) ** 2)

    ring_mask = (dist >= region.radius - 8) & (dist <= region.radius + 8)
    frame_mask = (dist >= region.radius + 12) & (dist <= region.radius + TEMPLATE_FRAME_PADDING)
    outer_mask = (dist >= region.radius + 120) & (dist <= region.radius + 190)
    inner_mask = dist <= region.radius - 25

    hue, sat, val = _rgb_to_hsv(rgb)
    gold_mask = (hue >= 24.0) & (hue <= 56.0) & (sat >= 0.28) & (val >= 0.25)

    ring_gold = float(gold_mask[ring_mask].mean())
    frame_gold = float(gold_mask[frame_mask].mean())

    rgb32 = rgb.astype(np.float32)
    navy_distance = np.linalg.norm(rgb32 - NAVY_RGB, axis=2)
    outer_navy = float((navy_distance[outer_mask] < 55.0).mean())
    inner_variance = float(rgb32[inner_mask].std(axis=0).mean() / 128.0)

    ring_score = _clip01((ring_gold - 0.20) / 0.30)
    frame_score = _clip01((frame_gold - 0.25) / 0.22)
    outer_score = _clip01((outer_navy - 0.40) / 0.40)
    inner_score = _clip01((inner_variance - 0.40) / 0.25)

    score = (
        (0.35 * ring_score)
        + (0.35 * frame_score)
        + (0.20 * outer_score)
        + (0.10 * inner_score)
    )
    return _clip01(0.90 + (0.10 * score))


def analyze_cover(jpg_path: Path, *, template_id: str = "navy_gold_medallion") -> CoverRegion:
    """Analyze a single cover JPG and return the target region."""
    if not jpg_path.exists():
        raise FileNotFoundError(f"Cover JPG not found: {jpg_path}")

    rgb = np.array(Image.open(jpg_path).convert("RGB"))
    height, width = rgb.shape[:2]
    if abs(width - EXPECTED_COVER_SIZE[0]) > 20 or abs(height - EXPECTED_COVER_SIZE[1]) > 20:
        raise ValueError(
            f"Unexpected cover size for {jpg_path.name}: {(width, height)} "
            f"(expected near {EXPECTED_COVER_SIZE})"
        )

    template_row = _cover_template(template_id)
    region_type = str(template_row.get("region_type", "circle")).strip().lower() or "circle"
    if region_type == "rectangle":
        region = _make_rectangle_region(width, height, template_row)
    else:
        region = _make_circle_region(width, height, template_row)

    region.confidence = _compute_confidence(rgb, region)
    return region


def analyze_all_covers(
    input_dir: Path,
    *,
    template_id: str = "navy_gold_medallion",
    regions_path: Path | None = None,
) -> dict[str, Any]:
    """Analyze all covers and return consensus + per-cover validation."""
    jpgs = _sorted_cover_jpgs(input_dir)
    if not jpgs:
        raise FileNotFoundError(f"No JPG covers found under: {input_dir}")

    template_row = _cover_template(template_id)
    region_type = str(template_row.get("region_type", "circle")).strip().lower() or "circle"

    entries: list[dict[str, Any]] = []
    outliers = 0

    for jpg_path in jpgs:
        region = analyze_cover(jpg_path, template_id=template_id)
        folder = jpg_path.parent.name
        cover_id = _parse_cover_id(folder)
        is_outlier = region.confidence < 0.90
        outliers += int(is_outlier)

        entries.append(
            {
                "cover_id": cover_id,
                "folder": folder,
                "jpg": str(jpg_path),
                **region.to_dict(),
                "is_outlier": is_outlier,
            }
        )

    consensus = analyze_cover(jpgs[0], template_id=template_id)
    payload: dict[str, Any] = {
        "template_name": str(template_row.get("id", template_id)),
        "region_type": region_type,
        "cover_size": {
            "width": EXPECTED_COVER_SIZE[0],
            "height": EXPECTED_COVER_SIZE[1],
            "dpi": 300,
        },
        "consensus_region": consensus.to_dict(),
        "cover_count": len(entries),
        "outlier_count": outliers,
        "covers": entries,
    }

    target_regions_path = regions_path or DEFAULT_REGIONS_JSON
    safe_json.atomic_write_json(target_regions_path, payload)
    logger.info("Wrote region config for %d covers to %s", len(entries), target_regions_path)
    return payload


def generate_compositing_mask(region: CoverRegion, cover_size: tuple[int, int]) -> np.ndarray:
    """Generate RGBA alpha mask for compositing."""
    width, height = cover_size
    mask = np.zeros((height, width, 4), dtype=np.uint8)

    if region.region_type == "rectangle" and region.rect_bbox is not None:
        x1, y1, x2, y2 = region.rect_bbox
        mask[y1:y2, x1:x2, 0:3] = 255
        mask[y1:y2, x1:x2, 3] = 255
        return mask

    yy, xx = np.ogrid[:height, :width]
    dist = np.sqrt((xx - region.center_x) ** 2 + (yy - region.center_y) ** 2)
    circle = dist <= region.radius
    mask[circle, 0:3] = 255
    mask[circle, 3] = 255
    return mask


def save_debug_overlays(input_dir: Path, region: CoverRegion, output_dir: Path, count: int = 5) -> None:
    """Save debug images showing detected region over sample covers."""
    output_dir.mkdir(parents=True, exist_ok=True)
    jpgs = _sorted_cover_jpgs(input_dir)

    for jpg_path in jpgs[:count]:
        cover = Image.open(jpg_path).convert("RGB")
        draw = ImageDraw.Draw(cover)

        if region.region_type == "rectangle" and region.rect_bbox is not None:
            draw.rectangle(region.rect_bbox, outline=(255, 0, 0), width=8)
            label = f"rect={region.rect_bbox}"
        else:
            x1 = region.center_x - region.radius
            y1 = region.center_y - region.radius
            x2 = region.center_x + region.radius
            y2 = region.center_y + region.radius
            draw.ellipse((x1, y1, x2, y2), outline=(255, 0, 0), width=8)
            draw.rectangle(region.frame_bbox, outline=(80, 255, 80), width=4)
            label = f"center=({region.center_x},{region.center_y}) r={region.radius}"

        draw.text((40, 40), label, fill=(255, 255, 255))
        cover_id = _parse_cover_id(jpg_path.parent.name)
        out_path = output_dir / f"debug_overlay_{cover_id:03d}.png"
        cover.save(out_path, format="PNG")

    logger.info("Wrote %d debug overlays to %s", min(count, len(jpgs)), output_dir)


def _write_mask_png(mask: np.ndarray, mask_path: Path) -> None:
    mask_path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(mask, mode="RGBA").save(mask_path, format="PNG")
    logger.info("Wrote compositing mask to %s", mask_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prompt 1A cover analysis runner")
    parser.add_argument("--input-dir", type=Path, default=Path("Input Covers"))
    parser.add_argument("--template-id", type=str, default="navy_gold_medallion")
    parser.add_argument("--regions-path", type=Path, default=DEFAULT_REGIONS_JSON)
    parser.add_argument("--mask-path", type=Path, default=DEFAULT_MASK_PNG)
    parser.add_argument("--debug-dir", type=Path, default=DEFAULT_DEBUG_DIR)
    parser.add_argument("--debug-count", type=int, default=5)
    args = parser.parse_args()

    payload = analyze_all_covers(
        args.input_dir,
        template_id=args.template_id,
        regions_path=args.regions_path,
    )
    consensus = CoverRegion(**payload["consensus_region"])
    mask = generate_compositing_mask(consensus, EXPECTED_COVER_SIZE)
    _write_mask_png(mask, args.mask_path)
    save_debug_overlays(args.input_dir, consensus, args.debug_dir, count=args.debug_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
