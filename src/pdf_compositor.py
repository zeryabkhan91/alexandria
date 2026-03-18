"""PDF-based compositor using JPG-space medallion blending.

This compositor keeps the current JPG-space strategy but uses shared medallion
geometry for standard Alexandria covers so the inserted art reaches the gold
ring consistently across books and across compositor paths.
"""

from __future__ import annotations

import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageOps

try:
    import pikepdf  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("pikepdf is required for PDF compositor") from exc

try:
    from src import config
    from src import frame_geometry
    from src import protrusion_overlay
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import frame_geometry  # type: ignore
    import protrusion_overlay  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EXPECTED_DPI = 300
EXPECTED_JPG_SIZE = (3784, 2777)          # full cover w x h
WINDOW_MASK_PATH = config.CONFIG_DIR / "compositing_mask.png"

# Im0-space blend parameters used only for non-standard fallback cases.
IM0_BLEND_RADIUS = 644
IM0_BLEND_FEATHER = 30
RING_PIXEL_DELTA_THRESHOLD = 15.0
RING_CHANGED_PCT_THRESHOLD = 5.0
RING_MEAN_DELTA_THRESHOLD = 10.0

# Art pre-processing
AI_ART_EDGE_TRIM_RATIO = 0.08
AI_UNIFORM_MARGIN_MAX_TRIM_RATIO = 0.22
AI_UNIFORM_MARGIN_COLOR_TOL = 26.0
AI_UNIFORM_MARGIN_STD_MAX = 22.0
AI_UNIFORM_MARGIN_MATCH_RATIO = 0.92


# ---------------------------------------------------------------------------
# Utility: trim uniform margins from generated art
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------
def _load_catalog(path: Path) -> list[dict[str, Any]]:
    payload = safe_json.load_json(path, [])
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _find_book_folder_name(*, catalog_path: Path, book_number: int) -> str:
    for row in _load_catalog(catalog_path):
        try:
            number = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        if number == int(book_number):
            return str(row.get("folder_name", "")).strip()
    return ""


def find_source_pdf_for_book(*, input_dir: Path, book_number: int, catalog_path: Path = config.BOOK_CATALOG_PATH) -> Path | None:
    """Return source PDF path for a book when available."""
    folder_name = _find_book_folder_name(catalog_path=catalog_path, book_number=book_number)
    if not folder_name:
        return None
    folder = input_dir / folder_name
    if not folder.exists() or not folder.is_dir():
        return None

    pdfs = sorted([path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".pdf"])
    if pdfs:
        return pdfs[0]

    ais = sorted([path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".ai"])
    if ais:
        return ais[0]
    return None


def find_source_jpg_for_book(*, input_dir: Path, book_number: int, catalog_path: Path = config.BOOK_CATALOG_PATH) -> Path | None:
    """Return the original Illustrator-rendered JPG for a book."""
    folder_name = _find_book_folder_name(catalog_path=catalog_path, book_number=book_number)
    if not folder_name:
        return None
    folder = input_dir / folder_name
    if not folder.exists() or not folder.is_dir():
        return None

    jpgs = sorted([path for path in folder.iterdir() if path.is_file() and path.suffix.lower() in (".jpg", ".jpeg")])
    if jpgs:
        return jpgs[0]
    return None


# ---------------------------------------------------------------------------
# PDF helpers — extract Im0 position on page
# ---------------------------------------------------------------------------
def _resolve_im0(page: Any) -> Any:
    """Find the Im0 image XObject in the PDF page."""
    resources = page.get("/Resources")
    if resources is None:
        raise ValueError("PDF page has no /Resources")
    xobjects = resources.get("/XObject")
    if xobjects is None:
        raise ValueError("PDF page has no /XObject resources")

    if "/Im0" in xobjects:
        return xobjects["/Im0"]

    for _name, obj in xobjects.items():
        try:
            subtype = str(obj.get("/Subtype", ""))
        except Exception:
            subtype = ""
        if subtype == "/Image" and obj.get("/SMask") is not None:
            return obj
    raise ValueError("No image XObject with SMask found (expected /Im0)")


def _extract_im0_transform(pdf_path: Path) -> dict[str, Any]:
    """Extract Im0 dimensions and its cm-transform from the page content stream.

    The content stream contains a `cm` operator that places Im0 on the page:
        a 0 0 d tx ty cm
    Where:
        a  = width in points
        d  = height in points
        tx = left offset in points from left edge
        ty = bottom offset in points from bottom edge
    """
    pdf = pikepdf.Pdf.open(str(pdf_path))
    try:
        if len(pdf.pages) == 0:
            raise ValueError("Source PDF has no pages")
        page = pdf.pages[0]
        im0 = _resolve_im0(page)

        im0_w = int(im0.get("/Width"))
        im0_h = int(im0.get("/Height"))

        # Parse content stream for cm transform preceding Im0 reference
        raw_content = page.Contents.read_bytes().decode("latin-1")

        # Find the cm transform — it's typically the last `cm` before `/Im0 Do`
        # Content stream pattern: ... a 0 0 d tx ty cm ... /Im0 Do ...
        # Find all cm operators
        cm_pattern = re.compile(
            r"([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+cm"
        )
        matches = list(cm_pattern.finditer(raw_content))

        # Find the position of /Im0 Do
        im0_pos = raw_content.find("/Im0")
        if im0_pos < 0:
            # Try to find any image reference
            im0_pos = raw_content.find("Do")

        # Get the cm transform closest before Im0
        best_match = None
        for m in matches:
            if m.start() < im0_pos:
                best_match = m

        if best_match is None:
            raise ValueError("Could not find cm transform for Im0 in content stream")

        a = float(best_match.group(1))   # width in points
        b = float(best_match.group(2))
        c = float(best_match.group(3))
        d = float(best_match.group(4))   # height in points
        tx = float(best_match.group(5))  # x offset in points
        ty = float(best_match.group(6))  # y offset in points

        # Get page dimensions in points
        mediabox = page.MediaBox
        page_w_pts = float(mediabox[2]) - float(mediabox[0])
        page_h_pts = float(mediabox[3]) - float(mediabox[1])

        return {
            "im0_w": im0_w,
            "im0_h": im0_h,
            "cm_a": a,
            "cm_d": d,
            "cm_tx": tx,
            "cm_ty": ty,
            "page_w_pts": page_w_pts,
            "page_h_pts": page_h_pts,
        }
    finally:
        pdf.close()


def _im0_to_jpg_mapping(transform: dict[str, Any], jpg_w: int, jpg_h: int) -> dict[str, Any]:
    """Compute the mapping from Im0 pixel coordinates to JPG pixel coordinates.

    The PDF places Im0 at (tx, ty) with size (a x d) points on a page of
    (page_w x page_h) points.  The JPG is a flat render of the full page.

    In JPG space:
        scale_x = jpg_w / page_w_pts
        scale_y = jpg_h / page_h_pts
        im0_left_jpg  = tx * scale_x
        im0_top_jpg   = (page_h_pts - ty - d) * scale_y   (PDF y is bottom-up)
        im0_width_jpg = a * scale_x
        im0_height_jpg = d * scale_y
    """
    page_w_pts = transform["page_w_pts"]
    page_h_pts = transform["page_h_pts"]
    a = transform["cm_a"]
    d = transform["cm_d"]
    tx = transform["cm_tx"]
    ty = transform["cm_ty"]

    scale_x = jpg_w / page_w_pts
    scale_y = jpg_h / page_h_pts

    im0_left = tx * scale_x
    im0_top = (page_h_pts - ty - d) * scale_y
    im0_w_jpg = a * scale_x
    im0_h_jpg = d * scale_y
    im0_cx = im0_left + im0_w_jpg / 2.0
    im0_cy = im0_top + im0_h_jpg / 2.0

    # Scale factor from Im0 pixel space to JPG pixel space
    im0_to_jpg_scale_x = im0_w_jpg / transform["im0_w"]
    im0_to_jpg_scale_y = im0_h_jpg / transform["im0_h"]

    return {
        "im0_left": im0_left,
        "im0_top": im0_top,
        "im0_w_jpg": im0_w_jpg,
        "im0_h_jpg": im0_h_jpg,
        "im0_cx": im0_cx,
        "im0_cy": im0_cy,
        "im0_to_jpg_scale_x": im0_to_jpg_scale_x,
        "im0_to_jpg_scale_y": im0_to_jpg_scale_y,
        "scale_x": scale_x,
        "scale_y": scale_y,
    }


# ---------------------------------------------------------------------------
# Art loading
# ---------------------------------------------------------------------------
def _load_ai_art_rgb(*, ai_art_path: Path, width: int, height: int) -> Image.Image:
    """Load AI art, trim margins, edge-trim, resize to (width, height) as RGB."""
    with Image.open(ai_art_path) as source:
        rgb_source = _trim_uniform_margins(source)
        if AI_ART_EDGE_TRIM_RATIO > 0:
            src_w, src_h = rgb_source.size
            trim_x = int(round(src_w * AI_ART_EDGE_TRIM_RATIO / 2.0))
            trim_y = int(round(src_h * AI_ART_EDGE_TRIM_RATIO / 2.0))
            if (src_w - 2 * trim_x) >= 64 and (src_h - 2 * trim_y) >= 64:
                rgb_source = rgb_source.crop((trim_x, trim_y, src_w - trim_x, src_h - trim_y))
        rgb = ImageOps.fit(
            rgb_source,
            (int(width), int(height)),
            method=Image.LANCZOS,
            centering=(0.5, 0.5),
        )
    return rgb.convert("RGB")


def _load_cover_regions_payload(regions_path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(regions_path, {})
    if isinstance(payload, dict):
        return payload
    return {}


def _resolve_book_geometry(
    *,
    size: tuple[int, int],
    book_number: int | None,
    regions_path: Path,
) -> tuple[frame_geometry.MedallionGeometry | None, str]:
    payload = _load_cover_regions_payload(regions_path)
    if payload:
        region_row: dict[str, Any] | None = None
        if book_number is not None and int(book_number) > 0:
            for row in payload.get("covers", []):
                if isinstance(row, dict) and int(row.get("cover_id", 0) or 0) == int(book_number):
                    region_row = row
                    break
        if region_row is None and isinstance(payload.get("consensus_region"), dict):
            region_row = payload.get("consensus_region")
        if isinstance(region_row, dict):
            center_x = int(region_row.get("center_x", 0) or 0)
            center_y = int(region_row.get("center_y", 0) or 0)
            radius = int(region_row.get("radius", 0) or 0)
            if center_x > 0 and center_y > 0 and radius > 0:
                return (
                    frame_geometry.resolve_reference_medallion_geometry(
                        size,
                        center_x=center_x,
                        center_y=center_y,
                        radius=radius,
                    ),
                    "cover_region" if book_number is not None and int(book_number) > 0 else "consensus_region",
                )
    if frame_geometry.is_standard_medallion_cover(size):
        return frame_geometry.resolve_standard_medallion_geometry(size), "template_geometry"
    return None, ""


def _load_strict_window_mask(size: tuple[int, int]) -> np.ndarray | None:
    if tuple(size) != EXPECTED_JPG_SIZE:
        return None
    if not WINDOW_MASK_PATH.exists():
        return None
    try:
        mask_rgba = Image.open(WINDOW_MASK_PATH).convert("RGBA")
    except Exception:
        logger.warning("Failed to read compositing mask at %s", WINDOW_MASK_PATH)
        return None
    if mask_rgba.size != size:
        mask_rgba = mask_rgba.resize(size, Image.LANCZOS)
    alpha = np.asarray(mask_rgba.split()[-1], dtype=np.float32) / 255.0
    if float(alpha.max()) <= 0.01 or float(alpha.min()) >= 0.99:
        return None
    return np.clip(alpha, 0.0, 1.0)


def _build_radial_blend_mask(
    *,
    width: int,
    height: int,
    center_x: float,
    center_y: float,
    inner_radius: float,
    outer_radius: float,
) -> np.ndarray:
    yy, xx = np.mgrid[0:height, 0:width]
    dist = np.sqrt((xx - float(center_x)) ** 2 + (yy - float(center_y)) ** 2)
    mask = np.zeros((height, width), dtype=np.float32)
    mask[dist <= float(inner_radius)] = 1.0
    transition = (dist > float(inner_radius)) & (dist < float(outer_radius))
    if np.any(transition) and float(outer_radius) > float(inner_radius):
        span = float(outer_radius) - float(inner_radius)
        mask[transition] = 1.0 - ((dist[transition] - float(inner_radius)) / span)
    return np.clip(mask, 0.0, 1.0)


def _paste_centered_art(
    *,
    canvas: np.ndarray,
    art_arr: np.ndarray,
    center_x: int,
    center_y: int,
) -> None:
    art_h, art_w = art_arr.shape[:2]
    left = int(round(center_x - (art_w / 2.0)))
    top = int(round(center_y - (art_h / 2.0)))
    right = left + art_w
    bottom = top + art_h

    dst_x1 = max(0, left)
    dst_y1 = max(0, top)
    dst_x2 = min(canvas.shape[1], right)
    dst_y2 = min(canvas.shape[0], bottom)
    if dst_x1 >= dst_x2 or dst_y1 >= dst_y2:
        return

    src_x1 = dst_x1 - left
    src_y1 = dst_y1 - top
    src_x2 = src_x1 + (dst_x2 - dst_x1)
    src_y2 = src_y1 + (dst_y2 - dst_y1)
    canvas[dst_y1:dst_y2, dst_x1:dst_x2] = art_arr[src_y1:src_y2, src_x1:src_x2]


def _compute_ring_integrity_metrics(
    *,
    original_arr: np.ndarray,
    composited_arr: np.ndarray,
    center_x: int,
    center_y: int,
    art_clip_radius: int,
) -> dict[str, float]:
    h, w = original_arr.shape[:2]
    yy, xx = np.ogrid[:h, :w]
    dist = np.sqrt((xx - float(center_x)) ** 2 + (yy - float(center_y)) ** 2)
    ring = (dist >= float(art_clip_radius) + 8.0) & (dist <= float(art_clip_radius) + 60.0)
    if not np.any(ring):
        return {"ring_changed_pct": 0.0, "ring_mean_delta": 0.0}

    diff = np.abs(composited_arr.astype(np.float32) - original_arr.astype(np.float32)).max(axis=2)
    ring_diff = diff[ring]
    if ring_diff.size <= 0:
        return {"ring_changed_pct": 0.0, "ring_mean_delta": 0.0}
    changed_pct = 100.0 * float(np.sum(ring_diff > RING_PIXEL_DELTA_THRESHOLD)) / float(ring_diff.size)
    mean_delta = float(ring_diff.mean())
    return {
        "ring_changed_pct": round(changed_pct, 4),
        "ring_mean_delta": round(mean_delta, 4),
    }


# ---------------------------------------------------------------------------
# Main entry: composite_cover_pdf  (JPG-level geometric blend)
# ---------------------------------------------------------------------------
def composite_cover_pdf(
    source_pdf_path: str,
    ai_art_path: str,
    output_pdf_path: str,
    output_jpg_path: str,
    output_ai_path: str | None = None,
    *,
    source_jpg_path: str | None = None,
    book_number: int | None = None,
    regions_path: Path | None = None,
) -> dict[str, Any]:
    """Replace medallion art using JPG-level geometric blend."""
    source_pdf = Path(source_pdf_path)
    art_path = Path(ai_art_path)
    output_pdf = Path(output_pdf_path)
    output_jpg = Path(output_jpg_path)
    output_ai = Path(output_ai_path) if output_ai_path else output_pdf.with_suffix(".ai")

    if not source_pdf.exists():
        raise FileNotFoundError(f"Source PDF not found: {source_pdf}")
    if not art_path.exists():
        raise FileNotFoundError(f"AI art image not found: {art_path}")

    # Find source JPG — either passed explicitly or inferred from same folder
    jpg_path: Path | None = None
    if source_jpg_path:
        jpg_path = Path(source_jpg_path)
    else:
        # Look for JPG in same folder as the source PDF
        folder = source_pdf.parent
        jpg_candidates = sorted([p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in (".jpg", ".jpeg")])
        if jpg_candidates:
            jpg_path = jpg_candidates[0]

    if jpg_path is None or not jpg_path.exists():
        raise FileNotFoundError(f"Source JPG not found alongside PDF: {source_pdf.parent}")

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    output_jpg.parent.mkdir(parents=True, exist_ok=True)
    output_ai.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "JPG compositor start: book=%s source_pdf=%s source_jpg=%s ai_art=%s output_jpg=%s output_pdf=%s",
        int(book_number or 0),
        source_pdf.name,
        jpg_path.name,
        art_path.name,
        output_jpg,
        output_pdf,
    )

    # --- Step 1: Extract Im0 position from PDF ---
    transform = _extract_im0_transform(source_pdf)

    # --- Step 2: Open original JPG ---
    base_jpg = Image.open(jpg_path).convert("RGB")
    jpg_w, jpg_h = base_jpg.size

    # --- Step 3: Map Im0 coordinates to JPG space ---
    mapping = _im0_to_jpg_mapping(transform, jpg_w, jpg_h)

    base_arr = np.asarray(base_jpg, dtype=np.float32)
    result_arr = base_arr.copy()
    overlay_source = "im0_scaled_blend"
    placement_source = "im0_mapping"
    geometry_path = "im0_mapping"
    strict_mask_used = False
    strict_mask_coverage = 0.0
    validation_art_radius = 0
    resolved_regions_path = regions_path or config.cover_regions_path()

    geometry, geometry_path = _resolve_book_geometry(
        size=(jpg_w, jpg_h),
        book_number=book_number,
        regions_path=resolved_regions_path,
    )

    if geometry is not None:
        art_radius = int(geometry.art_clip_radius)
        hole_radius = int(geometry.frame_hole_radius)
        art_diameter = max(2, art_radius * 2)
        new_art = _load_ai_art_rgb(ai_art_path=art_path, width=art_diameter, height=art_diameter)
        art_canvas = np.zeros_like(base_arr)
        _paste_centered_art(
            canvas=art_canvas,
            art_arr=np.asarray(new_art, dtype=np.float32),
            center_x=int(geometry.center_x),
            center_y=int(geometry.center_y),
        )
        mask = _build_radial_blend_mask(
            width=jpg_w,
            height=jpg_h,
            center_x=geometry.center_x,
            center_y=geometry.center_y,
            inner_radius=hole_radius,
            outer_radius=art_radius,
        )
        strict_mask = _load_strict_window_mask((jpg_w, jpg_h))
        if strict_mask is not None:
            strict_mask_used = True
            strict_mask_coverage = float(strict_mask.mean())
            mask = np.minimum(mask, strict_mask)
        logger.info(
            "JPG compositor geometry: book=%s path=%s center=(%d,%d) hole_radius=%d art_radius=%d strict_mask=%s strict_mask_coverage=%.6f regions_path=%s",
            int(book_number or 0),
            geometry_path,
            int(geometry.center_x),
            int(geometry.center_y),
            int(hole_radius),
            int(art_radius),
            "yes" if strict_mask_used else "no",
            float(strict_mask_coverage),
            resolved_regions_path,
        )
        mask_3ch = mask[:, :, np.newaxis]
        result_arr = (art_canvas * mask_3ch) + (base_arr * (1.0 - mask_3ch))
        center_x = int(geometry.center_x)
        center_y = int(geometry.center_y)
        placement_width = art_diameter
        placement_height = art_diameter
        overlay_source = "region_window_mask_blend" if strict_mask_used else "region_circle_blend"
        placement_source = geometry_path
        validation_metrics = _compute_ring_integrity_metrics(
            original_arr=base_arr,
            composited_arr=result_arr,
            center_x=center_x,
            center_y=center_y,
            art_clip_radius=art_radius,
        )
        validation_art_radius = int(art_radius)
    else:
        im0_region_w = int(round(mapping["im0_w_jpg"]))
        im0_region_h = int(round(mapping["im0_h_jpg"]))
        im0_cx = mapping["im0_cx"]
        im0_cy = mapping["im0_cy"]
        im0_scale = frame_geometry.average_jpg_scale(mapping)
        blend_radius_jpg = IM0_BLEND_RADIUS * im0_scale
        feather_jpg = IM0_BLEND_FEATHER * im0_scale

        logger.info(
            "JPG compositor im0 fallback: book=%s im0_left=%.2f im0_top=%.2f im0_size=%.2fx%.2f im0_center=(%.2f,%.2f) jpg_scale=%.6f blend_radius=%.2f feather=%.2f",
            int(book_number or 0),
            float(mapping["im0_left"]),
            float(mapping["im0_top"]),
            float(mapping["im0_w_jpg"]),
            float(mapping["im0_h_jpg"]),
            float(im0_cx),
            float(im0_cy),
            float(im0_scale),
            float(blend_radius_jpg),
            float(feather_jpg),
        )

        new_art = _load_ai_art_rgb(ai_art_path=art_path, width=im0_region_w, height=im0_region_h)
        im0_left = int(round(mapping["im0_left"]))
        im0_top = int(round(mapping["im0_top"]))
        new_art_arr = np.asarray(new_art, dtype=np.float32)

        yy, xx = np.mgrid[0:im0_region_h, 0:im0_region_w]
        rcx = im0_region_w / 2.0
        rcy = im0_region_h / 2.0
        dist = np.sqrt((xx - rcx) ** 2 + (yy - rcy) ** 2)

        inner_r = blend_radius_jpg - feather_jpg / 2.0
        outer_r = blend_radius_jpg + feather_jpg / 2.0
        mask = np.clip((outer_r - dist) / max(1.0, outer_r - inner_r), 0.0, 1.0)
        mask_3ch = mask[:, :, np.newaxis]

        src_y1 = max(0, im0_top)
        src_y2 = min(jpg_h, im0_top + im0_region_h)
        src_x1 = max(0, im0_left)
        src_x2 = min(jpg_w, im0_left + im0_region_w)

        art_y1 = src_y1 - im0_top
        art_y2 = art_y1 + (src_y2 - src_y1)
        art_x1 = src_x1 - im0_left
        art_x2 = art_x1 + (src_x2 - src_x1)

        region_original = result_arr[src_y1:src_y2, src_x1:src_x2]
        region_art = new_art_arr[art_y1:art_y2, art_x1:art_x2]
        region_mask = mask_3ch[art_y1:art_y2, art_x1:art_x2]

        blended = region_art * region_mask + region_original * (1.0 - region_mask)
        result_arr[src_y1:src_y2, src_x1:src_x2] = blended
        center_x = int(round(im0_cx))
        center_y = int(round(im0_cy))
        placement_width = im0_region_w
        placement_height = im0_region_h
        validation_metrics = _compute_ring_integrity_metrics(
            original_arr=base_arr,
            composited_arr=result_arr,
            center_x=center_x,
            center_y=center_y,
            art_clip_radius=max(20, int(round(outer_r))),
        )
        validation_art_radius = max(20, int(round(outer_r)))

    # --- Step 6: Save result ---
    result_img = Image.fromarray(np.clip(result_arr, 0, 255).astype(np.uint8), "RGB")

    # Ensure expected dimensions
    if result_img.size != EXPECTED_JPG_SIZE:
        result_img = result_img.resize(EXPECTED_JPG_SIZE, Image.LANCZOS)

    result_img, protrusion_details = protrusion_overlay.apply_shared_protrusion_overlay(
        image=result_img,
        center_x=int(center_x),
        center_y=int(center_y),
        cover_size=result_img.size,
    )
    logger.info(
        "JPG compositor protrusion overlay: book=%s applied=%s reason=%s overlay_size=%dx%d paste=(%d,%d) center=(%d,%d) applied_center=(%d,%d) components=%s",
        int(book_number or 0),
        "yes" if protrusion_details.get("applied") else "no",
        str(protrusion_details.get("reason", "")),
        int(protrusion_details.get("overlay_width", 0)),
        int(protrusion_details.get("overlay_height", 0)),
        int(protrusion_details.get("paste_x", 0)),
        int(protrusion_details.get("paste_y", 0)),
        int(center_x),
        int(center_y),
        int(protrusion_details.get("applied_center_x", center_x)),
        int(protrusion_details.get("applied_center_y", center_y)),
        protrusion_details.get("components", []),
    )

    final_arr = np.asarray(result_img, dtype=np.float32)
    validation_metrics = _compute_ring_integrity_metrics(
        original_arr=base_arr,
        composited_arr=final_arr,
        center_x=center_x,
        center_y=center_y,
        art_clip_radius=max(20, int(validation_art_radius)),
    )

    result_img.save(output_jpg, format="JPEG", quality=100, subsampling=0, dpi=(EXPECTED_DPI, EXPECTED_DPI))

    # Copy source PDF and AI files for reference (they are not modified)
    shutil.copyfile(source_pdf, output_pdf)
    shutil.copyfile(source_pdf, output_ai)

    valid = (
        float(validation_metrics.get("ring_changed_pct", 0.0)) <= RING_CHANGED_PCT_THRESHOLD
        and float(validation_metrics.get("ring_mean_delta", 0.0)) <= RING_MEAN_DELTA_THRESHOLD
    )
    issues: list[str] = []
    if not valid:
        issues.append("frame_ring_changed")

    logger.info(
        "JPG compositor validation: book=%s valid=%s overlay=%s placement=%s geometry=%s ring_changed_pct=%.4f ring_mean_delta=%.4f thresholds=(%.1f%%, %.1f)",
        int(book_number or 0),
        "yes" if valid else "no",
        overlay_source,
        placement_source,
        geometry_path,
        float(validation_metrics.get("ring_changed_pct", 0.0)),
        float(validation_metrics.get("ring_mean_delta", 0.0)),
        float(RING_CHANGED_PCT_THRESHOLD),
        float(RING_MEAN_DELTA_THRESHOLD),
    )
    if not valid:
        logger.warning(
            "JPG compositor frame-integrity failure: book=%s output_jpg=%s issues=%s metrics=%s",
            int(book_number or 0),
            output_jpg,
            ",".join(issues) or "none",
            validation_metrics,
        )

    logger.info(
        "JPG compositor variant completed: book=%s center=(%d,%d) placement=%dx%d output_jpg=%s output_pdf=%s",
        int(book_number or 0),
        int(center_x),
        int(center_y),
        int(placement_width),
        int(placement_height),
        output_jpg,
        output_pdf,
    )

    return {
        "success": True,
        "source_pdf": str(source_pdf),
        "source_jpg": str(jpg_path),
        "output_pdf": str(output_pdf),
        "output_jpg": str(output_jpg),
        "output_ai": str(output_ai),
        "center_x": center_x,
        "center_y": center_y,
        "image_width": int(transform["im0_w"]),
        "image_height": int(transform["im0_h"]),
        "valid": valid,
        "issues": issues,
        "validation_metrics": validation_metrics,
        "overlay_source": overlay_source,
        "placement_source": placement_source,
        "geometry_path": geometry_path,
        "strict_mask_used": strict_mask_used,
        "strict_mask_coverage": round(strict_mask_coverage, 6),
        "protrusion_overlay": protrusion_details,
    }


# ---------------------------------------------------------------------------
# Batch: composite all variants for a book
# ---------------------------------------------------------------------------
def _parse_variant(stem: str) -> int:
    if "variant_" not in stem:
        return 0
    token = stem.split("variant_", 1)[1].split("_", 1)[0]
    try:
        return int(token)
    except ValueError:
        return 0


def _collect_generated_for_book(generated_dir: Path, book_number: int) -> list[dict[str, Any]]:
    base = generated_dir / str(book_number)
    if not base.exists():
        return []

    rows: list[dict[str, Any]] = []
    image_extensions = {".png", ".jpg", ".jpeg", ".webp"}

    for model_dir in sorted([path for path in base.iterdir() if path.is_dir()]):
        if model_dir.name == "history":
            continue
        for image in sorted([p for p in model_dir.iterdir() if p.is_file() and p.suffix.lower() in image_extensions]):
            variant = _parse_variant(image.stem)
            if variant <= 0:
                continue
            rows.append({"model": model_dir.name, "variant": variant, "path": image})

    for image in sorted([p for p in base.iterdir() if p.is_file() and p.suffix.lower() in image_extensions]):
        variant = _parse_variant(image.stem)
        if variant <= 0:
            continue
        rows.append({"model": "default", "variant": variant, "path": image})

    dedup: dict[tuple[str, int], dict[str, Any]] = {}
    for row in rows:
        dedup[(str(row["model"]), int(row["variant"]))] = row
    return sorted(dedup.values(), key=lambda row: (str(row["model"]), int(row["variant"])))


def composite_all_variants(
    *,
    book_number: int,
    input_dir: Path,
    generated_dir: Path,
    output_dir: Path,
    catalog_path: Path = config.BOOK_CATALOG_PATH,
    regions_path: Path | None = None,
) -> list[Path]:
    """Composite all generated variants for a book via JPG-level blend."""
    source_pdf = find_source_pdf_for_book(input_dir=input_dir, book_number=book_number, catalog_path=catalog_path)
    if source_pdf is None:
        raise FileNotFoundError(f"No source PDF found for book {book_number}")

    source_jpg = find_source_jpg_for_book(input_dir=input_dir, book_number=book_number, catalog_path=catalog_path)
    if source_jpg is None:
        raise FileNotFoundError(f"No source JPG found for book {book_number}")

    image_rows = _collect_generated_for_book(generated_dir=generated_dir, book_number=book_number)
    if not image_rows:
        raise FileNotFoundError(f"No generated variants found for book {book_number}")

    outputs: list[Path] = []
    report_items: list[dict[str, Any]] = []

    for row in image_rows:
        model = str(row["model"])
        variant = int(row["variant"])
        image_path = Path(row["path"])

        if model == "default":
            base_output = output_dir / str(book_number) / f"variant_{variant}"
        else:
            base_output = output_dir / str(book_number) / model / f"variant_{variant}"

        output_pdf = base_output.with_suffix(".pdf")
        output_jpg = base_output.with_suffix(".jpg")
        output_ai = base_output.with_suffix(".ai")

        result = composite_cover_pdf(
            source_pdf_path=str(source_pdf),
            ai_art_path=str(image_path),
            output_pdf_path=str(output_pdf),
            output_jpg_path=str(output_jpg),
            output_ai_path=str(output_ai),
            source_jpg_path=str(source_jpg),
            book_number=book_number,
            regions_path=regions_path,
        )
        outputs.append(output_jpg)
        report_items.append(
            {
                "output_path": str(output_jpg),
                "valid": bool(result.get("valid", True)),
                "issues": list(result.get("issues", [])),
                "mode": "jpg_blend",
                "source_pdf": str(source_pdf),
                "source_jpg": str(source_jpg),
                "variant": variant,
                "model": model,
                "overlay_source": str(result.get("overlay_source", "")),
                "placement_source": str(result.get("placement_source", "")),
                "metrics": {
                    "image_width": float(result.get("image_width", 0)),
                    "image_height": float(result.get("image_height", 0)),
                    **{
                        str(key): float(value)
                        for key, value in dict(result.get("validation_metrics", {})).items()
                        if isinstance(value, (int, float))
                    },
                },
            }
        )

    report = {
        "book_number": int(book_number),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(report_items),
        "invalid": sum(1 for row in report_items if not bool(row.get("valid", False))),
        "items": report_items,
    }
    safe_json.atomic_write_json(output_dir / str(book_number) / "composite_validation.json", report)
    logger.info(
        "JPG compositor batch completed: book=%s variants=%d invalid=%d source_pdf=%s source_jpg=%s output_dir=%s",
        int(book_number),
        int(len(outputs)),
        int(report["invalid"]),
        source_pdf,
        source_jpg,
        output_dir / str(book_number),
    )
    return outputs


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="PDF compositor (JPG blend) for one generated image")
    parser.add_argument("source_pdf", type=Path)
    parser.add_argument("ai_art", type=Path)
    parser.add_argument("output_pdf", type=Path)
    parser.add_argument("output_jpg", type=Path)
    parser.add_argument("--output-ai", type=Path, default=None)
    parser.add_argument("--source-jpg", type=Path, default=None, help="Original JPG (auto-detected if not given)")
    args = parser.parse_args()

    result = composite_cover_pdf(
        source_pdf_path=str(args.source_pdf),
        ai_art_path=str(args.ai_art),
        output_pdf_path=str(args.output_pdf),
        output_jpg_path=str(args.output_jpg),
        output_ai_path=str(args.output_ai) if args.output_ai else None,
        source_jpg_path=str(args.source_jpg) if args.source_jpg else None,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
