#!/usr/bin/env python3
"""
Visual Regression Test for Alexandria Cover Designer Compositing.

Supports two modes:
1. PDF MODE (preferred): Uses the source PDF's SMask to verify frame preservation
2. JPG MODE (fallback): Compares composite JPG against source cover JPG

Usage:
    # PDF mode (preferred - exact SMask-based verification):
    python scripts/verify_composite.py <composited.jpg> --source-pdf <source.pdf> --output-pdf <output.pdf>

    # JPG mode (fallback - radial zone comparison):
    python scripts/verify_composite.py <composited.jpg> <source_cover.jpg>

    # Strict mode (tighter thresholds):
    python scripts/verify_composite.py <composited.jpg> --source-pdf <source.pdf> --output-pdf <output.pdf> --strict

    # JSON output:
    python scripts/verify_composite.py <composited.jpg> --source-pdf <source.pdf> --output-pdf <output.pdf> --ai-art <ai_art.png> --json

Exit codes:
    0 = ALL CHECKS PASSED
    1 = ONE OR MORE CHECKS FAILED
    2 = ERROR (missing files, wrong dimensions, etc.)

This script MUST be run after every compositor change before committing.
Both Claude Cowork and Codex are required to run this and report results.
"""

import argparse
import io
import json
import sys
import zlib
from pathlib import Path

import numpy as np
from PIL import Image

# -- Known Geometry (page-level, for JPG mode) --
CENTER_X = 2864
CENTER_Y = 1620
OUTER_FRAME_RADIUS = 500
PDF_ORNAMENT_SAFE_RADIUS = 520
SMASK_FRAME_MIN = 5
SMASK_FRAME_MAX = 250

# -- Test Zone Radii (for JPG mode) --
ORNAMENT_ZONE_MIN = 480
ART_ZONE_MAX = 370

# -- Thresholds (normal mode) --
ORNAMENT_MATCH_THRESHOLD = 0.995
ART_DIFFER_THRESHOLD = 0.90
CHANNEL_DIFF_TOLERANCE = 2
CENTERING_TOLERANCE_PX = 5
TRANSITION_HARSH_THRESHOLD = 0.02

# -- Strict thresholds --
STRICT_ORNAMENT_MATCH = 0.999
STRICT_ART_DIFFER = 0.95
STRICT_CENTERING_PX = 3

# -- Geometry constants (explicit names for upgraded checks) --
# JPG-space (rendered at 300 DPI, output size 3784x2777)
JPG_CENTER_X = 2864
JPG_CENTER_Y = 1620
JPG_FRAME_RADIUS = 480
RENDER_DPI = 300

# Embedded AI-art analysis space
AI_ART_WIDTH = 2480
AI_ART_HEIGHT = 2470
AI_ART_CENTER_X = 1240
AI_ART_CENTER_Y = 1235
AI_ART_FRAME_INNER_R = 420
AI_ART_FRAME_OUTER_R = 480

# Check 8 thresholds (AI border detection)
AI_BORDER_EDGE_DENSITY_THRESHOLD = 0.08
AI_BORDER_SOBEL_MAGNITUDE_THRESHOLD = 30

# Check 9 thresholds (visual rendered frame comparison)
VISUAL_FRAME_MEAN_DIFF_THRESHOLD = 5.0

# -- Prompt-12 batch verification thresholds --
RING_INNER_R = 420
RING_OUTER_R = 520
THRESHOLD_CHANGED_PCT = 2.0
THRESHOLD_MEAN_DELTA = 5.0
THRESHOLD_MAX_DELTA = 10.0
PIXEL_CHANGE_THRESHOLD = 15


def _import_runtime_modules():
    """Import runtime config/safe_json from src with fallback when script is run directly."""
    root = Path(__file__).resolve().parent.parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    try:
        from src import config as runtime_config  # type: ignore
        from src import safe_json as runtime_safe_json  # type: ignore
    except ModuleNotFoundError:  # pragma: no cover
        import config as runtime_config  # type: ignore
        import safe_json as runtime_safe_json  # type: ignore
    return runtime_config, runtime_safe_json


def _first_file_with_suffix(folder: Path, suffixes: set[str]) -> Path | None:
    if not folder.exists() or not folder.is_dir():
        return None
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in suffixes]
    return sorted(files)[0] if files else None


def _first_composite_for_book(output_dir: Path, book_number: int) -> Path | None:
    book_dir = output_dir / str(book_number)
    if not book_dir.exists() or not book_dir.is_dir():
        return None
    jpgs = sorted(book_dir.rglob("*.jpg"))
    return jpgs[0] if jpgs else None


def verify_single_ring(original_path: Path, composited_path: Path, output_dir: Path | None = None) -> dict:
    """Prompt-12 annulus check for one original/composited cover pair."""
    orig = np.array(Image.open(original_path).convert("RGB"), dtype=np.float32)
    comp_img = Image.open(composited_path).convert("RGB")
    if comp_img.size != (orig.shape[1], orig.shape[0]):
        comp_img = comp_img.resize((orig.shape[1], orig.shape[0]), Image.LANCZOS)
    comp = np.array(comp_img, dtype=np.float32)

    h, w = orig.shape[:2]
    yy, xx = np.ogrid[:h, :w]
    dist = np.sqrt((xx - CENTER_X) ** 2 + (yy - CENTER_Y) ** 2)
    ring = (dist >= RING_INNER_R) & (dist <= RING_OUTER_R)

    diff = np.abs(orig - comp)
    channel_diff = diff.max(axis=2)
    ring_pixels = channel_diff[ring]
    changed = ring_pixels > PIXEL_CHANGE_THRESHOLD
    changed_pct = 100.0 * float(np.sum(changed)) / max(1, int(ring_pixels.size))
    mean_delta = float(ring_pixels.mean()) if ring_pixels.size else 0.0
    max_delta = float(ring_pixels.max()) if ring_pixels.size else 0.0

    passed = (
        changed_pct < THRESHOLD_CHANGED_PCT
        and mean_delta < THRESHOLD_MEAN_DELTA
        and max_delta < THRESHOLD_MAX_DELTA
    )

    result = {
        "original": str(original_path),
        "composited": str(composited_path),
        "ring_changed_pct": round(changed_pct, 3),
        "ring_mean_delta": round(mean_delta, 3),
        "ring_max_delta": round(max_delta, 3),
        "passed": bool(passed),
        "verdict": "PASS" if passed else "FAIL",
    }

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        diff_vis = np.clip(diff * 3.0, 0, 255).astype(np.uint8)
        ring_border = ((dist >= RING_INNER_R - 2) & (dist <= RING_INNER_R + 2)) | (
            (dist >= RING_OUTER_R - 2) & (dist <= RING_OUTER_R + 2)
        )
        diff_vis[ring_border] = [255, 0, 0]
        comparison = np.concatenate([orig.astype(np.uint8), comp.astype(np.uint8), diff_vis], axis=1)
        stem = original_path.stem
        Image.fromarray(comparison).save(output_dir / f"{stem}_verify.jpg", quality=85)

    return result


def verify_catalog_batch(
    *,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
    catalog_path: Path | None = None,
    verify_dir: Path = Path("tmp/verification"),
    report_path: Path = Path("tmp/verification_report.json"),
) -> int:
    """Prompt-12 batch verification across all catalog books."""
    runtime_config, runtime_safe_json = _import_runtime_modules()
    input_dir = input_dir or runtime_config.INPUT_DIR
    output_dir = output_dir or Path("Output Covers")
    catalog_path = catalog_path or runtime_config.BOOK_CATALOG_PATH

    catalog = runtime_safe_json.load_json(catalog_path, [])
    if not isinstance(catalog, list):
        print(f"ERROR: invalid catalog payload at {catalog_path}", file=sys.stderr)
        return 2

    verify_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict] = []
    passes = 0
    fails = 0

    for entry in catalog:
        if not isinstance(entry, dict):
            continue
        book_number = int(entry.get("number", 0) or 0)
        folder_name = str(entry.get("folder_name", "")).strip()
        if book_number <= 0 or not folder_name:
            continue

        source_folder = input_dir / folder_name
        source_jpg = _first_file_with_suffix(source_folder, {".jpg", ".jpeg", ".png", ".webp"})
        composited_jpg = _first_composite_for_book(output_dir, book_number)
        if source_jpg is None or composited_jpg is None:
            continue

        result = verify_single_ring(source_jpg, composited_jpg, verify_dir)
        result["book_number"] = book_number
        result["folder_name"] = folder_name
        results.append(result)
        if result["passed"]:
            passes += 1
        else:
            fails += 1
            print(
                f"FAIL {book_number:>3}: changed={result['ring_changed_pct']}% "
                f"mean={result['ring_mean_delta']} max={result['ring_max_delta']} "
                f"({Path(result['composited']).name})"
            )

    avg_changed = float(np.mean([float(r["ring_changed_pct"]) for r in results])) if results else 0.0
    avg_mean_delta = float(np.mean([float(r["ring_mean_delta"]) for r in results])) if results else 0.0
    avg_max_delta = float(np.mean([float(r["ring_max_delta"]) for r in results])) if results else 0.0
    report = {
        "total": len(results),
        "passed": passes,
        "failed": fails,
        "pass_rate": f"{100.0 * passes / max(1, len(results)):.1f}%",
        "averages": {
            "ring_changed_pct": round(avg_changed, 3),
            "ring_mean_delta": round(avg_mean_delta, 3),
            "ring_max_delta": round(avg_max_delta, 3),
        },
        "thresholds": {
            "ring_changed_pct_lt": THRESHOLD_CHANGED_PCT,
            "ring_mean_delta_lt": THRESHOLD_MEAN_DELTA,
            "ring_max_delta_lt": THRESHOLD_MAX_DELTA,
            "pixel_change_threshold": PIXEL_CHANGE_THRESHOLD,
        },
        "verify_dir": str(verify_dir),
        "results": results,
    }
    runtime_safe_json.atomic_write_json(report_path, report)

    print("\n" + ("=" * 60))
    print("PROMPT-12 VERIFICATION REPORT")
    print("=" * 60)
    print(f"Input covers:      {input_dir}")
    print(f"Composited covers: {output_dir}")
    print(f"Compared covers:   {len(results)}")
    print(f"PASSED:            {passes}")
    print(f"FAILED:            {fails}")
    print(f"Avg changed %:     {avg_changed:.3f} (threshold < {THRESHOLD_CHANGED_PCT})")
    print(f"Avg mean delta:    {avg_mean_delta:.3f} (threshold < {THRESHOLD_MEAN_DELTA})")
    print(f"Avg max delta:     {avg_max_delta:.3f} (threshold < {THRESHOLD_MAX_DELTA})")
    print(f"Report JSON:       {report_path}")
    print(f"Comparison images: {verify_dir}")
    print("=" * 60 + "\n")

    if fails > 0:
        print("*** VERIFICATION FAILED - DO NOT COMMIT ***")
        return 1
    print("All covers passed verification.")
    return 0


def load_image_array(path: Path) -> np.ndarray:
    """Load image as RGB numpy array."""
    img = Image.open(path).convert("RGB")
    return np.array(img, dtype=np.uint8)


def normalize_render_shape(render: np.ndarray, target_shape: tuple[int, int, int] | tuple[int, int]) -> np.ndarray:
    """Resize rendered image to match target HxW when PDF renderer rounding differs."""
    target_h = int(target_shape[0])
    target_w = int(target_shape[1])
    if render.shape[0] == target_h and render.shape[1] == target_w:
        return render
    resized = Image.fromarray(render, mode="RGB").resize((target_w, target_h), Image.LANCZOS)
    return np.array(resized, dtype=np.uint8)


def normalize_reference_jpeg(reference: np.ndarray) -> np.ndarray:
    """Match comparison baseline to output JPG compression characteristics."""
    buffer = io.BytesIO()
    Image.fromarray(reference, mode="RGB").save(
        buffer,
        format="JPEG",
        quality=100,
        subsampling=0,
        dpi=(300, 300),
    )
    buffer.seek(0)
    with Image.open(buffer) as image:
        return np.array(image.convert("RGB"), dtype=np.uint8)


def make_radial_mask(shape, center_x, center_y, radius):
    """Create a boolean mask for pixels within radius of center."""
    h, w = shape[:2]
    yy, xx = np.ogrid[:h, :w]
    dist_sq = (xx - center_x).astype(np.float64) ** 2 + (yy - center_y).astype(np.float64) ** 2
    return dist_sq <= radius**2


# =================================================================
# PDF MODE - SMask-based verification (preferred)
# =================================================================

def extract_pdf_smask_and_image(pdf_path: Path):
    """Extract SMask and original CMYK image from source PDF."""
    import pikepdf

    pdf = pikepdf.Pdf.open(str(pdf_path))
    page = pdf.pages[0]
    xobjects = page.get("/Resources").get("/XObject")
    im0 = xobjects["/Im0"]

    w = int(im0.get("/Width"))
    h = int(im0.get("/Height"))
    raw = zlib.decompress(bytes(im0.read_raw_bytes()))
    cmyk = np.frombuffer(raw, dtype=np.uint8).reshape(h, w, 4)

    smask_obj = im0.get("/SMask")
    smask_raw = zlib.decompress(bytes(smask_obj.read_raw_bytes()))
    smask = np.frombuffer(smask_raw, dtype=np.uint8).reshape(h, w)

    pdf.close()
    return cmyk, smask, w, h


def check_ornament_zone_pdf(
    composite_jpg: np.ndarray,
    source_pdf_path: Path,
    threshold: float,
    output_pdf_path: Path | None = None,
) -> dict:
    """
    PDF-mode ornament check.
    Render the SOURCE PDF to JPG, then confirm that pixels in the
    ornament zone (SMask 5-250) are identical between source render
    and composite output.
    """
    if output_pdf_path and Path(output_pdf_path).exists():
        cmyk_src, smask, _, _ = extract_pdf_smask_and_image(source_pdf_path)

        import pikepdf

        pdf = pikepdf.Pdf.open(str(output_pdf_path))
        page = pdf.pages[0]
        xobjects = page.get("/Resources").get("/XObject")
        im0 = xobjects["/Im0"]
        w = int(im0.get("/Width"))
        h = int(im0.get("/Height"))
        raw = zlib.decompress(bytes(im0.read_raw_bytes()))
        cmyk_out = np.frombuffer(raw, dtype=np.uint8).reshape(h, w, 4)
        pdf.close()

        preserve_mask = smask <= SMASK_FRAME_MAX
        total = int(np.sum(preserve_mask))
        if total == 0:
            return {"pass": False, "error": "No non-opaque ornament pixels found in SMask"}

        matching = int(np.sum(np.all(cmyk_src[preserve_mask] == cmyk_out[preserve_mask], axis=1)))
        ratio = matching / total
        passed = ratio >= threshold
        return {
            "pass": passed,
            "match_ratio": round(ratio, 6),
            "threshold": threshold,
            "total_pixels": total,
            "matching_pixels": matching,
            "mismatched_pixels": total - matching,
            "message": (
                f"PASS: {ratio:.2%} ornament/non-opaque CMYK pixels match source"
                if passed
                else f"FAIL: {ratio:.2%} ornament/non-opaque pixels match (need {threshold:.1%}). "
                f"{total - matching:,} pixels differ."
            ),
        }

    import fitz

    doc = fitz.open(str(source_pdf_path))
    page = doc[0]
    mat = fitz.Matrix(300 / 72, 300 / 72)
    pix = page.get_pixmap(matrix=mat)
    source_render = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
    if pix.n == 4:  # RGBA
        source_render = source_render[:, :, :3]
    doc.close()
    source_render = normalize_render_shape(source_render, composite_jpg.shape)
    source_render = normalize_reference_jpeg(source_render)

    ornament_mask = ~make_radial_mask(composite_jpg.shape, CENTER_X, CENTER_Y, PDF_ORNAMENT_SAFE_RADIUS)
    h = min(composite_jpg.shape[0], source_render.shape[0])
    w = min(composite_jpg.shape[1], source_render.shape[1])
    comp = composite_jpg[:h, :w]
    src = source_render[:h, :w]
    om = ornament_mask[:h, :w]

    total = int(np.sum(om))
    if total == 0:
        return {"pass": False, "error": "No ornament zone pixels found"}
    diff = np.max(np.abs(comp.astype(np.int16) - src.astype(np.int16)), axis=2)
    matching = int(np.sum(diff[om] <= CHANNEL_DIFF_TOLERANCE))
    ratio = matching / total
    passed = ratio >= threshold
    return {
        "pass": passed,
        "match_ratio": round(ratio, 6),
        "threshold": threshold,
        "total_pixels": total,
        "matching_pixels": matching,
        "mismatched_pixels": total - matching,
        "message": (
            f"PASS: {ratio:.2%} ornament pixels match source PDF render"
            if passed
            else f"FAIL: {ratio:.2%} ornament pixels match (need {threshold:.1%}). "
            f"{total - matching:,} pixels differ."
        ),
    }


def check_art_zone_pdf(composite_jpg: np.ndarray, source_pdf_path: Path, threshold: float) -> dict:
    """
    PDF-mode art check.
    Pixels in the art zone (r < 370 from center) must DIFFER from the
    source PDF render - meaning AI art has replaced the original illustration.
    """
    import fitz

    doc = fitz.open(str(source_pdf_path))
    page = doc[0]
    mat = fitz.Matrix(300 / 72, 300 / 72)
    pix = page.get_pixmap(matrix=mat)
    source_render = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
    if pix.n == 4:
        source_render = source_render[:, :, :3]
    doc.close()
    source_render = normalize_render_shape(source_render, composite_jpg.shape)
    source_render = normalize_reference_jpeg(source_render)

    art_mask = make_radial_mask(composite_jpg.shape, CENTER_X, CENTER_Y, ART_ZONE_MAX)

    h = min(composite_jpg.shape[0], source_render.shape[0])
    w = min(composite_jpg.shape[1], source_render.shape[1])
    comp = composite_jpg[:h, :w]
    src = source_render[:h, :w]
    am = art_mask[:h, :w]

    total = int(np.sum(am))
    if total == 0:
        return {"pass": False, "error": "No art zone pixels found"}

    diff = np.max(np.abs(comp.astype(np.int16) - src.astype(np.int16)), axis=2)
    different = int(np.sum(diff[am] > CHANNEL_DIFF_TOLERANCE))
    ratio = different / total

    passed = ratio >= threshold
    return {
        "pass": passed,
        "differ_ratio": round(ratio, 6),
        "threshold": threshold,
        "total_pixels": total,
        "different_pixels": different,
        "same_pixels": total - different,
        "message": (
            f"PASS: {ratio:.2%} art zone pixels differ from source (AI art present)"
            if passed
            else f"FAIL: {ratio:.2%} art zone pixels differ (need {threshold:.0%}). "
            f"Original illustration may still be visible."
        ),
    }


def check_smask_integrity(source_pdf_path: Path, output_pdf_path: Path) -> dict:
    """
    NEW CHECK - Verify SMask in the output PDF is bit-identical to source.
    This is the most critical check: the SMask defines the frame boundary
    and must NEVER be modified.
    """
    _, smask_src, _, _ = extract_pdf_smask_and_image(source_pdf_path)

    import pikepdf

    pdf = pikepdf.Pdf.open(str(output_pdf_path))
    page = pdf.pages[0]
    xobjects = page.get("/Resources").get("/XObject")
    im0 = xobjects["/Im0"]
    smask_obj = im0.get("/SMask")
    w = int(smask_obj.get("/Width"))
    h = int(smask_obj.get("/Height"))
    smask_out_raw = zlib.decompress(bytes(smask_obj.read_raw_bytes()))
    smask_out = np.frombuffer(smask_out_raw, dtype=np.uint8).reshape(h, w)
    pdf.close()

    identical = np.array_equal(smask_src, smask_out)
    if not identical:
        diff_count = int(np.sum(smask_src != smask_out))
        total = smask_src.size
    else:
        diff_count = 0
        total = smask_src.size

    return {
        "pass": identical,
        "total_pixels": total,
        "differing_pixels": diff_count,
        "message": (
            f"PASS: SMask is bit-identical between source and output ({total:,} pixels)"
            if identical
            else f"FAIL: SMask was modified! {diff_count:,} of {total:,} pixels differ. "
            f"The SMask must NEVER be changed."
        ),
    }


def check_frame_pixels_preserved(source_pdf_path: Path, output_pdf_path: Path) -> dict:
    """
    NEW CHECK - At the raster image level, verify that pixels in the frame
    ring (SMask 5-250) are identical between source and output Im0 data.
    This confirms the compositor kept original frame pixels.
    """
    cmyk_src, smask, _, _ = extract_pdf_smask_and_image(source_pdf_path)

    import pikepdf

    pdf = pikepdf.Pdf.open(str(output_pdf_path))
    page = pdf.pages[0]
    xobjects = page.get("/Resources").get("/XObject")
    im0 = xobjects["/Im0"]
    w = int(im0.get("/Width"))
    h = int(im0.get("/Height"))
    raw = zlib.decompress(bytes(im0.read_raw_bytes()))
    cmyk_out = np.frombuffer(raw, dtype=np.uint8).reshape(h, w, 4)
    pdf.close()

    frame_mask = (smask >= 5) & (smask <= 250)
    total = int(np.sum(frame_mask))
    if total == 0:
        return {"pass": False, "error": "No frame ring pixels found in SMask"}

    # Frame pixels must be byte-identical (no JPEG tolerance - this is raw CMYK)
    matching = int(np.sum(np.all(cmyk_src[frame_mask] == cmyk_out[frame_mask], axis=1)))
    ratio = matching / total

    passed = ratio >= 0.9999  # Essentially 100% - allow 1-2 pixels for edge cases
    return {
        "pass": passed,
        "match_ratio": round(ratio, 6),
        "total_frame_pixels": total,
        "matching_pixels": matching,
        "differing_pixels": total - matching,
        "message": (
            f"PASS: {ratio:.4%} of frame ring pixels ({total:,}) preserved from source"
            if passed
            else f"FAIL: {ratio:.4%} of frame ring pixels match. "
            f"{total - matching:,} frame pixels were corrupted."
        ),
    }


def check_ai_art_border(ai_art_path: Path) -> dict:
    """
    Check 8 - AI Art Border Detection.

    Detect structural edge density in the annular ring that maps to the
    ornamental frame zone. High edge density in this ring suggests the
    generated art has a decorative border that will bleed through the
    semi-transparent frame pixels.
    """
    try:
        import cv2
    except Exception as exc:  # pragma: no cover - import guard
        return {
            "pass": False,
            "message": f"FAIL: Check 8 unavailable because cv2 could not be imported ({exc})",
        }

    image = cv2.imread(str(ai_art_path), cv2.IMREAD_GRAYSCALE)
    if image is None:
        return {"pass": False, "message": f"FAIL: Check 8 could not load AI art: {ai_art_path}"}

    h, w = image.shape
    resized_note = ""
    if (w, h) != (AI_ART_WIDTH, AI_ART_HEIGHT):
        image = cv2.resize(image, (AI_ART_WIDTH, AI_ART_HEIGHT), interpolation=cv2.INTER_LINEAR)
        resized_note = f" (resized from {w}x{h})"
        h, w = image.shape

    ys, xs = np.mgrid[0:h, 0:w]
    dist = np.sqrt((xs - AI_ART_CENTER_X) ** 2 + (ys - AI_ART_CENTER_Y) ** 2)
    ring_mask = (dist >= AI_ART_FRAME_INNER_R) & (dist <= AI_ART_FRAME_OUTER_R)
    total = int(np.sum(ring_mask))
    if total == 0:
        return {"pass": False, "message": "FAIL: Check 8 frame-zone mask is empty"}

    sobel_x = cv2.Sobel(image, cv2.CV_64F, 1, 0, ksize=3)
    sobel_y = cv2.Sobel(image, cv2.CV_64F, 0, 1, ksize=3)
    sobel_mag = np.sqrt(sobel_x**2 + sobel_y**2)
    sobel_mag_8u = np.clip(sobel_mag, 0, 255).astype(np.uint8)
    ring_pixels = sobel_mag_8u[ring_mask]
    strong_edges = int(np.sum(ring_pixels >= AI_BORDER_SOBEL_MAGNITUDE_THRESHOLD))
    edge_density = strong_edges / total

    passed = edge_density <= AI_BORDER_EDGE_DENSITY_THRESHOLD
    if passed:
        msg = (
            f"PASS: AI art frame zone is clean. Edge density {edge_density:.4f} "
            f"(threshold {AI_BORDER_EDGE_DENSITY_THRESHOLD:.4f}){resized_note}"
        )
    else:
        msg = (
            f"FAIL: AI art likely contains decorative border. Edge density {edge_density:.4f} "
            f"(threshold {AI_BORDER_EDGE_DENSITY_THRESHOLD:.4f}){resized_note}"
        )
    return {
        "pass": passed,
        "edge_density": round(edge_density, 6),
        "threshold": AI_BORDER_EDGE_DENSITY_THRESHOLD,
        "message": msg,
    }


def check_visual_frame(source_pdf_path: Path, output_pdf_path: Path) -> dict:
    """
    Check 9 - Visual rendered frame comparison.

    Render both PDFs at 300 DPI and compare frame-zone pixels (r > 480 from
    center in 3784x2777 JPG-space). This catches visual corruption that can
    be missed by raw CMYK stream checks.
    """
    import fitz

    def _render_rgb(path: Path) -> np.ndarray:
        doc = fitz.open(str(path))
        try:
            page = doc.load_page(0)
            mat = fitz.Matrix(RENDER_DPI / 72.0, RENDER_DPI / 72.0)
            pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB, alpha=False)
            return np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, 3)
        finally:
            doc.close()

    src_rgb = _render_rgb(source_pdf_path)
    out_rgb = _render_rgb(output_pdf_path)

    expected_h, expected_w = 2777, 3784
    src_h, src_w = src_rgb.shape[:2]
    out_h, out_w = out_rgb.shape[:2]
    if (src_h, src_w) != (out_h, out_w):
        return {
            "pass": False,
            "message": (
                "FAIL: Check 9 source/output render size mismatch "
                f"({src_w}x{src_h} vs {out_w}x{out_h})"
            ),
        }

    # Some PDFs render a few pixels smaller/larger at fixed DPI due to source
    # page boxes. Keep the check robust by scaling frame geometry to actual size.
    actual_h, actual_w = src_h, src_w
    scale_x = actual_w / expected_w
    scale_y = actual_h / expected_h
    center_x = JPG_CENTER_X * scale_x
    center_y = JPG_CENTER_Y * scale_y

    ys, xs = np.mgrid[0:actual_h, 0:actual_w]
    dist = np.sqrt(((xs - center_x) / scale_x) ** 2 + ((ys - center_y) / scale_y) ** 2)
    frame_mask = dist > JPG_FRAME_RADIUS
    frame_pixels = int(np.sum(frame_mask))
    if frame_pixels == 0:
        return {"pass": False, "message": "FAIL: Check 9 frame mask is empty"}

    diff = np.abs(src_rgb.astype(np.float32) - out_rgb.astype(np.float32))
    frame_diff = diff[frame_mask]
    mean_diff = float(frame_diff.mean()) if frame_diff.size else 0.0

    passed = mean_diff <= VISUAL_FRAME_MEAN_DIFF_THRESHOLD
    return {
        "pass": passed,
        "mean_abs_diff": round(mean_diff, 6),
        "threshold": VISUAL_FRAME_MEAN_DIFF_THRESHOLD,
        "frame_pixels": frame_pixels,
        "message": (
            f"PASS: Rendered frame zone matches source. Mean diff {mean_diff:.3f}"
            if passed
            else (
                f"FAIL: Rendered frame zone changed. Mean diff {mean_diff:.3f} "
                f"(threshold {VISUAL_FRAME_MEAN_DIFF_THRESHOLD:.3f})"
            )
        ),
    }


# =================================================================
# JPG MODE - Radial zone comparison (fallback)
# =================================================================

def check_ornament_zone_jpg(composite, source, threshold):
    """JPG-mode: Check ornament zone pixels match source."""
    ornament_mask = ~make_radial_mask(composite.shape, CENTER_X, CENTER_Y, ORNAMENT_ZONE_MIN)
    frame_outer = make_radial_mask(composite.shape, CENTER_X, CENTER_Y, OUTER_FRAME_RADIUS + 50)
    full_cover = ~make_radial_mask(composite.shape, CENTER_X, CENTER_Y, OUTER_FRAME_RADIUS + 50)
    check_mask = (ornament_mask & frame_outer) | full_cover

    total = int(np.sum(check_mask))
    if total == 0:
        return {"pass": False, "error": "No ornament zone pixels found"}

    diff = np.max(np.abs(composite.astype(np.int16) - source.astype(np.int16)), axis=2)
    matching = int(np.sum(diff[check_mask] <= CHANNEL_DIFF_TOLERANCE))
    ratio = matching / total

    passed = ratio >= threshold
    return {
        "pass": passed,
        "match_ratio": round(ratio, 6),
        "threshold": threshold,
        "total_pixels": total,
        "matching_pixels": matching,
        "mismatched_pixels": total - matching,
        "message": (
            f"PASS: {ratio:.2%} ornament pixels match source"
            if passed
            else f"FAIL: {ratio:.2%} match (need {threshold:.1%}). {total - matching:,} differ."
        ),
    }


def check_art_zone_jpg(composite, source, threshold):
    """JPG-mode: Check art zone pixels differ from source."""
    art_mask = make_radial_mask(composite.shape, CENTER_X, CENTER_Y, ART_ZONE_MAX)
    total = int(np.sum(art_mask))
    if total == 0:
        return {"pass": False, "error": "No art zone pixels found"}

    diff = np.max(np.abs(composite.astype(np.int16) - source.astype(np.int16)), axis=2)
    different = int(np.sum(diff[art_mask] > CHANNEL_DIFF_TOLERANCE))
    ratio = different / total

    passed = ratio >= threshold
    return {
        "pass": passed,
        "differ_ratio": round(ratio, 6),
        "threshold": threshold,
        "total_pixels": total,
        "different_pixels": different,
        "same_pixels": total - different,
        "message": (
            f"PASS: {ratio:.2%} art zone pixels differ (AI art present)"
            if passed
            else f"FAIL: {ratio:.2%} differ (need {threshold:.0%}). Original art may show."
        ),
    }


# =================================================================
# COMMON CHECKS (both modes)
# =================================================================

def check_dimensions(composite, expected_w=3784, expected_h=2777):
    h, w = composite.shape[:2]
    passed = (w == expected_w) and (h == expected_h)
    return {
        "pass": passed,
        "actual_size": f"{w}x{h}",
        "expected_size": f"{expected_w}x{expected_h}",
        "message": (
            f"PASS: Dimensions {w}x{h} match expected"
            if passed
            else f"FAIL: Dimensions {w}x{h}, expected {expected_w}x{expected_h}"
        ),
    }


def check_centering(composite, source):
    """Check AI art is centered at medallion center."""
    diff = np.max(np.abs(composite.astype(np.int16) - source.astype(np.int16)), axis=2)
    art_pixels = diff > 20
    medallion_mask = make_radial_mask(composite.shape, CENTER_X, CENTER_Y, OUTER_FRAME_RADIUS)
    art_in_medallion = art_pixels & medallion_mask

    if not np.any(art_in_medallion):
        return {"pass": False, "error": "No art detected in medallion area"}

    ys, xs = np.where(art_in_medallion)
    cx, cy = float(np.mean(xs)), float(np.mean(ys))
    offset = ((cx - CENTER_X) ** 2 + (cy - CENTER_Y) ** 2) ** 0.5

    passed = offset <= CENTERING_TOLERANCE_PX
    return {
        "pass": passed,
        "art_center_x": round(cx, 1),
        "art_center_y": round(cy, 1),
        "offset_total": round(offset, 1),
        "tolerance": CENTERING_TOLERANCE_PX,
        "message": (
            f"PASS: Art centered at ({cx:.0f},{cy:.0f}), offset {offset:.1f}px"
            if passed
            else f"FAIL: Art at ({cx:.0f},{cy:.0f}), offset {offset:.1f}px > {CENTERING_TOLERANCE_PX}px"
        ),
    }


def check_transition_zone(composite):
    """Check transition zone for harsh artifacts."""
    inner = make_radial_mask(composite.shape, CENTER_X, CENTER_Y, ART_ZONE_MAX)
    outer = make_radial_mask(composite.shape, CENTER_X, CENTER_Y, ORNAMENT_ZONE_MIN)
    transition = outer & ~inner

    total = int(np.sum(transition))
    if total == 0:
        return {"pass": False, "error": "No transition zone pixels"}

    gray = np.mean(composite.astype(np.float32), axis=2)
    gx = np.abs(np.diff(gray, axis=1, prepend=gray[:, :1]))
    gy = np.abs(np.diff(gray, axis=0, prepend=gray[:1, :]))
    gradient = np.sqrt(gx**2 + gy**2)

    harsh = int(np.sum(gradient[transition] > 100))
    ratio = harsh / total

    passed = ratio < TRANSITION_HARSH_THRESHOLD
    return {
        "pass": passed,
        "harsh_ratio": round(ratio, 6),
        "total_pixels": total,
        "message": (
            f"PASS: Transition zone clean ({ratio:.2%} harsh pixels)"
            if passed
            else f"FAIL: Transition artifacts ({ratio:.2%} harsh pixels)"
        ),
    }


# =================================================================
# MAIN ORCHESTRATOR
# =================================================================

def verify_composite(
    composite_path,
    source_jpg_path=None,
    source_pdf_path=None,
    output_pdf_path=None,
    ai_art_path=None,
    strict=False,
):
    """Run all verification checks. Auto-selects PDF or JPG mode."""

    if strict:
        global ORNAMENT_MATCH_THRESHOLD, ART_DIFFER_THRESHOLD, CENTERING_TOLERANCE_PX
        ORNAMENT_MATCH_THRESHOLD = STRICT_ORNAMENT_MATCH
        ART_DIFFER_THRESHOLD = STRICT_ART_DIFFER
        CENTERING_TOLERANCE_PX = STRICT_CENTERING_PX

    mode = "PDF" if source_pdf_path else "JPG"
    print(f"\n{'=' * 70}")
    print(f"COMPOSITE VERIFICATION [{mode} MODE]{'  (STRICT)' if strict else ''}")
    print(f"  Composite: {composite_path}")
    if source_pdf_path:
        print(f"  Source PDF: {source_pdf_path}")
    if source_jpg_path:
        print(f"  Source JPG: {source_jpg_path}")
    if output_pdf_path:
        print(f"  Output PDF: {output_pdf_path}")
    print(f"{'=' * 70}\n")

    composite = load_image_array(composite_path)
    checks = {}

    # 1. Dimensions
    checks["dimensions"] = check_dimensions(composite)

    if mode == "PDF":
        # PDF-mode checks
        checks["ornament_zone"] = check_ornament_zone_pdf(
            composite,
            source_pdf_path,
            ORNAMENT_MATCH_THRESHOLD,
            output_pdf_path=output_pdf_path,
        )
        checks["art_zone"] = check_art_zone_pdf(composite, source_pdf_path, ART_DIFFER_THRESHOLD)

        # Render source PDF for centering check
        import fitz

        doc = fitz.open(str(source_pdf_path))
        page = doc[0]
        mat = fitz.Matrix(300 / 72, 300 / 72)
        pix = page.get_pixmap(matrix=mat)
        src_render = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w, pix.n)
        if pix.n == 4:
            src_render = src_render[:, :, :3]
        doc.close()
        src_render = normalize_render_shape(src_render, composite.shape)
        src_render = normalize_reference_jpeg(src_render)
        h = min(composite.shape[0], src_render.shape[0])
        w = min(composite.shape[1], src_render.shape[1])
        checks["centering"] = check_centering(composite[:h, :w], src_render[:h, :w])

        # SMask integrity (if output PDF provided)
        if output_pdf_path and Path(output_pdf_path).exists():
            checks["smask_integrity"] = check_smask_integrity(source_pdf_path, output_pdf_path)
            checks["frame_pixels"] = check_frame_pixels_preserved(source_pdf_path, output_pdf_path)
        else:
            checks["smask_integrity"] = {
                "pass": False,
                "message": "FAIL: Check 6 requires --output-pdf in PDF mode",
            }
            checks["frame_pixels"] = {
                "pass": False,
                "message": "FAIL: Check 7 requires --output-pdf in PDF mode",
            }

        # Check 8 - AI Art Border Detection (optional)
        if ai_art_path:
            checks["ai_art_border"] = check_ai_art_border(ai_art_path)
        else:
            print("  [!] ai_art_border: SKIPPED - no --ai-art path provided")

        # Check 9 - Visual frame comparison (required in upgraded PDF protocol)
        if output_pdf_path and Path(output_pdf_path).exists():
            checks["visual_frame"] = check_visual_frame(source_pdf_path, output_pdf_path)
        else:
            checks["visual_frame"] = {
                "pass": False,
                "message": "FAIL: Check 9 requires --output-pdf in PDF mode",
            }
    else:
        # JPG-mode checks
        source = load_image_array(source_jpg_path)
        checks["ornament_zone"] = check_ornament_zone_jpg(composite, source, ORNAMENT_MATCH_THRESHOLD)
        checks["art_zone"] = check_art_zone_jpg(composite, source, ART_DIFFER_THRESHOLD)
        checks["centering"] = check_centering(composite, source)

    # Transition quality (both modes)
    checks["transition_quality"] = check_transition_zone(composite)

    # Print results
    all_passed = all(c["pass"] for c in checks.values())
    for name, result in checks.items():
        icon = "+" if result["pass"] else "X"
        print(f"  [{icon}] {name}: {result.get('message', '')}")

    print(f"\n{'=' * 70}")
    if all_passed:
        print("  RESULT: ALL CHECKS PASSED - safe to commit")
    else:
        failed = [k for k, v in checks.items() if not v["pass"]]
        print(f"  RESULT: FAILED ({len(failed)} check(s): {', '.join(failed)})")
        print("  DO NOT COMMIT. Fix issues and re-run.")
    print(f"{'=' * 70}\n")

    return {"overall_pass": all_passed, "mode": mode, "checks": checks}


def main():
    parser = argparse.ArgumentParser(description="Verify composited cover output (single-file or Prompt-12 batch mode).")
    parser.add_argument("composite", type=Path, nargs="?", default=None, help="Path to composited output JPG")
    parser.add_argument("source_jpg", type=Path, nargs="?", default=None, help="Path to source cover JPG (JPG mode)")
    parser.add_argument("--source-pdf", type=Path, default=None, help="Path to source cover PDF (enables PDF mode)")
    parser.add_argument("--output-pdf", type=Path, default=None, help="Path to output PDF (for SMask integrity check)")
    parser.add_argument(
        "--ai-art",
        type=Path,
        default=None,
        help=(
            "Path to AI art image before compositing (enables Check 8 in PDF mode). "
            "If omitted, Check 8 is skipped with a warning."
        ),
    )
    parser.add_argument("--strict", action="store_true", help="Stricter thresholds")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run Prompt-12 catalog verification (no positional args required).",
    )
    parser.add_argument("--input-dir", type=Path, default=None, help="Input covers directory for --batch mode")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("Output Covers"),
        help="Composited covers directory for --batch mode",
    )
    parser.add_argument("--catalog-path", type=Path, default=None, help="Book catalog path for --batch mode")
    parser.add_argument(
        "--verify-dir",
        type=Path,
        default=Path("tmp/verification"),
        help="Output directory for per-cover comparison images in --batch mode",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path("tmp/verification_report.json"),
        help="JSON report path for --batch mode",
    )
    args = parser.parse_args()

    if args.batch or args.composite is None:
        code = verify_catalog_batch(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            catalog_path=args.catalog_path,
            verify_dir=args.verify_dir,
            report_path=args.report_path,
        )
        sys.exit(code)

    if not args.composite.exists():
        print(f"ERROR: Composite not found: {args.composite}", file=sys.stderr)
        sys.exit(2)

    if args.source_pdf and not args.source_pdf.exists():
        print(f"ERROR: Source PDF not found: {args.source_pdf}", file=sys.stderr)
        sys.exit(2)

    if args.output_pdf and not args.output_pdf.exists():
        print(f"ERROR: Output PDF not found: {args.output_pdf}", file=sys.stderr)
        sys.exit(2)

    if not args.source_pdf and args.source_jpg and not args.source_jpg.exists():
        print(f"ERROR: Source JPG not found: {args.source_jpg}", file=sys.stderr)
        sys.exit(2)

    if args.ai_art and not args.ai_art.exists():
        print(f"ERROR: AI art image not found: {args.ai_art}", file=sys.stderr)
        sys.exit(2)

    if not args.source_pdf and not args.source_jpg:
        print("ERROR: Must provide either --source-pdf or a source JPG path", file=sys.stderr)
        sys.exit(2)

    result = verify_composite(
        args.composite,
        source_jpg_path=args.source_jpg,
        source_pdf_path=args.source_pdf,
        output_pdf_path=args.output_pdf,
        ai_art_path=args.ai_art,
        strict=args.strict,
    )

    if args.json:

        def sanitize(obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj

        clean = json.loads(json.dumps(result, default=sanitize))
        print(json.dumps(clean, indent=2))

    sys.exit(0 if result["overall_pass"] else 1)


if __name__ == "__main__":
    main()
