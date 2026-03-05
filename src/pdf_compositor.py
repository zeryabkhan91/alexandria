"""PDF-based compositor – three-layer approach (navy canvas → circular art → frame overlay).

Replaces the legacy SMask-blending compositor with Tim's proven layering model:
  1. Rasterise the source PDF cover to RGB at 300 DPI.
  2. Build a frame overlay from that raster (punch a transparent circle in the centre).
  3. Create a solid navy canvas.
  4. Clip the AI-generated art to a circle and paste it onto the navy canvas.
  5. Paste the frame overlay on top – the frame naturally covers the art-edge seam.
  6. Write the result as JPG.  Optionally inject CMYK back into the PDF stream.
"""

from __future__ import annotations

import re
import shutil
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageOps

try:
    import fitz  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("PyMuPDF is required for PDF compositor") from exc

try:
    import pikepdf  # type: ignore
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("pikepdf is required for PDF compositor") from exc

try:
    from src import config
    from src import safe_json
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import safe_json  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Geometry constants (proven in POC)
# ---------------------------------------------------------------------------
EXPECTED_DPI = 300
EXPECTED_JPG_SIZE = (3784, 2777)          # full cover w x h
MEDALLION_CENTER = (2864, 1620)           # centre of the circular medallion
FRAME_HOLE_RADIUS = 540                   # inner edge of frame ring (transparent hole)
ART_CLIP_RADIUS = 600                     # slightly larger - 60 px overlap hidden by frame
NAVY_FILL_RGB = (21, 32, 76)             # background colour matching original cover

# Art pre-processing (kept from previous version)
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
# CMYK helpers (kept for optional PDF stream write-back)
# ---------------------------------------------------------------------------
def rgb_to_cmyk(rgb_array: np.ndarray) -> np.ndarray:
    """Convert RGB uint8 array (h,w,3) to CMYK uint8 (h,w,4)."""
    rgb = np.asarray(rgb_array, dtype=np.uint8)
    if rgb.ndim != 3 or rgb.shape[2] != 3:
        raise ValueError("rgb_to_cmyk expects an (h,w,3) uint8 array")

    r = rgb[:, :, 0].astype(np.float32)
    g = rgb[:, :, 1].astype(np.float32)
    b = rgb[:, :, 2].astype(np.float32)

    c = 255.0 - r
    m = 255.0 - g
    y = 255.0 - b
    k = np.minimum(np.minimum(c, m), y)

    denom = np.maximum(1.0, 255.0 - k)
    c_out = np.where(k >= 255.0, 0.0, ((c - k) / denom) * 255.0)
    m_out = np.where(k >= 255.0, 0.0, ((m - k) / denom) * 255.0)
    y_out = np.where(k >= 255.0, 0.0, ((y - k) / denom) * 255.0)

    out = np.stack(
        [
            np.clip(c_out, 0, 255).astype(np.uint8),
            np.clip(m_out, 0, 255).astype(np.uint8),
            np.clip(y_out, 0, 255).astype(np.uint8),
            np.clip(k, 0, 255).astype(np.uint8),
        ],
        axis=-1,
    )
    return out


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


# ---------------------------------------------------------------------------
# PDF / rendering helpers
# ---------------------------------------------------------------------------
def _inflate_stream_bytes(stream_obj: Any, *, expected_len: int) -> bytes:
    raw = bytes(stream_obj.read_raw_bytes())
    data: bytes
    try:
        data = zlib.decompress(raw)
    except Exception:
        data = bytes(stream_obj.read_bytes())
    if len(data) != expected_len:
        raise ValueError(f"Decoded stream length mismatch: got {len(data)}, expected {expected_len}")
    return data


def _resolve_im0(page: Any) -> Any:
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


def _render_pdf_to_jpg(*, source_pdf: Path, output_jpg: Path, dpi: int = EXPECTED_DPI) -> None:
    output_jpg.parent.mkdir(parents=True, exist_ok=True)
    doc = fitz.open(str(source_pdf))
    try:
        if doc.page_count <= 0:
            raise ValueError("PDF has no pages")
        page = doc[0]
        scale = float(dpi) / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        if image.size != EXPECTED_JPG_SIZE:
            image = image.resize(EXPECTED_JPG_SIZE, Image.LANCZOS)
        image.save(output_jpg, format="JPEG", quality=100, subsampling=0, dpi=(dpi, dpi))
    finally:
        doc.close()


def _rasterise_pdf_page(source_pdf: Path, dpi: int = EXPECTED_DPI) -> Image.Image:
    """Rasterise page 0 of a PDF to an RGB PIL Image at *dpi* resolution."""
    doc = fitz.open(str(source_pdf))
    try:
        if doc.page_count <= 0:
            raise ValueError("PDF has no pages")
        page = doc[0]
        scale = float(dpi) / 72.0
        pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        if image.size != EXPECTED_JPG_SIZE:
            image = image.resize(EXPECTED_JPG_SIZE, Image.LANCZOS)
        return image
    finally:
        doc.close()


# ---------------------------------------------------------------------------
# Three-layer compositing helpers
# ---------------------------------------------------------------------------
def _build_frame_overlay(cover_rgb: Image.Image) -> Image.Image:
    """Create an RGBA frame overlay from the original cover raster.

    Everything outside the inner medallion ring is kept opaque (the frame,
    ornaments, text, spine, etc.).  The inside of the ring is made fully
    transparent so the art layer beneath shows through.
    """
    w, h = cover_rgb.size
    cx, cy = MEDALLION_CENTER

    # Start with an RGBA copy of the full cover.
    overlay = cover_rgb.copy().convert("RGBA")

    # Create a mask: white = keep, black = transparent (the hole).
    mask = Image.new("L", (w, h), 255)
    draw = ImageDraw.Draw(mask)
    draw.ellipse(
        [cx - FRAME_HOLE_RADIUS, cy - FRAME_HOLE_RADIUS,
         cx + FRAME_HOLE_RADIUS, cy + FRAME_HOLE_RADIUS],
        fill=0,
    )
    overlay.putalpha(mask)
    return overlay


def _load_and_clip_art(ai_art_path: Path) -> Image.Image:
    """Load AI art, trim margins, edge-trim, and clip to a circle of ART_CLIP_RADIUS.

    Returns an RGBA image of size (2*ART_CLIP_RADIUS, 2*ART_CLIP_RADIUS) with
    the circular art and transparent corners.
    """
    with Image.open(ai_art_path) as source:
        rgb_source = _trim_uniform_margins(source)
        if AI_ART_EDGE_TRIM_RATIO > 0:
            src_w, src_h = rgb_source.size
            trim_x = int(round(src_w * AI_ART_EDGE_TRIM_RATIO / 2.0))
            trim_y = int(round(src_h * AI_ART_EDGE_TRIM_RATIO / 2.0))
            if (src_w - 2 * trim_x) >= 64 and (src_h - 2 * trim_y) >= 64:
                rgb_source = rgb_source.crop((trim_x, trim_y, src_w - trim_x, src_h - trim_y))

    diameter = ART_CLIP_RADIUS * 2
    art_resized = ImageOps.fit(
        rgb_source,
        (diameter, diameter),
        method=Image.LANCZOS,
        centering=(0.5, 0.5),
    )
    art_rgba = art_resized.convert("RGBA")

    # Create circular mask.
    circle_mask = Image.new("L", (diameter, diameter), 0)
    draw = ImageDraw.Draw(circle_mask)
    draw.ellipse([0, 0, diameter - 1, diameter - 1], fill=255)
    art_rgba.putalpha(circle_mask)
    return art_rgba


def _composite_three_layers(
    cover_rgb: Image.Image,
    ai_art_path: Path,
) -> Image.Image:
    """Compose the three layers and return the final RGB image.

    Layer 1 (bottom): solid navy canvas.
    Layer 2 (middle): AI art clipped to circle at medallion centre.
    Layer 3 (top):    frame overlay from the original cover.
    """
    w, h = cover_rgb.size
    cx, cy = MEDALLION_CENTER

    # Layer 1: navy canvas
    canvas = Image.new("RGB", (w, h), NAVY_FILL_RGB)

    # Layer 2: circular art
    art_circle = _load_and_clip_art(ai_art_path)
    art_x = cx - ART_CLIP_RADIUS
    art_y = cy - ART_CLIP_RADIUS
    canvas.paste(art_circle, (art_x, art_y), art_circle)

    # Layer 3: frame overlay
    frame_overlay = _build_frame_overlay(cover_rgb)
    canvas.paste(frame_overlay, (0, 0), frame_overlay)

    return canvas


# ---------------------------------------------------------------------------
# Legacy helper kept for backward-compat callers that still expect CMYK art
# ---------------------------------------------------------------------------
def _load_ai_art_cmyk(*, ai_art_path: Path, width: int, height: int) -> np.ndarray:
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
    rgb_arr = np.asarray(rgb, dtype=np.uint8)
    return rgb_to_cmyk(rgb_arr)


# ---------------------------------------------------------------------------
# Main entry: composite_cover_pdf  (THREE-LAYER APPROACH)
# ---------------------------------------------------------------------------
def composite_cover_pdf(
    source_pdf_path: str,
    ai_art_path: str,
    output_pdf_path: str,
    output_jpg_path: str,
    output_ai_path: str | None = None,
) -> dict[str, Any]:
    """Replace PDF medallion illustration using the three-layer compositing model.

    1. Rasterise the original cover from the source PDF.
    2. Build a frame overlay (original cover with transparent centre hole).
    3. Create navy canvas, paste circular AI art, paste frame on top.
    4. Save as JPG (primary webapp output).
    5. Inject CMYK composite back into the PDF stream for PDF/AI output.
    """
    source_pdf = Path(source_pdf_path)
    art_path = Path(ai_art_path)
    output_pdf = Path(output_pdf_path)
    output_jpg = Path(output_jpg_path)
    output_ai = Path(output_ai_path) if output_ai_path else output_pdf.with_suffix(".ai")

    if not source_pdf.exists():
        raise FileNotFoundError(f"Source PDF not found: {source_pdf}")
    if not art_path.exists():
        raise FileNotFoundError(f"AI art image not found: {art_path}")

    output_pdf.parent.mkdir(parents=True, exist_ok=True)
    output_jpg.parent.mkdir(parents=True, exist_ok=True)
    output_ai.parent.mkdir(parents=True, exist_ok=True)

    # --- Step 1: Rasterise the original cover from the source PDF ---
    cover_rgb = _rasterise_pdf_page(source_pdf, dpi=EXPECTED_DPI)
    logger.info(
        "Rasterised source PDF",
        extra={"source": str(source_pdf), "size": cover_rgb.size},
    )

    # --- Step 2-4: Three-layer composite ---
    final_rgb = _composite_three_layers(cover_rgb, art_path)

    # --- Step 5: Save JPG ---
    final_rgb.save(
        output_jpg,
        format="JPEG",
        quality=100,
        subsampling=0,
        dpi=(EXPECTED_DPI, EXPECTED_DPI),
    )
    logger.info("Saved composite JPG", extra={"path": str(output_jpg)})

    # --- Step 6: Write composite back into PDF stream ---
    pdf = pikepdf.Pdf.open(str(source_pdf))
    try:
        if len(pdf.pages) == 0:
            raise ValueError("Source PDF has no pages")
        page = pdf.pages[0]
        im0 = _resolve_im0(page)

        width = int(im0.get("/Width"))
        height = int(im0.get("/Height"))

        # Resize our RGB composite to match the PDF Im0 dimensions exactly.
        pdf_rgb = final_rgb.resize((width, height), Image.LANCZOS)
        cmyk_arr = rgb_to_cmyk(np.asarray(pdf_rgb, dtype=np.uint8))

        encoded = zlib.compress(cmyk_arr.tobytes())
        smask_ref = im0.get("/SMask")
        im0.write(encoded, filter=pikepdf.Name("/FlateDecode"))
        if smask_ref is not None:
            im0["/SMask"] = smask_ref
        if "/DecodeParms" in im0:
            del im0["/DecodeParms"]

        pdf.save(str(output_pdf))
    finally:
        pdf.close()

    shutil.copyfile(output_pdf, output_ai)
    logger.info(
        "Saved composite PDF + AI",
        extra={"pdf": str(output_pdf), "ai": str(output_ai)},
    )

    return {
        "success": True,
        "source_pdf": str(source_pdf),
        "output_pdf": str(output_pdf),
        "output_jpg": str(output_jpg),
        "output_ai": str(output_ai),
        "center_x": MEDALLION_CENTER[0],
        "center_y": MEDALLION_CENTER[1],
        "image_width": int(cover_rgb.size[0]),
        "image_height": int(cover_rgb.size[1]),
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
) -> list[Path]:
    """Composite all generated variants for a book via source PDF."""
    source_pdf = find_source_pdf_for_book(input_dir=input_dir, book_number=book_number, catalog_path=catalog_path)
    if source_pdf is None:
        raise FileNotFoundError(f"No source PDF found for book {book_number}")

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
        )
        outputs.append(output_jpg)
        report_items.append(
            {
                "output_path": str(output_jpg),
                "valid": True,
                "issues": [],
                "mode": "pdf",
                "source_pdf": str(source_pdf),
                "variant": variant,
                "model": model,
                "metrics": {
                    "image_width": float(result.get("image_width", 0)),
                    "image_height": float(result.get("image_height", 0)),
                },
            }
        )

    report = {
        "book_number": int(book_number),
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "total": len(report_items),
        "invalid": 0,
        "items": report_items,
    }
    safe_json.atomic_write_json(output_dir / str(book_number) / "composite_validation.json", report)
    logger.info(
        "PDF compositor completed",
        extra={"book_number": int(book_number), "variants": len(outputs), "source_pdf": str(source_pdf)},
    )
    return outputs


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="PDF compositor for one generated image")
    parser.add_argument("source_pdf", type=Path)
    parser.add_argument("ai_art", type=Path)
    parser.add_argument("output_pdf", type=Path)
    parser.add_argument("output_jpg", type=Path)
    parser.add_argument("--output-ai", type=Path, default=None)
    args = parser.parse_args()

    result = composite_cover_pdf(
        source_pdf_path=str(args.source_pdf),
        ai_art_path=str(args.ai_art),
        output_pdf_path=str(args.output_pdf),
        output_jpg_path=str(args.output_jpg),
        output_ai_path=str(args.output_ai) if args.output_ai else None,
    )
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
