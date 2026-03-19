"""Im0 layer-swap compositor for medallion covers.

This module replaces only the center art inside the source PDF's ``/Im0``
image XObject while preserving the ornamental frame and the original ``/SMask``.
The modified PDF is then rendered to the final composite JPG.

It is intentionally not the correct compositor for the replacement-frame-only
workflow, because that workflow requires deleting the legacy source medallion
and showing only ``Untitled__4_frame.png`` in the final result.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import zlib
from pathlib import Path

import numpy as np
import pikepdf
from PIL import Image, ImageOps

try:
    from src import frame_geometry
    from src import protrusion_overlay
except ModuleNotFoundError:  # pragma: no cover
    import frame_geometry  # type: ignore
    import protrusion_overlay  # type: ignore

logger = logging.getLogger(__name__)

DEFAULT_BLEND_RADIUS = 844
DEFAULT_FEATHER_PX = 20
DEFAULT_BORDER_TRIM_RATIO = 0.05
JPEG_QUALITY = 100
RENDER_DPI = 300


def composite_via_pdf_swap(
    *,
    source_pdf_path: Path,
    ai_art_path: Path,
    output_jpg_path: Path,
    blend_radius: int | None = None,
    feather_px: int = DEFAULT_FEATHER_PX,
    render_dpi: int = RENDER_DPI,
    border_trim_ratio: float = DEFAULT_BORDER_TRIM_RATIO,
    expected_output_size: tuple[int, int] | None = None,
    overlay_center: tuple[int, int] | None = None,
) -> Path:
    """Swap AI art into ``/Im0`` and render the modified PDF to JPG.

    A companion PDF is written beside ``output_jpg_path`` using the same stem.
    """

    source_pdf_path = Path(source_pdf_path)
    ai_art_path = Path(ai_art_path)
    output_jpg_path = Path(output_jpg_path)
    output_pdf_path = output_jpg_path.with_suffix(".pdf")

    if not source_pdf_path.exists():
        raise FileNotFoundError(f"Source PDF not found: {source_pdf_path}")
    if not ai_art_path.exists():
        raise FileNotFoundError(f"AI art not found: {ai_art_path}")

    logger.info(
        "PDF swap start: source_pdf=%s ai_art=%s output_jpg=%s requested_outer_radius=%s feather_px=%d render_dpi=%d expected_output_size=%s",
        source_pdf_path,
        ai_art_path,
        output_jpg_path,
        "auto" if blend_radius is None else int(blend_radius),
        int(feather_px),
        int(render_dpi),
        expected_output_size,
    )

    with pikepdf.Pdf.open(str(source_pdf_path)) as pdf:
        page = pdf.pages[0]
        im0_obj = _resolve_im0(page)
        smask_obj = im0_obj.get("/SMask")
        if smask_obj is None:
            raise ValueError(f"{source_pdf_path.name} /Im0 has no /SMask")

        original_image = pikepdf.PdfImage(im0_obj).as_pil_image()
        width, height = original_image.size
        mode = original_image.mode
        bands = len(original_image.getbands())
        if bands not in (3, 4):
            raise ValueError(f"Unsupported /Im0 mode: {mode}")

        decoded = bytes(im0_obj.read_bytes())
        expected_len = width * height * bands
        if len(decoded) != expected_len:
            raise ValueError(
                f"Decoded /Im0 length mismatch: got {len(decoded)}, expected {expected_len}"
            )
        original_arr = np.frombuffer(decoded, dtype=np.uint8).reshape(height, width, bands).copy()

        smask_pil = pikepdf.PdfImage(smask_obj).as_pil_image().convert("L")
        smask_arr = np.array(smask_pil, dtype=np.uint8)
        if smask_arr.shape != (height, width):
            raise ValueError(
                f"Decoded /SMask shape mismatch: got {smask_arr.shape}, expected {(height, width)}"
            )
        logger.info(
            "PDF swap source geometry: source_pdf=%s im0_size=%dx%d mode=%s bands=%d smask_shape=%s",
            source_pdf_path.name,
            int(width),
            int(height),
            mode,
            int(bands),
            tuple(int(v) for v in smask_arr.shape),
        )

        fitted_art = _load_ai_art(
            ai_art_path=ai_art_path,
            size=(width, height),
            mode=mode,
            border_trim_ratio=border_trim_ratio,
        )
        art_arr = np.array(fitted_art, dtype=np.uint8)
        if art_arr.ndim == 2:
            art_arr = art_arr[:, :, np.newaxis]
        if art_arr.shape != original_arr.shape:
            raise ValueError(
                f"AI art shape mismatch: got {art_arr.shape}, expected {original_arr.shape}"
            )

        safe_outer_radius = detect_blend_radius_from_smask(smask_arr)
        target_inner_radius, target_outer_radius = _resolve_target_radii(
            source_pdf_path=source_pdf_path,
            smask_arr=smask_arr,
            expected_output_size=expected_output_size,
            requested_outer_radius=(int(blend_radius) if blend_radius is not None else None),
            feather_px=feather_px,
        )
        art_mask = _build_art_mask(
            width=width,
            height=height,
            inner_radius=target_inner_radius,
            outer_radius=target_outer_radius,
        )

        blended = original_arr.copy()
        mix = art_mask[:, :, np.newaxis]
        blended_float = (art_arr.astype(np.float32) * mix) + (original_arr.astype(np.float32) * (1.0 - mix))
        blended[:] = np.clip(blended_float, 0.0, 255.0).astype(np.uint8)

        if np.any(art_mask <= 0.0):
            preserve = art_mask <= 0.0
            blended[preserve] = original_arr[preserve]

        _write_im0_stream(
            pdf=pdf,
            im0_obj=im0_obj,
            image_bytes=blended.tobytes(),
            width=width,
            height=height,
            bands=bands,
        )

        output_pdf_path.parent.mkdir(parents=True, exist_ok=True)
        pdf.save(str(output_pdf_path))

    _render_pdf_to_jpg(
        source_pdf_path=output_pdf_path,
        output_jpg_path=output_jpg_path,
        render_dpi=render_dpi,
        expected_output_size=expected_output_size,
    )
    with Image.open(output_jpg_path) as rendered:
        rendered_rgb = rendered.convert("RGB")
    if overlay_center is None and frame_geometry.is_standard_medallion_cover(rendered_rgb.size):
        geometry = frame_geometry.resolve_standard_medallion_geometry(rendered_rgb.size)
        overlay_center = (int(geometry.center_x), int(geometry.center_y))
    if overlay_center is not None:
        rendered_rgb, overlay_details = protrusion_overlay.apply_shared_protrusion_overlay(
            image=rendered_rgb,
            center_x=int(overlay_center[0]),
            center_y=int(overlay_center[1]),
            cover_size=rendered_rgb.size,
        )
        rendered_rgb.save(
            output_jpg_path,
            format="JPEG",
            quality=JPEG_QUALITY,
            subsampling=0,
            dpi=(render_dpi, render_dpi),
        )
        logger.info(
            "PDF swap protrusion overlay: source=%s applied=%s reason=%s overlay_size=%dx%d paste=(%d,%d) requested_center=(%d,%d) applied_center=(%d,%d) components=%s",
            source_pdf_path.name,
            "yes" if overlay_details.get("applied") else "no",
            str(overlay_details.get("reason", "")),
            int(overlay_details.get("overlay_width", 0)),
            int(overlay_details.get("overlay_height", 0)),
            int(overlay_details.get("paste_x", 0)),
            int(overlay_details.get("paste_y", 0)),
            int(overlay_center[0]),
            int(overlay_center[1]),
            int(overlay_details.get("applied_center_x", overlay_center[0])),
            int(overlay_details.get("applied_center_y", overlay_center[1])),
            overlay_details.get("components", []),
        )
    logger.info(
        "PDF swap composite complete: source=%s output=%s safe_radius=%d hole_radius=%d art_radius=%d",
        source_pdf_path.name,
        output_jpg_path,
        safe_outer_radius,
        target_inner_radius,
        target_outer_radius,
    )
    return output_jpg_path


def detect_blend_radius_from_smask(smask_arr: np.ndarray) -> int:
    """Return the safe art radius where frame ornaments have not yet begun."""

    if smask_arr.ndim != 2:
        raise ValueError("SMask array must be 2D")
    height, width = smask_arr.shape
    center_x = (width - 1) / 2.0
    center_y = (height - 1) / 2.0
    max_radius = int(min(center_x, center_y))
    if max_radius <= 0:
        return DEFAULT_BLEND_RADIUS

    directional_limits: list[int] = []
    for angle in np.linspace(0.0, np.pi * 2.0, 96, endpoint=False):
        cos_a = float(np.cos(angle))
        sin_a = float(np.sin(angle))
        last_opaque = 0
        for radius in range(1, max_radius + 1):
            px = int(np.clip(round(center_x + (cos_a * radius)), 0, width - 1))
            py = int(np.clip(round(center_y + (sin_a * radius)), 0, height - 1))
            if int(smask_arr[py, px]) >= 250:
                last_opaque = radius
                continue
            break
        directional_limits.append(last_opaque)

    candidate = int(np.percentile(np.array(directional_limits, dtype=np.float32), 10))
    if candidate <= 0:
        return DEFAULT_BLEND_RADIUS
    return max(20, int(candidate - 16))


def _build_art_mask(*, width: int, height: int, inner_radius: int, outer_radius: int) -> np.ndarray:
    center_x = (width - 1) / 2.0
    center_y = (height - 1) / 2.0
    inner_radius = max(0.0, float(inner_radius))
    outer_radius = max(inner_radius, float(outer_radius))

    yy, xx = np.ogrid[:height, :width]
    dist = np.sqrt((xx - center_x) ** 2 + (yy - center_y) ** 2)
    mask = np.zeros((height, width), dtype=np.float32)
    mask[dist <= inner_radius] = 1.0

    transition = (dist > inner_radius) & (dist < float(outer_radius))
    if np.any(transition) and outer_radius > inner_radius:
        span = float(outer_radius) - inner_radius
        mask[transition] = 1.0 - ((dist[transition] - inner_radius) / span)

    return np.clip(mask, 0.0, 1.0)


def _resolve_target_radii(
    *,
    source_pdf_path: Path,
    smask_arr: np.ndarray,
    expected_output_size: tuple[int, int] | None,
    requested_outer_radius: int | None,
    feather_px: int,
) -> tuple[int, int]:
    if requested_outer_radius is not None:
        outer = max(20, int(requested_outer_radius))
        inner = max(20, int(outer - max(0, feather_px)))
        logger.info(
            "PDF swap radii: source=%s mode=requested requested_outer=%d feather_px=%d hole_radius=%d art_radius=%d",
            source_pdf_path.name,
            int(requested_outer_radius),
            int(feather_px),
            int(inner),
            int(outer),
        )
        return inner, outer

    if expected_output_size and frame_geometry.is_standard_medallion_cover(expected_output_size):
        try:
            try:
                from src import pdf_compositor as jpg_pdf_compositor
            except ModuleNotFoundError:  # pragma: no cover
                import pdf_compositor as jpg_pdf_compositor  # type: ignore

            transform = jpg_pdf_compositor._extract_im0_transform(source_pdf_path)
            mapping = jpg_pdf_compositor._im0_to_jpg_mapping(
                transform,
                int(expected_output_size[0]),
                int(expected_output_size[1]),
            )
            inner, outer = frame_geometry.template_geometry_to_im0(mapping, expected_output_size)
            effective_radius = int(inner)
            logger.info(
                "PDF swap radii: source=%s mode=template_mapping expected_output_size=%s im0_center=(%.2f,%.2f) jpg_scale=%.6f hole_radius=%d art_radius=%d",
                source_pdf_path.name,
                expected_output_size,
                float(mapping["im0_cx"]),
                float(mapping["im0_cy"]),
                float(frame_geometry.average_jpg_scale(mapping)),
                effective_radius,
                effective_radius,
            )
            return effective_radius, effective_radius
        except Exception as exc:
            logger.warning("Template radius mapping failed for %s: %s", source_pdf_path.name, exc)

    outer = detect_blend_radius_from_smask(smask_arr)
    inner = max(20, int(outer - max(0, feather_px)))
    logger.info(
        "PDF swap radii: source=%s mode=smask_detected safe_outer_radius=%d feather_px=%d hole_radius=%d art_radius=%d",
        source_pdf_path.name,
        int(outer),
        int(feather_px),
        int(inner),
        int(outer),
    )
    return inner, outer


def _load_ai_art(
    *,
    ai_art_path: Path,
    size: tuple[int, int],
    mode: str,
    border_trim_ratio: float,
) -> Image.Image:
    with Image.open(ai_art_path) as source:
        prepared = _strip_border(source.convert("RGB"), border_trim_ratio=border_trim_ratio)
        fitted = ImageOps.fit(
            prepared,
            size,
            method=Image.LANCZOS,
            centering=(0.5, 0.5),
        )
        if fitted.mode != mode:
            fitted = fitted.convert(mode)
        logger.info(
            "PDF swap art fit: ai_art=%s source=%dx%d prepared=%dx%d target=%dx%d centering=(%.4f,%.4f) focus=(%.4f,%.4f) confidence=%.6f",
            ai_art_path.name,
            int(source.size[0]),
            int(source.size[1]),
            int(prepared.size[0]),
            int(prepared.size[1]),
            int(size[0]),
            int(size[1]),
            0.5,
            0.5,
            0.5,
            0.5,
            0.0,
        )
        return fitted


def _strip_border(image: Image.Image, *, border_trim_ratio: float) -> Image.Image:
    ratio = max(0.0, min(0.35, float(border_trim_ratio)))
    if ratio <= 0.0:
        return image
    width, height = image.size
    trim_x = int(round(width * ratio / 2.0))
    trim_y = int(round(height * ratio / 2.0))
    if width - (trim_x * 2) < 32 or height - (trim_y * 2) < 32:
        return image
    return image.crop((trim_x, trim_y, width - trim_x, height - trim_y))


def _resolve_im0(page: pikepdf.Page) -> pikepdf.Object:
    resources = page.get("/Resources")
    if resources is None:
        raise ValueError("PDF page has no /Resources")
    xobjects = resources.get("/XObject")
    if xobjects is None:
        raise ValueError("PDF page has no /XObject resources")

    im0_obj = xobjects.get("/Im0")
    if im0_obj is None:
        raise ValueError("PDF page has no /Im0 image XObject")
    return im0_obj


def _write_im0_stream(
    *,
    pdf: pikepdf.Pdf,
    im0_obj: pikepdf.Object,
    image_bytes: bytes,
    width: int,
    height: int,
    bands: int,
) -> None:
    colorspace = im0_obj.get("/ColorSpace")
    if colorspace is None:
        if bands == 4:
            colorspace = pikepdf.Name("/DeviceCMYK")
        elif bands == 3:
            colorspace = pikepdf.Name("/DeviceRGB")
        else:
            colorspace = pikepdf.Name("/DeviceGray")

    smask_ref = im0_obj.get("/SMask")
    encoded = zlib.compress(image_bytes)

    im0_obj.write(encoded, filter=pikepdf.Name("/FlateDecode"), type_check=False)
    im0_obj["/Type"] = pikepdf.Name("/XObject")
    im0_obj["/Subtype"] = pikepdf.Name("/Image")
    im0_obj["/Width"] = int(width)
    im0_obj["/Height"] = int(height)
    im0_obj["/ColorSpace"] = colorspace
    im0_obj["/BitsPerComponent"] = int(im0_obj.get("/BitsPerComponent", 8))
    im0_obj["/Filter"] = pikepdf.Name("/FlateDecode")
    if smask_ref is not None:
        im0_obj["/SMask"] = smask_ref
    if "/DecodeParms" in im0_obj:
        del im0_obj["/DecodeParms"]


def _render_pdf_to_jpg(
    *,
    source_pdf_path: Path,
    output_jpg_path: Path,
    render_dpi: int,
    expected_output_size: tuple[int, int] | None,
) -> None:
    output_jpg_path.parent.mkdir(parents=True, exist_ok=True)
    pdftoppm = shutil.which("pdftoppm")
    if pdftoppm:
        stem = str(output_jpg_path.with_suffix(""))
        result = subprocess.run(
            [
                pdftoppm,
                "-jpeg",
                "-jpegopt",
                f"quality={JPEG_QUALITY},progressive=n,optimize=n",
                "-r",
                str(int(render_dpi)),
                "-singlefile",
                str(source_pdf_path),
                stem,
            ],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"pdftoppm failed: {result.stderr.strip() or result.stdout.strip()}")
        if not output_jpg_path.exists():
            raise FileNotFoundError(f"Rendered JPG not found: {output_jpg_path}")
        with Image.open(output_jpg_path) as rendered:
            rendered_rgb = rendered.convert("RGB")
            if expected_output_size and rendered_rgb.size != expected_output_size:
                rendered_rgb = rendered_rgb.resize(expected_output_size, Image.LANCZOS)
            rendered_rgb.save(
                output_jpg_path,
                format="JPEG",
                quality=JPEG_QUALITY,
                subsampling=0,
                dpi=(render_dpi, render_dpi),
            )
        return

    logger.warning("pdftoppm not available; falling back to PyMuPDF render")
    try:
        import fitz  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pdftoppm is unavailable and PyMuPDF is not installed") from exc

    doc = fitz.open(str(source_pdf_path))
    try:
        if doc.page_count <= 0:
            raise ValueError("PDF has no pages")
        scale = float(render_dpi) / 72.0
        pix = doc[0].get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
        image = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
        if expected_output_size and image.size != expected_output_size:
            image = image.resize(expected_output_size, Image.LANCZOS)
        image.save(
            output_jpg_path,
            format="JPEG",
            quality=JPEG_QUALITY,
            subsampling=0,
            dpi=(render_dpi, render_dpi),
        )
    finally:
        doc.close()
