#!/usr/bin/env python3
"""Re-run a single cover composite with existing AI art and emit review artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import visual_qa  # noqa: E402
from src import config  # noqa: E402
from src import cover_compositor  # noqa: E402
from src import frame_geometry  # noqa: E402
from src import pdf_compositor  # noqa: E402
from src import safe_json  # noqa: E402


def _load_regions(runtime: config.Config) -> dict[str, Any]:
    payload = safe_json.load_json(config.cover_regions_path(catalog_id=runtime.catalog_id, config_dir=runtime.config_dir), {})
    return payload if isinstance(payload, dict) else {}


def _resolve_region(runtime: config.Config, book_number: int) -> dict[str, Any]:
    regions = _load_regions(runtime)
    return cover_compositor._region_for_book(regions, book_number)  # type: ignore[attr-defined]


def _resolve_source_assets(runtime: config.Config, book_number: int) -> tuple[Path, Path | None]:
    source_jpg = pdf_compositor.find_source_jpg_for_book(
        input_dir=runtime.input_dir,
        book_number=book_number,
        catalog_path=runtime.book_catalog_path,
    )
    if source_jpg is None or not source_jpg.exists():
        raise FileNotFoundError(f"No source JPG found for book {book_number}")
    source_pdf = pdf_compositor.find_source_pdf_for_book(
        input_dir=runtime.input_dir,
        book_number=book_number,
        catalog_path=runtime.book_catalog_path,
    )
    return source_jpg, source_pdf


def _cover_title(runtime: config.Config, book_number: int) -> str:
    catalog = safe_json.load_json(runtime.book_catalog_path, [])
    for row in catalog if isinstance(catalog, list) else []:
        if not isinstance(row, dict):
            continue
        if int(row.get("number", 0) or 0) == int(book_number):
            return str(row.get("title", f"Book {book_number}"))
    return f"Book {book_number}"


def _medallion_geometry(size: tuple[int, int], region: dict[str, Any]) -> tuple[int, int, int]:
    if frame_geometry.is_standard_medallion_cover(size):
        template = frame_geometry.resolve_standard_medallion_geometry(size)
        center_x = int(region.get("center_x", 0) or template.center_x)
        center_y = int(region.get("center_y", 0) or template.center_y)
        frame_bbox = region.get("frame_bbox")
        if isinstance(frame_bbox, (list, tuple)) and len(frame_bbox) == 4:
            x1, y1, x2, y2 = [int(v) for v in frame_bbox]
            radius = int(max(abs(x1 - center_x), abs(x2 - center_x), abs(y1 - center_y), abs(y2 - center_y)))
        else:
            radius = int(template.art_clip_radius + 110)
        return center_x, center_y, max(40, radius)
    center_x = int(region.get("center_x", 0) or (size[0] // 2))
    center_y = int(region.get("center_y", 0) or (size[1] // 2))
    radius = int(region.get("radius", 0) or min(size) // 5)
    return center_x, center_y, max(40, radius)


def _crop_box(size: tuple[int, int], center_x: int, center_y: int, radius: int, padding: int = 48) -> tuple[int, int, int, int]:
    half = max(120, int(radius + padding))
    left = max(0, int(center_x - half))
    top = max(0, int(center_y - half))
    right = min(int(size[0]), int(center_x + half))
    bottom = min(int(size[1]), int(center_y + half))
    return left, top, right, bottom


def _annotate(image: Image.Image, label: str) -> Image.Image:
    out = image.convert("RGB").copy()
    draw = ImageDraw.Draw(out)
    draw.rectangle((0, 0, out.width, 36), fill=(20, 20, 20))
    draw.text((12, 9), label, fill=(245, 245, 245))
    return out


def _save_compare_row(images: list[tuple[str, Image.Image]], output_path: Path) -> None:
    prepared = [_annotate(image, label) for label, image in images]
    width = sum(image.width for image in prepared)
    height = max(image.height for image in prepared)
    canvas = Image.new("RGB", (width, height), (14, 18, 28))
    offset_x = 0
    for image in prepared:
        canvas.paste(image, (offset_x, 0))
        offset_x += image.width
    output_path.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output_path, format="JPEG", quality=95)


def _draw_box(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], *, outline: tuple[int, int, int], width: int = 4) -> None:
    draw.rectangle(box, outline=outline, width=width)


def _save_anchor_debug(
    *,
    source_image: Image.Image,
    final_image: Image.Image,
    crop_box: tuple[int, int, int, int],
    replacement_frame: dict[str, Any],
    output_path: Path,
) -> None:
    source_box = replacement_frame.get("source_anchor_box", [])
    final_box = replacement_frame.get("overlay_anchor_box_scaled", [])
    if not isinstance(source_box, (list, tuple)) or len(source_box) != 4:
        return
    if not isinstance(final_box, (list, tuple)) or len(final_box) != 4:
        return

    def _shift(box: list[int] | tuple[int, ...]) -> tuple[int, int, int, int]:
        left, top = int(crop_box[0]), int(crop_box[1])
        return (
            int(box[0]) - left,
            int(box[1]) - top,
            int(box[2]) - left,
            int(box[3]) - top,
        )

    source_crop = source_image.convert("RGB").crop(crop_box)
    final_crop = final_image.convert("RGB").crop(crop_box)
    source_annotated = source_crop.copy()
    final_annotated = final_crop.copy()
    source_draw = ImageDraw.Draw(source_annotated)
    final_draw = ImageDraw.Draw(final_annotated)
    _draw_box(source_draw, _shift(source_box), outline=(255, 200, 60))
    _draw_box(final_draw, _shift(final_box), outline=(255, 90, 90))
    _save_compare_row(
        [("Source Anchors", source_annotated), ("Final Anchors", final_annotated)],
        output_path,
    )


def _load_validation_json(path: Path) -> dict[str, Any]:
    payload = safe_json.load_json(path, {})
    return payload if isinstance(payload, dict) else {}


def run_once(
    *,
    runtime: config.Config,
    book_number: int,
    ai_art_path: Path,
    out_dir: Path,
    source_jpg_path: Path | None = None,
    source_pdf_path: Path | None = None,
    export_pdf_ai: bool = False,
    compare_old_path: Path | None = None,
    run_visual_qa: bool = False,
) -> dict[str, Any]:
    source_jpg, resolved_pdf = _resolve_source_assets(runtime, book_number)
    if source_jpg_path is not None:
        source_jpg = Path(source_jpg_path)
    if source_pdf_path is not None:
        resolved_pdf = Path(source_pdf_path)

    region = _resolve_region(runtime, book_number)
    out_dir.mkdir(parents=True, exist_ok=True)
    final_jpg = out_dir / "final_cover.jpg"
    final_pdf = out_dir / "final_cover.pdf"
    final_ai = out_dir / "final_cover.ai"

    if resolved_pdf is not None and resolved_pdf.exists():
        result = pdf_compositor.composite_cover_pdf(
            source_pdf_path=str(resolved_pdf),
            source_jpg_path=str(source_jpg),
            ai_art_path=str(ai_art_path),
            output_pdf_path=str(final_pdf),
            output_jpg_path=str(final_jpg),
            output_ai_path=str(final_ai),
            book_number=book_number,
            regions_path=config.cover_regions_path(catalog_id=runtime.catalog_id, config_dir=runtime.config_dir),
        )
        compositor_mode = "replacement_frame" if str(result.get("overlay_source", "")) == "replacement_frame_overlay" else "jpg_blend"
        validation_payload = {
            "compositor_mode": compositor_mode,
            "replacement_frame": result.get("replacement_frame", {}),
            "issues": result.get("issues", []),
            "valid": bool(result.get("valid", False)),
            "validation_metrics": result.get("validation_metrics", {}),
            "overlay_source": str(result.get("overlay_source", "")),
            "placement_source": str(result.get("placement_source", "")),
        }
        if not export_pdf_ai:
            for sidecar in (final_pdf, final_ai):
                if sidecar.exists():
                    sidecar.unlink()
    else:
        cover_compositor.composite_single(
            cover_path=source_jpg,
            illustration_path=ai_art_path,
            region=region,
            output_path=final_jpg,
            source_pdf_path=None,
        )
        validation_payload = _load_validation_json(final_jpg.with_suffix(final_jpg.suffix + ".validation.json"))
        compositor_mode = str(validation_payload.get("compositor_mode", "legacy") or "legacy")
        if export_pdf_ai:
            raise RuntimeError("PDF/AI export requires a source PDF and the PDF compositor path")

    with Image.open(source_jpg) as src_image, Image.open(final_jpg) as final_image:
        center_x, center_y, crop_radius = _medallion_geometry(final_image.size, region)
        crop_box = _crop_box(final_image.size, center_x, center_y, crop_radius)
        source_crop = src_image.convert("RGB").crop(crop_box)
        final_crop = final_image.convert("RGB").crop(crop_box)
        source_crop_path = out_dir / "source_medallion_crop.jpg"
        final_crop_path = out_dir / "medallion_crop.jpg"
        source_crop.save(source_crop_path, format="JPEG", quality=95)
        final_crop.save(final_crop_path, format="JPEG", quality=95)
        _save_compare_row(
            [("Source", src_image.convert("RGB")), ("Final", final_image.convert("RGB"))],
            out_dir / "source_vs_final_compare.jpg",
        )
        _save_compare_row(
            [("Source Crop", source_crop), ("Final Crop", final_crop)],
            out_dir / "source_vs_final_crop_compare.jpg",
        )
        replacement_payload = validation_payload.get("replacement_frame", {}) if isinstance(validation_payload, dict) else {}
        if isinstance(replacement_payload, dict) and replacement_payload:
            _save_anchor_debug(
                source_image=src_image,
                final_image=final_image,
                crop_box=crop_box,
                replacement_frame=replacement_payload,
                output_path=out_dir / "anchor_debug_compare.jpg",
            )

        if compare_old_path is not None and Path(compare_old_path).exists():
            with Image.open(compare_old_path) as old_image:
                old_rgb = old_image.convert("RGB")
                _save_compare_row(
                    [("Source", src_image.convert("RGB")), ("Old", old_rgb), ("New", final_image.convert("RGB"))],
                    out_dir / "source_vs_old_vs_new_compare.jpg",
                )
                _save_compare_row(
                    [("Source Crop", source_crop), ("Old Crop", old_rgb.crop(crop_box)), ("New Crop", final_crop)],
                    out_dir / "source_vs_old_vs_new_crop_compare.jpg",
                )

    visual_report: dict[str, Any] | None = None
    if run_visual_qa:
        qa_dir = out_dir / "qa"
        visual_report = visual_qa.verify_composite(
            original_path=source_jpg,
            composite_path=final_jpg,
            book_number=book_number,
            book_title=_cover_title(runtime, book_number),
            center_x=center_x,
            center_y=center_y,
            radius=max(20, int(region.get("radius", frame_geometry.BASE_ART_CLIP_RADIUS) or frame_geometry.BASE_ART_CLIP_RADIUS)),
            output_dir=qa_dir,
            golden_dir=qa_dir / "golden",
            compositor_mode=compositor_mode,
            replacement_metrics=validation_payload.get("replacement_frame", {}) if isinstance(validation_payload, dict) else None,
        )

    summary = {
        "book_number": int(book_number),
        "title": _cover_title(runtime, book_number),
        "source_jpg": str(source_jpg),
        "source_pdf": str(resolved_pdf) if resolved_pdf is not None and resolved_pdf.exists() else "",
        "ai_art_path": str(ai_art_path),
        "compositor_mode": compositor_mode,
        "region": region,
        "outputs": {
            "final_cover": str(final_jpg),
            "final_pdf": str(final_pdf) if final_pdf.exists() else "",
            "final_ai": str(final_ai) if final_ai.exists() else "",
            "medallion_crop": str(out_dir / "medallion_crop.jpg"),
            "source_medallion_crop": str(out_dir / "source_medallion_crop.jpg"),
            "source_vs_final_compare": str(out_dir / "source_vs_final_compare.jpg"),
            "source_vs_final_crop_compare": str(out_dir / "source_vs_final_crop_compare.jpg"),
            "source_vs_old_vs_new_compare": str(out_dir / "source_vs_old_vs_new_compare.jpg") if (out_dir / "source_vs_old_vs_new_compare.jpg").exists() else "",
            "anchor_debug_compare": str(out_dir / "anchor_debug_compare.jpg") if (out_dir / "anchor_debug_compare.jpg").exists() else "",
        },
        "validation": validation_payload,
        "visual_qa": visual_report or {},
    }
    safe_json.atomic_write_json(out_dir / "validation.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-run one cover composite using existing AI art")
    parser.add_argument("--catalog", default=config.DEFAULT_CATALOG_ID, help="Catalog id")
    parser.add_argument("--book", type=int, required=True, help="Book number")
    parser.add_argument("--ai-art", required=True, help="Path to existing AI art")
    parser.add_argument("--source-jpg", default="", help="Optional override for source JPG")
    parser.add_argument("--source-pdf", default="", help="Optional override for source PDF")
    parser.add_argument("--out-dir", default="", help="Output directory")
    parser.add_argument("--export-pdf-ai", action="store_true", help="Keep exported PDF/AI when source PDF is available")
    parser.add_argument("--compare-old", default="", help="Optional old composite to compare against")
    parser.add_argument("--run-visual-qa", action="store_true", help="Run structural visual QA on the result")
    args = parser.parse_args()

    runtime = config.get_config(args.catalog)
    out_dir = (
        Path(args.out_dir).expanduser()
        if str(args.out_dir).strip()
        else runtime.tmp_dir / "manual_verify" / f"book_{int(args.book):03d}"
    )
    summary = run_once(
        runtime=runtime,
        book_number=int(args.book),
        ai_art_path=Path(args.ai_art).expanduser(),
        out_dir=out_dir,
        source_jpg_path=Path(args.source_jpg).expanduser() if str(args.source_jpg).strip() else None,
        source_pdf_path=Path(args.source_pdf).expanduser() if str(args.source_pdf).strip() else None,
        export_pdf_ai=bool(args.export_pdf_ai),
        compare_old_path=Path(args.compare_old).expanduser() if str(args.compare_old).strip() else None,
        run_visual_qa=bool(args.run_visual_qa),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
