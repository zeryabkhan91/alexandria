"""IngramSpark export pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image
try:
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas
except Exception:  # pragma: no cover - optional dependency at runtime
    inch = None  # type: ignore
    canvas = None  # type: ignore

try:
    from src import export_utils
except ModuleNotFoundError:  # pragma: no cover
    import export_utils  # type: ignore


def _spine_width_inches(page_count: int, paper_thickness_in: float = 0.0025) -> float:
    pages = max(1, int(page_count))
    return max(0.08, pages * float(paper_thickness_in))


def _draw_trim_marks(pdf: canvas.Canvas, *, width_pt: float, height_pt: float, margin_pt: float = 10.0) -> None:
    pdf.setLineWidth(0.6)
    pdf.setStrokeColorRGB(0, 0, 0)
    # top-left
    pdf.line(margin_pt, height_pt - margin_pt, margin_pt + 30, height_pt - margin_pt)
    pdf.line(margin_pt, height_pt - margin_pt, margin_pt, height_pt - margin_pt - 30)
    # top-right
    pdf.line(width_pt - margin_pt - 30, height_pt - margin_pt, width_pt - margin_pt, height_pt - margin_pt)
    pdf.line(width_pt - margin_pt, height_pt - margin_pt, width_pt - margin_pt, height_pt - margin_pt - 30)
    # bottom-left
    pdf.line(margin_pt, margin_pt, margin_pt + 30, margin_pt)
    pdf.line(margin_pt, margin_pt, margin_pt, margin_pt + 30)
    # bottom-right
    pdf.line(width_pt - margin_pt - 30, margin_pt, width_pt - margin_pt, margin_pt)
    pdf.line(width_pt - margin_pt, margin_pt, width_pt - margin_pt, margin_pt + 30)


def export_book(
    *,
    book_number: int,
    catalog_id: str,
    catalog_path: Path,
    output_root: Path,
    selections_path: Path,
    quality_path: Path,
    exports_root: Path,
) -> dict[str, Any]:
    if canvas is None or inch is None:
        raise RuntimeError("reportlab is not installed; Ingram export requires reportlab")
    winners = export_utils.load_winner_books(
        catalog_path=catalog_path,
        output_root=output_root,
        selections_path=selections_path,
        quality_path=quality_path,
    )
    winner = winners.get(int(book_number))
    if winner is None:
        raise ValueError(f"Winner not available for book {book_number}")

    cover = Image.open(winner.cover_path).convert("CMYK")
    width_px, height_px = cover.size
    width_in = width_px / 300.0
    height_in = height_px / 300.0
    bleed_in = 0.125
    spine_in = _spine_width_inches(winner.page_count)

    book_dir = exports_root / "ingram" / catalog_id / str(book_number)
    book_dir.mkdir(parents=True, exist_ok=True)

    cmyk_cover_path = book_dir / f"{winner.isbn}_cover_cmyk.jpg"
    cover.save(cmyk_cover_path, format="JPEG", quality=95, optimize=True, dpi=(300, 300))

    pdf_path = book_dir / f"{winner.isbn}_cover.pdf"
    page_width_pt = (width_in + 2 * bleed_in) * inch
    page_height_pt = (height_in + 2 * bleed_in) * inch
    pdf = canvas.Canvas(str(pdf_path), pagesize=(page_width_pt, page_height_pt), pageCompression=1)

    pdf.drawImage(
        str(cmyk_cover_path),
        bleed_in * inch,
        bleed_in * inch,
        width=width_in * inch,
        height=height_in * inch,
        preserveAspectRatio=True,
        mask="auto",
    )
    _draw_trim_marks(pdf, width_pt=page_width_pt, height_pt=page_height_pt)
    pdf.setFont("Helvetica", 8)
    pdf.drawString(16, 16, f"Spine width: {spine_in:.4f} in")
    pdf.drawRightString(page_width_pt - 16, 16, "Profile: PDF/X-1a (approx)")
    pdf.save()

    return {
        "book_number": int(book_number),
        "catalog": catalog_id,
        "export_type": "ingram",
        "export_path": str(book_dir),
        "file_count": 2,
        "files": [cmyk_cover_path.name, pdf_path.name],
        "isbn": winner.isbn,
        "page_count": int(winner.page_count),
        "spine_width_inches": round(float(spine_in), 6),
    }


def export_catalog(
    *,
    catalog_id: str,
    catalog_path: Path,
    output_root: Path,
    selections_path: Path,
    quality_path: Path,
    exports_root: Path,
    books: list[int] | None = None,
) -> dict[str, Any]:
    winners = export_utils.load_winner_books(
        catalog_path=catalog_path,
        output_root=output_root,
        selections_path=selections_path,
        quality_path=quality_path,
    )
    target_books = sorted(int(b) for b in books) if books else sorted(winners.keys())
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for book in target_books:
        try:
            results.append(
                export_book(
                    book_number=book,
                    catalog_id=catalog_id,
                    catalog_path=catalog_path,
                    output_root=output_root,
                    selections_path=selections_path,
                    quality_path=quality_path,
                    exports_root=exports_root,
                )
            )
        except Exception as exc:
            errors.append({"book_number": int(book), "error": str(exc)})
    return {
        "ok": len(errors) == 0,
        "catalog": catalog_id,
        "export_type": "ingram",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books_requested": len(target_books),
        "books_exported": len(results),
        "file_count": sum(int(item.get("file_count", 0)) for item in results),
        "results": results,
        "errors": errors,
    }
