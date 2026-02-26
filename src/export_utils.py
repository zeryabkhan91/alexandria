"""Shared helpers for platform export pipelines."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image

try:
    from src import mockup_generator
    from src import safe_json
except ModuleNotFoundError:  # pragma: no cover
    import mockup_generator  # type: ignore
    import safe_json  # type: ignore


@dataclass(slots=True)
class WinnerBook:
    """Winner-cover metadata used by export modules."""

    book_number: int
    title: str
    author: str
    folder_name: str
    winner_variant: int
    cover_path: Path
    isbn: str
    page_count: int
    quality_score: float


def default_isbn(book_number: int) -> str:
    return f"BOOK{int(book_number):05d}"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _find_first_jpg(folder: Path) -> Path | None:
    for candidate in sorted(folder.glob("*.jpg")):
        if candidate.is_file():
            return candidate
    return None


def _quality_lookup(quality_path: Path) -> dict[tuple[int, int], float]:
    payload = safe_json.load_json(quality_path, {})
    rows = payload.get("scores", []) if isinstance(payload, dict) else []
    lookup: dict[tuple[int, int], float] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        book = _safe_int(row.get("book_number"), 0)
        variant = _safe_int(row.get("variant_id"), 0)
        if book <= 0 or variant <= 0:
            continue
        score = _safe_float(row.get("overall_score"), 0.0)
        lookup[(book, variant)] = max(lookup.get((book, variant), 0.0), score)
    return lookup


def load_winner_books(
    *,
    catalog_path: Path,
    output_root: Path,
    selections_path: Path,
    quality_path: Path | None = None,
) -> dict[int, WinnerBook]:
    """Resolve winner-cover files and metadata for a catalog."""
    loaded = safe_json.load_json(catalog_path, [])
    catalog_payload = loaded if isinstance(loaded, list) else []

    catalog_rows = {int(row.get("number")): row for row in catalog_payload if isinstance(row, dict) and _safe_int(row.get("number"), 0) > 0}
    records = mockup_generator.load_book_records(catalog_path)
    winner_map = mockup_generator.load_winner_map(selections_path)
    scores = _quality_lookup(quality_path) if quality_path else {}

    out: dict[int, WinnerBook] = {}
    for book_number, variant in winner_map.items():
        record = records.get(book_number)
        if record is None:
            continue
        variant_dir = output_root / record.folder_name / f"Variant-{variant}"
        cover_path = _find_first_jpg(variant_dir)
        if cover_path is None:
            continue
        row = catalog_rows.get(book_number, {})
        isbn = str(row.get("isbn", "")).strip() if isinstance(row, dict) else ""
        if not isbn:
            isbn = default_isbn(book_number)
        page_count = _safe_int(row.get("page_count") if isinstance(row, dict) else 0, 320)
        if page_count <= 0:
            page_count = 320
        quality_score = scores.get((book_number, int(variant)), 0.0)
        out[book_number] = WinnerBook(
            book_number=int(book_number),
            title=str(record.title or ""),
            author=str(record.author or ""),
            folder_name=str(record.folder_name or ""),
            winner_variant=int(variant),
            cover_path=cover_path,
            isbn=isbn,
            page_count=page_count,
            quality_score=float(quality_score),
        )
    return out


def crop_cover_regions(cover: Image.Image, *, spine_ratio: float = 0.075) -> tuple[Image.Image, Image.Image, Image.Image, Image.Image]:
    """Split full-wrap cover into front/spine/back/detail."""
    width, height = cover.size
    spine_width = max(24, int(width * spine_ratio))
    front_width = max(1, int((width - spine_width) * 0.5))
    back_width = max(1, width - front_width - spine_width)

    back = cover.crop((0, 0, back_width, height))
    spine = cover.crop((back_width, 0, back_width + spine_width, height))
    front = cover.crop((back_width + spine_width, 0, width, height))

    cx = int(front.width * 0.52)
    cy = int(front.height * 0.52)
    radius = int(min(front.width, front.height) * 0.27)
    detail = front.crop((max(0, cx - radius), max(0, cy - radius), min(front.width, cx + radius), min(front.height, cy + radius)))
    return front, spine, back, detail


def ensure_rgb_jpeg(image: Image.Image, *, quality: int = 92, dpi: tuple[int, int] = (300, 300), destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    image.convert("RGB").save(destination, format="JPEG", quality=int(max(60, min(100, quality))), optimize=True, dpi=dpi)
    return destination
