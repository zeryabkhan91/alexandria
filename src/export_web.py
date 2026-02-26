"""Website and marketing web-asset export pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image

try:
    from src import export_utils
    from src import safe_json
except ModuleNotFoundError:  # pragma: no cover
    import export_utils  # type: ignore
    import safe_json  # type: ignore


def _resize_longest(image: Image.Image, longest: int) -> Image.Image:
    width, height = image.size
    current = max(width, height)
    if current <= int(longest):
        return image.copy()
    scale = float(longest) / float(current)
    return image.resize((max(1, int(width * scale)), max(1, int(height * scale))), Image.LANCZOS)


def _find_mockups(output_root: Path, folder_name: str) -> list[str]:
    mockup_dir = output_root / "Mockups" / folder_name
    if not mockup_dir.exists():
        return []
    out: list[str] = []
    for image_path in sorted(mockup_dir.glob("*.jpg")):
        out.append(str(image_path))
        if len(out) >= 6:
            break
    return out


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
    winners = export_utils.load_winner_books(
        catalog_path=catalog_path,
        output_root=output_root,
        selections_path=selections_path,
        quality_path=quality_path,
    )
    winner = winners.get(int(book_number))
    if winner is None:
        raise ValueError(f"Winner not available for book {book_number}")

    cover = Image.open(winner.cover_path).convert("RGB")
    book_dir = exports_root / "web" / catalog_id / str(book_number)
    book_dir.mkdir(parents=True, exist_ok=True)

    large = _resize_longest(cover, 2000)
    medium = _resize_longest(cover, 600)
    small = _resize_longest(cover, 300)
    thumb = _resize_longest(cover, 150)

    large_jpg = export_utils.ensure_rgb_jpeg(large, quality=85, dpi=(72, 72), destination=book_dir / "cover_2000.jpg")
    medium_jpg = export_utils.ensure_rgb_jpeg(medium, quality=85, dpi=(72, 72), destination=book_dir / "cover_600.jpg")
    small_jpg = export_utils.ensure_rgb_jpeg(small, quality=85, dpi=(72, 72), destination=book_dir / "cover_300.jpg")
    thumb_jpg = export_utils.ensure_rgb_jpeg(thumb, quality=85, dpi=(72, 72), destination=book_dir / "cover_150.jpg")
    webp_path = book_dir / "cover_2000.webp"
    large.save(webp_path, format="WEBP", quality=85, method=6)

    metadata = {
        "book_number": int(winner.book_number),
        "title": winner.title,
        "author": winner.author,
        "cover_large": large_jpg.name,
        "cover_medium": medium_jpg.name,
        "cover_small": small_jpg.name,
        "cover_thumb": thumb_jpg.name,
        "cover_webp": webp_path.name,
        "quality_score": round(float(winner.quality_score), 6),
        "mockups": _find_mockups(output_root, winner.folder_name),
    }
    safe_json.atomic_write_json(book_dir / "metadata.json", metadata)
    return {
        "book_number": int(winner.book_number),
        "catalog": catalog_id,
        "export_type": "web",
        "export_path": str(book_dir),
        "file_count": 6,
        "files": [large_jpg.name, medium_jpg.name, small_jpg.name, thumb_jpg.name, webp_path.name, "metadata.json"],
        "metadata": metadata,
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

    manifest_rows = [dict(item.get("metadata", {})) for item in results if isinstance(item.get("metadata"), dict)]
    manifest = {
        "catalog": catalog_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(manifest_rows),
        "books": manifest_rows,
    }
    manifest_path = exports_root / "web" / catalog_id / "manifest.json"
    safe_json.atomic_write_json(manifest_path, manifest)

    return {
        "ok": len(errors) == 0,
        "catalog": catalog_id,
        "export_type": "web",
        "generated_at": manifest["generated_at"],
        "books_requested": len(target_books),
        "books_exported": len(results),
        "file_count": sum(int(item.get("file_count", 0)) for item in results) + 1,
        "manifest_path": str(manifest_path),
        "results": results,
        "errors": errors,
    }
