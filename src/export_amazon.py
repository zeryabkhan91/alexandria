"""Amazon KDP export pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFilter, ImageFont

try:
    from src import export_utils
except ModuleNotFoundError:  # pragma: no cover
    import export_utils  # type: ignore


def _load_font(size: int) -> ImageFont.ImageFont:
    for candidate in ("Times New Roman.ttf", "Georgia.ttf", "DejaVuSerif.ttf"):
        try:
            return ImageFont.truetype(candidate, size=max(12, int(size)))
        except OSError:
            continue
    return ImageFont.load_default()


def _ensure_kdp_size(image: Image.Image) -> Image.Image:
    width, height = image.size
    shortest = min(width, height)
    longest = max(width, height)
    if shortest >= 625 and longest <= 10000:
        return image

    scale = 1.0
    if shortest < 625:
        scale = max(scale, 625.0 / float(shortest))
    if longest * scale > 10000:
        scale = min(scale, 10000.0 / float(longest))
    new_w = max(625, int(round(width * scale)))
    new_h = max(625, int(round(height * scale)))
    return image.resize((new_w, new_h), Image.LANCZOS)


def _mockup_on_white(front: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2400, 2400), (255, 255, 255))
    target_h = int(canvas.height * 0.84)
    target_w = int(front.width * (target_h / max(1, front.height)))
    target = front.resize((target_w, target_h), Image.LANCZOS)

    shadow = Image.new("RGBA", target.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(shadow, "RGBA")
    draw.rectangle((12, 12, target.width - 1, target.height - 1), fill=(0, 0, 0, 90))
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=10))
    x = (canvas.width - target_w) // 2
    y = (canvas.height - target_h) // 2
    canvas.paste(shadow.convert("RGB"), (x + 18, y + 22))
    canvas.paste(target, (x, y))
    return canvas


def _lifestyle_scene(front: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2400, 2400), (241, 235, 224))
    draw = ImageDraw.Draw(canvas, "RGBA")
    draw.rectangle((0, 0, 2400, 1400), fill=(229, 221, 206, 255))
    draw.rectangle((0, 1400, 2400, 2400), fill=(200, 175, 140, 255))
    draw.polygon([(0, 1400), (2400, 1260), (2400, 1400)], fill=(183, 160, 130, 220))

    target_h = int(canvas.height * 0.72)
    target_w = int(front.width * (target_h / max(1, front.height)))
    cover = front.resize((target_w, target_h), Image.LANCZOS)
    x = (canvas.width - target_w) // 2
    y = int(canvas.height * 0.16)
    canvas.paste(cover, (x, y))

    shadow = Image.new("RGBA", (target_w + 40, 120), (0, 0, 0, 0))
    sdraw = ImageDraw.Draw(shadow, "RGBA")
    sdraw.ellipse((0, 20, target_w + 40, 110), fill=(0, 0, 0, 80))
    shadow = shadow.filter(ImageFilter.GaussianBlur(12))
    canvas.paste(shadow.convert("RGB"), (x - 20, y + target_h - 20))
    return canvas


def _branding_image(*, title: str, author: str, detail: Image.Image) -> Image.Image:
    canvas = Image.new("RGB", (2400, 2400), (26, 39, 68))
    draw = ImageDraw.Draw(canvas, "RGBA")
    draw.rectangle((0, 0, 2400, 180), fill=(196, 163, 82, 255))
    draw.rectangle((0, 2220, 2400, 2400), fill=(196, 163, 82, 255))

    medallion = detail.resize((920, 920), Image.LANCZOS)
    mask = Image.new("L", medallion.size, 0)
    mdraw = ImageDraw.Draw(mask)
    mdraw.ellipse((0, 0, medallion.width - 1, medallion.height - 1), fill=255)
    x = (canvas.width - medallion.width) // 2
    y = 640
    canvas.paste(medallion, (x, y), mask)
    draw.ellipse((x - 16, y - 16, x + medallion.width + 16, y + medallion.height + 16), outline=(216, 190, 124, 255), width=14)

    title_font = _load_font(90)
    author_font = _load_font(52)
    draw.text((200, 260), title[:80], fill=(245, 230, 200, 255), font=title_font)
    draw.text((200, 430), author[:80], fill=(245, 230, 200, 255), font=author_font)
    draw.text((200, 2160), "Alexandria Premium Classics", fill=(26, 39, 68, 255), font=_load_font(44))
    return canvas


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
    front, spine, back, detail = export_utils.crop_cover_regions(cover)

    book_dir = exports_root / "amazon" / catalog_id / str(book_number)
    book_dir.mkdir(parents=True, exist_ok=True)

    isbn_file = export_utils.ensure_rgb_jpeg(
        _ensure_kdp_size(front),
        quality=95,
        dpi=(300, 300),
        destination=book_dir / f"{winner.isbn}_cover.jpg",
    )
    files = [
        export_utils.ensure_rgb_jpeg(_ensure_kdp_size(front), quality=95, dpi=(300, 300), destination=book_dir / "01_main_cover.jpg"),
        export_utils.ensure_rgb_jpeg(_ensure_kdp_size(back), quality=95, dpi=(300, 300), destination=book_dir / "02_back_cover.jpg"),
        export_utils.ensure_rgb_jpeg(_ensure_kdp_size(spine), quality=95, dpi=(300, 300), destination=book_dir / "03_spine_detail.jpg"),
        export_utils.ensure_rgb_jpeg(_ensure_kdp_size(detail), quality=95, dpi=(300, 300), destination=book_dir / "04_medallion_detail.jpg"),
        export_utils.ensure_rgb_jpeg(_mockup_on_white(front), quality=95, dpi=(300, 300), destination=book_dir / "05_mockup_white.jpg"),
        export_utils.ensure_rgb_jpeg(_lifestyle_scene(front), quality=95, dpi=(300, 300), destination=book_dir / "06_lifestyle_scene.jpg"),
        export_utils.ensure_rgb_jpeg(
            _branding_image(title=winner.title, author=winner.author, detail=detail),
            quality=95,
            dpi=(300, 300),
            destination=book_dir / "07_branding.jpg",
        ),
    ]
    files.append(isbn_file)

    return {
        "book_number": int(book_number),
        "catalog": catalog_id,
        "export_type": "amazon",
        "export_path": str(book_dir),
        "file_count": len(files),
        "files": [path.name for path in files],
        "isbn": winner.isbn,
        "quality_score": round(float(winner.quality_score), 6),
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
        "export_type": "amazon",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books_requested": len(target_books),
        "books_exported": len(results),
        "file_count": sum(int(item.get("file_count", 0)) for item in results),
        "results": results,
        "errors": errors,
    }
