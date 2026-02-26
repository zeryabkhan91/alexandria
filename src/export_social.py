"""Social-media export pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

try:
    from src import export_utils
except ModuleNotFoundError:  # pragma: no cover
    import export_utils  # type: ignore


SOCIAL_SPECS: dict[str, list[tuple[str, tuple[int, int]]]] = {
    "instagram": [("square", (1080, 1080)), ("portrait", (1080, 1350)), ("story", (1080, 1920))],
    "facebook": [("link_share", (1200, 630)), ("post", (1080, 1080))],
    "twitter": [("card", (1200, 675)), ("post", (1080, 1080))],
    "pinterest": [("pin", (1000, 1500))],
    "tiktok": [("vertical", (1080, 1920))],
}


def _load_font(size: int) -> ImageFont.ImageFont:
    for candidate in ("Times New Roman.ttf", "Georgia.ttf", "DejaVuSerif.ttf"):
        try:
            return ImageFont.truetype(candidate, size=max(12, int(size)))
        except OSError:
            continue
    return ImageFont.load_default()


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, *, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = [w for w in str(text or "").split() if w]
    lines: list[str] = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if not current or (bbox[2] - bbox[0]) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def _base_canvas(size: tuple[int, int]) -> Image.Image:
    width, height = size
    canvas = Image.new("RGB", size, (24, 37, 64))
    draw = ImageDraw.Draw(canvas, "RGBA")
    draw.rectangle((0, 0, width, height), fill=(26, 39, 68, 255))
    draw.rectangle((0, 0, int(width * 0.35), height), fill=(20, 32, 56, 255))
    draw.polygon([(int(width * 0.2), 0), (int(width * 0.52), 0), (int(width * 0.08), height)], fill=(196, 163, 82, 40))
    for idx in range(6):
        y = int(height * (0.18 + idx * 0.12))
        draw.line((0, y, width, y), fill=(255, 255, 255, 18 if idx % 2 == 0 else 10), width=2)
    return canvas


def _render_asset(
    *,
    cover: Image.Image,
    size: tuple[int, int],
    title: str,
    author: str,
    watermark: bool,
) -> Image.Image:
    canvas = _base_canvas(size)
    draw = ImageDraw.Draw(canvas, "RGBA")
    width, height = size

    cover_max_w = int(width * 0.62)
    cover_max_h = int(height * 0.62)
    scale = min(cover_max_w / max(1, cover.width), cover_max_h / max(1, cover.height))
    cover_w = max(1, int(cover.width * scale))
    cover_h = max(1, int(cover.height * scale))
    resized = cover.resize((cover_w, cover_h), Image.LANCZOS)

    x = (width - cover_w) // 2
    y = int(height * 0.08)
    canvas.paste(resized, (x, y))

    title_font = _load_font(max(30, int(min(width, height) * 0.055)))
    author_font = _load_font(max(20, int(min(width, height) * 0.032)))
    body_font = _load_font(max(16, int(min(width, height) * 0.024)))

    text_y = y + cover_h + int(height * 0.04)
    max_width = int(width * 0.82)
    lines = _wrap_text(draw, title, font=title_font, max_width=max_width)[:3]
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=title_font)
        text_x = (width - (bbox[2] - bbox[0])) // 2
        draw.text((text_x, text_y), line, font=title_font, fill=(245, 230, 200, 255))
        text_y += (bbox[3] - bbox[1]) + 6

    author_line = str(author or "").strip()[:100]
    if author_line:
        abox = draw.textbbox((0, 0), author_line, font=author_font)
        author_x = (width - (abox[2] - abox[0])) // 2
        draw.text((author_x, text_y + 8), author_line, font=author_font, fill=(196, 163, 82, 255))
        text_y += (abox[3] - abox[1]) + 8

    cta = "New Edition Available"
    cta_bbox = draw.textbbox((0, 0), cta, font=body_font)
    pill_w = (cta_bbox[2] - cta_bbox[0]) + 40
    pill_h = (cta_bbox[3] - cta_bbox[1]) + 20
    pill_x = (width - pill_w) // 2
    pill_y = min(height - pill_h - 24, text_y + 20)
    draw.rounded_rectangle((pill_x, pill_y, pill_x + pill_w, pill_y + pill_h), radius=20, fill=(196, 163, 82, 235))
    draw.text((pill_x + 20, pill_y + 10), cta, font=body_font, fill=(20, 32, 57, 255))

    if watermark:
        wm = "Alexandria"
        wfont = _load_font(max(14, int(min(width, height) * 0.018)))
        draw.text((20, height - 40), wm, font=wfont, fill=(245, 230, 200, 150))

    return canvas


def _normalize_platforms(platforms: list[str] | str | None) -> list[str]:
    if platforms is None:
        return sorted(SOCIAL_SPECS.keys())
    if isinstance(platforms, str):
        token = platforms.strip().lower()
        if not token or token == "all":
            return sorted(SOCIAL_SPECS.keys())
        return [p.strip().lower() for p in token.split(",") if p.strip().lower() in SOCIAL_SPECS]
    out = [str(p).strip().lower() for p in platforms if str(p).strip().lower() in SOCIAL_SPECS]
    return sorted(set(out)) or sorted(SOCIAL_SPECS.keys())


def export_book(
    *,
    book_number: int,
    catalog_id: str,
    catalog_path: Path,
    output_root: Path,
    selections_path: Path,
    quality_path: Path,
    exports_root: Path,
    platforms: list[str] | str | None = None,
    watermark: bool = True,
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

    selected = _normalize_platforms(platforms)
    if not selected:
        raise ValueError("No valid platforms selected")
    cover = Image.open(winner.cover_path).convert("RGB")

    written: list[Path] = []
    for platform in selected:
        platform_dir = exports_root / "social" / catalog_id / str(book_number) / platform
        platform_dir.mkdir(parents=True, exist_ok=True)
        for variant_name, size in SOCIAL_SPECS[platform]:
            image = _render_asset(cover=cover, size=size, title=winner.title, author=winner.author, watermark=watermark)
            target = platform_dir / f"{platform}_{variant_name}.jpg"
            export_utils.ensure_rgb_jpeg(image, quality=92, dpi=(72, 72), destination=target)
            written.append(target)

    return {
        "book_number": int(book_number),
        "catalog": catalog_id,
        "export_type": "social",
        "platforms": selected,
        "file_count": len(written),
        "files": [str(path.relative_to(exports_root)) for path in written],
        "export_path": str((exports_root / "social" / catalog_id / str(book_number))),
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
    platforms: list[str] | str | None = None,
    watermark: bool = True,
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
                    platforms=platforms,
                    watermark=watermark,
                )
            )
        except Exception as exc:
            errors.append({"book_number": int(book), "error": str(exc)})
    return {
        "ok": len(errors) == 0,
        "catalog": catalog_id,
        "export_type": "social",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books_requested": len(target_books),
        "books_exported": len(results),
        "file_count": sum(int(item.get("file_count", 0)) for item in results),
        "results": results,
        "errors": errors,
    }
