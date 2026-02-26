"""Prompt 11D social and marketing card generator."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont

try:
    from src import config
    from src import mockup_generator
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    import config  # type: ignore
    import mockup_generator  # type: ignore
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)

DEFAULT_OUTPUT_ROOT = config.OUTPUT_DIR
DEFAULT_SOCIAL_ROOT = DEFAULT_OUTPUT_ROOT / "Social"

SOCIAL_SPECS = {
    "instagram": {"size": (1080, 1080), "label": "Instagram Post"},
    "facebook": {"size": (1200, 630), "label": "Facebook/LinkedIn OG"},
    "twitter": {"size": (1200, 675), "label": "Twitter/X Card"},
    "story": {"size": (1080, 1920), "label": "Instagram Story"},
    "pinterest": {"size": (1000, 1500), "label": "Pinterest Pin"},
}


@dataclass(slots=True)
class SocialResult:
    book: int
    format: str
    output_path: str



def _load_font(size: int) -> ImageFont.ImageFont:
    for candidate in ("Times New Roman.ttf", "Georgia.ttf", "DejaVuSerif.ttf"):
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _text_wrap(draw: ImageDraw.ImageDraw, text: str, *, max_width: int, font: ImageFont.ImageFont) -> list[str]:
    words = [token for token in text.split() if token]
    lines: list[str] = []
    current = ""

    for word in words:
        trial = f"{current} {word}".strip()
        bbox = draw.textbbox((0, 0), trial, font=font)
        if bbox[2] - bbox[0] <= max_width or not current:
            current = trial
        else:
            lines.append(current)
            current = word

    if current:
        lines.append(current)
    return lines


def _draw_text_block(
    draw: ImageDraw.ImageDraw,
    *,
    text: str,
    start: tuple[int, int],
    width: int,
    font: ImageFont.ImageFont,
    fill: tuple[int, int, int, int],
    line_gap: int,
    max_lines: int,
) -> int:
    x, y = start
    lines = _text_wrap(draw, text, max_width=width, font=font)[:max_lines]
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        bbox = draw.textbbox((0, 0), line, font=font)
        y += (bbox[3] - bbox[1]) + line_gap
    return y


def _gradient_background(size: tuple[int, int]) -> Image.Image:
    width, height = size
    canvas = Image.new("RGBA", size, (26, 39, 68, 255))
    draw = ImageDraw.Draw(canvas, "RGBA")

    draw.rectangle((0, 0, width, height), fill=(25, 39, 67, 255))
    draw.rectangle((0, 0, int(width * 0.32), height), fill=(20, 31, 56, 255))
    draw.polygon(
        [(int(width * 0.24), 0), (int(width * 0.55), 0), (int(width * 0.12), height)],
        fill=(196, 163, 82, 45),
    )

    for idx in range(6):
        y = int(height * (0.15 + idx * 0.12))
        alpha = 18 if idx % 2 == 0 else 10
        draw.line((0, y, width, y), fill=(255, 255, 255, alpha), width=2)

    draw.line((int(width * 0.04), int(height * 0.92), int(width * 0.96), int(height * 0.92)), fill=(196, 163, 82, 220), width=4)
    return canvas


def _load_mockup_or_generate(
    *,
    cover_path: Path,
    book_title: str,
    book_author: str,
    template_id: str,
    temp_dir: Path,
) -> Image.Image:
    temp_dir.mkdir(parents=True, exist_ok=True)
    target = temp_dir / f"{template_id}.jpg"
    if not target.exists():
        mockup_generator.generate_mockup(
            cover_image_path=str(cover_path),
            template_id=template_id,
            output_path=str(target),
            spine_width_px=100,
            book_title=book_title,
            book_author=book_author,
        )
    return Image.open(target).convert("RGBA")


def _compose_format(
    *,
    fmt: str,
    book_title: str,
    author: str,
    standing_front: Image.Image,
    standing_angled: Image.Image,
) -> Image.Image:
    spec = SOCIAL_SPECS[fmt]
    width, height = spec["size"]
    canvas = _gradient_background((width, height))
    draw = ImageDraw.Draw(canvas, "RGBA")

    title_font = _load_font(max(28, int(min(width, height) * 0.054)))
    author_font = _load_font(max(20, int(min(width, height) * 0.032)))
    body_font = _load_font(max(16, int(min(width, height) * 0.024)))

    if fmt == "instagram":
        mock = standing_angled.resize((int(width * 0.58), int(height * 0.58)), Image.LANCZOS)
        canvas.alpha_composite(mock, ((width - mock.width) // 2, int(height * 0.08)))
        y = _draw_text_block(draw, text=book_title, start=(int(width * 0.1), int(height * 0.7)), width=int(width * 0.8), font=title_font, fill=(245, 230, 200, 255), line_gap=8, max_lines=3)
        draw.text((int(width * 0.1), y + 6), author, font=author_font, fill=(196, 163, 82, 255))
        draw.rounded_rectangle((int(width * 0.66), int(height * 0.84), int(width * 0.93), int(height * 0.91)), radius=22, fill=(196, 163, 82, 235))
        draw.text((int(width * 0.695), int(height * 0.855)), "Available Now", font=body_font, fill=(18, 31, 57, 255))

    elif fmt in {"facebook", "twitter"}:
        mock_h = int(height * 0.82)
        mock_w = int(mock_h * standing_front.width / max(1, standing_front.height))
        mock = standing_front.resize((mock_w, mock_h), Image.LANCZOS)
        canvas.alpha_composite(mock, (int(width * 0.06), (height - mock_h) // 2))

        text_x = int(width * 0.42)
        y = _draw_text_block(draw, text=book_title, start=(text_x, int(height * 0.16)), width=int(width * 0.52), font=title_font, fill=(245, 230, 200, 255), line_gap=10, max_lines=4)
        y = _draw_text_block(draw, text=author, start=(text_x, y + 8), width=int(width * 0.52), font=author_font, fill=(196, 163, 82, 255), line_gap=6, max_lines=2)
        draw.text((text_x, y + 14), "New edition available from Alexandria Publishing", font=body_font, fill=(214, 202, 177, 245))

    elif fmt == "story":
        mock = standing_angled.resize((int(width * 0.72), int(width * 0.72)), Image.LANCZOS)
        canvas.alpha_composite(mock, ((width - mock.width) // 2, int(height * 0.2)))
        y = _draw_text_block(draw, text=book_title, start=(int(width * 0.1), int(height * 0.07)), width=int(width * 0.8), font=title_font, fill=(245, 230, 200, 255), line_gap=10, max_lines=4)
        draw.text((int(width * 0.1), y + 6), author, font=author_font, fill=(196, 163, 82, 255))
        draw.rounded_rectangle((int(width * 0.12), int(height * 0.88), int(width * 0.88), int(height * 0.95)), radius=30, fill=(196, 163, 82, 228))
        draw.text((int(width * 0.28), int(height * 0.902)), "Swipe up to preview", font=body_font, fill=(20, 32, 57, 255))

    else:  # pinterest
        mock = standing_front.resize((int(width * 0.78), int(height * 0.48)), Image.LANCZOS)
        canvas.alpha_composite(mock, ((width - mock.width) // 2, int(height * 0.05)))
        y = _draw_text_block(draw, text=book_title, start=(int(width * 0.1), int(height * 0.58)), width=int(width * 0.8), font=title_font, fill=(245, 230, 200, 255), line_gap=10, max_lines=4)
        y = _draw_text_block(draw, text=author, start=(int(width * 0.1), y + 8), width=int(width * 0.8), font=author_font, fill=(196, 163, 82, 255), line_gap=8, max_lines=2)
        draw.text((int(width * 0.1), y + 12), "Classic literature collector edition", font=body_font, fill=(224, 209, 178, 245))

    return canvas.convert("RGB")


def generate_social_cards_for_book(
    *,
    book_number: int,
    formats: list[str],
    output_root: Path,
    selections_path: Path,
) -> dict[str, Any]:
    catalog = mockup_generator.load_book_records()
    winners = mockup_generator.load_winner_map(selections_path)
    record = catalog.get(book_number)
    if not record:
        raise RuntimeError(f"Book {book_number} not in catalog")

    cover_path = mockup_generator.winner_cover_path(
        book_number=book_number,
        output_root=output_root,
        catalog=catalog,
        winner_map=winners,
    )

    temp_dir = config.TMP_DIR / "social_base" / str(book_number)
    standing_front = _load_mockup_or_generate(
        cover_path=cover_path,
        book_title=record.title,
        book_author=record.author,
        template_id="standing_front",
        temp_dir=temp_dir,
    )
    standing_angled = _load_mockup_or_generate(
        cover_path=cover_path,
        book_title=record.title,
        book_author=record.author,
        template_id="standing_angled",
        temp_dir=temp_dir,
    )

    book_dir = output_root / "Social" / record.folder_name
    book_dir.mkdir(parents=True, exist_ok=True)

    generated: list[str] = []
    for fmt in formats:
        if fmt not in SOCIAL_SPECS:
            continue
        image = _compose_format(
            fmt=fmt,
            book_title=record.title,
            author=record.author,
            standing_front=standing_front,
            standing_angled=standing_angled,
        )
        target = book_dir / f"{fmt}.jpg"
        image.save(target, format="JPEG", quality=95, optimize=True)
        generated.append(str(target))

    return {
        "book": book_number,
        "folder": record.folder_name,
        "formats": formats,
        "output": str(book_dir),
        "generated": generated,
    }


def generate_social_cards(
    *,
    output_dir: str = "Output Covers",
    selections_path: str = "data/winner_selections.json",
    book: int | None = None,
    all_books: bool = False,
    formats: list[str] | None = None,
) -> dict[str, Any]:
    output_root = Path(output_dir).resolve()
    selections = Path(selections_path).resolve()
    winners = mockup_generator.load_winner_map(selections)

    if formats is None:
        formats = ["instagram", "facebook", "twitter", "story", "pinterest"]

    wanted_formats = [fmt.strip().lower() for fmt in formats if fmt.strip().lower() in SOCIAL_SPECS]
    if not wanted_formats:
        raise RuntimeError("No valid social formats selected")

    if book is not None:
        target_books = [book]
    elif all_books:
        target_books = sorted(winners.keys())
    else:
        target_books = sorted(winners.keys())

    results = []
    failed = 0
    for number in target_books:
        try:
            results.append(
                generate_social_cards_for_book(
                    book_number=number,
                    formats=wanted_formats,
                    output_root=output_root,
                    selections_path=selections,
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            failed += 1
            logger.warning("Social card generation failed for %s: %s", number, exc)

    return {
        "books": len(results),
        "failed": failed,
        "formats": wanted_formats,
        "output": str((output_root / "Social").resolve()),
        "results": results,
    }


def _parse_formats(token: str | None) -> list[str] | None:
    if not token:
        return None
    return [piece.strip().lower() for piece in token.split(",") if piece.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate social media marketing cards")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--book", type=int, default=None, help="Book number")
    parser.add_argument("--all-books", action="store_true", help="Generate for all books")
    parser.add_argument("--formats", type=str, default="instagram,facebook,twitter,story,pinterest")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--selections", type=Path, default=None)

    args = parser.parse_args()
    catalog_id = str(getattr(args, "catalog", config.DEFAULT_CATALOG_ID) or config.DEFAULT_CATALOG_ID)
    runtime = config.get_config(catalog_id)
    selections_path = args.selections or config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    summary = generate_social_cards(
        output_dir=str(args.output_dir),
        selections_path=str(selections_path),
        book=args.book,
        all_books=args.all_books,
        formats=_parse_formats(args.formats),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
