#!/usr/bin/env python3
"""Generate Alexandria catalog PDFs (winners, contact sheet, all variants) using Pillow."""

from __future__ import annotations

import argparse
import json
import math
import textwrap
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

from PIL import Image, ImageDraw, ImageFont

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src import config
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    from src import config  # type: ignore
    from src.logger import get_logger  # type: ignore

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "Output Covers"
DEFAULT_SELECTIONS = PROJECT_ROOT / "data" / "winner_selections.json"
DEFAULT_QUALITY = config.quality_scores_path()
DEFAULT_CATALOG = PROJECT_ROOT / "config" / "book_catalog.json"
DEFAULT_PROMPTS = PROJECT_ROOT / "config" / "book_prompts.json"
DEFAULT_OUTPUT = DEFAULT_OUTPUT_DIR / "Alexandria_Cover_Catalog.pdf"

# A4 landscape at 300 DPI.
PAGE_SIZE = (3508, 2480)
MARGIN = 236

NAVY = (26, 39, 68)
NAVY_LIGHT = (36, 52, 84)
GOLD = (196, 163, 82)
CREAM = (245, 230, 200)
INK = (27, 34, 51)
WHITE = (255, 255, 255)
logger = get_logger(__name__)


@dataclass(slots=True)
class BookWinner:
    number: int
    title: str
    author: str
    folder_name: str
    winner_variant: int
    winner_score: float
    winner_model: str
    prompt: str
    image_path: Path | None


def _font(size: int, *, bold: bool = False, italic: bool = False) -> ImageFont.ImageFont:
    candidates: list[str] = []
    if bold and italic:
        candidates.extend(
            [
                "/System/Library/Fonts/Supplemental/Times New Roman Bold Italic.ttf",
                "/System/Library/Fonts/Supplemental/Georgia Bold Italic.ttf",
            ]
        )
    elif bold:
        candidates.extend(
            [
                "/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf",
                "/System/Library/Fonts/Supplemental/Georgia Bold.ttf",
                "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
            ]
        )
    elif italic:
        candidates.extend(
            [
                "/System/Library/Fonts/Supplemental/Times New Roman Italic.ttf",
                "/System/Library/Fonts/Supplemental/Georgia Italic.ttf",
            ]
        )
    else:
        candidates.extend(
            [
                "/System/Library/Fonts/Supplemental/Times New Roman.ttf",
                "/System/Library/Fonts/Supplemental/Georgia.ttf",
                "/System/Library/Fonts/Supplemental/Arial.ttf",
            ]
        )

    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _norm_folder_name(folder_name: str) -> str:
    return folder_name[:-5] if folder_name.endswith(" copy") else folder_name


def _load_quality_maps(quality_path: Path) -> tuple[dict[tuple[int, int], dict[str, Any]], list[dict[str, Any]]]:
    payload = _load_json(quality_path, {"scores": []})
    rows = payload.get("scores", []) if isinstance(payload, dict) else []
    by_book_variant: dict[tuple[int, int], dict[str, Any]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        book = _safe_int(row.get("book_number"), 0)
        variant = _safe_int(row.get("variant_id"), 0)
        if book <= 0 or variant <= 0:
            continue

        key = (book, variant)
        score = _safe_float(row.get("overall_score"), 0.0)
        existing = by_book_variant.get(key)
        if existing is None or score >= _safe_float(existing.get("overall_score"), 0.0):
            by_book_variant[key] = row

    return by_book_variant, rows


def _resolve_winner_map(selections_path: Path) -> dict[int, dict[str, Any]]:
    payload = _load_json(selections_path, {"selections": {}})
    raw = payload.get("selections", payload) if isinstance(payload, dict) else {}
    out: dict[int, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out

    for key, value in raw.items():
        book = _safe_int(key, 0)
        if book <= 0:
            continue
        if isinstance(value, dict):
            out[book] = {
                "winner": _safe_int(value.get("winner"), 0),
                "score": _safe_float(value.get("score"), 0.0),
                "auto_selected": bool(value.get("auto_selected", True)),
                "confirmed": bool(value.get("confirmed", False)),
            }
        else:
            out[book] = {
                "winner": _safe_int(value, 0),
                "score": 0.0,
                "auto_selected": True,
                "confirmed": False,
            }
    return out


def _load_prompt_map(prompts_path: Path) -> dict[tuple[int, int], str]:
    payload = _load_json(prompts_path, {"books": []})
    out: dict[tuple[int, int], str] = {}
    for book in payload.get("books", []):
        number = _safe_int(book.get("number"), 0)
        for variant in book.get("variants", []):
            variant_id = _safe_int(variant.get("variant_id"), 0)
            if number > 0 and variant_id > 0:
                out[(number, variant_id)] = str(variant.get("prompt", ""))
    return out


def _find_winner_image(output_dir: Path, folder_name: str, variant: int) -> Path | None:
    book_dir = output_dir / folder_name
    variant_dir = book_dir / f"Variant-{variant}"
    if variant_dir.exists():
        jpgs = sorted(variant_dir.glob("*.jpg"))
        if jpgs:
            return jpgs[0]

    root_jpgs = sorted(book_dir.glob("*.jpg"))
    return root_jpgs[0] if root_jpgs else None


def _find_variant_image(output_dir: Path, folder_name: str, book_number: int, variant: int, score_row: dict[str, Any] | None) -> Path | None:
    book_dir = output_dir / folder_name
    variant_dir = book_dir / f"Variant-{variant}"
    if variant_dir.exists():
        jpgs = sorted(variant_dir.glob("*.jpg"))
        if jpgs:
            return jpgs[0]

    if score_row:
        image_path = score_row.get("image_path")
        if isinstance(image_path, str) and image_path:
            candidate = (PROJECT_ROOT / image_path.replace("tmp/generated", "tmp/composited")).with_suffix(".jpg")
            if candidate.exists():
                return candidate

    candidates = sorted((PROJECT_ROOT / "tmp" / "composited" / str(book_number)).rglob(f"variant_{variant}.jpg"))
    return candidates[0] if candidates else None


def _draw_header(img: Image.Image, title: str, subtitle: str = "") -> None:
    draw = ImageDraw.Draw(img)
    w, _ = img.size
    draw.rectangle((0, 0, w, 200), fill=NAVY)
    draw.text((MARGIN, 52), title, fill=GOLD, font=_font(62, bold=True))
    if subtitle:
        draw.text((MARGIN, 128), subtitle, fill=CREAM, font=_font(28))


def _paste_contained(img: Image.Image, source_path: Path, x: int, y: int, w: int, h: int) -> None:
    try:
        source = Image.open(source_path).convert("RGB")
    except Exception:
        return
    ratio = min(w / source.width, h / source.height)
    nw = max(1, int(source.width * ratio))
    nh = max(1, int(source.height * ratio))
    thumb = source.resize((nw, nh), Image.LANCZOS)
    img.paste(thumb, (x + (w - nw) // 2, y + (h - nh) // 2))


def _build_winner_entries(
    *,
    output_dir: Path,
    selections_path: Path,
    quality_path: Path,
    catalog_path: Path,
    prompts_path: Path,
) -> tuple[list[BookWinner], list[dict[str, Any]], dict[tuple[int, int], dict[str, Any]]]:
    catalog = _load_json(catalog_path, [])
    selection_map = _resolve_winner_map(selections_path)
    quality_map, quality_rows = _load_quality_maps(quality_path)
    prompt_map = _load_prompt_map(prompts_path)

    entries: list[BookWinner] = []
    for row in sorted(catalog, key=lambda item: _safe_int(item.get("number"), 0)):
        number = _safe_int(row.get("number"), 0)
        if number <= 0:
            continue
        title = str(row.get("title", ""))
        author = str(row.get("author", ""))
        folder_name = _norm_folder_name(str(row.get("folder_name", "")))

        winner_variant = _safe_int(selection_map.get(number, {}).get("winner"), 0)
        if winner_variant <= 0:
            candidates = [
                (variant, _safe_float(data.get("overall_score"), 0.0))
                for (book, variant), data in quality_map.items()
                if book == number
            ]
            winner_variant = sorted(candidates, key=lambda item: (-item[1], item[0]))[0][0] if candidates else 1

        quality_row = quality_map.get((number, winner_variant), {})
        winner_score = _safe_float(selection_map.get(number, {}).get("score"), 0.0) or _safe_float(quality_row.get("overall_score"), 0.0)
        winner_model = str(quality_row.get("model", "unknown"))
        prompt = prompt_map.get((number, winner_variant), "")
        image_path = _find_winner_image(output_dir, folder_name, winner_variant)

        entries.append(
            BookWinner(
                number=number,
                title=title,
                author=author,
                folder_name=folder_name,
                winner_variant=winner_variant,
                winner_score=winner_score,
                winner_model=winner_model,
                prompt=prompt,
                image_path=image_path,
            )
        )

    return entries, quality_rows, quality_map


def _model_success_rows(quality_rows: list[dict[str, Any]]) -> list[tuple[str, int, int, float]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in quality_rows:
        if not isinstance(row, dict):
            continue
        grouped.setdefault(str(row.get("model", "unknown")), []).append(row)

    out: list[tuple[str, int, int, float]] = []
    for model, rows in grouped.items():
        total = len(rows)
        passed = sum(1 for row in rows if bool(row.get("passed", False)))
        rate = (passed / total) if total else 0.0
        out.append((model, passed, total, rate))
    out.sort(key=lambda item: item[3], reverse=True)
    return out


def _estimate_cost(quality_rows: list[dict[str, Any]]) -> float:
    explicit = [_safe_float(row.get("cost"), -1.0) for row in quality_rows if isinstance(row, dict)]
    valid = [x for x in explicit if x >= 0.0]
    if valid:
        return round(sum(valid), 2)

    model_cost = {
        "flux-2-pro": 0.055,
        "flux-2-schnell": 0.003,
        "gpt-image-1-high": 0.167,
        "gpt-image-1-medium": 0.040,
        "imagen-4-ultra": 0.060,
        "imagen-4-fast": 0.030,
        "nano-banana-pro": 0.067,
        "openai__gpt-image-1": 0.040,
    }
    return round(sum(model_cost.get(str(row.get("model", "unknown")), 0.04) for row in quality_rows if isinstance(row, dict)), 2)


def _generation_range(output_dir: Path) -> tuple[str, str]:
    files = [path for path in output_dir.rglob("*.jpg") if path.is_file()]
    if not files:
        now = datetime.now(timezone.utc).isoformat()
        return now, now
    mtimes = [path.stat().st_mtime for path in files]
    start = datetime.fromtimestamp(min(mtimes), tz=timezone.utc).isoformat()
    end = datetime.fromtimestamp(max(mtimes), tz=timezone.utc).isoformat()
    return start, end


def _cover_page(total_books: int) -> Image.Image:
    img = Image.new("RGB", PAGE_SIZE, NAVY)
    draw = ImageDraw.Draw(img)

    draw.text((MARGIN, 430), "Alexandria Publishing — Cover Collection", fill=GOLD, font=_font(86, bold=True))
    draw.text((MARGIN, 560), f"{total_books} Classical Literature Titles", fill=CREAM, font=_font(52))
    draw.text((MARGIN, 650), f"Generated: {datetime.now().strftime('%B %d, %Y')}", fill=CREAM, font=_font(38))

    draw.rounded_rectangle((MARGIN, 1700, PAGE_SIZE[0] - MARGIN, 2120), radius=24, fill=NAVY_LIGHT)
    draw.text((MARGIN + 40, 1800), "Classical template preserved: navy background, gold ornaments, medallion layout.", fill=CREAM, font=_font(32))
    draw.text((MARGIN + 40, 1860), "Center illustration replaced and optimized across the full title set.", fill=CREAM, font=_font(32))
    return img


def _toc_pages(entries: list[BookWinner], first_book_page: int) -> list[Image.Image]:
    lines = [f"{item.number:>3}. {item.title} — {item.author} .... {first_book_page + idx}" for idx, item in enumerate(entries)]
    per_page = 48
    pages: list[Image.Image] = []

    total = max(1, math.ceil(len(lines) / per_page))
    for page_idx in range(total):
        img = Image.new("RGB", PAGE_SIZE, (248, 245, 236))
        _draw_header(img, "Table of Contents", f"Page {page_idx + 1} of {total}")
        draw = ImageDraw.Draw(img)
        y = 260
        for line in lines[page_idx * per_page : (page_idx + 1) * per_page]:
            draw.text((MARGIN, y), line[:170], fill=INK, font=_font(24))
            y += 42
        pages.append(img)
    return pages


def _winner_page(item: BookWinner) -> Image.Image:
    img = Image.new("RGB", PAGE_SIZE, (250, 246, 236))
    _draw_header(img, item.title, f"Book {item.number}")
    draw = ImageDraw.Draw(img)

    draw.text((MARGIN, 240), item.author, fill=INK, font=_font(42, bold=True))

    image_box = (MARGIN + 20, 330, 2330, 2160)
    draw.rounded_rectangle((image_box[0] - 10, image_box[1] - 10, image_box[2] + 10, image_box[3] + 10), radius=18, fill=WHITE)
    if item.image_path and item.image_path.exists():
        _paste_contained(img, item.image_path, image_box[0], image_box[1], image_box[2] - image_box[0], image_box[3] - image_box[1])

    panel = (2400, 330, PAGE_SIZE[0] - MARGIN, 2160)
    draw.rounded_rectangle(panel, radius=16, fill=NAVY_LIGHT)

    draw.text((panel[0] + 30, panel[1] + 30), "Winner", fill=CREAM, font=_font(34, bold=True))
    draw.text((panel[0] + 30, panel[1] + 90), f"Variant {item.winner_variant} — Sketch: Iconic Scene", fill=CREAM, font=_font(26))

    badge = (panel[0] + 30, panel[1] + 150, panel[0] + 320, panel[1] + 225)
    draw.rounded_rectangle(badge, radius=12, fill=GOLD)
    draw.text((badge[0] + 16, badge[1] + 18), f"Score {item.winner_score:.3f}", fill=NAVY, font=_font(30, bold=True))

    model = item.winner_model.replace("__", "/")
    draw.text((panel[0] + 30, panel[1] + 255), "Model", fill=CREAM, font=_font(22))
    draw.text((panel[0] + 30, panel[1] + 285), model[:36], fill=CREAM, font=_font(26, bold=True))

    draw.text((panel[0] + 30, panel[1] + 365), "Prompt", fill=CREAM, font=_font(22, italic=True))
    wrapped = textwrap.wrap(item.prompt or "Prompt data unavailable.", width=38)
    y = panel[1] + 402
    for line in wrapped[:18]:
        draw.text((panel[0] + 30, y), line, fill=CREAM, font=_font(18, italic=True))
        y += 28

    return img


def _summary_page(entries: list[BookWinner], quality_rows: list[dict[str, Any]], output_dir: Path) -> Image.Image:
    img = Image.new("RGB", PAGE_SIZE, (247, 242, 232))
    _draw_header(img, "Catalog Summary")
    draw = ImageDraw.Draw(img)

    total_books = len(entries)
    total_variants = len(quality_rows)
    avg_winner = mean([item.winner_score for item in entries]) if entries else 0.0
    total_cost = _estimate_cost(quality_rows)
    start_ts, end_ts = _generation_range(output_dir)

    y = 280
    stats = [
        f"Total books: {total_books}",
        f"Total variants generated: {total_variants}",
        f"Average winner quality score: {avg_winner:.3f}",
        f"Estimated generation cost: ${total_cost:.2f}",
        f"Generation date range: {start_ts} to {end_ts}",
    ]
    for row in stats:
        draw.text((MARGIN, y), row, fill=INK, font=_font(32, bold=True))
        y += 58

    draw.text((MARGIN, y + 24), "Models and success rates", fill=NAVY, font=_font(34, bold=True))
    y += 90
    for model, passed, total, rate in _model_success_rows(quality_rows)[:16]:
        draw.text((MARGIN + 10, y), f"{model}: {passed}/{total} passed ({rate:.1%})", fill=INK, font=_font(24))
        y += 38
        if y > PAGE_SIZE[1] - 120:
            break

    return img


def generate_professional_catalog(
    *,
    output_dir: Path,
    selections_path: Path,
    quality_path: Path,
    catalog_path: Path,
    prompts_path: Path,
    catalog_output: Path,
) -> Path:
    entries, quality_rows, _ = _build_winner_entries(
        output_dir=output_dir,
        selections_path=selections_path,
        quality_path=quality_path,
        catalog_path=catalog_path,
        prompts_path=prompts_path,
    )

    toc_estimate = max(1, math.ceil(len(entries) / 48))
    first_book_page = 2 + toc_estimate

    pages: list[Image.Image] = [_cover_page(len(entries))]
    pages.extend(_toc_pages(entries, first_book_page))
    pages.extend([_winner_page(item) for item in entries])
    pages.append(_summary_page(entries, quality_rows, output_dir))

    _save_pdf_pages(pages, catalog_output)
    return catalog_output


def generate_contact_sheet(
    *,
    output_dir: Path,
    selections_path: Path,
    quality_path: Path,
    catalog_path: Path,
    prompts_path: Path,
    catalog_output: Path,
) -> Path:
    entries, _, _ = _build_winner_entries(
        output_dir=output_dir,
        selections_path=selections_path,
        quality_path=quality_path,
        catalog_path=catalog_path,
        prompts_path=prompts_path,
    )

    img = Image.new("RGB", PAGE_SIZE, (251, 248, 240))
    _draw_header(img, "Alexandria Contact Sheet", "All winning covers in one page")
    draw = ImageDraw.Draw(img)

    cols = 10
    rows = 10
    grid_left = MARGIN
    grid_top = 260
    grid_w = PAGE_SIZE[0] - (2 * MARGIN)
    grid_h = PAGE_SIZE[1] - 300
    cell_w = grid_w / cols
    cell_h = grid_h / rows

    thumb_w = 200
    thumb_h = int(thumb_w * 2777 / 3784)

    for idx, item in enumerate(entries[:100]):
        r = idx // cols
        c = idx % cols
        x = int(grid_left + (c * cell_w) + (cell_w - thumb_w) / 2)
        y = int(grid_top + (r * cell_h) + (cell_h - thumb_h) / 2 - 10)

        draw.rounded_rectangle((x - 2, y - 2, x + thumb_w + 2, y + thumb_h + 2), radius=4, fill=WHITE)
        if item.image_path and item.image_path.exists():
            _paste_contained(img, item.image_path, x, y, thumb_w, thumb_h)

        label = str(item.number)
        lw = draw.textlength(label, font=_font(15, bold=True))
        draw.text((x + (thumb_w - lw) / 2, y + thumb_h + 6), label, fill=INK, font=_font(15, bold=True))

    _save_pdf_pages([img], catalog_output)
    return catalog_output


def generate_all_variants_catalog(
    *,
    output_dir: Path,
    selections_path: Path,
    quality_path: Path,
    catalog_path: Path,
    prompts_path: Path,
    catalog_output: Path,
) -> Path:
    entries, _, quality_map = _build_winner_entries(
        output_dir=output_dir,
        selections_path=selections_path,
        quality_path=quality_path,
        catalog_path=catalog_path,
        prompts_path=prompts_path,
    )

    pages: list[Image.Image] = []
    for item in entries:
        img = Image.new("RGB", PAGE_SIZE, (249, 245, 236))
        _draw_header(img, f"{item.number}. {item.title}", item.author)
        draw = ImageDraw.Draw(img)

        left = MARGIN
        top = 280
        right = PAGE_SIZE[0] - MARGIN
        bottom = PAGE_SIZE[1] - 120

        cols = 5
        cell_w = (right - left) / cols
        cell_h = bottom - top

        for variant in range(1, 6):
            x = int(left + (variant - 1) * cell_w + 10)
            y = int(top + 10)
            w = int(cell_w - 20)
            h = int(cell_h - 110)

            score_row = quality_map.get((item.number, variant))
            score = _safe_float((score_row or {}).get("overall_score"), 0.0)
            model = str((score_row or {}).get("model", "unknown")).replace("__", "/")
            image_path = _find_variant_image(output_dir, item.folder_name, item.number, variant, score_row)

            draw.rounded_rectangle((x - 4, y - 4, x + w + 4, y + h + 4), radius=8, fill=WHITE)
            if image_path and image_path.exists():
                _paste_contained(img, image_path, x, y, w, h)

            winner = variant == item.winner_variant
            border = GOLD if winner else (130, 140, 158)
            width = 5 if winner else 2
            draw.rounded_rectangle((x - 4, y - 4, x + w + 4, y + h + 4), radius=8, outline=border, width=width)

            heading = f"Variant {variant}{'  * Winner' if winner else ''}"
            draw.text((x, y + h + 12), heading, fill=INK, font=_font(22, bold=True))
            draw.text((x, y + h + 46), f"Score: {score:.3f}", fill=INK, font=_font(18))
            draw.text((x, y + h + 74), f"Model: {model[:26]}", fill=INK, font=_font(16))

        pages.append(img)

    _save_pdf_pages(pages, catalog_output)
    return catalog_output


def _save_pdf_pages(pages: list[Image.Image], output_path: Path) -> None:
    if not pages:
        raise ValueError("No pages generated")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rgb_pages = [page.convert("RGB") for page in pages]
    rgb_pages[0].save(output_path, format="PDF", save_all=True, append_images=rgb_pages[1:], resolution=300)


def generate_catalog(
    *,
    output_dir: Path,
    selections: Path,
    quality_data: Path,
    catalog_output: Path,
    contact_sheet: bool,
    all_variants: bool,
    catalog_path: Path,
    prompts_path: Path,
) -> Path:
    catalog_output.parent.mkdir(parents=True, exist_ok=True)

    if contact_sheet:
        return generate_contact_sheet(
            output_dir=output_dir,
            selections_path=selections,
            quality_path=quality_data,
            catalog_path=catalog_path,
            prompts_path=prompts_path,
            catalog_output=catalog_output,
        )

    if all_variants:
        return generate_all_variants_catalog(
            output_dir=output_dir,
            selections_path=selections,
            quality_path=quality_data,
            catalog_path=catalog_path,
            prompts_path=prompts_path,
            catalog_output=catalog_output,
        )

    return generate_professional_catalog(
        output_dir=output_dir,
        selections_path=selections,
        quality_path=quality_data,
        catalog_path=catalog_path,
        prompts_path=prompts_path,
        catalog_output=catalog_output,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Alexandria catalog PDFs")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--selections", type=Path, default=None)
    parser.add_argument("--quality-data", type=Path, default=None)
    parser.add_argument("--catalog-path", type=Path, default=None, help="Override catalog JSON path")
    parser.add_argument("--prompts", type=Path, default=None)
    parser.add_argument("--catalog-output", type=Path, default=None)
    parser.add_argument("--mode", choices=["catalog", "contact_sheet", "all_variants"], default=None, help="Legacy mode selector")
    parser.add_argument("--contact-sheet", action="store_true")
    parser.add_argument("--all-variants", action="store_true")
    args = parser.parse_args()
    runtime = config.get_config(args.catalog)

    contact_sheet = bool(args.contact_sheet)
    all_variants = bool(args.all_variants)
    if args.mode:
        if args.mode == "contact_sheet":
            contact_sheet = True
            all_variants = False
        elif args.mode == "all_variants":
            all_variants = True
            contact_sheet = False
        else:
            contact_sheet = False
            all_variants = False

    output_dir = args.output_dir or runtime.output_dir
    selections = args.selections or config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    quality_data = args.quality_data or config.quality_scores_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    catalog_path = args.catalog_path or runtime.book_catalog_path
    prompts_path = args.prompts or runtime.prompts_path
    default_catalog_output = output_dir / (
        "Alexandria_Contact_Sheet.pdf"
        if contact_sheet
        else ("Alexandria_All_Variants_Catalog.pdf" if all_variants else "Alexandria_Cover_Catalog.pdf")
    )
    catalog_output = args.catalog_output or default_catalog_output

    output = generate_catalog(
        output_dir=output_dir,
        selections=selections,
        quality_data=quality_data,
        catalog_output=catalog_output,
        contact_sheet=contact_sheet,
        all_variants=all_variants,
        catalog_path=catalog_path,
        prompts_path=prompts_path,
    )

    logger.info(
        "Catalog generated: %s",
        json.dumps(
            {
                "ok": True,
                "output": str(output),
                "contact_sheet": contact_sheet,
                "all_variants": all_variants,
            },
            ensure_ascii=False,
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
