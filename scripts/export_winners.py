#!/usr/bin/env python3
"""Export winning covers in flat or organized formats."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from src import config
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    from src import config  # type: ignore
    from src.logger import get_logger  # type: ignore

DEFAULT_SELECTIONS = PROJECT_ROOT / "data" / "winner_selections.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "Output Covers"
DEFAULT_DEST_DIR = PROJECT_ROOT / "Winners"
DEFAULT_CATALOG = PROJECT_ROOT / "config" / "book_catalog.json"
logger = get_logger(__name__)


def _book_number_from_folder(name: str) -> int | None:
    token = name.split(".", 1)[0].strip()
    if token.isdigit():
        return int(token)
    return None


def _load_winners(path: Path, *, include_unconfirmed: bool = False) -> dict[int, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload.get("selections", payload) if isinstance(payload, dict) else {}
    out: dict[int, int] = {}
    if not isinstance(raw, dict):
        return out

    for key, value in raw.items():
        try:
            book = int(str(key))
        except ValueError:
            continue

        if isinstance(value, dict):
            if (not include_unconfirmed) and (not bool(value.get("confirmed", False))):
                continue
            winner = int(value.get("winner", 0) or 0)
        else:
            winner = int(value or 0)

        if winner > 0:
            out[book] = winner
    return out


def _load_catalog_titles(path: Path) -> dict[int, str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: dict[int, str] = {}
    if not isinstance(payload, list):
        return out
    for row in payload:
        try:
            number = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        out[number] = str(row.get("title", f"Book {number}"))
    return out


def _winner_file_candidates(book_dir: Path, winner_variant: int) -> list[Path]:
    variant_dir = book_dir / f"Variant-{winner_variant}"
    if variant_dir.exists():
        return sorted([p for p in variant_dir.glob("*") if p.is_file() and p.suffix.lower() in {".jpg", ".pdf", ".ai"}])
    # After archive/flatten workflow, winner files may live at book root.
    return sorted([p for p in book_dir.glob("*") if p.is_file() and p.suffix.lower() in {".jpg", ".pdf", ".ai"}])


def export_winners(
    *,
    selections_path: Path,
    output_dir: Path,
    destination: Path,
    export_format: str,
    catalog_path: Path,
    include_unconfirmed: bool = False,
) -> dict[str, Any]:
    winners = _load_winners(selections_path, include_unconfirmed=include_unconfirmed)
    titles = _load_catalog_titles(catalog_path)

    destination.mkdir(parents=True, exist_ok=True)
    exported_files = 0
    exported_books = 0
    skipped_books: list[int] = []

    for book_dir in sorted(p for p in output_dir.iterdir() if p.is_dir() and p.name != "Archive"):
        book = _book_number_from_folder(book_dir.name)
        if book is None:
            continue
        winner_variant = winners.get(book)
        if not winner_variant:
            continue

        files = _winner_file_candidates(book_dir, winner_variant)
        if not files:
            skipped_books.append(book)
            continue

        exported_books += 1

        if export_format == "flat":
            # Only JPG for flat export.
            jpg = next((p for p in files if p.suffix.lower() == ".jpg"), None)
            if not jpg:
                skipped_books.append(book)
                continue
            title = titles.get(book, f"Book {book}")
            safe_title = "".join(ch for ch in title if ch not in '/\\:*?"<>|').strip()
            target = destination / f"{book:02d} - {safe_title}.jpg"
            shutil.copy2(jpg, target)
            exported_files += 1
            continue

        # organized format: keep .ai/.jpg/.pdf for each winner
        target_dir = destination / book_dir.name
        target_dir.mkdir(parents=True, exist_ok=True)
        for file_path in files:
            shutil.copy2(file_path, target_dir / file_path.name)
            exported_files += 1

    return {
        "format": export_format,
        "exported_books": exported_books,
        "exported_files": exported_files,
        "skipped_books": skipped_books,
        "destination": str(destination),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export winners in flat or organized format.")
    parser.add_argument("--catalog-id", "--catalog", dest="catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--selections", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--destination", type=Path, default=DEFAULT_DEST_DIR)
    parser.add_argument("--catalog-path", type=Path, default=None, help="Override book catalog JSON path")
    parser.add_argument("--format", choices=["flat", "organized"], default="organized")
    parser.add_argument("--include-unconfirmed", action="store_true")
    args = parser.parse_args()
    runtime = config.get_config(args.catalog)
    selections_path = args.selections or config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    output_dir = args.output_dir or runtime.output_dir
    catalog_path = args.catalog_path or runtime.book_catalog_path

    summary = export_winners(
        selections_path=selections_path,
        output_dir=output_dir,
        destination=args.destination,
        export_format=args.format,
        catalog_path=catalog_path,
        include_unconfirmed=args.include_unconfirmed,
    )
    logger.info("Export summary: %s", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
