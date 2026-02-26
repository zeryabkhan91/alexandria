#!/usr/bin/env python3
"""Archive non-winning variants while preserving winners."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
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
DEFAULT_LOG_PATH = PROJECT_ROOT / "data" / "archive_log.json"
logger = get_logger(__name__)


def _book_number_from_folder(name: str) -> int | None:
    token = name.split(".", 1)[0].strip()
    if token.isdigit():
        return int(token)
    return None


def _load_selection_map(path: Path, *, include_unconfirmed: bool = False) -> dict[int, int]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw = payload.get("selections", payload) if isinstance(payload, dict) else {}

    winners: dict[int, int] = {}
    if not isinstance(raw, dict):
        return winners

    for key, value in raw.items():
        try:
            book = int(str(key).strip())
        except ValueError:
            continue

        if isinstance(value, dict):
            if (not include_unconfirmed) and (not bool(value.get("confirmed", False))):
                continue
            winner = int(value.get("winner", 0) or 0)
        else:
            winner = int(value or 0)

        if winner > 0:
            winners[book] = winner
    return winners


def _variant_number_from_name(name: str) -> int | None:
    if not name.startswith("Variant-"):
        return None
    token = name.split("-", 1)[1].strip()
    if token.isdigit():
        return int(token)
    return None


def _dir_size_bytes(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def archive_non_winners(
    *,
    selections_path: Path,
    output_dir: Path,
    archive_dir: Path,
    log_path: Path,
    dry_run: bool = False,
    include_unconfirmed: bool = False,
) -> dict[str, Any]:
    winners = _load_selection_map(selections_path, include_unconfirmed=include_unconfirmed)

    moved_variant_dirs = 0
    moved_files = 0
    kept_files = 0
    reclaimed_bytes = 0
    operations: list[dict[str, Any]] = []

    for book_dir in sorted(p for p in output_dir.iterdir() if p.is_dir() and p.name != "Archive"):
        book = _book_number_from_folder(book_dir.name)
        if book is None:
            continue

        winner_variant = winners.get(book)
        if not winner_variant:
            continue

        variant_dirs = sorted([p for p in book_dir.iterdir() if p.is_dir() and p.name.startswith("Variant-")])
        if not variant_dirs:
            # Already flattened/archived previously.
            continue

        winner_dir = None
        for variant_dir in variant_dirs:
            variant_num = _variant_number_from_name(variant_dir.name)
            if variant_num is None:
                continue

            if variant_num == winner_variant:
                winner_dir = variant_dir
                continue

            target = archive_dir / book_dir.name / variant_dir.name
            size = _dir_size_bytes(variant_dir)
            file_count = len([f for f in variant_dir.rglob("*") if f.is_file()])

            operations.append(
                {
                    "book_number": book,
                    "action": "archive_variant",
                    "source": str(variant_dir),
                    "target": str(target),
                    "files": file_count,
                    "bytes": size,
                }
            )
            moved_variant_dirs += 1
            moved_files += file_count
            reclaimed_bytes += size

            if not dry_run:
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
                    target = target.with_name(f"{target.name}__{suffix}")
                shutil.move(str(variant_dir), str(target))

        if not winner_dir or not winner_dir.exists():
            continue

        # Flatten winner files into book root.
        winner_files = sorted([p for p in winner_dir.glob("*") if p.is_file()])
        for source_file in winner_files:
            target_file = book_dir / source_file.name
            operations.append(
                {
                    "book_number": book,
                    "action": "flatten_winner_file",
                    "source": str(source_file),
                    "target": str(target_file),
                    "files": 1,
                    "bytes": source_file.stat().st_size,
                }
            )
            kept_files += 1
            if not dry_run:
                if target_file.exists():
                    target_file.unlink()
                shutil.move(str(source_file), str(target_file))

        if not dry_run and winner_dir.exists():
            try:
                winner_dir.rmdir()
            except OSError:
                pass

    summary = {
        "selection_date": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "books_with_confirmed_winners": len(winners),
        "moved_variant_dirs": moved_variant_dirs,
        "moved_files": moved_files,
        "kept_files": kept_files,
        "disk_space_freed_bytes": reclaimed_bytes,
        "disk_space_freed_mb": round(reclaimed_bytes / (1024 * 1024), 3),
        "operations": operations,
    }

    if not dry_run:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Archive non-winning variants (move, never delete).")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--selections", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--archive-dir", type=Path, default=None, help="Archive destination (default: <output-dir>/Archive)")
    parser.add_argument("--log-path", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-unconfirmed", action="store_true")
    args = parser.parse_args()
    runtime = config.get_config(args.catalog)
    selections_path = args.selections or config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    output_dir = args.output_dir or runtime.output_dir
    log_path = args.log_path or config.archive_log_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    archive_dir = args.archive_dir or (output_dir / "Archive")

    summary = archive_non_winners(
        selections_path=selections_path,
        output_dir=output_dir,
        archive_dir=archive_dir,
        log_path=log_path,
        dry_run=args.dry_run,
        include_unconfirmed=args.include_unconfirmed,
    )
    logger.info("Archive summary: %s", json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
