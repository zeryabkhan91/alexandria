#!/usr/bin/env python3
"""Auto-select winning variant per book from quality scores."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
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

DEFAULT_INPUT_DIR = PROJECT_ROOT / "Output Covers"
DEFAULT_QUALITY_DATA = config.quality_scores_path()
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "winner_selections.json"
logger = get_logger(__name__)


def _default_output_path(runtime: config.Config) -> Path:
    return config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)


def _book_number_from_folder(name: str) -> int | None:
    token = name.split(".", 1)[0].strip()
    if not token.isdigit():
        return None
    return int(token)


def _load_quality_scores(path: Path) -> dict[int, dict[int, float]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("scores", []) if isinstance(payload, dict) else []

    by_book: dict[int, dict[int, float]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            book = int(row.get("book_number", 0))
            variant = int(row.get("variant_id", 0))
            score = float(row.get("overall_score", 0.0))
        except (TypeError, ValueError):
            continue
        if book <= 0 or variant <= 0:
            continue
        variants = by_book.setdefault(book, {})
        # Keep the strongest score for this book/variant.
        variants[variant] = max(variants.get(variant, 0.0), score)

    return by_book


def _candidate_priority(variant: int) -> int:
    # Prefer sketch variants (1-3) over wildcard variants (4-5).
    return 0 if variant in {1, 2, 3} else 1


def _pick_winner(variant_scores: dict[int, float]) -> tuple[int, float]:
    ordered = sorted(
        variant_scores.items(),
        key=lambda item: (
            -item[1],  # higher score first
            _candidate_priority(item[0]),  # sketch preference
            item[0],  # lower variant number as deterministic tie-breaker
        ),
    )
    winner, score = ordered[0]
    return winner, score


def auto_select_winners(*, input_dir: Path, quality_data: Path, output_path: Path) -> dict[str, Any]:
    quality_by_book = _load_quality_scores(quality_data)

    selections: dict[str, dict[str, Any]] = {}
    winner_scores: list[float] = []

    for folder in sorted(p for p in input_dir.iterdir() if p.is_dir() and p.name != "Archive"):
        book_number = _book_number_from_folder(folder.name)
        if book_number is None:
            continue

        variant_scores = quality_by_book.get(book_number, {})
        # Fall back to Variant-1 when quality data is missing.
        if not variant_scores:
            winner_variant = 1
            winner_score = 0.0
        else:
            winner_variant, winner_score = _pick_winner(variant_scores)

        selections[str(book_number)] = {
            "winner": int(winner_variant),
            "score": round(float(winner_score), 4),
            "auto_selected": True,
            "confirmed": False,
        }
        winner_scores.append(float(winner_score))

    payload = {
        "selections": selections,
        "selection_date": datetime.now(timezone.utc).isoformat(),
        "total_books": len(selections),
        "average_winner_score": round(mean(winner_scores), 4) if winner_scores else 0.0,
        "min_winner_score": round(min(winner_scores), 4) if winner_scores else 0.0,
        "max_winner_score": round(max(winner_scores), 4) if winner_scores else 0.0,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Auto-select winning variants by quality score.")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--input", type=Path, default=None, help="Output Covers root")
    parser.add_argument("--quality-data", type=Path, default=None, help="quality_scores.json path")
    parser.add_argument("--output", type=Path, default=None, help="winner selections output path")
    args = parser.parse_args()
    runtime = config.get_config(args.catalog)
    input_dir = args.input or runtime.output_dir
    quality_data = args.quality_data or config.quality_scores_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    output_path = args.output or _default_output_path(runtime)

    summary = auto_select_winners(input_dir=input_dir, quality_data=quality_data, output_path=output_path)
    logger.info(
        "Selected winners for %s books | avg: %s | min: %s | max: %s",
        summary["total_books"],
        summary["average_winner_score"],
        summary["min_winner_score"],
        summary["max_winner_score"],
    )
    logger.info("Wrote winner selections to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
