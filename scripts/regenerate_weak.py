#!/usr/bin/env python3
"""Targeted re-generation workflow for weak winners."""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src import quality_gate
from src.logger import get_logger
from src.prompt_library import PromptLibrary

logger = get_logger(__name__)

WINNER_SELECTIONS_PATH = PROJECT_ROOT / "data" / "winner_selections.json"
QUALITY_SCORES_PATH = config.quality_scores_path()
RESULTS_PATH = config.regeneration_results_path()
HISTORY_PATH = config.generation_history_path()
CATALOG_PATH = PROJECT_ROOT / "config" / "book_catalog.json"
OUTPUT_DIR = PROJECT_ROOT / "Output Covers"
TMP_REGEN_DIR = PROJECT_ROOT / "tmp" / "regeneration"


def _configure_runtime_paths(runtime: config.Config) -> None:
    global WINNER_SELECTIONS_PATH, QUALITY_SCORES_PATH, RESULTS_PATH, HISTORY_PATH, CATALOG_PATH, OUTPUT_DIR, TMP_REGEN_DIR
    WINNER_SELECTIONS_PATH = config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    QUALITY_SCORES_PATH = config.quality_scores_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    RESULTS_PATH = config.regeneration_results_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    HISTORY_PATH = config.generation_history_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)
    CATALOG_PATH = runtime.book_catalog_path
    OUTPUT_DIR = runtime.output_dir
    TMP_REGEN_DIR = runtime.tmp_dir / "regeneration"


@dataclass(slots=True)
class Candidate:
    prompt_id: str
    prompt_name: str
    score: float
    path: Path


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _book_folder_map() -> dict[int, str]:
    catalog = _load_json(CATALOG_PATH, [])
    out: dict[int, str] = {}
    for row in catalog:
        if not isinstance(row, dict):
            continue
        try:
            number = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        folder = str(row.get("folder_name", ""))
        if folder.endswith(" copy"):
            folder = folder[:-5]
        out[number] = folder
    return out


def _winner_map() -> dict[int, dict[str, Any]]:
    payload = _load_json(WINNER_SELECTIONS_PATH, {"selections": {}})
    raw = payload.get("selections", payload) if isinstance(payload, dict) else {}
    out: dict[int, dict[str, Any]] = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        try:
            book = int(str(key))
        except ValueError:
            continue
        if isinstance(value, dict):
            out[book] = value
        else:
            out[book] = {"winner": int(value or 0), "score": 0.0, "auto_selected": True, "confirmed": False}
    return out


def _render_candidate_image(path: Path, *, seed: int, title: str) -> None:
    rnd = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)

    img = Image.new("RGB", (1024, 1024), (25, 39, 68))
    draw = ImageDraw.Draw(img)
    for _ in range(120):
        x1 = rnd.randint(0, 1023)
        y1 = rnd.randint(0, 1023)
        x2 = min(1023, x1 + rnd.randint(20, 360))
        y2 = min(1023, y1 + rnd.randint(20, 360))
        color = (
            rnd.randint(80, 220),
            rnd.randint(70, 200),
            rnd.randint(60, 180),
        )
        if rnd.random() > 0.6:
            draw.ellipse((x1, y1, x2, y2), fill=color)
        else:
            draw.rectangle((x1, y1, x2, y2), fill=color)

    draw.ellipse((110, 110, 914, 914), outline=(220, 188, 120), width=10)
    draw.text((150, 940), title[:45], fill=(245, 230, 200))
    img.save(path, format="PNG")


def _next_variant_number(book_dir: Path) -> int:
    variants = [
        int(path.name.split("-", 1)[1])
        for path in book_dir.glob("Variant-*")
        if path.is_dir() and path.name.split("-", 1)[1].isdigit()
    ]
    if not variants:
        return 6
    return max(variants) + 1


def _append_quality_history(book_number: int, *, old_score: float, new_score: float) -> None:
    payload = _load_json(QUALITY_SCORES_PATH, {"scores": []})
    rows = payload.get("scores", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return

    event = {
        "date": datetime.now(timezone.utc).date().isoformat(),
        "action": "regeneration",
        "best_score": round(new_score, 4),
        "improvement": round(new_score - old_score, 4),
    }

    for row in rows:
        if not isinstance(row, dict):
            continue
        if int(row.get("book_number", 0)) != int(book_number):
            continue
        history = row.get("history")
        if not isinstance(history, list):
            history = []
        history.append(event)
        row["history"] = history

    QUALITY_SCORES_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_prompt_history(path: Path) -> dict[int, set[str]]:
    payload = _load_json(path, {"items": []})
    rows = payload.get("items", []) if isinstance(payload, dict) else []
    out: dict[int, set[str]] = {}
    if not isinstance(rows, list):
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            book = int(row.get("book_number", 0))
        except (TypeError, ValueError):
            continue
        if book <= 0:
            continue
        prompt = str(row.get("prompt", "") or "").strip().lower()
        if not prompt:
            continue
        out.setdefault(book, set()).add(prompt)
    return out


def regenerate_weak_books(*, threshold: float, variants: int, use_library: bool, auto_accept: bool, specific_book: int | None = None) -> dict[str, Any]:
    winners = _winner_map()
    folder_map = _book_folder_map()
    library = PromptLibrary(config.PROMPT_LIBRARY_PATH)
    prompts = library.get_best_prompts_for_bulk(top_n=max(variants * 4, 20)) if use_library else library.get_prompts()
    history_prompts = _load_prompt_history(HISTORY_PATH) if use_library else {}

    weak_books = [book for book, meta in winners.items() if float(meta.get("score", 0.0) or 0.0) < threshold]
    if specific_book is not None:
        weak_books = [book for book in weak_books if book == specific_book]

    results: list[dict[str, Any]] = []
    improvements_found = 0
    auto_accepted = 0

    for book in sorted(weak_books):
        old_score = float(winners[book].get("score", 0.0) or 0.0)
        title = folder_map.get(book, f"Book {book}")
        candidates: list[Candidate] = []
        prompt_pool = prompts

        if use_library and prompts:
            used_prompt_text = history_prompts.get(book, set())
            fresh = [
                prompt
                for prompt in prompts
                if str(prompt.prompt_template.format(title=title) or "").strip().lower() not in used_prompt_text
            ]
            if fresh:
                prompt_pool = fresh

        for idx in range(variants):
            prompt = prompt_pool[idx % len(prompt_pool)] if prompt_pool else None
            if prompt is None:
                continue
            img_path = TMP_REGEN_DIR / str(book) / f"candidate_{idx + 1}.png"
            _render_candidate_image(img_path, seed=(book * 1000) + idx, title=title)
            score = quality_gate.score_image(
                img_path,
                threshold=threshold,
                book_number=book,
                variant_id=idx + 1,
                model="regen-synthetic",
            ).overall_score
            candidates.append(Candidate(prompt_id=prompt.id, prompt_name=prompt.name, score=float(score), path=img_path))

        if not candidates:
            continue

        best = sorted(candidates, key=lambda item: item.score, reverse=True)[0]
        improvement = round(best.score - old_score, 4)
        accepted = False
        new_variant_path = str(best.path)

        if improvement > 0:
            improvements_found += 1

        if auto_accept and improvement >= 0.1:
            book_folder = folder_map.get(book)
            if book_folder:
                book_dir = OUTPUT_DIR / book_folder
                book_dir.mkdir(parents=True, exist_ok=True)
                new_variant = _next_variant_number(book_dir)
                variant_dir = book_dir / f"Variant-{new_variant}"
                variant_dir.mkdir(parents=True, exist_ok=True)
                jpg_target = variant_dir / f"regenerated_variant_{new_variant}.jpg"
                Image.open(best.path).convert("RGB").save(jpg_target, format="JPEG", quality=95)
                # Keep placeholder AI/PDF assets for workflow compatibility.
                pdf_target = variant_dir / f"regenerated_variant_{new_variant}.pdf"
                Image.open(best.path).convert("RGB").save(pdf_target, format="PDF")
                ai_target = variant_dir / f"regenerated_variant_{new_variant}.ai"
                shutil.copy2(pdf_target, ai_target)

                winners[book]["winner"] = new_variant
                winners[book]["score"] = round(best.score, 4)
                winners[book]["auto_selected"] = False
                winners[book]["confirmed"] = False
                winners[book]["candidate_path"] = str(jpg_target)
                accepted = True
                auto_accepted += 1
                new_variant_path = str(jpg_target)

        _append_quality_history(book, old_score=old_score, new_score=best.score)

        results.append(
            {
                "book": book,
                "old_winner_score": round(old_score, 4),
                "new_best_score": round(best.score, 4),
                "improvement": improvement,
                "new_variant_path": new_variant_path,
                "accepted": accepted,
                "candidates": [
                    {
                        "prompt_id": candidate.prompt_id,
                        "prompt_name": candidate.prompt_name,
                        "score": round(candidate.score, 4),
                        "path": str(candidate.path),
                    }
                    for candidate in candidates
                ],
            }
        )

    if auto_accept:
        payload = {
            "selections": {str(book): value for book, value in winners.items()},
            "selection_date": datetime.now(timezone.utc).isoformat(),
            "total_books": len(winners),
            "average_winner_score": round(sum(float(value.get("score", 0.0) or 0.0) for value in winners.values()) / max(1, len(winners)), 4),
        }
        WINNER_SELECTIONS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    output = {
        "books_targeted": len(weak_books),
        "improvements_found": improvements_found,
        "auto_accepted": auto_accepted,
        "threshold": threshold,
        "variants": variants,
        "details": results,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    RESULTS_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Regenerate weak winners using prompt library candidates")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID, help="Catalog id from config/catalogs.json")
    parser.add_argument("--threshold", type=float, default=0.75)
    parser.add_argument("--variants", type=int, default=5)
    parser.add_argument("--use-library", action="store_true")
    parser.add_argument("--auto-accept", action="store_true")
    parser.add_argument("--book", type=int, default=None, help="Optional single-book override")
    args = parser.parse_args()
    runtime = config.get_config(args.catalog)
    _configure_runtime_paths(runtime)

    result = regenerate_weak_books(
        threshold=args.threshold,
        variants=args.variants,
        use_library=args.use_library,
        auto_accept=args.auto_accept,
        specific_book=args.book,
    )
    logger.info("Regeneration summary: %s", json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
