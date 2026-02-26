#!/usr/bin/env python3
"""Prompt A/B testing utility."""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import quality_gate
from src.logger import get_logger

logger = get_logger(__name__)
CATALOG_PATH = PROJECT_ROOT / "config" / "book_catalog.json"
OUTPUT_PATH = PROJECT_ROOT / "data" / "ab_test_results.json"
TMP_DIR = PROJECT_ROOT / "tmp" / "ab_test"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _parse_books(raw: str) -> list[int]:
    values: set[int] = set()
    for token in raw.split(","):
        text = token.strip()
        if not text:
            continue
        if "-" in text:
            a, b = text.split("-", 1)
            for n in range(min(int(a), int(b)), max(int(a), int(b)) + 1):
                values.add(n)
        else:
            values.add(int(text))
    return sorted(values)


def _book_title_map() -> dict[int, str]:
    rows = _load_json(CATALOG_PATH, [])
    out: dict[int, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            number = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        out[number] = str(row.get("title", f"Book {number}"))
    return out


def _render(path: Path, *, seed: int, label: str) -> None:
    rnd = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", (1024, 1024), (24, 40, 69))
    draw = ImageDraw.Draw(img)
    for _ in range(80):
        x1 = rnd.randint(0, 1023)
        y1 = rnd.randint(0, 1023)
        x2 = min(1023, x1 + rnd.randint(30, 320))
        y2 = min(1023, y1 + rnd.randint(30, 320))
        color = (rnd.randint(70, 230), rnd.randint(60, 220), rnd.randint(50, 200))
        if rnd.random() > 0.5:
            draw.ellipse((x1, y1, x2, y2), fill=color)
        else:
            draw.rectangle((x1, y1, x2, y2), fill=color)
    draw.text((120, 950), label[:40], fill=(245, 230, 200))
    img.save(path, format="PNG")


def _score_set(*, books: list[int], prompt_file: Path, variants: int, group: str) -> list[dict[str, Any]]:
    payload = _load_json(prompt_file, {})
    title_map = _book_title_map()
    rows: list[dict[str, Any]] = []

    for book in books:
        title = title_map.get(book, f"Book {book}")
        for variant in range(1, variants + 1):
            img_path = TMP_DIR / group / str(book) / f"variant_{variant}.png"
            _render(img_path, seed=hash((group, book, variant)) & 0xFFFFFFFF, label=f"{group}:{title}")
            score = quality_gate.score_image(
                img_path,
                threshold=0.7,
                book_number=book,
                variant_id=variant,
                model="ab-test-synthetic",
            )
            rows.append(
                {
                    "group": group,
                    "book": book,
                    "book_title": title,
                    "variant": variant,
                    "prompt_source": str(prompt_file),
                    "prompt_meta": payload.get("version") if isinstance(payload, dict) else None,
                    "score": score.overall_score,
                    "image_path": str(img_path),
                }
            )
    return rows


def run_ab_test(*, books: list[int], prompt_a: Path, prompt_b: Path, variants: int) -> dict[str, Any]:
    scores_a = _score_set(books=books, prompt_file=prompt_a, variants=variants, group="A")
    scores_b = _score_set(books=books, prompt_file=prompt_b, variants=variants, group="B")

    avg_a = sum(row["score"] for row in scores_a) / max(1, len(scores_a))
    avg_b = sum(row["score"] for row in scores_b) / max(1, len(scores_b))

    per_book = []
    for book in books:
        a_rows = [row for row in scores_a if row["book"] == book]
        b_rows = [row for row in scores_b if row["book"] == book]
        a_avg = sum(row["score"] for row in a_rows) / max(1, len(a_rows))
        b_avg = sum(row["score"] for row in b_rows) / max(1, len(b_rows))
        per_book.append(
            {
                "book": book,
                "avg_a": round(a_avg, 4),
                "avg_b": round(b_avg, 4),
                "winner": "A" if a_avg >= b_avg else "B",
            }
        )

    effect = abs(avg_a - avg_b)
    confidence = min(0.99, 0.50 + (effect * 2.5) + (len(books) * variants / 100.0))

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "books": books,
        "variants_per_group": variants,
        "prompt_a": str(prompt_a),
        "prompt_b": str(prompt_b),
        "average_score_a": round(avg_a, 4),
        "average_score_b": round(avg_b, 4),
        "overall_winner": "A" if avg_a >= avg_b else "B",
        "confidence": round(confidence, 4),
        "per_book": per_book,
        "scores_a": scores_a,
        "scores_b": scores_b,
    }
    OUTPUT_PATH.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="A/B test two prompt template files")
    parser.add_argument("--books", required=True, help="Books list, e.g. 1,10,20")
    parser.add_argument("--prompt-a", type=Path, required=True)
    parser.add_argument("--prompt-b", type=Path, required=True)
    parser.add_argument("--variants", type=int, default=3)
    args = parser.parse_args()

    result = run_ab_test(
        books=_parse_books(args.books),
        prompt_a=args.prompt_a,
        prompt_b=args.prompt_b,
        variants=args.variants,
    )
    logger.info("A/B test summary: %s", json.dumps({k: result[k] for k in ['average_score_a','average_score_b','overall_winner','confidence']}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
