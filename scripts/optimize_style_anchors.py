#!/usr/bin/env python3
"""Optimize style anchors by synthetic score probing."""

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
from src.prompt_library import PromptLibrary, StyleAnchor

logger = get_logger(__name__)
CATALOG_PATH = PROJECT_ROOT / "config" / "book_catalog.json"
RESULTS_PATH = PROJECT_ROOT / "data" / "style_anchor_optimization.json"
TMP_DIR = PROJECT_ROOT / "tmp" / "anchor_optimization"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _sample_books(count: int) -> list[tuple[int, str]]:
    rows = _load_json(CATALOG_PATH, [])
    pairs = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            number = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        pairs.append((number, str(row.get("title", f"Book {number}"))))
    return pairs[:count]


def _render_candidate(path: Path, *, seed: int, label: str) -> None:
    rnd = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", (1024, 1024), (24, 38, 66))
    draw = ImageDraw.Draw(img)
    for _ in range(90):
        x1 = rnd.randint(0, 1023)
        y1 = rnd.randint(0, 1023)
        x2 = min(1023, x1 + rnd.randint(18, 260))
        y2 = min(1023, y1 + rnd.randint(18, 260))
        color = (rnd.randint(70, 230), rnd.randint(70, 220), rnd.randint(60, 210))
        draw.ellipse((x1, y1, x2, y2), fill=color)
    draw.text((120, 940), label[:42], fill=(245, 230, 200))
    img.save(path, format="PNG")


def _anchor_variations(text: str) -> list[str]:
    swaps = [
        ("pen-and-ink sketch", "copper plate engraving"),
        ("sepia tones", "warm umber wash"),
        ("crosshatching", "dense etching hatchwork"),
        ("classical oil painting", "baroque oil tableau"),
    ]
    variants = [text]
    for left, right in swaps:
        if left in text:
            variants.append(text.replace(left, right))
        else:
            variants.append(f"{text}, {right}")
    return variants


def optimize(*, sample_books: int, iterations: int) -> dict[str, Any]:
    library = PromptLibrary(PROJECT_ROOT / "config" / "prompt_library.json")
    books = _sample_books(sample_books)
    anchors = library.get_style_anchors()
    details: list[dict[str, Any]] = []

    for anchor in anchors:
        candidates = _anchor_variations(anchor.style_text)[: max(2, iterations + 1)]
        scored: list[tuple[str, float]] = []
        for idx, text in enumerate(candidates):
            scores = []
            for book_number, title in books:
                img_path = TMP_DIR / anchor.name / f"iter_{idx}" / f"book_{book_number}.png"
                _render_candidate(img_path, seed=hash((anchor.name, idx, book_number)) & 0xFFFFFFFF, label=title)
                score = quality_gate.score_image(
                    img_path,
                    threshold=0.7,
                    book_number=book_number,
                    variant_id=idx + 1,
                    model="anchor-opt",
                ).overall_score
                scores.append(score)
            avg = sum(scores) / max(1, len(scores))
            scored.append((text, avg))

        scored.sort(key=lambda row: row[1], reverse=True)
        best_text, best_score = scored[0]
        baseline_score = scored[-1][1]
        improvement = round(best_score - baseline_score, 4)
        details.append(
            {
                "anchor": anchor.name,
                "baseline_score": round(baseline_score, 4),
                "best_score": round(best_score, 4),
                "improvement": improvement,
                "best_text": best_text,
            }
        )

        if best_text != anchor.style_text:
            optimized_anchor = StyleAnchor(
                name=f"{anchor.name}_opt_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                description=f"Optimized variant of {anchor.name}",
                style_text=best_text,
                tags=list(anchor.tags) + ["optimized"],
            )
            library.add_style_anchor(optimized_anchor)

    details.sort(key=lambda row: row["improvement"], reverse=True)
    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_books": sample_books,
        "iterations": iterations,
        "details": details,
        "top_modifications": details[:5],
    }
    RESULTS_PATH.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Optimize style anchors")
    parser.add_argument("--sample-books", type=int, default=5)
    parser.add_argument("--iterations", type=int, default=3)
    args = parser.parse_args()

    result = optimize(sample_books=args.sample_books, iterations=args.iterations)
    logger.info("Style anchor optimization summary: %s", json.dumps(result.get('top_modifications', []), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
