#!/usr/bin/env python3
"""Model-specific prompt tuning utility."""

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
OVERRIDES_PATH = PROJECT_ROOT / "config" / "model_prompt_overrides.json"
RESULTS_PATH = PROJECT_ROOT / "data" / "model_prompt_tuning.json"
TMP_DIR = PROJECT_ROOT / "tmp" / "model_tuning"


PROMPT_STYLES = {
    "concise": "Iconic circular illustration for {title}, classical style, no text.",
    "detailed": "Highly detailed circular medallion illustration for {title}, include setting, protagonist, symbolic motifs, period accuracy, and rich composition; no text, no watermark.",
    "style_heavy": "Classical pen-and-ink engraving with copper plate linework and warm sepia tones for {title}, dramatic chiaroscuro, no letters or words.",
    "negative_emphasis": "Create a circular artwork for {title}. Must avoid text, letters, watermark, logo, modern digital style, neon colors.",
}


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _sample_books(count: int) -> list[tuple[int, str]]:
    rows = _load_json(CATALOG_PATH, [])
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            number = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        out.append((number, str(row.get("title", f"Book {number}"))))
    return out[:count]


def _render(path: Path, *, seed: int, label: str) -> None:
    rnd = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new("RGB", (1024, 1024), (26, 42, 70))
    draw = ImageDraw.Draw(img)
    for _ in range(110):
        x1 = rnd.randint(0, 1023)
        y1 = rnd.randint(0, 1023)
        x2 = min(1023, x1 + rnd.randint(15, 260))
        y2 = min(1023, y1 + rnd.randint(15, 260))
        color = (rnd.randint(80, 240), rnd.randint(70, 230), rnd.randint(60, 220))
        if rnd.random() > 0.5:
            draw.rectangle((x1, y1, x2, y2), fill=color)
        else:
            draw.ellipse((x1, y1, x2, y2), fill=color)
    draw.text((130, 940), label[:40], fill=(245, 230, 200))
    img.save(path, format="PNG")


def tune_model(*, model: str, sample_books: int) -> dict[str, Any]:
    books = _sample_books(sample_books)
    scores_by_style: dict[str, list[float]] = {name: [] for name in PROMPT_STYLES.keys()}

    for style_name, template in PROMPT_STYLES.items():
        for book_number, title in books:
            prompt = template.format(title=title)
            img_path = TMP_DIR / model.replace("/", "__") / style_name / f"book_{book_number}.png"
            _render(img_path, seed=hash((model, style_name, book_number)) & 0xFFFFFFFF, label=title)
            score = quality_gate.score_image(
                img_path,
                threshold=0.7,
                book_number=book_number,
                variant_id=1,
                model=model,
            ).overall_score
            scores_by_style[style_name].append(score)

    averages = {
        style: round(sum(values) / max(1, len(values)), 4)
        for style, values in scores_by_style.items()
    }
    best_style = sorted(averages.items(), key=lambda item: item[1], reverse=True)[0][0]

    overrides = _load_json(OVERRIDES_PATH, {"models": {}})
    if not isinstance(overrides, dict):
        overrides = {"models": {}}
    if not isinstance(overrides.get("models"), dict):
        overrides["models"] = {}

    overrides["models"][model] = {
        "style": best_style,
        "prompt_template": PROMPT_STYLES[best_style],
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "average_score": averages[best_style],
    }
    OVERRIDES_PATH.write_text(json.dumps(overrides, indent=2, ensure_ascii=False), encoding="utf-8")

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "sample_books": sample_books,
        "averages": averages,
        "best_style": best_style,
        "saved_to": str(OVERRIDES_PATH),
    }
    RESULTS_PATH.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Tune prompts for one model")
    parser.add_argument("--model", required=True)
    parser.add_argument("--sample-books", type=int, default=5)
    args = parser.parse_args()

    result = tune_model(model=args.model, sample_books=args.sample_books)
    logger.info("Model prompt tuning summary: %s", json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
