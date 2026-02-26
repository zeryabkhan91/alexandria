#!/usr/bin/env python3
"""Generate synthetic scale fixtures for pagination/performance tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image


def generate(*, count: int, output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = output_dir / "book_catalog.json"
    winners_path = output_dir / "winner_selections.json"
    covers_root = output_dir / "Output Covers"
    covers_root.mkdir(parents=True, exist_ok=True)

    catalog: list[dict[str, object]] = []
    winners: dict[str, dict[str, int]] = {"selections": {}}
    for idx in range(1, int(count) + 1):
        folder = f"{idx}. Synthetic Title {idx} - Synthetic Author"
        catalog.append(
            {
                "number": idx,
                "title": f"Synthetic Title {idx}",
                "author": f"Synthetic Author {idx}",
                "genre": "synthetic",
                "folder_name": folder,
            }
        )
        winners["selections"][str(idx)] = {"winner": 1}
        variant_dir = covers_root / folder / "Variant-1"
        variant_dir.mkdir(parents=True, exist_ok=True)
        color = (idx * 3) % 255
        Image.new("RGB", (50, 50), (color, 120, 200)).save(variant_dir / f"cover_{idx}.jpg", format="JPEG", quality=85)

    catalog_path.write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    winners_path.write_text(json.dumps(winners, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "catalog_path": str(catalog_path),
        "winners_path": str(winners_path),
        "covers_root": str(covers_root),
        "count": str(count),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate synthetic scale fixture data")
    parser.add_argument("--count", type=int, default=2500)
    parser.add_argument("--output-dir", type=Path, default=Path("tests/fixtures/scale/generated"))
    args = parser.parse_args()
    payload = generate(count=max(1, int(args.count)), output_dir=args.output_dir)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
