#!/usr/bin/env python3
"""Generate thumbnail cache for cover, review, and mockup images."""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from PIL import Image

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = PROJECT_ROOT / "Output Covers"
DEFAULT_DEST = PROJECT_ROOT / "tmp" / "thumbnails"
SIZES = {
    "small": 200,
    "medium": 400,
    "large": 800,
}


def _collect_images(source: Path) -> list[Path]:
    patterns = ["*.jpg", "*.jpeg", "*.png", "*.JPG", "*.JPEG", "*.PNG"]
    rows: list[Path] = []
    for pattern in patterns:
        rows.extend(source.rglob(pattern))
    return sorted({path for path in rows if path.is_file()})


def _one(source: Path, output: Path, max_dim: int, quality: int) -> tuple[bool, str]:
    try:
        with Image.open(source) as image:
            rgb = image.convert("RGB")
            rgb.thumbnail((max_dim, max_dim), Image.LANCZOS)
            output.parent.mkdir(parents=True, exist_ok=True)
            rgb.save(output, format="JPEG", quality=quality, optimize=True)
        return True, str(source)
    except Exception as exc:  # pragma: no cover - defensive
        return False, f"{source}: {exc}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate thumbnail cache")
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--quality", type=int, default=80)
    parser.add_argument("--workers", type=int, default=max(2, min((os.cpu_count() or 4), 8)))
    args = parser.parse_args()

    source = args.source.resolve()
    dest = args.dest.resolve()
    if not source.exists():
        print(f"source not found: {source}")
        return 1

    images = _collect_images(source)
    if not images:
        print(f"no images found under {source}")
        return 0

    total = len(images) * len(SIZES)
    completed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = {}
        for image in images:
            rel = image.relative_to(source)
            for name, dim in SIZES.items():
                target = (dest / name / rel).with_suffix(".jpg")
                fut = pool.submit(_one, image, target, dim, args.quality)
                futures[fut] = (name, str(rel))

        for fut in as_completed(futures):
            completed += 1
            ok, _msg = fut.result()
            if not ok:
                failed += 1
            if completed % 200 == 0 or completed == total:
                print(f"{completed}/{total} complete, failed={failed}")

    small_count = len(list((dest / "small").rglob("*.jpg")))
    medium_count = len(list((dest / "medium").rglob("*.jpg")))
    large_count = len(list((dest / "large").rglob("*.jpg")))
    print(
        f"done. source_images={len(images)} generated={small_count + medium_count + large_count} failed={failed} dest={dest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
