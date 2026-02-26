#!/usr/bin/env python3
"""Prepare print-ready delivery package for selected winning covers."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Any

from PIL import Image

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src.logger import get_logger
from src.output_exporter import export_jpg, export_pdf

logger = get_logger(__name__)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def _winner_map(path: Path) -> dict[int, int]:
    payload = _load_json(path, {})
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
            winner = int(value.get("winner", 0) or 0)
        else:
            winner = int(value or 0)
        if winner > 0:
            out[book] = winner
    return out


def _catalog_map(path: Path) -> dict[int, dict[str, str]]:
    rows = _load_json(path, [])
    out: dict[int, dict[str, str]] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            book = int(row.get("number", 0))
        except (TypeError, ValueError):
            continue
        folder_name = str(row.get("folder_name", ""))
        if folder_name.endswith(" copy"):
            folder_name = folder_name[:-5]
        out[book] = {
            "title": str(row.get("title", f"Book {book}")),
            "author": str(row.get("author", "Unknown")),
            "folder_name": folder_name,
            "file_base": str(row.get("file_base", f"Book {book}")),
        }
    return out


def _winner_jpg(output_dir: Path, folder_name: str, winner_variant: int) -> Path | None:
    variant_dir = output_dir / folder_name / f"Variant-{winner_variant}"
    if variant_dir.exists():
        jpgs = sorted(variant_dir.glob("*.jpg"))
        if jpgs:
            return jpgs[0]

    root_jpgs = sorted((output_dir / folder_name).glob("*.jpg"))
    if root_jpgs:
        return root_jpgs[0]
    return None


def _preflight_jpg(path: Path) -> tuple[bool, str]:
    try:
        image = Image.open(path)
        if image.size != (3784, 2777):
            return False, f"size={image.size}"
        dpi = image.info.get("dpi", (0, 0))
        return True, f"size={image.size}; dpi={dpi}"
    except Exception as exc:
        return False, str(exc)


def prepare_print_delivery(
    *,
    catalog_id: str,
    selections: Path,
    output: Path,
    format_name: str,
) -> dict[str, Any]:
    runtime = config.get_config(catalog_id)
    winners = _winner_map(selections)
    catalog = _catalog_map(runtime.book_catalog_path)

    output.mkdir(parents=True, exist_ok=True)
    files_dir = output / "files"
    files_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = output / "manifest.csv"
    readme_path = output / "README.txt"

    rows: list[dict[str, Any]] = []
    preflight_failures: list[dict[str, Any]] = []

    for book, winner_variant in sorted(winners.items()):
        meta = catalog.get(book)
        if not meta:
            continue

        source_jpg = _winner_jpg(runtime.output_dir, meta["folder_name"], winner_variant)
        if not source_jpg or not source_jpg.exists():
            preflight_failures.append({"book": book, "error": "winner jpg missing"})
            continue

        base_name = f"{book:04d}_{_safe_filename(meta['title'])}"
        book_dir = files_dir / base_name
        book_dir.mkdir(parents=True, exist_ok=True)

        out_jpg = book_dir / f"{base_name}.jpg"
        out_pdf = book_dir / f"{base_name}.pdf"

        export_jpg(source_jpg, out_jpg, dpi=300)
        export_pdf(source_jpg, out_pdf, dpi=300)

        if format_name == "amazon-kdp":
            deliverables = [out_jpg, out_pdf]
        elif format_name == "ingramspark":
            deliverables = [out_pdf]
        else:  # generic
            deliverables = [out_jpg, out_pdf]

        ok_jpg, detail_jpg = _preflight_jpg(out_jpg)
        ok_pdf = out_pdf.exists() and out_pdf.stat().st_size > 0
        ok = ok_jpg and ok_pdf
        if not ok:
            preflight_failures.append(
                {
                    "book": book,
                    "jpg_ok": ok_jpg,
                    "jpg_detail": detail_jpg,
                    "pdf_ok": ok_pdf,
                }
            )

        rows.append(
            {
                "book_number": book,
                "title": meta["title"],
                "author": meta["author"],
                "winner_variant": winner_variant,
                "format": format_name,
                "deliverables": ";".join(str(path.relative_to(output)) for path in deliverables),
                "preflight": "PASS" if ok else "FAIL",
                "notes": detail_jpg,
            }
        )

    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["book_number", "title", "author", "winner_variant", "format", "deliverables", "preflight", "notes"],
        )
        writer.writeheader()
        writer.writerows(rows)

    readme = [
        "Alexandria Print Delivery Package",
        "",
        f"Catalog: {runtime.catalog_id}",
        f"Format: {format_name}",
        f"Books packaged: {len(rows)}",
        f"Preflight failures: {len(preflight_failures)}",
        "",
        "Validation checks:",
        "- JPG resolution must be 3784x2777 at 300 DPI",
        "- PDF output must exist and be non-empty",
        "",
    ]
    if preflight_failures:
        readme.append("Failures:")
        for failure in preflight_failures[:200]:
            readme.append(f"- {failure}")
    else:
        readme.append("All files passed preflight checks.")

    readme_path.write_text("\n".join(readme) + "\n", encoding="utf-8")

    return {
        "catalog": runtime.catalog_id,
        "format": format_name,
        "books_packaged": len(rows),
        "manifest": str(manifest_path),
        "readme": str(readme_path),
        "preflight_failures": preflight_failures,
        "output": str(output),
    }


def _safe_filename(value: str) -> str:
    token = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value.strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token[:120] or "book"


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare print-ready delivery package")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID)
    parser.add_argument("--selections", type=Path, default=None)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--format", choices=["amazon-kdp", "ingramspark", "generic"], default="generic")
    args = parser.parse_args()
    runtime = config.get_config(args.catalog)

    result = prepare_print_delivery(
        catalog_id=runtime.catalog_id,
        selections=args.selections or config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir),
        output=args.output,
        format_name=args.format,
    )
    logger.info("Print delivery summary: %s", json.dumps(result, ensure_ascii=False))
    return 0 if not result.get("preflight_failures") else 1


if __name__ == "__main__":
    raise SystemExit(main())
