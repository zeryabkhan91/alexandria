#!/usr/bin/env python3
"""Import a new catalog/series into Alexandria multi-catalog config."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src import cover_analyzer
from src import prompt_generator
from src.logger import get_logger

logger = get_logger(__name__)


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "catalog"


def _parse_folder(folder_name: str) -> tuple[int, str, str]:
    token = folder_name.strip()
    m = re.match(r"^(\d+)\.\s*(.+)$", token)
    if not m:
        raise ValueError(f"Folder name missing numeric prefix: {folder_name}")

    number = int(m.group(1))
    tail = m.group(2).strip()
    if " - " in tail:
        title, author = tail.rsplit(" - ", 1)
    elif " — " in tail:
        title, author = tail.rsplit(" — ", 1)
    else:
        title = tail
        author = "Unknown"

    return number, title.strip(), author.strip()


def _scan_catalog(input_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for folder in sorted([p for p in input_dir.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
        try:
            number, title, author = _parse_folder(folder.name)
        except ValueError:
            continue

        file_base = f"{title} - {author}".strip()
        rows.append(
            {
                "number": number,
                "title": title,
                "author": author,
                "folder_name": folder.name,
                "file_base": file_base,
                "genre": "unknown",
                "themes": [],
            }
        )
    rows.sort(key=lambda row: int(row.get("number", 0)))
    return rows


def _register_catalog(
    *,
    catalog_id: str,
    name: str,
    book_count: int,
    catalog_file: Path,
    prompts_file: Path,
    input_dir: Path,
    output_dir: Path,
    cover_style: str,
) -> None:
    payload = config._load_catalogs_payload()  # type: ignore[attr-defined]
    rows = payload.get("catalogs", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []

    def _repo_rel(path: Path) -> str:
        resolved = path.resolve()
        root = PROJECT_ROOT.resolve()
        try:
            return str(resolved.relative_to(root))
        except ValueError:
            return str(path)

    entry = {
        "id": catalog_id,
        "name": name,
        "book_count": int(book_count),
        "catalog_file": _repo_rel(catalog_file),
        "prompts_file": _repo_rel(prompts_file),
        "input_covers_dir": _repo_rel(input_dir),
        "output_covers_dir": _repo_rel(output_dir),
        "cover_style": cover_style,
        "status": "imported",
    }

    replaced = False
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip().lower() == catalog_id.lower():
            rows[idx] = entry
            replaced = True
            break

    if not replaced:
        rows.append(entry)

    payload = {"catalogs": rows}
    config.CATALOGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    config.CATALOGS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def import_catalog(
    *,
    name: str,
    catalog_id: str,
    input_dir: Path,
    output_dir: Path,
    cover_style: str,
) -> dict[str, Any]:
    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    catalog_id = _safe_slug(catalog_id)
    output_dir.mkdir(parents=True, exist_ok=True)

    catalog_rows = _scan_catalog(input_dir)
    if not catalog_rows:
        raise ValueError("No valid cover folders found in input directory")

    catalog_file = config.catalog_scoped_config_path("book_catalog.json", catalog_id=catalog_id, config_dir=config.CONFIG_DIR)
    prompts_file = config.catalog_scoped_config_path("book_prompts.json", catalog_id=catalog_id, config_dir=config.CONFIG_DIR)
    regions_file = config.cover_regions_path(catalog_id=catalog_id, config_dir=config.CONFIG_DIR)

    catalog_file.write_text(json.dumps(catalog_rows, indent=2, ensure_ascii=False), encoding="utf-8")

    prompts = prompt_generator.generate_all_prompts(catalog_path=catalog_file, templates_path=config.PROMPT_TEMPLATES_PATH)
    prompt_generator.save_prompts(prompts, prompts_file)

    analysis = cover_analyzer.analyze_all_covers(input_dir, template_id=cover_style, regions_path=regions_file)

    _register_catalog(
        catalog_id=catalog_id,
        name=name,
        book_count=len(catalog_rows),
        catalog_file=catalog_file,
        prompts_file=prompts_file,
        input_dir=input_dir,
        output_dir=output_dir,
        cover_style=cover_style,
    )

    return {
        "catalog_id": catalog_id,
        "name": name,
        "books_found": len(catalog_rows),
        "covers_analyzed": int(analysis.get("cover_count", 0)),
        "prompts_generated": len(prompts) * 5,
        "catalog_file": str(catalog_file),
        "prompts_file": str(prompts_file),
        "regions_file": str(regions_file),
        "output_dir": str(output_dir),
        "input_dir": str(input_dir),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Import and register a new catalog")
    parser.add_argument("--name", required=True)
    parser.add_argument("--id", required=True)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cover-style", type=str, default="navy_gold_medallion")
    args = parser.parse_args()

    result = import_catalog(
        name=args.name,
        catalog_id=args.id,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        cover_style=args.cover_style,
    )
    logger.info("Catalog import summary: %s", json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
