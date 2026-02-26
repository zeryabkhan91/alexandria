#!/usr/bin/env python3
"""Disk cleanup and reporting utilities."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
import zipfile
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

logger = get_logger(__name__)


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(file_path.stat().st_size for file_path in path.rglob("*") if file_path.is_file())


def _to_mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 * 1024), 3)


def report_disk_usage(runtime: config.Config) -> dict[str, Any]:
    output_dir = runtime.output_dir
    archive_dir = output_dir / "Archive"
    tmp_dir = runtime.tmp_dir
    data_dir = runtime.data_dir

    sizes = {
        "output_covers_mb": _to_mb(_dir_size(output_dir)),
        "archive_mb": _to_mb(_dir_size(archive_dir)),
        "tmp_mb": _to_mb(_dir_size(tmp_dir)),
        "data_mb": _to_mb(_dir_size(data_dir)),
    }
    sizes["total_tracked_mb"] = round(
        sizes["output_covers_mb"] + sizes["archive_mb"] + sizes["tmp_mb"] + sizes["data_mb"],
        3,
    )
    disk = shutil.disk_usage(runtime.project_root)
    sizes["disk_free_gb"] = round(disk.free / (1024 ** 3), 3)
    sizes["disk_total_gb"] = round(disk.total / (1024 ** 3), 3)
    return sizes


def clean_tmp(runtime: config.Config, *, older_than_days: int = 7) -> dict[str, Any]:
    cutoff = time.time() - (older_than_days * 24 * 3600)
    removed_files = 0
    removed_bytes = 0

    if runtime.tmp_dir.exists():
        for path in runtime.tmp_dir.rglob("*"):
            if not path.is_file():
                continue
            if path.stat().st_mtime >= cutoff:
                continue
            try:
                removed_bytes += path.stat().st_size
                path.unlink()
                removed_files += 1
            except OSError:
                continue

    return {
        "removed_files": removed_files,
        "removed_mb": _to_mb(removed_bytes),
        "older_than_days": older_than_days,
    }


def compress_archive(runtime: config.Config) -> dict[str, Any]:
    archive_dir = runtime.output_dir / "Archive"
    if not archive_dir.exists():
        return {"compressed_archives": 0, "archive_exists": False}

    zip_root = archive_dir.parent / "Archive_Zips"
    zip_root.mkdir(parents=True, exist_ok=True)

    compressed = 0
    for book_dir in sorted([p for p in archive_dir.iterdir() if p.is_dir()]):
        zip_path = zip_root / f"{book_dir.name}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for file_path in book_dir.rglob("*"):
                if file_path.is_file():
                    zf.write(file_path, arcname=str(file_path.relative_to(book_dir)))
        compressed += 1

    return {
        "compressed_archives": compressed,
        "zip_output_dir": str(zip_root),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Cleanup tools for Alexandria Cover Designer")
    parser.add_argument("--report", action="store_true", help="Show disk usage report")
    parser.add_argument("--clean-tmp", action="store_true", help="Remove tmp files older than 7 days")
    parser.add_argument("--compress-archive", action="store_true", help="Compress Archive folders into zip files")
    args = parser.parse_args()

    runtime = config.get_config()

    if not (args.report or args.clean_tmp or args.compress_archive):
        parser.error("Specify at least one action: --report, --clean-tmp, or --compress-archive")

    output: dict[str, Any] = {}
    if args.report:
        output["report"] = report_disk_usage(runtime)
    if args.clean_tmp:
        output["clean_tmp"] = clean_tmp(runtime)
    if args.compress_archive:
        output["compress_archive"] = compress_archive(runtime)

    logger.info("Cleanup result: %s", json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
