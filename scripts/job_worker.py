#!/usr/bin/env python3
"""Dedicated worker entrypoint for async generation queue."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import quality_review  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Alexandria async generation workers")
    parser.add_argument("--catalog", type=str, default=None, help="Catalog id from config/catalogs.json")
    parser.add_argument("--workers", type=int, default=quality_review.JOB_WORKER_COUNT, help="Worker thread count")
    args = parser.parse_args()
    quality_review.run_worker_service(catalog_id=args.catalog, worker_count=args.workers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

