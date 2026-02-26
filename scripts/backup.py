#!/usr/bin/env python3
"""Automated backup utility for SQLite + JSON project state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src import disaster_recovery


def _snapshot_root(runtime: config.Config, override: Path | None) -> Path:
    root = override or (runtime.data_dir / "snapshots")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _prune_snapshots(root: Path, *, keep: int) -> dict[str, int]:
    keep_count = max(1, int(keep))
    snapshots = sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True)
    removed = 0
    removed_bytes = 0
    for path in snapshots[keep_count:]:
        size = 0
        for child in path.rglob("*"):
            if child.is_file():
                size += int(child.stat().st_size)
        for child in sorted(path.rglob("*"), reverse=True):
            if child.is_file():
                child.unlink(missing_ok=True)
            elif child.is_dir():
                child.rmdir()
        path.rmdir()
        removed += 1
        removed_bytes += size
    return {"removed": removed, "removed_bytes": removed_bytes}


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a project snapshot backup")
    parser.add_argument("--catalog", default=config.DEFAULT_CATALOG_ID, help="Catalog id")
    parser.add_argument("--snapshot-root", type=Path, default=None, help="Override snapshot root")
    parser.add_argument("--keep", type=int, default=10, help="How many latest snapshots to keep")
    args = parser.parse_args()

    try:
        runtime = config.get_config(str(args.catalog or config.DEFAULT_CATALOG_ID))
    except TypeError:  # pragma: no cover
        runtime = config.get_config()  # type: ignore[call-arg]
    root = _snapshot_root(runtime, args.snapshot_root)

    summary = disaster_recovery.create_snapshot(runtime=runtime, snapshot_root=root)
    prune = _prune_snapshots(root, keep=max(1, int(args.keep)))

    payload = {
        "ok": True,
        "catalog": runtime.catalog_id,
        "snapshot": summary.to_dict(),
        "retention": {"keep": max(1, int(args.keep)), **prune},
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
