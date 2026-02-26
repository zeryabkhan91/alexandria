#!/usr/bin/env python3
"""Restore utility for previously created snapshot backups."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import config
from src import disaster_recovery


def _snapshot_root(runtime: config.Config, override: Path | None) -> Path:
    root = override or (runtime.data_dir / "snapshots")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _list_snapshots(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name, reverse=True):
        manifest = path / "manifest.json"
        created_at = ""
        files = 0
        if manifest.exists():
            try:
                payload = json.loads(manifest.read_text(encoding="utf-8"))
            except Exception:
                payload = {}
            if isinstance(payload, dict):
                created_at = str(payload.get("created_at", ""))
                items = payload.get("items", [])
                files = len(items) if isinstance(items, list) else 0
        size = 0
        for child in path.rglob("*"):
            if child.is_file():
                size += int(child.stat().st_size)
        rows.append(
            {
                "id": path.name,
                "path": str(path),
                "created_at": created_at,
                "file_count": files,
                "size_bytes": size,
            }
        )
    return rows


def _resolve_snapshot(root: Path, token: str) -> Path:
    raw = str(token or "").strip()
    if not raw:
        raise ValueError("snapshot id/path is required")
    candidate = Path(raw)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    by_id = root / raw
    if by_id.exists():
        return by_id
    raise FileNotFoundError(f"Snapshot not found: {raw}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Restore a project snapshot")
    parser.add_argument("--catalog", default=config.DEFAULT_CATALOG_ID, help="Catalog id")
    parser.add_argument("--snapshot-root", type=Path, default=None, help="Override snapshot root")
    parser.add_argument("--list", action="store_true", help="List available snapshots")
    parser.add_argument("--snapshot", default="", help="Snapshot id or absolute path")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    parser.add_argument("--skip-validate", action="store_true", help="Skip snapshot checksum validation")
    args = parser.parse_args()

    try:
        runtime = config.get_config(str(args.catalog or config.DEFAULT_CATALOG_ID))
    except TypeError:  # pragma: no cover
        runtime = config.get_config()  # type: ignore[call-arg]
    root = _snapshot_root(runtime, args.snapshot_root)

    if args.list:
        print(json.dumps({"ok": True, "catalog": runtime.catalog_id, "snapshots": _list_snapshots(root)}, indent=2))
        return 0

    snapshot_dir = _resolve_snapshot(root, str(args.snapshot or ""))
    if not snapshot_dir.exists() or not snapshot_dir.is_dir():
        raise FileNotFoundError(f"Snapshot not found: {snapshot_dir}")

    validation = {"ok": True, "checked": 0, "mismatches": 0}
    if not args.skip_validate:
        validation = disaster_recovery.validate_snapshot(snapshot_dir)
        if not bool(validation.get("ok", False)):
            print(json.dumps({"ok": False, "error": "Snapshot validation failed", "validation": validation}, indent=2))
            return 1

    restored = disaster_recovery.restore_snapshot(snapshot_dir=snapshot_dir, runtime=runtime, force=bool(args.force))
    print(
        json.dumps(
            {
                "ok": bool(restored.get("ok", False)),
                "catalog": runtime.catalog_id,
                "snapshot": str(snapshot_dir),
                "validation": validation,
                "restore": restored,
            },
            indent=2,
        )
    )
    return 0 if bool(restored.get("ok", False)) else 1


if __name__ == "__main__":
    raise SystemExit(main())
