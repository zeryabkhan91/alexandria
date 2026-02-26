"""Snapshot/restore utilities for disaster recovery."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import config
from src.safe_json import atomic_write_json, load_json


SNAPSHOT_EXTENSIONS = {".json", ".sqlite3", ".db", ".md", ".txt", ".csv", ".toml"}


@dataclass(slots=True)
class SnapshotSummary:
    snapshot_dir: Path
    files_copied: int
    bytes_copied: int
    output_index_files: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_dir": str(self.snapshot_dir),
            "files_copied": int(self.files_copied),
            "bytes_copied": int(self.bytes_copied),
            "output_index_files": int(self.output_index_files),
        }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _should_copy_file(path: Path) -> bool:
    return path.suffix.lower() in SNAPSHOT_EXTENSIONS


def _iter_snapshot_sources(runtime: config.Config) -> list[tuple[str, Path, Path]]:
    rows: list[tuple[str, Path, Path]] = []
    for scope, root in [("config", runtime.config_dir), ("data", runtime.data_dir)]:
        if not root.exists():
            continue
        for file_path in sorted(root.rglob("*")):
            if not file_path.is_file():
                continue
            if scope == "data" and "snapshots" in file_path.parts:
                continue
            if not _should_copy_file(file_path):
                continue
            rows.append((scope, root, file_path))
    return rows


def _build_output_index(output_dir: Path) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    total_size = 0
    for file_path in sorted(output_dir.rglob("*")) if output_dir.exists() else []:
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() not in {".jpg", ".jpeg", ".png", ".pdf", ".ai"}:
            continue
        size = int(file_path.stat().st_size)
        total_size += size
        items.append(
            {
                "path": str(file_path.relative_to(output_dir)),
                "size": size,
                "sha256": _sha256_file(file_path),
            }
        )
    return {
        "generated_at": _utc_now_iso(),
        "output_root": str(output_dir),
        "total_files": len(items),
        "total_size_bytes": total_size,
        "items": items,
    }


def create_snapshot(
    *,
    runtime: config.Config | None = None,
    snapshot_root: Path | None = None,
) -> SnapshotSummary:
    runtime = runtime or config.get_config()
    root = snapshot_root or (runtime.data_dir / "snapshots")
    root.mkdir(parents=True, exist_ok=True)

    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    snapshot_dir = root / snapshot_id
    if snapshot_dir.exists():
        raise FileExistsError(f"Snapshot already exists: {snapshot_dir}")
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    manifest_items: list[dict[str, Any]] = []
    files_copied = 0
    bytes_copied = 0

    for scope, base, source in _iter_snapshot_sources(runtime):
        rel = source.relative_to(base)
        target = snapshot_dir / scope / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        size = int(target.stat().st_size)
        files_copied += 1
        bytes_copied += size
        manifest_items.append(
            {
                "scope": scope,
                "path": str(Path(scope) / rel),
                "size": size,
                "sha256": _sha256_file(target),
            }
        )

    output_index = _build_output_index(runtime.output_dir)
    output_index_path = snapshot_dir / "output_index.json"
    atomic_write_json(output_index_path, output_index)
    manifest_items.append(
        {
            "scope": "meta",
            "path": "output_index.json",
            "size": int(output_index_path.stat().st_size),
            "sha256": _sha256_file(output_index_path),
        }
    )

    manifest = {
        "created_at": _utc_now_iso(),
        "catalog_id": runtime.catalog_id,
        "project_root": str(runtime.project_root),
        "source": {
            "config_dir": str(runtime.config_dir),
            "data_dir": str(runtime.data_dir),
            "output_dir": str(runtime.output_dir),
        },
        "items": manifest_items,
    }
    atomic_write_json(snapshot_dir / "manifest.json", manifest)
    return SnapshotSummary(
        snapshot_dir=snapshot_dir,
        files_copied=files_copied,
        bytes_copied=bytes_copied,
        output_index_files=int(output_index.get("total_files", 0) or 0),
    )


def validate_snapshot(snapshot_dir: Path) -> dict[str, Any]:
    manifest_path = snapshot_dir / "manifest.json"
    manifest = load_json(manifest_path, {"items": []})
    if not isinstance(manifest, dict):
        return {"ok": False, "error": "Invalid manifest format", "checked": 0, "mismatches": 0}
    items = manifest.get("items", [])
    if not isinstance(items, list):
        items = []

    checked = 0
    mismatches: list[dict[str, Any]] = []
    for row in items:
        if not isinstance(row, dict):
            continue
        rel = str(row.get("path", "")).strip()
        expected = str(row.get("sha256", "")).strip()
        if not rel or not expected:
            continue
        file_path = snapshot_dir / rel
        checked += 1
        if not file_path.exists():
            mismatches.append({"path": rel, "reason": "missing"})
            continue
        actual = _sha256_file(file_path)
        if actual != expected:
            mismatches.append({"path": rel, "reason": "hash_mismatch"})
    return {
        "ok": len(mismatches) == 0,
        "checked": checked,
        "mismatches": len(mismatches),
        "issues": mismatches,
    }


def restore_snapshot(
    *,
    snapshot_dir: Path,
    runtime: config.Config | None = None,
    force: bool = False,
) -> dict[str, Any]:
    runtime = runtime or config.get_config()
    summary = {"restored_files": 0, "config_files": 0, "data_files": 0, "skipped_existing": 0}

    for scope, target_root in [("config", runtime.config_dir), ("data", runtime.data_dir)]:
        source_root = snapshot_dir / scope
        if not source_root.exists():
            continue
        for source in sorted(source_root.rglob("*")):
            if not source.is_file():
                continue
            rel = source.relative_to(source_root)
            target = target_root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() and not force:
                summary["skipped_existing"] += 1
                continue
            shutil.copy2(source, target)
            summary["restored_files"] += 1
            if scope == "config":
                summary["config_files"] += 1
            else:
                summary["data_files"] += 1
    summary["ok"] = True
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Disaster recovery snapshot/restore tools")
    sub = parser.add_subparsers(dest="command", required=True)

    snapshot_cmd = sub.add_parser("snapshot", help="Create snapshot")
    snapshot_cmd.add_argument("--snapshot-root", type=Path, default=None)

    validate_cmd = sub.add_parser("validate", help="Validate snapshot")
    validate_cmd.add_argument("--snapshot", type=Path, required=True)

    restore_cmd = sub.add_parser("restore", help="Restore snapshot")
    restore_cmd.add_argument("--snapshot", type=Path, required=True)
    restore_cmd.add_argument("--force", action="store_true")

    args = parser.parse_args()
    if args.command == "snapshot":
        summary = create_snapshot(snapshot_root=args.snapshot_root)
        print(json.dumps(summary.to_dict(), indent=2))
        return 0
    if args.command == "validate":
        result = validate_snapshot(args.snapshot)
        print(json.dumps(result, indent=2))
        return 0 if result.get("ok", False) else 1
    if args.command == "restore":
        result = restore_snapshot(snapshot_dir=args.snapshot, force=bool(args.force))
        print(json.dumps(result, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
