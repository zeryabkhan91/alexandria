"""Lightweight error metrics tracker."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import config
from src.safe_json import atomic_write_json, load_json


METRICS_PATH = config.error_metrics_path()


def _metrics_path(*, catalog_id: str | None = None, path: Path | None = None) -> Path:
    if path is not None:
        return path
    if catalog_id:
        return config.error_metrics_path(catalog_id=catalog_id, data_dir=config.DATA_DIR)
    return METRICS_PATH


def record_error(
    code: str,
    *,
    endpoint: str = "",
    details: dict[str, Any] | None = None,
    catalog_id: str | None = None,
    path: Path | None = None,
) -> None:
    target_path = _metrics_path(catalog_id=catalog_id, path=path)
    payload = load_json(target_path, {"updated_at": "", "total": 0, "by_code": {}, "recent": []})
    if not isinstance(payload, dict):
        payload = {"updated_at": "", "total": 0, "by_code": {}, "recent": []}

    by_code = payload.get("by_code", {})
    if not isinstance(by_code, dict):
        by_code = {}
    counter = Counter({str(k): int(v) for k, v in by_code.items() if isinstance(v, (int, float, str))})
    counter[str(code)] += 1

    recent = payload.get("recent", [])
    if not isinstance(recent, list):
        recent = []
    recent.append(
        {
            "time": datetime.now(timezone.utc).isoformat(),
            "code": str(code),
            "endpoint": str(endpoint or ""),
            "details": details or {},
        }
    )
    payload["recent"] = recent[-200:]
    payload["total"] = int(payload.get("total", 0) or 0) + 1
    payload["by_code"] = dict(counter)
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    atomic_write_json(target_path, payload)


def get_metrics(*, catalog_id: str | None = None, path: Path | None = None) -> dict[str, Any]:
    target_path = _metrics_path(catalog_id=catalog_id, path=path)
    payload = load_json(target_path, {"updated_at": "", "total": 0, "by_code": {}, "recent": []})
    if not isinstance(payload, dict):
        return {"updated_at": "", "total": 0, "by_code": {}, "recent": []}
    return payload
