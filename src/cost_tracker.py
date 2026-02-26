"""Cost ledger and budget tracking helpers for analytics endpoints."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src import config
from src import safe_json


DEFAULT_LEDGER_PATH = config.cost_ledger_path()
DEFAULT_BUDGET_PATH = config.budget_config_path()
MAX_LEDGER_ENTRIES = 200_000


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_datetime(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(entry.get("id") or str(uuid.uuid4())),
        "timestamp": str(entry.get("timestamp") or _utc_now_iso()),
        "catalog": str(entry.get("catalog") or config.DEFAULT_CATALOG_ID),
        "book_number": _safe_int(entry.get("book_number"), 0),
        "job_id": str(entry.get("job_id") or ""),
        "model": str(entry.get("model") or "unknown"),
        "provider": str(entry.get("provider") or "unknown"),
        "operation": str(entry.get("operation") or "generate"),
        "tokens_in": max(0, _safe_int(entry.get("tokens_in"), 0)),
        "tokens_out": max(0, _safe_int(entry.get("tokens_out"), 0)),
        "images_generated": max(0, _safe_int(entry.get("images_generated"), 0)),
        "cost_usd": round(max(0.0, _safe_float(entry.get("cost_usd"), 0.0)), 6),
        "duration_seconds": round(max(0.0, _safe_float(entry.get("duration_seconds"), 0.0)), 4),
        "metadata": entry.get("metadata", {}) if isinstance(entry.get("metadata"), dict) else {},
    }


def load_ledger(path: Path = DEFAULT_LEDGER_PATH) -> dict[str, Any]:
    payload = safe_json.load_json(path, {"updated_at": "", "entries": []})
    if not isinstance(payload, dict):
        payload = {"updated_at": "", "entries": []}
    entries = payload.get("entries")
    if not isinstance(entries, list):
        entries = []
    normalized = [_normalize_entry(row) for row in entries if isinstance(row, dict)]
    return {"updated_at": str(payload.get("updated_at", "") or ""), "entries": normalized}


def record_entry(path: Path = DEFAULT_LEDGER_PATH, *, entry: dict[str, Any]) -> dict[str, Any]:
    payload = load_ledger(path)
    normalized = _normalize_entry(entry)
    rows = payload.get("entries", [])
    rows.append(normalized)
    payload["entries"] = rows[-MAX_LEDGER_ENTRIES:]
    payload["updated_at"] = _utc_now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(path, payload)
    return normalized


def record_entries(path: Path = DEFAULT_LEDGER_PATH, *, entries: list[dict[str, Any]]) -> int:
    payload = load_ledger(path)
    rows = payload.get("entries", [])
    count = 0
    for row in entries:
        if not isinstance(row, dict):
            continue
        rows.append(_normalize_entry(row))
        count += 1
    payload["entries"] = rows[-MAX_LEDGER_ENTRIES:]
    payload["updated_at"] = _utc_now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(path, payload)
    return count


def _period_start(period: str | None) -> datetime | None:
    token = str(period or "").strip().lower()
    if not token:
        return None
    if token in {"all", "all_time"}:
        return None
    if token.endswith("d") and token[:-1].isdigit():
        return datetime.now(timezone.utc) - timedelta(days=max(1, int(token[:-1])))
    if token.endswith("h") and token[:-1].isdigit():
        return datetime.now(timezone.utc) - timedelta(hours=max(1, int(token[:-1])))
    return None


def list_entries(
    path: Path = DEFAULT_LEDGER_PATH,
    *,
    catalog_id: str | None = None,
    period: str | None = None,
) -> list[dict[str, Any]]:
    payload = load_ledger(path)
    rows = payload.get("entries", [])
    if not isinstance(rows, list):
        rows = []
    since = _period_start(period)
    selected: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if catalog_id and str(row.get("catalog", "")).strip() != str(catalog_id).strip():
            continue
        if since is not None:
            dt = _as_datetime(str(row.get("timestamp", "")))
            if dt is None or dt < since:
                continue
        selected.append(row)
    selected.sort(key=lambda item: str(item.get("timestamp", "")), reverse=True)
    return selected


def summarize(entries: list[dict[str, Any]]) -> dict[str, Any]:
    if not entries:
        return {
            "entries": 0,
            "total_cost_usd": 0.0,
            "images_generated": 0,
            "avg_cost_per_image": 0.0,
            "avg_duration_seconds": 0.0,
            "period_start": None,
            "period_end": None,
        }
    total_cost = sum(_safe_float(row.get("cost_usd"), 0.0) for row in entries)
    total_images = sum(_safe_int(row.get("images_generated"), 0) for row in entries)
    avg_duration = sum(_safe_float(row.get("duration_seconds"), 0.0) for row in entries) / max(1, len(entries))
    stamps = [str(row.get("timestamp", "")) for row in entries if str(row.get("timestamp", "")).strip()]
    return {
        "entries": len(entries),
        "total_cost_usd": round(total_cost, 6),
        "images_generated": int(total_images),
        "avg_cost_per_image": round((total_cost / total_images), 6) if total_images else 0.0,
        "avg_duration_seconds": round(avg_duration, 4),
        "period_start": min(stamps) if stamps else None,
        "period_end": max(stamps) if stamps else None,
    }


def by_book(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[int, dict[str, Any]] = {}
    for entry in entries:
        book = _safe_int(entry.get("book_number"), 0)
        if book <= 0:
            continue
        row = rows.setdefault(
            book,
            {
                "book_number": book,
                "entries": 0,
                "images_generated": 0,
                "total_cost_usd": 0.0,
                "operations": {},
            },
        )
        row["entries"] += 1
        row["images_generated"] += _safe_int(entry.get("images_generated"), 0)
        row["total_cost_usd"] += _safe_float(entry.get("cost_usd"), 0.0)
        op = str(entry.get("operation", "generate"))
        ops = row["operations"]
        ops[op] = ops.get(op, 0) + 1
    out = list(rows.values())
    for row in out:
        row["total_cost_usd"] = round(_safe_float(row.get("total_cost_usd"), 0.0), 6)
    out.sort(key=lambda item: (_safe_float(item.get("total_cost_usd"), 0.0), _safe_int(item.get("book_number"), 0)), reverse=True)
    return out


def by_model(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    for entry in entries:
        model = str(entry.get("model", "unknown"))
        provider = str(entry.get("provider", "unknown"))
        key = (model, provider)
        row = rows.setdefault(
            key,
            {
                "model": model,
                "provider": provider,
                "entries": 0,
                "images_generated": 0,
                "total_cost_usd": 0.0,
                "avg_duration_seconds": 0.0,
            },
        )
        row["entries"] += 1
        row["images_generated"] += _safe_int(entry.get("images_generated"), 0)
        row["total_cost_usd"] += _safe_float(entry.get("cost_usd"), 0.0)
        row["avg_duration_seconds"] += _safe_float(entry.get("duration_seconds"), 0.0)
    out = list(rows.values())
    for row in out:
        count = max(1, _safe_int(row.get("entries"), 1))
        row["total_cost_usd"] = round(_safe_float(row.get("total_cost_usd"), 0.0), 6)
        row["avg_duration_seconds"] = round(_safe_float(row.get("avg_duration_seconds"), 0.0) / count, 4)
        row["avg_cost_per_variant"] = round(_safe_float(row.get("total_cost_usd"), 0.0) / max(1, _safe_int(row.get("images_generated"), 0)), 6)
    out.sort(key=lambda item: _safe_float(item.get("total_cost_usd"), 0.0), reverse=True)
    return out


def by_operation(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for entry in entries:
        op = str(entry.get("operation", "generate"))
        row = rows.setdefault(op, {"operation": op, "entries": 0, "total_cost_usd": 0.0, "images_generated": 0})
        row["entries"] += 1
        row["total_cost_usd"] += _safe_float(entry.get("cost_usd"), 0.0)
        row["images_generated"] += _safe_int(entry.get("images_generated"), 0)
    out = list(rows.values())
    for row in out:
        row["total_cost_usd"] = round(_safe_float(row.get("total_cost_usd"), 0.0), 6)
    out.sort(key=lambda item: _safe_float(item.get("total_cost_usd"), 0.0), reverse=True)
    return out


def timeline(entries: list[dict[str, Any]], *, granularity: str = "daily") -> list[dict[str, Any]]:
    bucket_format = "%Y-%m-%d" if str(granularity).strip().lower() != "hourly" else "%Y-%m-%dT%H:00:00Z"
    rows: dict[str, dict[str, Any]] = {}
    for entry in entries:
        dt = _as_datetime(str(entry.get("timestamp", "")))
        if dt is None:
            continue
        bucket = dt.strftime(bucket_format)
        row = rows.setdefault(bucket, {"bucket": bucket, "total_cost_usd": 0.0, "entries": 0, "images_generated": 0})
        row["total_cost_usd"] += _safe_float(entry.get("cost_usd"), 0.0)
        row["entries"] += 1
        row["images_generated"] += _safe_int(entry.get("images_generated"), 0)
    out = list(rows.values())
    out.sort(key=lambda item: str(item.get("bucket", "")))
    running = 0.0
    for row in out:
        running += _safe_float(row.get("total_cost_usd"), 0.0)
        row["total_cost_usd"] = round(_safe_float(row.get("total_cost_usd"), 0.0), 6)
        row["cumulative_cost_usd"] = round(running, 6)
    return out


def _default_budget_payload() -> dict[str, Any]:
    return {
        "global": {
            "limit_usd": float(config.MAX_COST_USD),
            "warning_threshold": 0.8,
            "hard_stop": True,
        },
        "catalogs": {},
        "overrides": {},
        "updated_at": _utc_now_iso(),
    }


def load_budget(path: Path = DEFAULT_BUDGET_PATH) -> dict[str, Any]:
    payload = safe_json.load_json(path, _default_budget_payload())
    if not isinstance(payload, dict):
        payload = _default_budget_payload()
    if not isinstance(payload.get("global"), dict):
        payload["global"] = _default_budget_payload()["global"]
    if not isinstance(payload.get("catalogs"), dict):
        payload["catalogs"] = {}
    if not isinstance(payload.get("overrides"), dict):
        payload["overrides"] = {}
    return payload


def set_budget(
    *,
    path: Path = DEFAULT_BUDGET_PATH,
    catalog_id: str | None = None,
    limit_usd: float,
    warning_threshold: float = 0.8,
    hard_stop: bool = True,
) -> dict[str, Any]:
    payload = load_budget(path)
    row = {
        "limit_usd": max(0.0, float(limit_usd)),
        "warning_threshold": min(0.99, max(0.01, float(warning_threshold))),
        "hard_stop": bool(hard_stop),
        "updated_at": _utc_now_iso(),
    }
    if catalog_id:
        catalogs = payload.get("catalogs", {})
        catalogs[str(catalog_id)] = row
        payload["catalogs"] = catalogs
    else:
        payload["global"] = row
    payload["updated_at"] = _utc_now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(path, payload)
    return payload


def set_override(
    *,
    path: Path = DEFAULT_BUDGET_PATH,
    catalog_id: str,
    extra_limit_usd: float,
    duration_hours: int = 24,
    reason: str = "",
) -> dict[str, Any]:
    payload = load_budget(path)
    overrides = payload.get("overrides", {})
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=max(1, int(duration_hours)))).isoformat()
    overrides[str(catalog_id)] = {
        "extra_limit_usd": max(0.0, float(extra_limit_usd)),
        "expires_at": expires_at,
        "reason": str(reason or ""),
        "created_at": _utc_now_iso(),
    }
    payload["overrides"] = overrides
    payload["updated_at"] = _utc_now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_json.atomic_write_json(path, payload)
    return payload


def budget_status(
    *,
    spent_usd: float,
    catalog_id: str,
    budget_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = budget_payload if isinstance(budget_payload, dict) else load_budget(DEFAULT_BUDGET_PATH)
    global_row = payload.get("global", {}) if isinstance(payload.get("global", {}), dict) else {}
    catalog_row = payload.get("catalogs", {}).get(str(catalog_id), {}) if isinstance(payload.get("catalogs", {}), dict) else {}
    row = dict(global_row)
    row.update(catalog_row if isinstance(catalog_row, dict) else {})
    limit_usd = max(0.0, _safe_float(row.get("limit_usd"), float(config.MAX_COST_USD)))
    warning_threshold = min(0.99, max(0.01, _safe_float(row.get("warning_threshold"), 0.8)))
    hard_stop = bool(row.get("hard_stop", True))

    override_payload = payload.get("overrides", {}).get(str(catalog_id), {}) if isinstance(payload.get("overrides", {}), dict) else {}
    override_active = False
    override_extra = 0.0
    override_expires_at = ""
    if isinstance(override_payload, dict):
        expires_at = _as_datetime(str(override_payload.get("expires_at", "")))
        if expires_at is not None and expires_at >= datetime.now(timezone.utc):
            override_active = True
            override_extra = max(0.0, _safe_float(override_payload.get("extra_limit_usd"), 0.0))
            override_expires_at = expires_at.isoformat()

    effective_limit = limit_usd + override_extra
    spent = max(0.0, _safe_float(spent_usd, 0.0))
    percent = (spent / effective_limit) if effective_limit > 0 else 0.0
    state = "ok"
    if percent >= 1.0:
        state = "blocked"
    elif percent >= warning_threshold:
        state = "warning"
    return {
        "catalog": str(catalog_id),
        "spent_usd": round(spent, 6),
        "limit_usd": round(limit_usd, 6),
        "effective_limit_usd": round(effective_limit, 6),
        "remaining_usd": round(max(0.0, effective_limit - spent), 6),
        "warning_threshold": warning_threshold,
        "percent_used": round(percent, 6),
        "state": state,
        "hard_stop": hard_stop,
        "override": {
            "active": override_active,
            "extra_limit_usd": round(override_extra, 6),
            "expires_at": override_expires_at,
            "reason": str(override_payload.get("reason", "")) if isinstance(override_payload, dict) else "",
        },
    }


def dump_json(path: Path = DEFAULT_LEDGER_PATH) -> str:
    payload = load_ledger(path)
    return json.dumps(payload, ensure_ascii=False, indent=2)
