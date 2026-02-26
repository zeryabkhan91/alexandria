"""Signed audit logging for cost-impacting and destructive operations."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src import config
from src.safe_json import atomic_write_json, load_json


AUDIT_LOG_PATH = config.audit_log_path()
AUDIT_MAX_ITEMS = 10_000


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _signature(secret: str, payload: dict[str, Any]) -> str:
    message = _canonical_json(payload).encode("utf-8")
    key = str(secret or "").encode("utf-8")
    return hmac.new(key, message, hashlib.sha256).hexdigest()


def _looks_sensitive_key(key: str) -> bool:
    token = str(key or "").lower()
    for marker in ("key", "token", "secret", "password", "authorization", "auth"):
        if marker in token:
            return True
    return False


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if _looks_sensitive_key(str(key)):
                out[str(key)] = "[REDACTED]"
            else:
                out[str(key)] = _redact(item)
        return out
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


def build_event(
    *,
    action: str,
    impact: str,
    actor: str,
    source_ip: str,
    endpoint: str,
    catalog_id: str,
    status: str,
    details: dict[str, Any] | None = None,
    secret: str | None = None,
) -> dict[str, Any]:
    payload = {
        "timestamp": _utc_now_iso(),
        "action": str(action),
        "impact": str(impact),
        "actor": str(actor or "unknown"),
        "source_ip": str(source_ip or "unknown"),
        "endpoint": str(endpoint or ""),
        "catalog_id": str(catalog_id or ""),
        "status": str(status or "ok"),
        "details": _redact(details or {}),
    }
    key = secret if secret is not None else os.getenv("AUDIT_LOG_SECRET", "")
    if key:
        payload["signature"] = _signature(key, payload)
        payload["signature_status"] = "signed"
    else:
        payload["signature"] = ""
        payload["signature_status"] = "unsigned_no_secret"
    return payload


def verify_event_signature(event: dict[str, Any], *, secret: str | None = None) -> bool:
    if not isinstance(event, dict):
        return False
    key = secret if secret is not None else os.getenv("AUDIT_LOG_SECRET", "")
    if not key:
        return False
    expected = str(event.get("signature", "") or "")
    if not expected:
        return False
    payload = {k: v for k, v in event.items() if k not in {"signature", "signature_status"}}
    actual = _signature(key, payload)
    return hmac.compare_digest(expected, actual)


def append_event(
    *,
    action: str,
    impact: str,
    actor: str,
    source_ip: str,
    endpoint: str,
    catalog_id: str,
    status: str,
    details: dict[str, Any] | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    event = build_event(
        action=action,
        impact=impact,
        actor=actor,
        source_ip=source_ip,
        endpoint=endpoint,
        catalog_id=catalog_id,
        status=status,
        details=details,
    )

    resolved_path = path or config.audit_log_path(catalog_id=catalog_id, data_dir=config.DATA_DIR)
    payload = load_json(resolved_path, {"updated_at": "", "items": []})
    if not isinstance(payload, dict):
        payload = {"updated_at": "", "items": []}
    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []
    items.append(event)
    payload["items"] = items[-AUDIT_MAX_ITEMS:]
    payload["updated_at"] = _utc_now_iso()
    atomic_write_json(resolved_path, payload)
    return event


def load_events(path: Path = AUDIT_LOG_PATH) -> list[dict[str, Any]]:
    payload = load_json(path, {"items": []})
    if not isinstance(payload, dict):
        return []
    rows = payload.get("items", [])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]
