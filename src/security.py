"""Security and sanitization helpers for API input handling."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def sanitize_string(value: Any, max_length: int = 1000) -> str:
    text = str(value or "").replace("\x00", "").strip()
    if max_length > 0 and len(text) > int(max_length):
        return text[: int(max_length)]
    return text


def sanitize_path(path: str, allowed_root: str | Path) -> Path:
    raw = sanitize_string(path, max_length=2048)
    if not raw:
        raise ValueError("Path cannot be empty")
    if ".." in raw.replace("\\", "/"):
        raise ValueError("Path traversal segments are not allowed")
    root = Path(allowed_root).resolve()
    target = (root / raw).resolve() if not Path(raw).is_absolute() else Path(raw).resolve()
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ValueError("Path escapes allowed root") from exc
    return target


def validate_book_number(value: Any, *, min_value: int = 1, max_value: int = 100000) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("Book number must be an integer") from exc
    if number < min_value or number > max_value:
        raise ValueError(f"Book number must be between {min_value} and {max_value}")
    return number


def validate_catalog_id(value: Any) -> str:
    token = sanitize_string(value, max_length=120).lower()
    if not token:
        raise ValueError("Catalog id is required")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-_]{0,119}", token):
        raise ValueError("Catalog id must be lowercase alphanumeric plus '-' or '_'")
    return token


def mask_api_key(key: str) -> str:
    token = sanitize_string(key, max_length=4096)
    if not token:
        return ""
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}...{token[-4:]}"


def _looks_sensitive_key(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(marker in lowered for marker in ("key", "token", "secret", "password", "authorization"))


def scrub_sensitive(payload: Any) -> Any:
    if isinstance(payload, dict):
        out: dict[str, Any] = {}
        for key, value in payload.items():
            if _looks_sensitive_key(str(key)):
                out[str(key)] = "[REDACTED]"
            else:
                out[str(key)] = scrub_sensitive(value)
        return out
    if isinstance(payload, list):
        return [scrub_sensitive(item) for item in payload]
    return payload
