"""Standardized API response helpers."""

from __future__ import annotations

from typing import Any


def error_payload(
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "error": True,
        "code": code,
        "message": message,
        "details": details or {},
    }


def success_payload(
    data: Any | None = None,
    *,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "error": False,
    }
    if data is not None:
        payload["data"] = data
    if meta:
        payload["meta"] = meta
    return payload
