"""Webhook notifications for long-running pipeline batches."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import requests

try:
    from src.logger import get_logger
except ModuleNotFoundError:  # pragma: no cover
    from logger import get_logger  # type: ignore

logger = get_logger(__name__)


class BatchNotifier:
    def __init__(self, *, runtime: Any, enabled: bool = False) -> None:
        self._enabled = bool(enabled)
        self._url = str(getattr(runtime, "webhook_url", "") or "").strip()
        self._events = {str(token).strip() for token in getattr(runtime, "webhook_events", []) if str(token).strip()}

    def _can_send(self, event: str) -> bool:
        if not self._enabled:
            return False
        if not self._url:
            return False
        if event == "batch_start":
            return True
        return (not self._events) or (event in self._events)

    def _post(self, event: str, payload: dict[str, Any]) -> None:
        if not self._can_send(event):
            return

        body = {
            "event": event,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        text = body.get("text") or f"[{event}] {json.dumps(payload, ensure_ascii=False)}"
        slack_payload = {
            "text": text,
            "attachments": [
                {
                    "color": _event_color(event),
                    "fields": [
                        {"title": key, "value": str(value), "short": True}
                        for key, value in body.items()
                        if key not in {"text"}
                    ][:12],
                }
            ],
        }

        try:
            response = requests.post(self._url, json=slack_payload, timeout=8.0)
            if response.status_code >= 400:
                logger.warning("Webhook notification failed (%s): %s", response.status_code, response.text[:200])
        except Exception as exc:  # pragma: no cover - network boundary
            logger.warning("Webhook notification error: %s", exc)

    def batch_start(self, *, batch_id: str, catalog_id: str, total_books: int, workers: int, models: list[str]) -> None:
        self._post(
            "batch_start",
            {
                "text": f"Batch started: {batch_id} ({catalog_id}) • books={total_books} • workers={workers}",
                "batch_id": batch_id,
                "catalog": catalog_id,
                "total_books": total_books,
                "workers": workers,
                "models": ", ".join(models),
            },
        )

    def milestone(
        self,
        *,
        batch_id: str,
        catalog_id: str,
        completed_books: int,
        total_books: int,
        avg_cost_per_book: float,
        estimated_completion: str | None,
    ) -> None:
        self._post(
            "milestone",
            {
                "text": (
                    f"Batch milestone: {batch_id} ({catalog_id}) • {completed_books}/{total_books} books complete "
                    f"• avg_cost/book=${avg_cost_per_book:.3f}"
                ),
                "batch_id": batch_id,
                "catalog": catalog_id,
                "completed_books": completed_books,
                "total_books": total_books,
                "avg_cost_per_book": round(avg_cost_per_book, 4),
                "estimated_completion": estimated_completion,
            },
        )

    def batch_error(self, *, batch_id: str, catalog_id: str, book_number: int, error: str) -> None:
        self._post(
            "batch_error",
            {
                "text": f"Batch error: {batch_id} ({catalog_id}) • book={book_number} • {error}",
                "batch_id": batch_id,
                "catalog": catalog_id,
                "book": book_number,
                "error": error,
            },
        )

    def batch_complete(
        self,
        *,
        batch_id: str,
        catalog_id: str,
        completed_books: int,
        failed_books: int,
        total_books: int,
        total_cost: float,
        estimated_completion: str | None,
    ) -> None:
        self._post(
            "batch_complete",
            {
                "text": (
                    f"Batch complete: {batch_id} ({catalog_id}) • completed={completed_books}/{total_books} "
                    f"• failed={failed_books} • total_cost=${total_cost:.2f}"
                ),
                "batch_id": batch_id,
                "catalog": catalog_id,
                "completed_books": completed_books,
                "failed_books": failed_books,
                "total_books": total_books,
                "total_cost": round(total_cost, 4),
                "estimated_completion": estimated_completion,
            },
        )


def _event_color(event: str) -> str:
    if event == "batch_complete":
        return "#2eb886"
    if event == "batch_error":
        return "#e74c3c"
    if event == "milestone":
        return "#f1c40f"
    return "#3498db"
