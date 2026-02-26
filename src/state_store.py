"""SQLite-backed state store for generation history and winner selections."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from src.logger import get_logger
from src import safe_json

logger = get_logger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _from_json(payload: str | None, default: Any) -> Any:
    if not payload:
        return default
    try:
        return json.loads(payload)
    except Exception:
        return default


class StateStore:
    """Persistent runtime state using SQLite with JSON compatibility bridges."""

    def __init__(
        self,
        db_path: Path,
        *,
        write_retry_attempts: int = 4,
        write_retry_base_delay_seconds: float = 0.05,
    ):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_retry_attempts = max(1, int(write_retry_attempts))
        self.write_retry_base_delay_seconds = max(0.0, float(write_retry_base_delay_seconds))
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def _managed_connection(self):
        conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._managed_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS generation_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    catalog_id TEXT NOT NULL,
                    book_number INTEGER NOT NULL,
                    variant INTEGER NOT NULL,
                    model TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    image_path TEXT,
                    composited_path TEXT,
                    success INTEGER NOT NULL DEFAULT 1,
                    error_text TEXT,
                    generation_time REAL NOT NULL DEFAULT 0,
                    cost REAL NOT NULL DEFAULT 0,
                    dry_run INTEGER NOT NULL DEFAULT 0,
                    similarity_warning TEXT,
                    similar_to_book INTEGER NOT NULL DEFAULT 0,
                    distinctiveness_score REAL NOT NULL DEFAULT 0,
                    timestamp TEXT NOT NULL,
                    fit_overlay_path TEXT,
                    job_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS winner_selections (
                    catalog_id TEXT NOT NULL,
                    book_number INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(catalog_id, book_number)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_generation_records_catalog_time "
                "ON generation_records(catalog_id, timestamp, id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_generation_records_catalog_book "
                "ON generation_records(catalog_id, book_number, timestamp)"
            )
            columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(generation_records)").fetchall()
                if isinstance(row, sqlite3.Row)
            }
            if "job_id" not in columns:
                conn.execute("ALTER TABLE generation_records ADD COLUMN job_id TEXT NOT NULL DEFAULT ''")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_generation_records_catalog_job "
                "ON generation_records(catalog_id, job_id, book_number, variant, model, provider, dry_run)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_winner_catalog_book "
                "ON winner_selections(catalog_id, book_number)"
            )

    @staticmethod
    def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
        message = str(exc).strip().lower()
        return "database is locked" in message or "database table is locked" in message

    def _run_write_transaction(
        self,
        operation: str,
        callback: Callable[[sqlite3.Connection], int],
    ) -> int:
        attempts = max(1, int(self.write_retry_attempts))
        for attempt in range(1, attempts + 1):
            with self._managed_connection() as conn:
                began = False
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    began = True
                    result = callback(conn)
                    conn.execute("COMMIT")
                    began = False
                    return int(result)
                except sqlite3.OperationalError as exc:
                    if began:
                        try:
                            conn.execute("ROLLBACK")
                        except Exception:
                            pass
                    if (not self._is_lock_error(exc)) or attempt >= attempts:
                        raise
                    delay = min(2.0, self.write_retry_base_delay_seconds * (2 ** (attempt - 1)))
                    logger.warning(
                        "StateStore write contention, retrying",
                        extra={
                            "operation": operation,
                            "attempt": attempt,
                            "max_attempts": attempts,
                            "delay_seconds": round(delay, 4),
                            "db_path": str(self.db_path),
                        },
                    )
                    if delay > 0:
                        time.sleep(delay)
                except Exception:
                    if began:
                        try:
                            conn.execute("ROLLBACK")
                        except Exception:
                            pass
                    raise
        return 0

    def append_generation_records(
        self,
        *,
        catalog_id: str,
        records: list[dict[str, Any]],
        job_id: str = "",
    ) -> int:
        if not records:
            return 0
        now = _utc_now_iso()
        job_token = str(job_id or "").strip()
        def _write(conn: sqlite3.Connection) -> int:
            count = 0
            for row in records:
                if not isinstance(row, dict):
                    continue
                book_number = int(row.get("book_number", 0) or 0)
                variant = int(row.get("variant", 0) or 0)
                if book_number <= 0 or variant <= 0:
                    continue
                model = str(row.get("model", "unknown"))
                provider = str(row.get("provider", ""))
                dry_run = 1 if bool(row.get("dry_run", False)) else 0
                if job_token:
                    existing = conn.execute(
                        """
                        SELECT 1
                        FROM generation_records
                        WHERE catalog_id = ?
                          AND job_id = ?
                          AND book_number = ?
                          AND variant = ?
                          AND model = ?
                          AND provider = ?
                          AND dry_run = ?
                        LIMIT 1
                        """,
                        (
                            str(catalog_id),
                            job_token,
                            book_number,
                            variant,
                            model,
                            provider,
                            dry_run,
                        ),
                    ).fetchone()
                    if existing is not None:
                        continue
                conn.execute(
                    """
                    INSERT INTO generation_records (
                        catalog_id,
                        book_number,
                        variant,
                        model,
                        prompt,
                        provider,
                        image_path,
                        composited_path,
                        success,
                        error_text,
                        generation_time,
                        cost,
                        dry_run,
                        similarity_warning,
                        similar_to_book,
                        distinctiveness_score,
                        timestamp,
                        fit_overlay_path,
                        job_id,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(catalog_id),
                        book_number,
                        variant,
                        model,
                        str(row.get("prompt", "")),
                        provider,
                        row.get("image_path"),
                        row.get("composited_path"),
                        1 if bool(row.get("success", True)) else 0,
                        str(row.get("error", "") or ""),
                        float(row.get("generation_time", 0.0) or 0.0),
                        float(row.get("cost", 0.0) or 0.0),
                        dry_run,
                        str(row.get("similarity_warning", "") or ""),
                        int(row.get("similar_to_book", 0) or 0),
                        float(row.get("distinctiveness_score", 0.0) or 0.0),
                        str(row.get("timestamp", "") or now),
                        row.get("fit_overlay_path"),
                        job_token,
                        now,
                    ),
                )
                count += 1
            return count

        return self._run_write_transaction("append_generation_records", _write)

    def count_generation_records(self, *, catalog_id: str) -> int:
        with self._managed_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM generation_records WHERE catalog_id = ?",
                (str(catalog_id),),
            ).fetchone()
        return int((row["c"] if row else 0) or 0)

    def list_generation_records(self, *, catalog_id: str, limit: int = 5000) -> list[dict[str, Any]]:
        size = max(1, min(50000, int(limit)))
        with self._managed_connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM (
                    SELECT *
                    FROM generation_records
                    WHERE catalog_id = ?
                    ORDER BY timestamp DESC, id DESC
                    LIMIT ?
                )
                ORDER BY timestamp ASC, id ASC
                """,
                (str(catalog_id), size),
            ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "book_number": int(row["book_number"] or 0),
                    "variant": int(row["variant"] or 0),
                    "model": str(row["model"] or "unknown"),
                    "prompt": str(row["prompt"] or ""),
                    "provider": str(row["provider"] or ""),
                    "image_path": row["image_path"],
                    "composited_path": row["composited_path"],
                    "success": bool(int(row["success"] or 0)),
                    "error": str(row["error_text"] or ""),
                    "generation_time": float(row["generation_time"] or 0.0),
                    "cost": float(row["cost"] or 0.0),
                    "dry_run": bool(int(row["dry_run"] or 0)),
                    "similarity_warning": str(row["similarity_warning"] or ""),
                    "similar_to_book": int(row["similar_to_book"] or 0),
                    "distinctiveness_score": float(row["distinctiveness_score"] or 0.0),
                    "timestamp": str(row["timestamp"] or ""),
                    "fit_overlay_path": row["fit_overlay_path"],
                    "job_id": str(row["job_id"] or ""),
                }
            )
        return out

    def export_history_payload(self, *, catalog_id: str, limit: int = 5000) -> dict[str, Any]:
        return {
            "updated_at": _utc_now_iso(),
            "items": self.list_generation_records(catalog_id=catalog_id, limit=limit),
        }

    def upsert_winner_selections(
        self,
        *,
        catalog_id: str,
        selections: dict[str, Any],
        replace: bool = True,
    ) -> int:
        now = _utc_now_iso()
        normalized: dict[int, dict[str, Any]] = {}
        for raw_book, raw_value in selections.items():
            try:
                book = int(str(raw_book).strip())
            except ValueError:
                continue
            if book <= 0:
                continue
            if isinstance(raw_value, dict):
                payload = dict(raw_value)
            else:
                try:
                    payload = {"winner": int(raw_value or 0)}
                except (TypeError, ValueError):
                    continue
            normalized[book] = payload

        def _write(conn: sqlite3.Connection) -> int:
            if replace:
                conn.execute("DELETE FROM winner_selections WHERE catalog_id = ?", (str(catalog_id),))
            for book, payload in normalized.items():
                conn.execute(
                    """
                    INSERT INTO winner_selections(catalog_id, book_number, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(catalog_id, book_number)
                    DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at
                    """,
                    (
                        str(catalog_id),
                        int(book),
                        _as_json(payload),
                        now,
                    ),
                )
            return len(normalized)

        return self._run_write_transaction("upsert_winner_selections", _write)

    def load_winner_selections(self, *, catalog_id: str) -> dict[str, Any]:
        with self._managed_connection() as conn:
            rows = conn.execute(
                """
                SELECT book_number, payload_json
                FROM winner_selections
                WHERE catalog_id = ?
                ORDER BY book_number ASC
                """,
                (str(catalog_id),),
            ).fetchall()
        out: dict[str, Any] = {}
        for row in rows:
            book = int(row["book_number"] or 0)
            if book <= 0:
                continue
            out[str(book)] = _from_json(row["payload_json"], {})
        return out

    def count_winner_selections(self, *, catalog_id: str) -> int:
        with self._managed_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM winner_selections WHERE catalog_id = ?",
                (str(catalog_id),),
            ).fetchone()
        return int((row["c"] if row else 0) or 0)

    def bootstrap_from_json(self, *, catalog_id: str, history_path: Path, winner_path: Path) -> dict[str, int]:
        imported_history = 0
        imported_winners = 0
        if self.count_generation_records(catalog_id=catalog_id) == 0 and history_path.exists():
            payload = safe_json.load_json(history_path, {"items": []})
            rows = payload.get("items", []) if isinstance(payload, dict) else []
            if isinstance(rows, list) and rows:
                imported_history = self.append_generation_records(catalog_id=catalog_id, records=rows)

        if self.count_winner_selections(catalog_id=catalog_id) == 0 and winner_path.exists():
            payload = safe_json.load_json(winner_path, {"selections": {}})

            selections: dict[str, Any]
            if isinstance(payload, dict) and isinstance(payload.get("selections"), dict):
                selections = payload.get("selections", {})
            elif isinstance(payload, dict):
                selections = payload
            else:
                selections = {}

            if selections:
                imported_winners = self.upsert_winner_selections(
                    catalog_id=catalog_id,
                    selections=selections,
                    replace=True,
                )

        if imported_history or imported_winners:
            logger.info(
                "Bootstrapped state store from JSON",
                extra={
                    "catalog": str(catalog_id),
                    "history_rows": imported_history,
                    "winner_rows": imported_winners,
                },
            )
        return {"history_rows": imported_history, "winner_rows": imported_winners}
