"""SQLite-backed persistent job store for async orchestration."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.logger import get_logger

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


def _normalize_status(raw: str) -> str:
    token = str(raw or "").strip().lower()
    if token in {"queued", "running", "retrying", "completed", "failed", "cancelled", "paused"}:
        return token
    return "queued"


@dataclass(slots=True)
class IdempotencyConflictError(ValueError):
    """Raised when an idempotency key is reused for different job semantics."""

    idempotency_key: str
    existing_job_id: str
    existing_status: str
    conflict_fields: list[str]
    message: str = "idempotency key already exists for a different job payload"

    def to_dict(self) -> dict[str, Any]:
        return {
            "idempotency_key": str(self.idempotency_key),
            "existing_job_id": str(self.existing_job_id),
            "existing_status": str(self.existing_status),
            "conflict_fields": list(self.conflict_fields),
        }


@dataclass(slots=True)
class JobRecord:
    id: str
    idempotency_key: str
    job_type: str
    status: str
    catalog_id: str
    book_number: int
    payload: dict[str, Any]
    result: dict[str, Any]
    error: dict[str, Any]
    attempts: int
    max_attempts: int
    priority: int
    retry_after: str
    created_at: str
    updated_at: str
    started_at: str
    finished_at: str
    worker_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = _normalize_status(self.status)
        return payload


class JobStore:
    """Persistent queue metadata using SQLite with leasing semantics."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
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
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    job_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    catalog_id TEXT NOT NULL,
                    book_number INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    result_json TEXT,
                    error_json TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL DEFAULT 3,
                    priority INTEGER NOT NULL DEFAULT 100,
                    retry_after TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    worker_id TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    error_text TEXT,
                    meta_json TEXT,
                    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_retry ON jobs(status, retry_after, created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_catalog_book ON jobs(catalog_id, book_number, created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_attempts_job ON job_attempts(job_id, attempt_number)")

    @staticmethod
    def _row_to_job(row: sqlite3.Row | None) -> JobRecord | None:
        if row is None:
            return None
        return JobRecord(
            id=str(row["id"]),
            idempotency_key=str(row["idempotency_key"]),
            job_type=str(row["job_type"]),
            status=_normalize_status(str(row["status"])),
            catalog_id=str(row["catalog_id"]),
            book_number=int(row["book_number"] or 0),
            payload=_from_json(row["payload_json"], {}),
            result=_from_json(row["result_json"], {}),
            error=_from_json(row["error_json"], {}),
            attempts=int(row["attempts"] or 0),
            max_attempts=int(row["max_attempts"] or 0),
            priority=int(row["priority"] or 100),
            retry_after=str(row["retry_after"] or ""),
            created_at=str(row["created_at"] or ""),
            updated_at=str(row["updated_at"] or ""),
            started_at=str(row["started_at"] or ""),
            finished_at=str(row["finished_at"] or ""),
            worker_id=str(row["worker_id"] or ""),
        )

    def create_or_get_job(
        self,
        *,
        job_id: str,
        idempotency_key: str,
        job_type: str,
        catalog_id: str,
        book_number: int,
        payload: dict[str, Any],
        max_attempts: int = 3,
        priority: int = 100,
    ) -> tuple[JobRecord, bool]:
        now = _utc_now_iso()
        inserted = False
        with self._managed_connection() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO jobs (
                        id, idempotency_key, job_type, status, catalog_id, book_number,
                        payload_json, attempts, max_attempts, priority, retry_after,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, 'queued', ?, ?, ?, 0, ?, ?, NULL, ?, ?)
                    """,
                    (
                        str(job_id),
                        str(idempotency_key),
                        str(job_type),
                        str(catalog_id),
                        int(book_number),
                        _as_json(payload),
                        max(1, int(max_attempts)),
                        int(priority),
                        now,
                        now,
                    ),
                )
                inserted = True
            except sqlite3.IntegrityError:
                inserted = False

            row = conn.execute("SELECT * FROM jobs WHERE idempotency_key = ?", (str(idempotency_key),)).fetchone()
            job = self._row_to_job(row)
            if job is None:
                raise RuntimeError(f"Failed to create or load job for idempotency key: {idempotency_key}")
            if not inserted:
                conflicts: list[str] = []
                if str(job.job_type) != str(job_type):
                    conflicts.append("job_type")
                if str(job.catalog_id) != str(catalog_id):
                    conflicts.append("catalog_id")
                if int(job.book_number) != int(book_number):
                    conflicts.append("book_number")
                if _as_json(job.payload) != _as_json(payload):
                    conflicts.append("payload")
                if conflicts:
                    raise IdempotencyConflictError(
                        idempotency_key=str(idempotency_key),
                        existing_job_id=str(job.id),
                        existing_status=str(job.status),
                        conflict_fields=conflicts,
                    )
            return job, inserted

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._managed_connection() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),)).fetchone()
            return self._row_to_job(row)

    def get_job_by_idempotency_key(self, idempotency_key: str) -> JobRecord | None:
        with self._managed_connection() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE idempotency_key = ?", (str(idempotency_key),)).fetchone()
            return self._row_to_job(row)

    def list_jobs(
        self,
        *,
        limit: int = 100,
        statuses: list[str] | None = None,
        catalog_id: str | None = None,
        book_number: int | None = None,
    ) -> list[JobRecord]:
        limit = max(1, min(1000, int(limit)))
        where: list[str] = []
        args: list[Any] = []
        if statuses:
            normalized = [_normalize_status(item) for item in statuses if str(item).strip()]
            if normalized:
                where.append(f"status IN ({','.join('?' for _ in normalized)})")
                args.extend(normalized)
        if catalog_id:
            where.append("catalog_id = ?")
            args.append(str(catalog_id))
        if book_number and int(book_number) > 0:
            where.append("book_number = ?")
            args.append(int(book_number))

        query = "SELECT * FROM jobs"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)

        with self._managed_connection() as conn:
            rows = conn.execute(query, tuple(args)).fetchall()
        out: list[JobRecord] = []
        for row in rows:
            job = self._row_to_job(row)
            if job is not None:
                out.append(job)
        return out

    def lease_next_job(self, *, worker_id: str, job_types: list[str] | None = None) -> JobRecord | None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            where = ["status IN ('queued', 'retrying')", "(retry_after IS NULL OR retry_after <= ?)"]
            args: list[Any] = [now]
            if job_types:
                where.append(f"job_type IN ({','.join('?' for _ in job_types)})")
                args.extend([str(item) for item in job_types])
            query = (
                "SELECT id FROM jobs WHERE "
                + " AND ".join(where)
                + " ORDER BY priority ASC, created_at ASC LIMIT 1"
            )
            row = conn.execute(query, tuple(args)).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None

            job_id = str(row["id"])
            conn.execute(
                """
                UPDATE jobs
                   SET status = 'running',
                       worker_id = ?,
                       started_at = COALESCE(started_at, ?),
                       updated_at = ?,
                       retry_after = NULL
                 WHERE id = ?
                """,
                (str(worker_id), now, now, job_id),
            )
            full_row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            conn.execute("COMMIT")
            return self._row_to_job(full_row)

    def mark_completed(self, job_id: str, *, result: dict[str, Any]) -> JobRecord | None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                   SET status = 'completed',
                       result_json = ?,
                       error_json = NULL,
                       updated_at = ?,
                       finished_at = ?,
                       retry_after = NULL
                 WHERE id = ?
                """,
                (_as_json(result), now, now, str(job_id)),
            )
        return self.get_job(job_id)

    def mark_cancelled(self, job_id: str, *, reason: str = "cancelled") -> JobRecord | None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                   SET status = 'cancelled',
                       error_json = ?,
                       updated_at = ?,
                       finished_at = ?,
                       retry_after = NULL
                 WHERE id = ? AND status IN ('queued', 'retrying', 'running', 'paused')
                """,
                (_as_json({"message": str(reason)}), now, now, str(job_id)),
            )
        return self.get_job(job_id)

    def mark_paused(self, job_id: str, *, reason: str = "paused") -> JobRecord | None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                   SET status = 'paused',
                       error_json = ?,
                       updated_at = ?,
                       retry_after = NULL
                 WHERE id = ? AND status IN ('queued', 'retrying', 'running')
                """,
                (_as_json({"message": str(reason)}), now, str(job_id)),
            )
        return self.get_job(job_id)

    def resume_job(self, job_id: str) -> JobRecord | None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                   SET status = 'queued',
                       updated_at = ?,
                       retry_after = NULL,
                       worker_id = NULL
                 WHERE id = ? AND status = 'paused'
                """,
                (now, str(job_id)),
            )
        return self.get_job(job_id)

    def retry_job(self, job_id: str) -> JobRecord | None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute(
                """
                UPDATE jobs
                   SET status = 'queued',
                       updated_at = ?,
                       retry_after = NULL,
                       started_at = NULL,
                       finished_at = NULL,
                       worker_id = NULL
                 WHERE id = ? AND status IN ('failed', 'cancelled')
                """,
                (now, str(job_id)),
            )
        return self.get_job(job_id)

    def delete_job(self, job_id: str) -> bool:
        with self._managed_connection() as conn:
            cursor = conn.execute(
                "DELETE FROM jobs WHERE id = ? AND status IN ('completed', 'failed', 'cancelled')",
                (str(job_id),),
            )
        return int(cursor.rowcount or 0) > 0

    def mark_failed(
        self,
        job_id: str,
        *,
        error: dict[str, Any],
        retryable: bool,
        retry_delay_seconds: float = 0.0,
    ) -> JobRecord | None:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        with self._managed_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT attempts, max_attempts FROM jobs WHERE id = ?", (str(job_id),)).fetchone()
            if row is None:
                conn.execute("COMMIT")
                return None
            attempts = int(row["attempts"] or 0) + 1
            max_attempts = max(1, int(row["max_attempts"] or 1))
            should_retry = bool(retryable) and attempts < max_attempts
            status = "retrying" if should_retry else "failed"
            retry_after = ""
            finished_at = ""
            if should_retry:
                retry_after = (now_dt + timedelta(seconds=max(0.0, float(retry_delay_seconds)))).isoformat()
            else:
                finished_at = now
            conn.execute(
                """
                UPDATE jobs
                   SET status = ?,
                       error_json = ?,
                       attempts = ?,
                       retry_after = ?,
                       updated_at = ?,
                       finished_at = CASE WHEN ? = '' THEN finished_at ELSE ? END
                 WHERE id = ?
                """,
                (
                    status,
                    _as_json(error),
                    attempts,
                    retry_after or None,
                    now,
                    finished_at,
                    finished_at,
                    str(job_id),
                ),
            )
            row_after = conn.execute("SELECT * FROM jobs WHERE id = ?", (str(job_id),)).fetchone()
            conn.execute("COMMIT")
            return self._row_to_job(row_after)

    def record_attempt_start(self, job_id: str, *, attempt_number: int, meta: dict[str, Any] | None = None) -> int:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO job_attempts(job_id, attempt_number, status, started_at, meta_json)
                VALUES (?, ?, 'running', ?, ?)
                """,
                (str(job_id), int(attempt_number), now, _as_json(meta or {})),
            )
            return int(cursor.lastrowid or 0)

    def record_attempt_end(
        self,
        attempt_id: int,
        *,
        status: str,
        error_text: str = "",
        meta: dict[str, Any] | None = None,
    ) -> None:
        now = _utc_now_iso()
        with self._managed_connection() as conn:
            conn.execute(
                """
                UPDATE job_attempts
                   SET status = ?,
                       finished_at = ?,
                       error_text = ?,
                       meta_json = ?
                 WHERE id = ?
                """,
                (
                    _normalize_status(status),
                    now,
                    str(error_text or ""),
                    _as_json(meta or {}),
                    int(attempt_id),
                ),
            )

    def list_attempts(self, job_id: str) -> list[dict[str, Any]]:
        with self._managed_connection() as conn:
            rows = conn.execute(
                "SELECT * FROM job_attempts WHERE job_id = ? ORDER BY attempt_number ASC, id ASC",
                (str(job_id),),
            ).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "id": int(row["id"]),
                    "job_id": str(row["job_id"]),
                    "attempt_number": int(row["attempt_number"]),
                    "status": _normalize_status(str(row["status"])),
                    "started_at": str(row["started_at"] or ""),
                    "finished_at": str(row["finished_at"] or ""),
                    "error_text": str(row["error_text"] or ""),
                    "meta": _from_json(row["meta_json"], {}),
                }
            )
        return out

    def status_counts(self) -> dict[str, int]:
        with self._managed_connection() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS c FROM jobs GROUP BY status").fetchall()
        counts = {"queued": 0, "running": 0, "retrying": 0, "paused": 0, "completed": 0, "failed": 0, "cancelled": 0}
        for row in rows:
            status = _normalize_status(str(row["status"]))
            counts[status] = int(row["c"] or 0)
        return counts

    def recover_stale_running_jobs(
        self,
        *,
        stale_after_seconds: float = 900.0,
        retry_delay_seconds: float = 2.0,
    ) -> int:
        """Recover stale running jobs after restart with attempt accounting."""
        stale_after = max(1.0, float(stale_after_seconds))
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        cutoff = (now_dt - timedelta(seconds=stale_after)).isoformat()
        retry_after = (now_dt + timedelta(seconds=max(0.0, float(retry_delay_seconds)))).isoformat()
        recovery_message = "Recovered stale running job after restart"
        with self._managed_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT id, attempts, max_attempts
                FROM jobs
                WHERE status = 'running'
                  AND (started_at IS NULL OR started_at <= ?)
                ORDER BY created_at ASC
                """,
                (cutoff,),
            ).fetchall()
            recovered = 0
            for row in rows:
                job_id = str(row["id"])
                attempts = int(row["attempts"] or 0) + 1
                max_attempts = max(1, int(row["max_attempts"] or 1))
                should_retry = attempts < max_attempts
                next_status = "retrying" if should_retry else "failed"
                finished_at = "" if should_retry else now
                recovery_error = {
                    "message": recovery_message,
                    "recovered_at": now,
                    "job_id": job_id,
                    "attempts": attempts,
                    "max_attempts": max_attempts,
                    "retryable": should_retry,
                }
                conn.execute(
                    """
                    UPDATE jobs
                       SET status = ?,
                           worker_id = NULL,
                           retry_after = ?,
                           updated_at = ?,
                           error_json = ?,
                           attempts = ?,
                           finished_at = CASE WHEN ? = '' THEN finished_at ELSE ? END
                     WHERE id = ?
                    """,
                    (
                        next_status,
                        retry_after if should_retry else None,
                        now,
                        _as_json(recovery_error),
                        attempts,
                        finished_at,
                        finished_at,
                        job_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE job_attempts
                       SET status = 'failed',
                           finished_at = CASE
                               WHEN finished_at IS NULL OR finished_at = '' THEN ?
                               ELSE finished_at
                           END,
                           error_text = CASE
                               WHEN error_text IS NULL OR error_text = '' THEN ?
                               ELSE error_text
                           END
                     WHERE job_id = ?
                       AND status = 'running'
                    """,
                    (
                        now,
                        recovery_message,
                        job_id,
                    ),
                )
                recovered += 1
            conn.execute("COMMIT")
        if recovered > 0:
            logger.warning(
                "Recovered stale running jobs",
                extra={"recovered": recovered, "stale_after_seconds": stale_after},
            )
        return recovered

    def slo_summary(self, *, window_days: int = 7, catalog_id: str | None = None) -> dict[str, Any]:
        """Aggregate job reliability counters for SLO evaluation."""
        days = max(1, int(window_days))
        since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        where = ["created_at >= ?"]
        args: list[Any] = [since]
        if catalog_id:
            where.append("catalog_id = ?")
            args.append(str(catalog_id))
        where_sql = " AND ".join(where)

        with self._managed_connection() as conn:
            terminal_row = conn.execute(
                f"""
                SELECT
                    COUNT(*) AS terminal_total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status IN ('failed', 'cancelled') THEN 1 ELSE 0 END) AS manual_total
                FROM jobs
                WHERE {where_sql}
                  AND status IN ('completed', 'failed', 'cancelled')
                """,
                tuple(args),
            ).fetchone()
            retry_row = conn.execute(
                f"""
                SELECT COUNT(*) AS retry_jobs
                FROM jobs
                WHERE {where_sql}
                  AND status = 'completed'
                  AND attempts > 0
                """,
                tuple(args),
            ).fetchone()
            duration_row = conn.execute(
                f"""
                SELECT
                    AVG((julianday(started_at) - julianday(created_at)) * 86400.0) AS avg_queue_seconds,
                    AVG((julianday(finished_at) - julianday(started_at)) * 86400.0) AS avg_runtime_seconds
                FROM jobs
                WHERE {where_sql}
                  AND started_at IS NOT NULL
                  AND finished_at IS NOT NULL
                  AND status IN ('completed', 'failed', 'cancelled')
                """,
                tuple(args),
            ).fetchone()

        terminal_total = int((terminal_row["terminal_total"] if terminal_row else 0) or 0)
        completed_total = int((terminal_row["completed_total"] if terminal_row else 0) or 0)
        manual_total = int((terminal_row["manual_total"] if terminal_row else 0) or 0)
        retry_jobs = int((retry_row["retry_jobs"] if retry_row else 0) or 0)
        avg_queue = float((duration_row["avg_queue_seconds"] if duration_row else 0.0) or 0.0)
        avg_runtime = float((duration_row["avg_runtime_seconds"] if duration_row else 0.0) or 0.0)

        completion_without_manual = (completed_total / terminal_total) if terminal_total else 1.0
        same_stage_retry_rate = (retry_jobs / completed_total) if completed_total else 0.0

        return {
            "window_days": days,
            "since": since,
            "terminal_total": terminal_total,
            "completed_total": completed_total,
            "manual_total": manual_total,
            "retry_jobs": retry_jobs,
            "completion_without_manual_intervention": round(completion_without_manual, 6),
            "same_stage_retry_rate": round(same_stage_retry_rate, 6),
            "avg_queue_seconds": round(avg_queue, 3),
            "avg_runtime_seconds": round(avg_runtime, 3),
        }
