"""Lightweight SQLite connection pool with retry semantics."""

from __future__ import annotations

import queue
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    from src import database
except ModuleNotFoundError:  # pragma: no cover
    import database  # type: ignore


class Database:
    """Connection-pooled SQLite access layer for repository and API usage."""

    def __init__(self, db_path: str | Path = "data/alexandria.db", *, pool_size: int = 5):
        self.db_path = Path(db_path)
        self.pool_size = max(1, int(pool_size))
        self._closed = False
        database.initialize_database(self.db_path)
        self._pool: queue.Queue[sqlite3.Connection] = queue.Queue(maxsize=self.pool_size)
        for _ in range(self.pool_size):
            self._pool.put(database.open_connection(self.db_path))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
            except queue.Empty:
                break
            conn.close()

    def __del__(self):  # pragma: no cover - defensive finalizer
        try:
            self.close()
        except Exception:
            pass

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = self._pool.get()
        try:
            yield conn
        finally:
            self._pool.put(conn)

    def _execute_with_retry(self, fn, *, retries: int = 3, backoff_seconds: float = 0.1):
        attempt = 0
        while True:
            attempt += 1
            try:
                return fn()
            except sqlite3.OperationalError as exc:
                if "database is locked" not in str(exc).lower() and "sqlite_busy" not in str(exc).lower():
                    raise
                if attempt >= max(1, int(retries)):
                    raise
                time.sleep(float(backoff_seconds) * attempt)

    def query(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        def _run() -> list[dict[str, Any]]:
            with self.connection() as conn:
                rows = conn.execute(sql, params).fetchall()
                return [dict(row) for row in rows]

        return self._execute_with_retry(_run)

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> int:
        def _run() -> int:
            with self.connection() as conn:
                cursor = conn.execute(sql, params)
                return int(cursor.rowcount)

        return self._execute_with_retry(_run)

    def executemany(self, sql: str, params: list[tuple[Any, ...]]) -> int:
        if not params:
            return 0

        def _run() -> int:
            with self.connection() as conn:
                cursor = conn.executemany(sql, params)
                return int(cursor.rowcount if cursor.rowcount is not None else len(params))

        return self._execute_with_retry(_run)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
