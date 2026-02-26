"""SQLite schema and initialization helpers for scale workloads."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS books (
      book_number INTEGER NOT NULL,
      catalog_id TEXT NOT NULL DEFAULT 'classic-literature',
      title TEXT NOT NULL,
      author TEXT,
      genre TEXT,
      source_path TEXT,
      status TEXT DEFAULT 'pending',
      quality_score REAL,
      winner_variant INTEGER,
      tags TEXT,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (book_number, catalog_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS variants (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      book_number INTEGER NOT NULL,
      variant_number INTEGER NOT NULL,
      catalog_id TEXT NOT NULL,
      model TEXT,
      provider TEXT,
      prompt TEXT,
      quality_score REAL,
      quality_breakdown TEXT,
      is_winner BOOLEAN DEFAULT FALSE,
      output_path TEXT,
      thumbnail_path TEXT,
      generated_at TEXT,
      generation_duration_seconds REAL,
      FOREIGN KEY (book_number, catalog_id) REFERENCES books(book_number, catalog_id) ON DELETE CASCADE,
      UNIQUE(book_number, variant_number, catalog_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS generations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      book_number INTEGER,
      catalog_id TEXT,
      job_id TEXT,
      model TEXT,
      provider TEXT,
      prompt_template TEXT,
      variants_requested INTEGER,
      variants_generated INTEGER,
      cost_usd REAL,
      duration_seconds REAL,
      status TEXT,
      error TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS jobs (
      job_id TEXT PRIMARY KEY,
      catalog_id TEXT,
      job_type TEXT,
      priority INTEGER DEFAULT 3,
      status TEXT DEFAULT 'queued',
      progress REAL DEFAULT 0.0,
      books_total INTEGER DEFAULT 0,
      books_completed INTEGER DEFAULT 0,
      books_failed INTEGER DEFAULT 0,
      params TEXT,
      result TEXT,
      error TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      started_at TEXT,
      completed_at TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS costs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      catalog_id TEXT,
      book_number INTEGER,
      job_id TEXT,
      model TEXT,
      provider TEXT,
      operation TEXT,
      tokens_in INTEGER DEFAULT 0,
      tokens_out INTEGER DEFAULT 0,
      images_generated INTEGER DEFAULT 0,
      cost_usd REAL DEFAULT 0.0,
      duration_seconds REAL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      catalog_id TEXT,
      action TEXT NOT NULL,
      book_number INTEGER,
      details TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_books_catalog ON books(catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_books_status ON books(status);",
    "CREATE INDEX IF NOT EXISTS idx_books_title_author ON books(title, author);",
    "CREATE INDEX IF NOT EXISTS idx_variants_book ON variants(book_number, catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_variants_winner ON variants(is_winner);",
    "CREATE INDEX IF NOT EXISTS idx_generations_book ON generations(book_number, catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_costs_catalog ON costs(catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_costs_created ON costs(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_audit_catalog ON audit_log(catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);",
    "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);",
    "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at);",
    "CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);",
    """
    CREATE VIRTUAL TABLE IF NOT EXISTS books_fts
    USING fts5(title, author, catalog_id UNINDEXED, book_number UNINDEXED);
    """,
    "DROP TRIGGER IF EXISTS trg_books_fts_insert;",
    "DROP TRIGGER IF EXISTS trg_books_fts_update;",
    "DROP TRIGGER IF EXISTS trg_books_fts_delete;",
    """
    CREATE TRIGGER trg_books_fts_insert
    AFTER INSERT ON books
    BEGIN
      INSERT INTO books_fts(rowid, title, author, catalog_id, book_number)
      VALUES (new.rowid, new.title, COALESCE(new.author, ''), new.catalog_id, new.book_number);
    END;
    """,
    """
    CREATE TRIGGER trg_books_fts_update
    AFTER UPDATE ON books
    BEGIN
      DELETE FROM books_fts WHERE rowid = old.rowid;
      INSERT INTO books_fts(rowid, title, author, catalog_id, book_number)
      VALUES (new.rowid, new.title, COALESCE(new.author, ''), new.catalog_id, new.book_number);
    END;
    """,
    """
    CREATE TRIGGER trg_books_fts_delete
    AFTER DELETE ON books
    BEGIN
      DELETE FROM books_fts WHERE rowid = old.rowid;
    END;
    """,
]


def open_connection(db_path: Path | str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize_database(db_path: Path | str) -> dict[str, Any]:
    conn = open_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return {"ok": True, "db_path": str(Path(db_path)), "statements": len(SCHEMA_STATEMENTS)}


def table_counts(db_path: Path | str) -> dict[str, int]:
    conn = open_connection(db_path)
    try:
        tables = ["books", "variants", "generations", "jobs", "costs", "audit_log"]
        out: dict[str, int] = {}
        for table in tables:
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            out[table] = int((row["c"] if row else 0) or 0)
        return out
    finally:
        conn.close()
