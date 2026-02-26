from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
import queue
import sqlite3

import pytest

from src import config
from src import database
from src import db as db_module
from src import repository


def test_database_initialize_and_counts(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    summary = database.initialize_database(db_path)
    assert summary["ok"] is True
    counts = database.table_counts(db_path)
    assert counts["books"] == 0
    assert counts["variants"] == 0


def test_database_initialize_rolls_back_on_schema_error(monkeypatch, tmp_path: Path):
    class _BrokenConn:
        def __init__(self):
            self.commands: list[str] = []
            self.closed = False

        def execute(self, sql):  # type: ignore[no-untyped-def]
            self.commands.append(str(sql))
            if "CREATE TABLE" in str(sql):
                raise sqlite3.OperationalError("forced failure")
            return None

        def close(self):  # type: ignore[no-untyped-def]
            self.closed = True

    broken = _BrokenConn()
    monkeypatch.setattr(database, "open_connection", lambda _path: broken)

    with pytest.raises(sqlite3.OperationalError):
        database.initialize_database(tmp_path / "broken.db")

    assert "BEGIN IMMEDIATE" in broken.commands[0]
    assert "ROLLBACK" in broken.commands
    assert broken.closed is True


def test_db_execute_query_transaction(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)
    with db.transaction() as conn:
        conn.execute(
            """
            INSERT INTO books (book_number, catalog_id, title, author, status)
            VALUES (1, 'classics', 'Title', 'Author', 'pending')
            """
        )
    rows = db.query("SELECT * FROM books WHERE catalog_id = ? AND book_number = ?", ("classics", 1))
    assert len(rows) == 1
    assert rows[0]["title"] == "Title"


def _build_runtime(tmp_path: Path) -> config.Config:
    config_dir = tmp_path / "config"
    data_dir = tmp_path / "data"
    output_dir = tmp_path / "Output Covers"
    input_dir = tmp_path / "Input Covers"
    tmp_dir = tmp_path / "tmp"
    for path in (config_dir, data_dir, output_dir, input_dir, tmp_dir):
        path.mkdir(parents=True, exist_ok=True)

    catalog_path = config_dir / "book_catalog.json"
    catalog_path.write_text(
        json.dumps(
            [
                {
                    "number": 1,
                    "title": "Hamlet",
                    "author": "William Shakespeare",
                    "genre": "drama",
                    "folder_name": "1. Hamlet - William Shakespeare",
                },
                {
                    "number": 2,
                    "title": "Moby Dick",
                    "author": "Herman Melville",
                    "genre": "fiction",
                    "folder_name": "2. Moby Dick - Herman Melville",
                },
            ]
        ),
        encoding="utf-8",
    )
    prompts = config_dir / "book_prompts.json"
    prompts.write_text("{}", encoding="utf-8")
    library = config_dir / "prompt_library.json"
    library.write_text(json.dumps({"prompts": [], "style_anchors": []}), encoding="utf-8")
    winners = data_dir / "winner_selections.json"
    winners.write_text(json.dumps({"selections": {"1": {"winner": 1}}}), encoding="utf-8")

    runtime = config.get_config()
    return replace(
        runtime,
        catalog_id="classics",
        config_dir=config_dir,
        data_dir=data_dir,
        output_dir=output_dir,
        input_dir=input_dir,
        tmp_dir=tmp_dir,
        book_catalog_path=catalog_path,
        prompts_path=prompts,
        prompt_library_path=library,
        use_sqlite=False,
    )


def test_json_repository_list_books_and_search(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    repo = repository.JsonBookRepository(runtime)
    rows, total = repo.list_books(catalog_id="classics", limit=25, offset=0, filters={"search": "hamlet"}, sort="title", order="asc")
    assert total == 1
    assert rows[0]["title"] == "Hamlet"


def test_sqlite_repository_roundtrip(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    database.initialize_database(db_path)
    db = db_module.Database(db_path)
    db.execute(
        """
        INSERT INTO books (book_number, catalog_id, title, author, status, tags, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "classics", "Book 1", "Author 1", "pending", '["tag1"]', "n1"),
    )
    repo = repository.SqliteBookRepository(db)
    rows, total = repo.list_books(catalog_id="classics", limit=25, offset=0, filters={"search": "Book"}, sort="book_number", order="asc")
    assert total == 1
    assert rows[0]["book_number"] == 1
    updated = repo.update_book(book_number=1, catalog_id="classics", data={"status": "processed", "tags": ["t2"]})
    assert updated["status"] == "processed"
    assert updated["tags"] == ["t2"]


def test_db_transaction_rolls_back_on_exception(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)
    with pytest.raises(RuntimeError):
        with db.transaction() as conn:
            conn.execute(
                """
                INSERT INTO books (book_number, catalog_id, title, author, status)
                VALUES (9, 'classics', 'Rollback', 'Author', 'pending')
                """
            )
            raise RuntimeError("force rollback")

    rows = db.query("SELECT COUNT(*) AS c FROM books")
    assert rows[0]["c"] == 0


def test_db_executemany_empty_and_close_idempotent(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)
    assert db.executemany("INSERT INTO books (book_number, catalog_id, title, author, status) VALUES (?, ?, ?, ?, ?)", []) == 0
    db.close()
    db.close()


def test_json_repository_variants_and_generation_history_filters(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    quality_path = runtime.data_dir / "quality_scores.json"
    quality_path.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 2, "overall_score": 0.85},
                    {"book_number": 2, "variant_id": 1, "overall_score": 0.65},
                ]
            }
        ),
        encoding="utf-8",
    )
    history_path = runtime.data_dir / "generation_history.json"
    history_path.write_text(
        json.dumps(
            {
                "items": [
                    {"book_number": 1, "model": "flux", "status": "success"},
                    {"book_number": 2, "model": "gpt-image", "status": "error"},
                ]
            }
        ),
        encoding="utf-8",
    )

    variant_dir = runtime.output_dir / "1. Hamlet - William Shakespeare" / "Variant-2"
    variant_dir.mkdir(parents=True, exist_ok=True)
    (variant_dir / "cover.jpg").write_bytes(b"x")

    repo = repository.JsonBookRepository(runtime)
    variants = repo.get_variants(book_number=1, catalog_id="classics")
    assert len(variants) == 1
    assert variants[0]["variant_number"] == 2
    assert variants[0]["quality_score"] == 0.85

    history, total = repo.list_generation_history(
        catalog_id="classics",
        limit=10,
        offset=0,
        filters={"book": 1, "model": "flux"},
    )
    assert total == 1
    assert history[0]["book_number"] == 1


def test_json_repository_quality_filters_reject_non_finite_values(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    quality_path = runtime.data_dir / "quality_scores.json"
    quality_path.write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 1, "overall_score": 0.9},
                    {"book_number": 2, "variant_id": 1, "overall_score": 0.4},
                ]
            }
        ),
        encoding="utf-8",
    )
    repo = repository.JsonBookRepository(runtime)
    rows, total = repo.list_books(
        catalog_id="classics",
        limit=25,
        offset=0,
        filters={"quality_min": "nan", "quality_max": "inf"},
        sort="book_number",
        order="asc",
    )
    assert total == 2
    assert len(rows) == 2


def test_sqlite_repository_missing_book_and_generation_filters(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    database.initialize_database(db_path)
    db = db_module.Database(db_path)
    db.execute(
        """
        INSERT INTO books (book_number, catalog_id, title, author, status, tags, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "classics", "Book 1", "Author 1", "pending", "[]", ""),
    )
    db.execute(
        """
        INSERT INTO generations (
            book_number, catalog_id, job_id, model, provider, prompt_template, variants_requested,
            variants_generated, cost_usd, duration_seconds, status, error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (1, "classics", "job-1", "flux", "openrouter", "tmpl", 3, 3, 0.3, 1.2, "success", ""),
    )
    db.execute(
        """
        INSERT INTO generations (
            book_number, catalog_id, job_id, model, provider, prompt_template, variants_requested,
            variants_generated, cost_usd, duration_seconds, status, error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (1, "classics", "job-2", "gpt-image", "openai", "tmpl2", 2, 1, 0.2, 2.5, "error", "timeout"),
    )

    repo = repository.SqliteBookRepository(db)
    assert repo.get_book(book_number=999, catalog_id="classics") is None

    rows, total = repo.list_generation_history(
        catalog_id="classics",
        limit=10,
        offset=0,
        filters={"provider": "openrouter", "status": "success"},
    )
    assert total == 1
    assert rows[0]["job_id"] == "job-1"


def test_get_repository_factory_json_and_sqlite(tmp_path: Path):
    runtime = _build_runtime(tmp_path)

    repo_json = repository.get_repository(runtime=runtime, use_sqlite=False)
    assert isinstance(repo_json, repository.JsonBookRepository)

    db_path = tmp_path / "alexandria.db"
    runtime_sqlite = replace(runtime, use_sqlite=True, sqlite_db_path=db_path)
    repo_sqlite = repository.get_repository(runtime=runtime_sqlite, use_sqlite=True, db_path=db_path)
    assert isinstance(repo_sqlite, repository.SqliteBookRepository)
    repo_sqlite.db.close()


def test_json_repository_handles_missing_and_invalid_json_inputs(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    runtime_non_classics = replace(runtime, catalog_id="science-fiction")
    repo = repository.JsonBookRepository(runtime_non_classics)

    # Missing generation history file path should return empty payload.
    history, total = repo.list_generation_history(catalog_id="science-fiction", limit=10, offset=0)
    assert history == []
    assert total == 0

    # Invalid JSON payloads should fail closed without raising.
    runtime_non_classics.book_catalog_path.write_text("{invalid", encoding="utf-8")
    (runtime_non_classics.data_dir / "winner_selections_science-fiction.json").write_text("{invalid", encoding="utf-8")
    (runtime_non_classics.data_dir / "quality_scores.json").write_text("{invalid", encoding="utf-8")
    (runtime_non_classics.data_dir / "generation_history.json").write_text("{invalid", encoding="utf-8")

    rows, total = repo.list_books(catalog_id="science-fiction", limit=10, offset=0, sort="unknown", order="desc")
    assert rows == []
    assert total == 0
    history, total = repo.list_generation_history(catalog_id="science-fiction", limit=10, offset=0)
    assert history == []
    assert total == 0


def test_json_repository_update_book_creates_metadata_entry(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    repo = repository.JsonBookRepository(runtime)

    updated = repo.update_book(
        book_number=777,
        catalog_id="classics",
        data={"tags": [" featured ", "", 123], "notes": "priority"},
    )
    assert updated["book_number"] == 777
    assert updated["tags"] == ["123", "featured"]
    assert updated["notes"] == "priority"


def test_json_repository_filters_and_variants_edge_paths(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    runtime.book_catalog_path.write_text(
        json.dumps(
            [
                {
                    "number": 1,
                    "title": "Hamlet",
                    "author": "William Shakespeare",
                    "genre": "drama",
                    "folder_name": "1. Hamlet - William Shakespeare",
                },
                "bad-row",
            ]
        ),
        encoding="utf-8",
    )
    (runtime.data_dir / "winner_selections.json").write_text(
        json.dumps({"selections": {"1": {"winner": 2}, "bad": {"winner": 9}, "2": 0}}),
        encoding="utf-8",
    )
    (runtime.data_dir / "quality_scores.json").write_text(
        json.dumps(
            {
                "scores": [
                    {"book_number": 1, "variant_id": 2, "overall_score": 0.92},
                    {"book_number": 1, "variant_id": 2, "overall_score": 0.7},
                    {"book_number": 1, "variant_id": 0, "overall_score": 1.0},
                    "bad-row",
                ]
            }
        ),
        encoding="utf-8",
    )
    meta_path = runtime.data_dir / "book_metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "books": {
                    "1": {
                        "tags": ["Classic", "Featured"],
                        "notes": "keep",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    repo = repository.JsonBookRepository(runtime)
    rows, total = repo.list_books(
        catalog_id="classics",
        limit=10,
        offset=0,
        filters={"status": "processed", "tags": "classic,featured"},
        sort="unknown-column",
        order="asc",
    )
    assert total == 1
    assert rows[0]["quality_score"] == 0.92
    assert rows[0]["winner_variant"] == 2

    # Missing folder should return no variants.
    assert repo.get_variants(book_number=999, catalog_id="classics") == []

    # Non-directory Variant-* entry should be ignored.
    book_dir = runtime.output_dir / "1. Hamlet - William Shakespeare"
    book_dir.mkdir(parents=True, exist_ok=True)
    (book_dir / "Variant-3").write_text("not-a-dir", encoding="utf-8")
    variant_dir = book_dir / "Variant-4"
    variant_dir.mkdir(parents=True, exist_ok=True)
    variants = repo.get_variants(book_number=1, catalog_id="classics")
    assert len(variants) == 1
    assert variants[0]["variant_number"] == 4
    assert variants[0]["output_path"] == ""


def test_json_repository_get_book_returns_none_for_missing_title(tmp_path: Path):
    runtime = _build_runtime(tmp_path)
    repo = repository.JsonBookRepository(runtime)
    assert repo.get_book(book_number=999, catalog_id="classics") is None


def test_sqlite_repository_filters_tags_and_invalid_json_tags(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    database.initialize_database(db_path)
    db = db_module.Database(db_path)
    db.execute(
        """
        INSERT INTO books (book_number, catalog_id, title, author, status, quality_score, tags, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "classics", "Book A", "Author A", "processed", 0.91, "{broken", "n"),
    )
    db.execute(
        """
        INSERT INTO books (book_number, catalog_id, title, author, status, quality_score, tags, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (2, "classics", "Book B", "Author B", "processed", 0.2, '{"label":"x"}', "n"),
    )

    repo = repository.SqliteBookRepository(db)
    rows, total = repo.list_books(
        catalog_id="classics",
        limit=10,
        offset=0,
        filters={"status": "processed", "quality_min": 0.5, "quality_max": 1.0, "tags": "broken"},
        sort="book_number",
        order="asc",
    )
    assert total == 1
    assert rows[0]["book_number"] == 1
    assert rows[0]["tags"] == []

    book = repo.get_book(book_number=1, catalog_id="classics")
    assert book is not None
    assert book["tags"] == []


def test_sqlite_repository_update_book_error_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "alexandria.db"
    database.initialize_database(db_path)
    db = db_module.Database(db_path)
    repo = repository.SqliteBookRepository(db)

    with pytest.raises(KeyError):
        repo.update_book(book_number=404, catalog_id="classics", data={"status": "processed"})

    db.execute(
        """
        INSERT INTO books (book_number, catalog_id, title, author, status, tags, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (5, "classics", "Book 5", "Author 5", "pending", "[]", ""),
    )
    original_get_book = repo.get_book
    calls = {"count": 0}

    def fake_get_book(*, book_number: int, catalog_id: str):  # type: ignore[no-untyped-def]
        calls["count"] += 1
        if calls["count"] == 1:
            return original_get_book(book_number=book_number, catalog_id=catalog_id)
        return None

    monkeypatch.setattr(repo, "get_book", fake_get_book)
    with pytest.raises(RuntimeError):
        repo.update_book(book_number=5, catalog_id="classics", data={"tags": "not-a-list"})


def test_sqlite_repository_variants_and_generation_filters_with_book_and_model(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    database.initialize_database(db_path)
    db = db_module.Database(db_path)
    db.execute(
        """
        INSERT INTO books (book_number, catalog_id, title, author, status, tags, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (1, "classics", "Book 1", "Author 1", "pending", "[]", ""),
    )
    db.execute(
        """
        INSERT INTO variants (book_number, variant_number, catalog_id, model, provider, prompt, quality_score, output_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, 2, "classics", "flux", "openrouter", "prompt", 0.8, "out.jpg"),
    )
    db.execute(
        """
        INSERT INTO generations (
            book_number, catalog_id, job_id, model, provider, prompt_template, variants_requested,
            variants_generated, cost_usd, duration_seconds, status, error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        (1, "classics", "job-1", "flux-pro", "openrouter", "tmpl", 3, 3, 0.4, 1.0, "success", ""),
    )

    repo = repository.SqliteBookRepository(db)
    variants = repo.get_variants(book_number=1, catalog_id="classics")
    assert len(variants) == 1
    assert variants[0]["variant_number"] == 2

    history, total = repo.list_generation_history(
        catalog_id="classics",
        limit=10,
        offset=0,
        filters={"book": 1, "model": "flux"},
    )
    assert total == 1
    assert history[0]["job_id"] == "job-1"


def test_db_execute_with_retry_re_raises_non_lock_errors(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)

    def _boom():  # type: ignore[no-untyped-def]
        raise sqlite3.OperationalError("syntax error")

    with pytest.raises(sqlite3.OperationalError):
        db._execute_with_retry(_boom, retries=3, backoff_seconds=0.0)


def test_db_execute_with_retry_retries_and_exhausts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)
    sleeps: list[float] = []
    monkeypatch.setattr(db_module.time, "sleep", lambda seconds: sleeps.append(float(seconds)))

    def _busy():  # type: ignore[no-untyped-def]
        raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError):
        db._execute_with_retry(_busy, retries=2, backoff_seconds=0.05)
    assert sleeps == [0.05]


def test_db_executemany_falls_back_to_param_count_when_rowcount_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)

    class _Cursor:
        rowcount = None

    class _Conn:
        def executemany(self, _sql, _params):  # type: ignore[no-untyped-def]
            return _Cursor()

    class _ConnectionCtx:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return _Conn()

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

    monkeypatch.setattr(db, "connection", lambda: _ConnectionCtx())
    count = db.executemany("INSERT INTO books VALUES (?)", [(1,), (2,), (3,)])
    assert count == 3


def test_db_close_handles_queue_empty_race(tmp_path: Path):
    db_path = tmp_path / "alexandria.db"
    db = db_module.Database(db_path)
    while not db._pool.empty():
        db._pool.get_nowait().close()

    class _RaceQueue:
        def empty(self) -> bool:
            return False

        def get_nowait(self):  # type: ignore[no-untyped-def]
            raise queue.Empty

    db._pool = _RaceQueue()  # type: ignore[assignment]
    db.close()
