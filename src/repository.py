"""Unified repository abstraction for JSON and SQLite-backed data access."""

from __future__ import annotations

import json
import math
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

try:
    from src import book_metadata
    from src import config
    from src import db as db_module
    from src import safe_json
except ModuleNotFoundError:  # pragma: no cover
    import book_metadata  # type: ignore
    import config  # type: ignore
    import db as db_module  # type: ignore
    import safe_json  # type: ignore


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(parsed):
        return float(default)
    return parsed


def _winner_path_for_runtime(runtime: config.Config) -> Path:
    return config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)


def _quality_scores_path(runtime: config.Config) -> Path:
    return config.quality_scores_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)


def _generation_history_path(runtime: config.Config) -> Path:
    return config.generation_history_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir)


class BookRepository(ABC):
    @abstractmethod
    def list_books(
        self,
        *,
        catalog_id: str,
        limit: int,
        offset: int,
        filters: dict[str, Any] | None = None,
        sort: str = "book_number",
        order: str = "asc",
    ) -> tuple[list[dict[str, Any]], int]:
        raise NotImplementedError

    @abstractmethod
    def get_book(self, *, book_number: int, catalog_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    @abstractmethod
    def update_book(self, *, book_number: int, catalog_id: str, data: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_variants(self, *, book_number: int, catalog_id: str) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def list_generation_history(
        self,
        *,
        catalog_id: str,
        limit: int,
        offset: int,
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        raise NotImplementedError


class JsonBookRepository(BookRepository):
    """Compatibility repository backed by existing JSON files."""

    def __init__(self, runtime: config.Config):
        self.runtime = runtime

    def _catalog_rows(self) -> list[dict[str, Any]]:
        payload = safe_json.load_json(self.runtime.book_catalog_path, [])
        return payload if isinstance(payload, list) else []

    def _winner_map(self) -> dict[int, int]:
        path = _winner_path_for_runtime(self.runtime)
        payload = safe_json.load_json(path, {})
        selections = payload.get("selections", payload) if isinstance(payload, dict) else {}
        out: dict[int, int] = {}
        if isinstance(selections, dict):
            for raw_book, raw_value in selections.items():
                book = _safe_int(raw_book, 0)
                if book <= 0:
                    continue
                if isinstance(raw_value, dict):
                    variant = _safe_int(raw_value.get("winner"), 0)
                else:
                    variant = _safe_int(raw_value, 0)
                if variant > 0:
                    out[book] = variant
        return out

    def _quality_lookup(self) -> dict[tuple[int, int], float]:
        path = _quality_scores_path(self.runtime)
        payload = safe_json.load_json(path, {})
        rows = payload.get("scores", []) if isinstance(payload, dict) else []
        out: dict[tuple[int, int], float] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            book = _safe_int(row.get("book_number"), 0)
            variant = _safe_int(row.get("variant_id"), 0)
            if book <= 0 or variant <= 0:
                continue
            score = _safe_float(row.get("overall_score"), 0.0)
            out[(book, variant)] = max(out.get((book, variant), 0.0), score)
        return out

    def _apply_filters(self, rows: list[dict[str, Any]], filters: dict[str, Any] | None) -> list[dict[str, Any]]:
        if not filters:
            return rows
        out = rows
        search = str(filters.get("search", "") or "").strip().lower()
        if search:
            out = [
                row
                for row in out
                if search in str(row.get("title", "")).lower()
                or search in str(row.get("author", "")).lower()
            ]
        status = str(filters.get("status", "") or "").strip().lower()
        if status:
            out = [row for row in out if str(row.get("status", "")).lower() == status]
        quality_min = filters.get("quality_min")
        quality_max = filters.get("quality_max")
        if quality_min is not None:
            out = [row for row in out if _safe_float(row.get("quality_score"), 0.0) >= _safe_float(quality_min, 0.0)]
        if quality_max is not None:
            out = [row for row in out if _safe_float(row.get("quality_score"), 0.0) <= _safe_float(quality_max, 1.0e9)]
        tags_raw = str(filters.get("tags", "") or "").strip()
        if tags_raw:
            wanted = {token.strip().lower() for token in tags_raw.split(",") if token.strip()}
            if wanted:
                out = [
                    row
                    for row in out
                    if wanted.issubset({str(tag).strip().lower() for tag in row.get("tags", []) if str(tag).strip()})
                ]
        return out

    @staticmethod
    def _apply_sort(rows: list[dict[str, Any]], *, sort: str, order: str) -> list[dict[str, Any]]:
        key = str(sort or "book_number").strip().lower()
        reverse = str(order or "asc").strip().lower() == "desc"
        valid = {"book_number", "title", "author", "quality_score", "status", "updated_at"}
        if key not in valid:
            key = "book_number"
        return sorted(rows, key=lambda row: row.get(key) if row.get(key) is not None else "", reverse=reverse)

    def list_books(
        self,
        *,
        catalog_id: str,
        limit: int,
        offset: int,
        filters: dict[str, Any] | None = None,
        sort: str = "book_number",
        order: str = "asc",
    ) -> tuple[list[dict[str, Any]], int]:
        metadata = book_metadata.list_books(book_metadata.metadata_path(data_dir=self.runtime.data_dir, catalog_id=catalog_id))
        winners = self._winner_map()
        quality = self._quality_lookup()
        rows: list[dict[str, Any]] = []
        for raw in self._catalog_rows():
            if not isinstance(raw, dict):
                continue
            number = _safe_int(raw.get("number"), 0)
            if number <= 0:
                continue
            variant = winners.get(number, 0)
            score = quality.get((number, variant), 0.0) if variant > 0 else 0.0
            meta = metadata.get(str(number), {})
            rows.append(
                {
                    "book_number": number,
                    "catalog_id": catalog_id,
                    "title": str(raw.get("title", "")),
                    "author": str(raw.get("author", "")),
                    "genre": str(raw.get("genre", "")),
                    "source_path": str(raw.get("folder_name", "")),
                    "status": "processed" if variant > 0 else "pending",
                    "quality_score": score,
                    "winner_variant": variant,
                    "tags": list(meta.get("tags", [])) if isinstance(meta, dict) else [],
                    "notes": str(meta.get("notes", "")) if isinstance(meta, dict) else "",
                    "created_at": "",
                    "updated_at": "",
                }
            )

        rows = self._apply_filters(rows, filters)
        rows = self._apply_sort(rows, sort=sort, order=order)
        total = len(rows)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return rows[start:end], total

    def get_book(self, *, book_number: int, catalog_id: str) -> dict[str, Any] | None:
        rows, _ = self.list_books(catalog_id=catalog_id, limit=1_000_000, offset=0)
        for row in rows:
            if int(row.get("book_number", 0)) == int(book_number):
                return row
        return None

    def update_book(self, *, book_number: int, catalog_id: str, data: dict[str, Any]) -> dict[str, Any]:
        current = self.get_book(book_number=book_number, catalog_id=catalog_id) or {
            "book_number": int(book_number),
            "catalog_id": catalog_id,
            "tags": [],
            "notes": "",
        }
        tags = data.get("tags", current.get("tags", []))
        notes = data.get("notes", current.get("notes", ""))
        meta_path = book_metadata.metadata_path(data_dir=self.runtime.data_dir, catalog_id=catalog_id)
        saved = book_metadata.set_book(meta_path, int(book_number), tags=[str(t) for t in tags if str(t).strip()], notes=str(notes))
        updated = dict(current)
        updated["tags"] = saved.get("tags", [])
        updated["notes"] = saved.get("notes", "")
        return updated

    def get_variants(self, *, book_number: int, catalog_id: str) -> list[dict[str, Any]]:
        folder_name = ""
        title = ""
        for row in self._catalog_rows():
            if not isinstance(row, dict):
                continue
            if _safe_int(row.get("number"), 0) == int(book_number):
                folder_name = str(row.get("folder_name", ""))
                title = str(row.get("title", ""))
                break
        if not folder_name:
            return []
        book_dir = self.runtime.output_dir / folder_name
        quality = self._quality_lookup()
        out: list[dict[str, Any]] = []
        for variant_dir in sorted(book_dir.glob("Variant-*")):
            if not variant_dir.is_dir():
                continue
            variant = _safe_int(variant_dir.name.split("-", 1)[1] if "-" in variant_dir.name else 0, 0)
            image_path = ""
            for jpg in sorted(variant_dir.glob("*.jpg")):
                image_path = str(jpg)
                break
            out.append(
                {
                    "book_number": int(book_number),
                    "catalog_id": catalog_id,
                    "variant_number": int(variant),
                    "title": title,
                    "output_path": image_path,
                    "quality_score": quality.get((int(book_number), int(variant)), 0.0),
                }
            )
        out.sort(key=lambda row: int(row.get("variant_number", 0)))
        return out

    def list_generation_history(
        self,
        *,
        catalog_id: str,
        limit: int,
        offset: int,
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        path = _generation_history_path(self.runtime)
        payload = safe_json.load_json(path, {})
        rows = payload.get("items", []) if isinstance(payload, dict) else []
        items = [row for row in rows if isinstance(row, dict)]
        if filters:
            if filters.get("book"):
                wanted_book = _safe_int(filters.get("book"), 0)
                if wanted_book > 0:
                    items = [row for row in items if _safe_int(row.get("book_number"), 0) == wanted_book]
            if filters.get("model"):
                token = str(filters.get("model", "")).strip().lower()
                items = [row for row in items if token in str(row.get("model", "")).lower()]
        total = len(items)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return items[start:end], total


class SqliteBookRepository(BookRepository):
    """SQLite implementation used when USE_SQLITE is enabled."""

    def __init__(self, db: db_module.Database):
        self.db = db

    def __del__(self):  # pragma: no cover - defensive finalizer
        try:
            self.db.close()
        except Exception:
            pass

    def _where_clause(self, *, catalog_id: str, filters: dict[str, Any] | None = None) -> tuple[str, list[Any]]:
        where = ["catalog_id = ?"]
        params: list[Any] = [catalog_id]
        if filters:
            search = str(filters.get("search", "") or "").strip()
            if search:
                where.append("rowid IN (SELECT rowid FROM books_fts WHERE books_fts MATCH ?)")
                params.append(search.replace('"', ""))
            status = str(filters.get("status", "") or "").strip()
            if status:
                where.append("status = ?")
                params.append(status)
            if filters.get("quality_min") is not None:
                where.append("COALESCE(quality_score, 0) >= ?")
                params.append(_safe_float(filters.get("quality_min"), 0.0))
            if filters.get("quality_max") is not None:
                where.append("COALESCE(quality_score, 0) <= ?")
                params.append(_safe_float(filters.get("quality_max"), 1.0e9))
            tags = str(filters.get("tags", "") or "").strip()
            if tags:
                for token in [t.strip().lower() for t in tags.split(",") if t.strip()]:
                    where.append("LOWER(COALESCE(tags, '')) LIKE ?")
                    params.append(f"%{token}%")
        return " AND ".join(where), params

    def list_books(
        self,
        *,
        catalog_id: str,
        limit: int,
        offset: int,
        filters: dict[str, Any] | None = None,
        sort: str = "book_number",
        order: str = "asc",
    ) -> tuple[list[dict[str, Any]], int]:
        sort_map = {
            "book_number": "book_number",
            "number": "book_number",
            "title": "title",
            "author": "author",
            "quality_score": "quality_score",
            "status": "status",
            "updated_at": "updated_at",
        }
        sort_column = sort_map.get(str(sort or "book_number").lower(), "book_number")
        order_sql = "DESC" if str(order or "asc").lower() == "desc" else "ASC"

        where_sql, params = self._where_clause(catalog_id=catalog_id, filters=filters)
        total_row = self.db.query(f"SELECT COUNT(*) AS c FROM books WHERE {where_sql}", tuple(params))
        total = int(total_row[0]["c"] if total_row else 0)

        params_with_limit = list(params) + [max(1, int(limit)), max(0, int(offset))]
        rows = self.db.query(
            f"""
            SELECT
                book_number, catalog_id, title, author, genre, source_path, status,
                quality_score, winner_variant, tags, notes, created_at, updated_at
            FROM books
            WHERE {where_sql}
            ORDER BY {sort_column} {order_sql}
            LIMIT ? OFFSET ?
            """,
            tuple(params_with_limit),
        )
        for row in rows:
            if isinstance(row.get("tags"), str):
                try:
                    row["tags"] = json.loads(row["tags"])
                except Exception:
                    row["tags"] = []
            if not isinstance(row.get("tags"), list):
                row["tags"] = []
        return rows, total

    def get_book(self, *, book_number: int, catalog_id: str) -> dict[str, Any] | None:
        rows = self.db.query(
            """
            SELECT
                book_number, catalog_id, title, author, genre, source_path, status,
                quality_score, winner_variant, tags, notes, created_at, updated_at
            FROM books
            WHERE catalog_id = ? AND book_number = ?
            LIMIT 1
            """,
            (catalog_id, int(book_number)),
        )
        if not rows:
            return None
        row = rows[0]
        if isinstance(row.get("tags"), str):
            try:
                row["tags"] = json.loads(row["tags"])
            except Exception:
                row["tags"] = []
        return row

    def update_book(self, *, book_number: int, catalog_id: str, data: dict[str, Any]) -> dict[str, Any]:
        current = self.get_book(book_number=book_number, catalog_id=catalog_id)
        if current is None:
            raise KeyError(f"Book {book_number} not found for catalog {catalog_id}")
        merged = dict(current)
        for key in ("title", "author", "genre", "source_path", "status", "quality_score", "winner_variant", "notes"):
            if key in data:
                merged[key] = data[key]
        tags = data.get("tags", merged.get("tags", []))
        if not isinstance(tags, list):
            tags = []
        merged["tags"] = [str(item) for item in tags if str(item).strip()]
        self.db.execute(
            """
            UPDATE books
            SET title = ?, author = ?, genre = ?, source_path = ?, status = ?,
                quality_score = ?, winner_variant = ?, tags = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE catalog_id = ? AND book_number = ?
            """,
            (
                str(merged.get("title", "")),
                str(merged.get("author", "")),
                str(merged.get("genre", "")),
                str(merged.get("source_path", "")),
                str(merged.get("status", "pending")),
                _safe_float(merged.get("quality_score"), 0.0),
                _safe_int(merged.get("winner_variant"), 0),
                json.dumps(merged.get("tags", []), ensure_ascii=False),
                str(merged.get("notes", "")),
                catalog_id,
                int(book_number),
            ),
        )
        updated = self.get_book(book_number=book_number, catalog_id=catalog_id)
        if updated is None:
            raise RuntimeError("Book update failed")
        return updated

    def get_variants(self, *, book_number: int, catalog_id: str) -> list[dict[str, Any]]:
        return self.db.query(
            """
            SELECT
                id, book_number, variant_number, catalog_id, model, provider, prompt,
                quality_score, quality_breakdown, is_winner, output_path, thumbnail_path,
                generated_at, generation_duration_seconds
            FROM variants
            WHERE catalog_id = ? AND book_number = ?
            ORDER BY variant_number ASC
            """,
            (catalog_id, int(book_number)),
        )

    def list_generation_history(
        self,
        *,
        catalog_id: str,
        limit: int,
        offset: int,
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        where = ["catalog_id = ?"]
        params: list[Any] = [catalog_id]
        if filters:
            if filters.get("book"):
                where.append("book_number = ?")
                params.append(_safe_int(filters.get("book"), 0))
            if filters.get("model"):
                where.append("LOWER(COALESCE(model,'')) LIKE ?")
                params.append(f"%{str(filters.get('model')).strip().lower()}%")
            if filters.get("provider"):
                where.append("LOWER(COALESCE(provider,'')) = ?")
                params.append(str(filters.get("provider")).strip().lower())
            if filters.get("status"):
                where.append("LOWER(COALESCE(status,'')) = ?")
                params.append(str(filters.get("status")).strip().lower())
        where_sql = " AND ".join(where)
        total_row = self.db.query(f"SELECT COUNT(*) AS c FROM generations WHERE {where_sql}", tuple(params))
        total = int(total_row[0]["c"] if total_row else 0)
        params2 = list(params) + [max(1, int(limit)), max(0, int(offset))]
        rows = self.db.query(
            f"""
            SELECT
                id, book_number, catalog_id, job_id, model, provider, prompt_template,
                variants_requested, variants_generated, cost_usd, duration_seconds, status, error, created_at
            FROM generations
            WHERE {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            tuple(params2),
        )
        return rows, total


def get_repository(
    *,
    runtime: config.Config | None = None,
    use_sqlite: bool | None = None,
    db_path: str | Path | None = None,
) -> BookRepository:
    runtime = runtime or config.get_config()
    use_sqlite_resolved = runtime.use_sqlite if use_sqlite is None else bool(use_sqlite)
    if use_sqlite_resolved:
        database = db_module.Database(db_path or runtime.sqlite_db_path)
        return SqliteBookRepository(database)
    return JsonBookRepository(runtime)
