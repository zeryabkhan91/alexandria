#!/usr/bin/env python3
"""Migrate JSON-based runtime data into the unified SQLite schema."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
import sys

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src import book_metadata  # noqa: E402
from src import config  # noqa: E402
from src import database  # noqa: E402
from src import db as db_module  # noqa: E402


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _winner_map(path: Path) -> dict[int, int]:
    payload = _load_json(path, {})
    selections = payload.get("selections", payload) if isinstance(payload, dict) else {}
    out: dict[int, int] = {}
    if not isinstance(selections, dict):
        return out
    for key, value in selections.items():
        book = _safe_int(key, 0)
        if book <= 0:
            continue
        if isinstance(value, dict):
            variant = _safe_int(value.get("winner"), 0)
        else:
            variant = _safe_int(value, 0)
        if variant > 0:
            out[book] = variant
    return out


def migrate_to_sqlite(*, catalog_id: str, db_path: Path, runtime: config.Config | None = None) -> dict[str, Any]:
    runtime = runtime or config.get_config(catalog_id)
    database.initialize_database(db_path)
    db = db_module.Database(db_path)

    catalog_rows = _load_json(runtime.book_catalog_path, [])
    if not isinstance(catalog_rows, list):
        catalog_rows = []

    metadata_path = book_metadata.metadata_path(data_dir=runtime.data_dir, catalog_id=runtime.catalog_id)
    metadata_rows = book_metadata.list_books(metadata_path)
    winners = _winner_map(config.winner_selections_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir))

    quality_payload = _load_json(
        config.quality_scores_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir),
        {"scores": []},
    )
    quality_rows = quality_payload.get("scores", []) if isinstance(quality_payload, dict) else []
    history_payload = _load_json(
        config.generation_history_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir),
        {"items": []},
    )
    history_rows = history_payload.get("items", []) if isinstance(history_payload, dict) else []
    cost_payload = _load_json(config.cost_ledger_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir), {"entries": []})
    cost_rows = cost_payload.get("entries", []) if isinstance(cost_payload, dict) else []
    audit_payload = _load_json(config.audit_log_path(catalog_id=runtime.catalog_id, data_dir=runtime.data_dir), {"items": []})
    audit_rows = audit_payload.get("items", []) if isinstance(audit_payload, dict) else []

    quality_lookup: dict[tuple[int, int], float] = {}
    for row in quality_rows if isinstance(quality_rows, list) else []:
        if not isinstance(row, dict):
            continue
        book = _safe_int(row.get("book_number"), 0)
        variant = _safe_int(row.get("variant_id"), 0)
        if book <= 0 or variant <= 0:
            continue
        quality_lookup[(book, variant)] = max(quality_lookup.get((book, variant), 0.0), _safe_float(row.get("overall_score"), 0.0))

    inserted = {
        "books": 0,
        "variants": 0,
        "generations": 0,
        "jobs": 0,
        "costs": 0,
        "audit_log": 0,
    }

    try:
        with db.transaction() as conn:
            for table in ("variants", "generations", "jobs", "costs", "audit_log", "books"):
                conn.execute(f"DELETE FROM {table}")

            for row in catalog_rows:
                if not isinstance(row, dict):
                    continue
                book = _safe_int(row.get("number"), 0)
                if book <= 0:
                    continue
                winner_variant = winners.get(book, 0)
                quality_score = quality_lookup.get((book, winner_variant), 0.0) if winner_variant > 0 else 0.0
                meta = metadata_rows.get(str(book), {})
                tags = meta.get("tags", []) if isinstance(meta, dict) else []
                notes = meta.get("notes", "") if isinstance(meta, dict) else ""
                conn.execute(
                    """
                    INSERT INTO books (
                      book_number, catalog_id, title, author, genre, source_path, status, quality_score, winner_variant, tags, notes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (
                        book,
                        runtime.catalog_id,
                        str(row.get("title", "")),
                        str(row.get("author", "")),
                        str(row.get("genre", "")),
                        str(row.get("folder_name", "")),
                        "processed" if winner_variant > 0 else "pending",
                        quality_score,
                        winner_variant,
                        json.dumps(tags if isinstance(tags, list) else []),
                        str(notes),
                    ),
                )
                inserted["books"] += 1

            for row in quality_rows if isinstance(quality_rows, list) else []:
                if not isinstance(row, dict):
                    continue
                book = _safe_int(row.get("book_number"), 0)
                variant = _safe_int(row.get("variant_id"), 0)
                if book <= 0 or variant <= 0:
                    continue
                model = str(row.get("model") or row.get("model_name") or "unknown")
                provider = str(row.get("provider") or "")
                breakdown = {
                    "technical": _safe_float(row.get("technical_score"), 0.0),
                    "composition": _safe_float(row.get("diversity_score"), 0.0),
                    "color": _safe_float(row.get("color_score"), 0.0),
                    "style": _safe_float(row.get("distinctiveness_score"), 0.0),
                    "artifact": _safe_float(row.get("artifact_score"), 0.0),
                }
                conn.execute(
                    """
                    INSERT OR REPLACE INTO variants (
                      book_number, variant_number, catalog_id, model, provider, prompt, quality_score, quality_breakdown,
                      is_winner, output_path, thumbnail_path, generated_at, generation_duration_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        book,
                        variant,
                        runtime.catalog_id,
                        model,
                        provider,
                        str(row.get("prompt", "")),
                        _safe_float(row.get("overall_score"), 0.0),
                        json.dumps(breakdown, ensure_ascii=False),
                        1 if winners.get(book, 0) == variant else 0,
                        str(row.get("composited_path") or row.get("image_path") or ""),
                        "",
                        str(row.get("timestamp", "")),
                        _safe_float(row.get("generation_time"), 0.0),
                    ),
                )
                inserted["variants"] += 1

            for idx, row in enumerate(history_rows if isinstance(history_rows, list) else []):
                if not isinstance(row, dict):
                    continue
                conn.execute(
                    """
                    INSERT INTO generations (
                      book_number, catalog_id, job_id, model, provider, prompt_template, variants_requested, variants_generated,
                      cost_usd, duration_seconds, status, error, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        _safe_int(row.get("book_number"), 0),
                        runtime.catalog_id,
                        str(row.get("job_id") or f"history-{idx+1}"),
                        str(row.get("model", "")),
                        str(row.get("provider", "")),
                        str(row.get("prompt", "")),
                        _safe_int(row.get("variants_requested"), 1),
                        _safe_int(row.get("variants_generated"), 1),
                        _safe_float(row.get("cost"), 0.0),
                        _safe_float(row.get("generation_time"), 0.0),
                        str(row.get("status", "success")),
                        str(row.get("error", "")),
                        str(row.get("timestamp", datetime.now(timezone.utc).isoformat())),
                    ),
                )
                inserted["generations"] += 1

            # Seed jobs from existing queue DB if available.
            jobs_db_path = runtime.data_dir / "jobs.sqlite3"
            if jobs_db_path.exists():
                import sqlite3

                source = sqlite3.connect(str(jobs_db_path))
                source.row_factory = sqlite3.Row
                try:
                    rows = source.execute("SELECT * FROM jobs").fetchall()
                except Exception:
                    rows = []
                for row in rows:
                    payload = row["payload_json"] if "payload_json" in row.keys() else "{}"
                    result = row["result_json"] if "result_json" in row.keys() else "{}"
                    error = row["error_json"] if "error_json" in row.keys() else "{}"
                    conn.execute(
                        """
                        INSERT INTO jobs (
                          job_id, catalog_id, job_type, priority, status, progress, books_total, books_completed, books_failed,
                          params, result, error, created_at, started_at, completed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(row["id"]),
                            str(row["catalog_id"]),
                            str(row["job_type"]),
                            _safe_int(row["priority"], 3),
                            str(row["status"]),
                            1.0 if str(row["status"]) == "completed" else (0.0 if str(row["status"]) == "queued" else 0.5),
                            1,
                            1 if str(row["status"]) == "completed" else 0,
                            1 if str(row["status"]) == "failed" else 0,
                            str(payload or "{}"),
                            str(result or "{}"),
                            str(error or "{}"),
                            str(row["created_at"] or datetime.now(timezone.utc).isoformat()),
                            str(row["started_at"] or ""),
                            str(row["finished_at"] or ""),
                        ),
                    )
                    inserted["jobs"] += 1
                source.close()

            for row in cost_rows if isinstance(cost_rows, list) else []:
                if not isinstance(row, dict):
                    continue
                conn.execute(
                    """
                    INSERT INTO costs (
                      catalog_id, book_number, job_id, model, provider, operation, tokens_in, tokens_out, images_generated,
                      cost_usd, duration_seconds, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row.get("catalog", runtime.catalog_id)),
                        _safe_int(row.get("book_number"), 0),
                        str(row.get("job_id", "")),
                        str(row.get("model", "")),
                        str(row.get("provider", "")),
                        str(row.get("operation", "generate")),
                        _safe_int(row.get("tokens_in"), 0),
                        _safe_int(row.get("tokens_out"), 0),
                        _safe_int(row.get("images_generated"), 0),
                        _safe_float(row.get("cost_usd"), 0.0),
                        _safe_float(row.get("duration_seconds"), 0.0),
                        str(row.get("timestamp", datetime.now(timezone.utc).isoformat())),
                    ),
                )
                inserted["costs"] += 1

            for row in audit_rows if isinstance(audit_rows, list) else []:
                if not isinstance(row, dict):
                    continue
                conn.execute(
                    """
                    INSERT INTO audit_log (catalog_id, action, book_number, details, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        str(row.get("catalog_id", runtime.catalog_id)),
                        str(row.get("action", "unknown")),
                        _safe_int(row.get("book_number"), 0),
                        json.dumps(row.get("details", {}), ensure_ascii=False),
                        str(row.get("timestamp", datetime.now(timezone.utc).isoformat())),
                    ),
                )
                inserted["audit_log"] += 1
    finally:
        db.close()

    counts = database.table_counts(db_path)
    return {
        "ok": True,
        "catalog_id": runtime.catalog_id,
        "db_path": str(db_path),
        "inserted": inserted,
        "counts": counts,
        "migrated_at": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate Alexandria JSON files to SQLite")
    parser.add_argument("--catalog", type=str, default=config.DEFAULT_CATALOG_ID)
    parser.add_argument("--db-path", type=Path, default=config.SQLITE_DB_PATH)
    args = parser.parse_args()

    runtime = config.get_config(args.catalog)
    summary = migrate_to_sqlite(catalog_id=runtime.catalog_id, db_path=args.db_path, runtime=runtime)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
