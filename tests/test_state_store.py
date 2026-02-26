from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from src import state_store as state_store_module
from src.state_store import StateStore, _from_json


def test_append_and_list_generation_records(tmp_path: Path):
    store = StateStore(tmp_path / "state.sqlite3")
    rows = [
        {
            "book_number": 1,
            "variant": 1,
            "model": "openrouter/flux-2-pro",
            "prompt": "A stormy sea",
            "provider": "openrouter",
            "success": True,
            "generation_time": 2.5,
            "cost": 0.055,
            "timestamp": "2026-02-22T00:00:00+00:00",
        },
        {
            "book_number": 1,
            "variant": 2,
            "model": "openai/gpt-image-1-high",
            "prompt": "A whale breach",
            "provider": "openai",
            "success": False,
            "error": "timeout",
            "generation_time": 1.2,
            "cost": 0.0,
            "timestamp": "2026-02-22T00:01:00+00:00",
        },
    ]
    inserted = store.append_generation_records(catalog_id="classics", records=rows)
    assert inserted == 2
    listed = store.list_generation_records(catalog_id="classics", limit=10)
    assert len(listed) == 2
    assert listed[0]["variant"] == 1
    assert listed[1]["variant"] == 2
    payload = store.export_history_payload(catalog_id="classics", limit=10)
    assert "items" in payload
    assert len(payload["items"]) == 2


def test_append_generation_records_dedupes_by_job_id(tmp_path: Path):
    store = StateStore(tmp_path / "state.sqlite3")
    rows = [
        {
            "book_number": 1,
            "variant": 1,
            "model": "openrouter/flux-2-pro",
            "prompt": "A stormy sea",
            "provider": "openrouter",
            "success": True,
            "generation_time": 2.5,
            "cost": 0.055,
            "timestamp": "2026-02-22T00:00:00+00:00",
        }
    ]

    first = store.append_generation_records(catalog_id="classics", records=rows, job_id="job-1")
    second = store.append_generation_records(catalog_id="classics", records=rows, job_id="job-1")
    third = store.append_generation_records(catalog_id="classics", records=rows, job_id="job-2")

    assert first == 1
    assert second == 0
    assert third == 1

    listed = store.list_generation_records(catalog_id="classics", limit=10)
    assert len(listed) == 2
    assert sorted([row["job_id"] for row in listed]) == ["job-1", "job-2"]


def test_winner_upsert_load_and_replace(tmp_path: Path):
    store = StateStore(tmp_path / "state.sqlite3")
    count = store.upsert_winner_selections(
        catalog_id="classics",
        selections={
            "1": {"winner": 2, "score": 0.81, "confirmed": True},
            "2": 3,
        },
        replace=True,
    )
    assert count == 2
    winners = store.load_winner_selections(catalog_id="classics")
    assert winners["1"]["winner"] == 2
    assert winners["2"]["winner"] == 3

    store.upsert_winner_selections(
        catalog_id="classics",
        selections={"3": {"winner": 1, "score": 0.7}},
        replace=True,
    )
    replaced = store.load_winner_selections(catalog_id="classics")
    assert sorted(replaced.keys()) == ["3"]


def test_bootstrap_from_json(tmp_path: Path):
    store = StateStore(tmp_path / "state.sqlite3")
    history_path = tmp_path / "generation_history.json"
    winner_path = tmp_path / "winner_selections.json"
    history_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "book_number": 5,
                        "variant": 1,
                        "model": "openrouter/flux-2-pro",
                        "prompt": "Prompt",
                        "provider": "openrouter",
                        "success": True,
                        "timestamp": "2026-02-22T00:00:00+00:00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    winner_path.write_text(
        json.dumps({"selections": {"5": {"winner": 1, "score": 0.9, "confirmed": True}}}),
        encoding="utf-8",
    )
    summary = store.bootstrap_from_json(
        catalog_id="classics",
        history_path=history_path,
        winner_path=winner_path,
    )
    assert summary["history_rows"] == 1
    assert summary["winner_rows"] == 1
    assert store.count_generation_records(catalog_id="classics") == 1
    winners = store.load_winner_selections(catalog_id="classics")
    assert winners["5"]["winner"] == 1


def test_state_store_helpers_and_skip_paths(tmp_path: Path):
    assert _from_json(None, {"x": 1}) == {"x": 1}
    assert _from_json("{bad-json", {"x": 2}) == {"x": 2}

    store = StateStore(tmp_path / "state.sqlite3")
    inserted = store.append_generation_records(
        catalog_id="classics",
        records=[
            "not-a-dict",
            {"book_number": 0, "variant": 1, "model": "m", "prompt": "p", "provider": "x"},
            {"book_number": 1, "variant": 0, "model": "m", "prompt": "p", "provider": "x"},
            {"book_number": 1, "variant": 1, "model": "m", "prompt": "p", "provider": "x"},
        ],
    )
    assert inserted == 1


def test_winner_selection_normalization_and_load_filters(tmp_path: Path):
    store = StateStore(tmp_path / "state.sqlite3")
    count = store.upsert_winner_selections(
        catalog_id="classics",
        selections={
            "x": {"winner": 1},  # non-int key -> ignored
            "-1": {"winner": 1},  # <=0 -> ignored
            "2": "bad",  # invalid int value -> ignored
            "3": "7",  # valid scalar -> normalized
            "4": {"winner": 2, "score": 0.9},  # dict path
        },
        replace=True,
    )
    assert count == 2
    winners = store.load_winner_selections(catalog_id="classics")
    assert winners["3"]["winner"] == 7
    assert winners["4"]["winner"] == 2

    # Directly insert invalid DB row to hit load filter path.
    with store._managed_connection() as conn:
        conn.execute(
            "INSERT INTO winner_selections(catalog_id, book_number, payload_json, updated_at) VALUES (?, ?, ?, ?)",
            ("classics", 0, "{}", "2026-02-22T00:00:00+00:00"),
        )
    loaded = store.load_winner_selections(catalog_id="classics")
    assert "0" not in loaded


def test_bootstrap_invalid_json_and_payload_shapes(tmp_path: Path):
    store = StateStore(tmp_path / "state.sqlite3")
    history_path = tmp_path / "history.json"
    winner_path = tmp_path / "winners.json"

    # Invalid JSON paths should not crash and should import nothing.
    history_path.write_text("{bad-json", encoding="utf-8")
    winner_path.write_text("{bad-json", encoding="utf-8")
    summary = store.bootstrap_from_json(catalog_id="classics", history_path=history_path, winner_path=winner_path)
    assert summary == {"history_rows": 0, "winner_rows": 0}

    # Non-dict winner payload should map to empty selections.
    history_path.write_text(json.dumps({"items": []}), encoding="utf-8")
    winner_path.write_text(json.dumps(["not-a-dict"]), encoding="utf-8")
    summary2 = store.bootstrap_from_json(catalog_id="classics", history_path=history_path, winner_path=winner_path)
    assert summary2 == {"history_rows": 0, "winner_rows": 0}

    # Dict winner payload without "selections" should be used as selections source.
    winner_path.write_text(json.dumps({"5": {"winner": 1}}), encoding="utf-8")
    summary3 = store.bootstrap_from_json(catalog_id="classics", history_path=history_path, winner_path=winner_path)
    assert summary3["winner_rows"] == 1


def test_state_store_migrates_legacy_generation_records_table(tmp_path: Path):
    db_path = tmp_path / "legacy.sqlite3"

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE generation_records (
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
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE winner_selections (
            catalog_id TEXT NOT NULL,
            book_number INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY(catalog_id, book_number)
        )
        """
    )
    conn.commit()
    conn.close()

    store = StateStore(db_path)
    inserted = store.append_generation_records(
        catalog_id="classics",
        records=[
            {
                "book_number": 1,
                "variant": 1,
                "model": "openrouter/flux-2-pro",
                "prompt": "Prompt",
                "provider": "openrouter",
            }
        ],
        job_id="legacy-job",
    )
    assert inserted == 1
    listed = store.list_generation_records(catalog_id="classics", limit=10)
    assert listed[0]["job_id"] == "legacy-job"


def test_state_store_write_transaction_retries_locked_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    store = StateStore(
        tmp_path / "state.sqlite3",
        write_retry_attempts=3,
        write_retry_base_delay_seconds=0.001,
    )
    calls = {"count": 0}
    sleeps: list[float] = []
    monkeypatch.setattr(state_store_module.time, "sleep", lambda delay: sleeps.append(float(delay)))

    def _callback(_conn):  # type: ignore[no-untyped-def]
        calls["count"] += 1
        if calls["count"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return 7

    result = store._run_write_transaction("retry-test", _callback)
    assert result == 7
    assert calls["count"] == 2
    assert len(sleeps) == 1


def test_state_store_write_transaction_does_not_retry_non_lock_error(tmp_path: Path):
    store = StateStore(
        tmp_path / "state.sqlite3",
        write_retry_attempts=4,
        write_retry_base_delay_seconds=0.0,
    )
    calls = {"count": 0}

    def _callback(_conn):  # type: ignore[no-untyped-def]
        calls["count"] += 1
        raise sqlite3.OperationalError("syntax error")

    with pytest.raises(sqlite3.OperationalError, match="syntax error"):
        store._run_write_transaction("non-retry-test", _callback)
    assert calls["count"] == 1
