from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src import cost_tracker


def _entry(*, catalog: str = "classics", book: int = 1, model: str = "m1", provider: str = "p1", cost: float = 0.1, op: str = "generate") -> dict:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "catalog": catalog,
        "book_number": book,
        "job_id": "job-1",
        "model": model,
        "provider": provider,
        "operation": op,
        "tokens_in": 10,
        "tokens_out": 5,
        "images_generated": 1,
        "cost_usd": cost,
        "duration_seconds": 2.5,
    }


def test_record_and_load_ledger(tmp_path: Path):
    ledger = tmp_path / "cost_ledger.json"
    row = cost_tracker.record_entry(ledger, entry=_entry(cost=0.42))
    payload = cost_tracker.load_ledger(ledger)
    assert row["cost_usd"] == 0.42
    assert len(payload["entries"]) == 1
    assert payload["entries"][0]["book_number"] == 1


def test_record_entries_and_groupings(tmp_path: Path):
    ledger = tmp_path / "cost_ledger.json"
    cost_tracker.record_entries(
        ledger,
        entries=[
            _entry(book=1, model="a", provider="openai", cost=0.1, op="generate"),
            _entry(book=1, model="a", provider="openai", cost=0.2, op="generate"),
            _entry(book=2, model="b", provider="openrouter", cost=0.3, op="enrich"),
        ],
    )
    rows = cost_tracker.list_entries(ledger, catalog_id="classics", period="all")
    assert len(rows) == 3
    by_book = cost_tracker.by_book(rows)
    assert by_book[0]["book_number"] in {1, 2}
    by_model = cost_tracker.by_model(rows)
    assert {row["model"] for row in by_model} == {"a", "b"}
    by_operation = cost_tracker.by_operation(rows)
    assert {row["operation"] for row in by_operation} == {"generate", "enrich"}


def test_list_entries_filters_by_catalog_and_period(tmp_path: Path):
    ledger = tmp_path / "cost_ledger.json"
    old = _entry(catalog="classics", cost=0.2)
    old["timestamp"] = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    recent = _entry(catalog="classics", cost=0.5)
    other = _entry(catalog="sci-fi", cost=0.8)
    cost_tracker.record_entries(ledger, entries=[old, recent, other])
    rows_7d = cost_tracker.list_entries(ledger, catalog_id="classics", period="7d")
    assert len(rows_7d) == 1
    assert rows_7d[0]["cost_usd"] == 0.5


def test_summary_and_timeline(tmp_path: Path):
    ledger = tmp_path / "cost_ledger.json"
    row_a = _entry(cost=0.5)
    row_b = _entry(cost=1.25)
    cost_tracker.record_entries(ledger, entries=[row_a, row_b])
    rows = cost_tracker.list_entries(ledger, catalog_id="classics", period="all")
    summary = cost_tracker.summarize(rows)
    assert summary["entries"] == 2
    assert summary["total_cost_usd"] == 1.75
    timeline = cost_tracker.timeline(rows, granularity="daily")
    assert timeline
    assert timeline[-1]["cumulative_cost_usd"] >= 1.75


def test_budget_set_and_status_warning_and_blocked(tmp_path: Path):
    budget_path = tmp_path / "budget.json"
    payload = cost_tracker.set_budget(path=budget_path, catalog_id="classics", limit_usd=10.0, warning_threshold=0.8, hard_stop=True)
    status_ok = cost_tracker.budget_status(spent_usd=5.0, catalog_id="classics", budget_payload=payload)
    status_warn = cost_tracker.budget_status(spent_usd=8.5, catalog_id="classics", budget_payload=payload)
    status_block = cost_tracker.budget_status(spent_usd=10.1, catalog_id="classics", budget_payload=payload)
    assert status_ok["state"] == "ok"
    assert status_warn["state"] == "warning"
    assert status_block["state"] == "blocked"


def test_budget_override_expands_effective_limit(tmp_path: Path):
    budget_path = tmp_path / "budget.json"
    cost_tracker.set_budget(path=budget_path, catalog_id="classics", limit_usd=10.0, warning_threshold=0.8, hard_stop=True)
    payload = cost_tracker.set_override(path=budget_path, catalog_id="classics", extra_limit_usd=5.0, duration_hours=12, reason="one-off run")
    status = cost_tracker.budget_status(spent_usd=12.0, catalog_id="classics", budget_payload=payload)
    assert status["effective_limit_usd"] == 15.0
    assert status["override"]["active"] is True
    assert status["state"] == "warning"


def test_cost_tracker_helper_and_malformed_payload_paths(tmp_path: Path, monkeypatch):
    assert cost_tracker._safe_float(None, 1.2) == 1.2
    assert cost_tracker._safe_float("bad", 1.5) == 1.5
    assert cost_tracker._safe_int("bad", 7) == 7

    assert cost_tracker._as_datetime("not-date") is None
    naive = cost_tracker._as_datetime("2026-02-22T00:00:00")
    assert naive is not None and naive.tzinfo is not None

    ledger = tmp_path / "cost_ledger.json"
    monkeypatch.setattr(cost_tracker.safe_json, "load_json", lambda *_args, **_kwargs: "bad")
    payload = cost_tracker.load_ledger(ledger)
    assert payload["entries"] == []

    monkeypatch.setattr(cost_tracker.safe_json, "load_json", lambda *_args, **_kwargs: {"updated_at": "", "entries": "bad"})
    payload2 = cost_tracker.load_ledger(ledger)
    assert payload2["entries"] == []

    # list_entries rows-not-list and non-dict rows.
    monkeypatch.setattr(cost_tracker, "load_ledger", lambda _path: {"entries": "bad"})
    assert cost_tracker.list_entries(ledger, catalog_id="classics", period="all") == []
    monkeypatch.setattr(cost_tracker, "load_ledger", lambda _path: {"entries": ["bad", _entry(catalog="classics")]})
    rows = cost_tracker.list_entries(ledger, catalog_id="classics", period="all")
    assert len(rows) == 1

    # record_entries skips non-dict rows.
    monkeypatch.setattr(cost_tracker, "load_ledger", lambda _path: {"updated_at": "", "entries": []})
    captured = {}
    monkeypatch.setattr(cost_tracker.safe_json, "atomic_write_json", lambda _path, payload: captured.update(payload))
    count = cost_tracker.record_entries(ledger, entries=["bad", _entry()])
    assert count == 1
    assert len(captured["entries"]) == 1

    assert cost_tracker._period_start(None) is None
    assert cost_tracker._period_start("12h") is not None
    assert cost_tracker._period_start("unknown") is None

    # by_book skips invalid book numbers.
    grouped = cost_tracker.by_book([{"book_number": 0, "cost_usd": 1.0}, _entry(book=2, cost=0.3)])
    assert len(grouped) == 1 and grouped[0]["book_number"] == 2

    # timeline skips invalid timestamps.
    tl = cost_tracker.timeline([{"timestamp": "bad", "cost_usd": 1.0}, _entry(cost=0.5)], granularity="hourly")
    assert tl and "cumulative_cost_usd" in tl[-1]


def test_budget_load_global_set_and_dump(tmp_path: Path, monkeypatch):
    budget_path = tmp_path / "budget.json"
    monkeypatch.setattr(cost_tracker.safe_json, "load_json", lambda *_args, **_kwargs: "bad")
    payload = cost_tracker.load_budget(budget_path)
    assert isinstance(payload["global"], dict)

    monkeypatch.setattr(
        cost_tracker.safe_json,
        "load_json",
        lambda *_args, **_kwargs: {"global": "bad", "catalogs": "bad", "overrides": "bad"},
    )
    payload2 = cost_tracker.load_budget(budget_path)
    assert isinstance(payload2["global"], dict)
    assert payload2["catalogs"] == {}
    assert payload2["overrides"] == {}

    # set_budget global branch.
    saved = {}
    monkeypatch.setattr(cost_tracker, "load_budget", lambda _path: {"global": {}, "catalogs": {}, "overrides": {}})
    monkeypatch.setattr(cost_tracker.safe_json, "atomic_write_json", lambda _path, payload: saved.update(payload))
    out = cost_tracker.set_budget(path=budget_path, catalog_id=None, limit_usd=20.0, warning_threshold=0.7, hard_stop=False)
    assert out["global"]["limit_usd"] == 20.0
    assert saved["global"]["hard_stop"] is False

    dumped = cost_tracker.dump_json(tmp_path / "ledger.json")
    assert dumped.startswith("{")
