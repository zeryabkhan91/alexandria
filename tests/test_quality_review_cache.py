from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

import scripts.quality_review as qr
from scripts.quality_review import DataCache, RequestTracker
from scripts.quality_review import RollingSLOTracker, SLOAlertManager, SimpleRateLimiter


def test_data_cache_set_get_and_stats():
    cache = DataCache(ttl_seconds=60)
    assert cache.get("x") is None
    cache.set("x", {"value": 1})
    assert cache.get("x") == {"value": 1}
    stats = cache.stats()
    assert stats["entries"] == 1
    assert stats["hits"] >= 1


def test_data_cache_expiry():
    cache = DataCache(ttl_seconds=1)
    cache.set("x", {"value": 1})
    time.sleep(1.05)
    assert cache.get("x") is None


def test_data_cache_invalidate_prefix():
    cache = DataCache(ttl_seconds=60)
    cache.set("catalog:/api/review-data", {"ok": True})
    cache.set("catalog:/api/review-queue", {"ok": True})
    removed = cache.invalidate_prefix("catalog:/api/review-")
    assert removed == 2
    assert cache.get("catalog:/api/review-data") is None


def test_request_tracker_start_finish():
    tracker = RequestTracker()
    assert tracker.start("generate:1") is True
    assert tracker.start("generate:1") is False
    assert tracker.active() == ["generate:1"]
    tracker.finish("generate:1")
    assert tracker.active() == []


def test_simple_rate_limiter():
    limiter = SimpleRateLimiter(per_minute=2)
    assert limiter.allow("ip-a") is True
    assert limiter.allow("ip-a") is True
    assert limiter.allow("ip-a") is False
    assert limiter.allow("ip-b") is True


def test_rolling_slo_tracker(tmp_path: Path):
    tracker = RollingSLOTracker(tmp_path / "slo.json")
    tracker.record_response(200)
    tracker.record_response(201)
    tracker.record_response(503)
    snapshot = tracker.snapshot(window_days=7)
    assert snapshot["total_requests"] == 3
    assert snapshot["server_errors"] == 1
    assert 0.0 <= snapshot["success_rate"] <= 1.0
    tracker.flush()
    assert (tmp_path / "slo.json").exists()


def test_rolling_slo_tracker_catalog_scope(tmp_path: Path):
    tracker = RollingSLOTracker(tmp_path / "slo.json")
    tracker.record_response(200, catalog_id="classics")
    tracker.record_response(503, catalog_id="classics")
    tracker.record_response(200, catalog_id="modern")

    classics = tracker.snapshot(window_days=7, catalog_id="classics")
    modern = tracker.snapshot(window_days=7, catalog_id="modern")
    all_catalogs = tracker.snapshot(window_days=7)

    assert classics["total_requests"] == 2
    assert classics["server_errors"] == 1
    assert modern["total_requests"] == 1
    assert modern["server_errors"] == 0
    assert all_catalogs["total_requests"] == 3
    assert all_catalogs["server_errors"] == 1
    assert all_catalogs["successful_requests"] == 2


def test_build_slo_evaluation_uses_catalog_scoped_api_snapshot(monkeypatch):
    calls: dict[str, dict[str, object]] = {}

    def _snapshot(*, window_days: int, catalog_id: str | None = None):  # type: ignore[no-untyped-def]
        calls["snapshot"] = {"window_days": window_days, "catalog_id": catalog_id}
        return {"success_rate": 1.0, "total_requests": 0, "server_errors": 0, "client_errors": 0}

    def _slo_summary(*, window_days: int, catalog_id: str):  # type: ignore[no-untyped-def]
        calls["slo_summary"] = {"window_days": window_days, "catalog_id": catalog_id}
        return {
            "window_days": window_days,
            "completion_without_manual_intervention": 1.0,
            "same_stage_retry_rate": 0.0,
            "terminal_total": 0,
            "retry_jobs": 0,
        }

    monkeypatch.setattr(qr, "_slo_tracker_for_runtime", lambda _runtime: SimpleNamespace(snapshot=_snapshot))
    monkeypatch.setattr(qr, "job_db_store", SimpleNamespace(slo_summary=_slo_summary))

    runtime = SimpleNamespace(catalog_id="classics", slo_window_days=7)
    qr._build_slo_evaluation(runtime=runtime)

    assert calls["snapshot"]["catalog_id"] == "classics"
    assert calls["slo_summary"]["catalog_id"] == "classics"


def test_slo_tracker_and_alert_manager_helpers_are_catalog_scoped(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(qr, "_slo_trackers_by_path", {})
    monkeypatch.setattr(qr, "_slo_alert_managers_by_path", {})

    runtime = SimpleNamespace(
        catalog_id="demo",
        data_dir=tmp_path,
        slo_alert_cooldown_seconds=600,
        slo_alert_levels=["breached", "at_risk"],
    )

    tracker_a = qr._slo_tracker_for_runtime(runtime)
    tracker_b = qr._slo_tracker_for_runtime(runtime)
    manager_a = qr._slo_alert_manager_for_runtime(runtime)
    manager_b = qr._slo_alert_manager_for_runtime(runtime)

    assert tracker_a is tracker_b
    assert manager_a is manager_b
    assert tracker_a.path == (tmp_path / "slo_metrics_demo.json")
    assert manager_a.state_path == (tmp_path / "slo_alert_state_demo.json")


def test_slo_alert_manager_cooldown(monkeypatch, tmp_path: Path):
    class DummyResponse:
        status_code = 200
        text = "ok"

    sent: list[dict[str, object]] = []

    def _fake_post(url, json, timeout):  # type: ignore[no-untyped-def]
        sent.append({"url": url, "payload": json, "timeout": timeout})
        return DummyResponse()

    monkeypatch.setattr("scripts.quality_review.requests.post", _fake_post)
    manager = SLOAlertManager(
        state_path=tmp_path / "alerts.json",
        cooldown_seconds=3600,
        alert_levels={"breached", "at_risk"},
    )
    runtime = SimpleNamespace(catalog_id="classics", webhook_url="https://hooks.example")
    evaluation = {
        "window_days": 7,
        "targets": {},
        "api_success_rate_7d": {"status": "breached", "actual": 0.9, "target": 0.995},
    }
    first = manager.maybe_alert(runtime=runtime, slo_evaluation=evaluation)
    second = manager.maybe_alert(runtime=runtime, slo_evaluation=evaluation)
    assert first["sent"] is True
    assert second["sent"] is False
    assert second["reason"] == "cooldown_active"
    assert len(sent) == 1
