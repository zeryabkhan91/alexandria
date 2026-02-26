from __future__ import annotations

from types import SimpleNamespace

from src import notifications as nf


def test_event_color_mapping():
    assert nf._event_color("batch_complete") == "#2eb886"
    assert nf._event_color("batch_error") == "#e74c3c"
    assert nf._event_color("milestone") == "#f1c40f"
    assert nf._event_color("anything_else") == "#3498db"


def test_notifier_disabled_or_missing_url_does_not_post(monkeypatch):
    calls: list[tuple[str, dict, float]] = []

    def _fake_post(url, json, timeout):  # type: ignore[no-untyped-def]
        calls.append((url, json, timeout))
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(nf.requests, "post", _fake_post)

    runtime = SimpleNamespace(webhook_url="", webhook_events=["batch_complete"])
    notifier = nf.BatchNotifier(runtime=runtime, enabled=False)
    notifier.batch_complete(
        batch_id="b1",
        catalog_id="classics",
        completed_books=1,
        failed_books=0,
        total_books=1,
        total_cost=1.23,
        estimated_completion=None,
    )
    assert calls == []


def test_notifier_posts_supported_events(monkeypatch):
    calls: list[tuple[str, dict, float]] = []

    def _fake_post(url, json, timeout):  # type: ignore[no-untyped-def]
        calls.append((url, json, timeout))
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr(nf.requests, "post", _fake_post)

    runtime = SimpleNamespace(webhook_url="https://hooks.example.test", webhook_events=["batch_complete"])
    notifier = nf.BatchNotifier(runtime=runtime, enabled=True)
    notifier.batch_start(
        batch_id="b1",
        catalog_id="classics",
        total_books=10,
        workers=2,
        models=["m1", "m2"],
    )
    notifier.milestone(
        batch_id="b1",
        catalog_id="classics",
        completed_books=5,
        total_books=10,
        avg_cost_per_book=0.11,
        estimated_completion="2026-02-22T00:00:00+00:00",
    )
    notifier.batch_complete(
        batch_id="b1",
        catalog_id="classics",
        completed_books=10,
        failed_books=0,
        total_books=10,
        total_cost=1.23,
        estimated_completion=None,
    )

    # batch_start always allowed, milestone filtered out, batch_complete allowed
    assert len(calls) == 2
    assert calls[0][0] == "https://hooks.example.test"
    assert "Batch started" in calls[0][1]["text"]
    assert "Batch complete" in calls[1][1]["text"]


def test_notifier_handles_http_error_status(monkeypatch):
    def _fake_post(_url, _json, _timeout):  # type: ignore[no-untyped-def]
        return SimpleNamespace(status_code=500, text="boom")

    monkeypatch.setattr(nf.requests, "post", _fake_post)
    runtime = SimpleNamespace(webhook_url="https://hooks.example.test", webhook_events=[])
    notifier = nf.BatchNotifier(runtime=runtime, enabled=True)
    notifier.batch_error(batch_id="b2", catalog_id="c1", book_number=7, error="failed")

