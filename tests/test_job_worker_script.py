from __future__ import annotations

import sys

from scripts import job_worker


def test_job_worker_main_invokes_service(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_run_worker_service(*, catalog_id=None, worker_count=None):  # type: ignore[no-untyped-def]
        captured["catalog_id"] = catalog_id
        captured["worker_count"] = worker_count

    monkeypatch.setattr(job_worker.quality_review, "run_worker_service", _fake_run_worker_service)
    monkeypatch.setattr(sys, "argv", ["job_worker.py", "--catalog", "classics", "--workers", "3"])
    assert job_worker.main() == 0
    assert captured["catalog_id"] == "classics"
    assert captured["worker_count"] == 3

