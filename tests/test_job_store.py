from __future__ import annotations

from pathlib import Path

from src import job_store as js
from src.job_store import JobStore


def test_create_or_get_job_idempotent(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    payload = {"catalog": "classics", "book": 1}
    first, created_first = store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-1",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload=payload,
    )
    second, created_second = store.create_or_get_job(
        job_id="job-b",
        idempotency_key="idem-1",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload=payload,
    )
    assert created_first is True
    assert created_second is False
    assert first.id == second.id
    assert second.payload["book"] == 1


def test_create_or_get_job_idempotency_conflict_detected(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-conflict",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload={"catalog": "classics", "book": 1, "models": ["openai/a"]},
    )

    try:
        store.create_or_get_job(
            job_id="job-b",
            idempotency_key="idem-conflict",
            job_type="generate_cover",
            catalog_id="classics",
            book_number=1,
            payload={"catalog": "classics", "book": 1, "models": ["openrouter/b"]},
        )
        assert False, "expected IdempotencyConflictError"
    except js.IdempotencyConflictError as exc:
        details = exc.to_dict()
        assert details["idempotency_key"] == "idem-conflict"
        assert "payload" in details["conflict_fields"]


def test_lease_complete_and_status_counts(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-1",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload={"book": 1},
    )
    leased = store.lease_next_job(worker_id="worker-1", job_types=["generate_cover"])
    assert leased is not None
    assert leased.status == "running"

    finished = store.mark_completed(leased.id, result={"ok": True, "results": []})
    assert finished is not None
    assert finished.status == "completed"
    counts = store.status_counts()
    assert counts["completed"] == 1


def test_mark_failed_retries_until_failed(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job, _ = store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-1",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload={"book": 1},
        max_attempts=2,
    )

    state1 = store.mark_failed(job.id, error={"message": "temporary"}, retryable=True, retry_delay_seconds=1)
    assert state1 is not None
    assert state1.status == "retrying"
    assert state1.attempts == 1

    state2 = store.mark_failed(job.id, error={"message": "temporary"}, retryable=True, retry_delay_seconds=1)
    assert state2 is not None
    assert state2.status == "failed"
    assert state2.attempts == 2


def test_attempt_lifecycle(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job, _ = store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-1",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload={"book": 1},
    )
    attempt_id = store.record_attempt_start(job.id, attempt_number=1, meta={"worker_id": "w1"})
    assert attempt_id > 0
    store.record_attempt_end(attempt_id, status="completed", meta={"duration_ms": 42})
    attempts = store.list_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0]["status"] == "completed"
    assert attempts[0]["meta"]["duration_ms"] == 42


def test_slo_summary(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    complete, _ = store.create_or_get_job(
        job_id="job-complete",
        idempotency_key="idem-complete",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=1,
        payload={"book": 1},
        max_attempts=3,
    )
    failed, _ = store.create_or_get_job(
        job_id="job-fail",
        idempotency_key="idem-fail",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=2,
        payload={"book": 2},
        max_attempts=2,
    )
    store.mark_failed(complete.id, error={"message": "retry"}, retryable=True, retry_delay_seconds=0)
    store.mark_completed(complete.id, result={"ok": True})
    store.mark_failed(failed.id, error={"message": "fatal"}, retryable=False, retry_delay_seconds=0)

    slo = store.slo_summary(window_days=7, catalog_id="classics")
    assert slo["terminal_total"] == 2
    assert slo["completed_total"] == 1
    assert slo["manual_total"] == 1
    assert slo["retry_jobs"] == 1
    assert 0.0 <= slo["completion_without_manual_intervention"] <= 1.0


def test_list_filters_cancel_and_missing_paths(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job_a, _ = store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-a",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=10,
        payload={"book": 10},
    )
    job_b, _ = store.create_or_get_job(
        job_id="job-b",
        idempotency_key="idem-b",
        job_type="generate_cover",
        catalog_id="other",
        book_number=11,
        payload={"book": 11},
    )
    assert store.get_job_by_idempotency_key("idem-a") is not None
    assert store.get_job_by_idempotency_key("missing") is None

    rows_catalog = store.list_jobs(catalog_id="classics", statuses=["queued"], limit=10)
    assert len(rows_catalog) == 1
    assert rows_catalog[0].id == job_a.id

    cancelled = store.mark_cancelled(job_b.id, reason="test")
    assert cancelled is not None
    assert cancelled.status == "cancelled"
    assert store.mark_cancelled("missing-job", reason="x") is None
    assert store.lease_next_job(worker_id="w", job_types=["unknown"]) is None
    assert store.mark_failed("missing-job", error={"message": "x"}, retryable=True) is None


def test_recover_stale_running_jobs(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job, _ = store.create_or_get_job(
        job_id="job-run",
        idempotency_key="idem-run",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=21,
        payload={"book": 21},
    )
    leased = store.lease_next_job(worker_id="worker-1", job_types=["generate_cover"])
    assert leased is not None
    attempt_id = store.record_attempt_start(job.id, attempt_number=1, meta={"worker_id": "worker-1"})
    assert attempt_id > 0
    with store._managed_connection() as conn:  # type: ignore[attr-defined]
        conn.execute("UPDATE jobs SET started_at = NULL WHERE id = ?", (job.id,))
    recovered = store.recover_stale_running_jobs(stale_after_seconds=1, retry_delay_seconds=0.1)
    assert recovered == 1
    row = store.get_job(job.id)
    assert row is not None
    assert row.status == "retrying"
    assert row.attempts == 1
    attempts = store.list_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0]["status"] == "failed"
    assert "Recovered stale running job after restart" in attempts[0]["error_text"]


def test_recover_stale_running_jobs_marks_terminal_after_max_attempts(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job, _ = store.create_or_get_job(
        job_id="job-run-max",
        idempotency_key="idem-run-max",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=22,
        payload={"book": 22},
        max_attempts=1,
    )
    leased = store.lease_next_job(worker_id="worker-1", job_types=["generate_cover"])
    assert leased is not None
    store.record_attempt_start(job.id, attempt_number=1, meta={"worker_id": "worker-1"})
    with store._managed_connection() as conn:  # type: ignore[attr-defined]
        conn.execute("UPDATE jobs SET started_at = NULL WHERE id = ?", (job.id,))

    recovered = store.recover_stale_running_jobs(stale_after_seconds=1, retry_delay_seconds=0.1)
    assert recovered == 1
    row = store.get_job(job.id)
    assert row is not None
    assert row.status == "failed"
    assert row.attempts == 1


def test_recovered_stale_job_counts_as_retry_in_slo_summary(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job, _ = store.create_or_get_job(
        job_id="job-recover-slo",
        idempotency_key="idem-recover-slo",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=23,
        payload={"book": 23},
        max_attempts=3,
    )
    leased = store.lease_next_job(worker_id="worker-1", job_types=["generate_cover"])
    assert leased is not None
    store.record_attempt_start(job.id, attempt_number=1, meta={"worker_id": "worker-1"})
    with store._managed_connection() as conn:  # type: ignore[attr-defined]
        conn.execute("UPDATE jobs SET started_at = NULL WHERE id = ?", (job.id,))

    recovered = store.recover_stale_running_jobs(stale_after_seconds=1, retry_delay_seconds=0)
    assert recovered == 1

    leased_retry = store.lease_next_job(worker_id="worker-2", job_types=["generate_cover"])
    assert leased_retry is not None
    assert leased_retry.id == job.id
    store.mark_completed(job.id, result={"ok": True})

    slo = store.slo_summary(window_days=7, catalog_id="classics")
    assert slo["completed_total"] == 1
    assert slo["retry_jobs"] == 1


def test_internal_helpers_and_status_normalization():
    assert js._normalize_status("RUNNING") == "running"
    assert js._normalize_status("PAUSED") == "paused"
    assert js._normalize_status("weird") == "queued"
    assert js._from_json("{bad", {"x": 1}) == {"x": 1}
    assert js.JobStore._row_to_job(None) is None


def test_pause_resume_retry_and_delete(tmp_path: Path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job, _ = store.create_or_get_job(
        job_id="job-a",
        idempotency_key="idem-a",
        job_type="generate_cover",
        catalog_id="classics",
        book_number=7,
        payload={"book": 7},
    )
    paused = store.mark_paused(job.id, reason="manual")
    assert paused is not None
    assert paused.status == "paused"

    resumed = store.resume_job(job.id)
    assert resumed is not None
    assert resumed.status == "queued"

    failed = store.mark_failed(job.id, error={"message": "fatal"}, retryable=False)
    assert failed is not None
    assert failed.status == "failed"

    retried = store.retry_job(job.id)
    assert retried is not None
    assert retried.status == "queued"

    completed = store.mark_completed(job.id, result={"ok": True})
    assert completed is not None
    assert completed.status == "completed"
    assert store.delete_job(job.id) is True
    assert store.get_job(job.id) is None
