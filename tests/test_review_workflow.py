from __future__ import annotations

import uuid

from tests.test_quality_review_server_smoke import _request_json, _start_server, _stop_server


def _pick_review_candidate(base_url: str) -> tuple[int, int]:
    status, payload = _request_json(base_url, "/api/review-data?catalog=classics&limit=200&offset=0")
    assert status == 200
    books = payload.get("books", []) if isinstance(payload, dict) else []
    for row in books if isinstance(books, list) else []:
        if not isinstance(row, dict):
            continue
        book_number = int(row.get("number", 0) or 0)
        variants = row.get("variants", [])
        if book_number <= 0 or not isinstance(variants, list) or not variants:
            continue
        first = variants[0]
        if not isinstance(first, dict):
            continue
        variant = int(first.get("variant", first.get("variant_id", 0)) or 0)
        if variant > 0:
            return book_number, variant
    raise RuntimeError("No review candidates with variants found")


def test_review_selection_and_session_roundtrip():
    process, base_url = _start_server()
    try:
        book, variant = _pick_review_candidate(base_url)
        status, saved = _request_json(
            base_url,
            "/api/review-selection?catalog=classics",
            method="POST",
            payload={"book": book, "variant": variant, "reviewer": "pytest"},
        )
        assert status == 200
        assert saved.get("ok") is True
        assert int(saved.get("saved", {}).get("winner", 0) or 0) == variant

        session_id = f"pytest-{uuid.uuid4()}"
        status, session_saved = _request_json(
            base_url,
            "/api/save-review-session?catalog=classics",
            method="POST",
            payload={
                "session_id": session_id,
                "books_reviewed": 1,
                "books_remaining": 0,
                "reviewer": "pytest",
                "completed": False,
            },
        )
        assert status == 200
        assert session_saved.get("ok") is True

        status, session_loaded = _request_json(base_url, f"/api/review-session/{session_id}?catalog=classics")
        assert status == 200
        assert session_loaded.get("ok") is True
        assert str(session_loaded.get("session", {}).get("session_id", "")) == session_id
    finally:
        _stop_server(process)


def test_review_queue_and_batch_approve_smoke():
    process, base_url = _start_server()
    try:
        status, queue = _request_json(base_url, "/api/review-queue?catalog=classics&threshold=0.9")
        assert status == 200
        assert queue.get("success") is True
        assert isinstance(queue.get("queue", []), list)

        status, approved = _request_json(
            base_url,
            "/api/batch-approve?catalog=classics",
            method="POST",
            payload={"threshold": 0.9},
        )
        assert status == 200
        assert approved.get("ok") is True
        assert "summary" in approved
    finally:
        _stop_server(process)


def test_export_all_and_export_status_tracking_smoke():
    process, base_url = _start_server()
    try:
        book, variant = _pick_review_candidate(base_url)
        status, _saved = _request_json(
            base_url,
            "/api/review-selection?catalog=classics",
            method="POST",
            payload={"book": book, "variant": variant, "reviewer": "pytest-export"},
        )
        assert status == 200

        status, exported = _request_json(
            base_url,
            "/api/export/all?catalog=classics",
            method="POST",
            payload={"books": [book], "platforms": ["web"]},
        )
        assert status == 200
        assert exported.get("ok") in {True, False}
        assert "combined_export_id" in exported

        status, tracking = _request_json(base_url, "/api/export/status?catalog=classics&limit=500&offset=0")
        assert status == 200
        assert tracking.get("ok") is True
        rows = tracking.get("items", [])
        assert isinstance(rows, list)
        assert any(int(row.get("book_number", 0) or 0) == book for row in rows if isinstance(row, dict))
    finally:
        _stop_server(process)


def test_delivery_tracking_payload_includes_exports_field():
    process, base_url = _start_server()
    try:
        status, payload = _request_json(base_url, "/api/delivery/tracking?catalog=classics&limit=20&offset=0")
        assert status == 200
        assert payload.get("ok") is True
        items = payload.get("items", [])
        assert isinstance(items, list)
        if items:
            first = items[0]
            assert isinstance(first, dict)
            assert "exports" in first
    finally:
        _stop_server(process)


def test_review_speed_stats_endpoint_smoke():
    process, base_url = _start_server()
    try:
        status, payload = _request_json(base_url, "/api/review-stats?catalog=classics")
        assert status == 200
        assert isinstance(payload, dict)
        assert "sessions" in payload
    finally:
        _stop_server(process)
