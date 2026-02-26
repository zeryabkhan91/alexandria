from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from tests.test_quality_review_server_smoke import _request_json, _start_server, _stop_server


def _first_models(base_url: str) -> list[str]:
    status, payload = _request_json(base_url, "/api/iterate-data?catalog=classics")
    assert status == 200
    models = payload.get("models", []) if isinstance(payload, dict) else []
    assert isinstance(models, list) and models
    return [str(models[0])]


def _request_json_allow_error(base_url: str, path: str, *, method: str = "GET", payload: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(payload or {}).encode("utf-8") if method in {"POST", "PUT", "PATCH"} else None
    request = Request(
        f"{base_url}{path}",
        method=method,
        data=data,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    try:
        with urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8")
            return int(getattr(response, "status", 200)), (json.loads(body) if body else {})
    except HTTPError as exc:
        body = exc.read().decode("utf-8")
        return int(exc.code), (json.loads(body) if body else {})


def test_batch_generate_start_and_status_fields():
    process, base_url = _start_server()
    try:
        models = _first_models(base_url)
        status, created = _request_json(
            base_url,
            "/api/batch-generate?catalog=classics",
            method="POST",
            payload={
                "catalog": "classics",
                "books": [1, 2],
                "models": models,
                "variants": 1,
                "promptSource": "template",
                "budgetUsd": 2.5,
            },
        )
        assert status == 200
        assert created.get("ok") is True
        batch_id = str(created.get("batchId", ""))
        assert batch_id

        status, snapshot = _request_json(base_url, f"/api/batch-generate/{batch_id}/status?catalog=classics&limit=10&offset=0")
        assert status == 200
        assert snapshot.get("ok") is True
        assert snapshot.get("batchId") == batch_id
        assert "progress" in snapshot
        assert "workers" in snapshot
        assert "cost" in snapshot
        assert "pagination" in snapshot
    finally:
        _stop_server(process)


def test_batch_generate_pagination_for_status_rows():
    process, base_url = _start_server()
    try:
        models = _first_models(base_url)
        status, created = _request_json(
            base_url,
            "/api/batch-generate?catalog=classics",
            method="POST",
            payload={
                "catalog": "classics",
                "books": [1, 2, 3],
                "models": models,
                "variants": 1,
                "promptSource": "template",
                "budgetUsd": 5,
            },
        )
        assert status == 200
        batch_id = str(created.get("batchId", ""))
        assert batch_id

        status, page = _request_json(base_url, f"/api/batch-generate/{batch_id}/status?catalog=classics&limit=1&offset=1")
        assert status == 200
        assert page.get("ok") is True
        books = page.get("books", [])
        assert isinstance(books, list)
        assert len(books) <= 1
        pagination = page.get("pagination", {})
        assert int(pagination.get("limit", 0) or 0) == 1
        assert int(pagination.get("offset", 0) or 0) == 1
    finally:
        _stop_server(process)


def test_batch_pause_resume_cancel_endpoints():
    process, base_url = _start_server()
    try:
        models = _first_models(base_url)
        status, created = _request_json(
            base_url,
            "/api/batch-generate?catalog=classics",
            method="POST",
            payload={
                "catalog": "classics",
                "books": [1, 2],
                "models": models,
                "variants": 1,
                "promptSource": "template",
                "budgetUsd": 5,
            },
        )
        assert status == 200
        batch_id = str(created.get("batchId", ""))
        assert batch_id

        for action in ("pause", "resume", "cancel"):
            status, payload = _request_json(
                base_url,
                f"/api/batch-generate/{batch_id}/{action}?catalog=classics",
                method="POST",
                payload={"reason": f"test {action}"},
            )
            assert status == 200
            assert payload.get("ok") is True
    finally:
        _stop_server(process)


def test_batch_generate_requires_books():
    process, base_url = _start_server()
    try:
        status, payload = _request_json_allow_error(
            base_url,
            "/api/batch-generate?catalog=classics",
            method="POST",
            payload={
                "catalog": "classics",
                "books": [],
                "models": ["openrouter/google/gemini-2.5-flash-image"],
                "variants": 1,
            },
        )
        assert status == 400
        assert payload.get("ok") is False
    finally:
        _stop_server(process)


def test_batch_event_stream_ready_event():
    process, base_url = _start_server()
    try:
        models = _first_models(base_url)
        status, created = _request_json(
            base_url,
            "/api/batch-generate?catalog=classics",
            method="POST",
            payload={
                "catalog": "classics",
                "books": [1],
                "models": models,
                "variants": 1,
                "promptSource": "template",
                "budgetUsd": 1,
            },
        )
        assert status == 200
        batch_id = str(created.get("batchId", ""))
        assert batch_id

        request = Request(f"{base_url}/api/events/batch/{batch_id}?catalog=classics")
        with urlopen(request, timeout=10) as response:
            lines: list[str] = []
            for _ in range(12):
                line = response.readline().decode("utf-8", errors="ignore")
                if not line:
                    break
                lines.append(line)
                if "event: ready" in line:
                    break
            body = "".join(lines)
        assert "event: ready" in body
    finally:
        _stop_server(process)
