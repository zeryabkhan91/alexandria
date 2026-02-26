from __future__ import annotations

from src import api_responses


def test_error_payload_shape():
    payload = api_responses.error_payload(
        code="INVALID_BOOK_NUMBER",
        message="Book number invalid",
        details={"received": "x"},
    )
    assert payload["error"] is True
    assert payload["code"] == "INVALID_BOOK_NUMBER"
    assert payload["message"] == "Book number invalid"
    assert payload["details"]["received"] == "x"


def test_success_payload_with_data_and_meta():
    payload = api_responses.success_payload(data={"ok": True}, meta={"count": 1})
    assert payload["error"] is False
    assert payload["data"]["ok"] is True
    assert payload["meta"]["count"] == 1


def test_success_payload_without_data():
    payload = api_responses.success_payload()
    assert payload == {"error": False}
