from __future__ import annotations

import json
from pathlib import Path

from src import audit_log


def test_build_event_signed_and_verify(monkeypatch):
    monkeypatch.setenv("AUDIT_LOG_SECRET", "super-secret")
    event = audit_log.build_event(
        action="generate_async",
        impact="cost",
        actor="tim",
        source_ip="127.0.0.1",
        endpoint="/api/generate",
        catalog_id="classics",
        status="ok",
        details={"book": 2},
    )
    assert event["signature_status"] == "signed"
    assert bool(event["signature"])
    assert audit_log.verify_event_signature(event) is True


def test_append_event_redacts_and_loads(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("AUDIT_LOG_SECRET", "super-secret")
    path = tmp_path / "audit.json"
    event = audit_log.append_event(
        action="sync_to_drive",
        impact="destructive",
        actor="tim",
        source_ip="127.0.0.1",
        endpoint="/api/sync-to-drive",
        catalog_id="classics",
        status="ok",
        details={"api_token": "abc", "nested": {"secret_key": "xyz"}},
        path=path,
    )
    assert event["details"]["api_token"] == "[REDACTED]"
    assert event["details"]["nested"]["secret_key"] == "[REDACTED]"
    rows = audit_log.load_events(path)
    assert len(rows) == 1
    assert rows[0]["action"] == "sync_to_drive"


def test_verify_signature_without_secret_is_false(monkeypatch):
    monkeypatch.delenv("AUDIT_LOG_SECRET", raising=False)
    event = {
        "timestamp": "2026-01-01T00:00:00+00:00",
        "action": "x",
        "impact": "cost",
        "actor": "a",
        "source_ip": "127.0.0.1",
        "endpoint": "/api/x",
        "catalog_id": "classics",
        "status": "ok",
        "details": {},
        "signature": "abc",
        "signature_status": "signed",
    }
    assert audit_log.verify_event_signature(event) is False


def test_load_events_invalid_payload(tmp_path: Path):
    path = tmp_path / "bad.json"
    path.write_text("{bad", encoding="utf-8")
    assert audit_log.load_events(path) == []
    path.write_text(json.dumps({"items": "bad"}), encoding="utf-8")
    assert audit_log.load_events(path) == []


def test_build_event_unsigned_and_list_redaction(monkeypatch):
    monkeypatch.delenv("AUDIT_LOG_SECRET", raising=False)
    event = audit_log.build_event(
        action="generate",
        impact="cost",
        actor="",
        source_ip="",
        endpoint="",
        catalog_id="",
        status="",
        details={"nested": [{"auth_header": "token-1"}, {"ok": True}]},
    )
    assert event["signature_status"] == "unsigned_no_secret"
    assert event["signature"] == ""
    assert event["details"]["nested"][0]["auth_header"] == "[REDACTED]"


def test_verify_signature_rejects_invalid_shapes(monkeypatch):
    monkeypatch.setenv("AUDIT_LOG_SECRET", "secret")
    assert audit_log.verify_event_signature("not-a-dict") is False
    assert audit_log.verify_event_signature({"action": "x", "signature": ""}) is False


def test_append_event_normalizes_non_dict_payload_and_non_list_items(monkeypatch, tmp_path: Path):
    path = tmp_path / "audit.json"
    calls: list[dict] = []
    responses = ["bad-payload", {"updated_at": "", "items": "bad-items"}]

    def _fake_load(_path, _default):
        return responses.pop(0)

    def _fake_write(_path, payload):
        calls.append(payload)

    monkeypatch.setattr(audit_log, "load_json", _fake_load)
    monkeypatch.setattr(audit_log, "atomic_write_json", _fake_write)
    monkeypatch.delenv("AUDIT_LOG_SECRET", raising=False)

    audit_log.append_event(
        action="one",
        impact="cost",
        actor="tim",
        source_ip="127.0.0.1",
        endpoint="/api/one",
        catalog_id="classics",
        status="ok",
        path=path,
    )
    audit_log.append_event(
        action="two",
        impact="cost",
        actor="tim",
        source_ip="127.0.0.1",
        endpoint="/api/two",
        catalog_id="classics",
        status="ok",
        path=path,
    )

    assert len(calls) == 2
    assert isinstance(calls[0]["items"], list)
    assert isinstance(calls[1]["items"], list)
    assert calls[0]["items"]
    assert calls[1]["items"]


def test_load_events_non_dict_root_returns_empty(tmp_path: Path):
    path = tmp_path / "rows.json"
    path.write_text("[]", encoding="utf-8")
    assert audit_log.load_events(path) == []


def test_append_event_default_path_is_catalog_scoped(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(audit_log.config, "DATA_DIR", tmp_path)
    audit_log.append_event(
        action="generate_async",
        impact="cost",
        actor="tim",
        source_ip="127.0.0.1",
        endpoint="/api/generate",
        catalog_id="demo",
        status="ok",
        details={"book": 3},
    )
    assert (tmp_path / "audit_log_demo.json").exists()
