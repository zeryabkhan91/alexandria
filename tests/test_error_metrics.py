from __future__ import annotations

from pathlib import Path

from src import error_metrics


def test_record_error_creates_metrics_file(tmp_path: Path, monkeypatch):
    metrics_path = tmp_path / "error_metrics.json"
    monkeypatch.setattr(error_metrics, "METRICS_PATH", metrics_path)

    error_metrics.record_error("INVALID_INPUT", endpoint="/api/test", details={"field": "book"})
    payload = error_metrics.get_metrics()

    assert payload["total"] == 1
    assert payload["by_code"]["INVALID_INPUT"] == 1
    assert len(payload["recent"]) == 1
    assert payload["recent"][0]["endpoint"] == "/api/test"


def test_record_error_accumulates_counts(tmp_path: Path, monkeypatch):
    metrics_path = tmp_path / "error_metrics.json"
    monkeypatch.setattr(error_metrics, "METRICS_PATH", metrics_path)

    error_metrics.record_error("A")
    error_metrics.record_error("A")
    error_metrics.record_error("B")
    payload = error_metrics.get_metrics()

    assert payload["total"] == 3
    assert payload["by_code"]["A"] == 2
    assert payload["by_code"]["B"] == 1


def test_record_error_normalizes_invalid_payload_shapes(tmp_path: Path, monkeypatch):
    metrics_path = tmp_path / "error_metrics.json"
    monkeypatch.setattr(error_metrics, "METRICS_PATH", metrics_path)
    captured = {}

    monkeypatch.setattr(error_metrics, "load_json", lambda *_args, **_kwargs: {"total": 5, "by_code": "bad", "recent": "bad"})
    monkeypatch.setattr(error_metrics, "atomic_write_json", lambda _path, payload: captured.update(payload))

    error_metrics.record_error("E1")
    assert captured["total"] == 6
    assert captured["by_code"]["E1"] == 1
    assert isinstance(captured["recent"], list)


def test_metrics_getter_fallback_for_non_dict(monkeypatch):
    monkeypatch.setattr(error_metrics, "load_json", lambda *_args, **_kwargs: "invalid")
    payload = error_metrics.get_metrics()
    assert payload["total"] == 0
    assert payload["by_code"] == {}


def test_record_and_get_metrics_support_catalog_scoped_default_paths(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(error_metrics.config, "DATA_DIR", tmp_path)
    error_metrics.record_error("SCOPED", catalog_id="demo")
    payload = error_metrics.get_metrics(catalog_id="demo")
    assert payload["total"] == 1
    assert payload["by_code"]["SCOPED"] == 1
    assert (tmp_path / "error_metrics_demo.json").exists()
